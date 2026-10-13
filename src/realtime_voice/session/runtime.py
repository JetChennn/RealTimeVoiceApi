"""单会话的监督生命周期与 Effect 执行器。"""

from __future__ import annotations

import asyncio
import base64
import logging
from collections.abc import AsyncIterator, Callable, Coroutine
from dataclasses import dataclass, replace
from math import isfinite
from time import monotonic
from typing import TYPE_CHECKING, Generic, Protocol, TypeVar

from realtime_voice.audio.pcm import pcm16_wav_bytes
from realtime_voice.audio.resampler import StreamingResampler
from realtime_voice.observability.logging import log_event
from realtime_voice.observability.metrics import Metrics

if TYPE_CHECKING:
    from realtime_voice.audio.vad import SpeechSegment
    from realtime_voice.speaker.gate import SessionSpeakerGate
from realtime_voice.clients.asr import AsrError
from realtime_voice.clients.asr_keywords import AsrKeywordExtractor, KeywordResult
from realtime_voice.clients.limits import AdmissionOverloaded
from realtime_voice.clients.thinker import (
    ThinkerClient,
    ThinkerDone,
    ThinkerReplyRequest,
    ThinkerStreamError,
    ThinkerTextDelta,
)
from realtime_voice.clients.tts import (
    TTS_SAMPLE_RATE,
    TtsClient,
    TtsEndTurn,
    TtsInput,
    TtsRequest,
    TtsStreamCancelled,
    TtsStreamError,
    TtsText,
)
from realtime_voice.protocol.decoder import DecodedSpeakerRegister
from realtime_voice.protocol.server_messages import (
    AudioDelta,
    ErrorMessage,
    ResponseEnd,
    ServerMessage,
    SpeakerRegisterResult,
    SpeakerStatus,
    TurnState,
)
from realtime_voice.session.actor import (
    CancelBistreamTts,
    CloseRuntime,
    EndBistreamTts,
    ExtractAsrKeywords,
    QueueAsr,
    RecordDiscardedAudio,
    RecordStaleEvent,
    RecordThinkerFallback,
    SendBistreamText,
    SendOutbound,
    SessionActor,
    SessionEffect,
    StartBistreamTts,
    StartNextThinker,
    StartThinker,
    StartTts,
    ThinkerFallbackPolicy,
)
from realtime_voice.session.events import (
    AsrFailed,
    AsrKeywordsExtracted,
    AsrSucceeded,
    AudioSegmentDiscarded,
    SessionEvent,
    SpeechSegmentReady,
    ThinkerCompleted,
    ThinkerDeltaReceived,
    ThinkerFailed,
    ThinkerSkipped,
    TtsChunkReceived,
    TtsCompleted,
    TtsFailed,
    UserInputCommitted,
)
from realtime_voice.session.registry import SessionRegistry
from realtime_voice.session.state import TERMINAL_TURN_STAGES, SessionState, TurnStage

DEFAULT_AUDIO_QUEUE_MAX_SECONDS = 3.0
DEFAULT_OUTBOUND_QUEUE_MAX_BYTES = 8 * 1024 * 1024
DEFAULT_SLOW_STAGE_WARNING_SECONDS = 2.0
KNOWN_FAILURE_REASONS = frozenset(
    {
        "client_disconnected",
        "first_audio_timeout",
        "idle_timeout",
        "internal_error",
        "TTS_FIRST_AUDIO_TIMEOUT",
        "TTS_PCM_ALIGNMENT",
        "TTS_PCM_EMPTY",
        "TTS_SAMPLE_RATE",
        "TTS_STREAM_FAILED",
        "TTS_STREAM_IDLE_TIMEOUT",
    }
)

QueueItem = TypeVar("QueueItem")


@dataclass(frozen=True, slots=True)
class QueueSnapshot:
    items: int
    bytes: int | None = None


class SessionQueueOverloaded(RuntimeError):
    """当单个待入队数据会使会话队列字节预算超限时抛出。"""

    def __init__(
        self,
        queue_name: str,
        *,
        item_bytes: int,
        queued_bytes: int,
        max_bytes: int,
    ) -> None:
        self.code = f"{queue_name.upper()}_QUEUE_BYTES_EXCEEDED"
        self.queue_name = queue_name
        self.item_bytes = item_bytes
        self.queued_bytes = queued_bytes
        self.max_bytes = max_bytes
        super().__init__(self.code)


class SlowClient(RuntimeError):
    """当有界出站队列无法再接纳新消息时抛出。"""

    code = "SLOW_CLIENT"


class BoundedByteQueue(asyncio.Queue[QueueItem], Generic[QueueItem]):
    """条目数有界的队列，同时立即拒绝超出字节预算的入队。"""

    def __init__(
        self,
        *,
        name: str,
        maxsize: int,
        max_bytes: int,
        item_size: Callable[[QueueItem], int],
        metrics: Metrics | None = None,
    ) -> None:
        if maxsize < 1:
            raise ValueError("queue size must be at least 1")
        if max_bytes < 1:
            raise ValueError("queue byte limit must be at least 1")
        super().__init__(maxsize=maxsize)
        self.name = name
        self.max_bytes = max_bytes
        self._item_size = item_size
        self._queued_bytes = 0
        self._metrics = metrics

    @classmethod
    def audio(
        cls,
        *,
        maxsize: int,
        max_bytes: int,
        metrics: Metrics | None = None,
    ) -> BoundedByteQueue[bytes | None]:
        """创建音频队列，使用运行时所需的 PCM 字节计量规则。"""
        return cls(
            name="audio",
            maxsize=maxsize,
            max_bytes=max_bytes,
            item_size=_audio_item_size,
            metrics=metrics,
        )

    @classmethod
    def outbound(
        cls,
        *,
        maxsize: int,
        max_bytes: int,
        metrics: Metrics | None = None,
    ) -> BoundedByteQueue[ServerMessage]:
        """创建出站队列，使用运行时所需的 JSON 字节计量规则。"""
        return cls(
            name="outbound",
            maxsize=maxsize,
            max_bytes=max_bytes,
            item_size=_outbound_item_size,
            metrics=metrics,
        )

    @property
    def queued_bytes(self) -> int:
        """返回当前已入队的负载字节数。"""
        return self._queued_bytes

    def put_nowait(self, item: QueueItem) -> None:
        self._reject_byte_overflow(item)
        try:
            super().put_nowait(item)
        except asyncio.QueueFull:
            if self._metrics is not None:
                self._metrics.record_queue_overload(self.name, "items")
            raise

    async def put(self, item: QueueItem) -> None:
        self._reject_byte_overflow(item)
        await super().put(item)

    def _put(self, item: QueueItem) -> None:
        self._queued_bytes += self._item_size(item)
        super()._put(item)

    def _get(self) -> QueueItem:
        item = super()._get()
        self._queued_bytes -= self._item_size(item)
        return item

    def _reject_byte_overflow(self, item: QueueItem) -> None:
        item_bytes = self._item_size(item)
        if item_bytes < 0:
            raise ValueError("queue item size must not be negative")
        if self._queued_bytes + item_bytes > self.max_bytes:
            if self._metrics is not None:
                self._metrics.record_queue_overload(self.name, "bytes")
            raise SessionQueueOverloaded(
                self.name,
                item_bytes=item_bytes,
                queued_bytes=self._queued_bytes,
                max_bytes=self.max_bytes,
            )

    def discard(self, predicate: Callable[[QueueItem], bool]) -> tuple[int, int]:
        """同步移除匹配条目并保持队列字节计数准确。"""
        retained: list[QueueItem] = []
        removed_items = 0
        removed_bytes = 0
        while self._queue:  # type: ignore[attr-defined]  # asyncio.Queue 的事件循环私有 deque
            item = self._queue.popleft()  # type: ignore[attr-defined]
            if predicate(item):
                removed_items += 1
                removed_bytes += self._item_size(item)
            else:
                retained.append(item)
        self._queue.extend(retained)  # type: ignore[attr-defined]
        self._queued_bytes -= removed_bytes
        # 防御性保护，避免观测计数因异常 predicate 漂移。
        self._queued_bytes = max(self._queued_bytes, 0)
        if removed_items:
            self._wakeup_next(self._putters)  # type: ignore[attr-defined]
        return removed_items, removed_bytes


class AsyncWorker(Protocol):
    """由会话 TaskGroup 监督的长驻工作协程。"""

    async def run(self) -> None: ...


class AsrClientProtocol(Protocol):
    async def transcribe(self, pcm16_16k: bytes, *, context: str = "") -> str: ...


class RegistryProtocol(Protocol):
    async def remove(self, key: str) -> None: ...


class SessionStop(Exception):
    """内部哨兵异常，用于一次性停止所有长驻工作协程。"""


class SessionRuntime:
    """持有单个会话的全部异步任务与队列。"""

    def __init__(
        self,
        *,
        state: SessionState,
        asr_client: AsrClientProtocol,
        thinker_client: ThinkerClient,
        tts_client: TtsClient,
        receiver: AsyncWorker,
        vad_worker: AsyncWorker,
        sender: AsyncWorker,
        rag_enabled: bool = False,
        scenes: tuple[str, ...] = (),
        registry: RegistryProtocol | None = None,
        event_queue_size: int = 256,
        audio_queue_size: int = 64,
        asr_queue_size: int = 64,
        outbound_queue_size: int = 256,
        audio_queue_max_seconds: float = DEFAULT_AUDIO_QUEUE_MAX_SECONDS,
        outbound_queue_max_bytes: int = DEFAULT_OUTBOUND_QUEUE_MAX_BYTES,
        event_queue: asyncio.Queue[SessionEvent] | None = None,
        audio_queue: BoundedByteQueue[bytes | None] | None = None,
        outbound_queue: BoundedByteQueue[ServerMessage] | None = None,
        asr_context_enabled: bool = True,
        asr_keyword_extractor: AsrKeywordExtractor | None = None,
        asr_context_extract_timeout_ms: float = 100,
        thinker_stream_timeout: float = 5.0,
        thinker_reply_total_timeout: float = 20.0,
        thinker_fallback_enabled: bool = False,
        thinker_fallback_texts: tuple[str, ...] = (),
        thinker_fallback_tone: str = "",
        metrics: Metrics | None = None,
        tts_prompt_override: str = "",
        slow_stage_warning_seconds: float = DEFAULT_SLOW_STAGE_WARNING_SECONDS,
        logger: logging.Logger | None = None,
        clock: Callable[[], float] = monotonic,
        turn_end_settings=None,
        semantic_detector=None,
        speaker_gate: SessionSpeakerGate | None = None,
    ) -> None:
        if min(event_queue_size, audio_queue_size, asr_queue_size, outbound_queue_size) < 1:
            raise ValueError("session queue sizes must be at least 1")
        if event_queue is not None and event_queue.maxsize < 1:
            raise ValueError("injected event queue must be bounded")
        if audio_queue is not None and not isinstance(audio_queue, BoundedByteQueue):
            raise TypeError("injected audio queue must enforce a byte budget")
        if outbound_queue is not None and not isinstance(outbound_queue, BoundedByteQueue):
            raise TypeError("injected outbound queue must enforce a byte budget")
        if not isfinite(audio_queue_max_seconds) or audio_queue_max_seconds <= 0:
            raise ValueError("audio queue duration must be positive and finite")
        if outbound_queue_max_bytes < 1:
            raise ValueError("outbound queue byte limit must be at least 1")
        if not isfinite(slow_stage_warning_seconds) or slow_stage_warning_seconds <= 0:
            raise ValueError("slow stage warning threshold must be positive and finite")

        self.actor = SessionActor(
            state,
            thinker_fallback=ThinkerFallbackPolicy(
                enabled=thinker_fallback_enabled,
                texts=thinker_fallback_texts,
                tone=thinker_fallback_tone,
            ),
            # 测试和外部注入的轻量 TTS fake 不必实现双向能力，默认按旧模式处理。
            bistream_enabled=bool(getattr(tts_client, "bistream_enabled", False)),
        )
        self.events = event_queue or asyncio.Queue(maxsize=event_queue_size)
        audio_max_bytes = int(state.sample_rate * 2 * audio_queue_max_seconds)
        if audio_queue is not None:
            _validate_injected_queue(
                audio_queue,
                queue_name="audio",
                maxsize=audio_queue_size,
                max_bytes=audio_max_bytes,
                item_size=_audio_item_size,
            )
        if outbound_queue is not None:
            _validate_injected_queue(
                outbound_queue,
                queue_name="outbound",
                maxsize=outbound_queue_size,
                max_bytes=outbound_queue_max_bytes,
                item_size=_outbound_item_size,
            )
        self.audio_queue = audio_queue or BoundedByteQueue.audio(
            maxsize=audio_queue_size,
            max_bytes=audio_max_bytes,
            metrics=metrics,
        )
        self.outbound = outbound_queue or BoundedByteQueue.outbound(
            maxsize=outbound_queue_size,
            max_bytes=outbound_queue_max_bytes,
            metrics=metrics,
        )
        self._asr_queue: asyncio.Queue[SpeechSegment] = asyncio.Queue(maxsize=asr_queue_size)

        self._thinker_rag_enabled = rag_enabled
        self._thinker_rag_scenes = tuple(scenes)
        self._asr_client = asr_client
        self._asr_context_enabled = asr_context_enabled
        self._asr_keyword_extractor = asr_keyword_extractor
        self._asr_context_extract_timeout = asr_context_extract_timeout_ms / 1000
        self._keyword_tasks: set[asyncio.Task[None]] = set()
        self._thinker_client = thinker_client
        self._tts_client = tts_client
        self._receiver = receiver
        self._vad_worker = vad_worker
        self._sender = sender
        self._registry = registry
        self._thinker_stream_timeout = thinker_stream_timeout
        self._thinker_reply_total_timeout = thinker_reply_total_timeout
        self._tts_prompt_override = str(tts_prompt_override or "").strip()
        self._slow_stage_warning_seconds = slow_stage_warning_seconds
        self._logger = logger or logging.getLogger(__name__)
        self._metrics = metrics
        self._clock = clock
        self._speaker_gate = speaker_gate
        self._last_audio_backpressure_notice_at: float | None = None

        self._close_requested = asyncio.Event()
        self._long_tasks: dict[str, asyncio.Task[None]] = {}
        self._background_tasks: set[asyncio.Task[None]] = set()
        self._thinker_tasks: set[asyncio.Task[None]] = set()
        self._speech_ends: dict[int, float] = {}
        self._turn_speech_ends: dict[int, float] = {}
        self._llm_milestones: set[int] = set()
        self._tts_tasks: set[asyncio.Task[None]] = set()
        self._tts_tasks_by_key: dict[tuple[int, int], asyncio.Task[None]] = {}
        self._tts_interrupt_signals: dict[tuple[int, int], asyncio.Event] = {}
        self._bistream_inputs: dict[tuple[int, int], asyncio.Queue[TtsInput]] = {}
        self._thinker_lock = asyncio.Lock()
        self._thinker_cleanup_safe = True
        self._closing = False
        self._is_running = False
        self._cleanup_lock = asyncio.Lock()
        self._cleanup_task: asyncio.Task[None] | None = None
        self._cleaned = False
        self.turn_end = None
        if semantic_detector is not None:
            from realtime_voice.turn_end.coordinator import TurnEndCoordinator

            self.turn_end = TurnEndCoordinator(
                state.session_id,
                turn_end_settings,
                semantic_detector,
                vad_worker.turn_end_audio,
                self.events,
                metrics,
                self._semantic_context,
                user_id=state.user_id,
            )

    @property
    def session_id(self) -> str:
        return self.actor.state.session_id

    @property
    def user_id(self) -> str:
        return self.actor.state.user_id

    @property
    def device_id(self) -> str:
        """The V1 protocol device identifier; retained as user_id for compatibility."""
        return self.actor.state.user_id

    @property
    def sample_rate(self) -> int:
        return self.actor.state.sample_rate

    @property
    def is_running(self) -> bool:
        return self._is_running

    @property
    def background_task_count(self) -> int:
        return sum(not task.done() for task in self._background_tasks)

    def queue_snapshot(self) -> dict[str, QueueSnapshot]:
        """返回本地各队列的占用快照，不等待也不修改运行时状态。"""
        return {
            "event": QueueSnapshot(self.events.qsize()),
            "audio": QueueSnapshot(self.audio_queue.qsize(), self.audio_queue.queued_bytes),
            "asr": QueueSnapshot(self._asr_queue.qsize()),
            "outbound": QueueSnapshot(self.outbound.qsize(), self.outbound.queued_bytes),
        }

    def bind_registry(self, registry: SessionRegistry) -> None:
        """绑定创建该会话的注册表，确保清理时一定释放其准入名额。"""
        if self._registry is not None and self._registry is not registry:
            raise RuntimeError("session runtime is already bound to another registry")
        self._registry = registry

    def request_close(self) -> None:
        """请求有序清理并关闭 TaskGroup。"""
        self._close_requested.set()

    def report_audio_backpressure(
        self,
        limit: str,
        queue_items: int,
        queued_bytes: int,
        chunk_bytes: int,
    ) -> None:
        """Report a dropped inbound chunk without terminating the session.

        Repeated drops are coalesced to at most one client warning per second. The
        queue's own overload metric still counts every rejected chunk.
        """
        now = self._clock()
        previous = self._last_audio_backpressure_notice_at
        if previous is not None and now - previous < 1.0:
            return
        self._last_audio_backpressure_notice_at = now
        self._observe(
            "client_audio_dropped",
            level=logging.WARNING,
            stage="TRANSPORT",
            error_code="CLIENT_AUDIO_BACKPRESSURE",
            queue_limit=limit,
            queue_items=queue_items,
            queued_bytes=queued_bytes,
            chunk_bytes=chunk_bytes,
        )
        message = ErrorMessage(
            type="ERROR",
            user_id=self.user_id,
            session_id=self.session_id,
            turn_id=0,
            interrupt=False,
            stage="TRANSPORT",
            code="CLIENT_AUDIO_BACKPRESSURE",
            message="server audio queue is full; this audio chunk was dropped and the current utterance may be incomplete",
            recoverable=True,
        )
        try:
            self.outbound.put_nowait(message)
        except (asyncio.QueueFull, SessionQueueOverloaded) as error:
            self.request_close()
            raise SlowClient(str(error)) from error

    def should_send_outbound(self, message: ServerMessage) -> bool:
        """在真正写入设备 WebSocket 前做最后一次旧轮音频隔离。"""
        if not isinstance(message, AudioDelta):
            return True
        turn = self.actor.state.turns.get(message.turn_id)
        return (
            turn is not None
            and not turn.interrupted
            and turn.stage in {TurnStage.STREAMING_LLM, TurnStage.STREAMING_TTS}
        )

    def report_outbound_discard(self, message: ServerMessage) -> None:
        """记录 Sender 已取出、但在写入前被中断状态拦截的音频。"""
        if not isinstance(message, AudioDelta):
            return
        try:
            byte_count = len(base64.b64decode(message.audio_b64, validate=True))
        except ValueError:
            byte_count = 0
        turn = self.actor.state.turns.get(message.turn_id)
        generation = turn.tts_generation if turn is not None else 0
        self._observe_discarded_tts_audio(
            message.turn_id,
            generation,
            self._turn_trace_id(message.turn_id),
            byte_count,
            source="websocket_sender",
        )

    async def run(self) -> None:
        """在单个 TaskGroup 中并行运行 Receiver/VAD/ASR/Actor/Sender 五个长任务。"""
        if self._is_running:
            raise RuntimeError("session runtime is already running")
        if self._cleaned:
            raise RuntimeError("session runtime cannot be restarted after cleanup")
        self._is_running = True
        self._observe("session_created", stage="SESSION")

        try:
            try:
                async with asyncio.TaskGroup() as group:
                    # 同一 TaskGroup 中并行启动 5 个长任务，任一异常都会取消整组
                    self._long_tasks = {
                        "receiver": group.create_task(
                            self._receiver_loop(), name=f"{self.session_id}:receiver"
                        ),
                        "vad": group.create_task(
                            self._worker_loop(self._vad_worker), name=f"{self.session_id}:vad"
                        ),
                        "asr": group.create_task(self._asr_loop(), name=f"{self.session_id}:asr"),
                        "actor": group.create_task(
                            self._actor_loop(), name=f"{self.session_id}:actor"
                        ),
                        "sender": group.create_task(
                            self._worker_loop(self._sender), name=f"{self.session_id}:sender"
                        ),
                    }
                    await self._close_requested.wait()
                    # 收到关闭请求后先做清理，再用 SessionStop 哨兵统一终止所有长任务
                    await self._finish_cleanup()
                    raise SessionStop
            except* SessionStop:
                # 哨兵异常仅用于优雅停止，吞掉即可
                pass
        finally:
            try:
                await self._finish_cleanup()
            finally:
                self._is_running = False
                self._long_tasks.clear()

    async def execute_effect(self, effect: SessionEffect) -> None:
        """执行 Actor 产出的一条 Effect，避免工作协程直接修改 Actor 状态。"""
        if isinstance(effect, ExtractAsrKeywords):
            if not self._closing and self._asr_context_enabled:
                deadline = monotonic() + self._asr_context_extract_timeout
                self._spawn_background(
                    self._extract_asr_keywords(effect, deadline),
                    tasks=self._keyword_tasks,
                    name=f"{self.session_id}:keywords:{effect.turn_id}:{effect.generation}",
                )
            return
        if isinstance(effect, SendOutbound):
            if isinstance(effect.message, TurnState):
                # 先启用输出抑制并移除尚未发给设备的旧音频，再异步取消下游。
                self._discard_queued_turn_audio(effect.message.turn_id)
                self._signal_tts_interruption(effect.message.turn_id)
                self._observe("turn_interrupted", turn_id=effect.message.turn_id, interrupt=True)
                if self._metrics is not None:
                    self._metrics.record_interruption()
            elif isinstance(effect.message, ResponseEnd) and effect.message.status == "FAILED":
                # 失败不是用户打断，也不能让已排队的旧音频在 FAILED 之后继续抵达设备。
                self._discard_queued_turn_audio(effect.message.turn_id)
            if not self._closing:
                try:
                    self.outbound.put_nowait(effect.message)
                except (asyncio.QueueFull, SessionQueueOverloaded) as error:
                    self.request_close()
                    self._observe(
                        "slow_client",
                        level=logging.WARNING,
                        stage="TRANSPORT",
                        error_code=SlowClient.code,
                        error_type=type(error).__name__,
                    )
                    if self._metrics is not None:
                        self._metrics.record_slow_client_close()
                    raise SlowClient(str(error)) from error
            return

        if isinstance(effect, QueueAsr):
            if not self._closing and effect.session_id == self.session_id:
                await self._asr_queue.put(effect.segment)
            return
        if isinstance(effect, (StartThinker, StartNextThinker)):
            if not self._closing:
                self._spawn_background(
                    self._run_thinker(effect),
                    tasks=self._thinker_tasks,
                    name=f"{self.session_id}:thinker:{effect.turn_id}:{effect.generation}",
                    marks_thinker=True,
                )
            return
        if isinstance(effect, StartTts):
            if not self._closing:
                key = (effect.turn_id, effect.generation)
                interrupt_signal = asyncio.Event()
                self._tts_interrupt_signals[key] = interrupt_signal
                # 若该轮 TTS 启动前即被判定为应抑制，立即取消。
                if self._tts_output_suppressed(effect):
                    interrupt_signal.set()
                task = self._spawn_background(
                    self._run_tts(effect, interrupt_signal),
                    tasks=self._tts_tasks,
                    name=f"{self.session_id}:tts:{effect.turn_id}:{effect.generation}",
                )
                self._track_tts_task(key, task)
                if interrupt_signal.is_set():
                    task.cancel()
            return
        if isinstance(effect, StartBistreamTts):
            if not self._closing:
                key = (effect.turn_id, effect.generation)
                if key in self._bistream_inputs:
                    return
                inputs: asyncio.Queue[TtsInput] = asyncio.Queue(maxsize=256)
                self._bistream_inputs[key] = inputs
                interrupt_signal = asyncio.Event()
                self._tts_interrupt_signals[key] = interrupt_signal
                task = self._spawn_background(
                    self._run_bistream_tts(effect, inputs, interrupt_signal),
                    tasks=self._tts_tasks,
                    name=f"{self.session_id}:tts-bistream:{effect.turn_id}:{effect.generation}",
                )
                self._track_tts_task(key, task)
                if self._tts_output_suppressed(effect):
                    interrupt_signal.set()
                    task.cancel()
            return
        if isinstance(effect, SendBistreamText):
            self._put_bistream_input(
                effect.turn_id, effect.generation, TtsText(effect.delta)
            )
            return
        if isinstance(effect, EndBistreamTts):
            self._put_bistream_input(effect.turn_id, effect.generation, TtsEndTurn())
            return
        if isinstance(effect, CancelBistreamTts):
            self._signal_tts_interruption(effect.turn_id)
            return
        if isinstance(effect, CloseRuntime):
            if effect.session_id == self.session_id:
                self.request_close()
            return
        if isinstance(effect, RecordStaleEvent):
            self._observe(
                "stale_session_event",
                turn_id=effect.turn_id,
                event_type=effect.event_type,
                reason=effect.reason,
            )
            return
        if isinstance(effect, RecordDiscardedAudio):
            self._observe(
                "tts_chunk_discarded",
                turn_id=effect.turn_id,
                generation=effect.generation,
                byte_count=effect.byte_count,
            )
            if self._metrics is not None:
                self._metrics.record_discarded_tts_chunk(byte_count=effect.byte_count)
            return
        if isinstance(effect, RecordThinkerFallback):
            self._observe(
                "thinker_fallback_selected",
                stage="LLM",
                turn_id=effect.turn_id,
                error_code=effect.code,
                tts_requested=effect.tts_requested,
            )
            if self._metrics is not None:
                self._metrics.record_thinker_fallback(
                    effect.code,
                    tts_requested=effect.tts_requested,
                )
            return
        raise TypeError(f"unsupported session effect: {type(effect).__name__}")

    async def register_speaker(self, request: DecodedSpeakerRegister) -> None:
        """Register or replace this session's reference voiceprint."""
        pcm16_16k = StreamingResampler(request.sample_rate, 16000).process_pcm16(
            request.pcm16, final=True
        )
        if self._speaker_gate is None:
            success = False
            registered = False
            replaced = False
            reason = "disabled"
            duration_ms = request.duration_ms
            min_audio_ms = 0.0
        else:
            result = await self._speaker_gate.register(pcm16_16k)
            success = result.success
            registered = result.registered
            replaced = result.replaced
            reason = result.reason
            duration_ms = result.duration_ms
            min_audio_ms = result.min_audio_ms

        codes = {
            "registered": "REGISTERED",
            "updated": "UPDATED",
            "too_short": "AUDIO_TOO_SHORT",
            "no_speech": "NO_SPEECH",
            "embedding_failed": "EMBEDDING_FAILED",
            "embedder_unavailable": "EMBEDDER_UNAVAILABLE",
            "disabled": "SPEAKER_DISABLED",
        }
        messages = {
            "registered": "speaker voiceprint registered",
            "updated": "speaker voiceprint replaced",
            "too_short": "speaker registration audio is too short",
            "no_speech": "speaker registration audio contains no usable speech signal",
            "embedding_failed": "speaker embedding extraction failed",
            "embedder_unavailable": "speaker embedding model is unavailable",
            "disabled": "speaker verification is disabled",
        }
        self._observe(
            "speaker_registration_completed",
            stage="SPEAKER",
            status=reason,
            duration_ms=duration_ms,
            registered=registered,
            replaced=replaced,
        )
        await self.execute_effect(
            SendOutbound(
                SpeakerRegisterResult(
                    type="SPEAKER_REGISTER_RESULT",
                    user_id=self.user_id,
                    session_id=self.session_id,
                    turn_id=0,
                    interrupt=False,
                    request_id=request.request_id,
                    success=success,
                    registered=registered,
                    replaced=replaced,
                    code=codes.get(reason, "REGISTER_FAILED"),
                    message=messages.get(reason, "speaker registration failed"),
                    duration_ms=duration_ms,
                    min_audio_ms=min_audio_ms,
                )
            )
        )

    async def _receiver_loop(self) -> None:
        await self._worker_loop(self._receiver)

    async def _worker_loop(self, worker: AsyncWorker) -> None:
        await worker.run()
        self.request_close()

    def _semantic_context(self) -> list[dict[str, str]]:
        """只向轮次结束模型提供上一轮已经产生的完整回复。"""
        if not self.actor.state.turns:
            return []
        previous = next(reversed(self.actor.state.turns.values()))
        if not previous.reply_text:
            return []
        return [{"role": "assistant", "content": previous.reply_text}]

    def _asr_context(self) -> tuple[str, str, int | None]:
        """读取最近一条合格回复的关键词缓存，绝不等待或现场提取。"""
        if not self._asr_context_enabled:
            return "disabled", "", None
        for turn in reversed(self.actor.state.turns.values()):
            if (
                turn.reply_text
                and turn.stage in {TurnStage.STREAMING_TTS, TurnStage.COMPLETED, TurnStage.FAILED}
                and not turn.interrupted
                and not turn.thinker_fallback_used
                and turn.thinker_failure_code is None
            ):
                if turn.asr_keywords_status == "success" and turn.asr_keywords:
                    return "provided", turn.asr_keywords, turn.turn_id
                return "unavailable", "", None
        return "unavailable", "", None

    async def _extract_asr_keywords(self, effect: ExtractAsrKeywords, deadline: float) -> None:
        started = deadline - self._asr_context_extract_timeout
        extractor = self._asr_keyword_extractor
        result = (
            await extractor.extract(effect.text, deadline=deadline)
            if extractor is not None
            else KeywordResult("failed")
        )
        elapsed = monotonic() - started
        self._observe(
            "asr_keywords_extracted",
            stage="ASR_CONTEXT",
            turn_id=effect.turn_id,
            generation=effect.generation,
            status=result.status,
            duration_ms=elapsed * 1000,
            keyword_count=result.count,
            context_chars=len(result.context),
        )
        if self._metrics is not None:
            self._metrics.record_asr_keyword_extraction(
                result.status,
                elapsed=elapsed,
                compute_seconds=result.compute_seconds,
                count=result.count,
            )
        await self._publish_event(
            AsrKeywordsExtracted(
                self.session_id, effect.turn_id, effect.generation,
                result.context, result.status, deadline,
            )
        )

    async def _actor_loop(self) -> None:
        while True:
            event = await self.events.get()
            if self.turn_end is not None:
                if isinstance(event, SpeechSegmentReady):
                    self._speech_ends[event.segment.segment_id] = event.speech_end_at
                    await self._asr_queue.put(event.segment)
                elif isinstance(event, (AsrSucceeded, AsrFailed)):
                    speech_end = self._speech_ends.pop(event.segment_id, None)
                    if speech_end is not None and self._metrics is not None:
                        self._metrics.observe_speech_end_to_asr(self._clock() - speech_end)
                elif isinstance(event, AudioSegmentDiscarded):
                    self._speech_ends.pop(event.segment_id, None)
                for ready in self.turn_end.process(event):
                    await self._handle_actor_event(ready)
            else:
                await self._handle_actor_event(event)

    async def _handle_actor_event(self, event):
        if isinstance(event, AsrKeywordsExtracted) and monotonic() >= event.deadline:
            event = replace(event, context="", status="timeout")
            self._observe(
                "asr_keywords_expired", stage="ASR_CONTEXT", turn_id=event.turn_id,
                generation=event.generation, status="timeout",
            )
        if isinstance(event, SpeechSegmentReady):
            self._speech_ends.setdefault(
                event.segment.segment_id,
                event.speech_end_at if event.speech_end_at is not None else self._clock(),
            )
            self._observe("vad_segment_ready", segment_id=event.segment.segment_id)
        next_turn_id = self.actor.state.next_turn_id
        effects = self.actor.handle(event)
        if isinstance(event, UserInputCommitted):
            self._turn_speech_ends[next_turn_id] = event.speech_end_at
        elif isinstance(event, AsrSucceeded):
            speech_end = self._speech_ends.pop(event.segment_id, None)
            if speech_end is not None and self._metrics is not None:
                self._metrics.observe_speech_end_to_asr(self._clock() - speech_end)
            if speech_end is not None and self.actor.state.next_turn_id == next_turn_id + 1:
                self._turn_speech_ends[next_turn_id] = speech_end
        elif isinstance(event, (AsrFailed, AudioSegmentDiscarded)):
            # ASR 失败不产生 turn，也不再需要这段的结束时间，及时清理避免字典随段数无限增长
            self._speech_ends.pop(event.segment_id, None)
        if isinstance(event, (ThinkerCompleted, ThinkerFailed, TtsCompleted, TtsFailed)):
            turn = self.actor.state.turns.get(event.turn_id)
            if turn is not None and turn.stage in TERMINAL_TURN_STAGES:
                self._turn_speech_ends.pop(event.turn_id, None)
                self._llm_milestones.discard(event.turn_id)
        for effect in effects:
            await self.execute_effect(effect)

    async def _asr_loop(self) -> None:
        while True:
            segment = await self._asr_queue.get()
            trace_id = self._segment_trace_id(segment.segment_id)
            if self._speaker_gate is not None:
                decision = await self._speaker_gate.evaluate(segment)
                if self._metrics is not None:
                    self._metrics.record_speaker_decision(decision.reason)
                self._observe(
                    "speaker_gate_decision",
                    stage="SPEAKER",
                    segment_id=segment.segment_id,
                    trace_id=trace_id,
                    decision=decision.reason,
                    allow=decision.allow,
                    registered=decision.registered,
                    similarity=decision.similarity,
                    voiced_ms=decision.voiced_ms,
                    min_audio_ms=decision.min_audio_ms,
                    audio_eligible=decision.audio_eligible,
                )
                await self.execute_effect(
                    SendOutbound(
                        SpeakerStatus(
                            type="SPEAKER_STATUS",
                            user_id=self.user_id,
                            session_id=self.session_id,
                            turn_id=0,
                            interrupt=False,
                            segment_id=segment.segment_id,
                            state=decision.reason,
                            registered=decision.registered,
                            voiced_ms=decision.voiced_ms,
                            min_audio_ms=decision.min_audio_ms,
                            audio_eligible=decision.audio_eligible,
                            similarity=decision.similarity,
                            allowed=decision.allow,
                        )
                    )
                )
                if not decision.allow:
                    if not self._closing:
                        await self.events.put(
                            AudioSegmentDiscarded(
                                session_id=self.session_id,
                                segment_id=segment.segment_id,
                                reason=decision.reason,
                            )
                        )
                    continue
            started = self._clock()
            context_status, context, context_turn_id = self._asr_context()
            self._observe(
                "asr_started",
                stage="ASR",
                segment_id=segment.segment_id,
                trace_id=trace_id,
                context_status=context_status,
                context_chars=len(context),
                context_turn_id=context_turn_id,
            )
            if self._metrics is not None:
                self._metrics.record_asr_context(context_status, char_count=len(context))
            try:
                text = await self._asr_client.transcribe(
                    segment.pcm16_16k,
                    context=context,
                )
                event: SessionEvent = AsrSucceeded(
                    session_id=self.session_id,
                    segment_id=segment.segment_id,
                    text=text,
                    audio_wav=pcm16_wav_bytes(segment.pcm16_16k, 16000),
                    trailing_silence_ms=segment.trailing_silence_samples / 16,
                )
            except AdmissionOverloaded as error:
                self._observe(
                    "asr_failed",
                    level=logging.WARNING,
                    stage="ASR",
                    segment_id=segment.segment_id,
                    trace_id=trace_id,
                    error_code="SERVICE_OVERLOADED",
                    error_type=type(error).__name__,
                )
                event = AsrFailed(
                    session_id=self.session_id,
                    segment_id=segment.segment_id,
                    code="SERVICE_OVERLOADED",
                    message=str(error),
                )
            except AsrError as error:
                self._observe(
                    "asr_failed",
                    level=logging.ERROR,
                    stage="ASR",
                    segment_id=segment.segment_id,
                    trace_id=trace_id,
                    error_code="ASR_FAILED",
                    error_type=type(error).__name__,
                )
                event = AsrFailed(
                    session_id=self.session_id,
                    segment_id=segment.segment_id,
                    code="ASR_FAILED",
                    message=_error_message(error, "ASR transcription failed"),
                )
            except Exception as error:  # noqa: BLE001 - worker failures become Actor events
                self._observe(
                    "asr_failed",
                    level=logging.ERROR,
                    exc_info=error,
                    stage="ASR",
                    segment_id=segment.segment_id,
                    trace_id=trace_id,
                    error_code="ASR_FAILED",
                    error_type=type(error).__name__,
                )
                event = AsrFailed(
                    session_id=self.session_id,
                    segment_id=segment.segment_id,
                    code="ASR_FAILED",
                    message=_error_message(error, "ASR transcription failed"),
                )
            elapsed = self._clock() - started
            if self._metrics is not None:
                self._metrics.observe_stage_latency("asr", elapsed)
                if isinstance(event, AsrFailed):
                    self._metrics.record_error("asr", event.code)
            if isinstance(event, AsrSucceeded):
                self._observe(
                    "asr_completed",
                    stage="ASR",
                    segment_id=segment.segment_id,
                    trace_id=trace_id,
                    duration_ms=elapsed * 1000,
                )
                self._observe_slow(
                    "asr_slow",
                    elapsed,
                    stage="ASR",
                    segment_id=segment.segment_id,
                    trace_id=trace_id,
                )
            if not self._closing:
                await self.events.put(event)

    def _spawn_background(
        self,
        coroutine: Coroutine[object, object, None],
        *,
        tasks: set[asyncio.Task[None]],
        name: str,
        marks_thinker: bool = False,
    ) -> asyncio.Task[None]:
        task = asyncio.create_task(coroutine, name=name)
        tasks.add(task)
        self._background_tasks.add(task)

        def finished(done: asyncio.Task[None]) -> None:
            tasks.discard(done)
            self._background_tasks.discard(done)
            if marks_thinker and done.cancelled():
                self._thinker_cleanup_safe = False

        task.add_done_callback(finished)
        return task

    def _track_tts_task(
        self,
        key: tuple[int, int],
        task: asyncio.Task[None],
    ) -> None:
        self._tts_tasks_by_key[key] = task

        def forget(done: asyncio.Task[None]) -> None:
            if self._tts_tasks_by_key.get(key) is done:
                self._tts_tasks_by_key.pop(key, None)
                self._tts_interrupt_signals.pop(key, None)
                self._bistream_inputs.pop(key, None)

        task.add_done_callback(forget)

    async def _run_thinker(self, effect: StartThinker | StartNextThinker) -> None:
        first_event_seen = False
        trace_id = self._turn_trace_id(effect.turn_id)
        try:
            async with self._thinker_lock:
                # thinker_lock 串行化所有 Thinker 请求，确保同一会话内不会并发调用 LLM
                if self._closing:
                    return
                turn = self.actor.state.turns.get(effect.turn_id)
                if turn is not None:
                    if (
                        turn.thinker_generation != effect.generation
                        or turn.stage is not TurnStage.STREAMING_LLM
                        or self.actor.state.active_llm_turn_id != effect.turn_id
                    ):
                        return
                    if turn.interrupted:
                        await self._publish_event(
                            ThinkerSkipped(self.session_id, effect.turn_id, effect.generation)
                        )
                        return
                if isinstance(effect, StartNextThinker) and effect.interrupt_first:
                    # 用户打断场景：先中断 Thinker 当前会话再发起新请求
                    await self._thinker_client.interrupt(self.user_id, self.session_id)
                request = ThinkerReplyRequest(
                    user_id=self.user_id,
                    session_id=self.session_id,
                    text=effect.text,
                    rag_enabled=self._thinker_rag_enabled,
                    rag_scenes=self._thinker_rag_scenes,
                )
                reply_text: str | None = None
                tone = ""
                first_delta = True
                first_delta_ts: float | None = None
                started = self._clock()
                self._observe(
                    "thinker_started",
                    stage="LLM",
                    turn_id=effect.turn_id,
                    generation=effect.generation,
                    trace_id=trace_id,
                )
                # 流消费段受两段式超时约束：
                # - 外层 thinker_reply_total_timeout 限制整轮回复总时长（含 done 处理）；
                # - 首个事件到达前 thinker_stream_timeout 单独限制首字等待（首包/首事件预算），
                #   首事件到达后即解除首字计时，只受总时长约束。
                # 任一超时即取消本轮，避免卡住的回复流长期占据会话的 LLM 串行槽；
                # Thinker 内部的 RAG 检索包含在该时限内
                stream = self._thinker_client.stream_reply(request).__aiter__()
                async with asyncio.timeout(self._thinker_reply_total_timeout):
                    while True:
                        try:
                            if first_event_seen:
                                item = await stream.__anext__()
                            else:
                                async with asyncio.timeout(self._thinker_stream_timeout):
                                    item = await stream.__anext__()
                                first_event_seen = True
                        except StopAsyncIteration:
                            break
                        if isinstance(item, ThinkerTextDelta):
                            if first_delta:
                                first_delta = False
                                first_delta_ts = self._clock()
                                elapsed = first_delta_ts - started
                                self._observe(
                                    "thinker_first_delta",
                                    stage="LLM",
                                    turn_id=effect.turn_id,
                                    generation=effect.generation,
                                    trace_id=trace_id,
                                    duration_ms=elapsed * 1000,
                                )
                                self._observe_slow(
                                    "thinker_first_delta_slow",
                                    elapsed,
                                    stage="LLM",
                                    turn_id=effect.turn_id,
                                    generation=effect.generation,
                                    trace_id=trace_id,
                                )
                                if self._metrics is not None:
                                    self._metrics.observe_stage_latency("thinker", elapsed)
                                    speech_end = self._turn_speech_ends.get(effect.turn_id)
                                    if (
                                        speech_end is not None
                                        and effect.turn_id not in self._llm_milestones
                                    ):
                                        self._llm_milestones.add(effect.turn_id)
                                        self._metrics.observe_speech_end_to_first_llm(
                                            first_delta_ts - speech_end
                                        )
                            await self._publish_event(
                                ThinkerDeltaReceived(
                                    session_id=self.session_id,
                                    turn_id=effect.turn_id,
                                    generation=effect.generation,
                                    delta=item.delta,
                                )
                            )
                        elif isinstance(item, ThinkerDone):
                            reply_text = item.reply_text
                            tone = item.tone
                            done_ts = self._clock()
                            if self._metrics is not None:
                                # 发起→末字：含首 token 等待，与非流式全量口径一致，可与 TTS prompt 全量对比
                                self._metrics.observe_thinker_full(done_ts - started)
                                if first_delta_ts is not None:
                                    # 首字→末字：衡量回复纯生成阶段的耗时
                                    self._metrics.observe_thinker_generate(
                                        done_ts - first_delta_ts
                                    )
                    if reply_text is None:
                        raise RuntimeError("Thinker stream ended without done")
                    if not reply_text.strip():
                        code = "THINKER_EMPTY_REPLY"
                        if self._metrics is not None:
                            self._metrics.record_error("thinker", code)
                        self._observe(
                            "thinker_failed",
                            level=logging.WARNING,
                            stage="LLM",
                            turn_id=effect.turn_id,
                            generation=effect.generation,
                            trace_id=trace_id,
                            error_code=code,
                            error_type="EmptyReply",
                        )
                        await self._publish_event(
                            ThinkerFailed(
                                session_id=self.session_id,
                                turn_id=effect.turn_id,
                                generation=effect.generation,
                                code=code,
                                message="Thinker reply text is empty",
                            )
                        )
                        return
                    completed_elapsed = self._clock() - started
                    self._observe(
                        "thinker_completed",
                        stage="LLM",
                        turn_id=effect.turn_id,
                        generation=effect.generation,
                        trace_id=trace_id,
                        duration_ms=completed_elapsed * 1000,
                    )
                    await self._publish_event(
                        ThinkerCompleted(
                            session_id=self.session_id,
                            turn_id=effect.turn_id,
                            generation=effect.generation,
                            reply_text=reply_text,
                            tone=tone,
                        )
                    )
        except AdmissionOverloaded as error:
            self._observe(
                "thinker_failed",
                level=logging.WARNING,
                stage="LLM",
                turn_id=effect.turn_id,
                generation=effect.generation,
                trace_id=trace_id,
                error_code="SERVICE_OVERLOADED",
                error_type=type(error).__name__,
            )
            if self._metrics is not None:
                self._metrics.record_error("thinker", "SERVICE_OVERLOADED")
            await self._publish_event(
                ThinkerFailed(
                    session_id=self.session_id,
                    turn_id=effect.turn_id,
                    generation=effect.generation,
                    code="SERVICE_OVERLOADED",
                    message=str(error),
                )
            )
        except TimeoutError:
            if not first_event_seen:
                # 首字超时：从进入流消费起，首个事件（无论 text_delta 还是别的）始终未到达
                code = "THINKER_TIMEOUT"
                message = (
                    f"Thinker 未在 {self._thinker_stream_timeout:g} 秒内开始返回回复，"
                    "本轮已取消，请继续对话"
                )
            else:
                # 总时长超时：回复已开始但超过总预算仍未完成
                code = "THINKER_REPLY_TIMEOUT"
                message = (
                    f"Thinker 回复超过 {self._thinker_reply_total_timeout:g} 秒未完成，"
                    "本轮已取消，请继续对话"
                )
            if self._metrics is not None:
                self._metrics.record_error("thinker", code)
            self._observe(
                "thinker_failed",
                level=logging.WARNING,
                stage="LLM",
                turn_id=effect.turn_id,
                generation=effect.generation,
                trace_id=trace_id,
                error_code=code,
                error_type="TimeoutError",
            )
            # fire-and-forget 通知 Thinker 收尾：不占 admission 槽，也不阻塞本轮失败发布
            self._notify_thinker_interrupt()
            await self._publish_event(
                ThinkerFailed(
                    session_id=self.session_id,
                    turn_id=effect.turn_id,
                    generation=effect.generation,
                    code=code,
                    message=message,
                )
            )
        except ThinkerStreamError as error:
            # 远端 error 事件收尾的流错误：错误码原样透传（如 THINKER_TIMEOUT/THINKER_SESSION_BUSY），
            # 流已在远端正常终结，无需再 fire-and-forget interrupt；本地流错误仍需通知收尾
            code = error.code or "THINKER_STREAM_FAILED"
            if self._metrics is not None:
                self._metrics.record_error("thinker", code)
            if not error.code:
                self._notify_thinker_interrupt()
            self._observe(
                "thinker_failed",
                level=logging.WARNING if "TIMEOUT" in code.upper() else logging.ERROR,
                stage="LLM",
                turn_id=effect.turn_id,
                generation=effect.generation,
                trace_id=trace_id,
                error_code=code,
                error_type=type(error).__name__,
                reason=_known_failure_reason(error),
            )
            await self._publish_event(
                ThinkerFailed(
                    session_id=self.session_id,
                    turn_id=effect.turn_id,
                    generation=effect.generation,
                    code=code,
                    message=_error_message(error, "Thinker reply stream failed"),
                )
            )
        except Exception as error:  # noqa: BLE001 - background failures become Actor events
            if self._metrics is not None:
                self._metrics.record_error("thinker", "THINKER_STREAM_FAILED")
            self._notify_thinker_interrupt()
            self._observe(
                "thinker_failed",
                level=logging.ERROR,
                exc_info=error,
                stage="LLM",
                turn_id=effect.turn_id,
                generation=effect.generation,
                trace_id=trace_id,
                error_code="THINKER_STREAM_FAILED",
                error_type=type(error).__name__,
            )
            await self._publish_event(
                ThinkerFailed(
                    session_id=self.session_id,
                    turn_id=effect.turn_id,
                    generation=effect.generation,
                    code="THINKER_STREAM_FAILED",
                    message=_error_message(error, "Thinker reply stream failed"),
                )
            )

    def _notify_thinker_interrupt(self) -> None:
        """流超时/失败后尽力通知 Thinker 中断收尾；失败不影响会话。"""

        async def notify() -> None:
            try:
                await self._thinker_client.interrupt(self.user_id, self.session_id)
            except Exception as error:  # noqa: BLE001 - best-effort notification
                self._observe(
                    "thinker_interrupt_notify_failed",
                    level=logging.WARNING,
                    stage="LLM",
                    error_code="THINKER_INTERRUPT_NOTIFY_FAILED",
                    error_type=type(error).__name__,
                )

        task = asyncio.create_task(
            notify(), name=f"{self.session_id}:thinker-interrupt-notify"
        )
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def _run_tts(
        self,
        effect: StartTts,
        interrupt_signal: asyncio.Event,
    ) -> None:
        key = (effect.turn_id, effect.generation)
        first_audio = True
        started = self._clock()
        last_chunk_at: float | None = None
        chunk_count = 0
        trace_id = self._turn_trace_id(effect.turn_id)
        resampler = StreamingResampler(TTS_SAMPLE_RATE, self.sample_rate)
        # 新旧模式统一使用网关固定 prompt；不再等待 Thinker done.tone。
        prompt = self._tts_prompt_override or "平和"
        request = TtsRequest(
            model_reply=effect.reply_text,
            prompt=prompt,
            trace_id=trace_id,
        )
        self._observe(
            "tts_started",
            stage="TTS",
            turn_id=effect.turn_id,
            generation=effect.generation,
            trace_id=trace_id,
        )
        try:
            async for chunk in self._tts_client.stream(request):
                received_at = self._clock()
                chunk_count += 1
                if last_chunk_at is not None:
                    gap = received_at - last_chunk_at
                    if gap >= self._slow_stage_warning_seconds:
                        self._observe(
                            "tts_chunk_gap_slow",
                            level=logging.WARNING,
                            stage="TTS",
                            turn_id=effect.turn_id,
                            generation=effect.generation,
                            trace_id=trace_id,
                            sequence=chunk.chunk_index,
                            gap_ms=gap * 1000,
                        )
                last_chunk_at = received_at
                if self._tts_output_suppressed(effect):
                    self._observe_discarded_tts_audio(
                        effect.turn_id,
                        effect.generation,
                        trace_id,
                        len(chunk.pcm16_24k),
                        source="tts_receive",
                    )
                    continue
                output = resampler.process_pcm16(
                    chunk.pcm16_24k,
                    final=chunk.finalize,
                )
                if output:
                    if first_audio:
                        first_audio = False
                        elapsed = self._clock() - started
                        self._observe(
                            "tts_first_audio",
                            stage="TTS",
                            turn_id=effect.turn_id,
                            generation=effect.generation,
                            trace_id=trace_id,
                            duration_ms=elapsed * 1000,
                        )
                        self._observe_slow(
                            "tts_first_audio_slow",
                            elapsed,
                            stage="TTS",
                            turn_id=effect.turn_id,
                            generation=effect.generation,
                            trace_id=trace_id,
                        )
                        if self._metrics is not None:
                            self._metrics.observe_stage_latency("tts", elapsed)
                            self._observe_speech_end_to_first_tts(effect.turn_id)
                    await self._publish_event(
                        TtsChunkReceived(
                            session_id=self.session_id,
                            turn_id=effect.turn_id,
                            generation=effect.generation,
                            sequence=chunk.chunk_index,
                            pcm16=output,
                            finalize=chunk.finalize,
                        )
                    )
            self._observe(
                "tts_completed",
                stage="TTS",
                turn_id=effect.turn_id,
                generation=effect.generation,
                trace_id=trace_id,
                duration_ms=(self._clock() - started) * 1000,
                chunk_count=chunk_count,
            )
            await self._publish_event(
                TtsCompleted(
                    session_id=self.session_id,
                    turn_id=effect.turn_id,
                    generation=effect.generation,
                )
            )
        except asyncio.CancelledError:
            if not interrupt_signal.is_set():
                raise
            self._observe(
                "tts_cancelled",
                stage="TTS",
                turn_id=effect.turn_id,
                generation=effect.generation,
                trace_id=trace_id,
                mode="dialogue_stream",
            )
            await self._publish_event(
                TtsCompleted(self.session_id, effect.turn_id, effect.generation)
            )
        except AdmissionOverloaded as error:
            self._observe(
                "tts_failed",
                level=logging.WARNING,
                stage="TTS",
                turn_id=effect.turn_id,
                generation=effect.generation,
                trace_id=trace_id,
                error_code="SERVICE_OVERLOADED",
                error_type=type(error).__name__,
            )
            if self._metrics is not None:
                self._metrics.record_error("tts", "SERVICE_OVERLOADED")
            await self._publish_event(
                TtsFailed(
                    session_id=self.session_id,
                    turn_id=effect.turn_id,
                    generation=effect.generation,
                    code="SERVICE_OVERLOADED",
                    message=str(error),
                )
            )
        except TtsStreamError as error:
            reason = _known_failure_reason(error)
            self._observe(
                "tts_failed",
                level=(
                    logging.WARNING
                    if reason is not None and "timeout" in reason.lower()
                    else logging.ERROR
                ),
                stage="TTS",
                turn_id=effect.turn_id,
                generation=effect.generation,
                trace_id=trace_id,
                error_code="TTS_STREAM_FAILED",
                downstream_error_code=reason,
                error_type=type(error).__name__,
                reason=reason,
            )
            if self._metrics is not None:
                self._metrics.record_error("tts", "TTS_STREAM_FAILED")
            await self._publish_event(
                TtsFailed(
                    session_id=self.session_id,
                    turn_id=effect.turn_id,
                    generation=effect.generation,
                    code="TTS_STREAM_FAILED",
                    message=_error_message(error, "TTS stream failed"),
                )
            )
        except Exception as error:  # noqa: BLE001 - background failures become Actor events
            self._observe(
                "tts_failed",
                level=logging.ERROR,
                exc_info=error,
                stage="TTS",
                turn_id=effect.turn_id,
                generation=effect.generation,
                trace_id=trace_id,
                error_code="TTS_STREAM_FAILED",
                error_type=type(error).__name__,
                reason=_known_failure_reason(error),
            )
            if self._metrics is not None:
                self._metrics.record_error("tts", "TTS_STREAM_FAILED")
            await self._publish_event(
                TtsFailed(
                    session_id=self.session_id,
                    turn_id=effect.turn_id,
                    generation=effect.generation,
                    code="TTS_STREAM_FAILED",
                    message=_error_message(error, "TTS stream failed"),
                )
            )
        finally:
            if self._tts_interrupt_signals.get(key) is interrupt_signal:
                self._tts_interrupt_signals.pop(key, None)

    async def _run_bistream_tts(
        self,
        effect: StartBistreamTts,
        inputs: asyncio.Queue[TtsInput],
        interrupt_signal: asyncio.Event,
    ) -> None:
        """将 Actor 指令队列桥接为 PromptTTSD 的全双工 WebSocket 会话。"""
        key = (effect.turn_id, effect.generation)
        first_audio = True
        started = self._clock()
        last_chunk_at: float | None = None
        chunk_count = 0
        trace_id = self._turn_trace_id(effect.turn_id)
        resampler = StreamingResampler(TTS_SAMPLE_RATE, self.sample_rate)

        async def input_stream() -> AsyncIterator[TtsInput]:
            while True:
                item = await inputs.get()
                yield item
                if isinstance(item, TtsEndTurn):
                    return

        request = TtsRequest(model_reply="", prompt=self._tts_prompt_override or "平和", trace_id=trace_id)
        self._observe(
            "tts_started",
            stage="TTS",
            turn_id=effect.turn_id,
            generation=effect.generation,
            trace_id=trace_id,
            mode="bistream",
        )
        stream = self._tts_client.stream_bistream(request, input_stream())
        try:
            async for chunk in stream:
                received_at = self._clock()
                chunk_count += 1
                if last_chunk_at is not None:
                    gap = received_at - last_chunk_at
                    if gap >= self._slow_stage_warning_seconds:
                        self._observe(
                            "tts_chunk_gap_slow",
                            level=logging.WARNING,
                            stage="TTS",
                            turn_id=effect.turn_id,
                            generation=effect.generation,
                            trace_id=trace_id,
                            sequence=chunk.chunk_index,
                            gap_ms=gap * 1000,
                        )
                last_chunk_at = received_at
                if self._tts_output_suppressed(effect):
                    self._observe_discarded_tts_audio(
                        effect.turn_id,
                        effect.generation,
                        trace_id,
                        len(chunk.pcm16_24k),
                        source="tts_receive",
                    )
                    continue
                output = resampler.process_pcm16(chunk.pcm16_24k, final=chunk.finalize)
                if not output:
                    continue
                if first_audio:
                    first_audio = False
                    elapsed = self._clock() - started
                    self._observe(
                        "tts_first_audio",
                        stage="TTS",
                        turn_id=effect.turn_id,
                        generation=effect.generation,
                        trace_id=trace_id,
                        duration_ms=elapsed * 1000,
                        mode="bistream",
                    )
                    if self._metrics is not None:
                        self._metrics.observe_stage_latency("tts", elapsed)
                        self._observe_speech_end_to_first_tts(effect.turn_id)
                await self._publish_event(
                    TtsChunkReceived(
                        session_id=self.session_id,
                        turn_id=effect.turn_id,
                        generation=effect.generation,
                        sequence=chunk.chunk_index,
                        pcm16=output,
                        finalize=chunk.finalize,
                    )
                )
            self._observe(
                "tts_completed",
                stage="TTS",
                turn_id=effect.turn_id,
                generation=effect.generation,
                trace_id=trace_id,
                duration_ms=(self._clock() - started) * 1000,
                chunk_count=chunk_count,
                mode="bistream",
            )
            await self._publish_event(
                TtsCompleted(self.session_id, effect.turn_id, effect.generation)
            )
        except TtsStreamCancelled as cancelled:
            self._observe_bistream_cancelled(effect, trace_id, cancelled)
            await self._publish_event(
                TtsCompleted(self.session_id, effect.turn_id, effect.generation)
            )
        except asyncio.CancelledError:
            if not interrupt_signal.is_set():
                raise
            cancel_bistream = getattr(self._tts_client, "cancel_bistream", None)
            if cancel_bistream is None:
                cancelled = TtsStreamCancelled(
                    cancel_sent=False,
                    connection_aborted=False,
                    cancel_error="cancel_bistream_unavailable",
                )
            else:
                cancelled = await asyncio.shield(cancel_bistream(trace_id))
            aclose = getattr(stream, "aclose", None)
            if aclose is not None:
                await asyncio.shield(aclose())
            self._observe_bistream_cancelled(effect, trace_id, cancelled)
            await self._publish_event(
                TtsCompleted(self.session_id, effect.turn_id, effect.generation)
            )
        except AdmissionOverloaded as error:
            if self._metrics is not None:
                self._metrics.record_error("tts", "SERVICE_OVERLOADED")
            await self._publish_event(
                TtsFailed(self.session_id, effect.turn_id, effect.generation, "SERVICE_OVERLOADED", str(error))
            )
        except TtsStreamError as error:
            self._observe(
                "tts_failed",
                level=(
                    logging.WARNING if "TIMEOUT" in error.code else logging.ERROR
                ),
                stage="TTS",
                turn_id=effect.turn_id,
                generation=effect.generation,
                trace_id=trace_id,
                error_code=error.code,
                error_type=type(error).__name__,
                mode="bistream",
            )
            if self._metrics is not None:
                self._metrics.record_error("tts", error.code)
            await self._publish_event(
                TtsFailed(
                    self.session_id, effect.turn_id, effect.generation,
                    error.code, _error_message(error, "TTS stream failed"),
                )
            )
        except Exception as error:  # noqa: BLE001
            self._observe(
                "tts_failed", level=logging.ERROR, exc_info=error, stage="TTS",
                turn_id=effect.turn_id, generation=effect.generation, trace_id=trace_id,
                error_code="TTS_STREAM_FAILED", error_type=type(error).__name__, mode="bistream",
            )
            await self._publish_event(
                TtsFailed(
                    self.session_id, effect.turn_id, effect.generation,
                    "TTS_STREAM_FAILED", _error_message(error, "TTS stream failed"),
                )
            )
        finally:
            self._bistream_inputs.pop(key, None)
            if self._tts_interrupt_signals.get(key) is interrupt_signal:
                self._tts_interrupt_signals.pop(key, None)

    def _observe_bistream_cancelled(
        self,
        effect: StartBistreamTts,
        trace_id: str,
        cancelled: TtsStreamCancelled,
    ) -> None:
        self._observe(
            "tts_cancel_sent" if cancelled.cancel_sent else "tts_cancel_send_failed",
            level=logging.INFO if cancelled.cancel_sent else logging.WARNING,
            stage="TTS",
            turn_id=effect.turn_id,
            generation=effect.generation,
            trace_id=trace_id,
            mode="bistream",
            error_type=cancelled.cancel_error,
        )
        if cancelled.connection_aborted:
            self._observe(
                "tts_connection_aborted",
                stage="TTS",
                turn_id=effect.turn_id,
                generation=effect.generation,
                trace_id=trace_id,
                mode="bistream",
            )

    def _signal_tts_interruption(self, turn_id: int) -> None:
        for key, signal in tuple(self._tts_interrupt_signals.items()):
            active_turn_id, generation = key
            if active_turn_id != turn_id:
                continue
            newly_requested = not signal.is_set()
            if newly_requested:
                self._observe(
                    "tts_cancel_requested",
                    stage="TTS",
                    turn_id=active_turn_id,
                    generation=generation,
                    trace_id=self._turn_trace_id(active_turn_id),
                    mode=("bistream" if key in self._bistream_inputs else "dialogue_stream"),
                )
                signal.set()
            task = self._tts_tasks_by_key.get(key)
            if newly_requested and task is not None and not task.done():
                task.cancel()

    def _discard_queued_turn_audio(self, turn_id: int) -> None:
        """清除尚未被 Sender 取出的旧轮音频，保留其他协议消息。"""
        turn = self.actor.state.turns.get(turn_id)
        generation = turn.tts_generation if turn is not None else 0
        discarded_chunks, discarded_bytes = self.outbound.discard(
            lambda message: isinstance(message, AudioDelta)
            and message.turn_id == turn_id
        )
        if not discarded_chunks:
            return
        self._observe(
            "tts_queued_audio_discarded",
            stage="TTS",
            turn_id=turn_id,
            generation=generation,
            trace_id=self._turn_trace_id(turn_id),
            discarded_chunks=discarded_chunks,
            discarded_bytes=discarded_bytes,
        )

    def _observe_discarded_tts_audio(
        self,
        turn_id: int,
        generation: int,
        trace_id: str,
        byte_count: int,
        *,
        source: str,
    ) -> None:
        self._observe(
            "tts_audio_discarded_after_interrupt",
            stage="TTS",
            turn_id=turn_id,
            generation=generation,
            trace_id=trace_id,
            discarded_chunks=1,
            discarded_bytes=byte_count,
            source=source,
        )
        if self._metrics is not None:
            self._metrics.record_discarded_tts_chunk(byte_count=byte_count)

    def _tts_output_suppressed(self, effect: StartTts | StartBistreamTts) -> bool:
        if self._closing:
            return True
        turn = self.actor.state.turns.get(effect.turn_id)
        # 抑制条件：轮次不存在 / 已被新 TTS 取代 / 已被显式打断
        return turn is None or turn.tts_generation != effect.generation or turn.interrupted

    def _observe_speech_end_to_first_tts(self, turn_id: int) -> None:
        """记录原始首音时延及扣除 VAD 实际尾部静音后的展示口径。"""
        if self._metrics is None:
            return
        speech_end = self._turn_speech_ends.pop(turn_id, None)
        if speech_end is None:
            return
        total_seconds = max(0.0, self._clock() - speech_end)
        turn = self.actor.state.turns.get(turn_id)
        silence_seconds = (
            max(0.0, turn.trailing_silence_ms / 1000)
            if turn is not None
            else 0.0
        )
        self._metrics.observe_speech_end_to_first_tts(total_seconds)
        self._metrics.observe_speech_end_to_first_tts_excluding_silence(
            max(0.0, total_seconds - silence_seconds)
        )

    def _put_bistream_input(self, turn_id: int, generation: int, item: TtsInput) -> None:
        """非阻塞投递，防止慢 TTS 发送端反向卡住整个 Actor。"""
        queue = self._bistream_inputs.get((turn_id, generation))
        if queue is None:
            return
        try:
            queue.put_nowait(item)
        except asyncio.QueueFull:
            # 关闭输入，让会话尽快释放；后续 TTS 失败会被 Actor 统一收尾。
            self._signal_tts_interruption(turn_id)

    def _observe(
        self,
        event: str,
        *,
        level: int = logging.INFO,
        exc_info: BaseException | bool | None = None,
        **fields: object,
    ) -> None:
        """尽力输出生命周期诊断信息，不影响会话行为。"""
        try:
            log_event(
                event,
                logger=self._logger,
                level=level,
                exc_info=exc_info,
                device_id=self.device_id,
                user_id=self.user_id,
                session_id=self.session_id,
                **fields,
            )
            if self._metrics is not None:
                self._metrics.record_lifecycle_event(event)
        except Exception:  # noqa: BLE001 - observability is best effort
            return

    def _observe_slow(self, event: str, elapsed: float, **fields: object) -> None:
        if elapsed >= self._slow_stage_warning_seconds:
            self._observe(
                event,
                level=logging.WARNING,
                duration_ms=elapsed * 1000,
                **fields,
            )

    def _turn_trace_id(self, turn_id: int) -> str:
        return f"{self.device_id}/{self.session_id}/turn-{turn_id}"

    def _segment_trace_id(self, segment_id: int) -> str:
        return f"{self.device_id}/{self.session_id}/segment-{segment_id}"

    async def _publish_event(self, event: SessionEvent) -> None:
        if not self._closing:
            await self.events.put(event)

    async def _finish_cleanup(self) -> None:
        if self._cleaned:
            return
        if self._cleanup_task is None:
            self._cleanup_task = asyncio.create_task(
                self._cleanup_once(), name=f"{self.session_id}:cleanup"
            )
        try:
            await asyncio.shield(self._cleanup_task)
        except asyncio.CancelledError:
            await self._cleanup_task
            raise

    async def _cleanup_once(self) -> None:
        async with self._cleanup_lock:
            if self._cleaned:
                return
            self._closing = True
            for task in tuple(self._keyword_tasks):
                task.cancel()
            await asyncio.gather(*tuple(self._keyword_tasks), return_exceptions=True)
            if self.turn_end is not None:
                await self.turn_end.aclose()
            try:
                # 清理顺序：先停音频采集/发送，再取消 ASR；随后 Thinker 与 TTS
                # 并发取消。不能等待 Thinker 自然收尾，否则已断开的客户端会长期
                # 占用下游 LLM/TTS 资源。
                await self._stop_audio()
                await self._cancel_asr()
                thinker_safe, _ = await asyncio.gather(
                    self._cancel_thinker(),
                    self._cancel_tts(),
                )
                if thinker_safe:
                    await self._delete_thinker_session()
                else:
                    self._observe(
                        "thinker_cleanup_skipped",
                        level=logging.WARNING,
                        stage="LLM",
                        error_code="THINKER_CLEANUP_SKIPPED",
                    )
            finally:
                # 无论前面是否成功，都要释放注册表准入并标记已清理，避免泄漏
                if self._registry is not None:
                    await self._registry.remove(self.session_id)
                self._speech_ends.clear()
                self._turn_speech_ends.clear()
                self._llm_milestones.clear()
                if self._speaker_gate is not None:
                    self._speaker_gate.clear()
                self._cleaned = True
                self._observe("session_cleanup", stage="SESSION")
                self._observe("session_closed", stage="SESSION")

    async def _stop_audio(self) -> None:
        await self._cancel_named_tasks("receiver", "vad", "sender")
        while not self.audio_queue.empty():
            self.audio_queue.get_nowait()

    async def _cancel_asr(self) -> None:
        await self._cancel_named_tasks("asr")
        while not self._asr_queue.empty():
            self._asr_queue.get_nowait()

    async def _cancel_named_tasks(self, *names: str) -> None:
        current = asyncio.current_task()
        tasks = [
            task
            for name in names
            if (task := self._long_tasks.get(name)) is not None
            and task is not current
            and not task.done()
        ]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _cancel_thinker(self) -> bool:
        """立即停止本地 Thinker 流，并尽力通知下游同一会话已中断。"""
        active = {task for task in self._thinker_tasks if not task.done()}
        if active:
            # interrupt 不等待网络响应：客户端已断连，本地任务和 TTS 都必须立即释放。
            # 远端收到该请求后会停止同一 session 的生成；若请求失败，日志记录但不妨碍清理。
            self._notify_thinker_interrupt()
            self._thinker_cleanup_safe = False
            await _cancel_tasks(active)
        return self._thinker_cleanup_safe

    async def _cancel_tts(self) -> None:
        for turn_id, _ in tuple(self._tts_interrupt_signals):
            self._signal_tts_interruption(turn_id)
        active = {task for task in self._tts_tasks if not task.done()}
        if active:
            await asyncio.gather(*active, return_exceptions=True)

    async def _delete_thinker_session(self) -> None:
        try:
            await self._thinker_client.delete_session(self.user_id, self.session_id)
        except Exception as error:  # noqa: BLE001 - cleanup must still release registry
            self._observe(
                "thinker_cleanup_failed",
                level=logging.WARNING,
                stage="LLM",
                error_code="THINKER_CLEANUP_FAILED",
                error_type=type(error).__name__,
            )


async def _cancel_tasks(tasks: set[asyncio.Task[None]]) -> None:
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def _error_message(error: Exception, fallback: str) -> str:
    message = str(error).strip()
    return message or fallback


def _known_failure_reason(error: Exception) -> str | None:
    """Return only explicitly approved machine reasons, never arbitrary downstream text."""
    code = getattr(error, "code", None)
    reason = code.strip() if isinstance(code, str) else str(error).strip()
    return reason if reason in KNOWN_FAILURE_REASONS else None


def _audio_item_size(item: bytes | None) -> int:
    return 0 if item is None else len(item)


def _outbound_item_size(message: ServerMessage) -> int:
    return len(message.model_dump_json(exclude_none=True).encode())


def _validate_injected_queue(
    queue: BoundedByteQueue[object],
    *,
    queue_name: str,
    maxsize: int,
    max_bytes: int,
    item_size: Callable[[object], int],
) -> None:
    if queue.maxsize > maxsize or queue.max_bytes > max_bytes:
        raise ValueError(f"injected {queue_name} queue weakens required limits")
    if queue._item_size is not item_size:
        raise ValueError(f"injected {queue_name} queue must use required queue sizing")
