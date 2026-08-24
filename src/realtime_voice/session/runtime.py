"""Supervised lifecycle and effect execution for one realtime session."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from math import isfinite
from time import monotonic
from typing import TYPE_CHECKING, Generic, Protocol, TypeVar

from realtime_voice.audio.pcm import pcm16_wav_bytes
from realtime_voice.audio.resampler import StreamingResampler
from realtime_voice.observability.logging import log_event
from realtime_voice.observability.metrics import Metrics

if TYPE_CHECKING:
    from realtime_voice.audio.vad import SpeechSegment
from realtime_voice.clients.berry import (
    BerryClient,
    BerryDone,
    BerryReplyRequest,
    BerryTextDelta,
)
from realtime_voice.clients.limits import AdmissionOverloaded
from realtime_voice.clients.tts import TTS_SAMPLE_RATE, TtsClient, TtsRequest
from realtime_voice.protocol.server_messages import ServerMessage, TurnState
from realtime_voice.session.actor import (
    CloseRuntime,
    QueueAsr,
    RecordDiscardedAudio,
    RecordStaleEvent,
    SendOutbound,
    SessionActor,
    SessionEffect,
    StartBerry,
    StartNextBerry,
    StartTts,
)
from realtime_voice.session.events import (
    AsrFailed,
    AsrSucceeded,
    BerryCompleted,
    BerryDeltaReceived,
    BerryFailed,
    SessionEvent,
    SpeechSegmentReady,
    TtsChunkReceived,
    TtsCompleted,
    TtsFailed,
)
from realtime_voice.session.registry import SessionRegistry
from realtime_voice.session.state import SessionState

BERRY_CLEANUP_SKIPPED = "BERRY_CLEANUP_SKIPPED"
DEFAULT_AUDIO_QUEUE_MAX_SECONDS = 3.0
DEFAULT_OUTBOUND_QUEUE_MAX_BYTES = 8 * 1024 * 1024

QueueItem = TypeVar("QueueItem")


@dataclass(frozen=True, slots=True)
class QueueSnapshot:
    items: int
    bytes: int | None = None


class SessionQueueOverloaded(RuntimeError):
    """Raised when an item would exceed a session queue's byte budget."""

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
    """Raised when the bounded outbound queue cannot admit another message."""

    code = "SLOW_CLIENT"


class BoundedByteQueue(asyncio.Queue[QueueItem], Generic[QueueItem]):
    """An item-bounded queue that also rejects byte-budget overflow immediately."""

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
        """Create an audio queue with the runtime's required PCM byte sizing."""
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
        """Create an outbound queue with the runtime's JSON byte sizing."""
        return cls(
            name="outbound",
            maxsize=maxsize,
            max_bytes=max_bytes,
            item_size=_outbound_item_size,
            metrics=metrics,
        )

    @property
    def queued_bytes(self) -> int:
        """Return the number of payload bytes currently admitted."""
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


class AsyncWorker(Protocol):
    """One long-lived worker supervised by the session TaskGroup."""

    async def run(self) -> None: ...


class AsrClientProtocol(Protocol):
    async def transcribe(self, pcm16_16k: bytes) -> str: ...


class RegistryProtocol(Protocol):
    async def remove(self, key: str) -> None: ...


class SessionStop(Exception):
    """Internal sentinel used to stop all long-lived workers together."""


class SessionRuntime:
    """Own all asynchronous work and queues belonging to one session."""

    def __init__(
        self,
        *,
        state: SessionState,
        asr_client: AsrClientProtocol,
        berry_client: BerryClient,
        tts_client: TtsClient,
        receiver: AsyncWorker,
        vad_worker: AsyncWorker,
        sender: AsyncWorker,
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
        berry_cleanup_timeout: float = 120.0,
        metrics: Metrics | None = None,
        tts_drain_timeout: float = 120.0,
        logger: logging.Logger | None = None,
        clock: Callable[[], float] = monotonic,
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
        if berry_cleanup_timeout <= 0 or tts_drain_timeout <= 0:
            raise ValueError("session cleanup timeouts must be positive")

        self.actor = SessionActor(state)
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

        self._asr_client = asr_client
        self._berry_client = berry_client
        self._tts_client = tts_client
        self._receiver = receiver
        self._vad_worker = vad_worker
        self._sender = sender
        self._registry = registry
        self._berry_cleanup_timeout = berry_cleanup_timeout
        self._tts_drain_timeout = tts_drain_timeout
        self._logger = logger or logging.getLogger(__name__)
        self._metrics = metrics
        self._clock = clock

        self._close_requested = asyncio.Event()
        self._long_tasks: dict[str, asyncio.Task[None]] = {}
        self._background_tasks: set[asyncio.Task[None]] = set()
        self._berry_tasks: set[asyncio.Task[None]] = set()
        self._speech_ends: dict[int, float] = {}
        self._turn_speech_ends: dict[int, float] = {}
        self._llm_milestones: set[int] = set()
        self._tts_tasks: set[asyncio.Task[None]] = set()
        self._tts_interrupt_signals: dict[tuple[int, int], asyncio.Event] = {}
        self._asr_berry_lock = asyncio.Lock()
        self._berry_cleanup_safe = True
        self._closing = False
        self._is_running = False
        self._cleanup_lock = asyncio.Lock()
        self._cleanup_task: asyncio.Task[None] | None = None
        self._cleaned = False

    @property
    def session_id(self) -> str:
        return self.actor.state.session_id

    @property
    def user_id(self) -> str:
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
        """Return local queue usage without waiting or mutating runtime state."""
        return {
            "event": QueueSnapshot(self.events.qsize()),
            "audio": QueueSnapshot(self.audio_queue.qsize(), self.audio_queue.queued_bytes),
            "asr": QueueSnapshot(self._asr_queue.qsize()),
            "outbound": QueueSnapshot(self.outbound.qsize(), self.outbound.queued_bytes),
        }

    def bind_registry(self, registry: SessionRegistry) -> None:
        """Bind the creating registry so cleanup always releases its admission."""
        if self._registry is not None and self._registry is not registry:
            raise RuntimeError("session runtime is already bound to another registry")
        self._registry = registry

    def request_close(self) -> None:
        """Request orderly cleanup and TaskGroup shutdown."""
        self._close_requested.set()

    async def run(self) -> None:
        """Run Receiver, VAD, ASR, Actor, and Sender in one TaskGroup."""
        if self._is_running:
            raise RuntimeError("session runtime is already running")
        if self._cleaned:
            raise RuntimeError("session runtime cannot be restarted after cleanup")
        self._is_running = True

        try:
            try:
                async with asyncio.TaskGroup() as group:
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
                    await self._finish_cleanup()
                    raise SessionStop
            except* SessionStop:
                pass
        finally:
            try:
                await self._finish_cleanup()
            finally:
                self._is_running = False
                self._long_tasks.clear()

    async def execute_effect(self, effect: SessionEffect) -> None:
        """Execute one Actor command without allowing workers to mutate Actor state."""
        if isinstance(effect, SendOutbound):
            if isinstance(effect.message, TurnState):
                self._signal_tts_interruption(effect.message.turn_id)
                self._observe("turn_interrupted", turn_id=effect.message.turn_id, interrupt=True)
                if self._metrics is not None:
                    self._metrics.record_interruption()
            if not self._closing:
                try:
                    self.outbound.put_nowait(effect.message)
                except (asyncio.QueueFull, SessionQueueOverloaded) as error:
                    self.request_close()
                    if self._metrics is not None:
                        self._metrics.record_slow_client_close()
                    raise SlowClient(str(error)) from error
            return
        if isinstance(effect, QueueAsr):
            if not self._closing and effect.session_id == self.session_id:
                await self._asr_queue.put(effect.segment)
            return
        if isinstance(effect, (StartBerry, StartNextBerry)):
            if not self._closing:
                self._spawn_background(
                    self._run_berry(effect),
                    tasks=self._berry_tasks,
                    name=f"{self.session_id}:berry:{effect.turn_id}:{effect.generation}",
                    marks_berry=True,
                )
            return
        if isinstance(effect, StartTts):
            if not self._closing:
                key = (effect.turn_id, effect.generation)
                interrupt_signal = asyncio.Event()
                self._tts_interrupt_signals[key] = interrupt_signal
                if self._tts_output_suppressed(effect):
                    interrupt_signal.set()
                self._spawn_background(
                    self._run_tts(effect, interrupt_signal),
                    tasks=self._tts_tasks,
                    name=f"{self.session_id}:tts:{effect.turn_id}:{effect.generation}",
                )
            return
        if isinstance(effect, CloseRuntime):
            if effect.session_id == self.session_id:
                self.request_close()
            return
        if isinstance(effect, RecordStaleEvent):
            self._logger.info(
                "STALE_SESSION_EVENT",
                extra={
                    "session_id": self.session_id,
                    "turn_id": effect.turn_id,
                    "event_type": effect.event_type,
                    "reason": effect.reason,
                },
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
        raise TypeError(f"unsupported session effect: {type(effect).__name__}")

    async def _receiver_loop(self) -> None:
        await self._worker_loop(self._receiver)

    async def _worker_loop(self, worker: AsyncWorker) -> None:
        await worker.run()
        self.request_close()

    async def _actor_loop(self) -> None:
        while True:
            event = await self.events.get()
            if isinstance(event, SpeechSegmentReady):
                self._speech_ends.setdefault(
                    event.segment.segment_id,
                    event.speech_end_at if event.speech_end_at is not None else self._clock(),
                )
                self._observe("vad_segment_ready", segment_id=event.segment.segment_id)
            effects = self.actor.handle(event)
            if isinstance(event, AsrSucceeded):
                speech_end = self._speech_ends.pop(event.segment_id, None)
                if speech_end is not None and self._metrics is not None:
                    self._metrics.observe_speech_end_to_asr(self._clock() - speech_end)
                if speech_end is not None:
                    for effect in effects:
                        if isinstance(effect, (StartBerry, StartNextBerry)):
                            self._turn_speech_ends[effect.turn_id] = speech_end
            for effect in effects:
                await self.execute_effect(effect)

    async def _asr_loop(self) -> None:
        while True:
            segment = await self._asr_queue.get()
            started = self._clock()
            try:
                async with self._asr_berry_lock:
                    text = await self._asr_client.transcribe(segment.pcm16_16k)
                event: SessionEvent = AsrSucceeded(
                    session_id=self.session_id,
                    segment_id=segment.segment_id,
                    text=text,
                    audio_wav=pcm16_wav_bytes(segment.pcm16_16k, 16000),
                )
            except AdmissionOverloaded as error:
                event = AsrFailed(
                    session_id=self.session_id,
                    segment_id=segment.segment_id,
                    code="SERVICE_OVERLOADED",
                    message=str(error),
                )
            except Exception as error:  # noqa: BLE001 - worker failures become Actor events
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
                    "asr_completed", segment_id=segment.segment_id, duration_ms=elapsed * 1000
                )
            if not self._closing:
                await self.events.put(event)

    def _spawn_background(
        self,
        coroutine: Coroutine[object, object, None],
        *,
        tasks: set[asyncio.Task[None]],
        name: str,
        marks_berry: bool = False,
    ) -> None:
        task = asyncio.create_task(coroutine, name=name)
        tasks.add(task)
        self._background_tasks.add(task)

        def finished(done: asyncio.Task[None]) -> None:
            tasks.discard(done)
            self._background_tasks.discard(done)
            if marks_berry and done.cancelled():
                self._berry_cleanup_safe = False

        task.add_done_callback(finished)

    async def _run_berry(self, effect: StartBerry | StartNextBerry) -> None:
        try:
            async with self._asr_berry_lock:
                if self._closing:
                    return
                if isinstance(effect, StartNextBerry) and effect.interrupt_first:
                    await self._berry_client.interrupt(self.user_id, self.session_id)
                request = BerryReplyRequest(
                    user_id=self.user_id,
                    session_id=self.session_id,
                    text=effect.text,
                    audio_wav=effect.audio_wav,
                )
                reply_text: str | None = None
                first_delta = True
                started = self._clock()
                async for item in self._berry_client.stream_reply(request):
                    if isinstance(item, BerryTextDelta):
                        if first_delta:
                            first_delta = False
                            elapsed = self._clock() - started
                            self._observe(
                                "berry_first_delta",
                                turn_id=effect.turn_id,
                                duration_ms=elapsed * 1000,
                            )
                            if self._metrics is not None:
                                self._metrics.observe_stage_latency("berry", elapsed)
                                speech_end = self._turn_speech_ends.get(effect.turn_id)
                                if speech_end is not None and effect.turn_id not in self._llm_milestones:
                                    self._llm_milestones.add(effect.turn_id)
                                    self._metrics.observe_speech_end_to_first_llm(
                                        self._clock() - speech_end
                                    )
                        await self._publish_event(
                            BerryDeltaReceived(
                                session_id=self.session_id,
                                turn_id=effect.turn_id,
                                generation=effect.generation,
                                delta=item.delta,
                            )
                        )
                    elif isinstance(item, BerryDone):
                        reply_text = item.reply_text
                if reply_text is None:
                    raise RuntimeError("Berry stream ended without done")
                await self._publish_event(
                    BerryCompleted(
                        session_id=self.session_id,
                        turn_id=effect.turn_id,
                        generation=effect.generation,
                        reply_text=reply_text,
                    )
                )
        except AdmissionOverloaded as error:
            if self._metrics is not None:
                self._metrics.record_error("berry", "SERVICE_OVERLOADED")
            await self._publish_event(
                BerryFailed(
                    session_id=self.session_id,
                    turn_id=effect.turn_id,
                    generation=effect.generation,
                    code="SERVICE_OVERLOADED",
                    message=str(error),
                )
            )
        except Exception as error:  # noqa: BLE001 - background failures become Actor events
            if self._metrics is not None:
                self._metrics.record_error("berry", "BERRY_STREAM_FAILED")
            await self._publish_event(
                BerryFailed(
                    session_id=self.session_id,
                    turn_id=effect.turn_id,
                    generation=effect.generation,
                    code="BERRY_STREAM_FAILED",
                    message=_error_message(error, "Berry reply stream failed"),
                )
            )

    async def _run_tts(
        self,
        effect: StartTts,
        interrupt_signal: asyncio.Event,
    ) -> None:
        key = (effect.turn_id, effect.generation)
        first_audio = True
        started = self._clock()
        resampler = StreamingResampler(TTS_SAMPLE_RATE, self.sample_rate)
        request = TtsRequest(
            user_input=effect.user_input,
            model_reply=effect.reply_text,
            trace_id=f"{self.user_id}/{self.session_id}/turn-{effect.turn_id}",
        )
        try:
            try:
                async with asyncio.timeout(None) as drain_timeout:
                    deadline_task = asyncio.create_task(
                        self._arm_tts_drain_timeout(interrupt_signal, drain_timeout),
                        name=(
                            f"{self.session_id}:tts-drain-deadline:"
                            f"{effect.turn_id}:{effect.generation}"
                        ),
                    )
                    try:
                        async for chunk in self._tts_client.stream(request):
                            if self._tts_output_suppressed(effect):
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
                                        turn_id=effect.turn_id,
                                        duration_ms=elapsed * 1000,
                                    )
                                    if self._metrics is not None:
                                        self._metrics.observe_stage_latency("tts", elapsed)
                                        speech_end = self._turn_speech_ends.pop(
                                            effect.turn_id, None
                                        )
                                        if speech_end is not None:
                                            self._metrics.observe_speech_end_to_first_tts(
                                                self._clock() - speech_end
                                            )
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
                    finally:
                        deadline_task.cancel()
                        await asyncio.gather(deadline_task, return_exceptions=True)
            except TimeoutError:
                pass
            await self._publish_event(
                TtsCompleted(
                    session_id=self.session_id,
                    turn_id=effect.turn_id,
                    generation=effect.generation,
                )
            )
        except AdmissionOverloaded as error:
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
        except Exception as error:  # noqa: BLE001 - background failures become Actor events
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

    async def _arm_tts_drain_timeout(
        self,
        interrupt_signal: asyncio.Event,
        drain_timeout: asyncio.Timeout,
    ) -> None:
        await interrupt_signal.wait()
        drain_timeout.reschedule(asyncio.get_running_loop().time() + self._tts_drain_timeout)

    def _signal_tts_interruption(self, turn_id: int) -> None:
        for (active_turn_id, _), signal in self._tts_interrupt_signals.items():
            if active_turn_id == turn_id:
                signal.set()

    def _tts_output_suppressed(self, effect: StartTts) -> bool:
        if self._closing:
            return True
        turn = self.actor.state.turns.get(effect.turn_id)
        return turn is None or turn.tts_generation != effect.generation or turn.interrupted

    def _observe(self, event: str, **fields: object) -> None:
        """Emit best-effort lifecycle diagnostics without changing session behavior."""
        try:
            log_event(event, user_id=self.user_id, session_id=self.session_id, **fields)
            if self._metrics is not None:
                self._metrics.record_lifecycle_event(event)
        except Exception:  # noqa: BLE001 - observability is best effort
            return

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
            try:
                await self._stop_audio()
                await self._cancel_asr()
                berry_safe = await self._wait_berry()
                await self._drain_tts()
                if berry_safe:
                    await self._delete_berry_session()
                else:
                    self._logger.warning(
                        BERRY_CLEANUP_SKIPPED,
                        extra={
                            "event": BERRY_CLEANUP_SKIPPED,
                            "user_id": self.user_id,
                            "session_id": self.session_id,
                        },
                    )
            finally:
                if self._registry is not None:
                    await self._registry.remove(self.session_id)
                self._cleaned = True
                self._observe("session_cleanup")

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

    async def _wait_berry(self) -> bool:
        pending = await _wait_tasks(self._berry_tasks, self._berry_cleanup_timeout)
        if pending:
            self._berry_cleanup_safe = False
            await _cancel_tasks(pending)
        return self._berry_cleanup_safe

    async def _drain_tts(self) -> None:
        pending = await _wait_tasks(self._tts_tasks, self._tts_drain_timeout)
        if pending:
            await _cancel_tasks(pending)

    async def _delete_berry_session(self) -> None:
        try:
            await self._berry_client.delete_session(self.user_id, self.session_id)
        except Exception as error:  # noqa: BLE001 - cleanup must still release registry
            self._logger.warning(
                "BERRY_CLEANUP_FAILED",
                extra={
                    "event": "BERRY_CLEANUP_FAILED",
                    "user_id": self.user_id,
                    "session_id": self.session_id,
                    "error_type": type(error).__name__,
                },
            )


async def _wait_tasks(tasks: set[asyncio.Task[None]], timeout: float) -> set[asyncio.Task[None]]:
    active = {task for task in tasks if not task.done()}
    if not active:
        return set()
    _, pending = await asyncio.wait(active, timeout=timeout)
    return pending


async def _cancel_tasks(tasks: set[asyncio.Task[None]]) -> None:
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def _error_message(error: Exception, fallback: str) -> str:
    message = str(error).strip()
    return message or fallback


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
