"""Supervised lifecycle and effect execution for one realtime session."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine
from typing import TYPE_CHECKING, Protocol

from realtime_voice.audio.pcm import pcm16_wav_bytes
from realtime_voice.audio.resampler import StreamingResampler

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
    TtsChunkReceived,
    TtsCompleted,
    TtsFailed,
)
from realtime_voice.session.registry import SessionRegistry
from realtime_voice.session.state import SessionState

BERRY_CLEANUP_SKIPPED = "BERRY_CLEANUP_SKIPPED"


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
        event_queue: asyncio.Queue[SessionEvent] | None = None,
        audio_queue: asyncio.Queue[bytes | None] | None = None,
        outbound_queue: asyncio.Queue[ServerMessage] | None = None,
        berry_cleanup_timeout: float = 120.0,
        tts_drain_timeout: float = 120.0,
        logger: logging.Logger | None = None,
    ) -> None:
        if min(event_queue_size, audio_queue_size, asr_queue_size, outbound_queue_size) < 1:
            raise ValueError("session queue sizes must be at least 1")
        injected_queues = (event_queue, audio_queue, outbound_queue)
        if any(queue is not None and queue.maxsize < 1 for queue in injected_queues):
            raise ValueError("injected session queues must be bounded")
        if berry_cleanup_timeout <= 0 or tts_drain_timeout <= 0:
            raise ValueError("session cleanup timeouts must be positive")

        self.actor = SessionActor(state)
        self.events = event_queue or asyncio.Queue(maxsize=event_queue_size)
        self.audio_queue = audio_queue or asyncio.Queue(maxsize=audio_queue_size)
        self.outbound = outbound_queue or asyncio.Queue(maxsize=outbound_queue_size)
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

        self._close_requested = asyncio.Event()
        self._long_tasks: dict[str, asyncio.Task[None]] = {}
        self._background_tasks: set[asyncio.Task[None]] = set()
        self._berry_tasks: set[asyncio.Task[None]] = set()
        self._tts_tasks: set[asyncio.Task[None]] = set()
        self._tts_interrupt_signals: dict[tuple[int, int], asyncio.Event] = {}
        self._berry_lock = asyncio.Lock()
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
                        "asr": group.create_task(
                            self._asr_loop(), name=f"{self.session_id}:asr"
                        ),
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
            if not self._closing:
                await self.outbound.put(effect.message)
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
            self._logger.info(
                "TTS_AUDIO_DISCARDED",
                extra={
                    "session_id": self.session_id,
                    "turn_id": effect.turn_id,
                    "generation": effect.generation,
                    "byte_count": effect.byte_count,
                },
            )
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
            for effect in self.actor.handle(event):
                await self.execute_effect(effect)

    async def _asr_loop(self) -> None:
        while True:
            segment = await self._asr_queue.get()
            try:
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
            async with self._berry_lock:
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
                async for item in self._berry_client.stream_reply(request):
                    if isinstance(item, BerryTextDelta):
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
        drain_timeout.reschedule(
            asyncio.get_running_loop().time() + self._tts_drain_timeout
        )

    def _signal_tts_interruption(self, turn_id: int) -> None:
        for (active_turn_id, _), signal in self._tts_interrupt_signals.items():
            if active_turn_id == turn_id:
                signal.set()

    def _tts_output_suppressed(self, effect: StartTts) -> bool:
        if self._closing:
            return True
        turn = self.actor.state.turns.get(effect.turn_id)
        return (
            turn is None
            or turn.tts_generation != effect.generation
            or turn.interrupted
        )

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


async def _wait_tasks(
    tasks: set[asyncio.Task[None]], timeout: float
) -> set[asyncio.Task[None]]:
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
