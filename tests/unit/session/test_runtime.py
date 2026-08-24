import asyncio
from collections.abc import AsyncIterator

import pytest

from realtime_voice.audio.vad import SpeechSegment
from realtime_voice.clients.berry import BerryDone, BerryReplyRequest, BerryTextDelta, DeleteResult
from realtime_voice.clients.limits import BoundedAdmission
from realtime_voice.clients.tts import TtsChunk, TtsRequest
from realtime_voice.protocol.server_messages import TextDelta, TurnState
from realtime_voice.session.actor import (
    QueueAsr,
    SendOutbound,
    StartBerry,
    StartNextBerry,
    StartTts,
)
from realtime_voice.session.events import (
    BerryCompleted,
    BerryDeltaReceived,
    SpeechSegmentReady,
    TtsChunkReceived,
    TtsCompleted,
)
from realtime_voice.session.runtime import SessionRuntime
from realtime_voice.session.state import SessionState, TurnContext, TurnStage
from tests.helpers import sine_pcm16, valid_wav


class BlockingWorker:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.stopped = asyncio.Event()
        self.task_name: str | None = None

    async def run(self) -> None:
        task = asyncio.current_task()
        self.task_name = None if task is None else task.get_name()
        self.started.set()
        try:
            await asyncio.Event().wait()
        finally:
            self.stopped.set()


class FailingWorker(BlockingWorker):
    async def run(self) -> None:
        task = asyncio.current_task()
        self.task_name = None if task is None else task.get_name()
        self.started.set()
        self.stopped.set()
        raise RuntimeError("receiver failed")



class ReturningReceiver(BlockingWorker):
    async def run(self) -> None:
        task = asyncio.current_task()
        self.task_name = None if task is None else task.get_name()
        self.started.set()
        self.stopped.set()


class EmptyAsr:
    async def transcribe(self, pcm16_16k: bytes) -> str:
        return ""


class SerialAsr:
    def __init__(self) -> None:
        self.calls: list[bytes] = []
        self.first_started = asyncio.Event()
        self.release_first = asyncio.Event()
        self.second_started = asyncio.Event()
        self.active = 0
        self.max_active = 0

    async def transcribe(self, pcm16_16k: bytes) -> str:
        self.calls.append(pcm16_16k)
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            if len(self.calls) == 1:
                self.first_started.set()
                await self.release_first.wait()
            else:
                self.second_started.set()
            return ""
        finally:
            self.active -= 1


class ImmediateBerry:
    def __init__(self) -> None:
        self.deleted = 0

    async def stream_reply(self, request: BerryReplyRequest) -> AsyncIterator[BerryDone]:
        yield BerryDone(reply_text=f"reply:{request.text}")

    async def interrupt(self, user_id: str, session_id: str) -> None:
        return None

    async def delete_session(self, user_id: str, session_id: str) -> DeleteResult:
        self.deleted += 1
        return DeleteResult.DELETED


class OrderedBerry(ImmediateBerry):
    def __init__(self) -> None:
        super().__init__()
        self.calls: list[str] = []
        self.first_started = asyncio.Event()
        self.release_first = asyncio.Event()

    async def stream_reply(
        self, request: BerryReplyRequest
    ) -> AsyncIterator[BerryTextDelta | BerryDone]:
        self.calls.append(f"start:{request.text}")
        if request.text == "first":
            self.first_started.set()
            await self.release_first.wait()
        yield BerryTextDelta(delta=request.text[0])
        yield BerryDone(reply_text=f"reply:{request.text}")

    async def interrupt(self, user_id: str, session_id: str) -> None:
        self.calls.append("interrupt")


class BlockingBerry(ImmediateBerry):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def stream_reply(self, request: BerryReplyRequest) -> AsyncIterator[BerryDone]:
        self.started.set()
        await self.release.wait()
        yield BerryDone(reply_text="reply")


class SignallingAsr:
    def __init__(self) -> None:
        self.started = asyncio.Event()

    async def transcribe(self, pcm16_16k: bytes) -> str:
        self.started.set()
        return ""


class EmptyTts:
    async def stream(self, request: TtsRequest) -> AsyncIterator[TtsChunk]:
        if False:
            yield TtsChunk(chunk_index=0, pcm16_24k=b"", finalize=True)


class ChunkedTts:
    def __init__(self, pcm16_24k: bytes) -> None:
        midpoint = len(pcm16_24k) // 2
        midpoint -= midpoint % 2
        self.chunks = (pcm16_24k[:midpoint], pcm16_24k[midpoint:])

    async def stream(self, request: TtsRequest) -> AsyncIterator[TtsChunk]:
        yield TtsChunk(chunk_index=0, pcm16_24k=self.chunks[0], finalize=False)
        await asyncio.sleep(0)
        yield TtsChunk(chunk_index=1, pcm16_24k=self.chunks[1], finalize=True)


class ConcurrentTts:
    def __init__(self, pcm16_24k: bytes) -> None:
        self.pcm16_24k = pcm16_24k
        self.started: list[str] = []
        self.consumed: list[str] = []
        self.first_started = asyncio.Event()
        self.second_completed = asyncio.Event()
        self.release_first = asyncio.Event()

    async def stream(self, request: TtsRequest) -> AsyncIterator[TtsChunk]:
        self.started.append(request.trace_id)
        if request.trace_id.endswith("turn-1"):
            self.first_started.set()
            await self.release_first.wait()
        yield TtsChunk(chunk_index=0, pcm16_24k=self.pcm16_24k, finalize=True)
        self.consumed.append(request.trace_id)
        if request.trace_id.endswith("turn-2"):
            self.second_completed.set()


class RecordingRegistry:
    def __init__(self) -> None:
        self.removed: list[str] = []

    async def remove(self, key: str) -> None:
        self.removed.append(key)


def make_runtime(
    *,
    asr: object | None = None,
    berry: object | None = None,
    tts: object | None = None,
    receiver: BlockingWorker | None = None,
    vad_worker: BlockingWorker | None = None,
    sender: BlockingWorker | None = None,
    sample_rate: int = 16000,
    registry: RecordingRegistry | None = None,
    berry_cleanup_timeout: float = 0.2,
    tts_drain_timeout: float = 0.2,
) -> tuple[SessionRuntime, tuple[BlockingWorker, BlockingWorker, BlockingWorker]]:
    resolved_receiver = receiver or BlockingWorker()
    vad = vad_worker or BlockingWorker()
    resolved_sender = sender or BlockingWorker()
    runtime = SessionRuntime(
        state=SessionState(user_id="u", session_id="s", sample_rate=sample_rate),
        asr_client=asr or EmptyAsr(),
        berry_client=berry or ImmediateBerry(),
        tts_client=tts or EmptyTts(),
        receiver=resolved_receiver,
        vad_worker=vad,
        sender=resolved_sender,
        registry=registry,
        berry_cleanup_timeout=berry_cleanup_timeout,
        tts_drain_timeout=tts_drain_timeout,
    )
    return runtime, (resolved_receiver, vad, resolved_sender)


async def next_event(runtime: SessionRuntime, event_type: type[object]) -> object:
    while True:
        event = await asyncio.wait_for(runtime.events.get(), timeout=1)
        if isinstance(event, event_type):
            return event


async def test_run_supervises_receiver_vad_asr_actor_and_sender_together() -> None:
    registry = RecordingRegistry()
    runtime, workers = make_runtime(registry=registry)

    run_task = asyncio.create_task(runtime.run())
    await asyncio.gather(*(worker.started.wait() for worker in workers))
    while not runtime.is_running:
        await asyncio.sleep(0)

    task_names = {task.get_name() for task in asyncio.all_tasks()}
    assert {
        "s:receiver",
        "s:vad",
        "s:asr",
        "s:actor",
        "s:sender",
    } <= task_names

    runtime.request_close()
    await asyncio.wait_for(run_task, timeout=1)

    assert {worker.task_name for worker in workers} == {
        "s:receiver",
        "s:vad",
        "s:sender",
    }
    assert all(worker.stopped.is_set() for worker in workers)
    assert registry.removed == ["s"]


async def test_actor_queue_asr_effect_is_consumed_serially_per_session() -> None:
    asr = SerialAsr()
    runtime, workers = make_runtime(asr=asr)
    run_task = asyncio.create_task(runtime.run())
    await asyncio.gather(*(worker.started.wait() for worker in workers))

    await runtime.events.put(
        SpeechSegmentReady(
            session_id="s",
            segment=SpeechSegment(segment_id=1, pcm16_16k=b"\x01\x00"),
        )
    )
    await runtime.events.put(
        SpeechSegmentReady(
            session_id="s",
            segment=SpeechSegment(segment_id=2, pcm16_16k=b"\x02\x00"),
        )
    )

    await asyncio.wait_for(asr.first_started.wait(), timeout=1)
    await asyncio.sleep(0)
    assert not asr.second_started.is_set()
    asr.release_first.set()
    await asyncio.wait_for(asr.second_started.wait(), timeout=1)

    runtime.request_close()
    await asyncio.wait_for(run_task, timeout=1)
    assert asr.calls == [b"\x01\x00", b"\x02\x00"]
    assert asr.max_active == 1


async def test_start_berry_runs_in_background_and_returns_session_events() -> None:
    berry = OrderedBerry()
    berry.release_first.set()
    runtime, _ = make_runtime(berry=berry)

    await runtime.execute_effect(
        StartBerry(turn_id=1, generation=1, text="first", audio_wav=valid_wav())
    )

    delta = await next_event(runtime, BerryDeltaReceived)
    completed = await next_event(runtime, BerryCompleted)
    assert delta == BerryDeltaReceived(
        session_id="s", turn_id=1, generation=1, delta="f"
    )
    assert completed == BerryCompleted(
        session_id="s", turn_id=1, generation=1, reply_text="reply:first"
    )


async def test_berry_effects_remain_fifo_and_interrupt_before_next_request() -> None:
    berry = OrderedBerry()
    runtime, _ = make_runtime(berry=berry)

    await runtime.execute_effect(
        StartBerry(turn_id=1, generation=1, text="first", audio_wav=valid_wav())
    )
    await runtime.execute_effect(
        StartNextBerry(
            turn_id=2,
            generation=1,
            text="second",
            audio_wav=valid_wav(),
            interrupt_first=True,
        )
    )

    await asyncio.wait_for(berry.first_started.wait(), timeout=1)
    await asyncio.sleep(0)
    assert berry.calls == ["start:first"]
    berry.release_first.set()
    completed = [
        await next_event(runtime, BerryCompleted),
        await next_event(runtime, BerryCompleted),
    ]

    assert berry.calls == ["start:first", "interrupt", "start:second"]
    assert [event.turn_id for event in completed] == [1, 2]


async def test_asr_waits_for_an_active_berry_stream_in_the_same_session() -> None:
    asr = SignallingAsr()
    berry = BlockingBerry()
    runtime, workers = make_runtime(asr=asr, berry=berry)
    run_task = asyncio.create_task(runtime.run())
    await asyncio.gather(*(worker.started.wait() for worker in workers))

    try:
        await runtime.execute_effect(
            StartBerry(turn_id=1, generation=1, text="question", audio_wav=valid_wav())
        )
        await asyncio.wait_for(berry.started.wait(), timeout=1)
        await runtime.execute_effect(
            QueueAsr(
                session_id="s",
                segment=SpeechSegment(segment_id=1, pcm16_16k=b"\x00\x00"),
            )
        )

        await asyncio.sleep(0)
        assert not asr.started.is_set()

        berry.release.set()
        await asyncio.wait_for(asr.started.wait(), timeout=1)
    finally:
        berry.release.set()
        runtime.request_close()
        await asyncio.wait_for(run_task, timeout=1)


async def test_audio_queue_rejects_bytes_past_three_seconds() -> None:
    runtime, _ = make_runtime(sample_rate=16000)
    maximum = 3 * runtime.sample_rate * 2

    await runtime.audio_queue.put(b"\x00" * maximum)
    assert runtime.audio_queue.queued_bytes == maximum

    with pytest.raises(RuntimeError, match="AUDIO_QUEUE_BYTES_EXCEEDED"):
        await runtime.audio_queue.put(b"\x00\x00")

    assert await runtime.audio_queue.get() == b"\x00" * maximum
    assert runtime.audio_queue.queued_bytes == 0
    await runtime.audio_queue.put(b"\x00\x00")


async def test_outbound_queue_accepts_the_byte_boundary_and_rejects_overflow() -> None:
    runtime, _ = make_runtime()
    maximum = 8 * 1024 * 1024
    one_byte = TextDelta(
        type="TEXT_DELTA",
        user_id="u",
        session_id="s",
        turn_id=1,
        interrupt=False,
        delta="x",
    )
    overhead = len(one_byte.model_dump_json().encode()) - 1
    boundary = TextDelta(
        type="TEXT_DELTA",
        user_id="u",
        session_id="s",
        turn_id=1,
        interrupt=False,
        delta="x" * (maximum - overhead),
    )
    assert len(boundary.model_dump_json().encode()) == maximum

    await runtime.execute_effect(SendOutbound(boundary))
    assert runtime.outbound.queued_bytes == maximum

    with pytest.raises(RuntimeError, match="OUTBOUND_QUEUE_BYTES_EXCEEDED"):
        await runtime.execute_effect(SendOutbound(one_byte))

    assert await runtime.outbound.get() == boundary
    assert runtime.outbound.queued_bytes == 0
    await runtime.execute_effect(SendOutbound(one_byte))


async def test_tts_uses_one_flushed_resampler_for_the_turn() -> None:
    source = sine_pcm16(sample_rate=24000, seconds=0.2, frequency=440)
    runtime, _ = make_runtime(tts=ChunkedTts(source), sample_rate=16000)
    runtime.actor.state.turns[1] = TurnContext(
        turn_id=1,
        asr_text="question",
        audio_wav=valid_wav(),
        stage=TurnStage.STREAMING_TTS,
        tts_generation=1,
    )

    await runtime.execute_effect(
        StartTts(turn_id=1, generation=1, user_input="question", reply_text="reply")
    )
    chunks: list[TtsChunkReceived] = []
    while True:
        event = await asyncio.wait_for(runtime.events.get(), timeout=1)
        if isinstance(event, TtsChunkReceived):
            chunks.append(event)
        if isinstance(event, TtsCompleted):
            break

    output = b"".join(chunk.pcm16 for chunk in chunks)
    assert abs(len(output) // 2 - 3200) <= 2
    assert chunks[-1].finalize is True
    assert all(chunk.session_id == "s" for chunk in chunks)


async def test_interrupted_tts_drains_while_new_turn_completes_independently() -> None:
    source = sine_pcm16(sample_rate=24000, seconds=0.04, frequency=440)
    tts = ConcurrentTts(source)
    runtime, _ = make_runtime(tts=tts, sample_rate=24000)
    runtime.actor.state.turns[1] = TurnContext(
        turn_id=1,
        asr_text="old",
        audio_wav=valid_wav(),
        stage=TurnStage.STREAMING_TTS,
        interrupted=True,
        tts_generation=1,
    )
    runtime.actor.state.turns[2] = TurnContext(
        turn_id=2,
        asr_text="new",
        audio_wav=valid_wav(),
        stage=TurnStage.STREAMING_TTS,
        tts_generation=1,
    )

    await runtime.execute_effect(
        StartTts(turn_id=1, generation=1, user_input="old", reply_text="old reply")
    )
    await asyncio.wait_for(tts.first_started.wait(), timeout=1)
    await runtime.execute_effect(
        StartTts(turn_id=2, generation=1, user_input="new", reply_text="new reply")
    )
    await asyncio.wait_for(tts.second_completed.wait(), timeout=1)

    second_chunk = await next_event(runtime, TtsChunkReceived)
    second_completed = await next_event(runtime, TtsCompleted)
    assert (second_chunk.turn_id, second_completed.turn_id) == (2, 2)
    assert tts.consumed == ["u/s/turn-2"]

    tts.release_first.set()
    first_completed = await next_event(runtime, TtsCompleted)
    assert first_completed.turn_id == 1
    assert tts.consumed == ["u/s/turn-2", "u/s/turn-1"]


async def test_worker_failure_cancels_siblings_and_releases_registry() -> None:
    registry = RecordingRegistry()
    receiver = FailingWorker()
    runtime, workers = make_runtime(receiver=receiver, registry=registry)

    with pytest.raises(ExceptionGroup) as raised:
        await asyncio.wait_for(runtime.run(), timeout=1)

    assert isinstance(raised.value.exceptions[0], RuntimeError)
    assert str(raised.value.exceptions[0]) == "receiver failed"
    assert all(worker.stopped.is_set() for worker in workers)
    assert registry.removed == ["s"]
    assert runtime.background_task_count == 0




async def test_receiver_normal_return_requests_orderly_runtime_close() -> None:
    registry = RecordingRegistry()
    receiver = ReturningReceiver()
    runtime, workers = make_runtime(receiver=receiver, registry=registry)

    await asyncio.wait_for(runtime.run(), timeout=1)

    assert receiver.started.is_set()
    assert all(worker.stopped.is_set() for worker in workers)
    assert registry.removed == ["s"]


@pytest.mark.parametrize("worker_name", ["vad_worker", "sender"])
async def test_returning_long_lived_worker_requests_orderly_runtime_close(
    worker_name: str,
) -> None:
    registry = RecordingRegistry()
    returned_worker = ReturningReceiver()
    runtime, workers = make_runtime(
        registry=registry,
        **{worker_name: returned_worker},
    )

    await asyncio.wait_for(runtime.run(), timeout=0.1)

    assert returned_worker.started.is_set()
    assert all(worker.stopped.is_set() for worker in workers)
    assert registry.removed == ["s"]

async def test_injected_queues_connect_receiver_vad_actor_and_sender() -> None:
    events: asyncio.Queue[object] = asyncio.Queue(maxsize=8)
    sent = asyncio.Event()
    received: list[object] = []
    runtime: SessionRuntime

    class Receiver:
        async def run(self) -> None:
            await runtime.audio_queue.put(b"\x01\x00")
            await asyncio.Event().wait()

    class Vad:
        async def run(self) -> None:
            pcm16 = await runtime.audio_queue.get()
            assert isinstance(pcm16, bytes)
            await events.put(
                SpeechSegmentReady(
                    session_id="s",
                    segment=SpeechSegment(segment_id=1, pcm16_16k=pcm16),
                )
            )
            await asyncio.Event().wait()

    class Asr:
        async def transcribe(self, pcm16_16k: bytes) -> str:
            return "hello"

    class Sender:
        async def run(self) -> None:
            received.append(await runtime.outbound.get())
            sent.set()
            await asyncio.Event().wait()

    runtime = SessionRuntime(
        state=SessionState(user_id="u", session_id="s", sample_rate=16000),
        asr_client=Asr(),
        berry_client=ImmediateBerry(),
        tts_client=EmptyTts(),
        receiver=Receiver(),
        vad_worker=Vad(),
        sender=Sender(),
        event_queue=events,
    )

    run_task = asyncio.create_task(runtime.run())
    await asyncio.wait_for(sent.wait(), timeout=1)
    runtime.request_close()
    await asyncio.wait_for(run_task, timeout=1)

    assert len(received) == 1
    assert received[0].type == "ASR_RESULT"
    assert received[0].text == "hello"



class NeverEndingTts:
    def __init__(self) -> None:
        self.admission = BoundedAdmission(name="tts", concurrency=1, max_waiters=1)
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def stream(self, request: TtsRequest) -> AsyncIterator[TtsChunk]:
        async with self.admission.slot():
            self.started.set()
            try:
                await asyncio.Event().wait()
            finally:
                self.cancelled.set()
        if False:
            yield TtsChunk(chunk_index=0, pcm16_24k=b"\x00\x00", finalize=True)


async def test_live_interrupted_tts_has_a_per_turn_drain_deadline() -> None:
    tts = NeverEndingTts()
    runtime, workers = make_runtime(tts=tts, tts_drain_timeout=0.01)
    runtime.actor.state.turns[1] = TurnContext(
        turn_id=1,
        asr_text="old",
        audio_wav=valid_wav(),
        stage=TurnStage.STREAMING_TTS,
        tts_generation=1,
    )
    run_task = asyncio.create_task(runtime.run())
    await asyncio.gather(*(worker.started.wait() for worker in workers))
    await runtime.execute_effect(
        StartTts(turn_id=1, generation=1, user_input="old", reply_text="old reply")
    )
    await asyncio.wait_for(tts.started.wait(), timeout=1)

    runtime.actor.state.turns[1].interrupted = True
    await runtime.execute_effect(
        SendOutbound(
            message=TurnState(
                type="TURN_STATE",
                user_id="u",
                session_id="s",
                turn_id=1,
                interrupt=True,
                state="INTERRUPTED",
            )
        )
    )
    try:
        await asyncio.wait_for(tts.cancelled.wait(), timeout=0.2)
        async with asyncio.timeout(0.2):
            while runtime.actor.state.turns[1].stage is TurnStage.STREAMING_TTS:
                await asyncio.sleep(0)
        assert runtime.actor.state.turns[1].stage is TurnStage.INTERRUPTED
    finally:
        runtime.request_close()
        await asyncio.wait_for(run_task, timeout=1)

    snapshot = await tts.admission.snapshot()
    assert (snapshot.active, snapshot.waiting) == (0, 0)
    assert runtime.background_task_count == 0
