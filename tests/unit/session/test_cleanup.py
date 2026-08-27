import asyncio
import logging
from collections.abc import AsyncIterator

import pytest

from realtime_voice.audio.vad import SpeechSegment
from realtime_voice.clients.limits import BoundedAdmission
from realtime_voice.clients.thinker import DeleteResult, ThinkerDone, ThinkerReplyRequest
from realtime_voice.clients.tts import TtsChunk, TtsRequest
from realtime_voice.session.actor import QueueAsr, StartThinker, StartTts
from realtime_voice.session.runtime import THINKER_CLEANUP_SKIPPED
from tests.helpers import valid_wav
from tests.unit.session.test_runtime import (
    BlockingWorker,
    EmptyAsr,
    EmptyTts,
    ImmediateThinker,
    RecordingRegistry,
    make_runtime,
)


class OrderedReceiver(BlockingWorker):
    def __init__(self, calls: list[str], stopped_audio: asyncio.Event) -> None:
        super().__init__()
        self.calls = calls
        self.stopped_audio = stopped_audio

    async def run(self) -> None:
        self.started.set()
        try:
            await asyncio.Event().wait()
        finally:
            self.calls.append("stop_audio")
            self.stopped_audio.set()
            self.stopped.set()


class OrderedAsr:
    def __init__(
        self,
        calls: list[str],
        stopped_audio: asyncio.Event,
        cancelled_asr: asyncio.Event,
    ) -> None:
        self.calls = calls
        self.stopped_audio = stopped_audio
        self.cancelled_asr = cancelled_asr
        self.started = asyncio.Event()

    async def transcribe(self, pcm16_16k: bytes) -> str:
        self.started.set()
        try:
            await asyncio.Event().wait()
        finally:
            assert self.stopped_audio.is_set()
            self.calls.append("cancel_asr")
            self.cancelled_asr.set()


class OrderedThinker(ImmediateThinker):
    def __init__(
        self,
        calls: list[str],
        cancelled_asr: asyncio.Event | None,
        thinker_finished: asyncio.Event,
        delete_result: DeleteResult = DeleteResult.DELETED,
        release: asyncio.Event | None = None,
    ) -> None:
        super().__init__()
        self.calls = calls
        self.cancelled_asr = cancelled_asr
        self.thinker_finished = thinker_finished
        self.delete_result = delete_result
        self.release = release
        self.started = asyncio.Event()

    async def stream_reply(self, request: ThinkerReplyRequest) -> AsyncIterator[ThinkerDone]:
        self.started.set()
        if self.cancelled_asr is not None:
            await self.cancelled_asr.wait()
        if self.release is not None:
            await self.release.wait()
        self.calls.append("wait_thinker")
        self.thinker_finished.set()
        yield ThinkerDone(reply_text="reply")

    async def delete_session(self, user_id: str, session_id: str) -> DeleteResult:
        self.calls.append("delete_thinker_session")
        return self.delete_result


class OrderedTts:
    def __init__(self, calls: list[str], thinker_finished: asyncio.Event) -> None:
        self.calls = calls
        self.thinker_finished = thinker_finished
        self.started = asyncio.Event()

    async def stream(self, request: TtsRequest) -> AsyncIterator[TtsChunk]:
        self.started.set()
        await self.thinker_finished.wait()
        self.calls.append("drain_tts")
        yield TtsChunk(chunk_index=0, pcm16_24k=b"\x00\x00" * 240, finalize=True)


class OrderedRegistry(RecordingRegistry):
    def __init__(self, calls: list[str]) -> None:
        super().__init__()
        self.calls = calls

    async def remove(self, key: str) -> None:
        self.calls.append("remove_registry")
        await super().remove(key)


class HungThinker(ImmediateThinker):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def stream_reply(self, request: ThinkerReplyRequest) -> AsyncIterator[ThinkerDone]:
        self.started.set()
        try:
            await asyncio.Event().wait()
        finally:
            self.cancelled.set()
        if False:
            yield ThinkerDone(reply_text="unreachable")


async def test_disconnect_cleanup_follows_the_required_order() -> None:
    calls: list[str] = []
    stopped_audio = asyncio.Event()
    cancelled_asr = asyncio.Event()
    thinker_finished = asyncio.Event()
    thinker_release = asyncio.Event()
    receiver = OrderedReceiver(calls, stopped_audio)
    asr = OrderedAsr(calls, stopped_audio, cancelled_asr)
    thinker = OrderedThinker(calls, None, thinker_finished, release=thinker_release)
    tts = OrderedTts(calls, thinker_finished)
    registry = OrderedRegistry(calls)
    runtime, workers = make_runtime(
        asr=asr,
        thinker=thinker,
        tts=tts,
        receiver=receiver,
        registry=registry,
        sample_rate=24000,
    )
    run_task = asyncio.create_task(runtime.run())
    await asyncio.gather(*(worker.started.wait() for worker in workers))

    try:
        await runtime.execute_effect(
            StartThinker(turn_id=1, generation=1, text="question", audio_wav=valid_wav())
        )
        await asyncio.wait_for(thinker.started.wait(), timeout=1)
        await runtime.execute_effect(
            QueueAsr(
                session_id="s",
                segment=SpeechSegment(segment_id=1, pcm16_16k=b"\x00\x00"),
            )
        )
        await runtime.execute_effect(
            StartTts(turn_id=1, generation=1, user_input="question", reply_text="reply")
        )
        await asyncio.wait_for(tts.started.wait(), timeout=1)
        await asyncio.wait_for(asr.started.wait(), timeout=1)

        runtime.request_close()
        async with asyncio.timeout(1):
            while not runtime._long_tasks["asr"].done():
                await asyncio.sleep(0)
        thinker_release.set()
        await asyncio.wait_for(run_task, timeout=1)
    finally:
        thinker_release.set()
        if not run_task.done():
            runtime.request_close()
            await asyncio.wait_for(run_task, timeout=1)

    assert calls == [
        "stop_audio",
        "cancel_asr",
        "wait_thinker",
        "drain_tts",
        "delete_thinker_session",
        "remove_registry",
    ]
    assert runtime.background_task_count == 0


async def test_thinker_cleanup_timeout_skips_delete_and_logs_stable_code(
    caplog: pytest.LogCaptureFixture,
) -> None:
    thinker = HungThinker()
    registry = RecordingRegistry()
    runtime, workers = make_runtime(
        thinker=thinker,
        registry=registry,
        thinker_cleanup_timeout=0.01,
    )
    run_task = asyncio.create_task(runtime.run())
    await asyncio.gather(*(worker.started.wait() for worker in workers))
    await runtime.execute_effect(
        StartThinker(turn_id=1, generation=1, text="question", audio_wav=valid_wav())
    )
    await asyncio.wait_for(thinker.started.wait(), timeout=1)

    with caplog.at_level(logging.WARNING):
        runtime.request_close()
        await asyncio.wait_for(run_task, timeout=1)

    assert thinker.cancelled.is_set()
    assert thinker.deleted == 0
    assert registry.removed == ["s"]
    assert runtime.background_task_count == 0
    assert [record.message for record in caplog.records] == [THINKER_CLEANUP_SKIPPED]


@pytest.mark.parametrize("result", [DeleteResult.DELETED, DeleteResult.NOT_FOUND])
async def test_cleanup_accepts_deleted_and_already_absent_thinker_session(
    result: DeleteResult,
) -> None:
    calls: list[str] = []
    thinker = OrderedThinker(calls, asyncio.Event(), asyncio.Event(), delete_result=result)
    registry = RecordingRegistry()
    runtime, workers = make_runtime(thinker=thinker, registry=registry)
    run_task = asyncio.create_task(runtime.run())
    await asyncio.gather(*(worker.started.wait() for worker in workers))

    runtime.request_close()
    await asyncio.wait_for(run_task, timeout=1)

    assert calls == ["delete_thinker_session"]
    assert registry.removed == ["s"]


async def test_external_cancellation_drains_background_work_and_releases_registry() -> None:
    registry = RecordingRegistry()
    runtime, workers = make_runtime(
        thinker=ImmediateThinker(),
        tts=EmptyTts(),
        asr=EmptyAsr(),
        registry=registry,
    )
    run_task = asyncio.create_task(runtime.run())
    await asyncio.gather(*(worker.started.wait() for worker in workers))
    await runtime.execute_effect(
        StartThinker(turn_id=1, generation=1, text="question", audio_wav=valid_wav())
    )
    await runtime.execute_effect(
        StartTts(turn_id=1, generation=1, user_input="question", reply_text="reply")
    )

    run_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run_task

    assert registry.removed == ["s"]


class HungTts:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def stream(self, request: TtsRequest) -> AsyncIterator[TtsChunk]:
        self.started.set()
        try:
            await asyncio.Event().wait()
        finally:
            self.cancelled.set()
        if False:
            yield TtsChunk(chunk_index=0, pcm16_24k=b"\x00\x00", finalize=True)


async def test_tts_cleanup_timeout_cancels_stream_before_delete() -> None:
    tts = HungTts()
    thinker = ImmediateThinker()
    registry = RecordingRegistry()
    runtime, workers = make_runtime(
        tts=tts,
        thinker=thinker,
        registry=registry,
        tts_drain_timeout=0.01,
    )
    run_task = asyncio.create_task(runtime.run())
    await asyncio.gather(*(worker.started.wait() for worker in workers))
    await runtime.execute_effect(
        StartTts(turn_id=1, generation=1, user_input="question", reply_text="reply")
    )
    await asyncio.wait_for(tts.started.wait(), timeout=1)

    runtime.request_close()
    await asyncio.wait_for(run_task, timeout=1)

    assert tts.cancelled.is_set()
    assert thinker.deleted == 1
    assert registry.removed == ["s"]
    assert runtime.background_task_count == 0


async def test_asr_cancellation_releases_admission_before_registry_removal() -> None:
    admission = BoundedAdmission(name="asr", concurrency=1, max_waiters=1)
    started = asyncio.Event()

    class AdmittedAsr:
        async def transcribe(self, pcm16_16k: bytes) -> str:
            async with admission.slot():
                started.set()
                await asyncio.Event().wait()
            return ""

    registry = RecordingRegistry()
    runtime, workers = make_runtime(asr=AdmittedAsr(), registry=registry)
    run_task = asyncio.create_task(runtime.run())
    await asyncio.gather(*(worker.started.wait() for worker in workers))
    await runtime.execute_effect(
        QueueAsr(
            session_id="s",
            segment=SpeechSegment(segment_id=1, pcm16_16k=b"\x00\x00"),
        )
    )
    await asyncio.wait_for(started.wait(), timeout=1)

    runtime.request_close()
    await asyncio.wait_for(run_task, timeout=1)

    snapshot = await admission.snapshot()
    assert (snapshot.active, snapshot.waiting) == (0, 0)
    assert registry.removed == ["s"]

    assert runtime.background_task_count == 0
