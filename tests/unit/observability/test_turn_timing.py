import asyncio
import json
from types import SimpleNamespace

import pytest
from pydantic import TypeAdapter, ValidationError

from realtime_voice.audio.vad import SpeechSegment
from realtime_voice.clients.limits import BoundedAdmission
from realtime_voice.clients.thinker import ThinkerDone, ThinkerTextDelta
from realtime_voice.clients.tts import TtsChunk
from realtime_voice.config import Settings
from realtime_voice.observability.metrics import Metrics
from realtime_voice.observability.turn_timing import (
    InputTiming,
    SegmentTiming,
    StageTiming,
    TurnTiming,
    current_stage,
)
from realtime_voice.protocol.client_messages import CreateSession
from realtime_voice.protocol.decoder import decode_client_message
from realtime_voice.protocol.encoder import encode_server_message
from realtime_voice.protocol.server_messages import ServerMessage, TurnMetrics
from realtime_voice.session.events import SpeechActivity, SpeechSegmentReady
from realtime_voice.turn_end.model import Decision
from tests.helpers import sine_pcm16
from tests.unit.session.test_runtime import (
    BlockingWorker,
    ControlledAsr,
    ControlledThinker,
    ControlledTts,
    ImmediateThinker,
    make_runtime,
)


class Clock:
    def __init__(self, value=10):
        self.value = value

    def __call__(self):
        return self.value


class ControlledSemantic:
    def __init__(self):
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def predict(self, context, text):
        self.started.set()
        await self.release.wait()
        return Decision(False, 0.1, "wait", 0.02)


class BistreamTts(ControlledTts):
    bistream_enabled = True

    async def stream_bistream(self, request, inputs):
        await anext(inputs)
        async for chunk in self.stream(request):
            yield chunk


async def until(predicate):
    async with asyncio.timeout(2):
        while not predicate():
            await asyncio.sleep(0)


async def collect_turn(runtime):
    messages = []
    async with asyncio.timeout(2):
        while True:
            message = await runtime.outbound.get()
            messages.append(message)
            if message.type == "RESPONSE_END":
                return messages


@pytest.mark.parametrize("bistream", [False, True])
@pytest.mark.parametrize("enabled", [False, True])
async def test_serial_timeline_excludes_initial_silence_retains_post_wait(bistream, enabled):
    clock = Clock()
    asr, thinker, semantic = ControlledAsr(), ControlledThinker(), ControlledSemantic()
    tts_type = BistreamTts if bistream else ControlledTts
    tts = tts_type(sine_pcm16(sample_rate=24000, seconds=0.02, frequency=440))
    vad = BlockingWorker()
    vad.turn_end_audio = SimpleNamespace(input_id=1, release=lambda _: None)
    metrics = Metrics()
    runtime, workers = make_runtime(
        asr=asr, thinker=thinker, tts=tts, vad_worker=vad, clock=clock,
        turn_end_settings=Settings(_env_file=None), semantic_detector=semantic,
        metrics=metrics, enable_turn_metrics=enabled,
    )
    task = asyncio.create_task(runtime.run())
    await asyncio.gather(*(w.started.wait() for w in workers))
    try:
        await runtime.events.put(SpeechSegmentReady(
            "s", SpeechSegment(1, b"\x01\x00" * 512, trailing_silence_samples=8000),
            speech_end_at=9.5, input_id=1, ready_at=10,
        ))
        await runtime.events.put(SpeechActivity("s", 1, 1, False, 500, 9.5))
        await asr.started.wait()
        clock.value = 10.1
        asr.release.set()
        await semantic.started.wait()
        clock.value = 10.12
        semantic.release.set()
        await until(lambda: runtime.turn_end.inputs[1].timing.waiting_at is not None)
        clock.value = 10.5
        await runtime.events.put(SpeechActivity("s", 1, 1, False, 2000, 9.5))
        await thinker.started.wait()
        clock.value = 10.7
        thinker.release.set()
        await tts.started.wait()
        clock.value = 10.8
        tts.release.set()
        messages = await collect_turn(runtime)
        snapshots = [m for m in messages if isinstance(m, TurnMetrics)]
        assert bool(snapshots) == enabled
        assert any(m.type == "AUDIO_DELTA" for m in messages)
        if enabled:
            first, final = snapshots[0], snapshots[-1]
            assert first.asr.ms == pytest.approx(100)
            assert first.llm.ms is None and first.total.ms is None
            assert final.status == "completed"
            assert final.asr.ms == pytest.approx(100)
            assert final.semantic.ms == pytest.approx(20)
            assert final.llm.ms == pytest.approx(200)
            assert final.tts.ms == pytest.approx(100)
            assert final.total.ms == pytest.approx(800)
            assert final.pre_segment_silence_ms == 500
            assert final.post_segment_wait_ms == pytest.approx(380)
            assert [m.sequence for m in snapshots] == list(range(1, len(snapshots) + 1))
            parsed = TypeAdapter(ServerMessage).validate_json(encode_server_message(final))
            assert parsed == final
        assert metrics.vad_ready_to_first_tts._sum.get() == pytest.approx(0.8)
        assert not runtime._turn_timings and not runtime._segment_timings
    finally:
        asr.release.set()
        semantic.release.set()
        thinker.release.set()
        tts.release.set()
        runtime.request_close()
        await asyncio.wait_for(task, 2)


def test_final_segment_and_version_are_primary_without_summing_history():
    clock = Clock()
    stages = []
    segments = []
    for sid, start, duration in [(1, 8, 0.2), (2, 10, 0.1)]:
        stage = StageTiming("asr", start, clock, segment_id=sid)
        stage.started_at = start
        stage.finish(at=start + duration)
        segments.append(SegmentTiming(sid, start, 500, stage))
        semantic = StageTiming("semantic", start + duration, clock, version=sid, segment_count=sid)
        semantic.finish("wait", at=start + duration + 0.02)
        stages.append(semantic)
    source = InputTiming(segments, stages, [(8.3, 9), (10.12, 10.5)], version=2)
    result = TurnTiming(source, StageTiming("llm", 10.5, clock)).snapshot()
    assert result["asr"]["ms"] == pytest.approx(100)
    assert result["semantic"]["ms"] == pytest.approx(20)
    assert result["post_segment_wait_ms"] == pytest.approx(380)
    assert len(result["details"]) == 5
    assert result["details"][0]["offset_ms"] == -2000


async def test_admission_queue_is_included_and_isolated_between_tasks():
    clock = Clock()
    limiter = BoundedAdmission("asr", 1, 1)
    entered, release = asyncio.Event(), asyncio.Event()
    stages = [StageTiming("asr", 10, clock), StageTiming("asr", 10, clock)]

    async def request(stage, blocked=False):
        token = current_stage.set(stage)
        try:
            stage.begin()
            async with limiter.slot():
                if blocked:
                    entered.set()
                    await release.wait()
                else:
                    clock.value = 10.3
                stage.finish()
        finally:
            current_stage.reset(token)

    first = asyncio.create_task(request(stages[0], True))
    await entered.wait()
    second = asyncio.create_task(request(stages[1]))
    await limiter.wait_until_waiting(1)
    clock.value = 10.2
    release.set()
    await asyncio.gather(first, second)
    assert stages[0].value()["queue_ms"] == 0
    assert stages[1].value()["queue_ms"] == pytest.approx(200)
    assert stages[1].value()["call_ms"] == pytest.approx(100)
    assert current_stage.get() is None


@pytest.mark.parametrize("stage_name", ["llm", "tts"])
@pytest.mark.parametrize("exit_mode", ["cancel", "late_acquire"])
async def test_interrupted_snapshot_freezes_in_flight_admission_wait(stage_name, exit_mode):
    from realtime_voice.protocol.server_messages import TurnState
    from realtime_voice.session.actor import SendOutbound
    from realtime_voice.session.state import TurnContext, TurnStage

    clock = Clock()
    limiter = BoundedAdmission("thinker" if stage_name == "llm" else "tts", 1, 1)
    release = asyncio.Event()
    executed = False
    runtime, _ = make_runtime(clock=clock, enable_turn_metrics=True)
    asr = StageTiming("asr", 9.9, clock)
    asr.finish(at=10)
    llm = StageTiming("llm", 10, clock, generation=1)
    if stage_name == "tts":
        llm.finish(at=10)
    tts = StageTiming("tts", 10, clock, generation=1) if stage_name == "tts" else None
    timing = TurnTiming(InputTiming([SegmentTiming(1, 9.9, 500, asr)]), llm, tts)
    stage = llm if stage_name == "llm" else tts
    runtime._turn_timings[1] = timing
    runtime.actor.state.turns[1] = TurnContext(
        1, "hello", b"", thinker_generation=1, tts_generation=1,
        stage=TurnStage.STREAMING_LLM if stage_name == "llm" else TurnStage.STREAMING_TTS,
        interrupted=True,
    )

    async def holder():
        async with limiter.slot():
            await release.wait()

    async def queued_request():
        nonlocal executed
        token = current_stage.set(stage)
        try:
            stage.begin()
            async with limiter.slot():
                executed = True
                stage.finish()
        finally:
            current_stage.reset(token)

    held = asyncio.create_task(holder())
    queued = None
    try:
        async with asyncio.timeout(2):
            await limiter.wait_until_active(1)
            queued = asyncio.create_task(queued_request())
            await limiter.wait_until_waiting(1)
            clock.value = 11
            await runtime.execute_effect(SendOutbound(TurnState(
                type="TURN_STATE", user_id="u", session_id="s", turn_id=1,
                interrupt=True, state="INTERRUPTED",
            )))
            messages = [runtime.outbound.get_nowait(), runtime.outbound.get_nowait()]
            snapshot = next(m for m in messages if isinstance(m, TurnMetrics))
            detail = next(d for d in snapshot.details if d.stage == stage_name)
            assert snapshot.status == "interrupted"
            assert getattr(snapshot, stage_name).ms is None
            assert detail.ms == pytest.approx(1000)
            assert detail.queue_ms == pytest.approx(1000)
            assert detail.call_ms == 0
            assert executed is False
            frozen_value = stage.value()

            # The snapshot is already on the outbound queue before cancellation
            # or a late admission grant resumes the request coroutine.
            clock.value = 12
            if exit_mode == "cancel":
                queued.cancel()
            release.set()
            await asyncio.gather(held, queued, return_exceptions=True)
            assert executed == (exit_mode == "late_acquire")
            assert stage.value() == frozen_value
            assert (await limiter.snapshot()).active == 0
            assert (await limiter.snapshot()).waiting == 0
    finally:
        release.set()
        if queued is not None:
            queued.cancel()
        await asyncio.gather(held, *([queued] if queued is not None else []), return_exceptions=True)


async def test_empty_text_is_not_llm_first_packet_and_missing_audio_is_not_zero():
    clock = Clock()

    class EmptyDeltaThinker(ImmediateThinker):
        async def stream_reply(self, request):
            yield ThinkerTextDelta("")
            clock.value = 10.4
            yield ThinkerTextDelta("reply")
            yield ThinkerDone("reply", "")

    asr = ControlledAsr()
    asr.release.set()
    runtime, workers = make_runtime(
        asr=asr, thinker=EmptyDeltaThinker(), clock=clock, enable_turn_metrics=True,
    )
    task = asyncio.create_task(runtime.run())
    await asyncio.gather(*(w.started.wait() for w in workers))
    try:
        await runtime.events.put(SpeechSegmentReady("s", SpeechSegment(1, b"\x01\x00")))
        messages = await collect_turn(runtime)
        final = [m for m in messages if isinstance(m, TurnMetrics)][-1]
        assert final.llm.ms == pytest.approx(400)
        assert final.semantic.status == "not_run"
        assert final.tts.ms is None and final.tts.status == "not_produced"
        assert final.total.ms is None
    finally:
        runtime.request_close()
        await asyncio.wait_for(task, 2)


def test_interruption_freezes_existing_values_and_late_results_cannot_rewrite_them():
    clock = Clock()
    asr = StageTiming("asr", 10, clock)
    asr.finish(at=10.1)
    source = InputTiming([SegmentTiming(1, 10, 500, asr)])
    llm = StageTiming("llm", 10.1, clock)
    timing = TurnTiming(source, llm)
    clock.value = 10.2
    timing.finish("interrupted")
    clock.value = 11
    llm.finish()
    value = timing.snapshot()
    assert value["asr"]["ms"] == pytest.approx(100)
    assert value["llm"]["ms"] is None
    assert value["llm"]["status"] == "interrupted"
    assert value["total"]["ms"] is None
    assert value["details"][-1]["ms"] == pytest.approx(100)


def test_turn_metrics_capability_defaults_off_and_requires_boolean():
    payload = {
        "type": "CREATE_SESSION", "protocol_version": 1, "device_id": "u", "session_id": "s",
        "audio_format": "PCM16", "audio_transport": "BASE64_JSON", "sample_rate": 16000,
        "channels": 1,
    }
    message = decode_client_message(json.dumps(payload))
    assert isinstance(message, CreateSession) and message.enable_turn_metrics is False
    assert CreateSession(**payload, enable_turn_metrics=True).enable_turn_metrics is True
    with pytest.raises(ValidationError):
        CreateSession(**payload, enable_turn_metrics="true")


async def test_asr_queue_includes_dispatch_delay_but_excludes_speaker_precheck():
    from realtime_voice.speaker.gate import SpeakerDecision

    clock = Clock(10.2)  # The VAD event was generated 200ms before dispatch.

    class Gate:
        async def evaluate(self, segment):
            clock.value += 0.05
            return SpeakerDecision(True, "matched", registered=True)

        def clear(self):
            pass

    class Asr:
        async def transcribe(self, pcm, *, context=""):
            clock.value += 0.1
            return "hello"

    runtime, workers = make_runtime(
        asr=Asr(), speaker_gate=Gate(), clock=clock, enable_turn_metrics=True,
    )
    task = asyncio.create_task(runtime.run())
    await asyncio.gather(*(w.started.wait() for w in workers))
    try:
        await runtime.events.put(SpeechSegmentReady("s", SpeechSegment(1, b"\x01\x00"), ready_at=10))
        snapshots = [m for m in await collect_turn(runtime) if isinstance(m, TurnMetrics)]
        final = snapshots[-1]
        assert final.asr.ms == pytest.approx(300)
        assert final.asr.queue_ms == pytest.approx(200)
        assert final.asr.call_ms == pytest.approx(100)
        speaker = next(d for d in final.details if d.stage == "speaker")
        assert speaker.ms == pytest.approx(50)
    finally:
        runtime.request_close()
        await asyncio.wait_for(task, 2)


@pytest.mark.parametrize("reason", ["semantic_end", "max_silence", "max_utterance"])
async def test_coordinator_silence_overlap_resumption_and_forced_commit_timings(reason):
    from realtime_voice.audio.pcm import pcm16_wav_bytes
    from realtime_voice.session.events import AsrSucceeded
    from realtime_voice.turn_end.audio import TurnEndAudio
    from realtime_voice.turn_end.coordinator import TurnEndCoordinator

    clock = Clock(10)
    settings = Settings(_env_file=None, turn_end_min_silence_ms=1000)
    events = asyncio.Queue()

    class Detector:
        async def predict(self, context, text):
            # Pretend model processing overlaps 300ms of observed silence.
            clock.value += 0.3
            end = reason == "semantic_end"
            return Decision(end, 0.9 if end else 0.1, "end" if end else "wait", 0.3)

    coordinator = TurnEndCoordinator(
        "s", settings, Detector(), TurnEndAudio("s", settings), events, Metrics(), list,
        clock=clock,
    )
    source = InputTiming()
    try:
        for sid in [1, 2]:
            clock.value = 10 + (sid - 1) * 2
            coordinator.process(SpeechActivity("s", 1, sid, True, 0, clock()))
            asr = StageTiming("asr", clock(), clock, segment_id=sid)
            segment = SegmentTiming(sid, clock(), 500, asr)
            source.segments.append(segment)
            forced = sid == 2 and reason == "max_utterance"
            coordinator.process(SpeechSegmentReady(
                "s", SpeechSegment(sid, b"\x01\x00"), input_id=1,
                force_commit=forced, ready_at=clock(), timing=segment,
            ))
            coordinator.process(SpeechActivity("s", 1, sid, False, 500, clock() - 0.5))
            clock.value += 0.1
            asr.finish()
            output = coordinator.process(AsrSucceeded(
                "s", sid, "第一段" if sid == 1 else "第二段", pcm16_wav_bytes(b"\x01\x00", 16000),
                timing=segment,
            ))
            if forced:
                break
            await asyncio.sleep(0)
            coordinator.process(await events.get())
        if reason != "max_utterance":
            clock.value = 12.8
            silence = 1000 if reason == "semantic_end" else 2000
            output = coordinator.process(SpeechActivity("s", 1, 2, False, silence, 11.5))
        committed = output[0]
        assert committed.reason == reason
        assert committed.timing.version == 2
        assert len(committed.timing.segments) == 2
        assert committed.timing.final.ready_at == 12
        expected_wait = 0 if reason == "max_utterance" else 400
        assert committed.timing.pure_wait_ms() == pytest.approx(expected_wait)
        # Earlier pause and semantic processing are not added to the final pure wait.
        assert committed.timing.segments[-1].asr.value()["ms"] == pytest.approx(100)
    finally:
        await coordinator.aclose()


async def test_interrupted_pending_llm_does_not_accept_a_late_first_packet():
    from realtime_voice.protocol.server_messages import TurnState
    from realtime_voice.session.actor import SendOutbound
    from realtime_voice.session.state import TurnContext

    clock = Clock()
    runtime, _ = make_runtime(clock=clock, enable_turn_metrics=True)
    asr = StageTiming("asr", 10, clock)
    asr.finish(at=10.1)
    runtime.actor.state.turns[1] = TurnContext(1, "hello", b"", thinker_generation=1)
    timing = TurnTiming(
        InputTiming([SegmentTiming(1, 10, 500, asr)]),
        StageTiming("llm", 10.1, clock, generation=1),
    )
    runtime._turn_timings[1] = timing
    runtime.actor.state.turns[1].interrupted = True
    clock.value = 10.2
    await runtime.execute_effect(SendOutbound(TurnState(
        type="TURN_STATE", user_id="u", session_id="s", turn_id=1,
        interrupt=True, state="INTERRUPTED",
    )))
    assert runtime._stage_timing(1, 1, "llm") is None
    assert runtime._stage_timing(1, 2, "llm") is None
    messages = [runtime.outbound.get_nowait(), runtime.outbound.get_nowait()]
    snapshot = next(m for m in messages if isinstance(m, TurnMetrics))
    assert snapshot.status == "interrupted"
    assert snapshot.llm.ms is None and snapshot.llm.status == "interrupted"
    assert snapshot.asr.ms == pytest.approx(100)


@pytest.mark.parametrize("failure", ["llm", "tts", "fallback"])
async def test_failure_and_fallback_keep_missing_first_packet_explicit(failure):
    clock = Clock()

    class FailedThinker(ImmediateThinker):
        async def stream_reply(self, request):
            if False:
                yield
            clock.value = 10.3
            raise TimeoutError("no first packet")

    class ReplyThinker(ImmediateThinker):
        async def stream_reply(self, request):
            clock.value = 10.2
            yield ThinkerTextDelta("reply")
            yield ThinkerDone("reply", "")

    class Tts:
        async def stream(self, request):
            clock.value = 10.4
            if failure == "tts":
                raise RuntimeError("TTS failed before audio")
            yield TtsChunk(0, b"\x01\x00" * 480, True)

    asr = ControlledAsr()
    asr.release.set()
    runtime, workers = make_runtime(
        asr=asr, thinker=ReplyThinker() if failure == "tts" else FailedThinker(),
        tts=Tts(), clock=clock, enable_turn_metrics=True,
        thinker_fallback_enabled=failure == "fallback", thinker_fallback_texts=("请再说一次",),
        thinker_fallback_tone="平和",
    )
    task = asyncio.create_task(runtime.run())
    await asyncio.gather(*(w.started.wait() for w in workers))
    try:
        await runtime.events.put(SpeechSegmentReady("s", SpeechSegment(1, b"\x01\x00")))
        messages = await collect_turn(runtime)
        final = [m for m in messages if isinstance(m, TurnMetrics)][-1]
        assert final.fallback == (failure == "fallback")
        if failure == "fallback":
            assert final.status == "completed"
            assert final.llm.ms is None and final.llm.status == "timeout"
            assert final.tts.ms == pytest.approx(100)
            assert final.total.ms == pytest.approx(400)
        else:
            assert final.status == "failed"
            assert final.total.ms is None and final.total.status == "failed"
            if failure == "llm":
                assert final.llm.ms is None and final.llm.status == "timeout"
            else:
                assert final.llm.ms == pytest.approx(200)
                assert final.tts.ms is None and final.tts.status == "failed"
    finally:
        runtime.request_close()
        await asyncio.wait_for(task, 2)
