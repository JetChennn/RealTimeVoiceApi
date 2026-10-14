import asyncio
import io
import wave

import pytest

from realtime_voice.audio.pcm import pcm16_wav_bytes
from realtime_voice.config import Settings
from realtime_voice.observability.metrics import Metrics
from realtime_voice.session.actor import RecordFilteredInput, SessionActor
from realtime_voice.session.events import (
    AsrFailed,
    AsrSucceeded,
    AudioSegmentDiscarded,
    SemanticEvaluated,
    SpeechSegmentReady,
    UserInputCommitted,
    UserInputFailed,
)
from realtime_voice.session.state import SessionState
from realtime_voice.turn_end.audio import TurnEndAudio
from realtime_voice.turn_end.coordinator import TurnEndCoordinator
from realtime_voice.turn_end.model import Decision


class Detector:
    def __init__(self, end=False):
        self.end = end
        self.calls = []

    async def predict(self, context, text):
        self.calls.append(text)
        return Decision(self.end, 0.9 if self.end else 0.1, "end" if self.end else "wait", 0.01)


class Harness:
    def __init__(self, end=False, **kwargs):
        self.settings = Settings(_env_file=None, turn_end_semantic_enabled=True, **kwargs)
        self.audio = TurnEndAudio("s", self.settings)
        self.queue = asyncio.Queue()
        self.detector = Detector(end)
        self.metrics = Metrics()
        self.coordinator = TurnEndCoordinator(
            "s", self.settings, self.detector, self.audio, self.queue, self.metrics, list
        )
        self.segments = []
        self.outputs = []
        self.asr_calls = []

    def feed(self, speaking, frames):
        for _ in range(frames):
            for event in self.audio.push(
                (b"\x01\x00" if speaking else b"\x00\x00") * 512, speaking
            ):
                if isinstance(event, SpeechSegmentReady):
                    self.segments.append(event)
                self.outputs.extend(self.coordinator.process(event))

    def asr(self, text, index=-1):
        event = self.segments[index]
        self.asr_calls.append(event.segment.segment_id)
        self.outputs.extend(
            self.coordinator.process(
                AsrSucceeded(
                    "s",
                    event.segment.segment_id,
                    text,
                    pcm16_wav_bytes(event.segment.pcm16_16k, 16000),
                )
            )
        )

    async def predictions(self):
        await asyncio.sleep(0)
        while not self.queue.empty():
            self.outputs.extend(self.coordinator.process(self.queue.get_nowait()))


def test_turn_end_audio_keeps_exact_pre_and_post_speech_padding() -> None:
    settings = Settings(
        _env_file=None,
        turn_end_semantic_enabled=True,
        speaker_verification_enabled=True,
    )
    audio = TurnEndAudio("s", settings)
    silence = b"\x00\x00" * 512
    speech = b"\x01\x00" * 512

    for _ in range(4):
        assert audio.push(silence, False) == []
    events = []
    for _ in range(16):
        events.extend(audio.push(speech, True))
    for _ in range(16):
        events.extend(audio.push(silence, False))

    ready = next(event for event in events if isinstance(event, SpeechSegmentReady))
    assert ready.segment.voiced_samples == 16 * 512
    assert ready.segment.padded_voiced_samples == (100 + 16 * 32 + 150) * 16
    assert ready.segment.pcm16_16k.startswith(b"\x00\x00" * (100 * 16))


async def test_filtered_segment_is_removed_without_marking_the_input_as_asr_failure():
    h = Harness()
    h.feed(True, 10)
    h.feed(False, 16)
    segment = h.segments[-1]

    output = h.coordinator.process(
        AudioSegmentDiscarded("s", segment.segment.segment_id, "mismatch")
    )

    assert output == []
    assert segment.segment.segment_id not in h.coordinator.segment_inputs
    assert h.coordinator.inputs[segment.input_id].failed is False
    await h.coordinator.aclose()


async def test_resumption_combines_once_and_does_not_commit_before_minimum():
    h = Harness(end=True, turn_end_min_silence_ms=1000)
    h.feed(True, 10)
    h.feed(False, 16)
    h.asr("真不知道。")
    await h.predictions()
    assert not h.outputs
    h.feed(True, 10)
    h.feed(False, 16)
    h.asr("事先一点预兆都没有。")
    await h.predictions()
    h.feed(False, 15)  # total silence 992ms
    assert not h.outputs
    h.feed(False, 1)
    assert len(h.outputs) == 1
    result = h.outputs[0]
    assert result.text == "真不知道。 事先一点预兆都没有。"
    assert result.reason == "semantic_end"
    assert h.asr_calls == [1, 2]
    with wave.open(io.BytesIO(result.audio_wav)) as w:
        assert w.getnframes() == 52 * 512
    assert h.detector.calls == ["真不知道。", result.text]
    await h.coordinator.aclose()


@pytest.mark.parametrize("second", ["啊！", "我想去杭州。"])
async def test_filler_filter_runs_after_resumed_fragments_merge_and_release(second):
    h = Harness(end=True, turn_end_min_silence_ms=1000)
    actor = SessionActor(SessionState("u", "s", 16000))
    try:
        h.feed(True, 10)
        h.feed(False, 16)
        h.asr("嗯，")
        await h.predictions()
        assert h.outputs == []
        h.feed(True, 10)
        h.feed(False, 16)
        h.asr(second)
        await h.predictions()
        h.feed(False, 16)
        assert len(h.outputs) == 1
        committed = h.outputs[0]
        assert committed.text == f"嗯， {second}"
        effects = actor.handle(committed)
        if second == "啊！":
            assert effects == [RecordFilteredInput(2)]
            assert actor.state.turns == {}
        else:
            assert actor.state.turns[1].asr_text == committed.text
        assert h.coordinator.inputs == {}
        assert h.coordinator.segment_inputs == {}
        assert not h.audio.started
        assert not h.audio.fragment
        assert h.audio.input_id == 2
        # 过滤完的输入已退休，后续输入不受影响。
        h.outputs.clear()
        h.feed(True, 10)
        h.feed(False, 32)
        h.asr("继续介绍")
        await h.predictions()
        assert len(h.outputs) == 1
        actor.handle(h.outputs[0])
        assert actor.state.turns[actor.state.next_turn_id - 1].asr_text == "继续介绍"
    finally:
        await h.coordinator.aclose()


async def test_semantic_end_commits_immediately_when_minimum_is_zero():
    h = Harness(end=True, turn_end_min_silence_ms=0)
    h.feed(True, 10)
    h.feed(False, 16)
    h.asr("这句话已经完整。")

    await h.predictions()

    assert len(h.outputs) == 1
    assert h.outputs[0].text == "这句话已经完整。"
    assert h.outputs[0].reason == "semantic_end"
    await h.coordinator.aclose()


async def test_wait_for_maximum_without_repeating_asr_or_inference():
    h = Harness()
    h.feed(True, 10)
    h.feed(False, 16)
    h.asr("如果明天")
    await h.predictions()
    h.feed(False, 46)  # 1984ms
    assert not h.outputs
    h.feed(False, 1)
    assert h.outputs[0].reason == "max_silence"
    assert h.asr_calls == [1]
    assert len(h.detector.calls) == 1
    await h.coordinator.aclose()


async def test_maximum_waits_for_asr_and_no_frames_is_not_silence():
    h = Harness()
    h.feed(True, 10)
    h.feed(False, 16)
    await asyncio.sleep(0.01)
    assert not h.outputs
    h.feed(False, 47)
    assert not h.outputs
    h.asr("后返回的完整文字")
    assert h.outputs[0].text == "后返回的完整文字"
    assert not h.detector.calls
    await h.coordinator.aclose()


async def test_stale_end_prediction_cannot_commit_resumed_speech():
    h = Harness()
    h.feed(True, 10)
    h.feed(False, 16)
    h.asr("第一部分")
    h.feed(True, 10)
    h.outputs.extend(
        h.coordinator.process(SemanticEvaluated("s", 1, 1, 1, True, 0.99, "end", 0.01))
    )
    h.feed(False, 16)
    h.asr("第二部分")
    await h.predictions()
    h.feed(False, 16)
    assert not h.outputs
    h.feed(False, 31)
    assert h.outputs[0].reason == "max_silence"
    await h.coordinator.aclose()


async def test_length_limit_preserves_order_and_bounds_audio_without_retranscription():
    h = Harness(turn_end_max_utterance_seconds=2)
    h.feed(True, 126)  # two forced inputs, no silence
    assert len(h.segments) == 2
    h.asr("第二段", 1)
    assert not h.outputs
    h.asr("第一段", 0)
    assert [v.text for v in h.outputs] == ["第一段", "第二段"]
    assert all(v.reason == "max_utterance" for v in h.outputs)
    assert len(h.audio.fragment) == 0
    assert not h.coordinator.segment_inputs
    await h.coordinator.aclose()


async def test_asr_failure_does_not_submit_successful_prefix_and_next_input_recovers():
    h = Harness()
    h.feed(True, 10)
    h.feed(False, 16)
    h.asr("第一部分")
    h.feed(True, 10)
    h.feed(False, 16)
    result = h.coordinator.process(AsrFailed("s", 2, "ASR_FAILED", "failed"))
    assert len(result) == 1 and isinstance(result[0], UserInputFailed)
    assert not any(isinstance(v, UserInputCommitted) for v in h.outputs)
    h.feed(True, 10)
    h.feed(False, 63)
    h.asr("新输入")
    assert h.outputs[0].text == "新输入"
    await h.coordinator.aclose()


async def test_shutdown_discards_pending_results_and_buffers():
    h = Harness()
    h.feed(True, 10)
    h.feed(False, 16)
    h.asr("候选")
    await h.coordinator.aclose()
    assert not h.coordinator.inputs and not h.coordinator.tasks
    assert not h.audio.fragment
