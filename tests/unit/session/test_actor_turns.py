import base64

from realtime_voice.session.actor import StartBerry, StartTts
from realtime_voice.session.events import (
    AsrFailed,
    AsrSucceeded,
    BerryCompleted,
    BerryDeltaReceived,
    BerryFailed,
    TtsChunkReceived,
    TtsCompleted,
    TtsFailed,
)
from realtime_voice.session.state import TurnStage
from tests.helpers import valid_wav
from tests.unit.session.conftest import actor_for_test, actor_with_tts_turn, outbound_of_type


def test_empty_asr_creates_no_turn() -> None:
    actor = actor_for_test()

    effects = actor.handle(AsrSucceeded(41, "", valid_wav()))

    assert actor.state.next_turn_id == 1
    assert actor.state.turns == {}
    assert effects == []


def test_empty_asr_does_not_interrupt_an_active_turn() -> None:
    actor = actor_for_test()
    actor.handle(AsrSucceeded(1, "active", valid_wav()))

    effects = actor.handle(AsrSucceeded(2, "", valid_wav()))

    assert effects == []
    assert actor.state.turns[1].interrupted is False
    assert actor.state.next_turn_id == 2


def test_non_empty_asr_allocates_public_turn_independently_from_segment_id() -> None:
    actor = actor_for_test()

    effects = actor.handle(AsrSucceeded(41, "你好", valid_wav()))

    assert actor.state.next_turn_id == 2
    assert actor.state.turns[1].asr_text == "你好"
    assert outbound_of_type(effects, "ASR_RESULT").turn_id == 1
    start = next(effect for effect in effects if isinstance(effect, StartBerry))
    assert (start.turn_id, start.generation, start.text) == (1, 1, "你好")


def test_uninterrupted_turn_streams_text_then_audio_and_completes() -> None:
    actor = actor_for_test()
    actor.handle(AsrSucceeded(7, "问题", valid_wav()))

    delta = outbound_of_type(
        actor.handle(BerryDeltaReceived(1, generation=1, delta="答")), "TEXT_DELTA"
    )
    completed = actor.handle(BerryCompleted(1, generation=1, reply_text="答案"))
    text_end = outbound_of_type(completed, "TEXT_END")
    start_tts = next(effect for effect in completed if isinstance(effect, StartTts))
    audio = outbound_of_type(
        actor.handle(TtsChunkReceived(1, 1, 8, b"\x01\x00", False)), "AUDIO_DELTA"
    )
    ended = outbound_of_type(actor.handle(TtsCompleted(1, generation=1)), "RESPONSE_END")

    assert (delta.delta, delta.interrupt) == ("答", False)
    assert (text_end.text, text_end.interrupt) == ("答案", False)
    assert (start_tts.turn_id, start_tts.generation, start_tts.reply_text) == (1, 1, "答案")
    assert (audio.sequence, audio.sample_rate, audio.interrupt) == (0, 16000, False)
    assert base64.b64decode(audio.audio_b64) == b"\x01\x00"
    assert (ended.status, ended.interrupt) == ("COMPLETED", False)
    assert actor.state.turns[1].stage is TurnStage.COMPLETED


def test_asr_failure_emits_session_level_recoverable_error_without_turn() -> None:
    actor = actor_for_test()

    effects = actor.handle(AsrFailed(9, code="ASR_TIMEOUT", message="timed out"))

    error = outbound_of_type(effects, "ERROR")
    assert (error.turn_id, error.stage, error.code, error.recoverable) == (
        0,
        "ASR",
        "ASR_TIMEOUT",
        True,
    )
    assert actor.state.turns == {}


def test_berry_failure_ends_turn_failed_and_allows_next_berry() -> None:
    actor = actor_for_test()
    actor.handle(AsrSucceeded(1, "one", valid_wav()))

    effects = actor.handle(BerryFailed(1, 1, code="BERRY_FAILED", message="bad stream"))

    error = outbound_of_type(effects, "ERROR")
    ended = outbound_of_type(effects, "RESPONSE_END")
    assert (error.stage, error.interrupt) == ("LLM", False)
    assert (ended.status, ended.interrupt) == ("FAILED", False)
    assert actor.state.turns[1].stage is TurnStage.FAILED


def test_tts_failure_ends_turn_failed() -> None:
    actor = actor_with_tts_turn()

    effects = actor.handle(TtsFailed(1, 1, code="TTS_FAILED", message="bad audio"))

    error = outbound_of_type(effects, "ERROR")
    ended = outbound_of_type(effects, "RESPONSE_END")
    assert (error.stage, error.interrupt) == ("TTS", False)
    assert ended.status == "FAILED"
    assert actor.state.turns[1].stage is TurnStage.FAILED
