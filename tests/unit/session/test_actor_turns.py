import base64
from copy import deepcopy

import pytest

from realtime_voice.session.actor import SendOutbound, StartBerry, StartTts
from realtime_voice.session.events import (
    AsrFailed,
    BerryCompleted,
    BerryDeltaReceived,
    BerryFailed,
    TtsChunkReceived,
    TtsCompleted,
    TtsFailed,
)
from realtime_voice.session.state import TurnStage
from tests.unit.session.conftest import (
    actor_for_test,
    actor_with_tts_turn,
    outbound_of_type,
    queue_segment,
    recognize,
)


def test_empty_asr_creates_no_turn() -> None:
    actor = actor_for_test()

    effects = recognize(actor, 41, "")

    assert actor.state.next_turn_id == 1
    assert actor.state.turns == {}
    assert effects == []


def test_empty_asr_does_not_interrupt_an_active_turn() -> None:
    actor = actor_for_test()
    recognize(actor, 1, "active")

    effects = recognize(actor, 2, "")

    assert effects == []
    assert actor.state.turns[1].interrupted is False
    assert actor.state.next_turn_id == 2


def test_non_empty_asr_allocates_public_turn_independently_from_segment_id() -> None:
    actor = actor_for_test()

    effects = recognize(actor, 41, "你好")

    assert actor.state.next_turn_id == 2
    assert actor.state.turns[1].asr_text == "你好"
    assert outbound_of_type(effects, "ASR_RESULT").turn_id == 1
    start = next(effect for effect in effects if isinstance(effect, StartBerry))
    assert (start.turn_id, start.generation, start.text) == (1, 1, "你好")


def test_uninterrupted_turn_streams_text_then_audio_and_completes() -> None:
    actor = actor_for_test()
    recognize(actor, 7, "问题")

    delta = outbound_of_type(
        actor.handle(
            BerryDeltaReceived(session_id="s", turn_id=1, generation=1, delta="答")
        ),
        "TEXT_DELTA",
    )
    completed = actor.handle(
        BerryCompleted(session_id="s", turn_id=1, generation=1, reply_text="答案")
    )
    text_end = outbound_of_type(completed, "TEXT_END")
    start_tts = next(effect for effect in completed if isinstance(effect, StartTts))
    audio = outbound_of_type(
        actor.handle(
            TtsChunkReceived(
                session_id="s",
                turn_id=1,
                generation=1,
                sequence=8,
                pcm16=bytes([1, 0]),
                finalize=False,
            )
        ),
        "AUDIO_DELTA",
    )
    ended = outbound_of_type(
        actor.handle(TtsCompleted(session_id="s", turn_id=1, generation=1)),
        "RESPONSE_END",
    )

    assert (delta.delta, delta.interrupt) == ("答", False)
    assert (text_end.text, text_end.interrupt) == ("答案", False)
    assert (start_tts.turn_id, start_tts.generation, start_tts.reply_text) == (1, 1, "答案")
    assert (audio.sequence, audio.sample_rate, audio.interrupt) == (0, 16000, False)
    assert base64.b64decode(audio.audio_b64) == b"\x01\x00"
    assert (ended.status, ended.interrupt) == ("COMPLETED", False)
    assert actor.state.turns[1].stage is TurnStage.COMPLETED


def test_asr_failure_emits_session_level_recoverable_error_without_turn() -> None:
    actor = actor_for_test()

    queue_segment(actor, 9)
    effects = actor.handle(
        AsrFailed(
            session_id="s",
            segment_id=9,
            code="ASR_TIMEOUT",
            message="timed out",
        )
    )

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
    recognize(actor, 1, "one")

    effects = actor.handle(
        BerryFailed(
            session_id="s",
            turn_id=1,
            generation=1,
            code="BERRY_FAILED",
            message="bad stream",
        )
    )

    error = outbound_of_type(effects, "ERROR")
    ended = outbound_of_type(effects, "RESPONSE_END")
    assert (error.stage, error.interrupt) == ("LLM", False)
    assert (ended.status, ended.interrupt) == ("FAILED", False)
    assert actor.state.turns[1].stage is TurnStage.FAILED


def test_tts_failure_ends_turn_failed() -> None:
    actor = actor_with_tts_turn()

    effects = actor.handle(
        TtsFailed(
            session_id="s",
            turn_id=1,
            generation=1,
            code="TTS_FAILED",
            message="bad audio",
        )
    )

    error = outbound_of_type(effects, "ERROR")
    ended = outbound_of_type(effects, "RESPONSE_END")
    assert (error.stage, error.interrupt) == ("TTS", False)
    assert ended.status == "FAILED"
    assert actor.state.turns[1].stage is TurnStage.FAILED


def test_empty_berry_delta_is_ignored_without_mutating_turn() -> None:
    actor = actor_for_test()
    recognize(actor, 1, "question")
    before = deepcopy(actor.state)

    effects = actor.handle(
        BerryDeltaReceived(session_id="s", turn_id=1, generation=1, delta="")
    )

    assert effects == []
    assert actor.state == before


@pytest.mark.parametrize("reply_text", ["", "   \t"])
def test_empty_berry_completion_fails_atomically(reply_text: str) -> None:
    actor = actor_for_test()
    recognize(actor, 1, "question")

    effects = actor.handle(
        BerryCompleted(
            session_id="s",
            turn_id=1,
            generation=1,
            reply_text=reply_text,
        )
    )

    error = outbound_of_type(effects, "ERROR")
    ended = outbound_of_type(effects, "RESPONSE_END")
    assert (error.stage, error.code, error.interrupt) == (
        "LLM",
        "BERRY_EMPTY_REPLY",
        False,
    )
    assert (ended.status, ended.interrupt) == ("FAILED", False)
    assert actor.state.active_llm_turn_id is None
    assert actor.state.turns[1].stage is TurnStage.FAILED
    assert actor.state.turns[1].reply_text == ""
    assert not any(
        isinstance(effect, SendOutbound) and effect.message.type == "TEXT_END"
        for effect in effects
    )
    assert not any(isinstance(effect, StartTts) for effect in effects)
