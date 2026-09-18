import base64
from copy import deepcopy

import pytest

from realtime_voice.session.actor import (
    RecordThinkerFallback,
    SendOutbound,
    StartThinker,
    StartTts,
)
from realtime_voice.session.events import (
    AsrFailed,
    ThinkerCompleted,
    ThinkerDeltaReceived,
    ThinkerFailed,
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
    start = next(effect for effect in effects if isinstance(effect, StartThinker))
    assert (start.turn_id, start.generation, start.text) == (1, 1, "你好")


def test_uninterrupted_turn_streams_text_then_audio_and_completes() -> None:
    actor = actor_for_test()
    recognize(actor, 7, "问题")

    delta = outbound_of_type(
        actor.handle(ThinkerDeltaReceived(session_id="s", turn_id=1, generation=1, delta="答")),
        "TEXT_DELTA",
    )
    completed = actor.handle(
        ThinkerCompleted(session_id="s", turn_id=1, generation=1, reply_text="答案", tone="温柔")
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


def test_thinker_failure_returns_fallback_text_and_starts_tts() -> None:
    actor = actor_for_test()
    recognize(actor, 1, "one")

    effects = actor.handle(
        ThinkerFailed(
            session_id="s",
            turn_id=1,
            generation=1,
            code="THINKER_FAILED",
            message="bad stream",
        )
    )

    text_end = outbound_of_type(effects, "TEXT_END")
    start_tts = next(effect for effect in effects if isinstance(effect, StartTts))
    recorded = next(effect for effect in effects if isinstance(effect, RecordThinkerFallback))
    assert (text_end.text, text_end.interrupt) == ("保底一", False)
    assert (start_tts.reply_text, start_tts.tone) == ("保底一", "平和")
    assert (recorded.code, recorded.tts_requested) == ("THINKER_FAILED", True)
    assert actor.state.turns[1].stage is TurnStage.STREAMING_TTS
    assert actor.state.turns[1].thinker_fallback_used is True
    assert actor.state.turns[1].thinker_failure_code == "THINKER_FAILED"
    assert not any(
        isinstance(effect, SendOutbound) and effect.message.type in {"ERROR", "RESPONSE_END"}
        for effect in effects
    )


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


def test_empty_thinker_delta_is_ignored_without_mutating_turn() -> None:
    actor = actor_for_test()
    recognize(actor, 1, "question")
    before = deepcopy(actor.state)

    effects = actor.handle(ThinkerDeltaReceived(session_id="s", turn_id=1, generation=1, delta=""))

    assert effects == []
    assert actor.state == before


@pytest.mark.parametrize("reply_text", ["", "   \t"])
def test_empty_thinker_completion_uses_fallback(reply_text: str) -> None:
    actor = actor_for_test()
    recognize(actor, 1, "question")

    effects = actor.handle(
        ThinkerCompleted(
            session_id="s",
            turn_id=1,
            generation=1,
            reply_text=reply_text,
            tone="温柔",
        )
    )

    text_end = outbound_of_type(effects, "TEXT_END")
    start_tts = next(effect for effect in effects if isinstance(effect, StartTts))
    assert (text_end.text, text_end.interrupt) == ("保底一", False)
    assert start_tts.reply_text == "保底一"
    assert actor.state.active_llm_turn_id is None
    assert actor.state.turns[1].stage is TurnStage.STREAMING_TTS
    assert actor.state.turns[1].reply_text == "保底一"


def test_disabled_thinker_fallback_preserves_failed_response() -> None:
    actor = actor_for_test(fallback_enabled=False)
    recognize(actor, 1, "question")

    effects = actor.handle(
        ThinkerFailed(
            session_id="s",
            turn_id=1,
            generation=1,
            code="THINKER_FAILED",
            message="bad stream",
        )
    )

    assert outbound_of_type(effects, "ERROR").code == "THINKER_FAILED"
    assert outbound_of_type(effects, "RESPONSE_END").status == "FAILED"
    assert actor.state.turns[1].stage is TurnStage.FAILED


def test_thinker_fallback_uses_the_selector_once_per_turn() -> None:
    selections = 0

    def select_last(texts: tuple[str, ...]) -> str:
        nonlocal selections
        selections += 1
        return texts[-1]

    actor = actor_for_test(fallback_selector=select_last)
    recognize(actor, 1, "question")
    actor.handle(ThinkerDeltaReceived(session_id="s", turn_id=1, generation=1, delta="半句话"))

    effects = actor.handle(
        ThinkerFailed(
            session_id="s",
            turn_id=1,
            generation=1,
            code="THINKER_REPLY_TIMEOUT",
            message="timed out",
        )
    )

    assert selections == 1
    assert outbound_of_type(effects, "TEXT_END").text == "保底三"
    assert next(effect for effect in effects if isinstance(effect, StartTts)).reply_text == "保底三"
