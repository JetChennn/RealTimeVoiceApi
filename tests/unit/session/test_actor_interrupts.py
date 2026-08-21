from realtime_voice.session.actor import (
    RecordDiscardedAudio,
    SendOutbound,
    StartBerry,
    StartNextBerry,
    StartTts,
)
from realtime_voice.session.events import (
    BerryCompleted,
    BerryDeltaReceived,
    TtsChunkReceived,
)
from realtime_voice.session.state import TurnStage
from tests.unit.session.conftest import (
    actor_for_test,
    actor_with_streaming_turn,
    actor_with_tts_turn,
    outbound_messages,
    outbound_of_type,
    recognize,
)


def test_new_turn_interrupts_llm_once_but_keeps_old_text_flow() -> None:
    actor = actor_with_streaming_turn()

    effects = recognize(actor, 2, "等一下")
    repeated = recognize(actor, 3, "再问")
    delta = actor.handle(
        BerryDeltaReceived(session_id="s", turn_id=1, generation=1, delta="旧文本")
    )

    assert actor.state.turns[1].interrupted is True
    assert outbound_of_type(effects, "TURN_STATE").turn_id == 1
    assert [message.turn_id for message in outbound_messages(repeated) if message.type == "TURN_STATE"] == [2]
    assert outbound_of_type(delta, "TEXT_DELTA").interrupt is True


def test_quick_turns_run_berry_fifo_and_interrupted_turns_skip_tts() -> None:
    actor = actor_for_test()
    first = recognize(actor, 101, "one")
    second = recognize(actor, 102, "two")
    third = recognize(actor, 103, "three")

    first_done = actor.handle(
        BerryCompleted(session_id="s", turn_id=1, generation=1, reply_text="reply-one")
    )
    second_done = actor.handle(
        BerryCompleted(session_id="s", turn_id=2, generation=1, reply_text="reply-two")
    )
    third_done = actor.handle(
        BerryCompleted(session_id="s", turn_id=3, generation=1, reply_text="reply-three")
    )

    starts = [effect for effects in (first, second, third) for effect in effects if isinstance(effect, StartBerry)]
    next_starts = [
        effect
        for effects in (first_done, second_done)
        for effect in effects
        if isinstance(effect, StartNextBerry)
    ]
    assert [effect.turn_id for effect in starts + next_starts] == [1, 2, 3]
    assert [effect.interrupt_first for effect in next_starts] == [True, True]
    assert not any(isinstance(effect, StartTts) for effect in first_done + second_done)
    assert outbound_of_type(first_done, "TEXT_END").interrupt is True
    assert outbound_of_type(first_done, "RESPONSE_END").status == "INTERRUPTED"
    assert any(isinstance(effect, StartTts) for effect in third_done)


def test_interrupted_tts_chunk_is_counted_and_not_sent() -> None:
    actor = actor_with_tts_turn(interrupted=True)

    effects = actor.handle(
        TtsChunkReceived(
            session_id="s",
            turn_id=1,
            generation=1,
            sequence=0,
            pcm16=bytes(2),
            finalize=False,
        )
    )

    assert not any(isinstance(effect, SendOutbound) for effect in effects)
    discarded = next(effect for effect in effects if isinstance(effect, RecordDiscardedAudio))
    assert (discarded.turn_id, discarded.byte_count) == (1, 2)


def test_new_llm_starts_without_waiting_for_interrupted_tts_to_drain() -> None:
    actor = actor_with_tts_turn()

    effects = recognize(actor, 2, "new")

    assert actor.state.turns[1].interrupted is True
    start = next(effect for effect in effects if isinstance(effect, StartBerry))
    assert start.turn_id == 2
    assert actor.state.turns[1].stage is TurnStage.STREAMING_TTS


def test_empty_interrupted_berry_completion_fails_and_starts_next_fifo_turn() -> None:
    actor = actor_for_test()
    recognize(actor, 1, "one")
    recognize(actor, 2, "two")

    effects = actor.handle(
        BerryCompleted(
            session_id="s",
            turn_id=1,
            generation=1,
            reply_text="   ",
        )
    )

    error = outbound_of_type(effects, "ERROR")
    ended = outbound_of_type(effects, "RESPONSE_END")
    next_start = next(effect for effect in effects if isinstance(effect, StartNextBerry))
    assert (error.code, error.interrupt) == ("BERRY_EMPTY_REPLY", True)
    assert (ended.status, ended.interrupt) == ("FAILED", True)
    assert (next_start.turn_id, next_start.interrupt_first) == (2, True)
    assert actor.state.turns[1].stage is TurnStage.FAILED
    assert actor.state.turns[1].reply_text == ""
    assert actor.state.active_llm_turn_id == 2
    assert not any(isinstance(effect, StartTts) for effect in effects)
