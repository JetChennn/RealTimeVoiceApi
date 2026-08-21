from copy import deepcopy

from realtime_voice.audio.vad import SpeechSegment
from realtime_voice.session.actor import CloseRuntime, RecordStaleEvent, SendOutbound
from realtime_voice.session.events import (
    AsrSucceeded,
    BerryCompleted,
    BerryDeltaReceived,
    SessionDisconnected,
    SpeechSegmentReady,
    TtsChunkReceived,
)
from tests.helpers import valid_wav
from tests.unit.session.conftest import (
    actor_for_test,
    actor_with_streaming_turn,
    actor_with_tts_turn,
)


def assert_only_stale(effects, reason: str) -> None:
    assert not any(isinstance(effect, SendOutbound) for effect in effects)
    assert len(effects) == 1
    assert isinstance(effects[0], RecordStaleEvent)
    assert effects[0].reason == reason


def test_old_berry_generation_is_stale_and_does_not_mutate_state() -> None:
    actor = actor_with_streaming_turn()
    before = deepcopy(actor.state)

    effects = actor.handle(BerryDeltaReceived(1, generation=0, delta="stale"))

    assert actor.state == before
    assert_only_stale(effects, "berry_generation")


def test_unknown_turn_is_stale_and_does_not_mutate_state() -> None:
    actor = actor_for_test()
    before = deepcopy(actor.state)

    effects = actor.handle(BerryCompleted(99, generation=1, reply_text="ghost"))

    assert actor.state == before
    assert_only_stale(effects, "unknown_turn")


def test_old_tts_generation_is_stale_and_does_not_mutate_state() -> None:
    actor = actor_with_tts_turn()
    before = deepcopy(actor.state)

    effects = actor.handle(TtsChunkReceived(1, 0, 0, b"\x00\x00", False))

    assert actor.state == before
    assert_only_stale(effects, "tts_generation")


def test_duplicate_asr_segment_is_stale_even_if_first_result_was_empty() -> None:
    actor = actor_for_test()
    assert actor.handle(AsrSucceeded(55, "", valid_wav())) == []

    effects = actor.handle(AsrSucceeded(55, "not empty", valid_wav()))

    assert actor.state.turns == {}
    assert_only_stale(effects, "asr_segment")


def test_asr_result_for_unpublished_segment_is_stale() -> None:
    actor = actor_for_test()
    actor.handle(
        SpeechSegmentReady("s", SpeechSegment(segment_id=1, pcm16_16k=b"\x00\x00"))
    )
    before = deepcopy(actor.state)

    effects = actor.handle(AsrSucceeded(99, "ghost", valid_wav()))

    assert actor.state == before
    assert_only_stale(effects, "unknown_segment")



def test_event_for_another_session_is_stale() -> None:
    actor = actor_for_test()
    segment = SpeechSegment(segment_id=1, pcm16_16k=b"\x00\x00")

    effects = actor.handle(SpeechSegmentReady("another", segment))

    assert_only_stale(effects, "session")


def test_disconnect_closes_once_and_later_events_are_stale() -> None:
    actor = actor_for_test()

    first = actor.handle(SessionDisconnected("s"))
    repeated = actor.handle(SessionDisconnected("s"))
    after_close = actor.handle(AsrSucceeded(1, "late", valid_wav()))

    assert actor.state.closing is True
    assert len(first) == 1 and isinstance(first[0], CloseRuntime)
    assert repeated == []
    assert_only_stale(after_close, "closing")
