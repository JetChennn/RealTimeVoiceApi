from copy import deepcopy

import pytest

from realtime_voice.audio.vad import SpeechSegment
from realtime_voice.session.actor import CloseRuntime, QueueAsr, RecordStaleEvent, SendOutbound
from realtime_voice.session.events import (
    AsrFailed,
    AsrSucceeded,
    BerryCompleted,
    BerryDeltaReceived,
    BerryFailed,
    SessionDisconnected,
    SpeechSegmentReady,
    TtsChunkReceived,
    TtsCompleted,
    TtsFailed,
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

    effects = actor.handle(
        BerryDeltaReceived(session_id="s", turn_id=1, generation=0, delta="stale")
    )

    assert actor.state == before
    assert_only_stale(effects, "berry_generation")


def test_unknown_turn_is_stale_and_does_not_mutate_state() -> None:
    actor = actor_for_test()
    before = deepcopy(actor.state)

    effects = actor.handle(
        BerryCompleted(session_id="s", turn_id=99, generation=1, reply_text="ghost")
    )

    assert actor.state == before
    assert_only_stale(effects, "unknown_turn")


def test_old_tts_generation_is_stale_and_does_not_mutate_state() -> None:
    actor = actor_with_tts_turn()
    before = deepcopy(actor.state)

    effects = actor.handle(
        TtsChunkReceived(
            session_id="s",
            turn_id=1,
            generation=0,
            sequence=0,
            pcm16=bytes(2),
            finalize=False,
        )
    )

    assert actor.state == before
    assert_only_stale(effects, "tts_generation")


def test_duplicate_asr_segment_is_stale_even_if_first_result_was_empty() -> None:
    actor = actor_for_test()
    actor.handle(
        SpeechSegmentReady(
            session_id="s",
            segment=SpeechSegment(segment_id=55, pcm16_16k=bytes(2)),
        )
    )
    assert actor.handle(
        AsrSucceeded(session_id="s", segment_id=55, text="", audio_wav=valid_wav())
    ) == []

    effects = actor.handle(
        AsrSucceeded(
            session_id="s",
            segment_id=55,
            text="not empty",
            audio_wav=valid_wav(),
        )
    )

    assert actor.state.turns == {}
    assert_only_stale(effects, "asr_segment")


def test_asr_result_for_unpublished_segment_is_stale() -> None:
    actor = actor_for_test()
    actor.handle(
        SpeechSegmentReady(
            session_id="s",
            segment=SpeechSegment(segment_id=1, pcm16_16k=bytes(2)),
        )
    )
    before = deepcopy(actor.state)

    effects = actor.handle(
        AsrSucceeded(
            session_id="s",
            segment_id=99,
            text="ghost",
            audio_wav=valid_wav(),
        )
    )

    assert actor.state == before
    assert_only_stale(effects, "unknown_segment")



def test_event_for_another_session_is_stale() -> None:
    actor = actor_for_test()
    segment = SpeechSegment(segment_id=1, pcm16_16k=b"\x00\x00")

    effects = actor.handle(SpeechSegmentReady(session_id="another", segment=segment))

    assert_only_stale(effects, "session")


def test_disconnect_closes_once_and_later_events_are_stale() -> None:
    actor = actor_for_test()

    first = actor.handle(SessionDisconnected(session_id="s"))
    repeated = actor.handle(SessionDisconnected(session_id="s"))
    after_close = actor.handle(
        AsrSucceeded(
            session_id="s",
            segment_id=1,
            text="late",
            audio_wav=valid_wav(),
        )
    )

    assert actor.state.closing is True
    assert len(first) == 1 and isinstance(first[0], CloseRuntime)
    assert repeated == []
    assert_only_stale(after_close, "closing")


def test_first_unpublished_asr_result_is_stale() -> None:
    actor = actor_for_test()
    before = deepcopy(actor.state)

    effects = actor.handle(
        AsrSucceeded(
            session_id="s",
            segment_id=99,
            text="ghost",
            audio_wav=valid_wav(),
        )
    )

    assert actor.state == before
    assert_only_stale(effects, "unknown_segment")


def test_first_speech_segment_queues_asr_and_duplicate_speech_is_stale() -> None:
    actor = actor_for_test()
    segment = SpeechSegment(segment_id=1, pcm16_16k=bytes(2))

    first = actor.handle(SpeechSegmentReady(session_id="s", segment=segment))
    state_after_first = deepcopy(actor.state)
    duplicate = actor.handle(SpeechSegmentReady(session_id="s", segment=segment))

    assert len(first) == 1
    assert isinstance(first[0], QueueAsr)
    assert first[0].session_id == "s"
    assert first[0].segment is segment
    assert actor.state == state_after_first
    assert actor.state.pending_asr_segment_ids == {1}
    assert_only_stale(duplicate, "asr_segment")


def test_asr_failure_consumes_pending_segment_and_duplicate_is_stale() -> None:
    actor = actor_for_test()
    actor.handle(
        SpeechSegmentReady(
            session_id="s",
            segment=SpeechSegment(segment_id=8, pcm16_16k=bytes(2)),
        )
    )

    actor.handle(
        AsrFailed(
            session_id="s",
            segment_id=8,
            code="ASR_FAILED",
            message="failed",
        )
    )

    assert actor.state.pending_asr_segment_ids == set()
    assert_only_stale(
        actor.handle(
            AsrFailed(
                session_id="s",
                segment_id=8,
                code="ASR_FAILED",
                message="again",
            )
        ),
        "asr_segment",
    )



@pytest.mark.parametrize(
    "event_factory",
    [
        lambda: AsrSucceeded(
            session_id="other", segment_id=1, text="late", audio_wav=valid_wav()
        ),
        lambda: AsrFailed(
            session_id="other", segment_id=1, code="ASR_FAILED", message="late"
        ),
        lambda: BerryDeltaReceived(
            session_id="other", turn_id=1, generation=1, delta="late"
        ),
        lambda: BerryCompleted(
            session_id="other", turn_id=1, generation=1, reply_text="late"
        ),
        lambda: BerryFailed(
            session_id="other",
            turn_id=1,
            generation=1,
            code="BERRY_FAILED",
            message="late",
        ),
        lambda: TtsChunkReceived(
            session_id="other",
            turn_id=1,
            generation=1,
            sequence=0,
            pcm16=bytes(2),
            finalize=False,
        ),
        lambda: TtsCompleted(session_id="other", turn_id=1, generation=1),
        lambda: TtsFailed(
            session_id="other",
            turn_id=1,
            generation=1,
            code="TTS_FAILED",
            message="late",
        ),
        lambda: SessionDisconnected(session_id="other"),
    ],
)
def test_background_event_for_another_session_is_stale_before_any_mutation(
    event_factory,
) -> None:
    actor = actor_for_test()
    before = deepcopy(actor.state)

    effects = actor.handle(event_factory())

    assert actor.state == before
    assert_only_stale(effects, "session")


def test_disconnect_requires_session_identity() -> None:
    with pytest.raises(TypeError):
        SessionDisconnected()
