from realtime_voice.session.actor import SendOutbound, SessionActor, SessionEffect
from realtime_voice.session.events import AsrSucceeded, BerryCompleted
from realtime_voice.session.state import SessionState
from tests.helpers import valid_wav


def actor_for_test() -> SessionActor:
    return SessionActor(SessionState(user_id="u", session_id="s", sample_rate=16000))


def actor_with_streaming_turn(segment_id: int = 1) -> SessionActor:
    actor = actor_for_test()
    actor.handle(AsrSucceeded(segment_id, f"text-{segment_id}", valid_wav()))
    return actor


def actor_with_tts_turn(interrupted: bool = False) -> SessionActor:
    actor = actor_with_streaming_turn()
    actor.handle(BerryCompleted(1, generation=1, reply_text="reply"))
    actor.state.turns[1].interrupted = interrupted
    return actor


def outbound_of_type(effects: list[SessionEffect], message_type: str):
    return next(
        effect.message
        for effect in effects
        if isinstance(effect, SendOutbound) and effect.message.type == message_type
    )


def outbound_messages(effects: list[SessionEffect]):
    return [effect.message for effect in effects if isinstance(effect, SendOutbound)]
