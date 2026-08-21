import base64
import json

import pytest
from pydantic import ValidationError

from realtime_voice.protocol.encoder import encode_server_message
from realtime_voice.protocol.server_messages import (
    AsrResult,
    AudioDelta,
    ErrorMessage,
    ResponseEnd,
    SessionCreated,
    TextDelta,
    TextEnd,
    TurnState,
)


@pytest.mark.parametrize(
    "message, expected",
    [
        (
            SessionCreated(
                type="SESSION_CREATED",
                protocol_version=1,
                user_id="device-01",
                session_id="session-100",
                turn_id=0,
                interrupt=False,
                audio_format="PCM16",
                audio_transport="BASE64_JSON",
                sample_rate=16000,
                channels=1,
            ),
            {"protocol_version": 1, "audio_format": "PCM16", "audio_transport": "BASE64_JSON"},
        ),
        (
            AsrResult(
                type="ASR_RESULT",
                user_id="device-01",
                session_id="session-100",
                turn_id=1,
                interrupt=False,
                text="recognized",
            ),
            {"text": "recognized"},
        ),
        (
            TextDelta(
                type="TEXT_DELTA",
                user_id="device-01",
                session_id="session-100",
                turn_id=1,
                interrupt=False,
                delta="partial",
            ),
            {"delta": "partial"},
        ),
        (
            TextEnd(
                type="TEXT_END",
                user_id="device-01",
                session_id="session-100",
                turn_id=1,
                interrupt=False,
                text="complete",
            ),
            {"text": "complete"},
        ),
        (
            AudioDelta(
                type="AUDIO_DELTA",
                user_id="device-01",
                session_id="session-100",
                turn_id=1,
                interrupt=False,
                sequence=0,
                audio_format="PCM16",
                sample_rate=24000,
                channels=1,
                audio_b64=base64.b64encode(b"\x00\x00").decode(),
            ),
            {"sequence": 0, "audio_format": "PCM16", "sample_rate": 24000, "channels": 1},
        ),
        (
            TurnState(
                type="TURN_STATE",
                user_id="device-01",
                session_id="session-100",
                turn_id=1,
                interrupt=True,
                state="INTERRUPTED",
            ),
            {"state": "INTERRUPTED"},
        ),
        (
            ResponseEnd(
                type="RESPONSE_END",
                user_id="device-01",
                session_id="session-100",
                turn_id=1,
                interrupt=False,
                status="COMPLETED",
            ),
            {"status": "COMPLETED"},
        ),
        (
            ErrorMessage(
                type="ERROR",
                user_id="device-01",
                session_id="session-100",
                turn_id=1,
                interrupt=False,
                stage="TTS",
                code="TTS_STREAM_FAILED",
                message="stream failed",
                recoverable=True,
            ),
            {"stage": "TTS", "code": "TTS_STREAM_FAILED", "message": "stream failed"},
        ),
    ],
)
def test_encode_server_message_preserves_v1_contract(message: object, expected: dict[str, object]) -> None:
    encoded = json.loads(encode_server_message(message))

    assert {key: encoded[key] for key in ("type", "user_id", "session_id", "turn_id", "interrupt")} == {
        "type": message.type,
        "user_id": "device-01",
        "session_id": "session-100",
        "turn_id": message.turn_id,
        "interrupt": message.interrupt,
    }
    assert expected.items() <= encoded.items()


def test_session_created_requires_session_level_common_fields() -> None:
    with pytest.raises(ValidationError):
        SessionCreated(
            type="SESSION_CREATED",
            protocol_version=1,
            user_id="device-01",
            session_id="session-100",
            turn_id=1,
            interrupt=False,
            audio_format="PCM16",
            audio_transport="BASE64_JSON",
            sample_rate=16000,
            channels=1,
        )


def test_server_messages_forbid_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        TextDelta(
            type="TEXT_DELTA",
            user_id="device-01",
            session_id="session-100",
            turn_id=1,
            interrupt=False,
            delta="partial",
            unexpected=True,
        )
