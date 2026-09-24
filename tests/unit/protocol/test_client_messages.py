import json

import pytest
from pydantic import ValidationError

from realtime_voice.protocol.client_messages import (
    AudioChunkMessage,
    CloseSession,
    CreateSession,
    SpeakerRegister,
)
from realtime_voice.protocol.decoder import decode_client_message
from realtime_voice.protocol.errors import ProtocolViolation


def test_create_session_accepts_v1_pcm16() -> None:
    message = decode_client_message(
        json.dumps(
            {
                "type": "CREATE_SESSION",
                "protocol_version": 1,
                "device_id": "device-01",
                "session_id": "session-100",
                "audio_format": "PCM16",
                "audio_transport": "BASE64_JSON",
                "sample_rate": 16000,
                "channels": 1,
            }
        )
    )

    assert isinstance(message, CreateSession)


@pytest.mark.parametrize(
    ("payload", "error_code"),
    [
        ("{", "INVALID_JSON"),
        (json.dumps([]), "INVALID_MESSAGE"),
        (json.dumps({"type": "UNKNOWN"}), "INVALID_MESSAGE"),
        (
            json.dumps(
                {
                    "type": "CREATE_SESSION",
                    "protocol_version": 2,
                    "device_id": "device-01",
                    "session_id": "session-100",
                    "audio_format": "PCM16",
                    "audio_transport": "BASE64_JSON",
                    "sample_rate": 16000,
                    "channels": 1,
                }
            ),
            "INVALID_MESSAGE",
        ),
    ],
)
def test_decode_client_message_reports_stable_errors(payload: str, error_code: str) -> None:
    with pytest.raises(ProtocolViolation, match=error_code):
        decode_client_message(payload)


def test_audio_chunk_forbids_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        AudioChunkMessage.model_validate(
            {
                "type": "AUDIO_CHUNK",
                "session_id": "session-100",
                "sequence": 0,
                "audio_b64": "AA==",
                "unexpected": True,
            }
        )


def test_speaker_register_accepts_one_complete_pcm_recording() -> None:
    message = decode_client_message(
        json.dumps(
            {
                "type": "SPEAKER_REGISTER",
                "session_id": "session-100",
                "request_id": "register-1",
                "audio_format": "PCM16",
                "sample_rate": 24000,
                "channels": 1,
                "audio_b64": "AAAA",
            }
        )
    )

    assert isinstance(message, SpeakerRegister)


def test_close_session_requires_nonempty_session_id() -> None:
    with pytest.raises(ValidationError):
        CloseSession(type="CLOSE_SESSION", session_id="")
