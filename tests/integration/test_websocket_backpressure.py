import base64

from fastapi.testclient import TestClient

from realtime_voice.config import Settings
from realtime_voice.main import create_app


def _create() -> dict[str, object]:
    return {
        "type": "CREATE_SESSION",
        "protocol_version": 1,
        "device_id": "device-1",
        "session_id": "session-1",
        "audio_format": "PCM16",
        "audio_transport": "BASE64_JSON",
        "sample_rate": 16000,
        "channels": 1,
    }


def test_websocket_rejects_an_audio_sequence_gap_after_negotiation() -> None:
    """Removing receiver sequence validation would accept a missing audio chunk."""
    app = create_app(Settings(_env_file=None))
    audio = base64.b64encode(bytes(320)).decode()

    with TestClient(app).websocket_connect("/v1/realtime") as websocket:
        websocket.send_json(_create())
        created = websocket.receive_json()
        websocket.send_json(
            {
                "type": "AUDIO_CHUNK",
                "session_id": "session-1",
                "sequence": 2,
                "audio_b64": audio,
            }
        )
        error = websocket.receive_json()

    assert created["type"] == "SESSION_CREATED"
    assert created["sample_rate"] == 16000
    assert error["code"] == "AUDIO_SEQUENCE_GAP"
