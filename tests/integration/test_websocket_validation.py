import base64

from fastapi.testclient import TestClient

from realtime_voice.config import Settings
from realtime_voice.main import create_app


def test_websocket_rejects_audio_for_a_different_session() -> None:
    """Removing receiver session validation would route foreign-session audio."""
    app = create_app(Settings(_env_file=None))
    create = {
        "type": "CREATE_SESSION",
        "protocol_version": 1,
        "device_id": "device-1",
        "session_id": "session-1",
        "audio_format": "PCM16",
        "audio_transport": "BASE64_JSON",
        "sample_rate": 24000,
        "channels": 1,
    }

    with TestClient(app).websocket_connect("/v1/realtime") as websocket:
        websocket.send_json(create)
        created = websocket.receive_json()
        websocket.send_json(
            {
                "type": "AUDIO_CHUNK",
                "session_id": "other-session",
                "sequence": 0,
                "audio_b64": base64.b64encode(bytes(480)).decode(),
            }
        )
        error = websocket.receive_json()

    assert created["sample_rate"] == 24000
    assert created["protocol_version"] == 1
    assert error["code"] == "SESSION_ID_MISMATCH"
