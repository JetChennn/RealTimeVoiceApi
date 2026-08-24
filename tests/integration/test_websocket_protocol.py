from fastapi.testclient import TestClient

from realtime_voice.config import Settings
from realtime_voice.main import create_app


def test_websocket_rejects_a_non_create_first_message() -> None:
    """A missing first-message gate would admit audio before a session exists."""
    app = create_app(Settings(_env_file=None))

    with TestClient(app).websocket_connect("/v1/realtime") as websocket:
        websocket.send_json(
            {
                "type": "AUDIO_CHUNK",
                "session_id": "session-1",
                "sequence": 0,
                "audio_b64": "AAAA",
            }
        )

        error = websocket.receive_json()

    assert error["type"] == "ERROR"
    assert error["code"] == "CREATE_SESSION_REQUIRED"
    assert error["stage"] == "TRANSPORT"
    assert error["turn_id"] == 0
