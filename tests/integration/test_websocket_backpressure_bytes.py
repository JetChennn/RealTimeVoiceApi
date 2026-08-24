import base64

from fastapi.testclient import TestClient

from realtime_voice.config import Settings
from realtime_voice.main import create_app


def test_websocket_closes_when_one_audio_chunk_exceeds_the_time_budget() -> None:
    """Removing byte-budget admission would let an over-budget client audio chunk through."""
    app = create_app(Settings(_env_file=None, session_audio_queue_max_seconds=0.005))
    create = {
        "type": "CREATE_SESSION",
        "protocol_version": 1,
        "device_id": "device-1",
        "session_id": "session-1",
        "audio_format": "PCM16",
        "audio_transport": "BASE64_JSON",
        "sample_rate": 16000,
        "channels": 1,
    }
    audio = base64.b64encode(bytes(320)).decode()

    with TestClient(app).websocket_connect("/v1/realtime") as websocket:
        websocket.send_json(create)
        assert websocket.receive_json()["type"] == "SESSION_CREATED"
        websocket.send_json(
            {
                "type": "AUDIO_CHUNK",
                "session_id": "session-1",
                "sequence": 0,
                "audio_b64": audio,
            }
        )
        error = websocket.receive_json()

    assert error["code"] == "CLIENT_AUDIO_BACKPRESSURE"
