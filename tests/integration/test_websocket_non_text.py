import pytest
from fastapi import WebSocketDisconnect
from fastapi.testclient import TestClient

from realtime_voice.config import Settings
from realtime_voice.main import create_app
from realtime_voice.protocol.errors import ProtocolViolation
from realtime_voice.transport.workers import WebSocketReceiver


class BinaryFrame:
    async def receive_text(self) -> str:
        raise KeyError("text")


async def test_receiver_normalizes_binary_frame_to_invalid_message() -> None:
    """A binary frame must not leak a framework KeyError through the runtime."""
    receiver = WebSocketReceiver(BinaryFrame(), "session-1", 16000, object(), lambda: None)

    with pytest.raises(ProtocolViolation, match="INVALID_MESSAGE"):
        await receiver.run()


def test_websocket_normalizes_binary_handshake_to_policy_error() -> None:
    with TestClient(create_app(Settings(_env_file=None))).websocket_connect(
        "/v1/realtime"
    ) as websocket:
        websocket.send_bytes(b"not text")
        assert websocket.receive_json()["code"] == "INVALID_MESSAGE"
        with pytest.raises(WebSocketDisconnect) as closed:
            websocket.receive_json()

    assert closed.value.code == 1008


def test_websocket_normalizes_binary_active_frame_to_policy_error() -> None:
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
    with TestClient(create_app(Settings(_env_file=None))).websocket_connect(
        "/v1/realtime"
    ) as websocket:
        websocket.send_json(create)
        assert websocket.receive_json()["type"] == "SESSION_CREATED"
        websocket.send_bytes(b"not text")
        assert websocket.receive_json()["code"] == "INVALID_MESSAGE"
        with pytest.raises(WebSocketDisconnect) as closed:
            websocket.receive_json()

    assert closed.value.code == 1008
