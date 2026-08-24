import asyncio
import base64
import json
from types import SimpleNamespace

import pytest
from fastapi import WebSocketDisconnect
from fastapi.testclient import TestClient

from realtime_voice.config import Settings
from realtime_voice.main import create_app
from realtime_voice.protocol.errors import ProtocolViolation
from realtime_voice.protocol.server_messages import TextDelta
from realtime_voice.session.actor import SendOutbound
from realtime_voice.session.runtime import BoundedByteQueue, SlowClient
from realtime_voice.transport.websocket import serve_realtime
from realtime_voice.transport.workers import WebSocketReceiver, WebSocketSender
from tests.unit.session.test_runtime import make_runtime


class Frames:
    def __init__(self, frame: str) -> None:
        self.frame = frame

    async def receive_text(self) -> str:
        return self.frame


class DisconnectingSocket:
    async def send_text(self, _: str) -> None:
        raise WebSocketDisconnect(1006)


class ClosedSocket:
    async def send_text(self, _: str) -> None:
        raise RuntimeError("Cannot call send once a close message has been sent")


class ErrorWriteRaceSocket:
    async def accept(self) -> None:
        return None

    async def receive_text(self) -> str:
        return json.dumps(
            {
                "type": "CREATE_SESSION",
                "protocol_version": 1,
                "device_id": "device-1",
                "session_id": "session-1",
                "audio_format": "PCM16",
                "audio_transport": "BASE64_JSON",
                "sample_rate": 16000,
                "channels": 1,
            }
        )

    async def send_text(self, _: str) -> None:
        raise RuntimeError("Cannot call send once a close message has been sent")

    async def close(self, code: int) -> None:
        raise RuntimeError(f"socket already closed before {code}")


class ProtocolFailingRuntime:
    def __init__(self) -> None:
        self.outbound: asyncio.Queue[object] = asyncio.Queue()

    async def run(self) -> None:
        raise ExceptionGroup("runtime", [ProtocolViolation("INVALID_MESSAGE", "invalid")])


class ProtocolFailingRegistry:
    async def create(self, create: object, websocket: object) -> ProtocolFailingRuntime:
        return ProtocolFailingRuntime()


async def test_receiver_maps_actual_65th_queued_audio_message_to_backpressure() -> None:
    queue = BoundedByteQueue.audio(maxsize=64, max_bytes=16000 * 2 * 30)
    for _ in range(64):
        queue.put_nowait(bytes(2))
    frame = json.dumps(
        {
            "type": "AUDIO_CHUNK",
            "session_id": "s",
            "sequence": 0,
            "audio_b64": base64.b64encode(bytes(320)).decode(),
        }
    )
    receiver = WebSocketReceiver(Frames(frame), "s", 16000, queue, lambda: None)

    with pytest.raises(ProtocolViolation, match="CLIENT_AUDIO_BACKPRESSURE"):
        await receiver.run()


@pytest.mark.parametrize("kind", ["count", "bytes"])
async def test_runtime_reports_slow_client_without_waiting_for_outbound_capacity(kind: str) -> None:
    runtime, _ = make_runtime()
    message = TextDelta(
        type="TEXT_DELTA", user_id="u", session_id="s", turn_id=1, interrupt=False, delta="x"
    )
    if kind == "count":
        for _ in range(256):
            runtime.outbound.put_nowait(message)
    else:
        overhead = len(message.model_dump_json().encode()) - 1
        runtime.outbound.put_nowait(
            TextDelta(
                type="TEXT_DELTA",
                user_id="u",
                session_id="s",
                turn_id=1,
                interrupt=False,
                delta="x" * (8 * 1024 * 1024 - overhead),
            )
        )

    with pytest.raises(SlowClient):
        await runtime.execute_effect(SendOutbound(message))


async def test_sender_absorbs_disconnect_during_write() -> None:
    outbound = asyncio.Queue()
    await outbound.put(
        TextDelta(
            type="TEXT_DELTA", user_id="u", session_id="s", turn_id=1, interrupt=False, delta="x"
        )
    )
    await WebSocketSender(DisconnectingSocket(), outbound).run()


async def test_sender_absorbs_closed_socket_runtime_error_during_write() -> None:
    outbound = asyncio.Queue()
    await outbound.put(
        TextDelta(
            type="TEXT_DELTA", user_id="u", session_id="s", turn_id=1, interrupt=False, delta="x"
        )
    )
    await WebSocketSender(ClosedSocket(), outbound).run()


async def test_protocol_error_write_race_does_not_escape_runtime_exception_group() -> None:
    services = SimpleNamespace(
        settings=SimpleNamespace(handshake_timeout_seconds=1),
        registry=ProtocolFailingRegistry(),
    )

    await serve_realtime(ErrorWriteRaceSocket(), services)


def test_websocket_handshake_timeout_returns_policy_error_and_close() -> None:
    app = create_app(Settings(_env_file=None, handshake_timeout_seconds=0.001))
    with TestClient(app).websocket_connect("/v1/realtime") as socket:
        error = socket.receive_json()
        assert error["code"] == "HANDSHAKE_TIMEOUT"
        with pytest.raises(WebSocketDisconnect) as closed:
            socket.receive_json()
    assert closed.value.code == 1008
