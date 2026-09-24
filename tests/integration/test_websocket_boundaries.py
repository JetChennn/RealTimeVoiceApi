import asyncio
import base64
import json
import logging
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

    async def close(self, code: int, reason: str = "") -> None:
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


async def test_receiver_discards_audio_received_during_speaker_registration() -> None:
    payload = bytes(16000 * 2 * 3)
    registration_frame = json.dumps(
        {
            "type": "SPEAKER_REGISTER",
            "session_id": "s",
            "request_id": "register-1",
            "audio_format": "PCM16",
            "sample_rate": 16000,
            "channels": 1,
            "audio_b64": base64.b64encode(payload).decode(),
        }
    )
    discarded_audio = bytes(320)
    retained_audio = bytes([1, 0]) * 160

    class QueuedSocket:
        def __init__(self) -> None:
            self.frames: asyncio.Queue[str] = asyncio.Queue()
            self.received = 0
            self.second_frame_received = asyncio.Event()

        async def receive_text(self) -> str:
            frame = await self.frames.get()
            self.received += 1
            if self.received == 2:
                self.second_frame_received.set()
            return frame

    socket = QueuedSocket()
    socket.frames.put_nowait(registration_frame)
    registration_started = asyncio.Event()
    finish_registration = asyncio.Event()
    captured = []

    async def register(request) -> None:
        captured.append(request)
        registration_started.set()
        await finish_registration.wait()

    queue = BoundedByteQueue.audio(maxsize=64, max_bytes=16000 * 2 * 3)
    closed = False

    def request_close() -> None:
        nonlocal closed
        closed = True

    receiver = WebSocketReceiver(
        socket,
        "s",
        16000,
        queue,
        request_close,
        register,
    )
    receiver_task = asyncio.create_task(receiver.run())

    await asyncio.wait_for(registration_started.wait(), 1)
    socket.frames.put_nowait(
        json.dumps(
            {
                "type": "AUDIO_CHUNK",
                "session_id": "s",
                "sequence": 0,
                "audio_b64": base64.b64encode(discarded_audio).decode(),
            }
        )
    )
    await asyncio.wait_for(socket.second_frame_received.wait(), 1)
    assert queue.empty()

    finish_registration.set()
    await asyncio.sleep(0)
    socket.frames.put_nowait(
        json.dumps(
            {
                "type": "AUDIO_CHUNK",
                "session_id": "s",
                "sequence": 1,
                "audio_b64": base64.b64encode(retained_audio).decode(),
            }
        )
    )
    socket.frames.put_nowait(json.dumps({"type": "CLOSE_SESSION", "session_id": "s"}))

    await asyncio.wait_for(receiver_task, 1)

    assert captured[0].request_id == "register-1"
    assert captured[0].duration_ms == 3000
    assert queue.get_nowait() == retained_audio
    assert queue.empty()
    assert closed is True


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


async def test_protocol_error_write_race_does_not_escape_runtime_exception_group(caplog) -> None:
    services = SimpleNamespace(
        settings=SimpleNamespace(handshake_timeout_seconds=1),
        registry=ProtocolFailingRegistry(),
    )

    application_logger = logging.getLogger("realtime_voice")
    application_logger.addHandler(caplog.handler)
    try:
        with caplog.at_level(logging.WARNING, logger="realtime_voice.transport.websocket"):
            await serve_realtime(ErrorWriteRaceSocket(), services)
    finally:
        application_logger.removeHandler(caplog.handler)

    protocol_log = next(
        json.loads(record.message)
        for record in caplog.records
        if json.loads(record.message).get("event") == "protocol_error"
    )
    assert protocol_log["device_id"] == "device-1"
    assert protocol_log["session_id"] == "session-1"
    assert protocol_log["error_code"] == "INVALID_MESSAGE"


def test_websocket_handshake_timeout_returns_policy_error_and_close() -> None:
    app = create_app(Settings(_env_file=None, handshake_timeout_seconds=0.001))
    with TestClient(app).websocket_connect("/v1/realtime") as socket:
        error = socket.receive_json()
        assert error["code"] == "HANDSHAKE_TIMEOUT"
        with pytest.raises(WebSocketDisconnect) as closed:
            socket.receive_json()
    assert closed.value.code == 1008
