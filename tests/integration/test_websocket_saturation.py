import asyncio
import base64
from typing import Literal

import pytest
from fastapi import WebSocket, WebSocketDisconnect
from fastapi.testclient import TestClient

from realtime_voice.config import Settings
from realtime_voice.main import create_app
from realtime_voice.protocol.client_messages import CreateSession
from realtime_voice.protocol.server_messages import TextDelta
from realtime_voice.session.actor import SendOutbound
from realtime_voice.session.registry import RuntimeFactory
from realtime_voice.session.runtime import BoundedByteQueue, SessionRuntime
from realtime_voice.session.state import SessionState
from realtime_voice.transport.workers import WebSocketReceiver, WebSocketSender


class BlockingWorker:
    async def run(self) -> None:
        await asyncio.Event().wait()


class NoopAsr:
    async def transcribe(self, _: bytes) -> str:
        return ""


class NoopBerry:
    async def delete_session(self, _: str, __: str) -> None:
        return None


class NoopTts:
    pass


class SaturateOutbound:
    def __init__(self, settings: Settings, kind: Literal["count", "bytes"]) -> None:
        self._settings = settings
        self._kind = kind
        self.runtime: SessionRuntime | None = None

    async def run(self) -> None:
        assert self.runtime is not None
        message = TextDelta(
            type="TEXT_DELTA",
            user_id=self.runtime.user_id,
            session_id=self.runtime.session_id,
            turn_id=1,
            interrupt=False,
            delta="x",
        )
        if self._kind == "count":
            for _ in range(self._settings.session_outbound_queue_size):
                await self.runtime.execute_effect(SendOutbound(message))
            return

        remaining = (
            self._settings.session_outbound_queue_max_bytes - self.runtime.outbound.queued_bytes
        )
        json_overhead = len(message.model_dump_json(exclude_none=True).encode()) - 1
        fill = message.model_copy(update={"delta": "x" * (remaining - json_overhead)})
        await self.runtime.execute_effect(SendOutbound(fill))
        await self.runtime.execute_effect(SendOutbound(message))


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


def _audio(sequence: int, duration_ms: int) -> dict[str, object]:
    pcm16 = bytes(16000 * 2 * duration_ms // 1000)
    return {
        "type": "AUDIO_CHUNK",
        "session_id": "session-1",
        "sequence": sequence,
        "audio_b64": base64.b64encode(pcm16).decode(),
    }


def _controlled_runtime_factory(
    settings: Settings,
    *,
    outbound_saturation: Literal["count", "bytes"] | None = None,
) -> RuntimeFactory:
    def factory(create: CreateSession, websocket: WebSocket) -> SessionRuntime:
        audio = BoundedByteQueue.audio(
            maxsize=settings.session_audio_queue_size,
            max_bytes=int(create.sample_rate * 2 * settings.session_audio_queue_max_seconds),
        )
        outbound = BoundedByteQueue.outbound(
            maxsize=settings.session_outbound_queue_size,
            max_bytes=settings.session_outbound_queue_max_bytes,
        )
        runtime: SessionRuntime
        receiver = WebSocketReceiver(
            websocket,
            create.session_id,
            create.sample_rate,
            audio,
            lambda: runtime.request_close(),
        )
        producer = (
            BlockingWorker()
            if outbound_saturation is None
            else SaturateOutbound(settings, outbound_saturation)
        )
        sender = (
            WebSocketSender(websocket, outbound)
            if outbound_saturation is None
            else BlockingWorker()
        )
        runtime = SessionRuntime(
            state=SessionState(
                user_id=create.device_id,
                session_id=create.session_id,
                sample_rate=create.sample_rate,
            ),
            asr_client=NoopAsr(),
            berry_client=NoopBerry(),
            tts_client=NoopTts(),
            receiver=receiver,
            vad_worker=producer,
            sender=sender,
            event_queue_size=settings.session_event_queue_size,
            audio_queue_size=settings.session_audio_queue_size,
            asr_queue_size=settings.session_asr_queue_size,
            outbound_queue_size=settings.session_outbound_queue_size,
            audio_queue_max_seconds=settings.session_audio_queue_max_seconds,
            outbound_queue_max_bytes=settings.session_outbound_queue_max_bytes,
            audio_queue=audio,
            outbound_queue=outbound,
            berry_cleanup_timeout=settings.berry_cleanup_timeout_seconds,
            tts_drain_timeout=settings.tts_drain_timeout_seconds,
        )
        if isinstance(producer, SaturateOutbound):
            producer.runtime = runtime
        return runtime

    return factory


@pytest.mark.parametrize(
    ("chunks", "duration_ms"),
    [(65, 10), (7, 500)],
    ids=["64-message-count", "3-second-duration"],
)
def test_websocket_closes_at_actual_inbound_audio_boundaries(chunks: int, duration_ms: int) -> None:
    settings = Settings(_env_file=None)
    app = create_app(settings, runtime_factory=_controlled_runtime_factory(settings))

    with TestClient(app).websocket_connect("/v1/realtime") as websocket:
        websocket.send_json(_create())
        assert websocket.receive_json()["type"] == "SESSION_CREATED"
        for sequence in range(chunks):
            actual_duration = 10 if duration_ms == 500 and sequence == chunks - 1 else duration_ms
            websocket.send_json(_audio(sequence, actual_duration))
        assert websocket.receive_json()["code"] == "CLIENT_AUDIO_BACKPRESSURE"
        with pytest.raises(WebSocketDisconnect) as closed:
            websocket.receive_json()

    assert closed.value.code == 1008


@pytest.mark.parametrize("kind", ["count", "bytes"])
def test_websocket_closes_at_actual_outbound_boundaries(kind: Literal["count", "bytes"]) -> None:
    settings = Settings(_env_file=None)
    app = create_app(
        settings,
        runtime_factory=_controlled_runtime_factory(settings, outbound_saturation=kind),
    )

    with TestClient(app).websocket_connect("/v1/realtime") as websocket:
        websocket.send_json(_create())
        assert websocket.receive_json()["code"] == "SLOW_CLIENT"
        with pytest.raises(WebSocketDisconnect) as closed:
            websocket.receive_json()

    assert closed.value.code == 1008
