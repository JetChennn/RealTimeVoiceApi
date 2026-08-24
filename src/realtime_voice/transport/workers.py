"""Runtime-managed WebSocket receiver and sender workers."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from fastapi import WebSocket, WebSocketDisconnect

from realtime_voice.protocol.client_messages import AudioChunkMessage, CloseSession
from realtime_voice.protocol.decoder import decode_client_message, decode_pcm16
from realtime_voice.protocol.encoder import encode_server_message
from realtime_voice.protocol.errors import ProtocolViolation
from realtime_voice.session.runtime import SessionQueueOverloaded


class WebSocketReceiver:
    """Validate inbound frames and admit only ordered audio to the runtime queue."""

    def __init__(
        self,
        websocket: WebSocket,
        session_id: str,
        sample_rate: int,
        audio_queue: Any,
        request_close: Callable[[], None],
    ) -> None:
        self._websocket = websocket
        self._session_id = session_id
        self._sample_rate = sample_rate
        self._audio_queue = audio_queue
        self._request_close = request_close
        self._next_sequence = 0

    async def run(self) -> None:
        try:
            while True:
                message = decode_client_message(await self._websocket.receive_text())
                if isinstance(message, CloseSession):
                    self._require_session(message.session_id)
                    self._request_close()
                    return
                if not isinstance(message, AudioChunkMessage):
                    raise ProtocolViolation(
                        "INVALID_MESSAGE", "message is not valid after CREATE_SESSION"
                    )
                self._require_session(message.session_id)
                if message.sequence != self._next_sequence:
                    raise ProtocolViolation(
                        "AUDIO_SEQUENCE_GAP", "audio sequence must strictly increment"
                    )
                chunk = decode_pcm16(message, self._sample_rate)
                try:
                    self._audio_queue.put_nowait(chunk.pcm16)
                except (asyncio.QueueFull, SessionQueueOverloaded) as error:
                    raise ProtocolViolation(
                        "CLIENT_AUDIO_BACKPRESSURE", "client audio backlog exceeds three seconds"
                    ) from error
                self._next_sequence += 1
        except KeyError as error:
            raise ProtocolViolation(
                "INVALID_MESSAGE", "message must be a JSON text frame"
            ) from error
        except WebSocketDisconnect:
            self._request_close()

    def _require_session(self, session_id: str) -> None:
        if session_id != self._session_id:
            raise ProtocolViolation("SESSION_ID_MISMATCH", "message session_id does not match")


class WebSocketSender:
    """The single long-lived owner of normal WebSocket text writes."""

    def __init__(self, websocket: WebSocket, outbound: Any) -> None:
        self._websocket = websocket
        self._outbound = outbound

    async def run(self) -> None:
        while True:
            payload = encode_server_message(await self._outbound.get())
            try:
                await self._websocket.send_text(payload)
            except (WebSocketDisconnect, RuntimeError):
                return
