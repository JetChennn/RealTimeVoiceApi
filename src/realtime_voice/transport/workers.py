"""Runtime-managed WebSocket receiver and sender workers."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress
from typing import Any

from fastapi import WebSocket, WebSocketDisconnect

from realtime_voice.protocol.client_messages import AudioChunkMessage, CloseSession, SpeakerRegister
from realtime_voice.protocol.decoder import (
    DecodedSpeakerRegister,
    decode_client_message,
    decode_pcm16,
    decode_speaker_register,
)
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
        register_speaker: Callable[[DecodedSpeakerRegister], Awaitable[None]] | None = None,
    ) -> None:
        self._websocket = websocket
        self._session_id = session_id
        self._sample_rate = sample_rate
        self._audio_queue = audio_queue
        self._request_close = request_close
        self._register_speaker = register_speaker
        self._next_sequence = 0

    async def run(self) -> None:
        try:
            while True:
                message = decode_client_message(await self._websocket.receive_text())
                if isinstance(message, CloseSession):
                    self._require_session(message.session_id)
                    self._request_close()
                    return
                if isinstance(message, SpeakerRegister):
                    self._require_session(message.session_id)
                    if self._register_speaker is None:
                        raise ProtocolViolation(
                            "SPEAKER_REGISTER_UNAVAILABLE",
                            "speaker registration is unavailable for this session",
                        )
                    registration = decode_speaker_register(message, self._sample_rate)
                    if not await self._register_while_discarding_audio(registration):
                        return
                    continue
                if not isinstance(message, AudioChunkMessage):
                    raise ProtocolViolation(
                        "INVALID_MESSAGE", "message is not valid after CREATE_SESSION"
                    )
                self._require_session(message.session_id)
                self._validate_audio_sequence(message)
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

    async def _register_while_discarding_audio(
        self, registration: DecodedSpeakerRegister
    ) -> bool:
        """Keep reading the socket and discard every audio frame received while registering."""
        if self._register_speaker is None:  # Guarded by run(); keeps the helper total.
            raise ProtocolViolation(
                "SPEAKER_REGISTER_UNAVAILABLE",
                "speaker registration is unavailable for this session",
            )

        registration_task = asyncio.create_task(self._register_speaker(registration))
        try:
            while not registration_task.done():
                receive_task = asyncio.create_task(self._websocket.receive_text())
                done, _ = await asyncio.wait(
                    (registration_task, receive_task),
                    return_when=asyncio.FIRST_COMPLETED,
                )

                # If both complete in the same loop turn, the frame arrived inside the
                # registration boundary and must still be discarded.
                if receive_task in done:
                    message = decode_client_message(receive_task.result())
                    if isinstance(message, CloseSession):
                        self._require_session(message.session_id)
                        self._request_close()
                        registration_task.cancel()
                        with suppress(asyncio.CancelledError):
                            await registration_task
                        return False
                    if isinstance(message, SpeakerRegister):
                        raise ProtocolViolation(
                            "SPEAKER_REGISTER_IN_PROGRESS",
                            "another speaker registration is already in progress",
                        )
                    if not isinstance(message, AudioChunkMessage):
                        raise ProtocolViolation(
                            "INVALID_MESSAGE", "message is not valid after CREATE_SESSION"
                        )
                    self._require_session(message.session_id)
                    self._validate_audio_sequence(message)
                    # Decode for the same protocol validation as normal audio, but never
                    # enqueue it: audio received during registration is intentionally lost.
                    decode_pcm16(message, self._sample_rate)
                    self._next_sequence += 1
                    continue

                receive_task.cancel()
                with suppress(asyncio.CancelledError):
                    await receive_task

            await registration_task
            return True
        except BaseException:
            if not registration_task.done():
                registration_task.cancel()
                with suppress(asyncio.CancelledError):
                    await registration_task
            raise

    def _validate_audio_sequence(self, message: AudioChunkMessage) -> None:
        if message.sequence != self._next_sequence:
            raise ProtocolViolation(
                "AUDIO_SEQUENCE_GAP", "audio sequence must strictly increment"
            )

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
