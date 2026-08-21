"""Decode V1 JSON and Base64 audio frames at the protocol boundary."""

import base64
import binascii
import json
from dataclasses import dataclass

from pydantic import TypeAdapter, ValidationError

from realtime_voice.protocol.client_messages import AudioChunkMessage, ClientMessage
from realtime_voice.protocol.errors import ProtocolViolation

_CLIENT_MESSAGE_ADAPTER = TypeAdapter(ClientMessage)


@dataclass(frozen=True, slots=True)
class DecodedAudioChunk:
    """Validated PCM16 bytes with their transport metadata."""

    sequence: int
    timestamp_ms: int | None
    pcm16: bytes
    duration_ms: float


def decode_client_message(raw: str) -> ClientMessage:
    """Decode and validate a client JSON text frame."""
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ProtocolViolation("INVALID_JSON", "message is not valid JSON") from exc

    if not isinstance(payload, dict):
        raise ProtocolViolation("INVALID_MESSAGE", "message must be a JSON object")

    try:
        return _CLIENT_MESSAGE_ADAPTER.validate_python(payload)
    except ValidationError as exc:
        raise ProtocolViolation("INVALID_MESSAGE", "message does not match protocol V1") from exc


def decode_pcm16(message: AudioChunkMessage, sample_rate: int) -> DecodedAudioChunk:
    """Decode and validate an audio chunk's Base64-encoded PCM16 payload."""
    try:
        payload = base64.b64decode(message.audio_b64, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ProtocolViolation("INVALID_BASE64", "audio_b64 is not valid Base64") from exc

    if not payload or len(payload) % 2:
        raise ProtocolViolation("PCM16_BYTE_ALIGNMENT", "PCM16 must contain complete int16 samples")

    duration_ms = len(payload) / 2 / sample_rate * 1000.0
    if not 10.0 <= duration_ms <= 500.0:
        raise ProtocolViolation("AUDIO_CHUNK_DURATION", "audio chunk must be 10-500 ms")

    return DecodedAudioChunk(
        sequence=message.sequence,
        timestamp_ms=message.timestamp_ms,
        pcm16=payload,
        duration_ms=duration_ms,
    )
