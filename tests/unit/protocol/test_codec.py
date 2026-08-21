import base64

import pytest

from realtime_voice.protocol.client_messages import AudioChunkMessage
from realtime_voice.protocol.decoder import decode_pcm16
from realtime_voice.protocol.errors import ProtocolViolation


def test_audio_chunk_rejects_odd_pcm_byte_count() -> None:
    message = AudioChunkMessage(
        type="AUDIO_CHUNK",
        session_id="s",
        sequence=0,
        audio_b64=base64.b64encode(b"\x00").decode(),
    )

    with pytest.raises(ProtocolViolation, match="PCM16_BYTE_ALIGNMENT"):
        decode_pcm16(message, sample_rate=16000)


def test_decode_pcm16_rejects_invalid_base64() -> None:
    message = AudioChunkMessage(type="AUDIO_CHUNK", session_id="s", sequence=0, audio_b64="!")

    with pytest.raises(ProtocolViolation, match="INVALID_BASE64"):
        decode_pcm16(message, sample_rate=16000)


@pytest.mark.parametrize("duration_ms", [9, 501])
def test_decode_pcm16_rejects_out_of_range_duration(duration_ms: int) -> None:
    payload = b"\x00\x00" * round(16000 * duration_ms / 1000)
    message = AudioChunkMessage(
        type="AUDIO_CHUNK",
        session_id="s",
        sequence=5,
        timestamp_ms=123,
        audio_b64=base64.b64encode(payload).decode(),
    )

    with pytest.raises(ProtocolViolation, match="AUDIO_CHUNK_DURATION"):
        decode_pcm16(message, sample_rate=16000)


def test_decode_pcm16_returns_payload_and_duration() -> None:
    payload = b"\x01\x00" * 1600
    message = AudioChunkMessage(
        type="AUDIO_CHUNK",
        session_id="s",
        sequence=5,
        timestamp_ms=123,
        audio_b64=base64.b64encode(payload).decode(),
    )

    decoded = decode_pcm16(message, sample_rate=16000)

    assert decoded.sequence == 5
    assert decoded.timestamp_ms == 123
    assert decoded.pcm16 == payload
    assert decoded.duration_ms == 100.0
