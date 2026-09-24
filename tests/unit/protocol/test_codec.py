import base64

import pytest

from realtime_voice.protocol.client_messages import AudioChunkMessage, SpeakerRegister
from realtime_voice.protocol.decoder import (
    decode_client_message,
    decode_pcm16,
    decode_speaker_register,
)
from realtime_voice.protocol.errors import ProtocolViolation


def test_decode_client_message_rejects_binary_frame() -> None:
    raw = b'{"type":"CLOSE_SESSION","session_id":"session-100"}'

    with pytest.raises(ProtocolViolation, match="INVALID_MESSAGE"):
        decode_client_message(raw)


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


def test_decode_pcm16_rejects_duration_over_500ms() -> None:
    payload = b"\x00\x00" * round(16000 * 501 / 1000)
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


@pytest.mark.parametrize("sample_rate", [8000, 0])
def test_decode_pcm16_rejects_unsupported_sample_rate(sample_rate: int) -> None:
    message = AudioChunkMessage(
        type="AUDIO_CHUNK",
        session_id="s",
        sequence=0,
        audio_b64=base64.b64encode(b"\x00\x00" * 160).decode(),
    )

    with pytest.raises(ProtocolViolation, match="INVALID_MESSAGE"):
        decode_pcm16(message, sample_rate=sample_rate)


def test_decode_speaker_register_returns_complete_recording() -> None:
    payload = b"\x01\x00" * (16000 * 3)
    message = SpeakerRegister(
        type="SPEAKER_REGISTER",
        session_id="s",
        request_id="r1",
        audio_format="PCM16",
        sample_rate=16000,
        channels=1,
        audio_b64=base64.b64encode(payload).decode(),
    )

    decoded = decode_speaker_register(message, 16000)

    assert decoded.request_id == "r1"
    assert decoded.pcm16 == payload
    assert decoded.duration_ms == 3000


def test_decode_speaker_register_rejects_session_sample_rate_mismatch() -> None:
    message = SpeakerRegister(
        type="SPEAKER_REGISTER",
        session_id="s",
        request_id="r1",
        audio_format="PCM16",
        sample_rate=24000,
        channels=1,
        audio_b64=base64.b64encode(b"\x00\x00").decode(),
    )

    with pytest.raises(ProtocolViolation, match="SPEAKER_REGISTER_SAMPLE_RATE"):
        decode_speaker_register(message, 16000)
