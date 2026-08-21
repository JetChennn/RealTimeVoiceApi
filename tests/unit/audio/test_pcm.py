import io
import wave
from importlib import import_module

import numpy as np
import pytest


def _pcm_module():
    return import_module("realtime_voice.audio.pcm")


def test_pcm16_round_trip_clips_and_preserves_shape() -> None:
    samples = np.array([-1.2, -0.5, 0.0, 0.5, 1.2], dtype=np.float32)

    encoded = _pcm_module().float32_to_pcm16_bytes(samples)
    decoded = _pcm_module().pcm16_bytes_to_float32(encoded)

    assert decoded.shape == samples.shape
    assert decoded[0] == pytest.approx(-1.0, abs=1e-4)
    assert decoded[-1] == pytest.approx(32767 / 32768, abs=1e-4)


def test_wav_header_describes_mono_16khz_pcm16() -> None:
    wav_data = _pcm_module().pcm16_wav_bytes(b"\x00\x00" * 160, 16000)

    with wave.open(io.BytesIO(wav_data), "rb") as wav:
        assert (wav.getnchannels(), wav.getsampwidth(), wav.getframerate()) == (1, 2, 16000)


def test_wav_rejects_incomplete_pcm16_sample() -> None:
    with pytest.raises(ValueError, match="PCM16.*complete samples"):
        _pcm_module().pcm16_wav_bytes(b"\x00", 16000)
