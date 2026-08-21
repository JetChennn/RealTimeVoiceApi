import io
import wave

import numpy as np


def pcm16_bytes_to_float32(data: bytes) -> np.ndarray:
    if len(data) % 2:
        raise ValueError("PCM16 data must contain complete samples")
    return np.frombuffer(data, dtype="<i2").astype(np.float32) / 32768.0


def float32_to_pcm16_bytes(samples: np.ndarray) -> bytes:
    clipped = np.clip(np.asarray(samples, dtype=np.float32), -1.0, 32767 / 32768)
    return np.rint(clipped * 32768.0).astype("<i2").tobytes()


def pcm16_wav_bytes(data: bytes, sample_rate: int) -> bytes:
    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(sample_rate)
        wav.writeframes(data)
    return buffer.getvalue()
