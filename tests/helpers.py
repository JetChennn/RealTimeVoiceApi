import io
import wave

import httpx
import numpy as np


def pcm_chunk(sample_rate: int, duration_ms: int, value: int = 0) -> bytes:
    samples = round(sample_rate * duration_ms / 1000)
    return np.full(samples, value, dtype="<i2").tobytes()


def sine_pcm16(sample_rate: int, seconds: float, frequency: float) -> bytes:
    count = round(sample_rate * seconds)
    time_axis = np.arange(count, dtype=np.float64) / sample_rate
    samples = np.sin(2 * np.pi * frequency * time_axis) * 0.5
    return np.rint(samples * 32767).astype("<i2").tobytes()


def valid_wav() -> bytes:
    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(16000)
        wav.writeframes(pcm_chunk(16000, 100))
    return buffer.getvalue()


class ChunkStream(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes]):
        self._chunks = chunks

    async def __aiter__(self):
        for chunk in self._chunks:
            yield chunk

    async def aclose(self) -> None:
        return None


def stream_transport(chunks: list[bytes], status_code: int = 200):
    captured: dict[str, httpx.Request] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["request"] = request
        return httpx.Response(status_code, stream=ChunkStream(chunks))

    return httpx.MockTransport(handler), captured
