import soxr

from realtime_voice.audio.pcm import float32_to_pcm16_bytes, pcm16_bytes_to_float32


class StreamingResampler:
    def __init__(self, input_rate: int, output_rate: int):
        self._bypass = input_rate == output_rate
        self._stream = (
            None
            if self._bypass
            else soxr.ResampleStream(input_rate, output_rate, 1, dtype="float32", quality="HQ")
        )

    def process_pcm16(self, data: bytes, final: bool = False) -> bytes:
        samples = pcm16_bytes_to_float32(data)
        if self._bypass:
            return data
        output = self._stream.resample_chunk(samples, last=final)
        return float32_to_pcm16_bytes(output)
