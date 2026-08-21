from importlib import import_module

import pytest

from tests.helpers import sine_pcm16


def test_streaming_resampler_matches_single_stream_length() -> None:
    source = sine_pcm16(sample_rate=48000, seconds=1.0, frequency=440)
    stream = import_module("realtime_voice.audio.resampler").StreamingResampler(48000, 16000)

    output = b"".join(
        [
            stream.process_pcm16(source[:24000]),
            stream.process_pcm16(source[24000:60000]),
            stream.process_pcm16(source[60000:], final=True),
        ]
    )

    assert abs(len(output) // 2 - 16000) <= 2


def test_bypass_resampler_rejects_incomplete_pcm16_sample() -> None:
    stream = import_module("realtime_voice.audio.resampler").StreamingResampler(16000, 16000)

    with pytest.raises(ValueError, match="PCM16.*complete samples"):
        stream.process_pcm16(b"\x00")
