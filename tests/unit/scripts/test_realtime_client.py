import wave
from pathlib import Path

import pytest

from scripts.realtime_client import TurnAudioWriter, iter_pcm_chunks, read_pcm16_wav


def test_iter_pcm_chunks_uses_40_ms_and_monotonic_sequences() -> None:
    chunks = list(iter_pcm_chunks(b"\x01\x00" * 1600, 16000))

    # 100ms = 两个 40ms 块 + 20ms 尾块；尾块照常保留。
    assert [chunk.sequence for chunk in chunks] == [0, 1, 2]
    assert [len(chunk.pcm) for chunk in chunks] == [1280, 1280, 640]


def test_iter_pcm_chunks_keeps_sub_10ms_remainder() -> None:
    # 3 个完整 40ms 块（3*640 样本）+ 8ms 余数（128 样本 = 256 字节）；
    # 余数照常发送，由服务端累积合并处理。
    chunks = list(iter_pcm_chunks(b"\x01\x00" * (1920 + 128), 16000))

    assert [chunk.sequence for chunk in chunks] == [0, 1, 2, 3]
    assert [len(chunk.pcm) for chunk in chunks] == [1280, 1280, 1280, 256]


def test_turn_audio_writer_discards_interrupted_turn_and_writes_playable_wav(
    tmp_path: Path,
) -> None:
    target = tmp_path / "reply.wav"
    writer = TurnAudioWriter(target, 16000)
    writer.add(1, 0, b"\x01\x00" * 8)
    writer.interrupt(1)
    writer.add(2, 0, b"\x02\x00" * 4)
    writer.add(2, 1, b"\x03\x00" * 4)

    assert writer.write() == 2
    with wave.open(str(target), "rb") as output:
        assert (output.getnchannels(), output.getsampwidth(), output.getframerate()) == (
            1,
            2,
            16000,
        )
        assert output.readframes(output.getnframes()) == b"\x02\x00" * 4 + b"\x03\x00" * 4


def test_turn_audio_writer_rejects_out_of_order_audio(tmp_path: Path) -> None:
    writer = TurnAudioWriter(tmp_path / "reply.wav", 16000)
    with pytest.raises(ValueError, match="expected 0, got 1"):
        writer.add(1, 1, b"\x00\x00")


def test_read_pcm16_wav_rejects_wrong_rate(tmp_path: Path) -> None:
    source = tmp_path / "input.wav"
    with wave.open(str(source), "wb") as output:
        output.setnchannels(1)
        output.setsampwidth(2)
        output.setframerate(24000)
        output.writeframes(b"\x00\x00" * 8)

    with pytest.raises(ValueError, match="16000 Hz"):
        read_pcm16_wav(source, 16000)
