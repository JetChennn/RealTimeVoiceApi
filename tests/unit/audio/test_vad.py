import asyncio
import threading
import wave
from pathlib import Path

import numpy as np
import pytest

from realtime_voice.audio.vad import (
    BoundedDetectorOffload,
    SileroDetector,
    SpeechSegmentReady,
    StreamingVadSegmenter,
    VadConfig,
    VadWorker,
)


def pcm_chunk(sample_rate: int, duration_ms: int, value: int = 0) -> bytes:
    return value.to_bytes(2, byteorder="little", signed=True) * (sample_rate * duration_ms // 1000)


def test_segment_ends_after_configured_silence() -> None:
    segmenter = StreamingVadSegmenter(
        VadConfig(sample_rate=16000, min_silence_ms=500, max_speech_seconds=30)
    )
    speech = pcm_chunk(16000, 100, value=1000)
    silence = pcm_chunk(16000, 100)

    assert segmenter.push(speech, has_speech=True) is None
    for _ in range(4):
        assert segmenter.push(silence, has_speech=False) is None

    segment = segmenter.push(silence, has_speech=False)

    assert segment is not None
    assert segment.segment_id == 1
    assert segment.pcm16_16k == speech + silence * 5
    assert segmenter.active is False


def test_vad_state_is_session_local() -> None:
    first = StreamingVadSegmenter(VadConfig())
    second = StreamingVadSegmenter(VadConfig())

    first.push(pcm_chunk(16000, 100, value=1000), has_speech=True)

    assert first.active is True
    assert second.active is False
    assert second.silence_ms == 0.0


def test_max_speech_duration_forces_segment() -> None:
    segmenter = StreamingVadSegmenter(VadConfig(max_speech_seconds=1))
    speech = pcm_chunk(16000, 100, value=1000)

    result = None
    for _ in range(10):
        result = segmenter.push(speech, has_speech=True)

    assert result is not None
    assert result.segment_id == 1
    assert result.pcm16_16k == speech * 10
    assert segmenter.active is False


def test_segment_ids_increment_within_one_session() -> None:
    segmenter = StreamingVadSegmenter(VadConfig(min_silence_ms=100))
    speech = pcm_chunk(16000, 100, value=1000)
    silence = pcm_chunk(16000, 100)

    first = segmenter.push(speech, has_speech=True)
    assert first is None
    first = segmenter.push(silence, has_speech=False)
    second = segmenter.push(speech, has_speech=True)
    assert second is None
    second = segmenter.push(silence, has_speech=False)

    assert first is not None
    assert second is not None
    assert (first.segment_id, second.segment_id) == (1, 2)


def test_silero_detector_uses_an_injected_local_model_and_threshold() -> None:
    calls: list[tuple[np.ndarray, int]] = []

    def model(samples: np.ndarray, sample_rate: int) -> float:
        calls.append((samples, sample_rate))
        return 0.75

    detector = SileroDetector(model=model, config=VadConfig(threshold=0.8))

    result = detector.has_speech(np.array([0.25, -0.25], dtype=np.float32))

    assert result is False
    assert len(calls) == 1
    np.testing.assert_array_equal(calls[0][0], np.array([0.25, -0.25], dtype=np.float32))
    assert calls[0][1] == 16000


def test_worker_offloads_detection_and_emits_only_completed_segments() -> None:
    class RecordingDetector:
        def __init__(self) -> None:
            self.thread_ids: list[int] = []

        def has_speech(self, samples: np.ndarray) -> bool:
            self.thread_ids.append(threading.get_ident())
            return bool(np.any(samples))

    async def run_worker() -> tuple[SpeechSegmentReady, bool, list[int], int]:
        audio_queue: asyncio.Queue[bytes | None] = asyncio.Queue()
        event_queue: asyncio.Queue[SpeechSegmentReady] = asyncio.Queue()
        speech = pcm_chunk(16000, 100, value=1000)
        silence = pcm_chunk(16000, 100)
        for chunk in [speech, silence, silence, silence, silence, silence, None]:
            audio_queue.put_nowait(chunk)

        detector = RecordingDetector()
        offload = BoundedDetectorOffload(max_workers=1)
        worker = VadWorker(
            session_id="session-1",
            audio_queue=audio_queue,
            event_queue=event_queue,
            segmenter=StreamingVadSegmenter(VadConfig(min_silence_ms=500)),
            detector=detector,
            detector_offload=offload,
        )
        event_loop_thread = threading.get_ident()
        try:
            await worker.run()
        finally:
            await offload.aclose()
        return event_queue.get_nowait(), event_queue.empty(), detector.thread_ids, event_loop_thread

    event, queue_is_empty, detector_thread_ids, event_loop_thread = asyncio.run(run_worker())
    assert event.session_id == "session-1"
    assert event.segment.segment_id == 1
    assert event.segment.pcm16_16k == pcm_chunk(16000, 100, 1000) + pcm_chunk(16000, 100) * 5
    assert queue_is_empty
    assert detector_thread_ids
    assert all(thread_id != event_loop_thread for thread_id in detector_thread_ids)


@pytest.mark.parametrize("name", ["speech_16k.wav", "silence_16k.wav"])
def test_audio_fixtures_are_mono_16khz_pcm16(name: str) -> None:
    fixture = Path(__file__).parents[2] / "fixtures" / "audio" / name

    with wave.open(str(fixture), "rb") as wav:
        assert (wav.getnchannels(), wav.getsampwidth(), wav.getframerate()) == (1, 2, 16000)
        assert wav.getnframes() == 1600
