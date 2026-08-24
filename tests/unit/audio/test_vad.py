import asyncio
import threading
import wave
from pathlib import Path

import numpy as np
import pytest
from prometheus_client import CollectorRegistry

from realtime_voice.audio.vad import (
    BoundedDetectorOffload,
    DetectorSnapshot,
    SileroDetector,
    StreamingVadSegmenter,
    VadConfig,
    VadWorker,
)
from realtime_voice.audio.vad import (
    SpeechSegmentReady as VadSpeechSegmentReady,
)
from realtime_voice.observability.metrics import Metrics
from realtime_voice.session.events import SpeechSegmentReady


def pcm_chunk(sample_rate: int, duration_ms: int, value: int = 0) -> bytes:
    return value.to_bytes(2, byteorder="little", signed=True) * (sample_rate * duration_ms // 1000)


def test_vad_reexports_the_formal_session_event() -> None:
    assert VadSpeechSegmentReady is SpeechSegmentReady


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

    samples = np.full(512, 0.25, dtype=np.float32)
    result = detector.has_speech(samples)

    assert result is False
    assert len(calls) == 1
    np.testing.assert_array_equal(calls[0][0], samples)
    assert calls[0][1] == 16000


def test_silero_detector_rejects_non_512_sample_frame() -> None:
    detector = SileroDetector(model=lambda samples, sample_rate: 0.75)

    with pytest.raises(ValueError, match="512"):
        detector.has_speech(np.zeros(511, dtype=np.float32))


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
        for chunk in [speech, silence, silence, silence, silence, silence, silence, silence, None]:
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
            clock=lambda: 42.0,
        )
        event_loop_thread = threading.get_ident()
        try:
            await worker.run()
        finally:
            await offload.aclose()
        return event_queue.get_nowait(), event_queue.empty(), detector.thread_ids, event_loop_thread

    event, queue_is_empty, detector_thread_ids, event_loop_thread = asyncio.run(run_worker())
    assert event.session_id == "session-1"
    assert event.speech_end_at == pytest.approx(41.488)
    assert event.segment.segment_id == 1
    assert len(event.segment.pcm16_16k) == 10240 * 2
    assert queue_is_empty
    assert detector_thread_ids
    assert all(thread_id != event_loop_thread for thread_id in detector_thread_ids)


async def test_worker_stamps_acoustic_speech_end_before_confirmation_silence() -> None:
    class FrameDetector:
        def has_speech(self, samples: np.ndarray) -> bool:
            return bool(np.any(samples))

    audio_queue: asyncio.Queue[bytes | None] = asyncio.Queue()
    event_queue: asyncio.Queue[SpeechSegmentReady] = asyncio.Queue()
    speech_frame = pcm_chunk(16000, 32, value=1000)
    silence_frame = pcm_chunk(16000, 32)
    audio_queue.put_nowait(speech_frame + silence_frame * 2)
    audio_queue.put_nowait(None)
    offload = BoundedDetectorOffload(max_workers=1)
    worker = VadWorker(
        session_id="acoustic-end",
        audio_queue=audio_queue,
        event_queue=event_queue,
        segmenter=StreamingVadSegmenter(VadConfig(min_silence_ms=64)),
        detector=FrameDetector(),
        detector_offload=offload,
        clock=lambda: 100.0,
    )

    try:
        await worker.run()
    finally:
        await offload.aclose()

    event = event_queue.get_nowait()
    assert event.speech_end_at == pytest.approx(99.936)
    assert event_queue.empty()


async def test_detector_offload_writes_actual_active_and_pending_metrics() -> None:
    metrics = Metrics(registry=CollectorRegistry())
    offload = BoundedDetectorOffload(max_workers=1, metrics=metrics)
    first_started = threading.Event()
    release = threading.Event()

    def blocking_call() -> bool:
        first_started.set()
        release.wait(timeout=1)
        return True

    first = asyncio.create_task(offload.run(blocking_call))
    second: asyncio.Task[bool] | None = None
    try:
        while not first_started.is_set():
            await asyncio.sleep(0)
        second = asyncio.create_task(offload.run(lambda: False))
        while offload.snapshot().pending != 1:
            await asyncio.sleep(0)

        rendered = metrics.render().decode()
        assert "realtime_voice_executor_active 1.0" in rendered
        assert "realtime_voice_executor_pending 1.0" in rendered

        release.set()
        assert await asyncio.gather(first, second) == [True, False]
        rendered = metrics.render().decode()
        assert "realtime_voice_executor_active 0.0" in rendered
        assert "realtime_voice_executor_pending 0.0" in rendered
    finally:
        release.set()
        await asyncio.gather(first, *(()) if second is None else (second,), return_exceptions=True)
        await offload.aclose()


async def test_detector_offload_cancellation_clears_queued_accounting() -> None:
    metrics = Metrics(registry=CollectorRegistry())
    offload = BoundedDetectorOffload(max_workers=1, metrics=metrics)
    first_started = threading.Event()
    release = threading.Event()

    def blocking_call() -> bool:
        first_started.set()
        release.wait(timeout=1)
        return True

    first = asyncio.create_task(offload.run(blocking_call))
    queued: asyncio.Task[bool] | None = None
    try:
        while not first_started.is_set():
            await asyncio.sleep(0)
        queued = asyncio.create_task(offload.run(lambda: False))
        while offload.snapshot().pending != 1:
            await asyncio.sleep(0)

        assert offload.snapshot().active == 1
        queued.cancel()
        with pytest.raises(asyncio.CancelledError):
            await queued
        release.set()
        assert await first is True

        assert offload.snapshot() == DetectorSnapshot(active=0, pending=0, workers=1)
        rendered = metrics.render().decode()
        assert "realtime_voice_executor_active 0.0" in rendered
        assert "realtime_voice_executor_pending 0.0" in rendered
    finally:
        release.set()
        await asyncio.gather(
            first, *(()) if queued is None else (queued,), return_exceptions=True
        )
        await offload.aclose()


async def test_vad_worker_records_real_processing_latency() -> None:
    metrics = Metrics(registry=CollectorRegistry())
    audio_queue: asyncio.Queue[bytes | None] = asyncio.Queue()
    event_queue: asyncio.Queue[SpeechSegmentReady] = asyncio.Queue()
    audio_queue.put_nowait(pcm_chunk(16000, 32, value=1000))
    audio_queue.put_nowait(None)
    offload = BoundedDetectorOffload(max_workers=1, metrics=metrics)
    worker = VadWorker(
        session_id="metrics",
        audio_queue=audio_queue,
        event_queue=event_queue,
        segmenter=StreamingVadSegmenter(VadConfig()),
        detector=SileroDetector(model=lambda samples, sample_rate: 1.0),
        detector_offload=offload,
        metrics=metrics,
    )

    try:
        await worker.run()
    finally:
        await offload.aclose()

    rendered = metrics.render().decode()
    assert 'realtime_voice_stage_latency_seconds_count{stage="vad"} 1.0' in rendered


@pytest.mark.parametrize("name", ["speech_16k.wav", "silence_16k.wav"])
def test_audio_fixtures_are_mono_16khz_pcm16(name: str) -> None:
    fixture = Path(__file__).parents[2] / "fixtures" / "audio" / name

    with wave.open(str(fixture), "rb") as wav:
        assert (wav.getnchannels(), wav.getsampwidth(), wav.getframerate()) == (1, 2, 16000)
        assert wav.getnframes() == 1600


def test_max_speech_duration_splits_at_exact_sample_limit_and_retains_overflow() -> None:
    segmenter = StreamingVadSegmenter(VadConfig(max_speech_seconds=1))
    speech = pcm_chunk(16000, 100, value=1000)

    assert segmenter.push(speech * 8, has_speech=True) is None
    first = segmenter.push(speech * 4, has_speech=True)
    second = segmenter.push(speech * 8, has_speech=True)

    assert first is not None
    assert first.segment_id == 1
    assert len(first.pcm16_16k) == 16000 * 2
    assert second is not None
    assert second.segment_id == 2
    assert len(second.pcm16_16k) == 16000 * 2


class StrictFrameDetector:
    def __init__(self) -> None:
        self.frames: list[np.ndarray] = []

    def has_speech(self, samples: np.ndarray) -> bool:
        if samples.shape != (512,):
            raise ValueError(f"expected 512 samples, got {samples.shape}")
        self.frames.append(samples.copy())
        return bool(np.any(samples))


async def run_vad_worker(
    input_sample_rate: int, source: bytes, detector: StrictFrameDetector, session_id: str
) -> list[SpeechSegmentReady]:
    audio_queue: asyncio.Queue[bytes | None] = asyncio.Queue()
    event_queue: asyncio.Queue[SpeechSegmentReady] = asyncio.Queue()
    chunk_size = (input_sample_rate // 37) * 2
    for offset in range(0, len(source), chunk_size):
        audio_queue.put_nowait(source[offset : offset + chunk_size])
    audio_queue.put_nowait(None)

    offload = BoundedDetectorOffload()
    worker = VadWorker(
        session_id=session_id,
        input_sample_rate=input_sample_rate,
        audio_queue=audio_queue,
        event_queue=event_queue,
        segmenter=StreamingVadSegmenter(VadConfig()),
        detector=detector,
        detector_offload=offload,
    )
    try:
        await worker.run()
    finally:
        await offload.aclose()
    events: list[SpeechSegmentReady] = []
    while not event_queue.empty():
        events.append(event_queue.get_nowait())
    return events


@pytest.mark.parametrize("input_sample_rate", [16000, 24000, 48000])
def test_worker_resamples_each_input_rate_into_512_sample_detector_frames(
    input_sample_rate: int,
) -> None:
    source = pcm_chunk(input_sample_rate, 200, value=1000) + pcm_chunk(input_sample_rate, 700)
    detector = StrictFrameDetector()

    events = asyncio.run(run_vad_worker(input_sample_rate, source, detector, "rate-test"))

    assert len(events) == 1
    assert events[0].segment.segment_id == 1
    assert 10000 <= len(events[0].segment.pcm16_16k) // 2 <= 12000
    assert detector.frames
    assert all(frame.shape == (512,) for frame in detector.frames)


def test_workers_keep_resampler_state_isolated() -> None:
    async def run_workers() -> tuple[list[SpeechSegmentReady], list[StrictFrameDetector]]:
        first_detector = StrictFrameDetector()
        second_detector = StrictFrameDetector()
        first_source = pcm_chunk(24000, 200, value=1000) + pcm_chunk(24000, 700)
        second_source = pcm_chunk(48000, 200, value=1000) + pcm_chunk(48000, 700)
        first_events, second_events = await asyncio.gather(
            run_vad_worker(24000, first_source, first_detector, "first"),
            run_vad_worker(48000, second_source, second_detector, "second"),
        )
        return first_events + second_events, [first_detector, second_detector]

    events, detectors = asyncio.run(run_workers())

    assert [event.segment.segment_id for event in events] == [1, 1]
    assert all(10000 <= len(event.segment.pcm16_16k) // 2 <= 12000 for event in events)
    assert all(detector.frames for detector in detectors)


def test_worker_evaluates_eos_remainder_without_padding_emitted_pcm() -> None:
    class RecordingDetector:
        def __init__(self) -> None:
            self.frames: list[np.ndarray] = []
            self.thread_ids: list[int] = []

        def has_speech(self, samples: np.ndarray) -> bool:
            assert samples.shape == (512,)
            self.frames.append(samples.copy())
            self.thread_ids.append(threading.get_ident())
            return bool(np.any(samples))

    speech = pcm_chunk(16000, 32, value=1000)
    silence = pcm_chunk(16000, 500)
    detector = RecordingDetector()
    event_loop_thread = threading.get_ident()

    events = asyncio.run(run_vad_worker(16000, speech + silence, detector, "eos-remainder"))

    assert len(events) == 1
    assert events[0].segment.pcm16_16k == speech + silence
    assert len(events[0].segment.pcm16_16k) == (512 + 8000) * 2
    assert len(detector.frames) == 17
    assert detector.frames[-1].shape == (512,)
    assert all(thread_id != event_loop_thread for thread_id in detector.thread_ids)



def test_worker_evaluates_eos_remainder_without_forcing_incomplete_segment() -> None:
    speech = pcm_chunk(16000, 32, value=1000)
    silence = pcm_chunk(16000, 490)
    detector = StrictFrameDetector()

    events = asyncio.run(run_vad_worker(16000, speech + silence, detector, "eos-incomplete"))

    assert events == []
    assert len(detector.frames) == 17
