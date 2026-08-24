"""Per-session streaming voice activity detection and segmentation."""

import asyncio
import importlib
import os
import threading
from collections import deque
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Protocol

import numpy as np

from realtime_voice.audio.pcm import pcm16_bytes_to_float32
from realtime_voice.audio.resampler import StreamingResampler
from realtime_voice.session.events import SpeechSegmentReady


@dataclass(frozen=True)
class VadConfig:
    sample_rate: int = 16000
    threshold: float = 0.5
    min_silence_ms: int = 500
    max_speech_seconds: int = 30


@dataclass(frozen=True)
class DetectorSnapshot:
    active: int
    pending: int
    workers: int


@dataclass(frozen=True)
class SpeechSegment:
    segment_id: int
    pcm16_16k: bytes


class SpeechDetector(Protocol):
    def has_speech(self, samples: np.ndarray) -> bool: ...


class DetectorOffload(Protocol):
    async def run(self, call: Callable[[], bool]) -> bool: ...


class StreamingVadSegmenter:
    """Stateful PCM16 segmenter; one instance belongs to exactly one session."""

    def __init__(self, config: VadConfig):
        self.config = config
        self.active = False
        self.silence_ms = 0.0
        self._chunks: list[bytes] = []
        self._next_segment_id = 1
        self._ready: deque[SpeechSegment] = deque()

    def push(self, pcm16_16k: bytes, has_speech: bool) -> SpeechSegment | None:
        if len(pcm16_16k) % 2:
            raise ValueError("PCM16 data must contain complete samples")

        previously_ready = self.pop_ready()
        remaining = pcm16_16k
        while remaining:
            if not self.active:
                if not has_speech:
                    break
                self.active = True
                self.silence_ms = 0.0

            capacity = self._max_samples - self._sample_count()
            if capacity <= 0:
                self._ready.append(self._finish())
                continue
            chunk = remaining[: capacity * 2]
            remaining = remaining[len(chunk) :]
            self._chunks.append(chunk)
            if has_speech:
                self.silence_ms = 0.0
            else:
                self.silence_ms += self._duration_ms(chunk)

            reached_silence = self.silence_ms >= self.config.min_silence_ms
            reached_limit = self._sample_count() >= self._max_samples
            if reached_silence or reached_limit:
                self._ready.append(self._finish())

        return previously_ready or self.pop_ready()

    @property
    def _max_samples(self) -> int:
        return self.config.sample_rate * self.config.max_speech_seconds

    def _sample_count(self) -> int:
        return sum(len(chunk) for chunk in self._chunks) // 2

    def _duration_ms(self, pcm16_16k: bytes) -> float:
        return len(pcm16_16k) / 2 / self.config.sample_rate * 1000

    def pop_ready(self) -> SpeechSegment | None:
        return self._ready.popleft() if self._ready else None

    def _finish(self) -> SpeechSegment:
        segment = SpeechSegment(self._next_segment_id, b"".join(self._chunks))
        self._next_segment_id += 1
        self.active = False
        self.silence_ms = 0.0
        self._chunks.clear()
        return segment


class SileroDetector:
    """Silero adapter whose model can be injected for deterministic tests."""

    def __init__(
        self,
        model: Callable[[np.ndarray, int], object] | None = None,
        config: VadConfig | None = None,
    ):
        self.config = config or VadConfig()
        self._model = model or self._load_local_model()

    def has_speech(self, samples: np.ndarray) -> bool:
        frame = np.asarray(samples, dtype=np.float32)
        if frame.shape != (512,):
            raise ValueError("Silero VAD requires exactly 512 samples at 16 kHz")
        score = self._model(frame, self.config.sample_rate)
        return self._score(score) >= self.config.threshold

    @staticmethod
    def _score(value: object) -> float:
        detached = getattr(value, "detach", lambda: value)()
        cpu_value = getattr(detached, "cpu", lambda: detached)()
        return float(np.asarray(cpu_value).reshape(-1)[0])

    @staticmethod
    def _load_local_model() -> Callable[[np.ndarray, int], object]:
        model_path = os.environ.get("RTVA_VAD_MODEL_PATH")
        if model_path:
            raw_model = SileroDetector._load_torchscript(Path(model_path))
        else:
            raw_model = SileroDetector._load_packaged_model()

        def detect(samples: np.ndarray, sample_rate: int) -> object:
            import torch

            with torch.no_grad():
                return raw_model(torch.from_numpy(samples), sample_rate)

        return detect

    @staticmethod
    def _load_torchscript(model_path: Path) -> object:
        if not model_path.is_file():
            raise FileNotFoundError(f"RTVA_VAD_MODEL_PATH is not a file: {model_path}")
        import torch

        return torch.jit.load(str(model_path), map_location="cpu")

    @staticmethod
    def _load_packaged_model() -> object:
        try:
            package = importlib.import_module("silero_vad")
        except ModuleNotFoundError as error:
            raise RuntimeError(
                "Silero VAD is unavailable; install silero-vad or set RTVA_VAD_MODEL_PATH"
            ) from error
        return package.load_silero_vad(onnx=False)


class BoundedDetectorOffload:
    """Runs blocking detector calls in a bounded thread pool."""

    def __init__(self, max_workers: int = 1):
        if max_workers < 1:
            raise ValueError("max_workers must be at least 1")
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="vad-detector"
        )
        self._workers = max_workers
        self._snapshot_lock = threading.Lock()
        self._active = 0
        self._pending = 0

    async def run(self, call: Callable[[], bool]) -> bool:
        with self._snapshot_lock:
            self._pending += 1

        def wrapped() -> bool:
            with self._snapshot_lock:
                self._pending -= 1
                self._active += 1
            try:
                return call()
            finally:
                with self._snapshot_lock:
                    self._active -= 1

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, wrapped)

    def snapshot(self) -> DetectorSnapshot:
        with self._snapshot_lock:
            return DetectorSnapshot(self._active, self._pending, self._workers)

    async def aclose(self) -> None:
        self._executor.shutdown(wait=True, cancel_futures=True)


class VadWorker:
    """Consumes one session's PCM chunks and publishes only completed segments."""

    _DETECTOR_FRAME_BYTES = 512 * 2

    def __init__(
        self,
        *,
        session_id: str,
        audio_queue: asyncio.Queue[bytes | None],
        event_queue: asyncio.Queue[SpeechSegmentReady],
        segmenter: StreamingVadSegmenter,
        detector: SpeechDetector,
        detector_offload: DetectorOffload,
        input_sample_rate: int = 16000,
    ):
        self._session_id = session_id
        self._audio_queue = audio_queue
        self._event_queue = event_queue
        self._segmenter = segmenter
        self._detector = detector
        self._detector_offload = detector_offload
        self._resampler = StreamingResampler(input_sample_rate, 16000)
        self._detector_remainder = bytearray()

    async def run(self) -> None:
        while (input_pcm16 := await self._audio_queue.get()) is not None:
            await self._process_resampled(self._resampler.process_pcm16(input_pcm16))

        await self._process_resampled(self._resampler.process_pcm16(b"", final=True))
        await self._process_eos_remainder()
        await self._publish_ready_segments()

    async def _process_eos_remainder(self) -> None:
        if not self._detector_remainder:
            return
        remainder = bytes(self._detector_remainder)
        self._detector_remainder.clear()
        detector_frame = remainder.ljust(self._DETECTOR_FRAME_BYTES, b"\x00")
        samples = pcm16_bytes_to_float32(detector_frame)
        has_speech = await self._detector_offload.run(partial(self._detector.has_speech, samples))
        segment = self._segmenter.push(remainder, has_speech)
        if segment is not None:
            await self._event_queue.put(
                SpeechSegmentReady(
                    session_id=self._session_id,
                    segment=segment,
                )
            )

    async def _process_resampled(self, pcm16_16k: bytes) -> None:
        self._detector_remainder.extend(pcm16_16k)
        while len(self._detector_remainder) >= self._DETECTOR_FRAME_BYTES:
            frame = bytes(self._detector_remainder[: self._DETECTOR_FRAME_BYTES])
            del self._detector_remainder[: self._DETECTOR_FRAME_BYTES]
            samples = pcm16_bytes_to_float32(frame)
            has_speech = await self._detector_offload.run(
                partial(self._detector.has_speech, samples)
            )
            segment = self._segmenter.push(frame, has_speech)
            if segment is not None:
                await self._event_queue.put(
                    SpeechSegmentReady(
                        session_id=self._session_id,
                        segment=segment,
                    )
                )
            await self._publish_ready_segments()

    async def _publish_ready_segments(self) -> None:
        while (segment := self._segmenter.pop_ready()) is not None:
            await self._event_queue.put(
                SpeechSegmentReady(
                    session_id=self._session_id,
                    segment=segment,
                )
            )
