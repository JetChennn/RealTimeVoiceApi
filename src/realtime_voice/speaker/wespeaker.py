"""ONNX adapter for 16 kHz WeSpeaker embedding models."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from time import monotonic
from typing import TYPE_CHECKING

import numpy as np

from realtime_voice.clients.limits import BoundedAdmission

if TYPE_CHECKING:
    from realtime_voice.observability.metrics import Metrics


class SpeakerEmbeddingError(RuntimeError):
    """The shared speaker model cannot produce a usable normalized vector."""


class WeSpeakerOnnxEmbedder:
    """Load one official WeSpeaker ONNX model and extract PCM embeddings off-loop.

    The implementation intentionally consumes the exported ONNX model directly rather
    than the full WeSpeaker Python runtime: the gateway needs only fbank extraction and
    inference, while direct ONNX use keeps dependencies and deployment surface small.
    """

    def __init__(
        self,
        model_path: str,
        *,
        concurrency: int,
        max_waiters: int,
        metrics: Metrics | None = None,
    ) -> None:
        self._path = Path(model_path)
        self._metrics = metrics
        self._executor = ThreadPoolExecutor(
            max_workers=concurrency, thread_name_prefix="speaker-embedding"
        )
        self._admission = BoundedAdmission(
            "speaker", concurrency, max_waiters, metrics=metrics
        )
        self._session = None
        self._input_name = ""
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._executor, self._load)
        self._started = True

    def _load(self) -> None:
        if not self._path.is_file():
            raise FileNotFoundError(f"WeSpeaker ONNX model is not a file: {self._path}")
        try:
            import onnxruntime as ort
        except ModuleNotFoundError as error:
            raise SpeakerEmbeddingError("onnxruntime is required for speaker verification") from error
        options = ort.SessionOptions()
        options.intra_op_num_threads = 1
        options.inter_op_num_threads = 1
        session = ort.InferenceSession(
            str(self._path), sess_options=options, providers=["CPUExecutionProvider"]
        )
        inputs = session.get_inputs()
        if len(inputs) != 1:
            raise SpeakerEmbeddingError("WeSpeaker ONNX model must have exactly one input")
        self._session = session
        self._input_name = inputs[0].name

    async def embed(self, pcm16_16k: bytes) -> np.ndarray:
        if not self._started or self._session is None:
            raise SpeakerEmbeddingError("WeSpeaker model has not completed startup")
        started = monotonic()
        try:
            async def operation() -> np.ndarray:
                loop = asyncio.get_running_loop()
                return await loop.run_in_executor(self._executor, self._embed_sync, pcm16_16k)

            return await self._admission.run(operation)
        finally:
            if self._metrics is not None:
                self._metrics.observe_stage_latency("speaker", monotonic() - started)

    def _embed_sync(self, pcm16_16k: bytes) -> np.ndarray:
        if len(pcm16_16k) % 2:
            raise SpeakerEmbeddingError("speaker audio is not aligned PCM16")
        if not pcm16_16k:
            raise SpeakerEmbeddingError("speaker audio is empty")
        try:
            import torch
            from torchaudio.compliance import kaldi
        except ModuleNotFoundError as error:
            raise SpeakerEmbeddingError("torch and torchaudio are required for WeSpeaker fbank") from error

        # torchaudio.load (used by the official WeSpeaker example) yields [-1, 1] float audio.
        samples = np.frombuffer(pcm16_16k, dtype="<i2").astype(np.float32, copy=True) / 32768.0
        waveform = torch.from_numpy(samples).unsqueeze(0)
        features = kaldi.fbank(
            waveform,
            num_mel_bins=80,
            frame_length=25,
            frame_shift=10,
            dither=0.0,
            sample_frequency=16000,
            window_type="hamming",
            use_energy=False,
        )
        if features.numel() == 0:
            raise SpeakerEmbeddingError("speaker audio is too short for fbank extraction")
        features = features - torch.mean(features, dim=0)
        output = self._session.run(None, {self._input_name: features.unsqueeze(0).numpy()})
        if not output:
            raise SpeakerEmbeddingError("WeSpeaker ONNX model returned no embedding")
        embedding = np.asarray(output[0], dtype=np.float32).reshape(-1)
        norm = float(np.linalg.norm(embedding))
        if not np.isfinite(norm) or norm <= 0:
            raise SpeakerEmbeddingError("WeSpeaker embedding is invalid")
        return embedding / norm

    async def aclose(self) -> None:
        self._session = None
        self._started = False
        self._executor.shutdown(wait=True, cancel_futures=True)
