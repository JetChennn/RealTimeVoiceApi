"""Per-session explicit speaker registration and verification state."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np

from realtime_voice.speaker.interfaces import SpeakerEmbedder

if TYPE_CHECKING:
    from realtime_voice.audio.vad import SpeechSegment


@dataclass(frozen=True, slots=True)
class SpeakerDecision:
    allow: bool
    reason: str
    similarity: float | None = None
    registered: bool = False
    voiced_ms: float = 0.0
    min_audio_ms: float = 0.0
    audio_eligible: bool = False


@dataclass(frozen=True, slots=True)
class SpeakerRegistration:
    success: bool
    reason: str
    registered: bool
    replaced: bool
    duration_ms: float
    min_audio_ms: float


class SessionSpeakerGate:
    """Own exactly one replaceable reference voiceprint for one session."""

    _REGISTER_MIN_RMS = 0.001
    _REGISTER_MIN_PEAK = 0.01

    def __init__(self, settings, embedder: SpeakerEmbedder | None) -> None:
        self._enabled = bool(settings.speaker_verification_enabled)
        self._embedder = embedder
        self._verify_threshold = settings.speaker_verification_threshold
        self._register_min_samples = settings.speaker_register_min_audio_ms * 16
        self._verify_min_samples = settings.speaker_verification_min_audio_ms * 16
        self._fail_open = settings.speaker_fail_open
        self._reference: np.ndarray | None = None
        # Registration and verification are serialized per session, so replacing the
        # reference is atomic and ordinary audio waits while registration is running.
        self._operation_lock = asyncio.Lock()

    @property
    def registered(self) -> bool:
        return self._reference is not None

    async def register(self, pcm16_16k: bytes) -> SpeakerRegistration:
        duration_ms = len(pcm16_16k) / 2 / 16
        minimum_ms = self._register_min_samples / 16
        if not self._enabled:
            return self._registration(False, "disabled", False, duration_ms, minimum_ms)
        if self._embedder is None:
            return self._registration(
                False, "embedder_unavailable", False, duration_ms, minimum_ms
            )
        if len(pcm16_16k) // 2 < self._register_min_samples:
            return self._registration(False, "too_short", False, duration_ms, minimum_ms)
        if not self._has_usable_registration_signal(pcm16_16k):
            return self._registration(False, "no_speech", False, duration_ms, minimum_ms)

        async with self._operation_lock:
            try:
                embedding = await self._normalized_embedding(pcm16_16k)
            except Exception:  # noqa: BLE001 - embedding backends expose heterogeneous failures
                return self._registration(
                    False, "embedding_failed", False, duration_ms, minimum_ms
                )

            previous = self._reference
            replaced = previous is not None
            self._reference = embedding
            if previous is not None:
                previous.fill(0)
            return self._registration(
                True,
                "updated" if replaced else "registered",
                replaced,
                duration_ms,
                minimum_ms,
            )

    async def evaluate(self, segment: SpeechSegment) -> SpeakerDecision:
        voiced_samples = segment.voiced_samples or max(
            0, len(segment.pcm16_16k) // 2 - segment.trailing_silence_samples
        )
        voiced_ms = voiced_samples / 16
        minimum_ms = self._verify_min_samples / 16
        eligible = voiced_samples >= self._verify_min_samples

        if not self._enabled:
            return self._decision(True, "disabled", voiced_ms, minimum_ms, eligible)

        async with self._operation_lock:
            if self._reference is None:
                return self._decision(True, "unregistered", voiced_ms, minimum_ms, eligible)
            if self._embedder is None:
                return self._decision(
                    self._fail_open,
                    "embedder_unavailable",
                    voiced_ms,
                    minimum_ms,
                    eligible,
                )
            if not eligible:
                return self._decision(False, "too_short", voiced_ms, minimum_ms, False)
            try:
                embedding = await self._normalized_embedding(
                    self._trim_trailing_silence(segment)
                )
            except Exception:  # noqa: BLE001 - embedding backends expose heterogeneous failures
                return self._decision(
                    self._fail_open,
                    "embedding_failed",
                    voiced_ms,
                    minimum_ms,
                    True,
                )
            # Float32 normalization can still produce a dot product a few ULPs
            # outside the mathematical cosine range. Clamp before serializing it
            # into the protocol model, which deliberately validates [-1, 1].
            similarity = float(np.clip(np.dot(self._reference, embedding), -1.0, 1.0))
            return self._decision(
                similarity >= self._verify_threshold,
                "matched" if similarity >= self._verify_threshold else "mismatch",
                voiced_ms,
                minimum_ms,
                True,
                similarity,
            )

    async def _normalized_embedding(self, pcm16_16k: bytes) -> np.ndarray:
        if self._embedder is None:
            raise RuntimeError("speaker embedder is unavailable")
        embedding = np.asarray(
            await self._embedder.embed(pcm16_16k), dtype=np.float32
        ).reshape(-1)
        norm = float(np.linalg.norm(embedding))
        if not np.isfinite(norm) or norm <= 0:
            raise ValueError("invalid speaker embedding")
        return embedding / norm

    def _decision(
        self,
        allow: bool,
        reason: str,
        voiced_ms: float,
        min_audio_ms: float,
        audio_eligible: bool,
        similarity: float | None = None,
    ) -> SpeakerDecision:
        return SpeakerDecision(
            allow=allow,
            reason=reason,
            similarity=similarity,
            registered=self.registered,
            voiced_ms=voiced_ms,
            min_audio_ms=min_audio_ms,
            audio_eligible=audio_eligible,
        )

    def _registration(
        self,
        success: bool,
        reason: str,
        replaced: bool,
        duration_ms: float,
        min_audio_ms: float,
    ) -> SpeakerRegistration:
        return SpeakerRegistration(
            success=success,
            reason=reason,
            registered=self.registered,
            replaced=replaced,
            duration_ms=duration_ms,
            min_audio_ms=min_audio_ms,
        )

    def clear(self) -> None:
        if self._reference is not None:
            self._reference.fill(0)
            self._reference = None

    @classmethod
    def _has_usable_registration_signal(cls, pcm16_16k: bytes) -> bool:
        """Reject silence, near-silence and isolated impulses before replacing a voiceprint."""
        samples = np.frombuffer(pcm16_16k, dtype="<i2").astype(np.float32)
        if samples.size == 0:
            return False
        samples /= 32768.0
        rms = float(np.sqrt(np.mean(np.square(samples))))
        peak = float(np.max(np.abs(samples)))
        return bool(
            np.isfinite(rms)
            and np.isfinite(peak)
            and rms >= cls._REGISTER_MIN_RMS
            and peak >= cls._REGISTER_MIN_PEAK
        )

    @staticmethod
    def _trim_trailing_silence(segment: SpeechSegment) -> bytes:
        """Keep acoustic speech only; ASR and turn-end retain the original segment."""
        sample_count = len(segment.pcm16_16k) // 2
        trailing_samples = min(max(segment.trailing_silence_samples, 0), sample_count)
        return segment.pcm16_16k[: (sample_count - trailing_samples) * 2]
