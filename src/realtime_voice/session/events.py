"""Immutable events consumed by one session actor."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeAlias

if TYPE_CHECKING:
    from realtime_voice.audio.vad import SpeechSegment


@dataclass(frozen=True, slots=True)
class SpeechSegmentReady:
    session_id: str
    segment: SpeechSegment
    speech_end_at: float | None = None
    input_id: int = 0
    force_commit: bool = False


@dataclass(frozen=True, slots=True)
class AudioSegmentDiscarded:
    """A queued speech segment was intentionally filtered before ASR."""

    session_id: str
    segment_id: int
    reason: str


@dataclass(frozen=True, slots=True)
class AsrSucceeded:
    session_id: str
    segment_id: int
    text: str
    audio_wav: bytes


@dataclass(frozen=True, slots=True)
class AsrFailed:
    session_id: str
    segment_id: int
    code: str
    message: str


@dataclass(frozen=True, slots=True)
class ThinkerDeltaReceived:
    session_id: str
    turn_id: int
    generation: int
    delta: str


@dataclass(frozen=True, slots=True)
class ThinkerCompleted:
    session_id: str
    turn_id: int
    generation: int
    reply_text: str
    tone: str


@dataclass(frozen=True, slots=True)
class ThinkerSkipped:
    session_id: str
    turn_id: int
    generation: int


@dataclass(frozen=True, slots=True)
class ThinkerFailed:
    session_id: str
    turn_id: int
    generation: int
    code: str
    message: str


@dataclass(frozen=True, slots=True)
class TtsChunkReceived:
    session_id: str
    turn_id: int
    generation: int
    sequence: int
    pcm16: bytes
    finalize: bool


@dataclass(frozen=True, slots=True)
class TtsCompleted:
    session_id: str
    turn_id: int
    generation: int


@dataclass(frozen=True, slots=True)
class TtsFailed:
    session_id: str
    turn_id: int
    generation: int
    code: str
    message: str


@dataclass(frozen=True, slots=True)
class SessionDisconnected:
    session_id: str


@dataclass(frozen=True, slots=True)
class SpeechActivity:
    session_id: str
    input_id: int
    version: int
    speaking: bool
    silence_ms: float
    speech_end_at: float
    force_commit: bool = False


@dataclass(frozen=True, slots=True)
class SemanticEvaluated:
    session_id: str
    input_id: int
    version: int
    segment_count: int
    end: bool
    probability: float | None
    status: str
    total_seconds: float


@dataclass(frozen=True, slots=True)
class UserInputCommitted:
    session_id: str
    text: str
    audio_wav: bytes
    speech_end_at: float
    reason: str


@dataclass(frozen=True, slots=True)
class UserInputFailed:
    session_id: str
    code: str
    message: str


SessionEvent: TypeAlias = (
    SpeechSegmentReady
    | AudioSegmentDiscarded
    | SpeechActivity
    | SemanticEvaluated
    | UserInputCommitted
    | UserInputFailed
    | AsrSucceeded
    | AsrFailed
    | ThinkerDeltaReceived
    | ThinkerCompleted
    | ThinkerSkipped
    | ThinkerFailed
    | TtsChunkReceived
    | TtsCompleted
    | TtsFailed
    | SessionDisconnected
)
