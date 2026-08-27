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


SessionEvent: TypeAlias = (
    SpeechSegmentReady
    | AsrSucceeded
    | AsrFailed
    | ThinkerDeltaReceived
    | ThinkerCompleted
    | ThinkerFailed
    | TtsChunkReceived
    | TtsCompleted
    | TtsFailed
    | SessionDisconnected
)
