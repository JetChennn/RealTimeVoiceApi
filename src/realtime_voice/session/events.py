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


@dataclass(frozen=True, slots=True)
class AsrSucceeded:
    segment_id: int
    text: str
    audio_wav: bytes


@dataclass(frozen=True, slots=True)
class AsrFailed:
    segment_id: int
    code: str
    message: str


@dataclass(frozen=True, slots=True)
class BerryDeltaReceived:
    turn_id: int
    generation: int
    delta: str


@dataclass(frozen=True, slots=True)
class BerryCompleted:
    turn_id: int
    generation: int
    reply_text: str


@dataclass(frozen=True, slots=True)
class BerryFailed:
    turn_id: int
    generation: int
    code: str
    message: str


@dataclass(frozen=True, slots=True)
class TtsChunkReceived:
    turn_id: int
    generation: int
    sequence: int
    pcm16: bytes
    finalize: bool


@dataclass(frozen=True, slots=True)
class TtsCompleted:
    turn_id: int
    generation: int


@dataclass(frozen=True, slots=True)
class TtsFailed:
    turn_id: int
    generation: int
    code: str
    message: str


@dataclass(frozen=True, slots=True)
class SessionDisconnected:
    session_id: str | None = None


SessionEvent: TypeAlias = (
    SpeechSegmentReady
    | AsrSucceeded
    | AsrFailed
    | BerryDeltaReceived
    | BerryCompleted
    | BerryFailed
    | TtsChunkReceived
    | TtsCompleted
    | TtsFailed
    | SessionDisconnected
)
