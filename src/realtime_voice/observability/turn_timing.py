"""Per-input/turn timings. Durations share the gateway's monotonic clock.

No wall-clock timestamps or audio/text payloads are exposed to clients. Stage
objects are task-local through a ContextVar, including downstream admission.
"""

from __future__ import annotations

from collections.abc import Callable
from contextvars import ContextVar
from dataclasses import dataclass, field
from time import monotonic


@dataclass
class StageTiming:
    stage: str
    submitted_at: float
    clock: Callable[[], float] = monotonic
    segment_id: int | None = None
    version: int | None = None
    segment_count: int | None = None
    generation: int = 0
    started_at: float | None = None
    finished_at: float | None = None
    admission_seconds: float = 0
    excluded_seconds: float = 0
    status: str = "pending"
    _admission_started_at: float | None = field(default=None, init=False, repr=False)

    def begin(self) -> None:
        if self.finished_at is None:
            self.started_at = self.clock()

    def begin_admission(self) -> None:
        if self.finished_at is None and self._admission_started_at is None:
            self._admission_started_at = self.clock()

    def end_admission(self) -> None:
        if self._admission_started_at is not None:
            ended_at = self.finished_at if self.finished_at is not None else self.clock()
            self.admission_seconds += max(0, ended_at - self._admission_started_at)
            self._admission_started_at = None

    def finish(self, status: str = "completed", *, at: float | None = None) -> None:
        if self.finished_at is None:
            self.finished_at = self.clock() if at is None else at
            # Freeze an in-flight admission wait before the terminal snapshot,
            # even if the request task has not received cancellation yet.
            self.end_admission()
            self.status = status

    def value(self) -> dict:
        # A failed/cancelled call has an elapsed time in details, but no successful
        # first-packet latency. Never substitute zero for a missing milestone.
        elapsed = None
        queue = None
        call = None
        if self.finished_at is not None:
            elapsed = max(0, self.finished_at - self.submitted_at - self.excluded_seconds)
            if self.started_at is not None:
                queue = max(
                    0, self.started_at - self.submitted_at
                    - self.excluded_seconds + self.admission_seconds,
                )
                queue = min(queue, elapsed)
                call = max(0, elapsed - queue)
            else:
                queue, call = elapsed, 0
        return {
            "ms": None if elapsed is None else elapsed * 1000,
            "queue_ms": None if queue is None else queue * 1000,
            "call_ms": None if call is None else call * 1000,
            "status": self.status,
        }


current_stage: ContextVar[StageTiming | None] = ContextVar("turn_timing_stage", default=None)


@dataclass
class SegmentTiming:
    segment_id: int
    ready_at: float
    silence_ms: float
    asr: StageTiming
    speaker: StageTiming | None = None


@dataclass
class InputTiming:
    segments: list[SegmentTiming] = field(default_factory=list)
    semantics: list[StageTiming] = field(default_factory=list)
    waits: list[tuple[float, float]] = field(default_factory=list)
    waiting_at: float | None = None
    version: int = 0
    reason: str = "vad"

    def wait(self, at: float, eligible: bool) -> None:
        if eligible and self.waiting_at is None:
            self.waiting_at = at
        elif not eligible and self.waiting_at is not None:
            self.waits.append((self.waiting_at, at))
            self.waiting_at = None

    @property
    def final(self) -> SegmentTiming | None:
        return self.segments[-1] if self.segments else None

    def pure_wait_ms(self) -> float:
        if self.final is None:
            return 0
        start = self.final.ready_at
        return sum(max(0, end - max(begin, start)) for begin, end in self.waits) * 1000


@dataclass
class TurnTiming:
    input: InputTiming
    llm: StageTiming
    tts: StageTiming | None = None
    first_audio_at: float | None = None
    status: str = "pending"
    sequence: int = 0
    fallback: bool = False

    def finish(self, status: str) -> None:
        if self.status != "pending":
            return
        self.status = status
        missing = "not_produced" if status == "completed" else status
        for stage in (self.llm, self.tts):
            if stage is not None:
                stage.finish(missing)

    def snapshot(self) -> dict:
        final = self.input.final
        candidates = [
            stage for stage in self.input.semantics
            if stage.version == self.input.version
            and stage.segment_count == len(self.input.segments)
        ]
        semantic = candidates[-1] if candidates else None
        absent = "not_produced" if self.status == "completed" else self.status

        def primary(stage, missing=absent):
            if stage is None:
                return {"status": missing}
            value = stage.value()
            if stage.status not in {"completed", "end", "wait"}:
                value = {**value, "ms": None}
            return value

        stages = []
        for segment in self.input.segments:
            stages.extend(s for s in (segment.speaker, segment.asr) if s is not None)
        stages.extend(self.input.semantics)
        stages.extend(s for s in (self.llm, self.tts) if s is not None)
        details = [
            {
                **stage.value(), "stage": stage.stage,
                "segment_id": stage.segment_id, "version": stage.version,
                "generation": stage.generation,
                "offset_ms": (stage.submitted_at - final.ready_at) * 1000 if final else 0,
            }
            for stage in stages
        ]
        self.sequence += 1
        return {
            "sequence": self.sequence,
            "status": self.status,
            "commit_reason": self.input.reason,
            "fallback": self.fallback,
            "asr": primary(final.asr if final else None, "not_run"),
            "semantic": primary(semantic, "not_run"),
            "llm": primary(self.llm),
            "tts": primary(self.tts),
            "total": {
                "ms": (max(0, self.first_audio_at - final.ready_at) * 1000
                       if self.first_audio_at is not None and final else None),
                "status": "completed" if self.first_audio_at is not None else absent,
            },
            "pre_segment_silence_ms": final.silence_ms if final else None,
            "post_segment_wait_ms": self.input.pure_wait_ms(),
            "details": details,
        }
