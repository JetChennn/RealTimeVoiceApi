"""Per-session candidate ASR accumulation; all state transitions run on the actor loop."""

from __future__ import annotations

import asyncio
import io
import wave
from collections import OrderedDict
from dataclasses import dataclass, field

from realtime_voice.audio.pcm import pcm16_wav_bytes
from realtime_voice.observability.logging import log_event
from realtime_voice.session.events import (
    AsrFailed,
    AsrSucceeded,
    AudioSegmentDiscarded,
    SemanticEvaluated,
    SpeechActivity,
    SpeechSegmentReady,
    UserInputCommitted,
    UserInputFailed,
)


@dataclass
class PendingInput:
    input_id: int
    version: int = 0
    speaking: bool = True
    silence_ms: float = 0
    speech_end_at: float = 0
    forced: bool = False
    failed: bool = False
    segments: dict[int, AsrSucceeded | None] = field(default_factory=dict)
    requested: tuple[int, int] | None = None
    end: bool = False
    job: asyncio.Task | None = None


class TurnEndCoordinator:
    def __init__(
        self,
        session_id,
        settings,
        detector,
        audio,
        events,
        metrics,
        context,
        *,
        user_id="unknown",
    ):
        self.session_id = session_id
        self.user_id = user_id
        self.settings = settings
        self.detector = detector
        self.audio = audio
        self.events = events
        self.metrics = metrics
        self.context = context
        self.inputs: OrderedDict[int, PendingInput] = OrderedDict()
        self.segment_inputs: dict[int, int] = {}
        self.retired = 0
        self.tasks: set[asyncio.Task] = set()

    def cancel(self, item):
        if item.job is not None:
            item.job.cancel()
            item.job = None

    def process(self, event):
        output = []
        if isinstance(event, (SpeechActivity, SpeechSegmentReady)):
            input_id = event.input_id
            if input_id <= self.retired:
                return output
            item = self.inputs.setdefault(input_id, PendingInput(input_id))
            # Bound queued utterances while ASR is slow, in addition to audio queue limits.
            if len(self.inputs) > self.settings.session_asr_queue_size:
                from realtime_voice.protocol.errors import ProtocolViolation

                raise ProtocolViolation("CLIENT_AUDIO_BACKPRESSURE", "too many pending utterances")
            if isinstance(event, SpeechSegmentReady):
                item.segments.setdefault(event.segment.segment_id, None)
                self.segment_inputs[event.segment.segment_id] = input_id
                item.forced |= event.force_commit
            else:
                if event.version != item.version:
                    self.cancel(item)
                    item.end = False
                    item.version = event.version
                item.speaking = event.speaking
                item.silence_ms = event.silence_ms
                item.speech_end_at = event.speech_end_at
                item.forced |= event.force_commit
        elif isinstance(event, AudioSegmentDiscarded):
            input_id = self.segment_inputs.pop(event.segment_id, None)
            if input_id not in self.inputs:
                return output
            self.inputs[input_id].segments.pop(event.segment_id, None)
        elif isinstance(event, (AsrSucceeded, AsrFailed)):
            input_id = self.segment_inputs.pop(event.segment_id, None)
            if input_id not in self.inputs:
                return output
            item = self.inputs[input_id]
            if isinstance(event, AsrFailed):
                item.failed = True
                item.segments.pop(event.segment_id, None)
                self.cancel(item)
                output.append(UserInputFailed(self.session_id, event.code, event.message))
                # Never submit an incomplete successful prefix as a full utterance.
                self.audio.release(input_id)
            else:
                item.segments[event.segment_id] = event
        elif isinstance(event, SemanticEvaluated):
            item = self.inputs.get(event.input_id)
            if (
                item is not None
                and item.requested == (event.version, event.segment_count)
                and item.version == event.version
            ):
                item.end = event.end
                log_event(
                    "semantic_evaluated",
                    device_id=self.user_id,
                    user_id=self.user_id,
                    session_id=self.session_id,
                    stage="TURN_END",
                    duration_ms=event.total_seconds * 1000,
                    status=event.status,
                    probability=event.probability,
                )
        else:
            return [event]

        for item in list(self.inputs.values()):
            pending = any(v is None for v in item.segments.values())
            texts = [v.text for v in item.segments.values() if v is not None and v.text]
            if not item.failed and not item.forced and not item.speaking and not pending and texts:
                key = (item.version, len(item.segments))
                if (
                    item.requested != key
                    and item.silence_ms < self.settings.turn_end_max_silence_ms
                ):
                    self.cancel(item)
                    item.requested = key
                    item.end = False
                    item.job = asyncio.create_task(
                        self._predict(item.input_id, key, " ".join(texts))
                    )
                    self.tasks.add(item.job)
                    item.job.add_done_callback(self.tasks.discard)
        # Preserve the order of forced utterances when ASR finishes later.
        while self.inputs:
            input_id, item = next(iter(self.inputs.items()))
            if any(v is None for v in item.segments.values()):
                break
            reason = (
                "asr_failed"
                if item.failed
                else "max_utterance"
                if item.forced
                else "max_silence"
                if not item.speaking and item.silence_ms >= self.settings.turn_end_max_silence_ms
                else "semantic_end"
                if not item.speaking
                and item.end
                and item.silence_ms >= self.settings.turn_end_min_silence_ms
                else None
            )
            if reason is None:
                break
            self.cancel(item)
            del self.inputs[input_id]
            self.retired = input_id
            self.audio.release(input_id)
            results = [v for v in item.segments.values() if v is not None and v.text]
            if not item.failed and results:
                # ASR only sees each fragment once; combine WAV payloads, never their headers.
                pcm = []
                for value in item.segments.values():
                    if value is not None:
                        with wave.open(io.BytesIO(value.audio_wav), "rb") as w:
                            pcm.append(w.readframes(w.getnframes()))
                output.append(
                    UserInputCommitted(
                        self.session_id,
                        " ".join(v.text for v in results),
                        pcm16_wav_bytes(b"".join(pcm), 16000),
                        item.speech_end_at,
                        reason,
                    )
                )
                self.metrics.turn_end_commits.labels(reason).inc()
                log_event(
                    "user_input_committed",
                    device_id=self.user_id,
                    user_id=self.user_id,
                    session_id=self.session_id,
                    stage="TURN_END",
                    reason=reason,
                    segment_count=len(item.segments),
                )
        return output

    async def _predict(self, input_id, key, text):
        decision = await self.detector.predict(self.context(), text)
        await self.events.put(
            SemanticEvaluated(
                self.session_id,
                input_id,
                key[0],
                key[1],
                decision.end,
                decision.probability,
                decision.status,
                decision.total_seconds,
            )
        )

    async def aclose(self):
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()
        self.inputs.clear()
        self.segment_inputs.clear()
        self.audio.release(self.audio.input_id)
