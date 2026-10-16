"""Candidate audio slicing with sample-based silence and immediate resumption events."""

from __future__ import annotations

from time import monotonic

from realtime_voice.audio.vad import SpeechSegment
from realtime_voice.session.events import SpeechActivity, SpeechSegmentReady


class TurnEndAudio:
    def __init__(self, session_id, settings, clock=monotonic):
        self.session_id = session_id
        self.settings = settings
        self.clock = clock
        self.input_id = 1
        self.segment_id = 0
        self.version = 0
        self.started = False
        self.speaking = False
        self.fragment = bytearray()
        self.fragment_raw_voiced_samples = 0
        self.fragment_effective_speech_samples = 0
        self.pre_roll = bytearray()
        self.samples = 0
        self.silence = 0
        self.speech_end_at = 0.0

    def release(self, input_id):
        if input_id != self.input_id:
            return
        self.input_id += 1
        self.version = 0
        self.started = False
        self.speaking = False
        self.fragment.clear()
        self.fragment_raw_voiced_samples = 0
        self.fragment_effective_speech_samples = 0
        self.pre_roll.clear()
        self.samples = 0
        self.silence = 0

    def push(self, pcm: bytes, has_speech: bool):
        result = []
        count = len(pcm) // 2
        if not has_speech:
            self._remember_pre_roll(pcm)
        if not self.started and not has_speech:
            return result
        was_started = self.started
        was_speaking = self.speaking
        previous_silence = self.silence
        if has_speech and not self.fragment and self.pre_roll:
            self.fragment.extend(self.pre_roll)
            self.fragment_effective_speech_samples += len(self.pre_roll) // 2
            if not was_started:
                self.samples += len(self.pre_roll) // 2
            self.pre_roll.clear()
        elif has_speech and not was_speaking and previous_silence:
            counted_post = min(previous_silence, self._post_padding_samples)
            self.fragment_effective_speech_samples += min(
                self._pre_padding_samples,
                max(0, previous_silence - counted_post),
            )
            self.pre_roll.clear()
        if has_speech and not self.speaking:
            self.version += 1
        self.started = True
        self.speaking = has_speech
        self.samples += count
        if has_speech:
            self.silence = 0
            self.speech_end_at = self.clock()
        else:
            self.silence += count
        # After a candidate is emitted, silence-only frames need no extra ASR.
        if self.fragment or has_speech:
            self.fragment.extend(pcm)
        if has_speech:
            self.fragment_raw_voiced_samples += count
            self.fragment_effective_speech_samples += count
        elif self.fragment:
            self.fragment_effective_speech_samples += max(
                0,
                min(self.silence, self._post_padding_samples)
                - min(previous_silence, self._post_padding_samples),
            )
        limit = self.samples >= self.settings.turn_end_max_utterance_seconds * 16000
        candidate = self.silence * 1000 >= self.settings.turn_end_candidate_silence_ms * 16000
        if self.fragment and (candidate or limit):
            self.segment_id += 1
            segment = SpeechSegment(
                segment_id=self.segment_id,
                pcm16_16k=bytes(self.fragment),
                trailing_silence_samples=min(self.silence, len(self.fragment) // 2),
                voiced_samples=self.fragment_raw_voiced_samples,
                padded_voiced_samples=self.fragment_effective_speech_samples,
            )
            result.append(
                SpeechSegmentReady(
                    self.session_id, segment, self.speech_end_at, self.input_id, limit,
                    ready_at=self.clock(),
                )
            )
            self.fragment.clear()
            self.fragment_raw_voiced_samples = 0
            self.fragment_effective_speech_samples = 0
        result.append(
            SpeechActivity(
                self.session_id,
                self.input_id,
                self.version,
                has_speech,
                self.silence / 16,
                self.speech_end_at,
                limit,
            )
        )
        if limit:
            self.release(self.input_id)
        return result

    @property
    def _pre_padding_samples(self) -> int:
        if not self.settings.speaker_verification_enabled:
            return 0
        return self.settings.speaker_vad_pre_padding_ms * 16

    @property
    def _post_padding_samples(self) -> int:
        if not self.settings.speaker_verification_enabled:
            return 0
        return self.settings.speaker_vad_post_padding_ms * 16

    def _remember_pre_roll(self, pcm: bytes) -> None:
        maximum_bytes = self._pre_padding_samples * 2
        if maximum_bytes <= 0:
            self.pre_roll.clear()
            return
        self.pre_roll.extend(pcm)
        if len(self.pre_roll) > maximum_bytes:
            del self.pre_roll[: len(self.pre_roll) - maximum_bytes]
