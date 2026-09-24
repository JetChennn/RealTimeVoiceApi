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
        self.fragment_voiced_samples = 0
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
        self.fragment_voiced_samples = 0
        self.samples = 0
        self.silence = 0

    def push(self, pcm: bytes, has_speech: bool):
        result = []
        if not self.started and not has_speech:
            return result
        if has_speech and not self.speaking:
            self.version += 1
        self.started = True
        self.speaking = has_speech
        count = len(pcm) // 2
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
            self.fragment_voiced_samples += count
        limit = self.samples >= self.settings.turn_end_max_utterance_seconds * 16000
        candidate = self.silence * 1000 >= self.settings.turn_end_candidate_silence_ms * 16000
        if self.fragment and (candidate or limit):
            self.segment_id += 1
            segment = SpeechSegment(
                self.segment_id,
                bytes(self.fragment),
                min(self.silence, len(self.fragment) // 2),
                self.fragment_voiced_samples,
            )
            result.append(
                SpeechSegmentReady(
                    self.session_id, segment, self.speech_end_at, self.input_id, limit
                )
            )
            self.fragment.clear()
            self.fragment_voiced_samples = 0
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
