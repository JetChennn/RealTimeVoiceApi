"""Pure synchronous state transitions for one realtime voice session."""

from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeAlias

if TYPE_CHECKING:
    from realtime_voice.audio.vad import SpeechSegment

from realtime_voice.protocol.server_messages import (
    AsrResult,
    AudioDelta,
    ErrorMessage,
    ResponseEnd,
    ServerMessage,
    TextDelta,
    TextEnd,
    TurnState,
)
from realtime_voice.session.events import (
    AsrFailed,
    AsrSucceeded,
    BerryCompleted,
    BerryDeltaReceived,
    BerryFailed,
    SessionDisconnected,
    SessionEvent,
    SpeechSegmentReady,
    TtsChunkReceived,
    TtsCompleted,
    TtsFailed,
)
from realtime_voice.session.state import (
    TERMINAL_TURN_STAGES,
    SessionState,
    TurnContext,
    TurnStage,
)


@dataclass(frozen=True, slots=True)
class QueueAsr:
    session_id: str
    segment: SpeechSegment


@dataclass(frozen=True, slots=True)
class SendOutbound:
    message: ServerMessage


@dataclass(frozen=True, slots=True)
class StartBerry:
    turn_id: int
    generation: int
    text: str
    audio_wav: bytes


@dataclass(frozen=True, slots=True)
class StartNextBerry:
    turn_id: int
    generation: int
    text: str
    audio_wav: bytes
    interrupt_first: bool


@dataclass(frozen=True, slots=True)
class StartTts:
    turn_id: int
    generation: int
    user_input: str
    reply_text: str


@dataclass(frozen=True, slots=True)
class CloseRuntime:
    session_id: str


@dataclass(frozen=True, slots=True)
class RecordStaleEvent:
    event_type: str
    reason: str
    turn_id: int | None = None


@dataclass(frozen=True, slots=True)
class RecordDiscardedAudio:
    turn_id: int
    generation: int
    byte_count: int


SessionEffect: TypeAlias = (
    SendOutbound
    | StartBerry
    | StartNextBerry
    | StartTts
    | CloseRuntime
    | RecordStaleEvent
    | RecordDiscardedAudio
    | QueueAsr
)


class SessionActor:
    """Own session state and translate events into runtime effects."""

    def __init__(self, state: SessionState):
        self.state = state

    def handle(self, event: SessionEvent) -> list[SessionEffect]:
        if event.session_id != self.state.session_id:
            return [self._stale(event, "session")]
        if isinstance(event, SessionDisconnected):
            return self._disconnect(event)
        if self.state.closing:
            return [self._stale(event, "closing")]
        if isinstance(event, SpeechSegmentReady):
            segment_id = event.segment.segment_id
            if segment_id in self.state.registered_asr_segment_ids:
                return [self._stale(event, "asr_segment")]
            self.state.registered_asr_segment_ids.add(segment_id)
            self.state.pending_asr_segment_ids.add(segment_id)
            return [QueueAsr(self.state.session_id, event.segment)]
        if isinstance(event, AsrSucceeded):
            return self._asr_succeeded(event)
        if isinstance(event, AsrFailed):
            return self._asr_failed(event)
        if isinstance(event, BerryDeltaReceived):
            return self._berry_delta(event)
        if isinstance(event, BerryCompleted):
            return self._berry_completed(event)
        if isinstance(event, BerryFailed):
            return self._berry_failed(event)
        if isinstance(event, TtsChunkReceived):
            return self._tts_chunk(event)
        if isinstance(event, TtsCompleted):
            return self._tts_completed(event)
        if isinstance(event, TtsFailed):
            return self._tts_failed(event)
        raise TypeError(f"unsupported session event: {type(event).__name__}")

    def _disconnect(self, event: SessionDisconnected) -> list[SessionEffect]:
        if self.state.closing:
            return []
        self.state.closing = True
        return [CloseRuntime(self.state.session_id)]

    def _asr_succeeded(self, event: AsrSucceeded) -> list[SessionEffect]:
        stale_reason = self._asr_stale_reason(event.segment_id)
        if stale_reason is not None:
            return [self._stale(event, stale_reason)]
        self.state.pending_asr_segment_ids.remove(event.segment_id)
        if not event.text:
            return []

        effects = self._interrupt_unfinished_turns()
        turn_id = self.state.next_turn_id
        self.state.next_turn_id += 1
        turn = TurnContext(turn_id, event.text, event.audio_wav)
        self.state.turns[turn_id] = turn
        self.state.llm_queue.append(turn_id)
        effects.append(
            SendOutbound(
                AsrResult(
                    type="ASR_RESULT",
                    user_id=self.state.user_id,
                    session_id=self.state.session_id,
                    turn_id=turn_id,
                    interrupt=False,
                    text=event.text,
                )
            )
        )
        if self.state.active_llm_turn_id is None:
            effects.append(self._start_berry(interrupt_first=None))
        return effects

    def _asr_failed(self, event: AsrFailed) -> list[SessionEffect]:
        stale_reason = self._asr_stale_reason(event.segment_id)
        if stale_reason is not None:
            return [self._stale(event, stale_reason)]
        self.state.pending_asr_segment_ids.remove(event.segment_id)
        return [self._error(0, False, "ASR", event.code, event.message)]

    def _asr_stale_reason(self, segment_id: int) -> str | None:
        if segment_id in self.state.pending_asr_segment_ids:
            return None
        if segment_id in self.state.registered_asr_segment_ids:
            return "asr_segment"
        return "unknown_segment"

    def _interrupt_unfinished_turns(self) -> list[SessionEffect]:
        effects: list[SessionEffect] = []
        for turn in self.state.turns.values():
            if turn.stage in TERMINAL_TURN_STAGES or turn.interrupted:
                continue
            turn.interrupted = True
            effects.append(
                SendOutbound(
                    TurnState(
                        type="TURN_STATE",
                        user_id=self.state.user_id,
                        session_id=self.state.session_id,
                        turn_id=turn.turn_id,
                        interrupt=True,
                        state="INTERRUPTED",
                    )
                )
            )
        return effects

    def _start_berry(self, interrupt_first: bool | None) -> StartBerry | StartNextBerry:
        turn_id = self.state.llm_queue.popleft()
        turn = self.state.turns[turn_id]
        turn.berry_generation += 1
        turn.stage = TurnStage.STREAMING_LLM
        self.state.active_llm_turn_id = turn_id
        assert turn.audio_wav is not None
        values = (turn.turn_id, turn.berry_generation, turn.asr_text, turn.audio_wav)
        if interrupt_first is None:
            return StartBerry(*values)
        return StartNextBerry(*values, interrupt_first=interrupt_first)

    def _berry_turn(
        self, event: object, turn_id: int, generation: int
    ) -> TurnContext | SessionEffect:
        turn = self.state.turns.get(turn_id)
        if turn is None:
            return self._stale(event, "unknown_turn", turn_id)
        if generation != turn.berry_generation:
            return self._stale(event, "berry_generation", turn_id)
        if turn.stage is not TurnStage.STREAMING_LLM or self.state.active_llm_turn_id != turn_id:
            return self._stale(event, "turn_stage", turn_id)
        return turn

    def _berry_delta(self, event: BerryDeltaReceived) -> list[SessionEffect]:
        turn = self._berry_turn(event, event.turn_id, event.generation)
        if not isinstance(turn, TurnContext):
            return [turn]
        if not event.delta:
            return []
        return [
            SendOutbound(
                TextDelta(
                    type="TEXT_DELTA",
                    user_id=self.state.user_id,
                    session_id=self.state.session_id,
                    turn_id=turn.turn_id,
                    interrupt=turn.interrupted,
                    delta=event.delta,
                )
            )
        ]

    def _berry_completed(self, event: BerryCompleted) -> list[SessionEffect]:
        turn = self._berry_turn(event, event.turn_id, event.generation)
        if not isinstance(turn, TurnContext):
            return [turn]
        if not event.reply_text.strip():
            return self._finish_berry_failure(
                turn,
                code="BERRY_EMPTY_REPLY",
                message="Berry reply text is empty",
            )
        turn.reply_text = event.reply_text
        self.state.active_llm_turn_id = None
        effects: list[SessionEffect] = [
            SendOutbound(
                TextEnd(
                    type="TEXT_END",
                    user_id=self.state.user_id,
                    session_id=self.state.session_id,
                    turn_id=turn.turn_id,
                    interrupt=turn.interrupted,
                    text=turn.reply_text,
                )
            )
        ]
        if turn.interrupted:
            turn.stage = TurnStage.INTERRUPTED
            effects.append(self._response_end(turn, "INTERRUPTED"))
            if self.state.llm_queue:
                effects.append(self._start_berry(interrupt_first=True))
            return effects

        turn.stage = TurnStage.STREAMING_TTS
        turn.tts_generation += 1
        effects.append(
            StartTts(
                turn.turn_id,
                turn.tts_generation,
                user_input=turn.asr_text,
                reply_text=turn.reply_text,
            )
        )
        return effects

    def _berry_failed(self, event: BerryFailed) -> list[SessionEffect]:
        turn = self._berry_turn(event, event.turn_id, event.generation)
        if not isinstance(turn, TurnContext):
            return [turn]
        self.state.active_llm_turn_id = None
        turn.stage = TurnStage.FAILED
        effects: list[SessionEffect] = [
            self._error(turn.turn_id, turn.interrupted, "LLM", event.code, event.message),
            self._response_end(turn, "FAILED"),
        ]
        if self.state.llm_queue:
            effects.append(self._start_berry(interrupt_first=turn.interrupted))
        return effects

    def _finish_berry_failure(
        self, turn: TurnContext, code: str, message: str
    ) -> list[SessionEffect]:
        self.state.active_llm_turn_id = None
        turn.stage = TurnStage.FAILED
        effects: list[SessionEffect] = [
            self._error(turn.turn_id, turn.interrupted, "LLM", code, message),
            self._response_end(turn, "FAILED"),
        ]
        if self.state.llm_queue:
            effects.append(self._start_berry(interrupt_first=turn.interrupted))
        return effects

    def _tts_turn(
        self, event: object, turn_id: int, generation: int
    ) -> TurnContext | SessionEffect:
        turn = self.state.turns.get(turn_id)
        if turn is None:
            return self._stale(event, "unknown_turn", turn_id)
        if generation != turn.tts_generation:
            return self._stale(event, "tts_generation", turn_id)
        if turn.stage is not TurnStage.STREAMING_TTS:
            return self._stale(event, "turn_stage", turn_id)
        return turn

    def _tts_chunk(self, event: TtsChunkReceived) -> list[SessionEffect]:
        turn = self._tts_turn(event, event.turn_id, event.generation)
        if not isinstance(turn, TurnContext):
            return [turn]
        if turn.interrupted:
            return [RecordDiscardedAudio(turn.turn_id, event.generation, len(event.pcm16))]
        sequence = turn.next_audio_sequence
        turn.next_audio_sequence += 1
        return [
            SendOutbound(
                AudioDelta(
                    type="AUDIO_DELTA",
                    user_id=self.state.user_id,
                    session_id=self.state.session_id,
                    turn_id=turn.turn_id,
                    interrupt=False,
                    sequence=sequence,
                    audio_format="PCM16",
                    sample_rate=self.state.sample_rate,
                    channels=1,
                    audio_b64=base64.b64encode(event.pcm16).decode("ascii"),
                )
            )
        ]

    def _tts_completed(self, event: TtsCompleted) -> list[SessionEffect]:
        turn = self._tts_turn(event, event.turn_id, event.generation)
        if not isinstance(turn, TurnContext):
            return [turn]
        status = "INTERRUPTED" if turn.interrupted else "COMPLETED"
        turn.stage = TurnStage.INTERRUPTED if turn.interrupted else TurnStage.COMPLETED
        return [self._response_end(turn, status)]

    def _tts_failed(self, event: TtsFailed) -> list[SessionEffect]:
        turn = self._tts_turn(event, event.turn_id, event.generation)
        if not isinstance(turn, TurnContext):
            return [turn]
        turn.stage = TurnStage.FAILED
        return [
            self._error(turn.turn_id, turn.interrupted, "TTS", event.code, event.message),
            self._response_end(turn, "FAILED"),
        ]

    def _response_end(self, turn: TurnContext, status: str) -> SendOutbound:
        return SendOutbound(
            ResponseEnd(
                type="RESPONSE_END",
                user_id=self.state.user_id,
                session_id=self.state.session_id,
                turn_id=turn.turn_id,
                interrupt=turn.interrupted,
                status=status,
            )
        )

    def _error(
        self, turn_id: int, interrupted: bool, stage: str, code: str, message: str
    ) -> SendOutbound:
        return SendOutbound(
            ErrorMessage(
                type="ERROR",
                user_id=self.state.user_id,
                session_id=self.state.session_id,
                turn_id=turn_id,
                interrupt=interrupted,
                stage=stage,
                code=code,
                message=message,
                recoverable=True,
            )
        )

    @staticmethod
    def _stale(event: object, reason: str, turn_id: int | None = None) -> RecordStaleEvent:
        return RecordStaleEvent(type(event).__name__, reason, turn_id)
