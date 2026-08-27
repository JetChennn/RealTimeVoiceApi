"""单会话的纯同步状态机：把事件翻译为运行时 Effect。"""

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
    SessionDisconnected,
    SessionEvent,
    SpeechSegmentReady,
    ThinkerCompleted,
    ThinkerDeltaReceived,
    ThinkerFailed,
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
    """把语音段入队交给 ASR 处理。"""

    session_id: str
    segment: SpeechSegment


@dataclass(frozen=True, slots=True)
class SendOutbound:
    """发送一条出站消息给客户端。"""

    message: ServerMessage


@dataclass(frozen=True, slots=True)
class StartThinker:
    """启动一次 Thinker（LLM）生成。"""

    turn_id: int
    generation: int
    text: str
    audio_wav: bytes


@dataclass(frozen=True, slots=True)
class StartNextThinker:
    """启动下一次 Thinker 生成，并指示是否先打断上一轮。"""

    turn_id: int
    generation: int
    text: str
    audio_wav: bytes
    interrupt_first: bool


@dataclass(frozen=True, slots=True)
class StartTts:
    """启动一次 TTS 合成。"""

    turn_id: int
    generation: int
    user_input: str
    reply_text: str


@dataclass(frozen=True, slots=True)
class CloseRuntime:
    """关闭会话运行时。"""

    session_id: str


@dataclass(frozen=True, slots=True)
class RecordStaleEvent:
    """记录一条过期事件，供观测/排查使用。"""

    event_type: str
    reason: str
    turn_id: int | None = None


@dataclass(frozen=True, slots=True)
class RecordDiscardedAudio:
    """记录因打断而丢弃的音频字节数。"""

    turn_id: int
    generation: int
    byte_count: int


SessionEffect: TypeAlias = (
    SendOutbound
    | StartThinker
    | StartNextThinker
    | StartTts
    | CloseRuntime
    | RecordStaleEvent
    | RecordDiscardedAudio
    | QueueAsr
)


class SessionActor:
    """持有单个会话的状态，把事件翻译为运行时 Effect。"""

    def __init__(self, state: SessionState):
        self.state = state

    def handle(self, event: SessionEvent) -> list[SessionEffect]:
        """把会话事件路由到对应处理方法，返回需执行的 Effect 列表。"""
        if event.session_id != self.state.session_id:
            return [self._stale(event, "session")]
        if isinstance(event, SessionDisconnected):
            return self._disconnect(event)
        if self.state.closing:
            return [self._stale(event, "closing")]
        if isinstance(event, SpeechSegmentReady):
            segment_id = event.segment.segment_id
            if segment_id in self.state.registered_asr_segment_ids:
                # 已注册过的段重复到达，按过期丢弃以去重
                return [self._stale(event, "asr_segment")]
            self.state.registered_asr_segment_ids.add(segment_id)
            self.state.pending_asr_segment_ids.add(segment_id)
            return [QueueAsr(self.state.session_id, event.segment)]
        if isinstance(event, AsrSucceeded):
            return self._asr_succeeded(event)
        if isinstance(event, AsrFailed):
            return self._asr_failed(event)
        if isinstance(event, ThinkerDeltaReceived):
            return self._thinker_delta(event)
        if isinstance(event, ThinkerCompleted):
            return self._thinker_completed(event)
        if isinstance(event, ThinkerFailed):
            return self._thinker_failed(event)
        if isinstance(event, TtsChunkReceived):
            return self._tts_chunk(event)
        if isinstance(event, TtsCompleted):
            return self._tts_completed(event)
        if isinstance(event, TtsFailed):
            return self._tts_failed(event)
        raise TypeError(f"unsupported session event: {type(event).__name__}")

    def _disconnect(self, event: SessionDisconnected) -> list[SessionEffect]:
        """标记会话为关闭态并下发关闭运行时 Effect；幂等。"""
        if self.state.closing:
            return []
        self.state.closing = True  # 首次进入关闭态，触发运行时关闭
        return [CloseRuntime(self.state.session_id)]

    def _asr_succeeded(self, event: AsrSucceeded) -> list[SessionEffect]:
        """ASR 成功：消耗段、打断未完成轮次、开新轮；无活跃 LLM 时立即启动 Thinker。"""
        stale_reason = self._asr_stale_reason(event.segment_id)
        if stale_reason is not None:
            return [self._stale(event, stale_reason)]
        self.state.pending_asr_segment_ids.remove(event.segment_id)
        if not event.text:
            return []

        # 新一轮用户输入到达：打断所有未完成轮次，开新轮并入 LLM 队列
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
        # 当前无活跃 LLM 轮，立即出队启动 Thinker，避免队头阻塞
        if self.state.active_llm_turn_id is None:
            effects.append(self._start_thinker(interrupt_first=None))
        return effects

    def _asr_failed(self, event: AsrFailed) -> list[SessionEffect]:
        """ASR 失败：消耗段并下发 ASR 错误（turn_id=0 表示不属于任何轮）。"""
        stale_reason = self._asr_stale_reason(event.segment_id)
        if stale_reason is not None:
            return [self._stale(event, stale_reason)]
        self.state.pending_asr_segment_ids.remove(event.segment_id)
        return [self._error(0, False, "ASR", event.code, event.message)]

    def _asr_stale_reason(self, segment_id: int) -> str | None:
        """判断 ASR 段是否过期：未处理返回 None，已处理返回 'asr_segment'，未注册返回 'unknown_segment'。"""
        if segment_id in self.state.pending_asr_segment_ids:
            return None
        if segment_id in self.state.registered_asr_segment_ids:
            return "asr_segment"
        return "unknown_segment"

    def _interrupt_unfinished_turns(self) -> list[SessionEffect]:
        """把所有未到终态且未打断的轮标记为已打断，并下发 INTERRUPTED 通知。"""
        effects: list[SessionEffect] = []
        for turn in self.state.turns.values():
            if turn.stage in TERMINAL_TURN_STAGES or turn.interrupted:
                continue
            turn.interrupted = True  # 标记打断，后续出站消息携带 interrupt=True
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

    def _start_thinker(self, interrupt_first: bool | None) -> StartThinker | StartNextThinker:
        """从 LLM 队列出队一个轮，置为 STREAMING_LLM 并发起新的 Thinker 生成。

        interrupt_first 为 None 表示正常启动；为 True/False 表示接续上一轮被打断的 Thinker。
        """
        turn_id = self.state.llm_queue.popleft()
        turn = self.state.turns[turn_id]
        turn.thinker_generation += 1
        turn.stage = TurnStage.STREAMING_LLM  # 进入 LLM 流式阶段
        self.state.active_llm_turn_id = turn_id
        assert turn.audio_wav is not None
        values = (turn.turn_id, turn.thinker_generation, turn.asr_text, turn.audio_wav)
        if interrupt_first is None:
            return StartThinker(*values)
        return StartNextThinker(*values, interrupt_first=interrupt_first)

    def _thinker_turn(
        self, event: object, turn_id: int, generation: int
    ) -> TurnContext | SessionEffect:
        """校验 Thinker 事件归属的轮：存在、代次一致、且处于活跃 STREAMING_LLM 阶段。"""
        turn = self.state.turns.get(turn_id)
        if turn is None:
            return self._stale(event, "unknown_turn", turn_id)
        if generation != turn.thinker_generation:
            return self._stale(event, "thinker_generation", turn_id)
        if turn.stage is not TurnStage.STREAMING_LLM or self.state.active_llm_turn_id != turn_id:
            return self._stale(event, "turn_stage", turn_id)
        return turn

    def _thinker_delta(self, event: ThinkerDeltaReceived) -> list[SessionEffect]:
        """转发 Thinker（LLM）增量文本到客户端；空 delta 直接忽略。"""
        turn = self._thinker_turn(event, event.turn_id, event.generation)
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

    def _thinker_completed(self, event: ThinkerCompleted) -> list[SessionEffect]:
        """Thinker 完成：落定回复文本、收尾 LLM 阶段；被打断则结束本轮，否则进入 TTS。"""
        turn = self._thinker_turn(event, event.turn_id, event.generation)
        if not isinstance(turn, TurnContext):
            return [turn]
        if not event.reply_text.strip():
            return self._finish_thinker_failure(
                turn,
                code="THINKER_EMPTY_REPLY",
                message="Thinker reply text is empty",
            )
        turn.reply_text = event.reply_text
        self.state.active_llm_turn_id = None  # LLM 阶段结束，释放活跃槽位
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
            # 本轮已被打断：直接结束，不再进入 TTS；若有排队轮则接续启动
            turn.stage = TurnStage.INTERRUPTED
            effects.append(self._response_end(turn, "INTERRUPTED"))
            if self.state.llm_queue:
                effects.append(self._start_thinker(interrupt_first=True))
            return effects

        # 进入 TTS 流式阶段，发起新的 TTS 合成
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

    def _thinker_failed(self, event: ThinkerFailed) -> list[SessionEffect]:
        """Thinker 失败：置 FAILED、下发错误与结束；若有排队轮则接续启动。"""
        turn = self._thinker_turn(event, event.turn_id, event.generation)
        if not isinstance(turn, TurnContext):
            return [turn]
        self.state.active_llm_turn_id = None  # 释放活跃 LLM 槽位
        turn.stage = TurnStage.FAILED
        effects: list[SessionEffect] = [
            self._error(turn.turn_id, turn.interrupted, "LLM", event.code, event.message),
            self._response_end(turn, "FAILED"),
        ]
        # 队列非空则按上一轮是否被打断决定是否先打断接续
        if self.state.llm_queue:
            effects.append(self._start_thinker(interrupt_first=turn.interrupted))
        return effects

    def _finish_thinker_failure(
        self, turn: TurnContext, code: str, message: str
    ) -> list[SessionEffect]:
        """以失败收尾 Thinker：置 FAILED、下发错误与结束，并按需接续下一轮。"""
        self.state.active_llm_turn_id = None
        turn.stage = TurnStage.FAILED
        effects: list[SessionEffect] = [
            self._error(turn.turn_id, turn.interrupted, "LLM", code, message),
            self._response_end(turn, "FAILED"),
        ]
        if self.state.llm_queue:
            effects.append(self._start_thinker(interrupt_first=turn.interrupted))
        return effects

    def _tts_turn(
        self, event: object, turn_id: int, generation: int
    ) -> TurnContext | SessionEffect:
        """校验 TTS 事件归属的轮：存在、代次一致、且处于 STREAMING_TTS 阶段。"""
        turn = self.state.turns.get(turn_id)
        if turn is None:
            return self._stale(event, "unknown_turn", turn_id)
        if generation != turn.tts_generation:
            return self._stale(event, "tts_generation", turn_id)
        if turn.stage is not TurnStage.STREAMING_TTS:
            return self._stale(event, "turn_stage", turn_id)
        return turn

    def _tts_chunk(self, event: TtsChunkReceived) -> list[SessionEffect]:
        """转发 TTS 音频块到客户端；已打断的轮直接记录丢弃字节，不再下发。"""
        turn = self._tts_turn(event, event.turn_id, event.generation)
        if not isinstance(turn, TurnContext):
            return [turn]
        if turn.interrupted:
            # 轮已被打断：TTS 仍在回流，仅记录丢弃量以便观测
            return [RecordDiscardedAudio(turn.turn_id, event.generation, len(event.pcm16))]
        sequence = turn.next_audio_sequence
        turn.next_audio_sequence += 1  # 单调递增的音频序号，供客户端排序
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
        """TTS 完成：根据是否被打断进入 INTERRUPTED/COMPLETED 终态，并下发结束。"""
        turn = self._tts_turn(event, event.turn_id, event.generation)
        if not isinstance(turn, TurnContext):
            return [turn]
        status = "INTERRUPTED" if turn.interrupted else "COMPLETED"
        turn.stage = TurnStage.INTERRUPTED if turn.interrupted else TurnStage.COMPLETED  # 进入终态
        return [self._response_end(turn, status)]

    def _tts_failed(self, event: TtsFailed) -> list[SessionEffect]:
        """TTS 失败：置 FAILED 并下发错误与结束。"""
        turn = self._tts_turn(event, event.turn_id, event.generation)
        if not isinstance(turn, TurnContext):
            return [turn]
        turn.stage = TurnStage.FAILED
        return [
            self._error(turn.turn_id, turn.interrupted, "TTS", event.code, event.message),
            self._response_end(turn, "FAILED"),
        ]

    def _response_end(self, turn: TurnContext, status: str) -> SendOutbound:
        """构造一条 RESPONSE_END 出站消息。"""
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
        """构造一条 ERROR 出站消息。"""
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
        """构造一条过期事件记录，仅用于观测/排查。"""
        return RecordStaleEvent(type(event).__name__, reason, turn_id)
