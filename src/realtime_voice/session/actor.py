"""单会话的纯同步状态机：把事件翻译为运行时 Effect。"""

from __future__ import annotations

import base64
from collections.abc import Callable
from dataclasses import dataclass
from random import choice
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
    AsrKeywordsExtracted,
    AsrSucceeded,
    AudioSegmentDiscarded,
    SessionDisconnected,
    SessionEvent,
    SpeechSegmentReady,
    ThinkerCompleted,
    ThinkerDeltaReceived,
    ThinkerFailed,
    ThinkerSkipped,
    TtsChunkReceived,
    TtsCompleted,
    TtsFailed,
    UserInputCommitted,
    UserInputFailed,
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
class ExtractAsrKeywords:
    turn_id: int
    generation: int
    text: str


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
    tone: str


@dataclass(frozen=True, slots=True)
class StartBistreamTts:
    """收到首个 LLM delta 后建立双向 TTS 会话。"""

    turn_id: int
    generation: int


@dataclass(frozen=True, slots=True)
class SendBistreamText:
    """向已建立的双向 TTS 会话发送一段原始 delta。"""

    turn_id: int
    generation: int
    delta: str


@dataclass(frozen=True, slots=True)
class EndBistreamTts:
    """通知双向 TTS 本轮文本输入结束。"""

    turn_id: int
    generation: int


@dataclass(frozen=True, slots=True)
class CancelBistreamTts:
    """取消双向 TTS，会话清理必须幂等。"""

    turn_id: int
    generation: int


@dataclass(frozen=True, slots=True)
class ThinkerFallbackPolicy:
    """网关在 Thinker 失败后使用的固定回复策略。"""

    enabled: bool
    texts: tuple[str, ...]
    tone: str


@dataclass(frozen=True, slots=True)
class RecordThinkerFallback:
    """记录一次 Thinker 保底，以及是否因打断跳过 TTS。"""

    turn_id: int
    code: str
    tts_requested: bool


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
    | StartBistreamTts
    | SendBistreamText
    | EndBistreamTts
    | CancelBistreamTts
    | RecordThinkerFallback
    | CloseRuntime
    | RecordStaleEvent
    | RecordDiscardedAudio
    | QueueAsr
    | ExtractAsrKeywords
)


class SessionActor:
    """持有单个会话的状态，把事件翻译为运行时 Effect。"""

    def __init__(
        self,
        state: SessionState,
        *,
        thinker_fallback: ThinkerFallbackPolicy | None = None,
        bistream_enabled: bool = False,
        fallback_selector: Callable[[tuple[str, ...]], str] = choice,
    ):
        self.state = state
        self._thinker_fallback = thinker_fallback
        self._bistream_enabled = bistream_enabled
        self._fallback_selector = fallback_selector
        if thinker_fallback is not None and thinker_fallback.enabled:
            if not thinker_fallback.texts or any(not text.strip() for text in thinker_fallback.texts):
                raise ValueError("enabled thinker fallback requires non-empty texts")
            if not thinker_fallback.tone.strip():
                raise ValueError("enabled thinker fallback requires a non-empty tone")

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
        if isinstance(event, AudioSegmentDiscarded):
            stale_reason = self._asr_stale_reason(event.segment_id)
            if stale_reason is not None:
                return [self._stale(event, stale_reason)]
            self.state.pending_asr_segment_ids.remove(event.segment_id)
            return []
        if isinstance(event, UserInputCommitted):
            return self._commit_user_input(event)
        if isinstance(event, UserInputFailed):
            return [self._error(0, False, "ASR", event.code, event.message)]
        if isinstance(event, AsrSucceeded):
            return self._asr_succeeded(event)
        if isinstance(event, AsrFailed):
            return self._asr_failed(event)
        if isinstance(event, ThinkerDeltaReceived):
            return self._thinker_delta(event)
        if isinstance(event, ThinkerCompleted):
            return self._thinker_completed(event)
        if isinstance(event, AsrKeywordsExtracted):
            turn = self.state.turns.get(event.turn_id)
            if (
                turn is None
                or turn.thinker_generation != event.generation
                or turn.interrupted
                or turn.thinker_fallback_used
                or turn.asr_keywords_status != "pending"
            ):
                return [self._stale(event, "asr_keywords", event.turn_id)]
            turn.asr_keywords = event.context if event.status == "success" else ""
            turn.asr_keywords_status = event.status
            return []
        if isinstance(event, ThinkerSkipped):
            turn = self._thinker_turn(event, event.turn_id, event.generation)
            if not isinstance(turn, TurnContext):
                return [turn]
            if not turn.interrupted:
                return [self._stale(event, "not_interrupted", event.turn_id)]
            self.state.active_llm_turn_id = None
            turn.stage = TurnStage.INTERRUPTED
            effects = [self._response_end(turn, "INTERRUPTED")]
            if self.state.llm_queue:
                effects.append(self._start_thinker(interrupt_first=False))
            return effects
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

        return self._commit_user_input(event)

    def _commit_user_input(self, event: AsrSucceeded | UserInputCommitted) -> list[SessionEffect]:
        # 新一轮用户输入到达：打断所有未完成轮次，开新轮并入 LLM 队列
        effects = self._interrupt_unfinished_turns()
        turn_id = self.state.next_turn_id
        self.state.next_turn_id += 1
        turn = TurnContext(
            turn_id,
            event.text,
            event.audio_wav,
            trailing_silence_ms=event.trailing_silence_ms,
        )
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
        effects: list[SessionEffect] = [
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
        if self._bistream_enabled:
            if not turn.tts_started:
                turn.tts_started = True
                turn.tts_generation += 1
                effects.insert(0, StartBistreamTts(turn.turn_id, turn.tts_generation))
            if turn.tts_failure is None:
                effects.insert(1, SendBistreamText(turn.turn_id, turn.tts_generation, event.delta))
        return effects

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

        # 进入 TTS 流式阶段。bistream 的输入已随 Thinker delta 发送，此处只收尾。
        turn.stage = TurnStage.STREAMING_TTS
        if self._bistream_enabled:
            if turn.tts_failure is not None:
                code, message = turn.tts_failure
                turn.stage = TurnStage.FAILED
                effects.extend([self._error(turn.turn_id, False, "TTS", code, message), self._response_end(turn, "FAILED")])
            else:
                if not turn.tts_started:
                    turn.tts_started = True
                    turn.tts_generation += 1
                    effects.extend([
                        StartBistreamTts(turn.turn_id, turn.tts_generation),
                        SendBistreamText(turn.turn_id, turn.tts_generation, turn.reply_text),
                    ])
                turn.tts_input_closed = True
                effects.append(EndBistreamTts(turn.turn_id, turn.tts_generation))
        else:
            turn.tts_generation += 1
            effects.append(
                StartTts(
                    turn.turn_id,
                    turn.tts_generation,
                    user_input=turn.asr_text,
                    reply_text=turn.reply_text,
                    tone=event.tone,
                )
            )
        turn.asr_keywords_status = "pending"
        effects.insert(0, ExtractAsrKeywords(turn.turn_id, turn.thinker_generation, turn.reply_text))
        return effects

    def _thinker_failed(self, event: ThinkerFailed) -> list[SessionEffect]:
        """Thinker 失败：按配置进入保底 TTS，或沿用原失败收尾。"""
        turn = self._thinker_turn(event, event.turn_id, event.generation)
        if not isinstance(turn, TurnContext):
            return [turn]
        return self._finish_thinker_failure(
            turn,
            event.code,
            event.message,
            suppress_error_after_interrupt=True,
        )

    def _finish_thinker_failure(
        self,
        turn: TurnContext,
        code: str,
        message: str,
        *,
        suppress_error_after_interrupt: bool = False,
    ) -> list[SessionEffect]:
        """以保底或失败收尾 Thinker，并按需接续下一轮。"""
        if turn.interrupted and suppress_error_after_interrupt:
            self.state.active_llm_turn_id = None
            turn.stage = TurnStage.INTERRUPTED
            effects: list[SessionEffect] = []
            if self._bistream_enabled and turn.tts_started:
                effects.append(CancelBistreamTts(turn.turn_id, turn.tts_generation))
            effects.append(self._response_end(turn, "INTERRUPTED"))
            if self.state.llm_queue:
                effects.append(self._start_thinker(interrupt_first=True))
            return effects
        if self._bistream_enabled and turn.tts_started:
            self.state.active_llm_turn_id = None
            turn.stage = TurnStage.FAILED
            effects: list[SessionEffect] = [
                CancelBistreamTts(turn.turn_id, turn.tts_generation),
                self._error(turn.turn_id, turn.interrupted, "LLM", code, message),
                self._response_end(turn, "FAILED"),
            ]
            if self.state.llm_queue:
                effects.append(self._start_thinker(interrupt_first=False))
            return effects
        fallback = self._thinker_fallback
        if fallback is not None and fallback.enabled:
            return self._start_thinker_fallback(turn, code, fallback)

        self.state.active_llm_turn_id = None
        turn.stage = TurnStage.FAILED
        effects: list[SessionEffect] = [
            self._error(turn.turn_id, turn.interrupted, "LLM", code, message),
            self._response_end(turn, "FAILED"),
        ]
        if self.state.llm_queue:
            effects.append(self._start_thinker(interrupt_first=turn.interrupted))
        return effects

    def _start_thinker_fallback(
        self,
        turn: TurnContext,
        code: str,
        fallback: ThinkerFallbackPolicy,
    ) -> list[SessionEffect]:
        """落定保底文本；被打断则只回文本，否则进入正常 TTS 流程。"""
        turn.reply_text = self._fallback_selector(fallback.texts)
        turn.thinker_fallback_used = True
        turn.thinker_failure_code = code
        self.state.active_llm_turn_id = None
        tts_requested = not turn.interrupted
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
            ),
            RecordThinkerFallback(turn.turn_id, code, tts_requested),
        ]
        if turn.interrupted:
            turn.stage = TurnStage.INTERRUPTED
            effects.append(self._response_end(turn, "INTERRUPTED"))
            if self.state.llm_queue:
                effects.append(self._start_thinker(interrupt_first=True))
            return effects

        turn.stage = TurnStage.STREAMING_TTS
        turn.tts_generation += 1
        if self._bistream_enabled:
            turn.tts_started = True
            turn.tts_input_closed = True
            effects.extend([
                StartBistreamTts(turn.turn_id, turn.tts_generation),
                SendBistreamText(turn.turn_id, turn.tts_generation, turn.reply_text),
                EndBistreamTts(turn.turn_id, turn.tts_generation),
            ])
        else:
            effects.append(
                StartTts(
                    turn.turn_id,
                    turn.tts_generation,
                    user_input=turn.asr_text,
                    reply_text=turn.reply_text,
                    tone=fallback.tone,
                )
            )
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
        if turn.stage not in {TurnStage.STREAMING_LLM, TurnStage.STREAMING_TTS}:
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
        # 正常 bistream 不会在 end_turn 前结束；若服务异常提前结束，等待 Thinker done
        # 统一完成最终状态判断。
        if turn.stage is TurnStage.STREAMING_LLM:
            # 用户打断时，TTS 提前结束是预期的取消结果；仍由 Thinker 的终态事件
            # 负责释放 active_llm_turn_id 和发送 INTERRUPTED。
            if turn.interrupted:
                return []
            turn.tts_failure = ("TTS_STREAM_FAILED", "TTS ended before Thinker completed")
            return []
        status = "INTERRUPTED" if turn.interrupted else "COMPLETED"
        turn.stage = TurnStage.INTERRUPTED if turn.interrupted else TurnStage.COMPLETED  # 进入终态
        return [self._response_end(turn, status)]

    def _tts_failed(self, event: TtsFailed) -> list[SessionEffect]:
        """TTS 失败：置 FAILED 并下发错误与结束。"""
        turn = self._tts_turn(event, event.turn_id, event.generation)
        if not isinstance(turn, TurnContext):
            return [turn]
        if turn.stage is TurnStage.STREAMING_LLM:
            # 主动打断会关闭 bistream；这不是客户端可见的 TTS 故障。不要抢先
            # 改写 LLM 状态，否则随后到达的 Thinker 收尾事件会变成 stale。
            if turn.interrupted:
                return []
            turn.tts_failure = (event.code, event.message)
            return []
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
