"""State owned exclusively by a single session actor."""

from collections import OrderedDict, deque
from dataclasses import dataclass, field
from enum import Enum


class TurnStage(str, Enum):
    WAITING_LLM = "WAITING_LLM"
    STREAMING_LLM = "STREAMING_LLM"
    STREAMING_TTS = "STREAMING_TTS"
    COMPLETED = "COMPLETED"
    INTERRUPTED = "INTERRUPTED"
    FAILED = "FAILED"


TERMINAL_TURN_STAGES = frozenset(
    {TurnStage.COMPLETED, TurnStage.INTERRUPTED, TurnStage.FAILED}
)


@dataclass
class TurnContext:
    turn_id: int
    asr_text: str
    audio_wav: bytes | None
    stage: TurnStage = TurnStage.WAITING_LLM
    interrupted: bool = False
    thinker_generation: int = 0
    tts_generation: int = 0
    reply_text: str = ""
    thinker_fallback_used: bool = False
    thinker_failure_code: str | None = None
    # bistream 允许 LLM 与 TTS 同时活动；这些字段把 TTS 生命周期从 stage 中拆开表示。
    tts_started: bool = False
    tts_input_closed: bool = False
    tts_failure: tuple[str, str] | None = None
    next_audio_sequence: int = 0
    # 当前用户输入被提交前已经确认的尾部静音，用于首帧 TTS 时延的展示口径。
    trailing_silence_ms: float = 0.0


@dataclass
class SessionState:
    user_id: str
    session_id: str
    sample_rate: int
    next_turn_id: int = 1
    active_llm_turn_id: int | None = None
    llm_queue: deque[int] = field(default_factory=deque)
    turns: OrderedDict[int, TurnContext] = field(default_factory=OrderedDict)
    registered_asr_segment_ids: set[int] = field(default_factory=set)
    pending_asr_segment_ids: set[int] = field(default_factory=set)
    closing: bool = False
