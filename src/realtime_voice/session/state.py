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
    berry_generation: int = 0
    tts_generation: int = 0
    reply_text: str = ""
    next_audio_sequence: int = 0


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
