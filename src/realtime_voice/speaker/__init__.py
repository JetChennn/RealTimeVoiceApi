"""会话级说话人验证：共享模型、独立会话状态。"""

from realtime_voice.speaker.gate import SessionSpeakerGate, SpeakerDecision, SpeakerRegistration
from realtime_voice.speaker.wespeaker import WeSpeakerOnnxEmbedder

__all__ = (
    "SessionSpeakerGate",
    "SpeakerDecision",
    "SpeakerRegistration",
    "WeSpeakerOnnxEmbedder",
)
