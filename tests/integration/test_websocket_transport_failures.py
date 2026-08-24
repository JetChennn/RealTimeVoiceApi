import base64

import pytest

from realtime_voice.protocol.errors import ProtocolViolation
from realtime_voice.session.runtime import BoundedByteQueue
from realtime_voice.transport.workers import WebSocketReceiver


class TextFrames:
    def __init__(self, frames: list[str]) -> None:
        self._frames = iter(frames)

    async def receive_text(self) -> str:
        return next(self._frames)


async def test_receiver_maps_audio_queue_message_count_overflow_to_backpressure() -> None:
    """A QueueFull from count admission must not escape as an internal failure."""
    audio = BoundedByteQueue.audio(maxsize=1, max_bytes=32000)
    audio.put_nowait(bytes(2))
    frame = {
        "type": "AUDIO_CHUNK",
        "session_id": "session-1",
        "sequence": 0,
        "audio_b64": base64.b64encode(bytes(320)).decode(),
    }
    receiver = WebSocketReceiver(
        TextFrames([__import__("json").dumps(frame)]),
        "session-1",
        16000,
        audio,
        lambda: None,
    )

    with pytest.raises(ProtocolViolation, match="CLIENT_AUDIO_BACKPRESSURE"):
        await receiver.run()
