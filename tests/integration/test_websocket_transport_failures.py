import base64
import json

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


async def test_receiver_can_drop_overflow_and_continue_with_the_next_sequence() -> None:
    audio = BoundedByteQueue.audio(maxsize=1, max_bytes=32000)
    audio.put_nowait(bytes([1, 0]))
    payload = bytes([2, 0]) * 160
    frames = [
        json.dumps(
            {
                "type": "AUDIO_CHUNK",
                "session_id": "session-1",
                "sequence": sequence,
                "audio_b64": base64.b64encode(payload).decode(),
            }
        )
        for sequence in (0, 1)
    ]
    frames.append(json.dumps({"type": "CLOSE_SESSION", "session_id": "session-1"}))
    reports = []
    closed = False

    def report(limit: str, items: int, queued_bytes: int, chunk_bytes: int) -> None:
        reports.append((limit, items, queued_bytes, chunk_bytes))
        assert audio.get_nowait() == bytes([1, 0])

    def request_close() -> None:
        nonlocal closed
        closed = True

    receiver = WebSocketReceiver(
        TextFrames(frames),
        "session-1",
        16000,
        audio,
        request_close,
        report_audio_backpressure=report,
    )

    await receiver.run()

    assert reports == [("items", 1, 2, len(payload))]
    assert audio.get_nowait() == payload
    assert closed is True
