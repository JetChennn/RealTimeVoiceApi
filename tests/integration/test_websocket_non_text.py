import pytest

from realtime_voice.protocol.errors import ProtocolViolation
from realtime_voice.transport.workers import WebSocketReceiver


class BinaryFrame:
    async def receive_text(self) -> str:
        raise KeyError("text")


async def test_receiver_normalizes_binary_frame_to_invalid_message() -> None:
    """A binary frame must not leak a framework KeyError through the runtime."""
    receiver = WebSocketReceiver(BinaryFrame(), "session-1", 16000, object(), lambda: None)

    with pytest.raises(ProtocolViolation, match="INVALID_MESSAGE"):
        await receiver.run()
