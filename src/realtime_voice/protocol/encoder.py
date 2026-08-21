"""Encode V1 server messages as JSON text frames."""

from realtime_voice.protocol.server_messages import ServerMessage


def encode_server_message(message: ServerMessage) -> str:
    """Serialize a validated server message for a WebSocket text frame."""
    return message.model_dump_json(exclude_none=True)
