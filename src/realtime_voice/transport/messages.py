"""Transport-owned session-level outbound messages."""

from realtime_voice.protocol.client_messages import CreateSession
from realtime_voice.protocol.server_messages import SessionCreated


def session_created(create: CreateSession) -> SessionCreated:
    return SessionCreated(
        type="SESSION_CREATED",
        protocol_version=1,
        user_id=create.device_id,
        session_id=create.session_id,
        turn_id=0,
        interrupt=False,
        audio_format=create.audio_format,
        audio_transport=create.audio_transport,
        sample_rate=create.sample_rate,
        channels=create.channels,
    )
