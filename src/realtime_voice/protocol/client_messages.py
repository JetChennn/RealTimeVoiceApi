"""Pydantic models for V1 client-to-server messages."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field


class CreateSession(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["CREATE_SESSION"]
    protocol_version: Literal[1]
    device_id: str = Field(min_length=1, max_length=128)
    session_id: str = Field(min_length=1, max_length=128)
    audio_format: Literal["PCM16"]
    audio_transport: Literal["BASE64_JSON"]
    sample_rate: Literal[16000, 24000, 48000]
    channels: Literal[1]


class AudioChunkMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["AUDIO_CHUNK"]
    session_id: str = Field(min_length=1, max_length=128)
    sequence: int = Field(ge=0)
    timestamp_ms: int | None = Field(default=None, ge=0)
    audio_b64: str = Field(min_length=1)


class CloseSession(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["CLOSE_SESSION"]
    session_id: str = Field(min_length=1, max_length=128)


ClientMessage = Annotated[
    CreateSession | AudioChunkMessage | CloseSession,
    Field(discriminator="type"),
]
