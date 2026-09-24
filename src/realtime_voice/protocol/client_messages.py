"""Pydantic models for V1 client-to-server messages."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, field_validator


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
    rag_enabled: StrictBool = False
    scenes: list[str] = Field(default_factory=list)

    @field_validator("scenes")
    @classmethod
    def normalize_scenes(cls, values: list[str]) -> list[str]:
        if any(not value.strip() for value in values):
            raise ValueError("scenes must contain nonempty names")
        values = list(dict.fromkeys(value.strip() for value in values))
        if len(values) > 3:
            raise ValueError("scenes supports at most 3 distinct names")
        return values


class AudioChunkMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["AUDIO_CHUNK"]
    session_id: str = Field(min_length=1, max_length=128)
    sequence: int = Field(ge=0)
    timestamp_ms: int | None = Field(default=None, ge=0)
    audio_b64: str = Field(min_length=1)


class SpeakerRegister(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["SPEAKER_REGISTER"]
    session_id: str = Field(min_length=1, max_length=128)
    request_id: str = Field(min_length=1, max_length=128)
    audio_format: Literal["PCM16"]
    sample_rate: Literal[16000, 24000, 48000]
    channels: Literal[1]
    audio_b64: str = Field(min_length=1)


class CloseSession(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["CLOSE_SESSION"]
    session_id: str = Field(min_length=1, max_length=128)


ClientMessage = Annotated[
    CreateSession | AudioChunkMessage | SpeakerRegister | CloseSession,
    Field(discriminator="type"),
]
