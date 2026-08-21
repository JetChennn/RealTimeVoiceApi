"""Pydantic models for V1 server-to-client messages."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field


class ServerMessageBase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: str
    user_id: str = Field(min_length=1, max_length=128)
    session_id: str = Field(min_length=1, max_length=128)
    turn_id: int = Field(ge=0)
    interrupt: bool


class SessionCreated(ServerMessageBase):
    type: Literal["SESSION_CREATED"]
    protocol_version: Literal[1]
    turn_id: Literal[0]
    interrupt: Literal[False]
    audio_format: Literal["PCM16"]
    audio_transport: Literal["BASE64_JSON"]
    sample_rate: Literal[16000, 24000, 48000]
    channels: Literal[1]


class AsrResult(ServerMessageBase):
    type: Literal["ASR_RESULT"]
    text: str = Field(min_length=1)


class TextDelta(ServerMessageBase):
    type: Literal["TEXT_DELTA"]
    delta: str = Field(min_length=1)


class TextEnd(ServerMessageBase):
    type: Literal["TEXT_END"]
    text: str = Field(min_length=1)


class AudioDelta(ServerMessageBase):
    type: Literal["AUDIO_DELTA"]
    sequence: int = Field(ge=0)
    audio_format: Literal["PCM16"]
    sample_rate: Literal[16000, 24000, 48000]
    channels: Literal[1]
    audio_b64: str = Field(min_length=1)


class TurnState(ServerMessageBase):
    type: Literal["TURN_STATE"]
    state: Literal["INTERRUPTED"]


class ResponseEnd(ServerMessageBase):
    type: Literal["RESPONSE_END"]
    status: Literal["COMPLETED", "INTERRUPTED", "FAILED"]


class ErrorMessage(ServerMessageBase):
    type: Literal["ERROR"]
    stage: str = Field(min_length=1)
    code: str = Field(min_length=1)
    message: str = Field(min_length=1)
    recoverable: bool


ServerMessage = Annotated[
    SessionCreated
    | AsrResult
    | TextDelta
    | TextEnd
    | AudioDelta
    | TurnState
    | ResponseEnd
    | ErrorMessage,
    Field(discriminator="type"),
]
