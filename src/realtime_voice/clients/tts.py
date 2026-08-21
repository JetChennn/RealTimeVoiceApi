"""Async streaming client for PromptDialogAPI dialogue TTS."""

import asyncio
import base64
import binascii
from collections.abc import AsyncIterator
from dataclasses import dataclass

import httpx

from realtime_voice.clients.limits import AdmissionOverloaded, BoundedAdmission
from realtime_voice.clients.ndjson import iter_ndjson

TTS_SAMPLE_RATE = 24000
TTS_FIRST_AUDIO_TIMEOUT = 60.0
TTS_IDLE_TIMEOUT = 30.0


class TtsStreamError(RuntimeError):
    """Raised when PromptDialogAPI cannot provide valid TTS audio."""


@dataclass(frozen=True, slots=True)
class TtsRequest:
    """Dialogue values required by PromptDialogAPI's TTS endpoint."""

    user_input: str
    model_reply: str
    trace_id: str


@dataclass(frozen=True, slots=True)
class TtsChunk:
    """One validated 24 kHz little-endian PCM16 audio chunk."""

    chunk_index: int
    pcm16_24k: bytes
    finalize: bool


class TtsClient:
    """Stream dialogue TTS using an injected shared HTTP client and admission limiter."""

    def __init__(
        self,
        http: httpx.AsyncClient,
        admission: BoundedAdmission,
        *,
        first_audio_timeout: float = TTS_FIRST_AUDIO_TIMEOUT,
        idle_timeout: float = TTS_IDLE_TIMEOUT,
    ) -> None:
        self.http = http
        self.admission = admission
        self.first_audio_timeout = first_audio_timeout
        self.idle_timeout = idle_timeout

    async def stream(self, request: TtsRequest) -> AsyncIterator[TtsChunk]:
        """Yield validated audio as it arrives, without buffering the HTTP stream."""
        payload = {
            "user_input": request.user_input,
            "model_reply": request.model_reply,
            "include_prompt_event": False,
            "trace_id": request.trace_id,
        }

        async with self.admission.slot():
            try:
                async with self.http.stream(
                    "POST", "/v1/dialogue-tts/stream", json=payload
                ) as response:
                    response.raise_for_status()
                    events = iter_ndjson(response.aiter_bytes())
                    first_audio = True
                    while chunk := await _next_audio_chunk(
                        events,
                        self.first_audio_timeout if first_audio else self.idle_timeout,
                    ):
                        first_audio = False
                        yield chunk
            except AdmissionOverloaded:
                raise
            except TtsStreamError:
                raise
            except TimeoutError as error:
                raise TtsStreamError("TTS stream timed out") from error
            except (binascii.Error, httpx.HTTPError, KeyError, TypeError, ValueError) as error:
                raise TtsStreamError("TTS stream failed") from error


async def _next_audio_chunk(
    events: AsyncIterator[dict[str, object]], timeout: float
) -> TtsChunk | None:
    """Read through prompt metadata until one audio chunk or stream end arrives."""
    try:
        async with asyncio.timeout(timeout):
            while True:
                event = await anext(events)
                chunk = _decode_audio_event(event)
                if chunk is not None:
                    return chunk
    except StopAsyncIteration:
        return None


def _decode_audio_event(event: dict[str, object]) -> TtsChunk | None:
    """Validate one PromptDialogAPI event and convert its PCM payload."""
    if "error" in event:
        message = event.get("message")
        raise TtsStreamError(message if isinstance(message, str) and message else str(event["error"]))

    if event.get("event") == "prompt":
        return None

    sample_rate = event["sample_rate"]
    if type(sample_rate) is not int:
        raise TypeError("TTS sample_rate must be an integer")
    if sample_rate != TTS_SAMPLE_RATE:
        raise TtsStreamError("TTS_SAMPLE_RATE") from ValueError("TTS_SAMPLE_RATE")

    chunk_index = event["chunk_index"]
    if type(chunk_index) is not int:
        raise TypeError("TTS chunk_index must be an integer")
    if chunk_index < 0:
        raise ValueError("TTS chunk_index must not be negative")

    finalize = event["finalize"]
    if type(finalize) is not bool:
        raise TypeError("TTS finalize must be a boolean")

    encoded_audio = event["audio_i16le_b64"]
    if not isinstance(encoded_audio, str):
        raise TypeError("TTS audio_i16le_b64 must be text")
    audio = base64.b64decode(encoded_audio, validate=True)
    if not audio:
        raise TtsStreamError("TTS_PCM_EMPTY") from ValueError("TTS_PCM_EMPTY")
    if len(audio) % 2:
        raise TtsStreamError("TTS_PCM_ALIGNMENT") from ValueError("TTS_PCM_ALIGNMENT")

    return TtsChunk(chunk_index, audio, finalize)
