"""Async client for BerryThinker reply streaming and session controls."""

from collections.abc import AsyncIterator
from dataclasses import dataclass
from enum import Enum
from urllib.parse import quote

import httpx

from realtime_voice.clients.limits import BoundedAdmission
from realtime_voice.clients.ndjson import iter_ndjson


class BerryError(RuntimeError):
    """Base error raised by the BerryThinker client boundary."""


class BerryStreamError(BerryError):
    """Raised when a reply stream cannot provide valid Berry events."""


class BerryInterruptError(BerryError):
    """Raised when the Berry interrupt control call fails."""


class BerryCleanupError(BerryError):
    """Raised when a Berry session cannot be cleaned up."""


@dataclass(frozen=True, slots=True)
class BerryReplyRequest:
    """The multimodal fields required by BerryThinker's reply endpoint."""

    user_id: str
    session_id: str
    text: str
    audio_wav: bytes


@dataclass(frozen=True, slots=True)
class BerryTextDelta:
    """One incremental reply-text fragment from BerryThinker."""

    delta: str


@dataclass(frozen=True, slots=True)
class BerryDone:
    """The complete BerryThinker reply text."""

    reply_text: str


BerryEvent = BerryTextDelta | BerryDone


class DeleteResult(str, Enum):
    """Successful outcomes from a session cleanup request."""

    DELETED = "deleted"
    NOT_FOUND = "not_found"


class BerryClient:
    """Use an injected shared HTTP client for BerryThinker operations."""

    def __init__(self, http: httpx.AsyncClient, admission: BoundedAdmission) -> None:
        self.http = http
        self.admission = admission

    async def stream_reply(self, request: BerryReplyRequest) -> AsyncIterator[BerryEvent]:
        """Send one VAD segment and yield Berry events without buffering the stream."""
        data = {
            "text": request.text,
            "user_id": request.user_id,
            "session_id": request.session_id,
            "stream": "true",
            "reply_mode": "dialogue",
            "audio_is_vad_segment": "true",
            "skip_internal_asr": "true",
        }
        files = {"audio": ("segment.wav", request.audio_wav, "audio/wav")}

        async with self.admission.slot():
            try:
                async with self.http.stream(
                    "POST",
                    "/api/v1/multimodal/reply",
                    data=data,
                    files=files,
                    timeout=180.0,
                ) as response:
                    response.raise_for_status()
                    async for payload in iter_ndjson(response.aiter_bytes()):
                        yield _berry_event(payload)
            except BerryStreamError:
                raise
            except (httpx.HTTPError, TypeError, ValueError) as error:
                raise BerryStreamError("Berry reply stream failed") from error

    async def interrupt(self, user_id: str, session_id: str) -> None:
        """Interrupt BerryThinker's active reply without competing for reply capacity."""
        try:
            response = await self.http.post(
                "/api/v1/interrupt",
                json={"user_id": user_id, "session_id": session_id},
                timeout=180.0,
            )
            response.raise_for_status()
        except httpx.HTTPError as error:
            raise BerryInterruptError("Berry interrupt failed") from error

    async def delete_session(self, user_id: str, session_id: str) -> DeleteResult:
        """Delete remote session state; an already absent session is a successful cleanup."""
        path = f"/api/v1/sessions/{quote(user_id, safe='')}/{quote(session_id, safe='')}"
        try:
            response = await self.http.delete(path, timeout=180.0)
        except httpx.HTTPError as error:
            raise BerryCleanupError("Berry session cleanup failed") from error

        if response.status_code == 200:
            return DeleteResult.DELETED
        if response.status_code == 404:
            return DeleteResult.NOT_FOUND
        raise BerryCleanupError("Berry session cleanup failed")


def _berry_event(payload: dict[str, object]) -> BerryEvent:
    event_type = payload.get("type")
    if event_type == "text_delta":
        delta = payload.get("delta")
        if not isinstance(delta, str):
            raise ValueError("Berry text_delta must contain text")
        return BerryTextDelta(delta)

    if event_type == "done":
        output = payload.get("output")
        if not isinstance(output, dict):
            raise ValueError("Berry done must contain an output object")
        reply_text = output.get("reply_text")
        if not isinstance(reply_text, str):
            raise ValueError("Berry done output must contain reply text")
        return BerryDone(reply_text)

    if event_type == "error":
        message = payload.get("error_message")
        raise BerryStreamError(message if isinstance(message, str) else "berry stream failed")

    raise ValueError("Berry stream event type is invalid")
