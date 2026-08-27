"""BerryThinker 回复流式与会话控制的异步客户端。"""

from collections.abc import AsyncIterator
from dataclasses import dataclass
from enum import Enum
from urllib.parse import quote

import httpx

from realtime_voice.clients.limits import BoundedAdmission
from realtime_voice.clients.ndjson import iter_ndjson


class ThinkerError(RuntimeError):
    """BerryThinker 客户端边界抛出的基础错误。"""


class ThinkerStreamError(ThinkerError):
    """当回复流无法提供有效 Thinker 事件时抛出。"""


class ThinkerInterruptError(ThinkerError):
    """当 Thinker 中断控制调用失败时抛出。"""


class ThinkerCleanupError(ThinkerError):
    """当 Thinker 会话无法清理时抛出。"""


@dataclass(frozen=True, slots=True)
class ThinkerReplyRequest:
    """BerryThinker 回复端点所需的多模态字段。"""

    user_id: str
    session_id: str
    text: str
    audio_wav: bytes


@dataclass(frozen=True, slots=True)
class ThinkerTextDelta:
    """BerryThinker 回复文本的一个增量片段。"""

    delta: str


@dataclass(frozen=True, slots=True)
class ThinkerDone:
    """BerryThinker 完整的回复文本。"""

    reply_text: str


ThinkerEvent = ThinkerTextDelta | ThinkerDone


class DeleteResult(str, Enum):
    """会话清理请求的成功结果。"""

    DELETED = "deleted"
    NOT_FOUND = "not_found"


class ThinkerClient:
    """使用注入的共享 HTTP 客户端执行 BerryThinker 操作。"""

    def __init__(self, http: httpx.AsyncClient, admission: BoundedAdmission) -> None:
        self.http = http
        self.admission = admission

    async def stream_reply(self, request: ThinkerReplyRequest) -> AsyncIterator[ThinkerEvent]:
        """发送单个 VAD 分段并产出 Thinker 事件，不缓冲整个流。"""
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
                        yield _thinker_event(payload)
            except ThinkerStreamError:
                raise
            except (httpx.HTTPError, TypeError, ValueError) as error:
                raise ThinkerStreamError("Thinker reply stream failed") from error

    async def interrupt(self, user_id: str, session_id: str) -> None:
        """中断 BerryThinker 正在进行的回复，且不争用回复容量。"""
        try:
            response = await self.http.post(
                "/api/v1/interrupt",
                json={"user_id": user_id, "session_id": session_id},
                timeout=180.0,
            )
            response.raise_for_status()
        except httpx.HTTPError as error:
            raise ThinkerInterruptError("Thinker interrupt failed") from error

    async def delete_session(self, user_id: str, session_id: str) -> DeleteResult:
        """删除远端会话状态；会话已不存在视为清理成功。"""
        path = (
            f"/api/v1/sessions/{_quote_path_identifier(user_id)}/"
            f"{_quote_path_identifier(session_id)}"
        )
        try:
            response = await self.http.delete(path, timeout=180.0)
        except httpx.HTTPError as error:
            raise ThinkerCleanupError("Thinker session cleanup failed") from error

        if response.status_code == 200:
            return DeleteResult.DELETED
        if response.status_code == 404:
            return DeleteResult.NOT_FOUND
        raise ThinkerCleanupError("Thinker session cleanup failed")


def _quote_path_identifier(value: str) -> str:
    """返回 Thinker 标识符的单个安全 URL 路径段。"""
    if value in {"", ".", ".."}:
        raise ThinkerCleanupError("Thinker session cleanup failed")
    return quote(value, safe="")


def _thinker_event(payload: dict[str, object]) -> ThinkerEvent:
    event_type = payload.get("type")
    if event_type == "text_delta":
        delta = payload.get("delta")
        if not isinstance(delta, str):
            raise ValueError("Thinker text_delta must contain text")
        return ThinkerTextDelta(delta)

    if event_type == "done":
        output = payload.get("output")
        if not isinstance(output, dict):
            raise ValueError("Thinker done must contain an output object")
        reply_text = output.get("reply_text")
        if not isinstance(reply_text, str):
            raise ValueError("Thinker done output must contain reply text")
        return ThinkerDone(reply_text)

    if event_type == "error":
        message = payload.get("error_message")
        raise ThinkerStreamError(message if isinstance(message, str) else "thinker stream failed")

    raise ValueError("Thinker stream event type is invalid")
