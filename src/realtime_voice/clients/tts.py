"""PromptDialogAPI 对话 TTS 的异步流式客户端。"""

import asyncio
import base64
import binascii
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack
from dataclasses import dataclass

import httpx

from realtime_voice.clients.limits import AdmissionOverloaded, BoundedAdmission
from realtime_voice.clients.ndjson import iter_ndjson

TTS_SAMPLE_RATE = 24000
TTS_FIRST_AUDIO_TIMEOUT = 60.0
TTS_IDLE_TIMEOUT = 30.0


class TtsStreamError(RuntimeError):
    """当 PromptDialogAPI 无法提供有效 TTS 音频时抛出。"""


@dataclass(frozen=True, slots=True)
class TtsRequest:
    """PromptDialogAPI TTS 端点所需的对话字段。"""

    user_input: str
    model_reply: str
    trace_id: str


@dataclass(frozen=True, slots=True)
class TtsChunk:
    """单个已校验的 24 kHz 小端 PCM16 音频块。"""

    chunk_index: int
    pcm16_24k: bytes
    finalize: bool


class TtsClient:
    """使用注入的共享 HTTP 客户端与准入限流器流式获取对话 TTS。"""

    def __init__(
        self,
        http: httpx.AsyncClient,
        admission: BoundedAdmission,
        *,
        first_audio_timeout: float = TTS_FIRST_AUDIO_TIMEOUT,
        idle_timeout: float = TTS_IDLE_TIMEOUT,
        prompt_override: str = "",
    ) -> None:
        self.http = http
        self.admission = admission
        self.first_audio_timeout = first_audio_timeout
        self.idle_timeout = idle_timeout
        self.prompt_override = prompt_override

    async def stream(self, request: TtsRequest) -> AsyncIterator[TtsChunk]:
        """流式产出校验后的音频块，不缓冲整个 HTTP 流。"""
        payload: dict[str, object] = {
            "user_input": request.user_input,
            "model_reply": request.model_reply,
            "include_prompt_event": False,
            "trace_id": request.trace_id,
        }
        if self.prompt_override:
            # 传固定 prompt 覆盖，跳过 TTS 内部的远程 qwen-flash prompt 生成
            payload["prompt_override"] = self.prompt_override

        async with self.admission.slot():
            try:
                timeout_code = (
                    "TTS_FIRST_AUDIO_TIMEOUT"  # 标记当前阶段，超时时据此区分首包超时与空闲超时
                )
                first_deadline = (
                    asyncio.get_running_loop().time() + self.first_audio_timeout
                )  # 首包截止时间：必须在此限内收到首个音频块
                request_timeout = httpx.Timeout(
                    connect=max(self.first_audio_timeout + 1.0, 1.0),
                    read=None,
                    write=max(self.first_audio_timeout + 1.0, 1.0),
                    pool=max(self.first_audio_timeout + 1.0, 1.0),
                )
                async with AsyncExitStack() as response_stack:
                    async with asyncio.timeout_at(first_deadline):
                        response = await response_stack.enter_async_context(
                            self.http.stream(
                                "POST",
                                "/v1/dialogue-tts/stream",
                                json=payload,
                                timeout=request_timeout,
                            )
                        )
                        response.raise_for_status()
                        events = iter_ndjson(response.aiter_bytes())
                        first_chunk = await _next_audio_chunk(events)

                    if first_chunk is None:
                        return
                    yield first_chunk

                    timeout_code = "TTS_STREAM_IDLE_TIMEOUT"  # 已收到首包，切换为空闲超时标记
                    while chunk := await _next_audio_chunk(events, timeout=self.idle_timeout):
                        yield chunk
            except AdmissionOverloaded:
                raise
            except TtsStreamError:
                raise
            except TimeoutError as error:
                raise TtsStreamError(timeout_code) from error
            except (binascii.Error, httpx.HTTPError, KeyError, TypeError, ValueError) as error:
                raise TtsStreamError("TTS stream failed") from error


async def _next_audio_chunk(
    events: AsyncIterator[dict[str, object]], timeout: float | None = None
) -> TtsChunk | None:
    """跳过 prompt 元数据事件，直至读取到一个音频块或流结束。"""
    while True:
        try:
            if timeout is None:
                event = await anext(events)
            else:
                async with asyncio.timeout(timeout):
                    event = await anext(events)
        except StopAsyncIteration:
            return None

        chunk = _decode_audio_event(event)
        if chunk is not None:
            return chunk


def _decode_audio_event(event: dict[str, object]) -> TtsChunk | None:
    """校验单个 PromptDialogAPI 事件并转换其 PCM 载荷。"""
    if "error" in event:
        message = event.get("message")
        raise TtsStreamError(
            message if isinstance(message, str) and message else str(event["error"])
        )

    if event.get("event") == "prompt":
        return None

    sample_rate = event["sample_rate"]
    if type(sample_rate) is not int:  # 用 type() 而非 isinstance() 以拒绝 bool（bool 是 int 子类）
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
    if len(audio) % 2:  # PCM16 每样本 2 字节，字节数必须为偶数
        raise TtsStreamError("TTS_PCM_ALIGNMENT") from ValueError("TTS_PCM_ALIGNMENT")

    return TtsChunk(chunk_index, audio, finalize)
