"""PromptTTSD 的旧 HTTP 与双向 WebSocket TTS 客户端。"""

import asyncio
import base64
import binascii
import json
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from typing import Literal

import httpx
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import WebSocketException

from realtime_voice.clients.limits import AdmissionOverloaded, BoundedAdmission
from realtime_voice.clients.ndjson import iter_ndjson

TTS_SAMPLE_RATE = 24000
TTS_FIRST_AUDIO_TIMEOUT = 60.0
TTS_IDLE_TIMEOUT = 30.0
TTS_CANCEL_WRITE_TIMEOUT = 0.05


class TtsStreamError(RuntimeError):
    """当 PromptTTSD 无法提供有效 TTS 音频时抛出。"""

    def __init__(self, message: str, *, code: str | None = None) -> None:
        super().__init__(message)
        self.code = code or message


class TtsStreamCancelled(RuntimeError):
    """网关主动取消一轮 TTS；这是正常打断，不是下游故障。"""

    def __init__(
        self,
        *,
        cancel_sent: bool,
        connection_aborted: bool,
        cancel_error: str | None = None,
    ) -> None:
        super().__init__("TTS stream cancelled")
        self.cancel_sent = cancel_sent
        self.connection_aborted = connection_aborted
        self.cancel_error = cancel_error


@dataclass(frozen=True, slots=True)
class TtsRequest:
    """一轮 TTS 的公共元数据；旧 HTTP 模式使用 model_reply。"""

    model_reply: str
    prompt: str
    trace_id: str


@dataclass(frozen=True, slots=True)
class TtsText:
    """发往 bistream 会话的一段原始 Thinker 文本增量。"""

    delta: str


@dataclass(frozen=True, slots=True)
class TtsEndTurn:
    """标记本轮文本已完整发送，要求 TTS 收尾。"""


TtsInput = TtsText | TtsEndTurn


@dataclass(slots=True)
class _ActiveBistream:
    """一条活跃 bistream 的可取消资源。"""

    websocket: ClientConnection
    sender: asyncio.Task[None] | None = None


@dataclass(frozen=True, slots=True)
class TtsChunk:
    """单个已校验的 24 kHz 小端 PCM16 音频块。"""

    chunk_index: int
    pcm16_24k: bytes
    finalize: bool


class TtsClient:
    """同时支持 legacy HTTP 与 /v1/tts/bistream 的下游客户端。"""

    def __init__(
        self,
        http: httpx.AsyncClient,
        admission: BoundedAdmission,
        *,
        first_audio_timeout: float = TTS_FIRST_AUDIO_TIMEOUT,
        idle_timeout: float = TTS_IDLE_TIMEOUT,
        mode: Literal["bistream", "dialogue_stream"] = "bistream",
        ref_audio: str = "ref_audio/tongtong.wav",
        ref_text: str = "在一座美丽的森林里，住着许多动物。",
    ) -> None:
        self.http = http
        self.admission = admission
        self.first_audio_timeout = first_audio_timeout
        self.idle_timeout = idle_timeout
        self.mode = mode
        self.ref_audio = ref_audio
        self.ref_text = ref_text
        self._active_bistreams: dict[str, _ActiveBistream] = {}

    @property
    def bistream_enabled(self) -> bool:
        return self.mode == "bistream"

    async def stream(self, request: TtsRequest) -> AsyncIterator[TtsChunk]:
        """旧 dialogue HTTP 流；仅由 dialogue_stream 回滚模式调用。"""
        payload: dict[str, object] = {
            "model_reply": request.model_reply,
            "prompt": request.prompt,
            "include_prompt_event": False,
            "trace_id": request.trace_id,
        }

        async with self.admission.slot():
            try:
                timeout_code = "TTS_FIRST_AUDIO_TIMEOUT"
                first_deadline = asyncio.get_running_loop().time() + self.first_audio_timeout
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
                                "POST", "/v1/dialogue-tts/stream", json=payload, timeout=request_timeout
                            )
                        )
                        response.raise_for_status()
                        events = iter_ndjson(response.aiter_bytes())
                        first_chunk = await _next_http_audio(events)

                    if first_chunk is None:
                        raise TtsStreamError("TTS_PCM_EMPTY", code="TTS_PCM_EMPTY")
                    yield first_chunk

                    timeout_code = "TTS_STREAM_IDLE_TIMEOUT"
                    while chunk := await _next_http_audio(events, timeout=self.idle_timeout):
                        yield chunk
            except AdmissionOverloaded:
                raise
            except TtsStreamError:
                raise
            except TimeoutError as error:
                raise TtsStreamError(timeout_code, code=timeout_code) from error
            except (binascii.Error, httpx.HTTPError, KeyError, TypeError, ValueError) as error:
                raise TtsStreamError("TTS stream failed", code="TTS_STREAM_FAILED") from error

    async def stream_bistream(
        self, request: TtsRequest, inputs: AsyncIterator[TtsInput]
    ) -> AsyncIterator[TtsChunk]:
        """全双工发送文本并接收音频；首音预算覆盖本地与远端排队。"""
        if not self.bistream_enabled:
            raise RuntimeError("stream_bistream is only available in bistream mode")

        timeout_code = "TTS_FIRST_AUDIO_TIMEOUT"
        first_deadline = asyncio.get_running_loop().time() + self.first_audio_timeout
        sender: asyncio.Task[None] | None = None
        sender_holder: list[asyncio.Task[None] | None] = [None]
        websocket: ClientConnection | None = None
        try:
            # 首音预算覆盖本地准入、建连、start 以及服务端排队；拿到第一段音频后
            # 必须立即解除该总预算，后续只受 idle_timeout 约束。
            async with asyncio.timeout_at(first_deadline) as first_audio_deadline:
                async with self.admission.slot():
                    async with _open_cancelable_bistream(
                        self._bistream_url(), sender_holder
                    ) as active_websocket:
                        websocket = active_websocket
                        active_bistream = _ActiveBistream(websocket)
                        self._active_bistreams[request.trace_id] = active_bistream
                        await websocket.send(
                            json.dumps(
                                {
                                    "type": "start",
                                    "request_id": request.trace_id,
                                    "ref_audio": self.ref_audio,
                                    "ref_text": self.ref_text,
                                    "speaker": 0,
                                    "prompt": request.prompt,
                                    "temperature": 0.6,
                                    "top_k": 100,
                                    "top_p": 0.9,
                                    "repetition_penalty": 1.25,
                                    "max_tokens": 3000,
                                },
                                ensure_ascii=False,
                            )
                        )
                        sender = asyncio.create_task(
                            self._send_bistream_inputs(websocket, inputs), name="tts-bistream-sender"
                        )
                        active_bistream.sender = sender
                        sender_holder[0] = sender
                        while True:
                            event = await _receive_bistream_event(
                                websocket, sender, _remaining(first_deadline)
                            )
                            chunk, terminal = _decode_bistream_event(event)
                            if terminal:
                                raise TtsStreamError("TTS_PCM_EMPTY", code="TTS_PCM_EMPTY")
                            if chunk is not None:
                                first_audio_deadline.reschedule(None)
                                yield chunk
                                break

                        timeout_code = "TTS_STREAM_IDLE_TIMEOUT"
                        while True:
                            event = await _receive_bistream_event(websocket, sender, self.idle_timeout)
                            chunk, terminal = _decode_bistream_event(event)
                            if chunk is not None:
                                yield chunk
                            if terminal:
                                return
        except asyncio.CancelledError:
            if sender is not None and not sender.done():
                sender.cancel()
                await asyncio.gather(sender, return_exceptions=True)
            cancel_sent = False
            connection_aborted = False
            if websocket is not None:
                cancel_sent, connection_aborted, cancel_error = await asyncio.shield(
                    _cancel_and_abort_bistream(websocket)
                )
            else:
                cancel_error = None
            raise TtsStreamCancelled(
                cancel_sent=cancel_sent,
                connection_aborted=connection_aborted,
                cancel_error=cancel_error,
            ) from None
        except AdmissionOverloaded:
            raise
        except TtsStreamError:
            raise
        except TimeoutError as error:
            raise TtsStreamError(timeout_code, code=timeout_code) from error
        except (OSError, WebSocketException, json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
            raise TtsStreamError("TTS stream failed", code="TTS_STREAM_FAILED") from error
        finally:
            active_bistream = self._active_bistreams.get(request.trace_id)
            if active_bistream is not None and active_bistream.websocket is websocket:
                self._active_bistreams.pop(request.trace_id, None)
            if sender is not None and not sender.done():
                sender.cancel()
            if sender is not None:
                await asyncio.gather(sender, return_exceptions=True)

    async def cancel_bistream(self, trace_id: str) -> TtsStreamCancelled:
        """取消已建立但当前消费者不在 recv 等待点的 bistream。"""
        active_bistream = self._active_bistreams.get(trace_id)
        if active_bistream is None:
            return TtsStreamCancelled(cancel_sent=False, connection_aborted=False)
        sender = active_bistream.sender
        if sender is not None and not sender.done():
            sender.cancel()
            await asyncio.gather(sender, return_exceptions=True)
        cancel_sent, connection_aborted, cancel_error = await _cancel_and_abort_bistream(
            active_bistream.websocket
        )
        return TtsStreamCancelled(
            cancel_sent=cancel_sent,
            connection_aborted=connection_aborted,
            cancel_error=cancel_error,
        )

    def _bistream_url(self) -> str:
        base = str(self.http.base_url).rstrip("/")
        if base.startswith("https://"):
            return "wss://" + base[len("https://") :] + "/v1/tts/bistream"
        if base.startswith("http://"):
            return "ws://" + base[len("http://") :] + "/v1/tts/bistream"
        raise ValueError("TTS base URL must use http or https")

    @staticmethod
    async def _send_bistream_inputs(
        websocket: ClientConnection, inputs: AsyncIterator[TtsInput]
    ) -> None:
        sequence = 1
        async for item in inputs:
            if isinstance(item, TtsText):
                await websocket.send(
                    json.dumps({"type": "text", "seq": sequence, "delta": item.delta}, ensure_ascii=False)
                )
                sequence += 1
            elif isinstance(item, TtsEndTurn):
                await websocket.send(json.dumps({"type": "end_turn", "seq": sequence}))
                # end_turn 只关闭文本输入。发送协程继续存活，确保运行时随后取消
                # 该会话时仍持有可写 WebSocket；正常 end/error 会在外层取消本任务。
                await asyncio.Future()
            else:
                raise TypeError("unsupported TTS bistream input")


async def _cancel_and_abort_bistream(
    websocket: ClientConnection,
) -> tuple[bool, bool, str | None]:
    """尽力写入 cancel，随后不等待关闭握手直接终止传输。"""
    cancel_sent = False
    cancel_error: str | None = None
    try:
        await asyncio.wait_for(
            websocket.send(json.dumps({"type": "cancel"})),
            timeout=TTS_CANCEL_WRITE_TIMEOUT,
        )
        cancel_sent = True
    except (TimeoutError, OSError, WebSocketException) as error:
        cancel_error = type(error).__name__

    transport = getattr(websocket, "transport", None)
    if transport is None:
        return cancel_sent, False, cancel_error
    try:
        transport.abort()
    except Exception as error:  # noqa: BLE001 - 取消路径必须尽力收尾
        return cancel_sent, False, cancel_error or type(error).__name__
    return cancel_sent, True, cancel_error


@asynccontextmanager
async def _open_cancelable_bistream(
    url: str,
    sender_holder: list[asyncio.Task[None] | None],
) -> AsyncIterator[ClientConnection]:
    """在 websockets 自动关闭之前完成 cancel + abort，避免关闭时序反转。"""
    async with connect(url, max_size=None, close_timeout=0) as websocket:
        try:
            yield websocket
        except asyncio.CancelledError:
            sender = sender_holder[0]
            if sender is not None and not sender.done():
                sender.cancel()
                await asyncio.gather(sender, return_exceptions=True)
            cancel_sent, connection_aborted, cancel_error = await asyncio.shield(
                _cancel_and_abort_bistream(websocket)
            )
            raise TtsStreamCancelled(
                cancel_sent=cancel_sent,
                connection_aborted=connection_aborted,
                cancel_error=cancel_error,
            ) from None


async def _receive_bistream_event(
    websocket: ClientConnection, sender: asyncio.Task[None], timeout: float
) -> dict[str, object]:
    """等待事件，并在发送协程异常时立即失败。"""
    if timeout <= 0:
        raise TimeoutError
    receiver = asyncio.create_task(websocket.recv(), name="tts-bistream-receiver")
    try:
        done, _ = await asyncio.wait(
            {receiver, sender}, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
        )
        if not done:
            raise TimeoutError
        if sender in done:
            error = sender.exception()
            if error is not None:
                raise error
            if receiver not in done:
                # 输入已正常结束；继续等待音频或 end，不再把已完成 sender 放入 wait 集。
                raw = await asyncio.wait_for(receiver, timeout=timeout)
            else:
                raw = receiver.result()
        else:
            raw = receiver.result()
        event = json.loads(raw)
        if not isinstance(event, dict):
            raise TypeError("bistream event must be an object")
        return event
    finally:
        if not receiver.done():
            receiver.cancel()
            await asyncio.gather(receiver, return_exceptions=True)


def _remaining(deadline: float) -> float:
    return deadline - asyncio.get_running_loop().time()


def _decode_bistream_event(event: dict[str, object]) -> tuple[TtsChunk | None, bool]:
    event_type = event.get("type")
    if event_type == "error":
        code = event.get("code")
        message = event.get("message")
        raise TtsStreamError(
            message if isinstance(message, str) and message else str(code or "TTS_STREAM_FAILED"),
            code=str(code or "TTS_STREAM_FAILED"),
        )
    if event_type in {"queued", "ready"}:
        return None, False
    if event_type == "end":
        return None, True
    if event_type != "audio":
        raise ValueError("unknown bistream event type")
    return _decode_audio_event(event), False


async def _next_http_audio(
    events: AsyncIterator[dict[str, object]], timeout: float | None = None
) -> TtsChunk | None:
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
    """校验 HTTP NDJSON 或 bistream 的统一音频载荷。"""
    if "error" in event:
        error_code = str(event["error"])
        message = event.get("message")
        raise TtsStreamError(
            message if isinstance(message, str) and message else error_code, code=error_code
        )
    if event.get("event") == "prompt":
        return None

    sample_rate = event["sample_rate"]
    if type(sample_rate) is not int:
        raise TypeError("TTS sample_rate must be an integer")
    if sample_rate != TTS_SAMPLE_RATE:
        raise TtsStreamError("TTS_SAMPLE_RATE", code="TTS_SAMPLE_RATE") from ValueError(
            "TTS_SAMPLE_RATE"
        )
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
        raise TtsStreamError("TTS_PCM_EMPTY", code="TTS_PCM_EMPTY") from ValueError(
            "TTS_PCM_EMPTY"
        )
    if len(audio) % 2:
        raise TtsStreamError("TTS_PCM_ALIGNMENT", code="TTS_PCM_ALIGNMENT") from ValueError(
            "TTS_PCM_ALIGNMENT"
        )
    return TtsChunk(chunk_index, audio, finalize)
