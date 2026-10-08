import asyncio
import base64
import json
from collections.abc import AsyncIterator

import httpx
import pytest
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

from realtime_voice.clients.limits import AdmissionOverloaded, BoundedAdmission
from realtime_voice.clients.tts import (
    TtsChunk,
    TtsClient,
    TtsEndTurn,
    TtsRequest,
    TtsStreamCancelled,
    TtsStreamError,
    TtsText,
    _cancel_and_abort_bistream,
)
from tests.helpers import stream_transport


def _audio_event(**changes: object) -> dict[str, object]:
    event: dict[str, object] = {
        "chunk_index": 0,
        "sample_rate": 24000,
        "finalize": True,
        "audio_i16le_b64": base64.b64encode(b"\x00\x00\x01\x00").decode("ascii"),
    }
    event.update(changes)
    return event


class BlockingStream(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.waiting = asyncio.Event()
        self.release = asyncio.Event()
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield json.dumps(_audio_event()).encode() + b"\n"
        self.waiting.set()
        await self.release.wait()

    async def aclose(self) -> None:
        self.closed = True


async def test_tts_maps_dialogue_and_decodes_audio() -> None:
    """A wrong route, payload, or audio decode must be observable at the client boundary."""
    transport, captured = stream_transport([json.dumps(_audio_event()).encode() + b"\n"])
    admission = BoundedAdmission("tts", concurrency=8, max_waiters=64)
    request = TtsRequest(
        model_reply="你好，我在。", prompt="温柔自然地回应。", trace_id="device/session/turn-1"
    )

    async with httpx.AsyncClient(transport=transport, base_url="http://tts") as http:
        chunks = [chunk async for chunk in TtsClient(http, admission).stream(request)]

    assert chunks == [TtsChunk(0, b"\x00\x00\x01\x00", True)]
    sent = captured["request"]
    assert sent.extensions["timeout"]["read"] is None
    assert sent.method == "POST"
    assert sent.url.path == "/v1/dialogue-tts/stream"
    assert json.loads(sent.content) == {
        "model_reply": "你好，我在。",
        "prompt": "温柔自然地回应。",
        "include_prompt_event": False,
        "trace_id": "device/session/turn-1",
    }


async def test_tts_bistream_sends_start_and_raw_deltas_then_decodes_audio() -> None:
    received: list[dict[str, object]] = []
    audio = base64.b64encode(b"\x00\x00\x01\x00").decode("ascii")

    async def handler(websocket) -> None:
        received.append(json.loads(await websocket.recv()))
        received.append(json.loads(await websocket.recv()))
        received.append(json.loads(await websocket.recv()))
        received.append(json.loads(await websocket.recv()))
        await websocket.send(json.dumps({"type": "queued"}))
        await websocket.send(json.dumps({"type": "ready", "sample_rate": 24000}))
        await websocket.send(json.dumps({
            "type": "audio", "chunk_index": 0, "sample_rate": 24000,
            "finalize": True, "audio_i16le_b64": audio,
        }))
        await websocket.send(json.dumps({"type": "end", "reason": "completed"}))

    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]

        async def inputs():
            yield TtsText("你")
            yield TtsText("好")
            yield TtsEndTurn()

        admission = BoundedAdmission("tts", concurrency=1, max_waiters=0)
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}", trust_env=False) as http:
            client = TtsClient(
                http, admission, mode="bistream", ref_audio="ref.wav", ref_text="参考文本"
            )
            chunks = [
                chunk async for chunk in client.stream_bistream(TtsRequest("", "平和", "trace-1"), inputs())
            ]

    assert chunks == [TtsChunk(0, b"\x00\x00\x01\x00", True)]
    assert received == [
        {
            "type": "start", "request_id": "trace-1", "ref_audio": "ref.wav",
            "ref_text": "参考文本", "speaker": 0, "prompt": "平和", "temperature": 0.6,
            "top_k": 100, "top_p": 0.9, "repetition_penalty": 1.25, "max_tokens": 3000,
        },
        {"type": "text", "seq": 1, "delta": "你"},
        {"type": "text", "seq": 2, "delta": "好"},
        {"type": "end_turn", "seq": 3},
    ]


async def test_tts_bistream_disarms_first_audio_deadline_after_first_chunk() -> None:
    """首块已到达后，后续音频只能受空闲超时约束，不能再受首音总预算影响。"""
    audio = base64.b64encode(b"\x00\x00\x01\x00").decode("ascii")

    async def handler(websocket) -> None:
        for _ in range(3):
            await websocket.recv()
        await websocket.send(json.dumps({
            "type": "audio", "chunk_index": 0, "sample_rate": 24000,
            "finalize": False, "audio_i16le_b64": audio,
        }))
        # 该间隔超过首音预算、但没有超过音频空闲预算。
        await asyncio.sleep(0.06)
        await websocket.send(json.dumps({
            "type": "audio", "chunk_index": 1, "sample_rate": 24000,
            "finalize": True, "audio_i16le_b64": audio,
        }))
        await websocket.send(json.dumps({"type": "end", "reason": "completed"}))

    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]

        async def inputs():
            yield TtsText("测试")
            yield TtsEndTurn()

        admission = BoundedAdmission("tts", concurrency=1, max_waiters=0)
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}", trust_env=False) as http:
            client = TtsClient(
                http, admission, mode="bistream", first_audio_timeout=0.03, idle_timeout=0.2
            )
            chunks = [
                chunk async for chunk in client.stream_bistream(TtsRequest("", "平和", "trace-1"), inputs())
            ]

    assert [chunk.chunk_index for chunk in chunks] == [0, 1]


async def test_tts_bistream_can_cancel_after_end_turn_and_aborts_connection() -> None:
    received: list[dict[str, object]] = []
    end_turn_received = asyncio.Event()
    disconnected = asyncio.Event()

    async def handler(websocket) -> None:
        try:
            while True:
                message = json.loads(await websocket.recv())
                received.append(message)
                if message["type"] == "end_turn":
                    end_turn_received.set()
        except ConnectionClosed:
            disconnected.set()

    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]

        async def inputs():
            yield TtsText("测试")
            yield TtsEndTurn()

        admission = BoundedAdmission("tts", concurrency=1, max_waiters=0)
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{port}", trust_env=False
        ) as http:
            client = TtsClient(http, admission, mode="bistream")

            async def consume() -> None:
                async for _ in client.stream_bistream(
                    TtsRequest("", "平和", "trace-cancel-after-end"), inputs()
                ):
                    pass

            task = asyncio.create_task(consume())
            await asyncio.wait_for(end_turn_received.wait(), timeout=1)
            task.cancel()
            with pytest.raises(TtsStreamCancelled) as cancelled:
                await task
            await asyncio.wait_for(disconnected.wait(), timeout=1)

    assert cancelled.value.cancel_sent is True, cancelled.value.cancel_error
    assert cancelled.value.connection_aborted is True
    assert [message["type"] for message in received] == [
        "start",
        "text",
        "end_turn",
        "cancel",
    ]


async def test_tts_bistream_cancel_preempts_pending_input_before_end_turn() -> None:
    received: list[dict[str, object]] = []
    text_received = asyncio.Event()
    disconnected = asyncio.Event()

    async def handler(websocket) -> None:
        try:
            while True:
                message = json.loads(await websocket.recv())
                received.append(message)
                if message["type"] == "text":
                    text_received.set()
        except ConnectionClosed:
            disconnected.set()

    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        release_input = asyncio.Event()

        async def inputs():
            yield TtsText("已发送")
            await release_input.wait()
            yield TtsText("不应发送")
            yield TtsEndTurn()

        admission = BoundedAdmission("tts", concurrency=1, max_waiters=0)
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{port}", trust_env=False
        ) as http:
            client = TtsClient(http, admission, mode="bistream")

            async def consume() -> None:
                async for _ in client.stream_bistream(
                    TtsRequest("", "平和", "trace-cancel-before-end"), inputs()
                ):
                    pass

            task = asyncio.create_task(consume())
            await asyncio.wait_for(text_received.wait(), timeout=1)
            task.cancel()
            with pytest.raises(TtsStreamCancelled):
                await task
            await asyncio.wait_for(disconnected.wait(), timeout=1)

    assert [message["type"] for message in received] == ["start", "text", "cancel"]


async def test_cancel_bistream_stops_active_sender_before_writing_cancel() -> None:
    received: list[dict[str, object]] = []
    input_cancelled = asyncio.Event()
    audio_sent = asyncio.Event()

    async def handler(websocket) -> None:
        received.append(json.loads(await websocket.recv()))
        received.append(json.loads(await websocket.recv()))
        await websocket.send(json.dumps({"type": "audio", **_audio_event()}))
        audio_sent.set()
        try:
            while True:
                received.append(json.loads(await websocket.recv()))
        except ConnectionClosed:
            return

    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        release_input = asyncio.Event()

        async def inputs():
            yield TtsText("已发送")
            try:
                await release_input.wait()
                yield TtsText("不应发送")
                yield TtsEndTurn()
            finally:
                input_cancelled.set()

        admission = BoundedAdmission("tts", concurrency=1, max_waiters=0)
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{port}", trust_env=False
        ) as http:
            client = TtsClient(http, admission, mode="bistream")
            stream = client.stream_bistream(
                TtsRequest("", "平和", "trace-explicit-cancel"), inputs()
            )
            await asyncio.wait_for(anext(stream), timeout=1)
            await asyncio.wait_for(audio_sent.wait(), timeout=1)

            cancelled = await client.cancel_bistream("trace-explicit-cancel")
            await stream.aclose()

    assert input_cancelled.is_set()
    assert cancelled.cancel_sent is True
    assert cancelled.connection_aborted is True
    assert [message["type"] for message in received] == ["start", "text", "cancel"]


async def test_tts_bistream_blocked_cancel_write_aborts_within_fixed_budget() -> None:
    class Transport:
        def __init__(self) -> None:
            self.aborted = False

        def abort(self) -> None:
            self.aborted = True

    class BlockedWebSocket:
        def __init__(self) -> None:
            self.transport = Transport()

        async def send(self, payload: str) -> None:
            await asyncio.Event().wait()

    websocket = BlockedWebSocket()
    started = asyncio.get_running_loop().time()

    sent, aborted, error = await _cancel_and_abort_bistream(websocket)  # type: ignore[arg-type]

    assert asyncio.get_running_loop().time() - started < 0.2
    assert (sent, aborted, error) == (False, True, "TimeoutError")
    assert websocket.transport.aborted is True


async def test_tts_ignores_prompt_events() -> None:
    """Prompt metadata must never be exposed as synthesized audio."""
    transport, _ = stream_transport(
        [
            b'{"event":"prompt","text":"ignored"}\n',
            json.dumps(_audio_event()).encode() + b"\n",
        ]
    )
    admission = BoundedAdmission("tts", concurrency=1, max_waiters=0)

    async with httpx.AsyncClient(transport=transport, base_url="http://tts") as http:
        chunks = [
            chunk
            async for chunk in TtsClient(http, admission).stream(TtsRequest("u", "m", "t"))
        ]

    assert chunks == [TtsChunk(0, b"\x00\x00\x01\x00", True)]


@pytest.mark.parametrize("status_code", [422, 500])
async def test_tts_wraps_non_success_statuses(status_code: int) -> None:
    """Changing status handling would leak HTTPX errors beyond the TTS boundary."""
    transport, _ = stream_transport([b"failure"], status_code=status_code)
    admission = BoundedAdmission("tts", concurrency=1, max_waiters=0)

    async with httpx.AsyncClient(transport=transport, base_url="http://tts") as http:
        with pytest.raises(TtsStreamError, match="TTS stream failed") as error:
            _ = [chunk async for chunk in TtsClient(http, admission).stream(TtsRequest("u", "m", "t"))]

    assert isinstance(error.value.__cause__, httpx.HTTPStatusError)
    assert error.value.code == "TTS_STREAM_FAILED"


async def test_tts_wraps_inline_error_events() -> None:
    """An HTTP-200 service error event must terminate the stream as a TTS failure."""
    transport, _ = stream_transport([b'{"error":"internal_error","message":"engine failed"}\n'])
    admission = BoundedAdmission("tts", concurrency=1, max_waiters=0)

    async with httpx.AsyncClient(transport=transport, base_url="http://tts") as http:
        with pytest.raises(TtsStreamError, match="engine failed") as error:
            _ = [chunk async for chunk in TtsClient(http, admission).stream(TtsRequest("u", "m", "t"))]

    assert error.value.code == "internal_error"


@pytest.mark.parametrize(
    ("record", "cause_type"),
    [
        (b"not-json\n", json.JSONDecodeError),
        (b"[]\n", TypeError),
        (json.dumps(_audio_event(audio_i16le_b64="not base64!")).encode() + b"\n", ValueError),
        (json.dumps(_audio_event(sample_rate="24000")).encode() + b"\n", TypeError),
        (json.dumps(_audio_event(chunk_index=True)).encode() + b"\n", TypeError),
        (json.dumps(_audio_event(finalize="true")).encode() + b"\n", TypeError),
    ],
)
async def test_tts_wraps_malformed_events_with_causes(
    record: bytes, cause_type: type[BaseException]
) -> None:
    """Bad protocol records must remain stable errors while retaining their diagnostic cause."""
    transport, _ = stream_transport([record])
    admission = BoundedAdmission("tts", concurrency=1, max_waiters=0)

    async with httpx.AsyncClient(transport=transport, base_url="http://tts") as http:
        with pytest.raises(TtsStreamError, match="TTS stream failed") as error:
            _ = [chunk async for chunk in TtsClient(http, admission).stream(TtsRequest("u", "m", "t"))]

    assert isinstance(error.value.__cause__, cause_type)
    assert error.value.code == "TTS_STREAM_FAILED"


@pytest.mark.parametrize(
    ("event", "message", "cause_type"),
    [
        (_audio_event(sample_rate=16000), "TTS_SAMPLE_RATE", ValueError),
        (
            _audio_event(audio_i16le_b64=base64.b64encode(b"\x00").decode()),
            "TTS_PCM_ALIGNMENT",
            ValueError,
        ),
        (_audio_event(audio_i16le_b64=""), "TTS_PCM_EMPTY", ValueError),
    ],
)
async def test_tts_rejects_invalid_audio_properties(
    event: dict[str, object], message: str, cause_type: type[BaseException]
) -> None:
    """Wrong rate or unusable PCM must be rejected before it reaches audio consumers."""
    transport, _ = stream_transport([json.dumps(event).encode() + b"\n"])
    admission = BoundedAdmission("tts", concurrency=1, max_waiters=0)

    async with httpx.AsyncClient(transport=transport, base_url="http://tts") as http:
        with pytest.raises(TtsStreamError, match=message) as error:
            _ = [chunk async for chunk in TtsClient(http, admission).stream(TtsRequest("u", "m", "t"))]
    assert isinstance(error.value.__cause__, cause_type)
    assert error.value.code == message


async def test_tts_wraps_request_timeouts() -> None:
    """Transport timeouts must have a stable TTS error while preserving the HTTP cause."""
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("read timed out", request=request)

    admission = BoundedAdmission("tts", concurrency=1, max_waiters=0)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://tts"
    ) as http:
        with pytest.raises(TtsStreamError, match="TTS stream failed") as error:
            _ = [chunk async for chunk in TtsClient(http, admission).stream(TtsRequest("u", "m", "t"))]

    assert isinstance(error.value.__cause__, httpx.ReadTimeout)
    assert error.value.code == "TTS_STREAM_FAILED"


async def test_tts_aclose_releases_response_and_admission_slot() -> None:
    """Stopping an audio consumer early must close its response and relinquish capacity."""
    stream = BlockingStream()
    admission = BoundedAdmission("tts", concurrency=1, max_waiters=0)

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://tts"
    ) as http:
        iterator = TtsClient(http, admission).stream(TtsRequest("u", "m", "t"))
        assert await anext(iterator) == TtsChunk(0, b"\x00\x00\x01\x00", True)
        assert (await admission.snapshot()).active == 1
        await iterator.aclose()

    assert stream.closed is True
    assert (await admission.snapshot()).active == 0


async def test_tts_cancellation_releases_response_and_admission_slot() -> None:
    """Cancelling a stream consumer must not strand an HTTP response or limiter slot."""
    stream = BlockingStream()
    admission = BoundedAdmission("tts", concurrency=1, max_waiters=0)

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream)

    async def consume(client: TtsClient) -> list[TtsChunk]:
        return [chunk async for chunk in client.stream(TtsRequest("u", "m", "t"))]

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://tts"
    ) as http:
        task = asyncio.create_task(consume(TtsClient(http, admission)))
        await stream.waiting.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert stream.closed is True
    assert (await admission.snapshot()).active == 0


async def test_tts_propagates_admission_overload() -> None:
    """A saturated TTS limiter must retain its overload signal for session backpressure."""
    stream = BlockingStream()
    admission = BoundedAdmission("tts", concurrency=1, max_waiters=0)

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://tts"
    ) as http:
        client = TtsClient(http, admission)
        iterator = client.stream(TtsRequest("u", "m", "t"))
        assert await anext(iterator) == TtsChunk(0, b"\x00\x00\x01\x00", True)
        with pytest.raises(AdmissionOverloaded):
            await anext(client.stream(TtsRequest("u", "m", "t")))
        await iterator.aclose()


class DelayedStream(httpx.AsyncByteStream):
    def __init__(self, parts: list[tuple[float, bytes]]) -> None:
        self.parts = parts
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for delay, chunk in self.parts:
            if delay:
                await asyncio.sleep(delay)
            yield chunk

    async def aclose(self) -> None:
        self.closed = True


class SlowHeaderTransport(httpx.AsyncBaseTransport):
    def __init__(self, delay: float, stream: DelayedStream) -> None:
        self.delay = delay
        self.stream = stream
        self.cancelled = False
        self.closed = False

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        try:
            await asyncio.sleep(self.delay)
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        return httpx.Response(200, request=request, stream=self.stream)

    async def aclose(self) -> None:
        self.closed = True


async def test_tts_first_audio_deadline_covers_response_headers_and_releases_slot() -> None:
    stream = DelayedStream([(0.0, json.dumps(_audio_event()).encode() + b"\n")])
    transport = SlowHeaderTransport(0.03, stream)
    admission = BoundedAdmission("tts", concurrency=1, max_waiters=0)

    async with httpx.AsyncClient(transport=transport, base_url="http://tts") as http:
        client = TtsClient(http, admission, first_audio_timeout=0.01, idle_timeout=0.1)
        with pytest.raises(TtsStreamError, match="TTS_FIRST_AUDIO_TIMEOUT") as error:
            _ = [chunk async for chunk in client.stream(TtsRequest("u", "m", "t"))]

    snapshot = await admission.snapshot()
    assert isinstance(error.value.__cause__, TimeoutError)
    assert error.value.code == "TTS_FIRST_AUDIO_TIMEOUT"
    assert transport.cancelled is True
    assert transport.closed is True
    assert snapshot.active == 0
    assert snapshot.waiting == 0


async def test_tts_prompt_does_not_reset_absolute_first_audio_deadline() -> None:
    stream = DelayedStream(
        [(0.0, b'{"event":"prompt"}\n'), (0.03, json.dumps(_audio_event()).encode() + b"\n")]
    )
    admission = BoundedAdmission("tts", concurrency=1, max_waiters=0)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, stream=stream)),
        base_url="http://tts",
    ) as http:
        with pytest.raises(TtsStreamError, match="TTS_FIRST_AUDIO_TIMEOUT") as error:
            _ = [
                chunk
                async for chunk in TtsClient(http, admission, first_audio_timeout=0.01).stream(
                    TtsRequest("u", "m", "t")
                )
            ]

    snapshot = await admission.snapshot()
    assert isinstance(error.value.__cause__, TimeoutError)
    assert error.value.code == "TTS_FIRST_AUDIO_TIMEOUT"
    assert stream.closed is True
    assert snapshot.active == 0
    assert snapshot.waiting == 0


async def test_tts_idle_timeout_closes_response_and_releases_slot() -> None:
    """A post-audio idle period must use the idle deadline and close the stream."""
    stream = DelayedStream(
        [(0.0, json.dumps(_audio_event()).encode() + b"\n"), (0.03, b"")]
    )
    admission = BoundedAdmission("tts", concurrency=1, max_waiters=0)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, stream=stream)),
        base_url="http://tts",
    ) as http:
        with pytest.raises(TtsStreamError, match="TTS_STREAM_IDLE_TIMEOUT") as error:
            _ = [
                chunk
                async for chunk in TtsClient(http, admission, idle_timeout=0.01).stream(
                    TtsRequest("u", "m", "t")
                )
            ]

    snapshot = await admission.snapshot()
    assert isinstance(error.value.__cause__, TimeoutError)
    assert error.value.code == "TTS_STREAM_IDLE_TIMEOUT"
    assert stream.closed is True
    assert snapshot.active == 0
    assert snapshot.waiting == 0
