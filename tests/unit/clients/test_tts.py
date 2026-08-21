import asyncio
import base64
import json
from collections.abc import AsyncIterator

import httpx
import pytest

from realtime_voice.clients.limits import AdmissionOverloaded, BoundedAdmission
from realtime_voice.clients.tts import TtsChunk, TtsClient, TtsRequest, TtsStreamError
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
        user_input="你好", model_reply="你好，我在。", trace_id="device/session/turn-1"
    )

    async with httpx.AsyncClient(transport=transport, base_url="http://tts") as http:
        chunks = [chunk async for chunk in TtsClient(http, admission).stream(request)]

    assert chunks == [TtsChunk(0, b"\x00\x00\x01\x00", True)]
    sent = captured["request"]
    assert sent.extensions["timeout"]["read"] is None
    assert sent.method == "POST"
    assert sent.url.path == "/v1/dialogue-tts/stream"
    assert json.loads(sent.content) == {
        "user_input": "你好",
        "model_reply": "你好，我在。",
        "include_prompt_event": False,
        "trace_id": "device/session/turn-1",
    }


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


async def test_tts_wraps_inline_error_events() -> None:
    """An HTTP-200 service error event must terminate the stream as a TTS failure."""
    transport, _ = stream_transport([b'{"error":"internal_error","message":"engine failed"}\n'])
    admission = BoundedAdmission("tts", concurrency=1, max_waiters=0)

    async with httpx.AsyncClient(transport=transport, base_url="http://tts") as http:
        with pytest.raises(TtsStreamError, match="engine failed"):
            _ = [chunk async for chunk in TtsClient(http, admission).stream(TtsRequest("u", "m", "t"))]


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
    assert stream.closed is True
    assert snapshot.active == 0
    assert snapshot.waiting == 0
