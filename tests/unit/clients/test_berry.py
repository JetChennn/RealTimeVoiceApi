import asyncio
import json
from collections.abc import AsyncIterator

import httpx
import pytest

from realtime_voice.clients.berry import (
    BerryCleanupError,
    BerryClient,
    BerryDone,
    BerryInterruptError,
    BerryReplyRequest,
    BerryStreamError,
    BerryTextDelta,
    DeleteResult,
)
from realtime_voice.clients.limits import AdmissionOverloaded, BoundedAdmission
from tests.helpers import stream_transport, valid_wav


class BlockingStream(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.waiting = asyncio.Event()
        self.release = asyncio.Event()
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b'{"type":"text_delta","delta":"first"}\n'
        self.waiting.set()
        await self.release.wait()

    async def aclose(self) -> None:
        self.closed = True


async def test_berry_streams_existing_multimodal_contract_across_chunk_boundaries() -> None:
    transport, captured = stream_transport(
        [
            b'{"type":"text_del',
            b'ta","delta":"\\u4f60"}\n{"type":"done","output":{"reply_',
            b'text":"\\u4f60\\u597d\\uff0c\\u6211\\u5728\\u3002"}}\n',
        ]
    )
    admission = BoundedAdmission("berry", concurrency=8, max_waiters=64)
    request = BerryReplyRequest("device-01", "session-100", "你好", valid_wav())

    async with httpx.AsyncClient(transport=transport, base_url="http://berry") as http:
        events = [event async for event in BerryClient(http, admission).stream_reply(request)]

    assert events == [BerryTextDelta("你"), BerryDone("你好，我在。")]
    sent = captured["request"]
    assert sent.method == "POST"
    assert sent.url.path == "/api/v1/multimodal/reply"
    for field in (
        b'name="text"',
        b'name="audio"',
        b'name="user_id"',
        b'name="session_id"',
        b'name="stream"',
        b'name="reply_mode"',
        b'name="audio_is_vad_segment"',
        b'name="skip_internal_asr"',
        b'filename="segment.wav"',
        b'Content-Type: audio/wav',
    ):
        assert field in sent.content


@pytest.mark.parametrize(
    "chunks",
    [
        [b'{"type":"error","error_message":"model failed"}\n'],
        [b'not-json\n'],
        [b'{"type":"done","output":{}}\n'],
    ],
)
async def test_berry_stream_wraps_event_and_ndjson_failures(chunks: list[bytes]) -> None:
    transport, _ = stream_transport(chunks)
    admission = BoundedAdmission("berry", concurrency=1, max_waiters=0)
    request = BerryReplyRequest("u", "s", "text", valid_wav())

    async with httpx.AsyncClient(transport=transport, base_url="http://berry") as http:
        with pytest.raises(BerryStreamError):
            _ = [event async for event in BerryClient(http, admission).stream_reply(request)]


async def test_berry_stream_wraps_http_failure() -> None:
    transport, _ = stream_transport([b"failure"], status_code=500)
    admission = BoundedAdmission("berry", concurrency=1, max_waiters=0)
    request = BerryReplyRequest("u", "s", "text", valid_wav())

    async with httpx.AsyncClient(transport=transport, base_url="http://berry") as http:
        with pytest.raises(BerryStreamError) as error:
            _ = [event async for event in BerryClient(http, admission).stream_reply(request)]

    assert isinstance(error.value.__cause__, httpx.HTTPStatusError)


async def test_berry_stream_aclose_releases_response_and_admission_slot() -> None:
    stream = BlockingStream()
    admission = BoundedAdmission("berry", concurrency=1, max_waiters=0)
    request = BerryReplyRequest("u", "s", "text", valid_wav())

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://berry"
    ) as http:
        iterator = BerryClient(http, admission).stream_reply(request)
        assert await anext(iterator) == BerryTextDelta("first")
        assert (await admission.snapshot()).active == 1
        await iterator.aclose()

    assert stream.closed is True
    assert (await admission.snapshot()).active == 0


async def test_berry_stream_cancellation_releases_response_and_admission_slot() -> None:
    stream = BlockingStream()
    admission = BoundedAdmission("berry", concurrency=1, max_waiters=0)
    request = BerryReplyRequest("u", "s", "text", valid_wav())

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream)

    async def consume(client: BerryClient) -> list[object]:
        return [event async for event in client.stream_reply(request)]

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://berry"
    ) as http:
        task = asyncio.create_task(consume(BerryClient(http, admission)))
        await stream.waiting.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert stream.closed is True
    assert (await admission.snapshot()).active == 0
async def test_berry_interrupt_sends_only_user_and_session_ids() -> None:
    captured: dict[str, httpx.Request] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["request"] = request
        return httpx.Response(200)

    admission = BoundedAdmission("berry", concurrency=1, max_waiters=0)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://berry"
    ) as http:
        await BerryClient(http, admission).interrupt("device-01", "session-100")

    assert captured["request"].url.path == "/api/v1/interrupt"
    assert json.loads(captured["request"].content) == {
        "user_id": "device-01",
        "session_id": "session-100",
    }


@pytest.mark.parametrize(
    ("status_code", "expected"),
    [(200, DeleteResult.DELETED), (404, DeleteResult.NOT_FOUND)],
)
async def test_berry_delete_session_accepts_deleted_or_absent_session(
    status_code: int, expected: DeleteResult
) -> None:
    captured: dict[str, httpx.Request] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["request"] = request
        return httpx.Response(status_code)

    admission = BoundedAdmission("berry", concurrency=1, max_waiters=0)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://berry"
    ) as http:
        result = await BerryClient(http, admission).delete_session("user/one", "session/two")

    assert result is expected
    assert captured["request"].url.raw_path == b"/api/v1/sessions/user%2Fone/session%2Ftwo"


async def test_berry_control_errors_are_stable_and_interrupt_skips_reply_limiter() -> None:
    reply_stream = BlockingStream()
    requests: list[httpx.Request] = []
    admission = BoundedAdmission("berry", concurrency=1, max_waiters=0)
    request = BerryReplyRequest("u", "s", "text", valid_wav())

    def handler(incoming: httpx.Request) -> httpx.Response:
        requests.append(incoming)
        if incoming.url.path == "/api/v1/multimodal/reply":
            return httpx.Response(200, stream=reply_stream)
        if incoming.url.path == "/api/v1/interrupt":
            return httpx.Response(200)
        return httpx.Response(500)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://berry"
    ) as http:
        client = BerryClient(http, admission)
        iterator = client.stream_reply(request)
        assert await anext(iterator) == BerryTextDelta("first")
        await client.interrupt("u", "s")
        with pytest.raises(AdmissionOverloaded):
            await anext(client.stream_reply(request))
        await iterator.aclose()
        with pytest.raises(BerryCleanupError):
            await client.delete_session("u", "s")

    assert [item.url.path for item in requests[:2]] == [
        "/api/v1/multimodal/reply",
        "/api/v1/interrupt",
    ]


async def test_berry_interrupt_wraps_http_errors() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, request=request)

    admission = BoundedAdmission("berry", concurrency=1, max_waiters=0)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://berry"
    ) as http:
        with pytest.raises(BerryInterruptError):
            await BerryClient(http, admission).interrupt("u", "s")
