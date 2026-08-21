import base64
import io
import json
import wave

import httpx
import pytest

from realtime_voice.clients.asr import AsrClient, AsrError
from realtime_voice.clients.limits import BoundedAdmission
from tests.helpers import stream_transport


async def test_asr_sends_wav_and_cleans_tagged_text() -> None:
    """ASR requests must carry 16 kHz WAV audio and return the tagged transcript."""
    response = {
        "choices": [{"message": {"content": "language: Chinese<asr_text> 你好 "}}]
    }
    transport, captured = stream_transport([json.dumps(response).encode("utf-8")])
    admission = BoundedAdmission("asr", concurrency=8, max_waiters=64)

    async with httpx.AsyncClient(transport=transport, base_url="http://asr") as http:
        client = AsrClient(http, admission)
        text = await client.transcribe(b"\x00\x00" * 1600)

    assert text == "你好"
    payload = json.loads(captured["request"].content)
    input_audio = payload["messages"][0]["content"][0]["input_audio"]
    assert input_audio["format"] == "wav"
    with wave.open(io.BytesIO(base64.b64decode(input_audio["data"]))) as audio:
        assert audio.getframerate() == 16000


@pytest.mark.parametrize("content", ["", "none", "NULL", " undefined "])
async def test_asr_returns_empty_for_empty_sentinel_content(content: str) -> None:
    """ASR empty sentinels must not be exposed to sessions as transcripts."""
    response = {"choices": [{"message": {"content": content}}]}
    transport, _ = stream_transport([json.dumps(response).encode("utf-8")])
    admission = BoundedAdmission("asr", concurrency=1, max_waiters=0)

    async with httpx.AsyncClient(transport=transport, base_url="http://asr") as http:
        text = await AsrClient(http, admission).transcribe(b"\x00\x00")

    assert text == ""


async def test_asr_trims_untagged_content() -> None:
    """Responses without Qwen's marker must retain their trimmed transcript."""
    response = {"choices": [{"message": {"content": "  hello  "}}]}
    transport, _ = stream_transport([json.dumps(response).encode("utf-8")])
    admission = BoundedAdmission("asr", concurrency=1, max_waiters=0)

    async with httpx.AsyncClient(transport=transport, base_url="http://asr") as http:
        text = await AsrClient(http, admission).transcribe(b"\x00\x00")

    assert text == "hello"


async def test_asr_wraps_missing_choices_as_stable_error() -> None:
    """A malformed completion body must not leak its parsing implementation error."""
    transport, _ = stream_transport([b"{}"])
    admission = BoundedAdmission("asr", concurrency=1, max_waiters=0)

    async with httpx.AsyncClient(transport=transport, base_url="http://asr") as http:
        with pytest.raises(AsrError, match="ASR transcription failed") as error:
            await AsrClient(http, admission).transcribe(b"\x00\x00")

    assert isinstance(error.value.__cause__, ValueError)


async def test_asr_wraps_invalid_json_as_stable_error() -> None:
    """A non-JSON completion body must surface only the ASR boundary error."""
    transport, _ = stream_transport([b"not-json"])
    admission = BoundedAdmission("asr", concurrency=1, max_waiters=0)

    async with httpx.AsyncClient(transport=transport, base_url="http://asr") as http:
        with pytest.raises(AsrError, match="ASR transcription failed") as error:
            await AsrClient(http, admission).transcribe(b"\x00\x00")

    assert isinstance(error.value.__cause__, json.JSONDecodeError)


async def test_asr_wraps_http_error_as_stable_error() -> None:
    """A non-success HTTP response must not leak an HTTPX exception."""
    transport, _ = stream_transport([b"failure"], status_code=500)
    admission = BoundedAdmission("asr", concurrency=1, max_waiters=0)

    async with httpx.AsyncClient(transport=transport, base_url="http://asr") as http:
        with pytest.raises(AsrError, match="ASR transcription failed") as error:
            await AsrClient(http, admission).transcribe(b"\x00\x00")

    assert isinstance(error.value.__cause__, httpx.HTTPStatusError)


async def test_asr_wraps_read_timeout_as_stable_error() -> None:
    """A downstream read timeout must keep its cause behind the ASR boundary error."""
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("read timed out", request=request)

    admission = BoundedAdmission("asr", concurrency=1, max_waiters=0)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://asr"
    ) as http:
        with pytest.raises(AsrError, match="ASR transcription failed") as error:
            await AsrClient(http, admission).transcribe(b"\x00\x00")

    assert isinstance(error.value.__cause__, httpx.ReadTimeout)


async def test_asr_wraps_non_string_content_as_stable_error() -> None:
    """A non-text completion content value must not leak a type error."""
    response = {"choices": [{"message": {"content": None}}]}
    transport, _ = stream_transport([json.dumps(response).encode("utf-8")])
    admission = BoundedAdmission("asr", concurrency=1, max_waiters=0)

    async with httpx.AsyncClient(transport=transport, base_url="http://asr") as http:
        with pytest.raises(AsrError, match="ASR transcription failed") as error:
            await AsrClient(http, admission).transcribe(b"\x00\x00")

    assert isinstance(error.value.__cause__, TypeError)
