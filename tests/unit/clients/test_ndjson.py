import json
from collections.abc import AsyncIterator

import pytest

from realtime_voice.clients.ndjson import iter_ndjson


async def async_chunks(parts: list[bytes]) -> AsyncIterator[bytes]:
    for part in parts:
        yield part


async def test_ndjson_parser_handles_split_and_joined_lines() -> None:
    chunks = async_chunks(
        [
            b'{"type":"text_',
            b'delta","delta":"\xe4\xbd\xa0"}\n{"type":"done"',
            b',"output":{"reply_text":"\xe4\xbd\xa0\xe5\xa5\xbd"}}\n',
        ]
    )

    events = [event async for event in iter_ndjson(chunks)]

    assert events == [
        {"type": "text_delta", "delta": "你"},
        {"type": "done", "output": {"reply_text": "你好"}},
    ]


async def test_ndjson_parser_accepts_complete_final_record_without_newline() -> None:
    events = [event async for event in iter_ndjson(async_chunks([b'{"type":"done"}']))]

    assert events == [{"type": "done"}]


async def test_ndjson_parser_rejects_incomplete_final_record() -> None:
    with pytest.raises(ValueError, match="incomplete JSON record"):
        _ = [event async for event in iter_ndjson(async_chunks([b'{"type":']))]


async def test_ndjson_parser_does_not_suppress_invalid_utf8_or_json() -> None:
    with pytest.raises(UnicodeDecodeError):
        _ = [event async for event in iter_ndjson(async_chunks([b'{"type":"\xff"}\n']))]

    with pytest.raises(json.JSONDecodeError):
        _ = [event async for event in iter_ndjson(async_chunks([b'{"type":}\n']))]
