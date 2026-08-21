"""Incremental newline-delimited JSON parsing."""

import json
from collections.abc import AsyncIterable, AsyncIterator
from typing import Any


def _decode_record(record: bytes) -> dict[str, Any]:
    value = json.loads(record.decode("utf-8"))
    if not isinstance(value, dict):
        raise TypeError("NDJSON records must be JSON objects")
    return value


async def iter_ndjson(chunks: AsyncIterable[bytes]) -> AsyncIterator[dict[str, Any]]:
    """Yield JSON-object records from arbitrarily split byte chunks."""
    buffer = bytearray()

    async for chunk in chunks:
        buffer.extend(chunk)
        while True:
            try:
                newline = buffer.index(b"\n")
            except ValueError:
                break

            record = bytes(buffer[:newline])
            del buffer[: newline + 1]
            yield _decode_record(record)

    if buffer:
        try:
            yield _decode_record(bytes(buffer))
        except json.JSONDecodeError as error:
            raise ValueError("NDJSON stream ended with an incomplete JSON record") from error
