"""Privacy-preserving structured application logs."""

import json
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

_CONTEXT: ContextVar[dict[str, str | int] | None] = ContextVar(
    "realtime_voice_log_context", default=None
)
_SAFE = frozenset(
    {
        "user_id",
        "session_id",
        "turn_id",
        "segment_id",
        "duration_ms",
        "queue_wait_ms",
        "interrupt",
        "stage",
        "error_code",
        "generation",
        "byte_count",
        "event_type",
        "reason",
        "error_type",
    }
)


@contextmanager
def bind_context(**context: str | int | None) -> Iterator[None]:
    token = _CONTEXT.set(
        {
            **(_CONTEXT.get() or {}),
            **{key: value for key, value in context.items() if key in _SAFE and value is not None},
        }
    )
    try:
        yield
    finally:
        _CONTEXT.reset(token)


def log_event(event: str, *, logger: logging.Logger | None = None, **fields: Any) -> None:
    payload: dict[str, str | int | float | bool] = {"event": event}
    payload.update(_CONTEXT.get() or {})
    for key, value in fields.items():
        if key in _SAFE and isinstance(value, (str, int, float, bool)):
            payload[key] = value
    (logger or logging.getLogger("realtime_voice")).info(
        json.dumps(payload, sort_keys=True, ensure_ascii=False)
    )
