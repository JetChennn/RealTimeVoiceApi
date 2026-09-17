"""Privacy-preserving structured application logs."""

import json
import logging
import sys
import traceback
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, Any

APPLICATION_LOGGER_NAME = "realtime_voice"
_HANDLER_MARKER = "_realtime_voice_application_handler"

_CONTEXT: ContextVar[dict[str, str | int] | None] = ContextVar(
    "realtime_voice_log_context", default=None
)
_SAFE = frozenset(
    {
        "device_id",
        "user_id",
        "session_id",
        "turn_id",
        "segment_id",
        "trace_id",
        "duration_ms",
        "gap_ms",
        "queue_wait_ms",
        "interrupt",
        "stage",
        "error_code",
        "downstream_error_code",
        "generation",
        "sequence",
        "byte_count",
        "chunk_count",
        "event_type",
        "reason",
        "status",
        "previous_status",
        "service",
        "snippet_count",
        "error_type",
        "probability",
        "segment_count",
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


def log_event(
    event: str,
    *,
    logger: logging.Logger | None = None,
    level: int = logging.INFO,
    exc_info: BaseException | bool | None = None,
    **fields: Any,
) -> None:
    """Write one privacy-filtered JSON event at the requested severity."""
    resolved_logger = logger or logging.getLogger(APPLICATION_LOGGER_NAME)
    payload: dict[str, str | int | float | bool] = {
        "event": event,
        "level": logging.getLevelName(level),
        "logger": resolved_logger.name,
        "timestamp": datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
    }
    payload.update(_CONTEXT.get() or {})
    for key, value in fields.items():
        if key in _SAFE and isinstance(value, (str, int, float, bool)):
            payload[key] = value

    error = _resolve_exception(exc_info)
    if error is not None:
        stack_trace = _sanitized_stack_trace(error)
        if stack_trace:
            payload["stack_trace"] = stack_trace

    resolved_logger.log(
        level,
        json.dumps(payload, sort_keys=True, ensure_ascii=False),
    )


def configure_application_logging(
    *, level: int = logging.INFO, stream: IO[str] | None = None
) -> logging.Logger:
    """Install a fallback handler unless the host already configured logging."""
    logger = logging.getLogger(APPLICATION_LOGGER_NAME)
    handler = next(
        (item for item in logger.handlers if getattr(item, _HANDLER_MARKER, False)),
        None,
    )
    if handler is not None:
        logger.setLevel(level)
        handler.setLevel(level)
        return logger

    root_logger = logging.getLogger()
    if logger.handlers or (logger.propagate and root_logger.handlers):
        return logger

    logger.setLevel(level)
    logger.propagate = False
    handler = logging.StreamHandler(stream)
    setattr(handler, _HANDLER_MARKER, True)
    handler.setFormatter(logging.Formatter("%(message)s"))
    handler.setLevel(level)
    logger.addHandler(handler)
    return logger


def _resolve_exception(exc_info: BaseException | bool | None) -> BaseException | None:
    if isinstance(exc_info, BaseException):
        return exc_info
    if exc_info:
        return sys.exc_info()[1]
    return None


def _sanitized_stack_trace(error: BaseException) -> str | None:
    """Return code locations only, deliberately excluding exception messages."""
    if error.__traceback__ is None:
        return None
    frames = traceback.extract_tb(error.__traceback__)[-12:]
    return " <- ".join(
        f"{Path(frame.filename).name}:{frame.lineno}:{frame.name}" for frame in frames
    )
