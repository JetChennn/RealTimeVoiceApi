import json
import logging
from datetime import datetime
from io import StringIO

from realtime_voice.observability.logging import (
    bind_context,
    configure_application_logging,
    log_event,
)


def test_structured_log_omits_audio_and_full_text(caplog):
    logger = logging.getLogger("test.structured.privacy")
    with caplog.at_level(logging.INFO), bind_context(user_id="u", session_id="s", turn_id=1):
        log_event(
            "asr_completed",
            logger=logger,
            duration_ms=20.0,
            audio_bytes=b"secret",
            text="complete conversation",
        )

    payload = json.loads(caplog.records[-1].message)
    timestamp = payload.pop("timestamp")
    assert datetime.fromisoformat(timestamp).tzinfo is not None
    assert payload == {
        "duration_ms": 20.0,
        "event": "asr_completed",
        "level": "INFO",
        "logger": "test.structured.privacy",
        "session_id": "s",
        "turn_id": 1,
        "user_id": "u",
    }


def test_bound_context_is_restored_after_logging_scope(caplog):
    logger = logging.getLogger("test.structured.context")
    with caplog.at_level(logging.INFO):
        with bind_context(user_id="u", session_id="s"):
            log_event("session_started", logger=logger)
        log_event("process_ready", logger=logger)

    bound, _unbound = (json.loads(record.message) for record in caplog.records[-2:])
    assert bound["session_id"] == "s"


def test_structured_log_supports_severity_and_correlation_fields(caplog):
    logger = logging.getLogger("test.structured.severity")
    with caplog.at_level(logging.WARNING, logger=logger.name):
        log_event(
            "tts_failed",
            logger=logger,
            level=logging.WARNING,
            device_id="device-1",
            user_id="device-1",
            session_id="session-1",
            turn_id=2,
            trace_id="device-1/session-1/turn-2",
            stage="TTS",
            error_code="TTS_STREAM_FAILED",
            reason="idle_timeout",
        )

    record = caplog.records[-1]
    payload = json.loads(record.message)
    assert record.levelno == logging.WARNING
    assert payload["device_id"] == "device-1"
    assert payload["session_id"] == "session-1"
    assert payload["turn_id"] == 2
    assert payload["reason"] == "idle_timeout"


def test_application_logger_emits_info_without_root_configuration():
    logger = logging.getLogger("realtime_voice")
    root_logger = logging.getLogger()
    original_handlers = list(logger.handlers)
    original_root_handlers = list(root_logger.handlers)
    original_level = logger.level
    original_propagate = logger.propagate
    stream = StringIO()
    try:
        logger.handlers.clear()
        root_logger.handlers.clear()
        logger.propagate = True
        configure_application_logging(stream=stream)
        log_event("production_info_probe")
        output = stream.getvalue()
    finally:
        for handler in logger.handlers:
            if handler not in original_handlers:
                handler.close()
        logger.handlers[:] = original_handlers
        root_logger.handlers[:] = original_root_handlers
        logger.setLevel(original_level)
        logger.propagate = original_propagate

    lines = output.splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["event"] == "production_info_probe"
    assert payload["level"] == "INFO"
    assert payload["logger"] == "realtime_voice"
    assert "timestamp" in payload


def test_application_logger_respects_existing_handler_configuration():
    logger = logging.getLogger("realtime_voice")
    original_handlers = list(logger.handlers)
    original_level = logger.level
    original_propagate = logger.propagate
    stream = StringIO()
    external_handler = logging.StreamHandler(stream)
    external_handler.setFormatter(logging.Formatter("external:%(message)s"))
    try:
        logger.handlers[:] = [external_handler]
        logger.setLevel(logging.WARNING)
        logger.propagate = False
        configure_application_logging(level=logging.INFO)
        log_event("external_probe", logger=logger, level=logging.WARNING)
    finally:
        logger.handlers[:] = original_handlers
        logger.setLevel(original_level)
        logger.propagate = original_propagate
        external_handler.close()

    assert len(stream.getvalue().splitlines()) == 1
    assert stream.getvalue().startswith("external:")


def test_exception_log_is_one_line_json_without_exception_message(caplog):
    logger = logging.getLogger("test.structured.exception")
    secret = "private-answer-token"

    try:
        raise RuntimeError(secret)
    except RuntimeError as error:
        with caplog.at_level(logging.ERROR, logger=logger.name):
            log_event(
                "tts_failed",
                logger=logger,
                level=logging.ERROR,
                exc_info=error,
                error_type=type(error).__name__,
            )

    rendered = caplog.records[-1].getMessage()
    payload = json.loads(rendered)
    assert "\n" not in rendered
    assert secret not in rendered
    assert payload["error_type"] == "RuntimeError"
    assert "test_logging.py" in payload["stack_trace"]
