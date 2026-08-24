import json
import logging

from realtime_voice.observability.logging import bind_context, log_event


def test_structured_log_omits_audio_and_full_text(caplog):
    with caplog.at_level(logging.INFO), bind_context(user_id="u", session_id="s", turn_id=1):
        log_event(
            "asr_completed",
            duration_ms=20.0,
            audio_bytes=b"secret",
            text="complete conversation",
        )

    payload = json.loads(caplog.records[-1].message)
    assert payload == {
        "duration_ms": 20.0,
        "event": "asr_completed",
        "session_id": "s",
        "turn_id": 1,
        "user_id": "u",
    }


def test_bound_context_is_restored_after_logging_scope(caplog):
    with caplog.at_level(logging.INFO):
        with bind_context(user_id="u", session_id="s"):
            log_event("session_started")
        log_event("process_ready")

    bound, _unbound = (json.loads(record.message) for record in caplog.records[-2:])
    assert bound["session_id"] == "s"
