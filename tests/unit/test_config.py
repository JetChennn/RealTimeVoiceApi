from pathlib import Path

import pytest
from pydantic import ValidationError

from realtime_voice.config import Settings


def test_settings_defaults():
    settings = Settings(_env_file=None)

    assert settings.host == "0.0.0.0"
    assert settings.port == 8000
    assert str(settings.asr_base_url).rstrip("/") == "http://127.0.0.1:8001"
    assert str(settings.thinker_base_url).rstrip("/") == "http://127.0.0.1:8002"
    assert str(settings.tts_base_url).rstrip("/") == "http://127.0.0.1:9000"
    assert str(settings.rag_base_url).rstrip("/") == "http://127.0.0.1:8003"
    assert settings.allowed_sample_rates == (16000, 24000, 48000)
    assert settings.max_sessions == 30
    assert settings.cpu_workers == 8
    assert settings.cpu_pending_jobs == 256
    assert settings.asr_concurrency == 30
    assert settings.asr_max_waiters == 30


def test_settings_exposes_runtime_queue_and_cleanup_limits() -> None:
    settings = Settings(_env_file=None)

    assert settings.session_event_queue_size == 256
    assert settings.session_audio_queue_size == 64
    assert settings.session_asr_queue_size == 64
    assert settings.session_outbound_queue_size == 256
    assert settings.session_audio_queue_max_seconds == 3.0
    assert settings.session_outbound_queue_max_bytes == 8 * 1024 * 1024
    assert settings.thinker_cleanup_timeout_seconds == 25.0
    assert settings.thinker_stream_timeout_seconds == 5.0
    assert settings.thinker_reply_total_timeout_seconds == 20.0
    assert settings.thinker_concurrency == 30
    assert settings.thinker_max_waiters == 64
    assert settings.thinker_fallback_enabled is True
    assert settings.thinker_fallback_text_1.startswith("抱歉")
    assert settings.thinker_fallback_text_2.startswith("不好意思")
    assert settings.thinker_fallback_text_3.startswith("抱歉")
    assert settings.thinker_fallback_tone == "平和、自然、略带歉意"
    assert settings.tts_concurrency == 30
    assert settings.tts_max_waiters == 64
    assert settings.tts_first_audio_timeout_seconds == 5.0
    assert settings.tts_idle_timeout_seconds == 5.0
    assert settings.tts_drain_timeout_seconds == 0.01
    assert settings.turn_end_concurrency == 30
    assert settings.turn_end_min_silence_ms == 0
    assert settings.turn_end_inference_timeout_ms == 500
    assert settings.turn_end_cpu_threads == 8
    assert settings.slow_stage_warning_seconds == 2.0


def test_env_example_covers_every_runtime_setting() -> None:
    example = Path(__file__).resolve().parents[2] / ".env.example"
    configured = {
        line.split("=", 1)[0]
        for line in example.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#") and "=" in line
    }
    expected = {
        f"RTVA_{name.upper()}"
        for name in Settings.model_fields
        if name != "turn_end_complete_threshold"
    }

    assert configured == expected
    Settings(_env_file=example)


def test_thinker_fallback_texts_must_be_non_empty_and_distinct() -> None:
    with pytest.raises(ValidationError, match="must not be empty"):
        Settings(_env_file=None, thinker_fallback_text_1="  ")
    with pytest.raises(ValidationError, match="must be distinct"):
        Settings(
            _env_file=None,
            thinker_fallback_text_2="同一句",
            thinker_fallback_text_3="同一句",
        )

@pytest.mark.parametrize(
    "field",
    [
        "session_event_queue_size",
        "session_audio_queue_size",
        "session_asr_queue_size",
        "session_outbound_queue_size",
        "session_audio_queue_max_seconds",
        "session_outbound_queue_max_bytes",
        "thinker_cleanup_timeout_seconds",
        "thinker_stream_timeout_seconds",
        "thinker_reply_total_timeout_seconds",
        "tts_first_audio_timeout_seconds",
        "tts_idle_timeout_seconds",
        "tts_drain_timeout_seconds",
        "slow_stage_warning_seconds",
    ],
)
def test_settings_rejects_nonpositive_runtime_limits(field: str) -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **{field: 0})
