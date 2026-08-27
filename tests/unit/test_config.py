import pytest
from pydantic import ValidationError

from realtime_voice.config import Settings


def test_settings_defaults():
    settings = Settings(_env_file=None)

    assert settings.host == "0.0.0.0"
    assert settings.port == 8003
    assert str(settings.asr_base_url).rstrip("/") == "http://127.0.0.1:8000"
    assert str(settings.thinker_base_url).rstrip("/") == "http://127.0.0.1:8082"
    assert str(settings.tts_base_url).rstrip("/") == "http://127.0.0.1:8001"
    assert settings.allowed_sample_rates == (16000, 24000, 48000)
    assert settings.max_sessions == 64
    assert settings.cpu_workers == 4


def test_settings_exposes_runtime_queue_and_cleanup_limits() -> None:
    settings = Settings(_env_file=None)

    assert settings.session_event_queue_size == 256
    assert settings.session_audio_queue_size == 64
    assert settings.session_asr_queue_size == 64
    assert settings.session_outbound_queue_size == 256
    assert settings.session_audio_queue_max_seconds == 3.0
    assert settings.session_outbound_queue_max_bytes == 8 * 1024 * 1024
    assert settings.thinker_cleanup_timeout_seconds == 120.0
    assert settings.tts_drain_timeout_seconds == 120.0



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
        "tts_drain_timeout_seconds",
    ],
)
def test_settings_rejects_nonpositive_runtime_limits(field: str) -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **{field: 0})
