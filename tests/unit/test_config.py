from realtime_voice.config import Settings


def test_settings_defaults():
    settings = Settings(_env_file=None)

    assert settings.host == "0.0.0.0"
    assert settings.port == 8003
    assert str(settings.asr_base_url).rstrip("/") == "http://127.0.0.1:8000"
    assert str(settings.berry_base_url).rstrip("/") == "http://127.0.0.1:8082"
    assert str(settings.tts_base_url).rstrip("/") == "http://127.0.0.1:8002"
    assert settings.allowed_sample_rates == (16000, 24000, 48000)
    assert settings.max_sessions == 64
    assert settings.cpu_workers == 4
