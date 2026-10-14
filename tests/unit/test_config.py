from pathlib import Path

import pytest
from pydantic import ValidationError

from realtime_voice.config import Settings


def test_filler_filter_defaults_and_environment(monkeypatch):
    settings = Settings(_env_file=None)
    assert settings.asr_filler_filter_enabled is True
    assert settings.asr_filler_filter_chars == "嗯哦噢喔啊呃额"
    assert settings.asr_filler_filter_max_chars == 6
    monkeypatch.setenv("RTVA_ASR_FILLER_FILTER_ENABLED", "false")
    monkeypatch.setenv("RTVA_ASR_FILLER_FILTER_CHARS", "嗯嗯啊")
    monkeypatch.setenv("RTVA_ASR_FILLER_FILTER_MAX_CHARS", "2")
    settings = Settings(_env_file=None)
    assert settings.asr_filler_filter_enabled is False
    assert settings.asr_filler_filter_chars == "嗯啊"
    assert settings.asr_filler_filter_max_chars == 2


@pytest.mark.parametrize("chars", ["", "嗯 哦", "嗯？", "啊?", "嗯，", "嗯\n"])
def test_invalid_filler_character_sets_fail_validation(chars):
    with pytest.raises(ValidationError):
        Settings(_env_file=None, asr_filler_filter_chars=chars)


@pytest.mark.parametrize("limit", [0, -1, 129])
def test_invalid_filler_limits_fail_validation(limit):
    with pytest.raises(ValidationError):
        Settings(_env_file=None, asr_filler_filter_max_chars=limit)


def test_settings_defaults():
    settings = Settings(_env_file=None)

    assert settings.host == "0.0.0.0"
    assert settings.port == 8000
    assert settings.keep_alive_seconds == 10
    assert str(settings.asr_base_url).rstrip("/") == "http://127.0.0.1:8001"
    assert str(settings.thinker_base_url).rstrip("/") == "http://127.0.0.1:8002"
    assert str(settings.tts_base_url).rstrip("/") == "http://127.0.0.1:9000"
    assert settings.tts_mode == "bistream"
    assert settings.tts_ref_audio == "ref_audio/tongtong.wav"
    assert settings.tts_ref_text == "在一座美丽的森林里，住着许多动物。"
    assert settings.allowed_sample_rates == (16000, 24000, 48000)
    assert settings.max_sessions == 30
    assert settings.asr_context_workers == 2
    assert settings.asr_context_extract_timeout_ms == 100
    assert settings.cpu_workers == 8
    assert settings.cpu_pending_jobs == 256
    assert settings.asr_concurrency == 30
    assert settings.asr_max_waiters == 30
    assert settings.asr_context_enabled is True
    assert settings.asr_context_max_keywords == 12
    assert settings.asr_context_max_chars == 128
    assert settings.asr_context_extract_timeout_ms == 100


def test_settings_exposes_runtime_queue_and_cleanup_limits() -> None:
    settings = Settings(_env_file=None)

    assert settings.session_event_queue_size == 256
    assert settings.session_audio_queue_size == 64
    assert settings.session_asr_queue_size == 64
    assert settings.session_outbound_queue_size == 256
    assert settings.session_audio_queue_max_seconds == 3.0
    assert settings.session_outbound_queue_max_bytes == 8 * 1024 * 1024
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
    assert not hasattr(settings, "tts_drain_timeout_seconds")
    assert settings.tts_prompt_override == "平和"
    assert settings.turn_end_concurrency == 30
    assert settings.turn_end_min_silence_ms == 0
    assert settings.turn_end_inference_timeout_ms == 500
    assert settings.turn_end_cpu_threads == 16
    assert settings.slow_stage_warning_seconds == 2.0
    assert settings.vad_min_speech_ms == 200
    assert settings.speaker_verification_enabled is False
    assert settings.speaker_register_min_audio_ms == 2000
    assert settings.speaker_verification_min_audio_ms == 600
    assert settings.speaker_vad_pre_padding_ms == 100
    assert settings.speaker_vad_post_padding_ms == 150


def test_keyword_process_count_can_be_configured(monkeypatch):
    monkeypatch.setenv("RTVA_ASR_CONTEXT_WORKERS", "3")
    assert Settings(_env_file=None).asr_context_workers == 3


@pytest.mark.parametrize("workers", [0, -1, 33])
def test_invalid_keyword_process_counts_are_rejected(workers):
    with pytest.raises(ValidationError):
        Settings(_env_file=None, asr_context_workers=workers)


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


def test_obsolete_tts_drain_environment_variable_is_ignored(tmp_path: Path) -> None:
    environment = tmp_path / ".env"
    environment.write_text("RTVA_TTS_DRAIN_TIMEOUT_SECONDS=0.5\n", encoding="utf-8")

    settings = Settings(_env_file=environment)

    assert not hasattr(settings, "tts_drain_timeout_seconds")


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
        "keep_alive_seconds",
        "session_event_queue_size",
        "session_audio_queue_size",
        "session_asr_queue_size",
        "session_outbound_queue_size",
        "session_audio_queue_max_seconds",
        "session_outbound_queue_max_bytes",
        "thinker_stream_timeout_seconds",
        "thinker_reply_total_timeout_seconds",
        "tts_first_audio_timeout_seconds",
        "tts_idle_timeout_seconds",
        "slow_stage_warning_seconds",
    ],
)
def test_settings_rejects_nonpositive_runtime_limits(field: str) -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **{field: 0})
