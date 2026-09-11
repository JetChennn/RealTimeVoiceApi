from pydantic import AnyHttpUrl, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="RTVA_", env_file=".env", extra="ignore", validate_default=True
    )

    host: str = "0.0.0.0"
    port: int = Field(default=8003, ge=1, le=65535)
    asr_base_url: AnyHttpUrl = "http://127.0.0.1:8000"
    thinker_base_url: AnyHttpUrl = "http://127.0.0.1:8082"
    tts_base_url: AnyHttpUrl = "http://127.0.0.1:8001"
    rag_base_url: AnyHttpUrl = "http://127.0.0.1:8004"
    rag_timeout_seconds: float = Field(default=2.0, gt=0, allow_inf_nan=False)
    rag_concurrency: int = Field(default=8, ge=1)
    rag_max_waiters: int = Field(default=64, ge=0)
    rag_top_k_per_scene: int = Field(default=5, ge=1, le=10)
    rag_top_k_total: int = Field(default=8, ge=3, le=20)
    rag_score_threshold: float = Field(default=0.0, ge=0, le=1)
    allowed_sample_rates: tuple[int, ...] = (16000, 24000, 48000)
    max_sessions: int = Field(default=64, ge=1)
    cpu_workers: int = Field(default=4, ge=1)
    cpu_pending_jobs: int = Field(default=128, ge=1)
    asr_concurrency: int = Field(default=8, ge=1)
    asr_max_waiters: int = Field(default=64, ge=0)
    handshake_timeout_seconds: float = Field(default=5.0, gt=0)
    session_event_queue_size: int = Field(default=256, ge=1)
    session_audio_queue_size: int = Field(default=64, ge=1)
    session_asr_queue_size: int = Field(default=64, ge=1)
    session_outbound_queue_size: int = Field(default=256, ge=1)
    session_audio_queue_max_seconds: float = Field(default=3.0, gt=0)
    session_outbound_queue_max_bytes: int = Field(default=8 * 1024 * 1024, ge=1)
    thinker_cleanup_timeout_seconds: float = Field(default=120.0, gt=0)
    tts_drain_timeout_seconds: float = Field(default=120.0, gt=0)
    # 下游 /health 后台探测：周期刷新缓存，避免 /health 路由产生网络等待
    downstream_probe_interval_seconds: float = Field(default=10.0, gt=0)
    downstream_probe_timeout_seconds: float = Field(default=2.0, gt=0)
    # 非空时透传给 TTS，跳过其内部 qwen-flash prompt 生成（可消除 ~18s 网络延迟）
    tts_prompt_override: str = ""
