from pydantic import AnyHttpUrl, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="RTVA_", env_file=".env", extra="ignore", validate_default=True
    )

    host: str = "0.0.0.0"
    port: int = Field(default=8003, ge=1, le=65535)
    asr_base_url: AnyHttpUrl = "http://127.0.0.1:8000"
    berry_base_url: AnyHttpUrl = "http://127.0.0.1:8082"
    tts_base_url: AnyHttpUrl = "http://127.0.0.1:8002"
    allowed_sample_rates: tuple[int, ...] = (16000, 24000, 48000)
    max_sessions: int = Field(default=64, ge=1)
    cpu_workers: int = Field(default=4, ge=1)
    cpu_pending_jobs: int = Field(default=128, ge=1)
    handshake_timeout_seconds: float = Field(default=5.0, gt=0)
