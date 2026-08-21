from fastapi import FastAPI

from realtime_voice.config import Settings


def create_app(settings: Settings | None = None) -> FastAPI:
    resolved = settings or Settings()
    app = FastAPI(title="RealTimeVoiceAPI", version="1.0.0")
    app.state.settings = resolved

    @app.get("/health")
    async def health() -> dict[str, object]:
        return {"status": "ok", "active_sessions": 0, "max_sessions": resolved.max_sessions}

    return app


app = create_app()
