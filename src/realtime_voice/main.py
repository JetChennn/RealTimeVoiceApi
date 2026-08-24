from dataclasses import dataclass

from fastapi import FastAPI, WebSocket

from realtime_voice.config import Settings
from realtime_voice.session.registry import RuntimeFactory
from realtime_voice.transport.factory import configure_services
from realtime_voice.transport.websocket import serve_realtime


@dataclass
class AppServices:
    """Application services used by the realtime WebSocket route."""

    settings: Settings
    runtime_factory: RuntimeFactory | None = None


def create_app(
    settings: Settings | None = None,
    *,
    runtime_factory: RuntimeFactory | None = None,
) -> FastAPI:
    resolved = settings or Settings()
    app = FastAPI(title="RealTimeVoiceAPI", version="1.0.0")
    app.state.settings = resolved
    app.state.services = AppServices(settings=resolved, runtime_factory=runtime_factory)
    configure_services(app.state.services)

    @app.get("/health")
    async def health() -> dict[str, object]:
        return {"status": "ok", "active_sessions": 0, "max_sessions": resolved.max_sessions}

    @app.websocket("/v1/realtime")
    async def realtime(websocket: WebSocket) -> None:
        await serve_realtime(websocket, websocket.app.state.services)

    return app


app = create_app()
