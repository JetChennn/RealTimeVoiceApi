import threading
from dataclasses import dataclass, field

from fastapi import FastAPI, Response, WebSocket
from prometheus_client import CONTENT_TYPE_LATEST

from realtime_voice.config import Settings
from realtime_voice.observability.metrics import Metrics
from realtime_voice.session.registry import RuntimeFactory
from realtime_voice.transport.factory import configure_services
from realtime_voice.transport.websocket import serve_realtime


@dataclass
class AppServices:
    """Application services used by the realtime WebSocket route."""

    settings: Settings
    runtime_factory: RuntimeFactory | None = None
    metrics: Metrics | None = None
    downstream_health: dict[str, dict[str, str]] = field(default_factory=dict)


def create_app(
    settings: Settings | None = None,
    *,
    runtime_factory: RuntimeFactory | None = None,
    metrics: Metrics | None = None,
) -> FastAPI:
    resolved = settings or Settings()
    app = FastAPI(title="RealTimeVoiceAPI", version="1.0.0")
    app.state.settings = resolved
    services = AppServices(
        settings=resolved,
        runtime_factory=runtime_factory,
        metrics=metrics or Metrics(),
        downstream_health={name: {"status": "unknown"} for name in ("asr", "berry", "tts")},
    )
    app.state.services = services
    configure_services(services)

    async def limiter_state() -> dict[str, dict[str, int]]:
        snapshots: dict[str, dict[str, int]] = {}
        for name in ("asr", "berry", "tts"):
            try:
                snapshot = await getattr(services, f"{name}_client").admission.snapshot()
            except Exception:  # noqa: BLE001 - health must remain available
                snapshots[name] = {"active": 0, "waiting": 0}
                continue
            snapshots[name] = {"active": snapshot.active, "waiting": snapshot.waiting}
            services.metrics.set_limiter_state(
                name, active=snapshot.active, waiting=snapshot.waiting
            )
        return snapshots

    @app.get("/health")
    async def health() -> dict[str, object]:
        limiters = await limiter_state()
        active_sessions = services.registry.active_count
        services.metrics.set_active_sessions(active_sessions)
        services.metrics.set_executor_workers(resolved.cpu_workers)
        return {
            "status": "ok",
            "ready": True,
            "active_sessions": active_sessions,
            "max_sessions": resolved.max_sessions,
            "capacity": {
                "max_sessions": resolved.max_sessions,
                "cpu_workers": resolved.cpu_workers,
                "cpu_pending_jobs": resolved.cpu_pending_jobs,
            },
            "activity": {"active_sessions": active_sessions},
            "limiters": limiters,
            "executor": {
                "configured_workers": resolved.cpu_workers,
                "pending_limit": resolved.cpu_pending_jobs,
                "process_threads": threading.active_count(),
            },
            "downstream": services.downstream_health,
        }

    @app.get("/metrics")
    async def prometheus_metrics() -> Response:
        await limiter_state()
        services.metrics.set_active_sessions(services.registry.active_count)
        services.metrics.set_executor_workers(resolved.cpu_workers)
        return Response(services.metrics.render(), media_type=CONTENT_TYPE_LATEST)

    @app.websocket("/v1/realtime")
    async def realtime(websocket: WebSocket) -> None:
        await serve_realtime(websocket, websocket.app.state.services)

    return app


app = create_app()
