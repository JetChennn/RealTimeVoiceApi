import asyncio
import os
import threading
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from time import monotonic

from fastapi import FastAPI, Response, WebSocket
from prometheus_client import CONTENT_TYPE_LATEST

from realtime_voice.config import Settings
from realtime_voice.observability.metrics import Metrics
from realtime_voice.session.registry import RuntimeFactory
from realtime_voice.transport.factory import configure_services
from realtime_voice.transport.websocket import serve_realtime


@dataclass(frozen=True, slots=True)
class ProcessSnapshot:
    threads: int
    memory_bytes: int


def local_process_snapshot() -> ProcessSnapshot:
    """Read local process state without network access or downstream waits."""
    statm = Path("/proc/self/statm").read_text(encoding="ascii").split()
    if len(statm) < 2:
        raise RuntimeError("process memory snapshot is unavailable")
    return ProcessSnapshot(
        threads=threading.active_count(),
        memory_bytes=int(statm[1]) * os.sysconf("SC_PAGE_SIZE"),
    )


async def run_event_loop_lag_sampler(
    metrics: Metrics,
    *,
    interval: float,
    clock: Callable[[], float] = monotonic,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    """Continuously observe scheduler delay using injectable local timing primitives."""
    while True:
        deadline = clock() + interval
        await sleep(interval)
        metrics.record_event_loop_lag(max(0.0, clock() - deadline))


@dataclass
class AppServices:
    """Application services used by the realtime WebSocket route."""

    settings: Settings
    runtime_factory: RuntimeFactory | None = None
    metrics: Metrics | None = None
    downstream_health: dict[str, dict[str, str]] = field(default_factory=dict)

    process_snapshot: Callable[[], ProcessSnapshot] = local_process_snapshot

def create_app(
    settings: Settings | None = None,
    *,
    runtime_factory: RuntimeFactory | None = None,
    metrics: Metrics | None = None,
    event_loop_clock: Callable[[], float] = monotonic,
    event_loop_sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    event_loop_interval: float = 1.0,
) -> FastAPI:
    if event_loop_interval <= 0:
        raise ValueError("event loop sampling interval must be positive")
    resolved = settings or Settings()
    services = AppServices(
        settings=resolved,
        runtime_factory=runtime_factory,
        metrics=metrics or Metrics(),
        downstream_health={name: {"status": "unknown"} for name in ("asr", "berry", "tts")},
    )
    configure_services(services)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        sampler = asyncio.create_task(
            run_event_loop_lag_sampler(
                services.metrics,
                interval=event_loop_interval,
                clock=event_loop_clock,
                sleep=event_loop_sleep,
            ),
            name="event-loop-lag-sampler",
        )
        try:
            yield
        finally:
            sampler.cancel()
            await asyncio.gather(sampler, return_exceptions=True)
            await services.detector_offload.aclose()

    app = FastAPI(title="RealTimeVoiceAPI", version="1.0.0", lifespan=lifespan)
    app.state.settings = resolved
    app.state.services = services

    async def limiter_state() -> dict[str, dict[str, object]]:
        snapshots: dict[str, dict[str, object]] = {}
        for name in ("asr", "berry", "tts"):
            try:
                snapshot = await getattr(services, f"{name}_client").admission.snapshot()
            except Exception as error:  # noqa: BLE001 - health must remain available
                snapshots[name] = {
                    "status": "unavailable",
                    "error_type": type(error).__name__,
                }
                continue
            snapshots[name] = {
                "status": "ok",
                "active": snapshot.active,
                "waiting": snapshot.waiting,
            }
            services.metrics.set_limiter_state(
                name, active=snapshot.active, waiting=snapshot.waiting
            )
        return snapshots

    async def collect_health() -> dict[str, object]:
        limiters = await limiter_state()

        try:
            registry_snapshot = services.registry.activity_snapshot()
        except Exception as error:  # noqa: BLE001 - health must remain available
            activity: dict[str, object] = {
                "status": "unavailable",
                "error_type": type(error).__name__,
            }
            active_sessions: int | None = None
        else:
            active_sessions = registry_snapshot.active_sessions
            activity = {
                "status": "ok",
                "active_sessions": active_sessions,
                "queues": registry_snapshot.queues,
            }
            services.metrics.set_active_sessions(active_sessions)
            for name, state in registry_snapshot.queues.items():
                services.metrics.set_queue_state(
                    name,
                    items=state["items"],
                    byte_count=state.get("bytes"),
                )

        try:
            detector = services.detector_offload.snapshot()
        except Exception as error:  # noqa: BLE001 - health must remain available
            executor: dict[str, object] = {
                "status": "unavailable",
                "error_type": type(error).__name__,
            }
        else:
            executor = {
                "status": "ok",
                "workers": detector.workers,
                "active": detector.active,
                "pending": detector.pending,
                "pending_limit": resolved.cpu_pending_jobs,
            }
            services.metrics.set_executor_workers(detector.workers)
            services.metrics.set_executor_state(active=detector.active, pending=detector.pending)

        try:
            process_snapshot = services.process_snapshot()
        except Exception as error:  # noqa: BLE001 - health must remain available
            process: dict[str, object] = {
                "status": "unavailable",
                "error_type": type(error).__name__,
            }
        else:
            process = {
                "status": "ok",
                "threads": process_snapshot.threads,
                "memory_bytes": process_snapshot.memory_bytes,
            }
            services.metrics.set_process_state(
                threads=process_snapshot.threads,
                memory_bytes=process_snapshot.memory_bytes,
            )

        snapshots_available = (
            activity["status"] == "ok"
            and executor["status"] == "ok"
            and process["status"] == "ok"
            and all(state["status"] == "ok" for state in limiters.values())
        )
        downstream_ready = all(
            state.get("status") in {"ok", "healthy"}
            for state in services.downstream_health.values()
        )
        capacity_available = (
            active_sessions is not None and active_sessions < resolved.max_sessions
        )
        executor_available = (
            executor["status"] == "ok"
            and isinstance(executor.get("pending"), int)
            and executor["pending"] < resolved.cpu_pending_jobs
        )
        ready = bool(
            snapshots_available and downstream_ready and capacity_available and executor_available
        )
        return {
            "status": "ok" if ready else "degraded",
            "ready": ready,
            "active_sessions": active_sessions,
            "max_sessions": resolved.max_sessions,
            "capacity": {
                "max_sessions": resolved.max_sessions,
                "cpu_workers": resolved.cpu_workers,
                "cpu_pending_jobs": resolved.cpu_pending_jobs,
            },
            "activity": activity,
            "limiters": limiters,
            "executor": executor,
            "process": process,
            "downstream": services.downstream_health,
        }

    @app.get("/health")
    async def health() -> dict[str, object]:
        return await collect_health()

    @app.get("/metrics")
    async def prometheus_metrics() -> Response:
        await collect_health()
        return Response(services.metrics.render(), media_type=CONTENT_TYPE_LATEST)

    @app.websocket("/v1/realtime")
    async def realtime(websocket: WebSocket) -> None:
        await serve_realtime(websocket, websocket.app.state.services)

    return app


app = create_app()
