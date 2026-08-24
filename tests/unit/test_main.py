import asyncio
import threading

from fastapi.testclient import TestClient

from realtime_voice.audio.vad import SpeechSegment
from realtime_voice.config import Settings
from realtime_voice.main import create_app
from realtime_voice.protocol.server_messages import TextDelta
from realtime_voice.session.actor import QueueAsr
from tests.unit.session.test_runtime import make_runtime


def mark_downstream_ok(app) -> None:
    for state in app.state.services.downstream_health.values():
        state["status"] = "ok"


def test_health_reports_ready():
    app = create_app(Settings(_env_file=None))
    mark_downstream_ok(app)

    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert response.json()["ready"] is True
    assert response.json()["active_sessions"] == 0


def test_health_readiness_transitions_with_cached_downstream_state() -> None:
    app = create_app(Settings(_env_file=None))

    with TestClient(app) as client:
        initial = client.get("/health").json()
        mark_downstream_ok(app)
        ready = client.get("/health").json()
        app.state.services.downstream_health["tts"]["status"] = "degraded"
        degraded = client.get("/health").json()

    assert (initial["status"], initial["ready"]) == ("degraded", False)
    assert (ready["status"], ready["ready"]) == ("ok", True)
    assert (degraded["status"], degraded["ready"]) == ("degraded", False)


def test_health_reports_cached_capacity_without_downstream_waits():
    app = create_app(Settings(_env_file=None, max_sessions=7, cpu_workers=2))
    app.state.services.downstream_health["asr"] = {"status": "degraded"}

    with TestClient(app) as client:
        response = client.get("/health")

    payload = response.json()
    assert response.status_code == 200
    assert payload["status"] == "degraded"
    assert payload["ready"] is False
    assert payload["capacity"] == {"max_sessions": 7, "cpu_workers": 2, "cpu_pending_jobs": 128}
    assert payload["activity"]["active_sessions"] == 0
    assert payload["downstream"]["asr"] == {"status": "degraded"}


def test_health_never_touches_downstream_network_clients() -> None:
    class NetworkSentinel:
        def __getattr__(self, name: str) -> object:
            raise AssertionError(f"health touched downstream network attribute {name}")

    app = create_app(Settings(_env_file=None))
    mark_downstream_ok(app)
    for name in ("asr", "berry", "tts"):
        getattr(app.state.services, f"{name}_client").http = NetworkSentinel()

    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["ready"] is True


def test_health_aggregates_actual_runtime_queues_executor_and_process_state() -> None:
    app = create_app(Settings(_env_file=None, max_sessions=3, cpu_workers=2))
    mark_downstream_ok(app)
    runtime, _ = make_runtime(metrics=app.state.services.metrics)
    second_runtime, _ = make_runtime(metrics=app.state.services.metrics)
    runtime.events.put_nowait(object())
    second_runtime.events.put_nowait(object())
    runtime.audio_queue.put_nowait(b"\x01\x00")
    second_runtime.audio_queue.put_nowait(b"\x02\x00\x03\x00")
    asyncio.run(
        runtime.execute_effect(
            QueueAsr(
                session_id="s",
                segment=SpeechSegment(segment_id=1, pcm16_16k=b"\x02\x00"),
            )
        )
    )
    runtime.outbound.put_nowait(
        TextDelta(
            type="TEXT_DELTA",
            user_id="u",
            session_id="s",
            turn_id=1,
            interrupt=False,
            delta="queued",
        )
    )
    asyncio.run(
        second_runtime.execute_effect(
            QueueAsr(
                session_id="s",
                segment=SpeechSegment(segment_id=2, pcm16_16k=b"\x04\x00"),
            )
        )
    )
    second_runtime.outbound.put_nowait(
        TextDelta(
            type="TEXT_DELTA",
            user_id="u",
            session_id="s",
            turn_id=2,
            interrupt=False,
            delta="also queued",
        )
    )
    asyncio.run(app.state.services.registry.add("s", runtime))
    asyncio.run(app.state.services.registry.add("second", second_runtime))

    with TestClient(app) as client:
        response = client.get("/health")
        metrics = client.get("/metrics").text

    payload = response.json()
    assert payload["activity"]["status"] == "ok"
    assert payload["activity"]["active_sessions"] == 2
    assert payload["activity"]["queues"] == {
        "event": {"items": 2},
        "audio": {"items": 2, "bytes": 6},
        "asr": {"items": 2},
        "outbound": {
            "items": 2,
            "bytes": runtime.outbound.queued_bytes + second_runtime.outbound.queued_bytes,
        },
    }
    assert payload["executor"] == {
        "status": "ok",
        "workers": 2,
        "active": 0,
        "pending": 0,
        "pending_limit": 128,
    }
    assert payload["process"]["status"] == "ok"
    assert payload["process"]["threads"] >= 1
    assert payload["process"]["memory_bytes"] > 0
    assert 'realtime_voice_queue_items{queue="audio"} 2.0' in metrics
    assert 'realtime_voice_queue_bytes{queue="audio"} 6.0' in metrics
    assert "realtime_voice_process_memory_bytes " in metrics


def test_health_snapshot_failures_are_unavailable_and_not_fake_zeroes() -> None:
    app = create_app(Settings(_env_file=None))
    mark_downstream_ok(app)

    async def limiter_failure():
        raise RuntimeError("limiter snapshot failed")

    def snapshot_failure():
        raise RuntimeError("snapshot failed")

    app.state.services.asr_client.admission.snapshot = limiter_failure
    app.state.services.registry.activity_snapshot = snapshot_failure
    app.state.services.detector_offload.snapshot = snapshot_failure
    app.state.services.process_snapshot = snapshot_failure

    with TestClient(app) as client:
        payload = client.get("/health").json()

    assert payload["status"] == "degraded"
    assert payload["ready"] is False
    assert payload["activity"] == {"status": "unavailable", "error_type": "RuntimeError"}
    assert payload["limiters"]["asr"] == {
        "status": "unavailable",
        "error_type": "RuntimeError",
    }
    assert payload["executor"] == {"status": "unavailable", "error_type": "RuntimeError"}
    assert payload["process"] == {"status": "unavailable", "error_type": "RuntimeError"}


def test_metrics_route_has_prometheus_content_type_and_isolation_per_app():
    first = create_app(Settings(_env_file=None))
    second = create_app(Settings(_env_file=None))
    first.state.services.metrics.record_error("asr", "FIRST_ONLY")

    with TestClient(first) as client:
        first_response = client.get("/metrics")
    with TestClient(second) as client:
        second_response = client.get("/metrics")

    assert first_response.status_code == 200
    assert first_response.headers["content-type"].startswith("text/plain; version=")
    assert 'code="FIRST_ONLY"' in first_response.text
    assert 'code="FIRST_ONLY"' not in second_response.text


def test_lifespan_sampler_records_injected_event_loop_lag() -> None:
    now = [100.0]
    sleep_calls = [0]
    sampler_waiting = threading.Event()

    async def fake_sleep(delay: float) -> None:
        sleep_calls[0] += 1
        if sleep_calls[0] == 1:
            now[0] += delay + 0.25
            return
        sampler_waiting.set()
        await asyncio.Event().wait()

    app = create_app(
        Settings(_env_file=None),
        event_loop_clock=lambda: now[0],
        event_loop_sleep=fake_sleep,
        event_loop_interval=1.0,
    )

    with TestClient(app):
        assert sampler_waiting.wait(timeout=1)
        rendered = app.state.services.metrics.render().decode()
        assert "realtime_voice_event_loop_lag_seconds_count 1.0" in rendered
        assert "realtime_voice_event_loop_lag_seconds_sum 0.25" in rendered
