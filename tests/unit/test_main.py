from fastapi.testclient import TestClient

from realtime_voice.config import Settings
from realtime_voice.main import create_app


def test_health_reports_ready():
    with TestClient(create_app(Settings(_env_file=None))) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert response.json()["active_sessions"] == 0


def test_health_reports_cached_capacity_without_downstream_waits():
    app = create_app(Settings(_env_file=None, max_sessions=7, cpu_workers=2))
    app.state.services.downstream_health["asr"] = {"status": "degraded"}

    with TestClient(app) as client:
        response = client.get("/health")

    payload = response.json()
    assert response.status_code == 200
    assert payload["ready"] is True
    assert payload["capacity"] == {"max_sessions": 7, "cpu_workers": 2, "cpu_pending_jobs": 128}
    assert payload["activity"]["active_sessions"] == 0
    assert payload["downstream"]["asr"] == {"status": "degraded"}


def test_metrics_route_has_prometheus_content_type_and_isolation_per_app():
    first = create_app(Settings(_env_file=None))
    second = create_app(Settings(_env_file=None))

    with TestClient(first) as client:
        response = client.get("/metrics")
    with TestClient(second) as client:
        response = client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain; version=")
    assert "realtime_voice_active_sessions" in response.text
