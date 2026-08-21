from fastapi.testclient import TestClient

from realtime_voice.config import Settings
from realtime_voice.main import create_app


def test_health_reports_ready():
    with TestClient(create_app(Settings(_env_file=None))) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert response.json()["active_sessions"] == 0
