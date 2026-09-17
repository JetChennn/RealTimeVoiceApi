import json

import pytest
from fastapi import WebSocketDisconnect
from fastapi.testclient import TestClient

from realtime_voice.config import Settings
from realtime_voice.main import create_app
from tests.integration.test_websocket_saturation import _controlled_runtime_factory, _create


@pytest.mark.parametrize(
    ("second_id", "code"),
    [("session-1", "DUPLICATE_SESSION"), ("session-2", "SESSION_CAPACITY_EXCEEDED")],
)
def test_rejected_session_sends_error_and_close_without_removing_owner(second_id, code):
    settings = Settings(_env_file=None, max_sessions=1)
    app = create_app(settings, runtime_factory=_controlled_runtime_factory(settings))
    with TestClient(app) as client, client.websocket_connect("/v1/realtime") as owner:
        owner.send_json(_create())
        assert owner.receive_json()["type"] == "SESSION_CREATED"
        with client.websocket_connect("/v1/realtime") as rejected:
            rejected.send_json(dict(_create(), session_id=second_id))
            error = rejected.receive_json()
            assert error["type"] == "ERROR"
            assert error["code"] == code
            assert error["message"]
            assert error["recoverable"] is False
            with pytest.raises(WebSocketDisconnect) as closed:
                rejected.receive_json()
            assert closed.value.code == 1008
            assert closed.value.reason == code
        assert client.get("/health").json()["active_sessions"] == 1
        owner.send_json({"type": "CLOSE_SESSION", "session_id": "session-1"})


@pytest.mark.parametrize("failure", [FileNotFoundError, TimeoutError, RuntimeError])
def test_session_initialization_failure_returns_reason_and_internal_close(failure, caplog):
    def failing_factory(create, websocket):
        raise failure("private model path /internal/model")

    app = create_app(Settings(_env_file=None), runtime_factory=failing_factory)
    with TestClient(app) as client, client.websocket_connect("/v1/realtime") as socket:
        socket.send_json(_create())
        error = socket.receive_json()
        assert error["type"] == "ERROR"
        assert error["code"] == "SESSION_CREATE_FAILED"
        assert error["stage"] == "TRANSPORT"
        assert error["message"] == ("server could not initialize the session; please retry later")
        assert error["recoverable"] is False
        with pytest.raises(WebSocketDisconnect) as closed:
            socket.receive_json()
        assert closed.value.code == 1011
        assert closed.value.reason == "SESSION_CREATE_FAILED"
        assert client.get("/health").json()["active_sessions"] == 0
    failure_log = next(
        json.loads(record.message)
        for record in caplog.records
        if json.loads(record.message).get("event") == "session_create_failed"
    )
    assert failure_log["error_code"] == "SESSION_CREATE_FAILED"
    assert failure_log["session_id"] == "session-1"
    assert failure_log["error_type"] == failure.__name__
    assert "stack_trace" in failure_log
    assert "private model path /internal/model" not in caplog.text
