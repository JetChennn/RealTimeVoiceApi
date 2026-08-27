import threading

from fastapi.testclient import TestClient

from tests.integration.fake_services import (
    FakeServiceHarness,
    receive_until,
    send_audio,
)


def test_disconnect_waits_for_thinker_before_delete() -> None:
    harness = FakeServiceHarness(["first"], block_first_thinker=True)
    with (
        TestClient(harness.app()) as client,
        client.websocket_connect("/v1/realtime") as websocket,
    ):
        websocket.send_json(
            {
                "type": "CREATE_SESSION",
                "protocol_version": 1,
                "device_id": "device-1",
                "session_id": "session-1",
                "audio_format": "PCM16",
                "audio_transport": "BASE64_JSON",
                "sample_rate": 16000,
                "channels": 1,
            }
        )
        assert websocket.receive_json()["type"] == "SESSION_CREATED"
        send_audio(websocket, 0)
        receive_until(websocket, "TEXT_DELTA", turn_id=1)
        assert harness.thinker.first_delta.wait(timeout=1)
        threading.Timer(0.05, harness.thinker.release_first.set).start()
        websocket.close()
        assert harness.thinker.deleted.wait(timeout=1)
    assert harness.thinker.calls.index("done:first") < harness.thinker.calls.index("delete")
