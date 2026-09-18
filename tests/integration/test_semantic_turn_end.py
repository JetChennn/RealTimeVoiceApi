import base64
import time

import pytest
from fastapi.testclient import TestClient

from realtime_voice.config import Settings
from realtime_voice.main import create_app
from realtime_voice.turn_end.model import SemanticDetector
from tests.integration.fake_services import FakeServiceHarness
from tests.integration.test_websocket_saturation import _create


class EnergyDetector:
    def has_speech(self, samples):
        return bool((samples != 0).any())


class EndModel:
    threshold = 0.5

    def predict(self, context, text):
        return 0.9


@pytest.mark.parametrize("end", [True, False])
def test_real_transport_asr_accumulation_emits_one_turn_and_releases_session(monkeypatch, end):
    from realtime_voice.transport import factory
    from realtime_voice.turn_end import model

    monkeypatch.setattr(factory, "SileroDetector", EnergyDetector)
    fake_model = EndModel()
    fake_model.threshold = 0.5 if end else 1
    monkeypatch.setattr(model, "LocalModel", lambda *args: fake_model)
    # Keep this test focused on resumed-speech accumulation. Immediate semantic
    # commit with the zero default is covered by the coordinator unit test.
    settings = Settings(
        _env_file=None,
        turn_end_semantic_enabled=True,
        turn_end_min_silence_ms=1000,
    )
    app = create_app(settings)
    harness = FakeServiceHarness(["真不知道。", "事先一点预兆都没有。"])
    services = app.state.services
    services.asr_client = harness.asr
    services.thinker_client = harness.thinker
    services.tts_client = harness.tts
    with TestClient(app) as client:
        health = client.get("/health").json()
        assert health["semantic"] == {
            "enabled": True,
            "ready": True,
            "concurrency": 30,
            "max_pending_jobs": 32,
            "pending_jobs": 0,
        }
        with client.websocket_connect("/v1/realtime") as ws:
            ws.send_json(_create())
            assert ws.receive_json()["type"] == "SESSION_CREATED"
            seq = 0
            # Continuous input including all silence; no extra ASR of previous segments.
            for speech, frames in [(True, 10), (False, 16), (True, 10), (False, 63)]:
                data = (b"\x01\x00" if speech else b"\0\0") * 512 * frames
                for offset in range(0, len(data), 10240):
                    ws.send_json(
                        {
                            "type": "AUDIO_CHUNK",
                            "session_id": "session-1",
                            "sequence": seq,
                            "audio_b64": base64.b64encode(data[offset : offset + 10240]).decode(),
                        }
                    )
                    seq += 1
            messages = []
            while True:
                message = ws.receive_json()
                messages.append(message)
                if message["type"] == "RESPONSE_END":
                    break
            assert [m["text"] for m in messages if m["type"] == "ASR_RESULT"] == [
                "真不知道。 事先一点预兆都没有。"
            ]
            assert all(m["turn_id"] == 1 for m in messages)
            assert not any(m["type"] in {"ERROR", "TURN_STATE"} for m in messages)
            assert messages[-1]["status"] == "COMPLETED"
            assert any(m["type"] == "AUDIO_DELTA" for m in messages)
            assert harness.asr.calls == 2
            runtime = services.registry._runtimes["session-1"]
            ws.send_json({"type": "CLOSE_SESSION", "session_id": "session-1"})
            deadline = time.monotonic() + 2
            while runtime.is_running and time.monotonic() < deadline:
                time.sleep(0.005)
            assert not runtime.is_running
        assert services.registry.active_count == 0


async def test_enabled_missing_model_fails_startup_clearly(tmp_path):
    from realtime_voice.observability.metrics import Metrics

    settings = Settings(
        _env_file=None, turn_end_semantic_enabled=True, turn_end_model_path=str(tmp_path)
    )
    detector = SemanticDetector(settings, Metrics())
    with pytest.raises(FileNotFoundError, match="manifest.json"):
        await detector.start()
    assert detector.closed
