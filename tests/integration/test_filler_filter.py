import base64
import time

import pytest
from fastapi.testclient import TestClient

from realtime_voice.config import Settings
from realtime_voice.main import create_app
from tests.integration.fake_services import (
    FakeServiceHarness,
    connected_session,
    receive_until,
    send_audio,
)
from tests.integration.test_semantic_turn_end import EndModel, EnergyDetector
from tests.integration.test_websocket_saturation import _create


def wait_until(predicate):
    deadline = time.monotonic() + 1
    while not predicate() and time.monotonic() < deadline:
        time.sleep(0.005)
    assert predicate()


@pytest.mark.parametrize("semantic", [False, True])
@pytest.mark.parametrize("enabled", [False, True])
def test_real_transport_filter_configuration_and_next_valid_turn(monkeypatch, semantic, enabled):
    from realtime_voice.transport import factory
    from realtime_voice.turn_end import model

    monkeypatch.setattr(factory, "SileroDetector", EnergyDetector)
    monkeypatch.setattr(model, "LocalModel", lambda *args: EndModel())
    app = create_app(
        Settings(
            _env_file=None,
            turn_end_semantic_enabled=semantic,
            asr_filler_filter_enabled=enabled,
            tts_mode="dialogue_stream",
        )
    )
    harness = FakeServiceHarness(["嗯……", "嗯，我想去杭州。"])
    services = app.state.services
    services.asr_client = harness.asr
    services.thinker_client = harness.thinker
    services.tts_client = harness.tts
    with TestClient(app) as client, client.websocket_connect("/v1/realtime") as ws:
        ws.send_json(_create())
        assert ws.receive_json()["type"] == "SESSION_CREATED"
        seq = 0

        def speak():
            nonlocal seq
            for speaking, frames in [(True, 10), (False, 16)]:
                data = (b"\x01\x00" if speaking else b"\x00\x00") * 512 * frames
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

        speak()
        if enabled:
            wait_until(
                lambda: (
                    services.metrics.registry.get_sample_value(
                        "realtime_voice_user_inputs_filtered_total", {"reason": "filler_only"}
                    )
                    == 1
                )
            )
            runtime = services.registry._runtimes["session-1"]
            assert runtime.actor.state.turns == {}
            assert runtime._turn_speech_ends == {}
            assert harness.thinker.requests == []
            assert harness.tts.requests == []
        else:
            messages = receive_until(ws, "RESPONSE_END")
            assert messages[0]["text"] == "嗯……"
            assert messages[0]["turn_id"] == 1
        speak()
        messages = receive_until(ws, "RESPONSE_END")
        assert messages[0]["type"] == "ASR_RESULT"
        assert messages[0]["text"] == "嗯，我想去杭州。"
        assert messages[0]["turn_id"] == (1 if enabled else 2)
        assert messages[-1]["status"] == "COMPLETED"
        assert not any(message["type"] in {"ERROR", "TURN_STATE"} for message in messages)
        assert len(harness.thinker.requests) == (1 if enabled else 2)
        assert len(harness.tts.requests) == (1 if enabled else 2)
        assert harness.asr.calls == 2


@pytest.mark.parametrize("stage", ["thinker", "tts"])
def test_filler_does_not_interrupt_active_response_over_websocket(stage):
    harness = FakeServiceHarness(
        ["first", "嗯哦！"],
        block_first_thinker=stage == "thinker",
        block_first_tts=stage == "tts",
    )
    with connected_session(harness) as (ws, _):
        send_audio(ws, 0)
        receive_until(ws, "TEXT_DELTA" if stage == "thinker" else "AUDIO_DELTA")
        send_audio(ws, 1)
        try:
            wait_until(
                lambda: (
                    2 in harness.runtime.actor.state.registered_asr_segment_ids
                    and 2 not in harness.runtime.actor.state.pending_asr_segment_ids
                )
            )
            assert harness.runtime.actor.state.next_turn_id == 2
            assert harness.runtime.actor.state.turns[1].interrupted is False
            assert "interrupt" not in harness.thinker.calls
        finally:
            harness.thinker.release_first.set()
            harness.tts.release_first.set()
        messages = receive_until(ws, "RESPONSE_END")
        assert messages[-1]["status"] == "COMPLETED"
        assert not any(message["type"] in {"ASR_RESULT", "TURN_STATE"} for message in messages)
        assert len(harness.thinker.requests) == 1
        assert len(harness.tts.requests) == 1
