import pytest

from tests.integration.fake_services import (
    FakeServiceHarness,
    connected_session,
    receive_until,
    send_audio,
)


def test_full_turn_emits_asr_text_and_audio() -> None:
    harness = FakeServiceHarness(["hello"])
    with connected_session(harness) as (websocket, created):
        send_audio(websocket, 0)
        messages = [created, *receive_until(websocket, "RESPONSE_END")]

    assert [message["type"] for message in messages] == [
        "SESSION_CREATED",
        "ASR_RESULT",
        "TEXT_DELTA",
        "TEXT_END",
        "AUDIO_DELTA",
        "RESPONSE_END",
    ]
    assert messages[-1]["status"] == "COMPLETED"


@pytest.mark.parametrize("stage", ["berry", "tts"])
def test_downstream_turn_failure_emits_error_and_failed_response(stage: str) -> None:
    harness = FakeServiceHarness(["hello"], fail_stage=stage)
    with connected_session(harness) as (websocket, _):
        send_audio(websocket, 0)
        messages = receive_until(websocket, "RESPONSE_END")

    errors = [message for message in messages if message["type"] == "ERROR"]
    assert len(errors) == 1
    assert errors[0]["stage"] == ("LLM" if stage == "berry" else "TTS")
    assert messages[-1]["status"] == "FAILED"


def test_asr_failure_is_recoverable_and_creates_no_turn() -> None:
    harness = FakeServiceHarness(["unused"], fail_stage="asr")
    with connected_session(harness) as (websocket, _):
        send_audio(websocket, 0)
        error = websocket.receive_json()
        websocket.send_json({"type": "CLOSE_SESSION", "session_id": "session-1"})

    assert error["type"] == "ERROR"
    assert error["stage"] == "ASR"
    assert error["turn_id"] == 0
