import time

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


def test_second_turn_asr_receives_previous_thinker_reply_as_context() -> None:
    harness = FakeServiceHarness(["李白在杭州游览西湖。", "second"])
    with connected_session(harness) as (websocket, _):
        send_audio(websocket, 0)
        receive_until(websocket, "RESPONSE_END")
        deadline = time.monotonic() + 1
        while harness.runtime._asr_context()[0] != "provided" and time.monotonic() < deadline:
            time.sleep(0.001)
        assert harness.runtime._asr_context()[0] == "provided"
        send_audio(websocket, 1)
        receive_until(websocket, "RESPONSE_END")

    assert harness.asr.contexts[0] == ""
    assert {"李白", "杭州", "西湖"} <= set(harness.asr.contexts[1].split())
    assert "reply:" not in harness.asr.contexts[1]


def test_websocket_rag_configuration_reaches_thinker_request() -> None:
    harness = FakeServiceHarness(["农业社会如何生产？"])
    with connected_session(
        harness,
        rag_enabled=True,
        scenes=["农业社会", "农业社会", "渔猎社会"],
    ) as (websocket, _):
        send_audio(websocket, 0)
        receive_until(websocket, "RESPONSE_END")

    request = harness.thinker.requests[0]
    assert request.rag_enabled is True
    assert request.rag_scenes == ("农业社会", "渔猎社会")


def test_websocket_empty_rag_scenes_reaches_thinker_as_general_knowledge() -> None:
    harness = FakeServiceHarness(["开放问题"])
    with connected_session(harness, rag_enabled=True) as (websocket, _):
        send_audio(websocket, 0)
        receive_until(websocket, "RESPONSE_END")

    request = harness.thinker.requests[0]
    assert request.rag_enabled is True
    assert request.rag_scenes == ()


def test_thinker_failure_uses_fallback_text_and_audio() -> None:
    harness = FakeServiceHarness(["hello"], fail_stage="thinker")
    with connected_session(harness) as (websocket, _):
        send_audio(websocket, 0)
        messages = receive_until(websocket, "RESPONSE_END")

    assert [message["type"] for message in messages] == [
        "ASR_RESULT",
        "TEXT_END",
        "AUDIO_DELTA",
        "RESPONSE_END",
    ]
    assert messages[1]["text"] in {"保底一", "保底二", "保底三"}
    assert harness.tts.requests[0].model_reply == messages[1]["text"]
    assert harness.tts.requests[0].prompt == "平和"
    assert messages[-1]["status"] == "COMPLETED"


def test_tts_failure_emits_error_and_failed_response() -> None:
    harness = FakeServiceHarness(["hello"], fail_stage="tts")
    with connected_session(harness) as (websocket, _):
        send_audio(websocket, 0)
        messages = receive_until(websocket, "RESPONSE_END")

    errors = [message for message in messages if message["type"] == "ERROR"]
    assert len(errors) == 1
    assert errors[0]["stage"] == "TTS"
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
