from tests.integration.fake_services import (
    FakeServiceHarness,
    connected_session,
    receive_until,
    send_audio,
)


def test_interrupted_thinker_failure_returns_fallback_text_without_tts() -> None:
    harness = FakeServiceHarness(
        ["first", "second"],
        block_first_thinker=True,
        fail_after_first_thinker_release=True,
    )
    with connected_session(harness) as (websocket, _):
        send_audio(websocket, 0)
        first = receive_until(websocket, "TEXT_DELTA", turn_id=1)
        assert harness.thinker.first_delta.wait(timeout=1)

        send_audio(websocket, 1)
        interrupted = receive_until(websocket, "TURN_STATE", turn_id=1)
        harness.thinker.release_first.set()
        remaining = receive_until(websocket, "RESPONSE_END", turn_id=2)

    messages = [*first, *interrupted, *remaining]
    fallback = next(
        message
        for message in messages
        if message["type"] == "TEXT_END" and message["turn_id"] == 1
    )
    ended = next(
        message
        for message in messages
        if message["type"] == "RESPONSE_END" and message["turn_id"] == 1
    )
    assert fallback["text"] in {"保底一", "保底二", "保底三"}
    assert fallback["interrupt"] is True
    assert ended["status"] == "INTERRUPTED"
    assert not any(request.trace_id.endswith("turn-1") for request in harness.tts.requests)


def test_started_fallback_tts_is_discarded_after_interruption() -> None:
    harness = FakeServiceHarness(
        ["first", "second"],
        block_first_tts=True,
        fail_stage="thinker",
    )
    with connected_session(harness) as (websocket, _):
        send_audio(websocket, 0)
        receive_until(websocket, "AUDIO_DELTA", turn_id=1)
        assert harness.tts.first_started.wait(timeout=1)

        send_audio(websocket, 1)
        messages = receive_until(websocket, "RESPONSE_END", turn_id=2)
        harness.tts.release_first.set()
        receive_until(websocket, "RESPONSE_END", turn_id=1)

    assert any(
        message["type"] == "AUDIO_DELTA" and message["turn_id"] == 2
        for message in messages
    )
    assert not any(
        message["type"] == "AUDIO_DELTA" and message["turn_id"] == 1
        for message in messages
    )
