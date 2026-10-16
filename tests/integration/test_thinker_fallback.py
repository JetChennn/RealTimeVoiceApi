from tests.integration.fake_services import (
    FakeServiceHarness,
    connected_session,
    receive_until,
    send_audio,
)


def test_interrupted_thinker_failure_ends_without_fallback_or_tts() -> None:
    harness = FakeServiceHarness(
        ["first", "second"],
        block_first_thinker=True,
        fail_after_first_thinker_release=True,
    )
    with connected_session(harness, enable_turn_metrics=True) as (websocket, _):
        send_audio(websocket, 0)
        first = receive_until(websocket, "TEXT_DELTA", turn_id=1)
        assert harness.thinker.first_delta.wait(timeout=1)

        send_audio(websocket, 1)
        interrupted = receive_until(websocket, "TURN_STATE", turn_id=1)
        harness.thinker.release_first.set()
        remaining = receive_until(websocket, "RESPONSE_END", turn_id=2)

    messages = [*first, *interrupted, *remaining]
    ended = next(
        message
        for message in messages
        if message["type"] == "RESPONSE_END" and message["turn_id"] == 1
    )
    assert not any(m["type"] == "TEXT_END" and m["turn_id"] == 1 for m in messages)
    assert ended["status"] == "INTERRUPTED"
    assert not any(request.trace_id.endswith("turn-1") for request in harness.tts.requests)
    snapshots = [m for m in messages if m["type"] == "TURN_METRICS" and m["turn_id"] == 1]
    assert snapshots[-1]["status"] == "interrupted"
    assert snapshots[-1]["llm"]["ms"] >= 0  # First text preceded the interruption.
    assert "ms" not in snapshots[-1]["total"]
    assert snapshots[-1]["fallback"] is False


def test_started_fallback_tts_is_discarded_after_interruption() -> None:
    harness = FakeServiceHarness(
        ["first", "second"],
        block_first_tts=True,
        fail_stage="thinker",
    )
    with connected_session(harness, enable_turn_metrics=True) as (websocket, _):
        send_audio(websocket, 0)
        receive_until(websocket, "AUDIO_DELTA", turn_id=1)
        assert harness.tts.first_started.wait(timeout=1)

        send_audio(websocket, 1)
        messages = receive_until(websocket, "RESPONSE_END", turn_id=2)
        harness.tts.release_first.set()
        # Interrupted TTS closes immediately; its end is already in messages.
        assert any(
            m["type"] == "RESPONSE_END" and m["turn_id"] == 1
            and m["status"] == "INTERRUPTED" for m in messages
        )

    assert any(
        message["type"] == "AUDIO_DELTA" and message["turn_id"] == 2
        for message in messages
    )
    assert not any(
        message["type"] == "AUDIO_DELTA" and message["turn_id"] == 1
        for message in messages
    )
    snapshots = [m for m in messages if m["type"] == "TURN_METRICS" and m["turn_id"] == 1]
    assert snapshots[-1]["status"] == "interrupted"
    assert snapshots[-1]["fallback"] is True
    assert "ms" not in snapshots[-1]["llm"]
    assert snapshots[-1]["tts"]["ms"] >= 0
    assert snapshots[-1]["total"]["ms"] >= 0
