from tests.integration.fake_services import (
    FakeServiceHarness,
    connected_session,
    receive_until,
    send_audio,
)


def test_new_turn_interrupts_active_thinker_but_allows_it_to_finish() -> None:
    harness = FakeServiceHarness(["first", "second"], block_first_thinker=True)
    with connected_session(harness) as (websocket, _):
        send_audio(websocket, 0)
        first = receive_until(websocket, "TEXT_DELTA", turn_id=1)
        assert harness.thinker.first_delta.wait(timeout=1)

        send_audio(websocket, 1)
        interrupted = receive_until(websocket, "TURN_STATE", turn_id=1)
        harness.thinker.release_first.set()
        remaining = receive_until(websocket, "RESPONSE_END", turn_id=2)

    messages = [*first, *interrupted, *remaining]
    late_text = [
        message
        for message in messages
        if message["type"] == "TEXT_DELTA" and message["turn_id"] == 1
    ][-1]
    assert late_text["interrupt"] is True
    assert not any(
        message["type"] == "AUDIO_DELTA" and message["turn_id"] == 1
        for message in messages
    )
    assert harness.thinker.calls.index("done:first") < harness.thinker.calls.index("interrupt")
    assert harness.thinker.calls.index("interrupt") < harness.thinker.calls.index("start:second")
