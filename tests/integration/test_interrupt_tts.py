from tests.integration.fake_services import (
    FakeServiceHarness,
    connected_session,
    receive_until,
    send_audio,
)


def test_new_tts_does_not_wait_for_interrupted_tts_cancellation() -> None:
    harness = FakeServiceHarness(["first", "second"], block_first_tts=True)
    with connected_session(harness) as (websocket, _):
        send_audio(websocket, 0)
        receive_until(websocket, "AUDIO_DELTA", turn_id=1)
        assert harness.tts.first_started.wait(timeout=1)

        send_audio(websocket, 1)
        messages = receive_until(websocket, "RESPONSE_END", turn_id=2)
        assert harness.tts.second_started.is_set()
        assert harness.tts.first_stopped.wait(timeout=1)
        if not any(
            message["type"] == "RESPONSE_END" and message["turn_id"] == 1
            for message in messages
        ):
            messages.extend(receive_until(websocket, "RESPONSE_END", turn_id=1))

    assert any(
        message["type"] == "AUDIO_DELTA" and message["turn_id"] == 2
        for message in messages
    )
    turn_one_audio = [
        message
        for message in messages
        if message["type"] == "AUDIO_DELTA" and message["turn_id"] == 1
    ]
    assert turn_one_audio == []
    turn_one_end = [
        message
        for message in messages
        if message["type"] == "RESPONSE_END" and message["turn_id"] == 1
    ]
    assert len(turn_one_end) == 1
    assert turn_one_end[0]["status"] == "INTERRUPTED"
    assert harness.tts.first_stopped.is_set()
