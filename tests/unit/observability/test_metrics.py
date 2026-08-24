from prometheus_client import CollectorRegistry

from realtime_voice.observability.metrics import Metrics
from realtime_voice.protocol.server_messages import TurnState
from realtime_voice.session.actor import RecordDiscardedAudio, SendOutbound
from tests.unit.session.test_runtime import make_runtime


def test_metrics_expose_activity_latency_and_discarded_tts_audio():
    metrics = Metrics(registry=CollectorRegistry())

    metrics.set_active_sessions(3)
    metrics.observe_speech_end_to_first_tts(0.25)
    metrics.record_discarded_tts_chunk(byte_count=24)

    rendered = metrics.render().decode()
    assert "realtime_voice_active_sessions 3.0" in rendered
    assert "realtime_voice_speech_end_to_first_tts_seconds_bucket" in rendered
    assert "realtime_voice_discarded_tts_chunks_total 1.0" in rendered
    assert "realtime_voice_discarded_tts_bytes_total 24.0" in rendered


def test_metrics_export_required_runtime_state_families():
    metrics = Metrics(registry=CollectorRegistry())

    metrics.record_event_loop_lag(0.01)
    metrics.record_admission_wait("asr", 0.02)
    metrics.record_admission_overload("asr")
    metrics.record_slow_client_close()
    metrics.set_executor_state(active=2, pending=3)
    metrics.set_process_state(threads=4, memory_bytes=5)

    rendered = metrics.render().decode()
    for family in (
        "event_loop_lag", "admission_wait", "admission_overload", "slow_client_close",
        "executor_active", "executor_pending", "process_memory_bytes",
    ):
        assert f"realtime_voice_{family}" in rendered


async def test_runtime_records_interruption_and_discard_at_effect_boundary():
    metrics = Metrics(registry=CollectorRegistry())
    runtime, _ = make_runtime(metrics=metrics)

    await runtime.execute_effect(RecordDiscardedAudio(turn_id=1, generation=1, byte_count=12))
    await runtime.execute_effect(
        SendOutbound(
            TurnState(
                type="TURN_STATE",
                user_id="u",
                session_id="s",
                turn_id=1,
                interrupt=True,
                state="INTERRUPTED",
            )
        )
    )

    rendered = metrics.render().decode()
    assert "realtime_voice_discarded_tts_bytes_total 12.0" in rendered
    assert "realtime_voice_turn_interruptions_total 1.0" in rendered
