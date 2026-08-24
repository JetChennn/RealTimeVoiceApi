import asyncio
import json

import pytest
from websockets.asyncio.server import ServerConnection, serve

from scripts.load_test import (
    ClientResult,
    measure_client,
    percentile,
    run_load,
    summarize_results,
)


def test_percentile_interpolates_sorted_values() -> None:
    assert percentile([40.0, 10.0, 20.0, 30.0], 50) == 25.0
    assert percentile([], 95) is None


def test_summarize_results_has_required_counts_latencies_and_errors() -> None:
    report = summarize_results(
        [
            ClientResult(True, speech_end_to_asr_ms=10, speech_end_to_text_ms=20),
            ClientResult(True, speech_end_to_asr_ms=30, speech_end_to_audio_ms=50),
            ClientResult(False, "CONNECT_ERROR"),
        ],
        1.23456,
    )

    assert report == {
        "clients": 3,
        "connected": 2,
        "failed": 1,
        "speech_end_to_asr_ms": {"p50": 20.0, "p95": 29.0, "p99": 29.8},
        "speech_end_to_text_ms": {"p50": 20.0, "p95": 20.0, "p99": 20.0},
        "speech_end_to_audio_ms": {"p50": 50.0, "p95": 50.0, "p99": 50.0},
        "errors_by_code": {"CONNECT_ERROR": 1},
        "duration_seconds": 1.235,
    }


@pytest.mark.asyncio
async def test_run_load_starts_clients_concurrently() -> None:
    all_started = asyncio.Event()
    started = 0

    async def runner(_: int) -> ClientResult:
        nonlocal started
        started += 1
        if started == 3:
            all_started.set()
        await asyncio.wait_for(all_started.wait(), timeout=0.5)
        return ClientResult(True)

    results, duration = await run_load(3, runner)

    assert len(results) == 3
    assert all(result.connected for result in results)
    assert duration >= 0


@pytest.mark.asyncio
async def test_three_clients_complete_against_fake_websocket_server() -> None:
    active = 0
    max_active = 0

    async def handler(websocket: ServerConnection) -> None:
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        try:
            await websocket.recv()
            await websocket.send(json.dumps({"type": "SESSION_CREATED"}))
            for _ in range(16):  # one speech chunk plus 600 ms of silence
                await websocket.recv()
            for message in (
                {"type": "ASR_RESULT"},
                {"type": "TEXT_DELTA"},
                {"type": "AUDIO_DELTA"},
                {"type": "RESPONSE_END", "status": "COMPLETED"},
            ):
                await websocket.send(json.dumps(message))
            await websocket.recv()
        finally:
            active -= 1

    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]

        async def runner(index: int) -> ClientResult:
            return await measure_client(
                index,
                url=f"ws://127.0.0.1:{port}",
                pcm16=b"\x01\x00" * 640,
                sample_rate=16000,
                timeout=2,
            )

        results, _ = await run_load(3, runner)

    assert max_active == 3
    assert all(result.error_code is None for result in results)
    assert all(result.speech_end_to_audio_ms is not None for result in results)
