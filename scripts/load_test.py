"""Run concurrent one-turn WebSocket clients and emit a latency report."""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import time
import uuid
from collections import Counter
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

from websockets.asyncio.client import connect

if __package__:
    from scripts.realtime_client import create_session_message, iter_pcm_chunks, read_pcm16_wav
else:
    from realtime_client import create_session_message, iter_pcm_chunks, read_pcm16_wav


@dataclass(frozen=True, slots=True)
class ClientResult:
    connected: bool
    error_code: str | None = None
    speech_end_to_asr_ms: float | None = None
    speech_end_to_text_ms: float | None = None
    speech_end_to_audio_ms: float | None = None


def percentile(values: Sequence[float], percent: float) -> float | None:
    if not values:
        return None
    if not 0 <= percent <= 100:
        raise ValueError("percent must be between 0 and 100")
    ordered = sorted(values)
    position = (len(ordered) - 1) * percent / 100
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return round(ordered[lower] * (1 - weight) + ordered[upper] * weight, 3)


def _latency_summary(results: Sequence[ClientResult], field: str) -> dict[str, float] | None:
    values = [float(value) for item in results if (value := getattr(item, field)) is not None]
    if not values:
        return None
    return {
        "p50": percentile(values, 50),
        "p95": percentile(values, 95),
        "p99": percentile(values, 99),
    }


def summarize_results(results: Sequence[ClientResult], duration_seconds: float) -> dict[str, object]:
    errors = Counter(item.error_code for item in results if item.error_code)
    return {
        "clients": len(results),
        "connected": sum(item.connected for item in results),
        "failed": sum(item.error_code is not None for item in results),
        "speech_end_to_asr_ms": _latency_summary(results, "speech_end_to_asr_ms"),
        "speech_end_to_text_ms": _latency_summary(results, "speech_end_to_text_ms"),
        "speech_end_to_audio_ms": _latency_summary(results, "speech_end_to_audio_ms"),
        "errors_by_code": dict(sorted(errors.items())),
        "duration_seconds": round(duration_seconds, 3),
    }


async def run_load(
    clients: int, runner: Callable[[int], Awaitable[ClientResult]]
) -> tuple[list[ClientResult], float]:
    if clients <= 0:
        raise ValueError("clients must be positive")
    started = time.monotonic()
    results = await asyncio.gather(*(runner(index) for index in range(clients)))
    return list(results), time.monotonic() - started


async def measure_client(
    index: int, *, url: str, pcm16: bytes, sample_rate: int, timeout: float
) -> ClientResult:
    session_id = f"load-{index}-{uuid.uuid4().hex}"
    connected = False
    try:
        async with connect(url, max_size=None, open_timeout=timeout) as websocket:
            await websocket.send(json.dumps(create_session_message(session_id, sample_rate)))
            created = json.loads(await asyncio.wait_for(websocket.recv(), timeout=timeout))
            if created.get("type") != "SESSION_CREATED":
                return ClientResult(False, created.get("code", "SESSION_CREATE_FAILED"))
            connected = True

            sequence = 0
            for chunk in iter_pcm_chunks(pcm16, sample_rate):
                await websocket.send(
                    json.dumps(
                        {
                            "type": "AUDIO_CHUNK",
                            "session_id": session_id,
                            "sequence": sequence,
                            "timestamp_ms": sequence * 40,
                            "audio_b64": base64.b64encode(chunk.pcm).decode("ascii"),
                        }
                    )
                )
                sequence += 1
                await asyncio.sleep(0.04)
            speech_ended_at = time.monotonic()

            silence = b"\x00\x00" * (sample_rate * 600 // 1000)
            for chunk in iter_pcm_chunks(silence, sample_rate):
                await websocket.send(
                    json.dumps(
                        {
                            "type": "AUDIO_CHUNK",
                            "session_id": session_id,
                            "sequence": sequence,
                            "timestamp_ms": sequence * 40,
                            "audio_b64": base64.b64encode(chunk.pcm).decode("ascii"),
                        }
                    )
                )
                sequence += 1
                await asyncio.sleep(0.04)

            latencies: dict[str, float] = {}
            error_code: str | None = None
            while True:
                message = json.loads(await asyncio.wait_for(websocket.recv(), timeout=timeout))
                kind = message.get("type")
                elapsed = (time.monotonic() - speech_ended_at) * 1000
                if kind == "ASR_RESULT" and "asr" not in latencies:
                    latencies["asr"] = elapsed
                elif kind in {"TEXT_DELTA", "TEXT_END"} and "text" not in latencies:
                    latencies["text"] = elapsed
                elif kind == "AUDIO_DELTA" and "audio" not in latencies:
                    latencies["audio"] = elapsed
                elif kind == "ERROR":
                    error_code = message.get("code", "SERVER_ERROR")
                    if not message.get("recoverable", False):
                        return ClientResult(
                            connected,
                            error_code,
                            latencies.get("asr"),
                            latencies.get("text"),
                            latencies.get("audio"),
                        )
                elif kind == "RESPONSE_END":
                    status = message.get("status")
                    if status not in {None, "COMPLETED"} and error_code is None:
                        error_code = f"RESPONSE_{status}"
                    await websocket.send(
                        json.dumps({"type": "CLOSE_SESSION", "session_id": session_id})
                    )
                    return ClientResult(
                        connected=True,
                        error_code=error_code,
                        speech_end_to_asr_ms=latencies.get("asr"),
                        speech_end_to_text_ms=latencies.get("text"),
                        speech_end_to_audio_ms=latencies.get("audio"),
                    )
    except TimeoutError:
        return ClientResult(connected, "TIMEOUT")
    except Exception as error:  # noqa: BLE001 - each load client must report independently
        return ClientResult(connected, type(error).__name__.upper())


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--clients", required=True, type=int)
    parser.add_argument("--wav", required=True, type=Path)
    parser.add_argument("--report", default=Path("report.json"), type=Path)
    parser.add_argument("--sample-rate", default=16000, type=int, choices=(16000, 24000, 48000))
    parser.add_argument("--timeout", default=180.0, type=float)
    return parser.parse_args(argv)


async def async_main(args: argparse.Namespace) -> dict[str, object]:
    pcm16 = read_pcm16_wav(args.wav, args.sample_rate)

    async def runner(index: int) -> ClientResult:
        return await measure_client(
            index,
            url=args.url,
            pcm16=pcm16,
            sample_rate=args.sample_rate,
            timeout=args.timeout,
        )

    results, duration = await run_load(args.clients, runner)
    return summarize_results(results, duration)


def main() -> None:
    args = parse_args()
    report = asyncio.run(async_main(args))
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
