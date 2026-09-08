"""Exercise five sequential three-turn WebSocket rounds and report end-to-end latency.

The default utterances are resolved relative to the repository layout:
``asr_zh.wav -> asr_en.wav -> asr_zh.wav``.  Each round opens one WebSocket
session and waits for a completed response before sending the next utterance,
so the timings describe the full ASR -> Thinker -> TTS path without turn
interruptions.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import subprocess
import time
import uuid
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.request import urlopen

from prometheus_client.parser import text_string_to_metric_families
from websockets.asyncio.client import connect

if __package__:
    from scripts.realtime_client import create_session_message, iter_pcm_chunks
else:
    from realtime_client import create_session_message, iter_pcm_chunks


PROJECT_DIR = Path(__file__).resolve().parents[1]
WORKSPACE_DIR = PROJECT_DIR.parent
DEFAULT_AUDIO = (
    WORKSPACE_DIR / "asr" / "asr_zh.wav",
    WORKSPACE_DIR / "asr" / "asr_en.wav",
    WORKSPACE_DIR / "asr" / "asr_zh.wav",
)
SILENCE_MS = 600


@dataclass(frozen=True, slots=True)
class PreparedAudio:
    path: str
    duration_ms: float
    pcm16: bytes


@dataclass(slots=True)
class TurnResult:
    round_index: int
    turn_index: int
    audio_path: str
    audio_duration_ms: float
    turn_id: int | None = None
    asr_text: str | None = None
    reply_text: str | None = None
    response_status: str | None = None
    error_code: str | None = None
    error_stage: str | None = None
    speech_end_to_asr_ms: float | None = None
    asr_to_first_llm_ms: float | None = None
    first_llm_to_text_end_ms: float | None = None
    text_end_to_first_tts_ms: float | None = None
    first_tts_to_response_end_ms: float | None = None
    speech_end_to_response_end_ms: float | None = None
    gateway_metrics_delta: dict[str, dict[str, float]] | None = None


def _milliseconds(started_at: float | None, finished_at: float | None) -> float | None:
    if started_at is None or finished_at is None:
        return None
    return round((finished_at - started_at) * 1000, 3)


def _percentile(values: Sequence[float], percent: int) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * percent / 100
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return round(ordered[lower] * (1 - weight) + ordered[upper] * weight, 3)


def _summary(turns: Iterable[TurnResult], field: str) -> dict[str, float] | None:
    values = [float(value) for turn in turns if (value := getattr(turn, field)) is not None]
    if not values:
        return None
    return {
        "count": len(values),
        "mean_ms": round(sum(values) / len(values), 3),
        "p50_ms": _percentile(values, 50),
        "p95_ms": _percentile(values, 95),
    }


def decode_as_pcm16(path: Path, sample_rate: int) -> PreparedAudio:
    """Decode any ffmpeg-supported source into the protocol's mono PCM16 format."""
    if not path.is_file():
        raise FileNotFoundError(f"audio file does not exist: {path}")
    command = [
        "ffmpeg",
        "-v",
        "error",
        "-i",
        str(path),
        "-f",
        "s16le",
        "-ac",
        "1",
        "-ar",
        str(sample_rate),
        "pipe:1",
    ]
    completed = subprocess.run(command, check=True, capture_output=True)
    pcm16 = completed.stdout
    if not pcm16 or len(pcm16) % 2:
        raise ValueError(f"could not decode complete PCM16 samples from {path}")
    duration_ms = len(pcm16) / 2 / sample_rate * 1000
    return PreparedAudio(str(path), round(duration_ms, 3), pcm16)


def metrics_snapshot(metrics_url: str) -> dict[str, tuple[float, float]]:
    """Return histogram count/sum pairs, keyed by metric and label set."""
    with urlopen(metrics_url, timeout=5) as response:
        payload = response.read().decode("utf-8")

    values: dict[str, dict[str, float]] = {}
    for family in text_string_to_metric_families(payload):
        for sample in family.samples:
            if sample.name.endswith("_count"):
                suffix = "count"
                base = sample.name.removesuffix("_count")
            elif sample.name.endswith("_sum"):
                suffix = "sum"
                base = sample.name.removesuffix("_sum")
            else:
                continue
            labels = ",".join(f"{key}={value}" for key, value in sorted(sample.labels.items()))
            key = f"{base}{{{labels}}}" if labels else base
            values.setdefault(key, {})[suffix] = float(sample.value)

    return {
        key: (sample.get("count", 0.0), sample.get("sum", 0.0))
        for key, sample in values.items()
        if "count" in sample or "sum" in sample
    }


def metrics_delta(
    before: dict[str, tuple[float, float]], after: dict[str, tuple[float, float]]
) -> dict[str, dict[str, float]]:
    """Render per-turn Prometheus histogram increments as milliseconds."""
    result: dict[str, dict[str, float]] = {}
    for key, (after_count, after_sum) in after.items():
        before_count, before_sum = before.get(key, (0.0, 0.0))
        count = after_count - before_count
        total_seconds = after_sum - before_sum
        if count <= 0:
            continue
        result[key] = {
            "count": count,
            "sum_ms": round(total_seconds * 1000, 3),
            "mean_ms": round(total_seconds * 1000 / count, 3),
        }
    return result


async def send_turn(
    websocket: Any,
    *,
    session_id: str,
    round_index: int,
    turn_index: int,
    audio: PreparedAudio,
    sample_rate: int,
    sequence: int,
    timeout: float,
) -> tuple[TurnResult, int]:
    result = TurnResult(round_index, turn_index, audio.path, audio.duration_ms)
    first_speech_sent_at: float | None = None
    speech_ended_at: float | None = None

    for chunk in iter_pcm_chunks(audio.pcm16, sample_rate):
        if first_speech_sent_at is None:
            first_speech_sent_at = time.monotonic()
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

    silence = b"\x00\x00" * (sample_rate * SILENCE_MS // 1000)
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

    asr_at: float | None = None
    first_llm_at: float | None = None
    text_end_at: float | None = None
    first_tts_at: float | None = None
    response_end_at: float | None = None
    text_parts: list[str] = []

    while True:
        raw = await asyncio.wait_for(websocket.recv(), timeout=timeout)
        if not isinstance(raw, str):
            raise TypeError("server sent an unexpected binary WebSocket frame")
        message = json.loads(raw)
        kind = message.get("type")
        now = time.monotonic()

        if kind == "ASR_RESULT":
            result.turn_id = int(message["turn_id"])
            result.asr_text = str(message.get("text", ""))
            asr_at = asr_at or now
        elif kind == "TEXT_DELTA":
            if result.turn_id is not None and message.get("turn_id") == result.turn_id:
                first_llm_at = first_llm_at or now
                text_parts.append(str(message.get("delta", "")))
        elif kind == "TEXT_END":
            if result.turn_id is not None and message.get("turn_id") == result.turn_id:
                first_llm_at = first_llm_at or now
                text_end_at = now
                result.reply_text = str(message.get("text", "")) or "".join(text_parts)
        elif kind == "AUDIO_DELTA":
            if result.turn_id is not None and message.get("turn_id") == result.turn_id:
                first_tts_at = first_tts_at or now
        elif kind == "ERROR":
            result.error_stage = str(message.get("stage", "UNKNOWN"))
            result.error_code = str(message.get("code", "SERVER_ERROR"))
            # ASR failures have no turn or RESPONSE_END, so they terminate this
            # test utterance immediately.
            if result.error_stage == "ASR":
                break
        elif kind == "RESPONSE_END":
            if result.turn_id is None or message.get("turn_id") == result.turn_id:
                result.response_status = str(message.get("status", "UNKNOWN"))
                response_end_at = now
                break

    result.speech_end_to_asr_ms = _milliseconds(speech_ended_at, asr_at)
    result.asr_to_first_llm_ms = _milliseconds(asr_at, first_llm_at)
    result.first_llm_to_text_end_ms = _milliseconds(first_llm_at, text_end_at)
    result.text_end_to_first_tts_ms = _milliseconds(text_end_at, first_tts_at)
    result.first_tts_to_response_end_ms = _milliseconds(first_tts_at, response_end_at)
    result.speech_end_to_response_end_ms = _milliseconds(speech_ended_at, response_end_at)
    return result, sequence


async def run_round(
    *,
    round_index: int,
    ws_url: str,
    metrics_url: str,
    audio: Sequence[PreparedAudio],
    sample_rate: int,
    timeout: float,
) -> list[TurnResult]:
    session_id = f"chain-latency-{round_index}-{uuid.uuid4().hex}"
    results: list[TurnResult] = []
    async with connect(ws_url, max_size=None, open_timeout=timeout) as websocket:
        await websocket.send(json.dumps(create_session_message(session_id, sample_rate)))
        created = json.loads(await asyncio.wait_for(websocket.recv(), timeout=timeout))
        if created.get("type") != "SESSION_CREATED":
            raise RuntimeError(f"session creation failed: {created}")

        sequence = 0
        for turn_index, utterance in enumerate(audio, start=1):
            before = await asyncio.to_thread(metrics_snapshot, metrics_url)
            result, sequence = await send_turn(
                websocket,
                session_id=session_id,
                round_index=round_index,
                turn_index=turn_index,
                audio=utterance,
                sample_rate=sample_rate,
                sequence=sequence,
                timeout=timeout,
            )
            after = await asyncio.to_thread(metrics_snapshot, metrics_url)
            result.gateway_metrics_delta = metrics_delta(before, after)
            results.append(result)

        await websocket.send(json.dumps({"type": "CLOSE_SESSION", "session_id": session_id}))
    return results


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ws-url", default="ws://127.0.0.1:8000/v1/realtime")
    parser.add_argument("--metrics-url", default="http://127.0.0.1:8000/metrics")
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--sample-rate", type=int, default=16000, choices=(16000, 24000, 48000))
    parser.add_argument("--timeout", type=float, default=240.0)
    parser.add_argument("--audio", type=Path, nargs=3, default=DEFAULT_AUDIO, metavar=("AUDIO_1", "AUDIO_2", "AUDIO_3"))
    parser.add_argument("--report", type=Path, help="JSON report path; defaults to reports/chain_latency_<UTC>.json")
    args = parser.parse_args(argv)
    if args.rounds < 1:
        parser.error("--rounds must be positive")
    return args


async def async_main(args: argparse.Namespace) -> dict[str, object]:
    audio = [decode_as_pcm16(path, args.sample_rate) for path in args.audio]
    started_at = time.monotonic()
    rounds: list[list[TurnResult]] = []
    for round_index in range(1, args.rounds + 1):
        print(f"Starting round {round_index}/{args.rounds}...")
        rounds.append(
            await run_round(
                round_index=round_index,
                ws_url=args.ws_url,
                metrics_url=args.metrics_url,
                audio=audio,
                sample_rate=args.sample_rate,
                timeout=args.timeout,
            )
        )

    turns = [turn for round_turns in rounds for turn in round_turns]
    latency_fields = (
        "speech_end_to_asr_ms",
        "asr_to_first_llm_ms",
        "first_llm_to_text_end_ms",
        "text_end_to_first_tts_ms",
        "first_tts_to_response_end_ms",
        "speech_end_to_response_end_ms",
    )
    return {
        "started_at_utc": datetime.now(UTC).isoformat(),
        "duration_seconds": round(time.monotonic() - started_at, 3),
        "configuration": {
            "ws_url": args.ws_url,
            "metrics_url": args.metrics_url,
            "rounds": args.rounds,
            "turns_per_round": len(audio),
            "sample_rate": args.sample_rate,
            "audio": [{"path": item.path, "duration_ms": item.duration_ms} for item in audio],
        },
        "summary": {
            "total_turns": len(turns),
            "completed_turns": sum(turn.response_status == "COMPLETED" for turn in turns),
            "failed_turns": sum(turn.error_code is not None for turn in turns),
            "latency": {field: _summary(turns, field) for field in latency_fields},
        },
        "rounds": [[asdict(turn) for turn in round_turns] for round_turns in rounds],
    }


def main() -> None:
    args = parse_args()
    report = asyncio.run(async_main(args))
    report_path = args.report
    if report_path is None:
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        report_path = PROJECT_DIR / "reports" / f"chain_latency_{stamp}.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(report["summary"], indent=2, ensure_ascii=False))
    print(f"Detailed report: {report_path}")


if __name__ == "__main__":
    main()
