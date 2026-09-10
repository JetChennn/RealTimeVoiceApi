"""Exercise one WebSocket session with N sequential turns of a single audio file.

The default utterance is resolved relative to the repository layout
(``asr_zh.wav``).  The script opens a single WebSocket connection and sends the
test audio ``--turns`` times, waiting for each completed response before
sending the next utterance, so the timings describe the full
ASR -> Thinker -> TTS path per turn without turn interruptions.  The report
is rendered as a human-readable Markdown document in Chinese.
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
DEFAULT_AUDIO = WORKSPACE_DIR / "asr" / "asr_zh.wav"
SILENCE_MS = 600

# 每个延迟指标的中文名称与说明，顺序即报告中的展示顺序。
METRIC_LABELS: dict[str, tuple[str, str]] = {
    "speech_end_to_asr_ms": (
        "语音结束 → ASR 结果",
        "发送完测试语音（含尾部静音）到收到 ASR 识别结果的耗时，衡量 ASR 链路延迟",
    ),
    "asr_to_first_llm_ms": (
        "ASR 结果 → 首段文本",
        "收到 ASR 结果到收到第一段 LLM 回复文本的耗时，衡量 LLM 首包延迟",
    ),
    "first_llm_to_text_end_ms": (
        "首段文本 → 文本结束",
        "第一段 LLM 文本到文本全部生成完毕的耗时，衡量 LLM 流式生成时长",
    ),
    "text_end_to_first_tts_ms": (
        "文本结束 → 首段音频",
        "文本生成结束到收到第一段 TTS 音频的耗时，衡量 TTS 首包延迟",
    ),
    "first_tts_to_response_end_ms": (
        "首段音频 → 响应结束",
        "第一段 TTS 音频到整轮响应结束的耗时，衡量 TTS 流式合成时长",
    ),
    "speech_end_to_response_end_ms": (
        "语音结束 → 响应结束",
        "整轮端到端耗时，即从语音发送完毕到收到完整回复的总延迟",
    ),
}


@dataclass(frozen=True, slots=True)
class PreparedAudio:
    path: str
    duration_ms: float
    pcm16: bytes


@dataclass(slots=True)
class TurnResult:
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
    turn_index: int,
    audio: PreparedAudio,
    sample_rate: int,
    sequence: int,
    timeout: float,
) -> tuple[TurnResult, int]:
    result = TurnResult(turn_index, audio.path, audio.duration_ms)
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


async def run_session(
    *,
    ws_url: str,
    metrics_url: str,
    audio: PreparedAudio,
    turns: int,
    sample_rate: int,
    timeout: float,
) -> list[TurnResult]:
    session_id = f"chain-latency-{uuid.uuid4().hex}"
    results: list[TurnResult] = []
    async with connect(ws_url, max_size=None, open_timeout=timeout) as websocket:
        await websocket.send(json.dumps(create_session_message(session_id, sample_rate)))
        created = json.loads(await asyncio.wait_for(websocket.recv(), timeout=timeout))
        if created.get("type") != "SESSION_CREATED":
            raise RuntimeError(f"session creation failed: {created}")

        sequence = 0
        for turn_index in range(1, turns + 1):
            before = await asyncio.to_thread(metrics_snapshot, metrics_url)
            result, sequence = await send_turn(
                websocket,
                session_id=session_id,
                turn_index=turn_index,
                audio=audio,
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
    parser.add_argument("--turns", type=int, default=5, help="number of times to send the audio within one connection")
    parser.add_argument("--sample-rate", type=int, default=16000, choices=(16000, 24000, 48000))
    parser.add_argument("--timeout", type=float, default=240.0)
    parser.add_argument("--audio", type=Path, default=DEFAULT_AUDIO, metavar="AUDIO")
    parser.add_argument("--report", type=Path, help="Markdown report path; defaults to reports/chain_latency_<UTC>.md")
    args = parser.parse_args(argv)
    if args.turns < 1:
        parser.error("--turns must be positive")
    return args


async def async_main(args: argparse.Namespace) -> dict[str, object]:
    audio = decode_as_pcm16(args.audio, args.sample_rate)
    started_at = time.monotonic()
    print(f"开始测试：单连接内发送 {args.turns} 轮音频 {audio.path}...")
    turns = await run_session(
        ws_url=args.ws_url,
        metrics_url=args.metrics_url,
        audio=audio,
        turns=args.turns,
        sample_rate=args.sample_rate,
        timeout=args.timeout,
    )

    return {
        "started_at_utc": datetime.now(UTC).isoformat(),
        "duration_seconds": round(time.monotonic() - started_at, 3),
        "configuration": {
            "ws_url": args.ws_url,
            "metrics_url": args.metrics_url,
            "turns": args.turns,
            "sample_rate": args.sample_rate,
            "audio": {"path": audio.path, "duration_ms": audio.duration_ms},
        },
        "summary": {
            "total_turns": len(turns),
            "completed_turns": sum(turn.response_status == "COMPLETED" for turn in turns),
            "failed_turns": sum(turn.error_code is not None for turn in turns),
            "latency": {field: _summary(turns, field) for field in METRIC_LABELS},
        },
        "turns": [asdict(turn) for turn in turns],
    }


def render_report(report: dict[str, object]) -> str:
    """Render the collected measurements as a human-readable Markdown document."""
    config = report["configuration"]
    summary = report["summary"]
    audio = config["audio"]
    lines: list[str] = [
        "# RealTimeVoiceApi 链路延迟测试报告",
        "",
        "## 测试配置",
        "",
        f"- 生成时间（UTC）：{report['started_at_utc']}",
        f"- 测试总耗时：{report['duration_seconds']} s",
        f"- WebSocket 地址：{config['ws_url']}",
        f"- 指标地址：{config['metrics_url']}",
        f"- 测试轮数：{config['turns']}（单连接内顺序发送）",
        f"- 采样率：{config['sample_rate']} Hz",
        f"- 测试音频：{audio['path']}（时长 {audio['duration_ms']} ms）",
        "",
        "## 结果概览",
        "",
        f"- 总轮数：{summary['total_turns']}",
        f"- 成功轮数：{summary['completed_turns']}（RESPONSE_END 状态为 COMPLETED）",
        f"- 失败轮数：{summary['failed_turns']}（收到 ERROR 消息）",
        "",
        "## 各阶段延迟统计（单位：毫秒）",
        "",
        "| 指标 | 样本数 | 平均值 | P50 | P95 |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]

    for field, (label, _) in METRIC_LABELS.items():
        stats = summary["latency"].get(field)
        if stats is None:
            lines.append(f"| {label} | 0 | - | - | - |")
        else:
            lines.append(
                f"| {label} | {stats['count']} | {stats['mean_ms']} | {stats['p50_ms']} | {stats['p95_ms']} |"
            )

    lines += ["", "## 指标说明", ""]
    lines += [f"- **{label}**：{description}" for label, description in METRIC_LABELS.values()]

    lines += ["", "## 每轮明细（单位：毫秒）"]
    for turn in report["turns"]:
        status = turn["response_status"] or turn["error_code"] or "未完成"
        header = f"### 第 {turn['turn_index']} 轮"
        if turn["turn_id"] is not None:
            header += f"（turn_id={turn['turn_id']}）"
        lines += ["", header, "", f"- 状态：{status}"]
        if turn["asr_text"]:
            lines.append(f"- ASR 识别文本：{turn['asr_text']}")
        if turn["reply_text"]:
            reply = turn["reply_text"]
            if len(reply) > 60:
                reply = reply[:60] + "…"
            lines.append(f"- 回复文本：{reply}")
        if turn["error_code"]:
            lines.append(f"- 错误：{turn['error_code']}（阶段：{turn['error_stage']}）")

        lines += ["", "| 阶段 | 延迟 |", "| --- | ---: |"]
        lines += [
            f"| {label} | {turn[field] if turn[field] is not None else '-'} |"
            for field, (label, _) in METRIC_LABELS.items()
        ]

        delta = turn["gateway_metrics_delta"] or {}
        if delta:
            lines += ["", "网关指标增量："]
            lines += [
                f"- {key}：{stats['count']} 次，总计 {stats['sum_ms']} ms，平均 {stats['mean_ms']} ms"
                for key, stats in sorted(delta.items())
            ]

    lines.append("")
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    report = asyncio.run(async_main(args))
    rendered = render_report(report)
    report_path = args.report
    if report_path is None:
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        report_path = PROJECT_DIR / "reports" / f"chain_latency_{stamp}.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    print(f"\n详细报告已保存：{report_path}")


if __name__ == "__main__":
    main()
