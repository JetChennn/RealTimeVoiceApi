"""Pinned LiveKit multilingual ONNX inference, with bounded off-event-loop work."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from time import monotonic

from realtime_voice.observability.metrics import Metrics

MODEL_REPOSITORY = "livekit/turn-detector"
MODEL_REVISION = "87e35fcb1e60a569bea70346191c4886ea92e281"  # v0.4.1-intl
MODEL_ASSET_SHA256 = {
    "onnx/model_q8.onnx": "bd2c30776882138a1d95a07faddc13756fe1a35bef6323505f1124fca349bc9c",
    "tokenizer.json": "9c5ae00e602b8860cbd784ba82a8aa14e8feecec692e7076590d014d7b7fdafa",
    "tokenizer_config.json": "83df6c86106c7f332995b3c456bc6a424cc46ee348bf4e0ae1b5410c63f4393f",
    "languages.json": "05a16e11e4282173331dbf140624048e581a727ba45c361bed35e3014ad68187",
    "config.json": "ce173959a04af7e0a3219cf97ee70cc4635f97272ee885e8fc855bde6012c787",
    "LICENSE": "4f5b74db4fd26299a166688de8a3bde046fd7d9cc13ecab64532c24310bc18d7",
}


def asset_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def validate_model_assets(path: Path) -> None:
    """Validate the pinned manifest and every required asset without loading ONNX."""
    manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("repository") != MODEL_REPOSITORY or manifest.get("revision") != MODEL_REVISION:
        raise ValueError("turn-end model revision does not match supported v0.4.1-intl")
    hashes = manifest.get("sha256")
    if not isinstance(hashes, dict):
        raise TypeError("turn-end model manifest does not contain asset hashes")
    for name, expected in MODEL_ASSET_SHA256.items():
        if hashes.get(name) != expected:
            raise ValueError(f"turn-end model manifest has an invalid hash for {name}")
        asset = path / name
        if asset_sha256(asset) != expected:
            raise ValueError(f"turn-end model asset hash mismatch: {name}")


def format_context(messages: list[dict[str, str]]) -> str:
    """Match the pinned model's normalization and Qwen chat format, without EOU suffix."""
    normalized: list[dict[str, str]] = []
    for message in messages[-6:]:
        if message["role"] not in {"user", "assistant"}:
            continue
        content = unicodedata.normalize("NFKC", message["content"].lower())
        content = "".join(
            c for c in content if not unicodedata.category(c).startswith("P") or c in "'-"
        )
        content = re.sub(r"\s+", " ", content).strip()
        if not content:
            continue
        if normalized and normalized[-1]["role"] == message["role"]:
            normalized[-1]["content"] += " " + content
        else:
            normalized.append({"role": message["role"], "content": content})
    text = "".join(f"<|im_start|>{m['role']}\n{m['content']}<|im_end|>\n" for m in normalized)
    return text[: text.rfind("<|im_end|>")] if text else ""


class LocalModel:
    def __init__(self, path: Path, language: str, threshold: float | None, threads: int):
        validate_model_assets(path)

        import onnxruntime as ort
        from tokenizers import Tokenizer

        languages = json.loads((path / "languages.json").read_text(encoding="utf-8"))
        language_code = language.split("-")[0].lower()
        if language_code not in languages:
            supported = ", ".join(sorted(languages))
            raise ValueError(
                f"unsupported turn-end language {language!r}; supported languages: {supported}"
            )
        default = languages[language_code]["threshold"]
        self.threshold = default if threshold is None else threshold
        self.tokenizer = Tokenizer.from_file(str(path / "tokenizer.json"))
        self.tokenizer.enable_truncation(max_length=128, direction="left")
        options = ort.SessionOptions()
        options.intra_op_num_threads = threads
        options.inter_op_num_threads = 1
        self.session = ort.InferenceSession(
            str(path / "onnx/model_q8.onnx"), options, providers=["CPUExecutionProvider"]
        )
        self.predict([], "你好")  # warm-up; do not count startup as a user prediction

    def predict(self, context: list[dict[str, str]], text: str) -> float:
        import numpy as np

        formatted = format_context([*context[-5:], {"role": "user", "content": text}])
        tokens = self.tokenizer.encode(formatted, add_special_tokens=False).ids
        if not tokens:
            return 0.0
        value = float(
            self.session.run(None, {"input_ids": np.array([tokens], dtype=np.int64)})[0].flatten()[
                -1
            ]
        )
        if not math.isfinite(value) or not 0 <= value <= 1:
            raise ValueError("invalid turn-end probability")
        return value


@dataclass(frozen=True)
class Decision:
    end: bool
    probability: float | None
    status: str
    total_seconds: float


class SemanticDetector:
    """Shared model with bounded parallel workers and accurate timed-out slot accounting."""

    def __init__(self, settings, metrics: Metrics, model=None):
        self.settings = settings
        self.metrics = metrics
        self.model = model
        self.executor = ThreadPoolExecutor(
            max_workers=settings.turn_end_concurrency,
            thread_name_prefix="semantic-turn-end",
        )
        self.closed = False
        self.pending = 0

    async def start(self):
        try:
            if self.model is None:
                s = self.settings
                self.model = await asyncio.get_running_loop().run_in_executor(
                    self.executor,
                    LocalModel,
                    Path(s.turn_end_model_path),
                    s.turn_end_language,
                    s.turn_end_complete_threshold,
                    s.turn_end_cpu_threads,
                )
        except BaseException:
            await self.aclose()
            raise

    async def predict(self, context: list[dict[str, str]], text: str) -> Decision:
        start = monotonic()
        status = "failed"
        try:
            if self.closed or self.model is None:
                raise RuntimeError("semantic detector is unavailable")
            # `pending` includes running work. Cancelled queued futures never execute.
            capacity = self.settings.turn_end_concurrency + self.settings.turn_end_max_pending_jobs
            if self.pending >= capacity:
                status = "overloaded"
                return Decision(False, None, status, monotonic() - start)
            self.pending += 1
            self.metrics.semantic_jobs.inc()
            loop = asyncio.get_running_loop()

            def infer():
                self.metrics.observe_stage_latency("semantic_queue", monotonic() - start)
                executing = monotonic()
                try:
                    return self.model.predict(context, text)
                finally:
                    self.metrics.observe_stage_latency(
                        "semantic_inference", monotonic() - executing
                    )

            def release(_):
                try:
                    loop.call_soon_threadsafe(released)
                except RuntimeError:
                    # The detector can finish after its owning event loop has shut down.
                    pass

            def released():
                self.pending -= 1
                self.metrics.semantic_jobs.dec()

            future = self.executor.submit(infer)
            future.add_done_callback(release)
            try:
                probability = await asyncio.wait_for(
                    asyncio.wrap_future(future), self.settings.turn_end_inference_timeout_ms / 1000
                )
            except BaseException:
                future.cancel()  # cancels queued work only; running work retains slot
                raise
            status = "end" if probability >= self.model.threshold else "wait"
            return Decision(status == "end", probability, status, monotonic() - start)
        except TimeoutError:
            status = "timeout"
            return Decision(False, None, status, monotonic() - start)
        except asyncio.CancelledError:
            status = "cancelled"
            raise
        except Exception:  # noqa: BLE001 - semantic failures fall back to silence
            self.metrics.record_error("semantic", "INFERENCE_FAILED")
            return Decision(False, None, status, monotonic() - start)
        finally:
            self.metrics.observe_stage_latency("semantic", monotonic() - start)
            self.metrics.semantic_results.labels(status).inc()

    async def aclose(self):
        self.closed = True
        await asyncio.to_thread(self.executor.shutdown, wait=True, cancel_futures=True)
        self.model = None
