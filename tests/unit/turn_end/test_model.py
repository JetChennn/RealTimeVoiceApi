import asyncio
import hashlib
import json
import threading

import pytest
from pydantic import ValidationError

from realtime_voice.config import Settings
from realtime_voice.observability.metrics import Metrics
from realtime_voice.turn_end import model as turn_end_model
from realtime_voice.turn_end.model import (
    MODEL_REVISION,
    LocalModel,
    SemanticDetector,
    format_context,
    validate_model_assets,
)


def test_normalization_preserves_latest_open_user_turn():
    text = format_context(
        [
            {"role": "assistant", "content": "哪儿？"},
            {"role": "user", "content": " ＢＥＩＪＩＮＧ。"},
            {"role": "user", "content": "明天！"},
        ]
    )
    assert text == "<|im_start|>assistant\n哪儿<|im_end|>\n<|im_start|>user\nbeijing 明天"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"turn_end_candidate_silence_ms": 2100},
        {"turn_end_min_silence_ms": 2100},
        {"turn_end_max_silence_ms": 0},
        {"turn_end_complete_threshold": 2},
        {"turn_end_max_utterance_seconds": 1},
        {"turn_end_cpu_threads": 0},
        {"turn_end_concurrency": 0},
    ],
)
def test_invalid_configuration_is_rejected(kwargs):
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **kwargs)


def test_zero_minimum_silence_is_allowed():
    settings = Settings(_env_file=None, turn_end_min_silence_ms=0)

    assert settings.turn_end_min_silence_ms == 0


def test_unsupported_model_language_has_clear_error(tmp_path, monkeypatch):
    monkeypatch.setattr(turn_end_model, "validate_model_assets", lambda _: None)
    (tmp_path / "manifest.json").write_text(
        f'{{"repository":"livekit/turn-detector","revision":"{MODEL_REVISION}"}}'
    )
    (tmp_path / "languages.json").write_text('{"zh":{"threshold":0.0066}}')

    with pytest.raises(ValueError, match="unsupported turn-end language 'xx'; supported.*zh"):
        LocalModel(tmp_path, "xx", None, 1)


def test_wrong_model_revision_is_rejected(tmp_path):
    (tmp_path / "manifest.json").write_text(
        '{"repository":"livekit/turn-detector","revision":"wrong"}'
    )

    with pytest.raises(ValueError, match="revision does not match"):
        validate_model_assets(tmp_path)


def test_model_asset_hashes_are_checked(tmp_path, monkeypatch):
    payload = b"pinned model"
    expected = hashlib.sha256(payload).hexdigest()
    monkeypatch.setattr(turn_end_model, "MODEL_ASSET_SHA256", {"model.bin": expected})
    (tmp_path / "model.bin").write_bytes(payload)
    (tmp_path / "manifest.json").write_text(
        json.dumps(
            {
                "repository": "livekit/turn-detector",
                "revision": MODEL_REVISION,
                "sha256": {"model.bin": expected},
            }
        )
    )

    validate_model_assets(tmp_path)
    (tmp_path / "model.bin").write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="asset hash mismatch: model.bin"):
        validate_model_assets(tmp_path)


class BlockingModel:
    threshold = 0.5

    def __init__(self):
        self.entered = threading.Event()
        self.release = threading.Event()
        self.calls = 0

    def predict(self, context, text):
        self.calls += 1
        self.entered.set()
        self.release.wait(2)
        return 0.9


async def test_timeout_retains_active_slot_and_does_not_block_event_loop():
    model = BlockingModel()
    metrics = Metrics()
    detector = SemanticDetector(
        Settings(
            _env_file=None,
            turn_end_concurrency=1,
            turn_end_inference_timeout_ms=20,
            turn_end_max_pending_jobs=0,
        ),
        metrics,
        model,
    )
    try:
        result = await detector.predict([], "hello")
        assert result.status == "timeout"
        assert model.entered.is_set()
        assert detector.pending == 1
        assert (await detector.predict([], "new")).status == "overloaded"
        model.release.set()
        for _ in range(100):
            if detector.pending == 0:
                break
            await asyncio.sleep(0.001)
        assert detector.pending == 0
        assert (await detector.predict([], "new")).end
        rendered = metrics.render().decode()
        assert 'stage="semantic_inference"' in rendered
        assert 'stage="semantic_queue"' in rendered
        assert 'stage="semantic"' in rendered
    finally:
        model.release.set()
        await detector.aclose()


async def test_cancelled_queued_request_never_runs():
    model = BlockingModel()
    detector = SemanticDetector(
        Settings(_env_file=None, turn_end_concurrency=1, turn_end_inference_timeout_ms=1000),
        Metrics(),
        model,
    )
    first = asyncio.create_task(detector.predict([], "first"))
    try:
        while not model.entered.is_set():
            await asyncio.sleep(0.001)
        second = asyncio.create_task(detector.predict([], "second"))
        await asyncio.sleep(0.001)
        second.cancel()
        await asyncio.gather(second, return_exceptions=True)
        model.release.set()
        await first
        assert model.calls == 1
    finally:
        model.release.set()
        await detector.aclose()


class ConcurrentModel:
    threshold = 0.5

    def __init__(self):
        self.lock = threading.Lock()
        self.release = threading.Event()
        self.active = 0
        self.max_active = 0

    def predict(self, context, text):
        with self.lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        self.release.wait(2)
        with self.lock:
            self.active -= 1
        return 0.9


async def test_four_predictions_execute_in_parallel():
    model = ConcurrentModel()
    detector = SemanticDetector(
        Settings(_env_file=None, turn_end_concurrency=4, turn_end_inference_timeout_ms=1000),
        Metrics(),
        model,
    )
    tasks = [asyncio.create_task(detector.predict([], str(index))) for index in range(4)]
    try:
        for _ in range(1000):
            if model.max_active == 4:
                break
            await asyncio.sleep(0.001)
        assert model.max_active == 4
        model.release.set()
        assert all(result.end for result in await asyncio.gather(*tasks))
    finally:
        model.release.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        await detector.aclose()
