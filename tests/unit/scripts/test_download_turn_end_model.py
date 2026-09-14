import hashlib
import io
import sys

import pytest

from realtime_voice.turn_end import model
from scripts import download_turn_end_model


def configure_fake_asset(monkeypatch, payload: bytes):
    expected = hashlib.sha256(payload).hexdigest()
    assets = {"nested/model.bin": expected}
    monkeypatch.setattr(model, "MODEL_ASSET_SHA256", assets)
    monkeypatch.setattr(download_turn_end_model, "MODEL_ASSET_SHA256", assets)
    return expected


def test_download_then_check_reuses_pinned_asset(tmp_path, monkeypatch):
    payload = b"pinned model payload"
    expected = configure_fake_asset(monkeypatch, payload)
    downloads = 0

    def fake_urlopen(url, timeout):
        nonlocal downloads
        downloads += 1
        assert "livekit/turn-detector" in url
        assert timeout == 60
        return io.BytesIO(payload)

    monkeypatch.setattr(download_turn_end_model, "urlopen", fake_urlopen)
    monkeypatch.setattr(sys, "argv", ["download_turn_end_model.py", "--output", str(tmp_path)])
    download_turn_end_model.main()

    assert downloads == 1
    assert (tmp_path / "nested/model.bin").read_bytes() == payload
    assert model.asset_sha256(tmp_path / "nested/model.bin") == expected

    monkeypatch.setattr(
        sys,
        "argv",
        ["download_turn_end_model.py", "--output", str(tmp_path), "--check"],
    )
    download_turn_end_model.main()
    assert downloads == 1


def test_check_only_fails_without_attempting_download(tmp_path, monkeypatch):
    configure_fake_asset(monkeypatch, b"model")
    monkeypatch.setattr(
        download_turn_end_model,
        "urlopen",
        lambda *args, **kwargs: pytest.fail("check mode must not access the network"),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["download_turn_end_model.py", "--output", str(tmp_path), "--check"],
    )

    with pytest.raises(SystemExit) as error:
        download_turn_end_model.main()
    assert error.value.code == 1
