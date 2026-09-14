"""Shared pytest configuration for RealTimeVoiceAPI tests."""

import pytest


@pytest.fixture(autouse=True)
def disable_real_semantic_model_by_default(monkeypatch):
    # Existing tests exercise the original transport/turn pipeline. Semantic tests opt in.
    monkeypatch.setenv("RTVA_TURN_END_SEMANTIC_ENABLED", "false")
