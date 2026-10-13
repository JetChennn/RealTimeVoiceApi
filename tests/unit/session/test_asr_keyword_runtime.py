import asyncio
import json
import logging
from time import monotonic

import pytest

from realtime_voice.clients.asr_keywords import KeywordResult
from realtime_voice.observability.metrics import Metrics
from realtime_voice.session.actor import ExtractAsrKeywords
from realtime_voice.session.events import AsrKeywordsExtracted, ThinkerCompleted
from realtime_voice.session.state import TurnContext, TurnStage
from tests.helpers import valid_wav
from tests.unit.session.test_runtime import make_runtime


@pytest.mark.parametrize("bistream", [False, True])
async def test_completed_reply_schedules_extraction_without_waiting_for_it(bistream):
    started, release = asyncio.Event(), asyncio.Event()

    class Extractor:
        async def extract(self, text, *, deadline):
            started.set()
            await release.wait()
            return KeywordResult("success", "李白 杭州", 2)

    runtime, _ = make_runtime(asr_keyword_extractor=Extractor())
    runtime.actor._bistream_enabled = bistream
    runtime.actor.state.active_llm_turn_id = 1
    runtime.actor.state.turns[1] = TurnContext(
        1, "用户输入", valid_wav(), stage=TurnStage.STREAMING_LLM, thinker_generation=1,
    )
    # Run only the keyword effect, leaving real TTS to existing runtime tests.
    effects = runtime.actor.handle(ThinkerCompleted("s", 1, 1, "李白在杭州游览。", "平和"))
    effect = next(item for item in effects if isinstance(item, ExtractAsrKeywords))
    await runtime.execute_effect(effect)
    try:
        await asyncio.wait_for(started.wait(), 1)
        assert runtime._asr_context() == ("unavailable", "", None)
        release.set()
        event = await asyncio.wait_for(runtime.events.get(), 1)
        assert isinstance(event, AsrKeywordsExtracted)
        runtime.actor.handle(event)
        assert runtime._asr_context() == ("provided", "李白 杭州", 1)
    finally:
        release.set()
        await asyncio.gather(*runtime._keyword_tasks)


@pytest.mark.parametrize("status", ["pending", "empty", "timeout", "failed", "overloaded"])
def test_latest_eligible_reply_without_keywords_does_not_reuse_older_keywords(status):
    runtime, _ = make_runtime()
    runtime.actor.state.turns[1] = TurnContext(
        1, "旧输入", valid_wav(), stage=TurnStage.COMPLETED, reply_text="北京",
        asr_keywords="北京", asr_keywords_status="success",
    )
    runtime.actor.state.turns[2] = TurnContext(
        2, "新输入", valid_wav(), stage=TurnStage.STREAMING_TTS, reply_text="好的。",
        asr_keywords_status=status,
    )
    assert runtime._asr_context() == ("unavailable", "", None)


@pytest.mark.parametrize("reason", ["interrupted", "generation", "session", "closing", "fallback"])
def test_late_extraction_cannot_pollute_session_cache(reason):
    runtime, _ = make_runtime()
    turn = TurnContext(
        1, "输入", valid_wav(), stage=TurnStage.COMPLETED, thinker_generation=1,
        reply_text="杭州", asr_keywords_status="pending",
    )
    runtime.actor.state.turns[1] = turn
    if reason == "interrupted":
        turn.interrupted = True
    elif reason == "generation":
        turn.thinker_generation = 2
    elif reason == "closing":
        runtime.actor.state.closing = True
    elif reason == "fallback":
        turn.thinker_fallback_used = True
    event = AsrKeywordsExtracted(
        "other" if reason == "session" else "s", 1, 1, "杭州", "success", monotonic() + 1,
    )
    runtime.actor.handle(event)
    assert turn.asr_keywords == ""


async def test_expired_result_in_actor_queue_is_discarded():
    runtime, _ = make_runtime()
    runtime.actor.state.turns[1] = TurnContext(
        1, "输入", valid_wav(), stage=TurnStage.COMPLETED, thinker_generation=1,
        reply_text="杭州", asr_keywords_status="pending",
    )
    await runtime._handle_actor_event(
        AsrKeywordsExtracted("s", 1, 1, "杭州", "success", monotonic() - 1)
    )
    assert runtime.actor.state.turns[1].asr_keywords_status == "timeout"
    assert runtime._asr_context() == ("unavailable", "", None)


async def test_keyword_observability_contains_metadata_only(caplog):
    class Extractor:
        async def extract(self, text, *, deadline):
            return KeywordResult("success", "李白 杭州", 2, 0.001)

    metrics = Metrics()
    logger = logging.getLogger("test.asr.keywords.privacy")
    runtime, _ = make_runtime(asr_keyword_extractor=Extractor(), metrics=metrics, logger=logger)
    with caplog.at_level(logging.INFO, logger=logger.name):
        await runtime._extract_asr_keywords(
            ExtractAsrKeywords(1, 1, "私密回复李白杭州"), monotonic() + 0.100
        )
    rendered = metrics.render().decode()
    assert 'realtime_voice_asr_keyword_extractions_total{status="success"} 1.0' in rendered
    assert "realtime_voice_asr_keyword_compute_seconds_sum 0.001" in rendered
    assert "realtime_voice_asr_keyword_count_sum 2.0" in rendered
    payload = json.loads(caplog.records[-1].message)
    assert payload["keyword_count"] == 2
    assert payload["context_chars"] == 5
    assert payload["status"] == "success"
    assert "李白" not in caplog.text + rendered
    assert "私密回复" not in caplog.text + rendered


async def test_disconnect_cancels_keyword_waiter_and_never_publishes_cache_result():
    started, cancelled = asyncio.Event(), asyncio.Event()

    class Extractor:
        async def extract(self, text, *, deadline):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

    runtime, _ = make_runtime(asr_keyword_extractor=Extractor())
    await runtime.execute_effect(ExtractAsrKeywords(1, 1, "杭州"))
    await asyncio.wait_for(started.wait(), 1)
    await asyncio.wait_for(runtime._finish_cleanup(), 1)
    assert cancelled.is_set()
    assert not runtime._keyword_tasks
    assert runtime.events.empty()
