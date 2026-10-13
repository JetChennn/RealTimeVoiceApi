import asyncio
from threading import Event
from time import monotonic
from types import SimpleNamespace

import pytest

from realtime_voice.clients.asr_keywords import AsrKeywordExtractor


async def test_builtin_dictionary_extracts_proper_nouns_without_user_dictionary():
    extractor = AsrKeywordExtractor()
    try:
        await extractor.start()
        result = await extractor.extract("李白在杭州游览西湖。", deadline=monotonic() + 1)
        assert result.status == "success"
        assert {"李白", "杭州", "西湖"} <= set(result.context.split())
    finally:
        await extractor.aclose()


def test_filters_deduplicates_and_preserves_whole_words_with_budgets():
    extractor = AsrKeywordExtractor(max_keywords=3, max_chars=8)
    words = [
        ("李白", "nr"), ("朋友", "n"), ("李白", "nr"), ("杭", "ns"),
        ("2026", "nz"), ("!！", "nz"), ("非常长的机构名称", "nt"),
        ("杭州", "ns"), ("西湖", "ns"), ("北京", "ns"),
    ]
    extractor._tagger = SimpleNamespace(
        cut=lambda text, HMM: [SimpleNamespace(word=word, flag=tag) for word, tag in words]
    )
    result = extractor.extract_sync("李白朋友杭2026!！非常长的机构名称杭州西湖北京")
    assert result.context == "李白 杭州 西湖"
    assert result.count == 3
    extractor._pool.shutdown()


def test_long_input_is_bounded_to_suffix():
    extractor = AsrKeywordExtractor()
    captured = []
    extractor._tagger = SimpleNamespace(cut=lambda text, HMM: captured.append(text) or [])
    result = extractor.extract_sync("前" * 3000 + "杭州")
    assert len(captured[0]) == 2048
    assert captured[0].endswith("杭州")
    assert result.status == "empty"
    extractor._pool.shutdown()


async def test_timeout_does_not_release_running_worker_capacity(monkeypatch):
    extractor = AsrKeywordExtractor(workers=1, max_waiters=0)
    started, release = Event(), Event()
    original = extractor.extract_sync

    def block(text):
        started.set()
        release.wait(2)
        return original(text)

    await extractor.start()
    monkeypatch.setattr(extractor, "extract_sync", block)
    task = asyncio.create_task(extractor.extract("杭州", deadline=monotonic() + 0.03))
    try:
        async with asyncio.timeout(1):
            while not started.is_set():
                await asyncio.sleep(0)
        assert (await task).status == "timeout"
        assert (await extractor.extract("北京", deadline=monotonic() + 1)).status == "overloaded"
        release.set()
        async with asyncio.timeout(1):
            while extractor._inflight:
                await asyncio.sleep(0)
        assert (await extractor.extract("北京", deadline=monotonic() + 1)).status == "success"
    finally:
        release.set()
        await extractor.aclose()


async def test_cancelled_waiter_keeps_running_work_bounded(monkeypatch):
    extractor = AsrKeywordExtractor(workers=1, max_waiters=0)
    started, release = Event(), Event()
    await extractor.start()

    def block(text):
        started.set()
        release.wait(2)
        raise ValueError("private text must not escape")

    monkeypatch.setattr(extractor, "extract_sync", block)
    task = asyncio.create_task(extractor.extract("杭州", deadline=monotonic() + 1))
    try:
        async with asyncio.timeout(1):
            while not started.is_set():
                await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert (await extractor.extract("北京", deadline=monotonic() + 1)).status == "overloaded"
    finally:
        release.set()
        await extractor.aclose()


async def test_failure_and_expired_deadline_return_empty_context(monkeypatch):
    extractor = AsrKeywordExtractor()
    await extractor.start()

    def fail(text):
        raise ValueError("private reply")

    monkeypatch.setattr(extractor, "extract_sync", fail)
    try:
        assert (await extractor.extract("杭州", deadline=monotonic() + 1)).status == "failed"
        result = await extractor.extract("杭州", deadline=monotonic() - 1)
        assert result.status == "timeout"
        assert result.context == ""
    finally:
        await extractor.aclose()
