import asyncio
import json
import logging
import multiprocessing
import os
import signal
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from threading import Event
from time import monotonic
from time import sleep as blocking_sleep
from types import SimpleNamespace

import pytest

from realtime_voice.clients import asr_keywords
from realtime_voice.clients.asr_keywords import (
    AsrKeywordExtractor,
    KeywordResult,
    _extract_keywords,
)


def cpu_probe():
    """Synchronize two actual subprocesses, then exercise Python bytecode in both."""
    asr_keywords._worker_startup_barrier.wait(timeout=5)
    started = monotonic()
    iterations = 0
    while monotonic() - started < 0.05:
        iterations += 1
    return os.getpid(), started, monotonic(), iterations


def slow_process_result(text, deadline):
    blocking_sleep(0.2)
    return KeywordResult("success", text, 1)


def stuck_worker_initializer(started, ignore_terminate):
    if ignore_terminate:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    started.set()
    Event().wait()  # No timeout: cleanup must terminate this actual subprocess.


def coordinated_stuck_initializer(ready, count):
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    with count.get_lock():
        count.value += 1
        if count.value == 2:
            ready.set()  # Both workers now ignore SIGTERM.
    Event().wait()


@pytest.mark.parametrize("attempt", range(3))
async def test_crashed_worker_and_sigterm_ignoring_peer_do_not_deadlock_cleanup(
    monkeypatch, attempt,
):
    real_pool = asr_keywords.ProcessPoolExecutor
    context = multiprocessing.get_context("spawn")
    ready, count = context.Event(), context.Value("i", 0)
    captured = []

    def broken_pool(**kwargs):
        kwargs["initializer"] = coordinated_stuck_initializer
        kwargs["initargs"] = (ready, count)
        pool = real_pool(**kwargs)
        captured.append(pool)
        return pool

    monkeypatch.setattr(asr_keywords, "ProcessPoolExecutor", broken_pool)
    monkeypatch.setattr(asr_keywords, "_STARTUP_TIMEOUT_SECONDS", 5.0)
    monkeypatch.setattr(asr_keywords, "_TERMINATE_GRACE_SECONDS", 0.1)
    monkeypatch.setattr(asr_keywords, "_KILL_GRACE_SECONDS", 0.5)
    monkeypatch.setattr(asr_keywords, "_MANAGER_GRACE_SECONDS", 0.5)
    extractor = AsrKeywordExtractor()
    task = asyncio.create_task(extractor.start())
    processes = []
    try:
        async with asyncio.timeout(3):
            while not ready.is_set():
                await asyncio.sleep(0.001)
        processes = list(captured[0]._processes.values())
        manager = captured[0]._executor_manager_thread
        begin = monotonic()
        # The first spawned worker is monitored before the manager starts.
        # A newly spawned peer's sentinel can temporarily be absent from its
        # wait set, so crash the known-monitored worker, not whichever initialized
        # first. This tests cleanup's lock race, not CPython's watch-set refresh.
        processes[0].kill()
        with pytest.raises(BrokenProcessPool):
            await asyncio.wait_for(task, timeout=3)
        assert monotonic() - begin < 3
        assert not any(asr_keywords._worker_alive(process) for process in processes)
        assert not manager.is_alive()
        assert extractor._closed and extractor._pool is None
        await asyncio.wait_for(extractor.aclose(), timeout=0.1)
    finally:
        for process in processes:
            if asr_keywords._worker_alive(process):
                process.kill()
                process.join(timeout=1)
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await extractor.aclose()


def test_residual_manager_timeout_never_calls_lock_blocking_shutdown(caplog, monkeypatch):
    class BlockedManager:
        def join(self, *, timeout):
            assert timeout == asr_keywords._MANAGER_GRACE_SECONDS

        def is_alive(self):
            return True

    class Pool:
        def __init__(self):
            self._processes = {}
            self._executor_manager_thread = BlockedManager()

        def shutdown(self, **kwargs):
            raise AssertionError("shutdown would block on the manager's lock")

    monkeypatch.setattr(logging.getLogger("realtime_voice"), "propagate", True)
    with caplog.at_level(logging.WARNING, logger="realtime_voice"):
        asr_keywords._abort_pool_startup(Pool())
    payload = json.loads(caplog.records[-1].message)
    assert payload["event"] == "asr_keyword_cleanup_incomplete"
    assert payload["reason"] == "manager_cleanup_timeout"


@pytest.mark.parametrize("cancel_startup", [False, True])
@pytest.mark.parametrize("ignore_terminate", [False, True])
async def test_stuck_initializer_is_reaped_after_startup_timeout_or_cancel(
    monkeypatch, cancel_startup, ignore_terminate,
):
    real_pool = asr_keywords.ProcessPoolExecutor
    started = multiprocessing.get_context("spawn").Event()
    captured = []

    def stuck_pool(**kwargs):
        kwargs["initializer"] = stuck_worker_initializer
        kwargs["initargs"] = (started, ignore_terminate)
        pool = real_pool(**kwargs)
        captured.append(pool)
        return pool

    monkeypatch.setattr(asr_keywords, "ProcessPoolExecutor", stuck_pool)
    monkeypatch.setattr(asr_keywords, "_STARTUP_TIMEOUT_SECONDS", 1.0)
    monkeypatch.setattr(asr_keywords, "_TERMINATE_GRACE_SECONDS", 0.1)
    monkeypatch.setattr(asr_keywords, "_KILL_GRACE_SECONDS", 0.5)
    monkeypatch.setattr(asr_keywords, "_MANAGER_GRACE_SECONDS", 0.5)
    extractor = AsrKeywordExtractor()
    begin = monotonic()
    task = asyncio.create_task(extractor.start())
    processes = []
    manager = None
    try:
        async with asyncio.timeout(3):
            while not started.is_set():
                await asyncio.sleep(0.001)
        processes = list(captured[0]._processes.values())
        manager = captured[0]._executor_manager_thread
        if cancel_startup:
            task.cancel()
        with pytest.raises(asyncio.CancelledError if cancel_startup else TimeoutError):
            await asyncio.wait_for(task, timeout=3)
        assert monotonic() - begin < 3
        assert not any(asr_keywords._worker_alive(process) for process in processes)
        assert not manager.is_alive()
        assert extractor._pool is None
        assert extractor._closed and not extractor._ready
        # The application also calls aclose after a startup exception.
        await asyncio.wait_for(extractor.aclose(), timeout=0.1)
    finally:
        # Keep the regression test safe even against the old, hanging code.
        for process in processes:
            if asr_keywords._worker_alive(process):
                process.kill()
                process.join(timeout=1)
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await extractor.aclose()


def controlled_extractor(monkeypatch, operation, *, max_waiters=0):
    # Use a deterministic executor double for admission/cancellation tests only.
    # Real spawned workers are exercised separately; closures cannot be pickled.
    extractor = AsrKeywordExtractor(workers=1, max_waiters=max_waiters)
    extractor._pool = ThreadPoolExecutor(max_workers=1)
    extractor._ready = True
    monkeypatch.setattr(asr_keywords, "_extract_before_deadline", operation)
    return extractor


async def test_builtin_dictionary_extracts_proper_nouns_without_user_dictionary(monkeypatch):
    def forbid_gateway_computation(*args):
        raise AssertionError("keyword computation ran in the gateway process")

    monkeypatch.setattr(asr_keywords, "_extract_keywords", forbid_gateway_computation)
    extractor = AsrKeywordExtractor(max_keywords=3)
    try:
        await extractor.start()
        result = await extractor.extract("李白在杭州游览西湖。", deadline=monotonic() + 1)
        assert result.status == "success"
        assert {"李白", "杭州", "西湖"} <= set(result.context.split())
        assert result.count == 3
    finally:
        await extractor.aclose()


async def test_all_workers_are_prewarmed_reused_and_run_python_outside_gateway():
    extractor = AsrKeywordExtractor(workers=2)
    try:
        await extractor.start()
        pids = extractor._worker_pids
        assert len(set(pids)) == 2
        assert os.getpid() not in pids
        assert extractor._pool._mp_context.get_start_method() == "spawn"
        await extractor.start()
        assert extractor._worker_pids == pids
        probes = await asyncio.gather(*(
            asyncio.wrap_future(extractor._pool.submit(cpu_probe)) for _ in range(2)
        ))
        assert {probe[0] for probe in probes} == set(pids)
        assert all(probe[3] > 0 for probe in probes)
        assert max(probe[1] for probe in probes) < min(probe[2] for probe in probes)
        results = await asyncio.gather(*(
            extractor.extract("李白在杭州游览西湖。", deadline=monotonic() + 2)
            for _ in range(30)
        ))
        assert all(result.status == "success" for result in results)
        assert extractor._worker_pids == pids
    finally:
        await extractor.aclose()
    assert not set(pids) & {child.pid for child in multiprocessing.active_children()}
    assert (await extractor.extract("杭州", deadline=monotonic() + 1)).status == "failed"


async def test_real_process_timeout_keeps_capacity_until_computation_finishes(monkeypatch):
    extractor = AsrKeywordExtractor(workers=1, max_waiters=0)
    try:
        await extractor.start()
        with monkeypatch.context() as patch:
            patch.setattr(asr_keywords, "_extract_before_deadline", slow_process_result)
            result = await extractor.extract("杭州", deadline=monotonic() + 0.03)
        assert result == KeywordResult("timeout")
        assert extractor._inflight == 1
        assert (await extractor.extract("北京", deadline=monotonic() + 1)).status == "overloaded"
        async with asyncio.timeout(1):
            while extractor._inflight:
                await asyncio.sleep(0.001)
        assert (await extractor.extract("北京", deadline=monotonic() + 1)).status == "success"
    finally:
        await extractor.aclose()


def test_filters_deduplicates_and_preserves_whole_words_with_budgets():
    words = [
        ("李白", "nr"), ("朋友", "n"), ("李白", "nr"), ("杭", "ns"),
        ("2026", "nz"), ("!！", "nz"), ("非常长的机构名称", "nt"),
        ("杭州", "ns"), ("西湖", "ns"), ("北京", "ns"),
    ]
    tagger = SimpleNamespace(
        cut=lambda text, HMM: [SimpleNamespace(word=word, flag=tag) for word, tag in words]
    )
    result = _extract_keywords("李白朋友杭2026!！非常长的机构名称杭州西湖北京", tagger, 3, 8)
    assert result.context == "李白 杭州 西湖"
    assert result.count == 3


def test_long_input_is_bounded_to_suffix():
    captured = []
    tagger = SimpleNamespace(cut=lambda text, HMM: captured.append(text) or [])
    result = _extract_keywords("前" * 3000 + "杭州", tagger, 12, 128)
    assert len(captured[0]) == 2048
    assert captured[0].endswith("杭州")
    assert result.status == "empty"


async def test_timeout_does_not_release_running_worker_capacity(monkeypatch):
    started, release = Event(), Event()

    def block(text, deadline):
        started.set()
        release.wait(2)
        return KeywordResult("success", text, 1)

    extractor = controlled_extractor(monkeypatch, block)
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
    started, release = Event(), Event()

    def block(text, deadline):
        started.set()
        release.wait(2)
        raise ValueError("private text must not escape")

    extractor = controlled_extractor(monkeypatch, block)
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
    def fail(text, deadline):
        raise ValueError("private reply")

    extractor = controlled_extractor(monkeypatch, fail)
    try:
        assert (await extractor.extract("杭州", deadline=monotonic() + 1)).status == "failed"
        result = await extractor.extract("杭州", deadline=monotonic() - 1)
        assert result.status == "timeout"
        assert result.context == ""
    finally:
        await extractor.aclose()


@pytest.mark.parametrize("cancel", [False, True])
async def test_waiting_job_never_submits_after_timeout_or_cancellation(monkeypatch, cancel):
    started, release = Event(), Event()
    calls = []

    def block(text, deadline):
        calls.append(text)
        started.set()
        release.wait(2)
        return KeywordResult("success", text, 1)

    extractor = controlled_extractor(monkeypatch, block, max_waiters=1)
    running = asyncio.create_task(extractor.extract("杭州", deadline=monotonic() + 1))
    try:
        async with asyncio.timeout(1):
            while not started.is_set():
                await asyncio.sleep(0)
        waiting = asyncio.create_task(extractor.extract("北京", deadline=monotonic() + 0.03))
        await asyncio.sleep(0)
        assert extractor._inflight == 2
        if cancel:
            waiting.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiting
        else:
            assert (await waiting).status == "timeout"
        assert extractor._inflight == 1
        assert calls == ["杭州"]
        release.set()
        assert (await running).status == "success"
    finally:
        release.set()
        await extractor.aclose()


async def test_submission_failure_returns_empty_and_releases_capacity(monkeypatch):
    extractor = controlled_extractor(monkeypatch, lambda text, deadline: KeywordResult("empty"))

    def fail(*args):
        raise RuntimeError("broken process pool")

    monkeypatch.setattr(extractor._pool, "submit", fail)
    try:
        result = await extractor.extract("杭州", deadline=monotonic() + 1)
        assert result == KeywordResult("failed")
        assert extractor._inflight == 0
        assert extractor._worker_slots._value == 1
    finally:
        await extractor.aclose()
