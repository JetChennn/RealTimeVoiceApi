"""Prewarmed, bounded local keyword extraction, outside the ASR request path."""

from __future__ import annotations

import asyncio
import logging
import multiprocessing
import os
import re
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from threading import Lock
from time import monotonic

_STARTUP_TIMEOUT_SECONDS = 60.0
_TERMINATE_GRACE_SECONDS = 1.0
_KILL_GRACE_SECONDS = 1.0
_MANAGER_GRACE_SECONDS = 1.0


@dataclass(frozen=True, slots=True)
class KeywordResult:
    status: str
    context: str = ""
    count: int = 0
    compute_seconds: float = 0.0


_worker_tagger = None
_worker_limits = (12, 128)
_worker_startup_barrier = None


def _initialize_worker(max_keywords: int, max_chars: int, startup_barrier) -> None:
    """Load a private dictionary once in each spawned worker, never in the gateway."""
    # posseg builds dictionary state at import time; keep even that work isolated.
    import jieba
    import jieba.posseg

    global _worker_tagger, _worker_limits, _worker_startup_barrier
    jieba.setLogLevel(logging.WARNING)
    _worker_tagger = jieba.posseg.POSTokenizer(jieba.Tokenizer())
    _worker_tagger.initialize()
    list(_worker_tagger.cut("李白在杭州游览西湖。", HMM=True))
    _worker_limits = (max_keywords, max_chars)
    _worker_startup_barrier = startup_barrier


def _worker_ready() -> int:
    # A fast probe alone could run repeatedly in just one process. The barrier
    # makes every configured process finish dictionary warmup before start returns.
    _worker_startup_barrier.wait(timeout=30)
    return os.getpid()


def _extract_keywords(text: str, tagger, max_keywords: int, max_chars: int) -> KeywordResult:
    """Analyze a bounded suffix, preserving whole original proper nouns."""
    started = monotonic()
    text = text[-2048:]
    words: list[str] = []
    seen: set[str] = set()
    chars = 0
    for token in tagger.cut(text, HMM=True):
        word = token.word
        if not token.flag.startswith(("nr", "ns", "nt", "nz")):
            continue
        if (
            word in seen
            or len(word) < 2
            or word not in text
            or not re.fullmatch(r"[\w\u3400-\u9fff]+", word)
            or word.isnumeric()
        ):
            continue
        seen.add(word)
        size = len(word) + bool(words)
        if chars + size > max_chars:
            continue
        words.append(word)
        chars += size
        if len(words) >= max_keywords:
            break
    return KeywordResult(
        "success" if words else "empty",
        " ".join(words),
        len(words),
        monotonic() - started,
    )


def _extract_before_deadline(text: str, deadline: float) -> KeywordResult:
    # Both processes use the same system monotonic clock. Do not start stale work
    # that expired while being serialized, delivered or scheduled by the OS.
    if monotonic() >= deadline:
        return KeywordResult("timeout")
    if _worker_tagger is None:
        raise RuntimeError("keyword worker is not initialized")
    return _extract_keywords(text, _worker_tagger, *_worker_limits)


def _worker_alive(process) -> bool:
    try:
        return process.is_alive()
    except ValueError:
        # The executor manager may already have reaped and closed this handle.
        return False


def _join_workers(processes, timeout: float) -> None:
    # One shared budget per phase, not a full timeout for every worker.
    deadline = monotonic() + timeout
    for process in processes:
        try:
            process.join(timeout=max(0.0, deadline - monotonic()))
        except ValueError:
            pass  # Already closed by the executor manager.


def _abort_pool_startup(pool: ProcessPoolExecutor) -> None:
    """Bounded failure cleanup, including initializers that ignore SIGTERM."""
    # Python 3.11-3.13 have no public worker termination API. Snapshot only this
    # pool's handles before shutdown invalidates them; never touch other children.
    processes = tuple((pool._processes or {}).copy().values())
    manager = pool._executor_manager_thread
    # Even shutdown(wait=False) takes the executor's shutdown lock. A broken
    # pool manager can hold it while joining a worker that ignores SIGTERM, so
    # signal/reap workers directly before touching any executor shutdown API.
    for process in processes:
        if _worker_alive(process):
            try:
                process.terminate()
            except (ProcessLookupError, ValueError):
                pass  # Exited/closed between the liveness check and the signal.
    _join_workers(processes, _TERMINATE_GRACE_SECONDS)
    for process in processes:
        if _worker_alive(process):
            try:
                process.kill()
            except (ProcessLookupError, ValueError):
                pass
    _join_workers(processes, _KILL_GRACE_SECONDS)
    if manager is not None:
        manager.join(timeout=_MANAGER_GRACE_SECONDS)
        if manager.is_alive():
            # Never turn the bounded manager join back into an unbounded lock
            # acquisition. Keep the original startup exception and report this
            # exceptional residual cleanup state without conversation content.
            from realtime_voice.observability.logging import log_event

            log_event(
                "asr_keyword_cleanup_incomplete", level=logging.WARNING,
                stage="ASR_CONTEXT", status="failed", reason="manager_cleanup_timeout",
            )
            return
    pool.shutdown(wait=False, cancel_futures=True)


class AsrKeywordExtractor:
    """Persistent isolated processes; retain capacity until timed-out work ends."""

    def __init__(
        self,
        *,
        max_keywords: int = 12,
        max_chars: int = 128,
        workers: int = 2,
        max_waiters: int = 32,
    ) -> None:
        if min(max_keywords, max_chars, workers) < 1 or max_waiters < 0:
            raise ValueError("invalid keyword extraction limits")
        self.max_keywords = max_keywords
        self.max_chars = max_chars
        self.workers = workers
        self._capacity = workers + max_waiters
        self._inflight = 0
        self._lock = Lock()
        self._worker_slots = asyncio.BoundedSemaphore(workers)
        self._pool: ProcessPoolExecutor | None = None
        self._worker_pids: tuple[int, ...] = ()
        self._ready = False
        self._closed = False

    async def start(self) -> None:
        if self._closed:
            raise RuntimeError("keyword extractor is closed")
        if self._ready:
            return
        # Never fork a gateway with live PyTorch/ONNX threads or network clients.
        context = multiprocessing.get_context("spawn")
        barrier = context.Barrier(self.workers)
        self._pool = ProcessPoolExecutor(
            max_workers=self.workers,
            mp_context=context,
            initializer=_initialize_worker,
            initargs=(self.max_keywords, self.max_chars, barrier),
        )
        try:
            futures = [self._pool.submit(_worker_ready) for _ in range(self.workers)]
            pids = await asyncio.wait_for(
                asyncio.gather(*(asyncio.wrap_future(future) for future in futures)),
                timeout=_STARTUP_TIMEOUT_SECONDS,
            )
            self._worker_pids = tuple(sorted(pids))
            self._ready = True
        except BaseException:
            self._closed = True
            self._ready = False
            # Detach before awaiting: the application's startup-failure finally
            # calls aclose again and must not resume an unbounded graceful join.
            pool, self._pool = self._pool, None
            await asyncio.shield(asyncio.to_thread(_abort_pool_startup, pool))
            raise

    async def extract(self, text: str, *, deadline: float) -> KeywordResult:
        if not self._ready or self._closed or self._pool is None:
            return KeywordResult("failed")
        if monotonic() >= deadline:
            return KeywordResult("timeout")
        with self._lock:
            if self._inflight >= self._capacity:
                return KeywordResult("overloaded")
            self._inflight += 1
        future = None
        acquired = False
        loop = asyncio.get_running_loop()

        def completed(_):
            self._release()
            if not loop.is_closed():
                loop.call_soon_threadsafe(self._worker_slots.release)

        try:
            async with asyncio.timeout(max(0.0, deadline - monotonic())):
                # Keep waiting asynchronous and bounded. Only serializable text
                # and its deadline cross the process boundary, never session state.
                await self._worker_slots.acquire()
                acquired = True
                if monotonic() >= deadline:
                    return KeywordResult("timeout")
                future = self._pool.submit(_extract_before_deadline, text[-2048:], deadline)
                future.add_done_callback(completed)
                wrapped = asyncio.wrap_future(future)
                # Consume errors even if this session times out or disconnects.
                wrapped.add_done_callback(
                    lambda done: None if done.cancelled() else done.exception()
                )
                result = await asyncio.shield(wrapped)
            return result if monotonic() < deadline else KeywordResult("timeout")
        except TimeoutError:
            if future is not None:
                future.cancel()  # Running work keeps its capacity until it really completes.
            return KeywordResult("timeout")
        except asyncio.CancelledError:
            if future is not None:
                future.cancel()
            raise
        except Exception:  # noqa: BLE001 - optional context must never break dialogue
            return KeywordResult("failed")
        finally:
            if future is None:
                self._release()
                if acquired:
                    self._worker_slots.release()

    def _release(self) -> None:
        with self._lock:
            self._inflight -= 1

    async def aclose(self) -> None:
        self._closed = True
        self._ready = False
        if self._pool is not None:
            await asyncio.to_thread(self._pool.shutdown, wait=True, cancel_futures=True)
