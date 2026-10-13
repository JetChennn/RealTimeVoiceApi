"""Prewarmed, bounded local keyword extraction, outside the ASR request path."""

from __future__ import annotations

import asyncio
import logging
import re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from threading import Lock
from time import monotonic

import jieba
import jieba.posseg


@dataclass(frozen=True, slots=True)
class KeywordResult:
    status: str
    context: str = ""
    count: int = 0
    compute_seconds: float = 0.0


class AsrKeywordExtractor:
    """Two shared workers; capacity stays occupied until timed-out work really ends."""

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
        # Jieba defaults to DEBUG with a plain-text handler. Keep startup logs
        # quiet so application diagnostics retain the structured JSON contract.
        jieba.setLogLevel(logging.WARNING)
        self.max_keywords = max_keywords
        self.max_chars = max_chars
        self._capacity = workers + max_waiters
        self._inflight = 0
        self._lock = Lock()
        self._worker_slots = asyncio.BoundedSemaphore(workers)
        self._pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="asr-keywords")
        self._tagger = jieba.posseg.POSTokenizer(jieba.Tokenizer())
        self._ready = False
        self._closed = False

    async def start(self) -> None:
        await asyncio.to_thread(self._warmup)
        self._ready = True

    def _warmup(self) -> None:
        self._tagger.initialize()
        list(self._tagger.cut("李白在杭州游览西湖。", HMM=True))

    def extract_sync(self, text: str) -> KeywordResult:
        """Analyze only a bounded suffix and preserve whole original proper nouns."""
        started = monotonic()
        text = text[-2048:]
        words: list[str] = []
        seen: set[str] = set()
        chars = 0
        for token in self._tagger.cut(text, HMM=True):
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
            if chars + size > self.max_chars:
                continue
            words.append(word)
            chars += size
            if len(words) >= self.max_keywords:
                break
        return KeywordResult(
            "success" if words else "empty",
            " ".join(words),
            len(words),
            monotonic() - started,
        )

    async def extract(self, text: str, *, deadline: float) -> KeywordResult:
        if not self._ready or self._closed:
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
            loop.call_soon_threadsafe(self._worker_slots.release)

        try:
            async with asyncio.timeout(max(0.0, deadline - monotonic())):
                # Queue in asyncio, not inside the executor: each completed job
                # must give the event loop a turn before another CPU job starts.
                await self._worker_slots.acquire()
                acquired = True
                if monotonic() >= deadline:
                    return KeywordResult("timeout")
                future = self._pool.submit(self._extract_before_deadline, text[-2048:], deadline)
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

    def _extract_before_deadline(self, text: str, deadline: float) -> KeywordResult:
        if monotonic() >= deadline:
            return KeywordResult("timeout")
        return self.extract_sync(text)

    def _release(self) -> None:
        with self._lock:
            self._inflight -= 1

    async def aclose(self) -> None:
        self._closed = True
        await asyncio.to_thread(self._pool.shutdown, wait=True, cancel_futures=True)
