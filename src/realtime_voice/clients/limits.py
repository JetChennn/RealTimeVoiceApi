"""下游服务的有界准入控制。"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from time import monotonic
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from realtime_voice.observability.metrics import Metrics

Result = TypeVar("Result")
class AdmissionOverloaded(RuntimeError):
    """当下游服务已无剩余等待容量时抛出。"""

    def __init__(self, service: str) -> None:
        self.service = service
        super().__init__(f"{service} admission queue is full")


@dataclass(frozen=True, slots=True)
class AdmissionSnapshot:
    """准入使用情况的瞬时快照。"""

    active: int
    waiting: int


@dataclass(slots=True)
class _Waiter:
    future: asyncio.Future[None]
    granted: bool = False


class BoundedAdmission:
    """限制下游并发量，并约束允许排队等待的任务数。"""

    def __init__(
        self, name: str, concurrency: int, max_waiters: int, *, metrics: Metrics | None = None
    ) -> None:
        self._metrics = metrics
        if concurrency < 1:
            raise ValueError("concurrency must be at least 1")
        if max_waiters < 0:
            raise ValueError("max_waiters must not be negative")

        self.name = name
        self._concurrency = concurrency
        self._max_waiters = max_waiters
        self._active = 0
        self._waiting = 0
        self._condition = asyncio.Condition()

        self._waiters: deque[_Waiter] = deque()

    async def run(self, operation: Callable[[], Awaitable[Result]]) -> Result:
        """获取容量后执行操作；队列已满时直接拒绝。"""
        async with self.slot():
            return await operation()

    @asynccontextmanager
    async def slot(self) -> AsyncIterator[None]:
        """预留一个容量槽，直到外层 async 上下文退出。"""
        await self._acquire()
        try:
            yield
        finally:
            async with self._condition:
                self._handoff_or_release_locked()

    async def _acquire(self) -> None:
        waiter: _Waiter | None = None

        async with self._condition:
            if self._active < self._concurrency and not self._waiters:  # 有空闲并发且无排队者时直接获取，避免插队
                self._active += 1
                self._condition.notify_all()
            else:
                if self._waiting >= self._max_waiters:  # 等待队列已满，快速失败而非无限堆积
                    if self._metrics is not None:
                        self._metrics.record_admission_overload(self.name)
                    raise AdmissionOverloaded(self.name)
                waiter = _Waiter(asyncio.get_running_loop().create_future())
                self._waiters.append(waiter)
                self._waiting += 1
                self._condition.notify_all()

            wait_started = monotonic()
        if waiter is None:
            if self._metrics is not None:
                self._metrics.record_admission_wait(self.name, 0.0)
            return

        try:
            await asyncio.shield(waiter.future)  # shield 防止取消时丢失唤醒；异常时据 granted 状态决定是否释放
            if self._metrics is not None:
                self._metrics.record_admission_wait(self.name, monotonic() - wait_started)
        except BaseException:
            async with self._condition:
                if waiter.granted:  # 已被授予容量则必须释放，否则仅从队列移除
                    self._handoff_or_release_locked()
                else:
                    self._waiters.remove(waiter)
                    self._waiting -= 1
                    waiter.future.cancel()
                    self._condition.notify_all()
            raise

    def _handoff_or_release_locked(self) -> None:
        if self._waiters:  # 有排队者时直接移交容量，避免先释放再竞争导致抖动
            waiter = self._waiters.popleft()
            waiter.granted = True
            self._waiting -= 1
            waiter.future.set_result(None)
            self._condition.notify_all()
            return

        self._active -= 1
        self._condition.notify_all()

    async def snapshot(self) -> AdmissionSnapshot:
        """返回当前使用情况，不改变准入状态。"""
        async with self._condition:
            return AdmissionSnapshot(active=self._active, waiting=self._waiting)

    async def wait_until_active(self, count: int) -> None:
        """等待直到可观测到至少 ``count`` 个活跃操作。"""
        async with self._condition:
            await self._condition.wait_for(lambda: self._active >= count)

    async def wait_until_waiting(self, count: int) -> None:
        """等待直到可观测到至少 ``count`` 个等待操作。"""
        async with self._condition:
            await self._condition.wait_for(lambda: self._waiting >= count)
