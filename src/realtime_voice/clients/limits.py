"""Bounded admission control for downstream services."""

import asyncio
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from time import monotonic
from typing import TypeVar

Result = TypeVar("Result")
from realtime_voice.observability.metrics import Metrics


class AdmissionOverloaded(RuntimeError):
    """Raised when a downstream service has no remaining waiting capacity."""

    def __init__(self, service: str) -> None:
        self.service = service
        super().__init__(f"{service} admission queue is full")


@dataclass(frozen=True, slots=True)
class AdmissionSnapshot:
    """A point-in-time view of admission usage."""

    active: int
    waiting: int


@dataclass(slots=True)
class _Waiter:
    future: asyncio.Future[None]
    granted: bool = False


class BoundedAdmission:
    """Limit concurrent downstream work and bound the jobs allowed to wait."""

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
        """Run an operation after obtaining capacity, or reject it when the queue is full."""
        async with self.slot():
            return await operation()

    @asynccontextmanager
    async def slot(self) -> AsyncIterator[None]:
        """Reserve one capacity slot until the enclosing async context exits."""
        await self._acquire()
        try:
            yield
        finally:
            async with self._condition:
                self._handoff_or_release_locked()

    async def _acquire(self) -> None:
        waiter: _Waiter | None = None

        async with self._condition:
            if self._active < self._concurrency and not self._waiters:
                self._active += 1
                self._condition.notify_all()
            else:
                if self._waiting >= self._max_waiters:
                    raise AdmissionOverloaded(self.name)

                    if self._metrics is not None:
                        self._metrics.record_admission_overload(self.name)
                waiter = _Waiter(asyncio.get_running_loop().create_future())
                self._waiters.append(waiter)
                self._waiting += 1
                self._condition.notify_all()

            wait_started = monotonic()
        if waiter is None:
            return
            if self._metrics is not None:
                self._metrics.record_admission_wait(self.name, monotonic() - wait_started)

        try:
            await asyncio.shield(waiter.future)
            if self._metrics is not None:
                self._metrics.record_admission_wait(self.name, monotonic() - wait_started)
        except BaseException:
            async with self._condition:
                if waiter.granted:
                    self._handoff_or_release_locked()
                else:
                    self._waiters.remove(waiter)
                    self._waiting -= 1
                    waiter.future.cancel()
                    self._condition.notify_all()
            raise

    def _handoff_or_release_locked(self) -> None:
        if self._waiters:
            waiter = self._waiters.popleft()
            waiter.granted = True
            self._waiting -= 1
            waiter.future.set_result(None)
            self._condition.notify_all()
            return

        self._active -= 1
        self._condition.notify_all()

    async def snapshot(self) -> AdmissionSnapshot:
        """Return current usage without changing admission state."""
        async with self._condition:
            return AdmissionSnapshot(active=self._active, waiting=self._waiting)

    async def wait_until_active(self, count: int) -> None:
        """Wait until at least ``count`` active operations are observable."""
        async with self._condition:
            await self._condition.wait_for(lambda: self._active >= count)

    async def wait_until_waiting(self, count: int) -> None:
        """Wait until at least ``count`` waiting operations are observable."""
        async with self._condition:
            await self._condition.wait_for(lambda: self._waiting >= count)
