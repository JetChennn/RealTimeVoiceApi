"""Bounded admission control for downstream services."""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TypeVar

Result = TypeVar("Result")


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


class BoundedAdmission:
    """Limit concurrent downstream work and bound the jobs allowed to wait."""

    def __init__(self, name: str, concurrency: int, max_waiters: int) -> None:
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

    async def run(self, operation: Callable[[], Awaitable[Result]]) -> Result:
        """Run an operation after obtaining capacity, or reject it when the queue is full."""
        async with self._condition:
            if self._active < self._concurrency and self._waiting == 0:
                self._active += 1
                self._condition.notify_all()
            else:
                if self._waiting >= self._max_waiters:
                    raise AdmissionOverloaded(self.name)

                self._waiting += 1
                self._condition.notify_all()
                try:
                    await self._condition.wait_for(lambda: self._active < self._concurrency)
                except BaseException:
                    self._waiting -= 1
                    self._condition.notify_all()
                    raise
                else:
                    self._waiting -= 1
                    self._active += 1
                    self._condition.notify_all()

        try:
            return await operation()
        finally:
            async with self._condition:
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
