"""Concurrency-safe registry for active realtime sessions."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fastapi import WebSocket

    from realtime_voice.protocol.client_messages import CreateSession
    from realtime_voice.session.runtime import SessionRuntime


RuntimeFactory = Callable[["CreateSession", "WebSocket"], "SessionRuntime"]


class DuplicateSession(RuntimeError):
    """Raised when an active session already owns the requested identifier."""

    code = "DUPLICATE_SESSION"

    def __init__(self, key: str) -> None:
        super().__init__(f"{self.code}: {key}")


class SessionCapacityExceeded(RuntimeError):
    """Raised when the configured active-session capacity is exhausted."""

    code = "SESSION_CAPACITY_EXCEEDED"

    def __init__(self, key: str) -> None:
        super().__init__(f"{self.code}: {key}")


class SessionRegistry:
    """Atomically admit, locate, and remove active session runtimes."""

    def __init__(
        self,
        max_sessions: int,
        runtime_factory: RuntimeFactory | None = None,
    ) -> None:
        if max_sessions < 1:
            raise ValueError("max_sessions must be at least 1")
        self.max_sessions = max_sessions
        self._runtime_factory = runtime_factory
        self._runtimes: dict[str, SessionRuntime] = {}
        self._lock = asyncio.Lock()

    @property
    def active_count(self) -> int:
        """Return the current number of admitted runtimes."""
        return len(self._runtimes)

    async def add(self, key: str, runtime: SessionRuntime) -> None:
        """Admit one runtime, preferring the stable duplicate error at capacity."""
        async with self._lock:
            if key in self._runtimes:
                raise DuplicateSession(key)
            if len(self._runtimes) >= self.max_sessions:
                raise SessionCapacityExceeded(key)
            self._runtimes[key] = runtime

    async def create(self, create: CreateSession, websocket: WebSocket) -> SessionRuntime:
        """Construct and atomically register a runtime for a CREATE_SESSION message."""
        if self._runtime_factory is None:
            raise RuntimeError("SessionRegistry requires a runtime factory to create sessions")
        runtime = self._runtime_factory(create, websocket)
        await self.add(create.session_id, runtime)
        try:
            runtime.bind_registry(self)
        except BaseException:
            await self.remove(create.session_id)
            raise
        return runtime

    async def remove(self, key: str) -> None:
        """Idempotently release one active session identifier."""
        async with self._lock:
            self._runtimes.pop(key, None)
