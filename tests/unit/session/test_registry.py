import asyncio
from typing import get_type_hints

import pytest
from fastapi import WebSocket

import realtime_voice.session.registry as registry_module
from realtime_voice.protocol.client_messages import CreateSession
from realtime_voice.session.registry import (
    DuplicateSession,
    SessionCapacityExceeded,
    SessionRegistry,
)
from realtime_voice.session.runtime import SessionRuntime


class RuntimeStub:
    def __init__(self) -> None:
        self.registry: SessionRegistry | None = None

    def bind_registry(self, registry: SessionRegistry) -> None:
        self.registry = registry


async def test_registry_rejects_duplicate_before_capacity() -> None:
    registry = SessionRegistry(max_sessions=1)
    first_runtime = RuntimeStub()
    await registry.add("session-1", first_runtime)

    with pytest.raises(DuplicateSession) as duplicate:
        await registry.add("session-1", RuntimeStub())
    with pytest.raises(SessionCapacityExceeded) as capacity:
        await registry.add("session-2", RuntimeStub())

    assert duplicate.value.code == "DUPLICATE_SESSION"
    assert str(duplicate.value) == "DUPLICATE_SESSION: session-1"
    assert capacity.value.code == "SESSION_CAPACITY_EXCEEDED"
    assert str(capacity.value) == "SESSION_CAPACITY_EXCEEDED: session-2"
    assert registry.active_count == 1


async def test_registry_duplicate_check_is_atomic_under_concurrency() -> None:
    registry = SessionRegistry(max_sessions=4)

    results = await asyncio.gather(
        *(registry.add("same-session", RuntimeStub()) for _ in range(8)),
        return_exceptions=True,
    )

    assert sum(result is None for result in results) == 1
    assert sum(isinstance(result, DuplicateSession) for result in results) == 7
    assert registry.active_count == 1


async def test_registry_create_registers_factory_runtime_and_binds_cleanup() -> None:
    created: list[tuple[object, object]] = []

    def factory(create: object, websocket: object) -> RuntimeStub:
        created.append((create, websocket))
        return RuntimeStub()

    registry = SessionRegistry(max_sessions=1, runtime_factory=factory)
    create = type("Create", (), {"session_id": "session-1"})()
    websocket = object()

    runtime = await registry.create(create, websocket)

    assert created == [(create, websocket)]
    assert runtime.registry is registry
    assert registry.active_count == 1

    await registry.remove("session-1")
    await registry.remove("session-1")
    assert registry.active_count == 0


async def test_registry_create_rolls_back_admission_when_binding_fails() -> None:
    class BrokenRuntime(RuntimeStub):
        def bind_registry(self, registry: SessionRegistry) -> None:
            raise RuntimeError("binding failed")

    registry = SessionRegistry(
        max_sessions=1,
        runtime_factory=lambda create, websocket: BrokenRuntime(),
    )
    create = type("Create", (), {"session_id": "session-1"})()

    with pytest.raises(RuntimeError, match="binding failed"):
        await registry.create(create, object())

    assert registry.active_count == 0



def test_registry_create_contract_returns_session_runtime() -> None:
    namespace = {
        **vars(registry_module),
        "CreateSession": CreateSession,
        "WebSocket": WebSocket,
        "SessionRuntime": SessionRuntime,
    }
    hints = get_type_hints(SessionRegistry.create, globalns=namespace)

    assert hints["return"] is SessionRuntime
