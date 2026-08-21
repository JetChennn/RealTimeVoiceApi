import asyncio

import pytest

from realtime_voice.clients.limits import AdmissionOverloaded, BoundedAdmission


async def test_admission_rejects_job_beyond_waiter_limit() -> None:
    gate = BoundedAdmission("asr", concurrency=1, max_waiters=1)
    release = asyncio.Event()

    first = asyncio.create_task(gate.run(lambda: release.wait()))
    await gate.wait_until_active(1)
    second = asyncio.create_task(gate.run(lambda: release.wait()))
    await gate.wait_until_waiting(1)

    with pytest.raises(AdmissionOverloaded, match="asr"):
        await gate.run(lambda: release.wait())

    release.set()
    await asyncio.gather(first, second)


async def test_admission_releases_waiter_slot_when_waiting_job_is_cancelled() -> None:
    gate = BoundedAdmission("tts", concurrency=1, max_waiters=1)
    release = asyncio.Event()

    first = asyncio.create_task(gate.run(lambda: release.wait()))
    await gate.wait_until_active(1)
    waiting = asyncio.create_task(gate.run(lambda: release.wait()))
    await gate.wait_until_waiting(1)

    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting

    replacement = asyncio.create_task(gate.run(lambda: release.wait()))
    await gate.wait_until_waiting(1)
    release.set()
    await asyncio.gather(first, replacement)


async def test_admission_releases_active_slot_when_job_raises() -> None:
    gate = BoundedAdmission("berry", concurrency=1, max_waiters=0)

    async def fails() -> None:
        raise RuntimeError("downstream failed")

    with pytest.raises(RuntimeError, match="downstream failed"):
        await gate.run(fails)

    assert await gate.run(lambda: asyncio.sleep(0, result="available")) == "available"


async def test_admission_starts_older_waiter_before_new_arrival_after_release() -> None:
    gate = BoundedAdmission("asr", concurrency=1, max_waiters=2)
    holder_release = asyncio.Event()
    old_release = asyncio.Event()
    new_release = asyncio.Event()
    start_order: list[str] = []
    old_started = asyncio.Event()
    new_started = asyncio.Event()

    async def old_operation() -> None:
        start_order.append("old")
        old_started.set()
        await old_release.wait()

    async def new_operation() -> None:
        start_order.append("new")
        new_started.set()
        await new_release.wait()

    holder = asyncio.create_task(gate.run(lambda: holder_release.wait()))
    await gate.wait_until_active(1)
    old = asyncio.create_task(gate.run(old_operation))
    await gate.wait_until_waiting(1)
    old_start = asyncio.create_task(old_started.wait())
    new_start = asyncio.create_task(new_started.wait())
    holder_release.set()
    new = asyncio.create_task(gate.run(new_operation))
    try:
        done, _ = await asyncio.wait((old_start, new_start), return_when=asyncio.FIRST_COMPLETED)
        assert old_start in done
        assert new_start not in done
        assert start_order == ["old"]
        old_release.set()
        await new_started.wait()
        assert start_order == ["old", "new"]
    finally:
        holder_release.set()
        old_release.set()
        new_release.set()
        await asyncio.gather(holder, old, new, return_exceptions=True)
        old_start.cancel()
        new_start.cancel()
        await asyncio.gather(old_start, new_start, return_exceptions=True)


async def test_admission_advances_after_cancelling_a_middle_waiter() -> None:
    gate = BoundedAdmission("asr", concurrency=1, max_waiters=3)
    holder_release = asyncio.Event()
    head_release = asyncio.Event()
    middle_release = asyncio.Event()
    tail_release = asyncio.Event()
    start_order: list[str] = []
    head_started = asyncio.Event()
    tail_started = asyncio.Event()

    async def head_operation() -> None:
        start_order.append("head")
        head_started.set()
        await head_release.wait()

    async def tail_operation() -> None:
        start_order.append("tail")
        tail_started.set()
        await tail_release.wait()

    holder = asyncio.create_task(gate.run(lambda: holder_release.wait()))
    await gate.wait_until_active(1)
    head = asyncio.create_task(gate.run(head_operation))
    await gate.wait_until_waiting(1)
    middle = asyncio.create_task(gate.run(lambda: middle_release.wait()))
    await gate.wait_until_waiting(2)
    tail = asyncio.create_task(gate.run(tail_operation))
    await gate.wait_until_waiting(3)
    try:
        middle.cancel()
        with pytest.raises(asyncio.CancelledError):
            await middle
        holder_release.set()
        await head_started.wait()
        assert start_order == ["head"]
        head_release.set()
        await tail_started.wait()
        assert start_order == ["head", "tail"]
    finally:
        holder_release.set()
        head_release.set()
        middle_release.set()
        tail_release.set()
        await asyncio.gather(holder, head, middle, tail, return_exceptions=True)


async def test_admission_handoffs_after_cancelling_an_active_granted_job() -> None:
    gate = BoundedAdmission("tts", concurrency=1, max_waiters=1)
    never_finish = asyncio.Event()
    successor_release = asyncio.Event()
    first_started = asyncio.Event()
    successor_started = asyncio.Event()

    async def first_operation() -> None:
        first_started.set()
        await never_finish.wait()

    async def successor_operation() -> None:
        successor_started.set()
        await successor_release.wait()

    first = asyncio.create_task(gate.run(first_operation))
    await first_started.wait()
    successor = asyncio.create_task(gate.run(successor_operation))
    await gate.wait_until_waiting(1)
    try:
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        await successor_started.wait()
    finally:
        never_finish.set()
        successor_release.set()
        await asyncio.gather(first, successor, return_exceptions=True)
