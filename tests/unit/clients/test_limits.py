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
