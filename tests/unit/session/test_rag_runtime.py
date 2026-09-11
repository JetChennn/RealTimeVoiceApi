import asyncio

import pytest

from realtime_voice.clients.rag import RagResult
from realtime_voice.clients.thinker import ThinkerDone
from realtime_voice.session.actor import StartThinker
from realtime_voice.session.events import ThinkerSkipped
from realtime_voice.session.state import TurnContext, TurnStage
from tests.unit.session.test_runtime import make_runtime


class Thinker:
    def __init__(self):
        self.requests = []

    async def stream_reply(self, request):
        self.requests.append(request)
        yield ThinkerDone("answer", "")


class Rag:
    def __init__(self, blocked=False):
        self.calls = []
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.cancelled = False
        if not blocked:
            self.release.set()

    async def retrieve(self, question, scenes):
        self.calls.append((question, scenes))
        self.started.set()
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        return RagResult("knowledge:" + question, "success", 1, 0.01)


def add_turn(runtime, n, *, interrupted=False):
    runtime.actor.state.turns[n] = TurnContext(
        n,
        str(n),
        b"wav",
        stage=TurnStage.STREAMING_LLM,
        thinker_generation=1,
        interrupted=interrupted,
    )
    runtime.actor.state.active_llm_turn_id = n
    return StartThinker(n, 1, str(n), b"wav")


@pytest.mark.parametrize("enabled", [False, True])
async def test_independent_turn_context_and_disabled_bypass(enabled):
    rag, thinker = Rag(), Thinker()
    runtime, _ = make_runtime(thinker=thinker, rag_client=rag, rag_enabled=enabled, scenes=("a",))
    for n in [1, 2]:
        await runtime._run_thinker(add_turn(runtime, n))
    assert len(rag.calls) == (2 if enabled else 0)
    assert [r.text for r in thinker.requests] == ["1", "2"]
    assert [r.knowledge_context for r in thinker.requests] == (
        ["knowledge:1", "knowledge:2"] if enabled else ["", ""]
    )


async def test_interrupted_retrieval_releases_actor_for_next_turn():
    rag, thinker = Rag(blocked=True), Thinker()
    runtime, _ = make_runtime(thinker=thinker, rag_client=rag, rag_enabled=True, scenes=("a",))
    effect = add_turn(runtime, 1)
    task = asyncio.create_task(runtime._run_thinker(effect))
    await asyncio.wait_for(rag.started.wait(), 1)
    for interrupt in runtime.actor._interrupt_unfinished_turns():
        await runtime.execute_effect(interrupt)
    runtime.actor.state.turns[2] = TurnContext(2, "next", b"wav")
    runtime.actor.state.llm_queue.append(2)
    await asyncio.wait_for(task, 1)
    event = await runtime.events.get()
    assert isinstance(event, ThinkerSkipped)
    effects = runtime.actor.handle(event)
    assert runtime.actor.state.active_llm_turn_id == 2
    assert runtime.actor.state.turns[1].stage == TurnStage.INTERRUPTED
    assert any(getattr(e, "text", None) == "next" for e in effects)
    assert rag.cancelled and not thinker.requests and not runtime._rag_tasks


@pytest.mark.parametrize("mode", ["closing", "stale", "interrupted"])
async def test_discard_results_without_starting_thinker(mode):
    rag, thinker = Rag(blocked=True), Thinker()
    runtime, _ = make_runtime(thinker=thinker, rag_client=rag, rag_enabled=True, scenes=("a",))
    task = asyncio.create_task(runtime._run_thinker(add_turn(runtime, 1)))
    await asyncio.wait_for(rag.started.wait(), 1)
    if mode == "closing":
        runtime._closing = True
    elif mode == "stale":
        runtime.actor.state.turns[1].thinker_generation += 1
    else:
        runtime.actor.state.turns[1].interrupted = True
    rag.release.set()
    await asyncio.wait_for(task, 1)
    assert not thinker.requests
    assert not runtime._rag_tasks


async def test_shared_client_keeps_sessions_separate():
    rag = Rag()
    runtimes = []
    thinkers = []
    for scene in ["a", "b"]:
        thinker = Thinker()
        runtime, _ = make_runtime(
            thinker=thinker, rag_client=rag, rag_enabled=True, scenes=(scene,)
        )
        runtimes.append(runtime)
        thinkers.append(thinker)
    await asyncio.gather(*(r._run_thinker(add_turn(r, i + 1)) for i, r in enumerate(runtimes)))
    assert set(rag.calls) == {("1", ("a",)), ("2", ("b",))}
    assert [t.requests[0].knowledge_context for t in thinkers] == ["knowledge:1", "knowledge:2"]


async def test_cleanup_cancels_rag_without_marking_thinker_unsafe():
    from realtime_voice.clients.thinker import DeleteResult

    class CleanupThinker(Thinker):
        deleted = False

        async def delete_session(self, user_id, session_id):
            self.deleted = True
            return DeleteResult.DELETED

    rag, thinker = Rag(blocked=True), CleanupThinker()
    runtime, _ = make_runtime(thinker=thinker, rag_client=rag, rag_enabled=True, scenes=("a",))
    await runtime.execute_effect(add_turn(runtime, 1))
    await asyncio.wait_for(rag.started.wait(), 1)
    await asyncio.wait_for(runtime._cleanup_once(), 1)
    assert rag.cancelled and thinker.deleted
    assert not thinker.requests and not runtime._rag_tasks
    assert runtime._thinker_cleanup_safe


async def test_real_rag_cancellation_releases_shared_capacity():
    import httpx

    from realtime_voice.clients.limits import BoundedAdmission
    from realtime_voice.clients.rag import RagClient
    from realtime_voice.config import Settings

    started = asyncio.Event()

    async def handler(request):
        started.set()
        await asyncio.Event().wait()

    gate = BoundedAdmission("rag", 1, 1)
    async with httpx.AsyncClient(
        base_url="http://kb", transport=httpx.MockTransport(handler)
    ) as http:
        rag = RagClient(http, gate, settings=Settings(_env_file=None))
        task = asyncio.create_task(rag.retrieve("q", ("a",)))
        await asyncio.wait_for(started.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        snapshot = await gate.snapshot()
        assert (snapshot.active, snapshot.waiting) == (0, 0)


async def test_factory_copies_create_session_configuration():
    from unittest.mock import patch

    from realtime_voice.config import Settings
    from realtime_voice.main import AppServices
    from realtime_voice.protocol.client_messages import CreateSession
    from realtime_voice.transport.factory import build_runtime, configure_services
    from tests.unit.clients.test_rag import CREATE

    services = AppServices(settings=Settings(_env_file=None))
    configure_services(services)
    create = CreateSession(**CREATE, rag_enabled=True, scenes=[" a ", "a", "b"])
    try:
        with patch("realtime_voice.transport.factory.SileroDetector"):
            runtime = build_runtime(create, object(), services)
        assert runtime._rag_enabled
        assert runtime._rag_client is services.rag_client
        assert runtime._rag_scenes == ("a", "b")
        create.scenes.append("c")
        assert runtime._rag_scenes == ("a", "b")
    finally:
        for name in ["rag", "asr", "thinker", "tts"]:
            await getattr(services, name + "_client").http.aclose()
        await services.detector_offload.aclose()
