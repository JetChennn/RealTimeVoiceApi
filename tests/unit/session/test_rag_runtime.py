import asyncio
from unittest.mock import patch

import pytest

from realtime_voice.clients.thinker import ThinkerDone
from realtime_voice.config import Settings
from realtime_voice.main import AppServices
from realtime_voice.protocol.client_messages import CreateSession
from realtime_voice.session.actor import StartThinker
from realtime_voice.session.events import ThinkerSkipped
from realtime_voice.session.state import TurnContext, TurnStage
from realtime_voice.transport.factory import build_runtime, configure_services
from tests.unit.clients.test_rag import CREATE
from tests.unit.session.test_runtime import make_runtime


class Thinker:
    def __init__(self):
        self.requests = []

    async def stream_reply(self, request):
        self.requests.append(request)
        yield ThinkerDone("answer", "")


def add_turn(runtime, turn_id: int) -> StartThinker:
    text = str(turn_id)
    runtime.actor.state.turns[turn_id] = TurnContext(
        turn_id,
        text,
        b"wav",
        stage=TurnStage.STREAMING_LLM,
        thinker_generation=1,
    )
    runtime.actor.state.active_llm_turn_id = turn_id
    return StartThinker(turn_id, 1, text, b"wav")


@pytest.mark.parametrize("enabled", [False, True])
async def test_session_rag_configuration_is_forwarded_on_every_thinker_request(enabled):
    thinker = Thinker()
    runtime, _ = make_runtime(
        thinker=thinker,
        rag_enabled=enabled,
        scenes=("a", "b"),
    )

    for turn_id in (1, 2):
        await runtime._run_thinker(add_turn(runtime, turn_id))

    assert [request.text for request in thinker.requests] == ["1", "2"]
    assert [request.rag_enabled for request in thinker.requests] == [enabled, enabled]
    assert [request.rag_scenes for request in thinker.requests] == [("a", "b"), ("a", "b")]


async def test_general_knowledge_rag_is_forwarded_with_empty_scenes():
    thinker = Thinker()
    runtime, _ = make_runtime(thinker=thinker, rag_enabled=True, scenes=())

    await runtime._run_thinker(add_turn(runtime, 1))

    assert thinker.requests[0].rag_enabled is True
    assert thinker.requests[0].rag_scenes == ()


async def test_interrupted_turn_is_skipped_before_calling_thinker():
    thinker = Thinker()
    runtime, _ = make_runtime(thinker=thinker, rag_enabled=True, scenes=("a",))
    effect = add_turn(runtime, 1)
    runtime.actor.state.turns[1].interrupted = True

    await runtime._run_thinker(effect)

    assert thinker.requests == []
    event = await asyncio.wait_for(runtime.events.get(), 1)
    assert isinstance(event, ThinkerSkipped)
    runtime.actor.handle(event)
    assert runtime.actor.state.turns[1].stage is TurnStage.INTERRUPTED
    assert runtime.actor.state.active_llm_turn_id is None


@pytest.mark.parametrize("mode", ["generation", "stage", "inactive"])
async def test_stale_thinker_effect_does_not_call_thinker(mode):
    thinker = Thinker()
    runtime, _ = make_runtime(thinker=thinker, rag_enabled=True, scenes=("a",))
    effect = add_turn(runtime, 1)
    turn = runtime.actor.state.turns[1]
    if mode == "generation":
        turn.thinker_generation += 1
    elif mode == "stage":
        turn.stage = TurnStage.COMPLETED
    else:
        runtime.actor.state.active_llm_turn_id = None

    await runtime._run_thinker(effect)

    assert thinker.requests == []
    assert runtime.events.empty()


async def test_sessions_keep_forwarded_rag_configuration_isolated():
    configurations = [(True, ("a",)), (True, ("b",)), (False, ())]
    runtimes = []
    thinkers = []
    for enabled, scenes in configurations:
        thinker = Thinker()
        runtime, _ = make_runtime(
            thinker=thinker,
            rag_enabled=enabled,
            scenes=scenes,
        )
        runtimes.append(runtime)
        thinkers.append(thinker)

    await asyncio.gather(
        *(
            runtime._run_thinker(add_turn(runtime, index + 1))
            for index, runtime in enumerate(runtimes)
        )
    )

    assert [
        (thinker.requests[0].rag_enabled, thinker.requests[0].rag_scenes)
        for thinker in thinkers
    ] == configurations


async def test_factory_copies_rag_configuration_without_creating_a_kb_client():
    services = AppServices(settings=Settings(_env_file=None))
    configure_services(services)
    create = CreateSession(**CREATE, rag_enabled=True, scenes=[" a ", "a", "b"])
    try:
        with patch("realtime_voice.transport.factory.SileroDetector"):
            runtime = build_runtime(create, object(), services)
        assert runtime._thinker_rag_enabled is True
        assert runtime._thinker_rag_scenes == ("a", "b")
        assert not hasattr(services, "rag_client")
        create.scenes.append("c")
        assert runtime._thinker_rag_scenes == ("a", "b")
    finally:
        for name in ["asr", "thinker", "tts"]:
            await getattr(services, name + "_client").http.aclose()
        await services.detector_offload.aclose()
