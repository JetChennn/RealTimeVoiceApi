import asyncio
import json

import httpx
import pytest
from pydantic import ValidationError

from realtime_voice.clients.limits import BoundedAdmission
from realtime_voice.clients.rag import RagClient
from realtime_voice.clients.thinker import ThinkerClient, ThinkerReplyRequest
from realtime_voice.config import Settings
from realtime_voice.observability.metrics import Metrics
from realtime_voice.protocol.client_messages import CreateSession

CREATE = {
    "type": "CREATE_SESSION",
    "protocol_version": 1,
    "device_id": "u",
    "session_id": "s",
    "audio_format": "PCM16",
    "audio_transport": "BASE64_JSON",
    "sample_rate": 16000,
    "channels": 1,
}
SNIPPET = {"scene": "农业社会", "source": "资料.md", "text": "农业以种植为主。"}


@pytest.mark.parametrize(
    "fields",
    [
        {"rag_enabled": True},
        {"scenes": [""]},
        {"scenes": ["a", "b", "c", "d"]},
        {"scenes": "a"},
        {"scenes": [1]},
        {"rag_enabled": "true"},
        {"rag_enabled": 1},
    ],
)
def test_invalid_configuration(fields):
    with pytest.raises(ValidationError):
        CreateSession(**CREATE, **fields)


def test_defaults_and_normalization():
    assert not CreateSession(**CREATE).rag_enabled
    assert CreateSession(**CREATE).scenes == []
    assert CreateSession(**CREATE, rag_enabled=True, scenes=[" a ", "a", "b", "b"]).scenes == [
        "a",
        "b",
    ]


@pytest.mark.parametrize(
    "payload,status,count",
    [
        ({"snippets": [SNIPPET], "errors": []}, "success", 1),
        ({"snippets": [SNIPPET], "errors": [{"scene": "bad"}]}, "partial_failure", 1),
        ({"snippets": [], "errors": []}, "no_match", 0),
        ({"snippets": [], "errors": [{"scene": "bad"}]}, "partial_failure", 0),
        ({"snippets": "bad", "errors": []}, "failed", 0),
        ({"snippets": [{"text": 1}], "errors": []}, "failed", 0),
        ({}, "failed", 0),
    ],
)
async def test_retrieval_contract(payload, status, count):
    def handler(request):
        assert request.url.path == "/retrieve/joint"
        assert json.loads(request.content) == {
            "question": "问题",
            "scenes": ["农业社会", "bad"],
            "top_k_per_scene": 5,
            "top_k_total": 8,
            "score_threshold": 0,
            "strict": False,
        }
        return httpx.Response(200, json=payload)

    metrics = Metrics()
    gate = BoundedAdmission("rag", 1, 1)
    async with httpx.AsyncClient(
        base_url="http://kb", transport=httpx.MockTransport(handler)
    ) as http:
        result = await RagClient(
            http, gate, settings=Settings(_env_file=None), metrics=metrics
        ).retrieve("问题", ("农业社会", "bad"))
    assert (result.status, result.snippet_count) == (status, count)
    assert bool(result.context) == bool(count)
    assert (await gate.snapshot()).active == 0
    assert f'status="{status}"' in metrics.render().decode()


@pytest.mark.parametrize("mode", ["timeout", "http", "connection", "json", "overload", "queue"])
async def test_failure_and_total_budget(mode):
    async def handler(request):
        if mode == "timeout":
            await asyncio.sleep(10)
        if mode == "connection":
            raise httpx.ConnectError("offline")
        return httpx.Response(503 if mode == "http" else 200, content=b"invalid")

    gate = BoundedAdmission("rag", 1, 0 if mode == "overload" else 1)
    async with httpx.AsyncClient(
        base_url="http://kb", transport=httpx.MockTransport(handler)
    ) as http:
        client = RagClient(http, gate, settings=Settings(_env_file=None, rag_timeout_seconds=0.02))
        if mode in {"overload", "queue"}:
            async with gate.slot():
                result = await client.retrieve("q", ("a",))
        else:
            result = await client.retrieve("q", ("a",))
    assert result.status == (
        "timeout"
        if mode in {"timeout", "queue"}
        else "overloaded"
        if mode == "overload"
        else "failed"
    )
    assert result.context == ""
    snapshot = await gate.snapshot()
    assert (snapshot.active, snapshot.waiting) == (0, 0)


async def test_thinker_context_is_separate_and_not_reused():
    bodies = []

    def handler(request):
        bodies.append(json.loads(request.content))
        return httpx.Response(
            200,
            text=json.dumps({"type": "done", "output": {"reply_text": "ok", "tone": ""}}) + "\n",
        )

    async with httpx.AsyncClient(
        base_url="http://thinker", transport=httpx.MockTransport(handler)
    ) as http:
        client = ThinkerClient(http, BoundedAdmission("thinker", 1, 1))
        for context in ["参考知识", ""]:
            async for _ in client.stream_reply(ThinkerReplyRequest("u", "s", "原文", context)):
                pass
    assert bodies[0]["text"] == "原文"
    assert bodies[0]["messages"] == [{"role": "system", "content": "参考知识"}]
    assert "messages" not in bodies[1]
