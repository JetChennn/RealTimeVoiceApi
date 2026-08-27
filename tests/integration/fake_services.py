from __future__ import annotations

import asyncio
import base64
import contextlib
import threading
from collections import deque
from collections.abc import Iterator
from time import monotonic

from fastapi import WebSocket
from fastapi.testclient import TestClient
from starlette.testclient import WebSocketTestSession

from realtime_voice.audio.vad import SpeechSegment
from realtime_voice.clients.thinker import DeleteResult, ThinkerDone, ThinkerTextDelta
from realtime_voice.clients.tts import TtsChunk
from realtime_voice.config import Settings
from realtime_voice.main import create_app
from realtime_voice.protocol.client_messages import CreateSession
from realtime_voice.session.events import SpeechSegmentReady
from realtime_voice.session.runtime import BoundedByteQueue, SessionRuntime
from realtime_voice.session.state import SessionState
from realtime_voice.transport.workers import WebSocketReceiver, WebSocketSender


class FakeAsr:
    def __init__(self, texts: list[str], *, fail: bool = False) -> None:
        self.texts = deque(texts)
        self.fail = fail
        self.calls = 0

    async def transcribe(self, pcm16_16k: bytes) -> str:
        self.calls += 1
        if self.fail:
            raise RuntimeError("fake ASR failure")
        return self.texts.popleft() if self.texts else ""


class FakeThinker:
    def __init__(self, *, block_first: bool = False, fail: bool = False) -> None:
        self.block_first = block_first
        self.fail = fail
        self.calls: list[str] = []
        self.first_delta = threading.Event()
        self.release_first = threading.Event()
        self.first_done = threading.Event()
        self.deleted = threading.Event()

    async def stream_reply(self, request):
        self.calls.append(f"start:{request.text}")
        if self.fail:
            raise RuntimeError("fake Thinker failure")
        if self.block_first and request.text == "first":
            yield ThinkerTextDelta(delta="first-")
            self.first_delta.set()
            released = await asyncio.to_thread(self.release_first.wait, 2)
            if not released:
                raise TimeoutError("fake Thinker release timed out")
            yield ThinkerTextDelta(delta="late")
        else:
            yield ThinkerTextDelta(delta=f"reply:{request.text}")
        self.calls.append(f"done:{request.text}")
        if request.text == "first":
            self.first_done.set()
        yield ThinkerDone(reply_text=f"reply:{request.text}")

    async def interrupt(self, user_id: str, session_id: str) -> None:
        self.calls.append("interrupt")

    async def delete_session(self, user_id: str, session_id: str) -> DeleteResult:
        self.calls.append("delete")
        self.deleted.set()
        return DeleteResult.DELETED


class FakeTts:
    def __init__(self, *, block_first: bool = False, fail: bool = False) -> None:
        self.block_first = block_first
        self.fail = fail
        self.calls: list[str] = []
        self.first_started = threading.Event()
        self.release_first = threading.Event()
        self.first_drained = threading.Event()
        self.second_started = threading.Event()
        self.audio = b"\x00\x00" * 4800

    async def stream(self, request):
        turn = request.trace_id.rsplit("-", 1)[-1]
        self.calls.append(f"start:{turn}")
        if self.fail:
            raise RuntimeError("fake TTS failure")
        if self.block_first and turn == "1":
            self.first_started.set()
            yield TtsChunk(chunk_index=0, pcm16_24k=self.audio, finalize=False)
            released = await asyncio.to_thread(self.release_first.wait, 2)
            if not released:
                raise TimeoutError("fake TTS release timed out")
            yield TtsChunk(chunk_index=1, pcm16_24k=self.audio, finalize=True)
            self.first_drained.set()
            return
        if turn == "2":
            self.second_started.set()
        yield TtsChunk(chunk_index=0, pcm16_24k=self.audio, finalize=True)


class FakeVadWorker:
    def __init__(self, session_id: str, audio, events) -> None:
        self.session_id = session_id
        self.audio = audio
        self.events = events
        self.next_segment_id = 1

    async def run(self) -> None:
        while (pcm := await self.audio.get()) is not None:
            segment = SpeechSegment(self.next_segment_id, pcm)
            self.next_segment_id += 1
            await self.events.put(
                SpeechSegmentReady(
                    session_id=self.session_id,
                    segment=segment,
                    speech_end_at=monotonic(),
                )
            )


class FakeServiceHarness:
    def __init__(
        self,
        texts: list[str],
        *,
        block_first_thinker: bool = False,
        block_first_tts: bool = False,
        fail_stage: str | None = None,
    ) -> None:
        self.asr = FakeAsr(texts, fail=fail_stage == "asr")
        self.thinker = FakeThinker(block_first=block_first_thinker, fail=fail_stage == "thinker")
        self.tts = FakeTts(block_first=block_first_tts, fail=fail_stage == "tts")
        self.runtime: SessionRuntime | None = None

    def runtime_factory(self, create: CreateSession, websocket: WebSocket) -> SessionRuntime:
        events = asyncio.Queue(maxsize=256)
        audio = BoundedByteQueue.audio(maxsize=64, max_bytes=create.sample_rate * 2 * 3)
        outbound = BoundedByteQueue.outbound(maxsize=256, max_bytes=8 * 1024 * 1024)
        runtime: SessionRuntime
        receiver = WebSocketReceiver(
            websocket,
            create.session_id,
            create.sample_rate,
            audio,
            lambda: runtime.request_close(),
        )
        runtime = SessionRuntime(
            state=SessionState(create.device_id, create.session_id, create.sample_rate),
            asr_client=self.asr,
            thinker_client=self.thinker,
            tts_client=self.tts,
            receiver=receiver,
            vad_worker=FakeVadWorker(create.session_id, audio, events),
            sender=WebSocketSender(websocket, outbound),
            event_queue=events,
            audio_queue=audio,
            outbound_queue=outbound,
            thinker_cleanup_timeout=2,
            tts_drain_timeout=2,
        )
        self.runtime = runtime
        return runtime

    def app(self):
        return create_app(
            Settings(_env_file=None),
            runtime_factory=self.runtime_factory,
        )


@contextlib.contextmanager
def connected_session(
    harness: FakeServiceHarness,
    *,
    session_id: str = "session-1",
) -> Iterator[tuple[WebSocketTestSession, dict[str, object]]]:
    with (
        TestClient(harness.app()) as client,
        client.websocket_connect("/v1/realtime") as websocket,
    ):
        websocket.send_json(
            {
                "type": "CREATE_SESSION",
                "protocol_version": 1,
                "device_id": "device-1",
                "session_id": session_id,
                "audio_format": "PCM16",
                "audio_transport": "BASE64_JSON",
                "sample_rate": 16000,
                "channels": 1,
            }
        )
        yield websocket, websocket.receive_json()


def send_audio(websocket: WebSocketTestSession, sequence: int, *, session_id: str = "session-1"):
    pcm = b"\x01\x00" * 1600
    websocket.send_json(
        {
            "type": "AUDIO_CHUNK",
            "session_id": session_id,
            "sequence": sequence,
            "audio_b64": base64.b64encode(pcm).decode("ascii"),
        }
    )


def receive_until(
    websocket: WebSocketTestSession,
    message_type: str,
    *,
    turn_id: int | None = None,
    limit: int = 40,
) -> list[dict[str, object]]:
    messages: list[dict[str, object]] = []
    for _ in range(limit):
        message = websocket.receive_json()
        messages.append(message)
        if message.get("type") == message_type and (
            turn_id is None or message.get("turn_id") == turn_id
        ):
            return messages
    raise AssertionError(f"did not receive {message_type} for turn {turn_id}")
