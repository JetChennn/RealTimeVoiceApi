"""Application-service and per-session runtime construction for WebSocket transport."""

from __future__ import annotations

import asyncio
from typing import Any

import httpx
from fastapi import WebSocket

from realtime_voice.audio.vad import (
    BoundedDetectorOffload,
    SileroDetector,
    StreamingVadSegmenter,
    VadConfig,
    VadWorker,
)
from realtime_voice.clients.asr import AsrClient
from realtime_voice.clients.berry import BerryClient
from realtime_voice.clients.limits import BoundedAdmission
from realtime_voice.clients.tts import TtsClient
from realtime_voice.protocol.client_messages import CreateSession
from realtime_voice.session.registry import SessionRegistry
from realtime_voice.session.runtime import BoundedByteQueue, SessionRuntime
from realtime_voice.session.state import SessionState
from realtime_voice.transport.workers import WebSocketReceiver, WebSocketSender


def configure_services(services: Any) -> None:
    """Attach shared downstream clients and a registry to application services once."""
    settings = services.settings
    services.asr_client = AsrClient(
        httpx.AsyncClient(base_url=str(settings.asr_base_url), trust_env=False), BoundedAdmission("asr", 8, 64)
    )
    services.berry_client = BerryClient(
        httpx.AsyncClient(base_url=str(settings.berry_base_url), trust_env=False), BoundedAdmission("berry", 8, 64)
    )
    services.tts_client = TtsClient(
        httpx.AsyncClient(base_url=str(settings.tts_base_url), trust_env=False), BoundedAdmission("tts", 8, 64)
    )
    services.detector_offload = BoundedDetectorOffload(settings.cpu_workers)
    services.registry = SessionRegistry(
        settings.max_sessions,
        runtime_factory=lambda create, websocket: build_runtime(create, websocket, services),
    )


def build_runtime(create: CreateSession, websocket: WebSocket, services: Any) -> SessionRuntime:
    """Create the five runtime-owned workers and their bounded queues for one session."""
    settings = services.settings
    state = SessionState(
        user_id=create.device_id, session_id=create.session_id, sample_rate=create.sample_rate
    )
    events = asyncio.Queue(maxsize=settings.session_event_queue_size)
    audio = BoundedByteQueue.audio(
        maxsize=settings.session_audio_queue_size,
        max_bytes=int(create.sample_rate * 2 * settings.session_audio_queue_max_seconds),
    )
    outbound = BoundedByteQueue.outbound(
        maxsize=settings.session_outbound_queue_size,
        max_bytes=settings.session_outbound_queue_max_bytes,
    )
    runtime: SessionRuntime
    receiver = WebSocketReceiver(
        websocket, create.session_id, create.sample_rate, audio, lambda: runtime.request_close()
    )
    sender = WebSocketSender(websocket, outbound)
    vad = VadWorker(
        session_id=create.session_id,
        audio_queue=audio,
        event_queue=events,
        segmenter=StreamingVadSegmenter(VadConfig()),
        detector=SileroDetector(),
        detector_offload=services.detector_offload,
        input_sample_rate=create.sample_rate,
    )
    runtime = SessionRuntime(
        state=state,
        asr_client=services.asr_client,
        berry_client=services.berry_client,
        tts_client=services.tts_client,
        receiver=receiver,
        vad_worker=vad,
        sender=sender,
        event_queue_size=settings.session_event_queue_size,
        audio_queue_size=settings.session_audio_queue_size,
        asr_queue_size=settings.session_asr_queue_size,
        outbound_queue_size=settings.session_outbound_queue_size,
        audio_queue_max_seconds=settings.session_audio_queue_max_seconds,
        outbound_queue_max_bytes=settings.session_outbound_queue_max_bytes,
        event_queue=events,
        audio_queue=audio,
        outbound_queue=outbound,
        berry_cleanup_timeout=settings.berry_cleanup_timeout_seconds,
        tts_drain_timeout=settings.tts_drain_timeout_seconds,
    )
    return runtime
