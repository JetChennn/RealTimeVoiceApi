"""Application-service and per-session runtime construction for WebSocket transport."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

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
from realtime_voice.clients.limits import BoundedAdmission
from realtime_voice.clients.thinker import ThinkerClient
from realtime_voice.clients.tts import TtsClient
from realtime_voice.protocol.client_messages import CreateSession
from realtime_voice.session.registry import SessionRegistry
from realtime_voice.session.runtime import BoundedByteQueue, SessionRuntime
from realtime_voice.session.state import SessionState
from realtime_voice.speaker.gate import SessionSpeakerGate
from realtime_voice.speaker.wespeaker import WeSpeakerOnnxEmbedder
from realtime_voice.transport.workers import WebSocketReceiver, WebSocketSender

if TYPE_CHECKING:
    from realtime_voice.main import AppServices


def configure_services(services: AppServices) -> None:
    """Attach shared downstream clients and a registry to application services once."""
    settings = services.settings
    services.asr_client = AsrClient(
        httpx.AsyncClient(base_url=str(settings.asr_base_url), trust_env=False),
        BoundedAdmission(
            "asr",
            settings.asr_concurrency,
            settings.asr_max_waiters,
            metrics=services.metrics,
        ),
    )
    services.thinker_client = ThinkerClient(
        httpx.AsyncClient(base_url=str(settings.thinker_base_url), trust_env=False),
        BoundedAdmission(
            "thinker",
            settings.thinker_concurrency,
            settings.thinker_max_waiters,
            metrics=services.metrics,
        ),
        stream_timeout=settings.thinker_stream_timeout_seconds,
        reply_total_timeout=settings.thinker_reply_total_timeout_seconds,
    )
    services.tts_client = TtsClient(
        httpx.AsyncClient(base_url=str(settings.tts_base_url), trust_env=False),
        BoundedAdmission(
            "tts",
            settings.tts_concurrency,
            settings.tts_max_waiters,
            metrics=services.metrics,
        ),
        first_audio_timeout=settings.tts_first_audio_timeout_seconds,
        idle_timeout=settings.tts_idle_timeout_seconds,
    )
    services.detector_offload = BoundedDetectorOffload(
        settings.cpu_workers, metrics=services.metrics
    )
    services.speaker_embedder = (
        WeSpeakerOnnxEmbedder(
            settings.speaker_model_path,
            concurrency=settings.speaker_concurrency,
            max_waiters=settings.speaker_max_waiters,
            metrics=services.metrics,
        )
        if settings.speaker_verification_enabled
        else None
    )
    runtime_factory = services.runtime_factory
    if runtime_factory is None:
        runtime_factory = lambda create, websocket: build_runtime(create, websocket, services)
    services.registry = SessionRegistry(
        settings.max_sessions,
        runtime_factory=runtime_factory,
    )


def build_runtime(
    create: CreateSession, websocket: WebSocket, services: AppServices
) -> SessionRuntime:
    """Create the five runtime-owned workers and their bounded queues for one session."""
    settings = services.settings
    state = SessionState(
        user_id=create.device_id, session_id=create.session_id, sample_rate=create.sample_rate
    )
    events = asyncio.Queue(maxsize=settings.session_event_queue_size)
    audio = BoundedByteQueue.audio(
        maxsize=settings.session_audio_queue_size,
        max_bytes=int(create.sample_rate * 2 * settings.session_audio_queue_max_seconds),
        metrics=services.metrics,
    )
    outbound = BoundedByteQueue.outbound(
        maxsize=settings.session_outbound_queue_size,
        max_bytes=settings.session_outbound_queue_max_bytes,
        metrics=services.metrics,
    )
    runtime: SessionRuntime
    receiver = WebSocketReceiver(
        websocket,
        create.session_id,
        create.sample_rate,
        audio,
        lambda: runtime.request_close(),
        lambda registration: runtime.register_speaker(registration),
    )
    sender = WebSocketSender(websocket, outbound)
    turn_end_audio = None
    if settings.turn_end_semantic_enabled:
        from realtime_voice.turn_end.audio import TurnEndAudio

        if services.semantic_detector is None:
            raise RuntimeError("semantic model has not completed startup")
        turn_end_audio = TurnEndAudio(create.session_id, settings)
    vad = VadWorker(
        turn_end_audio=turn_end_audio,
        session_id=create.session_id,
        audio_queue=audio,
        event_queue=events,
        segmenter=StreamingVadSegmenter(VadConfig(min_speech_ms=settings.vad_min_speech_ms)),
        detector=SileroDetector(),
        detector_offload=services.detector_offload,
        input_sample_rate=create.sample_rate,
        metrics=services.metrics,
    )
    runtime = SessionRuntime(
        turn_end_settings=settings,
        semantic_detector=services.semantic_detector,
        state=state,
        asr_client=services.asr_client,
        rag_enabled=create.rag_enabled,
        scenes=tuple(create.scenes),
        thinker_client=services.thinker_client,
        tts_client=services.tts_client,
        receiver=receiver,
        vad_worker=vad,
        sender=sender,
        metrics=services.metrics,
        event_queue_size=settings.session_event_queue_size,
        audio_queue_size=settings.session_audio_queue_size,
        asr_queue_size=settings.session_asr_queue_size,
        outbound_queue_size=settings.session_outbound_queue_size,
        audio_queue_max_seconds=settings.session_audio_queue_max_seconds,
        outbound_queue_max_bytes=settings.session_outbound_queue_max_bytes,
        event_queue=events,
        audio_queue=audio,
        outbound_queue=outbound,
        thinker_cleanup_timeout=settings.thinker_cleanup_timeout_seconds,
        thinker_stream_timeout=settings.thinker_stream_timeout_seconds,
        thinker_reply_total_timeout=settings.thinker_reply_total_timeout_seconds,
        thinker_fallback_enabled=settings.thinker_fallback_enabled,
        thinker_fallback_texts=(
            settings.thinker_fallback_text_1,
            settings.thinker_fallback_text_2,
            settings.thinker_fallback_text_3,
        ),
        thinker_fallback_tone=settings.thinker_fallback_tone,
        tts_drain_timeout=settings.tts_drain_timeout_seconds,
        tts_prompt_override=settings.tts_prompt_override,
        slow_stage_warning_seconds=settings.slow_stage_warning_seconds,
        speaker_gate=(
            SessionSpeakerGate(settings, services.speaker_embedder)
            if settings.speaker_verification_enabled
            else None
        ),
    )
    return runtime
