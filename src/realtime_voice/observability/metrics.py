"""Owned Prometheus metrics for one realtime application."""

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest


class Metrics:
    def __init__(self, *, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        r = self.registry
        self.active_sessions = Gauge(
            "realtime_voice_active_sessions", "Admitted realtime sessions", registry=r
        )
        self.queue_items = Gauge("realtime_voice_queue_items", "Queue items", ["queue"], registry=r)
        self.queue_bytes = Gauge("realtime_voice_queue_bytes", "Queue bytes", ["queue"], registry=r)
        self.queue_overload = Counter(
            "realtime_voice_queue_overload", "Queue overloads", ["queue", "limit"], registry=r
        )
        self.limiter_active = Gauge(
            "realtime_voice_limiter_active", "Active limiter slots", ["service"], registry=r
        )
        self.limiter_waiting = Gauge(
            "realtime_voice_limiter_waiting", "Waiting limiter slots", ["service"], registry=r
        )
        self.executor_workers = Gauge(
            "realtime_voice_executor_workers", "CPU executor workers", registry=r
        )
        self.stage_latency = Histogram(
            "realtime_voice_stage_latency_seconds", "Stage duration", ["stage"], registry=r
        )
        self.speech_end_to_asr = Histogram(
            "realtime_voice_speech_end_to_asr_seconds", "Speech end to ASR", registry=r
        )
        self.speech_end_to_first_llm = Histogram(
            "realtime_voice_speech_end_to_first_llm_seconds", "Speech end to LLM", registry=r
        )
        self.speech_end_to_first_tts = Histogram(
            "realtime_voice_speech_end_to_first_tts_seconds", "Speech end to TTS", registry=r
        )
        self.interruptions = Counter(
            "realtime_voice_turn_interruptions", "Interrupted turns", registry=r
        )
        self.discarded_tts_chunks = Counter(
            "realtime_voice_discarded_tts_chunks", "Discarded TTS chunks", registry=r
        )
        self.discarded_tts_bytes = Counter(
            "realtime_voice_discarded_tts_bytes", "Discarded TTS bytes", registry=r
        )
        self.errors = Counter(
            "realtime_voice_errors", "Runtime errors", ["stage", "code"], registry=r
        )
        self.lifecycle_events = Counter(
            "realtime_voice_lifecycle_events", "Lifecycle events", ["event"], registry=r
        )
        self.event_loop_lag = Histogram(
            "realtime_voice_event_loop_lag_seconds", "Event-loop scheduling lag", registry=r
        )
        self.admission_wait = Histogram(
            "realtime_voice_admission_wait_seconds", "Admission wait", ["service"], registry=r
        )
        self.admission_overload = Counter(
            "realtime_voice_admission_overload", "Admission overloads", ["service"], registry=r
        )
        self.slow_client_closes = Counter(
            "realtime_voice_slow_client_close", "Slow-client closes", registry=r
        )
        self.executor_active = Gauge(
            "realtime_voice_executor_active", "Active CPU work", registry=r
        )
        self.executor_pending = Gauge(
            "realtime_voice_executor_pending", "Pending CPU work", registry=r
        )
        self.process_threads = Gauge(
            "realtime_voice_process_threads", "Process threads", registry=r
        )
        self.process_memory = Gauge(
            "realtime_voice_process_memory_bytes", "Process memory", registry=r
        )

    def set_active_sessions(self, count: int) -> None:
        self._safe(self.active_sessions.set, count)

    def set_queue_state(self, queue: str, *, items: int, byte_count: int | None = None) -> None:
        self._safe(self.queue_items.labels(queue).set, items)
        if byte_count is not None:
            self._safe(self.queue_bytes.labels(queue).set, byte_count)

    def record_queue_overload(self, queue: str, limit: str) -> None:
        self._safe(self.queue_overload.labels(queue, limit).inc)

    def set_limiter_state(self, service: str, *, active: int, waiting: int) -> None:
        self._safe(self.limiter_active.labels(service).set, active)
        self._safe(self.limiter_waiting.labels(service).set, waiting)

    def set_executor_workers(self, workers: int) -> None:
        self._safe(self.executor_workers.set, workers)

    def observe_stage_latency(self, stage: str, seconds: float) -> None:
        self._safe(self.stage_latency.labels(stage).observe, seconds)

    def observe_speech_end_to_asr(self, seconds: float) -> None:
        self._safe(self.speech_end_to_asr.observe, seconds)

    def observe_speech_end_to_first_llm(self, seconds: float) -> None:
        self._safe(self.speech_end_to_first_llm.observe, seconds)

    def observe_speech_end_to_first_tts(self, seconds: float) -> None:
        self._safe(self.speech_end_to_first_tts.observe, seconds)

    def record_interruption(self) -> None:
        self._safe(self.interruptions.inc)

    def record_discarded_tts_chunk(self, *, byte_count: int) -> None:
        self._safe(self.discarded_tts_chunks.inc)
        self._safe(self.discarded_tts_bytes.inc, byte_count)

    def record_error(self, stage: str, code: str) -> None:
        self._safe(self.errors.labels(stage, code).inc)

    def record_lifecycle_event(self, event: str) -> None:
        self._safe(self.lifecycle_events.labels(event).inc)

    def record_event_loop_lag(self, seconds: float) -> None:
        self._safe(self.event_loop_lag.observe, seconds)

    def record_admission_wait(self, service: str, seconds: float) -> None:
        self._safe(self.admission_wait.labels(service).observe, seconds)

    def record_admission_overload(self, service: str) -> None:
        self._safe(self.admission_overload.labels(service).inc)

    def record_slow_client_close(self) -> None:
        self._safe(self.slow_client_closes.inc)

    def set_executor_state(self, *, active: int, pending: int) -> None:
        self._safe(self.executor_active.set, active)
        self._safe(self.executor_pending.set, pending)

    def set_process_state(self, *, threads: int, memory_bytes: int) -> None:
        self._safe(self.process_threads.set, threads)
        self._safe(self.process_memory.set, memory_bytes)

    def render(self) -> bytes:
        return generate_latest(self.registry)

    @staticmethod
    def _safe(operation, *args) -> None:
        try:
            operation(*args)
        except Exception:  # noqa: BLE001 - metrics must not affect business behavior
            return
