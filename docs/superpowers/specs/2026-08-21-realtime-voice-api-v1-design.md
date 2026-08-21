# RealTimeVoiceAPI V1 Design

**Date:** 2026-08-21

**Status:** Approved in design review

## 1. Goal

Build a single-process asynchronous WebSocket gateway that accepts continuous
client audio, detects complete speech segments, calls ASR, streams a
BerryThinker reply, synthesizes that reply through PromptDialogAPI, and returns
text, state, and audio to the client. The service must support at least 30
simultaneous sessions while preserving per-session ordering and the interruption
semantics defined here.

This specification refines the repository `README.md`. Where the two differ,
this specification governs V1 implementation. In particular, V1 transports
PCM16 through Base64 JSON and defers Opus until a concrete client codec/container
requirement exists.

## 2. Scope and Constraints

V1 must:

- expose one versioned WebSocket interface and HTTP health/metrics endpoints;
- support continuous, mono PCM16 input and output at 16000, 24000, or 48000 Hz;
- carry audio bytes as Base64 inside JSON text frames;
- run VAD in this process with independent state for every session;
- call the existing ASR, BerryThinker, and PromptDialogAPI interfaces without
  changing those services;
- keep Session, Turn, task, queue, and interruption state in this process only;
- keep ASR and BerryThinker work ordered within one session while allowing
  different sessions to progress concurrently;
- continue an interrupted BerryThinker stream so its history/memory workflow
  completes;
- drain and discard an interrupted TTS stream while allowing the new turn to
  proceed;
- use bounded queues, bounded downstream concurrency, bounded CPU execution,
  and explicit timeouts;
- include unit, integration, real-service smoke, and concurrent load-test tools;
- run without Redis, Docker, Kubernetes, service discovery, or a distributed
  task queue; and
- assume trusted internal-network use and perform no authentication in V1.

V1 does not:

- support Opus, WebM, Ogg, MP3, stereo audio, or WebSocket binary audio frames;
- restore a disconnected real-time session;
- store conversation history or long-term memory in RealTimeVoiceAPI;
- cancel an already-running BerryThinker reply;
- require downstream services to accept new business parameters; or
- implement downstream instance discovery or load balancing.

## 3. Fixed Service Addresses

The defaults are configurable through environment variables:

| Service | Default |
|---|---|
| RealTimeVoiceAPI bind host | `0.0.0.0` |
| RealTimeVoiceAPI port | `8003` |
| WebSocket route | `/v1/realtime` |
| Health route | `/health` |
| Metrics route | `/metrics` |
| ASR base URL | `http://127.0.0.1:8000` |
| ASR route | `/v1/chat/completions` |
| BerryThinker base URL | `http://127.0.0.1:8082` |
| Berry reply route | `/api/v1/multimodal/reply` |
| Berry interrupt route | `/api/v1/interrupt` |
| Berry session cleanup route | `/api/v1/sessions/{user_id}/{session_id}` |
| TTS base URL | `http://127.0.0.1:8002` |
| TTS route | `/v1/dialogue-tts/stream` |

## 4. Runtime Architecture

RealTimeVoiceAPI is one FastAPI/Uvicorn process with one asyncio event loop.
Network I/O, queues, and Actor state transitions run on the event-loop thread.
CPU-bound or blocking audio operations run through a bounded executor so they
cannot block all sessions.

Application-lifetime shared resources are:

- `SessionRegistry`, which rejects a duplicate active `session_id`;
- one shared `httpx.AsyncClient` or separately tuned shared clients for ASR,
  BerryThinker, and TTS;
- one bounded concurrency limiter and one bounded waiting queue per downstream;
- one bounded CPU executor for VAD, resampling, and non-trivial audio conversion;
- application configuration and structured logging; and
- Prometheus-compatible metrics.

Every accepted WebSocket session owns a `SessionRuntime`. The runtime owns and
supervises these long-lived tasks through `asyncio.TaskGroup`:

1. **Receiver** parses client JSON, validates sequence and session identity,
   Base64-decodes PCM16, and writes `AudioChunk` values to a bounded audio queue.
2. **VAD worker** consumes audio continuously, performs streaming resampling to
   16 kHz, runs per-session VAD, accumulates speech, and emits a completed
   `SpeechSegmentReady` event.
3. **ASR worker** consumes one session's speech-segment queue sequentially and
   emits `AsrCompleted` or `AsrFailed` events.
4. **Session Actor** is the sole writer of Session/Turn business state and
   controls turn creation, downstream launch gates, interruption, and cleanup
   decisions.
5. **Sender** serializes internal outbound messages and sends them from a bounded
   queue without blocking the Actor on a slow WebSocket.

The `SessionRuntime` owns task lifetime; the `SessionActor` owns business state.
Background ASR, BerryThinker, and TTS tasks never mutate Session state directly.
They emit typed events carrying `session_id`, `segment_id` or `turn_id`, and a
task generation. The Actor rejects an event whose identity or generation is no
longer current.

## 5. Proposed Source Boundaries

Implementation should use focused modules with these responsibilities:

```text
src/realtime_voice/
├── main.py                 # FastAPI routes and application lifespan
├── config.py               # validated environment configuration
├── protocol/
│   ├── client_messages.py  # V1 inbound Pydantic models
│   ├── server_messages.py  # V1 outbound Pydantic models
│   ├── decoder.py          # wire JSON/Base64 -> internal commands
│   ├── encoder.py          # internal messages -> V1 JSON
│   └── errors.py           # stable protocol error codes
├── transport/
│   └── websocket.py        # handshake, receiver, sender, close handling
├── audio/
│   ├── pcm.py              # PCM16 validation and WAV serialization
│   ├── resampler.py        # stateful streaming resampling
│   └── vad.py              # per-session Silero state and segment emission
├── session/
│   ├── events.py           # immutable internal event types
│   ├── state.py            # SessionState and TurnContext
│   ├── actor.py            # serialized state transitions
│   ├── runtime.py          # TaskGroup and lifecycle supervision
│   └── registry.py         # active-session registry and capacity
├── clients/
│   ├── asr.py              # OpenAI-compatible ASR call and parser
│   ├── berry.py            # multipart Berry stream/interrupt/cleanup
│   ├── tts.py              # dialogue TTS NDJSON stream parser
│   └── limits.py           # bounded admission and concurrency controls
└── observability/
    ├── logging.py          # structured context and event logging
    └── metrics.py          # counters, gauges, and latency histograms
```

Wire models must not leak into audio, session, or downstream-client modules.
This makes a future binary or Opus protocol an adapter change rather than an
Actor/VAD/ASR rewrite.

## 6. V1 WebSocket Protocol

The WebSocket carries JSON text frames only. All server-to-client messages have
the common fields `type`, `user_id`, `session_id`, `turn_id`, and `interrupt`.
Session-level messages use `turn_id=0` and `interrupt=false`.

### 6.1 Session creation

The first client message must arrive within five seconds:

```json
{
  "type": "CREATE_SESSION",
  "protocol_version": 1,
  "device_id": "device-01",
  "session_id": "session-100",
  "audio_format": "PCM16",
  "audio_transport": "BASE64_JSON",
  "sample_rate": 16000,
  "channels": 1
}
```

The server accepts only protocol version 1, `PCM16`, `BASE64_JSON`, mono audio,
and sample rates 16000, 24000, or 48000. `user_id` is the submitted `device_id`.

Success response:

```json
{
  "type": "SESSION_CREATED",
  "protocol_version": 1,
  "user_id": "device-01",
  "session_id": "session-100",
  "turn_id": 0,
  "interrupt": false,
  "audio_format": "PCM16",
  "audio_transport": "BASE64_JSON",
  "sample_rate": 16000,
  "channels": 1
}
```

### 6.2 Client audio

```json
{
  "type": "AUDIO_CHUNK",
  "session_id": "session-100",
  "sequence": 0,
  "timestamp_ms": 1787306400123,
  "audio_b64": "AAA0Aaj/cgI="
}
```

`sequence` starts at zero and increases by exactly one. `timestamp_ms` is
optional and used for logging/latency only. WebSocket order, not client time,
defines ingestion order. Decoded PCM must be non-empty and have an even byte
length. A chunk must represent between 10 and 500 ms; clients should send
40-100 ms chunks. Inbound audio has no `turn_id` because a turn exists only
after non-empty ASR.

### 6.3 ASR result

```json
{
  "type": "ASR_RESULT",
  "user_id": "device-01",
  "session_id": "session-100",
  "turn_id": 1,
  "interrupt": false,
  "text": "今天天气怎么样？"
}
```

Empty ASR text creates no turn and produces no `ASR_RESULT`.

### 6.4 LLM text

```json
{
  "type": "TEXT_DELTA",
  "user_id": "device-01",
  "session_id": "session-100",
  "turn_id": 1,
  "interrupt": false,
  "delta": "今天天气"
}
```

```json
{
  "type": "TEXT_END",
  "user_id": "device-01",
  "session_id": "session-100",
  "turn_id": 1,
  "interrupt": false,
  "text": "今天天气不错，适合出去走走。"
}
```

An interrupted Berry stream continues forwarding text with
`interrupt=true`.

### 6.5 TTS audio

```json
{
  "type": "AUDIO_DELTA",
  "user_id": "device-01",
  "session_id": "session-100",
  "turn_id": 1,
  "interrupt": false,
  "sequence": 0,
  "audio_format": "PCM16",
  "sample_rate": 16000,
  "channels": 1,
  "audio_b64": "AABx/5gB9v6K"
}
```

Audio sequence starts at zero for every turn. An interrupted turn emits no more
`AUDIO_DELTA` messages.

### 6.6 State, completion, and errors

Immediate interruption notification:

```json
{
  "type": "TURN_STATE",
  "user_id": "device-01",
  "session_id": "session-100",
  "turn_id": 1,
  "interrupt": true,
  "state": "INTERRUPTED"
}
```

Turn completion:

```json
{
  "type": "RESPONSE_END",
  "user_id": "device-01",
  "session_id": "session-100",
  "turn_id": 1,
  "interrupt": false,
  "status": "COMPLETED"
}
```

`status` is `COMPLETED`, `INTERRUPTED`, or `FAILED`.

Error shape:

```json
{
  "type": "ERROR",
  "user_id": "device-01",
  "session_id": "session-100",
  "turn_id": 1,
  "interrupt": false,
  "stage": "TTS",
  "code": "TTS_STREAM_FAILED",
  "message": "TTS stream returned an internal error",
  "recoverable": true
}
```

Errors before turn creation use `turn_id=0`. The client may request graceful
closure with `{"type":"CLOSE_SESSION","session_id":"session-100"}`.

Breaking wire changes require `/v2/realtime` or an explicitly supported newer
`protocol_version`. Additive optional fields and new message types may remain
within V1 when old clients can safely ignore them.

## 7. Audio and VAD Pipeline

Inbound processing is:

```text
Base64(PCM16 at session rate)
-> validated PCM16 bytes
-> stateful resampling to 16 kHz
-> normalized mono float32
-> per-session Silero VAD
-> complete 16 kHz speech segment
```

VAD defaults are:

| Setting | Default |
|---|---:|
| Internal sample rate | 16000 Hz |
| Threshold | 0.5 |
| Minimum ending silence | 500 ms |
| Maximum speech segment | 30 seconds |

The threshold, ending silence, maximum duration, model path, and CPU concurrency
are validated configuration. Silero weights must be local at runtime; startup
must not fetch a model over the network. The current host already has TorchScript
and ONNX Silero weights, but implementation must use an installed dependency or
configured model path rather than hard-code the user's cache location.

VAD state, streaming resampler state, accumulated speech bytes, silence time,
and `segment_id` are session-local. A speech segment longer than 30 seconds is
forced closed to bound memory.

Completed 16 kHz segments are converted to PCM16 WAV for ASR and BerryThinker.
TTS returns 24 kHz little-endian PCM16. Each TTS turn owns a streaming resampler
that converts 24 kHz to the negotiated session rate without resetting filter
state at every NDJSON chunk. A 24 kHz session bypasses output resampling.

## 8. Downstream Contracts

### 8.1 ASR

Call `POST /v1/chat/completions` with the existing OpenAI-compatible input-audio
request. `input_audio.data` is Base64 of the full WAV file and
`input_audio.format` is `wav`.

Parse `choices[0].message.content` using the behavior referenced by
`BerryThinker/mio_core/tools/qwen_asr_utils.py`: extract `<asr_text>`, strip
whitespace, and treat empty, `none`, `null`, and `undefined` as empty. Do not
depend on synchronous `requests` or a fixed URL copied from BerryThinker.

One session's ASR worker processes segments sequentially. Different sessions
share global admission/concurrency limits and may run concurrently.

### 8.2 BerryThinker

Call `POST /api/v1/multimodal/reply` as multipart form data:

| Field | Value |
|---|---|
| `text` | cleaned ASR text |
| `audio` | complete 16 kHz PCM16 WAV segment |
| `user_id` | `device_id` |
| `session_id` | real-time session ID |
| `stream` | `true` |
| `reply_mode` | `dialogue` |
| `audio_is_vad_segment` | `true` |
| `skip_internal_asr` | `true` |

Parse the NDJSON stream incrementally across arbitrary HTTP chunk boundaries.
Forward `text_delta` events and use the `done.output.reply_text` value as the
authoritative final reply.

When a newer valid turn interrupts an older one, allow the older Berry stream to
finish. After it finishes and before starting the next queued Berry request,
call `POST /api/v1/interrupt` with the existing `user_id` and `session_id` JSON.

### 8.3 PromptDialogAPI

Call `POST /v1/dialogue-tts/stream` with:

```json
{
  "user_input": "cleaned ASR text",
  "model_reply": "complete BerryThinker reply",
  "include_prompt_event": false,
  "trace_id": "device-01/session-100/turn-1"
}
```

Parse NDJSON audio events, Base64-decode `audio_i16le_b64`, verify a 24000 Hz
sample rate, and emit resampled V1 `AUDIO_DELTA` messages. Treat an NDJSON
`error` object as a failed TTS stream even when the HTTP status is already 200.

## 9. Actor State and Ordering

Session states are `CONNECTING`, `ACTIVE`, `CLOSING`, and `CLOSED`. Only
`ACTIVE` accepts audio.

A speech segment is not a turn. It receives a private monotonically increasing
`segment_id`. A turn is created only after that segment's ASR succeeds with
non-empty text. Public `turn_id` starts at one and increases within the session.

Turn processing stages are:

```text
WAITING_LLM
-> LLM_STREAMING
-> LLM_COMPLETED
-> TTS_STREAMING or COMPLETED
-> TTS_DRAINING when interrupted during TTS
-> COMPLETED
```

`interrupted` is an independent, monotonic Boolean flag rather than a stage.
Once true it never returns to false. This permits an interrupted turn to remain
in `LLM_STREAMING` or `TTS_DRAINING` while all of its later messages truthfully
carry `interrupt=true`.

The Actor enforces these launch gates:

- only `SpeechSegmentReady` may enqueue ASR;
- only non-empty `AsrCompleted` may allocate a turn;
- only the head of the per-session LLM queue may call BerryThinker;
- only a completed, non-interrupted Berry reply may start TTS;
- only audio for a known non-interrupted TTS turn may enter the outbound queue;
- a stale session, turn, segment, or task generation is discarded and counted.

If turns 1, 2, and 3 arrive rapidly, BerryThinker processes all three in order so
its short-term history remains complete. Turns 1 and 2 continue streaming text
with `interrupt=true` and skip TTS. Turn 3 starts TTS only if no later valid turn
interrupts it.

## 10. Interruption Semantics

VAD speech start never interrupts. Empty ASR never interrupts. A non-empty ASR
result creates a new turn and atomically marks all older unfinished turns
interrupted. The Actor immediately emits one `TURN_STATE` event for each turn
newly marked interrupted.

For an interrupted LLM turn:

- do not cancel BerryThinker;
- continue forwarding text with `interrupt=true`;
- wait for the normal `done` event;
- skip TTS;
- emit `RESPONSE_END(status=INTERRUPTED)`; and
- serialize the required Berry interrupt call before the next Berry request.

For an interrupted TTS turn:

- immediately stop emitting its audio;
- continue reading and discarding its HTTP stream;
- cap draining with an explicit timeout;
- do not make the new LLM/TTS wait for old TTS drain; and
- emit interrupted completion when the old stream finishes or its drain timeout
  closes the response.

## 11. Errors, Backpressure, and Timeouts

Stage behavior is:

- ASR failure emits a recoverable `ERROR` with `turn_id=0`, creates no turn, and
  does not interrupt an older turn.
- Berry failure emits `ERROR(stage=LLM)` and
  `RESPONSE_END(status=FAILED)`; later turns continue.
- TTS failure retains already-sent text, emits `ERROR(stage=TTS)` and
  `RESPONSE_END(status=FAILED)`; later turns continue.
- A full downstream admission queue fails the affected segment/turn with
  `SERVICE_OVERLOADED`; it never creates an unbounded task.

Initial validated defaults are:

| Limit/timeout | Default |
|---|---:|
| Active sessions | 64 |
| Handshake timeout | 5 s |
| WebSocket JSON message | 1 MiB |
| Actor event queue | 256 events |
| Audio queue | 64 messages and no more than 3 s of audio |
| Outbound queue | 256 messages and no more than 8 MiB |
| Buffered inbound audio | 3 s |
| Audio chunk duration | 10-500 ms |
| Speech segment | 30 s |
| CPU executor workers | 4 |
| CPU executor pending work | 128 jobs |
| ASR active requests / waiting jobs | 8 / 64 |
| Berry active requests / waiting jobs | 8 / 64 |
| TTS active requests / waiting jobs | 8 / 64 |
| ASR request | 30 s |
| Berry total request | 180 s |
| TTS first audio | 60 s |
| TTS stream idle | 30 s |
| Interrupted TTS drain | 120 s |
| Disconnect wait for active Berry | 120 s |

Outbound buffering has both message-count and byte-size limits so a single slow
client cannot retain unbounded audio. Persistent inbound backlog causes
`CLIENT_AUDIO_BACKPRESSURE` and connection closure. Persistent outbound backlog
causes `SLOW_CLIENT` and connection closure.

Downstream maximum concurrency and queue depths are individually configurable.
The table values are V1 defaults and are visible through health/metrics so a
deployment can tune them to actual ASR/TTS instance counts.

## 12. Disconnect and Cleanup

V1 does not resume a disconnected session. A reconnect must use a new
`session_id`; a duplicate active ID is rejected.

Cleanup order is:

1. mark the Session `CLOSING` and reject further audio;
2. stop the Receiver and VAD worker and discard an incomplete speech segment;
3. cancel queued and active ASR work, which has no Berry history side effect;
4. stop all WebSocket output;
5. allow active Berry streams to finish for at most 120 seconds;
6. drain active TTS streams without output for at most 120 seconds;
7. if Berry work finished, call
   `DELETE /api/v1/sessions/{user_id}/{session_id}`;
8. treat Berry cleanup HTTP 200 and 404 as success;
9. if Berry did not finish in time, skip DELETE and log a warning so its own
   bounded Agent pool may evict the session safely;
10. close responses/tasks, clear queues/state, remove the registry entry, and
    mark the Session `CLOSED`.

The verified Berry DELETE implementation removes and closes its in-process
Session Agent. This drops Berry's short-term in-memory history for that session
but does not execute Mem0, SQLite, event-memory, or user-profile deletion. That
effect is compatible with V1 because disconnected sessions cannot resume.

## 13. Observability

Structured log records include `user_id`, `session_id`, `turn_id`,
`segment_id`, `stage`, `event`, `duration_ms`, `queue_wait_ms`, `interrupt`, and
`error_code` where applicable. Audio bytes and full dialogue text must not be
logged by default.

`GET /health` reports process readiness, active-session count, configured
capacity, queue/limiter utilization, CPU-executor status, and cached downstream
health status. Health checks must not synchronously block for every downstream
request; downstream probes run with short timeouts and cached results.

`GET /metrics` exposes at least:

- active WebSockets and sessions;
- event-loop lag;
- queue sizes, admissions, overloads, and waits;
- VAD, ASR, Berry, and TTS latency/error counts;
- speech-end to first ASR result, first LLM delta, and first TTS audio latency;
- interrupted turns and discarded TTS chunks/bytes;
- slow-client closures; and
- executor activity, process threads, and process memory.

## 14. Test and Verification Strategy

Development is test-driven. Protocol, state-machine, and client behavior are
tested without real model services by using deterministic fake HTTP services.

Unit tests cover:

- every V1 message model, common outbound fields, version checks, Base64/PCM
  validation, sequence checks, and stable error codes;
- 16/24/48 kHz input conversion, streaming-resampler continuity, PCM/WAV
  serialization, VAD silence/noise/speech/forced-cut behavior, and session
  isolation;
- empty ASR, turn allocation, ASR/LLM order, rapid turns, LLM interruption, TTS
  drain, stale generations, all failures, and cleanup timeouts;
- ASR response cleanup and NDJSON parsing across arbitrary HTTP chunk splits;
  and
- TTS prompt/audio/error events including an error after HTTP 200.

Integration tests run RealTimeVoiceAPI against fake ASR, BerryThinker, and TTS
applications and cover:

- one complete VAD-ASR-LLM-TTS exchange;
- interruption during LLM;
- interruption during TTS where the new TTS does not wait for old drain;
- invalid/slow clients, queue saturation, downstream failure and timeout; and
- disconnect ordering and conditional Berry DELETE.

An optional real-service smoke client targets ports 8000, 8082, and 8002. It
sends a fixture WAV, verifies `ASR_RESULT`, `TEXT_DELTA`/`TEXT_END`, and
`AUDIO_DELTA`, then writes the returned PCM to a WAV for listening. It is not in
the default test suite because it depends on running models and GPU capacity.

A load-test script drives 30, 40, and 60 concurrent WebSockets and records
connection success, event-loop lag, stage latency, queue waiting, first-audio
latency, error rate, memory, and thread count. Acceptance requires:

- at least 30 concurrent clients can upload and receive streaming output;
- no unbounded queue, thread creation, or sustained memory growth;
- no sustained event-loop blocking attributable to VAD/resampling;
- empty ASR never creates or interrupts a turn;
- per-session Berry order and public `turn_id` remain consistent;
- all downlink messages carry correct `interrupt` values;
- old Berry replies complete, old TTS audio is not emitted, and new TTS does not
  wait for old drain; and
- when downstream capacity is sufficient, speech end to first valid TTS audio
  is no more than five seconds.

## 15. Deliverables

Implementation will add:

- `pyproject.toml` and a reproducible dependency/quality configuration;
- the `src/realtime_voice` service described above;
- unit and fake-service integration tests;
- a documented example WebSocket client;
- a concurrent load-test tool;
- `.env.example` with every operational setting;
- an updated top-level `README.md` containing protocol, startup, smoke-test, and
  deployment instructions; and
- supervisord and/or systemd example configuration without changing the user's
  machine service state.

