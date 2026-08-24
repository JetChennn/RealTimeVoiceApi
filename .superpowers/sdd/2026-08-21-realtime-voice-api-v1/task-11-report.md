# Task 11 report: WebSocket transport

## Implementation

- Added `WS /v1/realtime`, preserving `create_app(settings)` and `/health`.
- Added shared app services, registry-backed session construction, runtime-managed WebSocket receiver/sender workers, negotiated `SESSION_CREATED`, ordered audio/session validation, and mapping audio byte-budget overload to `CLIENT_AUDIO_BACKPRESSURE`.
- The runtime remains the sole owner of Receiver, VAD, ASR, Actor, and Sender in its TaskGroup. The sender is the only long-running text-frame writer.

## Files

- `src/realtime_voice/main.py`
- `src/realtime_voice/transport/{__init__,factory,messages,websocket,workers}.py`
- `tests/integration/test_websocket_{protocol,backpressure,backpressure_bytes,validation}.py`

## TDD evidence

- RED: `.venv/bin/pytest tests/integration/test_websocket_protocol.py::test_websocket_rejects_a_non_create_first_message -q` failed with `WebSocketDisconnect` because `/v1/realtime` did not exist.
- GREEN: the same command passed after route/handshake implementation.
- RED: `.venv/bin/pytest tests/integration/test_websocket_backpressure.py::test_websocket_rejects_an_audio_sequence_gap_after_negotiation -q` failed with an unhandled `ExceptionGroup` containing `AUDIO_SEQUENCE_GAP`.
- GREEN: the same command passed after mapping runtime receiver `ProtocolViolation` to an `ERROR` frame and close code 1008.

## Verification

- Focused integration: `4 passed` (six third-party Silero/Torch deprecation warnings).
- Full suite: `.venv/bin/pytest -q` → `195 passed` (the same six third-party warnings).
- `.venv/bin/ruff check src tests` → `All checks passed!`.
- `git diff --check` → clean.

## Self-review and concerns

- Reviewed the full diff: inbound audio uses `BoundedByteQueue` admission, sequence/session mismatches use stable protocol errors, and `CLOSE_SESSION` requests orderly runtime shutdown.
- The configured Silero package emits six upstream deprecation warnings during real TestClient session tests. Also, this host has `max_user_namespaces=0`, so the required `apply_patch` helper could not update existing files; a narrowly-scoped `patch` fallback was used after capturing the error.
- Committed as `defca19 feat: expose realtime websocket endpoint`.
