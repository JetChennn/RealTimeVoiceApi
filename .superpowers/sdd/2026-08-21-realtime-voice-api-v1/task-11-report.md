# Task 11 report: WebSocket transport

## Outcome

Task 11 now exposes `WS /v1/realtime` with a five-second configurable
`CREATE_SESSION` handshake, ordered/session-scoped inbound audio, bounded
runtime queues, stable transport errors, and policy close code 1008.
`create_app(Settings(...))` still constructs the real `SessionRuntime`; the only
new construction seam is the typed optional `RuntimeFactory` passed through
`AppServices` to `SessionRegistry` for controlled integration tests.

## Review findings closed

1. Inbound count and byte/duration overflow both map to
   `CLIENT_AUDIO_BACKPRESSURE` and close 1008. End-to-end tests block the VAD
   consumer and exercise the actual 65th item and the first audio beyond three
   seconds using the production defaults (64 items / 3 seconds).
2. Outbound count and byte saturation use non-blocking admission, raise
   `SlowClient`, send `SLOW_CLIENT` directly rather than enqueueing into the full
   queue, and close 1008. End-to-end tests block the sender and exercise the
   actual 256-item and 8 MiB defaults.
3. Long-lived sender writes and direct protocol-error/close writes absorb both
   `WebSocketDisconnect` and Starlette closed-state `RuntimeError`. The latter
   is verified while handling a runtime `ExceptionGroup`.
4. Binary handshake and active-session frames normalize to `ERROR` with
   `INVALID_MESSAGE` and close 1008.
5. Coverage includes actual inbound/outbound default-limit saturation,
   handshake timeout, all policy closes, and disconnect/closed-state writes.
6. This report records the real branch commits and fresh verification output;
   the stale nonexistent SHAs from the earlier draft were removed.

## Files

- `src/realtime_voice/main.py`
- `src/realtime_voice/session/runtime.py`
- `src/realtime_voice/transport/{factory,websocket,workers}.py`
- `tests/integration/test_websocket_{boundaries,non_text,saturation,transport_failures}.py`

## Commits

- `fe68bcdff0a5d6049dd57e22df5ac282c9219572` — initial WebSocket endpoint.
- `c6828e08a59234850bcdd1f79c97728675ff9502` — queue-overflow mapping,
  deterministic outbound admission, and disconnect handling.
- `04a06ae19d54e88d3fdd943f26c002631adccea3` — non-text frame normalization.
- `cafaf8eede96c2939a8c493477ef6fc7da6c731d` — direct queue-boundary,
  timeout, and disconnect tests.
- `1f202f16f4587988ddfa60357fece02ca4cb50b8` — typed runtime factory seam,
  end-to-end default-limit tests, policy-close assertions, and closed-write race
  hardening.

## TDD and audit evidence

Previously recorded implementation evidence (retained without inventing new
output):

- Initial RED: the focused non-create-first test disconnected because the route
  did not exist; GREEN followed the endpoint/handshake implementation.
- Initial RED: the sequence-gap test leaked an `ExceptionGroup`; GREEN followed
  stable `ProtocolViolation` mapping.
- Fix-round RED: the receiver count-overflow test leaked `asyncio.QueueFull`;
  GREEN followed `CLIENT_AUDIO_BACKPRESSURE` mapping in `c6828e0`.

Takeover evidence captured in this round:

- RED: `.venv/bin/pytest tests/integration/test_websocket_saturation.py -q`
  produced `4 failed`; every case failed with `TypeError: create_app() got an
  unexpected keyword argument 'runtime_factory'`.
- RED: the two new closed-write race tests produced `2 failed`: sender
  `RuntimeError` escaped directly, and the direct protocol-error write escaped
  while handling the runtime `ExceptionGroup`.
- GREEN: the four saturation cases plus both race cases produced `6 passed`
  after the typed seam and narrowly scoped transport-write handling.
- Binary handshake/active integration tests are characterization coverage of
  behavior introduced by `04a06ae`; no RED claim is made for those later tests.

## Fresh verification

- Task 11 focused command over all nine WebSocket integration files:
  `19 passed, 8 warnings in 5.81s`.
- `.venv/bin/pytest -q`: `210 passed, 8 warnings in 6.75s`.
- `.venv/bin/ruff check src tests`: `All checks passed!`.
- `git diff --check`: clean.
- Warnings are the existing third-party Silero importlib-resources and Torch JIT
  deprecations triggered by four real-runtime integration tests.

## Self-review

Reviewed `fe68bcdff0a5d6049dd57e22df5ac282c9219572..1f202f16f4587988ddfa60357fece02ca4cb50b8`
line by line. Production limits and protocol fields are unchanged. The injected
factory is keyword-only, typed, and absent from the wire protocol. Runtime and
error paths do not wait on or write into a saturated queue. `RuntimeError`
handling is restricted to WebSocket `send_text`/`close` calls so encoding and
queue failures remain visible.

The host patch helper could not create its nested namespace (`ENOSPC`). After
verifying the failed partial patch and its `.orig` backup contained no unique
work, the backup was removed; subsequent edits used exact-context, path-scoped
fallbacks and were diff-reviewed.

## Exact branch evidence

Captured after the final behavior commit and before this report-only commit
(the report commit necessarily cannot contain its own SHA):

```text
$ git log --oneline 33663b3..HEAD
1f202f1 fix: verify websocket saturation boundaries
cafaf8e test: cover websocket queue boundaries
04a06ae fix: normalize websocket non-text frames
c6828e0 fix: harden websocket backpressure handling
fe68bcd feat: expose realtime websocket endpoint
$ git rev-parse HEAD
1f202f16f4587988ddfa60357fece02ca4cb50b8
```
