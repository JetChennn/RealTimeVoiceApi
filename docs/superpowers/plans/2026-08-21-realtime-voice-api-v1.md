# RealTimeVoiceAPI V1 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现一个支持 30+ 并发 Session 的实时语音 WebSocket 网关，按 VAD→ASR→BerryThinker→TTS 顺序处理 PCM16/Base64 音频，并正确处理流式文本、音频、打断和断线清理。

**Architecture:** 使用单进程 FastAPI/asyncio；每个 Session 由 `SessionRuntime` 管理 Receiver、VAD、ASR、Actor、Sender 协程，`SessionActor` 是业务状态唯一写入者。三个下游共享异步 HTTP 连接池和有界准入器，VAD/重采样通过有界 CPU 执行器隔离。

**Tech Stack:** Python 3.11+、FastAPI、Uvicorn、httpx、Pydantic v2/pydantic-settings、NumPy、soxr、Silero VAD、Prometheus client、pytest/pytest-asyncio

**Spec:** `docs/superpowers/specs/2026-08-21-realtime-voice-api-v1-design.md`

## Global Constraints

- 默认监听 `0.0.0.0:8003`，WebSocket 路径为 `/v1/realtime`。
- ASR 默认地址为 `http://127.0.0.1:8000/v1/chat/completions`。
- BerryThinker 默认地址为 `http://127.0.0.1:8082`。
- TTS 默认地址为 `http://127.0.0.1:8002/v1/dialogue-tts/stream`。
- V1 只支持 `PCM16 + BASE64_JSON + mono`，采样率为 16000、24000、48000 Hz。
- 同一 Session 的 ASR 和 BerryThinker 严格串行；不同 Session 可以并发。
- VAD 开始说话和 ASR 空文本不得触发打断。
- 被打断 Berry 流必须完成；被打断 TTS 流必须有界排空并丢弃。
- Session 断线后不恢复；安全完成 Berry 后才调用 DELETE Session。
- 默认活动 Session 上限 64；CPU worker 4、等待 128。
- ASR/Berry/TTS 默认活动请求 8、等待任务 64。
- 所有队列和缓存必须同时具备明确容量与过载行为。
- 不引入 Redis、Docker、Kubernetes、鉴权或下游服务改造。
- 所有功能代码遵循测试驱动：先看见目标测试失败，再写最小实现。

---

### Task 1: 项目骨架、配置与应用生命周期

**Files:**
- Create: `pyproject.toml`
- Create: `src/realtime_voice/__init__.py`
- Create: `src/realtime_voice/config.py`
- Create: `src/realtime_voice/main.py`
- Create: `tests/conftest.py`
- Create: `tests/helpers.py`
- Create: `tests/unit/test_config.py`
- Create: `tests/unit/test_main.py`

**Interfaces:**
- Produces: `Settings()`，集中提供所有 `RTVA_*` 环境配置。
- Produces: `create_app(settings: Settings | None = None) -> FastAPI`。
- Produces: `GET /health`，初始返回进程状态和零个活动 Session。
- Produces: `tests.helpers` 中复用的 PCM/WAV 与 httpx 流测试工具。

- [ ] **Step 1: 写失败的配置测试**

```python
def test_settings_defaults():
    settings = Settings(_env_file=None)
    assert settings.host == "0.0.0.0"
    assert settings.port == 8003
    assert str(settings.asr_base_url).rstrip("/") == "http://127.0.0.1:8000"
    assert str(settings.berry_base_url).rstrip("/") == "http://127.0.0.1:8082"
    assert str(settings.tts_base_url).rstrip("/") == "http://127.0.0.1:8002"
    assert settings.allowed_sample_rates == (16000, 24000, 48000)
    assert settings.max_sessions == 64
    assert settings.cpu_workers == 4
```

- [ ] **Step 2: 运行测试并确认失败**

Run: `pytest tests/unit/test_config.py -q`

Expected: FAIL，原因是 `realtime_voice.config` 尚不存在。

- [ ] **Step 3: 创建项目依赖和配置模型**

```toml
[build-system]
requires = ["hatchling>=1.27,<2"]
build-backend = "hatchling.build"

[project]
name = "realtime-voice-api"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
  "fastapi>=0.115,<1",
  "uvicorn[standard]>=0.34,<1",
  "httpx>=0.28,<1",
  "pydantic>=2.10,<3",
  "pydantic-settings>=2.7,<3",
  "numpy>=1.26,<3",
  "soxr>=0.5,<1",
  "torch>=2,<3",
  "silero-vad>=6,<7",
  "prometheus-client>=0.21,<1",
]

[project.optional-dependencies]
dev = [
  "pytest>=8.3,<9",
  "pytest-asyncio>=0.25,<1",
  "ruff>=0.9,<1",
  "websockets>=14,<16",
]

[tool.pytest.ini_options]
pythonpath = ["src"]
asyncio_mode = "auto"

[tool.ruff]
line-length = 100
target-version = "py311"
```

```python
class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="RTVA_", env_file=".env", extra="ignore", validate_default=True
    )

    host: str = "0.0.0.0"
    port: int = Field(default=8003, ge=1, le=65535)
    asr_base_url: AnyHttpUrl = "http://127.0.0.1:8000"
    berry_base_url: AnyHttpUrl = "http://127.0.0.1:8082"
    tts_base_url: AnyHttpUrl = "http://127.0.0.1:8002"
    allowed_sample_rates: tuple[int, ...] = (16000, 24000, 48000)
    max_sessions: int = Field(default=64, ge=1)
    cpu_workers: int = Field(default=4, ge=1)
    cpu_pending_jobs: int = Field(default=128, ge=1)
    handshake_timeout_seconds: float = Field(default=5.0, gt=0)
```

在 `tests/helpers.py` 中定义后续任务使用的真实辅助函数，避免测试引用隐含接口：

```python
def pcm_chunk(sample_rate: int, duration_ms: int, value: int = 0) -> bytes:
    samples = round(sample_rate * duration_ms / 1000)
    return np.full(samples, value, dtype="<i2").tobytes()

def sine_pcm16(sample_rate: int, seconds: float, frequency: float) -> bytes:
    count = round(sample_rate * seconds)
    time_axis = np.arange(count, dtype=np.float64) / sample_rate
    samples = np.sin(2 * np.pi * frequency * time_axis) * 0.5
    return np.rint(samples * 32767).astype("<i2").tobytes()

def valid_wav() -> bytes:
    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(16000)
        wav.writeframes(pcm_chunk(16000, 100))
    return buffer.getvalue()

class ChunkStream(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes]):
        self._chunks = chunks

    async def __aiter__(self):
        for chunk in self._chunks:
            yield chunk

    async def aclose(self) -> None:
        return None

def stream_transport(chunks: list[bytes], status_code: int = 200):
    captured: dict[str, httpx.Request] = {}
    def handler(request: httpx.Request) -> httpx.Response:
        captured["request"] = request
        return httpx.Response(status_code, stream=ChunkStream(chunks))
    return httpx.MockTransport(handler), captured
```

- [ ] **Step 4: 写应用工厂与健康检查测试**

```python
def test_health_reports_ready():
    with TestClient(create_app(Settings(_env_file=None))) as client:
        response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert response.json()["active_sessions"] == 0
```

- [ ] **Step 5: 实现应用工厂并运行测试**

```python
def create_app(settings: Settings | None = None) -> FastAPI:
    resolved = settings or Settings()
    app = FastAPI(title="RealTimeVoiceAPI", version="1.0.0")
    app.state.settings = resolved

    @app.get("/health")
    async def health() -> dict[str, object]:
        return {"status": "ok", "active_sessions": 0, "max_sessions": resolved.max_sessions}

    return app

app = create_app()
```

Run: `pytest tests/unit/test_config.py tests/unit/test_main.py -q`

Expected: PASS。

- [ ] **Step 6: 提交**

```bash
git add pyproject.toml src/realtime_voice tests/conftest.py tests/unit/test_config.py tests/unit/test_main.py
git commit -m "feat: scaffold realtime voice service"
```

### Task 2: V1 协议模型与编解码边界

**Files:**
- Create: `src/realtime_voice/protocol/__init__.py`
- Create: `src/realtime_voice/protocol/client_messages.py`
- Create: `src/realtime_voice/protocol/server_messages.py`
- Create: `src/realtime_voice/protocol/decoder.py`
- Create: `src/realtime_voice/protocol/encoder.py`
- Create: `src/realtime_voice/protocol/errors.py`
- Create: `tests/unit/protocol/test_client_messages.py`
- Create: `tests/unit/protocol/test_server_messages.py`
- Create: `tests/unit/protocol/test_codec.py`

**Interfaces:**
- Produces: `CreateSession`、`AudioChunkMessage`、`CloseSession`。
- Produces: `decode_client_message(raw: str) -> ClientMessage`。
- Produces: `decode_pcm16(message: AudioChunkMessage, sample_rate: int) -> DecodedAudioChunk`。
- Produces: `encode_server_message(message: ServerMessage) -> str`。
- Produces: 所有下行消息的公共字段模型。

- [ ] **Step 1: 写失败的握手和音频校验测试**

```python
def test_create_session_accepts_v1_pcm16():
    message = decode_client_message(json.dumps({
        "type": "CREATE_SESSION",
        "protocol_version": 1,
        "device_id": "device-01",
        "session_id": "session-100",
        "audio_format": "PCM16",
        "audio_transport": "BASE64_JSON",
        "sample_rate": 16000,
        "channels": 1,
    }))
    assert isinstance(message, CreateSession)

def test_audio_chunk_rejects_odd_pcm_byte_count():
    message = AudioChunkMessage(
        type="AUDIO_CHUNK", session_id="s", sequence=0,
        audio_b64=base64.b64encode(b"\x00").decode(),
    )
    with pytest.raises(ProtocolViolation, match="PCM16_BYTE_ALIGNMENT"):
        decode_pcm16(message, sample_rate=16000)
```

- [ ] **Step 2: 运行协议测试并确认失败**

Run: `pytest tests/unit/protocol -q`

Expected: FAIL，原因是协议模块尚不存在。

- [ ] **Step 3: 实现带判别字段的客户端模型**

```python
class CreateSession(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["CREATE_SESSION"]
    protocol_version: Literal[1]
    device_id: str = Field(min_length=1, max_length=128)
    session_id: str = Field(min_length=1, max_length=128)
    audio_format: Literal["PCM16"]
    audio_transport: Literal["BASE64_JSON"]
    sample_rate: Literal[16000, 24000, 48000]
    channels: Literal[1]

class AudioChunkMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["AUDIO_CHUNK"]
    session_id: str = Field(min_length=1, max_length=128)
    sequence: int = Field(ge=0)
    timestamp_ms: int | None = Field(default=None, ge=0)
    audio_b64: str = Field(min_length=1)

class CloseSession(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["CLOSE_SESSION"]
    session_id: str = Field(min_length=1, max_length=128)

ClientMessage = Annotated[
    CreateSession | AudioChunkMessage | CloseSession,
    Field(discriminator="type"),
]
```

- [ ] **Step 4: 实现 Base64/PCM 和下行编码**

```python
def decode_pcm16(message: AudioChunkMessage, sample_rate: int) -> DecodedAudioChunk:
    try:
        payload = base64.b64decode(message.audio_b64, validate=True)
    except binascii.Error as exc:
        raise ProtocolViolation("INVALID_BASE64", "audio_b64 is not valid Base64") from exc
    if not payload or len(payload) % 2:
        raise ProtocolViolation("PCM16_BYTE_ALIGNMENT", "PCM16 must contain complete int16 samples")
    duration_ms = len(payload) / 2 / sample_rate * 1000.0
    if not 10.0 <= duration_ms <= 500.0:
        raise ProtocolViolation("AUDIO_CHUNK_DURATION", "audio chunk must be 10-500 ms")
    return DecodedAudioChunk(message.sequence, message.timestamp_ms, payload, duration_ms)
```

```python
class ServerMessageBase(BaseModel):
    type: str
    user_id: str
    session_id: str
    turn_id: int = Field(ge=0)
    interrupt: bool

def encode_server_message(message: ServerMessage) -> str:
    return message.model_dump_json(exclude_none=True)
```

- [ ] **Step 5: 增加每种服务端消息的契约测试并运行**

为 `SESSION_CREATED`、`ASR_RESULT`、`TEXT_DELTA`、`TEXT_END`、
`AUDIO_DELTA`、`TURN_STATE`、`RESPONSE_END`、`ERROR` 各写一个断言，
验证公共字段和协议专有字段。

Run: `pytest tests/unit/protocol -q`

Expected: PASS。

- [ ] **Step 6: 提交**

```bash
git add src/realtime_voice/protocol tests/unit/protocol
git commit -m "feat: define websocket protocol v1"
```

### Task 3: PCM16、WAV 与流式重采样

**Files:**
- Create: `src/realtime_voice/audio/__init__.py`
- Create: `src/realtime_voice/audio/pcm.py`
- Create: `src/realtime_voice/audio/resampler.py`
- Create: `tests/unit/audio/test_pcm.py`
- Create: `tests/unit/audio/test_resampler.py`

**Interfaces:**
- Produces: `pcm16_bytes_to_float32(data: bytes) -> np.ndarray`。
- Produces: `float32_to_pcm16_bytes(samples: np.ndarray) -> bytes`。
- Produces: `pcm16_wav_bytes(data: bytes, sample_rate: int) -> bytes`。
- Produces: `StreamingResampler(input_rate: int, output_rate: int)`，方法
  `process_pcm16(data: bytes, final: bool = False) -> bytes`。

- [ ] **Step 1: 写 PCM 和 WAV 失败测试**

```python
def test_pcm16_round_trip_clips_and_preserves_shape():
    samples = np.array([-1.2, -0.5, 0.0, 0.5, 1.2], dtype=np.float32)
    encoded = float32_to_pcm16_bytes(samples)
    decoded = pcm16_bytes_to_float32(encoded)
    assert decoded.shape == samples.shape
    assert decoded[0] == pytest.approx(-1.0, abs=1e-4)
    assert decoded[-1] == pytest.approx(32767 / 32768, abs=1e-4)

def test_wav_header_describes_mono_16khz_pcm16():
    wav_data = pcm16_wav_bytes(b"\x00\x00" * 160, 16000)
    with wave.open(io.BytesIO(wav_data), "rb") as wav:
        assert (wav.getnchannels(), wav.getsampwidth(), wav.getframerate()) == (1, 2, 16000)
```

- [ ] **Step 2: 写跨 chunk 重采样连续性失败测试**

```python
def test_streaming_resampler_matches_single_stream_length():
    source = sine_pcm16(sample_rate=48000, seconds=1.0, frequency=440)
    stream = StreamingResampler(48000, 16000)
    output = b"".join([
        stream.process_pcm16(source[:24000]),
        stream.process_pcm16(source[24000:60000]),
        stream.process_pcm16(source[60000:], final=True),
    ])
    assert abs(len(output) // 2 - 16000) <= 2
```

- [ ] **Step 3: 实现 PCM/WAV 和 soxr 流式封装**

```python
def pcm16_bytes_to_float32(data: bytes) -> np.ndarray:
    if len(data) % 2:
        raise ValueError("PCM16 data must contain complete samples")
    return np.frombuffer(data, dtype="<i2").astype(np.float32) / 32768.0

def float32_to_pcm16_bytes(samples: np.ndarray) -> bytes:
    clipped = np.clip(np.asarray(samples, dtype=np.float32), -1.0, 32767 / 32768)
    return np.rint(clipped * 32768.0).astype("<i2").tobytes()

class StreamingResampler:
    def __init__(self, input_rate: int, output_rate: int):
        self._bypass = input_rate == output_rate
        self._stream = None if self._bypass else soxr.ResampleStream(
            input_rate, output_rate, 1, dtype="float32", quality="HQ"
        )

    def process_pcm16(self, data: bytes, final: bool = False) -> bytes:
        if self._bypass:
            return data
        samples = pcm16_bytes_to_float32(data)
        output = self._stream.resample_chunk(samples, last=final)
        return float32_to_pcm16_bytes(output)
```

- [ ] **Step 4: 运行音频测试**

Run: `pytest tests/unit/audio/test_pcm.py tests/unit/audio/test_resampler.py -q`

Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add src/realtime_voice/audio tests/unit/audio
git commit -m "feat: add pcm and streaming resampling"
```

### Task 4: 每 Session VAD 与语音切段

**Files:**
- Create: `src/realtime_voice/audio/vad.py`
- Create: `tests/unit/audio/test_vad.py`
- Create: `tests/fixtures/audio/speech_16k.wav`
- Create: `tests/fixtures/audio/silence_16k.wav`

**Interfaces:**
- Produces: `VadConfig`。
- Produces: `SpeechSegment(segment_id: int, pcm16_16k: bytes)`。
- Produces: `StreamingVadSegmenter.push(pcm16_16k: bytes, has_speech: bool) -> SpeechSegment | None`。
- Produces: `SileroDetector.has_speech(samples: np.ndarray) -> bool`。
- Produces: `VadWorker.run() -> None`，只向 Actor 事件队列发送完整 segment。

- [ ] **Step 1: 写确定性的切段状态测试**

```python
def test_segment_ends_after_configured_silence():
    segmenter = StreamingVadSegmenter(
        VadConfig(sample_rate=16000, min_silence_ms=500, max_speech_seconds=30)
    )
    speech = pcm_chunk(16000, 100)
    silence = pcm_chunk(16000, 100)
    assert segmenter.push(speech, has_speech=True) is None
    for _ in range(4):
        assert segmenter.push(silence, has_speech=False) is None
    segment = segmenter.push(silence, has_speech=False)
    assert segment is not None
    assert segment.segment_id == 1
```

- [ ] **Step 2: 写 Session 隔离和 30 秒强制切段测试**

```python
def test_vad_state_is_session_local():
    first = StreamingVadSegmenter(VadConfig())
    second = StreamingVadSegmenter(VadConfig())
    first.push(pcm_chunk(16000, 100), has_speech=True)
    assert second.active is False

def test_max_speech_duration_forces_segment():
    segmenter = StreamingVadSegmenter(VadConfig(max_speech_seconds=1))
    result = None
    for _ in range(10):
        result = segmenter.push(pcm_chunk(16000, 100), has_speech=True)
    assert result is not None
```

- [ ] **Step 3: 实现切段器和可注入的 Silero 检测器**

```python
@dataclass(frozen=True)
class VadConfig:
    sample_rate: int = 16000
    threshold: float = 0.5
    min_silence_ms: int = 500
    max_speech_seconds: int = 30

class StreamingVadSegmenter:
    def push(self, pcm16_16k: bytes, has_speech: bool) -> SpeechSegment | None:
        duration_ms = len(pcm16_16k) / 2 / self.config.sample_rate * 1000
        if has_speech:
            self.active = True
            self.silence_ms = 0.0
            self._chunks.append(pcm16_16k)
        elif self.active:
            self.silence_ms += duration_ms
            self._chunks.append(pcm16_16k)
        reached_silence = self.active and self.silence_ms >= self.config.min_silence_ms
        reached_limit = self._sample_count() >= self.config.sample_rate * self.config.max_speech_seconds
        return self._finish() if reached_silence or reached_limit else None
```

Silero 模型从安装包或 `RTVA_VAD_MODEL_PATH` 加载；禁止在启动阶段使用远端
`torch.hub`。模型调用通过依赖注入，使状态测试不依赖模型推理。

- [ ] **Step 4: 实现 VAD Worker 的执行器隔离测试**

使用假 detector 记录调用线程 ID，断言 `has_speech` 不在事件循环线程执行，
且 Worker 只在 segment 完成时发送一次 `SpeechSegmentReady`。

Run: `pytest tests/unit/audio/test_vad.py -q`

Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add src/realtime_voice/audio/vad.py tests/unit/audio/test_vad.py tests/fixtures/audio
git commit -m "feat: add per-session streaming vad"
```

### Task 5: 有界下游准入与 NDJSON 增量解析

**Files:**
- Create: `src/realtime_voice/clients/__init__.py`
- Create: `src/realtime_voice/clients/limits.py`
- Create: `src/realtime_voice/clients/ndjson.py`
- Create: `tests/unit/clients/test_limits.py`
- Create: `tests/unit/clients/test_ndjson.py`

**Interfaces:**
- Produces: `BoundedAdmission(name: str, concurrency: int, max_waiters: int)`。
- Produces: `AdmissionOverloaded(service: str)`。
- Produces: `iter_ndjson(chunks: AsyncIterable[bytes]) -> AsyncIterator[dict[str, Any]]`。

- [ ] **Step 1: 写队列满载失败测试**

```python
async def test_admission_rejects_job_beyond_waiter_limit():
    gate = BoundedAdmission("asr", concurrency=1, max_waiters=1)
    release = asyncio.Event()

    first = asyncio.create_task(gate.run(lambda: release.wait()))
    await gate.wait_until_active(1)
    second = asyncio.create_task(gate.run(lambda: release.wait()))
    await gate.wait_until_waiting(1)

    with pytest.raises(AdmissionOverloaded, match="asr"):
        await gate.run(lambda: release.wait())

    release.set()
    await asyncio.gather(first, second)
```

- [ ] **Step 2: 写任意 chunk 边界 NDJSON 测试**

```python
async def test_ndjson_parser_handles_split_and_joined_lines():
    chunks = async_chunks([
        b'{"type":"text_',
        b'delta","delta":"你"}\n{"type":"done"',
        b',"output":{"reply_text":"你好"}}\n',
    ])
    events = [event async for event in iter_ndjson(chunks)]
    assert events == [
        {"type": "text_delta", "delta": "你"},
        {"type": "done", "output": {"reply_text": "你好"}},
    ]
```

- [ ] **Step 3: 实现准入器和解析器**

准入器在进入等待前原子增加 waiter；达到 `max_waiters` 立即抛出
`AdmissionOverloaded`；离开时在 `finally` 中减少计数。解析器保留跨 chunk
字节缓冲，只在换行处 JSON 解码，流结束时非空尾部必须是完整 JSON。

- [ ] **Step 4: 运行测试**

Run: `pytest tests/unit/clients/test_limits.py tests/unit/clients/test_ndjson.py -q`

Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add src/realtime_voice/clients tests/unit/clients
git commit -m "feat: add bounded downstream admission"
```

### Task 6: 异步 ASR 客户端

**Files:**
- Create: `src/realtime_voice/clients/asr.py`
- Create: `tests/unit/clients/test_asr.py`

**Interfaces:**
- Produces: `AsrClient.transcribe(pcm16_16k: bytes) -> str`。
- Consumes: `pcm16_wav_bytes`、`BoundedAdmission`、共享 `httpx.AsyncClient`。

- [ ] **Step 1: 写请求结构和清洗测试**

```python
async def test_asr_sends_wav_and_cleans_tagged_text():
    response = {
        "choices": [{"message": {
            "content": "language: Chinese<asr_text> 你好 "
        }}]
    }
    transport, captured = stream_transport([json.dumps(response).encode("utf-8")])
    admission = BoundedAdmission("asr", concurrency=8, max_waiters=64)
    async with httpx.AsyncClient(transport=transport, base_url="http://asr") as http:
        client = AsrClient(http, admission)
        text = await client.transcribe(b"\x00\x00" * 1600)
    assert text == "你好"
    payload = json.loads(captured["request"].content)
    input_audio = payload["messages"][0]["content"][0]["input_audio"]
    assert input_audio["format"] == "wav"
    assert wave.open(io.BytesIO(base64.b64decode(input_audio["data"]))).getframerate() == 16000
```

- [ ] **Step 2: 写空文本、超时和坏响应测试**

覆盖 `""`、`none`、`null`、`undefined`、缺失 `choices`、HTTP 500 和
`httpx.ReadTimeout`，断言分别返回空文本或抛出稳定的 `AsrError`。

- [ ] **Step 3: 实现客户端**

```python
class AsrClient:
    async def transcribe(self, pcm16_16k: bytes) -> str:
        wav = pcm16_wav_bytes(pcm16_16k, 16000)
        payload = {
            "messages": [{
                "role": "user",
                "content": [{
                    "type": "input_audio",
                    "input_audio": {
                        "data": base64.b64encode(wav).decode("ascii"),
                        "format": "wav",
                    },
                }],
            }],
        }
        async def operation() -> str:
            response = await self.http.post("/v1/chat/completions", json=payload)
            response.raise_for_status()
            content = response.json()["choices"][0]["message"]["content"]
            return clean_asr_text(content)
        return await self.admission.run(operation)
```

- [ ] **Step 4: 运行测试并提交**

Run: `pytest tests/unit/clients/test_asr.py -q`

Expected: PASS。

```bash
git add src/realtime_voice/clients/asr.py tests/unit/clients/test_asr.py
git commit -m "feat: add asynchronous asr client"
```

### Task 7: BerryThinker 流、打断与 Session 清理客户端

**Files:**
- Create: `src/realtime_voice/clients/berry.py`
- Create: `tests/unit/clients/test_berry.py`

**Interfaces:**
- Produces: `BerryTextDelta(delta: str)`、`BerryDone(reply_text: str)`。
- Produces: `BerryClient.stream_reply(request: BerryReplyRequest) -> AsyncIterator[BerryEvent]`。
- Produces: `BerryClient.interrupt(user_id: str, session_id: str) -> None`。
- Produces: `BerryClient.delete_session(user_id: str, session_id: str) -> DeleteResult`。

- [ ] **Step 1: 写 multipart 和 NDJSON 测试**

```python
async def test_berry_request_uses_existing_multimodal_contract():
    chunks = [
        b'{"type":"text_delta","delta":"\\u4f60"}\n',
        b'{"type":"done","output":{"reply_text":"\\u4f60\\u597d\\uff0c\\u6211\\u5728\\u3002"}}\n',
    ]
    transport, captured = stream_transport(chunks)
    admission = BoundedAdmission("berry", concurrency=8, max_waiters=64)
    async with httpx.AsyncClient(transport=transport, base_url="http://berry") as http:
        client = BerryClient(http, admission)
        events = [event async for event in client.stream_reply(BerryReplyRequest(
            user_id="device-01", session_id="session-100",
            text="你好", audio_wav=valid_wav(),
        ))]
    assert events == [BerryTextDelta("你"), BerryDone("你好，我在。")]
    body = captured["request"].content
    assert b'name="stream"' in body
    assert b'name="audio_is_vad_segment"' in body
    assert b'name="skip_internal_asr"' in body
```

- [ ] **Step 2: 写 interrupt 和 DELETE 语义测试**

断言 interrupt 只发送 `user_id + session_id`；DELETE 的 200 和 404 都返回成功，
其他状态抛出 `BerryCleanupError`。

- [ ] **Step 3: 实现客户端并处理流内 error**

```python
async def stream_reply(self, request: BerryReplyRequest) -> AsyncIterator[BerryEvent]:
    files = {"audio": ("segment.wav", request.audio_wav, "audio/wav")}
    data = {
        "text": request.text,
        "user_id": request.user_id,
        "session_id": request.session_id,
        "stream": "true",
        "reply_mode": "dialogue",
        "audio_is_vad_segment": "true",
        "skip_internal_asr": "true",
    }
    async with self.http.stream("POST", "/api/v1/multimodal/reply", data=data, files=files) as response:
        response.raise_for_status()
        async for event in iter_ndjson(response.aiter_bytes()):
            if event.get("type") == "text_delta":
                yield BerryTextDelta(str(event.get("delta") or ""))
            elif event.get("type") == "done":
                yield BerryDone(str(event["output"]["reply_text"]))
            elif event.get("type") == "error":
                raise BerryStreamError(str(event.get("error_message") or "berry stream failed"))
```

- [ ] **Step 4: 运行测试并提交**

Run: `pytest tests/unit/clients/test_berry.py -q`

Expected: PASS。

```bash
git add src/realtime_voice/clients/berry.py tests/unit/clients/test_berry.py
git commit -m "feat: add berry thinker streaming client"
```

### Task 8: PromptDialogAPI 流式 TTS 客户端

**Files:**
- Create: `src/realtime_voice/clients/tts.py`
- Create: `tests/unit/clients/test_tts.py`

**Interfaces:**
- Produces: `TtsChunk(chunk_index: int, pcm16_24k: bytes, finalize: bool)`。
- Produces: `TtsClient.stream(request: TtsRequest) -> AsyncIterator[TtsChunk]`。

- [ ] **Step 1: 写请求映射和音频事件测试**

```python
async def test_tts_maps_dialogue_and_decodes_audio():
    event = {
        "chunk_index": 0, "sample_rate": 24000, "finalize": True,
        "audio_i16le_b64": base64.b64encode(b"\x00\x00\x01\x00").decode(),
    }
    transport, captured = stream_transport([json.dumps(event).encode() + b"\n"])
    admission = BoundedAdmission("tts", concurrency=8, max_waiters=64)
    async with httpx.AsyncClient(transport=transport, base_url="http://tts") as http:
        client = TtsClient(http, admission)
        chunks = [chunk async for chunk in client.stream(TtsRequest(
            user_input="你好", model_reply="你好，我在。",
            trace_id="device/session/turn-1",
        ))]
    assert chunks[0].pcm16_24k == b"\x00\x00\x01\x00"
    assert chunks[0].finalize is True
    assert json.loads(captured["request"].content)["include_prompt_event"] is False
```

- [ ] **Step 2: 写错误和采样率测试**

覆盖 HTTP 422、HTTP 500、流内 `{"error":"internal_error"}`、非法 Base64、
奇数字节和非 24000 采样率，断言抛出稳定的 `TtsStreamError`。

- [ ] **Step 3: 实现客户端**

```python
async def stream(self, request: TtsRequest) -> AsyncIterator[TtsChunk]:
    payload = {
        "user_input": request.user_input,
        "model_reply": request.model_reply,
        "include_prompt_event": False,
        "trace_id": request.trace_id,
    }
    async with self.http.stream("POST", "/v1/dialogue-tts/stream", json=payload) as response:
        response.raise_for_status()
        async for event in iter_ndjson(response.aiter_bytes()):
            if "error" in event:
                raise TtsStreamError(str(event.get("message") or event["error"]))
            if event.get("event") == "prompt":
                continue
            if int(event["sample_rate"]) != 24000:
                raise TtsStreamError("TTS_SAMPLE_RATE")
            audio = base64.b64decode(event["audio_i16le_b64"], validate=True)
            if len(audio) % 2:
                raise TtsStreamError("TTS_PCM_ALIGNMENT")
            yield TtsChunk(int(event["chunk_index"]), audio, bool(event["finalize"]))
```

- [ ] **Step 4: 运行测试并提交**

Run: `pytest tests/unit/clients/test_tts.py -q`

Expected: PASS。

```bash
git add src/realtime_voice/clients/tts.py tests/unit/clients/test_tts.py
git commit -m "feat: add dialogue tts streaming client"
```

### Task 9: Session 事件、状态与 Actor 状态机

**Files:**
- Create: `src/realtime_voice/session/__init__.py`
- Create: `src/realtime_voice/session/events.py`
- Create: `src/realtime_voice/session/state.py`
- Create: `src/realtime_voice/session/actor.py`
- Create: `tests/unit/session/conftest.py`
- Create: `tests/unit/session/test_actor_turns.py`
- Create: `tests/unit/session/test_actor_interrupts.py`
- Create: `tests/unit/session/test_actor_stale_events.py`

**Interfaces:**
- Produces events: `SpeechSegmentReady`、`AsrSucceeded`、`AsrFailed`、
  `BerryDeltaReceived`、`BerryCompleted`、`BerryFailed`、`TtsChunkReceived`、
  `TtsCompleted`、`TtsFailed`、`SessionDisconnected`。
- Produces effects: `SendOutbound`、`StartBerry`、`StartTts`、
  `StartNextBerry(interrupt_first: bool)`、`CloseRuntime`。
- Produces: `SessionActor.handle(event: SessionEvent) -> list[SessionEffect]`。

- [ ] **Step 1: 写空 ASR 和 turn 分配失败测试**

在 `tests/unit/session/conftest.py` 中定义测试构造器：

```python
def actor_for_test() -> SessionActor:
    return SessionActor(SessionState(user_id="u", session_id="s", sample_rate=16000))

def actor_with_streaming_turn(turn_id: int) -> SessionActor:
    actor = actor_for_test()
    actor.handle(AsrSucceeded(turn_id, f"text-{turn_id}", valid_wav()))
    return actor

def actor_with_tts_turn(turn_id: int, interrupted: bool) -> SessionActor:
    actor = actor_with_streaming_turn(turn_id)
    actor.handle(BerryCompleted(turn_id, generation=1, reply_text="reply"))
    actor.state.turns[turn_id].interrupted = interrupted
    return actor

def outbound_of_type(effects: list[SessionEffect], message_type: str) -> ServerMessage:
    return next(
        effect.message for effect in effects
        if isinstance(effect, SendOutbound) and effect.message.type == message_type
    )
```

```python
def test_empty_asr_creates_no_turn():
    actor = actor_for_test()
    effects = actor.handle(AsrSucceeded(1, "", valid_wav()))
    assert actor.state.next_turn_id == 1
    assert effects == []

def test_non_empty_asr_allocates_turn_and_emits_result():
    actor = actor_for_test()
    effects = actor.handle(AsrSucceeded(1, "你好", valid_wav()))
    assert actor.state.turns[1].asr_text == "你好"
    assert any(isinstance(effect, StartBerry) for effect in effects)
    assert outbound_of_type(effects, "ASR_RESULT").turn_id == 1
```

- [ ] **Step 2: 写 LLM 和 TTS 打断失败测试**

```python
def test_new_turn_interrupts_llm_but_keeps_text_flow():
    actor = actor_with_streaming_turn(1)
    effects = actor.handle(AsrSucceeded(2, "等一下", valid_wav()))
    assert actor.state.turns[1].interrupted is True
    assert outbound_of_type(effects, "TURN_STATE").turn_id == 1

    delta = actor.handle(BerryDeltaReceived(1, generation=1, delta="旧文本"))
    message = outbound_of_type(delta, "TEXT_DELTA")
    assert message.interrupt is True

def test_interrupted_tts_chunk_is_discarded():
    actor = actor_with_tts_turn(1, interrupted=True)
    effects = actor.handle(TtsChunkReceived(1, 1, 0, b"\x00\x00", False))
    assert not any(isinstance(effect, SendOutbound) for effect in effects)
```

- [ ] **Step 3: 实现显式状态和纯状态转换**

```python
@dataclass
class TurnContext:
    turn_id: int
    asr_text: str
    audio_wav: bytes | None
    stage: TurnStage = TurnStage.WAITING_LLM
    interrupted: bool = False
    berry_generation: int = 0
    tts_generation: int = 0
    reply_text: str = ""

@dataclass
class SessionState:
    user_id: str
    session_id: str
    sample_rate: int
    next_turn_id: int = 1
    active_llm_turn_id: int | None = None
    llm_queue: deque[int] = field(default_factory=deque)
    turns: OrderedDict[int, TurnContext] = field(default_factory=OrderedDict)
    closing: bool = False
```

`handle()` 只能同步修改状态和返回 effect，不执行网络请求、不创建后台任务、
不直接写 WebSocket。

- [ ] **Step 4: 写快速 turn 1/2/3 和过期代次测试**

断言 Berry 启动顺序为 1、2、3；turn 1/2 跳过 TTS；旧 `generation` 的 Berry/TTS
事件不发送消息、不修改当前状态，并增加 stale-event 计数 effect。

- [ ] **Step 5: 运行 Actor 测试**

Run: `pytest tests/unit/session/test_actor_turns.py tests/unit/session/test_actor_interrupts.py tests/unit/session/test_actor_stale_events.py -q`

Expected: PASS。

- [ ] **Step 6: 提交**

```bash
git add src/realtime_voice/session tests/unit/session
git commit -m "feat: add session actor state machine"
```

### Task 10: SessionRuntime、Worker 与安全清理

**Files:**
- Create: `src/realtime_voice/session/runtime.py`
- Create: `src/realtime_voice/session/registry.py`
- Create: `tests/unit/session/test_registry.py`
- Create: `tests/unit/session/test_runtime.py`
- Create: `tests/unit/session/test_cleanup.py`

**Interfaces:**
- Produces: `SessionRegistry.add(key: str, runtime: SessionRuntime) -> None`、`create(create: CreateSession, websocket: WebSocket) -> SessionRuntime` 和 `remove(key: str) -> None`。
- Produces: `SessionRuntime.run() -> None`。
- Consumes: Actor effects和 ASR/Berry/TTS 客户端。
- Guarantees: 一个 Session 的 Worker 由一个 `TaskGroup` 统一停止。

- [ ] **Step 1: 写容量和重复 Session 测试**

```python
async def test_registry_rejects_duplicate_and_capacity():
    registry = SessionRegistry(max_sessions=1)
    first_runtime = create_autospec(SessionRuntime, instance=True)
    await registry.add("session-1", first_runtime)
    with pytest.raises(DuplicateSession):
        await registry.add("session-1", create_autospec(SessionRuntime, instance=True))
    with pytest.raises(SessionCapacityExceeded):
        await registry.add("session-2", create_autospec(SessionRuntime, instance=True))
```

- [ ] **Step 2: 写 Worker 拓扑和 effect 执行测试**

```python
async def test_start_berry_effect_runs_in_background_and_returns_events():
    runtime = runtime_with_fake_clients()
    await runtime.execute_effect(StartBerry(turn_id=1, generation=1,
                                             asr_text="你好", audio_wav=valid_wav()))
    event = await asyncio.wait_for(runtime.events.get(), timeout=1)
    assert event == BerryDeltaReceived(1, 1, "你")
```

执行 `StartTts` effect 时，为该 turn 创建 `StreamingResampler(24000, session_sample_rate)`；
每个 `TtsChunk` 先经过同一个重采样器，再发送 `TtsChunkReceived`，`finalize=true` 时
以 `final=True` 刷新尾部采样。被打断 turn 仍读取 TTS，但不再执行输出重采样或下发。

- [ ] **Step 3: 实现 TaskGroup 和 effect 调度**

```python
async def run(self) -> None:
    try:
        async with asyncio.TaskGroup() as group:
            self._group = group
            group.create_task(self._actor_loop(), name=f"{self.session_id}:actor")
            group.create_task(self._vad_loop(), name=f"{self.session_id}:vad")
            group.create_task(self._asr_loop(), name=f"{self.session_id}:asr")
            group.create_task(self._sender_loop(), name=f"{self.session_id}:sender")
            await self._close_requested.wait()
            raise SessionStop()
    except* SessionStop:
        pass
    finally:
        await self._cleanup()
```

后台 effect 任务必须将 delta/completed/failed 事件放回 `events`；不能直接修改
`SessionState`。

- [ ] **Step 4: 写断线清理顺序测试**

使用记录调用顺序的 fake clients，断言：

```python
assert calls == [
    "stop_audio",
    "cancel_asr",
    "wait_berry",
    "drain_tts",
    "delete_berry_session",
    "remove_registry",
]
```

另写 Berry 超时测试，断言不调用 DELETE、记录 `BERRY_CLEANUP_SKIPPED`；
DELETE 200/404 均成功。

- [ ] **Step 5: 运行 Runtime 测试并提交**

Run: `pytest tests/unit/session/test_registry.py tests/unit/session/test_runtime.py tests/unit/session/test_cleanup.py -q`

Expected: PASS。

```bash
git add src/realtime_voice/session tests/unit/session
git commit -m "feat: supervise realtime session runtime"
```

### Task 11: WebSocket 接入、握手、收发与背压

**Files:**
- Create: `src/realtime_voice/transport/__init__.py`
- Create: `src/realtime_voice/transport/websocket.py`
- Modify: `src/realtime_voice/main.py`
- Create: `tests/integration/test_websocket_protocol.py`
- Create: `tests/integration/test_websocket_backpressure.py`

**Interfaces:**
- Produces: `serve_realtime(websocket: WebSocket, services: AppServices) -> None`。
- Exposes: `WS /v1/realtime`。
- Guarantees: 第一条消息在 5 秒内为合法 `CREATE_SESSION`。
- Guarantees: 上行 `sequence` 从 0 严格递增，Session ID 必须一致。

- [ ] **Step 1: 写握手成功和失败测试**

```python
def test_websocket_requires_create_session_first(app):
    with TestClient(app).websocket_connect("/v1/realtime") as ws:
        ws.send_json({"type": "AUDIO_CHUNK", "session_id": "s",
                      "sequence": 0, "audio_b64": "AAAA"})
        error = ws.receive_json()
        assert error["type"] == "ERROR"
        assert error["code"] == "CREATE_SESSION_REQUIRED"

def test_websocket_echoes_negotiated_audio(app):
    with connected_session(app, sample_rate=24000) as ws:
        created = ws.receive_json()
        assert created["type"] == "SESSION_CREATED"
        assert created["sample_rate"] == 24000
        assert created["protocol_version"] == 1
```

- [ ] **Step 2: 写序号、Session 不匹配和慢客户端测试**

断言 `sequence=0,2` 返回 `AUDIO_SEQUENCE_GAP`；消息中的其他 Session ID
返回 `SESSION_ID_MISMATCH`；超过 3 秒音频积压返回
`CLIENT_AUDIO_BACKPRESSURE` 并关闭。

- [ ] **Step 3: 实现 WebSocket 服务函数**

```python
async def serve_realtime(websocket: WebSocket, services: AppServices) -> None:
    await websocket.accept()
    try:
        raw = await asyncio.wait_for(
            websocket.receive_text(),
            timeout=services.settings.handshake_timeout_seconds,
        )
        create = require_create_session(decode_client_message(raw))
        runtime = await services.registry.create(create, websocket)
        await runtime.run()
    except ProtocolViolation as exc:
        await send_protocol_error(websocket, exc)
        await websocket.close(code=1008)
    except WebSocketDisconnect:
        return
```

Receiver 只做 JSON/PCM 校验并写 `audio_queue`；Sender 是唯一调用
`websocket.send_text` 的长期协程。

- [ ] **Step 4: 运行 WebSocket 集成测试**

Run: `pytest tests/integration/test_websocket_protocol.py tests/integration/test_websocket_backpressure.py -q`

Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add src/realtime_voice/main.py src/realtime_voice/transport tests/integration
git commit -m "feat: expose realtime websocket endpoint"
```

### Task 12: 结构化日志、Prometheus 指标与健康状态

**Files:**
- Create: `src/realtime_voice/observability/__init__.py`
- Create: `src/realtime_voice/observability/logging.py`
- Create: `src/realtime_voice/observability/metrics.py`
- Modify: `src/realtime_voice/main.py`
- Modify: `src/realtime_voice/session/runtime.py`
- Create: `tests/unit/observability/test_logging.py`
- Create: `tests/unit/observability/test_metrics.py`
- Modify: `tests/unit/test_main.py`

**Interfaces:**
- Produces: `bind_context(user_id, session_id, turn_id, segment_id)`。
- Produces: `Metrics`，记录活动 Session、队列、阶段延迟、打断和丢弃音频。
- Exposes: `GET /metrics`。
- Extends: `GET /health` 返回容量、活动 Session、limiter/executor 状态和缓存下游健康。

- [ ] **Step 1: 写日志隐私和公共字段测试**

```python
def test_structured_log_omits_audio_and_full_text(caplog):
    log_event("asr_completed", user_id="u", session_id="s", turn_id=1,
              duration_ms=20.0, audio_bytes=b"secret", text="完整对话")
    payload = json.loads(caplog.records[-1].message)
    assert payload["user_id"] == "u"
    assert "audio_bytes" not in payload
    assert "text" not in payload
```

- [ ] **Step 2: 写指标和健康检查测试**

验证活动 Session Gauge、`speech_end_to_first_tts_seconds` Histogram、
`discarded_tts_bytes_total` Counter；验证 `/metrics` content type；
验证 `/health` 不同步等待下游网络。

- [ ] **Step 3: 实现并接入生命周期埋点**

Actor/Runtime 在事件发生处记录确定事件名：`vad_segment_ready`、
`asr_completed`、`berry_first_delta`、`tts_first_audio`、
`turn_interrupted`、`tts_chunk_discarded`、`session_cleanup`。

- [ ] **Step 4: 运行测试并提交**

Run: `pytest tests/unit/observability tests/unit/test_main.py -q`

Expected: PASS。

```bash
git add src/realtime_voice/observability src/realtime_voice/main.py src/realtime_voice/session/runtime.py tests/unit
git commit -m "feat: add service observability"
```

### Task 13: 完整假服务集成场景

**Files:**
- Create: `tests/integration/fake_services.py`
- Create: `tests/integration/test_full_turn.py`
- Create: `tests/integration/test_interrupt_llm.py`
- Create: `tests/integration/test_interrupt_tts.py`
- Create: `tests/integration/test_disconnect_cleanup.py`

**Interfaces:**
- Produces: 可控制延迟和事件顺序的 fake ASR/Berry/TTS FastAPI 应用。
- Verifies: 客户端可观察的完整协议，而不是 Actor 内部实现。

- [ ] **Step 1: 实现 fake 服务夹具**

Fake ASR 返回配置文本；Fake Berry 使用 NDJSON Event 控制器逐项释放 delta/done；
Fake TTS 使用 NDJSON 控制器逐项释放 24 kHz PCM chunk，并记录流是否被排空；
Fake Berry 记录 interrupt 和 DELETE 调用。

- [ ] **Step 2: 写完整 turn 失败测试**

```python
def test_full_turn_emits_asr_text_and_audio(integration_app):
    with connected_session(integration_app, sample_rate=16000) as ws:
        send_fixture_audio(ws, "speech_16k.wav")
        messages = receive_until(ws, "RESPONSE_END")
    assert message_types(messages) == [
        "SESSION_CREATED", "ASR_RESULT", "TEXT_DELTA", "TEXT_END",
        "AUDIO_DELTA", "RESPONSE_END",
    ]
    assert messages[-1]["status"] == "COMPLETED"
```

- [ ] **Step 3: 写 LLM 打断场景**

控制 turn 1 Berry 未结束时让 turn 2 ASR 返回。断言 turn 1 后续文本
`interrupt=true`、turn 1 无 `AUDIO_DELTA`、Berry interrupt 在 turn 1 done
之后且 turn 2 请求之前调用。

- [ ] **Step 4: 写 TTS 打断场景**

让 turn 1 TTS 保持打开，随后创建 turn 2。断言 turn 1 后续音频未下发但 fake TTS
被完整读取；turn 2 的首个 TTS chunk 在 turn 1 drain 结束前到达客户端。

- [ ] **Step 5: 写断线和下游错误场景**

断言断线后停止下行、等待 Berry、排空 TTS，再 DELETE；ASR 失败不创建 turn；
Berry/TTS 失败分别返回规定的 `ERROR + RESPONSE_END`。

- [ ] **Step 6: 运行全部集成测试并提交**

Run: `pytest tests/integration -q`

Expected: PASS。

```bash
git add tests/integration
git commit -m "test: cover realtime voice orchestration"
```

### Task 14: 示例客户端、压测、部署文档与最终验收

**Files:**
- Create: `scripts/realtime_client.py`
- Create: `scripts/load_test.py`
- Create: `.env.example`
- Create: `deploy/realtime-voice-api.service`
- Create: `deploy/supervisord.conf`
- Modify: `README.md`
- Create: `tests/unit/scripts/test_realtime_client.py`
- Create: `tests/unit/scripts/test_load_test.py`

**Interfaces:**
- Produces: `realtime_client.py --url --wav --sample-rate --output`。
- Produces: `load_test.py --url --clients 30 --wav --report report.json`。
- Documents: 安装、配置、启动、协议、真实服务冒烟测试、压测和守护进程示例。

- [ ] **Step 1: 写客户端分块和输出拼接测试**

```python
def test_audio_chunks_are_40ms_and_sequences_are_monotonic():
    chunks = list(iter_pcm_chunks(pcm=b"\x00\x00" * 1600,
                                  sample_rate=16000, chunk_ms=40))
    assert [chunk.sequence for chunk in chunks] == [0, 1, 2]
    assert len(chunks[0].pcm) == 1280

def test_audio_delta_writer_rejects_wrong_turn_sequence(tmp_path):
    writer = TurnAudioWriter(tmp_path / "reply.wav", sample_rate=16000)
    writer.add(turn_id=1, sequence=0, pcm=b"\x00\x00")
    with pytest.raises(ValueError, match="sequence"):
        writer.add(turn_id=1, sequence=2, pcm=b"\x00\x00")
```

- [ ] **Step 2: 实现示例客户端**

客户端读取 mono PCM16 WAV，按 40 ms 发送 `AUDIO_CHUNK`，打印
`ASR_RESULT/TEXT_*`，按 turn 拼接 `AUDIO_DELTA`，收到
`TURN_STATE(interrupt=true)` 时停止写旧 turn，最终生成可播放 WAV。

- [ ] **Step 3: 实现压测工具及报告测试**

报告 JSON 必须包含：`clients`、`connected`、`failed`、
`speech_end_to_asr_ms`、`speech_end_to_text_ms`、
`speech_end_to_audio_ms`、`errors_by_code`、`duration_seconds`。
测试用 fake WebSocket server 验证 3 个并发客户端和 percentile 计算。

- [ ] **Step 4: 写配置与部署文档**

`.env.example` 列出规格中的全部默认值；systemd 使用
`ExecStart=/path/to/venv/bin/uvicorn realtime_voice.main:app --host 0.0.0.0 --port 8003`；
supervisord 示例使用同一命令。只提供文件，不运行 `systemctl` 或修改机器服务。

- [ ] **Step 5: 更新 README**

README 必须包含：

1. Python 3.11 环境和安装命令；
2. ASR/Berry/TTS 默认地址；
3. 启动和健康检查；
4. 全部 V1 WebSocket 消息示例；
5. 示例客户端命令；
6. 单元/集成测试命令；
7. 30/40/60 并发压测命令；
8. Opus 和断线恢复不在 V1 范围的说明。

- [ ] **Step 6: 运行完整静态检查和测试**

Run: `ruff check src tests scripts`

Expected: exit 0。

Run: `pytest -q`

Expected: 全部 PASS，0 failure。

- [ ] **Step 7: 在服务可用时运行真实冒烟测试**

Run:

```bash
python scripts/realtime_client.py \
  --url ws://127.0.0.1:8003/v1/realtime \
  --wav tests/fixtures/audio/speech_16k.wav \
  --sample-rate 16000 \
  --output /tmp/realtime-voice-reply.wav
```

Expected: 收到 `ASR_RESULT`、`TEXT_END`、至少一个 `AUDIO_DELTA` 和
`RESPONSE_END(status=COMPLETED)`，输出 WAV 可打开。

- [ ] **Step 8: 运行 30 路压测**

Run:

```bash
python scripts/load_test.py \
  --url ws://127.0.0.1:8003/v1/realtime \
  --clients 30 \
  --wav tests/fixtures/audio/speech_16k.wav \
  --report /tmp/realtime-voice-load-30.json
```

Expected: 30 个连接均完成；无队列/线程/内存持续增长；下游容量足够时报告中的
首段 TTS 音频延迟 p95 不超过 5000 ms。

- [ ] **Step 9: 提交**

```bash
git add scripts .env.example deploy README.md tests/unit/scripts
git commit -m "docs: add realtime voice operations tooling"
```

## 规格覆盖矩阵

| 规格章节 | 实施任务 | 验证证据 |
|---|---|---|
| 2 范围与约束 | Task 1、11、14 | 配置测试、WebSocket 集成、README/压测 |
| 3 服务地址 | Task 1、6、7、8 | 配置默认值和三个客户端请求测试 |
| 4 运行架构 | Task 9、10、11 | Actor、TaskGroup、Receiver/Sender 测试 |
| 5 源码边界 | Task 1～12 | 文件结构和导入方向 |
| 6 V1 协议 | Task 2、11、13 | 契约测试和端到端消息序列 |
| 7 音频与 VAD | Task 3、4 | PCM、重采样、VAD 隔离/切段测试 |
| 8 下游接口 | Task 5～8 | MockTransport 请求/流解析测试 |
| 9 Actor 状态与顺序 | Task 9、10 | turn、代次、快速连续 turn 测试 |
| 10 打断语义 | Task 9、13 | LLM/TTS 打断集成场景 |
| 11 错误、背压与超时 | Task 5、10、11、13 | 满载、慢客户端和超时测试 |
| 12 断线与清理 | Task 7、10、13 | DELETE 语义和清理顺序测试 |
| 13 可观测性 | Task 12 | 日志隐私、指标和健康检查测试 |
| 14 测试与验证 | Task 13、14 | 全套 pytest、冒烟和 30 路压测 |
| 15 交付物 | Task 14 | 客户端、压测、配置和部署文档 |

## Final Verification

- [ ] 运行 `ruff check src tests scripts`，确认 exit 0。
- [ ] 运行 `pytest -q`，确认 0 failure。
- [ ] 运行 `git diff --check`，确认无空白错误。
- [ ] 检查 `git status --short`，确认只包含预期变更。
- [ ] 对照规格第 2、6、9、10、11、12、14 节逐项映射到测试。
- [ ] 如果本地 8000/8082/8002 可用，运行真实冒烟和 30 路压测并保存报告。

