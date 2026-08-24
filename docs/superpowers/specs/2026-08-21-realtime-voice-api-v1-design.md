# RealTimeVoiceAPI V1 设计规格

**日期：** 2026-08-21

**状态：** 已通过设计评审

## 1. 目标

构建一个单进程异步 WebSocket 网关：持续接收客户端音频，检测完整语音段，
调用 ASR，流式转发 BerryThinker 回复，通过 PromptDialogAPI 合成回复语音，
并把文本、状态和音频返回客户端。服务必须至少支持 30 个并发 Session，
同时保证每个 Session 内的处理顺序以及本文定义的打断语义。

本文是仓库 `README.md` 的 V1 细化规格。两者不一致时，以本文为准。
其中最重要的范围调整是：V1 使用 JSON 中的 Base64 传输 PCM16；
在明确具体客户端使用的编解码器和容器格式之前，暂缓 Opus。

## 2. 范围与约束

V1 必须：

- 提供一个带版本的 WebSocket 接口，以及 HTTP 健康检查和指标接口；
- 支持连续的单声道 PCM16 上下行音频，采样率为 16000、24000 或 48000 Hz；
- 把音频字节编码成 Base64，放在 JSON 文本帧中传输；
- 在本进程运行 VAD，并为每个 Session 保存独立状态；
- 调用 ASR、BerryThinker 和 PromptDialogAPI 的现有接口，不修改这些服务；
- Session、Turn、任务、队列和打断状态只保存在本进程；
- 同一 Session 的 ASR 和 BerryThinker 严格有序，不同 Session 可以并发；
- 被打断的 BerryThinker 流继续读取到结束，保证其历史和记忆流程完成；
- 被打断的 TTS 流继续排空并丢弃，同时允许新 turn 继续处理；
- 所有队列、下游并发、CPU 执行和超时均有明确上限；
- 提供单元测试、集成测试、真实服务冒烟测试和并发压测工具；
- 不引入 Redis、Docker、Kubernetes、服务发现或分布式任务队列；
- V1 只在可信内网使用，不实现鉴权。

V1 不支持：

- Opus、WebM、Ogg、MP3、立体声或 WebSocket 二进制音频帧；
- 恢复已经断开的实时 Session；
- 在 RealTimeVoiceAPI 中保存短期对话历史或长期记忆；
- 取消已经开始的 BerryThinker 回复；
- 要求下游服务增加业务参数；
- 下游实例发现或负载均衡。

## 3. 固定服务地址

以下是默认值，均可通过环境变量覆盖：

| 服务 | 默认值 |
|---|---|
| RealTimeVoiceAPI 监听地址 | `0.0.0.0` |
| RealTimeVoiceAPI 端口 | `8003` |
| WebSocket 路径 | `/v1/realtime` |
| 健康检查路径 | `/health` |
| 指标路径 | `/metrics` |
| ASR 基础地址 | `http://127.0.0.1:8000` |
| ASR 路径 | `/v1/chat/completions` |
| BerryThinker 基础地址 | `http://127.0.0.1:8082` |
| BerryThinker 回复路径 | `/api/v1/multimodal/reply` |
| BerryThinker 打断路径 | `/api/v1/interrupt` |
| BerryThinker Session 清理路径 | `/api/v1/sessions/{user_id}/{session_id}` |
| TTS 基础地址 | `http://127.0.0.1:8001` |
| TTS 路径 | `/v1/dialogue-tts/stream` |

## 4. 运行架构

RealTimeVoiceAPI 是一个 FastAPI/Uvicorn 单进程服务，运行一个 asyncio 事件循环。
网络 I/O、队列操作和 Actor 状态变化运行在事件循环线程；CPU 密集或阻塞的音频操作
通过有界执行器运行，不能阻塞全部 Session。

应用级共享资源包括：

- `SessionRegistry`：登记活动 Session，并拒绝重复的活动 `session_id`；
- ASR、BerryThinker、TTS 共用的 `httpx.AsyncClient`，或分别调优的共享客户端；
- 每个下游独立的有界并发限制器和有界等待队列；
- VAD、重采样和较重音频转换使用的有界 CPU 执行器；
- 应用配置和结构化日志；
- Prometheus 兼容指标。

每个 WebSocket Session 对应一个 `SessionRuntime`。它通过
`asyncio.TaskGroup` 管理以下长期协程：

1. **Receiver（接收器）**：解析客户端 JSON，校验序号和 Session，Base64 解码
   PCM16，并将 `AudioChunk` 写入有界音频队列。
2. **VAD Worker**：持续消费音频，流式重采样到 16 kHz，执行每 Session 独立的
   VAD，累积语音并产生 `SpeechSegmentReady` 事件。
3. **ASR Worker**：顺序消费本 Session 的语音段队列，产生 `AsrCompleted`
   或 `AsrFailed` 事件。
4. **Session Actor**：Session/Turn 业务状态的唯一写入者，控制 turn 创建、
   下游启动条件、打断和清理决策。
5. **Sender（发送器）**：从有界下行队列序列化并发送消息，避免慢客户端阻塞 Actor。

`SessionRuntime` 管理协程生命周期；`SessionActor` 管理业务状态。
ASR、BerryThinker 和 TTS 后台任务不得直接修改 Session 状态，只能发送带有
`session_id`、`segment_id` 或 `turn_id` 以及任务代次的强类型事件。
Actor 会丢弃 Session、标识或代次已经过期的事件。

## 5. 源码边界

实现按职责拆分为以下模块：

```text
src/realtime_voice/
├── main.py                 # FastAPI 路由和应用生命周期
├── config.py               # 环境变量与配置校验
├── protocol/
│   ├── client_messages.py  # V1 客户端消息 Pydantic 模型
│   ├── server_messages.py  # V1 服务端消息 Pydantic 模型
│   ├── decoder.py          # JSON/Base64 转内部命令
│   ├── encoder.py          # 内部消息转 V1 JSON
│   └── errors.py           # 稳定的协议错误码
├── transport/
│   └── websocket.py        # 握手、接收、发送和关闭处理
├── audio/
│   ├── pcm.py              # PCM16 校验和 WAV 封装
│   ├── resampler.py        # 有状态的流式重采样
│   └── vad.py              # 每 Session Silero 状态与语音切段
├── session/
│   ├── events.py           # 不可变内部事件类型
│   ├── state.py            # SessionState 与 TurnContext
│   ├── actor.py            # 串行状态转换
│   ├── runtime.py          # TaskGroup 和生命周期管理
│   └── registry.py         # 活动 Session 注册和容量控制
├── clients/
│   ├── asr.py              # OpenAI 兼容 ASR 调用与解析
│   ├── berry.py            # Berry multipart 流、打断和清理
│   ├── tts.py              # 对话式 TTS NDJSON 流解析
│   └── limits.py           # 有界准入和并发控制
└── observability/
    ├── logging.py          # 结构化日志上下文
    └── metrics.py          # 计数器、Gauge 和延迟 Histogram
```

音频、Session 和下游客户端模块不得依赖 WebSocket JSON 模型。未来改为二进制或
Opus 时，只替换协议适配层，不重写 Actor、VAD、ASR 和 TTS 编排。

## 6. V1 WebSocket 协议

WebSocket 只传输 JSON 文本帧。所有服务端下行消息统一包含：`type`、`user_id`、
`session_id`、`turn_id` 和 `interrupt`。Session 级消息使用 `turn_id=0`、
`interrupt=false`。

### 6.1 创建 Session

客户端必须在连接后的 5 秒内发送第一条消息：

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

服务端只接受协议版本 1、`PCM16`、`BASE64_JSON`、单声道，以及
16000、24000、48000 三种采样率。`user_id` 等于客户端提交的 `device_id`。

成功响应：

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

### 6.2 客户端上传音频

```json
{
  "type": "AUDIO_CHUNK",
  "session_id": "session-100",
  "sequence": 0,
  "timestamp_ms": 1787306400123,
  "audio_b64": "AAA0Aaj/cgI="
}
```

`sequence` 从 0 开始，每次严格加 1。`timestamp_ms` 可选，只用于日志和延迟统计。
音频顺序以 WebSocket 到达顺序为准，不使用客户端时间排序。PCM 解码结果必须非空，
且字节数必须为偶数。每个 chunk 必须代表 10～500 ms 音频，建议客户端发送
40～100 ms。上行音频没有 `turn_id`，因为只有非空 ASR 结果才会创建 turn。

### 6.3 ASR 结果

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

ASR 返回空文本时，不创建 turn，也不发送 `ASR_RESULT`。

### 6.4 LLM 文本流

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

BerryThinker 流被打断后仍继续转发文本，但 `interrupt=true`。

### 6.5 TTS 音频流

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

音频 `sequence` 在每个 turn 内从 0 开始。turn 被打断后不再发送它的
`AUDIO_DELTA`。

### 6.6 状态、结束和错误

立即通知打断：

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

turn 结束：

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

`status` 取值为 `COMPLETED`、`INTERRUPTED` 或 `FAILED`。

错误消息：

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

创建 turn 之前的错误使用 `turn_id=0`。客户端可以发送
`{"type":"CLOSE_SESSION","session_id":"session-100"}` 请求正常关闭。

破坏兼容性的协议修改必须使用 `/v2/realtime` 或服务端明确支持的新
`protocol_version`。如果旧客户端可以安全忽略，新增可选字段或消息类型可以保留在 V1。

## 7. 音频与 VAD 流程

上行处理链路：

```text
Base64（Session 采样率的 PCM16）
→ 校验后的 PCM16 字节
→ 有状态地流式重采样到 16 kHz
→ 归一化的单声道 float32
→ 每 Session 独立的 Silero VAD
→ 完整的 16 kHz 语音段
```

VAD 默认值：

| 配置 | 默认值 |
|---|---:|
| 内部采样率 | 16000 Hz |
| 阈值 | 0.5 |
| 最短结束静音 | 500 ms |
| 最长语音段 | 30 秒 |

阈值、结束静音、最长语音、模型路径和 CPU 并发都通过配置校验。
Silero 权重必须在运行时本地可用，服务启动时不得联网下载模型。
当前机器已有 TorchScript 和 ONNX 权重，但实现必须使用已安装依赖或可配置模型路径，
不能硬编码用户缓存目录。

VAD 状态、流式重采样状态、语音缓存、静音时长和 `segment_id` 都属于单个 Session。
语音超过 30 秒时强制结束当前 segment，以限制内存。

完整的 16 kHz segment 封装成 PCM16 WAV 后发送给 ASR 和 BerryThinker。
TTS 返回 24 kHz little-endian PCM16。每个 TTS turn 拥有独立的流式重采样器，
不能在每个 NDJSON chunk 上重置滤波状态。Session 为 24 kHz 时跳过输出重采样。

## 8. 下游接口约定

### 8.1 ASR

调用 `POST /v1/chat/completions`，使用现有 OpenAI 兼容 input-audio 请求。
`input_audio.data` 是完整 WAV 文件的 Base64，`input_audio.format` 为 `wav`。

按照 `BerryThinker/mio_core/tools/qwen_asr_utils.py` 的行为解析
`choices[0].message.content`：提取 `<asr_text>`，去除首尾空白，并将空字符串、
`none`、`null`、`undefined` 视为空文本。不得复制同步 `requests` 调用或固定 URL。

同一 Session 的 ASR worker 顺序处理 segment；不同 Session 通过全局有界准入和
并发限制器并发调用。

### 8.2 BerryThinker

以 multipart/form-data 调用 `POST /api/v1/multimodal/reply`：

| 字段 | 值 |
|---|---|
| `text` | 清洗后的 ASR 文本 |
| `audio` | 完整 16 kHz PCM16 WAV segment |
| `user_id` | `device_id` |
| `session_id` | 实时 Session ID |
| `stream` | `true` |
| `reply_mode` | `dialogue` |
| `audio_is_vad_segment` | `true` |
| `skip_internal_asr` | `true` |

必须跨任意 HTTP chunk 边界增量解析 NDJSON。转发 `text_delta` 事件，
并以 `done.output.reply_text` 作为最终完整回复。

当新有效 turn 打断旧 turn 时，旧 BerryThinker 流仍需完成。旧流完成之后、
下一个 BerryThinker 请求开始之前，调用 `POST /api/v1/interrupt`，JSON 中只传
现有的 `user_id` 和 `session_id`。

### 8.3 PromptDialogAPI

调用 `POST /v1/dialogue-tts/stream`：

```json
{
  "user_input": "清洗后的 ASR 文本",
  "model_reply": "BerryThinker 完整回复",
  "include_prompt_event": false,
  "trace_id": "device-01/session-100/turn-1"
}
```

解析 NDJSON 音频事件，Base64 解码 `audio_i16le_b64`，校验采样率为 24000，
并输出重采样后的 V1 `AUDIO_DELTA`。即使 HTTP 状态已经是 200，流中的
`error` 对象也必须视为 TTS 失败。

## 9. Actor 状态与顺序

Session 状态为 `CONNECTING`、`ACTIVE`、`CLOSING`、`CLOSED`。
只有 `ACTIVE` 状态接受音频。

语音 segment 不是 turn。每个 segment 获得仅服务端内部使用的递增
`segment_id`。只有该 segment 的 ASR 成功返回非空文本后才创建 turn。
公开的 `turn_id` 在每个 Session 内从 1 开始递增。

Turn 阶段：

```text
WAITING_LLM
→ LLM_STREAMING
→ LLM_COMPLETED
→ TTS_STREAMING 或 COMPLETED
→ TTS 期间被打断时进入 TTS_DRAINING
→ COMPLETED
```

`interrupted` 是独立且单调的布尔状态，不是唯一阶段。一旦为 true 就不会恢复为 false。
因此被打断的 turn 仍可处于 `LLM_STREAMING` 或 `TTS_DRAINING`，且其后续消息
可以正确携带 `interrupt=true`。

Actor 强制执行以下启动条件：

- 只有 `SpeechSegmentReady` 可以进入 ASR 队列；
- 只有非空的 `AsrCompleted` 可以分配 turn；
- 只有本 Session LLM 队列头部可以调用 BerryThinker；
- 只有已完成且未被打断的 BerryThinker 回复可以启动 TTS；
- 只有属于已知且未被打断 TTS turn 的音频才能进入下行队列；
- Session、turn、segment 或任务代次过期的事件必须丢弃并计数。

如果 turn 1、2、3 快速连续产生，BerryThinker 仍按顺序处理全部三个 turn，
保证短期历史完整。turn 1 和 turn 2 继续发送 `interrupt=true` 的文本并跳过 TTS；
只有在没有更新 turn 打断的情况下，turn 3 才启动 TTS。

## 10. 打断语义

VAD 检测到开始说话时不打断；ASR 空文本不打断；ASR 非空文本才创建新 turn，
并在一次 Actor 事件处理中将所有未结束的旧 turn 标为 `interrupted=true`。
Actor 为每个首次被标记打断的 turn 立即发送一次 `TURN_STATE`。

旧 turn 正在生成 LLM 时：

- 不取消 BerryThinker；
- 继续发送文本，且 `interrupt=true`；
- 等待正常的 `done` 事件；
- 跳过 TTS；
- 发送 `RESPONSE_END(status=INTERRUPTED)`；
- 在下一个 BerryThinker 请求之前串行调用 Berry interrupt。

旧 turn 正在生成 TTS 时：

- 立即停止发送该 turn 的音频；
- 继续读取并丢弃其 HTTP 流；
- 使用明确超时限制排空时间；
- 新 turn 的 LLM/TTS 不等待旧 TTS 排空；
- 旧流结束或排空超时关闭后，发送被打断的结束状态。

## 11. 错误、背压与超时

阶段错误规则：

- ASR 失败时发送可恢复的 `ERROR`，`turn_id=0`；不创建 turn，不打断旧 turn。
- BerryThinker 失败时发送 `ERROR(stage=LLM)` 和
  `RESPONSE_END(status=FAILED)`；后续 turn 可以继续。
- TTS 失败时保留已发送文本，发送 `ERROR(stage=TTS)` 和
  `RESPONSE_END(status=FAILED)`；后续 turn 可以继续。
- 下游准入队列已满时，以 `SERVICE_OVERLOADED` 结束对应 segment/turn，
  不创建无界任务。

V1 配置默认值：

| 限制或超时 | 默认值 |
|---|---:|
| 活动 Session | 64 |
| 握手超时 | 5 秒 |
| 单条 WebSocket JSON | 1 MiB |
| Actor 事件队列 | 256 个事件 |
| 音频队列 | 64 条消息，且累计音频不超过 3 秒 |
| 下行队列 | 256 条消息，且累计不超过 8 MiB |
| 上行积压音频 | 3 秒 |
| 音频 chunk 时长 | 10～500 ms |
| 语音 segment | 30 秒 |
| CPU 执行器线程数 | 4 |
| CPU 执行器等待任务 | 128 |
| ASR 活动请求/等待任务 | 8 / 64 |
| Berry 活动请求/等待任务 | 8 / 64 |
| TTS 活动请求/等待任务 | 8 / 64 |
| ASR 请求 | 30 秒 |
| Berry 总请求 | 180 秒 |
| TTS 首段音频 | 60 秒 |
| TTS 流空闲 | 30 秒 |
| 被打断 TTS 排空 | 120 秒 |
| 断线等待活动 Berry | 120 秒 |

下行缓存同时受消息数和字节数限制，单个慢客户端不能无限占用音频内存。
上行持续积压时发送 `CLIENT_AUDIO_BACKPRESSURE` 并关闭连接；
下行持续积压时发送或记录 `SLOW_CLIENT` 并关闭连接。

下游最大并发和等待队列分别可配置。表中数值是 V1 默认值，并通过健康检查和指标
暴露，部署时可根据实际 ASR/TTS 实例数量调整。

## 12. 断线与清理

V1 不恢复断线 Session。客户端重连必须使用新的 `session_id`；重复的活动 ID 被拒绝。

清理顺序：

1. 将 Session 标记为 `CLOSING`，拒绝后续音频；
2. 停止 Receiver 和 VAD worker，丢弃未完成的语音段；
3. 取消排队中和正在执行的 ASR，因为 ASR 不产生 Berry 历史副作用；
4. 停止所有 WebSocket 下行；
5. 活动 BerryThinker 流最多等待 120 秒以完成；
6. 活动 TTS 流最多排空 120 秒，期间不发送音频；
7. Berry 工作完成后调用
   `DELETE /api/v1/sessions/{user_id}/{session_id}`；
8. Berry 清理返回 HTTP 200 或 404 均视为成功；
9. Berry 超时未完成时跳过 DELETE 并记录告警，由其自身有界 Agent 池安全淘汰；
10. 关闭响应和任务，清空队列与状态，从注册表移除，标记为 `CLOSED`。

已经核对 Berry DELETE 实现：它从进程内 Session Agent 池移除并关闭 Agent，
会丢失该 Session 的 Berry 进程内短期历史，但不会执行 Mem0、SQLite、事件记忆或
用户画像删除。由于 V1 不恢复断线 Session，该行为符合本设计。

## 13. 可观测性

结构化日志按需包含 `user_id`、`session_id`、`turn_id`、`segment_id`、
`stage`、`event`、`duration_ms`、`queue_wait_ms`、`interrupt`、`error_code`。
默认不得记录音频字节或完整对话文本。

`GET /health` 返回进程就绪状态、活动 Session 数、容量配置、队列/限制器使用情况、
CPU 执行器状态和缓存的下游健康状态。健康检查不能每次同步等待全部下游；
下游探测使用短超时异步运行并缓存结果。

`GET /metrics` 至少暴露：

- 活动 WebSocket 和 Session；
- event-loop lag；
- 队列长度、准入、过载和等待时间；
- VAD、ASR、Berry、TTS 的延迟和错误数；
- speech end 到 ASR 结果、首个 LLM delta、首段 TTS 音频的延迟；
- 被打断 turn 和丢弃的 TTS chunk/字节数；
- 慢客户端关闭数；
- 执行器活动、进程线程数和内存。

## 14. 测试与验证策略

采用测试驱动开发。协议、状态机和客户端行为使用确定性的假 HTTP 服务测试，
默认测试不依赖真实模型服务。

单元测试覆盖：

- 所有 V1 消息模型、下行公共字段、版本检查、Base64/PCM 校验、序号检查和稳定错误码；
- 16/24/48 kHz 输入转换、流式重采样连续性、PCM/WAV 封装、VAD 静音/噪声/
  语音/强制切段以及 Session 隔离；
- 空 ASR、turn 分配、ASR/LLM 顺序、快速连续 turn、LLM 打断、TTS 排空、
  过期任务代次、各类失败和清理超时；
- ASR 返回清洗，以及跨任意 HTTP chunk 边界的 NDJSON 解析；
- TTS prompt、audio、error 事件，包括 HTTP 200 后流内错误。

集成测试启动假的 ASR、BerryThinker 和 TTS 应用，覆盖：

- 一次完整 VAD→ASR→LLM→TTS；
- LLM 阶段打断；
- TTS 阶段打断，并验证新 TTS 不等待旧流排空；
- 非法或慢客户端、队列饱和、下游失败和超时；
- 断线清理顺序和条件式 Berry DELETE。

可选真实服务冒烟客户端连接 8000、8082、8002，发送测试 WAV，验证
`ASR_RESULT`、`TEXT_DELTA`/`TEXT_END`、`AUDIO_DELTA`，并将返回 PCM
写入 WAV 供人工试听。该测试依赖模型和 GPU，不进入默认测试套件。

压测脚本驱动 30、40、60 个并发 WebSocket，记录建连成功率、event-loop lag、
阶段延迟、队列等待、首段音频延迟、错误率、内存和线程数。

验收要求：

- 至少 30 个客户端可同时上传并接收流式输出；
- 没有无界队列、无界线程或持续内存增长；
- VAD/重采样不造成持续事件循环阻塞；
- ASR 空文本不创建或打断 turn；
- 每 Session 的 Berry 顺序和公开 `turn_id` 一致；
- 所有下行消息的 `interrupt` 正确；
- 旧 Berry 完成、旧 TTS 音频不下发、新 TTS 不等待旧流；
- 下游容量足够时，speech end 到首段有效 TTS 音频不超过 5 秒。

## 15. 交付物

实现将新增：

- `pyproject.toml` 和可复现的依赖、质量配置；
- 上述 `src/realtime_voice` 完整服务；
- 单元测试和假服务集成测试；
- 有文档说明的 WebSocket 示例客户端；
- 并发压测工具；
- 包含全部运行配置的 `.env.example`；
- 更新后的顶层 `README.md`，包含协议、启动、冒烟测试和部署说明；
- supervisord 和/或 systemd 示例配置，但不修改用户机器上的服务状态。
