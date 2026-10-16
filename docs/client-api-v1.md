# 实时语音 API V1 快速接入

网关入口：

- WebSocket：`ws://<host>:8000/v1/realtime`
- 健康检查：`http://<host>:8000/health`
- 浏览器测试台：`http://<host>:8000/test/`

实际处理链路：

```text
客户端音频 → VAD → ASR → 语义轮次结束判断 → Thinker（内部可选 RAG）→ TTS → 客户端
```

WebSocket 上下行均使用**文本帧 JSON**。一个连接支持多轮对话。
生产环境如果通过 HTTPS 暴露服务，请使用对应的 `wss://` 地址。

## 1. 最快调试方式

### 1.1 检查服务

```bash
curl http://127.0.0.1:8000/health
```

必须检查响应中的 `ready`，HTTP 200 不等于服务可用：

```json
{
  "status": "ok",
  "ready": true,
  "downstream": {
    "asr": {"status": "ok"},
    "thinker": {"status": "ok"},
    "tts": {"status": "healthy"}
  }
}
```

## 2. 客户端交互流程

1. 建立 WebSocket 连接。
2. 在 5 秒内发送 `CREATE_SESSION`。
3. 等待并校验 `SESSION_CREATED`。
4. 可选：按第 4.1 节发送 `SPEAKER_REGISTER` 注册会话声纹；需要更新时重复相同流程。
5. 持续发送 `AUDIO_CHUNK`，说完后也要继续发送静音。
6. 按 `turn_id` 接收 `ASR_RESULT`、文本、音频和 `RESPONSE_END`。
7. 下一轮继续发送音频；上行 `sequence` 不重置。
8. 对话结束时发送 `CLOSE_SESSION` 或直接断开。

## 3. 创建会话

```json
{
  "type": "CREATE_SESSION",
  "protocol_version": 1,
  "device_id": "device-demo",
  "session_id": "session-demo-001",
  "audio_format": "PCM16",
  "audio_transport": "BASE64_JSON",
  "sample_rate": 16000,
  "channels": 1,
  "rag_enabled": true,
  "scenes": ["农业社会"]
}
```

| 字段 | 说明 |
|---|---|
| `device_id` | 设备/用户标识，1～128 字符；下行 `user_id` 与其相同 |
| `session_id` | 会话标识，1～128 字符；活跃会话之间不能重复 |
| `sample_rate` | `16000`、`24000` 或 `48000`，必须与上传音频一致 |
| `audio_format` | 固定为 `PCM16` |
| `audio_transport` | 固定为 `BASE64_JSON` |
| `channels` | 固定为 `1` |
| `rag_enabled` | 可选，默认 `false` |
| `enable_turn_metrics` | 可选布尔值，默认 `false`；开启后额外接收逐轮 `TURN_METRICS` 快照 |
| `scenes` | 可选，默认 `[]`；去空白、去重后最多 3 个非空场景名 |

除 RAG 和逐轮指标字段外，其余字段必填，不要添加未定义字段。

### 3.1 逐轮计时（可选）

创建会话时设置 `"enable_turn_metrics": true`。未开启的会话不会收到新消息；协议版本仍为 1。测试台默认开启。计时均使用网关的单调时钟，单位为毫秒，不包括浏览器收包或播放缓冲。

网关在 `ASR_RESULT` 建立轮次后推送首个快照，首段非空回复文本、首块可发送音频和本轮结束时更新。打断时立即冻结已有数据；终态快照在 `RESPONSE_END` 之前发送。客户端以 `(session_id, turn_id)` 关联，用单轮递增的 `sequence` 拒绝旧快照，并忽略终态后的更新。

```json
{
  "type": "TURN_METRICS",
  "user_id": "device-demo",
  "session_id": "session-demo-001",
  "turn_id": 1,
  "interrupt": false,
  "sequence": 4,
  "status": "completed",
  "commit_reason": "max_silence",
  "fallback": false,
  "asr": {"ms": 100, "queue_ms": 10, "call_ms": 90, "status": "completed"},
  "semantic": {"ms": 20, "queue_ms": 2, "call_ms": 18, "status": "wait"},
  "llm": {"ms": 200, "queue_ms": 0, "call_ms": 200, "status": "completed"},
  "tts": {"ms": 100, "queue_ms": 0, "call_ms": 100, "status": "completed"},
  "total": {"ms": 800, "status": "completed"},
  "pre_segment_silence_ms": 500,
  "post_segment_wait_ms": 380,
  "details": [
    {"stage": "asr", "segment_id": 1, "generation": 0, "offset_ms": 0,
     "ms": 100, "queue_ms": 10, "call_ms": 90, "status": "completed"}
  ]
}
```

| 字段 | 计时口径 |
|---|---|
| `asr` | 最终有效语音段的 ASR 排队与识别；声纹预检单列，不混入 ASR |
| `semantic` | 最终语音版本、最终分段组合对应的语义任务，提交到返回，包含执行器排队 |
| `llm` | 输入提交并进入 LLM 队列到网关收到首段非空文本，包含会话排队、准入和 Thinker 内部 RAG |
| `tts` | 网关提交合成任务到首块可发送音频，包含排队、通信和必要重采样；双向流包含后续文本等待 |
| `total` | **最终有效语音段由 VAD 交出到网关首块可发送音频**，包含之后的处理与等待 |
| `pre_segment_silence_ms` | 最终切段前已确认的尾部静音，不计入 `total` |
| `post_segment_wait_ms` | 最终切段后，仅等待静音阈值且没有 ASR/语义任务排队或执行的区间，已包含在 `total` |
| `details` | 全轮各段声纹/ASR、所有语义尝试和 LLM/TTS 明细；`offset_ms` 是任务提交相对最终切段的偏移，可为负值 |

阶段的 `queue_ms` 包含网关会话排队与下游准入等待；`call_ms` 为其余调用耗时，包含无法从网关拆分的下游内部排队。LLM/TTS 可重叠，不能将阶段值直接相加得到总耗时。

顶层 `status` 为 `pending`、`completed`、`interrupted` 或 `failed`。阶段还可能为 `end`、`wait`、`not_run`、`cancelled`、`timeout`、`not_produced`。成功到达的里程碑保留数值；缺失里程碑的 `ms` **省略**，不能当成 0。失败/取消尝试的已耗时间仅在 `details` 展示。语义未执行时显示 `not_run`，是否静音兜底由 `commit_reason` 判断；`fallback=true` 表示使用网关兜底文本，该文本不作为正常 LLM 首包。

单独的 Prometheus 指标 `realtime_voice_vad_ready_to_first_tts_seconds` 使用相同总耗时口径，统计所有会话，不添加用户/轮次标签。已有指标保留原含义。

RAG 在整个会话内固定：

- `rag_enabled=false`：Thinker 请求不包含 RAG 参数；
- `rag_enabled=true, scenes=[]`：Thinker 检索通用知识；
- `rag_enabled=true` 且有场景：Thinker 检索通用知识和指定场景知识。

网关不访问 KBService，只在每轮请求中把 RAG 配置传给 Thinker。协议当前不返回独立的 RAG 命中或降级状态。

创建成功响应：

```json
{
  "type": "SESSION_CREATED",
  "protocol_version": 1,
  "user_id": "device-demo",
  "session_id": "session-demo-001",
  "turn_id": 0,
  "interrupt": false,
  "audio_format": "PCM16",
  "audio_transport": "BASE64_JSON",
  "sample_rate": 16000,
  "channels": 1
}
```

## 4. 上传音频

```json
{
  "type": "AUDIO_CHUNK",
  "session_id": "session-demo-001",
  "sequence": 0,
  "timestamp_ms": 0,
  "audio_b64": "AAAAAA=="
}
```

音频要求：

- PCM16、小端、单声道裸数据的 Base64；不要包含 WAV 头，不支持 MP3/Float32；
- 建议每块 20～40ms，单块不能超过 500ms；
- `sequence` 从 0 开始严格递增，**跨轮次不能重置**；
- `timestamp_ms` 可选，表示音频流中的位置；
- 按真实时间发送，不要瞬间灌入整段音频。

客户端不发送“说完了”消息。当前默认行为是：静音约 500ms 后产生候选 ASR，网关结合合并文本和上下文判断语义是否完整；如果继续说话，仍合并到本次输入；静音达到 2000ms 或单次输入达到 30 秒时强制提交。

因此，说完后必须继续上传静音。文件测试建议补至少 2200ms 静音；只等待、不发送音频不会推进静音计时。

### 4.1 注册或替换会话声纹

声纹只保存在当前 Session，断开后清除。未注册时普通音频不过滤；注册成功后，普通语音会先通过声纹验证，再进入 ASR。

#### 客户端操作

1. 收到 `SESSION_CREATED` 后，录制约 3 秒目标用户的清晰单人语音。
2. 暂停发送普通 `AUDIO_CHUNK`。
3. 将整段录音作为一条 `SPEAKER_REGISTER` 发送。首次注册和更新声纹使用相同消息，每次使用新的 `request_id`。
4. 等待 `request_id` 匹配的 `SPEAKER_REGISTER_RESULT`，再恢复发送普通音频。

```json
{
  "type": "SPEAKER_REGISTER",
  "session_id": "session-demo-001",
  "request_id": "register-001",
  "audio_format": "PCM16",
  "sample_rate": 16000,
  "channels": 1,
  "audio_b64": "AAAAAA=="
}
```

注册音频必须是 PCM16、小端、单声道裸数据的 Base64，不包含 WAV 头；采样率必须与 Session 一致。音频最长 10 秒，当前默认至少 2000ms。注册音频只用于提取声纹，不进入对话流程。

#### 注册结果

```json
{
  "type": "SPEAKER_REGISTER_RESULT",
  "user_id": "device-demo",
  "session_id": "session-demo-001",
  "turn_id": 0,
  "interrupt": false,
  "request_id": "register-001",
  "success": true,
  "registered": true,
  "replaced": false,
  "code": "REGISTERED",
  "message": "speaker voiceprint registered",
  "duration_ms": 3000.0,
  "min_audio_ms": 2000.0
}
```

客户端应先匹配 `request_id`，再按以下组合处理：

| `success` | `registered` | `replaced` | 含义 |
|---:|---:|---:|---|
| `true` | `true` | `false` | 首次注册成功，`code=REGISTERED` |
| `true` | `true` | `true` | 更新成功，`code=UPDATED`，新声纹已覆盖旧声纹 |
| `false` | `false` | `false` | 注册失败，当前仍无声纹 |
| `false` | `true` | `false` | 更新失败，原声纹继续有效 |

失败码包括 `AUDIO_TOO_SHORT`、`NO_SPEECH`、`EMBEDDING_FAILED`、`EMBEDDER_UNAVAILABLE` 和 `SPEAKER_DISABLED`。`NO_SPEECH` 表示录音中没有检测到可用的声音信号。只有新声纹提取成功才会覆盖旧声纹。

#### 关键规则

- 同一时刻只能有一个注册请求；并发注册会返回 `SPEAKER_REGISTER_IN_PROGRESS` 并关闭连接。
- 注册处理中收到的普通 `AUDIO_CHUNK` 会被服务端直接丢弃，但其 `sequence` 仍会被校验并推进。
- 消息格式、采样率或音频上限不合法时，服务端返回不可恢复的 `ERROR`，不会返回 `SPEAKER_REGISTER_RESULT`。
- 注册成功后，客户端可通过 `SPEAKER_STATUS.allowed` 判断普通语音是已放行还是已过滤；`similarity` 可能为 `null`。
- 更新失败不会影响原声纹；当前协议没有单独删除声纹的消息，结束 Session 即删除。

## 5. 接收消息

所有下行消息都包含：

```text
type, user_id, session_id, turn_id, interrupt
```

| `type` | 关键字段 | 客户端处理 |
|---|---|---|
| `SESSION_CREATED` | 协商后的音频参数 | 开始上传音频 |
| `SPEAKER_REGISTER_RESULT` | `request_id`, `success`, `registered`, `replaced`, `code` | 结束注册暂停；成功后记录已注册/已修改，失败时保留旧状态 |
| `SPEAKER_STATUS` | `state`, `registered`, `similarity`, `allowed` | 展示普通语音的声纹匹配或过滤结果 |
| `ASR_RESULT` | `text` | 本轮最终合并识别文本 |
| `TEXT_DELTA` | `delta` | 追加流式回复文本 |
| `TEXT_END` | `text` | 本轮最终文本，以它覆盖并落定增量文本 |
| `AUDIO_DELTA` | `sequence`, `audio_b64`, `sample_rate` | 按序解码并播放 PCM16；每轮音频序号从 0 开始 |
| `TURN_STATE` | `state="INTERRUPTED"` | 停止该轮播放并清空该轮音频缓冲 |
| `RESPONSE_END` | `status` | 本轮结束，连接仍可继续使用 |
| `ERROR` | `stage`, `code`, `message`, `recoverable` | 按第 7 节处理 |

典型成功顺序：

```text
ASR_RESULT → TEXT_DELTA... → TEXT_END → AUDIO_DELTA... → RESPONSE_END(COMPLETED)
```

`TEXT_DELTA` 和 `AUDIO_DELTA` 可能有多条；某些回复也可能没有 `TEXT_DELTA`。`RESPONSE_END` 表示服务端已结束该轮，不代表客户端音频播放队列已经播完。

## 6. 多轮与打断

继续上传音频即可开始下一轮，不要重复发送 `CREATE_SESSION`。

当新的用户输入正式提交时，网关会打断尚未结束的旧轮：

1. 旧轮收到 `TURN_STATE/INTERRUPTED`；
2. 新轮收到新的 `ASR_RESULT`；
3. 旧轮后续消息带原 `turn_id`，可能与新轮消息交错；
4. 旧轮最终收到 `RESPONSE_END/INTERRUPTED`。

客户端必须按 `turn_id` 分开维护文本和音频状态。收到 `TURN_STATE` 后立即停止旧轮播放；不要因为旧轮结束而关闭整个 WebSocket。

## 7. 错误与保底

`ERROR.recoverable` 的处理规则：

- `false`：当前连接不能继续，修正问题后重新连接；
- `true`：连接仍可继续，但客户端需要决定是否重试当前操作。

常见问题：

| 错误码/阶段 | 原因 |
|---|---|
| `HANDSHAKE_TIMEOUT` | 5 秒内未发送 `CREATE_SESSION` |
| `INVALID_JSON` / `INVALID_MESSAGE` | JSON、字段或文本帧格式错误 |
| `DUPLICATE_SESSION` | `session_id` 已被活跃连接占用 |
| `SESSION_CAPACITY_EXCEEDED` | 网关会话容量已满 |
| `AUDIO_SEQUENCE_GAP` | 上行音频序号不连续或跨轮重置 |
| `CLIENT_AUDIO_BACKPRESSURE` | 音频发送过快，网关已丢弃当前音频块；`recoverable=true` 时连接保持可用，当前话语可能不完整，客户端应提示用户停顿后重说 |
| `SESSION_ID_MISMATCH` | 消息中的 `session_id` 与当前连接不一致 |
| `SPEAKER_REGISTER_SAMPLE_RATE` | 注册音频采样率与当前 Session 不一致 |
| `SPEAKER_REGISTER_DURATION` | 注册音频超过 10 秒 |
| `SPEAKER_REGISTER_IN_PROGRESS` | 前一次声纹注册尚未完成又发起了新注册 |
| `stage=ASR` | 当前尚未提交的输入失败，不会产生对应 `RESPONSE_END` |
| `stage=TTS` | 当前轮以 `RESPONSE_END/FAILED` 结束 |

默认开启 Thinker 保底：Thinker 超时、连接失败、流异常或空回复时，网关发送一条保底 `TEXT_END`，继续合成音频，最终通常仍为 `RESPONSE_END/COMPLETED`。若失败前已有 `TEXT_DELTA`，以最终 `TEXT_END.text` 为准。

## 8. 关闭会话

```json
{
  "type": "CLOSE_SESSION",
  "session_id": "session-demo-001"
}
```

需要完整回复时，先等待当前轮的 `RESPONSE_END`。服务端没有 `SESSION_CLOSED` 回执，断线重连也不会恢复旧会话。

## 9. 联调检查清单

- `/health` 返回 `ready=true`；
- 首帧是 `CREATE_SESSION`，且在连接后 5 秒内发送；
- 只发送 WebSocket 文本帧 JSON；
- 音频是与会话采样率一致的单声道 PCM16；
- `sequence` 严格递增且跨轮不重置；
- 首次注册和更新声纹都使用 `SPEAKER_REGISTER`，每次使用新的 `request_id`；
- 注册期间暂停普通音频，收到匹配的 `SPEAKER_REGISTER_RESULT` 后再恢复；
- 使用 `success` 判断本次操作、使用 `registered` 判断当前会话是否已有可用声纹；
- 说完后仍持续发送至少 2 秒静音；
- 按 `turn_id` 管理文本、音频和打断状态；
- 用 `TEXT_END` 落定文本，用 `RESPONSE_END` 判断服务端轮次结束。

服务部署、配置和监控说明见 [README](../README.md)。
