# 实时语音 API 接入说明（V1）

连接 `ws://<服务地址>:8000/v1/realtime`，端口及 `wss` 地址以部署方提供为准。所有消息均为 **WebSocket 文本帧中的 JSON 对象**，不要添加未定义字段。

流程：**创建会话 → 持续上传音频（包括静音）→ 服务端确认用户输入结束 → 接收识别、回复文本和音频 → 结束会话**。上传和接收必须同时进行，同一连接支持多轮对话。

服务端默认使用“候选静音 + ASR 文本合并 + 语义完整判断 + 最长静音兜底”确认用户是否说完。该能力由服务端统一配置，创建会话时没有语义判断参数，客户端只需持续、实时地发送包含静音的音频流。

## 1. 创建会话

连接后 **5 秒内**发送以下消息，收到 `SESSION_CREATED` 后开始上传音频：

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

| 字段 | 要求 |
|---|---|
| `type`、`protocol_version` | 固定为 `"CREATE_SESSION"`、`1` |
| `device_id` | 设备/用户标识，1～128 字符；下行 `user_id` 取此值 |
| `session_id` | 会话标识，1～128 字符；建议每次连接使用新 UUID，不得与活跃会话重复 |
| `audio_format`、`audio_transport`、`channels` | 固定为 `"PCM16"`、`"BASE64_JSON"`、`1` |
| `sample_rate` | `16000`、`24000` 或 `48000`；必须与上传音频一致 |
| `rag_enabled` | 可选，布尔值，默认 `false`；开启知识检索时传 `true` |
| `scenes` | 可选，字符串数组，默认 `[]`；开启 RAG 时必传 1～3 个场景名，由部署方提供 |

除两个 RAG 字段外，其余字段必填。场景名去除首尾空白、去重后最多三个，不允许空名称。**不需要 RAG 时，省略 `rag_enabled` 和 `scenes` 即可。**

RAG 配置在会话内固定，修改需重新建连。开启后每轮在识别完成后检索知识，默认最多等待 2 秒；无结果或检索失败时继续普通回答。没有独立的 RAG 状态消息，`SESSION_CREATED` 也不回显 RAG 参数。

## 2. 上传音频

持续发送 `AUDIO_CHUNK`，包括说话后的静音。以下音频值仅为短静音示例：

```json
{
  "type": "AUDIO_CHUNK",
  "session_id": "session-demo-001",
  "sequence": 0,
  "timestamp_ms": 0,
  "audio_b64": "AAAAAA=="
}
```

- `session_id`：与创建时一致。
- `sequence`：从 `0` 开始，每块加 `1`，**跨轮次继续累计**。
- `timestamp_ms`：可选，非负整数，表示音频流中的毫秒位置。
- `audio_b64`：裸 **PCM16、小端、单声道**采样的 Base64，非空且解码后字节数为偶数；不要发送 WAV 文件头、MP3 或 Float32。

建议每块 **20～40ms**，按实时速度发送，单块最多 **500ms**。默认连续静音达到 500ms 时，服务端截取一个候选片段并调用一次 ASR；该候选结果只在服务端累积，不会立即下发。服务端用当前输入已合并的 ASR 文本和最近会话上下文判断语义是否完整：

- 语义完整且静音达到 1000ms：提交整段输入。
- 用户在提交前恢复说话：旧判断失效，后续候选 ASR 文本合并到同一输入，不创建新轮次。
- 语义判断要求继续等待、超时、失败或过载：继续收音，静音达到 2000ms 时保底提交。
- 单次输入持续达到 30 秒：不再等待语义结果，强制提交。

以上是当前服务端默认值，部署方可以调整。说完后必须继续上传静音，文件测试建议补 **2200ms 静音音频**；仅等待而不发音频不会推进判断窗口。不需要发送“提交”或“语音结束”消息。

## 3. 接收结果

每条消息都有 `type`、`user_id`、`session_id`、`turn_id`、`interrupt`。有效轮次的 `turn_id` 从 `1` 递增，建连及无轮次错误使用 `0`。

| `type` | 关键字段 | 客户端处理 |
|---|---|---|
| `SESSION_CREATED` | `protocol_version`、音频格式、`sample_rate`、`channels` | 创建成功，开始上传 |
| `ASR_RESULT` | `text` | 展示已提交整段输入的最终合并识别文本；每个用户轮次只下发一次 |
| `TEXT_DELTA` | `delta` | 按轮次追加回复文本 |
| `TEXT_END` | `text` | 完整回复，替换/落定展示，不要重复追加 |
| `AUDIO_DELTA` | `sequence`、`audio_b64`、`audio_format`、`sample_rate`、`channels` | 解码 PCM16，按该消息采样率顺序播放；序号每轮从 `0` 开始 |
| `TURN_STATE` | `state: "INTERRUPTED"` | 立即停止该轮播放，清空其音频缓冲 |
| `RESPONSE_END` | `status: "COMPLETED" / "INTERRUPTED" / "FAILED"` | 该轮结束，连接仍可继续使用 |
| `ERROR` | `stage`、`code`、`message`、`recoverable` | 按第 5 节处理 |

正常顺序：`ASR_RESULT → TEXT_DELTA → TEXT_END → AUDIO_DELTA → RESPONSE_END`。客户端看不到内部候选 ASR 结果，也不需要自行拼接文本。文本完成后才开始合成音频；`RESPONSE_END` 不代表客户端播放队列已经播完。纯静音或整段候选均为空转写时可能没有任何结果，客户端需自行设置等待超时。

## 4. 多轮、打断与关闭

继续上传音频即可开始下一轮，**不要重复创建会话或重置上行序号**。

新的用户输入正式提交时会打断未完成的旧轮；短停顿后继续说话仍属于同一输入，不会触发打断。提交新输入时，服务端先发送旧轮的 `TURN_STATE/INTERRUPTED`，再发送新轮的 `ASR_RESULT`。收到打断消息后停止旧轮播放；后续 `interrupt=true` 的旧消息不能覆盖新轮内容。按 `turn_id` 处理交错消息，不要因旧轮结束而停止整个接收循环。检索阶段被打断的轮次可能直接结束，没有回复文本或音频。

整个对话结束后发送以下消息，或直接断开连接：

```json
{"type":"CLOSE_SESSION","session_id":"session-demo-001"}
```

需要完整回复时，先等对应轮的 `RESPONSE_END`。没有 `SESSION_CLOSED` 回执，断线重连不恢复旧会话。

## 5. 错误处理

- `recoverable=false`：通常为参数或协议错误，服务端关闭连接；根据 `code` 修正后重新建连。常见原因是字段错误、RAG 场景不合法、音频序号不连续、上传过快或接收过慢。
- `recoverable=true`：连接可以继续使用，不代表自动重试。任一候选片段发生 `stage=ASR` 错误时，当前尚未提交的整段输入会被放弃，该错误没有对应 `ASR_RESULT` 或 `RESPONSE_END`；客户端可以继续发送下一段输入。语义判断超时、失败或过载不会下发 `ERROR`，服务端会等待最长静音兜底。`stage=LLM/TTS` 的错误会终结对应轮次。
- 协议错误的 `user_id`、`session_id` 可能为 `"unknown"`，不要因标识不匹配而丢弃错误。
- WebSocket 已建立后，创建失败会先发送 `ERROR`，再发送关闭帧；客户端应优先展示 `code` 和 `message`，不要被随后通用的断线提示覆盖。关闭帧的 `reason` 也携带错误码。

| 创建阶段错误码 | 原因与处理 | 关闭码 |
|---|---|---|
| `INVALID_MESSAGE` / `CREATE_SESSION_REQUIRED` | 请求格式或首条消息错误，修正请求 | `1008` |
| `HANDSHAKE_TIMEOUT` | 未及时发送创建请求 | `1008` |
| `DUPLICATE_SESSION` | 会话 ID 仍被占用，更换 ID 或等待旧会话清理完成 | `1008` |
| `SESSION_CAPACITY_EXCEEDED` | 实例会话名额已满，等待名额释放后重试 | `1008` |
| `SESSION_CREATE_FAILED` | 服务端初始化会话失败，稍后重试；详细异常记录在服务端日志 | `1011` |

这些错误均为 `stage=TRANSPORT`、`turn_id=0`、`recoverable=false`，表示本连接无法继续，重试需要新建连接。TCP/TLS/HTTP 升级失败、进程退出或网络已断开时，服务端无法保证交付 JSON 错误，客户端需保留网络错误提示。

无需自行编写客户端时，可使用网关 `/test/` 麦克风测试页；端口转发方式见 [README](../README.md#浏览器麦克风测试台)。

部署、服务端配置与排障见 [README](../README.md)。
