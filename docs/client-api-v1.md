# 实时语音 API 接入说明（V1）

连接 `ws://<服务地址>:8000/v1/realtime`，端口及 `wss` 地址以部署方提供为准。所有消息均为 **WebSocket 文本帧中的 JSON 对象**，不要添加未定义字段。

流程：**创建会话 → 持续上传音频 → 接收识别、回复文本和音频 → 结束会话**。上传和接收必须同时进行，同一连接支持多轮对话。

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

建议每块 **20～40ms**，按实时速度发送，单块最多 **500ms**。服务端自动切句；说完后继续上传静音，文件测试建议补 **600ms 静音音频**，仅等待而不发音频无效。不需要发送“提交”或“语音结束”消息。

## 3. 接收结果

每条消息都有 `type`、`user_id`、`session_id`、`turn_id`、`interrupt`。有效轮次的 `turn_id` 从 `1` 递增，建连及无轮次错误使用 `0`。

| `type` | 关键字段 | 客户端处理 |
|---|---|---|
| `SESSION_CREATED` | `protocol_version`、音频格式、`sample_rate`、`channels` | 创建成功，开始上传 |
| `ASR_RESULT` | `text` | 展示用户语音的最终识别文本 |
| `TEXT_DELTA` | `delta` | 按轮次追加回复文本 |
| `TEXT_END` | `text` | 完整回复，替换/落定展示，不要重复追加 |
| `AUDIO_DELTA` | `sequence`、`audio_b64`、`audio_format`、`sample_rate`、`channels` | 解码 PCM16，按该消息采样率顺序播放；序号每轮从 `0` 开始 |
| `TURN_STATE` | `state: "INTERRUPTED"` | 立即停止该轮播放，清空其音频缓冲 |
| `RESPONSE_END` | `status: "COMPLETED" / "INTERRUPTED" / "FAILED"` | 该轮结束，连接仍可继续使用 |
| `ERROR` | `stage`、`code`、`message`、`recoverable` | 按第 5 节处理 |

正常顺序：`ASR_RESULT → TEXT_DELTA → TEXT_END → AUDIO_DELTA → RESPONSE_END`。文本完成后才开始合成音频；`RESPONSE_END` 不代表客户端播放队列已经播完。纯静音或空转写可能没有任何结果，客户端需自行设置等待超时。

## 4. 多轮、打断与关闭

继续上传音频即可开始下一轮，**不要重复创建会话或重置上行序号**。

新语音识别成功时会打断未完成的旧轮。收到 `TURN_STATE/INTERRUPTED` 后停止旧轮播放；后续 `interrupt=true` 的旧消息不能覆盖新轮内容。按 `turn_id` 处理交错消息，不要因旧轮结束而停止整个接收循环。检索阶段被打断的轮次可能直接结束，没有回复文本或音频。

整个对话结束后发送以下消息，或直接断开连接：

```json
{"type":"CLOSE_SESSION","session_id":"session-demo-001"}
```

需要完整回复时，先等对应轮的 `RESPONSE_END`。没有 `SESSION_CLOSED` 回执，断线重连不恢复旧会话。

## 5. 错误处理

- `recoverable=false`：通常为参数或协议错误，服务端关闭连接；根据 `code` 修正后重新建连。常见原因是字段错误、RAG 场景不合法、音频序号不连续、上传过快或接收过慢。
- `recoverable=true`：连接可以继续使用，不代表自动重试。`stage=ASR` 的错误没有对应 `RESPONSE_END`；`stage=LLM/TTS` 的错误会终结该轮。
- 协议错误的 `user_id`、`session_id` 可能为 `"unknown"`，不要因标识不匹配而丢弃错误。
- 未收到 `SESSION_CREATED` 就断线：检查地址、会话重复或容量问题，稍后用新会话 ID 重试。

部署、服务端配置与排障见 [README](../README.md)。
