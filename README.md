# RealTimeVoiceAPI

RealTimeVoiceAPI 是一个异步 WebSocket 语音网关，在单个会话中编排本地 VAD、ASR、BerryThinker 和 TTS，并向客户端流式返回识别文本、回复文本、PCM16 音频和打断状态。

## 安装与启动

需要 Python 3.11 或更高版本。默认下游地址为 ASR `http://127.0.0.1:8000`、BerryThinker `http://127.0.0.1:8082`、TTS `http://127.0.0.1:8001`。

```bash
python3.11 -m venv .venv
. .venv/bin/activate
pip install -e '.[dev]'
cp .env.example .env
uvicorn realtime_voice.main:app --host 0.0.0.0 --port 8003
```

检查服务状态和指标：

```bash
curl http://127.0.0.1:8003/health
curl http://127.0.0.1:8003/metrics
```

所有配置使用 `RTVA_` 前缀；完整默认值见 `.env.example`。生产环境模板见 `deploy/realtime-voice-api.service` 和 `deploy/supervisord.conf`，使用前替换其中的用户、目录和虚拟环境路径。

## WebSocket 协议 V1

连接地址为 `ws://HOST:8003/v1/realtime`。客户端只发送 JSON 文本帧；音频必须是单声道 PCM16，使用 Base64 放入 JSON。允许采样率为 16000、24000 或 48000 Hz，建议每个 `AUDIO_CHUNK` 为 40 ms。

客户端消息示例：

```json
{"type":"CREATE_SESSION","protocol_version":1,"device_id":"device-01","session_id":"session-100","audio_format":"PCM16","audio_transport":"BASE64_JSON","sample_rate":16000,"channels":1}
{"type":"AUDIO_CHUNK","session_id":"session-100","sequence":0,"timestamp_ms":0,"audio_b64":"AAAAAA=="}
{"type":"CLOSE_SESSION","session_id":"session-100"}
```

服务端消息示例（`user_id`、`session_id`、`turn_id` 和 `interrupt` 在所有消息中均存在）：

```json
{"type":"SESSION_CREATED","user_id":"device-01","session_id":"session-100","turn_id":0,"interrupt":false,"protocol_version":1,"audio_format":"PCM16","audio_transport":"BASE64_JSON","sample_rate":16000,"channels":1}
{"type":"ASR_RESULT","user_id":"device-01","session_id":"session-100","turn_id":1,"interrupt":false,"text":"你好"}
{"type":"TEXT_DELTA","user_id":"device-01","session_id":"session-100","turn_id":1,"interrupt":false,"delta":"你"}
{"type":"TEXT_END","user_id":"device-01","session_id":"session-100","turn_id":1,"interrupt":false,"text":"你好！"}
{"type":"AUDIO_DELTA","user_id":"device-01","session_id":"session-100","turn_id":1,"interrupt":false,"sequence":0,"audio_format":"PCM16","sample_rate":16000,"channels":1,"audio_b64":"AAAAAA=="}
{"type":"TURN_STATE","user_id":"device-01","session_id":"session-100","turn_id":1,"interrupt":true,"state":"INTERRUPTED"}
{"type":"RESPONSE_END","user_id":"device-01","session_id":"session-100","turn_id":1,"interrupt":false,"status":"COMPLETED"}
{"type":"ERROR","user_id":"device-01","session_id":"session-100","turn_id":1,"interrupt":false,"stage":"ASR","code":"ASR_TIMEOUT","message":"ASR timed out","recoverable":true}
```

新语音段可以打断正在生成的旧 turn。客户端收到旧 turn 的 `TURN_STATE/INTERRUPTED` 后应停止播放并丢弃该 turn 后续数据；消息按每个 turn 的 `sequence` 排序。

## 客户端与压测

发送一段单声道 PCM16 WAV，并将最新未被打断 turn 的回复保存为可播放 WAV：

```bash
python scripts/realtime_client.py \
  --url ws://127.0.0.1:8003/v1/realtime \
  --wav tests/fixtures/audio/speech_16k.wav \
  --sample-rate 16000 \
  --output reply.wav
```

分别执行 30、40、60 并发压测：

```bash
python scripts/load_test.py --url ws://127.0.0.1:8003/v1/realtime --clients 30 --wav tests/fixtures/audio/speech_16k.wav --report report-30.json
python scripts/load_test.py --url ws://127.0.0.1:8003/v1/realtime --clients 40 --wav tests/fixtures/audio/speech_16k.wav --report report-40.json
python scripts/load_test.py --url ws://127.0.0.1:8003/v1/realtime --clients 60 --wav tests/fixtures/audio/speech_16k.wav --report report-60.json
```

报告包含连接/失败数、错误码统计，以及从语音结束到 ASR、首段文本、首段音频的 p50/p95/p99 延迟。

## 验证

```bash
pytest -q tests/unit
pytest -q tests/integration
ruff check .
```

V1 明确不支持 Opus，也不提供跨进程或服务重启后的 Session 恢复/重连。需要这些能力时应在后续协议版本中增加能力协商和外部状态存储。
