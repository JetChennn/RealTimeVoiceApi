# RealTimeVoiceAPI 内部调用时序图

本文档描述一次完整请求在 RealTimeVoiceAPI 内部的调用链路：从 WebSocket 握手、客户端音频输入，到 VAD → ASR → Thinker(LLM) → TTS 再回传音频给客户端的全过程，以及会话关闭流程。

## 总体架构

`SessionRuntime.run()` 在一个 `asyncio.TaskGroup` 中并行启动 5 个长任务，彼此通过队列通信；`SessionActor` 是纯同步状态机，把事件翻译为运行时 Effect；Thinker/TTS 是由 Effect 触发的后台任务。

| 长任务 | 入口 | 输入队列 | 输出 |
|--------|------|----------|------|
| receiver | `WebSocketReceiver.run` | websocket | `audio_queue` |
| vad | `VadWorker.run` | `audio_queue` | `events`（SpeechSegmentReady） |
| asr | `SessionRuntime._asr_loop` | `_asr_queue` | `events`（AsrSucceeded/Failed） |
| actor | `SessionRuntime._actor_loop` | `events` | 多个 Effect（驱动 thinker/tts/sender） |
| sender | `WebSocketSender.run` | `outbound` | websocket |

## 图一：握手 + 音频输入 + VAD + ASR

本图覆盖从建连到 ASR 出结果这一段的链路，涉及文件：[main.py](../src/realtime_voice/main.py)、[transport/websocket.py](../src/realtime_voice/transport/websocket.py)、[transport/workers.py](../src/realtime_voice/transport/workers.py)、[audio/vad.py](../src/realtime_voice/audio/vad.py)、[session/actor.py](../src/realtime_voice/session/actor.py)、[session/runtime.py](../src/realtime_voice/session/runtime.py)、[clients/asr.py](../src/realtime_voice/clients/asr.py)。

```mermaid
sequenceDiagram
    autonumber
    participant Client as 客户端
    participant WS as serve_realtime
    participant Reg as SessionRegistry
    participant RT as SessionRuntime
    participant Recv as Receiver
    participant VAD as VadWorker
    participant ActorLoop as actor_loop
    participant SActor as SessionActor
    participant ASRLoop as asr_loop
    participant Sender as Sender
    participant DASR as 下游ASR

    Note over Client,WS: 阶段1 WebSocket握手
    Client->>WS: 建立WS连接
    WS->>WS: websocket.accept
    WS->>WS: 等待首帧 handshake_timeout
    Client->>WS: CREATE_SESSION 首帧含采样率
    WS->>WS: decode_client_message 校验
    WS->>Reg: registry.create
    Reg->>RT: build_runtime 装配5个worker和4个队列
    Reg->>Reg: add 注册session
    RT->>RT: bind_registry
    WS->>RT: outbound.put SESSION_CREATED
    RT->>Sender: sender取出消息
    Sender->>Client: SESSION_CREATED
    WS->>RT: runtime.run 启动TaskGroup
    Note over RT: 并行启动5个长任务 receiver vad asr actor sender

    Note over Client,DASR: 阶段2 音频输入到VAD分段
    loop 持续推送音频
        Client->>Recv: AUDIO_CHUNK base64 PCM16
        Recv->>Recv: decode_pcm16 校验采样率对齐时长
        Recv->>Recv: 校验session_id和sequence递增
        Recv->>VAD: audio_queue.put_nowait
    end
    VAD->>VAD: StreamingResampler重采样到16k
    VAD->>VAD: 按512乘2字节切帧
    VAD->>VAD: 线程池跑SileroDetector
    VAD->>VAD: StreamingVadSegmenter状态机分段
    VAD->>ActorLoop: events.put SpeechSegmentReady

    Note over Client,DASR: 阶段3 VAD段到ASR队列
    ActorLoop->>ActorLoop: events.get取事件
    ActorLoop->>SActor: handle SpeechSegmentReady
    SActor-->>ActorLoop: QueueAsr segment
    ActorLoop->>ASRLoop: asr_queue.put segment

    Note over Client,DASR: 阶段4 ASR转写
    ASRLoop->>ASRLoop: asr_queue.get
    ASRLoop->>DASR: transcribe POST /v1/chat/completions
    DASR-->>ASRLoop: 转写文本
    ASRLoop->>ActorLoop: events.put AsrSucceeded
```

## 图二：开新轮 + Thinker + TTS + 轮次结束

本图侧重 ASR 成功之后、开新轮并完成一轮编排的链路。Thinker/TTS 是由 Actor 产出的 Effect 触发的后台任务。

```mermaid
sequenceDiagram
    autonumber
    participant Client as 客户端
    participant ActorLoop as actor_loop
    participant SActor as SessionActor
    participant Sender as Sender
    participant Thinker as run_thinker
    participant TTS as run_tts
    participant DThinker as 下游Thinker
    participant DTTS as 下游TTS

    Note over Client,DTTS: 阶段5 ASR成功开新轮次启动Thinker
    ActorLoop->>SActor: handle AsrSucceeded
    SActor->>SActor: 中断未完成turn置interrupted
    SActor->>SActor: 分配next_turn_id建TurnContext
    SActor-->>ActorLoop: SendOutbound AsrResult
    SActor-->>ActorLoop: SendOutbound TurnState INTERRUPTED
    SActor-->>ActorLoop: StartThinker
    ActorLoop->>Sender: outbound.put AsrResult
    Sender->>Client: ASR_RESULT
    ActorLoop->>Sender: outbound.put TurnState
    Sender->>Client: TURN_STATE中断上一轮
    Note over ActorLoop: TurnState触发_signal_tts_interruption
    ActorLoop->>Thinker: spawn _run_thinker

    Note over Client,DTTS: 阶段6 Thinker流式回复
    Thinker->>DThinker: stream_reply POST /api/v1/multimodal/reply
    loop 流式NDJSON
        DThinker-->>Thinker: ThinkerTextDelta
        Thinker->>ActorLoop: events.put ThinkerDeltaReceived
        ActorLoop->>SActor: handle ThinkerDeltaReceived
        SActor-->>ActorLoop: SendOutbound TextDelta
        ActorLoop->>Sender: outbound.put TextDelta
        Sender->>Client: TEXT_DELTA
    end
    DThinker-->>Thinker: ThinkerDone reply_text
    Thinker->>ActorLoop: events.put ThinkerCompleted

    Note over Client,DTTS: 阶段7 Thinker完成启动TTS
    ActorLoop->>SActor: handle ThinkerCompleted
    SActor->>SActor: 存reply_text释放活跃LLM槽
    SActor->>SActor: stage切到STREAMING_TTS
    SActor-->>ActorLoop: SendOutbound TextEnd
    SActor-->>ActorLoop: StartTts
    ActorLoop->>Sender: outbound.put TextEnd
    Sender->>Client: TEXT_END
    ActorLoop->>TTS: spawn _run_tts

    Note over Client,DTTS: 阶段8 TTS流式合成
    TTS->>DTTS: stream POST /v1/dialogue-tts/stream
    Note over TTS: 重采样24k到客户端采样率 收到中断信号给宽限期
    loop 流式NDJSON音频块
        DTTS-->>TTS: TtsChunk pcm16_24k
        TTS->>TTS: 重采样到客户端采样率
        TTS->>ActorLoop: events.put TtsChunkReceived
        ActorLoop->>SActor: handle TtsChunkReceived
        Note over SActor: 已interrupted则RecordDiscardedAudio 否则发AudioDelta
        SActor-->>ActorLoop: SendOutbound AudioDelta
        ActorLoop->>Sender: outbound.put AudioDelta
        Sender->>Client: AUDIO_DELTA
    end
    TTS->>ActorLoop: events.put TtsCompleted

    Note over Client,DTTS: 阶段9 TTS完成轮次结束
    ActorLoop->>SActor: handle TtsCompleted
    SActor->>SActor: stage切到COMPLETED或INTERRUPTED
    SActor-->>ActorLoop: SendOutbound ResponseEnd
    ActorLoop->>Sender: outbound.put ResponseEnd
    Sender->>Client: RESPONSE_END
```

## 关闭流程

关闭可由以下信号触发：
- 客户端发送 `CLOSE_SESSION`（[workers.py](../src/realtime_voice/transport/workers.py)）
- receiver 退出（连接断开）
- `SlowClient`：出站队列满（[runtime.py](../src/realtime_voice/session/runtime.py)）

```mermaid
sequenceDiagram
    autonumber
    participant Trig as 关闭触发源
    participant RT as SessionRuntime
    participant Reg as SessionRegistry
    participant Audio as receiver_vad_sender
    participant ASR as asr任务
    participant Thinker as thinker后台任务
    participant TTS as tts后台任务
    participant DThinker as 下游Thinker

    Trig->>RT: request_close 设置_close_requested
    RT->>RT: _close_requested.wait唤醒
    RT->>RT: _finish_cleanup到_cleanup_once
    Note over RT: 1 _stop_audio取消receiver_vad_sender
    Note over RT: 2 _cancel_asr取消asr并清空队列
    RT->>Audio: cancel并gather
    RT->>ASR: cancel并gather
    Note over RT: 3 _wait_thinker等待thinker任务 超时则cancel
    RT->>Thinker: 等待 thinker_cleanup_timeout
    Note over RT: 4 _drain_tts等待tts任务 超时则cancel
    RT->>TTS: 等待 tts_drain_timeout
    Note over RT: 5 若thinker_safe则_delete_thinker_session
    RT->>DThinker: delete_session DELETE /api/v1/sessions
    Note over RT: finally 无论成败registry.remove释放准入
    RT->>Reg: remove session_id
    Note over RT: run抛SessionStop哨兵 TaskGroup静默结束
```

## 中断传播机制

当新一轮 `AsrSucceeded` 到达时，对上一轮未结束的 turn 会触发中断链：

1. **Actor 标记中断**：[actor.py](../src/realtime_voice/session/actor.py) 的 `_interrupt_unfinished_turns` 对所有非终态 turn 置 `interrupted=True`，产出 `SendOutbound(TurnState INTERRUPTED)`。
2. **Runtime 通知 TTS**：[runtime.py](../src/realtime_voice/session/runtime.py) 的 `execute_effect` 收到 `TurnState` 时调用 `_signal_tts_interruption(turn_id)`，`set()` 对应的 `interrupt_signal`。
3. **TTS 排空宽限**：[runtime.py](../src/realtime_voice/session/runtime.py) 的 `_arm_tts_drain_timeout` 监听到信号后，把 `drain_timeout` 重设为 `_tts_drain_timeout` 秒宽限期，让 TTS 尽量消费并丢弃尾部音频后再结束。
4. **Thinker 中断**：[runtime.py](../src/realtime_voice/session/runtime.py) 中若 `StartNextThinker(interrupt_first=True)`，先调 `thinker_client.interrupt` 再发起新请求。

## 采样率转换

- **输入**：客户端采样率（16k/24k/48k）的 PCM16，在 [vad.py](../src/realtime_voice/audio/vad.py) 中由 `StreamingResampler(input_rate, 16000)` 转为 16k 供 VAD/ASR 使用。
- **输出**：TTS 固定输出 24k，在 [runtime.py](../src/realtime_voice/session/runtime.py) 中由 `StreamingResampler(24000, client_rate)` 转回客户端采样率后发给客户端。

## 背压机制

- **audio 队列**：item 数 + 字节数双重上限（[runtime.py](../src/realtime_voice/session/runtime.py) 中的 `BoundedByteQueue`），满则 `CLIENT_AUDIO_BACKPRESSURE`。
- **outbound 队列**：满则 `SlowClient` 触发会话关闭。
- **下游准入**：`BoundedAdmission`（[limits.py](../src/realtime_voice/clients/limits.py)）限制并发并设有等待队列上限，超限抛 `AdmissionOverloaded`，转为 `AsrFailed/ThinkerFailed/TtsFailed` 事件。
