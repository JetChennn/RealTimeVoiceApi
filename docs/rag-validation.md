# RAG 接入验证记录

验证日期：2026-09-11。仅修改 RealTimeVoiceApi；未修改或重启 KBService、BerryThinker 或正在运行的网关进程。

## 自动化验证

- `ruff check .`、`git diff --check` 通过。
- `pytest -q --disable-warnings tests/unit`：257 通过。
- `pytest -q --disable-warnings tests/integration`：26 通过。
- 本次新增 32 个测试案例，覆盖配置校验、服务工厂传参、检索响应校验、降级、排队预算、取消释放、上下文隔离、打断接续、关闭与过期结果处理。
- 早期全量测试曾间歇出现 `tests/integration/test_full_turn.py` 在 WebSocket 退出阶段抛出 `concurrent.futures.CancelledError`；在独立导出的未修改 HEAD 上也复现（250 通过、1 失败）。最后分别运行单元与集成测试均通过，未修改现有 WebSocket 退出逻辑。

## 真实服务验证

通过网关现有 ASR、RAG、Thinker、TTS 客户端，以及新增的 SessionRuntime RAG 路径进行验证；未重启部署进程，未对在线 WebSocket 入口启用新代码。输入为 TTS 合成的测试问题，ASR 识别为“农业社会的生产方式是什么？”。测试使用独立 Thinker 会话，结束后删除该测试会话。

- 冷启动：检索在约 2.005 秒超时，普通 Thinker 回答和 TTS 正常完成，输出 518400 字节 PCM 音频。
- 后续单场景检索成功：5 条片段，约 0.021 秒。
- 双场景联合检索成功：8 条片段，约 0.481 秒。
- 有效场景加不存在场景：部分成功，5 条片段，约 0.100 秒。
- 完整带知识链路：SessionRuntime 检索成功，5 条片段，约 0.016 秒；Thinker 完成回答，TTS 输出 535680 字节 PCM 音频。

以上耗时为本次实测样本，不代表负载性能保证。首次加载后的成功结果不构成新增自动重试或预热功能。

本地详细结果：`reports/rag-smoke.json`（冷启动）、`reports/rag-retrieval-check.json`（联合检索）、`reports/rag-smoke-warm.json`（带知识链路）。报告目录沿用仓库忽略规则。
