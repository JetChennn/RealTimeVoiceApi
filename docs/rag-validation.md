# RAG 透传链路验证说明

当前架构中，RealTimeVoiceApi 不直接访问 KBService。客户端在 `CREATE_SESSION` 中提供 `rag_enabled` 和 `scenes`，网关在每轮调用 BerryThinker `POST /api/v1/reply` 时将其转换为：

```json
{
  "rag": {
    "enabled": true,
    "scenes": ["农业社会"]
  }
}
```

## 行为约定

- `rag_enabled=false`：Thinker 请求不包含 `rag` 字段。
- `rag_enabled=true, scenes=[]`：传递空场景数组，由 Thinker 使用通用知识库。
- `rag_enabled=true` 且包含 1～3 个场景：按去空白、去重后的顺序逐轮传递。
- 不再传递由网关拼装的 `messages` 知识上下文。
- 网关不再创建 KBService 客户端，不再维护 RAG 限流器、超时、片段或结果指标。

## 自动化覆盖

- 协议层校验默认值、布尔类型、空场景、去空白、去重和最多三个场景。
- Thinker HTTP 客户端校验关闭、指定场景以及空场景三类请求体。
- SessionRuntime 校验每轮透传、通用知识模式和会话间配置隔离。
- WebSocket 集成测试校验 `CREATE_SESSION` 参数能够经过完整网关链路到达 Thinker 请求。
- 服务工厂测试校验网关不再构造或注入 KBService/RAG 客户端。

真实知识召回、命中、降级及耗时由 BerryThinker 和 KBService 的配置、日志与指标验证。网关侧的 Thinker 首事件和总时长预算包含 Thinker 内部 RAG 耗时。
