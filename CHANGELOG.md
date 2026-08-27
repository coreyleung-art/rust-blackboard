# rust-blackboard CHANGELOG

## v0.6.1 (2026-08-28)
- **修复**: SSE 广播断链——`do_put` 写入后补 `sse::broadcast`（此前只 `store.put` 不广播，SSE 客户端只收 hello/ping 收不到 change 事件，导致 central-inbox 跨设备注入全断）
- **影响**: 修复 mac↔MBP/i9 黑板消息注入链路（双向注入验证通过）

## v0.6.0 (2026-08-25)
- Rust 版黑板（Python 版无缝替换）：KV + 订阅 + 全局 seq HLC 时间轴 + audit 持久化 + SSE 事件桥（原 8803 并入）
