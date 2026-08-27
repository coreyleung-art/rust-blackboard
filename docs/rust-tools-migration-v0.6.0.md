# 常驻工具全量 Rust 化复盘（P0 黑板 + P1 基因库 + P2 事件桥）

> 出品：mac-mini 中枢 · 2026-08-25 · 用户指令「全部推进」
> 原则：协议/API/存储格式一字兼容（不砸对讲机）· 数据无缝迁移 · 三平台产物

---

## 一、交付总览

| 服务 | Python 原版 | Rust 版 | 端口 | 产物 | 验证 |
|---|---|---|---|---|---|
| 黑板（通讯协议核心）| blackboard-server-v0.6.py（454 行）| rust-blackboard（含事件桥）| 8792 + 8803 | Win 548K / Linux 642K / macOS 493K | ✅ 全端点+真实数据+端到端 |
| 基因库（AI 网盘）| genebank-server.py（262 行）| rust-genebank | 8801 + 静态共享 | Win 468K / Linux 556K / macOS 427K | ✅ 全端点+真实数据 |
| 黑板事件桥 | blackboard-events.py（103 行）| 并入 rust-blackboard | 8803（同进程）| — | ✅ SSE 实测 |
| 8793 文件共享 | python http.server | 并入 rust-genebank /shared | — | — | ✅ 静态文件 |

**合计**：Python 4 个进程（454+262+103+http.server）→ **2 个 Rust 二进制**（黑板 493K + 基因库 427K）

## 二、内存收益（预期）

| 服务 | Python RSS | Rust 预期 | 降幅 |
|---|---|---|---|
| 黑板 8792 | 104.2MB | ~10MB | ~90% |
| 事件桥 8803 | 8.6MB | 并入（0 增量）| 100% |
| 基因库 8801 | 3.9MB | ~5MB | ~-20%（文件服务 1.5G 磁盘主导）|
| 8793 共享 | 3.9MB | 并入 | 100% |

## 三、历史问题复盘映射

| Python 问题 | Rust 解决 |
|---|---|
| PUT body 读取丢字节（curl 无响应）| 从已读缓冲提取 header 后 body 剩余（Q1）|
| ts 时区错位 → 存活误判 | chrono Local 对齐（Q2）|
| 存活判定依赖客户端 ts 格式 | 发现记录，建议后续 onboard-gui 改进（Q3）|
| 多进程（黑板+事件桥+共享）分散 | 2 个二进制合并进程 |
| 解释器依赖/GBK | 单二进制 + encoding_rs |
| 104MB 常驻 | 状态 HashMap 原生内存 |

## 四、数据兼容验证

| 数据 | Python | Rust 读取 | 结果 |
|---|---|---|---|
| 黑板 snapshot（8832 键）+ audit | 9171 键 | 同格式 | ✅ seq 与 Python 完全一致 |
| 黑板 timeline | 20000 条 | 同 | ✅ |
| 黑板 subs | 4 个 | 同 | ✅ |
| 基因库 registry.jsonl | 22383 条 | 同 | ✅ count 22383 |
| 基因库 genes/ | 16798 文件 | 同 | ✅ |

## 五、端到端验证

- node-bridge → rust-blackboard：注册/心跳/任务/回报/清卡 ✅（E2E_THROUGH_RUST_BB）
- SSE：订阅 → PUT 触发 → event: change 广播 ✅
- genebank：注册/校验/上传/X-Offset/Range 下载/静态共享 ✅

## 六、切换方案（生产替换）

```bash
# 1. 停 Python 三件套
pkill -f blackboard-server-v0.6.py
pkill -f blackboard-events.py
pkill -f "http.server 8793"
pkill -f genebank-server.py

# 2. 起 Rust 版（同一 data-dir，数据无缝）
~/dsh-collab/rust-blackboard/dist/rust-blackboard-macos-arm64-v0.6.0 \
  --port 8792 --sse-port 8803 --data-dir ~/dsh-collab/token-monitor/blackboard
~/dsh-collab/rust-genebank/dist/rust-genebank-macos-arm64-v1.0.0 --port 8801

# 3. 验证
curl http://127.0.0.1:8792/clock          # seq 应大于旧值
curl http://127.0.0.1:8801/api/v1/registry # count 22383
curl -N http://127.0.0.1:8803/events       # SSE hello

# 4. 回滚：随时切回 Python 版（数据格式不变）
```

## 七、遗留
- 审计归档 gzip：Rust 保留 .jsonl 未压缩（读取兼容，归档略大）
- 存活判定增强（用服务端 ts）列后续
- 切换时机：用户确认后执行（当前 Python 版仍在跑，双系统并存验证中）

## 八、变更记录
- 2026-08-25：P0 黑板（含事件桥）+ P1 基因库（含共享）Rust 化完成，三平台产物 + 全量验证。
