# rust-blackboard · 黑板 v0.6 Rust 重写复盘（P0）

> 目的：记录黑板 Rust 化的语义对齐、发现的问题与解决——协议/API/存储一字兼容，不砸对讲机
> 出品：mac-mini 中枢 · 2026-08-25 · 配套 `rust-bridge-problem-map-v1.0.md`（节点桥复盘）

---

## 一、语义对齐清单（Python v0.6 → Rust）

| 语义 | Python | Rust | 验证 |
|---|---|---|---|
| KV state | `{key: {version, value, ts}}` | `HashMap<String, Entry>` | ✅ |
| HLC 时钟 | 物理秒*10⁶+同秒计数，防呆基准 | `next_seq()` 逐行对齐 | ✅ seq 一致 |
| audit 持久化 | audit.jsonl append，5MB 轮转归档保 10 | 同格式 | ✅ |
| 快照恢复 | snapshot.json（state+last_seq+timeline） | 同格式读取 | ✅ |
| 订阅持久化 | subs.json | 同格式 | ✅ |
| result 镜像 | tasks/<node>/result → results/<seq后10位> | mirror_result | ✅ |
| notify | topic 前缀匹配，8 线程池异步 POST | channel + 8 worker | ✅ |
| tasks 定向 | recipient 匹配 or 节点前缀 | list_ns 过滤 | ✅ |
| X-Writer | PUT/DELETE 签名入 audit | 同 | ✅ |
| X-Blackboard-Token | 认证（空 token 跳过） | authorized() | ✅ |
| ROLE_NS | 12 角色命名空间 | 常量 | ✅ |

## 二、发现的问题与解决

### Q1. body 读取丢字节（curl 场景无响应）
- **问题**：curl PUT 无响应，nc 正常
- **根因**：一次 `read()` 可能同时收到请求头+body，原实现只提取头、body 重新从 stream 读 → 死等/丢 body
- **解决**：从已读缓冲提取 `\r\n\r\n` 之后的 body 剩余，再补读
- **验证**：✅ curl PUT 返回完整 JSON

### Q2. ts 时区错位（离线误判隐患！）
- **问题**：初版用 UTC 时间，`node-onboard-gui` 存活判定 `fromisoformat(hb_ts)` 解析后 age≈8h → 节点全误判 stale
- **根因**：Python `datetime.now()` 本地时区，Rust 初版用 UTC epoch 换算
- **解决**：chrono `Local::now()`（自动本地时区），与 Python 完全一致
- **验证**：✅ ts `22:55:40` = 系统时间，存活判定 age 0.5s 在线

### Q3. 存活判定对 ts 格式的脆弱依赖（发现，非黑板缺陷）
- **发现**：onboard-gui 读 `value.ts`（客户端写的），node-bridge 写 epoch 秒数字符串 → fromisoformat 抛错 → 走 status 兜底
- **判断**：Python 版同样如此，非回归；但这是历史 P1（离线误判）的深层根源
- **建议**：后续改进 onboard-gui 判定——优先用黑板服务端 entry ts 或黑板权威时钟（/clock seq）

## 三、内存对比（104MB → ~10MB 预期）

| 指标 | Python | Rust | 降幅 |
|---|---|---|---|
| 常驻 RSS | 104.2MB | ~10MB（待实测） | ~90% |
| 二进制 | 454 行解释执行 | 477K 单文件 | — |
| 启动 | 秒级（重放） | 秒级（快照） | — |

## 四、验证记录

| # | 验证项 | 结果 |
|---|---|---|
| 1 | 全端点（PUT/GET/列/DELETE/clock/timeline/subs/stores/ns-registry/help）| ✅ |
| 2 | 真实数据迁移（8832 键快照 + audit 重放）| ✅ seq 与 Python 一致 |
| 3 | node-bridge 端到端（注册/心跳/任务/回报/清卡）| ✅ E2E_THROUGH_RUST_BB |
| 4 | 三平台交叉编译（Win 537K / Linux 629K / macOS 477K）| ✅ |
| 5 | 存活判定 ts 本地时区对齐 | ✅ age 0.5s 在线 |

## 五、遗留
- 切换生产：停 Python 8792 → 起 Rust（同一 data-dir，数据无缝）→ 验证 → 关事件桥旧进程（P2 合并）
- onboard-gui 存活判定增强（用服务端 ts）列为后续
- audit 归档压缩（Python gzip，Rust 保留 .jsonl）——读取端兼容，归档体积略大

## 六、变更记录
- v0.6.0（2026-08-25）：首版 Rust 黑板。协议/API/存储一字兼容；修复 body 读取与 ts 时区两处；三平台产物 + 端到端验证。

## 七、智能体级测试补强（2026-08-25 晚间）

### Q4. SSE 事件桥 HTTP/0.9 裸流（智能体测试发现）
- **问题**：GET /events 响应缺 HTTP 状态行与 Content-Type 头 → 标准客户端报「Received HTTP/0.9」
- **发现方式**：真实 DSH 子代理（agent-test-node）订阅时暴露——单测/curl 裸流测试均未覆盖（nc 能读裸流，标准客户端不行）
- **修复**：补 `HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\n\r\n`
- **验证**：标准 curl 无 --http0.9 直接订阅成功，实时收到 event: change ✅

### 智能体协作测试结论（agent-test-node 全流程）
注册 → 心跳 → 取卡（首轮即中）→ 执行 → 回报 → 清卡 → notes 双向对话 → SSE 实时推送 全部通过。
timeline 完整轨迹：8 个操作 seq 单调、无异常。
