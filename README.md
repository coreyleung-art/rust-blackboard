# rust-blackboard — 跨设备黑板（AI 消息总线存储层）

Rust 实现的本地优先黑板服务器：KV 存储 + 全局 seq 时间轴 + audit 审计 + SSE 事件桥。

## 功能

- **KV 存储**：`notes/`（消息）`data/`（状态）`tasks/`（任务卡）`nodes/`（设备）命名空间
- **全局时间轴**：HLC seq（`/clock` 对时），增量事件可 `since_seq` 拉取
- **audit 审计**：所有写操作落 `audit.jsonl`（5MB 轮转 + ARCHIVE_KEEP=10 保留）
- **SSE 事件桥**：`GET /events` 实时推送变更（v0.6.1 修复：写入即广播）
- **设备注册**：`POST /register`（v0.6.3）→ device-id + token，存 `nodes/<name>/identity`

## 启动

```bash
# launchd 托管（推荐）
launchctl kickstart -k gui/$(id -u)/com.dsh.hr.blackboard-server

# 或手动
rust-blackboard --port 8792 --sse-port 8803 --data-dir <data-dir>
# 可选认证（v0.6.3+）：BLACKBOARD_TOKEN=<token> 环境变量
```

## API

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | /clock | 全局 seq + 权威时间 |
| PUT | /<ns>/<key> | 写 KV（body=value 扁平，无 value 包装）|
| GET | /<ns>/ | 列命名空间 |
| GET | /<ns>/<key> | 读单个 |
| DELETE | /<ns>/<key> | 删（占全局 seq）|
| POST | /register | 设备注册（device-id + token）|
| POST | /subscribe | 订阅 topic→callback |
| GET | /events | SSE 事件流（实时推送）|

## 版本历史

见 CHANGELOG.md（v0.6.0 Rust 版 / v0.6.1 SSE 广播修复 / v0.6.2 audit .gz 保留 / v0.6.3 register API）

## 设计要点

- 手写 HTTP/1.1 零外部依赖（serde_json + chrono 仅依赖）
- 数据格式与 Python 版一字兼容（snapshot.json + audit.jsonl）
- SSE 广播：写入端必须 broadcast（v0.6.1 修复前只存不广播，订阅端静默失联）

## 关联

- node-bridge：跨设备桥（心跳/队列/worker）
- dsh-tools：bb-read/bb-sub 等黑板工具
- dsh-plugin-central-inbox：黑板→会话注入桥
