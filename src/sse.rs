// sse.rs — 黑板事件桥（原 blackboard-events.py 并入）：变更 → SSE /events 广播
// 兼容：POST /cb（黑板回调入口）→ 广播；GET /events（SSE 流）→ 客户端订阅
// v0.6.9（2026-10-03 变更3）：事件带全局递增 id + 有界重放缓冲，支持 Last-Event-ID catch-up。
use serde_json::{json, Value};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

/// 全局 SSE hub：保存所有客户端 sender，广播时逐个发
static CLIENTS: OnceLock<Mutex<Vec<std::sync::mpsc::Sender<(u64, String)>>>> = OnceLock::new();
/// 事件序号（全局递增，1 起）
static SEQ: AtomicU64 = AtomicU64::new(0);
/// 重放缓冲：(seq, evt)，有界 FIFO
static REPLAY: OnceLock<Mutex<VecDeque<(u64, String)>>> = OnceLock::new();
const REPLAY_CAP: usize = 500;

fn clients() -> &'static Mutex<Vec<std::sync::mpsc::Sender<(u64, String)>>> {
    CLIENTS.get_or_init(|| Mutex::new(Vec::new()))
}

fn replay_buf() -> &'static Mutex<VecDeque<(u64, String)>> {
    REPLAY.get_or_init(|| Mutex::new(VecDeque::new()))
}

/// 黑板变更 → 广播给所有 SSE 客户端（原 blackboard-events.broadcast）
pub fn broadcast(key: &str, value: Option<&Value>, version: u64) {
    let seq = SEQ.fetch_add(1, Ordering::SeqCst) + 1;
    let evt = json!({
        "key": key,
        "value": value.cloned().unwrap_or(Value::Null),
        "version": version,
        "ts": crate::store::now_ts_public(),
    })
    .to_string();
    {
        let mut buf = replay_buf().lock().unwrap();
        buf.push_back((seq, evt.clone()));
        while buf.len() > REPLAY_CAP {
            buf.pop_front();
        }
    }
    let mut list = clients().lock().unwrap();
    let mut dead = Vec::new();
    for (i, tx) in list.iter().enumerate() {
        match tx.send((seq, evt.clone())) {
            Ok(_) => {}
            Err(_) => dead.push(i), // 客户端断开，标记清理
        }
    }
    for i in dead.into_iter().rev() {
        list.remove(i);
    }
}

/// SSE 客户端注册：返回 receiver（调用方持有轮询）；负载为 (seq, evt)
pub fn register_client() -> std::sync::mpsc::Receiver<(u64, String)> {
    let (tx, rx) = std::sync::mpsc::channel::<(u64, String)>();
    clients().lock().unwrap().push(tx);
    rx
}

/// Last-Event-ID 重放：返回 last_id 之后的所有缓冲事件（按序）
pub fn replay_after(last_id: u64) -> Vec<(u64, String)> {
    replay_buf()
        .lock()
        .unwrap()
        .iter()
        .filter(|(s, _)| *s > last_id)
        .cloned()
        .collect()
}

/// 黑板回调入口（原 /cb）：外部回调 → 广播
pub fn handle_cb(body: &Value) -> Value {
    let key = body.get("key").and_then(|k| k.as_str()).unwrap_or("?").to_string();
    let value = body.get("value").cloned();
    let version = body.get("version").and_then(|v| v.as_u64()).unwrap_or(0);
    broadcast(&key, value.as_ref(), version);
    json!({"ok": true})
}

pub const PING_SECS: u64 = 15;
