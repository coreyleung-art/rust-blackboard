// sse.rs — 黑板事件桥（原 blackboard-events.py 并入）：变更 → SSE /events 广播
// 兼容：POST /cb（黑板回调入口）→ 广播；GET /events（SSE 流）→ 客户端订阅
use serde_json::{json, Value};
use std::sync::{Mutex, OnceLock};

/// 全局 SSE hub：保存所有客户端 sender，广播时逐个发
static CLIENTS: OnceLock<Mutex<Vec<std::sync::mpsc::Sender<String>>>> = OnceLock::new();

fn clients() -> &'static Mutex<Vec<std::sync::mpsc::Sender<String>>> {
    CLIENTS.get_or_init(|| Mutex::new(Vec::new()))
}

/// 黑板变更 → 广播给所有 SSE 客户端（原 blackboard-events.broadcast）
pub fn broadcast(key: &str, value: Option<&Value>, version: u64) {
    let evt = json!({
        "key": key,
        "value": value.cloned().unwrap_or(Value::Null),
        "version": version,
        "ts": crate::store::now_ts_public(),
    })
    .to_string();
    let mut list = clients().lock().unwrap();
    let mut dead = Vec::new();
    for (i, tx) in list.iter().enumerate() {
        match tx.send(evt.clone()) {
            Ok(_) => {}
            Err(_) => dead.push(i), // 客户端断开，标记清理
        }
    }
    for i in dead.into_iter().rev() {
        list.remove(i);
    }
}

/// SSE 客户端注册：返回 receiver（调用方持有轮询）
pub fn register_client() -> std::sync::mpsc::Receiver<String> {
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    clients().lock().unwrap().push(tx);
    rx
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
