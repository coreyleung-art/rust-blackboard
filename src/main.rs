// main.rs — 黑板 blackboard-server v0.6 Rust 版（含事件桥，原 8803 并入）
// 协议/API/存储格式与 Python 版一字兼容（不砸对讲机）：KV + 订阅 + 全局 seq HLC 时间轴 + audit 持久化
// 设计：手写 HTTP 服务（零外部依赖），数据直接读原 Python 的 snapshot/audit/subs → 无缝替换
// 事件桥：变更 → SSE 广播（原 blackboard-events.py :8803 职责并入，单进程）
mod store;
mod logger;
mod http;
mod sse;

use store::{Store, Config};
use std::sync::Arc;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut port = 8792u16;
    let mut sse_port = 8803u16;
    let mut data_dir = store::default_data_dir();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--port" => { i += 1; if i < args.len() { port = args[i].parse().unwrap_or(8792); } }
            "--sse-port" => { i += 1; if i < args.len() { sse_port = args[i].parse().unwrap_or(8803); } }
            "--data-dir" => { i += 1; if i < args.len() { data_dir = args[i].clone(); } }
            _ => {}
        }
        i += 1;
    }

    logger::init(logger::LEVEL_INFO);
    let token = std::env::var("BLACKBOARD_TOKEN").unwrap_or_default();
    let cfg = Config { port, data_dir: data_dir.clone(), token: token.clone() };
    let store = Arc::new(Store::new(cfg));

    logger::log(logger::LEVEL_INFO, "INFO", "main", &format!(
        "rust-blackboard v{} on :{} (token={}) data={} keys={} seq={} timeline={} subs={} | SSE :{}",
        env!("CARGO_PKG_VERSION"), port,
        if token.is_empty() { "off" } else { "on" },
        data_dir, store.state_len(), store.clock_seq(), store.timeline_len(), store.subs_len(),
        sse_port
    ));

    // SSE 事件桥端口（原 8803 语义）
    if sse_port > 0 && sse_port != port {
        let sse_store = Arc::clone(&store);
        std::thread::spawn(move || {
            http::serve_sse(sse_store, sse_port);
        });
    }

    http::serve(store);
}
