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

fn usage() -> String {
    format!(
"rust-blackboard v{} — 黑板 KV + SSE 事件桥（协议与 Python v0.6 一字兼容）

用法:
  rust-blackboard [--port <N>] [--sse-port <N>] [--data-dir <PATH>]
  rust-blackboard --help | -h          显示本帮助（exit 0）
  rust-blackboard --tool-version       机器可读版本（exit 0）

选项:
  --port <N>         HTTP 端口（默认 8792）
  --sse-port <N>     SSE 事件桥端口（默认 8803；设为 0 或与 --port 相同则不开）
  --data-dir <PATH>  数据目录（snapshot.json / audit.jsonl / subs.json）

退出码（R006 ⑨② 语义固定）: 0 成功 · 1 运行失败 · 2 用法错误
未知旗标 → 打印用法并以 exit 2 退出（**不会启动服务**）。
", env!("CARGO_PKG_VERSION"))
}

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
            // v0.6.8：补 R006 ⑨⑤ `--help` 自解释 与 ⑥ `--tool-version` 机器可读；
            // 并修正 ⑨② —— 旧实现对未知旗标 `_ => {}` **静默忽略并继续启动服务**：
            // 一个拼错的旗标不会报错，而会去抢端口/碰数据目录（实测 `--help` 会尝试启动）。
            "--help" | "-h" => { print!("{}", usage()); std::process::exit(0); }
            "--tool-version" => {
                println!("{{\"tool\":\"rust-blackboard\",\"version\":\"{}\",\"source\":\"Cargo.toml (env! CARGO_PKG_VERSION)\"}}",
                         env!("CARGO_PKG_VERSION"));
                std::process::exit(0);
            }
            other if other.starts_with('-') => {
                eprintln!("未知旗标: {}\n", other);
                eprint!("{}", usage());
                std::process::exit(2);
            }
            _ => {}
        }
        i += 1;
    }

    logger::init(logger::LEVEL_INFO);
    let token = std::env::var("BLACKBOARD_TOKEN").unwrap_or_default();
    // ★ v0.6.12 多端白名单：BLACKBOARD_TOKENS 逗号分隔优先；否则单 token；两者皆空=鉴权关
    let tokens: Vec<String> = std::env::var("BLACKBOARD_TOKENS")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(|s| s.split(',').map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect())
        .unwrap_or_else(|| if token.is_empty() { vec![] } else { vec![token.clone()] });
    let auth_on = !tokens.is_empty(); // v0.6.15: 显示判定先算（Config 构造会移动 tokens）
    let cfg = Config { port, data_dir: data_dir.clone(), token: token.clone(), tokens };
    let store = Arc::new(Store::new(cfg));

    logger::log(logger::LEVEL_INFO, "INFO", "main", &format!(
        "rust-blackboard v{} on :{} (token={}) data={} keys={} seq={} timeline={} subs={} | SSE :{}",
        env!("CARGO_PKG_VERSION"), port,
        if auth_on { "on" } else { "off" },
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
