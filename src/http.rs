// http.rs — 黑板 HTTP 服务层（手写 HTTP/1.1，零外部依赖）
// 端点与 Python blackboard-server-v0.6 一字对齐：PUT/GET/DELETE/POST + /clock /timeline /subs /stores /ns-registry /help /tasks
// + 事件桥（原 8803）：GET /events（SSE）+ POST /cb（黑板回调入口）
use crate::store::Store;
use serde_json::{json, Value};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::time::Duration;

pub fn serve(store: Arc<Store>) {
    let port = store.port;
    let addr = format!("0.0.0.0:{}", port);
    let listener = match TcpListener::bind(&addr) {
        Ok(l) => l,
        Err(e) => { eprintln!("bind {} failed: {}", addr, e); std::process::exit(1); }
    };
    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                let st = Arc::clone(&store);
                std::thread::spawn(move || {
                    let _ = handle_conn(&st, s);
                });
            }
            Err(_) => continue,
        }
    }
}

// ── SSE 事件桥（原 8803）：GET /events 长连接 + POST /cb 回调入口 ──
pub fn serve_sse(_store: Arc<Store>, port: u16) {
    let listener = match TcpListener::bind(format!("0.0.0.0:{}", port)) {
        Ok(l) => l,
        Err(e) => { eprintln!("sse bind :{} failed: {}", port, e); return; }
    };
    for stream in listener.incoming() {
        if let Ok(s) = stream {
            std::thread::spawn(move || {
                let _ = handle_sse_conn(s);
            });
        }
    }
}

fn handle_sse_conn(mut stream: TcpStream) -> Result<(), String> {
    let req = match parse_request(&mut stream) { Some(r) => r, None => return Ok(()) };
    let path = req.path.trim_end_matches('/').to_string();
    match req.method.as_str() {
        "POST" => {
            // 黑板回调入口（原 /cb）：body {key, value, version} → SSE 广播
            let body: Value = serde_json::from_slice(&req.body).unwrap_or(json!({}));
            let resp = crate::sse::handle_cb(&body);
            respond(&mut stream, 200, &resp)
        }
        "GET" if path.starts_with("/events") => {
            // SSE 长连接（标准 HTTP/1.1 响应头——原实现缺头导致客户端报 HTTP/0.9）
            let rx = crate::sse::register_client();
            let header = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\n\r\n";
            stream.write_all(header.as_bytes()).map_err(|e| e.to_string())?;
            stream.flush().ok();
            let hello = format!(
                "event: hello\ndata: {{\"bridge\":\"rust-blackboard-events\",\"ts\":\"{}\"}}\n\n",
                crate::store::now_ts_public()
            );
            stream.write_all(hello.as_bytes()).map_err(|e| e.to_string())?;
            stream.flush().ok();
            loop {
                match rx.recv_timeout(Duration::from_secs(crate::sse::PING_SECS)) {
                    Ok(evt) => {
                        let frame = format!("event: change\ndata: {}\n\n", evt);
                        if stream.write_all(frame.as_bytes()).is_err() { break; }
                        stream.flush().ok();
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        if stream.write_all(b": ping\n\n").is_err() { break; }
                        stream.flush().ok();
                    }
                    Err(_) => break,
                }
            }
            Ok(())
        }
        _ => respond(&mut stream, 404, &json!({"error": "not found"})),
    }
}

// ── 请求解析 ──
struct Req {
    method: String,
    path: String,   // 不含 query
    query: Vec<(String, String)>,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

fn parse_request(stream: &mut TcpStream) -> Option<Req> {
    stream.set_read_timeout(Some(Duration::from_secs(30))).ok();
    // 读请求头（直到 \r\n\r\n）
    let mut buf: Vec<u8> = Vec::new();
    let mut tmp = [0u8; 4096];
    loop {
        match stream.read(&mut tmp) {
            Ok(0) => return None,
            Ok(n) => {
                buf.extend_from_slice(&tmp[..n]);
                if buf.windows(4).any(|w| w == b"\r\n\r\n") { break; }
                if buf.len() > 1_000_000 { return None; }
            }
            Err(_) => return None,
        }
    }
    let head = String::from_utf8_lossy(&buf).to_string();
    let mut lines = head.split("\r\n");
    let req_line = lines.next()?;
    let mut parts = req_line.split_whitespace();
    let method = parts.next()?.to_string();
    let target = parts.next()?.to_string();
    let (path, query) = match target.find('?') {
        Some(i) => (target[..i].to_string(), target[i + 1..].to_string()),
        None => (target.clone(), String::new()),
    };
    let mut headers = Vec::new();
    let mut content_len = 0usize;
    for l in lines {
        if l.is_empty() { break; }
        if let Some(i) = l.find(':') {
            let k = l[..i].trim().to_lowercase();
            let v = l[i + 1..].trim().to_string();
            if k == "content-length" {
                content_len = v.parse().unwrap_or(0);
            }
            headers.push((k, v));
        }
    }
    // body 优先从已读缓冲提取（一次 read 可能头+body 同来）
    let header_end = buf.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4).unwrap_or(buf.len());
    let mut body: Vec<u8> = buf[header_end..].to_vec();
    while body.len() < content_len && content_len < 10_000_000 {
        match stream.read(&mut tmp) {
            Ok(0) => break,
            Ok(n) => body.extend_from_slice(&tmp[..n]),
            Err(_) => break,
        }
    }
    body.truncate(content_len);
    let query: Vec<(String, String)> = if query.is_empty() {
        Vec::new()
    } else {
        query.split('&').filter_map(|kv| {
            let mut it = kv.splitn(2, '=');
            Some((it.next()?.to_string(), it.next().unwrap_or("").to_string()))
        }).collect()
    };
    Some(Req { method, path, query, headers, body })
}

fn qp(query: &[(String, String)], name: &str) -> Option<String> {
    query.iter().find(|(k, _)| k == name).map(|(_, v)| v.clone())
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str())
}

fn handle_conn(store: &Arc<Store>, mut stream: TcpStream) -> Result<(), String> {
    let req = match parse_request(&mut stream) {
        Some(r) => r,
        None => return Ok(()),
    };
    // 认证
    let token_hdr = header(&req.headers, "x-blackboard-token");
    if !store.authorized(token_hdr) {
        return respond(&mut stream, 401, &json!({"error": "unauthorized"}));
    }
    let writer = header(&req.headers, "x-writer").map(|s| s.to_string());
    let path = req.path.trim_start_matches('/').to_string();

    let resp: (u16, Value) = match req.method.as_str() {
        "GET" => handle_get(store, &path, &req.query),
        "PUT" => {
            let value: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
            if value.is_null() && !req.body.is_empty() {
                // body 解析失败 → 空对象（Python 行为）
                let v = Value::Object(serde_json::Map::new());
                do_put(store, &path, v, writer.as_deref())
            } else if value.is_null() {
                do_put(store, &path, Value::Object(serde_json::Map::new()), writer.as_deref())
            } else {
                do_put(store, &path, value, writer.as_deref())
            }
        }
        "DELETE" => {
            if let Some(m) = ns_match(&path) {
                store.delete(&m, writer.as_deref());
                (200, json!({"deleted": true}))
            } else {
                (200, json!({"deleted": true}))
            }
        }
        "POST" => {
            let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
            if path.trim_end_matches('/').ends_with("subscribe") {
                let topic = body.get("topic").and_then(|t| t.as_str()).unwrap_or("");
                let cb = body.get("callback").and_then(|c| c.as_str()).unwrap_or("");
                let unsub = body.get("unsub").and_then(|u| u.as_bool()).unwrap_or(false);
                let n = store.subscribe(topic, cb, unsub);
                if unsub {
                    let removed = 1; // Python 版返回 removed 计数（简化：按匹配数）
                    (200, json!({"unsubscribed": removed, "subscribed": n}))
                } else {
                    (200, json!({"subscribed": n}))
                }
            } else {
                (400, json!({"error": "use /subscribe"}))
            }
        }
        _ => (405, json!({"error": "method not allowed"})),
    };
    respond(&mut stream, resp.0, &resp.1)
}

fn do_put(store: &Arc<Store>, path: &str, value: Value, writer: Option<&str>) -> (u16, Value) {
    match ns_match(path) {
        Some(full) => {
            let (ver, seq) = store.put(&full, value.clone(), writer);
            // 2026-08-28 修复：写入后必须广播 SSE（此前只 store.put 未广播，
            // 导致 SSE 事件桥收不到任何 change 事件 → central-inbox 跨设备注入全断）
            crate::sse::broadcast(&full, Some(&value), ver);
            let mut r = json!({"key": full, "version": ver, "seq": seq});
            if let Some(w) = writer {
                r["writer"] = json!(w);
            }
            (200, r)
        }
        None => (400, json!({"error": "bad key"})),
    }
}

fn ns_match(path: &str) -> Option<String> {
    // NS_RE ^([a-z]+)/([\w\-./]+)$
    let mut parts = path.splitn(2, '/');
    let ns = parts.next()?;
    let key = parts.next()?;
    if ns.is_empty() || key.is_empty() { return None; }
    if !ns.chars().all(|c| c.is_ascii_lowercase()) { return None; }
    if !key.chars().all(|c| c.is_ascii_alphanumeric() || "-_./".contains(c)) { return None; }
    Some(format!("{}/{}", ns, key))
}

fn handle_get(store: &Arc<Store>, path: &str, query: &[(String, String)]) -> (u16, Value) {
    let limit = qp(query, "limit").and_then(|s| s.parse::<usize>().ok());
    let offset = qp(query, "offset").and_then(|s| s.parse::<usize>().ok()).unwrap_or(0);
    let since_seq = qp(query, "since_seq").and_then(|s| s.parse::<u64>().ok());
    let op_filter = qp(query, "op");
    let node_filter = qp(query, "node");

    match path {
        "help" => (200, json!({
            "server": "blackboard v0.6",
            "endpoints": [
                {"method": "PUT", "path": "/<ns>/<key>", "desc": "写 KV（X-Writer 签名，响应带 seq）"},
                {"method": "GET", "path": "/<ns>/<key>", "desc": "读单个 KV"},
                {"method": "GET", "path": "/<ns>/", "desc": "列命名空间（?limit=&offset= 分页）"},
                {"method": "DELETE", "path": "/<ns>/<key>", "desc": "删 KV（占全局 seq）"},
                {"method": "POST", "path": "/subscribe", "desc": "订阅 topic→callback（unsub=true 退订；GET /subs 列表）"},
                {"method": "GET", "path": "/clock", "desc": "对时：当前全局 seq + 黑板权威时间"},
                {"method": "GET", "path": "/timeline?since_seq=N&limit=M&op=PUT|DELETE", "desc": "增量事件时间轴（基线对齐）"},
                {"method": "GET", "path": "/ns-registry", "desc": "角色→职责命名空间注册表"},
                {"method": "GET", "path": "/tasks?node=<id>", "desc": "任务卡列表（?node= 收件定向过滤，v0.6）"},
                {"method": "GET", "path": "/stores", "desc": "门店类型节点列表（v0.6）"},
                {"method": "GET", "path": "/help", "desc": "本帮助"},
            ],
            "write_example": "curl -X PUT http://127.0.0.1:8792/data/<role>/<key> -d '{\"status\":\"...\"}' -H 'Content-Type: application/json' -H 'X-Writer: <agent-id>'",
            "role_ns": Store::role_ns_json(),
        })),
        "stores" => {
            let (stores, count) = store.list_stores();
            (200, json!({"stores": stores, "count": count, "ts": store_ts()}))
        }
        "ns-registry" => (200, json!({
            "registry": Store::role_ns_json(),
            "note": "各角色 STATUS/结果写自己的 ns，事件桥 data/ 前缀订阅自动回流（零 ACK）",
            "ts": store_ts(),
        })),
        "clock" => (200, json!({
            "seq": store.clock_seq(),
            "ts": store_ts(),
            "server": "blackboard",
            "host": hostname(),
        })),
        "timeline" => {
            let events: Vec<Value> = store.timeline_snapshot().iter()
                .filter(|e| since_seq.map_or(true, |s| e.seq > s))
                .filter(|e| op_filter.as_deref().map_or(true, |op| e.op == op))
                .map(|e| json!({"seq": e.seq, "op": e.op, "key": e.key, "ts": e.ts, "version": e.version}))
                .collect();
            let total = events.len();
            let events: Vec<Value> = if limit.is_some() {
                events.into_iter().skip(offset).take(limit.unwrap_or(0)).collect()
            } else {
                events
            };
            (200, json!({
                "events": events, "latest_seq": store.clock_seq(),
                "since_seq": since_seq, "op": op_filter,
                "total": total, "limit": limit, "offset": offset,
            }))
        }
        "subs" => {
            let subs: Vec<Value> = store.subs_list().iter()
                .map(|(t, c, ts)| json!({"topic": t, "callback": c, "ts": ts}))
                .collect();
            (200, json!({"subscribed": store.subs_len(), "subs": subs}))
        }
        _ if path.starts_with("audit") => (200, json!({"audit": true})),
        _ => {
            if !path.contains('/') || path.ends_with('/') {
                // 列命名空间
                let ns = path.trim_end_matches('/').split('/').next().unwrap_or("").to_string();
                if ns.is_empty() { return (400, json!({"error": "bad key"})); }
                let (items, total) = store.list_ns(&ns, node_filter.as_deref(), limit, offset);
                (200, json!({"list": items, "total": total, "limit": limit, "offset": offset}))
            } else {
                // 读单键
                match ns_match(path) {
                    Some(full) => {
                        match store.get(&full) {
                            Some(e) => (200, json!({"key": full, "version": e.version, "value": e.value, "ts": e.ts})),
                            None => (404, json!({"error": "not found"})),
                        }
                    }
                    None => (400, json!({"error": "bad key"})),
                }
            }
        }
    }
}

fn store_ts() -> String {
    crate::store::now_ts_public()
}

fn hostname() -> String {
    std::env::var("HOSTNAME").unwrap_or_else(|_| "mac-mini".into())
}

// ── 响应 ──
fn respond(stream: &mut TcpStream, status: u16, body: &Value) -> Result<(), String> {
    let body_s = body.to_string();
    let status_line = match status {
        200 => "200 OK",
        400 => "400 Bad Request",
        401 => "401 Unauthorized",
        404 => "404 Not Found",
        405 => "405 Method Not Allowed",
        _ => "500 Internal Server Error",
    };
    let resp = format!(
        "HTTP/1.1 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status_line, body_s.len(), body_s
    );
    stream.write_all(resp.as_bytes()).map_err(|e| e.to_string())?;
    Ok(())
}
