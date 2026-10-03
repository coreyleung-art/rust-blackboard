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
pub fn serve_sse(store: Arc<Store>, port: u16) {
    let listener = match TcpListener::bind(format!("0.0.0.0:{}", port)) {
        Ok(l) => l,
        Err(e) => { eprintln!("sse bind :{} failed: {}", port, e); return; }
    };
    for stream in listener.incoming() {
        if let Ok(s) = stream {
            let st = store.clone();
            std::thread::spawn(move || {
                let _ = handle_sse_conn(st, s);
            });
        }
    }
}

fn handle_sse_conn(store: Arc<Store>, mut stream: TcpStream) -> Result<(), String> {
    let req = match parse_request(&mut stream) { Some(r) => r, None => return Ok(()) };
    let path = req.path.trim_end_matches('/').to_string();
    // P1-3c: SSE 端点认证（token 启用时校验；/events 非公开白名单）
    // v0.6.5: /events 豁免认证——SSE 是只读事件流（含 i9 订阅，其 central-inbox 无 token），写端才需 token
    let sse_public = path.ends_with("/events") || path == "events";
    {
        // v0.6.6: 本机回环豁免（同 handle_conn 逻辑）
        let is_loopback = stream.peer_addr()
            .map(|addr| addr.ip().is_loopback())
            .unwrap_or(false);
        let token_hdr = header(&req.headers, "x-blackboard-token")
            .or_else(|| header(&req.headers, "authorization"));
        if !sse_public && !is_loopback && !store.authorized(token_hdr) {
            respond(&mut stream, 401, &json!({"error": "unauthorized"}));
            return Ok(());
        }
    }
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
            // ★ v0.6.9 变更3：Last-Event-ID catch-up——断线期间的漏事件按序重放
            let last_id: u64 = req
                .headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("last-event-id"))
                .and_then(|(_, v)| v.trim().parse::<u64>().ok())
                .unwrap_or(0);
            if last_id > 0 {
                for (seq, evt) in crate::sse::replay_after(last_id) {
                    let frame = format!("event: change\nid: {}\ndata: {}\n\n", seq, evt);
                    if stream.write_all(frame.as_bytes()).is_err() { return Ok(()); }
                    stream.flush().ok();
                }
            }
            loop {
                match rx.recv_timeout(Duration::from_secs(crate::sse::PING_SECS)) {
                    Ok((seq, evt)) => {
                        let frame = format!("event: change\nid: {}\ndata: {}\n\n", seq, evt);
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
    // 认证（P1-3c：白名单端点免认证，其余校验 token）
    let raw_path = req.path.trim_start_matches('/').to_string();
    // v0.6.5: 认证判定用完整 path?query（tasks?node=i9 豁免需要 query）
    let full_path = if req.query.is_empty() {
        raw_path.clone()
    } else {
        format!("{}?{}", raw_path, req.query.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<_>>().join("&"))
    };
    if !store.is_public_path(&full_path) {
        // v0.6.6: 本机回环豁免——127.0.0.1 本地请求免 token（本机所有进程/脚本自然通过，
        // 老登脚本等无需逐个加 token；远端经 Tailscale IP 访问仍需 token，不影响跨端安全）
        let is_loopback = stream.peer_addr()
            .map(|addr| addr.ip().is_loopback())
            .unwrap_or(false);
        let token_hdr = header(&req.headers, "x-blackboard-token")
            .or_else(|| header(&req.headers, "authorization"));
        // ★ v0.6.14：只对写方法鉴权（GET 公开——设计承诺「读公开、写鉴权」；
        //   flip 首轮把读也封了，MBP 同步读 401 属事故，已修）
        let is_write = matches!(req.method.as_str(), "PUT" | "POST" | "DELETE");
        if is_write && !is_loopback && !store.authorized(token_hdr) {
            return respond(&mut stream, 401, &json!({"error": "unauthorized"}));
        }
    }
    let writer = header(&req.headers, "x-writer").map(|s| s.to_string());
    let path = req.path.trim_start_matches('/').to_string();

    let resp: (u16, Value) = match req.method.as_str() {
        "GET" => handle_get(store, &path, &req.query),
        "PUT" => {
            // v0.6.8 修复：静默丢数据（accept-and-discard）
            //
            // 旧实现（承袭 Python 版）：body 非空但解析不出 JSON 时，直接落盘空对象 {}
            // 并返回 200 —— 调用方无法从状态码区分「写成功」与「写丢了」。
            // 实测复现（2026-09-11）：裸文本 body → 200，回读 value={}；
            // 且无论 Content-Type 是 text/plain 还是 application/json 都一样
            // （服务端从不读 Content-Type，触发条件只是「body 不是合法 JSON」）。
            //
            // 现改为 fail loud：非 JSON body 明确 400 拒绝，并给出可用写法；
            // 保留合法用法：空 body = 建键/占位 → {}；JSON 任意类型按原样落盘。
            // 兼容逃生门：显式声明 X-Body-Text: 1 时按纯文本接收，
            // 落成 {"content": <text>}（保住内容，不再丢）。
            let raw_text_optin = header(&req.headers, "x-body-text")
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false);
            if req.body.is_empty() {
                do_put(store, &path, Value::Object(serde_json::Map::new()), writer.as_deref())
            } else if raw_text_optin {
                let text = String::from_utf8_lossy(&req.body).to_string();
                do_put(store, &path, json!({ "content": text }), writer.as_deref())
            } else {
                match serde_json::from_slice::<Value>(&req.body) {
                    Ok(Value::Null) => do_put(
                        store, &path, Value::Object(serde_json::Map::new()), writer.as_deref(),
                    ),
                    Ok(v) => do_put(store, &path, v, writer.as_deref()),
                    Err(e) => {
                        let prefix: String =
                            String::from_utf8_lossy(&req.body).chars().take(80).collect();
                        (
                            400,
                            json!({
                                "error": "invalid body: expected JSON",
                                "detail": e.to_string(),
                                "received_prefix": prefix,
                                "key": path,
                                "hint": "PUT body 必须是合法 JSON（对象/数组/字符串均可）。纯文本请加请求头 'X-Body-Text: 1'，将落成 {\"content\": \"...\"}。"
                            }),
                        )
                    }
                }
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
            let path_clean = path.trim_end_matches('/').to_string();
            if path_clean == "register" {
                // P1-3a 设备注册：生成 device-id + token，存 nodes/<device>/identity
                let name = body.get("name").and_then(|n| n.as_str()).unwrap_or("device").to_string();
                let ts = crate::store::now_ts_public();
                let seed = format!("{}:{}:{}", name, ts, std::process::id());
                let device_id = format!("dev-{}", simple_hash(&seed));
                let token = format!("tk-{}", simple_hash(&format!("{}:{}", device_id, seed)));
                let identity = json!({
                    "device_id": device_id,
                    "name": name,
                    "token": token,
                    "registered": ts,
                    "status": "active"
                });
                let (_ver, _seq) = store.put(&format!("nodes/{}/identity", name), identity.clone(), Some("register"));
                // ★ v0.6.10：广播由 store.put→notify 单点发出（此处原为冗余二次广播，SSE 客户端每事件收两遍）
                (200, json!({
                    "device_id": device_id,
                    "token": token,
                    "name": name,
                    "registered": ts
                }))
            } else if path_clean.ends_with("subscribe") {
                let topic = body.get("topic").and_then(|t| t.as_str()).unwrap_or("");
                let cb = body.get("callback").and_then(|c| c.as_str()).unwrap_or("");
                let unsub = body.get("unsub").and_then(|u| u.as_bool()).unwrap_or(false);
                let n = store.subscribe(topic, cb, unsub);
                if unsub {
                    let removed = 1;
                    (200, json!({"unsubscribed": removed, "subscribed": n}))
                } else {
                    (200, json!({"subscribed": n}))
                }
            } else {
                (400, json!({"error": "use /subscribe or /register"}))
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
            // ★ v0.6.10：冗余二次广播已删（store.put→notify 单点广播）
            let mut r = json!({"key": full, "version": ver, "seq": seq});
            if let Some(w) = writer {
                r["writer"] = json!(w);
            }
            (200, r)
        }
        None => (400, bad_key_err(path)),
    }
}

/// 400「键写法非法」的统一响应体（v0.6.8 新增）
///
/// 起因（明鉴首例 2026-09-10，R003 已入册）：调用方拿到 400 的第一反应常是
/// 「对象不存在 / 卡没写进去」→ **排查方向被误导**（去查"为什么写失败"，而非"键写法对不对"）。
/// 服务端此前只回 `{"error":"bad key"}`，不说明"是写法错、不是不存在"，于是每个直接 curl
/// 的智能体都要重走一遍误判（bb-write.py / reflect-collect 已在客户端侧做过提示，
/// 但服务端侧一直是裸状态码）。现补足：**保留 `error` 字段**（兼容既有判据），
/// 并给出原因、收到的键、合法示例，以及 400/404 的语义对照。
fn bad_key_err(path: &str) -> Value {
    let first = path.trim_start_matches('/').split('/').next().unwrap_or("");
    let reason = if first.is_empty() {
        "键为空".to_string()
    } else if !first.chars().all(|c| c.is_ascii_lowercase()) {
        format!(
            "首段命名空间 '{}' 非法：必须是纯小写字母 [a-z]+（不能含连字符 / 数字 / 下划线 / 大写）",
            first
        )
    } else {
        "键格式非法（如空段 //、含空白字符等）".to_string()
    };
    json!({
        "error": "bad key",
        "detail": "400 = 键【写法非法】，不是「对象不存在」",
        "reason": reason,
        "received": path,
        "hint": "首段须为纯小写字母命名空间。合法示例: data/cld-health/<key> · notes/<会话或节点>/<key> · tasks/<节点>/<key>。对照：404 = 写法合法但该键不存在",
        "legal_examples": ["data/cld-health/phi13-example", "notes/mac-mini/example", "tasks/i9/example"],
    })
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

/// 简单确定性哈希（device-id/token 生成，非密码学用途；签名防伪走 Ed25519）
fn simple_hash(s: &str) -> String {
    let mut h: u64 = 1469598103934665603;
    for b in s.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(1099511628211);
    }
    format!("{:016x}", h)
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
                //
                // v0.6.8 修复（2026-09-11，跨三方实测取证）：
                // 旧实现 `path.trim_end_matches('/').split('/').next()` **把路径截断成第一段**，
                // 于是 `GET /data/reflect/` 与 `GET /data/` 返回**逐字节相同**的整个 data 命名空间
                // （实测 34.88MB / 20,214 键；而真正属 `data/reflect/` 前缀的只有 11 个）。
                // 注意：**store.list_ns 本身是按前缀过滤的**（`k.starts_with("{ns}/")`），
                // 即"前缀列举"从来不是缺失功能，而是被这一行截断吃掉了。
                // 后果（三方实测并写入 R003）：① 据"按前缀列举"写逻辑必错 ② 「回读含标记即落地」必然假通过
                // ③ 单次回读 35MB → 小工具 OOM（本机 cage 4GB，今夜已 17 次 OOM）。
                // 现按**完整前缀**列举：`GET /data/` → data 全部；`GET /data/reflect/` → 仅该前缀。
                let ns = path.trim_end_matches('/').to_string();
                if ns.is_empty() { return (400, bad_key_err(path)); }
                let (items, total) = store.list_ns(&ns, node_filter.as_deref(), limit, offset);
                (200, json!({"list": items, "total": total, "limit": limit, "offset": offset}))
            } else {
                // 读单键
                match ns_match(path) {
                    Some(full) => {
                        match store.get(&full) {
                            Some(e) => (200, json!({"key": full, "version": e.version, "value": e.value, "ts": e.ts})),
                            None => (404, json!({
                                "error": "not found",
                                "key": full,
                                "detail": "404 = 键写法合法，但该键不存在（≠ 400 写法非法）",
                                "hint": "先确认键写法是否合法；若刚写入，必须回读校验内容（写入返回 200 不等于内容已落盘）",
                            })),
                        }
                    }
                    None => (400, bad_key_err(path)),
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
