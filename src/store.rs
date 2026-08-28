// store.rs — 黑板状态层：KV state + HLC 时钟 + audit 持久化 + 快照 + 订阅 + 镜像 + notify
// 数据格式与 Python blackboard-server-v0.6 完全一致（snapshot.json / audit.jsonl / subs.json）
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Mutex;

pub struct Config {
    pub port: u16,
    pub data_dir: String,
    pub token: String,
}

pub fn default_data_dir() -> String {
    std::env::var("HOME").unwrap_or_else(|_| ".".into()) + "/dsh-collab/token-monitor/blackboard"
}

const AUDIT_ROTATE_BYTES: u64 = 5 * 1024 * 1024;
const SNAPSHOT_FILE: &str = "snapshot.json";
const SUBS_FILE: &str = "subs.json";
const ARCHIVE_KEEP: usize = 10;
const TIMELINE_MAX: usize = 20000;
const NOTIFY_POOL: usize = 8; // notify 异步线程池大小（与 Python ThreadPoolExecutor(8) 对齐）

// 职责命名空间注册表（v0.5，与 Python 版一致）
const ROLE_NS: [(&str, (&str, &str)); 12] = [
    ("6ed4daf2", ("恢复自查", "data/recovery/")),
    ("a3bc8cba", ("学习", "data/learning/")),
    ("aa528267", ("运营", "data/ops/")),
    ("45f89009", ("运营", "data/ops/")),
    ("4787d717", ("数据调查", "data/investigate/")),
    ("0e84e65c", ("供应链", "data/supply-chain/")),
    ("ffb7c3ab", ("QA", "data/qa/")),
    ("54e809ed", ("媒体", "data/media/")),
    ("55d4d1bd", ("摄取", "data/ingest/")),
    ("2a15e6b1", ("HR", "data/registry/")),
    ("b193c782", ("客服", "data/customer-service/")),
    ("coordinator", ("协调者", "data/iterations/")),
];

#[derive(Clone)]
pub struct Entry {
    pub version: u64,
    pub value: Value,
    pub ts: String,
}

pub struct Sub {
    pub topic: String,
    pub callback: String,
    pub ts: String,
}

pub struct Store {
    pub data_dir: String,
    pub token: String,
    pub port: u16,
    state: Mutex<HashMap<String, Entry>>,
    subs: Mutex<HashMap<(String, String), Sub>>,
    // HLC 时钟
    clock: Mutex<Clock>,
    timeline: Mutex<Vec<TimelineEvent>>,
    io_lock: Mutex<()>, // audit/快照/归档 写锁
}

pub fn now_ts_public() -> String { now_ts() }

struct Clock {
    last_seq: u64,
    last_phys: u64,
    snapshot_base: u64,
}

#[derive(Clone)]
pub struct TimelineEvent {
    pub seq: u64,
    pub op: String,
    pub key: String,
    pub ts: String,
    pub version: u64,
}

fn now_ts() -> String {
    // ISO 秒级本地时间，与 Python datetime.now().isoformat(timespec="seconds") 完全一致（本地时区）
    // 用 chrono Local（自动读系统本地时区；存活判定依赖 ts 本地解析，时区错位会误判离线）
    chrono::Local::now().format("%Y-%m-%dT%H:%M:%S").to_string()
}

impl Store {
    pub fn new(cfg: Config) -> Store {
        let s = Store {
            data_dir: cfg.data_dir,
            token: cfg.token,
            port: cfg.port,
            state: Mutex::new(HashMap::new()),
            subs: Mutex::new(HashMap::new()),
            clock: Mutex::new(Clock { last_seq: 0, last_phys: 0, snapshot_base: 0 }),
            timeline: Mutex::new(Vec::new()),
            io_lock: Mutex::new(()),
        };
        s.load();
        s
    }

    // ── 加载（snapshot 优先 + audit 增量 + subs）──
    fn load(&self) {
        fs::create_dir_all(&self.data_dir).ok();
        let sp = PathBuf::from(&self.data_dir).join(SNAPSHOT_FILE);
        let mut max_seq: u64 = 0;
        if sp.exists() {
            if let Ok(txt) = fs::read_to_string(&sp) {
                if let Ok(snap) = serde_json::from_str::<Value>(&txt) {
                    if let Some(st) = snap.get("state").and_then(|s| s.as_object()) {
                        let mut state = self.state.lock().unwrap();
                        for (k, v) in st {
                            if let Some(entry) = parse_entry(v) {
                                state.insert(k.clone(), entry);
                            }
                        }
                    }
                    max_seq = snap.get("last_seq").and_then(|s| s.as_u64()).unwrap_or(0);
                    if let Some(tl) = snap.get("timeline").and_then(|t| t.as_array()) {
                        let mut timeline = self.timeline.lock().unwrap();
                        for e in tl {
                            if let Some(ev) = parse_timeline(e) {
                                timeline.push(ev);
                            }
                        }
                    }
                }
            }
        }
        // 重放当前 audit.jsonl（快照之后的增量）
        let ap = PathBuf::from(&self.data_dir).join("audit.jsonl");
        if ap.exists() {
            if let Ok(txt) = fs::read_to_string(&ap) {
                for line in txt.lines() {
                    let line = line.trim();
                    if line.is_empty() { continue; }
                    if let Ok(e) = serde_json::from_str::<Value>(line) {
                        let op = e.get("op").and_then(|o| o.as_str()).unwrap_or("");
                        let key = e.get("key").and_then(|k| k.as_str()).unwrap_or("").to_string();
                        if op == "PUT" {
                            if let Some(v) = e.get("value") {
                                let ver = e.get("version").and_then(|x| x.as_u64()).unwrap_or(1);
                                let ts = e.get("ts").and_then(|x| x.as_str()).unwrap_or("").to_string();
                                self.state.lock().unwrap().insert(key.clone(), Entry { version: ver, value: v.clone(), ts });
                            }
                        } else if op == "DELETE" {
                            self.state.lock().unwrap().remove(&key);
                        }
                        let seq = e.get("seq").and_then(|s| s.as_u64()).unwrap_or(0);
                        if seq > 0 {
                            let ver = e.get("version").and_then(|x| x.as_u64()).unwrap_or(0);
                            self.timeline_append(seq, op.to_string(), key, ver);
                            if seq > max_seq { max_seq = seq; }
                        }
                    }
                }
            }
        }
        if max_seq > 0 {
            let mut clk = self.clock.lock().unwrap();
            clk.last_seq = max_seq;
            clk.snapshot_base = max_seq;
            clk.last_phys = max_seq / 1_000_000;
        }
        self.load_subs();
    }

    fn load_subs(&self) {
        let p = PathBuf::from(&self.data_dir).join(SUBS_FILE);
        if p.exists() {
            if let Ok(txt) = fs::read_to_string(&p) {
                if let Ok(d) = serde_json::from_str::<Value>(&txt) {
                    if let Some(list) = d.get("subs").and_then(|s| s.as_array()) {
                        let mut subs = self.subs.lock().unwrap();
                        for s in list {
                            let topic = s.get("topic").and_then(|t| t.as_str()).unwrap_or("").to_string();
                            let cb = s.get("callback").and_then(|c| c.as_str()).unwrap_or("").to_string();
                            let ts = s.get("ts").and_then(|x| x.as_str()).unwrap_or("").to_string();
                            if !topic.is_empty() && !cb.is_empty() {
                                subs.insert((topic.clone(), cb.clone()), Sub { topic, callback: cb, ts });
                            }
                        }
                    }
                }
            }
        }
    }

    // ── HLC 时钟（与 Python _next_seq 逐行对齐）──
    pub fn next_seq(&self) -> u64 {
        let mut clk = self.clock.lock().unwrap();
        let phys = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        if phys == clk.last_phys {
            clk.last_seq += 1;
        } else if phys > clk.last_phys {
            clk.last_seq = phys * 1_000_000 + 1;
            clk.last_phys = phys;
        } else {
            clk.last_seq += 1; // 时钟回拨兜底
        }
        if clk.last_seq <= clk.snapshot_base {
            clk.last_seq = clk.snapshot_base + 1;
        }
        clk.last_seq
    }

    pub fn clock_seq(&self) -> u64 {
        self.clock.lock().unwrap().last_seq
    }

    fn timeline_append(&self, seq: u64, op: String, key: String, version: u64) {
        let mut tl = self.timeline.lock().unwrap();
        tl.push(TimelineEvent { seq, op, key, ts: now_ts(), version });
        if tl.len() > TIMELINE_MAX {
            let excess = tl.len() - TIMELINE_MAX;
            tl.drain(..excess);
        }
    }

    pub fn timeline_snapshot(&self) -> Vec<TimelineEvent> {
        self.timeline.lock().unwrap().clone()
    }

    pub fn timeline_len(&self) -> usize {
        self.timeline.lock().unwrap().len()
    }

    // ── audit 持久化（与 Python _audit 对齐：含 writer 签名）──
    fn audit(&self, op: &str, key: &str, value: Option<&Value>, seq: u64, writer: Option<&str>) {
        fs::create_dir_all(&self.data_dir).ok();
        let ver = self.state.lock().unwrap().get(key).map(|e| e.version).unwrap_or(1);
        let mut entry = json!({
            "op": op, "key": key, "version": ver,
            "value": value.cloned().unwrap_or(Value::Null),
            "ts": now_ts(), "seq": seq,
        });
        if let Some(w) = writer {
            entry["writer"] = json!(w);
        }
        let _guard = self.io_lock.lock().unwrap();
        let ap = PathBuf::from(&self.data_dir).join("audit.jsonl");
        if let Ok(mut f) = fs::OpenOptions::new().create(true).append(true).open(&ap) {
            let _ = writeln!(f, "{}", entry);
        }
    }

    // ── PUT（与 Python do_PUT 对齐：写 state → audit → timeline → 镜像 → notify → 轮转）──
    pub fn put(&self, ns_key: &str, value: Value, writer: Option<&str>) -> (u64, u64) {
        let seq = self.next_seq();
        let cur = self.state.lock().unwrap().get(ns_key).map(|e| e.version).unwrap_or(0);
        let ver = cur + 1;
        self.state.lock().unwrap().insert(ns_key.to_string(), Entry { version: ver, value: value.clone(), ts: now_ts() });
        self.audit("PUT", ns_key, Some(&value), seq, writer);
        self.timeline_append(seq, "PUT".into(), ns_key.to_string(), ver);
        self.mirror_result(ns_key, seq);
        self.notify(ns_key, seq);
        self.rotate_audit();
        (ver, seq)
    }

    // ── DELETE（与 Python do_DELETE 对齐）──
    pub fn delete(&self, ns_key: &str, writer: Option<&str>) -> bool {
        let exists = self.state.lock().unwrap().contains_key(ns_key);
        if exists {
            let seq = self.next_seq();
            self.state.lock().unwrap().remove(ns_key);
            self.audit("DELETE", ns_key, None, seq, writer);
            self.timeline_append(seq, "DELETE".into(), ns_key.to_string(), 0);
            self.notify(ns_key, seq);
        }
        exists
    }

    // ── v0.2 镜像：tasks/<node>/result → tasks/<node>/results/<seq 后10位> ──
    fn mirror_result(&self, key: &str, seq: u64) {
        // 正则 ^tasks/([\w\-]+)/result$ 的手写匹配
        if let Some(rest) = key.strip_prefix("tasks/") {
            if let Some(node) = rest.strip_suffix("/result") {
                if !node.is_empty() && !node.contains('/') {
                    let val = self.state.lock().unwrap().get(key).map(|e| e.value.clone());
                    if let Some(Value::Object(_)) = val {
                        let seq_s = format!("{:010}", seq % 10_000_000_000);
                        let mk = format!("tasks/{}/results/{}", node, seq_s);
                        let cur = self.state.lock().unwrap().get(&mk).map(|e| e.version).unwrap_or(0);
                        let val = self.state.lock().unwrap().get(key).map(|e| e.value.clone()).unwrap_or(Value::Null);
                        self.state.lock().unwrap().insert(mk.clone(), Entry { version: cur + 1, value: val, ts: now_ts() });
                        self.audit("PUT", &mk, None, seq, None); // 镜像不触发 notify
                    }
                }
            }
        }
    }

    // ── 订阅（与 Python do_POST 对齐）──
    pub fn subscribe(&self, topic: &str, callback: &str, unsub: bool) -> usize {
        if unsub {
            {
                let mut subs = self.subs.lock().unwrap();
                subs.retain(|(t, c), _| !(t == topic && c == callback));
            } // 先释放 subs 锁再保存（Mutex 不可重入，持锁调 save_subs 会死锁）
            self.save_subs();
            return self.subs.lock().unwrap().len();
        }
        {
            let mut subs = self.subs.lock().unwrap();
            let key = (topic.to_string(), callback.to_string());
            if !subs.contains_key(&key) {
                subs.insert(key, Sub { topic: topic.into(), callback: callback.into(), ts: now_ts() });
            }
        }
        self.save_subs();
        self.subs.lock().unwrap().len()
    }

    pub fn subs_list(&self) -> Vec<(String, String, String)> {
        self.subs.lock().unwrap().values()
            .map(|s| (s.topic.clone(), s.callback.clone(), s.ts.clone()))
            .collect()
    }

    pub fn subs_len(&self) -> usize {
        self.subs.lock().unwrap().len()
    }

    fn save_subs(&self) {
        let list: Vec<Value> = self.subs.lock().unwrap().values()
            .map(|s| json!({"topic": s.topic, "callback": s.callback, "ts": s.ts}))
            .collect();
        let d = json!({"subs": list});
        let _guard = self.io_lock.lock().unwrap();
        let p = PathBuf::from(&self.data_dir).join(SUBS_FILE);
        let tmp = PathBuf::from(&self.data_dir).join(format!("{}.tmp", SUBS_FILE));
        if let Ok(txt) = serde_json::to_string(&d) {
            if fs::write(&tmp, txt).is_ok() {
                let _ = fs::rename(&tmp, &p);
            }
        }
    }

    // ── notify：topic 前缀匹配 → 线程池异步 POST 回调（失败静默）+ SSE 广播 ──
    fn notify(&self, key: &str, seq: u64) {
        // SSE 广播（事件桥并入：原 8803 独立进程的职责，现在黑板内直接广播）
        let entry = self.state.lock().unwrap().get(key).cloned();
        crate::sse::broadcast(key, entry.as_ref().map(|e| &e.value), entry.as_ref().map(|e| e.version).unwrap_or(0));
        // HTTP 回调（原有订阅者）
        let targets: Vec<(String, String)> = self.subs.lock().unwrap().values()
            .filter(|s| key.starts_with(&s.topic))
            .map(|s| (s.callback.clone(), key.to_string()))
            .collect();
        for (cb, k) in targets {
            let k2 = k.clone();
            let state_snapshot = self.state.lock().unwrap().get(&k2).cloned();
            notify_pool_send(cb, k2, seq, state_snapshot);
        }
    }

    // ── audit 轮转（与 Python _rotate_audit 对齐）──
    fn rotate_audit(&self) {
        let _guard = self.io_lock.lock().unwrap();
        let ap = PathBuf::from(&self.data_dir).join("audit.jsonl");
        let size = fs::metadata(&ap).map(|m| m.len()).unwrap_or(0);
        if size <= AUDIT_ROTATE_BYTES { return; }
        // 快照
        self.write_snapshot_inner();
        // 归档
        let ts = now_ts().replace([':', '-'], "").replace('T', "-");
        let arch = PathBuf::from(&self.data_dir).join(format!("audit-{}.jsonl", &ts[..14]));
        let _ = fs::rename(&ap, &arch);
        // 压缩（简单：直接保留 .jsonl；Python 版 gzip——为兼容保留未压缩即可，读取端用 jsonl 也行）
        // 清理超龄（2026-08-28 补全：.jsonl 与 .gz 统一纳入保留策略，各保留 ARCHIVE_KEEP 份）
        let mut audits: Vec<String> = fs::read_dir(&self.data_dir)
            .map(|rd| rd.flatten().filter_map(|e| {
                let n = e.file_name().to_string_lossy().to_string();
                if n.starts_with("audit-") && (n.ends_with(".jsonl") || n.ends_with(".jsonl.gz")) { Some(n) } else { None }
            }).collect())
            .unwrap_or_default();
        audits.sort();
        let remove_n = audits.len().saturating_sub(ARCHIVE_KEEP);
        for old in audits.into_iter().take(remove_n) {
            let _ = fs::remove_file(PathBuf::from(&self.data_dir).join(old));
        }
    }

    fn write_snapshot_inner(&self) {
        let state_map: HashMap<String, Value> = self.state.lock().unwrap().iter()
            .map(|(k, e)| (k.clone(), json!({"version": e.version, "value": e.value, "ts": e.ts})))
            .collect();
        let tl: Vec<Value> = self.timeline.lock().unwrap().iter()
            .map(|e| json!({"seq": e.seq, "op": e.op, "key": e.key, "ts": e.ts, "version": e.version}))
            .collect();
        let snap = json!({
            "state": state_map,
            "last_seq": self.clock_seq(),
            "timeline": tl,
            "ts": now_ts(),
            "v": "0.4-snapshot",
        });
        let p = PathBuf::from(&self.data_dir).join(SNAPSHOT_FILE);
        let tmp = PathBuf::from(&self.data_dir).join(format!("{}.tmp", SNAPSHOT_FILE));
        if let Ok(txt) = serde_json::to_string(&snap) {
            if fs::write(&tmp, txt).is_ok() {
                let _ = fs::rename(&tmp, &p);
            }
        }
    }

    // ── 查询 ──
    pub fn get(&self, ns_key: &str) -> Option<Entry> {
        self.state.lock().unwrap().get(ns_key).cloned()
    }

    pub fn state_len(&self) -> usize {
        self.state.lock().unwrap().len()
    }

    pub fn list_ns(&self, ns: &str, node_filter: Option<&str>, limit: Option<usize>, offset: usize)
        -> (HashMap<String, Value>, usize)
    {
        let prefix = format!("{}/", ns);
        let state = self.state.lock().unwrap();
        let mut items: HashMap<String, Value> = HashMap::new();
        for (k, e) in state.iter() {
            if !k.starts_with(&prefix) { continue; }
            if ns == "tasks" {
                if let Some(nf) = node_filter {
                    let recv = e.value.get("recipient").and_then(|r| r.as_str());
                    let matched = match recv {
                        Some(r) => r == nf,
                        None => k.starts_with(&format!("tasks/{}/", nf)),
                    };
                    if !matched { continue; }
                }
            }
            items.insert(k.clone(), json!({"version": e.version, "value": e.value, "ts": e.ts}));
        }
        let total = items.len();
        let items = if limit.is_some() {
            let mut keys: Vec<&String> = items.keys().collect();
            keys.sort();
            let mut page = HashMap::new();
            for k in keys.into_iter().skip(offset).take(limit.unwrap_or(0)) {
                page.insert(k.clone(), items[k].clone());
            }
            page
        } else {
            items
        };
        (items, total)
    }

    // stores（v0.6）
    pub fn list_stores(&self) -> (HashMap<String, Value>, usize) {
        let state = self.state.lock().unwrap();
        let mut stores = HashMap::new();
        for (k, e) in state.iter() {
            if !k.starts_with("nodes/") || k.contains("/heartbeat") { continue; }
            let rest = &k[6..];
            if rest.contains('/') { continue; }
            if let Some(v) = e.value.as_object() {
                if v.get("type").and_then(|t| t.as_str()) == Some("store") {
                    stores.insert(rest.to_string(), e.value.clone());
                }
            }
        }
        let n = stores.len();
        (stores, n)
    }

    pub fn role_ns_json() -> Value {
        let mut m = serde_json::Map::new();
        for (k, (role, ns)) in ROLE_NS.iter() {
            m.insert(k.to_string(), json!({"role": role, "ns": ns}));
        }
        json!(m)
    }

    // 认证
    /// P1-3c 认证中间件：token 空=不校验；支持 X-Blackboard-Token 或 Bearer
    pub fn authorized(&self, header_token: Option<&str>) -> bool {
        if self.token.is_empty() { return true; }
        match header_token {
            Some(t) => {
                // 支持 X-Blackboard-Token: <token> 或 Authorization: Bearer <token>
                let clean = t.strip_prefix("Bearer ").unwrap_or(t).trim();
                clean == self.token.as_str()
            }
            None => false,
        }
    }
    /// 公开端点白名单（健康检查/对时免认证）
    pub fn is_public_path(&self, path: &str) -> bool {
        matches!(path, "clock" | "help" | "ns-registry")
    }
}

fn parse_entry(v: &Value) -> Option<Entry> {
    let version = v.get("version").and_then(|x| x.as_u64()).unwrap_or(0);
    let value = v.get("value").cloned().unwrap_or(Value::Null);
    let ts = v.get("ts").and_then(|x| x.as_str()).unwrap_or("").to_string();
    Some(Entry { version, value, ts })
}

fn parse_timeline(e: &Value) -> Option<TimelineEvent> {
    Some(TimelineEvent {
        seq: e.get("seq").and_then(|x| x.as_u64())?,
        op: e.get("op").and_then(|x| x.as_str()).unwrap_or("").to_string(),
        key: e.get("key").and_then(|x| x.as_str()).unwrap_or("").to_string(),
        ts: e.get("ts").and_then(|x| x.as_str()).unwrap_or("").to_string(),
        version: e.get("version").and_then(|x| x.as_u64()).unwrap_or(0),
    })
}

// ── notify 线程池（channel + 固定 8 worker，对齐 Python ThreadPoolExecutor(8)）──
struct NotifyMsg {
    callback: String,
    key: String,
    seq: u64,
    value: Option<Value>,
    version: u64,
}

fn notify_pool_send(callback: String, key: String, seq: u64, entry: Option<Entry>) {
    let version = entry.as_ref().map(|e| e.version).unwrap_or(0);
    let value = entry.as_ref().map(|e| e.value.clone());
    let msg = NotifyMsg { callback, key, seq, value, version };
    let _ = notify_tx().send(msg); // 队列满则丢弃（Python 线程池同样有上限，失败静默）
}

fn notify_tx() -> &'static std::sync::mpsc::Sender<NotifyMsg> {
    use std::sync::OnceLock;
    static TX: OnceLock<std::sync::mpsc::Sender<NotifyMsg>> = OnceLock::new();
    TX.get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::channel::<NotifyMsg>();
        let rx_shared: std::sync::Arc<std::sync::Mutex<std::sync::mpsc::Receiver<NotifyMsg>>> =
            std::sync::Arc::new(std::sync::Mutex::new(rx));
        for _ in 0..NOTIFY_POOL {
            let rx_clone = std::sync::Arc::clone(&rx_shared);
            std::thread::spawn(move || worker_loop(&rx_clone));
        }
        tx
    })
}

fn worker_loop(rx: &std::sync::Arc<std::sync::Mutex<std::sync::mpsc::Receiver<NotifyMsg>>>) {
    loop {
        let msg = rx.lock().unwrap().recv();
        match msg {
            Ok(m) => {
                let body = json!({
                    "key": m.key,
                    "value": m.value.unwrap_or(Value::Null),
                    "version": m.version,
                    "seq": m.seq,
                });
                let _ = http_post_json(&m.callback, &body);
            }
            Err(_) => break,
        }
    }
}

// 轻量 HTTP POST（notify 回调用，纯 std）
fn http_post_json(url: &str, body: &Value) -> Result<(), String> {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    let rest = url.trim_start_matches("http://");
    let (host, port) = match rest.find(':') {
        Some(i) => {
            let hp = &rest[..i];
            let port_s = rest[i + 1..].split('/').next().unwrap_or("80");
            (hp.to_string(), port_s.parse().unwrap_or(80))
        }
        None => {
            let hp = rest.split('/').next().unwrap_or(rest);
            (hp.to_string(), 80)
        }
    };
    let path = rest.split('/').skip(1).collect::<Vec<_>>().join("/");
    let path = format!("/{}", path);
    let body_s = body.to_string();
    let mut stream = TcpStream::connect((host.as_str(), port)).map_err(|e| e.to_string())?;
    stream.set_write_timeout(Some(std::time::Duration::from_secs(5))).ok();
    let req = format!(
        "POST {} HTTP/1.1\r\nHost: {}:{}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        path, host, port, body_s.len(), body_s
    );
    stream.write_all(req.as_bytes()).map_err(|e| e.to_string())?;
    let mut buf = Vec::new();
    let _ = stream.read_to_end(&mut buf);
    Ok(())
}

// ── 单元测试 ──
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn test_store() -> Store {
        let dir = format!("/tmp/bb-test-{}", std::process::id());
        let _ = fs::remove_dir_all(&dir);
        Store::new(Config { port: 0, data_dir: dir, token: String::new() })
    }

    #[test]
    fn hlc_clock_monotonic() {
        let s = test_store();
        let a = s.next_seq();
        let b = s.next_seq();
        assert!(b > a, "seq 必须严格单调递增: {} -> {}", a, b);
        // 同秒内应为 a+1
        assert!(b - a <= 2, "同秒递增应为 +1: {} -> {}", a, b);
        // 格式检查：物理秒*10^6 + 计数
        assert!(a >= 1_700_000_000_000_000, "seq 应含物理秒前缀: {}", a);
    }

    #[test]
    fn hlc_clock_snapshot_base_guard() {
        let s = test_store();
        // 防呆基准：snapshot_base 之后 seq 必须更大
        {
            let mut clk = s.clock.lock().unwrap();
            clk.snapshot_base = 999_999_999_999_999;
        }
        let seq = s.next_seq();
        assert!(seq > 999_999_999_999_999, "防呆应保证 seq > snapshot_base: {}", seq);
    }

    #[test]
    fn put_get_delete_roundtrip() {
        let s = test_store();
        let (ver, seq) = s.put("data/test/k1", json!({"a": 1}), Some("writer-x"));
        assert_eq!(ver, 1);
        assert!(seq > 0);
        let e = s.get("data/test/k1").expect("应读到 k1");
        assert_eq!(e.value["a"], 1);
        assert_eq!(e.version, 1);
        // 二次 put 版本递增
        let (ver2, _) = s.put("data/test/k1", json!({"a": 2}), None);
        assert_eq!(ver2, 2);
        // delete
        assert!(s.delete("data/test/k1", None));
        assert!(s.get("data/test/k1").is_none());
        assert!(!s.delete("data/test/k1", None), "重复 delete 应返回 false");
    }

    #[test]
    fn result_mirror_created() {
        let s = test_store();
        let (_, seq) = s.put("tasks/node1/result", json!({"ok": true, "task_id": "t1"}), None);
        // 镜像键 tasks/node1/results/<seq 后10位>
        let mirror = format!("tasks/node1/results/{:010}", seq % 10_000_000_000);
        let e = s.get(&mirror).expect("result 应自动镜像");
        assert_eq!(e.value["ok"], true);
    }

    #[test]
    fn mirror_only_for_result() {
        let s = test_store();
        s.put("data/normal/key", json!({"x": 1}), None);
        // 非 result 键不产生镜像
        let list = s.list_ns("data", None, None, 0).0;
        assert_eq!(list.len(), 1, "只有 1 个 data 键，无镜像");
    }

    #[test]
    fn subscribe_unsubscribe() {
        let s = test_store();
        assert_eq!(s.subscribe("data/", "http://cb:1", false), 1);
        // 去重
        assert_eq!(s.subscribe("data/", "http://cb:1", false), 1);
        // 不同 topic
        assert_eq!(s.subscribe("tasks/", "http://cb:1", false), 2);
        // 退订
        assert_eq!(s.subscribe("data/", "http://cb:1", true), 1);
        assert_eq!(s.subs_len(), 1);
    }

    #[test]
    fn list_ns_pagination() {
        let s = test_store();
        for i in 0..5 {
            s.put(&format!("data/pg/k{}", i), json!({"i": i}), None);
        }
        // limit=2
        let (items, total) = s.list_ns("data", None, Some(2), 0);
        assert_eq!(total, 5);
        assert_eq!(items.len(), 2);
        // offset=2 翻页
        let (items2, _) = s.list_ns("data", None, Some(2), 2);
        assert_eq!(items2.len(), 2);
        // 两页键不重叠
        let k1: Vec<&String> = items.keys().collect();
        let k2: Vec<&String> = items2.keys().collect();
        assert!(k1.iter().all(|k| !k2.contains(k)), "分页键不应重叠");
    }

    #[test]
    fn tasks_recipient_filter() {
        let s = test_store();
        s.put("tasks/node-a/queue/1", json!({"task_id": "a1", "recipient": "node-a"}), None);
        s.put("tasks/node-b/queue/2", json!({"task_id": "b1", "recipient": "node-b"}), None);
        s.put("tasks/node-a/queue/3", json!({"task_id": "a2"}), None); // 无 recipient 但前缀匹配
        let (items_a, _) = s.list_ns("tasks", Some("node-a"), None, 0);
        assert_eq!(items_a.len(), 2, "node-a 应收到 recipient=a + 前缀 tasks/node-a/ 的卡");
        let (items_b, _) = s.list_ns("tasks", Some("node-b"), None, 0);
        assert_eq!(items_b.len(), 1);
        // 无过滤 = 全部 3 张
        let (all, _) = s.list_ns("tasks", None, None, 0);
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn stores_filter() {
        let s = test_store();
        s.put("nodes/s1", json!({"type": "store", "name": "门店1"}), None);
        s.put("nodes/s2", json!({"type": "store"}), None);
        s.put("nodes/s3", json!({"type": "other"}), None);
        s.put("nodes/s1/heartbeat", json!({"health": "ok"}), None); // 排除 heartbeat
        let (stores, n) = s.list_stores();
        assert_eq!(n, 2, "只有 2 个 store 类型节点（s3 不是 store，s1/heartbeat 排除）");
        assert!(stores.contains_key("s1") && stores.contains_key("s2"));
    }

    #[test]
    fn authorized_token_check() {
        let dir = format!("/tmp/bb-tok-{}", std::process::id());
        let _ = fs::remove_dir_all(&dir);
        let s = Store::new(Config { port: 0, data_dir: dir, token: "secret".into() });
        assert!(!s.authorized(Some("wrong")));
        assert!(s.authorized(Some("secret")));
        assert!(!s.authorized(None));
    }

    #[test]
    fn role_ns_complete() {
        let v = Store::role_ns_json();
        let m = v.as_object().expect("registry 应为对象");
        assert!(m.contains_key("coordinator"), "协调者命名空间必在");
        assert!(m.contains_key("a3bc8cba"), "学习会话命名空间必在");
        assert_eq!(m.len(), 12, "12 个角色命名空间");
    }
}
