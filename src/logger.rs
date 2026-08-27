// logger.rs — 统一结构化日志（模板：三项目复制此文件 + 改 PROJECT 常量）
// 规范：rust-toolchain/docs/logging-spec-v1.0.md
// 特性：JSON 行格式 / 双写 stdout+文件 / 5MB 轮转保留 3 / 级别过滤
use std::io::Write;
use std::sync::Mutex;

pub const PROJECT: &str = "rust-blackboard"; // 每项目改这里

pub const LEVEL_DEBUG: u8 = 0;
pub const LEVEL_INFO: u8 = 1;
pub const LEVEL_WARN: u8 = 2;
pub const LEVEL_ERROR: u8 = 3;

static LOG_LEVEL: Mutex<u8> = Mutex::new(LEVEL_INFO);
static LOG_DIR: Mutex<Option<String>> = Mutex::new(None);

fn now_ts_local() -> String {
    chrono::Local::now().format("%Y-%m-%dT%H:%M:%S").to_string()
}

pub fn init(level: u8) {
    *LOG_LEVEL.lock().unwrap() = level;
    if let Ok(dir) = std::env::var("DSH_LOG_DIR") {
        *LOG_DIR.lock().unwrap() = Some(dir);
    }
    std::fs::create_dir_all(log_dir()).ok();
}

fn log_dir() -> String {
    if let Some(d) = LOG_DIR.lock().unwrap().as_ref() {
        return d.clone();
    }
    // 默认路径按平台
    if cfg!(windows) {
        format!("{}\\.dsh\\logs", std::env::var("USERPROFILE").unwrap_or_else(|_| ".".into()))
    } else if cfg!(target_os = "macos") {
        format!("{}/dsh-collab/logs", std::env::var("HOME").unwrap_or_else(|_| ".".into()))
    } else {
        std::env::var("HOME").map(|h| format!("{}/dsh-collab/logs", h)).unwrap_or_else(|_| "/tmp".into())
    }
}

fn log_path() -> String {
    format!("{}/{}.log", log_dir(), PROJECT)
}

pub fn log(level: u8, level_name: &str, comp: &str, msg: &str) {
    if level < *LOG_LEVEL.lock().unwrap() {
        return;
    }
    // JSON 转义 msg
    let msg_escaped = msg.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n");
    let ts = now_ts_local();
    let line = format!(
        "{{\"ts\":\"{}\",\"level\":\"{}\",\"comp\":\"{}\",\"msg\":\"{}\"}}\n",
        ts, level_name, comp, msg_escaped
    );
    // stdout（忽略错误——Windows 无 console 不 panic）
    let _ = std::io::stdout().write_all(line.as_bytes());
    // 文件（轮转 + 追加）
    let _ = write_with_rotate(&line);
}

fn write_with_rotate(line: &str) -> std::io::Result<()> {
    let path = log_path();
    // 轮转检查：> 5MB
    if let Ok(md) = std::fs::metadata(&path) {
        if md.len() > 5 * 1024 * 1024 {
            rotate();
        }
    }
    let mut f = std::fs::OpenOptions::new().create(true).append(true).open(&path)?;
    f.write_all(line.as_bytes())
}

fn rotate() {
    let base = log_path();
    for i in (1..3).rev() {
        let src = format!("{}.{}", base, i);
        let dst = format!("{}.{}", base, i + 1);
        if std::path::Path::new(&src).exists() {
            let _ = std::fs::rename(&src, &dst);
        }
    }
    if std::path::Path::new(&base).exists() {
        let _ = std::fs::rename(&base, format!("{}.1", base));
    }
}

#[macro_export]
macro_rules! log_info {
    ($comp:expr, $msg:expr) => { $crate::logger::log($crate::logger::LEVEL_INFO, "INFO", $comp, $msg) };
}
#[macro_export]
macro_rules! log_warn {
    ($comp:expr, $msg:expr) => { $crate::logger::log($crate::logger::LEVEL_WARN, "WARN", $comp, $msg) };
}
#[macro_export]
macro_rules! log_error {
    ($comp:expr, $msg:expr) => { $crate::logger::log($crate::logger::LEVEL_ERROR, "ERROR", $comp, $msg) };
}
#[macro_export]
macro_rules! log_debug {
    ($comp:expr, $msg:expr) => { $crate::logger::log($crate::logger::LEVEL_DEBUG, "DEBUG", $comp, $msg) };
}
