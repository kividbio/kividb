//! The server log.
//!
//! Every line is written whole, with a timestamp, the process id, its level
//! and the message:
//!
//! ```text
//! 2026-10-09T12:34:56.789Z 4242 [WARN] AOF: fsync failed: ...
//! ```
//!
//! or, with `log-format json`, as one JSON object per line:
//!
//! ```text
//! {"ts":"2026-10-09T12:34:56.789Z","pid":4242,"level":"warn","msg":"AOF: fsync failed: ..."}
//! ```
//!
//! Lines go to standard error, or to `logfile` when one is set. The levels
//! are Redis's: `debug`, `verbose`, `notice` (also `info`), `warning`, and
//! `nothing`, which here still lets errors through — a server should not
//! fail without saying why. docs/LOGGING.md lists what is logged at each.
//!
//! Use the macros — `log_fatal!`, `log_error!`, `log_warn!`, `log_info!`,
//! `log_verbose!`, `log_debug!` — rather than `eprintln!`. Below the level
//! that is set, a call costs one atomic load: the message is not formatted.

use std::fmt;
use std::io::Write;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};

pub const ERROR: u8 = 0;
pub const WARN: u8 = 1;
pub const INFO: u8 = 2;
pub const VERBOSE: u8 = 3;
pub const DEBUG: u8 = 4;
/// Printed whatever the level; the process is about to exit.
pub const FATAL: u8 = u8::MAX;

/// Runtime log level: one of `ERROR` … `DEBUG`.
pub static LOG_LEVEL: AtomicU8 = AtomicU8::new(INFO);

static JSON: AtomicBool = AtomicBool::new(false);
static LOGFILE: Mutex<Option<std::fs::File>> = Mutex::new(None);

/// Whether a line at `level` would be written.
#[inline]
pub fn enabled(level: u8) -> bool {
    level == FATAL || level <= LOG_LEVEL.load(Ordering::Relaxed)
}

/// Write lines as JSON objects (`log-format json`) or as text.
pub fn set_json(json: bool) {
    JSON.store(json, Ordering::Relaxed);
}

/// Send the log to `path`, appending, instead of standard error. An empty
/// path means standard error.
pub fn set_logfile(path: &str) -> std::io::Result<()> {
    let file = if path.is_empty() {
        None
    } else {
        Some(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)?,
        )
    };
    *LOGFILE.lock().unwrap_or_else(|e| e.into_inner()) = file;
    Ok(())
}

fn tag(level: u8) -> &'static str {
    match level {
        FATAL => "FATAL",
        ERROR => "ERROR",
        WARN => "WARN",
        INFO => "INFO",
        VERBOSE => "VERBOSE",
        _ => "DEBUG",
    }
}

/// Write one line. Called by the macros, after `enabled`.
pub fn write(level: u8, args: fmt::Arguments<'_>) {
    let ts = timestamp(std::time::SystemTime::now());
    let pid = std::process::id();
    let line = if JSON.load(Ordering::Relaxed) {
        let mut msg = String::new();
        let _ = fmt::write(&mut msg, args);
        format!(
            "{{\"ts\":\"{ts}\",\"pid\":{pid},\"level\":\"{}\",\"msg\":\"{}\"}}\n",
            tag(level).to_ascii_lowercase(),
            json_escape(&msg)
        )
    } else {
        format!("{ts} {pid} [{}] {args}\n", tag(level))
    };
    let mut file = LOGFILE.lock().unwrap_or_else(|e| e.into_inner());
    match file.as_mut() {
        Some(f) => {
            let _ = f.write_all(line.as_bytes());
        }
        None => {
            let _ = std::io::stderr().lock().write_all(line.as_bytes());
        }
    }
}

/// For `log_throttled!`: true at most once every `secs` seconds per `last`.
pub fn throttle(last: &AtomicU64, secs: u64) -> bool {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(1, |d| d.as_secs().max(1));
    let prev = last.load(Ordering::Relaxed);
    if prev != 0 && now < prev + secs {
        return false;
    }
    last.compare_exchange(prev, now, Ordering::Relaxed, Ordering::Relaxed)
        .is_ok()
}

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 8);
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

/// `2026-10-09T12:34:56.789Z`, in UTC.
pub fn timestamp(t: std::time::SystemTime) -> String {
    let d = t.duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
    let secs = d.as_secs();
    let (h, m, s) = ((secs / 3600) % 24, (secs / 60) % 60, secs % 60);
    // Howard Hinnant's days-to-civil.
    let z = (secs / 86_400) as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{h:02}:{m:02}:{s:02}.{:03}Z",
        d.subsec_millis()
    )
}

#[macro_export]
macro_rules! log_at {
    ($level:expr, $($arg:tt)*) => {
        if $crate::loglevel::enabled($level) {
            $crate::loglevel::write($level, format_args!($($arg)*));
        }
    };
}

/// Log at `$level` at most once every `$secs` seconds from this call site,
/// for errors that can repeat as fast as the work that hits them (a full
/// disk fails every AOF write; a file-descriptor limit fails every accept).
#[macro_export]
macro_rules! log_throttled {
    ($secs:expr, $level:expr, $($arg:tt)*) => {{
        static LAST: ::std::sync::atomic::AtomicU64 = ::std::sync::atomic::AtomicU64::new(0);
        if $crate::loglevel::enabled($level) && $crate::loglevel::throttle(&LAST, $secs) {
            $crate::loglevel::write($level, format_args!($($arg)*));
        }
    }};
}

/// The process cannot go on; the caller exits after this.
#[macro_export]
macro_rules! log_fatal {
    ($($arg:tt)*) => { $crate::log_at!($crate::loglevel::FATAL, $($arg)*) };
}
/// Something failed and the operator has to act: data not saved, a
/// replica that cannot sync, a file that cannot be read.
#[macro_export]
macro_rules! log_error {
    ($($arg:tt)*) => { $crate::log_at!($crate::loglevel::ERROR, $($arg)*) };
}
/// Something is wrong or about to be, but the server carries on.
#[macro_export]
macro_rules! log_warn {
    ($($arg:tt)*) => { $crate::log_at!($crate::loglevel::WARN, $($arg)*) };
}
/// The server's life story: startup, loading, saving, role changes,
/// configuration changes, shutdown. The default level.
#[macro_export]
macro_rules! log_info {
    ($($arg:tt)*) => { $crate::log_at!($crate::loglevel::INFO, $($arg)*) };
}
/// Per-client events: connections, authentication failures, protocol
/// errors. Useful when chasing a client problem; noisy otherwise.
#[macro_export]
macro_rules! log_verbose {
    ($($arg:tt)*) => { $crate::log_at!($crate::loglevel::VERBOSE, $($arg)*) };
}
/// Internals, for whoever is debugging KiviDB itself.
#[macro_export]
macro_rules! log_debug {
    ($($arg:tt)*) => { $crate::log_at!($crate::loglevel::DEBUG, $($arg)*) };
}

pub mod log {
    use super::{DEBUG, ERROR, INFO, VERBOSE, WARN};

    /// Redis's level names, and KiviDB's older ones.
    pub fn parse_level(s: &str) -> Option<u8> {
        match s.to_ascii_lowercase().as_str() {
            "debug" => Some(DEBUG),
            "verbose" => Some(VERBOSE),
            "notice" | "info" => Some(INFO),
            "warning" | "warn" => Some(WARN),
            "nothing" | "error" => Some(ERROR),
            _ => None,
        }
    }

    /// The level's name, as `CONFIG GET loglevel` gives it: Redis's.
    pub fn level_str(l: u8) -> &'static str {
        match l {
            DEBUG => "debug",
            VERBOSE => "verbose",
            INFO => "notice",
            WARN => "warning",
            _ => "nothing",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_are_utc_iso_8601() {
        let t = std::time::UNIX_EPOCH + std::time::Duration::from_millis(1_791_549_296_789);
        assert_eq!(timestamp(t), "2026-10-09T12:34:56.789Z");
        assert_eq!(timestamp(std::time::UNIX_EPOCH), "1970-01-01T00:00:00.000Z");
        let leap = std::time::UNIX_EPOCH + std::time::Duration::from_secs(951_782_400);
        assert_eq!(timestamp(leap), "2000-02-29T00:00:00.000Z");
    }

    #[test]
    fn levels_round_trip_under_redis_names() {
        for name in ["debug", "verbose", "notice", "warning", "nothing"] {
            assert_eq!(log::level_str(log::parse_level(name).unwrap()), name);
        }
        assert_eq!(log::parse_level("info"), Some(INFO));
        assert_eq!(log::parse_level("warn"), Some(WARN));
        assert_eq!(log::parse_level("error"), Some(ERROR));
        assert_eq!(log::parse_level("shouting"), None);
    }

    #[test]
    fn json_escaping() {
        assert_eq!(json_escape("a\"b\\c\nd\u{1}"), "a\\\"b\\\\c\\nd\\u0001");
    }
}
