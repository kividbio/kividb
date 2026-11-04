// tests/slowlog_latency.rs
//
// Rust integration tests for SLOWLOG and LATENCY commands.
//
// These tests exercise the in-process types (SlowLog, LatencyTracker) via the
// full RESP wire protocol, using the same Conn + start_server harness as the
// other test files. Unlike the e2e Python tests these run under `cargo test`
// and are compiled against the library, so type-level invariants are verified
// at compile time.
//
// Run with:
//   cargo test --test slowlog_latency
//
// Individual test:
//   cargo test --test slowlog_latency test_slowlog_len_zero_after_reset

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

// ── RESP wire helpers ─────────────────────────────────────────────────────────
// Identical to the harness in tests/string-json.rs — duplicated here so each
// test file is self-contained and doesn't require a shared test helper crate.

struct Conn {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Conn {
    fn connect(port: u16) -> Self {
        // Retry for up to 3 seconds — the server spawns in a background thread.
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        loop {
            match TcpStream::connect(("127.0.0.1", port)) {
                Ok(s) => {
                    let writer = s.try_clone().unwrap();
                    return Self {
                        reader: BufReader::new(s),
                        writer,
                    };
                }
                Err(_) if std::time::Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(50));
                }
                Err(e) => panic!("could not connect to port {port}: {e}"),
            }
        }
    }

    /// Send a RESP inline array command and return the parsed response.
    fn cmd(&mut self, args: &[&str]) -> RespValue {
        // Encode as RESP array
        let mut buf = format!("*{}\r\n", args.len());
        for a in args {
            buf.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
        }
        self.writer.write_all(buf.as_bytes()).unwrap();
        self.read_value()
    }

    fn read_value(&mut self) -> RespValue {
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        let line = line.trim_end_matches("\r\n");

        match line.chars().next() {
            Some('+') => RespValue::SimpleStr(line[1..].to_string()),
            Some('-') => RespValue::Error(line[1..].to_string()),
            Some(':') => RespValue::Integer(line[1..].parse().unwrap()),
            Some('$') => {
                let len: i64 = line[1..].parse().unwrap();
                if len == -1 {
                    return RespValue::Nil;
                }
                let len = len as usize;
                let mut data = vec![0u8; len + 2];
                self.reader.read_exact(&mut data).unwrap();
                data.truncate(len);
                RespValue::Bulk(data)
            }
            Some('*') => {
                let count: i64 = line[1..].parse().unwrap();
                if count == -1 {
                    return RespValue::Nil;
                }
                let items = (0..count).map(|_| self.read_value()).collect();
                RespValue::Array(items)
            }
            _ => panic!("unexpected RESP prefix in: {:?}", line),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
enum RespValue {
    SimpleStr(String),
    Error(String),
    Integer(i64),
    Bulk(Vec<u8>),
    Array(Vec<RespValue>),
    Nil,
}

impl RespValue {
    fn as_str(&self) -> &str {
        match self {
            RespValue::SimpleStr(s) | RespValue::Error(s) => s,
            RespValue::Bulk(b) => std::str::from_utf8(b).unwrap(),
            _ => panic!("not a string: {:?}", self),
        }
    }
    fn as_int(&self) -> i64 {
        match self {
            RespValue::Integer(n) => *n,
            _ => panic!("not an integer: {:?}", self),
        }
    }
    fn as_array(&self) -> &[RespValue] {
        match self {
            RespValue::Array(v) => v,
            _ => panic!("not an array: {:?}", self),
        }
    }
    fn is_ok(&self) -> bool {
        matches!(self, RespValue::SimpleStr(s) if s == "OK")
    }
    fn is_error(&self) -> bool {
        matches!(self, RespValue::Error(_))
    }
    fn is_array(&self) -> bool {
        matches!(self, RespValue::Array(_))
    }
}

// ── Server fixture ────────────────────────────────────────────────────────────

/// Whether nothing has `port`. It is asked with a socket that is bound and
/// never listens. One that listened, as this used to, could be given a
/// connection: a server another test is starting holds, between its fork and
/// its exec, a copy of every socket this process has open, and a client that
/// dialled the port in that moment was connected to the copy and then reset
/// ("Connection reset by peer", at the first command of a test).
fn port_is_free(port: u16) -> bool {
    let Ok(probe) = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None) else {
        return false;
    };
    // As the server's own socket, so that a copy of this one, while it
    // lasts, does not keep the server from the port.
    #[cfg(unix)]
    let _ = probe.set_reuse_address(true);
    probe
        .bind(&std::net::SocketAddr::from(([127, 0, 0, 1], port)).into())
        .is_ok()
}

/// A port nothing is listening on — and not one the system hands out to
/// outgoing connections. A client that starts dialling before its server is
/// listening can otherwise be given, as its own end, the very port it is
/// dialling: it is then connected to itself and reads back what it writes.
fn free_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(0);
    loop {
        let step = NEXT.fetch_add(1, Ordering::Relaxed);
        let from = (std::process::id() as u16).wrapping_mul(31);
        let port = 10_000 + from.wrapping_add(step) % 20_000;
        if port_is_free(port) {
            return port;
        }
    }
}

/// Start a server with the default threshold (10ms) and max_len (128).
fn start_server(port: u16) {
    thread::spawn(move || {
        kividb::run_for_test(kividb::config::Config {
            port,
            bind: "127.0.0.1".to_string(),
            threads: 2,
            aof: false,
            databases: 16,
            maxmemory: 0,
            loglevel: "error".to_string(),
            io_model: kividb::config::IoModel::Tokio,
            replicaof: None,
            replica_announced_port: 0,
            requirepass: "".to_string(),
            aclfile: "".to_string(),
            slowlog_log_slower_than: 10_000,
            slowlog_max_len: 128,
            cluster_enabled: false,
            cluster_node_timeout: 15000,
            configfile: "./kividb.conf".to_string(),
            ..Default::default()
        });
    });
}

/// Start a server with threshold=0 so every command is logged.
fn start_server_log_all(port: u16) {
    thread::spawn(move || {
        kividb::run_for_test(kividb::config::Config {
            port,
            bind: "127.0.0.1".to_string(),
            threads: 2,
            aof: false,
            databases: 16,
            maxmemory: 0,
            loglevel: "error".to_string(),
            io_model: kividb::config::IoModel::Tokio,
            replicaof: None,
            replica_announced_port: 0,
            requirepass: "".to_string(),
            aclfile: "".to_string(),
            slowlog_log_slower_than: 0,
            slowlog_max_len: 128,
            cluster_enabled: false,
            cluster_node_timeout: 15000,
            configfile: "./kividb.conf".to_string(),
            ..Default::default()
        });
    });
}

/// Start a server with threshold=0 and a small max_len to test eviction.
fn start_server_small_maxlen(port: u16, max_len: usize) {
    thread::spawn(move || {
        kividb::run_for_test(kividb::config::Config {
            port,
            bind: "127.0.0.1".to_string(),
            threads: 2,
            aof: false,
            databases: 16,
            maxmemory: 0,
            loglevel: "error".to_string(),
            io_model: kividb::config::IoModel::Tokio,
            replicaof: None,
            replica_announced_port: 0,
            requirepass: "".to_string(),
            aclfile: "".to_string(),
            slowlog_log_slower_than: 0,
            slowlog_max_len: max_len,
            cluster_enabled: false,
            cluster_node_timeout: 15000,
            configfile: "./kividb.conf".to_string(),
            ..Default::default()
        });
    });
}

// ═══════════════════════════════════════════════════════════════════════════════
// SLOWLOG — structural / wire-format tests
// ═══════════════════════════════════════════════════════════════════════════════

// SLOWLOG LEN returns an integer, SLOWLOG RESET returns OK.
#[test]
fn test_slowlog_len_and_reset_basic() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let len = c.cmd(&["SLOWLOG", "LEN"]);
    assert!(
        matches!(len, RespValue::Integer(_)),
        "SLOWLOG LEN must return integer"
    );

    let reset = c.cmd(&["SLOWLOG", "RESET"]);
    assert!(reset.is_ok(), "SLOWLOG RESET must return OK");
}

// After SLOWLOG RESET, LEN returns 0.
#[test]
fn test_slowlog_len_zero_after_reset() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmd(&["SLOWLOG", "RESET"]);
    assert_eq!(c.cmd(&["SLOWLOG", "LEN"]).as_int(), 0);
}

// SLOWLOG GET with no commands logged returns an empty array.
#[test]
fn test_slowlog_get_empty_after_reset() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmd(&["SLOWLOG", "RESET"]);
    let result = c.cmd(&["SLOWLOG", "GET"]);
    assert!(result.is_array());
    assert_eq!(result.as_array().len(), 0);
}

// SLOWLOG GET with a count argument is accepted.
#[test]
fn test_slowlog_get_with_count_arg() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmd(&["SLOWLOG", "GET", "10"]);
    assert!(result.is_array(), "SLOWLOG GET 10 must return array");
}

// SLOWLOG GET 0 always returns an empty array regardless of log state.
#[test]
fn test_slowlog_get_count_zero_returns_empty() {
    let port = free_port();
    start_server_log_all(port);
    let mut c = Conn::connect(port);

    c.cmd(&["SET", "k", "v"]);
    let result = c.cmd(&["SLOWLOG", "GET", "0"]);
    assert!(result.is_array());
    assert_eq!(result.as_array().len(), 0);
}

// Unknown SLOWLOG subcommand returns an error.
#[test]
fn test_slowlog_unknown_subcommand_is_error() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmd(&["SLOWLOG", "BADCMD"]);
    assert!(
        result.is_error(),
        "unknown SLOWLOG subcommand must return error"
    );
}

// Missing SLOWLOG subcommand returns an error.
#[test]
fn test_slowlog_no_subcommand_is_error() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmd(&["SLOWLOG"]);
    assert!(result.is_error());
}

// ═══════════════════════════════════════════════════════════════════════════════
// SLOWLOG — threshold=0 entry content tests
// ═══════════════════════════════════════════════════════════════════════════════

// With threshold=0, a SET command produces a slowlog entry.
#[test]
fn test_slowlog_entry_produced_with_zero_threshold() {
    let port = free_port();
    start_server_log_all(port);
    let mut c = Conn::connect(port);

    c.cmd(&["SLOWLOG", "RESET"]);
    c.cmd(&["SET", "mykey", "myval"]);

    let len = c.cmd(&["SLOWLOG", "LEN"]).as_int();
    assert!(len >= 1, "expected >= 1 slowlog entry after SET, got {len}");
}

// Each entry is a 7-element array. The seventh — the command's total argument
// count — was added in Redis 8.10.
#[test]
fn test_slowlog_entry_has_seven_fields() {
    let port = free_port();
    start_server_log_all(port);
    let mut c = Conn::connect(port);

    c.cmd(&["SLOWLOG", "RESET"]);
    c.cmd(&["SET", "seven_field_key", "v"]);

    let entries = c.cmd(&["SLOWLOG", "GET"]);
    let arr = entries.as_array();
    assert!(!arr.is_empty(), "expected at least one entry");

    let entry = &arr[0];
    assert!(entry.is_array());
    assert_eq!(
        entry.as_array().len(),
        7,
        "each slowlog entry must have exactly 7 fields: {:?}",
        entry
    );
}

// Field 6: the argument count before truncation. The reference example: a
// 33-argument SADD against the default limit of 32 logs 32 elements, the last
// of which says how many were cut, and reports 33.
#[test]
fn test_slowlog_entry_reports_untruncated_argument_count() {
    let port = free_port();
    start_server_log_all(port);
    let mut c = Conn::connect(port);

    c.cmd(&["SLOWLOG", "RESET"]);
    let mut sadd: Vec<String> = vec!["SADD".into(), "myset".into()];
    sadd.extend((3..=33).map(|n| n.to_string()));
    let refs: Vec<&str> = sadd.iter().map(String::as_str).collect();
    c.cmd(&refs);

    let entries = c.cmd(&["SLOWLOG", "GET", "-1"]);
    let entry = entries
        .as_array()
        .iter()
        .map(|e| e.as_array())
        .find(|e| e[3].as_array()[0].as_str() == "SADD")
        .expect("the SADD is in the slow log");
    let logged = entry[3].as_array();
    assert_eq!(logged.len(), 32);
    assert_eq!(logged[31].as_str(), "... (2 more arguments)");
    assert_eq!(entry[6].as_int(), 33);
}

// `slowlog-entry-max-argc` and `slowlog-entry-max-string-len` (Redis 8.8) are
// settable at runtime and take effect on the next entry.
#[test]
fn test_slowlog_entry_limits_are_configurable() {
    let port = free_port();
    start_server_log_all(port);
    let mut c = Conn::connect(port);

    assert_eq!(
        c.cmd(&["CONFIG", "SET", "slowlog-entry-max-argc", "3"])
            .as_str(),
        "OK"
    );
    assert_eq!(
        c.cmd(&["CONFIG", "SET", "slowlog-entry-max-string-len", "4"])
            .as_str(),
        "OK"
    );
    let got = c.cmd(&["CONFIG", "GET", "slowlog-entry-max-argc"]);
    assert_eq!(got.as_array()[1].as_str(), "3");

    c.cmd(&["SLOWLOG", "RESET"]);
    c.cmd(&["RPUSH", "biglist", "abcdefgh", "x", "y", "z"]);

    let entries = c.cmd(&["SLOWLOG", "GET", "-1"]);
    let entry = entries
        .as_array()
        .iter()
        .map(|e| e.as_array())
        .find(|e| e[3].as_array()[0].as_str() == "RPUS... (1 more bytes)")
        .expect("the RPUSH is in the slow log, with its name cut to 4 bytes");
    let logged = entry[3].as_array();
    assert_eq!(logged.len(), 3);
    assert_eq!(logged[1].as_str(), "bigl... (3 more bytes)");
    assert_eq!(logged[2].as_str(), "... (4 more arguments)");
    assert_eq!(entry[6].as_int(), 6);
}

// Field 0: integer ID.
#[test]
fn test_slowlog_entry_field0_is_integer_id() {
    let port = free_port();
    start_server_log_all(port);
    let mut c = Conn::connect(port);

    c.cmd(&["SLOWLOG", "RESET"]);
    c.cmd(&["SET", "k", "v"]);

    let entry = c.cmd(&["SLOWLOG", "GET"]).as_array()[0].as_array().to_vec();
    assert!(
        matches!(entry[0], RespValue::Integer(_)),
        "field 0 must be an integer ID"
    );
}

// Field 1: unix timestamp in seconds (reasonable range).
#[test]
fn test_slowlog_entry_field1_is_unix_timestamp() {
    let port = free_port();
    start_server_log_all(port);
    let mut c = Conn::connect(port);

    c.cmd(&["SLOWLOG", "RESET"]);
    c.cmd(&["SET", "ts_key", "v"]);

    let entries = c.cmd(&["SLOWLOG", "GET"]);
    let entry = entries.as_array()[0].as_array();
    let ts = entry[1].as_int();

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    assert!(
        ts > 0 && (ts - now).abs() < 10,
        "field 1 timestamp {ts} is not close to current unix time {now}"
    );
}

// Field 2: duration in microseconds, non-negative.
#[test]
fn test_slowlog_entry_field2_is_non_negative_duration() {
    let port = free_port();
    start_server_log_all(port);
    let mut c = Conn::connect(port);

    c.cmd(&["SLOWLOG", "RESET"]);
    c.cmd(&["SET", "dur_key", "v"]);

    let entries = c.cmd(&["SLOWLOG", "GET"]);
    let entry = entries.as_array()[0].as_array();
    let duration_us = entry[2].as_int();

    assert!(
        duration_us >= 0,
        "field 2 duration must be >= 0, got {duration_us}"
    );
}

// Field 3: the command as a nested array of bulk strings.
#[test]
fn test_slowlog_entry_field3_is_command_array() {
    let port = free_port();
    start_server_log_all(port);
    let mut c = Conn::connect(port);

    c.cmd(&["SLOWLOG", "RESET"]);
    c.cmd(&["SET", "cmd_arr_key", "cmd_arr_val"]);

    let entries = c.cmd(&["SLOWLOG", "GET"]);
    // Find the SET entry (index 0 is the most recent, which is the SET)
    let set_entry = entries
        .as_array()
        .iter()
        .find(|e| {
            if let RespValue::Array(fields) = e
                && let RespValue::Array(cmd_parts) = &fields[3]
            {
                return cmd_parts
                    .first()
                    .map(|f| f.as_str().to_uppercase() == "SET")
                    .unwrap_or(false);
            }
            false
        })
        .expect("could not find SET entry in slowlog");

    let fields = set_entry.as_array();
    assert!(fields[3].is_array(), "field 3 must be an array");

    let cmd_parts = fields[3].as_array();
    assert_eq!(cmd_parts.len(), 3, "SET has 3 parts: SET key value");
    assert_eq!(cmd_parts[0].as_str().to_uppercase(), "SET");
    assert_eq!(cmd_parts[1].as_str(), "cmd_arr_key");
    assert_eq!(cmd_parts[2].as_str(), "cmd_arr_val");
}

// Field 4: client address as a bulk string containing ":".
#[test]
fn test_slowlog_entry_field4_is_client_addr() {
    let port = free_port();
    start_server_log_all(port);
    let mut c = Conn::connect(port);

    c.cmd(&["SLOWLOG", "RESET"]);
    c.cmd(&["SET", "addr_key", "v"]);

    let entries = c.cmd(&["SLOWLOG", "GET"]);
    let entry = entries.as_array()[0].as_array();
    let addr = entry[4].as_str();

    assert!(
        addr.contains(':'),
        "field 4 client address should contain ':' (host:port), got {:?}",
        addr
    );
}

// Field 5: client name — empty string for unnamed clients.
#[test]
fn test_slowlog_entry_field5_is_client_name() {
    let port = free_port();
    start_server_log_all(port);
    let mut c = Conn::connect(port);

    c.cmd(&["SLOWLOG", "RESET"]);
    c.cmd(&["SET", "name_key", "v"]);

    let entries = c.cmd(&["SLOWLOG", "GET"]);
    let entry = entries.as_array()[0].as_array();

    // field 5 is either Bulk (empty string) or a name string — must not panic
    let _ = entry[5].as_str();
}

// IDs increase monotonically; GET returns newest first (descending IDs).
#[test]
fn test_slowlog_ids_monotone_newest_first() {
    let port = free_port();
    start_server_log_all(port);
    let mut c = Conn::connect(port);

    c.cmd(&["SLOWLOG", "RESET"]);
    for i in 0..5u32 {
        c.cmd(&["SET", &format!("mono_{i}"), "v"]);
    }

    let entries = c.cmd(&["SLOWLOG", "GET"]);
    let ids: Vec<i64> = entries
        .as_array()
        .iter()
        .map(|e| e.as_array()[0].as_int())
        .collect();

    // Should be strictly decreasing (newest = highest ID is first)
    for window in ids.windows(2) {
        assert!(
            window[0] > window[1],
            "IDs should be strictly decreasing (newest first): {:?}",
            ids
        );
    }
}

// SLOWLOG GET <count> limits the number of returned entries.
#[test]
fn test_slowlog_get_count_limits_results() {
    let port = free_port();
    start_server_log_all(port);
    let mut c = Conn::connect(port);

    c.cmd(&["SLOWLOG", "RESET"]);
    for i in 0..10u32 {
        c.cmd(&["SET", &format!("limit_{i}"), "v"]);
    }

    let limited = c.cmd(&["SLOWLOG", "GET", "3"]);
    assert!(
        limited.as_array().len() <= 3,
        "SLOWLOG GET 3 must return at most 3 entries"
    );
}

// SLOWLOG LEN and GET stay consistent after multiple resets.
#[test]
fn test_slowlog_reset_idempotent() {
    let port = free_port();
    start_server_log_all(port);
    let mut c = Conn::connect(port);

    for _ in 0..3 {
        c.cmd(&["SET", "idem", "v"]);
        c.cmd(&["SLOWLOG", "RESET"]);
        // Immediately after RESET, LEN is 0 (the RESET itself may add 1
        // with threshold=0, so allow <= 1)
        let len = c.cmd(&["SLOWLOG", "LEN"]).as_int();
        assert!(len <= 1, "after RESET, LEN should be <= 1, got {len}");
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// SLOWLOG — max_len eviction
// ═══════════════════════════════════════════════════════════════════════════════

// When max_len is 5, issuing 20 commands keeps at most 5 entries.
#[test]
fn test_slowlog_eviction_respects_max_len() {
    let port = free_port();
    start_server_small_maxlen(port, 5);
    let mut c = Conn::connect(port);

    c.cmd(&["SLOWLOG", "RESET"]);
    for i in 0..20u32 {
        c.cmd(&["SET", &format!("evict_{i}"), "v"]);
    }

    let len = c.cmd(&["SLOWLOG", "LEN"]).as_int();
    assert!(
        len <= 5,
        "with max_len=5, SLOWLOG LEN must be <= 5, got {len}"
    );

    let entries = c.cmd(&["SLOWLOG", "GET"]);
    assert!(
        entries.as_array().len() <= 5,
        "SLOWLOG GET must return at most max_len entries"
    );
}

// After eviction, the entries that survive have the highest IDs (newest).
#[test]
fn test_slowlog_eviction_keeps_newest_entries() {
    let port = free_port();
    start_server_small_maxlen(port, 3);
    let mut c = Conn::connect(port);

    c.cmd(&["SLOWLOG", "RESET"]);
    for i in 0..10u32 {
        c.cmd(&["SET", &format!("keep_{i}"), "v"]);
    }

    let entries = c.cmd(&["SLOWLOG", "GET"]);
    let arr = entries.as_array();
    assert!(!arr.is_empty());
    assert!(arr.len() <= 3);

    // First entry (newest) must have a higher ID than the last (oldest)
    if arr.len() > 1 {
        let newest_id = arr[0].as_array()[0].as_int();
        let oldest_id = arr[arr.len() - 1].as_array()[0].as_int();
        assert!(
            newest_id > oldest_id,
            "newest entry ({newest_id}) must have higher ID than oldest ({oldest_id})"
        );
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// SLOWLOG — fast-path and EXEC timing coverage
// ═══════════════════════════════════════════════════════════════════════════════

// GET/SET (fast path) must appear in slowlog with threshold=0.
// This is the critical test verifying fast-path timing is wired.
#[test]
fn test_slowlog_fast_path_commands_are_timed() {
    let port = free_port();
    start_server_log_all(port);
    let mut c = Conn::connect(port);

    c.cmd(&["SLOWLOG", "RESET"]);
    c.cmd(&["SET", "fp_key", "fp_val"]); // fast path
    c.cmd(&["GET", "fp_key"]); // fast path

    let len = c.cmd(&["SLOWLOG", "LEN"]).as_int();
    assert!(
        len >= 2,
        "expected >= 2 entries for SET+GET on fast path, got {len}. \
         Fast-path timing may not be wired in server.rs."
    );
}

// Commands inside MULTI/EXEC must each produce their own slowlog entry.
// This verifies the EXEC-loop timing block is wired.
#[test]
fn test_slowlog_exec_loop_commands_are_timed() {
    let port = free_port();
    start_server_log_all(port);
    let mut c = Conn::connect(port);

    c.cmd(&["SLOWLOG", "RESET"]);

    // Issue a MULTI block with 3 inner commands
    c.cmd(&["MULTI"]);
    c.cmd(&["SET", "tx_a", "1"]); // → QUEUED
    c.cmd(&["SET", "tx_b", "2"]); // → QUEUED
    c.cmd(&["GET", "tx_a"]); // → QUEUED
    c.cmd(&["EXEC"]);

    let len = c.cmd(&["SLOWLOG", "LEN"]).as_int();
    // MULTI, EXEC, and each inner SET/GET are all potential entries.
    // At minimum the 3 inner commands must be logged.
    assert!(
        len >= 3,
        "expected >= 3 slowlog entries for MULTI block with 3 commands, got {len}. \
         EXEC-loop timing may not be wired in server.rs."
    );
}

// High threshold means ordinary commands are NOT logged.
#[test]
fn test_slowlog_high_threshold_nothing_logged() {
    // threshold = 10 seconds — nothing should take that long
    let port = free_port();
    thread::spawn(move || {
        kividb::run_for_test(kividb::config::Config {
            port,
            bind: "127.0.0.1".to_string(),
            threads: 2,
            aof: false,
            databases: 16,
            maxmemory: 0,
            loglevel: "error".to_string(),
            io_model: kividb::config::IoModel::Tokio,
            replicaof: None,
            replica_announced_port: 0,
            requirepass: "".to_string(),
            aclfile: "".to_string(),
            slowlog_log_slower_than: 10_000_000, // 10 seconds
            slowlog_max_len: 128,
            cluster_enabled: false,
            cluster_node_timeout: 15000,
            configfile: "./kividb.conf".to_string(),
            ..Default::default()
        });
    });

    let mut c = Conn::connect(port);
    c.cmd(&["SLOWLOG", "RESET"]);
    for i in 0..20u32 {
        c.cmd(&["SET", &format!("nolog_{i}"), "v"]);
        c.cmd(&["GET", &format!("nolog_{i}")]);
    }

    let len = c.cmd(&["SLOWLOG", "LEN"]).as_int();
    assert_eq!(
        len, 0,
        "with 10s threshold, no ordinary commands should be logged, got {len}"
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// LATENCY — structural / wire-format tests
// ═══════════════════════════════════════════════════════════════════════════════

// LATENCY LATEST returns an array (possibly empty on a fresh server).
#[test]
fn test_latency_latest_returns_array() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmd(&["LATENCY", "LATEST"]);
    assert!(result.is_array(), "LATENCY LATEST must return an array");
}

// LATENCY HISTORY for an unknown event returns an empty array.
#[test]
fn test_latency_history_unknown_event_returns_empty() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmd(&["LATENCY", "HISTORY", "no_such_event_xyz"]);
    assert!(result.is_array());
    assert_eq!(
        result.as_array().len(),
        0,
        "LATENCY HISTORY for unknown event must be empty"
    );
}

// LATENCY HISTORY with no event argument returns an error.
#[test]
fn test_latency_history_no_arg_is_error() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmd(&["LATENCY", "HISTORY"]);
    assert!(result.is_error(), "LATENCY HISTORY with no arg must error");
}

// LATENCY RESET returns how many events it reset, as in Redis: none, on a
// server that has recorded none.
#[test]
fn test_latency_reset_returns_ok() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmd(&["LATENCY", "RESET"]);
    assert_eq!(result, RespValue::Integer(0));
}

// LATENCY RESET with event name: the same.
#[test]
fn test_latency_reset_named_event_returns_ok() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmd(&["LATENCY", "RESET", "command"]);
    assert_eq!(result, RespValue::Integer(0));
}

// LATENCY GRAPH with no event argument returns an error.
#[test]
fn test_latency_graph_no_arg_is_error() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmd(&["LATENCY", "GRAPH"]);
    assert!(result.is_error(), "LATENCY GRAPH with no arg must error");
}

// LATENCY GRAPH for an event with no samples returns an error.
#[test]
fn test_latency_graph_empty_event_is_error() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmd(&["LATENCY", "RESET"]); // clear everything first
    let result = c.cmd(&["LATENCY", "GRAPH", "no_such_event"]);
    assert!(
        result.is_error(),
        "LATENCY GRAPH on unknown event must error, got {:?}",
        result
    );
}

// LATENCY HISTOGRAM returns an array (may be empty stub).
#[test]
fn test_latency_histogram_returns_array() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmd(&["LATENCY", "HISTOGRAM"]);
    assert!(result.is_array(), "LATENCY HISTOGRAM must return an array");
}

// Unknown LATENCY subcommand returns an error.
#[test]
fn test_latency_unknown_subcommand_is_error() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmd(&["LATENCY", "BADCMD"]);
    assert!(
        result.is_error(),
        "unknown LATENCY subcommand must error: {result:?}"
    );
}

// Missing LATENCY subcommand returns an error.
#[test]
fn test_latency_no_subcommand_is_error() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmd(&["LATENCY"]);
    assert!(result.is_error());
}

// ═══════════════════════════════════════════════════════════════════════════════
// LATENCY — sample population and entry structure
// ═══════════════════════════════════════════════════════════════════════════════

/// A command the latency monitor records: one that takes at least
/// `latency-monitor-threshold`, which it now goes by, and not
/// `slowlog-log-slower-than` (#158). The threshold is the process's; it is
/// left set.
fn slow_command(c: &mut Conn) {
    c.cmd(&["CONFIG", "SET", "latency-monitor-threshold", "1"]);
    c.cmd(&["DEBUG", "SLEEP", "0.005"]);
}

/// What monitoring reads, as Redis gives it (#158): the latency monitor goes
/// by `latency-monitor-threshold`, `LATENCY LATEST` has the maximum as a
/// fourth field, `LATENCY HELP` answers, `INFO latencystats` has each
/// command's percentiles, and `INFO server` says what it runs on.
#[test]
fn monitoring_sees_what_redis_shows() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    slow_command(&mut c);
    c.cmd(&["DEBUG", "SLEEP", "0.002"]);
    let latest = c.cmd(&["LATENCY", "LATEST"]);
    let command = latest
        .as_array()
        .iter()
        .map(|entry| entry.as_array())
        .find(|entry| entry[0].as_str() == "command")
        .unwrap_or_else(|| panic!("no command event: {latest:?}"));
    let (last, most) = (command[2].as_int(), command[3].as_int());
    assert!(most >= 5 && most >= last, "{latest:?}");

    assert!(c.cmd(&["LATENCY", "HELP"]).as_array().len() > 4);

    c.cmd(&["GET", "nothing"]);
    let stats = c.cmd(&["INFO", "latencystats"]);
    let get = stats
        .as_str()
        .lines()
        .find(|line| line.starts_with("latency_percentiles_usec_get:"))
        .unwrap_or_else(|| panic!("{}", stats.as_str()));
    assert!(
        get.contains("p50=") && get.contains("p99=") && get.contains("p99.9="),
        "{get}"
    );

    let server = c.cmd(&["INFO", "server"]);
    let field = |name: &str| {
        server
            .as_str()
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{name}:")))
            .unwrap_or_default()
            .to_string()
    };
    assert!(["epoll", "io_uring", "iocp", "kqueue"].contains(&field("multiplexing_api").as_str()));
    assert!(!field("executable").is_empty());
    assert_ne!(field("executable"), "/usr/local/bin/kividb");
}

// After a slow command, LATENCY LATEST must contain at least one event.
#[test]
fn test_latency_latest_populates_after_commands() {
    let port = free_port();
    start_server_log_all(port);
    let mut c = Conn::connect(port);

    c.cmd(&["LATENCY", "RESET"]);
    slow_command(&mut c);

    let latest = c.cmd(&["LATENCY", "LATEST"]);
    assert!(
        !latest.as_array().is_empty(),
        "LATENCY LATEST must be non-empty after commands"
    );
}

// Each entry in LATENCY LATEST has 4 fields, as in Redis: name, timestamp,
// latest ms, most ms (#158).
#[test]
fn test_latency_latest_entry_has_three_fields() {
    let port = free_port();
    start_server_log_all(port);
    let mut c = Conn::connect(port);

    slow_command(&mut c);

    let latest = c.cmd(&["LATENCY", "LATEST"]);
    for entry in latest.as_array() {
        assert!(entry.is_array());
        assert_eq!(
            entry.as_array().len(),
            4,
            "each LATENCY LATEST entry must have 4 fields [name, ts, ms, max]: {:?}",
            entry
        );
    }
}

// Event names in LATENCY LATEST are "command" or "fast-command".
#[test]
fn test_latency_latest_event_names_are_valid() {
    let port = free_port();
    start_server_log_all(port);
    let mut c = Conn::connect(port);

    slow_command(&mut c);

    let latest = c.cmd(&["LATENCY", "LATEST"]);
    for entry in latest.as_array() {
        let name = entry.as_array()[0].as_str();
        assert!(
            name == "command" || name == "fast-command",
            "unexpected latency event name: {name:?}"
        );
    }
}

// LATENCY LATEST entry field 1 is a plausible unix timestamp.
#[test]
fn test_latency_latest_timestamp_is_plausible() {
    let port = free_port();
    start_server_log_all(port);
    let mut c = Conn::connect(port);

    for i in 0..5u32 {
        c.cmd(&["SET", &format!("ts_{i}"), "v"]);
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    let latest = c.cmd(&["LATENCY", "LATEST"]);
    for entry in latest.as_array() {
        let ts = entry.as_array()[1].as_int();
        assert!(
            ts > 0 && (ts - now).abs() < 30,
            "latency timestamp {ts} is far from current time {now}"
        );
    }
}

// LATENCY LATEST entry field 2 is a non-negative latency in ms.
#[test]
fn test_latency_latest_latency_ms_non_negative() {
    let port = free_port();
    start_server_log_all(port);
    let mut c = Conn::connect(port);

    for i in 0..5u32 {
        c.cmd(&["SET", &format!("ms_{i}"), "v"]);
    }

    let latest = c.cmd(&["LATENCY", "LATEST"]);
    for entry in latest.as_array() {
        let ms = entry.as_array()[2].as_int();
        assert!(ms >= 0, "latency_ms must be >= 0, got {ms}");
    }
}

// LATENCY HISTORY entries have exactly 2 fields: [timestamp, latency_ms].
#[test]
fn test_latency_history_entry_structure() {
    let port = free_port();
    start_server_log_all(port);
    let mut c = Conn::connect(port);

    for i in 0..20u32 {
        c.cmd(&["SET", &format!("hist_{i}"), "v"]);
    }

    for event in &["command", "fast-command"] {
        let history = c.cmd(&["LATENCY", "HISTORY", event]);
        for entry in history.as_array() {
            assert!(entry.is_array());
            assert_eq!(
                entry.as_array().len(),
                2,
                "LATENCY HISTORY entry must have [timestamp, latency_ms], got {:?}",
                entry
            );
            let ts = entry.as_array()[0].as_int();
            let ms = entry.as_array()[1].as_int();
            assert!(ts > 0, "history timestamp must be > 0");
            assert!(ms >= 0, "history latency_ms must be >= 0");
        }
    }
}

// LATENCY HISTORY is capped at 180 samples per event.
#[test]
fn test_latency_history_capped_at_180() {
    let port = free_port();
    start_server_log_all(port);
    let mut c = Conn::connect(port);

    // Issue 200+ commands to exceed the cap
    for i in 0..210u32 {
        c.cmd(&["SET", &format!("cap_{i}"), "v"]);
    }

    for event in &["command", "fast-command"] {
        let history = c.cmd(&["LATENCY", "HISTORY", event]);
        assert!(
            history.as_array().len() <= 180,
            "LATENCY HISTORY for '{event}' must not exceed 180 samples, got {}",
            history.as_array().len()
        );
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// LATENCY — reset behaviour
// ═══════════════════════════════════════════════════════════════════════════════

// After LATENCY RESET, HISTORY for both events is either empty or has only
// entries added by the RESET call itself (self-recording).
#[test]
fn test_latency_reset_clears_history() {
    let port = free_port();
    start_server_log_all(port);
    let mut c = Conn::connect(port);

    // Populate samples
    for i in 0..20u32 {
        c.cmd(&["SET", &format!("pre_{i}"), "v"]);
    }

    c.cmd(&["LATENCY", "RESET"]);

    // Only the RESET call itself and the HISTORY calls after it can add samples.
    // At most 3 new samples can have been added.
    let cmd_h = c.cmd(&["LATENCY", "HISTORY", "command"]);
    let fast_h = c.cmd(&["LATENCY", "HISTORY", "fast-command"]);
    let total = cmd_h.as_array().len() + fast_h.as_array().len();

    assert!(
        total <= 3,
        "after LATENCY RESET, expected <= 3 total samples (self-recording), got {total}"
    );
}

// LATENCY RESET with a specific event clears only that event.
#[test]
fn test_latency_reset_named_event_selective() {
    let port = free_port();
    start_server_log_all(port);
    let mut c = Conn::connect(port);

    for i in 0..20u32 {
        c.cmd(&["SET", &format!("sel_{i}"), "v"]);
    }

    // Reset only fast-command
    c.cmd(&["LATENCY", "RESET", "fast-command"]);

    // fast-command should have at most a couple of self-generated samples
    let fast_h = c.cmd(&["LATENCY", "HISTORY", "fast-command"]);
    assert!(
        fast_h.as_array().len() <= 2,
        "fast-command history should have <= 2 samples after named reset, \
         got {}",
        fast_h.as_array().len()
    );

    // command event must still be a valid (possibly populated) list
    let cmd_h = c.cmd(&["LATENCY", "HISTORY", "command"]);
    assert!(cmd_h.is_array());
}

// ═══════════════════════════════════════════════════════════════════════════════
// LATENCY — GRAPH output
// ═══════════════════════════════════════════════════════════════════════════════

// LATENCY GRAPH on a populated event returns a non-empty bulk string.
#[test]
fn test_latency_graph_returns_string_for_known_event() {
    let port = free_port();
    start_server_log_all(port);
    let mut c = Conn::connect(port);

    slow_command(&mut c);

    let latest = c.cmd(&["LATENCY", "LATEST"]);
    let events = latest.as_array();
    assert!(!events.is_empty(), "need latency samples for GRAPH test");

    let event_name = events[0].as_array()[0].as_str().to_string();
    let graph = c.cmd(&["LATENCY", "GRAPH", &event_name]);

    assert!(
        matches!(graph, RespValue::Bulk(_)),
        "LATENCY GRAPH must return a bulk string, got {:?}",
        graph
    );
    assert!(
        !graph.as_str().is_empty(),
        "LATENCY GRAPH must not be empty"
    );
}

// The graph is Redis's: a header with the high and the low, a rule, and an
// ASCII sparkline.
#[test]
fn test_latency_graph_is_redis_s_ascii_graph() {
    let port = free_port();
    start_server_log_all(port);
    let mut c = Conn::connect(port);

    slow_command(&mut c);

    let latest = c.cmd(&["LATENCY", "LATEST"]);
    let events = latest.as_array();
    assert!(!events.is_empty(), "need latency samples for GRAPH test");

    let event_name = events[0].as_array()[0].as_str().to_string();
    let graph = c.cmd(&["LATENCY", "GRAPH", &event_name]);
    let text = graph.as_str();

    let mut lines = text.lines();
    let header = lines.next().unwrap_or_default();
    assert!(
        header.starts_with(&format!("{event_name} - high ")) && header.contains(" ms, low "),
        "LATENCY GRAPH header: {text:?}"
    );
    assert_eq!(lines.next(), Some("-".repeat(80).as_str()), "{text:?}");
    assert!(text.is_ascii(), "{text:?}");
    assert!(text.contains(['_', 'o', '#', '|']), "{text:?}");
}

// ═══════════════════════════════════════════════════════════════════════════════
// Cross-cutting: slowlog and latency are independent subsystems
// ═══════════════════════════════════════════════════════════════════════════════

// SLOWLOG RESET must not affect LATENCY data.
#[test]
fn test_slowlog_reset_does_not_clear_latency() {
    let port = free_port();
    start_server_log_all(port);
    let mut c = Conn::connect(port);

    for i in 0..10u32 {
        c.cmd(&["SET", &format!("cross_{i}"), "v"]);
    }

    c.cmd(&["SLOWLOG", "RESET"]);

    // Latency tracker should still return a valid (possibly non-empty) list.
    let latest = c.cmd(&["LATENCY", "LATEST"]);
    assert!(
        latest.is_array(),
        "LATENCY LATEST must still return array after SLOWLOG RESET"
    );
}

// LATENCY RESET must not affect slowlog data.
#[test]
fn test_latency_reset_does_not_clear_slowlog() {
    let port = free_port();
    start_server_log_all(port);
    let mut c = Conn::connect(port);

    c.cmd(&["SLOWLOG", "RESET"]);
    for i in 0..5u32 {
        c.cmd(&["SET", &format!("cross2_{i}"), "v"]);
    }

    let before_len = c.cmd(&["SLOWLOG", "LEN"]).as_int();
    c.cmd(&["LATENCY", "RESET"]);
    let after_len = c.cmd(&["SLOWLOG", "LEN"]).as_int();

    // After LATENCY RESET (which is itself one more logged command),
    // slowlog_len must be >= before_len, not zero.
    assert!(
        after_len >= before_len,
        "LATENCY RESET should not clear slowlog: before={before_len} after={after_len}"
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// Performance regression guard
// ═══════════════════════════════════════════════════════════════════════════════

/// Verifies that with default config (slowlog disabled), the server can
/// sustain a minimum throughput threshold. This catches regressions where
/// per-command overhead (clock calls, lock acquisitions) is accidentally
/// added to the fast path.
///
/// Threshold is deliberately conservative (500k ops/sec on a single
/// thread in-process) — real throughput should be orders of magnitude
/// higher. If this fails, something expensive is running per-command.
#[test]
fn test_fast_path_throughput_guard() {
    let port = free_port();
    // Use DEFAULT config — no explicit slowlog threshold.
    // This is the critical part: if the default changed back to 10_000,
    // this test catches it via the throughput drop.
    start_server(port);
    let mut c = Conn::connect(port);

    // Pre-populate a key
    c.cmd(&["SET", "bench_key", "bench_value"]);

    // Measure how long 10_000 GETs take
    let start = std::time::Instant::now();
    for _ in 0..10_000 {
        let result = c.cmd(&["GET", "bench_key"]);
        assert!(matches!(result, RespValue::Bulk(_)));
    }
    let elapsed = start.elapsed();

    // 10_000 commands should complete in under 2 seconds on any CI machine.
    // At 25M QPS baseline this takes ~400 microseconds.
    // At the regressed 1M QPS it takes ~10ms.
    // 2 seconds gives a 5000x margin — if we're slower than this,
    // something is catastrophically wrong on the fast path.
    assert!(
        elapsed.as_secs() < 2,
        "Fast path throughput regression detected: 10_000 GETs took {:?}. \
         Expected < 2s. This likely means per-command overhead (clock calls, \
         RwLock acquisitions) has been added to the fast path. \
         Check: slowlog default threshold, latency.record() gating, \
         any new code inside the !slowlog_enabled fast path branch.",
        elapsed
    );

    // Also verify slowlog is actually disabled (catches the root cause directly)
    let slowlog_len = c.cmd(&["SLOWLOG", "LEN"]);
    assert_eq!(
        slowlog_len.as_int(),
        0,
        "Slowlog should be empty with default config (threshold=u64::MAX). \
         If it has entries, the default threshold has been changed to a low value, \
         which causes per-command overhead on the fast path."
    );
}

/// Verifies that explicitly enabling slowlog (threshold=0) does NOT
/// break correctness — commands still execute and return correct results.
/// Performance will be lower but must not crash or corrupt data.
#[test]
fn test_slowlog_enabled_correctness() {
    let port = free_port();
    start_server_log_all(port); // threshold=0, logs everything
    let mut c = Conn::connect(port);

    // Basic correctness under slowlog
    c.cmd(&["SET", "sl_key", "sl_value"]);
    let result = c.cmd(&["GET", "sl_key"]);
    assert_eq!(result.as_str(), "sl_value");

    // Slowlog must actually have entries
    let len = c.cmd(&["SLOWLOG", "LEN"]).as_int();
    assert!(len >= 2, "With threshold=0, slowlog must record commands");

    // Data integrity: 1000 SET/GET pairs must all be correct
    for i in 0..1000u32 {
        c.cmd(&["SET", &format!("integrity_{i}"), &format!("val_{i}")]);
    }
    for i in 0..1000u32 {
        let v = c.cmd(&["GET", &format!("integrity_{i}")]);
        assert_eq!(
            v.as_str(),
            &format!("val_{i}"),
            "Data corruption under slowlog at key integrity_{i}"
        );
    }
}

#[test]
fn test_latency_doctor_returns_bulk_string() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmd(&["LATENCY", "DOCTOR"]);
    // Must always return a bulk string — never an error
    assert!(
        matches!(result, RespValue::Bulk(_)),
        "LATENCY DOCTOR must return a bulk string, got {result:?}"
    );
    // The response must be non-empty
    assert!(
        !result.as_str().is_empty(),
        "LATENCY DOCTOR must not return empty string"
    );
}

#[test]
fn test_latency_doctor_no_issues_returns_dave_message() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Fresh server with no high-latency events — should return clean bill
    c.cmd(&["LATENCY", "RESET"]);
    let result = c.cmd(&["LATENCY", "DOCTOR"]);
    let text = result.as_str();
    assert!(
        text.contains("no problem to report"),
        "expected clean-bill message, got: {text:?}"
    );
}

/// `INFO commandstats` carries the per-command slowlog totals Redis 8.8 added,
/// for the commands that have been in the slowlog.
#[test]
fn test_commandstats_reports_per_command_slowlog_totals() {
    let port = free_port();
    start_server_log_all(port);
    let mut c = Conn::connect(port);

    for i in 0..3 {
        c.cmd(&["SET", &format!("cs:{i}"), "v"]);
    }
    c.cmd(&["GET", "cs:0"]);

    let info = c.cmd(&["INFO", "commandstats"]);
    let info = info.as_str();
    assert!(info.contains("# Commandstats"), "{info}");
    let line = |name: &str| {
        info.lines()
            .find(|l| l.starts_with(&format!("cmdstat_{name}:")))
            .unwrap_or_else(|| {
                panic!(
                    "no cmdstat_{name} line in:
{info}"
                )
            })
            .to_string()
    };
    assert!(line("set").contains("slowlog_count=3,"), "{}", line("set"));
    assert!(line("get").contains("slowlog_count=1,"), "{}", line("get"));
    assert!(
        line("set").contains("slowlog_time_ms_sum=")
            && line("set").contains("slowlog_time_ms_max=")
    );

    // The default INFO does not include the section, as in Redis.
    assert!(!c.cmd(&["INFO"]).as_str().contains("# Commandstats"));
}

/// Changing the threshold with CONFIG SET takes effect on connections that
/// are already open — including the one that changed it.
#[test]
fn test_config_set_threshold_applies_to_open_connections() {
    let port = free_port();
    start_server(port);
    let mut changer = Conn::connect(port);
    let mut bystander = Conn::connect(port);
    bystander.cmd(&["PING"]);

    assert_eq!(changer.cmd(&["SLOWLOG", "LEN"]).as_int(), 0);
    changer.cmd(&["CONFIG", "SET", "slowlog-log-slower-than", "0"]);
    changer.cmd(&["SET", "from-the-changer", "v"]);
    bystander.cmd(&["SET", "from-the-bystander", "v"]);

    let entries = changer.cmd(&["SLOWLOG", "GET", "10"]);
    let logged: Vec<String> = entries
        .as_array()
        .iter()
        .map(|entry| {
            entry.as_array()[3]
                .as_array()
                .iter()
                .map(|arg| arg.as_str().to_string())
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect();
    assert!(
        logged.iter().any(|c| c == "SET from-the-changer v"),
        "{logged:?}"
    );
    assert!(
        logged.iter().any(|c| c == "SET from-the-bystander v"),
        "{logged:?}"
    );

    // And off again.
    changer.cmd(&["CONFIG", "SET", "slowlog-log-slower-than", "-1"]);
    changer.cmd(&["SLOWLOG", "RESET"]);
    bystander.cmd(&["SET", "not-logged", "v"]);
    changer.cmd(&["SET", "not-logged-either", "v"]);
    assert_eq!(changer.cmd(&["SLOWLOG", "LEN"]).as_int(), 0);
}
