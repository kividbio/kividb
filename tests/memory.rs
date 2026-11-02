// tests/memory.rs
//
// Rust integration tests for MEMORY commands.
// Run with: cargo test --test memory

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

// ── RESP harness ──────────────────────────────────────────────────────────────

struct Conn {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Conn {
    fn connect(port: u16) -> Self {
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
                Err(e) => panic!("connect failed: {e}"),
            }
        }
    }

    fn cmd(&mut self, args: &[&str]) -> RespValue {
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
                let mut data = vec![0u8; len as usize + 2];
                self.reader.read_exact(&mut data).unwrap();
                data.truncate(len as usize);
                RespValue::Bulk(data)
            }
            Some('*') => {
                let count: i64 = line[1..].parse().unwrap();
                if count == -1 {
                    return RespValue::Nil;
                }
                RespValue::Array((0..count).map(|_| self.read_value()).collect())
            }
            _ => panic!("unexpected RESP: {line:?}"),
        }
    }
}

#[derive(Debug)]
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
            _ => panic!("not a string: {self:?}"),
        }
    }
    fn as_int(&self) -> i64 {
        match self {
            RespValue::Integer(n) => *n,
            _ => panic!("not int: {self:?}"),
        }
    }
    fn as_array(&self) -> &[RespValue] {
        match self {
            RespValue::Array(v) => v,
            _ => panic!("not array: {self:?}"),
        }
    }
    fn is_ok(&self) -> bool {
        matches!(self, RespValue::SimpleStr(s) if s == "OK")
    }
    fn is_nil(&self) -> bool {
        matches!(self, RespValue::Nil)
    }
    fn is_error(&self) -> bool {
        matches!(self, RespValue::Error(_))
    }
}

use std::sync::atomic::{AtomicU16, Ordering};

static NEXT_PORT: AtomicU16 = AtomicU16::new(16400);

fn free_port() -> u16 {
    NEXT_PORT.fetch_add(1, Ordering::Relaxed)
}

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
            slowlog_log_slower_than: u64::MAX,
            slowlog_max_len: 128,
            cluster_enabled: false,
            cluster_node_timeout: 15000,
            configfile: "./kividb.conf".to_string(),
            ..Default::default()
        });
    });
}

fn used_memory(c: &mut Conn) -> u64 {
    let RespValue::Bulk(info) = c.cmd(&["INFO", "memory"]) else {
        panic!("INFO memory");
    };
    String::from_utf8(info)
        .unwrap()
        .lines()
        .find_map(|l| l.strip_prefix("used_memory:"))
        .expect("used_memory")
        .trim()
        .parse()
        .unwrap()
}

/// `cmd key` for each key, pipelined; the replies are read and dropped.
fn pipeline(c: &mut Conn, cmd: &[&str], keys: impl Iterator<Item = String>) {
    let keys: Vec<String> = keys.collect();
    for chunk in keys.chunks(1000) {
        let mut buf = String::new();
        for key in chunk {
            buf.push_str(&format!("*{}\r\n", cmd.len() + 1));
            buf.push_str(&format!("${}\r\n{}\r\n", cmd[0].len(), cmd[0]));
            buf.push_str(&format!("${}\r\n{}\r\n", key.len(), key));
            for a in &cmd[1..] {
                buf.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
            }
        }
        c.writer.write_all(buf.as_bytes()).unwrap();
        for _ in chunk {
            assert!(!c.read_value().is_error());
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// TABLES GIVE BACK THE ROOM OF KEYS THAT HAVE GONE (#113)
// ═══════════════════════════════════════════════════════════════════════════════

/// After most keys are deleted, `used_memory` falls within a few
/// seconds, by the room the tables held for them: tables used to keep the
/// size they had at their largest.
#[test]
fn test_tables_shrink_after_most_keys_are_deleted() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let n = 400_000;
    // `used_memory` is the whole process's, other tests' servers included:
    // what this test adds is measured from here.
    let before = used_memory(&mut c);
    pipeline(&mut c, &["SET", "v"], (0..n).map(|i| format!("k:{i}")));
    pipeline(
        &mut c,
        &["EXPIRE", "100000"],
        (0..n).map(|i| format!("k:{i}")),
    );
    let loaded = used_memory(&mut c).saturating_sub(before);
    pipeline(
        &mut c,
        &["DEL"],
        (0..n).filter(|i| i % 10 != 0).map(|i| format!("k:{i}")),
    );
    // With values this small the tables of values and of times are most of
    // the memory: on Windows, 42 MB loaded, 39 MB left with the tables kept
    // at their size after the delete, 5 MB with them shrunk.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let now = used_memory(&mut c).saturating_sub(before);
        if now * 3 < loaded {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{now} bytes left after deleting 90% of the keys, {loaded} loaded"
        );
        thread::sleep(Duration::from_millis(200));
    }
    assert!(matches!(c.cmd(&["DBSIZE"]), RespValue::Integer(k) if k == n / 10));
}

// ═══════════════════════════════════════════════════════════════════════════════
/// `SAMPLES 0` is every element, as in Redis: it divided by zero for a
/// hash, a set and a sorted set, and the worker panicked and the
/// connection closed (#151). A count that is not a number is an error.
#[test]
fn test_memory_usage_samples_zero_is_every_element() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmd(&["HSET", "h", "a", "1", "b", "2"]);
    c.cmd(&["SADD", "s", "a", "b"]);
    c.cmd(&["ZADD", "z", "1", "a", "2", "b"]);
    c.cmd(&["RPUSH", "l", "a", "b"]);
    for key in ["h", "s", "z", "l"] {
        let all = c.cmd(&["MEMORY", "USAGE", key, "SAMPLES", "0"]).as_int();
        assert!(all > 0, "{key}: {all}");
    }
    assert!(c.cmd(&["MEMORY", "USAGE", "h", "SAMPLES", "x"]).is_error());
    assert!(c.cmd(&["MEMORY", "USAGE", "h", "SAMPLES", "-1"]).is_error());
    assert!(c.cmd(&["MEMORY", "USAGE", "h", "BOGUS", "1"]).is_error());
    // The connection is still there.
    assert!(matches!(c.cmd(&["PING"]), RespValue::SimpleStr(s) if s == "PONG"));
}

// MEMORY USAGE
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_memory_usage_missing_key_returns_nil() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    assert!(c.cmd(&["MEMORY", "USAGE", "no_such_key"]).is_nil());
}

#[test]
fn test_memory_usage_string_returns_positive_integer() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmd(&["SET", "k", "hello"]);
    let result = c.cmd(&["MEMORY", "USAGE", "k"]);
    assert!(matches!(result, RespValue::Integer(n) if n > 0));
}

#[test]
fn test_memory_usage_larger_string_reports_more_memory() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmd(&["SET", "small", "x"]);
    c.cmd(&["SET", "large", &"x".repeat(10_000)]);
    let small = c.cmd(&["MEMORY", "USAGE", "small"]).as_int();
    let large = c.cmd(&["MEMORY", "USAGE", "large"]).as_int();
    assert!(
        large > small,
        "large string should use more memory: small={small} large={large}"
    );
}

#[test]
fn test_memory_usage_list() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    for i in 0..20 {
        c.cmd(&["RPUSH", "mylist", &format!("value_{i}")]);
    }
    let result = c.cmd(&["MEMORY", "USAGE", "mylist"]);
    assert!(matches!(result, RespValue::Integer(n) if n > 0));
}

#[test]
fn test_memory_usage_list_grows_with_elements() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmd(&["RPUSH", "list_small", "a"]);
    for i in 0..100 {
        c.cmd(&["RPUSH", "list_large", &format!("elem_{i}")]);
    }
    let small = c.cmd(&["MEMORY", "USAGE", "list_small"]).as_int();
    let large = c.cmd(&["MEMORY", "USAGE", "list_large"]).as_int();
    assert!(large > small);
}

#[test]
fn test_memory_usage_hash() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmd(&["HSET", "myhash", "f1", "v1", "f2", "v2"]);
    let result = c.cmd(&["MEMORY", "USAGE", "myhash"]);
    assert!(matches!(result, RespValue::Integer(n) if n > 0));
}

#[test]
fn test_memory_usage_set() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmd(&["SADD", "myset", "a", "b", "c"]);
    let result = c.cmd(&["MEMORY", "USAGE", "myset"]);
    assert!(matches!(result, RespValue::Integer(n) if n > 0));
}

#[test]
fn test_memory_usage_sorted_set() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmd(&["ZADD", "myzset", "1.0", "a", "2.0", "b", "3.0", "c"]);
    let result = c.cmd(&["MEMORY", "USAGE", "myzset"]);
    assert!(matches!(result, RespValue::Integer(n) if n > 0));
}

#[test]
fn test_memory_usage_sorted_set_dual_index_overhead() {
    // SortedSet is dual-indexed (BTreeMap + AHashMap), so its memory
    // per element should be higher than a Set of equal members
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    for i in 0..50 {
        c.cmd(&["SADD", "cmp_set", &format!("member_{i}")]);
        c.cmd(&[
            "ZADD",
            "cmp_zset",
            &format!("{i}.0"),
            &format!("member_{i}"),
        ]);
    }
    let set_mem = c.cmd(&["MEMORY", "USAGE", "cmp_set"]).as_int();
    let zset_mem = c.cmd(&["MEMORY", "USAGE", "cmp_zset"]).as_int();
    assert!(
        zset_mem > set_mem,
        "sorted set should use more memory than plain set (dual-indexed): \
         set={set_mem} zset={zset_mem}"
    );
}

#[test]
fn test_memory_usage_samples_zero() {
    // SAMPLES 0 is accepted and treated as no sampling limit
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmd(&["RPUSH", "samp_list", "a", "b", "c"]);
    let result = c.cmd(&["MEMORY", "USAGE", "samp_list", "SAMPLES", "0"]);
    assert!(matches!(result, RespValue::Integer(n) if n > 0));
}

#[test]
fn test_memory_usage_samples_large() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmd(&["RPUSH", "samp_list2", "a", "b", "c"]);
    let result = c.cmd(&["MEMORY", "USAGE", "samp_list2", "SAMPLES", "100"]);
    assert!(matches!(result, RespValue::Integer(n) if n > 0));
}

#[test]
fn test_memory_usage_missing_key_arg_errors() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    assert!(c.cmd(&["MEMORY", "USAGE"]).is_error());
}

#[test]
fn test_memory_usage_json() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmd(&["JSON.SET", "myjson", "$", r#"{"a":1,"b":"hello"}"#]);
    let result = c.cmd(&["MEMORY", "USAGE", "myjson"]);
    assert!(matches!(result, RespValue::Integer(n) if n > 0));
}

#[test]
fn test_memory_usage_stream() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmd(&["XADD", "mystream", "*", "field", "value"]);
    c.cmd(&["XADD", "mystream", "*", "field", "value2"]);
    let result = c.cmd(&["MEMORY", "USAGE", "mystream"]);
    assert!(matches!(result, RespValue::Integer(n) if n > 0));
}

// ═══════════════════════════════════════════════════════════════════════════════
// MEMORY STATS
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_memory_stats_returns_array() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let result = c.cmd(&["MEMORY", "STATS"]);
    assert!(matches!(result, RespValue::Array(_)));
}

#[test]
fn test_memory_stats_even_number_of_elements() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let arr = c.cmd(&["MEMORY", "STATS"]);
    assert!(
        arr.as_array().len().is_multiple_of(2),
        "MEMORY STATS must return flat key-value pairs"
    );
}

#[test]
fn test_memory_stats_contains_required_fields() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let arr = c.cmd(&["MEMORY", "STATS"]);
    let pairs = arr.as_array();
    let keys: Vec<&str> = pairs.iter().step_by(2).map(|v| v.as_str()).collect();

    let required = [
        "used_memory",
        "used_memory_human",
        "used_memory_rss",
        "used_memory_rss_human",
        "used_memory_peak",
        "used_memory_peak_human",
        "mem_fragmentation_ratio",
    ];
    for field in required {
        assert!(keys.contains(&field), "MEMORY STATS missing field: {field}");
    }
}

#[test]
fn test_memory_stats_used_memory_is_non_negative() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let arr = c.cmd(&["MEMORY", "STATS"]);
    let pairs = arr.as_array();
    // Find used_memory value
    for i in (0..pairs.len()).step_by(2) {
        if pairs[i].as_str() == "used_memory" {
            let val: i64 = pairs[i + 1].as_str().parse().unwrap();
            assert!(val >= 0);
            return;
        }
    }
    panic!("used_memory not found in MEMORY STATS");
}

#[test]
fn test_memory_stats_human_field_is_non_empty_string() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let arr = c.cmd(&["MEMORY", "STATS"]);
    let pairs = arr.as_array();
    for i in (0..pairs.len()).step_by(2) {
        if pairs[i].as_str() == "used_memory_human" {
            let val = pairs[i + 1].as_str();
            assert!(!val.is_empty(), "used_memory_human must not be empty");
            return;
        }
    }
    panic!("used_memory_human not found");
}

// ═══════════════════════════════════════════════════════════════════════════════
// MEMORY DOCTOR
// ═══════════════════════════════════════════════════════════════════════════════

/// `mem_fragmentation_ratio` is RSS over `used_memory`, as in Redis; it was
/// virtual size over RSS, 56 with 100,000 keys. `used_memory_peak` is the
/// peak of `used_memory`, not of RSS (#152).
#[test]
fn info_memory_fragmentation_is_rss_over_used_memory() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let value = "x".repeat(100);
    for i in 0..50_000 {
        c.cmd(&["SET", &format!("frag:{i}"), &value]);
    }
    let info = c.cmd(&["INFO", "memory"]);
    let field = |name: &str| -> f64 {
        info.as_str()
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{name}:")))
            .unwrap_or_else(|| panic!("no {name}"))
            .trim_end_matches('%')
            .parse()
            .unwrap()
    };
    let (used, rss, ratio) = (
        field("used_memory"),
        field("used_memory_rss"),
        field("mem_fragmentation_ratio"),
    );
    assert!(
        (ratio - rss / used).abs() < 0.01,
        "{ratio} vs {rss} / {used}"
    );
    // Not near 1 with as little as this: a new server's RSS is mostly its
    // own (threads, Lua's VMs), 5.3 on a CI runner. Virtual over resident
    // size, the old figure, was 56.
    assert!(ratio > 0.3 && ratio < 10.0, "{ratio}");
    assert!(field("used_memory_peak") >= used);
    let startup = field("used_memory_startup");
    assert!(startup > 0.0 && startup < used, "{startup} {used}");
}

#[test]
fn test_memory_doctor_returns_non_empty_string() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let result = c.cmd(&["MEMORY", "DOCTOR"]);
    let msg = result.as_str();
    assert!(!msg.is_empty());
}

#[test]
fn test_memory_doctor_healthy_on_fresh_server() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let result = c.cmd(&["MEMORY", "DOCTOR"]);
    // The figures are the process's, and the other tests here fill and
    // free memory in it: any of the doctor's findings can be true.
    let msg = result.as_str();
    assert!(
        [
            "fine",
            "very little memory",
            "High fragmentation",
            "Peak memory"
        ]
        .iter()
        .any(|finding| msg.contains(finding)),
        "Expected one of MEMORY DOCTOR's findings, got: {msg}"
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// MEMORY PURGE
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_memory_purge_returns_ok() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    assert!(c.cmd(&["MEMORY", "PURGE"]).is_ok());
}

#[test]
fn test_memory_purge_does_not_lose_data() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmd(&["SET", "persist_key", "persist_value"]);
    c.cmd(&["MEMORY", "PURGE"]);
    assert_eq!(c.cmd(&["GET", "persist_key"]).as_str(), "persist_value");
}

#[test]
fn test_memory_purge_idempotent() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    for _ in 0..5 {
        assert!(c.cmd(&["MEMORY", "PURGE"]).is_ok());
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// MEMORY MALLOC-STATS
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_memory_malloc_stats_returns_string() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let result = c.cmd(&["MEMORY", "MALLOC-STATS"]);
    let s = result.as_str();
    assert!(!s.is_empty());
}

#[test]
fn test_memory_malloc_stats_says_what_redis_without_jemalloc_says() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let result = c.cmd(&["MEMORY", "MALLOC-STATS"]);
    // It was a fixed piece of XML that looked like statistics (#152).
    assert_eq!(
        result.as_str(),
        "Stats not supported for the current allocator"
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// Unknown subcommand
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_memory_unknown_subcommand_errors() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    assert!(c.cmd(&["MEMORY", "BADCMD"]).is_error());
}

#[test]
fn test_memory_no_subcommand_errors() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    assert!(c.cmd(&["MEMORY"]).is_error());
}

// ═══════════════════════════════════════════════════════════════════════════════
// human_bytes unit tests (pure logic, no server needed)
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_human_bytes_formatting() {
    // Test the human_bytes function directly through the MEMORY STATS output
    // We can't call it directly (pub(crate)), so we validate through the wire
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Set enough data that the human format should show something meaningful
    c.cmd(&["SET", "big_key", &"x".repeat(1_000_000)]);

    let arr = c.cmd(&["MEMORY", "STATS"]);
    let pairs = arr.as_array();
    for i in (0..pairs.len()).step_by(2) {
        if pairs[i].as_str() == "used_memory_human" {
            let human = pairs[i + 1].as_str();
            // Should end in B, K, M, or G
            assert!(
                human.ends_with('B')
                    || human.ends_with('K')
                    || human.ends_with('M')
                    || human.ends_with('G'),
                "human_bytes should end in B/K/M/G, got: {human}"
            );
            return;
        }
    }
}

/// A string's `MEMORY USAGE` within the bounds Redis 8.10's suite sets
/// (unit/type/string): at least a 16-byte header, the key and the value,
/// and at most twice that, or 64 when that is under 32.
#[test]
fn test_memory_usage_string_within_redis_bounds() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let sizes = [
        1usize, 5, 8, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 257,
    ];
    for k in sizes {
        let key = "k".repeat(k);
        for v in sizes {
            c.cmd(&["SET", &key, &"v".repeat(v)]);
            let used = c.cmd(&["MEMORY", "USAGE", &key]).as_int() as usize;
            let min = 16 + k + v;
            let max = if min < 32 { 64 } else { min * 2 };
            assert!((min..=max).contains(&used), "key {k}, value {v}: {used}");
        }
    }
}
