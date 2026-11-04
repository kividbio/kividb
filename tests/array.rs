// tests/array.rs
//
// Integration tests for the Redis 8.8 array type (AR* commands).
//
// The `spec_example_*` tests are transcriptions of the worked examples on the
// redis.io command pages — they are the closest thing to a conformance suite
// this type has, so they're kept verbatim (same commands, same expected
// replies) rather than paraphrased.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

// ── Minimal RESP client ───────────────────────────────────────────────────────

struct Conn {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

impl Conn {
    fn connect(port: u16) -> Self {
        let stream = (0..40)
            .find_map(|_| {
                thread::sleep(Duration::from_millis(50));
                TcpStream::connect(("127.0.0.1", port)).ok()
            })
            .expect("could not connect to test server");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let reader = BufReader::new(stream.try_clone().unwrap());
        Self { stream, reader }
    }

    fn cmd(&mut self, args: &[&[u8]]) -> RespValue {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            buf.extend_from_slice(arg);
            buf.extend_from_slice(b"\r\n");
        }
        self.stream.write_all(&buf).unwrap();
        self.read_value()
    }

    fn cmds(&mut self, args: &[&str]) -> RespValue {
        let owned: Vec<Vec<u8>> = args.iter().map(|s| s.as_bytes().to_vec()).collect();
        let refs: Vec<&[u8]> = owned.iter().map(|v| v.as_slice()).collect();
        self.cmd(&refs)
    }

    fn read_value(&mut self) -> RespValue {
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        let line = line.trim_end_matches("\r\n");
        match line.as_bytes().first() {
            Some(b'+') => RespValue::SimpleStr(line[1..].to_string()),
            Some(b'-') => RespValue::Error(line[1..].to_string()),
            Some(b':') => RespValue::Int(line[1..].parse().unwrap()),
            Some(b'$') => {
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
            Some(b'*') => {
                let count: i64 = line[1..].parse().unwrap();
                if count == -1 {
                    return RespValue::Nil;
                }
                let items = (0..count).map(|_| self.read_value()).collect();
                RespValue::Array(items)
            }
            _ => panic!("unexpected RESP: {:?}", line),
        }
    }
}

#[derive(Debug, PartialEq)]
enum RespValue {
    SimpleStr(String),
    Error(String),
    Int(i64),
    Bulk(Vec<u8>),
    Array(Vec<RespValue>),
    Nil,
}

impl RespValue {
    fn as_str(&self) -> &str {
        match self {
            RespValue::SimpleStr(s) => s,
            RespValue::Bulk(b) => std::str::from_utf8(b).unwrap(),
            _ => panic!("not a string: {:?}", self),
        }
    }
    fn as_int(&self) -> i64 {
        match self {
            RespValue::Int(n) => *n,
            RespValue::Bulk(b) => std::str::from_utf8(b).unwrap().parse().unwrap(),
            _ => panic!("not an int: {:?}", self),
        }
    }
    fn as_array(&self) -> &[RespValue] {
        match self {
            RespValue::Array(v) => v,
            _ => panic!("not an array: {:?}", self),
        }
    }
    fn is_nil(&self) -> bool {
        matches!(self, RespValue::Nil)
    }
    fn is_error(&self) -> bool {
        matches!(self, RespValue::Error(_))
    }
    /// Flatten an array reply to plain strings, rendering nils as `<nil>`.
    fn strs(&self) -> Vec<String> {
        self.as_array()
            .iter()
            .map(|v| match v {
                RespValue::Nil => "<nil>".to_string(),
                other => other.as_str().to_string(),
            })
            .collect()
    }
    /// Flatten an array reply to plain strings, rendering integers too.
    /// The elements, with the `[index, value]` pairs of ARSCAN and ARGREP
    /// WITHVALUES laid end to end.
    fn flat(&self) -> Vec<String> {
        self.as_array()
            .iter()
            .flat_map(|v| match v {
                RespValue::Array(pair) => pair.iter().collect::<Vec<_>>(),
                single => vec![single],
            })
            .map(|v| match v {
                RespValue::Nil => "<nil>".to_string(),
                RespValue::Int(n) => n.to_string(),
                other => other.as_str().to_string(),
            })
            .collect()
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

fn start_server(port: u16) {
    thread::spawn(move || {
        let config = kividb::config::Config {
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
        };
        kividb::run_for_test(config);
    });
}

/// One server shared by every test in this file; each test uses its own keys.
fn conn() -> Conn {
    use std::sync::OnceLock;
    static PORT: OnceLock<u16> = OnceLock::new();
    let port = *PORT.get_or_init(|| {
        let p = free_port();
        start_server(p);
        p
    });
    Conn::connect(port)
}

// ── Spec examples, transcribed from redis.io ─────────────────────────────────

/// ARSET / ARGET page examples.
#[test]
fn spec_example_arset_arget() {
    let mut c = conn();
    let k = "t:arset";
    c.cmds(&["DEL", k]);

    assert_eq!(c.cmds(&["ARSET", k, "0", "hello"]).as_int(), 1);
    assert_eq!(c.cmds(&["ARGET", k, "0"]).as_str(), "hello");
    // Three contiguous values from index 2, all previously empty.
    assert_eq!(c.cmds(&["ARSET", k, "2", "a", "b", "c"]).as_int(), 3);
    assert_eq!(c.cmds(&["ARGET", k, "2"]).as_str(), "a");
    assert_eq!(c.cmds(&["ARGET", k, "4"]).as_str(), "c");
    // Index 1 was never written.
    assert!(c.cmds(&["ARGET", k, "1"]).is_nil());
    // Overwriting an occupied slot reports zero *new* slots.
    assert_eq!(c.cmds(&["ARSET", k, "0", "world"]).as_int(), 0);
}

/// ARCOUNT / ARLEN page examples — len is max index + 1, count is occupancy.
#[test]
fn spec_example_arcount_arlen() {
    let mut c = conn();
    let k = "t:arcount";
    c.cmds(&["DEL", k]);

    assert_eq!(c.cmds(&["ARSET", k, "0", "a"]).as_int(), 1);
    assert_eq!(c.cmds(&["ARSET", k, "5", "b"]).as_int(), 1);
    assert_eq!(c.cmds(&["ARCOUNT", k]).as_int(), 2);
    assert_eq!(c.cmds(&["ARLEN", k]).as_int(), 6);

    // Missing key replies 0 for both, not an error.
    assert_eq!(c.cmds(&["ARCOUNT", "t:absent"]).as_int(), 0);
    assert_eq!(c.cmds(&["ARLEN", "t:absent"]).as_int(), 0);
}

/// ARMSET / ARMGET page examples — sparse writes, order-preserving reads.
#[test]
fn spec_example_armset_armget() {
    let mut c = conn();
    let k = "t:armset";
    c.cmds(&["DEL", k]);

    assert_eq!(
        c.cmds(&["ARMSET", k, "0", "alpha", "5", "beta", "100", "gamma"])
            .as_int(),
        3
    );
    assert_eq!(c.cmds(&["ARGET", k, "0"]).as_str(), "alpha");
    assert_eq!(c.cmds(&["ARGET", k, "5"]).as_str(), "beta");
    assert_eq!(c.cmds(&["ARGET", k, "100"]).as_str(), "gamma");

    let k2 = "t:armget";
    c.cmds(&["DEL", k2]);
    assert_eq!(
        c.cmds(&["ARMSET", k2, "0", "a", "1", "b", "5", "c"])
            .as_int(),
        3
    );
    // Index 3 is empty → nil, and the reply keeps the requested order.
    assert_eq!(
        c.cmds(&["ARMGET", k2, "0", "1", "5", "3"]).strs(),
        vec!["a", "b", "c", "<nil>"]
    );
}

/// ARDEL page example.
#[test]
fn spec_example_ardel() {
    let mut c = conn();
    let k = "t:ardel";
    c.cmds(&["DEL", k]);

    c.cmds(&["ARSET", k, "0", "a"]);
    c.cmds(&["ARSET", k, "1", "b"]);
    c.cmds(&["ARSET", k, "2", "c"]);
    assert_eq!(c.cmds(&["ARDEL", k, "1"]).as_int(), 1);
    assert!(c.cmds(&["ARGET", k, "1"]).is_nil());
    assert_eq!(c.cmds(&["ARDEL", k, "0", "2"]).as_int(), 2);
    assert_eq!(c.cmds(&["ARCOUNT", k]).as_int(), 0);
    // Deleting an absent index is a no-op, not an error.
    assert_eq!(c.cmds(&["ARDEL", k, "9"]).as_int(), 0);
}

/// ARDELRANGE page example.
#[test]
fn spec_example_ardelrange() {
    let mut c = conn();
    let k = "t:ardelrange";
    c.cmds(&["DEL", k]);

    assert_eq!(
        c.cmds(&[
            "ARMSET", k, "0", "a", "1", "b", "2", "c", "3", "d", "4", "e"
        ])
        .as_int(),
        5
    );
    assert_eq!(c.cmds(&["ARDELRANGE", k, "1", "3"]).as_int(), 3);
    assert_eq!(c.cmds(&["ARCOUNT", k]).as_int(), 2);
    assert_eq!(c.cmds(&["ARGET", k, "0"]).as_str(), "a");
    assert_eq!(c.cmds(&["ARGET", k, "4"]).as_str(), "e");
}

/// Overlapping ARDELRANGE spans must count each element once.
#[test]
fn ardelrange_overlapping_spans_count_once() {
    let mut c = conn();
    let k = "t:ardelrange:overlap";
    c.cmds(&["DEL", k]);
    c.cmds(&["ARMSET", k, "0", "a", "1", "b", "2", "c", "3", "d"]);

    // [0,2] and [1,3] overlap on indices 1 and 2 — 4 distinct elements total.
    assert_eq!(c.cmds(&["ARDELRANGE", k, "0", "2", "1", "3"]).as_int(), 4);
    assert_eq!(c.cmds(&["ARCOUNT", k]).as_int(), 0);
}

/// ARGETRANGE page example, including the reverse form.
#[test]
fn spec_example_argetrange() {
    let mut c = conn();
    let k = "t:argetrange";
    c.cmds(&["DEL", k]);

    assert_eq!(
        c.cmds(&["ARMSET", k, "0", "a", "1", "b", "3", "d"])
            .as_int(),
        3
    );
    // Empty slots are materialised as nils — this is what separates
    // ARGETRANGE from ARSCAN.
    assert_eq!(
        c.cmds(&["ARGETRANGE", k, "0", "3"]).strs(),
        vec!["a", "b", "<nil>", "d"]
    );
    // start > end reverses the reported order.
    assert_eq!(
        c.cmds(&["ARGETRANGE", k, "3", "0"]).strs(),
        vec!["d", "<nil>", "b", "a"]
    );
}

/// ARINSERT / ARNEXT page examples — the cursor advances one per value.
#[test]
fn spec_example_arinsert_arnext() {
    let mut c = conn();
    let k = "t:arinsert";
    c.cmds(&["DEL", k]);

    assert_eq!(c.cmds(&["ARINSERT", k, "alpha"]).as_int(), 0);
    assert_eq!(c.cmds(&["ARINSERT", k, "beta"]).as_int(), 1);
    assert_eq!(c.cmds(&["ARINSERT", k, "gamma"]).as_int(), 2);
    assert_eq!(c.cmds(&["ARGET", k, "1"]).as_str(), "beta");
    assert_eq!(c.cmds(&["ARNEXT", k]).as_int(), 3);

    // Multi-value insert reports the *last* index written.
    assert_eq!(c.cmds(&["ARINSERT", k, "d", "e"]).as_int(), 4);
    assert_eq!(c.cmds(&["ARNEXT", k]).as_int(), 5);

    // ARNEXT on a missing key is 0, not nil.
    assert_eq!(c.cmds(&["ARNEXT", "t:absent"]).as_int(), 0);
}

/// ARSEEK page example — note the reply is 1 (cursor set), and the *next*
/// ARINSERT lands at the sought index.
#[test]
fn spec_example_arseek() {
    let mut c = conn();
    let k = "t:arseek";
    c.cmds(&["DEL", k]);

    assert_eq!(c.cmds(&["ARINSERT", k, "a"]).as_int(), 0);
    assert_eq!(c.cmds(&["ARINSERT", k, "b"]).as_int(), 1);
    assert_eq!(c.cmds(&["ARNEXT", k]).as_int(), 2);
    assert_eq!(c.cmds(&["ARSEEK", k, "10"]).as_int(), 1);
    assert_eq!(c.cmds(&["ARINSERT", k, "c"]).as_int(), 10);
    assert_eq!(c.cmds(&["ARNEXT", k]).as_int(), 11);
}

/// ARSEEK must not create the key — the 0 reply is how a caller detects a miss.
#[test]
fn arseek_on_missing_key_returns_zero_and_does_not_create() {
    let mut c = conn();
    let k = "t:arseek:missing";
    c.cmds(&["DEL", k]);

    assert_eq!(c.cmds(&["ARSEEK", k, "5"]).as_int(), 0);
    assert_eq!(c.cmds(&["EXISTS", k]).as_int(), 0);
}

/// ARRING page example — the wrap, the overwrite, and ARLASTITEMS ordering.
#[test]
fn spec_example_arring() {
    let mut c = conn();
    let k = "t:arring";
    c.cmds(&["DEL", k]);

    assert_eq!(c.cmds(&["ARRING", k, "3", "v0"]).as_int(), 0);
    assert_eq!(c.cmds(&["ARRING", k, "3", "v1"]).as_int(), 1);
    assert_eq!(c.cmds(&["ARRING", k, "3", "v2"]).as_int(), 2);
    // Wraps back to 0 and overwrites v0.
    assert_eq!(c.cmds(&["ARRING", k, "3", "v3"]).as_int(), 0);
    assert_eq!(c.cmds(&["ARGET", k, "0"]).as_str(), "v3");
    assert_eq!(c.cmds(&["ARCOUNT", k]).as_int(), 3);
    assert_eq!(
        c.cmds(&["ARLASTITEMS", k, "3"]).strs(),
        vec!["v1", "v2", "v3"]
    );
}

/// Shrinking the ring window truncates to fit.
#[test]
fn arring_shrinking_window_truncates() {
    let mut c = conn();
    let k = "t:arring:shrink";
    c.cmds(&["DEL", k]);

    c.cmds(&["ARRING", k, "5", "a", "b", "c", "d", "e"]);
    assert_eq!(c.cmds(&["ARCOUNT", k]).as_int(), 5);
    // Re-sizing to 2 drops everything at index >= 2.
    c.cmds(&["ARRING", k, "2", "x"]);
    assert_eq!(c.cmds(&["ARCOUNT", k]).as_int(), 2);
    assert_eq!(c.cmds(&["ARLEN", k]).as_int(), 2);
}

/// ARLASTITEMS page example — default oldest-first, REV newest-first.
#[test]
fn spec_example_arlastitems() {
    let mut c = conn();
    let k = "t:arlastitems";
    c.cmds(&["DEL", k]);

    c.cmds(&["ARINSERT", k, "first"]);
    c.cmds(&["ARINSERT", k, "second"]);
    c.cmds(&["ARINSERT", k, "third"]);
    assert_eq!(
        c.cmds(&["ARLASTITEMS", k, "2"]).strs(),
        vec!["second", "third"]
    );
    assert_eq!(
        c.cmds(&["ARLASTITEMS", k, "2", "REV"]).strs(),
        vec!["third", "second"]
    );
    // count larger than the array returns everything.
    assert_eq!(
        c.cmds(&["ARLASTITEMS", k, "99"]).strs(),
        vec!["first", "second", "third"]
    );
}

/// ARSCAN page example. Reply is flat `[idx, val, ...]` per the command page's
/// normative "Return information" section.
#[test]
fn spec_example_arscan() {
    let mut c = conn();
    let k = "t:arscan";
    c.cmds(&["DEL", k]);

    c.cmds(&["ARSET", k, "0", "a"]);
    c.cmds(&["ARSET", k, "5", "b"]);
    c.cmds(&["ARSET", k, "9", "c"]);

    assert_eq!(
        c.cmds(&["ARSCAN", k, "0", "10"]).flat(),
        vec!["0", "a", "5", "b", "9", "c"]
    );
    // As pairs, as Redis 8.10 replies.
    assert_eq!(
        c.cmds(&["ARSCAN", k, "0", "0"]),
        RespValue::Array(vec![RespValue::Array(vec![
            RespValue::Int(0),
            RespValue::Bulk(b"a".to_vec())
        ])])
    );
    // LIMIT caps *existing* elements, not slots visited.
    assert_eq!(
        c.cmds(&["ARSCAN", k, "0", "10", "LIMIT", "2"]).flat(),
        vec!["0", "a", "5", "b"]
    );
    // Reverse traversal when start > end.
    assert_eq!(
        c.cmds(&["ARSCAN", k, "10", "0"]).flat(),
        vec!["9", "c", "5", "b", "0", "a"]
    );
}

/// ARGREP page examples — all four predicates, NOCASE, WITHVALUES, LIMIT,
/// explicit OR, and the `-`/`+` bounds.
#[test]
fn spec_example_argrep() {
    let mut c = conn();
    let k = "t:argrep";
    c.cmds(&["DEL", k]);

    assert_eq!(
        c.cmds(&[
            "ARMSET",
            k,
            "0",
            "boot: ok",
            "1",
            "warn: disk",
            "2",
            "ERROR: cpu",
            "3",
            "info: ready",
            "4",
            "error: net",
        ])
        .as_int(),
        5
    );

    // MATCH is substring; NOCASE folds case; `-`/`+` are first/last index.
    assert_eq!(
        c.cmds(&["ARGREP", k, "-", "+", "MATCH", "error", "NOCASE"])
            .flat(),
        vec!["2", "4"]
    );
    assert_eq!(
        c.cmds(&[
            "ARGREP",
            k,
            "-",
            "+",
            "MATCH",
            "error",
            "NOCASE",
            "WITHVALUES"
        ])
        .flat(),
        vec!["2", "ERROR: cpu", "4", "error: net"]
    );
    // Two GLOB predicates combined with an explicit OR.
    assert_eq!(
        c.cmds(&[
            "ARGREP", k, "0", "4", "GLOB", "warn:*", "OR", "GLOB", "error:*"
        ])
        .flat(),
        vec!["1", "4"]
    );
    // RE with NOCASE.
    assert_eq!(
        c.cmds(&[
            "ARGREP",
            k,
            "0",
            "4",
            "RE",
            "^[A-Za-z]+: (cpu|net)$",
            "NOCASE",
            "WITHVALUES",
        ])
        .flat(),
        vec!["2", "ERROR: cpu", "4", "error: net"]
    );
    assert_eq!(
        c.cmds(&["ARGREP", k, "0", "4", "EXACT", "info: ready"])
            .flat(),
        vec!["3"]
    );
    assert_eq!(
        c.cmds(&[
            "ARGREP", k, "-", "+", "MATCH", "error", "NOCASE", "LIMIT", "1"
        ])
        .flat(),
        vec!["2"]
    );
}

/// ARGREP AND requires every predicate to match.
#[test]
fn argrep_and_requires_all_predicates() {
    let mut c = conn();
    let k = "t:argrep:and";
    c.cmds(&["DEL", k]);
    c.cmds(&["ARMSET", k, "0", "alpha beta", "1", "alpha", "2", "beta"]);

    assert_eq!(
        c.cmds(&[
            "ARGREP", k, "0", "2", "MATCH", "alpha", "MATCH", "beta", "AND"
        ])
        .flat(),
        vec!["0"]
    );
    // Default combiner is OR — every element matches at least one.
    assert_eq!(
        c.cmds(&["ARGREP", k, "0", "2", "MATCH", "alpha", "MATCH", "beta"])
            .flat(),
        vec!["0", "1", "2"]
    );
}

/// ARGREP reverses when start > end.
#[test]
fn argrep_reverse_order() {
    let mut c = conn();
    let k = "t:argrep:rev";
    c.cmds(&["DEL", k]);
    c.cmds(&["ARMSET", k, "0", "x1", "1", "x2", "2", "x3"]);

    assert_eq!(
        c.cmds(&["ARGREP", k, "2", "0", "GLOB", "x*"]).flat(),
        vec!["2", "1", "0"]
    );
}

/// AROP page examples — all eight operations.
#[test]
fn spec_example_arop() {
    let mut c = conn();
    let k = "t:arop";
    c.cmds(&["DEL", k]);

    assert_eq!(
        c.cmds(&["ARMSET", k, "0", "10", "1", "20", "2", "30"])
            .as_int(),
        3
    );
    assert_eq!(c.cmds(&["AROP", k, "0", "2", "SUM"]).as_str(), "60");
    assert_eq!(c.cmds(&["AROP", k, "0", "2", "MIN"]).as_str(), "10");
    assert_eq!(c.cmds(&["AROP", k, "0", "2", "MAX"]).as_str(), "30");
    assert_eq!(c.cmds(&["AROP", k, "0", "2", "MATCH", "10"]).as_int(), 1);
    assert_eq!(c.cmds(&["AROP", k, "0", "2", "USED"]).as_int(), 3);

    let f = "t:arop:flags";
    c.cmds(&["DEL", f]);
    assert_eq!(
        c.cmds(&["ARMSET", f, "0", "255", "1", "15", "2", "240"])
            .as_int(),
        3
    );
    assert_eq!(c.cmds(&["AROP", f, "0", "2", "AND"]).as_int(), 0);
    assert_eq!(c.cmds(&["AROP", f, "0", "2", "OR"]).as_int(), 255);
    // 255 ^ 15 ^ 240 == 0
    assert_eq!(c.cmds(&["AROP", f, "0", "2", "XOR"]).as_int(), 0);
}

/// AROP replies nil when the range holds no numeric elements.
#[test]
fn arop_nil_when_no_numeric_elements() {
    let mut c = conn();
    let k = "t:arop:nonnum";
    c.cmds(&["DEL", k]);
    c.cmds(&["ARMSET", k, "0", "abc", "1", "def"]);

    assert!(c.cmds(&["AROP", k, "0", "1", "SUM"]).is_nil());
    assert!(c.cmds(&["AROP", k, "0", "1", "MIN"]).is_nil());
    assert!(c.cmds(&["AROP", k, "0", "1", "AND"]).is_nil());
    // USED still counts elements regardless of whether they parse as numbers.
    assert_eq!(c.cmds(&["AROP", k, "0", "1", "USED"]).as_int(), 2);
}

/// ARINFO page example — field names and values.
#[test]
fn spec_example_arinfo() {
    let mut c = conn();
    let k = "t:arinfo";
    c.cmds(&["DEL", k]);

    assert_eq!(
        c.cmds(&["ARMSET", k, "0", "a", "1", "b", "100", "c"])
            .as_int(),
        3
    );
    // ARINSERT writes at cursor 0, overwriting "a" — count stays 3.
    assert_eq!(c.cmds(&["ARINSERT", k, "d"]).as_int(), 0);

    let info = c.cmds(&["ARINFO", k]);
    let flat = info.flat();
    let get = |name: &str| -> i64 {
        let pos = flat.iter().position(|f| f == name).unwrap_or_else(|| {
            panic!("ARINFO missing field {name}: {flat:?}");
        });
        flat[pos + 1].parse().unwrap()
    };
    assert_eq!(get("count"), 3);
    assert_eq!(get("len"), 101);
    assert_eq!(get("next-insert-index"), 1);
    assert_eq!(get("slices"), 1);
    assert_eq!(get("directory-size"), 1);
    assert_eq!(get("super-dir-entries"), 0);
    assert_eq!(get("slice-size"), 4096);

    // FULL adds the per-slice statistics.
    let full = c.cmds(&["ARINFO", k, "FULL"]).flat();
    for field in [
        "dense-slices",
        "sparse-slices",
        "avg-dense-size",
        "avg-dense-fill",
        "avg-sparse-size",
    ] {
        assert!(
            full.iter().any(|f| f == field),
            "FULL missing {field}: {full:?}"
        );
    }
}

// ── Type / error behaviour ───────────────────────────────────────────────────

/// TYPE reports "array", and other type commands reject the key.
#[test]
fn array_type_name_and_wrongtype() {
    let mut c = conn();
    let k = "t:type";
    c.cmds(&["DEL", k]);
    c.cmds(&["ARSET", k, "0", "v"]);

    assert_eq!(c.cmds(&["TYPE", k]).as_str(), "array");
    assert!(c.cmds(&["GET", k]).is_error());
    assert!(c.cmds(&["LPUSH", k, "x"]).is_error());

    // And the reverse: AR* against a string key is WRONGTYPE.
    let s = "t:type:str";
    c.cmds(&["DEL", s]);
    c.cmds(&["SET", s, "hello"]);
    for cmd in [
        vec!["ARGET", s, "0"],
        vec!["ARSET", s, "0", "v"],
        vec!["ARCOUNT", s],
        vec!["ARLEN", s],
        vec!["ARINSERT", s, "v"],
        vec!["ARDEL", s, "0"],
        vec!["ARINFO", s],
    ] {
        let r = c.cmds(&cmd);
        assert!(
            r.is_error() && r.as_error_str().starts_with("WRONGTYPE"),
            "{cmd:?} should be WRONGTYPE, got {r:?}"
        );
    }
}

/// Negative indices are rejected rather than treated as end-relative — an
/// array's index space is sparse, so there is no element for -1 to name.
#[test]
fn negative_indices_rejected() {
    let mut c = conn();
    let k = "t:neg";
    c.cmds(&["DEL", k]);
    c.cmds(&["ARSET", k, "0", "v"]);

    assert!(c.cmds(&["ARGET", k, "-1"]).is_error());
    assert!(c.cmds(&["ARSET", k, "-1", "v"]).is_error());
    assert!(c.cmds(&["ARDEL", k, "-1"]).is_error());
}

/// ARGETRANGE materialises nils, so an unbounded span must be refused rather
/// than allowed to build a reply the size of the index space.
#[test]
fn argetrange_rejects_unbounded_span() {
    let mut c = conn();
    let k = "t:span";
    c.cmds(&["DEL", k]);
    c.cmds(&["ARSET", k, "0", "v"]);

    let r = c.cmds(&["ARGETRANGE", k, "0", "9223372036854775806"]);
    assert!(r.is_error(), "expected a span-limit error, got {r:?}");
    // ARSCAN over the same span is fine — it only visits existing elements.
    assert_eq!(
        c.cmds(&["ARSCAN", k, "0", "9223372036854775806"]).flat(),
        vec!["0", "v"]
    );
}

/// A malformed RE pattern is a clean error, not a panic.
#[test]
fn argrep_invalid_regex_errors() {
    let mut c = conn();
    let k = "t:re";
    c.cmds(&["DEL", k]);
    c.cmds(&["ARSET", k, "0", "v"]);

    assert!(
        c.cmds(&["ARGREP", k, "0", "0", "RE", "([unclosed"])
            .is_error()
    );
}

/// Arity and syntax errors.
#[test]
fn arity_and_syntax_errors() {
    let mut c = conn();
    let k = "t:arity";

    assert!(c.cmds(&["ARSET", k, "0"]).is_error());
    assert!(c.cmds(&["ARGET", k]).is_error());
    assert!(c.cmds(&["ARLEN"]).is_error());
    // ARMSET needs whole index/value pairs.
    assert!(c.cmds(&["ARMSET", k, "0", "a", "1"]).is_error());
    // ARDELRANGE needs whole start/end pairs.
    assert!(c.cmds(&["ARDELRANGE", k, "0", "1", "2"]).is_error());
    // Unknown option tokens.
    assert!(c.cmds(&["ARSCAN", k, "0", "1", "NOPE", "2"]).is_error());
    assert!(c.cmds(&["ARLASTITEMS", k, "1", "NOPE"]).is_error());
    assert!(c.cmds(&["ARINFO", k, "NOPE"]).is_error());
    assert!(c.cmds(&["ARGREP", k, "0", "1", "NOPE", "x"]).is_error());
    // ARRING requires a positive window.
    assert!(c.cmds(&["ARRING", k, "0", "v"]).is_error());
}

/// A failed ARMSET must not apply its earlier pairs.
#[test]
fn armset_is_all_or_nothing_on_bad_index() {
    let mut c = conn();
    let k = "t:armset:atomic";
    c.cmds(&["DEL", k]);

    assert!(c.cmds(&["ARMSET", k, "0", "a", "-5", "b"]).is_error());
    assert_eq!(c.cmds(&["ARCOUNT", k]).as_int(), 0);
    assert_eq!(c.cmds(&["EXISTS", k]).as_int(), 0);
}

/// Arrays participate in the normal keyspace: EXPIRE, DEL, EXISTS.
#[test]
fn array_participates_in_keyspace() {
    let mut c = conn();
    let k = "t:keyspace";
    c.cmds(&["DEL", k]);
    c.cmds(&["ARSET", k, "0", "v"]);

    assert_eq!(c.cmds(&["EXISTS", k]).as_int(), 1);
    assert_eq!(c.cmds(&["EXPIRE", k, "100"]).as_int(), 1);
    assert!(c.cmds(&["TTL", k]).as_int() > 0);
    assert_eq!(c.cmds(&["PERSIST", k]).as_int(), 1);
    assert_eq!(c.cmds(&["DEL", k]).as_int(), 1);
    assert_eq!(c.cmds(&["EXISTS", k]).as_int(), 0);
}

/// DUMP/RESTORE round-trips an array including its cursor and ring window.
#[test]
fn array_dump_restore_roundtrip() {
    let mut c = conn();
    let src = "t:dump:src";
    let dst = "t:dump:dst";
    c.cmds(&["DEL", src]);
    c.cmds(&["DEL", dst]);

    c.cmds(&["ARMSET", src, "0", "a", "7", "b"]);
    c.cmds(&["ARSEEK", src, "42"]);

    let dumped = match c.cmds(&["DUMP", src]) {
        RespValue::Bulk(b) => b,
        other => panic!("DUMP should return a bulk payload, got {other:?}"),
    };
    let r = c.cmd(&[b"RESTORE", dst.as_bytes(), b"0", &dumped]);
    assert!(!r.is_error(), "RESTORE failed: {r:?}");

    assert_eq!(c.cmds(&["ARGET", dst, "0"]).as_str(), "a");
    assert_eq!(c.cmds(&["ARGET", dst, "7"]).as_str(), "b");
    assert_eq!(c.cmds(&["ARLEN", dst]).as_int(), 8);
    // The cursor is part of the value, so it has to survive the round trip.
    assert_eq!(c.cmds(&["ARNEXT", dst]).as_int(), 42);
}

/// AR* commands are registered for COMMAND / ACL introspection.
#[test]
fn array_commands_are_registered() {
    let mut c = conn();

    let cat = c.cmds(&["ACL", "CAT", "array"]);
    let names: Vec<String> = cat.strs().iter().map(|s| s.to_uppercase()).collect();
    for expected in ["ARSET", "ARGET", "ARCOUNT", "ARGREP", "AROP", "ARINFO"] {
        assert!(
            names.iter().any(|n| n == expected),
            "ACL CAT array missing {expected}"
        );
    }

    // `array` must show up in the category list itself.
    let cats: Vec<String> = c.cmds(&["ACL", "CAT"]).strs();
    assert!(cats.iter().any(|c| c == "array"), "ACL CAT missing 'array'");

    // Writes land in @write, reads in @read, via is_write_command.
    let writes: Vec<String> = c.cmds(&["ACL", "CAT", "write"]).strs();
    assert!(writes.iter().any(|n| n == "ARSET"));
    assert!(writes.iter().any(|n| n == "ARSEEK"));
    let reads: Vec<String> = c.cmds(&["ACL", "CAT", "read"]).strs();
    assert!(reads.iter().any(|n| n == "ARGET"));
    assert!(!reads.iter().any(|n| n == "ARSET"));
}

impl RespValue {
    fn as_error_str(&self) -> &str {
        match self {
            RespValue::Error(s) => s,
            _ => panic!("not an error: {:?}", self),
        }
    }
}

/// `ARINFO FULL`'s dense and sparse slices, as Redis 8.10 keeps them (its
/// unit/type/array): a slice is sparse until it would pass 10 elements
/// (`array-sparse-kmax`), and a dense one becomes sparse again only when a
/// deletion leaves 5 or fewer (`array-sparse-kmin`); the dense window grows
/// by powers of two.
#[test]
fn arinfo_slices_are_dense_or_sparse_as_redis_keeps_them() {
    let mut c = conn();
    let k = "t:arslices";
    let info = |c: &mut Conn| -> (i64, i64, f64) {
        let full = c.cmds(&["ARINFO", k, "FULL"]).flat();
        let get = |name: &str| {
            let at = full.iter().position(|f| f == name).unwrap();
            full[at + 1].clone()
        };
        (
            get("dense-slices").parse().unwrap(),
            get("sparse-slices").parse().unwrap(),
            get("avg-dense-size").parse().unwrap(),
        )
    };

    // Growing to the right: one dense slice, a window of at least 128.
    c.cmds(&["DEL", k]);
    for i in 0..100 {
        c.cmds(&["ARSET", k, &i.to_string(), "v"]);
    }
    let (dense, sparse, size) = info(&mut c);
    assert_eq!((dense, sparse), (1, 0));
    assert!(size >= 128.0, "{size}");

    // And to the left.
    c.cmds(&["DEL", k]);
    c.cmds(&["ARSET", k, "500", "anchor"]);
    for i in (400..500).rev() {
        c.cmds(&["ARSET", k, &i.to_string(), "v"]);
    }
    let (dense, _, size) = info(&mut c);
    assert_eq!(dense, 1);
    assert!(size >= 128.0, "{size}");

    // Ten elements are sparse; the eleventh makes the slice dense.
    c.cmds(&["DEL", k]);
    for i in 0..10 {
        c.cmds(&["ARSET", k, &(i * 3).to_string(), "v"]);
    }
    assert_eq!(info(&mut c).0, 0);
    c.cmds(&["ARSET", k, "100", "v"]);
    let (dense, sparse, _) = info(&mut c);
    assert_eq!((dense, sparse), (1, 0));

    // Deleting down to seven keeps it dense; to three, sparse.
    c.cmds(&["DEL", k]);
    for i in 0..50 {
        c.cmds(&["ARSET", k, &i.to_string(), "v"]);
    }
    c.cmds(&["ARDELRANGE", k, "7", "49"]);
    assert_eq!(info(&mut c).0, 1);
    c.cmds(&["ARDELRANGE", k, "3", "6"]);
    let (dense, sparse, _) = info(&mut c);
    assert_eq!((dense, sparse), (0, 1));
    assert_eq!(c.cmds(&["ARCOUNT", k]).as_int(), 3);
    c.cmds(&["DEL", k]);
}

/// `ARLASTITEMS` after `ARSEEK 0` gives the array's tail, and an insert past
/// the last index is `insert index overflow`, as in Redis.
#[test]
fn arlastitems_after_arseek_0_and_insert_overflow() {
    let mut c = conn();
    let k = "t:arlast";
    c.cmds(&["DEL", k]);
    for i in 0..5 {
        c.cmds(&["ARINSERT", k, &(i * 10).to_string()]);
    }
    assert_eq!(c.cmds(&["ARSEEK", k, "0"]).as_int(), 1);
    assert_eq!(c.cmds(&["ARLASTITEMS", k, "3"]).flat(), ["20", "30", "40"]);
    c.cmds(&["DEL", k]);
    c.cmds(&["ARINSERT", k, "a"]);
    c.cmds(&["ARSEEK", k, "18446744073709551615"]);
    let r = c.cmds(&["ARINSERT", k, "b"]);
    assert!(
        r.is_error() && format!("{r:?}").contains("insert index overflow"),
        "{r:?}"
    );
    c.cmds(&["DEL", k]);
}

/// `ARRING` as Redis 8.10 has it: the ring is the array, a size that changes
/// its shape rebuilds it, keeping the newest items as far as the first hole,
/// oldest first from 0. `OBJECT ENCODING` is `sliced-array`.
#[test]
fn arring_rebuilds_as_redis_does() {
    let mut c = conn();
    let k = "t:arring2";
    let get = |c: &mut Conn, i: u64| c.cmds(&["ARGET", k, &i.to_string()]);

    // Grown after wrapping: compacted first, the new value after the newest.
    c.cmds(&["DEL", k]);
    for i in 0..8 {
        c.cmds(&["ARRING", k, "5", &format!("v{i}")]);
    }
    c.cmds(&["ARRING", k, "8", "grown"]);
    for (i, want) in ["v3", "v4", "v5", "v6", "v7", "grown"].iter().enumerate() {
        assert_eq!(get(&mut c, i as u64).as_str(), *want, "{i}");
    }
    assert_eq!(c.cmds(&["ARNEXT", k]).as_int(), 6);

    // Shrunk: back from the newest, stopping at a hole.
    c.cmds(&["DEL", k]);
    for i in 0..5 {
        c.cmds(&["ARRING", k, "5", &format!("v{i}")]);
    }
    c.cmds(&["ARDEL", k, "3"]);
    c.cmds(&["ARRING", k, "3", "new"]);
    assert_eq!(c.cmds(&["ARCOUNT", k]).as_int(), 2);
    assert_eq!(get(&mut c, 0).as_str(), "v4");
    assert_eq!(get(&mut c, 1).as_str(), "new");
    assert!(get(&mut c, 2).is_nil());
    assert_eq!(c.cmds(&["OBJECT", "ENCODING", k]).as_str(), "sliced-array");
    c.cmds(&["DEL", k]);
}
