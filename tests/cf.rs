#![allow(clippy::disallowed_methods)]
// tests/cf.rs
//
// Cuckoo filters (`CF.*`), against what Redis 8.10.2 does:
//
//   * every reply Redis 8.10.2 gave to four randomised sequences of CF
//     commands, byte for byte, in RESP2 and RESP3, on both I/O loops: the
//     filters are Redis's byte for byte, so `CF.SCANDUMP` chunks, counts and
//     `CF.INFO` included (`fixtures/redis-8.10.2-cf-replies.json`, made by
//     `scripts/checks/sketch/cf/compare.py --capture`);
//   * an RDB file written by Redis 8.10.2 loads, and its filters answer as
//     they did there; Redis's `DUMP` restores here, and ours round-trips;
//   * a replica with other `cf-*` settings, a snapshot, an AOF replay and an
//     AOF rewrite end with the master's filters (`DEBUG DIGEST-VALUE`);
//   * `COPY`, `RENAME`, `TYPE`, `OBJECT ENCODING`, `MEMORY USAGE`, `WATCH`;
//   * the size limits and the per-command limit, and hostile payloads.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

mod common;
use common::start_verified;

fn serial() -> MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

// ── A client that keeps replies as bytes ─────────────────────────────────

struct Conn {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Conn {
    fn connect(port: u16) -> Conn {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match TcpStream::connect(("127.0.0.1", port)) {
                Ok(stream) => {
                    stream
                        .set_read_timeout(Some(Duration::from_secs(30)))
                        .unwrap();
                    return Conn {
                        stream,
                        buf: Vec::new(),
                    };
                }
                Err(_) if Instant::now() < deadline => thread::sleep(Duration::from_millis(50)),
                Err(e) => panic!("could not connect to port {port}: {e}"),
            }
        }
    }

    fn raw(&mut self, args: &[&[u8]]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            out.extend_from_slice(a);
            out.extend_from_slice(b"\r\n");
        }
        self.stream.write_all(&out).unwrap();
        loop {
            if let Some(end) = reply_end(&self.buf, 0) {
                return self.buf.drain(..end).collect();
            }
            let mut chunk = [0u8; 65536];
            let n = self.stream.read(&mut chunk).expect("a reply");
            assert!(n > 0, "connection closed");
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }

    fn cmd(&mut self, args: &[&str]) -> Vec<u8> {
        let args: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
        self.raw(&args)
    }
}

/// Where the RESP value starting at `at` ends, if all of it is there.
fn reply_end(buf: &[u8], at: usize) -> Option<usize> {
    let line_end = at + buf.get(at..)?.windows(2).position(|w| w == b"\r\n")?;
    let head = std::str::from_utf8(&buf[at + 1..line_end]).ok()?;
    let after = line_end + 2;
    match buf[at] {
        b'+' | b'-' | b':' | b',' | b'#' | b'_' | b'(' => Some(after),
        b'$' | b'=' | b'!' => {
            let n: i64 = head.parse().ok()?;
            if n < 0 {
                return Some(after);
            }
            let end = after + n as usize + 2;
            (buf.len() >= end).then_some(end)
        }
        b'*' | b'~' | b'>' | b'%' | b'|' => {
            let n: i64 = head.parse().ok()?;
            let n = if matches!(buf[at], b'%' | b'|') {
                2 * n
            } else {
                n
            };
            let mut p = after;
            for _ in 0..n.max(0) {
                p = reply_end(buf, p)?;
            }
            Some(p)
        }
        other => panic!("not RESP: {other:#x}"),
    }
}

fn shown(bytes: &[u8]) -> String {
    bytes.escape_ascii().to_string()
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

/// The bulk string a reply is.
fn bulk(reply: &[u8]) -> Vec<u8> {
    assert_eq!(reply[0], b'$', "{}", shown(reply));
    let nl = reply.iter().position(|&b| b == b'\n').unwrap();
    reply[nl + 1..reply.len() - 2].to_vec()
}

// ── Servers ──────────────────────────────────────────────────────────────

fn config(io_model: kividb::config::IoModel) -> kividb::config::Config {
    kividb::config::Config {
        bind: "127.0.0.1".to_string(),
        threads: 2,
        aof: false,
        databases: 16,
        loglevel: "error".to_string(),
        io_model,
        configfile: "./kividb.conf".to_string(),
        ..Default::default()
    }
}

/// A server in this process with `config`, on a port of its own.
fn start(mut config: kividb::config::Config) -> u16 {
    let port = common::free_port();
    config.port = port;
    thread::spawn(move || kividb::run_for_test(config));
    wait_until("the server to start", || {
        TcpStream::connect(("127.0.0.1", port)).is_ok_and(|mut c| {
            c.write_all(b"PING\r\n").is_ok() && {
                let mut b = [0u8; 7];
                c.read_exact(&mut b).is_ok() && &b == b"+PONG\r\n"
            }
        })
    });
    port
}

fn tmp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("kividb-cf-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn wait_until(what: &str, mut pred: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if pred() {
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("timed out waiting for {what}");
}

/// A `kividb` process in `dir`, killed when dropped.
struct Child {
    port: u16,
    child: std::process::Child,
}

impl Child {
    fn start(dir: &std::path::Path, extra: &[&str]) -> Child {
        let (child, port) = start_verified(|port| {
            std::process::Command::new(env!("CARGO_BIN_EXE_kividb"))
                .current_dir(dir)
                .args(["--port", &port.to_string(), "--bind", "127.0.0.1"])
                .args(["--threads", "2", "--loglevel", "warning"])
                .args(["--configfile", "kividb.conf", "--dir"])
                .arg(dir)
                .args(extra)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .spawn()
                .expect("could not start kividb")
        });
        let child = Child { port, child };
        wait_until("kividb to answer", || {
            TcpStream::connect(("127.0.0.1", port)).is_ok_and(|mut c| {
                c.write_all(b"PING\r\n").is_ok() && {
                    let mut b = [0u8; 7];
                    c.read_exact(&mut b).is_ok() && &b == b"+PONG\r\n"
                }
            })
        });
        child
    }

    fn conn(&self) -> Conn {
        Conn::connect(self.port)
    }
}

impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// ── Redis's replies ──────────────────────────────────────────────────────

/// Every reply of the captures, in order: each sequence in RESP2 on
/// database 0, then in RESP3 on database 1, from an empty database. A case
/// `#WATCH key cmd...` is `WATCH key`, `cmd` from a second connection, then
/// `MULTI`, `PING`, `EXEC`: its reply is the `EXEC`'s.
fn replay_captures(port: u16) {
    let golden: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/redis-8.10.2-cf-replies.json")).unwrap();
    let mut failures = Vec::new();
    let mut compared = 0;
    for sequence in golden["sequences"].as_array().unwrap() {
        for (resp3, db) in [(false, "0"), (true, "1")] {
            let mut c = Conn::connect(port);
            let mut other = Conn::connect(port);
            for conn in [&mut c, &mut other] {
                if resp3 {
                    conn.cmd(&["HELLO", "3"]);
                }
                assert_eq!(conn.cmd(&["SELECT", db]), b"+OK\r\n");
            }
            assert_eq!(c.cmd(&["FLUSHDB"]), b"+OK\r\n");
            default_settings(&mut c);
            for case in sequence["cases"].as_array().unwrap() {
                let args: Vec<Vec<u8>> = case["args"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|a| unhex(a.as_str().unwrap()))
                    .collect();
                let refs: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();
                let got = if refs[0] == b"#WATCH" {
                    c.raw(&[b"WATCH", refs[1]]);
                    other.raw(&refs[2..]);
                    c.cmd(&["MULTI"]);
                    c.cmd(&["PING"]);
                    c.cmd(&["EXEC"])
                } else if refs[0] == b"#DUMPLOAD" {
                    dumpload(&mut c, refs[1])
                } else {
                    c.raw(&refs)
                };
                let field = if resp3 { "resp3" } else { "resp2" };
                let want = unhex(case[field].as_str().unwrap());
                compared += 1;
                if got != want {
                    let shown_args: Vec<String> = args.iter().map(|a| shown(a)).collect();
                    failures.push(format!(
                        "{} ({field})\n  want {}\n  got  {}",
                        shown_args.join(" "),
                        shown(&want),
                        shown(&got)
                    ));
                }
            }
        }
    }
    assert!(compared >= 1600, "{compared} replies compared");
    assert!(
        failures.is_empty(),
        "{} of {compared} differ:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn replies_are_redis_8_10_2_on_tokio() {
    let _serial = serial();
    replay_captures(start(config(kividb::config::IoModel::Tokio)));
}

/// The other I/O loop: io_uring where the system has it.
#[test]
fn replies_are_redis_8_10_2_on_the_default_loop() {
    let _serial = serial();
    replay_captures(start(config(kividb::config::IoModel::Auto)));
}

/// `#DUMPLOAD key` of the captures: `CF.SCANDUMP` of the whole filter, each
/// chunk `CF.LOADCHUNK`ed into `copy`, then `CF.INFO copy`: every reply
/// concatenated.
fn dumpload(c: &mut Conn, key: &[u8]) -> Vec<u8> {
    let mut replies = c.cmd(&["DEL", "copy"]);
    replies.clear();
    let mut iter = b"0".to_vec();
    for _ in 0..64 {
        let r = c.raw(&[b"CF.SCANDUMP", key, &iter]);
        replies.extend_from_slice(&r);
        if !r.starts_with(b"*2") {
            break;
        }
        let text = r.splitn(4, |&b| b == b'\n').collect::<Vec<_>>();
        let next = text[1][1..text[1].len() - 1].to_vec();
        if next == b"0" {
            break;
        }
        let chunk = &text[3][..text[3].len() - 2];
        replies.extend_from_slice(&c.raw(&[b"CF.LOADCHUNK", b"copy", &next, chunk]));
        iter = next;
    }
    replies.extend_from_slice(&c.cmd(&["CF.INFO", "copy"]));
    replies
}

fn default_settings(c: &mut Conn) {
    for (name, value) in [
        ("cf-bucket-size", "2"),
        ("cf-initial-size", "1024"),
        ("cf-expansion-factor", "1"),
        ("cf-max-iterations", "20"),
        ("cf-max-expansions", "32"),
    ] {
        assert_eq!(c.cmd(&["CONFIG", "SET", name, value]), b"+OK\r\n", "{name}");
    }
}

// ── Redis's files and payloads ───────────────────────────────────────────

/// An RDB file of Redis 8.10.2 with Cuckoo filters (grown, fixed, made by
/// `CF.ADD`, one with a TTL, all with deletes): each answers here as it did
/// there.
#[test]
fn a_redis_rdb_file_with_filters_loads() {
    let answers: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/redis-cf-answers.json")).unwrap();
    let rdb =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/redis-8.10.2-cf.rdb");
    let dir = tmp_dir("rdb");
    let preload = format!("rdb:{}", rdb.display());
    let server = Child::start(&dir, &["--preload-file", &preload]);
    let mut c = server.conn();
    let probes: Vec<String> = answers["_probes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p.as_str().unwrap().to_string())
        .collect();
    for key in ["cf:grown", "cf:fixed", "cf:ttl", "cf:auto"] {
        assert_eq!(c.cmd(&["TYPE", key]), b"+MBbloomCF\r\n", "{key}");
        let mut args = vec!["CF.MEXISTS", key];
        args.extend(probes.iter().map(String::as_str));
        let mut want = format!("*{}\r\n", probes.len()).into_bytes();
        for v in answers[key]["mexists"].as_array().unwrap() {
            want.extend(format!(":{}\r\n", v.as_i64().unwrap()).as_bytes());
        }
        assert_eq!(shown(&c.cmd(&args)), shown(&want), "{key}: CF.MEXISTS");
        let mut want_info = b"*16\r\n".to_vec();
        for pair in answers[key]["info"].as_array().unwrap().chunks(2) {
            want_info.extend(format!("+{}\r\n", pair[0].as_str().unwrap()).as_bytes());
            want_info.extend(format!(":{}\r\n", pair[1].as_i64().unwrap()).as_bytes());
        }
        assert_eq!(
            shown(&c.cmd(&["CF.INFO", key])),
            shown(&want_info),
            "{key}: CF.INFO"
        );
    }
    assert_eq!(c.cmd(&["EXPIRETIME", "cf:ttl"]), b":4102444800\r\n");
    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}

/// `DUMP` of Redis 8.10.2 (a filter of two sub-filters) restores here and
/// is the filter KiviDB makes from the same commands; `DUMP` here is Redis's
/// format; `MIGRATE` moves a filter between two nodes.
#[test]
fn dump_restore_and_migrate() {
    let redis = unhex(REDIS_DUMP);
    let (dir_a, dir_b) = (tmp_dir("dump-a"), tmp_dir("dump-b"));
    let a = Child::start(&dir_a, &[]);
    let b = Child::start(&dir_b, &[]);
    let mut c = a.conn();
    assert_eq!(
        c.raw(&[b"RESTORE", b"from-redis", b"0", &redis]),
        b"+OK\r\n"
    );
    c.cmd(&[
        "CF.RESERVE",
        "here",
        "24",
        "BUCKETSIZE",
        "3",
        "EXPANSION",
        "4",
        "MAXITERATIONS",
        "7",
    ]);
    for i in 0..40 {
        c.cmd(&["CF.ADD", "here", &format!("x{i}")]);
    }
    c.cmd(&["CF.DEL", "here", "x1"]);
    assert_eq!(
        c.cmd(&["DEBUG", "DIGEST-VALUE", "here"]),
        c.cmd(&["DEBUG", "DIGEST-VALUE", "from-redis"])
    );
    let payload = bulk(&c.cmd(&["DUMP", "here"]));
    assert_eq!(&payload[..10], &redis[..10], "type and module ID");
    assert_eq!(c.raw(&[b"RESTORE", b"again", b"0", &payload]), b"+OK\r\n");
    assert_eq!(
        c.cmd(&["DEBUG", "DIGEST-VALUE", "again"]),
        c.cmd(&["DEBUG", "DIGEST-VALUE", "here"])
    );
    assert_eq!(
        c.cmd(&[
            "MIGRATE",
            "127.0.0.1",
            &b.port.to_string(),
            "here",
            "0",
            "5000"
        ]),
        b"+OK\r\n"
    );
    assert_eq!(b.conn().cmd(&["CF.EXISTS", "here", "x5"]), b":1\r\n");
    drop((a, b));
    let _ = std::fs::remove_dir_all(&dir_a);
    let _ = std::fs::remove_dir_all(&dir_b);
}

// ── Persistence and replication ──────────────────────────────────────────

/// A deterministic random workload on keys `w:0` to `w:5`: adds that grow
/// filters, deletes that compact them, inserts, filters made from the
/// settings.
fn workload(c: &mut Conn, seed: u64, steps: usize) {
    let mut s = seed;
    let mut next = move || {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        s
    };
    for i in 0..3 {
        let key = format!("w:{i}");
        let bs = (1 + i).to_string();
        c.cmd(&["CF.RESERVE", &key, "8", "BUCKETSIZE", &bs, "EXPANSION", "2"]);
    }
    for _ in 0..steps {
        let key = format!("w:{}", next() % 6);
        let item = format!("i{}", next() % 300);
        match next() % 10 {
            0..=4 => {
                c.cmd(&["CF.ADD", &key, &item]);
            }
            5 | 6 => {
                c.cmd(&["CF.DEL", &key, &item]);
            }
            7 => {
                let other = format!("i{}", next() % 300);
                c.cmd(&["CF.INSERTNX", &key, "ITEMS", &item, &other]);
            }
            8 => {
                c.cmd(&["CF.INSERT", &key, "CAPACITY", "16", "ITEMS", &item]);
            }
            _ => {
                c.cmd(&["CF.COMPACT", &key]);
            }
        }
    }
}

fn digests(c: &mut Conn) -> Vec<Vec<u8>> {
    (0..6)
        .map(|i| c.cmd(&["DEBUG", "DIGEST-VALUE", &format!("w:{i}")]))
        .collect()
}

/// A replica with other `cf-*` settings, a snapshot, an AOF replay and an
/// AOF rewrite all end with the master's filters, byte for byte: a filter
/// made from the master's settings is sent naming them.
#[test]
fn replicas_snapshots_and_the_aof_keep_every_byte() {
    let (dir, replica_dir) = (tmp_dir("aof"), tmp_dir("replica"));
    let on_master = {
        let master = Child::start(&dir, &["--aof"]);
        let replica = Child::start(
            &replica_dir,
            &["--replicaof", "127.0.0.1", &master.port.to_string()],
        );
        let mut m = master.conn();
        let mut r = replica.conn();
        wait_until("the replica to sync", || {
            String::from_utf8_lossy(&r.cmd(&["INFO", "replication"]))
                .contains("master_link_status:up")
        });
        // The master's settings make the filters CF.ADD makes; the
        // replica's differ, and must not matter.
        for (name, value) in [("cf-bucket-size", "3"), ("cf-initial-size", "40")] {
            assert_eq!(m.cmd(&["CONFIG", "SET", name, value]), b"+OK\r\n");
        }
        for (name, value) in [("cf-bucket-size", "1"), ("cf-max-iterations", "3")] {
            assert_eq!(r.cmd(&["CONFIG", "SET", name, value]), b"+OK\r\n");
        }
        workload(&mut m, 0x9e37_79b9_7f4a_7c15, 3000);
        let on_master = digests(&mut m);
        assert!(on_master.iter().all(|d| d.len() > 8));
        assert_eq!(m.cmd(&["SET", "last", "1"]), b"+OK\r\n");
        wait_until("the replica to catch up", || {
            r.cmd(&["GET", "last"]) == b"$1\r\n1\r\n"
        });
        assert_eq!(digests(&mut r), on_master, "the replica");
        assert_eq!(m.cmd(&["SAVE"]), b"+OK\r\n");
        on_master
    };
    {
        let server = Child::start(&dir, &["--aof"]);
        let mut c = server.conn();
        assert_eq!(digests(&mut c), on_master, "the AOF");
        c.cmd(&["BGREWRITEAOF"]);
        wait_until("the rewrite", || {
            String::from_utf8_lossy(&c.cmd(&["INFO", "persistence"]))
                .contains("aof_rewrite_in_progress:0")
        });
        thread::sleep(Duration::from_millis(300));
    }
    {
        let server = Child::start(&dir, &["--aof"]);
        let mut c = server.conn();
        assert_eq!(digests(&mut c), on_master, "the rewritten AOF");
    }
    {
        let server = Child::start(&dir, &[]);
        let mut c = server.conn();
        assert_eq!(digests(&mut c), on_master, "the snapshot");
    }
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&replica_dir);
}

// ── Generic commands ─────────────────────────────────────────────────────

#[test]
fn generic_commands() {
    let _serial = serial();
    let port = start(config(kividb::config::IoModel::Tokio));
    let mut c = Conn::connect(port);
    c.cmd(&["FLUSHALL"]);
    default_settings(&mut c);
    assert_eq!(c.cmd(&["CF.RESERVE", "g", "1000"]), b"+OK\r\n");
    c.cmd(&["CF.ADD", "g", "a"]);
    assert_eq!(c.cmd(&["TYPE", "g"]), b"+MBbloomCF\r\n");
    assert_eq!(c.cmd(&["OBJECT", "ENCODING", "g"]), b"$3\r\nraw\r\n");
    let usage = String::from_utf8_lossy(&c.cmd(&["MEMORY", "USAGE", "g"])).to_string();
    let usage: u64 = usage.trim_start_matches(':').trim().parse().unwrap();
    assert!(usage > 1024 && usage < 2000, "{usage}");
    assert_eq!(c.cmd(&["COPY", "g", "g2"]), b":1\r\n");
    c.cmd(&["CF.ADD", "g2", "b"]);
    assert_eq!(c.cmd(&["CF.EXISTS", "g", "b"]), b":0\r\n");
    assert_eq!(c.cmd(&["CF.EXISTS", "g2", "b"]), b":1\r\n");
    assert_eq!(c.cmd(&["RENAME", "g2", "g3"]), b"+OK\r\n");
    assert_eq!(c.cmd(&["CF.EXISTS", "g3", "a"]), b":1\r\n");
    assert_eq!(c.cmd(&["UNLINK", "g3"]), b":1\r\n");
    // The settings, as CONFIG GET lists them.
    let got = String::from_utf8_lossy(&c.cmd(&["CONFIG", "GET", "cf-*"])).to_string();
    for name in [
        "cf-bucket-size",
        "cf-initial-size",
        "cf-expansion-factor",
        "cf-max-iterations",
        "cf-max-expansions",
    ] {
        assert!(got.contains(name), "{name}: {got}");
    }
    let cat = String::from_utf8_lossy(&c.cmd(&["ACL", "CAT", "cuckoo"])).to_lowercase();
    for name in [
        "cf.reserve",
        "cf.add",
        "cf.loadchunk",
        "cf.compact",
        "cf.info",
    ] {
        assert!(cat.contains(name), "{name} in @cuckoo: {cat}");
    }
}

#[test]
fn watch_is_invalidated_as_in_redis() {
    let _serial = serial();
    let port = start(config(kividb::config::IoModel::Tokio));
    let mut a = Conn::connect(port);
    let mut b = Conn::connect(port);
    b.cmd(&["FLUSHALL"]);
    default_settings(&mut b);
    b.cmd(&["CF.RESERVE", "w", "100"]);
    b.cmd(&["CF.ADD", "w", "a"]);
    for (other, aborted) in [
        (vec!["CF.ADD", "w", "x"], true),
        (vec!["CF.ADDNX", "w", "a"], true),
        (vec!["CF.DEL", "w", "nothere"], true),
        (vec!["CF.RESERVE", "w", "10"], true),
        (vec!["CF.INSERTNX", "w", "NOCREATE", "ITEMS", "a"], true),
        (vec!["CF.INSERT", "w", "CAPACITY", "0", "ITEMS", "a"], false),
        (vec!["CF.EXISTS", "w", "a"], false),
        (vec!["CF.INFO", "w"], false),
    ] {
        a.cmd(&["WATCH", "w"]);
        b.cmd(&other);
        a.cmd(&["MULTI"]);
        a.cmd(&["PING"]);
        let exec = a.cmd(&["EXEC"]);
        assert_eq!(
            exec.starts_with(b"*-1"),
            aborted,
            "{other:?}: {}",
            shown(&exec)
        );
    }
}

// ── Limits ───────────────────────────────────────────────────────────────

const TOO_LARGE: &[u8] = b"-ERR filter is too large\r\n";

fn header(items: u64, buckets: u64, filters: u64, bucket_size: u16, expansion: u16) -> Vec<u8> {
    let mut h = Vec::new();
    h.extend_from_slice(&items.to_le_bytes());
    h.extend_from_slice(&buckets.to_le_bytes());
    h.extend_from_slice(&0u64.to_le_bytes());
    h.extend_from_slice(&filters.to_le_bytes());
    h.extend_from_slice(&bucket_size.to_le_bytes());
    h.extend_from_slice(&5u16.to_le_bytes());
    h.extend_from_slice(&expansion.to_le_bytes());
    h
}

#[test]
fn size_and_per_command_limits() {
    let _serial = serial();
    let port = start(config(kividb::config::IoModel::Tokio));
    let mut c = Conn::connect(port);
    c.cmd(&["FLUSHALL"]);
    default_settings(&mut c);
    // 8,388,608 buckets of 255: 2 GiB.
    assert_eq!(
        shown(&c.cmd(&["CF.RESERVE", "big", "1073741824", "BUCKETSIZE", "255"])),
        shown(TOO_LARGE)
    );
    // Headers of 1 TiB, and of 40 sub-filters growing 32768-fold.
    for h in [header(1, 1 << 40, 1, 1, 1), header(1, 4, 40, 1, 32768)] {
        assert_eq!(
            shown(&c.raw(&[b"CF.LOADCHUNK", b"big", b"1", &h])),
            shown(TOO_LARGE)
        );
    }
    // 100 sub-filters, as Redis: not a filter there can be.
    assert_eq!(
        c.raw(&[b"CF.LOADCHUNK", b"big", b"1", &header(1, 4, 100, 1, 1)]),
        b"-Couldn't create filter!\r\n"
    );
    assert_eq!(c.cmd(&["EXISTS", "big"]), b":0\r\n");
    // A filter whose adds may cost much: 8,000,000 over the items.
    assert_eq!(
        c.cmd(&[
            "CF.RESERVE",
            "slow",
            "510",
            "BUCKETSIZE",
            "255",
            "MAXITERATIONS",
            "65535"
        ]),
        b"+OK\r\n"
    );
    let items: Vec<String> = (0..300).map(|i| format!("i{i}")).collect();
    let mut args = vec!["CF.INSERT", "slow", "ITEMS"];
    args.extend(items.iter().map(String::as_str));
    let reply = c.cmd(&args);
    assert!(
        reply.starts_with(b"-ERR command reached the per-command limit"),
        "{}",
        shown(&reply)
    );
    assert_eq!(c.cmd(&["CF.ADD", "slow", "one"]), b":1\r\n");
}

#[test]
fn maxmemory_refuses_a_filter_that_does_not_fit() {
    let _serial = serial();
    let port = start(config(kividb::config::IoModel::Tokio));
    let mut c = Conn::connect(port);
    c.cmd(&["FLUSHALL"]);
    default_settings(&mut c);
    let info = String::from_utf8_lossy(&c.cmd(&["INFO", "memory"])).to_string();
    let used: u64 = info
        .lines()
        .find_map(|l| l.strip_prefix("used_memory:"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let limit = (used + (8 << 20)).to_string();
    assert_eq!(c.cmd(&["CONFIG", "SET", "maxmemory", &limit]), b"+OK\r\n");
    assert_eq!(
        c.cmd(&["CONFIG", "SET", "maxmemory-policy", "noeviction"]),
        b"+OK\r\n"
    );
    let reply = c.cmd(&["CF.RESERVE", "huge", "67108864", "BUCKETSIZE", "1"]);
    c.cmd(&["CONFIG", "SET", "maxmemory", "0"]);
    assert!(reply.starts_with(b"-OOM"), "{}", shown(&reply));
    assert_eq!(c.cmd(&["EXISTS", "huge"]), b":0\r\n");
}

/// Redis's CRC64 (Jones polynomial, reflected), which seals a Redis `DUMP`.
fn redis_crc64(data: &[u8]) -> u64 {
    let mut crc = 0u64;
    for &byte in data {
        crc ^= u64::from(byte);
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0x95ac_9329_ac4b_c9b5
            } else {
                crc >> 1
            };
        }
    }
    crc
}

/// Corrupted `RESTORE` payloads and `CF.LOADCHUNK` headers never take the
/// server down, nor make a filter over the limits.
#[test]
fn hostile_payloads_are_refused() {
    let _serial = serial();
    let port = start(config(kividb::config::IoModel::Tokio));
    let mut c = Conn::connect(port);
    c.cmd(&["FLUSHALL"]);
    default_settings(&mut c);
    let good = unhex(REDIS_DUMP);
    let body = good.len() - 8;
    for at in 0..body {
        for flip in [0x01u8, 0x80, 0xff] {
            let mut bad = good[..body].to_vec();
            bad[at] ^= flip;
            let crc = redis_crc64(&bad);
            bad.extend_from_slice(&crc.to_le_bytes());
            let reply = c.raw(&[b"RESTORE", b"fuzz", b"0", &bad, b"REPLACE"]);
            if reply == b"+OK\r\n" {
                let info = c.cmd(&["CF.INFO", "fuzz"]);
                assert!(
                    info.starts_with(b"*16") || info.starts_with(b"-WRONGTYPE"),
                    "{}",
                    shown(&info)
                );
            }
        }
    }
    let mut seed = 0x2545_f491_4f6c_dd1du64;
    for _ in 0..2000 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let mut h = header(5, 4, 2, 2, 2);
        let at = seed as usize % h.len();
        h[at] = (seed >> 32) as u8;
        c.cmd(&["DEL", "h"]);
        let reply = c.raw(&[b"CF.LOADCHUNK", b"h", b"1", &h]);
        if reply == b"+OK\r\n" {
            let info = c.cmd(&["CF.INFO", "h"]);
            assert!(info.starts_with(b"*16"), "{}", shown(&info));
        }
    }
    assert_eq!(c.cmd(&["PING"]), b"+PONG\r\n");
}

const REDIS_DUMP: &str = "07813016e5a289821404020202080227020102030207020402080518ce62a000e214c07bc5b2287cfc65b328da864700003f12ac022005c33c4060010000e00400005c600d004d6005e006000036e0050f0ac200001d0000b5fbdef830401803a10000cf40070b00ea14007200005d00001cb1200d0000000f0038111f3ba6bb5b4f";

// ── maxmemory: room is made before a command takes its keys ─────────────

/// A field of `INFO section`.
fn info_field(c: &mut Conn, section: &str, field: &str) -> String {
    let prefix = format!("{field}:");
    String::from_utf8_lossy(&c.cmd(&["INFO", section]))
        .lines()
        .find_map(|l| l.strip_prefix(&prefix).map(|v| v.trim().to_string()))
        .unwrap_or_default()
}

/// The keys of the selected database, sorted.
fn all_keys(c: &mut Conn) -> Vec<String> {
    let reply = c.cmd(&["KEYS", "*"]);
    let mut keys: Vec<String> = String::from_utf8_lossy(&reply)
        .split("\r\n")
        .skip(1)
        .filter(|l| !l.is_empty() && !l.starts_with('$'))
        .map(str::to_string)
        .collect();
    keys.sort();
    keys
}

/// Room for a filter is made before the command takes its key, where it
/// is made for every command: the keys evicted for it are deleted in the
/// order of writes, so that a write to one of them by another client meets
/// the master and its replica in the same order. With evictions forced by
/// filters made and written while other clients write the keys evicted,
/// the replica ends with every key the master has, and nothing else, each
/// alike (`DEBUG DIGEST-VALUE`).
#[test]
fn room_is_made_before_the_command_takes_its_keys() {
    let (dir, replica_dir) = (tmp_dir("room-order"), tmp_dir("room-order-replica"));
    let master = Child::start(&dir, &["--maxmemory-policy", "allkeys-lru"]);
    let replica = Child::start(
        &replica_dir,
        &["--replicaof", "127.0.0.1", &master.port.to_string()],
    );
    let mut m = master.conn();
    let mut r = replica.conn();
    wait_until("the replica to sync", || {
        info_field(&mut r, "replication", "master_link_status") == "up"
    });
    let used: u64 = info_field(&mut m, "memory", "used_memory").parse().unwrap();
    let limit = (used + 4_000_000).to_string();
    assert_eq!(m.cmd(&["CONFIG", "SET", "maxmemory", &limit]), b"+OK\r\n");
    let stop = std::sync::atomic::AtomicBool::new(false);
    let value = "v".repeat(8_000);
    thread::scope(|s| {
        s.spawn(|| {
            let mut c = master.conn();
            let mut i = 0u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let key = format!("f{}", i % 24);
                let other = format!("f{}", (i + 7) % 24);
                let n = i.to_string();
                c.cmd(&["DEL", &key]);
                c.cmd(&["CF.RESERVE", &key, "100000"]);
                c.cmd(&["CF.ADD", "grown", &n]);
                c.cmd(&["CF.INSERT", &other, "CAPACITY", "1000", "ITEMS", &n]);
                i += 1;
            }
        });
        for t in 0..2 {
            let (value, stop, master) = (&value, &stop, &master);
            s.spawn(move || {
                let mut c = master.conn();
                let mut i = 0u64;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let key = format!("s{}", (i * 7 + t) % 300);
                    if i.is_multiple_of(5) {
                        c.cmd(&["DEL", &key]);
                    } else {
                        c.cmd(&["SET", &key, value]);
                    }
                    i += 1;
                }
            });
        }
        thread::sleep(Duration::from_secs(4));
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
    });
    let evicted: u64 = info_field(&mut m, "stats", "evicted_keys").parse().unwrap();
    assert!(evicted > 0, "room was made by evicting");
    assert_eq!(m.cmd(&["CONFIG", "SET", "maxmemory", "0"]), b"+OK\r\n");
    assert_eq!(m.cmd(&["SET", "last", "1"]), b"+OK\r\n");
    wait_until("the replica to have the last write", || {
        r.cmd(&["GET", "last"]) == b"$1\r\n1\r\n"
    });
    let keys = all_keys(&mut m);
    assert_eq!(all_keys(&mut r), keys);
    for key in &keys {
        assert_eq!(
            r.cmd(&["DEBUG", "DIGEST-VALUE", key]),
            m.cmd(&["DEBUG", "DIGEST-VALUE", key]),
            "{key}"
        );
    }
    drop((master, replica));
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&replica_dir);
}

/// A command that finds no room when it runs is refused with `OOM`: it
/// evicts nothing itself, as room is made only before a command takes its
/// keys. (Room for it is not asked for first here, by a hook of debug
/// builds: as if what was made had been taken meanwhile.)
#[test]
#[cfg(debug_assertions)]
fn a_command_that_finds_no_room_evicts_nothing() {
    let dir = tmp_dir("room-none");
    let (child, port) = start_verified(|port| {
        std::process::Command::new(env!("CARGO_BIN_EXE_kividb"))
            .current_dir(&dir)
            .env("KIVIDB_TEST_NO_ROOM_BEFORE", "1")
            .args(["--port", &port.to_string(), "--bind", "127.0.0.1"])
            .args(["--threads", "2", "--loglevel", "warning"])
            .args(["--configfile", "kividb.conf", "--dir"])
            .arg(&dir)
            .args(["--maxmemory-policy", "allkeys-lru"])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .spawn()
            .expect("could not start kividb")
    });
    let server = Child { port, child };
    wait_until("kividb to answer", || {
        TcpStream::connect(("127.0.0.1", port)).is_ok()
    });
    let mut c = server.conn();
    let value = "v".repeat(10_000);
    for i in 0..100 {
        assert_eq!(c.cmd(&["SET", &format!("s{i}"), &value]), b"+OK\r\n");
    }
    let used: u64 = info_field(&mut c, "memory", "used_memory").parse().unwrap();
    let limit = (used + 500_000).to_string();
    assert_eq!(c.cmd(&["CONFIG", "SET", "maxmemory", &limit]), b"+OK\r\n");
    // More than is left.
    assert_eq!(
        c.cmd(&["CF.RESERVE", "big", "2000000"]),
        b"-OOM command not allowed when used memory > 'maxmemory'.\r\n"
    );
    assert_eq!(c.cmd(&["EXISTS", "big"]), b":0\r\n");
    assert_eq!(info_field(&mut c, "stats", "evicted_keys"), "0");
    assert_eq!(c.cmd(&["DBSIZE"]), b":100\r\n");
    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}
