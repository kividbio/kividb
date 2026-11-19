#![allow(clippy::disallowed_methods)]
// tests/cms.rs
//
// Count-min sketches (`CMS.*`), against what Redis 8.10.2 does:
//
//   * every reply Redis 8.10.2 gave to four randomised sequences of CMS
//     commands, byte for byte, in RESP2 and RESP3, on both I/O loops
//     (`fixtures/redis-8.10.2-cms-replies.json`, made by
//     `scripts/checks/sketch/cms/compare.py --capture`);
//   * an RDB file written by Redis 8.10.2 loads, and its sketches answer as
//     they did there; Redis's `DUMP` restores here, and ours round-trips;
//   * a replica, a snapshot, an AOF replay and an AOF rewrite end with the
//     same sketches as the master (`DEBUG DIGEST-VALUE` of every key);
//   * `COPY`, `RENAME`, `TYPE`, `OBJECT ENCODING`, `MEMORY USAGE`, `WATCH`;
//   * the size limits and per-command limits, and hostile payloads.

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
    let dir = std::env::temp_dir().join(format!("kividb-cms-{tag}-{}", std::process::id()));
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
        serde_json::from_str(include_str!("fixtures/redis-8.10.2-cms-replies.json")).unwrap();
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
    assert!(compared >= 2000, "{compared} replies compared");
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

// ── Redis's files and payloads ───────────────────────────────────────────

/// An RDB file of Redis 8.10.2 with sketches made by `CMS.INITBYDIM` and
/// `CMS.INITBYPROB`, written to past 2^32 − 1, and one with a TTL: each
/// answers here as it did there.
#[test]
fn a_redis_rdb_file_with_sketches_loads() {
    let answers: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/redis-cms-answers.json")).unwrap();
    let rdb = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/redis-8.10.2-cms.rdb");
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
    for key in ["cms:dim", "cms:prob", "cms:narrow", "cms:ttl"] {
        assert_eq!(c.cmd(&["TYPE", key]), b"+CMSk-TYPE\r\n", "{key}");
        let mut args = vec!["CMS.QUERY", key];
        args.extend(probes.iter().map(String::as_str));
        let mut want = format!("*{}\r\n", probes.len()).into_bytes();
        for v in answers[key]["query"].as_array().unwrap() {
            want.extend(format!(":{}\r\n", v.as_i64().unwrap()).as_bytes());
        }
        assert_eq!(shown(&c.cmd(&args)), shown(&want), "{key}: CMS.QUERY");
        let mut want_info = b"*6\r\n".to_vec();
        for pair in answers[key]["info"].as_array().unwrap().chunks(2) {
            want_info.extend(format!("+{}\r\n", pair[0].as_str().unwrap()).as_bytes());
            want_info.extend(format!(":{}\r\n", pair[1].as_i64().unwrap()).as_bytes());
        }
        assert_eq!(
            shown(&c.cmd(&["CMS.INFO", key])),
            shown(&want_info),
            "{key}: CMS.INFO"
        );
    }
    assert_eq!(c.cmd(&["EXPIRETIME", "cms:ttl"]), b":4102444800\r\n");
    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}

/// `DUMP` of Redis 8.10.2 (`CMS.INITBYDIM h 1000 5`, `CMS.INCRBY h hello
/// 1`) restores here; `DUMP` here is Redis's format (module type
/// `CMSk-TYPE`, RDB version 9 and Redis's CRC), which restores here and in
/// Redis; `MIGRATE` moves a sketch between two nodes.
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
    assert_eq!(
        c.cmd(&["CMS.QUERY", "from-redis", "hello", "other"]),
        b"*2\r\n:1\r\n:0\r\n"
    );
    assert_eq!(
        c.cmd(&["CMS.INFO", "from-redis"]),
        b"*6\r\n+width\r\n:1000\r\n+depth\r\n:5\r\n+count\r\n:1\r\n"
    );
    c.cmd(&["CMS.INITBYDIM", "here", "1000", "5"]);
    c.cmd(&["CMS.INCRBY", "here", "hello", "1"]);
    let payload = bulk(&c.cmd(&["DUMP", "here"]));
    assert_eq!(&payload[..10], &redis[..10], "type and module ID");
    assert_eq!(
        &payload[payload.len() - 10..payload.len() - 8],
        &[9, 0],
        "RDB version 9"
    );
    assert_eq!(c.raw(&[b"RESTORE", b"again", b"0", &payload]), b"+OK\r\n");
    assert_eq!(
        c.cmd(&["DEBUG", "DIGEST-VALUE", "again"]),
        c.cmd(&["DEBUG", "DIGEST-VALUE", "from-redis"])
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
    assert_eq!(
        b.conn().cmd(&["CMS.QUERY", "here", "hello"]),
        b"*1\r\n:1\r\n"
    );
    assert_eq!(c.cmd(&["EXISTS", "here"]), b":0\r\n");
    drop((a, b));
    let _ = std::fs::remove_dir_all(&dir_a);
    let _ = std::fs::remove_dir_all(&dir_b);
}

// ── Persistence and replication ──────────────────────────────────────────

/// A deterministic random workload of every write, on keys `w:0` to `w:5`.
fn workload(c: &mut Conn, seed: u64, steps: usize) {
    let mut s = seed;
    let mut next = move || {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        s
    };
    for i in 0..6 {
        let key = format!("w:{i}");
        if i % 2 == 0 {
            c.cmd(&["CMS.INITBYDIM", &key, "40", "4"]);
        } else {
            c.cmd(&["CMS.INITBYPROB", &key, "0.05", "0.0625"]);
        }
    }
    for _ in 0..steps {
        let key = format!("w:{}", next() % 6);
        match next() % 10 {
            0 => {
                let src = format!("w:{}", next() % 6);
                let w = (next() % 5).to_string();
                c.cmd(&["CMS.MERGE", &key, "2", &key, &src, "WEIGHTS", "1", &w]);
            }
            1 => {
                c.cmd(&["CMS.INCRBY", &key, "big", "4294967000"]);
            }
            _ => {
                let items: Vec<String> = (0..1 + next() % 8)
                    .flat_map(|_| [format!("i{}", next() % 500), (next() % 100).to_string()])
                    .collect();
                let mut args = vec!["CMS.INCRBY", key.as_str()];
                args.extend(items.iter().map(String::as_str));
                c.cmd(&args);
            }
        }
    }
}

fn digests(c: &mut Conn) -> Vec<Vec<u8>> {
    (0..6)
        .map(|i| c.cmd(&["DEBUG", "DIGEST-VALUE", &format!("w:{i}")]))
        .collect()
}

/// A replica, a snapshot, an AOF replay and an AOF rewrite all end with
/// the master's sketches, counter for counter.
#[test]
fn replicas_snapshots_and_the_aof_keep_every_counter() {
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
    // The AOF replayed.
    {
        let server = Child::start(&dir, &["--aof"]);
        let mut c = server.conn();
        assert_eq!(digests(&mut c), on_master, "the AOF");
        // Rewritten, then replayed again.
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
    // The snapshot alone.
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
    assert_eq!(c.cmd(&["CMS.INITBYDIM", "g", "1000", "4"]), b"+OK\r\n");
    c.cmd(&["CMS.INCRBY", "g", "a", "3"]);
    assert_eq!(c.cmd(&["TYPE", "g"]), b"+CMSk-TYPE\r\n");
    assert_eq!(c.cmd(&["OBJECT", "ENCODING", "g"]), b"$3\r\nraw\r\n");
    // Its counters and more.
    let usage = String::from_utf8_lossy(&c.cmd(&["MEMORY", "USAGE", "g"])).to_string();
    let usage: u64 = usage.trim_start_matches(':').trim().parse().unwrap();
    assert!(usage > 16_000 && usage < 17_000, "{usage}");
    // COPY (which Redis refuses for a module value) copies it; the copy is
    // its own.
    assert_eq!(c.cmd(&["COPY", "g", "g2"]), b":1\r\n");
    c.cmd(&["CMS.INCRBY", "g2", "a", "1"]);
    assert_eq!(c.cmd(&["CMS.QUERY", "g", "a"]), b"*1\r\n:3\r\n");
    assert_eq!(c.cmd(&["CMS.QUERY", "g2", "a"]), b"*1\r\n:4\r\n");
    assert_eq!(c.cmd(&["RENAME", "g2", "g3"]), b"+OK\r\n");
    assert_eq!(c.cmd(&["CMS.QUERY", "g3", "a"]), b"*1\r\n:4\r\n");
    assert_eq!(c.cmd(&["EXPIRE", "g3", "100"]), b":1\r\n");
    assert_eq!(c.cmd(&["UNLINK", "g3"]), b":1\r\n");
    // Keys are bytes.
    assert_eq!(
        c.raw(&[b"CMS.INITBYDIM", b"\xff\x00k", b"10", b"2"]),
        b"+OK\r\n"
    );
    assert_eq!(
        c.raw(&[b"CMS.INCRBY", b"\xff\x00k", b"\x00", b"2"]),
        b"*1\r\n:2\r\n"
    );
    // ACL category and COMMAND INFO.
    let cat = String::from_utf8_lossy(&c.cmd(&["ACL", "CAT", "cms"])).to_lowercase();
    for name in [
        "cms.initbydim",
        "cms.initbyprob",
        "cms.incrby",
        "cms.query",
        "cms.merge",
        "cms.info",
    ] {
        assert!(cat.contains(name), "{name} in @cms: {cat}");
    }
    let info = String::from_utf8_lossy(&c.cmd(&["COMMAND", "INFO", "CMS.MERGE"])).to_string();
    assert!(
        info.contains("cms.merge") && info.contains("write"),
        "{info}"
    );
    assert!(info.contains(":-4\r\n"), "{info}");
}

/// A write invalidates a `WATCH` on its key, even one that fails, the key
/// there or not, as in Redis; a read does not.
#[test]
fn watch_is_invalidated_as_in_redis() {
    let _serial = serial();
    let port = start(config(kividb::config::IoModel::Tokio));
    let mut a = Conn::connect(port);
    let mut b = Conn::connect(port);
    b.cmd(&["FLUSHALL"]);
    b.cmd(&["CMS.INITBYDIM", "w", "10", "2"]);
    for (other, aborted) in [
        (vec!["CMS.INCRBY", "w", "x", "1"], true),
        (vec!["CMS.INCRBY", "w", "x", "-1"], true),
        (vec!["CMS.INITBYDIM", "w", "10", "2"], true),
        (vec!["CMS.MERGE", "w", "1", "nokey"], true),
        (vec!["CMS.INCRBY", "nokey", "x", "1"], true),
        (vec!["CMS.QUERY", "w", "x"], false),
        (vec!["CMS.INFO", "w"], false),
    ] {
        let key = other[1];
        a.cmd(&["WATCH", key]);
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

const TOO_LARGE: &[u8] =
    b"-CMS: sketch is too large (at most 134217728 counters and 65536 rows)\r\n";
const OVER_BUDGET: &[u8] =
    b"-CMS: command reached the per-command limit; send fewer items per command\r\n";

#[test]
fn size_and_per_command_limits() {
    let _serial = serial();
    let port = start(config(kividb::config::IoModel::Tokio));
    let mut c = Conn::connect(port);
    c.cmd(&["FLUSHALL"]);
    for (width, depth) in [
        ("134217729", "1"),
        ("1", "65537"),
        ("67108864", "3"),
        ("4294967296", "1"),
        ("9223372036854775807", "9223372036854775807"),
    ] {
        assert_eq!(
            shown(&c.cmd(&["CMS.INITBYDIM", "big", width, depth])),
            shown(TOO_LARGE),
            "{width} x {depth}"
        );
    }
    // 2e9 counters wide.
    assert_eq!(
        shown(&c.cmd(&["CMS.INITBYPROB", "big", "1e-9", "0.5"])),
        shown(TOO_LARGE)
    );
    assert_eq!(c.cmd(&["EXISTS", "big"]), b":0\r\n");
    // The deepest sketch: one item is 65,536 counters, three items are
    // within the limit (250,000 counters), four are not; nothing of a
    // command over it is done.
    assert_eq!(c.cmd(&["CMS.INITBYDIM", "deep", "1", "65536"]), b"+OK\r\n");
    assert_eq!(
        c.cmd(&["CMS.INCRBY", "deep", "a", "1", "b", "1", "c", "1"]),
        b"*3\r\n:1\r\n:2\r\n:3\r\n"
    );
    assert_eq!(
        shown(&c.cmd(&["CMS.INCRBY", "deep", "a", "1", "b", "1", "c", "1", "d", "1"])),
        shown(OVER_BUDGET)
    );
    assert_eq!(
        shown(&c.cmd(&["CMS.QUERY", "deep", "a", "b", "c", "d"])),
        shown(OVER_BUDGET)
    );
    assert_eq!(
        c.cmd(&["CMS.INFO", "deep"]),
        b"*6\r\n+width\r\n:1\r\n+depth\r\n:65536\r\n+count\r\n:3\r\n"
    );
    // A merge reads at most 2^23 counters: 9 sources of 2^20 counters are
    // over it, 8 are not.
    assert_eq!(c.cmd(&["CMS.INITBYDIM", "m", "1048576", "1"]), b"+OK\r\n");
    let mut args = vec!["CMS.MERGE", "m", "9"];
    args.extend(std::iter::repeat_n("m", 9));
    assert_eq!(shown(&c.cmd(&args)), shown(OVER_BUDGET));
    let mut args = vec!["CMS.MERGE", "m", "8"];
    args.extend(std::iter::repeat_n("m", 8));
    assert_eq!(c.cmd(&args), b"+OK\r\n");
}

/// Under `maxmemory` with `noeviction`, a sketch that would not fit is
/// refused with `OOM`, before anything is allocated.
#[test]
fn maxmemory_refuses_a_sketch_that_does_not_fit() {
    let _serial = serial();
    let port = start(config(kividb::config::IoModel::Tokio));
    let mut c = Conn::connect(port);
    c.cmd(&["FLUSHALL"]);
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
    let reply = c.cmd(&["CMS.INITBYDIM", "huge", "16777216", "2"]);
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

/// A Redis `DUMP` payload of a sketch of `width × depth` with `counters`
/// as its counters' bytes, sealed with a good CRC.
fn cms_payload(width: u64, depth: u64, counters: &[u8]) -> Vec<u8> {
    fn len(out: &mut Vec<u8>, n: u64) {
        out.push(0x81);
        out.extend_from_slice(&n.to_be_bytes());
    }
    let redis = unhex(REDIS_DUMP);
    // Type byte and module ID, as Redis wrote them.
    let mut out = redis[..10].to_vec();
    for v in [width, depth, 0] {
        out.push(2);
        len(&mut out, v);
    }
    out.push(5);
    len(&mut out, counters.len() as u64);
    out.extend_from_slice(counters);
    out.push(0);
    out.extend_from_slice(&[9, 0]);
    let crc = redis_crc64(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

/// Payloads that claim huge sketches, or whose counters do not match their
/// shape, are refused and make no key; corrupted payloads never take the
/// server down.
#[test]
fn hostile_payloads_are_refused() {
    let _serial = serial();
    let port = start(config(kividb::config::IoModel::Tokio));
    let mut c = Conn::connect(port);
    c.cmd(&["FLUSHALL"]);
    assert_eq!(
        c.raw(&[b"RESTORE", b"ok", b"0", &cms_payload(2, 2, &[0; 16])]),
        b"+OK\r\n"
    );
    for (what, payload) in [
        ("2^40 counters", cms_payload(1 << 20, 1 << 20, &[0; 16])),
        ("width 0", cms_payload(0, 4, &[])),
        ("depth 2^64 - 1", cms_payload(1, u64::MAX, &[0; 16])),
        ("too few counters", cms_payload(4, 4, &[0; 60])),
        ("too many counters", cms_payload(2, 2, &[0; 20])),
    ] {
        let reply = c.raw(&[b"RESTORE", b"hostile", b"0", &payload]);
        assert!(reply.starts_with(b"-"), "{what}: {}", shown(&reply));
        assert_eq!(c.cmd(&["EXISTS", "hostile"]), b":0\r\n", "{what}");
    }
    // Every byte of a good payload changed in turn, CRC resealed.
    let good = cms_payload(3, 2, &[7; 24]);
    let body = good.len() - 8;
    for at in 0..body {
        for flip in [0x01u8, 0x80, 0xff] {
            let mut bad = good[..body].to_vec();
            bad[at] ^= flip;
            let crc = redis_crc64(&bad);
            bad.extend_from_slice(&crc.to_le_bytes());
            let reply = c.raw(&[b"RESTORE", b"fuzz", b"0", &bad, b"REPLACE"]);
            if reply == b"+OK\r\n" {
                let info = c.cmd(&["CMS.INFO", "fuzz"]);
                assert!(
                    info.starts_with(b"*6") || info.starts_with(b"-WRONGTYPE"),
                    "{}",
                    shown(&info)
                );
            }
        }
    }
    assert_eq!(c.cmd(&["CMS.QUERY", "ok", "x"]), b"*1\r\n:0\r\n");
    assert_eq!(c.cmd(&["PING"]), b"+PONG\r\n");
}

const REDIS_DUMP: &str = "078108c4a4f9360f10000243e80205020105c340f88000004e20010000e0ff00e0ff00e051000001e0515ae0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e03800f3ff2be0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e05300ebffb3e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e02b00edff9be0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e07f00efffffe0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0ff00e0bd00010000000f007b16b8c25045f969";

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

/// Room for a sketch is made before the command takes its key, where it
/// is made for every command: the keys evicted for it are deleted in the
/// order of writes, so that a write to one of them by another client meets
/// the master and its replica in the same order. With evictions forced by
/// sketches made and written while other clients write the keys evicted,
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
                c.cmd(&["CMS.INITBYDIM", &key, "20000", "2"]);
                c.cmd(&["CMS.INCRBY", &other, &n, "1"]);
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
        c.cmd(&["CMS.INITBYDIM", "big", "200000", "2"]),
        b"-OOM command not allowed when used memory > 'maxmemory'.\r\n"
    );
    assert_eq!(c.cmd(&["EXISTS", "big"]), b":0\r\n");
    assert_eq!(info_field(&mut c, "stats", "evicted_keys"), "0");
    assert_eq!(c.cmd(&["DBSIZE"]), b":100\r\n");
    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}

/// `CMS.MERGE` reads its sources and writes its destination: a user with
/// read on the sources and write on the destination may merge; without read
/// on a source, or with only read on the destination, it may not.
#[test]
fn merge_reads_its_sources_and_writes_its_destination() {
    let dir = tmp_dir("merge-acl");
    let server = Child::start(&dir, &[]);
    let mut c = server.conn();
    c.cmd(&["CMS.INITBYDIM", "dst", "10", "2"]);
    c.cmd(&["CMS.INITBYDIM", "src1", "10", "2"]);
    c.cmd(&["CMS.INITBYDIM", "src2", "10", "2"]);
    let merge = |spec: &[&str]| {
        let mut args = vec!["ACL", "SETUSER", "u", "reset", "on", ">pw", "+@all"];
        args.extend_from_slice(spec);
        assert_eq!(c_cmd(server.port, &args), b"+OK\r\n");
        let mut u = server.conn();
        assert_eq!(u.cmd(&["AUTH", "u", "pw"]), b"+OK\r\n");
        u.cmd(&["CMS.MERGE", "dst", "2", "src1", "src2"])
    };
    assert_eq!(merge(&["%R~src*", "%W~dst"]), b"+OK\r\n");
    assert_eq!(merge(&["%R~src*", "%RW~dst"]), b"+OK\r\n");
    for spec in [
        &["%R~src1", "%W~dst"][..],
        &["%W~src*", "%W~dst"][..],
        &["%R~src*", "%R~dst"][..],
        &["%R~src*"][..],
    ] {
        let reply = merge(spec);
        assert!(reply.starts_with(b"-NOPERM"), "{spec:?}: {}", shown(&reply));
    }
    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}

/// One command on a connection of its own.
fn c_cmd(port: u16, args: &[&str]) -> Vec<u8> {
    Conn::connect(port).cmd(args)
}
