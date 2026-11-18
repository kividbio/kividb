// tests/topk.rs
//
// Top-K sketches (`TOPK.*`), against what Redis 8.10.2 does:
//
//   * every reply Redis 8.10.2 gave to four randomised sequences of TOPK
//     commands on sketches with a decay of 1 (where nothing is left to
//     chance), byte for byte, in RESP2 and RESP3, on both I/O loops
//     (`fixtures/redis-8.10.2-topk-replies.json`, made by
//     `scripts/checks/sketch/topk/compare.py --capture`);
//   * an RDB file written by Redis 8.10.2 loads, and its sketches answer as
//     they did there; Redis's `DUMP` restores here, and ours round-trips;
//   * a replica, a snapshot, an AOF replay and an AOF rewrite end with the
//     master's sketches though the decay leaves choices to chance
//     (`DEBUG DIGEST-VALUE` of every key);
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
    let dir = std::env::temp_dir().join(format!("kividb-topk-{tag}-{}", std::process::id()));
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
        serde_json::from_str(include_str!("fixtures/redis-8.10.2-topk-replies.json")).unwrap();
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
    assert!(compared >= 1700, "{compared} replies compared");
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

/// An RDB file of Redis 8.10.2 with Top-K sketches (decay 1 and the
/// default 0.9, one with a TTL): each lists, finds and counts here as it
/// did there.
#[test]
fn a_redis_rdb_file_with_sketches_loads() {
    let answers: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/redis-topk-answers.json")).unwrap();
    let rdb = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/redis-8.10.2-topk.rdb");
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
    for key in ["topk:exact", "topk:default", "topk:ttl"] {
        assert_eq!(c.cmd(&["TYPE", key]), b"+TopK-TYPE\r\n", "{key}");
        let want = &answers[key];
        let mut want_list = format!("*{}\r\n", want["list"].as_array().unwrap().len()).into_bytes();
        for v in want["list"].as_array().unwrap() {
            match v.as_i64() {
                Some(n) => want_list.extend(format!(":{n}\r\n").as_bytes()),
                None => {
                    let s = v.as_str().unwrap();
                    want_list.extend(format!("${}\r\n{s}\r\n", s.len()).as_bytes());
                }
            }
        }
        assert_eq!(
            shown(&c.cmd(&["TOPK.LIST", key, "WITHCOUNT"])),
            shown(&want_list),
            "{key}: TOPK.LIST"
        );
        for (cmd, field) in [("TOPK.QUERY", "query"), ("TOPK.COUNT", "count")] {
            let mut args = vec![cmd, key];
            args.extend(probes.iter().map(String::as_str));
            let mut want_reply = format!("*{}\r\n", probes.len()).into_bytes();
            for v in want[field].as_array().unwrap() {
                want_reply.extend(format!(":{}\r\n", v.as_i64().unwrap()).as_bytes());
            }
            assert_eq!(shown(&c.cmd(&args)), shown(&want_reply), "{key}: {cmd}");
        }
    }
    assert_eq!(c.cmd(&["EXPIRETIME", "topk:ttl"]), b":4102444800\r\n");
    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}

/// `DUMP` of Redis 8.10.2 (`TOPK.RESERVE t2 2 3 1 1`, `TOPK.ADD t2
/// hello`) restores here; `DUMP` here is Redis's format (module type
/// `TopK-TYPE`, RDB version 9), which restores here; `MIGRATE` moves a
/// sketch between two nodes.
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
        c.cmd(&["TOPK.LIST", "from-redis", "WITHCOUNT"]),
        b"*2\r\n$5\r\nhello\r\n:1\r\n"
    );
    c.cmd(&["TOPK.RESERVE", "here", "2", "3", "1", "1"]);
    c.cmd(&["TOPK.ADD", "here", "hello"]);
    assert_eq!(
        c.cmd(&["DEBUG", "DIGEST-VALUE", "here"]),
        c.cmd(&["DEBUG", "DIGEST-VALUE", "from-redis"])
    );
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
    assert_eq!(
        b.conn().cmd(&["TOPK.QUERY", "here", "hello"]),
        b"*1\r\n:1\r\n"
    );
    drop((a, b));
    let _ = std::fs::remove_dir_all(&dir_a);
    let _ = std::fs::remove_dir_all(&dir_b);
}

// ── Persistence and replication ──────────────────────────────────────────

/// A deterministic random workload on keys `w:0` to `w:5`, the decay below
/// 1 on most, so that chance is involved.
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
        let decay = ["0.9", "0.5", "0.99", "1", "0.7", "0.95"][i];
        c.cmd(&["TOPK.RESERVE", &key, "5", "12", "3", decay]);
    }
    for _ in 0..steps {
        let key = format!("w:{}", next() % 6);
        let items: Vec<String> = (0..1 + next() % 6)
            .map(|_| format!("i{}", (next() % 40).min(next() % 40)))
            .collect();
        if next() % 3 == 0 {
            let mut args = vec!["TOPK.INCRBY".to_string(), key];
            for item in items {
                args.push(item);
                args.push((next() % 7).to_string());
            }
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            c.cmd(&args);
        } else {
            let mut args = vec!["TOPK.ADD", key.as_str()];
            args.extend(items.iter().map(String::as_str));
            c.cmd(&args);
        }
    }
}

fn digests(c: &mut Conn) -> Vec<Vec<u8>> {
    (0..6)
        .map(|i| c.cmd(&["DEBUG", "DIGEST-VALUE", &format!("w:{i}")]))
        .collect()
}

/// A replica, a snapshot, an AOF replay and an AOF rewrite all end with
/// the master's sketches, bucket for bucket, though the decay leaves
/// choices to chance: they are drawn from a generator the sketch keeps.
#[test]
fn replicas_snapshots_and_the_aof_keep_every_bucket() {
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
        for i in 0..6 {
            let key = format!("w:{i}");
            assert_eq!(
                r.cmd(&["TOPK.LIST", &key, "WITHCOUNT"]),
                m.cmd(&["TOPK.LIST", &key, "WITHCOUNT"]),
                "{key}"
            );
        }
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
    assert_eq!(
        c.cmd(&["TOPK.RESERVE", "g", "3", "100", "4", "1"]),
        b"+OK\r\n"
    );
    c.cmd(&["TOPK.ADD", "g", "a", "a", "b"]);
    assert_eq!(c.cmd(&["TYPE", "g"]), b"+TopK-TYPE\r\n");
    assert_eq!(c.cmd(&["OBJECT", "ENCODING", "g"]), b"$3\r\nraw\r\n");
    let usage = String::from_utf8_lossy(&c.cmd(&["MEMORY", "USAGE", "g"])).to_string();
    let usage: u64 = usage.trim_start_matches(':').trim().parse().unwrap();
    assert!(usage > 3200 && usage < 8000, "{usage}");
    // COPY (which Redis refuses for a module value) copies it; the copy is
    // its own.
    assert_eq!(c.cmd(&["COPY", "g", "g2"]), b":1\r\n");
    c.cmd(&["TOPK.INCRBY", "g2", "c", "10"]);
    assert_eq!(c.cmd(&["TOPK.QUERY", "g", "c"]), b"*1\r\n:0\r\n");
    assert_eq!(c.cmd(&["TOPK.QUERY", "g2", "c"]), b"*1\r\n:1\r\n");
    assert_eq!(c.cmd(&["RENAME", "g2", "g3"]), b"+OK\r\n");
    assert_eq!(c.cmd(&["TOPK.QUERY", "g3", "c"]), b"*1\r\n:1\r\n");
    assert_eq!(c.cmd(&["UNLINK", "g3"]), b":1\r\n");
    // Items are bytes, zero bytes included (Redis cuts them at the first).
    assert_eq!(
        c.raw(&[b"TOPK.ADD", b"g", b"\x00z", b"\x00z", b"\x00z"]),
        b"*3\r\n$-1\r\n$-1\r\n$-1\r\n"
    );
    assert_eq!(
        c.cmd(&["TOPK.LIST", "g"]),
        b"*3\r\n$2\r\n\x00z\r\n$1\r\na\r\n$1\r\nb\r\n"
    );
    let cat = String::from_utf8_lossy(&c.cmd(&["ACL", "CAT", "topk"])).to_lowercase();
    for name in [
        "topk.reserve",
        "topk.add",
        "topk.incrby",
        "topk.query",
        "topk.count",
        "topk.list",
        "topk.info",
    ] {
        assert!(cat.contains(name), "{name} in @topk: {cat}");
    }
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
    b.cmd(&["TOPK.RESERVE", "w", "3"]);
    for (other, aborted) in [
        (vec!["TOPK.ADD", "w", "x"], true),
        (vec!["TOPK.INCRBY", "w", "x", "-1"], true),
        (vec!["TOPK.RESERVE", "w", "3"], true),
        (vec!["TOPK.ADD", "nokey", "x"], true),
        (vec!["TOPK.QUERY", "w", "x"], false),
        (vec!["TOPK.LIST", "w"], false),
        (vec!["TOPK.INFO", "w"], false),
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
    b"-TopK: sketch is too large (at most k 65536, 67108864 buckets and 65536 rows)\r\n";
const OVER_BUDGET: &[u8] =
    b"-TopK: command reached the per-command limit; send fewer items per command\r\n";

#[test]
fn size_and_per_command_limits() {
    let _serial = serial();
    let port = start(config(kividb::config::IoModel::Tokio));
    let mut c = Conn::connect(port);
    c.cmd(&["FLUSHALL"]);
    for args in [
        ["65537", "8", "7", "0.9"],
        ["1", "67108865", "1", "0.9"],
        ["1", "1", "65537", "0.9"],
        ["1", "4294967295", "4294967295", "0.9"],
    ] {
        let mut cmd = vec!["TOPK.RESERVE", "big"];
        cmd.extend(args);
        assert_eq!(shown(&c.cmd(&cmd)), shown(TOO_LARGE), "{args:?}");
    }
    assert_eq!(c.cmd(&["EXISTS", "big"]), b":0\r\n");
    // 1,000 rows: one item at the largest increment is over the limit.
    assert_eq!(
        c.cmd(&["TOPK.RESERVE", "deep", "1", "1", "1000", "1"]),
        b"+OK\r\n"
    );
    assert_eq!(
        shown(&c.cmd(&["TOPK.INCRBY", "deep", "a", "100000"])),
        shown(OVER_BUDGET)
    );
    assert_eq!(
        c.cmd(&["TOPK.INCRBY", "deep", "a", "3000"]),
        b"*1\r\n$-1\r\n"
    );
    // 2,001 items of 1,000 rows each are over it too; nothing of the command
    // is done.
    let items: Vec<String> = (0..2001).map(|i| format!("i{i}")).collect();
    let mut args = vec!["TOPK.ADD", "deep"];
    args.extend(items.iter().map(String::as_str));
    assert_eq!(shown(&c.cmd(&args)), shown(OVER_BUDGET));
    assert_eq!(
        c.cmd(&["TOPK.LIST", "deep", "WITHCOUNT"]),
        b"*2\r\n$1\r\na\r\n:3000\r\n"
    );
}

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
    let reply = c.cmd(&["TOPK.RESERVE", "huge", "10", "8388608", "2", "0.9"]);
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

/// Payloads that claim huge sketches are refused and make no key;
/// corrupted payloads never take the server down.
#[test]
fn hostile_payloads_are_refused() {
    let _serial = serial();
    let port = start(config(kividb::config::IoModel::Tokio));
    let mut c = Conn::connect(port);
    c.cmd(&["FLUSHALL"]);
    let good = unhex(REDIS_DUMP);
    let body = good.len() - 8;
    // k, width or depth of 2^62 (the 0x02 0x02 at bytes 10, 12, 14 are
    // `k 2`, `width 3`, `depth 1`, each a one-byte length).
    for at in [11, 13, 15] {
        let mut bad = good[..at].to_vec();
        bad.push(0x81);
        bad.extend_from_slice(&(1u64 << 62).to_be_bytes());
        bad.extend_from_slice(&good[at + 1..body]);
        let crc = redis_crc64(&bad);
        bad.extend_from_slice(&crc.to_le_bytes());
        let reply = c.raw(&[b"RESTORE", b"hostile", b"0", &bad]);
        assert!(reply.starts_with(b"-"), "{at}: {}", shown(&reply));
        assert_eq!(c.cmd(&["EXISTS", "hostile"]), b":0\r\n");
    }
    for at in 0..body {
        for flip in [0x01u8, 0x80, 0xff] {
            let mut bad = good[..body].to_vec();
            bad[at] ^= flip;
            let crc = redis_crc64(&bad);
            bad.extend_from_slice(&crc.to_le_bytes());
            let reply = c.raw(&[b"RESTORE", b"fuzz", b"0", &bad, b"REPLACE"]);
            if reply == b"+OK\r\n" {
                let list = c.cmd(&["TOPK.LIST", "fuzz"]);
                assert!(
                    list.starts_with(b"*") || list.starts_with(b"-WRONGTYPE"),
                    "{}",
                    shown(&list)
                );
            }
        }
    }
    assert_eq!(c.cmd(&["PING"]), b"+PONG\r\n");
}

const REDIS_DUMP: &str = "07814e8a4af9360f100002020203020104000000000000f03f05c30d180503ad4d230100e0070001000005c31e30010000e00d000403ad4d2305201a0898d02582407e000001200b20000000050100050668656c6c6f00000f005531a20539918c1c";

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
                c.cmd(&["TOPK.RESERVE", &key, "10", "20000", "2", "0.9"]);
                c.cmd(&["TOPK.ADD", &other, &n]);
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
        c.cmd(&["TOPK.RESERVE", "big", "10", "100000", "4", "0.9"]),
        b"-OOM command not allowed when used memory > 'maxmemory'.\r\n"
    );
    assert_eq!(c.cmd(&["EXISTS", "big"]), b":0\r\n");
    assert_eq!(info_field(&mut c, "stats", "evicted_keys"), "0");
    assert_eq!(c.cmd(&["DBSIZE"]), b":100\r\n");
    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}

// ── Replicas that attach late, replays, transactions ────────────────────

/// A replica that attaches part-way through a workload takes the sketches
/// in its full sync, each with its generator, and then makes the choices
/// the master makes: it ends with the master's sketches.
#[test]
fn a_replica_that_attaches_mid_workload_ends_with_the_masters_sketches() {
    let (dir, replica_dir) = (tmp_dir("mid"), tmp_dir("mid-replica"));
    let master = Child::start(&dir, &[]);
    let mut m = master.conn();
    workload(&mut m, 0x2545_f491_4f6c_dd1d, 1500);
    let replica = Child::start(
        &replica_dir,
        &["--replicaof", "127.0.0.1", &master.port.to_string()],
    );
    // Writes while it syncs, and after.
    workload(&mut m, 0x6a09_e667_f3bc_c908, 1500);
    let mut r = replica.conn();
    wait_until("the replica to sync", || {
        String::from_utf8_lossy(&r.cmd(&["INFO", "replication"])).contains("master_link_status:up")
    });
    workload(&mut m, 0xbb67_ae85_84ca_a73b, 1500);
    assert_eq!(m.cmd(&["SET", "last", "1"]), b"+OK\r\n");
    wait_until("the replica to catch up", || {
        r.cmd(&["GET", "last"]) == b"$1\r\n1\r\n"
    });
    assert_eq!(digests(&mut r), digests(&mut m));
    drop((master, replica));
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&replica_dir);
}

/// A restart with nothing but the commands in the AOF (no snapshot taken,
/// the AOF's base made before any sketch) replays them, chances and all,
/// and ends with the sketches the master had.
#[test]
fn a_restart_that_replays_the_aof_commands_ends_with_the_sketches() {
    let dir = tmp_dir("aof-replay");
    let on_master = {
        let master = Child::start(&dir, &["--aof"]);
        let mut m = master.conn();
        workload(&mut m, 0x3c6e_f372_fe94_f82b, 3000);
        let on_master = digests(&mut m);
        assert_eq!(m.cmd(&["SET", "last", "1"]), b"+OK\r\n");
        wait_until("the AOF to hold the last write", || {
            aof_files(&dir)
                .iter()
                .any(|(_, bytes)| bytes.windows(8).any(|w| w == b"\r\nlast\r\n"))
        });
        on_master
    };
    // What is there to load: the commands, and no snapshot.
    assert!(!dir.join("dump.kdb").exists() && !dir.join("dump.rdb").exists());
    let files = aof_files(&dir);
    assert!(!files.is_empty());
    for (name, bytes) in &files {
        // Commands only, after the log's 16-byte header: no snapshot in it.
        let body = bytes
            .strip_prefix(b"KIVIDLT1")
            .map_or(&bytes[..], |rest| &rest[8..]);
        assert!(body.starts_with(b"*"), "{name}");
        assert!(body.windows(8).any(|w| w == b"TOPK.ADD"), "{name}");
    }
    let server = Child::start(&dir, &["--aof"]);
    let mut c = server.conn();
    assert_eq!(digests(&mut c), on_master);
    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}

/// The AOF's files under `dir` (its `appendonlydir`, or `dir` itself): name
/// and bytes.
fn aof_files(dir: &std::path::Path) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    for d in [dir.to_path_buf(), dir.join("appendonlydir")] {
        let Ok(entries) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in entries.filter_map(Result::ok) {
            let name = e.file_name().to_string_lossy().to_string();
            if name.contains("appendonly") && e.path().is_file() {
                out.push((name, std::fs::read(e.path()).unwrap_or_default()));
            }
        }
    }
    out
}

/// A transaction of Top-K writes reaches a replica as one, and the replica
/// makes the master's choices in it.
#[test]
fn a_transaction_applied_on_a_replica_ends_with_the_masters_sketches() {
    let (dir, replica_dir) = (tmp_dir("multi"), tmp_dir("multi-replica"));
    let master = Child::start(&dir, &[]);
    let replica = Child::start(
        &replica_dir,
        &["--replicaof", "127.0.0.1", &master.port.to_string()],
    );
    let mut m = master.conn();
    let mut r = replica.conn();
    wait_until("the replica to sync", || {
        String::from_utf8_lossy(&r.cmd(&["INFO", "replication"])).contains("master_link_status:up")
    });
    workload(&mut m, 0xa54f_f53a_5f1d_36f1, 200);
    for round in 0..50 {
        assert_eq!(m.cmd(&["MULTI"]), b"+OK\r\n");
        for i in 0..6 {
            let key = format!("w:{i}");
            let a = format!("t{}", (round * 7 + i) % 30);
            let b = format!("t{}", (round * 3 + i) % 11);
            assert_eq!(m.cmd(&["TOPK.ADD", &key, &a, &b, &a]), b"+QUEUED\r\n");
            assert_eq!(
                m.cmd(&["TOPK.INCRBY", &key, &b, "5", &a, "3"]),
                b"+QUEUED\r\n"
            );
        }
        let exec = m.cmd(&["EXEC"]);
        assert!(exec.starts_with(b"*12\r\n"), "{}", shown(&exec));
    }
    assert_eq!(m.cmd(&["SET", "last", "1"]), b"+OK\r\n");
    wait_until("the replica to catch up", || {
        r.cmd(&["GET", "last"]) == b"$1\r\n1\r\n"
    });
    assert_eq!(digests(&mut r), digests(&mut m));
    drop((master, replica));
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&replica_dir);
}

/// A count past 2^32 − 1 wraps, in the buckets and in the heap, as Redis
/// 8.10.2's does: 42,949 `TOPK.INCRBY t x 100000` count 4,294,900,000;
/// one more, 32,704 (observed on Redis 8.10.2).
#[test]
fn counts_wrap_at_the_top_as_in_redis() {
    let _serial = serial();
    let port = start(config(kividb::config::IoModel::Tokio));
    let mut c = Conn::connect(port);
    c.cmd(&["FLUSHALL"]);
    for (key, shape) in [("t", ["2", "1", "1", "1"]), ("u", ["2", "2", "3", "0.9"])] {
        let mut args = vec!["TOPK.RESERVE", key];
        args.extend(shape);
        assert_eq!(c.cmd(&args), b"+OK\r\n");
        for _ in 0..42_948 {
            c.cmd(&["TOPK.INCRBY", key, "x", "100000"]);
        }
        c.cmd(&["TOPK.INCRBY", key, "x", "100000"]);
        assert_eq!(
            c.cmd(&["TOPK.LIST", key, "WITHCOUNT"]),
            b"*2\r\n$1\r\nx\r\n:4294900000\r\n",
            "{key}"
        );
        assert_eq!(c.cmd(&["TOPK.COUNT", key, "x"]), b"*1\r\n:4294900000\r\n");
        c.cmd(&["TOPK.INCRBY", key, "x", "100000"]);
        assert_eq!(
            c.cmd(&["TOPK.LIST", key, "WITHCOUNT"]),
            b"*2\r\n$1\r\nx\r\n:32704\r\n",
            "{key}"
        );
        assert_eq!(c.cmd(&["TOPK.COUNT", key, "x"]), b"*1\r\n:32704\r\n");
        // Another item: in `t`'s one bucket it only decays x's count; `u`
        // has buckets for it.
        c.cmd(&["TOPK.INCRBY", key, "y", "5"]);
        let listed: &[u8] = if key == "t" {
            b"*2\r\n$1\r\nx\r\n:32704\r\n"
        } else {
            b"*4\r\n$1\r\nx\r\n:32704\r\n$1\r\ny\r\n:5\r\n"
        };
        assert_eq!(c.cmd(&["TOPK.LIST", key, "WITHCOUNT"]), listed, "{key}");
    }
}
