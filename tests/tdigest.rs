// tests/tdigest.rs
//
// t-digests (`TDIGEST.*`), against what Redis 8.10.2 does:
//
//   * every reply Redis 8.10.2 gave to four randomised sequences of TDIGEST
//     commands where nothing is approximated (errors, arity, wrong types,
//     missing keys, `nan`/`inf`, digests whose centroids are single values),
//     byte for byte, in RESP2 and RESP3, on both I/O loops
//     (`fixtures/redis-8.10.2-tdigest-replies.json`, made by
//     `scripts/checks/sketch/tdigest/compare.py --capture`);
//   * `DUMP` round-trips (KiviDB's own payload), Redis's is refused;
//   * a replica, a snapshot, an AOF replay and an AOF rewrite end with the
//     master's digests (`DEBUG DIGEST-VALUE`): a query changes nothing;
//   * `COPY`, `RENAME`, `TYPE`, `OBJECT ENCODING`, `WATCH`;
//   * the compression limit and the per-command limit, and hostile payloads.

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
    let dir = std::env::temp_dir().join(format!("kividb-tdigest-{tag}-{}", std::process::id()));
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
        serde_json::from_str(include_str!("fixtures/redis-8.10.2-tdigest-replies.json")).unwrap();
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
    assert!(compared >= 1500, "{compared} replies compared");
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

// ── Payloads ─────────────────────────────────────────────────────────────

/// `DUMP` here is KiviDB's own payload, which restores here (and moves with
/// `MIGRATE`); Redis's `DUMP` of a t-digest (module type `TDIS-TYPE`) is
/// refused cleanly, making no key.
#[test]
fn dump_restore_and_migrate() {
    let (dir_a, dir_b) = (tmp_dir("dump-a"), tmp_dir("dump-b"));
    let a = Child::start(&dir_a, &[]);
    let b = Child::start(&dir_b, &[]);
    let mut c = a.conn();
    let redis = unhex(REDIS_DUMP);
    let reply = c.raw(&[b"RESTORE", b"from-redis", b"0", &redis]);
    assert!(reply.starts_with(b"-"), "{}", shown(&reply));
    assert_eq!(c.cmd(&["EXISTS", "from-redis"]), b":0\r\n");
    c.cmd(&["TDIGEST.CREATE", "here", "COMPRESSION", "50"]);
    let values: Vec<String> = (0..500).map(|i| format!("{}", (i * 37) % 101)).collect();
    let mut args = vec!["TDIGEST.ADD", "here"];
    args.extend(values.iter().map(String::as_str));
    assert_eq!(c.cmd(&args), b"+OK\r\n");
    let payload = bulk(&c.cmd(&["DUMP", "here"]));
    assert_eq!(c.raw(&[b"RESTORE", b"again", b"0", &payload]), b"+OK\r\n");
    assert_eq!(
        c.cmd(&["DEBUG", "DIGEST-VALUE", "again"]),
        c.cmd(&["DEBUG", "DIGEST-VALUE", "here"])
    );
    let q = c.cmd(&["TDIGEST.QUANTILE", "here", "0.5", "0.99"]);
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
        b.conn().cmd(&["TDIGEST.QUANTILE", "here", "0.5", "0.99"]),
        q
    );
    drop((a, b));
    let _ = std::fs::remove_dir_all(&dir_a);
    let _ = std::fs::remove_dir_all(&dir_b);
}

// ── Persistence and replication ──────────────────────────────────────────

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
        let comp = ["20", "50", "100", "200", "5", "100"][i];
        c.cmd(&["TDIGEST.CREATE", &key, "COMPRESSION", comp]);
    }
    for _ in 0..steps {
        let key = format!("w:{}", next() % 6);
        match next() % 12 {
            0 => {
                let src = format!("w:{}", next() % 6);
                c.cmd(&["TDIGEST.MERGE", &key, "2", &key, &src]);
            }
            1 => {
                c.cmd(&["TDIGEST.QUANTILE", &key, "0.5"]);
            }
            2 if next() % 10 == 0 => {
                c.cmd(&["TDIGEST.RESET", &key]);
            }
            _ => {
                let values: Vec<String> = (0..1 + next() % 40)
                    .map(|_| format!("{}", (next() % 100_000) as f64 / 7.0))
                    .collect();
                let mut args = vec!["TDIGEST.ADD", key.as_str()];
                args.extend(values.iter().map(String::as_str));
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
/// the master's digests, byte for byte: queries on the master change
/// nothing a replica does not see.
#[test]
fn replicas_snapshots_and_the_aof_keep_every_centroid() {
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
        workload(&mut m, 0x9e37_79b9_7f4a_7c15, 2000);
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
    assert_eq!(c.cmd(&["TDIGEST.CREATE", "g"]), b"+OK\r\n");
    c.cmd(&["TDIGEST.ADD", "g", "1", "2", "3"]);
    assert_eq!(c.cmd(&["TYPE", "g"]), b"+TDIS-TYPE\r\n");
    assert_eq!(c.cmd(&["OBJECT", "ENCODING", "g"]), b"$3\r\nraw\r\n");
    assert_eq!(c.cmd(&["COPY", "g", "g2"]), b":1\r\n");
    c.cmd(&["TDIGEST.ADD", "g2", "100"]);
    assert_eq!(c.cmd(&["TDIGEST.MAX", "g"]), b"$1\r\n3\r\n");
    assert_eq!(c.cmd(&["TDIGEST.MAX", "g2"]), b"$3\r\n100\r\n");
    assert_eq!(c.cmd(&["RENAME", "g2", "g3"]), b"+OK\r\n");
    assert_eq!(c.cmd(&["UNLINK", "g3"]), b":1\r\n");
    // A query changes nothing.
    let before = c.cmd(&["DEBUG", "DIGEST-VALUE", "g"]);
    c.cmd(&["TDIGEST.QUANTILE", "g", "0.5"]);
    c.cmd(&["TDIGEST.CDF", "g", "2"]);
    assert_eq!(c.cmd(&["DEBUG", "DIGEST-VALUE", "g"]), before);
    let cat = String::from_utf8_lossy(&c.cmd(&["ACL", "CAT", "tdigest"])).to_lowercase();
    for name in [
        "tdigest.create",
        "tdigest.add",
        "tdigest.merge",
        "tdigest.quantile",
        "tdigest.trimmed_mean",
    ] {
        assert!(cat.contains(name), "{name} in @tdigest: {cat}");
    }
    let info = String::from_utf8_lossy(&c.cmd(&["COMMAND", "INFO", "TDIGEST.MERGE"])).to_string();
    assert!(info.contains("movablekeys"), "{info}");
}

#[test]
fn watch_is_invalidated_as_in_redis() {
    let _serial = serial();
    let port = start(config(kividb::config::IoModel::Tokio));
    let mut a = Conn::connect(port);
    let mut b = Conn::connect(port);
    b.cmd(&["FLUSHALL"]);
    b.cmd(&["TDIGEST.CREATE", "w"]);
    b.cmd(&["TDIGEST.CREATE", "z"]);
    for (other, aborted) in [
        (vec!["TDIGEST.ADD", "w", "1"], true),
        (vec!["TDIGEST.ADD", "w", "x"], true),
        (vec!["TDIGEST.CREATE", "w"], true),
        (vec!["TDIGEST.RESET", "w"], true),
        (vec!["TDIGEST.MERGE", "w", "1", "z"], true),
        (
            vec!["TDIGEST.MERGE", "w", "1", "z", "COMPRESSION", "x"],
            false,
        ),
        (vec!["TDIGEST.MIN", "w"], false),
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

#[test]
fn size_and_per_command_limits() {
    let _serial = serial();
    let port = start(config(kividb::config::IoModel::Tokio));
    let mut c = Conn::connect(port);
    c.cmd(&["FLUSHALL"]);
    for comp in ["100001", "2147483648", "9223372036854775807"] {
        assert_eq!(
            c.cmd(&["TDIGEST.CREATE", "big", "COMPRESSION", comp]),
            b"-ERR T-Digest: compression parameter is too large (at most 100000)\r\n",
            "{comp}"
        );
    }
    assert_eq!(c.cmd(&["EXISTS", "big"]), b":0\r\n");
    assert_eq!(
        c.cmd(&["TDIGEST.CREATE", "wide", "COMPRESSION", "100000"]),
        b"+OK\r\n"
    );
    // Some 100,000 centroids: 300 quantiles of it are over the limit.
    let values: Vec<String> = (0..100_000).map(|i| i.to_string()).collect();
    for chunk in values.chunks(10_000) {
        let mut args = vec!["TDIGEST.ADD", "wide"];
        args.extend(chunk.iter().map(String::as_str));
        assert_eq!(c.cmd(&args), b"+OK\r\n");
    }
    let mut args = vec!["TDIGEST.CDF", "wide"];
    args.extend(std::iter::repeat_n("5", 300));
    let reply = c.cmd(&args);
    assert!(
        reply.starts_with(b"-ERR T-Digest: command reached the per-command limit"),
        "{}",
        shown(&reply)
    );
    let median = String::from_utf8_lossy(&c.cmd(&["TDIGEST.QUANTILE", "wide", "0.5"])).to_string();
    let median: f64 = median.split("\r\n").nth(2).unwrap().parse().unwrap();
    assert!((median - 50_000.0).abs() < 100.0, "{median}");
}

/// Corrupted payloads of KiviDB's own are refused, or read as a digest
/// within the limits; never a crash.
#[test]
fn hostile_payloads_are_refused() {
    let _serial = serial();
    let port = start(config(kividb::config::IoModel::Tokio));
    let mut c = Conn::connect(port);
    c.cmd(&["FLUSHALL"]);
    c.cmd(&["TDIGEST.CREATE", "h", "COMPRESSION", "10"]);
    let values: Vec<String> = (0..200).map(|i| i.to_string()).collect();
    let mut args = vec!["TDIGEST.ADD", "h"];
    args.extend(values.iter().map(String::as_str));
    c.cmd(&args);
    let good = bulk(&c.cmd(&["DUMP", "h"]));
    let body = good.len() - 10;
    let mut seed = 0x2545_f491_4f6c_dd1du64;
    for _ in 0..3000 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let mut bad = good.clone();
        let at = seed as usize % body;
        bad[at] ^= (seed >> 40) as u8 | 1;
        let reply = c.raw(&[b"RESTORE", b"fuzz", b"0", &bad, b"REPLACE"]);
        if reply == b"+OK\r\n" {
            let q = c.cmd(&["TDIGEST.INFO", "fuzz"]);
            assert!(
                q.starts_with(b"*18") || q.starts_with(b"-WRONGTYPE"),
                "{}",
                shown(&q)
            );
        }
    }
    assert_eq!(c.cmd(&["PING"]), b"+PONG\r\n");
}

const REDIS_DUMP: &str = "07814c3212f9360f100004000000000000594004000000000000f03f04000000000000084002426202030200020104000000000000084004000000000000000004000000000000f03f04000000000000004004000000000000084004000000000000f03f04000000000000f03f04000000000000f03f000f0050d69bd25f27bb47";
