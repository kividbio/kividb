// tests/bloom.rs
//
// Bloom filters (`BF.*`), against what Redis 8.10 does:
//
//   * every reply Redis 8.10.3 gave to the BF commands of four captures,
//     byte for byte, in RESP2 and RESP3 (`fixtures/redis-8.10-bloom-replies.json`);
//   * RDB files with Bloom filters written by Redis 8.10 and Redis Stack
//     6.2, 7.2 and 7.4 load, and the filters answer as they did there;
//   * a snapshot, an AOF replay and a replica keep them; a filter a command
//     made for want of one is sent as a `BF.INSERT` that names how it was
//     made;
//   * `DUMP` writes what Redis's `RESTORE` reads; `WATCH`, `COPY`, the
//     `bf-*` settings and keyspace events behave as described in
//     `commands::bf`.
//
// The `bf-*` settings are the process's, and these tests share a process:
// they run one at a time (`serial`).

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

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
                        .set_read_timeout(Some(Duration::from_secs(10)))
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

    fn send(&mut self, args: &[&[u8]]) {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            out.extend_from_slice(a);
            out.extend_from_slice(b"\r\n");
        }
        self.stream.write_all(&out).unwrap();
    }

    /// One reply, as the bytes it came in.
    fn read(&mut self) -> Vec<u8> {
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

    fn raw(&mut self, args: &[&[u8]]) -> Vec<u8> {
        self.send(args);
        self.read()
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

// ── Servers ──────────────────────────────────────────────────────────────

/// Whether nothing has `port` (asked with a socket that never listens; see
/// tests/vset.rs).
fn port_is_free(port: u16) -> bool {
    let Ok(probe) = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None) else {
        return false;
    };
    #[cfg(unix)]
    let _ = probe.set_reuse_address(true);
    probe
        .bind(&std::net::SocketAddr::from(([127, 0, 0, 1], port)).into())
        .is_ok()
}

fn free_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(0);
    loop {
        let step = NEXT.fetch_add(1, Ordering::Relaxed);
        let from = (std::process::id() as u16).wrapping_mul(37);
        let port = 10_000 + from.wrapping_add(step) % 20_000;
        if port_is_free(port) {
            return port;
        }
    }
}

fn config() -> kividb::config::Config {
    kividb::config::Config {
        bind: "127.0.0.1".to_string(),
        threads: 2,
        aof: false,
        databases: 16,
        loglevel: "error".to_string(),
        io_model: kividb::config::IoModel::Tokio,
        configfile: "./kividb.conf".to_string(),
        ..Default::default()
    }
}

/// A server with `config`, on a port of its own; the port.
fn start(mut config: kividb::config::Config) -> u16 {
    let port = free_port();
    config.port = port;
    thread::spawn(move || kividb::run_for_test(config));
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if let Ok(mut c) = TcpStream::connect(("127.0.0.1", port)) {
            let _ = c.write_all(b"PING\r\n");
            let mut b = [0u8; 7];
            if c.read_exact(&mut b).is_ok() && &b == b"+PONG\r\n" {
                return port;
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("server on {port} did not start");
}

fn tmp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("kividb-bloom-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn wait_until(what: &str, mut pred: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if pred() {
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("timed out waiting for {what}");
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

// ── Redis's replies ──────────────────────────────────────────────────────

/// Every BF reply of the captures, in order, RESP2 on database 0 and RESP3
/// on database 1, each sequence from an empty server.
#[test]
fn replies_are_redis_8_10_byte_for_byte() {
    let _serial = serial();
    let golden: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/redis-8.10-bloom-replies.json")).unwrap();
    let port = start(config());
    let mut failures = Vec::new();
    let mut compared = 0;
    for sequence in golden["sequences"].as_array().unwrap() {
        let mut c2 = Conn::connect(port);
        let mut c3 = Conn::connect(port);
        assert_eq!(c2.cmd(&["FLUSHALL"]), b"+OK\r\n");
        c3.cmd(&["HELLO", "3"]);
        assert_eq!(c3.cmd(&["SELECT", "1"]), b"+OK\r\n");
        for case in sequence["cases"].as_array().unwrap() {
            let args: Vec<Vec<u8>> = case["args"]
                .as_array()
                .unwrap()
                .iter()
                .map(|a| unhex(a.as_str().unwrap()))
                .collect();
            let refs: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();
            let got2 = c2.raw(&refs);
            let want2 = unhex(case["resp2"].as_str().unwrap());
            let name = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
            let shown_args: String = args
                .iter()
                .map(|a| shown(&a[..a.len().min(30)]))
                .collect::<Vec<_>>()
                .join(" ");
            compared += 1;
            if got2 != want2 {
                failures.push(format!(
                    "{shown_args}\n  RESP2 want {}\n        got  {}",
                    shown(&want2),
                    shown(&got2)
                ));
            }
            // A setting is the server's: Redis's capture set it once.
            if name == "CONFIG" {
                continue;
            }
            if let Some(want3) = case["resp3"].as_str() {
                let want3 = unhex(want3);
                let got3 = c3.raw(&refs);
                if got3 != want3 {
                    failures.push(format!(
                        "{shown_args}\n  RESP3 want {}\n        got  {}",
                        shown(&want3),
                        shown(&got3)
                    ));
                }
            }
        }
    }
    assert!(compared > 400, "{compared} replies compared");
    assert!(
        failures.is_empty(),
        "{} of {compared} differ:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

// ── Servers of their own (files, replicas) ───────────────────────────────

/// A `kividb` process in `dir`, killed when dropped.
struct Child {
    port: u16,
    child: std::process::Child,
}

impl Child {
    fn start(dir: &std::path::Path, extra: &[&str]) -> Child {
        let port = free_port();
        let child = std::process::Command::new(env!("CARGO_BIN_EXE_kividb"))
            .current_dir(dir)
            .args(["--port", &port.to_string(), "--bind", "127.0.0.1"])
            .args([
                "--io-model",
                "tokio",
                "--threads",
                "2",
                "--loglevel",
                "warning",
            ])
            .args(["--configfile", "kividb.conf", "--dir"])
            .arg(dir)
            .args(extra)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .spawn()
            .expect("could not start kividb");
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

// ── Redis's files ────────────────────────────────────────────────────────

/// RDB files from Redis 8.10 and Redis Stack 6.2, 7.2 and 7.4 holding
/// Bloom filters of every shape (scaling, non-scaling, `EXPANSION 0`, a
/// layer large enough to be compressed, one made by `BF.ADD`, one with a
/// TTL): each filter answers here as it did there.
#[test]
fn redis_rdb_files_with_bloom_filters_load() {
    let answers: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/redis-bloom-answers.json")).unwrap();
    let fixtures = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    for label in [
        "redis-8.10",
        "redis-stack-6.2",
        "redis-stack-7.2",
        "redis-stack-7.4",
    ] {
        let rdb = fixtures.join(format!("{label}-bloom.rdb"));
        let dir = tmp_dir(label);
        let preload = format!("rdb:{}", rdb.display());
        let server = Child::start(&dir, &["--preload-file", &preload]);
        let mut c = server.conn();
        let want = &answers[label];
        let probes: Vec<String> = want["_probes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p.as_str().unwrap().to_string())
            .collect();
        for key in [
            "bf:small",
            "bf:nonscaling",
            "bf:exp0",
            "bf:exp3",
            "bf:big",
            "bf:auto",
        ] {
            assert_eq!(
                c.cmd(&["TYPE", key]),
                b"+MBbloom--\r\n",
                "{label} {key}: TYPE"
            );
            let mut args = vec!["BF.MEXISTS", key];
            args.extend(probes.iter().map(String::as_str));
            let mut want_mexists = format!("*{}\r\n", probes.len()).into_bytes();
            for v in want[key]["mexists"].as_array().unwrap() {
                want_mexists.extend(format!(":{}\r\n", v.as_i64().unwrap()).as_bytes());
            }
            assert_eq!(
                shown(&c.cmd(&args)),
                shown(&want_mexists),
                "{label} {key}: BF.MEXISTS"
            );

            let mut want_info = b"*10\r\n".to_vec();
            for pair in want[key]["info"].as_array().unwrap().chunks(2) {
                want_info.extend(format!("+{}\r\n", pair[0].as_str().unwrap()).as_bytes());
                match pair[1].as_i64() {
                    Some(n) => want_info.extend(format!(":{n}\r\n").as_bytes()),
                    None => want_info.extend(b"$-1\r\n"),
                }
            }
            assert_eq!(
                shown(&c.cmd(&["BF.INFO", key])),
                shown(&want_info),
                "{label} {key}: BF.INFO"
            );

            let lines = want[key]["debug"].as_array().unwrap();
            let mut want_debug = format!("*{}\r\n", lines.len()).into_bytes();
            for line in lines {
                let line = line.as_str().unwrap();
                want_debug.extend(format!("${}\r\n{line}\r\n", line.len()).as_bytes());
            }
            assert_eq!(
                shown(&c.cmd(&["BF.DEBUG", key])),
                shown(&want_debug),
                "{label} {key}: BF.DEBUG"
            );
        }
        assert_eq!(c.cmd(&["GET", "plain"]), b"$6\r\nstring\r\n", "{label}");
        // The files were saved with `EXPIREAT bf:small 4102444800`, in 2100,
        // so that the key does not run out.
        assert_eq!(
            c.cmd(&["PEXPIRETIME", "bf:small"]),
            b":4102444800000\r\n",
            "{label}: the TTL came across"
        );
        // The filters keep working: a scaling one takes more, a full one
        // refuses.
        assert_eq!(c.cmd(&["BF.ADD", "bf:auto", "new-item-1"]), b":1\r\n");
        assert_eq!(
            c.cmd(&["BF.ADD", "bf:nonscaling", "one-too-many"]),
            b"-ERR non scaling filter is full\r\n",
            "{label}"
        );
        drop(server);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// `DUMP` of Redis 8.10 (`BF.INSERT bf4 CAPACITY 50 ERROR 0.05 EXPANSION
/// 3 ITEMS a b c`) restores here; `DUMP` here gives the same value, with
/// RDB version 9 and Redis's CRC, which every Redis since 6.0 restores;
/// `MIGRATE` moves a filter between two nodes.
#[test]
fn dump_restore_and_migrate() {
    let redis = unhex(
        "07813016e5a289bef80402030201020502030232049a9999999999993f0206040bd81f7830b61e400241800200053010000000000001200040200000c2000000000008001000000000008400000000000400020000000020000080100000000203000f000ec4213c2476ee3a",
    );
    let (dir_a, dir_b) = (tmp_dir("dump-a"), tmp_dir("dump-b"));
    let a = Child::start(&dir_a, &[]);
    let b = Child::start(&dir_b, &[]);
    let mut c = a.conn();
    assert_eq!(
        c.raw(&[b"RESTORE", b"from-redis", b"0", &redis]),
        b"+OK\r\n"
    );
    assert_eq!(
        c.cmd(&["BF.MEXISTS", "from-redis", "a", "b", "c", "zz"]),
        b"*4\r\n:1\r\n:1\r\n:1\r\n:0\r\n"
    );
    assert_eq!(
        c.cmd(&[
            "BF.INSERT",
            "here",
            "CAPACITY",
            "50",
            "ERROR",
            "0.05",
            "EXPANSION",
            "3",
            "ITEMS",
            "a",
            "b",
            "c"
        ]),
        b"*3\r\n:1\r\n:1\r\n:1\r\n"
    );
    let dumped = c.cmd(&["DUMP", "here"]);
    let payload = &dumped[dumped.iter().position(|&b| b == b'\n').unwrap() + 1..dumped.len() - 2];
    assert_eq!(
        payload[..payload.len() - 10],
        redis[..redis.len() - 10],
        "the value as Redis writes it"
    );
    assert_eq!(
        &payload[payload.len() - 10..payload.len() - 8],
        &[9, 0],
        "RDB version 9"
    );
    assert_eq!(c.raw(&[b"RESTORE", b"again", b"0", payload]), b"+OK\r\n");
    assert_eq!(c.cmd(&["BF.CARD", "again"]), b":3\r\n");
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
        b.conn().cmd(&["BF.MEXISTS", "here", "a", "b", "c", "zz"]),
        b"*4\r\n:1\r\n:1\r\n:1\r\n:0\r\n"
    );
    assert_eq!(c.cmd(&["EXISTS", "here"]), b":0\r\n");
    drop((a, b));
    let _ = std::fs::remove_dir_all(&dir_a);
    let _ = std::fs::remove_dir_all(&dir_b);
}

// ── Persistence and replication ──────────────────────────────────────────

fn some_filters(c: &mut Conn) {
    assert_eq!(
        c.cmd(&["BF.RESERVE", "p:scaling", "0.01", "10", "EXPANSION", "3"]),
        b"+OK\r\n"
    );
    let items: Vec<String> = (0..100).map(|i| format!("i{i}")).collect();
    let mut args = vec!["BF.MADD", "p:scaling"];
    args.extend(items.iter().map(String::as_str));
    c.cmd(&args);
    c.cmd(&["BF.RESERVE", "p:fixed", "0.001", "5", "NONSCALING"]);
    c.cmd(&["BF.ADD", "p:fixed", "x"]);
    c.cmd(&["BF.ADD", "p:auto", "y"]);
}

/// What `BF.SCANDUMP` gives for a key: the whole filter, byte for byte.
fn scandump(c: &mut Conn, key: &str) -> Vec<Vec<u8>> {
    let mut iter = "0".to_string();
    let mut chunks = Vec::new();
    for _ in 0..1000 {
        let reply = c.cmd(&["BF.SCANDUMP", key, &iter]);
        assert!(
            reply.starts_with(b"*2"),
            "BF.SCANDUMP {key}: {}",
            shown(&reply)
        );
        chunks.push(reply.clone());
        let text = String::from_utf8_lossy(&reply);
        let next = text
            .split("\r\n")
            .nth(1)
            .unwrap()
            .trim_start_matches(':')
            .to_string();
        if next == "0" {
            return chunks;
        }
        iter = next;
    }
    panic!("SCANDUMP of {key} did not end");
}

#[test]
fn snapshots_keep_filters() {
    let dir = tmp_dir("kdb");
    let keys = ["p:scaling", "p:fixed", "p:auto"];
    let before: Vec<_> = {
        let server = Child::start(&dir, &[]);
        let mut c = server.conn();
        some_filters(&mut c);
        assert_eq!(c.cmd(&["SAVE"]), b"+OK\r\n");
        keys.iter().map(|k| scandump(&mut c, k)).collect()
    };
    let server = Child::start(&dir, &[]);
    let mut c = server.conn();
    let after: Vec<_> = keys.iter().map(|k| scandump(&mut c, k)).collect();
    assert_eq!(after, before);
    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn the_aof_and_replicas_get_the_filter_the_master_made() {
    let (dir, replica_dir) = (tmp_dir("aof"), tmp_dir("replica"));
    let keys = ["p:scaling", "p:fixed", "p:auto", "p:insert", "p:madd"];
    let on_master: Vec<_> = {
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
        some_filters(&mut m);
        m.cmd(&["BF.INSERT", "p:insert", "ERROR", "0.2", "ITEMS", "a", "b"]);
        m.cmd(&["BF.MADD", "p:madd", "a", "b"]);
        let on_master: Vec<_> = keys.iter().map(|k| scandump(&mut m, k)).collect();
        wait_until("the replica to have every filter", || {
            r.cmd(&["EXISTS", "p:madd"]) == b":1\r\n"
        });
        let on_replica: Vec<_> = keys.iter().map(|k| scandump(&mut r, k)).collect();
        assert_eq!(on_replica, on_master);
        on_master
    };
    // The AOF has the filters made for want of one as BF.INSERTs that name
    // how they were made.
    let aof: Vec<u8> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.file_name().to_string_lossy().contains("appendonly"))
        .filter_map(|e| std::fs::read(e.path()).ok())
        .flatten()
        .collect();
    let text = String::from_utf8_lossy(&aof);
    assert!(
        text.contains(
            "BF.INSERT\r\n$6\r\np:auto\r\n$8\r\nCAPACITY\r\n$3\r\n100\r\n$5\r\nERROR\r\n$4\r\n0.01\r\n$9\r\nEXPANSION\r\n$1\r\n2\r\n$5\r\nITEMS\r\n$1\r\ny"
        ),
        "{text}"
    );
    assert!(
        text.contains("$10\r\nBF.RESERVE\r\n$9\r\np:scaling"),
        "{text}"
    );
    // And a replay rebuilds every filter.
    let server = Child::start(&dir, &["--aof"]);
    let mut c = server.conn();
    let replayed: Vec<_> = keys.iter().map(|k| scandump(&mut c, k)).collect();
    assert_eq!(replayed, on_master);
    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&replica_dir);
}

// ── Generic behaviour ────────────────────────────────────────────────────

/// As in Redis: a write to the key invalidates a `WATCH` on it even when it
/// changes nothing or fails once it has found the key; a read does not.
#[test]
fn watch_is_invalidated_as_in_redis() {
    let _serial = serial();
    let port = start(config());
    let mut a = Conn::connect(port);
    let mut b = Conn::connect(port);
    b.cmd(&["BF.ADD", "w", "x"]);
    for (other, aborted) in [
        (vec!["BF.ADD", "w", "y"], true),
        (vec!["BF.ADD", "w", "x"], true),
        (vec!["BF.MADD", "w", "x"], true),
        (vec!["BF.INSERT", "w", "ITEMS", "x"], true),
        (vec!["BF.RESERVE", "w", "0.1", "10"], true),
        (vec!["BF.EXISTS", "w", "x"], false),
        (vec!["BF.INFO", "w"], false),
        (vec!["BF.SCANDUMP", "w", "0"], false),
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

#[test]
fn copy_rename_move_and_expire() {
    let _serial = serial();
    let port = start(config());
    let mut c = Conn::connect(port);
    c.cmd(&["BF.MADD", "c", "a", "b"]);
    // Redis refuses COPY of a Bloom filter; KiviDB copies it, and the copy
    // is a filter of its own.
    assert_eq!(c.cmd(&["COPY", "c", "c2"]), b":1\r\n");
    c.cmd(&["BF.ADD", "c2", "only-in-the-copy"]);
    assert_eq!(c.cmd(&["BF.EXISTS", "c", "only-in-the-copy"]), b":0\r\n");
    assert_eq!(c.cmd(&["BF.EXISTS", "c2", "a"]), b":1\r\n");
    assert_eq!(c.cmd(&["RENAME", "c2", "c3"]), b"+OK\r\n");
    assert_eq!(c.cmd(&["BF.CARD", "c3"]), b":3\r\n");
    assert_eq!(c.cmd(&["MOVE", "c3", "5"]), b":1\r\n");
    assert_eq!(c.cmd(&["EXPIRE", "c", "100"]), b":1\r\n");
    assert_eq!(c.cmd(&["OBJECT", "ENCODING", "c"]), b"$3\r\nraw\r\n");
    assert_eq!(c.cmd(&["TYPE", "c"]), b"+MBbloom--\r\n");
    assert_eq!(
        c.cmd(&["SCAN", "0", "TYPE", "MBbloom--"]),
        b"*2\r\n$1\r\n0\r\n*1\r\n$1\r\nc\r\n"
    );
    let usage = String::from_utf8(c.cmd(&["MEMORY", "USAGE", "c"])).unwrap();
    let usage: usize = usage.trim_start_matches(':').trim_end().parse().unwrap();
    assert!((240..1000).contains(&usage), "MEMORY USAGE {usage}");
}

#[test]
fn bf_settings() {
    let _serial = serial();
    let port = start(config());
    let mut c = Conn::connect(port);
    assert_eq!(
        c.cmd(&["CONFIG", "GET", "bf-*"]),
        b"*6\r\n$15\r\nbf-initial-size\r\n$3\r\n100\r\n$19\r\nbf-expansion-factor\r\n$1\r\n2\r\n$13\r\nbf-error-rate\r\n$4\r\n0.01\r\n"
    );
    assert_eq!(
        c.cmd(&["CONFIG", "SET", "bf-initial-size", "7"]),
        b"+OK\r\n"
    );
    assert_eq!(
        c.cmd(&["CONFIG", "SET", "bf-expansion-factor", "0"]),
        b"+OK\r\n"
    );
    assert_eq!(
        c.cmd(&["CONFIG", "SET", "bf-error-rate", "0.5"]),
        b"+OK\r\n"
    );
    assert_eq!(
        c.cmd(&["CONFIG", "GET", "bf-error-rate"]),
        b"*2\r\n$13\r\nbf-error-rate\r\n$4\r\n0.25\r\n"
    );
    c.cmd(&["BF.ADD", "made", "x"]);
    assert_eq!(
        c.cmd(&["BF.DEBUG", "made"]),
        b"*2\r\n$6\r\nsize:1\r\n$66\r\nbytes:8 bits:64 hashes:3 hashwidth:64 capacity:7 size:1 ratio:0.25\r\n"
    );
    assert_eq!(
        c.cmd(&["CONFIG", "SET", "bf-initial-size", "0"]),
        b"-ERR CONFIG SET failed (possibly related to argument 'bf-initial-size') - Value for `bf-initial-size` must be in the range [1, 1073741824]\r\n"
    );
    for (name, value) in [
        ("bf-initial-size", "100"),
        ("bf-expansion-factor", "2"),
        ("bf-error-rate", "0.01"),
    ] {
        assert_eq!(c.cmd(&["CONFIG", "SET", name, value]), b"+OK\r\n");
    }
}

/// Redis sends no keyspace event for any `BF.*` command; neither does
/// KiviDB. The generic commands still send theirs.
#[test]
fn no_keyspace_events() {
    let _serial = serial();
    let port = start(config());
    let mut c = Conn::connect(port);
    let mut sub = Conn::connect(port);
    c.cmd(&["CONFIG", "SET", "notify-keyspace-events", "KEA"]);
    sub.cmd(&["PSUBSCRIBE", "__keyevent@0__:*"]);
    for cmd in [
        vec!["BF.RESERVE", "e", "0.1", "10"],
        vec!["BF.ADD", "e", "a"],
        vec!["BF.MADD", "e", "b"],
        vec!["BF.INSERT", "e2", "ITEMS", "a"],
    ] {
        c.cmd(&cmd);
    }
    c.cmd(&["DEL", "e"]);
    let event = sub.read();
    c.cmd(&["CONFIG", "SET", "notify-keyspace-events", ""]);
    assert!(
        shown(&event).contains("__keyevent@0__:del"),
        "the first event is DEL's: {}",
        shown(&event)
    );
}

// ── Hostile input ────────────────────────────────────────────────────────

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

/// A `BF.SCANDUMP` header for one layer: 10 items of capacity at 10%
/// unless told otherwise.
fn header(capacity: u64, hashes: u32, bytes: u64) -> Vec<u8> {
    let per_item = -(0.1f64.ln()) / 0.480453013918201;
    let mut h = Vec::new();
    h.extend(0u64.to_le_bytes()); // items
    h.extend(1u32.to_le_bytes()); // layers
    h.extend(5u32.to_le_bytes()); // flags
    h.extend(2u32.to_le_bytes()); // expansion
    h.extend(bytes.to_le_bytes());
    h.extend((bytes.wrapping_mul(8)).to_le_bytes());
    h.extend(0u64.to_le_bytes()); // items in the layer
    h.extend(0.1f64.to_le_bytes());
    h.extend(per_item.to_le_bytes());
    h.extend(hashes.to_le_bytes());
    h.extend(capacity.to_le_bytes());
    h.push(0);
    h
}

/// What a client, a file or a payload may claim about a filter, and must
/// not get: each is refused, the key is not made, and the server goes on.
/// (A capacity of 0 used to make the next `BF.ADD` divide by zero; a hash
/// count of 2^32 - 1 made every lookup loop for minutes under the shard's
/// lock; a 64 GiB layer was allocated, or aborted the process.)
#[test]
fn hostile_input_is_refused() {
    let _serial = serial();
    let port = start(config());
    let mut c = Conn::connect(port);
    let good = header(10, 4, 8);
    assert_eq!(c.raw(&[b"BF.LOADCHUNK", b"ok", b"1", &good]), b"+OK\r\n");
    for (what, raw, want) in [
        (
            "capacity 0",
            header(0, 4, 8),
            &b"-ERR received bad data\r\n"[..],
        ),
        (
            "hashes 2^32 - 1",
            header(10, u32::MAX, 8),
            b"-ERR received bad data\r\n",
        ),
        (
            "hashes 1076",
            header(10, 1076, 8),
            b"-ERR received bad data\r\n",
        ),
        (
            "a 64 GiB layer",
            header(10, 4, 1 << 36),
            b"-ERR filter is too large\r\n",
        ),
    ] {
        assert_eq!(
            shown(&c.raw(&[b"BF.LOADCHUNK", b"hostile", b"1", &raw])),
            shown(want),
            "{what}"
        );
        assert_eq!(c.cmd(&["EXISTS", "hostile"]), b":0\r\n", "{what}");
    }
    for iter in ["0", "-1", "-9223372036854775808"] {
        assert_eq!(
            c.cmd(&["BF.LOADCHUNK", "ok", iter, "x"]),
            b"-ERR received bad data\r\n",
            "iterator {iter}"
        );
    }
    // A layer far past what may be had, asked for by a command.
    assert_eq!(
        c.cmd(&["BF.RESERVE", "big", "1e-30", "1073741824"]),
        b"-ERR filter is too large\r\n"
    );
    // A Redis DUMP payload of a filter whose layer has no capacity: not
    // restored.
    let mut payload = unhex(
        "07813016e5a289bef80402030201020502030200049a9999999999993f0206040bd81f7830b61e400241800200053010000000000001200040200000c2000000000008001000000000008400000000000400020000000020000080100000000203000f00",
    );
    let crc = redis_crc64(&payload);
    payload.extend(crc.to_le_bytes());
    assert_eq!(
        c.raw(&[b"RESTORE", b"hostile", b"0", &payload]),
        b"-ERR Bad data format\r\n"
    );
    assert_eq!(c.cmd(&["EXISTS", "hostile"]), b":0\r\n");
    // The filter made from the good header still works, and so does the
    // server.
    assert_eq!(c.cmd(&["BF.ADD", "ok", "x"]), b":1\r\n");
    assert_eq!(c.cmd(&["PING"]), b"+PONG\r\n");
}
