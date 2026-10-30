// tests/index-persistence.rs
//
// End-to-end tests verifying that FT.* index schemas survive server restarts.
//
// Strategy:
//   1. Spawn a real kividb process in a temp directory.
//   2. Create indexes, insert data, issue SAVE.
//   3. Send SHUTDOWN NOSAVE (kills without another snapshot write).
//   4. Spawn a second process in the *same* temp directory.
//   5. Assert that indexes are present, correct, and return the right results.
//
// Also covers AOF rewrite: BGREWRITEAOF checkpoints dump.kdb and resets the
// AOF to a KIVIDLT1 delta so dump-then-AOF recovery reproduces the index.
//
// Each test picks its own port via `free_port()`, but `cargo test` still runs
// this integration test binary in parallel with *other* test binaries that
// also spawn `kividb`, which can cause port reuse races. Every test acquires
// `persist_serial()` for its whole body so spawns from this file are serialized.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::process::{Child, Command};
use std::time::{Duration, Instant};
use std::{env, thread};

// ── Binary path ───────────────────────────────────────────────────────────────

fn kividb_bin() -> String {
    env::var("CARGO_BIN_EXE_kividb").unwrap_or_else(|_| {
        let manifest = env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into());
        format!("{manifest}/target/debug/kividb")
    })
}

// ── Process helpers ───────────────────────────────────────────────────────────

fn start_server_in_dir(port: u16, dir: &Path) -> Child {
    Command::new(kividb_bin())
        .args(["--port", &port.to_string()])
        .current_dir(dir)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("failed to spawn kividb — run `cargo build` first")
}

fn wait_for_server(port: u16, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return true;
        }
        thread::sleep(Duration::from_millis(30));
    }
    false
}

fn wait_for_exit(child: &mut Child, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        match child.try_wait() {
            Ok(Some(_)) | Err(_) => return true,
            Ok(None) => {}
        }
        thread::sleep(Duration::from_millis(30));
    }
    false
}

fn kill_server(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

// ── Unique temp directory ─────────────────────────────────────────────────────

fn temp_dir(tag: &str) -> std::path::PathBuf {
    let id = std::thread::current().id();
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("kivi_persist_{tag}_{id:?}_{ts}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

// ── Free port ─────────────────────────────────────────────────────────────────

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

/// `cargo test` runs integration test binaries in parallel by default; each test
/// here spawns a real `kividb` and uses `free_port()`, so another test process
/// can steal the same port between `free_port()` and `bind(2)` in the server.
/// Holding this guard for the whole test serializes this file's spawns.
#[allow(dead_code)]
struct PersistSerialGuard(std::sync::MutexGuard<'static, ()>);

fn persist_serial() -> PersistSerialGuard {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    PersistSerialGuard(
        LOCK.get_or_init(|| std::sync::Mutex::new(()))
            .lock()
            .expect("persist_serial mutex poisoned"),
    )
}

// ── Minimal RESP client ───────────────────────────────────────────────────────

struct Conn {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

impl Conn {
    fn connect(port: u16) -> Self {
        let stream = (0..60)
            .find_map(|_| {
                thread::sleep(Duration::from_millis(50));
                TcpStream::connect(("127.0.0.1", port)).ok()
            })
            .unwrap_or_else(|| panic!("could not connect to port {port}"));
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
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
                let items = (0..count as usize).map(|_| self.read_value()).collect();
                RespValue::Array(items)
            }
            _ => panic!("unexpected RESP line: {line:?}"),
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
            _ => panic!("not a string: {self:?}"),
        }
    }
    fn try_as_str(&self) -> Option<&str> {
        match self {
            RespValue::SimpleStr(s) => Some(s.as_str()),
            RespValue::Bulk(b) => std::str::from_utf8(b).ok(),
            _ => None,
        }
    }
    fn as_int(&self) -> i64 {
        match self {
            RespValue::Int(n) => *n,
            RespValue::Bulk(b) => std::str::from_utf8(b)
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(|| panic!("not an int: {self:?}")),
            _ => panic!("not an int: {self:?}"),
        }
    }
    fn as_array(&self) -> &[RespValue] {
        match self {
            RespValue::Array(v) => v,
            _ => panic!("not an array: {self:?}"),
        }
    }
    fn is_ok(&self) -> bool {
        matches!(self, RespValue::SimpleStr(s) if s == "OK")
    }
    fn is_error(&self) -> bool {
        matches!(self, RespValue::Error(_))
    }
}

// ── Vector helpers ────────────────────────────────────────────────────────────

fn pack_f32(values: &[f32]) -> Vec<u8> {
    values.iter().flat_map(|f| f.to_le_bytes()).collect()
}

fn send_shutdown_nosave(port: u16) {
    if let Ok(mut s) = TcpStream::connect(("127.0.0.1", port)) {
        let _ = s.write_all(b"*2\r\n$8\r\nSHUTDOWN\r\n$6\r\nNOSAVE\r\n");
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// Snapshot persistence — FLAT vector index
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn flat_index_survives_snapshot_restart() {
    let _serial = persist_serial();
    let dir = temp_dir("flat_snap");
    let port1 = free_port();

    // ── First server ──────────────────────────────────────────────────────────
    let mut srv1 = start_server_in_dir(port1, &dir);
    assert!(
        wait_for_server(port1, Duration::from_secs(10)),
        "server 1 did not start"
    );

    {
        let mut c = Conn::connect(port1);
        // Create a FLAT index on prefix "doc:"
        assert!(
            c.cmds(&[
                "FT.CREATE",
                "idx:flat",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "doc:",
                "SCHEMA",
                "vec",
                "VECTOR",
                "FLAT",
                "6",
                "TYPE",
                "FLOAT32",
                "DIM",
                "4",
                "DISTANCE_METRIC",
                "L2",
            ])
            .is_ok()
        );

        // Insert vectors
        c.cmd(&[b"HSET", b"doc:a", b"vec", &pack_f32(&[1.0, 0.0, 0.0, 0.0])]);
        c.cmd(&[b"HSET", b"doc:b", b"vec", &pack_f32(&[0.0, 1.0, 0.0, 0.0])]);
        c.cmd(&[b"HSET", b"doc:c", b"vec", &pack_f32(&[0.0, 0.0, 1.0, 0.0])]);

        // Save snapshot then shut down without another save
        assert!(c.cmds(&["SAVE"]).is_ok());
    }

    send_shutdown_nosave(port1);
    assert!(
        wait_for_exit(&mut srv1, Duration::from_secs(8)),
        "server 1 did not stop"
    );

    // Verify dump.kdb was written
    assert!(dir.join("dump.kdb").exists(), "dump.kdb not written");

    // ── Second server ─────────────────────────────────────────────────────────
    let port2 = free_port();
    let mut srv2 = start_server_in_dir(port2, &dir);
    assert!(
        wait_for_server(port2, Duration::from_secs(10)),
        "server 2 did not start"
    );

    {
        let mut c = Conn::connect(port2);

        // Index must exist after restart
        let list = c.cmds(&["FT._LIST"]);
        let names: Vec<&str> = list.as_array().iter().map(|v| v.as_str()).collect();
        assert!(
            names.contains(&"idx:flat"),
            "idx:flat missing after restart: {names:?}"
        );

        // KNN search must return the closest vector (doc:a ≈ query [1,0,0,0])
        let q = pack_f32(&[1.0, 0.0, 0.0, 0.0]);
        let res = c.cmd(&[
            b"FT.SEARCH",
            b"idx:flat",
            b"*=>[KNN 1 @vec $q AS score]",
            b"PARAMS",
            b"2",
            b"q",
            &q,
            b"RETURN",
            b"1",
            b"score",
            b"DIALECT",
            b"2",
        ]);
        let arr = res.as_array();
        assert_eq!(arr[0].as_int(), 1, "expected 1 result");
        assert_eq!(
            arr[1].as_str(),
            "doc:a",
            "closest to [1,0,0,0] should be doc:a"
        );
        let score: f32 = arr[2].as_array()[1].as_str().parse().unwrap();
        assert!(score < 1e-5, "score should be ~0, got {score}");
    }

    kill_server(&mut srv2);
    let _ = std::fs::remove_dir_all(&dir);
}

// ═════════════════════════════════════════════════════════════════════════════
// Snapshot persistence — HNSW vector index
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn hnsw_index_survives_snapshot_restart() {
    let _serial = persist_serial();
    let dir = temp_dir("hnsw_snap");
    let port1 = free_port();

    let mut srv1 = start_server_in_dir(port1, &dir);
    assert!(
        wait_for_server(port1, Duration::from_secs(10)),
        "srv1 start"
    );

    {
        let mut c = Conn::connect(port1);
        assert!(
            c.cmds(&[
                "FT.CREATE",
                "idx:hnsw",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "emb:",
                "SCHEMA",
                "vec",
                "VECTOR",
                "HNSW",
                "12",
                "TYPE",
                "FLOAT32",
                "DIM",
                "4",
                "DISTANCE_METRIC",
                "COSINE",
                "M",
                "8",
                "EF_CONSTRUCTION",
                "100",
            ])
            .is_ok()
        );

        // Insert 10 vectors
        for i in 0..10u32 {
            let v: Vec<f32> = (0..4)
                .map(|j| if j == (i % 4) as usize { 1.0 } else { 0.0 })
                .collect();
            let key = format!("emb:{i}");
            c.cmd(&[b"HSET", key.as_bytes(), b"vec", &pack_f32(&v)]);
        }

        assert!(c.cmds(&["SAVE"]).is_ok());
    }

    send_shutdown_nosave(port1);
    assert!(
        wait_for_exit(&mut srv1, Duration::from_secs(8)),
        "srv1 stop"
    );

    let port2 = free_port();
    let mut srv2 = start_server_in_dir(port2, &dir);
    assert!(
        wait_for_server(port2, Duration::from_secs(10)),
        "srv2 start"
    );

    {
        let mut c = Conn::connect(port2);

        // idx:hnsw must survive
        let list = c.cmds(&["FT._LIST"]);
        let names: Vec<&str> = list.as_array().iter().map(|v| v.as_str()).collect();
        assert!(names.contains(&"idx:hnsw"), "idx:hnsw missing: {names:?}");

        // FT.INFO must show the right field type
        let info = c.cmds(&["FT.INFO", "idx:hnsw"]);
        let info_arr = info.as_array();
        // Find "index_name" field
        let pos = info_arr.iter().position(|v| {
            matches!(v, RespValue::SimpleStr(_) | RespValue::Bulk(_) if v.as_str() == "index_name")
        });
        assert!(pos.is_some(), "index_name key not in FT.INFO");
        assert_eq!(info_arr[pos.unwrap() + 1].as_str(), "idx:hnsw");

        // KNN must return a meaningful result (emb:0 = [1,0,0,0])
        let q = pack_f32(&[1.0, 0.0, 0.0, 0.0]);
        let res = c.cmd(&[
            b"FT.SEARCH",
            b"idx:hnsw",
            b"*=>[KNN 1 @vec $q AS score]",
            b"PARAMS",
            b"2",
            b"q",
            &q,
            b"RETURN",
            b"1",
            b"score",
            b"DIALECT",
            b"2",
        ]);
        let arr = res.as_array();
        assert_eq!(arr[0].as_int(), 1, "expected 1 result from HNSW");
        // emb:0 and emb:4 and emb:8 all have first dim = 1, any is acceptable
        let returned_key = arr[1].as_str();
        assert!(
            returned_key == "emb:0" || returned_key == "emb:4" || returned_key == "emb:8",
            "unexpected nearest key: {returned_key}"
        );
        let score: f32 = arr[2].as_array()[1].as_str().parse().unwrap();
        assert!(score < 1e-4, "cosine distance should be ~0, got {score}");
    }

    kill_server(&mut srv2);
    let _ = std::fs::remove_dir_all(&dir);
}

// ═════════════════════════════════════════════════════════════════════════════
// Snapshot persistence — full-text (inverted) index
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn text_index_survives_snapshot_restart() {
    let _serial = persist_serial();
    let dir = temp_dir("text_snap");
    let port1 = free_port();

    let mut srv1 = start_server_in_dir(port1, &dir);
    assert!(
        wait_for_server(port1, Duration::from_secs(10)),
        "srv1 start"
    );

    {
        let mut c = Conn::connect(port1);
        assert!(
            c.cmds(&[
                "FT.CREATE",
                "idx:text",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "article:",
                "SCHEMA",
                "body",
                "TEXT",
            ])
            .is_ok()
        );

        c.cmds(&[
            "HSET",
            "article:1",
            "body",
            "rust is a systems programming language",
        ]);
        c.cmds(&[
            "HSET",
            "article:2",
            "body",
            "python is great for data science",
        ]);
        c.cmds(&["HSET", "article:3", "body", "rust excels at memory safety"]);

        assert!(c.cmds(&["SAVE"]).is_ok());
    }

    send_shutdown_nosave(port1);
    assert!(
        wait_for_exit(&mut srv1, Duration::from_secs(8)),
        "srv1 stop"
    );

    let port2 = free_port();
    let mut srv2 = start_server_in_dir(port2, &dir);
    assert!(
        wait_for_server(port2, Duration::from_secs(10)),
        "srv2 start"
    );

    {
        let mut c = Conn::connect(port2);

        let list = c.cmds(&["FT._LIST"]);
        let names: Vec<&str> = list.as_array().iter().map(|v| v.as_str()).collect();
        assert!(names.contains(&"idx:text"), "idx:text missing: {names:?}");

        // "rust" appears in article:1 and article:3 — both must be found
        let res = c.cmds(&["FT.SEARCH", "idx:text", "@body:(rust)", "NOCONTENT"]);
        let arr = res.as_array();
        let count = arr[0].as_int();
        assert_eq!(
            count, 2,
            "expected 2 rust articles after restart, got {count}"
        );

        // Response with NOCONTENT: [count, key1, key2, ...]
        let keys: Vec<&str> = arr[1..].iter().map(|v| v.as_str()).collect();
        assert!(
            keys.contains(&"article:1"),
            "article:1 missing from results"
        );
        assert!(
            keys.contains(&"article:3"),
            "article:3 missing from results"
        );

        // "python" only in article:2
        let res2 = c.cmds(&["FT.SEARCH", "idx:text", "@body:(python)", "NOCONTENT"]);
        assert_eq!(res2.as_array()[0].as_int(), 1);
        assert_eq!(res2.as_array()[1].as_str(), "article:2");
    }

    kill_server(&mut srv2);
    let _ = std::fs::remove_dir_all(&dir);
}

// ═════════════════════════════════════════════════════════════════════════════
// Snapshot persistence — mixed TEXT + VECTOR index
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn mixed_text_vector_index_survives_restart() {
    let _serial = persist_serial();
    let dir = temp_dir("mixed_snap");
    let port1 = free_port();

    let mut srv1 = start_server_in_dir(port1, &dir);
    assert!(
        wait_for_server(port1, Duration::from_secs(10)),
        "srv1 start"
    );

    {
        let mut c = Conn::connect(port1);
        assert!(
            c.cmds(&[
                "FT.CREATE",
                "idx:mixed",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "item:",
                "SCHEMA",
                "title",
                "TEXT",
                "embedding",
                "VECTOR",
                "FLAT",
                "6",
                "TYPE",
                "FLOAT32",
                "DIM",
                "3",
                "DISTANCE_METRIC",
                "L2",
            ])
            .is_ok()
        );

        c.cmd(&[
            b"HSET",
            b"item:1",
            b"title",
            b"rust book",
            b"embedding",
            &pack_f32(&[1.0, 0.0, 0.0]),
        ]);
        c.cmd(&[
            b"HSET",
            b"item:2",
            b"title",
            b"python guide",
            b"embedding",
            &pack_f32(&[0.0, 1.0, 0.0]),
        ]);

        assert!(c.cmds(&["SAVE"]).is_ok());
    }

    send_shutdown_nosave(port1);
    assert!(
        wait_for_exit(&mut srv1, Duration::from_secs(8)),
        "srv1 stop"
    );

    let port2 = free_port();
    let mut srv2 = start_server_in_dir(port2, &dir);
    assert!(
        wait_for_server(port2, Duration::from_secs(10)),
        "srv2 start"
    );

    {
        let mut c = Conn::connect(port2);

        // Text search still works
        let res = c.cmds(&["FT.SEARCH", "idx:mixed", "@title:(rust)", "NOCONTENT"]);
        assert_eq!(res.as_array()[0].as_int(), 1);
        assert_eq!(res.as_array()[1].as_str(), "item:1");

        // Vector KNN still works
        let q = pack_f32(&[0.0, 1.0, 0.0]);
        let res = c.cmd(&[
            b"FT.SEARCH",
            b"idx:mixed",
            b"*=>[KNN 1 @embedding $q AS score]",
            b"PARAMS",
            b"2",
            b"q",
            &q,
            b"RETURN",
            b"1",
            b"score",
            b"DIALECT",
            b"2",
        ]);
        let arr = res.as_array();
        assert_eq!(arr[0].as_int(), 1);
        assert_eq!(arr[1].as_str(), "item:2");
        let score: f32 = arr[2].as_array()[1].as_str().parse().unwrap();
        assert!(score < 1e-5, "L2 to [0,1,0] should be ~0, got {score}");
    }

    kill_server(&mut srv2);
    let _ = std::fs::remove_dir_all(&dir);
}

// ═════════════════════════════════════════════════════════════════════════════
// Snapshot persistence — PREFIX filter is preserved
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn prefix_filter_survives_restart() {
    let _serial = persist_serial();
    let dir = temp_dir("prefix_snap");
    let port1 = free_port();

    let mut srv1 = start_server_in_dir(port1, &dir);
    assert!(
        wait_for_server(port1, Duration::from_secs(10)),
        "srv1 start"
    );

    {
        let mut c = Conn::connect(port1);
        // Index only covers "product:" keys, not "raw:"
        assert!(
            c.cmds(&[
                "FT.CREATE",
                "idx:products",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "product:",
                "SCHEMA",
                "vec",
                "VECTOR",
                "FLAT",
                "6",
                "TYPE",
                "FLOAT32",
                "DIM",
                "2",
                "DISTANCE_METRIC",
                "L2",
            ])
            .is_ok()
        );

        c.cmd(&[b"HSET", b"product:1", b"vec", &pack_f32(&[1.0, 0.0])]);
        c.cmd(&[b"HSET", b"product:2", b"vec", &pack_f32(&[0.0, 1.0])]);
        // This key doesn't match the prefix — must not appear in results
        c.cmd(&[b"HSET", b"raw:1", b"vec", &pack_f32(&[1.0, 0.0])]);

        assert!(c.cmds(&["SAVE"]).is_ok());
    }

    send_shutdown_nosave(port1);
    assert!(
        wait_for_exit(&mut srv1, Duration::from_secs(8)),
        "srv1 stop"
    );

    let port2 = free_port();
    let mut srv2 = start_server_in_dir(port2, &dir);
    assert!(
        wait_for_server(port2, Duration::from_secs(10)),
        "srv2 start"
    );

    {
        let mut c = Conn::connect(port2);

        // KNN 10 — raw:1 must NOT appear because prefix is "product:"
        let q = pack_f32(&[1.0, 0.0]);
        let res = c.cmd(&[
            b"FT.SEARCH",
            b"idx:products",
            b"*=>[KNN 10 @vec $q AS score]",
            b"PARAMS",
            b"2",
            b"q",
            &q,
            b"RETURN",
            b"1",
            b"score",
            b"DIALECT",
            b"2",
        ]);
        let arr = res.as_array();
        let count = arr[0].as_int();
        assert_eq!(count, 2, "expected exactly 2 product: results, got {count}");

        for i in (1..arr.len()).step_by(2) {
            let key = arr[i].as_str();
            assert!(
                key.starts_with("product:"),
                "non-product key {key} leaked through prefix filter after restart"
            );
        }
    }

    kill_server(&mut srv2);
    let _ = std::fs::remove_dir_all(&dir);
}

// ═════════════════════════════════════════════════════════════════════════════
// Snapshot persistence — multiple indexes survive together
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn multiple_indexes_survive_restart() {
    let _serial = persist_serial();
    let dir = temp_dir("multi_snap");
    let port1 = free_port();

    let mut srv1 = start_server_in_dir(port1, &dir);
    assert!(
        wait_for_server(port1, Duration::from_secs(10)),
        "srv1 start"
    );

    {
        let mut c = Conn::connect(port1);

        // Create two independent indexes
        assert!(
            c.cmds(&[
                "FT.CREATE",
                "idx:alpha",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "a:",
                "SCHEMA",
                "vec",
                "VECTOR",
                "FLAT",
                "6",
                "TYPE",
                "FLOAT32",
                "DIM",
                "2",
                "DISTANCE_METRIC",
                "L2",
            ])
            .is_ok()
        );

        assert!(
            c.cmds(&[
                "FT.CREATE",
                "idx:beta",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "b:",
                "SCHEMA",
                "text",
                "TEXT",
            ])
            .is_ok()
        );

        c.cmd(&[b"HSET", b"a:1", b"vec", &pack_f32(&[1.0, 0.0])]);
        c.cmd(&[b"HSET", b"a:2", b"vec", &pack_f32(&[0.0, 1.0])]);
        c.cmds(&["HSET", "b:1", "text", "hello world"]);
        c.cmds(&["HSET", "b:2", "text", "goodbye world"]);

        assert!(c.cmds(&["SAVE"]).is_ok());
    }

    send_shutdown_nosave(port1);
    assert!(
        wait_for_exit(&mut srv1, Duration::from_secs(8)),
        "srv1 stop"
    );

    let port2 = free_port();
    let mut srv2 = start_server_in_dir(port2, &dir);
    assert!(
        wait_for_server(port2, Duration::from_secs(10)),
        "srv2 start"
    );

    {
        let mut c = Conn::connect(port2);

        let list = c.cmds(&["FT._LIST"]);
        let names: std::collections::HashSet<&str> =
            list.as_array().iter().map(|v| v.as_str()).collect();
        assert!(names.contains("idx:alpha"), "idx:alpha missing: {names:?}");
        assert!(names.contains("idx:beta"), "idx:beta missing: {names:?}");

        // idx:alpha — vector search
        let q = pack_f32(&[1.0, 0.0]);
        let res = c.cmd(&[
            b"FT.SEARCH",
            b"idx:alpha",
            b"*=>[KNN 1 @vec $q AS score]",
            b"PARAMS",
            b"2",
            b"q",
            &q,
            b"RETURN",
            b"0",
            b"DIALECT",
            b"2",
        ]);
        assert_eq!(res.as_array()[0].as_int(), 1);
        assert_eq!(res.as_array()[1].as_str(), "a:1");

        // idx:beta — text search
        let res2 = c.cmds(&["FT.SEARCH", "idx:beta", "@text:(hello)", "NOCONTENT"]);
        assert_eq!(res2.as_array()[0].as_int(), 1);
        assert_eq!(res2.as_array()[1].as_str(), "b:1");
    }

    kill_server(&mut srv2);
    let _ = std::fs::remove_dir_all(&dir);
}

// ═════════════════════════════════════════════════════════════════════════════
// Snapshot persistence — FT.INFO schema details survive restart
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn ft_info_schema_correct_after_restart() {
    let _serial = persist_serial();
    let dir = temp_dir("info_snap");
    let port1 = free_port();

    let mut srv1 = start_server_in_dir(port1, &dir);
    assert!(
        wait_for_server(port1, Duration::from_secs(10)),
        "srv1 start"
    );

    {
        let mut c = Conn::connect(port1);
        assert!(
            c.cmds(&[
                "FT.CREATE",
                "idx:schema",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "doc:",
                "SCHEMA",
                "title",
                "TEXT",
                "tags",
                "TAG",
                "price",
                "NUMERIC",
                "emb",
                "VECTOR",
                "HNSW",
                "8",
                "TYPE",
                "FLOAT32",
                "DIM",
                "8",
                "DISTANCE_METRIC",
                "COSINE",
                "M",
                "16",
            ])
            .is_ok()
        );

        assert!(c.cmds(&["SAVE"]).is_ok());
    }

    send_shutdown_nosave(port1);
    assert!(
        wait_for_exit(&mut srv1, Duration::from_secs(8)),
        "srv1 stop"
    );

    let port2 = free_port();
    let mut srv2 = start_server_in_dir(port2, &dir);
    assert!(
        wait_for_server(port2, Duration::from_secs(10)),
        "srv2 start"
    );

    {
        let mut c = Conn::connect(port2);

        let info = c.cmds(&["FT.INFO", "idx:schema"]);
        let arr = info.as_array();

        // index_name must be correct
        let name_pos = arr
            .iter()
            .position(|v| v.try_as_str() == Some("index_name"))
            .expect("index_name not in FT.INFO");
        assert_eq!(arr[name_pos + 1].as_str(), "idx:schema");

        // num_fields must be 4
        let nf_pos = arr
            .iter()
            .position(|v| v.try_as_str() == Some("num_fields"))
            .expect("num_fields not in FT.INFO");
        assert_eq!(
            arr[nf_pos + 1].as_int(),
            4,
            "expected 4 fields after restart"
        );

        // "fields" array contains strings like "title (TEXT)", "tags (TAG)", etc.
        let fields_pos = arr
            .iter()
            .position(|v| v.try_as_str() == Some("fields"))
            .expect("fields not in FT.INFO");
        let fields_raw = arr[fields_pos + 1].as_array();
        // Each entry is "fieldname (TYPE)" — extract the name before " ("
        let field_names: Vec<&str> = fields_raw
            .iter()
            .filter_map(|v| v.try_as_str().and_then(|s| s.split(" (").next()))
            .collect();

        assert!(
            field_names.contains(&"title"),
            "title field missing after restart"
        );
        assert!(
            field_names.contains(&"tags"),
            "tags field missing after restart"
        );
        assert!(
            field_names.contains(&"price"),
            "price field missing after restart"
        );
        assert!(
            field_names.contains(&"emb"),
            "emb field missing after restart"
        );
    }

    kill_server(&mut srv2);
    let _ = std::fs::remove_dir_all(&dir);
}

// ═════════════════════════════════════════════════════════════════════════════
// Snapshot persistence — incremental indexing still works after restart
// (insert new keys after restart, they are indexed immediately)
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn incremental_indexing_works_after_restart() {
    let _serial = persist_serial();
    let dir = temp_dir("incr_snap");
    let port1 = free_port();

    let mut srv1 = start_server_in_dir(port1, &dir);
    assert!(
        wait_for_server(port1, Duration::from_secs(10)),
        "srv1 start"
    );

    {
        let mut c = Conn::connect(port1);
        // Create empty index (no data before save)
        assert!(
            c.cmds(&[
                "FT.CREATE",
                "idx:incr",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "n:",
                "SCHEMA",
                "vec",
                "VECTOR",
                "FLAT",
                "6",
                "TYPE",
                "FLOAT32",
                "DIM",
                "2",
                "DISTANCE_METRIC",
                "L2",
            ])
            .is_ok()
        );
        c.cmd(&[b"HSET", b"n:1", b"vec", &pack_f32(&[1.0, 0.0])]);
        assert!(c.cmds(&["SAVE"]).is_ok());
    }

    send_shutdown_nosave(port1);
    assert!(
        wait_for_exit(&mut srv1, Duration::from_secs(8)),
        "srv1 stop"
    );

    let port2 = free_port();
    let mut srv2 = start_server_in_dir(port2, &dir);
    assert!(
        wait_for_server(port2, Duration::from_secs(10)),
        "srv2 start"
    );

    {
        let mut c = Conn::connect(port2);

        // Insert new key on the restarted server — must be indexed
        c.cmd(&[b"HSET", b"n:2", b"vec", &pack_f32(&[0.0, 1.0])]);

        // KNN 2 should return both n:1 and n:2
        let q = pack_f32(&[1.0, 0.0]);
        let res = c.cmd(&[
            b"FT.SEARCH",
            b"idx:incr",
            b"*=>[KNN 2 @vec $q AS score]",
            b"PARAMS",
            b"2",
            b"q",
            &q,
            b"RETURN",
            b"0",
            b"DIALECT",
            b"2",
        ]);
        let arr = res.as_array();
        assert_eq!(arr[0].as_int(), 2, "both n:1 and n:2 should be indexed");

        // Response with RETURN 0: [count, key1, key2, ...]
        let keys: Vec<&str> = arr[1..].iter().map(|v| v.as_str()).collect();
        assert!(keys.contains(&"n:1"), "n:1 (pre-restart) missing");
        assert!(
            keys.contains(&"n:2"),
            "n:2 (post-restart) missing — incremental indexing broken"
        );
    }

    kill_server(&mut srv2);
    let _ = std::fs::remove_dir_all(&dir);
}

// ═════════════════════════════════════════════════════════════════════════════
// Snapshot persistence — FT.DROPINDEX after restart removes the index
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn dropindex_works_after_restart() {
    let _serial = persist_serial();
    let dir = temp_dir("drop_snap");
    let port1 = free_port();

    let mut srv1 = start_server_in_dir(port1, &dir);
    assert!(
        wait_for_server(port1, Duration::from_secs(10)),
        "srv1 start"
    );

    {
        let mut c = Conn::connect(port1);
        assert!(
            c.cmds(&[
                "FT.CREATE",
                "idx:todrop",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "x:",
                "SCHEMA",
                "text",
                "TEXT",
            ])
            .is_ok()
        );
        c.cmds(&["HSET", "x:1", "text", "hello"]);
        assert!(c.cmds(&["SAVE"]).is_ok());
    }

    send_shutdown_nosave(port1);
    assert!(
        wait_for_exit(&mut srv1, Duration::from_secs(8)),
        "srv1 stop"
    );

    let port2 = free_port();
    let mut srv2 = start_server_in_dir(port2, &dir);
    assert!(
        wait_for_server(port2, Duration::from_secs(10)),
        "srv2 start"
    );

    {
        let mut c = Conn::connect(port2);

        // Index exists
        let list = c.cmds(&["FT._LIST"]);
        let names: Vec<&str> = list.as_array().iter().map(|v| v.as_str()).collect();
        assert!(
            names.contains(&"idx:todrop"),
            "idx:todrop missing after restart"
        );

        // Drop it
        assert!(c.cmds(&["FT.DROPINDEX", "idx:todrop"]).is_ok());

        // Now gone
        let list2 = c.cmds(&["FT._LIST"]);
        let names2: Vec<&str> = list2.as_array().iter().map(|v| v.as_str()).collect();
        assert!(
            !names2.contains(&"idx:todrop"),
            "idx:todrop still present after DROPINDEX"
        );

        // Search returns an error
        assert!(
            c.cmds(&["FT.SEARCH", "idx:todrop", "*", "NOCONTENT"])
                .is_error(),
            "expected error searching dropped index"
        );
    }

    kill_server(&mut srv2);
    let _ = std::fs::remove_dir_all(&dir);
}

// ═════════════════════════════════════════════════════════════════════════════
// AOF — a rewritten log carries the index definition through a restart
// ═════════════════════════════════════════════════════════════════════════════

/// Start a server with AOF on, and whatever extra flags the case needs.
fn start_aof_server(port: u16, dir: &std::path::Path, extra: &[&str]) -> std::process::Child {
    let mut args = vec!["--port".to_string(), port.to_string(), "--aof".to_string()];
    args.extend(extra.iter().map(|s| s.to_string()));
    Command::new(kividb_bin())
        .args(&args)
        .current_dir(dir)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap()
}

fn create_aof_index(port: u16) {
    let mut c = Conn::connect(port);
    assert!(
        c.cmds(&[
            "FT.CREATE",
            "idx:aof",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "body",
            "TEXT",
        ])
        .is_ok()
    );
    c.cmds(&["HSET", "doc:1", "body", "hello world"]);

    // Trigger a checkpoint: dump.kdb plus an empty AOF delta.
    let bgrw = c.cmds(&["BGREWRITEAOF"]);
    assert!(!bgrw.is_error(), "BGREWRITEAOF failed: {bgrw:?}");
    // It is in progress from the reply on, until it is done — however long
    // a busy machine takes to get round to it.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while c
        .cmds(&["INFO", "persistence"])
        .as_str()
        .contains("aof_rewrite_in_progress:1")
    {
        assert!(
            std::time::Instant::now() < deadline,
            "the rewrite did not finish"
        );
        thread::sleep(Duration::from_millis(50));
    }
    // And the writer has swapped the files.
    thread::sleep(Duration::from_millis(500));
}

/// The index and its documents must come back from dump.kdb plus the AOF delta.
fn assert_index_recovers_from_aof(dir: &std::path::Path, extra: &[&str]) {
    let port = free_port();
    let mut srv = start_aof_server(port, dir, extra);
    assert!(
        wait_for_server(port, Duration::from_secs(10)),
        "restarted server did not come up"
    );

    {
        let mut c = Conn::connect(port);
        let list = c.cmds(&["FT._LIST"]);
        let names: Vec<&str> = list.as_array().iter().map(|v| v.as_str()).collect();
        assert!(
            names.contains(&"idx:aof"),
            "idx:aof did not survive the AOF: {names:?}"
        );

        let res = c.cmds(&["FT.SEARCH", "idx:aof", "@body:(hello)", "NOCONTENT"]);
        let arr = res.as_array();
        assert_eq!(
            arr[0].as_int(),
            1,
            "the indexed document was not searchable after recovery: {res:?}"
        );
    }

    send_shutdown_nosave(port);
    let _ = wait_for_exit(&mut srv, Duration::from_secs(8));
}

#[test]
fn bgrewriteaof_index_survives_restart() {
    let _serial = persist_serial();
    let dir = temp_dir("aof_ftcreate");
    let port1 = free_port();

    let mut srv1 = start_aof_server(port1, &dir, &[]);
    assert!(
        wait_for_server(port1, Duration::from_secs(10)),
        "srv1 start"
    );
    create_aof_index(port1);
    kill_server(&mut srv1);

    let aof_path = dir.join("appendonly.aof");
    assert!(aof_path.exists(), "appendonly.aof not found");
    assert!(
        dir.join("dump.kdb").exists(),
        "BGREWRITEAOF must write dump.kdb so restart can load the snapshot first"
    );
    let bytes = std::fs::read(&aof_path).unwrap();
    assert!(
        bytes.starts_with(b"KIVIDLT1"),
        "BGREWRITEAOF should reset the AOF to a delta on top of the dump"
    );

    assert_index_recovers_from_aof(&dir, &[]);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn bgrewriteaof_still_recovers_without_a_preamble_flag() {
    // Checkpoint (dump + delta) does not depend on --aof-preamble.
    let _serial = persist_serial();
    let dir = temp_dir("aof_ftcreate_legacy");
    let port1 = free_port();

    let mut srv1 = start_aof_server(port1, &dir, &["--aof-preamble", "false"]);
    assert!(
        wait_for_server(port1, Duration::from_secs(10)),
        "srv1 start"
    );
    create_aof_index(port1);
    kill_server(&mut srv1);

    assert!(
        dir.join("dump.kdb").exists(),
        "dump.kdb missing after BGREWRITEAOF"
    );
    let bytes = std::fs::read(dir.join("appendonly.aof")).unwrap();
    assert!(
        bytes.starts_with(b"KIVIDLT1"),
        "AOF should be a delta after checkpoint"
    );

    assert_index_recovers_from_aof(&dir, &["--aof-preamble", "false"]);
    let _ = std::fs::remove_dir_all(&dir);
}

// ═════════════════════════════════════════════════════════════════════════════
// Snapshot persistence — no index pollution: keys outside PREFIX are excluded
// after data is inserted before AND after restart
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn non_prefix_keys_excluded_before_and_after_restart() {
    let _serial = persist_serial();
    let dir = temp_dir("exclude_snap");
    let port1 = free_port();

    let mut srv1 = start_server_in_dir(port1, &dir);
    assert!(
        wait_for_server(port1, Duration::from_secs(10)),
        "srv1 start"
    );

    {
        let mut c = Conn::connect(port1);
        assert!(
            c.cmds(&[
                "FT.CREATE",
                "idx:scoped",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "scoped:",
                "SCHEMA",
                "vec",
                "VECTOR",
                "FLAT",
                "6",
                "TYPE",
                "FLOAT32",
                "DIM",
                "2",
                "DISTANCE_METRIC",
                "L2",
            ])
            .is_ok()
        );

        // Matching key
        c.cmd(&[b"HSET", b"scoped:good", b"vec", &pack_f32(&[1.0, 0.0])]);
        // Non-matching keys (wrong prefix)
        c.cmd(&[b"HSET", b"other:bad1", b"vec", &pack_f32(&[1.0, 0.0])]);
        c.cmd(&[b"HSET", b"bad2", b"vec", &pack_f32(&[1.0, 0.0])]);

        assert!(c.cmds(&["SAVE"]).is_ok());
    }

    send_shutdown_nosave(port1);
    assert!(
        wait_for_exit(&mut srv1, Duration::from_secs(8)),
        "srv1 stop"
    );

    let port2 = free_port();
    let mut srv2 = start_server_in_dir(port2, &dir);
    assert!(
        wait_for_server(port2, Duration::from_secs(10)),
        "srv2 start"
    );

    {
        let mut c = Conn::connect(port2);

        // Insert another non-matching key post-restart
        c.cmd(&[b"HSET", b"wrong:post", b"vec", &pack_f32(&[1.0, 0.0])]);

        let q = pack_f32(&[1.0, 0.0]);
        let res = c.cmd(&[
            b"FT.SEARCH",
            b"idx:scoped",
            b"*=>[KNN 10 @vec $q AS score]",
            b"PARAMS",
            b"2",
            b"q",
            &q,
            b"RETURN",
            b"0",
            b"DIALECT",
            b"2",
        ]);
        let arr = res.as_array();
        assert_eq!(arr[0].as_int(), 1, "only scoped:good should match");
        assert_eq!(arr[1].as_str(), "scoped:good");
    }

    kill_server(&mut srv2);
    let _ = std::fs::remove_dir_all(&dir);
}

// ═════════════════════════════════════════════════════════════════════════════
// Snapshot persistence — data keys survive even if index was dropped pre-save
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn dropped_index_not_restored_but_data_preserved() {
    let _serial = persist_serial();
    let dir = temp_dir("drop_data_snap");
    let port1 = free_port();

    let mut srv1 = start_server_in_dir(port1, &dir);
    assert!(
        wait_for_server(port1, Duration::from_secs(10)),
        "srv1 start"
    );

    {
        let mut c = Conn::connect(port1);
        assert!(
            c.cmds(&[
                "FT.CREATE",
                "idx:temporary",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "t:",
                "SCHEMA",
                "text",
                "TEXT",
            ])
            .is_ok()
        );
        c.cmds(&["HSET", "t:1", "text", "data should survive"]);

        // Drop the index before saving
        assert!(c.cmds(&["FT.DROPINDEX", "idx:temporary"]).is_ok());
        assert!(c.cmds(&["SAVE"]).is_ok());
    }

    send_shutdown_nosave(port1);
    assert!(
        wait_for_exit(&mut srv1, Duration::from_secs(8)),
        "srv1 stop"
    );

    let port2 = free_port();
    let mut srv2 = start_server_in_dir(port2, &dir);
    assert!(
        wait_for_server(port2, Duration::from_secs(10)),
        "srv2 start"
    );

    {
        let mut c = Conn::connect(port2);

        // Index must NOT exist (it was dropped before SAVE)
        let list = c.cmds(&["FT._LIST"]);
        let names: Vec<&str> = list.as_array().iter().map(|v| v.as_str()).collect();
        assert!(
            !names.contains(&"idx:temporary"),
            "dropped index incorrectly restored: {names:?}"
        );

        // Data must still be present
        assert_eq!(
            c.cmds(&["HGET", "t:1", "text"]).as_str(),
            "data should survive",
            "data key t:1 lost after restart"
        );
    }

    kill_server(&mut srv2);
    let _ = std::fs::remove_dir_all(&dir);
}

// ═════════════════════════════════════════════════════════════════════════════
// Snapshot persistence — HNSW with larger dataset: top-1 recall after restart
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn hnsw_large_dataset_top1_recall_after_restart() {
    let _serial = persist_serial();
    let dir = temp_dir("hnsw_large");
    let port1 = free_port();

    let mut srv1 = start_server_in_dir(port1, &dir);
    assert!(
        wait_for_server(port1, Duration::from_secs(10)),
        "srv1 start"
    );

    // The exact-match vector — we'll look for it after restart
    let needle: Vec<f32> = vec![0.42, 0.58, 0.11, 0.87, 0.23, 0.67, 0.45, 0.33];

    {
        let mut c = Conn::connect(port1);
        assert!(
            c.cmds(&[
                "FT.CREATE",
                "idx:large",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "v:",
                "SCHEMA",
                "vec",
                "VECTOR",
                "HNSW",
                "8",
                "TYPE",
                "FLOAT32",
                "DIM",
                "8",
                "DISTANCE_METRIC",
                "L2",
                "M",
                "16",
                "EF_CONSTRUCTION",
                "200",
            ])
            .is_ok()
        );

        // Insert 100 random-ish vectors
        for i in 0..100u32 {
            let v: Vec<f32> = (0..8).map(|j| ((i + j * 13) % 17) as f32 / 17.0).collect();
            let key = format!("v:{i}");
            c.cmd(&[b"HSET", key.as_bytes(), b"vec", &pack_f32(&v)]);
        }
        // Insert the needle at a known key
        c.cmd(&[b"HSET", b"v:needle", b"vec", &pack_f32(&needle)]);

        assert!(c.cmds(&["SAVE"]).is_ok());
    }

    send_shutdown_nosave(port1);
    assert!(
        wait_for_exit(&mut srv1, Duration::from_secs(8)),
        "srv1 stop"
    );

    let port2 = free_port();
    let mut srv2 = start_server_in_dir(port2, &dir);
    assert!(
        wait_for_server(port2, Duration::from_secs(10)),
        "srv2 start"
    );

    {
        let mut c = Conn::connect(port2);

        // Querying the exact needle vector — top-1 must be v:needle (dist=0)
        let res = c.cmd(&[
            b"FT.SEARCH",
            b"idx:large",
            b"*=>[KNN 1 @vec $q AS score]",
            b"PARAMS",
            b"2",
            b"q",
            &pack_f32(&needle),
            b"RETURN",
            b"1",
            b"score",
            b"DIALECT",
            b"2",
        ]);
        let arr = res.as_array();
        assert_eq!(arr[0].as_int(), 1, "expected 1 result");
        assert_eq!(
            arr[1].as_str(),
            "v:needle",
            "top-1 should be exact match v:needle after HNSW restart rebuild"
        );
        let score: f32 = arr[2].as_array()[1].as_str().parse().unwrap();
        assert!(
            score < 1e-5,
            "distance to exact match should be ~0, got {score}"
        );
    }

    kill_server(&mut srv2);
    let _ = std::fs::remove_dir_all(&dir);
}

// ═════════════════════════════════════════════════════════════════════════════
// Snapshot persistence — JSON index (ON JSON) survives restart
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn json_index_schema_survives_restart() {
    let _serial = persist_serial();
    let dir = temp_dir("json_snap");
    let port1 = free_port();

    let mut srv1 = start_server_in_dir(port1, &dir);
    assert!(
        wait_for_server(port1, Duration::from_secs(10)),
        "srv1 start"
    );

    {
        let mut c = Conn::connect(port1);
        // ON JSON index — we only check that the schema round-trips; JSON
        // indexing itself is a separate concern.
        assert!(
            c.cmds(&[
                "FT.CREATE",
                "idx:json",
                "ON",
                "JSON",
                "PREFIX",
                "1",
                "jdoc:",
                "SCHEMA",
                "$.name",
                "AS",
                "name",
                "TEXT",
            ])
            .is_ok()
        );

        assert!(c.cmds(&["SAVE"]).is_ok());
    }

    send_shutdown_nosave(port1);
    assert!(
        wait_for_exit(&mut srv1, Duration::from_secs(8)),
        "srv1 stop"
    );

    let port2 = free_port();
    let mut srv2 = start_server_in_dir(port2, &dir);
    assert!(
        wait_for_server(port2, Duration::from_secs(10)),
        "srv2 start"
    );

    {
        let mut c = Conn::connect(port2);

        let list = c.cmds(&["FT._LIST"]);
        let names: Vec<&str> = list.as_array().iter().map(|v| v.as_str()).collect();
        assert!(
            names.contains(&"idx:json"),
            "idx:json missing after restart: {names:?}"
        );

        // FT.INFO must show ON=JSON via the "index_on" field
        let info = c.cmds(&["FT.INFO", "idx:json"]);
        let arr = info.as_array();
        let on_pos = arr
            .iter()
            .position(|v| v.try_as_str() == Some("index_on"))
            .expect("index_on not in FT.INFO");
        assert_eq!(
            arr[on_pos + 1].as_str().to_uppercase(),
            "JSON",
            "expected ON=JSON in index_on after restart"
        );
    }

    kill_server(&mut srv2);
    let _ = std::fs::remove_dir_all(&dir);
}

// ═════════════════════════════════════════════════════════════════════════════
// Snapshot persistence — TAG + NUMERIC fields survive restart
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn tag_numeric_fields_survive_restart() {
    let _serial = persist_serial();
    let dir = temp_dir("tagnum_snap");
    let port1 = free_port();

    let mut srv1 = start_server_in_dir(port1, &dir);
    assert!(
        wait_for_server(port1, Duration::from_secs(10)),
        "srv1 start"
    );

    {
        let mut c = Conn::connect(port1);
        assert!(
            c.cmds(&[
                "FT.CREATE",
                "idx:tagnum",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "prod:",
                "SCHEMA",
                "category",
                "TAG",
                "price",
                "NUMERIC",
            ])
            .is_ok()
        );

        assert!(c.cmds(&["SAVE"]).is_ok());
    }

    send_shutdown_nosave(port1);
    assert!(
        wait_for_exit(&mut srv1, Duration::from_secs(8)),
        "srv1 stop"
    );

    let port2 = free_port();
    let mut srv2 = start_server_in_dir(port2, &dir);
    assert!(
        wait_for_server(port2, Duration::from_secs(10)),
        "srv2 start"
    );

    {
        let mut c = Conn::connect(port2);

        let list = c.cmds(&["FT._LIST"]);
        let names: Vec<&str> = list.as_array().iter().map(|v| v.as_str()).collect();
        assert!(
            names.contains(&"idx:tagnum"),
            "idx:tagnum missing: {names:?}"
        );

        let info = c.cmds(&["FT.INFO", "idx:tagnum"]);
        let arr = info.as_array();

        let nf_pos = arr
            .iter()
            .position(|v| v.try_as_str() == Some("num_fields"))
            .unwrap();
        assert_eq!(
            arr[nf_pos + 1].as_int(),
            2,
            "expected 2 fields (TAG + NUMERIC)"
        );
    }

    kill_server(&mut srv2);
    let _ = std::fs::remove_dir_all(&dir);
}

// ═════════════════════════════════════════════════════════════════════════════
// Snapshot persistence — synonyms, dictionaries, JSON documents
// ═════════════════════════════════════════════════════════════════════════════

/// Every string in a reply, however deeply nested.
fn strings_in(value: &RespValue, out: &mut Vec<String>) {
    if let Some(s) = value.try_as_str() {
        out.push(s.to_string());
    } else if let RespValue::Array(items) = value {
        for item in items {
            strings_in(item, out);
        }
    }
}

/// Synonym groups (`FT.SYNUPDATE`) and dictionaries (`FT.DICTADD`) were not
/// part of a snapshot: a restart came back without them. And an `ON JSON`
/// index came back with its schema and none of its documents.
#[test]
fn synonyms_dictionaries_and_json_documents_survive_restart() {
    let _serial = persist_serial();
    let dir = temp_dir("syn_dict_json");
    let port1 = free_port();
    let mut srv1 = start_server_in_dir(port1, &dir);
    assert!(
        wait_for_server(port1, Duration::from_secs(10)),
        "srv1 start"
    );
    {
        let mut c = Conn::connect(port1);
        assert!(
            c.cmds(&[
                "FT.CREATE",
                "idx:sd",
                "ON",
                "JSON",
                "PREFIX",
                "1",
                "sd:",
                "SCHEMA",
                "$.title",
                "AS",
                "title",
                "TEXT",
                "$.price",
                "AS",
                "price",
                "NUMERIC",
            ])
            .is_ok()
        );
        assert!(
            c.cmds(&["JSON.SET", "sd:1", "$", r#"{"title":"fast car","price":9}"#])
                .is_ok()
        );
        assert!(
            c.cmds(&[
                "JSON.SET",
                "sd:2",
                "$",
                r#"{"title":"slow boat","price":70}"#
            ])
            .is_ok()
        );
        assert!(
            c.cmds(&["FT.SYNUPDATE", "idx:sd", "vehicles", "car", "automobile"])
                .is_ok()
        );
        assert_eq!(
            c.cmds(&["FT.DICTADD", "brands", "kivi", "parcel"]).as_int(),
            2
        );
        assert!(c.cmds(&["SAVE"]).is_ok());
    }
    send_shutdown_nosave(port1);
    assert!(
        wait_for_exit(&mut srv1, Duration::from_secs(8)),
        "srv1 stop"
    );

    let port2 = free_port();
    let mut srv2 = start_server_in_dir(port2, &dir);
    assert!(
        wait_for_server(port2, Duration::from_secs(10)),
        "srv2 start"
    );
    {
        let mut c = Conn::connect(port2);

        let mut synonyms = Vec::new();
        strings_in(&c.cmds(&["FT.SYNDUMP", "idx:sd"]), &mut synonyms);
        synonyms.sort();
        synonyms.dedup();
        assert_eq!(synonyms, ["automobile", "car", "vehicles"]);

        let mut brands = Vec::new();
        strings_in(&c.cmds(&["FT.DICTDUMP", "brands"]), &mut brands);
        brands.sort();
        assert_eq!(brands, ["kivi", "parcel"]);

        // The documents are found by what the index reads of them.
        let found = c.cmds(&["FT.SEARCH", "idx:sd", "@title:car", "NOCONTENT"]);
        assert_eq!(found.as_array()[0].as_int(), 1, "{found:?}");
        assert_eq!(found.as_array()[1].as_str(), "sd:1");
        let found = c.cmds(&["FT.SEARCH", "idx:sd", "@price:[50 100]", "NOCONTENT"]);
        assert_eq!(found.as_array()[1].as_str(), "sd:2");
    }
    kill_server(&mut srv2);
    let _ = std::fs::remove_dir_all(&dir);
}

// ═════════════════════════════════════════════════════════════════════════════
// Snapshot persistence — vector element types
// ═════════════════════════════════════════════════════════════════════════════

/// A FLOAT64 HNSW field and a FLOAT16 FLAT field come back with their types:
/// after a restart their FLOAT64 and FLOAT16 blobs are still indexed, and
/// still found by a query blob of the same type.
#[test]
fn vector_types_survive_snapshot_restart() {
    let _serial = persist_serial();
    let dir = temp_dir("vector_types_snap");
    let f64s = |v: &[f64]| -> Vec<u8> { v.iter().flat_map(|x| x.to_le_bytes()).collect() };
    // 0.0, 1.0 and 2.0 in IEEE half precision.
    let f16s = |v: &[u16]| -> Vec<u8> { v.iter().flat_map(|x| x.to_le_bytes()).collect() };
    let (zero, one, two) = (0x0000u16, 0x3c00u16, 0x4000u16);

    let port1 = free_port();
    let mut srv1 = start_server_in_dir(port1, &dir);
    assert!(
        wait_for_server(port1, Duration::from_secs(10)),
        "srv1 start"
    );
    {
        let mut c = Conn::connect(port1);
        for (index, prefix, algo, ty) in [
            ("idx:f64", "d:", "HNSW", "FLOAT64"),
            ("idx:f16", "h:", "FLAT", "FLOAT16"),
        ] {
            assert!(
                c.cmds(&[
                    "FT.CREATE",
                    index,
                    "ON",
                    "HASH",
                    "PREFIX",
                    "1",
                    prefix,
                    "SCHEMA",
                    "vec",
                    "VECTOR",
                    algo,
                    "6",
                    "TYPE",
                    ty,
                    "DIM",
                    "2",
                    "DISTANCE_METRIC",
                    "L2",
                ])
                .is_ok(),
                "{index}"
            );
        }
        c.cmd(&[b"HSET", b"d:a", b"vec", &f64s(&[0.0, 0.0])]);
        c.cmd(&[b"HSET", b"d:b", b"vec", &f64s(&[2.0, 2.0])]);
        c.cmd(&[b"HSET", b"h:a", b"vec", &f16s(&[zero, zero])]);
        c.cmd(&[b"HSET", b"h:b", b"vec", &f16s(&[two, two])]);
        assert!(c.cmds(&["SAVE"]).is_ok());
    }
    send_shutdown_nosave(port1);
    assert!(
        wait_for_exit(&mut srv1, Duration::from_secs(8)),
        "srv1 stop"
    );

    let port2 = free_port();
    let mut srv2 = start_server_in_dir(port2, &dir);
    assert!(
        wait_for_server(port2, Duration::from_secs(10)),
        "srv2 start"
    );
    {
        let mut c = Conn::connect(port2);
        for (index, query, want) in [
            ("idx:f64", f64s(&[2.0, 1.5]), "d:b"),
            ("idx:f16", f16s(&[two, one]), "h:b"),
        ] {
            let r = c.cmd(&[
                b"FT.SEARCH",
                index.as_bytes(),
                b"*=>[KNN 2 @vec $q AS d]",
                b"PARAMS",
                b"2",
                b"q",
                &query,
                b"SORTBY",
                b"d",
                b"NOCONTENT",
                b"DIALECT",
                b"2",
            ]);
            let arr = r.as_array();
            assert_eq!(arr.first().map(|v| v.as_int()), Some(2), "{index}: {r:?}");
            assert_eq!(arr[1].as_str(), want, "{index}");
        }
        let info = format!("{:?}", c.cmds(&["FT.INFO", "idx:f64"]));
        assert!(info.contains(&format!("{:?}", b"FLOAT64")), "{info}");
    }
    kill_server(&mut srv2);
    let _ = std::fs::remove_dir_all(&dir);
}

// ═════════════════════════════════════════════════════════════════════════════
// TAG and NUMERIC postings after a restart
// ═════════════════════════════════════════════════════════════════════════════

/// Queries that the postings answer, for comparing a server with another.
const POSTINGS_QUERIES: [&str; 8] = [
    "@c:{red}",
    "@c:{red | blue}",
    "-@c:{red}",
    "@n:[10 (40]",
    "(@c:{blue} @n:[-inf 25])",
    "(@c:{gr*} | @n>=90)",
    "@n!=7",
    "*",
];

fn postings_answers(c: &mut Conn) -> Vec<Vec<String>> {
    let mut out = Vec::new();
    for query in POSTINGS_QUERIES {
        // A server still building its indexes says LOADING: asked again.
        let deadline = Instant::now() + Duration::from_secs(30);
        let reply = loop {
            let reply = c.cmds(&[
                "FT.SEARCH",
                "idx:post",
                query,
                "NOCONTENT",
                "LIMIT",
                "0",
                "10000",
            ]);
            match &reply {
                RespValue::Error(e) if e.starts_with("LOADING") && Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(50));
                }
                _ => break reply,
            }
        };
        let mut keys: Vec<String> = reply.as_array()[1..]
            .iter()
            .map(|v| v.as_str().to_string())
            .collect();
        keys.sort();
        out.push(keys);
    }
    let q = pack_f32(&[0.5, 0.5, 0.5, 0.5]);
    let reply = c.cmd(&[
        b"FT.SEARCH",
        b"idx:post",
        b"(@c:{blue})=>[KNN 5 @v $q]",
        b"NOCONTENT",
        b"PARAMS",
        b"2",
        b"q",
        &q,
        b"DIALECT",
        b"2",
    ]);
    out.push(
        reply.as_array()[1..]
            .iter()
            .map(|v| v.as_str().to_string())
            .collect(),
    );
    out
}

/// Documents, then writes that change what the postings hold.
fn postings_dataset(c: &mut Conn) {
    assert!(
        c.cmds(&[
            "FT.CREATE",
            "idx:post",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "p:",
            "SCHEMA",
            "c",
            "TAG",
            "n",
            "NUMERIC",
            "v",
            "VECTOR",
            "HNSW",
            "6",
            "TYPE",
            "FLOAT32",
            "DIM",
            "4",
            "DISTANCE_METRIC",
            "L2",
        ])
        .is_ok()
    );
    let colours = ["red", "blue", "green", "grey", "red,blue"];
    for i in 0..300 {
        let key = format!("p:{i}");
        let n = (i % 100).to_string();
        let v = pack_f32(&[i as f32 / 300.0, 0.5, (i % 7) as f32 / 7.0, 0.25]);
        c.cmd(&[
            b"HSET",
            key.as_bytes(),
            b"c",
            colours[i % 5].as_bytes(),
            b"n",
            n.as_bytes(),
            b"v",
            &v,
        ]);
    }
    for i in (0..300).step_by(7) {
        c.cmds(&["HDEL", &format!("p:{i}"), "c"]);
    }
    for i in (1..300).step_by(11) {
        c.cmds(&["HINCRBY", &format!("p:{i}"), "n", "50"]);
    }
    for i in (2..300).step_by(13) {
        c.cmds(&["DEL", &format!("p:{i}")]);
    }
    for i in (3..300).step_by(17) {
        c.cmds(&["RENAME", &format!("p:{i}"), &format!("p:r{i}")]);
    }
    c.cmds(&["HSET", "p:5", "c", "green"]);
}

#[test]
fn postings_are_the_same_after_a_snapshot_restart() {
    let _serial = persist_serial();
    let dir = temp_dir("postings_snap");
    let port1 = free_port();
    let mut srv1 = start_server_in_dir(port1, &dir);
    assert!(
        wait_for_server(port1, Duration::from_secs(10)),
        "srv1 start"
    );
    let before = {
        let mut c = Conn::connect(port1);
        postings_dataset(&mut c);
        let before = postings_answers(&mut c);
        assert!(c.cmds(&["SAVE"]).is_ok());
        before
    };
    assert!(before[0].len() > 50, "{before:?}");
    send_shutdown_nosave(port1);
    assert!(
        wait_for_exit(&mut srv1, Duration::from_secs(8)),
        "srv1 stop"
    );

    let port2 = free_port();
    let mut srv2 = start_server_in_dir(port2, &dir);
    assert!(
        wait_for_server(port2, Duration::from_secs(10)),
        "srv2 start"
    );
    {
        let mut c = Conn::connect(port2);
        assert_eq!(postings_answers(&mut c), before);
        // And kept up to date after.
        c.cmds(&["HSET", "p:0", "c", "red"]);
        c.cmds(&["DEL", "p:1"]);
        let mut expected = before[0].clone();
        if !expected.contains(&"p:0".to_string()) {
            expected.push("p:0".to_string());
        }
        expected.retain(|k| k != "p:1");
        expected.sort();
        assert_eq!(postings_answers(&mut c)[0], expected);
    }
    kill_server(&mut srv2);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn postings_are_the_same_after_an_aof_replay() {
    let _serial = persist_serial();
    let dir = temp_dir("postings_aof");
    let port1 = free_port();
    let mut srv1 = start_aof_server(port1, &dir, &[]);
    assert!(
        wait_for_server(port1, Duration::from_secs(10)),
        "srv1 start"
    );
    let before = {
        let mut c = Conn::connect(port1);
        postings_dataset(&mut c);
        // Written and synced: the AOF is flushed every second.
        thread::sleep(Duration::from_millis(1500));
        postings_answers(&mut c)
    };
    send_shutdown_nosave(port1);
    assert!(
        wait_for_exit(&mut srv1, Duration::from_secs(8)),
        "srv1 stop"
    );

    let port2 = free_port();
    let mut srv2 = start_aof_server(port2, &dir, &[]);
    assert!(
        wait_for_server(port2, Duration::from_secs(10)),
        "srv2 start"
    );
    {
        let mut c = Conn::connect(port2);
        assert_eq!(postings_answers(&mut c), before);
    }
    send_shutdown_nosave(port2);
    let _ = wait_for_exit(&mut srv2, Duration::from_secs(8));
    let _ = std::fs::remove_dir_all(&dir);
}
