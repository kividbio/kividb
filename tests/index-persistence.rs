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

/// A reply as text: its strings and numbers, spaced.
fn text_of(v: &RespValue) -> String {
    match v {
        RespValue::Array(items) => items.iter().map(text_of).collect::<Vec<_>>().join(" "),
        RespValue::Int(n) => n.to_string(),
        other => other.try_as_str().unwrap_or("?").to_string(),
    }
}

/// The stems of TEXT fields, the index's language and its language field
/// come back after a restart from the snapshot and the AOF written after it:
/// the same documents found, with the same scores.
#[test]
fn stems_and_languages_survive_restart() {
    let _serial = persist_serial();
    let dir = temp_dir("stems");
    let port1 = free_port();
    let mut srv1 = start_aof_server(port1, &dir, &[]);
    assert!(
        wait_for_server(port1, Duration::from_secs(10)),
        "srv1 start"
    );

    let queries: &[(&str, &str)] = &[
        ("en", "@t:preferences"),
        ("en", "dog"),
        ("en", "@n:run"),
        ("de", "häusern"),
        ("ml", "haus"),
    ];
    let run = |c: &mut Conn| -> Vec<String> {
        queries
            .iter()
            .map(|(index, query)| {
                let mut args = vec!["FT.SEARCH", index, query, "WITHSCORES", "NOCONTENT"];
                if *index == "ml" {
                    args.extend(["LANGUAGE", "german"]);
                }
                text_of(&c.cmds(&args))
            })
            .collect()
    };
    let before = {
        let mut c = Conn::connect(port1);
        for create in [
            &[
                "FT.CREATE",
                "en",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "en:",
                "SCHEMA",
                "t",
                "TEXT",
                "n",
                "TEXT",
                "NOSTEM",
            ][..],
            &[
                "FT.CREATE",
                "de",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "de:",
                "LANGUAGE",
                "german",
                "SCHEMA",
                "t",
                "TEXT",
            ],
            &[
                "FT.CREATE",
                "ml",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "ml:",
                "LANGUAGE_FIELD",
                "lang",
                "SCHEMA",
                "t",
                "TEXT",
            ],
        ] {
            assert!(c.cmds(create).is_ok());
        }
        c.cmds(&["HSET", "en:1", "t", "I prefer email", "n", "running"]);
        c.cmds(&["HSET", "de:1", "t", "Die Häuser sind schön"]);
        c.cmds(&["HSET", "ml:1", "t", "laufende Häuser", "lang", "german"]);
        // Into the snapshot: dump.kdb, and an AOF delta after it.
        assert!(!c.cmds(&["BGREWRITEAOF"]).is_error());
        let deadline = Instant::now() + Duration::from_secs(30);
        while c
            .cmds(&["INFO", "persistence"])
            .as_str()
            .contains("aof_rewrite_in_progress:1")
        {
            assert!(Instant::now() < deadline, "the rewrite did not finish");
            thread::sleep(Duration::from_millis(50));
        }
        thread::sleep(Duration::from_millis(500));
        // Into the AOF only.
        c.cmds(&["HSET", "en:2", "t", "running dogs"]);
        c.cmds(&["HSET", "de:2", "t", "ein Haus"]);
        c.cmds(&["HSET", "ml:2", "t", "Häuser", "lang", "german"]);
        let before = run(&mut c);
        assert!(before[0].contains("en:1"), "{before:?}");
        assert!(before[1].contains("en:2"), "{before:?}");
        assert!(!before[2].contains("en:"), "NOSTEM: {before:?}");
        assert!(
            before[3].contains("de:1") && before[3].contains("de:2"),
            "{before:?}"
        );
        assert!(
            before[4].contains("ml:1") && before[4].contains("ml:2"),
            "{before:?}"
        );
        before
    };
    kill_server(&mut srv1);

    let port2 = free_port();
    let mut srv2 = start_aof_server(port2, &dir, &[]);
    assert!(
        wait_for_server(port2, Duration::from_secs(10)),
        "srv2 start"
    );
    {
        let mut c = Conn::connect(port2);
        assert_eq!(run(&mut c), before);
        let info = text_of(&c.cmds(&["FT.INFO", "de"]));
        assert!(info.contains("default_language"), "{info}");
        let info = text_of(&c.cmds(&["FT.INFO", "ml"]));
        assert!(info.contains("language_field"), "{info}");
    }
    send_shutdown_nosave(port2);
    let _ = wait_for_exit(&mut srv2, Duration::from_secs(8));
    let _ = std::fs::remove_dir_all(&dir);
}

// ═════════════════════════════════════════════════════════════════════════════
// FT.DROPINDEX DD — the deletes are events, AOF records and survive a restart
// ═════════════════════════════════════════════════════════════════════════════

/// The AOF records of `aof`, each as its arguments joined by spaces.
fn aof_records(aof: &[u8]) -> Vec<String> {
    let mut records = Vec::new();
    let mut at = 0;
    let line = |at: &mut usize| -> Option<String> {
        let end = aof[*at..].windows(2).position(|w| w == b"\r\n")? + *at;
        let text = String::from_utf8_lossy(&aof[*at..end]).to_string();
        *at = end + 2;
        Some(text)
    };
    while at < aof.len() {
        let Some(head) = line(&mut at) else { break };
        let Some(count) = head.strip_prefix('*').and_then(|n| n.parse::<usize>().ok()) else {
            // Not a record: a preamble, which comes first if at all.
            continue;
        };
        let mut parts = Vec::with_capacity(count);
        for _ in 0..count {
            let len: usize = line(&mut at).unwrap()[1..].parse().unwrap();
            parts.push(String::from_utf8_lossy(&aof[at..at + len]).to_string());
            at += len + 2;
        }
        records.push(parts.join(" "));
    }
    records
}

/// `commands` with each run of DELs made one, its keys in order: the
/// documents of an index are deleted in no particular order, and sent in
/// as many DELs as it takes.
fn dels_merged(commands: &[String]) -> Vec<String> {
    let mut merged: Vec<String> = Vec::new();
    for command in commands {
        match (command.strip_prefix("DEL "), merged.last_mut()) {
            (Some(keys), Some(last)) if last.starts_with("DEL ") => {
                last.push(' ');
                last.push_str(keys);
            }
            _ => merged.push(command.clone()),
        }
    }
    for command in &mut merged {
        if let Some(keys) = command.strip_prefix("DEL ") {
            let mut keys: Vec<&str> = keys.split(' ').collect();
            keys.sort();
            *command = format!("DEL {}", keys.join(" "));
        }
    }
    merged
}

/// `FT.DROPINDEX … DD` deletes the documents, each with a `del` event as in
/// Redis, for a HASH index, a JSON one and one in another database. The AOF
/// holds a DEL of each document, then the drop without `DD`, as Redis 8.10
/// writes them, and a restart replays that: the documents stay gone, the
/// rest stays.
#[test]
fn dropindex_dd_is_logged_as_a_del_of_each_document_then_the_drop() {
    let _serial = persist_serial();
    let dir = temp_dir("aof_dd");
    let port1 = free_port();
    let mut srv1 = start_aof_server(port1, &dir, &[]);
    assert!(
        wait_for_server(port1, Duration::from_secs(10)),
        "srv1 start"
    );
    let mut c = Conn::connect(port1);
    assert!(
        c.cmds(&["CONFIG", "SET", "notify-keyspace-events", "Eg"])
            .is_ok()
    );
    let create = |c: &mut Conn, name: &str, on: &str, prefix: &str, field: &[&str]| {
        let mut args = vec!["FT.CREATE", name, "ON", on, "PREFIX", "1", prefix, "SCHEMA"];
        args.extend_from_slice(field);
        let reply = c.cmds(&args);
        assert!(reply.is_ok(), "{reply:?}");
    };
    create(&mut c, "idx:dd", "HASH", "doc:", &["body", "TEXT"]);
    create(
        &mut c,
        "idx:ddj",
        "JSON",
        "jdoc:",
        &["$.body", "AS", "body", "TEXT"],
    );
    c.cmds(&["HSET", "doc:1", "body", "hello"]);
    c.cmds(&["HSET", "doc:2", "body", "world"]);
    c.cmds(&["SET", "doc:3", "a string, not a document"]);
    c.cmds(&["HSET", "other:1", "body", "hello"]);
    c.cmds(&["JSON.SET", "jdoc:1", "$", r#"{"body":"hello"}"#]);
    c.cmds(&["HSET", "jdoc:2", "body", "a hash, not a JSON document"]);
    let mut db3 = Conn::connect(port1);
    assert!(db3.cmds(&["SELECT", "3"]).is_ok());
    create(&mut db3, "idx:dd3", "HASH", "d3:", &["body", "TEXT"]);
    db3.cmds(&["HSET", "d3:1", "body", "hello"]);
    // The same name in database 0: not a document of the index.
    c.cmds(&["HSET", "d3:1", "body", "database 0's"]);

    let mut sub = Conn::connect(port1);
    sub.cmds(&["PSUBSCRIBE", "__keyevent@*__:del"]);
    sub.cmds(&["SUBSCRIBE", "done"]);

    assert!(c.cmds(&["FT.DROPINDEX", "idx:dd", "DD"]).is_ok());
    assert!(c.cmds(&["FT.DROPINDEX", "idx:ddj", "dd"]).is_ok());
    assert!(db3.cmds(&["FT.DROPINDEX", "idx:dd3", "DD"]).is_ok());
    c.cmds(&["PUBLISH", "done", "1"]);
    let mut deleted = Vec::new();
    loop {
        let message = sub.read_value();
        let parts = message.as_array();
        if parts[0].as_str() == "message" {
            break;
        }
        deleted.push(format!("{} {}", parts[2].as_str(), parts[3].as_str()));
    }
    deleted.sort();
    assert_eq!(
        deleted,
        [
            "__keyevent@0__:del doc:1",
            "__keyevent@0__:del doc:2",
            "__keyevent@0__:del jdoc:1",
            "__keyevent@3__:del d3:1",
        ]
    );
    assert_eq!(c.cmds(&["EXISTS", "doc:1", "doc:2", "jdoc:1"]).as_int(), 0);
    assert_eq!(
        c.cmds(&["EXISTS", "doc:3", "other:1", "jdoc:2", "d3:1"])
            .as_int(),
        4
    );
    assert_eq!(db3.cmds(&["EXISTS", "d3:1"]).as_int(), 0);

    // Written and synced: the AOF is flushed every second.
    thread::sleep(Duration::from_millis(1500));
    let records = aof_records(&std::fs::read(dir.join("appendonly.aof")).unwrap());
    assert!(
        !records
            .iter()
            .any(|r| r.to_ascii_uppercase().ends_with(" DD")),
        "DD logged: {records:?}"
    );
    let first_del = records
        .iter()
        .position(|r| r.starts_with("DEL "))
        .unwrap_or_else(|| panic!("no DEL logged: {records:?}"));
    // Once a database other than 0 has been used, every record carries its
    // own `SELECT`: only those that change the database are kept here.
    let mut db = "0";
    let mut logged = Vec::new();
    for record in &records[first_del..] {
        if let Some(selected) = record.strip_prefix("SELECT ") {
            if selected == db {
                continue;
            }
            db = selected;
        }
        logged.push(record.clone());
    }
    assert_eq!(
        dels_merged(&logged),
        [
            "DEL doc:1 doc:2",
            "FT.DROPINDEX idx:dd",
            "DEL jdoc:1",
            "FT.DROPINDEX idx:ddj",
            "SELECT 3",
            "DEL d3:1",
            "FT.DROPINDEX idx:dd3",
        ]
    );

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
        assert_eq!(c.cmds(&["EXISTS", "doc:1", "doc:2", "jdoc:1"]).as_int(), 0);
        assert_eq!(
            c.cmds(&["EXISTS", "doc:3", "other:1", "jdoc:2", "d3:1"])
                .as_int(),
            4
        );
        assert!(c.cmds(&["SELECT", "3"]).is_ok());
        assert_eq!(c.cmds(&["EXISTS", "d3:1"]).as_int(), 0);
        assert!(c.cmds(&["FT._LIST"]).as_array().is_empty());
    }
    send_shutdown_nosave(port2);
    let _ = wait_for_exit(&mut srv2, Duration::from_secs(8));
    let _ = std::fs::remove_dir_all(&dir);
}

/// An AOF written by 1.1.2 has `FT.DROPINDEX … DD` in it, as it was typed,
/// from a server that kept the documents. Replayed now, it drops the index
/// and keeps them, as that server did. (A 1.2.0 release candidate logged
/// `DD` the same way and deleted them; nothing in the file tells the two
/// apart, and keeping is what loses no data.)
#[test]
fn a_dd_in_an_aof_written_by_1_1_2_keeps_the_documents() {
    let _serial = persist_serial();
    let dir = temp_dir("aof_old_dd");
    let records: &[&[&str]] = &[
        &["SELECT", "0"],
        &[
            "FT.CREATE",
            "idx:old",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "body",
            "TEXT",
        ],
        &["HSET", "doc:1", "body", "hello"],
        &["HSET", "other:1", "body", "hello"],
        &["FT.DROPINDEX", "idx:old", "DD"],
        &["HSET", "doc:2", "body", "after the drop"],
    ];
    let mut aof = Vec::new();
    for record in records {
        aof.extend_from_slice(format!("*{}\r\n", record.len()).as_bytes());
        for part in *record {
            aof.extend_from_slice(format!("${}\r\n{part}\r\n", part.len()).as_bytes());
        }
    }
    std::fs::write(dir.join("appendonly.aof"), aof).unwrap();

    let port = free_port();
    let mut srv = start_aof_server(port, &dir, &[]);
    assert!(
        wait_for_server(port, Duration::from_secs(10)),
        "server start"
    );
    {
        let mut c = Conn::connect(port);
        assert_eq!(c.cmds(&["EXISTS", "doc:1", "doc:2", "other:1"]).as_int(), 3);
        assert!(c.cmds(&["FT._LIST"]).as_array().is_empty());
    }
    send_shutdown_nosave(port);
    let _ = wait_for_exit(&mut srv, Duration::from_secs(8));
    let _ = std::fs::remove_dir_all(&dir);
}

// ═════════════════════════════════════════════════════════════════════════════
// Writes made after a checkpoint, and a crash: the HNSW graph that came with
// the snapshot must take in what the AOF delta replays on top of it
// ═════════════════════════════════════════════════════════════════════════════
//
// A checkpoint (BGREWRITEAOF, SAVE, BGSAVE with the AOF on) writes dump.kdb,
// with the HNSW graphs, and starts the AOF over as a delta. After a kill -9,
// startup loads the dump and replays the delta. The graph used to be taken
// as the snapshot had it: the vectors written after the checkpoint were
// counted in `num_docs` and their hashes were there, but no KNN query found
// them (0 of 1000 in the 1.1.3 gate); and a vector deleted or overwritten
// after it was still found under its key.

/// The vector numbered `seed`: eight floats, as good as random and distinct.
fn ckpt_vec(seed: u32) -> Vec<f32> {
    let mut x = (u64::from(seed) + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    (0..8)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x >> 40) as f32 / (1u64 << 24) as f32 + 0.01
        })
        .collect()
}

/// What one document holds, as the test wrote it.
#[derive(Clone)]
struct CkptDoc {
    emb: Option<u32>,
    flat: Option<u32>,
    tag: &'static str,
    num: u32,
    word: &'static str,
}

/// The documents of one database's index, and the vectors that keys held
/// once and must not be found under them any more.
#[derive(Default)]
struct CkptModel {
    db: usize,
    docs: std::collections::BTreeMap<String, CkptDoc>,
    gone: Vec<(String, &'static str, u32)>,
}

const CKPT_WORDS: [&str; 3] = ["apple", "banana", "cherry"];

impl CkptModel {
    fn index(&self) -> String {
        format!("idx:ckpt{}", self.db)
    }

    fn key(&self, i: u32) -> String {
        format!("d{}:{i}", self.db)
    }

    /// A number unique to (database, document, version), for its vectors.
    fn seed(&self, i: u32, version: u32, field: u32) -> u32 {
        (self.db as u32) * 1_000_000 + version * 100_000 + i * 2 + field
    }

    fn create(&self, c: &mut Conn) {
        assert!(c.cmds(&["SELECT", &self.db.to_string()]).is_ok());
        let prefix = format!("d{}:", self.db);
        let created = c.cmds(&[
            "FT.CREATE",
            &self.index(),
            "ON",
            "HASH",
            "PREFIX",
            "1",
            &prefix,
            "SCHEMA",
            "emb",
            "VECTOR",
            "HNSW",
            "6",
            "TYPE",
            "FLOAT32",
            "DIM",
            "8",
            "DISTANCE_METRIC",
            "COSINE",
            "flat",
            "VECTOR",
            "FLAT",
            "6",
            "TYPE",
            "FLOAT32",
            "DIM",
            "8",
            "DISTANCE_METRIC",
            "L2",
            "t",
            "TAG",
            "n",
            "NUMERIC",
            "body",
            "TEXT",
        ]);
        assert!(created.is_ok(), "FT.CREATE: {created:?}");
    }

    /// Write document `i` whole, at `version`.
    fn put(&mut self, c: &mut Conn, i: u32, version: u32) {
        let doc = CkptDoc {
            emb: Some(self.seed(i, version, 0)),
            flat: Some(self.seed(i, version, 1)),
            tag: if version == 0 { "even" } else { "later" },
            num: i,
            word: CKPT_WORDS[(i + version) as usize % 3],
        };
        self.forget_vectors(i, &doc);
        let key = self.key(i);
        let emb = pack_f32(&ckpt_vec(doc.emb.unwrap()));
        let flat = pack_f32(&ckpt_vec(doc.flat.unwrap()));
        let num = doc.num.to_string();
        let reply = c.cmd(&[
            b"HSET",
            key.as_bytes(),
            b"emb",
            &emb,
            b"flat",
            &flat,
            b"t",
            doc.tag.as_bytes(),
            b"n",
            num.as_bytes(),
            b"body",
            doc.word.as_bytes(),
        ]);
        assert!(!reply.is_error(), "HSET {key}: {reply:?}");
        self.docs.insert(key, doc);
    }

    /// Give document `i` a new HNSW vector only.
    fn new_emb(&mut self, c: &mut Conn, i: u32, version: u32) {
        let key = self.key(i);
        let mut doc = self.docs[&key].clone();
        doc.emb = Some(self.seed(i, version, 0));
        self.forget_vectors(i, &doc);
        let emb = pack_f32(&ckpt_vec(doc.emb.unwrap()));
        let reply = c.cmd(&[b"HSET", key.as_bytes(), b"emb", &emb]);
        assert!(!reply.is_error(), "HSET {key}: {reply:?}");
        self.docs.insert(key, doc);
    }

    /// Take the HNSW vector out of document `i`; the rest stays.
    fn drop_emb(&mut self, c: &mut Conn, i: u32) {
        let key = self.key(i);
        let mut doc = self.docs[&key].clone();
        doc.emb = None;
        self.forget_vectors(i, &doc);
        assert_eq!(c.cmds(&["HDEL", &key, "emb"]).as_int(), 1);
        self.docs.insert(key, doc);
    }

    fn delete(&mut self, c: &mut Conn, i: u32) {
        let key = self.key(i);
        let gone = CkptDoc {
            emb: None,
            flat: None,
            tag: "",
            num: 0,
            word: "",
        };
        self.forget_vectors(i, &gone);
        assert_eq!(c.cmds(&["DEL", &key]).as_int(), 1);
        self.docs.remove(&key);
    }

    /// The vectors document `i` holds now and will not after `next`.
    fn forget_vectors(&mut self, i: u32, next: &CkptDoc) {
        let key = self.key(i);
        if let Some(old) = self.docs.get(&key) {
            if let Some(seed) = old.emb.filter(|&s| next.emb != Some(s)) {
                self.gone.push((key.clone(), "emb", seed));
            }
            if let Some(seed) = old.flat.filter(|&s| next.flat != Some(s)) {
                self.gone.push((key.clone(), "flat", seed));
            }
        }
    }

    /// The nearest document to vector `seed` by `field`, and its distance.
    fn nearest(&self, c: &mut Conn, field: &str, seed: u32) -> Option<(String, f32)> {
        let query = format!("*=>[KNN 1 @{field} $q AS score EF_RUNTIME 400]");
        let q = pack_f32(&ckpt_vec(seed));
        let deadline = Instant::now() + Duration::from_secs(60);
        let reply = loop {
            let reply = c.cmd(&[
                b"FT.SEARCH",
                self.index().as_bytes(),
                query.as_bytes(),
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
            match &reply {
                RespValue::Error(e) if e.starts_with("LOADING") && Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(50));
                }
                _ => break reply,
            }
        };
        let arr = reply.as_array();
        if arr[0].as_int() == 0 {
            return None;
        }
        // A key the graph has and the keyspace does not comes back with no
        // fields: the graph said it is the nearest, so it counts as found.
        let score: f32 = match arr[2].as_array() {
            [_, score] => score.as_str().parse().unwrap(),
            _ => 0.0,
        };
        Some((arr[1].as_str().to_string(), score))
    }

    fn count(&self, c: &mut Conn, query: &str) -> i64 {
        let reply = c.cmds(&[
            "FT.SEARCH",
            &self.index(),
            query,
            "NOCONTENT",
            "LIMIT",
            "0",
            "0",
        ]);
        reply.as_array()[0].as_int()
    }

    /// Everything the server answers for this index is what the model says,
    /// or the reasons it is not.
    fn mismatches(&self, c: &mut Conn) -> Vec<String> {
        assert!(c.cmds(&["SELECT", &self.db.to_string()]).is_ok());
        let mut out = Vec::new();
        for field in ["emb", "flat"] {
            let held: Vec<(&String, u32)> = self
                .docs
                .iter()
                .filter_map(|(key, doc)| {
                    let seed = if field == "emb" { doc.emb } else { doc.flat };
                    seed.map(|s| (key, s))
                })
                .collect();
            let missing: Vec<&String> = held
                .iter()
                .filter(|(key, seed)| match self.nearest(c, field, *seed) {
                    Some((found, score)) => found != **key || score > 1e-4,
                    None => true,
                })
                .map(|(key, _)| *key)
                .collect();
            if !missing.is_empty() {
                out.push(format!(
                    "{}: {} of {} vectors of @{field} not found by KNN, e.g. {:?}",
                    self.index(),
                    missing.len(),
                    held.len(),
                    &missing[..missing.len().min(5)]
                ));
            }
            let ghosts: Vec<&String> = self
                .gone
                .iter()
                .filter(|(_, f, _)| *f == field)
                .filter(|(key, _, seed)| {
                    matches!(self.nearest(c, field, *seed),
                        Some((found, score)) if found == *key && score <= 1e-4)
                })
                .map(|(key, _, _)| key)
                .collect();
            if !ghosts.is_empty() {
                out.push(format!(
                    "{}: {} deleted or overwritten vectors of @{field} still found under their \
                     keys, e.g. {:?}",
                    self.index(),
                    ghosts.len(),
                    &ghosts[..ghosts.len().min(5)]
                ));
            }
        }
        let mut expect = |what: &str, query: &str, want: usize| {
            let got = self.count(c, query);
            if got != want as i64 {
                out.push(format!(
                    "{}: {what} {query}: {got}, want {want}",
                    self.index()
                ));
            }
        };
        for tag in ["even", "later"] {
            let want = self.docs.values().filter(|d| d.tag == tag).count();
            expect("TAG", &format!("@t:{{{tag}}}"), want);
        }
        let want = self.docs.values().filter(|d| d.num < 50).count();
        expect("NUMERIC", "@n:[0 49]", want);
        for word in CKPT_WORDS {
            let want = self.docs.values().filter(|d| d.word == word).count();
            expect("TEXT", &format!("@body:{word}"), want);
        }
        expect("all", "*", self.docs.len());
        out
    }
}

/// Before the checkpoint: `before` documents in each of two databases.
fn ckpt_models(c: &mut Conn, before: u32) -> Vec<CkptModel> {
    let mut models: Vec<CkptModel> = (0..2)
        .map(|db| CkptModel {
            db,
            ..Default::default()
        })
        .collect();
    for m in &mut models {
        m.create(c);
        for i in 0..before {
            m.put(c, i, 0);
        }
    }
    models
}

/// After the checkpoint: new documents (numbered from `from`), documents
/// overwritten whole or in their HNSW vector only, documents deleted,
/// vectors taken out — of the documents the checkpoint has, and of ones
/// written after it. Each `round` changes other documents than the last.
fn ckpt_delta(c: &mut Conn, models: &mut [CkptModel], from: u32, after: u32, round: u32) {
    let old = round * 60;
    let version = round * 3;
    for m in models.iter_mut() {
        assert!(c.cmds(&["SELECT", &m.db.to_string()]).is_ok());
        for i in from..from + after {
            m.put(c, i, 0);
        }
        for i in old..old + 15 {
            m.put(c, i, version + 1);
        }
        for i in old + 15..old + 30 {
            m.new_emb(c, i, version + 2);
        }
        for i in old + 30..old + 45 {
            m.delete(c, i);
        }
        for i in old + 45..old + 55 {
            m.drop_emb(c, i);
        }
        for i in from..from + 5 {
            m.delete(c, i);
        }
        for i in from + 5..from + 10 {
            m.new_emb(c, i, version + 3);
        }
    }
}

/// Wait until a checkpoint started by `cmd` is over.
fn ckpt_wait(c: &mut Conn, cmd: &str) {
    let reply = c.cmds(&[cmd]);
    assert!(!reply.is_error(), "{cmd}: {reply:?}");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let info = c.cmds(&["INFO", "persistence"]);
        let info = info.as_str();
        if !info.contains("aof_rewrite_in_progress:1") && !info.contains("rdb_bgsave_in_progress:1")
        {
            break;
        }
        assert!(Instant::now() < deadline, "{cmd} did not finish");
        thread::sleep(Duration::from_millis(50));
    }
    // And the writer has swapped the files.
    thread::sleep(Duration::from_millis(500));
}

/// Until the AOF holds the last write.
fn ckpt_wait_logged(dir: &Path) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let aof = std::fs::read(dir.join("appendonly.aof")).unwrap_or_default();
        if aof.windows(14).any(|w| w == b"ckpt-last-mark") {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the AOF never held the last write"
        );
        thread::sleep(Duration::from_millis(50));
    }
}

fn assert_models(c: &mut Conn, models: &[CkptModel], when: &str) {
    let wrong: Vec<String> = models.iter().flat_map(|m| m.mismatches(c)).collect();
    assert!(wrong.is_empty(), "{when}:\n  {}", wrong.join("\n  "));
}

/// Documents, a checkpoint by `checkpoint`, more writes, kill -9, restart:
/// every index answers as before the kill.
fn writes_after_a_checkpoint_survive_a_kill(checkpoint: &str) {
    const BEFORE: u32 = 120;
    const AFTER: u32 = 80;
    let _serial = persist_serial();
    let dir = temp_dir(&format!("ckpt_{}", checkpoint.to_lowercase()));
    let extra: [&str; 0] = [];

    let port1 = free_port();
    let mut srv1 = start_aof_server(port1, &dir, &extra);
    assert!(
        wait_for_server(port1, Duration::from_secs(10)),
        "srv1 start"
    );
    let mut models = {
        let mut c = Conn::connect(port1);
        let mut models = ckpt_models(&mut c, BEFORE);
        ckpt_wait(&mut c, checkpoint);
        ckpt_delta(&mut c, &mut models, BEFORE, AFTER, 0);
        assert!(c.cmds(&["SET", "ckpt-last-mark", "1"]).is_ok());
        assert_models(&mut c, &models, "before the kill");
        models
    };
    ckpt_wait_logged(&dir);
    assert!(
        dir.join("dump.kdb").exists(),
        "{checkpoint} wrote no dump.kdb"
    );
    kill_server(&mut srv1);

    let port2 = free_port();
    let mut srv2 = start_aof_server(port2, &dir, &extra);
    assert!(
        wait_for_server(port2, Duration::from_secs(10)),
        "srv2 start"
    );
    {
        let mut c = Conn::connect(port2);
        assert_models(
            &mut c,
            &models,
            &format!("after {checkpoint}, writes, kill -9, restart"),
        );
        // And it goes on from there: what is written now is found, and a
        // second restart (from the snapshot written at shutdown) agrees.
        ckpt_delta(&mut c, &mut models, BEFORE + AFTER, 20, 1);
        assert_models(&mut c, &models, "writes after the restart");
        assert!(c.cmds(&["SET", "ckpt-last-mark", "2"]).is_ok());
    }
    ckpt_wait_logged(&dir);
    kill_server(&mut srv2);

    let port3 = free_port();
    let mut srv3 = start_aof_server(port3, &dir, &extra);
    assert!(
        wait_for_server(port3, Duration::from_secs(10)),
        "srv3 start"
    );
    {
        let mut c = Conn::connect(port3);
        assert_models(&mut c, &models, "after a second kill -9 and restart");
    }
    send_shutdown_nosave(port3);
    let _ = wait_for_exit(&mut srv3, Duration::from_secs(8));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn hnsw_writes_after_a_bgrewriteaof_survive_a_kill() {
    writes_after_a_checkpoint_survive_a_kill("BGREWRITEAOF");
}

#[test]
fn hnsw_writes_after_a_save_survive_a_kill() {
    writes_after_a_checkpoint_survive_a_kill("SAVE");
}

#[test]
fn hnsw_writes_after_a_bgsave_survive_a_kill() {
    writes_after_a_checkpoint_survive_a_kill("BGSAVE");
}

/// Without the AOF a restart has the snapshot only: what was written after
/// the SAVE is gone, from the keys and from the graphs alike.
#[test]
fn hnsw_without_aof_comes_back_as_saved() {
    const BEFORE: u32 = 120;
    let _serial = persist_serial();
    let dir = temp_dir("ckpt_noaof");
    let port1 = free_port();
    let mut srv1 = start_server_in_dir(port1, &dir);
    assert!(
        wait_for_server(port1, Duration::from_secs(10)),
        "srv1 start"
    );
    let models = {
        let mut c = Conn::connect(port1);
        let models = ckpt_models(&mut c, BEFORE);
        assert!(c.cmds(&["SAVE"]).is_ok());
        ckpt_writes_after_save(&mut c);
        models
    };
    kill_server(&mut srv1);

    let port2 = free_port();
    let mut srv2 = start_server_in_dir(port2, &dir);
    assert!(
        wait_for_server(port2, Duration::from_secs(10)),
        "srv2 start"
    );
    {
        let mut c = Conn::connect(port2);
        assert_models(
            &mut c,
            &models,
            "after SAVE, writes, kill -9, restart without the AOF",
        );
    }
    send_shutdown_nosave(port2);
    let _ = wait_for_exit(&mut srv2, Duration::from_secs(8));
    let _ = std::fs::remove_dir_all(&dir);
}

/// Writes after the SAVE of the test above, which the restart must not have.
fn ckpt_writes_after_save(c: &mut Conn) {
    let mut models: Vec<CkptModel> = (0..2)
        .map(|db| CkptModel {
            db,
            ..Default::default()
        })
        .collect();
    for m in &mut models {
        assert!(c.cmds(&["SELECT", &m.db.to_string()]).is_ok());
        for i in 0..10 {
            m.put(c, i, 9);
        }
        for i in 500..520 {
            m.put(c, i, 0);
        }
        m.delete(c, 20);
    }
}

/// A replica: a full sync from a master with documents, writes after it
/// through the stream, then the replica killed and started on its own files
/// (its dump, written when it rebased its AOF on the sync, and its AOF).
#[test]
fn hnsw_writes_after_a_full_sync_survive_a_replica_kill() {
    const BEFORE: u32 = 120;
    const AFTER: u32 = 80;
    let _serial = persist_serial();
    let mdir = temp_dir("ckpt_master");
    let rdir = temp_dir("ckpt_replica");
    let extra: [&str; 0] = [];

    let mport = free_port();
    let mut master = start_server_in_dir(mport, &mdir);
    assert!(
        wait_for_server(mport, Duration::from_secs(10)),
        "master start"
    );
    let mut mc = Conn::connect(mport);
    let mut models = ckpt_models(&mut mc, BEFORE);

    let rport = free_port();
    let mport_s = mport.to_string();
    let mut replica_args = extra.to_vec();
    replica_args.extend(["--replicaof", "127.0.0.1", mport_s.as_str()]);
    let mut replica = start_aof_server(rport, &rdir, &replica_args);
    assert!(
        wait_for_server(rport, Duration::from_secs(10)),
        "replica start"
    );
    let mut rc = Conn::connect(rport);
    let deadline = Instant::now() + Duration::from_secs(60);
    while !rc
        .cmds(&["INFO", "replication"])
        .as_str()
        .contains("master_link_status:up")
    {
        assert!(Instant::now() < deadline, "the replica never synced");
        thread::sleep(Duration::from_millis(50));
    }

    ckpt_delta(&mut mc, &mut models, BEFORE, AFTER, 0);
    assert!(mc.cmds(&["SELECT", "0"]).is_ok());
    assert!(mc.cmds(&["SET", "ckpt-last-mark", "1"]).is_ok());
    let deadline = Instant::now() + Duration::from_secs(60);
    assert!(rc.cmds(&["SELECT", "0"]).is_ok());
    while rc.cmds(&["GET", "ckpt-last-mark"]) == RespValue::Nil {
        assert!(
            Instant::now() < deadline,
            "the replica never got the writes"
        );
        thread::sleep(Duration::from_millis(50));
    }
    assert_models(
        &mut rc,
        &models,
        "on the replica, after the full sync and the stream",
    );
    ckpt_wait_logged(&rdir);
    drop(rc);
    kill_server(&mut replica);
    send_shutdown_nosave(mport);
    let _ = wait_for_exit(&mut master, Duration::from_secs(8));

    let rport2 = free_port();
    let mut alone = start_aof_server(rport2, &rdir, &extra);
    assert!(
        wait_for_server(rport2, Duration::from_secs(10)),
        "replica restart"
    );
    {
        let mut c = Conn::connect(rport2);
        assert_models(&mut c, &models, "the replica's own files, after kill -9");
    }
    send_shutdown_nosave(rport2);
    let _ = wait_for_exit(&mut alone, Duration::from_secs(8));
    let _ = std::fs::remove_dir_all(&mdir);
    let _ = std::fs::remove_dir_all(&rdir);
}
