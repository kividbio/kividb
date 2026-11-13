// Quantized vector sets through restarts, AOF, replicas and old snapshots.

// The downgrade test needs `KIVIDB_1_2_BIN` pointing at a 1.2 binary.
#![allow(dead_code)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

// ── RESP client ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum V {
    Str(String),
    Bulk(Vec<u8>),
    Err(String),
    Int(i64),
    Arr(Vec<V>),
    Nil,
}

impl V {
    fn s(&self) -> String {
        match self {
            V::Str(s) | V::Err(s) => s.clone(),
            V::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
            other => panic!("not a string: {other:?}"),
        }
    }
    fn i(&self) -> i64 {
        match self {
            V::Int(n) => *n,
            other => panic!("not an integer: {other:?}"),
        }
    }
    fn arr(&self) -> &[V] {
        match self {
            V::Arr(v) => v,
            other => panic!("not an array: {other:?}"),
        }
    }
    fn is_err(&self) -> bool {
        matches!(self, V::Err(_))
    }
}

struct Conn {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Conn {
    fn connect(port: u16) -> Self {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Ok(s) = TcpStream::connect(("127.0.0.1", port)) {
                s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
                let writer = s.try_clone().unwrap();
                return Self {
                    reader: BufReader::new(s),
                    writer,
                };
            }
            assert!(Instant::now() < deadline, "could not connect to {port}");
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn cmd<A: AsRef<str>>(&mut self, args: &[A]) -> V {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            let a = a.as_ref();
            buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            buf.extend_from_slice(a.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&buf).unwrap();
        self.read()
    }

    fn read(&mut self) -> V {
        let mut line = String::new();
        self.reader.read_line(&mut line).expect("reply");
        let line = line.trim_end_matches(['\r', '\n']);
        let (kind, rest) = line.split_at(1);
        match kind {
            "+" => V::Str(rest.to_string()),
            "-" => V::Err(rest.to_string()),
            ":" => V::Int(rest.parse().unwrap()),
            "$" => {
                let len: i64 = rest.parse().unwrap();
                if len < 0 {
                    return V::Nil;
                }
                let mut data = vec![0u8; len as usize + 2];
                self.reader.read_exact(&mut data).unwrap();
                data.truncate(len as usize);
                V::Bulk(data)
            }
            "*" => {
                let n: i64 = rest.parse().unwrap();
                if n < 0 {
                    return V::Nil;
                }
                V::Arr((0..n).map(|_| self.read()).collect())
            }
            other => panic!("unexpected RESP type {other:?} in {line:?}"),
        }
    }

    fn info(&mut self, section: &str, field: &str) -> String {
        let reply = self.cmd(&["INFO", section]).s();
        reply
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{field}:")))
            .unwrap_or_else(|| panic!("no {field} in INFO {section}"))
            .trim()
            .to_string()
    }

    fn used_memory(&mut self) -> i64 {
        self.info("memory", "used_memory").parse().unwrap()
    }
}

// ── Server fixture ────────────────────────────────────────────────────────────

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
    use std::sync::atomic::AtomicU16;
    static NEXT: AtomicU16 = AtomicU16::new(0);
    loop {
        let step = NEXT.fetch_add(1, Ordering::Relaxed);
        let from = (std::process::id() as u16).wrapping_mul(43);
        let port = 10_000 + from.wrapping_add(step) % 20_000;
        if port_is_free(port) {
            return port;
        }
    }
}

fn spawn(bin: &Path, dir: &Path, port: u16, extra: &[String]) -> Child {
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("server.log"))
        .unwrap();
    let mut command = Command::new(bin);
    command.current_dir(dir).args([
        "--port",
        &port.to_string(),
        "--bind",
        "127.0.0.1",
        "--threads",
        "4",
        "--configfile",
        "kividb.conf",
    ]);
    if let Ok(model) = std::env::var("KIVIDB_TEST_IO_MODEL") {
        command.args(["--io-model", &model]);
    }
    command
        .args(extra)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(log)
        .spawn()
        .expect("could not start kividb")
}

fn this_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_kividb"))
}

struct Server {
    port: u16,
    dir: PathBuf,
    child: Child,
    args: Vec<String>,
}

impl Server {
    fn start(extra: &[&str]) -> Self {
        Self::start_in(tempfile::tempdir().unwrap().keep(), extra)
    }

    fn start_in(dir: PathBuf, extra: &[&str]) -> Self {
        let port = free_port();
        let args: Vec<String> = extra.iter().map(|s| s.to_string()).collect();
        let child = spawn(&this_bin(), &dir, port, &args);
        let mut server = Server {
            port,
            dir,
            child,
            args,
        };
        server.wait_up();
        server
    }

    fn conn(&self) -> Conn {
        Conn::connect(self.port)
    }

    fn log(&self) -> String {
        std::fs::read_to_string(self.dir.join("server.log")).unwrap_or_default()
    }

    fn shutdown(&mut self) {
        let mut c = self.conn();
        let _ = c.writer.write_all(b"*1\r\n$8\r\nSHUTDOWN\r\n");
        let deadline = Instant::now() + Duration::from_secs(20);
        while self.child.try_wait().unwrap().is_none() {
            assert!(Instant::now() < deadline, "the server did not shut down");
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// No final save: only the AOF can bring writes back.
    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    fn restart(&mut self) {
        self.child = spawn(&this_bin(), &self.dir, self.port, &self.args);
        self.wait_up();
    }

    fn wait_up(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let up = TcpStream::connect(("127.0.0.1", self.port)).and_then(|mut s| {
                s.set_read_timeout(Some(Duration::from_secs(10)))?;
                s.write_all(b"*1\r\n$4\r\nPING\r\n")?;
                let mut reply = [0u8; 7];
                s.read_exact(&mut reply)?;
                Ok(&reply == b"+PONG\r\n")
            });
            if up.unwrap_or(false) {
                return;
            }
            if let Ok(Some(status)) = self.child.try_wait() {
                panic!("the server exited ({status}):\n{}", self.log());
            }
            assert!(Instant::now() < deadline, "the server did not come up");
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn wait_until(what: &str, secs: u64, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn wait_caught_up(master: &mut Conn, replica: &mut Conn) {
    wait_until("the replica's link to come up", 30, || {
        replica.info("replication", "master_link_status") == "up"
    });
    wait_until("the replica to catch up", 60, || {
        let offset = master.info("replication", "master_repl_offset");
        replica.info("replication", "slave_repl_offset") == offset
    });
}

// ── Data helpers ──────────────────────────────────────────────────────────────

/// A deterministic vector with negative, positive and zero components.
fn vector(seed: u64, dim: usize) -> Vec<f32> {
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    (0..dim)
        .map(|i| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            if i % 17 == 5 {
                0.0
            } else {
                ((state >> 40) as f32 / (1u64 << 24) as f32 - 0.5) * 8.0
            }
        })
        .collect()
}

fn vadd(c: &mut Conn, key: &str, v: &[f32], element: &str, opts: &[&str]) -> V {
    let mut args = vec![
        "VADD".to_string(),
        key.to_string(),
        "VALUES".to_string(),
        v.len().to_string(),
    ];
    args.extend(v.iter().map(|x| x.to_string()));
    args.push(element.to_string());
    args.extend(opts.iter().map(|s| s.to_string()));
    c.cmd(&args)
}

const SETS: [(&str, &str, usize); 4] = [
    ("vq8", "Q8", 768),
    ("vbin", "BIN", 70),
    ("vnoquant", "NOQUANT", 128),
    ("vdefault", "", 128),
];

fn populate(c: &mut Conn) {
    for (key, opt, dim) in SETS {
        let opts: Vec<&str> = if opt.is_empty() { vec![] } else { vec![opt] };
        for n in 0..20u64 {
            let mut v = vector(n + dim as u64, dim);
            if n == 0 {
                v.iter_mut().for_each(|x| *x = 0.0);
            }
            let r = vadd(c, key, &v, &format!("e{n}"), &opts);
            assert_eq!(r, V::Int(1), "{key} e{n}");
        }
        c.cmd(&["VSETATTR", key, "e3", r#"{"n":3}"#]);
    }
}

/// Everything about the sets that a restart, a replay or a sync must keep.
#[derive(Debug, PartialEq)]
struct Snapshot {
    info: Vec<V>,
    raw: Vec<V>,
    vemb: Vec<V>,
    attrs: Vec<V>,
    sim: Vec<V>,
    usage: Vec<i64>,
}

fn capture(c: &mut Conn) -> Snapshot {
    let mut s = Snapshot {
        info: vec![],
        raw: vec![],
        vemb: vec![],
        attrs: vec![],
        sim: vec![],
        usage: vec![],
    };
    for (key, _, _) in SETS {
        s.info.push(c.cmd(&["VINFO", key]));
        s.usage
            .push(c.cmd(&["MEMORY", "USAGE", key, "SAMPLES", "0"]).i());
        s.sim
            .push(c.cmd(&["VSIM", key, "ELE", "e7", "COUNT", "5", "WITHSCORES"]));
        let card = c.cmd(&["VCARD", key]).i();
        for n in 0..40 {
            let e = format!("e{n}");
            s.raw.push(c.cmd(&["VEMB", key, &e, "RAW"]));
            s.vemb.push(c.cmd(&["VEMB", key, &e]));
            s.attrs.push(c.cmd(&["VGETATTR", key, &e]));
        }
        s.info.push(V::Int(card));
    }
    s
}

/// Names the first field that differs, instead of dumping both.
fn assert_same(a: &Snapshot, b: &Snapshot, what: &str) {
    let fields: [(&str, &Vec<V>, &Vec<V>); 5] = [
        ("info", &a.info, &b.info),
        ("raw", &a.raw, &b.raw),
        ("vemb", &a.vemb, &b.vemb),
        ("attrs", &a.attrs, &b.attrs),
        ("sim", &a.sim, &b.sim),
    ];
    for (name, x, y) in fields {
        for (i, (p, q)) in x.iter().zip(y.iter()).enumerate() {
            assert!(p == q, "{what}: {name}[{i}] differs:\n{p:?}\n{q:?}");
        }
        assert_eq!(x.len(), y.len(), "{what}: {name}");
    }
    assert_eq!(a.usage, b.usage, "{what}: MEMORY USAGE");
}

fn quant_type(c: &mut Conn, key: &str) -> String {
    let info = c.cmd(&["VINFO", key]);
    let info = info.arr();
    let at = info
        .iter()
        .position(|v| matches!(v, V::Bulk(b) if b == b"quant-type"))
        .expect("quant-type");
    info[at + 1].s()
}

fn check_types(c: &mut Conn) {
    assert_eq!(quant_type(c, "vq8"), "int8");
    assert_eq!(quant_type(c, "vbin"), "bin");
    assert_eq!(quant_type(c, "vnoquant"), "f32");
    assert_eq!(quant_type(c, "vdefault"), "f32");
    // Stored bytes: 1 per component, 1 bit per component, 4 per component.
    let raw = |c: &mut Conn, key: &str| match &c.cmd(&["VEMB", key, "e1", "RAW"]).arr()[1] {
        V::Bulk(b) => b.len(),
        other => panic!("{other:?}"),
    };
    assert_eq!(raw(c, "vq8"), 768);
    assert_eq!(raw(c, "vbin"), 16);
    assert_eq!(raw(c, "vnoquant"), 512);
}

/// A set made with a quantization keeps refusing others; a default one not.
fn check_rules(c: &mut Conn) {
    let v = vector(99, 768);
    let r = vadd(c, "vq8", &v, "x", &["BIN"]);
    assert_eq!(
        r,
        V::Err("ERR asked quantization mismatch with existing vector set".into())
    );
    let r = vadd(c, "vq8", &v, "x", &["NOQUANT"]);
    assert!(r.is_err(), "{r:?}");
    let v = vector(99, 128);
    assert!(vadd(c, "vnoquant", &v, "x", &["Q8"]).is_err());
    assert_eq!(vadd(c, "vdefault", &v, "x", &["Q8"]), V::Int(1));
    assert_eq!(quant_type(c, "vdefault"), "f32");
    c.cmd(&["VREM", "vdefault", "x"]);
}

// ── Restart, AOF, replication ─────────────────────────────────────────────────

#[test]
fn snapshot_restart_keeps_each_quantization() {
    let mut server = Server::start(&[]);
    let mut c = server.conn();
    populate(&mut c);
    check_types(&mut c);
    let before = capture(&mut c);
    assert_eq!(c.cmd(&["SAVE"]).s(), "OK");
    server.shutdown();
    server.restart();
    let mut c = server.conn();
    assert_same(&capture(&mut c), &before, "restart");
    check_types(&mut c);
    check_rules(&mut c);

    // Replacing a vector after the restart keeps the set's quantization.
    let old = c.cmd(&["VEMB", "vq8", "e2", "RAW"]);
    assert_eq!(
        vadd(&mut c, "vq8", &vector(1234, 768), "e2", &[]),
        V::Int(0)
    );
    let new = c.cmd(&["VEMB", "vq8", "e2", "RAW"]);
    assert_ne!(old, new);
    assert_eq!(new.arr()[0].s(), "int8");
    assert_eq!(c.cmd(&["VCARD", "vq8"]).i(), 20);
}

#[test]
fn aof_replay_keeps_each_quantization() {
    let mut server = Server::start(&["--aof"]);
    let mut c = server.conn();
    populate(&mut c);
    // Removed and replaced elements must replay too.
    for (key, _, dim) in SETS {
        assert_eq!(c.cmd(&["VREM", key, "e19"]).i(), 1);
        let opts: &[&str] = &[];
        assert_eq!(vadd(&mut c, key, &vector(777, dim), "e4", opts), V::Int(0));
    }
    let before = capture(&mut c);
    server.kill();
    server.restart();
    let mut c = server.conn();
    assert_same(&capture(&mut c), &before, "AOF replay");
    check_types(&mut c);
    check_rules(&mut c);
}

#[test]
fn replica_full_and_incremental_sync() {
    let master = Server::start(&[]);
    let mut m = master.conn();
    populate(&mut m);
    let port = master.port.to_string();
    let replica = Server::start(&["--replicaof", "127.0.0.1", &port]);
    let mut r = replica.conn();
    wait_caught_up(&mut m, &mut r);
    // Full sync: the master's snapshot.
    assert_same(&capture(&mut r), &capture(&mut m), "full sync");
    check_types(&mut r);

    // Incremental: the write stream.
    for (key, opt, dim) in SETS {
        let opts: Vec<&str> = if opt.is_empty() { vec![] } else { vec![opt] };
        assert_eq!(vadd(&mut m, key, &vector(5, dim), "e30", &opts), V::Int(1));
        assert_eq!(vadd(&mut m, key, &vector(6, dim), "e5", &[]), V::Int(0));
        assert_eq!(m.cmd(&["VREM", key, "e6"]).i(), 1);
    }
    // A refused write must not reach the replica.
    assert!(vadd(&mut m, "vq8", &vector(7, 768), "bad", &["BIN"]).is_err());
    assert!(vadd(&mut m, "newbin", &[f32::INFINITY, 1.0], "bad", &["BIN"]).is_err());
    wait_caught_up(&mut m, &mut r);
    assert_same(&capture(&mut r), &capture(&mut m), "incremental");
    assert_eq!(r.cmd(&["VISMEMBER", "vq8", "bad"]).i(), 0);
    assert_eq!(r.cmd(&["EXISTS", "newbin"]).i(), 0);
}

/// A 1.2 log may mix options on one key; replay must keep every write.
#[test]
fn aof_from_1_2_with_mixed_options_replays_every_write() {
    let dir = tempfile::tempdir().unwrap().keep();
    let records: [&[&str]; 3] = [
        &["VADD", "k", "VALUES", "3", "3", "-4", "0", "a", "Q8"],
        &["VADD", "k", "VALUES", "3", "1", "2", "3", "b", "NOQUANT"],
        &["VADD", "k", "VALUES", "3", "1", "0", "0", "c", "BIN"],
    ];
    let mut log = Vec::new();
    for args in records {
        log.extend(format!("*{}\r\n", args.len()).bytes());
        for a in args {
            log.extend(format!("${}\r\n{a}\r\n", a.len()).bytes());
        }
    }
    std::fs::write(dir.join("appendonly.aof"), log).unwrap();
    let server = Server::start_in(dir, &["--aof"]);
    let mut c = server.conn();
    assert_eq!(c.cmd(&["VCARD", "k"]).i(), 3, "{}", server.log());
    // The first record made the set Q8; the rest are kept in it.
    assert_eq!(quant_type(&mut c, "k"), "int8");
    // A client is still refused.
    assert!(vadd(&mut c, "k", &[1.0, 1.0, 1.0], "d", &["BIN"]).is_err());
}

/// A 1.2 master stores f32 whatever is asked; its replica must match it.
#[test]
#[ignore = "needs a 1.2 binary in KIVIDB_1_2_BIN"]
fn replica_of_1_2_master_matches_it() {
    let old = PathBuf::from(std::env::var("KIVIDB_1_2_BIN").expect("KIVIDB_1_2_BIN"));
    let dir = tempfile::tempdir().unwrap().keep();
    let port = free_port();
    let mut master = spawn(&old, &dir, port, &[]);
    let mut m = Conn::connect(port);
    wait_until(
        "the 1.2 master",
        20,
        || matches!(m.cmd(&["PING"]), V::Str(ref s) if s == "PONG"),
    );
    let replica = Server::start(&["--replicaof", "127.0.0.1", &port.to_string()]);
    let mut r = replica.conn();
    wait_caught_up(&mut m, &mut r);
    for (e, opt) in [("a", "Q8"), ("b", "NOQUANT"), ("c", "BIN"), ("d", "")] {
        let opts: Vec<&str> = if opt.is_empty() { vec![] } else { vec![opt] };
        assert!(matches!(
            vadd(&mut m, "k", &[3.0, -4.0, 0.5], e, &opts),
            V::Int(_)
        ));
    }
    wait_caught_up(&mut m, &mut r);
    assert_eq!(r.cmd(&["VCARD", "k"]).i(), 4);
    assert_eq!(quant_type(&mut r, "k"), "f32");
    assert_eq!(r.cmd(&["VEMB", "k", "a"]), m.cmd(&["VEMB", "k", "a"]));
    let _ = master.kill();
    let _ = master.wait();
    let _ = std::fs::remove_dir_all(&dir);
}

// ── Older snapshots ───────────────────────────────────────────────────────────

// Saved by KiviDB 1.2.1; its contents are asserted below.
const LEGACY: &str = "tests/fixtures/kividb-1.2-vset.kdb";

fn floats(reply: &V) -> Vec<f64> {
    reply.arr().iter().map(|v| v.s().parse().unwrap()).collect()
}

#[test]
fn snapshot_from_1_2_loads_as_f32() {
    let dir = tempfile::tempdir().unwrap().keep();
    std::fs::copy(LEGACY, dir.join("dump.kdb")).unwrap();
    let mut server = Server::start_in(dir, &[]);
    let mut c = server.conn();
    for key in ["legacy", "asked", "l2"] {
        assert_eq!(quant_type(&mut c, key), "f32", "{key}");
    }
    // Exact values, as 1.2 kept them.
    assert_eq!(floats(&c.cmd(&["VEMB", "legacy", "a"])), [3.0, -4.0, 0.0]);
    assert_eq!(
        floats(&c.cmd(&["VEMB", "legacy", "b"])),
        [0.1f32 as f64, 0.2f32 as f64, 0.3f32 as f64]
    );
    assert_eq!(c.cmd(&["VGETATTR", "legacy", "a"]).s(), r#"{"k":1}"#);
    assert_eq!(floats(&c.cmd(&["VEMB", "l2", "p"])), [1.0, 1.0]);
    assert_eq!(
        c.cmd(&[
            "VSIM", "legacy", "VALUES", "3", "3", "-4", "1", "COUNT", "1"
        ]),
        V::Arr(vec![V::Bulk(b"a".to_vec())])
    );

    // 1.2 accepted any quantization option; such sets still do.
    for (opt, added) in [("Q8", 1), ("BIN", 0), ("NOQUANT", 0)] {
        let r = vadd(&mut c, "asked", &[0.0, 0.0, 1.0], "z", &[opt]);
        assert_eq!(r, V::Int(added), "{opt}");
    }
    assert_eq!(quant_type(&mut c, "asked"), "f32");

    // Replaced after the upgrade, then kept through a save and restart.
    assert_eq!(
        vadd(&mut c, "legacy", &[5.0, 6.0, 7.0], "a", &["Q8"]),
        V::Int(0)
    );
    c.cmd(&["SAVE"]);
    server.shutdown();
    server.restart();
    let mut c = server.conn();
    assert_eq!(floats(&c.cmd(&["VEMB", "legacy", "a"])), [5.0, 6.0, 7.0]);
    assert_eq!(quant_type(&mut c, "legacy"), "f32");
    assert_eq!(
        vadd(&mut c, "legacy", &[1.0, 1.0, 1.0], "q", &["BIN"]),
        V::Int(1)
    );
}

/// A 1.2 binary must load plain sets and refuse quantized ones.
#[test]
#[ignore = "needs a 1.2 binary in KIVIDB_1_2_BIN"]
fn downgrade_to_1_2() {
    let old = PathBuf::from(std::env::var("KIVIDB_1_2_BIN").expect("KIVIDB_1_2_BIN"));
    let start_old = |dir: &Path| {
        let port = free_port();
        let mut child = spawn(&old, dir, port, &[]);
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Ok(Some(status)) = child.try_wait() {
                return Err(status);
            }
            if TcpStream::connect(("127.0.0.1", port)).is_ok() {
                return Ok((child, port));
            }
            assert!(Instant::now() < deadline, "1.2 neither started nor exited");
            std::thread::sleep(Duration::from_millis(50));
        }
    };

    // Only sets made without a quantization: 1.2 reads them unchanged.
    let mut server = Server::start(&[]);
    let mut c = server.conn();
    assert_eq!(
        vadd(&mut c, "plain", &[3.0, -4.0, 0.5], "a", &[]),
        V::Int(1)
    );
    c.cmd(&["SAVE"]);
    server.shutdown();
    let dir = server.dir.clone();
    let (mut child, port) = start_old(&dir).expect("1.2 refused a plain set");
    let mut c = Conn::connect(port);
    assert_eq!(floats(&c.cmd(&["VEMB", "plain", "a"])).len(), 3);
    let _ = child.kill();
    let _ = child.wait();

    // A quantized set: 1.2 must refuse the whole snapshot, not misread it.
    for opt in ["Q8", "BIN", "NOQUANT"] {
        let mut server = Server::start(&[]);
        let mut c = server.conn();
        assert_eq!(vadd(&mut c, "q", &[3.0, -4.0, 0.5], "a", &[opt]), V::Int(1));
        c.cmd(&["SAVE"]);
        server.shutdown();
        let image = std::fs::read(server.dir.join("dump.kdb")).unwrap();
        let status = start_old(&server.dir)
            .err()
            .unwrap_or_else(|| panic!("1.2 started on a {opt} set"));
        assert!(!status.success());
        let log = server.log();
        assert!(log.contains("unknown type tag 16"), "{opt}: {log}");
        // The refused snapshot is left as it was.
        assert_eq!(std::fs::read(server.dir.join("dump.kdb")).unwrap(), image);
    }
}

// ── Memory ────────────────────────────────────────────────────────────────────

/// Stored bytes drive MEMORY USAGE: about 4x less for Q8.
#[test]
fn memory_usage_follows_quantization() {
    let server = Server::start(&[]);
    let mut c = server.conn();
    let dim = 768;
    for (key, opt) in [("f", "NOQUANT"), ("q", "Q8"), ("b", "BIN")] {
        for n in 0..200 {
            vadd(&mut c, key, &vector(n, dim), &format!("e{n}"), &[opt]);
        }
    }
    let usage = |c: &mut Conn, key: &str| c.cmd(&["MEMORY", "USAGE", key, "SAMPLES", "0"]).i();
    let (f, q, b) = (usage(&mut c, "f"), usage(&mut c, "q"), usage(&mut c, "b"));
    let per = |total: i64| total / 200;
    assert!(per(f) >= 768 * 4, "f32 {f}");
    assert!(per(q) >= 768 && per(q) < 768 + 200, "q8 {q}");
    assert!(per(b) >= 96 && per(b) < 96 + 200, "bin {b}");
}

/// Waits for `used_memory` to fall back to within `slack` of `base`.
fn released(c: &mut Conn, base: i64, slack: i64, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let last = c.used_memory();
        if last - base <= slack {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{what}: used_memory {last}, {} above {base}",
            last - base
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[test]
fn memory_is_released() {
    let server = Server::start(&[]);
    let mut c = server.conn();
    let dim = 768;
    let n = 2000u64;
    // Writes made off the measured path, so `used_memory` settles first.
    c.cmd(&["SET", "warm", "1"]);
    std::thread::sleep(Duration::from_millis(300));
    let base = c.used_memory();
    // Room for client buffers; far below the megabytes loaded.
    let slack = 256 * 1024;

    let fill = |c: &mut Conn, key: &str, opt: &str| {
        for e in 0..n {
            vadd(c, key, &vector(e, dim), &format!("e{e}"), &[opt]);
        }
    };

    for opt in ["Q8", "BIN", "NOQUANT"] {
        // VREM of every element, which deletes the key.
        fill(&mut c, "k", opt);
        let grown = c.used_memory() - base;
        assert!(grown > (n as i64) * 96, "{opt}: grew only {grown}");
        for e in 0..n {
            c.cmd(&["VREM", "k", &format!("e{e}")]);
        }
        assert_eq!(c.cmd(&["EXISTS", "k"]).i(), 0);
        released(&mut c, base, slack, &format!("{opt} VREM"));

        // DEL.
        fill(&mut c, "k", opt);
        c.cmd(&["DEL", "k"]);
        released(&mut c, base, slack, &format!("{opt} DEL"));

        // FLUSHDB.
        fill(&mut c, "k", opt);
        c.cmd(&["FLUSHDB"]);
        released(&mut c, base, slack, &format!("{opt} FLUSHDB"));

        // Expiry.
        fill(&mut c, "k", opt);
        c.cmd(&["PEXPIRE", "k", "50"]);
        wait_until("the key to expire", 20, || c.cmd(&["EXISTS", "k"]).i() == 0);
        released(&mut c, base, slack, &format!("{opt} expiry"));
    }

    // Replacing a vector frees the old one: no growth over many rounds.
    vadd(&mut c, "r", &vector(0, dim), "e", &["Q8"]);
    let one = c.used_memory();
    for round in 0..3000 {
        vadd(&mut c, "r", &vector(round, dim), "e", &[]);
    }
    released(&mut c, one, 64 * 1024, "replacement");

    // Add/remove cycles return to where they started.
    for _ in 0..20 {
        for e in 0..200 {
            vadd(&mut c, "r", &vector(e, dim), &format!("c{e}"), &[]);
        }
        for e in 0..200 {
            c.cmd(&["VREM", "r", &format!("c{e}")]);
        }
    }
    released(&mut c, one, 64 * 1024, "add/remove cycles");
    c.cmd(&["DEL", "r"]);
    released(&mut c, base, slack, "final DEL");
}

/// A set loaded from a snapshot is freed when deleted.
#[test]
fn snapshot_reload_then_delete_releases_memory() {
    let mut server = Server::start(&[]);
    let mut c = server.conn();
    for e in 0..2000 {
        vadd(&mut c, "k", &vector(e, 768), &format!("e{e}"), &["Q8"]);
    }
    c.cmd(&["SAVE"]);
    server.shutdown();
    server.restart();
    let mut c = server.conn();
    let loaded = c.used_memory();
    assert_eq!(c.cmd(&["VCARD", "k"]).i(), 2000);
    c.cmd(&["DEL", "k"]);
    let want = loaded - 2000 * 768;
    released(&mut c, want, 256 * 1024, "DEL after load");
}
