// Quantized vector sets through restarts, AOF, replicas and old snapshots.

// The ignored tests need `KIVIDB_1_2_BIN`: a KiviDB 1.2 release binary.
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

fn encode(args: &[&[u8]]) -> Vec<u8> {
    let mut buf = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        buf.extend_from_slice(a);
        buf.extend_from_slice(b"\r\n");
    }
    buf
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

    fn cmd_bytes(&mut self, args: &[&[u8]]) -> V {
        self.writer.write_all(&encode(args)).unwrap();
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

    fn info_opt(&mut self, section: &str, field: &str) -> Option<String> {
        let reply = self.cmd(&["INFO", section]).s();
        reply
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{field}:")))
            .map(|v| v.trim().to_string())
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

fn spawn(bin: &Path, dir: &Path, port: u16, extra: &[String], env: &[(&str, &str)]) -> Child {
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
        .envs(env.iter().copied())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(log)
        .spawn()
        .expect("could not start kividb")
}

fn this_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_kividb"))
}

/// The 1.2 release binary. Asked for by the ignored tests: never skipped.
fn old_bin() -> PathBuf {
    let bin = std::env::var("KIVIDB_1_2_BIN")
        .expect("KIVIDB_1_2_BIN must name a KiviDB 1.2 release binary");
    let bin = PathBuf::from(bin);
    let out = Command::new(&bin)
        .arg("--version")
        .output()
        .unwrap_or_else(|e| panic!("{} does not run: {e}", bin.display()));
    let version = String::from_utf8_lossy(&out.stdout).trim().to_string();
    assert!(
        version.starts_with("kividb 1.2."),
        "{} is {version:?}, not a 1.2 release",
        bin.display()
    );
    bin
}

struct Server {
    port: u16,
    dir: PathBuf,
    /// Removed with the server, unless a test failed: then kept for its log.
    tmp: Option<tempfile::TempDir>,
    child: Child,
    bin: PathBuf,
    args: Vec<String>,
    /// Started as a master that hides its quantization capability.
    hides: bool,
}

/// What makes a 1.3 master answer as an older one would.
const HIDE: [(&str, &str); 1] = [("KIVIDB_TEST_NO_VSET_QUANT_CAPABILITY", "1")];

impl Server {
    fn start(extra: &[&str]) -> Self {
        Self::start_in(tempfile::tempdir().unwrap(), extra)
    }

    fn start_in(tmp: tempfile::TempDir, extra: &[&str]) -> Self {
        Self::start_bin(this_bin(), tmp, extra)
    }

    fn start_bin(bin: PathBuf, tmp: tempfile::TempDir, extra: &[&str]) -> Self {
        Self::start_with(bin, tmp, extra, false)
    }

    /// A 1.3 master whose `PSYNC` replies and `INFO` do not offer `vset-quant`.
    fn start_hiding(extra: &[&str]) -> Self {
        Self::start_with(this_bin(), tempfile::tempdir().unwrap(), extra, true)
    }

    fn start_with(bin: PathBuf, tmp: tempfile::TempDir, extra: &[&str], hides: bool) -> Self {
        let port = free_port();
        let args: Vec<String> = extra.iter().map(|s| s.to_string()).collect();
        let dir = tmp.path().to_path_buf();
        let env: &[(&str, &str)] = if hides { &HIDE } else { &[] };
        let child = spawn(&bin, &dir, port, &args, env);
        let mut server = Server {
            port,
            dir,
            tmp: Some(tmp),
            child,
            bin,
            args,
            hides,
        };
        server.wait_up();
        server
    }

    fn env(&self) -> &'static [(&'static str, &'static str)] {
        if self.hides { &HIDE } else { &[] }
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
        // Else the new process cannot bind, and the old one answers.
        assert!(self.child.try_wait().unwrap().is_some(), "still running");
        self.child = spawn(&self.bin, &self.dir, self.port, &self.args, self.env());
        self.wait_up();
    }

    /// Started again on the same files, as another binary or with other arguments.
    fn restart_as(&mut self, bin: PathBuf, extra: &[&str]) {
        self.bin = bin;
        self.args = extra.iter().map(|s| s.to_string()).collect();
        self.restart();
    }

    /// Started on the same files and exited: its status and log.
    fn try_restart_as(&mut self, bin: PathBuf, extra: &[&str]) -> Option<std::process::ExitStatus> {
        self.bin = bin;
        self.args = extra.iter().map(|s| s.to_string()).collect();
        assert!(self.child.try_wait().unwrap().is_some(), "still running");
        self.child = spawn(&self.bin, &self.dir, self.port, &self.args, self.env());
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Ok(Some(status)) = self.child.try_wait() {
                return Some(status);
            }
            if TcpStream::connect(("127.0.0.1", self.port)).is_ok() {
                self.wait_up();
                return None;
            }
            assert!(Instant::now() < deadline, "neither started nor exited");
            std::thread::sleep(Duration::from_millis(50));
        }
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
        if std::thread::panicking()
            && let Some(tmp) = self.tmp.take()
        {
            let kept = tmp.keep();
            eprintln!("kept {} for its server.log", kept.display());
        }
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
    check_rules_with(c, true);
}

/// `noquant_kept`: whether the NOQUANT set still knows it was asked for.
fn check_rules_with(c: &mut Conn, noquant_kept: bool) {
    let v = vector(99, 768);
    let r = vadd(c, "vq8", &v, "x", &["BIN"]);
    assert_eq!(
        r,
        V::Err("ERR asked quantization mismatch with existing vector set".into())
    );
    let r = vadd(c, "vq8", &v, "x", &["NOQUANT"]);
    assert!(r.is_err(), "{r:?}");
    let v = vector(99, 128);
    if noquant_kept {
        assert!(vadd(c, "vnoquant", &v, "x", &["Q8"]).is_err());
    } else {
        // Written as a plain f32 set, so that 1.2 reads it: taken as one.
        assert_eq!(vadd(c, "vnoquant", &v, "x", &["Q8"]), V::Int(1));
        assert_eq!(quant_type(c, "vnoquant"), "f32");
        c.cmd(&["VREM", "vnoquant", "x"]);
    }
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
    check_rules_with(&mut c, false);

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

// ── Logs and replicas of 1.2 ──────────────────────────────────────────────────

fn record(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend(format!("${}\r\n{a}\r\n", a.len()).bytes());
    }
    out
}

/// What 1.2 logged for these: the commands as sent.
fn records_of_1_2() -> Vec<u8> {
    [
        record(&["VADD", "k", "VALUES", "3", "3", "-4", "0", "a", "Q8"]),
        record(&["VADD", "k", "VALUES", "3", "1", "2", "3", "b", "NOQUANT"]),
        record(&["VADD", "k", "VALUES", "3", "1", "0", "0", "c", "BIN"]),
        record(&["VADD", "bin", "VALUES", "3", "1", "-1", "1", "a", "BIN"]),
        record(&["VADD", "q8", "VALUES", "3", "0.5", "1", "2", "a", "Q8"]),
    ]
    .concat()
}

/// 1.2 stored f32 whatever `VADD` asked: so does a replay of its log.
fn check_replayed_as_1_2(c: &mut Conn, log: &str) {
    assert_eq!(c.cmd(&["VCARD", "k"]).i(), 3, "{log}");
    for key in ["k", "bin", "q8"] {
        assert_eq!(quant_type(c, key), "f32", "{key}");
    }
    assert_eq!(floats(&c.cmd(&["VEMB", "k", "a"])), [3.0, -4.0, 0.0]);
    assert_eq!(floats(&c.cmd(&["VEMB", "q8", "a"])), [0.5, 1.0, 2.0]);
    // As 1.2 left them: any option is still taken.
    assert_eq!(vadd(c, "k", &[1.0, 1.0, 1.0], "d", &["BIN"]), V::Int(1));
    assert_eq!(quant_type(c, "k"), "f32");
}

/// A 1.2 log, plain or a delta, replays as 1.2 stored it.
#[test]
fn aof_from_1_2_replays_as_f32() {
    for delta in [false, true] {
        let tmp = tempfile::tempdir().unwrap();
        let mut log = Vec::new();
        if delta {
            log.extend_from_slice(b"KIVIDLT1");
            log.extend_from_slice(&0u64.to_le_bytes());
        }
        log.extend(records_of_1_2());
        std::fs::write(tmp.path().join("appendonly.aof"), log).unwrap();
        let server = Server::start_in(tmp, &["--aof"]);
        let mut c = server.conn();
        check_replayed_as_1_2(&mut c, &server.log());
    }
}

/// Upgraded in place: each record keeps its writer's meaning across restarts and rewrites.
#[test]
fn aof_upgrade_keeps_each_writers_meaning() {
    let tmp = tempfile::tempdir().unwrap();
    let mut log = b"KIVIDLT1".to_vec();
    log.extend_from_slice(&0u64.to_le_bytes());
    log.extend(records_of_1_2());
    std::fs::write(tmp.path().join("appendonly.aof"), log).unwrap();
    let mut server = Server::start_in(tmp, &["--aof"]);
    let mut c = server.conn();
    check_replayed_as_1_2(&mut c, &server.log());
    assert_eq!(
        vadd(&mut c, "n8", &[3.0, -4.0, 0.5], "a", &["Q8"]),
        V::Int(1)
    );
    assert_eq!(
        vadd(&mut c, "nbin", &[3.0, -4.0, 0.5], "a", &["BIN"]),
        V::Int(1)
    );
    assert_eq!(
        vadd(&mut c, "nf", &[3.0, -4.0, 0.5], "a", &["NOQUANT"]),
        V::Int(1)
    );
    let check = |c: &mut Conn, what: &str| {
        for (key, want) in [
            ("k", "f32"),
            ("bin", "f32"),
            ("q8", "f32"),
            ("n8", "int8"),
            ("nbin", "bin"),
            ("nf", "f32"),
        ] {
            assert_eq!(quant_type(c, key), want, "{what}: {key}");
        }
        assert_eq!(c.cmd(&["VCARD", "k"]).i(), 4, "{what}");
    };
    check(&mut c, "before");
    let before = [
        c.cmd(&["VEMB", "n8", "a", "RAW"]),
        c.cmd(&["VEMB", "nbin", "a", "RAW"]),
        c.cmd(&["VEMB", "k", "d", "RAW"]),
    ];

    server.kill();
    server.restart();
    let mut c = server.conn();
    check(&mut c, "replayed");
    let raw = |c: &mut Conn| {
        [
            c.cmd(&["VEMB", "n8", "a", "RAW"]),
            c.cmd(&["VEMB", "nbin", "a", "RAW"]),
            c.cmd(&["VEMB", "k", "d", "RAW"]),
        ]
    };
    assert_eq!(raw(&mut c), before);

    // A rewrite puts it all in the snapshot; the log starts again.
    rewrite(&mut c);
    assert_eq!(
        vadd(&mut c, "late", &[1.0, 2.0, 3.0], "a", &["Q8"]),
        V::Int(1)
    );
    server.kill();
    server.restart();
    let mut c = server.conn();
    check(&mut c, "after the rewrite");
    assert_eq!(raw(&mut c), before);
    assert_eq!(quant_type(&mut c, "late"), "int8");
}

/// What 1.3 writes after a `VADD` whose options it kept.
const MARKER: &str = "KIVIDB.AOF-VSET-QUANT";

/// The records of an AOF, past a delta header if it has one.
fn aof_records(log: &[u8]) -> Vec<Vec<String>> {
    let mut at = if log.starts_with(b"KIVIDLT1") { 16 } else { 0 };
    let mut out = Vec::new();
    let line = |at: &mut usize| -> Option<usize> {
        let end = log[*at..].windows(2).position(|w| w == b"\r\n")? + *at;
        let n = std::str::from_utf8(&log[*at + 1..end]).ok()?.parse().ok()?;
        *at = end + 2;
        Some(n)
    };
    while at < log.len() && log[at] == b'*' {
        let Some(argc) = line(&mut at) else { break };
        let mut args = Vec::new();
        for _ in 0..argc {
            let Some(len) = line(&mut at) else { return out };
            let Some(arg) = log.get(at..at + len) else {
                return out;
            };
            args.push(String::from_utf8_lossy(arg).into_owned());
            at += len + 2;
        }
        out.push(args);
    }
    out
}

fn aof_of(server: &Server) -> Vec<Vec<String>> {
    aof_records(&std::fs::read(server.dir.join("appendonly.aof")).unwrap())
}

fn is_option(arg: &str) -> bool {
    ["Q8", "BIN", "NOQUANT"]
        .iter()
        .any(|o| arg.eq_ignore_ascii_case(o))
}

fn vadd_with_option(record: &[String]) -> bool {
    record[0].eq_ignore_ascii_case("VADD") && record.iter().skip(2).any(|a| is_option(a))
}

/// Each marker follows a `VADD` with an option, and each such `VADD` has one.
fn assert_markers_follow_their_vadds(records: &[Vec<String>]) -> usize {
    let mut marked = 0;
    for (i, record) in records.iter().enumerate() {
        if record == &[MARKER] {
            let before = i.checked_sub(1).map(|b| &records[b]);
            assert!(
                before.is_some_and(|b| vadd_with_option(b)),
                "record {i}, a marker after {before:?}"
            );
            marked += 1;
            continue;
        }
        let next_is_marker = records.get(i + 1).is_some_and(|r| r == &[MARKER]);
        assert_eq!(
            vadd_with_option(record),
            next_is_marker,
            "record {i}: {record:?}"
        );
    }
    marked
}

fn rewrite(c: &mut Conn) {
    assert!(!c.cmd(&["BGREWRITEAOF"]).is_err());
    wait_until("the rewrite", 30, || {
        c.info("persistence", "aof_rewrite_in_progress") == "0"
            && c.info("persistence", "aof_last_bgrewrite_status") == "ok"
    });
}

/// A replica's log holds `VADD`s as applied: no option, so no marker.
fn assert_log_applied_as_f32(server: &Server) {
    let records = aof_of(server);
    assert!(!records.is_empty(), "no log at :{}", server.port);
    for record in &records {
        assert_ne!(record, &[MARKER], "a marker in the log of :{}", server.port);
        if record[0].eq_ignore_ascii_case("VADD") {
            assert!(
                !record.iter().skip(2).any(|a| is_option(a)),
                "{record:?} in the log of :{}",
                server.port
            );
        }
    }
}

// ── The AOF marker ────────────────────────────────────────────────────────────

const V3: [f32; 3] = [3.0, -4.0, 0.5];

fn opts_of(opt: &str) -> Vec<&str> {
    if opt.is_empty() { vec![] } else { vec![opt] }
}

/// The type `VEMB RAW` names for an element.
fn raw_type(c: &mut Conn, key: &str, element: &str) -> String {
    c.cmd(&["VEMB", key, element, "RAW"]).arr()[0].s()
}

/// Only a `VADD` with an option is marked, in a transaction too.
#[test]
fn the_aof_marks_only_vadds_with_an_option() {
    let mut server = Server::start(&["--aof"]);
    let mut c = server.conn();
    for (key, opt) in [("q", "Q8"), ("b", "bin"), ("n", "NOQUANT"), ("d", "")] {
        assert_eq!(vadd(&mut c, key, &V3, "a", &opts_of(opt)), V::Int(1));
        // Into the set it made: marked as well, and stored as the set is.
        assert_eq!(vadd(&mut c, key, &V3, "b", &opts_of(opt)), V::Int(1));
    }
    assert_eq!(vadd(&mut c, "q", &V3, "c", &[]), V::Int(1));
    c.cmd(&["VSETATTR", "q", "a", r#"{"n":1}"#]);
    assert_eq!(c.cmd(&["VREM", "b", "b"]).i(), 1);
    assert_eq!(c.cmd(&["SET", "Q8", "BIN"]).s(), "OK");
    assert_eq!(c.cmd(&["MULTI"]).s(), "OK");
    vadd(&mut c, "t", &V3, "a", &["Q8"]);
    c.cmd(&["SET", "s", "1"]);
    vadd(&mut c, "t", &V3, "b", &[]);
    vadd(&mut c, "u", &V3, "a", &["BIN"]);
    assert_eq!(c.cmd(&["EXEC"]).arr().len(), 4);
    // Refused: not logged.
    assert!(vadd(&mut c, "q", &V3, "x", &["BIN"]).is_err());

    let records = aof_of(&server);
    assert_eq!(assert_markers_follow_their_vadds(&records), 8);
    let multi = records.iter().position(|r| r == &["MULTI"]).unwrap();
    let names: Vec<&str> = records[multi..].iter().map(|r| r[0].as_str()).collect();
    assert_eq!(
        names,
        [
            "MULTI", "VADD", MARKER, "SET", "VADD", "VADD", MARKER, "EXEC"
        ]
    );

    server.kill();
    server.restart();
    let mut c = server.conn();
    for (key, want) in [
        ("q", "int8"),
        ("b", "bin"),
        ("n", "f32"),
        ("d", "f32"),
        ("t", "int8"),
        ("u", "bin"),
    ] {
        assert_eq!(quant_type(&mut c, key), want, "{key}");
    }
    assert_eq!(raw_type(&mut c, "q", "c"), "int8");
    assert_eq!(c.cmd(&["VCARD", "b"]).i(), 1);
    assert_eq!(c.cmd(&["GET", "s"]).s(), "1");
    // NOQUANT was asked for: still refuses another; the default set does not.
    assert!(vadd(&mut c, "n", &V3, "x", &["Q8"]).is_err());
    assert_eq!(vadd(&mut c, "d", &V3, "x", &["Q8"]), V::Int(1));
    assert_eq!(quant_type(&mut c, "d"), "f32");
}

/// Logs `VADD key a opt`, then cuts the log's last `cut` bytes.
fn torn_after(opt: &str, cut: usize) -> Server {
    let mut server = Server::start(&["--aof"]);
    let mut c = server.conn();
    assert_eq!(vadd(&mut c, "k", &V3, "a", &[opt]), V::Int(1));
    server.kill();
    let path = server.dir.join("appendonly.aof");
    let log = std::fs::read(&path).unwrap();
    assert!(log.ends_with(&record(&[MARKER])), "{opt}");
    std::fs::write(&path, &log[..log.len() - cut]).unwrap();
    server.restart();
    server
}

/// A crash cuts the marker: the set its `VADD` made is usable f32.
#[test]
fn a_torn_marker_leaves_the_set_f32() {
    for opt in ["Q8", "BIN", "NOQUANT"] {
        let mut server = torn_after(opt, 7);
        assert!(
            server.log().contains("is cut off"),
            "{opt}: {}",
            server.log()
        );
        let mut c = server.conn();
        assert_eq!(quant_type(&mut c, "k"), "f32", "{opt}");
        assert_eq!(
            floats(&c.cmd(&["VEMB", "k", "a"])),
            [3.0, -4.0, 0.5],
            "{opt}"
        );
        // As a set made with no option: every later `VADD` is taken, as f32.
        for (n, later) in ["Q8", "BIN", "NOQUANT", ""].iter().enumerate() {
            let v = vector(n as u64, 3);
            let r = vadd(&mut c, "k", &v, &format!("l{n}"), &opts_of(later));
            assert_eq!(r, V::Int(1), "{opt} then {later:?}");
            assert_eq!(raw_type(&mut c, "k", &format!("l{n}")), "f32", "{opt}");
            assert_eq!(vemb_floats(&mut c, "k", &format!("l{n}")), v, "{opt}");
        }
        assert_eq!(quant_type(&mut c, "k"), "f32", "{opt}");
        let sim = sim_names(&mut c, "k", "a");
        let usage = c.cmd(&["MEMORY", "USAGE", "k", "SAMPLES", "0"]);

        // And after another crash: the later writes replay the same.
        server.kill();
        server.restart();
        let mut c = server.conn();
        assert_eq!(quant_type(&mut c, "k"), "f32", "{opt}");
        assert_eq!(c.cmd(&["VCARD", "k"]).i(), 5, "{opt}");
        assert_eq!(floats(&c.cmd(&["VEMB", "k", "a"])), [3.0, -4.0, 0.5]);
        assert_eq!(sim_names(&mut c, "k", "a"), sim, "{opt}");
        assert_eq!(c.cmd(&["MEMORY", "USAGE", "k", "SAMPLES", "0"]), usage);
    }
}

/// A script's and a function's `VADD`s are marked like a client's.
#[cfg(feature = "lua")]
#[test]
fn script_vadds_are_marked() {
    let mut server = Server::start(&["--aof"]);
    let mut c = server.conn();
    let script = "return redis.call('VADD', KEYS[1], 'VALUES', 3, 3, -4, 0.5, 'a', ARGV[1])";
    assert_eq!(c.cmd(&["EVAL", script, "1", "q", "Q8"]), V::Int(1));
    let library = format!(
        "#!lua name=vlib\nredis.register_function('vadd', function(keys, args) {} end)",
        script
            .replace("KEYS[1]", "keys[1]")
            .replace("ARGV[1]", "args[1]")
    );
    assert!(!c.cmd(&["FUNCTION", "LOAD", &library]).is_err());
    assert_eq!(c.cmd(&["FCALL", "vadd", "1", "b", "BIN"]), V::Int(1));
    assert_eq!(c.cmd(&["EVAL", script, "1", "f", "NOQUANT"]), V::Int(1));
    assert_eq!(assert_markers_follow_their_vadds(&aof_of(&server)), 3);
    server.kill();
    server.restart();
    let mut c = server.conn();
    for (key, want) in [("q", "int8"), ("b", "bin"), ("f", "f32")] {
        assert_eq!(quant_type(&mut c, key), want, "{key}");
    }
    // NOQUANT kept by its marker: Q8 is refused.
    assert!(vadd(&mut c, "f", &V3, "x", &["Q8"]).is_err());
}

/// The marker whole: the set is as asked.
#[test]
fn a_whole_marker_keeps_the_option() {
    // Stored bytes compared with a server that never restarted.
    let reference = Server::start(&[]);
    let mut r = reference.conn();
    for (opt, want) in [("Q8", "int8"), ("BIN", "bin"), ("NOQUANT", "f32")] {
        let mut server = torn_after(opt, 0);
        let mut c = server.conn();
        assert_eq!(quant_type(&mut c, "k"), want, "{opt}");
        assert_eq!(raw_type(&mut c, "k", "a"), want, "{opt}");
        assert_eq!(vadd(&mut r, opt, &V3, "a", &[opt]), V::Int(1));
        assert_eq!(
            c.cmd(&["VEMB", "k", "a", "RAW"]),
            r.cmd(&["VEMB", opt, "a", "RAW"]),
            "{opt}"
        );
        // Asked for, so another option is refused, NOQUANT too.
        let other = if opt == "Q8" { "BIN" } else { "Q8" };
        assert!(vadd(&mut c, "k", &V3, "x", &[other]).is_err(), "{opt}");
        server.kill();
    }
}

/// Only a set's first `VADD` reads its marker; later ones follow the set.
#[test]
fn a_torn_marker_after_a_later_vadd_changes_nothing() {
    for (opt, want) in [("Q8", "int8"), ("BIN", "bin")] {
        let mut server = Server::start(&["--aof"]);
        let mut c = server.conn();
        assert_eq!(vadd(&mut c, "k", &V3, "a", &[opt]), V::Int(1));
        assert_eq!(vadd(&mut c, "k", &vector(1, 3), "b", &[opt]), V::Int(1));
        let raw = c.cmd(&["VEMB", "k", "b", "RAW"]);
        server.kill();
        let path = server.dir.join("appendonly.aof");
        let log = std::fs::read(&path).unwrap();
        std::fs::write(&path, &log[..log.len() - 7]).unwrap();
        server.restart();
        let mut c = server.conn();
        assert_eq!(quant_type(&mut c, "k"), want, "{opt}");
        assert_eq!(c.cmd(&["VEMB", "k", "b", "RAW"]), raw, "{opt}");
    }
}

/// A rewrite saves each set as stored; the log starts again, without markers.
#[test]
fn aof_rewrite_keeps_each_set_as_stored() {
    let mut server = Server::start(&["--aof"]);
    let mut c = server.conn();
    populate(&mut c);
    let before = capture(&mut c);
    // Three sets with an option, 20 `VADD`s each.
    assert_eq!(assert_markers_follow_their_vadds(&aof_of(&server)), 60);
    let size = |s: &Server| {
        std::fs::metadata(s.dir.join("appendonly.aof"))
            .unwrap()
            .len()
    };
    for round in 0..3 {
        rewrite(&mut c);
        assert!(aof_of(&server).is_empty(), "round {round}");
        assert!(size(&server) <= 16, "round {round}: {}", size(&server));
        server.kill();
        server.restart();
        c = server.conn();
        assert_same(&capture(&mut c), &before, &format!("rewrite {round}"));
        check_types(&mut c);
    }
    // Saved as f32, a NOQUANT set reloads as one made with no option.
    check_rules_with(&mut c, false);
    // Written after the rewrite: marked once each, the two above included.
    assert_eq!(assert_markers_follow_their_vadds(&aof_of(&server)), 2);
    assert_eq!(vadd(&mut c, "vq8", &vector(5, 768), "late", &[]), V::Int(1));
    assert_eq!(vadd(&mut c, "late", &V3, "a", &["BIN"]), V::Int(1));
    assert_eq!(assert_markers_follow_their_vadds(&aof_of(&server)), 3);
    server.kill();
    server.restart();
    let mut c = server.conn();
    assert_eq!(quant_type(&mut c, "late"), "bin");
    assert_eq!(raw_type(&mut c, "vq8", "late"), "int8");
}

/// `BACKUP STATUS`'s state.
fn backup_state(c: &mut Conn) -> String {
    let reply = c.cmd(&["BACKUP", "STATUS"]);
    let fields = reply.arr();
    let at = fields
        .iter()
        .position(|v| matches!(v, V::Bulk(b) if b == b"state"))
        .expect("state");
    fields[at + 1].s()
}

/// Backup increments mark `VADD`s like the AOF; restored sets match the source.
#[test]
fn a_backup_increment_keeps_each_quantization() {
    let server = Server::start(&[]);
    let mut c = server.conn();
    assert_eq!(vadd(&mut c, "base", &V3, "a", &["Q8"]), V::Int(1));
    assert!(!c.cmd(&["BACKUP", "START"]).is_err());
    wait_until("the increment", 20, || {
        backup_state(&mut c) == "incrementing"
    });
    populate(&mut c);
    assert_eq!(c.cmd(&["MULTI"]).s(), "OK");
    vadd(&mut c, "t", &V3, "a", &["Q8"]);
    c.cmd(&["SET", "s", "1"]);
    assert_eq!(c.cmd(&["EXEC"]).arr().len(), 2);
    let want = capture(&mut c);
    assert!(!c.cmd(&["BACKUP", "SEAL"]).is_err());
    let files: Vec<String> = c.cmd(&["BACKUP", "LIST"]).arr().iter().map(V::s).collect();
    let incr = files.iter().find(|f| f.ends_with(".incr.aof")).unwrap();
    let records = aof_records(&std::fs::read(incr).unwrap());
    assert_eq!(assert_markers_follow_their_vadds(&records), 61);

    let manifest = files.iter().find(|f| f.ends_with(".manifest")).unwrap();
    let restored = Server::start(&["--preload-file", &format!("aof:{manifest}")]);
    let mut r = restored.conn();
    assert_same(&capture(&mut r), &want, "restored");
    check_types(&mut r);
    assert_eq!(quant_type(&mut r, "base"), "int8");
    assert_eq!(quant_type(&mut r, "t"), "int8");
    assert_eq!(r.cmd(&["GET", "s"]).s(), "1");
}

/// As f32: 1.2 prints shortest f32 text, 1.3 prints doubles.
fn vemb_floats(c: &mut Conn, key: &str, element: &str) -> Vec<f32> {
    let reply = c.cmd(&["VEMB", key, element]);
    reply.arr().iter().map(|v| v.s().parse().unwrap()).collect()
}

fn sim_names(c: &mut Conn, key: &str, element: &str) -> Vec<V> {
    c.cmd(&["VSIM", key, "ELE", element, "COUNT", "10", "WITHSCORES"])
        .arr()
        .to_vec()
}

const OPTIONS: [&str; 4] = ["Q8", "BIN", "NOQUANT", ""];

/// Writes each option's sets `{prefix}0..3` on the 1.2 master.
fn write_to_old(m: &mut Conn, round: u64, prefix: &str) {
    for (n, opt) in OPTIONS.iter().enumerate() {
        let opts: Vec<&str> = if opt.is_empty() { vec![] } else { vec![opt] };
        for e in 0..5u64 {
            let v = vector(round * 100 + n as u64 * 10 + e, 16);
            let r = vadd(
                m,
                &format!("{prefix}{n}"),
                &v,
                &format!("e{round}-{e}"),
                &opts,
            );
            assert!(matches!(r, V::Int(_)), "{r:?}");
        }
    }
}

fn keys(prefixes: &[&str]) -> Vec<String> {
    prefixes
        .iter()
        .flat_map(|p| (0..OPTIONS.len()).map(move |n| format!("{p}{n}")))
        .collect()
}

/// Each set as the 1.2 master keeps it: f32, the values exactly.
fn assert_matches_old(old: &mut Conn, r: &mut Conn, prefixes: &[&str], what: &str) {
    for key in keys(prefixes) {
        assert_eq!(quant_type(r, &key), "f32", "{what}: {key}");
        assert_eq!(r.cmd(&["VCARD", &key]), old.cmd(&["VCARD", &key]), "{what}");
        let names: Vec<String> = old
            .cmd(&["VRANDMEMBER", &key, "-1000"])
            .arr()
            .iter()
            .map(V::s)
            .collect();
        for e in names {
            assert_eq!(
                vemb_floats(r, &key, &e),
                vemb_floats(old, &key, &e),
                "{what}: {key} {e}"
            );
        }
    }
}

/// Two 1.3 nodes keep the same bytes, scores and sizes.
fn assert_same_sets(a: &mut Conn, b: &mut Conn, prefixes: &[&str], what: &str) {
    for key in keys(prefixes) {
        assert_eq!(a.cmd(&["VINFO", &key]), b.cmd(&["VINFO", &key]), "{what}");
        let usage = |c: &mut Conn| c.cmd(&["MEMORY", "USAGE", &key, "SAMPLES", "0"]);
        assert_eq!(usage(a), usage(b), "{what}: {key}");
        let mut names: Vec<String> = a
            .cmd(&["VRANDMEMBER", &key, "-1000"])
            .arr()
            .iter()
            .map(V::s)
            .collect();
        names.sort();
        names.dedup();
        let first = names.first().expect("a member");
        assert_eq!(
            sim_names(a, &key, first),
            sim_names(b, &key, first),
            "{what}: {key}"
        );
        for e in names {
            assert_eq!(
                a.cmd(&["VEMB", &key, &e, "RAW"]),
                b.cmd(&["VEMB", &key, &e, "RAW"]),
                "{what}: {key} {e}"
            );
        }
    }
}

/// 1.3 replicas of 1.2 store f32 through sync, replay and promotion.
#[test]
#[ignore = "needs a 1.2 binary in KIVIDB_1_2_BIN"]
fn replicas_of_1_2_match_it_through_promotion() {
    let old = old_bin();
    let master = Server::start_bin(old, tempfile::tempdir().unwrap(), &[]);
    let mut m = master.conn();
    write_to_old(&mut m, 0, "s");
    let mport = master.port.to_string();
    let mut r1 = Server::start(&["--aof", "--replicaof", "127.0.0.1", &mport]);
    let mut r2 = Server::start(&["--aof", "--replicaof", "127.0.0.1", &mport]);
    let (mut c1, mut c2) = (r1.conn(), r2.conn());
    wait_caught_up(&mut m, &mut c1);
    wait_caught_up(&mut m, &mut c2);
    // Full sync, then the stream.
    write_to_old(&mut m, 1, "s");
    // Sets the stream makes, not the snapshot.
    write_to_old(&mut m, 1, "u");
    wait_caught_up(&mut m, &mut c1);
    wait_caught_up(&mut m, &mut c2);
    assert_matches_old(&mut m, &mut c1, &["s", "u"], "R1");
    assert_matches_old(&mut m, &mut c2, &["s", "u"], "R2");
    assert_same_sets(&mut c1, &mut c2, &["s", "u"], "R1 and R2");
    // Neither says it keeps quantization: its stream does not.
    for c in [&mut m, &mut c1, &mut c2] {
        assert_eq!(c.info_opt("server", "kividb_vset_quantization"), None);
    }

    // A partial resync: R1's link dropped, and continued.
    let partial = m.info("stats", "sync_partial_ok").parse::<u64>().unwrap();
    assert!(!c1.cmd(&["CLIENT", "KILL", "TYPE", "master"]).is_err());
    wait_until("R1 to be continued", 30, || {
        m.info("stats", "sync_partial_ok").parse::<u64>().unwrap() > partial
    });
    write_to_old(&mut m, 2, "s");
    write_to_old(&mut m, 2, "w");
    wait_caught_up(&mut m, &mut c1);
    wait_caught_up(&mut m, &mut c2);
    assert_matches_old(&mut m, &mut c1, &["s", "u", "w"], "R1 continued");
    assert_same_sets(&mut c1, &mut c2, &["s", "u", "w"], "R1 continued, R2");

    // KiviDB does not chain replicas: R2 under R1 is refused.
    let r1port = r1.port.to_string();
    let mut probe = Conn::connect(r1.port);
    let refused = probe.cmd(&["PSYNC", "?", "-1"]);
    assert!(
        matches!(&refused, V::Err(e) if e.contains("does not chain replicas")),
        "{refused:?}"
    );

    // Each replica's own log replays as it applied it.
    for server in [&mut r1, &mut r2] {
        assert_log_applied_as_f32(server);
    }
    let before = (
        c1.cmd(&["VEMB", "u0", "e1-1", "RAW"]),
        c2.cmd(&["VEMB", "u0", "e1-1", "RAW"]),
    );
    for server in [&mut r1, &mut r2] {
        server.kill();
        // As a master, so that only its log fills it.
        let args = server.args[..1].to_vec();
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        let bin = server.bin.clone();
        server.restart_as(bin, &args);
    }
    let (mut c1, mut c2) = (r1.conn(), r2.conn());
    assert_matches_old(&mut m, &mut c1, &["s", "u", "w"], "R1 replayed");
    assert_matches_old(&mut m, &mut c2, &["s", "u", "w"], "R2 replayed");
    assert_eq!(
        (
            c1.cmd(&["VEMB", "u0", "e1-1", "RAW"]),
            c2.cmd(&["VEMB", "u0", "e1-1", "RAW"])
        ),
        before
    );

    // Back under M; R2 then stops cleanly, its place in the stream saved.
    for server in [&mut r1, &mut r2] {
        server.kill();
        let bin = server.bin.clone();
        server.restart_as(bin, &["--aof", "--replicaof", "127.0.0.1", &mport]);
    }
    let (mut c1, mut c2) = (r1.conn(), r2.conn());
    wait_caught_up(&mut m, &mut c1);
    wait_caught_up(&mut m, &mut c2);
    r2.shutdown();
    // Writes R2 does not see, R1 applies as M does: f32.
    write_to_old(&mut m, 3, "s");
    write_to_old(&mut m, 3, "t");
    wait_caught_up(&mut m, &mut c1);
    assert_matches_old(&mut m, &mut c1, &["s", "u", "w", "t"], "R1 alone");

    // Promoted R1 full-syncs R2, which would otherwise apply M's `Q8` as asked.
    assert_eq!(c1.cmd(&["REPLICAOF", "NO", "ONE"]).s(), "OK");
    assert_eq!(
        c1.info_opt("server", "kividb_vset_quantization").as_deref(),
        Some("1")
    );
    let bin = r2.bin.clone();
    r2.restart_as(bin, &["--aof", "--replicaof", "127.0.0.1", &r1port]);
    let mut c2 = r2.conn();
    wait_caught_up(&mut c1, &mut c2);
    assert_eq!(c1.info("stats", "sync_full"), "1", "{}", r1.log());
    assert!(r1.log().contains("syncing it in full"), "{}", r1.log());
    assert_matches_old(
        &mut m,
        &mut c2,
        &["s", "u", "w", "t"],
        "R2 under the promoted R1",
    );
    assert_same_sets(
        &mut c1,
        &mut c2,
        &["s", "u", "w", "t"],
        "promoted R1 and R2",
    );

    // R1's own writes now keep what they ask, on both.
    for (n, opt) in OPTIONS.iter().enumerate() {
        let opts: Vec<&str> = if opt.is_empty() { vec![] } else { vec![opt] };
        let key = format!("p{n}");
        assert_eq!(vadd(&mut c1, &key, &vector(9, 16), "e1", &opts), V::Int(1));
    }
    wait_caught_up(&mut c1, &mut c2);
    for (n, want) in ["int8", "bin", "f32", "f32"].iter().enumerate() {
        let key = format!("p{n}");
        assert_eq!(quant_type(&mut c2, &key), *want, "{key}");
        assert_eq!(
            c1.cmd(&["VEMB", &key, "e1", "RAW"]),
            c2.cmd(&["VEMB", &key, "e1", "RAW"])
        );
    }
}

/// Redis's minimal replica user: no `INFO`.
const REPL_ACL: [&str; 8] = [
    "ACL",
    "SETUSER",
    "repl",
    "on",
    ">pw",
    "+psync",
    "+replconf",
    "+ping",
];

fn as_repl(port: u16) -> Vec<String> {
    [
        "--masteruser",
        "repl",
        "--masterauth",
        "pw",
        "--replicaof",
        "127.0.0.1",
    ]
    .iter()
    .map(|s| s.to_string())
    .chain([port.to_string()])
    .collect()
}

fn args(v: &[String]) -> Vec<&str> {
    v.iter().map(String::as_str).collect()
}

/// Without `INFO`, the `PSYNC` reply says the master keeps the options.
#[test]
fn a_replica_without_info_keeps_each_quantization() {
    let master = Server::start(&["--aof"]);
    let mut m = master.conn();
    assert!(!m.cmd(&REPL_ACL).is_err());
    populate(&mut m);
    let mut replica_args = vec!["--aof".to_string()];
    replica_args.extend(as_repl(master.port));
    let mut replica = Server::start(&args(&replica_args));
    let mut r = replica.conn();
    wait_caught_up(&mut m, &mut r);
    // Sets the stream makes, not the snapshot.
    write_to_old(&mut m, 0, "s");
    wait_caught_up(&mut m, &mut r);
    assert_same(&capture(&mut r), &capture(&mut m), "full sync");
    check_types(&mut r);
    assert_same_sets(&mut m, &mut r, &["s"], "stream");
    for (n, want) in ["int8", "bin", "f32", "f32"].iter().enumerate() {
        assert_eq!(quant_type(&mut r, &format!("s{n}")), *want);
    }
    assert_eq!(
        r.info_opt("server", "kividb_vset_quantization").as_deref(),
        Some("1")
    );
    assert!(
        !replica.log().contains("did not offer"),
        "{}",
        replica.log()
    );

    // Continued after its link dropped: the word comes with `+CONTINUE` too.
    let partial = m.info("stats", "sync_partial_ok").parse::<u64>().unwrap();
    assert!(!r.cmd(&["CLIENT", "KILL", "TYPE", "master"]).is_err());
    wait_until("the replica to be continued", 30, || {
        m.info("stats", "sync_partial_ok").parse::<u64>().unwrap() > partial
    });
    write_to_old(&mut m, 1, "s");
    write_to_old(&mut m, 1, "p");
    wait_caught_up(&mut m, &mut r);
    assert_eq!(m.info("stats", "sync_full"), "1");
    assert_same_sets(&mut m, &mut r, &["s", "p"], "continued");

    // Its own log, replayed: the same.
    replica.kill();
    replica.restart_as(this_bin(), &["--aof"]);
    let mut r = replica.conn();
    assert_same_sets(&mut m, &mut r, &["s", "p"], "replayed");
    assert_same(&capture(&mut r), &capture(&mut m), "replayed");

    // Failover: the replica takes over, and the old master follows it.
    assert!(!r.cmd(&REPL_ACL).is_err());
    for (name, value) in [("masteruser", "repl"), ("masterauth", "pw")] {
        assert_eq!(m.cmd(&["CONFIG", "SET", name, value]).s(), "OK");
    }
    let rport = replica.port.to_string();
    assert_eq!(m.cmd(&["REPLICAOF", "127.0.0.1", &rport]).s(), "OK");
    wait_caught_up(&mut r, &mut m);
    write_to_old(&mut r, 2, "f");
    wait_caught_up(&mut r, &mut m);
    assert_same_sets(&mut r, &mut m, &["s", "p", "f"], "after the failover");
    assert_same(&capture(&mut m), &capture(&mut r), "after the failover");
    assert!(!master.log().contains("did not offer"), "{}", master.log());
}

/// One line read off a replica link: the `PSYNC` reply.
fn psync_reply(port: u16, capas: &[&str], psync: [&str; 2]) -> String {
    let mut c = Conn::connect(port);
    let mut replconf = vec!["REPLCONF"];
    for capa in capas {
        replconf.extend(["capa", capa]);
    }
    assert_eq!(c.cmd(&replconf).s(), "OK");
    c.cmd(&["PSYNC", psync[0], psync[1]]).s()
}

/// The word goes only to replicas that announce `vset-quant`; offsets skip it.
#[test]
fn the_psync_reply_offers_vset_quant_only_when_asked() {
    let master = Server::start(&[]);
    let mut m = master.conn();
    assert_eq!(vadd(&mut m, "q", &V3, "a", &["Q8"]), V::Int(1));
    let full = |capas: &[&str]| psync_reply(master.port, capas, ["?", "-1"]);
    // Redis's replicas, and a client naming no capability.
    for capas in [&["eof", "psync2"][..], &[][..]] {
        let reply = full(capas);
        let words: Vec<&str> = reply.split(' ').collect();
        assert_eq!(words[0], "FULLRESYNC", "{reply}");
        assert_eq!(words.len(), 3, "{capas:?}: {reply}");
    }
    let reply = full(&["psync2", "vset-quant"]);
    assert!(reply.ends_with(" vset-quant"), "{reply}");
    assert_eq!(reply.split(' ').count(), 4, "{reply}");

    let id = m.info("replication", "master_replid");
    let offset = m.info("replication", "master_repl_offset");
    let next = (offset.parse::<u64>().unwrap() + 1).to_string();
    let reply = psync_reply(master.port, &["psync2"], [&id, &next]);
    assert_eq!(reply, format!("CONTINUE {id}"));
    let reply = psync_reply(master.port, &["psync2", "vset-quant"], [&id, &next]);
    assert_eq!(reply, format!("CONTINUE {id} vset-quant"));
    // No handshake moved the stream.
    assert_eq!(m.info("replication", "master_repl_offset"), offset);

    // A master that hides it answers as an older one.
    let hiding = Server::start_hiding(&[]);
    let reply = psync_reply(hiding.port, &["psync2", "vset-quant"], ["?", "-1"]);
    assert_eq!(reply.split(' ').count(), 3, "{reply}");
    assert_eq!(
        hiding.conn().info_opt("server", "kividb_vset_quantization"),
        None
    );
}

/// A backup taken on a replica that drops the options restores as f32.
#[test]
fn a_backup_on_a_stripping_replica_is_f32() {
    let old = Server::start_hiding(&[]);
    let mut m = old.conn();
    let replica = Server::start(&["--replicaof", "127.0.0.1", &old.port.to_string()]);
    let mut r = replica.conn();
    write_to_old(&mut m, 0, "s");
    wait_caught_up(&mut m, &mut r);
    assert!(!r.cmd(&["BACKUP", "START"]).is_err());
    wait_until("the increment", 20, || {
        backup_state(&mut r) == "incrementing"
    });
    write_to_old(&mut m, 1, "t");
    wait_caught_up(&mut m, &mut r);
    assert!(!r.cmd(&["BACKUP", "SEAL"]).is_err());
    let files: Vec<String> = r.cmd(&["BACKUP", "LIST"]).arr().iter().map(V::s).collect();
    let incr = files.iter().find(|f| f.ends_with(".incr.aof")).unwrap();
    let records = aof_records(&std::fs::read(incr).unwrap());
    let vadds = records.iter().filter(|rec| rec[0] == "VADD").count();
    assert_eq!(vadds, 20, "{records:?}");
    for record in &records {
        assert_ne!(record, &[MARKER]);
        assert!(!vadd_with_option(record), "{record:?}");
    }
    let manifest = files.iter().find(|f| f.ends_with(".manifest")).unwrap();
    let restored = Server::start(&["--preload-file", &format!("aof:{manifest}")]);
    let mut x = restored.conn();
    assert_same_sets(&mut r, &mut x, &["s", "t"], "restored");
    for key in keys(&["s", "t"]) {
        assert_eq!(quant_type(&mut x, &key), "f32", "{key}");
    }
}

/// Under a master that drops the options, they never reach the log.
#[test]
fn a_replica_of_an_older_master_strips_before_its_log() {
    let old = Server::start_hiding(&[]);
    let mut m = old.conn();
    let mut r1 = Server::start(&["--aof", "--replicaof", "127.0.0.1", &old.port.to_string()]);
    let mut c1 = r1.conn();
    wait_caught_up(&mut m, &mut c1);
    write_to_old(&mut m, 0, "s");
    wait_caught_up(&mut m, &mut c1);
    for n in 0..OPTIONS.len() {
        assert_eq!(quant_type(&mut c1, &format!("s{n}")), "f32", "s{n}");
    }
    assert!(
        r1.log().contains("did not offer vset-quant"),
        "{}",
        r1.log()
    );
    assert_eq!(c1.info_opt("server", "kividb_vset_quantization"), None);
    assert_log_applied_as_f32(&r1);
    // KiviDB does not chain replicas.
    let refused = Conn::connect(r1.port).cmd(&["PSYNC", "?", "-1"]);
    assert!(
        matches!(&refused, V::Err(e) if e.contains("does not chain replicas")),
        "{refused:?}"
    );
    let raw: Vec<V> = (0..OPTIONS.len())
        .map(|n| c1.cmd(&["VEMB", &format!("s{n}"), "e0-1", "RAW"]))
        .collect();

    // Its log replays the same; promoted, it keeps the options.
    r1.kill();
    r1.restart_as(this_bin(), &["--aof"]);
    let mut c1 = r1.conn();
    let again: Vec<V> = (0..OPTIONS.len())
        .map(|n| c1.cmd(&["VEMB", &format!("s{n}"), "e0-1", "RAW"]))
        .collect();
    assert_eq!(again, raw);
    assert_eq!(
        c1.info_opt("server", "kividb_vset_quantization").as_deref(),
        Some("1")
    );

    // Each master is judged at its own handshake.
    let new = Server::start(&[]);
    let mut n = new.conn();
    // Not empty, or the replica's full-sync guard refuses it.
    assert_eq!(n.cmd(&["SET", "x", "1"]).s(), "OK");
    assert_eq!(
        c1.cmd(&["REPLICAOF", "127.0.0.1", &new.port.to_string()])
            .s(),
        "OK"
    );
    wait_caught_up(&mut n, &mut c1);
    write_to_old(&mut n, 1, "n");
    wait_caught_up(&mut n, &mut c1);
    assert_same_sets(&mut n, &mut c1, &["n"], "under a 1.3 master");
    assert_eq!(
        c1.cmd(&["REPLICAOF", "127.0.0.1", &old.port.to_string()])
            .s(),
        "OK"
    );
    wait_caught_up(&mut m, &mut c1);
    write_to_old(&mut m, 2, "o");
    wait_caught_up(&mut m, &mut c1);
    for k in 0..OPTIONS.len() {
        assert_eq!(quant_type(&mut c1, &format!("o{k}")), "f32", "o{k}");
    }
}

/// Promoted, a replica that stripped the options never sends them on.
#[test]
fn promotion_does_not_pass_on_stripped_options() {
    let old = Server::start_hiding(&[]);
    let mut m = old.conn();
    let mport = old.port.to_string();
    let r1 = Server::start(&["--replicaof", "127.0.0.1", &mport]);
    let mut r2 = Server::start(&["--replicaof", "127.0.0.1", &mport]);
    let r3 = Server::start(&["--replicaof", "127.0.0.1", &mport]);
    let (mut c1, mut c2, mut c3) = (r1.conn(), r2.conn(), r3.conn());
    write_to_old(&mut m, 0, "s");
    for c in [&mut c1, &mut c2, &mut c3] {
        wait_caught_up(&mut m, c);
    }
    // R2 stops where it is; R1 and R3 strip more.
    r2.shutdown();
    write_to_old(&mut m, 1, "t");
    wait_caught_up(&mut m, &mut c1);
    wait_caught_up(&mut m, &mut c3);

    assert_eq!(c1.cmd(&["REPLICAOF", "NO", "ONE"]).s(), "OK");
    let r1port = r1.port.to_string();
    // R3 has all R1 stripped: continued.
    assert_eq!(c3.cmd(&["REPLICAOF", "127.0.0.1", &r1port]).s(), "OK");
    wait_caught_up(&mut c1, &mut c3);
    assert_eq!(c1.info("stats", "sync_partial_ok"), "1", "{}", r1.log());
    // R2 would be sent raw `Q8`s from R1's backlog: synced in full instead.
    r2.restart_as(this_bin(), &["--replicaof", "127.0.0.1", &r1port]);
    let mut c2 = r2.conn();
    wait_caught_up(&mut c1, &mut c2);
    assert_eq!(c1.info("stats", "sync_full"), "1", "{}", r1.log());
    assert!(r1.log().contains("syncing it in full"), "{}", r1.log());
    for c in [&mut c2, &mut c3] {
        assert_same_sets(&mut c1, c, &["s", "t"], "under the promoted R1");
        for n in 0..OPTIONS.len() {
            assert_eq!(quant_type(c, &format!("t{n}")), "f32", "t{n}");
        }
    }
    // R1's own writes keep their options, on every node.
    write_to_old(&mut c1, 2, "p");
    wait_caught_up(&mut c1, &mut c2);
    wait_caught_up(&mut c1, &mut c3);
    for c in [&mut c2, &mut c3] {
        assert_same_sets(&mut c1, c, &["p"], "R1's writes");
        assert_eq!(quant_type(c, "p0"), "int8");
        assert_eq!(quant_type(c, "p1"), "bin");
    }
}

/// A full sync from a new master drops the old master's stripped-VADD watermark.
#[test]
fn a_full_sync_from_another_master_resets_the_watermark() {
    // R1 strips the options of M, which does not offer to keep them.
    let old = Server::start_hiding(&[]);
    let mut m = old.conn();
    let r1 = Server::start(&["--replicaof", "127.0.0.1", &old.port.to_string()]);
    let mut c1 = r1.conn();
    wait_caught_up(&mut m, &mut c1);
    for e in 0..200 {
        let r = vadd(&mut m, "old", &vector(e, 16), &format!("e{e}"), &["Q8"]);
        assert!(matches!(r, V::Int(_)), "{r:?}");
    }
    wait_caught_up(&mut m, &mut c1);
    assert_eq!(quant_type(&mut c1, "old"), "f32");

    // Then R1 and R2 follow Y, whose offsets are far below M's.
    let y = Server::start(&[]);
    let mut cy = y.conn();
    let yport = y.port.to_string();
    assert_eq!(c1.cmd(&["REPLICAOF", "127.0.0.1", &yport]).s(), "OK");
    let r2 = Server::start(&["--replicaof", "127.0.0.1", &yport]);
    let mut c2 = r2.conn();
    assert!(cy.cmd(&["SET", "k", "1"]).s() == "OK");
    wait_caught_up(&mut cy, &mut c1);
    wait_caught_up(&mut cy, &mut c2);
    let below = cy
        .info("replication", "master_repl_offset")
        .parse::<u64>()
        .unwrap();
    let above = m
        .info("replication", "master_repl_offset")
        .parse::<u64>()
        .unwrap();
    assert!(below < above, "{below} {above}");

    // R1 promoted: R2 is continued, not synced in full.
    assert_eq!(c1.cmd(&["REPLICAOF", "NO", "ONE"]).s(), "OK");
    let r1port = r1.port.to_string();
    assert_eq!(c2.cmd(&["REPLICAOF", "127.0.0.1", &r1port]).s(), "OK");
    wait_caught_up(&mut c1, &mut c2);
    assert_eq!(c1.info("stats", "sync_full"), "0", "{}", r1.log());
    assert_eq!(c1.info("stats", "sync_partial_ok"), "1", "{}", r1.log());
}

/// 1.3 master, 1.3 replicas: each keeps what is asked, and says so.
#[test]
fn replicas_of_a_1_3_master_keep_each_quantization() {
    // With its AOF on: markers are written, and must stay out of the stream.
    let master = Server::start(&["--aof"]);
    let mut m = master.conn();
    populate(&mut m);
    let port = master.port.to_string();
    let a = Server::start(&["--aof", "--replicaof", "127.0.0.1", &port]);
    let mut b = Server::start(&["--aof", "--replicaof", "127.0.0.1", &port]);
    let (mut ca, mut cb) = (a.conn(), b.conn());
    wait_caught_up(&mut m, &mut ca);
    wait_caught_up(&mut m, &mut cb);
    for (key, opt, dim) in SETS {
        let opts: Vec<&str> = if opt.is_empty() { vec![] } else { vec![opt] };
        assert_eq!(vadd(&mut m, key, &vector(5, dim), "e31", &opts), V::Int(1));
    }
    wait_caught_up(&mut m, &mut ca);
    wait_caught_up(&mut m, &mut cb);
    let want = capture(&mut m);
    for (c, what) in [(&mut ca, "a"), (&mut cb, "b")] {
        assert_same(&capture(c), &want, what);
        check_types(c);
        assert_eq!(
            c.info_opt("server", "kividb_vset_quantization").as_deref(),
            Some("1")
        );
    }
    // The replicas' logs start at the full sync.
    for (server, marked) in [(&master, 63), (&a, 3), (&b, 3)] {
        assert!(!server.log().contains(MARKER), "{}", server.log());
        assert_eq!(assert_markers_follow_their_vadds(&aof_of(server)), marked);
    }
    assert_eq!(
        m.info("replication", "master_repl_offset"),
        ca.info("replication", "slave_repl_offset")
    );
    // Its own log replays to the same.
    b.kill();
    b.restart_as(this_bin(), &["--aof"]);
    let mut cb = b.conn();
    assert_same(&capture(&mut cb), &want, "b replayed");
}

// ── Older snapshots ───────────────────────────────────────────────────────────

// Saved by KiviDB 1.2.1; its contents are asserted below.
const LEGACY: &str = "tests/fixtures/kividb-1.2-vset.kdb";

fn floats(reply: &V) -> Vec<f64> {
    reply.arr().iter().map(|v| v.s().parse().unwrap()).collect()
}

#[test]
fn snapshot_from_1_2_loads_as_f32() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::copy(LEGACY, tmp.path().join("dump.kdb")).unwrap();
    let mut server = Server::start_in(tmp, &[]);
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

/// `image` with element `name`'s vector made `len` long, its floats to match.
fn with_length(image: &[u8], name: u8, len: u8) -> Vec<u8> {
    let layout = [1u8, 0, 0, 0, name, 3, 0, 0, 0];
    let at = image
        .windows(layout.len())
        .position(|w| w == layout)
        .expect("a 3-component element")
        + 5;
    let mut image = image.to_vec();
    image[at] = len;
    let floats = at + 4 + 12;
    match len {
        2 => drop(image.drain(floats - 4..floats)),
        4 => drop(image.splice(floats..floats, 7.0f32.to_le_bytes())),
        _ => unreachable!(),
    }
    image
}

/// `payload` with `name`'s vector made `len` long, then resealed.
fn dump_with_length(payload: &[u8], name: u8, len: u8) -> Vec<u8> {
    let mut data = with_length(&payload[..payload.len() - 10], name, len);
    let crc = kividb::snapshot::crc64_digest(&data);
    data.extend_from_slice(&payload[payload.len() - 10..payload.len() - 8]);
    data.extend_from_slice(&crc.to_le_bytes());
    data
}

/// A wrong-length 1.2 element stops startup, and the file is left as it was.
#[test]
fn snapshot_with_a_wrong_length_element_is_refused() {
    let good = std::fs::read(LEGACY).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("dump.kdb"), &good).unwrap();
    let mut server = Server::start_in(tmp, &[]);
    assert_eq!(server.conn().cmd(&["VCARD", "legacy"]).i(), 3);
    server.kill();
    let dump = server.dir.join("dump.kdb");
    for len in [2, 4] {
        let bad = with_length(&good, b'a', len);
        std::fs::write(&dump, &bad).unwrap();
        let status = server.try_restart_as(this_bin(), &[]);
        let status = status.unwrap_or_else(|| panic!("started on a {len}-component element"));
        assert!(!status.success());
        let log = server.log();
        let says = format!("element \"a\" of a vector set has {len} components, not its set's 3");
        assert!(log.contains(&says), "{log}");
        assert_eq!(std::fs::read(&dump).unwrap(), bad, "the file was changed");
    }
    // Whole again, it loads as before.
    std::fs::write(&dump, &good).unwrap();
    server.restart();
    let mut c = server.conn();
    assert_eq!(c.cmd(&["VCARD", "legacy"]).i(), 3);
    assert_eq!(floats(&c.cmd(&["VEMB", "legacy", "a"])), [3.0, -4.0, 0.0]);
}

fn dumped_set(c: &mut Conn, key: &str) -> Vec<u8> {
    for (e, v) in [
        ("a", [1.0, 2.0, 3.0]),
        ("b", [4.0, 5.0, 6.0]),
        ("c", [7.0, 8.0, 9.0]),
    ] {
        assert_eq!(vadd(c, key, &v, e, &[]), V::Int(1));
    }
    match c.cmd(&["DUMP", key]) {
        V::Bulk(payload) => payload,
        other => panic!("DUMP: {other:?}"),
    }
}

/// `RESTORE` of a wrong-length element is refused whole; nothing is changed.
#[test]
fn restore_refuses_a_wrong_length_element() {
    let server = Server::start(&[]);
    let mut c = server.conn();
    let payload = dumped_set(&mut c, "src");
    let bad_data = || V::Err("ERR Bad data format".into());
    let short = dump_with_length(&payload, b'b', 2);
    let long = dump_with_length(&payload, b'c', 4);
    // Every element wrong: refused, not restored as an empty set.
    let mut all = dump_with_length(&payload, b'a', 2);
    all = dump_with_length(&all, b'b', 4);
    all = dump_with_length(&all, b'c', 2);
    for bad in [&short, &long, &all] {
        assert_eq!(c.cmd_bytes(&[b"RESTORE", b"new", b"0", bad]), bad_data());
    }
    assert_eq!(c.cmd(&["EXISTS", "new"]).i(), 0);

    // An existing destination is kept as it was, with `REPLACE` too.
    assert_eq!(
        vadd(&mut c, "dst", &[9.0, 9.0, 9.0], "x", &["Q8"]),
        V::Int(1)
    );
    let before = c.cmd(&["VEMB", "dst", "x", "RAW"]);
    let r = c.cmd_bytes(&[b"RESTORE", b"dst", b"0", &short, b"REPLACE"]);
    assert_eq!(r, bad_data());
    let r = c.cmd_bytes(&[b"RESTORE", b"dst", b"0", &short]);
    assert!(r.s().starts_with("BUSYKEY"), "{r:?}");
    assert_eq!(c.cmd(&["VCARD", "dst"]).i(), 1);
    assert_eq!(c.cmd(&["VEMB", "dst", "x", "RAW"]), before);

    // The payload as dumped restores whole, over it.
    let r = c.cmd_bytes(&[b"RESTORE", b"dst", b"0", &payload, b"REPLACE"]);
    assert_eq!(r, V::Str("OK".into()));
    assert_eq!(c.cmd(&["VCARD", "dst"]).i(), 3);
    assert_eq!(floats(&c.cmd(&["VEMB", "dst", "b"])), [4.0, 5.0, 6.0]);
    assert_eq!(quant_type(&mut c, "dst"), "f32");
}

/// Reads one command a client sent; `None` at its end.
fn read_command(r: &mut BufReader<TcpStream>) -> Option<Vec<Vec<u8>>> {
    let mut line = String::new();
    if r.read_line(&mut line).ok()? == 0 {
        return None;
    }
    let n: usize = line.trim_end().strip_prefix('*')?.parse().ok()?;
    let mut args = Vec::with_capacity(n);
    for _ in 0..n {
        line.clear();
        r.read_line(&mut line).ok()?;
        let len: usize = line.trim_end().strip_prefix('$')?.parse().ok()?;
        let mut arg = vec![0u8; len + 2];
        r.read_exact(&mut arg).ok()?;
        arg.truncate(len);
        args.push(arg);
    }
    Some(args)
}

/// A port forwarding to `target`, each command passed through `edit` first.
fn proxy(target: u16, edit: fn(&mut [Vec<u8>])) -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for client in listener.incoming() {
            let Ok(client) = client else { return };
            let Ok(mut server) = TcpStream::connect(("127.0.0.1", target)) else {
                return;
            };
            let (mut back, mut to_client) =
                (server.try_clone().unwrap(), client.try_clone().unwrap());
            std::thread::spawn(move || std::io::copy(&mut back, &mut to_client));
            let mut reader = BufReader::new(client);
            std::thread::spawn(move || {
                while let Some(mut args) = read_command(&mut reader) {
                    edit(&mut args);
                    let refs: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();
                    if server.write_all(&encode(&refs)).is_err() {
                        return;
                    }
                }
            });
        }
    });
    port
}

/// A `MIGRATE` whose target refuses a wrong-length element fails; the key stays.
#[test]
fn migrate_reports_a_wrong_length_element() {
    let source = Server::start(&[]);
    let target = Server::start(&[]);
    let (mut s, mut t) = (source.conn(), target.conn());
    dumped_set(&mut s, "bad");
    dumped_set(&mut s, "good");
    // Corrupted on its way, as a source with such an element would send it.
    let port = proxy(target.port, |args| {
        if args[0].eq_ignore_ascii_case(b"RESTORE") && args[1] == b"bad" {
            args[3] = dump_with_length(&args[3], b'b', 2);
        }
    });
    let port = port.to_string();
    let r = s.cmd(&["MIGRATE", "127.0.0.1", &port, "bad", "0", "5000"]);
    assert_eq!(
        r,
        V::Err("ERR Target instance replied with error: ERR Bad data format".into())
    );
    assert_eq!(s.cmd(&["VCARD", "bad"]).i(), 3);
    assert_eq!(t.cmd(&["EXISTS", "bad"]).i(), 0);
    // The same way, untouched: moved whole.
    let r = s.cmd(&["MIGRATE", "127.0.0.1", &port, "good", "0", "5000"]);
    assert_eq!(r, V::Str("OK".into()));
    assert_eq!(s.cmd(&["EXISTS", "good"]).i(), 0);
    assert_eq!(t.cmd(&["VCARD", "good"]).i(), 3);
    assert_eq!(floats(&t.cmd(&["VEMB", "good", "c"])), [7.0, 8.0, 9.0]);
}

/// 1.2 loads every f32 set, NOQUANT too, and refuses Q8 and BIN whole.
#[test]
#[ignore = "needs a 1.2 binary in KIVIDB_1_2_BIN"]
fn downgrade_to_1_2() {
    let old = old_bin();
    for (opts, loads, stored) in [
        (&[][..], true, "f32"),
        (&["NOQUANT"][..], true, "f32"),
        (&["Q8"][..], false, "int8"),
        (&["BIN"][..], false, "bin"),
    ] {
        let mut server = Server::start(&[]);
        let mut c = server.conn();
        assert_eq!(vadd(&mut c, "v", &[3.0, -4.0, 0.5], "a", opts), V::Int(1));
        c.cmd(&["VSETATTR", "v", "a", r#"{"n":1}"#]);
        assert_eq!(c.cmd(&["SAVE"]).s(), "OK");
        server.shutdown();
        let image = std::fs::read(server.dir.join("dump.kdb")).unwrap();
        let status = server.try_restart_as(old.clone(), &[]);
        if loads {
            assert!(status.is_none(), "{opts:?}: 1.2 refused:\n{}", server.log());
            let mut c = server.conn();
            assert_eq!(quant_type(&mut c, "v"), "f32", "{opts:?} on 1.2");
            assert_eq!(floats(&c.cmd(&["VEMB", "v", "a"])), [3.0, -4.0, 0.5]);
            assert_eq!(c.cmd(&["VGETATTR", "v", "a"]).s(), r#"{"n":1}"#);
            // 1.2 saves it; 1.3 reads it as made without an option.
            server.shutdown();
            server.restart_as(this_bin(), &[]);
            let mut c = server.conn();
            assert_eq!(quant_type(&mut c, "v"), "f32", "{opts:?} back on 1.3");
            assert_eq!(floats(&c.cmd(&["VEMB", "v", "a"])), [3.0, -4.0, 0.5]);
            assert_eq!(vadd(&mut c, "v", &[1.0, 2.0, 3.0], "b", &["Q8"]), V::Int(1));
            assert_eq!(quant_type(&mut c, "v"), "f32");
        } else {
            let status = status.unwrap_or_else(|| panic!("1.2 started on a {opts:?} set"));
            assert!(!status.success());
            let log = server.log();
            assert!(log.contains("unknown type tag 16"), "{opts:?}: {log}");
            // The refused snapshot is left as it was, and 1.3 still loads it.
            assert_eq!(std::fs::read(server.dir.join("dump.kdb")).unwrap(), image);
            server.restart_as(this_bin(), &[]);
            let mut c = server.conn();
            assert_eq!(quant_type(&mut c, "v"), stored, "{opts:?} back on 1.3");
            assert_eq!(c.cmd(&["VGETATTR", "v", "a"]).s(), r#"{"n":1}"#);
        }
    }
}

/// 1.2 and 1.3 take turns on one log; each replays it correctly.
#[test]
#[ignore = "needs a 1.2 binary in KIVIDB_1_2_BIN"]
fn aof_between_1_2_and_1_3() {
    let old = old_bin();
    let mut server = Server::start_bin(old.clone(), tempfile::tempdir().unwrap(), &["--aof"]);
    let mut c = server.conn();
    for (key, opt) in [("q8", "Q8"), ("bin", "BIN"), ("nf", "NOQUANT")] {
        assert_eq!(vadd(&mut c, key, &[3.0, -4.0, 0.5], "a", &[opt]), V::Int(1));
    }
    // No final save: the records are all there is.
    server.kill();
    server.restart_as(this_bin(), &["--aof"]);
    let mut c = server.conn();
    for key in ["q8", "bin", "nf"] {
        assert_eq!(quant_type(&mut c, key), "f32", "{key}");
        assert_eq!(floats(&c.cmd(&["VEMB", key, "a"])), [3.0, -4.0, 0.5]);
    }
    assert_eq!(
        vadd(&mut c, "n8", &[3.0, -4.0, 0.5], "a", &["Q8"]),
        V::Int(1)
    );
    assert_eq!(quant_type(&mut c, "n8"), "int8");
    server.kill();

    // 1.2 on the mixed log: it starts, and its sets are all f32.
    server.restart_as(old.clone(), &["--aof"]);
    let mut c = server.conn();
    for key in ["q8", "bin", "nf", "n8"] {
        assert_eq!(c.cmd(&["VCARD", key]).i(), 1, "{key}: {}", server.log());
        assert_eq!(floats(&c.cmd(&["VEMB", key, "a"])), [3.0, -4.0, 0.5]);
    }
    assert!(
        server.log().contains("KIVIDB.AOF-VSET-QUANT"),
        "{}",
        server.log()
    );
    server.kill();

    // And 1.3 again: each record as its writer stored it.
    server.restart_as(this_bin(), &["--aof"]);
    let mut c = server.conn();
    for (key, want) in [("q8", "f32"), ("bin", "f32"), ("nf", "f32"), ("n8", "int8")] {
        assert_eq!(quant_type(&mut c, key), want, "{key}");
    }
}

/// 1.3 crashes, 1.2 writes and crashes, 1.3 again: 1.2's records stay f32.
#[test]
#[ignore = "needs a 1.2 binary in KIVIDB_1_2_BIN"]
fn a_1_2_crash_between_two_1_3_crashes() {
    let old = old_bin();
    let mut server = Server::start(&["--aof"]);
    let mut c = server.conn();
    assert_eq!(vadd(&mut c, "b", &V3, "a", &["Q8"]), V::Int(1));
    assert_eq!(vadd(&mut c, "f", &V3, "a", &["NOQUANT"]), V::Int(1));
    server.kill();

    server.restart_as(old.clone(), &["--aof"]);
    let mut c = server.conn();
    for key in ["b", "f"] {
        assert_eq!(quant_type(&mut c, key), "f32", "1.2: {key}");
        assert_eq!(floats(&c.cmd(&["VEMB", key, "a"])), [3.0, -4.0, 0.5]);
    }
    assert_eq!(vadd(&mut c, "c", &V3, "a", &["Q8"]), V::Int(1));
    assert_eq!(c.cmd(&["MULTI"]).s(), "OK");
    vadd(&mut c, "d", &V3, "a", &["BIN"]);
    c.cmd(&["SET", "s", "1"]);
    assert_eq!(c.cmd(&["EXEC"]).arr().len(), 2);
    server.kill();

    server.restart_as(this_bin(), &["--aof"]);
    let mut c = server.conn();
    for (key, want) in [("b", "int8"), ("f", "f32"), ("c", "f32"), ("d", "f32")] {
        assert_eq!(quant_type(&mut c, key), want, "{key}: {}", server.log());
    }
    for key in ["c", "d"] {
        assert_eq!(
            floats(&c.cmd(&["VEMB", key, "a"])),
            [3.0, -4.0, 0.5],
            "{key}"
        );
    }
    assert_eq!(c.cmd(&["GET", "s"]).s(), "1");
    // And once more, with 1.3's records after 1.2's.
    assert_eq!(vadd(&mut c, "e", &V3, "a", &["Q8"]), V::Int(1));
    server.kill();
    server.restart();
    let mut c = server.conn();
    for (key, want) in [("b", "int8"), ("c", "f32"), ("d", "f32"), ("e", "int8")] {
        assert_eq!(quant_type(&mut c, key), want, "{key}");
    }
}

/// A marked log with a torn end: 1.2 replays f32, 1.3 keeps each.
#[test]
#[ignore = "needs a 1.2 binary in KIVIDB_1_2_BIN"]
fn a_marked_log_on_1_2_and_back() {
    let old = old_bin();
    let mut server = Server::start(&["--aof"]);
    let mut c = server.conn();
    for (key, opt) in [("q", "Q8"), ("b", "BIN"), ("n", "NOQUANT"), ("d", "")] {
        assert_eq!(vadd(&mut c, key, &V3, "a", &opts_of(opt)), V::Int(1));
    }
    assert_eq!(c.cmd(&["MULTI"]).s(), "OK");
    vadd(&mut c, "t", &V3, "a", &["Q8"]);
    c.cmd(&["SET", "s", "1"]);
    assert_eq!(c.cmd(&["EXEC"]).arr().len(), 2);
    assert_eq!(vadd(&mut c, "z", &V3, "a", &["Q8"]), V::Int(1));
    server.kill();
    let path = server.dir.join("appendonly.aof");
    let log = std::fs::read(&path).unwrap();
    std::fs::write(&path, &log[..log.len() - 7]).unwrap();

    server.restart_as(old.clone(), &["--aof"]);
    let mut c = server.conn();
    for key in ["q", "b", "n", "d", "t", "z"] {
        assert_eq!(quant_type(&mut c, key), "f32", "1.2: {key}");
        assert_eq!(
            floats(&c.cmd(&["VEMB", key, "a"])),
            [3.0, -4.0, 0.5],
            "{key}"
        );
    }
    assert_eq!(c.cmd(&["GET", "s"]).s(), "1");
    let log = server.log();
    assert!(log.contains("is cut off"), "{log}");
    // One warning for each whole marker.
    let warned = log
        .matches("unknown command 'KIVIDB.AOF-VSET-QUANT'")
        .count();
    assert_eq!(warned, 4, "{log}");
    assert_eq!(vadd(&mut c, "w", &V3, "a", &["Q8"]), V::Int(1));
    server.kill();

    server.restart_as(this_bin(), &["--aof"]);
    let mut c = server.conn();
    for (key, want) in [
        ("q", "int8"),
        ("b", "bin"),
        ("n", "f32"),
        ("d", "f32"),
        ("t", "int8"),
        ("z", "f32"),
        ("w", "f32"),
    ] {
        assert_eq!(quant_type(&mut c, key), want, "{key}");
    }
    assert_eq!(floats(&c.cmd(&["VEMB", "z", "a"])), [3.0, -4.0, 0.5]);
}

/// After a rewrite, and from a backup: 1.2 reads f32, 1.3 each as asked.
#[test]
#[ignore = "needs a 1.2 binary in KIVIDB_1_2_BIN"]
fn a_rewritten_log_and_a_backup_on_1_2() {
    let old = old_bin();
    let mut server = Server::start(&["--aof"]);
    let mut c = server.conn();
    // Only f32 sets in the snapshot, which 1.2 reads.
    assert_eq!(vadd(&mut c, "f", &V3, "a", &["NOQUANT"]), V::Int(1));
    assert_eq!(vadd(&mut c, "d", &V3, "a", &[]), V::Int(1));
    rewrite(&mut c);
    assert_eq!(vadd(&mut c, "q", &V3, "a", &["Q8"]), V::Int(1));
    assert_eq!(vadd(&mut c, "n", &V3, "a", &["NOQUANT"]), V::Int(1));
    assert_eq!(assert_markers_follow_their_vadds(&aof_of(&server)), 2);
    server.kill();

    server.restart_as(old.clone(), &["--aof"]);
    let mut c = server.conn();
    for key in ["f", "d", "q", "n"] {
        assert_eq!(
            quant_type(&mut c, key),
            "f32",
            "1.2: {key}: {}",
            server.log()
        );
        assert_eq!(
            floats(&c.cmd(&["VEMB", key, "a"])),
            [3.0, -4.0, 0.5],
            "{key}"
        );
    }
    assert_eq!(vadd(&mut c, "w", &V3, "a", &["BIN"]), V::Int(1));
    server.kill();
    server.restart_as(this_bin(), &["--aof"]);
    let mut c = server.conn();
    for (key, want) in [
        ("f", "f32"),
        ("d", "f32"),
        ("q", "int8"),
        ("n", "f32"),
        ("w", "f32"),
    ] {
        assert_eq!(quant_type(&mut c, key), want, "{key}");
    }
    // NOQUANT kept by its marker, and lost by the snapshot, as documented.
    assert!(vadd(&mut c, "n", &V3, "x", &["Q8"]).is_err());
    assert_eq!(vadd(&mut c, "f", &V3, "x", &["Q8"]), V::Int(1));

    // A backup whose base 1.2 reads, and whose increment is marked.
    let source = Server::start(&[]);
    let mut c = source.conn();
    assert_eq!(vadd(&mut c, "base", &V3, "a", &["NOQUANT"]), V::Int(1));
    assert!(!c.cmd(&["BACKUP", "START"]).is_err());
    wait_until("the increment", 20, || {
        backup_state(&mut c) == "incrementing"
    });
    assert_eq!(vadd(&mut c, "q", &V3, "a", &["Q8"]), V::Int(1));
    assert_eq!(vadd(&mut c, "b", &V3, "a", &["BIN"]), V::Int(1));
    assert!(!c.cmd(&["BACKUP", "SEAL"]).is_err());
    let files: Vec<String> = c.cmd(&["BACKUP", "LIST"]).arr().iter().map(V::s).collect();
    let manifest = files.iter().find(|f| f.ends_with(".manifest")).unwrap();
    let preload = format!("aof:{manifest}");
    let on_old = Server::start_bin(
        old,
        tempfile::tempdir().unwrap(),
        &["--preload-file", &preload],
    );
    let mut o = on_old.conn();
    for key in ["base", "q", "b"] {
        assert_eq!(quant_type(&mut o, key), "f32", "1.2: {key}");
        assert_eq!(
            floats(&o.cmd(&["VEMB", key, "a"])),
            [3.0, -4.0, 0.5],
            "{key}"
        );
    }
    let on_new = Server::start(&["--preload-file", &preload]);
    let mut n = on_new.conn();
    for (key, want) in [("base", "f32"), ("q", "int8"), ("b", "bin")] {
        assert_eq!(quant_type(&mut n, key), want, "{key}");
    }
}

/// A 1.3 master refuses quantized sets to 1.2 replicas, but syncs f32.
#[test]
#[ignore = "needs a 1.2 binary in KIVIDB_1_2_BIN"]
fn a_1_2_replica_is_refused_quantized_sets() {
    let old = old_bin();
    let master = Server::start(&[]);
    let mut m = master.conn();
    assert_eq!(
        vadd(&mut m, "f", &[3.0, -4.0, 0.5], "a", &["NOQUANT"]),
        V::Int(1)
    );
    let port = master.port.to_string();
    let replica = Server::start_bin(
        old.clone(),
        tempfile::tempdir().unwrap(),
        &["--replicaof", "127.0.0.1", &port],
    );
    let mut r = replica.conn();
    wait_caught_up(&mut m, &mut r);
    assert_eq!(floats(&r.cmd(&["VEMB", "f", "a"])), [3.0, -4.0, 0.5]);
    drop(replica);

    assert_eq!(
        vadd(&mut m, "q", &[3.0, -4.0, 0.5], "a", &["Q8"]),
        V::Int(1)
    );
    let full = m.info("stats", "sync_full");
    let replica = Server::start_bin(
        old,
        tempfile::tempdir().unwrap(),
        &["--replicaof", "127.0.0.1", &port],
    );
    wait_until("the refusal", 20, || {
        replica.log().contains("cannot read: upgrade the replica")
    });
    std::thread::sleep(Duration::from_secs(3));
    assert_eq!(m.info("stats", "sync_full"), full, "no snapshot was sent");
    assert!(
        master.log().contains("Upgrade replicas before masters"),
        "{}",
        master.log()
    );
    assert_eq!(
        replica.conn().info("replication", "master_link_status"),
        "down"
    );
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
    // Slow log entries for 768-value VADDs would count against the slack.
    assert_eq!(
        c.cmd(&["CONFIG", "SET", "slowlog-log-slower-than", "-1"])
            .s(),
        "OK"
    );
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
