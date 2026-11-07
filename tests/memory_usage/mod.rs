//! What the `MEMORY USAGE` accuracy tests share: a server process of its
//! own for each case, a client that pipelines, and the measurement of a
//! case against the growth of `used_memory` (what the allocator was asked
//! for, which is what `maxmemory` is measured against) per key.
//!
//! What the server allocates besides the keys while they are loaded (a
//! connection's buffers, a command's first statistics) is made before the
//! measurement, by loading a few keys of the shape in another database
//! first.

// Each test binary uses what it needs of this.
#![allow(dead_code)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// How far a case's average estimate may be from the bytes measured per key.
///
/// The estimate is computed from the structures as `used_memory` counts
/// them, so with every member alike it is exact to the byte; what is left
/// is the sample (members of different lengths), the server's own
/// allocations while the keys load (a byte or so per key, spread over many
/// keys), and what no one key owns or what the structures do not show.
/// Measured on macOS and Linux (glibc), every case without deletions was
/// within 0.25% and 1 byte: [`Tolerance::EXACT`] is four times that.
#[derive(Clone, Copy)]
pub struct Tolerance {
    /// Below the measured bytes, as a fraction of them.
    pub under: f64,
    /// Above them.
    pub over: f64,
    /// Or this many bytes either way, for keys too small for a fraction.
    pub bytes: f64,
}

impl Tolerance {
    pub const EXACT: Tolerance = Tolerance {
        under: 0.01,
        over: 0.01,
        bytes: 16.0,
    };

    /// A hash with field TTLs is on the list of such hashes (`field_expiry`),
    /// a table for every hash with them: the estimate counts the hash's
    /// bucket there, not the table's empty buckets, which no hash owns and
    /// deleting one does not give back. A table grown to fit is at least
    /// 7/16 full, so the empty buckets come to at most 9/7 of a bucket a hash.
    /// A sorted set past one leaf of score index, whose nodes are counted
    /// from how many keys it has (`memory_layout::score_index`): measured
    /// from 4.6% under (members in order, 5000 of them) to 7.7% over (20 at
    /// random), on macOS and Linux.
    pub const ZSET: Tolerance = Tolerance {
        under: 0.07,
        over: 0.10,
        bytes: 16.0,
    };

    pub fn with_field_ttls(self) -> Tolerance {
        let bucket = std::mem::size_of::<((usize, usize, String), Instant)>() + 1;
        Tolerance {
            bytes: self.bytes + bucket as f64 * 9.0 / 7.0,
            ..self
        }
    }

    /// A hash some of whose fields were deleted: its vector of entries keeps
    /// its room, but the map reports the room of its table of indexes, which
    /// deletions can leave fewer free buckets in (hashbrown's tombstones), so
    /// part of the vector's room is not seen. 128 fields cut to 65 measured
    /// 3.8% under, 512 cut to 65 8%.
    pub fn after_deletions(self, under: f64) -> Tolerance {
        Tolerance { under, ..self }
    }
}

/// Members (or fields) loaded for each case, over all its keys: enough keys
/// that what the server allocates besides them is a byte or two per key.
pub const MEMBERS_PER_CASE: usize = 200_000;

// ── a server process ────────────────────────────────────────────────────────

pub struct Server {
    port: u16,
    dir: PathBuf,
    child: Child,
}

/// A socket bound to `port` that never listens, or none if the port is
/// taken. Not a `TcpListener`: the tests of a binary start their servers at
/// once, and a server being started holds a copy of every socket the test
/// process has open, from its fork to its exec. A listening probe copied so
/// took the connection its test made to the port it had found free, and
/// reset it when the exec closed the copy (#506).
pub fn probe(port: u16) -> Option<socket2::Socket> {
    let probe = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None).ok()?;
    // As the server's own socket, so that a copy of this one, while it
    // lasts, does not keep the server from the port.
    #[cfg(unix)]
    let _ = probe.set_reuse_address(true);
    probe
        .bind(&std::net::SocketAddr::from(([127, 0, 0, 1], port)).into())
        .ok()?;
    Some(probe)
}

fn port_is_free(port: u16) -> bool {
    probe(port).is_some()
}

pub fn free_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(0);
    loop {
        let step = NEXT.fetch_add(1, Ordering::Relaxed);
        let from = (std::process::id() as u16).wrapping_mul(61);
        let port = 10_000 + from.wrapping_add(step) % 20_000;
        if port_is_free(port) {
            return port;
        }
    }
}

impl Server {
    pub fn start() -> Self {
        let dir = tempfile::tempdir().unwrap().keep();
        let port = free_port();
        let mut command = Command::new(env!("CARGO_BIN_EXE_kividb"));
        command
            .current_dir(&dir)
            .args(["--port", &port.to_string(), "--bind", "127.0.0.1"])
            .args(["--configfile", "kividb.conf", "--threads", "2"])
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if let Ok(model) = std::env::var("KIVIDB_TEST_IO_MODEL") {
            command.args(["--io-model", &model]);
        }
        let child = command.spawn().expect("start kividb");
        Server { port, dir, child }
    }

    pub fn connect(&self) -> Conn {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            match TcpStream::connect(("127.0.0.1", self.port)) {
                Ok(s) => {
                    let mut c = Conn {
                        reader: BufReader::new(s.try_clone().unwrap()),
                        writer: s,
                    };
                    // No snapshots while it is measured.
                    c.ok(&[b"CONFIG", b"SET", b"save", b""]);
                    return c;
                }
                Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(20))
                }
                Err(e) => panic!("connect: {e}"),
            }
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

// ── a client that pipelines ─────────────────────────────────────────────────

#[derive(Debug)]
pub enum Reply {
    Simple,
    Error(String),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Array,
}

pub struct Conn {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

pub type Cmd = Vec<Vec<u8>>;

fn encode(buf: &mut Vec<u8>, cmd: &[Vec<u8>]) {
    buf.extend_from_slice(format!("*{}\r\n", cmd.len()).as_bytes());
    for arg in cmd {
        buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        buf.extend_from_slice(arg);
        buf.extend_from_slice(b"\r\n");
    }
}

impl Conn {
    pub fn read(&mut self) -> Reply {
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        let line = line.trim_end_matches("\r\n");
        let rest = &line[1..];
        match line.as_bytes()[0] {
            b'+' => Reply::Simple,
            b'-' => Reply::Error(rest.to_string()),
            b':' => Reply::Int(rest.parse().unwrap()),
            b'$' => {
                let len: i64 = rest.parse().unwrap();
                if len < 0 {
                    return Reply::Bulk(None);
                }
                let mut data = vec![0u8; len as usize + 2];
                self.reader.read_exact(&mut data).unwrap();
                data.truncate(len as usize);
                Reply::Bulk(Some(data))
            }
            b'*' => {
                let n: i64 = rest.parse().unwrap();
                (0..n.max(0)).for_each(|_| drop(self.read()));
                Reply::Array
            }
            _ => panic!("unexpected reply {line:?}"),
        }
    }

    /// Sends `cmds` in batches and hands back every reply, none an error.
    pub fn pipeline(&mut self, cmds: &[Cmd]) -> Vec<Reply> {
        let replies = self.send(cmds);
        if let Some(Reply::Error(e)) = replies.iter().find(|r| matches!(r, Reply::Error(_))) {
            panic!("{e}");
        }
        replies
    }

    /// Sends `cmds` in batches and hands back every reply.
    pub fn send(&mut self, cmds: &[Cmd]) -> Vec<Reply> {
        let mut replies = Vec::with_capacity(cmds.len());
        for chunk in cmds.chunks(500) {
            let mut buf = Vec::new();
            for cmd in chunk {
                encode(&mut buf, cmd);
            }
            self.writer.write_all(&buf).unwrap();
            for _ in chunk {
                replies.push(self.read());
            }
        }
        replies
    }

    pub fn cmd(&mut self, cmd: &[&[u8]]) -> Reply {
        let cmd: Cmd = cmd.iter().map(|a| a.to_vec()).collect();
        self.send(&[cmd]).pop().unwrap()
    }

    pub fn ok(&mut self, cmd: &[&[u8]]) {
        match self.cmd(cmd) {
            Reply::Simple => {}
            other => panic!("{other:?}"),
        }
    }

    pub fn int(&mut self, cmd: &[&[u8]]) -> i64 {
        match self.cmd(cmd) {
            Reply::Int(n) => n,
            other => panic!("{other:?}"),
        }
    }

    pub fn used_memory(&mut self) -> u64 {
        let Reply::Bulk(Some(info)) = self.cmd(&[b"INFO", b"memory"]) else {
            panic!("INFO memory");
        };
        String::from_utf8(info)
            .unwrap()
            .lines()
            .find_map(|l| l.strip_prefix("used_memory:"))
            .unwrap()
            .trim()
            .parse()
            .unwrap()
    }

    /// `used_memory` once two readings 50 ms apart agree (background frees
    /// done), for at most two seconds.
    pub fn settled_used_memory(&mut self) -> u64 {
        let mut last = self.used_memory();
        for _ in 0..40 {
            std::thread::sleep(Duration::from_millis(50));
            let now = self.used_memory();
            if now == last {
                return now;
            }
            last = now;
        }
        last
    }
}

pub fn b(s: impl ToString) -> Vec<u8> {
    s.to_string().into_bytes()
}

// ── the cases ───────────────────────────────────────────────────────────────

/// The commands that make a key, given its name.
pub type Make = Box<dyn Fn(&[u8]) -> Vec<Cmd>>;
/// What is waited for once the keys are loaded, given their names.
pub type Settle = Box<dyn Fn(&mut Conn, &[Vec<u8>])>;

/// One shape of key: the commands that make the key, and how many members
/// or fields it ends with.
pub struct Case {
    pub name: String,
    pub size: usize,
    /// `SCARD` or `HLEN`.
    pub len_cmd: &'static [u8],
    pub make: Make,
    /// Waited for after loading: fields expiring.
    pub settle: Option<Settle>,
    pub tolerance: Tolerance,
}

pub struct Row {
    pub name: String,
    pub size: usize,
    pub keys: usize,
    pub measured: f64,
    pub usage_default: f64,
    pub usage_all: f64,
}

pub fn run(case: &Case) -> Row {
    let server = Server::start();
    let mut c = server.connect();
    let keys = (MEMBERS_PER_CASE / case.size.max(1)).clamp(200, 20_000);

    // Whatever the shape's commands allocate once, allocated before.
    c.ok(&[b"SELECT", b"1"]);
    let warm: Vec<Cmd> = (0..100)
        .flat_map(|i| (case.make)(format!("warm:{i}").as_bytes()))
        .collect();
    c.pipeline(&warm);
    c.ok(&[b"SELECT", b"0"]);

    let names: Vec<Vec<u8>> = (0..keys).map(|i| b(format!("key:{i}"))).collect();
    let before = c.settled_used_memory();
    let load: Vec<Cmd> = names.iter().flat_map(|k| (case.make)(k)).collect();
    c.pipeline(&load);
    drop(load);
    if let Some(settle) = &case.settle {
        settle(&mut c, &names);
    }
    let after = c.settled_used_memory();

    // Every key is there, the size it should be.
    let lens: Vec<Cmd> = names
        .iter()
        .map(|k| vec![case.len_cmd.to_vec(), k.clone()])
        .collect();
    for (k, reply) in names.iter().zip(c.pipeline(&lens)) {
        assert!(
            matches!(reply, Reply::Int(n) if n as usize == case.size),
            "{}: {} is {reply:?}, not {}",
            case.name,
            String::from_utf8_lossy(k),
            case.size
        );
    }

    let usage = |c: &mut Conn, all: bool| -> f64 {
        let cmds: Vec<Cmd> = names
            .iter()
            .map(|k| {
                let mut cmd = vec![b("MEMORY"), b("USAGE"), k.clone()];
                if all {
                    cmd.extend([b("SAMPLES"), b("0")]);
                }
                cmd
            })
            .collect();
        let total: i64 = c
            .pipeline(&cmds)
            .into_iter()
            .map(|r| match r {
                Reply::Int(n) => n,
                other => panic!("MEMORY USAGE: {other:?}"),
            })
            .sum();
        total as f64 / keys as f64
    };
    let usage_default = usage(&mut c, false);
    let usage_all = usage(&mut c, true);
    Row {
        name: case.name.clone(),
        size: case.size,
        keys,
        measured: after.saturating_sub(before) as f64 / keys as f64,
        usage_default,
        usage_all,
    }
}

pub fn sadd(key: &[u8], members: impl Iterator<Item = Vec<u8>>) -> Cmd {
    let mut cmd = vec![b("SADD"), key.to_vec()];
    cmd.extend(members);
    cmd
}

pub fn hset(key: &[u8], fields: impl Iterator<Item = (Vec<u8>, Vec<u8>)>) -> Cmd {
    let mut cmd = vec![b("HSET"), key.to_vec()];
    for (f, v) in fields {
        cmd.push(f);
        cmd.push(v);
    }
    cmd
}

pub fn within(row: &Row, tolerance: Tolerance, estimate: f64) -> bool {
    let error = estimate - row.measured;
    if error.abs() <= tolerance.bytes {
        return true;
    }
    let allowed = if error < 0.0 {
        tolerance.under
    } else {
        tolerance.over
    };
    error.abs() <= row.measured * allowed
}

/// Runs every case (those whose name has `KIVIDB_MEMORY_ONLY` in it, if it
/// is set) and fails with the ones whose estimates, with the default sample
/// or with `SAMPLES 0`, are not within their tolerance of what was measured.
/// `KIVIDB_MEMORY_REPORT=1` prints every case.
pub fn check(cases: Vec<Case>) {
    let report = std::env::var_os("KIVIDB_MEMORY_REPORT").is_some();
    let mut failures = Vec::new();
    let only = std::env::var("KIVIDB_MEMORY_ONLY").ok();
    for case in cases {
        if only
            .as_ref()
            .is_some_and(|o| !case.name.contains(o.as_str()))
        {
            continue;
        }
        let row = run(&case);
        let pct = |e: f64| (e - row.measured) / row.measured * 100.0;
        let line = format!(
            "{:42} {:>5} {:>6} keys  measured {:>9.1}  usage {:>9.1} ({:+.2}%)  samples 0 {:>9.1} ({:+.2}%)",
            row.name,
            row.size,
            row.keys,
            row.measured,
            row.usage_default,
            pct(row.usage_default),
            row.usage_all,
            pct(row.usage_all),
        );
        if report {
            eprintln!("{line}");
        }
        let tolerance = case.tolerance;
        if !within(&row, tolerance, row.usage_default) || !within(&row, tolerance, row.usage_all) {
            failures.push(line);
        }
    }
    assert!(
        failures.is_empty(),
        "estimates off:\n{}",
        failures.join("\n")
    );
}
