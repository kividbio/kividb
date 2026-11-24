#![allow(clippy::disallowed_methods)]
// tests/bitmap_in_place.rs
//
// A SETBIT that changes a bit writes it where the value is, unless something
// else holds the value, as a snapshot or a full sync being written does.
// Whatever held it keeps the bytes it had. A reply is a copy. Real `kividb`
// processes; a server started with `held` stops each snapshot once it has
// captured the dataset (`failpoint.snapshot-writing.<n>` in its directory),
// for as long as the test leaves the file there.

mod common;

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

#[derive(Debug, PartialEq)]
enum V {
    Str(Vec<u8>),
    Err(String),
    Int(i64),
    Arr(Vec<V>),
    Nil,
}

struct Conn {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Conn {
    fn connect(port: u16) -> Self {
        let s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        s.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
        let writer = s.try_clone().unwrap();
        Self {
            reader: BufReader::new(s),
            writer,
        }
    }

    fn send(&mut self, args: &[&[u8]]) {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            buf.extend_from_slice(a);
            buf.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&buf).unwrap();
    }

    fn cmd(&mut self, args: &[&[u8]]) -> V {
        self.send(args);
        self.read()
    }

    fn line(&mut self) -> String {
        let mut line = String::new();
        self.reader.read_line(&mut line).expect("a reply");
        line.trim_end_matches(['\r', '\n']).to_string()
    }

    fn read(&mut self) -> V {
        let line = self.line();
        let (kind, rest) = line.split_at(1);
        match kind {
            "+" => V::Str(rest.as_bytes().to_vec()),
            "-" => V::Err(rest.to_string()),
            ":" => V::Int(rest.parse().unwrap()),
            "$" if rest == "-1" => V::Nil,
            "$" => {
                let mut data = vec![0u8; rest.parse::<usize>().unwrap() + 2];
                self.reader.read_exact(&mut data).unwrap();
                data.truncate(data.len() - 2);
                V::Str(data)
            }
            "*" if rest == "-1" => V::Nil,
            "*" => V::Arr(
                (0..rest.parse::<usize>().unwrap())
                    .map(|_| self.read())
                    .collect(),
            ),
            _ => panic!("not a reply: {line:?}"),
        }
    }

    fn info(&mut self, field: &str) -> String {
        let V::Str(info) = self.cmd(&[b"INFO"]) else {
            panic!("INFO");
        };
        let prefix = format!("{field}:");
        String::from_utf8_lossy(&info)
            .lines()
            .find_map(|l| l.strip_prefix(&prefix).map(str::to_string))
            .unwrap_or_default()
    }

    fn wait_for(&mut self, field: &str, value: &str) {
        let deadline = Instant::now() + Duration::from_secs(60);
        while self.info(field) != value {
            assert!(Instant::now() < deadline, "{field} never became {value}");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn setbit(&mut self, key: &str, bit: usize, value: u8) -> V {
        let (bit, value) = (bit.to_string(), value.to_string());
        self.cmd(&[b"SETBIT", key.as_bytes(), bit.as_bytes(), value.as_bytes()])
    }

    fn get(&mut self, key: &str) -> V {
        self.cmd(&[b"GET", key.as_bytes()])
    }
}

struct Server {
    port: u16,
    dir: PathBuf,
    child: Child,
}

impl Server {
    /// A server in a directory of its own; with `held`, its snapshots stop
    /// once captured.
    fn start(extra: &[&str], held: bool) -> Self {
        Self::start_in(tempfile::tempdir().unwrap().keep(), extra, held)
    }

    fn start_in(dir: PathBuf, extra: &[&str], held: bool) -> Self {
        let (child, port) = common::start_verified(|port| {
            let log = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(dir.join("server.log"))
                .unwrap();
            let mut command = Command::new(env!("CARGO_BIN_EXE_kividb"));
            command
                .current_dir(&dir)
                .args(["--port", &port.to_string(), "--bind", "127.0.0.1"])
                .args(["--threads", "4", "--io-model", "tokio"])
                .args(["--configfile", "kividb.conf"])
                .args(extra)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(log);
            if held {
                command.env("KIVIDB_TEST_FAILPOINTS", "snapshot-writing");
            }
            command.spawn().expect("could not start kividb")
        });
        Server { port, dir, child }
    }

    fn conn(&self) -> Conn {
        Conn::connect(self.port)
    }

    /// Killed, with no save of its own, and started again on what it wrote.
    fn restart(mut self, extra: &[&str]) -> Self {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let dir = std::mem::take(&mut self.dir);
        Self::start_in(dir, extra, false)
    }

    /// Wait for the `n`th snapshot to have captured the dataset and stopped.
    /// Removing the file returned lets it go on.
    fn snapshot_held(&self, n: u32) -> PathBuf {
        let path = self.dir.join(format!("failpoint.snapshot-writing.{n}"));
        let deadline = Instant::now() + Duration::from_secs(60);
        while !Path::new(&path).exists() {
            assert!(Instant::now() < deadline, "no snapshot reached {path:?}");
            std::thread::sleep(Duration::from_millis(10));
        }
        path
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if !self.dir.as_os_str().is_empty() {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

/// `len` bytes that differ from their neighbours; the first is zero.
fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// A string in its entry, in a slab, on its own, and a large one.
const SIZES: [usize; 4] = [100, 16 << 10, 1 << 20, 64 << 20];

fn key(len: usize) -> String {
    format!("k{len}")
}

fn fill(c: &mut Conn, sizes: &[usize]) {
    for &len in sizes {
        let reply = c.cmd(&[b"SET", key(len).as_bytes(), &pattern(len)]);
        assert_eq!(reply, V::Str(b"OK".to_vec()), "{len}");
    }
}

/// A client reading a 64 MiB `GET` slowly gets the value as the `GET` found
/// it, whatever `SETBIT`s run before it has read the rest. The reply is a
/// copy made under the shard's lock, holding nothing of the stored value, so
/// those `SETBIT`s are written in place.
#[test]
fn a_slow_reader_of_a_get_gets_the_bytes_from_before_a_setbit() {
    let server = Server::start(&[], false);
    let mut c = server.conn();
    let len = 64 << 20;
    fill(&mut c, &[len]);
    let value = pattern(len);
    let mut reader = server.conn();
    reader.send(&[b"GET", key(len).as_bytes()]);
    // The reply has begun, so the `GET` has run.
    assert_eq!(reader.line(), format!("${len}"));
    let mut got = vec![0u8; 1 << 20];
    reader.reader.read_exact(&mut got).unwrap();
    // A bit in the part read, and one in the part not read yet.
    for byte in [0, len - 1] {
        let old = value[byte] >> 7;
        assert_eq!(c.setbit(&key(len), byte * 8, 1 - old), V::Int(old as i64));
    }
    let mut rest = vec![0u8; len - got.len() + 2];
    reader.reader.read_exact(&mut rest).unwrap();
    got.extend_from_slice(&rest[..rest.len() - 2]);
    assert!(got == value, "the reply changed while it was read");
    let mut now = value;
    now[0] ^= 0x80;
    now[len - 1] ^= 0x80;
    assert!(c.get(&key(len)) == V::Str(now), "the value did not change");
}

/// A `BGSAVE` holds each string it captured until it has written it:
/// `SETBIT`s meanwhile copy the value, and in place once it is unheld.
/// The dump has the bytes from before them; the live value has both.
#[test]
fn a_bgsave_writes_the_bytes_from_before_a_setbit() {
    let server = Server::start(&[], true);
    let mut c = server.conn();
    fill(&mut c, &SIZES);
    assert_eq!(
        c.cmd(&[b"BGSAVE"]),
        V::Str(b"Background saving started".to_vec())
    );
    let held = server.snapshot_held(1);
    for len in SIZES {
        // The first copies the value from the capture; the second is in place.
        assert_eq!(c.setbit(&key(len), 0, 1), V::Int(0), "{len}");
        assert_eq!(c.setbit(&key(len), 1, 1), V::Int(0), "{len}");
    }
    std::fs::remove_file(held).unwrap();
    c.wait_for("rdb_bgsave_in_progress", "0");
    assert_eq!(c.info("rdb_last_bgsave_status"), "ok");
    for len in SIZES {
        let mut now = pattern(len);
        now[0] = 0xC0;
        assert!(c.get(&key(len)) == V::Str(now), "{len}");
    }
    let server = server.restart(&[]);
    let mut c = server.conn();
    for len in SIZES {
        assert!(
            c.get(&key(len)) == V::Str(pattern(len)),
            "{len} in the dump"
        );
    }
}

/// A replica's full sync captures the values; `SETBIT`s on the master while
/// it is held, and after, reach the replica, which ends with the master's
/// bytes.
#[test]
fn a_replica_synced_across_setbits_ends_with_the_masters_bytes() {
    let master = Server::start(&[], true);
    let mut m = master.conn();
    let sizes = &SIZES[..3];
    fill(&mut m, sizes);
    let port = master.port.to_string();
    let replica = Server::start(&["--replicaof", "127.0.0.1", &port], false);
    let held = master.snapshot_held(1);
    for &len in sizes {
        assert_eq!(m.setbit(&key(len), 0, 1), V::Int(0), "{len}");
        assert_eq!(m.setbit(&key(len), 9, 1), V::Int(0), "{len}");
    }
    std::fs::remove_file(held).unwrap();
    let mut r = replica.conn();
    r.wait_for("master_link_status", "up");
    for &len in sizes {
        // In place on both, now nothing holds the values.
        assert_eq!(m.setbit(&key(len), 17, 1), V::Int(0), "{len}");
        // Grown: a copy on both.
        assert_eq!(m.setbit(&key(len), len * 8 + 3, 1), V::Int(0), "{len}");
    }
    let deadline = Instant::now() + Duration::from_secs(60);
    for &len in sizes {
        let want = m.get(&key(len));
        let mut now = pattern(len);
        now[0] = 0x80;
        now[1] |= 0x40;
        now[2] |= 0x40;
        now.push(0x10);
        assert!(want == V::Str(now), "{len} on the master");
        while r.get(&key(len)) != want {
            assert!(Instant::now() < deadline, "{len}: the replica differs");
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

/// The image a full sync sends has the values as it captured them:
/// `SETBIT`s while it is held do not reach it. Read as a replica would,
/// then loaded by a server of its own. (A replica alone would not show
/// it: it replays the same `SETBIT`s over the image.)
#[test]
fn a_full_sync_sends_the_bytes_from_before_a_setbit() {
    let master = Server::start(&[], true);
    let mut m = master.conn();
    fill(&mut m, &SIZES);
    let sync = TcpStream::connect(("127.0.0.1", master.port)).unwrap();
    sync.set_read_timeout(Some(Duration::from_secs(60)))
        .unwrap();
    (&sync)
        .write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
        .unwrap();
    let held = master.snapshot_held(1);
    for len in SIZES {
        // The first copies the value from the capture; the second is in place.
        assert_eq!(m.setbit(&key(len), 0, 1), V::Int(0), "{len}");
        assert_eq!(m.setbit(&key(len), 1, 1), V::Int(0), "{len}");
    }
    std::fs::remove_file(held).unwrap();
    let mut sync = BufReader::new(sync);
    let mut line = String::new();
    sync.read_line(&mut line).unwrap();
    assert!(line.starts_with("+FULLRESYNC"), "{line:?}");
    // The image's length, after any newlines sent to keep the link alive.
    let len = loop {
        line.clear();
        sync.read_line(&mut line).unwrap();
        if let Some(len) = line.trim_end().strip_prefix('$') {
            break len.parse::<usize>().unwrap();
        }
        assert!(line.trim_end().is_empty(), "{line:?}");
    };
    let mut image = vec![0u8; len];
    sync.read_exact(&mut image).unwrap();
    let dir = tempfile::tempdir().unwrap().keep();
    std::fs::write(dir.join("dump.kdb"), &image).unwrap();
    let loaded = Server::start_in(dir, &[], false);
    let mut c = loaded.conn();
    for len in SIZES {
        assert!(
            c.get(&key(len)) == V::Str(pattern(len)),
            "{len} in the image"
        );
    }
    let mut now = pattern(SIZES[0]);
    now[0] = 0xC0;
    assert!(m.get(&key(SIZES[0])) == V::Str(now), "the master's value");
}

/// `SETBIT`s written in place are logged and replayed as before, and one
/// that finds the bit as asked is not logged at all.
#[test]
fn aof_replays_setbits_written_in_place_and_skips_those_that_change_nothing() {
    let aof = ["--appendonly", "yes", "--appendfsync", "always"];
    let server = Server::start(&aof, false);
    let mut c = server.conn();
    let len = 64 << 10;
    fill(&mut c, &[len]);
    let before = c.info("rdb_changes_since_last_save");
    assert_eq!(c.setbit(&key(len), 0, 1), V::Int(0));
    assert_eq!(c.setbit(&key(len), 0, 1), V::Int(1));
    assert_eq!(c.setbit(&key(len), 14, 0), V::Int(0));
    assert_eq!(c.setbit(&key(len), 8 * len - 1, 1), V::Int(0));
    let after: i64 = c.info("rdb_changes_since_last_save").parse().unwrap();
    assert_eq!(after - before.parse::<i64>().unwrap(), 2, "dirty");
    let logged = logged_setbits(&server.dir);
    assert_eq!(logged, 2, "SETBITs in the AOF");
    let server = server.restart(&aof);
    let mut c = server.conn();
    let mut want = pattern(len);
    want[0] = 0x80;
    want[len - 1] |= 1;
    assert!(c.get(&key(len)) == V::Str(want), "replayed");
}

/// How many `SETBIT`s the AOF files under `dir` hold.
fn logged_setbits(dir: &Path) -> usize {
    let mut count = 0;
    let mut dirs = vec![dir.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(dir).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                dirs.push(path);
            } else if path.to_string_lossy().contains(".aof") {
                let bytes = std::fs::read(&path).unwrap();
                count += bytes
                    .windows(6)
                    .filter(|w| w.eq_ignore_ascii_case(b"SETBIT"))
                    .count();
            }
        }
    }
    count
}
