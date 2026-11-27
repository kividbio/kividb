#![allow(clippy::disallowed_methods)]
// tests/append_in_place.rs
//
// An APPEND or a SETRANGE writes where the string is kept unless something
// else holds it, as a snapshot or a full sync being written does; a long
// string keeps room to spare for the next appends. Whatever held it keeps
// the bytes it had, and nothing that saves, sends or copies a string sees
// the room.
// Real `kividb` processes; a server started with `held` stops each
// snapshot once it has captured the dataset
// (`failpoint.snapshot-writing.<n>` in its directory), for as long as the
// test leaves the file there.

mod common;

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

#[derive(Debug, PartialEq, Clone)]
enum V {
    Str(Vec<u8>),
    Err(String),
    Int(i64),
    Arr(Vec<V>),
    Nil,
}

impl V {
    /// The text of every string in it, one after another.
    fn text(&self) -> String {
        match self {
            V::Str(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            V::Arr(items) => items.iter().map(V::text).collect::<Vec<_>>().join(" "),
            other => format!("{other:?}"),
        }
    }

    fn int(&self) -> i64 {
        match self {
            V::Int(n) => *n,
            other => panic!("not an integer: {other:?}"),
        }
    }
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
        let V::Str(info) = self.cmd(&[b"INFO", b"everything"]) else {
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

    fn append(&mut self, key: &str, extra: &[u8]) -> V {
        self.cmd(&[b"APPEND", key.as_bytes(), extra])
    }

    fn get(&mut self, key: &str) -> V {
        self.cmd(&[b"GET", key.as_bytes()])
    }

    fn usage(&mut self, key: &str) -> i64 {
        self.cmd(&[b"MEMORY", b"USAGE", key.as_bytes()]).int()
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
                .args(["--enable-debug-command", "yes"])
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

/// `len` bytes that differ from their neighbours.
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

/// `pattern(len)` with `tail` after it.
fn with(len: usize, tail: &[u8]) -> Vec<u8> {
    let mut bytes = pattern(len);
    bytes.extend_from_slice(tail);
    bytes
}

/// The reply is the new length; a key not there is made; the time to live
/// stays across every form the string passes through; another type is
/// refused, and a key past its time is not there.
#[test]
fn appends_reply_the_length_keep_the_time_to_live_and_refuse_as_redis() {
    let server = Server::start(&[], false);
    let mut c = server.conn();
    assert_eq!(c.append("k", b"ab"), V::Int(2));
    assert_eq!(c.append("k", b""), V::Int(2));
    assert_eq!(c.append("e", b""), V::Int(0));
    assert_eq!(c.cmd(&[b"EXISTS", b"e"]), V::Int(1));
    assert_eq!(c.cmd(&[b"EXPIRE", b"k", b"1000"]), V::Int(1));
    let mut model = b"ab".to_vec();
    // From the entry through the slabs to a block of its own.
    for i in 0..400usize {
        let extra = vec![b'a' + (i % 26) as u8; 1 + i * 7];
        model.extend_from_slice(&extra);
        assert_eq!(c.append("k", &extra), V::Int(model.len() as i64), "{i}");
    }
    assert!(model.len() > 500_000);
    assert!(c.get("k") == V::Str(model.clone()));
    assert_eq!(c.cmd(&[b"STRLEN", b"k"]), V::Int(model.len() as i64));
    let ttl = c.cmd(&[b"TTL", b"k"]).int();
    assert!((990..=1000).contains(&ttl), "{ttl}");
    let end = (model.len() - 1).to_string();
    let last = c.cmd(&[b"GETRANGE", b"k", end.as_bytes(), end.as_bytes()]);
    assert_eq!(last, V::Str(model[model.len() - 1..].to_vec()));
    c.cmd(&[b"RPUSH", b"list", b"x"]);
    let wrong = c.append("list", b"x");
    assert!(
        matches!(&wrong, V::Err(e) if e.starts_with("WRONGTYPE")),
        "{wrong:?}"
    );
    c.cmd(&[b"SET", b"old", b"gone", b"PX", b"1"]);
    std::thread::sleep(Duration::from_millis(20));
    assert_eq!(c.append("old", b"new"), V::Int(3));
    assert_eq!(c.get("old"), V::Str(b"new".to_vec()));
    assert_eq!(c.cmd(&[b"TTL", b"old"]), V::Int(-1));
}

/// An append written in place fails a `WATCH` on its key and announces
/// itself, as one that copies does; so does an empty one, as in Redis.
#[test]
fn an_append_in_place_fails_a_watch_and_announces_itself() {
    let server = Server::start(&["--notify-keyspace-events", "KA"], false);
    let mut c = server.conn();
    let mut sub = server.conn();
    assert!(matches!(
        sub.cmd(&[b"PSUBSCRIBE", b"__keyspace@0__:*"]),
        V::Arr(_)
    ));
    let mut w = server.conn();
    for len in [100, 16 << 10, 1 << 20] {
        fill(&mut c, &[len]);
        // The first append grows the block; the next ones are in its room.
        c.append(&key(len), b"x");
        let set = sub.read().text();
        assert!(set.ends_with(" set"), "{set}");
        let first = sub.read().text();
        assert!(first.ends_with(" append"), "{first}");
        for extra in [&b"yz"[..], b""] {
            assert_eq!(
                w.cmd(&[b"WATCH", key(len).as_bytes()]),
                V::Str(b"OK".to_vec())
            );
            assert_eq!(w.cmd(&[b"MULTI"]), V::Str(b"OK".to_vec()));
            assert_eq!(
                w.cmd(&[b"GET", key(len).as_bytes()]),
                V::Str(b"QUEUED".to_vec())
            );
            c.append(&key(len), extra);
            assert_eq!(w.cmd(&[b"EXEC"]), V::Nil, "{len} {extra:?}");
            let event = sub.read().text();
            let want = format!("__keyspace@0__:{} append", key(len));
            assert!(event.ends_with(&want), "{event}");
        }
        assert!(c.get(&key(len)) == V::Str(with(len, b"xyz")), "{len}");
    }
}

/// A long string grown by appends keeps room, which `MEMORY USAGE` counts,
/// and nothing else sees: its length, bytes, digest and `DUMP` are a `SET`
/// string's of the same bytes, a `COPY` and a snapshot have exactly those
/// bytes, and the string a snapshot loads has no room.
#[test]
fn an_appended_string_is_saved_dumped_copied_and_digested_by_its_length() {
    let server = Server::start(&[], false);
    let mut c = server.conn();
    for len in [100, 16 << 10, 1 << 20] {
        let (appended, set) = (format!("a{len}"), format!("s{len}"));
        c.cmd(&[b"SET", appended.as_bytes(), &pattern(len)]);
        let tail = b"0123456789".repeat(10);
        assert_eq!(c.append(&appended, &tail), V::Int((len + 100) as i64));
        let want = with(len, &tail);
        c.cmd(&[b"SET", set.as_bytes(), &want]);
        assert_eq!(
            c.cmd(&[b"STRLEN", appended.as_bytes()]),
            V::Int(want.len() as i64)
        );
        assert!(c.get(&appended) == V::Str(want.clone()));
        let digest = |c: &mut Conn, k: &str| c.cmd(&[b"DEBUG", b"DIGEST-VALUE", k.as_bytes()]);
        assert_eq!(digest(&mut c, &appended), digest(&mut c, &set), "{len}");
        let dump = |c: &mut Conn, k: &str| c.cmd(&[b"DUMP", k.as_bytes()]);
        assert!(dump(&mut c, &appended) == dump(&mut c, &set), "{len}");
        let room = c.usage(&appended) - c.usage(&set);
        if len == 1 << 20 {
            // A quarter of its length, kept for the next appends.
            assert_eq!(room, (want.len() / 4) as i64, "{len}");
        } else {
            assert_eq!(room, 0, "{len}");
        }
        let copy = format!("c{len}");
        c.cmd(&[b"COPY", appended.as_bytes(), copy.as_bytes()]);
        assert!(c.get(&copy) == V::Str(want.clone()));
        assert_eq!(c.append(&copy, b"!"), V::Int(want.len() as i64 + 1));
        assert!(
            c.get(&appended) == V::Str(want.clone()),
            "{len}: the copy's append"
        );
    }
    assert_eq!(c.cmd(&[b"SAVE"]), V::Str(b"OK".to_vec()));
    let server = server.restart(&[]);
    let mut c = server.conn();
    for len in [100, 16 << 10, 1 << 20] {
        let (appended, set) = (format!("a{len}"), format!("s{len}"));
        assert!(c.get(&appended) == c.get(&set), "{len} loaded");
        assert_eq!(c.usage(&appended), c.usage(&set), "{len}: loaded with room");
    }
}

/// `used_memory` follows the block an append grows: up when it grows, by
/// what the allocator was asked for, and down to where it was when the key
/// goes.
#[test]
fn used_memory_counts_the_room_an_append_keeps() {
    let server = Server::start(&[], false);
    let mut c = server.conn();
    let used = |c: &mut Conn| c.info("used_memory").parse::<i64>().unwrap();
    let len = 8 << 20;
    c.cmd(&[b"SET", b"big", &pattern(len)]);
    let before = used(&mut c);
    c.append("big", b"x");
    let grown = used(&mut c) - before;
    // A quarter more, give or take what other threads allocate meanwhile.
    let room = ((len + 1) / 4) as i64;
    assert!(
        (room - (64 << 10)..room + (256 << 10)).contains(&grown),
        "{grown} for {room}"
    );
    for _ in 0..1000 {
        c.append("big", b"0123456789");
    }
    let after = used(&mut c) - before;
    assert!(
        (after - grown).abs() < 256 << 10,
        "{after} after appends in the room"
    );
    c.cmd(&[b"DEL", b"big"]);
    // Freed on the lazy-free thread: wait for the reading to settle there.
    let deadline = Instant::now() + Duration::from_secs(30);
    while used(&mut c) >= before - (len as i64) / 2 {
        assert!(Instant::now() < deadline, "the block never went");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// A `BGSAVE` holds each string it captured until it has written it:
/// `APPEND`s meanwhile copy the value, then go in its room once unheld.
/// The dump has the bytes from before them; the live value has both.
#[test]
fn a_bgsave_writes_the_bytes_from_before_an_append() {
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
        assert_eq!(c.append(&key(len), b"ab"), V::Int(len as i64 + 2), "{len}");
        assert_eq!(c.append(&key(len), b"cd"), V::Int(len as i64 + 4), "{len}");
    }
    std::fs::remove_file(held).unwrap();
    c.wait_for("rdb_bgsave_in_progress", "0");
    assert_eq!(c.info("rdb_last_bgsave_status"), "ok");
    for len in SIZES {
        assert!(c.get(&key(len)) == V::Str(with(len, b"abcd")), "{len}");
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

/// A replica's full sync captures the values; `APPEND`s on the master while
/// it is held, and after, reach the replica, which ends with the master's
/// bytes.
#[test]
fn a_replica_synced_across_appends_ends_with_the_masters_bytes() {
    let master = Server::start(&[], true);
    let mut m = master.conn();
    let sizes = &SIZES[..3];
    fill(&mut m, sizes);
    let port = master.port.to_string();
    let replica = Server::start(&["--replicaof", "127.0.0.1", &port], false);
    let held = master.snapshot_held(1);
    for &len in sizes {
        m.append(&key(len), b"ab");
        m.append(&key(len), b"cd");
    }
    std::fs::remove_file(held).unwrap();
    let mut r = replica.conn();
    r.wait_for("master_link_status", "up");
    let tail = vec![b'z'; 300_000];
    for &len in sizes {
        // In place on both, now nothing holds the values; then grown.
        m.append(&key(len), b"ef");
        m.append(&key(len), &tail);
    }
    let deadline = Instant::now() + Duration::from_secs(60);
    for &len in sizes {
        let want = m.get(&key(len));
        let mut now = with(len, b"abcdef");
        now.extend_from_slice(&tail);
        assert!(want == V::Str(now), "{len} on the master");
        while r.get(&key(len)) != want {
            assert!(Instant::now() < deadline, "{len}: the replica differs");
            std::thread::sleep(Duration::from_millis(20));
        }
        let digest = |c: &mut Conn| c.cmd(&[b"DEBUG", b"DIGEST-VALUE", key(len).as_bytes()]);
        assert_eq!(digest(&mut m), digest(&mut r), "{len}");
    }
}

/// The image a full sync sends has the values as it captured them:
/// `APPEND`s while it is held do not reach it. Read as a replica would,
/// then loaded by a server of its own.
#[test]
fn a_full_sync_sends_the_bytes_from_before_an_append() {
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
        m.append(&key(len), b"ab");
        m.append(&key(len), b"cd");
    }
    std::fs::remove_file(held).unwrap();
    let mut sync = BufReader::new(sync);
    let mut line = String::new();
    sync.read_line(&mut line).unwrap();
    assert!(line.starts_with("+FULLRESYNC"), "{line:?}");
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
    assert!(m.get(&key(SIZES[2])) == V::Str(with(SIZES[2], b"abcd")));
}

/// An AOF rewrite's base is the dataset as the rewrite captured it:
/// `APPEND`s while it is held go to the log after the base, not into it.
/// Loaded alone, the base has the bytes from before; with its log, after.
#[test]
fn an_aof_rewrites_base_has_the_bytes_from_before_an_append() {
    let aof = ["--appendonly", "yes", "--appendfsync", "always"];
    let server = Server::start(&aof, true);
    let mut c = server.conn();
    fill(&mut c, &SIZES);
    assert_eq!(
        c.cmd(&[b"BGREWRITEAOF"]),
        V::Str(b"Background append only file rewriting started".to_vec())
    );
    let held = server.snapshot_held(1);
    for len in SIZES {
        c.append(&key(len), b"ab");
        c.append(&key(len), b"cd");
    }
    std::fs::remove_file(held).unwrap();
    c.wait_for("aof_rewrite_in_progress", "0");
    assert_eq!(c.info("aof_last_bgrewrite_status"), "ok");
    let dir = tempfile::tempdir().unwrap().keep();
    std::fs::copy(server.dir.join("dump.kdb"), dir.join("dump.kdb")).unwrap();
    let base = Server::start_in(dir, &[], false);
    let mut b = base.conn();
    for len in SIZES {
        assert!(
            b.get(&key(len)) == V::Str(pattern(len)),
            "{len} in the base"
        );
    }
    let server = server.restart(&aof);
    let mut c = server.conn();
    for len in SIZES {
        assert!(
            c.get(&key(len)) == V::Str(with(len, b"abcd")),
            "{len} from the base and log"
        );
    }
}

/// `APPEND`s written in place are logged and replayed as before, each a
/// change; an empty one too, as in Redis.
#[test]
fn aof_replays_appends_written_in_place() {
    let aof = ["--appendonly", "yes", "--appendfsync", "always"];
    let server = Server::start(&aof, false);
    let mut c = server.conn();
    let len = 1 << 20;
    fill(&mut c, &[len]);
    let before: i64 = c.info("rdb_changes_since_last_save").parse().unwrap();
    for extra in [&b"ab"[..], b"cd", b"", b"ef"] {
        c.append(&key(len), extra);
    }
    let after: i64 = c.info("rdb_changes_since_last_save").parse().unwrap();
    assert_eq!(after - before, 4, "dirty");
    let server = server.restart(&aof);
    let mut c = server.conn();
    assert!(c.get(&key(len)) == V::Str(with(len, b"abcdef")), "replayed");
}

/// The bytes the stress test's key `i` has after `n` appends: chunks of
/// `chunk` bytes, the `j`th all `j % 251`.
fn stream(chunk: usize, n: usize) -> Vec<u8> {
    (0..n).flat_map(|j| vec![(j % 251) as u8; chunk]).collect()
}

/// How many appends `bytes` is the stress test's value after, if it is
/// that value at one moment.
fn appends_in(bytes: &[u8], chunk: usize, start: usize) -> Option<usize> {
    if !bytes.len().is_multiple_of(chunk) || bytes.len() / chunk < start {
        return None;
    }
    let n = bytes.len() / chunk;
    (bytes == stream(chunk, n)).then_some(n - start)
}

/// Writers appending to keys in every form, readers holding the values
/// through `GETEX` and `DUMP`, and `BGSAVE`s held while the writers go
/// on. Each value read, and each one in a dump, is the value at one
/// moment: readers see it only grow, and a dump has it as at its capture.
#[test]
fn readers_and_dumps_see_each_value_at_one_moment_across_appends() {
    const SAVES: u32 = 3;
    const GAP: usize = 40;
    const WRITES: usize = 3000;
    // (chunk, appends the value starts with): in the entry, a slab block,
    // and a block of its own.
    let keys: [(usize, usize); 3] = [(1, 10), (37, 300), (1000, 1200)];
    let server = Server::start(&[], true);
    let mut c = server.conn();
    for (i, &(chunk, start)) in keys.iter().enumerate() {
        let reply = c.cmd(&[b"SET", format!("s{i}").as_bytes(), &stream(chunk, start)]);
        assert_eq!(reply, V::Str(b"OK".to_vec()));
    }
    let sent: Vec<AtomicUsize> = keys.iter().map(|_| AtomicUsize::new(0)).collect();
    let answered: Vec<AtomicUsize> = keys.iter().map(|_| AtomicUsize::new(0)).collect();
    let done = AtomicBool::new(false);
    let failed = AtomicBool::new(false);
    let load = |v: &[AtomicUsize]| {
        v.iter()
            .map(|a| a.load(Ordering::SeqCst))
            .collect::<Vec<_>>()
    };
    let images = std::thread::scope(|scope| {
        let writers: Vec<_> = keys
            .iter()
            .enumerate()
            .map(|(i, &(chunk, start))| {
                let (server, sent, answered, failed) = (&server, &sent, &answered, &failed);
                scope.spawn(move || {
                    let mut c = server.conn();
                    let key = format!("s{i}");
                    for n in 0..WRITES {
                        if failed.load(Ordering::SeqCst) {
                            return;
                        }
                        let j = start + n;
                        sent[i].fetch_add(1, Ordering::SeqCst);
                        let reply = c.append(&key, &vec![(j % 251) as u8; chunk]);
                        assert_eq!(reply, V::Int(((j + 1) * chunk) as i64), "{key}");
                        answered[i].fetch_add(1, Ordering::SeqCst);
                    }
                })
            })
            .collect();
        let readers: Vec<_> = (0..4)
            .map(|r| {
                let (server, done) = (&server, &done);
                scope.spawn(move || {
                    let mut c = server.conn();
                    let dumps = r % 2 == 1;
                    let copy = format!("copy{r}");
                    let mut last = vec![0; keys.len()];
                    let mut rounds = 0;
                    loop {
                        let finished = done.load(Ordering::SeqCst);
                        for (i, &(chunk, start)) in keys.iter().enumerate() {
                            let key = format!("s{i}");
                            let bytes = if dumps {
                                let V::Str(dump) = c.cmd(&[b"DUMP", key.as_bytes()]) else {
                                    panic!("DUMP {key}");
                                };
                                let restore: [&[u8]; 5] =
                                    [b"RESTORE", copy.as_bytes(), b"0", &dump, b"REPLACE"];
                                assert_eq!(c.cmd(&restore), V::Str(b"OK".to_vec()), "{key}");
                                c.get(&copy)
                            } else {
                                c.cmd(&[b"GETEX", key.as_bytes()])
                            };
                            let V::Str(bytes) = bytes else {
                                panic!("{key}: {bytes:?}");
                            };
                            let at = appends_in(&bytes, chunk, start)
                                .unwrap_or_else(|| panic!("{key}: not one moment's value"));
                            assert!(at >= last[i], "{key}: {at} after {}", last[i]);
                            last[i] = at;
                        }
                        rounds += 1;
                        if finished {
                            break;
                        }
                    }
                    assert!(last.iter().all(|&n| n == WRITES), "{last:?}");
                    rounds
                })
            })
            .collect();
        let mut c = server.conn();
        let mut images = Vec::new();
        for n in 1..=SAVES {
            // Wait for a share of the writes before each save.
            let share = WRITES * n as usize / (SAVES as usize + 1);
            let deadline = Instant::now() + Duration::from_secs(120);
            while load(&answered).iter().any(|&a| a < share) {
                assert!(Instant::now() < deadline, "the writers stalled");
                std::thread::sleep(Duration::from_millis(5));
            }
            let lo = load(&answered);
            let reply = c.cmd(&[b"BGSAVE"]);
            assert_eq!(reply, V::Str(b"Background saving started".to_vec()));
            let held = server.snapshot_held(n);
            let hi = load(&sent);
            // Writes land while the capture is held: each copies, then in place.
            let deadline = Instant::now() + Duration::from_secs(120);
            while load(&answered)
                .iter()
                .zip(&hi)
                .any(|(&a, &h)| a < (h + GAP).min(WRITES))
            {
                assert!(Instant::now() < deadline, "the writers stalled while held");
                std::thread::sleep(Duration::from_millis(5));
            }
            std::fs::remove_file(held).unwrap();
            c.wait_for("rdb_bgsave_in_progress", "0");
            assert_eq!(c.info("rdb_last_bgsave_status"), "ok");
            let image = std::fs::read(server.dir.join("dump.kdb")).unwrap();
            images.push((image, lo, hi));
        }
        for writer in writers {
            if writer.join().is_err() {
                failed.store(true, Ordering::SeqCst);
                panic!("a writer failed");
            }
        }
        done.store(true, Ordering::SeqCst);
        let rounds: usize = readers.into_iter().map(|r| r.join().unwrap()).sum();
        assert!(rounds > 4, "{rounds} rounds of reads");
        images
    });
    for (n, (image, lo, hi)) in images.into_iter().enumerate() {
        let dir = tempfile::tempdir().unwrap().keep();
        std::fs::write(dir.join("dump.kdb"), image).unwrap();
        let loaded = Server::start_in(dir, &[], false);
        let mut c = loaded.conn();
        for (i, &(chunk, start)) in keys.iter().enumerate() {
            let V::Str(bytes) = c.get(&format!("s{i}")) else {
                panic!("dump {n}: no s{i}");
            };
            let at = appends_in(&bytes, chunk, start);
            assert!(
                at.is_some_and(|at| lo[i] <= at && at <= hi[i]),
                "dump {n}, s{i}: {at:?}, captured between {} and {}",
                lo[i],
                hi[i]
            );
        }
    }
}

// ── SETRANGE ────────────────────────────────────────────────────────────────

fn setrange(c: &mut Conn, key: &str, offset: usize, patch: &[u8]) -> V {
    let offset = offset.to_string();
    c.cmd(&[b"SETRANGE", key.as_bytes(), offset.as_bytes(), patch])
}

/// `bytes` with `patch` written from `offset`, zero-filled up to it. An
/// empty patch leaves them as they are, however far the offset.
fn patched(mut bytes: Vec<u8>, offset: usize, patch: &[u8]) -> Vec<u8> {
    if patch.is_empty() {
        return bytes;
    }
    if bytes.len() < offset + patch.len() {
        bytes.resize(offset + patch.len(), 0);
    }
    bytes[offset..offset + patch.len()].copy_from_slice(patch);
    bytes
}

/// A `BGSAVE` holds each string it captured until it has written it: a
/// `SETRANGE` meanwhile copies the value, and the holder keeps its bytes.
/// The dump has the bytes from before; the live value has every write.
#[test]
fn a_bgsave_writes_the_bytes_from_before_a_setrange() {
    let server = Server::start(&[], true);
    let mut c = server.conn();
    fill(&mut c, &SIZES);
    assert_eq!(
        c.cmd(&[b"BGSAVE"]),
        V::Str(b"Background saving started".to_vec())
    );
    let held = server.snapshot_held(1);
    for len in SIZES {
        let k = key(len);
        assert_eq!(setrange(&mut c, &k, 1, b"ab"), V::Int(len as i64), "{len}");
        let grown = setrange(&mut c, &k, len + 2, b"cd");
        assert_eq!(grown, V::Int(len as i64 + 4), "{len}");
    }
    std::fs::remove_file(held).unwrap();
    c.wait_for("rdb_bgsave_in_progress", "0");
    assert_eq!(c.info("rdb_last_bgsave_status"), "ok");
    for len in SIZES {
        let want = patched(patched(pattern(len), 1, b"ab"), len + 2, b"cd");
        assert!(c.get(&key(len)) == V::Str(want), "{len}");
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

/// A replica's full sync captures the values; `SETRANGE`s on the master
/// while it is held, and after (in place, empty, grown), reach the
/// replica, which ends with the master's bytes. An empty one on a missing
/// key makes it on neither.
#[test]
fn a_replica_synced_across_setranges_ends_with_the_masters_bytes() {
    let master = Server::start(&[], true);
    let mut m = master.conn();
    let sizes = &SIZES[..3];
    fill(&mut m, sizes);
    let port = master.port.to_string();
    let replica = Server::start(&["--replicaof", "127.0.0.1", &port], false);
    let held = master.snapshot_held(1);
    for &len in sizes {
        setrange(&mut m, &key(len), 1, b"ab");
        setrange(&mut m, &key(len), 5, b"cd");
    }
    std::fs::remove_file(held).unwrap();
    let mut r = replica.conn();
    r.wait_for("master_link_status", "up");
    for &len in sizes {
        setrange(&mut m, &key(len), len / 2, b"ef");
        setrange(&mut m, &key(len), len + 100, b"");
        setrange(&mut m, &key(len), len + 3, b"gh");
    }
    assert_eq!(setrange(&mut m, "missing", 7, b""), V::Int(0));
    m.cmd(&[b"SET", b"marker", b"1"]);
    let deadline = Instant::now() + Duration::from_secs(60);
    for &len in sizes {
        let want = m.get(&key(len));
        let mut now = patched(patched(pattern(len), 1, b"ab"), 5, b"cd");
        // Every call the master took, the empty one past the end included.
        now = patched(patched(now, len / 2, b"ef"), len + 100, b"");
        assert_eq!(now.len(), len, "{len}: an empty patch grows nothing");
        now = patched(now, len + 3, b"gh");
        assert!(want == V::Str(now), "{len} on the master");
        while r.get(&key(len)) != want {
            assert!(Instant::now() < deadline, "{len}: the replica differs");
            std::thread::sleep(Duration::from_millis(20));
        }
        let digest = |c: &mut Conn| c.cmd(&[b"DEBUG", b"DIGEST-VALUE", key(len).as_bytes()]);
        assert_eq!(digest(&mut m), digest(&mut r), "{len}");
    }
    while r.get("marker") != V::Str(b"1".to_vec()) {
        assert!(Instant::now() < deadline, "the marker never arrived");
        std::thread::sleep(Duration::from_millis(20));
    }
    for c in [&mut m, &mut r] {
        assert_eq!(c.cmd(&[b"EXISTS", b"missing"]), V::Int(0));
    }
}

/// A `SETRANGE` on a missing key makes it, kept inline, in a slab or on its
/// own: zeros up to the offset, then the patch byte for byte (NULs and
/// bytes that are not UTF-8 included), and no time to live.
#[test]
fn a_setrange_on_a_missing_key_makes_it_zero_filled() {
    let server = Server::start(&[], false);
    let mut c = server.conn();
    let patch = b"\x00\xff\xfe\x00a\x80";
    for (k, offset) in [
        ("at0", 0),
        ("inline", 5),
        ("slab", 16 << 10),
        ("own", 1 << 20),
    ] {
        let len = (offset + patch.len()) as i64;
        assert_eq!(setrange(&mut c, k, offset, patch), V::Int(len), "{k}");
        let mut want = vec![0u8; offset];
        want.extend_from_slice(patch);
        assert!(c.get(k) == V::Str(want), "{k}");
        assert_eq!(c.cmd(&[b"STRLEN", k.as_bytes()]), V::Int(len), "{k}");
        assert_eq!(c.cmd(&[b"TTL", k.as_bytes()]), V::Int(-1), "{k}");
    }
}

/// `SETRANGE`s written in place, copied and grown are logged and replayed.
/// An empty one is neither counted as a change nor logged, as in Redis,
/// and a missing key is not made.
#[test]
fn aof_replays_setranges_written_in_place() {
    let aof = ["--appendonly", "yes", "--appendfsync", "always"];
    let server = Server::start(&aof, false);
    let mut c = server.conn();
    let len = 1 << 20;
    fill(&mut c, &[len]);
    let before: i64 = c.info("rdb_changes_since_last_save").parse().unwrap();
    let mut want = pattern(len);
    let edits: [(usize, &[u8]); 5] = [
        (0, b"ab"),
        (10, b"cd"),
        (5, b""),
        (len + 3, b"ef"),
        (len / 2, b"gh"),
    ];
    for (offset, patch) in edits {
        setrange(&mut c, &key(len), offset, patch);
        want = patched(want, offset, patch);
    }
    assert_eq!(setrange(&mut c, "missing", 7, b""), V::Int(0));
    let after: i64 = c.info("rdb_changes_since_last_save").parse().unwrap();
    assert_eq!(after - before, 4, "dirty");
    assert_eq!(logged(&server.dir, b"SETRANGE"), 4, "SETRANGEs in the AOF");
    let server = server.restart(&aof);
    let mut c = server.conn();
    assert!(c.get(&key(len)) == V::Str(want), "replayed");
    assert_eq!(c.cmd(&[b"EXISTS", b"missing"]), V::Int(0));
}

/// How many times the AOF files under `dir` name `command`.
fn logged(dir: &Path, command: &[u8]) -> usize {
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
                    .windows(command.len())
                    .filter(|w| w.eq_ignore_ascii_case(command))
                    .count();
            }
        }
    }
    count
}

/// `SETRANGE`s in place allocate nothing: `used_memory` and `MEMORY USAGE`
/// stay where they were.
#[test]
fn used_memory_stays_across_setranges_in_place() {
    let server = Server::start(&[], false);
    let mut c = server.conn();
    let used = |c: &mut Conn| c.info("used_memory").parse::<i64>().unwrap();
    let len = 8 << 20;
    c.cmd(&[b"SET", b"big", &pattern(len)]);
    let usage = c.usage("big");
    let before = used(&mut c);
    for i in 0..1000 {
        setrange(&mut c, "big", i * 8000, b"0123456789");
    }
    let after = used(&mut c);
    assert!((after - before).abs() < 256 << 10, "{before} -> {after}");
    assert_eq!(c.usage("big"), usage);
}

/// A `BGSAVE` holds each string it captured: `SETRANGE`s growing it from
/// its end meanwhile copy it once, then write in the copy's room. The dump
/// has the bytes from before; the live value has every growth, zero gap
/// included.
#[test]
fn a_bgsave_writes_the_bytes_from_before_growing_setranges() {
    let server = Server::start(&[], true);
    let mut c = server.conn();
    fill(&mut c, &SIZES);
    assert_eq!(
        c.cmd(&[b"BGSAVE"]),
        V::Str(b"Background saving started".to_vec())
    );
    let held = server.snapshot_held(1);
    for len in SIZES {
        let k = key(len);
        assert_eq!(setrange(&mut c, &k, len, b"ab"), V::Int(len as i64 + 2));
        assert_eq!(setrange(&mut c, &k, len + 5, b"cd"), V::Int(len as i64 + 7));
    }
    std::fs::remove_file(held).unwrap();
    c.wait_for("rdb_bgsave_in_progress", "0");
    assert_eq!(c.info("rdb_last_bgsave_status"), "ok");
    for len in SIZES {
        assert!(
            c.get(&key(len)) == V::Str(with(len, b"ab\0\0\0cd")),
            "{len}"
        );
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

/// `used_memory` follows the block a growing `SETRANGE` grows, as for an
/// `APPEND`: up by a quarter at the first growth, then flat while later
/// growths fit the room; `MEMORY USAGE` counts the room.
#[test]
fn used_memory_counts_the_room_a_growing_setrange_keeps() {
    let server = Server::start(&[], false);
    let mut c = server.conn();
    let used = |c: &mut Conn| c.info("used_memory").parse::<i64>().unwrap();
    let len = 8 << 20;
    c.cmd(&[b"SET", b"big", &pattern(len)]);
    let usage = c.usage("big");
    let before = used(&mut c);
    setrange(&mut c, "big", len, b"x");
    let grown = used(&mut c) - before;
    let room = ((len + 1) / 4) as i64;
    assert!(
        (room - (64 << 10)..room + (256 << 10)).contains(&grown),
        "{grown} for {room}"
    );
    assert!(
        c.usage("big") - usage >= room - 64,
        "MEMORY USAGE counts the room"
    );
    for i in 0..1000 {
        setrange(&mut c, "big", len + 1 + i * 10, b"0123456789");
    }
    let after = used(&mut c) - before;
    assert!(
        (after - grown).abs() < 256 << 10,
        "{after} after growths in the room"
    );
    assert_eq!(c.cmd(&[b"STRLEN", b"big"]), V::Int(len as i64 + 1 + 10_000));
}

/// Growing `SETRANGE`s, in the room and past it, with and without a gap,
/// reach a replica and the AOF: the replica ends with the master's bytes
/// and a restart replays them.
#[test]
fn growing_setranges_reach_the_replica_and_the_aof() {
    let aof = ["--appendonly", "yes", "--appendfsync", "always"];
    let master = Server::start(&aof, false);
    let mut m = master.conn();
    let sizes = &SIZES[..3];
    fill(&mut m, sizes);
    let port = master.port.to_string();
    let replica = Server::start(&["--replicaof", "127.0.0.1", &port], false);
    let mut r = replica.conn();
    r.wait_for("master_link_status", "up");
    let before: i64 = m.info("rdb_changes_since_last_save").parse().unwrap();
    let mut wants = vec![];
    for &len in sizes {
        let mut want = pattern(len);
        for i in 0..50 {
            let gap = if i % 2 == 0 { 0 } else { 4095 };
            let offset = want.len() + gap;
            setrange(&mut m, &key(len), offset, b"gh");
            want = patched(want, offset, b"gh");
        }
        wants.push(want);
    }
    let after: i64 = m.info("rdb_changes_since_last_save").parse().unwrap();
    assert_eq!(after - before, 50 * sizes.len() as i64, "dirty");
    let deadline = Instant::now() + Duration::from_secs(60);
    for (&len, want) in sizes.iter().zip(&wants) {
        assert!(
            m.get(&key(len)) == V::Str(want.clone()),
            "{len} on the master"
        );
        while r.get(&key(len)) != V::Str(want.clone()) {
            assert!(Instant::now() < deadline, "{len}: the replica differs");
            std::thread::sleep(Duration::from_millis(20));
        }
        let digest = |c: &mut Conn| c.cmd(&[b"DEBUG", b"DIGEST-VALUE", key(len).as_bytes()]);
        assert_eq!(digest(&mut m), digest(&mut r), "{len}");
    }
    drop(r);
    drop(replica);
    let master = master.restart(&aof);
    let mut m = master.conn();
    for (&len, want) in sizes.iter().zip(&wants) {
        assert!(m.get(&key(len)) == V::Str(want.clone()), "{len} replayed");
    }
}

/// A `SETRANGE` that grows a watched string in its room fails the
/// `EXEC`; an empty one past the end does not.
#[test]
fn a_growing_setrange_in_the_room_fails_a_watch() {
    let server = Server::start(&[], false);
    let mut c = server.conn();
    let mut other = server.conn();
    let len = 1 << 20;
    fill(&mut c, &[len]);
    // The first growth makes the room; the watched one writes in it.
    setrange(&mut other, &key(len), len, b"a");
    for (patch, fails) in [(&b"b"[..], true), (&b""[..], false)] {
        c.cmd(&[b"WATCH", key(len).as_bytes()]);
        let strlen = match other.cmd(&[b"STRLEN", key(len).as_bytes()]) {
            V::Int(n) => n as usize,
            v => panic!("{v:?}"),
        };
        setrange(&mut other, &key(len), strlen + 3, patch);
        c.cmd(&[b"MULTI"]);
        c.cmd(&[b"PING"]);
        let exec = c.cmd(&[b"EXEC"]);
        assert_eq!(exec == V::Nil, fails, "{patch:?}: {exec:?}");
    }
}

/// The room a growing `SETRANGE` keeps is allocated memory that `maxmemory`
/// counts: with `noeviction`, a limit with room for the string but not for
/// the quarter more its block grows by refuses the write after the growth,
/// and leaves the string as the growth left it.
#[test]
fn maxmemory_counts_the_room_a_growing_setrange_keeps() {
    let server = Server::start(&["--maxmemory-policy", "noeviction"], false);
    let mut c = server.conn();
    let used = |c: &mut Conn| c.info("used_memory").parse::<i64>().unwrap();
    let len = 8 << 20;
    c.cmd(&[b"SET", b"big", &pattern(len)]);
    let before = used(&mut c);
    let limit = (before + (1 << 20)).to_string();
    let set = c.cmd(&[b"CONFIG", b"SET", b"maxmemory", limit.as_bytes()]);
    assert_eq!(set, V::Str(b"OK".to_vec()));
    assert_eq!(setrange(&mut c, "big", len, b"x"), V::Int(len as i64 + 1));
    let grown = used(&mut c) - before;
    assert!(grown > 1 << 20, "{grown}: the room is counted");
    let refused = setrange(&mut c, "big", len + 1, b"y");
    assert!(
        matches!(&refused, V::Err(e) if e.starts_with("OOM")),
        "{refused:?}"
    );
    assert_eq!(c.cmd(&[b"STRLEN", b"big"]), V::Int(len as i64 + 1));
    assert!(c.get("big") == V::Str(with(len, b"x")));
}
