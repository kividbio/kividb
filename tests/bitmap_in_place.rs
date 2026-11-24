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

/// An AOF rewrite's base is the dataset as the rewrite captured it:
/// `SETBIT`s while it is held go to the log after the base, not into it.
/// Loaded alone, the base has the bytes from before; with its log, after.
#[test]
fn an_aof_rewrites_base_has_the_bytes_from_before_a_setbit() {
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
        // The capture holds each value: neither write may reach the base.
        assert_eq!(c.setbit(&key(len), 0, 1), V::Int(0), "{len}");
        assert_eq!(c.setbit(&key(len), 1, 1), V::Int(0), "{len}");
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
        let mut now = pattern(len);
        now[0] = 0xC0;
        assert!(
            c.get(&key(len)) == V::Str(now),
            "{len} from the base and log"
        );
    }
}

/// Bits at each end of a stress test's value, set in order, then cleared.
const ENDS: usize = 64;

/// The stress test's steps per value: a bit at each end, the start's first.
/// Each end's bits are set, then all but the last cleared.
const STEPS: usize = 2 * ENDS - 1;

/// How many `SETBIT`s a stress test's value of `len` bytes has had, if
/// `bytes` is that value at some moment; `None` for bytes from two moments.
fn version(bytes: &[u8], len: usize) -> Option<usize> {
    let bit = |i: usize| bytes[i / 8] >> (7 - i % 8) & 1 == 1;
    // An end's bits set from its first, then cleared from its first.
    let steps = |from: usize| {
        let first = bit(from);
        let run = (from..from + ENDS).take_while(|&i| bit(i) == first).count();
        if (from + run..from + ENDS).any(|i| bit(i) == first) {
            return None;
        }
        Some(match (first, run) {
            (false, ENDS) => 0,
            (true, run) => run,
            (false, run) => ENDS + run,
        })
    };
    if bytes.len() != len || bytes[ENDS / 8..len - ENDS / 8].iter().any(|&b| b != 0) {
        return None;
    }
    let (start, end) = (steps(0)?, steps(len * 8 - ENDS)?);
    (start == end || start == end + 1).then_some(start + end)
}

/// What the stress test's threads have done, and wait on.
#[derive(Default)]
struct Counts {
    /// `SETBIT`s sent, and answered, per value.
    sent: Vec<usize>,
    answered: Vec<usize>,
    /// `DUMP`s about to be sent, per value.
    dumps: Vec<usize>,
    /// `BGSAVE`s that have captured the dataset and are held.
    held: usize,
    writers_done: usize,
    /// A thread failed: the others stop waiting.
    failed: bool,
}

#[derive(Default)]
struct Progress {
    counts: std::sync::Mutex<Counts>,
    moved: std::sync::Condvar,
}

impl Progress {
    fn lock(&self) -> std::sync::MutexGuard<'_, Counts> {
        self.counts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn update(&self, f: impl FnOnce(&mut Counts)) {
        f(&mut self.lock());
        self.moved.notify_all();
    }

    /// What `ready` returns once it returns something, within a minute.
    fn wait<R>(&self, ready: impl Fn(&Counts) -> Option<R>) -> R {
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut counts = self.lock();
        loop {
            assert!(
                !counts.failed,
                "another of the stress test's threads failed"
            );
            if let Some(r) = ready(&counts) {
                return r;
            }
            let left = deadline.checked_duration_since(Instant::now());
            let left = left.expect("the stress test stalled");
            let waited = self.moved.wait_timeout(counts, left);
            counts = waited.unwrap_or_else(std::sync::PoisonError::into_inner).0;
        }
    }
}

/// Marks the stress test failed if the thread holding it panics.
struct Failing<'a>(&'a Progress);

impl Drop for Failing<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.update(|c| c.failed = true);
        }
    }
}

/// Readers holding the values through `GETEX` and `DUMP`, a writer per
/// value, and `BGSAVE`s held while the writers go on. Each value read,
/// and each one in a dump, is the value at one moment: readers see it
/// only move forward, and a dump has it as at its capture.
///
/// A `DUMP` is a read, unordered against writes: each batch of `SETBIT`s
/// is sent as one starts, to land while it copies the value. A `GETEX`
/// is a write, which its key's order runs whole before or after them.
#[test]
fn readers_and_dumps_see_each_value_at_one_moment_across_setbits() {
    // Readers through `GETEX`, and as many through `DUMP`.
    const READERS: usize = 2;
    const SAVES: u32 = 3;
    // Writes each dump waits for, after its capture, before it is written.
    const GAP: usize = 32;
    // The largest is copied for longest, while several writes land.
    let sizes = &[100, 16 << 10, 4 << 20];
    let writes = 2 * STEPS;
    let server = Server::start(&[], true);
    let mut c = server.conn();
    for &len in sizes {
        let reply = c.cmd(&[b"SET", key(len).as_bytes(), &vec![0; len]]);
        assert_eq!(reply, V::Str(b"OK".to_vec()), "{len}");
    }
    let progress = Progress::default();
    progress.update(|c| {
        c.sent = vec![0; sizes.len()];
        c.answered = vec![0; sizes.len()];
        c.dumps = vec![0; sizes.len()];
    });
    let (images, between) = std::thread::scope(|scope| {
        for (i, &len) in sizes.iter().enumerate() {
            let (server, progress) = (&server, &progress);
            scope.spawn(move || {
                let _failing = Failing(progress);
                let mut c = server.conn();
                let key = key(len);
                let mut seen = 0;
                let batches = STEPS.div_ceil(8);
                for (b, steps) in (0..STEPS).collect::<Vec<_>>().chunks(8).enumerate() {
                    // A `DUMP` the last batch was answered before. Each quarter
                    // of the batches waits for a dump held before it.
                    let saved = b * (SAVES as usize + 1) / batches;
                    progress.wait(|c| (c.dumps[i] > seen && c.held >= saved).then_some(()));
                    progress.update(|c| c.sent[i] += 2 * steps.len());
                    for &step in steps {
                        let value = if step < ENDS { b"1" } else { b"0" };
                        for bit in [step % ENDS, len * 8 - ENDS + step % ENDS] {
                            let bit = bit.to_string();
                            c.send(&[b"SETBIT", key.as_bytes(), bit.as_bytes(), value]);
                        }
                    }
                    for &step in steps.iter().flat_map(|s| [s, s]) {
                        let old = i64::from(step >= ENDS);
                        assert_eq!(c.read(), V::Int(old), "{len}: step {step}");
                    }
                    progress.update(|c| c.answered[i] += 2 * steps.len());
                    seen = progress.wait(|c| Some(c.dumps[i]));
                }
                progress.update(|c| c.writers_done += 1);
            });
        }
        let readers: Vec<_> = (0..2 * READERS)
            .map(|r| {
                let (server, progress) = (&server, &progress);
                let dumps = r >= READERS;
                let how = if dumps { "DUMP" } else { "GETEX" };
                scope.spawn(move || {
                    let _failing = Failing(progress);
                    let mut c = server.conn();
                    let copy = format!("copy{r}");
                    let mut last = vec![0; sizes.len()];
                    let mut between = 0;
                    loop {
                        let done = progress.wait(|c| Some(c.writers_done == sizes.len()));
                        for (i, &len) in sizes.iter().enumerate() {
                            let key = key(len);
                            let bytes = if dumps {
                                progress.update(|c| c.dumps[i] += 1);
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
                                panic!("{how} {key}: {bytes:?}");
                            };
                            let at = version(&bytes, len);
                            let at = at.unwrap_or_else(|| panic!("{how} {key}: two moments"));
                            assert!(at >= last[i], "{how} {key}: {at} after {}", last[i]);
                            last[i] = at;
                            between += usize::from(0 < at && at < writes);
                        }
                        if done {
                            break;
                        }
                    }
                    // The last round began with every write answered.
                    for (&at, len) in last.iter().zip(sizes) {
                        assert_eq!(at, writes, "{how} {len}");
                    }
                    between
                })
            })
            .collect();
        let _failing = Failing(&progress);
        let mut c = server.conn();
        let mut images = Vec::new();
        for n in 1..=SAVES {
            let lo = progress.wait(|c| Some(c.answered.clone()));
            let reply = c.cmd(&[b"BGSAVE"]);
            assert_eq!(reply, V::Str(b"Background saving started".to_vec()));
            let held = server.snapshot_held(n);
            // Writers wait at the next quarter until this dump is held: writes remain.
            let hi = progress.wait(|c| Some(c.sent.clone()));
            assert!(hi.iter().all(|&h| h + GAP <= writes), "{hi:?}");
            progress.update(|c| c.held = n as usize);
            // Those writes are answered before the dump is written.
            progress.wait(|c| {
                let on = |(&a, &h): (&usize, &usize)| a >= h + GAP;
                c.answered.iter().zip(&hi).all(on).then_some(())
            });
            std::fs::remove_file(held).unwrap();
            c.wait_for("rdb_bgsave_in_progress", "0");
            assert_eq!(c.info("rdb_last_bgsave_status"), "ok");
            let image = std::fs::read(server.dir.join("dump.kdb")).unwrap();
            images.push((image, lo, hi));
        }
        let between: usize = readers.into_iter().map(|r| r.join().unwrap()).sum();
        (images, between)
    });
    assert!(
        between > 0,
        "no reader saw a value between its first and last write"
    );
    for (n, (image, lo, hi)) in images.into_iter().enumerate() {
        let dir = tempfile::tempdir().unwrap().keep();
        std::fs::write(dir.join("dump.kdb"), image).unwrap();
        let loaded = Server::start_in(dir, &[], false);
        let mut c = loaded.conn();
        for (i, &len) in sizes.iter().enumerate() {
            let V::Str(bytes) = c.get(&key(len)) else {
                panic!("dump {n}: no {len}");
            };
            let at = version(&bytes, len);
            assert!(
                at.is_some_and(|at| lo[i] <= at && at <= hi[i]),
                "dump {n}, {len} bytes: {at:?}, captured between {} and {}",
                lo[i],
                hi[i]
            );
        }
    }
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
