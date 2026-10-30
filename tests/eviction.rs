// tests/eviction.rs
//
// `maxmemory-policy`: what the server does when it reaches `maxmemory`. Real
// `kividb` processes, one per test: the limit is on what the process has
// allocated, and a server inside the test process would be sharing that
// with the tests.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

// ── RESP client ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum V {
    Str(String),
    Err(String),
    Int(i64),
    Arr(Vec<V>),
    Nil,
}

impl V {
    fn s(&self) -> &str {
        match self {
            V::Str(s) | V::Err(s) => s,
            other => panic!("not a string: {other:?}"),
        }
    }
    fn i(&self) -> i64 {
        match self {
            V::Int(n) => *n,
            other => panic!("not an integer: {other:?}"),
        }
    }
    fn ok(&self) -> bool {
        matches!(self, V::Str(s) if s == "OK")
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

    fn cmd(&mut self, args: &[&str]) -> V {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
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
                V::Str(String::from_utf8_lossy(&data).into_owned())
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

    /// `DUMP key`, as bytes: a payload is not text.
    fn dump(&mut self, key: &str) -> Vec<u8> {
        self.writer
            .write_all(format!("*2\r\n$4\r\nDUMP\r\n${}\r\n{key}\r\n", key.len()).as_bytes())
            .unwrap();
        let mut line = String::new();
        self.reader.read_line(&mut line).expect("reply");
        let len: usize = line.trim_end().strip_prefix('$').unwrap().parse().unwrap();
        let mut data = vec![0u8; len + 2];
        self.reader.read_exact(&mut data).unwrap();
        data.truncate(len);
        data
    }

    /// `RESTORE key 0 payload` and then `extra`.
    fn restore(&mut self, key: &str, payload: &[u8], extra: &[&str]) -> V {
        let mut buf = format!("*{}\r\n", 4 + extra.len()).into_bytes();
        for arg in [&b"RESTORE"[..], key.as_bytes(), b"0", payload]
            .into_iter()
            .chain(extra.iter().map(|a| a.as_bytes()))
        {
            buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            buf.extend_from_slice(arg);
            buf.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&buf).unwrap();
        self.read()
    }

    /// One field of `INFO <section>`.
    fn info(&mut self, section: &str, field: &str) -> String {
        let reply = self.cmd(&["INFO", section]);
        reply
            .s()
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{field}:")))
            .unwrap_or_else(|| panic!("no {field} in INFO {section}"))
            .trim()
            .to_string()
    }

    fn used_memory(&mut self) -> i64 {
        self.info("memory", "used_memory").parse().unwrap()
    }

    fn evicted(&mut self) -> i64 {
        self.info("stats", "evicted_keys").parse().unwrap()
    }

    /// Allow the server `room` bytes more than it is using now.
    fn limit_to(&mut self, room: i64) -> i64 {
        let limit = self.used_memory() + room;
        assert!(
            self.cmd(&["CONFIG", "SET", "maxmemory", &limit.to_string()])
                .ok()
        );
        limit
    }

    /// Take the limit away, to count what is there.
    ///
    /// Over the limit, room is made before any command runs — one that only
    /// reads too, as in Redis — and a server that has just evicted its way
    /// under the limit is within a reply's worth of memory of being over it
    /// again: with the limit in place, the counting would be of a dataset
    /// that the counting itself may take a key from.
    fn lift_the_limit(&mut self) {
        assert!(self.cmd(&["CONFIG", "SET", "maxmemory", "0"]).ok());
    }

    /// How many of `prefix0 .. prefix{count-1}` exist.
    fn surviving(&mut self, prefix: &str, count: usize) -> usize {
        (0..count)
            .filter(|i| self.cmd(&["EXISTS", &format!("{prefix}{i}")]).i() == 1)
            .count()
    }
}

// ── Server fixture ────────────────────────────────────────────────────────────

struct Server {
    port: u16,
    dir: PathBuf,
    child: Child,
}

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

impl Server {
    fn start(extra: &[&str]) -> Self {
        Self::start_in(tempfile::tempdir().unwrap().keep(), extra)
    }

    /// Start one in `dir`, on whatever is saved there.
    fn start_in(dir: PathBuf, extra: &[&str]) -> Self {
        let port = free_port();
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("server.log"))
            .unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_kividb"))
            .current_dir(&dir)
            .args([
                "--port",
                &port.to_string(),
                "--bind",
                "127.0.0.1",
                "--io-model",
                "tokio",
                "--threads",
                "4",
                "--configfile",
                "kividb.conf",
            ])
            .args(extra)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .expect("could not start kividb");
        let server = Server { port, dir, child };
        assert_eq!(server.conn().cmd(&["PING"]).s(), "PONG");
        server
    }

    fn conn(&self) -> Conn {
        Conn::connect(self.port)
    }
}

impl Server {
    /// Kill it — no final save — and leave what it wrote on disk.
    fn kill_keeping_its_files(mut self) -> PathBuf {
        let _ = self.child.kill();
        let _ = self.child.wait();
        std::mem::take(&mut self.dir)
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

const KB: i64 = 1024;
const MB: i64 = 1024 * KB;

fn value() -> String {
    "v".repeat(1000)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// The default. A write that would add to memory is refused; one that frees
/// it is not — a server that is full has to be able to be emptied.
#[test]
fn noeviction_refuses_what_adds_and_allows_what_frees() {
    let server = Server::start(&[]);
    let mut c = server.conn();
    assert_eq!(c.info("memory", "maxmemory_policy"), "noeviction");
    let limit = c.limit_to(4 * MB);

    let value = value();
    let mut written = 0;
    let refused = loop {
        let reply = c.cmd(&["SET", &format!("k{written}"), &value]);
        if reply.is_err() {
            break reply;
        }
        written += 1;
        assert!(written < 20_000, "20 MB were written under a 4 MB limit");
    };
    assert!(
        refused.s().starts_with("OOM command not allowed"),
        "{refused:?}"
    );
    // About 4 MB of 1 KB values got in, not a handful and not twice as many.
    assert!((2_000..6_000).contains(&written), "{written} keys");
    assert_eq!(c.evicted(), 0);
    assert_eq!(c.cmd(&["DBSIZE"]).i(), written);

    // Reading is unaffected, and so are the commands that make room.
    assert_eq!(c.cmd(&["GET", "k0"]).s(), value);
    assert_eq!(c.cmd(&["EXPIRE", "k1", "1000"]).i(), 1);
    for i in 0..written / 2 {
        assert_eq!(c.cmd(&["DEL", &format!("k{i}")]).i(), 1);
    }
    // With room again, writes are accepted — the memory freed was seen.
    assert!(c.used_memory() < limit);
    assert!(c.cmd(&["SET", "again", &value]).ok());
}

/// The least recently used keys go, to keep the server under its limit, and
/// no write is refused.
#[test]
fn allkeys_lru_makes_room_by_deleting_what_was_used_longest_ago() {
    let server = Server::start(&["--maxmemory-policy", "allkeys-lru"]);
    let mut c = server.conn();
    assert_eq!(c.info("memory", "maxmemory_policy"), "allkeys-lru");
    assert_eq!(
        c.cmd(&["CONFIG", "GET", "maxmemory-policy"]),
        V::Arr(vec![
            V::Str("maxmemory-policy".into()),
            V::Str("allkeys-lru".into())
        ])
    );
    // Room for about 5,300 of the 6,200 keys written below, at about 1,080
    // bytes a key (a key and its 1,000-byte value are one block of 1,024):
    // some 900 go. At 6 MB too few went to tell; at 5 MB, so many of the
    // old keys went that a sample with none of them in it was common, and
    // a hot key went instead about one run in three.
    let limit = c.limit_to(11 * MB / 2);
    let value = value();

    for i in 0..3000 {
        assert!(c.cmd(&["SET", &format!("old{i}"), &value]).ok());
    }
    // The clock keys are aged by has a resolution of a second.
    std::thread::sleep(Duration::from_millis(2100));
    for i in 0..200 {
        assert!(c.cmd(&["SET", &format!("hot{i}"), &value]).ok());
    }
    assert_eq!(c.evicted(), 0);

    // More than fits. The hot keys go on being read all the while.
    for i in 0..3000 {
        let reply = c.cmd(&["SET", &format!("new{i}"), &value]);
        assert!(reply.ok(), "write {i} was refused: {reply:?}");
        c.cmd(&["GET", &format!("hot{}", i % 200)]);
    }

    let used = c.used_memory();
    assert!(used <= limit + MB, "{used} in use, the limit is {limit}");
    c.lift_the_limit();
    let evicted = c.evicted();
    assert!(evicted > 500, "only {evicted} keys were evicted");

    let (old, hot, new) = (
        c.surviving("old", 3000),
        c.surviving("hot", 200),
        c.surviving("new", 3000),
    );
    // What went was what had not been touched: the choice is from a sample,
    // so not to the last key, but nearly.
    assert!(old < 2600, "{old} old keys are left");
    assert!(hot >= 190, "only {hot} of 200 hot keys are left");
    assert!(new >= 2850, "only {new} of 3000 new keys are left");
    assert_eq!(c.cmd(&["DBSIZE"]).i() as usize, old + hot + new);

    // How long since a key was used can be asked; how often cannot, here.
    // (Of a key that is there: up to ten of the hot keys may have gone, and
    // `hot0` was now and then one of them. The limit is lifted, so writing
    // it again evicts nothing.)
    assert!(c.cmd(&["SET", "hot0", &value]).ok());
    c.cmd(&["GET", "hot0"]);
    assert!(c.cmd(&["OBJECT", "IDLETIME", "hot0"]).i() <= 2);
    assert!(
        c.cmd(&["OBJECT", "FREQ", "hot0"])
            .s()
            .starts_with("ERR An LFU maxmemory policy is not selected")
    );
}

/// A `volatile-` policy touches only keys that have a TTL; `volatile-ttl`
/// takes the ones nearest to expiring first. When there are none left, the
/// write is refused.
#[test]
fn volatile_ttl_deletes_only_keys_with_a_ttl_the_nearest_to_expiring_first() {
    let server = Server::start(&[]);
    let mut c = server.conn();
    assert!(
        c.cmd(&["CONFIG", "SET", "maxmemory-policy", "volatile-ttl"])
            .ok()
    );
    assert!(
        c.cmd(&["CONFIG", "SET", "maxmemory-policy", "volatile-mru"])
            .is_err()
    );
    assert_eq!(c.info("memory", "maxmemory_policy"), "volatile-ttl");
    c.limit_to(5 * MB);
    let value = value();

    for i in 0..500 {
        assert!(
            c.cmd(&["SET", &format!("soon{i}"), &value, "EX", "1000"])
                .ok()
        );
        assert!(
            c.cmd(&["SET", &format!("later{i}"), &value, "EX", "1000000"])
                .ok()
        );
        assert!(c.cmd(&["SET", &format!("keep{i}"), &value]).ok());
    }

    // Keys without a TTL, until about half of those with one have had to go.
    let mut more = 0;
    while c.evicted() < 500 {
        for _ in 0..100 {
            assert!(c.cmd(&["SET", &format!("more{more}"), &value]).ok());
            more += 1;
        }
        assert!(more < 20_000, "nothing is being evicted");
    }
    let (soon, later) = (c.surviving("soon", 500), c.surviving("later", 500));
    assert!(
        soon + 150 < later,
        "{soon} keys expiring soon are left and {later} expiring later"
    );
    assert_eq!(c.surviving("keep", 500), 500);

    // And on, until there is nothing with a TTL left to delete.
    let refused = loop {
        let reply = c.cmd(&["SET", &format!("more{more}"), &value]);
        if reply.is_err() {
            break reply;
        }
        more += 1;
        assert!(more < 20_000, "20 MB were written under a 5 MB limit");
    };
    assert!(
        refused.s().starts_with("OOM command not allowed"),
        "{refused:?}"
    );
    assert_eq!(c.evicted(), 1000);
    assert_eq!(c.surviving("soon", 500) + c.surviving("later", 500), 0);
    assert_eq!(c.surviving("keep", 500), 500);
    assert_eq!(c.cmd(&["DBSIZE"]).i(), 500 + more as i64);
}

/// With an LFU policy the keys used least often go, and how often a key has
/// been used can be asked.
#[test]
fn allkeys_lfu_keeps_what_is_used_often() {
    let server = Server::start(&["--maxmemory-policy", "allkeys-lfu"]);
    let mut c = server.conn();
    let limit = c.limit_to(5 * MB);
    let value = value();

    for i in 0..100 {
        assert!(c.cmd(&["SET", &format!("busy{i}"), &value]).ok());
    }
    // Used often: the count is logarithmic, so often means often.
    for _ in 0..300 {
        for i in 0..100 {
            c.cmd(&["GET", &format!("busy{i}")]);
        }
    }
    assert!(c.cmd(&["SET", "idle", &value]).ok());
    let (busy, idle) = (
        c.cmd(&["OBJECT", "FREQ", "busy0"]).i(),
        c.cmd(&["OBJECT", "FREQ", "idle"]).i(),
    );
    assert!(busy > idle && idle >= 5, "busy {busy}, idle {idle}");
    assert!(
        c.cmd(&["OBJECT", "IDLETIME", "busy0"])
            .s()
            .starts_with("ERR An LFU maxmemory policy is selected")
    );
    assert_eq!(c.cmd(&["OBJECT", "FREQ", "nothere"]), V::Nil);

    for i in 0..8000 {
        let reply = c.cmd(&["SET", &format!("once{i}"), &value]);
        assert!(reply.ok(), "write {i} was refused: {reply:?}");
    }
    assert!(c.used_memory() <= limit + MB);
    c.lift_the_limit();
    assert!(c.evicted() > 2000);
    let busy = c.surviving("busy", 100);
    assert!(busy >= 95, "only {busy} of 100 busy keys are left");
}

/// What a master evicts, its replicas delete: an eviction is sent to them
/// like any other deletion.
#[test]
fn evictions_reach_replicas() {
    let master = Server::start(&["--maxmemory-policy", "allkeys-random"]);
    let replica = Server::start(&[]);
    let (mut m, mut r) = (master.conn(), replica.conn());
    assert!(
        r.cmd(&["REPLICAOF", "127.0.0.1", &master.port.to_string()])
            .ok()
    );
    let deadline = Instant::now() + Duration::from_secs(20);
    while r.info("replication", "master_link_status") != "up" {
        assert!(Instant::now() < deadline, "the replica did not connect");
        std::thread::sleep(Duration::from_millis(50));
    }

    m.limit_to(4 * MB);
    let value = value();
    for i in 0..8000 {
        let reply = m.cmd(&["SET", &format!("k{i}"), &value]);
        assert!(reply.ok(), "write {i} was refused: {reply:?}");
    }
    m.lift_the_limit();
    let evicted = m.evicted();
    assert!(evicted > 2000, "only {evicted} keys were evicted");
    assert_eq!(m.cmd(&["WAIT", "1", "10000"]).i(), 1);

    // The replica has no limit of its own, and evicted nothing itself.
    assert_eq!(r.evicted(), 0);
    let keys = m.cmd(&["DBSIZE"]).i();
    assert_eq!(keys, 8000 - evicted);
    assert_eq!(r.cmd(&["DBSIZE"]).i(), keys);
    for i in (0..8000).step_by(97) {
        let key = format!("k{i}");
        assert_eq!(
            m.cmd(&["EXISTS", &key]),
            r.cmd(&["EXISTS", &key]),
            "{key} is on one and not the other"
        );
    }
}

/// A policy the server does not know is not a server that starts.
#[test]
fn an_unknown_policy_is_refused_at_startup() {
    let dir = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_kividb"))
        .current_dir(dir.path())
        .args([
            "--port",
            &free_port().to_string(),
            "--maxmemory-policy",
            "allkeys-mru",
        ])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(!output.status.success());
    let said = String::from_utf8_lossy(&output.stderr).to_string()
        + &String::from_utf8_lossy(&output.stdout);
    assert!(said.contains("maxmemory-policy"), "{said}");
}

/// The limit is on what clients may add, not on what the server already
/// holds: a dataset larger than `maxmemory` is loaded whole at startup — the
/// append-only file replayed without a write of it being refused or a key
/// evicted to make room for the next.
#[test]
fn what_is_on_disk_is_loaded_whatever_the_limit() {
    let value = value();
    let keys = 4000;
    // A limit that an empty server is well under and this dataset — some
    // 5 MB — is well over.
    let (dir, limit) = {
        let server = Server::start(&["--aof"]);
        let mut c = server.conn();
        let limit = c.used_memory() + 2 * MB;
        for i in 0..keys {
            assert!(c.cmd(&["SET", &format!("k{i}"), &value]).ok());
        }
        assert!(c.used_memory() > limit + MB);
        // Killed, not shut down: the next start has the log to replay, not
        // a snapshot to load. The log is synced every second.
        std::thread::sleep(Duration::from_millis(2500));
        (server.kill_keeping_its_files(), limit.to_string())
    };

    for policy in ["noeviction", "allkeys-random"] {
        let server = Server::start_in(
            dir.clone(),
            &["--aof", "--maxmemory", &limit, "--maxmemory-policy", policy],
        );
        let mut c = server.conn();
        if policy == "noeviction" {
            assert_eq!(c.cmd(&["DBSIZE"]).i(), keys);
            assert_eq!(c.evicted(), 0);
            // The limit is there for what comes next.
            let reply = c.cmd(&["SET", "one-more", &value]);
            assert!(reply.s().starts_with("OOM"), "{reply:?}");
            // For the next round: what is on disk is still everything.
            let _ = server.kill_keeping_its_files();
        } else {
            // Nothing was evicted while loading: every key was there when
            // the first client arrived. The limit applies from that client's
            // first command — whatever the command, as in Redis — so what is
            // counted is what is left and what that eviction removed, and
            // together they are everything.
            let before = c.evicted();
            let left = c.cmd(&["DBSIZE"]).i();
            let after = c.evicted();
            assert!(
                before <= keys - left && keys - left <= after,
                "{left} of {keys} keys are left, {before} to {after} were evicted"
            );
            assert!(after > 500 && after < keys, "{after} evicted");
            let reply = c.cmd(&["SET", "one-more", &value]);
            assert!(reply.ok(), "{reply:?}");
            c.lift_the_limit();
            assert_eq!(c.cmd(&["DBSIZE"]).i(), keys + 1 - c.evicted());
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// At its limit a server frees a block for every block it takes, and what
/// the slabs hold beyond the blocks in them is memory the process holds and
/// cannot use: it stays small (#202). Used again only once a quarter of
/// them was free, the slabs settled about 14% (1 KB values) and 25%
/// (100-byte values) above what was in them.
#[test]
fn at_the_limit_the_slabs_stay_full() {
    // (value size, room under the limit, keys written): about four times
    // what fits.
    for (size, room, keys) in [(1000, 16 * MB, 64_000), (100, 4 * MB, 128_000)] {
        let server = Server::start(&["--maxmemory-policy", "allkeys-random"]);
        let mut c = server.conn();
        c.limit_to(room);
        let value = "v".repeat(size);
        // Pipelined, a thousand at a time; the keys evicted are spread over
        // the slabs at random.
        for batch in 0..keys / 1000 {
            let mut buf = Vec::new();
            for i in 0..1000 {
                let key = format!("k{}", batch * 1000 + i);
                buf.extend_from_slice(
                    format!(
                        "*3\r\n$3\r\nSET\r\n${}\r\n{key}\r\n${size}\r\n{value}\r\n",
                        key.len()
                    )
                    .as_bytes(),
                );
            }
            c.writer.write_all(&buf).unwrap();
            for _ in 0..1000 {
                let reply = c.read();
                assert!(reply.ok(), "{reply:?}");
            }
        }
        let evicted = c.evicted();
        assert!(evicted > keys as i64 / 2, "{evicted} evicted of {keys}");
        let slabs: i64 = c.info("memory", "value_slab_bytes").parse().unwrap();
        let live: i64 = c.info("memory", "value_slab_live_bytes").parse().unwrap();
        assert!(
            slabs - live <= live / 25 + 256 * KB,
            "values of {size} bytes: {slabs} bytes of slabs for {live} bytes in them"
        );
    }
}

/// `RESTORE ... IDLETIME` and `FREQ` give the restored key the standing it
/// had where it was dumped (#21). They were read and thrown away.
#[test]
fn restore_keeps_a_keys_idle_time_or_frequency() {
    let server = Server::start(&["--maxmemory-policy", "allkeys-lru"]);
    let mut c = server.conn();
    assert!(c.cmd(&["SET", "foo", "bar"]).ok());
    let payload = c.dump("foo");
    c.cmd(&["DEL", "foo"]);
    // Older than the server: a key can have been idle for longer than this
    // one has been up.
    assert!(c.restore("foo", &payload, &["IDLETIME", "1000"]).ok());
    let idle = c.cmd(&["OBJECT", "IDLETIME", "foo"]).i();
    assert!((1000..=1010).contains(&idle), "idle {idle}");
    assert_eq!(c.cmd(&["GET", "foo"]).s(), "bar");
    assert!(
        c.cmd(&["OBJECT", "IDLETIME", "foo"]).i() < 2,
        "a read uses it"
    );

    assert!(
        c.cmd(&["CONFIG", "SET", "maxmemory-policy", "allkeys-lfu"])
            .ok()
    );
    c.cmd(&["DEL", "foo"]);
    assert!(c.restore("foo", &payload, &["FREQ", "100"]).ok());
    let freq = c.cmd(&["OBJECT", "FREQ", "foo"]).i();
    // 99 if a minute turned between the two: the count decays by one a minute.
    assert!((99..=100).contains(&freq), "freq {freq}");

    for (extra, error) in [
        (
            &["IDLETIME", "-1"][..],
            "ERR Invalid IDLETIME value, must be >= 0",
        ),
        (
            &["FREQ", "256"],
            "ERR Invalid FREQ value, must be >= 0 and <= 255",
        ),
        (
            &["IDLETIME", "1", "FREQ", "1"],
            "ERR IDLETIME and FREQ cannot be set at the same time.",
        ),
        (&["IDLETIME"], "ERR syntax error"),
    ] {
        let mut args = vec!["REPLACE"];
        args.extend_from_slice(extra);
        assert_eq!(
            c.restore("foo", &payload, &args),
            V::Err(error.into()),
            "{extra:?}"
        );
    }
}

/// With `key-access-tracking yes`, a key's idle time is kept under
/// `noeviction` too, and only a use of the key resets it: not `TTL`, `TYPE`,
/// `EXISTS` or `OBJECT` (#21). Off, the default, it is not kept: 0.
#[test]
fn key_access_tracking_keeps_idle_time_under_noeviction() {
    let server = Server::start(&[]);
    let mut c = server.conn();
    let get = |c: &mut Conn, name: &str| match c.cmd(&["CONFIG", "GET", name]) {
        V::Arr(row) => row[1].s().to_string(),
        other => panic!("{other:?}"),
    };
    assert_eq!(get(&mut c, "maxmemory-policy"), "noeviction");
    assert_eq!(get(&mut c, "key-access-tracking"), "no");
    assert!(c.cmd(&["SET", "untracked", "bar"]).ok());
    std::thread::sleep(Duration::from_millis(1100));
    assert_eq!(c.cmd(&["OBJECT", "IDLETIME", "untracked"]).i(), 0);
    assert!(
        c.cmd(&["CONFIG", "SET", "key-access-tracking", "maybe"])
            .is_err()
    );

    assert!(c.cmd(&["CONFIG", "SET", "key-access-tracking", "yes"]).ok());
    assert_eq!(get(&mut c, "key-access-tracking"), "yes");
    assert!(c.cmd(&["SET", "foo", "bar"]).ok());
    assert!(c.cmd(&["SET", "other", "bar"]).ok());
    std::thread::sleep(Duration::from_millis(2100));
    for command in ["TTL", "PTTL", "TYPE", "EXISTS", "EXPIRETIME"] {
        assert!(!c.cmd(&[command, "foo"]).is_err(), "{command}");
    }
    c.cmd(&["OBJECT", "ENCODING", "foo"]);
    let idle = c.cmd(&["OBJECT", "IDLETIME", "foo"]).i();
    assert!(idle >= 2, "idle {idle} after TTL, TYPE, EXISTS and OBJECT");

    assert_eq!(c.cmd(&["TOUCH", "foo"]).i(), 1);
    assert!(
        c.cmd(&["OBJECT", "IDLETIME", "foo"]).i() < 2,
        "TOUCH uses it"
    );
    c.cmd(&["GET", "other"]);
    assert!(
        c.cmd(&["OBJECT", "IDLETIME", "other"]).i() < 2,
        "GET uses it"
    );
}

/// `CONFIG SET maxmemory-policy` between the LRU and LFU kinds at runtime:
/// what a key's word held for the other kind counts as nothing known (a
/// count of 5, an idle time as long as the server has run) until the key
/// is used again. Under an LRM policy only a write is a use.
#[test]
fn switching_between_lru_and_lfu_at_runtime_starts_each_key_afresh() {
    let server = Server::start(&["--maxmemory-policy", "allkeys-lru"]);
    let mut c = server.conn();
    let set_policy = |c: &mut Conn, policy: &str| {
        assert!(
            c.cmd(&["CONFIG", "SET", "maxmemory-policy", policy]).ok(),
            "{policy}"
        );
    };
    assert!(c.cmd(&["SET", "foo", "bar"]).ok());
    std::thread::sleep(Duration::from_millis(2100));
    assert!(c.cmd(&["OBJECT", "IDLETIME", "foo"]).i() >= 2);

    set_policy(&mut c, "allkeys-lfu");
    assert_eq!(
        c.cmd(&["OBJECT", "FREQ", "foo"]).i(),
        5,
        "a clock is no count"
    );
    for _ in 0..3 {
        c.cmd(&["GET", "foo"]);
    }
    let freq = c.cmd(&["OBJECT", "FREQ", "foo"]).i();
    assert!((6..=8).contains(&freq), "freq {freq} after three uses");
    // OBJECT, TYPE and EXISTS are not uses.
    c.cmd(&["TYPE", "foo"]);
    c.cmd(&["EXISTS", "foo"]);
    assert_eq!(c.cmd(&["OBJECT", "FREQ", "foo"]).i(), freq);
    // A value written over keeps its count, as in Redis.
    assert!(c.cmd(&["SET", "foo", "baz"]).ok());
    assert!(c.cmd(&["OBJECT", "FREQ", "foo"]).i() >= freq);

    set_policy(&mut c, "volatile-lru");
    let idle = c.cmd(&["OBJECT", "IDLETIME", "foo"]).i();
    assert!(idle >= 2, "a count is no clock: idle {idle}");
    c.cmd(&["GET", "foo"]);
    assert!(c.cmd(&["OBJECT", "IDLETIME", "foo"]).i() < 2);

    set_policy(&mut c, "allkeys-lrm");
    assert!(c.cmd(&["SET", "bar", "1"]).ok());
    std::thread::sleep(Duration::from_millis(2100));
    c.cmd(&["GET", "bar"]);
    assert!(
        c.cmd(&["OBJECT", "IDLETIME", "bar"]).i() >= 2,
        "a read is no modification"
    );
    assert_eq!(c.cmd(&["INCR", "bar"]).i(), 2);
    assert!(c.cmd(&["OBJECT", "IDLETIME", "bar"]).i() < 2);
}
