#![allow(clippy::disallowed_methods)]
//! Room under `maxmemory` for the commands of the probabilistic types
//! (`sketch::room`), across the five types: what a command is asked before
//! it runs changes nothing, is asked only of a command nothing else
//! refuses, and is enough for it to succeed or do nothing; and a master and
//! its replica end alike whatever the commands met.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::{Duration, Instant};

mod common;
use common::{start_verified, start_verified_cluster};

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
                        .set_read_timeout(Some(Duration::from_secs(60)))
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

    fn raw(&mut self, args: &[&[u8]]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            out.extend_from_slice(a);
            out.extend_from_slice(b"\r\n");
        }
        self.stream.write_all(&out).unwrap();
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

    fn cmd(&mut self, args: &[&str]) -> Vec<u8> {
        let args: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
        self.raw(&args)
    }

    fn ok(&mut self, args: &[&str]) {
        let reply = self.cmd(args);
        assert_eq!(reply, b"+OK\r\n", "{args:?}: {}", shown(&reply));
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

/// The bulk strings of an array reply.
fn bulks(reply: &[u8]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut at = reply.iter().position(|&b| b == b'\n').unwrap() + 1;
    while at < reply.len() {
        let end = reply_end(reply, at).unwrap();
        let item = &reply[at..end];
        if item[0] == b'$' {
            let nl = item.iter().position(|&b| b == b'\n').unwrap();
            out.push(item[nl + 1..item.len() - 2].to_vec());
        }
        at = end;
    }
    out
}

fn wait_until(what: &str, mut pred: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if pred() {
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("timed out waiting for {what}");
}

fn tmp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("kividb-room-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A `kividb` process in a directory of its own, killed when dropped.
struct Child {
    port: u16,
    child: std::process::Child,
    dir: std::path::PathBuf,
    args: Vec<String>,
    env: Vec<(String, String)>,
}

impl Child {
    fn start(tag: &str, extra: &[&str]) -> Child {
        Child::start_env(tag, extra, &[])
    }

    fn start_env(tag: &str, extra: &[&str], env: &[(&str, &str)]) -> Child {
        let args = extra.iter().map(|a| a.to_string()).collect();
        let env = env
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Child::spawn_in(tmp_dir(tag), args, env)
    }

    fn spawn_in(dir: std::path::PathBuf, args: Vec<String>, env: Vec<(String, String)>) -> Child {
        let spawn = |port: u16| {
            std::process::Command::new(env!("CARGO_BIN_EXE_kividb"))
                .current_dir(&dir)
                .envs(env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
                .args(["--port", &port.to_string(), "--bind", "127.0.0.1"])
                .args(["--threads", "2", "--loglevel", "warning"])
                .args(["--enable-debug-command", "yes"])
                .args(["--configfile", "kividb.conf", "--dir"])
                .arg(&dir)
                .args(&args)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .spawn()
                .expect("could not start kividb")
        };
        // A cluster node needs its bus port too (see `start_verified_cluster`).
        let (child, port) = if args.iter().any(|a| a == "--cluster-enabled") {
            start_verified_cluster(spawn)
        } else {
            start_verified(spawn)
        };
        let child = Child {
            port,
            child,
            dir,
            args,
            env,
        };
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

    /// Killed and started again in its directory, as it was started.
    fn restart(mut self) -> Child {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let dir = std::mem::take(&mut self.dir);
        let args = std::mem::take(&mut self.args);
        let env = std::mem::take(&mut self.env);
        std::mem::forget(self);
        Child::spawn_in(dir, args, env)
    }

    /// A replica of `master`, in sync.
    fn replica_of(tag: &str, master: &Child) -> Child {
        let replica = Child::start(tag, &["--replicaof", "127.0.0.1", &master.port.to_string()]);
        let mut r = replica.conn();
        wait_until("the replica to sync", || {
            info_field(&mut r, "replication", "master_link_status") == "up"
        });
        replica
    }

    fn conn(&self) -> Conn {
        Conn::connect(self.port)
    }
}

impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if !self.dir.as_os_str().is_empty() {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

/// A field of `INFO section`.
fn info_field(c: &mut Conn, section: &str, field: &str) -> String {
    let prefix = format!("{field}:");
    String::from_utf8_lossy(&c.cmd(&["INFO", section]))
        .lines()
        .find_map(|l| l.strip_prefix(&prefix).map(|v| v.trim().to_string()))
        .unwrap_or_default()
}

fn used_memory(c: &mut Conn) -> u64 {
    info_field(c, "memory", "used_memory").parse().unwrap()
}

fn evicted(c: &mut Conn) -> u64 {
    info_field(c, "stats", "evicted_keys").parse().unwrap()
}

/// Every key of the selected database and its `DEBUG DIGEST-VALUE`,
/// sorted: the dataset, to compare.
fn dataset(c: &mut Conn) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut keys = bulks(&c.cmd(&["KEYS", "*"]));
    keys.sort();
    keys.into_iter()
        .map(|k| {
            let d = c.raw(&[b"DEBUG", b"DIGEST-VALUE", &k]);
            assert!(!d.starts_with(b"-"), "{}", shown(&d));
            (k, d)
        })
        .collect()
}

/// `master` and `replica` hold the same dataset, once the replica has had
/// everything the master sent.
fn alike(master: &Child, replica: &Child, what: &str) {
    let (mut m, mut r) = (master.conn(), replica.conn());
    let mark = format!("mark:{what}");
    m.ok(&["SET", &mark, "1"]);
    wait_until("the replica to catch up", || {
        r.cmd(&["GET", &mark]) == b"$1\r\n1\r\n"
    });
    let on_master = dataset(&mut m);
    assert!(on_master.len() > 1, "{what}: a dataset to compare");
    assert_eq!(dataset(&mut r), on_master, "{what}: the replica differs");
}

const OOM: &[u8] = b"-OOM command not allowed when used memory > 'maxmemory'.\r\n";

// ── Asking changes nothing ───────────────────────────────────────────────

/// Every command that is asked what it allocates, on every path its
/// handler has, run where that is all that happens to it: over the limit
/// with `noeviction`, it is asked, finds no room, and is refused. The
/// dataset is as it was after each (a handler that wrote while only asked,
/// as `CF.LOADCHUNK` of a chunk did, changed it outside the write gate and
/// sent nothing to replicas). The values are shared with copies (`COPY`),
/// so that every write to them has a copy to allocate first.
#[test]
fn asking_what_a_command_allocates_changes_nothing() {
    let server = Child::start("asking", &["--maxmemory-policy", "noeviction"]);
    let mut c = server.conn();
    c.ok(&[
        "CF.RESERVE",
        "cf",
        "4096",
        "BUCKETSIZE",
        "2",
        "EXPANSION",
        "2",
    ]);
    for i in 0..9000 {
        c.cmd(&["CF.ADD", "cf", &format!("i{i}")]);
    }
    c.ok(&["CF.RESERVE", "cf1", "4096"]);
    c.cmd(&["CF.ADD", "cf1", "one"]);
    c.ok(&["BF.RESERVE", "bf", "0.01", "100000"]);
    c.cmd(&["BF.ADD", "bf", "one"]);
    c.ok(&["CMS.INITBYDIM", "cms", "100000", "5"]);
    c.ok(&["CMS.INITBYDIM", "cms2", "100000", "5"]);
    c.ok(&["TOPK.RESERVE", "topk", "50", "20000", "7", "0.9"]);
    c.ok(&["TDIGEST.CREATE", "td"]);
    c.ok(&["TDIGEST.ADD", "td", "1", "2", "3"]);
    for key in ["cf", "cf1", "bf", "cms", "topk", "td"] {
        assert_eq!(
            c.cmd(&["COPY", key, &format!("{key}:copy")]),
            b":1\r\n",
            "{key}"
        );
    }
    // A header and a chunk of each kind of filter, to load; a payload to
    // restore.
    let (_, cf_header) = scandump(&mut c, "CF.SCANDUMP", "cf1", "0");
    let (cf_next, cf_chunk) = scandump(&mut c, "CF.SCANDUMP", "cf1", "1");
    let (bf_first, bf_header) = scandump(&mut c, "BF.SCANDUMP", "bf", "0");
    let (bf_next, bf_chunk) = scandump(&mut c, "BF.SCANDUMP", "bf", &bf_first);
    let payload = bulk(&c.cmd(&["DUMP", "cms"]));
    let zeros = vec![0u8; cf_chunk.len()];
    let before = dataset(&mut c);
    c.ok(&["CONFIG", "SET", "maxmemory", "1"]);
    let mut commands: Vec<Vec<Vec<u8>>> = Vec::new();
    let mut add = |args: &[&[u8]]| commands.push(args.iter().map(|a| a.to_vec()).collect());
    add(&[b"BF.ADD", b"bf", b"two"]);
    add(&[b"BF.ADD", b"bf:new", b"two"]);
    add(&[b"BF.MADD", b"bf", b"a", b"b"]);
    add(&[b"BF.INSERT", b"bf", b"ITEMS", b"a", b"b"]);
    add(&[
        b"BF.INSERT",
        b"bf:new",
        b"CAPACITY",
        b"100000",
        b"ITEMS",
        b"a",
    ]);
    add(&[b"BF.RESERVE", b"bf:new", b"0.01", b"1000000"]);
    add(&[b"BF.LOADCHUNK", b"bf:new", bf_first.as_bytes(), &bf_header]);
    add(&[b"BF.LOADCHUNK", b"bf", bf_next.as_bytes(), &bf_chunk]);
    add(&[b"CMS.INITBYDIM", b"cms:new", b"100000", b"5"]);
    add(&[b"CMS.INITBYPROB", b"cms:new", b"0.0001", b"0.001"]);
    add(&[b"CMS.INCRBY", b"cms", b"a", b"1", b"b", b"2"]);
    add(&[b"CMS.MERGE", b"cms", b"1", b"cms2"]);
    add(&[
        b"CMS.MERGE",
        b"cms2",
        b"2",
        b"cms",
        b"cms2",
        b"WEIGHTS",
        b"1",
        b"2",
    ]);
    add(&[b"TOPK.RESERVE", b"topk:new", b"50", b"20000", b"7", b"0.9"]);
    add(&[b"TOPK.ADD", b"topk", b"a", b"b"]);
    add(&[b"TOPK.INCRBY", b"topk", b"a", b"3"]);
    add(&[b"CF.RESERVE", b"cf:new", b"100000"]);
    add(&[b"CF.ADD", b"cf", b"new"]);
    add(&[b"CF.ADD", b"cf:new", b"new"]);
    add(&[b"CF.ADDNX", b"cf", b"new"]);
    add(&[b"CF.ADDNX", b"cf:new", b"new"]);
    add(&[b"CF.INSERT", b"cf", b"ITEMS", b"a", b"b"]);
    add(&[
        b"CF.INSERT",
        b"cf:new",
        b"CAPACITY",
        b"100000",
        b"ITEMS",
        b"a",
    ]);
    add(&[b"CF.INSERTNX", b"cf", b"ITEMS", b"a", b"b"]);
    add(&[b"CF.INSERTNX", b"cf:new", b"ITEMS", b"a"]);
    add(&[b"CF.LOADCHUNK", b"cf:new", b"1", &cf_header]);
    // Zeros, not the bytes there: a write while asked would show.
    add(&[b"CF.LOADCHUNK", b"cf1", cf_next.as_bytes(), &zeros]);
    add(&[b"CF.DEL", b"cf", b"i1"]);
    add(&[b"CF.DEL", b"cf1", b"one"]);
    add(&[b"CF.COMPACT", b"cf"]);
    add(&[b"TDIGEST.CREATE", b"td:new"]);
    add(&[b"TDIGEST.MERGE", b"td:new", b"1", b"td"]);
    add(&[b"TDIGEST.MERGE", b"td", b"1", b"td:copy"]);
    add(&[b"RESTORE", b"cms:restored", b"0", &payload]);
    for args in &commands {
        let refs: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();
        let reply = c.raw(&refs);
        let name = String::from_utf8_lossy(&args[0]).to_string();
        assert!(reply.starts_with(b"-"), "{name}: {}", shown(&reply));
        assert_ne!(
            &reply[..reply.len().min(14)],
            b"-ERR internal ",
            "{name}: {}",
            shown(&reply)
        );
        c.ok(&["CONFIG", "SET", "maxmemory", "0"]);
        assert_eq!(dataset(&mut c), before, "{name} changed the dataset");
        c.ok(&["CONFIG", "SET", "maxmemory", "1"]);
    }
    c.ok(&["CONFIG", "SET", "maxmemory", "0"]);
    // Without the limit, the same command runs: it was refused only for
    // room.
    let refs: Vec<&[u8]> = vec![b"CF.LOADCHUNK", b"cf1", cf_next.as_bytes(), &zeros];
    assert_eq!(c.raw(&refs), b"+OK\r\n");
    assert_eq!(c.cmd(&["CF.EXISTS", "cf1", "one"]), b":0\r\n");
    assert_eq!(c.cmd(&["CF.EXISTS", "cf1:copy", "one"]), b":1\r\n");
}

/// The iterator and the bytes a `*.SCANDUMP key iter` gives.
fn scandump(c: &mut Conn, command: &str, key: &str, iter: &str) -> (String, Vec<u8>) {
    let reply = c.cmd(&[command, key, iter]);
    let text = String::from_utf8_lossy(&reply).to_string();
    let next = text
        .lines()
        .nth(1)
        .unwrap()
        .trim_start_matches(':')
        .to_string();
    (next, bulks(&reply)[0].clone())
}

/// The bulk string a reply is.
fn bulk(reply: &[u8]) -> Vec<u8> {
    assert_eq!(reply[0], b'$', "{}", shown(reply));
    let nl = reply.iter().position(|&b| b == b'\n').unwrap();
    reply[nl + 1..reply.len() - 2].to_vec()
}

/// A command refused before it runs is not asked what it allocates: with
/// `min-replicas-to-write` and no replica, `CF.LOADCHUNK` of a chunk is
/// refused `NOREPLICAS`, and the filter is as it was (asked first, it was
/// written to then).
#[test]
fn a_command_refused_before_it_runs_is_not_asked() {
    let server = Child::start("noreplicas", &["--maxmemory-policy", "allkeys-lru"]);
    let mut c = server.conn();
    c.ok(&["CF.RESERVE", "cf", "1024"]);
    c.cmd(&["CF.ADD", "cf", "one"]);
    let chunk = bulks(&c.cmd(&["CF.SCANDUMP", "cf", "1"]))[0].clone();
    let next = (1 + chunk.len()).to_string();
    let zeros = vec![0u8; chunk.len()];
    let before = dataset(&mut c);
    c.ok(&["CONFIG", "SET", "maxmemory", "1gb"]);
    c.ok(&["CONFIG", "SET", "min-replicas-to-write", "1"]);
    let reply = c.raw(&[b"CF.LOADCHUNK", b"cf", next.as_bytes(), &zeros]);
    assert!(reply.starts_with(b"-NOREPLICAS"), "{}", shown(&reply));
    let reply = c.cmd(&["CF.RESERVE", "cf:new", "100000"]);
    assert!(reply.starts_with(b"-NOREPLICAS"), "{}", shown(&reply));
    c.ok(&["CONFIG", "SET", "min-replicas-to-write", "0"]);
    c.ok(&["CONFIG", "SET", "maxmemory", "0"]);
    assert_eq!(dataset(&mut c), before);
    assert_eq!(c.cmd(&["CF.EXISTS", "cf", "one"]), b":1\r\n");
}

/// Fill `c` with values of `size` bytes until memory use is at `limit`, and
/// keys have been evicted for them (under an evicting policy).
fn fill_to_the_limit(c: &mut Conn, prefix: &str, size: usize, ttl: bool) {
    let value = "v".repeat(size);
    let mut i = 0;
    let start = evicted(c);
    while evicted(c) == start {
        for _ in 0..50 {
            let key = format!("{prefix}{i}");
            // Refused for a while if evicting gives nothing back (#284).
            let reply = if ttl {
                c.cmd(&["SET", &key, &value, "EX", "3600"])
            } else {
                c.cmd(&["SET", &key, &value])
            };
            assert!(reply == b"+OK\r\n" || reply == OOM, "{}", shown(&reply));
            i += 1;
        }
        assert!(i < 200_000, "never reached the limit");
    }
}

/// In a cluster, a command for a slot another node serves is answered
/// `MOVED` before it is asked what it allocates: nothing is evicted for a
/// command that does not run here.
#[test]
fn moved_is_answered_without_evicting() {
    let a = Child::start(
        "moved-a",
        &[
            "--cluster-enabled",
            "yes",
            "--maxmemory-policy",
            "allkeys-lru",
        ],
    );
    let b = Child::start("moved-b", &["--cluster-enabled", "yes"]);
    let (mut ca, mut cb) = (a.conn(), b.conn());
    ca.ok(&["CLUSTER", "ADDSLOTSRANGE", "0", "8191"]);
    cb.ok(&["CLUSTER", "ADDSLOTSRANGE", "8192", "16383"]);
    ca.ok(&["CLUSTER", "MEET", "127.0.0.1", &b.port.to_string()]);
    let slot = |c: &mut Conn, key: &str| -> u64 {
        let r = c.cmd(&["CLUSTER", "KEYSLOT", key]);
        String::from_utf8_lossy(&r[1..r.len() - 2]).parse().unwrap()
    };
    let theirs = (0..)
        .map(|i| format!("f{i}"))
        .find(|k| slot(&mut ca, k) >= 8192)
        .unwrap();
    wait_until("the nodes to know each other's slots", || {
        ca.cmd(&["CF.EXISTS", &theirs, "x"]).starts_with(b"-MOVED")
    });
    // The values all in a slot of this node's.
    let tag = (0..)
        .map(|i| format!("{{t{i}}}"))
        .find(|k| slot(&mut ca, k) < 8192)
        .unwrap();
    let used = used_memory(&mut ca);
    ca.ok(&[
        "CONFIG",
        "SET",
        "maxmemory",
        &(used + 3_000_000).to_string(),
    ]);
    fill_to_the_limit(&mut ca, &tag, 10_000, false);
    let before = evicted(&mut ca);
    // Some 8 MB, which would evict hundreds of the values.
    let reply = ca.cmd(&["CF.RESERVE", &theirs, "8000000", "BUCKETSIZE", "1"]);
    assert!(reply.starts_with(b"-MOVED"), "{}", shown(&reply));
    let reply = ca.cmd(&["CMS.INITBYDIM", &theirs, "2000000", "1"]);
    assert!(reply.starts_with(b"-MOVED"), "{}", shown(&reply));
    let after = evicted(&mut ca);
    assert!(
        after - before < 20,
        "{} keys evicted for MOVED",
        after - before
    );
    // Here, it is given its room.
    let ours = format!("{tag}:cf");
    ca.ok(&["CF.RESERVE", &ours, "2000000", "BUCKETSIZE", "1"]);
    assert!(evicted(&mut ca) - after > 100);
}

// ── A command that makes a value either succeeds or does nothing ─────────

/// A Cuckoo write that would have made its filter and then failed (left on
/// the master alone, as error replies are not sent to replicas) now fails
/// before it makes anything, or succeeds: over the per-command limit
/// (`CF.INSERT` of 62,501 items to a missing key); and at the limit with an
/// evicting policy (`CF.ADD` to missing keys, given room for the filter and
/// asked for more by the add). The replica ends with the master's dataset.
#[test]
fn a_cuckoo_write_never_makes_a_filter_and_fails() {
    let master = Child::start("cf-fail", &["--maxmemory-policy", "allkeys-lru"]);
    let replica = Child::replica_of("cf-fail-replica", &master);
    let mut m = master.conn();
    m.ok(&["SET", "kept", "1"]);
    let mut args: Vec<String> = vec!["CF.INSERT".into(), "over".into(), "ITEMS".into()];
    args.extend((0..62_501).map(|i| format!("i{i}")));
    let refs: Vec<&str> = args.iter().map(String::as_str).collect();
    let reply = m.cmd(&refs);
    assert!(
        reply.starts_with(b"-ERR command reached the per-command limit"),
        "{}",
        shown(&reply)
    );
    assert_eq!(m.cmd(&["EXISTS", "over"]), b":0\r\n");
    alike(&master, &replica, "over the per-command limit");
    // At the limit, with a replica: whatever eviction does (it may stop
    // for a while when what it frees stays allocated, #284), an add either
    // makes its filter and is sent, or is refused and makes nothing. How
    // many get room is not asserted here (see the test after this one).
    let used = used_memory(&mut m);
    m.ok(&[
        "CONFIG",
        "SET",
        "maxmemory",
        &(used + 10_000_000).to_string(),
    ]);
    fill_to_the_limit(&mut m, "s", 10_000, false);
    for i in 0..50 {
        let key = format!("new{i}");
        match m.cmd(&["CF.ADD", &key, "x"]).as_slice() {
            b":1\r\n" => {}
            OOM => assert_eq!(m.cmd(&["EXISTS", &key]), b":0\r\n", "{key}"),
            other => panic!("{key}: {}", shown(other)),
        }
    }
    m.ok(&["CONFIG", "SET", "maxmemory", "0"]);
    alike(&master, &replica, "at the limit");
}

/// At the limit with an evicting policy (no replica, whose queue can keep
/// what eviction frees allocated), every `CF.ADD` to a missing key is
/// given room for its filter and succeeds: the room asked for before it
/// ran is all it needs.
#[test]
fn adds_that_make_filters_at_the_limit_all_succeed() {
    let server = Child::start("cf-at-limit", &["--maxmemory-policy", "allkeys-lru"]);
    let mut c = server.conn();
    let used = used_memory(&mut c);
    c.ok(&[
        "CONFIG",
        "SET",
        "maxmemory",
        &(used + 10_000_000).to_string(),
    ]);
    fill_to_the_limit(&mut c, "s", 10_000, false);
    for i in 0..100 {
        let key = format!("new{i}");
        assert_eq!(c.cmd(&["CF.ADD", &key, "x"]), b":1\r\n", "{key}");
    }
}

/// A `CF.INSERT` that makes its filter (with `CAPACITY`, and the master's
/// `cf-*` settings) and has items refused for room is sent as the filter
/// it made and the items it added: the replica, and the master started
/// again from its AOF, end with the same filter.
#[test]
fn an_insert_that_makes_its_filter_and_refuses_items_reaches_the_replica_and_the_aof() {
    let master = Child::start(
        "cf-made-refused",
        &["--aof", "--maxmemory-policy", "noeviction"],
    );
    let replica = Child::replica_of("cf-made-refused-replica", &master);
    let mut m = master.conn();
    for (name, value) in [
        ("cf-bucket-size", "1"),
        ("cf-max-iterations", "1"),
        ("cf-expansion-factor", "2"),
    ] {
        m.ok(&["CONFIG", "SET", name, value]);
    }
    m.ok(&["SET", "other", "1"]);
    let used = used_memory(&mut m);
    m.ok(&[
        "CONFIG",
        "SET",
        "maxmemory",
        &(used + 1_000_000).to_string(),
    ]);
    let mut args = vec!["CF.INSERT", "k", "CAPACITY", "4", "ITEMS"];
    args.extend(std::iter::repeat_n("x", 60));
    let reply = m.cmd(&args);
    m.ok(&["CONFIG", "SET", "maxmemory", "0"]);
    let text = String::from_utf8_lossy(&reply).to_string();
    assert!(text.starts_with("*60\r\n"), "{text}");
    assert!(text.contains(":1\r\n"), "{text}");
    assert!(text.contains("-OOM"), "{text}");
    alike(&master, &replica, "made, items refused");
    let info = m.cmd(&["CF.INFO", "k"]);
    assert_eq!(replica.conn().cmd(&["CF.INFO", "k"]), info);
    let before = dataset(&mut m);
    thread::sleep(Duration::from_millis(1500));
    drop(m);
    let master = master.restart();
    let mut m = master.conn();
    assert_eq!(dataset(&mut m), before, "the AOF");
}

/// A command of many items is given room for the growth that holds them;
/// an item that would grow the filter further when there is no room left
/// is answered with `OOM` on its own and changes nothing, and the command
/// is sent to the replica as the items it added: the two end alike.
#[test]
fn an_insert_past_its_room_is_refused_item_by_item() {
    let master = Child::start("cf-items", &["--maxmemory-policy", "noeviction"]);
    let replica = Child::replica_of("cf-items-replica", &master);
    let mut m = master.conn();
    m.ok(&[
        "CF.RESERVE",
        "k",
        "4",
        "BUCKETSIZE",
        "1",
        "MAXITERATIONS",
        "1",
        "EXPANSION",
        "2",
    ]);
    let used = used_memory(&mut m);
    m.ok(&[
        "CONFIG",
        "SET",
        "maxmemory",
        &(used + 1_000_000).to_string(),
    ]);
    // The same item, again and again: each sub-filter takes it twice at
    // most, so it grows by a sub-filter twice the last every item or two.
    let mut args = vec!["CF.INSERT", "k", "ITEMS"];
    args.extend(std::iter::repeat_n("x", 60));
    let reply = m.cmd(&args);
    m.ok(&["CONFIG", "SET", "maxmemory", "0"]);
    let text = String::from_utf8_lossy(&reply).to_string();
    assert!(text.starts_with("*60\r\n"), "{text}");
    assert!(text.contains(":1\r\n"), "{text}");
    assert!(text.contains("-OOM"), "{text}");
    alike(&master, &replica, "items refused for room");
}

/// Under `maxmemory`, an add to a large filter needs room for a new
/// sub-filter only when it would grow the filter: near the limit with
/// `noeviction`, adds to a 16 MiB filter with free slots succeed.
#[test]
fn adds_to_a_large_filter_near_the_limit() {
    let server = Child::start("cf-large", &["--maxmemory-policy", "noeviction"]);
    let mut c = server.conn();
    c.ok(&["CF.RESERVE", "big", "16777216", "BUCKETSIZE", "1"]);
    let used = used_memory(&mut c);
    c.ok(&[
        "CONFIG",
        "SET",
        "maxmemory",
        &(used + 1_000_000).to_string(),
    ]);
    for i in 0..2000 {
        assert_eq!(
            c.cmd(&["CF.ADD", "big", &format!("i{i}")]),
            b":1\r\n",
            "{i}"
        );
    }
    let mut args = vec!["CF.INSERT".to_string(), "big".into(), "ITEMS".into()];
    args.extend((0..1000).map(|i| format!("j{i}")));
    let refs: Vec<&str> = args.iter().map(String::as_str).collect();
    let reply = c.cmd(&refs);
    assert!(
        !String::from_utf8_lossy(&reply).contains('-'),
        "{}",
        shown(&reply)
    );
    c.ok(&["CONFIG", "SET", "maxmemory", "0"]);
}

/// A filter a copy shares (`COPY`) is copied before a delete writes to it:
/// that copy needs room, as any allocation does. At the limit with
/// `noeviction`, `CF.DEL` of it is refused (and the copy left as it was);
/// with an evicting policy, room is made for it. An unshared filter needs
/// none: `CF.DEL` frees, at the limit too.
#[test]
fn a_shared_filter_needs_room_to_be_written() {
    let server = Child::start("cf-shared", &["--maxmemory-policy", "noeviction"]);
    let mut c = server.conn();
    c.ok(&["CF.RESERVE", "cf", "1000000", "BUCKETSIZE", "1"]);
    c.ok(&["CF.RESERVE", "alone", "1024"]);
    for i in 0..100 {
        c.cmd(&["CF.ADD", "cf", &format!("i{i}")]);
        c.cmd(&["CF.ADD", "alone", &format!("i{i}")]);
    }
    assert_eq!(c.cmd(&["COPY", "cf", "copy"]), b":1\r\n");
    let used = used_memory(&mut c);
    c.ok(&["CONFIG", "SET", "maxmemory", &(used + 300_000).to_string()]);
    assert_eq!(c.cmd(&["CF.DEL", "cf", "i1"]), OOM);
    assert_eq!(c.cmd(&["CF.EXISTS", "cf", "i1"]), b":1\r\n");
    assert_eq!(c.cmd(&["CF.DEL", "alone", "i1"]), b":1\r\n");
    // Only the values with a time to live are evicted.
    c.ok(&["CONFIG", "SET", "maxmemory-policy", "volatile-lru"]);
    let used = used_memory(&mut c);
    c.ok(&[
        "CONFIG",
        "SET",
        "maxmemory",
        &(used + 3_000_000).to_string(),
    ]);
    fill_to_the_limit(&mut c, "s", 10_000, true);
    let before = evicted(&mut c);
    assert_eq!(c.cmd(&["CF.DEL", "cf", "i2"]), b":1\r\n");
    assert!(evicted(&mut c) - before > 50, "room was made for the copy");
    c.ok(&["CONFIG", "SET", "maxmemory", "0"]);
    assert_eq!(c.cmd(&["CF.EXISTS", "copy", "i2"]), b":1\r\n");
    assert_eq!(c.cmd(&["CF.EXISTS", "cf", "i2"]), b":0\r\n");
}

/// `RESTORE` of a probabilistic value asks for room for the size its
/// payload declares, before it allocates it: at the limit with
/// `noeviction`, a 4 MB count-min sketch is refused with `OOM`; with an
/// evicting policy, room is made for it before the command takes its key.
#[test]
fn restore_asks_for_room() {
    let server = Child::start("restore", &["--maxmemory-policy", "noeviction"]);
    let mut c = server.conn();
    c.ok(&["CMS.INITBYDIM", "cms", "1000000", "1"]);
    let payload = bulk(&c.cmd(&["DUMP", "cms"]));
    assert_eq!(c.cmd(&["DEL", "cms"]), b":1\r\n");
    let used = used_memory(&mut c);
    c.ok(&[
        "CONFIG",
        "SET",
        "maxmemory",
        &(used + 1_000_000).to_string(),
    ]);
    assert_eq!(c.raw(&[b"RESTORE", b"cms", b"0", &payload]), OOM);
    assert_eq!(c.cmd(&["EXISTS", "cms"]), b":0\r\n");
    c.ok(&["CONFIG", "SET", "maxmemory-policy", "allkeys-lru"]);
    // The payload is counted too while the command runs, twice (as read
    // and as parsed): room for it all.
    let used = used_memory(&mut c);
    c.ok(&[
        "CONFIG",
        "SET",
        "maxmemory",
        &(used + 24_000_000).to_string(),
    ]);
    fill_to_the_limit(&mut c, "s", 10_000, false);
    let before = evicted(&mut c);
    assert_eq!(c.raw(&[b"RESTORE", b"cms", b"0", &payload]), b"+OK\r\n");
    assert!(evicted(&mut c) - before > 100, "room was made for it");
    c.ok(&["CONFIG", "SET", "maxmemory", "0"]);
    assert_eq!(c.cmd(&["CMS.QUERY", "cms", "a"]), b"*1\r\n:0\r\n");
}

// ── Room and what it was asked under ─────────────────────────────────────

/// `CF.DEL` and `CF.COMPACT` of a filter nothing shares allocate nothing
/// and free: over the limit with `noeviction` they run (a write that may
/// add is refused, as before).
#[test]
fn deletes_that_allocate_nothing_run_over_the_limit() {
    let server = Child::start("cf-over", &["--maxmemory-policy", "noeviction"]);
    let mut c = server.conn();
    c.ok(&["CF.RESERVE", "f", "1024", "EXPANSION", "1"]);
    for i in 0..3000 {
        c.cmd(&["CF.ADD", "f", &format!("i{i}")]);
    }
    c.ok(&["SET", "pad", &"v".repeat(2_000_000)]);
    let used = used_memory(&mut c);
    c.ok(&["CONFIG", "SET", "maxmemory", &(used / 2).to_string()]);
    assert_eq!(c.cmd(&["CF.DEL", "f", "i1"]), b":1\r\n");
    assert_eq!(c.cmd(&["CF.COMPACT", "f"]), b"+OK\r\n");
    assert_eq!(c.cmd(&["CF.ADD", "f", "new"]), OOM);
    c.ok(&["CONFIG", "SET", "maxmemory", "0"]);
}

/// A Bloom filter of several layers restored near the limit with an
/// evicting policy is given room for all its layers, and each layer is
/// held to its own size as it is read.
#[test]
fn a_bloom_filter_of_several_layers_restores_near_the_limit() {
    let server = Child::start("bf-layers", &["--maxmemory-policy", "allkeys-lru"]);
    let mut c = server.conn();
    c.ok(&["BF.RESERVE", "b", "0.01", "50000", "EXPANSION", "2"]);
    let mut n = 0;
    while n < 350_000 {
        let mut args = vec!["BF.MADD".to_string(), "b".to_string()];
        args.extend((n..n + 1000).map(|i| format!("i{i}")));
        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
        c.cmd(&refs);
        n += 1000;
    }
    let filters = c.cmd(&["BF.INFO", "b", "FILTERS"]);
    let layers: u64 = String::from_utf8_lossy(&filters)
        .lines()
        .find_map(|l| l.strip_prefix(':')?.parse().ok())
        .unwrap_or(0);
    assert!(layers >= 3, "{}", shown(&filters));
    let payload = bulk(&c.cmd(&["DUMP", "b"]));
    assert_eq!(c.cmd(&["DEL", "b"]), b":1\r\n");
    let used = used_memory(&mut c);
    c.ok(&[
        "CONFIG",
        "SET",
        "maxmemory",
        &(used + 8_000_000).to_string(),
    ]);
    fill_to_the_limit(&mut c, "s", 10_000, false);
    assert_eq!(c.raw(&[b"RESTORE", b"b", b"0", &payload]), b"+OK\r\n");
    c.ok(&["CONFIG", "SET", "maxmemory", "0"]);
    assert_eq!(c.cmd(&["BF.EXISTS", "b", "i7"]), b":1\r\n");
}

/// A `RESTORE` refused `BUSYKEY` is given no room (nothing is evicted for
/// it); and a command that wants more than evicting every key could free
/// is refused at once, the keys left where they are.
#[test]
fn room_is_not_made_for_what_cannot_run() {
    let server = Child::start("not-made", &["--maxmemory-policy", "allkeys-lru"]);
    let mut c = server.conn();
    c.ok(&["CMS.INITBYDIM", "cms", "500000", "1"]);
    let payload = bulk(&c.cmd(&["DUMP", "cms"]));
    c.ok(&["SET", "busy", "1"]);
    for i in 0..300 {
        c.ok(&["SET", &format!("s{i}"), &"v".repeat(10_000)]);
    }
    // Room for the payload (counted twice while the command runs) but not
    // for the sketch too: room would be made by evicting, were the command
    // asked what it wants.
    let limit = used_memory(&mut c) + 5_000_000;
    c.ok(&["CONFIG", "SET", "maxmemory", &limit.to_string()]);
    let before = evicted(&mut c);
    let reply = c.raw(&[b"RESTORE", b"busy", b"0", &payload]);
    assert!(reply.starts_with(b"-BUSYKEY"), "{}", shown(&reply));
    assert_eq!(evicted(&mut c), before, "evicted for BUSYKEY");
    fill_to_the_limit(&mut c, "t", 10_000, false);
    // Between the limit less what the server held before it had any data,
    // and the limit: eviction could never free it.
    let startup: u64 = info_field(&mut c, "memory", "used_memory_startup")
        .parse()
        .unwrap();
    assert!(startup > 0, "used_memory_startup {startup}");
    // With the 128 KiB a command is given beyond what it says.
    let width = ((limit - (128 << 10) - startup / 2) / 4).to_string();
    let before = evicted(&mut c);
    assert_eq!(c.cmd(&["CMS.INITBYDIM", "huge", &width, "1"]), OOM);
    assert!(
        evicted(&mut c) - before < 20,
        "evicted for a request that cannot fit"
    );
}

/// Eviction for one command looks again, in each step, at what it was
/// asked under: writes paused, this node made a replica, `maxmemory`
/// changed. (Debug builds: a hook stalls 300 ms between steps.)
#[cfg(debug_assertions)]
#[test]
fn eviction_in_steps_stops_when_what_it_was_asked_under_changes() {
    for change in ["pause", "replica", "maxmemory"] {
        let master = Child::start_env(
            &format!("steps-{change}"),
            &["--maxmemory-policy", "allkeys-lru"],
            &[("KIVIDB_TEST_FAILPOINTS", "sketch-room-step-stall")],
        );
        // No replica: one's queue can keep what eviction frees allocated
        // (#284), and eviction then stops of itself.
        let mut c = master.conn();
        let used = used_memory(&mut c);
        c.ok(&[
            "CONFIG",
            "SET",
            "maxmemory",
            &(used + 30_000_000).to_string(),
        ]);
        fill_to_the_limit(&mut c, "s", 10_000, false);
        // 16 MiB (a power of two of buckets): four steps and more, 300 ms
        // apart.
        let offset_before = info_field(&mut c, "replication", "master_repl_offset");
        let port = master.port;
        let sized = thread::spawn(move || {
            let mut c = Conn::connect(port);
            c.cmd(&["CF.RESERVE", "big", "16000000", "BUCKETSIZE", "1"])
        });
        thread::sleep(Duration::from_millis(450));
        match change {
            "pause" => c.ok(&["CLIENT", "PAUSE", "3000", "WRITE"]),
            "replica" => {
                let nowhere = common::free_port().to_string();
                c.ok(&["REPLICAOF", "127.0.0.1", &nowhere]);
            }
            _ => c.ok(&["CONFIG", "SET", "maxmemory", "0"]),
        }
        thread::sleep(Duration::from_millis(400));
        let (evicted_then, offset_then) = (
            evicted(&mut c),
            info_field(&mut c, "replication", "master_repl_offset"),
        );
        thread::sleep(Duration::from_millis(1200));
        assert_eq!(evicted(&mut c), evicted_then, "{change}: went on evicting");
        if change == "pause" {
            assert_eq!(
                info_field(&mut c, "replication", "master_repl_offset"),
                offset_then,
                "the offset of a paused master moved"
            );
        }
        let reply = sized.join().unwrap();
        match change {
            // Held by the pause, then dispatched, and sized, again: it
            // evicted again once the pause was over.
            "pause" => {
                assert!(evicted(&mut c) > evicted_then, "not sized again");
                assert_eq!(reply, b"+OK\r\n", "{}", shown(&reply));
                let _ = &offset_before;
            }
            "replica" => assert!(reply.starts_with(b"-"), "{}", shown(&reply)),
            _ => assert_eq!(reply, b"+OK\r\n", "{}", shown(&reply)),
        }
    }
}

// ── Transactions and scripts ─────────────────────────────────────────────

/// Room is made for a command a transaction runs, as for one from a client.
#[test]
fn room_is_made_in_a_transaction() {
    let server = Child::start("multi", &["--maxmemory-policy", "allkeys-lru"]);
    let mut c = server.conn();
    let used = used_memory(&mut c);
    c.ok(&[
        "CONFIG",
        "SET",
        "maxmemory",
        &(used + 3_000_000).to_string(),
    ]);
    fill_to_the_limit(&mut c, "s", 10_000, false);
    let before = evicted(&mut c);
    c.ok(&["MULTI"]);
    assert_eq!(
        c.cmd(&["CF.RESERVE", "cf", "2000000", "BUCKETSIZE", "1"]),
        b"+QUEUED\r\n"
    );
    assert_eq!(
        c.cmd(&["CMS.INITBYDIM", "cms", "100000", "2"]),
        b"+QUEUED\r\n"
    );
    assert_eq!(c.cmd(&["EXEC"]), b"*2\r\n+OK\r\n+OK\r\n");
    assert!(evicted(&mut c) - before > 100, "room was made for them");
}

/// Room is made for a command a script calls, as for one from a client
/// (the script runs alone, holding the gates).
#[cfg(feature = "lua")]
#[test]
fn room_is_made_for_a_script() {
    let server = Child::start("script", &["--maxmemory-policy", "allkeys-lru"]);
    let mut c = server.conn();
    let used = used_memory(&mut c);
    c.ok(&[
        "CONFIG",
        "SET",
        "maxmemory",
        &(used + 3_000_000).to_string(),
    ]);
    fill_to_the_limit(&mut c, "s", 10_000, false);
    let before = evicted(&mut c);
    let reply = c.cmd(&[
        "EVAL",
        "return redis.call('CF.RESERVE', KEYS[1], '2000000', 'BUCKETSIZE', '1')",
        "1",
        "cf",
    ]);
    assert_eq!(reply, b"+OK\r\n", "{}", shown(&reply));
    assert!(evicted(&mut c) - before > 100, "room was made for it");
}

// ── Replicas under concurrent writers ────────────────────────────────────

/// t-digests and count-min sketches written by several clients at once
/// (adds, increments, merges into them and resets) end on the replica as
/// on the master.
#[test]
fn digests_and_sketches_written_at_once_reach_the_replica_alike() {
    let master = Child::start("concurrent", &[]);
    let replica = Child::replica_of("concurrent-replica", &master);
    let mut m = master.conn();
    for i in 0..4 {
        m.ok(&["TDIGEST.CREATE", &format!("td{i}"), "COMPRESSION", "50"]);
        m.ok(&["CMS.INITBYDIM", &format!("cms{i}"), "200", "4"]);
    }
    thread::scope(|s| {
        for t in 0..6u64 {
            let master = &master;
            s.spawn(move || {
                let mut c = master.conn();
                let mut seed = 0x9e37_79b9_7f4a_7c15u64 ^ (t + 1);
                let mut next = move || {
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    seed
                };
                for _ in 0..1500 {
                    let (a, b) = (next() % 4, next() % 4);
                    let v = format!("{}.{}", next() % 1000, next() % 100);
                    match next() % 10 {
                        0..=3 => {
                            c.cmd(&["TDIGEST.ADD", &format!("td{a}"), &v, "7"]);
                        }
                        4..=6 => {
                            let item = format!("i{}", next() % 50);
                            let by = (1 + next() % 9).to_string();
                            c.cmd(&["CMS.INCRBY", &format!("cms{a}"), &item, &by]);
                        }
                        7 => {
                            c.cmd(&[
                                "TDIGEST.MERGE",
                                &format!("td{a}"),
                                "2",
                                &format!("td{a}"),
                                &format!("td{b}"),
                            ]);
                        }
                        8 if a != b => {
                            c.cmd(&[
                                "CMS.MERGE",
                                &format!("cms{a}"),
                                "2",
                                &format!("cms{a}"),
                                &format!("cms{b}"),
                            ]);
                        }
                        _ => {
                            if next() % 8 == 0 {
                                c.cmd(&["TDIGEST.RESET", &format!("td{a}")]);
                            } else {
                                c.cmd(&["TDIGEST.ADD", &format!("td{b}"), &v]);
                            }
                        }
                    }
                }
            });
        }
    });
    alike(&master, &replica, "concurrent writers");
}
