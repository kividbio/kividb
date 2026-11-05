// tests/resync.rs
//
// Partial resynchronisation, and how a full one is loaded. Real `kividb`
// processes: the points of interest are a replica losing its connection and a
// replica being restarted, and neither can be done to a server running inside
// the test process.
//
// Problems found by orchestration tooling restarting and moving replicas.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
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
    fn arr(&self) -> &[V] {
        match self {
            V::Arr(v) => v,
            other => panic!("not an array: {other:?}"),
        }
    }
    fn ok(&self) -> bool {
        matches!(self, V::Str(s) if s == "OK")
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

    fn stat(&mut self, field: &str) -> i64 {
        self.info("stats", field).parse().unwrap()
    }
}

// ── Server fixture ────────────────────────────────────────────────────────────

struct Server {
    port: u16,
    dir: PathBuf,
    child: Child,
    args: Vec<String>,
    /// Environment of its own, for the test hooks.
    env: Vec<(String, String)>,
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

fn kividb(dir: &Path, port: u16, extra: &[String]) -> Child {
    kividb_with(dir, port, extra, &[])
}

fn kividb_with(dir: &Path, port: u16, extra: &[String], env: &[(String, String)]) -> Child {
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("server.log"))
        .unwrap();
    Command::new(env!("CARGO_BIN_EXE_kividb"))
        .current_dir(dir)
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
        .envs(env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(log)
        .spawn()
        .expect("could not start kividb")
}

impl Server {
    fn start(extra: &[&str]) -> Self {
        Self::start_with_env(extra, &[])
    }

    fn start_with_env(extra: &[&str], env: &[(&str, &str)]) -> Self {
        let dir = tempfile::tempdir().unwrap().keep();
        let port = free_port();
        let args: Vec<String> = extra.iter().map(|s| s.to_string()).collect();
        let env: Vec<(String, String)> = env
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let child = kividb_with(&dir, port, &args, &env);
        let server = Server {
            port,
            dir,
            child,
            args,
            env,
        };
        assert_eq!(server.conn().cmd(&["PING"]).s(), "PONG");
        server
    }

    fn conn(&self) -> Conn {
        Conn::connect(self.port)
    }

    fn log(&self) -> String {
        std::fs::read_to_string(self.dir.join("server.log")).unwrap_or_default()
    }

    /// `SHUTDOWN`: the final save, and then the process is gone.
    fn shutdown(&mut self) {
        let mut c = self.conn();
        let _ = c.writer.write_all(b"*1\r\n$8\r\nSHUTDOWN\r\n");
        let deadline = Instant::now() + Duration::from_secs(20);
        while self.child.try_wait().unwrap().is_none() {
            assert!(Instant::now() < deadline, "server did not shut down");
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    fn restart(&mut self) {
        self.child = kividb_with(&self.dir, self.port, &self.args, &self.env);
        assert_eq!(self.conn().cmd(&["PING"]).s(), "PONG");
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

// ── A link that can be cut ────────────────────────────────────────────────────

/// A TCP relay in front of a master. A replica pointed at the relay has a
/// replication link the test can cut — and keep cut — without touching
/// either server.
struct Relay {
    port: u16,
    open: Arc<AtomicBool>,
    live: Arc<Mutex<Vec<TcpStream>>>,
    /// How many bytes a connection may carry from the master before it is
    /// cut. `u64::MAX`: as many as it likes.
    budget: Arc<std::sync::atomic::AtomicU64>,
    /// While set, nothing is carried from the master, and the connections
    /// stay open: the replica is connected, and receives nothing.
    paused: Arc<AtomicBool>,
    /// A connection that has carried this many bytes from the master
    /// pauses the relay. `u64::MAX`: none does.
    pause_at: Arc<std::sync::atomic::AtomicU64>,
}

impl Relay {
    fn to(target: u16) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let open = Arc::new(AtomicBool::new(true));
        let live: Arc<Mutex<Vec<TcpStream>>> = Arc::new(Mutex::new(Vec::new()));
        let budget = Arc::new(std::sync::atomic::AtomicU64::new(u64::MAX));
        let paused = Arc::new(AtomicBool::new(false));
        let pause_at = Arc::new(std::sync::atomic::AtomicU64::new(u64::MAX));
        let (open_bg, live_bg, budget_bg, paused_bg, pause_at_bg) = (
            Arc::clone(&open),
            Arc::clone(&live),
            Arc::clone(&budget),
            Arc::clone(&paused),
            Arc::clone(&pause_at),
        );
        std::thread::spawn(move || {
            for client in listener.incoming().flatten() {
                if !open_bg.load(Ordering::SeqCst) {
                    let _ = client.shutdown(Shutdown::Both);
                    continue;
                }
                let Ok(upstream) = TcpStream::connect(("127.0.0.1", target)) else {
                    continue;
                };
                let mut live = live_bg.lock().unwrap();
                // To the master: everything.
                {
                    let (mut from, mut to) =
                        (client.try_clone().unwrap(), upstream.try_clone().unwrap());
                    std::thread::spawn(move || {
                        let _ = std::io::copy(&mut from, &mut to);
                        let _ = to.shutdown(Shutdown::Both);
                        let _ = from.shutdown(Shutdown::Both);
                    });
                }
                // From the master: until this connection's budget is spent.
                {
                    let (mut from, mut to) =
                        (upstream.try_clone().unwrap(), client.try_clone().unwrap());
                    let budget = budget_bg.load(Ordering::SeqCst);
                    let paused = Arc::clone(&paused_bg);
                    let pause_at = Arc::clone(&pause_at_bg);
                    std::thread::spawn(move || {
                        let mut carried = 0u64;
                        let mut buf = [0u8; 16 * 1024];
                        loop {
                            while paused.load(Ordering::SeqCst) {
                                std::thread::sleep(Duration::from_millis(10));
                            }
                            let n = match from.read(&mut buf) {
                                Ok(0) | Err(_) => break,
                                Ok(n) => n,
                            };
                            let allowed = (budget - carried).min(n as u64) as usize;
                            if to.write_all(&buf[..allowed]).is_err() {
                                break;
                            }
                            carried += allowed as u64;
                            if carried >= budget {
                                break;
                            }
                            if carried >= pause_at.load(Ordering::SeqCst) {
                                paused.store(true, Ordering::SeqCst);
                            }
                        }
                        let _ = to.shutdown(Shutdown::Both);
                        let _ = from.shutdown(Shutdown::Both);
                    });
                }
                live.push(client);
                live.push(upstream);
            }
        });
        Relay {
            port,
            open,
            live,
            budget,
            paused,
            pause_at,
        }
    }

    /// Pause, as [`Self::pause`] does, once a connection has carried this
    /// many bytes from the master: in the middle of a full sync's payload,
    /// for one larger than that.
    fn pause_after(&self, bytes: u64) {
        self.pause_at.store(bytes, Ordering::SeqCst);
    }

    /// Carry nothing more from the master until [`Self::resume`], with the
    /// connections left open.
    fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
    }

    fn resume(&self) {
        self.pause_at.store(u64::MAX, Ordering::SeqCst);
        self.paused.store(false, Ordering::SeqCst);
    }

    /// From now on, cut each new connection once it has carried this many
    /// bytes from the master.
    fn cut_after(&self, bytes: u64) {
        self.budget.store(bytes, Ordering::SeqCst);
    }

    /// Drop every connection through the relay, and refuse new ones.
    fn cut(&self) {
        self.open.store(false, Ordering::SeqCst);
        for stream in self.live.lock().unwrap().drain(..) {
            let _ = stream.shutdown(Shutdown::Both);
        }
    }

    fn restore(&self) {
        self.open.store(true, Ordering::SeqCst);
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn wait_link_up(replica: &mut Conn) {
    wait_until("the replica's link to come up", || {
        replica.info("replication", "master_link_status") == "up"
    });
}

fn wait_link_down(replica: &mut Conn) {
    wait_until("the replica's link to go down", || {
        replica.info("replication", "master_link_status") == "down"
    });
}

/// Writes whose result differs if any of them is applied twice or not at all.
fn write_batch(master: &mut Conn, round: usize, count: usize) {
    for i in 0..count {
        assert!(master.cmd(&["INCR", "counter"]).i() > 0);
        assert!(master.cmd(&["RPUSH", "list", &format!("{round}-{i}")]).i() > 0);
        assert!(
            master
                .cmd(&["SET", &format!("key:{round}:{i}"), "value"])
                .ok()
        );
    }
}

/// The same data on both sides — compared through commands whose answers
/// depend on every write having been applied exactly once.
fn assert_same_dataset(master: &mut Conn, replica: &mut Conn) {
    // At least as far as the master was: its heartbeats go on adding to its
    // offset, and the replica can step past this one between two looks.
    let offset: u64 = master
        .info("replication", "master_repl_offset")
        .parse()
        .unwrap();
    wait_until("the replica to catch up", || {
        replica
            .info("replication", "slave_repl_offset")
            .parse::<u64>()
            .is_ok_and(|applied| applied >= offset)
    });
    for probe in [
        &["GET", "counter"][..],
        &["LLEN", "list"],
        &["LRANGE", "list", "0", "-1"],
        &["DBSIZE"],
    ] {
        assert_eq!(master.cmd(probe), replica.cmd(probe), "{probe:?}");
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// A replica that loses its connection and gets it back is sent what it
/// missed, not the dataset. It used to be sent the dataset every time.
#[test]
fn a_dropped_link_is_resumed_with_a_partial_resync() {
    let master = Server::start(&[]);
    let relay = Relay::to(master.port);
    let replica = Server::start(&["--replicaof", "127.0.0.1", &relay.port.to_string()]);
    let (mut m, mut r) = (master.conn(), replica.conn());

    write_batch(&mut m, 0, 50);
    wait_link_up(&mut r);
    assert_same_dataset(&mut m, &mut r);
    assert_eq!(m.stat("sync_full"), 1);
    assert_eq!(m.info("replication", "repl_backlog_active"), "1");
    assert!(
        m.info("replication", "slave0").contains("state=online"),
        "{}",
        m.info("replication", "slave0")
    );

    // Three outages, with writes during each.
    for round in 1..=3 {
        relay.cut();
        wait_link_down(&mut r);
        write_batch(&mut m, round, 40);
        // Across databases too: the stream's current database is part of
        // where the replica is.
        assert!(m.cmd(&["SELECT", "3"]).ok());
        assert!(m.cmd(&["SET", &format!("in-db-3:{round}"), "v"]).ok());
        assert!(m.cmd(&["SELECT", "0"]).ok());
        relay.restore();
        wait_link_up(&mut r);
        assert_same_dataset(&mut m, &mut r);
        assert_eq!(m.stat("sync_partial_ok"), round as i64);
    }
    assert_eq!(m.stat("sync_full"), 1, "no dataset was transferred again");
    assert_eq!(m.stat("sync_partial_err"), 0);
    assert!(r.cmd(&["SELECT", "3"]).ok());
    assert_eq!(r.cmd(&["DBSIZE"]).i(), 3);
    assert!(
        replica.log().contains("Partial resync"),
        "{}",
        replica.log()
    );
}

/// More was written during the outage than the backlog holds: a full resync,
/// and still the right data.
#[test]
fn an_outage_longer_than_the_backlog_ends_in_a_full_resync() {
    // 16 KiB is the smallest backlog there is.
    let master = Server::start(&["--repl-backlog-size", "16384"]);
    let relay = Relay::to(master.port);
    let replica = Server::start(&["--replicaof", "127.0.0.1", &relay.port.to_string()]);
    let (mut m, mut r) = (master.conn(), replica.conn());

    write_batch(&mut m, 0, 20);
    wait_link_up(&mut r);
    assert_same_dataset(&mut m, &mut r);

    relay.cut();
    wait_link_down(&mut r);
    write_batch(&mut m, 1, 400); // ~40 KiB of stream
    relay.restore();
    wait_link_up(&mut r);
    assert_same_dataset(&mut m, &mut r);
    assert_eq!(m.stat("sync_full"), 2);
    assert_eq!(m.stat("sync_partial_ok"), 0);
    assert_eq!(m.stat("sync_partial_err"), 1);
}

/// A replica shut down cleanly and started again asks to continue from the
/// dataset it saved — with or without an append-only file.
#[test]
fn a_cleanly_restarted_replica_continues_instead_of_resyncing() {
    for aof in [false, true] {
        let master = Server::start(&[]);
        let port = master.port.to_string();
        let mut args = vec!["--replicaof", "127.0.0.1", &port];
        if aof {
            args.push("--aof");
        }
        let mut replica = Server::start(&args);
        let mut m = master.conn();
        let mut r = replica.conn();

        write_batch(&mut m, 0, 60);
        wait_link_up(&mut r);
        assert_same_dataset(&mut m, &mut r);
        drop(r);

        replica.shutdown();
        assert!(replica.dir.join("dump.kdb.repl").exists(), "aof={aof}");
        write_batch(&mut m, 1, 60);
        replica.restart();

        let mut r = replica.conn();
        wait_link_up(&mut r);
        assert_same_dataset(&mut m, &mut r);
        assert_eq!(m.stat("sync_full"), 1, "aof={aof}: {}", replica.log());
        assert_eq!(m.stat("sync_partial_ok"), 1, "aof={aof}");

        // And once more, to show the restarted replica saved a usable point
        // of its own.
        drop(r);
        replica.shutdown();
        write_batch(&mut m, 2, 10);
        replica.restart();
        let mut r = replica.conn();
        wait_link_up(&mut r);
        assert_same_dataset(&mut m, &mut r);
        assert_eq!(m.stat("sync_full"), 1, "aof={aof}");
        assert_eq!(m.stat("sync_partial_ok"), 2, "aof={aof}");
    }
}

/// A master and its replica both stopped cleanly and started again, as in a
/// planned restart of a whole cluster: the master carries on the stream it
/// left, and the replica is continued from where it was, not sent the data
/// it already has. A master stopped with kill -9 starts a new stream.
#[test]
fn a_cleanly_restarted_master_continues_its_replicas() {
    let mut master = Server::start(&["--aof"]);
    let port = master.port.to_string();
    let mut replica = Server::start(&["--replicaof", "127.0.0.1", &port, "--aof"]);
    let mut m = master.conn();
    let mut r = replica.conn();
    write_batch(&mut m, 0, 60);
    wait_link_up(&mut r);
    assert_same_dataset(&mut m, &mut r);
    let replid = m.info("replication", "master_replid");
    drop((m, r));

    replica.shutdown();
    master.shutdown();
    master.restart();
    let mut m = master.conn();
    assert_eq!(
        m.info("replication", "master_replid"),
        replid,
        "{}",
        master.log()
    );
    write_batch(&mut m, 1, 20);
    replica.restart();
    let mut r = replica.conn();
    wait_link_up(&mut r);
    assert_same_dataset(&mut m, &mut r);
    assert_eq!(m.stat("sync_full"), 0, "{}", replica.log());
    assert_eq!(m.stat("sync_partial_ok"), 1);

    // Killed, it cannot vouch for where its old stream stood, and starts a
    // new one; the replica, holding the same data, is continued on that
    // (see `a_killed_master_continues_a_replica_that_holds_the_same_data`).
    drop((m, r));
    std::thread::sleep(Duration::from_millis(1500));
    master.kill();
    master.restart();
    let mut m = master.conn();
    assert_ne!(m.info("replication", "master_replid"), replid);
    let mut r = replica.conn();
    write_batch(&mut m, 2, 5);
    wait_link_up(&mut r);
    assert_same_dataset(&mut m, &mut r);
    assert_eq!(m.stat("sync_full"), 0);
}

/// Writes up to the moment of the shutdown, keys and hash fields expiring
/// through it, and a replica `PING` every second: a master stopped cleanly
/// still continues its replica (#306). Its final save records the offset at
/// its cut, and the replica used to end a few bytes away from it: behind,
/// by writes and expiries not yet delivered when the process exited, or
/// ahead, by a `PING` or a `DEL` written after the cut. Either way the
/// restarted master sent it the whole dataset again.
#[test]
fn a_master_stopped_under_writes_with_keys_expiring_continues_its_replica() {
    let mut master = Server::start(&["--aof", "--repl-ping-replica-period", "1"]);
    let port = master.port.to_string();
    let replica = Server::start(&["--replicaof", "127.0.0.1", &port, "--aof"]);
    let mut r = replica.conn();
    for round in 0..3 {
        let mut m = master.conn();
        write_batch(&mut m, round, 30);
        wait_link_up(&mut r);
        // Keys and fields that expire around the shutdown, and writes until
        // it starts: no time for the replica to have them all.
        for i in 0..200 {
            let ttl = (50 + 7 * i).to_string();
            assert!(
                m.cmd(&["SET", &format!("ttl:{round}:{i}"), "v", "PX", &ttl])
                    .ok()
            );
            let hash = format!("h:{round}:{}", i % 20);
            let field = format!("f{i}");
            m.cmd(&["HSET", &hash, &field, "v"]);
            m.cmd(&["HPEXPIRE", &hash, &ttl, "FIELDS", "1", &field]);
            m.cmd(&["INCR", "counter"]);
        }
        drop(m);
        master.shutdown();
        let log = master.log();
        assert!(
            log.matches("has the whole stream").count() > round,
            "round {round}: the master did not see its replica take the whole stream:\n{log}"
        );
        master.restart();
        let mut m = master.conn();
        wait_link_up(&mut r);
        assert_eq!(
            m.stat("sync_full"),
            0,
            "round {round}: the replica was sent the dataset again:\n{}\n{}",
            master.log(),
            replica.log()
        );
        assert_eq!(m.stat("sync_partial_ok"), 1, "round {round}");
        // Every key and field with a TTL has gone by now, on both sides.
        std::thread::sleep(Duration::from_millis(2000));
        assert_same_dataset(&mut m, &mut r);
    }
}

/// The shutdown's wait for its replicas lasts at most `shutdown-timeout`,
/// and `SHUTDOWN NOW` does not wait at all.
#[test]
fn a_shutdown_waits_for_a_silent_replica_no_longer_than_shutdown_timeout() {
    let mut master = Server::start(&["--aof"]);
    let relay = Relay::to(master.port);
    let replica = Server::start(&["--replicaof", "127.0.0.1", &relay.port.to_string()]);
    let (mut m, mut r) = (master.conn(), replica.conn());
    assert_eq!(
        m.cmd(&["CONFIG", "GET", "shutdown-timeout"]).arr()[1].s(),
        "10"
    );
    write_batch(&mut m, 0, 20);
    wait_link_up(&mut r);
    assert_same_dataset(&mut m, &mut r);

    // Connected, and receiving nothing: it never acknowledges what follows.
    relay.pause();
    write_batch(&mut m, 1, 20);
    let started = Instant::now();
    let _ = m.writer.write_all(b"*2\r\n$8\r\nSHUTDOWN\r\n$3\r\nNOW\r\n");
    while master.child.try_wait().unwrap().is_none() {
        assert!(started.elapsed() < Duration::from_secs(20), "no exit");
        std::thread::sleep(Duration::from_millis(20));
    }
    let now_took = started.elapsed();
    assert!(
        now_took < Duration::from_secs(5),
        "SHUTDOWN NOW waited {now_took:?}"
    );
    assert!(!master.log().contains("did not acknowledge"));

    relay.resume();
    master.restart();
    let mut m = master.conn();
    wait_link_up(&mut r);
    assert_same_dataset(&mut m, &mut r);
    assert!(m.cmd(&["CONFIG", "SET", "shutdown-timeout", "2"]).ok());
    assert_eq!(
        m.cmd(&["CONFIG", "GET", "shutdown-timeout"]).arr()[1].s(),
        "2"
    );
    relay.pause();
    write_batch(&mut m, 2, 20);
    drop(m);
    let started = Instant::now();
    master.shutdown();
    let took = started.elapsed();
    assert!(
        took >= Duration::from_secs(2) && took < Duration::from_secs(10),
        "the shutdown took {took:?}"
    );
    assert!(
        master
            .log()
            .contains("did not acknowledge the end of the stream"),
        "{}",
        master.log()
    );
    relay.resume();
}

/// `queued=` of the master's `slave0` line, or `None` while it has none.
fn queued(master: &mut Conn) -> Option<u64> {
    let info = master.cmd(&["INFO", "replication"]);
    let row = info
        .s()
        .lines()
        .find(|l| l.starts_with("slave0:"))?
        .to_string();
    row.split(',')
        .find_map(|kv| kv.strip_prefix("queued="))
        .map(|n| n.trim().parse().unwrap())
}

/// A master whose replica's full sync is held up by `pause_after`, with
/// 20 MB in its dataset and `repl-replica-max-backlog-bytes` 1 MB; and 4 MB
/// written while the replica can read nothing.
fn full_sync_with_writes_queued(sync_limit: &str) -> (Server, Relay, Server) {
    // A backlog smaller than the queue the sync is allowed: it is the larger
    // of the two that applies, and a replica dropped from its sync is then
    // sent the dataset again.
    let master = Server::start(&[
        "--repl-replica-max-backlog-bytes",
        "1048576",
        "--repl-backlog-size",
        "1048576",
    ]);
    let mut m = master.conn();
    assert!(
        m.cmd(&[
            "CONFIG",
            "SET",
            "repl-replica-sync-max-backlog-bytes",
            sync_limit
        ])
        .ok()
    );
    let value = "v".repeat(1000);
    for i in 0..20_000 {
        assert!(m.cmd(&["SET", &format!("base:{i}"), &value]).ok());
    }
    let relay = Relay::to(master.port);
    // The handshake goes through, and the payload stops part-way.
    relay.pause_after(64 * 1024);
    let replica = Server::start(&["--replicaof", "127.0.0.1", &relay.port.to_string()]);
    wait_until("the master to register its replica", || {
        queued(&mut m).is_some()
    });
    for i in 0..4_000 {
        assert!(m.cmd(&["SET", &format!("during:{i}"), &value]).ok());
    }
    assert!(m.cmd(&["INCR", "counter"]).i() > 0);
    (master, relay, replica)
}

/// A full sync completes even though what is written while the replica
/// receives and loads the dataset is more than `repl-replica-max-backlog-bytes`
/// (#305): the replica reads nothing of the stream until it has loaded, and
/// the sync has an allowance of its own, `repl-replica-sync-max-backlog-bytes`.
/// It used to be dropped at the streaming limit and start again, for as long
/// as the writes lasted. Both limits can be changed at runtime, and `INFO`
/// shows what is queued for each replica.
#[test]
fn a_full_sync_has_room_for_the_writes_made_while_it_loads() {
    let (master, relay, replica) = full_sync_with_writes_queued("64mb");
    let mut m = master.conn();
    assert_eq!(
        m.cmd(&["CONFIG", "GET", "repl-replica-sync-max-backlog-bytes"])
            .arr()[1]
            .s(),
        "67108864"
    );
    assert_eq!(
        m.cmd(&["CONFIG", "GET", "repl-replica-max-backlog-bytes"])
            .arr()[1]
            .s(),
        "1048576"
    );
    let waiting = queued(&mut m).unwrap();
    assert!(waiting > 1024 * 1024, "queued={waiting}");

    relay.resume();
    let mut r = replica.conn();
    wait_link_up(&mut r);
    assert_same_dataset(&mut m, &mut r);
    assert_eq!(m.stat("sync_full"), 1, "{}", master.log());
    // The queue's peak is logged once the replica streams, at the next write.
    assert!(m.cmd(&["INCR", "counter"]).i() > 0);
    wait_until("the master to log the sync's queue peak", || {
        master.log().contains("is streaming: its queue peaked at")
    });
    assert!(!master.log().contains("exceeded backlog limit"));
    assert!(
        m.cmd(&["CONFIG", "SET", "repl-replica-max-backlog-bytes", "2mb"])
            .ok()
    );
    assert_eq!(
        m.cmd(&["CONFIG", "GET", "repl-replica-max-backlog-bytes"])
            .arr()[1]
            .s(),
        "2097152"
    );
}

/// The sync's allowance is a bound: past it the replica is dropped, as a
/// streaming one is past its own, and the master's memory stays bounded.
#[test]
fn a_full_sync_past_its_own_queue_limit_is_dropped_and_tried_again() {
    let (master, relay, replica) = full_sync_with_writes_queued("1mb");
    let mut m = master.conn();
    wait_until("the master to drop the replica", || {
        master.log().contains("during its full sync) — evicting")
    });
    relay.resume();
    let mut r = replica.conn();
    wait_link_up(&mut r);
    assert_same_dataset(&mut m, &mut r);
    assert!(m.stat("sync_full") >= 2, "{}", master.log());
}

/// A replica stopped in the middle of a full sync, with its dataset dropped
/// and the master's not yet loaded, does not save that over its last good
/// dump: `dump.kdb` and its resync point stay as they were, and the next
/// start syncs again. It used to write a dump of what it held then, empty
/// or in part, and a restart loaded that.
#[cfg(unix)]
#[test]
fn a_replica_stopped_during_a_full_sync_keeps_its_last_good_dump() {
    // 16 KiB of backlog: the replica's restart cannot be continued.
    let master = Server::start(&["--repl-backlog-size", "16384"]);
    let port = master.port.to_string();
    let mut replica = Server::start_with_env(
        &["--replicaof", "127.0.0.1", &port],
        &[("KIVIDB_TEST_FULL_SYNC_LOAD_DELAY_MS", "4000")],
    );
    let mut m = master.conn();
    write_batch(&mut m, 0, 200);
    let mut r = replica.conn();
    wait_link_up(&mut r);
    assert_same_dataset(&mut m, &mut r);
    drop(r);
    replica.shutdown();
    let good = std::fs::read(replica.dir.join("dump.kdb")).unwrap();
    let point = std::fs::read(replica.dir.join("dump.kdb.repl")).unwrap();

    write_batch(&mut m, 1, 400); // more than the backlog holds
    replica.restart();
    let mut r = replica.conn();
    wait_until("the replica to be loading the full sync", || {
        r.info("replication", "master_sync_in_progress") == "1"
    });
    let pid = replica.child.id().to_string();
    assert!(
        Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .unwrap()
            .success()
    );
    let deadline = Instant::now() + Duration::from_secs(20);
    while replica.child.try_wait().unwrap().is_none() {
        assert!(Instant::now() < deadline, "the replica did not exit");
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        replica.log().contains("the final save is skipped"),
        "{}",
        replica.log()
    );
    assert!(std::fs::read(replica.dir.join("dump.kdb")).unwrap() == good);
    assert!(std::fs::read(replica.dir.join("dump.kdb.repl")).unwrap() == point);

    // Started again, it syncs again.
    replica.restart();
    let mut r = replica.conn();
    wait_link_up(&mut r);
    assert_same_dataset(&mut m, &mut r);
}

/// A master killed outright and started from its AOF knows nothing of where
/// its old stream stood. Its replica, if it holds the same data, is still
/// continued rather than sent everything: the master fingerprints its
/// dataset as it starts and the replica compares. Holding something else,
/// the replica is sent everything, as before.
#[test]
fn a_killed_master_continues_a_replica_that_holds_the_same_data() {
    let mut master = Server::start(&["--aof"]);
    let port = master.port.to_string();
    let mut replica = Server::start(&["--replicaof", "127.0.0.1", &port, "--aof"]);
    let mut m = master.conn();
    let mut r = replica.conn();
    write_batch(&mut m, 0, 200);
    wait_link_up(&mut r);
    assert_same_dataset(&mut m, &mut r);
    drop((m, r));
    // Let the AOF reach the disk, then kill: no save, no saved position.
    std::thread::sleep(Duration::from_millis(1500));
    master.kill();
    master.restart();
    let mut m = master.conn();
    let mut r = replica.conn();
    wait_link_up(&mut r);
    write_batch(&mut m, 1, 20);
    assert_same_dataset(&mut m, &mut r);
    assert_eq!(
        m.stat("sync_full"),
        0,
        "{}\n{}",
        master.log(),
        replica.log()
    );
    assert_eq!(m.stat("sync_partial_ok"), 1);

    // Both killed and started again: the same.
    drop((m, r));
    std::thread::sleep(Duration::from_millis(1500));
    replica.kill();
    master.kill();
    master.restart();
    replica.restart();
    let mut m = master.conn();
    let mut r = replica.conn();
    wait_link_up(&mut r);
    assert_same_dataset(&mut m, &mut r);
    assert_eq!(m.stat("sync_full"), 0, "{}", replica.log());

    // The replica down while the master took writes: it holds less, and is
    // sent everything.
    drop((m, r));
    replica.kill();
    let mut m = master.conn();
    write_batch(&mut m, 2, 20);
    drop(m);
    std::thread::sleep(Duration::from_millis(1500));
    master.kill();
    master.restart();
    replica.restart();
    let mut m = master.conn();
    let mut r = replica.conn();
    wait_link_up(&mut r);
    assert_same_dataset(&mut m, &mut r);
    assert_eq!(m.stat("sync_full"), 1);
}

/// A replica that was killed has an append-only file with more in it than
/// its last save: the saved position is not where its dataset is, and it
/// must not be used.
#[test]
fn a_killed_replica_with_an_aof_resyncs_in_full() {
    let master = Server::start(&[]);
    let port = master.port.to_string();
    let mut replica = Server::start(&["--replicaof", "127.0.0.1", &port, "--aof"]);
    let mut m = master.conn();
    let mut r = replica.conn();

    write_batch(&mut m, 0, 30);
    wait_link_up(&mut r);
    assert_same_dataset(&mut m, &mut r);
    // These reach the replica's AOF, after the save made at the first sync.
    write_batch(&mut m, 1, 30);
    assert_same_dataset(&mut m, &mut r);
    drop(r);
    std::thread::sleep(Duration::from_millis(1500));

    replica.kill();
    write_batch(&mut m, 2, 30);
    replica.restart();
    let mut r = replica.conn();
    wait_link_up(&mut r);
    assert_same_dataset(&mut m, &mut r);
    assert_eq!(m.stat("sync_full"), 2);
    assert_eq!(m.stat("sync_partial_ok"), 0);
}

/// A node that took writes of its own is no longer a position in its old
/// master's stream.
#[test]
fn a_replica_that_was_written_to_resyncs_in_full() {
    let master = Server::start(&[]);
    let port = master.port.to_string();
    let replica = Server::start(&["--replicaof", "127.0.0.1", &port]);
    let (mut m, mut r) = (master.conn(), replica.conn());

    write_batch(&mut m, 0, 20);
    wait_link_up(&mut r);
    assert_same_dataset(&mut m, &mut r);

    assert!(r.cmd(&["REPLICAOF", "NO", "ONE"]).ok());
    assert!(r.cmd(&["SET", "written-while-master", "x"]).ok());
    assert!(r.cmd(&["INCR", "counter"]).i() > 0);
    // The old master never empties here, so the dataset guard has no say.
    assert!(r.cmd(&["REPLICAOF", "127.0.0.1", &port]).ok());
    wait_link_up(&mut r);
    assert_same_dataset(&mut m, &mut r);
    assert_eq!(r.cmd(&["EXISTS", "written-while-master"]).i(), 0);
    assert_eq!(m.stat("sync_full"), 2);
    assert_eq!(m.stat("sync_partial_ok"), 0);
}

/// `repl-diskless-load`: by default the local dataset is dropped before the
/// master's is loaded, so a full resync needs memory for one dataset; with
/// `swapdb` the old one is kept until the new one is in. Either way the
/// replica ends up with the master's data and nothing else — and either way
/// the guard against an empty master is asked first.
#[test]
fn a_full_resync_replaces_the_local_dataset_in_either_load_mode() {
    for mode in ["disabled", "swapdb"] {
        let master = Server::start(&[]);
        let port = master.port.to_string();
        let replica = Server::start(&["--repl-diskless-load", mode]);
        let (mut m, mut r) = (master.conn(), replica.conn());

        for i in 0..200 {
            assert!(r.cmd(&["SET", &format!("local:{i}"), "x"]).ok());
        }
        write_batch(&mut m, 0, 30);
        assert!(r.cmd(&["REPLICAOF", "127.0.0.1", &port]).ok());
        wait_link_up(&mut r);
        assert_same_dataset(&mut m, &mut r);
        assert_eq!(r.cmd(&["EXISTS", "local:0", "local:199"]).i(), 0, "{mode}");
        assert_eq!(r.info("persistence", "loading"), "0");
        assert_eq!(
            replica.log().contains("Dropped the 200 local keys"),
            mode == "disabled",
            "{mode}: {}",
            replica.log()
        );
        assert_eq!(
            r.cmd(&["CONFIG", "GET", "repl-diskless-load"]).arr()[1].s(),
            mode
        );
    }

    // An empty master is refused before anything is dropped.
    let master = Server::start(&[]);
    let port = master.port.to_string();
    let replica = Server::start(&[]);
    let mut r = replica.conn();
    for i in 0..50 {
        assert!(r.cmd(&["SET", &format!("local:{i}"), "x"]).ok());
    }
    assert!(r.cmd(&["REPLICAOF", "127.0.0.1", &port]).ok());
    wait_until("the refusal", || {
        r.info("replication", "replica_full_syncs_refused") != "0"
    });
    assert_eq!(r.cmd(&["DBSIZE"]).i(), 50);
    assert!(!replica.log().contains("Dropped the"), "{}", replica.log());
}

/// The same, with clients writing all the while and the master's heartbeat
/// going into the stream every second: the position a replica continues from
/// has to be exact whatever was in flight when the link was cut.
#[test]
fn partial_resyncs_under_load_lose_and_repeat_nothing() {
    // A backlog the writers cannot outrun. What is tested here is that the
    // replica continues from exactly where it was, not how much the master
    // keeps: with the default megabyte, four writers on a slow machine write
    // more than that while the link is down, and the master — rightly —
    // answers the next PSYNC with a full resync.
    let master = Server::start(&[
        "--repl-ping-replica-period",
        "1",
        "--repl-backlog-size",
        "268435456",
    ]);
    let relay = Relay::to(master.port);
    let replica = Server::start(&["--replicaof", "127.0.0.1", &relay.port.to_string()]);
    let (mut m, mut r) = (master.conn(), replica.conn());
    wait_link_up(&mut r);

    let stop = Arc::new(AtomicBool::new(false));
    let writers: Vec<_> = (0..4)
        .map(|w| {
            let stop = Arc::clone(&stop);
            let mut c = master.conn();
            std::thread::spawn(move || {
                let mut n = 0usize;
                while !stop.load(Ordering::Relaxed) {
                    assert!(c.cmd(&["INCR", "counter"]).i() > 0);
                    assert!(c.cmd(&["RPUSH", "list", &format!("{w}-{n}")]).i() > 0);
                    assert!(c.cmd(&["APPEND", &format!("log:{w}"), "x"]).i() > 0);
                    n += 1;
                    if n.is_multiple_of(50) {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                }
            })
        })
        .collect();

    for _ in 0..6 {
        std::thread::sleep(Duration::from_millis(300));
        relay.cut();
        wait_link_down(&mut r);
        std::thread::sleep(Duration::from_millis(200));
        relay.restore();
        wait_link_up(&mut r);
    }
    // Long enough for a heartbeat or two to go by.
    std::thread::sleep(Duration::from_millis(2500));
    stop.store(true, Ordering::Relaxed);
    for w in writers {
        w.join().unwrap();
    }

    assert_same_dataset(&mut m, &mut r);
    for w in 0..4 {
        let key = format!("log:{w}");
        assert_eq!(m.cmd(&["STRLEN", &key]), r.cmd(&["STRLEN", &key]));
    }
    assert_eq!(m.stat("sync_full"), 1, "{}", replica.log());
    assert_eq!(m.stat("sync_partial_ok"), 6);
}

/// A replica promoted while clients are already writing to it, as after a
/// `CLUSTER FAILOVER`: the replication session it ran as a replica noticed
/// the promotion up to 200 ms later, and then wrote its own last position
/// over the new master's offset. The writes of that moment stayed in the
/// backlog and reached the replicas continued from it, but the master's
/// offset no longer counted them. The next full sync restarted the backlog
/// at the shorter offset ("Backlog restarted at offset X (it ended at X+N)"),
/// and a replica that had been continued before it, asking after a restart
/// to continue from its own position, was sent the stream from N bytes too
/// far on: from the middle of a command. It parsed nothing of it, ever, and
/// went on acknowledging the position it was stuck at — online, lag 0, and
/// no write applied after its partial resync.
#[test]
fn a_replica_continued_by_a_promoted_master_gets_every_write_after_its_next_resync() {
    let master = Server::start(&[]);
    let master_port = master.port.to_string();
    let promoted = Server::start(&[
        "--replicaof",
        "127.0.0.1",
        &master_port,
        "--repl-backlog-size",
        "16777216",
    ]);
    let follower = Server::start(&["--replicaof", "127.0.0.1", &master_port]);
    let (mut m, mut p, mut f) = (master.conn(), promoted.conn(), follower.conn());
    write_batch(&mut m, 0, 50);
    wait_link_up(&mut p);
    wait_link_up(&mut f);
    assert_same_dataset(&mut m, &mut p);
    assert_same_dataset(&mut m, &mut f);

    // A client that writes to the replica until it is promoted, and goes on
    // writing to it afterwards, through the moment its old session notices.
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let stop = Arc::clone(&stop);
        let mut c = promoted.conn();
        std::thread::spawn(move || {
            let mut written = 0usize;
            while !stop.load(Ordering::Relaxed) {
                // Of different lengths, so that a position off by some
                // number of writes is not also a command boundary.
                let value = "v".repeat(written % 7 + 1);
                if c.cmd(&["SET", &format!("during:{written}"), &value]).ok() {
                    written += 1;
                }
            }
            written
        })
    };
    std::thread::sleep(Duration::from_millis(100));
    assert!(p.cmd(&["REPLICAOF", "NO", "ONE"]).ok());
    std::thread::sleep(Duration::from_millis(600));
    stop.store(true, Ordering::Relaxed);
    assert!(
        writer.join().unwrap() > 0,
        "no write reached the promoted node"
    );

    // The master's other replica follows the promoted one, through a link
    // the test can cut, and is continued: same stream, carried on.
    let relay = Relay::to(promoted.port);
    assert!(
        f.cmd(&["REPLICAOF", "127.0.0.1", &relay.port.to_string()])
            .ok()
    );
    wait_until("the follower to have every write", || {
        f.cmd(&["DBSIZE"]) == p.cmd(&["DBSIZE"])
    });
    assert_eq!(p.stat("sync_partial_ok"), 1, "{}", promoted.log());
    assert_eq!(p.stat("sync_full"), 0);

    // A full sync on the promoted node: a new replica.
    let third = Server::start(&["--replicaof", "127.0.0.1", &promoted.port.to_string()]);
    let mut t = third.conn();
    wait_link_up(&mut t);
    assert_eq!(p.stat("sync_full"), 1);

    // The follower loses its link, as when it is restarted, while more is
    // written than the promoted node's writes of that moment came to; and
    // asks to continue from where it is.
    relay.cut();
    wait_link_down(&mut f);
    let big = "b".repeat(256 * 1024);
    for i in 0..8 {
        assert!(p.cmd(&["SET", &format!("big:{i}"), &big]).ok());
    }
    write_batch(&mut p, 1, 30);
    relay.restore();
    wait_link_up(&mut f);
    // And writes made after it was continued reach it.
    write_batch(&mut p, 2, 30);

    assert_same_dataset(&mut p, &mut f);
    assert_same_dataset(&mut p, &mut t);
    assert_eq!(p.stat("sync_partial_ok"), 2, "{}", follower.log());
    assert_eq!(p.stat("sync_full"), 1, "{}", follower.log());
    assert!(
        !promoted.log().contains("Backlog restarted"),
        "{}",
        promoted.log()
    );
}

/// Several connections writing the same keys at once: the replica — and the
/// append-only file — must take those writes in the order the master did.
/// They did not; with six writers, hundreds of list elements were out of
/// place on the replica after every run, and appended strings differed.
#[cfg(feature = "lua")]
#[test]
fn concurrent_writes_to_one_key_are_replicated_in_the_order_they_were_applied() {
    let mut master = Server::start(&["--aof"]);
    let port = master.port.to_string();
    let replica = Server::start(&["--replicaof", "127.0.0.1", &port]);
    let (mut m, mut r) = (master.conn(), replica.conn());
    wait_link_up(&mut r);

    let script = "redis.call('RPUSH', KEYS[1], ARGV[1]) \
                  redis.call('APPEND', 'str', ARGV[2]) \
                  return redis.call('INCR', 'counter')";
    let writers: Vec<_> = (0..6)
        .map(|w| {
            let mut c = master.conn();
            std::thread::spawn(move || {
                for n in 0..800 {
                    let tag = format!("{w}-{n}");
                    if w < 2 {
                        // Two of them through a script, whose writes are
                        // ordered against everybody else's as a unit — and
                        // which wait for each other, and are waited for,
                        // rather than anyone being told BUSY.
                        let reply = c.cmd(&["EVAL", script, "1", "list", &tag, "s"]);
                        assert!(matches!(reply, V::Int(_)), "{reply:?}");
                    } else {
                        assert!(c.cmd(&["RPUSH", "list", &tag]).i() > 0);
                        assert!(c.cmd(&["APPEND", "str", &w.to_string()]).i() > 0);
                        assert!(c.cmd(&["INCR", "counter"]).i() > 0);
                    }
                    assert!(c.cmd(&["SET", "last", &tag]).ok());
                    assert!(c.cmd(&["MSET", "pair:a", &tag, "pair:b", &tag]).ok());
                }
            })
        })
        .collect();
    for w in writers {
        w.join().unwrap();
    }

    let probes: [&[&str]; 7] = [
        &["GET", "counter"],
        &["LRANGE", "list", "0", "-1"],
        &["GET", "str"],
        &["GET", "last"],
        &["MGET", "pair:a", "pair:b"],
        &["LLEN", "list"],
        &["DBSIZE"],
    ];
    assert_same_dataset(&mut m, &mut r);
    let on_master: Vec<V> = probes.iter().map(|p| m.cmd(p)).collect();
    for (probe, expected) in probes.iter().zip(&on_master) {
        // Lengths first: a mismatch in a 4800-element list is not something
        // to print.
        let got = r.cmd(probe);
        if let (V::Arr(a), V::Arr(b)) = (expected, &got) {
            let wrong = a.iter().zip(b).filter(|(x, y)| x != y).count();
            assert_eq!(
                wrong, 0,
                "{probe:?}: {wrong} positions differ on the replica"
            );
        }
        assert!(*expected == got, "{probe:?} differs on the replica");
    }

    // The append-only file holds the same order: kill the master, so that
    // nothing is saved on the way out, and let it replay.
    drop(m);
    std::thread::sleep(Duration::from_millis(1500));
    master.kill();
    master.restart();
    let mut m = master.conn();
    for (probe, expected) in probes.iter().zip(&on_master) {
        assert!(
            *expected == m.cmd(probe),
            "{probe:?} differs after AOF replay"
        );
    }
}

/// A master that goes
/// away in the middle of sending a full sync used to leave the replica with
/// no keys at all — and, with a master restarting in a loop, every replica
/// at once. The replica keeps what it has until a whole payload has arrived
/// and been read through.
#[test]
fn a_full_sync_cut_off_part_way_costs_the_replica_nothing() {
    let master = Server::start(&[]);
    let relay = Relay::to(master.port);
    let replica = Server::start(&[]);
    let (mut m, mut r) = (master.conn(), replica.conn());

    // A few megabytes on the master, so that there is a middle to cut in.
    let value = "x".repeat(400);
    for i in 0..15_000 {
        assert!(m.cmd(&["SET", &format!("big:{i}"), &value]).ok());
    }
    // And a dataset of its own on the replica.
    for i in 0..500 {
        assert!(r.cmd(&["SET", &format!("mine:{i}"), "kept"]).ok());
    }

    // Every attempt is cut after 300 KiB of a payload of several megabytes.
    relay.cut_after(300 * 1024);
    assert!(
        r.cmd(&["REPLICAOF", "127.0.0.1", &relay.port.to_string()])
            .ok()
    );
    // The master counts each full sync it starts; the replica retries
    // every second.
    wait_until("a few failed attempts", || m.stat("sync_full") >= 3);
    assert_eq!(r.info("replication", "master_link_status"), "down");
    assert_eq!(r.cmd(&["DBSIZE"]).i(), 500, "{}", replica.log());
    assert_eq!(r.cmd(&["GET", "mine:0"]).s(), "kept");
    assert!(
        !replica.log().contains("Dropped the"),
        "the local dataset was dropped for a payload that never arrived: {}",
        replica.log()
    );

    // When a whole payload does arrive, it replaces the old dataset.
    relay.cut_after(u64::MAX);
    wait_link_up(&mut r);
    assert_eq!(r.cmd(&["DBSIZE"]).i(), 15_000);
    assert_eq!(r.cmd(&["EXISTS", "mine:0"]).i(), 0);
    assert!(replica.log().contains("Dropped the 500 local keys"));
}

/// On SIGTERM the
/// process must not exit before its final snapshot is on disk. It did, when
/// connections were arriving: each one let an accept worker notice the
/// shutdown flag, and once they all had, `main` returned.
#[cfg(unix)]
/// A second engine started in a running one's data directory waits for the
/// first to exit before it reads anything there (#303). It used to load the
/// dump the first was about to replace and could then carry on the first
/// one's replication stream with the older data. Now what it loads is the
/// first one's final save, everything that was acknowledged.
#[test]
fn a_second_engine_in_the_same_directory_waits_for_the_first() {
    let mut first = Server::start(&["--aof"]);
    let mut c = first.conn();
    write_batch(&mut c, 0, 100);
    assert!(c.cmd(&["SAVE"]).ok());

    let port = free_port();
    let mut second = kividb(&first.dir, port, &["--aof".to_string()]);
    wait_until("the second engine to wait for the directory", || {
        first.log().contains("is in use by another process")
    });
    assert!(
        TcpStream::connect(("127.0.0.1", port)).is_err(),
        "the second engine started while the first had the directory:\n{}",
        first.log()
    );

    // Written after the second engine started: in neither the dump nor the
    // log it would have read then.
    write_batch(&mut c, 1, 100);
    let keys = c.cmd(&["DBSIZE"]).i();
    let counter = c.cmd(&["GET", "counter"]);
    drop(c);
    first.shutdown();

    let mut c = Conn::connect(port);
    assert_eq!(c.cmd(&["DBSIZE"]).i(), keys, "{}", first.log());
    assert_eq!(c.cmd(&["GET", "counter"]), counter);
    assert!(first.log().contains("The data directory is free"));
    drop(c);
    let _ = second.kill();
    let _ = second.wait();
}

#[test]
fn sigterm_waits_for_the_final_snapshot() {
    // The default I/O model, which on Linux is the one this was seen with.
    let dir = tempfile::tempdir().unwrap().keep();
    let port = free_port();
    let log = std::fs::File::create(dir.join("server.log")).unwrap();
    let spawn = |dir: &Path| {
        Command::new(env!("CARGO_BIN_EXE_kividb"))
            .current_dir(dir)
            .args([
                "--port",
                &port.to_string(),
                "--bind",
                "127.0.0.1",
                "--threads",
                "4",
                "--configfile",
                "kividb.conf",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(log.try_clone().unwrap())
            .spawn()
            .expect("could not start kividb")
    };
    let mut child = spawn(&dir);
    let mut c = Conn::connect(port);

    // Enough that writing it out takes a moment.
    let value = "v".repeat(200);
    let mut pipeline = Vec::new();
    let total = 300_000;
    for i in 0..total {
        let key = format!("k{i}");
        pipeline.extend_from_slice(
            format!(
                "*3\r\n$3\r\nSET\r\n${}\r\n{key}\r\n${}\r\n{value}\r\n",
                key.len(),
                value.len()
            )
            .as_bytes(),
        );
        if pipeline.len() > 1 << 20 || i == total - 1 {
            c.writer.write_all(&pipeline).unwrap();
            pipeline.clear();
        }
    }
    for _ in 0..total {
        assert!(c.read().ok());
    }
    assert_eq!(c.cmd(&["DBSIZE"]).i(), total as i64);

    // Connections keep arriving while it shuts down — as probes and
    // reconnecting clients do.
    let stop = Arc::new(AtomicBool::new(false));
    let knockers: Vec<_> = (0..8)
        .map(|_| {
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    if let Ok(mut s) = TcpStream::connect(("127.0.0.1", port)) {
                        let _ = s.write_all(b"*1\r\n$4\r\nPING\r\n");
                    }
                    std::thread::sleep(Duration::from_millis(2));
                }
            })
        })
        .collect();
    std::thread::sleep(Duration::from_millis(100));

    unsafe {
        libc::kill(child.id() as libc::pid_t, libc::SIGTERM);
    }
    let status = child.wait().unwrap();
    stop.store(true, Ordering::Relaxed);
    for k in knockers {
        k.join().unwrap();
    }
    let output = std::fs::read_to_string(dir.join("server.log")).unwrap();
    assert!(status.success(), "{output}");
    assert!(
        output.contains("Snapshot saved") && output.contains("Goodbye"),
        "the process ended before its snapshot was written:\n{output}"
    );

    // And what it saved is everything.
    let mut child = spawn(&dir);
    let mut c = Conn::connect(port);
    assert_eq!(c.cmd(&["DBSIZE"]).i(), total as i64);
    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A replica stopped with SIGTERM while its master writes to it, keys
/// expiring among the writes, exits with its final save (#308). Its shutdown
/// closed the command gate and then waited for the lock its applier holds
/// while it runs a command of the stream; the applier, holding it, waited at
/// the closed gate. A third of such stops under load never ended.
#[cfg(unix)]
#[test]
fn a_replica_stopped_under_writes_exits_with_its_final_save() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    let master = Server::start(&[]);
    let port = master.port.to_string();
    let stop = Arc::new(AtomicBool::new(false));
    let writers: Vec<_> = (0..4)
        .map(|w| {
            let (stop, port) = (Arc::clone(&stop), master.port);
            std::thread::spawn(move || {
                let mut c = Conn::connect(port);
                let mut n = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    // A pipeline of 50: keys that expire in 20 ms, counters
                    // and hashes.
                    let mut buf = Vec::new();
                    for _ in 0..50 {
                        n += 1;
                        let (key, cmd): (String, Vec<String>) = match n % 3 {
                            0 => (
                                format!("x:{w}:{n}"),
                                vec![
                                    "SET".into(),
                                    String::new(),
                                    "v".into(),
                                    "PX".into(),
                                    "20".into(),
                                ],
                            ),
                            1 => (
                                format!("c:{w}:{}", n % 100),
                                vec!["INCR".into(), String::new()],
                            ),
                            _ => (
                                format!("h:{w}:{}", n % 100),
                                vec!["HSET".into(), String::new(), format!("f{n}"), "v".into()],
                            ),
                        };
                        buf.extend_from_slice(format!("*{}\r\n", cmd.len()).as_bytes());
                        for (i, arg) in cmd.iter().enumerate() {
                            let arg = if i == 1 { &key } else { arg };
                            buf.extend_from_slice(
                                format!("${}\r\n{arg}\r\n", arg.len()).as_bytes(),
                            );
                        }
                    }
                    if c.writer.write_all(&buf).is_err() {
                        return;
                    }
                    for _ in 0..50 {
                        c.read();
                    }
                }
            })
        })
        .collect();

    for round in 0..5 {
        let mut replica = Server::start(&["--replicaof", "127.0.0.1", &port]);
        let mut r = replica.conn();
        wait_link_up(&mut r);
        drop(r);
        std::thread::sleep(Duration::from_millis(500));
        unsafe {
            libc::kill(replica.child.id() as libc::pid_t, libc::SIGTERM);
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        while replica.child.try_wait().unwrap().is_none() {
            assert!(
                Instant::now() < deadline,
                "round {round}: the replica did not exit after SIGTERM:\n{}",
                replica.log()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        let log = replica.log();
        assert!(
            log.to_lowercase().contains("writing final snapshot") && log.contains("Goodbye."),
            "round {round}: no final save:\n{log}"
        );
        assert!(replica.dir.join("dump.kdb.repl").exists(), "round {round}");
    }
    stop.store(true, Ordering::Relaxed);
    for w in writers {
        w.join().unwrap();
    }
}

/// A library loaded with `FUNCTION LOAD` is part of the dataset, which Redis
/// keeps in its RDB file and AOF. KiviDB's snapshot had no place for one, so
/// each save dropped every library: `SAVE` and a restart, `SHUTDOWN` with the
/// AOF on (the log is rebased onto the final snapshot), and a replica's full
/// sync (#70).
#[cfg(feature = "lua")]
#[test]
fn function_libraries_survive_a_restart_and_reach_a_replica_in_a_full_sync() {
    const LIBRARY: &str = "#!lua name=mylib\n\
        redis.register_function{function_name='seven', \
        callback=function() return 7 end, flags={'no-writes'}}";

    for aof in [false, true] {
        let mut server = Server::start(if aof { &["--aof"] } else { &[] });
        let mut c = server.conn();
        assert_eq!(c.cmd(&["FUNCTION", "LOAD", LIBRARY]).s(), "mylib");
        if !aof {
            assert!(c.cmd(&["SAVE"]).ok());
        }
        drop(c);
        server.shutdown();
        server.restart();
        let mut c = server.conn();
        assert_eq!(
            c.cmd(&["FCALL_RO", "seven", "0"]),
            V::Int(7),
            "aof={aof}: {}",
            server.log()
        );
        if !aof {
            // And from the same snapshot given as `--preload-file`.
            let file = format!("rdb:{}", server.dir.join("dump.kdb").display());
            let preloaded = Server::start(&["--preload-file", &file]);
            assert_eq!(
                preloaded.conn().cmd(&["FCALL_RO", "seven", "0"]),
                V::Int(7),
                "{}",
                preloaded.log()
            );
        }
    }

    // A replica's libraries are its master's after a full sync: the ones it
    // had are gone, and a master without any leaves it none, as in Redis.
    let master = Server::start(&[]);
    let mut m = master.conn();
    assert_eq!(m.cmd(&["FUNCTION", "LOAD", LIBRARY]).s(), "mylib");
    let replica = Server::start(&[]);
    let mut r = replica.conn();
    let own = "#!lua name=own\n\
        redis.register_function{function_name='eight', \
        callback=function() return 8 end, flags={'no-writes'}}";
    assert_eq!(r.cmd(&["FUNCTION", "LOAD", own]).s(), "own");
    let port = master.port.to_string();
    assert!(r.cmd(&["REPLICAOF", "127.0.0.1", &port]).ok());
    wait_link_up(&mut r);
    assert_eq!(
        r.cmd(&["FCALL_RO", "seven", "0"]),
        V::Int(7),
        "{}",
        replica.log()
    );
    assert!(matches!(r.cmd(&["FCALL_RO", "eight", "0"]), V::Err(_)));

    let bare = Server::start(&[]);
    let port = bare.port.to_string();
    assert!(r.cmd(&["REPLICAOF", "127.0.0.1", &port]).ok());
    wait_until("the full sync from a master without libraries", || {
        r.cmd(&["FUNCTION", "LIST"]).arr().is_empty()
    });
}

/// A Redis `dump.rdb` with function libraries: they are loaded at startup
/// and from `--preload-file`, and the functions run (#71). The file comes
/// from Redis 8.10.2: library `seven` with a read-only `seven`, and
/// `counter` with `incr_by_two` and a read-only `peek`.
#[cfg(feature = "lua")]
#[test]
fn function_libraries_load_from_a_redis_dump_rdb() {
    let fixture = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/redis-8.10-functions.rdb"
    );

    let dir = tempfile::tempdir().unwrap().keep();
    std::fs::copy(fixture, dir.join("dump.rdb")).unwrap();
    let port = free_port();
    let server = Server {
        port,
        child: kividb(&dir, port, &[]),
        dir,
        args: Vec::new(),
        env: Vec::new(),
    };
    let mut c = server.conn();
    assert_eq!(
        c.cmd(&["FCALL_RO", "seven", "0"]),
        V::Int(7),
        "{}",
        server.log()
    );
    assert_eq!(c.cmd(&["FCALL", "incr_by_two", "1", "n"]), V::Int(2));
    assert_eq!(c.cmd(&["FCALL_RO", "peek", "1", "s"]).s(), "hello");

    let preload = format!("rdb:{fixture}");
    let preloaded = Server::start(&["--preload-file", &preload]);
    assert_eq!(
        preloaded.conn().cmd(&["FCALL_RO", "seven", "0"]),
        V::Int(7),
        "{}",
        preloaded.log()
    );
}

/// Send a command and do not wait for its reply.
fn send_only(c: &mut Conn, args: &[&str]) {
    let mut buf = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        buf.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
    }
    c.writer.write_all(&buf).unwrap();
}

/// The keys of database `db`, sorted.
fn keys_of_db(c: &mut Conn, db: &str) -> Vec<String> {
    assert!(c.cmd(&["SELECT", db]).ok());
    let mut keys: Vec<String> = c
        .cmd(&["KEYS", "*"])
        .arr()
        .iter()
        .map(|key| key.s().to_string())
        .collect();
    keys.sort();
    keys
}

/// A database-0 write on its way when a client first selects another
/// database reaches the replica, and the AOF, in database 0 (#423).
///
/// Until a client leaves database 0, a write goes out with no `SELECT` of
/// its own. The first `SELECT 3` switches every write to carrying one, and
/// has to wait out the writes on their way first. It did not: its cut was
/// taken inside the write gate, where a cut is none, so its plain
/// `SELECT 3` went out ahead of the write, and the replica and a replay of
/// the AOF applied that write in database 3. The test hook holds the write
/// between being encoded and being queued.
#[test]
fn a_write_on_its_way_when_a_client_first_selects_another_database_stays_in_db_0() {
    let mut master =
        Server::start_with_env(&["--aof"], &[("KIVIDB_TEST_BARE_WRITE_DELAY_MS", "1500")]);
    let port = master.port.to_string();
    let replica = Server::start(&["--replicaof", "127.0.0.1", &port]);
    let mut r = replica.conn();
    wait_link_up(&mut r);
    let mut m = master.conn();
    // The first write after the replica came goes out with a `SELECT 0`;
    // the stream is in database 0 after it, and the next is bare.
    let started = Instant::now();
    assert!(m.cmd(&["SET", "first", "0"]).ok());
    assert!(
        started.elapsed() < Duration::from_millis(1000),
        "the first write was held: the stream was in database 0 already"
    );

    let mut a = master.conn();
    let mut b = master.conn();
    send_only(&mut b, &["SET", "j", "0"]);
    std::thread::sleep(Duration::from_millis(300));
    send_only(&mut a, &["SELECT", "3"]);
    assert!(b.read().ok());
    assert!(a.read().ok());
    assert!(a.cmd(&["SET", "k", "3"]).ok());
    assert!(b.cmd(&["SET", "j2", "0"]).ok());

    let offset: u64 = m.info("replication", "master_repl_offset").parse().unwrap();
    wait_until("the replica to catch up", || {
        r.info("replication", "slave_repl_offset")
            .parse::<u64>()
            .is_ok_and(|applied| applied >= offset)
    });
    let db0 = ["first", "j", "j2"].map(String::from).to_vec();
    assert_eq!(keys_of_db(&mut r, "3"), ["k"], "{}", replica.log());
    assert_eq!(keys_of_db(&mut r, "0"), db0);

    // From the AOF alone: every write on disk, then no final save.
    assert_eq!(b.cmd(&["WAITAOF", "1", "0", "5000"]).arr()[0].i(), 1);
    drop((m, a, b));
    master.kill();
    master.restart();
    let mut m = master.conn();
    assert_eq!(keys_of_db(&mut m, "3"), ["k"], "{}", master.log());
    assert_eq!(keys_of_db(&mut m, "0"), db0);
}
