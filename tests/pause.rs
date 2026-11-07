// CLIENT PAUSE and CLIENT UNBLOCK (Redis 8.10's unit/pause): what a pause
// held is let go before anything else runs once the pause is ended.
//
// A pause is the process's, not a server's: a test that pauses holds every
// other test's writes, so these have a binary of their own.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::{Duration, Instant};

struct Conn {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

#[derive(Debug, PartialEq)]
enum V {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Vec<u8>),
    Array(Vec<V>),
    Nil,
}

impl Conn {
    fn connect(port: u16) -> Self {
        let stream = (0..40)
            .find_map(|_| {
                thread::sleep(Duration::from_millis(50));
                TcpStream::connect(("127.0.0.1", port)).ok()
            })
            .expect("could not connect to test server");
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let reader = BufReader::new(stream.try_clone().unwrap());
        Self { stream, reader }
    }

    fn cmd(&mut self, args: &[&str]) -> V {
        self.send(args);
        self.read()
    }

    /// `args`, its reply left to read.
    fn send(&mut self, args: &[&str]) {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            buf.extend_from_slice(format!("${}\r\n{arg}\r\n", arg.len()).as_bytes());
        }
        self.stream.write_all(&buf).unwrap();
    }

    fn read(&mut self) -> V {
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        let line = line.trim_end_matches("\r\n");
        match line.as_bytes().first() {
            Some(b'+') => V::Simple(line[1..].to_string()),
            Some(b'-') => V::Error(line[1..].to_string()),
            Some(b':') => V::Int(line[1..].parse().unwrap()),
            Some(b'$') => {
                let len: i64 = line[1..].parse().unwrap();
                if len < 0 {
                    return V::Nil;
                }
                let mut data = vec![0u8; len as usize + 2];
                self.reader.read_exact(&mut data).unwrap();
                data.truncate(len as usize);
                V::Bulk(data)
            }
            Some(b'*') => {
                let count: i64 = line[1..].parse().unwrap();
                if count < 0 {
                    return V::Nil;
                }
                V::Array((0..count).map(|_| self.read()).collect())
            }
            _ => panic!("unexpected RESP: {line:?}"),
        }
    }

    fn ok(&mut self, args: &[&str]) {
        assert_eq!(self.cmd(args), V::Simple("OK".into()), "{args:?}");
    }
}

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

/// A port nothing listens on, outside the range handed to outgoing
/// connections.
fn free_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(0);
    loop {
        let step = NEXT.fetch_add(1, Ordering::Relaxed);
        let from = (std::process::id() as u16).wrapping_mul(37);
        let port = 10_000 + from.wrapping_add(step) % 20_000;
        if port_is_free(port) {
            return port;
        }
    }
}

/// Held by each test: the pause, and the count of blocked clients, are the
/// process's.
fn one_pause_at_a_time() -> std::sync::MutexGuard<'static, ()> {
    static ONE: std::sync::Mutex<()> = std::sync::Mutex::new(());
    ONE.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn start_server() -> Conn {
    let port = free_port();
    thread::spawn(move || {
        let config = kividb::config::Config {
            port,
            bind: "127.0.0.1".to_string(),
            threads: 2,
            aof: false,
            databases: 16,
            loglevel: "error".to_string(),
            io_model: kividb::config::IoModel::Tokio,
            configfile: "./kividb.conf".to_string(),
            ..Default::default()
        };
        kividb::run_for_test(config);
    });
    Conn::connect(port)
}

/// Waits until `INFO` counts `n` blocked clients.
fn wait_blocked(c: &mut Conn, n: u64) {
    let want = format!("blocked_clients:{n}");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let V::Bulk(info) = c.cmd(&["INFO", "clients"]) else {
            panic!("INFO is a bulk string");
        };
        if String::from_utf8(info)
            .unwrap()
            .lines()
            .any(|l| l.trim() == want)
        {
            return;
        }
        assert!(Instant::now() < deadline, "never {want}");
        thread::sleep(Duration::from_millis(1));
    }
}

const BLPOP: &[u8] = b"*3\r\n$5\r\nBLPOP\r\n$6\r\nnolist\r\n$1\r\n0\r\n";

/// A `BLPOP` the pause held runs as soon as `CLIENT UNPAUSE` ends the
/// pause, as in Redis: a `CLIENT UNBLOCK` sent straight after finds it
/// blocked on its key. While the pause lasts, there is nothing to unblock.
#[test]
fn client_unblock_after_unpause_finds_the_held_client_blocked() {
    let _one = one_pause_at_a_time();
    let mut c = start_server();
    let port = c.stream.peer_addr().unwrap().port();
    let mut held = Conn::connect(port);
    let V::Int(id) = held.cmd(&["CLIENT", "ID"]) else {
        panic!("CLIENT ID is a number");
    };
    let id = id.to_string();

    c.ok(&["CLIENT", "PAUSE", "100000", "WRITE"]);
    held.stream.write_all(BLPOP).unwrap();
    wait_blocked(&mut c, 1);
    assert_eq!(c.cmd(&["CLIENT", "UNBLOCK", &id, "TIMEOUT"]), V::Int(0));

    for _ in 0..20 {
        // Straight after: no time for a poll to see the pause gone.
        c.ok(&["CLIENT", "UNPAUSE"]);
        assert_eq!(c.cmd(&["CLIENT", "UNBLOCK", &id, "TIMEOUT"]), V::Int(1));
        assert_eq!(held.read(), V::Nil);
        c.ok(&["CLIENT", "PAUSE", "100000", "WRITE"]);
        held.stream.write_all(BLPOP).unwrap();
        wait_blocked(&mut c, 1);
    }
    c.ok(&["CLIENT", "UNPAUSE"]);
    assert_eq!(c.cmd(&["CLIENT", "UNBLOCK", &id, "ERROR"]), V::Int(1));
    assert!(matches!(held.read(), V::Error(e) if e.starts_with("UNBLOCKED")));

    // Ended, the pause holds nothing up: a write goes through at once.
    let started = Instant::now();
    c.ok(&["SET", "k", "v"]);
    assert!(started.elapsed() < Duration::from_millis(50));
}

/// The `INFO replication` field `field` of `c`.
fn repl_field(c: &mut Conn, field: &str) -> String {
    let V::Bulk(info) = c.cmd(&["INFO", "replication"]) else {
        panic!("INFO is a bulk string");
    };
    String::from_utf8(info)
        .unwrap()
        .lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")).map(str::to_string))
        .unwrap_or_default()
}

/// `SELECT` writes nothing, and `CLIENT PAUSE WRITE` does not hold it, as
/// in Redis (#411): it, and a transaction that only selects and reads, are
/// answered at once. Nor does it move the replication offset while writes
/// are paused: a write after the pause still reaches the replica in the
/// database selected.
#[test]
fn client_pause_write_does_not_hold_select() {
    let _one = one_pause_at_a_time();
    let mut c = start_server();
    let port = c.stream.peer_addr().unwrap().port();
    // No PING of the replicas to move the offset meanwhile.
    c.ok(&["CONFIG", "SET", "repl-ping-replica-period", "3600"]);
    let replica_port = free_port();
    thread::spawn(move || {
        let config = kividb::config::Config {
            port: replica_port,
            bind: "127.0.0.1".to_string(),
            threads: 2,
            databases: 16,
            loglevel: "error".to_string(),
            io_model: kividb::config::IoModel::Tokio,
            replicaof: Some(vec!["127.0.0.1".to_string(), port.to_string()]),
            replica_announced_port: replica_port,
            configfile: "./kividb.conf".to_string(),
            ..Default::default()
        };
        kividb::run_for_test(config);
    });
    let mut replica = Conn::connect(replica_port);
    c.ok(&["SET", "a", "1"]);
    let deadline = Instant::now() + Duration::from_secs(20);
    while replica.cmd(&["GET", "a"]) != V::Bulk(b"1".to_vec()) {
        assert!(Instant::now() < deadline, "the replica never caught up");
        thread::sleep(Duration::from_millis(20));
    }

    let mut other = Conn::connect(port);
    let mut writer = Conn::connect(port);
    c.ok(&["CLIENT", "PAUSE", "5000", "WRITE"]);
    // Read once writes are paused: nothing may move it from here on.
    let offset = repl_field(&mut c, "master_repl_offset");
    let started = Instant::now();
    other.ok(&["SELECT", "1"]);
    assert_eq!(other.cmd(&["GET", "a"]), V::Nil);
    other.ok(&["MULTI"]);
    assert_eq!(other.cmd(&["SELECT", "2"]), V::Simple("QUEUED".into()));
    assert_eq!(other.cmd(&["GET", "a"]), V::Simple("QUEUED".into()));
    assert_eq!(
        other.cmd(&["EXEC"]),
        V::Array(vec![V::Simple("OK".into()), V::Nil])
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "SELECT was held for {:?}",
        started.elapsed()
    );
    assert_eq!(repl_field(&mut c, "master_repl_offset"), offset);
    // A write is still held.
    writer
        .stream
        .write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n1\r\n")
        .unwrap();
    wait_blocked(&mut c, 1);
    c.ok(&["CLIENT", "UNPAUSE"]);
    assert_eq!(writer.read(), V::Simple("OK".into()));

    // In database 2, where `other` is now.
    other.ok(&["SET", "c", "2"]);
    replica.ok(&["SELECT", "2"]);
    let deadline = Instant::now() + Duration::from_secs(20);
    while replica.cmd(&["GET", "c"]) != V::Bulk(b"2".to_vec()) {
        assert!(Instant::now() < deadline, "SET c never reached db 2");
        thread::sleep(Duration::from_millis(20));
    }
    replica.ok(&["SELECT", "0"]);
    assert_eq!(replica.cmd(&["GET", "c"]), V::Nil);
}

/// `SCRIPT LOAD` and `SCRIPT FLUSH` change the script cache, not the
/// dataset, and `CLIENT PAUSE WRITE` does not hold them, as in Redis
/// (`NOSCRIPT`, `STALE`), alone or in a transaction (#518). They were held
/// for the whole pause, and Redis's `unit/pause` hung on its `SCRIPT LOAD`
/// of a `no-writes` script. They are kept out of the stream meanwhile: the
/// replication offset does not move. `FUNCTION LOAD`, a `WRITE` command, is
/// still held. Once the pause has ended, a `SCRIPT LOAD` reaches the
/// replica again.
#[cfg(feature = "lua")]
#[test]
fn client_pause_write_does_not_hold_script_load_or_flush() {
    let _one = one_pause_at_a_time();
    let mut c = start_server();
    let port = c.stream.peer_addr().unwrap().port();
    // No PING of the replicas to move the offset meanwhile.
    c.ok(&["CONFIG", "SET", "repl-ping-replica-period", "3600"]);
    let replica_port = free_port();
    thread::spawn(move || {
        let config = kividb::config::Config {
            port: replica_port,
            bind: "127.0.0.1".to_string(),
            threads: 2,
            databases: 16,
            loglevel: "error".to_string(),
            io_model: kividb::config::IoModel::Tokio,
            replicaof: Some(vec!["127.0.0.1".to_string(), port.to_string()]),
            replica_announced_port: replica_port,
            configfile: "./kividb.conf".to_string(),
            ..Default::default()
        };
        kividb::run_for_test(config);
    });
    let mut replica = Conn::connect(replica_port);
    c.ok(&["SET", "x", "y"]);
    let deadline = Instant::now() + Duration::from_secs(20);
    while replica.cmd(&["GET", "x"]) != V::Bulk(b"y".to_vec()) {
        assert!(Instant::now() < deadline, "the replica never caught up");
        thread::sleep(Duration::from_millis(20));
    }

    let mut rr = Conn::connect(port);
    // A held command would time this read out, well inside the pause.
    rr.stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let mut function_loader = Conn::connect(port);
    c.ok(&["CLIENT", "PAUSE", "100000", "WRITE"]);
    // Read once writes are paused: nothing may move it from here on.
    let offset = repl_field(&mut c, "master_repl_offset");
    let started = Instant::now();

    // As Redis's unit/pause, "Test RO scripts are not blocked by pause RO".
    let V::Bulk(sha) = rr.cmd(&["SCRIPT", "LOAD", "#!lua flags=no-writes\nreturn 2"]) else {
        panic!("SCRIPT LOAD answers the sha");
    };
    let sha = String::from_utf8(sha).unwrap();
    assert_eq!(rr.cmd(&["EVALSHA", &sha, "0"]), V::Int(2));
    let V::Bulk(plain) = rr.cmd(&["SCRIPT", "LOAD", "return 3"]) else {
        panic!("SCRIPT LOAD answers the sha");
    };
    let plain = String::from_utf8(plain).unwrap();
    assert_eq!(
        rr.cmd(&["SCRIPT", "EXISTS", &sha, &plain]),
        V::Array(vec![V::Int(1), V::Int(1)])
    );
    rr.ok(&["MULTI"]);
    assert_eq!(
        rr.cmd(&["SCRIPT", "LOAD", "return 4"]),
        V::Simple("QUEUED".into())
    );
    assert!(matches!(rr.cmd(&["EXEC"]), V::Array(r) if r.len() == 1));
    rr.ok(&["SCRIPT", "FLUSH"]);
    rr.ok(&["SCRIPT", "FLUSH", "ASYNC"]);
    assert_eq!(
        rr.cmd(&["SCRIPT", "EXISTS", &sha]),
        V::Array(vec![V::Int(0)])
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "SCRIPT was held for {:?}",
        started.elapsed()
    );
    assert_eq!(repl_field(&mut c, "master_repl_offset"), offset);

    // `FUNCTION LOAD` is a write in Redis, and is held.
    function_loader.send(&[
        "FUNCTION",
        "LOAD",
        "#!lua name=lib518\nredis.register_function('f518', function() return 1 end)",
    ]);
    wait_blocked(&mut c, 1);
    c.ok(&["CLIENT", "UNPAUSE"]);
    assert_eq!(function_loader.read(), V::Bulk(b"lib518".to_vec()));

    // With the pause over, `SCRIPT LOAD` is propagated again. (The script
    // cache is the process's, which the replica here shares: the offset
    // is what tells.)
    let before = repl_field(&mut c, "master_repl_offset");
    assert!(matches!(
        rr.cmd(&["SCRIPT", "LOAD", "return 5"]),
        V::Bulk(_)
    ));
    assert_ne!(repl_field(&mut c, "master_repl_offset"), before);
}

/// A pause of writers holds `GEORADIUS … STORE` and `GEORADIUSBYMEMBER …
/// STOREDIST`, alone and in a transaction, as Redis does (#407). They were
/// judged by their names, a read's, and ran: a failover that pauses writes
/// to let its replica catch up lost what they stored.
#[test]
fn a_pause_of_writers_holds_georadius_store() {
    let _one = one_pause_at_a_time();
    let mut c = start_server();
    let port = c.stream.peer_addr().unwrap().port();
    let places = ["GEOADD", "places", "13.361389", "38.115556", "Palermo"];
    assert_eq!(c.cmd(&places), V::Int(1));

    // Queued before the pause, which would hold it (as Redis does).
    let mut in_multi = Conn::connect(port);
    in_multi.ok(&["MULTI"]);
    let store = [
        "GEORADIUS",
        "places",
        "15",
        "37",
        "200",
        "km",
        "STORE",
        "near",
    ];
    assert_eq!(in_multi.cmd(&store), V::Simple("QUEUED".into()));

    c.ok(&["CLIENT", "PAUSE", "100000", "WRITE"]);
    in_multi.send(&["EXEC"]);
    wait_blocked(&mut c, 1);
    let mut alone = Conn::connect(port);
    alone.send(&[
        "GEORADIUSBYMEMBER",
        "places",
        "Palermo",
        "10",
        "km",
        "STOREDIST",
        "dist",
    ]);
    wait_blocked(&mut c, 2);
    let mut store_alone = Conn::connect(port);
    store_alone.send(&store);
    wait_blocked(&mut c, 3);
    // Held, not run: nothing is stored yet.
    assert_eq!(c.cmd(&["EXISTS", "near", "dist"]), V::Int(0));

    c.ok(&["CLIENT", "UNPAUSE"]);
    assert_eq!(in_multi.read(), V::Array(vec![V::Int(1)]));
    assert_eq!(alone.read(), V::Int(1));
    assert_eq!(store_alone.read(), V::Int(1));
    assert_eq!(c.cmd(&["EXISTS", "near", "dist"]), V::Int(2));
}
