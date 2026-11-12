// One time for a whole EXEC (#659).
//
// Redis takes one time snapshot for a transaction (`commandTimeSnapshot`,
// set by `call()` at the outermost level only): a key or hash field whose
// deadline falls while an EXEC runs is there for every command in it, or
// gone for every one. KiviDB read the clock for each queued command, so a
// transaction that ran across a deadline found the key in its first
// commands and not in its later ones.
//
// Each test puts a deadline inside an EXEC with a queued `DEBUG SLEEP`, on
// each I/O model, on a master and on a replica. A file of its own: the
// EXECs hold the process's command gate for the length of their sleep, which
// would hold up the tests of a shared file; and the tests here take turns.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

/// The tests take turns: each holds the process's command gate for a while.
static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

/// How long each key or field lives, and how long the EXEC sleeps: the
/// deadline falls inside the EXEC if the EXEC starts within `TTL_MS`.
const TTL_MS: u64 = 600;
const SLEEP: &str = "1.0";

// ── Minimal RESP client ───────────────────────────────────────────────────────

struct Conn {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

impl Conn {
    fn connect(port: u16) -> Self {
        let stream = (0..100)
            .find_map(|_| {
                thread::sleep(Duration::from_millis(50));
                TcpStream::connect(("127.0.0.1", port)).ok()
            })
            .expect("could not connect to test server");
        stream
            .set_read_timeout(Some(Duration::from_secs(15)))
            .unwrap();
        let reader = BufReader::new(stream.try_clone().unwrap());
        Self { stream, reader }
    }

    fn cmds(&mut self, args: &[&str]) -> Resp {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            buf.extend_from_slice(arg.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        self.stream.write_all(&buf).unwrap();
        self.read_value()
    }

    /// `MULTI`, each of `queued`, then `EXEC`: EXEC's reply.
    fn transaction(&mut self, queued: &[&[&str]]) -> Resp {
        assert_eq!(self.cmds(&["MULTI"]), Resp::Simple("OK".into()));
        for cmd in queued {
            assert_eq!(self.cmds(cmd), Resp::Simple("QUEUED".into()), "{cmd:?}");
        }
        self.cmds(&["EXEC"])
    }

    fn read_value(&mut self) -> Resp {
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        let line = line.trim_end_matches("\r\n");
        match line.as_bytes().first() {
            Some(b'+') => Resp::Simple(line[1..].to_string()),
            Some(b'-') => Resp::Error(line[1..].to_string()),
            Some(b':') => Resp::Int(line[1..].parse().unwrap()),
            Some(b'$') => {
                let len: i64 = line[1..].parse().unwrap();
                if len == -1 {
                    return Resp::Nil;
                }
                let mut data = vec![0u8; len as usize + 2];
                self.reader.read_exact(&mut data).unwrap();
                data.truncate(len as usize);
                Resp::Bulk(String::from_utf8(data).unwrap())
            }
            Some(b'*') => {
                let count: i64 = line[1..].parse().unwrap();
                if count == -1 {
                    return Resp::Nil;
                }
                Resp::Array((0..count).map(|_| self.read_value()).collect())
            }
            _ => panic!("unexpected RESP line: {line:?}"),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
enum Resp {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(String),
    Array(Vec<Resp>),
    Nil,
}

impl Resp {
    fn int(&self) -> i64 {
        match self {
            Resp::Int(n) => *n,
            _ => panic!("not an integer: {self:?}"),
        }
    }
    fn array(&self) -> &[Resp] {
        match self {
            Resp::Array(items) => items,
            _ => panic!("not an array: {self:?}"),
        }
    }
    fn bulk(&self) -> &str {
        match self {
            Resp::Bulk(s) => s,
            _ => panic!("not a bulk string: {self:?}"),
        }
    }
    /// A flat `HGETALL` reply's field names.
    fn fields(&self) -> Vec<&str> {
        self.array().iter().step_by(2).map(Resp::bulk).collect()
    }
}

fn ok() -> Resp {
    Resp::Simple("OK".into())
}

// ── Servers ───────────────────────────────────────────────────────────────────

/// Whether nothing has `port`: asked with a socket that is bound and never
/// listens (see tests/transaction.rs).
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

/// A free port outside the range the system hands out to outgoing
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

fn start_server(
    port: u16,
    io_model: &kividb::config::IoModel,
    replica_of: Option<u16>,
    dir: &std::path::Path,
) {
    let replicaof = replica_of.map(|master| vec!["127.0.0.1".to_string(), master.to_string()]);
    let dir = dir.to_string_lossy().into_owned();
    let io_model = io_model.clone();
    thread::spawn(move || {
        let config = kividb::config::Config {
            port,
            bind: "127.0.0.1".to_string(),
            threads: 2,
            aof: false,
            databases: 16,
            maxmemory: 0,
            loglevel: "error".to_string(),
            io_model,
            replicaof,
            dir,
            replica_announced_port: 0,
            requirepass: String::new(),
            aclfile: "".to_string(),
            cluster_enabled: false,
            configfile: "./kividb.conf".to_string(),
            ..Default::default()
        };
        kividb::run_for_test(config);
    });
}

/// Both I/O models where io_uring exists, or the one `KIVIDB_TEST_IO_MODEL`
/// names.
fn io_models() -> Vec<kividb::config::IoModel> {
    use kividb::config::IoModel;
    match std::env::var("KIVIDB_TEST_IO_MODEL").as_deref() {
        Ok("tokio") => vec![IoModel::Tokio],
        Ok("uring") => vec![IoModel::Uring],
        _ if cfg!(target_os = "linux") => vec![IoModel::Tokio, IoModel::Uring],
        _ => vec![IoModel::Tokio],
    }
}

/// A master and its replica, both on `io_model`, the replica in sync.
struct Pair {
    master: Conn,
    replica: Conn,
    _dir: tempfile::TempDir,
}

fn pair(io_model: &kividb::config::IoModel) -> Pair {
    let dir = tempfile::tempdir().unwrap();
    let (master_dir, replica_dir) = (dir.path().join("m"), dir.path().join("r"));
    std::fs::create_dir_all(&master_dir).unwrap();
    std::fs::create_dir_all(&replica_dir).unwrap();
    let master = free_port();
    start_server(master, io_model, None, &master_dir);
    let replica = free_port();
    start_server(replica, io_model, Some(master), &replica_dir);
    let mut m = Conn::connect(master);
    let r = Conn::connect(replica);
    assert_eq!(m.cmds(&["SET", "sync", "1"]), ok());
    caught_up(&mut m);
    Pair {
        master: m,
        replica: r,
        _dir: dir,
    }
}

/// Wait until the replica has everything the master has written.
fn caught_up(m: &mut Conn) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while m.cmds(&["WAIT", "1", "200"]).int() != 1 {
        assert!(Instant::now() < deadline, "the replica never caught up");
    }
}

/// Run `attempt` until the EXEC it times began before its deadline: it
/// returns whether it did (a slow machine can start one late, and then the
/// key is gone for the whole EXEC, which proves nothing). Every attempt
/// checks its replies agree with each other either way.
fn until_inside(mut attempt: impl FnMut(usize) -> bool) {
    for i in 0..5 {
        if attempt(i) {
            return;
        }
    }
    panic!("no EXEC began before its deadline in five tries");
}

/// The time to live of `TTL_MS`, as `SET`'s `PX` takes it.
fn ttl() -> String {
    TTL_MS.to_string()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// A key whose deadline falls inside an EXEC is there for all of it: every
/// read says so, `PTTL` says it has time left, and `TIME` does not move, as
/// in Redis 8.10. After the EXEC it is gone.
#[test]
fn a_key_is_there_for_the_whole_exec_its_deadline_falls_in() {
    let _turn = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    for io_model in io_models() {
        let dir = tempfile::tempdir().unwrap();
        let port = free_port();
        start_server(port, &io_model, None, dir.path());
        let mut c = Conn::connect(port);
        until_inside(|i| {
            let k = format!("k{i}");
            assert_eq!(c.cmds(&["SET", &k, "v", "PX", &ttl()]), ok());
            let exec = c.transaction(&[
                &["EXISTS", &k],
                &["GET", &k],
                &["TIME"],
                &["DEBUG", "SLEEP", SLEEP],
                &["EXISTS", &k],
                &["GET", &k],
                &["PTTL", &k],
                &["TIME"],
                &["EXISTS", &k],
            ]);
            let r = exec.array();
            assert_eq!(r.len(), 9, "{io_model:?} {r:?}");
            assert_eq!(r[3], ok());
            let alive = r[0].int();
            for (n, reply) in [(1, &r[1]), (5, &r[5])] {
                assert_eq!(
                    *reply,
                    if alive == 1 {
                        Resp::Bulk("v".into())
                    } else {
                        Resp::Nil
                    },
                    "{io_model:?} reply {n}: {r:?}"
                );
            }
            assert_eq!(r[4].int(), alive, "{io_model:?} {r:?}");
            assert_eq!(r[8].int(), alive, "{io_model:?} {r:?}");
            // `TIME` is the transaction's time, as Redis 8.10's (its cached
            // `server.ustime` is updated at the outermost call only).
            assert_eq!(r[2], r[7], "{io_model:?} TIME moved inside EXEC");
            if alive == 1 {
                let left = r[6].int();
                assert!(
                    left > 0 && left <= TTL_MS as i64,
                    "{io_model:?} PTTL {left}"
                );
                // The deadline did fall inside it.
                assert_eq!(c.cmds(&["EXISTS", &k]).int(), 0, "{io_model:?}");
            } else {
                assert_eq!(r[6].int(), -2, "{io_model:?} {r:?}");
            }
            alive == 1
        });
    }
}

/// A hash field whose deadline falls inside an EXEC is there for all of it:
/// `HGETALL` and `HEXISTS`, before and after, agree.
#[test]
fn a_hash_field_is_there_for_the_whole_exec_its_deadline_falls_in() {
    let _turn = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    for io_model in io_models() {
        let dir = tempfile::tempdir().unwrap();
        let port = free_port();
        start_server(port, &io_model, None, dir.path());
        let mut c = Conn::connect(port);
        until_inside(|i| {
            let h = format!("h{i}");
            assert_eq!(c.cmds(&["HSET", &h, "f", "1", "g", "2"]).int(), 2);
            let set = c.cmds(&["HPEXPIRE", &h, &ttl(), "FIELDS", "1", "f"]);
            assert_eq!(set.array()[0].int(), 1);
            let exec = c.transaction(&[
                &["HGETALL", &h],
                &["HEXISTS", &h, "f"],
                &["DEBUG", "SLEEP", SLEEP],
                &["HEXISTS", &h, "f"],
                &["HGET", &h, "f"],
                &["HPTTL", &h, "FIELDS", "1", "f"],
                &["HGETALL", &h],
            ]);
            let r = exec.array();
            assert_eq!(r.len(), 7, "{io_model:?} {r:?}");
            let alive = r[1].int();
            let fields = if alive == 1 {
                vec!["f", "g"]
            } else {
                vec!["g"]
            };
            assert_eq!(r[0].fields(), fields, "{io_model:?} {r:?}");
            assert_eq!(r[3].int(), alive, "{io_model:?} {r:?}");
            assert_eq!(r[6].fields(), fields, "{io_model:?} {r:?}");
            if alive == 1 {
                assert_eq!(r[4], Resp::Bulk("1".into()), "{io_model:?} {r:?}");
                let left = r[5].array()[0].int();
                assert!(
                    left > 0 && left <= TTL_MS as i64,
                    "{io_model:?} HPTTL {left}"
                );
                assert_eq!(c.cmds(&["HEXISTS", &h, "f"]).int(), 0, "{io_model:?}");
            }
            alive == 1
        });
    }
}

/// Writes inside an EXEC that a deadline falls in work on the key and the
/// field as they were when it began, and the replica ends with what the
/// master has: `INCR` then `PERSIST` keep the old value, as do `HINCRBY`
/// then `HPERSIST`. Read per command, the key and the field were gone by
/// the `INCR` and the `HINCRBY`, which started them again from nothing.
#[test]
fn writes_across_a_deadline_inside_exec_reach_the_replica_as_they_ran() {
    let _turn = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    for io_model in io_models() {
        let Pair {
            master: mut m,
            replica: mut r,
            _dir,
        } = pair(&io_model);
        until_inside(|i| {
            let (k, h) = (format!("wk{i}"), format!("wh{i}"));
            let started = Instant::now();
            assert_eq!(m.cmds(&["SET", &k, "5", "PX", &ttl()]), ok());
            assert_eq!(m.cmds(&["HSET", &h, "f", "1", "g", "2"]).int(), 2);
            let set = m.cmds(&["HPEXPIRE", &h, &ttl(), "FIELDS", "1", "f"]);
            assert_eq!(set.array()[0].int(), 1);
            let exec = m.transaction(&[
                &["EXISTS", &k],
                &["DEBUG", "SLEEP", SLEEP],
                &["INCR", &k],
                &["PERSIST", &k],
                &["HINCRBY", &h, "f", "1"],
                &["HPERSIST", &h, "FIELDS", "1", "f"],
            ]);
            let x = exec.array();
            assert_eq!(x.len(), 6, "{io_model:?} {x:?}");
            if x[0].int() == 0 {
                // Begun after the deadline: nothing to see.
                assert!(started.elapsed() > Duration::from_millis(TTL_MS / 2));
                return false;
            }
            assert_eq!(x[2].int(), 6, "{io_model:?} {x:?}");
            assert_eq!(x[3].int(), 1, "{io_model:?} {x:?}");
            assert_eq!(x[4].int(), 2, "{io_model:?} {x:?}");
            assert_eq!(x[5].array(), &[Resp::Int(1)], "{io_model:?} {x:?}");
            caught_up(&mut m);
            for (who, c) in [("master", &mut m), ("replica", &mut r)] {
                assert_eq!(c.cmds(&["GET", &k]), Resp::Bulk("6".into()), "{who}");
                assert_eq!(c.cmds(&["PTTL", &k]).int(), -1, "{who}");
                assert_eq!(c.cmds(&["HGET", &h, "f"]), Resp::Bulk("2".into()), "{who}");
                let ttl = c.cmds(&["HPTTL", &h, "FIELDS", "1", "f"]);
                assert_eq!(ttl.array(), &[Resp::Int(-1)], "{who} {io_model:?}");
                assert_eq!(c.cmds(&["HGET", &h, "g"]), Resp::Bulk("2".into()), "{who}");
            }
            true
        });
    }
}

/// On a replica, an EXEC that a key's and a field's deadlines fall in reads
/// them as there throughout; the master's `DEL` waits for the EXEC to end.
#[test]
fn a_replica_reads_one_time_for_the_whole_exec() {
    let _turn = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    for io_model in io_models() {
        let Pair {
            master: mut m,
            replica: mut r,
            _dir,
        } = pair(&io_model);
        until_inside(|i| {
            let (k, h) = (format!("rk{i}"), format!("rh{i}"));
            assert_eq!(m.cmds(&["SET", &k, "v", "PX", &ttl()]), ok());
            assert_eq!(m.cmds(&["HSET", &h, "f", "1", "g", "2"]).int(), 2);
            let set = m.cmds(&["HPEXPIRE", &h, &ttl(), "FIELDS", "1", "f"]);
            assert_eq!(set.array()[0].int(), 1);
            caught_up(&mut m);
            let exec = r.transaction(&[
                &["EXISTS", &k],
                &["HEXISTS", &h, "f"],
                &["DEBUG", "SLEEP", SLEEP],
                &["EXISTS", &k],
                &["GET", &k],
                &["HEXISTS", &h, "f"],
                &["HGETALL", &h],
            ]);
            let x = exec.array();
            assert_eq!(x.len(), 7, "{io_model:?} {x:?}");
            let alive = x[0].int();
            assert_eq!(x[1].int(), alive, "{io_model:?} {x:?}");
            assert_eq!(x[3].int(), alive, "{io_model:?} {x:?}");
            assert_eq!(
                x[4],
                if alive == 1 {
                    Resp::Bulk("v".into())
                } else {
                    Resp::Nil
                },
                "{io_model:?} {x:?}"
            );
            assert_eq!(x[5].int(), alive, "{io_model:?} {x:?}");
            let fields = if alive == 1 {
                vec!["f", "g"]
            } else {
                vec!["g"]
            };
            assert_eq!(x[6].fields(), fields, "{io_model:?} {x:?}");
            if alive == 1 {
                assert_eq!(r.cmds(&["EXISTS", &k]).int(), 0, "{io_model:?}");
                assert_eq!(r.cmds(&["HEXISTS", &h, "f"]).int(), 0, "{io_model:?}");
            }
            alive == 1
        });
    }
}

/// WATCH and a deadline, by the transaction's time as Redis has it
/// (`isWatchedKeyExpired`, by `commandTimeSnapshot`): a watched key whose
/// deadline falls inside the EXEC was alive when the EXEC began, so the EXEC
/// runs, and finds it throughout; one whose deadline fell before the EXEC
/// began refuses it, on the master and on a replica.
#[test]
fn a_watched_key_is_judged_by_the_time_its_exec_begins() {
    let _turn = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    for io_model in io_models() {
        let Pair {
            master: mut m,
            replica: mut r,
            _dir,
        } = pair(&io_model);

        // Inside: it runs.
        until_inside(|i| {
            let k = format!("wi{i}");
            assert_eq!(m.cmds(&["SET", &k, "v", "PX", &ttl()]), ok());
            assert_eq!(m.cmds(&["WATCH", &k]), ok());
            let exec = m.transaction(&[&["GET", &k], &["DEBUG", "SLEEP", SLEEP], &["GET", &k]]);
            if exec == Resp::Nil {
                // Begun after the deadline: refused, as it must be.
                return false;
            }
            let x = exec.array();
            assert_eq!(x[0], Resp::Bulk("v".into()), "{io_model:?} {x:?}");
            assert_eq!(x[2], Resp::Bulk("v".into()), "{io_model:?} {x:?}");
            true
        });

        // Before: refused, on the master and on a replica.
        let k = "wb";
        assert_eq!(m.cmds(&["SET", k, "v", "PX", "300"]), ok());
        caught_up(&mut m);
        for c in [&mut m, &mut r] {
            assert_eq!(c.cmds(&["WATCH", k]), ok());
        }
        thread::sleep(Duration::from_millis(500));
        for (who, c) in [("master", &mut m), ("replica", &mut r)] {
            let exec = c.transaction(&[&["GET", k]]);
            assert_eq!(exec, Resp::Nil, "{who} {io_model:?}");
        }
    }
}

/// A script run by an EXEC runs at the EXEC's time, as in Redis, where a
/// script inside a transaction is a nested call and reads the same
/// `commandTimeSnapshot`.
#[cfg(feature = "lua")]
#[test]
fn a_script_inside_exec_keeps_the_exec_time() {
    let _turn = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    for io_model in io_models() {
        let dir = tempfile::tempdir().unwrap();
        let port = free_port();
        start_server(port, &io_model, None, dir.path());
        let mut c = Conn::connect(port);
        let script = "return {redis.call('EXISTS', KEYS[1]), redis.call('TIME')[1]}";
        until_inside(|i| {
            let k = format!("sk{i}");
            assert_eq!(c.cmds(&["SET", &k, "v", "PX", &ttl()]), ok());
            let exec = c.transaction(&[
                &["EXISTS", &k],
                &["TIME"],
                &["DEBUG", "SLEEP", SLEEP],
                &["EVAL", script, "1", &k],
            ]);
            let x = exec.array();
            assert_eq!(x.len(), 4, "{io_model:?} {x:?}");
            let alive = x[0].int();
            let from_script = x[3].array();
            assert_eq!(from_script[0].int(), alive, "{io_model:?} {x:?}");
            assert_eq!(
                from_script[1].bulk(),
                x[1].array()[0].bulk(),
                "{io_model:?} the script's TIME is not the EXEC's"
            );
            alive == 1
        });
    }
}
