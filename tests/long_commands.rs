// A long command on one connection does not hold up the others (#754).
//
// On the Tokio runtime a command runs inline on a worker thread. The worker
// that got a long `KEYS` was, as often as not, the one that had been waiting
// on the sockets for everybody: while it ran, nothing waited on them, and a
// `PING` on another connection, served by any other worker, waited for the
// `KEYS` to end. Freeing a hash of millions of fields after `DEL` did the
// same.
//
// Each test runs the long command a few times while another connection
// sends `PING` every millisecond, and checks that no `PING` waited for as
// much as half of the command. Before the fix a run stalled the `PING` for
// the whole command three times in four.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

/// The tests here measure time, and count the process's command gate
/// slots: they take turns.
fn one_at_a_time() -> MutexGuard<'static, ()> {
    static TURN: Mutex<()> = Mutex::new(());
    TURN.lock().unwrap_or_else(|e| e.into_inner())
}

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
            .set_read_timeout(Some(Duration::from_secs(60)))
            .unwrap();
        stream.set_nodelay(true).unwrap();
        let reader = BufReader::new(stream.try_clone().unwrap());
        Self { stream, reader }
    }

    fn send(&mut self, args: &[&str]) {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            buf.extend_from_slice(format!("${}\r\n{arg}\r\n", arg.len()).as_bytes());
        }
        self.stream.write_all(&buf).unwrap();
    }

    fn cmd(&mut self, args: &[&str]) -> V {
        self.send(args);
        self.read()
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

    /// A hash of `fields` fields at `key`, sent a thousand fields a command
    /// and the commands pipelined.
    fn big_hash(&mut self, key: &str, fields: usize) {
        let mut sent = 0;
        for start in (0..fields).step_by(1000) {
            let names: Vec<String> = (start..fields.min(start + 1000))
                .map(|i| format!("f{i}"))
                .collect();
            let mut args = vec!["HSET", key];
            for name in &names {
                args.push(name);
                args.push("v");
            }
            self.send(&args);
            sent += 1;
        }
        for _ in 0..sent {
            assert!(matches!(self.read(), V::Int(_)));
        }
        assert_eq!(self.cmd(&["HLEN", key]), V::Int(fields as i64));
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
    use std::sync::atomic::AtomicU16;
    static NEXT: AtomicU16 = AtomicU16::new(0);
    loop {
        let step = NEXT.fetch_add(1, Ordering::Relaxed);
        let from = (std::process::id() as u16).wrapping_mul(41);
        let port = 10_000 + from.wrapping_add(step) % 20_000;
        if port_is_free(port) {
            return port;
        }
    }
}

/// A server on the Tokio I/O model, the one this fixes. On io_uring a long
/// command still holds up the connections of its own thread (#762).
///
/// Without `lua-parallel-scripts`: with it (a build with Lua), `KEYS` and
/// `SCAN` take the command gate alone, so as not to see a script that runs
/// beside others part-way through (`tag_locks::sees_the_whole_keyspace`),
/// and every other command waits for them, by design. Switched on, it stays
/// on for the whole test process.
fn start_server() -> u16 {
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
            lua_parallel_scripts: false,
            configfile: "./kividb.conf".to_string(),
            ..Default::default()
        };
        kividb::run_for_test(config);
    });
    assert_on_tokio(port);
    port
}

/// The server at `port` serves its clients with Tokio, not io_uring, and
/// runs every script alone: what the tests here measure.
fn assert_on_tokio(port: u16) {
    let mut c = Conn::connect(port);
    let V::Bulk(info) = c.cmd(&["INFO", "server"]) else {
        panic!("INFO is a bulk string");
    };
    let info = String::from_utf8(info).unwrap();
    let api = info
        .lines()
        .find_map(|line| line.strip_prefix("multiplexing_api:"))
        .expect("multiplexing_api in INFO server");
    assert_ne!(
        api.trim(),
        "io_uring",
        "the server under test is on io_uring"
    );
    let on = V::Bulk(b"yes".to_vec());
    let parallel = c.cmd(&["CONFIG", "GET", "lua-parallel-scripts"]);
    let parallel_on = matches!(&parallel, V::Array(kv) if kv.get(1) == Some(&on));
    assert!(!parallel_on, "lua-parallel-scripts is on: {parallel:?}");
}

/// Run `heavy` on its own connection `runs` times while a second one sends
/// `PING` every millisecond. For each run: how long the command took, and
/// the longest a `PING` sent while it ran waited for its answer.
fn probe_while(
    port: u16,
    runs: usize,
    mut heavy: impl FnMut(&mut Conn),
) -> Vec<(Duration, Duration)> {
    let mut long = Conn::connect(port);
    let mut out = Vec::new();
    for _ in 0..runs {
        let stop = Arc::new(AtomicBool::new(false));
        let prober = {
            let stop = Arc::clone(&stop);
            thread::spawn(move || {
                let mut probe = Conn::connect(port);
                let mut times = Vec::new();
                while !stop.load(Ordering::Relaxed) {
                    let sent = Instant::now();
                    assert_eq!(probe.cmd(&["PING"]), V::Simple("PONG".into()));
                    times.push((sent, sent.elapsed()));
                    thread::sleep(Duration::from_millis(1));
                }
                times
            })
        };
        thread::sleep(Duration::from_millis(200));
        // A command right before the long one: the connection is served,
        // and whichever worker serves it is the one the long one runs on.
        assert_eq!(long.cmd(&["PING"]), V::Simple("PONG".into()));
        let started = Instant::now();
        heavy(&mut long);
        let ended = Instant::now();
        thread::sleep(Duration::from_millis(100));
        stop.store(true, Ordering::Relaxed);
        let times = prober.join().unwrap();
        let worst = times
            .iter()
            .filter(|(sent, waited)| *sent <= ended && *sent + *waited >= started)
            .map(|(_, waited)| *waited)
            .max()
            .unwrap_or_default();
        out.push((ended - started, worst));
    }
    out
}

/// Every run took long enough to tell, and in none did a `PING` wait as
/// much as half of it.
fn assert_not_held_up(what: &str, runs: &[(Duration, Duration)]) {
    eprintln!("{what}: (took, longest PING) {runs:?}");
    for &(took, worst) in runs {
        assert!(
            took >= Duration::from_millis(40),
            "{what} took {took:?}: too short to tell a stall from noise; runs {runs:?}"
        );
        assert!(
            worst < took / 2,
            "a PING on another connection waited {worst:?} while {what} took {took:?}; \
             runs {runs:?}"
        );
    }
}

#[test]
fn keys_and_scan_over_many_keys_do_not_hold_up_other_connections() {
    let _turn = one_at_a_time();
    let port = start_server();
    let mut c = Conn::connect(port);
    c.ok(&["DEBUG", "POPULATE", "1000000", "user"]);

    let keys = probe_while(port, 4, |c| {
        let V::Array(found) = c.cmd(&["KEYS", "user:99*"]) else {
            panic!("KEYS answers an array");
        };
        assert_eq!(found.len(), 11_111);
    });
    assert_not_held_up("KEYS", &keys);

    let scan = probe_while(port, 4, |c| {
        let V::Array(reply) = c.cmd(&["SCAN", "0", "MATCH", "user:99*", "COUNT", "2000000"]) else {
            panic!("SCAN answers an array");
        };
        assert_eq!(
            reply[0],
            V::Bulk(b"0".to_vec()),
            "one call goes through it all"
        );
        let V::Array(found) = &reply[1] else {
            panic!("SCAN's keys are an array");
        };
        assert_eq!(found.len(), 11_111);
    });
    assert_not_held_up("SCAN", &scan);
}

#[test]
fn freeing_a_big_value_after_del_does_not_hold_up_other_connections() {
    let _turn = one_at_a_time();
    let port = start_server();
    let mut c = Conn::connect(port);
    let mut timed = Vec::new();
    for _ in 0..4 {
        c.big_hash("big", 1_000_000);
        timed.extend(probe_while(port, 1, |long| {
            assert_eq!(long.cmd(&["DEL", "big"]), V::Int(1));
        }));
        assert_eq!(c.cmd(&["EXISTS", "big"]), V::Int(0));
    }
    assert_not_held_up("DEL of a hash of 1,000,000 fields", &timed);
}

/// Each long command hands its worker's tasks to a thread of the runtime's
/// blocking pool; the thread it ran on joins the pool. Those threads must
/// be the ones the next long commands use, or every long command makes a
/// new thread, and each takes a command gate slot for good: past 128 every
/// `EXEC` and script takes them all. With Tokio's default, idle pool
/// threads went after 10 s, so long commands further apart than that each
/// made one (`helpers::RUNTIME_THREAD_KEEP_ALIVE`).
#[test]
fn long_commands_do_not_use_up_command_gate_slots() {
    let _turn = one_at_a_time();
    let port = start_server();
    let mut c = Conn::connect(port);
    // Past `helpers::LONG_WORK`, so that each `KEYS` is long work.
    c.ok(&["DEBUG", "POPULATE", "70000", "k"]);
    // Other connections send commands all along, so that a thread that
    // takes over a worker's tasks while a `KEYS` runs serves some of them,
    // and takes its slot then.
    let stop = Arc::new(AtomicBool::new(false));
    let pingers: Vec<_> = (0..3)
        .map(|_| {
            let stop = Arc::clone(&stop);
            thread::spawn(move || {
                let mut other = Conn::connect(port);
                while !stop.load(Ordering::Relaxed) {
                    assert_eq!(other.cmd(&["PING"]), V::Simple("PONG".into()));
                    thread::sleep(Duration::from_micros(200));
                }
            })
        })
        .collect();
    let mut long = |runs: usize| {
        for _ in 0..runs {
            let V::Array(found) = c.cmd(&["KEYS", "k:1234*"]) else {
                panic!("KEYS answers an array");
            };
            assert_eq!(found.len(), 11);
        }
    };
    long(10);
    let before = kividb::command_gate::slots_handed_out();

    long(200);
    // And spaced by more than Tokio's default keep-alive of idle threads.
    for _ in 0..3 {
        thread::sleep(Duration::from_secs(11));
        long(10);
    }
    stop.store(true, Ordering::Relaxed);
    for pinger in pingers {
        pinger.join().unwrap();
    }
    let after = kividb::command_gate::slots_handed_out();
    eprintln!("command gate slots: {before} before, {after} after");
    assert!(
        after <= before + 2,
        "the command gate handed out {} slots over 230 long commands \
         ({before} before, {after} after)",
        after - before
    );
}
