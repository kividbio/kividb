#![allow(clippy::disallowed_methods)]
// tests/blocking_many_keys.rs
//
// Many clients blocked in `BRPOP`/`BLPOP`/`BLMOVE`/`BZPOPMIN` across many
// keys at once, the job-queue shape (#758): every element pushed is received
// exactly once, by a client that is there to receive it, while clients time
// out, are unblocked with `CLIENT UNBLOCK` and go away at the same moments
// as the pushes land; and clients blocked on the same key are served in the
// order they blocked.
//
// Every test runs a real `kividb` on each I/O model there is: tokio
// everywhere, and io_uring as well on Linux.

use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

mod common;

// ── RESP client ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum V {
    Str(String),
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
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Ok(stream) = TcpStream::connect(("127.0.0.1", port)) {
                stream
                    .set_read_timeout(Some(Duration::from_secs(30)))
                    .unwrap();
                let writer = stream.try_clone().unwrap();
                return Self {
                    reader: BufReader::new(stream),
                    writer,
                };
            }
            assert!(Instant::now() < deadline, "could not connect to {port}");
            thread::sleep(Duration::from_millis(50));
        }
    }

    fn send(&mut self, args: &[&str]) {
        self.writer.write_all(&request(args)).unwrap();
    }

    fn cmd(&mut self, args: &[&str]) -> V {
        self.send(args);
        self.read().expect("the connection was closed")
    }

    /// The next reply, or `None` at the end of the stream.
    fn read(&mut self) -> Option<V> {
        let mut line = String::new();
        self.reader.read_line(&mut line).expect("reply");
        let line = line.trim_end_matches(['\r', '\n']);
        if line.is_empty() {
            return None;
        }
        let (kind, rest) = line.split_at(1);
        Some(match kind {
            "+" => V::Str(rest.to_string()),
            "-" => V::Err(rest.to_string()),
            ":" => V::Int(rest.parse().unwrap()),
            "_" => V::Nil,
            "$" => {
                let len: i64 = rest.parse().unwrap();
                if len < 0 {
                    return Some(V::Nil);
                }
                let mut data = vec![0u8; len as usize + 2];
                self.reader.read_exact(&mut data).unwrap();
                data.truncate(len as usize);
                V::Str(String::from_utf8_lossy(&data).into_owned())
            }
            "*" => {
                let n: i64 = rest.parse().unwrap();
                if n < 0 {
                    return Some(V::Nil);
                }
                V::Arr((0..n).map(|_| self.read().unwrap()).collect())
            }
            other => panic!("unexpected RESP type {other:?} in {line:?}"),
        })
    }
}

fn request(args: &[&str]) -> Vec<u8> {
    let mut buf = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        buf.extend_from_slice(a.as_bytes());
        buf.extend_from_slice(b"\r\n");
    }
    buf
}

// ── The server: one for each test and I/O model ───────────────────────────────

fn io_models() -> &'static [&'static str] {
    match std::env::var("KIVIDB_TEST_IO_MODEL").as_deref() {
        Ok("tokio") => &["tokio"],
        Ok("uring") => &["uring"],
        _ if cfg!(target_os = "linux") => &["tokio", "uring"],
        _ => &["tokio"],
    }
}

struct Server {
    port: u16,
    child: Child,
    dir: PathBuf,
}

impl Server {
    fn start(io: &str) -> Self {
        let dir = tempfile::tempdir().unwrap().keep();
        // Verified: the server answering on the port is the one started
        // (`tests/common`).
        let (child, port) = common::start_verified(|port| {
            let log = std::fs::File::create(dir.join("server.log")).unwrap();
            Command::new(env!("CARGO_BIN_EXE_kividb"))
                .current_dir(&dir)
                .args([
                    "--port",
                    &port.to_string(),
                    "--bind",
                    "127.0.0.1",
                    "--io-model",
                    io,
                    "--threads",
                    "4",
                    "--configfile",
                    "kividb.conf",
                ])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(log)
                .spawn()
                .expect("could not start kividb")
        });
        let server = Server { port, child, dir };
        assert_eq!(server.conn().cmd(&["PING"]), V::Str("PONG".into()));
        if io == "uring" {
            let log = std::fs::read_to_string(server.dir.join("server.log")).unwrap();
            assert!(log.contains("I/O model      : io_uring"), "{log}");
        }
        server
    }

    fn conn(&self) -> Conn {
        Conn::connect(self.port)
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn info_field(c: &mut Conn, section: &str, field: &str) -> i64 {
    let V::Str(info) = c.cmd(&["INFO", section]) else {
        panic!("INFO is a bulk string");
    };
    let prefix = format!("{field}:");
    info.lines()
        .find_map(|line| line.trim().strip_prefix(&prefix)?.parse().ok())
        .unwrap_or_else(|| panic!("no {field} in INFO {section}"))
}

fn wait_blocked(c: &mut Conn, n: i64) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while info_field(c, "clients", "blocked_clients") != n {
        assert!(Instant::now() < deadline, "never {n} blocked clients");
        thread::sleep(Duration::from_millis(5));
    }
}

// ── Exactly once ──────────────────────────────────────────────────────────────

/// What one client does in a round.
#[derive(Clone, Copy)]
enum Mode {
    /// Waits for its reply.
    Wait,
    /// Sends its command and shuts its sending side at once, as a client
    /// that goes does, then reads whatever it was answered until the server
    /// closes the connection.
    Go,
}

/// The element a reply hands the client, if any: `BRPOP`/`BLPOP` and
/// `BZPOPMIN` reply with the key and the element (and a score), `BLMOVE`
/// with the element alone, which is moved to the client's own list.
fn element_of(reply: &V) -> Option<String> {
    match reply {
        V::Arr(items) if items.len() >= 2 => match &items[1] {
            V::Str(element) => Some(element.clone()),
            other => panic!("unexpected element {other:?}"),
        },
        V::Str(element) => Some(element.clone()),
        V::Nil | V::Arr(_) => None,
        V::Err(e) if e.starts_with("UNBLOCKED") => None,
        other => panic!("unexpected reply {other:?}"),
    }
}

/// `clients` clients each block again and again on all of `keys`, in an
/// order of their own, with short timeouts; some of their commands are
/// `CLIENT UNBLOCK`ed and some clients go mid-wait; meanwhile `pushes`
/// elements, each different, are pushed to random keys. Every element must
/// end up received exactly once or still in its key.
fn exactly_once(io: &str, kind: &str, clients: usize, nkeys: usize, pushes: usize) {
    let server = Server::start(io);
    let port = server.port;
    let keys: Vec<String> = (0..nkeys).map(|i| format!("{kind}:q{i}")).collect();
    let received: Arc<Mutex<Vec<String>>> = Arc::default();
    let stop = Arc::new(AtomicBool::new(false));
    let ids: Arc<Mutex<Vec<i64>>> = Arc::default();

    let workers: Vec<_> = (0..clients)
        .map(|n| {
            let (keys, received, stop, ids) =
                (keys.clone(), received.clone(), stop.clone(), ids.clone());
            let kind = kind.to_string();
            thread::spawn(move || {
                let mut rng = StdRng::seed_from_u64(n as u64);
                let mut c = Conn::connect(port);
                let V::Int(id) = c.cmd(&["CLIENT", "ID"]) else {
                    panic!()
                };
                ids.lock().unwrap().push(id);
                let mut mine = Vec::new();
                let dest = format!("{kind}:dest{n}");
                while !stop.load(Ordering::Relaxed) {
                    let mut order = keys.clone();
                    order.shuffle(&mut rng);
                    let timeout = format!("0.0{}", rng.gen_range(1..10));
                    let mut args: Vec<&str> = Vec::new();
                    match kind.as_str() {
                        "brpop" => {
                            args.push(if rng.gen_bool(0.5) { "BRPOP" } else { "BLPOP" });
                            args.extend(order.iter().map(String::as_str));
                            args.push(&timeout);
                        }
                        "bzpopmin" => {
                            args.push("BZPOPMIN");
                            args.extend(order.iter().map(String::as_str));
                            args.push(&timeout);
                        }
                        // One source: BLMOVE takes one key.
                        "blmove" => {
                            args.extend([
                                "BLMOVE",
                                order[0].as_str(),
                                dest.as_str(),
                                "RIGHT",
                                "LEFT",
                            ]);
                            args.push(&timeout);
                        }
                        _ => unreachable!(),
                    }
                    let mode = if rng.gen_range(0..20) == 0 {
                        Mode::Go
                    } else {
                        Mode::Wait
                    };
                    c.send(&args);
                    match mode {
                        Mode::Wait => {
                            let reply = c.read().expect("the connection was closed");
                            mine.extend(element_of(&reply));
                        }
                        Mode::Go => {
                            let _ = c.writer.shutdown(Shutdown::Write);
                            while let Some(reply) = c.read() {
                                mine.extend(element_of(&reply));
                            }
                            c = Conn::connect(port);
                            let V::Int(id) = c.cmd(&["CLIENT", "ID"]) else {
                                panic!()
                            };
                            ids.lock().unwrap().push(id);
                        }
                    }
                }
                // BLMOVE moved its elements to the client's own list.
                if kind == "blmove" {
                    mine.clear();
                    let V::Arr(moved) = c.cmd(&["LRANGE", &dest, "0", "-1"]) else {
                        panic!()
                    };
                    mine.extend(moved.iter().map(|v| match v {
                        V::Str(s) => s.clone(),
                        other => panic!("{other:?}"),
                    }));
                }
                received.lock().unwrap().extend(mine);
            })
        })
        .collect();

    // CLIENT UNBLOCK, TIMEOUT or ERROR, of random clients while it all runs.
    let unblocker = {
        let (ids, stop) = (ids.clone(), stop.clone());
        thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(99);
            let mut c = Conn::connect(port);
            while !stop.load(Ordering::Relaxed) {
                let id = {
                    let ids = ids.lock().unwrap();
                    if ids.is_empty() {
                        continue;
                    }
                    ids[rng.gen_range(0..ids.len())]
                };
                let how = if rng.gen_bool(0.5) {
                    "TIMEOUT"
                } else {
                    "ERROR"
                };
                c.cmd(&["CLIENT", "UNBLOCK", &id.to_string(), how]);
                thread::sleep(Duration::from_micros(rng.gen_range(0..2000)));
            }
        })
    };

    let mut pusher = server.conn();
    let mut rng = StdRng::seed_from_u64(7);
    let mut pushed = Vec::new();
    for i in 0..pushes {
        let element = format!("e{i}");
        let key = &keys[rng.gen_range(0..keys.len())];
        let reply = match kind {
            "bzpopmin" => pusher.cmd(&["ZADD", key, &i.to_string(), &element]),
            _ => pusher.cmd(&["LPUSH", key, &element]),
        };
        assert!(matches!(reply, V::Int(_)), "{reply:?}");
        pushed.push(element);
        if i % 64 == 0 {
            thread::sleep(Duration::from_millis(rng.gen_range(0..5)));
        }
    }
    // Until the workers have taken all there is.
    let deadline = Instant::now() + Duration::from_secs(30);
    let left = |c: &mut Conn| -> i64 {
        keys.iter()
            .map(
                |key| match c.cmd(&[if kind == "bzpopmin" { "ZCARD" } else { "LLEN" }, key]) {
                    V::Int(n) => n,
                    other => panic!("{other:?}"),
                },
            )
            .sum()
    };
    while left(&mut pusher) > 0 {
        assert!(
            Instant::now() < deadline,
            "the workers never took everything"
        );
        thread::sleep(Duration::from_millis(20));
    }
    stop.store(true, Ordering::Relaxed);
    for worker in workers {
        worker.join().unwrap();
    }
    unblocker.join().unwrap();

    let received = received.lock().unwrap();
    let mut seen: HashMap<&str, usize> = HashMap::new();
    for element in received.iter() {
        *seen.entry(element.as_str()).or_default() += 1;
    }
    let twice: Vec<_> = seen.iter().filter(|(_, n)| **n > 1).collect();
    assert!(
        twice.is_empty(),
        "{io} {kind}: received more than once: {twice:?}"
    );
    let lost: Vec<_> = pushed
        .iter()
        .filter(|e| !seen.contains_key(e.as_str()))
        .collect();
    assert!(
        lost.is_empty(),
        "{io} {kind}: {} of {pushes} lost: {:?}",
        lost.len(),
        &lost[..lost.len().min(20)]
    );
    assert_eq!(received.len(), pushes, "{io} {kind}");
    // Nobody is left blocked, and no line is left behind.
    wait_blocked(&mut pusher, 0);
    assert_eq!(
        info_field(&mut pusher, "clients", "total_blocking_keys"),
        0,
        "{io} {kind}"
    );
}

#[test]
fn every_element_pushed_to_many_keys_is_received_exactly_once() {
    for io in io_models() {
        exactly_once(io, "brpop", 24, 60, 3000);
    }
}

#[test]
fn every_member_added_to_many_sorted_sets_is_popped_exactly_once() {
    for io in io_models() {
        exactly_once(io, "bzpopmin", 16, 40, 1500);
    }
}

#[test]
fn every_element_moved_by_blmove_is_moved_exactly_once() {
    for io in io_models() {
        exactly_once(io, "blmove", 16, 4, 1500);
    }
}

// ── Order ─────────────────────────────────────────────────────────────────────

/// Clients blocked on the same 270 keys, in orders of their own, are served
/// in the order they blocked, whichever key is pushed to; and each takes
/// from the first of its keys, in its own order, that has an element.
#[test]
fn clients_blocked_on_many_keys_are_served_in_the_order_they_blocked() {
    for io in io_models() {
        let server = Server::start(io);
        let mut ctl = server.conn();
        let keys: Vec<String> = (0..270).map(|i| format!("queue:{i}")).collect();
        let mut rng = StdRng::seed_from_u64(1);
        let mut clients = Vec::new();
        for n in 0..8 {
            let mut order = keys.clone();
            order.shuffle(&mut rng);
            let mut args: Vec<&str> = vec!["BRPOP"];
            args.extend(order.iter().map(String::as_str));
            args.push("0");
            let mut c = server.conn();
            c.send(&args);
            wait_blocked(&mut ctl, n + 1);
            clients.push(c);
        }
        // One element at a time, each to another key: the first in line gets
        // the first, and so on.
        for (n, c) in clients.iter_mut().enumerate().take(4) {
            let key = format!("queue:{}", 17 * n + 3);
            ctl.cmd(&["RPUSH", &key, &format!("job{n}")]);
            assert_eq!(
                c.read().unwrap(),
                V::Arr(vec![V::Str(key.clone()), V::Str(format!("job{n}"))]),
                "{io}: client {n}"
            );
            wait_blocked(&mut ctl, 7 - n as i64);
        }
        // Four elements in one push: the four still waiting get one each, in
        // the order they blocked.
        ctl.cmd(&["RPUSH", "queue:200", "a", "b", "c", "d"]);
        for (c, want) in clients.iter_mut().skip(4).zip(["d", "c", "b", "a"]) {
            assert_eq!(
                c.read().unwrap(),
                V::Arr(vec![V::Str("queue:200".into()), V::Str(want.into())]),
                "{io}"
            );
        }
        wait_blocked(&mut ctl, 0);
        assert_eq!(info_field(&mut ctl, "clients", "total_blocking_keys"), 0);
    }
}

/// A client blocked on many keys that times out leaves every line: the
/// next push goes to a client still waiting, and none of the lines is
/// left behind.
#[test]
fn a_client_that_times_out_leaves_every_line() {
    for io in io_models() {
        let server = Server::start(io);
        let mut ctl = server.conn();
        let keys: Vec<String> = (0..270).map(|i| format!("queue:{i}")).collect();
        let block = |timeout: &str| {
            let mut args: Vec<&str> = vec!["BLPOP"];
            args.extend(keys.iter().map(String::as_str));
            args.push(timeout);
            request(&args)
        };
        let mut early = server.conn();
        early.writer.write_all(&block("2")).unwrap();
        wait_blocked(&mut ctl, 1);
        let mut late = server.conn();
        late.writer.write_all(&block("0")).unwrap();
        wait_blocked(&mut ctl, 2);
        assert_eq!(early.read().unwrap(), V::Nil, "{io}");
        wait_blocked(&mut ctl, 1);
        assert_eq!(info_field(&mut ctl, "clients", "total_blocking_keys"), 270);
        ctl.cmd(&["LPUSH", "queue:269", "job"]);
        assert_eq!(
            late.read().unwrap(),
            V::Arr(vec![V::Str("queue:269".into()), V::Str("job".into())]),
            "{io}"
        );
        wait_blocked(&mut ctl, 0);
        assert_eq!(info_field(&mut ctl, "clients", "total_blocking_keys"), 0);
    }
}

/// `CLIENT UNBLOCK … ERROR` sent together with a push to the key: the
/// client is told `-UNBLOCKED` and takes nothing, and the element stays.
/// It used to be woken as if its time had run out, look once more, pop the
/// element that had just arrived, and be answered `-UNBLOCKED` instead of
/// it: the element was gone.
#[test]
fn a_client_unblocked_with_an_error_takes_nothing() {
    for io in io_models() {
        let server = Server::start(io);
        let mut ctl = server.conn();
        for round in 0..50 {
            let key = format!("unblocked:{round}");
            let mut c = server.conn();
            let V::Int(id) = c.cmd(&["CLIENT", "ID"]) else {
                panic!()
            };
            c.send(&["BRPOP", "other", &key, "0"]);
            wait_blocked(&mut ctl, 1);
            let mut both = request(&["CLIENT", "UNBLOCK", &id.to_string(), "ERROR"]);
            both.extend(request(&["LPUSH", &key, "job"]));
            ctl.writer.write_all(&both).unwrap();
            assert_eq!(ctl.read().unwrap(), V::Int(1));
            assert_eq!(ctl.read().unwrap(), V::Int(1));
            match c.read().unwrap() {
                V::Err(e) => {
                    assert!(e.starts_with("UNBLOCKED"), "{e}");
                    assert_eq!(
                        ctl.cmd(&["LLEN", &key]),
                        V::Int(1),
                        "{io}: round {round}: the element was lost"
                    );
                }
                // Its wait saw the push's wake-up first: it has the element.
                V::Arr(items) => {
                    assert_eq!(items, [V::Str(key.clone()), V::Str("job".into())], "{io}");
                    assert_eq!(ctl.cmd(&["LLEN", &key]), V::Int(0), "{io}");
                }
                other => panic!("{io}: round {round}: {other:?}"),
            }
            wait_blocked(&mut ctl, 0);
        }
    }
}
