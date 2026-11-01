// tests/concurrent_writes.rs
//
// Writes to one key from many clients at once, with the AOF off and no
// replica: the result must be one that running the same commands one after
// another could have given (#271).
//
// Every write holds the order of its keys (`write_order`) from before it
// reads them until after it has written them. That used to be taken only
// while something observed the write stream (a replica, the AOF), and a
// command that reads a key and writes it back under separate locks of its
// shard lost updates when nothing did: `PFADD`, `HINCRBY` on a new key,
// `BITFIELD INCRBY`, `SADD` against a `SREM` that removed the set it had
// just made, the `*STORE` commands, `COPY` without `REPLACE`.
//
// A few single-key commands that do all of their reading and writing under
// one hold of the shard (`SADD`, `HSET`, `ZADD`, `LPUSH`, `INCR`, `SET`,
// `PFADD`, ...) take that order shared when nothing observes them, so that
// a hot key is not held to one command at a time; the tests of one key from
// many clients, and of those commands beside ones that hold the order whole
// (`SREM`, `HDEL`, `ZREM`, `LPOP`, `SUNIONSTORE`), check that path.
//
// A real server process, on the default I/O model (io_uring on Linux), so
// that the clients' commands run on several threads at once.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq)]
enum V {
    Str(String),
    Err(String),
    Int(i64),
    Arr(Vec<V>),
    Nil,
}

impl V {
    fn i(&self) -> i64 {
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
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Ok(stream) = TcpStream::connect(("127.0.0.1", port)) {
                // Long: a debug build, beside the other tests of this file
                // and every server they start, is slow.
                stream
                    .set_read_timeout(Some(Duration::from_secs(300)))
                    .unwrap();
                stream.set_nodelay(true).unwrap();
                let writer = stream.try_clone().unwrap();
                return Self {
                    reader: BufReader::new(stream),
                    writer,
                };
            }
            assert!(Instant::now() < deadline, "could not connect to {port}");
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn encode(buf: &mut Vec<u8>, args: &[&str]) {
        buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
        for a in args {
            buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            buf.extend_from_slice(a.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
    }

    fn cmd(&mut self, args: &[&str]) -> V {
        let mut buf = Vec::new();
        Self::encode(&mut buf, args);
        self.writer.write_all(&buf).unwrap();
        self.read()
    }

    /// Every command of `batch` sent at once, then every reply read.
    fn pipeline(&mut self, batch: &[Vec<String>]) -> Vec<V> {
        let mut buf = Vec::new();
        for args in batch {
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            Self::encode(&mut buf, &args);
        }
        self.writer.write_all(&buf).unwrap();
        (0..batch.len()).map(|_| self.read()).collect()
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
}

struct Server {
    port: u16,
    child: Child,
    dir: std::path::PathBuf,
}

/// Whether nothing has `port`, asked with a socket that is bound and never
/// listens (see `tests/uring.rs`).
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

impl Server {
    /// The AOF off (the default) and no replica: nothing observes the writes.
    fn start() -> Self {
        let dir = tempfile::tempdir().unwrap().keep();
        let port = free_port();
        let log = std::fs::File::create(dir.join("server.log")).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_kividb"))
            .current_dir(&dir)
            .args([
                "--port",
                &port.to_string(),
                "--bind",
                "127.0.0.1",
                "--configfile",
                "kividb.conf",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .expect("could not start kividb");
        let server = Server { port, child, dir };
        assert_eq!(server.conn().cmd(&["PING"]), V::Str("PONG".into()));
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

/// Run `clients` clients at once, each given its number and a barrier shared
/// by all of them, and collect what each returns.
fn at_once<T: Send + 'static>(
    server: &Server,
    clients: usize,
    work: impl Fn(usize, &mut Conn, &Barrier) -> T + Send + Sync + 'static,
) -> Vec<T> {
    let work = Arc::new(work);
    let barrier = Arc::new(Barrier::new(clients));
    let threads: Vec<_> = (0..clients)
        .map(|c| {
            let mut conn = server.conn();
            let work = Arc::clone(&work);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                work(c, &mut conn, &barrier)
            })
        })
        .collect();
    threads.into_iter().map(|t| t.join().unwrap()).collect()
}

fn args(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|p| p.to_string()).collect()
}

/// `count` keys made from `prefix`, all in the shard of `anchor`, as the
/// server lays them out on this machine: a command naming several of them
/// runs beside other commands, not alone.
fn keys_in_shard_of(anchor: &str, prefix: &str, count: usize) -> Vec<String> {
    let shards = kividb::store::num_shards();
    let shard = kividb::store::shard_index(shards, anchor);
    (0..)
        .map(|i| format!("{prefix}{i}"))
        .filter(|k| kividb::store::shard_index(shards, k) == shard)
        .take(count)
        .collect()
}

/// The issue's own case: 100 000 distinct elements added by 50 clients give
/// the registers, and so the count, that one client adding them gives.
#[test]
fn pfadd_from_fifty_clients_keeps_every_element() {
    let server = Server::start();
    const ELEMENTS: usize = 100_000;
    const CLIENTS: usize = 50;

    let mut one = server.conn();
    for chunk in (0..ELEMENTS).collect::<Vec<_>>().chunks(1000) {
        let batch: Vec<Vec<String>> = chunk
            .iter()
            .map(|i| args(&["PFADD", "hll:serial", &format!("member:{i:08}")]))
            .collect();
        one.pipeline(&batch);
    }

    at_once(&server, CLIENTS, |c, conn, _| {
        let mine: Vec<usize> = (c..ELEMENTS).step_by(CLIENTS).collect();
        for chunk in mine.chunks(50) {
            let batch: Vec<Vec<String>> = chunk
                .iter()
                .map(|i| args(&["PFADD", "hll:concurrent", &format!("member:{i:08}")]))
                .collect();
            conn.pipeline(&batch);
        }
    });

    let serial = one.cmd(&["PFCOUNT", "hll:serial"]).i();
    let concurrent = one.cmd(&["PFCOUNT", "hll:concurrent"]).i();
    assert_eq!(concurrent, serial, "PFCOUNT differs from the serial run's");
    let serial = one.cmd(&["PFDEBUG", "GETREG", "hll:serial"]);
    let concurrent = one.cmd(&["PFDEBUG", "GETREG", "hll:concurrent"]);
    let (V::Arr(serial), V::Arr(concurrent)) = (serial, concurrent) else {
        panic!("PFDEBUG GETREG did not return registers");
    };
    let differ = serial
        .iter()
        .zip(&concurrent)
        .filter(|(a, b)| a != b)
        .count();
    assert_eq!(differ, 0, "{differ} registers differ from the serial run's");
}

/// Fifty clients increment the same field of the same new hashes: each ends
/// at fifty. Creating the hash was a separate step from finding it absent,
/// and two increments of a new hash both answered 1.
#[test]
fn hincrby_on_new_hashes_from_fifty_clients_counts_every_increment() {
    let server = Server::start();
    const CLIENTS: usize = 50;
    const KEYS: usize = 4000;
    const BATCH: usize = 100;

    at_once(&server, CLIENTS, |_, conn, barrier| {
        for start in (0..KEYS).step_by(BATCH) {
            barrier.wait();
            let batch: Vec<Vec<String>> = (start..start + BATCH)
                .map(|k| args(&["HINCRBY", &format!("counter:{k}"), "f", "1"]))
                .collect();
            conn.pipeline(&batch);
        }
    });

    let mut one = server.conn();
    let short: Vec<(usize, i64)> = (0..KEYS)
        .map(|k| {
            let n = match one.cmd(&["HGET", &format!("counter:{k}"), "f"]) {
                V::Str(s) => s.parse().unwrap(),
                V::Nil => 0,
                other => panic!("HGET: {other:?}"),
            };
            (k, n)
        })
        .filter(|&(_, n)| n != CLIENTS as i64)
        .collect();
    assert!(
        short.is_empty(),
        "{} of {KEYS} hashes lost increments, e.g. {:?}",
        short.len(),
        &short[..short.len().min(5)]
    );
}

/// `SADD` makes a set while `SREM`s of a member it never had run on the same
/// key: whatever order they run in, the member is there afterwards. The set
/// `SADD` had just made, still empty, used to be taken for one a `SREM` had
/// emptied, and removed, with the member then added to a set no longer in
/// the keyspace.
#[test]
fn sadd_beside_srem_of_another_member_keeps_the_member() {
    let server = Server::start();
    const ADDERS: usize = 10;
    const REMOVERS: usize = 40;
    const KEYS: usize = 20_000;
    const BATCH: usize = 200;

    let replies = at_once(&server, ADDERS + REMOVERS, |c, conn, barrier| {
        let mut added = Vec::new();
        for start in (0..KEYS).step_by(BATCH) {
            barrier.wait();
            let batch: Vec<Vec<String>> = (start..start + BATCH)
                .map(|k| {
                    let key = format!("set:{k}");
                    if c < ADDERS {
                        args(&["SADD", &key, "member"])
                    } else {
                        args(&["SREM", &key, "never"])
                    }
                })
                .collect();
            if c < ADDERS {
                added.extend(conn.pipeline(&batch).iter().map(V::i));
            } else {
                conn.pipeline(&batch);
            }
        }
        added
    });

    let mut one = server.conn();
    let missing = (0..KEYS)
        .filter(|k| one.cmd(&["SISMEMBER", &format!("set:{k}"), "member"]).i() != 1)
        .count();
    assert_eq!(missing, 0, "{missing} of {KEYS} sets lost their member");
    // And exactly one `SADD` of each key added it.
    let added: i64 = replies.iter().flatten().sum();
    assert_eq!(
        added, KEYS as i64,
        "SADD replied 1 {added} times for {KEYS} keys"
    );
}

/// `BITFIELD ... INCRBY` from fifty clients on one key adds up.
#[test]
fn bitfield_incrby_from_fifty_clients_adds_up() {
    let server = Server::start();
    const CLIENTS: usize = 50;
    const EACH: usize = 2000;

    at_once(&server, CLIENTS, |_, conn, _| {
        let batch: Vec<Vec<String>> = (0..50)
            .map(|_| args(&["BITFIELD", "bits", "INCRBY", "u32", "0", "1"]))
            .collect();
        for _ in 0..EACH / 50 {
            conn.pipeline(&batch);
        }
    });

    let mut one = server.conn();
    let V::Arr(got) = one.cmd(&["BITFIELD", "bits", "GET", "u32", "0"]) else {
        panic!("BITFIELD GET");
    };
    assert_eq!(got[0].i(), (CLIENTS * EACH) as i64);
}

/// `SUNIONSTORE acc acc other`, both keys in one shard, beside `SADD`s to
/// `acc`: every member added is still there. The union read `acc`, a `SADD`
/// landed, and the union wrote the old `acc` back over it.
///
/// Each union copies the whole of `acc`: kept small, so that the test takes
/// seconds and not minutes (it took 215 s in a release build with 50 000
/// members, and a debug build's clients gave up waiting).
#[test]
fn sunionstore_of_one_shard_beside_sadd_loses_no_member() {
    let server = Server::start();
    let keys = keys_in_shard_of("acc", "union:", 2);
    let (acc, other) = (keys[0].clone(), keys[1].clone());
    const ADDERS: usize = 20;
    const STORERS: usize = 10;
    const EACH: usize = 400;

    let mut one = server.conn();
    one.cmd(&["SADD", &other, "from-other"]);

    at_once(&server, ADDERS + STORERS, move |c, conn, _| {
        for start in (0..EACH).step_by(50) {
            let batch: Vec<Vec<String>> = (start..start + 50)
                .map(|i| {
                    if c < ADDERS {
                        args(&["SADD", &acc, &format!("m:{c}:{i}")])
                    } else {
                        args(&["SUNIONSTORE", &acc, &acc, &other])
                    }
                })
                .collect();
            conn.pipeline(&batch);
        }
    });

    let keys = keys_in_shard_of("acc", "union:", 1);
    let card = one.cmd(&["SCARD", &keys[0]]).i();
    assert_eq!(card, (ADDERS * EACH) as i64 + 1, "members were lost");
}

/// Two `COPY`s without `REPLACE` to the same new key, every key in one
/// shard: exactly one of them copies. Both used to find the destination
/// free, and both answered 1.
#[test]
fn copy_to_one_destination_from_two_clients_copies_once() {
    let server = Server::start();
    const KEYS: usize = 3000;
    const BATCH: usize = 100;
    let keys = keys_in_shard_of("copy", "copy:", KEYS + 2);
    let (a, b) = (keys[0].clone(), keys[1].clone());
    let destinations: Vec<String> = keys[2..].to_vec();

    let mut one = server.conn();
    one.cmd(&["SET", &a, "from-a"]);
    one.cmd(&["SET", &b, "from-b"]);

    let replies = at_once(&server, 2, move |c, conn, barrier| {
        let source = if c == 0 { &a } else { &b };
        let mut replies = Vec::new();
        for chunk in destinations.chunks(BATCH) {
            barrier.wait();
            let batch: Vec<Vec<String>> =
                chunk.iter().map(|d| args(&["COPY", source, d])).collect();
            replies.extend(conn.pipeline(&batch).iter().map(V::i));
        }
        replies
    });

    let both = (0..KEYS)
        .filter(|&k| replies[0][k] + replies[1][k] != 1)
        .count();
    assert_eq!(
        both, 0,
        "{both} of {KEYS} destinations were copied to twice, or never"
    );
}

/// Adders run `add` on each key while removers run `remove` of something it
/// never held, which removes the key once it finds it empty. Whatever order
/// they ran in, what was added is there afterwards: `holds` answers `1`, or
/// a score, for every key. The collection a write had just made, still
/// empty, used to be taken for one emptied, and removed, with the write then
/// going into a collection no longer in the keyspace.
fn adds_beside_removes_of_something_else(
    add: &'static [&'static str],
    remove: &'static [&'static str],
    holds: &'static [&'static str],
) {
    let server = Server::start();
    const ADDERS: usize = 10;
    const REMOVERS: usize = 30;
    const KEYS: usize = 10_000;
    const BATCH: usize = 200;
    let command = |words: &[&str], key: &str| -> Vec<String> {
        let mut command = vec![words[0].to_string(), key.to_string()];
        command.extend(words[1..].iter().map(|w| w.to_string()));
        command
    };

    at_once(&server, ADDERS + REMOVERS, move |c, conn, barrier| {
        for start in (0..KEYS).step_by(BATCH) {
            barrier.wait();
            let batch: Vec<Vec<String>> = (start..start + BATCH)
                .map(|k| {
                    let key = format!("key:{k}");
                    command(if c < ADDERS { add } else { remove }, &key)
                })
                .collect();
            conn.pipeline(&batch);
        }
    });

    let mut one = server.conn();
    let missing = (0..KEYS)
        .filter(|k| {
            let args = command(holds, &format!("key:{k}"));
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            // `1`, or the score.
            !matches!(one.cmd(&args), V::Int(1) | V::Str(_))
        })
        .count();
    assert_eq!(missing, 0, "{missing} of {KEYS} keys lost what was added");
}

#[test]
fn hset_beside_hdel_of_another_field_keeps_the_field() {
    adds_beside_removes_of_something_else(
        &["HSET", "field", "value"],
        &["HDEL", "never"],
        &["HEXISTS", "field"],
    );
}

#[test]
fn zadd_beside_zrem_of_another_member_keeps_the_member() {
    adds_beside_removes_of_something_else(
        &["ZADD", "1", "member"],
        &["ZREM", "never"],
        &["ZSCORE", "member"],
    );
}

/// Pushes beside pops that empty the list: every element pushed is popped
/// once or still in the list, and none twice. A push into a list a pop had
/// just emptied and removed would be lost.
#[test]
fn lpush_beside_lpop_that_empties_the_list_loses_nothing() {
    let server = Server::start();
    const PUSHERS: usize = 20;
    const POPPERS: usize = 20;
    const EACH: usize = 2000;

    let popped = at_once(&server, PUSHERS + POPPERS, |c, conn, _| {
        let mut popped = Vec::new();
        for start in (0..EACH).step_by(50) {
            let batch: Vec<Vec<String>> = (start..start + 50)
                .map(|i| {
                    if c < PUSHERS {
                        args(&["LPUSH", "list", &format!("{c}:{i}")])
                    } else {
                        args(&["LPOP", "list"])
                    }
                })
                .collect();
            let replies = conn.pipeline(&batch);
            if c >= PUSHERS {
                popped.extend(replies.into_iter().filter_map(|v| match v {
                    V::Str(s) => Some(s),
                    _ => None,
                }));
            }
        }
        popped
    });

    let mut one = server.conn();
    let V::Arr(left) = one.cmd(&["LRANGE", "list", "0", "-1"]) else {
        panic!("LRANGE");
    };
    let mut seen: Vec<String> = popped.into_iter().flatten().collect();
    seen.extend(left.into_iter().map(|v| match v {
        V::Str(s) => s,
        other => panic!("LRANGE element: {other:?}"),
    }));
    let all = seen.len();
    seen.sort();
    seen.dedup();
    assert_eq!(
        all,
        seen.len(),
        "{} elements came out twice",
        all - seen.len()
    );
    assert_eq!(seen.len(), PUSHERS * EACH, "elements were lost");
}

/// Fifty clients on one key at once — the case `write_order` lets such
/// writes share (`lock_key_shared`): `INCR`, `HINCRBY`, `ZINCRBY` and
/// `SADD` add up as one client's would.
#[test]
fn one_hot_key_of_each_kind_from_fifty_clients_adds_up() {
    let server = Server::start();
    const CLIENTS: usize = 50;
    const EACH: usize = 1000;

    at_once(&server, CLIENTS, |c, conn, _| {
        for start in (0..EACH).step_by(50) {
            let batch: Vec<Vec<String>> = (start..start + 50)
                .flat_map(|i| {
                    [
                        args(&["INCR", "counter"]),
                        args(&["HINCRBY", "hash", "field", "1"]),
                        args(&["ZINCRBY", "zset", "1", "member"]),
                        args(&["SADD", "set", &format!("{c}:{i}")]),
                    ]
                })
                .collect();
            conn.pipeline(&batch);
        }
    });

    let total = (CLIENTS * EACH) as i64;
    let mut one = server.conn();
    let number = |v: V| match v {
        V::Str(s) => s.parse::<f64>().unwrap() as i64,
        V::Int(n) => n,
        other => panic!("not a number: {other:?}"),
    };
    assert_eq!(number(one.cmd(&["GET", "counter"])), total, "INCR");
    assert_eq!(
        number(one.cmd(&["HGET", "hash", "field"])),
        total,
        "HINCRBY"
    );
    assert_eq!(
        number(one.cmd(&["ZSCORE", "zset", "member"])),
        total,
        "ZINCRBY"
    );
    assert_eq!(one.cmd(&["SCARD", "set"]).i(), total, "SADD");
}

/// `SET ... NX` of the same new keys from fifty clients: exactly one of them
/// sets each key, and the key holds what that one wrote.
#[test]
fn set_nx_from_fifty_clients_sets_each_key_once() {
    let server = Server::start();
    const CLIENTS: usize = 50;
    const KEYS: usize = 2000;
    const BATCH: usize = 100;

    let replies = at_once(&server, CLIENTS, |c, conn, barrier| {
        let mut set = Vec::new();
        for start in (0..KEYS).step_by(BATCH) {
            barrier.wait();
            let batch: Vec<Vec<String>> = (start..start + BATCH)
                .map(|k| args(&["SET", &format!("nx:{k}"), &c.to_string(), "NX"]))
                .collect();
            set.extend(
                conn.pipeline(&batch)
                    .into_iter()
                    .map(|v| v == V::Str("OK".into())),
            );
        }
        set
    });

    // Who set each key.
    let mut winners = vec![Vec::new(); KEYS];
    for (c, set) in replies.iter().enumerate() {
        for (k, &won) in set.iter().enumerate() {
            if won {
                winners[k].push(c);
            }
        }
    }
    let mut one = server.conn();
    for (k, winners) in winners.iter().enumerate() {
        assert_eq!(winners.len(), 1, "nx:{k} was set by {winners:?}");
        let value = one.cmd(&["GET", &format!("nx:{k}")]);
        assert_eq!(value, V::Str(winners[0].to_string()), "nx:{k}");
    }
}

/// `HSET` of a new field beside the field-expiry sweep that empties the hash
/// of its one field with a time and removes it: the new field is there
/// afterwards. The sweep holds the hash's order whole, and `HSET` holds the
/// hash's shard while it writes the field.
///
/// A guard more than a reproduction: the window is a few instructions wide,
/// and the sweep comes once every tenth of a second.
#[test]
fn hset_beside_the_field_expiry_sweep_keeps_the_new_field() {
    let server = Server::start();
    const CLIENTS: usize = 20;
    const KEYS: usize = 2000;
    const ROUNDS: usize = 3;

    let mut one = server.conn();
    for round in 0..ROUNDS {
        let key = move |k: usize| format!("fields:{round}:{k}");
        let batch: Vec<Vec<String>> = (0..KEYS)
            .flat_map(|k| {
                [
                    args(&["HSET", &key(k), "old", "v"]),
                    args(&["HPEXPIRE", &key(k), "50", "FIELDS", "1", "old"]),
                ]
            })
            .collect();
        one.pipeline(&batch);
        // Due in 50 ms, and swept within a tenth of a second of that: the
        // new fields are written from just before until well after.
        std::thread::sleep(Duration::from_millis(45));
        at_once(&server, CLIENTS, move |c, conn, _| {
            let started = Instant::now();
            let mut k = c;
            while started.elapsed() < Duration::from_millis(250) {
                let batch: Vec<Vec<String>> = (0..50)
                    .map(|i| args(&["HSET", &key((k + i * CLIENTS) % KEYS), "new", "v"]))
                    .collect();
                conn.pipeline(&batch);
                k += 50 * CLIENTS;
            }
        });
        let missing = (0..KEYS)
            .filter(|&k| one.cmd(&["HEXISTS", &key(k), "new"]).i() != 1)
            .count();
        assert_eq!(
            missing, 0,
            "round {round}: {missing} of {KEYS} hashes lost the new field"
        );
    }
}
