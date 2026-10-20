// tests/parallel_scripts.rs
//
// `lua-parallel-scripts`: scripts whose KEYS share a hash tag, run beside
// each other under their tags' locks (`src/tag_locks.rs`). Real `kividb`
// processes: the setting is the whole process's, and so is the set of
// scripts a server has seen to be quick.
#![cfg(feature = "lua")]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
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
    fn s(&self) -> &str {
        match self {
            V::Str(s) | V::Err(s) => s,
            other => panic!("not a string: {other:?}"),
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
                s.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
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
}

struct Server {
    port: u16,
    child: Child,
    _dir: PathBuf,
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

/// A port nothing is listening on, and not one the system hands out to
/// outgoing connections (see tests/eviction.rs).
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
    fn start(extra: &[&str]) -> Self {
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
                "--io-model",
                "tokio",
                "--threads",
                "4",
                "--update-check",
                "no",
            ])
            .args(extra)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .expect("could not start kividb");
        let server = Server {
            port,
            child,
            _dir: dir,
        };
        assert_eq!(server.conn().cmd(&["PING"]).s(), "PONG");
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
    }
}

const READS_A_KEY: &str = "return redis.call('GET', ARGV[1])";

/// On unless switched off: a server started with nothing said holds a tagged
/// script to its tag.
#[test]
fn it_is_on_unless_switched_off() {
    let server = Server::start(&[]);
    let mut c = server.conn();
    assert_eq!(
        c.cmd(&["CONFIG", "GET", "lua-parallel-scripts"]),
        V::Arr(vec![
            V::Str("lua-parallel-scripts".into()),
            V::Str("yes".into())
        ])
    );
    assert!(c.cmd(&["SET", "elsewhere", "v"]).ok());
    let refused = c.cmd(&["EVAL", READS_A_KEY, "1", "{q}:a", "elsewhere"]);
    assert!(refused.is_err(), "{refused:?}");
}

/// Switched off, a script may touch any key, as it always could.
#[test]
fn switched_off_a_script_may_touch_any_key() {
    let server = Server::start(&["--lua-parallel-scripts", "false"]);
    let mut c = server.conn();
    assert_eq!(
        c.cmd(&["CONFIG", "GET", "lua-parallel-scripts"]),
        V::Arr(vec![
            V::Str("lua-parallel-scripts".into()),
            V::Str("no".into())
        ])
    );
    assert!(c.cmd(&["SET", "elsewhere", "v"]).ok());
    for _ in 0..5 {
        assert_eq!(
            c.cmd(&["EVAL", READS_A_KEY, "1", "{q}:a", "elsewhere"]),
            V::Str("v".into())
        );
    }
}

/// On, a script whose keys share a hash tag is held to that tag — from its
/// first run, and every time after, so that it never works until the day it
/// is run another way. A script with other keys may touch what it likes.
#[test]
fn a_tagged_script_is_held_to_its_tag() {
    let server = Server::start(&["--lua-parallel-scripts", "true"]);
    let mut c = server.conn();
    assert_eq!(
        c.cmd(&["CONFIG", "GET", "lua-parallel-scripts"]),
        V::Arr(vec![
            V::Str("lua-parallel-scripts".into()),
            V::Str("yes".into())
        ])
    );
    assert!(c.cmd(&["SET", "elsewhere", "v"]).ok());
    assert!(c.cmd(&["SET", "{q}:b", "mine"]).ok());
    assert!(c.cmd(&["SET", "{r}:b", "theirs"]).ok());

    // Many times: the first runs and the later ones take different roads.
    for run in 0..8 {
        assert_eq!(
            c.cmd(&["EVAL", READS_A_KEY, "1", "{q}:a", "{q}:b"]),
            V::Str("mine".into()),
            "run {run}"
        );
        for outside in ["elsewhere", "{r}:b"] {
            let refused = c.cmd(&["EVAL", READS_A_KEY, "1", "{q}:a", outside]);
            assert!(
                refused.is_err(),
                "run {run}: {outside} was read: {refused:?}"
            );
            assert!(
                refused.s().contains("outside the hash tag 'q'"),
                "run {run}: {refused:?}"
            );
        }
        // No keys, an untagged key, two tags: not held to anything.
        assert_eq!(
            c.cmd(&["EVAL", READS_A_KEY, "0", "elsewhere"]),
            V::Str("v".into())
        );
        assert_eq!(
            c.cmd(&["EVAL", READS_A_KEY, "1", "plain", "{r}:b"]),
            V::Str("theirs".into())
        );
        assert_eq!(
            c.cmd(&["EVAL", READS_A_KEY, "2", "{q}:a", "{r}:a", "elsewhere"]),
            V::Str("v".into())
        );
    }

    // Nor may it look at keys it does not name.
    let refused = c.cmd(&["EVAL", "return redis.call('DBSIZE')", "1", "{q}:a"]);
    assert!(refused.is_err(), "{refused:?}");
    assert!(
        refused.s().contains("looks at keys it does not name"),
        "{refused:?}"
    );
    // A client may, and a script with no tag.
    assert!(matches!(c.cmd(&["DBSIZE"]), V::Int(3)));
    assert!(matches!(
        c.cmd(&["EVAL", "return redis.call('DBSIZE')", "0"]),
        V::Int(3)
    ));

    // Inside a transaction too.
    assert!(c.cmd(&["MULTI"]).ok());
    assert_eq!(
        c.cmd(&["EVAL", READS_A_KEY, "1", "{q}:a", "elsewhere"]).s(),
        "QUEUED"
    );
    let V::Arr(replies) = c.cmd(&["EXEC"]) else {
        panic!("EXEC did not answer with an array")
    };
    assert!(
        replies[0].s().contains("outside the hash tag 'q'"),
        "{replies:?}"
    );

    // Switched off, it may again.
    assert!(c.cmd(&["CONFIG", "SET", "lua-parallel-scripts", "no"]).ok());
    assert_eq!(
        c.cmd(&["EVAL", READS_A_KEY, "1", "{q}:a", "elsewhere"]),
        V::Str("v".into())
    );
}

/// Counts to ARGV[1], writes KEYS[1]. Quick or slow as it is asked to be.
const COUNTS: &str = "local n = 0 for i = 1, tonumber(ARGV[1]) do n = n + 1 end \
                      redis.call('SET', KEYS[1], ARGV[2]) return n";

/// A script that is busy on one tag does not hold up a script on another,
/// nor a client's command on another; a client's command on its own tag
/// waits for it, and sees what it wrote.
#[test]
fn scripts_on_two_tags_run_at_once_and_a_command_waits_for_its_own() {
    let server = Server::start(&["--lua-parallel-scripts", "true"]);
    let mut c = server.conn();
    let V::Str(sha) = c.cmd(&["SCRIPT", "LOAD", COUNTS]) else {
        panic!("SCRIPT LOAD did not answer with a digest")
    };
    assert!(c.cmd(&["SET", "{a}:k", "before"]).ok());
    assert!(c.cmd(&["SET", "{b}:k", "before"]).ok());
    // What the reads below read: a key of the other tag that nothing writes,
    // so that they all read the same whichever comes first.
    assert!(c.cmd(&["SET", "{b}:still", "still"]).ok());
    // Seen to be quick, on both tags, so that it is run beside others.
    for _ in 0..4 {
        assert_eq!(
            c.cmd(&["EVALSHA", &sha, "1", "{a}:k", "1", "before"]),
            V::Int(1)
        );
        assert_eq!(
            c.cmd(&["EVALSHA", &sha, "1", "{b}:k", "1", "before"]),
            V::Int(1)
        );
    }

    // How many steps make the slow script last about a second here. A fixed
    // count did on some machines and not others: a fast CI runner finished
    // 60 million in 325 ms, too soon to tell waiting from not waiting, and
    // the guard below failed the test (#50). It is measured with a copy of
    // the script under another digest, on a tag of its own: a script the
    // server has seen to be slow runs alone from then on, and the one timed
    // here must not be the one the test needs run beside others.
    let steps = {
        const MEASURE: u64 = 5_000_000;
        let copy = format!("{COUNTS} -- timed once to size the slow run");
        let timed = Instant::now();
        let reply = c.cmd(&["EVAL", &copy, "1", "{c}:k", &MEASURE.to_string(), "x"]);
        assert_eq!(reply, V::Int(MEASURE as i64));
        let per_step = timed.elapsed().as_secs_f64() / MEASURE as f64;
        // Never fewer than the 60 million this used to run.
        ((1.0 / per_step) as u64).clamp(60_000_000, 2_000_000_000)
    };

    let port = server.port;
    let started = Instant::now();
    let slow_sha = sha.clone();
    let slow_steps = steps.to_string();
    let slow = std::thread::spawn(move || {
        let mut c = Conn::connect(port);
        let reply = c.cmd(&["EVALSHA", &slow_sha, "1", "{a}:k", &slow_steps, "after"]);
        (reply, started.elapsed())
    });
    std::thread::sleep(Duration::from_millis(100));

    let probe = |args: Vec<String>| {
        std::thread::spawn(move || {
            let mut c = Conn::connect(port);
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            let reply = c.cmd(&args);
            (reply, started.elapsed())
        })
    };
    let words = |words: &[&str]| words.iter().map(|w| w.to_string()).collect::<Vec<_>>();
    // Several connections for each thing that must not wait: the slow script
    // is run by the thread its connection is on, and a connection that
    // lands on that same thread waits for the thread, whatever its key. One
    // of several that is answered early shows that the key did not wait.
    let several = |args: Vec<String>| (0..6).map(|_| probe(args.clone())).collect::<Vec<_>>();
    let other_script = several(words(&["EVALSHA", &sha, "1", "{b}:k", "1", "other"]));
    let other_read = several(words(&["GET", "{b}:still"]));
    let untagged = several(words(&["SET", "plain", "v"]));
    let same_tag = probe(words(&["GET", "{a}:k"]));
    let earliest = |probes: Vec<std::thread::JoinHandle<(V, Duration)>>| {
        let done: Vec<(V, Duration)> = probes.into_iter().map(|p| p.join().unwrap()).collect();
        let reply = done[0].0.clone();
        assert!(done.iter().all(|(r, _)| *r == reply), "{done:?}");
        (reply, done.iter().map(|(_, at)| *at).min().unwrap())
    };

    let (slow_reply, slow_done) = slow.join().unwrap();
    assert_eq!(slow_reply, V::Int(steps as i64));
    assert!(
        slow_done > Duration::from_millis(400),
        "the slow script took only {slow_done:?} for {steps} steps, sized to take about a \
         second: the test proves nothing on this machine"
    );

    let margin = Duration::from_millis(100);
    let (reply, done) = earliest(other_script);
    assert_eq!(reply, V::Int(1));
    assert!(
        done + margin < slow_done,
        "a script on another tag waited: {done:?} of {slow_done:?}"
    );
    let (reply, done) = earliest(other_read);
    assert!(matches!(reply, V::Str(_)), "{reply:?}");
    assert!(
        done + margin < slow_done,
        "a read on another tag waited: {done:?} of {slow_done:?}"
    );
    let (reply, done) = earliest(untagged);
    assert!(reply.ok());
    assert!(
        done + margin < slow_done,
        "a key with no tag waited: {done:?} of {slow_done:?}"
    );

    // Its own tag: not before the script was done, and with what it wrote.
    let (reply, done) = same_tag.join().unwrap();
    assert_eq!(reply, V::Str("after".into()));
    assert!(
        done + Duration::from_millis(20) >= slow_done,
        "a read of the script's own tag did not wait: {done:?} of {slow_done:?}"
    );
}
