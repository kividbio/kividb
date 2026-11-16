// tests/search_walks.rs
//
// The walks a search command makes over every document of an index — the
// backfill of `FT.CREATE` and `FT.ALTER`, the deletions of `FT.DROPINDEX DD`
// — against a write to a document made while the walk is under way (#275).
//
// Real `kividb` processes, each started with `KIVIDB_TEST_FAILPOINTS`: at a
// step named there the server creates `failpoint.<step>.<n>` in its working
// directory and waits while that file is there. A test waits for the file,
// makes the write, and removes the file to let the walk go on. The AOF is on,
// so that a write holds its keys' write order (as every write does with
// #271).

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
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
            V::Str(s) => s,
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

    fn cmd_bytes(&mut self, args: &[&[u8]]) -> V {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            buf.extend_from_slice(a);
            buf.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&buf).unwrap();
        self.read()
    }

    fn cmd(&mut self, args: &[&str]) -> V {
        let args: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
        self.cmd_bytes(&args)
    }

    fn read(&mut self) -> V {
        let mut line = String::new();
        self.reader
            .read_line(&mut line)
            .expect("no reply in 30 s: the server hangs");
        let line = line.trim_end_matches(['\r', '\n']);
        assert!(!line.is_empty(), "connection closed");
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

// ── Server fixture ────────────────────────────────────────────────────────────

/// Whether nothing has `port`, asked with a socket that is bound and never
/// listens (see tests/backup.rs).
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

/// A port nothing is listening on, outside the range the system hands out
/// to outgoing connections.
fn free_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
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

struct Server {
    port: u16,
    dir: PathBuf,
    child: Child,
}

impl Server {
    /// A server with the AOF on, stopping at the failpoint `step`.
    fn start(step: &str) -> Self {
        let dir = tempfile::tempdir().unwrap().keep();
        let port = free_port();
        let log = std::fs::File::create(dir.join("server.log")).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_kividb"))
            .current_dir(&dir)
            .args(["--port", &port.to_string(), "--bind", "127.0.0.1", "--aof"])
            .env("KIVIDB_TEST_FAILPOINTS", step)
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
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

    /// Run `args` on a connection of its own, and `meanwhile` once the
    /// server has stopped at `failpoint.<step>.1`; then let it go on, and
    /// give back what `args` was answered. `meanwhile` is given the port,
    /// and runs its writes on threads of their own, not waited for: one
    /// that waits for the walk would otherwise hold the test up.
    fn run_stopping_at(
        &self,
        step: &str,
        args: Vec<Vec<u8>>,
        meanwhile: impl FnOnce(u16) -> Vec<std::thread::JoinHandle<V>>,
    ) -> (V, Vec<V>) {
        let port = self.port;
        let walk = std::thread::spawn(move || {
            let refs: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();
            Conn::connect(port).cmd_bytes(&refs)
        });
        let failpoint = self.dir.join(format!("failpoint.{step}.1"));
        wait_for(&failpoint);
        let writes = meanwhile(port);
        // Time for a write that does not wait to be made.
        std::thread::sleep(Duration::from_millis(300));
        std::fs::remove_file(&failpoint).unwrap();
        let reply = walk.join().unwrap();
        let written = writes.into_iter().map(|w| w.join().unwrap()).collect();
        (reply, written)
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn wait_for(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "{} never appeared",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// A command run on a thread of its own, on a connection of its own.
fn spawn_cmd(port: u16, args: &[&str]) -> std::thread::JoinHandle<V> {
    let args: Vec<String> = args.iter().map(|a| a.to_string()).collect();
    std::thread::spawn(move || {
        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
        Conn::connect(port).cmd(&refs)
    })
}

fn owned(args: &[&str]) -> Vec<Vec<u8>> {
    args.iter().map(|a| a.as_bytes().to_vec()).collect()
}

fn vector(values: &[f32]) -> Vec<u8> {
    values.iter().flat_map(|v| v.to_le_bytes()).collect()
}

/// The documents an `FT.SEARCH` reply names.
fn found(reply: &V) -> Vec<String> {
    let items = reply.arr();
    items[1..]
        .iter()
        .filter_map(|item| match item {
            V::Str(key) => Some(key.clone()),
            _ => None,
        })
        .collect()
}

// ── FT.ALTER ──────────────────────────────────────────────────────────────────

/// A vector field added by `FT.ALTER` holds a document's vector as it is
/// after a write made while the backfill was at it, not the backfill's
/// older copy.
#[test]
fn alter_backfill_of_a_vector_does_not_put_back_an_older_one() {
    let server = Server::start("index-walk-read");
    let mut c = server.conn();
    let create = c.cmd(&[
        "FT.CREATE",
        "idx",
        "ON",
        "JSON",
        "PREFIX",
        "1",
        "doc:",
        "SCHEMA",
        "$.t",
        "AS",
        "t",
        "TEXT",
    ]);
    assert!(create.ok(), "{create:?}");
    let set = c.cmd(&["JSON.SET", "doc:1", "$", r#"{"t":"hello","v":[1,0]}"#]);
    assert!(set.ok(), "{set:?}");

    let alter = owned(&[
        "FT.ALTER",
        "idx",
        "SCHEMA",
        "ADD",
        "$.v",
        "AS",
        "v",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "2",
        "DISTANCE_METRIC",
        "L2",
    ]);
    let (reply, written) = server.run_stopping_at("index-walk-read", alter, |port| {
        // The vector becomes [1, 1].
        vec![spawn_cmd(port, &["JSON.NUMINCRBY", "doc:1", "$.v[1]", "1"])]
    });
    assert!(reply.ok(), "FT.ALTER: {reply:?}");
    assert_eq!(written[0].s(), "[1]");

    // The nearest to [1, 1] is the document, at distance 0.
    let q = vector(&[1.0, 1.0]);
    let reply = c.cmd_bytes(&[
        b"FT.SEARCH",
        b"idx",
        b"*=>[KNN 1 @v $q AS d]",
        b"PARAMS",
        b"2",
        b"q",
        &q,
        b"RETURN",
        b"1",
        b"d",
        b"DIALECT",
        b"2",
    ]);
    let items = reply.arr();
    assert_eq!(items[0].i(), 1, "{reply:?}");
    let fields = items[2].arr();
    let distance: f64 = fields[1].s().parse().unwrap();
    assert!(
        distance.abs() < 1e-6,
        "the index holds the older vector: {reply:?}"
    );
}

/// The same for a TEXT field: the words are the document's as written.
#[test]
fn alter_backfill_of_text_does_not_put_back_older_words() {
    let server = Server::start("index-walk-read");
    let mut c = server.conn();
    let create = c.cmd(&[
        "FT.CREATE",
        "idx",
        "ON",
        "JSON",
        "PREFIX",
        "1",
        "doc:",
        "SCHEMA",
        "$.n",
        "AS",
        "n",
        "NUMERIC",
    ]);
    assert!(create.ok(), "{create:?}");
    let set = c.cmd(&["JSON.SET", "doc:1", "$", r#"{"n":1,"t":"hello"}"#]);
    assert!(set.ok(), "{set:?}");

    let alter = owned(&["FT.ALTER", "idx", "SCHEMA", "ADD", "$.t", "AS", "t", "TEXT"]);
    let (reply, written) = server.run_stopping_at("index-walk-read", alter, |port| {
        vec![spawn_cmd(
            port,
            &["JSON.STRAPPEND", "doc:1", "$.t", r#"" world""#],
        )]
    });
    assert!(reply.ok(), "FT.ALTER: {reply:?}");
    assert_eq!(written[0].arr(), [V::Int(11)]);

    let reply = c.cmd(&["FT.SEARCH", "idx", "@t:world", "NOCONTENT"]);
    assert_eq!(found(&reply), ["doc:1"], "{reply:?}");
}

/// And for a TAG field (its postings).
#[test]
fn alter_backfill_of_a_tag_does_not_put_back_an_older_one() {
    let server = Server::start("index-walk-read");
    let mut c = server.conn();
    let create = c.cmd(&[
        "FT.CREATE",
        "idx",
        "ON",
        "JSON",
        "PREFIX",
        "1",
        "doc:",
        "SCHEMA",
        "$.n",
        "AS",
        "n",
        "NUMERIC",
    ]);
    assert!(create.ok(), "{create:?}");
    let set = c.cmd(&["JSON.SET", "doc:1", "$", r#"{"n":1,"tag":"red"}"#]);
    assert!(set.ok(), "{set:?}");

    let alter = owned(&[
        "FT.ALTER", "idx", "SCHEMA", "ADD", "$.tag", "AS", "tag", "TAG",
    ]);
    let (reply, written) = server.run_stopping_at("index-walk-read", alter, |port| {
        vec![spawn_cmd(
            port,
            &["JSON.STRAPPEND", "doc:1", "$.tag", r#""dish""#],
        )]
    });
    assert!(reply.ok(), "FT.ALTER: {reply:?}");
    assert_eq!(written[0].arr(), [V::Int(7)]);

    let reply = c.cmd(&["FT.SEARCH", "idx", "@tag:{reddish}", "NOCONTENT"]);
    assert_eq!(found(&reply), ["doc:1"], "{reply:?}");
    let reply = c.cmd(&["FT.SEARCH", "idx", "@tag:{red}", "NOCONTENT"]);
    assert!(found(&reply).is_empty(), "{reply:?}");
}

// ── FT.CREATE ─────────────────────────────────────────────────────────────────

/// A document written right after `FT.CREATE` has looked through the
/// documents already there is indexed. The index was registered only after
/// that walk, and a document written in between was in no index until its
/// next write.
#[test]
fn a_document_written_while_create_indexes_is_indexed() {
    let server = Server::start("ft-create-walked");
    let mut c = server.conn();
    assert_eq!(c.cmd(&["HSET", "doc:1", "t", "hello"]).i(), 1);

    let create = owned(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "doc:",
        "SCHEMA",
        "t",
        "TEXT",
    ]);
    let (reply, written) = server.run_stopping_at("ft-create-walked", create, |port| {
        vec![spawn_cmd(port, &["HSET", "doc:2", "t", "hello"])]
    });
    assert!(reply.ok(), "FT.CREATE: {reply:?}");
    assert_eq!(written[0].i(), 1);

    let reply = c.cmd(&["FT.SEARCH", "idx", "@t:hello", "NOCONTENT"]);
    let mut docs = found(&reply);
    docs.sort();
    assert_eq!(docs, ["doc:1", "doc:2"], "{reply:?}");
}

// ── FT.DROPINDEX DD ───────────────────────────────────────────────────────────

/// `DD` deletes what is a document of the index when it deletes it: a key
/// written as a string after `DD` found the documents, and before it
/// deleted them, is kept, as it would have been had the write come first.
#[test]
fn dropindex_dd_keeps_a_key_that_is_no_longer_a_document() {
    let server = Server::start("dropindex-collected");
    let mut c = server.conn();
    let create = c.cmd(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "doc:",
        "SCHEMA",
        "t",
        "TEXT",
    ]);
    assert!(create.ok(), "{create:?}");
    assert_eq!(c.cmd(&["HSET", "doc:1", "t", "a"]).i(), 1);
    assert_eq!(c.cmd(&["HSET", "doc:2", "t", "b"]).i(), 1);

    let drop = owned(&["FT.DROPINDEX", "idx", "DD"]);
    let (reply, written) = server.run_stopping_at("dropindex-collected", drop, |port| {
        vec![spawn_cmd(port, &["SET", "doc:1", "a string now"])]
    });
    assert!(reply.ok(), "FT.DROPINDEX: {reply:?}");
    assert!(written[0].ok());

    assert_eq!(c.cmd(&["GET", "doc:1"]).s(), "a string now");
    assert_eq!(c.cmd(&["EXISTS", "doc:2"]).i(), 0);
}
