// tests/redis_differences.rs
//
// Small differences from Redis 8.10 in argument checking, error texts and
// replies (#561, #562, #564, #566, #576), over the wire. Each test runs
// against a real `kividb` process of its own, on the tokio loop and, on
// Linux, on io_uring too: several of these are answered by the connection
// loops, which each have their own copy (`src/server.rs`,
// `src/server_uring.rs`).

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

// ── RESP client ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum V {
    Simple(String),
    Bulk(String),
    Err(String),
    Int(i64),
    Arr(Vec<V>),
    Nil,
}

impl V {
    fn s(&self) -> &str {
        match self {
            V::Simple(s) | V::Bulk(s) => s,
            other => panic!("not a string: {other:?}"),
        }
    }
    fn i(&self) -> i64 {
        match self {
            V::Int(n) => *n,
            other => panic!("not an integer: {other:?}"),
        }
    }
    fn err(&self) -> &str {
        match self {
            V::Err(e) => e,
            other => panic!("not an error: {other:?}"),
        }
    }
    fn arr(&self) -> &[V] {
        match self {
            V::Arr(v) => v,
            other => panic!("not an array: {other:?}"),
        }
    }
    fn ok(&self) -> bool {
        matches!(self, V::Simple(s) if s == "OK")
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
                stream
                    .set_read_timeout(Some(Duration::from_secs(20)))
                    .unwrap();
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
            "+" => V::Simple(rest.to_string()),
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
                V::Bulk(String::from_utf8_lossy(&data).into_owned())
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

// ── Servers ───────────────────────────────────────────────────────────────────

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

/// A free port outside the range the system gives outgoing connections.
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

struct Server {
    port: u16,
    child: Child,
    dir: std::path::PathBuf,
}

impl Server {
    fn start(io_model: &str, extra: &[&str]) -> Self {
        let dir = tempfile::tempdir().unwrap().keep();
        let port = free_port();
        let log = std::fs::File::create(dir.join("server.log")).unwrap();
        let port_text = port.to_string();
        let mut args = vec![
            "--port",
            &port_text,
            "--io-model",
            io_model,
            "--threads",
            "2",
            "--configfile",
            "kividb.conf",
        ];
        if !extra.contains(&"--bind") {
            args.extend(["--bind", "127.0.0.1"]);
        }
        args.extend(extra);
        let child = Command::new(env!("CARGO_BIN_EXE_kividb"))
            .current_dir(&dir)
            .args(&args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .expect("could not start kividb");
        let server = Server { port, child, dir };
        assert_eq!(server.conn().cmd(&["PING"]), V::Simple("PONG".into()));
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

/// The connection loops a test runs on: tokio everywhere, io_uring on Linux.
fn io_models() -> &'static [&'static str] {
    if cfg!(target_os = "linux") {
        &["tokio", "uring"]
    } else {
        &["tokio"]
    }
}

/// Run `test` on a server of its own on each connection loop.
fn on_each_loop(extra: &[&str], test: impl Fn(&Server, &str)) {
    for io_model in io_models() {
        let server = Server::start(io_model, extra);
        test(&server, io_model);
    }
}

fn info_field(c: &mut Conn, section: &str, field: &str) -> String {
    let info = c.cmd(&["INFO", section]);
    info.s()
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{field}:")))
        .unwrap_or_else(|| panic!("no {field} in INFO {section}"))
        .to_string()
}

const XGROUP_NO_KEY: &str = "ERR The XGROUP subcommand requires the key to exist. Note that for \
                             CREATE you may want to use the MKSTREAM option to create an empty \
                             stream automatically.";

// ── #561: arguments ───────────────────────────────────────────────────────────

/// `FLUSHALL` and `FLUSHDB` take `ASYNC` or `SYNC`, and nothing else: an
/// unknown word was a synchronous flush, and more words were ignored.
#[test]
fn flush_refuses_what_is_not_async_or_sync() {
    let server = Server::start("tokio", &[]);
    let mut c = server.conn();
    c.cmd(&["SET", "k", "v"]);
    for args in [
        &["FLUSHALL", "foo"][..],
        &["FLUSHALL", "SYNC", "extra"],
        &["FLUSHDB", "foo"],
        &["FLUSHDB", "ASYNC", "extra"],
    ] {
        assert_eq!(c.cmd(args).err(), "ERR syntax error", "{args:?}");
    }
    // Nothing was flushed by the refused ones.
    assert_eq!(c.cmd(&["DBSIZE"]).i(), 1);
    assert!(c.cmd(&["FLUSHDB", "sync"]).ok());
    c.cmd(&["SET", "k", "v"]);
    assert!(c.cmd(&["FLUSHALL", "Async"]).ok());
    c.cmd(&["SET", "k", "v"]);
    assert!(c.cmd(&["FLUSHALL"]).ok());
    assert_eq!(c.cmd(&["DBSIZE"]).i(), 0);
}

/// `WAIT` takes two integers, the timeout not negative: each used to be 0
/// when it was not a number, and a negative timeout waited for ever.
/// Without replicas it waits out its timeout and answers 0, as Redis does.
#[test]
fn wait_checks_its_arguments_and_waits_without_replicas() {
    on_each_loop(&[], |server, io| {
        let mut c = server.conn();
        assert_eq!(
            c.cmd(&["WAIT", "x", "0"]).err(),
            "ERR value is not an integer or out of range",
            "{io}"
        );
        assert_eq!(
            c.cmd(&["WAIT", "1", "x"]).err(),
            "ERR timeout is not an integer or out of range",
            "{io}"
        );
        assert_eq!(
            c.cmd(&["WAIT", "1", "-5"]).err(),
            "ERR timeout is negative",
            "{io}"
        );
        assert!(
            c.cmd(&["WAIT", "1", "2", "3"])
                .err()
                .starts_with("ERR wrong number"),
            "{io}"
        );
        assert_eq!(c.cmd(&["WAIT", "0", "0"]).i(), 0, "{io}");
        assert_eq!(c.cmd(&["WAIT", "-1", "0"]).i(), 0, "{io}");
        let started = Instant::now();
        assert_eq!(c.cmd(&["WAIT", "1", "300"]).i(), 0, "{io}");
        assert!(
            started.elapsed() >= Duration::from_millis(250),
            "{io}: WAIT 1 300 answered after {:?}",
            started.elapsed()
        );
    });
}

/// `LOLWUT VERSION` takes an integer, and so do the options of versions 5
/// and 6.
#[test]
fn lolwut_checks_its_version_and_options() {
    let server = Server::start("tokio", &[]);
    let mut c = server.conn();
    let not_int = "ERR value is not an integer or out of range";
    assert_eq!(c.cmd(&["LOLWUT", "VERSION", "x"]).err(), not_int);
    assert_eq!(c.cmd(&["LOLWUT", "VERSION", "5", "x"]).err(), not_int);
    assert_eq!(c.cmd(&["LOLWUT", "VERSION", "6", "1", "x"]).err(), not_int);
    assert!(matches!(
        c.cmd(&["LOLWUT", "VERSION", "6", "1", "2"]),
        V::Bulk(_)
    ));
    assert!(matches!(
        c.cmd(&["LOLWUT", "VERSION", "8", "x"]),
        V::Bulk(_)
    ));
    assert!(matches!(c.cmd(&["LOLWUT", "foo"]), V::Bulk(_)));
}

/// `COMMAND INFO` reports RedisJSON's arity for the JSON commands, not a
/// guess from where their keys are.
#[test]
fn command_info_has_the_json_arities() {
    let server = Server::start("tokio", &[]);
    let mut c = server.conn();
    for (name, arity) in [
        ("JSON.ARRAPPEND", -3),
        ("JSON.ARRINSERT", -5),
        ("JSON.MSET", -4),
        ("JSON.SET", -4),
        ("JSON.MGET", -3),
        ("JSON.ARRTRIM", 5),
        ("JSON.TOGGLE", 3),
        ("JSON.GET", -2),
    ] {
        let info = c.cmd(&["COMMAND", "INFO", name]);
        assert_eq!(info.arr()[0].arr()[1].i(), arity, "{name}");
    }
}

// ── #562: error texts and HELP ────────────────────────────────────────────────

#[test]
fn help_subcommands_and_unknown_subcommands_are_as_in_redis() {
    let server = Server::start("tokio", &[]);
    let mut c = server.conn();
    for (command, first) in [
        (
            "XGROUP",
            "XGROUP <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        ),
        (
            "XINFO",
            "XINFO <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        ),
        (
            "SLOWLOG",
            "SLOWLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        ),
    ] {
        let help = c.cmd(&[command, "HELP"]);
        assert_eq!(help.arr()[0], V::Simple(first.into()), "{command}");
        // Every line a simple string, as Redis's `addReplyHelp` writes them.
        assert!(
            help.arr().iter().all(|line| matches!(line, V::Simple(_))),
            "{command} HELP: {help:?}"
        );
        assert_eq!(
            *help.arr().last().unwrap(),
            V::Simple("    Print this help.".into())
        );
    }
    for command in ["XGROUP", "XINFO", "PUBSUB", "SLOWLOG", "MODULE", "CLUSTER"] {
        let args: &[&str] = if command == "XINFO" {
            &[command, "foo", "k"]
        } else {
            &[command, "foo"]
        };
        assert_eq!(
            c.cmd(args).err(),
            format!("ERR unknown subcommand 'foo'. Try {command} HELP."),
            "{command}"
        );
    }
    // CLUSTER HELP on a standalone server: as in Redis.
    assert_eq!(
        c.cmd(&["CLUSTER", "HELP"]).err(),
        "ERR This instance has cluster support disabled"
    );
    let module = c.cmd(&["MODULE", "LOAD", "x"]);
    assert!(!module.err().contains("COMMANDS.md"), "{module:?}");
    assert_eq!(
        c.cmd(&["BACKUP", "LIST", "x"]).err(),
        "ERR wrong number of arguments for 'backup|list' command"
    );
    c.cmd(&["HSET", "h", "f", "v"]);
    let text = "ERR The `numfields` parameter must match the number of arguments";
    assert_eq!(c.cmd(&["HPTTL", "h", "FIELDS", "2", "f"]).err(), text);
}

#[test]
fn cluster_help_on_a_cluster_node() {
    let server = Server::start("tokio", &["--cluster-enabled", "yes"]);
    let mut c = server.conn();
    let help = c.cmd(&["CLUSTER", "HELP"]);
    assert_eq!(
        help.arr()[0],
        V::Simple("CLUSTER <subcommand> [<arg> [value] [opt] ...]. Subcommands are:".into())
    );
    assert!(help.arr().contains(&V::Simple(
        "SETSLOT <slot> (IMPORTING <node-id>|MIGRATING <node-id>|STABLE|NODE <node-id>)".into()
    )));
    assert_eq!(
        c.cmd(&["CLUSTER", "foo"]).err(),
        "ERR unknown subcommand 'foo'. Try CLUSTER HELP."
    );
}

#[cfg(feature = "lua")]
#[test]
fn script_and_function_texts_are_as_in_redis() {
    let server = Server::start("tokio", &[]);
    let mut c = server.conn();
    let not_busy = "NOTBUSY No scripts in execution right now.";
    assert_eq!(c.cmd(&["SCRIPT", "KILL"]).err(), not_busy);
    assert_eq!(c.cmd(&["FUNCTION", "KILL"]).err(), not_busy);
    assert_eq!(
        c.cmd(&["FUNCTION", "foo"]).err(),
        "ERR unknown subcommand 'foo'. Try FUNCTION HELP."
    );
    assert_eq!(
        c.cmd(&["SCRIPT", "foo"]).err(),
        "ERR unknown subcommand 'foo'. Try SCRIPT HELP."
    );
}

/// While a function runs past the busy threshold, `BUSY` names `FUNCTION
/// KILL`; it named `SCRIPT KILL`.
#[cfg(feature = "lua")]
#[test]
fn busy_names_function_kill_while_a_function_runs() {
    let server = Server::start("tokio", &[]);
    let mut c = server.conn();
    assert!(
        c.cmd(&["CONFIG", "SET", "busy-reply-threshold", "100"])
            .ok()
    );
    let lib = "#!lua name=spin\nredis.register_function('spin', function() \
               local t = redis.call('TIME') local s = tonumber(t[1]) \
               while true do local n = redis.call('TIME') \
               if tonumber(n[1]) - s >= 2 then break end end return 1 end)";
    assert_eq!(c.cmd(&["FUNCTION", "LOAD", lib]), V::Bulk("spin".into()));
    c.writer
        .write_all(b"*3\r\n$5\r\nFCALL\r\n$4\r\nspin\r\n$1\r\n0\r\n")
        .unwrap();
    std::thread::sleep(Duration::from_millis(400));
    let mut other = server.conn();
    assert_eq!(
        other.cmd(&["GET", "k"]).err(),
        "BUSY Redis is busy running a script. You can only call FUNCTION KILL or SHUTDOWN NOSAVE."
    );
    assert_eq!(other.cmd(&["FUNCTION", "KILL"]), V::Simple("OK".into()));
    let _ = c.read();
}

// ── #564: streams and Pub/Sub ─────────────────────────────────────────────────

#[test]
fn xgroup_on_a_missing_key_and_xpending_order() {
    let server = Server::start("tokio", &[]);
    let mut c = server.conn();
    for args in [
        &["XGROUP", "CREATE", "missing", "g", "$"][..],
        &["XGROUP", "SETID", "missing", "g", "$"],
        &["XGROUP", "CREATECONSUMER", "missing", "g", "c"],
        &["XGROUP", "DELCONSUMER", "missing", "g", "c"],
        &["XGROUP", "DESTROY", "missing", "g"],
    ] {
        assert_eq!(c.cmd(args).err(), XGROUP_NO_KEY, "{args:?}");
    }

    assert!(c.cmd(&["XGROUP", "CREATE", "s", "g", "$", "MKSTREAM"]).ok());
    for _ in 0..3 {
        c.cmd(&["XADD", "s", "*", "f", "v"]);
    }
    for consumer in ["zed", "bob", "alice"] {
        c.cmd(&[
            "XREADGROUP",
            "GROUP",
            "g",
            consumer,
            "COUNT",
            "1",
            "STREAMS",
            "s",
            ">",
        ]);
    }
    let summary = c.cmd(&["XPENDING", "s", "g"]);
    let names: Vec<&str> = summary.arr()[3]
        .arr()
        .iter()
        .map(|pair| pair.arr()[0].s())
        .collect();
    assert_eq!(names, ["alice", "bob", "zed"]);
}

/// `XGROUP CREATE` takes its options after the ID only, and refuses an ID
/// that is not one (#576).
#[test]
fn xgroup_create_options_come_after_the_id() {
    let server = Server::start("tokio", &[]);
    let mut c = server.conn();
    // A key or group called `mkstream` is not the option.
    assert_eq!(
        c.cmd(&["XGROUP", "CREATE", "mkstream", "g", "$"]).err(),
        XGROUP_NO_KEY
    );
    assert_eq!(
        c.cmd(&["XGROUP", "CREATE", "k", "mkstream", "$"]).err(),
        XGROUP_NO_KEY
    );
    assert_eq!(c.cmd(&["EXISTS", "mkstream", "k"]).i(), 0);
    // An ID that is not one is refused, not taken as `$`.
    c.writer
        .write_all(b"*6\r\n$6\r\nXGROUP\r\n$6\r\nCREATE\r\n$1\r\nk\r\n$1\r\ng\r\n$1\r\n\xff\r\n$8\r\nMKSTREAM\r\n")
        .unwrap();
    assert_eq!(
        c.read().err(),
        "ERR Invalid stream ID specified as stream command argument"
    );
    // A word that is no option is refused, not ignored.
    assert_eq!(
        c.cmd(&["XGROUP", "CREATE", "k", "g", "$", "MKSTREAM", "foo"])
            .err(),
        "ERR unknown subcommand or wrong number of arguments for 'CREATE'. Try XGROUP HELP."
    );
    assert!(c.cmd(&["XGROUP", "CREATE", "k", "g", "$", "MKSTREAM"]).ok());
}

/// Shard (un)subscribes count shard channels; the others count channels and
/// patterns. They were all counted together.
#[test]
fn shard_subscriptions_are_counted_on_their_own() {
    on_each_loop(&[], |server, io| {
        let mut c = server.conn();
        let ack = |kind: &str, name: &str, n: i64| {
            V::Arr(vec![V::Bulk(kind.into()), V::Bulk(name.into()), V::Int(n)])
        };
        assert_eq!(c.cmd(&["SUBSCRIBE", "a"]), ack("subscribe", "a", 1), "{io}");
        assert_eq!(
            c.cmd(&["SSUBSCRIBE", "s"]),
            ack("ssubscribe", "s", 1),
            "{io}"
        );
        assert_eq!(
            c.cmd(&["PSUBSCRIBE", "p*"]),
            ack("psubscribe", "p*", 2),
            "{io}"
        );
        assert_eq!(
            c.cmd(&["SUNSUBSCRIBE", "s"]),
            ack("sunsubscribe", "s", 0),
            "{io}"
        );
        assert_eq!(
            c.cmd(&["UNSUBSCRIBE", "a"]),
            ack("unsubscribe", "a", 1),
            "{io}"
        );
    });
}

// ── #566: server and client ───────────────────────────────────────────────────

/// `CLIENT NO-EVICT on` shows as `e`; `laddr` is the address connected to,
/// not the one bound; `RESET` forgets the `HIMPORT` fieldsets and NO-EVICT.
#[test]
fn client_flags_laddr_and_reset() {
    on_each_loop(&["--bind", "0.0.0.0"], |server, io| {
        let mut c = server.conn();
        assert!(c.cmd(&["CLIENT", "NO-EVICT", "on"]).ok());
        let info = c.cmd(&["CLIENT", "INFO"]);
        let field = |info: &V, name: &str| -> String {
            info.s()
                .split_whitespace()
                .find_map(|kv| kv.strip_prefix(&format!("{name}=")))
                .unwrap_or_else(|| panic!("no {name} in {info:?}"))
                .to_string()
        };
        assert_eq!(field(&info, "flags"), "e", "{io}");
        assert_eq!(
            field(&info, "laddr"),
            format!("127.0.0.1:{}", server.port),
            "{io}"
        );

        assert!(c.cmd(&["HIMPORT", "PREPARE", "fs", "a", "b"]).ok());
        assert_eq!(c.cmd(&["RESET"]), V::Simple("RESET".into()));
        assert_eq!(field(&c.cmd(&["CLIENT", "INFO"]), "flags"), "N", "{io}");
        assert!(
            c.cmd(&["HIMPORT", "SET", "k", "fs", "1", "2"])
                .err()
                .contains("fieldset"),
            "{io}"
        );
    });
}

/// `CONFIG RESETSTAT` resets statistics; how much has changed since the
/// last save is not one of them.
#[test]
fn resetstat_keeps_the_changes_since_the_last_save() {
    let server = Server::start("tokio", &[]);
    let mut c = server.conn();
    c.cmd(&["SET", "a", "1"]);
    c.cmd(&["SET", "b", "1"]);
    let before = info_field(&mut c, "persistence", "rdb_changes_since_last_save");
    assert_ne!(before, "0");
    assert!(c.cmd(&["CONFIG", "RESETSTAT"]).ok());
    assert_eq!(
        info_field(&mut c, "persistence", "rdb_changes_since_last_save"),
        before
    );
}

/// One latency sample a second for an event, the highest of that second.
#[test]
fn latency_history_keeps_one_sample_a_second() {
    let server = Server::start("tokio", &[]);
    let mut c = server.conn();
    assert!(
        c.cmd(&["CONFIG", "SET", "latency-monitor-threshold", "1"])
            .ok()
    );
    for _ in 0..5 {
        c.cmd(&["DEBUG", "SLEEP", "0.003"]);
    }
    let history = c.cmd(&["LATENCY", "HISTORY", "command"]);
    let seconds: Vec<i64> = history.arr().iter().map(|s| s.arr()[0].i()).collect();
    let mut distinct = seconds.clone();
    distinct.dedup();
    assert_eq!(seconds, distinct, "two samples for one second: {history:?}");
}

/// `RANDOMKEY` keeps looking past expired keys while live ones are left: it
/// gave up after 64 picks and answered nil.
#[test]
fn randomkey_finds_the_live_key_among_expired_ones() {
    let server = Server::start("tokio", &[]);
    let mut c = server.conn();
    assert!(c.cmd(&["DEBUG", "SET-ACTIVE-EXPIRE", "0"]).ok());
    for i in 0..2000 {
        c.cmd(&["SET", &format!("dead:{i}"), "v", "PX", "1"]);
    }
    c.cmd(&["SET", "live", "v"]);
    std::thread::sleep(Duration::from_millis(20));
    assert_eq!(c.cmd(&["RANDOMKEY"]), V::Bulk("live".into()));
}

/// `HOTKEYS STOP` with nothing collecting answers nil, as Redis 8.10 does,
/// and a session past its `DURATION` is not collecting.
#[test]
fn hotkeys_stop_when_nothing_collects() {
    let server = Server::start("tokio", &[]);
    let mut c = server.conn();
    assert_eq!(c.cmd(&["HOTKEYS", "STOP"]), V::Nil);
    assert!(c.cmd(&["HOTKEYS", "START", "METRICS", "1", "CPU"]).ok());
    assert!(c.cmd(&["HOTKEYS", "STOP"]).ok());
    assert_eq!(c.cmd(&["HOTKEYS", "STOP"]), V::Nil);
    assert!(
        c.cmd(&["HOTKEYS", "START", "METRICS", "1", "CPU", "DURATION", "1"])
            .ok()
    );
    std::thread::sleep(Duration::from_millis(1500));
    assert_eq!(c.cmd(&["HOTKEYS", "STOP"]), V::Nil);
}

/// `GEORADIUS` is a write command, refused on a read-only replica with or
/// without `STORE`; `GEORADIUS_RO` is the read.
#[test]
fn georadius_is_refused_on_a_replica() {
    let master = Server::start("tokio", &[]);
    let mut m = master.conn();
    m.cmd(&["GEOADD", "g", "13.36", "38.11", "palermo"]);
    let port = master.port.to_string();
    let replica = Server::start("tokio", &["--replicaof", "127.0.0.1", &port]);
    let mut r = replica.conn();
    let deadline = Instant::now() + Duration::from_secs(15);
    while info_field(&mut r, "replication", "master_link_status") != "up" {
        assert!(Instant::now() < deadline, "the replica did not sync");
        std::thread::sleep(Duration::from_millis(50));
    }
    let deadline = Instant::now() + Duration::from_secs(15);
    while r.cmd(&["EXISTS", "g"]).i() == 0 {
        assert!(
            Instant::now() < deadline,
            "the key did not reach the replica"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    for name in ["GEORADIUS", "GEORADIUSBYMEMBER"] {
        let args: &[&str] = if name == "GEORADIUS" {
            &[name, "g", "13.36", "38.11", "10", "km"]
        } else {
            &[name, "g", "palermo", "10", "km"]
        };
        assert!(r.cmd(args).err().starts_with("READONLY"), "{name}");
    }
    let found = r.cmd(&["GEORADIUS_RO", "g", "13.36", "38.11", "10", "km"]);
    assert_eq!(found.arr(), [V::Bulk("palermo".into())]);
}

/// Only the ACL subcommands that administer users are `@admin`: a user
/// without it can still ask who it is.
#[test]
fn acl_whoami_is_not_admin() {
    let server = Server::start("tokio", &[]);
    let mut c = server.conn();
    assert!(
        c.cmd(&["ACL", "SETUSER", "u", "on", "nopass", "+@all", "-@admin"])
            .ok()
    );
    let admin = c.cmd(&["ACL", "CAT", "admin"]);
    let names: Vec<String> = admin
        .arr()
        .iter()
        .map(|v| v.s().to_ascii_lowercase())
        .collect();
    assert!(names.contains(&"acl|setuser".to_string()), "{names:?}");
    assert!(!names.contains(&"acl".to_string()), "{names:?}");
    assert!(!names.contains(&"acl|whoami".to_string()), "{names:?}");

    let mut u = server.conn();
    assert!(u.cmd(&["AUTH", "u", "x"]).ok());
    assert_eq!(u.cmd(&["ACL", "WHOAMI"]), V::Bulk("u".into()));
    assert!(matches!(u.cmd(&["ACL", "CAT"]), V::Arr(_)));
    assert!(u.cmd(&["ACL", "SETUSER", "v"]).err().starts_with("NOPERM"));
    assert!(u.cmd(&["ACL", "LIST"]).err().starts_with("NOPERM"));
}

/// `SCRIPT LOAD` compiles the script and refuses one that does not; `SCRIPT
/// DEBUG` takes YES, SYNC or NO.
#[cfg(feature = "lua")]
#[test]
fn script_load_compiles_and_script_debug_checks_its_mode() {
    let server = Server::start("tokio", &[]);
    let mut c = server.conn();
    let refused = c.cmd(&["SCRIPT", "LOAD", "return ("]);
    assert!(
        refused
            .err()
            .starts_with("ERR Error compiling script (new function): user_script:1:"),
        "{refused:?}"
    );
    assert_eq!(
        c.cmd(&["SCRIPT", "LOAD", "#!lua flags=bogus\nreturn 1"])
            .err(),
        "ERR Unexpected flag in script shebang: bogus"
    );
    // Nothing was kept.
    assert_eq!(
        c.cmd(&[
            "SCRIPT",
            "EXISTS",
            "728acb63e2aaef0ee859ece5db586bff5d800d1e"
        ]),
        V::Arr(vec![V::Int(0)])
    );
    let sha = c.cmd(&["SCRIPT", "LOAD", "return 1"]);
    assert_eq!(c.cmd(&["EVALSHA", sha.s(), "0"]).i(), 1);
    assert!(matches!(
        c.cmd(&["SCRIPT", "LOAD", "#!lua flags=no-writes\nreturn 2"]),
        V::Bulk(_)
    ));

    assert_eq!(
        c.cmd(&["SCRIPT", "DEBUG", "anything"]).err(),
        "ERR Use SCRIPT DEBUG YES/SYNC/NO"
    );
    assert!(c.cmd(&["SCRIPT", "DEBUG", "no"]).ok());
}
