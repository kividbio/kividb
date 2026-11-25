#![allow(clippy::disallowed_methods)]
// tests/config.rs
use std::thread;
// tests/config.rs

use kividb::config::{Config, IoModel};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

mod common;
use common::restart_verified;

// ── Helpers (same as string-json.rs) ─────────────────────────────────────────

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

fn start_server(port: u16) {
    thread::spawn(move || {
        let config = Config {
            port,
            bind: "127.0.0.1".to_string(),
            threads: 2,
            aof: false,
            databases: 16,
            maxmemory: 0,
            loglevel: "error".to_string(),
            io_model: IoModel::Tokio,
            replicaof: None,
            replica_announced_port: 0,
            requirepass: "".to_string(),
            aclfile: "".to_string(),
            slowlog_log_slower_than: u64::MAX,
            slowlog_max_len: 128,
            cluster_enabled: false,
            cluster_node_timeout: 15000,
            configfile: "./kividb.conf".to_string(),
            ..Default::default()
        };
        kividb::run_for_test(config);
    });
}

#[allow(dead_code)]
#[derive(Debug)]
enum RespValue {
    SimpleStr(String),
    Error(String),
    Integer(i64),
    BulkStr(String),
    Nil,
    Array(Vec<RespValue>),
}

impl RespValue {
    fn as_str(&self) -> &str {
        match self {
            RespValue::SimpleStr(s) | RespValue::BulkStr(s) => s.as_str(),
            _ => panic!("not a string: {:?}", self),
        }
    }
    // fn as_int(&self) -> i64 {
    //     match self {
    //         RespValue::Integer(n) => *n,
    //         _ => panic!("not an int: {:?}", self),
    //     }
    // }
    fn as_array(&self) -> &[RespValue] {
        match self {
            RespValue::Array(v) => v,
            _ => panic!("not an array: {:?}", self),
        }
    }
    fn is_ok(&self) -> bool {
        matches!(self, RespValue::SimpleStr(s) if s == "OK")
            || matches!(self, RespValue::BulkStr(s) if s == "OK")
    }
    // fn is_nil(&self) -> bool {
    //     matches!(self, RespValue::Nil)
    // }
    fn is_error(&self) -> bool {
        matches!(self, RespValue::Error(_))
    }
}

struct Conn(TcpStream);

impl Conn {
    fn connect(port: u16) -> Self {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let last_error = match TcpStream::connect(("127.0.0.1", port)) {
                Ok(mut stream) => {
                    stream
                        .set_read_timeout(Some(Duration::from_millis(250)))
                        .unwrap();
                    stream
                        .set_write_timeout(Some(Duration::from_millis(250)))
                        .unwrap();
                    let mut pong = [0u8; 7];
                    if stream.write_all(b"*1\r\n$4\r\nPING\r\n").is_ok()
                        && stream.read_exact(&mut pong).is_ok()
                        && &pong == b"+PONG\r\n"
                    {
                        stream
                            .set_read_timeout(Some(Duration::from_secs(5)))
                            .unwrap();
                        stream.set_write_timeout(None).unwrap();
                        return Self(stream);
                    }
                    "server did not answer PING".to_string()
                }
                Err(error) => error.to_string(),
            };
            if Instant::now() >= deadline {
                panic!("could not connect to server on port {port}: {last_error}");
            }
            thread::sleep(Duration::from_millis(100));
        }
    }

    fn cmds(&mut self, args: &[&str]) -> RespValue {
        // Build RESP array
        let mut buf = format!("*{}\r\n", args.len());
        for a in args {
            buf.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
        }
        self.0.write_all(buf.as_bytes()).unwrap();
        self.read_value()
    }

    /// `GET key` as raw bytes, for values that are not text.
    fn get_bytes(&mut self, key: &str) -> Vec<u8> {
        let command = format!("*2\r\n$3\r\nGET\r\n${}\r\n{key}\r\n", key.len());
        self.0.write_all(command.as_bytes()).unwrap();
        let mut line = Vec::new();
        loop {
            let mut byte = [0u8; 1];
            self.0.read_exact(&mut byte).unwrap();
            if byte[0] == b'\n' {
                break;
            }
            if byte[0] != b'\r' {
                line.push(byte[0]);
            }
        }
        let line = String::from_utf8(line).unwrap();
        let len: usize = line
            .strip_prefix('$')
            .unwrap_or_else(|| panic!("expected a bulk reply, got {line:?}"))
            .parse()
            .unwrap();
        let mut data = vec![0u8; len + 2];
        self.0.read_exact(&mut data).unwrap();
        data.truncate(len);
        data
    }

    fn read_value(&mut self) -> RespValue {
        let mut line = String::new();
        loop {
            let mut b = [0u8; 1];
            self.0.read_exact(&mut b).unwrap();
            if b[0] == b'\n' {
                break;
            }
            if b[0] != b'\r' {
                line.push(b[0] as char);
            }
        }
        match line.chars().next() {
            Some('+') => RespValue::SimpleStr(line[1..].to_string()),
            Some('-') => RespValue::Error(line[1..].to_string()),
            Some(':') => RespValue::Integer(line[1..].parse().unwrap()),
            Some('$') => {
                let n: i64 = line[1..].parse().unwrap();
                if n < 0 {
                    return RespValue::Nil;
                }
                let mut buf = vec![0u8; n as usize + 2];
                self.0.read_exact(&mut buf).unwrap();
                RespValue::BulkStr(String::from_utf8_lossy(&buf[..n as usize]).into_owned())
            }
            Some('*') => {
                let n: i64 = line[1..].parse().unwrap();
                if n < 0 {
                    return RespValue::Nil;
                }
                RespValue::Array((0..n).map(|_| self.read_value()).collect())
            }
            _ => panic!("unexpected RESP prefix in: {line:?}"),
        }
    }
}

// reuse the same Conn and RespValue helpers from your other test files
// (or put them in a shared tests/common/mod.rs)

#[test]
fn test_config_set_requirepass_updates_auth() {
    let _turn = maxmemory_turn();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // No password initially
    assert!(c.cmds(&["PING"]).is_ok() || matches!(c.cmds(&["PING"]), _));

    // Set a password
    assert!(c.cmds(&["CONFIG", "SET", "requirepass", "mypass"]).is_ok());

    // AUTH with correct password succeeds
    assert!(c.cmds(&["AUTH", "mypass"]).is_ok());

    // AUTH with wrong password fails
    assert!(c.cmds(&["AUTH", "wrongpass"]).is_error());

    // Remove the password
    assert!(c.cmds(&["AUTH", "mypass"]).is_ok());
    assert!(c.cmds(&["CONFIG", "SET", "requirepass", ""]).is_ok());

    // Now commands work without auth
    assert!(c.cmds(&["PING"]).is_ok() || !c.cmds(&["PING"]).is_error());
}

/// Protected mode is on by default, and a client on this machine (as here)
/// is let in all the same.
#[test]
fn protected_mode_can_be_read_and_set_and_lets_local_clients_in() {
    let _turn = maxmemory_turn();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["CONFIG", "GET", "protected-mode"]);
    assert_eq!(result.as_array()[1].as_str(), "yes");
    assert!(c.cmds(&["CONFIG", "SET", "protected-mode", "no"]).is_ok());
    let result = c.cmds(&["CONFIG", "GET", "protected-mode"]);
    assert_eq!(result.as_array()[1].as_str(), "no");
    assert!(
        c.cmds(&["CONFIG", "SET", "protected-mode", "maybe"])
            .is_error()
    );
    assert!(c.cmds(&["CONFIG", "SET", "protected-mode", "yes"]).is_ok());
    assert!(Conn::connect(port).cmds(&["PING"]).as_str() == "PONG");
}

#[test]
fn test_config_get_requirepass_redacted() {
    let _turn = maxmemory_turn();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Empty when no password
    let result = c.cmds(&["CONFIG", "GET", "requirepass"]);
    let arr = result.as_array();
    assert_eq!(arr[0].as_str(), "requirepass");
    assert_eq!(arr[1].as_str(), "");

    // Set a password
    c.cmds(&["CONFIG", "SET", "requirepass", "secret"]);

    // Should be redacted, not the actual value
    let result = c.cmds(&["CONFIG", "GET", "requirepass"]);
    let arr = result.as_array();
    assert_ne!(arr[1].as_str(), "secret");
    assert_eq!(arr[1].as_str(), "**redacted**");

    // Clean up
    c.cmds(&["AUTH", "secret"]);
    c.cmds(&["CONFIG", "SET", "requirepass", ""]);
}

/// `CONFIG GET threads` gives the `--threads` the server was started with
/// (#257). It used to give nothing, though `CONFIG SET threads` was refused
/// as a setting that cannot change while the server runs.
#[test]
fn config_get_threads_gives_the_configured_value() {
    let _turn = maxmemory_turn();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["CONFIG", "GET", "threads"]);
    let arr = result.as_array();
    assert_eq!(arr.len(), 2, "{result:?}");
    assert_eq!(arr[0].as_str(), "threads");
    assert_eq!(arr[1].as_str(), "2");
    assert!(c.cmds(&["CONFIG", "SET", "threads", "2"]).is_ok());
    let reply = c.cmds(&["CONFIG", "SET", "threads", "4"]);
    assert!(
        matches!(&reply, RespValue::Error(e) if e == "ERR CONFIG SET failed (possibly related to \
            argument 'threads') - can't set immutable config"),
        "{reply:?}"
    );
}

/// `CONFIG SET` with a name and no value to pair with it is a syntax error,
/// as in Redis 8.10, and not an arity error (#257): `CONFIG SET replicaof
/// <host> <port>` read as a wrong number of arguments, where Redis says
/// `ERR syntax error`. `replicaof` as one value is refused as Redis refuses
/// it: it cannot be set at runtime.
#[test]
fn config_set_with_an_unpaired_name_is_a_syntax_error() {
    let _turn = maxmemory_turn();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    for args in [
        &["CONFIG", "SET", "replicaof", "127.0.0.1", "6390"][..],
        &["CONFIG", "SET", "maxmemory", "1", "maxmemory-policy"][..],
    ] {
        let reply = c.cmds(args);
        assert!(
            matches!(&reply, RespValue::Error(e) if e == "ERR syntax error"),
            "{args:?}: {reply:?}"
        );
    }
    let reply = c.cmds(&["CONFIG", "SET", "replicaof", "127.0.0.1 6390"]);
    assert!(
        matches!(&reply, RespValue::Error(e) if e == "ERR CONFIG SET failed (possibly related to \
            argument 'replicaof') - can't set immutable config"),
        "{reply:?}"
    );
    let reply = c.cmds(&["CONFIG", "SET", "maxmemory"]);
    assert!(
        matches!(&reply, RespValue::Error(e)
            if e == "ERR wrong number of arguments for 'config|set' command"),
        "{reply:?}"
    );
}

#[test]
fn test_config_rewrite_with_configfile() {
    let port = free_port();
    let mut tmp = std::env::temp_dir();
    tmp.push(format!("kividb_rewrite_test_{port}.conf"));
    // Started from a file, which may be empty: there is nothing to rewrite
    // otherwise (#442).
    std::fs::write(&tmp, "").unwrap();

    let mut server = std::process::Command::new(
        std::env::var("CARGO_BIN_EXE_kividb").unwrap_or_else(|_| "target/debug/kividb".into()),
    )
    .current_dir(tempfile::tempdir().unwrap().keep())
    .args([
        "--port",
        &port.to_string(),
        "--configfile",
        tmp.to_str().unwrap(),
    ])
    .spawn()
    .expect("failed to start kividb");

    let mut c = Conn::connect(port);

    assert!(
        c.cmds(&["CONFIG", "SET", "hll-write-format", "redis"])
            .is_ok()
    );

    // REWRITE should succeed and write the file
    let result = c.cmds(&["CONFIG", "REWRITE"]);
    assert!(result.is_ok(), "CONFIG REWRITE failed: {result:?}");

    let contents = std::fs::read_to_string(&tmp).unwrap();
    // Verify the port we started with is in the file
    assert!(
        contents.contains(&format!("port {port}")),
        "rewritten file missing port: {contents}"
    );
    assert!(
        contents.contains("hll-write-format redis"),
        "rewritten file missing hll-write-format: {contents}"
    );
    // Verify it's a well-formed file with expected keys
    assert!(
        contents.contains("loglevel"),
        "missing loglevel: {contents}"
    );
    assert!(
        contents.contains("databases"),
        "missing databases: {contents}"
    );

    server.kill().unwrap();
    let _ = server.wait();
    let _ = std::fs::remove_file(&tmp);
}

/// Starts the binary from `conf`, in `cwd`, and connects to it.
fn start_from_file(
    conf: &std::path::Path,
    cwd: &std::path::Path,
    port: u16,
) -> std::process::Child {
    let server = std::process::Command::new(
        std::env::var("CARGO_BIN_EXE_kividb").unwrap_or_else(|_| "target/debug/kividb".into()),
    )
    .current_dir(cwd)
    .args(["--configfile", conf.to_str().unwrap()])
    .stdout(std::process::Stdio::null())
    .stderr(std::process::Stdio::null())
    .spawn()
    .expect("failed to start kividb");
    drop(Conn::connect(port));
    server
}

/// `CONFIG GET *`, by name.
fn config_get_all(c: &mut Conn) -> std::collections::BTreeMap<String, String> {
    let reply = c.cmds(&["CONFIG", "GET", "*"]);
    let text = |v: &RespValue| match v {
        RespValue::BulkStr(s) | RespValue::SimpleStr(s) => s.clone(),
        other => panic!("not a string: {other:?}"),
    };
    reply
        .as_array()
        .chunks(2)
        .map(|pair| (text(&pair[0]), text(&pair[1])))
        .collect()
}

/// `CONFIG REWRITE` writes every setting the server was started with: a
/// server restarted from the file it wrote, in another directory, gives the
/// same `CONFIG GET *`: `dir`, `io-model`, `replica-announced-port`,
/// `preload-file`, the TLS settings, `maxclients`, `hz`, `rdbcompression`,
/// the search and stream settings and the rest. Redis Sentinel sends
/// `CONFIG REWRITE` after each `REPLICAOF`, so a node restarted after a
/// failover starts from such a file.
///
/// Then every name `CONFIG GET *` lists is checked against the file: it is
/// written, or it is an older name of one that is, or it is empty, or it is
/// one KiviDB keeps without acting on and is at its default, or
/// `NOT_REWRITTEN` says why not. A setting added later fails here until the
/// rewrite writes it or the list gives the reason.
#[test]
fn config_rewrite_round_trip_keeps_every_setting() {
    let port = free_port();
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data dir");
    std::fs::create_dir(&data).unwrap();
    let preload =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/redis-7.4.rdb");
    let conf = tmp.path().join("kividb.conf");
    std::fs::write(
        &conf,
        format!(
            "port {port}\n\
             dir \"{}\"\n\
             io-model tokio\n\
             replica-announced-port 7100\n\
             preload-file rdb:{}\n\
             maxmemory-samples 7\n\
             maxclients 500\n\
             hz 20\n\
             rdbcompression yes\n\
             lua-time-limit 7000\n\
             search-default-dialect 2\n\
             stream-node-max-entries 50\n\
             hash-max-ziplist-entries 99\n\
             tracking-table-max-keys 5000\n\
             latency-tracking-info-percentiles 50 90\n",
            data.display(),
            preload.display(),
        ),
    )
    .unwrap();

    let first_cwd = tmp.path().join("first");
    std::fs::create_dir(&first_cwd).unwrap();
    let mut server = start_from_file(&conf, &first_cwd, port);
    let mut c = Conn::connect(port);
    let before = config_get_all(&mut c);
    assert_eq!(
        std::path::Path::new(&before["dir"]),
        data.canonicalize().unwrap(),
        "{before:?}"
    );
    // Set at run time, and read back whole: spaces, quotes and `#`.
    assert!(
        c.cmds(&["CONFIG", "SET", "masteruser", "a \"user\" # 'one'"])
            .is_ok()
    );
    let before = config_get_all(&mut c);
    assert_eq!(before["masteruser"], "a \"user\" # 'one'");
    let reply = c.cmds(&["CONFIG", "REWRITE"]);
    assert!(reply.is_ok(), "CONFIG REWRITE: {reply:?}");
    drop(c);
    server.kill().unwrap();
    let _ = server.wait();

    // Another working directory: a file without `dir` would start there.
    let second_cwd = tmp.path().join("second");
    std::fs::create_dir(&second_cwd).unwrap();
    let mut server = start_from_file(&conf, &second_cwd, port);
    let mut c = Conn::connect(port);
    let after = config_get_all(&mut c);
    drop(c);
    server.kill().unwrap();
    let _ = server.wait();
    let written = std::fs::read_to_string(&conf).unwrap();
    for (name, value) in &before {
        assert_eq!(
            after.get(name),
            Some(value),
            "{name} after a restart from the rewritten file:\n{written}"
        );
    }

    let in_file = |name: &str| {
        written
            .lines()
            .any(|line| line.split_whitespace().next() == Some(name))
    };
    let left_out: Vec<String> = after
        .iter()
        .filter(|(name, value)| {
            !(in_file(name)
                || kividb::config::setting_name(name) != name.as_str()
                || value.is_empty()
                || kividb::config::NOT_REWRITTEN.iter().any(|(n, _)| n == name)
                || kividb::redis_settings::remembered_default(name).as_ref() == Some(*value))
        })
        .map(|(name, value)| format!("{name} = {value}"))
        .collect();
    assert!(
        left_out.is_empty(),
        "CONFIG REWRITE leaves these out, and NOT_REWRITTEN does not say why: {left_out:?}\n{written}"
    );
}

/// A kividb started from `conf` in `cwd`, killed when dropped, once its port
/// takes connections. (`start_from_file` sends `PING`, which a server with a
/// password refuses before `AUTH`.)
fn spawn_from_file(conf: &std::path::Path, cwd: &std::path::Path, port: u16) -> Spawned {
    let server = Spawned(
        std::process::Command::new(env!("CARGO_BIN_EXE_kividb"))
            .current_dir(cwd)
            .args(["--configfile", conf.to_str().unwrap()])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("start kividb"),
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(Instant::now() < deadline, "nothing on port {port}");
        thread::sleep(Duration::from_millis(50));
    }
    server
}

/// The reply to `AUTH password` on a new connection to `port`, and the
/// connection.
fn auth(port: u16, password: &str) -> (Conn, RespValue) {
    let stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut c = Conn(stream);
    let reply = c.cmds(&["AUTH", password]);
    (c, reply)
}

/// A connection to `port` that `AUTH password` let in.
fn authed(port: u16, password: &str) -> Conn {
    let (c, reply) = auth(port, password);
    assert!(reply.is_ok(), "AUTH on {port}: {reply:?}");
    c
}

/// Waits until the replica on `c` has its link to the master up.
fn wait_for_link(c: &mut Conn) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let info = c.cmds(&["INFO", "replication"]).as_str().to_string();
        if info.contains("master_link_status:up") {
            return;
        }
        assert!(Instant::now() < deadline, "the link is not up: {info}");
        thread::sleep(Duration::from_millis(100));
    }
}

/// `CONFIG REWRITE` writes `requirepass`, `masterauth` and `masteruser` as
/// `CONFIG SET` set them, quoted: a master and its replica restarted from the
/// files they wrote take the passwords set at run time, clients with them,
/// and the replica authenticates to its master with them. `CONFIG GET`
/// shows the passwords as `**redacted**`.
#[test]
fn config_rewrite_writes_passwords_as_set_now() {
    let master_port = free_port();
    let replica_port = free_port();
    let tmp = tempfile::tempdir().unwrap();
    let at_start = "at start";
    let set_now = "set \"now\" # with spaces, 'quotes' and é";
    let conf = |name: &str, lines: String| {
        let dir = tmp.path().join(name);
        std::fs::create_dir(&dir).unwrap();
        let conf = dir.join("node.conf");
        std::fs::write(
            &conf,
            format!(
                "dir \"{}\"\nrequirepass \"{at_start}\"\n{lines}",
                dir.display()
            ),
        )
        .unwrap();
        (conf, dir)
    };
    let (master_conf, master_dir) = conf("master", format!("port {master_port}\n"));
    let (replica_conf, replica_dir) = conf(
        "replica",
        format!(
            "port {replica_port}\nmasterauth \"{at_start}\"\n\
             replicaof 127.0.0.1 {master_port}\n"
        ),
    );

    let master = spawn_from_file(&master_conf, &master_dir, master_port);
    let replica = spawn_from_file(&replica_conf, &replica_dir, replica_port);
    let mut m = authed(master_port, at_start);
    let mut r = authed(replica_port, at_start);
    wait_for_link(&mut r);
    for (name, value) in [
        ("masterauth", set_now),
        ("masteruser", "default"),
        ("requirepass", set_now),
    ] {
        let reply = r.cmds(&["CONFIG", "SET", name, value]);
        assert!(reply.is_ok(), "CONFIG SET {name}: {reply:?}");
    }
    assert!(m.cmds(&["CONFIG", "SET", "requirepass", set_now]).is_ok());
    for c in [&mut m, &mut r] {
        assert_eq!(config_get(c, "requirepass"), "**redacted**");
        let reply = c.cmds(&["CONFIG", "REWRITE"]);
        assert!(reply.is_ok(), "CONFIG REWRITE: {reply:?}");
    }
    assert_eq!(config_get(&mut r, "masterauth"), "**redacted**");
    drop((m, r, replica, master));

    let _master = spawn_from_file(&master_conf, &master_dir, master_port);
    let _replica = spawn_from_file(&replica_conf, &replica_dir, replica_port);
    for (port, conf) in [(master_port, &master_conf), (replica_port, &replica_conf)] {
        let written = std::fs::read_to_string(conf).unwrap();
        let (_, reply) = auth(port, at_start);
        assert!(
            reply.is_error(),
            "AUTH '{at_start}' on {port}: {reply:?}\n{written}"
        );
        let (_, reply) = auth(port, set_now);
        assert!(reply.is_ok(), "AUTH on {port}: {reply:?}\n{written}");
    }
    let mut m = authed(master_port, set_now);
    let mut r = authed(replica_port, set_now);
    wait_for_link(&mut r);
    assert!(m.cmds(&["SET", "after-restart", "1"]).is_ok());
    let deadline = Instant::now() + Duration::from_secs(10);
    while r.cmds(&["GET", "after-restart"]).as_str() != "1" {
        assert!(
            Instant::now() < deadline,
            "the write did not reach the replica"
        );
        thread::sleep(Duration::from_millis(50));
    }
}

/// A password set with `CONFIG SET` on a server started without one is
/// written, and one unset (`CONFIG SET requirepass ""`) is left out: the
/// server restarted from the file asks for a password, and then for none.
#[test]
fn config_rewrite_writes_requirepass_set_or_unset_at_run_time() {
    let port = free_port();
    let tmp = tempfile::tempdir().unwrap();
    let conf = tmp.path().join("kividb.conf");
    std::fs::write(
        &conf,
        format!("port {port}\ndir \"{}\"\n", tmp.path().display()),
    )
    .unwrap();
    let server = spawn_from_file(&conf, tmp.path(), port);
    let mut c = Conn::connect(port);
    assert!(
        c.cmds(&["CONFIG", "SET", "requirepass", "set at run time"])
            .is_ok()
    );
    assert!(c.cmds(&["AUTH", "set at run time"]).is_ok());
    assert!(c.cmds(&["CONFIG", "REWRITE"]).is_ok());
    drop((c, server));

    let server = spawn_from_file(&conf, tmp.path(), port);
    let written = std::fs::read_to_string(&conf).unwrap();
    let (mut c, reply) = auth(port, "set at run time");
    assert!(reply.is_ok(), "AUTH: {reply:?}\n{written}");
    let (mut other, _) = auth(port, "not it");
    assert!(other.cmds(&["PING"]).is_error(), "{written}");
    assert!(c.cmds(&["CONFIG", "SET", "requirepass", ""]).is_ok());
    assert!(c.cmds(&["CONFIG", "REWRITE"]).is_ok());
    drop((c, other, server));

    let _server = spawn_from_file(&conf, tmp.path(), port);
    let written = std::fs::read_to_string(&conf).unwrap();
    assert!(!written.contains("requirepass"), "{written}");
    let mut c = Conn::connect(port);
    assert_eq!(c.cmds(&["PING"]).as_str(), "PONG");
}

/// When the new file cannot be made beside the old one (here a directory is
/// in its place), `CONFIG REWRITE` replies with an error and the old file
/// is as it was.
#[test]
fn config_rewrite_that_cannot_make_its_file_leaves_the_old_one() {
    let port = free_port();
    let tmp = tempfile::tempdir().unwrap();
    let conf = tmp.path().join("kividb.conf");
    let text = format!("port {port}\ndir \"{}\"\n", tmp.path().display());
    std::fs::write(&conf, &text).unwrap();
    let in_the_way = conf.with_extension("tmp");
    std::fs::create_dir(&in_the_way).unwrap();
    std::fs::write(in_the_way.join("keep"), "").unwrap();
    let _server = spawn_from_file(&conf, tmp.path(), port);
    let mut c = Conn::connect(port);
    assert!(c.cmds(&["CONFIG", "SET", "maxmemory-samples", "9"]).is_ok());
    let reply = c.cmds(&["CONFIG", "REWRITE"]);
    assert!(reply.is_error(), "CONFIG REWRITE: {reply:?}");
    assert_eq!(std::fs::read_to_string(&conf).unwrap(), text);
}

/// `REPLICAOF NO ONE` makes the server a master, and `CONFIG REWRITE` after
/// it writes no `replicaof`: a replica Sentinel promoted (`REPLICAOF NO ONE`,
/// then `CONFIG REWRITE`) is a master when it restarts.
#[test]
fn config_rewrite_after_replicaof_no_one_drops_replicaof() {
    let port = free_port();
    let master = free_port();
    let tmp = tempfile::tempdir().unwrap();
    let conf = tmp.path().join("kividb.conf");
    std::fs::write(
        &conf,
        format!(
            "port {port}\ndir {}\nreplicaof 127.0.0.1 {master}\n",
            tmp.path().display()
        ),
    )
    .unwrap();
    let mut server = start_from_file(&conf, tmp.path(), port);
    let mut c = Conn::connect(port);
    let reply = c.cmds(&["CONFIG", "REWRITE"]);
    assert!(reply.is_ok(), "CONFIG REWRITE: {reply:?}");
    let written = std::fs::read_to_string(&conf).unwrap();
    assert!(
        written.contains(&format!("replicaof 127.0.0.1 {master}\n")),
        "{written}"
    );

    assert!(c.cmds(&["REPLICAOF", "NO", "ONE"]).is_ok());
    let reply = c.cmds(&["CONFIG", "REWRITE"]);
    assert!(reply.is_ok(), "CONFIG REWRITE: {reply:?}");
    let written = std::fs::read_to_string(&conf).unwrap();
    assert!(!written.contains("replicaof"), "{written}");
    drop(c);
    server.kill().unwrap();
    let _ = server.wait();
}

/// A server not started from a config file refuses `CONFIG REWRITE`, as
/// Redis does, and writes nothing (#442). It used to create the file at its
/// `--configfile` path (by default `/etc/kividb/kividb.conf`, which every
/// server on the host shares), and the next server started with that path
/// loaded it: after Sentinel's `REPLICAOF` and `CONFIG REWRITE`, unrelated
/// servers came up as replicas and a full sync replaced their data.
#[test]
fn config_rewrite_without_a_config_file_is_refused_and_writes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let conf = dir.path().join("absent.conf");
    let spawn = |port: u16| {
        std::process::Command::new(
            std::env::var("CARGO_BIN_EXE_kividb").unwrap_or_else(|_| "target/debug/kividb".into()),
        )
        .current_dir(dir.path())
        .args([
            "--port",
            &port.to_string(),
            "--bind",
            "127.0.0.1",
            "--dir",
            dir.path().to_str().unwrap(),
            "--configfile",
            conf.to_str().unwrap(),
        ])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .spawn()
        .expect("failed to start kividb")
    };
    let info_field = |c: &mut Conn, section: &str, field: &str| -> String {
        let info = c.cmds(&["INFO", section]);
        info.as_str()
            .lines()
            .find_map(|l| l.strip_prefix(&format!("{field}:")).map(str::to_string))
            .unwrap_or_else(|| panic!("no {field} in INFO {section}"))
    };

    let port = free_port();
    let master_port = free_port();
    let mut server = spawn(port);
    let mut c = Conn::connect(port);
    assert_eq!(info_field(&mut c, "server", "config_file"), "");
    assert!(
        c.cmds(&["REPLICAOF", "127.0.0.1", &master_port.to_string()])
            .is_ok()
    );
    let reply = c.cmds(&["CONFIG", "REWRITE"]);
    server.kill().unwrap();
    let _ = server.wait();
    assert!(
        matches!(&reply, RespValue::Error(e) if e == "ERR The server is running without a config file"),
        "{reply:?}"
    );
    assert!(!conf.exists(), "CONFIG REWRITE wrote {}", conf.display());

    // Another server given the same path is not made a replica by it.
    let other = free_port();
    let mut server = spawn(other);
    let mut c = Conn::connect(other);
    let role = info_field(&mut c, "replication", "role");
    server.kill().unwrap();
    let _ = server.wait();
    assert_eq!(role, "master");
}

/// `hll-write-format` from the config file is in effect before the AOF is
/// replayed, and stays in effect after a restart. `legacy` is the default.
#[test]
fn hll_write_format_from_config_file_survives_restart_and_aof_replay() {
    fn write_config(path: &std::path::Path, port: u16, dir: &str, format: Option<&str>) {
        let mut text =
            format!("bind 127.0.0.1\nport {port}\ndir {dir}\nappendonly yes\nloglevel error\n");
        if let Some(format) = format {
            text.push_str(&format!("hll-write-format {format}\n"));
        }
        std::fs::write(path, text).unwrap();
    }
    fn spawn(config: &std::path::Path) -> std::process::Child {
        std::process::Command::new(
            std::env::var("CARGO_BIN_EXE_kividb").unwrap_or_else(|_| "target/debug/kividb".into()),
        )
        .args(["--configfile", config.to_str().unwrap()])
        .spawn()
        .expect("failed to start kividb")
    }
    fn format_of(c: &mut Conn) -> String {
        let result = c.cmds(&["CONFIG", "GET", "hll-write-format"]);
        result.as_array()[1].as_str().to_string()
    }

    let root = std::env::temp_dir().join(format!("kividb_hll_config_{}", std::process::id()));
    let redis_dir = root.join("redis");
    let legacy_dir = root.join("legacy");
    std::fs::create_dir_all(&redis_dir).unwrap();
    std::fs::create_dir_all(&legacy_dir).unwrap();
    let slash = |path: &std::path::Path| path.to_str().unwrap().replace('\\', "/");

    let redis_conf = root.join("redis.conf");
    let redis_port = free_port();
    write_config(&redis_conf, redis_port, &slash(&redis_dir), Some("redis"));
    let mut server = spawn(&redis_conf);
    let mut c = Conn::connect(redis_port);
    assert_eq!(format_of(&mut c), "redis");
    c.cmds(&["PFADD", "hll", "a", "b", "c"]);
    c.cmds(&["PFADD", "replayed_create", "a", "b", "c"]);
    assert_eq!(c.get_bytes("hll")[4], 0, "redis dense encoding");

    // Give the once-a-second AOF sync its moment before killing the server.
    thread::sleep(Duration::from_millis(1_500));
    server.kill().unwrap();
    let _ = server.wait();

    // On the port of the config file if it is still free, and on another (the
    // file rewritten) if a test of another process has taken it in the gap.
    let (mut server, redis_port, _) = restart_verified(
        |port| {
            write_config(&redis_conf, port, &slash(&redis_dir), Some("redis"));
            spawn(&redis_conf)
        },
        redis_port,
    );
    let mut c = Conn::connect(redis_port);
    assert_eq!(format_of(&mut c), "redis", "setting lost across restart");
    assert_eq!(c.get_bytes("hll")[4], 0, "replayed value kept its format");
    assert!(matches!(
        c.cmds(&["EXISTS", "replayed_create"]),
        RespValue::Integer(1)
    ));
    assert_eq!(
        c.get_bytes("replayed_create")[4],
        0,
        "a key created while replaying the AOF must follow the configured format"
    );
    server.kill().unwrap();
    let _ = server.wait();

    let legacy_conf = root.join("legacy.conf");
    let legacy_port = free_port();
    write_config(&legacy_conf, legacy_port, &slash(&legacy_dir), None);
    let mut server = spawn(&legacy_conf);
    let mut c = Conn::connect(legacy_port);
    assert_eq!(format_of(&mut c), "legacy");
    c.cmds(&["PFADD", "hll", "a", "b", "c"]);
    assert_eq!(c.get_bytes("hll")[4], 1, "legacy encoding by default");
    server.kill().unwrap();
    let _ = server.wait();

    let _ = std::fs::remove_dir_all(&root);
}

/// CONFIG REWRITE keeps the config file's mode. It used to write a new file
/// with the default mode (0644), as Redis does, so a file kept private
/// because it holds `requirepass` became readable by every user (#51). With
/// no file to keep the mode of (it was removed after the server started from
/// it), the new one is 0600. Whatever is already at the temporary path, a
/// file left by an earlier rewrite or a symlink put there by another user,
/// is never written through.
#[cfg(unix)]
#[test]
fn test_config_rewrite_keeps_the_file_mode() {
    use std::os::unix::fs::PermissionsExt;

    enum Stale {
        Nothing,
        File,
        Symlink,
    }
    let rewrite = |original: Option<u32>, stale: Stale| -> u32 {
        let port = free_port();
        let mut path = std::env::temp_dir();
        path.push(format!("kividb_rewrite_mode_test_{port}.conf"));
        let _ = std::fs::remove_file(&path);
        // The server starts from the file; `None` removes it before the
        // rewrite.
        std::fs::write(&path, format!("port {port}\n")).unwrap();
        std::fs::set_permissions(
            &path,
            std::fs::Permissions::from_mode(original.unwrap_or(0o644)),
        )
        .unwrap();
        let tmp = path.with_extension("tmp");
        let victim = path.with_extension("victim");
        let _ = std::fs::remove_file(&tmp);
        match stale {
            Stale::Nothing => {}
            Stale::File => {
                std::fs::write(&tmp, "left over\n").unwrap();
                std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o666)).unwrap();
            }
            Stale::Symlink => {
                std::fs::write(&victim, "not the config\n").unwrap();
                std::os::unix::fs::symlink(&victim, &tmp).unwrap();
            }
        }
        let mut server = std::process::Command::new(
            std::env::var("CARGO_BIN_EXE_kividb").unwrap_or_else(|_| "target/debug/kividb".into()),
        )
        .current_dir(tempfile::tempdir().unwrap().keep())
        .args([
            "--port",
            &port.to_string(),
            "--configfile",
            path.to_str().unwrap(),
        ])
        .spawn()
        .expect("failed to start kividb");
        let mut c = Conn::connect(port);
        if original.is_none() {
            std::fs::remove_file(&path).unwrap();
        }
        let result = c.cmds(&["CONFIG", "REWRITE"]);
        server.kill().unwrap();
        let _ = server.wait();
        assert!(result.is_ok(), "CONFIG REWRITE failed: {result:?}");
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o7777;
        if let Ok(kept) = std::fs::read_to_string(&victim) {
            assert_eq!(kept, "not the config\n", "the symlink's target was written");
        }
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&tmp);
        let _ = std::fs::remove_file(&victim);
        mode
    };

    assert_eq!(
        rewrite(Some(0o600), Stale::Nothing),
        0o600,
        "a 0600 file must stay 0600"
    );
    assert_eq!(
        rewrite(Some(0o640), Stale::Nothing),
        0o640,
        "a 0640 file must stay 0640"
    );
    assert_eq!(
        rewrite(None, Stale::Nothing),
        0o600,
        "a new file must be 0600"
    );
    assert_eq!(
        rewrite(Some(0o600), Stale::File),
        0o600,
        "a stale 0666 .tmp must not pass its mode on"
    );
    assert_eq!(
        rewrite(Some(0o600), Stale::Symlink),
        0o600,
        "a symlink at .tmp must not be followed"
    );
}

/// `maxmemory` and the remembered settings are the process's, not a
/// server's, and a server starting sets them: the tests that start one in
/// this process take turns. One starting while another test had just set
/// `maxmemory` put it back to 0 under it.
fn maxmemory_turn() -> std::sync::MutexGuard<'static, ()> {
    static TURN: std::sync::Mutex<()> = std::sync::Mutex::new(());
    TURN.lock().unwrap_or_else(|e| e.into_inner())
}

/// `CONFIG SET` with several settings sets all or none: one bad value, and
/// the ones before it are put back (#24, the Redis suite's "CONFIG SET
/// rollback on set error"). `maxmemory-clients` and
/// `client-query-buffer-limit` took any value.
#[test]
fn config_set_with_a_bad_value_changes_nothing() {
    let _turn = maxmemory_turn();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let get = |c: &mut Conn, name: &str| {
        c.cmds(&["CONFIG", "GET", name]).as_array()[1]
            .as_str()
            .to_string()
    };

    assert!(c.cmds(&["CONFIG", "SET", "maxmemory", "10000002"]).is_ok());
    let clients = get(&mut c, "maxmemory-clients");
    let buffer = get(&mut c, "client-query-buffer-limit");
    let reply = c.cmds(&[
        "CONFIG",
        "SET",
        "maxmemory",
        "10000001",
        "maxmemory-clients",
        "200%",
        "client-query-buffer-limit",
        "invalid",
    ]);
    assert!(
        matches!(&reply, RespValue::Error(e) if e == "ERR CONFIG SET failed (possibly related to \
            argument 'maxmemory-clients') - percentage argument must be less or equal to 100"),
        "{reply:?}"
    );
    assert_eq!(get(&mut c, "maxmemory"), "10000002");
    assert_eq!(get(&mut c, "maxmemory-clients"), clients);
    assert_eq!(get(&mut c, "client-query-buffer-limit"), buffer);

    for (name, bad) in [
        ("maxmemory-clients", "lots"),
        ("maxmemory-clients", "x%"),
        ("client-query-buffer-limit", "invalid"),
        ("client-query-buffer-limit", "1000"),
    ] {
        let reply = c.cmds(&["CONFIG", "SET", name, bad]);
        assert!(reply.is_error(), "{name} {bad}: {reply:?}");
    }

    // Good values, as Redis reports them.
    assert!(
        c.cmds(&["CONFIG", "SET", "maxmemory-clients", "10%"])
            .is_ok()
    );
    assert_eq!(get(&mut c, "maxmemory-clients"), "10%");
    assert!(
        c.cmds(&["CONFIG", "SET", "maxmemory-clients", "1mb"])
            .is_ok()
    );
    assert_eq!(get(&mut c, "maxmemory-clients"), "1048576");
    assert!(
        c.cmds(&["CONFIG", "SET", "client-query-buffer-limit", "10mb"])
            .is_ok()
    );
    assert_eq!(get(&mut c, "client-query-buffer-limit"), "10485760");
    assert!(c.cmds(&["CONFIG", "SET", "maxmemory", "0"]).is_ok());
}

/// `CONFIG SET port` moves the listener (#24): new connections are taken on
/// the new port, and not on the old one; open connections stay. A port that
/// is taken is refused with Redis's error, and with it every setting of the
/// same `CONFIG SET` (the Redis suite's "CONFIG SET rollback on apply error").
#[test]
fn config_set_port_moves_the_listener_or_changes_nothing() {
    let _turn = maxmemory_turn();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let get = |c: &mut Conn, name: &str| {
        c.cmds(&["CONFIG", "GET", name]).as_array()[1]
            .as_str()
            .to_string()
    };

    // Taken: refused, and nothing changes.
    let taken = std::net::TcpListener::bind(("127.0.0.1", free_port())).unwrap();
    let taken_port = taken.local_addr().unwrap().port().to_string();
    let maxmemory = get(&mut c, "maxmemory");
    let buffer = get(&mut c, "client-query-buffer-limit");
    let reply = c.cmds(&[
        "CONFIG",
        "SET",
        "maxmemory",
        "10000001",
        "port",
        &taken_port,
        "client-query-buffer-limit",
        "10mb",
    ]);
    assert!(
        matches!(&reply, RespValue::Error(e) if e.starts_with("ERR CONFIG SET failed (possibly \
            related to argument 'port') - Unable to listen on this port")),
        "{reply:?}"
    );
    assert_eq!(get(&mut c, "maxmemory"), maxmemory);
    assert_eq!(get(&mut c, "client-query-buffer-limit"), buffer);
    assert_eq!(get(&mut c, "port"), port.to_string());
    assert!(Conn::connect(port).cmds(&["PING"]).as_str() == "PONG");
    drop(taken);

    // Free: moved.
    let moved = free_port();
    assert!(
        c.cmds(&["CONFIG", "SET", "port", &moved.to_string()])
            .is_ok()
    );
    assert_eq!(get(&mut c, "port"), moved.to_string());
    let mut there = Conn::connect(moved);
    assert_eq!(there.cmds(&["PING"]).as_str(), "PONG");
    assert_eq!(
        c.cmds(&["PING"]).as_str(),
        "PONG",
        "the open connection stays"
    );
    let info = there.cmds(&["INFO", "server"]);
    assert!(info.as_str().contains(&format!("tcp_port:{moved}\r\n")));
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while TcpStream::connect(("127.0.0.1", port)).is_ok() {
        assert!(
            std::time::Instant::now() < deadline,
            "the old port still takes connections"
        );
        thread::sleep(Duration::from_millis(50));
    }

    // The same port again, and back to the first.
    assert!(
        there
            .cmds(&["CONFIG", "SET", "port", &moved.to_string()])
            .is_ok()
    );
    assert!(
        there
            .cmds(&["CONFIG", "SET", "port", &port.to_string()])
            .is_ok()
    );
    assert_eq!(Conn::connect(port).cmds(&["PING"]).as_str(), "PONG");
    assert!(there.cmds(&["CONFIG", "SET", "port", "0"]).is_error());
    assert!(there.cmds(&["CONFIG", "SET", "port", "x"]).is_error());
}

/// The slot migration settings take effect at runtime, and `CONFIG GET`
/// says what they are; a cluster setting this server does not have is
/// refused, not taken and ignored (#111). Settings of Redis's that mean
/// nothing here, outside the cluster's, are still taken.
/// `CONFIG SET` refuses a name it does not know, and one it cannot change
/// while the server runs, as Redis does; Redis's settings that mean nothing
/// here are taken and remembered (#143).
#[test]
fn config_set_refuses_unknown_and_startup_only_settings() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let get = |c: &mut Conn, name: &str| match c.cmds(&["CONFIG", "GET", name]).as_array() {
        [_, value] => Some(value.as_str().to_string()),
        _ => None,
    };

    let reply = c.cmds(&["CONFIG", "SET", "bogus-name", "1"]);
    assert!(
        matches!(&reply, RespValue::Error(e)
            if e == "ERR Unknown option or number of arguments for CONFIG SET - 'bogus-name'"),
        "{reply:?}"
    );
    assert_eq!(get(&mut c, "bogus-name"), None);

    for name in ["threads", "io-model", "aof", "backupdirname"] {
        let reply = c.cmds(&["CONFIG", "SET", name, "1000"]);
        assert!(
            matches!(&reply, RespValue::Error(e) if e == &format!(
                "ERR CONFIG SET failed (possibly related to argument '{name}') - can't set \
                 immutable config"
            )),
            "{name}: {reply:?}"
        );
    }
    assert_eq!(get(&mut c, "backupdirname").as_deref(), Some("backupdir"));
    // To what it already is, is no change.
    assert!(
        c.cmds(&["CONFIG", "SET", "backupdirname", "backupdir"])
            .is_ok()
    );
    // The replicas' queue limits can be changed (#305).
    for name in [
        "repl-replica-max-backlog-bytes",
        "repl-replica-sync-max-backlog-bytes",
    ] {
        assert!(c.cmds(&["CONFIG", "SET", name, "512mb"]).is_ok(), "{name}");
        assert_eq!(get(&mut c, name).as_deref(), Some("536870912"), "{name}");
        // 0 is no limit, as in Redis (#774).
        assert!(c.cmds(&["CONFIG", "SET", name, "0"]).is_ok(), "{name}");
        assert_eq!(get(&mut c, name).as_deref(), Some("0"), "{name}");
        let reply = c.cmds(&["CONFIG", "SET", name, "-1"]);
        assert!(reply.is_error(), "{name}: {reply:?}");
    }
    // One refused takes the others back with it.
    let reply = c.cmds(&["CONFIG", "SET", "hz", "20", "bogus-name", "1"]);
    assert!(reply.is_error(), "{reply:?}");
    assert_eq!(get(&mut c, "hz").as_deref(), Some("10"));

    assert_eq!(
        get(&mut c, "latency-monitor-threshold").as_deref(),
        Some("0")
    );
    for (name, value) in [
        ("hz", "20"),
        ("tcp-keepalive", "60"),
        ("maxclients", "500"),
        ("acllog-max-len", "64"),
        ("watchdog-period", "0"),
    ] {
        assert!(c.cmds(&["CONFIG", "SET", name, value]).is_ok(), "{name}");
        assert_eq!(get(&mut c, name).as_deref(), Some(value), "{name}");
    }
    // A debugging setting of Redis's is answered by name, not listed.
    let all = c.cmds(&["CONFIG", "GET", "*"]);
    assert!(
        !all.as_array()
            .iter()
            .any(|v| v.as_str() == "watchdog-period"),
        "{all:?}"
    );
}

/// `CONFIG SET latency-tracking-info-percentiles` takes what Redis takes and
/// refuses what it refuses, with its words (#255). Each reply below is what
/// Redis 8.10.3 answered; a refused value leaves what was there.
#[test]
fn config_set_latency_tracking_info_percentiles_refuses_what_redis_refuses() {
    let _turn = maxmemory_turn();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let key = "latency-tracking-info-percentiles";
    let get = |c: &mut Conn| {
        c.cmds(&["CONFIG", "GET", key]).as_array()[1]
            .as_str()
            .to_string()
    };
    let refused = format!("ERR CONFIG SET failed (possibly related to argument '{key}') - ");
    let not_numbers = "Invalid latency-tracking-info-percentiles parameters";
    let out_of_range =
        "latency-tracking-info-percentiles parameters should sit between [0.0,100.0]";

    assert_eq!(get(&mut c), "50 99 99.9");
    for (value, why) in [
        ("abc", not_numbers),
        ("50 abc", not_numbers),
        ("101", out_of_range),
        ("-1", out_of_range),
        (" 50", not_numbers),
        ("50 ", not_numbers),
        ("50  99", not_numbers),
        ("nan", not_numbers),
        ("1e-400", not_numbers),
        ("inf", out_of_range),
    ] {
        match c.cmds(&["CONFIG", "SET", key, value]) {
            RespValue::Error(message) => {
                assert_eq!(message, format!("{refused}{why}"), "{value:?}")
            }
            other => panic!("{value:?} was not refused: {other:?}"),
        }
        assert_eq!(
            get(&mut c),
            "50 99 99.9",
            "{value:?} changed what was there"
        );
    }
    for (value, reads_back) in [
        ("", ""),
        ("50 99 99.9", "50 99 99.9"),
        ("0", "0"),
        ("100", "100"),
        ("0x10", "16"),
        ("1e1", "10"),
        ("50.0 99.90", "50 99.9"),
        ("99.9999999", "100"),
        ("-0", "-0"),
        ("1e-310", "0"),
    ] {
        let reply = c.cmds(&["CONFIG", "SET", key, value]);
        assert!(reply.is_ok(), "{value:?}: {reply:?}");
        assert_eq!(get(&mut c), reads_back, "{value:?}");
    }
    // Put back what other tests of this process go by.
    assert!(c.cmds(&["CONFIG", "SET", key, "50 99 99.9"]).is_ok());
}

#[test]
fn config_set_cluster_settings_take_effect_or_are_refused() {
    let _turn = maxmemory_turn();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let get = |c: &mut Conn, name: &str| {
        c.cmds(&["CONFIG", "GET", name]).as_array()[1]
            .as_str()
            .to_string()
    };

    assert_eq!(
        get(&mut c, "cluster-slot-migration-handoff-max-lag-bytes"),
        "1048576"
    );
    assert_eq!(
        get(&mut c, "cluster-slot-migration-write-pause-timeout"),
        "10000"
    );
    assert!(
        c.cmds(&[
            "CONFIG",
            "SET",
            "cluster-slot-migration-handoff-max-lag-bytes",
            "2mb",
            "cluster-slot-migration-write-pause-timeout",
            "500",
        ])
        .is_ok()
    );
    assert_eq!(
        get(&mut c, "cluster-slot-migration-handoff-max-lag-bytes"),
        "2097152"
    );
    assert_eq!(
        get(&mut c, "cluster-slot-migration-write-pause-timeout"),
        "500"
    );
    for (name, bad) in [
        ("cluster-slot-migration-handoff-max-lag-bytes", "lots"),
        ("cluster-slot-migration-write-pause-timeout", "-1"),
    ] {
        let reply = c.cmds(&["CONFIG", "SET", name, bad]);
        assert!(reply.is_error(), "{name} {bad}: {reply:?}");
    }
    assert_eq!(
        get(&mut c, "cluster-slot-migration-write-pause-timeout"),
        "500"
    );

    for name in [
        "cluster-allow-reads-when-down",
        "cluster-migration-barrier",
        "cluster-no-such-setting",
    ] {
        let reply = c.cmds(&["CONFIG", "SET", name, "yes"]);
        assert!(
            matches!(&reply, RespValue::Error(e) if e == &format!(
                "ERR Unknown option or number of arguments for CONFIG SET - '{name}'"
            )),
            "{name}: {reply:?}"
        );
    }
    assert!(
        c.cmds(&["CONFIG", "SET", "hash-max-listpack-entries", "256"])
            .is_ok()
    );

    // Back to how the server started, for any test after this one.
    assert!(
        c.cmds(&[
            "CONFIG",
            "SET",
            "cluster-slot-migration-handoff-max-lag-bytes",
            "1048576",
            "cluster-slot-migration-write-pause-timeout",
            "10000",
        ])
        .is_ok()
    );
}

/// A kividb of its own, killed when dropped. Its log is `kividb.log` in `dir`.
#[cfg(unix)]
struct Limited {
    child: std::process::Child,
    port: u16,
    dir: tempfile::TempDir,
}

#[cfg(unix)]
impl Drop for Limited {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[cfg(unix)]
impl Limited {
    fn log(&self) -> String {
        std::fs::read_to_string(self.dir.path().join("kividb.log")).unwrap()
    }
}

/// A kividb with `soft` and `hard` open-file limits, set in the child only,
/// and `conf` as its config file.
#[cfg(unix)]
fn start_with_open_files(soft: u64, hard: u64, conf: &str) -> Limited {
    use std::os::unix::process::CommandExt;
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("kividb.conf");
    std::fs::write(
        &file,
        format!(
            "{conf}\nlogfile {}\n",
            dir.path().join("kividb.log").display()
        ),
    )
    .unwrap();
    let port = free_port();
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_kividb"));
    command.args([
        "--port",
        &port.to_string(),
        "--dir",
        dir.path().to_str().unwrap(),
        "--configfile",
        file.to_str().unwrap(),
    ]);
    // SAFETY: setrlimit is async-signal-safe.
    unsafe {
        command.pre_exec(move || {
            let limit = libc::rlimit {
                rlim_cur: soft as libc::rlim_t,
                rlim_max: hard as libc::rlim_t,
            };
            if libc::setrlimit(libc::RLIMIT_NOFILE, &limit) == 0 {
                Ok(())
            } else {
                Err(std::io::Error::last_os_error())
            }
        });
    }
    Limited {
        child: command.spawn().expect("start kividb"),
        port,
        dir,
    }
}

/// Whether a connection to `port` is turned away for `maxclients`.
#[cfg(unix)]
fn turned_away(port: u16) -> bool {
    let mut extra = TcpStream::connect(("127.0.0.1", port)).unwrap();
    extra
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut reply = String::new();
    extra.read_to_string(&mut reply).unwrap();
    reply == "-ERR max number of clients reached\r\n"
}

// A connection turned away at maxclients is counted in INFO stats rejected_connections.
#[cfg(unix)]
#[test]
fn stats_are_updated_when_connection_is_turned_away() {
    let server = start_with_open_files(40, 100, "io-model tokio\nthreads 1\nmaxclients 1");

    let mut conn = Conn::connect(server.port);
    assert_eq!(maxclients(&mut conn), "1");

    let rejected = |conn: &mut Conn| -> u64 {
        conn.cmds(&["INFO", "stats"])
            .as_str()
            .lines()
            .find_map(|line| line.strip_prefix("rejected_connections:"))
            .expect("rejected_connections in INFO stats")
            .parse()
            .unwrap()
    };

    let before = rejected(&mut conn);
    assert!(turned_away(server.port));
    let after = rejected(&mut conn);

    assert_eq!(
        after,
        before + 1,
        "rejected_connections should increase by one"
    );
}

#[cfg(unix)]
fn maxclients(c: &mut Conn) -> String {
    c.cmds(&["CONFIG", "GET", "maxclients"]).as_array()[1]
        .as_str()
        .to_string()
}

#[cfg(unix)]
#[test]
fn maxclients_is_kept_when_the_soft_limit_allows_it() {
    let server = start_with_open_files(100, 100, "io-model tokio\nthreads 1\nmaxclients 50");
    let mut c = Conn::connect(server.port);
    assert_eq!(maxclients(&mut c), "50");
    let log = server.log();
    assert!(!log.contains("open files"), "{log}");
}

/// The soft limit is raised for `maxclients`, and that many clients connect;
/// one more is turned away.
#[cfg(unix)]
#[test]
fn the_soft_limit_is_raised_up_to_the_hard_limit_for_maxclients() {
    let server = start_with_open_files(40, 100, "io-model tokio\nthreads 1\nmaxclients 50");
    let port = server.port;
    let mut clients: Vec<Conn> = (0..50).map(|_| Conn::connect(port)).collect();
    for c in &mut clients {
        assert_eq!(c.cmds(&["PING"]).as_str(), "PONG");
    }
    assert_eq!(maxclients(&mut clients[0]), "50");
    assert!(turned_away(port));
    // Lower than the clients connected: they stay, no new one comes in.
    assert!(
        clients[0]
            .cmds(&["CONFIG", "SET", "maxclients", "10"])
            .is_ok()
    );
    assert_eq!(clients[49].cmds(&["PING"]).as_str(), "PONG");
    assert!(turned_away(port));
    clients.truncate(5);
    // Room again, once the server has seen them go.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !matches!(Conn::connect(port).cmds(&["PING"]), RespValue::SimpleStr(s) if s == "PONG") {
        assert!(
            std::time::Instant::now() < deadline,
            "no room after clients left"
        );
        thread::sleep(Duration::from_millis(50));
    }
}

#[cfg(unix)]
#[test]
fn maxclients_is_lowered_to_the_hard_limit_and_not_set_past_it() {
    let server = start_with_open_files(40, 60, "io-model tokio\nthreads 1\nmaxclients 100");
    let mut c = Conn::connect(server.port);
    assert_eq!(maxclients(&mut c), "28");
    let info = c.cmds(&["INFO", "clients"]);
    assert!(info.as_str().contains("maxclients:28\r\n"), "{info:?}");
    let log = server.log();
    assert!(
        log.contains("maxclients 100 needs 132 open files, but the limit (ulimit -n) allows 60: maxclients is 28"),
        "{log}"
    );

    for refused in [
        &["maxclients", "29"][..],
        &["hz", "20", "maxclients", "1000"],
    ] {
        let reply = c.cmds(&[&["CONFIG", "SET"][..], refused].concat());
        assert!(
            matches!(&reply, RespValue::Error(e) if e.ends_with(
                "The operating system is not able to handle the specified number of clients, try with 28"
            )),
            "{reply:?}"
        );
        assert_eq!(maxclients(&mut c), "28");
    }
    assert_eq!(
        c.cmds(&["CONFIG", "GET", "hz"]).as_array()[1].as_str(),
        "10"
    );
    // Lowered, and taken back with a setting refused after it.
    assert!(
        c.cmds(&["CONFIG", "SET", "maxclients", "5", "bogus-name", "1"])
            .is_error()
    );
    assert_eq!(maxclients(&mut c), "28");
    assert!(c.cmds(&["CONFIG", "SET", "maxclients", "10"]).is_ok());
    assert!(c.cmds(&["CONFIG", "SET", "maxclients", "28"]).is_ok());
    assert_eq!(maxclients(&mut c), "28");
}

#[cfg(unix)]
#[test]
fn a_limit_with_no_room_for_a_client_stops_the_server() {
    let mut server = start_with_open_files(32, 32, "io-model tokio\nthreads 1");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = server.child.try_wait().unwrap() {
            break status;
        }
        assert!(std::time::Instant::now() < deadline, "still running");
        thread::sleep(Duration::from_millis(50));
    };
    assert!(!status.success());
    let log = server.log();
    assert!(
        log.contains("The open-file limit (ulimit -n) is too low"),
        "{log}"
    );
}

/// With 150 open files, `maxclients` clients connect and are answered, and
/// one more is turned away: on io_uring, whose 4 workers keep 72 files back,
/// and on tokio, which keeps 32 whatever its threads.
#[cfg(target_os = "linux")]
#[test]
fn maxclients_leaves_room_for_what_the_io_model_holds() {
    for (io_model, fits) in [("uring", 78), ("tokio", 118)] {
        let server = start_with_open_files(
            150,
            150,
            &format!("io-model {io_model}\nthreads 4\nmaxclients 1000"),
        );
        let port = server.port;
        // As many as it lets in are answered, before their number is checked.
        let mut clients = vec![Conn::connect(port)];
        let max: usize = maxclients(&mut clients[0]).parse().unwrap();
        clients.extend((1..max).map(|_| Conn::connect(port)));
        for c in &mut clients {
            assert_eq!(c.cmds(&["PING"]).as_str(), "PONG", "{io_model}");
        }
        assert_eq!(max, fits, "{io_model}");
        assert!(turned_away(port), "{io_model}");
        let more = (fits + 1).to_string();
        let reply = clients[0].cmds(&["CONFIG", "SET", "maxclients", &more]);
        assert!(
            matches!(&reply, RespValue::Error(e) if e.ends_with(&format!("try with {fits}"))),
            "{io_model}: {reply:?}"
        );
    }
}

/// A kividb process, killed when dropped.
struct Spawned(std::process::Child);

impl Drop for Spawned {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_kividb(args: &[&str]) -> Spawned {
    Spawned(
        std::process::Command::new(env!("CARGO_BIN_EXE_kividb"))
            .args(args)
            .spawn()
            .expect("start kividb"),
    )
}

/// `INFO replication`'s value of `field`, once the server is a replica.
fn replica_info(c: &mut Conn, field: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let info = c.cmds(&["INFO", "replication"]).as_str().to_string();
        if info.contains("role:slave") {
            let prefix = format!("{field}:");
            return info
                .lines()
                .find_map(|line| line.strip_prefix(&prefix))
                .unwrap_or_else(|| panic!("no {field} in {info}"))
                .to_string();
        }
        assert!(Instant::now() < deadline, "not a replica: {info}");
        thread::sleep(Duration::from_millis(50));
    }
}

fn config_get(c: &mut Conn, name: &str) -> String {
    let reply = c.cmds(&["CONFIG", "GET", name]);
    reply.as_array()[1].as_str().to_string()
}

/// `replica-priority` is what `INFO replication` reports as
/// `slave_priority`, which Sentinel reads to choose the replica to promote
/// and never promotes at 0. It was taken by `CONFIG SET` and the config file
/// and only remembered: `INFO` said 100 whatever was set, so Sentinel
/// promoted a replica set to 0 (#443). It is set from the config file, the
/// command line and `CONFIG SET`, under both its names, and `CONFIG
/// REWRITE` keeps it. `replica-announced` likewise reaches
/// `replica_announced`.
#[test]
fn replica_priority_and_announced_reach_info_replication() {
    let dir = tempfile::tempdir().unwrap();
    let master_port = free_port();
    let _master = spawn_kividb(&[
        "--port",
        &master_port.to_string(),
        "--bind",
        "127.0.0.1",
        "--loglevel",
        "warning",
        "--dir",
        dir.path().to_str().unwrap(),
    ]);
    Conn::connect(master_port);

    // From the config file, under Redis's old name.
    let conf = dir.path().join("replica.conf");
    let replica_port = free_port();
    std::fs::write(
        &conf,
        format!(
            "port {replica_port}\nbind 127.0.0.1\nloglevel warning\n\
             replicaof 127.0.0.1 {master_port}\nslave-priority 7\nreplica-announced no\n"
        ),
    )
    .unwrap();
    let replica_dir = dir.path().join("replica");
    std::fs::create_dir_all(&replica_dir).unwrap();
    let replica = spawn_kividb(&[
        "--configfile",
        conf.to_str().unwrap(),
        "--dir",
        replica_dir.to_str().unwrap(),
    ]);
    let mut c = Conn::connect(replica_port);
    assert_eq!(replica_info(&mut c, "slave_priority"), "7");
    assert_eq!(replica_info(&mut c, "replica_announced"), "0");
    assert_eq!(config_get(&mut c, "replica-priority"), "7");
    assert_eq!(config_get(&mut c, "slave-priority"), "7");
    assert_eq!(config_get(&mut c, "replica-announced"), "no");

    // From CONFIG SET, under either name.
    assert!(c.cmds(&["CONFIG", "SET", "replica-priority", "0"]).is_ok());
    assert_eq!(replica_info(&mut c, "slave_priority"), "0");
    assert_eq!(config_get(&mut c, "slave-priority"), "0");
    assert!(c.cmds(&["CONFIG", "SET", "slave-priority", "5"]).is_ok());
    assert_eq!(replica_info(&mut c, "slave_priority"), "5");
    assert_eq!(config_get(&mut c, "replica-priority"), "5");
    assert!(
        c.cmds(&["CONFIG", "SET", "replica-announced", "yes"])
            .is_ok()
    );
    assert_eq!(replica_info(&mut c, "replica_announced"), "1");
    for (name, value, why) in [
        (
            "replica-priority",
            "-1",
            "argument must be between 0 and 2147483647 inclusive",
        ),
        (
            "replica-priority",
            "2147483648",
            "argument must be between 0 and 2147483647 inclusive",
        ),
        (
            "replica-priority",
            "high",
            "argument couldn't be parsed into an integer",
        ),
        (
            "replica-announced",
            "maybe",
            "argument must be 'yes' or 'no'",
        ),
    ] {
        let reply = c.cmds(&["CONFIG", "SET", name, value]);
        let expected =
            format!("ERR CONFIG SET failed (possibly related to argument '{name}') - {why}");
        assert!(
            matches!(&reply, RespValue::Error(e) if *e == expected),
            "{name} {value}: {reply:?}"
        );
    }
    assert_eq!(replica_info(&mut c, "slave_priority"), "5");
    assert_eq!(replica_info(&mut c, "replica_announced"), "1");

    // CONFIG REWRITE keeps what was set, for the next start.
    assert!(c.cmds(&["CONFIG", "SET", "replica-priority", "0"]).is_ok());
    assert!(
        c.cmds(&["CONFIG", "SET", "replica-announced", "no"])
            .is_ok()
    );
    assert!(c.cmds(&["CONFIG", "REWRITE"]).is_ok());
    drop(c);
    drop(replica);
    let _replica = spawn_kividb(&[
        "--configfile",
        conf.to_str().unwrap(),
        "--dir",
        replica_dir.to_str().unwrap(),
    ]);
    let mut c = Conn::connect(replica_port);
    assert_eq!(replica_info(&mut c, "slave_priority"), "0");
    assert_eq!(replica_info(&mut c, "replica_announced"), "0");

    // From the command line.
    let other_port = free_port();
    let other_dir = dir.path().join("other");
    std::fs::create_dir_all(&other_dir).unwrap();
    let _other = spawn_kividb(&[
        "--port",
        &other_port.to_string(),
        "--bind",
        "127.0.0.1",
        "--loglevel",
        "warning",
        "--dir",
        other_dir.to_str().unwrap(),
        "--replicaof",
        "127.0.0.1",
        &master_port.to_string(),
        "--replica-priority",
        "3",
    ]);
    let mut c = Conn::connect(other_port);
    assert_eq!(replica_info(&mut c, "slave_priority"), "3");
    assert_eq!(replica_info(&mut c, "replica_announced"), "1");
}

/// `CONFIG GET dbfilename` names the snapshot file the server writes and
/// loads, and `CONFIG GET dir` the directory it is in, so that a tool that
/// takes a snapshot over the protocol finds it (#846). It answered nothing.
/// The name is what the server uses whatever `--dbfilename` said, and a
/// pattern that matches it lists it.
#[test]
fn config_get_dbfilename_and_dir_find_the_snapshot_file() {
    /// The server, ended with the test, whether it passes or not.
    struct Killed(std::process::Child);
    impl Drop for Killed {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let _server = Killed(
        std::process::Command::new(
            std::env::var("CARGO_BIN_EXE_kividb").unwrap_or_else(|_| "target/debug/kividb".into()),
        )
        .current_dir(dir.path())
        .args([
            "--port",
            &port.to_string(),
            "--bind",
            "127.0.0.1",
            "--dir",
            dir.path().to_str().unwrap(),
            "--dbfilename",
            "dump.rdb",
            "--loglevel",
            "error",
        ])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .spawn()
        .expect("failed to start kividb"),
    );
    let mut c = Conn::connect(port);
    let pair = |c: &mut Conn, args: &[&str]| -> Vec<String> {
        c.cmds(args)
            .as_array()
            .iter()
            .map(|v| v.as_str().to_string())
            .collect()
    };

    let name = pair(&mut c, &["CONFIG", "GET", "dbfilename"]);
    assert_eq!(name, ["dbfilename", "dump.kdb"]);
    let folder = pair(&mut c, &["CONFIG", "GET", "dir"]);
    assert_eq!(folder[0], "dir");
    assert_eq!(
        std::fs::canonicalize(&folder[1]).unwrap(),
        std::fs::canonicalize(dir.path()).unwrap()
    );
    // A pattern lists both.
    let listed = pair(&mut c, &["CONFIG", "GET", "d*"]);
    for want in ["dbfilename", "dump.kdb", "dir"] {
        assert!(listed.iter().any(|v| v == want), "{want} in {listed:?}");
    }
    // The file a save writes is the one named, where `dir` says.
    assert!(c.cmds(&["SET", "k", "v"]).is_ok());
    assert!(c.cmds(&["SAVE"]).is_ok());
    assert!(
        std::path::Path::new(&folder[1]).join(&name[1]).is_file(),
        "no {} in {}",
        name[1],
        folder[1]
    );
    // It cannot be renamed: another name is refused, and it stays what it is.
    let reply = c.cmds(&["CONFIG", "SET", "dbfilename", "other.rdb"]);
    assert!(
        matches!(&reply, RespValue::Error(e) if e == &format!(
            "ERR CONFIG SET failed (possibly related to argument '{}') - can't set immutable config",
            "dbfilename"
        )),
        "{reply:?}"
    );
    assert!(c.cmds(&["CONFIG", "SET", "dbfilename", "dump.kdb"]).is_ok());
    assert_eq!(
        pair(&mut c, &["CONFIG", "GET", "dbfilename"]),
        ["dbfilename", "dump.kdb"]
    );
}

/// `CONFIG GET appendfilename` names the append-only file the server writes,
/// as Redis does (#855). It answered an empty list, and `CONFIG SET` of the
/// same name was refused for want of a row to compare with.
#[test]
fn config_get_appendfilename_names_the_append_only_file() {
    /// The server, ended with the test, whether it passes or not.
    struct Killed(std::process::Child);
    impl Drop for Killed {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let _server = Killed(
        std::process::Command::new(
            std::env::var("CARGO_BIN_EXE_kividb").unwrap_or_else(|_| "target/debug/kividb".into()),
        )
        .current_dir(dir.path())
        .args([
            "--port",
            &port.to_string(),
            "--bind",
            "127.0.0.1",
            "--dir",
            dir.path().to_str().unwrap(),
            "--appendonly",
            "yes",
            "--loglevel",
            "error",
        ])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .spawn()
        .expect("failed to start kividb"),
    );
    let mut c = Conn::connect(port);
    let pair = |c: &mut Conn, args: &[&str]| -> Vec<String> {
        c.cmds(args)
            .as_array()
            .iter()
            .map(|v| v.as_str().to_string())
            .collect()
    };

    let name = pair(&mut c, &["CONFIG", "GET", "appendfilename"]);
    assert_eq!(name, ["appendfilename", "appendonly.aof"]);
    // A pattern lists it once.
    let listed = pair(&mut c, &["CONFIG", "GET", "appendfilen*"]);
    assert_eq!(listed, ["appendfilename", "appendonly.aof"]);
    // The file the log is written to is the one named, where `dir` says.
    assert!(c.cmds(&["SET", "k", "v"]).is_ok());
    let folder = pair(&mut c, &["CONFIG", "GET", "dir"]);
    let mut waited = 0;
    while !std::path::Path::new(&folder[1]).join(&name[1]).is_file() {
        waited += 1;
        assert!(waited < 500, "no {} in {}", name[1], folder[1]);
        thread::sleep(Duration::from_millis(20));
    }
    // Its own name is accepted; another is refused and it stays what it is.
    assert!(
        c.cmds(&["CONFIG", "SET", "appendfilename", "appendonly.aof"])
            .is_ok()
    );
    let reply = c.cmds(&["CONFIG", "SET", "appendfilename", "other.aof"]);
    assert!(
        matches!(&reply, RespValue::Error(e) if e.contains("appendfilename")),
        "{reply:?}"
    );
    assert_eq!(
        pair(&mut c, &["CONFIG", "GET", "appendfilename"]),
        ["appendfilename", "appendonly.aof"]
    );
}

/// Redis's refusal of a `DEBUG` that `enable-debug-command` does not allow.
const DEBUG_NOT_ALLOWED: &str = "ERR DEBUG command not allowed. If the enable-debug-command \
    option is set to \"local\", you can run it from a local connection, otherwise you need to \
    set this option in the configuration file, and then restart the server.";

fn is_debug_refusal(reply: &RespValue) -> bool {
    matches!(reply, RespValue::Error(e) if e == DEBUG_NOT_ALLOWED)
}

/// `DEBUG` is refused by default and with `no`, and allowed with `yes`.
#[test]
fn debug_is_refused_unless_enable_debug_command_allows_it() {
    let conf_dir = tempfile::tempdir().unwrap();
    let conf = conf_dir.path().join("kividb.conf");
    std::fs::write(&conf, "enable-debug-command no\n").unwrap();
    let conf = conf.to_str().unwrap();
    for (args, mode) in [
        (["--configfile", ""], "no"),
        (["--configfile", conf], "no"),
        (["--enable-debug-command", "yes"], "yes"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let port = free_port().to_string();
        let mut all = vec![
            "--port",
            &port,
            "--bind",
            "127.0.0.1",
            "--loglevel",
            "warning",
        ];
        all.extend(["--dir", dir.path().to_str().unwrap()]);
        all.extend(args);
        let _server = spawn_kividb(&all);
        let mut c = Conn::connect(port.parse().unwrap());
        assert_eq!(config_get(&mut c, "enable-debug-command"), mode, "{args:?}");
        let reply = c.cmds(&["DEBUG", "SLEEP", "0"]);
        if mode == "yes" {
            assert!(reply.is_ok(), "{args:?}: {reply:?}");
        } else {
            assert!(is_debug_refusal(&reply), "{args:?}: {reply:?}");
        }
    }
}

/// As in Redis, `enable-debug-command` is set at startup only.
#[test]
fn config_set_cannot_change_enable_debug_command() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    for value in ["no", "yes", "local"] {
        let reply = c.cmds(&["CONFIG", "SET", "enable-debug-command", value]);
        assert!(
            matches!(&reply, RespValue::Error(e) if e == "ERR CONFIG SET failed (possibly \
                related to argument 'enable-debug-command') - can't set immutable config"),
            "{value}: {reply:?}"
        );
    }
    assert_eq!(config_get(&mut c, "enable-debug-command"), "no");
    assert!(is_debug_refusal(&c.cmds(&["DEBUG", "SLEEP", "0"])));
}

/// Refused inside `MULTI` and scripts too, and only after the ACL check.
#[test]
fn debug_is_refused_in_transactions_and_scripts_after_the_acl_check() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let aborted =
        |reply: &RespValue| matches!(reply, RespValue::Error(e) if e.starts_with("EXECABORT"));
    assert!(c.cmds(&["MULTI"]).is_ok());
    assert!(is_debug_refusal(&c.cmds(&["DEBUG", "SLEEP", "0"])));
    let exec = c.cmds(&["EXEC"]);
    assert!(aborted(&exec), "{exec:?}");
    #[cfg(feature = "lua")]
    {
        let reply = c.cmds(&["EVAL", "return redis.call('DEBUG', 'SLEEP', '0')", "0"]);
        assert!(
            matches!(&reply, RespValue::Error(e) if e.contains("not allowed from script")),
            "{reply:?}"
        );
    }

    let user = [
        "ACL", "SETUSER", "nodebug", "on", "nopass", "+@all", "-debug",
    ];
    assert!(c.cmds(&user).is_ok());
    assert!(c.cmds(&["AUTH", "nodebug", "any"]).is_ok());
    let noperm =
        |reply: &RespValue| matches!(reply, RespValue::Error(e) if e.starts_with("NOPERM"));
    let reply = c.cmds(&["DEBUG", "SLEEP", "0"]);
    assert!(noperm(&reply), "{reply:?}");
    assert!(c.cmds(&["MULTI"]).is_ok());
    let reply = c.cmds(&["DEBUG", "SLEEP", "0"]);
    assert!(noperm(&reply), "{reply:?}");
    let exec = c.cmds(&["EXEC"]);
    assert!(aborted(&exec), "{exec:?}");
}

/// With `local`, loopback clients may run `DEBUG` and others may not.
#[test]
fn enable_debug_command_local_lets_only_loopback_clients_debug() {
    let dir = tempfile::tempdir().unwrap();
    let conf = dir.path().join("kividb.conf");
    std::fs::write(&conf, "enable-debug-command local\n").unwrap();
    let port = free_port();
    let _server = spawn_kividb(&[
        "--configfile",
        conf.to_str().unwrap(),
        "--port",
        &port.to_string(),
        "--bind",
        "0.0.0.0",
        "--loglevel",
        "warning",
        "--dir",
        dir.path().to_str().unwrap(),
    ]);
    let mut c = Conn::connect(port);
    assert_eq!(config_get(&mut c, "enable-debug-command"), "local");
    assert!(c.cmds(&["DEBUG", "SLEEP", "0"]).is_ok());

    let Some(ip) = non_loopback_ip() else {
        eprintln!("no address here but loopback: the non-local client is not tried");
        return;
    };
    let stream = TcpStream::connect((ip, port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let reply = Conn(stream).cmds(&["DEBUG", "SLEEP", "0"]);
    assert!(is_debug_refusal(&reply), "{ip}: {reply:?}");
}

/// This machine's address on its default route, if it has one.
fn non_loopback_ip() -> Option<std::net::IpAddr> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    // A UDP connect sends nothing; it only picks the route.
    socket.connect("192.0.2.1:9").ok()?;
    let ip = socket.local_addr().ok()?.ip();
    (!ip.is_loopback() && !ip.is_unspecified()).then_some(ip)
}
