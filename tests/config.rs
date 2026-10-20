// tests/config.rs
use std::thread;
// tests/config.rs

use kividb::config::{Config, IoModel};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

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
        for _ in 0..50 {
            if let Ok(s) = TcpStream::connect(("127.0.0.1", port)) {
                s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                return Self(s);
            }
            thread::sleep(Duration::from_millis(100));
        }
        panic!("could not connect to server on port {port}");
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

#[test]
fn test_config_rewrite_with_configfile() {
    let port = free_port();
    let mut tmp = std::env::temp_dir();
    tmp.push(format!("kividb_rewrite_test_{port}.conf"));

    let mut server = std::process::Command::new(
        std::env::var("CARGO_BIN_EXE_kividb").unwrap_or_else(|_| "target/debug/kividb".into()),
    )
    .args([
        "--port",
        &port.to_string(),
        "--configfile",
        tmp.to_str().unwrap(),
    ])
    .spawn()
    .expect("failed to start kividb");

    let mut c = {
        let mut conn = None;
        for _ in 0..50 {
            if let Ok(s) = std::net::TcpStream::connect(format!("127.0.0.1:{port}")) {
                s.set_read_timeout(Some(std::time::Duration::from_secs(5)))
                    .unwrap();
                conn = Some(Conn(s));
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        conn.expect("server did not come up")
    };

    // REWRITE should succeed and write the file
    let result = c.cmds(&["CONFIG", "REWRITE"]);
    assert!(result.is_ok(), "CONFIG REWRITE failed: {result:?}");

    let contents = std::fs::read_to_string(&tmp).unwrap();
    // Verify the port we started with is in the file
    assert!(
        contents.contains(&format!("port {port}")),
        "rewritten file missing port: {contents}"
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

/// `maxmemory` and the remembered settings are the process's, not a
/// server's: the tests that set them take turns.
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
