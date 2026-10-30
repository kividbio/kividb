// tests/acl.rs
//
// Integration tests for the ACL subsystem (PR2):
//   ACL WHOAMI, ACL LIST, ACL USERS, ACL CAT, ACL GETUSER,
//   ACL LOG, ACL SETUSER, ACL DELUSER, ACL GENPASS, ACL DRYRUN,
//   ACL SAVE/LOAD (no aclfile configured → ERR),
//   AUTH with multi-user ACL store,
//   Command permission enforcement (NOPERM),
//   is_command_allowed unit tests (pure logic, no server needed)

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

// ── Minimal RESP client ───────────────────────────────────────────────────────

struct Conn {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

impl Conn {
    fn connect(port: u16) -> Self {
        let stream = (0..20)
            .find_map(|_| {
                thread::sleep(Duration::from_millis(50));
                TcpStream::connect(("127.0.0.1", port)).ok()
            })
            .expect("could not connect to test server");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let reader = BufReader::new(stream.try_clone().unwrap());
        Self { stream, reader }
    }

    fn cmd(&mut self, args: &[&[u8]]) -> RespValue {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            buf.extend_from_slice(arg);
            buf.extend_from_slice(b"\r\n");
        }
        self.stream.write_all(&buf).unwrap();
        self.read_value()
    }

    fn cmds(&mut self, args: &[&str]) -> RespValue {
        let owned: Vec<Vec<u8>> = args.iter().map(|s| s.as_bytes().to_vec()).collect();
        let refs: Vec<&[u8]> = owned.iter().map(|v| v.as_slice()).collect();
        self.cmd(&refs)
    }

    fn read_value(&mut self) -> RespValue {
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        let line = line.trim_end_matches("\r\n");
        match line.as_bytes().first() {
            Some(b'+') => RespValue::SimpleStr(line[1..].to_string()),
            Some(b'-') => RespValue::Error(line[1..].to_string()),
            Some(b':') => RespValue::Int(line[1..].parse().unwrap()),
            Some(b'$') => {
                let len: i64 = line[1..].parse().unwrap();
                if len == -1 {
                    return RespValue::Nil;
                }
                let len = len as usize;
                let mut data = vec![0u8; len + 2];
                self.reader.read_exact(&mut data).unwrap();
                data.truncate(len);
                RespValue::Bulk(data)
            }
            Some(b'*') => {
                let count: i64 = line[1..].parse().unwrap();
                if count == -1 {
                    return RespValue::Nil;
                }
                let items = (0..count).map(|_| self.read_value()).collect();
                RespValue::Array(items)
            }
            _ => panic!("unexpected RESP: {:?}", line),
        }
    }
}

#[derive(Debug, PartialEq)]
enum RespValue {
    SimpleStr(String),
    Error(String),
    Int(i64),
    Bulk(Vec<u8>),
    Array(Vec<RespValue>),
    Nil,
}

impl RespValue {
    fn as_str(&self) -> &str {
        match self {
            RespValue::SimpleStr(s) => s,
            RespValue::Bulk(b) => std::str::from_utf8(b).unwrap(),
            _ => panic!("not a string: {:?}", self),
        }
    }
    fn as_int(&self) -> i64 {
        match self {
            RespValue::Int(n) => *n,
            RespValue::Bulk(b) => std::str::from_utf8(b).unwrap().parse().unwrap(),
            _ => panic!("not an int: {:?}", self),
        }
    }
    fn as_array(&self) -> &[RespValue] {
        match self {
            RespValue::Array(v) => v,
            _ => panic!("not an array: {:?}", self),
        }
    }
    fn is_ok(&self) -> bool {
        matches!(self, RespValue::SimpleStr(s) if s == "OK")
            || matches!(self, RespValue::Bulk(b) if b.as_slice() == b"OK")
    }
    fn is_nil(&self) -> bool {
        matches!(self, RespValue::Nil)
    }
    fn is_error(&self) -> bool {
        matches!(self, RespValue::Error(_))
    }
    fn error_str(&self) -> &str {
        match self {
            RespValue::Error(s) => s,
            _ => panic!("not an error: {:?}", self),
        }
    }
}

// ── Server fixtures ───────────────────────────────────────────────────────────

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

/// Start a server with no password (default ACL user, nopass).
fn start_server(port: u16) {
    thread::spawn(move || {
        let config = kividb::config::Config {
            port,
            bind: "127.0.0.1".to_string(),
            threads: 2,
            aof: false,
            databases: 16,
            maxmemory: 0,
            loglevel: "error".to_string(),
            io_model: kividb::config::IoModel::Tokio,
            replicaof: None,
            replica_announced_port: 0,
            requirepass: "".to_string(),
            aclfile: "".to_string(),
            slowlog_log_slower_than: 10_000,
            slowlog_max_len: 128,
            cluster_enabled: false,
            cluster_node_timeout: 15000,
            configfile: "./kividb.conf".to_string(),
            ..Default::default()
        };
        kividb::run_for_test(config);
    });
}

/// Start a server with a password set.
fn start_server_with_pass(port: u16, pass: &'static str) {
    thread::spawn(move || {
        let config = kividb::config::Config {
            port,
            bind: "127.0.0.1".to_string(),
            threads: 2,
            aof: false,
            databases: 16,
            maxmemory: 0,
            loglevel: "error".to_string(),
            io_model: kividb::config::IoModel::Tokio,
            replicaof: None,
            replica_announced_port: 0,
            requirepass: pass.to_string(),
            aclfile: "".to_string(),
            slowlog_log_slower_than: 10_000,
            slowlog_max_len: 128,
            cluster_enabled: false,
            cluster_node_timeout: 15000,
            configfile: "./kividb.conf".to_string(),
            ..Default::default()
        };
        kividb::run_for_test(config);
    });
}

/// Start a server with no requirepass (so connections implicitly authenticate
/// as "default") but a real `--aclfile` on disk, restricting that "default"
/// user's own rules before the server ever starts. This exercises a
/// different code path than `ACL SETUSER` on a live connection: the
/// connection-open-time seeding of the restriction counters, not the
/// AUTH-time seeding.
fn start_server_with_aclfile(port: u16, acl_contents: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("kividb_test_default_acl_{port}.acl"));
    std::fs::write(&path, acl_contents).expect("write test aclfile");
    let path_str = path.to_string_lossy().to_string();
    thread::spawn(move || {
        let config = kividb::config::Config {
            port,
            bind: "127.0.0.1".to_string(),
            threads: 2,
            aof: false,
            databases: 16,
            maxmemory: 0,
            loglevel: "error".to_string(),
            io_model: kividb::config::IoModel::Tokio,
            replicaof: None,
            replica_announced_port: 0,
            requirepass: "".to_string(),
            aclfile: path_str,
            slowlog_log_slower_than: 10_000,
            slowlog_max_len: 128,
            cluster_enabled: false,
            cluster_node_timeout: 15000,
            configfile: "./kividb.conf".to_string(),
            ..Default::default()
        };
        kividb::run_for_test(config);
    });
    path
}

// ═════════════════════════════════════════════════════════════════════════════
// Pure unit tests for acl::is_command_allowed (no server needed)
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn test_is_command_allowed_plus_all() {
    let user = acl_user("default", true, true, vec![], "+@all", true);
    assert!(allowed(&user, "GET"));
    assert!(allowed(&user, "SET"));
    assert!(allowed(&user, "ZADD"));
}

#[test]
fn test_is_command_allowed_minus_all() {
    let user = acl_user("readonly", true, true, vec![], "-@all", true);
    assert!(!allowed(&user, "GET"));
    assert!(!allowed(&user, "SET"));
}

#[test]
fn test_is_command_allowed_category_grant() {
    // -@all then +@string: only string commands allowed
    let user = acl_user("stronly", true, true, vec![], "-@all +@string", true);
    assert!(allowed(&user, "GET"));
    assert!(allowed(&user, "SET"));
    assert!(!allowed(&user, "LPUSH"));
    assert!(!allowed(&user, "ZADD"));
}

#[test]
fn test_is_command_allowed_specific_command_grant() {
    // -@all then +get: only GET allowed
    let user = acl_user("getonly", true, true, vec![], "-@all +get", true);
    assert!(allowed(&user, "GET"));
    assert!(!allowed(&user, "SET"));
    assert!(!allowed(&user, "MGET"));
}

#[test]
fn test_is_command_allowed_disabled_user() {
    // Disabled user — no commands allowed regardless of rules
    let user = acl_user("disabled", false, true, vec![], "+@all", true);
    assert!(!allowed(&user, "GET"));
}

#[test]
fn test_is_command_allowed_rules_applied_left_to_right() {
    // +@all -@string: everything allowed except string commands
    let user = acl_user("nostr", true, true, vec![], "+@all -@string", true);
    // String commands denied
    assert!(!allowed(&user, "GET"));
    assert!(!allowed(&user, "SET"));
    // Non-string commands allowed
    assert!(allowed(&user, "LPUSH"));
    assert!(allowed(&user, "ZADD"));
}

#[test]
fn test_apply_rule_on_off() {
    let mut user = acl_user("u", false, false, vec![], "-@all", false);
    kividb::acl::apply_rule(&mut user, "on").unwrap();
    assert!(user.enabled);
    kividb::acl::apply_rule(&mut user, "off").unwrap();
    assert!(!user.enabled);
}

#[test]
fn test_apply_rule_password() {
    let mut user = acl_user("u", true, false, vec![], "+@all", true);
    kividb::acl::apply_rule(&mut user, ">mysecret").unwrap();
    assert_eq!(user.passwords.len(), 1);
    assert!(!user.nopass);

    // Adding same password again should not duplicate
    kividb::acl::apply_rule(&mut user, ">mysecret").unwrap();
    assert_eq!(user.passwords.len(), 1);

    // Remove the password
    kividb::acl::apply_rule(&mut user, "<mysecret").unwrap();
    assert_eq!(user.passwords.len(), 0);
}

#[test]
fn test_apply_rule_nopass_clears_passwords() {
    let mut user = acl_user("u", true, false, vec!["somehash".into()], "+@all", true);
    kividb::acl::apply_rule(&mut user, "nopass").unwrap();
    assert!(user.nopass);
    assert!(user.passwords.is_empty());
}

#[test]
fn test_apply_rule_reset() {
    let mut user = acl_user("u", true, true, vec!["hash".into()], "+@all", true);
    kividb::acl::apply_rule(&mut user, "reset").unwrap();
    assert!(!user.enabled);
    assert!(!user.nopass);
    assert!(user.passwords.is_empty());
    assert_eq!(user.commands, "-@all");
    assert!(user.keys.is_empty());
    assert!(user.channels.is_empty());
}

#[test]
fn test_sha256_hex_known_value() {
    // SHA-256("password") = well-known value
    let result = kividb::acl::sha256_hex("password");
    assert_eq!(
        result,
        "5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8"
    );
}

// ═════════════════════════════════════════════════════════════════════════════
// ACL WHOAMI
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn test_acl_whoami_default() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    assert_eq!(c.cmds(&["ACL", "WHOAMI"]).as_str(), "default");
}

// ═════════════════════════════════════════════════════════════════════════════
// ACL LIST
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn test_acl_list_contains_default() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["ACL", "LIST"]);
    let arr = result.as_array();
    assert!(!arr.is_empty());

    // At least one entry should start with "user default"
    assert!(
        arr.iter().any(|v| v.as_str().starts_with("user default")),
        "ACL LIST should contain the default user"
    );
}

#[test]
fn test_acl_list_default_user_nopass() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["ACL", "LIST"]);
    let default_line = result
        .as_array()
        .iter()
        .find(|v| v.as_str().starts_with("user default"))
        .expect("default user missing from ACL LIST")
        .as_str()
        .to_string();

    assert!(
        default_line.contains("nopass"),
        "default user without requirepass should have nopass: {default_line}"
    );
    assert!(
        default_line.contains("+@all"),
        "default user should have +@all: {default_line}"
    );
}

#[test]
fn test_acl_list_with_password_has_hash() {
    let port = free_port();
    start_server_with_pass(port, "mysecret");
    let mut c = Conn::connect(port);
    assert!(c.cmds(&["AUTH", "mysecret"]).is_ok());

    let result = c.cmds(&["ACL", "LIST"]);
    let default_line = result
        .as_array()
        .iter()
        .find(|v| v.as_str().starts_with("user default"))
        .expect("default user missing")
        .as_str()
        .to_string();

    // Should have a hash (#...) not nopass
    assert!(
        default_line.contains('#'),
        "password-protected server should have hash in ACL LIST: {default_line}"
    );
    assert!(
        !default_line.contains("nopass"),
        "password-protected server should not have nopass: {default_line}"
    );
}

// ═════════════════════════════════════════════════════════════════════════════
// ACL USERS
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn test_acl_users_contains_default() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["ACL", "USERS"]);
    let users: Vec<&str> = result.as_array().iter().map(|v| v.as_str()).collect();
    assert!(
        users.contains(&"default"),
        "ACL USERS should contain 'default'"
    );
}

// ═════════════════════════════════════════════════════════════════════════════
// ACL CAT
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn test_acl_cat_no_args_returns_categories() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["ACL", "CAT"]);
    let cats: Vec<&str> = result.as_array().iter().map(|v| v.as_str()).collect();

    for expected in &["string", "hash", "list", "set", "generic", "server"] {
        assert!(
            cats.contains(expected),
            "ACL CAT missing category: {expected}"
        );
    }
}

#[test]
fn test_acl_cat_string_contains_get_set() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["ACL", "CAT", "string"]);
    let cmds: Vec<String> = result
        .as_array()
        .iter()
        .map(|v| v.as_str().to_uppercase())
        .collect();

    assert!(cmds.contains(&"GET".to_string()));
    assert!(cmds.contains(&"SET".to_string()));
    assert!(cmds.contains(&"APPEND".to_string()));
}

#[test]
fn test_acl_cat_hash_contains_hset() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["ACL", "CAT", "hash"]);
    let cmds: Vec<String> = result
        .as_array()
        .iter()
        .map(|v| v.as_str().to_uppercase())
        .collect();

    assert!(cmds.contains(&"HSET".to_string()));
    assert!(cmds.contains(&"HGET".to_string()));
}

#[test]
fn test_acl_cat_unknown_category_returns_empty() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["ACL", "CAT", "nonexistent_category"]);
    assert!(result.is_error());
}

// ═════════════════════════════════════════════════════════════════════════════
// ACL GETUSER
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn test_acl_getuser_default() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["ACL", "GETUSER", "default"]);
    let arr = result.as_array();

    // Should be a flat key-value array
    assert!(!arr.is_empty());
    assert_eq!(
        arr.len() % 2,
        0,
        "GETUSER result should have even number of elements"
    );

    // Find "flags" key and check its value
    let map: std::collections::HashMap<&str, &RespValue> = arr
        .chunks(2)
        .map(|pair| (pair[0].as_str(), &pair[1]))
        .collect();

    assert!(
        map.contains_key("flags"),
        "GETUSER should have 'flags' field"
    );
    assert!(
        map.contains_key("commands"),
        "GETUSER should have 'commands' field"
    );
    assert!(map.contains_key("keys"), "GETUSER should have 'keys' field");
}

#[test]
fn test_acl_getuser_missing_user_returns_nil() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["ACL", "GETUSER", "nosuchuser"]).is_nil());
}

// ═════════════════════════════════════════════════════════════════════════════
// ACL SETUSER / DELUSER
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn test_acl_setuser_creates_user() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(
        c.cmds(&["ACL", "SETUSER", "alice", "on", "nopass", "+@all", "~*"])
            .is_ok()
    );

    let users: Vec<String> = c
        .cmds(&["ACL", "USERS"])
        .as_array()
        .iter()
        .map(|v| v.as_str().to_string())
        .collect();
    assert!(users.contains(&"alice".to_string()));
}

#[test]
fn test_acl_setuser_with_password() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(
        c.cmds(&["ACL", "SETUSER", "bob", "on", ">bobsecret", "+@all", "~*"])
            .is_ok()
    );

    let list = c.cmds(&["ACL", "LIST"]);
    let bob_line = list
        .as_array()
        .iter()
        .find(|v| v.as_str().contains("bob"))
        .expect("bob not in ACL LIST")
        .as_str()
        .to_string();

    assert!(
        bob_line.contains('#'),
        "bob should have hashed password: {bob_line}"
    );
    assert!(!bob_line.contains("nopass"));
}

#[test]
fn test_acl_setuser_idempotent() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(
        c.cmds(&["ACL", "SETUSER", "carol", "on", "nopass", "+@all"])
            .is_ok()
    );
    // Setting again should succeed and update
    assert!(c.cmds(&["ACL", "SETUSER", "carol", "off"]).is_ok());

    let list = c.cmds(&["ACL", "LIST"]);
    let carol_line = list
        .as_array()
        .iter()
        .find(|v| v.as_str().contains("carol"))
        .expect("carol not in ACL LIST")
        .as_str()
        .to_string();
    assert!(
        carol_line.contains(" off "),
        "carol should be off: {carol_line}"
    );
}

#[test]
fn test_acl_setuser_reset_rule() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ACL", "SETUSER", "dave", "on", "nopass", "+@all"]);
    // Reset should clear everything
    c.cmds(&["ACL", "SETUSER", "dave", "reset"]);

    let result = c.cmds(&["ACL", "GETUSER", "dave"]);
    let arr = result.as_array();
    let map: std::collections::HashMap<&str, &RespValue> = arr
        .chunks(2)
        .map(|pair| (pair[0].as_str(), &pair[1]))
        .collect();

    // After reset: disabled, no commands
    let flags: Vec<&str> = map["flags"].as_array().iter().map(|v| v.as_str()).collect();
    assert!(flags.contains(&"off"), "dave should be off after reset");
}

#[test]
fn test_acl_deluser_removes_user() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ACL", "SETUSER", "eve", "on", "nopass", "+@all"]);
    let deleted = c.cmds(&["ACL", "DELUSER", "eve"]).as_int();
    assert_eq!(deleted, 1);

    assert!(c.cmds(&["ACL", "GETUSER", "eve"]).is_nil());
}

#[test]
fn test_acl_deluser_multiple() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ACL", "SETUSER", "u1", "on", "nopass"]);
    c.cmds(&["ACL", "SETUSER", "u2", "on", "nopass"]);
    let deleted = c.cmds(&["ACL", "DELUSER", "u1", "u2", "nosuch"]).as_int();
    assert_eq!(deleted, 2); // nosuch not counted
}

#[test]
fn test_acl_deluser_default_fails() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["ACL", "DELUSER", "default"]).is_error());
}

// ═════════════════════════════════════════════════════════════════════════════
// ACL GENPASS
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn test_acl_genpass_default_64_chars() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["ACL", "GENPASS"]);
    let pass = result.as_str();
    // 256 bits = 32 bytes = 64 hex chars
    assert_eq!(pass.len(), 64, "GENPASS default should be 64 hex chars");
    assert!(
        pass.chars().all(|ch| ch.is_ascii_hexdigit()),
        "GENPASS should be all hex: {pass}"
    );
}

#[test]
fn test_acl_genpass_custom_bits() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["ACL", "GENPASS", "128"]);
    let pass = result.as_str();
    // 128 bits = 16 bytes = 32 hex chars
    assert_eq!(pass.len(), 32);
    assert!(pass.chars().all(|ch| ch.is_ascii_hexdigit()));
}

#[test]
fn test_acl_genpass_randomness() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Two consecutive calls should (with overwhelming probability) differ
    let p1 = c.cmds(&["ACL", "GENPASS"]).as_str().to_string();
    let p2 = c.cmds(&["ACL", "GENPASS"]).as_str().to_string();
    assert_ne!(p1, p2, "GENPASS should produce different values each call");
}

#[test]
fn test_acl_genpass_invalid_bits_errors() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Non-multiples of 4 are rounded up to the next multiple of 4 (8 → 8 hex chars)
    let r7 = c.cmds(&["ACL", "GENPASS", "7"]);
    assert!(!r7.is_error());
    // 0 bits — out of range
    assert!(c.cmds(&["ACL", "GENPASS", "0"]).is_error());
    // Too large — out of range
    assert!(c.cmds(&["ACL", "GENPASS", "8192"]).is_error());
}

// ═════════════════════════════════════════════════════════════════════════════
// ACL DRYRUN
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn test_acl_dryrun_default_user_allowed() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // default user has +@all, so GET should be allowed
    assert!(c.cmds(&["ACL", "DRYRUN", "default", "GET", "key"]).is_ok());
}

#[test]
fn test_acl_dryrun_restricted_user_denied() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Create a string-only user
    c.cmds(&[
        "ACL", "SETUSER", "stronly", "on", "nopass", "-@all", "+@string", "~*",
    ]);

    // String command allowed
    assert!(c.cmds(&["ACL", "DRYRUN", "stronly", "GET", "key"]).is_ok());

    // List command denied — DRYRUN returns a bulk string, not OK
    let result = c.cmds(&["ACL", "DRYRUN", "stronly", "LPUSH", "key", "val"]);
    assert!(
        !result.is_ok(),
        "DRYRUN should not return OK for denied command"
    );
}

#[test]
fn test_acl_dryrun_unknown_user_errors() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(
        c.cmds(&["ACL", "DRYRUN", "nosuchuser", "GET", "key"])
            .is_error()
    );
}

#[test]
fn test_acl_dryrun_disabled_user_denied() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "ACL",
        "SETUSER",
        "disabled_user",
        "off",
        "nopass",
        "+@all",
        "~*",
    ]);

    let result = c.cmds(&["ACL", "DRYRUN", "disabled_user", "GET", "key"]);
    assert!(!result.is_ok(), "Disabled user should be denied by DRYRUN");
}

// ═════════════════════════════════════════════════════════════════════════════
// ACL LOG
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn test_acl_log_empty_initially() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["ACL", "LOG"]);
    assert_eq!(result.as_array().len(), 0);
}

#[test]
fn test_acl_log_records_noauth_attempt() {
    let port = free_port();
    start_server_with_pass(port, "secret");
    let mut c = Conn::connect(port);

    // Try a command without authenticating
    c.cmds(&["GET", "somekey"]); // will fail with NOAUTH

    // Now authenticate and check log
    c.cmds(&["AUTH", "secret"]);
    let log = c.cmds(&["ACL", "LOG"]);
    let entries = log.as_array();

    assert!(
        !entries.is_empty(),
        "ACL LOG should have recorded the NOAUTH attempt"
    );
}

#[test]
fn test_acl_log_reset_clears() {
    let port = free_port();
    start_server_with_pass(port, "secret");
    let mut c = Conn::connect(port);

    c.cmds(&["GET", "key"]); // NOAUTH
    c.cmds(&["AUTH", "secret"]);

    // Verify there's something in the log
    assert!(!c.cmds(&["ACL", "LOG"]).as_array().is_empty());

    // Reset
    assert!(c.cmds(&["ACL", "LOG", "RESET"]).is_ok());

    // Should be empty now
    assert_eq!(c.cmds(&["ACL", "LOG"]).as_array().len(), 0);
}

#[test]
fn test_acl_log_with_count() {
    let port = free_port();
    start_server_with_pass(port, "secret");
    let mut c = Conn::connect(port);

    // Generate 3 auth failures
    for _ in 0..3 {
        c.cmds(&["GET", "key"]);
    }
    c.cmds(&["AUTH", "secret"]);

    let all = c.cmds(&["ACL", "LOG"]).as_array().len();
    let limited = c.cmds(&["ACL", "LOG", "1"]).as_array().len();

    assert!(all >= 1);
    assert_eq!(limited, 1, "ACL LOG 1 should return exactly 1 entry");
}

// ═════════════════════════════════════════════════════════════════════════════
// ACL SAVE / LOAD (no aclfile configured → ERR)
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn test_acl_save_no_file_configured_errors() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["ACL", "SAVE"]);
    assert!(result.is_error());
    assert!(
        result.error_str().contains("not configured"),
        "ACL SAVE without aclfile should say not configured: {}",
        result.error_str()
    );
}

#[test]
fn test_acl_load_no_file_configured_errors() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["ACL", "LOAD"]);
    assert!(result.is_error());
    assert!(result.error_str().contains("not configured"));
}

// ═════════════════════════════════════════════════════════════════════════════
// AUTH enforcement
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn test_auth_correct_password_succeeds() {
    let port = free_port();
    start_server_with_pass(port, "correctpass");
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["AUTH", "correctpass"]).is_ok());
    // Can now issue commands
    assert!(c.cmds(&["SET", "k", "v"]).is_ok());
}

#[test]
fn test_auth_wrong_password_fails() {
    let port = free_port();
    start_server_with_pass(port, "correctpass");
    let mut c = Conn::connect(port);

    let result = c.cmds(&["AUTH", "wrongpass"]);
    assert!(result.is_error());
    assert!(result.error_str().contains("WRONGPASS"));
}

#[test]
fn test_noauth_blocks_commands() {
    let port = free_port();
    start_server_with_pass(port, "secret");
    let mut c = Conn::connect(port);

    let result = c.cmds(&["SET", "k", "v"]);
    assert!(result.is_error());
    assert!(result.error_str().contains("NOAUTH"));
}

#[test]
fn test_ping_requires_auth() {
    let port = free_port();
    start_server_with_pass(port, "secret");
    let mut c = Conn::connect(port);

    // PING is NOT part of the pre-auth allowlist (matches real Redis, which
    // only allows AUTH/HELLO/RESET/QUIT before authentication).
    let result = c.cmds(&["PING"]);
    assert!(result.is_error());
    assert!(result.error_str().contains("NOAUTH"));
}

/// With no password set, `AUTH <password>` is Redis's error: how a client
/// finds out that the password it was given is not in use. It was OK (#135).
/// `AUTH default <anything>` logs in, as a `nopass` user does in Redis.
#[test]
fn test_auth_password_when_none_is_set_is_an_error() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert_eq!(
        c.cmds(&["AUTH", "anything"]).error_str(),
        "ERR AUTH <password> called without any password configured for the default user. \
         Are you sure your configuration is correct?"
    );
    assert!(c.cmds(&["AUTH", "default", "anything"]).is_ok());
    assert!(c.cmds(&["PING"]).as_str() == "PONG");
}

#[test]
fn test_auth_redis6_username_form() {
    // AUTH username password — username is accepted but only password is checked
    let port = free_port();
    start_server_with_pass(port, "mypass");
    let mut c = Conn::connect(port);

    // AUTH default mypass (Redis 6 ACL form)
    assert!(c.cmds(&["AUTH", "default", "mypass"]).is_ok());
}

// ═════════════════════════════════════════════════════════════════════════════
// Command permission enforcement (NOPERM)
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn test_noperm_restricted_user_blocked() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Create a user with only string commands, give them a password
    c.cmds(&[
        "ACL", "SETUSER", "limited", "on", ">limpass", "-@all", "+@string", "+acl", "~*",
    ]);

    // Connect as limited user — need a fresh connection
    // We can't easily switch users on the same connection in the test harness,
    // so we verify via DRYRUN that the permission model is correct
    assert!(c.cmds(&["ACL", "DRYRUN", "limited", "GET", "k"]).is_ok());
    let result = c.cmds(&["ACL", "DRYRUN", "limited", "LPUSH", "k", "v"]);
    assert!(
        !result.is_ok(),
        "LPUSH should be denied for string-only user"
    );
}

#[test]
fn test_acl_key_pattern_enforced() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Create a user with access only to foo:* keys, no password
    assert!(
        c.cmds(&[
            "ACL", "SETUSER", "keyuser", "on", "nopass", "+@all", "~foo:*"
        ])
        .is_ok()
    );

    // Verify user was created
    let users: Vec<String> = c
        .cmds(&["ACL", "USERS"])
        .as_array()
        .iter()
        .map(|v| v.as_str().to_string())
        .collect();
    assert!(
        users.contains(&"keyuser".to_string()),
        "keyuser not created"
    );

    // Connect as keyuser — for nopass users AUTH still works with any password
    let mut u = Conn::connect(port);
    let auth_result = u.cmds(&["AUTH", "keyuser", "anypassword"]);
    assert!(
        auth_result.is_ok(),
        "AUTH as keyuser failed: {auth_result:?}"
    );

    // Verify we're actually authenticated as keyuser
    let whoami = u.cmds(&["ACL", "WHOAMI"]);
    assert_eq!(whoami.as_str(), "keyuser", "not authenticated as keyuser");

    // foo:bar should be allowed
    let r1 = u.cmds(&["SET", "foo:bar", "value"]);
    assert!(r1.is_ok(), "SET foo:bar should be allowed: {r1:?}");

    // bar:key should be denied
    let r2 = u.cmds(&["SET", "bar:key", "value"]);
    assert!(r2.is_error(), "SET bar:key should be denied: {r2:?}");
    assert!(
        r2.error_str().contains("NOPERM"),
        "expected NOPERM, got: {}",
        r2.error_str()
    );
}

/// Live regression test for the documented ACL command-enforcement bug:
/// a user configured `+@all -flushall -flushdb` must not be able to run
/// FLUSHALL on a real connection (previously only checked via ACL DRYRUN,
/// which never exercised the fast path where the bug lived).
#[test]
fn test_noperm_command_rule_enforced_live() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(
        c.cmds(&[
            "ACL",
            "SETUSER",
            "noflusher",
            "on",
            "nopass",
            "~*",
            "+@all",
            "-flushall",
            "-flushdb",
        ])
        .is_ok()
    );

    let mut u = Conn::connect(port);
    let auth_result = u.cmds(&["AUTH", "noflusher", "anypassword"]);
    assert!(auth_result.is_ok(), "AUTH failed: {auth_result:?}");
    assert_eq!(u.cmds(&["ACL", "WHOAMI"]).as_str(), "noflusher");

    // Ordinary commands still work.
    assert!(u.cmds(&["SET", "k", "v"]).is_ok());

    // FLUSHALL/FLUSHDB must be denied, live — this is exactly the scenario
    // of a user denied only the flush commands.
    let flushall = u.cmds(&["FLUSHALL"]);
    assert!(
        flushall.is_error(),
        "FLUSHALL should be denied: {flushall:?}"
    );
    assert!(flushall.error_str().contains("NOPERM"));

    let flushdb = u.cmds(&["FLUSHDB"]);
    assert!(flushdb.is_error(), "FLUSHDB should be denied: {flushdb:?}");
    assert!(flushdb.error_str().contains("NOPERM"));
}

/// The `readonly` sugar keyword (expands to `-@write`) blocks data-mutating
/// commands live, while leaving reads intact.
#[test]
fn test_readonly_template_enforced_live() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(
        c.cmds(&[
            "ACL", "SETUSER", "analyst", "on", "nopass", "~*", "+@all", "readonly",
        ])
        .is_ok()
    );

    // readonly expands to -@write immediately — GETUSER should reflect that.
    let getuser = c.cmds(&["ACL", "GETUSER", "analyst"]);
    assert!(!getuser.is_error(), "GETUSER failed: {getuser:?}");
    let commands_str = getuser.as_array()[5].as_str();
    assert!(
        commands_str.contains("-@write"),
        "expected -@write in commands rule, got: {commands_str}"
    );

    c.cmds(&["SET", "seed", "value"]); // seed data via the admin connection

    let mut u = Conn::connect(port);
    assert!(u.cmds(&["AUTH", "analyst", "anypassword"]).is_ok());
    assert_eq!(u.cmds(&["ACL", "WHOAMI"]).as_str(), "analyst");

    // Reads are allowed.
    assert_eq!(u.cmds(&["GET", "seed"]).as_str(), "value");

    // Writes are denied.
    for write_cmd in [
        vec!["SET".to_string(), "seed".to_string(), "other".to_string()],
        vec!["DEL".to_string(), "seed".to_string()],
        vec!["FLUSHALL".to_string()],
    ] {
        let args: Vec<&str> = write_cmd.iter().map(String::as_str).collect();
        let result = u.cmds(&args);
        assert!(
            result.is_error(),
            "{args:?} should be denied for readonly user: {result:?}"
        );
        assert!(result.error_str().contains("NOPERM"));
    }
}

/// `ACL CAT read`/`ACL CAT write` must actually enumerate commands — they
/// were previously advertised by bare `ACL CAT` but had no members defined,
/// making `+@read`/`-@write` silent no-ops.
#[test]
fn test_acl_cat_read_write_populated() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let read_cmds: Vec<String> = c
        .cmds(&["ACL", "CAT", "read"])
        .as_array()
        .iter()
        .map(|v| v.as_str().to_ascii_uppercase())
        .collect();
    assert!(!read_cmds.is_empty(), "@read category should be populated");
    assert!(read_cmds.contains(&"GET".to_string()));
    assert!(!read_cmds.contains(&"SET".to_string()));

    let write_cmds: Vec<String> = c
        .cmds(&["ACL", "CAT", "write"])
        .as_array()
        .iter()
        .map(|v| v.as_str().to_ascii_uppercase())
        .collect();
    assert!(
        !write_cmds.is_empty(),
        "@write category should be populated"
    );
    assert!(write_cmds.contains(&"SET".to_string()));
    assert!(write_cmds.contains(&"FLUSHALL".to_string()));
    assert!(!write_cmds.contains(&"GET".to_string()));
}

/// Regression test for a second instance of the same bug class as
/// `test_noperm_command_rule_enforced_live`: the enforcement gate in
/// `execute_command` used to have a blanket exemption for
/// `authenticated_user == "default"`, so restricting the *literal* default
/// user's own commands (`ACL SETUSER default ... -flushall`) was silently
/// unenforced even though `ACL GETUSER default` correctly echoed the rule.
/// Uses `start_server_with_aclfile` (not `ACL SETUSER` on a live connection)
/// because the specific bug was in the connection-open-time seeding of the
/// restriction counters for connections that authenticate implicitly (no
/// requirepass set) rather than via an explicit `AUTH`.
#[test]
fn test_default_user_command_rule_enforced_live() {
    let port = free_port();
    start_server_with_aclfile(
        port,
        "user default on nopass ~* &* +@all -flushall -flushdb\n",
    );
    let mut c = Conn::connect(port);

    // Implicitly authenticated as "default" — no AUTH sent.
    assert_eq!(c.cmds(&["ACL", "WHOAMI"]).as_str(), "default");

    assert!(c.cmds(&["SET", "k", "v"]).is_ok());

    let flushall = c.cmds(&["FLUSHALL"]);
    assert!(
        flushall.is_error(),
        "FLUSHALL should be denied for a restricted default user: {flushall:?}"
    );
    assert!(flushall.error_str().contains("NOPERM"));

    let flushdb = c.cmds(&["FLUSHDB"]);
    assert!(
        flushdb.is_error(),
        "FLUSHDB should be denied for a restricted default user: {flushdb:?}"
    );
    assert!(flushdb.error_str().contains("NOPERM"));

    // The key set before FLUSHALL was denied should still be there.
    assert_eq!(c.cmds(&["GET", "k"]).as_str(), "v");
}

/// Same bug class, but for the pre-existing key-pattern restriction (not
/// touched by the command-rule fix above, but sharing the same root cause:
/// the "default" username was never checked against its own ACL rules on
/// the implicit-auth connection-open path).
#[test]
fn test_default_user_key_pattern_enforced_live() {
    let port = free_port();
    start_server_with_aclfile(port, "user default on nopass ~app:* &* +@all\n");
    let mut c = Conn::connect(port);

    assert_eq!(c.cmds(&["ACL", "WHOAMI"]).as_str(), "default");

    let denied = c.cmds(&["SET", "other:key", "v"]);
    assert!(
        denied.is_error(),
        "SET outside ~app:* should be denied for a restricted default user: {denied:?}"
    );
    assert!(denied.error_str().contains("NOPERM"));

    assert!(c.cmds(&["SET", "app:key", "v"]).is_ok());
}

// ═════════════════════════════════════════════════════════════════════════════
// AUTH reply type and QUIT
// ═════════════════════════════════════════════════════════════════════════════

/// Raw-wire helper: send bytes, read back everything that arrives, and report
/// whether the peer closed the connection.
///
/// These tests assert on the exact RESP *type* markers, which the parsing
/// `Conn` helper deliberately normalises away (`is_ok()` accepts both `+OK` and
/// `$2\r\nOK`) — that normalisation is precisely what hid this bug.
fn raw_exchange(port: u16, payload: &[u8]) -> (String, bool) {
    use std::io::ErrorKind;
    let mut s = (0..40)
        .find_map(|_| {
            thread::sleep(Duration::from_millis(50));
            TcpStream::connect(("127.0.0.1", port)).ok()
        })
        .expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    s.write_all(payload).unwrap();

    let mut out = Vec::new();
    let mut buf = [0u8; 4096];
    let mut closed = false;
    loop {
        match s.read(&mut buf) {
            Ok(0) => {
                closed = true;
                break;
            }
            Ok(n) => out.extend_from_slice(&buf[..n]),
            Err(e) if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut => break,
            Err(e) => panic!("read failed: {e}"),
        }
    }
    (String::from_utf8_lossy(&out).into_owned(), closed)
}

/// AUTH must reply with the simple string `+OK`, not a bulk string.
///
/// A bulk `$2\r\nOK\r\n` is legal RESP2 but real Redis sends `+OK`, and clients
/// that text-match a leading `+OK` on control connections read the bulk form as
/// a failed AUTH.
#[test]
fn test_auth_replies_simple_string_ok() {
    let port = free_port();
    start_server_with_pass(port, "secret");

    let (reply, _) = raw_exchange(port, b"*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n");
    assert_eq!(
        reply, "+OK\r\n",
        "AUTH success must be the simple string +OK, got {reply:?}"
    );
}

/// QUIT must reply `+OK` and close the connection.
///
/// QUIT appeared in both the pre-auth allowlist and the always-allowed list but
/// had no dispatch arm, so it returned `-ERR unknown command` and left the
/// socket open — clients relying on it to close blocked until their own timeout.
#[test]
fn test_quit_replies_ok_and_closes_connection() {
    let port = free_port();
    start_server(port);

    let (reply, closed) = raw_exchange(port, b"*1\r\n$4\r\nQUIT\r\n");
    assert_eq!(reply, "+OK\r\n", "QUIT must reply +OK, got {reply:?}");
    assert!(closed, "QUIT must close the connection");
}

/// QUIT works before authentication — it is in the pre-auth allowlist.
#[test]
fn test_quit_works_before_auth() {
    let port = free_port();
    start_server_with_pass(port, "secret");

    let (reply, closed) = raw_exchange(port, b"*1\r\n$4\r\nQUIT\r\n");
    assert_eq!(
        reply, "+OK\r\n",
        "pre-auth QUIT must reply +OK, got {reply:?}"
    );
    assert!(closed, "pre-auth QUIT must close the connection");
}

/// The exact pipelined pattern from the issue report:
/// `AUTH … / <command> / QUIT` in a single write.
///
/// This used to come back as `$2\r\nOK\r\n+OK\r\n-ERR unknown command\r\n`,
/// where a naive "no -ERR anywhere" success check concluded the *real* command
/// had failed even though it succeeded.
#[test]
fn test_pipelined_auth_command_quit_has_no_spurious_error() {
    let port = free_port();
    start_server_with_pass(port, "secret");

    let (reply, closed) = raw_exchange(
        port,
        b"*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n\
          *3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n\
          *1\r\n$4\r\nQUIT\r\n",
    );

    assert_eq!(
        reply, "+OK\r\n+OK\r\n+OK\r\n",
        "expected three simple-string OKs (AUTH, SET, QUIT), got {reply:?}"
    );
    assert!(
        !reply.contains("-ERR"),
        "no spurious error may appear in the batch, got {reply:?}"
    );
    assert!(closed, "the connection must be closed after QUIT");
}

/// Commands pipelined *after* QUIT are not executed — the connection is closing.
#[test]
fn test_commands_after_quit_are_not_executed() {
    let port = free_port();
    start_server(port);

    let (reply, closed) = raw_exchange(
        port,
        b"*1\r\n$4\r\nQUIT\r\n*3\r\n$3\r\nSET\r\n$5\r\nafter\r\n$1\r\n1\r\n",
    );
    assert_eq!(reply, "+OK\r\n", "only QUIT's own reply, got {reply:?}");
    assert!(closed);

    // Confirm the SET never landed.
    let mut c = Conn::connect(port);
    assert!(c.cmds(&["GET", "after"]).is_nil());
}

// ─────────────────────────────────────────────────────────────────────────────
// The admin / dangerous / blocking / fast / slow categories, the module
// categories, and per-subcommand rules
// ─────────────────────────────────────────────────────────────────────────────

fn user_with_commands(commands: &str) -> kividb::acl::AclUser {
    acl_user("u", true, true, vec![], commands, true)
}

/// A user for the unit tests below. `everything` gives it every key and
/// every channel; otherwise it has none.
fn acl_user(
    name: &str,
    enabled: bool,
    nopass: bool,
    passwords: Vec<String>,
    commands: &str,
    everything: bool,
) -> kividb::acl::AclUser {
    let mut user = kividb::acl::AclUser::new(name);
    user.enabled = enabled;
    user.nopass = nopass;
    user.passwords = passwords;
    user.commands = commands.into();
    if everything {
        user.keys = vec![kividb::acl::KeyPattern::read_write("*")];
        user.channels = vec!["*".into()];
    }
    user
}

/// Whether the user's rules allow the command. `name` is a command, or
/// `COMMAND|SUBCOMMAND`.
fn allowed(user: &kividb::acl::AclUser, name: &str) -> bool {
    let cmd: Vec<&str> = name.split('|').collect();
    !matches!(
        kividb::acl::check_permission(user, &cmd[0].to_ascii_uppercase(), &cmd),
        Err(kividb::acl::Denied::Command(_))
    )
}

#[test]
fn test_dangerous_category_denies_what_it_names() {
    let user = user_with_commands("+@all -@dangerous");
    for denied in [
        "FLUSHALL",
        "FLUSHDB",
        "KEYS",
        "CONFIG",
        "CONFIG|SET",
        "DEBUG",
        "SHUTDOWN",
        "REPLICAOF",
        "ACL",
        "ACL|SETUSER",
        "CLUSTER|RESET",
        "CLUSTER|FORGET",
        "CLUSTER|MIGRATION",
        "CLUSTER|SYNCSLOTS",
        "CLIENT|KILL",
        "BACKUP|START",
        "HOTKEYS|START",
    ] {
        assert!(!allowed(&user, denied), "{denied} should be denied");
    }
    // What a client library needs in order to work stays available.
    for kept in [
        "GET",
        "SET",
        "DEL",
        "CLUSTER|SLOTS",
        "CLUSTER|SHARDS",
        "CLUSTER|NODES",
        "CLUSTER|INFO",
        "CLIENT|SETNAME",
        "CLIENT|SETINFO",
        "CLIENT|ID",
        "HELLO",
        "PING",
        "LMOVEM",
        "HIMPORT|PREPARE",
    ] {
        assert!(allowed(&user, kept), "{kept} should be allowed");
    }
}

#[test]
fn test_admin_category_is_narrower_than_dangerous() {
    let user = user_with_commands("+@all -@admin");
    assert!(!allowed(&user, "CONFIG|GET"));
    assert!(!allowed(&user, "CLUSTER|MEET"));
    assert!(!allowed(&user, "SLOWLOG|GET"));
    // Destructive, but not administrative.
    assert!(allowed(&user, "FLUSHALL"));
    assert!(allowed(&user, "KEYS"));
    assert!(allowed(&user, "INFO"));
}

#[test]
fn test_subcommand_rules() {
    // A subcommand can be given back after its command was taken away…
    let user = user_with_commands("+@all -config +config|get");
    assert!(allowed(&user, "CONFIG|GET"));
    assert!(!allowed(&user, "CONFIG|SET"));
    assert!(!allowed(&user, "CONFIG"));
    // …and taken away on its own.
    let user = user_with_commands("+@all -client|kill");
    assert!(!allowed(&user, "CLIENT|KILL"));
    assert!(allowed(&user, "CLIENT|LIST"));
    // A rule naming the command covers every subcommand.
    let user = user_with_commands("-@all +cluster");
    assert!(allowed(&user, "CLUSTER|RESET"));
    assert!(!allowed(&user, "GET"));
    // The category that was removed can be partly restored.
    let user = user_with_commands("+@all -@dangerous +info +cluster|replicas");
    assert!(allowed(&user, "INFO"));
    assert!(allowed(&user, "CLUSTER|REPLICAS"));
    assert!(!allowed(&user, "CLUSTER|RESET"));
}

#[test]
fn test_acl_command_name() {
    use kividb::acl::acl_command_name;
    assert_eq!(acl_command_name("CLUSTER", Some(b"reset")), "CLUSTER|RESET");
    assert_eq!(acl_command_name("CONFIG", Some(b"Get")), "CONFIG|GET");
    assert_eq!(acl_command_name("CLUSTER", None), "CLUSTER");
    // Not a command with subcommands: the argument is a key.
    assert_eq!(acl_command_name("GET", Some(b"reset")), "GET");
}

#[test]
fn test_module_blocking_and_speed_categories() {
    let cat = kividb::commands::server::acl_cat_commands_pub;
    assert!(cat("json").contains(&"JSON.SET"));
    assert!(cat("search").contains(&"FT.SEARCH"));
    assert!(cat("search").contains(&"FT.ALIASLIST"));
    assert!(cat("timeseries").contains(&"TS.READ"));
    assert!(cat("vector").contains(&"VSIM"));
    assert!(cat("blocking").contains(&"BLMOVEM"));
    assert!(cat("transaction").contains(&"EXEC"));
    assert_eq!(cat("transaction"), cat("transactions"));
    assert!(cat("fast").contains(&"GET"));
    assert!(!cat("fast").contains(&"KEYS"));
    assert!(cat("slow").contains(&"KEYS"));
    assert!(!cat("slow").contains(&"GET"));
    // The module commands are reads and writes like any other.
    assert!(cat("read").contains(&"JSON.GET"));
    assert!(cat("read").contains(&"TS.RANGE"));
    assert!(!cat("read").contains(&"JSON.SET"));
    assert!(cat("write").contains(&"JSON.SET"));
    assert!(cat("write").contains(&"LMOVEM"));
    assert!(cat("write").contains(&"HIMPORT"));
    assert!(cat("read").contains(&"SUNIONCARD"));

    let reader = user_with_commands("-@all +@read");
    assert!(allowed(&reader, "JSON.GET"));
    assert!(!allowed(&reader, "JSON.SET"));
    let no_blocking = user_with_commands("+@all -@blocking");
    assert!(!allowed(&no_blocking, "BLPOP"));
    assert!(allowed(&no_blocking, "LPOP"));
}

#[test]
fn test_dangerous_category_end_to_end() {
    let port = free_port();
    start_server(port);
    let mut admin = Conn::connect(port);
    let r = admin.cmds(&[
        "ACL",
        "SETUSER",
        "app",
        "on",
        ">pw",
        "~*",
        "&*",
        "+@all",
        "-@dangerous",
    ]);
    assert!(r.is_ok(), "{r:?}");
    // Every category ACL CAT lists is one ACL CAT can describe.
    let cats = admin.cmds(&["ACL", "CAT"]);
    for cat in cats.as_array() {
        let listed = admin.cmds(&["ACL", "CAT", cat.as_str()]);
        assert!(!listed.is_error(), "ACL CAT {}: {listed:?}", cat.as_str());
        assert!(
            !listed.as_array().is_empty(),
            "ACL CAT {} is empty",
            cat.as_str()
        );
    }

    let mut app = Conn::connect(port);
    assert!(app.cmds(&["AUTH", "app", "pw"]).is_ok());
    assert!(app.cmds(&["SET", "k", "v"]).is_ok());
    assert!(app.cmds(&["CLIENT", "SETNAME", "worker"]).is_ok());
    for denied in [
        vec!["FLUSHALL"],
        vec!["KEYS", "*"],
        vec!["CONFIG", "GET", "maxmemory"],
        vec!["CONFIG", "SET", "maxmemory", "0"],
        vec!["CLIENT", "KILL", "ID", "1"],
        vec!["ACL", "SETUSER", "app", "+@all"],
        vec!["DEBUG", "SLEEP", "0"],
    ] {
        let r = app.cmds(&denied);
        assert!(
            r.is_error() && r.error_str().starts_with("NOPERM"),
            "{denied:?} → {r:?}"
        );
    }
    // The same inside a transaction and from a script.
    assert!(app.cmds(&["MULTI"]).is_ok());
    let queued = app.cmds(&["FLUSHALL"]);
    assert!(queued.is_error(), "{queued:?}");
    let _ = app.cmds(&["DISCARD"]);
    assert_eq!(app.cmds(&["GET", "k"]).as_str(), "v");

    // DRYRUN agrees with what actually happens.
    assert!(
        admin
            .cmds(&["ACL", "DRYRUN", "app", "CLIENT", "SETNAME", "x"])
            .is_ok()
    );
    let dry = admin.cmds(&["ACL", "DRYRUN", "app", "CLIENT", "KILL", "ID", "1"]);
    assert!(dry.as_str().contains("no permissions"), "{dry:?}");
}

// ─────────────────────────────────────────────────────────────────────────────
// ACL file: the default password, key patterns, ACL LOAD, and rewrites
// ─────────────────────────────────────────────────────────────────────────────

fn sha256(password: &str) -> String {
    kividb::acl::sha256_hex(password)
}

/// A password on `default` in the ACL file, with no `--requirepass`,
/// was not asked for: a connection that never sent AUTH was `default`.
#[test]
fn test_default_password_from_acl_file_is_enforced() {
    let port = free_port();
    start_server_with_aclfile(
        port,
        &format!("user default on #{} ~* &* +@all\n", sha256("secret")),
    );

    let mut c = Conn::connect(port);
    for cmd in [
        &["SET", "k", "v"][..],
        &["GET", "k"],
        &["PING"],
        &["ACL", "WHOAMI"],
        &["ACL", "LIST"],
        &["HELLO", "3"],
    ] {
        let reply = c.cmds(cmd);
        assert!(
            reply.is_error() && reply.error_str().starts_with("NOAUTH"),
            "{cmd:?} without AUTH: {reply:?}"
        );
    }

    // A failed AUTH leaves the connection where it was: unauthenticated.
    let reply = c.cmds(&["AUTH", "wrong"]);
    assert!(reply.error_str().starts_with("WRONGPASS"), "{reply:?}");
    assert!(c.cmds(&["GET", "k"]).error_str().starts_with("NOAUTH"));

    assert!(c.cmds(&["AUTH", "secret"]).is_ok());
    assert!(c.cmds(&["SET", "k", "v"]).is_ok());
    assert_eq!(c.cmds(&["ACL", "WHOAMI"]).as_str(), "default");

    // HELLO can carry the AUTH.
    let mut h = Conn::connect(port);
    assert!(
        h.cmds(&["HELLO", "2", "AUTH", "default", "wrong"])
            .is_error()
    );
    assert!(
        !h.cmds(&["HELLO", "2", "AUTH", "default", "secret"])
            .is_error()
    );
    assert_eq!(h.cmds(&["GET", "k"]).as_str(), "v");
}

/// The same when the password is given to `default` on a running server.
#[test]
fn test_default_password_set_at_runtime_is_enforced_for_new_connections() {
    let port = free_port();
    start_server(port);
    let mut admin = Conn::connect(port);
    assert!(admin.cmds(&["ACL", "SETUSER", "default", ">later"]).is_ok());

    let mut c = Conn::connect(port);
    assert!(c.cmds(&["GET", "k"]).error_str().starts_with("NOAUTH"));
    assert!(c.cmds(&["AUTH", "later"]).is_ok());
    assert!(c.cmds(&["GET", "k"]).is_nil());

    // `HELLO … AUTH default <pw>` authenticates as `default`, and as nobody
    // else: it used to accept any user whose password matched.
    assert!(
        admin
            .cmds(&["ACL", "SETUSER", "other", "on", ">otherpw", "~*", "+@all"])
            .is_ok()
    );
    let mut h = Conn::connect(port);
    assert!(
        h.cmds(&["HELLO", "2", "AUTH", "default", "otherpw"])
            .is_error()
    );

    // And off again.
    assert!(admin.cmds(&["ACL", "SETUSER", "default", "nopass"]).is_ok());
    let mut c = Conn::connect(port);
    assert!(c.cmds(&["GET", "k"]).is_nil());
}

/// Only the last key pattern of a user was kept.
#[test]
fn test_a_user_keeps_every_key_pattern() {
    let port = free_port();
    start_server_with_aclfile(
        port,
        &format!(
            "user default on nopass ~* &* +@all\nuser app on #{} ~app:* ~session:* &* +@all\n",
            sha256("apppw")
        ),
    );
    let mut a = Conn::connect(port);
    assert!(a.cmds(&["AUTH", "app", "apppw"]).is_ok());
    assert!(a.cmds(&["SET", "app:x", "1"]).is_ok());
    assert!(a.cmds(&["SET", "session:x", "1"]).is_ok());
    let denied = a.cmds(&["SET", "other:x", "1"]);
    assert_eq!(
        denied.error_str(),
        "NOPERM User app has no permissions to access the 'other:x' key"
    );
    assert!(a.cmds(&["MGET", "app:x", "session:x"]).as_array().len() == 2);
    assert!(a.cmds(&["MGET", "app:x", "other:x"]).is_error());

    let mut c = Conn::connect(port);
    let user = c.cmds(&["ACL", "GETUSER", "app"]);
    let fields = user.as_array();
    assert_eq!(fields[6].as_str(), "keys");
    assert_eq!(fields[7].as_str(), "~app:* ~session:*");
    assert_eq!(fields[8].as_str(), "channels");
    assert_eq!(fields[9].as_str(), "&*");

    // Patterns added one SETUSER at a time accumulate too.
    assert!(c.cmds(&["ACL", "SETUSER", "app", "~cache:*"]).is_ok());
    assert!(a.cmds(&["SET", "cache:x", "1"]).is_ok());
    assert!(a.cmds(&["SET", "app:x", "2"]).is_ok());
    // And `resetkeys` takes them all away: no patterns is no keys.
    assert!(c.cmds(&["ACL", "SETUSER", "app", "resetkeys"]).is_ok());
    assert!(a.cmds(&["GET", "app:x"]).is_error());
    assert!(a.cmds(&["PING"]).as_str() == "PONG");
}

/// `ACL LOAD` added the file's rules to the users in memory: an old
/// password went on working, a removed pattern stayed, a removed user stayed.
#[test]
fn test_acl_load_replaces_the_users() {
    let port = free_port();
    let path = start_server_with_aclfile(
        port,
        &format!(
            "user default on nopass ~* &* +@all\n\
             user app on #{pw1} ~app:* ~session:* &* +@all\n\
             user gone on #{pw1} ~* &* +@all\n",
            pw1 = sha256("pw1")
        ),
    );
    let mut c = Conn::connect(port);
    let mut app = Conn::connect(port);
    assert!(app.cmds(&["AUTH", "app", "pw1"]).is_ok());
    assert!(app.cmds(&["SET", "session:x", "1"]).is_ok());
    // A user made with SETUSER and not saved: not in the file either.
    assert!(
        c.cmds(&["ACL", "SETUSER", "temp", "on", "nopass", "~*", "+@all"])
            .is_ok()
    );

    std::fs::write(
        &path,
        format!(
            "user default on nopass ~* &* +@all\nuser app on #{} ~app:* &* +@all\n",
            sha256("pw2")
        ),
    )
    .unwrap();
    assert!(c.cmds(&["ACL", "LOAD"]).is_ok());

    let users: Vec<String> = c
        .cmds(&["ACL", "USERS"])
        .as_array()
        .iter()
        .map(|u| u.as_str().to_string())
        .collect();
    assert_eq!(users, ["app", "default"]);

    let mut n = Conn::connect(port);
    assert!(
        n.cmds(&["AUTH", "app", "pw1"]).is_error(),
        "the old password must stop working"
    );
    assert!(n.cmds(&["AUTH", "gone", "pw1"]).is_error());
    assert!(n.cmds(&["AUTH", "app", "pw2"]).is_ok());
    assert!(n.cmds(&["SET", "app:x", "1"]).is_ok());
    assert!(n.cmds(&["SET", "session:x", "1"]).is_error());
    // The connection that was already open as `app` loses the pattern too.
    assert!(app.cmds(&["SET", "session:x", "2"]).is_error());
    assert!(app.cmds(&["SET", "app:y", "2"]).is_ok());

    // A file with a mistake in it changes nothing at all.
    std::fs::write(
        &path,
        "user default on nopass ~* &* +@all\nuser added on nopass ~* +@all\nuser bad on +@nosuchcategory\n",
    )
    .unwrap();
    let reply = c.cmds(&["ACL", "LOAD"]);
    assert!(reply.is_error(), "{reply:?}");
    assert!(reply.error_str().contains("line 3"), "{reply:?}");
    let users: Vec<String> = c
        .cmds(&["ACL", "USERS"])
        .as_array()
        .iter()
        .map(|u| u.as_str().to_string())
        .collect();
    assert_eq!(users, ["app", "default"]);
    assert!(n.cmds(&["SET", "app:z", "1"]).is_ok());
}

/// SETUSER and DELUSER rewrote the ACL file, and where it could not
/// be written they returned an error — having already made the change.
#[test]
fn test_setuser_and_deluser_do_not_touch_the_acl_file() {
    let port = free_port();
    let contents = "user default on nopass ~* &* +@all\n";
    let path = start_server_with_aclfile(port, contents);
    let mut c = Conn::connect(port);

    assert!(
        c.cmds(&["ACL", "SETUSER", "probe", "on", ">a", "~*", "+@all"])
            .is_ok()
    );
    assert_eq!(std::fs::read_to_string(&path).unwrap(), contents);
    assert_eq!(c.cmds(&["ACL", "DELUSER", "probe"]).as_int(), 1);
    assert_eq!(std::fs::read_to_string(&path).unwrap(), contents);

    // The file is written by ACL SAVE.
    assert!(
        c.cmds(&[
            "ACL", "SETUSER", "kept", "on", ">a", "~k:*", "~j:*", "&ch", "+get"
        ])
        .is_ok()
    );
    assert!(c.cmds(&["ACL", "SAVE"]).is_ok());
    let saved = std::fs::read_to_string(&path).unwrap();
    assert!(
        saved.contains(&format!(
            "user kept on #{} ~k:* ~j:* &ch -@all +get",
            sha256("a")
        )),
        "{saved}"
    );
    // …and what it wrote, it reads back.
    assert!(c.cmds(&["ACL", "LOAD"]).is_ok());
    let user = c.cmds(&["ACL", "GETUSER", "kept"]);
    assert_eq!(user.as_array()[7].as_str(), "~k:* ~j:*");
}

/// A SETUSER that returns an error has changed nothing, and a rule that
/// means nothing is an error.
#[test]
fn test_setuser_is_all_or_nothing() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let reply = c.cmds(&["ACL", "SETUSER", "new", "on", "nopass", "+@nosuchcategory"]);
    assert!(
        reply
            .error_str()
            .contains("Error in ACL SETUSER modifier '+@nosuchcategory'"),
        "{reply:?}"
    );
    assert!(c.cmds(&["ACL", "GETUSER", "new"]).is_nil());

    assert!(
        c.cmds(&["ACL", "SETUSER", "u", "on", "nopass", "~a:*", "+get"])
            .is_ok()
    );
    assert!(
        c.cmds(&["ACL", "SETUSER", "u", "~b:*", "bogus-rule"])
            .is_error()
    );
    assert_eq!(
        c.cmds(&["ACL", "GETUSER", "u"]).as_array()[7].as_str(),
        "~a:*"
    );

    // Deleting `default` is refused before anything else is deleted.
    assert!(c.cmds(&["ACL", "DELUSER", "u", "default"]).is_error());
    assert!(!c.cmds(&["ACL", "GETUSER", "u"]).is_nil());

    // A selector, written as one argument or as several.
    assert!(c.cmds(&["ACL", "SETUSER", "u", "(~cache:* +set)"]).is_ok());
    assert!(c.cmds(&["ACL", "SETUSER", "u", "(~tmp:*", "+del)"]).is_ok());
    let user = c.cmds(&["ACL", "GETUSER", "u"]);
    assert_eq!(user.as_array()[11].as_array().len(), 2);
    let mut u = Conn::connect(port);
    assert!(u.cmds(&["AUTH", "u", "x"]).is_ok());
    assert!(u.cmds(&["SET", "cache:k", "v"]).is_ok());
    assert!(u.cmds(&["GET", "cache:k"]).is_error());
    assert!(u.cmds(&["GET", "a:k"]).is_nil());
    assert!(u.cmds(&["SET", "a:k", "v"]).is_error());

    // DRYRUN answers for keys as well as for the command.
    assert!(c.cmds(&["ACL", "DRYRUN", "u", "GET", "a:k"]).is_ok());
    assert_eq!(
        c.cmds(&["ACL", "DRYRUN", "u", "GET", "zzz"]).as_str(),
        "User u has no permissions to access the 'zzz' key"
    );
    assert_eq!(
        c.cmds(&["ACL", "DRYRUN", "u", "LPUSH", "a:k", "v"])
            .as_str(),
        "User u has no permissions to run the 'lpush' command"
    );
}

/// Channel patterns were stored and never looked at.
#[test]
fn test_channel_patterns_are_enforced() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    assert!(
        c.cmds(&[
            "ACL", "SETUSER", "pub", "on", "nopass", "~*", "&news.*", "+@all"
        ])
        .is_ok()
    );

    let mut p = Conn::connect(port);
    assert!(p.cmds(&["AUTH", "pub", "x"]).is_ok());
    assert_eq!(p.cmds(&["PUBLISH", "news.uk", "hello"]).as_int(), 0);
    assert_eq!(
        p.cmds(&["PUBLISH", "secret", "hello"]).error_str(),
        "NOPERM User pub has no permissions to access the 'secret' channel"
    );
    assert!(p.cmds(&["SUBSCRIBE", "secret"]).is_error());
    assert!(p.cmds(&["PSUBSCRIBE", "*"]).is_error());
    // Allowed — and once subscribed, a further SUBSCRIBE is checked as well.
    assert!(!p.cmds(&["SUBSCRIBE", "news.uk"]).is_error());
    assert!(p.cmds(&["SUBSCRIBE", "secret"]).is_error());
    assert!(!p.cmds(&["SUBSCRIBE", "news.fr"]).is_error());

    let log = c.cmds(&["ACL", "LOG"]);
    assert!(
        log.as_array().iter().any(|entry| {
            let entry = entry.as_array();
            entry[9].as_str() == "channel" && entry[13].as_str() == "secret"
        }),
        "a refused channel is logged: {log:?}"
    );
}

/// `HELLO` and `AUTH` are not subject to the command rules of the user a
/// connection is before it authenticates. A default user allowed nothing
/// but `AUTH` used to refuse `HELLO 3 AUTH user pass` with NOPERM, so the
/// clients that open a connection with it (ioredis, Ruby's redis-client)
/// could not connect at all.
#[test]
fn test_hello_with_auth_is_not_refused_by_the_default_users_rules() {
    let port = free_port();
    start_server(port);
    let mut admin = Conn::connect(port);
    assert!(
        admin
            .cmds(&["ACL", "SETUSER", "app", "on", ">secret", "~*", "+@all"])
            .is_ok()
    );
    assert!(
        admin
            .cmds(&[
                "ACL", "SETUSER", "default", "on", "nopass", "-@all", "+auth"
            ])
            .is_ok()
    );

    // As the default user: nothing but AUTH and HELLO.
    let mut c = Conn::connect(port);
    let refused = c.cmds(&["GET", "k"]);
    assert!(refused.is_error(), "GET should be refused: {refused:?}");
    assert!(refused.error_str().contains("NOPERM"));
    let hello = c.cmds(&["HELLO", "2"]);
    assert!(!hello.is_error(), "HELLO was refused: {hello:?}");

    // And the AUTH inside a HELLO is what decides who the connection is.
    let hello = c.cmds(&["HELLO", "2", "AUTH", "app", "secret"]);
    assert!(!hello.is_error(), "HELLO ... AUTH was refused: {hello:?}");
    assert_eq!(c.cmds(&["ACL", "WHOAMI"]).as_str(), "app");
    assert!(c.cmds(&["SET", "k", "v"]).is_ok());

    // A wrong password is still a wrong password.
    let mut wrong = Conn::connect(port);
    let hello = wrong.cmds(&["HELLO", "2", "AUTH", "app", "nope"]);
    assert!(hello.is_error(), "a wrong password was accepted: {hello:?}");
    assert!(hello.error_str().contains("WRONGPASS"));
}

/// What a script calls is checked against the ACL of the user running it.
/// A user allowed only EVAL on `app:*` keys could run any command on any key
/// from a script, `ACL SETUSER` on itself included.
#[cfg(feature = "lua")]
#[test]
fn a_script_runs_under_its_users_acl() {
    let port = free_port();
    start_server(port);
    let mut admin = Conn::connect(port);
    assert_eq!(
        admin
            .cmds(&[
                "ACL", "SETUSER", "lim", "on", ">p", "-@all", "+eval", "+set", "~app:*"
            ])
            .as_str(),
        "OK"
    );
    let mut c = Conn::connect(port);
    assert_eq!(c.cmds(&["AUTH", "lim", "p"]).as_str(), "OK");
    let allowed = c.cmds(&[
        "EVAL",
        "return redis.call('SET', KEYS[1], 'x')",
        "1",
        "app:1",
    ]);
    assert_eq!(allowed.as_str(), "OK");
    let other_key = c.cmds(&["EVAL", "return redis.call('SET', 'other', 'x')", "0"]);
    assert!(
        other_key.error_str().contains("NOPERM"),
        "{:?}",
        other_key.error_str()
    );
    let escalate = c.cmds(&[
        "EVAL",
        "return redis.call('ACL', 'SETUSER', 'lim', '+@all', '~*')",
        "0",
    ]);
    assert!(
        escalate.error_str().contains("NOPERM"),
        "{:?}",
        escalate.error_str()
    );
    assert!(matches!(
        admin.cmds(&["EXISTS", "other"]),
        RespValue::Int(0)
    ));
}

/// `ACL HELP` answers, as every other container's `HELP` does; it was
/// unknown, in an error that said to try it. `COMMAND` with a subcommand it
/// does not have is an error, not nil (#134).
#[test]
fn acl_help_and_command_s_unknown_subcommand() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let help = c.cmds(&["ACL", "HELP"]);
    let lines: Vec<&str> = help.as_array().iter().map(|line| line.as_str()).collect();
    assert!(lines[0].starts_with("ACL <subcommand>"), "{lines:?}");
    assert!(lines.contains(&"SETUSER <username> <attribute> [<attribute> ...]"));

    let nope = c.cmds(&["COMMAND", "NOPE"]);
    assert_eq!(
        nope.error_str(),
        "ERR unknown subcommand 'NOPE'. Try COMMAND HELP."
    );
}

/// `ACL GENPASS bits` gives a hex digit per 4 bits, as Redis does: 1 to 4
/// bits gave an empty password (#140).
#[test]
fn acl_genpass_gives_a_digit_per_four_bits() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    for (bits, digits) in [
        (1, 1),
        (4, 1),
        (5, 2),
        (8, 2),
        (9, 3),
        (256, 64),
        (4096, 1024),
    ] {
        let pass = c.cmds(&["ACL", "GENPASS", &bits.to_string()]);
        let pass = pass.as_str();
        assert_eq!(pass.len(), digits, "{bits} bits: {pass:?}");
        assert!(pass.chars().all(|ch| ch.is_ascii_hexdigit()), "{pass:?}");
    }
    assert_eq!(c.cmds(&["ACL", "GENPASS"]).as_str().len(), 64);
}
