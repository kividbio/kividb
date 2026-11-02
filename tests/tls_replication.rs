// tests/tls_replication.rs
// Requires --features tls.
//
// A replica's link to its master over TLS (`tls-replication`). The TLS a
// node connects to others with is the process's, not a server's, so these
// tests have a file of their own, and all of them use one CA and one
// certificate: whichever test sets it up last, the others are served alike.

#![cfg(feature = "tls")]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::OnceLock;
use std::thread;
use std::time::{Duration, Instant};

use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair};

/// The CA and the certificate every server here presents.
struct Pki {
    #[allow(dead_code)]
    dir: tempfile::TempDir,
    ca: String,
    cert: String,
    key: String,
}

fn pki() -> &'static Pki {
    static PKI: OnceLock<Pki> = OnceLock::new();
    PKI.get_or_init(|| {
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(DnType::CommonName, "kividb test ca");
        let ca_key = KeyPair::generate().unwrap();
        let ca = params.self_signed(&ca_key).unwrap();
        let key = KeyPair::generate().unwrap();
        let cert = CertificateParams::new(vec!["127.0.0.1".to_string(), "localhost".to_string()])
            .unwrap()
            .signed_by(&key, &ca, &ca_key)
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = |name: &str| dir.path().join(name).to_string_lossy().into_owned();
        std::fs::write(path("ca.crt"), ca.pem()).unwrap();
        std::fs::write(path("node.crt"), cert.pem()).unwrap();
        std::fs::write(path("node.key"), key.serialize_pem()).unwrap();
        Pki {
            ca: path("ca.crt"),
            cert: path("node.crt"),
            key: path("node.key"),
            dir,
        }
    })
}

/// Whether nothing has `port`; see tests/tls.rs.
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
/// connections; see tests/tls.rs.
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

fn wait_open(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(Instant::now() < deadline, "port {port} never opened");
        thread::sleep(Duration::from_millis(50));
    }
}

fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(100));
    }
}

/// A server with TLS from `pki()`, client certificates required.
fn tls_config(port: u16, tls_port: u16) -> kividb::config::Config {
    let pki = pki();
    kividb::config::Config {
        port,
        bind: "127.0.0.1".to_string(),
        tls_port,
        tls_cert_file: pki.cert.clone(),
        tls_key_file: pki.key.clone(),
        tls_ca_cert_file: pki.ca.clone(),
        ..Default::default()
    }
}

fn start(cfg: kividb::config::Config) {
    let ports = [cfg.port, cfg.tls_port];
    thread::spawn(move || kividb::run_for_test(cfg));
    for port in ports.into_iter().filter(|&port| port != 0) {
        wait_open(port);
    }
}

// ── A RESP client, plain or TLS ───────────────────────────────────────────────

use tokio_rustls::rustls;

type TlsStream = rustls::StreamOwned<rustls::ClientConnection, TcpStream>;

enum Conn {
    Plain(TcpStream),
    Tls(Box<TlsStream>),
}

impl Read for Conn {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Conn::Plain(s) => s.read(buf),
            Conn::Tls(s) => s.read(buf),
        }
    }
}

impl Write for Conn {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Conn::Plain(s) => s.write(buf),
            Conn::Tls(s) => s.write(buf),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Conn::Plain(s) => s.flush(),
            Conn::Tls(s) => s.flush(),
        }
    }
}

fn plain(port: u16) -> Conn {
    let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    Conn::Plain(s)
}

/// A TLS connection that trusts `pki()`'s CA and presents its certificate.
fn tls(port: u16) -> Conn {
    use rustls::pki_types::ServerName;
    let pki = pki();
    let pem = |path: &str| std::io::BufReader::new(std::fs::File::open(path).unwrap());
    let mut roots = rustls::RootCertStore::empty();
    for ca in rustls_pemfile::certs(&mut pem(&pki.ca)) {
        roots.add(ca.unwrap()).unwrap();
    }
    let chain: Vec<_> = rustls_pemfile::certs(&mut pem(&pki.cert))
        .map(Result::unwrap)
        .collect();
    let key = rustls_pemfile::private_key(&mut pem(&pki.key))
        .unwrap()
        .unwrap();
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(chain, key)
        .unwrap();
    let name = ServerName::try_from("127.0.0.1").unwrap();
    let conn = rustls::ClientConnection::new(std::sync::Arc::new(config), name).unwrap();
    let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    Conn::Tls(Box::new(rustls::StreamOwned::new(conn, s)))
}

fn read_line(conn: &mut Conn) -> String {
    let mut line = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        conn.read_exact(&mut byte).unwrap();
        match byte[0] {
            b'\n' => break,
            b'\r' => {}
            b => line.push(b),
        }
    }
    String::from_utf8(line).unwrap()
}

/// A command's reply as text: an array's items joined by spaces.
fn cmd(conn: &mut Conn, args: &[&str]) -> String {
    let mut buf = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        buf.extend_from_slice(format!("${}\r\n{arg}\r\n", arg.len()).as_bytes());
    }
    conn.write_all(&buf).unwrap();
    conn.flush().unwrap();
    reply(conn)
}

fn reply(conn: &mut Conn) -> String {
    let line = read_line(conn);
    match line.as_bytes().first() {
        Some(b'$') => {
            let len: i64 = line[1..].parse().unwrap();
            if len < 0 {
                return "(nil)".to_string();
            }
            let mut body = vec![0u8; len as usize + 2];
            conn.read_exact(&mut body).unwrap();
            body.truncate(len as usize);
            String::from_utf8(body).unwrap()
        }
        Some(b'*') => {
            let n: i64 = line[1..].parse().unwrap();
            (0..n.max(0))
                .map(|_| reply(conn))
                .collect::<Vec<_>>()
                .join(" ")
        }
        _ => line[1..].to_string(),
    }
}

fn info_field(conn: &mut Conn, section: &str, field: &str) -> Option<String> {
    cmd(conn, &["INFO", section])
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{field}:")).map(str::to_string))
}

/// One test at a time: each starts with TLS between nodes off, and turns it
/// off again when it is done.
fn serial() -> std::sync::MutexGuard<'static, ()> {
    static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// `CONFIG SET tls-replication yes` on a replica that started without TLS:
/// its next connection to its master is over TLS, and it syncs.
#[test]
fn tls_replication_is_turned_on_at_runtime() {
    let _serial = serial();
    let pki = pki();
    let (master_port, master_tls) = (free_port(), free_port());
    start(tls_config(master_port, master_tls));
    let replica_port = free_port();
    start(kividb::config::Config {
        port: replica_port,
        bind: "127.0.0.1".to_string(),
        ..Default::default()
    });
    let mut m = plain(master_port);
    assert_eq!(cmd(&mut m, &["SET", "k", "v"]), "OK");

    let mut r = plain(replica_port);
    assert_eq!(
        cmd(&mut r, &["CONFIG", "GET", "tls-replication"]),
        "tls-replication no"
    );
    // Without the files it would connect with, refused.
    let refused = cmd(&mut r, &["CONFIG", "SET", "tls-replication", "yes"]);
    assert!(
        refused.contains("Unable to update TLS configuration"),
        "{refused}"
    );
    assert_eq!(
        cmd(
            &mut r,
            &[
                "CONFIG",
                "SET",
                "tls-cert-file",
                &pki.cert,
                "tls-key-file",
                &pki.key,
                "tls-ca-cert-file",
                &pki.ca,
                "tls-replication",
                "yes",
            ],
        ),
        "OK"
    );
    assert_eq!(
        cmd(&mut r, &["CONFIG", "GET", "tls-replication"]),
        "tls-replication yes"
    );
    assert_eq!(
        cmd(&mut r, &["REPLICAOF", "127.0.0.1", &master_tls.to_string()]),
        "OK"
    );
    wait_until("the replica to sync over TLS", || {
        info_field(&mut r, "replication", "master_link_status").as_deref() == Some("up")
    });
    wait_until("the key to reach the replica", || {
        cmd(&mut r, &["GET", "k"]) == "v"
    });
    assert_eq!(cmd(&mut tls(master_tls), &["PING"]), "PONG");

    // Off again: the link that is up stays up.
    assert_eq!(
        cmd(&mut r, &["CONFIG", "SET", "tls-replication", "no"]),
        "OK"
    );
    assert_eq!(cmd(&mut m, &["SET", "k", "w"]), "OK");
    wait_until("the replica to go on syncing", || {
        cmd(&mut r, &["GET", "k"]) == "w"
    });
}

/// A master and a replica with no plaintext port (port 0): the replica
/// connects over TLS, and tells its master its TLS port as the one to
/// reach it on.
#[test]
fn a_tls_only_replica_replicates_a_tls_only_master() {
    let _serial = serial();
    let master_tls = free_port();
    // The replica is a client of its master: its certificate's names are
    // checked as any client's.
    let master = kividb::config::Config {
        tls_auth_clients_allowed_names: "localhost".to_string(),
        ..tls_config(0, master_tls)
    };
    assert!(master.validate(true).is_ok());
    start(master);
    let replica_tls = free_port();
    let replica = kividb::config::Config {
        tls_replication: true,
        replicaof: Some(vec!["127.0.0.1".to_string(), master_tls.to_string()]),
        ..tls_config(0, replica_tls)
    };
    assert!(replica.validate(true).is_ok());
    start(replica);

    let mut m = tls(master_tls);
    assert_eq!(cmd(&mut m, &["SET", "only", "tls"]), "OK");
    let mut r = tls(replica_tls);
    wait_until("the replica to sync over TLS", || {
        info_field(&mut r, "replication", "master_link_status").as_deref() == Some("up")
    });
    wait_until("the key to reach the replica", || {
        cmd(&mut r, &["GET", "only"]) == "tls"
    });
    let slave = info_field(&mut m, "replication", "slave0").unwrap_or_default();
    assert!(slave.contains(&format!("port={replica_tls},")), "{slave}");
    assert_eq!(
        info_field(&mut r, "server", "tcp_port").as_deref(),
        Some("0")
    );

    // TLS between nodes off again, for the other tests here.
    assert_eq!(
        cmd(&mut r, &["CONFIG", "SET", "tls-replication", "no"]),
        "OK"
    );
}
