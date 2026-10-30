// tests/tls.rs
// Requires --features tls.
// Run with: cargo test --features tls --test tls
//
// These tests generate a self-signed cert + key in a temp dir at runtime
// using rcgen (a pure-Rust X.509 generator already in the ecosystem).
// No openssl CLI dependency.

#![cfg(feature = "tls")]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

// ── Cert generation ───────────────────────────────────────────────────────────
// rcgen must be added to [dev-dependencies]:
//   rcgen = "0.13"

use rcgen::{CertifiedKey, generate_simple_self_signed};

struct TempCerts {
    /// Keeps certificate files on disk until the test finishes.
    #[allow(dead_code)]
    dir: tempfile::TempDir,
    cert_path: PathBuf,
    key_path: PathBuf,
    /// DER bytes of the CA cert for rustls client verification
    cert_der: Vec<u8>,
}

fn generate_certs() -> TempCerts {
    let CertifiedKey { cert, key_pair } =
        generate_simple_self_signed(vec!["127.0.0.1".to_string(), "localhost".to_string()])
            .expect("cert generation failed");

    let dir = tempfile::tempdir().expect("tempdir");
    let cert_path = dir.path().join("server.crt");
    let key_path = dir.path().join("server.key");

    std::fs::write(&cert_path, cert.pem()).unwrap();
    std::fs::write(&key_path, key_pair.serialize_pem()).unwrap();

    let cert_der = cert.der().to_vec();

    TempCerts {
        dir,
        cert_path,
        key_path,
        cert_der,
    }
}

// ── TLS client helper ─────────────────────────────────────────────────────────
// `tokio_rustls::TlsStream` is async-only; wrap it with a mini runtime so tests can use
// `std::io::{Read, Write}` helpers unchanged.

use tokio_rustls::rustls::{self, ClientConfig};

/// Holds a Tokio runtime plus one TLS session — implements blocking `Read`/`Write`.
struct BlockingTls {
    rt: tokio::runtime::Runtime,
    stream: tokio_rustls::client::TlsStream<tokio::net::TcpStream>,
}

impl Read for BlockingTls {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.rt.block_on(async { self.stream.read(buf).await })
    }
}

impl Write for BlockingTls {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.rt.block_on(async { self.stream.write(buf).await })
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.rt.block_on(async { self.stream.flush().await })
    }
}

fn make_tls_client(port: u16, cert_der: &[u8]) -> BlockingTls {
    try_tls_client(port, cert_der).expect("tls handshake")
}

/// A TLS client, or `None` if its handshake fails.
fn try_tls_client(port: u16, cert_der: &[u8]) -> Option<BlockingTls> {
    let mut root_store = rustls::RootCertStore::empty();
    root_store
        .add(rustls::pki_types::CertificateDer::from(cert_der.to_vec()))
        .expect("add cert to root store");

    let config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    let stream = rt.block_on(async move {
        let stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("tcp connect");
        let domain = rustls::pki_types::ServerName::try_from("127.0.0.1").expect("server name");
        connector.connect(domain, stream).await.ok()
    })?;

    Some(BlockingTls { rt, stream })
}

// ── Minimal blocking RESP helpers ─────────────────────────────────────────────

fn send_cmd(stream: &mut impl Write, args: &[&str]) {
    let mut buf = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        buf.extend_from_slice(arg.as_bytes());
        buf.extend_from_slice(b"\r\n");
    }
    stream.write_all(&buf).unwrap();
}

fn read_line(stream: &mut impl Read) -> String {
    let mut line = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        stream.read_exact(&mut byte).unwrap();
        if byte[0] == b'\n' {
            break;
        }
        if byte[0] != b'\r' {
            line.push(byte[0]);
        }
    }
    String::from_utf8(line).unwrap()
}

fn read_resp(stream: &mut impl Read) -> String {
    let line = read_line(stream);
    match line.as_bytes().first() {
        Some(b'+') => line[1..].to_string(),
        Some(b'-') => line[1..].to_string(),
        Some(b':') => line[1..].to_string(),
        Some(b'$') => {
            let len: i64 = line[1..].parse().unwrap();
            if len == -1 {
                return "(nil)".to_string();
            }
            let mut buf = vec![0u8; len as usize + 2];
            stream.read_exact(&mut buf).unwrap();
            buf.truncate(len as usize);
            String::from_utf8(buf).unwrap()
        }
        _ => line,
    }
}

// ── Server startup ────────────────────────────────────────────────────────────

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

fn start_server_with_tls(plain_port: u16, tls_port: u16, certs: &TempCerts) {
    let cert_path = certs.cert_path.to_str().unwrap().to_string();
    let key_path = certs.key_path.to_str().unwrap().to_string();

    thread::spawn(move || {
        let cfg = kividb::config::Config {
            port: plain_port,
            bind: "127.0.0.1".to_string(),
            tls_port,
            tls_cert_file: cert_path,
            tls_key_file: key_path,
            ..Default::default()
        };
        kividb::run_for_test(cfg);
    });

    // Wait for both ports to be ready
    for port in [plain_port, tls_port] {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if TcpStream::connect(("127.0.0.1", port)).is_ok() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "server port {port} never opened"
            );
            thread::sleep(Duration::from_millis(50));
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Basic TLS: PING over TLS returns PONG.
#[test]
fn test_tls_ping() {
    let certs = generate_certs();
    let plain = free_port();
    let tls = free_port();
    start_server_with_tls(plain, tls, &certs);

    let mut conn = make_tls_client(tls, &certs.cert_der);
    send_cmd(&mut conn, &["PING"]);
    let resp = read_resp(&mut conn);
    assert_eq!(resp, "PONG");
}

/// SET and GET over TLS produce the expected value.
#[test]
fn test_tls_set_get() {
    let certs = generate_certs();
    let plain = free_port();
    let tls = free_port();
    start_server_with_tls(plain, tls, &certs);

    let mut conn = make_tls_client(tls, &certs.cert_der);

    send_cmd(&mut conn, &["SET", "tls_key", "tls_value"]);
    assert_eq!(read_resp(&mut conn), "OK");

    send_cmd(&mut conn, &["GET", "tls_key"]);
    assert_eq!(read_resp(&mut conn), "tls_value");
}

/// Plain port still works when TLS port is also active.
/// This validates that the TLS listener spawning does not break the plain path.
#[test]
fn test_plain_port_works_alongside_tls() {
    let certs = generate_certs();
    let plain = free_port();
    let tls = free_port();
    start_server_with_tls(plain, tls, &certs);

    // Plain connection
    let mut plain_conn = TcpStream::connect(("127.0.0.1", plain)).unwrap();
    plain_conn
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();

    send_cmd(&mut plain_conn, &["PING"]);
    assert_eq!(read_resp(&mut plain_conn), "PONG");
}

/// Data written on the plain port is readable on the TLS port.
/// Both paths share the same store — this validates that TLS connections
/// are not isolated to a separate data namespace.
#[test]
fn test_plain_and_tls_share_store() {
    let certs = generate_certs();
    let plain = free_port();
    let tls = free_port();
    start_server_with_tls(plain, tls, &certs);

    // Write via plain
    let mut plain_conn = TcpStream::connect(("127.0.0.1", plain)).unwrap();
    plain_conn
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    send_cmd(&mut plain_conn, &["SET", "shared_key", "written_plain"]);
    assert_eq!(read_resp(&mut plain_conn), "OK");

    // Read via TLS
    let mut tls_conn = make_tls_client(tls, &certs.cert_der);
    send_cmd(&mut tls_conn, &["GET", "shared_key"]);
    assert_eq!(read_resp(&mut tls_conn), "written_plain");
}

/// Connecting to the TLS port without TLS fails — the server closes the
/// connection or returns garbage. The plain TcpStream should not get a
/// valid RESP response.
#[test]
fn test_plain_connection_to_tls_port_fails() {
    let certs = generate_certs();
    let plain = free_port();
    let tls = free_port();
    start_server_with_tls(plain, tls, &certs);

    let mut raw = TcpStream::connect(("127.0.0.1", tls)).unwrap();
    raw.set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();

    // Send a plain PING — the TLS server will see garbage (not a TLS ClientHello)
    // and close the connection or return an error record.
    send_cmd(&mut raw, &["PING"]);

    let mut buf = [0u8; 64];
    let result = raw.read(&mut buf);

    // Either the connection was closed (Ok(0)) or we got non-RESP bytes back.
    // A valid RESP +PONG\r\n starts with '+' — if we get that, the test fails.
    match result {
        Ok(0) => {} // connection closed — correct
        Ok(n) => {
            assert_ne!(
                buf[0],
                b'+',
                "plain connection to TLS port should not return valid RESP, got: {:?}",
                &buf[..n]
            );
        }
        Err(_) => {} // read error — also correct
    }
}

/// TLS connection survives pipelined commands.
/// Validates that the TLS stream framing doesn't break on batched RESP.
#[test]
fn test_tls_pipelined_commands() {
    let certs = generate_certs();
    let plain = free_port();
    let tls = free_port();
    start_server_with_tls(plain, tls, &certs);

    let mut conn = make_tls_client(tls, &certs.cert_der);

    // Send 5 commands without reading between them
    for i in 0..5u8 {
        send_cmd(
            &mut conn,
            &["SET", &format!("pipe_key_{i}"), &format!("val_{i}")],
        );
    }

    // Read all 5 responses
    for _ in 0..5 {
        assert_eq!(read_resp(&mut conn), "OK");
    }

    // Verify all keys
    for i in 0..5u8 {
        send_cmd(&mut conn, &["GET", &format!("pipe_key_{i}")]);
        assert_eq!(read_resp(&mut conn), format!("val_{i}"));
    }
}

/// Multiple concurrent TLS connections each see an isolated selected_db
/// and shared key space.
#[test]
fn test_tls_concurrent_connections() {
    let certs = generate_certs();
    let plain = free_port();
    let tls = free_port();
    start_server_with_tls(plain, tls, &certs);

    let cert_der = certs.cert_der.clone();
    let cert_der2 = certs.cert_der.clone();

    let h1 = thread::spawn(move || {
        let mut conn = make_tls_client(tls, &cert_der);
        for i in 0..50u32 {
            send_cmd(&mut conn, &["SET", &format!("c1_key_{i}"), "c1"]);
            assert_eq!(read_resp(&mut conn), "OK");
        }
    });

    let h2 = thread::spawn(move || {
        let mut conn = make_tls_client(tls, &cert_der2);
        for i in 0..50u32 {
            send_cmd(&mut conn, &["SET", &format!("c2_key_{i}"), "c2"]);
            assert_eq!(read_resp(&mut conn), "OK");
        }
    });

    h1.join().unwrap();
    h2.join().unwrap();

    // Spot-check via plain
    let mut plain_conn = TcpStream::connect(("127.0.0.1", plain)).unwrap();
    plain_conn
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    send_cmd(&mut plain_conn, &["GET", "c1_key_0"]);
    assert_eq!(read_resp(&mut plain_conn), "c1");
    send_cmd(&mut plain_conn, &["GET", "c2_key_0"]);
    assert_eq!(read_resp(&mut plain_conn), "c2");
}

/// INFO server section reports tls_port correctly when TLS is active.
#[test]
fn test_tls_info_section() {
    let certs = generate_certs();
    let plain = free_port();
    let tls = free_port();
    start_server_with_tls(plain, tls, &certs);

    let mut conn = make_tls_client(tls, &certs.cert_der);
    send_cmd(&mut conn, &["INFO", "tls"]);

    // Read bulk response
    let line = read_line(&mut conn);
    assert!(line.starts_with('$'), "expected bulk, got: {line}");
    let len: usize = line[1..].parse().unwrap();
    let mut body = vec![0u8; len + 2];
    conn.read_exact(&mut body).unwrap();
    let info = String::from_utf8(body[..len].to_vec()).unwrap();

    assert!(
        info.contains("tls_enabled:1"),
        "tls_enabled should be 1, got:\n{info}"
    );
    assert!(
        info.contains(&format!("tls_port:{tls}")),
        "tls_port should be {tls}, got:\n{info}"
    );
}

// ── Client certificates (tls-ca-cert-file + tls-auth-clients) ────────────────

use rcgen::{BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair};
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

/// A CA, a server certificate it signs and the CA's key, so it can sign
/// client certificates too.
struct Pki {
    #[allow(dead_code)]
    dir: tempfile::TempDir,
    ca_path: PathBuf,
    cert_path: PathBuf,
    key_path: PathBuf,
    ca_der: Vec<u8>,
    ca_cert: rcgen::Certificate,
    ca_key: KeyPair,
}

fn new_ca(name: &str) -> (rcgen::Certificate, KeyPair) {
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.distinguished_name.push(DnType::CommonName, name);
    let key = KeyPair::generate().unwrap();
    let cert = params.self_signed(&key).unwrap();
    (cert, key)
}

fn generate_pki() -> Pki {
    let (ca_cert, ca_key) = new_ca("kividb test ca");
    let server_key = KeyPair::generate().unwrap();
    let server_cert =
        CertificateParams::new(vec!["127.0.0.1".to_string(), "localhost".to_string()])
            .unwrap()
            .signed_by(&server_key, &ca_cert, &ca_key)
            .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let ca_path = dir.path().join("ca.crt");
    let cert_path = dir.path().join("server.crt");
    let key_path = dir.path().join("server.key");
    std::fs::write(&ca_path, ca_cert.pem()).unwrap();
    std::fs::write(&cert_path, server_cert.pem()).unwrap();
    std::fs::write(&key_path, server_key.serialize_pem()).unwrap();

    Pki {
        dir,
        ca_path,
        cert_path,
        key_path,
        ca_der: ca_cert.der().to_vec(),
        ca_cert,
        ca_key,
    }
}

/// A client certificate signed by `ca`.
fn client_cert(ca: &rcgen::Certificate, ca_key: &KeyPair) -> (Vec<u8>, Vec<u8>) {
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.distinguished_name.push(DnType::CommonName, "client");
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let key = KeyPair::generate().unwrap();
    let cert = params.signed_by(&key, ca, ca_key).unwrap();
    (cert.der().to_vec(), key.serialize_der())
}

fn start_server_with_client_auth(pki: &Pki, ca: bool, auth_clients: &str) -> u16 {
    let plain = free_port();
    let tls = free_port();
    let cfg = kividb::config::Config {
        port: plain,
        bind: "127.0.0.1".to_string(),
        tls_port: tls,
        tls_cert_file: pki.cert_path.to_str().unwrap().to_string(),
        tls_key_file: pki.key_path.to_str().unwrap().to_string(),
        tls_ca_cert_file: if ca {
            pki.ca_path.to_str().unwrap().to_string()
        } else {
            String::new()
        },
        tls_auth_clients: auth_clients.to_string(),
        ..Default::default()
    };
    thread::spawn(move || kividb::run_for_test(cfg));
    for port in [plain, tls] {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while TcpStream::connect(("127.0.0.1", port)).is_err() {
            assert!(
                std::time::Instant::now() < deadline,
                "server port {port} never opened"
            );
            thread::sleep(Duration::from_millis(50));
        }
    }
    tls
}

/// PING over TLS, optionally presenting a client certificate. Any failure —
/// in the handshake, or the server closing the connection after it (how a
/// refused client certificate shows under TLS 1.3) — is an `Err`.
fn tls_ping(
    port: u16,
    ca_der: &[u8],
    client: Option<(Vec<u8>, Vec<u8>)>,
) -> Result<String, String> {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(CertificateDer::from(ca_der.to_vec())).unwrap();
    let builder = ClientConfig::builder().with_root_certificates(roots);
    let config = match client {
        Some((cert, key)) => builder
            .with_client_auth_cert(
                vec![CertificateDer::from(cert)],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key)),
            )
            .unwrap(),
        None => builder.with_no_client_auth(),
    };
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        tokio::time::timeout(Duration::from_secs(5), async move {
            let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .map_err(|e| format!("connect: {e}"))?;
            let name = rustls::pki_types::ServerName::try_from("127.0.0.1").unwrap();
            let mut tls = connector
                .connect(name, tcp)
                .await
                .map_err(|e| format!("handshake: {e}"))?;
            tls.write_all(b"*1\r\n$4\r\nPING\r\n")
                .await
                .map_err(|e| format!("write: {e}"))?;
            let mut buf = [0u8; 64];
            let n = tls.read(&mut buf).await.map_err(|e| format!("read: {e}"))?;
            if n == 0 {
                return Err("connection closed".to_string());
            }
            Ok(String::from_utf8_lossy(&buf[..n]).trim_end().to_string())
        })
        .await
        .map_err(|_| "timed out".to_string())?
    })
}

/// With a CA file and the default `tls-auth-clients yes`, a client must
/// present a certificate the CA signed. 1.0.5 accepted the CA file and never
/// asked for a certificate.
#[test]
fn test_client_cert_required_by_default_with_ca_file() {
    let pki = generate_pki();
    let port = start_server_with_client_auth(&pki, true, "yes");

    let good = client_cert(&pki.ca_cert, &pki.ca_key);
    assert_eq!(
        tls_ping(port, &pki.ca_der, Some(good)).as_deref(),
        Ok("+PONG")
    );

    let none = tls_ping(port, &pki.ca_der, None);
    assert!(
        none.is_err(),
        "client without a certificate was accepted: {none:?}"
    );

    let (other_ca, other_key) = new_ca("some other ca");
    let foreign = tls_ping(port, &pki.ca_der, Some(client_cert(&other_ca, &other_key)));
    assert!(
        foreign.is_err(),
        "certificate from another CA was accepted: {foreign:?}"
    );
}

/// `optional`: no certificate is fine, an invalid one is not.
#[test]
fn test_client_cert_optional() {
    let pki = generate_pki();
    let port = start_server_with_client_auth(&pki, true, "optional");

    assert_eq!(tls_ping(port, &pki.ca_der, None).as_deref(), Ok("+PONG"));

    let good = client_cert(&pki.ca_cert, &pki.ca_key);
    assert_eq!(
        tls_ping(port, &pki.ca_der, Some(good)).as_deref(),
        Ok("+PONG")
    );

    let (other_ca, other_key) = new_ca("some other ca");
    let foreign = tls_ping(port, &pki.ca_der, Some(client_cert(&other_ca, &other_key)));
    assert!(
        foreign.is_err(),
        "certificate from another CA was accepted: {foreign:?}"
    );
}

/// `no`: clients are not asked for a certificate even with a CA file.
#[test]
fn test_client_cert_not_requested_with_auth_clients_no() {
    let pki = generate_pki();
    let port = start_server_with_client_auth(&pki, true, "no");

    assert_eq!(tls_ping(port, &pki.ca_der, None).as_deref(), Ok("+PONG"));
}

/// Without a CA file nothing changes: clients are not asked for a certificate,
/// whatever `tls-auth-clients` says.
#[test]
fn test_no_ca_file_means_no_client_cert() {
    let pki = generate_pki();
    let port = start_server_with_client_auth(&pki, false, "yes");

    assert_eq!(tls_ping(port, &pki.ca_der, None).as_deref(), Ok("+PONG"));
}

/// Whether a TLS client connects to `port` and is answered, within 5 s.
fn handshake_ok(port: u16, cert_der: &[u8]) -> bool {
    tls_ping(port, cert_der, None).as_deref() == Ok("+PONG")
}

/// Wait until something listens on `port`.
fn wait_open(port: u16) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(
            std::time::Instant::now() < deadline,
            "server port {port} never opened"
        );
        thread::sleep(Duration::from_millis(50));
    }
}

/// A kividb binary, killed when dropped.
struct Killed(std::process::Child);

impl Drop for Killed {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// The kividb binary with TLS, with its default I/O model (`None`: io_uring
/// on Linux) or `io_model`, and `conf` as its config file.
fn start_binary_with_tls(
    certs: &TempCerts,
    io_model: Option<&str>,
    conf: &str,
) -> (Killed, u16, u16, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("kividb.conf");
    std::fs::write(&file, conf).unwrap();
    let plain = free_port();
    let tls = free_port();
    let mut args = vec![
        "--port".to_string(),
        plain.to_string(),
        "--bind".into(),
        "127.0.0.1".into(),
        "--dir".into(),
        dir.path().to_str().unwrap().into(),
        "--configfile".into(),
        file.to_str().unwrap().into(),
        "--tls-port".into(),
        tls.to_string(),
        "--tls-cert-file".into(),
        certs.cert_path.to_str().unwrap().into(),
        "--tls-key-file".into(),
        certs.key_path.to_str().unwrap().into(),
    ];
    if let Some(model) = io_model {
        args.extend(["--io-model".into(), model.into()]);
    }
    let server = Killed(
        std::process::Command::new(env!("CARGO_BIN_EXE_kividb"))
            .args(&args)
            .current_dir(dir.path())
            .stdout(std::process::Stdio::null())
            .spawn()
            .expect("failed to start kividb"),
    );
    wait_open(plain);
    wait_open(tls);
    (server, plain, tls, dir)
}

/// A client that never finishes its handshake holds up no one else, and is
/// dropped after the handshake timeout (10 s).
#[test]
fn a_stalled_handshake_does_not_block_other_clients() {
    let certs = generate_certs();
    let plain = free_port();
    let tls = free_port();
    start_server_with_tls(plain, tls, &certs);

    let mut idle = TcpStream::connect(("127.0.0.1", tls)).unwrap();
    // A failed handshake, before and while the other is pending.
    let mut garbage = TcpStream::connect(("127.0.0.1", tls)).unwrap();
    send_cmd(&mut garbage, &["PING"]);
    drop(garbage);

    assert!(handshake_ok(tls, &certs.cert_der));
    assert!(handshake_ok(tls, &certs.cert_der));

    idle.set_read_timeout(Some(Duration::from_secs(15)))
        .unwrap();
    let mut buf = [0u8; 1];
    match idle.read(&mut buf) {
        Ok(0) => {}
        Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => {}
        other => panic!("the stalled handshake was not dropped: {other:?}"),
    }
}

/// As above, in the server binary: with its default I/O model, io_uring on
/// Linux, and with tokio.
#[test]
fn a_stalled_handshake_does_not_block_the_binary() {
    let certs = generate_certs();
    for io_model in [None, Some("tokio")] {
        let (_server, _plain, tls, _dir) = start_binary_with_tls(&certs, io_model, "");
        let _idle = TcpStream::connect(("127.0.0.1", tls)).unwrap();
        assert!(
            handshake_ok(tls, &certs.cert_der),
            "a TLS client waited on an idle one (io model {io_model:?})"
        );
    }
}

/// `maxclients` counts TLS clients still in their handshake, and connected
/// ones, plaintext or TLS. A place is given back when its connection ends:
/// closed, a failed handshake, or one that ran out of time. In the binary,
/// with its default I/O model, io_uring on Linux, and with tokio.
#[test]
fn maxclients_counts_pending_handshakes() {
    /// What `f` gives, once it gives something: a connection just closed
    /// gives its place back a moment later.
    fn until<T>(mut f: impl FnMut() -> Option<T>) -> T {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(v) = f() {
                return v;
            }
            assert!(std::time::Instant::now() < deadline, "no place was free");
            thread::sleep(Duration::from_millis(50));
        }
    }
    fn plain_client(port: u16) -> Option<TcpStream> {
        let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        send_cmd(&mut c, &["PING"]);
        let mut reply = [0u8; 7];
        (c.read_exact(&mut reply).is_ok() && &reply == b"+PONG\r\n").then_some(c)
    }
    fn turned_away(port: u16) -> bool {
        let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let mut reply = String::new();
        c.read_to_string(&mut reply).is_ok() && reply == "-ERR max number of clients reached\r\n"
    }

    let certs = generate_certs();
    for io_model in [None, Some("tokio")] {
        let (_server, plain, tls, _dir) = start_binary_with_tls(&certs, io_model, "maxclients 3\n");
        let model = format!("io model {io_model:?}");
        // `wait_open` took places that are given back once the server has
        // seen those connections close.
        let _first = until(|| plain_client(plain));
        drop(_first);

        // One place held by a stalled handshake leaves two for others.
        let mut stalled = TcpStream::connect(("127.0.0.1", tls)).unwrap();
        let mut connected = until(|| try_tls_client(tls, &certs.cert_der));
        send_cmd(&mut connected, &["PING"]);
        assert_eq!(read_resp(&mut connected), "PONG", "{model}");
        let closing = until(|| plain_client(plain));

        // Full.
        assert!(turned_away(plain), "{model}");
        assert!(!handshake_ok(tls, &certs.cert_der), "{model}");
        send_cmd(&mut connected, &["PING"]);
        assert_eq!(read_resp(&mut connected), "PONG", "{model}");

        // A client that closes gives its place back.
        drop(closing);
        let failing = until(|| plain_client(plain));
        assert!(turned_away(plain), "{model}");

        // So does a failed handshake.
        drop(failing);
        for _ in 0..3 {
            let mut garbage = TcpStream::connect(("127.0.0.1", tls)).unwrap();
            garbage
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            send_cmd(&mut garbage, &["PING"]);
            let _ = garbage.read_to_end(&mut Vec::new());
        }
        let last = until(|| plain_client(plain));
        assert!(turned_away(plain), "{model}");

        // And a handshake that runs out of time.
        stalled
            .set_read_timeout(Some(Duration::from_secs(15)))
            .unwrap();
        match stalled.read(&mut [0u8; 1]) {
            Ok(0) => {}
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => {}
            other => panic!("the stalled handshake was not dropped ({model}): {other:?}"),
        }
        let _more = until(|| plain_client(plain));
        assert!(turned_away(plain), "{model}");
        drop((connected, last));
    }
}
