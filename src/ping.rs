// src/ping.rs
//
// `kividb --ping`: a health check that needs nothing but the binary (#433).
//
// The images are distroless, with no shell and no `redis-cli`, so a Compose
// file or a platform template had no way to ask the server whether it is up.
// `["CMD", "kividb", "--ping"]` runs in exec form: it connects to the server
// the command line and config file describe, sends PING (after AUTH when
// there is a password), and exits 0 on PONG or 1 on anything else, all
// within about a second.
//
// It is a client and nothing more. It reads the command line and the config
// file as the server does, then returns before anything a server does: no
// listener, no log file, no `dir`, no data directory lock, no update check.
// Health checks run every few seconds, so it uses plain blocking sockets and
// no runtime.

use crate::config::Config;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

/// How long the whole check may take, from the first connect to PONG.
const TIMEOUT: Duration = Duration::from_secs(1);

/// The password, read before `requirepass`, so that it need not be on the
/// command line (where `ps` and `docker inspect` show it). In a container
/// the health check runs with the container's environment.
pub const PASSWORD_ENV: &str = "KIVIDB_PASSWORD";

/// The ACL user to authenticate as, with `KIVIDB_PASSWORD`: `AUTH <user>
/// <password>`. Unset, the check authenticates as the default user.
pub const USER_ENV: &str = "KIVIDB_USER";

/// Run the check, and return the exit code: 0 on PONG, 1 otherwise. Says
/// why on standard error when it fails, which Docker keeps in the
/// container's health log.
pub fn run(config: &Config) -> i32 {
    match ping(config) {
        Ok(()) => {
            println!("PONG");
            0
        }
        Err(e) => {
            eprintln!("kividb --ping: {e}");
            1
        }
    }
}

/// The port to connect to, and whether to speak TLS on it: `--ping-port`,
/// else the port the server takes clients on (`port`, or `tls-port` when
/// `port` is 0). The TLS port is spoken to over TLS.
fn target(config: &Config) -> (u16, bool) {
    let port = if config.ping_port != 0 {
        config.ping_port
    } else {
        config.client_port()
    };
    (port, config.tls_port != 0 && port == config.tls_port)
}

/// The `AUTH` arguments to send first, if any, given what the environment
/// holds for `KIVIDB_USER` and `KIVIDB_PASSWORD` (unset and empty are the
/// same): the user and `KIVIDB_PASSWORD`, else `KIVIDB_PASSWORD` or
/// `requirepass` for the default user. `requirepass` is the default user's
/// password, so a named user needs `KIVIDB_PASSWORD`.
fn auth(
    config: &Config,
    user: Option<String>,
    password: Option<String>,
) -> Result<Option<Vec<String>>, String> {
    let user = user.filter(|u| !u.is_empty());
    let password = password.filter(|p| !p.is_empty());
    Ok(match (user, password) {
        (Some(user), Some(password)) => Some(vec![user, password]),
        (Some(_), None) => return Err(format!("{USER_ENV} is set, and {PASSWORD_ENV} is not")),
        (None, Some(password)) => Some(vec![password]),
        (None, None) => (!config.requirepass.is_empty()).then(|| vec![config.requirepass.clone()]),
    })
}

fn ping(config: &Config) -> Result<(), String> {
    let deadline = Instant::now() + TIMEOUT;
    let (port, tls) = target(config);
    if port == 0 {
        return Err("no port to connect to: set --port, --tls-port or --ping-port".into());
    }
    let auth = auth(
        config,
        std::env::var(USER_ENV).ok(),
        std::env::var(PASSWORD_ENV).ok(),
    )?;
    let host = config.ping_host.as_str();
    let tcp = connect(host, port, deadline)?;

    // AUTH and PING in one write. Only PING's reply decides: an AUTH the
    // server did not need (no password set) is refused, and PING still
    // answers PONG; a wrong password leaves PING answering NOAUTH.
    let mut request = Vec::new();
    if let Some(args) = &auth {
        let mut words: Vec<&[u8]> = vec![b"AUTH"];
        words.extend(args.iter().map(|a| a.as_bytes()));
        request.extend(command(&words));
    }
    request.extend(command(&[b"PING"]));
    let replies = if auth.is_some() { 2 } else { 1 };

    let lines = if tls {
        exchange(
            &mut tls_stream(config, host, port, tcp)?,
            &request,
            replies,
            deadline,
        )?
    } else {
        let mut tcp = tcp;
        exchange(&mut tcp, &request, replies, deadline)?
    };
    let pong = lines.last().map(String::as_str).unwrap_or_default();
    if pong == "+PONG" {
        return Ok(());
    }
    match lines.first() {
        Some(auth) if replies == 2 && auth.starts_with('-') && pong.starts_with("-NOAUTH") => {
            Err(format!("AUTH refused: {}", &auth[1..]))
        }
        _ => Err(format!("{host}:{port} answered PING with: {pong}")),
    }
}

/// A RESP array of bulk strings.
fn command(args: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        out.extend(format!("${}\r\n", arg.len()).into_bytes());
        out.extend_from_slice(arg);
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// What is left of the deadline, or an error once it has passed.
fn remaining(deadline: Instant) -> Result<Duration, String> {
    let left = deadline.saturating_duration_since(Instant::now());
    if left.is_zero() {
        Err(format!("no reply within {} s", TIMEOUT.as_secs()))
    } else {
        Ok(left)
    }
}

/// Connect to the first address of `host` that answers before the deadline.
fn connect(host: &str, port: u16, deadline: Instant) -> Result<TcpStream, String> {
    let addrs = (host, port)
        .to_socket_addrs()
        .map_err(|e| format!("can't resolve '{host}': {e}"))?;
    let mut last = format!("can't connect to {host}:{port}: no address");
    for addr in addrs {
        match TcpStream::connect_timeout(&addr, remaining(deadline)?) {
            Ok(tcp) => {
                let _ = tcp.set_nodelay(true);
                return Ok(tcp);
            }
            Err(e) => last = format!("can't connect to {addr}: {e}"),
        }
    }
    Err(last)
}

/// A connection to talk on, and the socket under it for its timeouts.
trait Conn: Read + Write {
    fn tcp(&self) -> &TcpStream;
}

impl Conn for TcpStream {
    fn tcp(&self) -> &TcpStream {
        self
    }
}

/// Give the socket what is left of the deadline for its next read or write.
fn arm(conn: &impl Conn, deadline: Instant) -> Result<(), String> {
    let left = remaining(deadline)?;
    let tcp = conn.tcp();
    tcp.set_read_timeout(Some(left))
        .and_then(|()| tcp.set_write_timeout(Some(left)))
        .map_err(|e| format!("socket: {e}"))
}

fn io_error(what: &str, e: std::io::Error) -> String {
    match e.kind() {
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut => {
            format!("no reply within {} s", TIMEOUT.as_secs())
        }
        _ => format!("{what}: {e}"),
    }
}

/// Send `request` and read `replies` one-line replies (`+…` or `-…`, which
/// is all AUTH and PING answer), returned without their CRLF.
fn exchange(
    conn: &mut impl Conn,
    request: &[u8],
    replies: usize,
    deadline: Instant,
) -> Result<Vec<String>, String> {
    arm(conn, deadline)?;
    conn.write_all(request)
        .and_then(|()| conn.flush())
        .map_err(|e| io_error("send", e))?;
    let mut buf = Vec::new();
    let mut chunk = [0u8; 512];
    while buf.windows(2).filter(|w| w == b"\r\n").count() < replies {
        // PONG and the errors AUTH answers with are short: anything long is
        // not a KiviDB, or not one in a state to say PONG.
        if buf.len() > 4096 {
            return Err("the reply is not a PING reply".into());
        }
        arm(conn, deadline)?;
        let n = conn.read(&mut chunk).map_err(|e| io_error("read", e))?;
        if n == 0 {
            return Err("the server closed the connection".into());
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    Ok(String::from_utf8_lossy(&buf)
        .split("\r\n")
        .take(replies)
        .map(str::to_string)
        .collect())
}

#[cfg(feature = "tls")]
mod tls {
    use std::sync::Arc;
    use tokio_rustls::rustls;
    use tokio_rustls::rustls::client::danger::{
        HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
    };
    use tokio_rustls::rustls::crypto::CryptoProvider;
    use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use tokio_rustls::rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};

    pub type Stream = rustls::StreamOwned<rustls::ClientConnection, std::net::TcpStream>;

    impl super::Conn for Stream {
        fn tcp(&self) -> &std::net::TcpStream {
            &self.sock
        }
    }

    /// Takes any certificate the server presents, still checking that the
    /// handshake is signed by its key. Used only without `tls-ca-cert-file`,
    /// when there is nothing to check the chain against, and only for a
    /// server on this host: see [`stream`].
    #[derive(Debug)]
    struct AnyCertificate(Arc<CryptoProvider>);

    impl ServerCertVerifier for AnyCertificate {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp: &[u8],
            _now: UnixTime,
        ) -> Result<ServerCertVerified, rustls::Error> {
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            rustls::crypto::verify_tls12_signature(
                message,
                cert,
                dss,
                &self.0.signature_verification_algorithms,
            )
        }

        fn verify_tls13_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            rustls::crypto::verify_tls13_signature(
                message,
                cert,
                dss,
                &self.0.signature_verification_algorithms,
            )
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            self.0.signature_verification_algorithms.supported_schemes()
        }
    }

    /// A TLS client on `tcp`. With `tls-ca-cert-file` it connects as one
    /// node does to another: it presents `tls-client-cert-file` (or
    /// `tls-cert-file`), which `tls-auth-clients yes` asks for, and checks
    /// the server's chain against the CA. Without one, the server asks for
    /// no certificate and there is no CA to check its own against: that is
    /// taken only from a server on this host (a loopback address), since the
    /// password goes to whoever answers. A server elsewhere needs the CA.
    ///
    /// The files are read as the server reads them, after changing to `dir`.
    pub fn stream(
        config: &crate::config::Config,
        host: &str,
        tcp: std::net::TcpStream,
    ) -> Result<Stream, String> {
        let mut files = crate::tls::acceptor::Files::of(config);
        for path in [
            &mut files.cert,
            &mut files.key,
            &mut files.ca,
            &mut files.client_cert,
            &mut files.client_key,
        ] {
            *path = super::in_dir(&config.dir, path);
        }
        let client = if files.ca.is_empty() {
            let peer = tcp.peer_addr().map_err(|e| format!("socket: {e}"))?;
            if !peer.ip().is_loopback() {
                return Err(format!(
                    "{peer} is not this host: set tls-ca-cert-file to check its certificate, \
                     since the password would go to whoever answers"
                ));
            }
            let builder = ClientConfig::builder();
            let provider = CryptoProvider::get_default()
                .cloned()
                .ok_or("no TLS crypto provider")?;
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(AnyCertificate(provider)))
                .with_no_client_auth()
        } else {
            crate::node_tls::client_config(&files)?
        };
        let name = ServerName::try_from(host.to_string())
            .unwrap_or_else(|_| ServerName::try_from("localhost").unwrap());
        let conn = rustls::ClientConnection::new(Arc::new(client), name)
            .map_err(|e| format!("TLS: {e}"))?;
        Ok(rustls::StreamOwned::new(conn, tcp))
    }
}

/// `path` as the server opens it: relative to `dir` (the server changes to
/// it before it reads its TLS files), unless absolute or `dir` is unset.
/// Empty stays empty.
#[cfg_attr(not(feature = "tls"), allow(dead_code))]
fn in_dir(dir: &str, path: &str) -> String {
    if dir.is_empty() || path.is_empty() || std::path::Path::new(path).is_absolute() {
        return path.to_string();
    }
    std::path::Path::new(dir)
        .join(path)
        .to_string_lossy()
        .into_owned()
}

#[cfg(feature = "tls")]
fn tls_stream(
    config: &Config,
    host: &str,
    _port: u16,
    tcp: TcpStream,
) -> Result<tls::Stream, String> {
    tls::stream(config, host, tcp)
}

#[cfg(not(feature = "tls"))]
fn tls_stream(
    _config: &Config,
    _host: &str,
    port: u16,
    _tcp: TcpStream,
) -> Result<TcpStream, String> {
    Err(format!(
        "port {port} is the TLS port, and this build has no TLS: use the tls or full build, \
         or --ping-port with the plaintext port"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_command_is_an_array_of_bulk_strings() {
        assert_eq!(command(&[b"PING"]), b"*1\r\n$4\r\nPING\r\n");
        assert_eq!(
            command(&[b"AUTH", b"pa ss"]),
            b"*2\r\n$4\r\nAUTH\r\n$5\r\npa ss\r\n"
        );
    }

    #[test]
    fn auth_takes_the_environment_first() {
        let s = |v: &str| Some(v.to_string());
        let none = Config::default();
        let pass = Config {
            requirepass: "conf".into(),
            ..Default::default()
        };
        assert_eq!(auth(&none, None, None), Ok(None));
        assert_eq!(auth(&pass, None, None), Ok(Some(vec!["conf".into()])));
        assert_eq!(auth(&pass, None, s("env")), Ok(Some(vec!["env".into()])));
        assert_eq!(auth(&pass, s(""), s("")), Ok(Some(vec!["conf".into()])));
        assert_eq!(
            auth(&pass, s("health"), s("pw")),
            Ok(Some(vec!["health".into(), "pw".into()]))
        );
        // requirepass is the default user's: a named user needs its own.
        assert!(auth(&pass, s("health"), None).is_err());
    }

    #[test]
    fn tls_files_are_relative_to_dir() {
        assert_eq!(in_dir("", "tls/ca.crt"), "tls/ca.crt");
        assert_eq!(in_dir("data", ""), "");
        let joined = in_dir("data", "tls/ca.crt");
        assert_eq!(
            std::path::Path::new(&joined),
            std::path::Path::new("data").join("tls/ca.crt")
        );
        let absolute = std::env::temp_dir().join("ca.crt");
        let absolute = absolute.to_str().unwrap();
        assert_eq!(in_dir("data", absolute), absolute);
    }

    #[test]
    fn the_target_follows_the_server_ports() {
        let plain = Config {
            port: 7000,
            ..Default::default()
        };
        assert_eq!(target(&plain), (7000, false));

        let tls_only = Config {
            port: 0,
            tls_port: 7001,
            ..Default::default()
        };
        assert_eq!(target(&tls_only), (7001, true));

        let both = Config {
            port: 7000,
            tls_port: 7001,
            ..Default::default()
        };
        assert_eq!(target(&both), (7000, false));
        assert_eq!(
            target(&Config {
                ping_port: 7001,
                ..both.clone()
            }),
            (7001, true)
        );
        assert_eq!(
            target(&Config {
                ping_port: 7100,
                ..both
            }),
            (7100, false)
        );
    }
}
