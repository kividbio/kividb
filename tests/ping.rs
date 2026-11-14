// tests/ping.rs
//
// `kividb --ping`, the health check for images with no shell (#433): it exits
// 0 when the server answers PING with PONG and 1 otherwise, reading the port,
// TLS and password as the server does, and never starting a server itself.

use std::net::TcpStream;
use std::process::{Child, Command, Output};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_kividb");

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

/// A port nothing is listening on, and not one the system hands out to
/// outgoing connections (see tests/config.rs).
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

/// A kividb process in a directory of its own, killed when dropped.
struct Server {
    child: Child,
    _dir: tempfile::TempDir,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Start a server with `args`, and wait for `port` to open.
fn start(port: u16, args: &[&str]) -> Server {
    let dir = tempfile::tempdir().unwrap();
    let child = Command::new(BIN)
        .current_dir(dir.path())
        .args([
            "--bind",
            "127.0.0.1",
            "--configfile",
            "",
            "--loglevel",
            "warning",
        ])
        .args(args)
        .stdout(std::process::Stdio::null())
        .spawn()
        .expect("start kividb");
    let server = Server { child, _dir: dir };
    let deadline = Instant::now() + Duration::from_secs(10);
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(Instant::now() < deadline, "port {port} never opened");
        std::thread::sleep(Duration::from_millis(50));
    }
    server
}

/// `kividb --ping` with `args`, and `KIVIDB_PASSWORD` set to `password`
/// (or unset).
fn ping(args: &[&str], password: Option<&str>) -> Output {
    let mut command = Command::new(BIN);
    command
        .arg("--ping")
        .args(args)
        .env_remove("KIVIDB_PASSWORD");
    if let Some(p) = password {
        command.env("KIVIDB_PASSWORD", p);
    }
    command.output().expect("run kividb --ping")
}

fn healthy(output: &Output) -> bool {
    match output.status.code() {
        Some(0) => true,
        Some(1) => false,
        other => panic!("exit code {other:?}: {output:?}"),
    }
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn exits_0_on_pong_and_1_with_nothing_listening() {
    let port = free_port();
    let p = port.to_string();

    let started = Instant::now();
    let down = ping(&["--port", &p, "--configfile", ""], None);
    assert!(!healthy(&down), "{down:?}");
    assert!(stderr(&down).starts_with("kividb --ping: "), "{down:?}");
    // About a second at most, however the connection fails.
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );

    let _server = start(port, &["--port", &p]);
    let up = ping(&["--port", &p, "--configfile", ""], None);
    assert!(healthy(&up), "{up:?}");
    assert_eq!(String::from_utf8_lossy(&up.stdout).trim(), "PONG");

    // `--ping-port` picks the port over `--port`.
    let other = free_port().to_string();
    let up = ping(
        &["--port", &other, "--ping-port", &p, "--configfile", ""],
        None,
    );
    assert!(healthy(&up), "{up:?}");
    let down = ping(
        &["--port", &p, "--ping-port", &other, "--configfile", ""],
        None,
    );
    assert!(!healthy(&down), "{down:?}");
}

#[test]
fn authenticates_from_the_environment_or_the_config_file() {
    let port = free_port();
    let p = port.to_string();
    let _server = start(port, &["--port", &p, "--requirepass", "s3cret"]);
    let args = ["--port", p.as_str(), "--configfile", ""];

    let none = ping(&args, None);
    assert!(!healthy(&none), "{none:?}");
    assert!(stderr(&none).contains("NOAUTH"), "{none:?}");

    let wrong = ping(&args, Some("nope"));
    assert!(!healthy(&wrong), "{wrong:?}");
    assert!(stderr(&wrong).contains("AUTH refused"), "{wrong:?}");

    assert!(healthy(&ping(&args, Some("s3cret"))));

    // As the server reads it: the port and password from the config file.
    let dir = tempfile::tempdir().unwrap();
    let conf = dir.path().join("kividb.conf");
    std::fs::write(&conf, format!("port {port}\nrequirepass s3cret\n")).unwrap();
    let conf = conf.to_str().unwrap();
    assert!(healthy(&ping(&["--configfile", conf], None)));
    // KIVIDB_PASSWORD comes first.
    assert!(!healthy(&ping(&["--configfile", conf], Some("nope"))));
}

#[test]
fn a_password_the_server_does_not_need_is_no_failure() {
    let port = free_port();
    let p = port.to_string();
    let _server = start(port, &["--port", &p]);
    let output = ping(&["--port", &p, "--configfile", ""], Some("unneeded"));
    assert!(healthy(&output), "{output:?}");
}

#[test]
fn starts_no_server_and_leaves_the_directory_alone() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir(&data).unwrap();
    let conf = dir.path().join("kividb.conf");
    let port = free_port();
    std::fs::write(
        &conf,
        format!("port {port}\ndir {}\nappendonly yes\n", data.display()),
    )
    .unwrap();
    let output = Command::new(BIN)
        .current_dir(dir.path())
        .args(["--ping", "--configfile", conf.to_str().unwrap()])
        .env_remove("KIVIDB_PASSWORD")
        .output()
        .unwrap();
    assert!(!healthy(&output), "{output:?}");
    assert!(port_is_free(port), "--ping must not listen");
    assert_eq!(
        std::fs::read_dir(&data).unwrap().count(),
        0,
        "data dir touched"
    );
    let mut left: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    left.sort();
    assert_eq!(left, ["data", "kividb.conf"]);
}

#[test]
fn a_config_file_it_cannot_read_is_a_failure() {
    let dir = tempfile::tempdir().unwrap();
    let conf = dir.path().join("kividb.conf");
    std::fs::write(&conf, "no-such-directive yes\n").unwrap();
    let output = ping(&["--configfile", conf.to_str().unwrap()], None);
    assert!(!healthy(&output), "{output:?}");
    assert!(stderr(&output).contains("no-such-directive"), "{output:?}");
}

#[cfg(feature = "tls")]
mod tls {
    use super::*;
    use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair};

    struct Files {
        _dir: tempfile::TempDir,
        ca: String,
        cert: String,
        key: String,
    }

    /// A CA, and a certificate for 127.0.0.1 it signed.
    fn files() -> Files {
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(DnType::CommonName, "ping test ca");
        let ca_key = KeyPair::generate().unwrap();
        let ca = params.self_signed(&ca_key).unwrap();
        let key = KeyPair::generate().unwrap();
        let cert = CertificateParams::new(vec!["127.0.0.1".to_string()])
            .unwrap()
            .signed_by(&key, &ca, &ca_key)
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let write = |name: &str, pem: String| {
            let path = dir.path().join(name);
            std::fs::write(&path, pem).unwrap();
            path.to_str().unwrap().to_string()
        };
        Files {
            ca: write("ca.crt", ca.pem()),
            cert: write("server.crt", cert.pem()),
            key: write("server.key", key.serialize_pem()),
            _dir: dir,
        }
    }

    /// With `port 0`, the TLS port is the one checked, over TLS.
    #[test]
    fn checks_the_tls_port_when_it_is_the_only_one() {
        let f = files();
        let tls = free_port();
        let t = tls.to_string();
        let tls_args = [
            "--port",
            "0",
            "--tls-port",
            &t,
            "--tls-cert-file",
            &f.cert,
            "--tls-key-file",
            &f.key,
        ];
        let _server = start(tls, &tls_args);
        let mut args = tls_args.to_vec();
        args.extend(["--configfile", ""]);
        let output = ping(&args, None);
        assert!(healthy(&output), "{output:?}");
    }

    /// With a CA file, clients must present a certificate it signed: the
    /// check presents the server's own, and checks the server's chain.
    #[test]
    fn presents_a_certificate_when_the_server_asks_for_one() {
        let f = files();
        let tls = free_port();
        let t = tls.to_string();
        let tls_args = [
            "--port",
            "0",
            "--tls-port",
            &t,
            "--tls-cert-file",
            &f.cert,
            "--tls-key-file",
            &f.key,
            "--tls-ca-cert-file",
            &f.ca,
            "--requirepass",
            "s3cret",
        ];
        let _server = start(tls, &tls_args);
        let mut args = tls_args.to_vec();
        args.extend(["--configfile", ""]);
        let output = ping(&args, None);
        assert!(healthy(&output), "{output:?}");

        // Without the CA file it has no certificate to present, and the
        // server refuses the handshake.
        let output = ping(&args[..8], None);
        assert!(!healthy(&output), "{output:?}");
    }

    /// The plaintext port of a server with both is spoken to in plaintext.
    #[test]
    fn checks_the_plaintext_port_beside_a_tls_one() {
        let f = files();
        let (plain, tls) = (free_port(), free_port());
        let (p, t) = (plain.to_string(), tls.to_string());
        let tls_args = [
            "--port",
            &p,
            "--tls-port",
            &t,
            "--tls-cert-file",
            &f.cert,
            "--tls-key-file",
            &f.key,
        ];
        let _server = start(tls, &tls_args);
        let mut args = tls_args.to_vec();
        args.extend(["--configfile", ""]);
        assert!(healthy(&ping(&args, None)));
        args.extend(["--ping-port", &t]);
        assert!(healthy(&ping(&args, None)));
    }
}
