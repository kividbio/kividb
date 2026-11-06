// RDB files with keys in databases past 15 (#477). The loader put every
// database of 16 or more into database 0, merged with its keys: where names
// clashed, keys were overwritten, and nothing said so. A server with enough
// `databases` now loads each key into its own database; one without refuses
// the file, naming the database and the setting.
//
// The fixtures were saved by Redis 8.10, Valkey 8.1 and 9.0 and KeyDB 6.3,
// each started with `--databases 32`, and by Dragonfly 2.1 (`--dbnum 32`,
// `SAVE RDB`): `shared` (`from-db-0`) and `only0` in database 0; `shared`
// (`from-db-20`), `only20` and the hash `h20` in database 20; `last` in
// database 31.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// A loaded database keyed by text, for these tests, whose keys all are:
/// a load gives keys back as bytes (`store::Key`).
fn texted<V>(
    map: std::collections::HashMap<kividb::store::Key, V>,
) -> std::collections::HashMap<String, V> {
    map.into_iter()
        .map(|(key, value)| (String::from_utf8(key.to_vec()).unwrap(), value))
        .collect()
}

fn texted_all<V>(
    maps: Vec<std::collections::HashMap<kividb::store::Key, V>>,
) -> Vec<std::collections::HashMap<String, V>> {
    maps.into_iter().map(texted).collect()
}

const FIXTURES: [&str; 5] = [
    "redis-8.10-databases-32.rdb",
    "valkey-8.1-databases-32.rdb",
    "valkey-9.0-databases-32.rdb",
    "keydb-6.3-databases-32.rdb",
    "dragonfly-2.1-databases-32.rdb",
];

fn fixture(name: &str) -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

/// The loader keeps each database's keys apart, up to the number of
/// databases it is told the server has, and refuses a file past them.
#[test]
fn the_loader_keeps_databases_past_15_apart_or_refuses_them() {
    use kividb::snapshot::{RDB_DATABASES, load_redis_rdb_with_expiry};
    use kividb::types::KiviDBValue;
    use std::sync::atomic::Ordering;

    let text = |value: &KiviDBValue| match value {
        KiviDBValue::String(bytes) => String::from_utf8(bytes.to_vec()).unwrap(),
        _ => panic!("not a string"),
    };
    for name in FIXTURES {
        let path = fixture(name);
        let path = path.to_str().unwrap();

        RDB_DATABASES.store(32, Ordering::Relaxed);
        let (dbs, _, _) =
            load_redis_rdb_with_expiry(path).unwrap_or_else(|e| panic!("{name}: {e}"));
        let dbs = texted_all(dbs);
        assert_eq!(dbs.len(), 32, "{name}");
        assert_eq!(text(&dbs[0]["shared"]), "from-db-0", "{name}");
        assert_eq!(dbs[0].len(), 2, "{name}: {:?}", dbs[0].keys());
        assert_eq!(text(&dbs[20]["shared"]), "from-db-20", "{name}");
        assert_eq!(text(&dbs[20]["only20"]), "twenty", "{name}");
        assert!(matches!(dbs[20]["h20"], KiviDBValue::Hash(_)), "{name}");
        assert_eq!(text(&dbs[31]["last"]), "thirty-one", "{name}");
        let others: usize = (1..32)
            .filter(|db| ![20, 31].contains(db))
            .map(|db| dbs[db].len())
            .sum();
        assert_eq!(others, 0, "{name}");

        for databases in [16, 31] {
            RDB_DATABASES.store(databases, Ordering::Relaxed);
            let err = load_redis_rdb_with_expiry(path)
                .err()
                .unwrap_or_else(|| panic!("{name} loaded into {databases} databases"));
            let err = err.to_string();
            let past = if databases == 16 { 20 } else { 31 };
            assert!(
                err.contains(&format!("database {past}"))
                    && err.contains(&format!("databases {}", past + 1)),
                "{name}, {databases}: {err}"
            );
        }
    }
    RDB_DATABASES.store(16, Ordering::Relaxed);
}

// ── Servers ──────────────────────────────────────────────────────────────────

fn port_is_free(port: u16) -> bool {
    use socket2::{Domain, Socket, Type};
    let Ok(socket) = Socket::new(Domain::IPV4, Type::STREAM, None) else {
        return false;
    };
    #[cfg(unix)]
    let _ = socket.set_reuse_address(true);
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], port).into();
    socket.bind(&addr.into()).is_ok()
}

/// A port nothing is listening on, outside the range the system hands out to
/// outgoing connections.
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

fn kividb(dir: &Path, port: u16, extra: &[&str]) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_kividb"));
    cmd.current_dir(dir)
        .args([
            "--port",
            &port.to_string(),
            "--bind",
            "127.0.0.1",
            "--io-model",
            "tokio",
            "--configfile",
            "kividb.conf",
        ])
        .args(extra)
        .stdin(Stdio::null())
        .stdout(Stdio::null());
    cmd
}

struct Conn {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

impl Conn {
    fn connect(port: u16) -> Conn {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Ok(stream) = TcpStream::connect(("127.0.0.1", port)) {
                stream
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                let reader = BufReader::new(stream.try_clone().unwrap());
                return Conn { stream, reader };
            }
            assert!(Instant::now() < deadline, "no server on {port}");
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// A command, and its reply as text: a bulk string's bytes, a simple
    /// string or error line, an integer; nil as `(nil)`.
    fn cmd(&mut self, args: &[&str]) -> String {
        let mut out = format!("*{}\r\n", args.len());
        for arg in args {
            out.push_str(&format!("${}\r\n{arg}\r\n", arg.len()));
        }
        self.stream.write_all(out.as_bytes()).unwrap();
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        let line = line.trim_end();
        match line.as_bytes().first() {
            Some(b'$') if line == "$-1" => "(nil)".into(),
            Some(b'$') => {
                let len: usize = line[1..].parse().unwrap();
                let mut data = vec![0u8; len + 2];
                self.reader.read_exact(&mut data).unwrap();
                String::from_utf8(data[..len].to_vec()).unwrap()
            }
            _ => line[1..].to_string(),
        }
    }
}

struct Server(std::process::Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Whether a server started in `dir` with `args` exited with an error within
/// 30 s (one that starts is stopped), and what it logged.
fn fails_to_start(dir: &Path, args: &[&str]) -> (bool, String) {
    let mut child = kividb(dir, free_port(), args)
        .stderr(Stdio::piped())
        .spawn()
        .expect("could not run kividb");
    let deadline = Instant::now() + Duration::from_secs(30);
    let failed = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break !status.success();
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            break false;
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    let mut stderr = String::new();
    let _ = child.stderr.take().unwrap().read_to_string(&mut stderr);
    (failed, stderr)
}

/// Each key is where the file had it.
fn assert_in_their_databases(c: &mut Conn, name: &str) {
    assert_eq!(c.cmd(&["GET", "shared"]), "from-db-0", "{name}");
    assert_eq!(c.cmd(&["DBSIZE"]), "2", "{name}");
    assert_eq!(c.cmd(&["SELECT", "20"]), "OK", "{name}");
    assert_eq!(c.cmd(&["GET", "shared"]), "from-db-20", "{name}");
    assert_eq!(c.cmd(&["GET", "only20"]), "twenty", "{name}");
    assert_eq!(c.cmd(&["HGET", "h20", "f"]), "v", "{name}");
    assert_eq!(c.cmd(&["DBSIZE"]), "3", "{name}");
    assert_eq!(c.cmd(&["SELECT", "31"]), "OK", "{name}");
    assert_eq!(c.cmd(&["GET", "last"]), "thirty-one", "{name}");
}

/// `--preload-file rdb:` and `dump.rdb` at startup: with `databases 32` each
/// key loads into its own database; with the default 16 the server does not
/// start, says which database and what to raise, and leaves `dump.rdb` as it
/// was. The same goes for the base file of an `aof:` preload.
#[test]
fn a_server_loads_databases_past_15_into_their_own_or_refuses_the_file() {
    for name in FIXTURES {
        let bytes = std::fs::read(fixture(name)).unwrap();

        // Preloaded.
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("source.rdb");
        std::fs::write(&file, &bytes).unwrap();
        let target = format!("rdb:{}", file.display());
        let (failed, stderr) = fails_to_start(dir.path(), &["--preload-file", &target]);
        assert!(failed, "{name} preloaded into 16 databases: {stderr}");
        assert!(
            stderr.contains("database 20") && stderr.contains("databases 21"),
            "{name}: {stderr}"
        );
        let port = free_port();
        let _server = Server(
            kividb(
                dir.path(),
                port,
                &["--databases", "32", "--preload-file", &target],
            )
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
        );
        assert_in_their_databases(&mut Conn::connect(port), name);
        drop(_server);

        // As the base file of an aof: preload.
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("base.rdb"), &bytes).unwrap();
        let manifest = dir.path().join("backup.manifest");
        std::fs::write(&manifest, "file base.rdb type b seq 1\n").unwrap();
        let target = format!("aof:{}", manifest.display());
        let (failed, stderr) = fails_to_start(dir.path(), &["--preload-file", &target]);
        assert!(failed, "{name}: aof base into 16 databases: {stderr}");
        assert!(stderr.contains("database 20"), "{name}: {stderr}");

        // As dump.rdb.
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("dump.rdb"), &bytes).unwrap();
        let (failed, stderr) = fails_to_start(dir.path(), &[]);
        assert!(failed, "{name}: dump.rdb into 16 databases: {stderr}");
        assert!(stderr.contains("database 20"), "{name}: {stderr}");
        assert_eq!(std::fs::read(dir.path().join("dump.rdb")).unwrap(), bytes);
        let port = free_port();
        let _server = Server(
            kividb(dir.path(), port, &["--databases", "32"])
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
        assert_in_their_databases(&mut Conn::connect(port), name);
    }
}

/// A full sync from a master whose payload has keys in database 20: a replica
/// with 16 databases refuses it and keeps what it has; one with 32 loads each
/// key into its own database. The master here is a stand-in that answers
/// `PSYNC` with the Redis file, as a Redis master does.
#[test]
fn a_full_sync_with_databases_past_15_is_refused_or_kept_apart() {
    let payload = std::fs::read(fixture("redis-8.10-databases-32.rdb")).unwrap();
    for databases in ["16", "32"] {
        let listener = std::net::TcpListener::bind(("127.0.0.1", free_port())).unwrap();
        let master_port = listener.local_addr().unwrap().port();
        let served = payload.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming().take(5) {
                let Ok(stream) = stream else { continue };
                let served = served.clone();
                std::thread::spawn(move || fake_master(stream, &served));
            }
        });
        let dir = tempfile::tempdir().unwrap();
        let port = free_port();
        let _replica = Server(
            kividb(
                dir.path(),
                port,
                &[
                    "--databases",
                    databases,
                    "--replicaof",
                    "127.0.0.1",
                    &master_port.to_string(),
                ],
            )
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
        );
        let mut c = Conn::connect(port);
        let deadline = Instant::now() + Duration::from_secs(20);
        if databases == "16" {
            // Tried, refused, and nothing loaded; the link stays down.
            std::thread::sleep(Duration::from_secs(3));
            assert_eq!(c.cmd(&["DBSIZE"]), "0");
            let info = c.cmd(&["INFO", "replication"]);
            assert!(info.contains("master_link_status:down"), "{info}");
        } else {
            while c.cmd(&["GET", "only0"]) != "zero" {
                assert!(Instant::now() < deadline, "the full sync did not load");
                std::thread::sleep(Duration::from_millis(100));
            }
            assert_in_their_databases(&mut c, "full sync");
        }
    }
}

/// A master that answers the replica's handshake and `PSYNC` with a full
/// resynchronisation carrying `payload`, then stays silent.
fn fake_master(stream: TcpStream, payload: &[u8]) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut stream = stream;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 {
            return;
        }
        let Some(count) = line.trim_end().strip_prefix('*') else {
            continue;
        };
        let count: usize = count.parse().unwrap_or(0);
        let mut args = Vec::new();
        for _ in 0..count {
            let mut header = String::new();
            reader.read_line(&mut header).unwrap();
            let len: usize = header.trim_end()[1..].parse().unwrap();
            let mut data = vec![0u8; len + 2];
            reader.read_exact(&mut data).unwrap();
            args.push(String::from_utf8_lossy(&data[..len]).to_ascii_uppercase());
        }
        let reply: Vec<u8> = match args.first().map(String::as_str) {
            Some("PING") => b"+PONG\r\n".to_vec(),
            Some("PSYNC") | Some("SYNC") => {
                let mut out = format!("+FULLRESYNC {} 0\r\n${}\r\n", "0".repeat(40), payload.len())
                    .into_bytes();
                out.extend_from_slice(payload);
                out
            }
            _ => b"+OK\r\n".to_vec(),
        };
        if stream.write_all(&reply).is_err() {
            return;
        }
    }
}
