// tests/torn_snapshot.rs
//
// A `dump.kdb` cut short inside the sections after its last database (#474).
// The sections hold the keys' times to live, the function libraries, and the
// search and vector indexes with their aliases. Cut inside the index, HNSW
// graph or alias section the image still loaded, with a warning, and the
// server started without its indexes (and without whatever was stored after
// the cut). It is now refused, as an image cut between keys is (#444), at
// startup and in a full sync, and an image cut inside a section's magic as
// well. A whole image with a section this build does not know still loads.
//
// Real `kividb` processes, each in its own directory: what is under test is
// whether a server starts, and what it leaves on disk.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Whether nothing has `port`, asked with a socket that is bound and never
/// listens (see `tests/backup.rs`).
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

/// A port nothing is listening on, outside the range the system hands out to
/// outgoing connections.
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

struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn start(dir: &Path, port: u16, extra: &[&str]) -> Server {
    Server(
        kividb(dir, port, extra)
            .stderr(Stdio::null())
            .spawn()
            .expect("could not start kividb"),
    )
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
    /// string or error line (an error keeps its `-`), an integer; nil as
    /// `(nil)`; an array as its elements' text, one per line.
    fn cmd<A: AsRef<[u8]>>(&mut self, args: &[A]) -> String {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            let arg = arg.as_ref();
            out.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            out.extend_from_slice(arg);
            out.extend_from_slice(b"\r\n");
        }
        self.stream.write_all(&out).unwrap();
        self.read()
    }

    fn read(&mut self) -> String {
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        let line = line.trim_end();
        match line.as_bytes().first() {
            Some(b'$') if line == "$-1" => "(nil)".into(),
            Some(b'$') => {
                let len: usize = line[1..].parse().unwrap();
                let mut data = vec![0u8; len + 2];
                self.reader.read_exact(&mut data).unwrap();
                String::from_utf8_lossy(&data[..len]).into_owned()
            }
            Some(b'*') => {
                let n: i64 = line[1..].parse().unwrap();
                (0..n.max(0))
                    .map(|_| self.read())
                    .collect::<Vec<_>>()
                    .join("\n")
            }
            Some(b'-') => line.to_string(),
            _ => line[1..].to_string(),
        }
    }
}

/// Every file of `dir` and what it holds.
fn directory(dir: &Path) -> Vec<(String, Vec<u8>)> {
    let mut files: Vec<(String, Vec<u8>)> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let name = entry.file_name().to_string_lossy().into_owned();
            (name, std::fs::read(entry.path()).unwrap_or_default())
        })
        .collect();
    files.sort();
    files
}

/// Whether a server started in `dir` exited with an error within 30 s (one
/// that starts is stopped), and what it logged.
fn fails_to_start(dir: &Path) -> (bool, String) {
    let mut child = kividb(dir, free_port(), &[])
        .stderr(Stdio::piped())
        .spawn()
        .expect("could not run kividb");
    let mut stderr = child.stderr.take().unwrap();
    let said = std::thread::spawn(move || {
        let mut said = String::new();
        let _ = stderr.read_to_string(&mut said);
        said
    });
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
    (failed, said.join().unwrap())
}

/// The magic of every section a `dump.kdb` can hold after its databases.
const SECTIONS: [(&str, &[u8; 8]); 10] = [
    ("expiry", b"KIVIEX1\0"),
    ("functions", b"KIVIFN1\0"),
    ("index", b"KIVIDX1\0"),
    ("HNSW graph", b"KIVIHN1\0"),
    ("alias", b"KIVIAL1\0"),
    ("field options", b"KIVIFO1\0"),
    ("synonyms", b"KIVISD1\0"),
    ("vector types", b"KIVIVT1\0"),
    ("language", b"KIVILG1\0"),
    ("score", b"KIVISR1\0"),
];

/// The sections these tests cut inside of. A build without Lua writes no
/// function libraries.
fn cut_sections() -> Vec<&'static str> {
    let mut wanted = vec!["expiry", "index", "HNSW graph", "alias"];
    if cfg!(feature = "lua") {
        wanted.push("functions");
    }
    wanted
}

/// A `dump.kdb` saved by a server holding a key with a time to live, a hash
/// indexed by a search index with an HNSW vector field, an alias of the
/// index and, with Lua, a function library.
fn saved_dump() -> Vec<u8> {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let _server = start(dir.path(), port, &[]);
    let mut c = Conn::connect(port);
    assert_eq!(c.cmd(&["SET", "timed", "v", "EX", "100000"]), "OK");
    let reply = c.cmd(&[
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
        "v",
        "VECTOR",
        "HNSW",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "2",
        "DISTANCE_METRIC",
        "L2",
    ]);
    assert_eq!(reply, "OK");
    for i in 0..4u32 {
        let vector: Vec<u8> = [i as f32, 1.0]
            .iter()
            .flat_map(|f| f.to_le_bytes())
            .collect();
        let key = format!("doc:{i}");
        let args: [&[u8]; 6] = [b"HSET", key.as_bytes(), b"t", b"hello", b"v", &vector];
        assert_eq!(c.cmd(&args), "2");
    }
    assert_eq!(c.cmd(&["FT.ALIASADD", "al", "idx"]), "OK");
    if cfg!(feature = "lua") {
        let library =
            "#!lua name=lib474\nredis.register_function('f474', function() return 474 end)";
        assert_eq!(c.cmd(&["FUNCTION", "LOAD", library]), "lib474");
    }
    assert_eq!(c.cmd(&["SAVE"]), "OK");
    std::fs::read(dir.path().join("dump.kdb")).unwrap()
}

/// Where each section of `image` starts and ends, by name.
fn section_spans(image: &[u8]) -> Vec<(&'static str, usize, usize)> {
    let mut starts: Vec<(&'static str, usize)> = SECTIONS
        .iter()
        .filter_map(|(name, magic)| {
            image
                .windows(8)
                .position(|w| w == *magic)
                .map(|at| (*name, at))
        })
        .collect();
    starts.sort_by_key(|&(_, at)| at);
    starts
        .iter()
        .enumerate()
        .map(|(i, &(name, at))| {
            let end = starts.get(i + 1).map_or(image.len(), |&(_, next)| next);
            (name, at, end)
        })
        .collect()
}

/// Places to cut a section from `at` to `end`: inside its magic, just after
/// it, a little way into its body, halfway, and one byte short of its end.
fn cuts(at: usize, end: usize) -> Vec<usize> {
    let mut cuts = vec![at + 3, at + 8, at + 10, (at + end) / 2, end - 1];
    cuts.retain(|&cut| cut > at && cut < end);
    cuts.sort_unstable();
    cuts.dedup();
    cuts
}

/// What a server that loaded `saved_dump` whole has.
fn assert_whole(c: &mut Conn, what: &str) {
    assert_eq!(c.cmd(&["GET", "timed"]), "v", "{what}");
    let ttl: i64 = c.cmd(&["TTL", "timed"]).parse().unwrap();
    assert!(ttl > 0, "{what}: the key lost its time to live ({ttl})");
    assert!(c.cmd(&["FT._LIST"]).lines().any(|l| l == "idx"), "{what}");
    assert!(!c.cmd(&["FT.INFO", "al"]).starts_with('-'), "{what}");
    if cfg!(feature = "lua") {
        assert_eq!(c.cmd(&["FCALL", "f474", "0"]), "474", "{what}");
    }
}

/// A `dump.kdb` cut at several places inside each section after its last
/// database, and inside a section's magic, does not start the server, which
/// says the snapshot is cut short and leaves its data directory as it was.
/// The whole file starts it with every TTL, index, alias and library.
#[test]
fn a_dump_cut_inside_a_trailing_section_does_not_start_the_server() {
    let image = saved_dump();
    let spans = section_spans(&image);
    for wanted in cut_sections() {
        assert!(
            spans.iter().any(|&(name, _, _)| name == wanted),
            "the dump has no {wanted} section: {spans:?}"
        );
    }

    for &(name, at, end) in &spans {
        if !cut_sections().contains(&name) {
            continue;
        }
        for cut in cuts(at, end) {
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(dir.path().join("dump.kdb"), &image[..cut]).unwrap();
            // And the lock file a server makes, which must stay as it is too.
            std::fs::write(dir.path().join("kividb.lock"), b"").unwrap();
            let before = directory(dir.path());
            let (failed, stderr) = fails_to_start(dir.path());
            let what = format!("cut at {cut} in the {name} section ({at}..{end})");
            assert!(failed, "{what}: the server started");
            assert!(
                stderr.contains("dump.kdb cannot be loaded") && stderr.contains("cut short"),
                "{what}: {stderr}"
            );
            assert!(
                directory(dir.path()) == before,
                "{what}: the data directory changed"
            );
        }
    }

    // The whole file loads, with everything in it.
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("dump.kdb"), &image).unwrap();
    let port = free_port();
    let _server = start(dir.path(), port, &[]);
    assert_whole(&mut Conn::connect(port), "the whole dump");
}

/// A whole `dump.kdb` that ends with a section this build does not know, as
/// a later release may write, still loads, and so does every section before
/// it.
#[test]
fn a_whole_dump_with_an_unknown_section_still_loads() {
    let mut image = saved_dump();
    image.extend_from_slice(b"KIVIZZ9\0and what it holds");
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("dump.kdb"), &image).unwrap();
    let port = free_port();
    let _server = start(dir.path(), port, &[]);
    assert_whole(&mut Conn::connect(port), "a dump with an unknown section");
}

/// A master that answers the replica's handshake and `PSYNC` with a full
/// resynchronisation carrying `payload`, says so on `sent`, then stays
/// silent.
fn fake_master(stream: TcpStream, payload: &[u8], sent: &std::sync::mpsc::Sender<()>) {
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
            if reader.read_line(&mut header).unwrap_or(0) == 0 {
                return;
            }
            let len: usize = header.trim_end()[1..].parse().unwrap();
            let mut data = vec![0u8; len + 2];
            if reader.read_exact(&mut data).is_err() {
                return;
            }
            args.push(String::from_utf8_lossy(&data[..len]).to_ascii_uppercase());
        }
        let full_sync = matches!(args.first().map(String::as_str), Some("PSYNC" | "SYNC"));
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
        if full_sync {
            let _ = sent.send(());
        }
    }
}

/// A full sync whose payload is cut short inside a section after its last
/// database (the transfer itself is whole: the master sends as many bytes
/// as it said) is refused, and the replica keeps the dataset it had.
#[test]
fn a_full_sync_cut_inside_a_trailing_section_keeps_the_replicas_data() {
    let image = saved_dump();
    let spans = section_spans(&image);
    for wanted in ["expiry", "index", "alias"] {
        let &(_, at, end) = spans
            .iter()
            .find(|&&(name, _, _)| name == wanted)
            .unwrap_or_else(|| panic!("the dump has no {wanted} section"));
        let payload = image[..(at + end) / 2].to_vec();
        let listener = std::net::TcpListener::bind(("127.0.0.1", free_port())).unwrap();
        let master_port = listener.local_addr().unwrap().port();
        let (sent, synced) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for stream in listener.incoming().take(5) {
                let Ok(stream) = stream else { continue };
                let payload = payload.clone();
                let sent = sent.clone();
                std::thread::spawn(move || fake_master(stream, &payload, &sent));
            }
        });
        let dir = tempfile::tempdir().unwrap();
        let port = free_port();
        let _replica = start(dir.path(), port, &[]);
        let mut c = Conn::connect(port);
        assert_eq!(c.cmd(&["SET", "mine", "kept"]), "OK");
        assert_eq!(
            c.cmd(&["REPLICAOF", "127.0.0.1", &master_port.to_string()]),
            "OK"
        );
        synced
            .recv_timeout(Duration::from_secs(20))
            .unwrap_or_else(|_| panic!("{wanted}: the replica never asked for a full sync"));
        // Read, refused; the link stays down.
        std::thread::sleep(Duration::from_secs(2));
        let what = format!("a full sync cut inside the {wanted} section");
        assert_eq!(c.cmd(&["GET", "mine"]), "kept", "{what}");
        assert_eq!(c.cmd(&["DBSIZE"]), "1", "{what}");
        assert_eq!(c.cmd(&["GET", "timed"]), "(nil)", "{what}");
        let info = c.cmd(&["INFO", "replication"]);
        assert!(info.contains("master_link_status:down"), "{what}: {info}");
    }
}
