// tests/cluster.rs
//
// Cluster mode, end to end: real `kividb` processes, each in its own working
// directory, talking over real sockets. Processes rather than in-process
// servers because the interesting cases involve a node *dying* — failover,
// rejoining after an outage, restarting as a replica — and a thread cannot be
// killed.
//
// Every node runs with a short `cluster-node-timeout` so failure detection
// and elections finish in a few seconds.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const NODE_TIMEOUT_MS: u64 = 2000;

// ── Minimal RESP client ───────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum Resp {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Vec<u8>),
    Array(Vec<Resp>),
    Map(Vec<(Resp, Resp)>),
    Nil,
}

impl Resp {
    fn text(&self) -> String {
        match self {
            Resp::Simple(s) | Resp::Error(s) => s.clone(),
            Resp::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
            Resp::Int(n) => n.to_string(),
            other => panic!("not text: {other:?}"),
        }
    }
    fn int(&self) -> i64 {
        match self {
            Resp::Int(n) => *n,
            other => panic!("not an integer: {other:?}"),
        }
    }
    fn array(&self) -> &[Resp] {
        match self {
            Resp::Array(v) => v,
            other => panic!("not an array: {other:?}"),
        }
    }
    fn map(&self) -> &[(Resp, Resp)] {
        match self {
            Resp::Map(v) => v,
            other => panic!("not a map: {other:?}"),
        }
    }
    fn is_ok(&self) -> bool {
        matches!(self, Resp::Simple(s) if s == "OK")
    }
    fn err(&self) -> Option<&str> {
        match self {
            Resp::Error(e) => Some(e),
            _ => None,
        }
    }
}

struct Conn {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

impl Conn {
    fn try_connect(port: u16) -> Option<Self> {
        let stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .ok()?;
        let reader = BufReader::new(stream.try_clone().ok()?);
        Some(Self { stream, reader })
    }

    fn connect(port: u16) -> Self {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(c) = Self::try_connect(port) {
                return c;
            }
            assert!(
                Instant::now() < deadline,
                "could not connect to node on {port}"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn try_cmd(&mut self, args: &[&str]) -> std::io::Result<Resp> {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            buf.extend_from_slice(a.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        self.stream.write_all(&buf)?;
        self.read()
    }

    /// A command whose arguments need not be text.
    fn cmd_bytes(&mut self, args: &[&[u8]]) -> Resp {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            buf.extend_from_slice(a);
            buf.extend_from_slice(b"\r\n");
        }
        self.stream.write_all(&buf).unwrap();
        self.read().unwrap()
    }

    fn cmd(&mut self, args: &[&str]) -> Resp {
        self.try_cmd(args)
            .unwrap_or_else(|e| panic!("{args:?} failed: {e}"))
    }

    /// Several commands in one write, so a timeout test can fill an index
    /// without a round trip per document.
    fn pipeline(&mut self, commands: &[Vec<&str>]) -> Vec<Resp> {
        let mut buf = Vec::new();
        for args in commands {
            buf.extend(format!("*{}\r\n", args.len()).into_bytes());
            for a in args {
                buf.extend(format!("${}\r\n", a.len()).as_bytes());
                buf.extend(a.as_bytes());
                buf.extend(b"\r\n");
            }
        }
        self.stream.write_all(&buf).unwrap();
        commands
            .iter()
            .map(|_| self.read().unwrap_or_else(|e| panic!("pipeline: {e}")))
            .collect()
    }

    fn read(&mut self) -> std::io::Result<Resp> {
        let mut line = String::new();
        if self.reader.read_line(&mut line)? == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        let line = line.trim_end_matches(['\r', '\n']);
        let (kind, rest) = line.split_at(1);
        Ok(match kind {
            "+" => Resp::Simple(rest.to_string()),
            "-" => Resp::Error(rest.to_string()),
            ":" => Resp::Int(rest.parse().unwrap()),
            "$" => {
                let len: i64 = rest.parse().unwrap();
                if len < 0 {
                    Resp::Nil
                } else {
                    let mut data = vec![0u8; len as usize + 2];
                    self.reader.read_exact(&mut data)?;
                    data.truncate(len as usize);
                    Resp::Bulk(data)
                }
            }
            "*" | ">" => {
                let n: i64 = rest.parse().unwrap();
                if n < 0 {
                    Resp::Nil
                } else {
                    let mut items = Vec::with_capacity(n as usize);
                    for _ in 0..n {
                        items.push(self.read()?);
                    }
                    Resp::Array(items)
                }
            }
            "%" => {
                let n: i64 = rest.parse().unwrap();
                let mut pairs = Vec::with_capacity(n.max(0) as usize);
                for _ in 0..n.max(0) {
                    pairs.push((self.read()?, self.read()?));
                }
                Resp::Map(pairs)
            }
            "," => Resp::Simple(rest.to_string()),
            "_" => Resp::Nil,
            other => panic!("unexpected RESP type {other:?} in {line:?}"),
        })
    }
}

// ── Node fixture ──────────────────────────────────────────────────────────────

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

/// A port whose bus port (`+10000`) is also free — and neither of them one
/// the system hands out to outgoing connections. The wait for a node to come
/// up dials both ports over and over; were either in the ephemeral range, one
/// of those dials could be given the port itself as its own end, connect to
/// itself, and hold the port against the node that is about to bind it.
/// (Linux hands out 32768 and up; Windows 49152 and up.)
fn cluster_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(0);
    let from = (std::process::id() as u16).wrapping_mul(31);
    loop {
        let step = NEXT.fetch_add(1, Ordering::Relaxed);
        // 10000–19999, so the bus is 20000–29999.
        let port = 10_000 + from.wrapping_add(step) % 10_000;
        let both_free = port_is_free(port) && port_is_free(port + 10_000);
        if both_free {
            return port;
        }
    }
}

struct Node {
    port: u16,
    dir: PathBuf,
    child: Option<Child>,
    extra: Vec<String>,
    /// Environment variables of its process, besides the test's own.
    env: Vec<(String, String)>,
}

impl Node {
    fn start(extra: &[&str]) -> Self {
        Self::start_with_env(extra, &[])
    }

    fn start_with_env(extra: &[&str], env: &[(&str, &str)]) -> Self {
        let dir = tempfile::tempdir().unwrap().keep();
        let mut node = Node {
            port: cluster_port(),
            dir,
            child: None,
            extra: extra.iter().map(|s| s.to_string()).collect(),
            env: env
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        };
        node.spawn();
        node
    }

    fn spawn(&mut self) {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join("node.log"))
            .unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_kividb"))
            .current_dir(&self.dir)
            .args([
                "--port",
                &self.port.to_string(),
                "--bind",
                "127.0.0.1",
                "--io-model",
                "tokio",
                "--threads",
                "2",
                "--cluster-enabled",
                "yes",
                "--cluster-node-timeout",
                &NODE_TIMEOUT_MS.to_string(),
                "--configfile",
                "kividb.conf",
            ])
            .args(&self.extra)
            .envs(self.env.iter().map(|(k, v)| (k, v)))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(log))
            .spawn()
            .expect("could not start kividb");
        self.child = Some(child);
        // Wait for both the client port and the bus. A new node writes its
        // nodes.conf durably before it listens, and on a disk every other
        // test is also syncing to (a container's overlay filesystem, the
        // whole suite at once) that has been seen to take 9 s.
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let up = TcpStream::connect(("127.0.0.1", self.port)).is_ok()
                && TcpStream::connect(("127.0.0.1", self.port + 10_000)).is_ok();
            if up {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "node on {} did not come up:\n{}",
                self.port,
                self.log()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// Kill the process outright — no shutdown, no final snapshot.
    fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    fn restart(&mut self) {
        self.kill();
        self.spawn();
    }

    fn conn(&self) -> Conn {
        Conn::connect(self.port)
    }

    fn id(&self) -> String {
        self.conn().cmd(&["CLUSTER", "MYID"]).text()
    }

    fn log(&self) -> String {
        std::fs::read_to_string(self.dir.join("node.log")).unwrap_or_default()
    }

    /// One field of `CLUSTER INFO`.
    fn cluster_info(&self, field: &str) -> String {
        info_field(&self.conn().cmd(&["CLUSTER", "INFO"]).text(), field)
            .unwrap_or_else(|| panic!("no {field} in CLUSTER INFO"))
    }

    /// One field of `INFO replication`, if present.
    fn repl_info(&self, field: &str) -> Option<String> {
        info_field(&self.conn().cmd(&["INFO", "replication"]).text(), field)
    }

    /// This node's own row of `CLUSTER NODES`, split on spaces.
    fn my_row(&self) -> Vec<String> {
        let nodes = self.conn().cmd(&["CLUSTER", "NODES"]).text();
        nodes
            .lines()
            .find(|l| l.split(' ').nth(2).is_some_and(|f| f.contains("myself")))
            .unwrap_or_else(|| panic!("no myself row in:\n{nodes}"))
            .split(' ')
            .map(String::from)
            .collect()
    }

    fn is_master(&self) -> bool {
        self.my_row()[2].contains("master")
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.kill();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn info_field(text: &str, field: &str) -> Option<String> {
    text.lines().find_map(|l| {
        l.strip_prefix(field)
            .and_then(|rest| rest.strip_prefix(':'))
            .map(|v| v.trim().to_string())
    })
}

fn wait_until(what: &str, timeout: Duration, mut pred: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if pred() {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("timed out after {timeout:?} waiting for: {what}");
}

/// CRC16/XMODEM, as used for hash slots.
fn slot_of(key: &str) -> u16 {
    let mut crc: u16 = 0;
    for &b in key.as_bytes() {
        crc ^= (b as u16) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
        }
    }
    crc % 16384
}

/// A key (of the form `prefix:N`) that hashes into `range`.
fn key_in(range: std::ops::RangeInclusive<u16>, prefix: &str) -> String {
    (0..100_000)
        .map(|i| format!("{prefix}:{i}"))
        .find(|k| range.contains(&slot_of(k)))
        .expect("a key in the slot range")
}

/// `masters` nodes sharing the slot space evenly, all introduced to each
/// other and agreeing the cluster is up. Returns the nodes and each one's
/// inclusive slot range.
fn form_cluster(masters: usize) -> (Vec<Node>, Vec<(u16, u16)>) {
    let nodes: Vec<Node> = (0..masters).map(|_| Node::start(&[])).collect();
    let per = 16384 / masters;
    let mut ranges = Vec::new();
    for (i, node) in nodes.iter().enumerate() {
        let start = (i * per) as u16;
        let end = if i == masters - 1 {
            16383
        } else {
            ((i + 1) * per - 1) as u16
        };
        let r = node.conn().cmd(&[
            "CLUSTER",
            "ADDSLOTSRANGE",
            &start.to_string(),
            &end.to_string(),
        ]);
        assert!(r.is_ok(), "ADDSLOTSRANGE: {r:?}");
        ranges.push((start, end));
    }
    for node in &nodes[1..] {
        let r = nodes[0]
            .conn()
            .cmd(&["CLUSTER", "MEET", "127.0.0.1", &node.port.to_string()]);
        assert!(r.is_ok(), "MEET: {r:?}");
    }
    wait_cluster_ok(&nodes.iter().collect::<Vec<_>>(), masters);
    (nodes, ranges)
}

/// Wait until every node in `nodes` knows `known` nodes and reports the
/// cluster as up.
fn wait_cluster_ok(nodes: &[&Node], known: usize) {
    wait_until("cluster to converge", Duration::from_secs(30), || {
        nodes.iter().all(|n| {
            n.cluster_info("cluster_state") == "ok"
                && n.cluster_info("cluster_known_nodes") == known.to_string()
        })
    });
}

/// Attach a fresh node as a replica of `master` and wait for it to sync.
fn add_replica(cluster: &[Node], master: &Node) -> Node {
    add_replica_with_env(cluster, master, &[])
}

/// [`add_replica`], its process given `env` as well.
fn add_replica_with_env(cluster: &[Node], master: &Node, env: &[(&str, &str)]) -> Node {
    let replica = Node::start_with_env(&[], env);
    let r = cluster[0]
        .conn()
        .cmd(&["CLUSTER", "MEET", "127.0.0.1", &replica.port.to_string()]);
    assert!(r.is_ok(), "MEET: {r:?}");
    let master_id = master.id();
    wait_until(
        "replica to learn of its master",
        Duration::from_secs(15),
        || {
            // The master's id is also in its other replicas' rows, as their
            // master, so look for the row that starts with it.
            replica
                .conn()
                .cmd(&["CLUSTER", "NODES"])
                .text()
                .lines()
                .any(|l| l.starts_with(&master_id))
        },
    );
    let r = replica.conn().cmd(&["CLUSTER", "REPLICATE", &master_id]);
    assert!(r.is_ok(), "REPLICATE: {r:?}");
    wait_until("replica to sync", Duration::from_secs(20), || {
        replica.repl_info("master_link_status").as_deref() == Some("up")
    });
    replica
}

// ── Formation and routing ─────────────────────────────────────────────────────

#[test]
fn cluster_forms_and_routes_by_slot() {
    let (nodes, ranges) = form_cluster(3);

    for node in &nodes {
        assert_eq!(node.cluster_info("cluster_slots_assigned"), "16384");
        assert_eq!(node.cluster_info("cluster_slots_ok"), "16384");
        assert_eq!(node.cluster_info("cluster_slots_fail"), "0");
        assert_eq!(node.cluster_info("cluster_size"), "3");
        // As Redis: INFO has a Cluster section, in the default sections
        // and on its own; redis-cli --cluster refuses a node without it.
        for info in [
            node.conn().cmd(&["INFO", "cluster"]).text(),
            node.conn().cmd(&["INFO"]).text(),
        ] {
            assert!(info.contains("# Cluster\r\n"), "{info}");
            assert_eq!(info_field(&info, "cluster_enabled").as_deref(), Some("1"));
        }
    }

    // A key owned by node 1, asked of node 0, is redirected to node 1 — by
    // address, so the client can follow it.
    let key = key_in(ranges[1].0..=ranges[1].1, "k");
    let reply = nodes[0].conn().cmd(&["SET", &key, "v"]);
    let moved = reply.err().expect("a redirect");
    assert_eq!(
        moved,
        format!("MOVED {} 127.0.0.1:{}", slot_of(&key), nodes[1].port)
    );
    assert!(nodes[1].conn().cmd(&["SET", &key, "v"]).is_ok());
    assert_eq!(nodes[1].conn().cmd(&["GET", &key]).text(), "v");

    // Keys in different slots cannot share a command.
    let other = key_in(ranges[0].0..=ranges[0].1, "k");
    let reply = nodes[1].conn().cmd(&["MGET", &key, &other]);
    assert!(reply.err().is_some_and(|e| e.starts_with("CROSSSLOT")));

    // SELECT has nothing to select in a cluster.
    let reply = nodes[0].conn().cmd(&["SELECT", "1"]);
    assert!(reply.err().is_some());
    assert!(nodes[0].conn().cmd(&["SELECT", "0"]).is_ok());
}

/// Commands whose key is not the first argument, and commands the routing
/// table did not know, were routed by the wrong argument or not at all.
#[test]
fn routing_uses_the_real_key_positions() {
    let (nodes, ranges) = form_cluster(2);
    let key = key_in(ranges[1].0..=ranges[1].1, "q");
    let owner = &nodes[1];
    let stranger = &nodes[0];

    let moved = |reply: Resp| reply.err().is_some_and(|e| e.starts_with("MOVED"));

    // Served by the owner...
    assert_eq!(owner.conn().cmd(&["RPUSH", &key, "a", "b", "c"]).int(), 3);
    let popped = owner.conn().cmd(&["LMPOP", "1", &key, "LEFT"]);
    assert_eq!(popped.array()[0].text(), key);
    // ...and redirected by anyone else, whatever the first argument is.
    assert!(moved(stranger.conn().cmd(&["LMPOP", "1", &key, "LEFT"])));
    assert!(moved(stranger.conn().cmd(&["OBJECT", "ENCODING", &key])));
    assert!(moved(stranger.conn().cmd(&["DELEX", &key])));
    assert!(moved(
        stranger.conn().cmd(&["MSETEX", "1", &key, "v", "EX", "10"])
    ));
    assert!(moved(stranger.conn().cmd(&[
        "ZREMRANGEBYSCORE",
        &key,
        "0",
        "1"
    ])));
    assert!(moved(stranger.conn().cmd(&["EVAL", "return 1", "1", &key])));

    let stream = key_in(ranges[1].0..=ranges[1].1, "stream");
    assert!(moved(
        stranger
            .conn()
            .cmd(&["XGROUP", "CREATE", &stream, "g", "$", "MKSTREAM"])
    ));
    assert!(
        owner
            .conn()
            .cmd(&["XGROUP", "CREATE", &stream, "g", "$", "MKSTREAM"])
            .is_ok()
    );
}

/// A script may use only the keys this node serves, and, unless it is
/// flagged `allow-cross-slot-keys`, only those in its own slot: one that
/// wrote a key of another node's slot left it here, where no client would
/// find it (#153).
#[cfg(feature = "lua")]
#[test]
fn a_script_uses_only_keys_this_node_serves() {
    let (nodes, ranges) = form_cluster(2);
    let here = &nodes[0];
    let mine = key_in(ranges[0].0..=ranges[0].1, "mine");
    let also_mine = (0..100_000)
        .map(|i| format!("also:{i}"))
        .find(|k| (ranges[0].0..=ranges[0].1).contains(&slot_of(k)) && slot_of(k) != slot_of(&mine))
        .expect("another key of node 0");
    let theirs = key_in(ranges[1].0..=ranges[1].1, "theirs");
    let write = |key: &str| format!("return redis.call('SET', '{key}', 'v')");
    let non_local = |reply: &Resp| {
        reply.err().is_some_and(|e| {
            e.contains("Script attempted to access a non local key in a cluster node")
        })
    };
    let same_slot = |reply: &Resp| {
        reply.err().is_some_and(|e| {
            e.contains("Script attempted to access keys that do not hash to the same slot")
        })
    };

    // Its own slot: fine, declared or not.
    assert!(
        here.conn()
            .cmd(&["EVAL", &write(&mine), "1", &mine])
            .is_ok()
    );
    assert!(here.conn().cmd(&["EVAL", &write(&mine), "0"]).is_ok());

    // Another node's key, with keys of its own or none: refused, and not stored.
    let r = here.conn().cmd(&["EVAL", &write(&theirs), "1", &mine]);
    assert!(same_slot(&r), "{r:?}");
    let r = here.conn().cmd(&["EVAL", &write(&theirs), "0"]);
    assert!(non_local(&r), "{r:?}");
    let slot = slot_of(&theirs).to_string();
    assert_eq!(
        here.conn()
            .cmd(&["CLUSTER", "COUNTKEYSINSLOT", &slot])
            .int(),
        0
    );

    // A key of this node in another slot: refused, unless the script says it
    // may; another node's key is refused all the same.
    let r = here.conn().cmd(&["EVAL", &write(&also_mine), "1", &mine]);
    assert!(same_slot(&r), "{r:?}");
    let flagged = |body: String| format!("#!lua flags=allow-cross-slot-keys\n{body}");
    assert!(
        here.conn()
            .cmd(&["EVAL", &flagged(write(&also_mine)), "1", &mine])
            .is_ok()
    );
    let r = here
        .conn()
        .cmd(&["EVAL", &flagged(write(&theirs)), "1", &mine]);
    assert!(non_local(&r), "{r:?}");

    // A script with no keys takes the slot of the first one it uses.
    let two =
        format!("redis.call('SET', '{mine}', 'v') return redis.call('SET', '{also_mine}', 'v')");
    let r = here.conn().cmd(&["EVAL", &two, "0"]);
    assert!(same_slot(&r), "{r:?}");

    // A function is held to the same, by its flags.
    let library = format!(
        "#!lua name=lib\n\
         redis.register_function('strict', function(keys) return redis.call('SET', '{theirs}', 'v') end)\n\
         redis.register_function{{function_name='loose', callback=function(keys) return redis.call('SET', '{also_mine}', 'v') end, flags={{'allow-cross-slot-keys'}}}}"
    );
    assert_eq!(
        here.conn().cmd(&["FUNCTION", "LOAD", &library]).text(),
        "lib"
    );
    let r = here.conn().cmd(&["FCALL", "strict", "1", &mine]);
    assert!(same_slot(&r), "{r:?}");
    assert!(here.conn().cmd(&["FCALL", "loose", "1", &mine]).is_ok());
}

/// Six masters, introduced only to the first: each has to learn of the other
/// four through gossip. (They did not, where the clock was coarse: gossip
/// kept mentioning the same three nodes.)
#[test]
fn a_cluster_forms_when_only_one_node_is_introduced_to_the_rest() {
    let (nodes, _) = form_cluster(6);
    let ids: Vec<String> = nodes.iter().map(|n| n.id()).collect();
    for node in &nodes {
        let table = node.conn().cmd(&["CLUSTER", "NODES"]).text();
        for id in &ids {
            assert!(
                table.contains(id.as_str()),
                "{} does not know {id}",
                node.id()
            );
        }
        assert_eq!(node.cluster_info("cluster_size"), "6");
    }
    // And every node routes to every other.
    let mut c = nodes[5].conn();
    let key = key_in(0..=100, "far");
    let reply = c.cmd(&["SET", &key, "v"]);
    assert_eq!(
        reply.err().expect("a redirect"),
        format!("MOVED {} 127.0.0.1:{}", slot_of(&key), nodes[0].port)
    );
}

/// `COMMAND` tells a client where each command's keys are. redis-py's cluster
/// client routes by nothing else: with an empty reply it could not send a
/// single command.
#[test]
fn command_describes_where_each_commands_keys_are() {
    let node = Node::start(&[]);
    let mut c = node.conn();

    // [name, arity, flags, first-key, last-key, key-step, categories, …]
    let describe = |c: &mut Conn, name: &str| -> (String, Vec<String>, [i64; 3]) {
        let reply = c.cmd(&["COMMAND", "INFO", name]);
        let entry = reply.array()[0].array().to_vec();
        assert_eq!(entry.len(), 10, "{name}: {entry:?}");
        let flags = entry[2].array().iter().map(|f| f.text()).collect();
        (
            entry[0].text(),
            flags,
            [entry[3].int(), entry[4].int(), entry[5].int()],
        )
    };
    let has = |flags: &[String], flag: &str| flags.iter().any(|f| f == flag);

    let (name, flags, keys) = describe(&mut c, "get");
    assert_eq!((name.as_str(), keys), ("get", [1, 1, 1]));
    assert!(
        has(&flags, "readonly") && !has(&flags, "write"),
        "{flags:?}"
    );
    let (_, flags, keys) = describe(&mut c, "SET");
    assert_eq!(keys, [1, 1, 1]);
    assert!(has(&flags, "write"), "{flags:?}");
    assert_eq!(describe(&mut c, "MGET").2, [1, -1, 1]);
    assert_eq!(describe(&mut c, "RENAME").2, [1, 2, 1]);
    assert_eq!(describe(&mut c, "PING").2, [0, 0, 0]);
    // Applied to an invocation, the numbers give the keys: MSET k1 v1 k2 v2.
    let [first, last, step] = describe(&mut c, "MSET").2;
    let last = 5 + last;
    let keys: Vec<i64> = (first..=last).step_by(step as usize).collect();
    assert_eq!(keys, [1, 3]);
    // Keys that follow a count or an option cannot be described that way: the
    // flag sends the client to COMMAND GETKEYS, which can.
    for movable in ["EVAL", "ZUNIONSTORE", "XREAD", "LMPOP", "SORT"] {
        let (_, flags, _) = describe(&mut c, movable);
        assert!(has(&flags, "movablekeys"), "{movable}: {flags:?}");
    }
    let keys = c.cmd(&["COMMAND", "GETKEYS", "ZUNIONSTORE", "dst", "2", "a", "b"]);
    let keys: Vec<String> = keys.array().iter().map(|k| k.text()).collect();
    assert_eq!(keys, ["dst", "a", "b"]);
    assert!(has(&describe(&mut c, "PUBLISH").1, "pubsub"));
    assert!(has(&describe(&mut c, "BLPOP").1, "blocking"));

    // The whole table, and names that are not commands.
    let all = c.cmd(&["COMMAND"]);
    let count = c.cmd(&["COMMAND", "COUNT"]).int();
    assert_eq!(all.array().len() as i64, count);
    assert!(count > 300, "{count}");
    assert_eq!(c.cmd(&["COMMAND", "INFO"]).array().len() as i64, count);
    let mixed = c.cmd(&["COMMAND", "INFO", "get", "nosuchcommand"]);
    assert_eq!(mixed.array().len(), 2);
    assert_eq!(mixed.array()[1], Resp::Nil);
    // Listed with no command behind it, and not Redis's (#126).
    let gone = c.cmd(&["COMMAND", "INFO", "geodelbymember"]);
    assert_eq!(gone.array(), [Resp::Nil]);
}

/// A transaction is routed command by command as it is queued. One that
/// names another node's slot is refused there and then, and EXEC runs
/// nothing — not the commands that were fine, and not on the wrong node.
#[test]
fn a_transaction_is_routed_when_it_is_queued() {
    let (nodes, ranges) = form_cluster(2);
    let mine = key_in(ranges[0].0..=ranges[0].1, "mine");
    let also_mine = (0..100_000)
        .map(|i| format!("also:{i}"))
        .find(|k| slot_of(k) <= ranges[0].1 && slot_of(k) != slot_of(&mine))
        .unwrap();
    let theirs = key_in(ranges[1].0..=ranges[1].1, "theirs");
    let mut c = nodes[0].conn();

    // A command for the other node: refused at once, and EXEC aborts.
    assert!(c.cmd(&["MULTI"]).is_ok());
    assert_eq!(c.cmd(&["SET", &mine, "1"]).text(), "QUEUED");
    let refused = c.cmd(&["SET", &theirs, "1"]);
    assert_eq!(
        refused.err().expect("a redirect"),
        format!("MOVED {} 127.0.0.1:{}", slot_of(&theirs), nodes[1].port)
    );
    let exec = c.cmd(&["EXEC"]);
    assert!(
        exec.err().is_some_and(|e| e.starts_with("EXECABORT")),
        "{exec:?}"
    );
    assert_eq!(c.cmd(&["EXISTS", &mine]).int(), 0, "nothing may have run");
    assert_eq!(nodes[0].conn().cmd(&["DBSIZE"]).int(), 0);
    assert_eq!(nodes[1].conn().cmd(&["DBSIZE"]).int(), 0);

    // The connection is out of the transaction and usable.
    assert!(c.cmd(&["SET", &mine, "plain"]).is_ok());

    // Keys in one slot: the transaction runs.
    let tagged = format!("{{{mine}}}:other");
    assert!(c.cmd(&["MULTI"]).is_ok());
    assert_eq!(c.cmd(&["SET", &mine, "2"]).text(), "QUEUED");
    assert_eq!(c.cmd(&["SET", &tagged, "3"]).text(), "QUEUED");
    assert_eq!(c.cmd(&["PING"]).text(), "QUEUED");
    assert_eq!(c.cmd(&["EXEC"]).array().len(), 3);
    assert_eq!(c.cmd(&["GET", &tagged]).text(), "3");

    // Two slots, both served here: each command is fine on its own, the
    // transaction as a whole is not.
    assert!(c.cmd(&["MULTI"]).is_ok());
    assert_eq!(c.cmd(&["SET", &mine, "4"]).text(), "QUEUED");
    assert_eq!(c.cmd(&["SET", &also_mine, "4"]).text(), "QUEUED");
    let exec = c.cmd(&["EXEC"]);
    assert!(
        exec.err().is_some_and(|e| e.starts_with("CROSSSLOT")),
        "{exec:?}"
    );
    assert_eq!(c.cmd(&["GET", &mine]).text(), "2");
    assert_eq!(c.cmd(&["EXISTS", &also_mine]).int(), 0);

    // A cross-slot command is refused when queued, like any other error.
    assert!(c.cmd(&["MULTI"]).is_ok());
    let cross = c.cmd(&["MSET", &mine, "5", &also_mine, "5"]);
    assert!(
        cross.err().is_some_and(|e| e.starts_with("CROSSSLOT")),
        "{cross:?}"
    );
    assert!(c.cmd(&["DISCARD"]).is_ok());
    // DISCARD clears the abort: the next transaction starts clean.
    assert!(c.cmd(&["MULTI"]).is_ok());
    assert_eq!(c.cmd(&["INCR", &also_mine]).text(), "QUEUED");
    assert_eq!(c.cmd(&["EXEC"]).array()[0].int(), 1);
}

// ── Replication inside a cluster ──────────────────────────────────────────────

#[test]
fn replica_follows_its_master_and_serves_reads_after_readonly() {
    let (nodes, ranges) = form_cluster(2);
    let master = &nodes[0];
    let key = key_in(ranges[0].0..=ranges[0].1, "r");
    assert!(master.conn().cmd(&["SET", &key, "v"]).is_ok());

    let replica = add_replica(&nodes, master);
    let all: Vec<&Node> = nodes.iter().chain(std::iter::once(&replica)).collect();
    wait_cluster_ok(&all, 3);
    assert_eq!(replica.repl_info("role").as_deref(), Some("slave"));
    assert!(!replica.is_master());

    // Replicas do not count toward the cluster's size.
    assert_eq!(master.cluster_info("cluster_size"), "2");

    // An array with a `CLUSTER NODES` line per replica, as in Redis; it was
    // one string (#138). A master without replicas has an empty one.
    let replicas = master.conn().cmd(&["CLUSTER", "REPLICAS", &master.id()]);
    assert_eq!(replicas.array().len(), 1, "{replicas:?}");
    let line = replicas.array()[0].text();
    assert!(
        line.starts_with(&replica.id()) && !line.ends_with('\n'),
        "{line:?}"
    );
    let none = master.conn().cmd(&["CLUSTER", "SLAVES", &nodes[1].id()]);
    assert!(none.array().is_empty(), "{none:?}");

    // Busiest slot first by default, as in Redis; it was the emptiest (#139).
    let stats = master.conn().cmd(&[
        "CLUSTER",
        "SLOT-STATS",
        "ORDERBY",
        "key-count",
        "LIMIT",
        "1",
    ]);
    let top = &stats.array()[0].array();
    assert_eq!(top[0].int(), slot_of(&key) as i64, "{stats:?}");

    // Without READONLY, the replica redirects to its master.
    let mut c = replica.conn();
    let reply = c.cmd(&["GET", &key]);
    assert!(
        reply.err().is_some_and(|e| e.starts_with("MOVED")),
        "{reply:?}"
    );
    // With it, it serves the read itself...
    assert!(c.cmd(&["READONLY"]).is_ok());
    assert_eq!(c.cmd(&["GET", &key]).text(), "v");
    // ...but never a write.
    let reply = c.cmd(&["SET", &key, "x"]);
    assert!(
        reply.err().is_some_and(|e| e.starts_with("MOVED")),
        "{reply:?}"
    );
    // READWRITE turns it back off.
    assert!(c.cmd(&["READWRITE"]).is_ok());
    assert!(c.cmd(&["GET", &key]).err().is_some());

    // REPLICAOF would change the topology behind the cluster's back.
    let reply = replica.conn().cmd(&["REPLICAOF", "NO", "ONE"]);
    assert!(reply.err().is_some_and(|e| e.contains("cluster mode")));
}

/// A cluster replica that restarts resumes following its master. On the
/// default I/O model this used to panic at startup.
#[test]
fn cluster_replica_survives_a_restart() {
    let (nodes, ranges) = form_cluster(2);
    let master = &nodes[0];
    let mut replica = add_replica(&nodes, master);
    let key = key_in(ranges[0].0..=ranges[0].1, "s");

    replica.restart();
    wait_until(
        "replica to resync after restart",
        Duration::from_secs(20),
        || replica.repl_info("master_link_status").as_deref() == Some("up"),
    );
    assert!(!replica.is_master());

    assert!(master.conn().cmd(&["SET", &key, "after-restart"]).is_ok());
    let mut c = replica.conn();
    assert!(c.cmd(&["READONLY"]).is_ok());
    wait_until(
        "the write to reach the restarted replica",
        Duration::from_secs(10),
        || matches!(c.cmd(&["GET", &key]), Resp::Bulk(v) if v == b"after-restart"),
    );
}

/// The whole cluster stopped and started again, after longer than the node
/// timeout: nobody is failed and nobody fails over. The last-heard times in
/// nodes.conf are from before the stop; a node used to judge its peers by
/// them, suspect every one of them at once, and with the others doing the
/// same, fail a master that was only starting up. Its replica then took
/// over, and the old master rejoined as a replica and resynced in full.
#[test]
fn a_cluster_restarted_whole_does_not_fail_anyone_over() {
    let (mut nodes, _) = form_cluster(3);
    let replica = add_replica(&nodes, &nodes[0]);
    nodes.push(replica);
    for node in &mut nodes {
        node.kill();
    }
    std::thread::sleep(Duration::from_millis(NODE_TIMEOUT_MS + 1000));
    // The replica first: it is the one that would run for election.
    for node in nodes.iter_mut().rev() {
        node.spawn();
    }
    let all: Vec<&Node> = nodes.iter().collect();
    wait_cluster_ok(&all, 4);
    // Long enough for a wrong verdict to be reached and acted on.
    std::thread::sleep(Duration::from_millis(3 * NODE_TIMEOUT_MS));
    for node in &nodes {
        let log = node.log();
        assert!(!log.contains("marked FAIL"), "{log}");
        let row = node.my_row();
        assert!(!row[2].contains("fail"), "{row:?}");
    }
    assert!(nodes[0].is_master() && !nodes[3].is_master());
}

/// `cluster-announce-hostname` and `cluster-preferred-endpoint-type`, as in
/// Redis 7: every node learns every hostname, CLUSTER NODES, SLOTS and
/// SHARDS show them, and with `hostname` preferred clients are redirected by
/// hostname. A restarted node remembers the hostnames it had learnt.
#[test]
fn nodes_announce_hostnames_and_clients_can_be_sent_by_them() {
    let (mut nodes, ranges) = form_cluster(3);
    let host = |i: usize| format!("node-{i}.cluster.example");
    for (i, node) in nodes.iter().enumerate() {
        let r = node
            .conn()
            .cmd(&["CONFIG", "SET", "cluster-announce-hostname", &host(i)]);
        assert!(r.is_ok(), "{r:?}");
    }
    let bad = nodes[0]
        .conn()
        .cmd(&["CONFIG", "SET", "cluster-announce-hostname", "no,commas"]);
    assert!(bad.err().is_some(), "{bad:?}");
    wait_until(
        "every node to learn every hostname",
        Duration::from_secs(15),
        || {
            nodes.iter().all(|n| {
                let text = n.conn().cmd(&["CLUSTER", "NODES"]).text();
                (0..3).all(|i| text.contains(&format!(",{} ", host(i))))
            })
        },
    );

    // The address stays the default endpoint; the hostname is extra.
    let mut c = nodes[0].conn();
    let slots = c.cmd(&["CLUSTER", "SLOTS"]);
    let first = &slots.array()[0].array()[2];
    assert_eq!(first.array()[0].text(), "127.0.0.1");
    let meta = first.array()[3].array();
    assert_eq!(meta[0].text(), "hostname");
    assert!(meta[1].text().starts_with("node-"));

    assert!(
        c.cmd(&[
            "CONFIG",
            "SET",
            "cluster-preferred-endpoint-type",
            "hostname"
        ])
        .is_ok()
    );
    assert_eq!(
        c.cmd(&["CONFIG", "GET", "cluster-preferred-endpoint-type"])
            .array()[1]
            .text(),
        "hostname"
    );
    let key = key_in(ranges[1].0..=ranges[1].1, "h");
    let moved = c.cmd(&["GET", &key]).err().unwrap().to_string();
    assert!(
        moved.ends_with(&format!(" {}:{}", host(1), nodes[1].port)),
        "{moved}"
    );
    let slots = c.cmd(&["CLUSTER", "SLOTS"]);
    let first = &slots.array()[0].array()[2];
    assert!(first.array()[0].text().starts_with("node-"));
    let meta = first.array()[3].array();
    assert_eq!(meta[0].text(), "ip");
    assert_eq!(meta[1].text(), "127.0.0.1");
    fn texts(r: &Resp, out: &mut Vec<String>) {
        match r {
            Resp::Array(items) => items.iter().for_each(|i| texts(i, out)),
            Resp::Bulk(b) => out.push(String::from_utf8_lossy(b).into_owned()),
            _ => {}
        }
    }
    let mut shards = Vec::new();
    texts(&c.cmd(&["CLUSTER", "SHARDS"]), &mut shards);
    assert!(
        shards.iter().any(|s| s == "hostname") && shards.contains(&host(2)),
        "{shards:?}"
    );

    // Remembered over a restart, before any peer has been heard from again;
    // its own, as Redis, from its config file.
    assert!(nodes[2].conn().cmd(&["CONFIG", "REWRITE"]).is_ok());
    nodes[2].restart();
    let text = nodes[2].conn().cmd(&["CLUSTER", "NODES"]).text();
    assert!(text.contains(&format!(",{} ", host(0))), "{text}");
    assert!(text.contains(&format!(",{} ", host(2))), "{text}");
}

/// `cluster-announce-client-port`: what clients are told to connect to,
/// where it is not the port nodes reach each other on (a NodePort, a load
/// balancer that maps ports) (#110). Redirects, `CLUSTER SLOTS`, `SHARDS` and
/// `NODES` give it; the cluster itself keeps working on the real ports.
#[test]
fn nodes_announce_a_client_port_of_their_own() {
    let (nodes, ranges) = form_cluster(3);
    let r = nodes[1]
        .conn()
        .cmd(&["CONFIG", "SET", "cluster-announce-client-port", "30001"]);
    assert!(r.is_ok(), "{r:?}");
    assert_eq!(
        nodes[1]
            .conn()
            .cmd(&["CONFIG", "GET", "cluster-announce-client-port"])
            .array()[1]
            .text(),
        "30001"
    );
    let mut c = nodes[0].conn();
    let key = key_in(ranges[1].0..=ranges[1].1, "cp");
    wait_until(
        "the others to tell clients the new port",
        Duration::from_secs(15),
        || {
            c.cmd(&["GET", &key])
                .err()
                .is_some_and(|e| e.ends_with(" 127.0.0.1:30001"))
        },
    );
    fn texts(r: &Resp, out: &mut Vec<String>) {
        match r {
            Resp::Array(items) => items.iter().for_each(|i| texts(i, out)),
            Resp::Bulk(b) => out.push(String::from_utf8_lossy(b).into_owned()),
            Resp::Int(n) => out.push(n.to_string()),
            _ => {}
        }
    }
    let mut slots = Vec::new();
    texts(&c.cmd(&["CLUSTER", "SLOTS"]), &mut slots);
    assert!(slots.contains(&"30001".to_string()), "{slots:?}");
    let mut shards = Vec::new();
    texts(&c.cmd(&["CLUSTER", "SHARDS"]), &mut shards);
    assert!(shards.contains(&"30001".to_string()), "{shards:?}");
    let text = c.cmd(&["CLUSTER", "NODES"]).text();
    assert!(
        text.contains(&format!("{} 127.0.0.1:30001@", nodes[1].id())),
        "{text}"
    );
    // The cluster itself still reaches it on its real port.
    assert_eq!(nodes[1].cluster_info("cluster_state"), "ok");
    assert_eq!(nodes[0].cluster_info("cluster_state"), "ok");

    // Back to none: the client port again.
    assert!(
        nodes[1]
            .conn()
            .cmd(&["CONFIG", "SET", "cluster-announce-client-port", "0"])
            .is_ok()
    );
    let real = format!(" 127.0.0.1:{}", nodes[1].port);
    wait_until(
        "clients to be sent to the real port again",
        Duration::from_secs(15),
        || {
            c.cmd(&["GET", &key])
                .err()
                .is_some_and(|e| e.ends_with(&real))
        },
    );
}

/// `CONFIG GET` while this node's entry in the node table is being written
/// does not stop the node. It read the table twice in one statement, still
/// holding the first read when it took the second, and a writer that queued
/// in between waited for the first while the second waited for the writer:
/// for good, and every command that reads the topology queued behind them.
/// In a cluster the writer is the bus, on every heartbeat; the operator asks
/// `CONFIG GET cluster-announce-hostname` of every node it looks after. Here
/// `CONFIG SET cluster-announce-client-port` writes, to hit it in seconds.
#[test]
fn config_get_while_the_node_table_is_written_does_not_hang_the_node() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    let node = Node::start(&[]);
    let stop = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicU64::new(0));
    let mut workers = Vec::new();
    for i in 0..8 {
        let (stop, done, port) = (Arc::clone(&stop), Arc::clone(&done), node.port);
        workers.push(std::thread::spawn(move || {
            let mut c = Conn::connect(port);
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                n += 1;
                let port = (30_000 + n % 2).to_string();
                let args: &[&str] = if i % 4 == 0 {
                    &["CONFIG", "SET", "cluster-announce-client-port", &port]
                } else {
                    &["CONFIG", "GET", "cluster-announce-hostname"]
                };
                // A reply that does not come (10 s) ends the loop.
                if c.try_cmd(args).is_err() {
                    return;
                }
                done.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    std::thread::sleep(Duration::from_secs(5));
    stop.store(true, Ordering::Relaxed);
    for w in workers {
        let _ = w.join();
    }
    let mut c = Conn::connect(node.port);
    c.stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let info = c.try_cmd(&["CLUSTER", "INFO"]);
    assert!(
        info.is_ok(),
        "the node stopped answering after {} commands: {info:?}",
        done.load(Ordering::Relaxed)
    );
    assert_eq!(c.cmd(&["PING"]), Resp::Simple("PONG".into()));
}

/// Heartbeats go over one connection per peer, kept open (#112): pinging
/// every peer every second used to open a new connection each time, N x
/// (N - 1) a second across the cluster. A peer that restarts is reached
/// again on a new one.
#[test]
fn heartbeats_reuse_one_connection_per_peer() {
    let (mut nodes, _) = form_cluster(6);
    let stat = |n: &Node, f: &str| -> u64 { n.cluster_info(f).parse().unwrap_or(0) };
    // Let every link come up.
    std::thread::sleep(Duration::from_secs(3));
    let (connects, sent) = (
        stat(&nodes[0], "cluster_stats_bus_connects"),
        stat(&nodes[0], "cluster_stats_messages_sent"),
    );
    std::thread::sleep(Duration::from_secs(5));
    let new_connects = stat(&nodes[0], "cluster_stats_bus_connects") - connects;
    let new_messages = stat(&nodes[0], "cluster_stats_messages_sent") - sent;
    // Five peers, five seconds: 25 pings, on connections already open.
    assert!(new_messages >= 20, "{new_messages} messages sent");
    assert!(
        new_connects <= 2,
        "{new_connects} connections opened in 5 s"
    );

    nodes[3].restart();
    wait_cluster_ok(&nodes.iter().collect::<Vec<_>>(), 6);
    wait_until(
        "the restarted node to be heard from again",
        Duration::from_secs(20),
        || {
            let id = nodes[3].id();
            nodes[0]
                .conn()
                .cmd(&["CLUSTER", "NODES"])
                .text()
                .lines()
                .find(|l| l.starts_with(&id))
                .is_some_and(|l| !l.contains("fail"))
        },
    );
}

/// `cluster-bus-secret`: nodes that share it form a cluster; a node with
/// another secret, or none, is never let onto the bus, so a MEET with it
/// goes nowhere. With a secret, protected mode lets a node start.
#[test]
fn only_nodes_with_the_bus_secret_join() {
    let with = |secret: &str| {
        Node::start(&[
            "--cluster-bus-secret",
            secret,
            "--cluster-bus-port-protected-mode",
            "yes",
        ])
    };
    let a = with("shared-s3cret");
    let b = with("shared-s3cret");
    assert!(
        a.conn()
            .cmd(&["CLUSTER", "ADDSLOTSRANGE", "0", "8191"])
            .is_ok()
    );
    assert!(
        b.conn()
            .cmd(&["CLUSTER", "ADDSLOTSRANGE", "8192", "16383"])
            .is_ok()
    );
    let meet = |from: &Node, to: &Node| {
        from.conn()
            .cmd(&["CLUSTER", "MEET", "127.0.0.1", &to.port.to_string()])
    };
    assert!(meet(&a, &b).is_ok());
    wait_cluster_ok(&[&a, &b], 2);
    let secret = a.conn().cmd(&["CONFIG", "GET", "cluster-bus-secret"]);
    assert_eq!(secret.array()[1].text(), "**redacted**");

    let wrong = with("another-secret");
    let none = Node::start(&[]);
    assert!(meet(&a, &wrong).is_ok());
    assert!(meet(&a, &none).is_ok());
    assert!(meet(&wrong, &a).is_ok());
    assert!(meet(&none, &b).is_ok());
    std::thread::sleep(Duration::from_millis(3 * NODE_TIMEOUT_MS));
    for node in [&a, &b] {
        assert_eq!(node.cluster_info("cluster_known_nodes"), "2");
        assert_eq!(node.cluster_info("cluster_state"), "ok");
    }
    for node in [&wrong, &none] {
        assert_eq!(node.cluster_info("cluster_known_nodes"), "1");
    }
    assert!(a.log().contains("Refused a bus connection"), "{}", a.log());
}

/// An index made, or dropped, on one master is made or dropped on all of
/// them, so that each indexes its own keys; a search answers from the keys
/// of the node it is sent to. A repeat fills in a node that lacks it.
#[test]
fn index_definitions_reach_every_master() {
    let (nodes, ranges) = form_cluster(3);
    let lists = |name: &str| {
        nodes.iter().all(|n| {
            n.conn()
                .cmd(&["FT._LIST"])
                .array()
                .iter()
                .any(|i| i.text() == name)
        })
    };
    let r = nodes[0].conn().cmd(&[
        "FT.CREATE",
        "docs",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "doc:",
        "SCHEMA",
        "title",
        "TEXT",
    ]);
    assert!(r.is_ok(), "{r:?}");
    assert!(lists("docs"));

    // Each master indexes the keys it holds.
    for (node, range) in nodes.iter().zip(&ranges) {
        let key = key_in(range.0..=range.1, "doc");
        let r = node.conn().cmd(&["HSET", &key, "title", "hello"]);
        assert_eq!(r.int(), 1, "{r:?}");
    }
    // A search sent to any of them is answered from all of them.
    for node in &nodes {
        let found = node.conn().cmd(&["FT.SEARCH", "docs", "hello"]);
        assert_eq!(found.array()[0].int(), 3, "{found:?}");
    }

    // Made on one node only, as by a node that missed it ...
    let mut lone = nodes[2].conn();
    assert!(
        lone.cmd(&["CLIENT", "SETNAME", "kividb-cluster-ddl"])
            .is_ok()
    );
    assert!(
        lone.cmd(&["FT.CREATE", "late", "ON", "HASH", "SCHEMA", "f", "TEXT"])
            .is_ok()
    );
    assert!(!lists("late"));
    // ... and filled in elsewhere by a repeat, wherever it is run.
    let r = nodes[0]
        .conn()
        .cmd(&["FT.CREATE", "late", "ON", "HASH", "SCHEMA", "f", "TEXT"]);
    assert!(r.is_ok(), "{r:?}");
    assert!(lists("late"));
    let again = nodes[2]
        .conn()
        .cmd(&["FT.CREATE", "late", "ON", "HASH", "SCHEMA", "f", "TEXT"]);
    assert!(again.err().is_some_and(|e| e.contains("already exists")));

    let r = nodes[1].conn().cmd(&["FT.DROPINDEX", "docs"]);
    assert!(r.is_ok(), "{r:?}");
    assert!(nodes.iter().all(|n| {
        n.conn()
            .cmd(&["FT._LIST"])
            .array()
            .iter()
            .all(|i| i.text() != "docs")
    }));
}

// ── Failover ──────────────────────────────────────────────────────────────────

/// The whole arc: a master dies, the other masters agree it has failed, its
/// replica is elected and takes the slots, the cluster comes back up with the
/// data intact — and when the old master returns, it finds it has been
/// replaced and rejoins as a replica instead of serving a stale dataset.
#[test]
fn failover_promotes_the_replica_and_the_old_master_rejoins_as_a_replica() {
    let (mut nodes, ranges) = form_cluster(3);
    let replica = add_replica(&nodes, &nodes[0]);
    {
        let all: Vec<&Node> = nodes.iter().chain(std::iter::once(&replica)).collect();
        wait_cluster_ok(&all, 4);
    }

    let key = key_in(ranges[0].0..=ranges[0].1, "f");
    let ttl_key = key_in(ranges[0].0..=ranges[0].1, "fttl");
    assert!(nodes[0].conn().cmd(&["SET", &key, "precious"]).is_ok());
    assert!(
        nodes[0]
            .conn()
            .cmd(&["SET", &ttl_key, "v", "EX", "1000"])
            .is_ok()
    );
    assert_eq!(nodes[0].conn().cmd(&["WAIT", "1", "5000"]).int(), 1);

    let old_master_id = nodes[0].id();
    nodes[0].kill();

    // While the master is down and nobody has replaced it, the cluster is
    // down — not "ok" with clients redirected to a dead address.
    wait_until(
        "survivors to notice the failure",
        Duration::from_secs(20),
        || nodes[1].cluster_info("cluster_slots_fail") != "0" || replica.is_master(),
    );

    wait_until(
        "the replica to be promoted",
        Duration::from_secs(30),
        || replica.is_master(),
    );
    wait_until(
        "the cluster to be up again",
        Duration::from_secs(30),
        || {
            [&nodes[1], &nodes[2], &replica]
                .iter()
                .all(|n| n.cluster_info("cluster_state") == "ok")
        },
    );
    assert_eq!(replica.repl_info("role").as_deref(), Some("master"));

    // The promoted replica has the data, TTLs included, and takes writes.
    assert_eq!(replica.conn().cmd(&["GET", &key]).text(), "precious");
    let ttl = replica.conn().cmd(&["TTL", &ttl_key]).int();
    assert!((1..=1000).contains(&ttl), "TTL after failover was {ttl}");
    assert!(replica.conn().cmd(&["SET", &key, "after-failover"]).is_ok());

    // Everyone redirects that slot to the new master.
    let reply = nodes[1].conn().cmd(&["GET", &key]);
    assert_eq!(
        reply.err().expect("a redirect"),
        format!("MOVED {} 127.0.0.1:{}", slot_of(&key), replica.port)
    );

    // The old master comes back.
    nodes[0].spawn();
    wait_until(
        "the old master to rejoin as a replica of the new one",
        Duration::from_secs(40),
        || {
            !nodes[0].is_master()
                && nodes[0].repl_info("master_link_status").as_deref() == Some("up")
        },
    );
    assert_eq!(nodes[0].id(), old_master_id, "it keeps its identity");
    assert_eq!(
        nodes[0].my_row()[3],
        replica.id(),
        "and follows the new master"
    );
    // It no longer has the FAIL flag in anyone's view...
    wait_until(
        "FAIL to be cleared everywhere",
        Duration::from_secs(20),
        || {
            [&nodes[1], &nodes[2], &replica].iter().all(|n| {
                n.conn()
                    .cmd(&["CLUSTER", "NODES"])
                    .text()
                    .lines()
                    .find(|l| l.starts_with(&old_master_id))
                    .is_some_and(|l| !l.contains("fail"))
            })
        },
    );
    // ...and it has the new master's data, not its own stale copy.
    let mut c = nodes[0].conn();
    assert!(c.cmd(&["READONLY"]).is_ok());
    assert_eq!(c.cmd(&["GET", &key]).text(), "after-failover");
}

/// After a failover, the failed master's other replica carries on from the
/// new master by a partial resync, not a full one (#109): the new master
/// keeps the old stream's id as its second, and has kept, as a replica, the
/// stream it read.
#[test]
fn after_a_failover_the_other_replica_is_continued_not_resynced() {
    let (mut nodes, ranges) = form_cluster(3);
    let a = add_replica(&nodes, &nodes[0]);
    let b = add_replica(&nodes, &nodes[0]);
    {
        let all: Vec<&Node> = nodes.iter().chain([&a, &b]).collect();
        wait_cluster_ok(&all, 5);
    }
    let range = ranges[0].0..=ranges[0].1;
    let keys: Vec<String> = (0..200)
        .map(|i| key_in(range.clone(), &format!("cont{i}")))
        .collect();
    for k in &keys {
        assert!(nodes[0].conn().cmd(&["SET", k, "before"]).is_ok());
    }
    assert_eq!(nodes[0].conn().cmd(&["WAIT", "2", "5000"]).int(), 2);
    let old_id = nodes[0].repl_info("master_replid").unwrap();

    nodes[0].kill();
    wait_until("a replica to be promoted", Duration::from_secs(40), || {
        a.is_master() || b.is_master()
    });
    let (winner, other) = if a.is_master() { (&a, &b) } else { (&b, &a) };
    // The cluster view says master a moment before replication does.
    wait_until(
        "the new master to keep the old stream's id",
        Duration::from_secs(10),
        || winner.repl_info("master_replid2").as_deref() == Some(old_id.as_str()),
    );
    // Writes on the new master before the other replica is back with it.
    assert!(winner.conn().cmd(&["SET", &keys[0], "after"]).is_ok());

    wait_until(
        "the other replica to follow the new master",
        Duration::from_secs(40),
        || {
            other.repl_info("master_link_status").as_deref() == Some("up")
                && other.my_row()[3] == winner.id()
        },
    );
    let stats = winner.conn().cmd(&["INFO", "stats"]).text();
    assert_eq!(
        info_field(&stats, "sync_full").as_deref(),
        Some("0"),
        "{stats}"
    );
    assert_eq!(
        info_field(&stats, "sync_partial_ok").as_deref(),
        Some("1"),
        "{stats}"
    );
    wait_until(
        "the write after the failover to arrive",
        Duration::from_secs(10),
        || {
            let mut c = other.conn();
            c.cmd(&["READONLY"]);
            c.cmd(&["GET", &keys[0]]).text() == "after"
        },
    );
    let mut c = other.conn();
    assert!(c.cmd(&["READONLY"]).is_ok());
    for k in &keys[1..] {
        assert_eq!(c.cmd(&["GET", k]).text(), "before", "{k}");
    }
}

/// With two replicas, the one that has read further wins the election
/// (#107). Replica B is stopped while writes go on, the master dies, and B
/// comes back behind: it asks A how far A has read, and stands a second
/// later. Before, either could win, and B winning lost the writes it missed.
#[test]
fn the_replica_that_read_further_wins_the_election() {
    let (mut nodes, ranges) = form_cluster(3);
    let a = add_replica(&nodes, &nodes[0]);
    let mut b = add_replica(&nodes, &nodes[0]);
    {
        let all: Vec<&Node> = nodes.iter().chain([&a, &b]).collect();
        wait_cluster_ok(&all, 5);
    }

    b.kill();
    let range = ranges[0].0..=ranges[0].1;
    let keys: Vec<String> = (0..50)
        .map(|i| key_in(range.clone(), &format!("rank{i}")))
        .collect();
    for k in &keys {
        assert!(nodes[0].conn().cmd(&["SET", k, "v"]).is_ok());
    }
    assert_eq!(nodes[0].conn().cmd(&["WAIT", "1", "5000"]).int(), 1);

    nodes[0].kill();
    b.spawn();
    wait_until("a replica to be promoted", Duration::from_secs(40), || {
        a.is_master() || b.is_master()
    });
    assert!(
        a.is_master() && !b.is_master(),
        "the replica that had read further must win"
    );
    for k in &keys {
        assert_eq!(a.conn().cmd(&["GET", k]).text(), "v", "{k}");
    }
}

/// Eight masters with two replicas each: once settled, heartbeats open no new
/// bus connections. When a master dies, one of its replicas takes over with
/// the highest config epoch, the other follows it, and the old master rejoins
/// as a replica.
#[test]
#[ignore = "starts 24 nodes"]
fn a_24_node_cluster_fails_over_and_the_old_master_rejoins() {
    const MASTERS: usize = 8;
    const REPLICAS_PER_MASTER: usize = 2;
    const VICTIM: usize = 3;
    const NODES: usize = MASTERS * (1 + REPLICAS_PER_MASTER);
    let (mut masters, mut ranges) = form_cluster(MASTERS);
    let mut replicas = Vec::new();
    for master in &masters {
        for _ in 0..REPLICAS_PER_MASTER {
            replicas.push(add_replica(&masters, master));
        }
    }
    let nodes_of = |n: &Node| n.conn().cmd(&["CLUSTER", "NODES"]).text();
    fn row<'a>(nodes: &'a str, id: &str) -> Option<Vec<&'a str>> {
        nodes
            .lines()
            .find(|l| l.starts_with(id))
            .map(|l| l.split(' ').collect())
    }
    let epoch = |n: &Node| -> u64 { n.my_row()[6].parse().unwrap() };
    {
        let all: Vec<&Node> = masters.iter().chain(&replicas).collect();
        wait_cluster_ok(&all, NODES);
        let pairs: Vec<(String, String)> = replicas
            .iter()
            .enumerate()
            .map(|(i, r)| (r.id(), masters[i / REPLICAS_PER_MASTER].id()))
            .collect();
        wait_until(
            "every node to agree on who replicates whom",
            Duration::from_secs(30),
            || {
                all.iter().all(|n| {
                    let nodes = nodes_of(n);
                    pairs.iter().all(|(replica, master)| {
                        row(&nodes, replica)
                            .is_some_and(|r| r[2].contains("slave") && r[3] == master)
                    })
                })
            },
        );
        wait_until(
            "the masters to have distinct config epochs",
            Duration::from_secs(15),
            || {
                let mut epochs: Vec<u64> = masters.iter().map(epoch).collect();
                epochs.sort_unstable();
                epochs.dedup();
                epochs.len() == MASTERS
            },
        );

        // Measured before the failover, which rightly redials the dead node.
        let total = |f: &str| -> u64 {
            all.iter()
                .map(|n| n.cluster_info(f).parse::<u64>().unwrap_or(0))
                .sum()
        };
        // Let every link come up.
        std::thread::sleep(Duration::from_secs(2));
        let (connects, sent) = (
            total("cluster_stats_bus_connects"),
            total("cluster_stats_messages_sent"),
        );
        std::thread::sleep(Duration::from_secs(5));
        let new_connects = total("cluster_stats_bus_connects") - connects;
        let new_messages = total("cluster_stats_messages_sent") - sent;
        assert!(
            new_messages >= (NODES * (NODES - 1)) as u64,
            "{new_messages} messages sent in 5 s"
        );
        assert!(
            new_connects <= 2,
            "{new_connects} connections opened in 5 s"
        );
    }

    let (start, end) = ranges.remove(VICTIM);
    let keys: Vec<String> = (0..100)
        .map(|i| key_in(start..=end, &format!("big{i}")))
        .collect();
    let mut writer = masters[VICTIM].conn();
    for k in &keys {
        assert!(writer.cmd(&["SET", k, "v"]).is_ok());
    }
    let wait = REPLICAS_PER_MASTER.to_string();
    assert_eq!(
        writer.cmd(&["WAIT", &wait, "5000"]).int(),
        REPLICAS_PER_MASTER as i64
    );
    let its_replicas = VICTIM * REPLICAS_PER_MASTER..(VICTIM + 1) * REPLICAS_PER_MASTER;
    for replica in &replicas[its_replicas.clone()] {
        let mut c = replica.conn();
        assert!(c.cmd(&["READONLY"]).is_ok());
        for k in &keys {
            assert_eq!(c.cmd(&["GET", k]).text(), "v", "{k}");
        }
    }

    let mut victim = masters.remove(VICTIM);
    let pair: Vec<Node> = replicas.drain(its_replicas).collect();
    let elsewhere: Vec<String> = ranges.iter().map(|r| key_in(r.0..=r.1, "kept")).collect();
    for (m, k) in masters.iter().zip(&elsewhere) {
        assert!(m.conn().cmd(&["SET", k, "v"]).is_ok());
    }
    let victim_id = victim.id();
    let survivors: Vec<&Node> = masters.iter().chain(&replicas).chain(&pair).collect();

    victim.kill();
    let killed = Instant::now();
    wait_until(
        "every survivor to mark the master FAIL",
        Duration::from_secs(30),
        || {
            survivors.iter().all(|n| {
                row(&nodes_of(n), &victim_id).is_some_and(|r| r[2].split(',').any(|f| f == "fail"))
            })
        },
    );
    let detected = killed.elapsed();
    wait_until("a replica to be promoted", Duration::from_secs(30), || {
        pair.iter().any(Node::is_master)
    });
    let promoted = killed.elapsed();
    let (winner, other) = if pair[0].is_master() {
        (&pair[0], &pair[1])
    } else {
        (&pair[1], &pair[0])
    };
    let moved = format!("MOVED {} 127.0.0.1:{}", slot_of(&keys[0]), winner.port);
    wait_until(
        "every survivor to be up and send the slot to the new master",
        Duration::from_secs(30),
        || {
            survivors.iter().all(|n| {
                n.cluster_info("cluster_state") == "ok"
                    && (n.port == winner.port
                        || n.conn().cmd(&["GET", &keys[0]]).err() == Some(moved.as_str()))
            })
        },
    );
    let recovered = killed.elapsed();
    eprintln!(
        "24 nodes: FAIL everywhere after {detected:?}, replica promoted after {promoted:?}, \
         cluster up after {recovered:?}"
    );

    assert!(!other.is_master(), "only one replica is promoted");
    let won_at = epoch(winner);
    for m in &masters {
        let at = epoch(m);
        assert!(
            at < won_at,
            "{} is at config epoch {at}, the new master at {won_at}",
            m.port
        );
    }
    assert_eq!(winner.my_row()[8..], [format!("{start}-{end}")]);
    for k in &keys {
        assert_eq!(winner.conn().cmd(&["GET", k]).text(), "v", "{k}");
    }
    for (m, k) in masters.iter().zip(&elsewhere) {
        assert_eq!(m.conn().cmd(&["GET", k]).text(), "v", "{k}");
    }
    assert!(winner.conn().cmd(&["SET", &keys[0], "after"]).is_ok());

    wait_until(
        "the other replica to follow the new master",
        Duration::from_secs(40),
        || {
            other.repl_info("master_link_status").as_deref() == Some("up")
                && other.my_row()[3] == winner.id()
        },
    );
    wait_until(
        "the write after the failover to reach the other replica",
        Duration::from_secs(10),
        || {
            let mut c = other.conn();
            c.cmd(&["READONLY"]);
            c.cmd(&["GET", &keys[0]]).text() == "after"
        },
    );

    victim.spawn();
    wait_until(
        "the old master to rejoin as a replica of the new one",
        Duration::from_secs(40),
        || {
            !victim.is_master()
                && victim.repl_info("master_link_status").as_deref() == Some("up")
                && victim.my_row()[3] == winner.id()
        },
    );
    wait_until(
        "FAIL to be cleared everywhere",
        Duration::from_secs(20),
        || {
            survivors
                .iter()
                .all(|n| row(&nodes_of(n), &victim_id).is_some_and(|r| !r[2].contains("fail")))
        },
    );
    let mut c = victim.conn();
    assert!(c.cmd(&["READONLY"]).is_ok());
    assert_eq!(c.cmd(&["GET", &keys[0]]).text(), "after");
    let all: Vec<&Node> = survivors.into_iter().chain([&victim]).collect();
    wait_cluster_ok(&all, NODES);
}

/// `CLUSTER FAILOVER` on a replica of a healthy master swaps the two, and no
/// write the master acknowledged is lost in the handover.
#[test]
fn manual_failover_swaps_roles_without_losing_acknowledged_writes() {
    let (nodes, ranges) = form_cluster(3);
    let replica = add_replica(&nodes, &nodes[0]);
    {
        let all: Vec<&Node> = nodes.iter().chain(std::iter::once(&replica)).collect();
        wait_cluster_ok(&all, 4);
    }
    let key = key_in(ranges[0].0..=ranges[0].1, "m");

    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = counting_writer(&stop, &key, [nodes[0].port, replica.port]);

    std::thread::sleep(Duration::from_millis(500));
    let reply = replica.conn().cmd(&["CLUSTER", "FAILOVER"]);
    assert!(reply.is_ok(), "CLUSTER FAILOVER: {reply:?}");

    let swapped = (0..300).any(|_| {
        std::thread::sleep(Duration::from_millis(100));
        replica.is_master() && !nodes[0].is_master()
    });
    if !swapped {
        let logs: Vec<String> = nodes
            .iter()
            .chain(std::iter::once(&replica))
            .map(|n| format!("==== node {}\n{}", n.port, n.log()))
            .collect();
        panic!("roles did not swap within 30 s\n{}", logs.join("\n"));
    }
    wait_until(
        "the old master to follow the new one",
        Duration::from_secs(30),
        || nodes[0].repl_info("master_link_status").as_deref() == Some("up"),
    );
    std::thread::sleep(Duration::from_millis(500));
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let acked = writer.join().unwrap();
    assert!(acked > 0, "the writer never got a write through");

    let value: i64 = replica.conn().cmd(&["GET", &key]).text().parse().unwrap();
    assert_eq!(
        value, acked,
        "the counter must equal the number of acknowledged INCRs"
    );

    // A failover against a master is refused.
    let reply = replica.conn().cmd(&["CLUSTER", "FAILOVER"]);
    assert!(reply.err().is_some_and(|e| e.contains("replica")));
}

/// A writer that INCRs `key` until `stop`, follows redirects between the two
/// `ports`, and counts only the INCRs acknowledged.
fn counting_writer(
    stop: &std::sync::Arc<std::sync::atomic::AtomicBool>,
    key: &str,
    ports: [u16; 2],
) -> std::thread::JoinHandle<i64> {
    let stop = std::sync::Arc::clone(stop);
    let key = key.to_string();
    std::thread::spawn(move || {
        let mut acked = 0i64;
        let mut target = 0usize;
        let mut conn = Conn::connect(ports[target]);
        while !stop.load(std::sync::atomic::Ordering::Relaxed) {
            match conn.try_cmd(&["INCR", &key]) {
                Ok(Resp::Int(_)) => acked += 1,
                // Redirected, read-only, paused or dropped: not
                // acknowledged. Try the other node.
                _ => {
                    target = 1 - target;
                    std::thread::sleep(Duration::from_millis(5));
                    if let Some(c) = Conn::try_connect(ports[target]) {
                        conn = c;
                    }
                }
            }
        }
        acked
    })
}

/// A manual failover whose election outlasts the time it has loses no write
/// the master acknowledged (#185). The votes reach the replica 6 s late, as
/// they do from voters whose disks are slow to sync them. The master used to
/// hold writes for 5 s and the replica to take over whenever it won: so the
/// master took writes again, and those it acknowledged between the replica's
/// promotion and its own hearing of it were lost. Now the master holds
/// writes for twice as long as the replica has to win in, and a replica that
/// has not won in that time gives the failover up.
#[test]
fn a_manual_failover_that_runs_out_of_time_loses_no_acknowledged_write() {
    let (nodes, ranges) = form_cluster(3);
    let replica = add_replica_with_env(&nodes, &nodes[0], &[("KIVIDB_TEST_VOTE_DELAY_MS", "6000")]);
    {
        let all: Vec<&Node> = nodes.iter().chain(std::iter::once(&replica)).collect();
        wait_cluster_ok(&all, 4);
    }
    let key = key_in(ranges[0].0..=ranges[0].1, "late");

    // Several writers, for more writes in whatever window there is.
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writers: Vec<_> = (0..4)
        .map(|_| counting_writer(&stop, &key, [nodes[0].port, replica.port]))
        .collect();

    std::thread::sleep(Duration::from_millis(500));
    let reply = replica.conn().cmd(&["CLUSTER", "FAILOVER"]);
    assert!(reply.is_ok(), "CLUSTER FAILOVER: {reply:?}");

    // Past the late votes and the master's hold on writes, whichever way
    // the failover went; then until the shard has one master again, and its
    // replica follows it.
    std::thread::sleep(Duration::from_secs(11));
    let master_and_replica = || {
        if replica.is_master() {
            (&replica, &nodes[0])
        } else {
            (&nodes[0], &replica)
        }
    };
    wait_until(
        "one master for the shard, followed by the other node",
        Duration::from_secs(30),
        || {
            let (master, follower) = master_and_replica();
            !follower.is_master()
                && master.is_master()
                && follower.repl_info("master_link_status").as_deref() == Some("up")
        },
    );
    std::thread::sleep(Duration::from_millis(500));
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let acked: i64 = writers.into_iter().map(|w| w.join().unwrap()).sum();
    assert!(acked > 0, "the writers never got a write through");

    // Not equal, necessarily: an INCR held by the pause for longer than the
    // client waits is applied, but was never acknowledged.
    let (master, _) = master_and_replica();
    let value: i64 = master.conn().cmd(&["GET", &key]).text().parse().unwrap();
    assert!(
        value >= acked,
        "{} acknowledged INCRs lost: the counter is {value}, {acked} were acknowledged",
        acked - value
    );
}

// ── Resharding ────────────────────────────────────────────────────────────────

/// Move a slot between masters the way the resharding tools do: mark it
/// migrating/importing, MIGRATE its keys in a batch, then assign it. Every
/// value type must arrive, with its TTL.
#[test]
fn migrate_moves_a_slot_with_every_type_and_ttl() {
    let (nodes, ranges) = form_cluster(2);
    let (src, dst) = (&nodes[0], &nodes[1]);
    let (src_id, dst_id) = (src.id(), dst.id());

    // Keys sharing one slot, via a hash tag.
    let tag = key_in(ranges[0].0..=ranges[0].1, "tag");
    let slot = slot_of(&tag).to_string();
    let k = |name: &str| format!("{{{tag}}}:{name}");

    let mut s = src.conn();
    assert!(s.cmd(&["SET", &k("str"), "v", "EX", "1000"]).is_ok());
    s.cmd(&["RPUSH", &k("list"), "a", "b"]);
    s.cmd(&["HSET", &k("hash"), "f", "v"]);
    s.cmd(&["SADD", &k("set"), "m"]);
    s.cmd(&["ZADD", &k("zset"), "1", "m"]);
    s.cmd(&["XADD", &k("stream"), "1-1", "f", "v"]);
    assert!(
        s.cmd(&["TS.CREATE", &k("ts"), "LABELS", "room", "attic"])
            .is_ok()
    );
    s.cmd(&["TS.ADD", &k("ts"), "1000", "21.5"]);
    let names = ["str", "list", "hash", "set", "zset", "stream", "ts"];

    // Giving the slot away while its keys are still here is refused.
    let reply = s.cmd(&["CLUSTER", "SETSLOT", &slot, "NODE", &dst_id]);
    assert!(
        reply.err().is_some_and(|e| e.contains("still hold")),
        "{reply:?}"
    );

    assert!(
        dst.conn()
            .cmd(&["CLUSTER", "SETSLOT", &slot, "IMPORTING", &src_id])
            .is_ok()
    );
    assert!(
        s.cmd(&["CLUSTER", "SETSLOT", &slot, "MIGRATING", &dst_id])
            .is_ok()
    );
    // Each side shows the open slot on its own line of CLUSTER NODES, as
    // Redis does: redis-cli --cluster check and fix go by it.
    let own_line = |c: &mut Conn| {
        let nodes = c.cmd(&["CLUSTER", "NODES"]).text();
        nodes
            .lines()
            .find(|l| l.contains("myself"))
            .unwrap()
            .to_string()
    };
    let line = own_line(&mut s);
    assert!(line.ends_with(&format!(" [{slot}->-{dst_id}]")), "{line}");
    let line = own_line(&mut dst.conn());
    assert!(line.ends_with(&format!(" [{slot}-<-{src_id}]")), "{line}");

    // One batch, the KEYS form, including a key that does not exist.
    let port = dst.port.to_string();
    let mut args: Vec<String> = ["MIGRATE", "127.0.0.1", &port, "", "0", "5000", "KEYS"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    args.extend(names.iter().map(|n| k(n)));
    args.push(k("missing"));
    let refs: Vec<&str> = args.iter().map(String::as_str).collect();
    let reply = s.cmd(&refs);
    assert!(reply.is_ok(), "MIGRATE: {reply:?}");

    // Gone from the source; a client asking there is sent on with -ASK.
    assert_eq!(s.cmd(&["CLUSTER", "COUNTKEYSINSLOT", &slot]).int(), 0);
    let reply = s.cmd(&["GET", &k("str")]);
    assert!(
        reply.err().is_some_and(|e| e.starts_with("ASK")),
        "{reply:?}"
    );

    // Nothing to move the second time.
    let reply = s.cmd(&["MIGRATE", "127.0.0.1", &port, &k("str"), "0", "5000"]);
    assert_eq!(reply.text(), "NOKEY");

    // Finish: both sides agree on the new owner.
    assert!(
        dst.conn()
            .cmd(&["CLUSTER", "SETSLOT", &slot, "NODE", &dst_id])
            .is_ok()
    );
    assert!(
        s.cmd(&["CLUSTER", "SETSLOT", &slot, "NODE", &dst_id])
            .is_ok()
    );

    let mut d = dst.conn();
    assert_eq!(
        d.cmd(&["CLUSTER", "COUNTKEYSINSLOT", &slot]).int(),
        names.len() as i64
    );
    assert_eq!(d.cmd(&["GET", &k("str")]).text(), "v");
    let ttl = d.cmd(&["TTL", &k("str")]).int();
    assert!((1..=1000).contains(&ttl), "TTL after MIGRATE was {ttl}");
    assert_eq!(d.cmd(&["LLEN", &k("list")]).int(), 2);
    assert_eq!(d.cmd(&["HGET", &k("hash"), "f"]).text(), "v");
    assert_eq!(d.cmd(&["SCARD", &k("set")]).int(), 1);
    assert_eq!(d.cmd(&["ZCARD", &k("zset")]).int(), 1);
    assert_eq!(d.cmd(&["XLEN", &k("stream")]).int(), 1);
    assert_eq!(d.cmd(&["TYPE", &k("ts")]).text(), "TSDB-TYPE");
    // The migrated series is findable by label on its new node.
    let found = d.cmd(&["TS.QUERYINDEX", "room=attic"]);
    assert_eq!(found.array().len(), 1);

    // The old owner now redirects, by MOVED rather than ASK.
    let reply = s.cmd(&["GET", &k("str")]);
    assert!(
        reply.err().is_some_and(|e| e.starts_with("MOVED")),
        "{reply:?}"
    );
}

// ── Atomic slot migration (CLUSTER MIGRATION) ─────────────────────────────────

/// One field of a task from `CLUSTER MIGRATION STATUS`.
/// The destination of an import goes down before it takes the slots over:
/// what it had received is deleted when it comes back, and the source still
/// serves all of it (#108). It used to stay: unreachable (MOVED), but in
/// DBSIZE, KEYS and snapshots, and live again if the slots came back.
#[test]
fn keys_of_an_import_cut_short_by_a_restart_are_dropped() {
    let (mut nodes, _) = form_cluster(2);
    let mut s = nodes[0].conn();
    let big = "x".repeat(20_000);
    let mut inside: Vec<String> = Vec::new();
    for i in 0..6000 {
        let key = format!("big:{i}");
        if slot_of(&key) <= 4000 {
            assert!(s.cmd(&["SET", &key, &big]).is_ok());
            inside.push(key);
        }
    }

    // What arrives has to outlive the restart, as it does with an AOF.
    let mut d = nodes[1].conn();
    assert!(d.cmd(&["CONFIG", "SET", "appendonly", "yes"]).is_ok());
    assert!(d.cmd(&["CONFIG", "REWRITE"]).is_ok());
    wait_until("the AOF to be on", Duration::from_secs(20), || {
        d.cmd(&["INFO", "persistence"])
            .text()
            .contains("aof_rewrite_in_progress:0")
    });
    let task_id = d
        .cmd(&["CLUSTER", "MIGRATION", "IMPORT", "0", "4000"])
        .text();
    wait_until("some keys to arrive", Duration::from_secs(20), || {
        d.cmd(&["DBSIZE"]).int() > 200
    });
    // Long enough for the AOF to have them on disk (fsync every second).
    std::thread::sleep(Duration::from_millis(1500));
    nodes[1].kill();
    let first = slot_of(&inside[0]).to_string();
    if s.cmd(&["CLUSTER", "COUNTKEYSINSLOT", &first]).int() == 0 {
        // Quick enough to take the slots over first: nothing to check.
        return;
    }
    let _ = task_id;

    nodes[1].spawn();
    let mut d = nodes[1].conn();
    wait_until(
        "the restarted destination to drop the partial import",
        Duration::from_secs(30),
        || d.cmd(&["DBSIZE"]).int() == 0,
    );
    // The source has it all, and the cluster agrees it owns it.
    for key in inside.iter().step_by(13) {
        assert_eq!(s.cmd(&["STRLEN", key]).int(), 20_000, "{key}");
    }
    let reply = d.cmd(&["GET", &inside[0]]);
    assert!(
        reply.err().is_some_and(|e| e.starts_with("MOVED")),
        "{reply:?}"
    );
    // And a later import of the same slots starts from nothing and completes.
    let again = d
        .cmd(&["CLUSTER", "MIGRATION", "IMPORT", "0", "4000"])
        .text();
    wait_until(
        "the second import to complete",
        Duration::from_secs(60),
        || {
            let status = d.cmd(&["CLUSTER", "MIGRATION", "STATUS", "ID", &again]);
            status
                .array()
                .first()
                .is_some_and(|task| task_field(task, "state").text() == "completed")
        },
    );
    for key in inside.iter().step_by(13) {
        assert_eq!(d.cmd(&["STRLEN", key]).int(), 20_000, "{key}");
    }
}

fn task_field(task: &Resp, name: &str) -> Resp {
    task.array()
        .chunks(2)
        .find(|pair| pair[0].text() == name)
        .map(|pair| pair[1].clone())
        .unwrap_or_else(|| panic!("no {name} in {task:?}"))
}

/// A new master takes slots from each of the others in turn, as
/// `redis-cli --cluster reshard` has it do: every node ends up agreeing who
/// owns what. Its epoch used to count up from its own, so its later claims
/// were no newer than the owners' and the other nodes kept the old owners.
#[test]
fn slots_taken_from_every_master_are_agreed_on_by_all() {
    // Formed as redis-cli --cluster create forms one: an epoch per node.
    let mut nodes: Vec<Node> = (0..3).map(|_| Node::start(&[])).collect();
    let ranges = [(0u16, 5460u16), (5461, 10922), (10923, 16383)];
    for (i, (node, (a, b))) in nodes.iter().zip(&ranges).enumerate() {
        let mut c = node.conn();
        let epoch = (i + 1).to_string();
        assert!(c.cmd(&["CLUSTER", "SET-CONFIG-EPOCH", &epoch]).is_ok());
        let r = c.cmd(&["CLUSTER", "ADDSLOTSRANGE", &a.to_string(), &b.to_string()]);
        assert!(r.is_ok(), "{r:?}");
    }
    for node in &nodes[1..] {
        let r = nodes[0]
            .conn()
            .cmd(&["CLUSTER", "MEET", "127.0.0.1", &node.port.to_string()]);
        assert!(r.is_ok(), "{r:?}");
    }
    wait_cluster_ok(&nodes.iter().collect::<Vec<_>>(), 3);
    let joiner = Node::start(&[]);
    let r = nodes[0]
        .conn()
        .cmd(&["CLUSTER", "MEET", "127.0.0.1", &joiner.port.to_string()]);
    assert!(r.is_ok(), "{r:?}");
    nodes.push(joiner);
    wait_cluster_ok(&nodes.iter().collect::<Vec<_>>(), 4);

    let mut j = nodes[3].conn();
    for (start, _) in &ranges {
        let (a, b) = (start.to_string(), (start + 99).to_string());
        let id = j.cmd(&["CLUSTER", "MIGRATION", "IMPORT", &a, &b]).text();
        wait_until("the import to finish", Duration::from_secs(30), || {
            let status = j.cmd(&["CLUSTER", "MIGRATION", "STATUS", "ID", &id]);
            status
                .array()
                .first()
                .is_some_and(|t| task_field(t, "state").text() == "completed")
        });
    }
    let joiner_id = nodes[3].id();
    wait_until("every node to agree", Duration::from_secs(20), || {
        nodes.iter().all(|n| {
            let text = n.conn().cmd(&["CLUSTER", "NODES"]).text();
            let line = text
                .lines()
                .find(|l| l.starts_with(&joiner_id))
                .unwrap_or("");
            ranges
                .iter()
                .all(|(s, _)| line.contains(&format!(" {}-{}", s, s + 99)))
        })
    });
}

/// A whole slot range moves to another master while a client keeps writing
/// to it. The client never sees -ASK or -TRYAGAIN — only, at the end, the
/// -MOVED that tells it where the slots now live — and no acknowledged write
/// is lost or applied twice.
#[test]
fn atomic_slot_migration_moves_a_range_under_load() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    let (nodes, _) = form_cluster(2);
    let (src, dst) = (&nodes[0], &nodes[1]);
    let dst_replica = add_replica(&nodes, dst);
    wait_cluster_ok(&[src, dst, &dst_replica], 3);
    let moving = 0..=1000u16;

    // Keys of several types in the range, some with a TTL, and one outside it.
    let mut s = src.conn();
    let mut inside: Vec<String> = Vec::new();
    for i in 0..4000 {
        let key = format!("k:{i}");
        if moving.contains(&slot_of(&key)) {
            assert!(s.cmd(&["SET", &key, &format!("v{i}")]).is_ok());
            inside.push(key);
        }
    }
    assert!(inside.len() > 100, "{} keys in the range", inside.len());
    let ttl_key = key_in(moving.clone(), "ttl");
    let list_key = key_in(moving.clone(), "list");
    let stream_key = key_in(moving.clone(), "stream");
    let counter = key_in(moving.clone(), "counter");
    let outside = key_in(2000..=8191, "outside");
    assert!(s.cmd(&["SET", &ttl_key, "v", "EX", "1000"]).is_ok());
    assert_eq!(s.cmd(&["RPUSH", &list_key, "a", "b", "c"]).int(), 3);
    let added = s.cmd(&["XADD", &stream_key, "IDMP", "p", "m1", "*", "f", "v"]);
    assert!(matches!(added, Resp::Bulk(_)), "{added:?}");
    assert!(s.cmd(&["SET", &outside, "stays"]).is_ok());
    assert!(s.cmd(&["SET", &counter, "0"]).is_ok());

    // A writer that follows -MOVED and tolerates nothing else.
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let stop = Arc::clone(&stop);
        let counter = counter.clone();
        let ports = [src.port, dst.port];
        std::thread::spawn(move || {
            let (mut target, mut acked, mut moved) = (0usize, 0i64, 0u32);
            let mut conn = Conn::connect(ports[target]);
            while !stop.load(Ordering::Relaxed) {
                match conn.cmd(&["INCR", &counter]) {
                    Resp::Int(_) => acked += 1,
                    Resp::Error(e) if e.starts_with("MOVED") => {
                        moved += 1;
                        target = 1 - target;
                        conn = Conn::connect(ports[target]);
                    }
                    other => panic!("the writer saw {other:?}"),
                }
            }
            (acked, moved)
        })
    };
    std::thread::sleep(Duration::from_millis(200));

    let task_id = dst
        .conn()
        .cmd(&["CLUSTER", "MIGRATION", "IMPORT", "0", "1000"])
        .text();
    assert_eq!(task_id.len(), 40, "{task_id}");
    wait_until("the import to complete", Duration::from_secs(60), || {
        let status = dst
            .conn()
            .cmd(&["CLUSTER", "MIGRATION", "STATUS", "ID", &task_id]);
        let task = &status.array()[0];
        let state = task_field(task, "state").text();
        assert!(state == "in_progress" || state == "completed", "{task:?}");
        state == "completed"
    });
    std::thread::sleep(Duration::from_millis(300));
    stop.store(true, Ordering::Relaxed);
    let (acked, moved) = writer.join().unwrap();
    assert!(acked > 0);
    assert_eq!(moved, 1, "one redirect, from the old owner to the new");

    // The destination owns the range and has everything, exactly once.
    let mut d = dst.conn();
    assert_eq!(d.cmd(&["GET", &counter]).text(), acked.to_string());
    for key in &inside {
        assert_eq!(d.cmd(&["GET", key]).text(), format!("v{}", &key[2..]));
    }
    let ttl = d.cmd(&["TTL", &ttl_key]).int();
    assert!((1..=1000).contains(&ttl), "TTL after migration was {ttl}");
    assert_eq!(d.cmd(&["LLEN", &list_key]).int(), 3);
    assert_eq!(d.cmd(&["XLEN", &stream_key]).int(), 1);
    // What the stream remembers about its producers moved with it.
    let retry = d.cmd(&["XADD", &stream_key, "IDMP", "p", "m1", "*", "f", "v"]);
    assert_eq!(retry, added);
    assert_eq!(d.cmd(&["XLEN", &stream_key]).int(), 1);

    // The source kept what was not moving, dropped what was, and redirects.
    let mut s = src.conn();
    assert_eq!(s.cmd(&["GET", &outside]).text(), "stays");
    wait_until(
        "the source to drop the keys",
        Duration::from_secs(10),
        || {
            inside.iter().step_by(11).all(|key| {
                let slot = slot_of(key).to_string();
                s.cmd(&["CLUSTER", "COUNTKEYSINSLOT", &slot]).int() == 0
            })
        },
    );
    let slot = slot_of(&inside[0]);
    let reply = s.cmd(&["GET", &inside[0]]);
    assert_eq!(
        reply.err().expect("a redirect"),
        format!("MOVED {slot} 127.0.0.1:{}", dst.port)
    );

    // Both sides report the task, and the cluster is whole. The source records
    // it completed once its nodes.conf is saved, which on a busy disk is a
    // moment after it has dropped the keys.
    wait_until(
        "the source to record the task completed",
        Duration::from_secs(10),
        || {
            let on_src = s.cmd(&["CLUSTER", "MIGRATION", "STATUS", "ALL"]);
            task_field(&on_src.array()[0], "state").text() == "completed"
        },
    );
    let on_src = s.cmd(&["CLUSTER", "MIGRATION", "STATUS", "ALL"]);
    let task = &on_src.array()[0];
    assert_eq!(task_field(task, "id").text(), task_id);
    assert_eq!(task_field(task, "operation").text(), "migrate");
    assert_eq!(task_field(task, "state").text(), "completed");
    assert_eq!(task_field(task, "slots").text(), "0-1000");
    assert_eq!(task_field(task, "source").text(), src.id());
    assert_eq!(task_field(task, "dest").text(), dst.id());
    assert_eq!(task_field(task, "last_error").text(), "");
    // Every pause of client writes is counted: the capture at the start as
    // well as the handoff. It used to count only the handoff.
    let total = task_field(task, "write_pause_ms").int();
    let capture = task_field(task, "capture_pause_ms").int();
    assert!(capture >= 0 && total >= capture, "{total} {capture}");
    let on_dst = d.cmd(&["CLUSTER", "MIGRATION", "STATUS"]);
    assert_eq!(task_field(&on_dst.array()[0], "operation").text(), "import");
    let dst_id = dst.id();
    wait_until(
        "every node to agree on the new owner",
        Duration::from_secs(20),
        || {
            [src, dst, &dst_replica].iter().all(|n| {
                n.cluster_info("cluster_state") == "ok"
                    && n.conn()
                        .cmd(&["CLUSTER", "NODES"])
                        .text()
                        .lines()
                        .any(|l| l.starts_with(&dst_id) && l.contains(" 0-1000"))
            })
        },
    );
    assert_eq!(src.cluster_info("cluster_slots_assigned"), "16384");

    // The destination's replica received the imported keys through ordinary
    // replication.
    let mut r = dst_replica.conn();
    assert!(r.cmd(&["READONLY"]).is_ok());
    wait_until(
        "the replica to have the migrated data",
        Duration::from_secs(10),
        || r.cmd(&["GET", &counter]) == Resp::Bulk(acked.to_string().into_bytes()),
    );
    assert_eq!(
        r.cmd(&["GET", &inside[0]]).text(),
        format!("v{}", &inside[0][2..])
    );
    assert_eq!(r.cmd(&["LLEN", &list_key]).int(), 3);
}

/// A migration that cannot finish — here because it is cancelled while the
/// source is still sending — leaves the slots, and every key, where they were.
#[test]
fn a_cancelled_slot_migration_leaves_the_source_intact() {
    let (nodes, _) = form_cluster(2);
    let (src, dst) = (&nodes[0], &nodes[1]);
    let mut s = src.conn();
    let big = "x".repeat(20_000);
    let mut inside: Vec<String> = Vec::new();
    for i in 0..6000 {
        let key = format!("big:{i}");
        if slot_of(&key) <= 4000 {
            assert!(s.cmd(&["SET", &key, &big]).is_ok());
            inside.push(key);
        }
    }

    let mut d = dst.conn();
    let task_id = d
        .cmd(&["CLUSTER", "MIGRATION", "IMPORT", "0", "4000"])
        .text();
    // The same slots cannot be imported twice at once.
    let again = d.cmd(&["CLUSTER", "MIGRATION", "IMPORT", "100", "200"]);
    let cancelled = d
        .cmd(&["CLUSTER", "MIGRATION", "CANCEL", "ID", &task_id])
        .int();
    // The task's state on a node, or "" where the node has not heard of it:
    // a cancel can arrive before the source was ever asked to start.
    let state = |c: &mut Conn| {
        let status = c.cmd(&["CLUSTER", "MIGRATION", "STATUS", "ID", &task_id]);
        match status.array().first() {
            Some(task) => task_field(task, "state").text(),
            None => String::new(),
        }
    };
    if cancelled == 0 {
        // The machine was quick enough to finish first; nothing to check.
        assert_eq!(state(&mut d), "completed");
        return;
    }
    assert!(
        again
            .err()
            .is_some_and(|e| e.contains("already in progress")),
        "{again:?}"
    );
    wait_until("the import to stop", Duration::from_secs(20), || {
        state(&mut d) == "canceled"
    });
    wait_until("the source to stop", Duration::from_secs(20), || {
        state(&mut s) != "in_progress"
    });
    let on_src = state(&mut s);
    assert!(on_src.is_empty() || on_src == "canceled", "{on_src}");

    // The source still owns and serves everything, and takes writes again.
    for key in inside.iter().step_by(13) {
        assert_eq!(s.cmd(&["STRLEN", key]).int(), 20_000, "{key}");
    }
    assert!(s.cmd(&["SET", &inside[0], "writable"]).is_ok());
    // The destination holds none of it and still redirects.
    wait_until(
        "the partial import to be dropped",
        Duration::from_secs(20),
        || {
            inside.iter().step_by(13).all(|key| {
                let slot = slot_of(key).to_string();
                d.cmd(&["CLUSTER", "COUNTKEYSINSLOT", &slot]).int() == 0
            })
        },
    );
    let reply = d.cmd(&["GET", &inside[0]]);
    assert!(
        reply.err().is_some_and(|e| e.starts_with("MOVED")),
        "{reply:?}"
    );
    assert_eq!(src.cluster_info("cluster_state"), "ok");
}

#[test]
fn slot_migration_refuses_what_it_cannot_do() {
    let (nodes, _) = form_cluster(3);
    let mut c = nodes[0].conn();
    let err = |r: Resp| match r {
        Resp::Error(e) => e,
        other => panic!("expected an error, got {other:?}"),
    };

    // Slots this node already owns.
    let own = err(c.cmd(&["CLUSTER", "MIGRATION", "IMPORT", "0", "10"]));
    assert!(own.contains("already own"), "{own}");
    // Slots owned by two different nodes.
    let both = err(c.cmd(&["CLUSTER", "MIGRATION", "IMPORT", "10000", "12000"]));
    assert!(both.contains("more than one node"), "{both}");
    // Malformed ranges.
    for bad in [
        vec![],
        vec!["5"],
        vec!["10", "5"],
        vec!["0", "16384"],
        vec!["0", "x"],
        vec!["6000", "6100", "6050", "6200"],
    ] {
        let mut cmd = vec!["CLUSTER", "MIGRATION", "IMPORT"];
        cmd.extend(bad);
        assert!(c.cmd(&cmd).err().is_some(), "{cmd:?}");
    }

    // Nothing running: nothing to cancel, nothing to report.
    assert_eq!(c.cmd(&["CLUSTER", "MIGRATION", "CANCEL", "ALL"]).int(), 0);
    let all = c.cmd(&["CLUSTER", "MIGRATION", "STATUS", "ALL"]);
    assert_eq!(all.array().len(), 0);
    let id = "0".repeat(40);
    let one = c.cmd(&["CLUSTER", "MIGRATION", "STATUS", "ID", &id]);
    assert_eq!(one.array().len(), 0);
    assert!(c.cmd(&["CLUSTER", "MIGRATION", "CANCEL"]).err().is_some());

    // The node-to-node half is refused for a task that does not exist.
    let apply = c.cmd(&["CLUSTER", "SYNCSLOTS", "APPLY", &id, "SET", "k", "v"]);
    assert!(apply.err().is_some(), "{apply:?}");
    let takeover = c.cmd(&["CLUSTER", "SYNCSLOTS", "TAKEOVER", &id]);
    assert!(takeover.err().is_some(), "{takeover:?}");
    // And nothing above disturbed the cluster.
    assert_eq!(nodes[0].cluster_info("cluster_slots_assigned"), "16384");
    assert_eq!(nodes[0].cluster_info("cluster_state"), "ok");
}

/// A slot taken over by another master without its keys being moved — here
/// with a bare `SETSLOT … NODE` on the other node — leaves nothing behind on
/// the node that lost it.
#[test]
fn keys_of_a_slot_lost_to_another_master_are_dropped() {
    let (nodes, ranges) = form_cluster(2);
    let (loser, winner) = (&nodes[0], &nodes[1]);
    let replica = add_replica(&nodes, loser);
    wait_cluster_ok(&[loser, winner, &replica], 3);

    let lost_key = key_in(ranges[0].0..=ranges[0].1, "lost");
    let slot = slot_of(&lost_key);
    let tagged: Vec<String> = (0..20).map(|i| format!("{{{lost_key}}}:{i}")).collect();
    let kept_key = (0..100_000)
        .map(|i| format!("kept:{i}"))
        .find(|k| slot_of(k) <= ranges[0].1 && slot_of(k) != slot)
        .unwrap();
    let mut l = loser.conn();
    for key in &tagged {
        assert!(l.cmd(&["SET", key, "v"]).is_ok());
    }
    assert!(l.cmd(&["SET", &kept_key, "v"]).is_ok());
    assert_eq!(l.cmd(&["WAIT", "1", "5000"]).int(), 1);

    // The other master claims the slot at a newer epoch.
    let mut w = winner.conn();
    let bumped = w.cmd(&["CLUSTER", "BUMPEPOCH"]);
    assert!(bumped.err().is_none(), "{bumped:?}");
    let r = w.cmd(&[
        "CLUSTER",
        "SETSLOT",
        &slot.to_string(),
        "NODE",
        &winner.id(),
    ]);
    assert!(r.is_ok(), "{r:?}");

    wait_until("the loser to redirect", Duration::from_secs(20), || {
        l.cmd(&["GET", &tagged[0]])
            .err()
            .is_some_and(|e| e.starts_with("MOVED"))
    });
    wait_until(
        "the lost keys to be dropped",
        Duration::from_secs(20),
        || {
            l.cmd(&["CLUSTER", "COUNTKEYSINSLOT", &slot.to_string()])
                .int()
                == 0
        },
    );
    assert_eq!(l.cmd(&["DBSIZE"]).int(), 1);
    assert_eq!(l.cmd(&["GET", &kept_key]).text(), "v");
    // Its replica drops them too, and keeps following it.
    wait_until("the replica to drop them", Duration::from_secs(20), || {
        replica.conn().cmd(&["DBSIZE"]).int() == 1
    });
    assert_eq!(
        replica.repl_info("master_link_status").as_deref(),
        Some("up")
    );
    assert!(loser.is_master());
}

// ── Pub/Sub ───────────────────────────────────────────────────────────────────

/// A message published on one node reaches a subscriber connected to another.
/// `SSUBSCRIBE` is routed by its channel's slot, as in Redis: `MOVED` from a
/// node that does not have it, `CROSSSLOT` for channels of two slots. It
/// succeeded anywhere (#161).
#[test]
fn ssubscribe_is_routed_by_slot() {
    let (nodes, ranges) = form_cluster(2);
    let mine = key_in(ranges[0].0..=ranges[0].1, "chan");
    let theirs = key_in(ranges[1].0..=ranges[1].1, "chan");
    let mut c = nodes[0].conn();
    let reply = c.cmd(&["SSUBSCRIBE", &theirs]);
    assert!(
        reply.err().is_some_and(|e| e.starts_with("MOVED")),
        "{reply:?}"
    );
    let reply = c.cmd(&["SSUBSCRIBE", &mine, &theirs]);
    assert!(
        reply.err().is_some_and(|e| e.starts_with("CROSSSLOT")),
        "{reply:?}"
    );
    let reply = c.cmd(&["SSUBSCRIBE", &mine]);
    assert_eq!(reply.array()[0].text(), "ssubscribe", "{reply:?}");
}

#[test]
fn publish_reaches_subscribers_on_other_nodes() {
    let (nodes, _) = form_cluster(3);

    let mut sub = nodes[2].conn();
    let ack = sub.cmd(&["SUBSCRIBE", "news"]);
    assert_eq!(ack.array()[0].text(), "subscribe");

    // Published on a different node; the reply counts local receivers only.
    assert_eq!(nodes[0].conn().cmd(&["PUBLISH", "news", "hello"]).int(), 0);

    let msg = sub.read().expect("the forwarded message");
    let msg = msg.array();
    assert_eq!(msg[0].text(), "message");
    assert_eq!(msg[1].text(), "news");
    assert_eq!(msg[2].text(), "hello");
}

// ── Membership ────────────────────────────────────────────────────────────────

/// CLUSTER FORGET has to stick. The forgotten node is still running and still
/// has us in its table; its heartbeats, and other nodes' gossip about it,
/// used to bring it straight back.
#[test]
fn forgetting_a_node_is_not_undone_by_gossip() {
    let (nodes, _) = form_cluster(3);
    let gone = nodes[2].id();

    for node in &nodes[..2] {
        assert!(node.conn().cmd(&["CLUSTER", "FORGET", &gone]).is_ok());
    }
    // Several heartbeat rounds later it is still gone from both.
    std::thread::sleep(Duration::from_secs(4));
    for node in &nodes[..2] {
        let table = node.conn().cmd(&["CLUSTER", "NODES"]).text();
        assert!(!table.contains(&gone), "node came back:\n{table}");
        assert_eq!(node.cluster_info("cluster_known_nodes"), "2");
    }

    // You cannot forget yourself, or a node nobody knows.
    let me = nodes[0].id();
    assert!(
        nodes[0]
            .conn()
            .cmd(&["CLUSTER", "FORGET", &me])
            .err()
            .is_some()
    );
    assert!(
        nodes[0]
            .conn()
            .cmd(&["CLUSTER", "FORGET", &gone])
            .err()
            .is_some_and(|e| e.contains("Unknown node"))
    );
}

/// A MEET that nobody answers must not leave a handshake row behind.
#[test]
fn a_failed_meet_does_not_leave_a_handshake_entry() {
    let node = Node::start(&[]);
    let nobody = cluster_port();
    assert!(
        node.conn()
            .cmd(&["CLUSTER", "MEET", "127.0.0.1", &nobody.to_string()])
            .is_ok()
    );
    wait_until(
        "the placeholder to be removed",
        Duration::from_secs(15),
        || {
            !node
                .conn()
                .cmd(&["CLUSTER", "NODES"])
                .text()
                .contains("handshake")
        },
    );
    assert_eq!(node.cluster_info("cluster_known_nodes"), "1");
}

/// A hard reset gives the node a new identity and an empty slate.
#[test]
fn cluster_reset_hard_assigns_a_new_identity() {
    let (nodes, _) = form_cluster(2);
    let before = nodes[1].id();
    assert!(nodes[1].conn().cmd(&["CLUSTER", "RESET", "HARD"]).is_ok());
    let after = nodes[1].id();
    assert_ne!(before, after);
    assert_eq!(nodes[1].cluster_info("cluster_known_nodes"), "1");
    assert_eq!(nodes[1].cluster_info("cluster_slots_assigned"), "0");
    assert_eq!(nodes[1].cluster_info("cluster_current_epoch"), "0");
}

// ── Configuration ─────────────────────────────────────────────────────────────

/// A cluster config file that cannot be read is fatal. It used to be replaced
/// with a fresh one — a new node id, no slots — after one log line.
#[test]
fn an_unreadable_cluster_config_file_is_fatal() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("nodes.conf"), "this is not a nodes.conf\n").unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_kividb"))
        .current_dir(dir.path())
        .args([
            "--port",
            &cluster_port().to_string(),
            "--bind",
            "127.0.0.1",
            "--io-model",
            "tokio",
            "--cluster-enabled",
            "yes",
            "--configfile",
            "kividb.conf",
        ])
        .stdin(Stdio::null())
        .output()
        .expect("could not run kividb");
    assert!(!out.status.success(), "the node must refuse to start");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("Refusing to start"), "{stderr}");
    // And the file it could not read is still there, untouched.
    assert_eq!(
        std::fs::read_to_string(dir.path().join("nodes.conf")).unwrap(),
        "this is not a nodes.conf\n"
    );
}

/// `cluster-bus-port-protected-mode yes` is a refusal to run with an
/// unauthenticated bus — which, with no tls-cluster, means not at all.
#[test]
fn bus_protected_mode_refuses_to_start_an_unauthenticated_bus() {
    let dir = tempfile::tempdir().unwrap();
    let run = |protected: &str| {
        Command::new(env!("CARGO_BIN_EXE_kividb"))
            .current_dir(dir.path())
            .args([
                "--port",
                &cluster_port().to_string(),
                "--bind",
                "127.0.0.1",
                "--io-model",
                "tokio",
                "--cluster-enabled",
                "yes",
                "--cluster-bus-port-protected-mode",
                protected,
                "--configfile",
                "kividb.conf",
                // Make the permissive run exit by itself: a TLS port on a
                // build without TLS is refused, after the bus check passes.
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("could not run kividb")
    };

    let out = run("yes").wait_with_output().unwrap();
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("cluster-bus-port-protected-mode"),
        "{stderr}"
    );

    // The default starts, and says the bus is open.
    let mut child = run("no");
    std::thread::sleep(Duration::from_millis(1500));
    let _ = child.kill();
    let out = child.wait_with_output().unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("not authenticated"), "{stderr}");
}

// ── TLS between nodes ─────────────────────────────────────────────────────────

/// A CA and one certificate it signs, for every node to present.
#[cfg(feature = "tls")]
fn node_pki() -> (tempfile::TempDir, [String; 3]) {
    use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair};
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(DnType::CommonName, "kividb test ca");
    let ca_key = KeyPair::generate().unwrap();
    let ca = params.self_signed(&ca_key).unwrap();
    let key = KeyPair::generate().unwrap();
    let cert = CertificateParams::new(vec!["kividb-node".to_string()])
        .unwrap()
        .signed_by(&key, &ca, &ca_key)
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = |name: &str| dir.path().join(name).to_string_lossy().into_owned();
    std::fs::write(path("ca.crt"), ca.pem()).unwrap();
    std::fs::write(path("node.crt"), cert.pem()).unwrap();
    std::fs::write(path("node.key"), key.serialize_pem()).unwrap();
    let files = [path("ca.crt"), path("node.crt"), path("node.key")];
    (dir, files)
}

/// `tls-cluster yes`: the bus is TLS, every node announces its TLS port, and
/// what nodes open to each other's client ports — a replica's link, a slot
/// migration, index definitions — is TLS too. A node without it cannot
/// join.
#[cfg(feature = "tls")]
#[test]
fn a_cluster_runs_over_tls_between_its_nodes() {
    let (_pki, [ca, cert, key]) = node_pki();
    let tls_node = || {
        let tls_port = cluster_port().to_string();
        Node::start(&[
            "--tls-port",
            &tls_port,
            "--tls-cert-file",
            &cert,
            "--tls-key-file",
            &key,
            "--tls-ca-cert-file",
            &ca,
            "--tls-cluster",
            "yes",
        ])
    };
    let nodes: Vec<Node> = (0..3).map(|_| tls_node()).collect();
    let ranges = [(0u16, 5460u16), (5461, 10922), (10923, 16383)];
    for (node, (a, b)) in nodes.iter().zip(&ranges) {
        let r = node
            .conn()
            .cmd(&["CLUSTER", "ADDSLOTSRANGE", &a.to_string(), &b.to_string()]);
        assert!(r.is_ok(), "{r:?}");
    }
    for node in &nodes[1..] {
        let r = nodes[0]
            .conn()
            .cmd(&["CLUSTER", "MEET", "127.0.0.1", &node.port.to_string()]);
        assert!(r.is_ok(), "{r:?}");
    }
    wait_cluster_ok(&nodes.iter().collect::<Vec<_>>(), 3);
    // Announced at its TLS port, with the bus where it was.
    let line = nodes[1].my_row();
    assert!(
        line[1].ends_with(&format!("@{}", nodes[1].port + 10_000))
            && !line[1].starts_with(&format!("127.0.0.1:{}@", nodes[1].port)),
        "{line:?}"
    );

    // A replica, replicating over TLS.
    let replica = tls_node();
    let r = nodes[0]
        .conn()
        .cmd(&["CLUSTER", "MEET", "127.0.0.1", &replica.port.to_string()]);
    assert!(r.is_ok(), "{r:?}");
    let master_id = nodes[0].id();
    wait_until(
        "the replica to learn of its master",
        Duration::from_secs(15),
        || {
            replica
                .conn()
                .cmd(&["CLUSTER", "NODES"])
                .text()
                .contains(&master_id)
        },
    );
    assert!(
        replica
            .conn()
            .cmd(&["CLUSTER", "REPLICATE", &master_id])
            .is_ok()
    );
    let key = key_in(0..=5460, "tls");
    assert!(nodes[0].conn().cmd(&["SET", &key, "over-tls"]).is_ok());
    wait_until(
        "the replica to sync over TLS",
        Duration::from_secs(20),
        || replica.repl_info("master_link_status").as_deref() == Some("up"),
    );

    // Index definitions, over TLS to the other masters.
    let r = nodes[0]
        .conn()
        .cmd(&["FT.CREATE", "t", "ON", "HASH", "SCHEMA", "f", "TEXT"]);
    assert!(r.is_ok(), "{r:?}");
    for node in &nodes {
        let list = node.conn().cmd(&["FT._LIST"]);
        assert!(list.array().iter().any(|i| i.text() == "t"), "{list:?}");
    }

    // A slot migration, over TLS between source and destination.
    let mut d = nodes[1].conn();
    let id = d.cmd(&["CLUSTER", "MIGRATION", "IMPORT", "0", "99"]).text();
    wait_until("the migration over TLS", Duration::from_secs(30), || {
        let status = d.cmd(&["CLUSTER", "MIGRATION", "STATUS", "ID", &id]);
        status
            .array()
            .first()
            .is_some_and(|t| task_field(t, "state").text() == "completed")
    });

    // A node on a plain bus is not let in.
    let plain = Node::start(&[]);
    assert!(
        plain
            .conn()
            .cmd(&["CLUSTER", "MEET", "127.0.0.1", &nodes[0].port.to_string()])
            .is_ok()
    );
    std::thread::sleep(Duration::from_millis(3 * NODE_TIMEOUT_MS));
    assert_eq!(nodes[0].cluster_info("cluster_known_nodes"), "4");
    assert_eq!(plain.cluster_info("cluster_known_nodes"), "1");
}

// ── Search across the cluster ─────────────────────────────────────────────────

/// FT.SEARCH and FT.AGGREGATE sent to any master are answered from every
/// master: a sorted page is the cluster's first, the nearest vectors are the
/// cluster's nearest, a GROUPBY counts every document, a cursor reads them
/// all. With a master down the search fails rather than answer from part of
/// the data.
#[test]
fn searches_are_answered_from_every_master() {
    let (mut nodes, ranges) = form_cluster(3);
    let owner = |key: &str| {
        let slot = slot_of(key);
        ranges
            .iter()
            .position(|(a, b)| (*a..=*b).contains(&slot))
            .unwrap()
    };
    let r = nodes[0].conn().cmd(&[
        "FT.CREATE",
        "items",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "item:",
        "SCHEMA",
        "name",
        "TEXT",
        "price",
        "NUMERIC",
        "SORTABLE",
        "color",
        "TAG",
        "vec",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "2",
        "DISTANCE_METRIC",
        "L2",
    ]);
    assert!(r.is_ok(), "{r:?}");
    let colors = ["red", "green", "blue"];
    let vector = |x: f32| -> Vec<u8> { [x, 0.0f32].iter().flat_map(|f| f.to_le_bytes()).collect() };
    let mut on = [0usize; 3];
    for i in 0..60u32 {
        let key = format!("item:{i}");
        let node = owner(&key);
        on[node] += 1;
        let name = if i % 4 == 0 { "gadget" } else { "widget" };
        let price = i.to_string();
        let blob = vector(i as f32);
        let r = nodes[node].conn().cmd_bytes(&[
            b"HSET",
            key.as_bytes(),
            b"name",
            name.as_bytes(),
            b"price",
            price.as_bytes(),
            b"color",
            colors[(i % 3) as usize].as_bytes(),
            b"vec",
            &blob,
        ]);
        assert!(matches!(r, Resp::Int(4)), "{r:?}");
    }
    assert!(on.iter().all(|n| *n > 5), "{on:?}");

    let keys_of = |reply: &Resp| -> Vec<String> {
        reply.array()[1..]
            .iter()
            .filter_map(|r| match r {
                Resp::Bulk(b) if b.starts_with(b"item:") => {
                    Some(String::from_utf8_lossy(b).into_owned())
                }
                _ => None,
            })
            .collect()
    };
    let items = |ids: &[u32]| -> Vec<String> { ids.iter().map(|i| format!("item:{i}")).collect() };
    for node in &nodes {
        let mut c = node.conn();
        let page = c.cmd(&[
            "FT.SEARCH",
            "items",
            "*",
            "SORTBY",
            "price",
            "ASC",
            "LIMIT",
            "0",
            "5",
            "NOCONTENT",
        ]);
        assert_eq!(page.array()[0].int(), 60, "{page:?}");
        assert_eq!(keys_of(&page), items(&[0, 1, 2, 3, 4]));
        let page = c.cmd(&[
            "FT.SEARCH",
            "items",
            "*",
            "SORTBY",
            "price",
            "ASC",
            "LIMIT",
            "5",
            "5",
            "NOCONTENT",
        ]);
        assert_eq!(keys_of(&page), items(&[5, 6, 7, 8, 9]));
        let page = c.cmd(&[
            "FT.SEARCH",
            "items",
            "*",
            "SORTBY",
            "price",
            "DESC",
            "LIMIT",
            "0",
            "3",
            "RETURN",
            "1",
            "price",
        ]);
        assert_eq!(keys_of(&page), items(&[59, 58, 57]));
        assert_eq!(page.array()[2].array()[1].text(), "59");
        let red = c.cmd(&[
            "FT.SEARCH",
            "items",
            "@color:{red}",
            "NOCONTENT",
            "LIMIT",
            "0",
            "100",
        ]);
        assert_eq!(red.array()[0].int(), 20);
        assert_eq!(keys_of(&red).len(), 20);
        let gadgets = c.cmd(&[
            "FT.SEARCH",
            "items",
            "@name:gadget",
            "NOCONTENT",
            "LIMIT",
            "0",
            "100",
        ]);
        assert_eq!(gadgets.array()[0].int(), 15);
    }

    // The cluster's nearest four to 10.2: 10, 11, 9, 12.
    let blob = vector(10.2);
    let knn = nodes[1].conn().cmd_bytes(&[
        b"FT.SEARCH",
        b"items",
        b"*=>[KNN 4 @vec $v]",
        b"PARAMS",
        b"2",
        b"v",
        &blob,
        b"NOCONTENT",
        b"DIALECT",
        b"2",
    ]);
    assert_eq!(keys_of(&knn), items(&[10, 11, 9, 12]), "{knn:?}");

    // Counted and summed over every master.
    let groups = nodes[2].conn().cmd(&[
        "FT.AGGREGATE",
        "items",
        "*",
        "GROUPBY",
        "1",
        "@color",
        "REDUCE",
        "COUNT",
        "0",
        "AS",
        "n",
        "REDUCE",
        "SUM",
        "1",
        "@price",
        "AS",
        "total",
        "SORTBY",
        "2",
        "@color",
        "ASC",
    ]);
    let rows: Vec<Vec<String>> = groups.array()[1..]
        .iter()
        .map(|row| row.array().iter().map(Resp::text).collect())
        .collect();
    let sum = |c: u32| (0..60u32).filter(|i| i % 3 == c).sum::<u32>().to_string();
    let expect = |color: &str, c: u32| {
        let row = rows
            .iter()
            .find(|r| r.contains(&color.to_string()))
            .unwrap();
        let get = |name: &str| row[row.iter().position(|v| v == name).unwrap() + 1].clone();
        assert_eq!(get("n"), "20", "{rows:?}");
        assert_eq!(get("total"), sum(c), "{rows:?}");
    };
    expect("red", 0);
    expect("green", 1);
    expect("blue", 2);

    // A cursor over all of them, kept on the node asked.
    let mut c = nodes[0].conn();
    let first = c.cmd(&[
        "FT.AGGREGATE",
        "items",
        "*",
        "LOAD",
        "1",
        "@price",
        "WITHCURSOR",
        "COUNT",
        "25",
    ]);
    let mut seen = first.array()[0].array().len() - 1;
    let mut cursor = first.array()[1].int();
    while cursor != 0 {
        let more = c.cmd(&["FT.CURSOR", "READ", "items", &cursor.to_string()]);
        seen += more.array()[0].array().len() - 1;
        cursor = more.array()[1].int();
    }
    assert_eq!(seen, 60);

    let tags = nodes[0].conn().cmd(&["FT.TAGVALS", "items", "color"]);
    let tags: Vec<String> = tags.array().iter().map(Resp::text).collect();
    assert_eq!(tags, ["blue", "green", "red"]);

    // FT.INFO counts the documents of every master.
    let info = nodes[1].conn().cmd(&["FT.INFO", "items"]);
    let at = info
        .array()
        .iter()
        .position(|i| matches!(i, Resp::Bulk(b) if b == b"num_docs"))
        .unwrap();
    assert_eq!(info.array()[at + 1].int(), 60);

    // A master down: no answer from part of the data.
    nodes[2].kill();
    let down = nodes[0]
        .conn()
        .cmd(&["FT.SEARCH", "items", "*", "LIMIT", "0", "1"]);
    assert!(
        down.err()
            .is_some_and(|e| e.contains("could not be answered")),
        "{down:?}"
    );
}

/// A master added after an index was created gets it: from the first
/// search sent anywhere in the cluster, or from the master it imports slots
/// from, before their keys. Its definition is the same in every detail.
#[test]
fn a_master_added_later_is_given_the_index() {
    let (mut nodes, _) = form_cluster(3);
    let r = nodes[0].conn().cmd(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "2",
        "a:",
        "b:",
        "STOPWORDS",
        "1",
        "the",
        "SCHEMA",
        "title",
        "TEXT",
        "WEIGHT",
        "2.5",
        "NOSTEM",
        "tags",
        "AS",
        "t",
        "TAG",
        "SEPARATOR",
        ";",
        "CASESENSITIVE",
        "price",
        "NUMERIC",
        "SORTABLE",
        "name",
        "TEXT",
        "SORTABLE",
        "UNF",
        "v",
        "VECTOR",
        "HNSW",
        "10",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "COSINE",
        "M",
        "8",
        "EF_CONSTRUCTION",
        "100",
    ]);
    assert!(r.is_ok(), "{r:?}");
    let attributes = |node: &Node| -> String {
        let info = node.conn().cmd(&["FT.INFO", "idx"]);
        let at = info
            .array()
            .iter()
            .position(|i| matches!(i, Resp::Bulk(b) if b == b"attributes"))
            .unwrap();
        format!("{:?}", info.array()[at + 1])
    };
    let want = attributes(&nodes[0]);

    // Added empty, and given the index by a search.
    let late = Node::start(&[]);
    let r = nodes[0]
        .conn()
        .cmd(&["CLUSTER", "MEET", "127.0.0.1", &late.port.to_string()]);
    assert!(r.is_ok(), "{r:?}");
    nodes.push(late);
    wait_cluster_ok(&nodes.iter().collect::<Vec<_>>(), 4);
    let found = nodes[1]
        .conn()
        .cmd(&["FT.SEARCH", "idx", "*", "LIMIT", "0", "0"]);
    assert_eq!(found.array()[0].int(), 0, "{found:?}");
    assert_eq!(attributes(&nodes[3]), want);

    // Added, and given it by the master it imports from.
    let importer = Node::start(&[]);
    let r = nodes[0]
        .conn()
        .cmd(&["CLUSTER", "MEET", "127.0.0.1", &importer.port.to_string()]);
    assert!(r.is_ok(), "{r:?}");
    nodes.push(importer);
    wait_cluster_ok(&nodes.iter().collect::<Vec<_>>(), 5);
    let key = key_in(0..=99, "a");
    assert!(
        nodes[0]
            .conn()
            .cmd(&["HSET", &key, "title", "hello world", "price", "5"])
            .int()
            == 2
    );
    let mut d = nodes[4].conn();
    let id = d.cmd(&["CLUSTER", "MIGRATION", "IMPORT", "0", "99"]).text();
    wait_until("the import", Duration::from_secs(30), || {
        let status = d.cmd(&["CLUSTER", "MIGRATION", "STATUS", "ID", &id]);
        status
            .array()
            .first()
            .is_some_and(|t| task_field(t, "state").text() == "completed")
    });
    assert_eq!(attributes(&nodes[4]), want);
    let found = nodes[4]
        .conn()
        .cmd(&["FT.SEARCH", "idx", "hello", "NOCONTENT"]);
    assert_eq!(found.array()[0].int(), 1, "{found:?}");
}

/// What nodes send each other's client ports (index definitions, searches,
/// slot migration) logs in as `masteruser` / `masterauth`, as replication
/// does, with `default` limited the way a managed service limits it. The internal
/// user needs only the commands listed in `INTERNAL_USER`.
#[test]
fn nodes_talk_to_each_other_as_masteruser_with_default_limited() {
    const INTERNAL_USER: &[&str] = &[
        "on",
        ">int-s3cret",
        "-@all",
        "+auth",
        "+ping",
        "+psync",
        "+replconf",
        "+client|setname",
        "+cluster|syncslots",
        "+ft.create",
        "+ft.alter",
        "+ft.dropindex",
        "+ft.aliasadd",
        "+ft.aliasupdate",
        "+ft.aliasdel",
        "+ft.synupdate",
        "+ft.dictadd",
        "+ft.dictdel",
        "+ft.search",
        "+ft.aggregate",
        "+ft.info",
        "+ft.tagvals",
    ];
    let lock_down = |node: &Node| {
        let mut c = node.conn();
        let mut setuser = vec!["ACL", "SETUSER", "internal"];
        setuser.extend_from_slice(INTERNAL_USER);
        let r = c.cmd(&setuser);
        assert!(r.is_ok(), "{r:?}");
        let r = c.cmd(&["ACL", "SETUSER", "admin", "on", ">adm", "+@all", "~*", "&*"]);
        assert!(r.is_ok(), "{r:?}");
        assert!(c.cmd(&["CONFIG", "SET", "masteruser", "internal"]).is_ok());
        assert!(
            c.cmd(&["CONFIG", "SET", "masterauth", "int-s3cret"])
                .is_ok()
        );
        let r = c.cmd(&[
            "ACL",
            "SETUSER",
            "default",
            "reset",
            "on",
            "nopass",
            "-@all",
            "+auth",
            "+hello",
            "+psync",
            "+replconf",
            "+ping",
        ]);
        assert!(r.is_ok(), "{r:?}");
    };
    let admin = |node: &Node| {
        let mut c = node.conn();
        assert!(c.cmd(&["AUTH", "admin", "adm"]).is_ok());
        c
    };
    let (mut nodes, ranges) = form_cluster(3);
    for node in &nodes {
        lock_down(node);
    }
    let denied = nodes[0].conn().cmd(&["FT._LIST"]);
    assert!(
        denied.err().is_some_and(|e| e.contains("NOPERM")),
        "{denied:?}"
    );

    // Definitions reach every master.
    let r = admin(&nodes[0]).cmd(&[
        "FT.CREATE",
        "docs",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "doc:",
        "SCHEMA",
        "title",
        "TEXT",
        "n",
        "NUMERIC",
        "SORTABLE",
        "t",
        "TAG",
    ]);
    assert!(r.is_ok(), "{r:?}");
    for node in &nodes {
        let list = admin(node).cmd(&["FT._LIST"]);
        assert!(list.array().iter().any(|i| i.text() == "docs"), "{list:?}");
    }
    for (i, (a, b)) in ranges.iter().enumerate() {
        let key = key_in(*a..=*b, "doc");
        let r = admin(&nodes[i]).cmd(&["HSET", &key, "title", "hello", "n", "1", "t", "x"]);
        assert_eq!(r.int(), 3, "{r:?}");
    }
    // Searches answer from every master.
    let mut c = admin(&nodes[1]);
    let found = c.cmd(&["FT.SEARCH", "docs", "hello", "NOCONTENT"]);
    assert_eq!(found.array()[0].int(), 3, "{found:?}");
    let grouped = c.cmd(&[
        "FT.AGGREGATE",
        "docs",
        "*",
        "GROUPBY",
        "1",
        "@t",
        "REDUCE",
        "COUNT",
        "0",
        "AS",
        "c",
    ]);
    assert!(format!("{grouped:?}").contains("Bulk([51])"), "{grouped:?}");
    let tags = c.cmd(&["FT.TAGVALS", "docs", "t"]);
    assert_eq!(tags.array().len(), 1, "{tags:?}");
    let info = c.cmd(&["FT.INFO", "docs"]);
    let at = info
        .array()
        .iter()
        .position(|i| matches!(i, Resp::Bulk(b) if b == b"num_docs"))
        .unwrap();
    assert_eq!(info.array()[at + 1].int(), 3);

    // A master added later: given the index by a search, and slots by a
    // migration, both as the internal user.
    let late = Node::start(&[]);
    lock_down(&late);
    let r = admin(&nodes[0]).cmd(&["CLUSTER", "MEET", "127.0.0.1", &late.port.to_string()]);
    assert!(r.is_ok(), "{r:?}");
    nodes.push(late);
    wait_until("the late node to join", Duration::from_secs(30), || {
        nodes.iter().all(|n| {
            let info = admin(n).cmd(&["CLUSTER", "INFO"]).text();
            info.contains("cluster_state:ok") && info.contains("cluster_known_nodes:4")
        })
    });
    let found = admin(&nodes[2]).cmd(&["FT.SEARCH", "docs", "hello", "NOCONTENT"]);
    assert_eq!(found.array()[0].int(), 3, "{found:?}");
    let mut d = admin(&nodes[3]);
    let r = d.cmd(&["CLUSTER", "MIGRATION", "IMPORT", "0", "99"]);
    let id = r.text();
    wait_until(
        "the import as the internal user",
        Duration::from_secs(30),
        || {
            let status = d.cmd(&["CLUSTER", "MIGRATION", "STATUS", "ID", &id]);
            status
                .array()
                .first()
                .is_some_and(|t| task_field(t, "state").text() == "completed")
        },
    );
    let found = admin(&nodes[3]).cmd(&["FT.SEARCH", "docs", "hello", "NOCONTENT"]);
    assert_eq!(found.array()[0].int(), 3, "{found:?}");
}

/// A shard that runs out of time tells the coordinator, which applies
/// `ON_TIMEOUT`: `fail` is an error, `return` a partial reply. RESP3 puts
/// the warning on the coordinator's reply; over RESP2 there is none.
/// Documents are loaded until a 1 ms search times out, then four times as
/// many again, so that a fast host does not finish the queries after it.
#[test]
fn a_timed_out_search_is_reported_across_the_cluster() {
    let (nodes, ranges) = form_cluster(3);
    let owner = |key: &str| {
        let slot = slot_of(key);
        ranges
            .iter()
            .position(|(a, b)| (*a..=*b).contains(&slot))
            .unwrap()
    };
    let r = nodes[0].conn().cmd(&[
        "FT.CREATE",
        "to",
        "PREFIX",
        "1",
        "to:",
        "SCHEMA",
        "t",
        "TEXT",
    ]);
    assert!(r.is_ok(), "{r:?}");

    let insert = |start: usize, n: usize| {
        let mut buckets: Vec<Vec<String>> = vec![Vec::new(); nodes.len()];
        for i in start..start + n {
            let key = format!("to:{i}");
            let node = owner(&key);
            buckets[node].push(key);
        }
        for (i, keys) in buckets.iter().enumerate() {
            if keys.is_empty() {
                continue;
            }
            let mut c = nodes[i].conn();
            for chunk in keys.chunks(250) {
                let cmds: Vec<Vec<&str>> = chunk
                    .iter()
                    .map(|k| vec!["HSET", k.as_str(), "t", "x"])
                    .collect();
                for r in c.pipeline(&cmds) {
                    assert!(matches!(r, Resp::Int(_)), "{r:?}");
                }
            }
        }
    };

    let warned = |reply: &Resp| {
        reply.map().iter().any(|(k, v)| {
            k.text() == "warning"
                && v.array()
                    .iter()
                    .any(|w| w.text().contains("Timeout limit was reached"))
        })
    };

    let mut n = 0usize;
    let mut timed_out = false;
    while n < 200_000 {
        insert(n, 8_000);
        n += 8_000;
        let mut c = nodes[0].conn();
        let hello = c.cmd(&["HELLO", "3"]);
        assert!(matches!(hello, Resp::Map(_)), "{hello:?}");
        // Twice in a row: one search held up by the host, not by its
        // documents, is not the size to grow from.
        let mut late = || {
            let reply = c.cmd(&["FT.SEARCH", "to", "*", "NOCONTENT", "TIMEOUT", "1"]);
            warned(&reply)
        };
        if late() && late() {
            timed_out = true;
            break;
        }
    }
    assert!(
        timed_out,
        "TIMEOUT 1 still finished after {n} documents across the cluster"
    );
    insert(n, 4 * n);
    n *= 5;

    // No limit: every master's part, whole.
    let mut c = nodes[1].conn();
    let all = c.cmd(&[
        "FT.SEARCH",
        "to",
        "*",
        "NOCONTENT",
        "LIMIT",
        "0",
        "0",
        "TIMEOUT",
        "0",
    ]);
    assert_eq!(all.array()[0].int(), n as i64, "{all:?}");
    let count = |c: &mut Conn, timeout: &str| {
        c.cmd(&[
            "FT.AGGREGATE",
            "to",
            "*",
            "TIMEOUT",
            timeout,
            "GROUPBY",
            "0",
            "REDUCE",
            "COUNT",
            "0",
            "AS",
            "c",
        ])
    };
    let all = count(&mut c, "0");
    assert_eq!(all.array()[1].array()[1].text(), n.to_string(), "{all:?}");

    assert!(c.cmd(&["FT.CONFIG", "SET", "ON_TIMEOUT", "fail"]).is_ok());
    let failed = c.cmd(&["FT.SEARCH", "to", "*", "TIMEOUT", "1"]);
    assert!(c.cmd(&["FT.CONFIG", "SET", "ON_TIMEOUT", "return"]).is_ok());
    assert!(
        failed
            .err()
            .is_some_and(|e| e.contains("Timeout limit was reached")),
        "{failed:?}"
    );

    assert!(c.cmd(&["FT.CONFIG", "SET", "ON_TIMEOUT", "fail"]).is_ok());
    let failed = count(&mut c, "1");
    assert!(c.cmd(&["FT.CONFIG", "SET", "ON_TIMEOUT", "return"]).is_ok());
    assert!(
        failed
            .err()
            .is_some_and(|e| e.contains("Timeout limit was reached")),
        "{failed:?}"
    );

    let mut c = nodes[2].conn();
    let hello = c.cmd(&["HELLO", "3"]);
    assert!(matches!(hello, Resp::Map(_)), "{hello:?}");
    let reply = c.cmd(&["FT.SEARCH", "to", "*", "NOCONTENT", "TIMEOUT", "1"]);
    assert!(warned(&reply), "{reply:?}");
    let reply = count(&mut c, "1");
    assert!(warned(&reply), "{reply:?}");

    // Over RESP2: a count short of the whole, and no error.
    let mut c = nodes[0].conn();
    let reply = c.cmd(&[
        "FT.SEARCH",
        "to",
        "*",
        "NOCONTENT",
        "LIMIT",
        "0",
        "0",
        "TIMEOUT",
        "1",
    ]);
    assert!(reply.array()[0].int() < n as i64, "{reply:?}");
}
