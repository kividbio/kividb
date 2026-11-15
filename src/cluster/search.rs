// src/cluster/search.rs
//
// Searching a whole cluster.
//
// Each master indexes the keys it holds. FT.SEARCH and FT.AGGREGATE sent to
// a node of a cluster are answered from all of them: the node that receives
// the command (the coordinator) sends it on to every other master, over the
// connections and with the credentials replication uses, marked so that the
// receiving node answers from its own keys rather than sending it on again.
// The coordinator merges what comes back:
//
//   FT.SEARCH     each master is asked for the first offset+count matches,
//                 with their scores and sort keys; the coordinator orders
//                 them all as one node would, and takes the page asked for.
//                 The total is the sum of the masters' totals. A master's
//                 reply says whether its scan ran out of time; the
//                 coordinator ORs those flags and applies ON_TIMEOUT.
//   FT.AGGREGATE  each master sends the rows that match the query, before
//                 any step, and whether it timed out; the coordinator runs
//                 the steps (GROUPBY, REDUCE, APPLY, FILTER, SORTBY, LIMIT)
//                 over all of them, so every reducer is exact, then applies
//                 ON_TIMEOUT. WITHCURSOR keeps the cursor on the coordinator.
//
// Whether a master's part timed out travels only when the coordinator asks
// for it (see `ask_for_timeout_flag`), so that during a rolling upgrade
// each side gets the reply it understands: a master from before the flag
// does not know the question and answers as it always did, and a
// coordinator from before it never asks.
//
// Relevance (BM25) is computed by each master over its own documents, as in
// RediSearch's coordinator. A node answers only for the slots it serves, so
// keys part-way through a migration are not counted twice.

use crate::cluster::ClusterState;
use crate::cluster::node::NodeFlags;
use std::io::{BufRead, BufReader, Write};
use std::net::ToSocketAddrs;
use std::time::Duration;

/// The client name a coordinator's requests are sent under.
pub const FORWARDED: &str = "kividb-cluster-search";

const TIMEOUT: Duration = Duration::from_secs(10);

/// A reply as read off the wire.
#[derive(Debug, Clone)]
pub enum Reply {
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Simple(String),
    Error(String),
    Array(Vec<Reply>),
}

impl Reply {
    pub fn text(&self) -> Option<String> {
        match self {
            Reply::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).into_owned()),
            Reply::Simple(s) => Some(s.clone()),
            Reply::Int(n) => Some(n.to_string()),
            _ => None,
        }
    }
}

/// Read one reply. RESP2, as a coordinator's connections speak it.
pub fn read_reply(reader: &mut impl BufRead) -> Result<Reply, String> {
    read_reply_within(reader, 0)
}

/// The deepest a reply from another node may nest, and the longest string
/// in it: what a search or an index's definition can come to, with room.
const REPLY_DEPTH_MAX: u32 = 32;
const REPLY_BULK_MAX: i64 = 512 * 1024 * 1024;

fn read_reply_within(reader: &mut impl BufRead, depth: u32) -> Result<Reply, String> {
    if depth > REPLY_DEPTH_MAX {
        return Err("a reply nested too deep".into());
    }
    let mut line = String::new();
    if reader.read_line(&mut line).map_err(|e| e.to_string())? == 0 {
        return Err("connection closed".into());
    }
    let line = line.trim_end_matches(['\r', '\n']);
    let (kind, rest) = line.split_at(line.len().min(1));
    let number = || {
        rest.parse::<i64>()
            .map_err(|_| format!("bad reply: {line}"))
    };
    match kind {
        "+" => Ok(Reply::Simple(rest.to_string())),
        "-" => Ok(Reply::Error(rest.to_string())),
        ":" => Ok(Reply::Int(number()?)),
        "$" => {
            let len = number()?;
            if len < 0 {
                return Ok(Reply::Bulk(None));
            }
            if len > REPLY_BULK_MAX {
                return Err(format!("bad reply: {line}"));
            }
            let mut body = vec![0u8; len as usize + 2];
            reader.read_exact(&mut body).map_err(|e| e.to_string())?;
            body.truncate(len as usize);
            Ok(Reply::Bulk(Some(body)))
        }
        "*" => {
            let n = number()?;
            if n < 0 {
                return Ok(Reply::Bulk(None));
            }
            // As many as the other node says, but room made only for some.
            let mut items = Vec::with_capacity((n as usize).min(1024));
            for _ in 0..n {
                items.push(read_reply_within(reader, depth + 1)?);
            }
            Ok(Reply::Array(items))
        }
        _ => Err(format!("unexpected reply: {line}")),
    }
}

/// Parse a reply this node produced itself.
pub fn parse_reply(bytes: &[u8]) -> Result<Reply, String> {
    read_reply(&mut BufReader::new(bytes))
}

/// The slots this node answers for, as [`serves`] has them: one bit each,
/// read off the slot table at once. A query that filters many keys looks
/// each one's slot up here instead of taking three locks for it.
pub fn served_slots(cluster: &ClusterState) -> Vec<u64> {
    let me = cluster.myself();
    let master = cluster.my_master_id();
    let slots = cluster.slots.read();
    let mut served = vec![0u64; slots.len().div_ceil(64)];
    for (slot, owner) in slots.iter().enumerate() {
        if owner.is_some() && (*owner == Some(me) || *owner == master) {
            served[slot / 64] |= 1 << (slot % 64);
        }
    }
    served
}

/// Whether this node answers for `key`: its slot is this node's, or, on a
/// replica, its master's.
///
/// It reads the slot table: never with a shard locked (see `ClusterState`).
/// A walk of the keyspace takes [`served_slots`] before, and asks
/// [`in_served`].
pub fn serves(cluster: &ClusterState, key: &(impl AsRef<[u8]> + ?Sized)) -> bool {
    let owner = cluster.slot_owner(crate::cluster::key_hash_slot(key.as_ref()));
    owner.is_some() && (owner == Some(cluster.myself()) || owner == cluster.my_master_id())
}

/// Whether `key`'s slot is one of `served` ([`served_slots`]).
pub fn in_served(served: &[u64], key: &(impl AsRef<[u8]> + ?Sized)) -> bool {
    let slot = crate::cluster::key_hash_slot(key.as_ref()) as usize;
    served[slot / 64] & (1 << (slot % 64)) != 0
}

/// Whether this command came from a coordinator.
pub fn is_forwarded(ctx: &crate::commands::ctx::Ctx) -> bool {
    ctx.cluster.enabled
        && ctx
            .client_registry
            .get(ctx.client_id)
            .is_some_and(|c| c.name == FORWARDED)
}

/// The masters, other than the one whose data this node holds, that a
/// coordinator asks.
fn others(cluster: &ClusterState) -> Vec<(String, u16)> {
    let me = cluster.myself();
    let mine = cluster.my_master_id().unwrap_or(me);
    cluster
        .nodes
        .read()
        .values()
        .filter(|n| {
            n.id != me
                && n.id != mine
                && n.flags.contains(NodeFlags::MASTER)
                && !n.flags.contains(NodeFlags::HANDSHAKE)
                && !n.ip.is_empty()
                && n.client_port != 0
        })
        .map(|n| (n.ip.clone(), n.client_port))
        .collect()
}

/// Whether this node coordinates a search: in a cluster with other masters,
/// for a client (not a coordinator), and not inside a transaction or a
/// script, which hold the server's gates (a search there answers from this
/// node, as before).
pub fn coordinates(ctx: &crate::commands::ctx::Ctx) -> bool {
    ctx.cluster.enabled
        && !crate::write_gate::held_by_this_thread()
        && !crate::command_gate::alone_on_this_thread()
        && !is_forwarded(ctx)
        && !others(ctx.cluster).is_empty()
}

fn encode(parts: &[&[u8]]) -> Vec<u8> {
    let owned: Vec<Vec<u8>> = parts.iter().map(|p| p.to_vec()).collect();
    crate::resp::write::encode_resp_command(&owned)
}

// ── The connections a coordinator asks the other masters over ────────────────
//
// A search used to dial each other master afresh, log in, and close the
// connection when the reply was in. The coordinator closed first, so every
// search left a socket per master in TIME_WAIT on the coordinator, and a few
// thousand searches a second used up its ephemeral ports (#723). They are
// kept open now, logged in, and reused.

/// How many idle connections to one master, opened as one login, are kept
/// for the next search, at least: as many as the node has command threads,
/// if that is more (see [`Pool::size_for`]). A burst of searches may open
/// more at once; those beyond this are closed when their search is done.
pub(crate) const POOL_IDLE_MAX: usize = 16;

/// What a kept connection was opened as: to where, logged in as whom, in
/// which database, over which setup of TLS between nodes. A connection is
/// reused only for the same key, so one logged in with `masteruser` and
/// `masterauth` as they were, or made with TLS files since replaced, is
/// never used once they change.
#[derive(Clone, PartialEq, Eq, Hash)]
struct PoolKey {
    host: String,
    port: u16,
    user: String,
    /// The password's SHA-256, not the password: a password no longer in
    /// use is not kept in memory for the pool's sake.
    password: [u8; 32],
    db: usize,
    /// [`crate::node_tls::generation`], when TLS between nodes is on.
    tls: Option<u64>,
}

impl PoolKey {
    fn new(host: &str, port: u16, credentials: &(String, String), db: usize) -> Self {
        use sha2::Digest;
        Self {
            host: host.to_string(),
            port,
            user: credentials.0.clone(),
            password: sha2::Sha256::digest(credentials.1.as_bytes()).into(),
            db,
            tls: crate::node_tls::cluster().then(crate::node_tls::generation),
        }
    }

    /// Whether `self` was logged in as `other` is: the same user, password
    /// and TLS setup, whatever the address and database.
    fn same_login(&self, other: &PoolKey) -> bool {
        self.user == other.user && self.password == other.password && self.tls == other.tls
    }
}

struct Conn {
    reader: BufReader<Box<dyn crate::node_tls::ReadWrite>>,
    /// The socket under `reader`, for its timeouts.
    socket: std::net::TcpStream,
}

/// Idle connections to the other masters, by what they were opened as. See
/// [`ask_once`] for when one is kept and when it is closed.
pub struct Pool {
    idle: parking_lot::Mutex<ahash::AHashMap<PoolKey, Vec<Conn>>>,
    /// How many idle connections are kept for one key.
    most: std::sync::atomic::AtomicUsize,
}

impl Default for Pool {
    fn default() -> Self {
        Self {
            idle: Default::default(),
            most: std::sync::atomic::AtomicUsize::new(POOL_IDLE_MAX),
        }
    }
}

impl Pool {
    /// Keep as many idle connections to a master as the node has command
    /// threads, each of which may be searching at once, or
    /// [`POOL_IDLE_MAX`] if that is more. Fewer, and a burst on a large
    /// host would open and close the difference every time.
    pub(crate) fn size_for(&self, threads: usize) {
        self.most.store(
            threads.max(POOL_IDLE_MAX),
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    fn most(&self) -> usize {
        self.most.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn take(&self, key: &PoolKey) -> Option<Conn> {
        self.idle.lock().get_mut(key).and_then(Vec::pop)
    }

    /// Keep `conn` for the next search, unless as many are kept already.
    fn put(&self, key: &PoolKey, conn: Conn) {
        let most = self.most();
        let extra = {
            let mut idle = self.idle.lock();
            let conns = idle.entry(key.clone()).or_default();
            if conns.len() < most {
                conns.push(conn);
                None
            } else {
                Some(conn)
            }
        };
        // Closed outside the lock.
        drop(extra);
    }

    /// Close the connections that are not to one of `targets`, or not
    /// logged in as `login` is: to a master forgotten, moved, or no longer
    /// a master, or opened with credentials or TLS files since changed. So
    /// what is kept never grows past [`Pool::size_for`] for each master
    /// there is now, and each database searched.
    fn keep_only(&self, targets: &[(String, u16)], login: &PoolKey) {
        let mut gone = Vec::new();
        {
            let mut idle = self.idle.lock();
            let stale: Vec<PoolKey> = idle
                .keys()
                .filter(|k| {
                    !k.same_login(login)
                        || !targets.iter().any(|(h, p)| *h == k.host && *p == k.port)
                })
                .cloned()
                .collect();
            for key in stale {
                gone.extend(idle.remove(&key));
            }
        }
        drop(gone);
    }

    /// How many connections are kept, to all masters.
    #[cfg(test)]
    fn idle(&self) -> usize {
        self.idle.lock().values().map(Vec::len).sum()
    }
}

/// How an exchange on a connection failed.
enum Failed {
    /// No byte of the reply came: the connection was found closed (a kept
    /// one, by the other node's restart, its `timeout`, or `CLIENT KILL`).
    /// The command did not run, or ran and said nothing; a search changes
    /// nothing, so it may be sent again.
    BeforeReply(String),
    /// Anything else: a timeout, an error after part of the reply, a reply
    /// that could not be read, a refused login.
    Other(String),
}

fn is_timeout(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
    )
}

/// Send `out` on `conn` and read the replies to its first `preamble`
/// commands, which must not be errors, then the reply to the last.
fn exchange(conn: &mut Conn, out: &[u8], preamble: usize) -> Result<Reply, Failed> {
    // The timeouts of every request, whichever connection it goes on.
    let _ = conn.socket.set_read_timeout(Some(TIMEOUT));
    let _ = conn.socket.set_write_timeout(Some(TIMEOUT));
    let writer = conn.reader.get_mut();
    if let Err(e) = writer.write_all(out).and_then(|()| writer.flush()) {
        return Err(if is_timeout(&e) {
            Failed::Other(e.to_string())
        } else {
            Failed::BeforeReply(e.to_string())
        });
    }
    match conn.reader.fill_buf() {
        Ok([]) => return Err(Failed::BeforeReply("connection closed".into())),
        Ok(_) => {}
        Err(e) if is_timeout(&e) => return Err(Failed::Other(e.to_string())),
        Err(e) => return Err(Failed::BeforeReply(e.to_string())),
    }
    for _ in 0..preamble {
        if let Reply::Error(e) = read_reply(&mut conn.reader).map_err(Failed::Other)? {
            return Err(Failed::Other(e));
        }
    }
    read_reply(&mut conn.reader).map_err(Failed::Other)
}

/// A new connection to `host:port`, and what to send on it first: the
/// login, the name, and the database, before the command.
fn dial(
    host: &str,
    port: u16,
    credentials: &(String, String),
    db: usize,
) -> Result<(Conn, Vec<u8>, usize), String> {
    let addr = (host, port)
        .to_socket_addrs()
        .ok()
        .and_then(|mut it| it.next())
        .ok_or("could not resolve")?;
    let (stream, socket) =
        crate::node_tls::connect_node(host, &addr, TIMEOUT).map_err(|e| e.to_string())?;
    let (user, password) = credentials;
    let mut out = Vec::new();
    let mut preamble = 0;
    if !password.is_empty() {
        out.extend(if user.is_empty() {
            // As the default user, by name: `AUTH <password>` is refused by a
            // node that has no password (#135), and with the name it logs in as
            // before.
            encode(&[b"AUTH", b"default", password.as_bytes()])
        } else {
            encode(&[b"AUTH", user.as_bytes(), password.as_bytes()])
        });
        preamble += 1;
    }
    out.extend(encode(&[b"CLIENT", b"SETNAME", FORWARDED.as_bytes()]));
    preamble += 1;
    // A cluster has database 0 only; SELECT would be one more command the
    // user these connections log in as must be allowed.
    if db != 0 {
        out.extend(encode(&[b"SELECT", db.to_string().as_bytes()]));
        preamble += 1;
    }
    let conn = Conn {
        reader: BufReader::new(stream),
        socket,
    };
    Ok((conn, out, preamble))
}

/// [`ask_once`]; and a master that does not have the index (added to the
/// cluster after it was created) is given it, and asked again.
fn ask(
    pool: &Pool,
    host: &str,
    port: u16,
    credentials: &(String, String),
    db: usize,
    cmd: &[Vec<u8>],
    definition: Option<&[Vec<u8>]>,
) -> Result<Reply, String> {
    match ask_once(pool, host, port, credentials, db, cmd) {
        Err(e) if crate::commands::search::is_no_such_index(&e) && definition.is_some() => {
            let create: Vec<&[u8]> = definition.unwrap().iter().map(Vec::as_slice).collect();
            crate::cluster::ddl::run_on(host, port, &credentials.0, &credentials.1, &create)?;
            log_info!("Cluster: gave {host}:{port} the definition of an index it lacked");
            ask_once(pool, host, port, credentials, db, cmd)
        }
        other => other,
    }
}

/// Send `cmd` to `host:port` and read its reply: on a kept connection if
/// there is one, else on a new one.
///
/// A connection goes back to the pool only once a whole reply has been
/// read off it, which leaves it ready for the next command; one that failed
/// in any way, timed out, or gave part of a reply is closed. A kept
/// connection found closed before any of the reply came is given up, and
/// the command is sent once more on a new one: searches change nothing, so
/// sending one twice is harmless. Nothing is sent again after part of a
/// reply.
fn ask_once(
    pool: &Pool,
    host: &str,
    port: u16,
    credentials: &(String, String),
    db: usize,
    cmd: &[Vec<u8>],
) -> Result<Reply, String> {
    let key = PoolKey::new(host, port, credentials, db);
    let parts: Vec<&[u8]> = cmd.iter().map(Vec::as_slice).collect();
    let command = encode(&parts);
    if let Some(mut conn) = pool.take(&key) {
        match exchange(&mut conn, &command, 0) {
            Ok(reply) => return done(pool, &key, conn, reply),
            // Closed here; sent again below.
            Err(Failed::BeforeReply(_)) => {}
            Err(Failed::Other(e)) => return Err(e),
        }
    }
    let (mut conn, mut out, preamble) = dial(host, port, credentials, db)?;
    out.extend(command);
    match exchange(&mut conn, &out, preamble) {
        Ok(reply) => done(pool, &key, conn, reply),
        Err(Failed::BeforeReply(e) | Failed::Other(e)) => Err(e),
    }
}

/// The reply, read whole off `conn`; and `conn` back in the pool.
fn done(pool: &Pool, key: &PoolKey, conn: Conn, reply: Reply) -> Result<Reply, String> {
    // A reply read whole leaves nothing behind it. Anything there would be
    // read as the next command's reply.
    let clean = conn.reader.buffer().is_empty();
    match reply {
        Reply::Error(e) => {
            // An error reply is a whole reply too, and the connection is as
            // good as before: unless it is no longer logged in.
            if clean && !e.starts_with("NOAUTH") && !e.starts_with("WRONGPASS") {
                pool.put(key, conn);
            }
            Err(e)
        }
        reply => {
            if clean {
                pool.put(key, conn);
            }
            Ok(reply)
        }
    }
}

/// Send `cmd` to every other master (see [`others`]) and collect their
/// replies. An error if any of them cannot answer: a search of part of the
/// data is not one of the whole.
pub fn fan_out(ctx: &crate::commands::ctx::Ctx, cmd: &[Vec<u8>]) -> Result<Vec<Reply>, String> {
    let targets = others(ctx.cluster);
    let credentials = ctx.repl_state.master_credentials();
    let db = *ctx.selected_db;
    // The index's definition, for a master that lacks it.
    let definition: Option<Vec<Vec<u8>>> = cmd.get(1).and_then(|name| {
        let name = String::from_utf8_lossy(name);
        let store = ctx.index_store.read();
        let real = crate::vector::alias_target(&store, &name).unwrap_or(name.to_string());
        store.get(&real).map(|def| def.create_args())
    });
    let definition = definition.as_deref();
    let pool = &ctx.cluster.search_pool;
    pool.keep_only(&targets, &PoolKey::new("", 0, &credentials, db));
    let results: Vec<Result<Reply, String>> = std::thread::scope(|scope| {
        let handles: Vec<_> = targets
            .iter()
            .map(|(host, port)| {
                let credentials = &credentials;
                scope.spawn(move || {
                    ask(pool, host, *port, credentials, db, cmd, definition)
                        .map_err(|e| format!("{host}:{port}: {e}"))
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap_or_else(|_| Err("panicked".into())))
            .collect()
    });
    let mut replies = Vec::with_capacity(results.len());
    let mut failed = Vec::new();
    for result in results {
        match result {
            Ok(reply) => replies.push(reply),
            Err(e) => failed.push(e),
        }
    }
    if failed.is_empty() {
        Ok(replies)
    } else {
        Err(format!(
            "ERR the search could not be answered by {} of the cluster's masters: {}",
            failed.len(),
            failed.join("; ")
        ))
    }
}

/// The parameter a coordinator adds to what it asks a master, to be told
/// whether that master's part ran out of time. A parameter, because every
/// version takes `PARAMS` anywhere in FT.SEARCH and FT.AGGREGATE, and one
/// the query does not use changes nothing: a master from before the flag
/// ignores it, and answers in the shape it always did.
const TIMEOUT_FLAG_PARAM: &[u8] = b"__kividb_timeout_flag";
const SCORES_MATTER_PARAM: &[u8] = b"__kividb_scores_matter";

pub fn ask_for_scores_matter(cmd: &mut Vec<Vec<u8>>, matters: bool) {
    cmd.extend([
        b"PARAMS".to_vec(),
        b"2".to_vec(),
        SCORES_MATTER_PARAM.to_vec(),
        if matters {
            b"1".to_vec()
        } else {
            b"0".to_vec()
        },
    ]);
}

pub fn scores_matter(cmd: &[&[u8]]) -> Option<bool> {
    let mut hint = None;
    let mut i = 0;
    while i + 1 < cmd.len() {
        if !cmd[i].eq_ignore_ascii_case(b"PARAMS") {
            i += 1;
            continue;
        }
        let n = std::str::from_utf8(cmd[i + 1])
            .ok()
            .and_then(|n| n.parse::<usize>().ok());
        let Some(n) = n else {
            i += 1;
            continue;
        };
        let start = i + 2;
        let end = start.saturating_add(n).min(cmd.len());
        let (pairs, _) = cmd[start..end].as_chunks::<2>();
        for pair in pairs {
            if pair[0] == SCORES_MATTER_PARAM {
                hint = Some(pair[1] != b"0");
            }
        }
        i = end;
    }
    hint
}

/// Ask the masters for their timeout flag: `PARAMS 2 <name> 1`, last.
pub fn ask_for_timeout_flag(cmd: &mut Vec<Vec<u8>>) {
    cmd.extend([
        b"PARAMS".to_vec(),
        b"2".to_vec(),
        TIMEOUT_FLAG_PARAM.to_vec(),
        b"1".to_vec(),
    ]);
}

/// Whether a coordinator asked this master for its timeout flag (see
/// [`ask_for_timeout_flag`]).
pub fn wants_timeout_flag(cmd: &[&[u8]]) -> bool {
    matches!(
        cmd,
        [.., params, two, name, one]
            if params.eq_ignore_ascii_case(b"PARAMS")
                && *two == b"2"
                && *name == TIMEOUT_FLAG_PARAM
                && *one == b"1"
    )
}

/// A master's FT.SEARCH reply to a coordinator that asked for the flag:
/// the RESP2 page, then whether that master's scan ran out of time.
pub fn encode_search(page: Vec<u8>, timed_out: bool) -> Vec<u8> {
    use crate::resp::write::{resp_array, resp_integer};
    resp_array(vec![page, resp_integer(i64::from(timed_out))])
}

/// The page and timeout flag in a master's reply to [`encode_search`]. A
/// master from before the flag answers with the page alone, which begins
/// with its total, an integer, where the flagged reply begins with the
/// page, an array: that page did not time out.
pub fn decode_search(reply: Reply) -> Result<(Reply, bool), String> {
    match reply {
        Reply::Array(mut items) if matches!(items.as_slice(), [Reply::Array(_), Reply::Int(_)]) => {
            let timed_out = matches!(items.pop(), Some(Reply::Int(n)) if n != 0);
            Ok((items.remove(0), timed_out))
        }
        Reply::Array(items) if matches!(items.first(), Some(Reply::Int(_))) => {
            Ok((Reply::Array(items), false))
        }
        _ => Err("ERR a master answered the search with something else".into()),
    }
}

/// The rows of an aggregation, as a master sends them to its coordinator:
/// every column of every row, then, if the coordinator asked for it (see
/// [`ask_for_timeout_flag`]), whether that master's scan ran out of time.
pub fn encode_rows(rows: &[ahash::AHashMap<String, String>], timed_out: Option<bool>) -> Vec<u8> {
    use crate::resp::write::{resp_array, resp_bulk_str, resp_integer};
    let body = resp_array(
        rows.iter()
            .map(|row| {
                resp_array(
                    row.iter()
                        .flat_map(|(k, v)| [resp_bulk_str(k), resp_bulk_str(v)])
                        .collect(),
                )
            })
            .collect(),
    );
    match timed_out {
        Some(timed_out) => resp_array(vec![body, resp_integer(i64::from(timed_out))]),
        None => body,
    }
}

/// The rows and timeout flag in a master's reply to [`encode_rows`]. A
/// master from before the flag sends the rows alone: an array of arrays,
/// where the flagged reply ends with an integer. Those rows did not time
/// out.
pub fn decode_rows(reply: Reply) -> (Vec<ahash::AHashMap<String, String>>, bool) {
    let (rows, timed_out) = match reply {
        Reply::Array(mut wrap) if matches!(wrap.as_slice(), [Reply::Array(_), Reply::Int(_)]) => {
            let timed_out = matches!(wrap.pop(), Some(Reply::Int(n)) if n != 0);
            (wrap.remove(0), timed_out)
        }
        rows => (rows, false),
    };
    let rows = match rows {
        Reply::Array(rows) => rows
            .into_iter()
            .filter_map(|row| match row {
                Reply::Array(cells) => Some(
                    cells
                        .chunks(2)
                        .filter_map(|pair| Some((pair.first()?.text()?, pair.get(1)?.text()?)))
                        .collect(),
                ),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    };
    (rows, timed_out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stand-in for another master: it answers each command with an
    /// array holding the number of connections it has accepted, and can
    /// close every connection it has, as `CLIENT KILL` or a restart would.
    struct Peer {
        port: u16,
        accepted: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        open: std::sync::Arc<parking_lot::Mutex<Vec<std::net::TcpStream>>>,
    }

    impl Peer {
        fn start() -> Self {
            use std::sync::atomic::Ordering;
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let port = listener.local_addr().unwrap().port();
            let accepted = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let open = std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
            let (count, streams) = (accepted.clone(), open.clone());
            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    let Ok(stream) = stream else { return };
                    let n = count.fetch_add(1, Ordering::SeqCst) + 1;
                    streams.lock().push(stream.try_clone().unwrap());
                    std::thread::spawn(move || {
                        let mut writer = stream.try_clone().unwrap();
                        let mut reader = BufReader::new(stream);
                        while let Ok(Reply::Array(cmd)) = read_reply(&mut reader) {
                            let name = cmd.first().and_then(Reply::text).unwrap_or_default();
                            let reply = if name.eq_ignore_ascii_case("FT.SEARCH") {
                                format!("*1\r\n:{n}\r\n")
                            } else {
                                "+OK\r\n".to_string()
                            };
                            if writer.write_all(reply.as_bytes()).is_err() {
                                return;
                            }
                        }
                    });
                }
            });
            Peer {
                port,
                accepted,
                open,
            }
        }

        fn accepted(&self) -> usize {
            self.accepted.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn close_all(&self) {
            for stream in self.open.lock().drain(..) {
                let _ = stream.shutdown(std::net::Shutdown::Both);
            }
        }
    }

    fn search() -> Vec<Vec<u8>> {
        vec![b"FT.SEARCH".to_vec(), b"idx".to_vec(), b"*".to_vec()]
    }

    /// Searches reuse one connection; one closed while kept is replaced
    /// and the search sent again on the new one; a change of credentials
    /// opens a new one and the old is closed; the pool keeps at most
    /// [`POOL_IDLE_MAX`] to a master (#723).
    #[test]
    fn searches_reuse_a_kept_connection_and_survive_its_closing() {
        let peer = Peer::start();
        let pool = Pool::default();
        let open = (String::new(), String::new());
        let at = |reply: Reply| match reply {
            Reply::Array(items) => match items.as_slice() {
                [Reply::Int(n)] => *n as usize,
                other => panic!("{other:?}"),
            },
            other => panic!("{other:?}"),
        };
        for _ in 0..200 {
            let reply = ask_once(&pool, "127.0.0.1", peer.port, &open, 0, &search()).unwrap();
            assert_eq!(at(reply), 1);
        }
        assert_eq!(peer.accepted(), 1);
        assert_eq!(pool.idle(), 1);

        // Closed by the other node while kept: sent again on a new one.
        peer.close_all();
        let reply = ask_once(&pool, "127.0.0.1", peer.port, &open, 0, &search()).unwrap();
        assert_eq!(at(reply), 2);
        assert_eq!(peer.accepted(), 2);
        assert_eq!(pool.idle(), 1);

        // New credentials: a new connection, never the old one.
        let changed = ("internal".to_string(), "s3cret".to_string());
        let reply = ask_once(&pool, "127.0.0.1", peer.port, &changed, 0, &search()).unwrap();
        assert_eq!(at(reply), 3);
        assert_eq!(pool.idle(), 2);
        let targets = [("127.0.0.1".to_string(), peer.port)];
        pool.keep_only(&targets, &PoolKey::new("", 0, &changed, 0));
        assert_eq!(pool.idle(), 1);
        // A master no longer among the targets: nothing kept for it.
        pool.keep_only(&[], &PoolKey::new("", 0, &changed, 0));
        assert_eq!(pool.idle(), 0);

        // No more than POOL_IDLE_MAX kept, however many were open at once.
        let key = PoolKey::new("127.0.0.1", peer.port, &open, 0);
        for _ in 0..POOL_IDLE_MAX + 4 {
            let (conn, _, _) = dial("127.0.0.1", peer.port, &open, 0).unwrap();
            pool.put(&key, conn);
        }
        assert_eq!(pool.idle(), POOL_IDLE_MAX);

        // As many as the node has command threads, where that is more.
        pool.size_for(2);
        assert_eq!(pool.most(), POOL_IDLE_MAX);
        pool.size_for(30);
        assert_eq!(pool.most(), 30);
        for _ in 0..20 {
            let (conn, _, _) = dial("127.0.0.1", peer.port, &open, 0).unwrap();
            pool.put(&key, conn);
        }
        assert_eq!(pool.idle(), 30);
    }

    #[test]
    fn a_login_is_told_apart_by_user_password_and_tls() {
        let a = PoolKey::new("h", 1, &("u".into(), "p".into()), 0);
        assert!(a.same_login(&PoolKey::new("x", 2, &("u".into(), "p".into()), 3)));
        assert!(!a.same_login(&PoolKey::new("h", 1, &("u".into(), "q".into()), 0)));
        assert!(!a.same_login(&PoolKey::new("h", 1, &("v".into(), "p".into()), 0)));
        let mut tls = a.clone();
        tls.tls = Some(7);
        assert!(!a.same_login(&tls));
        assert!(a != PoolKey::new("h", 1, &("u".into(), "p".into()), 1));
    }

    #[test]
    fn replies_are_read_whole_and_rows_survive_the_trip() {
        let reply = parse_reply(b"*3\r\n:2\r\n$3\r\nabc\r\n*2\r\n$-1\r\n+OK\r\n").unwrap();
        let Reply::Array(items) = reply else { panic!() };
        assert!(matches!(items[0], Reply::Int(2)));
        assert_eq!(items[1].text().as_deref(), Some("abc"));

        let mut row = ahash::AHashMap::new();
        row.insert("__key".to_string(), "doc:1".to_string());
        row.insert("price".to_string(), "10".to_string());
        let (back, timed_out) =
            decode_rows(parse_reply(&encode_rows(&[row.clone()], Some(false))).unwrap());
        assert_eq!(back, vec![row.clone()]);
        assert!(!timed_out);

        let (empty, timed_out) = decode_rows(parse_reply(&encode_rows(&[], Some(true))).unwrap());
        assert!(empty.is_empty());
        assert!(timed_out);

        let page = crate::resp::write::resp_array(vec![crate::resp::write::resp_integer(2)]);
        let (inner, timed_out) =
            decode_search(parse_reply(&encode_search(page, true)).unwrap()).unwrap();
        assert!(timed_out);
        let Reply::Array(items) = inner else { panic!() };
        assert!(matches!(items[0], Reply::Int(2)));
    }

    /// A master from before the timeout flag answers with the page or the
    /// rows alone: read as not timed out, in every shape they take.
    #[test]
    fn a_reply_without_the_flag_is_one_that_did_not_time_out() {
        use crate::resp::write::{resp_array, resp_bulk_str, resp_integer};
        // Pages: none found; a key and its score; one with its fields too.
        for page in [
            resp_array(vec![resp_integer(0)]),
            resp_array(vec![
                resp_integer(1),
                resp_bulk_str("doc:1"),
                resp_bulk_str("1"),
            ]),
            resp_array(vec![
                resp_integer(7),
                resp_bulk_str("doc:1"),
                resp_bulk_str("0.5"),
                resp_array(vec![resp_bulk_str("t"), resp_bulk_str("hello")]),
            ]),
        ] {
            let alone = parse_reply(&page).unwrap();
            let (inner, timed_out) = decode_search(alone.clone()).unwrap();
            assert!(!timed_out);
            assert_eq!(format!("{inner:?}"), format!("{alone:?}"));
            // The same page with the flag is told apart from it.
            let (flagged, timed_out) =
                decode_search(parse_reply(&encode_search(page, true)).unwrap()).unwrap();
            assert!(timed_out);
            assert_eq!(format!("{flagged:?}"), format!("{alone:?}"));
        }
        assert!(decode_search(Reply::Simple("OK".into())).is_err());
        assert!(decode_search(Reply::Array(vec![Reply::Bulk(Some(b"x".to_vec()))])).is_err());

        // Rows: none, one, two.
        let mut row = ahash::AHashMap::new();
        row.insert("__key".to_string(), "doc:1".to_string());
        for rows in [vec![], vec![row.clone()], vec![row.clone(), row.clone()]] {
            let (back, timed_out) = decode_rows(parse_reply(&encode_rows(&rows, None)).unwrap());
            assert_eq!(back, rows);
            assert!(!timed_out);
            let (back, timed_out) =
                decode_rows(parse_reply(&encode_rows(&rows, Some(true))).unwrap());
            assert_eq!(back, rows);
            assert!(timed_out);
        }
    }

    #[test]
    fn only_a_coordinator_that_asks_is_told_whether_a_part_timed_out() {
        let asks = |cmd: &[Vec<u8>]| -> bool {
            let parts: Vec<&[u8]> = cmd.iter().map(Vec::as_slice).collect();
            wants_timeout_flag(&parts)
        };
        let mut cmd: Vec<Vec<u8>> = vec![b"FT.SEARCH".to_vec(), b"idx".to_vec(), b"*".to_vec()];
        assert!(!asks(&cmd));
        cmd.extend([
            b"PARAMS".to_vec(),
            b"2".to_vec(),
            b"a".to_vec(),
            b"1".to_vec(),
        ]);
        assert!(!asks(&cmd));
        ask_for_timeout_flag(&mut cmd);
        assert!(asks(&cmd));
    }

    #[test]
    fn only_a_coordinator_that_asks_says_whether_scores_are_needed() {
        let hint = |cmd: &[Vec<u8>]| -> Option<bool> {
            let parts: Vec<&[u8]> = cmd.iter().map(Vec::as_slice).collect();
            scores_matter(&parts)
        };
        let mut cmd: Vec<Vec<u8>> = vec![b"FT.SEARCH".to_vec(), b"idx".to_vec(), b"*".to_vec()];
        assert_eq!(hint(&cmd), None);
        ask_for_scores_matter(&mut cmd, false);
        assert_eq!(hint(&cmd), Some(false));
        ask_for_timeout_flag(&mut cmd);
        assert_eq!(hint(&cmd), Some(false));
        assert_eq!(
            hint(&[
                b"FT.SEARCH".to_vec(),
                b"idx".to_vec(),
                b"*".to_vec(),
                b"PARAMS".to_vec(),
                b"2".to_vec(),
                SCORES_MATTER_PARAM.to_vec(),
                b"1".to_vec(),
            ]),
            Some(true)
        );
    }
}
