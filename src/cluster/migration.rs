// src/cluster/migration.rs
//
// Atomic slot migration — `CLUSTER MIGRATION` (Redis 8.4).
//
// `CLUSTER MIGRATION IMPORT <start> <end> ...` on the destination master moves
// whole slot ranges to it from the node that owns them, without the
// key-by-key MIGRATING / IMPORTING dance and without clients ever seeing an
// `-ASK` or `-TRYAGAIN`: until the last instant every command for those slots
// is served by the source, and from that instant by the destination.
//
// How it works, with S the source and D the destination:
//
//   1. D asks S to start (`CLUSTER SYNCSLOTS START`).
//   2. S closes the write gate for an instant. In that instant it attaches a
//      tap to its write stream and captures every key in the slots — so the
//      capture and the stream meet exactly, as they do for a full resync or a
//      backup.
//   3. S sends the captured keys to D, then the writes that have touched
//      those slots since, as they happen. D applies them but does not own the
//      slots yet, so clients are still sent to S.
//   4. When the stream has caught up, S pauses client writes, sends the last
//      of the stream, and asks D to take over. D claims the slots with a new
//      epoch; S records D as their owner, resumes writes — which for those
//      slots now redirect to D — and deletes its copy of the keys.
//
// If anything fails before step 4 completes, S simply carries on owning the
// slots, D throws away what it had received, and D tries again.
//
// S pushes to D over an ordinary client connection, authenticating with the
// same credentials replication uses (`masteruser` / `masterauth`).

use super::{ClusterState, NodeId, key_hash_slot};
use crate::replication::{HubTx, ReplPayload, ReplState};
use parking_lot::Mutex;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Inclusive slot ranges.
pub type Ranges = Vec<(u16, u16)>;

// `cluster-slot-migration-handoff-max-lag-bytes` and
// `cluster-slot-migration-write-pause-timeout` as `CONFIG SET` last made
// them; `UNSET` until then, and the values the server started with apply.
// Read at the start of each handoff, so a change applies to the next one.
const UNSET: u64 = u64::MAX;
static HANDOFF_MAX_LAG_BYTES: AtomicU64 = AtomicU64::new(UNSET);
static WRITE_PAUSE_TIMEOUT_MS: AtomicU64 = AtomicU64::new(UNSET);

/// How far behind the stream may be when the handoff starts.
pub fn handoff_max_lag_bytes(config: &crate::config::Config) -> u64 {
    match HANDOFF_MAX_LAG_BYTES.load(Ordering::Relaxed) {
        UNSET => config.cluster_slot_migration_handoff_max_lag_bytes,
        v => v,
    }
}

/// How long the handoff may hold client writes, in milliseconds.
pub fn write_pause_timeout_ms(config: &crate::config::Config) -> u64 {
    match WRITE_PAUSE_TIMEOUT_MS.load(Ordering::Relaxed) {
        UNSET => config.cluster_slot_migration_write_pause_timeout,
        v => v,
    }
}

/// `CONFIG SET cluster-slot-migration-handoff-max-lag-bytes`.
pub fn set_handoff_max_lag_bytes(bytes: u64) {
    HANDOFF_MAX_LAG_BYTES.store(bytes.min(UNSET - 1), Ordering::Relaxed);
}

/// `CONFIG SET cluster-slot-migration-write-pause-timeout`.
pub fn set_write_pause_timeout_ms(ms: u64) {
    WRITE_PAUSE_TIMEOUT_MS.store(ms.min(UNSET - 1), Ordering::Relaxed);
}

/// Finished tasks kept for `CLUSTER MIGRATION STATUS`.
const FINISHED_TASKS_KEPT: usize = 64;
/// Commands per round trip while sending.
const BATCH_COMMANDS: usize = 256;
const BATCH_BYTES: usize = 4 * 1024 * 1024;
/// How long the destination waits without hearing from the source before it
/// gives the attempt up.
const SOURCE_SILENCE_LIMIT: Duration = Duration::from_secs(30);
const RETRY_DELAY: Duration = Duration::from_secs(1);
const IO_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Operation {
    /// This node is the destination.
    Import,
    /// This node is the source.
    Migrate,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TaskState {
    InProgress,
    Completed,
    Failed,
    Canceled,
}

impl TaskState {
    pub fn name(self) -> &'static str {
        match self {
            TaskState::InProgress => "in_progress",
            TaskState::Completed => "completed",
            TaskState::Failed => "failed",
            TaskState::Canceled => "canceled",
        }
    }
}

#[derive(Clone)]
pub struct Task {
    pub id: String,
    pub ranges: Ranges,
    pub source: NodeId,
    pub dest: NodeId,
    pub operation: Operation,
    pub state: TaskState,
    pub last_error: String,
    pub retries: u64,
    pub create_time_ms: u64,
    pub start_time_ms: u64,
    pub end_time_ms: u64,
    /// How long client writes were held on this node for the migration, all
    /// told: at its start while the slots were captured, and at the handoff.
    pub write_pause_ms: u64,
    /// Of which, at the start.
    pub capture_pause_ms: u64,
    /// Asks the task's thread to stop.
    pub cancel: Arc<AtomicBool>,
    /// Import only: when the source last sent anything (Unix ms).
    pub last_activity_ms: Arc<AtomicU64>,
    /// Import only: the source reported that this attempt failed.
    pub attempt_failed: Arc<AtomicBool>,
}

/// This node's migration tasks, running and recently finished.
#[derive(Default)]
pub struct Registry {
    tasks: Mutex<Vec<Task>>,
}

impl Registry {
    /// The task with this id. An id is used again when a destination retries
    /// an import; the latest attempt is the one that counts.
    pub fn get(&self, id: &str) -> Option<Task> {
        self.tasks.lock().iter().rev().find(|t| t.id == id).cloned()
    }

    pub fn all(&self) -> Vec<Task> {
        self.tasks.lock().clone()
    }

    fn update(&self, id: &str, f: impl FnOnce(&mut Task)) {
        if let Some(task) = self.tasks.lock().iter_mut().rev().find(|t| t.id == id) {
            f(task);
        }
    }

    /// Add `task` unless a running task already covers one of its slots.
    fn add(&self, task: Task) -> Result<(), String> {
        let mut tasks = self.tasks.lock();
        let clash = tasks
            .iter()
            .any(|t| t.state == TaskState::InProgress && ranges_overlap(&t.ranges, &task.ranges));
        if clash {
            return Err("a migration of some of these slots is already in progress".into());
        }
        // A retry of a task that failed here replaces its record. Both used to
        // be kept, and the first was the one found: the retry ran as the old
        // task and its end was recorded on that, so the new record stayed
        // `in_progress` for ever (and `redis-cli --cluster reshard` waited on
        // it until it timed out).
        tasks.retain(|t| t.id != task.id);
        // Forget the oldest finished tasks beyond the number worth keeping.
        let finished = tasks
            .iter()
            .filter(|t| t.state != TaskState::InProgress)
            .count();
        if finished >= FINISHED_TASKS_KEPT
            && let Some(oldest) = tasks.iter().position(|t| t.state != TaskState::InProgress)
        {
            tasks.remove(oldest);
        }
        tasks.push(task);
        Ok(())
    }

    fn finish(&self, id: &str, state: TaskState, error: Option<String>) {
        self.update(id, |task| {
            if task.state == TaskState::InProgress {
                task.state = state;
                task.end_time_ms = now_ms();
                if let Some(error) = error {
                    task.last_error = error;
                }
            }
        });
    }

    /// Ask running tasks to stop: the one with this id, or all of them.
    /// Returns how many were asked.
    pub fn cancel(&self, id: Option<&str>) -> usize {
        let tasks = self.tasks.lock();
        let mut n = 0;
        for task in tasks.iter() {
            if task.state == TaskState::InProgress && id.is_none_or(|id| task.id == id) {
                task.cancel.store(true, Ordering::Relaxed);
                n += 1;
            }
        }
        n
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Parse `start end [start end ...]`.
pub fn parse_ranges(args: &[&[u8]]) -> Result<Ranges, String> {
    if args.is_empty() || !args.len().is_multiple_of(2) {
        return Err("slot ranges are given as pairs: start-slot end-slot".into());
    }
    let slot = |arg: &[u8]| -> Result<u16, String> {
        std::str::from_utf8(arg)
            .ok()
            .and_then(|s| s.parse::<u16>().ok())
            .filter(|s| (*s as usize) < super::CLUSTER_SLOTS)
            .ok_or_else(|| "Invalid or out of range slot".to_string())
    };
    let mut ranges: Ranges = Vec::with_capacity(args.len() / 2);
    for pair in args.chunks(2) {
        let (start, end) = (slot(pair[0])?, slot(pair[1])?);
        if start > end {
            return Err(format!("start slot {start} is greater than end slot {end}"));
        }
        if ranges_overlap(&ranges, &[(start, end)]) {
            return Err("slot ranges overlap".into());
        }
        ranges.push((start, end));
    }
    Ok(ranges)
}

pub fn in_ranges(ranges: &[(u16, u16)], slot: u16) -> bool {
    ranges
        .iter()
        .any(|(start, end)| (*start..=*end).contains(&slot))
}

fn ranges_overlap(a: &[(u16, u16)], b: &[(u16, u16)]) -> bool {
    a.iter()
        .any(|(a0, a1)| b.iter().any(|(b0, b1)| a0 <= b1 && b0 <= a1))
}

fn slots_of(ranges: &[(u16, u16)]) -> impl Iterator<Item = u16> + '_ {
    ranges.iter().flat_map(|(start, end)| *start..=*end)
}

/// `0-1000 2000-3000`
pub fn ranges_text(ranges: &[(u16, u16)]) -> String {
    ranges
        .iter()
        .map(|(start, end)| format!("{start}-{end}"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// What a migration thread needs from the server.
#[derive(Clone)]
pub struct Env {
    pub store: crate::store::Store,
    pub expiry_store: crate::store::ExpiryStore,
    pub index_store: crate::vector::IndexStore,
    pub repl_state: Arc<ReplState>,
    pub hub_tx: HubTx,
    pub cluster: Arc<ClusterState>,
    pub config: Arc<crate::config::Config>,
}

impl Env {
    pub fn from_ctx(ctx: &crate::commands::ctx::Ctx<'_>) -> Self {
        Self {
            store: Arc::clone(ctx.store),
            expiry_store: Arc::clone(ctx.expiry_store),
            index_store: Arc::clone(ctx.index_store),
            repl_state: Arc::clone(ctx.repl_state),
            hub_tx: ctx.hub_tx.clone(),
            cluster: Arc::clone(ctx.cluster),
            config: Arc::clone(ctx.config),
        }
    }

    fn registry(&self) -> &Registry {
        &self.cluster.slot_migrations
    }

    fn address_of(&self, node: &NodeId) -> Option<(String, u16)> {
        self.cluster
            .nodes
            .read()
            .get(node)
            .filter(|n| !n.ip.is_empty() && n.client_port != 0)
            .map(|n| (n.ip.clone(), n.client_port))
    }
}

/// Give the destination the definitions of this node's indexes before its
/// keys, so that they are indexed as they arrive: a master added to the
/// cluster after an index was created has none. Sent as the commands
/// themselves, so that the destination's replicas and log have them; under
/// the name of forwarded definitions, so that the destination does not send
/// them on (see `cluster::ddl`). One it has already is left as it is.
fn give_definitions(env: &Env, peer: &mut Peer) {
    let definitions: Vec<Vec<Vec<u8>>> = env
        .index_store
        .read()
        .values()
        .filter(|def| def.db_index == 0 && !crate::vector::is_alias(&def.name, def))
        .map(|def| def.create_args())
        .collect();
    if definitions.is_empty() {
        return;
    }
    let _ = peer.call(&command(&[
        b"CLIENT",
        b"SETNAME",
        crate::cluster::ddl::FORWARDED.as_bytes(),
    ]));
    for create in definitions {
        let parts: Vec<&[u8]> = create.iter().map(Vec::as_slice).collect();
        match peer.call(&command(&parts)) {
            Err(e) if !e.contains("already exists") => {
                log_warn!("Cluster: could not give the destination an index definition: {e}")
            }
            _ => {}
        }
    }
}

// ── A connection to the other node ───────────────────────────────────────────

struct Peer {
    /// Read and written through: a TLS stream with `tls-cluster`.
    reader: BufReader<Box<dyn crate::node_tls::ReadWrite>>,
    /// The socket under it, for its timeouts.
    sock: TcpStream,
}

fn command(parts: &[&[u8]]) -> Vec<u8> {
    let owned: Vec<Vec<u8>> = parts.iter().map(|p| p.to_vec()).collect();
    crate::resp::write::encode_resp_command(&owned)
}

impl Peer {
    fn connect(env: &Env, node: &NodeId) -> Result<Self, String> {
        let (host, port) = env
            .address_of(node)
            .ok_or_else(|| "the other node's address is not known".to_string())?;
        let addr = (host.as_str(), port)
            .to_socket_addrs()
            .ok()
            .and_then(|mut it| it.next())
            .ok_or_else(|| format!("could not resolve {host}:{port}"))?;
        let (stream, sock) = crate::node_tls::connect_node(&host, &addr, Duration::from_secs(5))
            .map_err(|e| format!("could not connect to {host}:{port}: {e}"))?;
        let _ = sock.set_read_timeout(Some(IO_TIMEOUT));
        let _ = sock.set_write_timeout(Some(IO_TIMEOUT));
        let mut peer = Peer {
            reader: BufReader::new(stream),
            sock,
        };

        let (user, password) = env.repl_state.master_credentials();
        if !password.is_empty() {
            let auth = if user.is_empty() {
                // As the default user, by name: `AUTH <password>` is refused by a
                // node that has no password (#135), and with the name it logs in as
                // before.
                command(&[b"AUTH", b"default", password.as_bytes()])
            } else {
                command(&[b"AUTH", user.as_bytes(), password.as_bytes()])
            };
            peer.call(&auth)
                .map_err(|e| format!("authentication with {host}:{port} failed: {e}"))?;
        }
        Ok(peer)
    }

    /// Send already-encoded commands and read one status reply for each.
    fn send(&mut self, encoded: &[u8], replies: usize) -> Result<(), String> {
        let stream = self.reader.get_mut();
        stream
            .write_all(encoded)
            .and_then(|()| stream.flush())
            .map_err(|e| format!("write to the other node failed: {e}"))?;
        for _ in 0..replies {
            self.read_status()?;
        }
        Ok(())
    }

    fn call(&mut self, encoded: &[u8]) -> Result<(), String> {
        self.send(encoded, 1)
    }

    /// Every reply in this protocol is a status or an error.
    fn read_status(&mut self) -> Result<(), String> {
        let mut line = String::new();
        match self.reader.read_line(&mut line) {
            Ok(0) => Err("the other node closed the connection".into()),
            Ok(_) => {
                let line = line.trim_end();
                match line.strip_prefix('+') {
                    Some(_) => Ok(()),
                    None => Err(line.trim_start_matches('-').to_string()),
                }
            }
            Err(e) => Err(format!("read from the other node failed: {e}")),
        }
    }
}

// ── Key helpers ──────────────────────────────────────────────────────────────

/// Delete every key in `ranges` from this node, telling replicas and the AOF.
/// Used by the source once the slots have moved, and by the destination to
/// discard a failed import.
fn delete_keys_in(env: &Env, ranges: &[(u16, u16)]) -> usize {
    delete_keys(
        &env.store,
        &env.expiry_store,
        &env.index_store,
        &env.repl_state,
        &env.hub_tx,
        |slot| in_ranges(ranges, slot),
    )
}

/// Delete every key whose slot `in_slots` picks, telling replicas and the AOF.
fn delete_keys(
    store: &crate::store::Store,
    expiry_store: &crate::store::ExpiryStore,
    index_store: &crate::vector::IndexStore,
    repl_state: &Arc<ReplState>,
    hub_tx: &HubTx,
    in_slots: impl Fn(u16) -> bool,
) -> usize {
    let mut removed = 0usize;
    let Some(db) = store.first() else {
        return 0;
    };
    for shard in db.iter() {
        let keys: Vec<String> = crate::read_lock!(shard)
            .keys()
            .filter(|k| in_slots(key_hash_slot(k.as_bytes())))
            .map(|k| k.to_string())
            .collect();
        for chunk in keys.chunks(128) {
            // One whole write, as far as a full sync or a backup is concerned.
            let _gate = crate::write_gate::enter();
            let _order = crate::write_order::lock_keys(0, chunk.iter().map(|k| k.as_bytes()));
            let mut del = vec![b"DEL".to_vec()];
            for key in chunk {
                if crate::commands::helpers::delete_key(store, expiry_store, 0, key) {
                    crate::vector::maybe_unindex(index_store, key);
                    del.push(key.as_bytes().to_vec());
                    removed += 1;
                }
            }
            if del.len() > 1 {
                crate::replication::replicate_from_background(repl_state, hub_tx, 0, &del);
            }
        }
    }
    removed
}

/// Delete the keys of slots another master has taken from this one.
///
/// A slot can leave without its keys: a `CLUSTER SETSLOT … NODE` given to the
/// other node only, or two masters that both served the slot across a
/// partition, of which this one lost. The keys can no longer be reached here —
/// every command for them is answered with -MOVED — but they would still take
/// memory, show up in `DBSIZE` and `KEYS`, be saved and replicated, and come
/// back as stale data if the slot ever returned. Redis deletes them too.
///
/// Runs on its own thread: the caller is handling a bus message.
pub(crate) fn drop_lost_slots(cluster: &ClusterState, slots: Vec<u16>) {
    let Some(hooks) = cluster.hooks.get() else {
        return;
    };
    let starter = &hooks.replica_starter;
    let store = Arc::clone(&starter.store);
    let expiry_store = Arc::clone(&starter.expiry_store);
    let index_store = Arc::clone(&starter.index_store);
    let repl_state = Arc::clone(&starter.repl_state);
    let hub_tx = starter.hub_tx.clone();
    let spawned = std::thread::Builder::new()
        .name("kividb-lost-slots".into())
        .spawn(move || {
            let mut lost = vec![false; crate::cluster::CLUSTER_SLOTS];
            for slot in &slots {
                lost[*slot as usize] = true;
            }
            let removed = delete_keys(
                &store,
                &expiry_store,
                &index_store,
                &repl_state,
                &hub_tx,
                |slot| lost[slot as usize],
            );
            if removed > 0 {
                log_info!(
                    "Cluster: deleted {removed} key(s) in {} slot(s) owned by another node",
                    slots.len()
                );
            }
        });
    if let Err(e) = spawned {
        log_error!(
            "Cluster: could not start the lost-slot cleanup thread: {e} — keys in slots now owned by another node stay here until the next restart"
        );
    }
}

/// A key captured at the cut, with a copy of its value of its own: a list,
/// hash or set can be changed in place once writes go on, and the copy is
/// as it was at the cut. Copying shares the elements (and a string's bytes,
/// which never change in place), so it is far quicker than laying the value
/// out as its `RESTORE`, which is done afterwards.
struct Captured {
    key: String,
    value: crate::types::KiviDBValue,
    expires_at: u64,
}

impl Captured {
    fn into_restore(self) -> std::io::Result<Vec<Vec<u8>>> {
        restore_command(self.key.as_bytes(), &self.value, self.expires_at)
    }
}

fn restore_command(
    key: &[u8],
    value: &crate::types::KiviDBValue,
    expires_at: u64,
) -> std::io::Result<Vec<Vec<u8>>> {
    let mut restore = vec![
        b"RESTORE".to_vec(),
        key.to_vec(),
        expires_at.to_string().into_bytes(),
        crate::snapshot::dump_payload(value)?,
        b"REPLACE".to_vec(),
    ];
    if expires_at != 0 {
        restore.push(b"ABSTTL".to_vec());
    }
    Ok(restore)
}

/// Every key in `ranges`, as what recreates it. Call with the write gate
/// closed: it does no more than must be done while writes wait. The values
/// are serialised afterwards (see [`Captured::into_restore`]); they used to
/// be serialised here, every one — a list of a million elements among them —
/// with every write on the node waiting.
fn capture(env: &Env, ranges: &[(u16, u16)]) -> std::io::Result<Vec<Captured>> {
    let now = now_ms();
    let mut out = Vec::new();
    let Some(db) = env.store.first() else {
        return Ok(out);
    };
    for shard in db.iter() {
        let map = crate::read_lock!(shard);
        for (key, value) in map.iter() {
            if !in_ranges(ranges, key_hash_slot(key.as_bytes())) {
                continue;
            }
            let expires_at = {
                let exp = crate::commands::helpers::get_expiry_shard(&env.expiry_store, 0, key);
                crate::read_lock!(exp)
                    .get(key.as_str())
                    .and_then(|at| at.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0)
            };
            if expires_at != 0 && expires_at <= now {
                continue;
            }
            out.push(Captured {
                key: key.to_string(),
                value: value.deep_clone(),
                expires_at,
            });
        }
    }
    Ok(out)
}

/// Whether a command from the write stream belongs to the migrating slots.
/// `Err` for one that makes the migration impossible to continue.
fn concerns(parts: &[Vec<u8>], ranges: &[(u16, u16)]) -> Result<bool, String> {
    let Some(name) = parts.first() else {
        return Ok(false);
    };
    let name = String::from_utf8_lossy(name).to_ascii_uppercase();
    if matches!(name.as_str(), "FLUSHALL" | "FLUSHDB") {
        return Err("the dataset was flushed during the migration".into());
    }
    let refs: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
    let keys = crate::keyspec::key_positions(&name, &refs);
    // A cluster command's keys share one slot, so the first decides.
    Ok(keys
        .first()
        .is_some_and(|pos| in_ranges(ranges, key_hash_slot(refs[pos]))))
}

/// Wrap a command for the destination: `CLUSTER SYNCSLOTS APPLY <task> ...`.
fn wrap_apply(task_id: &str, parts: &[Vec<u8>]) -> Vec<u8> {
    let mut wrapped: Vec<Vec<u8>> = Vec::with_capacity(parts.len() + 4);
    wrapped.push(b"CLUSTER".to_vec());
    wrapped.push(b"SYNCSLOTS".to_vec());
    wrapped.push(b"APPLY".to_vec());
    wrapped.push(task_id.as_bytes().to_vec());
    wrapped.extend(parts.iter().cloned());
    crate::resp::write::encode_resp_command(&wrapped)
}

/// Send `commands` to the destination in pipelined batches.
fn forward(peer: &mut Peer, task_id: &str, commands: &[Vec<Vec<u8>>]) -> Result<(), String> {
    let mut batch = Vec::new();
    let mut count = 0usize;
    for parts in commands {
        batch.extend_from_slice(&wrap_apply(task_id, parts));
        count += 1;
        if count >= BATCH_COMMANDS || batch.len() >= BATCH_BYTES {
            peer.send(&batch, count)?;
            batch.clear();
            count = 0;
        }
    }
    if count > 0 {
        peer.send(&batch, count)?;
    }
    Ok(())
}

// ── Source ───────────────────────────────────────────────────────────────────

/// `CLUSTER SYNCSLOTS START`, on the source: begin moving `ranges` to `dest`.
pub fn start_source(env: Env, task_id: &str, dest: NodeId, ranges: Ranges) -> Result<(), String> {
    let cluster = &env.cluster;
    let me = cluster.myself();
    if dest == me {
        return Err("cannot migrate slots to myself".into());
    }
    if cluster.my_master_id().is_some() {
        return Err("I am a replica".into());
    }
    if let Some(slot) = slots_of(&ranges).find(|s| !cluster.owns_slot(*s)) {
        return Err(format!("I am not the owner of slot {slot}"));
    }
    if env.address_of(&dest).is_none() {
        return Err("unknown destination node".into());
    }
    env.registry().add(Task {
        id: task_id.to_string(),
        ranges: ranges.clone(),
        source: me,
        dest,
        operation: Operation::Migrate,
        state: TaskState::InProgress,
        last_error: String::new(),
        retries: 0,
        create_time_ms: now_ms(),
        start_time_ms: now_ms(),
        end_time_ms: 0,
        write_pause_ms: 0,
        capture_pause_ms: 0,
        cancel: Arc::new(AtomicBool::new(false)),
        last_activity_ms: Arc::new(AtomicU64::new(0)),
        attempt_failed: Arc::new(AtomicBool::new(false)),
    })?;

    let id = task_id.to_string();
    let thread_env = env.clone();
    let spawned = std::thread::Builder::new()
        .name("kividb-slot-migrate".into())
        .spawn(move || run_source(thread_env, id));
    if let Err(e) = spawned {
        env.registry()
            .finish(task_id, TaskState::Failed, Some(e.to_string()));
        return Err(format!("could not start the migration thread: {e}"));
    }
    Ok(())
}

fn run_source(env: Env, task_id: String) {
    let Some(task) = env.registry().get(&task_id) else {
        return;
    };
    match migrate_out(&env, &task) {
        Ok(pause_ms) => {
            env.registry()
                .update(&task_id, |t| t.write_pause_ms = pause_ms);
            env.registry().finish(&task_id, TaskState::Completed, None);
            log_info!(
                "Cluster: Slots {} migrated to {} (writes paused {pause_ms} ms)",
                ranges_text(&task.ranges),
                super::node_id_str(&task.dest)
            );
        }
        Err(error) => {
            let canceled = task.cancel.load(Ordering::Relaxed);
            // Tell the destination, so it does not wait out the silence.
            if let Ok(mut peer) = Peer::connect(&env, &task.dest) {
                let _ = peer.call(&command(&[
                    b"CLUSTER",
                    b"SYNCSLOTS",
                    b"FAIL",
                    task_id.as_bytes(),
                    error.as_bytes(),
                ]));
            }
            log_warn!(
                "Cluster: Migration of slots {} did not complete: {error}",
                ranges_text(&task.ranges)
            );
            let state = if canceled {
                TaskState::Canceled
            } else {
                TaskState::Failed
            };
            env.registry().finish(&task_id, state, Some(error));
        }
    }
}

/// Detaches the tap when the migration ends, however it ends.
struct TapGuard<'a> {
    repl_state: &'a ReplState,
    id: u64,
    attached: bool,
}

impl TapGuard<'_> {
    /// Detach between two whole writes.
    fn detach(&mut self) {
        if self.attached {
            let _cut = crate::write_gate::cut();
            self.repl_state.detach_tap(self.id);
            self.attached = false;
        }
    }
}

impl Drop for TapGuard<'_> {
    fn drop(&mut self) {
        self.detach();
    }
}

/// Lifts the write pause when the handoff ends, however it ends.
struct PauseGuard(bool);

impl Drop for PauseGuard {
    fn drop(&mut self) {
        if self.0 {
            crate::pause::clear_pause();
        }
    }
}

static NEXT_TAP: AtomicU64 = AtomicU64::new(1 << 32);

/// The source's side of one migration. Returns how long writes were paused.
fn migrate_out(env: &Env, task: &Task) -> Result<u64, String> {
    let canceled = || -> Result<(), String> {
        if task.cancel.load(Ordering::Relaxed) {
            Err("canceled".into())
        } else {
            Ok(())
        }
    };
    let mut peer = Peer::connect(env, &task.dest)?;
    give_definitions(env, &mut peer);

    // ── The cut: capture the slots and start listening, at one instant ───
    let (tx, rx) = std::sync::mpsc::channel::<ReplPayload>();
    let tap_id = NEXT_TAP.fetch_add(1, Ordering::Relaxed);
    let held_from = Instant::now();
    let (captured, mut tap) = {
        let _cut = crate::write_gate::cut();
        env.repl_state.attach_tap(tap_id, tx);
        let tap = TapGuard {
            repl_state: &env.repl_state,
            id: tap_id,
            attached: true,
        };
        (capture(env, &task.ranges), tap)
    };
    // Counted whatever becomes of the migration: clients waited for it.
    let capture_ms = held_from.elapsed().as_millis() as u64;
    env.registry().update(&task.id, |t| {
        t.capture_pause_ms = capture_ms;
        t.write_pause_ms = capture_ms;
    });
    let captured = captured
        .and_then(|keys| keys.into_iter().map(Captured::into_restore).collect())
        .map_err(|e| format!("could not serialise the slots' keys: {e}"))?;
    let captured: Vec<Vec<Vec<u8>>> = captured;

    forward(&mut peer, &task.id, &captured)?;
    drop(captured);
    canceled()?;

    // ── Follow the stream until it is nearly caught up ───────────────────
    let max_lag = handoff_max_lag_bytes(&env.config) as usize;
    // Decode whatever the tap holds right now into the commands that matter.
    let drain = |commands: &mut Vec<Vec<Vec<u8>>>| -> Result<usize, String> {
        let mut bytes = 0usize;
        while let Ok(payload) = rx.try_recv() {
            bytes += payload.len();
            if let Some((parts, _)) = crate::aof::parse_one_command(&payload)
                && concerns(&parts, &task.ranges)?
            {
                commands.push(parts);
            }
            if bytes >= BATCH_BYTES {
                break;
            }
        }
        Ok(bytes)
    };
    loop {
        canceled()?;
        let mut commands = Vec::new();
        let bytes = drain(&mut commands)?;
        forward(&mut peer, &task.id, &commands)?;
        if bytes <= max_lag {
            break;
        }
    }

    // ── Handoff ──────────────────────────────────────────────────────────
    // Pause client writes, let the ones in flight finish, send what they
    // wrote, and hand the slots over.
    let pause_timeout = write_pause_timeout_ms(&env.config).max(100);
    let paused_at = Instant::now();
    crate::pause::set_handoff_pause(pause_timeout as i64);
    let pause = PauseGuard(true);
    // Counted as it goes, so that a handoff that fails part way is counted
    // too: writes were held for it all the same.
    struct Counted<'a> {
        env: &'a Env,
        task: &'a str,
        from: Instant,
        before: u64,
    }
    impl Drop for Counted<'_> {
        fn drop(&mut self) {
            let total = self.before + self.from.elapsed().as_millis() as u64;
            self.env
                .registry()
                .update(self.task, |t| t.write_pause_ms = total);
        }
    }
    let counted = Counted {
        env,
        task: &task.id,
        from: paused_at,
        before: capture_ms,
    };
    tap.detach();
    let gate_ms = paused_at.elapsed().as_millis();
    loop {
        let mut commands = Vec::new();
        let bytes = drain(&mut commands)?;
        forward(&mut peer, &task.id, &commands)?;
        if bytes == 0 {
            break;
        }
    }
    let sent_ms = paused_at.elapsed().as_millis();
    canceled()?;

    let deadline = paused_at + Duration::from_millis(pause_timeout);
    let _ = peer.sock.set_read_timeout(Some(
        deadline
            .saturating_duration_since(Instant::now())
            .max(Duration::from_millis(100)),
    ));
    let takeover = peer.call(&command(&[
        b"CLUSTER",
        b"SYNCSLOTS",
        b"TAKEOVER",
        task.id.as_bytes(),
    ]));
    if let Err(error) = takeover {
        // No answer is not a refusal: the destination may have taken the
        // slots and the reply been lost. Keep writes paused until its claim
        // arrives over the bus or the pause runs out — resuming them now
        // could accept writes to slots that have already moved.
        let first = task.ranges[0].0;
        let moved = || env.cluster.slot_owner(first) == Some(task.dest);
        while !moved() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        if !moved() {
            return Err(format!(
                "the destination did not take the slots over: {error}"
            ));
        }
    }

    let taken_ms = paused_at.elapsed().as_millis();

    // The destination owns the slots. Say so here, then let writes resume —
    // for these slots they now redirect — and drop our copy of the keys.
    // nodes.conf is saved after writes resume: the save is synced, and with
    // the disk busy that took seconds, all of them with writes paused. Lost
    // to a crash in between, the record is put right by the destination's
    // claim, which has the newer epoch.
    env.cluster
        .hand_over_slots(slots_of(&task.ranges), task.dest);
    let paused_ms = capture_ms + paused_at.elapsed().as_millis() as u64;
    drop(pause);
    drop(counted);
    let _ = env.cluster.save_to_disk();
    log_info!(
        "Cluster: handover of slots {}: writes paused {} ms (in-flight writes {gate_ms},          last of the stream {}, the destination's takeover {})",
        ranges_text(&task.ranges),
        paused_ms - capture_ms,
        sent_ms - gate_ms,
        taken_ms - sent_ms,
    );
    delete_keys_in(env, &task.ranges);
    Ok(paused_ms)
}

// ── Destination ──────────────────────────────────────────────────────────────

/// `CLUSTER MIGRATION IMPORT`: start importing `ranges`. Returns the task id.
pub fn start_import(env: Env, ranges: Ranges) -> Result<String, String> {
    let cluster = &env.cluster;
    if !cluster.enabled {
        return Err("This instance has cluster support disabled".into());
    }
    if cluster.my_master_id().is_some() {
        return Err("slots can only be imported by a master".into());
    }
    let source = single_owner(cluster, &ranges)?;
    let id = super::node_id_str(&super::generate_node_id()).to_string();
    env.registry().add(Task {
        id: id.clone(),
        ranges,
        source,
        dest: cluster.myself(),
        operation: Operation::Import,
        state: TaskState::InProgress,
        last_error: String::new(),
        retries: 0,
        create_time_ms: now_ms(),
        start_time_ms: 0,
        end_time_ms: 0,
        write_pause_ms: 0,
        capture_pause_ms: 0,
        cancel: Arc::new(AtomicBool::new(false)),
        last_activity_ms: Arc::new(AtomicU64::new(now_ms())),
        attempt_failed: Arc::new(AtomicBool::new(false)),
    })?;

    save_imports(cluster);
    let task_id = id.clone();
    let thread_env = env.clone();
    let spawned = std::thread::Builder::new()
        .name("kividb-slot-import".into())
        .spawn(move || run_import(thread_env, task_id));
    if let Err(e) = spawned {
        env.registry()
            .finish(&id, TaskState::Failed, Some(e.to_string()));
        save_imports(&env.cluster);
        return Err(format!("could not start the import thread: {e}"));
    }
    Ok(id)
}

/// The one node that owns every slot in `ranges`, which must not be us.
fn single_owner(cluster: &ClusterState, ranges: &[(u16, u16)]) -> Result<NodeId, String> {
    let me = cluster.myself();
    let mut owner: Option<NodeId> = None;
    for slot in slots_of(ranges) {
        match cluster.slot_owner(slot) {
            None => return Err(format!("slot {slot} is not assigned to any node")),
            Some(id) if id == me => return Err(format!("I already own slot {slot}")),
            Some(id) => match owner {
                None => owner = Some(id),
                Some(first) if first != id => {
                    return Err("the slots are owned by more than one node; import each \
                                owner's slots separately"
                        .into());
                }
                Some(_) => {}
            },
        }
    }
    owner.ok_or_else(|| "no slots given".to_string())
}

// ── Imports under way, on disk ───────────────────────────────────────────────
//
// The keys an import has received are a copy: the source keeps serving the
// slots, and deletes its own copy only once this node has taken them over.
// If this node goes down first, the task (kept in memory) is gone, and so is
// anything that would remove the partial copy: it would stay, unreachable
// (-MOVED), in DBSIZE, KEYS and snapshots, and come back to life if the slots
// were ever given to this node (#108). So the ranges of the imports under way
// are kept in a file next to the cluster config, and at startup the keys in
// those of them this node does not own are deleted.
//
// Only the atomic import's: keys moved here by MIGRATE (key by key) were
// deleted at the source as they left, and are the only copy. Those Redis
// keeps, and so does this.

/// `<cluster-config-file>.imports`.
fn imports_path(cluster: &ClusterState) -> Option<std::path::PathBuf> {
    cluster.config_path.as_ref().map(|p| {
        let mut name = p.as_os_str().to_owned();
        name.push(".imports");
        std::path::PathBuf::from(name)
    })
}

/// Write down the ranges of the imports under way (none: remove the file).
fn save_imports(cluster: &ClusterState) {
    let Some(path) = imports_path(cluster) else {
        return;
    };
    let lines: Vec<String> = cluster
        .slot_migrations
        .all()
        .iter()
        .filter(|t| t.operation == Operation::Import && t.state == TaskState::InProgress)
        .map(|t| format!("{} {}", t.id, ranges_text(&t.ranges)))
        .collect();
    let written = if lines.is_empty() {
        match std::fs::remove_file(&path) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        }
    } else {
        let mut tmp = path.as_os_str().to_owned();
        tmp.push(".tmp");
        let tmp = std::path::PathBuf::from(tmp);
        std::fs::write(&tmp, lines.join("\n") + "\n").and_then(|()| std::fs::rename(&tmp, &path))
    };
    if let Err(e) = written {
        log_warn!(
            "Cluster: Could not record the imports under way in {}: {e}",
            path.display()
        );
    }
}

/// At startup: delete the keys of the imports that were under way when this
/// node went down, in the slots it does not own (#108). Then forget them.
pub(crate) fn drop_unfinished_imports(cluster: &ClusterState) {
    let Some(path) = imports_path(cluster) else {
        return;
    };
    let Ok(text) = std::fs::read_to_string(&path) else {
        return;
    };
    let me = cluster.myself();
    let mut cut_short: Vec<u16> = Vec::new();
    for line in text.lines() {
        let mut words = line.split_whitespace();
        let _task = words.next();
        let ranges: Vec<&[u8]> = words
            .flat_map(|r| r.split('-'))
            .map(str::as_bytes)
            .collect();
        let Ok(ranges) = parse_ranges(&ranges) else {
            continue;
        };
        let slots = cluster.slots.read();
        cut_short.extend(slots_of(&ranges).filter(|&s| slots[s as usize] != Some(me)));
    }
    cut_short.sort_unstable();
    cut_short.dedup();
    if !cut_short.is_empty() {
        log_info!(
            "Cluster: {} slot(s) were being imported when this node stopped; \
             deleting what had arrived of them",
            cut_short.len()
        );
        drop_lost_slots(cluster, cut_short);
    }
    let _ = std::fs::remove_file(&path);
}

fn run_import(env: Env, task_id: String) {
    let registry = env.registry();
    loop {
        let Some(task) = registry.get(&task_id) else {
            return;
        };
        if task.state != TaskState::InProgress {
            return;
        }
        if task.cancel.load(Ordering::Relaxed) {
            delete_unowned(&env, &task.ranges);
            registry.finish(&task_id, TaskState::Canceled, None);
            save_imports(&env.cluster);
            return;
        }

        // The owner may have changed since the last attempt (a failover).
        let source = match single_owner(&env.cluster, &task.ranges) {
            Ok(source) => source,
            Err(error) => {
                // Including: the slots are ours already. Either way there is
                // nothing left to import.
                registry.finish(&task_id, TaskState::Failed, Some(error));
                save_imports(&env.cluster);
                return;
            }
        };
        task.attempt_failed.store(false, Ordering::Relaxed);
        task.last_activity_ms.store(now_ms(), Ordering::Relaxed);
        registry.update(&task_id, |t| {
            t.source = source;
            t.start_time_ms = now_ms();
        });

        let outcome = import_attempt(&env, &task, source);
        match outcome {
            // Taken over, canceled, or otherwise finished by a handler.
            None => return,
            Some(error) => {
                delete_unowned(&env, &task.ranges);
                registry.update(&task_id, |t| {
                    t.last_error = error;
                    t.retries += 1;
                });
            }
        }
        // Try again shortly, unless asked to stop meanwhile.
        let retry_at = Instant::now() + RETRY_DELAY;
        while Instant::now() < retry_at && !task.cancel.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

/// One attempt at an import. `None` when the task has finished (completed or
/// canceled); otherwise why the attempt failed.
fn import_attempt(env: &Env, task: &Task, source: NodeId) -> Option<String> {
    let registry = env.registry();
    let mut start: Vec<Vec<u8>> = vec![
        b"CLUSTER".to_vec(),
        b"SYNCSLOTS".to_vec(),
        b"START".to_vec(),
        task.id.as_bytes().to_vec(),
        super::node_id_str(&env.cluster.myself())
            .as_bytes()
            .to_vec(),
    ];
    for (a, b) in &task.ranges {
        start.push(a.to_string().into_bytes());
        start.push(b.to_string().into_bytes());
    }
    let started = Peer::connect(env, &source)
        .and_then(|mut peer| peer.call(&crate::resp::write::encode_resp_command(&start)));
    if let Err(error) = started {
        return Some(format!("the source refused to start: {error}"));
    }

    loop {
        std::thread::sleep(Duration::from_millis(20));
        match registry.get(&task.id).map(|t| t.state) {
            Some(TaskState::InProgress) => {}
            _ => return None,
        }
        if task.cancel.load(Ordering::Relaxed) {
            // Stop the source too; it would otherwise keep sending.
            if let Ok(mut peer) = Peer::connect(env, &source) {
                let _ = peer.call(&command(&[
                    b"CLUSTER",
                    b"SYNCSLOTS",
                    b"CANCEL",
                    task.id.as_bytes(),
                ]));
            }
            delete_unowned(env, &task.ranges);
            registry.finish(&task.id, TaskState::Canceled, None);
            save_imports(&env.cluster);
            return None;
        }
        if task.attempt_failed.load(Ordering::Relaxed) {
            return Some(
                registry
                    .get(&task.id)
                    .map(|t| t.last_error)
                    .unwrap_or_default(),
            );
        }
        let silent_for = now_ms().saturating_sub(task.last_activity_ms.load(Ordering::Relaxed));
        if silent_for > SOURCE_SILENCE_LIMIT.as_millis() as u64 {
            return Some("the source went silent".into());
        }
    }
}

/// Discard the keys of an import that did not complete — but never the keys
/// of slots this node has come to own.
fn delete_unowned(env: &Env, ranges: &[(u16, u16)]) {
    let unowned: Ranges = slots_of(ranges)
        .filter(|slot| !env.cluster.owns_slot(*slot))
        .map(|slot| (slot, slot))
        .collect();
    if !unowned.is_empty() {
        delete_keys_in(env, &unowned);
    }
}

/// `CLUSTER SYNCSLOTS APPLY`, on the destination: is this task expecting data?
pub fn accept_apply(cluster: &ClusterState, task_id: &str) -> Result<(), String> {
    match cluster.slot_migrations.get(task_id) {
        Some(task)
            if task.operation == Operation::Import
                && task.state == TaskState::InProgress
                && !task.cancel.load(Ordering::Relaxed) =>
        {
            task.last_activity_ms.store(now_ms(), Ordering::Relaxed);
            Ok(())
        }
        _ => Err("no such import in progress".into()),
    }
}

/// `CLUSTER SYNCSLOTS TAKEOVER`, on the destination: everything has arrived;
/// take ownership of the slots.
pub fn take_over(cluster: &ClusterState, task_id: &str) -> Result<(), String> {
    accept_apply(cluster, task_id)?;
    let Some(task) = cluster.slot_migrations.get(task_id) else {
        return Err("no such import in progress".into());
    };
    // The source keeps client writes paused until this answer arrives.
    let started = Instant::now();
    // Saved before the source is told: it deletes its copy when told, and a
    // restart of this node with the slots not in its config would delete
    // this one (#108). Not saved, not taken over: the slots go back to the
    // source, which keeps serving them.
    if let Err(e) =
        super::persist::with_deferred_sync(|| cluster.take_over_slots(slots_of(&task.ranges)))
    {
        cluster.hand_over_slots(slots_of(&task.ranges), task.source);
        log_warn!(
            "Cluster: Could not save the cluster config while taking over slots {} ({e}); \
             leaving them with {}",
            ranges_text(&task.ranges),
            super::node_id_str(&task.source)
        );
        return Err(format!("could not save the cluster config: {e}"));
    }
    cluster
        .slot_migrations
        .finish(task_id, TaskState::Completed, None);
    save_imports(cluster);
    log_info!(
        "Cluster: Slots {} imported from {} (taken over in {} ms)",
        ranges_text(&task.ranges),
        super::node_id_str(&task.source),
        started.elapsed().as_millis()
    );
    Ok(())
}

/// `CLUSTER SYNCSLOTS FAIL`, on the destination: the source gave up.
pub fn source_failed(cluster: &ClusterState, task_id: &str, error: &str) {
    if let Some(task) = cluster.slot_migrations.get(task_id) {
        cluster
            .slot_migrations
            .update(task_id, |t| t.last_error = error.to_string());
        task.attempt_failed.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ranges(args: &[&str]) -> Result<Ranges, String> {
        let args: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
        parse_ranges(&args)
    }

    #[test]
    fn slot_ranges_are_pairs_in_range_and_disjoint() {
        assert_eq!(
            ranges(&["0", "1000", "2000", "3000"]),
            Ok(vec![(0, 1000), (2000, 3000)])
        );
        assert_eq!(ranges(&["5", "5"]), Ok(vec![(5, 5)]));
        assert!(ranges(&[]).is_err());
        assert!(ranges(&["0"]).is_err());
        assert!(ranges(&["10", "5"]).is_err());
        assert!(ranges(&["0", "16384"]).is_err());
        assert!(ranges(&["0", "x"]).is_err());
        assert!(ranges(&["0", "10", "10", "20"]).is_err(), "overlap");

        let r = vec![(0, 10), (20, 30)];
        assert!(in_ranges(&r, 0) && in_ranges(&r, 10) && in_ranges(&r, 25));
        assert!(!in_ranges(&r, 11) && !in_ranges(&r, 31));
        assert_eq!(ranges_text(&r), "0-10 20-30");
        assert_eq!(
            slots_of(&[(3, 5), (9, 9)]).collect::<Vec<_>>(),
            [3, 4, 5, 9]
        );
    }

    #[test]
    fn only_writes_to_the_migrating_slots_are_forwarded() {
        let cmd = |parts: &[&str]| -> Vec<Vec<u8>> {
            parts.iter().map(|p| p.as_bytes().to_vec()).collect()
        };
        let slot = key_hash_slot(b"foo");
        let mine = vec![(slot, slot)];
        let other = vec![((slot + 1) % 16384, (slot + 1) % 16384)];

        assert_eq!(concerns(&cmd(&["SET", "foo", "v"]), &mine), Ok(true));
        assert_eq!(concerns(&cmd(&["SET", "foo", "v"]), &other), Ok(false));
        // The key is found wherever the command puts it.
        assert_eq!(
            concerns(&cmd(&["XGROUP", "CREATE", "foo", "g", "$"]), &mine),
            Ok(true)
        );
        assert_eq!(
            concerns(&cmd(&["DEL", "{foo}a", "{foo}b"]), &mine),
            Ok(true)
        );
        // Keyless commands are not slot data.
        assert_eq!(concerns(&cmd(&["SELECT", "0"]), &mine), Ok(false));
        assert_eq!(concerns(&cmd(&["PING"]), &mine), Ok(false));
        // A flush cannot be replayed onto another node's slots.
        assert!(concerns(&cmd(&["FLUSHALL"]), &mine).is_err());
    }

    fn task(id: &str, ranges: Ranges) -> Task {
        Task {
            id: id.into(),
            ranges,
            source: super::super::NODE_ID_ZERO,
            dest: super::super::NODE_ID_ZERO,
            operation: Operation::Import,
            state: TaskState::InProgress,
            last_error: String::new(),
            retries: 0,
            create_time_ms: 0,
            start_time_ms: 0,
            end_time_ms: 0,
            write_pause_ms: 0,
            capture_pause_ms: 0,
            cancel: Arc::new(AtomicBool::new(false)),
            last_activity_ms: Arc::new(AtomicU64::new(0)),
            attempt_failed: Arc::new(AtomicBool::new(false)),
        }
    }

    #[test]
    fn a_slot_is_in_one_running_task_at_a_time() {
        let registry = Registry::default();
        registry.add(task("a", vec![(0, 100)])).unwrap();
        assert!(registry.add(task("b", vec![(100, 200)])).is_err());
        registry.add(task("c", vec![(101, 200)])).unwrap();

        // Once a task has finished, its slots are free again.
        registry.finish("a", TaskState::Failed, Some("boom".into()));
        let a = registry.get("a").unwrap();
        assert_eq!(
            (a.state, a.last_error.as_str()),
            (TaskState::Failed, "boom")
        );
        assert!(a.end_time_ms > 0);
        registry.add(task("d", vec![(0, 100)])).unwrap();

        // Finishing is final.
        registry.finish("a", TaskState::Completed, None);
        assert_eq!(registry.get("a").unwrap().state, TaskState::Failed);
    }

    /// A source asked to retry a task that failed here gets a fresh record
    /// under the same id, and that record is the one the retry ends.
    #[test]
    fn a_retried_task_replaces_its_failed_record() {
        let registry = Registry::default();
        registry.add(task("t", vec![(0, 10)])).unwrap();
        registry.finish("t", TaskState::Failed, Some("timed out".into()));
        registry.add(task("t", vec![(0, 10)])).unwrap();
        assert_eq!(registry.all().len(), 1);
        assert_eq!(registry.get("t").unwrap().state, TaskState::InProgress);
        registry.finish("t", TaskState::Completed, None);
        assert_eq!(registry.get("t").unwrap().state, TaskState::Completed);
    }

    #[test]
    fn cancel_targets_running_tasks() {
        let registry = Registry::default();
        registry.add(task("a", vec![(0, 1)])).unwrap();
        registry.add(task("b", vec![(2, 3)])).unwrap();
        registry.finish("b", TaskState::Completed, None);

        assert_eq!(registry.cancel(Some("b")), 0, "already finished");
        assert_eq!(registry.cancel(Some("nope")), 0);
        assert_eq!(registry.cancel(None), 1);
        assert!(registry.get("a").unwrap().cancel.load(Ordering::Relaxed));
    }
}
