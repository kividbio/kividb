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
// slots, D throws away what it had received, and D tries again. If D's
// answer to the takeover does not come, S keeps writes paused until it
// knows whether D took the slots (see `await_takeover`).
//
// S pushes to D over an ordinary client connection, authenticating with the
// same credentials replication uses (`masteruser` / `masterauth`).

use super::{ClusterState, MigrationState, NodeId, key_hash_slot};
use crate::replication::{HubTx, ReplPayload, ReplState};
use parking_lot::Mutex;
use std::collections::HashMap;
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
/// The destination's answers to `CLUSTER SYNCSLOTS FAIL` that are not a
/// promise that the slots stay with the source (see [`Registry::fence`]).
const FENCE_TAKEN: &str = "the slots have been taken over";
const FENCE_BUSY: &str = "the slots are being taken over";
/// What [`parse_ranges`] says of an argument that is not a slot.
const INVALID_SLOT: &str = "Invalid or out of range slot";
/// How often a source that does not know whether the destination took the
/// slots over asks it, and how often it says why writes are still held.
const ASK_AGAIN: Duration = Duration::from_millis(200);
const LOG_HELD_EVERY: Duration = Duration::from_secs(5);

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
    /// Import only: the source reported that this attempt failed. A
    /// `TAKEOVER` that comes after it is refused (see [`Registry::fence`]).
    pub attempt_failed: Arc<AtomicBool>,
    /// Import only: the client id this node gave out last before it asked
    /// the source to start the current attempt. The source connects for
    /// each attempt afresh, after it has been asked, so a `TAKEOVER` on a
    /// connection of this id or a lower one belongs to an earlier attempt,
    /// and is refused.
    pub attempt_floor: Arc<AtomicU64>,
    /// Import only: a `TAKEOVER` has begun taking the slots over. From then
    /// on the source can no longer be told that they were not taken.
    pub taking_over: bool,
    /// When the task ended, on the monotonic clock (`end_time_ms` is the
    /// wall clock's, for `CLUSTER MIGRATION STATUS`).
    pub ended_at: Option<Instant>,
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
                task.ended_at = Some(Instant::now());
                if let Some(error) = error {
                    task.last_error = error;
                }
            }
        });
    }

    /// The destination's side of `TAKEOVER`, from client `client_id`: begin
    /// taking the slots over, if the import's current attempt is still on.
    /// Once begun, [`Self::fence`] answers that the slots are taken, or are
    /// being taken; before, a fence makes this refuse.
    fn begin_takeover(&self, id: &str, client_id: u64) -> Result<Task, String> {
        let mut tasks = self.tasks.lock();
        let task = tasks
            .iter_mut()
            .rev()
            .find(|t| t.id == id && t.operation == Operation::Import)
            .filter(|t| t.state == TaskState::InProgress && !t.cancel.load(Ordering::Relaxed))
            .ok_or_else(|| "no such import in progress".to_string())?;
        if task.attempt_failed.load(Ordering::SeqCst) {
            return Err("the source has given this attempt up".into());
        }
        if client_id <= task.attempt_floor.load(Ordering::SeqCst) {
            return Err("this takeover belongs to an earlier attempt".into());
        }
        task.taking_over = true;
        task.last_activity_ms.store(now_ms(), Ordering::Relaxed);
        Ok(task.clone())
    }

    /// A takeover begun with [`Self::begin_takeover`] did not happen.
    fn takeover_undone(&self, id: &str) {
        self.update(id, |t| t.taking_over = false);
    }

    /// The destination's side of `CLUSTER SYNCSLOTS FAIL`: the source has
    /// given the current attempt up, and asks whether the slots were taken
    /// over all the same (#278). `owned` says whether this node owns any of
    /// them. `Ok` is a promise: they were not, and this attempt will not
    /// take them (a later `TAKEOVER` of it is refused). Otherwise the error
    /// says that they have been taken ([`FENCE_TAKEN`]) or are being taken
    /// ([`FENCE_BUSY`]: ask again).
    fn fence(&self, id: &str, error: &str, owned: impl FnOnce() -> bool) -> Result<(), String> {
        let mut tasks = self.tasks.lock();
        let task = tasks
            .iter_mut()
            .rev()
            .find(|t| t.id == id && t.operation == Operation::Import);
        match task {
            Some(t) if t.state == TaskState::Completed => Err(FENCE_TAKEN.into()),
            Some(t) if t.taking_over => Err(FENCE_BUSY.into()),
            // Read with the tasks locked, so that a takeover cannot begin
            // or be undone in between.
            _ if owned() => Err(FENCE_TAKEN.into()),
            Some(t) => {
                t.last_error = error.to_string();
                t.attempt_failed.store(true, Ordering::SeqCst);
                Ok(())
            }
            // Unknown here (this node restarted since, or never had it): no
            // takeover can begin without the task.
            None => Ok(()),
        }
    }

    /// A failover is about to hand the slots of `source` (with `None`, of
    /// this node) to a replica of it (#516): the imports from it give their
    /// current attempt up, as when the source gives one up ([`Self::fence`]),
    /// and its `TAKEOVER` is refused. They retry, from the slots' owner then.
    /// Refused itself, with why, if one of them is taking the slots over
    /// now, or, from `source`, took them over in the last
    /// [`super::HANDOVER_FAILOVER_GRACE`]: the replica would claim slots
    /// that are this node's.
    pub(crate) fn fence_imports(&self, source: Option<NodeId>, why: &str) -> Result<(), String> {
        let mut tasks = self.tasks.lock();
        let from = |t: &Task| {
            t.operation == Operation::Import && source.is_none_or(|source| t.source == source)
        };
        if let Some(t) = tasks
            .iter()
            .find(|t| from(t) && t.state == TaskState::InProgress && t.taking_over)
        {
            return Err(format!(
                "this node is taking slots {} over in an atomic slot migration",
                ranges_text(&t.ranges)
            ));
        }
        if source.is_some()
            && let Some((t, ago)) = tasks.iter().find_map(|t| {
                let ago = t.ended_at?.elapsed();
                (from(t) && t.state == TaskState::Completed && ago < super::HANDOVER_FAILOVER_GRACE)
                    .then_some((t, ago))
            })
        {
            return Err(format!(
                "this node took slots {} over from that master {} ms ago",
                ranges_text(&t.ranges),
                ago.as_millis()
            ));
        }
        for t in tasks
            .iter_mut()
            .filter(|t| from(t) && t.state == TaskState::InProgress)
        {
            if !t.attempt_failed.swap(true, Ordering::SeqCst) {
                t.last_error = why.to_string();
            }
        }
        Ok(())
    }

    /// `f` of the slot ranges of the atomic imports in progress here, but
    /// for the one with the id `except`, with the tasks locked: none begins
    /// or ends meanwhile.
    fn with_imports<R>(&self, except: Option<&str>, f: impl FnOnce(&[&Ranges]) -> R) -> R {
        let tasks = self.tasks.lock();
        let importing: Vec<&Ranges> = tasks
            .iter()
            .filter(|t| {
                t.operation == Operation::Import
                    && t.state == TaskState::InProgress
                    && except != Some(t.id.as_str())
            })
            .map(|t| &t.ranges)
            .collect();
        f(&importing)
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
            .ok_or_else(|| INVALID_SLOT.to_string())
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

/// This node's dataset as its own, a master's, when it set out to delete
/// the keys of slots it does not serve. They are its to delete only while
/// it still is (#348). A node that has become a replica since has its
/// master's dataset, or is about to be given it, and those keys in it are
/// the master's. An atomic slot migration that drained the source, whose
/// destination's claim came before its answer to the `TAKEOVER`, had the
/// source follow the destination, load its dataset, and then delete from it
/// the keys it had just handed over. Promoted later, it had lost them.
///
/// Kept as the replication generation: a node that has been a replica
/// since, even one promoted again, is not the master that decided.
#[derive(Clone, Copy)]
pub(crate) struct OwnDataset(u64);

impl OwnDataset {
    /// The dataset now, if it is this node's own; `None` on a replica.
    pub(crate) fn now(repl_state: &ReplState) -> Option<Self> {
        let generation = repl_state.generation();
        (!repl_state.is_slave()).then_some(OwnDataset(generation))
    }

    /// Whether it still is. Exact in the write gate: a master becomes a
    /// replica with the gate cut (`ReplState::become_slave`), once it has
    /// taken a new generation (`begin_replication_epoch`), and a full sync
    /// puts the master's dataset in place with the gate cut too.
    fn still(self, repl_state: &ReplState) -> bool {
        !repl_state.is_slave() && repl_state.generation() == self.0
    }
}

/// What [`delete_keys`] did.
struct Deleted {
    removed: usize,
    /// It stopped: the dataset was no longer this node's own.
    stopped: bool,
    /// Slots whose keys it stopped deleting, because they came back to
    /// this node meanwhile (see [`SlotDeletions`]).
    came_back: usize,
}

// ── Deleting the keys of slots that left ─────────────────────────────────────

/// The deletions of the keys of slots that have left this node, and the
/// slots that come to it, to tell which keys a deletion may still delete
/// (#353).
///
/// Such a deletion runs on its own, in batches, for as long as the keys
/// take, and a slot can come back to this node before it ends: imported
/// again (`CLUSTER SETSLOT IMPORTING` and `MIGRATE`, or `CLUSTER MIGRATION
/// IMPORT`), assigned again (`SETSLOT NODE`, `ADDSLOTS`), or taken over in a
/// failover. Keys written to it here from then on are acknowledged writes,
/// and the deletion used to delete them with the others.
///
/// So whatever gives this node a slot counts it as come ([`Self::came_here`]),
/// holding the lock of the slot table or of the migration states it
/// changes, before a write to the slot can be routed here. A deletion notes
/// each slot's count when it begins. In each batch it takes the lock of the
/// counts, deletes only the keys of slots whose count has not moved, and
/// lets the lock go once they are deleted. A slot cannot come here in
/// between: counting it waits for the batch, at most one batch of 128 keys.
/// So a write to a key of the batch was routed to a slot that had come
/// before the batch looked, which it sees, or comes after the batch has
/// deleted the key. That holds for every write, also those that lock no
/// key (`write_order`): the commands of a transaction, a script, a write of
/// keys of several shards. A slot that has come back is left from then on,
/// whatever becomes of it.
///
/// What gives this node a slot by a command, or imports one, deletes first,
/// itself, the keys the node still had of the slot, if a deletion of them
/// is under way ([`delete_before_coming`]), as Redis deletes a lost slot's
/// keys before it does anything else. Left to the deletion, which leaves
/// the slot once it has come, the keys it had not reached would stay:
/// beside the keys an import brings, and keys the slot's owner had deleted
/// meanwhile would be back.
#[derive(Default)]
pub struct SlotDeletions {
    /// Held by a deletion's batch from its look to its last delete, which
    /// lock shards, the index store, each index's own locks (postings, an
    /// HNSW graph's `key_to_id` and `nodes`, flat vectors) and the write
    /// stream's queue meanwhile. Whatever gives this node a slot takes it
    /// holding `slots` or `migrations`. So nothing that holds one of those
    /// may take this, nor any lock of `ClusterState` (see there): it would
    /// wait for the batch, which waits for it.
    counts: Mutex<DeletionCounts>,
}

#[derive(Default)]
struct DeletionCounts {
    /// Per slot, once anything has been counted: how many times it has come
    /// to this node.
    came: Vec<u32>,
    /// Per slot: how many deletions of its keys are under way.
    running: Vec<u32>,
}

impl DeletionCounts {
    /// One entry per slot, from the first use on.
    fn sized(&mut self) -> &mut Self {
        if self.came.is_empty() {
            self.came = vec![0; super::CLUSTER_SLOTS];
            self.running = vec![0; super::CLUSTER_SLOTS];
        }
        self
    }
}

impl SlotDeletions {
    /// `slots` have come to this node: they are its own, or are being
    /// imported into it. Called by whatever gives them, holding the lock of
    /// what it changes, before a write to them can be routed here; for an
    /// atomic import, before it asks the source to start.
    pub fn came_here(&self, slots: impl IntoIterator<Item = u16>) {
        let mut counts = self.counts.lock();
        let counts = counts.sized();
        for slot in slots {
            let came = &mut counts.came[slot as usize];
            *came = came.wrapping_add(1);
        }
    }

    /// Whether the keys of one of `slots` are being deleted.
    pub fn running(&self, mut slots: impl Iterator<Item = u16>) -> bool {
        let counts = self.counts.lock();
        !counts.running.is_empty() && slots.any(|slot| counts.running[slot as usize] > 0)
    }

    /// Those of `slots` whose keys are being deleted.
    fn deleting(&self, slots: impl Iterator<Item = u16>) -> Vec<u16> {
        let counts = self.counts.lock();
        if counts.running.is_empty() {
            return Vec::new();
        }
        slots
            .filter(|&slot| counts.running[slot as usize] > 0)
            .collect()
    }
}

/// One deletion of the keys of some slots, since it began: see
/// [`SlotDeletions`].
struct DeleteRun {
    deletions: Arc<SlotDeletions>,
    /// Per slot whose keys it still deletes, how many times the slot had
    /// come to this node when it began; `None` for the other slots.
    seen: Vec<Option<u32>>,
    /// How many of its slots came back to this node while it ran.
    came_back: usize,
}

impl DeleteRun {
    /// Begin deleting the keys of those of `slots` that are not this
    /// node's, nor being imported into it, now. `import` is the atomic
    /// import whose keys these are, when it is one that did not complete:
    /// the slots of the others in progress are left out.
    fn begin(
        cluster: &ClusterState,
        slots: impl IntoIterator<Item = u16>,
        import: Option<&str>,
    ) -> Self {
        let me = cluster.myself();
        let deletions = Arc::clone(&cluster.slot_deletions);
        let mut seen = vec![None; super::CLUSTER_SLOTS];
        // The import tasks, the slot table, the migration states, the
        // counts: in this order (see `ClusterState`). What gives this node a
        // slot changes one of them first, and counts it under that lock.
        cluster.slot_migrations.with_imports(import, |importing| {
            let owners = cluster.slots.read();
            let migrations = cluster.migrations.read();
            let mut counts = deletions.counts.lock();
            let counts = counts.sized();
            for slot in slots {
                let i = slot as usize;
                let here = owners[i] == Some(me)
                    || matches!(
                        migrations.get(&slot),
                        Some(MigrationState::Importing { .. })
                    )
                    || importing.iter().any(|ranges| in_ranges(ranges, slot));
                if here || seen[i].is_some() {
                    continue;
                }
                seen[i] = Some(counts.came[i]);
                counts.running[i] += 1;
            }
        });
        DeleteRun {
            deletions,
            seen,
            came_back: 0,
        }
    }

    /// Whether it deletes any keys at all.
    fn deletes_any(&self) -> bool {
        self.seen.iter().any(Option::is_some)
    }

    /// Whether it still deletes the keys of `slot`.
    fn deletes(&self, slot: u16) -> bool {
        self.seen[slot as usize].is_some()
    }

    /// Of `keys`, which to delete: those of slots that have not come to
    /// this node since it began. One look at the `counts`, which the caller
    /// holds until they are deleted.
    fn still_to_delete(
        &mut self,
        counts: &mut DeletionCounts,
        keys: &[crate::store::Key],
    ) -> Vec<bool> {
        let counts = counts.sized();
        let mut delete = Vec::with_capacity(keys.len());
        for key in keys {
            let i = key_hash_slot(key.as_bytes()) as usize;
            delete.push(match self.seen[i] {
                Some(at) if counts.came[i] == at => true,
                Some(_) => {
                    self.seen[i] = None;
                    counts.running[i] -= 1;
                    self.came_back += 1;
                    false
                }
                None => false,
            });
        }
        delete
    }
}

impl Drop for DeleteRun {
    fn drop(&mut self) {
        let mut counts = self.deletions.counts.lock();
        let counts = counts.sized();
        for (i, seen) in self.seen.iter().enumerate() {
            if seen.is_some() {
                counts.running[i] -= 1;
            }
        }
    }
}

/// Delete the keys of the slots of `run`, telling replicas and the AOF, for
/// as long as the dataset is this node's `own`, and of each slot for as
/// long as it has not come back to this node. `checked` is called once a
/// batch's keys are picked, before they are deleted, with the batch held
/// whole; `between` between two batches, out of the write gate. Tests hold
/// the deletion there.
#[allow(clippy::too_many_arguments)]
fn delete_keys(
    store: &crate::store::Store,
    repl_state: &Arc<ReplState>,
    hub_tx: &HubTx,
    run: &mut DeleteRun,
    own: OwnDataset,
    mut checked: impl FnMut(),
    mut between: impl FnMut(),
) -> Deleted {
    let deletions = Arc::clone(&run.deletions);
    let mut removed = 0usize;
    let Some(db) = store.first() else {
        return Deleted {
            removed,
            stopped: false,
            came_back: run.came_back,
        };
    };
    let holds = crate::store::shards_of_slots(db.len(), |slot| run.deletes(slot));
    for (shard, _) in db.iter().zip(holds).filter(|(_, holds)| *holds) {
        let keys: Vec<crate::store::Key> = crate::read_lock!(shard)
            .keys()
            .filter(|k| run.deletes(key_hash_slot(k)))
            .map(crate::store::Key::new)
            .collect();
        for chunk in keys.chunks(128) {
            {
                // One whole write, as far as a full sync or a backup is
                // concerned.
                let _gate = crate::write_gate::enter();
                if !own.still(repl_state) {
                    return Deleted {
                        removed,
                        stopped: true,
                        came_back: run.came_back,
                    };
                }
                let _order = crate::write_order::lock_keys(0, chunk.iter().map(|k| k.as_bytes()));
                // Held until the batch's keys are deleted: a slot comes back
                // before the look, which sees it, or after the deletes, and
                // so does every write to it here (#353). See `SlotDeletions`.
                let mut counts = deletions.counts.lock();
                let delete = run.still_to_delete(&mut counts, chunk);
                checked();
                let mut del = vec![b"DEL".to_vec()];
                for (key, _) in chunk.iter().zip(delete).filter(|(_, delete)| *delete) {
                    // A document leaves its indexes with its key
                    // (`ShardMap::remove`).
                    if crate::commands::helpers::delete_key(store, 0, key) {
                        del.push(key.as_bytes().to_vec());
                        removed += 1;
                    }
                }
                if del.len() > 1 {
                    crate::replication::replicate_from_background(repl_state, hub_tx, 0, &del);
                }
            }
            between();
        }
    }
    Deleted {
        removed,
        stopped: false,
        came_back: run.came_back,
    }
}

/// [`delete_keys`]' `checked` and `between` outside tests: once, in the
/// first batch, the failpoint `name` (`aof::failpoint`), at which a test
/// holds the deletion part way: `slot-keys-checked`, with the batch's keys
/// picked and not yet deleted, or `slot-keys-deleting`, after the batch.
fn stall_once_for_test(name: &'static str) -> impl FnMut() {
    let mut first = true;
    move || {
        if std::mem::take(&mut first) {
            crate::aof::failpoint(name);
        }
    }
}

/// Before `slots` come to this node by `what` (`SETSLOT IMPORTING`,
/// `SETSLOT NODE`, `ADDSLOTS`, an atomic import): if a deletion of the keys
/// it had of some of them is under way, on its own, delete them here and
/// now, as Redis deletes a lost slot's keys before it does anything else
/// (#353). Left to that deletion, which leaves a slot once it has come, the
/// keys it had not reached would stay: beside the keys an import brings
/// (`MIGRATE` without `REPLACE` refuses them, `BUSYKEY`), and keys the
/// slot's owner had deleted meanwhile would be back.
///
/// One pass over the keyspace, as the deletion makes, without waiting for
/// the deletion's other slots. Not inside a transaction, which holds every
/// other command back: there the command is refused, `TRYAGAIN`, to be sent
/// again. Not on a replica, whose keys are its master's. `import` is the
/// atomic import that brings them, if one does: its slots are not left out
/// as being imported here.
pub fn delete_before_coming(
    env: &Env,
    slots: &[u16],
    what: &str,
    import: Option<&str>,
) -> Result<(), String> {
    let deleting = env.cluster.slot_deletions.deleting(slots.iter().copied());
    let Some(&first) = deleting.first() else {
        return Ok(());
    };
    let own = match OwnDataset::now(&env.repl_state) {
        Some(own) if env.cluster.my_master_id().is_none() => own,
        _ => return Ok(()),
    };
    let mut run = DeleteRun::begin(&env.cluster, deleting.iter().copied(), import);
    if !run.deletes_any() {
        return Ok(());
    }
    if crate::command_gate::alone_on_this_thread() || crate::write_gate::held_by_this_thread() {
        return Err(format!(
            "TRYAGAIN the keys this node had of slot {first} are still being deleted; \
             send {what} again outside a transaction, or once they are"
        ));
    }
    let deleted = off_the_runtime(|| {
        delete_keys(
            &env.store,
            &env.repl_state,
            &env.hub_tx,
            &mut run,
            own,
            || {},
            || {},
        )
    });
    log_info!(
        "Cluster: {what}: deleted the {} key(s) this node still had of {} slot(s), which \
         were being deleted, before it takes them",
        deleted.removed,
        deleting.len()
    );
    log_deletion(what, &deleted);
    Ok(())
}

/// Logged when [`delete_keys`] stopped part way, or left slots that came
/// back.
fn log_deletion(what: &str, deleted: &Deleted) {
    if deleted.stopped {
        log_info!(
            "Cluster: stopped deleting the keys of {what} after {}: this node has become \
             a replica since it began, or a master afresh, and the keys it has now are \
             not the ones it set out to delete",
            deleted.removed
        );
    }
    if deleted.came_back > 0 {
        log_info!(
            "Cluster: kept the keys of {} slot(s) of {what}: they came back to this node \
             while they were being deleted, and keys written to them here since are not \
             to be deleted",
            deleted.came_back
        );
    }
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
/// Runs on its own thread: the caller is handling a bus message. Only on a
/// master, and only for as long as it is one (see [`OwnDataset`]): on a
/// replica those keys are its master's.
pub(crate) fn drop_lost_slots(cluster: &ClusterState, slots: Vec<u16>) {
    let Some(hooks) = cluster.hooks.get() else {
        return;
    };
    let starter = &hooks.replica_starter;
    let _ = start_dropping(
        cluster,
        &starter.store,
        &starter.repl_state,
        &starter.hub_tx,
        slots,
    );
}

/// [`drop_lost_slots`], given the server's parts. The thread that deletes,
/// or `None` when there is nothing to delete: on a replica, or with every
/// slot back here already.
fn start_dropping(
    cluster: &ClusterState,
    store: &crate::store::Store,
    repl_state: &Arc<ReplState>,
    hub_tx: &HubTx,
    slots: Vec<u16>,
) -> Option<std::thread::JoinHandle<Deleted>> {
    let own = match OwnDataset::now(repl_state) {
        Some(own) if cluster.my_master_id().is_none() => own,
        _ => return None,
    };
    // Begun here, not on its thread: an import into one of the slots waits
    // for it from now on.
    let mut run = DeleteRun::begin(cluster, slots.iter().copied(), None);
    if !run.deletes_any() {
        return None;
    }
    let store = Arc::clone(store);
    let repl_state = Arc::clone(repl_state);
    let hub_tx = hub_tx.clone();
    let spawned = std::thread::Builder::new()
        .name("kividb-lost-slots".into())
        .spawn(move || {
            let deleted = delete_keys(
                &store,
                &repl_state,
                &hub_tx,
                &mut run,
                own,
                stall_once_for_test("slot-keys-checked"),
                stall_once_for_test("slot-keys-deleting"),
            );
            drop(run);
            log_deletion("slots owned by another node", &deleted);
            log_info!(
                "Cluster: done with the keys of {} slot(s) owned by another node: {} \
                 deleted",
                slots.len(),
                deleted.removed
            );
            deleted
        });
    match spawned {
        Ok(thread) => Some(thread),
        Err(e) => {
            log_error!(
                "Cluster: could not start the lost-slot cleanup thread: {e} — keys in slots now owned by another node stay here until the next restart"
            );
            None
        }
    }
}

/// A key captured at the cut, with a copy of its value of its own: a list,
/// hash or set can be changed in place once writes go on, and the copy is
/// as it was at the cut. Copying shares the elements (and a string's bytes,
/// which never change in place), so it is far quicker than laying the value
/// out as its `RESTORE`, which is done afterwards.
struct Captured {
    key: crate::store::Key,
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

/// The time series compaction rules among `keys`, as the `TS.CREATERULE`s
/// that make them again, to be sent after the keys' `RESTORE`s, which carry
/// no links (`snapshot::dump_payload`). A rule's two series are in one slot
/// and move together. Only a rule whose destination moves and names its
/// source back; any other is dead (#593).
fn compaction_rules(keys: &[Captured]) -> Vec<Vec<Vec<u8>>> {
    use crate::types::KiviDBValue;
    let sources: HashMap<&[u8], Option<String>> = keys
        .iter()
        .filter_map(|c| match &c.value {
            KiviDBValue::TimeSeries(series) => {
                Some((c.key.as_bytes(), series.read().source.clone()))
            }
            _ => None,
        })
        .collect();
    let mut out = Vec::new();
    for c in keys {
        let KiviDBValue::TimeSeries(series) = &c.value else {
            continue;
        };
        let name = crate::key_bytes::text_of(c.key.as_bytes());
        for rule in &series.read().rules {
            let dest = crate::key_bytes::bytes_of(&rule.dest_key);
            if sources
                .get(&*dest)
                .is_some_and(|source| source.as_deref() == Some(&*name))
            {
                out.push(vec![
                    b"TS.CREATERULE".to_vec(),
                    c.key.as_bytes().to_vec(),
                    dest.into_owned(),
                    b"AGGREGATION".to_vec(),
                    rule.aggregation.name().as_bytes().to_vec(),
                    rule.bucket_duration.to_string().into_bytes(),
                    rule.align_timestamp.to_string().into_bytes(),
                ]);
            }
        }
    }
    out
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
    let holds = crate::store::shards_of_slots(db.len(), |slot| in_ranges(ranges, slot));
    for (shard, _) in db.iter().zip(holds).filter(|(_, holds)| *holds) {
        let map = crate::read_lock!(shard);
        for entry in map.entries() {
            let key = entry.key();
            if !in_ranges(ranges, key_hash_slot(key)) {
                continue;
            }
            // In the entry beside the value.
            let expires_at = entry.expires().unwrap_or(0);
            if expires_at != 0 && expires_at <= now {
                continue;
            }
            out.push(Captured {
                key: crate::store::Key::new(key),
                value: entry.value().deep_clone(),
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
    if cluster.my_master_id().is_some() || env.repl_state.is_slave() {
        return Err("I am a replica".into());
    }
    // A replica of this node may be taking its slots over (#516).
    if cluster.writes_held() {
        return Err(
            "a failover of this node is under way: it does not know yet whether a \
                    replica of it has taken its slots over"
                .into(),
        );
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
        attempt_floor: Arc::new(AtomicU64::new(0)),
        taking_over: false,
        ended_at: None,
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
            let _ = ask_destination(&env, &task, &error);
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

/// Holds writes for a handoff, and lets them go when it ends, however it
/// ends (`ClusterState::end_handoff_hold`): unless they have been let go
/// already (`CLIENT UNPAUSE`, or this master's replica took its slots over),
/// or this master holds them for a reason of its own as well (a manual
/// failover, #242, or another handoff), which lets them go itself.
struct PauseGuard<'a>(&'a ClusterState);

impl<'a> PauseGuard<'a> {
    fn hold(cluster: &'a ClusterState) -> Self {
        cluster.hold_writes_for_handoff();
        PauseGuard(cluster)
    }
}

impl Drop for PauseGuard<'_> {
    fn drop(&mut self) {
        self.0.end_handoff_hold();
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
    // What this node is as the migration begins: the slots are its to hand
    // over only while it still is (#516).
    let failovers_at_start = env.cluster.failovers_begun();
    let no_failover_since = || -> Result<(), String> {
        if env.cluster.failovers_begun() == failovers_at_start {
            Ok(())
        } else {
            Err(super::FAILOVER_BEGAN.into())
        }
    };
    let own_at_start = OwnDataset::now(&env.repl_state).ok_or("I am a replica")?;
    let config_epoch = || {
        env.cluster
            .nodes
            .read()
            .get(&env.cluster.myself())
            .map(|n| n.config_epoch)
    };
    let epoch_at_start = config_epoch();
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
        .and_then(|keys| {
            let rules = compaction_rules(&keys);
            let mut commands = keys
                .into_iter()
                .map(Captured::into_restore)
                .collect::<std::io::Result<Vec<_>>>()?;
            commands.extend(rules);
            Ok(commands)
        })
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
        // Nothing more to stream for: the handover will be refused.
        no_failover_since()?;
        let mut commands = Vec::new();
        let bytes = drain(&mut commands)?;
        forward(&mut peer, &task.id, &commands)?;
        if bytes <= max_lag {
            break;
        }
    }

    if let Some(delay) = test_failpoint(&TEST_HANDOFF_DELAY) {
        log_warn!(
            "Cluster: (test) the handoff of slots {} waits {delay:?}",
            ranges_text(&task.ranges)
        );
        std::thread::sleep(delay);
    }
    no_failover_since()?;

    // ── Handoff ──────────────────────────────────────────────────────────
    // Pause client writes, let the ones in flight finish, send what they
    // wrote, and hand the slots over.
    //
    // Writes are held with no end of their own, until this node knows
    // whether the destination took the slots (#278). Once the tap is off,
    // a write this node takes is not sent on: it used to take them again
    // when the pause ran out by itself, with the drain or the TAKEOVER
    // still under way or its answer lost, and when the destination took
    // the slots after all, the keys were deleted here and those writes
    // lost. `write-pause-timeout` is now how long the source waits for the
    // TAKEOVER's answer before it asks.
    let pause_timeout = write_pause_timeout_ms(&env.config).max(100);
    let paused_at = Instant::now();
    // This node's dataset as the handoff begins: the keys of the slots it
    // hands over are its to delete only while it still is (#348).
    let own = OwnDataset::now(&env.repl_state);
    let pause = PauseGuard::hold(&env.cluster);
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
    if !crate::pause::handoff() {
        // Let go (`CLIENT UNPAUSE`): what was written since was not sent,
        // and the slots must not move without it.
        return Err("writes were let go (CLIENT UNPAUSE) before the slots were handed over".into());
    }
    // Only by the master of the slots, as it was when the migration began,
    // and not if a failover of it has begun since: a replica of it may
    // have taken the slots over, and served writes to them that never
    // reached the destination (#516). From here until the handover ends,
    // a failover of this node is refused.
    env.cluster.begin_handover(failovers_at_start, || {
        if !own_at_start.still(&env.repl_state) || env.cluster.my_master_id().is_some() {
            return Err("this node has become a replica".into());
        }
        if let Some(slot) = slots_of(&task.ranges).find(|&s| !env.cluster.owns_slot(s)) {
            return Err(format!("slot {slot} is no longer this node's"));
        }
        let epoch = config_epoch();
        if epoch != epoch_at_start {
            return Err(format!(
                "this node's config epoch went from {} to {} during the migration",
                epoch_at_start.unwrap_or(0),
                epoch.unwrap_or(0)
            ));
        }
        Ok(())
    })?;
    struct Handover<'a>(&'a ClusterState);
    impl Drop for Handover<'_> {
        fn drop(&mut self) {
            self.0.end_handover();
        }
    }
    let _handover = Handover(&env.cluster);

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
        // No answer is not a refusal, nor is an error: the destination may
        // have taken the slots and the reply been lost, or take them yet.
        // Writes stay held until this node knows which.
        await_takeover(env, task, &error)?;
    }

    let taken_ms = paused_at.elapsed().as_millis();

    // The destination owns the slots. Say so here, then let writes resume —
    // for these slots they now redirect — and drop our copy of the keys.
    // nodes.conf is saved after writes resume: the save is synced, and with
    // the disk busy that took seconds, all of them with writes paused. Lost
    // to a crash in between, the record is put right by the destination's
    // claim, which has the newer epoch.
    //
    // Only the slots that are still this node's, and only while its dataset
    // is its own: the destination's claim can have come first, and taken
    // them, and made this node a replica of the destination if it left it
    // with none (see `hand_over_slots`). Their keys are then not this
    // node's to delete (#348).
    let own = own.filter(|own| own.still(&env.repl_state));
    let handed = match own {
        Some(_) => env
            .cluster
            .hand_over_slots(slots_of(&task.ranges), task.dest),
        None => Vec::new(),
    };
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
    let slots = slots_of(&task.ranges).count();
    if handed.len() < slots {
        log_info!(
            "Cluster: {} of slots {} had gone to {} by its claim before its answer: \
             their keys were left to that claim (dropped with the slots, or, on a \
             node it left with none, its new master's)",
            slots - handed.len(),
            ranges_text(&task.ranges),
            super::node_id_str(&task.dest)
        );
    }
    if let Some(own) = own
        && !handed.is_empty()
    {
        let mut run = DeleteRun::begin(&env.cluster, handed.iter().copied(), None);
        let deleted = delete_keys(
            &env.store,
            &env.repl_state,
            &env.hub_tx,
            &mut run,
            own,
            stall_once_for_test("slot-keys-checked"),
            stall_once_for_test("slot-keys-deleting"),
        );
        log_deletion("the slots handed over", &deleted);
    }
    Ok(paused_ms)
}

/// What the destination says when asked whether it took the slots over.
#[derive(Debug, PartialEq)]
enum Asked {
    /// It has: they are its.
    Taken,
    /// It has not, and has promised it will not in this attempt.
    NotTaken,
    /// No answer that says either (why).
    Unsure(String),
}

/// Ask the destination, over a connection of its own, whether it took the
/// slots over, telling it the attempt is given up if it has not
/// (`CLUSTER SYNCSLOTS FAIL`, see [`Registry::fence`]).
fn ask_destination(env: &Env, task: &Task, why: &str) -> Asked {
    let mut parts: Vec<Vec<u8>> = vec![
        b"CLUSTER".to_vec(),
        b"SYNCSLOTS".to_vec(),
        b"FAIL".to_vec(),
        task.id.as_bytes().to_vec(),
        why.as_bytes().to_vec(),
    ];
    for (start, end) in &task.ranges {
        parts.push(start.to_string().into_bytes());
        parts.push(end.to_string().into_bytes());
    }
    let legacy = parts.clone();
    // Who asks: the destination heeds it only from the source of its
    // current attempt (#516).
    parts.push(b"FROM".to_vec());
    parts.push(
        super::node_id_str(&env.cluster.myself())
            .as_bytes()
            .to_vec(),
    );
    let ask = |parts: &[Vec<u8>]| {
        let mut peer = Peer::connect(env, &task.dest)?;
        let _ = peer.sock.set_read_timeout(Some(Duration::from_secs(5)));
        peer.call(&crate::resp::write::encode_resp_command(parts))
    };
    let answer = match ask(&parts) {
        // A destination of an earlier version reads `FROM` as a slot.
        Err(e) if e.contains(INVALID_SLOT) => ask(&legacy),
        answer => answer,
    };
    match answer {
        Ok(()) => Asked::NotTaken,
        Err(e) if e.contains(FENCE_TAKEN) => Asked::Taken,
        Err(e) => Asked::Unsure(e),
    }
}

/// After a `TAKEOVER` that went unanswered (`error`), with writes held:
/// wait until this node knows whether the destination took the slots over.
/// `Ok` once it knows it did, and the caller hands them over; an error once
/// it knows it did not, and the caller gives the attempt up and lets the
/// writes go. Until then writes stay held (#278): taken again while the
/// destination might yet take the slots, they would be deleted with the
/// keys when it does.
///
/// Every 200 ms it looks for the destination's claim, and asks the
/// destination over a fresh connection (`ask_destination`): it says the
/// slots are its, or promises that this attempt will not take them. Why it
/// cannot be sure is logged every 5 s.
///
/// A destination that is down, or cut off, cannot answer. Writes are let go
/// without its answer only when the cluster has marked it FAIL (a majority
/// of the masters agreed it is unreachable; or it has been forgotten) and no
/// claim of the slots has reached this node. That is the cluster's verdict
/// that it is gone, as #242 relies on the cluster's view of a master; but
/// a destination that did take the slots, and failed before its claim went
/// out, has them in its saved config and claims them again when it comes
/// back. So before writes are let go, this node claims the slots again at an
/// epoch above any the destination can have taken them at: two above the
/// highest this node has seen. The destination took them at one above the
/// highest it had seen, and it hears of epochs from the same cluster. A
/// claim of the destination that arrives later is older than this one, and
/// it is the destination that gives the slots up, with what it had of them.
///
/// `CLIENT UNPAUSE` lets the writes go, as it does a master's hold for a
/// manual failover (#242), and that is logged; the wait goes on, to know
/// how the task ended. A cancel is not heeded here: the slots may have
/// moved already.
fn await_takeover(env: &Env, task: &Task, error: &str) -> Result<(), String> {
    let first = task.ranges[0].0;
    let me = env.cluster.myself();
    let held_from = Instant::now();
    let mut logged = Instant::now();
    let mut let_go_logged = false;
    let why = format!("no answer to TAKEOVER: {error}");
    loop {
        match env.cluster.slot_owner(first) {
            Some(owner) if owner == task.dest => return Ok(()),
            Some(owner) if owner == me => {}
            // This node lost the slots some other way (a replica of it took
            // its slots over): whatever the destination did, they are not
            // this node's to hand over.
            _ => return Err("the slots have been taken over by another node".into()),
        }
        let unsure = match ask_destination(env, task, &why) {
            Asked::Taken => return Ok(()),
            Asked::NotTaken => {
                return Err(format!(
                    "the destination did not take the slots over: {error}"
                ));
            }
            Asked::Unsure(why) => why,
        };
        if env.cluster.slot_owner(first) == Some(task.dest) {
            return Ok(());
        }
        let failed = env
            .cluster
            .nodes
            .read()
            .get(&task.dest)
            .is_none_or(|n| n.flags.contains(super::NodeFlags::FAIL));
        if failed {
            let _ = env.cluster.bump_epoch();
            if let Err(e) = env.cluster.take_over_slots(slots_of(&task.ranges)) {
                log_warn!("Cluster: Could not save the cluster config: {e}");
            }
            return Err(format!(
                "the destination has failed; it did not answer whether it took the slots over \
                 ({unsure}), and they are claimed here again at epoch {}",
                env.cluster.current_epoch()
            ));
        }
        if !let_go_logged && !crate::pause::handoff() {
            let_go_logged = true;
            log_warn!(
                "Cluster: Writes held for the handoff of slots {} were let go (CLIENT UNPAUSE) \
                 before this node knew whether {} took the slots over",
                ranges_text(&task.ranges),
                super::node_id_str(&task.dest)
            );
        }
        if logged.elapsed() >= LOG_HELD_EVERY {
            logged = Instant::now();
            log_warn!(
                "Cluster: The handoff of slots {} has waited {} ms (writes {}): it is not known \
                 whether {} took them over ({unsure})",
                ranges_text(&task.ranges),
                held_from.elapsed().as_millis(),
                if let_go_logged { "let go" } else { "held" },
                super::node_id_str(&task.dest)
            );
        }
        std::thread::sleep(ASK_AGAIN);
    }
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
        attempt_floor: Arc::new(AtomicU64::new(0)),
        taking_over: false,
        ended_at: None,
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
            delete_unowned(&env, &task);
            registry.finish(&task_id, TaskState::Canceled, None);
            save_imports(&env.cluster);
            return;
        }
        // A replica of this node took its slots over: it is no destination.
        // What arrived is its new master's to keep or drop.
        if env.cluster.my_master_id().is_some() {
            registry.finish(
                &task_id,
                TaskState::Failed,
                Some("this node has become a replica".into()),
            );
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
        // A TAKEOVER of an earlier attempt that is still on its way is
        // refused: its connection was made before this one was asked for.
        task.attempt_floor
            .store(crate::client_registry::next_client_id(), Ordering::SeqCst);
        task.attempt_failed.store(false, Ordering::SeqCst);
        task.last_activity_ms.store(now_ms(), Ordering::Relaxed);
        registry.update(&task_id, |t| {
            t.source = source;
            t.start_time_ms = now_ms();
            t.taking_over = false;
        });

        // The keys this node still had of these slots, if a deletion of
        // them is under way, go first: it would keep those it had not
        // reached beside the ones the import brings (#353). Then the slots
        // have come here, before the source is asked to send anything: a
        // deletion of their keys begun before leaves them, and every key the
        // import writes. On its own thread: never in a transaction.
        // Not from a source a replica of which may be taking the slots over
        // (#516): its TAKEOVER would be refused, and it would stream them
        // again and again for nothing.
        let mut probe = task.clone();
        probe.source = source;
        let outcome = match source_still_owns(&env.cluster, &probe) {
            Err(why) => Some(format!("the source was not asked to start: {why}")),
            Ok(()) => {
                let slots: Vec<u16> = slots_of(&task.ranges).collect();
                let _ = delete_before_coming(&env, &slots, "an atomic import", Some(&task.id));
                env.cluster.slot_deletions.came_here(slots);
                import_attempt(&env, &task, source)
            }
        };
        match outcome {
            // Taken over, canceled, or otherwise finished by a handler.
            None => return,
            Some(error) => {
                delete_unowned(&env, &task);
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
            delete_unowned(env, task);
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
/// of slots this node has come to own or imports otherwise, nor any on a
/// node that has become a replica, whose keys are its master's (see
/// [`OwnDataset`]).
fn delete_unowned(env: &Env, task: &Task) {
    if env.cluster.my_master_id().is_some() {
        return;
    }
    let Some(own) = OwnDataset::now(&env.repl_state) else {
        return;
    };
    let mut run = DeleteRun::begin(&env.cluster, slots_of(&task.ranges), Some(&task.id));
    if !run.deletes_any() {
        return;
    }
    let deleted = delete_keys(
        &env.store,
        &env.repl_state,
        &env.hub_tx,
        &mut run,
        own,
        || {},
        || {},
    );
    log_deletion("an import that did not complete", &deleted);
}

/// `CLUSTER SYNCSLOTS APPLY`, on the destination, from client `client_id`:
/// is this task expecting data on that connection? Only from the current
/// attempt's source, which connected after it was asked to start (see
/// [`Task::attempt_floor`]), and not once the attempt is given up. The
/// stream of an earlier attempt's source, a demoted one, still sending,
/// used to land in the next attempt: pushes applied twice, an old value
/// over a newer acknowledged one (#516).
pub fn accept_apply(cluster: &ClusterState, task_id: &str, client_id: u64) -> Result<(), String> {
    match cluster.slot_migrations.get(task_id) {
        Some(task)
            if task.operation == Operation::Import
                && task.state == TaskState::InProgress
                && !task.cancel.load(Ordering::Relaxed)
                && !task.attempt_failed.load(Ordering::SeqCst)
                && client_id > task.attempt_floor.load(Ordering::SeqCst) =>
        {
            task.last_activity_ms.store(now_ms(), Ordering::Relaxed);
            Ok(())
        }
        _ => Err("no such import in progress".into()),
    }
}

/// `CLUSTER SYNCSLOTS TAKEOVER`, on the destination, from client
/// `client_id`: everything has arrived; take ownership of the slots.
pub fn take_over(cluster: &ClusterState, task_id: &str, client_id: u64) -> Result<(), String> {
    if let Some(delay) = test_failpoint(&TEST_TAKEOVER_DELAY) {
        log_warn!("Cluster: (test) TAKEOVER waits {delay:?} before it takes the slots");
        test_sleep(delay);
    }
    // The reply, and the claim the bus would carry, are not to reach the
    // source in time: the bus is muted before the slots are taken.
    let lost = test_failpoint(&TEST_TAKEOVER_LOST);
    if lost.is_some() {
        super::bus::heartbeat::set_test_bus_muted(true);
    }
    let result = take_over_now(cluster, task_id, client_id);
    if let Some(delay) = lost {
        log_warn!("Cluster: (test) the TAKEOVER's reply and claim are held back {delay:?}");
        test_sleep(delay);
        super::bus::heartbeat::set_test_bus_muted(false);
    }
    if let Some(delay) = test_failpoint(&TEST_TAKEOVER_REPLY_DELAY) {
        log_warn!("Cluster: (test) the TAKEOVER's reply is held back {delay:?}, its claim is not");
        test_sleep(delay);
    }
    result
}

fn take_over_now(cluster: &ClusterState, task_id: &str, client_id: u64) -> Result<(), String> {
    // Refused if the source has given this attempt up meanwhile: it has
    // been told that the slots stay with it, and takes writes again.
    let task = cluster.slot_migrations.begin_takeover(task_id, client_id)?;
    // Only from the master of the slots, as this node sees it (#516). From
    // here a vote for a replica of the source is refused (`fence_imports`).
    if let Err(why) = source_still_owns(cluster, &task) {
        task.attempt_failed.store(true, Ordering::SeqCst);
        cluster.slot_migrations.takeover_undone(task_id);
        cluster
            .slot_migrations
            .update(task_id, |t| t.last_error = why.clone());
        log_warn!(
            "Cluster: Not taking slots {} over from {}: {why}",
            ranges_text(&task.ranges),
            super::node_id_str(&task.source)
        );
        return Err(why);
    }
    // The source keeps client writes paused until it knows how this ends.
    let started = Instant::now();
    // Saved before the source is told: it deletes its copy when told, and a
    // restart of this node with the slots not in its config would delete
    // this one (#108). Not saved, not taken over: the slots stay with the
    // source, which keeps serving them. They are taken only once saved, so
    // there is nothing to give back: no write was served to them here, and
    // no claim of them went out (#337).
    if let Err(e) =
        super::persist::with_deferred_sync(|| cluster.take_over_slots(slots_of(&task.ranges)))
    {
        cluster.slot_migrations.takeover_undone(task_id);
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

/// Whether the source of `task` is still the master of its slots, in this
/// node's view, for the destination to take them over (#516). A replica of
/// the source that has taken them over, or is being elected to, serves
/// writes that the source never sent; taken over from the source, the slots
/// would lose them.
fn source_still_owns(cluster: &ClusterState, task: &Task) -> Result<(), String> {
    if cluster.my_master_id().is_some() {
        return Err("this node has become a replica".into());
    }
    let source = super::node_id_str(&task.source).to_string();
    let master = cluster.nodes.read().get(&task.source).is_some_and(|n| {
        n.flags.contains(super::NodeFlags::MASTER) && !n.flags.contains(super::NodeFlags::FAIL)
    });
    if !master {
        return Err(format!("the source {source} is no longer a master here"));
    }
    if let Some(slot) = slots_of(&task.ranges).find(|&s| cluster.slot_owner(s) != Some(task.source))
    {
        return Err(format!(
            "slot {slot} is no longer the source's here: a replica of it may have taken it over"
        ));
    }
    // For as long as the replica voted for can still win with this vote.
    let voted = cluster.last_vote_for.lock().get(&task.source).copied();
    if let Some(at) = voted
        && at.elapsed() < cluster.failover_window()
    {
        return Err(format!(
            "this node voted for a replica of the source {source} to take its slots over {} ms ago",
            at.elapsed().as_millis()
        ));
    }
    Ok(())
}

/// `CLUSTER SYNCSLOTS FAIL <task> <reason> [start end ...]`, on the
/// destination: the source gave the attempt up. `Ok` promises that this
/// node has not taken the slots (`ranges`, or the task's own) and that the
/// attempt will not take them; the error says they have been taken, or are
/// being taken. See [`Registry::fence`].
///
/// A source asks this when it does not know whether the slots were taken
/// over: the `TAKEOVER` went unanswered. It holds writes until it knows
/// (#278). A source of an earlier version did not send the ranges, nor
/// look at the answer.
///
/// `from`, the node that asks, if it says (a source of an earlier version
/// does not): the question is heeded only from the source of the current
/// attempt. An earlier attempt's, demoted since, would fence the attempt
/// from the new owner (#516).
pub fn source_failed(
    cluster: &ClusterState,
    task_id: &str,
    error: &str,
    ranges: &[(u16, u16)],
    from: Option<NodeId>,
) -> Result<(), String> {
    if let Some(from) = from
        && let Some(task) = cluster.slot_migrations.get(task_id)
        && task.operation == Operation::Import
        && task.source != from
    {
        return Err(format!(
            "{} is not the source of this import's current attempt",
            super::node_id_str(&from)
        ));
    }
    let ranges = match ranges {
        [] => cluster
            .slot_migrations
            .get(task_id)
            .map(|t| t.ranges)
            .unwrap_or_default(),
        given => given.to_vec(),
    };
    cluster.slot_migrations.fence(task_id, error, || {
        slots_of(&ranges).any(|slot| cluster.owns_slot(slot))
    })
}

// ── For the tests of a handoff whose TAKEOVER goes unanswered ────────────────

/// `KIVIDB_TEST_TAKEOVER_DELAY_MS`, never set otherwise: the first `TAKEOVER`
/// this node is sent waits this long before it takes the slots over, as on
/// a destination too busy to get to it in time.
static TEST_TAKEOVER_DELAY: (&str, AtomicBool) =
    ("KIVIDB_TEST_TAKEOVER_DELAY_MS", AtomicBool::new(false));

/// `KIVIDB_TEST_TAKEOVER_LOST_MS`, never set otherwise: the first `TAKEOVER`
/// this node is sent takes the slots over, and then for this long neither
/// its reply nor this node's claim goes out (its cluster bus is muted), as
/// when the reply is lost and the bus is slow.
static TEST_TAKEOVER_LOST: (&str, AtomicBool) =
    ("KIVIDB_TEST_TAKEOVER_LOST_MS", AtomicBool::new(false));

/// `KIVIDB_TEST_TAKEOVER_REPLY_DELAY_MS`, never set otherwise: the first
/// `TAKEOVER` this node is sent takes the slots over and then waits this
/// long before it answers, its cluster bus running meanwhile: the source
/// hears of the takeover from this node's claim first.
static TEST_TAKEOVER_REPLY_DELAY: (&str, AtomicBool) = (
    "KIVIDB_TEST_TAKEOVER_REPLY_DELAY_MS",
    AtomicBool::new(false),
);

/// `KIVIDB_TEST_HANDOFF_DELAY_MS`, never set otherwise: the first migration
/// away from this node waits this long before its handoff, with the stream
/// still running, as a long one does, for a failover to come meanwhile.
static TEST_HANDOFF_DELAY: (&str, AtomicBool) =
    ("KIVIDB_TEST_HANDOFF_DELAY_MS", AtomicBool::new(false));

/// The delay a failpoint asks for, the first time it is reached.
fn test_failpoint(failpoint: &(&str, AtomicBool)) -> Option<Duration> {
    let ms: u64 = std::env::var(failpoint.0).ok()?.parse().ok()?;
    if failpoint.1.swap(true, Ordering::Relaxed) {
        return None;
    }
    Some(Duration::from_millis(ms))
}

/// Sleep on a command's thread (see [`off_the_runtime`]): the source's
/// question comes on a connection of its own, and has to be answered
/// meanwhile.
fn test_sleep(delay: Duration) {
    off_the_runtime(|| std::thread::sleep(delay));
}

/// Run `f`, which waits, on a command's thread, leaving its other tasks to
/// another worker of the multi-threaded runtime (as
/// `pause::wait_out_handoff` does).
pub(crate) fn off_the_runtime<R>(f: impl FnOnce() -> R) -> R {
    let on_multi_thread_runtime = tokio::runtime::Handle::try_current().is_ok_and(|runtime| {
        runtime.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread
    });
    if on_multi_thread_runtime {
        tokio::task::block_in_place(f)
    } else {
        f()
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
            attempt_floor: Arc::new(AtomicU64::new(0)),
            taking_over: false,
            ended_at: None,
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

    /// The destination's promise to a source that does not know whether its
    /// TAKEOVER got through (#278): once it has said the slots were not
    /// taken, this attempt cannot take them; once a takeover has begun, it
    /// cannot say so.
    #[test]
    fn a_fenced_attempt_takes_nothing_over_and_a_takeover_cannot_be_fenced() {
        let registry = Registry::default();
        let not_owned = || false;

        // Fenced first: the TAKEOVER that comes after is refused.
        registry.add(task("a", vec![(0, 10)])).unwrap();
        assert_eq!(registry.fence("a", "gave up", not_owned), Ok(()));
        assert_eq!(registry.get("a").unwrap().last_error, "gave up");
        assert!(registry.begin_takeover("a", 1).is_err());
        // And it can be asked again, with the same answer.
        assert_eq!(registry.fence("a", "gave up", not_owned), Ok(()));

        // Taking over first: the source is told to ask again, then that
        // the slots are taken.
        registry.add(task("b", vec![(20, 30)])).unwrap();
        assert!(registry.begin_takeover("b", 1).is_ok());
        assert_eq!(registry.fence("b", "x", not_owned), Err(FENCE_BUSY.into()));
        registry.finish("b", TaskState::Completed, None);
        assert_eq!(registry.fence("b", "x", not_owned), Err(FENCE_TAKEN.into()));
        // A takeover that failed (its config could not be saved) is not.
        registry.add(task("c", vec![(40, 50)])).unwrap();
        assert!(registry.begin_takeover("c", 1).is_ok());
        registry.takeover_undone("c");
        assert_eq!(registry.fence("c", "x", not_owned), Ok(()));

        // Slots this node owns are taken, whether it knows the task or not:
        // it may have restarted since it took them.
        assert_eq!(
            registry.fence("gone", "x", || true),
            Err(FENCE_TAKEN.into())
        );
        assert_eq!(registry.fence("gone", "x", not_owned), Ok(()));

        // A TAKEOVER on a connection older than the current attempt is from
        // an earlier one.
        let d = task("d", vec![(60, 70)]);
        d.attempt_floor.store(100, Ordering::SeqCst);
        registry.add(d).unwrap();
        assert!(registry.begin_takeover("d", 100).is_err());
        assert!(registry.begin_takeover("d", 101).is_ok());
    }

    /// A failover of the source fences the imports from it (#516): their
    /// current attempt is given up, and its `TAKEOVER` refused. Not while one
    /// is taking the slots over, nor just after one took them from it.
    #[test]
    fn a_failover_of_the_source_fences_the_imports_from_it() {
        let source = super::super::generate_node_id();
        let other = super::super::generate_node_id();
        let registry = Registry::default();
        let from = |id: &str, ranges: Ranges, source| {
            let mut t = task(id, ranges);
            t.source = source;
            t
        };
        registry.add(from("a", vec![(0, 10)], source)).unwrap();
        registry.add(from("b", vec![(20, 30)], other)).unwrap();
        assert_eq!(registry.fence_imports(Some(source), "elected"), Ok(()));
        assert_eq!(registry.get("a").unwrap().last_error, "elected");
        assert!(registry.begin_takeover("a", 1).is_err());
        // Another source's import goes on.
        assert!(registry.begin_takeover("b", 1).is_ok());
        // One taking its slots over refuses the failover of its source, and
        // of this node (`None`)...
        let refused = registry.fence_imports(Some(other), "x").unwrap_err();
        assert!(refused.contains("taking slots 20-30 over"), "{refused}");
        assert!(registry.fence_imports(None, "x").is_err());
        // ... and once it has, its source's, for a moment.
        registry.finish("b", TaskState::Completed, None);
        let refused = registry.fence_imports(Some(other), "x").unwrap_err();
        assert!(refused.contains("took slots 20-30 over"), "{refused}");
        assert_eq!(registry.fence_imports(None, "x"), Ok(()));
    }

    /// An earlier attempt's source, still streaming, or asking whether the
    /// slots were taken, is not heeded in the next attempt (#516).
    #[test]
    fn an_earlier_attempts_source_is_not_heeded() {
        let cs = ClusterState::new_enabled(None, 15000);
        let (source, stale) = (
            super::super::generate_node_id(),
            super::super::generate_node_id(),
        );
        let mut t = task("a", vec![(0, 10)]);
        t.source = source;
        t.attempt_floor.store(100, Ordering::SeqCst);
        cs.slot_migrations.add(t).unwrap();
        // A connection made before this attempt was asked for.
        assert!(accept_apply(&cs, "a", 100).is_err());
        assert!(accept_apply(&cs, "a", 101).is_ok());
        // A FAIL from another node fences nothing.
        assert!(source_failed(&cs, "a", "x", &[], Some(stale)).is_err());
        assert!(
            !cs.slot_migrations
                .get("a")
                .unwrap()
                .attempt_failed
                .load(Ordering::SeqCst)
        );
        assert_eq!(
            source_failed(&cs, "a", "gave up", &[], Some(source)),
            Ok(())
        );
        assert!(
            accept_apply(&cs, "a", 101).is_err(),
            "the attempt is given up"
        );
        // A source that does not say who it is, as before.
        let mut t = task("b", vec![(20, 30)]);
        t.source = source;
        cs.slot_migrations.add(t).unwrap();
        assert_eq!(source_failed(&cs, "b", "x", &[], None), Ok(()));
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

    // ── Deleting the keys of slots that left (#353) ──────────────────────

    /// The slot another master has taken from this node.
    const LOST: u16 = 1234;
    /// How many keys this node had of it.
    const OLD: usize = 2000;

    /// A hash tag of `LOST`.
    fn tag() -> String {
        (0..)
            .map(|i| format!("t{i}"))
            .find(|t| key_hash_slot(t.as_bytes()) == LOST)
            .unwrap()
    }

    fn old_key(i: usize) -> String {
        format!("{{{}}}:old:{i}", tag())
    }

    fn set(store: &crate::store::Store, key: &str, value: &str) {
        let _ = crate::store::get_shard(&store[0], key)
            .write()
            .insert_str(key, value);
    }

    /// A master that owned every slot and has lost `LOST`, with its `OLD`
    /// keys of it, to `other`.
    struct Lost {
        cluster: Arc<ClusterState>,
        other: NodeId,
        store: crate::store::Store,
        index_store: crate::vector::IndexStore,
        repl_state: Arc<ReplState>,
        hub_tx: HubTx,
        _hub_rx: tokio::sync::mpsc::UnboundedReceiver<crate::replication::HubMsg>,
    }

    impl Lost {
        fn new() -> Self {
            let cluster = ClusterState::new_enabled(None, 15000);
            cluster.claim_slot_range(0, 16383).unwrap();
            let other = crate::cluster::generate_node_id();
            let mut node = crate::cluster::ClusterNode::new(other, "10.0.0.1".into(), 7000, 17000);
            node.flags |= crate::cluster::NodeFlags::MASTER;
            cluster.nodes.write().insert(other, node);
            cluster.slots.write()[LOST as usize] = Some(other);
            let store = crate::store::new_store(1);
            for i in 0..OLD {
                set(&store, &old_key(i), "old");
            }
            let (hub_tx, _hub_rx) = tokio::sync::mpsc::unbounded_channel();
            Lost {
                cluster: Arc::new(cluster),
                other,
                store,
                index_store: crate::vector::new_index_store(),
                repl_state: Arc::new(ReplState::new_master()),
                hub_tx,
                _hub_rx,
            }
        }

        /// Delete the keys of `LOST` as `drop_lost_slots` does, `between`
        /// run once, after the first batch.
        fn delete(&self, between: impl FnOnce()) -> Deleted {
            self.delete_held(|| {}, between)
        }

        /// [`Self::delete`], `checked` run once too, in the first batch,
        /// once its keys are picked and before they are deleted.
        fn delete_held(&self, checked: impl FnOnce(), between: impl FnOnce()) -> Deleted {
            let own = OwnDataset::now(&self.repl_state).unwrap();
            let mut run = DeleteRun::begin(&self.cluster, [LOST], None);
            let (mut checked, mut between) = (Some(checked), Some(between));
            delete_keys(
                &self.store,
                &self.repl_state,
                &self.hub_tx,
                &mut run,
                own,
                || {
                    if let Some(checked) = checked.take() {
                        checked();
                    }
                },
                || {
                    if let Some(between) = between.take() {
                        between();
                    }
                },
            )
        }

        fn value(&self, key: &str) -> Option<Vec<u8>> {
            let shard = crate::store::get_shard(&self.store[0], key);
            match crate::read_lock!(shard).get(key)?.to_value() {
                crate::types::KiviDBValue::String(bytes) => Some(bytes.as_slice().to_vec()),
                _ => None,
            }
        }

        fn old_keys_left(&self) -> usize {
            (0..OLD)
                .filter(|&i| self.value(&old_key(i)).is_some())
                .count()
        }

        fn env(&self) -> Env {
            Env {
                store: Arc::clone(&self.store),
                index_store: Arc::clone(&self.index_store),
                repl_state: Arc::clone(&self.repl_state),
                hub_tx: self.hub_tx.clone(),
                cluster: Arc::clone(&self.cluster),
                config: Arc::new(crate::config::Config::default()),
            }
        }
    }

    /// The slot coming back to this node while its keys are deleted, each
    /// way it can, and keys written to it here then: by an import, or by
    /// clients of the node that owns it again. The deletion went on, and
    /// deleted them with the keys it had set out to delete (#353). It now
    /// stops at the slot, and keeps every one of them. On its own, with
    /// nothing coming back, it deletes them all.
    #[test]
    fn keys_written_to_a_slot_that_came_back_while_its_keys_were_deleted_are_kept() {
        let lost = Lost::new();
        let deleted = lost.delete(|| {});
        assert_eq!((deleted.removed, deleted.came_back), (OLD, 0));
        assert!(!deleted.stopped);
        assert_eq!(lost.old_keys_left(), 0);

        type Coming = fn(&Lost);
        let comings: [(&str, Coming); 7] = [
            ("SETSLOT IMPORTING", |l| {
                l.cluster.set_importing(LOST, l.other).unwrap()
            }),
            ("SETSLOT NODE", |l| {
                l.cluster.finalize_slot(LOST, l.cluster.myself()).unwrap()
            }),
            ("ADDSLOTS", |l| {
                l.cluster.release_slot(LOST).unwrap();
                l.cluster.claim_slot(LOST).unwrap();
            }),
            ("ADDSLOTSRANGE", |l| {
                l.cluster.release_slot(LOST).unwrap();
                l.cluster.claim_slot_range(LOST, LOST).unwrap();
            }),
            ("an atomic import's takeover", |l| {
                l.cluster.take_over_slots([LOST].into_iter()).unwrap()
            }),
            ("an atomic import's start", |l| {
                // What `run_import` says before it asks the source to start.
                l.cluster.slot_deletions.came_here([LOST])
            }),
            ("a failover", |l| {
                let _ = l.cluster.promote_self(l.other, 1_000_000);
            }),
        ];
        for (how, come) in comings {
            let lost = Lost::new();
            let new: Vec<String> = (0..300).map(|i| format!("{{{}}}:new:{i}", tag())).collect();
            let mut overwritten = Vec::new();
            let deleted = lost.delete(|| {
                come(&lost);
                for key in &new {
                    set(&lost.store, key, "new");
                }
                for key in (0..OLD).step_by(7).map(old_key) {
                    if lost.value(&key).is_some() {
                        set(&lost.store, &key, "new");
                        overwritten.push(key);
                    }
                }
            });
            assert!(
                deleted.removed > 0 && deleted.removed < OLD && !deleted.stopped,
                "{how}: deleted {} of {OLD}",
                deleted.removed
            );
            assert_eq!(deleted.came_back, 1, "{how}");
            assert!(!overwritten.is_empty());
            for key in new.iter().chain(&overwritten) {
                assert_eq!(
                    lost.value(key).as_deref(),
                    Some(&b"new"[..]),
                    "{how}: {key}, written after the slot came back, was deleted"
                );
            }
            assert_eq!(lost.old_keys_left(), OLD - deleted.removed, "{how}");
            assert!(!lost.cluster.slot_deletions.running([LOST].into_iter()));
        }
    }

    /// A deletion begun while an atomic import of the slot is in progress
    /// here leaves the slot: the import's keys are not the deletion's to
    /// delete, but the import's own, when it does not complete. So does one
    /// begun while the slot is being imported with `SETSLOT IMPORTING`, or
    /// is this node's again.
    #[test]
    fn a_deletion_leaves_a_slot_that_is_here_when_it_begins() {
        let lost = Lost::new();
        lost.cluster
            .slot_migrations
            .add(task("in", vec![(LOST, LOST)]))
            .unwrap();
        assert!(!DeleteRun::begin(&lost.cluster, [LOST], None).deletes_any());
        assert!(DeleteRun::begin(&lost.cluster, [LOST], Some("in")).deletes_any());
        lost.cluster
            .slot_migrations
            .finish("in", TaskState::Failed, None);
        assert!(DeleteRun::begin(&lost.cluster, [LOST], None).deletes_any());

        lost.cluster.set_importing(LOST, lost.other).unwrap();
        assert!(!DeleteRun::begin(&lost.cluster, [LOST], None).deletes_any());
        lost.cluster.clear_migration(LOST);
        lost.cluster
            .finalize_slot(LOST, lost.cluster.myself())
            .unwrap();
        assert!(!DeleteRun::begin(&lost.cluster, [LOST], None).deletes_any());
    }

    /// A deletion under way counts as running for its slots until it ends,
    /// or until the slot comes back: what gives the node a slot looks at
    /// that.
    #[test]
    fn a_deletion_is_running_until_it_ends_or_its_slot_comes_back() {
        let lost = Lost::new();
        let deletions = &lost.cluster.slot_deletions;
        let run = DeleteRun::begin(&lost.cluster, [LOST], None);
        assert!(deletions.running([LOST].into_iter()));
        assert!(!deletions.running([LOST + 1].into_iter()));
        assert_eq!(
            deletions.deleting([LOST - 1, LOST, LOST + 1].into_iter()),
            vec![LOST]
        );
        drop(run);
        assert!(!deletions.running([LOST].into_iter()));

        let lost = Lost::new();
        let deleted = lost.delete(|| {
            assert!(lost.cluster.slot_deletions.running([LOST].into_iter()));
            lost.cluster.set_importing(LOST, lost.other).unwrap();
        });
        assert_eq!(deleted.came_back, 1);
        assert!(!lost.cluster.slot_deletions.running([LOST].into_iter()));
    }

    /// A deletion stops at its next batch when the node has become a
    /// replica since it began, or has been made a master afresh: the keys
    /// it has then are a master's, or were (#348). The keys it has not
    /// deleted yet stay.
    #[test]
    fn a_deletion_stops_when_the_node_becomes_a_replica_part_way() {
        type Change = fn(&ReplState);
        let changes: [(&str, Change); 3] = [
            ("a replica", |r| r.become_slave()),
            ("a replica, then promoted", |r| {
                r.become_slave();
                r.become_master();
            }),
            ("a master afresh", |r| r.become_master()),
        ];
        for (how, change) in changes {
            let lost = Lost::new();
            let deleted = lost.delete(|| change(&lost.repl_state));
            assert!(deleted.stopped, "{how}: it did not stop");
            assert!(
                deleted.removed > 0 && deleted.removed < OLD,
                "{how}: deleted {}",
                deleted.removed
            );
            assert_eq!(lost.old_keys_left(), OLD - deleted.removed, "{how}");
        }
    }

    /// The keys of slots lost to another master are not dropped on a
    /// replica, whose keys are its master's (#348): whether the replication
    /// layer or the cluster's view says it is one. On a master they are.
    #[test]
    fn lost_slots_are_dropped_on_a_master_only() {
        let start = |lost: &Lost| {
            start_dropping(
                &lost.cluster,
                &lost.store,
                &lost.repl_state,
                &lost.hub_tx,
                vec![LOST],
            )
        };
        let lost = Lost::new();
        lost.repl_state.become_slave();
        assert!(start(&lost).is_none());
        assert_eq!(lost.old_keys_left(), OLD);

        let lost = Lost::new();
        let me = lost.cluster.myself();
        lost.cluster.nodes.write().get_mut(&me).unwrap().master_id = Some(lost.other);
        assert!(start(&lost).is_none());
        assert_eq!(lost.old_keys_left(), OLD);

        let lost = Lost::new();
        let deleted = start(&lost).unwrap().join().unwrap();
        assert_eq!((deleted.removed, deleted.stopped), (OLD, false));
        assert_eq!(lost.old_keys_left(), 0);
        assert!(!lost.cluster.slot_deletions.running([LOST].into_iter()));
    }

    /// The keys of an import that did not complete are not deleted on a
    /// replica (#348), whichever says it is one, nor in a slot this node
    /// owns. On a master they are.
    #[test]
    fn an_import_that_did_not_complete_is_deleted_on_a_master_only() {
        let import = task("in", vec![(LOST, LOST)]);
        let lost = Lost::new();
        lost.repl_state.become_slave();
        delete_unowned(&lost.env(), &import);
        assert_eq!(lost.old_keys_left(), OLD);

        let lost = Lost::new();
        let me = lost.cluster.myself();
        lost.cluster.nodes.write().get_mut(&me).unwrap().master_id = Some(lost.other);
        delete_unowned(&lost.env(), &import);
        assert_eq!(lost.old_keys_left(), OLD);

        let lost = Lost::new();
        lost.cluster
            .finalize_slot(LOST, lost.cluster.myself())
            .unwrap();
        delete_unowned(&lost.env(), &import);
        assert_eq!(lost.old_keys_left(), OLD);

        let lost = Lost::new();
        lost.cluster.slot_migrations.add(import.clone()).unwrap();
        delete_unowned(&lost.env(), &import);
        assert_eq!(lost.old_keys_left(), 0);
    }

    /// The slot comes back while a batch of its deletion has picked its
    /// keys and not deleted them yet, as nothing gives it back by a command
    /// (a failover), and a write that locks no key (`write_order`: a
    /// transaction's, a script's, a write of keys of several shards) writes
    /// every old key again. The coming used to go through at once, and the
    /// batch then deleted the write's keys of it (#353). The coming now
    /// waits until the batch has deleted them, and the write comes after:
    /// every key it wrote is kept.
    #[test]
    fn a_slot_that_comes_back_during_a_batch_waits_for_its_deletes() {
        use std::sync::atomic::AtomicBool;
        let lost = Lost::new();
        let came = Arc::new(AtomicBool::new(false));
        let mut writer = None;
        let deleted = lost.delete_held(
            || {
                let (cluster, store) = (Arc::clone(&lost.cluster), Arc::clone(&lost.store));
                let came_there = Arc::clone(&came);
                writer = Some(std::thread::spawn(move || {
                    cluster.slot_deletions.came_here([LOST]);
                    came_there.store(true, Ordering::SeqCst);
                    // Locking no key, as a transaction's commands do.
                    for i in 0..OLD {
                        set(&store, &old_key(i), "new");
                    }
                }));
                // Long enough for a coming that does not wait to have come,
                // and the write to have begun.
                let until = Instant::now() + Duration::from_secs(1);
                while !came.load(Ordering::SeqCst) && Instant::now() < until {
                    std::thread::sleep(Duration::from_millis(5));
                }
                std::thread::sleep(Duration::from_millis(50));
            },
            || {},
        );
        writer.unwrap().join().unwrap();
        // 0 if the deletion was done before the coming got its turn.
        assert!(deleted.came_back <= 1);
        let lost_writes = (0..OLD)
            .filter(|&i| lost.value(&old_key(i)).as_deref() != Some(&b"new"[..]))
            .count();
        assert_eq!(
            lost_writes, 0,
            "writes made once the slot came back were deleted"
        );
    }

    /// A batch holds the counts while it deletes, and what gives this node
    /// a slot waits for them holding the slot table (here `ADDSLOTS` of
    /// another slot). A walk of the keyspace that looked each key's slot up
    /// with the key's shard locked (`CLUSTER SLOT-STATS`) then waited for
    /// the slot table, the batch for the walk's shard, and the giving for
    /// the batch: for good, and the node with them (#353). The walk now
    /// reads the slot table before it locks a shard, and all three end.
    #[test]
    fn a_keyspace_walk_beside_a_held_batch_and_a_slot_given_does_not_deadlock() {
        const OTHER: u16 = 9000;
        let lost = Lost::new();
        lost.cluster.release_slot(OTHER).unwrap();
        let cluster = Arc::clone(&lost.cluster);
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut others = Vec::new();
            let deleted = lost.delete_held(
                || {
                    let cluster = Arc::clone(&lost.cluster);
                    others.push(std::thread::spawn(move || {
                        cluster.claim_slot(OTHER).unwrap();
                    }));
                    // Until the giving holds the slot table, waiting for the
                    // batch.
                    let until = Instant::now() + Duration::from_secs(5);
                    while lost.cluster.slots.try_read().is_some() && Instant::now() < until {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    let (cluster, store) = (Arc::clone(&lost.cluster), Arc::clone(&lost.store));
                    others.push(std::thread::spawn(move || {
                        let counts =
                            crate::commands::cluster::key_counts_per_slot(&store[0], &cluster);
                        assert!(!counts.contains_key(&LOST));
                    }));
                    // Long enough for the walk to have locked a shard.
                    std::thread::sleep(Duration::from_millis(200));
                },
                || {},
            );
            for other in others {
                other.join().unwrap();
            }
            let _ = done_tx.send(deleted.removed);
        });
        let removed = done_rx
            .recv_timeout(Duration::from_secs(20))
            .expect("the batch, the walk and the giving waited for each other");
        assert_eq!(removed, OLD);
        assert!(cluster.owns_slot(OTHER));
    }

    /// What gives this node a slot by a command first deletes, itself, the
    /// keys it still had of it, when a deletion of them is under way: the
    /// slot comes back with none of them (#353). Inside a transaction, which
    /// holds every other command back, it is refused, `TRYAGAIN`, and the
    /// keys stay. With no deletion under way, it deletes nothing; nor on a
    /// replica.
    #[test]
    fn keys_still_being_deleted_go_before_the_slot_comes_back() {
        let lost = Lost::new();
        assert_eq!(
            delete_before_coming(&lost.env(), &[LOST], "ADDSLOTS", None),
            Ok(())
        );
        assert_eq!(lost.old_keys_left(), OLD);

        let under_way = DeleteRun::begin(&lost.cluster, [LOST], None);
        {
            let _transaction = crate::command_gate::enter_alone();
            let refused = delete_before_coming(&lost.env(), &[LOST], "ADDSLOTS", None).unwrap_err();
            assert!(refused.starts_with("TRYAGAIN"), "{refused}");
        }
        assert_eq!(lost.old_keys_left(), OLD);
        assert_eq!(
            delete_before_coming(&lost.env(), &[LOST, LOST + 1], "ADDSLOTS", None),
            Ok(())
        );
        assert_eq!(lost.old_keys_left(), 0);
        drop(under_way);

        let lost = Lost::new();
        let under_way = DeleteRun::begin(&lost.cluster, [LOST], None);
        lost.repl_state.become_slave();
        assert_eq!(
            delete_before_coming(&lost.env(), &[LOST], "ADDSLOTS", None),
            Ok(())
        );
        assert_eq!(lost.old_keys_left(), OLD);
        drop(under_way);

        // An atomic import's own slots are not left out as being imported.
        let lost = Lost::new();
        let under_way = DeleteRun::begin(&lost.cluster, [LOST], None);
        lost.cluster
            .slot_migrations
            .add(task("in", vec![(LOST, LOST)]))
            .unwrap();
        assert_eq!(
            delete_before_coming(&lost.env(), &[LOST], "an atomic import", Some("in")),
            Ok(())
        );
        assert_eq!(lost.old_keys_left(), 0);
        drop(under_way);
    }
}
