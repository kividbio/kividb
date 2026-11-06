// src/replication/mod.rs
//
// Replication subsystem.
//
// The ReplicationHub receives each propagated command on an **unbounded** inbox
// (propagate is sync from execute_command — must not drop commits). Each channel-backed
// replica uses an **unbounded** hub→replica queue so backlog can accumulate while FULLRESYNC
// runs (bounded queues filled before the forwarding task drained them, falsely showing
// connected_slaves:0 via eviction). Disconnect only when `send` fails (receiver gone).

use parking_lot::RwLock;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicU32, AtomicU64, Ordering};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc;

pub mod digest;
pub mod replica_client; // Step 4 lives here
pub mod script_feed;

// ── Types ──────────────────────────────────────────────────────────────────

/// One command propagated to replicas: raw RESP wire bytes.
pub type ReplPayload = Arc<[u8]>;

/// The sender half — cloned once into the command context.
pub type ReplTx = mpsc::UnboundedSender<ReplPayload>;

/// Replication metadata shared between the hub and the INFO command.
#[derive(Debug)]
pub struct ReplState {
    /// "master" or "slave"
    pub role: RwLock<String>,
    /// Lock-free mirror of whether [`Self::role`] is `"slave"` — updated only via
    /// [`Self::become_master`] / [`Self::become_slave`] alongside the string.
    slave_mode: AtomicBool,
    /// Monotonically increasing byte offset of the replication stream.
    pub repl_offset: AtomicU64,
    /// Number of currently connected replicas.
    pub connected_replicas: AtomicI64,
    /// The dataset's fingerprint when this node started as a master, at
    /// offset 0 of its stream: see [`Self::fingerprint_at_start`].
    pub startup_digest: parking_lot::Mutex<Option<u64>>,
    /// Master replication ID (40 hex chars). Generated at startup, or kept
    /// from before a clean restart (see [`Self::resume_as_master`]); a new
    /// one whenever this node becomes a replica. Read with [`Self::repl_id`].
    repl_id: RwLock<String>,
    pub replica_acks: parking_lot::Mutex<std::collections::HashMap<u64, u64>>, // NEW
    /// `(replica_id, ip, port)` in registration order — for INFO `slaveN:` lines.
    pub replica_peers: parking_lot::Mutex<Vec<(u64, String, u16)>>,
    /// True while this replica has an active TCP replication session to its master (PSYNC/stream).
    /// Used only when `role` is `"slave"`; INFO reports `master_link_status` from this.
    pub slave_link_up: AtomicBool,
    /// Replica-only: KV / aux structures are swapped in from staging during bulk full-sync (RDB).
    /// Mirrors Redis-style `LOADING`; INFO sets `master_sync_in_progress`.
    pub slave_sync_in_progress: AtomicBool,
    /// Last logical DB index written to the replication backlog (`usize::MAX` = never emitted).
    /// Drives implicit `SELECT` lines so replicas stay aligned with per-client `selected_db`.
    pub repl_stream_last_db: AtomicU64,
    /// Set, and never cleared, once a client has selected a database other
    /// than 0. From then on every write travels with its own `SELECT`; see
    /// [`ReplState::note_database_selected`].
    pub several_databases: AtomicBool,
    /// Whether a `FT.DROPINDEX … DD` this replica's master sends deleted the
    /// documents on the master. Set at each connection to a master, from the
    /// release it says it is (`INFO server`); see [`dd_deleted_by`].
    pub master_dd_deletes: AtomicBool,
    /// Whether this replica's master sends a `DEL` (or `UNLINK`) for every
    /// key it expires, as KiviDB does since 1.1.3 and Redis always has. Then
    /// the replica deletes no expired key itself and waits for that `DEL`
    /// (see `expiry`). A master that does not (KiviDB 1.1.2 and before, and
    /// 1.2.0-rc1 and -rc2) leaves its replica to expire keys by its own clock,
    /// as replicas did before 1.1.3. Set at each connection to a master, from
    /// its `INFO server` ([`expiry_propagated_by`]).
    pub master_propagates_expiry: AtomicBool,
    /// Whether this node's dataset may hold keys past their time that no
    /// master will send a `DEL` of: it was loaded at startup (it may have
    /// been written while following a master that sent none), or it has
    /// followed such a master since. Cleared once a master that sends them
    /// has either replaced the dataset (a full sync) or swept it
    /// (`commands::helpers::expire_keys_due_before`, at a partial resync).
    pub expiry_sweep_owed: AtomicBool,
    /// How many taps are attached (see `taps`). Read on every write, so that
    /// the common case — none — costs one relaxed load.
    pub stream_taps: std::sync::atomic::AtomicUsize,
    /// Consumers of the write stream that are not replicas — a backup
    /// collecting its incremental file. Each receives every propagated write
    /// from the moment it is attached, in order. They are fed directly by the
    /// writer, not through the hub: a tap has to be attachable and detachable
    /// at an exact point in the stream by a thread that cannot wait for the
    /// hub task to be scheduled.
    pub taps: RwLock<Vec<(u64, std::sync::mpsc::Sender<ReplPayload>)>>,
    /// Monotonic replication epoch. Bumped every time this node's replication
    /// *intent* changes — a new `REPLICAOF <host> <port>` (via
    /// [`Self::begin_replication_epoch`], called by `start_replica` itself) or a
    /// promotion to master (via [`Self::become_master`]).
    ///
    /// Each spawned `start_replica` task remembers the epoch it claimed and exits
    /// as soon as it observes a different one. Without this, re-pointing a replica
    /// at a *different* master while it stays `"slave"` throughout — the common
    /// case during failover — left the previous task running forever: its only exit
    /// condition was the role changing *away from* `"slave"`, which never happens.
    /// The stale task kept applying its old master's writes and the new one never
    /// took effect, so a replica whose master died could never be re-pointed.
    repl_generation: AtomicU64,
    /// Held by a replication session from its last look at the epoch until
    /// the master's dataset is in place, and by every epoch change. A
    /// promotion that arrives while a full sync is replacing the dataset
    /// waits for it to finish, rather than racing it; and a session that was
    /// superseded before it got here leaves the dataset alone. Without it a
    /// node promoted while a payload was still arriving threw its own data
    /// away and loaded the old master's.
    pub dataset_commit: parking_lot::Mutex<()>,
    /// Live replication target — `Some((host, port))` while this node is following
    /// a master, `None` once promoted. Authoritative for `INFO replication`'s
    /// `master_host`/`master_port` and `ROLE`.
    ///
    /// Before this existed both read `config.replicaof_addr()`, i.e. the
    /// *startup* `--replicaof` value, which `REPLICAOF` never updates. A replica
    /// re-pointed at a new master therefore kept reporting the old host forever,
    /// independently of whether the re-point itself worked — the control plane
    /// read that as "still attached to the dead master" and re-issued REPLICAOF
    /// on a loop.
    master_addr: RwLock<Option<(String, u16)>>,
    /// Master side: exact per-replica stream accounting, keyed by replica id.
    /// See [`ReplicaProgress`] for why the global `repl_offset` is not enough.
    pub replica_progress: parking_lot::Mutex<std::collections::HashMap<u64, Arc<ReplicaProgress>>>,
    /// Replica side: where the link to the master is in its lifecycle
    /// ([`LinkState`] as a `u8`). `ROLE` reports this; `slave_link_up` is the
    /// "is it `Connected`" projection the hot paths read.
    slave_link_state: AtomicU8,
    /// Replica side: Unix ms of the last byte received from the master, 0 if
    /// none yet in this process. `INFO`'s `master_last_io_seconds_ago` derives
    /// from this, so it measures real I/O instead of always reading 0.
    pub master_last_io_ms: AtomicU64,
    /// Replica side: Unix ms at which the link last went down, 0 while it is up.
    pub master_link_down_since_ms: AtomicU64,
    /// Replica side: Unix ms of the last moment this replica was streaming
    /// from its master with everything it had been sent applied; 0 if it has
    /// not been since it started or since its last full sync began. Its
    /// data's age, for the cluster's election, counts from here, not from
    /// the last time the link came up: a replica whose full sync starts over
    /// and over has its link come up after every load, and was never caught
    /// up (see `cluster::bus::heartbeat::stand_refusal`).
    pub in_step_ms: AtomicU64,
    /// Replica side: a full sync has begun to replace this node's dataset,
    /// and the replica has not streamed since it loaded it. Until then it
    /// may hold part of the master's dataset, or none of it.
    pub full_sync_unfinished: AtomicBool,
    /// Replica side: full resyncs refused by the dataset guard, and why the
    /// most recent one was refused. See `replica_client::guard_full_sync`.
    pub full_syncs_refused: AtomicU64,
    pub last_sync_refusal: RwLock<Option<String>>,
    /// Runtime-settable replication knobs (`CONFIG SET`), seeded from `Config`.
    /// Kept here rather than in process-wide statics because the test harness
    /// runs several servers in one process and they need independent values.
    pub repl_timeout_secs: AtomicU64,
    pub repl_ping_period_secs: AtomicU64,
    /// `shutdown-timeout`: how long a master that is shutting down waits for
    /// its replicas to acknowledge the end of its stream.
    pub shutdown_timeout_secs: AtomicU64,
    /// `replica-priority` and `replica-announced`: what `INFO replication`
    /// tells Sentinel of this replica (`slave_priority`, where 0 is never
    /// promoted, and `replica_announced`).
    pub replica_priority: AtomicU32,
    pub replica_announced: AtomicBool,
    /// Set by a shutdown once no command runs: nothing more goes into the
    /// write stream, not even the replica `PING` or a key that expires. See
    /// [`end_stream_for_shutdown`].
    pub stream_closed: AtomicBool,
    /// `hll-write-format`: whether new HyperLogLog values use Redis's dense
    /// encoding and hash space, or the legacy KiviDB one. Per server rather
    /// than process-wide: the test harness runs several servers in one
    /// process, and an AOF-replaying server must have it before replay starts.
    hll_write_redis: AtomicBool,
    /// Redis `min-replicas-to-write`: a master refuses writes while fewer
    /// replicas than this are good (see `min_replicas_max_lag`). 0 is off.
    pub min_replicas_to_write: AtomicU32,
    /// Redis `min-replicas-max-lag`: a replica is good while it is online
    /// and its last acknowledgement is at most this many seconds old. 0
    /// turns `min_replicas_to_write` off, as in Redis.
    pub min_replicas_max_lag: AtomicU64,
    full_sync_guard: AtomicU8,
    pub full_sync_guard_ratio: AtomicU64,
    /// `(masteruser, masterauth)` — what this node authenticates with when it
    /// connects to a master as a replica.
    master_credentials: RwLock<(String, String)>,
    /// Master side: the hub is keeping a backlog of the write stream, so every
    /// write has to reach it — replicas connected or not. Set at the cut of
    /// the first full sync; cleared by the hub once no replica has been
    /// attached for `repl-backlog-ttl`. See [`Backlog`].
    pub backlog_active: AtomicBool,
    /// `repl-backlog-size`, in bytes; and `repl-backlog-ttl`, in seconds
    /// (0 = keep the backlog for ever).
    pub backlog_size: AtomicU64,
    pub backlog_ttl_secs: AtomicU64,
    /// `repl-replica-max-backlog-bytes`: the most the master queues for a
    /// replica that is streaming before it drops it; and
    /// `repl-replica-sync-max-backlog-bytes`, the most it queues for one
    /// during its full sync (#305). See [`start_hub`].
    pub replica_max_backlog_bytes: AtomicU64,
    pub replica_sync_max_backlog_bytes: AtomicU64,
    /// Master side: what `INFO` reports about the backlog. Written by the hub.
    pub backlog_first_offset: AtomicU64,
    pub backlog_histlen: AtomicU64,
    /// Master side: `sync_full`, `sync_partial_ok`, `sync_partial_err`.
    pub sync_full: AtomicU64,
    pub sync_partial_ok: AtomicU64,
    pub sync_partial_err: AtomicU64,
    /// Replica side: where in whose stream this node's dataset stands — what
    /// it asks its master to continue from. See [`ResyncPoint`].
    pub resync_point: parking_lot::Mutex<Option<ResyncPoint>>,
    /// Mirror of `resync_point.is_some()`, for the write path: a write made
    /// here by a client takes the dataset off the stream it was following.
    pub has_resync_point: AtomicBool,
    /// Replica side: held while a batch of the master's commands is applied
    /// and the resync point moved past them, and by a save that records the
    /// point beside the dataset. Holding it means: the dataset is exactly the
    /// stream up to `resync_point`, no more and no less.
    pub apply_lock: parking_lot::Mutex<()>,
    /// Replica side: drop the local dataset before loading a full sync,
    /// instead of holding both until the new one is in (`repl-diskless-load`).
    pub flush_before_load: AtomicBool,
    /// Master side: the stream this node followed as a replica until it was
    /// promoted, and the first offset past what it had read of it (Redis's
    /// `master_replid2` and `second_repl_offset`). The new master's stream
    /// carries on from there, so a replica that followed the old one is
    /// continued instead of resynced in full (#109).
    pub repl_id2: RwLock<Option<(String, u64)>>,
    /// The hub, for what is not a client's write: a replica's record of the
    /// stream it reads, and a promotion's restart of the backlog. Weak, so
    /// that the hub still ends when every real sender is gone.
    hub: std::sync::OnceLock<mpsc::WeakUnboundedSender<HubMsg>>,
    /// Replica side, in a cluster: the node whose stream `resync_point` is
    /// a position in — the master this node followed when its dataset last
    /// became a copy of a master's. `None` when not known.
    stream_origin: RwLock<Option<String>>,
    /// What the replication client needs to ask of the cluster layer; see
    /// [`ClusterLink`]. Not set outside cluster mode.
    cluster_link: std::sync::OnceLock<ClusterLink>,
}

/// The replication client's view of the cluster, installed by
/// `ClusterState::start`. The client runs on a thread of its own and knows
/// its master only by address; these answer by node id.
pub struct ClusterLink {
    /// The node id of the master this node follows, in the cluster's view.
    pub master_node: MasterNodeFn,
    /// Have this replica take over from master `node` (a node id), because
    /// `why`: an election that does not wait for the master to be failed.
    /// False when it cannot (it is not that master's replica).
    pub take_over: TakeOverFn,
}

/// See [`ClusterLink::master_node`].
pub type MasterNodeFn = Box<dyn Fn() -> Option<String> + Send + Sync>;
/// See [`ClusterLink::take_over`].
pub type TakeOverFn = Box<dyn Fn(&str, &str) -> bool + Send + Sync>;

impl std::fmt::Debug for ClusterLink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ClusterLink")
    }
}

/// A position in a master's write stream, and the dataset that corresponds to
/// it: "this node holds exactly what `repl_id`'s stream produced up to byte
/// `offset`, and the stream's next command is for database `db`".
///
/// It is what makes a partial resync possible. A replica that loses its
/// connection — or is restarted cleanly — asks the master for the stream
/// from `offset` on, and if the master still has that in its backlog, only
/// the missing part is sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResyncPoint {
    pub repl_id: String,
    pub offset: u64,
    pub db: usize,
}

/// `PSYNC <replid> <offset>` as a replica sent it. `offset` is the first byte
/// it wants, one past the last it has.
#[derive(Debug, Clone)]
pub struct PsyncRequest {
    pub repl_id: String,
    pub offset: u64,
}

impl PsyncRequest {
    /// From the arguments of a `PSYNC` command. `None` for `PSYNC ? -1`, or
    /// anything else that cannot be continued from.
    pub fn parse(cmd: &[&[u8]]) -> Option<Self> {
        let repl_id = std::str::from_utf8(cmd.get(1)?).ok()?;
        let offset: u64 = std::str::from_utf8(cmd.get(2)?).ok()?.parse().ok()?;
        (repl_id.len() == 40 && offset > 0).then(|| Self {
            repl_id: repl_id.to_string(),
            offset,
        })
    }
}

/// The state of a replica as its master sees it — `state=` in the master's
/// `INFO replication`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ReplicaState {
    /// Registered; the master is preparing the dataset for it.
    WaitBgsave = 0,
    /// The dataset is being sent.
    SendBulk = 1,
    /// It has the dataset loaded and is following the stream. Reached when it
    /// first acknowledges — which a replica does only once it has loaded.
    Online = 2,
}

impl ReplicaState {
    pub fn as_str(self) -> &'static str {
        match self {
            ReplicaState::WaitBgsave => "wait_bgsave",
            ReplicaState::SendBulk => "send_bulk",
            ReplicaState::Online => "online",
        }
    }
}

/// Where a replica's link to its master stands. The names are the ones Redis
/// uses for the fourth element of the replica's `ROLE` reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LinkState {
    /// Not connected; a connection attempt is due.
    Connect = 0,
    /// TCP connect or handshake in progress.
    Connecting = 1,
    /// Receiving / loading the full-sync payload.
    Sync = 2,
    /// Synced and streaming.
    Connected = 3,
}

impl LinkState {
    pub fn as_str(self) -> &'static str {
        match self {
            LinkState::Connect => "connect",
            LinkState::Connecting => "connecting",
            LinkState::Sync => "sync",
            LinkState::Connected => "connected",
        }
    }
}

/// What a replica does when a full resync would shrink its dataset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FullSyncGuard {
    Off = 0,
    Empty = 1,
    Shrink = 2,
}

impl FullSyncGuard {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "off" | "no" => Some(Self::Off),
            "empty" => Some(Self::Empty),
            "shrink" => Some(Self::Shrink),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Empty => "empty",
            Self::Shrink => "shrink",
        }
    }
}

/// One connected replica's position in the replication stream, as the master
/// sees it.
///
/// The global `repl_offset` cannot answer "has this replica received
/// everything?". A replica's ACKs count from the offset its `+FULLRESYNC`
/// announced, but that offset is read before the replica is registered with
/// the hub, so any write landing in between advances `repl_offset` without
/// ever being sent to that replica. Its ACK then trails the global offset by
/// that amount forever: `WAIT` never counted it and its lag never reached
/// zero, however caught up it actually was.
///
/// The hub is the only place that knows exactly which bytes each replica was
/// handed, so it keeps the count here and every "is it caught up" question is
/// asked per replica: bytes delivered (`acked - base`) against bytes `sent`.
#[derive(Debug)]
pub struct ReplicaProgress {
    /// The offset announced in this replica's `+FULLRESYNC`.
    pub base: u64,
    /// Stream bytes queued for this replica since it registered.
    pub sent: AtomicU64,
    /// Its latest `REPLCONF ACK` value (absolute: `base` + bytes it received).
    pub acked: AtomicU64,
    /// Unix ms of the latest ACK; registration time until the first arrives.
    pub last_ack_ms: AtomicU64,
    /// Where the replica is between asking for a sync and following the
    /// stream ([`ReplicaState`] as a `u8`).
    state: AtomicU8,
    /// The bytes queued for it and not yet taken by its forwarder, shared
    /// with the hub (`HubMsg::RegisterChannel::pending_bytes`). Set once the
    /// hub feeds it.
    queue: std::sync::OnceLock<Arc<AtomicU64>>,
}

impl ReplicaProgress {
    pub fn new(base: u64) -> Self {
        Self {
            base,
            sent: AtomicU64::new(0),
            acked: AtomicU64::new(base),
            last_ack_ms: AtomicU64::new(unix_ms()),
            state: AtomicU8::new(ReplicaState::WaitBgsave as u8),
            queue: std::sync::OnceLock::new(),
        }
    }

    /// Bytes queued for this replica that its connection has not taken yet:
    /// `queued` in `INFO replication`.
    pub fn queued(&self) -> u64 {
        self.queue
            .get()
            .map_or(0, |queue| queue.load(Ordering::Relaxed))
    }

    pub fn state(&self) -> ReplicaState {
        match self.state.load(Ordering::Relaxed) {
            0 => ReplicaState::WaitBgsave,
            1 => ReplicaState::SendBulk,
            _ => ReplicaState::Online,
        }
    }

    pub fn set_state(&self, state: ReplicaState) {
        self.state.store(state as u8, Ordering::Relaxed);
    }

    /// Stream bytes this replica has acknowledged receiving.
    pub fn delivered(&self) -> u64 {
        self.acked.load(Ordering::Relaxed).saturating_sub(self.base)
    }

    /// Bytes sent but not yet acknowledged.
    pub fn behind_bytes(&self) -> u64 {
        self.sent
            .load(Ordering::Relaxed)
            .saturating_sub(self.delivered())
    }

    /// Whole seconds since the last ACK — Redis's `lag` in `INFO replication`.
    pub fn lag_secs(&self) -> u64 {
        unix_ms().saturating_sub(self.last_ack_ms.load(Ordering::Relaxed)) / 1000
    }
}

/// Wall-clock milliseconds since the Unix epoch.
pub fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl ReplState {
    pub fn new_master() -> Self {
        Self {
            role: RwLock::new("master".into()),
            slave_mode: AtomicBool::new(false),
            repl_offset: AtomicU64::new(0),
            connected_replicas: AtomicI64::new(0),
            repl_id: RwLock::new(generate_repl_id()),
            startup_digest: parking_lot::Mutex::new(None),
            replica_acks: parking_lot::Mutex::new(std::collections::HashMap::new()), // NEW
            replica_peers: parking_lot::Mutex::new(Vec::new()),
            slave_link_up: AtomicBool::new(false),
            slave_sync_in_progress: AtomicBool::new(false),
            repl_stream_last_db: AtomicU64::new(u64::MAX),
            several_databases: AtomicBool::new(false),
            master_dd_deletes: AtomicBool::new(false),
            master_propagates_expiry: AtomicBool::new(true),
            expiry_sweep_owed: AtomicBool::new(true),
            stream_taps: std::sync::atomic::AtomicUsize::new(0),
            taps: RwLock::new(Vec::new()),
            repl_generation: AtomicU64::new(0),
            dataset_commit: parking_lot::Mutex::new(()),
            master_addr: RwLock::new(None),
            replica_progress: parking_lot::Mutex::new(std::collections::HashMap::new()),
            slave_link_state: AtomicU8::new(LinkState::Connect as u8),
            master_last_io_ms: AtomicU64::new(0),
            master_link_down_since_ms: AtomicU64::new(0),
            in_step_ms: AtomicU64::new(0),
            full_sync_unfinished: AtomicBool::new(false),
            full_syncs_refused: AtomicU64::new(0),
            last_sync_refusal: RwLock::new(None),
            repl_timeout_secs: AtomicU64::new(60),
            repl_ping_period_secs: AtomicU64::new(10),
            shutdown_timeout_secs: AtomicU64::new(10),
            replica_priority: AtomicU32::new(100),
            replica_announced: AtomicBool::new(true),
            stream_closed: AtomicBool::new(false),
            hll_write_redis: AtomicBool::new(false),
            min_replicas_to_write: AtomicU32::new(0),
            min_replicas_max_lag: AtomicU64::new(10),
            full_sync_guard: AtomicU8::new(FullSyncGuard::Empty as u8),
            full_sync_guard_ratio: AtomicU64::new(50),
            master_credentials: RwLock::new((String::new(), String::new())),
            backlog_active: AtomicBool::new(false),
            backlog_size: AtomicU64::new(64 * 1024 * 1024),
            backlog_ttl_secs: AtomicU64::new(3600),
            replica_max_backlog_bytes: AtomicU64::new(256 * 1024 * 1024),
            replica_sync_max_backlog_bytes: AtomicU64::new(1024 * 1024 * 1024),
            backlog_first_offset: AtomicU64::new(0),
            backlog_histlen: AtomicU64::new(0),
            sync_full: AtomicU64::new(0),
            sync_partial_ok: AtomicU64::new(0),
            sync_partial_err: AtomicU64::new(0),
            resync_point: parking_lot::Mutex::new(None),
            has_resync_point: AtomicBool::new(false),
            apply_lock: parking_lot::Mutex::new(()),
            flush_before_load: AtomicBool::new(true),
            repl_id2: RwLock::new(None),
            hub: std::sync::OnceLock::new(),
            stream_origin: RwLock::new(None),
            cluster_link: std::sync::OnceLock::new(),
        }
    }

    /// Give the replication client its view of the cluster (once).
    pub fn install_cluster_link(&self, link: ClusterLink) {
        let _ = self.cluster_link.set(link);
    }

    /// The node id of the master this node follows, if this is a cluster
    /// replica.
    pub fn cluster_master_node(&self) -> Option<String> {
        self.cluster_link.get().and_then(|l| (l.master_node)())
    }

    /// Ask the cluster layer to have this replica take over from `node`.
    pub fn request_take_over(&self, node: &str, why: &str) -> bool {
        self.cluster_link
            .get()
            .is_some_and(|l| (l.take_over)(node, why))
    }

    /// The node whose stream this replica's dataset is from, if known.
    pub fn stream_origin(&self) -> Option<String> {
        self.stream_origin.read().clone()
    }

    pub fn set_stream_origin(&self, node: Option<String>) {
        *self.stream_origin.write() = node;
    }

    /// A replica that started on a saved resync point: that point is a
    /// position in the stream of the master its cluster config names.
    pub fn note_stream_origin_if_unknown(&self, node: Option<String>) {
        if self.resync_point.lock().is_none() {
            return;
        }
        let mut origin = self.stream_origin.write();
        if origin.is_none() {
            *origin = node;
        }
    }

    /// Whether a write on this master has to be sent to the hub: a replica is
    /// attached, or the backlog is being kept for one that may come back.
    #[inline]
    pub fn feeds_hub(&self) -> bool {
        self.connected_replicas.load(Ordering::Relaxed) > 0
            || self.backlog_active.load(Ordering::Relaxed)
    }

    /// Record where this node's dataset stands in its master's stream.
    pub fn set_resync_point(&self, point: Option<ResyncPoint>) {
        let mut slot = self.resync_point.lock();
        self.has_resync_point
            .store(point.is_some(), Ordering::Relaxed);
        *slot = point;
    }

    /// A client wrote to this node directly. Whatever stream its dataset was
    /// a position in, it is not one any more.
    #[inline]
    pub fn note_local_write(&self) {
        if self.has_resync_point.load(Ordering::Relaxed) {
            self.set_resync_point(None);
        }
    }

    /// The replica's row for `replica_id`, if it is registered.
    pub fn replica(&self, replica_id: u64) -> Option<Arc<ReplicaProgress>> {
        self.replica_progress.lock().get(&replica_id).cloned()
    }

    /// Attach a tap to the write stream. Call with the write gate closed
    /// (`write_gate::cut`), so that it starts between two whole writes.
    pub fn attach_tap(&self, id: u64, tx: std::sync::mpsc::Sender<ReplPayload>) {
        let mut taps = self.taps.write();
        taps.push((id, tx));
        self.stream_taps.store(taps.len(), Ordering::Relaxed);
        // The first write a new consumer sees must say which database it is
        // for.
        self.repl_stream_last_db.store(u64::MAX, Ordering::Relaxed);
    }

    /// Detach a tap; its receiver then sees the end of the stream. Call with
    /// the write gate closed, so that it ends between two whole writes.
    pub fn detach_tap(&self, id: u64) {
        let mut taps = self.taps.write();
        taps.retain(|(tap, _)| *tap != id);
        self.stream_taps.store(taps.len(), Ordering::Relaxed);
    }

    /// A client is switching to database `db`.
    ///
    /// The write stream says which database a write belongs to with a
    /// `SELECT` in front of it, sent only when the database changes. That is
    /// exact while one client at a time writes. With two — one in db 0, one in
    /// db 3 — the check "has the database changed?" and the two sends are not
    /// one step: the db-0 client's write can be queued behind the other's
    /// `SELECT 3`, and a replica (or an AOF replay, or a restore) then applies
    /// it to db 3. Keys end up in the wrong database on the other side.
    ///
    /// So from the first time any client leaves db 0, every write carries its
    /// own `SELECT` in the same buffer, and nothing can come between them. A
    /// server that only ever uses db 0 — most of them, and every cluster node
    /// — pays nothing: no `SELECT`s, and one atomic load to know it.
    ///
    /// The switch happens with the write gate closed, so that no write is
    /// already on its way without a `SELECT` when the first one with a
    /// different database is sent. A client's `SELECT` is a write, and in
    /// the gate when it runs, where a cut is none: it makes the switch
    /// before it enters the gate (`handler::first_select_of_another_database`,
    /// #423), and this, when it runs, finds it made.
    pub fn note_database_selected(&self, db: usize) {
        if db == 0 || self.several_databases.load(Ordering::Relaxed) {
            return;
        }
        // `None` inside a transaction or a script that holds the command
        // gate alone, so that no other command's write is in flight; the
        // flag is set all the same. A script beside others may not make the
        // switch (`handler::run_from_script`).
        let _cut = crate::write_gate::cut();
        self.several_databases.store(true, Ordering::Relaxed);
    }

    /// `parts` encoded for the write stream, in database `db`: with the
    /// `SELECT` the stream needs before it, if it needs one.
    ///
    /// Usually one buffer. Two — a `SELECT`, then the command — only while
    /// the server has never used a database other than 0, where the one
    /// `SELECT 0` that opens the stream cannot be mistaken for another's.
    fn encode_for_stream(&self, db: usize, parts: &[Vec<u8>]) -> Vec<Vec<u8>> {
        let select = || {
            crate::resp::write::encode_resp_command(&[
                b"SELECT".to_vec(),
                db.to_string().into_bytes(),
            ])
        };
        let command = crate::resp::write::encode_resp_command(parts);
        if self.several_databases.load(Ordering::Relaxed) {
            let mut both = select();
            both.extend_from_slice(&command);
            vec![both]
        } else if self.repl_stream_last_db.load(Ordering::Relaxed) != db as u64 {
            vec![select(), command]
        } else {
            test_delay_of_a_bare_write(parts);
            vec![command]
        }
    }

    /// Hand one encoded write to every tap.
    fn feed_taps(&self, payload: &[u8]) {
        let taps = self.taps.read();
        if taps.is_empty() {
            return;
        }
        let payload: ReplPayload = Arc::from(payload);
        for (_, tx) in taps.iter() {
            // A tap whose receiver has gone is detached by its owner.
            let _ = tx.send(Arc::clone(&payload));
        }
    }

    /// `(masteruser, masterauth)`. An empty password means "do not AUTH".
    pub fn master_credentials(&self) -> (String, String) {
        self.master_credentials.read().clone()
    }

    pub fn set_master_user(&self, user: &str) {
        self.master_credentials.write().0 = user.to_string();
    }

    pub fn set_master_auth(&self, password: &str) {
        self.master_credentials.write().1 = password.to_string();
    }

    /// A master-role state carrying the replication settings from `config`.
    pub fn from_config(config: &crate::config::Config) -> Self {
        let state = Self::new_master();
        state
            .repl_timeout_secs
            .store(config.repl_timeout.max(1), Ordering::Relaxed);
        state
            .repl_ping_period_secs
            .store(config.repl_ping_replica_period, Ordering::Relaxed);
        state
            .shutdown_timeout_secs
            .store(config.shutdown_timeout, Ordering::Relaxed);
        state
            .replica_priority
            .store(config.replica_priority, Ordering::Relaxed);
        state
            .replica_announced
            .store(config.replica_announced, Ordering::Relaxed);
        state
            .min_replicas_to_write
            .store(config.min_replicas_to_write, Ordering::Relaxed);
        state
            .min_replicas_max_lag
            .store(config.min_replicas_max_lag, Ordering::Relaxed);
        state.set_full_sync_guard(
            FullSyncGuard::parse(&config.replica_full_sync_guard).unwrap_or(FullSyncGuard::Empty),
        );
        state.full_sync_guard_ratio.store(
            config.replica_full_sync_guard_ratio.min(100),
            Ordering::Relaxed,
        );
        state.set_master_user(&config.masteruser);
        state.set_master_auth(&config.masterauth);
        state
            .backlog_size
            .store(config.repl_backlog_size, Ordering::Relaxed);
        state
            .backlog_ttl_secs
            .store(config.repl_backlog_ttl, Ordering::Relaxed);
        state
            .replica_max_backlog_bytes
            .store(config.repl_replica_max_backlog_bytes, Ordering::Relaxed);
        state.replica_sync_max_backlog_bytes.store(
            config.repl_replica_sync_max_backlog_bytes,
            Ordering::Relaxed,
        );
        state.hll_write_redis.store(
            config.hll_write_format.eq_ignore_ascii_case("redis"),
            Ordering::Relaxed,
        );
        state.flush_before_load.store(
            flush_before_load(&config.repl_diskless_load).unwrap_or(true),
            Ordering::Relaxed,
        );
        state
    }

    pub fn hll_write_redis(&self) -> bool {
        self.hll_write_redis.load(Ordering::Relaxed)
    }

    pub fn set_hll_write_redis(&self, redis: bool) {
        self.hll_write_redis.store(redis, Ordering::Relaxed);
    }

    pub fn full_sync_guard(&self) -> FullSyncGuard {
        match self.full_sync_guard.load(Ordering::Relaxed) {
            0 => FullSyncGuard::Off,
            2 => FullSyncGuard::Shrink,
            _ => FullSyncGuard::Empty,
        }
    }

    pub fn set_full_sync_guard(&self, guard: FullSyncGuard) {
        self.full_sync_guard.store(guard as u8, Ordering::Relaxed);
    }

    /// How long a replica waits on a silent master before giving up on it,
    /// and a master on a silent replica.
    pub fn repl_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.repl_timeout_secs.load(Ordering::Relaxed).max(1))
    }

    /// Whether replica `replica_id` is online and has sent no `REPLCONF ACK`
    /// for longer than `repl-timeout`.
    fn replica_is_silent(&self, replica_id: u64) -> bool {
        let timeout_ms = self.repl_timeout().as_millis() as u64;
        self.replica_progress
            .lock()
            .get(&replica_id)
            .is_some_and(|p| {
                p.state() == ReplicaState::Online
                    && unix_ms().saturating_sub(p.last_ack_ms.load(Ordering::Relaxed)) > timeout_ms
            })
    }

    /// Resolves once replica `replica_id` has gone silent (see
    /// [`Self::replica_is_silent`]); its connection is then to be closed.
    ///
    /// A replica sends an ACK every second. One whose host is gone without
    /// closing the connection (a terminated VM, a lost network path, a paused
    /// container) sends none, and nothing else ended its connection: the
    /// socket stays open while the peer's kernel answers, and when it does
    /// not, for the kernel's retransmission limit, a quarter of an hour. It
    /// was listed `online` with its lag growing, and counted in
    /// `connected_slaves`, until the master restarted. Redis drops it after
    /// `repl-timeout`, as this does. Only once online: a replica sends no ACK
    /// while it loads the dataset.
    pub async fn replica_timed_out(&self, replica_id: u64) {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            if self.replica_is_silent(replica_id) {
                return;
            }
        }
    }

    pub fn link_state(&self) -> LinkState {
        match self.slave_link_state.load(Ordering::Acquire) {
            1 => LinkState::Connecting,
            2 => LinkState::Sync,
            3 => LinkState::Connected,
            _ => LinkState::Connect,
        }
    }

    /// Move the replica link to `state`, keeping `slave_link_up` and the
    /// down-since timestamp consistent with it.
    ///
    /// "Up" means synced and streaming, as it does in Redis — not "a TCP
    /// connection exists". A link that is still handshaking or loading the
    /// full-sync payload reports down, with `master_sync_in_progress` saying
    /// why.
    pub fn set_link_state(&self, state: LinkState) {
        let was_up = self.slave_link_up.load(Ordering::Acquire);
        let now_up = state == LinkState::Connected;
        self.slave_link_state.store(state as u8, Ordering::Release);
        self.slave_link_up.store(now_up, Ordering::Release);
        if now_up {
            self.master_link_down_since_ms.store(0, Ordering::Relaxed);
        } else if was_up || self.master_link_down_since_ms.load(Ordering::Relaxed) == 0 {
            self.master_link_down_since_ms
                .store(unix_ms(), Ordering::Relaxed);
        }
    }

    /// Replica side: a full sync from the master has begun. Until the
    /// replica has loaded it and streamed since, it does not stand for
    /// election ([`Self::full_sync_unfinished`]).
    pub fn note_full_sync_started(&self) {
        self.full_sync_unfinished.store(true, Ordering::SeqCst);
        self.in_step_ms.store(0, Ordering::SeqCst);
    }

    /// Replica side: streaming, with everything the master has sent so far
    /// applied ([`Self::in_step_ms`]).
    #[inline]
    pub fn note_in_step(&self) {
        self.in_step_ms.store(unix_ms(), Ordering::Relaxed);
        if self.full_sync_unfinished.load(Ordering::Relaxed) {
            self.full_sync_unfinished.store(false, Ordering::SeqCst);
        }
    }

    /// Record that bytes just arrived from the master.
    #[inline]
    pub fn note_master_io(&self) {
        self.master_last_io_ms.store(unix_ms(), Ordering::Relaxed);
    }

    /// Seconds since the last byte from the master, or `-1` while the link is
    /// down (Redis reports `-1` there too: a stale timestamp says nothing
    /// useful about a link that no longer exists).
    pub fn master_last_io_seconds_ago(&self) -> i64 {
        if !self.slave_link_up.load(Ordering::Acquire) {
            return -1;
        }
        match self.master_last_io_ms.load(Ordering::Relaxed) {
            0 => -1,
            at => (unix_ms().saturating_sub(at) / 1000) as i64,
        }
    }

    /// Seconds the link has been down, or `None` while it is up.
    pub fn master_link_down_since_seconds(&self) -> Option<u64> {
        if self.slave_link_up.load(Ordering::Acquire) {
            return None;
        }
        match self.master_link_down_since_ms.load(Ordering::Relaxed) {
            0 => Some(0),
            at => Some(unix_ms().saturating_sub(at) / 1000),
        }
    }

    /// Record a replica in the shared state. Synchronous and idempotent.
    ///
    /// Everything a *writer* consults to decide what to emit has to change at
    /// the instant of the full-sync cut, not whenever the hub task gets round
    /// to the registration message:
    ///
    /// * `connected_replicas` — a write only propagates if it reads this as
    ///   non-zero. Left to the hub, the first replica's registration raced the
    ///   first writes after the cut; those were applied on the master, missing
    ///   from the payload, and never sent.
    /// * `repl_stream_last_db` — reset so the first write the new replica is
    ///   sent carries a `SELECT`. Left to the hub, a write could slip out
    ///   without one and be applied to db 0 on the replica.
    ///
    /// The hub calls this too when it processes the registration; the second
    /// call finds the row and changes nothing.
    pub fn register_replica(
        &self,
        replica_id: u64,
        peer_ip: String,
        peer_port: u16,
        base: u64,
    ) -> Arc<ReplicaProgress> {
        let mut acks = self.replica_acks.lock();
        if acks.contains_key(&replica_id)
            && let Some(existing) = self.replica_progress.lock().get(&replica_id)
        {
            return Arc::clone(existing);
        }
        self.repl_stream_last_db.store(u64::MAX, Ordering::Relaxed);
        acks.insert(replica_id, base);
        let progress = Arc::new(ReplicaProgress::new(base));
        self.replica_progress
            .lock()
            .insert(replica_id, Arc::clone(&progress));
        self.replica_peers
            .lock()
            .push((replica_id, peer_ip, peer_port));
        self.connected_replicas.fetch_add(1, Ordering::Relaxed);
        progress
    }

    /// Snapshot, per connected replica, how many stream bytes it has been sent
    /// so far. `WAIT` takes this after a hub barrier and then waits for each
    /// replica to acknowledge its own target — see [`ReplicaProgress`].
    pub fn wait_targets(&self) -> Vec<(Arc<ReplicaProgress>, u64)> {
        self.replica_progress
            .lock()
            .values()
            .map(|p| (Arc::clone(p), p.sent.load(Ordering::Relaxed)))
            .collect()
    }

    /// How many of `targets` have acknowledged everything they had been sent
    /// when the snapshot was taken.
    pub fn count_reached(targets: &[(Arc<ReplicaProgress>, u64)]) -> i64 {
        targets
            .iter()
            .filter(|(p, target)| p.delivered() >= *target)
            .count() as i64
    }

    /// Whether this server, a master, is to refuse writes for want of good
    /// replicas (`min-replicas-to-write`, `min-replicas-max-lag`): Redis's
    /// rule, for a master cut off from its replicas. Without it a master on
    /// the wrong side of a partition goes on acknowledging writes until it
    /// learns it has been replaced, and those writes are lost when the
    /// partition heals. One relaxed load when the setting is off.
    pub fn lacks_good_replicas(&self) -> bool {
        let wanted = self.min_replicas_to_write.load(Ordering::Relaxed);
        if wanted == 0 {
            return false;
        }
        let max_lag = self.min_replicas_max_lag.load(Ordering::Relaxed);
        if max_lag == 0 || self.is_slave() {
            return false;
        }
        self.good_replica_count(max_lag) < wanted as i64
    }

    /// Replicas that are online and acknowledged within the last
    /// `max_lag_secs` — Redis's definition of a "good" replica for
    /// `min-replicas-to-write`. One still receiving its dataset is not one.
    pub fn good_replica_count(&self, max_lag_secs: u64) -> i64 {
        self.replica_progress
            .lock()
            .values()
            .filter(|p| p.state() == ReplicaState::Online && p.lag_secs() <= max_lag_secs)
            .count() as i64
    }

    /// The master this node is currently following, if any.
    pub fn master_addr(&self) -> Option<(String, u16)> {
        self.master_addr.read().clone()
    }

    /// Record a new replication target. Called by `REPLICAOF <host> <port>` (and
    /// the cluster/startup equivalents) so `INFO`/`ROLE` reflect where this node
    /// is actually pointed rather than where it was pointed at boot.
    pub fn set_master_addr(&self, host: &str, port: u16) {
        *self.master_addr.write() = Some((host.to_string(), port));
    }

    /// True when this node is already following exactly `host:port`. `REPLICAOF`
    /// uses this to no-op instead of tearing down a healthy link — operators
    /// re-issue `REPLICAOF` on every reconcile pass, and restarting replication
    /// each time would mean a full resync per pass.
    pub fn is_already_following(&self, host: &str, port: u16) -> bool {
        self.is_slave()
            && matches!(
                &*self.master_addr.read(),
                Some((h, p)) if *p == port && h.eq_ignore_ascii_case(host)
            )
    }

    pub fn role(&self) -> String {
        self.role.read().clone()
    }

    /// The current replication epoch. A `start_replica` task whose claimed epoch
    /// no longer equals this has been superseded and must stop.
    #[inline]
    pub fn generation(&self) -> u64 {
        self.repl_generation.load(Ordering::Acquire)
    }

    /// Claim a fresh replication epoch, superseding every task holding an older
    /// one. Called by `start_replica` at task entry, so *every* spawn path
    /// (`REPLICAOF`, cluster reattach, `--replicaof` at startup) supersedes the
    /// previous task without each call site having to remember to do it.
    #[inline]
    pub fn begin_replication_epoch(&self) -> u64 {
        let _commit = self.dataset_commit.lock();
        self.repl_generation.fetch_add(1, Ordering::AcqRel) + 1
    }

    #[inline]
    pub fn become_master(&self) {
        // Promotion supersedes any in-flight replication task, including one
        // blocked mid-stream on a master that no longer exists. Not while a
        // full sync is putting the master's dataset in place: see
        // `dataset_commit`.
        let _commit = self.dataset_commit.lock();
        self.repl_generation.fetch_add(1, Ordering::AcqRel);
        // Where this replica stands in the stream it followed, read between
        // two of its applier's batches: that stream goes on as this master's
        // own (#109).
        let followed = self.slave_mode.load(Ordering::Acquire).then(|| {
            let _applying = self.apply_lock.lock();
            self.resync_point.lock().clone()
        });
        self.slave_mode.store(false, Ordering::Release);
        *self.master_addr.write() = None;
        *self.role.write() = "master".into();
        // A master has no upstream link. Leaving these set would have a later
        // demotion briefly report the previous master's link state.
        self.slave_link_up.store(false, Ordering::Release);
        self.slave_link_state
            .store(LinkState::Connect as u8, Ordering::Release);
        self.slave_sync_in_progress.store(false, Ordering::Release);
        self.master_link_down_since_ms.store(0, Ordering::Relaxed);
        // As a replica again, it is in step with its next master only once
        // it has streamed from it.
        self.in_step_ms.store(0, Ordering::Relaxed);
        self.full_sync_unfinished.store(false, Ordering::SeqCst);
        *self.last_sync_refusal.write() = None;
        // A master's dataset is its own from here on.
        self.set_resync_point(None);
        self.set_stream_origin(None);
        // Its new stream starts where it had read the old one to, and keeps
        // the old one's id as its second: the old master's other replicas
        // ask to be continued under that id, from where each of them is,
        // and get what they missed from the backlog this node kept as a
        // replica. As Redis's `shiftReplicationId`.
        if let Some(Some(point)) = followed {
            self.repl_offset.store(point.offset, Ordering::Relaxed);
            *self.repl_id2.write() = Some((point.repl_id.clone(), point.offset + 1));
            // Recorded from here on, before any client writes: a replica
            // continued later must find every write since the promotion.
            self.backlog_active.store(true, Ordering::Relaxed);
            if let Some(hub) = self.hub.get().and_then(|w| w.upgrade()) {
                let _ = hub.send(HubMsg::Resume {
                    offset: point.offset,
                });
            }
            log_info!(
                "Replication: Promoted; the stream {} goes on as {} from offset {} — its replicas \
                 can be continued",
                point.repl_id,
                self.repl_id(),
                point.offset
            );
        }
    }

    /// Replica side: record what was just read of the master's stream and
    /// applied, ending at `end`, in this node's own backlog, so that if it is
    /// promoted the master's other replicas can be continued from it.
    pub fn record_received(&self, bytes: Vec<u8>, end: u64) {
        if bytes.is_empty() {
            return;
        }
        if let Some(hub) = self.hub.get().and_then(|w| w.upgrade()) {
            let _ = hub.send(HubMsg::Received { bytes, end });
        }
    }

    /// Whether a replica asking to continue `repl_id` from `offset` (the
    /// first byte it wants) followed this node's stream: under its own id,
    /// or under the one it followed before it was promoted, up to where it
    /// had read it.
    pub fn continues_stream(&self, repl_id: &str, offset: u64) -> bool {
        repl_id == self.repl_id()
            || self
                .repl_id2
                .read()
                .as_ref()
                .is_some_and(|(id, until)| id == repl_id && offset <= *until)
    }

    #[inline]
    pub fn become_slave(&self) {
        // A master's dataset is its own write stream up to where that stream
        // stands, read where no write is half-way. Kept as where this node
        // stands, a master that continues the stream — its replica, promoted
        // in a failover — continues this node from there instead of sending
        // it everything (as Redis keeps a demoted master's stream as its
        // cached master). Only while the stream counted every write
        // (`feeds_hub`); a write of this node's own after it clears it
        // (`note_local_write`). The new master continues it only if it read
        // exactly this far: a byte more here, and it sends the dataset.
        let was_master = !self.slave_mode.load(Ordering::Acquire);
        let _cut = if was_master {
            crate::write_gate::cut()
        } else {
            None
        };
        if was_master && self.feeds_hub() {
            let offset = self.repl_offset.load(Ordering::Relaxed);
            if offset > 0 {
                let db = match self.repl_stream_last_db.load(Ordering::Relaxed) {
                    u64::MAX => 0,
                    db => db as usize,
                };
                let repl_id = self.repl_id();
                log_info!(
                    "Replication: This node's dataset is offset {offset} of its own stream \
                     {repl_id}: a master that continues that stream can continue this node"
                );
                self.set_resync_point(Some(ResyncPoint {
                    repl_id,
                    offset,
                    db,
                }));
            }
        }
        self.slave_mode.store(true, Ordering::Release);
        *self.role.write() = "slave".into();
        // Its own replicas would get nothing from here on: a replica does
        // not pass its master's stream on.
        if let Some(hub) = self.hub.get().and_then(|w| w.upgrade()) {
            let _ = hub.send(HubMsg::DropReplicas);
        }
        *self.repl_id2.write() = None;
        // The stream this node wrote as a master ends here. Were it promoted
        // again later under the same id, a replica that had followed the old
        // stream could be continued from an offset in another one.
        *self.repl_id.write() = generate_repl_id();
    }

    /// This node's replication id, as a master.
    pub fn repl_id(&self) -> String {
        self.repl_id.read().clone()
    }

    /// Fingerprint the dataset as this master starts (see `digest`), and
    /// keep the stream from its first byte, so that a replica whose dataset
    /// has the same fingerprint can be continued from there. Call before any
    /// client is served.
    pub fn fingerprint_at_start(&self, store: &crate::store::Store, hub_tx: &HubTx) {
        let started = std::time::Instant::now();
        let digest = digest::dataset_digest(store);
        *self.startup_digest.lock() = Some(digest);
        self.resume_as_master(self.repl_id(), 0, hub_tx);
        log_info!(
            "Replication: dataset fingerprinted in {} ms — a replica that holds the same \
             data will be continued rather than sent it",
            started.elapsed().as_millis()
        );
    }

    /// What to answer a replica's `PSYNC` with, if not the sync itself.
    ///
    /// A cluster master that came back without its dataset (see
    /// `ClusterState::check_dataset_at_start`) first tells a replica that
    /// takes the word (`REPLCONF capa lost-data`) so: `+LOSTDATA <node id>`.
    /// A replica that holds the dataset takes over instead of syncing; one
    /// that holds nothing asks again, and once every replica of this master
    /// has said that, this master serves what it has and the sync goes
    /// ahead. A replica that does not take the word is offered the sync as
    /// before: its own guard refuses an empty master.
    ///
    /// Otherwise the fingerprint offer, [`Self::digest_offer`].
    pub fn psync_offer(
        &self,
        cluster: &crate::cluster::ClusterState,
        registry: &crate::client_registry::ClientRegistry,
        client_id: u64,
        cmd: &[&[u8]],
    ) -> Option<Vec<u8>> {
        if cluster.lost_dataset() && !self.is_slave() {
            let (mut stage, mut addr, mut port) = (0u8, String::new(), 0u16);
            registry.update(client_id, |info| {
                stage = info.replica_lost_data;
                if stage == 1 {
                    info.replica_lost_data = 2;
                }
                addr = info.addr.clone();
                port = info.replica_listening_port;
            });
            match stage {
                1 => {
                    cluster.note_lost_dataset_offer();
                    log_warn!("Replication: Told replica {addr} that this master lost its dataset");
                    return Some(
                        format!(
                            "+LOSTDATA {}\r\n",
                            crate::cluster::node_id_str(&cluster.myself())
                        )
                        .into_bytes(),
                    );
                }
                2 => {
                    let (ip, source_port) = replica_client::peer_ip_port(&addr);
                    let port = if port != 0 { port } else { source_port };
                    if !cluster.replica_has_nothing(&ip, port) {
                        return Some(
                            b"-LOSTDATA this master lost its dataset and waits for its other \
                              replicas\r\n"
                                .to_vec(),
                        );
                    }
                }
                _ => {}
            }
        }
        self.digest_offer(registry, client_id, cmd)
    }

    /// The answer to a `PSYNC` that cannot be continued, from a replica that
    /// takes a fingerprint: this node's stream and its dataset's fingerprint
    /// at that stream's start. Offered once per connection; the replica
    /// compares and asks again.
    pub fn digest_offer(
        &self,
        registry: &crate::client_registry::ClientRegistry,
        client_id: u64,
        cmd: &[&[u8]],
    ) -> Option<Vec<u8>> {
        if self.is_slave() {
            return None;
        }
        let digest = (*self.startup_digest.lock())?;
        let continues = PsyncRequest::parse(cmd).is_some_and(|p| p.repl_id == self.repl_id());
        if continues {
            return None;
        }
        let mut takes = false;
        registry.update(client_id, |info| {
            takes = info.replica_takes_digest;
            info.replica_takes_digest = false;
        });
        takes.then(|| format!("+DIGEST {} {digest:016x}\r\n", self.repl_id()).into_bytes())
    }

    /// Carry on, as a master, the stream a clean shutdown left off: the
    /// dataset just loaded is exactly `repl_id`'s stream up to `offset`
    /// (see [`load_saved_master_point`]). The backlog is kept from there, so
    /// that a replica that was caught up when it was stopped cleanly too is
    /// continued instead of being sent everything again. Call before any
    /// client is served.
    pub fn resume_as_master(&self, repl_id: String, offset: u64, hub_tx: &HubTx) {
        *self.repl_id.write() = repl_id;
        self.repl_offset.store(offset, Ordering::Relaxed);
        self.backlog_active.store(true, Ordering::Relaxed);
        let _ = hub_tx.send(HubMsg::Resume { offset });
    }

    /// Hot-path: relaxed load only (no [`Self::role`] lock — use [`Self::become_master`] /
    /// [`Self::become_slave`] whenever the string changes).
    #[inline]
    pub fn is_slave(&self) -> bool {
        self.slave_mode.load(Ordering::Relaxed)
    }
}

/// Left beside `dump.kdb` by a cluster node whose final save at shutdown
/// succeeded: that dump is everything the node had. A master that starts
/// without it (and without an AOF) did not stop cleanly, and its dataset may
/// be behind its replicas' — see `ClusterState::check_dataset_at_start`.
pub const CLEAN_EXIT_PATH: &str = "dump.kdb.clean";
const CLEAN_EXIT_MAGIC: &str = "kividb-clean-exit 1";

/// Whether this process leaves [`CLEAN_EXIT_PATH`]: set in cluster mode.
pub static CLEAN_EXIT_MARKER: AtomicBool = AtomicBool::new(false);

/// After a successful final save at shutdown: say so beside the dump.
pub fn note_clean_exit() {
    if !CLEAN_EXIT_MARKER.load(Ordering::Relaxed) {
        return;
    }
    if let Ok((len, hash)) = dump_fingerprint("dump.kdb") {
        let tmp = format!("{CLEAN_EXIT_PATH}.tmp");
        if std::fs::write(&tmp, format!("{CLEAN_EXIT_MAGIC}\n{len}\n{hash}\n")).is_ok() {
            let _ = std::fs::rename(&tmp, CLEAN_EXIT_PATH);
        }
    }
}

/// Whether the previous process stopped cleanly with the `dump.kdb` this
/// process loaded: `loaded` is the fingerprint of the bytes that were read
/// ([`load_dump`]), not of the file there now, which another process may
/// have replaced since (#303). The marker is removed — durably, before
/// anything is served — so that a crash later in this process is not taken
/// for a clean stop.
pub fn take_clean_exit_marker(loaded: Option<DumpFingerprint>) -> bool {
    let Ok(text) = std::fs::read_to_string(CLEAN_EXIT_PATH) else {
        return false;
    };
    let _ = std::fs::remove_file(CLEAN_EXIT_PATH);
    if let Ok(dir) = std::fs::File::open(".") {
        let _ = dir.sync_all();
    }
    let mut lines = text.lines();
    if lines.next() != Some(CLEAN_EXIT_MAGIC) {
        return false;
    }
    let len: Option<u64> = lines.next().and_then(|l| l.parse().ok());
    let hash: Option<u64> = lines.next().and_then(|l| l.parse().ok());
    matches!((len, hash, loaded), (Some(l), Some(h), Some((dl, dh))) if l == dl && h == dh)
}

/// Where a save leaves the resync point of the dataset it wrote.
pub const RESYNC_PATH: &str = "dump.kdb.repl";
const RESYNC_MAGIC: &str = "kividb-resync 1";

/// A dump's length, and a hash of its first and last 64 KiB: enough to tell
/// whether a dump is the one a resync point or a clean-exit marker was
/// written for.
pub type DumpFingerprint = (u64, u64);

/// How much of each end of a dump its fingerprint hashes.
const FINGERPRINT_EDGE: usize = 64 * 1024;

/// The fingerprint of a dump `len` bytes long, from its first
/// [`FINGERPRINT_EDGE`] bytes and the last ones after those, up to as many
/// again. Both [`dump_fingerprint`] and [`FingerprintReader`] end here.
fn fingerprint(len: u64, head: &[u8], tail: &[u8]) -> DumpFingerprint {
    let mut sample = Vec::with_capacity(head.len() + tail.len());
    sample.extend_from_slice(head);
    sample.extend_from_slice(tail);
    (len, xxhash_rust::xxh3::xxh3_64(&sample))
}

/// The fingerprint of the file at `path` as it is now.
fn dump_fingerprint(path: &str) -> std::io::Result<DumpFingerprint> {
    use std::io::{Read, Seek, SeekFrom};
    const EDGE: u64 = FINGERPRINT_EDGE as u64;
    let mut file = std::fs::File::open(path)?;
    let len = file.metadata()?.len();
    let mut head = Vec::with_capacity(FINGERPRINT_EDGE);
    (&mut file).take(EDGE).read_to_end(&mut head)?;
    let mut tail = Vec::new();
    if len > EDGE {
        file.seek(SeekFrom::Start(len - EDGE.min(len - EDGE)))?;
        file.read_to_end(&mut tail)?;
    }
    Ok(fingerprint(len, &head, &tail))
}

/// A reader that takes the fingerprint of what is read through it.
///
/// The dump is loaded through one, so that what was loaded is what a saved
/// point is compared with. Compared with the file on disk instead, a point
/// saved for a newer dump, written by another process after this one had
/// read the older one, was taken for the point of the older (#303).
pub struct FingerprintReader<R> {
    inner: R,
    len: u64,
    head: Vec<u8>,
    /// The last bytes read after the head, at most [`FINGERPRINT_EDGE`]: a
    /// ring once full, its oldest byte at `tail_at`.
    tail: Vec<u8>,
    tail_at: usize,
}

impl<R: std::io::Read> FingerprintReader<R> {
    pub fn new(inner: R) -> Self {
        FingerprintReader {
            inner,
            len: 0,
            head: Vec::with_capacity(FINGERPRINT_EDGE),
            tail: Vec::with_capacity(FINGERPRINT_EDGE),
            tail_at: 0,
        }
    }

    fn note(&mut self, mut bytes: &[u8]) {
        self.len += bytes.len() as u64;
        if self.head.len() < FINGERPRINT_EDGE {
            let take = (FINGERPRINT_EDGE - self.head.len()).min(bytes.len());
            self.head.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];
        }
        if bytes.len() >= FINGERPRINT_EDGE {
            self.tail.clear();
            self.tail
                .extend_from_slice(&bytes[bytes.len() - FINGERPRINT_EDGE..]);
            self.tail_at = 0;
            return;
        }
        if self.tail.len() < FINGERPRINT_EDGE {
            let take = (FINGERPRINT_EDGE - self.tail.len()).min(bytes.len());
            self.tail.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];
        }
        while !bytes.is_empty() {
            let take = (FINGERPRINT_EDGE - self.tail_at).min(bytes.len());
            self.tail[self.tail_at..self.tail_at + take].copy_from_slice(&bytes[..take]);
            self.tail_at = (self.tail_at + take) % FINGERPRINT_EDGE;
            bytes = &bytes[take..];
        }
    }

    /// Read the rest, to the end, and give the fingerprint of everything.
    pub fn finish(mut self) -> std::io::Result<DumpFingerprint> {
        std::io::copy(&mut self, &mut std::io::sink())?;
        let (newer, older) = self.tail.split_at(self.tail_at);
        let mut tail = Vec::with_capacity(self.tail.len());
        tail.extend_from_slice(older);
        tail.extend_from_slice(newer);
        Ok(fingerprint(self.len, &self.head, &tail))
    }
}

impl<R: std::io::Read> std::io::Read for FingerprintReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.note(&buf[..n]);
        Ok(n)
    }
}

/// Load the dump at `path`, as `snapshot::load_snapshot` does, with the
/// fingerprint of the bytes that were loaded.
pub fn load_dump(
    path: &str,
) -> std::io::Result<(crate::snapshot::LoadedSnapshot, DumpFingerprint)> {
    // Opened again for each reading `load_snapshot_with` makes: the
    // fingerprint is of the reading that loaded it.
    let (loaded, reader) = crate::snapshot::load_snapshot_with(|| {
        let file = std::fs::File::open(path)?;
        Ok(std::io::BufReader::new(FingerprintReader::new(file)))
    })?;
    // What the buffer holds beyond what was parsed has been fingerprinted.
    let fingerprint = reader.into_inner().finish()?;
    Ok((loaded, fingerprint))
}

/// Write the dataset to disk — `dump.kdb`, and with the AOF on, the log reset
/// to an empty delta on top of it — and beside it, on a replica, the position
/// in its master's stream that the dataset corresponds to.
///
/// That position is what lets a replica that is restarted cleanly ask its
/// master to continue rather than send everything again.
pub fn save_dataset(
    store: &crate::store::Store,
    index_store: &crate::vector::IndexStore,
    repl_state: &ReplState,
) -> std::io::Result<()> {
    // No other checkpoint or save from here to the point saved beside the
    // dump below, which must be the point of this dump. Taken before the
    // applier's lock, so the applier does not wait while this waits for
    // its turn (and nothing that has its turn waits for that lock).
    let turn = crate::aof::checkpoint_turn()?;
    // The replica's applier is between commands, and stays there until the
    // save is done: the point read here is the dataset written below.
    let _applying = repl_state.apply_lock.lock();
    save_dataset_with(&turn, store, index_store, repl_state)
}

/// [`save_dataset`], with the checkpoint turn and the applier's lock held.
fn save_dataset_with(
    turn: &crate::aof::CheckpointTurn,
    store: &crate::store::Store,
    index_store: &crate::vector::IndexStore,
    repl_state: &ReplState,
) -> std::io::Result<()> {
    // First, so that a save that fails half-way cannot leave an old point
    // beside a new dump.
    let _ = std::fs::remove_file(RESYNC_PATH);
    let point = repl_state.resync_point.lock().clone();

    // On a master with its stream live, where the stream stands at the
    // instant the dataset is captured: no write is half-way then.
    let mut master_point: Option<(String, u64)> = None;
    if crate::aof::AOF_ENABLED.load(Ordering::Relaxed) {
        crate::aof::checkpoint_noting(turn, store, index_store, &mut || {
            if !repl_state.is_slave() && repl_state.feeds_hub() {
                master_point = Some((
                    repl_state.repl_id(),
                    repl_state.repl_offset.load(Ordering::Relaxed),
                ));
            }
        })?;
    } else {
        // One instant, as with the AOF on: the dataset is captured with
        // writes held, then written while they go on. It was copied a shard
        // at a time, so a transaction, a script or an MSET whose keys were
        // in two shards could be saved half applied (#148).
        let frozen = {
            let _cut = crate::write_gate::cut();
            crate::snapshot::freeze_dataset_for_disk(store, Some(index_store))
        }?;
        static TMP_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let tmp = format!(
            "dump.kdb.{}.{}.tmp",
            std::process::id(),
            TMP_SEQ.fetch_add(1, Ordering::Relaxed)
        );
        // With a save number of its own, as a checkpoint's: put in place
        // without one, the dump kept the number of the log beside it, and a
        // start with the AOF on replayed that log over it (#354).
        let generation = crate::aof::next_generation();
        if let Err(e) = frozen
            .write_file(&tmp)
            .and_then(|()| crate::aof::install_dump(&tmp, generation, true))
        {
            let _ = std::fs::remove_file(&tmp);
            return Err(e);
        }
        // The open log no longer goes on from it (#350); none on disk goes
        // with it (#354).
        crate::aof::dump_saved_without_log();
        // As the writer this replaced said it: operators and tests look for it.
        let shown = std::env::current_dir()
            .map(|dir| dir.join("dump.kdb"))
            .unwrap_or_else(|_| "dump.kdb".into());
        log_info!(
            "Snapshot saved  → {} ({} databases)",
            shown.display(),
            store.len()
        );
    }

    // Best effort: without the file, the next start does a full resync.
    let saved = match (point.filter(|_| repl_state.is_slave()), master_point) {
        (Some(point), _) => Some((point.repl_id, point.offset, point.db, "replica")),
        (None, Some((repl_id, offset))) if offset > 0 => Some((repl_id, offset, 0, "master")),
        _ => None,
    };
    if let Some((repl_id, offset, db, role)) = saved
        && let Ok((len, hash)) = dump_fingerprint("dump.kdb")
    {
        let text = format!("{RESYNC_MAGIC}\n{repl_id}\n{offset}\n{db}\n{len}\n{hash}\n{role}\n");
        let tmp = format!("{RESYNC_PATH}.tmp");
        if std::fs::write(&tmp, text).is_ok() {
            let _ = std::fs::rename(&tmp, RESYNC_PATH);
        }
    }
    crate::stats::RDB_SAVES.fetch_add(1, Ordering::Relaxed);
    crate::stats::record_save();
    Ok(())
}

/// The resync point saved beside `dump.kdb`, if it was saved for the dump
/// this process loaded, `loaded` being that dump's fingerprint
/// ([`load_dump`]). The caller must also know that the dataset in memory is
/// that dump and nothing more — see `aof::LocalRecovery::exact_dump`.
pub fn load_saved_resync_point(loaded: DumpFingerprint) -> Option<ResyncPoint> {
    saved_point(RESYNC_PATH, loaded, false)
}

/// The point a master saved at a clean shutdown, if it was saved for the
/// dump this process loaded: its replication id and offset. Same caveat as
/// [`load_saved_resync_point`].
pub fn load_saved_master_point(loaded: DumpFingerprint) -> Option<(String, u64)> {
    saved_point(RESYNC_PATH, loaded, true).map(|point| (point.repl_id, point.offset))
}

/// The point saved at `path` by a master (`master`) or a replica (a file
/// without the role is a replica's, from before masters saved theirs), if
/// it was saved for the dump whose fingerprint is `loaded`.
fn saved_point(path: &str, loaded: DumpFingerprint, master: bool) -> Option<ResyncPoint> {
    let text = std::fs::read_to_string(path).ok()?;
    let mut lines = text.lines();
    if lines.next()? != RESYNC_MAGIC {
        return None;
    }
    let repl_id = lines.next()?.to_string();
    let offset: u64 = lines.next()?.parse().ok()?;
    let db: usize = lines.next()?.parse().ok()?;
    let len: u64 = lines.next()?.parse().ok()?;
    let hash: u64 = lines.next()?.parse().ok()?;
    if (lines.next() == Some("master")) != master || repl_id.len() != 40 {
        return None;
    }
    if loaded != (len, hash) {
        log_info!(
            "Replication: {path} was saved for another dump than the one loaded — \
             not continuing from it"
        );
        return None;
    }
    Some(ResyncPoint {
        repl_id,
        offset,
        db,
    })
}

#[cfg(test)]
mod dump_fingerprint_tests {
    use super::*;
    use std::io::Read;

    /// The fingerprint taken while reading is the one taken of the file,
    /// however the reads are cut up and wherever the reader stops.
    #[test]
    fn the_fingerprint_of_what_was_read_is_the_fingerprint_of_the_file() {
        const EDGE: usize = FINGERPRINT_EDGE;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dump.kdb");
        let path = path.to_str().unwrap();
        for len in [
            0,
            1,
            1000,
            EDGE - 1,
            EDGE,
            EDGE + 1,
            2 * EDGE - 1,
            2 * EDGE,
            2 * EDGE + 1,
            5 * EDGE + 12_345,
        ] {
            let bytes: Vec<u8> = (0..len).map(|i| (i * 7 % 251) as u8).collect();
            std::fs::write(path, &bytes).unwrap();
            let expected = dump_fingerprint(path).unwrap();
            for chunk in [1, 7, 4096, EDGE - 1, EDGE, EDGE + 3, 3 * EDGE] {
                for stop in [0, len / 3, len] {
                    let mut reader = FingerprintReader::new(&bytes[..]);
                    let mut buf = vec![0u8; chunk];
                    let mut read = 0;
                    while read < stop {
                        let want = chunk.min(stop - read);
                        let n = reader.read(&mut buf[..want]).unwrap();
                        assert!(n > 0);
                        read += n;
                    }
                    assert_eq!(
                        reader.finish().unwrap(),
                        expected,
                        "length {len}, reads of {chunk}, stopped at {stop}"
                    );
                }
            }
        }
    }

    /// A point saved for a later dump than the one loaded is not used,
    /// though it matches the dump on disk (#303).
    #[test]
    fn a_saved_point_is_used_only_for_the_dump_that_was_loaded() {
        let dir = tempfile::tempdir().unwrap();
        let dump = dir.path().join("dump.kdb");
        let dump = dump.to_str().unwrap();
        let point = dir.path().join("dump.kdb.repl");
        let point = point.to_str().unwrap();

        std::fs::write(dump, vec![1u8; 100_000]).unwrap();
        let older = FingerprintReader::new(std::fs::File::open(dump).unwrap())
            .finish()
            .unwrap();
        // Another process saves a newer dump, and its point beside it.
        std::fs::write(dump, vec![2u8; 150_000]).unwrap();
        let (len, hash) = dump_fingerprint(dump).unwrap();
        let repl_id = "a".repeat(40);
        std::fs::write(
            point,
            format!("{RESYNC_MAGIC}\n{repl_id}\n4242\n0\n{len}\n{hash}\nmaster\n"),
        )
        .unwrap();

        assert!(saved_point(point, older, true).is_none());
        assert!(saved_point(point, (len, hash), false).is_none());
        let saved = saved_point(point, (len, hash), true).unwrap();
        assert_eq!((saved.repl_id, saved.offset), (repl_id, 4242));
    }
}

/// `repl-diskless-load`: whether a replica drops its dataset before loading a
/// full sync (`true`) or keeps it until the new one is loaded (`false`).
///
/// * `disabled`, `flushdb`, `on-empty-db` — drop first. Needs memory for one
///   dataset. The payload is received whole and read through to the end
///   before anything is dropped, so a transfer that is cut off, or a payload
///   that cannot be read, costs the replica nothing.
/// * `swapdb` — keep the old dataset, serving it, until the new one has
///   loaded, and swap. Needs memory for two.
pub fn flush_before_load(value: &str) -> Option<bool> {
    match value.to_ascii_lowercase().as_str() {
        "disabled" | "flushdb" | "on-empty-db" => Some(true),
        "swapdb" => Some(false),
        _ => None,
    }
}

/// Admin / connection commands still accepted on replicas during bulk snapshot apply.
#[inline]
pub fn slave_command_allowed_during_loading(name: &str) -> bool {
    matches!(
        name,
        "AUTH"
            | "HELLO"
            | "QUIT"
            | "RESET"
            | "PING"
            | "ECHO"
            | "CLIENT"
            | "REPLCONF"
            | "PSYNC"
            | "WAIT"
            | "WAITAOF"
            | "REPLICAOF"
            | "SLAVEOF"
            | "INFO"
            | "ROLE"
            | "LASTSAVE"
            | "TIME"
            | "COMMAND"
            | "CONFIG"
            | "SLOWLOG"
            | "LATENCY"
            | "MEMORY"
            | "MODULE"
            | "CLUSTER"
            | "READONLY"
            | "READWRITE"
            | "FAILOVER"
            | "SHUTDOWN"
            | "SELECT"
            | "ASKING"
            | "SUBSCRIBE"
            | "UNSUBSCRIBE"
            | "PSUBSCRIBE"
            | "PUNSUBSCRIBE"
    )
}

/// `KIVIDB_TEST_BARE_WRITE_DELAY_MS`, never set otherwise: the first write
/// encoded with no `SELECT` of its own, to a key whose name begins with
/// `delayed:`, waits this long, in the write gate, before it is queued. A
/// test's window to send another command meanwhile (#423). Unset, a load
/// and a branch, on a write that is propagated.
fn test_delay_of_a_bare_write(parts: &[Vec<u8>]) {
    static DELAY: std::sync::OnceLock<Option<std::time::Duration>> = std::sync::OnceLock::new();
    static TAKEN: AtomicBool = AtomicBool::new(false);
    let delay = DELAY.get_or_init(|| {
        std::env::var("KIVIDB_TEST_BARE_WRITE_DELAY_MS")
            .ok()
            .and_then(|ms| ms.parse().ok())
            .map(std::time::Duration::from_millis)
    });
    if let Some(delay) = *delay
        && parts.get(1).is_some_and(|key| key.starts_with(b"delayed:"))
        && !TAKEN.swap(true, Ordering::Relaxed)
    {
        crate::cluster::migration::off_the_runtime(|| std::thread::sleep(delay));
    }
}

/// The refusal of a write for want of good replicas, as Redis words it.
pub const NOREPLICAS: &str = "NOREPLICAS Not enough good replicas to write.";

#[inline]
pub fn is_write_command_bytes(cmd0: &[u8]) -> bool {
    // Avoid String allocation — just uppercase the bytes and check
    let mut buf = [0u8; 32];
    if cmd0.len() > 32 {
        return false;
    }
    for (i, &b) in cmd0.iter().enumerate() {
        buf[i] = b.to_ascii_uppercase();
    }
    let name = std::str::from_utf8(&buf[..cmd0.len()]).unwrap_or("");
    is_write_command(name)
}

/// Whether `name` (upper case) changes the dataset, as Redis's `write` flag
/// has it: [`is_write_command`] but `SELECT`. `SELECT` is propagated, so that
/// a replica and the AOF follow the switch of database, but it writes
/// nothing. So the checks that refuse or hold a write let it through, as
/// Redis does: `min-replicas-to-write`, `CLIENT PAUSE WRITE` and the ACL's
/// `write` category (#411, #392). (`maxmemory` has a list of its own,
/// `eviction::allowed_when_out_of_memory`.)
#[inline]
pub fn is_data_write(name: &str) -> bool {
    name != "SELECT" && is_write_command(name)
}

/// [`is_data_write`] of a command name as the client sent it.
#[inline]
pub fn is_data_write_bytes(cmd0: &[u8]) -> bool {
    !cmd0.eq_ignore_ascii_case(b"SELECT") && is_write_command_bytes(cmd0)
}

fn generate_repl_id() -> String {
    // 40 hex digits of randomness. A replica decides whether it can continue
    // from its master's history by comparing this ID, so two masters must not
    // share one — and the clock and the PID it used to be made from are not
    // far from shared between containers started together.
    let bytes: [u8; 20] = rand::random();
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

// ── Hub ────────────────────────────────────────────────────────────────────

/// A connected replica's write socket, registered with the hub.
pub struct ReplicaConn {
    pub addr: String,
    pub writer: OwnedWriteHalf,
}

/// Messages sent to the hub from the registration task.
// Add to HubMsg enum:
pub enum HubMsg {
    Register(ReplicaConn),
    RegisterChannel {
        addr: String,
        peer_ip: String,
        peer_port: u16,
        tx: mpsc::UnboundedSender<ReplPayload>,
        replica_id: u64,
        /// Shared with the replica's forwarder task: hub increments on enqueue,
        /// forwarder decrements after each successful flush. Lets the hub evict a
        /// replica whose outbound queue has grown past the configured limit instead
        /// of buffering an unbounded backlog for a replica that never catches up.
        pending_bytes: Arc<AtomicU64>,
        /// The offset this replica's `+FULLRESYNC` announced. Its ACKs count
        /// from here; see [`ReplicaProgress`].
        base: u64,
    },
    Propagate(ReplPayload),
    /// Replica side: bytes of the master's stream this node has read and
    /// applied, ending at offset `end`. Kept in the backlog, not forwarded.
    Received {
        bytes: Vec<u8>,
        end: u64,
    },
    AckOffset {
        replica_id: u64,
        offset: u64,
    }, // NEW
    Deregister(u64), // NEW
    /// A replica asking to continue from `offset` instead of taking a full
    /// sync. If the backlog still holds everything after that offset, the hub
    /// queues it for the replica, registers it and answers `Ok`; otherwise
    /// the registration is handed back and a full sync follows.
    Continue {
        registration: ReplicaRegistration,
        offset: u64,
        reply: tokio::sync::oneshot::Sender<Result<(), ReplicaRegistration>>,
    },
    /// Resolved once every message queued ahead of it has been processed.
    ///
    /// `propagate` is fire-and-forget, so a write can have returned to its
    /// client while its payload still sits in this inbox. `WAIT` sends a
    /// barrier first; when it resolves, every earlier write has been handed to
    /// the replicas and their `sent` counters are final for that write.
    Barrier(tokio::sync::oneshot::Sender<()>),
    /// Start the backlog at `offset`: see [`ReplState::resume_as_master`].
    Resume {
        offset: u64,
    },
    /// Let go of every replica: this node has become a replica itself, and
    /// does not pass its master's stream on (#144). Their connections close,
    /// and when they connect again they are told why.
    DropReplicas,
}

/// What a replica answers a node that asks to replicate from it: KiviDB does
/// not chain replicas, and one that took the request would send its data
/// once and nothing after (#144).
pub const NOT_A_MASTER: &str = "ERR this server is a replica, and KiviDB does not chain replicas: \
     replicate from its master instead";

/// The most recent part of the write stream, kept by the hub so that a
/// replica which lost its connection can be sent what it missed instead of
/// the whole dataset.
///
/// Offsets are positions in the stream: after the byte at offset *n* has been
/// applied, a replica's offset is *n*. The backlog holds the bytes
/// `start()+1 ..= end`.
struct Backlog {
    buf: std::collections::VecDeque<u8>,
    /// Offset of the last byte in `buf` — the stream's offset as the hub has
    /// seen it.
    end: u64,
    /// False until the first full sync, and again after the backlog is given
    /// up: there is nothing to continue from.
    valid: bool,
}

impl Backlog {
    fn new() -> Self {
        Self {
            buf: std::collections::VecDeque::new(),
            end: 0,
            valid: false,
        }
    }

    fn start(&self) -> u64 {
        self.end - self.buf.len() as u64
    }

    /// A full sync was cut at `base`. If the backlog already ends there it
    /// carries on; if not — it was inactive, so writes went by unrecorded —
    /// it starts again from `base`.
    fn align(&mut self, base: u64) {
        if !self.valid || self.end != base {
            if self.valid {
                log_info!(
                    "Replication: Backlog restarted at offset {base} (it ended at {})",
                    self.end
                );
            }
            self.buf.clear();
            self.end = base;
            self.valid = true;
        }
    }

    fn push(&mut self, payload: &[u8], capacity: usize) {
        if !self.valid {
            return;
        }
        self.end += payload.len() as u64;
        let capacity = capacity.max(16 * 1024);
        if payload.len() >= capacity {
            self.buf.clear();
            self.buf.extend(&payload[payload.len() - capacity..]);
            return;
        }
        let overflow = (self.buf.len() + payload.len()).saturating_sub(capacity);
        self.buf.drain(..overflow);
        self.buf.extend(payload);
    }

    /// Everything after `offset`, if all of it is still here.
    fn after(&self, offset: u64) -> Option<Vec<u8>> {
        if !self.valid || offset < self.start() || offset > self.end {
            return None;
        }
        let skip = (offset - self.start()) as usize;
        Some(self.buf.iter().skip(skip).copied().collect())
    }

    fn give_up(&mut self) {
        self.buf = std::collections::VecDeque::new();
        self.valid = false;
    }

    fn report(&self, state: &ReplState) {
        state.backlog_first_offset.store(
            if self.valid { self.start() + 1 } else { 0 },
            Ordering::Relaxed,
        );
        state
            .backlog_histlen
            .store(self.buf.len() as u64, Ordering::Relaxed);
    }
}

/// The replication stream's heartbeat: an inline-safe `PING`, RESP-encoded.
const REPL_PING: &[u8] = b"*1\r\n$4\r\nPING\r\n";

// NEW: global replica id counter
static NEXT_REPLICA_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// Whether KiviDB `version` (`kividb_version` of `INFO server`) deleted the
/// documents of `FT.DROPINDEX … DD`, and sent the command on as typed.
/// 1.1 and before kept them. 1.2.0's release candidates deleted them;
/// releases since send the deletes instead, and never `DD`, so whatever
/// this says of them is not asked. A version that cannot be read is taken
/// to have kept them: deleting what such a master kept would lose data.
pub fn dd_deleted_by(version: &str) -> bool {
    let mut numbers = version
        .split(['.', '-'])
        .map(|part| part.parse::<u32>().ok());
    match (numbers.next().flatten(), numbers.next().flatten()) {
        (Some(major), Some(minor)) => (major, minor) >= (1, 2),
        _ => false,
    }
}

/// The line of `INFO server` by which a KiviDB master says it sends a `DEL`
/// for every key it expires. Releases before it that did so are known by
/// their version ([`expiry_propagated_by`]).
pub const EXPIRY_PROPAGATED_INFO: &str = "kividb_expired_keys_propagated:1";

/// Whether a master whose `INFO server` reply is `info` sends a `DEL` (or
/// `UNLINK`) for every key it expires, so that its replicas need not
/// expire keys themselves (see `ReplState::master_propagates_expiry`).
///
/// * It says so ([`EXPIRY_PROPAGATED_INFO`]): yes.
/// * It is KiviDB (`kividb_version`) and does not say so: 1.1.3 and the
///   1.1 releases after it do; 1.1.2 and before do not, nor do 1.2.0-rc1 and
///   -rc2, which were cut before the fix reached 1.2. A version that cannot
///   be read is taken to send them, as below.
/// * It is not KiviDB (no `kividb_version`; every KiviDB release has said
///   its version there): Redis and Valkey have always sent them.
/// * It did not answer (`None`): `INFO` refused to the replication user by
///   an ACL, say. Taken to send them, as 1.1.3 replicas take every master:
///   a replica that waits for `DEL`s that never come keeps keys that read
///   as gone, using memory, while one that deletes keys by its own clock
///   under a master that also sends them can come apart from that master
///   (a write the master made to a key still alive by its clock lands on
///   a key the replica already deleted). Memory over divergence.
pub fn expiry_propagated_by(info: Option<&str>) -> bool {
    let Some(info) = info else {
        return true;
    };
    if info
        .lines()
        .any(|line| line.trim() == EXPIRY_PROPAGATED_INFO)
    {
        return true;
    }
    let Some(version) = info
        .lines()
        .find_map(|line| line.strip_prefix("kividb_version:"))
        .map(str::trim)
    else {
        return true;
    };
    let (release, pre) = match version.split_once('-') {
        Some((release, pre)) => (release, Some(pre)),
        None => (version, None),
    };
    let numbers: Option<Vec<u32>> = release.split('.').map(|n| n.parse().ok()).collect();
    match numbers.as_deref() {
        Some(&[major, minor, patch]) => match (major, minor, patch, pre) {
            // The release candidates of 1.2.0 cut before the fix.
            (1, 2, 0, Some(_)) => false,
            _ => (major, minor, patch) >= (1, 1, 3),
        },
        _ => true,
    }
}

/// The master's `uptime_in_seconds` in its `INFO server` reply `info`.
pub fn uptime_in(info: &str) -> Option<u64> {
    info.lines()
        .find_map(|line| line.strip_prefix("uptime_in_seconds:"))
        .and_then(|n| n.trim().parse().ok())
}

#[cfg(test)]
mod expiry_version_tests {
    use super::{EXPIRY_PROPAGATED_INFO, expiry_propagated_by, uptime_in};

    fn kividb(version: &str) -> String {
        format!("# Server\r\nredis_version:8.10.0\r\nkividb_version:{version}\r\n")
    }

    #[test]
    fn masters_from_1_1_3_send_their_expired_keys_as_deletions() {
        for old in [
            "1.1.2",
            "1.1.0-rc1",
            "1.0.6",
            "0.9.0",
            "1.2.0-rc1",
            "1.2.0-rc2",
        ] {
            assert!(!expiry_propagated_by(Some(&kividb(old))), "{old}");
        }
        for new in ["1.1.3", "1.1.4", "1.2.0", "1.2.1", "1.3.0-rc1", "2.0.0"] {
            assert!(expiry_propagated_by(Some(&kividb(new))), "{new}");
        }
        // A build that says so, whatever its version.
        let says = format!("{}{EXPIRY_PROPAGATED_INFO}\r\n", kividb("1.2.0-rc2"));
        assert!(expiry_propagated_by(Some(&says)));
    }

    #[test]
    fn a_master_that_is_not_kividb_or_does_not_say_is_taken_to_send_them() {
        let redis = "# Server\r\nredis_version:8.10.0\r\nuptime_in_seconds:5\r\n";
        assert!(expiry_propagated_by(Some(redis)));
        assert!(expiry_propagated_by(None));
        assert!(expiry_propagated_by(Some(&kividb("garbage"))));
        assert_eq!(uptime_in(redis), Some(5));
        assert_eq!(uptime_in(&kividb("1.1.2")), None);
    }
}

#[cfg(test)]
mod dd_version_tests {
    use super::dd_deleted_by;

    #[test]
    fn only_1_2_and_later_deleted_the_documents_of_dd() {
        for kept in ["1.1.2", "1.1.0-rc1", "1.0.6", "0.9.0", "", "garbage", "1"] {
            assert!(!dd_deleted_by(kept), "{kept}");
        }
        for deleted in ["1.2.0-rc1", "1.2.0-rc2", "1.2.0", "1.3.0", "2.0.0"] {
            assert!(dd_deleted_by(deleted), "{deleted}");
        }
    }
}

pub fn next_replica_id() -> u64 {
    NEXT_REPLICA_ID.fetch_add(1, Ordering::Relaxed)
}

pub type HubTx = mpsc::UnboundedSender<HubMsg>;

/// Spawn the replication hub task. Returns the sender used to
/// register replicas and to propagate write commands.
///
/// `repl-replica-max-backlog-bytes` (`ReplState::replica_max_backlog_bytes`)
/// bounds how much unflushed data the hub will queue for any single replica
/// (see [`HubMsg::RegisterChannel::pending_bytes`]). A replica that can't keep
/// up — slow network, stalled disk, wedged process — gets disconnected once its
/// queue crosses this line instead of growing the master's memory without
/// bound.
///
/// A replica in a full sync has its own, larger bound,
/// `repl-replica-sync-max-backlog-bytes` (#305). Everything written while its
/// dataset is sent and loaded waits in its queue: it reads nothing of the
/// stream until it has loaded, and has it all to read then. Held to the
/// streaming limit, a large dataset under heavy writes never synced: the queue
/// passed the limit before the load was done, the replica was dropped, and the
/// sync began again, for as long as the writes lasted. The sync's bound holds
/// from the cut until the replica, having loaded the dataset and acknowledged
/// it, has drained its queue to a quarter of the streaming limit; the
/// streaming limit holds from then on. Both are read for each write, so `CONFIG SET` changes
/// them for replicas already attached. The master logs how far each
/// replica's queue went during its sync.
pub fn start_hub(state: Arc<ReplState>) -> HubTx {
    let (hub_tx, mut hub_rx) = mpsc::unbounded_channel::<HubMsg>();
    let _ = state.hub.set(hub_tx.downgrade());
    // For the heartbeat, which the hub sends to itself. Weak, so that the hub
    // still ends when every real sender is gone.
    let own_tx = hub_tx.downgrade();

    tokio::spawn(async move {
        // Two lists: socket-backed and channel-backed replicas
        let mut socket_replicas: Vec<ReplicaConn> = Vec::new();
        let mut channel_replicas: Vec<ChannelReplica> = Vec::new();

        // Heartbeat clock. Checked once a second rather than slept for a whole
        // period so `CONFIG SET repl-ping-replica-period` takes effect promptly.
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last_ping = tokio::time::Instant::now();
        let mut backlog = Backlog::new();
        // When the last replica went away, while the backlog is being kept.
        let mut without_replicas: Option<tokio::time::Instant> = None;

        loop {
            // What is queued for the replicas and not yet taken by their
            // forwarders is not counted against `maxmemory`, as Redis does not
            // count its replicas' output buffers: a full sync queues every
            // write made while its payload is laid out and sent, and evicting
            // keys for it gives nothing back (#284). Each replica is queued
            // the same writes, shared: the longest queue is what they hold.
            // Taken again after every message and every second.
            crate::memory_use::set_replica_queued(
                channel_replicas
                    .iter()
                    .map(|r| r.pending_bytes.load(Ordering::Relaxed))
                    .max()
                    .unwrap_or(0) as usize,
            );
            let msg = tokio::select! {
                msg = hub_rx.recv() => match msg {
                    Some(m) => m,
                    None => break,
                },
                _ = tick.tick() => {
                    backlog.report(&state);
                    // Give the backlog up once no replica has been attached
                    // for `repl-backlog-ttl`: keeping it means encoding every
                    // write for a replica that is not coming back.
                    if channel_replicas.is_empty() && backlog.valid {
                        let since = *without_replicas.get_or_insert_with(tokio::time::Instant::now);
                        let ttl = state.backlog_ttl_secs.load(Ordering::Relaxed);
                        if ttl > 0 && since.elapsed() >= std::time::Duration::from_secs(ttl) {
                            // As a writer in the gate, so that this cannot
                            // cross the cut of a full sync that is switching
                            // the backlog on.
                            let _gate = crate::write_gate::enter();
                            if state.connected_replicas.load(Ordering::Relaxed) == 0 {
                                state.backlog_active.store(false, Ordering::Relaxed);
                                backlog.give_up();
                                backlog.report(&state);
                                log_info!(
                                    "Replication: No replica for {ttl}s — replication backlog released"
                                );
                            }
                        }
                    } else {
                        without_replicas = None;
                    }
                    let period = state.repl_ping_period_secs.load(Ordering::Relaxed);
                    if period == 0
                        || channel_replicas.is_empty()
                        || last_ping.elapsed() < std::time::Duration::from_secs(period)
                    {
                        continue;
                    }
                    last_ping = tokio::time::Instant::now();
                    // A master with nothing to replicate still has to say so:
                    // an idle link and a dead one are otherwise identical from
                    // the replica's side, which is how a replica came to
                    // report `master_link_status:up` for 15 minutes after its
                    // master was powered off. The PING is ordinary stream
                    // content, so it advances the offset like any other byte.
                    //
                    // Counted inside the write gate, like a write: a full
                    // sync's cut then reads an offset that either includes
                    // this PING, with the PING already in the stream, or does
                    // not — never one without the other.
                    //
                    // And queued, not handled here and now, for the same
                    // reason: its place in the stream has to be its place in
                    // the queue, relative to a registration made at a cut.
                    //
                    // None once a shutdown has ended the stream: the offset
                    // it saves is the last byte its replicas are sent. Looked
                    // at inside the gate, which the shutdown cycles after it
                    // sets the flag, so that a PING is either wholly before
                    // the end or not sent.
                    {
                        let _gate = crate::write_gate::enter();
                        // Nor while writes are paused (a manual failover's
                        // handoff, `CLIENT PAUSE`), as in Redis: a replica
                        // promoted at the offset it was told to reach must
                        // find this node there when it follows it.
                        if state.stream_closed.load(Ordering::SeqCst)
                            || crate::pause::expiry_deferred()
                        {
                            continue;
                        }
                        state
                            .repl_offset
                            .fetch_add(REPL_PING.len() as u64, Ordering::Relaxed);
                        if let Some(tx) = own_tx.upgrade() {
                            let _ = tx.send(HubMsg::Propagate(Arc::from(REPL_PING)));
                        }
                    }
                    continue;
                }
            };
            match msg {
                HubMsg::AckOffset { replica_id, offset } => {
                    state.replica_acks.lock().insert(replica_id, offset);
                    if let Some(p) = state.replica_progress.lock().get(&replica_id) {
                        p.acked.store(offset, Ordering::Relaxed);
                        p.last_ack_ms.store(unix_ms(), Ordering::Relaxed);
                        // A replica acknowledges only once it has the dataset
                        // loaded: this is what "online" means.
                        p.set_state(ReplicaState::Online);
                    }
                }

                HubMsg::Continue {
                    registration,
                    offset,
                    reply,
                } => match backlog.after(offset) {
                    // What follows a replica's position begins a command. If
                    // it does not, the position is not one in this backlog's
                    // numbering, and a replica sent it would read nothing of
                    // it, ever.
                    Some(missed) if !crate::resp::parse::can_start_command(&missed) => {
                        log_warn!(
                            "Replication: Partial resync refused for {}: offset {offset} is not \
                             where a command begins in this master's stream — sending the dataset",
                            registration.addr
                        );
                        state.sync_partial_err.fetch_add(1, Ordering::Relaxed);
                        let _ = reply.send(Err(registration));
                    }
                    Some(missed) => {
                        let progress = state.register_replica(
                            registration.replica_id,
                            registration.peer_ip.clone(),
                            registration.peer_port,
                            offset,
                        );
                        progress.set_state(ReplicaState::Online);
                        let _ = progress.queue.set(Arc::clone(&registration.pending_bytes));
                        let missed_len = missed.len() as u64;
                        let delivered = missed.is_empty() || {
                            registration
                                .pending_bytes
                                .fetch_add(missed_len, Ordering::Relaxed);
                            registration.tx.send(Arc::from(missed)).is_ok()
                        };
                        if !delivered {
                            forget_replica(&state, registration.replica_id);
                            continue;
                        }
                        progress.sent.fetch_add(missed_len, Ordering::Relaxed);
                        state.sync_partial_ok.fetch_add(1, Ordering::Relaxed);
                        log_info!(
                            "Replication: Partial resync for {}: continuing from offset {offset} \
                             ({missed_len} bytes of backlog)",
                            registration.addr
                        );
                        channel_replicas.push(ChannelReplica {
                            id: registration.replica_id,
                            addr: registration.addr,
                            tx: registration.tx,
                            pending_bytes: registration.pending_bytes,
                            progress,
                            syncing: false,
                            peak: missed_len,
                        });
                        if reply.send(Ok(())).is_err() {
                            // The connection went away while it was asking.
                            if let Some(gone) = channel_replicas.pop() {
                                forget_replica(&state, gone.id);
                            }
                        }
                    }
                    None => {
                        if backlog.valid {
                            log_info!(
                                "Replication: Partial resync not possible for {}: offset {offset} \
                                 is not in the backlog, which holds offsets {} to {} \
                                 (repl-backlog-size) — sending the dataset",
                                registration.addr,
                                backlog.start() + 1,
                                backlog.end
                            );
                        }
                        state.sync_partial_err.fetch_add(1, Ordering::Relaxed);
                        let _ = reply.send(Err(registration));
                    }
                },

                HubMsg::Resume { offset } => {
                    backlog.align(offset);
                    backlog.report(&state);
                }
                HubMsg::Received { bytes, end } => {
                    // Contiguous with what is kept, or the start of a new run
                    // (after a full sync, or a gap).
                    let start = end.saturating_sub(bytes.len() as u64);
                    backlog.align(start);
                    backlog.push(&bytes, state.backlog_size.load(Ordering::Relaxed) as usize);
                    backlog.report(&state);
                }
                HubMsg::Barrier(done) => {
                    let _ = done.send(());
                }

                HubMsg::Deregister(replica_id) => {
                    // Tear down registration (client disconnect / rollback).
                    channel_replicas.retain(|r| {
                        if r.id == replica_id && r.syncing {
                            log_info!(
                                "Replication: Replica {} left during its full sync; its queue \
                                 peaked at {} bytes",
                                r.addr,
                                r.peak
                            );
                        }
                        r.id != replica_id
                    });
                    forget_replica(&state, replica_id);
                }
                HubMsg::DropReplicas => {
                    // Dropping each `tx` ends its forwarder, and so its
                    // connection, as an eviction does.
                    for replica in channel_replicas.drain(..) {
                        forget_replica(&state, replica.id);
                        log_info!(
                            "Replication: Replica {} disconnected: this node is now a replica \
                             itself",
                            replica.addr
                        );
                    }
                    if !socket_replicas.is_empty() {
                        let n = socket_replicas.len();
                        socket_replicas.clear();
                        let before = state
                            .connected_replicas
                            .fetch_sub(n as i64, Ordering::Relaxed);
                        if before < n as i64 {
                            state.connected_replicas.store(0, Ordering::Relaxed);
                        }
                    }
                }
                HubMsg::Register(conn) => {
                    log_info!("Replication: Socket replica registered: {}", conn.addr);
                    state.connected_replicas.fetch_add(1, Ordering::Relaxed);
                    // Fresh replication consumer — force implicit SELECT on next propagated write.
                    state.repl_stream_last_db.store(u64::MAX, Ordering::Relaxed);
                    socket_replicas.push(conn);
                }

                HubMsg::RegisterChannel {
                    addr,
                    peer_ip,
                    peer_port,
                    tx,
                    replica_id,
                    pending_bytes,
                    base,
                } => {
                    // Everything queued before this message is in the
                    // dataset the replica was sent; the backlog records what
                    // comes after.
                    if backlog.valid && backlog.end != base && !channel_replicas.is_empty() {
                        // The replicas attached count the stream to
                        // `backlog.end`, the master's offset to `base`: the
                        // two have come apart, and from here the backlog
                        // counts as the offset does. A replica continued
                        // later from a position it counted the old way
                        // would be sent the stream from somewhere else.
                        // So they are let go, and the stream gets a new
                        // id: none of them is continued, each is sent the
                        // dataset.
                        log_warn!(
                            "Replication: The stream's offset is {base} but {} replica(s) were \
                             sent it to {}; disconnecting them, and starting a new replication \
                             id so that each resyncs in full",
                            channel_replicas.len(),
                            backlog.end
                        );
                        for replica in channel_replicas.drain(..) {
                            forget_replica(&state, replica.id);
                        }
                        *state.repl_id.write() = generate_repl_id();
                        *state.repl_id2.write() = None;
                    }
                    backlog.align(base);
                    state.backlog_active.store(true, Ordering::Relaxed);
                    // Already done at the cut by `begin_full_sync`; a no-op
                    // then. Done here for registrations that did not come
                    // through it.
                    let progress = state.register_replica(replica_id, peer_ip, peer_port, base);
                    let _ = progress.queue.set(Arc::clone(&pending_bytes));
                    channel_replicas.push(ChannelReplica {
                        id: replica_id,
                        addr: addr.clone(),
                        tx,
                        pending_bytes,
                        progress,
                        // In its full sync until it has loaded the dataset
                        // and drained what was queued meanwhile.
                        syncing: true,
                        peak: 0,
                    });
                    log_info!("Replication: Channel replica registered: {addr}");
                }

                HubMsg::Propagate(payload) => {
                    // `repl_offset` is advanced in `execute_command` when we encode replication
                    // bytes so standalone masters skip encode/hub entirely without paying twice here.
                    backlog.push(
                        &payload,
                        state.backlog_size.load(Ordering::Relaxed) as usize,
                    );

                    if socket_replicas.is_empty() && channel_replicas.is_empty() {
                        continue;
                    }

                    // Fan out to socket replicas
                    let mut alive_sockets = Vec::with_capacity(socket_replicas.len());
                    for mut r in socket_replicas.drain(..) {
                        use tokio::io::AsyncWriteExt;
                        match r.writer.write_all(&payload).await {
                            Ok(_) => alive_sockets.push(r),
                            Err(e) => {
                                log_warn!("Replication: Socket replica {} dropped: {e}", r.addr);
                                state.connected_replicas.fetch_sub(1, Ordering::Relaxed);
                            }
                        }
                    }
                    socket_replicas = alive_sockets;

                    // Fan out to channel replicas (unbounded outbound — never fills before drain,
                    // but capped in bytes below so a replica that never catches up can't hold
                    // an unbounded amount of undelivered data in master memory).
                    let mut alive_channels = Vec::with_capacity(channel_replicas.len());
                    // Read for each write, so that `CONFIG SET` applies at once.
                    let streaming_limit = state.replica_max_backlog_bytes.load(Ordering::Relaxed);
                    let sync_limit = state
                        .replica_sync_max_backlog_bytes
                        .load(Ordering::Relaxed)
                        .max(streaming_limit);
                    for mut replica in channel_replicas.drain(..) {
                        // Account for the payload *before* handing it over. The
                        // forwarder subtracts as soon as it has the payload, and
                        // it runs on another thread: counting afterwards let it
                        // subtract first, wrapping the counter below zero. A
                        // debug build then panicked on the addition here, which
                        // killed the hub and with it all replication.
                        let now_pending = replica
                            .pending_bytes
                            .fetch_add(payload.len() as u64, Ordering::Relaxed)
                            .saturating_add(payload.len() as u64);
                        if let Err(_closed) = replica.tx.send(Arc::clone(&payload)) {
                            forget_replica(&state, replica.id);
                            log_warn!(
                                "Replication: Channel replica {} disconnected (hub send failed)",
                                replica.addr
                            );
                            continue;
                        }
                        replica
                            .progress
                            .sent
                            .fetch_add(payload.len() as u64, Ordering::Relaxed);

                        replica.peak = replica.peak.max(now_pending);
                        let limit = if replica.syncing {
                            sync_limit
                        } else {
                            streaming_limit
                        };
                        if now_pending > limit {
                            // Replica isn't draining its queue fast enough — evict rather than
                            // let the backlog grow without bound. Dropping `tx` here (by not
                            // pushing it back into `channel_replicas`) closes its forwarder's
                            // channel; the owning connection task treats "forwarder ended" as
                            // a disconnect and tears the rest of the connection down. It will
                            // need a full resync to catch back up.
                            forget_replica(&state, replica.id);
                            log_warn!(
                                "Replication: Channel replica {} exceeded backlog limit \
                                 ({now_pending} > {limit} bytes{}) — evicting",
                                replica.addr,
                                if replica.syncing {
                                    ", repl-replica-sync-max-backlog-bytes, during its full sync"
                                } else {
                                    ""
                                }
                            );
                            continue;
                        }
                        // The sync is over once the replica has loaded the
                        // dataset (it acknowledges only then) and drained
                        // its queue to a quarter of the streaming limit.
                        // Not at the limit itself: a replica still draining
                        // dips under it and climbs back over it as it
                        // pauses (its own AOF checkpoint holds its applier
                        // for a second or two), and was dropped moments
                        // after its sync had ended.
                        if replica.syncing
                            && now_pending <= streaming_limit / 4
                            && replica.progress.state() == ReplicaState::Online
                        {
                            replica.syncing = false;
                            log_info!(
                                "Replication: Replica {} is streaming: its queue peaked at {} \
                                 bytes during the full sync (repl-replica-sync-max-backlog-bytes \
                                 {sync_limit}, repl-replica-max-backlog-bytes {streaming_limit})",
                                replica.addr,
                                replica.peak
                            );
                        }

                        alive_channels.push(replica);
                    }
                    channel_replicas = alive_channels;
                }
            }
        }
    });

    hub_tx
}

/// What the hub needs to start feeding a replica. Handed to
/// [`begin_full_sync`], which registers it at the cut.
pub struct ReplicaRegistration {
    pub replica_id: u64,
    pub addr: String,
    pub peer_ip: String,
    pub peer_port: u16,
    pub tx: mpsc::UnboundedSender<ReplPayload>,
    pub pending_bytes: Arc<AtomicU64>,
}

/// Take the cut for a full resync: register the replica with the hub and
/// serialise the dataset at one instant.
///
/// Returns the offset to announce in `+FULLRESYNC` and the payload. Blocks —
/// call it from a blocking thread, never from a runtime worker.
///
/// The two used to be separate steps with writes flowing in between: the
/// replica was registered, and then the dataset was walked. A write landing
/// in that window was both in the payload (the walk saw its effect) and in the
/// stream (the hub forwarded its payload), so the replica applied it twice. A
/// replica attached to a master taking `INCR`s or `RPUSH`es came up with
/// counters ahead of the master's and duplicated list elements, and stayed
/// that way.
///
/// With the write gate closed there is no "in between": every write in flight
/// has finished — store mutation *and* hub send — and no new one has started.
/// Whatever the hub was sent before the registration message is already in the
/// dataset and is not forwarded; whatever comes after is not in the dataset
/// and is.
///
/// Writes stall for the duration of the serialisation. Reads are not blocked.
pub fn begin_full_sync(
    store: &crate::store::Store,
    index_store: &crate::vector::IndexStore,
    repl_state: &ReplState,
    hub_tx: &HubTx,
    registration: ReplicaRegistration,
) -> std::io::Result<FullSync> {
    let started = std::time::Instant::now();
    let cut = crate::write_gate::cut();

    // From here every write goes to the hub, replicas attached or not, so
    // that the backlog has no holes in it.
    repl_state.backlog_active.store(true, Ordering::Relaxed);
    repl_state.sync_full.fetch_add(1, Ordering::Relaxed);
    let base = repl_state.repl_offset.load(Ordering::Relaxed);
    // The shared state changes now, under the cut — see `register_replica`.
    // The message tells the hub where to send; being queued before the gate
    // reopens, it is ahead of every payload a post-cut write will queue.
    repl_state.register_replica(
        registration.replica_id,
        registration.peer_ip.clone(),
        registration.peer_port,
        base,
    );
    if hub_tx
        .send(HubMsg::RegisterChannel {
            addr: registration.addr,
            peer_ip: registration.peer_ip,
            peer_port: registration.peer_port,
            tx: registration.tx,
            replica_id: registration.replica_id,
            pending_bytes: registration.pending_bytes,
            base,
        })
        .is_err()
    {
        forget_replica(repl_state, registration.replica_id);
        return Err(std::io::Error::other("replication hub is gone"));
    }

    // Only what must be captured atomically is captured under the cut. String
    // values are immutable and shared, so they are merely referenced here and
    // copied out afterwards; see `snapshot::freeze_dataset`.
    let frozen = match crate::snapshot::freeze_dataset(store, Some(index_store)) {
        Ok(f) => f,
        Err(e) => {
            drop(cut);
            let _ = hub_tx.send(HubMsg::Deregister(registration.replica_id));
            return Err(e);
        }
    };
    drop(cut);
    let held = started.elapsed();

    let (payload, not_counted) = frozen.into_image()?;
    if held > std::time::Duration::from_millis(100) {
        log_info!(
            "Replication: Full-sync cut held writes for {} ms ({} byte payload)",
            held.as_millis(),
            payload.len()
        );
    }
    Ok(FullSync {
        base,
        payload,
        not_counted,
    })
}

/// What [`begin_full_sync`] cut: the offset the payload corresponds to, and
/// the payload.
pub struct FullSync {
    pub base: u64,
    pub payload: Vec<u8>,
    /// The payload is not counted against `maxmemory` while this lives
    /// (#284): kept until the payload is sent, as Redis does not count what
    /// is queued for its replicas either.
    pub not_counted: crate::memory_use::NotCounted,
}

impl FullSync {
    /// The line that announces it.
    pub fn announcement(&self, repl_id: &str) -> String {
        format!("+FULLRESYNC {repl_id} {}\r\n", self.base)
    }
}

/// Ask the hub to continue a replica from where it says it is.
///
/// `Ok` means the replica is registered and what it missed is queued for it:
/// answer `+CONTINUE` and start forwarding. `Err` hands the registration back
/// for a full sync.
pub async fn request_continuation(
    hub_tx: &HubTx,
    repl_state: &ReplState,
    psync: Option<&PsyncRequest>,
    registration: ReplicaRegistration,
) -> Result<(), ReplicaRegistration> {
    let Some(psync) = psync else {
        return Err(registration);
    };
    if !repl_state.continues_stream(&psync.repl_id, psync.offset) {
        // It followed some other master.
        repl_state.sync_partial_err.fetch_add(1, Ordering::Relaxed);
        return Err(registration);
    }
    let (reply, answer) = tokio::sync::oneshot::channel();
    let request = HubMsg::Continue {
        registration,
        // It asked for the byte after the last one it has.
        offset: psync.offset - 1,
        reply,
    };
    if let Err(mpsc::error::SendError(HubMsg::Continue { registration, .. })) = hub_tx.send(request)
    {
        return Err(registration);
    }
    match answer.await {
        Ok(result) => result,
        // The hub is gone; so is the registration it was holding.
        Err(_) => Ok(()),
    }
}

/// A replica fed through a channel (every replica today; the socket-backed
/// list above is legacy).
struct ChannelReplica {
    id: u64,
    addr: String,
    tx: mpsc::UnboundedSender<ReplPayload>,
    pending_bytes: Arc<AtomicU64>,
    progress: Arc<ReplicaProgress>,
    /// In its full sync: held to `repl-replica-sync-max-backlog-bytes`
    /// rather than `repl-replica-max-backlog-bytes`. See [`start_hub`].
    syncing: bool,
    /// The most that has been queued for it at once.
    peak: u64,
}

/// Drop every trace of a replica from the shared state. Idempotent: the
/// `connected_replicas` decrement is keyed on the ACK row still existing, so a
/// second teardown of the same replica cannot drive the count negative.
fn forget_replica(state: &ReplState, replica_id: u64) {
    state
        .replica_peers
        .lock()
        .retain(|(id, _, _)| *id != replica_id);
    state.replica_progress.lock().remove(&replica_id);
    if state.replica_acks.lock().remove(&replica_id).is_some() {
        // Pairs with fetch_add on successful RegisterChannel in the hub task.
        state.connected_replicas.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Shutdown: end the write stream here. A master restarted from its final
/// save carries its stream on from the offset that save records, and
/// continues a replica only if that replica stopped at exactly that offset
/// (#306). Called once no command runs or can start (the clients are paused,
/// or the command gate is held alone); the sweeps of expired keys and fields
/// wait while clients are paused, and enter the command gate. What is left is
/// the replica `PING`, which the hub writes on its own. It looks at this flag
/// inside the write gate: cycling the gate after setting it waits for one
/// that was being written, and for any write still in flight, so every
/// `PING` is either wholly in the stream before the end or never sent. The
/// sweeps look at the flag too.
pub fn end_stream_for_shutdown(state: &ReplState) {
    state.stream_closed.store(true, Ordering::SeqCst);
    drop(crate::write_gate::cut());
}

/// Shutdown, just before the final save: the dataset may be saved, and stays
/// as it is until the save is done (while the guard lives); or `None`, while
/// a full sync is replacing this replica's dataset. Its dataset is then the
/// master's in part, or nothing at all (the default `repl-diskless-load`
/// drops the old one first), and a save would write that over the last good
/// `dump.kdb`, which the next start would load. The caller skips the save:
/// the last dump and its resync point stay, and the next start syncs again.
/// The guard is the lock a full sync holds from before it touches the
/// dataset until the master's is in place, waited for briefly: other
/// holders let go of it at once.
pub fn dataset_for_final_save(state: &ReplState) -> Option<parking_lot::MutexGuard<'_, ()>> {
    if state.slave_sync_in_progress.load(Ordering::SeqCst) {
        return None;
    }
    let settled = state
        .dataset_commit
        .try_lock_for(std::time::Duration::from_secs(1))?;
    (!state.slave_sync_in_progress.load(Ordering::SeqCst)).then_some(settled)
}

/// What a shutdown holds from when it closes the command gate to the exit:
/// the gate, alone, and what its final save takes ([`hold_for_final_save`]).
/// Never let go of: a gate opened again before the exit let writes run that
/// were acknowledged and lost with the process.
pub struct FinalSaveHold<'a> {
    _alone: crate::command_gate::AloneGuard,
    _applying: parking_lot::MutexGuard<'a, ()>,
    turn: crate::aof::CheckpointTurn,
}

/// Shutdown: close the command gate for the final save, then take the
/// replica applier's lock, which the save takes too: the checkpoint turn,
/// the gate, the lock, as every other holder of the lock takes them.
///
/// The applier takes the gate before its lock, for every turn of its
/// master's stream (#373), so once the gate is closed it holds the lock
/// only to publish its offset, a moment. It used to take the lock first and
/// enter the gate inside it: a shutdown that closed the gate and then waited
/// for the lock waited for ever (#308), and this took the lock first, then
/// the gate, the other way round from `SAVE` and a promotion.
///
/// A command already running that itself waits for the turn (`SAVE`) would
/// keep the gate from closing: if it does not close in a moment, the turn is
/// let go of, so that the command can finish, and both are taken again.
/// Clients are paused by then, so no new command starts meanwhile.
pub fn hold_for_final_save(state: &ReplState) -> std::io::Result<FinalSaveHold<'_>> {
    use std::time::{Duration, Instant};
    loop {
        let turn = crate::aof::checkpoint_turn()?;
        let deadline = Instant::now() + Duration::from_millis(50);
        if let Some(alone) =
            crate::command_gate::enter_alone_unless(Duration::from_millis(5), || {
                Instant::now() >= deadline
            })
        {
            let applying = state.apply_lock.lock();
            return Ok(FinalSaveHold {
                _alone: alone,
                _applying: applying,
                turn,
            });
        }
        drop(turn);
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// [`save_dataset`] for a shutdown, under its [`hold_for_final_save`].
pub fn save_dataset_held(
    hold: &FinalSaveHold<'_>,
    store: &crate::store::Store,
    index_store: &crate::vector::IndexStore,
    repl_state: &ReplState,
) -> std::io::Result<()> {
    save_dataset_with(&hold.turn, store, index_store, repl_state)
}

/// What a shutdown logs when [`dataset_for_final_save`] says no.
pub const FINAL_SAVE_SKIPPED: &str = "Shutdown: this replica is in the middle of a full sync, and \
     its dataset is only part of the master's: the final save is skipped, so that dump.kdb (and \
     its resync point) stays the last good one. The next start syncs again";

/// How long clients stay paused for a shutdown: until the process exits.
const SHUTDOWN_PAUSE_MS: i64 = 100 * 365 * 24 * 3600 * 1000;

/// Shutdown, before the command gate is taken: pause every client, end the
/// stream ([`end_stream_for_shutdown`]) and wait, for at most
/// `shutdown-timeout`, until every streaming replica has acknowledged all it
/// has been sent. Returns when the wait ends, for [`report_replicas_at_shutdown`].
///
/// Clients are paused rather than held at the command gate for the wait: a
/// command that waits at the gate holds its worker thread, and on io_uring
/// that is every connection on the worker, the replica's among them, so the
/// replica was sent nothing more and the wait always ran out. A paused client
/// waits without holding its thread (`pause::gate`), as in Redis, which pauses
/// its clients while a shutdown waits for the replicas. No command starts
/// again before the exit: the pause has no end, and the caller then holds the
/// command gate alone. Blocking: called from the thread that then saves.
pub fn drain_replicas_for_shutdown(state: &ReplState, hub_tx: &HubTx) -> std::time::Instant {
    crate::pause::set_pause(SHUTDOWN_PAUSE_MS, false);
    end_stream_for_shutdown(state);
    let timeout =
        std::time::Duration::from_secs(state.shutdown_timeout_secs.load(Ordering::Relaxed));
    let deadline = std::time::Instant::now() + timeout;
    wait_for_replicas_until(state, hub_tx, deadline);
    deadline
}

/// Shutdown, with the command gate held alone, just before the save: wait
/// what is left of `shutdown-timeout` for a write that was past the pause's
/// check when the pause began and has only now been queued (rare), and say
/// which replicas have the whole stream. One that has is continued from the
/// final save after the restart; one that has not, or that was still being
/// sent the dataset, gets the dataset again.
pub fn report_replicas_at_shutdown(
    state: &ReplState,
    hub_tx: &HubTx,
    started: std::time::Instant,
    deadline: std::time::Instant,
) {
    if state.is_slave() || state.shutdown_timeout_secs.load(Ordering::Relaxed) == 0 {
        return;
    }
    wait_for_replicas_until(state, hub_tx, deadline);
    let waited = started.elapsed().as_millis();
    for (addr, p) in replicas_at_shutdown(state) {
        let sent = p.sent.load(Ordering::Relaxed);
        let end = p.base + sent;
        if p.state() != ReplicaState::Online {
            log_warn!(
                "Replication: Shutdown: replica {addr} was still being sent the dataset ({}); it \
                 will be sent it again after the restart",
                p.state().as_str()
            );
        } else if p.delivered() >= sent {
            log_info!(
                "Replication: Shutdown: replica {addr} has the whole stream, to offset {end} \
                 ({waited} ms); it can be continued after the restart"
            );
        } else {
            log_warn!(
                "Replication: Shutdown: replica {addr} did not acknowledge the end of the stream \
                 within shutdown-timeout ({} s): it is at offset {} of {end}; it will be sent \
                 the dataset after the restart",
                state.shutdown_timeout_secs.load(Ordering::Relaxed),
                p.acked.load(Ordering::Relaxed)
            );
        }
    }
}

/// Every registered replica and its progress, by address.
fn replicas_at_shutdown(state: &ReplState) -> Vec<(String, Arc<ReplicaProgress>)> {
    let peers = state.replica_peers.lock().clone();
    let progress = state.replica_progress.lock();
    peers
        .into_iter()
        .filter_map(|(id, ip, port)| Some((format!("{ip}:{port}"), Arc::clone(progress.get(&id)?))))
        .collect()
}

/// Until `deadline`, or until every streaming replica has acknowledged all it
/// has been sent. A hub barrier first: once the hub has answered, everything
/// written before has been handed to the replicas' queues.
fn wait_for_replicas_until(state: &ReplState, hub_tx: &HubTx, deadline: std::time::Instant) {
    if state.is_slave() || state.replica_progress.lock().is_empty() {
        return;
    }
    let pause = std::time::Duration::from_millis(5);
    let (done_tx, mut done_rx) = tokio::sync::oneshot::channel();
    if hub_tx.send(HubMsg::Barrier(done_tx)).is_ok() {
        while matches!(
            done_rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ) && std::time::Instant::now() < deadline
        {
            std::thread::sleep(pause);
        }
    }
    while std::time::Instant::now() < deadline
        && replicas_at_shutdown(state).iter().any(|(_, p)| {
            p.state() == ReplicaState::Online && p.delivered() < p.sent.load(Ordering::Relaxed)
        })
    {
        std::thread::sleep(pause);
    }
}

/// Convenience: propagate a write command from the sync command path.
/// Never drops commits: unbounded hub inbox; channel replicas accumulate backlog until drained.
#[inline]
pub fn propagate(hub_tx: &HubTx, raw: Vec<u8>) {
    let payload: ReplPayload = Arc::from(raw.as_slice());
    if hub_tx.send(HubMsg::Propagate(payload)).is_err() {
        log_warn!("Replication: Hub sender closed — replication cannot proceed");
    }
}

/// One successful write: bump replication offset, fan out to replicas, optional AOF.
/// Skips when [`crate::commands::ctx::Ctx::apply_replication`] is true (replica applier).
/// True when a propagated write would actually be observed by anything.
///
/// [`replicate_one_write`] builds its RESP payload *before* it checks whether
/// there is a consumer, and its caller in `apply_write_side_effects` first
/// clones every argument into a `Vec<Vec<u8>>`. On a standalone master with AOF
/// off — the shape every throughput benchmark runs in — that means each write
/// allocated a vector of copies of its own arguments, encoded the full command
/// into another fresh buffer, and then dropped all of it because
/// `has_replicas || aof_enabled` was false.
///
/// This mirrors *exactly* the conditions under which `replicate_one_write` has
/// an observable effect: the offset advance, the hub propagate, the AOF append,
/// and the `repl_stream_last_db` store all live inside that same condition. When
/// this returns false the call is a no-op, so callers can skip building the
/// payload at all.
///
/// Kept next to `replicate_one_write` deliberately — if that function ever grows
/// an effect outside the `has_replicas || aof_enabled` branch, this predicate
/// has to grow with it.
#[inline]
pub fn write_is_observed(ctx: &crate::commands::ctx::Ctx<'_>) -> bool {
    if ctx.apply_replication {
        // A write arriving from the master still has exactly one observer: this
        // node's own AOF. Without this, a replica applied its master's stream
        // and logged none of it, so its AOF stayed empty for as long as it was
        // a replica — and promoting it produced a node that looked durable and
        // had nothing on disk to recover from.
        //
        // `aof_enabled` is false for AOF replay (which also sets this flag), so
        // replay still cannot write back into the log it is reading.
        //
        // A backup being taken on this replica is an observer too.
        return ctx.aof_enabled || ctx.repl_state.stream_taps.load(Ordering::Relaxed) > 0;
    }
    if ctx.repl_state.is_slave() {
        // Replicas only re-emit into their own AOF; they never propagate onward.
        return ctx.aof_enabled || ctx.repl_state.stream_taps.load(Ordering::Relaxed) > 0;
    }
    ctx.aof_enabled
        || ctx.repl_state.feeds_hub()
        || ctx.repl_state.stream_taps.load(Ordering::Relaxed) > 0
}

pub fn replicate_one_write(ctx: &crate::commands::ctx::Ctx<'_>, parts: &[Vec<u8>]) {
    let cmd0 = parts.first().map(|v| v.as_slice()).unwrap_or_default();
    let payloads: Vec<Vec<u8>> = if cmd0.eq_ignore_ascii_case(b"SELECT") && parts.len() >= 2 {
        // `CLIENT PAUSE WRITE` lets a `SELECT` run (it writes nothing), but
        // nothing may be added to the stream while writes are paused: a
        // failover waits for its replica to reach the master's offset, and
        // takes that offset to stay put. A `SELECT` sent now is not needed
        // there either: the next write in another database carries its own
        // (`encode_for_stream`), since the stream's database is not moved.
        // Not on a replica, whose offset is its master's.
        if !ctx.apply_replication && !ctx.repl_state.is_slave() && crate::pause::active().is_some()
        {
            return;
        }
        vec![crate::resp::write::encode_resp_command(parts)]
    } else {
        in_db_for_stream(ctx.repl_state, *ctx.selected_db, parts).0
    };

    // Two cases end at this node's own AOF and go no further:
    //
    // * `apply_replication` — a write handed to us by our master. It must not
    //   be forwarded, and must not advance `repl_offset`: the stream reader
    //   owns that counter, deriving it from bytes actually received. Counting
    //   it twice would make this replica ACK an offset its master never sent,
    //   breaking `WAIT`.
    // * `is_slave` with a client writing directly to the replica — same
    //   reasoning, different origin.
    let tapped = ctx.repl_state.stream_taps.load(Ordering::Relaxed) > 0;
    let local_aof_only = ctx.apply_replication || ctx.repl_state.is_slave();
    if local_aof_only {
        // A replica has no replicas of its own to forward to, but a backup
        // being taken on it reads the same stream.
        if tapped {
            for payload in &payloads {
                ctx.repl_state.feed_taps(payload);
            }
        }
        if ctx.aof_enabled {
            // Part of a transaction its master sent, applied as one: see
            // `PropagationGroup`.
            note_group_write(true);
            append_to_own_aof(ctx, cmd0, parts, payloads);
        } else if tapped {
            advance_stream_db(ctx, cmd0, parts);
        }
        return;
    }

    // "Has replicas" in the sense that matters here: the hub wants the write
    // — for a replica, or for the backlog a replica may come back to.
    let has_replicas = ctx.repl_state.feeds_hub();
    if !has_replicas && !ctx.aof_enabled && !tapped {
        return;
    }
    // One thread-local read, for a write that is not part of a transaction
    // or a script.
    let grouped = in_group();
    if grouped {
        note_group_write(ctx.aof_enabled);
    }

    for payload_bytes in payloads {
        if tapped {
            ctx.repl_state.feed_taps(&payload_bytes);
        }
        let mut raw_opt = Some(payload_bytes);
        let enc_len = raw_opt.as_ref().expect("just encoded").len() as u64;
        if grouped && has_replicas {
            // Into the stream with the rest of the group, when it ends.
            GROUP_STREAM.with(|stream| {
                stream
                    .borrow_mut()
                    .extend_from_slice(raw_opt.as_ref().expect("just encoded"))
            });
        } else {
            ctx.repl_state
                .repl_offset
                .fetch_add(enc_len, Ordering::Relaxed);
            if has_replicas {
                let hub_payload = if ctx.aof_enabled {
                    raw_opt.as_ref().expect("just encoded").clone()
                } else {
                    raw_opt.take().expect("just encoded")
                };
                propagate(ctx.hub_tx, hub_payload);
            }
        }
        if ctx.aof_enabled {
            crate::aof::AOF_WRITTEN.fetch_add(enc_len, std::sync::atomic::Ordering::Relaxed);
            crate::aof::queue_record(ctx.aof_tx, raw_opt.take().expect("encoded for AOF path"));
        }
    }
    // Only advance after bytes are emitted: writes that ran with no replicas must not
    // consume the implicit `SELECT`, or the first observer would miss `{select *}` on stream.
    advance_stream_db(ctx, cmd0, parts);
}

/// Hand something other than a write — a `PUBLISH` — to replicas: now, or,
/// inside a transaction or a script, in its place among the group's writes.
pub fn propagate_in_order(repl_state: &ReplState, hub_tx: &HubTx, payload: Vec<u8>) {
    if in_group() {
        GROUP_STREAM.with(|stream| stream.borrow_mut().extend_from_slice(&payload));
        return;
    }
    repl_state
        .repl_offset
        .fetch_add(payload.len() as u64, Ordering::Relaxed);
    propagate(hub_tx, payload);
}

// ── Transactions and scripts in the stream ─────────────────────────────────
//
// On the master an `EXEC` is one step: it holds the command gate alone, so no
// client sees half of it. Its writes used to reach replicas, and the AOF, as
// one command after another. A replica applying them one by one showed its
// readers half a transfer for as long as the second half took to arrive, and
// a process killed while the AOF held half of them replayed half (#172).
//
// So, as in Redis, the writes of a transaction — and of a script, which is
// one step in the same way — go into the stream between a `MULTI` and an
// `EXEC`. A replica keeps what follows a `MULTI` until the `EXEC` has
// arrived, then applies it as one step (`replica_client`); an AOF replay
// drops a `MULTI` whose `EXEC` never made it to the file (`aof::replay_aof`).
//
// As in Redis 7, only a group that writes twice or more is wrapped: one
// write is one step already. A group that writes nothing sends nothing.
//
// What the group puts in the stream is held until it ends and handed to the
// hub as one piece, as its AOF records are (`aof::RecordGroup`). Sent as it
// was made, it could be split by what other threads put in the stream
// meanwhile: the hub's heartbeat, a slot migration's deletes — and, with
// scripts running beside each other by hash tag (`tag_locks`), another
// script's writes, whose own `MULTI` would then land inside this one. Holding
// it back changes no order that matters: no other command touches the
// group's keys until it ends — an `EXEC` runs alone, a script beside others
// holds its tag.
//
// The stream's taps (a backup, a slot migration) still get each write as it
// is made, without the `MULTI` and `EXEC`: a migration reads one command per
// piece, and a backup's file is replayed whole, by itself.

/// `MULTI` and `EXEC` as they go into the stream and the AOF.
pub(crate) const MULTI_RECORD: &[u8] = b"*1\r\n$5\r\nMULTI\r\n";
pub(crate) const EXEC_RECORD: &[u8] = b"*1\r\n$4\r\nEXEC\r\n";

/// What an open [`PropagationGroup`] has written so far.
#[derive(Default)]
struct GroupState {
    writes: u32,
    /// Its `MULTI` is at the front of its part of the stream.
    stream_wrapped: bool,
    /// Its `MULTI` is at the front of the AOF records held for it.
    aof_wrapped: bool,
}

thread_local! {
    /// Whether this thread has a [`PropagationGroup`] open: the one thing a
    /// write that is not in one has to read.
    static IN_GROUP: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static GROUP: std::cell::RefCell<GroupState> = const {
        std::cell::RefCell::new(GroupState { writes: 0, stream_wrapped: false, aof_wrapped: false })
    };
    /// The group's part of the replication stream, sent when it ends. Empty
    /// when nothing it wrote is for the hub.
    static GROUP_STREAM: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
}

#[inline]
fn in_group() -> bool {
    IN_GROUP.with(|g| g.get())
}

/// Count a write of the open group, if one is open. At the second, the group
/// becomes a transaction: its `MULTI` goes in front of the first.
#[inline]
fn note_group_write(aof: bool) {
    if !in_group() {
        return;
    }
    GROUP.with(|group| {
        let mut group = group.borrow_mut();
        group.writes += 1;
        if group.writes != 2 {
            return;
        }
        GROUP_STREAM.with(|stream| {
            let mut stream = stream.borrow_mut();
            if !stream.is_empty() {
                stream.splice(0..0, MULTI_RECORD.iter().copied());
                group.stream_wrapped = true;
            }
        });
        if aof && crate::aof::put_in_front_of_group(MULTI_RECORD) {
            crate::aof::AOF_WRITTEN.fetch_add(MULTI_RECORD.len() as u64, Ordering::Relaxed);
            group.aof_wrapped = true;
        }
    });
}

/// The writes of one transaction or script, as one step to replicas and the
/// AOF: its part of the stream handed to the hub as one piece when it ends,
/// its AOF records queued as one (an [`crate::aof::RecordGroup`]), and both
/// wrapped in `MULTI` ... `EXEC` when it wrote more than once.
///
/// Open it where the transaction or script starts, holding what makes it one
/// step on this node — the command gate alone, or the script's tag; the write
/// gate — and let it be dropped before they are. A group opened inside
/// another (a script queued in a transaction) is part of that one.
pub struct PropagationGroup<'a> {
    repl_state: &'a ReplState,
    hub_tx: &'a HubTx,
    aof_tx: &'a dyn crate::commands::ctx::AofSender,
    /// False for a group opened inside another.
    outermost: bool,
    /// Dropped after [`Drop::drop`] has queued the `EXEC`: the records go
    /// to the AOF with it.
    _records: crate::aof::RecordGroup<'a>,
    /// Its state is this thread's.
    _not_send: std::marker::PhantomData<*const ()>,
}

impl<'a> PropagationGroup<'a> {
    pub fn begin(
        repl_state: &'a ReplState,
        hub_tx: &'a HubTx,
        aof_tx: &'a dyn crate::commands::ctx::AofSender,
    ) -> Self {
        let outermost = !IN_GROUP.with(|g| g.replace(true));
        PropagationGroup {
            repl_state,
            hub_tx,
            aof_tx,
            outermost,
            _records: crate::aof::RecordGroup::begin(aof_tx),
            _not_send: std::marker::PhantomData,
        }
    }
}

impl Drop for PropagationGroup<'_> {
    fn drop(&mut self) {
        if !self.outermost {
            return;
        }
        IN_GROUP.with(|g| g.set(false));
        let group = GROUP.with(|g| std::mem::take(&mut *g.borrow_mut()));
        if group.aof_wrapped {
            crate::aof::AOF_WRITTEN.fetch_add(EXEC_RECORD.len() as u64, Ordering::Relaxed);
            crate::aof::queue_record(self.aof_tx, EXEC_RECORD.to_vec());
        }
        let mut stream = GROUP_STREAM.with(|s| std::mem::take(&mut *s.borrow_mut()));
        if !stream.is_empty() {
            if group.stream_wrapped {
                stream.extend_from_slice(EXEC_RECORD);
            }
            self.repl_state
                .repl_offset
                .fetch_add(stream.len() as u64, Ordering::Relaxed);
            propagate(self.hub_tx, stream);
        }
    }
}

// Propagate a write made by the server itself rather than by a client's
/// command — a slot migration deleting the keys it has moved away, say —
/// to replicas, taps and the AOF, as [`replicate_one_write`] does for a
/// command. Call it holding the write gate, with the store already changed.
///
/// A replica originates nothing: its master tells it what to delete.
pub fn replicate_from_background(
    repl_state: &ReplState,
    hub_tx: &HubTx,
    db: usize,
    parts: &[Vec<u8>],
) {
    if repl_state.is_slave() {
        return;
    }
    let aof = crate::aof::AOF_ENABLED.load(Ordering::Relaxed);
    let has_replicas = repl_state.feeds_hub();
    let tapped = repl_state.stream_taps.load(Ordering::Relaxed) > 0;
    if !aof && !has_replicas && !tapped {
        return;
    }

    for payload in repl_state.encode_for_stream(db, parts) {
        repl_state
            .repl_offset
            .fetch_add(payload.len() as u64, Ordering::Relaxed);
        if tapped {
            repl_state.feed_taps(&payload);
        }
        if has_replicas {
            propagate(hub_tx, payload.clone());
        }
        if aof {
            crate::aof::AOF_WRITTEN.fetch_add(payload.len() as u64, Ordering::Relaxed);
            crate::aof::send_to_global_writer(payload);
        }
    }
    repl_state
        .repl_stream_last_db
        .store(db as u64, Ordering::Relaxed);
}

/// A key this master has expired in database `db`: `parts`, its `DEL` (or
/// `UNLINK`), to replicas, taps and the AOF, as the write that deleted it
/// (see `expiry`). As [`replicate_from_background`], but also from inside a
/// command: in a transaction or a script it goes with the group's writes,
/// where it was made, and to the AOF through the group's records.
///
/// Call it holding the write gate and the key's place in the order of
/// writes (`expiry::order`), with the key already gone. A replica
/// propagates nothing: its master tells it what expired.
pub fn replicate_expired(repl_state: &ReplState, hub_tx: &HubTx, db: usize, parts: &[Vec<u8>]) {
    if repl_state.is_slave() {
        return;
    }
    let aof = crate::aof::AOF_ENABLED.load(Ordering::Relaxed);
    let has_replicas = repl_state.feeds_hub();
    let tapped = repl_state.stream_taps.load(Ordering::Relaxed) > 0;
    if !aof && !has_replicas && !tapped {
        return;
    }
    let grouped = in_group();
    if grouped {
        note_group_write(aof);
    }
    // One AOF record: a `SELECT` and the `DEL` it qualifies must not be
    // separable by a crash.
    let mut record = Vec::new();
    let (payloads, now_in) = in_db_for_stream(repl_state, db, parts);
    for payload in payloads {
        if tapped {
            repl_state.feed_taps(&payload);
        }
        if aof {
            record.extend_from_slice(&payload);
        }
        if grouped && has_replicas {
            GROUP_STREAM.with(|stream| stream.borrow_mut().extend_from_slice(&payload));
        } else {
            repl_state
                .repl_offset
                .fetch_add(payload.len() as u64, Ordering::Relaxed);
            if has_replicas {
                propagate(hub_tx, payload);
            }
        }
    }
    if aof {
        crate::aof::AOF_WRITTEN.fetch_add(record.len() as u64, Ordering::Relaxed);
        crate::aof::queue_record(&crate::commands::ctx::GlobalAofSender, record);
    }
    repl_state
        .repl_stream_last_db
        .store(now_in as u64, Ordering::Relaxed);
}

/// `parts`, a write of database `db`, encoded for the write stream; and the
/// database the stream is in after it.
///
/// Written for expiry first, and as true of a key evicted, or of the
/// documents `FT.DROPINDEX ... DD` deletes, in another database: those are
/// written in the key's database whichever one the client has selected
/// (`eviction`, `vector::propagate_in_db`), and were sent as a `SELECT` and
/// the command, with the stream then taken to be there (#423).
///
/// While no client has left database 0, a client's write carries no
/// `SELECT`: whether it needs one is read from `repl_stream_last_db` before
/// it is sent ([`ReplState::encode_for_stream`]). A key can be in another
/// database all the same (`MOVE`, `COPY ... DB`, `SWAPDB`, a dataset
/// loaded), and expiry deletes it beside those writes. Sent as a `SELECT`
/// and a `DEL`, with the stream then taken to be in the key's database, a
/// write that read 0 meanwhile went after the `SELECT` and was applied in
/// the key's database by the replica and by a replay of the AOF; and as it
/// stored 0 after, every write of database 0 from then on was too (3,300
/// keys deleted on the master stayed on its replica in 8 s of six clients
/// writing). So the deletion goes in one piece that goes back to 0, which
/// leaves the stream where those writes take it to be.
fn in_db_for_stream(repl_state: &ReplState, db: usize, parts: &[Vec<u8>]) -> (Vec<Vec<u8>>, usize) {
    if db == 0 || repl_state.several_databases.load(Ordering::Relaxed) {
        return (repl_state.encode_for_stream(db, parts), db);
    }
    let select = |db: usize| {
        crate::resp::write::encode_resp_command(&[b"SELECT".to_vec(), db.to_string().into_bytes()])
    };
    let mut piece = select(db);
    piece.extend_from_slice(&crate::resp::write::encode_resp_command(parts));
    piece.extend_from_slice(&select(0));
    (vec![piece], 0)
}

/// Append an already-encoded write to this node's own AOF and stop.
///
/// Used by both nodes that must log without forwarding: a replica applying its
/// master's stream, and a client writing directly to a replica. Neither touches
/// `repl_offset` (the stream reader owns it) or the hub (nothing downstream).
///
/// `payloads` already carries any `SELECT` the db switch required, built by the
/// caller against `repl_stream_last_db` — which here tracks the last db written
/// to *the AOF*, since that is the only stream this node emits.
fn append_to_own_aof(
    ctx: &crate::commands::ctx::Ctx<'_>,
    cmd0: &[u8],
    parts: &[Vec<u8>],
    payloads: Vec<Vec<u8>>,
) {
    // One record, not one per payload: a `SELECT` and the write it qualifies
    // must not be separable by a crash, or replay applies the write to
    // whichever db happened to be selected.
    let mut combined = Vec::new();
    for p in payloads {
        combined.extend_from_slice(&p);
    }
    crate::aof::AOF_WRITTEN.fetch_add(combined.len() as u64, Ordering::Relaxed);
    crate::aof::queue_record(ctx.aof_tx, combined);
    advance_stream_db(ctx, cmd0, parts);
}

/// Record which db the emitted stream is now positioned on, so the next write
/// only pays for a `SELECT` when it actually changes.
fn advance_stream_db(ctx: &crate::commands::ctx::Ctx<'_>, cmd0: &[u8], parts: &[Vec<u8>]) {
    if cmd0.eq_ignore_ascii_case(b"SELECT") && parts.len() >= 2 {
        if let Ok(db_s) = std::str::from_utf8(&parts[1])
            && let Ok(db) = db_s.parse::<u64>()
        {
            ctx.repl_state
                .repl_stream_last_db
                .store(db, Ordering::Relaxed);
        }
    } else {
        // Before any client has left database 0, a write of another went in
        // one piece that goes back to 0 (`in_db_for_stream`).
        let db =
            if *ctx.selected_db != 0 && !ctx.repl_state.several_databases.load(Ordering::Relaxed) {
                0
            } else {
                *ctx.selected_db
            };
        ctx.repl_state
            .repl_stream_last_db
            .store(db as u64, Ordering::Relaxed);
    }
}

/// Returns true if this command name mutates state and must be
/// propagated to replicas. Read-only commands are not propagated.
///
/// Dispatches on length before comparing any bytes. rustc lowers a flat
/// `matches!` over string literals to a chain of equality tests, so the *miss*
/// case — every read command, evaluated on every single dispatch via
/// `command_mutates_for_replication` — used to walk all 139 entries before
/// returning false. Bucketing by length turns that into a jump on `len()` plus a
/// handful of comparisons; `GET` now tests against 2 candidates instead of 139.
///
/// The buckets are generated from the flat list, and
/// `is_write_command_matches_reference` asserts the two agree on every name, so
/// a command cannot be silently dropped or added when this is edited.
#[inline]
pub fn is_write_command(name: &str) -> bool {
    match name.len() {
        3 => matches!(name, "DEL" | "SET"),
        4 => matches!(
            name,
            "COPY"
                | "DECR"
                | "HDEL"
                | "HSET"
                | "INCR"
                | "LPOP"
                | "LREM"
                | "LSET"
                | "MOVE"
                | "MSET"
                | "RPOP"
                | "SADD"
                | "SORT"
                | "SPOP"
                | "SREM"
                | "VADD"
                | "VREM"
                | "XACK"
                | "XADD"
                | "XDEL"
                | "ZADD"
                | "ZREM"
        ),
        5 => matches!(
            name,
            "ARDEL"
                | "ARSET"
                | "BITOP"
                | "BLPOP"
                | "BRPOP"
                | "DELEX"
                | "GETEX"
                | "HMSET"
                | "LMOVE"
                | "LMPOP"
                | "LPUSH"
                | "LTRIM"
                | "PFADD"
                | "RPUSH"
                | "SETEX"
                | "SETNX"
                | "SMOVE"
                | "XNACK"
                | "XTRIM"
                | "ZMPOP"
        ),
        6 => matches!(
            name,
            "APPEND"
                | "ARMSET"
                | "ARRING"
                | "ARSEEK"
                | "BLMOVE"
                | "BLMPOP"
                | "BZMPOP"
                | "DECRBY"
                | "EXPIRE"
                | "GEOADD"
                | "GETDEL"
                | "GETSET"
                | "HGETEX"
                | "HSETEX"
                | "HSETNX"
                | "INCRBY"
                | "INCREX"
                | "LMOVEM"
                | "LPUSHX"
                | "MSETEX"
                | "MSETNX"
                | "PSETEX"
                | "RENAME"
                | "RPUSHX"
                | "SELECT"
                | "SETBIT"
                | "SWAPDB"
                | "TS.ADD"
                | "TS.DEL"
                | "UNLINK"
                | "XCLAIM"
                | "XDELEX"
                | "XGROUP"
                | "XSETID"
        ),
        7 => matches!(
            name,
            "FLUSHDB"
                | "HEXPIRE"
                | "HGETDEL"
                | "HINCRBY"
                | "HIMPORT"
                | "LINSERT"
                | "PERSIST"
                | "PEXPIRE"
                | "PFDEBUG"
                | "PFMERGE"
                | "RESTORE"
                | "TS.MADD"
                | "XACKDEL"
                | "XCFGSET"
                | "BLMOVEM"
                | "ZINCRBY"
                | "ZPOPMAX"
                | "ZPOPMIN"
        ),
        8 => matches!(
            name,
            "ARINSERT"
                | "BITFIELD"
                | "BZPOPMAX"
                | "BZPOPMIN"
                | "EXPIREAT"
                | "FLUSHALL"
                | "FT.ALTER"
                | "HPERSIST"
                | "HPEXPIRE"
                | "JSON.DEL"
                | "JSON.SET"
                | "RENAMENX"
                | "SETRANGE"
                | "TS.ALTER"
                | "VSETATTR"
        ),
        9 => matches!(
            name,
            "FT.CREATE"
                | "HEXPIREAT"
                | "JSON.MSET"
                | "PEXPIREAT"
                | "RPOPLPUSH"
                | "TS.CREATE"
                | "TS.DECRBY"
                | "TS.INCRBY"
        ),
        10 => matches!(
            name,
            "ARDELRANGE"
                | "BRPOPLPUSH"
                | "FT.DICTADD"
                | "FT.DICTDEL"
                | "HPEXPIREAT"
                | "JSON.CLEAR"
                | "JSON.MERGE"
                | "SDIFFSTORE"
                | "XAUTOCLAIM"
                | "XREADGROUP"
                | "ZDIFFSTORE"
        ),
        11 => matches!(
            name,
            "FT.ALIASADD"
                | "FT.ALIASDEL"
                | "INCRBYFLOAT"
                | "JSON.ARRPOP"
                | "JSON.FORGET"
                | "JSON.OBJDEL"
                | "JSON.TOGGLE"
                | "SINTERSTORE"
                | "SUNIONSTORE"
                | "XIDMPRECORD"
                | "ZINTERSTORE"
                | "ZRANGESTORE"
                | "ZUNIONSTORE"
        ),
        12 => matches!(
            name,
            "FT.DROPINDEX" | "FT.SYNUPDATE" | "HINCRBYFLOAT" | "JSON.ARRTRIM"
        ),
        13 => matches!(name, "TS.CREATERULE" | "TS.DELETERULE"),
        14 => matches!(
            name,
            "FT.ALIASUPDATE"
                | "GEOSEARCHSTORE"
                | "JSON.ARRAPPEND"
                | "JSON.ARRINSERT"
                | "JSON.NUMINCRBY"
                | "JSON.NUMMULTBY"
                | "JSON.STRAPPEND"
                | "ZREMRANGEBYLEX"
        ),
        15 => name == "ZREMRANGEBYRANK",
        16 => name == "ZREMRANGEBYSCORE",
        _ => false,
    }
}

/// The flat list this function's buckets are derived from, kept as the
/// authoritative reference for the equivalence test below.
/// The write-command names, for the bucketing equivalence test.
#[cfg(test)]
pub(crate) const WRITE_COMMANDS_FOR_TEST: &[&str] = &[
    "APPEND",
    "ARDEL",
    "ARDELRANGE",
    "ARINSERT",
    "ARMSET",
    "ARRING",
    "ARSEEK",
    "ARSET",
    "BITFIELD",
    "BITOP",
    "BLMOVE",
    "BLMOVEM",
    "BLMPOP",
    "BLPOP",
    "BRPOP",
    "BRPOPLPUSH",
    "BZMPOP",
    "BZPOPMAX",
    "BZPOPMIN",
    "COPY",
    "DECR",
    "DECRBY",
    "DEL",
    "DELEX",
    "EXPIRE",
    "EXPIREAT",
    "FLUSHALL",
    "FLUSHDB",
    "FT.ALIASADD",
    "FT.ALIASDEL",
    "FT.ALIASUPDATE",
    "FT.ALTER",
    "FT.CREATE",
    "FT.DICTADD",
    "FT.DICTDEL",
    "FT.DROPINDEX",
    "FT.SYNUPDATE",
    "GEOADD",
    "GEOSEARCHSTORE",
    "GETDEL",
    "GETEX",
    "GETSET",
    "HDEL",
    "HEXPIRE",
    "HEXPIREAT",
    "HGETDEL",
    "HGETEX",
    "HIMPORT",
    "HINCRBY",
    "HINCRBYFLOAT",
    "HMSET",
    "HPERSIST",
    "HPEXPIRE",
    "HPEXPIREAT",
    "HSET",
    "HSETEX",
    "HSETNX",
    "INCR",
    "INCRBY",
    "INCRBYFLOAT",
    "INCREX",
    "JSON.ARRAPPEND",
    "JSON.ARRINSERT",
    "JSON.ARRPOP",
    "JSON.ARRTRIM",
    "JSON.CLEAR",
    "JSON.DEL",
    "JSON.FORGET",
    "JSON.MERGE",
    "JSON.MSET",
    "JSON.NUMINCRBY",
    "JSON.NUMMULTBY",
    "JSON.OBJDEL",
    "JSON.SET",
    "JSON.STRAPPEND",
    "JSON.TOGGLE",
    "LINSERT",
    "LMOVE",
    "LMOVEM",
    "LMPOP",
    "LPOP",
    "LPUSH",
    "LPUSHX",
    "LREM",
    "LSET",
    "LTRIM",
    "MOVE",
    "MSET",
    "MSETEX",
    "MSETNX",
    "PERSIST",
    "PEXPIRE",
    "PEXPIREAT",
    "PFADD",
    "PFDEBUG",
    "PFMERGE",
    "PSETEX",
    "RENAME",
    "RENAMENX",
    "RESTORE",
    "RPOP",
    "RPOPLPUSH",
    "RPUSH",
    "RPUSHX",
    "SADD",
    "SDIFFSTORE",
    "SELECT",
    "SET",
    "SETBIT",
    "SETEX",
    "SETNX",
    "SETRANGE",
    "SINTERSTORE",
    "SMOVE",
    "SORT",
    "SPOP",
    "SREM",
    "SUNIONSTORE",
    "SWAPDB",
    "TS.ADD",
    "TS.ALTER",
    "TS.CREATE",
    "TS.CREATERULE",
    "TS.DECRBY",
    "TS.DEL",
    "TS.DELETERULE",
    "TS.INCRBY",
    "TS.MADD",
    "UNLINK",
    "VADD",
    "VREM",
    "VSETATTR",
    "XACK",
    "XACKDEL",
    "XADD",
    "XAUTOCLAIM",
    "XCFGSET",
    "XIDMPRECORD",
    "XCLAIM",
    "XDEL",
    "XDELEX",
    "XGROUP",
    "XNACK",
    "XREADGROUP",
    "XSETID",
    "XTRIM",
    "ZADD",
    "ZDIFFSTORE",
    "ZINCRBY",
    "ZINTERSTORE",
    "ZMPOP",
    "ZPOPMAX",
    "ZPOPMIN",
    "ZRANGESTORE",
    "ZREM",
    "ZREMRANGEBYLEX",
    "ZREMRANGEBYRANK",
    "ZREMRANGEBYSCORE",
    "ZUNIONSTORE",
];

#[cfg(test)]
fn is_write_command_reference(name: &str) -> bool {
    matches!(
        name,
        // Strings
        "SET" | "SETNX" | "SETEX" | "PSETEX" | "MSET" | "MSETNX" | "GETSET" |
        "GETDEL" | "GETEX" | "APPEND" | "INCR" | "DECR" | "INCRBY" | "DECRBY" |
        "INCRBYFLOAT" | "SETRANGE" | "INCREX" |
        // Redis 8.4 string commands. These executed on the master but were in
        // neither list, so they never reached a replica or the AOF.
        "DELEX" | "MSETEX" |
        // Keys / generics
        "DEL" | "UNLINK" | "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" |
        "PERSIST" | "RESTORE" | "RENAME" | "RENAMENX" | "COPY" | "MOVE" | "SWAPDB" | "SORT" |
        // Lists
        "LPUSH" | "RPUSH" | "LPUSHX" | "RPUSHX" | "LPOP" | "RPOP" |
        "LSET" | "LINSERT" | "LREM" | "LTRIM" | "LMOVE" | "RPOPLPUSH" |
        "LMPOP" | "BLPOP" | "BRPOP" | "BLMOVE" | "BLMPOP" | "BRPOPLPUSH" |
        "LMOVEM" | "BLMOVEM" |
        // Sets
        "SADD" | "SREM" | "SPOP" | "SMOVE" | "SUNIONSTORE" | "SINTERSTORE" |
        "SDIFFSTORE" |
        // Sorted sets
        "ZADD" | "ZREM" | "ZINCRBY" | "ZPOPMIN" | "ZPOPMAX" | "ZRANGESTORE" |
        "ZDIFFSTORE" | "ZUNIONSTORE" | "ZINTERSTORE" | "ZMPOP" | "BZMPOP" |
        "BZPOPMIN" | "BZPOPMAX" |
        "ZREMRANGEBYLEX" | "ZREMRANGEBYRANK" | "ZREMRANGEBYSCORE" |
        // Hashes
        "HSET" | "HMSET" | "HSETNX" | "HDEL" | 
        "HINCRBY" | "HINCRBYFLOAT" | "HEXPIRE" | "HPEXPIRE" | "HEXPIREAT" | "HPEXPIREAT" | "HPERSIST" |
        "HGETDEL" | "HGETEX" | "HSETEX" | "HIMPORT" |
        // Streams. The claim/read-group commands are here because they move
        // entries between consumers' pending lists — state a promoted replica
        // has to have, or a failover silently reassigns or redelivers work.
        "XADD" | "XDEL" | "XTRIM" | "XSETID" | "XGROUP" | "XACK" | "XDELEX" |
        "XNACK" | "XACKDEL" | "XCFGSET" | "XIDMPRECORD" | "XCLAIM" | "XAUTOCLAIM" | "XREADGROUP" |
        // JSON
        "JSON.SET" | "JSON.DEL" | "JSON.MSET" | "JSON.NUMINCRBY" |
        "JSON.NUMMULTBY" | "JSON.STRAPPEND" | "JSON.ARRAPPEND" | "JSON.ARRINSERT" |
        "JSON.ARRPOP" | "JSON.ARRTRIM" | "JSON.OBJDEL" | "JSON.CLEAR" |
        "JSON.TOGGLE" | "JSON.MERGE" | "JSON.FORGET" |
        // TimeSeries
        "TS.ADD" | "TS.MADD" | "TS.INCRBY" | "TS.DECRBY" | "TS.CREATE" |
        "TS.DEL" | "TS.ALTER" | "TS.CREATERULE" | "TS.DELETERULE" |
        // Vector sets
        "VADD" | "VREM" | "VSETATTR" |
        // FT (index mutations)
        "FT.CREATE" | "FT.ALTER" | "FT.DROPINDEX" | "FT.ALIASADD" |
        "FT.ALIASDEL" | "FT.ALIASUPDATE" | "FT.SYNUPDATE" |
        "FT.DICTADD" | "FT.DICTDEL" |
        // Bitmaps / HLL / Geo
        "SETBIT" | "BITOP" | "BITFIELD" | "PFADD" | "PFDEBUG" | "PFMERGE" |
        "GEOADD" | "GEOSEARCHSTORE" |
        // Arrays (Redis 8.8). ARSEEK counts as a write: it moves the insert
        // cursor, which is persisted state a replica must mirror or its
        // ARINSERT/ARNEXT would diverge from the master's.
        "ARSET" | "ARMSET" | "ARDEL" | "ARDELRANGE" | "ARINSERT" | "ARRING" |
        "ARSEEK" |
        // Persistence (master still propagates these)
        "FLUSHDB" | "FLUSHALL" | "SELECT"
    )
}

/// True if this command may not run on a read-only replica (READONLY reply).
#[inline]
pub fn is_denied_on_readonly_replica(name: &str, cmd: &[&[u8]]) -> bool {
    match name {
        // `SELECT` is in `is_write_command` because the replication stream and
        // the AOF both need it propagated, not because it writes anything — it
        // changes per-connection state only. Conflating "must be propagated"
        // with "a read-only replica must reject it" made every database except
        // 0 unreachable on a replica: any client configured with `db=N` sends
        // SELECT on connect and got
        // `READONLY You can't write against a read only replica.` back, so it
        // could not even read. Redis allows SELECT on a replica.
        "SELECT" => false,
        // As Redis: a script runs on a replica, and a write it makes is
        // refused there (`run_from_script`). One that declares it may write
        // (a shebang without `no-writes`, a function without it) is refused
        // before it runs. Every script was, the ones that only read too
        // (#145).
        "EVAL" | "EVALSHA" | "FCALL" => script_may_write(name, cmd),
        "GEORADIUS" | "GEORADIUSBYMEMBER" => georadius_stores(cmd),
        // Only TODENSE and RESET rewrite the value; GETREG, DECODE and
        // ENCODING are diagnostics, deliberately read-only here.
        "PFDEBUG" => pfdebug_mutates(cmd),
        // MIGRATE removes keys from this node.
        "MIGRATE" => true,
        // Only HIMPORT SET writes; PREPARE / DISCARD are session state.
        "HIMPORT" => crate::commands::hash_import::is_write(cmd),
        "SCRIPT" if cmd.len() >= 2 => {
            let sub = cmd[1];
            matches!(
                std::str::from_utf8(sub)
                    .ok()
                    .map(|s| s.to_ascii_uppercase())
                    .as_deref(),
                Some("LOAD") | Some("FLUSH") | Some("RESET")
            )
        }
        "FUNCTION" if cmd.len() >= 2 => {
            matches!(
                std::str::from_utf8(cmd[1])
                    .ok()
                    .map(|s| s.to_ascii_uppercase())
                    .as_deref(),
                Some("LOAD") | Some("DELETE") | Some("FLUSH") | Some("RESTORE")
            )
        }
        _ => is_write_command(name),
    }
}

/// Whether `EVAL`, `EVALSHA` or `FCALL` runs something that has declared it
/// may write. See [`is_denied_on_readonly_replica`].
fn script_may_write(name: &str, cmd: &[&[u8]]) -> bool {
    #[cfg(feature = "lua")]
    {
        let Some(target) = cmd.get(1) else {
            return false;
        };
        match name {
            "EVAL" => crate::commands::script_declares_writes(target),
            "EVALSHA" => crate::commands::cached_script_declares_writes(target),
            _ => !crate::commands::function_declares_no_writes(target),
        }
    }
    #[cfg(not(feature = "lua"))]
    {
        let _ = (name, cmd);
        false
    }
}

/// `GEORADIUS` / `GEORADIUSBYMEMBER` are reads unless they carry `STORE` or
/// `STOREDIST`, which write the result set to another key.
fn georadius_stores(cmd: &[&[u8]]) -> bool {
    cmd.iter()
        .skip(1)
        .any(|a| a.eq_ignore_ascii_case(b"STORE") || a.eq_ignore_ascii_case(b"STOREDIST"))
}

/// `PFDEBUG` is a write only for the subcommands that rewrite the value.
/// `GETREG` used to be listed here: Redis converts a sparse value to dense
/// while printing the registers, but this implementation returns the registers
/// without changing the stored encoding.
fn pfdebug_mutates(cmd: &[&[u8]]) -> bool {
    cmd.get(1).is_some_and(|sub| {
        sub.eq_ignore_ascii_case(b"TODENSE") || sub.eq_ignore_ascii_case(b"RESET")
    })
}

/// True if the full command should be replicated to replicas / counted as a mutating write.
#[inline]
pub fn command_mutates_for_replication(name: &str, cmd: &[&[u8]]) -> bool {
    match name {
        "EVAL_RO" | "EVALSHA_RO" | "FCALL_RO" => false,
        // Writes replicate as inner `redis.call` commands, not as `EVAL` / `EVALSHA` / `FCALL`.
        "EVAL" | "EVALSHA" | "FCALL" => false,
        "GEORADIUS" | "GEORADIUSBYMEMBER" => georadius_stores(cmd),
        "PFDEBUG" => pfdebug_mutates(cmd),
        // Propagated as the `DEL` of whatever it moved away; see
        // `script_feed::commands_for_propagation`.
        "MIGRATE" => true,
        "HIMPORT" => crate::commands::hash_import::is_write(cmd),
        "SCRIPT" if cmd.len() >= 2 => {
            let sub = cmd[1];
            matches!(
                std::str::from_utf8(sub)
                    .ok()
                    .map(|s| s.to_ascii_uppercase())
                    .as_deref(),
                Some("LOAD") | Some("FLUSH") | Some("RESET")
            )
        }
        "FUNCTION" if cmd.len() >= 2 => {
            matches!(
                std::str::from_utf8(cmd[1])
                    .ok()
                    .map(|s| s.to_ascii_uppercase())
                    .as_deref(),
                Some("LOAD") | Some("DELETE") | Some("FLUSH") | Some("RESTORE")
            )
        }
        _ => is_write_command(name),
    }
}

/// Write commands blocked inside an `EVAL_RO` / `EVALSHA_RO` script (`redis.call`).
#[inline]
pub fn inner_command_blocked_in_readonly_script(name: &str, cmd: &[&[u8]]) -> bool {
    match name {
        "EVAL" | "EVALSHA" | "EVAL_RO" | "EVALSHA_RO" | "FCALL" | "FCALL_RO" => true,
        // Not writes to the dataset, but to the replication stream — or,
        // `PFCOUNT`, possibly to its key: not what a script that has said it
        // writes nothing may do.
        "PUBLISH" | "SPUBLISH" | "PFCOUNT" => true,
        "GEORADIUS" | "GEORADIUSBYMEMBER" => georadius_stores(cmd),
        "PFDEBUG" => pfdebug_mutates(cmd),
        "MIGRATE" => true,
        "HIMPORT" => crate::commands::hash_import::is_write(cmd),
        "SCRIPT" if cmd.len() >= 2 => {
            let sub = cmd[1];
            matches!(
                std::str::from_utf8(sub)
                    .ok()
                    .map(|s| s.to_ascii_uppercase())
                    .as_deref(),
                Some("LOAD") | Some("FLUSH") | Some("RESET")
            )
        }
        "FUNCTION" if cmd.len() >= 2 => {
            matches!(
                std::str::from_utf8(cmd[1])
                    .ok()
                    .map(|s| s.to_ascii_uppercase())
                    .as_deref(),
                Some("LOAD") | Some("DELETE") | Some("FLUSH") | Some("RESTORE")
            )
        }
        _ => is_write_command(name),
    }
}

#[cfg(test)]
mod replication_invariants_tests {
    use super::{
        HubMsg, ReplPayload, ReplState, propagate, slave_command_allowed_during_loading, start_hub,
    };
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    async fn await_connected(state: &ReplState, want: i64) {
        for _ in 0..5000 {
            if state.connected_replicas.load(Ordering::Relaxed) == want {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!(
            "timed out waiting connected_replicas={want}, got {}",
            state.connected_replicas.load(Ordering::Relaxed)
        );
    }

    /// RegisterChannel must bump `connected_slaves` inside the hub task (pairs with Deregister).
    #[tokio::test]
    async fn hub_register_channel_increments_connected_replicas() {
        let state = Arc::new(ReplState::new_master());
        let hub = start_hub(Arc::clone(&state));
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<ReplPayload>();

        hub.send(HubMsg::RegisterChannel {
            addr: "test-replica".into(),
            peer_ip: "127.0.0.1".into(),
            peer_port: 9_001,
            tx,
            replica_id: 9001,
            pending_bytes: Arc::new(AtomicU64::new(0)),
            base: 0,
        })
        .unwrap();

        await_connected(&state, 1).await;
        hub.send(HubMsg::Deregister(9001)).unwrap();
        await_connected(&state, 0).await;
    }

    /// Second Deregister must not decrement again (would drive count negative otherwise).
    #[tokio::test]
    async fn hub_second_deregister_is_idempotent_for_counter() {
        let state = Arc::new(ReplState::new_master());
        let hub = start_hub(Arc::clone(&state));
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<ReplPayload>();

        hub.send(HubMsg::RegisterChannel {
            addr: "test-replica".into(),
            peer_ip: "127.0.0.1".into(),
            peer_port: 9_002,
            tx,
            replica_id: 9002,
            pending_bytes: Arc::new(AtomicU64::new(0)),
            base: 0,
        })
        .unwrap();

        await_connected(&state, 1).await;
        hub.send(HubMsg::Deregister(9002)).unwrap();
        hub.send(HubMsg::Deregister(9002)).unwrap();
        await_connected(&state, 0).await;

        assert!(
            state.connected_replicas.load(Ordering::Relaxed) >= 0,
            "connected_replicas must not go negative after duplicate teardown"
        );
    }

    /// Dropping the replica receiver must make the next propagate evict accounting once.
    #[tokio::test]
    async fn hub_propagate_drops_replica_when_channel_closed() {
        let state = Arc::new(ReplState::new_master());
        let hub = start_hub(Arc::clone(&state));
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<ReplPayload>();

        hub.send(HubMsg::RegisterChannel {
            addr: "test-replica".into(),
            peer_ip: "127.0.0.1".into(),
            peer_port: 9_003,
            tx,
            replica_id: 9003,
            pending_bytes: Arc::new(AtomicU64::new(0)),
            base: 0,
        })
        .unwrap();

        await_connected(&state, 1).await;
        drop(rx);

        propagate(&hub, vec![0xAB, 0xCD]); // bogus RESP bytes — delivery attempt still runs send()
        await_connected(&state, 0).await;
        assert!(
            state.replica_acks.lock().get(&9003).is_none(),
            "dead replica ACK row should be removed"
        );
    }

    /// A replica whose queue crosses the configured byte cap must be evicted —
    /// the hub drops its `tx`, closing the forwarder's channel — rather than the
    /// backlog growing without bound for a replica that never drains it.
    #[tokio::test]
    async fn hub_propagate_evicts_replica_exceeding_backlog_limit() {
        let state = Arc::new(ReplState::new_master());
        const TINY_LIMIT: u64 = 100; // bytes
        // Both limits: the replica is registered as in a full sync.
        state
            .replica_max_backlog_bytes
            .store(TINY_LIMIT, Ordering::Relaxed);
        state
            .replica_sync_max_backlog_bytes
            .store(TINY_LIMIT, Ordering::Relaxed);
        let hub = start_hub(Arc::clone(&state));
        // Receiver is never drained — simulates a replica that can't keep up.
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<ReplPayload>();

        hub.send(HubMsg::RegisterChannel {
            addr: "slow-replica".into(),
            peer_ip: "127.0.0.1".into(),
            peer_port: 9_004,
            tx,
            replica_id: 9004,
            pending_bytes: Arc::new(AtomicU64::new(0)),
            base: 0,
        })
        .unwrap();
        await_connected(&state, 1).await;

        // Each propagated payload is well under TINY_LIMIT alone, but the queue
        // is never drained, so the cumulative backlog crosses it within a few sends.
        for _ in 0..10 {
            propagate(&hub, vec![0u8; 50]);
        }

        await_connected(&state, 0).await;
        assert!(
            state.replica_acks.lock().get(&9004).is_none(),
            "evicted replica's ACK row should be removed"
        );
    }

    async fn barrier(hub: &super::HubTx) {
        let (done, wait) = tokio::sync::oneshot::channel();
        hub.send(HubMsg::Barrier(done)).unwrap();
        wait.await.unwrap();
    }

    fn register_syncing(hub: &super::HubTx, id: u64, pending: &Arc<AtomicU64>) -> ReplRx {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<ReplPayload>();
        hub.send(HubMsg::RegisterChannel {
            addr: format!("replica-{id}"),
            peer_ip: "127.0.0.1".into(),
            peer_port: id as u16,
            tx,
            replica_id: id,
            pending_bytes: Arc::clone(pending),
            base: 0,
        })
        .unwrap();
        rx
    }

    type ReplRx = tokio::sync::mpsc::UnboundedReceiver<ReplPayload>;

    /// A replica in its full sync reads nothing of the stream until it has
    /// loaded the dataset, so what is written meanwhile waits in its queue:
    /// it is held to `repl-replica-sync-max-backlog-bytes`, not to the
    /// streaming limit (#305). Once it has acknowledged the dataset and
    /// drained its queue, the streaming limit holds.
    #[tokio::test]
    async fn a_full_sync_has_its_own_queue_limit_until_the_replica_streams() {
        let state = Arc::new(ReplState::new_master());
        state
            .replica_max_backlog_bytes
            .store(100, Ordering::Relaxed);
        state
            .replica_sync_max_backlog_bytes
            .store(1000, Ordering::Relaxed);
        let hub = start_hub(Arc::clone(&state));
        let pending = Arc::new(AtomicU64::new(0));
        let mut rx = register_syncing(&hub, 9005, &pending);
        await_connected(&state, 1).await;

        // 500 bytes while it loads: over the streaming limit, under the sync's.
        for _ in 0..10 {
            propagate(&hub, vec![0u8; 50]);
        }
        barrier(&hub).await;
        assert_eq!(state.connected_replicas.load(Ordering::Relaxed), 1);
        assert_eq!(state.replica(9005).unwrap().queued(), 500);

        // Loaded and acknowledged; its connection takes what was queued.
        hub.send(HubMsg::AckOffset {
            replica_id: 9005,
            offset: 0,
        })
        .unwrap();
        while rx.try_recv().is_ok() {}
        pending.store(0, Ordering::Relaxed);
        // The next write finds it streaming (its queue is a quarter of the
        // streaming limit or less)...
        propagate(&hub, vec![0u8; 20]);
        barrier(&hub).await;
        assert_eq!(state.connected_replicas.load(Ordering::Relaxed), 1);
        // ...and from then on the streaming limit holds.
        for _ in 0..3 {
            propagate(&hub, vec![0u8; 50]);
        }
        await_connected(&state, 0).await;
    }

    /// The sync's allowance is a bound too: the master's memory stays bounded
    /// for a replica that never finishes loading.
    #[tokio::test]
    async fn a_full_sync_past_its_queue_limit_is_dropped() {
        let state = Arc::new(ReplState::new_master());
        state
            .replica_max_backlog_bytes
            .store(100, Ordering::Relaxed);
        state
            .replica_sync_max_backlog_bytes
            .store(1000, Ordering::Relaxed);
        let hub = start_hub(Arc::clone(&state));
        let pending = Arc::new(AtomicU64::new(0));
        let _rx = register_syncing(&hub, 9006, &pending);
        await_connected(&state, 1).await;
        for _ in 0..19 {
            propagate(&hub, vec![0u8; 50]);
        }
        barrier(&hub).await;
        assert_eq!(state.connected_replicas.load(Ordering::Relaxed), 1);
        // `CONFIG SET` applies to a replica already attached.
        state
            .replica_sync_max_backlog_bytes
            .store(500, Ordering::Relaxed);
        propagate(&hub, vec![0u8; 50]);
        await_connected(&state, 0).await;
    }

    #[test]
    fn slave_loading_whitelist_allows_ping_not_get() {
        assert!(slave_command_allowed_during_loading("PING"));
        assert!(slave_command_allowed_during_loading("INFO"));
        assert!(!slave_command_allowed_during_loading("GET"));
        assert!(!slave_command_allowed_during_loading("SET"));
    }

    // ── Replication epoch ───────────────────────────────────────────────────

    /// Each spawn claims a strictly newer epoch, so an older task can always
    /// tell it has been superseded.
    #[test]
    fn begin_replication_epoch_supersedes_previous_claim() {
        let s = ReplState::new_master();
        let first = s.begin_replication_epoch();
        assert_eq!(s.generation(), first);

        let second = s.begin_replication_epoch();
        assert!(second > first, "epoch must advance: {first} → {second}");
        // The first task now observes a generation that isn't its own.
        assert_ne!(s.generation(), first);
        assert_eq!(s.generation(), second);
    }

    /// Promotion must supersede a running task too — this is the failover case
    /// where the replica's master is gone and there is no I/O error to fall out
    /// on.
    #[test]
    fn become_master_supersedes_running_replica_task() {
        let s = ReplState::new_master();
        let epoch = s.begin_replication_epoch();
        s.set_master_addr("10.0.0.1", 6379);
        s.become_slave();

        s.become_master();
        assert_ne!(
            s.generation(),
            epoch,
            "promotion must invalidate the in-flight replication epoch"
        );
        assert_eq!(
            s.master_addr(),
            None,
            "promotion must clear the recorded master"
        );
    }

    /// Re-pointing while staying "slave" throughout — the case the old
    /// role-only exit check could never detect.
    #[test]
    fn repoint_between_masters_supersedes_without_role_change() {
        let s = ReplState::new_master();
        let first_epoch = s.begin_replication_epoch();
        s.set_master_addr("10.0.0.1", 6379);
        s.become_slave();

        // A second start_replica for a different master.
        let second_epoch = s.begin_replication_epoch();
        s.set_master_addr("10.0.0.2", 6379);

        assert_eq!(s.role(), "slave", "role never left slave in this scenario");
        assert_ne!(s.generation(), first_epoch, "old task must see it is stale");
        assert_eq!(s.generation(), second_epoch);
        assert_eq!(s.master_addr(), Some(("10.0.0.2".to_string(), 6379)));
    }

    /// `is_already_following` gates the REPLICAOF no-op, so it must be exact
    /// about port and only true while actually a slave.
    #[test]
    fn is_already_following_matches_only_the_current_target() {
        let s = ReplState::new_master();
        s.set_master_addr("10.0.0.1", 6379);

        // Recorded address but still a master → not following anything.
        assert!(!s.is_already_following("10.0.0.1", 6379));

        s.become_slave();
        assert!(s.is_already_following("10.0.0.1", 6379));
        // Hostnames are case-insensitive; ports are not fuzzy.
        assert!(s.is_already_following("10.0.0.1", 6379));
        assert!(!s.is_already_following("10.0.0.1", 6380));
        assert!(!s.is_already_following("10.0.0.2", 6379));

        s.become_master();
        assert!(!s.is_already_following("10.0.0.1", 6379));
    }

    /// The length-bucketed `is_write_command` must be *exactly* equivalent to the
    /// flat list it was generated from.
    ///
    /// `SELECT` must stay propagated *and* stay allowed on a read-only replica.
    ///
    /// It is in `is_write_command` so the replication stream and the AOF carry
    /// the db switch, but it mutates nothing. Denying it made every database
    /// except 0 unreachable on a replica — a client configured with `db=N`
    /// sends SELECT on connect and could not even read. The two predicates have
    /// to disagree about this one command, so pin both sides.
    #[test]
    fn select_is_propagated_but_allowed_on_a_replica() {
        assert!(
            super::is_write_command("SELECT"),
            "SELECT must still be propagated, or a replica applies writes to the wrong db"
        );
        assert!(
            !super::is_denied_on_readonly_replica("SELECT", &[b"SELECT", b"3"]),
            "SELECT must be allowed on a read-only replica"
        );
        // The commands it sits next to are genuine writes and must stay denied.
        for name in ["SET", "SWAPDB", "SETBIT", "FLUSHDB", "MOVE"] {
            assert!(
                super::is_denied_on_readonly_replica(name, &[name.as_bytes()]),
                "{name} must stay denied on a read-only replica"
            );
        }
    }

    /// This is the safety net for the bucketing optimisation: a command moved to
    /// the wrong length arm, or dropped while editing, would silently stop
    /// replicating — data loss on a replica, with no test failing anywhere else.
    #[test]
    fn is_write_command_matches_reference() {
        // Every name the reference list itself contains.
        for name in super::WRITE_COMMANDS_FOR_TEST {
            assert!(
                super::is_write_command(name),
                "{name} is in the reference list but bucketed lookup says not a write"
            );
            assert!(
                super::is_write_command_reference(name),
                "{name} extraction bug: not in reference"
            );
        }
        // Every command the server knows about, write or not.
        for name in crate::commands::server::all_commands_for_test() {
            assert_eq!(
                super::is_write_command(name),
                super::is_write_command_reference(name),
                "disagreement on {name}"
            );
        }
        // Edge cases: empty, wrong case, near-misses, over-long.
        for name in [
            "",
            "G",
            "GE",
            "GET",
            "SE",
            "SET",
            "SETX",
            "set",
            "Set",
            "DELE",
            "DE",
            "ARSETX",
            "ARSE",
            "INCREXX",
            "XNACKK",
            "ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ",
        ] {
            assert_eq!(
                super::is_write_command(name),
                super::is_write_command_reference(name),
                "disagreement on edge case {name:?}"
            );
        }
    }

    /// The most common read must not be misclassified — this is the case the
    /// bucketing exists to make cheap.
    #[test]
    fn common_reads_are_not_write_commands() {
        for name in [
            "GET", "MGET", "EXISTS", "TTL", "TYPE", "SCAN", "PING", "INFO",
        ] {
            assert!(!super::is_write_command(name), "{name} must not replicate");
        }
    }

    /// Commands that were implemented but missing from the write list ran on
    /// the master and went nowhere: not to replicas, not to the AOF. Found by
    /// diffing a replica against its master after `ZREMRANGEBYSCORE`, `MSETEX`
    /// and `BRPOPLPUSH`.
    #[test]
    fn previously_unpropagated_writes_are_now_writes() {
        for cmd in [
            "BRPOPLPUSH",
            "DELEX",
            "MSETEX",
            "GEOSEARCHSTORE",
            "JSON.MERGE",
            "JSON.FORGET",
            "FT.DICTADD",
            "FT.DICTDEL",
            "XACKDEL",
            "XCFGSET",
            "XIDMPRECORD",
            "XCLAIM",
            "XAUTOCLAIM",
            "XREADGROUP",
            "ZREMRANGEBYLEX",
            "ZREMRANGEBYRANK",
            "ZREMRANGEBYSCORE",
        ] {
            assert!(super::is_write_command(cmd), "{cmd} must replicate");
            assert!(
                super::is_denied_on_readonly_replica(cmd, &[cmd.as_bytes()]),
                "{cmd} must be refused on a read-only replica"
            );
        }
    }

    /// GEORADIUS is a read until it is asked to store its result.
    #[test]
    fn georadius_is_a_write_only_with_store() {
        let read: Vec<&[u8]> = vec![b"GEORADIUS", b"k", b"0", b"0", b"1", b"km"];
        let store: Vec<&[u8]> = vec![b"GEORADIUS", b"k", b"0", b"0", b"1", b"km", b"store", b"d"];
        let storedist: Vec<&[u8]> = vec![
            b"GEORADIUSBYMEMBER",
            b"k",
            b"m",
            b"1",
            b"km",
            b"STOREDIST",
            b"d",
        ];
        assert!(!super::command_mutates_for_replication("GEORADIUS", &read));
        assert!(!super::is_denied_on_readonly_replica("GEORADIUS", &read));
        assert!(super::command_mutates_for_replication("GEORADIUS", &store));
        assert!(super::is_denied_on_readonly_replica("GEORADIUS", &store));
        assert!(super::command_mutates_for_replication(
            "GEORADIUSBYMEMBER",
            &storedist
        ));
    }

    /// `PFDEBUG` is a write command by name, but only `TODENSE` and `RESET`
    /// rewrite the stored HLL. The diagnostic subcommands must stay readable
    /// on a replica and inside a read-only script, and must not replicate.
    #[test]
    fn pfdebug_is_a_write_only_when_it_rewrites() {
        assert!(super::is_write_command("PFDEBUG"));
        for sub in ["GETREG", "DECODE", "ENCODING"] {
            let cmd: Vec<&[u8]> = vec![b"PFDEBUG", sub.as_bytes(), b"k"];
            assert!(
                !super::command_mutates_for_replication("PFDEBUG", &cmd),
                "PFDEBUG {sub} must not propagate"
            );
            assert!(
                !super::is_denied_on_readonly_replica("PFDEBUG", &cmd),
                "PFDEBUG {sub} must be allowed on a read-only replica"
            );
            assert!(
                !super::inner_command_blocked_in_readonly_script("PFDEBUG", &cmd),
                "PFDEBUG {sub} must be allowed in a read-only script"
            );
        }
        for sub in ["TODENSE", "RESET", "todense"] {
            let cmd: Vec<&[u8]> = vec![b"PFDEBUG", sub.as_bytes(), b"k"];
            assert!(
                super::command_mutates_for_replication("PFDEBUG", &cmd),
                "PFDEBUG {sub} must propagate"
            );
            assert!(
                super::is_denied_on_readonly_replica("PFDEBUG", &cmd),
                "PFDEBUG {sub} must be refused on a read-only replica"
            );
            assert!(
                super::inner_command_blocked_in_readonly_script("PFDEBUG", &cmd),
                "PFDEBUG {sub} must be refused in a read-only script"
            );
        }
    }

    /// The PING the master writes into an idle stream must be exactly one
    /// well-formed RESP command, or the replica's parser would stall on it.
    #[test]
    fn repl_ping_is_one_complete_resp_command() {
        let (consumed, cmd) =
            crate::resp::parse::parse_resp_command(super::REPL_PING).expect("parses");
        assert_eq!(consumed, super::REPL_PING.len());
        assert_eq!(cmd.len(), 1);
        assert!(cmd[0].eq_ignore_ascii_case(b"PING"));
    }

    /// A replica's ACK counts from the offset its FULLRESYNC announced, so
    /// "caught up" has to be judged against what *that replica* was sent, not
    /// the global offset.
    #[test]
    fn replica_progress_measures_delivery_from_its_own_base() {
        let p = super::ReplicaProgress::new(1_000);
        assert_eq!(p.delivered(), 0);
        assert_eq!(p.behind_bytes(), 0);

        p.sent.store(300, Ordering::Relaxed);
        assert_eq!(p.behind_bytes(), 300);

        p.acked.store(1_200, Ordering::Relaxed);
        assert_eq!(p.delivered(), 200);
        assert_eq!(p.behind_bytes(), 100);

        p.acked.store(1_300, Ordering::Relaxed);
        assert_eq!(p.behind_bytes(), 0);
    }

    /// A write that lands between a replica's FULLRESYNC reply and its hub
    /// registration advances the global offset without ever being sent to that
    /// replica. Judged against the global offset it could never catch up;
    /// judged against its own `sent` counter it is caught up immediately.
    #[tokio::test]
    async fn wait_targets_ignore_writes_the_replica_was_never_sent() {
        let state = Arc::new(ReplState::new_master());
        let hub = start_hub(Arc::clone(&state));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ReplPayload>();

        // FULLRESYNC announced offset 0, then 500 bytes of writes happened
        // before the replica registered.
        state.repl_offset.store(500, Ordering::Relaxed);
        hub.send(HubMsg::RegisterChannel {
            addr: "late-replica".into(),
            peer_ip: "127.0.0.1".into(),
            peer_port: 9_005,
            tx,
            replica_id: 9005,
            pending_bytes: Arc::new(AtomicU64::new(0)),
            base: 0,
        })
        .unwrap();
        await_connected(&state, 1).await;

        // Nothing sent yet: the replica is already at its target.
        let targets = state.wait_targets();
        assert_eq!(ReplState::count_reached(&targets), 1);

        // One 10-byte write goes out; the barrier guarantees the hub has
        // fanned it out before we look.
        propagate(&hub, vec![0u8; 10]);
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        hub.send(HubMsg::Barrier(done_tx)).unwrap();
        done_rx.await.unwrap();
        assert_eq!(rx.recv().await.unwrap().len(), 10);

        let targets = state.wait_targets();
        assert_eq!(ReplState::count_reached(&targets), 0, "not acked yet");

        // The replica acks base + bytes received = 10, far below the global
        // offset of 500, and that is still fully caught up.
        hub.send(HubMsg::AckOffset {
            replica_id: 9005,
            offset: 10,
        })
        .unwrap();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        hub.send(HubMsg::Barrier(done_tx)).unwrap();
        done_rx.await.unwrap();
        assert_eq!(ReplState::count_reached(&targets), 1);
    }

    /// A master that becomes a replica keeps where its own stream stands as
    /// where its dataset stands: its replica, promoted, continues it from
    /// there. Not when the stream did not count every write.
    #[test]
    fn a_demoted_master_keeps_its_own_stream_position() {
        let s = ReplState::new_master();
        s.backlog_active.store(true, Ordering::Relaxed);
        s.repl_offset.store(1234, Ordering::Relaxed);
        let id = s.repl_id();
        s.become_slave();
        let point = s.resync_point.lock().clone().expect("a resync point");
        assert_eq!((point.repl_id.as_str(), point.offset), (id.as_str(), 1234));
        assert_ne!(s.repl_id(), id);
        // Already a replica: a re-point keeps what it has.
        s.become_slave();
        assert_eq!(s.resync_point.lock().as_ref().map(|p| p.offset), Some(1234));

        let quiet = ReplState::new_master();
        quiet.repl_offset.store(99, Ordering::Relaxed);
        quiet.become_slave();
        assert!(quiet.resync_point.lock().is_none());
    }

    /// Link state drives both `ROLE` and `master_link_status`; "up" must mean
    /// synced and streaming, not merely "a socket exists".
    #[test]
    fn link_is_up_only_when_connected() {
        use super::LinkState;
        let s = ReplState::new_master();
        assert_eq!(s.link_state(), LinkState::Connect);
        assert!(!s.slave_link_up.load(Ordering::Acquire));

        for not_up in [LinkState::Connecting, LinkState::Sync] {
            s.set_link_state(not_up);
            assert!(!s.slave_link_up.load(Ordering::Acquire), "{not_up:?}");
            assert_eq!(s.master_last_io_seconds_ago(), -1);
            assert!(s.master_link_down_since_seconds().is_some());
        }

        s.set_link_state(LinkState::Connected);
        s.note_master_io();
        assert!(s.slave_link_up.load(Ordering::Acquire));
        assert_eq!(s.master_last_io_seconds_ago(), 0);
        assert_eq!(s.master_link_down_since_seconds(), None);

        s.set_link_state(LinkState::Connect);
        assert!(!s.slave_link_up.load(Ordering::Acquire));
        assert_eq!(s.master_last_io_seconds_ago(), -1);
    }

    /// Array writes must propagate to replicas, reads must not.
    #[test]
    fn array_write_commands_are_replicated() {
        for cmd in [
            "ARSET",
            "ARMSET",
            "ARDEL",
            "ARDELRANGE",
            "ARINSERT",
            "ARRING",
            "ARSEEK",
        ] {
            assert!(super::is_write_command(cmd), "{cmd} must replicate");
        }
        for cmd in [
            "ARGET",
            "ARMGET",
            "ARGETRANGE",
            "ARLEN",
            "ARCOUNT",
            "ARNEXT",
            "ARSCAN",
            "ARGREP",
            "AROP",
            "ARLASTITEMS",
            "ARINFO",
        ] {
            assert!(!super::is_write_command(cmd), "{cmd} must not replicate");
        }
    }
}

#[cfg(test)]
mod backlog_tests {
    use super::*;

    #[test]
    fn the_backlog_serves_what_follows_an_offset_while_it_still_has_it() {
        let mut b = Backlog::new();
        assert_eq!(
            b.after(0),
            None,
            "nothing to continue from before a full sync"
        );
        b.push(b"ignored", 1 << 20);
        assert_eq!(b.end, 0);

        b.align(100);
        assert_eq!((b.start(), b.end), (100, 100));
        assert_eq!(b.after(100), Some(Vec::new()));
        b.push(b"abcde", 1 << 20);
        b.push(b"fgh", 1 << 20);
        assert_eq!((b.start(), b.end), (100, 108));
        assert_eq!(b.after(100).unwrap(), b"abcdefgh");
        assert_eq!(b.after(105).unwrap(), b"fgh");
        assert_eq!(b.after(108).unwrap(), b"");
        assert_eq!(b.after(99), None, "before the backlog starts");
        assert_eq!(b.after(109), None, "ahead of the stream");

        // A second full sync cut where the backlog ends leaves it whole…
        b.align(108);
        assert_eq!(b.after(100).unwrap(), b"abcdefgh");
        // …one cut anywhere else starts it again.
        b.align(500);
        assert_eq!((b.start(), b.end), (500, 500));
        assert_eq!(b.after(105), None);

        b.give_up();
        assert_eq!(b.after(500), None);
    }

    #[test]
    fn the_backlog_keeps_only_its_capacity() {
        let capacity = 16 * 1024;
        let mut b = Backlog::new();
        b.align(0);
        let chunk = vec![b'x'; 1000];
        for _ in 0..40 {
            b.push(&chunk, capacity);
        }
        assert_eq!(b.end, 40_000);
        assert_eq!(b.buf.len(), capacity);
        assert_eq!(b.start(), 40_000 - capacity as u64);
        assert!(b.after(b.start()).is_some());
        assert!(b.after(b.start() - 1).is_none());
        // One payload larger than the whole backlog: its tail is kept.
        let big: Vec<u8> = (0..capacity * 2).map(|i| (i % 251) as u8).collect();
        b.push(&big, capacity);
        assert_eq!(b.buf.len(), capacity);
        assert_eq!(b.after(b.start()).unwrap(), &big[capacity..]);
    }

    #[test]
    fn psync_arguments() {
        let id = "a".repeat(40);
        let req = PsyncRequest::parse(&[b"PSYNC", id.as_bytes(), b"101"]).unwrap();
        assert_eq!((req.repl_id.as_str(), req.offset), (id.as_str(), 101));
        assert!(PsyncRequest::parse(&[b"PSYNC", b"?", b"-1"]).is_none());
        assert!(PsyncRequest::parse(&[b"PSYNC", id.as_bytes(), b"-1"]).is_none());
        assert!(PsyncRequest::parse(&[b"PSYNC", id.as_bytes(), b"0"]).is_none());
        assert!(PsyncRequest::parse(&[b"SYNC"]).is_none());
    }

    #[test]
    fn diskless_load_spellings() {
        assert_eq!(flush_before_load("disabled"), Some(true));
        assert_eq!(flush_before_load("flushdb"), Some(true));
        assert_eq!(flush_before_load("on-empty-db"), Some(true));
        assert_eq!(flush_before_load("swapdb"), Some(false));
        assert_eq!(flush_before_load("sometimes"), None);
    }
}
