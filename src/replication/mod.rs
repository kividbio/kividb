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
    /// Replica side: full resyncs refused by the dataset guard, and why the
    /// most recent one was refused. See `replica_client::guard_full_sync`.
    pub full_syncs_refused: AtomicU64,
    pub last_sync_refusal: RwLock<Option<String>>,
    /// Runtime-settable replication knobs (`CONFIG SET`), seeded from `Config`.
    /// Kept here rather than in process-wide statics because the test harness
    /// runs several servers in one process and they need independent values.
    pub repl_timeout_secs: AtomicU64,
    pub repl_ping_period_secs: AtomicU64,
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
}

impl ReplicaProgress {
    pub fn new(base: u64) -> Self {
        Self {
            base,
            sent: AtomicU64::new(0),
            acked: AtomicU64::new(base),
            last_ack_ms: AtomicU64::new(unix_ms()),
            state: AtomicU8::new(ReplicaState::WaitBgsave as u8),
        }
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
            stream_taps: std::sync::atomic::AtomicUsize::new(0),
            taps: RwLock::new(Vec::new()),
            repl_generation: AtomicU64::new(0),
            dataset_commit: parking_lot::Mutex::new(()),
            master_addr: RwLock::new(None),
            replica_progress: parking_lot::Mutex::new(std::collections::HashMap::new()),
            slave_link_state: AtomicU8::new(LinkState::Connect as u8),
            master_last_io_ms: AtomicU64::new(0),
            master_link_down_since_ms: AtomicU64::new(0),
            full_syncs_refused: AtomicU64::new(0),
            last_sync_refusal: RwLock::new(None),
            repl_timeout_secs: AtomicU64::new(60),
            repl_ping_period_secs: AtomicU64::new(10),
            min_replicas_to_write: AtomicU32::new(0),
            min_replicas_max_lag: AtomicU64::new(10),
            full_sync_guard: AtomicU8::new(FullSyncGuard::Empty as u8),
            full_sync_guard_ratio: AtomicU64::new(50),
            master_credentials: RwLock::new((String::new(), String::new())),
            backlog_active: AtomicBool::new(false),
            backlog_size: AtomicU64::new(1024 * 1024),
            backlog_ttl_secs: AtomicU64::new(3600),
            backlog_first_offset: AtomicU64::new(0),
            backlog_histlen: AtomicU64::new(0),
            sync_full: AtomicU64::new(0),
            sync_partial_ok: AtomicU64::new(0),
            sync_partial_err: AtomicU64::new(0),
            resync_point: parking_lot::Mutex::new(None),
            has_resync_point: AtomicBool::new(false),
            apply_lock: parking_lot::Mutex::new(()),
            flush_before_load: AtomicBool::new(true),
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
    /// different database is sent.
    pub fn note_database_selected(&self, db: usize) {
        if db == 0 || self.several_databases.load(Ordering::Relaxed) {
            return;
        }
        // `None` inside a script, which holds the gate itself; the flag is
        // set all the same.
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
        state.flush_before_load.store(
            flush_before_load(&config.repl_diskless_load).unwrap_or(true),
            Ordering::Relaxed,
        );
        state
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

    /// How long a replica waits on a silent master before giving up on it.
    pub fn repl_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.repl_timeout_secs.load(Ordering::Relaxed).max(1))
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
        *self.last_sync_refusal.write() = None;
        // A master's dataset is its own from here on.
        self.set_resync_point(None);
    }

    #[inline]
    pub fn become_slave(&self) {
        self.slave_mode.store(true, Ordering::Release);
        *self.role.write() = "slave".into();
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
    pub fn fingerprint_at_start(
        &self,
        store: &crate::store::Store,
        expiry_store: &crate::store::ExpiryStore,
        hub_tx: &HubTx,
    ) {
        let started = std::time::Instant::now();
        let digest = digest::dataset_digest(store, expiry_store);
        *self.startup_digest.lock() = Some(digest);
        self.resume_as_master(self.repl_id(), 0, hub_tx);
        log_info!(
            "Replication: dataset fingerprinted in {} ms — a replica that holds the same \
             data will be continued rather than sent it",
            started.elapsed().as_millis()
        );
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

/// Where a save leaves the resync point of the dataset it wrote.
pub const RESYNC_PATH: &str = "dump.kdb.repl";
const RESYNC_MAGIC: &str = "kividb-resync 1";

/// Enough of `dump.kdb` to tell whether it is still the file a resync point
/// was written for: its length, and a hash of its first and last 64 KiB.
fn dump_fingerprint(path: &str) -> std::io::Result<(u64, u64)> {
    use std::io::{Read, Seek, SeekFrom};
    const EDGE: u64 = 64 * 1024;
    let mut file = std::fs::File::open(path)?;
    let len = file.metadata()?.len();
    let mut sample = Vec::with_capacity((2 * EDGE) as usize);
    (&mut file).take(EDGE).read_to_end(&mut sample)?;
    if len > EDGE {
        file.seek(SeekFrom::Start(len - EDGE.min(len - EDGE)))?;
        file.read_to_end(&mut sample)?;
    }
    Ok((len, xxhash_rust::xxh3::xxh3_64(&sample)))
}

/// Write the dataset to disk — `dump.kdb`, and with the AOF on, the log reset
/// to an empty delta on top of it — and beside it, on a replica, the position
/// in its master's stream that the dataset corresponds to.
///
/// That position is what lets a replica that is restarted cleanly ask its
/// master to continue rather than send everything again.
pub fn save_dataset(
    store: &crate::store::Store,
    expiry_store: &crate::store::ExpiryStore,
    index_store: &crate::vector::IndexStore,
    repl_state: &ReplState,
) -> std::io::Result<()> {
    // The replica's applier is between commands, and stays there until the
    // save is done: the point read here is the dataset written below.
    let _applying = repl_state.apply_lock.lock();
    // First, so that a save that fails half-way cannot leave an old point
    // beside a new dump.
    let _ = std::fs::remove_file(RESYNC_PATH);
    let point = repl_state.resync_point.lock().clone();

    // On a master with its stream live, where the stream stands at the
    // instant the dataset is captured: no write is half-way then.
    let mut master_point: Option<(String, u64)> = None;
    if crate::aof::AOF_ENABLED.load(Ordering::Relaxed) {
        crate::aof::checkpoint_noting(store, expiry_store, index_store, &mut || {
            if !repl_state.is_slave() && repl_state.feeds_hub() {
                master_point = Some((
                    repl_state.repl_id(),
                    repl_state.repl_offset.load(Ordering::Relaxed),
                ));
            }
        })?;
    } else {
        let maps: Vec<_> = store
            .iter()
            .map(crate::snapshot::collect_snapshot)
            .collect();
        crate::snapshot::write_snapshot_with_expiry(
            "dump.kdb",
            &maps,
            Some(index_store),
            Some(expiry_store),
        )?;
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
    Ok(())
}

/// The resync point saved beside `dump.kdb`, if that dump is still the one it
/// was saved for. The caller must also know that the dataset in memory is
/// that dump and nothing more — see `aof::LocalRecovery::exact_dump`.
pub fn load_saved_resync_point() -> Option<ResyncPoint> {
    saved_point().and_then(|(point, master)| (!master).then_some(point))
}

/// The point a master saved at a clean shutdown, if the dump beside it is
/// still the one it was saved for: its replication id and offset. Same
/// caveat as [`load_saved_resync_point`].
pub fn load_saved_master_point() -> Option<(String, u64)> {
    saved_point().and_then(|(point, master)| master.then_some((point.repl_id, point.offset)))
}

/// The saved point, and whether a master saved it (a file without the role
/// is a replica's, from before masters saved theirs).
fn saved_point() -> Option<(ResyncPoint, bool)> {
    let text = std::fs::read_to_string(RESYNC_PATH).ok()?;
    let mut lines = text.lines();
    if lines.next()? != RESYNC_MAGIC {
        return None;
    }
    let repl_id = lines.next()?.to_string();
    let offset: u64 = lines.next()?.parse().ok()?;
    let db: usize = lines.next()?.parse().ok()?;
    let len: u64 = lines.next()?.parse().ok()?;
    let hash: u64 = lines.next()?.parse().ok()?;
    let master = lines.next() == Some("master");
    if repl_id.len() != 40 || dump_fingerprint("dump.kdb").ok()? != (len, hash) {
        return None;
    }
    Some((
        ResyncPoint {
            repl_id,
            offset,
            db,
        },
        master,
    ))
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
}

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

pub fn next_replica_id() -> u64 {
    NEXT_REPLICA_ID.fetch_add(1, Ordering::Relaxed)
}

pub type HubTx = mpsc::UnboundedSender<HubMsg>;

/// Spawn the replication hub task. Returns the sender used to
/// register replicas and to propagate write commands.
///
/// `max_replica_backlog_bytes` bounds how much unflushed data the hub will queue for
/// any single replica (see [`HubMsg::RegisterChannel::pending_bytes`]). A replica that
/// can't keep up — slow network, stalled disk, wedged process — gets disconnected once
/// its queue crosses this line instead of growing the master's memory without bound.
pub fn start_hub(state: Arc<ReplState>, max_replica_backlog_bytes: u64) -> HubTx {
    let (hub_tx, mut hub_rx) = mpsc::unbounded_channel::<HubMsg>();
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
                    {
                        let _gate = crate::write_gate::enter();
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
                    Some(missed) => {
                        let progress = state.register_replica(
                            registration.replica_id,
                            registration.peer_ip.clone(),
                            registration.peer_port,
                            offset,
                        );
                        progress.set_state(ReplicaState::Online);
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
                        });
                        if reply.send(Ok(())).is_err() {
                            // The connection went away while it was asking.
                            if let Some(gone) = channel_replicas.pop() {
                                forget_replica(&state, gone.id);
                            }
                        }
                    }
                    None => {
                        state.sync_partial_err.fetch_add(1, Ordering::Relaxed);
                        let _ = reply.send(Err(registration));
                    }
                },

                HubMsg::Resume { offset } => {
                    backlog.align(offset);
                    backlog.report(&state);
                }
                HubMsg::Barrier(done) => {
                    let _ = done.send(());
                }

                HubMsg::Deregister(replica_id) => {
                    // Tear down registration (client disconnect / rollback).
                    channel_replicas.retain(|r| r.id != replica_id);
                    forget_replica(&state, replica_id);
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
                    backlog.align(base);
                    state.backlog_active.store(true, Ordering::Relaxed);
                    // Already done at the cut by `begin_full_sync`; a no-op
                    // then. Done here for registrations that did not come
                    // through it.
                    let progress = state.register_replica(replica_id, peer_ip, peer_port, base);
                    channel_replicas.push(ChannelReplica {
                        id: replica_id,
                        addr: addr.clone(),
                        tx,
                        pending_bytes,
                        progress,
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
                    for replica in channel_replicas.drain(..) {
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

                        if now_pending > max_replica_backlog_bytes {
                            // Replica isn't draining its queue fast enough — evict rather than
                            // let the backlog grow without bound. Dropping `tx` here (by not
                            // pushing it back into `channel_replicas`) closes its forwarder's
                            // channel; the owning connection task treats "forwarder ended" as
                            // a disconnect and tears the rest of the connection down. It will
                            // need a full resync to catch back up.
                            forget_replica(&state, replica.id);
                            log_warn!(
                                "Replication: Channel replica {} exceeded backlog limit \
                                 ({now_pending} > {max_replica_backlog_bytes} bytes) — evicting",
                                replica.addr
                            );
                            continue;
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
    expiry_store: &crate::store::ExpiryStore,
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
    let frozen = match crate::snapshot::freeze_dataset(store, Some(expiry_store), Some(index_store))
    {
        Ok(f) => f,
        Err(e) => {
            drop(cut);
            let _ = hub_tx.send(HubMsg::Deregister(registration.replica_id));
            return Err(e);
        }
    };
    drop(cut);
    let held = started.elapsed();

    let payload = frozen.into_payload()?;
    if held > std::time::Duration::from_millis(100) {
        log_info!(
            "Replication: Full-sync cut held writes for {} ms ({} byte payload)",
            held.as_millis(),
            payload.len()
        );
    }
    Ok(FullSync { base, payload })
}

/// What [`begin_full_sync`] cut: the offset the payload corresponds to, and
/// the payload.
pub struct FullSync {
    pub base: u64,
    pub payload: Vec<u8>,
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
    if psync.repl_id != repl_state.repl_id() {
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
        vec![crate::resp::write::encode_resp_command(parts)]
    } else {
        ctx.repl_state.encode_for_stream(*ctx.selected_db, parts)
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

    for payload_bytes in payloads {
        if tapped {
            ctx.repl_state.feed_taps(&payload_bytes);
        }
        let mut raw_opt = Some(payload_bytes);
        let enc_len = raw_opt.as_ref().expect("just encoded").len() as u64;
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
        if ctx.aof_enabled {
            crate::aof::AOF_WRITTEN.fetch_add(enc_len, std::sync::atomic::Ordering::Relaxed);
            crate::aof::queue_record(ctx.aof_tx, raw_opt.take().expect("encoded for AOF path"));
        }
    }
    // Only advance after bytes are emitted: writes that ran with no replicas must not
    // consume the implicit `SELECT`, or the first observer would miss `{select *}` on stream.
    advance_stream_db(ctx, cmd0, parts);
}

/// Propagate a write made by the server itself rather than by a client's
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
        ctx.repl_state
            .repl_stream_last_db
            .store(*ctx.selected_db as u64, Ordering::Relaxed);
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
                | "GEODELBYMEMBER"
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
    "GEODELBYMEMBER",
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
        "SETBIT" | "BITOP" | "BITFIELD" | "PFADD" | "PFMERGE" |
        "GEOADD" | "GEODELBYMEMBER" | "GEOSEARCHSTORE" |
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
        "EVAL" | "EVALSHA" | "FCALL" => true,
        "GEORADIUS" | "GEORADIUSBYMEMBER" => georadius_stores(cmd),
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

/// `GEORADIUS` / `GEORADIUSBYMEMBER` are reads unless they carry `STORE` or
/// `STOREDIST`, which write the result set to another key.
fn georadius_stores(cmd: &[&[u8]]) -> bool {
    cmd.iter()
        .skip(1)
        .any(|a| a.eq_ignore_ascii_case(b"STORE") || a.eq_ignore_ascii_case(b"STOREDIST"))
}

/// True if the full command should be replicated to replicas / counted as a mutating write.
#[inline]
pub fn command_mutates_for_replication(name: &str, cmd: &[&[u8]]) -> bool {
    match name {
        "EVAL_RO" | "EVALSHA_RO" | "FCALL_RO" => false,
        // Writes replicate as inner `redis.call` commands, not as `EVAL` / `EVALSHA` / `FCALL`.
        "EVAL" | "EVALSHA" | "FCALL" => false,
        "GEORADIUS" | "GEORADIUSBYMEMBER" => georadius_stores(cmd),
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

    const TEST_MAX_BACKLOG_BYTES: u64 = 256 * 1024 * 1024;

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
        let hub = start_hub(Arc::clone(&state), TEST_MAX_BACKLOG_BYTES);
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
        let hub = start_hub(Arc::clone(&state), TEST_MAX_BACKLOG_BYTES);
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
        let hub = start_hub(Arc::clone(&state), TEST_MAX_BACKLOG_BYTES);
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
        let hub = start_hub(Arc::clone(&state), TINY_LIMIT);
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
        let hub = start_hub(Arc::clone(&state), TEST_MAX_BACKLOG_BYTES);
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
