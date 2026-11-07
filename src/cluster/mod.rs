// src/cluster/mod.rs
//
// ClusterState — the in-memory representation of this node's view of the
// cluster. Wired into Ctx as &Arc<ClusterState>, accessible from every
// command handler.
//
// On the hot path (every write command), we will:
//   1. Compute key_hash_slot(key)
//   2. Read slots[slot] under a read lock
//   3. Compare to myself; serve or emit MOVED
//
// That means the slots array MUST be:
//   - Direct-indexable (no hash lookup): hence Box<[Option<NodeId>; 16384]>
//   - Behind a single RwLock so 99% reads coexist (slot reassignment is rare)
//
// We don't use ArcSwap yet — it's a lock-free read trick we can reach for
// later if profiling shows the RwLock as a hot spot. Premature.

// src/cluster/mod.rs

pub mod bus;
pub mod ddl;
pub mod migration;
pub mod node;
pub mod persist;
pub mod routing;
pub mod search;
pub mod slot;

pub use node::{
    ClusterNode, NODE_ID_ZERO, NodeFlags, NodeId, generate_node_id, node_id_str, parse_node_id,
};
pub use routing::{RouteDecision, clusterdown_error, crossslot_error, moved_error, route_command};
pub use slot::{CLUSTER_SLOTS, crc16, key_hash_slot};

/// How long a slot assigned or removed here by `SETSLOT NODE` or `DELSLOTS`
/// keeps that assignment whatever a peer claims, unless another command
/// changes it (see `ClusterState::note_by_hand`).
pub const BY_HAND_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

use ahash::AHashMap;
use parking_lot::RwLock;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// State a replica maintains while running for election.
#[derive(Debug, Clone)]
pub struct ElectionState {
    /// The epoch we bumped current_epoch to when starting this election.
    pub epoch: u64,
    /// The ID of the master we're trying to replace.
    pub failed_master_id: NodeId,
    /// Time we started the election (for timeout).
    pub started_at_ms: u64,
    /// IDs of masters who've ACK'd our request this epoch.
    pub votes_received: ahash::AHashSet<NodeId>,
    /// Whether we've already broadcast PONG with the new ownership.
    /// Prevents double-promotion.
    pub completed: bool,
    /// A manual failover (`CLUSTER FAILOVER`): the master is healthy, so
    /// voters are asked to grant the vote without it being FAIL, and the
    /// election is not abandoned just because the master is alive.
    pub manual: bool,
    /// For a manual failover whose master is holding writes for it: the
    /// time (Unix ms) by which it has to be won, or it is given up. See
    /// `heartbeat::MANUAL_FAILOVER_TIMEOUT`.
    pub deadline_ms: Option<u64>,
}

/// Why a master holds writes until it has made sure it still owns its
/// slots. See [`ClusterState::hold_writes_until_confirmed`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HoldReason {
    /// Its replica asked to take over (`CLUSTER FAILOVER`): writes are
    /// paused, as for the failover.
    ManualFailover,
    /// The cluster has marked it FAIL, and told it so: it serves none of its
    /// slots (`-CLUSTERDOWN`).
    MarkedFailed,
}

impl HoldReason {
    fn describe(self) -> &'static str {
        match self {
            HoldReason::ManualFailover => "a manual failover",
            HoldReason::MarkedFailed => "this node was marked FAIL",
        }
    }
}

/// Writes held by a master until it has made sure it still owns its slots.
#[derive(Debug)]
struct HeldWrites {
    /// Why they are held: the first reason, if there came more.
    reason: HoldReason,
    /// When the hold began (Unix ms).
    since_ms: u64,
    /// When the cluster may next be asked (Unix ms).
    ask_from_ms: u64,
    /// A round of asking is under way.
    asking: bool,
    /// When "still held" was last logged (Unix ms; 0: never).
    logged_ms: u64,
    /// Writes are paused for it (`pause::hold_writes`).
    paused: bool,
}

/// How long after a round that could not make sure the next one begins.
const HELD_WRITES_RETRY_MS: u64 = 200;

/// What the cluster layer needs from the rest of the server. Installed once
/// at startup; absent in unit tests that exercise `ClusterState` on its own.
pub struct ClusterHooks {
    /// Starts (or re-points) this node's replication client. Used when the
    /// cluster reconfigures us: a failover moved our master's slots to another
    /// node, or we came back from an outage to find we had been replaced.
    pub replica_starter: crate::replication::replica_client::ReplicaStarter,
    /// The server's pub/sub hub, for delivering messages published on another
    /// node to subscribers connected to this one.
    pub pubsub: crate::pubsub::PubSubHub,
}

/// How long a node removed with `CLUSTER FORGET` is refused re-admission.
/// Redis uses the same minute: long enough for the operator to send FORGET to
/// every node before gossip from one that has not heard yet re-adds it.
pub const FORGET_BAN_MS: u64 = 60_000;

/// `cluster-preferred-endpoint-type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Endpoint {
    Ip = 0,
    Hostname = 1,
    Unknown = 2,
}

impl Endpoint {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "ip" => Some(Endpoint::Ip),
            "hostname" => Some(Endpoint::Hostname),
            "unknown-endpoint" => Some(Endpoint::Unknown),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Endpoint::Ip => "ip",
            Endpoint::Hostname => "hostname",
            Endpoint::Unknown => "unknown-endpoint",
        }
    }
}

/// A hostname as Redis accepts one for `cluster-announce-hostname`: letters,
/// digits, `-` and `.`; empty for none.
pub fn valid_hostname(s: &str) -> bool {
    s.len() <= 255
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.')
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationState {
    /// We own this slot but are exporting it to dest_id.
    /// Reads/writes for keys we still hold succeed; for keys we've lost, -ASK to dest.
    Migrating { dest_id: NodeId },
    /// We do NOT own this slot but are receiving it from src_id.
    /// Reads/writes only succeed if the client sent ASKING this command.
    Importing { src_id: NodeId },
}

/// A master that came back without its dataset, still owning its slots.
///
/// With no AOF, a master killed outright (no final snapshot) restarts with
/// nothing, or with a snapshot from some earlier clean stop. If it is back
/// inside the node timeout nobody fails it over: it keeps its node id and
/// its slots from nodes.conf and serves them, empty or stale, while its
/// replica holds the only good copy. The replica used to refuse an empty
/// master only until clients had written a key or two to it, and then drop
/// everything it had and load those (the AKS run's B2: 25,939 acknowledged
/// writes gone, then 53,271 more).
///
/// So such a master is fenced: it serves none of its slots (`CLUSTERDOWN`,
/// and `cluster_state:fail` in its own `CLUSTER INFO`) until one of these:
///
/// * a replica that holds its data takes over (it is told, on asking for a
///   sync, `+LOSTDATA`), and this node, its slots gone, follows it;
/// * every live replica answers that it holds nothing either — a whole
///   cluster restarted with no persistence, where someone has to serve;
/// * every replica of it has failed (FAIL, the masters' verdict: a replica
///   that is only suspected may be busy taking over);
/// * no replica that understands `+LOSTDATA` has asked for a sync within
///   `max(30 s, 6 node timeouts)` — replicas of an older version.
#[derive(Default)]
pub struct LostDataset {
    fenced: AtomicBool,
    /// When the fence went up (Unix ms).
    since_ms: AtomicU64,
    /// How many times a replica was told `+LOSTDATA`.
    offers: AtomicU64,
    /// The replicas (`ip`, `port`) that answered they hold nothing.
    empty_replicas: parking_lot::Mutex<ahash::AHashSet<(String, u16)>>,
}

/// The locks below are `parking_lot` locks: a reader queues behind a waiting
/// writer, so a thread must never take a lock it already holds, read or
/// write, and a guard must not be kept across an `.await`. Where one thread
/// holds several, it takes them in this order and no other: `handoff_holds`,
/// `forgotten`, `save_lock`, the tasks of `slot_migrations`,
/// `failure_reports`, `nodes`, `slots`, `migrations`, `myself`,
/// `slot_deletions`. `held_writes` is held alone, or under `handoff_holds`.
/// A temporary guard (`self.nodes.read().get(..)`) is held to the end of
/// its statement, or of the `match` / `if let` / `for` it heads.
///
/// None of these is taken while a store shard's lock is held, nor the
/// index store's, the replication taps', or one of an index's own: its
/// postings, its HNSW graph's `key_to_id` and `nodes`, its flat vectors.
/// Not `nodes`, `slots`, `migrations`, `myself`, the tasks of
/// `slot_migrations` nor `slot_deletions`. A batch of a deletion of a lost
/// slot's keys holds `slot_deletions` while it takes all of those, to
/// delete and unindex its keys (`migration::SlotDeletions`); the postings
/// block a writer once their queue is full. Whatever gives this node a slot
/// holds `slots` or `migrations` while it takes `slot_deletions`: a thread
/// holding a shard and waiting for `slots` closes the circle, and the node
/// stops for good (#353). Code that walks the keyspace and needs the slot
/// table reads it once before (`owned_slots`, `search::served_slots`).
pub struct ClusterState {
    pub enabled: bool,
    /// This node's id. Behind a lock only so `CLUSTER RESET HARD` can replace
    /// it; read through [`Self::myself`].
    myself: RwLock<NodeId>,
    pub slots: RwLock<Box<[Option<NodeId>; CLUSTER_SLOTS]>>,
    pub nodes: RwLock<AHashMap<NodeId, ClusterNode>>,
    pub current_epoch: AtomicU64,
    pub config_path: Option<PathBuf>,

    /// Milliseconds without PONG before a peer is marked PFAIL.
    /// Loaded from config at startup; rarely changed at runtime.
    pub node_timeout_ms: u64,

    /// failure_reports[reportee][reporter] = timestamp_ms.
    /// "Reporter believes reportee is failing as of timestamp."
    /// Reports older than 2*node_timeout are discarded as stale.
    pub failure_reports: RwLock<AHashMap<NodeId, AHashMap<NodeId, u64>>>,
    /// Peers whose address is still the one nodes.conf gave us: none of
    /// them has reached us since this process started. A node that moved
    /// while we were down is only reachable at an address a peer relays.
    pub(crate) unconfirmed_addresses: RwLock<ahash::AHashSet<NodeId>>,
    /// The epoch at which we last cast a FAILOVER_AUTH_ACK. Prevents voting
    /// twice in the same epoch — a key safety property of the Raft-like
    /// election. Persisted to nodes.conf.
    pub last_vote_epoch: AtomicU64,

    /// Election state for *us*, when we're a replica running for promotion.
    /// None when not actively running an election.
    pub election: parking_lot::Mutex<Option<ElectionState>>,
    pub election_pending: std::sync::atomic::AtomicBool,
    /// Per-slot migration state. Empty entries mean "not migrating".
    /// Mutating via CLUSTER SETSLOT MIGRATING/IMPORTING/STABLE/NODE.
    pub migrations: RwLock<AHashMap<u16, MigrationState>>,
    /// Atomic slot migrations (`CLUSTER MIGRATION`), running and recent.
    pub slot_migrations: migration::Registry,
    /// The deletions of the keys of slots that left this node, and the
    /// slots that come to it meanwhile. Whatever gives this node a slot,
    /// or starts importing one into it, says so here
    /// ([`migration::SlotDeletions::came_here`]) while it holds `slots` or
    /// `migrations`: a deletion begun before then leaves the slot's keys,
    /// which may be acknowledged writes from then on (#353).
    pub slot_deletions: std::sync::Arc<migration::SlotDeletions>,
    /// Slots assigned with `SETSLOT NODE` or removed with `DELSLOTS` here,
    /// and when: for
    /// [`BY_HAND_GRACE`] a peer's claim does not change them (see
    /// [`Self::note_by_hand`]). Taken after `slots` and `migrations`, and
    /// nothing is taken while it is held.
    pub by_hand: parking_lot::Mutex<AHashMap<u16, std::time::Instant>>,
    /// Nodes removed with `CLUSTER FORGET`, and until when (Unix ms) they are
    /// refused re-admission.
    pub forgotten: RwLock<AHashMap<NodeId, u64>>,
    /// When we last voted for a replica of each master (Unix ms). A master's
    /// replicas are not all granted votes in quick succession, or two of them
    /// could each win an election a moment apart.
    pub last_vote_for: parking_lot::Mutex<AHashMap<NodeId, u64>>,
    /// See [`ClusterHooks`].
    pub hooks: std::sync::OnceLock<ClusterHooks>,
    /// Runtime the cluster bus runs on. Command handlers that need to send a
    /// bus message spawn onto it, whichever runtime they are called from.
    pub bus_runtime: std::sync::OnceLock<tokio::runtime::Handle>,
    /// Whether this node is serving: every slot assigned to a node that has
    /// not failed, and a majority of masters reachable. Recomputed by
    /// [`Self::refresh_state`].
    state_ok: AtomicBool,
    /// What the routing path reads: serving, by `state_ok` or because
    /// `cluster-require-full-coverage` is off, and not fenced for a lost
    /// dataset. One load. Recomputed by [`Self::refresh_state`].
    serving: AtomicBool,
    /// See [`LostDataset`].
    lost: LostDataset,
    /// Writes this master holds until it has made sure it still owns its
    /// slots. See [`Self::hold_writes_until_confirmed`].
    held_writes: parking_lot::Mutex<Option<HeldWrites>>,
    /// How many atomic slot migrations away from this node hold writes for
    /// their handoff ([`Self::hold_writes_for_handoff`]). Writes are let go
    /// when neither they nor [`Self::held_writes`] hold them any more;
    /// whichever of the two ends last lets them go, under this lock.
    handoff_holds: parking_lot::Mutex<u32>,
    /// Handovers of atomic slot migrations away from this node that may
    /// reach their destination's `TAKEOVER` ([`Self::begin_handover`]), and
    /// when the last of them ended (Unix ms); and how many failovers of
    /// this node have begun ([`Self::failover_may_begin`],
    /// [`Self::hold_writes_until_confirmed`]). All three change only under
    /// `handoff_holds`: a failover and a handover exclude each other (#516).
    handovers: AtomicU64,
    last_handover_end_ms: AtomicU64,
    failovers_begun: AtomicU64,
    /// This master serves none of its slots, having been told it was marked
    /// FAIL, until it has made sure it still owns them
    /// ([`Self::note_marked_failed`]).
    marked_failed: AtomicBool,
    /// `cluster-require-full-coverage`.
    require_full_coverage: AtomicBool,
    /// `cluster-replica-validity-factor`: how stale a replica's data may be
    /// for it to stand for election, in node timeouts; 0 = always stand.
    replica_validity_factor: AtomicU64,
    /// True while our own IP is the loopback or nodes.conf guess used when
    /// binding to a wildcard address with no `cluster-announce-ip`. While set,
    /// we send an empty address so peers record the one they observe instead.
    self_ip_is_guess: AtomicBool,
    /// The thread that writes nodes.conf for [`Self::save_soon`].
    background_save: std::sync::OnceLock<std::sync::mpsc::SyncSender<()>>,
    /// `cluster-bus-secret`, when there is one: see `bus::auth`.
    pub bus_secret: std::sync::OnceLock<Vec<u8>>,
    /// `cluster-preferred-endpoint-type`: what MOVED, ASK and the
    /// topology replies give clients to connect to. See [`Endpoint`].
    preferred_endpoint: std::sync::atomic::AtomicU8,
    /// Serialises `save_to_disk`, and remembers what was last written so an
    /// unchanged topology is not rewritten. See `persist::save_from_state`.
    pub(crate) save_lock: parking_lot::Mutex<()>,
    pub(crate) last_saved_fingerprint: AtomicU64,
    /// Bus traffic counters for `CLUSTER INFO`.
    pub stats_messages_sent: AtomicU64,
    pub stats_messages_received: AtomicU64,
    /// Outbound bus connections opened (`cluster_stats_bus_connects`).
    pub stats_bus_connects: AtomicU64,
    /// Long-lived outbound bus links, by `ip:bus_port`. See
    /// `bus::dialer::send_frame`.
    pub(crate) links: parking_lot::Mutex<AHashMap<String, tokio::sync::mpsc::Sender<Vec<u8>>>>,
    /// The connection each peer is pinged over, by `ip:bus_port`, kept open
    /// between heartbeats. See `bus::dialer::send_ping`.
    pub(crate) ping_links: parking_lot::Mutex<AHashMap<String, bus::dialer::PingLink>>,
}

impl ClusterState {
    /// The one place every field is initialised.
    #[allow(clippy::too_many_arguments)]
    fn build(
        enabled: bool,
        myself: NodeId,
        slots: Box<[Option<NodeId>; CLUSTER_SLOTS]>,
        nodes: AHashMap<NodeId, ClusterNode>,
        current_epoch: u64,
        last_vote_epoch: u64,
        config_path: Option<PathBuf>,
        node_timeout_ms: u64,
    ) -> Self {
        let state = Self {
            enabled,
            myself: RwLock::new(myself),
            slots: RwLock::new(slots),
            nodes: RwLock::new(nodes),
            current_epoch: AtomicU64::new(current_epoch),
            config_path,
            node_timeout_ms,
            failure_reports: RwLock::new(AHashMap::new()),
            unconfirmed_addresses: RwLock::new(ahash::AHashSet::new()),
            last_vote_epoch: AtomicU64::new(last_vote_epoch),
            election: parking_lot::Mutex::new(None),
            election_pending: AtomicBool::new(false),
            migrations: RwLock::new(AHashMap::new()),
            slot_migrations: migration::Registry::default(),
            slot_deletions: Default::default(),
            by_hand: parking_lot::Mutex::new(AHashMap::new()),
            forgotten: RwLock::new(AHashMap::new()),
            last_vote_for: parking_lot::Mutex::new(AHashMap::new()),
            hooks: std::sync::OnceLock::new(),
            bus_runtime: std::sync::OnceLock::new(),
            state_ok: AtomicBool::new(false),
            serving: AtomicBool::new(false),
            lost: LostDataset::default(),
            held_writes: parking_lot::Mutex::new(None),
            handoff_holds: parking_lot::Mutex::new(0),
            handovers: AtomicU64::new(0),
            last_handover_end_ms: AtomicU64::new(0),
            failovers_begun: AtomicU64::new(0),
            marked_failed: AtomicBool::new(false),
            require_full_coverage: AtomicBool::new(true),
            replica_validity_factor: AtomicU64::new(10),
            self_ip_is_guess: AtomicBool::new(false),
            bus_secret: std::sync::OnceLock::new(),
            background_save: std::sync::OnceLock::new(),
            preferred_endpoint: std::sync::atomic::AtomicU8::new(Endpoint::Ip as u8),
            save_lock: parking_lot::Mutex::new(()),
            last_saved_fingerprint: AtomicU64::new(0),
            stats_messages_sent: AtomicU64::new(0),
            stats_messages_received: AtomicU64::new(0),
            stats_bus_connects: AtomicU64::new(0),
            links: parking_lot::Mutex::new(AHashMap::new()),
            ping_links: parking_lot::Mutex::new(AHashMap::new()),
        };
        state.refresh_state();
        state
    }

    pub fn new_disabled() -> Self {
        Self::build(
            false,
            NODE_ID_ZERO,
            empty_slots(),
            AHashMap::new(),
            0,
            0,
            None,
            15000,
        )
    }

    /// This node's id.
    #[inline]
    pub fn myself(&self) -> NodeId {
        *self.myself.read()
    }

    pub fn new_enabled(config_path: Option<PathBuf>, node_timeout_ms: u64) -> Self {
        let myself = generate_node_id();
        let mut nodes = AHashMap::new();
        nodes.insert(
            myself,
            ClusterNode {
                id: myself,
                ip: String::new(),
                client_port: 0,
                cluster_port: 0,
                flags: NodeFlags::MYSELF | NodeFlags::MASTER,
                master_id: None,
                config_epoch: 0,
                ping_sent_ms: 0,
                pong_received_ms: 0,
                fail_since_ms: 0,
                hostname: String::new(),
                announced_client_port: 0,
            },
        );
        Self::build(
            true,
            myself,
            empty_slots(),
            nodes,
            0,
            0,
            config_path,
            node_timeout_ms,
        )
    }

    /// Construct from an existing nodes.conf if present, else generate fresh.
    /// Use this on cluster-mode startup. If `path` is None, behaves like
    /// new_enabled with no persistence (tests).
    pub fn from_disk_or_fresh(
        path: Option<PathBuf>,
        node_timeout_ms: u64,
    ) -> std::io::Result<Self> {
        let path = match path {
            Some(p) => p,
            None => return Ok(Self::new_enabled(None, node_timeout_ms)),
        };

        match persist::load(&path)? {
            Some(loaded) => Ok(Self::build(
                true,
                loaded.myself,
                loaded.slots,
                loaded.nodes,
                loaded.current_epoch,
                loaded.last_vote_epoch,
                Some(path),
                node_timeout_ms,
            )),
            None => {
                let state = Self::new_enabled(Some(path), node_timeout_ms);
                state.save_to_disk()?;
                Ok(state)
            }
        }
    }

    /// Build this node's cluster state from its configuration: disabled in
    /// standalone mode, otherwise loaded from `cluster-config-file` (or created
    /// fresh on first start) with this node's address and settings applied.
    ///
    /// A cluster config file that exists but cannot be read is an error, not a
    /// reason to start over. Starting over means a new node id and no slots —
    /// the node would rejoin as a stranger, own nothing, and overwrite the
    /// file that recorded what it used to be. It used to do exactly that,
    /// after logging one line.
    pub fn from_config(config: &crate::config::Config) -> std::io::Result<Self> {
        if !config.cluster_enabled {
            return Ok(Self::new_disabled());
        }
        if !valid_hostname(&config.cluster_announce_hostname) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "cluster-announce-hostname may only have letters, digits, '-' and '.', \
                     not '{}'",
                    config.cluster_announce_hostname
                ),
            ));
        }
        if Endpoint::parse(&config.cluster_preferred_endpoint_type).is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "cluster-preferred-endpoint-type must be ip, hostname or unknown-endpoint, \
                     not '{}'",
                    config.cluster_preferred_endpoint_type
                ),
            ));
        }
        let path = PathBuf::from(&config.cluster_config_file);
        let state =
            Self::from_disk_or_fresh(Some(path), config.cluster_node_timeout).map_err(|e| {
                std::io::Error::new(
                    e.kind(),
                    format!(
                        "cluster config file '{}' is unreadable: {e}. Refusing to start with a \
                         new identity; repair or remove the file",
                        config.cluster_config_file
                    ),
                )
            })?;
        // With `tls-cluster` the port to reach this node on is its TLS port,
        // for clients and for the other nodes alike (see `node_tls`); the bus
        // stays where it is. A node with no plaintext port (port 0) has
        // `tls-cluster`: see `Config::validate`.
        let client_port = if config.tls_cluster || config.port == 0 {
            config.tls_port
        } else {
            config.port
        };
        let bus_port = if config.cluster_announce_bus_port != 0 {
            config.cluster_announce_bus_port
        } else {
            config.cluster_bus_port()
        };
        state.configure_self_address(
            &config.bind,
            client_port,
            &config.cluster_announce_ip,
            config.cluster_announce_port,
            bus_port,
        );
        state.set_require_full_coverage(config.cluster_require_full_coverage);
        state.set_replica_validity_factor(config.cluster_replica_validity_factor);
        state.set_my_hostname(&config.cluster_announce_hostname);
        state.set_my_announced_client_port(config.cluster_announce_client_port);
        if !config.cluster_bus_secret.is_empty() {
            let _ = state
                .bus_secret
                .set(config.cluster_bus_secret.as_bytes().to_vec());
        }
        state.set_preferred_endpoint(
            Endpoint::parse(&config.cluster_preferred_endpoint_type).unwrap_or(Endpoint::Ip),
        );
        // Save back so the address persists if it was unset before.
        state.save_to_disk()?;
        Ok(state)
    }

    /// Give the cluster layer its handles on the rest of the server and start
    /// the bus. Call once, after the stores and the replication state exist.
    pub fn start(self: &std::sync::Arc<Self>, config: &crate::config::Config, hooks: ClusterHooks) {
        if !self.enabled {
            return;
        }
        let repl_state = std::sync::Arc::clone(&hooks.replica_starter.repl_state);
        let _ = self.hooks.set(hooks);
        let (asked, taker) = (
            std::sync::Arc::downgrade(self),
            std::sync::Arc::downgrade(self),
        );
        repl_state.install_cluster_link(crate::replication::ClusterLink {
            master_node: Box::new(move || {
                let state = asked.upgrade()?;
                state.my_master_id().map(|id| node_id_str(&id).to_string())
            }),
            take_over: Box::new(move |master, why| {
                taker
                    .upgrade()
                    .is_some_and(|state| state.take_over_from(master, why))
            }),
        });
        // A replica that started on a saved resync point holds the stream of
        // the master nodes.conf names.
        repl_state.note_stream_origin_if_unknown(
            self.my_master_id().map(|id| node_id_str(&id).to_string()),
        );
        // The keys of an import this node was receiving when it went down,
        // which never completed (#108).
        migration::drop_unfinished_imports(self);
        self.start_the_clock_on_peers();
        self.start_background_save();
        bus::start(
            std::sync::Arc::clone(self),
            repl_state,
            config.bind.clone(),
            config.cluster_bus_port(),
        );
    }

    /// Count every peer as heard from now, as the bus starts. The times kept
    /// in nodes.conf are from before the restart (it is saved only when the
    /// topology changes), and the first failure check, 100 ms after start,
    /// used to find every peer silent for longer than the node timeout and
    /// suspect it. Nodes restarted together then agreed on those suspicions
    /// and failed a master that was only starting up too; its replica held
    /// an election and the old master came back as a replica and resynced
    /// in full. Redis does the same on loading its nodes.conf.
    ///
    /// And a node timeout more on top: peers restarted with this one come
    /// up over a second or two, the first heartbeat goes out a second after
    /// the bus starts, and a peer that started last was otherwise suspected
    /// by everyone the moment one timeout had passed.
    fn start_the_clock_on_peers(&self) {
        let now = now_ms() + self.node_timeout_ms;
        let me = self.myself();
        let mut unconfirmed = ahash::AHashSet::new();
        for node in self.nodes.write().values_mut() {
            if node.id == me {
                continue;
            }
            node.ping_sent_ms = 0;
            node.pong_received_ms = now;
            node.flags.remove(NodeFlags::PFAIL);
            unconfirmed.insert(node.id);
        }
        *self.unconfirmed_addresses.write() = unconfirmed;
        self.failure_reports.write().clear();
    }

    /// Save nodes.conf on a thread of its own, and return at once; saves
    /// asked for while one is being written are made as one, of the latest
    /// state. For the bus: a save is written and synced, and a bus thread
    /// waiting on a disk busy with logs and snapshots answered nobody for
    /// seconds at a time, until its peers decided it had failed (every node
    /// of a cluster sharing a disk, during a resharding). What the cluster
    /// serves from is brought up to date here, as by [`Self::save_to_disk`].
    pub fn save_soon(&self) {
        self.refresh_state();
        match self.background_save.get() {
            // Full: a save is already waiting, and will see this state.
            Some(tx) => {
                let _ = tx.try_send(());
            }
            None => {
                let _ = persist::save_from_state(self);
            }
        }
    }

    fn start_background_save(self: &std::sync::Arc<Self>) {
        let (tx, rx) = std::sync::mpsc::sync_channel::<()>(1);
        let state = std::sync::Arc::downgrade(self);
        let spawned = std::thread::Builder::new()
            .name("kividb-nodes-conf".into())
            .spawn(move || {
                while rx.recv().is_ok() {
                    let Some(state) = state.upgrade() else {
                        return;
                    };
                    if let Err(e) = persist::save_from_state(&state) {
                        log_warn!("Cluster: could not save the cluster config file: {e}");
                    }
                }
            });
        if spawned.is_ok() {
            let _ = self.background_save.set(tx);
        }
    }

    /// Persist current state to disk, if a config path is configured.
    /// Safe to call from any thread; takes both read locks briefly to snapshot.
    pub fn save_to_disk(&self) -> std::io::Result<()> {
        // Every topology mutation ends in a save, which makes this the one
        // place that can keep the derived serving state current without each
        // mutator having to remember to.
        self.refresh_state();
        persist::save_from_state(self)
    }

    /// Set the bind address and port on our own node entry. Called from
    /// runtime startup once we know what we're listening on.
    pub fn set_self_address(&self, ip: String, client_port: u16) {
        self.configure_self_address(&ip, client_port, "", 0, 0);
    }

    /// Record how this node is to be reached, from its configuration.
    ///
    /// `announce_*` are `cluster-announce-ip` / `-port` / `-bus-port`; empty
    /// and zero mean "not set". With no announce IP and a wildcard bind there
    /// is no address to advertise: `0.0.0.0` is not somewhere a peer can
    /// connect to. The entry keeps the address it had learnt in an earlier
    /// run, from nodes.conf, as Redis does, or else falls back to loopback —
    /// right for a single-machine cluster — and is flagged as a guess, which
    /// has two effects: outgoing bus messages carry an empty address so each
    /// peer records the one it actually sees us on, and the guess is replaced
    /// with the real interface address the first time we reach a peer that
    /// is not on this machine (see [`Self::learn_self_ip`]).
    ///
    /// A restart used to start from loopback whatever nodes.conf said, so a
    /// node restarted in place gave `127.0.0.1` as its own address in
    /// `CLUSTER SLOTS` until it next reached a peer, and saved it (#307).
    ///
    /// It used to advertise the loopback guess as fact. Every node of a
    /// multi-host cluster bound to `0.0.0.0` — the default, and the norm in
    /// containers — then told its peers it lived at `127.0.0.1`, and clients
    /// following a `MOVED` were sent to themselves.
    pub fn configure_self_address(
        &self,
        bind: &str,
        client_port: u16,
        announce_ip: &str,
        announce_port: u16,
        announce_bus_port: u16,
    ) {
        let wildcard = bind.is_empty() || bind == "0.0.0.0" || bind == "::" || bind == "[::]";
        let (ip, guess) = if !announce_ip.is_empty() {
            (announce_ip.to_string(), false)
        } else if !wildcard {
            (bind.to_string(), false)
        } else {
            let learnt = self
                .nodes
                .read()
                .get(&self.myself())
                .map(|me| me.ip.clone())
                .filter(|ip| {
                    ip.parse::<std::net::IpAddr>()
                        .is_ok_and(|ip| !ip.is_loopback() && !ip.is_unspecified())
                });
            (learnt.unwrap_or_else(|| "127.0.0.1".to_string()), true)
        };
        self.self_ip_is_guess.store(guess, Ordering::Relaxed);
        let announced_client = if announce_port != 0 {
            announce_port
        } else {
            client_port
        };
        let announced_bus = if announce_bus_port != 0 {
            announce_bus_port
        } else {
            client_port.saturating_add(10000)
        };
        let mut nodes = self.nodes.write();
        if let Some(me) = nodes.get_mut(&self.myself()) {
            me.ip = ip;
            me.client_port = announced_client;
            me.cluster_port = announced_bus;
        }
    }

    /// Set this node's `cluster-announce-hostname` (empty: none). Peers
    /// learn it from the next heartbeat.
    pub fn set_my_hostname(&self, hostname: &str) {
        if let Some(me) = self.nodes.write().get_mut(&self.myself()) {
            me.hostname = hostname.to_string();
        }
    }

    /// Set this node's `cluster-announce-client-port` (0: none). Peers learn
    /// it from the next heartbeat.
    pub fn set_my_announced_client_port(&self, port: u16) {
        if let Some(me) = self.nodes.write().get_mut(&self.myself()) {
            me.announced_client_port = port;
        }
    }

    /// Record the client port a peer announces. As for its hostname: its own
    /// word stands, none included; second-hand it only fills in.
    pub fn set_peer_client_port(&self, id: NodeId, port: u16, authoritative: bool) {
        if id == self.myself() || (port == 0 && !authoritative) {
            return;
        }
        if let Some(node) = self.nodes.write().get_mut(&id) {
            node.announced_client_port = port;
        }
    }

    /// Record a peer's hostname. `authoritative` when it is the peer's own
    /// word, which may also be that it has none; second-hand it only fills
    /// in. Saved when it changes.
    pub fn set_peer_hostname(&self, id: NodeId, hostname: &str, authoritative: bool) {
        if id == self.myself() || (hostname.is_empty() && !authoritative) {
            return;
        }
        let changed = match self.nodes.write().get_mut(&id) {
            Some(node) if node.hostname != hostname => {
                node.hostname = hostname.to_string();
                true
            }
            _ => false,
        };
        if changed {
            self.save_soon();
        }
    }

    pub fn set_preferred_endpoint(&self, endpoint: Endpoint) {
        self.preferred_endpoint
            .store(endpoint as u8, Ordering::Relaxed);
    }

    pub fn preferred_endpoint(&self) -> Endpoint {
        match self.preferred_endpoint.load(Ordering::Relaxed) {
            1 => Endpoint::Hostname,
            2 => Endpoint::Unknown,
            _ => Endpoint::Ip,
        }
    }

    /// Where clients are told to find `node`, as Redis's
    /// `cluster-preferred-endpoint-type` has it: its address; its hostname,
    /// `?` if it announces none; or empty, for "the address you used".
    pub fn endpoint_of(&self, node: &ClusterNode) -> String {
        match self.preferred_endpoint() {
            Endpoint::Ip => node.ip.clone(),
            Endpoint::Hostname if node.hostname.is_empty() => "?".to_string(),
            Endpoint::Hostname => node.hostname.clone(),
            Endpoint::Unknown => String::new(),
        }
    }

    /// The address to put in an outgoing bus message as our own: empty while
    /// it is only a guess, so the receiver uses the address it observes.
    pub fn advertised_ip(&self, recorded: &str) -> String {
        if self.self_ip_is_guess.load(Ordering::Relaxed) {
            String::new()
        } else {
            recorded.to_string()
        }
    }

    /// We reached a peer from `local_ip`. If our own address is still the
    /// loopback or nodes.conf guess and this is a real interface address,
    /// adopt it.
    pub fn learn_self_ip(&self, local_ip: std::net::IpAddr) {
        if !self.self_ip_is_guess.load(Ordering::Relaxed)
            || local_ip.is_loopback()
            || local_ip.is_unspecified()
        {
            return;
        }
        let mut nodes = self.nodes.write();
        if let Some(me) = nodes.get_mut(&self.myself()) {
            me.ip = local_ip.to_string();
            self.self_ip_is_guess.store(false, Ordering::Relaxed);
            log_info!("Cluster: Learned own address from an outgoing link: {local_ip}");
        }
    }

    /// `cluster-replica-validity-factor`.
    pub fn replica_validity_factor(&self) -> u64 {
        self.replica_validity_factor.load(Ordering::Relaxed)
    }

    pub fn set_replica_validity_factor(&self, factor: u64) {
        self.replica_validity_factor
            .store(factor, Ordering::Relaxed);
    }

    pub fn set_require_full_coverage(&self, yes: bool) {
        self.require_full_coverage.store(yes, Ordering::Relaxed);
        self.refresh_state();
    }

    #[inline]
    pub fn slot_owner(&self, slot: u16) -> Option<NodeId> {
        if !self.enabled {
            return None;
        }
        self.slots.read()[slot as usize]
    }

    #[inline]
    pub fn owns_slot(&self, slot: u16) -> bool {
        if !self.enabled {
            return true;
        }
        match self.slots.read()[slot as usize] {
            Some(owner) => owner == self.myself(),
            None => false,
        }
    }

    /// Per slot, whether this node owns it (every slot, outside a
    /// cluster), read off the slot table at once: for a walk of the
    /// keyspace, which must not look at the slot table while it holds a
    /// shard (see `ClusterState`).
    pub fn owned_slots(&self) -> Vec<bool> {
        if !self.enabled {
            return vec![true; CLUSTER_SLOTS];
        }
        let me = self.myself();
        self.slots
            .read()
            .iter()
            .map(|owner| *owner == Some(me))
            .collect()
    }

    /// How many slots does this node own? Used by CLUSTER INFO.
    pub fn my_slot_count(&self) -> usize {
        if !self.enabled {
            return 0;
        }
        let slots = self.slots.read();
        slots.iter().filter(|s| **s == Some(self.myself())).count()
    }

    /// How many slots are assigned to *anyone* (vs. unassigned)?
    pub fn assigned_slot_count(&self) -> usize {
        if !self.enabled {
            return 0;
        }
        self.slots.read().iter().filter(|s| s.is_some()).count()
    }

    /// `cluster_state` for CLUSTER INFO.
    pub fn cluster_state_str(&self) -> &'static str {
        if !self.enabled || self.state_ok.load(Ordering::Relaxed) {
            "ok"
        } else {
            "fail"
        }
    }

    /// Whether keyed commands may be served right now: the cluster is up,
    /// or `cluster-require-full-coverage` is off; and this node is not a
    /// master fenced for a lost dataset ([`LostDataset`]).
    #[inline]
    pub fn is_serving(&self) -> bool {
        !self.enabled || self.serving.load(Ordering::Relaxed)
    }

    /// `(assigned, ok, pfail, fail)` slot counts, by the state of each slot's
    /// owner as this node sees it.
    pub fn slot_health(&self) -> (usize, usize, usize, usize) {
        let nodes = self.nodes.read();
        let slots = self.slots.read();
        let (mut assigned, mut pfail, mut fail) = (0usize, 0usize, 0usize);
        // Slots come in long runs with one owner; look each owner up once.
        let mut last: Option<(NodeId, NodeFlags)> = None;
        for owner in slots.iter().flatten() {
            assigned += 1;
            let flags = match last {
                Some((id, flags)) if id == *owner => flags,
                _ => {
                    let flags = nodes.get(owner).map(|n| n.flags).unwrap_or(NodeFlags::FAIL);
                    last = Some((*owner, flags));
                    flags
                }
            };
            if flags.contains(NodeFlags::FAIL) {
                fail += 1;
            } else if flags.contains(NodeFlags::PFAIL) {
                pfail += 1;
            }
        }
        (assigned, assigned - pfail - fail, pfail, fail)
    }

    /// Number of masters that own at least one slot — the cluster's "size",
    /// and the electorate for failure verdicts and failover votes.
    pub fn voting_master_count(&self) -> usize {
        let slots = self.slots.read();
        let mut owners: ahash::AHashSet<NodeId> = ahash::AHashSet::new();
        let mut last: Option<NodeId> = None;
        for owner in slots.iter().flatten() {
            if last != Some(*owner) {
                owners.insert(*owner);
                last = Some(*owner);
            }
        }
        owners.len()
    }

    /// Whether `id` owns at least one slot.
    pub fn owns_any_slot(&self, id: NodeId) -> bool {
        self.slots.read().contains(&Some(id))
    }

    /// Recompute whether this node should be serving.
    ///
    /// The cluster is down, from this node's point of view, when a slot has no
    /// owner, when a slot's owner has been declared failed (until a replica
    /// takes over), or when this node cannot reach a majority of the masters —
    /// the minority side of a partition must stop accepting writes that the
    /// majority side will never see.
    ///
    /// It used to be "every slot is assigned", which stayed `ok` with a master
    /// dead and no replica to replace it: clients kept being redirected to a
    /// node that was not there.
    pub fn refresh_state(&self) {
        if !self.enabled {
            self.state_ok.store(true, Ordering::Relaxed);
            self.serving.store(true, Ordering::Relaxed);
            return;
        }
        let (assigned, _ok, _pfail, fail) = self.slot_health();
        let mut ok = assigned == CLUSTER_SLOTS && fail == 0;
        if ok {
            // Partition check: of the masters that own slots, how many can we
            // not reach?
            let nodes = self.nodes.read();
            let slots = self.slots.read();
            let mut owners: ahash::AHashSet<NodeId> = ahash::AHashSet::new();
            let mut last: Option<NodeId> = None;
            for owner in slots.iter().flatten() {
                if last != Some(*owner) {
                    owners.insert(*owner);
                    last = Some(*owner);
                }
            }
            let unreachable = owners
                .iter()
                .filter(|id| {
                    nodes
                        .get(*id)
                        .is_none_or(|n| n.flags.intersects(NodeFlags::PFAIL | NodeFlags::FAIL))
                })
                .count();
            if unreachable > 0 && unreachable > owners.len() / 2 {
                ok = false;
            }
        }
        let fenced = self.fence_holds() || self.marked_failed.load(Ordering::Relaxed);
        let ok = ok && !fenced;
        self.state_ok.store(ok, Ordering::Relaxed);
        let serving = !fenced && (ok || !self.require_full_coverage.load(Ordering::Relaxed));
        if self.serving.swap(serving, Ordering::Relaxed) && !serving {
            // A write routed while this node served, and not yet in the write
            // gate, is routed again there, and refused (#337).
            crate::pause::routes_moved();
        }
    }

    // ── A master back without its dataset ────────────────────────────────────

    /// Decide, once at startup — after the dataset is loaded, before any
    /// client is served — whether this node is a master that came back
    /// without its dataset: see [`LostDataset`]. `trusted` is true when the
    /// dataset is known to be all the node had: the AOF is on, or the
    /// previous process stopped cleanly with the snapshot that was loaded
    /// (`replication::take_clean_exit_marker`), or an operator chose it
    /// (`preload-file`). A restart of either kind goes on exactly as before.
    pub fn check_dataset_at_start(&self, trusted: bool, keys: usize) {
        if !self.enabled || trusted {
            return;
        }
        let me = self.myself();
        let (is_master, replicas) = {
            let nodes = self.nodes.read();
            let is_master = nodes
                .get(&me)
                .is_some_and(|n| n.flags.contains(NodeFlags::MASTER));
            let replicas = nodes.values().filter(|n| n.master_id == Some(me)).count();
            (is_master, replicas)
        };
        if !is_master || !self.owns_any_slot(me) {
            return;
        }
        if replicas == 0 {
            log_warn!(
                "Cluster: This master came back without its dataset ({keys} keys loaded; no AOF, \
                 and no snapshot from a clean shutdown), and has no replica that could hold it: \
                 serving what it has"
            );
            return;
        }
        self.lost.since_ms.store(now_ms(), Ordering::Relaxed);
        self.lost.fenced.store(true, Ordering::Relaxed);
        log_warn!(
            "Cluster: This master came back without its dataset ({keys} keys loaded; no AOF, and \
             no snapshot from a clean shutdown). It serves none of its slots until a replica that \
             holds them takes over, or its replicas answer that they hold nothing either"
        );
        self.refresh_state();
    }

    /// Whether this node is fenced for a lost dataset ([`LostDataset`]).
    pub fn lost_dataset(&self) -> bool {
        self.lost.fenced.load(Ordering::Relaxed)
    }

    /// A replica was told `+LOSTDATA`.
    pub fn note_lost_dataset_offer(&self) {
        self.lost.offers.fetch_add(1, Ordering::Relaxed);
    }

    /// A replica told `+LOSTDATA` answered that it holds nothing (it asked
    /// for a sync again). Returns whether the fence is down: every live
    /// replica of this master has said so.
    pub fn replica_has_nothing(&self, ip: &str, port: u16) -> bool {
        if !self.lost.fenced.load(Ordering::Relaxed) {
            return true;
        }
        let answered = {
            let mut set = self.lost.empty_replicas.lock();
            set.insert((ip.to_string(), port));
            set.len()
        };
        let live = self.live_replicas_of_me();
        if answered < live {
            log_info!(
                "Cluster: Replica {ip}:{port} holds nothing of this master's lost dataset; \
                 waiting for {} more",
                live - answered
            );
            return false;
        }
        self.lift_fence("its replicas hold nothing of it either");
        self.refresh_state();
        true
    }

    /// The periodic look at the fence (from the failure detector): lifted
    /// when every replica has failed, or when none that understands
    /// `+LOSTDATA` has asked for a sync in time.
    pub fn check_lost_dataset_fence(&self) {
        if !self.lost.fenced.load(Ordering::Relaxed) {
            return;
        }
        if self.live_replicas_of_me() == 0 {
            self.lift_fence("every replica of it has failed: none can take over");
            self.refresh_state();
            return;
        }
        let waited = now_ms().saturating_sub(self.lost.since_ms.load(Ordering::Relaxed));
        let limit = (6 * self.node_timeout_ms).max(30_000);
        if self.lost.offers.load(Ordering::Relaxed) == 0 && waited > limit {
            self.lift_fence(&format!(
                "no replica able to take over has asked for a sync in {} s (an older version?)",
                limit / 1000
            ));
            self.refresh_state();
        }
    }

    /// Whether the fence still holds; it falls by itself once this node no
    /// longer serves slots as a master (a replica took over, and this node
    /// follows it). Called by `refresh_state`: takes `nodes` and `slots`,
    /// one after the other.
    fn fence_holds(&self) -> bool {
        if !self.lost.fenced.load(Ordering::Relaxed) {
            return false;
        }
        let me = self.myself();
        let master = self
            .nodes
            .read()
            .get(&me)
            .is_some_and(|n| n.flags.contains(NodeFlags::MASTER));
        if master && self.owns_any_slot(me) {
            return true;
        }
        self.lift_fence("it serves no slots any more");
        false
    }

    fn lift_fence(&self, why: &str) {
        if self.lost.fenced.swap(false, Ordering::Relaxed) {
            log_warn!("Cluster: Lost-dataset fence lifted: {why}");
        }
    }

    /// This master's replicas, in its view, that have not failed. One only
    /// suspected (PFAIL) counts: a replica winning an election can be slow
    /// to answer pings while it saves its new configuration, and the fence
    /// must not fall then — this master would take writes that the
    /// promotion then throws away.
    fn live_replicas_of_me(&self) -> usize {
        let me = self.myself();
        self.nodes
            .read()
            .values()
            .filter(|n| {
                n.master_id == Some(me)
                    && !n.flags.intersects(NodeFlags::FAIL | NodeFlags::HANDSHAKE)
            })
            .count()
    }

    /// Have this replica take over from `master` (a node id) at once, with
    /// an election that does not wait for the master to be failed — what
    /// `CLUSTER FAILOVER FORCE` starts. For a master that has lost the data
    /// this replica holds. False if `master` is not this node's master.
    pub fn take_over_from(self: &std::sync::Arc<Self>, master: &str, why: &str) -> bool {
        let Some(master_id) = parse_node_id(master.as_bytes()) else {
            return false;
        };
        if self.my_master_id() != Some(master_id) {
            return false;
        }
        if self.election.lock().is_some() || self.election_pending.load(Ordering::SeqCst) {
            return true;
        }
        let Some(bus) = self.bus_runtime.get() else {
            return false;
        };
        log_warn!("Cluster: Taking over from master {master}: {why}");
        let state = std::sync::Arc::clone(self);
        bus.spawn(async move {
            bus::heartbeat::start_forced_election(&state, master_id, None).await;
        });
        true
    }

    pub fn current_epoch(&self) -> u64 {
        self.current_epoch.load(Ordering::Relaxed)
    }

    /// A new epoch, above every one this node has seen (Redis's
    /// `currentEpoch` is the highest seen). It used to count up from this
    /// node's own: a node that had just joined took over slots at epoch 3
    /// from a master at epoch 3, and its peers kept the old owner, since a
    /// claim must be newer than the owner's.
    pub fn bump_epoch(&self) -> u64 {
        let seen = self
            .nodes
            .read()
            .values()
            .map(|n| n.config_epoch)
            .max()
            .unwrap_or(0);
        self.current_epoch.fetch_max(seen, Ordering::SeqCst);
        self.current_epoch.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Note an epoch a peer is at.
    pub fn observe_epoch(&self, epoch: u64) {
        self.current_epoch.fetch_max(epoch, Ordering::SeqCst);
    }

    /// Two masters with one config epoch: the one with the lower id moves to
    /// a new epoch, as in Redis (`clusterHandleConfigEpochCollision`), so
    /// that a claim of one over the other can be decided. Returns whether
    /// this node moved.
    pub fn resolve_epoch_collision(&self, sender: NodeId, sender_epoch: u64) -> bool {
        let me = self.myself();
        if sender < me {
            return false;
        }
        let colliding =
            self.nodes.read().get(&me).is_some_and(|n| {
                n.flags.contains(NodeFlags::MASTER) && n.config_epoch == sender_epoch
            });
        if !colliding {
            return false;
        }
        let epoch = self.bump_epoch();
        if let Some(node) = self.nodes.write().get_mut(&me) {
            node.config_epoch = epoch;
        }
        log_info!(
            "Cluster: config epoch collision with {} — moved to {epoch}",
            node_id_str(&sender)
        );
        true
    }

    // ── Mutators (Step 3) ─────────────────────────────────────────────────────
    //
    // All of these mutate slot ownership or epoch state. Each one returns a
    // Result so the command handler can produce a precise error message.
    // All successful mutations call save_to_disk() — there's no "save later"
    // mode, because half-saved cluster state during a crash is a real
    // operational problem. The disk write is small (kilobytes) and fast.

    /// Claim ownership of a single slot. Errors if the slot is already
    /// owned by another node — Redis's own ADDSLOTS rejects re-claiming
    /// slots already assigned to *anyone*, even ourselves.
    pub fn claim_slot(&self, slot: u16) -> Result<(), String> {
        if !self.enabled {
            return Err("This instance has cluster support disabled".into());
        }
        if slot as usize >= CLUSTER_SLOTS {
            return Err(format!("Invalid slot {slot}"));
        }
        let mut slots = self.slots.write();
        if slots[slot as usize].is_some() {
            return Err(format!("Slot {slot} is already busy"));
        }
        slots[slot as usize] = Some(self.myself());
        self.slot_deletions.came_here([slot]);
        Ok(())
    }

    /// Remove a slot from this node's view of the cluster: CLUSTER DELSLOTS.
    /// See [`Self::release_slots`].
    pub fn release_slot(&self, slot: u16) -> Result<(), String> {
        self.release_slots(&[slot]).map(|_| ())
    }

    /// Remove `list` from this node's view of the cluster, whoever owns each
    /// slot, as Redis's DELSLOTS does. Errors if one of them is unassigned
    /// here already, and then changes none.
    ///
    /// Only this node's view changes. The owner's next claim of a slot it
    /// still holds wins it back here, unless ADDSLOTS or SETSLOT gives it to
    /// another node first. That is how `redis-cli --cluster fix` closes a slot
    /// an interrupted reshard left open: it picks one owner, and on each other
    /// node that has keys in the slot, deletes the slot and sets it importing
    /// from that owner, before moving the keys there. A node refused a slot
    /// another node owned, and the keys already moved stayed where no client
    /// was sent for them (#295). An IMPORTING state of the slot goes with
    /// it, as in Redis: the node is no longer taking the slot from anyone.
    ///
    /// For [`BY_HAND_GRACE`] after, a peer's claim of the slot does not take
    /// it back (see [`Self::note_by_hand`]). Fix sends `MULTI`, `DELSLOTS`, `ADDSLOTS`, `BUMPEPOCH`,
    /// `EXEC` to the owner it picks. In Redis nothing comes between the
    /// commands of a transaction; here the cluster bus applies claims on a
    /// thread of its own, and DELSLOTS saves `nodes.conf` before ADDSLOTS
    /// runs: the old owner's claim, arriving in that moment, took the slot
    /// back, and ADDSLOTS failed with `Slot N is already busy`.
    pub fn release_slots(&self, list: &[u16]) -> Result<usize, String> {
        if !self.enabled {
            return Err("This instance has cluster support disabled".into());
        }
        if let Some(slot) = list.iter().find(|&&s| s as usize >= CLUSTER_SLOTS) {
            return Err(format!("Invalid slot {slot}"));
        }
        // `slots` before `migrations`, as everywhere (see `ClusterState`).
        let mut slots = self.slots.write();
        if let Some(slot) = list.iter().find(|&&s| slots[s as usize].is_none()) {
            return Err(format!("Slot {slot} is already unassigned"));
        }
        let mut migrations = self.migrations.write();
        for &slot in list {
            slots[slot as usize] = None;
            if matches!(
                migrations.get(&slot),
                Some(MigrationState::Importing { .. })
            ) {
                migrations.remove(&slot);
            }
        }
        // A write routed here before this, and not yet in the write gate, is
        // routed again there: it used to run, on a slot this node no longer
        // serves (#337).
        crate::pause::routes_moved();
        self.note_by_hand(list.iter().copied());
        Ok(list.len())
    }

    /// Note that `slots` were just assigned (`SETSLOT NODE`) or removed
    /// (`DELSLOTS`) here: for [`BY_HAND_GRACE`],
    /// [`Self::apply_peer_slot_claims`] leaves them as they are.
    ///
    /// Not `ADDSLOTS`: that is how a cluster is first laid out, and a
    /// failover straight after must not wait.
    ///
    /// A peer's claim can be older than the command. `redis-cli --cluster
    /// reshard` moves a slot with `SETSLOT NODE` to the target, then the
    /// source, then the others. A heartbeat the source sent just before its
    /// `SETSLOT` still claims the slot, at the source's epoch. Heartbeats go
    /// once a second, so the target's own new epoch, taken from the highest
    /// it has heard of, can be below the source's latest, after the source
    /// was itself the target of the previous reshard. That claim then took
    /// the slot from the target, which no longer claimed it, while the
    /// source had given it away: no node served it, and the next reshard or
    /// fix found `Not all 16384 slots are covered`. The same claim arriving
    /// between fix's `DELSLOTS` and `ADDSLOTS` failed the `ADDSLOTS` ("Slot
    /// N is already busy"). After the grace the claim is gone, and claims
    /// are weighed by epoch as before.
    fn note_by_hand(&self, slots: impl IntoIterator<Item = u16>) {
        let mut by_hand = self.by_hand.lock();
        let now = std::time::Instant::now();
        by_hand.retain(|_, at| now.duration_since(*at) < BY_HAND_GRACE);
        for slot in slots {
            by_hand.insert(slot, now);
        }
    }

    /// Claim a contiguous inclusive range of slots. Atomic: if any slot in
    /// the range is already owned, none are claimed.
    ///
    /// "Atomic" here means: we validate first, then mutate under a single
    /// write lock. We don't need transactions — the lock IS our atomicity.
    pub fn claim_slot_range(&self, start: u16, end: u16) -> Result<usize, String> {
        if !self.enabled {
            return Err("This instance has cluster support disabled".into());
        }
        if start > end {
            return Err(format!("Invalid range {start}-{end}"));
        }
        if (end as usize) >= CLUSTER_SLOTS {
            return Err(format!("Invalid slot {end}"));
        }
        let mut slots = self.slots.write();
        // Validate the whole range first.
        for i in start..=end {
            if slots[i as usize].is_some() {
                return Err(format!("Slot {i} is already busy"));
            }
        }
        // All clear — claim them.
        for i in start..=end {
            slots[i as usize] = Some(self.myself());
        }
        self.slot_deletions.came_here(start..=end);
        Ok((end - start + 1) as usize)
    }

    /// Release a contiguous range: [`Self::release_slots`], all or nothing.
    pub fn release_slot_range(&self, start: u16, end: u16) -> Result<usize, String> {
        if !self.enabled {
            return Err("This instance has cluster support disabled".into());
        }
        if start > end {
            return Err(format!("Invalid range {start}-{end}"));
        }
        self.release_slots(&(start..=end).collect::<Vec<u16>>())
    }

    /// Wipe all slot ownership records on this node. Used by FLUSHSLOTS.
    /// Note: this only affects our local view; other nodes still think we
    /// own them until gossip propagates the change (Step 5).
    pub fn flush_slots(&self) {
        if !self.enabled {
            return;
        }
        let mut slots = self.slots.write();
        for s in slots.iter_mut() {
            *s = None;
        }
        // As for DELSLOTS (`release_slots`).
        crate::pause::routes_moved();
    }

    /// Set our own node's config_epoch. Used by SET-CONFIG-EPOCH (only
    /// allowed when current epoch is 0 — Redis enforces this to prevent
    /// epoch hijacking after the cluster is alive).
    pub fn set_my_config_epoch(&self, new_epoch: u64) -> Result<(), String> {
        if !self.enabled {
            return Err("This instance has cluster support disabled".into());
        }
        let mut nodes = self.nodes.write();
        let me = nodes
            .get_mut(&self.myself())
            .ok_or_else(|| "internal error: self not in nodes".to_string())?;
        if me.config_epoch != 0 {
            return Err("Node config epoch is already non-zero".into());
        }
        me.config_epoch = new_epoch;
        // Keep cluster current_epoch >= any node's config_epoch.
        let cur = self.current_epoch.load(Ordering::Relaxed);
        if new_epoch > cur {
            self.current_epoch.store(new_epoch, Ordering::Relaxed);
        }
        Ok(())
    }

    // ── Slot claim exchange (Step 5b) ────────────────────────────────────────

    /// Compute our current slot claims as contiguous ranges, paired with
    /// our current config_epoch. Used by the heartbeat task to populate
    /// outgoing PING/PONG messages.
    pub fn my_slot_claims(&self) -> Vec<crate::cluster::bus::SlotClaim> {
        if !self.enabled {
            return vec![];
        }
        let my_epoch = {
            let nodes = self.nodes.read();
            nodes
                .get(&self.myself())
                .map(|n| n.config_epoch)
                .unwrap_or(0)
        };
        let slots = self.slots.read();
        let mut out = Vec::new();
        let mut start: Option<usize> = None;
        for i in 0..CLUSTER_SLOTS {
            let owned = matches!(slots[i], Some(o) if o == self.myself());
            match (start, owned) {
                (None, true) => start = Some(i),
                (Some(s), false) => {
                    out.push(crate::cluster::bus::SlotClaim {
                        start: s as u16,
                        end: (i - 1) as u16,
                        epoch: my_epoch,
                    });
                    start = None;
                }
                _ => {}
            }
        }
        if let Some(s) = start {
            out.push(crate::cluster::bus::SlotClaim {
                start: s as u16,
                end: (CLUSTER_SLOTS - 1) as u16,
                epoch: my_epoch,
            });
        }
        out
    }

    /// Apply slot claims received from a peer. Each claim says "node
    /// `owner_id` owns slots [start..=end] as of epoch `epoch`".
    ///
    /// A claim is accepted for a slot that has no owner in our view, or whose
    /// current owner's config epoch is strictly lower. Equal epochs keep the
    /// current owner, so two PINGs crossing in flight cannot flap a slot.
    ///
    /// An unowned slot used to need a strictly higher epoch too, which with
    /// the comparison against 0 meant a claim at epoch 0 was never accepted:
    /// slots assigned with `CLUSTER ADDSLOTS` on a node that had not been
    /// given a config epoch were invisible to every other node.
    ///
    /// If the claims take the last slot away from the master this node
    /// belongs to — itself, or the master it replicates — the node follows the
    /// claimant instead. That is how a failover completes: the promoted
    /// replica announces the slots at its new epoch, the other replicas of the
    /// old master re-point to it, and the old master, if it ever returns,
    /// finds itself replaced and becomes a replica rather than serving a stale
    /// dataset beside the new one.
    pub fn apply_peer_slot_claims(
        &self,
        owner_id: NodeId,
        claims: &[crate::cluster::bus::SlotClaim],
    ) {
        if !self.enabled || claims.is_empty() {
            return;
        }
        let me = self.myself();
        // Don't accept claims for ourselves from peers — trust local state.
        if owner_id == me {
            return;
        }
        // We need each slot's current owner's epoch to compare. Snapshot the
        // nodes map's epochs once to avoid re-reading per slot.
        let (owner_epochs, my_shard_master): (ahash::AHashMap<NodeId, u64>, NodeId) = {
            let nodes = self.nodes.read();
            let epochs = nodes.iter().map(|(id, n)| (*id, n.config_epoch)).collect();
            let shard_master = nodes.get(&me).and_then(|n| n.master_id).unwrap_or(me);
            (epochs, shard_master)
        };

        let mut changed = false;
        let mut shard_lost_slots = false;
        // Slots this node itself was serving and has just lost.
        let mut lost: Vec<u16> = Vec::new();
        {
            // `slots` before `migrations`, the order everything else takes
            // them in (see `ClusterState`). Taken the other way round, a bus
            // thread holding `migrations` and waiting for `slots` met the end
            // of a slot migration holding `slots` and waiting for
            // `migrations` (`take_over_slots`, `hand_over_slots`), and both
            // waited for good.
            let mut slots = self.slots.write();
            let migrations = self.migrations.read();
            let by_hand = self.by_hand.lock();
            for claim in claims {
                for slot in claim.start..=claim.end {
                    let i = slot as usize;
                    if i >= CLUSTER_SLOTS || slots[i] == Some(owner_id) {
                        continue;
                    }
                    // Just assigned or removed here by hand: see `note_by_hand`.
                    if by_hand
                        .get(&slot)
                        .is_some_and(|at| at.elapsed() < BY_HAND_GRACE)
                    {
                        continue;
                    }
                    // A slot we are importing is ours to finalise with
                    // SETSLOT NODE; gossip must not move it out from under
                    // the migration.
                    if matches!(
                        migrations.get(&slot),
                        Some(MigrationState::Importing { .. })
                    ) {
                        continue;
                    }
                    let accept = match slots[i] {
                        None => true,
                        Some(cur) => claim.epoch > owner_epochs.get(&cur).copied().unwrap_or(0),
                    };
                    if accept {
                        if slots[i] == Some(my_shard_master) {
                            shard_lost_slots = true;
                        }
                        if slots[i] == Some(me) {
                            lost.push(slot);
                        }
                        slots[i] = Some(owner_id);
                        changed = true;
                    }
                }
            }
        }
        if shard_lost_slots || !lost.is_empty() {
            // A write routed here before this, and not yet in the write gate,
            // is routed again there (#337). Before the keys of the slots are
            // dropped, and before a handoff's pause is ended below
            // (`follow_new_master`): that is a change of the same flag, so a
            // write that finds the pause over finds this too. In an atomic
            // slot migration that drains this node, the destination's claim
            // can come before its answer to the `TAKEOVER`; a write held
            // between its routing and the gate then ran here once the pause
            // ended, and was lost with the resync.
            crate::pause::routes_moved();
        }
        if !changed {
            return;
        }

        // A MIGRATING marker only means something on the slot's owner.
        {
            let slots = self.slots.read();
            self.migrations.write().retain(|slot, state| {
                !matches!(state, MigrationState::Migrating { .. })
                    || slots[*slot as usize] == Some(me)
            });
        }

        if shard_lost_slots && !self.owns_any_slot(my_shard_master) {
            // Left with nothing, this node becomes a replica of the claimant,
            // and the resync replaces its dataset.
            self.follow_new_master(owner_id);
        } else if !lost.is_empty() {
            // Still a master, of fewer slots: the keys of the ones that went
            // are no longer this node's to keep.
            migration::drop_lost_slots(self, lost);
        }
    }

    /// Become a replica of `new_master` because it now owns the slots this
    /// node's shard used to. See [`Self::apply_peer_slot_claims`].
    fn follow_new_master(&self, new_master: NodeId) {
        let me = self.myself();
        let target = {
            let mut nodes = self.nodes.write();
            let Some(addr) = nodes
                .get(&new_master)
                .map(|n| (n.ip.clone(), n.client_port))
            else {
                return;
            };
            if let Some(my_node) = nodes.get_mut(&me) {
                my_node.flags.remove(NodeFlags::MASTER);
                my_node.flags |= NodeFlags::REPLICA;
                my_node.master_id = Some(new_master);
            }
            addr
        };
        // Whatever election we were running is moot: the shard has a master.
        *self.election.lock() = None;
        self.election_pending.store(false, Ordering::SeqCst);
        // If we were holding writes, for a manual failover or until we knew
        // whether we still owned our slots, the answer is in: let them go, to
        // be answered with a redirect to the new master. (The caller has
        // moved the routes: a write held between its routing and the gate is
        // routed again there.)
        *self.held_writes.lock() = None;
        crate::pause::clear_pause();
        if self.marked_failed.swap(false, Ordering::Relaxed) {
            self.refresh_state();
        }
        log_info!(
            "Cluster: Slots of this shard are now owned by {} — following it as a replica",
            node_id_str(&new_master)
        );
        self.start_replicating(&target.0, target.1);
    }

    /// Point the replication client at `ip:port`, if the server wired one in.
    fn start_replicating(&self, ip: &str, port: u16) {
        if ip.is_empty() || port == 0 {
            return;
        }
        if let Some(hooks) = self.hooks.get() {
            hooks.replica_starter.start(ip.to_string(), port);
        }
    }

    /// Bring the replication client in line with the cluster's view of this
    /// node. Called periodically from the bus.
    ///
    /// The cluster view is the authority on *who* our master is; the
    /// replication client only knows the address it was started with. They
    /// drift apart when the master's address changes (a node rebuilt on a new
    /// IP, learned through gossip), when the address was not known yet at
    /// startup, and when a promotion or demotion happened in one layer and
    /// the other was not told.
    pub fn reconcile_replication(&self) {
        let Some(hooks) = self.hooks.get() else {
            return;
        };
        let repl_state = &hooks.replica_starter.repl_state;
        let me = self.myself();
        let wanted = {
            let nodes = self.nodes.read();
            match nodes.get(&me).and_then(|n| n.master_id) {
                Some(master_id) => nodes.get(&master_id).map(|m| (m.ip.clone(), m.client_port)),
                None => None,
            }
        };
        match wanted {
            Some((ip, port)) => {
                if ip.is_empty() || port == 0 {
                    return;
                }
                if !repl_state.is_already_following(&ip, port) {
                    log_info!("Cluster: Replication target is now {ip}:{port} — re-pointing");
                    hooks.replica_starter.start(ip, port);
                }
            }
            None => {
                let i_am_master = self
                    .nodes
                    .read()
                    .get(&me)
                    .is_some_and(|n| n.flags.contains(NodeFlags::MASTER));
                if i_am_master && repl_state.is_slave() {
                    log_info!("Cluster: Cluster view says master — leaving replica mode");
                    repl_state.become_master();
                }
            }
        }
    }

    // ── Failure detection (Step 6) ────────────────────────────────────────────

    /// Record that `reporter` believes `reportee` is failing.
    /// Returns the current count of distinct reporters for `reportee` so
    /// the caller can decide whether to escalate to FAIL.
    pub fn record_failure_report(&self, reportee: NodeId, reporter: NodeId) -> usize {
        if reportee == self.myself() {
            // Don't track reports against ourselves — we know our own state.
            return 0;
        }
        // Only masters decide that a node has failed. Counting replicas let
        // them outvote the masters: a master's own replicas, losing sight of
        // it, could declare it failed between themselves and promote one.
        let reporter_votes = self
            .nodes
            .read()
            .get(&reporter)
            .is_some_and(|n| n.flags.contains(NodeFlags::MASTER));
        if !reporter_votes {
            return self.failure_report_count(reportee);
        }
        let now = now_ms();
        let mut reports = self.failure_reports.write();
        let entry = reports.entry(reportee).or_default();
        entry.insert(reporter, now);
        // Evict stale reporters before counting.
        let cutoff = now.saturating_sub(2 * self.node_timeout_ms);
        entry.retain(|_, ts| *ts > cutoff);
        entry.len()
    }

    /// Drop reports about `reportee` (when it's been forgotten or revives).
    pub fn clear_failure_reports(&self, reportee: NodeId) {
        self.failure_reports.write().remove(&reportee);
    }

    /// Number of distinct reporters who currently flag `reportee` as failing.
    /// Excludes stale reports.
    pub fn failure_report_count(&self, reportee: NodeId) -> usize {
        let now = now_ms();
        let cutoff = now.saturating_sub(2 * self.node_timeout_ms);
        match self.failure_reports.read().get(&reportee) {
            Some(reports) => reports.values().filter(|ts| **ts > cutoff).count(),
            None => 0,
        }
    }

    /// How many masters must agree before a node is declared failed: a
    /// majority of *all* slot-owning masters, the suspect included.
    ///
    /// The count used to leave out the suspect and every master already
    /// marked FAIL, so the bar dropped as nodes went missing. In a two-master
    /// cluster each side of a partition needed one vote — its own — to fail
    /// the other; with four masters split two and two, each pair could. Both
    /// sides then promoted replicas and served writes: split brain. Measured
    /// against the whole electorate, the minority side can never reach it.
    pub fn fail_quorum(&self, _reportee: NodeId) -> usize {
        self.voting_master_count() / 2 + 1
    }

    /// Walk every peer and apply timeout-based PFAIL.
    /// Called every tick by the failure-detection task.
    /// Returns the list of NodeIds we just escalated to FAIL, so the caller
    /// can broadcast the verdict.
    pub fn run_failure_detection_tick(&self) -> Vec<NodeId> {
        if !self.enabled {
            return vec![];
        }
        let now = now_ms();
        let timeout = self.node_timeout_ms;
        let me = self.myself();
        let mut newly_failed: Vec<NodeId> = Vec::new();

        // Phase 1: set PFAIL on peers whose PONG is stale, and drop handshake
        // placeholders whose MEET never completed.
        // Hold the write lock just long enough to flip flags; don't call
        // anything that re-locks nodes during this block.
        let i_am_master;
        {
            let mut nodes = self.nodes.write();
            i_am_master = nodes
                .get(&me)
                .is_some_and(|n| n.flags.contains(NodeFlags::MASTER));
            // A placeholder records when it was created in `ping_sent_ms`.
            let handshake_timeout = timeout.max(1_000);
            nodes.retain(|_, n| {
                !(n.flags.contains(NodeFlags::HANDSHAKE)
                    && now.saturating_sub(n.ping_sent_ms) > handshake_timeout)
            });
            for node in nodes.values_mut() {
                if node.id == me {
                    continue;
                }
                if node.flags.contains(NodeFlags::HANDSHAKE) || node.flags.contains(NodeFlags::FAIL)
                {
                    continue;
                }
                // pong_received_ms == 0 means "never heard from them" — for a
                // peer that just MEET'd us, give it one full timeout window
                // before suspecting failure.
                let last_seen = if node.pong_received_ms == 0 {
                    node.ping_sent_ms
                } else {
                    node.pong_received_ms
                };
                if last_seen != 0
                    && now.saturating_sub(last_seen) > timeout
                    && !node.flags.contains(NodeFlags::PFAIL)
                {
                    node.flags |= NodeFlags::PFAIL;
                }
            }
        }
        self.forgotten.write().retain(|_, until| *until > now);

        // Phase 2: for every PFAIL'd peer in our table or failure_reports,
        // decide whether quorum has been reached.
        let candidates: Vec<NodeId> = {
            let reports = self.failure_reports.read();
            let nodes = self.nodes.read();
            let mut set: ahash::AHashSet<NodeId> = ahash::AHashSet::new();
            for id in reports.keys() {
                set.insert(*id);
            }
            for (id, n) in nodes.iter() {
                if n.flags.contains(NodeFlags::PFAIL) {
                    set.insert(*id);
                }
            }
            set.into_iter().collect()
        };

        for reportee in candidates {
            // We only escalate a node we cannot reach ourselves. Others'
            // reports corroborate our suspicion; they do not replace it.
            let mut report_count = self.failure_report_count(reportee);
            {
                let nodes = self.nodes.read();
                match nodes.get(&reportee) {
                    Some(n) if n.flags.contains(NodeFlags::FAIL) => continue,
                    Some(n) if n.flags.contains(NodeFlags::PFAIL) => {
                        // Our own opinion is a vote only if we are a master.
                        if i_am_master {
                            report_count += 1;
                        }
                    }
                    _ => continue,
                }
            }

            let quorum = self.fail_quorum(reportee);
            if report_count >= quorum && quorum > 0 {
                let mut nodes = self.nodes.write();
                if let Some(n) = nodes.get_mut(&reportee)
                    && !n.flags.contains(NodeFlags::FAIL)
                {
                    n.mark_failed(now);
                    newly_failed.push(reportee);
                }
            }
        }

        self.refresh_state();
        newly_failed
    }

    /// Mark `id` failed on another node's say-so — a FAIL broadcast, sent by a
    /// node that reached quorum. Returns whether this changed anything.
    pub fn mark_node_failed(&self, id: NodeId) -> bool {
        if id == self.myself() {
            return false;
        }
        let changed = {
            let mut nodes = self.nodes.write();
            match nodes.get_mut(&id) {
                Some(n) if !n.flags.contains(NodeFlags::FAIL) => {
                    n.mark_failed(now_ms());
                    true
                }
                _ => false,
            }
        };
        if changed {
            self.refresh_state();
        }
        changed
    }

    /// A node we had marked FAIL has contacted us. Decide whether the verdict
    /// still stands.
    ///
    /// A replica, or a master with no slots, is un-failed at once: nothing
    /// depends on it. A master that owns slots is only un-failed once it has
    /// been FAIL for twice the node timeout — long enough that its replicas
    /// have had every chance to take over. If none did, taking the old master
    /// back is the only way the slots get served again; if one did, the old
    /// master has lost its slots by now and falls in the first group.
    ///
    /// Without this a FAIL verdict was permanent: nothing ever cleared the
    /// flag, and since failed nodes are not pinged, a master that came back
    /// from a reboot stayed failed in every peer's view for good.
    ///
    /// Must be called with `nodes` not held.
    pub fn maybe_clear_fail_on_contact(&self, id: NodeId) {
        let owns_slots = self.owns_any_slot(id);
        let now = now_ms();
        let cleared = {
            let mut nodes = self.nodes.write();
            match nodes.get_mut(&id) {
                Some(n) if n.flags.contains(NodeFlags::FAIL) => {
                    let serving_master = n.flags.contains(NodeFlags::MASTER) && owns_slots;
                    let waited = now.saturating_sub(n.fail_since_ms) > self.node_timeout_ms * 2;
                    if !serving_master || waited {
                        n.clear_failed();
                        true
                    } else {
                        false
                    }
                }
                _ => false,
            }
        };
        if cleared {
            log_info!(
                "Cluster: Node {} is reachable again — clearing FAIL",
                node_id_str(&id)
            );
            self.clear_failure_reports(id);
            self.refresh_state();
        }
    }

    /// `CLUSTER FORGET`: remove a node, release its slots, and refuse to
    /// re-admit it for [`FORGET_BAN_MS`].
    ///
    /// Without the ban a forgotten node was back within a second: the next
    /// gossip section mentioning it, or its own next PING, re-added it.
    pub fn forget_node(&self, id: NodeId) -> bool {
        let removed = {
            let mut nodes = self.nodes.write();
            let removed = nodes.remove(&id).is_some();
            if removed {
                let mut slots = self.slots.write();
                for s in slots.iter_mut() {
                    if *s == Some(id) {
                        *s = None;
                    }
                }
            }
            removed
        };
        if removed {
            self.forgotten.write().insert(id, now_ms() + FORGET_BAN_MS);
            self.clear_failure_reports(id);
        }
        removed
    }

    /// Whether `id` was recently removed with `CLUSTER FORGET`.
    pub fn is_forgotten(&self, id: &NodeId) -> bool {
        self.forgotten
            .read()
            .get(id)
            .is_some_and(|until| *until > now_ms())
    }

    // ── Replication topology (Step 7a) ────────────────────────────────────────

    /// Make this node a replica of `master_id`. Validates that the master
    /// exists in our routing table and is currently flagged MASTER.
    /// Persists the change and triggers our existing replication client
    /// machinery via the caller (see cmd_replicate).
    pub fn set_replica_of(&self, master_id: NodeId) -> Result<(String, u16), String> {
        if !self.enabled {
            return Err("This instance has cluster support disabled".into());
        }
        if master_id == self.myself() {
            return Err("Can't replicate myself".into());
        }

        // Step 1: read master info under a short read lock.
        let (master_ip, master_port) = {
            let nodes = self.nodes.read();
            let master = nodes
                .get(&master_id)
                .ok_or_else(|| "Unknown node".to_string())?;
            if !master.flags.contains(NodeFlags::MASTER) {
                return Err("Target node is not a master".into());
            }
            if master.flags.contains(NodeFlags::REPLICA) {
                return Err("Target node is itself a replica".into());
            }
            (master.ip.clone(), master.client_port)
        };

        // Step 2: refuse if we're a master with assigned slots — Redis
        // requires the candidate to be empty of slot ownership before it
        // can become a replica. This prevents accidental data loss.
        let owned = self.my_slot_count();
        if owned > 0 {
            return Err(format!(
                "To set a master as replica it must be empty of slots ({owned} owned)"
            ));
        }

        // Step 3: update our own node entry.
        {
            let mut nodes = self.nodes.write();
            let me = nodes
                .get_mut(&self.myself())
                .ok_or_else(|| "internal: self missing from nodes".to_string())?;
            me.flags.remove(NodeFlags::MASTER);
            me.flags |= NodeFlags::REPLICA;
            me.master_id = Some(master_id);
        }

        Ok((master_ip, master_port))
    }

    /// Convert ourselves back to a master role (no replication source).
    /// Used by manual failover and by REPLICAOF NO ONE inside a cluster.
    pub fn unset_replica(&self) {
        if !self.enabled {
            return;
        }
        let mut nodes = self.nodes.write();
        if let Some(me) = nodes.get_mut(&self.myself()) {
            me.flags.remove(NodeFlags::REPLICA);
            me.flags |= NodeFlags::MASTER;
            me.master_id = None;
        }
    }

    /// Return our master's NodeId if we are configured as a replica.
    pub fn my_master_id(&self) -> Option<NodeId> {
        let nodes = self.nodes.read();
        nodes.get(&self.myself()).and_then(|me| me.master_id)
    }

    // ── Election (Step 7b) ────────────────────────────────────────────────────

    /// Decide a FAILOVER_AUTH_REQUEST: `candidate`, a replica of `master_id`,
    /// asks for our vote at `request_epoch`. `force` marks a manual failover,
    /// where the master is healthy and is being replaced on purpose.
    ///
    /// The vote is granted only if every one of these holds:
    ///
    /// * we are a master that owns slots — the electorate is the nodes
    ///   serving data, the same set the quorum is counted over;
    /// * the candidate is, in our view, a replica of that master;
    /// * that master is FAIL in our view, unless the failover is manual;
    /// * the epoch is not behind the cluster's, and we have not voted in it —
    ///   one vote per epoch is what makes two winners impossible;
    /// * we have not voted for another replica of the same master within the
    ///   last two node timeouts.
    pub fn cast_vote(
        &self,
        candidate: NodeId,
        master_id: NodeId,
        request_epoch: u64,
        force: bool,
    ) -> bool {
        if !self.enabled {
            return false;
        }
        let me = self.myself();
        {
            let nodes = self.nodes.read();
            let i_am_master = nodes
                .get(&me)
                .is_some_and(|n| n.flags.contains(NodeFlags::MASTER));
            if !i_am_master {
                return false;
            }
            let candidate_ok = nodes.get(&candidate).is_some_and(|n| {
                n.flags.contains(NodeFlags::REPLICA) && n.master_id == Some(master_id)
            });
            if !candidate_ok {
                return false;
            }
            let master_failed = nodes
                .get(&master_id)
                .is_some_and(|n| n.flags.contains(NodeFlags::FAIL));
            if !master_failed && !force {
                return false;
            }
        }
        if !self.owns_any_slot(me) {
            return false;
        }
        if request_epoch < self.current_epoch.load(Ordering::SeqCst) {
            return false;
        }
        // A failover and the handover of an atomic slot migration exclude
        // each other (#516). For a replica of this node, see
        // `failover_may_begin`. For one of a master this node imports from,
        // the import's attempt is given up: its `TAKEOVER` is refused.
        let fenced = if master_id == me {
            self.failover_may_begin()
        } else {
            self.slot_migrations.fence_imports(
                Some(master_id),
                "a replica of the source is being elected to take its slots over",
            )
        };
        if let Err(why) = fenced {
            log_info!(
                "Cluster: Not voting for {} to take over from {}: {why}",
                node_id_str(&candidate),
                node_id_str(&master_id)
            );
            return false;
        }

        let now = now_ms();
        let mut voted_for = self.last_vote_for.lock();
        if let Some(at) = voted_for.get(&master_id)
            && now.saturating_sub(*at) < self.node_timeout_ms * 2
        {
            return false;
        }

        // Compare-and-swap on last_vote_epoch.
        loop {
            let current = self.last_vote_epoch.load(Ordering::SeqCst);
            if request_epoch <= current {
                return false; // already voted in this or a higher epoch
            }
            if self
                .last_vote_epoch
                .compare_exchange(current, request_epoch, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                // Voting on epoch N implies seeing N.
                self.current_epoch
                    .fetch_max(request_epoch, Ordering::SeqCst);
                voted_for.insert(master_id, now);
                return true;
            }
        }
    }

    /// Begin an election to replace `master_id`. Returns the new epoch. Bumps
    /// current_epoch so the election claim takes the highest epoch in the
    /// cluster. `deadline_ms`: see [`ElectionState::deadline_ms`].
    pub fn start_election(&self, master_id: NodeId, manual: bool, deadline_ms: Option<u64>) -> u64 {
        let new_epoch = self.bump_epoch();
        let mut election = self.election.lock();
        *election = Some(ElectionState {
            epoch: new_epoch,
            failed_master_id: master_id,
            started_at_ms: now_ms(),
            votes_received: ahash::AHashSet::new(),
            completed: false,
            manual,
            deadline_ms,
        });
        new_epoch
    }

    /// Record an incoming vote ACK. Returns the current vote count.
    pub fn record_vote(&self, voter: NodeId, epoch: u64) -> usize {
        // Only a slot-owning master's vote counts, the same rule the voter
        // applies to itself before granting one.
        let voter_counts = self
            .nodes
            .read()
            .get(&voter)
            .is_some_and(|n| n.flags.contains(NodeFlags::MASTER))
            && self.owns_any_slot(voter);
        let mut election = self.election.lock();
        match election.as_mut() {
            Some(e) if e.epoch == epoch && !e.completed => {
                if voter_counts {
                    e.votes_received.insert(voter);
                } else {
                    log_info!(
                        "Cluster: Vote from {} for epoch {epoch} not counted: it is not a master                          owning slots here",
                        node_id_str(&voter)
                    );
                }
                e.votes_received.len()
            }
            other => {
                log_info!(
                    "Cluster: Vote from {} for epoch {epoch} not counted: the election here is {}",
                    node_id_str(&voter),
                    match other {
                        None => "over".to_string(),
                        Some(e) => format!("at epoch {}, completed {}", e.epoch, e.completed),
                    }
                );
                0
            }
        }
    }

    /// Mark current election as completed (won or abandoned). Returns
    /// previous state so caller can decide whether promotion happened.
    pub fn end_election(&self) -> Option<ElectionState> {
        self.election.lock().take()
    }

    /// Votes needed to win an election: a majority of all slot-owning
    /// masters, the one being replaced included.
    ///
    /// Like [`Self::fail_quorum`], this used to be counted over the masters
    /// still standing, so the fewer were reachable the easier an election
    /// became — a replica cut off with a single master could elect itself.
    pub fn election_quorum(&self, _failed_master_id: NodeId) -> usize {
        self.voting_master_count() / 2 + 1
    }

    /// Promote ourselves: become master, claim the failed master's slots,
    /// stamp our config_epoch to the election epoch, broadcast PONG.
    /// Caller is responsible for updating repl_state.role, and then for
    /// [`Self::drop_slots_lost_while_promoting`] of the slots this returns:
    /// the ones it claimed.
    pub fn promote_self(&self, failed_master_id: NodeId, election_epoch: u64) -> Vec<u16> {
        if !self.enabled {
            return Vec::new();
        }

        // Phase 1: claim the failed master's slots.
        let mut claimed = Vec::new();
        {
            let me = self.myself();
            let mut slots = self.slots.write();
            for (slot, owner) in slots.iter_mut().enumerate() {
                if matches!(*owner, Some(o) if o == failed_master_id) {
                    *owner = Some(me);
                    claimed.push(slot as u16);
                }
            }
            self.slot_deletions.came_here(claimed.iter().copied());
        }

        // Phase 2: update our own node entry.
        {
            let mut nodes = self.nodes.write();
            if let Some(me) = nodes.get_mut(&self.myself()) {
                me.flags.remove(NodeFlags::REPLICA);
                me.flags |= NodeFlags::MASTER;
                me.master_id = None;
                me.config_epoch = election_epoch;
            }
            // Mark the old master as no longer authoritative.
            if let Some(old_master) = nodes.get_mut(&failed_master_id) {
                // Keep its FAIL flag, but it no longer has master role.
                // (If it comes back, gossip's epoch comparison will demote it.)
                old_master.flags.remove(NodeFlags::MASTER);
            }
        }

        // Phase 3: persist (which also refreshes the serving state).
        let _ = self.save_to_disk();
        claimed
    }

    /// Of the slots a promotion `claimed`, those another master has taken
    /// since. See [`Self::drop_slots_lost_while_promoting`].
    fn slots_lost_while_promoting(&self, claimed: &[u16]) -> Vec<u16> {
        let me = self.myself();
        let owners = self.slots.read();
        claimed
            .iter()
            .copied()
            .filter(|&slot| owners[slot as usize].is_some_and(|owner| owner != me))
            .collect()
    }

    /// After a promotion, once the replication layer has made this node a
    /// master too: drop the keys of the slots it `claimed` that another
    /// master's claim has taken since. A claim between the two found this
    /// node a master of the slots and a replica still to the replication
    /// layer, and left their keys here (`drop_lost_slots` deletes only from
    /// a master's own dataset); they would have stayed, stale, until a
    /// restart. Keys of a slot removed with `DELSLOTS` are left, as
    /// `DELSLOTS` leaves them.
    pub fn drop_slots_lost_while_promoting(&self, claimed: &[u16]) {
        let lost = self.slots_lost_while_promoting(claimed);
        if !lost.is_empty() {
            migration::drop_lost_slots(self, lost);
        }
    }

    // ── Migration state (Step 7c) ─────────────────────────────────────────────

    pub fn migration_state(&self, slot: u16) -> Option<MigrationState> {
        self.migrations.read().get(&slot).copied()
    }

    pub fn set_migrating(&self, slot: u16, dest_id: NodeId) -> Result<(), String> {
        if !self.enabled {
            return Err("This instance has cluster support disabled".into());
        }
        if !self.owns_slot(slot) {
            return Err(format!("I'm not the owner of slot {slot}"));
        }
        if dest_id == self.myself() {
            return Err("Can't migrate to myself".into());
        }
        // Verify destination is a known master.
        {
            let nodes = self.nodes.read();
            match nodes.get(&dest_id) {
                Some(n) if n.flags.contains(NodeFlags::MASTER) => {}
                Some(_) => return Err("Target is not a master".into()),
                None => return Err("Unknown destination node".into()),
            }
        }
        self.migrations
            .write()
            .insert(slot, MigrationState::Migrating { dest_id });
        Ok(())
    }

    pub fn set_importing(&self, slot: u16, src_id: NodeId) -> Result<(), String> {
        if !self.enabled {
            return Err("This instance has cluster support disabled".into());
        }
        if self.owns_slot(slot) {
            return Err(format!("I'm already the owner of slot {slot}"));
        }
        if src_id == self.myself() {
            return Err("Can't import from myself".into());
        }
        {
            let nodes = self.nodes.read();
            if !nodes.contains_key(&src_id) {
                return Err("Unknown source node".into());
            }
        }
        let mut migrations = self.migrations.write();
        migrations.insert(slot, MigrationState::Importing { src_id });
        self.slot_deletions.came_here([slot]);
        Ok(())
    }

    pub fn clear_migration(&self, slot: u16) {
        self.migrations.write().remove(&slot);
    }

    /// CLUSTER SETSLOT NODE — finalize. Set ownership to new_owner and clear
    /// any migration state. Bumps current_epoch so the new ownership wins
    /// any gossip race.
    pub fn finalize_slot(&self, slot: u16, new_owner: NodeId) -> Result<(), String> {
        if !self.enabled {
            return Err("This instance has cluster support disabled".into());
        }
        if slot as usize >= CLUSTER_SLOTS {
            return Err(format!("Invalid slot {slot}"));
        }
        if !self.nodes.read().contains_key(&new_owner) {
            return Err("Unknown node".into());
        }
        // If we're becoming the new owner, bump our config_epoch via election epoch
        // so peers accept the claim.
        if new_owner == self.myself() {
            let new_epoch = self.bump_epoch();
            let mut nodes = self.nodes.write();
            if let Some(me) = nodes.get_mut(&self.myself())
                && new_epoch > me.config_epoch
            {
                me.config_epoch = new_epoch;
            }
        }
        {
            let mut owners = self.slots.write();
            owners[slot as usize] = Some(new_owner);
            if new_owner == self.myself() {
                self.slot_deletions.came_here([slot]);
            }
        }
        self.migrations.write().remove(&slot);
        self.note_by_hand([slot]);
        Ok(())
    }

    /// Take ownership of `slots` at the end of an atomic migration to this
    /// node. One new epoch covers them all, so the claim wins over the
    /// previous owner's wherever the two meet.
    ///
    /// Saved in the cluster config before anything else sees it: a client
    /// is not served the slots, nor do they go out in this node's claims,
    /// until the config says they are this node's. They used to be taken
    /// first and saved after; when the save failed, the destination of a
    /// migration gave them back, having served writes to them meanwhile,
    /// lost when it gave them back, and its claim could have reached the
    /// source in that time, which then dropped its keys of them, and owned
    /// them in nobody's view (#337). Not saved, nothing changes.
    pub fn take_over_slots(&self, slots: impl Iterator<Item = u16>) -> std::io::Result<()> {
        let t = std::time::Instant::now();
        let mut steps: Vec<(&str, u128)> = Vec::new();
        let slots: Vec<u16> = slots.collect();
        let me = self.myself();
        let new_epoch = self.bump_epoch();
        steps.push(("epoch", t.elapsed().as_millis()));
        if let Some(node) = self.nodes.write().get_mut(&me)
            && new_epoch > node.config_epoch
        {
            node.config_epoch = new_epoch;
        }
        steps.push(("nodes", t.elapsed().as_millis()));
        let saved = persist::save_from_state_then(self, &slots, || {
            let mut owners = self.slots.write();
            let mut migrations = self.migrations.write();
            for &slot in &slots {
                owners[slot as usize] = Some(me);
                migrations.remove(&slot);
            }
            self.slot_deletions.came_here(slots.iter().copied());
        });
        steps.push(("save", t.elapsed().as_millis()));
        if saved.is_ok() {
            self.refresh_state();
            steps.push(("refresh", t.elapsed().as_millis()));
        }
        if t.elapsed() > std::time::Duration::from_millis(100) {
            log_info!("Cluster: slow takeover, ms at each step: {steps:?}");
        }
        saved
    }

    /// Record that `slots` now belong to `new_owner`, at the end of an atomic
    /// migration away from this node. Returns the ones it took from this
    /// node: only their keys are the caller's to delete.
    ///
    /// A slot that is no longer this node's is left as it is. The
    /// destination's claim can reach this node before its answer to the
    /// `TAKEOVER`, and take the slots first (`apply_peer_slot_claims`). What
    /// became of their keys was decided then: they were dropped with the
    /// lost slots, or, if that left this node with no slots, it followed the
    /// destination as a replica, and its dataset is now the destination's.
    /// The source used to delete the keys of every slot it handed over all
    /// the same, from its master's dataset in the second case (#348). The
    /// claim takes a slot under `slots`, as this does, so each slot leaves
    /// this node one way only.
    pub fn hand_over_slots(&self, slots: impl Iterator<Item = u16>, new_owner: NodeId) -> Vec<u16> {
        let me = self.myself();
        let mut handed = Vec::new();
        {
            let mut owners = self.slots.write();
            let mut migrations = self.migrations.write();
            for slot in slots {
                if owners[slot as usize] == Some(me) {
                    owners[slot as usize] = Some(new_owner);
                    handed.push(slot);
                }
                migrations.remove(&slot);
            }
        }
        // A write routed here before this, and held up before the write gate
        // until the handoff lets writes go, is routed again there (#331).
        // Before the caller ends the handoff's pause: that is a change of the
        // same flag, so a write that finds the pause over finds this too.
        crate::pause::routes_moved();
        // The caller saves, once writes are no longer paused.
        self.refresh_state();
        handed
    }

    // ── Writes held until this master knows it still owns its slots ──────────

    /// Hold writes until this master has made sure it still owns its slots
    /// (asked from `ask_from_ms` on, by the failure detector: see
    /// [`Self::held_writes_due`]), or has learned that a replica took them
    /// over and follows it ([`Self::follow_new_master`]). For a manual
    /// failover they are paused, as a handoff pauses them; for a FAIL verdict
    /// they are refused, with every other command on this master's keys
    /// (`-CLUSTERDOWN`, see [`Self::note_marked_failed`]).
    ///
    /// Two things put a master in doubt (#242). Its replica asked to take
    /// over (`CLUSTER FAILOVER`): the replica gives the failover up if it has
    /// not won it in time, and the master used to take writes again when its
    /// hold for it ran out. But a master slow enough for the failover to run
    /// out of time can be slow enough to be marked FAIL, and its replica then
    /// stands again, of its own accord. Or the cluster has marked it FAIL and
    /// told it so: its replicas are about to stand. Either way, what it
    /// acknowledged between a replica's promotion and its own hearing of it
    /// was lost.
    pub fn hold_writes_until_confirmed(&self, reason: HoldReason, ask_from_ms: u64) {
        let pause = reason == HoldReason::ManualFailover;
        // Not to be let go by a handoff that ends in between (see
        // `end_handoff_hold`).
        let _holds = self.handoff_holds.lock();
        // A migration away from this node that has not handed its slots
        // over yet gives the handover up (`begin_handover`).
        self.failovers_begun.fetch_add(1, Ordering::SeqCst);
        if pause {
            crate::pause::hold_writes();
        } else {
            self.marked_failed.store(true, Ordering::Relaxed);
            self.refresh_state();
        }
        let mut held = self.held_writes.lock();
        match held.as_mut() {
            Some(h) => {
                h.ask_from_ms = h.ask_from_ms.max(ask_from_ms);
                h.paused |= pause;
            }
            None => {
                *held = Some(HeldWrites {
                    reason,
                    since_ms: now_ms(),
                    ask_from_ms,
                    asking: false,
                    logged_ms: 0,
                    paused: pause,
                })
            }
        }
    }

    /// How many failovers of this node have begun: a replica of it was told
    /// to go ahead (`CLUSTER FAILOVER`) or given its vote, or it was marked
    /// FAIL. See [`Self::begin_handover`].
    pub(crate) fn failovers_begun(&self) -> u64 {
        self.failovers_begun.load(Ordering::SeqCst)
    }

    /// The handoff of an atomic slot migration away from this node is about
    /// to ask the destination to take the slots over (`TAKEOVER`): whether
    /// it may (#516). `still` says whether this node still is what it was
    /// when the migration began: the master of the slots, with its own
    /// dataset and config epoch. No failover of this node may have begun
    /// since the migration did (`failovers_since`, from
    /// [`Self::failovers_begun`]), nor be in doubt: writes held until this
    /// node knows whether a replica of it took its slots over.
    ///
    /// A replica of the source promoted while the slots moved used to lose
    /// them to the destination all the same. The old master, a replica by
    /// then, handed them over from its own dataset, and the destination took
    /// them at a newer epoch. What the new master had acknowledged was lost:
    /// the writes since its promotion, which the migration's stream never
    /// carried, and the writes it took until it heard of the destination's
    /// claim, deleted with the slots. A failover and a handover now exclude
    /// each other: this is decided under the same lock as
    /// [`Self::failover_may_begin`], which refuses a failover from here
    /// until [`Self::end_handover`], and for a moment after.
    pub(crate) fn begin_handover(
        &self,
        failovers_since: u64,
        still: impl FnOnce() -> Result<(), String>,
    ) -> Result<(), String> {
        let _holds = self.handoff_holds.lock();
        if self.failovers_begun.load(Ordering::SeqCst) != failovers_since {
            return Err(
                "a failover of this node began during the migration (CLUSTER FAILOVER \
                        on a replica of it, a vote for one, or a FAIL verdict)"
                    .into(),
            );
        }
        if self.held_writes.lock().is_some() {
            return Err(
                "this node does not know yet whether a replica of it has taken its slots over"
                    .into(),
            );
        }
        still()?;
        self.handovers.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    /// A handover that [`Self::begin_handover`] let begin has ended, however
    /// it ended.
    pub(crate) fn end_handover(&self) {
        let _holds = self.handoff_holds.lock();
        let n = self.handovers.load(Ordering::SeqCst);
        self.handovers.store(n.saturating_sub(1), Ordering::SeqCst);
        self.last_handover_end_ms.store(now_ms(), Ordering::SeqCst);
    }

    /// Whether a replica of this master may take its slots over now, as it
    /// asks with `CLUSTER FAILOVER` or for this node's vote (#516). Not while
    /// an atomic slot migration away from this node is handing slots over,
    /// nor for [`HANDOVER_FAILOVER_GRACE_MS`] after one ended: a promoted
    /// replica claims the slots it sees this node own, and until it has
    /// heard of the destination's claim, those include the ones that moved.
    /// Nor while an import into this node is taking slots over.
    ///
    /// If it may, a migration away from this node that has not reached its
    /// handover gives it up ([`Self::begin_handover`]), and the imports into
    /// this node give their attempt up: a replica that takes over has only
    /// what the stream has brought, and is no destination of theirs.
    pub(crate) fn failover_may_begin(&self) -> Result<(), String> {
        let _holds = self.handoff_holds.lock();
        if self.handovers.load(Ordering::SeqCst) > 0 {
            return Err(
                "an atomic slot migration away from this node is handing its slots over".into(),
            );
        }
        let since = now_ms().saturating_sub(self.last_handover_end_ms.load(Ordering::SeqCst));
        if since < HANDOVER_FAILOVER_GRACE_MS {
            return Err(format!(
                "an atomic slot migration away from this node ended {since} ms ago: its \
                 replicas may not have heard yet which slots moved"
            ));
        }
        self.slot_migrations
            .fence_imports(None, "this node is failing over to a replica of it")?;
        self.failovers_begun.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    /// Whether this master holds writes until it has made sure it still owns
    /// its slots.
    pub fn writes_held(&self) -> bool {
        self.held_writes.lock().is_some()
    }

    /// Let the writes held go, if they are held, and log `why`.
    ///
    /// Not while an atomic slot migration away from this node holds them
    /// for its handoff as well ([`Self::hold_writes_for_handoff`]): that
    /// lets them go when it ends. A manual failover that ended in the middle
    /// of one let them go, and what was written then was not sent to the
    /// destination, and was lost with the slots (#337).
    pub fn release_held_writes(&self, why: &str) {
        let holds = self.handoff_holds.lock();
        let Some(held) = self.held_writes.lock().take() else {
            return;
        };
        // Said before it is done: a client is answered the moment it is.
        log_info!(
            "Cluster: Writes held since {} resumed after {} ms: {why}",
            held.reason.describe(),
            now_ms().saturating_sub(held.since_ms)
        );
        if self.marked_failed.swap(false, Ordering::Relaxed) {
            self.refresh_state();
        }
        if held.paused && *holds == 0 && crate::pause::handoff() {
            crate::pause::clear_pause();
        }
    }

    /// Hold writes for the handoff of an atomic slot migration away from
    /// this node, until [`Self::end_handoff_hold`]. See `migration::migrate_out`.
    pub(crate) fn hold_writes_for_handoff(&self) {
        let mut holds = self.handoff_holds.lock();
        *holds += 1;
        crate::pause::hold_writes();
    }

    /// A handoff that [`Self::hold_writes_for_handoff`] held writes for has
    /// ended, however it ended: let them go, unless they have been let go
    /// already (`CLIENT UNPAUSE`, or this master's replica took its slots
    /// over), or another handoff or a manual failover (#242) still holds
    /// them, which lets them go itself.
    ///
    /// Not a hold for a FAIL verdict (`MarkedFailed`): that one refuses
    /// writes (`marked_failed`) and does not pause them, so it does not end
    /// the pause when it is let go (`release_held_writes`). With one taken
    /// during the handoff, the handoff left its pause on for the hold to end,
    /// the hold did not, and writes waited until the pause ran out, a
    /// century later (#349).
    pub(crate) fn end_handoff_hold(&self) {
        let mut holds = self.handoff_holds.lock();
        *holds = holds.saturating_sub(1);
        if *holds == 0
            && crate::pause::handoff()
            && !self.held_writes.lock().as_ref().is_some_and(|h| h.paused)
        {
            crate::pause::clear_pause();
        }
    }

    /// From the failure detector: whether to ask the cluster, now, whether
    /// this master still owns its slots. True once per round; the round ends
    /// with [`Self::held_writes_answered`].
    pub fn held_writes_due(&self) -> bool {
        let mut held = self.held_writes.lock();
        let Some(h) = held.as_mut() else {
            return false;
        };
        if h.paused && !crate::pause::handoff() {
            // Let go some other way (`CLIENT UNPAUSE`).
            h.paused = false;
            if !self.marked_failed.load(Ordering::Relaxed) {
                let reason = h.reason;
                *held = None;
                drop(held);
                log_warn!(
                    "Cluster: Writes held since {} were let go before this node had made sure \
                     it still owns its slots",
                    reason.describe()
                );
                return false;
            }
        }
        if h.asking || now_ms() < h.ask_from_ms {
            return false;
        }
        h.asking = true;
        true
    }

    /// The end of a round of asking: `Ok` if this master still owns its
    /// slots, or why it cannot be sure of it yet.
    pub fn held_writes_answered(&self, answer: Result<(), String>) {
        let now = now_ms();
        let mut held = self.held_writes.lock();
        let Some(h) = held.as_mut() else {
            return;
        };
        h.asking = false;
        match answer {
            Ok(()) => {
                drop(held);
                self.release_held_writes("this node still owns its slots");
            }
            Err(why) => {
                h.ask_from_ms = now + HELD_WRITES_RETRY_MS;
                if now.saturating_sub(h.logged_ms) >= 5_000 {
                    h.logged_ms = now;
                    let (reason, since) = (h.reason, h.since_ms);
                    drop(held);
                    log_warn!(
                        "Cluster: Writes held since {} ({} ms ago) stay held: {why}",
                        reason.describe(),
                        now.saturating_sub(since)
                    );
                }
            }
        }
    }

    /// `reporter` says the cluster has marked this node FAIL. A master that
    /// owns slots, and has a replica that can take them over, serves none of
    /// them until it has made sure it still owns them (see
    /// [`Self::hold_writes_until_confirmed`]): its replicas are about to
    /// stand for election, and what it acknowledged after one of them was
    /// promoted, and before it heard of it, would be lost. It answers
    /// `-CLUSTERDOWN`, as a node does that sees the cluster down — and as
    /// Redis does on a master whose messages do not get through, which sees
    /// the others as unreachable. This node can still hear them, so its own
    /// view of the cluster says nothing is wrong.
    pub fn note_marked_failed(&self, reporter: NodeId) {
        if !self.enabled {
            return;
        }
        let me = self.myself();
        let master = self
            .nodes
            .read()
            .get(&me)
            .is_some_and(|n| n.flags.contains(NodeFlags::MASTER));
        if !master || !self.owns_any_slot(me) {
            return;
        }
        if self.live_replicas_of_me() == 0 {
            log_warn!(
                "Cluster: {} says this node has been marked FAIL; no replica of it can take its \
                 slots over, so it serves on",
                node_id_str(&reporter)
            );
            return;
        }
        let first = !self.marked_failed.load(Ordering::Relaxed);
        self.hold_writes_until_confirmed(HoldReason::MarkedFailed, now_ms());
        if first {
            log_warn!(
                "Cluster: {} says this node has been marked FAIL: it serves none of its slots \
                 (CLUSTERDOWN) until it has made sure it still owns them, or a replica has \
                 taken them over",
                node_id_str(&reporter)
            );
        }
    }

    // ── Reset (CLUSTER RESET) ─────────────────────────────────────────────────

    /// Soft reset: clears slot assignments, peer table (keeping ourselves),
    /// migrations, and failure reports. Keeps our node ID and config_epoch.
    /// Used when removing a node from one cluster to add to another.
    pub fn soft_reset(&self) {
        if !self.enabled {
            return;
        }
        // Serialize with heartbeat merges and temporarily ban the old peers.
        // An in-flight heartbeat may already have passed its membership check;
        // without this guard it can repopulate slots after the reset returns.
        let mut forgotten = self.forgotten.write();
        let me = self.myself();
        let until = now_ms() + FORGET_BAN_MS;
        for id in self.nodes.read().keys().copied().filter(|id| *id != me) {
            forgotten.insert(id, until);
        }
        // Clear all slot ownership.
        {
            let mut slots = self.slots.write();
            for s in slots.iter_mut() {
                *s = None;
            }
        }
        // As for DELSLOTS (`release_slots`).
        crate::pause::routes_moved();
        // Clear peer table — keep only ourselves.
        {
            let mut nodes = self.nodes.write();
            let me = nodes.get(&self.myself()).cloned();
            nodes.clear();
            if let Some(mut me) = me {
                // Reset our own role to plain master, no replication source.
                me.flags.remove(NodeFlags::REPLICA);
                me.flags |= NodeFlags::MASTER | NodeFlags::MYSELF;
                me.master_id = None;
                nodes.insert(self.myself(), me);
            }
        }
        // Clear transient state.
        self.migrations.write().clear();
        self.failure_reports.write().clear();
        *self.election.lock() = None;
        // Peers are banned and the tables cleared: a heartbeat merged from
        // here on finds the ban. Let go of before `release_held_writes`,
        // which takes `handoff_holds`, a lock that comes before `forgotten`.
        drop(forgotten);
        self.release_held_writes("the node was reset");
        let _ = self.save_to_disk();
    }

    /// Hard reset: a soft reset, plus a new node ID and zeroed epochs. The
    /// caller is responsible for flushing the data store.
    ///
    /// The new ID is the point of a hard reset — it is what lets the node
    /// join another cluster as a stranger rather than as a returning member
    /// with an old identity. It used to keep its ID, and log that it had.
    pub fn hard_reset(&self) -> NodeId {
        if !self.enabled {
            return self.myself();
        }
        self.soft_reset();
        self.current_epoch.store(0, Ordering::SeqCst);
        self.last_vote_epoch.store(0, Ordering::SeqCst);
        self.last_vote_for.lock().clear();
        let new_id = generate_node_id();
        {
            let mut nodes = self.nodes.write();
            let mut myself = self.myself.write();
            if let Some(mut me) = nodes.remove(&*myself) {
                me.id = new_id;
                me.config_epoch = 0;
                nodes.insert(new_id, me);
            }
            *myself = new_id;
        }
        let _ = self.save_to_disk();
        new_id
    }
}

/// How long after an atomic slot migration away from a master handed slots
/// over its replicas may not take them over (`failover_may_begin`), nor
/// the destination vote for one of them: two heartbeats, time for them to
/// hear of the destination's claim (#516).
pub(crate) const HANDOVER_FAILOVER_GRACE_MS: u64 = 3_000;

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn empty_slots() -> Box<[Option<NodeId>; CLUSTER_SLOTS]> {
    let v: Vec<Option<NodeId>> = vec![None; CLUSTER_SLOTS];
    v.into_boxed_slice()
        .try_into()
        .expect("vec is exactly CLUSTER_SLOTS long")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_state_owns_everything() {
        let cs = ClusterState::new_disabled();
        for slot in [0u16, 1, 5474, 12182, 16383] {
            assert!(cs.owns_slot(slot));
            assert_eq!(cs.slot_owner(slot), None);
        }
    }

    #[test]
    fn enabled_state_owns_nothing_initially() {
        let cs = ClusterState::new_enabled(None, 15000);
        assert!(!cs.owns_slot(0));
        assert!(!cs.owns_slot(16383));
        assert!(cs.nodes.read().contains_key(&cs.myself()));
        assert_eq!(cs.cluster_state_str(), "fail"); // no slots covered yet
    }

    #[test]
    fn slot_assignment_round_trip() {
        let cs = ClusterState::new_enabled(None, 15000);
        let me = cs.myself();
        cs.slots.write()[100] = Some(me);
        assert!(cs.owns_slot(100));
        assert!(!cs.owns_slot(101));
        assert_eq!(cs.slot_owner(100), Some(me));
        assert_eq!(cs.my_slot_count(), 1);
    }

    #[test]
    fn full_coverage_means_ok() {
        let cs = ClusterState::new_enabled(None, 15000);
        let me = cs.myself();
        {
            let mut slots = cs.slots.write();
            for i in 0..CLUSTER_SLOTS {
                slots[i] = Some(me);
            }
        }
        cs.refresh_state();
        assert_eq!(cs.cluster_state_str(), "ok");
        assert_eq!(cs.my_slot_count(), CLUSTER_SLOTS);
    }

    // ── Helpers for the multi-node scenarios below ───────────────────────

    /// Add a master to `cs`'s node table and give it `slots`.
    fn add_master(cs: &ClusterState, slots: std::ops::RangeInclusive<usize>) -> NodeId {
        let id = generate_node_id();
        let mut node = ClusterNode::new(id, "10.0.0.1".into(), 7000, 17000);
        node.flags |= NodeFlags::MASTER;
        node.pong_received_ms = now_ms();
        cs.nodes.write().insert(id, node);
        let mut table = cs.slots.write();
        for s in slots {
            table[s] = Some(id);
        }
        id
    }

    fn add_replica(cs: &ClusterState, master: NodeId) -> NodeId {
        let id = generate_node_id();
        let mut node = ClusterNode::new(id, "10.0.0.2".into(), 7001, 17001);
        node.flags |= NodeFlags::REPLICA;
        node.master_id = Some(master);
        node.pong_received_ms = now_ms();
        cs.nodes.write().insert(id, node);
        id
    }

    fn set_flag(cs: &ClusterState, id: NodeId, flag: NodeFlags) {
        cs.nodes.write().get_mut(&id).unwrap().flags |= flag;
    }

    /// A cluster as seen from a master that owns the first third of the slots,
    /// with two other slot-owning masters. Returns `(state, other_a, other_b)`.
    fn three_master_cluster() -> (ClusterState, NodeId, NodeId) {
        let cs = ClusterState::new_enabled(None, 15000);
        cs.claim_slot_range(0, 5460).unwrap();
        let a = add_master(&cs, 5461..=10922);
        let b = add_master(&cs, 10923..=16383);
        cs.refresh_state();
        (cs, a, b)
    }

    /// A failover of this master and the handover of an atomic slot
    /// migration away from it exclude each other (#516).
    #[test]
    fn a_failover_and_a_slot_handover_exclude_each_other() {
        let (cs, _, _) = three_master_cluster();
        // A failover begun during the migration: its handover is refused.
        let begun = cs.failovers_begun();
        cs.failover_may_begin().unwrap();
        let refused = cs.begin_handover(begun, || Ok(())).unwrap_err();
        assert!(
            refused.contains("a failover of this node began"),
            "{refused}"
        );

        // A handover begun first: a failover is refused until it has ended,
        // and for a moment after.
        let begun = cs.failovers_begun();
        cs.begin_handover(begun, || Ok(())).unwrap();
        let refused = cs.failover_may_begin().unwrap_err();
        assert!(refused.contains("handing its slots over"), "{refused}");
        cs.end_handover();
        let refused = cs.failover_may_begin().unwrap_err();
        assert!(refused.contains("ended"), "{refused}");
        cs.last_handover_end_ms
            .store(now_ms() - HANDOVER_FAILOVER_GRACE_MS, Ordering::SeqCst);
        cs.failover_may_begin().unwrap();

        // A handover this node is no longer fit for holds nothing.
        let begun = cs.failovers_begun();
        assert_eq!(
            cs.begin_handover(begun, || Err("not the master".into())),
            Err("not the master".to_string())
        );
        cs.failover_may_begin().unwrap();
    }

    // ── A write routed before a slot left (#331, #337) ───────────────────

    /// Each way this node stops serving a slot moves its routes: a write
    /// routed before, and not yet in the write gate, is routed again there.
    /// DELSLOTS, DELSLOTSRANGE, FLUSHSLOTS, a reset, and this node no longer
    /// serving (marked FAIL here). They did not, and such a write ran.
    /// (The count is the process's: another test can only move it further.)
    #[test]
    fn every_way_a_slot_leaves_this_node_moves_its_routes() {
        let cs = ClusterState::new_enabled(None, 15000);
        cs.claim_slot_range(0, 16383).unwrap();
        cs.refresh_state();
        assert!(cs.is_serving());
        let moved = |what: &str, change: &dyn Fn()| {
            let routed = crate::pause::routes_now();
            change();
            assert!(
                crate::pause::routes_moved_since(routed),
                "{what} did not move the routes"
            );
        };
        moved("DELSLOTS", &|| {
            cs.release_slots(&[5]).unwrap();
        });
        moved("DELSLOTSRANGE", &|| {
            cs.release_slot_range(10, 20).unwrap();
        });
        moved("a node no longer serving", &|| {
            cs.hold_writes_until_confirmed(HoldReason::MarkedFailed, now_ms() + 60_000);
            assert!(!cs.is_serving());
        });
        moved("FLUSHSLOTS", &|| cs.flush_slots());
        cs.claim_slot_range(0, 100).unwrap();
        moved("CLUSTER RESET", &|| cs.soft_reset());
    }

    /// Slots taken over at the end of a migration are not this node's, nor
    /// claimed by it, until the cluster config says they are. When the
    /// config could not be saved, they used to be served and claimed in the
    /// meantime, and then given back (#337).
    #[test]
    fn slots_taken_over_are_not_taken_until_they_are_saved() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("not-yet").join("nodes.conf");
        let cs = ClusterState::new_enabled(Some(path.clone()), 15000);
        let me = cs.myself();
        let other = add_master(&cs, 0..=99);
        assert!(cs.take_over_slots(0..=99).is_err());
        assert_eq!(cs.slot_owner(0), Some(other));
        assert_eq!(cs.slot_owner(99), Some(other));
        assert!(cs.my_slot_claims().is_empty());

        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        cs.take_over_slots(0..=99).unwrap();
        assert_eq!(cs.slot_owner(0), Some(me));
        assert_eq!(cs.my_slot_claims().len(), 1);
        let saved = std::fs::read_to_string(&path).unwrap();
        let mine = saved
            .lines()
            .find(|l| l.contains("myself"))
            .expect("this node's line");
        assert!(mine.ends_with(" 0-99"), "{mine}");
    }

    /// A manual failover that ends while an atomic slot migration's handoff
    /// holds writes leaves them held; the handoff lets them go when it ends.
    /// And the other way round. The failover used to let them go in the
    /// middle of the handoff (#337).
    #[test]
    fn writes_are_let_go_when_the_last_hold_on_them_ends() {
        let _pause = crate::pause::TEST_LOCK.lock();
        crate::pause::clear_pause();
        let cs = ClusterState::new_enabled(None, 15000);
        let ask = now_ms() + 60_000;

        cs.hold_writes_for_handoff();
        cs.hold_writes_until_confirmed(HoldReason::ManualFailover, ask);
        cs.release_held_writes("the test");
        assert!(
            crate::pause::handoff(),
            "a failover let a handoff's writes go"
        );
        cs.end_handoff_hold();
        assert!(!crate::pause::handoff());

        cs.hold_writes_until_confirmed(HoldReason::ManualFailover, ask);
        cs.hold_writes_for_handoff();
        cs.end_handoff_hold();
        assert!(
            crate::pause::handoff(),
            "a handoff let a failover's writes go"
        );
        cs.release_held_writes("the test");
        assert!(!crate::pause::handoff());

        // Two handoffs at once: the second to end lets them go.
        cs.hold_writes_for_handoff();
        cs.hold_writes_for_handoff();
        cs.end_handoff_hold();
        assert!(crate::pause::handoff());
        cs.end_handoff_hold();
        assert!(!crate::pause::handoff());
    }

    /// A master told it has been marked FAIL holds writes by refusing them
    /// (`MarkedFailed`), not by pausing them. Told so during an atomic slot
    /// migration's handoff, it kept the handoff's pause on when the handoff
    /// ended, and letting the hold go did not end it: writes waited for the
    /// pause to run out, a century later (#349). Now the handoff ends its
    /// pause, and the hold keeps refusing writes until it is let go. A
    /// manual failover's hold still keeps the pause, whatever joins it.
    #[test]
    fn a_handoff_ends_its_pause_beside_a_hold_for_a_fail_verdict() {
        let _pause = crate::pause::TEST_LOCK.lock();
        crate::pause::clear_pause();
        let cs = ClusterState::new_enabled(None, 15000);
        let ask = now_ms() + 60_000;

        cs.hold_writes_for_handoff();
        cs.hold_writes_until_confirmed(HoldReason::MarkedFailed, ask);
        cs.end_handoff_hold();
        assert!(
            !crate::pause::handoff(),
            "a hold for a FAIL verdict kept a handoff's pause on"
        );
        assert!(cs.writes_held());
        cs.release_held_writes("the test");
        assert!(!crate::pause::handoff() && !cs.writes_held());

        // The verdict first, then the handoff.
        cs.hold_writes_until_confirmed(HoldReason::MarkedFailed, ask);
        cs.hold_writes_for_handoff();
        cs.release_held_writes("the test");
        assert!(
            crate::pause::handoff(),
            "a hold for a FAIL verdict let a handoff's writes go"
        );
        cs.end_handoff_hold();
        assert!(!crate::pause::handoff());

        // A verdict joining a manual failover's hold.
        cs.hold_writes_for_handoff();
        cs.hold_writes_until_confirmed(HoldReason::ManualFailover, ask);
        cs.hold_writes_until_confirmed(HoldReason::MarkedFailed, ask);
        cs.end_handoff_hold();
        assert!(
            crate::pause::handoff(),
            "a handoff let a failover's writes go"
        );
        cs.release_held_writes("the test");
        assert!(!crate::pause::handoff());
    }

    /// A promotion claims the failed master's slots before the replication
    /// layer makes the node a master. A claim of some of them by another
    /// master in between found it a replica there, and left their keys
    /// (`drop_lost_slots`): they are the ones dropped once it is a master.
    /// Not the keys of a slot removed with `DELSLOTS`, which leaves them.
    #[test]
    fn slots_claimed_away_while_promoting_are_the_ones_dropped() {
        let (cs, a, b) = three_master_cluster();
        let me = cs.myself();
        let claimed = cs.promote_self(a, 1_000);
        assert_eq!(claimed, (5461..=10922).collect::<Vec<u16>>());
        let claim = [crate::cluster::bus::SlotClaim {
            start: 6000,
            end: 6002,
            epoch: 1_000_000,
        }];
        cs.apply_peer_slot_claims(b, &claim);
        cs.release_slot(6010).unwrap();
        assert_eq!(cs.slot_owner(6001), Some(b));
        assert_eq!(cs.slot_owner(6003), Some(me));
        assert_eq!(cs.slot_owner(6010), None);
        assert_eq!(
            cs.slots_lost_while_promoting(&claimed),
            vec![6000, 6001, 6002]
        );
    }

    /// The end of an atomic slot migration hands over only the slots that
    /// are still this node's. One the destination's claim took first is
    /// left to the claim, and is not among those whose keys the source
    /// deletes (#348); nor is one this node never had.
    #[test]
    fn only_the_slots_still_here_are_handed_over() {
        let (cs, a, b) = three_master_cluster();
        let claim = [crate::cluster::bus::SlotClaim {
            start: 100,
            end: 100,
            epoch: 1_000_000,
        }];
        cs.apply_peer_slot_claims(a, &claim);
        assert_eq!(cs.slot_owner(100), Some(a));
        let handed = cs.hand_over_slots([100, 101, 6000].into_iter(), b);
        assert_eq!(handed, vec![101]);
        assert_eq!(cs.slot_owner(100), Some(a));
        assert_eq!(cs.slot_owner(101), Some(b));
        assert_eq!(cs.slot_owner(6000), Some(a));
    }

    // ── DELSLOTS ─────────────────────────────────────────────────────────

    /// A slot just given to a node by `SETSLOT NODE` keeps its owner for a
    /// moment whatever a peer claims: the old owner's last heartbeat, sent
    /// before its own `SETSLOT`, can claim it at a higher epoch than the new
    /// owner's. That claim used to take the slot from the new owner, and no
    /// node served it. Afterwards, claims are weighed by epoch as before.
    #[test]
    fn a_slot_set_by_hand_is_not_taken_by_an_older_claim() {
        let (cs, a, _b) = three_master_cluster();
        let me = cs.myself();
        // `a` had slot 5461 and is a reshard's source; this node, the target,
        // has just been given it.
        cs.finalize_slot(5461, me).unwrap();
        let stale = [crate::cluster::bus::SlotClaim {
            start: 5461,
            end: 5461,
            epoch: 1_000_000,
        }];
        cs.apply_peer_slot_claims(a, &stale);
        assert_eq!(cs.slot_owner(5461), Some(me));

        let past = std::time::Instant::now()
            .checked_sub(BY_HAND_GRACE * 2)
            .unwrap();
        for at in cs.by_hand.lock().values_mut() {
            *at = past;
        }
        cs.apply_peer_slot_claims(a, &stale);
        assert_eq!(cs.slot_owner(5461), Some(a));
    }

    /// DELSLOTS clears a slot from this node's view whoever owns it, and
    /// with it the slot's IMPORTING state, as in Redis (#295). Only a slot that
    /// is unassigned here already is refused, and then nothing changes.
    #[test]
    fn delslots_clears_a_slot_whoever_owns_it() {
        let (cs, a, b) = three_master_cluster();
        // Another node's slot, open here as an interrupted reshard leaves it.
        cs.set_importing(5461, a).unwrap();
        cs.release_slot(5461).unwrap();
        assert_eq!(cs.slot_owner(5461), None);
        assert_eq!(cs.migration_state(5461), None);
        assert_eq!(
            cs.release_slot(5461).unwrap_err(),
            "Slot 5461 is already unassigned"
        );

        // All or nothing.
        assert_eq!(
            cs.release_slot_range(5460, 5462).unwrap_err(),
            "Slot 5461 is already unassigned"
        );
        assert_eq!(
            cs.release_slots(&[10923, 5461]).unwrap_err(),
            "Slot 5461 is already unassigned"
        );
        assert_eq!(cs.slot_owner(5460), Some(cs.myself()));
        assert_eq!(cs.slot_owner(5462), Some(a));
        assert_eq!(cs.slot_owner(10923), Some(b));
        assert_eq!(cs.release_slot_range(5462, 5470), Ok(9));
        assert_eq!(cs.slot_owner(5470), None);

        // The owner's own claim does not take them back at once: the
        // ADDSLOTS that follows in `redis-cli --cluster fix`'s transaction
        // must find them free.
        let claim = [crate::cluster::bus::SlotClaim {
            start: 5461,
            end: 5470,
            epoch: 1,
        }];
        cs.apply_peer_slot_claims(a, &claim);
        assert_eq!(cs.slot_owner(5461), None);
        assert!(cs.claim_slot(5461).is_ok());
        assert_eq!(cs.slot_owner(5461), Some(cs.myself()));
        // After the grace, the owner's claim wins the rest back.
        let past = std::time::Instant::now()
            .checked_sub(BY_HAND_GRACE * 2)
            .unwrap();
        for at in cs.by_hand.lock().values_mut() {
            *at = past;
        }
        cs.apply_peer_slot_claims(a, &claim);
        assert!((5462..=5470).all(|s| cs.slot_owner(s) == Some(a)));
    }

    // ── Quorum ───────────────────────────────────────────────────────────

    /// The bar for declaring a node failed, and for winning an election, is a
    /// majority of the whole electorate — and it does not drop as nodes go
    /// missing. It used to be counted over the masters still standing.
    #[test]
    fn quorums_are_a_majority_of_all_slot_owning_masters() {
        let (cs, a, b) = three_master_cluster();
        assert_eq!(cs.voting_master_count(), 3);
        assert_eq!(cs.fail_quorum(a), 2);
        assert_eq!(cs.election_quorum(a), 2);

        // One master is already FAIL; the bar for the next one is unchanged.
        cs.nodes.write().get_mut(&a).unwrap().mark_failed(now_ms());
        assert_eq!(cs.fail_quorum(b), 2);
        assert_eq!(cs.election_quorum(b), 2);
    }

    /// Two masters partitioned from each other: each side has one vote, the
    /// quorum is two, and neither can declare the other failed. (Each used to
    /// need only its own vote, and both promoted replicas.)
    #[test]
    fn a_two_master_cluster_cannot_fail_a_node_from_one_side() {
        let cs = ClusterState::new_enabled(None, 100);
        cs.claim_slot_range(0, 8191).unwrap();
        let other = add_master(&cs, 8192..=16383);
        cs.nodes.write().get_mut(&other).unwrap().pong_received_ms = 1; // long silent

        assert_eq!(cs.fail_quorum(other), 2);
        let failed = cs.run_failure_detection_tick();
        assert!(failed.is_empty());
        let nodes = cs.nodes.read();
        let n = nodes.get(&other).unwrap();
        assert!(n.flags.contains(NodeFlags::PFAIL), "suspected, yes");
        assert!(
            !n.flags.contains(NodeFlags::FAIL),
            "but not declared failed"
        );
    }

    /// With a second master agreeing, the verdict is reached and time-stamped.
    #[test]
    fn a_node_is_failed_once_a_majority_of_masters_agree() {
        let (cs, a, b) = three_master_cluster();
        cs.nodes.write().get_mut(&a).unwrap().pong_received_ms = 1;
        assert!(
            cs.run_failure_detection_tick().is_empty(),
            "one vote of two"
        );

        cs.record_failure_report(a, b);
        assert_eq!(cs.run_failure_detection_tick(), vec![a]);
        let nodes = cs.nodes.read();
        let n = nodes.get(&a).unwrap();
        assert!(n.flags.contains(NodeFlags::FAIL));
        assert!(!n.flags.contains(NodeFlags::PFAIL));
        assert!(n.fail_since_ms > 0);
    }

    /// A node we can still reach is not failed on hearsay, however many others
    /// report it.
    #[test]
    fn reports_alone_do_not_fail_a_node_we_can_reach() {
        let (cs, a, b) = three_master_cluster();
        cs.record_failure_report(a, b);
        cs.record_failure_report(a, cs.myself());
        assert!(cs.run_failure_detection_tick().is_empty());
        assert!(
            !cs.nodes
                .read()
                .get(&a)
                .unwrap()
                .flags
                .contains(NodeFlags::FAIL)
        );
    }

    /// A replica's own suspicion is not a vote; only masters decide.
    #[test]
    fn a_replica_does_not_count_its_own_suspicion() {
        let (cs, a, b) = three_master_cluster();
        // Turn ourselves into a replica of `b`.
        cs.release_slot_range(0, 5460).unwrap();
        {
            let mut nodes = cs.nodes.write();
            let me = nodes.get_mut(&cs.myself()).unwrap();
            me.flags.remove(NodeFlags::MASTER);
            me.flags |= NodeFlags::REPLICA;
            me.master_id = Some(b);
        }
        cs.nodes.write().get_mut(&a).unwrap().pong_received_ms = 1;
        // Two voting masters remain → quorum 2. One report from `b` plus our
        // own (non-voting) suspicion is not enough.
        cs.record_failure_report(a, b);
        assert_eq!(cs.fail_quorum(a), 2);
        assert!(cs.run_failure_detection_tick().is_empty());
    }

    // ── Cluster state ────────────────────────────────────────────────────

    /// The minority side of a partition must stop serving.
    #[test]
    fn losing_sight_of_a_majority_of_masters_takes_the_cluster_down() {
        let (cs, a, b) = three_master_cluster();
        assert!(cs.is_serving());

        set_flag(&cs, a, NodeFlags::PFAIL);
        cs.refresh_state();
        assert!(
            cs.is_serving(),
            "one of three unreachable: still the majority side"
        );

        set_flag(&cs, b, NodeFlags::PFAIL);
        cs.refresh_state();
        assert!(
            !cs.is_serving(),
            "two of three unreachable: the minority side"
        );
        assert_eq!(cs.cluster_state_str(), "fail");
    }

    // ── A master back without its dataset ────────────────────────────────

    /// A dataset known to be whole (AOF, clean stop) is served as before; a
    /// master back without one serves nothing until its replicas have said
    /// they hold nothing either.
    #[test]
    fn a_master_back_without_its_dataset_is_fenced_until_its_replicas_answer() {
        let (cs, _a, _b) = three_master_cluster();
        let me = cs.myself();
        add_replica(&cs, me);
        add_replica(&cs, me);
        cs.check_dataset_at_start(true, 0);
        assert!(cs.is_serving() && !cs.lost_dataset());

        cs.check_dataset_at_start(false, 0);
        assert!(cs.lost_dataset());
        assert!(!cs.is_serving());
        assert_eq!(cs.cluster_state_str(), "fail");
        // Periodic look: both replicas are live, so it holds.
        cs.check_lost_dataset_fence();
        assert!(!cs.is_serving());

        assert!(!cs.replica_has_nothing("10.0.0.2", 7001));
        assert!(
            !cs.replica_has_nothing("10.0.0.2", 7001),
            "the same one twice"
        );
        assert!(!cs.is_serving());
        assert!(cs.replica_has_nothing("10.0.0.3", 7001));
        assert!(cs.is_serving() && !cs.lost_dataset());
        assert_eq!(cs.cluster_state_str(), "ok");
    }

    /// With no replica that could take over, there is nothing to wait for.
    #[test]
    fn a_master_back_without_its_dataset_or_reachable_replicas_serves() {
        let (cs, _a, _b) = three_master_cluster();
        cs.check_dataset_at_start(false, 0);
        assert!(cs.is_serving(), "no replica at all: not fenced");

        let me = cs.myself();
        let r = add_replica(&cs, me);
        cs.check_dataset_at_start(false, 0);
        assert!(!cs.is_serving());
        set_flag(&cs, r, NodeFlags::PFAIL);
        cs.check_lost_dataset_fence();
        assert!(!cs.is_serving(), "only suspected: it may be taking over");
        set_flag(&cs, r, NodeFlags::FAIL);
        cs.check_lost_dataset_fence();
        assert!(cs.is_serving() && !cs.lost_dataset());
    }

    /// Once a replica has taken the slots over, the fence falls with them.
    #[test]
    fn the_fence_falls_with_the_slots() {
        let (cs, _a, _b) = three_master_cluster();
        let me = cs.myself();
        add_replica(&cs, me);
        cs.check_dataset_at_start(false, 0);
        assert!(!cs.is_serving());
        cs.release_slot_range(0, 5460).unwrap();
        cs.refresh_state();
        assert!(!cs.lost_dataset());
    }

    // ── FAIL is not forever ──────────────────────────────────────────────

    #[test]
    fn a_failed_replica_is_cleared_as_soon_as_it_is_heard_from() {
        let (cs, a, _b) = three_master_cluster();
        let replica = add_replica(&cs, a);
        cs.nodes
            .write()
            .get_mut(&replica)
            .unwrap()
            .mark_failed(now_ms());
        cs.maybe_clear_fail_on_contact(replica);
        assert!(
            !cs.nodes
                .read()
                .get(&replica)
                .unwrap()
                .flags
                .contains(NodeFlags::FAIL)
        );
    }

    /// A failed master that still owns slots gets its replicas' window first.
    #[test]
    fn a_failed_slot_owner_is_only_cleared_after_the_failover_window() {
        let (cs, a, _b) = three_master_cluster();
        let timeout = cs.node_timeout_ms;

        cs.nodes.write().get_mut(&a).unwrap().mark_failed(now_ms());
        cs.maybe_clear_fail_on_contact(a);
        assert!(
            cs.nodes
                .read()
                .get(&a)
                .unwrap()
                .flags
                .contains(NodeFlags::FAIL),
            "replicas must get their chance to take over first"
        );

        cs.nodes.write().get_mut(&a).unwrap().fail_since_ms = now_ms() - timeout * 2 - 1;
        cs.maybe_clear_fail_on_contact(a);
        assert!(
            !cs.nodes
                .read()
                .get(&a)
                .unwrap()
                .flags
                .contains(NodeFlags::FAIL)
        );
    }

    /// Once a replica has taken its slots, the old master owns nothing and is
    /// cleared at once — so it can rejoin as a replica.
    #[test]
    fn a_failed_master_that_lost_its_slots_is_cleared_at_once() {
        let (cs, a, b) = three_master_cluster();
        cs.nodes.write().get_mut(&a).unwrap().mark_failed(now_ms());
        for s in cs.slots.write().iter_mut() {
            if *s == Some(a) {
                *s = Some(b);
            }
        }
        cs.maybe_clear_fail_on_contact(a);
        assert!(
            !cs.nodes
                .read()
                .get(&a)
                .unwrap()
                .flags
                .contains(NodeFlags::FAIL)
        );
    }

    // ── Voting ───────────────────────────────────────────────────────────

    #[test]
    fn a_vote_requires_a_failed_master_unless_the_failover_is_manual() {
        let (cs, a, _b) = three_master_cluster();
        let replica = add_replica(&cs, a);

        assert!(!cs.cast_vote(replica, a, 5, false), "master is healthy");
        assert!(cs.cast_vote(replica, a, 5, true), "manual failover");
    }

    #[test]
    fn one_vote_per_epoch_and_never_for_a_stale_epoch() {
        let (cs, a, b) = three_master_cluster();
        let (ra, rb) = (add_replica(&cs, a), add_replica(&cs, b));
        cs.nodes.write().get_mut(&a).unwrap().mark_failed(now_ms());
        cs.nodes.write().get_mut(&b).unwrap().mark_failed(now_ms());

        assert!(cs.cast_vote(ra, a, 5, false));
        assert!(!cs.cast_vote(rb, b, 5, false), "already voted in epoch 5");
        assert!(
            !cs.cast_vote(rb, b, 4, false),
            "epoch 4 is behind the cluster"
        );
        assert!(cs.cast_vote(rb, b, 6, false));
        assert_eq!(cs.current_epoch(), 6);
    }

    /// Two replicas of one master must not both be elected moments apart.
    #[test]
    fn a_second_replica_of_the_same_master_is_not_voted_for_straight_away() {
        let (cs, a, _b) = three_master_cluster();
        let (r1, r2) = (add_replica(&cs, a), add_replica(&cs, a));
        cs.nodes.write().get_mut(&a).unwrap().mark_failed(now_ms());

        assert!(cs.cast_vote(r1, a, 5, false));
        assert!(!cs.cast_vote(r2, a, 6, false));
    }

    #[test]
    fn only_slot_owning_masters_vote() {
        let cs = ClusterState::new_enabled(None, 15000); // owns no slots
        let a = add_master(&cs, 0..=16383);
        let replica = add_replica(&cs, a);
        cs.nodes.write().get_mut(&a).unwrap().mark_failed(now_ms());
        assert!(!cs.cast_vote(replica, a, 5, false));
    }

    #[test]
    fn a_candidate_must_be_a_replica_of_the_master_it_wants_to_replace() {
        let (cs, a, b) = three_master_cluster();
        let replica_of_b = add_replica(&cs, b);
        cs.nodes.write().get_mut(&a).unwrap().mark_failed(now_ms());
        assert!(!cs.cast_vote(replica_of_b, a, 5, false));
    }

    // ── FORGET, RESET ────────────────────────────────────────────────────

    #[test]
    fn forgetting_a_node_releases_its_slots_and_bans_it() {
        let (cs, a, _b) = three_master_cluster();
        assert!(cs.forget_node(a));
        assert!(!cs.nodes.read().contains_key(&a));
        assert_eq!(cs.slot_owner(6000), None);
        assert!(cs.is_forgotten(&a));
        assert!(!cs.forget_node(a), "already gone");
    }

    /// A hard reset is a new identity. It used to keep the old id.
    #[test]
    fn hard_reset_assigns_a_new_node_id() {
        let (cs, _a, _b) = three_master_cluster();
        let before = cs.myself();
        let after = cs.hard_reset();
        assert_ne!(before, after);
        assert_eq!(cs.myself(), after);
        let nodes = cs.nodes.read();
        assert_eq!(nodes.len(), 1, "peers are forgotten");
        assert!(nodes.contains_key(&after));
        assert!(nodes.get(&after).unwrap().flags.contains(NodeFlags::MYSELF));
        drop(nodes);
        assert_eq!(cs.my_slot_count(), 0);
        assert_eq!(cs.current_epoch(), 0);
    }

    // ── Self address ─────────────────────────────────────────────────────

    /// Bound to a wildcard with no announce address, a node has nothing true
    /// to say about where it lives — so it says nothing, and peers use the
    /// address they see. It used to announce 127.0.0.1.
    #[test]
    fn wildcard_bind_advertises_no_address_until_one_is_learned() {
        let cs = ClusterState::new_enabled(None, 15000);
        cs.configure_self_address("0.0.0.0", 7000, "", 0, 0);
        let recorded = cs.nodes.read().get(&cs.myself()).unwrap().ip.clone();
        assert_eq!(cs.advertised_ip(&recorded), "");

        // Reaching a peer on the same machine teaches nothing...
        cs.learn_self_ip("127.0.0.1".parse().unwrap());
        assert_eq!(cs.advertised_ip(&recorded), "");

        // ...reaching one elsewhere does.
        cs.learn_self_ip("10.1.2.3".parse().unwrap());
        let recorded = cs.nodes.read().get(&cs.myself()).unwrap().ip.clone();
        assert_eq!(recorded, "10.1.2.3");
        assert_eq!(cs.advertised_ip(&recorded), "10.1.2.3");
    }

    #[test]
    fn announce_settings_override_the_bind_address_and_ports() {
        let cs = ClusterState::new_enabled(None, 15000);
        cs.configure_self_address("0.0.0.0", 7000, "203.0.113.9", 30001, 30002);
        let nodes = cs.nodes.read();
        let me = nodes.get(&cs.myself()).unwrap();
        assert_eq!(me.ip, "203.0.113.9");
        assert_eq!(me.client_port, 30001);
        assert_eq!(me.cluster_port, 30002);
        drop(nodes);
        assert_eq!(cs.advertised_ip("203.0.113.9"), "203.0.113.9");
        // An explicit address is never replaced by a learned one.
        cs.learn_self_ip("10.1.2.3".parse().unwrap());
        assert_eq!(cs.nodes.read().get(&cs.myself()).unwrap().ip, "203.0.113.9");
    }
}
