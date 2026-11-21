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
pub(crate) mod followed_imports;
pub(crate) mod migrate_conns;
pub mod migration;
pub mod node;
pub(crate) mod orphans;
pub mod persist;
pub mod routing;
pub mod search;
pub mod slot;

pub use node::{
    ClientIps, ClusterNode, NODE_ID_ZERO, NodeFlags, NodeId, generate_node_id, is_ipv6_peer,
    node_id_str, parse_node_id,
};
pub use routing::{RouteDecision, clusterdown_error, crossslot_error, moved_error, route_command};
pub use slot::{CLUSTER_SLOTS, crc16, key_hash_slot};

/// How long a slot assigned or removed here by `SETSLOT NODE` or `DELSLOTS`
/// keeps that assignment whatever a peer claims, unless another command
/// changes it (see `ClusterState::note_by_hand`).
pub const BY_HAND_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

use crate::cpus::BuilderExt as _;
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

/// What one master last said, in a PONG, of the slots it holds this node
/// as the owner of. See [`ClusterState::promotion_to_complete`].
#[derive(Debug, Clone)]
struct HeldAsOwner {
    /// The slots, each range at this node's config epoch in that master's
    /// view, and how many masters own slots there.
    report: crate::cluster::bus::OwnerReport,
    /// When it said so.
    at: std::time::Instant,
}

/// This master's reports to one node that says it is a replica while this
/// master holds it as the owner of slots. See
/// [`ClusterState::owner_report_for`].
#[derive(Debug, Clone, Copy)]
struct Reporting {
    /// When this master first reported to it, since it last said it is a
    /// master or last owned no slots here.
    began: std::time::Instant,
    /// When this master last sent it a report.
    last: Option<std::time::Instant>,
    /// It reported for [`ClusterState::report_limit`] without the node
    /// saying it is a master: no more reports, until it does, or owns no
    /// slots here.
    gave_up: bool,
}

/// The wait before a replica's first sync after it starts: see
/// [`ClusterState::first_sync_may_begin`].
struct FirstSync {
    /// When the cluster state was made (at startup).
    started: std::time::Instant,
    /// The wait is over, for good.
    open: AtomicBool,
    /// The nodes this one has had a PONG from since it started.
    heard: parking_lot::Mutex<ahash::AHashSet<NodeId>>,
}

/// Whether this node, a replica, is to complete a promotion it does not
/// remember: see [`ClusterState::promotion_to_complete`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnsavedPromotion {
    /// No quorum of masters holds this node as the owner of its master's
    /// slots.
    None,
    /// A quorum does, but the promotion is not completed, for this reason.
    NotCompleted(String),
    /// Take the slots of `master` over at config epoch `epoch`: `slots`,
    /// the ranges `master` owns in this node's view, every one of which the
    /// quorum holds this node as the owner of.
    Complete {
        master: NodeId,
        epoch: u64,
        slots: Vec<(u16, u16)>,
    },
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

/// [`ClusterState::migrations`]: the state of each slot being migrated or
/// imported, behind a lock, and how many there are, kept beside it in an
/// atomic for [`ClusterState::slots_open`]. Every change is made through
/// [`Self::write`], whose guard stores the count as it lets go of the
/// lock: no writer can leave it out of date.
#[derive(Default)]
pub struct Migrations {
    map: RwLock<AHashMap<u16, MigrationState>>,
    open: std::sync::atomic::AtomicUsize,
}

impl Migrations {
    pub fn read(&self) -> parking_lot::RwLockReadGuard<'_, AHashMap<u16, MigrationState>> {
        self.map.read()
    }

    pub fn write(&self) -> MigrationsWrite<'_> {
        MigrationsWrite {
            map: self.map.write(),
            open: &self.open,
        }
    }

    /// Whether any slot is being migrated or imported here. One relaxed
    /// load of the count [`MigrationsWrite`] stores (with `Release`, under
    /// the lock): a slot opened by a `SETSLOT` this thread has not
    /// synchronised with is not seen at once, as if the command asking ran
    /// just before.
    #[inline]
    pub fn any_open(&self) -> bool {
        self.open.load(Ordering::Relaxed) != 0
    }
}

/// [`Migrations::write`]'s guard.
pub struct MigrationsWrite<'a> {
    map: parking_lot::RwLockWriteGuard<'a, AHashMap<u16, MigrationState>>,
    open: &'a std::sync::atomic::AtomicUsize,
}

impl std::ops::Deref for MigrationsWrite<'_> {
    type Target = AHashMap<u16, MigrationState>;
    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl std::ops::DerefMut for MigrationsWrite<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.map
    }
}

impl Drop for MigrationsWrite<'_> {
    fn drop(&mut self) {
        // Before the lock is let go of (the fields are dropped after this).
        self.open.store(self.map.len(), Ordering::Release);
    }
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
    /// For a fence put up as a promotion was completed
    /// ([`ClusterState::fence_completion`]): the master it took
    /// over from, whose replicas count as this node's while the fence holds
    /// (they follow this node once they hear of it).
    old_master: parking_lot::Mutex<Option<NodeId>>,
}

/// This node's announce settings, and what an unset one falls back to.
#[derive(Default)]
struct Announce {
    /// `cluster-announce-ip`; empty: unset.
    ip: String,
    /// `cluster-announce-port`; 0: unset.
    port: u16,
    /// `cluster-announce-bus-port`; 0: unset.
    bus_port: u16,
    /// `bind`, fixed at startup.
    bind: String,
    /// The port clients reach this node on, fixed at startup.
    client_port: u16,
    /// The bus port this node listens on, fixed at startup.
    listen_bus_port: u16,
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
    /// What each master last told this node, in a PONG, of the slots it
    /// holds this node as the owner of while this node says it is a
    /// replica. See [`Self::promotion_to_complete`]. Taken alone.
    held_as_owner: parking_lot::Mutex<AHashMap<NodeId, HeldAsOwner>>,
    /// The members this master has told, in a PONG, that it holds them as
    /// the owner of slots while they say they are replicas, and since when
    /// ([`Self::owner_report_for`]). Taken under `last_vote_for`, or alone:
    /// a report and a vote for a replacement of the same node exclude each
    /// other (see [`Self::cast_vote`]).
    reported_as_owner: parking_lot::Mutex<AHashMap<NodeId, Reporting>>,
    /// How many keys this node loaded at startup, if its dataset was not
    /// known to be all it had (see [`Self::check_dataset_at_start`]); `None`
    /// if it was. A promotion completed after such a start is fenced as a
    /// master that came back without its dataset is
    /// ([`Self::fence_completion`]).
    untrusted_start: parking_lot::Mutex<Option<usize>>,
    /// See [`Self::first_sync_may_begin`]. `heard` is taken alone.
    first_sync: FirstSync,
    /// Per-slot migration state. Empty entries mean "not migrating".
    /// Mutating via CLUSTER SETSLOT MIGRATING/IMPORTING/STABLE/NODE.
    pub migrations: Migrations,
    /// Atomic slot migrations (`CLUSTER MIGRATION`), running and recent.
    pub slot_migrations: migration::Registry,
    /// The deletions of the keys of slots that left this node, and the
    /// slots that come to it meanwhile. Whatever gives this node a slot,
    /// or starts importing one into it, says so here
    /// ([`migration::SlotDeletions::came_here`]) while it holds `slots` or
    /// `migrations`: a deletion begun before then leaves the slot's keys,
    /// which may be acknowledged writes from then on (#353).
    pub slot_deletions: std::sync::Arc<migration::SlotDeletions>,
    /// The keys this master holds of slots it does not serve, as last
    /// counted, and the thread that counts them (#517). See `orphans`.
    pub(crate) orphans: std::sync::Arc<orphans::Orphans>,
    /// The atomic slot imports of the master this node follows, or of its
    /// old master once promoted, until what they brought is deleted (#741).
    /// See `followed_imports`.
    pub(crate) followed_imports: followed_imports::FollowedImports,
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
    pub last_vote_for: parking_lot::Mutex<AHashMap<NodeId, std::time::Instant>>,
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
    /// when the last of them ended; how many failovers of this node have
    /// begun ([`Self::failover_may_begin`],
    /// [`Self::hold_writes_until_confirmed`]), and when the last one did.
    /// All change only under `handoff_holds`: a failover and a handover
    /// exclude each other (#516). Monotonic times: a clock stepped back
    /// must not stretch either window.
    handovers: AtomicU64,
    last_handover_end: parking_lot::Mutex<Option<std::time::Instant>>,
    failovers_begun: AtomicU64,
    last_failover_began: parking_lot::Mutex<Option<std::time::Instant>>,
    /// This master serves none of its slots, having been told it was marked
    /// FAIL, until it has made sure it still owns them
    /// ([`Self::note_marked_failed`]).
    marked_failed: AtomicBool,
    /// `cluster-require-full-coverage`.
    require_full_coverage: AtomicBool,
    /// `cluster-announce-ip`, `-port` and `-bus-port`, as configured at
    /// startup or since by `CONFIG SET`. See [`Self::set_announce`].
    announce: parking_lot::Mutex<Announce>,
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
    /// Where the saves of nodes.conf stand: a save is synced without
    /// `save_lock`, and is not put in place over a newer one.
    pub(crate) saves: std::sync::Arc<persist::Saves>,
    /// `ip:bus_port` of each `CLUSTER MEET` waiting for its answer, or for
    /// this node's save of the peer (see `bus::dialer::send_meet`): its
    /// handshake entry is kept meanwhile, though that can take longer than
    /// the failure detector otherwise gives one.
    pub(crate) meets: parking_lot::Mutex<ahash::AHashSet<String>>,
    /// `ip:bus_port` of each `CLUSTER MEET` given up after the peer was
    /// reached, until when: the peer may have made this node a member, so a
    /// heartbeat from there takes the MEET up again (`bus::listener`).
    pub(crate) meets_given_up: parking_lot::Mutex<AHashMap<String, u64>>,
    /// Nodes being admitted after a `CLUSTER MEET`, saved with the members
    /// until they are members: see [`Self::admit_met_node`]. Each entry is
    /// that of one admission, by its number: two of the same node (a MEET
    /// and its retry) each remove only their own.
    pub(crate) joining: parking_lot::Mutex<Vec<(u64, ClusterNode)>>,
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
    /// The connections a search is sent on to the other masters, kept open
    /// between searches. See `search::Pool`.
    pub(crate) search_pool: search::Pool,
    /// The peers that said, in their own last PING or PONG, that they decide
    /// a vote on the candidate's claims whatever the kind of failover and
    /// answer a refusal with an `UPDATE` (`NodeFlags::VOTE_CLAIMS`). Only
    /// they are sent the claims of a manual failover: a node of an earlier
    /// version refuses a request of six fields (#742). Kept in memory only:
    /// learnt again from the first heartbeat after a restart.
    vote_claims_peers: RwLock<ahash::AHashSet<NodeId>>,
    /// The master this node last took slots over from, by an election, a
    /// `TAKEOVER` or a promotion completed after a restart: on a config
    /// epoch collision with it, this node is the one that moves (#743).
    took_over_from: parking_lot::Mutex<Option<NodeId>>,
    /// Replicas of this node that took its slots over at its own config
    /// epoch (a `TAKEOVER` whose epoch collided with one this node took
    /// unannounced): this node never moves on a collision with them, which
    /// would put it above them and give it their slots back (#743).
    /// Both this and `took_over_from` are saved in nodes.conf's `vars`
    /// line (`tookOverFrom`, `yieldedTo`; an earlier version skips them),
    /// so that a restart of either node in between keeps the rule, and are
    /// let go once the collision is over ([`Self::settle_takeover_links`]).
    yielded_to: parking_lot::Mutex<ahash::AHashSet<NodeId>>,
    /// What each master last claimed in its own PING or PONG, at the
    /// epochs it gave: all an `UPDATE` relays of it (#742). Its config
    /// epoch here can be newer (gossip raises it) than what this node
    /// knows of its slots.
    own_claims: RwLock<AHashMap<NodeId, Vec<crate::cluster::bus::SlotClaim>>>,
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
        let starts_as_replica = nodes
            .get(&myself)
            .is_some_and(|n| n.flags.contains(NodeFlags::REPLICA));
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
            held_as_owner: parking_lot::Mutex::new(AHashMap::new()),
            reported_as_owner: parking_lot::Mutex::new(AHashMap::new()),
            untrusted_start: parking_lot::Mutex::new(None),
            first_sync: FirstSync {
                started: std::time::Instant::now(),
                // Only a node that starts as a replica waits.
                open: AtomicBool::new(!starts_as_replica),
                heard: parking_lot::Mutex::new(ahash::AHashSet::new()),
            },
            migrations: Migrations::default(),
            slot_migrations: migration::Registry::default(),
            slot_deletions: Default::default(),
            orphans: Default::default(),
            followed_imports: Default::default(),
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
            last_handover_end: parking_lot::Mutex::new(None),
            failovers_begun: AtomicU64::new(0),
            last_failover_began: parking_lot::Mutex::new(None),
            marked_failed: AtomicBool::new(false),
            require_full_coverage: AtomicBool::new(true),
            announce: parking_lot::Mutex::new(Announce::default()),
            replica_validity_factor: AtomicU64::new(10),
            self_ip_is_guess: AtomicBool::new(false),
            bus_secret: std::sync::OnceLock::new(),
            background_save: std::sync::OnceLock::new(),
            preferred_endpoint: std::sync::atomic::AtomicU8::new(Endpoint::Ip as u8),
            save_lock: parking_lot::Mutex::new(()),
            last_saved_fingerprint: AtomicU64::new(0),
            saves: std::sync::Arc::new(persist::Saves::default()),
            meets: parking_lot::Mutex::new(ahash::AHashSet::new()),
            meets_given_up: parking_lot::Mutex::new(AHashMap::new()),
            joining: parking_lot::Mutex::new(Vec::new()),
            stats_messages_sent: AtomicU64::new(0),
            stats_messages_received: AtomicU64::new(0),
            stats_bus_connects: AtomicU64::new(0),
            links: parking_lot::Mutex::new(AHashMap::new()),
            ping_links: parking_lot::Mutex::new(AHashMap::new()),
            search_pool: search::Pool::default(),
            vote_claims_peers: RwLock::new(ahash::AHashSet::new()),
            took_over_from: parking_lot::Mutex::new(None),
            yielded_to: parking_lot::Mutex::new(ahash::AHashSet::new()),
            own_claims: RwLock::new(AHashMap::new()),
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
                client_ips: Default::default(),
                shard_id: generate_node_id(),
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
        persist::remove_leftover_temps(&path);

        match persist::load(&path)? {
            Some(loaded) => {
                let state = Self::build(
                    true,
                    loaded.myself,
                    loaded.slots,
                    loaded.nodes,
                    loaded.current_epoch,
                    loaded.last_vote_epoch,
                    Some(path),
                    node_timeout_ms,
                );
                *state.took_over_from.lock() = loaded.took_over_from;
                state.yielded_to.lock().extend(loaded.yielded_to);
                Ok(state)
            }
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
            // Kept for `CONFIG GET`, `SET` and `REWRITE`, as Redis keeps
            // them outside cluster mode too.
            let state = Self::new_disabled();
            state.configure_announce(config);
            state
                .require_full_coverage
                .store(config.cluster_require_full_coverage, Ordering::Relaxed);
            return Ok(state);
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
        // A record of slot handovers it may have been in the middle of that
        // cannot be read whole: not started, rather than serve slots that
        // may have moved (#743).
        migration::check_handovers_file(&state)?;
        // With `tls-cluster` the port to reach this node on is its TLS port,
        // for clients and for the other nodes alike (see `node_tls`); the bus
        // stays where it is. A node with no plaintext port (port 0) has
        // `tls-cluster`: see `Config::validate`.
        state.configure_announce(config);
        state.set_require_full_coverage(config.cluster_require_full_coverage);
        state.set_replica_validity_factor(config.cluster_replica_validity_factor);
        state.set_my_hostname(&config.cluster_announce_hostname);
        state.set_my_announced_client_port(config.cluster_announce_client_port);
        state.set_my_client_ips(ClientIps {
            v4: config.cluster_announce_client_ipv4.clone(),
            v6: config.cluster_announce_client_ipv6.clone(),
        });
        state.search_pool.size_for(config.resolve_threads());
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
        let _ = FOR_SHUTDOWN.set(std::sync::Arc::downgrade(self));
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
            slot_imports: followed_imports::hook(self),
        });
        // A replica that started on a saved resync point holds the stream of
        // the master nodes.conf names.
        repl_state.note_stream_origin_if_unknown(
            self.my_master_id().map(|id| node_id_str(&id).to_string()),
        );
        // The keys of an import this node was receiving when it went down,
        // which never completed (#108).
        migration::drop_unfinished_imports(self);
        // A handover away from this node it was in the middle of: writes are
        // held until it is settled (#743). Before any client is served.
        migration::settle_unfinished_handovers(self, &repl_state);
        // Those of its master, or of its old master, that it knew of (#741).
        followed_imports::start(self, &repl_state);
        migration::save_imports(self);
        // Keys of slots this node does not serve are reported, from a
        // moment after startup on (#517).
        orphans::start_watching(self);
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
            .spawn_released(move || {
                let mut last: Option<std::time::Instant> = None;
                let mut failing = false;
                loop {
                    // A save that failed is made again every second, until
                    // one works (a promotion, say, that must reach the
                    // disk), as well as when one is asked for.
                    let asked = if failing {
                        !matches!(
                            rx.recv_timeout(std::time::Duration::from_secs(1)),
                            Err(std::sync::mpsc::RecvTimeoutError::Disconnected)
                        )
                    } else {
                        rx.recv().is_ok()
                    };
                    if !asked {
                        return;
                    }
                    // One save in `BACKGROUND_SAVE_GAP` at most, of all the
                    // changes asked for meanwhile: see there.
                    if let Some(since) = last.map(|t| t.elapsed())
                        && since < BACKGROUND_SAVE_GAP
                    {
                        std::thread::sleep(BACKGROUND_SAVE_GAP - since);
                    }
                    let _ = rx.try_recv();
                    let Some(state) = state.upgrade() else {
                        return;
                    };
                    // Not while a save something waits on is made (a vote,
                    // a node met): the disk would commit the two in turn,
                    // and the vote, which counts, would wait on this one.
                    let waited = std::time::Instant::now();
                    while state.saves.urgent.load(Ordering::SeqCst) > 0
                        && waited.elapsed() < std::time::Duration::from_secs(5)
                    {
                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }
                    let saved = persist::save_from_state(&state);
                    last = Some(std::time::Instant::now());
                    failing = saved.is_err();
                    if let Err(e) = saved {
                        log_verbose!("Cluster: background save of nodes.conf failed: {e}");
                    }
                }
            });
        if spawned.is_ok() {
            let _ = self.background_save.set(tx);
        }
    }

    /// Make `node`, met by a `CLUSTER MEET` (this node sent it, or was sent
    /// it), a member with `add`, once nodes.conf has it, synced: a node met
    /// is shown as a member (in `CLUSTER NODES`, without `handshake`) only
    /// once it is saved. Both ends of a MEET save the other before it is
    /// shown, and the receiving end before it answers. A cluster stopped
    /// whole (killed, even) from then on keeps the new node: a node that has
    /// it in its nodes.conf tells the others of it again, which matters
    /// because a node that is not a member cannot join by sending
    /// heartbeats (#808).
    ///
    /// The nodes waiting are saved together, in the next save, not one save
    /// each: a node met by many at once (`redis-cli --cluster create` has
    /// every node meet the first) waits for one or two writes. If the save
    /// fails (a full or read-only disk), `node` is not admitted and `false`
    /// is returned: the MEET is not answered, and the end that sent it tries
    /// again. The file in place may have the node all the same (a save that
    /// failed at the directory's sync was renamed into place): it is saved
    /// again, without it.
    ///
    /// Blocks on the disk: not to be called on the bus's thread.
    pub fn admit_met_node(&self, node: ClusterNode, add: impl FnOnce()) -> bool {
        let admission = self.begin_admission(node);
        self.finish_admission(admission, add)
    }

    /// The first half of [`Self::admit_met_node`]: `node` is in every save
    /// numbered from the one returned on, until [`Self::finish_admission`].
    fn begin_admission(&self, node: ClusterNode) -> Admission {
        static NUMBER: AtomicU64 = AtomicU64::new(0);
        let number = NUMBER.fetch_add(1, Ordering::Relaxed);
        let id = node.id;
        self.joining.lock().push((number, node));
        Admission {
            number,
            id,
            // Every save numbered from here on has it.
            from: self.saves.written.load(Ordering::SeqCst) + 1,
        }
    }

    /// The second half of [`Self::admit_met_node`].
    fn finish_admission(&self, admission: Admission, add: impl FnOnce()) -> bool {
        let deadline = std::time::Instant::now() + ADMIT_SAVE_WAIT;
        let saved = persist::wait_for_save(self, admission.from, deadline);
        if saved.is_ok() {
            add();
        }
        // Only this admission's entry: another of the same node, under way,
        // keeps its own until its save.
        self.joining
            .lock()
            .retain(|(number, _)| *number != admission.number);
        if let Err(e) = &saved {
            log_verbose!(
                "Cluster: {} not made a member: it could not be saved ({e})",
                node_id_str(&admission.id)
            );
            self.save_soon();
        }
        saved.is_ok()
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

    /// Take this node's announce settings from its configuration, and set
    /// its own address from them.
    pub fn configure_announce(&self, config: &crate::config::Config) {
        // With `tls-cluster` the port to reach this node on is its TLS port,
        // for clients and for the other nodes alike (see `node_tls`); the bus
        // stays where it is. A node with no plaintext port (port 0) has
        // `tls-cluster`: see `Config::validate`.
        let client_port = if config.tls_cluster || config.port == 0 {
            config.tls_port
        } else {
            config.port
        };
        let mut announce = self.announce.lock();
        *announce = Announce {
            ip: config.cluster_announce_ip.clone(),
            port: config.cluster_announce_port,
            bus_port: config.cluster_announce_bus_port,
            bind: config.bind.clone(),
            client_port,
            listen_bus_port: config.cluster_bus_port(),
        };
        self.apply_announce(&announce);
    }

    fn apply_announce(&self, announce: &Announce) {
        let bus_port = if announce.bus_port != 0 {
            announce.bus_port
        } else {
            announce.listen_bus_port
        };
        self.configure_self_address(
            &announce.bind,
            announce.client_port,
            &announce.ip,
            announce.port,
            bus_port,
        );
    }

    /// `cluster-announce-ip`, `cluster-announce-port` and
    /// `cluster-announce-bus-port` as they are set now (empty or 0: unset).
    pub fn announce(&self) -> (String, u16, u16) {
        let announce = self.announce.lock();
        (announce.ip.clone(), announce.port, announce.bus_port)
    }

    /// Change `cluster-announce-ip`, `-port` or `-bus-port` while the node
    /// runs (`CONFIG SET`), as Redis allows (#391). This node's own address
    /// follows, and nodes.conf is saved when it changed. Peers take the new
    /// address from this node's next heartbeat, as they would after a
    /// restart with the new settings.
    pub fn set_announce(
        &self,
        ip: Option<&str>,
        port: Option<u16>,
        bus_port: Option<u16>,
    ) -> std::io::Result<()> {
        let address = || {
            self.nodes
                .read()
                .get(&self.myself())
                .map(|me| (me.ip.clone(), me.client_port, me.cluster_port))
        };
        let before = address();
        {
            let mut announce = self.announce.lock();
            if let Some(ip) = ip {
                announce.ip = ip.to_string();
            }
            if let Some(port) = port {
                announce.port = port;
            }
            if let Some(bus_port) = bus_port {
                announce.bus_port = bus_port;
            }
            if self.enabled {
                self.apply_announce(&announce);
            }
        }
        if self.enabled && address() != before {
            let (ip, port, bus_port) = self.announce();
            log_info!(
                "Cluster: announce settings changed (ip '{ip}', port {port}, bus port {bus_port})"
            );
            self.save_to_disk()?;
        }
        Ok(())
    }

    /// `cluster-require-full-coverage`.
    pub fn require_full_coverage(&self) -> bool {
        self.require_full_coverage.load(Ordering::Relaxed)
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

    /// Set this node's `cluster-announce-client-ipv4` and `-ipv6` (empty:
    /// none). Peers learn them from the next heartbeat (#847).
    pub fn set_my_client_ips(&self, ips: ClientIps) {
        if let Some(me) = self.nodes.write().get_mut(&self.myself()) {
            me.client_ips = ips;
        }
    }

    /// Change one of this node's client addresses, `cluster-announce-client-ipv6`
    /// if `v6`, else `-ipv4` (empty: none), under one write of the node table,
    /// so that two set at once both stay (#847).
    pub fn set_my_client_ip(&self, v6: bool, ip: &str) {
        if let Some(me) = self.nodes.write().get_mut(&self.myself()) {
            if v6 {
                me.client_ips.v6 = ip.to_string();
            } else {
                me.client_ips.v4 = ip.to_string();
            }
        }
    }

    /// Record the client addresses a peer says it announces, none included.
    /// Only a node's own heartbeat says them: never gossip, as in Valkey, or
    /// a node that had not heard of a change yet would bring the old ones
    /// back (#847).
    pub fn set_peer_client_ips(&self, id: NodeId, ips: &ClientIps) {
        if id == self.myself() {
            return;
        }
        if let Some(node) = self.nodes.write().get_mut(&id)
            && node.client_ips != *ips
        {
            node.client_ips = ips.clone();
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

    /// This node's shard id (`CLUSTER MYSHARDID`).
    pub fn my_shard_id(&self) -> NodeId {
        self.nodes
            .read()
            .get(&self.myself())
            .map(|me| me.shard_id)
            .unwrap_or_else(|| self.myself())
    }

    /// What a heartbeat from `sender` says of its shard, `said` (`None` from
    /// a node of an earlier version), taken in as Redis's `updateShardId`
    /// does (#563): a master's own word is its shard's, so its replicas take
    /// it too, this node among them if it is one; a replica is in its
    /// master's shard, whatever it says. Nothing else changes a shard id:
    /// a replica that is promoted keeps the one it had, its master's, so the
    /// shard keeps its id across a failover. The caller saves.
    pub fn take_shard_id(&self, sender: NodeId, said: Option<NodeId>) {
        let mut nodes = self.nodes.write();
        let Some(master_id) = nodes.get(&sender).map(|n| n.master_id) else {
            return;
        };
        match master_id {
            Some(master) => {
                if let Some(shard) = nodes.get(&master).map(|m| m.shard_id)
                    && let Some(node) = nodes.get_mut(&sender)
                {
                    node.shard_id = shard;
                }
            }
            None => {
                let Some(shard) = said else {
                    return;
                };
                for node in nodes.values_mut() {
                    if node.id == sender || node.master_id == Some(sender) {
                        node.shard_id = shard;
                    }
                }
            }
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
    /// `cluster-preferred-endpoint-type` has it: its address (for a client
    /// connected over IPv6 or IPv4, the client address it announces for
    /// that family, if any: `ClusterNode::client_ip`); its hostname, `?` if
    /// it announces none; or empty, for "the address you used".
    pub fn endpoint_of(&self, node: &ClusterNode, ipv6_client: bool) -> String {
        match self.preferred_endpoint() {
            Endpoint::Ip => node.client_ip(ipv6_client).to_string(),
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
        // A node listening on `::` that reached a peer over IPv4 has the
        // IPv4-mapped form (`::ffff:a.b.c.d`) as its end: its IPv4 address
        // is the one to take, and to hand to clients (#847).
        let local_ip = local_ip.to_canonical();
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
        // Said in the log as Redis says it: tests and operators wait for
        // the line (#430).
        if self.state_ok.swap(ok, Ordering::Relaxed) != ok {
            log_info!("Cluster state changed: {}", if ok { "ok" } else { "fail" });
        }
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
    ///
    /// What it found is kept: a node that starts as a replica and then
    /// completes a promotion ([`Self::promotion_to_complete`]) is fenced
    /// the same way at that moment ([`Self::fence_completion`]).
    pub fn check_dataset_at_start(&self, trusted: bool, keys: usize) {
        if !self.enabled {
            return;
        }
        *self.untrusted_start.lock() = (!trusted).then_some(keys);
        if trusted {
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

    /// The fence of a promotion completed by [`Self::complete_promotion_self`],
    /// decided and put up under its locks (`nodes` is its node table),
    /// before any of the slots is this node's here: if its dataset at
    /// startup was not known to be all it had (no AOF, and no snapshot from
    /// a clean shutdown: [`Self::check_dataset_at_start`]), and no full
    /// sync has replaced it since ([`Self::trust_dataset`]), it holds what
    /// it last saved, while a replica of the shard may hold what it
    /// acknowledged since. As for a master that came back without its
    /// dataset ([`LostDataset`]): with a live replica, of this node or of
    /// `old_master` in its view (they follow it once they hear of it), it
    /// serves none of the slots until a replica that holds them takes over
    /// (`+LOSTDATA`) or the replicas answer that they hold nothing either;
    /// with none, it serves what it has. Returns what to log, once the locks
    /// are let go. The locks of `lost` and `untrusted_start` are taken
    /// alone, never with another held but these.
    fn fence_completion(
        &self,
        nodes: &AHashMap<NodeId, ClusterNode>,
        me: NodeId,
        old_master: NodeId,
    ) -> Option<String> {
        let keys = (*self.untrusted_start.lock())?;
        let replicas = nodes
            .values()
            .filter(|n| {
                n.id != me
                    && n.master_id.is_some_and(|m| m == me || m == old_master)
                    && !n.flags.intersects(NodeFlags::FAIL | NodeFlags::HANDSHAKE)
            })
            .count();
        if replicas == 0 {
            return Some(format!(
                "Cluster: This node completed its promotion with the dataset it started with \
                 ({keys} keys loaded; no AOF, and no snapshot from a clean shutdown), and has no \
                 replica that could hold more: serving what it has"
            ));
        }
        *self.lost.old_master.lock() = Some(old_master);
        self.lost.offers.store(0, Ordering::Relaxed);
        self.lost.empty_replicas.lock().clear();
        self.lost.since_ms.store(now_ms(), Ordering::Relaxed);
        self.lost.fenced.store(true, Ordering::Relaxed);
        Some(format!(
            "Cluster: This node completed its promotion with the dataset it started with ({keys} \
             keys loaded; no AOF, and no snapshot from a clean shutdown). It serves none of its \
             slots until a replica that holds them takes over, or its replicas answer that they \
             hold nothing either"
        ))
    }

    /// A full sync has replaced the dataset this node started with: it is a
    /// master's copy, and a promotion completed from here on is not fenced
    /// for what it loaded at startup.
    pub fn trust_dataset(&self) {
        *self.untrusted_start.lock() = None;
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
            *self.lost.old_master.lock() = None;
            log_warn!("Cluster: Lost-dataset fence lifted: {why}");
        }
    }

    /// This master's replicas, in its view, that have not failed. One only
    /// suspected (PFAIL) counts: a replica winning an election can be slow
    /// to answer pings while it saves its new configuration, and the fence
    /// must not fall then — this master would take writes that the
    /// promotion then throws away.
    ///
    /// For a fence put up as a promotion was completed, the replicas of the
    /// master it took over from count too (`LostDataset::old_master`).
    fn live_replicas_of_me(&self) -> usize {
        let me = self.myself();
        let old_master = *self.lost.old_master.lock();
        self.nodes
            .read()
            .values()
            .filter(|n| {
                n.id != me
                    && n.master_id
                        .is_some_and(|m| m == me || Some(m) == old_master)
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

    /// CLUSTER BUMPEPOCH, as Redis's `clusterBumpConfigEpochWithoutConsensus`:
    /// unless this node's config epoch is already the highest there is (and
    /// not 0), it takes a new epoch, above every one this node has seen, as
    /// its config epoch. Returns whether it did, and its config epoch.
    pub fn bump_config_epoch_without_consensus(&self) -> (bool, u64) {
        let me = self.myself();
        let mut nodes = self.nodes.write();
        let highest = nodes
            .values()
            .map(|n| n.config_epoch)
            .max()
            .unwrap_or(0)
            .max(self.current_epoch.load(Ordering::SeqCst));
        let Some(node) = nodes.get_mut(&me) else {
            return (false, 0);
        };
        if node.config_epoch != 0 && node.config_epoch == highest {
            return (false, node.config_epoch);
        }
        self.current_epoch.fetch_max(highest, Ordering::SeqCst);
        let epoch = self.current_epoch.fetch_add(1, Ordering::SeqCst) + 1;
        node.config_epoch = epoch;
        (true, epoch)
    }

    /// Note an epoch a peer is at.
    pub fn observe_epoch(&self, epoch: u64) {
        self.current_epoch.fetch_max(epoch, Ordering::SeqCst);
    }

    /// Two masters with one config epoch: the one with the lower id moves to
    /// a new epoch, as in Redis (`clusterHandleConfigEpochCollision`), so
    /// that a claim of one over the other can be decided. Returns whether
    /// this node moved.
    ///
    /// But between a master and the replica that took its slots over (a
    /// `CLUSTER FAILOVER TAKEOVER` picks its epoch alone, and one the master
    /// took unannounced, at the end of an import or by `BUMPEPOCH`, can be
    /// the same), it is always the replica that moves, whatever the ids: the
    /// master moving above it would have its next claim take the slots back
    /// from the replica that serves them (#743). The
    /// replica knows it by [`Self::took_over_from`], the master by
    /// [`Self::yielded_to`] (it gave the replica the slots at the tie:
    /// [`Self::apply_peer_slot_claims_from`]). Both in memory: after a
    /// restart of either, the ids decide again, the slots being where the
    /// tie put them.
    pub fn resolve_epoch_collision(&self, sender: NodeId, sender_epoch: u64) -> bool {
        let me = self.myself();
        if self.yielded_to.lock().contains(&sender) {
            return false;
        }
        let took_over_from_sender = *self.took_over_from.lock() == Some(sender);
        if sender < me && !took_over_from_sender {
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
        self.claim_slots(&[slot]).map(|_| ())
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
        self.claim_slots(&(start..=end).collect::<Vec<u16>>())
    }

    /// Why `list` cannot all be claimed here, if it cannot: the first slot,
    /// in order, that is assigned already or named twice. As Redis's
    /// `checkSlotAssignmentsOrReply`.
    fn unclaimable(table: &[Option<NodeId>], list: &[u16]) -> Option<String> {
        let mut seen = vec![false; CLUSTER_SLOTS];
        for &slot in list {
            if slot as usize >= CLUSTER_SLOTS {
                return Some(format!("Invalid slot {slot}"));
            }
            if table[slot as usize].is_some() {
                return Some(format!("Slot {slot} is already busy"));
            }
            if std::mem::replace(&mut seen[slot as usize], true) {
                return Some(format!("Slot {slot} specified multiple times"));
            }
        }
        None
    }

    /// Whether every slot of `list` could be claimed here now: see
    /// [`Self::claim_slots`].
    pub fn check_slots_claimable(&self, list: &[u16]) -> Result<(), String> {
        if !self.enabled {
            return Err("This instance has cluster support disabled".into());
        }
        match Self::unclaimable(&self.slots.read()[..], list) {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Claim every slot of `list`, or none: CLUSTER ADDSLOTS and
    /// ADDSLOTSRANGE. Errors, changing nothing, if one of them is assigned
    /// already or named twice, as in Redis. They used to claim slot by
    /// slot, or range by range, and the ones before the first busy slot
    /// stayed claimed (#553).
    ///
    /// A slot claimed is no longer being imported: its `SETSLOT IMPORTING`
    /// state goes, as in Redis's `clusterUpdateSlots`. It stayed, and the
    /// slot was still shown as importing in `CLUSTER NODES`. (An atomic
    /// import, `CLUSTER MIGRATION`, is tracked apart, in `slot_migrations`,
    /// and is not touched.)
    pub fn claim_slots(&self, list: &[u16]) -> Result<usize, String> {
        if !self.enabled {
            return Err("This instance has cluster support disabled".into());
        }
        let me = self.myself();
        // `slots` before `migrations`, as everywhere (see `ClusterState`).
        let mut slots = self.slots.write();
        if let Some(e) = Self::unclaimable(&slots[..], list) {
            return Err(e);
        }
        let mut migrations = self.migrations.write();
        for &slot in list {
            slots[slot as usize] = Some(me);
            if matches!(
                migrations.get(&slot),
                Some(MigrationState::Importing { .. })
            ) {
                migrations.remove(&slot);
            }
        }
        drop(migrations);
        self.slot_deletions.came_here(list.iter().copied());
        Ok(list.len())
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

    /// Give up every slot this node owns: CLUSTER FLUSHSLOTS, as Redis's
    /// `clusterDelNodeSlots(myself)`. The other masters' slots stay as they
    /// are; this used to unassign every slot in this node's view (#552).
    /// Returns how many it gave up.
    pub fn flush_slots(&self) -> usize {
        if !self.enabled {
            return 0;
        }
        let me = self.myself();
        let mut slots = self.slots.write();
        let mut flushed = 0;
        for s in slots.iter_mut() {
            if *s == Some(me) {
                *s = None;
                flushed += 1;
            }
        }
        drop(slots);
        // As for DELSLOTS (`release_slots`).
        crate::pause::routes_moved();
        flushed
    }

    /// Set our own node's config_epoch. Used by SET-CONFIG-EPOCH: only on a
    /// node that knows no other node and has no config epoch yet, as in
    /// Redis. It used to be allowed on a node in a cluster (#563).
    pub fn set_my_config_epoch(&self, new_epoch: u64) -> Result<(), String> {
        if !self.enabled {
            return Err("This instance has cluster support disabled".into());
        }
        let mut nodes = self.nodes.write();
        if nodes.len() > 1 {
            return Err("The user can assign a config epoch only when the \
                        node does not know any other node."
                .into());
        }
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
        self.slot_claims_of(self.myself())
    }

    /// The slots `owner` owns in this node's view, as contiguous ranges,
    /// paired with its config epoch in this node's view: this node's own
    /// claims ([`Self::my_slot_claims`]), or what a replica standing for
    /// election claims for its master (#524, [`Self::cast_vote`]).
    pub fn slot_claims_of(&self, owner: NodeId) -> Vec<crate::cluster::bus::SlotClaim> {
        if !self.enabled {
            return vec![];
        }
        let my_epoch = {
            let nodes = self.nodes.read();
            nodes.get(&owner).map(|n| n.config_epoch).unwrap_or(0)
        };
        let slots = self.slots.read();
        let mut out = Vec::new();
        let mut start: Option<usize> = None;
        for i in 0..CLUSTER_SLOTS {
            let owned = matches!(slots[i], Some(o) if o == owner);
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
        self.apply_peer_slot_claims_from(owner_id, claims, None);
    }

    /// [`Self::apply_peer_slot_claims`], from a heartbeat of `owner_id`
    /// that made it a master here, where it was a replica of
    /// `former_master`: a claim of a slot `former_master` owns at the same
    /// config epoch as the claim's is accepted too. That is a replica that
    /// took its master's slots over (`TAKEOVER`) at an epoch the master
    /// had taken too, unannounced (#743): two events at one epoch, which
    /// the epochs cannot order. The takeover is the later of the two (the
    /// replica took over because the master was gone), and every node that
    /// knew it as the master's replica decides the same way at its first
    /// heartbeat as a master, the master itself included (it then follows
    /// it, or keeps the slots it has left). A node that did not know it as
    /// one keeps what it has until the replica moves to a newer epoch,
    /// which it does on the collision ([`Self::resolve_epoch_collision`]).
    pub fn apply_peer_slot_claims_from(
        &self,
        owner_id: NodeId,
        claims: &[crate::cluster::bus::SlotClaim],
        former_master: Option<NodeId>,
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
        // Some of them to a replica of this node at this node's own epoch.
        let mut yielded = false;
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
                        Some(cur) => {
                            let cur_epoch = owner_epochs.get(&cur).copied().unwrap_or(0);
                            claim.epoch > cur_epoch
                                || (claim.epoch == cur_epoch && former_master == Some(cur))
                        }
                    };
                    if accept {
                        if slots[i] == Some(me)
                            && claim.epoch == owner_epochs.get(&me).copied().unwrap_or(0)
                        {
                            yielded = true;
                        }
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
        if yielded {
            self.yielded_to.lock().insert(owner_id);
            log_warn!(
                "Cluster: {} took this node's slots over at this node's own config epoch \
                 (a takeover whose epoch is one this node had taken unannounced): it is \
                 the later of the two, and keeps them",
                node_id_str(&owner_id)
            );
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
            let shard = nodes.get(&new_master).map(|m| m.shard_id);
            if let Some(my_node) = nodes.get_mut(&me) {
                my_node.flags.remove(NodeFlags::MASTER);
                my_node.flags |= NodeFlags::REPLICA;
                my_node.master_id = Some(new_master);
                // Into the new master's shard. It is this node's own when the
                // shard failed over away from it.
                if let Some(shard) = shard {
                    my_node.shard_id = shard;
                }
            }
            addr
        };
        self.forget_takeover_links();
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
                if ip.is_empty() || port == 0 || !self.first_sync_may_begin() {
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
            // Not while its MEET is under way: the MEET waits for the peer
            // to save this node, which can take longer (see `send_meet`).
            let meets = self.meets.lock();
            nodes.retain(|_, n| {
                !(n.flags.contains(NodeFlags::HANDSHAKE)
                    && now.saturating_sub(n.ping_sent_ms) > handshake_timeout
                    && !meets.contains(&format!("{}:{}", n.ip, n.cluster_port)))
            });
            drop(meets);
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
            // Nothing of it to relay, nor a collision with it to settle.
            self.own_claims.write().remove(&id);
            if *self.took_over_from.lock() == Some(id) {
                *self.took_over_from.lock() = None;
            }
            self.yielded_to.lock().remove(&id);
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
        // Into its master's shard, as Redis's `clusterSetMaster` puts it.
        {
            let mut nodes = self.nodes.write();
            if let Some(shard) = nodes.get(&master_id).map(|m| m.shard_id)
                && let Some(me) = nodes.get_mut(&self.myself())
            {
                me.shard_id = shard;
            }
        }
        self.forget_takeover_links();

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
    ///   last two node timeouts;
    /// * that master does not say it is a replica while this node has told
    ///   it, within the last three node timeouts, that it holds it as the
    ///   owner of its slots ([`Self::owner_report_for`]); this node stops
    ///   telling it so after [`Self::report_limit`], so this refusal lasts
    ///   six node timeouts at most;
    /// * none of the slots the candidate `claims` for its master, if the
    ///   request says (an automatic failover's; and a manual one's, sent to
    ///   a node that says it decides them, `NodeFlags::VOTE_CLAIMS`, #742),
    ///   has an owner here other than that master at a newer config epoch
    ///   than the claim's, as in Redis (#524). A replica that has not heard
    ///   that its master gave a slot away, in a slot handover or a
    ///   failover, would claim it at the election's epoch once promoted,
    ///   newer than the owner's, and the owner's acknowledged writes to it
    ///   would be lost. The refusal is answered with an `UPDATE` that names
    ///   those owners ([`Self::newer_owners_of_claimed`]), and the
    ///   candidate asks again with its claims put right.
    pub fn cast_vote(
        &self,
        candidate: NodeId,
        master_id: NodeId,
        request_epoch: u64,
        force: bool,
        claims: Option<&[crate::cluster::bus::SlotClaim]>,
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
        if let Some(claims) = claims
            && let Some(why) = self.newer_owner_of_claimed(master_id, claims)
        {
            log_info!(
                "Cluster: Not voting for {} to take over from {}: {why}",
                node_id_str(&candidate),
                node_id_str(&master_id)
            );
            return false;
        }
        #[cfg(test)]
        before_vote_lock();
        // Taken before `handoff_holds` and the tasks of `slot_migrations`
        // (below), and never while either is held.
        let mut voted_for = self.last_vote_for.lock();
        if let Some(at) = voted_for.get(&master_id)
            && at.elapsed() < std::time::Duration::from_millis(self.node_timeout_ms * 2)
        {
            return false;
        }
        // A master that says it is a replica, and that this node has told,
        // in a PONG, that it holds it as the owner of its slots: it may be
        // completing its promotion on what this node said
        // (`promotion_to_complete`). Under `last_vote_for`, as the report
        // is made (`owner_report_for`): one or the other, never both. Its
        // role is read here, under the lock: a heartbeat that makes it a
        // replica here comes before the report it is answered with, which
        // is made under the lock too.
        let master_says_replica = self
            .nodes
            .read()
            .get(&master_id)
            .is_some_and(|n| n.flags.contains(NodeFlags::REPLICA));
        if master_says_replica
            && self
                .reported_as_owner
                .lock()
                .get(&master_id)
                .and_then(|r| r.last)
                .is_some_and(|at| at.elapsed() < self.report_withholds_votes())
        {
            log_info!(
                "Cluster: Not voting for {} to take over from {}: that node says it is a \
                 replica, and this node told it that it holds it as the owner of its slots",
                node_id_str(&candidate),
                node_id_str(&master_id)
            );
            return false;
        }
        if request_epoch <= self.last_vote_epoch.load(Ordering::SeqCst) {
            return false; // already voted in this or a higher epoch
        }
        // Only now that the vote would be given: a failover and the
        // handover of an atomic slot migration exclude each other (#516).
        // For a replica of this node, see `failover_may_begin`. For one of a
        // master this node imports from, the import's attempt is given up:
        // its `TAKEOVER` is refused.
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
                voted_for.insert(master_id, std::time::Instant::now());
                return true;
            }
        }
    }

    /// Of the slots a candidate `claims` for `master_id`, the first that
    /// another node owns here at a newer config epoch than the claim's, and
    /// why it is refused: see [`Self::cast_vote`].
    fn newer_owner_of_claimed(
        &self,
        master_id: NodeId,
        claims: &[crate::cluster::bus::SlotClaim],
    ) -> Option<String> {
        let epochs: AHashMap<NodeId, u64> = {
            let nodes = self.nodes.read();
            nodes.iter().map(|(id, n)| (*id, n.config_epoch)).collect()
        };
        let slots = self.slots.read();
        for claim in claims {
            let end = claim.end.min((CLUSTER_SLOTS - 1) as u16);
            for slot in claim.start..=end {
                if let Some(owner) = slots[slot as usize]
                    && owner != master_id
                    && let Some(&epoch) = epochs.get(&owner)
                    && epoch > claim.epoch
                {
                    return Some(format!(
                        "it claims slot {slot} at epoch {}, and {} owns it at epoch {epoch}",
                        claim.epoch,
                        node_id_str(&owner)
                    ));
                }
            }
        }
        None
    }

    /// The newer owners of the slots a candidate `claims` for `master_id`,
    /// as this node knows them, for the `UPDATE` that answers a vote it
    /// refused for that reason ([`Self::cast_vote`]): each with its
    /// address and exactly what that owner last claimed in its own PING or
    /// PONG to this node, at the epochs it claimed them at
    /// ([`Self::note_own_claims`]); this node's own claims when it is the
    /// owner. Never this node's slot table paired with the owner's config
    /// epoch here: gossip raises that epoch without the slot view, and a
    /// slot the owner has since given to another, at an epoch below the
    /// raised one, would be relayed at the raised one and win against the
    /// first-hand claim of its new owner on the candidate (#742). An owner
    /// not heard from first-hand is not named. A candidate learns a slot's
    /// new owner from that owner's heartbeats otherwise, and one that
    /// cannot hear them (the owner down, or cut off from it) would never
    /// learn it.
    pub fn newer_owners_of_claimed(
        &self,
        master_id: NodeId,
        claims: &[crate::cluster::bus::SlotClaim],
    ) -> Vec<crate::cluster::bus::OwnerUpdate> {
        let epochs: AHashMap<NodeId, u64> = {
            let nodes = self.nodes.read();
            nodes.iter().map(|(id, n)| (*id, n.config_epoch)).collect()
        };
        let mut owners: Vec<NodeId> = Vec::new();
        {
            let slots = self.slots.read();
            for claim in claims {
                let end = claim.end.min((CLUSTER_SLOTS - 1) as u16);
                for slot in claim.start..=end {
                    if let Some(owner) = slots[slot as usize]
                        && owner != master_id
                        && epochs.get(&owner).is_some_and(|&e| e > claim.epoch)
                        && !owners.contains(&owner)
                    {
                        owners.push(owner);
                    }
                }
            }
        }
        let me = self.myself();
        owners
            .into_iter()
            .filter_map(|owner| {
                let (ip, client_port, cluster_port) = {
                    let nodes = self.nodes.read();
                    let n = nodes.get(&owner)?;
                    (n.ip.clone(), n.client_port, n.cluster_port)
                };
                let claims = if owner == me {
                    self.my_slot_claims()
                } else {
                    self.own_claims.read().get(&owner).cloned()?
                };
                // The epoch the owner itself gave, not the one gossip
                // raised here.
                let config_epoch = claims.iter().map(|c| c.epoch).max()?;
                (!claims.is_empty()).then_some(crate::cluster::bus::OwnerUpdate {
                    id: owner,
                    ip,
                    client_port,
                    cluster_port,
                    config_epoch,
                    claims,
                })
            })
            .collect()
    }

    /// Note what master `id` claimed in its own PING or PONG, as it gave
    /// it: what an `UPDATE` relays of it ([`Self::newer_owners_of_claimed`]).
    pub fn note_own_claims(&self, id: NodeId, claims: &[crate::cluster::bus::SlotClaim]) {
        let mut own = self.own_claims.write();
        if own.get(&id).is_some_and(|c| c.as_slice() == claims) {
            return;
        }
        own.insert(id, claims.to_vec());
    }

    /// The takeover links of [`Self::took_over_from`] and
    /// [`Self::yielded_to`], as nodes.conf's `vars` line carries them (with
    /// a leading space; empty when there are none). An earlier version
    /// skips them.
    pub(crate) fn takeover_links_vars(&self) -> String {
        let mut out = String::new();
        if let Some(id) = *self.took_over_from.lock() {
            out.push_str(" tookOverFrom ");
            out.push_str(node_id_str(&id));
        }
        let mut yielded: Vec<NodeId> = self.yielded_to.lock().iter().copied().collect();
        if !yielded.is_empty() {
            yielded.sort();
            out.push_str(" yieldedTo ");
            let ids: Vec<&str> = yielded.iter().map(node_id_str).collect();
            out.push_str(&ids.join(","));
        }
        out
    }

    /// After a heartbeat of `sender` (`sender_master`, at `sender_epoch`)
    /// has been taken in: the takeover links with it are let go once a
    /// collision with it is over: it is a master above this node's config
    /// epoch, or this node is no longer a master; for `took_over_from`, also
    /// once the old master says it is a replica (it follows someone). Never
    /// on a heartbeat below this node's epoch: one sent before the takeover
    /// and still on its way would drop the link just before the collision
    /// it is for. Kept until then, across restarts (nodes.conf), so that a
    /// node that took over, restarted before its old master came back,
    /// still moves on the collision; a node that yielded with the lower id
    /// and its replica both staying put left two masters at one epoch
    /// (#743). The node that took over always moves while its link holds,
    /// so the two cannot both stay put; and the one that yielded drops its
    /// own link only once the other is above it.
    pub fn settle_takeover_links(&self, sender: NodeId, sender_master: bool, sender_epoch: u64) {
        let me = self.myself();
        let (i_am_master, my_epoch) = self
            .nodes
            .read()
            .get(&me)
            .map(|n| (n.flags.contains(NodeFlags::MASTER), n.config_epoch))
            .unwrap_or((false, 0));
        let above = sender_master && sender_epoch > my_epoch;
        let mut changed = false;
        {
            let mut took = self.took_over_from.lock();
            if *took == Some(sender) && (above || !sender_master || !i_am_master) {
                *took = None;
                changed = true;
            }
        }
        if above || !i_am_master {
            changed |= self.yielded_to.lock().remove(&sender);
        }
        if changed {
            self.save_soon();
        }
    }

    /// This node no longer leads a shard: no takeover link of it applies.
    fn forget_takeover_links(&self) {
        *self.took_over_from.lock() = None;
        self.yielded_to.lock().clear();
    }

    /// Note what `id` said of itself in its own PING or PONG: whether it
    /// decides votes on a candidate's claims for a manual failover too
    /// (`NodeFlags::VOTE_CLAIMS`). See [`Self::vote_claims_peers`].
    pub fn note_vote_claims(&self, id: NodeId, yes: bool) {
        let known = self.vote_claims_peers.read().contains(&id);
        if known != yes {
            let mut peers = self.vote_claims_peers.write();
            if yes {
                peers.insert(id);
            } else {
                peers.remove(&id);
            }
        }
    }

    /// Whether `id` said, in its last own heartbeat, that it decides a
    /// manual failover's vote on the candidate's claims.
    pub fn decides_on_claims(&self, id: &NodeId) -> bool {
        self.vote_claims_peers.read().contains(id)
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
        // On a config epoch collision with it, this node moves (#743). It
        // yielded to no one as the new master it now is.
        *self.took_over_from.lock() = Some(failed_master_id);
        self.yielded_to.lock().clear();

        // Phase 3: persist, by the caller: see `save_promotion`. The election
        // is won and the cluster has agreed to it: the promotion holds,
        // whether or not it can be saved now.
        self.refresh_state();
        claimed
    }

    /// Save a promotion [`Self::promote_self`] just made, before it is
    /// announced: waits for the save at most [`PROMOTION_SAVE_WAIT`], on a
    /// thread of its own (a save on a slow disk takes seconds, and the bus
    /// must go on answering meanwhile). The save is made at once, not after
    /// the gap between the bus's own saves. `Err` with the reason if it
    /// failed or is not done in that time: the promotion is announced all
    /// the same, and holds in memory; a save that failed is made again on
    /// the save thread, every second until one works, and the failure is
    /// logged as an error until then.
    ///
    /// Blocks for up to [`PROMOTION_SAVE_WAIT`]: for a command, or through
    /// `spawn_blocking` on the bus.
    pub fn save_promotion(self: &std::sync::Arc<Self>) -> Result<(), String> {
        if self.config_path.is_none() {
            return Ok(());
        }
        // Every save numbered from here on has the promotion.
        let from = self.saves.written.load(Ordering::SeqCst) + 1;
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let state = std::sync::Arc::clone(self);
        crate::cpus::spawn(move || {
            let deadline = std::time::Instant::now() + ELECTION_LIFETIME;
            let saved = persist::wait_for_save(&state, from, deadline);
            if saved.is_err() {
                state.save_soon();
            }
            let _ = tx.send(saved);
        });
        let saved = match rx.recv_timeout(PROMOTION_SAVE_WAIT) {
            Ok(saved) => saved,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => Err(format!(
                "the save did not end in {} ms",
                PROMOTION_SAVE_WAIT.as_millis()
            )),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                Err("the save thread ended".into())
            }
        };
        if let Err(e) = &saved {
            log_warn!(
                "Cluster: the promotion is announced before it is on disk ({e}); it is saved \
                 in the background"
            );
        }
        saved
    }

    /// Note what master `from` says, in a PONG, of the slots it holds this
    /// node as the owner of (`None`: it holds it as the owner of nothing, is
    /// of an earlier version, or sent a field that is not a valid report).
    /// Only ever from the master's own PONG to this node, never relayed. See
    /// [`Self::promotion_to_complete`].
    pub fn note_held_as_owner(
        &self,
        from: NodeId,
        report: Option<crate::cluster::bus::OwnerReport>,
    ) {
        let mut held = self.held_as_owner.lock();
        match report {
            None => {
                held.remove(&from);
            }
            Some(report) => {
                held.insert(
                    from,
                    HeldAsOwner {
                        report,
                        at: std::time::Instant::now(),
                    },
                );
            }
        }
    }

    /// The report for a PONG to member `to`, which says it is a replica:
    /// the slots this node holds it as the owner of, at its config epoch
    /// here, and how many masters own slots here; `None` if it owns none
    /// here. Also `None` while this node's vote for a replacement of `to`
    /// may still be won and announced ([`Self::vote_withholds_reports`]):
    /// a replica elected to replace it, with this vote, may be saving its
    /// promotion and not yet have announced it, and `to` is not to
    /// complete a promotion of its own meanwhile: the winner's newer claim
    /// is on its way. A report sent is noted, and this node votes for no
    /// replacement of `to` for a while after it ([`Self::cast_vote`]).
    /// Both are decided under `last_vote_for`: one or the other.
    ///
    /// Reports to one node end [`Self::report_limit`] after the first, if
    /// it has not said it is a master by then (it cannot complete: too few
    /// masters report to it, or it refuses their epoch): from then on this
    /// master sends it none, until it says it is a master or owns no slots
    /// here, and, three node timeouts after the last report, votes for a
    /// replacement of it again. That survives the node's restarts: a node
    /// that restarts over and over as a replica holds up an election of
    /// its replacement for six node timeouts at most.
    pub fn owner_report_for(&self, to: NodeId) -> Option<crate::cluster::bus::OwnerReport> {
        let claims = self.slot_claims_of(to);
        if claims.is_empty() {
            self.reported_as_owner.lock().remove(&to);
            return None;
        }
        let masters = self.voting_master_count();
        let voted_for = self.last_vote_for.lock();
        if voted_for
            .get(&to)
            .is_some_and(|at| at.elapsed() < self.vote_withholds_reports())
        {
            return None;
        }
        let report = crate::cluster::bus::OwnerReport::valid(masters, claims)?;
        let now = std::time::Instant::now();
        let mut reporting = self.reported_as_owner.lock();
        let r = reporting.entry(to).or_insert(Reporting {
            began: now,
            last: None,
            gave_up: false,
        });
        if r.gave_up {
            return None;
        }
        if now.duration_since(r.began) >= self.report_limit() {
            r.gave_up = true;
            log_warn!(
                "Cluster: Node {} still says it is a replica {} ms after this master first \
                 reported it the owner of its slots here: no more reports to it until it says it \
                 is a master",
                node_id_str(&to),
                self.report_limit().as_millis()
            );
            return None;
        }
        r.last = Some(now);
        drop(reporting);
        drop(voted_for);
        Some(report)
    }

    /// How long this master reports to one node at most
    /// ([`Self::owner_report_for`]): three node timeouts. A node restarted
    /// before its promotion was saved completes it within a heartbeat or
    /// two of the first report.
    fn report_limit(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.node_timeout_ms.saturating_mul(3))
    }

    /// Member `id` says, in a heartbeat, that it is a master: reports to it
    /// start afresh, should it say it is a replica again (see
    /// [`Self::owner_report_for`]).
    pub fn note_says_master(&self, id: NodeId) {
        let mut reporting = self.reported_as_owner.lock();
        if !reporting.is_empty() {
            reporting.remove(&id);
        }
    }

    /// For how long after this node voted for a replacement of a master it
    /// sends that master no report ([`Self::owner_report_for`]): two node
    /// timeouts, and at least as long as an election lasts plus the wait
    /// for the winner's save before it announces (`ELECTION_LIFETIME`,
    /// `PROMOTION_SAVE_WAIT`), so that a vote this node gave is won and
    /// announced, or given up, within it.
    fn vote_withholds_reports(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.node_timeout_ms.saturating_mul(2))
            .max(ELECTION_LIFETIME + PROMOTION_SAVE_WAIT)
    }

    /// For how long after this node sent a master a report it votes for no
    /// replacement of that master while it says it is a replica
    /// ([`Self::cast_vote`]): three node timeouts, one more than the two
    /// for which the master counts the report
    /// ([`Self::promotion_to_complete`]), for the time the report takes to
    /// reach it.
    fn report_withholds_votes(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.node_timeout_ms.saturating_mul(3))
    }

    /// What each master said of this node within the last two node
    /// timeouts (older word is dropped).
    fn current_reports(&self) -> Vec<(NodeId, crate::cluster::bus::OwnerReport)> {
        let window = std::time::Duration::from_millis(self.node_timeout_ms.saturating_mul(2));
        let mut held = self.held_as_owner.lock();
        held.retain(|_, h| h.at.elapsed() <= window);
        held.iter().map(|(id, h)| (*id, h.report.clone())).collect()
    }

    /// Whether this node, a replica, is to complete a promotion that the
    /// rest of the cluster has and it does not: elected, its promotion
    /// announced, and the process stopped before a save of it was done (a
    /// full disk, a read-only directory, a disk slower than
    /// `PROMOTION_SAVE_WAIT`). Restarted from nodes.conf, it is a replica of
    /// the old master in its own view, and the owner of that master's slots
    /// in the others'.
    ///
    /// The rule. This node, a replica of master M in its own view, where M
    /// owns the slots S (not none), completes the promotion when Q masters
    /// have each told it, in their own PONG to it within the last two node
    /// timeouts, that they hold it as the owner of every slot of S at a
    /// config epoch greater than every config epoch this node knows for S
    /// (M's, and its own), each of them a master that owns slots in this
    /// node's view. Q is a majority, as for an election, of N masters: N
    /// the greater of the number of masters that own slots in this node's
    /// view and the largest number of slot-owning masters any of those
    /// PONGs reports (this node's view is as old as its nodes.conf, and the
    /// cluster may have grown since). It takes exactly S, at the greatest
    /// of those epochs: no slot outside S, even one the masters also hold
    /// it as the owner of (that is no part of the promotion; such a slot
    /// stays as each node's view has it). Not while a full sync it began is
    /// unfinished (it may hold part of a dataset). S is checked again, with
    /// its own role, as it takes the slots
    /// ([`Self::complete_promotion_self`]).
    ///
    /// A master does not report while its vote for a replacement of this
    /// node may still be won and announced, and votes for no replacement of
    /// it for a while after it reported ([`Self::owner_report_for`]): this
    /// node completes nothing on the word of a master that helped elect
    /// another in its place, and no replacement is elected with the votes
    /// of masters whose word it may be completing on. A master reports to
    /// one node for three node timeouts at most, so the votes are refused
    /// for six at most.
    ///
    /// Limit: a master that crashes and restarts within about a heartbeat
    /// forgets that it reported (or voted), as the reports and the
    /// replacements it voted for are not saved; the exclusion holds for the
    /// masters that keep running.
    ///
    /// Why this cannot make two owners of S:
    ///
    /// * The epoch is one a quorum already holds this node at for S, so the
    ///   cluster's routing does not change: no other node's claim is
    ///   overruled, here or anywhere, beyond what the quorum has decided.
    /// * A master that owns S, or some of it, at a newer epoch than that one
    ///   (another replica elected after this node, a `CLUSTER FAILOVER` or
    ///   `TAKEOVER` by another, a slot handover) has its claim taken in here
    ///   like any other: then M no longer owns S here, or this node follows
    ///   that master, and the rule does not apply. Each master that took that
    ///   claim in no longer holds this node as the owner of S and says so,
    ///   so it counts no more; and a claim of this node's at an older epoch
    ///   is refused everywhere.
    /// * Only what a master says of its own slot table counts, in its own
    ///   PONG: gossip carries none of it, so a relayed, stale view never
    ///   does. A master's word counts for two node timeouts, and is
    ///   withdrawn by its next PONG that holds this node as the owner of
    ///   nothing.
    /// * An epoch not greater than one this node knows for S, M's or its
    ///   own, is from before this node knew M as the owner: from a view in
    ///   which this node owned S before M did (M took S from it since), or
    ///   of other slots. It is refused.
    /// * The old master M, if it is alive (it was cut off, and is back),
    ///   owns S at an older epoch than the one taken: it takes this node's
    ///   claim in and follows it, as after any failover.
    pub fn promotion_to_complete(&self, full_sync_unfinished: bool) -> UnsavedPromotion {
        if !self.enabled {
            return UnsavedPromotion::None;
        }
        let me = self.myself();
        let (master, known_epoch) = {
            let nodes = self.nodes.read();
            let Some(node) = nodes.get(&me) else {
                return UnsavedPromotion::None;
            };
            let Some(master) = node.master_id else {
                return UnsavedPromotion::None;
            };
            if !node.flags.contains(NodeFlags::REPLICA) {
                return UnsavedPromotion::None;
            }
            let master_epoch = nodes.get(&master).map_or(0, |m| m.config_epoch);
            (master, master_epoch.max(node.config_epoch))
        };
        let reports = self.current_reports();
        if reports.is_empty() {
            return UnsavedPromotion::None;
        }
        let (master_slots, owners) = self.ranges_and_owners(master);
        decide_unsaved_promotion(
            master,
            &master_slots,
            known_epoch,
            &owners,
            &reports,
            full_sync_unfinished,
        )
    }

    /// The slot ranges `master` owns in this node's view, and the masters
    /// that own slots in it. One pass over the slot table.
    fn ranges_and_owners(&self, master: NodeId) -> (Vec<(u16, u16)>, ahash::AHashSet<NodeId>) {
        let slots = self.slots.read();
        let mut ranges: Vec<(u16, u16)> = Vec::new();
        let mut owners: ahash::AHashSet<NodeId> = ahash::AHashSet::new();
        let mut last: Option<NodeId> = None;
        for (i, owner) in slots.iter().enumerate() {
            if let Some(owner) = owner
                && last != Some(*owner)
            {
                owners.insert(*owner);
            }
            if *owner == Some(master) {
                let slot = i as u16;
                match ranges.last_mut() {
                    Some((_, end)) if *end + 1 == slot => *end = slot,
                    _ => ranges.push((slot, slot)),
                }
            }
            last = *owner;
        }
        (ranges, owners)
    }

    /// Forget what the masters said of this node's slots: its promotion is
    /// completed.
    pub fn forget_held_as_owner(&self) {
        self.held_as_owner.lock().clear();
    }

    /// Take over the slots `ranges` of `old_master` at `epoch`, as
    /// [`Self::promote_self`] does, for a promotion completed after a
    /// restart ([`Self::promotion_to_complete`]); but only if this node is
    /// still a replica of `old_master` and `old_master` still owns every one
    /// of them, checked with the slots taken under the same locks: a newer
    /// owner's claim (a `CLUSTER FAILOVER TAKEOVER`, another replica's
    /// election) taken in since the decision leaves them alone. `Err` says
    /// why not. Takes `nodes`, then `slots`. Fenced, if its dataset at
    /// startup was not known to be whole, before the slots are its own
    /// here ([`Self::fence_completion`]).
    pub fn complete_promotion_self(
        &self,
        old_master: NodeId,
        epoch: u64,
        ranges: &[(u16, u16)],
    ) -> Result<Vec<u16>, String> {
        if !self.enabled || ranges.is_empty() {
            return Err("nothing to take".into());
        }
        let me = self.myself();
        let mut claimed = Vec::new();
        let fenced;
        {
            let mut nodes = self.nodes.write();
            let still_its_replica = nodes.get(&me).is_some_and(|n| {
                n.flags.contains(NodeFlags::REPLICA) && n.master_id == Some(old_master)
            });
            if !still_its_replica {
                return Err(format!(
                    "this node no longer follows {}",
                    node_id_str(&old_master)
                ));
            }
            {
                let mut slots = self.slots.write();
                for &(start, end) in ranges {
                    for slot in start..=end {
                        let owner = slots.get(slot as usize).copied().flatten();
                        if owner != Some(old_master) {
                            return Err(format!(
                                "slot {slot} is no longer {}'s here",
                                node_id_str(&old_master)
                            ));
                        }
                    }
                }
                // Up before any of the slots is this node's here, so that
                // routing never sees it the owner unfenced.
                fenced = self.fence_completion(&nodes, me, old_master);
                for &(start, end) in ranges {
                    for slot in start..=end {
                        slots[slot as usize] = Some(me);
                        claimed.push(slot);
                    }
                }
                self.slot_deletions.came_here(claimed.iter().copied());
            }
            if let Some(node) = nodes.get_mut(&me) {
                node.flags.remove(NodeFlags::REPLICA);
                node.flags |= NodeFlags::MASTER;
                node.master_id = None;
                node.config_epoch = epoch;
            }
            // As `promote_self`: the old master, if it comes back, follows
            // the claim.
            if let Some(old) = nodes.get_mut(&old_master) {
                old.flags.remove(NodeFlags::MASTER);
            }
        }
        if let Some(said) = fenced {
            log_warn!("{said}");
        }
        *self.took_over_from.lock() = Some(old_master);
        self.refresh_state();
        Ok(claimed)
    }

    /// Note that master `from` has answered this node with a PONG: see
    /// [`Self::first_sync_may_begin`].
    pub fn note_pong_heard(&self, from: NodeId) {
        if !self.first_sync.open.load(Ordering::Relaxed) {
            self.first_sync.heard.lock().insert(from);
        }
    }

    /// Whether this node, a replica since it started, may begin its first
    /// sync with the master its nodes.conf names. It waits only while the
    /// rule of [`Self::promotion_to_complete`] can still fire: until enough
    /// of the masters have answered without a report that it cannot (more
    /// than `N - Q` of them, of the masters that own slots in its view, `N`
    /// and `Q` as in that rule), or its master owns no slots here; and one
    /// node timeout since it started at most. Not while the rule says the
    /// promotion is to be completed. Once true it stays true: only the
    /// first sync after a start waits, never a later one, nor a node that is
    /// no replica (or not in cluster mode).
    ///
    /// So a node restarted before its promotion was saved completes the
    /// promotion with the dataset it has, and a replica restarted in an
    /// ordinary way begins its sync once a majority of the masters, at most,
    /// have answered it: with 1 or 2 masters, on the first answer (its own
    /// master's, say); with 3 or 4, on the second; 5, the third; 7, the
    /// fourth. The masters answer its first heartbeats, sent as its bus
    /// starts.
    fn first_sync_may_begin(&self) -> bool {
        if self.first_sync.open.load(Ordering::Relaxed) {
            return true;
        }
        let me = self.myself();
        let master = self.nodes.read().get(&me).and_then(|n| n.master_id);
        let reports = self.current_reports();
        let (master_slots, owners) = match master {
            Some(master) => self.ranges_and_owners(master),
            None => (Vec::new(), ahash::AHashSet::new()),
        };
        let can_fire = !master_slots.is_empty() && {
            let (n, quorum) = completion_quorum(&owners, &reports);
            let reporting: ahash::AHashSet<NodeId> = reports.iter().map(|(id, _)| *id).collect();
            let without = self
                .first_sync
                .heard
                .lock()
                .iter()
                .filter(|id| owners.contains(*id) && !reporting.contains(*id))
                .count();
            without <= n.saturating_sub(quorum)
        };
        let waited = self.first_sync.started.elapsed()
            >= std::time::Duration::from_millis(self.node_timeout_ms);
        if can_fire && !waited {
            return false;
        }
        // Decided on this tick or the next by the election check, which
        // runs before this one.
        if matches!(
            self.promotion_to_complete(false),
            UnsavedPromotion::Complete { .. }
        ) {
            return false;
        }
        self.first_sync.open.store(true, Ordering::Relaxed);
        self.first_sync.heard.lock().clear();
        true
    }

    /// For the tests: what master `from` last said of this node's slots.
    #[cfg(test)]
    pub(crate) fn held_as_owner_by(&self, from: NodeId) -> Option<Vec<(u16, u16, u64)>> {
        self.held_as_owner.lock().get(&from).map(|h| {
            h.report
                .claims
                .iter()
                .map(|c| (c.start, c.end, c.epoch))
                .collect()
        })
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
        // Its old master may have handed slots over that it still has the
        // keys of (#517): they are reported, never deleted on their own.
        self.orphans.ask(Some("after its promotion"));
    }

    // ── Migration state (Step 7c) ─────────────────────────────────────────────

    pub fn migration_state(&self, slot: u16) -> Option<MigrationState> {
        self.migrations.read().get(&slot).copied()
    }

    /// Whether any slot is being migrated away from this node or imported
    /// into it (`SETSLOT MIGRATING` / `IMPORTING`): one relaxed load.
    #[inline]
    pub fn slots_open(&self) -> bool {
        self.migrations.any_open()
    }

    /// `CLUSTER SETSLOT <slot> ...`: `f`, the change it makes, unless an
    /// atomic slot migration of `slot` to or from this node is under way
    /// (`migration::Registry::setslot_refusal`, #755). Done with the
    /// migration tasks locked: no atomic one begins in between, as one does
    /// not begin on a slot in a legacy migration. `f` takes no lock that
    /// comes before them (see [`ClusterState`]).
    pub fn setslot_unless_migrating<R>(
        &self,
        slot: u16,
        f: impl FnOnce() -> Result<R, String>,
    ) -> Result<R, String> {
        self.slot_migrations.with_tasks(|tasks| {
            if let Some(why) = migration::Registry::setslot_refusal(tasks, slot) {
                return Err(why);
            }
            f()
        })
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
        self.failover_began();
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

    /// Count a failover of this node as begun, now. Under `handoff_holds`.
    fn failover_began(&self) {
        self.failovers_begun.fetch_add(1, Ordering::SeqCst);
        *self.last_failover_began.lock() = Some(std::time::Instant::now());
    }

    /// How long after a failover of a master began a replica of it may still
    /// be promoted by it: an election lives for [`ELECTION_LIFETIME`], and a
    /// vote is counted only while it does (the candidate waits for one at
    /// most that long, or two node timeouts); a margin of a node timeout (at
    /// least 2 s) on top. Never less than two node
    /// timeouts, the time a master waits between votes for replicas of the
    /// same master. Neither a source nor a destination hands slots over in
    /// that time after it saw such a failover begin (#516).
    pub(crate) fn failover_window(&self) -> std::time::Duration {
        let node_timeout = std::time::Duration::from_millis(self.node_timeout_ms);
        let vote_wait = node_timeout.max(std::time::Duration::from_secs(2));
        (ELECTION_LIFETIME + vote_wait).max(node_timeout * 2)
    }

    /// Whether a replica of `dest`, a destination this node handed slots to
    /// and that failed before it said whether it took them, may still be
    /// elected in its place: `dest` is FAIL here, a replica of it is not
    /// failed, none of its replicas has become a master yet (`watch` keeps
    /// the ones seen across calls: a promoted one no longer names `dest` as
    /// its master), and `dest` was marked FAIL less than a
    /// [`Self::failover_window`] ago. Whatever `dest` owns here: a new,
    /// empty destination (a scale-out) owns nothing here but the slots this
    /// node may have handed it, and its replica can be elected with just
    /// those. Such a replica, if it heard `dest`'s claim of the slots, takes
    /// them over at its election's epoch, with the writes `dest` took to
    /// them; this node claiming them back meanwhile would make a second
    /// claim of them after `dest` failed, and only one of the two can stand
    /// (#743). So this node waits for that failover first, and claims the
    /// slots back only if it ends without them going to the replica. A
    /// replica seen promoted is waited for one moment more ([`FailoverWatch`]):
    /// its claims come in the same heartbeat as its role.
    pub(crate) fn destination_may_fail_over(
        &self,
        dest: NodeId,
        watch: &mut FailoverWatch,
    ) -> bool {
        let fail_since_ms = {
            let nodes = self.nodes.read();
            let Some(d) = nodes.get(&dest) else {
                return false;
            };
            if !d.flags.contains(NodeFlags::FAIL) {
                return false;
            }
            for n in nodes.values() {
                if n.master_id == Some(dest)
                    && n.flags.contains(NodeFlags::REPLICA)
                    && !watch.replicas.contains(&n.id)
                {
                    watch.replicas.push(n.id);
                }
            }
            let promoted = watch.replicas.iter().any(|id| {
                nodes
                    .get(id)
                    .is_some_and(|n| n.flags.contains(NodeFlags::MASTER))
            });
            if promoted {
                let seen = *watch
                    .promoted_at
                    .get_or_insert_with(std::time::Instant::now);
                return seen.elapsed() < FAILOVER_WATCH_SETTLE;
            }
            let replica_up = watch.replicas.iter().any(|id| {
                nodes.get(id).is_some_and(|n| {
                    n.master_id == Some(dest)
                        && n.flags.contains(NodeFlags::REPLICA)
                        && !n.flags.intersects(NodeFlags::FAIL | NodeFlags::HANDSHAKE)
                })
            });
            if !replica_up {
                return false;
            }
            d.fail_since_ms
        };
        now_ms().saturating_sub(fail_since_ms) < self.failover_window().as_millis() as u64
    }

    /// The handoff of an atomic slot migration away from this node is about
    /// to ask the destination to take the slots over (`TAKEOVER`): whether
    /// it may (#516). `still` says whether this node still is what it was
    /// when the migration began: the master of the slots, with its own
    /// dataset and config epoch. No failover of this node may have begun
    /// since the migration did (`failovers_since`, from
    /// [`Self::failovers_begun`]), nor in the [`Self::failover_window`]
    /// before now, whenever the migration began; nor be in doubt: writes
    /// held until this node knows whether a replica of it took its slots
    /// over.
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
            return Err(FAILOVER_BEGAN.into());
        }
        if let Some(at) = *self.last_failover_began.lock()
            && at.elapsed() < self.failover_window()
        {
            return Err(format!(
                "{FAILOVER_BEGAN_RECENTLY} {} ms ago, and a replica of it may still be elected",
                at.elapsed().as_millis()
            ));
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
        *self.last_handover_end.lock() = Some(std::time::Instant::now());
    }

    /// Whether a replica of this master may take its slots over now, as it
    /// asks with `CLUSTER FAILOVER` or for this node's vote (#516). Not while
    /// an atomic slot migration away from this node is handing slots over,
    /// nor for [`HANDOVER_FAILOVER_GRACE`] after one ended: a promoted
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
        if let Some(at) = *self.last_handover_end.lock()
            && at.elapsed() < HANDOVER_FAILOVER_GRACE
        {
            return Err(format!(
                "an atomic slot migration away from this node ended {} ms ago: its \
                 replicas may not have heard yet which slots moved; ask again in a few seconds",
                at.elapsed().as_millis()
            ));
        }
        self.slot_migrations
            .fence_imports(None, "this node is failing over to a replica of it")?;
        self.failover_began();
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
        self.forget_takeover_links();
        self.own_claims.write().clear();
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
        self.reported_as_owner.lock().clear();
        self.held_as_owner.lock().clear();
        let new_id = generate_node_id();
        {
            let mut nodes = self.nodes.write();
            let mut myself = self.myself.write();
            if let Some(mut me) = nodes.remove(&*myself) {
                me.id = new_id;
                me.config_epoch = 0;
                // A new shard too, as Redis's hard reset makes one.
                me.shard_id = generate_node_id();
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
/// the destination vote for one of them: three heartbeats, time for them to
/// hear of the destination's claim (#516).
pub(crate) const HANDOVER_FAILOVER_GRACE: std::time::Duration =
    std::time::Duration::from_secs(3 * bus::heartbeat::HEARTBEAT_INTERVAL.as_secs());

/// How long an election runs before it is given up (`check_election`).
pub(crate) const ELECTION_LIFETIME: std::time::Duration = std::time::Duration::from_secs(10);

/// Why a migration away from this node gives its handover up when a failover
/// of it has begun (#516). The tests look for it in the log.
pub(crate) const FAILOVER_BEGAN: &str = "a failover of this node began during the migration \
     (CLUSTER FAILOVER on a replica of it, a vote for one, or a FAIL verdict)";
const FAILOVER_BEGAN_RECENTLY: &str = "a failover of this node began";

/// The least time between two of the saves the bus asks for (`save_soon`),
/// which are made on a thread of their own. A save is synced twice (the file
/// before it is renamed into place, and its directory after): in a cluster
/// being formed, each node learning every other within a second or two, a
/// save for each node learnt would keep a disk the nodes share so busy
/// that `redis-cli --cluster create` of 30 nodes takes a second longer
/// (measured). A save something waits on (a node met, a vote, a command's,
/// a promotion) is made at once, by what waits.
const BACKGROUND_SAVE_GAP: std::time::Duration = std::time::Duration::from_millis(100);

/// What a source waiting on a failed destination's failover has seen of it
/// across its checks: see [`ClusterState::destination_may_fail_over`].
#[derive(Debug, Default)]
pub(crate) struct FailoverWatch {
    /// The destination's replicas seen so far.
    replicas: Vec<NodeId>,
    /// When one of them was first seen a master.
    promoted_at: Option<std::time::Instant>,
}

/// How long a source still waits once a replica of its failed destination
/// is seen a master, for that replica's claims (#743). They come in the
/// heartbeat that says it is a master, taken in just after its role.
const FAILOVER_WATCH_SETTLE: std::time::Duration = std::time::Duration::from_secs(1);

/// How long a node met waits for the save that makes it a member.
const ADMIT_SAVE_WAIT: std::time::Duration = std::time::Duration::from_secs(20);

/// One admission of a node met: see [`ClusterState::admit_met_node`].
struct Admission {
    /// Its entry in `ClusterState::joining`.
    number: u64,
    id: NodeId,
    /// The first save that has the node.
    from: u64,
}

/// How long a promotion waits for its save before it is announced: see
/// [`ClusterState::save_promotion`].
pub(crate) const PROMOTION_SAVE_WAIT: std::time::Duration = std::time::Duration::from_secs(1);

/// This process's cluster state, once its bus has started: see
/// [`save_at_shutdown`].
static FOR_SHUTDOWN: std::sync::OnceLock<std::sync::Weak<ClusterState>> =
    std::sync::OnceLock::new();

/// Save nodes.conf as the node knows the cluster now, at the start of a
/// shutdown, and wait for it. Changes learnt from the bus are saved on a
/// thread of their own, which the process does not wait for when it exits:
/// this save is, so the file it leaves has every change it knew of (#808).
/// Nothing outside cluster mode, and nothing is written if nothing has
/// changed since the last save.
pub fn save_at_shutdown() {
    let Some(state) = FOR_SHUTDOWN.get().and_then(std::sync::Weak::upgrade) else {
        return;
    };
    if let Err(e) = state.save_to_disk() {
        log_warn!("Cluster: could not save the cluster config file at shutdown: {e}");
    }
}

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// The `N` and `Q` of [`ClusterState::promotion_to_complete`]: `N` the
/// greater of the number of `owners` (the masters that own slots in this
/// node's view) and the largest count of slot-owning masters among the
/// `reports` of those owners (a member that owns no slots here does not
/// raise it); `Q` a majority of `N`.
fn completion_quorum(
    owners: &ahash::AHashSet<NodeId>,
    reports: &[(NodeId, crate::cluster::bus::OwnerReport)],
) -> (usize, usize) {
    let reported = reports
        .iter()
        .filter(|(from, _)| owners.contains(from))
        .map(|(_, r)| r.masters)
        .max()
        .unwrap_or(0);
    let n = owners.len().max(reported);
    (n, n / 2 + 1)
}

#[cfg(test)]
thread_local! {
    /// For a test of what comes in between: run in `cast_vote` just before
    /// it takes `last_vote_for`.
    static BEFORE_VOTE_LOCK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

/// Have `hook` run, on this thread, in the next `cast_vote` just before it
/// takes `last_vote_for`.
#[cfg(test)]
pub(crate) fn run_before_vote_lock(hook: impl FnOnce() + 'static) {
    BEFORE_VOTE_LOCK.with(|h| *h.borrow_mut() = Some(Box::new(hook)));
}

#[cfg(test)]
fn before_vote_lock() {
    if let Some(hook) = BEFORE_VOTE_LOCK.with(|h| h.borrow_mut().take()) {
        hook();
    }
}

/// Whether `claims` cover every slot of `ranges` (each sorted, disjoint).
/// Compares ranges: no table of the slots is built.
fn claims_cover(claims: &[crate::cluster::bus::SlotClaim], ranges: &[(u16, u16)]) -> bool {
    let mut spans: Vec<(u16, u16)> = claims.iter().map(|c| (c.start, c.end)).collect();
    spans.sort_unstable();
    // Merged, touching ones too.
    let mut merged: Vec<(u16, u16)> = Vec::with_capacity(spans.len());
    for (start, end) in spans {
        match merged.last_mut() {
            Some((_, last_end)) if u32::from(start) <= u32::from(*last_end) + 1 => {
                *last_end = (*last_end).max(end);
            }
            _ => merged.push((start, end)),
        }
    }
    ranges.iter().all(|&(start, end)| {
        // The last merged span that starts at or before `start`.
        let i = merged.partition_point(|&(s, _)| s <= start);
        i > 0 && merged[i - 1].1 >= end
    })
}

/// The rule of [`ClusterState::promotion_to_complete`], as a function of
/// this node's view: its master `master`, the slot ranges it owns here
/// (`master_slots`, sorted), the greatest config epoch this node knows for
/// them (`known_epoch`: the master's or its own), the masters that own slots
/// here (`owners`), and what each master said (`reports`: the slots it holds
/// this node as the owner of, each at this node's config epoch in its view,
/// and how many masters own slots there).
fn decide_unsaved_promotion(
    master: NodeId,
    master_slots: &[(u16, u16)],
    known_epoch: u64,
    owners: &ahash::AHashSet<NodeId>,
    reports: &[(NodeId, crate::cluster::bus::OwnerReport)],
    full_sync_unfinished: bool,
) -> UnsavedPromotion {
    if master_slots.is_empty() || reports.is_empty() {
        return UnsavedPromotion::None;
    }
    let (n, quorum) = completion_quorum(owners, reports);
    // The epoch each master that owns slots here holds this node at as the
    // owner of every one of the master's slots.
    let mut covering: Vec<u64> = Vec::new();
    for (from, report) in reports {
        if !owners.contains(from) || !claims_cover(&report.claims, master_slots) {
            continue;
        }
        // One epoch, this node's in that master's view; the lowest, if a
        // claim ever said otherwise.
        covering.push(report.claims.iter().map(|c| c.epoch).min().unwrap_or(0));
    }
    if covering.len() < quorum {
        // Enough in this node's own view, not in the cluster's as the
        // masters report it: said, as it would be taken otherwise.
        if covering.len() > owners.len() / 2 {
            return UnsavedPromotion::NotCompleted(format!(
                "{} masters hold it as the owner of the slots of {}, and the masters report {n} \
                 masters that own slots, of which {quorum} are needed",
                covering.len(),
                node_id_str(&master),
            ));
        }
        return UnsavedPromotion::None;
    }
    let newer: Vec<u64> = covering
        .iter()
        .copied()
        .filter(|&e| e > known_epoch)
        .collect();
    if newer.len() < quorum {
        return UnsavedPromotion::NotCompleted(format!(
            "{} masters hold it as the owner of the slots of {}, {} of them at a config epoch \
             newer than the epoch {known_epoch} it knows for those slots, and {quorum} of {n} \
             are needed",
            covering.len(),
            node_id_str(&master),
            newer.len(),
        ));
    }
    if full_sync_unfinished {
        return UnsavedPromotion::NotCompleted(
            "it has not finished loading a full sync, and may hold part of a dataset".into(),
        );
    }
    UnsavedPromotion::Complete {
        master,
        epoch: newer.into_iter().max().unwrap_or(known_epoch),
        slots: master_slots.to_vec(),
    }
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

    fn shard_of(cs: &ClusterState, id: NodeId) -> NodeId {
        cs.nodes.read()[&id].shard_id
    }

    /// A master's word on its shard id is its shard's: its replicas take it,
    /// this node among them. A replica's is not taken: it is in its master's.
    /// A node of an earlier version says none, and changes nothing (#563).
    #[test]
    fn a_shard_takes_its_masters_id() {
        let cs = ClusterState::new_enabled(None, 15000);
        let master = add_master(&cs, 0..=99);
        let other = add_replica(&cs, master);
        cs.set_replica_of(master).unwrap();
        assert_eq!(
            cs.my_shard_id(),
            shard_of(&cs, master),
            "REPLICATE takes it"
        );

        let said = generate_node_id();
        cs.take_shard_id(master, Some(said));
        assert_eq!(shard_of(&cs, master), said);
        assert_eq!(shard_of(&cs, other), said);
        assert_eq!(cs.my_shard_id(), said);

        cs.take_shard_id(other, Some(generate_node_id()));
        assert_eq!(
            shard_of(&cs, other),
            said,
            "a replica's own word is not taken"
        );
        cs.take_shard_id(master, None);
        assert_eq!(shard_of(&cs, master), said, "an earlier version's silence");
    }

    /// A replica that takes over keeps its shard id, its master's: the shard
    /// has the same id after the failover. A hard reset makes a new one.
    #[test]
    fn a_shard_keeps_its_id_across_a_failover() {
        let cs = ClusterState::new_enabled(None, 15000);
        let master = add_master(&cs, 0..=99);
        cs.set_replica_of(master).unwrap();
        let shard = cs.my_shard_id();
        cs.promote_self(master, 5);
        assert_eq!(cs.my_shard_id(), shard);
        assert_eq!(cs.my_master_id(), None);

        cs.hard_reset();
        assert_ne!(cs.my_shard_id(), shard);
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

    /// FLUSHSLOTS gives up this node's own slots only (#552).
    #[test]
    fn flush_slots_gives_up_only_this_nodes_slots() {
        let (cs, a, b) = three_master_cluster();
        assert_eq!(cs.flush_slots(), 5461);
        assert!((0..=5460).all(|s| cs.slot_owner(s).is_none()));
        assert!((5461..=10922).all(|s| cs.slot_owner(s) == Some(a)));
        assert!((10923..=16383).all(|s| cs.slot_owner(s) == Some(b)));
    }

    /// ADDSLOTS and ADDSLOTSRANGE claim all their slots or none (#553).
    #[test]
    fn claiming_slots_is_all_or_nothing() {
        let cs = ClusterState::new_enabled(None, 15000);
        cs.claim_slots(&[200]).unwrap();
        let list: Vec<u16> = (0..=99).chain(150..=250).collect();
        assert_eq!(
            cs.claim_slots(&list).unwrap_err(),
            "Slot 200 is already busy"
        );
        assert_eq!(
            cs.check_slots_claimable(&list).unwrap_err(),
            "Slot 200 is already busy"
        );
        assert!((0..=99).all(|s| cs.slot_owner(s).is_none()));
        assert_eq!(
            cs.claim_slots(&[1, 2, 1]).unwrap_err(),
            "Slot 1 specified multiple times"
        );
        assert!(cs.slot_owner(1).is_none());
        assert_eq!(cs.claim_slots(&[1, 2]).unwrap(), 2);
        assert_eq!(cs.slot_owner(2), Some(cs.myself()));
    }

    /// BUMPEPOCH gives this node a config epoch above every other, or
    /// says it has the highest already (#563).
    #[test]
    fn bump_config_epoch_without_consensus_is_redis_s() {
        let (cs, a, _) = three_master_cluster();
        cs.nodes.write().get_mut(&a).unwrap().config_epoch = 7;
        assert_eq!(cs.bump_config_epoch_without_consensus(), (true, 8));
        assert_eq!(cs.current_epoch(), 8);
        assert_eq!(cs.bump_config_epoch_without_consensus(), (false, 8));
        // A peer moves ahead: this node bumps again.
        cs.observe_epoch(12);
        assert_eq!(cs.bump_config_epoch_without_consensus(), (true, 13));
    }

    /// A slot claimed with ADDSLOTS is no longer importing, as in Redis's
    /// `clusterUpdateSlots`.
    #[test]
    fn claiming_a_slot_ends_its_import() {
        let cs = ClusterState::new_enabled(None, 15000);
        let other = add_master(&cs, 100..=100);
        cs.set_importing(5, other).unwrap();
        cs.set_importing(6, other).unwrap();
        cs.claim_slots(&[5]).unwrap();
        assert!(!cs.migrations.read().contains_key(&5));
        assert!(matches!(
            cs.migrations.read().get(&6),
            Some(MigrationState::Importing { .. })
        ));
        cs.claim_slot_range(6, 7).unwrap();
        assert!(cs.migrations.read().is_empty());
    }

    /// `slots_open` follows every change of the migration states, whoever
    /// makes it.
    #[test]
    fn slots_open_says_whether_any_slot_is_migrating_or_importing() {
        let cs = ClusterState::new_enabled(None, 15000);
        assert!(!cs.slots_open());
        let other = add_master(&cs, 100..=100);
        cs.set_importing(5, other).unwrap();
        assert!(cs.slots_open());
        cs.claim_slots(&[1]).unwrap();
        cs.set_migrating(1, other).unwrap();
        cs.clear_migration(5);
        assert!(cs.slots_open());
        cs.finalize_slot(1, other).unwrap();
        assert!(!cs.slots_open());
        cs.set_importing(7, other).unwrap();
        cs.migrations.write().retain(|_, _| false);
        assert!(!cs.slots_open());
        cs.set_importing(7, other).unwrap();
        cs.soft_reset();
        assert!(!cs.slots_open());
    }

    /// SET-CONFIG-EPOCH only on a node that knows no other one (#563).
    #[test]
    fn set_config_epoch_needs_a_lone_node() {
        let (cs, _, _) = three_master_cluster();
        let e = cs.set_my_config_epoch(5).unwrap_err();
        assert!(e.contains("does not know any other node"), "{e}");
        let lone = ClusterState::new_enabled(None, 15000);
        lone.set_my_config_epoch(5).unwrap();
        assert_eq!(
            lone.set_my_config_epoch(6).unwrap_err(),
            "Node config epoch is already non-zero"
        );
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
        *cs.last_failover_began.lock() = None;
        let begun = cs.failovers_begun();
        cs.begin_handover(begun, || Ok(())).unwrap();
        let refused = cs.failover_may_begin().unwrap_err();
        assert!(refused.contains("handing its slots over"), "{refused}");
        cs.end_handover();
        let refused = cs.failover_may_begin().unwrap_err();
        assert!(refused.contains("ended"), "{refused}");
        *cs.last_handover_end.lock() = None;
        cs.failover_may_begin().unwrap();

        // A handover in the time a replica may still be elected in, by a
        // failover begun before the migration did, is refused too.
        let begun = cs.failovers_begun();
        let refused = cs.begin_handover(begun, || Ok(())).unwrap_err();
        assert!(refused.contains("may still be elected"), "{refused}");

        // A handover this node is no longer fit for holds nothing.
        *cs.last_failover_began.lock() = None;
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
        moved("FLUSHSLOTS", &|| {
            cs.flush_slots();
        });
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

        assert!(
            !cs.cast_vote(replica, a, 5, false, None),
            "master is healthy"
        );
        assert!(cs.cast_vote(replica, a, 5, true, None), "manual failover");
    }

    /// A vote is refused to a candidate that claims, in an automatic
    /// failover's request, a slot this node knows a newer owner of: a slot
    /// its master gave away, in a handover or a failover it has not heard of
    /// (#524). Promoted, it would claim the slot at the election's epoch,
    /// newer than the owner's, and the owner's acknowledged writes to it
    /// would be lost. A claim at the owner's epoch or newer, of its
    /// master's own slots or of slots no one owns, is not refused; nor is a
    /// request without claims, as an earlier version sends it.
    #[test]
    fn no_vote_for_a_candidate_claiming_a_slot_with_a_newer_owner() {
        let claim = |start, end, epoch| crate::cluster::bus::SlotClaim { start, end, epoch };
        let (cs, a, b) = three_master_cluster();
        let me = cs.myself();
        let replica = add_replica(&cs, a);
        {
            let mut nodes = cs.nodes.write();
            nodes.get_mut(&a).unwrap().mark_failed(now_ms());
            nodes.get_mut(&a).unwrap().config_epoch = 3;
            nodes.get_mut(&b).unwrap().config_epoch = 7;
            nodes.get_mut(&me).unwrap().config_epoch = 5;
        }
        // `b` has taken 5461-5500 from `a`, in this node's view.
        {
            let mut slots = cs.slots.write();
            for slot in 5461..=5500 {
                slots[slot] = Some(b);
            }
        }
        let stale = [claim(5461, 10922, 3)];
        assert!(!cs.cast_vote(replica, a, 10, false, Some(&stale)));
        // One of this node's own.
        assert!(!cs.cast_vote(replica, a, 11, false, Some(&[claim(100, 100, 3)])));
        // Refused, the vote is not spent: the same candidate, its claims
        // now current, has it. So would a slot no one owns, or one of `b`'s
        // claimed at `b`'s epoch.
        let current = [claim(5501, 10922, 3), claim(5461, 5461, 7)];
        assert!(cs.cast_vote(replica, a, 12, false, Some(&current)));

        // A request without claims is decided as before.
        let (cs, a, b) = three_master_cluster();
        let replica = add_replica(&cs, a);
        {
            let mut nodes = cs.nodes.write();
            nodes.get_mut(&a).unwrap().mark_failed(now_ms());
            nodes.get_mut(&b).unwrap().config_epoch = 7;
        }
        cs.slots.write()[5461] = Some(b);
        assert!(cs.cast_vote(replica, a, 10, false, None));
    }

    /// What an `UPDATE` names for a refused candidate: each newer owner of
    /// a claimed slot once, with what it last claimed in its own words;
    /// nothing for slots no one newer owns, nor for an owner not heard
    /// from first-hand (#742).
    #[test]
    fn the_newer_owners_of_claimed_slots_are_named_with_all_their_slots() {
        let claim = |start, end, epoch| crate::cluster::bus::SlotClaim { start, end, epoch };
        let (cs, a, b) = three_master_cluster();
        {
            let mut nodes = cs.nodes.write();
            nodes.get_mut(&a).unwrap().config_epoch = 3;
            nodes.get_mut(&b).unwrap().config_epoch = 7;
        }
        {
            let mut slots = cs.slots.write();
            for slot in 5461..=5500 {
                slots[slot] = Some(b);
            }
        }
        // Not heard from b itself: nothing relayed.
        assert!(
            cs.newer_owners_of_claimed(a, &[claim(5461, 10922, 3)])
                .is_empty()
        );
        cs.note_own_claims(b, &[claim(5461, 5500, 7), claim(10923, 16383, 7)]);
        let owners = cs.newer_owners_of_claimed(a, &[claim(5461, 10922, 3)]);
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0].id, b);
        assert_eq!(owners[0].config_epoch, 7);
        assert_eq!(
            owners[0].claims,
            vec![claim(5461, 5500, 7), claim(10923, 16383, 7)]
        );
        assert!(
            cs.newer_owners_of_claimed(a, &[claim(5501, 10922, 3)])
                .is_empty()
        );
        // Claimed at b's own epoch or later: not newer.
        assert!(
            cs.newer_owners_of_claimed(a, &[claim(5461, 5461, 7)])
                .is_empty()
        );
    }

    /// A node that took a master's slots over moves on a config epoch
    /// collision with it, whatever the ids; a node that gave its slots to a
    /// replica at a tie never moves on a collision with it (#743).
    #[test]
    fn on_a_collision_the_node_that_took_over_moves() {
        for _ in 0..16 {
            let cs = ClusterState::new_enabled(None, 15000);
            let a = add_master(&cs, 0..=99);
            let _b = add_master(&cs, 100..=16383);
            cs.nodes.write().get_mut(&a).unwrap().config_epoch = 4;
            cs.observe_epoch(4);
            let me = cs.myself();
            cs.set_replica_of(a).unwrap();
            cs.promote_self(a, 9);
            {
                let mut nodes = cs.nodes.write();
                let old = nodes.get_mut(&a).unwrap();
                old.config_epoch = 9;
                old.flags |= NodeFlags::MASTER;
            }
            assert!(
                cs.resolve_epoch_collision(a, 9),
                "the node that took over moves"
            );
            assert!(cs.nodes.read()[&me].config_epoch > 9);
        }
        for _ in 0..16 {
            let (cs, a, _) = three_master_cluster();
            let me = cs.myself();
            cs.nodes.write().get_mut(&me).unwrap().config_epoch = 9;
            cs.nodes.write().get_mut(&a).unwrap().config_epoch = 9;
            cs.yielded_to.lock().insert(a);
            assert!(!cs.resolve_epoch_collision(a, 9));
            assert_eq!(cs.nodes.read()[&me].config_epoch, 9);
        }
    }

    /// The takeover links survive a restart: nodes.conf's `vars` line
    /// carries them, a node started from it has them, and the file still
    /// reads as before (an earlier version skips the pairs it does not
    /// know). They go once the collision with that node is over (#743).
    #[test]
    fn takeover_links_are_saved_read_back_and_let_go() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nodes.conf");
        let cs = ClusterState::from_disk_or_fresh(Some(path.clone()), 15000).unwrap();
        let m = add_master(&cs, 0..=99);
        let y = add_master(&cs, 100..=199);
        *cs.took_over_from.lock() = Some(m);
        cs.yielded_to.lock().insert(y);
        cs.save_to_disk().unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        // The last line but for the blank padding of a direct write.
        let vars = text.lines().rfind(|l| !l.trim().is_empty()).unwrap();
        assert!(vars.starts_with("vars currentEpoch "), "{vars}");
        assert!(vars.contains(&format!("tookOverFrom {}", node_id_str(&m))));
        assert!(vars.contains(&format!("yieldedTo {}", node_id_str(&y))));
        let again = ClusterState::from_disk_or_fresh(Some(path.clone()), 15000).unwrap();
        assert_eq!(*again.took_over_from.lock(), Some(m));
        assert!(again.yielded_to.lock().contains(&y));

        // Same epoch, or below, both masters: kept. Above: let go.
        let me = again.myself();
        again.nodes.write().get_mut(&me).unwrap().config_epoch = 5;
        again.settle_takeover_links(m, true, 5);
        again.settle_takeover_links(m, true, 4);
        assert_eq!(*again.took_over_from.lock(), Some(m));
        again.settle_takeover_links(m, true, 6);
        assert_eq!(*again.took_over_from.lock(), None);
        // The node yielded to says replica, or is below: kept; above: gone.
        again.settle_takeover_links(y, false, 5);
        again.settle_takeover_links(y, true, 4);
        assert!(again.yielded_to.lock().contains(&y));
        again.settle_takeover_links(y, true, 6);
        assert!(again.yielded_to.lock().is_empty());
        again.save_to_disk().unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(!text.contains("tookOverFrom") && !text.contains("yieldedTo"));
    }

    /// A heartbeat of the old master sent before the takeover, at E-1, and
    /// still on its way when the replica has taken over at E, does not let
    /// the replica's link go: the old master's next heartbeat, at E, is a
    /// collision, and the replica moves, whatever the ids (the old master,
    /// which yielded, does not). Its own stale heartbeats as a replica, and
    /// below, leave the old master's link too (#743).
    #[test]
    fn a_stale_heartbeat_keeps_the_takeover_links_until_the_collision() {
        for _ in 0..16 {
            // The replica's side, its old master's id the lower.
            let cs = ClusterState::new_enabled(None, 15000);
            let me = cs.myself();
            let m = loop {
                let m = add_master(&cs, 0..=99);
                if m < me {
                    break m;
                }
                cs.nodes.write().remove(&m);
            };
            let _b = add_master(&cs, 100..=16383);
            cs.nodes.write().get_mut(&m).unwrap().config_epoch = 4;
            cs.observe_epoch(4);
            cs.set_replica_of(m).unwrap();
            cs.promote_self(m, 5);
            {
                let mut nodes = cs.nodes.write();
                let old = nodes.get_mut(&m).unwrap();
                old.flags |= NodeFlags::MASTER;
            }
            // Stale, at E-1.
            cs.settle_takeover_links(m, true, 4);
            assert_eq!(
                *cs.took_over_from.lock(),
                Some(m),
                "a stale heartbeat let it go"
            );
            // The collision at E.
            cs.nodes.write().get_mut(&m).unwrap().config_epoch = 5;
            assert!(cs.resolve_epoch_collision(m, 5), "the replica must move");
            assert!(cs.nodes.read()[&me].config_epoch > 5);

            // The old master's side: it yielded to r at E.
            let (cs, r, _) = three_master_cluster();
            let me = cs.myself();
            cs.nodes.write().get_mut(&me).unwrap().config_epoch = 5;
            cs.nodes.write().get_mut(&r).unwrap().config_epoch = 5;
            cs.yielded_to.lock().insert(r);
            cs.settle_takeover_links(r, false, 3);
            cs.settle_takeover_links(r, true, 4);
            assert!(cs.yielded_to.lock().contains(&r));
            assert!(
                !cs.resolve_epoch_collision(r, 5),
                "the old master must not move"
            );
            // Once the replica is above, the link goes.
            cs.settle_takeover_links(r, true, 6);
            assert!(!cs.yielded_to.lock().contains(&r));
        }
    }

    /// A source whose destination failed before it said whether it took
    /// the slots waits while a replica of that destination may still be
    /// elected in its place, and no longer than that (#743).
    #[test]
    fn a_failed_destination_with_a_replica_may_fail_over_for_a_while() {
        let (cs, a, _) = three_master_cluster();
        let mut w = FailoverWatch::default();
        assert!(!cs.destination_may_fail_over(a, &mut w), "not failed");
        cs.nodes.write().get_mut(&a).unwrap().mark_failed(now_ms());
        assert!(!cs.destination_may_fail_over(a, &mut w), "no replica");
        let r = add_replica(&cs, a);
        assert!(cs.destination_may_fail_over(a, &mut w));
        set_flag(&cs, r, NodeFlags::FAIL);
        assert!(
            !cs.destination_may_fail_over(a, &mut w),
            "its replica failed too"
        );
        cs.nodes
            .write()
            .get_mut(&r)
            .unwrap()
            .flags
            .remove(NodeFlags::FAIL);
        let long_ago = now_ms() - cs.failover_window().as_millis() as u64 - 1;
        cs.nodes.write().get_mut(&a).unwrap().fail_since_ms = long_ago;
        assert!(
            !cs.destination_may_fail_over(a, &mut w),
            "the window is over"
        );
        cs.nodes.write().get_mut(&a).unwrap().fail_since_ms = now_ms();
        assert!(cs.destination_may_fail_over(a, &mut w));
        // Its replica elected: a master, which no longer names `a`. Waited
        // for a moment more, for its claims, then not.
        {
            let mut nodes = cs.nodes.write();
            let n = nodes.get_mut(&r).unwrap();
            n.flags.remove(NodeFlags::REPLICA);
            n.flags |= NodeFlags::MASTER;
            n.master_id = None;
        }
        assert!(
            cs.destination_may_fail_over(a, &mut w),
            "its claims may follow"
        );
        w.promoted_at = Some(std::time::Instant::now() - FAILOVER_WATCH_SETTLE);
        assert!(!cs.destination_may_fail_over(a, &mut w), "failed over");

        // A new, empty destination (a scale-out) owns nothing here, and is
        // waited for all the same while its replica may be elected.
        let (cs, _, _) = three_master_cluster();
        let d = add_master(&cs, 0..=0);
        cs.slots.write()[0] = Some(cs.myself());
        assert!(!cs.owns_any_slot(d));
        let _rd = add_replica(&cs, d);
        cs.nodes.write().get_mut(&d).unwrap().mark_failed(now_ms());
        let mut w = FailoverWatch::default();
        assert!(cs.destination_may_fail_over(d, &mut w), "empty destination");
    }

    /// The claims a candidate sends: its master's slots in its view, as
    /// ranges, at the master's config epoch.
    #[test]
    fn slot_claims_of_a_master_are_its_ranges_at_its_epoch() {
        let (cs, a, _b) = three_master_cluster();
        cs.nodes.write().get_mut(&a).unwrap().config_epoch = 4;
        cs.slots.write()[6000] = None;
        let claims: Vec<(u16, u16, u64)> = cs
            .slot_claims_of(a)
            .iter()
            .map(|c| (c.start, c.end, c.epoch))
            .collect();
        assert_eq!(claims, vec![(5461, 5999, 4), (6001, 10922, 4)]);
    }

    /// A report of `ranges` at `epoch`, from a master that counts `masters`
    /// slot-owning masters.
    fn held_n(
        ranges: &[(u16, u16)],
        epoch: u64,
        masters: usize,
    ) -> Option<crate::cluster::bus::OwnerReport> {
        Some(crate::cluster::bus::OwnerReport {
            masters,
            claims: ranges
                .iter()
                .map(|&(start, end)| crate::cluster::bus::SlotClaim { start, end, epoch })
                .collect(),
        })
    }

    /// A report of `ranges` at `epoch`, from a master that counts three.
    fn held(ranges: &[(u16, u16)], epoch: u64) -> Option<crate::cluster::bus::OwnerReport> {
        held_n(ranges, epoch, 3)
    }

    /// This node, a replica of `m` (slots 0-99, config epoch 3) in a cluster
    /// of three masters, `m`, `x` and `y`. Its own config epoch is 1.
    fn restarted_replica() -> (ClusterState, NodeId, NodeId, NodeId) {
        let cs = ClusterState::new_enabled(None, 15000);
        let m = add_master(&cs, 0..=99);
        let x = add_master(&cs, 100..=199);
        let y = add_master(&cs, 200..=299);
        cs.nodes.write().get_mut(&m).unwrap().config_epoch = 3;
        cs.set_replica_of(m).unwrap();
        cs.nodes.write().get_mut(&cs.myself()).unwrap().config_epoch = 1;
        (cs, m, x, y)
    }

    fn owner(cs: &ClusterState, slot: usize) -> Option<NodeId> {
        cs.slots.read()[slot]
    }

    /// The rule of `promotion_to_complete`: a majority of the masters that
    /// own slots (two of three, the old master counted) hold this node as
    /// the owner of all its master's slots at an epoch newer than its
    /// master's and its own. The greatest such epoch is taken.
    #[test]
    fn a_replica_completes_a_promotion_a_majority_holds_it_to() {
        let (cs, m, x, y) = restarted_replica();
        assert_eq!(cs.promotion_to_complete(false), UnsavedPromotion::None);
        cs.note_held_as_owner(x, held(&[(0, 49), (50, 99)], 7));
        // One master of three.
        assert_eq!(cs.promotion_to_complete(false), UnsavedPromotion::None);
        cs.note_held_as_owner(y, held(&[(0, 99)], 8));
        assert_eq!(
            cs.promotion_to_complete(false),
            UnsavedPromotion::Complete {
                master: m,
                epoch: 8,
                slots: vec![(0, 99)],
            }
        );
        // Not while a full sync is being loaded.
        assert!(matches!(
            cs.promotion_to_complete(true),
            UnsavedPromotion::NotCompleted(_)
        ));
        // A master that holds it as the owner of nothing any more withdraws.
        cs.note_held_as_owner(y, None);
        assert_eq!(cs.promotion_to_complete(false), UnsavedPromotion::None);
    }

    /// Only a master that owns slots, and holds this node as the owner of
    /// every slot of its master, counts.
    #[test]
    fn only_slot_owners_that_hold_all_the_masters_slots_count() {
        let (cs, _m, x, y) = restarted_replica();
        let stranger = add_replica(&cs, x);
        cs.note_held_as_owner(x, held(&[(0, 99)], 7));
        cs.note_held_as_owner(stranger, held(&[(0, 99)], 7));
        assert_eq!(cs.promotion_to_complete(false), UnsavedPromotion::None);
        // Slot 99 is another's in y's view: it has heard of a handover.
        cs.note_held_as_owner(y, held(&[(0, 98)], 7));
        assert_eq!(cs.promotion_to_complete(false), UnsavedPromotion::None);
    }

    /// The quorum is a majority of the greater of the masters that own
    /// slots in this node's view and the most any report counts: a view as
    /// old as nodes.conf may count fewer than the cluster has. The reports
    /// come from that many distinct masters, each a slot owner in this
    /// node's view.
    #[test]
    fn the_quorum_counts_the_masters_the_reports_count() {
        let (cs, m, x, y) = restarted_replica();
        // Two of three in this view; the two count seven: four are needed.
        cs.note_held_as_owner(x, held_n(&[(0, 99)], 7, 7));
        cs.note_held_as_owner(y, held_n(&[(0, 99)], 7, 7));
        let UnsavedPromotion::NotCompleted(why) = cs.promotion_to_complete(false) else {
            panic!("completed with two reports of seven masters");
        };
        assert!(why.contains("report 7 masters"), "{why}");
        // One report counting seven is enough to raise it.
        cs.note_held_as_owner(y, held_n(&[(0, 99)], 7, 3));
        assert!(matches!(
            cs.promotion_to_complete(false),
            UnsavedPromotion::NotCompleted(_)
        ));
        // Four masters own slots here once their claims are in; reports of
        // two more of them make four.
        let z = add_master(&cs, 300..=399);
        let w = add_master(&cs, 400..=499);
        cs.note_held_as_owner(z, held_n(&[(0, 99)], 7, 7));
        assert!(matches!(
            cs.promotion_to_complete(false),
            UnsavedPromotion::NotCompleted(_)
        ));
        cs.note_held_as_owner(w, held_n(&[(0, 99)], 7, 7));
        assert_eq!(
            cs.promotion_to_complete(false),
            UnsavedPromotion::Complete {
                master: m,
                epoch: 7,
                slots: vec![(0, 99)],
            }
        );
        // A report from a node that owns no slots here does not count.
        let (cs, _m, x, _y) = restarted_replica();
        let unknown = generate_node_id();
        cs.note_held_as_owner(x, held(&[(0, 99)], 7));
        cs.note_held_as_owner(unknown, held(&[(0, 99)], 7));
        assert_eq!(cs.promotion_to_complete(false), UnsavedPromotion::None);
    }

    /// The node takes exactly its old master's slots: reports that hold it
    /// as the owner of more (its config epoch was raised for another slot
    /// since) are taken for those, and the other slot stays its owner's in
    /// this node's view.
    #[test]
    fn only_the_old_masters_slots_are_taken() {
        let (cs, m, x, y) = restarted_replica();
        let z = add_master(&cs, 500..=599);
        cs.nodes.write().get_mut(&z).unwrap().config_epoch = 4;
        cs.note_held_as_owner(x, held_n(&[(0, 99), (500, 509)], 9, 4));
        cs.note_held_as_owner(y, held_n(&[(500, 509), (0, 99)], 9, 4));
        cs.note_held_as_owner(z, held_n(&[(0, 99), (500, 509)], 9, 4));
        let UnsavedPromotion::Complete {
            master,
            epoch,
            slots,
        } = cs.promotion_to_complete(false)
        else {
            panic!("not completed");
        };
        assert_eq!((master, epoch, slots.clone()), (m, 9, vec![(0, 99)]));
        let claimed = cs.complete_promotion_self(m, epoch, &slots).unwrap();
        assert_eq!(claimed.len(), 100);
        let me = cs.myself();
        assert!((0..=99).all(|s| owner(&cs, s) == Some(me)));
        assert!((500..=599).all(|s| owner(&cs, s) == Some(z)));
        assert_eq!(cs.my_slot_claims().len(), 1);
    }

    /// Some of the old master's slots went to another shard since this
    /// node's nodes.conf was saved: the masters hold it as the owner of the
    /// rest only. Nothing is taken while this node's view still has them
    /// the old master's; once the new owner's claim is in, the rest is
    /// taken, and the slots that went stay where they went.
    #[test]
    fn slots_the_old_master_gave_away_since_are_not_taken() {
        let (cs, m, x, y) = restarted_replica();
        let z = add_master(&cs, 300..=399);
        cs.note_held_as_owner(x, held_n(&[(0, 49)], 7, 4));
        cs.note_held_as_owner(y, held_n(&[(0, 49)], 7, 4));
        cs.note_held_as_owner(z, held_n(&[(0, 49)], 7, 4));
        assert_eq!(cs.promotion_to_complete(false), UnsavedPromotion::None);
        cs.nodes.write().get_mut(&z).unwrap().config_epoch = 8;
        cs.apply_peer_slot_claims(
            z,
            &[crate::cluster::bus::SlotClaim {
                start: 50,
                end: 99,
                epoch: 8,
            }],
        );
        let decided = cs.promotion_to_complete(false);
        assert_eq!(
            decided,
            UnsavedPromotion::Complete {
                master: m,
                epoch: 7,
                slots: vec![(0, 49)],
            }
        );
        cs.complete_promotion_self(m, 7, &[(0, 49)]).unwrap();
        let me = cs.myself();
        assert!((0..=49).all(|s| owner(&cs, s) == Some(me)));
        assert!((50..=99).all(|s| owner(&cs, s) == Some(z)));
    }

    /// A newer owner's claim taken in between the decision and the taking
    /// (a `CLUSTER FAILOVER TAKEOVER` by another replica, say): nothing is
    /// taken, and this node follows that owner.
    #[test]
    fn a_newer_claim_between_the_decision_and_the_taking_wins() {
        let (cs, m, x, y) = restarted_replica();
        let other = add_replica(&cs, m);
        cs.note_held_as_owner(x, held(&[(0, 99)], 7));
        cs.note_held_as_owner(y, held(&[(0, 99)], 7));
        let UnsavedPromotion::Complete { epoch, slots, .. } = cs.promotion_to_complete(false)
        else {
            panic!("not completed");
        };
        cs.nodes.write().get_mut(&other).unwrap().config_epoch = 9;
        cs.apply_peer_slot_claims(other, &held(&[(0, 99)], 9).unwrap().claims);
        let refused = cs.complete_promotion_self(m, epoch, &slots);
        assert!(refused.is_err(), "{refused:?}");
        assert_eq!(cs.my_master_id(), Some(other));
        assert!((0..=99).all(|s| owner(&cs, s) == Some(other)));
        // Its old master, still the owner of the slots here but no longer
        // followed: refused too.
        let (cs, m, _x, _y) = restarted_replica();
        let other = add_master(&cs, 1000..=1099);
        cs.set_replica_of(other).unwrap();
        assert!(cs.complete_promotion_self(m, 7, &[(0, 99)]).is_err());
        assert!((0..=99).all(|s| owner(&cs, s) == Some(m)));
    }

    /// A promotion completed from a dataset not known to be all the node
    /// had (no AOF, no snapshot from a clean shutdown) is fenced as a master
    /// back without its dataset is, when a replica of it or of its old
    /// master may hold more; with none, it serves what it has; from a
    /// trusted start it is not fenced.
    #[test]
    fn a_promotion_completed_from_an_untrusted_start_is_fenced() {
        // A replica of the old master, which follows this node once it
        // hears of it.
        let (cs, m, _x, _y) = restarted_replica();
        add_replica(&cs, m);
        cs.check_dataset_at_start(false, 5);
        assert!(!cs.lost_dataset(), "a replica at the start: no fence");
        cs.complete_promotion_self(m, 7, &[(0, 99)]).unwrap();
        assert!(cs.lost_dataset());
        cs.check_lost_dataset_fence();
        assert!(cs.lost_dataset(), "the old master's replica is live");
        assert!(cs.replica_has_nothing("10.0.0.2", 7001));
        assert!(!cs.lost_dataset());

        // No replica that could hold more: serving what it has.
        let (cs, m, _x, _y) = restarted_replica();
        cs.check_dataset_at_start(false, 5);
        cs.complete_promotion_self(m, 7, &[(0, 99)]).unwrap();
        assert!(!cs.lost_dataset());

        // A trusted start: not fenced.
        let (cs, m, _x, _y) = restarted_replica();
        add_replica(&cs, m);
        cs.check_dataset_at_start(true, 5);
        cs.complete_promotion_self(m, 7, &[(0, 99)]).unwrap();
        assert!(!cs.lost_dataset());

        // A full sync loaded since an untrusted start: not fenced.
        let (cs, m, _x, _y) = restarted_replica();
        add_replica(&cs, m);
        cs.check_dataset_at_start(false, 5);
        cs.trust_dataset();
        cs.complete_promotion_self(m, 7, &[(0, 99)]).unwrap();
        assert!(!cs.lost_dataset());
    }

    /// The fence is up by the time any of the slots is this node's here:
    /// a thread that reads the slot table under its lock never sees this
    /// node the owner while it is unfenced.
    #[test]
    fn the_fence_is_up_before_the_slots_are_this_nodes() {
        let (cs, m, _x, _y) = restarted_replica();
        add_replica(&cs, m);
        cs.check_dataset_at_start(false, 5);
        let cs = std::sync::Arc::new(cs);
        let me = cs.myself();
        let done = std::sync::Arc::new(AtomicBool::new(false));
        let watcher = {
            let (cs, done) = (std::sync::Arc::clone(&cs), std::sync::Arc::clone(&done));
            std::thread::spawn(move || {
                let mut seen = 0u64;
                while !done.load(Ordering::SeqCst) {
                    let slots = cs.slots.read();
                    if slots[0] == Some(me) {
                        assert!(cs.lost_dataset(), "the owner, unfenced");
                        seen += 1;
                    }
                }
                seen
            })
        };
        std::thread::sleep(std::time::Duration::from_millis(20));
        cs.complete_promotion_self(m, 7, &[(0, 99)]).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
        done.store(true, Ordering::SeqCst);
        assert!(watcher.join().unwrap() > 0);
    }

    /// Only reports of masters that own slots in this node's view raise the
    /// quorum: a member that owns none, with a count of its own, does not
    /// make the quorum unreachable.
    #[test]
    fn only_slot_owners_reports_raise_the_quorum() {
        let (cs, m, x, y) = restarted_replica();
        let stranger = generate_node_id();
        cs.note_held_as_owner(stranger, held_n(&[(0, 99)], 7, 100));
        cs.note_held_as_owner(x, held(&[(0, 99)], 7));
        cs.note_held_as_owner(y, held(&[(0, 99)], 7));
        assert_eq!(
            cs.promotion_to_complete(false),
            UnsavedPromotion::Complete {
                master: m,
                epoch: 7,
                slots: vec![(0, 99)],
            }
        );
        // Nor does it hold the first sync back.
        cs.first_sync.open.store(false, Ordering::Relaxed);
        cs.note_held_as_owner(x, None);
        cs.note_held_as_owner(y, None);
        cs.note_pong_heard(x);
        cs.note_pong_heard(y);
        assert!(cs.first_sync_may_begin());
    }

    /// Coverage is decided on ranges, whatever their order, overlaps and
    /// joins.
    #[test]
    fn reports_cover_ranges_in_any_order() {
        let claims = |ranges: &[(u16, u16)]| held(ranges, 1).unwrap().claims;
        let wanted = [(0, 99), (200, 299)];
        assert!(claims_cover(&claims(&[(200, 299), (0, 99)]), &wanted));
        assert!(claims_cover(
            &claims(&[(50, 99), (0, 60), (200, 250), (251, 299)]),
            &wanted
        ));
        assert!(claims_cover(&claims(&[(0, 16383)]), &wanted));
        assert!(!claims_cover(&claims(&[(0, 98), (200, 299)]), &wanted));
        assert!(!claims_cover(&claims(&[(0, 99), (201, 299)]), &wanted));
        assert!(!claims_cover(&claims(&[(0, 99), (100, 199)]), &wanted));
        assert!(claims_cover(&claims(&[(16383, 16383)]), &[(16383, 16383)]));
    }

    /// A majority that holds this node as the owner at an epoch no newer than
    /// one it knows for the slots (its master's, or its own) is a view from
    /// before: refused, and said why.
    #[test]
    fn an_epoch_no_newer_than_one_known_for_the_slots_is_refused() {
        let (cs, _m, x, y) = restarted_replica();
        cs.note_held_as_owner(x, held(&[(0, 99)], 3));
        cs.note_held_as_owner(y, held(&[(0, 99)], 2));
        let UnsavedPromotion::NotCompleted(why) = cs.promotion_to_complete(false) else {
            panic!("completed at an old epoch");
        };
        assert!(why.contains("newer than the epoch 3"), "{why}");
        // Its own config epoch counts too.
        cs.nodes.write().get_mut(&cs.myself()).unwrap().config_epoch = 9;
        cs.note_held_as_owner(x, held(&[(0, 99)], 8));
        cs.note_held_as_owner(y, held(&[(0, 99)], 8));
        assert!(matches!(
            cs.promotion_to_complete(false),
            UnsavedPromotion::NotCompleted(_)
        ));
    }

    /// Another replica of the same master that took the slots over at a newer
    /// epoch: its claim is taken in, this node follows it, and the masters'
    /// word from before, about the new master's slots at an older epoch than
    /// its, is refused.
    #[test]
    fn a_newer_owner_of_the_masters_slots_is_followed_not_overruled() {
        let (cs, m, x, y) = restarted_replica();
        let other = add_replica(&cs, m);
        cs.note_held_as_owner(x, held(&[(0, 99)], 7));
        cs.note_held_as_owner(y, held(&[(0, 99)], 7));
        cs.nodes.write().get_mut(&other).unwrap().config_epoch = 9;
        cs.apply_peer_slot_claims(other, &held(&[(0, 99)], 9).unwrap().claims);
        assert_eq!(cs.my_master_id(), Some(other));
        assert!(matches!(
            cs.promotion_to_complete(false),
            UnsavedPromotion::NotCompleted(_)
        ));
    }

    /// This node, a replica of the first of `n` masters (100 slots each),
    /// as started from a nodes.conf that has it a replica: its first sync
    /// waits. The masters, its own first.
    fn first_sync_among(n: usize, node_timeout_ms: u64) -> (ClusterState, Vec<NodeId>) {
        let cs = ClusterState::new_enabled(None, node_timeout_ms);
        let masters: Vec<NodeId> = (0..n)
            .map(|i| add_master(&cs, i * 100..=i * 100 + 99))
            .collect();
        cs.set_replica_of(masters[0]).unwrap();
        cs.first_sync.open.store(false, Ordering::Relaxed);
        (cs, masters)
    }

    /// The first sync after a start waits only while the rule can still
    /// fire: it begins once more than `N - Q` of the masters have answered
    /// without a report. With 1 or 2 masters that is the first answer; 3 or
    /// 4, the second; 5, the third; 7, the fourth: whichever master is down,
    /// the others' answers are enough.
    #[test]
    fn a_replica_begins_its_first_sync_once_the_rule_cannot_fire() {
        for (n, needed) in [(1, 1), (2, 1), (3, 2), (4, 2), (5, 3), (7, 4)] {
            assert_eq!(needed, n - (n / 2 + 1) + 1);
            // Its own master down, or another, or none.
            for down in [Some(0), Some(n - 1), None] {
                if n == 1 && down.is_some() {
                    continue;
                }
                let (cs, masters) = first_sync_among(n, 15_000);
                let answering: Vec<NodeId> = masters
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| Some(*i) != down)
                    .map(|(_, id)| *id)
                    .collect();
                for (k, id) in answering.iter().enumerate() {
                    assert_eq!(
                        cs.first_sync_may_begin(),
                        k >= needed,
                        "{n} masters, {down:?} down, {k} answered"
                    );
                    cs.note_pong_heard(*id);
                }
                assert!(cs.first_sync_may_begin(), "{n} masters, {down:?} down");
            }
        }
        // Its only master down: no answer comes; one node timeout.
        let (cs, _) = first_sync_among(1, 30);
        assert!(!cs.first_sync_may_begin());
        std::thread::sleep(std::time::Duration::from_millis(40));
        assert!(cs.first_sync_may_begin());
    }

    /// A master that reports does not count as one that answered without:
    /// with the rule still able to fire, or firing, the first sync waits.
    /// Once begun, it is never held back again.
    #[test]
    fn a_replica_starts_its_first_sync_once_a_quorum_has_answered() {
        let (cs, m, x, y) = restarted_replica();
        // As if started from a nodes.conf that has it a replica.
        cs.first_sync.open.store(false, Ordering::Relaxed);
        cs.note_pong_heard(m);
        cs.note_held_as_owner(x, held(&[(0, 99)], 7));
        cs.note_pong_heard(x);
        assert!(!cs.first_sync_may_begin(), "y may still report");
        cs.note_held_as_owner(y, held(&[(0, 99)], 7));
        cs.note_pong_heard(y);
        assert!(
            !cs.first_sync_may_begin(),
            "began a sync over its promotion"
        );
        cs.note_held_as_owner(y, None);
        assert!(cs.first_sync_may_begin());
        cs.note_held_as_owner(y, held(&[(0, 99)], 7));
        assert!(cs.first_sync_may_begin(), "held back again");
    }

    /// A master, or a node that is not a replica of anyone, has nothing to
    /// complete.
    #[test]
    fn a_master_has_no_promotion_to_complete() {
        let cs = ClusterState::new_enabled(None, 15000);
        let x = add_master(&cs, 100..=199);
        let y = add_master(&cs, 200..=299);
        cs.slots.write()[0] = Some(cs.myself());
        cs.note_held_as_owner(x, held(&[(0, 0)], 7));
        cs.note_held_as_owner(y, held(&[(0, 0)], 7));
        assert_eq!(cs.promotion_to_complete(false), UnsavedPromotion::None);
    }

    #[test]
    fn one_vote_per_epoch_and_never_for_a_stale_epoch() {
        let (cs, a, b) = three_master_cluster();
        let (ra, rb) = (add_replica(&cs, a), add_replica(&cs, b));
        cs.nodes.write().get_mut(&a).unwrap().mark_failed(now_ms());
        cs.nodes.write().get_mut(&b).unwrap().mark_failed(now_ms());

        assert!(cs.cast_vote(ra, a, 5, false, None));
        assert!(
            !cs.cast_vote(rb, b, 5, false, None),
            "already voted in epoch 5"
        );
        assert!(
            !cs.cast_vote(rb, b, 4, false, None),
            "epoch 4 is behind the cluster"
        );
        assert!(cs.cast_vote(rb, b, 6, false, None));
        assert_eq!(cs.current_epoch(), 6);
    }

    /// A vote whose save fails is not sent, and the epoch counts as voted
    /// in: a second candidate in it gets no vote either, its save or not.
    #[test]
    fn a_vote_that_cannot_be_saved_is_not_sent_nor_given_again_in_its_epoch() {
        let (mut cs, a, b) = three_master_cluster();
        let (ra, rb) = (add_replica(&cs, a), add_replica(&cs, b));
        cs.nodes.write().get_mut(&a).unwrap().mark_failed(now_ms());
        cs.nodes.write().get_mut(&b).unwrap().mark_failed(now_ms());
        // A directory that is not there: every save fails.
        cs.config_path = Some(
            std::env::temp_dir()
                .join("kividb-no-such-dir-808")
                .join("nodes.conf"),
        );
        let ask = |sender, master| crate::cluster::bus::BusMessage::FailoverAuthRequest {
            sender_id: sender,
            sender_ip: "127.0.0.1".into(),
            sender_client_port: 7000,
            sender_cluster_port: 17000,
            request_epoch: 5,
            master_id: master,
            force: false,
            claims: None,
            claims_unreadable: false,
        };
        let peer: std::net::SocketAddr = "127.0.0.1:17000".parse().unwrap();
        assert!(crate::cluster::bus::listener::process_message(&cs, ask(ra, a), peer).is_none());
        assert_eq!(cs.last_vote_epoch.load(Ordering::SeqCst), 5);
        cs.config_path = None;
        assert!(crate::cluster::bus::listener::process_message(&cs, ask(rb, b), peer).is_none());
        assert!(!cs.cast_vote(rb, b, 5, false, None));
    }

    /// Two admissions of one node under way at once (a MEET and its retry),
    /// the first failing: it takes only its own entry with it, so the save
    /// the second waits on has the node, and the node is a member once that
    /// save is on disk.
    #[test]
    fn a_failed_admission_leaves_another_of_the_same_node_in_its_save() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("sub");
        let conf = sub.join("nodes.conf");
        let mut cs = ClusterState::new_enabled(None, 15000);
        // Not there yet: saves fail.
        cs.config_path = Some(conf.clone());
        let id = generate_node_id();
        let node = ClusterNode::new(id, "127.0.0.1".into(), 7001, 17001);
        let first = cs.begin_admission(node.clone());
        assert!(persist::save_from_state(&cs).is_err());
        // Begun after the first's save failed: it waits on a later one.
        let second = cs.begin_admission(node);
        std::fs::create_dir(&sub).unwrap();
        assert!(!cs.finish_admission(first, || panic!("a member unsaved")));
        let mut added = false;
        assert!(cs.finish_admission(second, || added = true));
        assert!(added);
        let saved = std::fs::read_to_string(&conf).unwrap();
        assert!(
            saved.lines().any(|l| l.starts_with(node_id_str(&id))),
            "{saved}"
        );
        assert!(cs.joining.lock().is_empty());
    }

    /// Two replicas of one master must not both be elected moments apart.
    #[test]
    fn a_second_replica_of_the_same_master_is_not_voted_for_straight_away() {
        let (cs, a, _b) = three_master_cluster();
        let (r1, r2) = (add_replica(&cs, a), add_replica(&cs, a));
        cs.nodes.write().get_mut(&a).unwrap().mark_failed(now_ms());

        assert!(cs.cast_vote(r1, a, 5, false, None));
        assert!(!cs.cast_vote(r2, a, 6, false, None));
    }

    #[test]
    fn only_slot_owning_masters_vote() {
        let cs = ClusterState::new_enabled(None, 15000); // owns no slots
        let a = add_master(&cs, 0..=16383);
        let replica = add_replica(&cs, a);
        cs.nodes.write().get_mut(&a).unwrap().mark_failed(now_ms());
        assert!(!cs.cast_vote(replica, a, 5, false, None));
    }

    #[test]
    fn a_candidate_must_be_a_replica_of_the_master_it_wants_to_replace() {
        let (cs, a, b) = three_master_cluster();
        let replica_of_b = add_replica(&cs, b);
        cs.nodes.write().get_mut(&a).unwrap().mark_failed(now_ms());
        assert!(!cs.cast_vote(replica_of_b, a, 5, false, None));
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

    /// Bound to `::`, a node that reaches a peer over IPv4 has the
    /// IPv4-mapped form of its address as its end: it learns the IPv4
    /// address, which is what clients are then told (#847).
    #[test]
    fn a_node_on_ipv6_wildcard_learns_its_ipv4_address_not_the_mapped_form() {
        let cs = ClusterState::new_enabled(None, 15000);
        cs.configure_self_address("::", 7000, "", 0, 0);
        let recorded = cs.nodes.read().get(&cs.myself()).unwrap().ip.clone();
        assert_eq!(cs.advertised_ip(&recorded), "");
        cs.learn_self_ip("::ffff:127.0.0.1".parse().unwrap());
        let recorded = cs.nodes.read().get(&cs.myself()).unwrap().ip.clone();
        assert_eq!(cs.advertised_ip(&recorded), "", "loopback teaches nothing");
        cs.learn_self_ip("::ffff:10.1.2.3".parse().unwrap());
        let recorded = cs.nodes.read().get(&cs.myself()).unwrap().ip.clone();
        assert_eq!(recorded, "10.1.2.3");
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
