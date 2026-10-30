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

/// The locks below are `parking_lot` locks: a reader queues behind a waiting
/// writer, so a thread must never take a lock it already holds, read or
/// write, and a guard must not be kept across an `.await`. Where one thread
/// holds several, it takes them in this order and no other: `forgotten`,
/// `save_lock`, `failure_reports`, `nodes`, `slots`, `migrations`, `myself`.
/// A temporary guard (`self.nodes.read().get(..)`) is held to the end of
/// its statement, or of the `match` / `if let` / `for` it heads.
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
    /// [`Self::refresh_state`]; read on the routing path.
    state_ok: AtomicBool,
    /// `cluster-require-full-coverage`.
    require_full_coverage: AtomicBool,
    /// `cluster-replica-validity-factor`: how stale a replica's data may be
    /// for it to stand for election, in node timeouts; 0 = always stand.
    replica_validity_factor: AtomicU64,
    /// True while our own IP is the loopback placeholder used when binding to
    /// a wildcard address with no `cluster-announce-ip`. While set, we send an
    /// empty address so peers record the one they observe instead.
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
            last_vote_epoch: AtomicU64::new(last_vote_epoch),
            election: parking_lot::Mutex::new(None),
            election_pending: AtomicBool::new(false),
            migrations: RwLock::new(AHashMap::new()),
            slot_migrations: migration::Registry::default(),
            forgotten: RwLock::new(AHashMap::new()),
            last_vote_for: parking_lot::Mutex::new(AHashMap::new()),
            hooks: std::sync::OnceLock::new(),
            bus_runtime: std::sync::OnceLock::new(),
            state_ok: AtomicBool::new(false),
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
        for node in self.nodes.write().values_mut() {
            if node.id == me {
                continue;
            }
            node.ping_sent_ms = 0;
            node.pong_received_ms = now;
            node.flags.remove(NodeFlags::PFAIL);
        }
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
    /// connect to. The entry falls back to loopback — right for a
    /// single-machine cluster — and is flagged as a guess, which has two
    /// effects: outgoing bus messages carry an empty address so each peer
    /// records the one it actually sees us on, and the guess is replaced with
    /// the real interface address the first time we reach a peer that is not
    /// on this machine (see [`Self::learn_self_ip`]).
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
            ("127.0.0.1".to_string(), true)
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
    /// loopback guess and this is a real interface address, adopt it.
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

    /// Whether keyed commands may be served right now. Always true with
    /// `cluster-require-full-coverage no`.
    #[inline]
    pub fn is_serving(&self) -> bool {
        !self.enabled
            || self.state_ok.load(Ordering::Relaxed)
            || !self.require_full_coverage.load(Ordering::Relaxed)
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
        self.state_ok.store(ok, Ordering::Relaxed);
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
        Ok(())
    }

    /// Release a slot we own. Errors if the slot isn't currently owned by us.
    pub fn release_slot(&self, slot: u16) -> Result<(), String> {
        if !self.enabled {
            return Err("This instance has cluster support disabled".into());
        }
        if slot as usize >= CLUSTER_SLOTS {
            return Err(format!("Invalid slot {slot}"));
        }
        let mut slots = self.slots.write();
        match slots[slot as usize] {
            Some(owner) if owner == self.myself() => {
                slots[slot as usize] = None;
                Ok(())
            }
            Some(_) => Err(format!("Slot {slot} is owned by another node")),
            None => Err(format!("Slot {slot} is already unassigned")),
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
        Ok((end - start + 1) as usize)
    }

    /// Release a contiguous range. Atomic in the same sense.
    pub fn release_slot_range(&self, start: u16, end: u16) -> Result<usize, String> {
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
        for i in start..=end {
            match slots[i as usize] {
                Some(owner) if owner == self.myself() => {}
                Some(_) => return Err(format!("Slot {i} is owned by another node")),
                None => return Err(format!("Slot {i} is already unassigned")),
            }
        }
        for i in start..=end {
            slots[i as usize] = None;
        }
        Ok((end - start + 1) as usize)
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
            for claim in claims {
                for slot in claim.start..=claim.end {
                    let i = slot as usize;
                    if i >= CLUSTER_SLOTS || slots[i] == Some(owner_id) {
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
        // If we were holding writes for a manual failover, it has happened:
        // release them, to be answered with a redirect to the new master.
        crate::pause::clear_pause();
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
    /// Caller is responsible for updating repl_state.role.
    pub fn promote_self(&self, failed_master_id: NodeId, election_epoch: u64) {
        if !self.enabled {
            return;
        }

        // Phase 1: claim the failed master's slots.
        {
            let mut slots = self.slots.write();
            for s in slots.iter_mut() {
                if matches!(*s, Some(o) if o == failed_master_id) {
                    *s = Some(self.myself());
                }
            }
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
        self.migrations
            .write()
            .insert(slot, MigrationState::Importing { src_id });
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
        self.slots.write()[slot as usize] = Some(new_owner);
        self.migrations.write().remove(&slot);
        Ok(())
    }

    /// Take ownership of `slots` at the end of an atomic migration to this
    /// node. One new epoch covers them all, so the claim wins over the
    /// previous owner's wherever the two meet.
    pub fn take_over_slots(&self, slots: impl Iterator<Item = u16>) -> std::io::Result<()> {
        let t = std::time::Instant::now();
        let mut steps: Vec<(&str, u128)> = Vec::new();
        let me = self.myself();
        let new_epoch = self.bump_epoch();
        steps.push(("epoch", t.elapsed().as_millis()));
        if let Some(node) = self.nodes.write().get_mut(&me)
            && new_epoch > node.config_epoch
        {
            node.config_epoch = new_epoch;
        }
        steps.push(("nodes", t.elapsed().as_millis()));
        {
            let mut owners = self.slots.write();
            let mut migrations = self.migrations.write();
            for slot in slots {
                owners[slot as usize] = Some(me);
                migrations.remove(&slot);
            }
        }
        steps.push(("slots", t.elapsed().as_millis()));
        self.refresh_state();
        steps.push(("refresh", t.elapsed().as_millis()));
        let saved = persist::save_from_state(self);
        steps.push(("save", t.elapsed().as_millis()));
        if t.elapsed() > std::time::Duration::from_millis(100) {
            log_info!("Cluster: slow takeover, ms at each step: {steps:?}");
        }
        saved
    }

    /// Record that `slots` now belong to `new_owner`, at the end of an atomic
    /// migration away from this node.
    pub fn hand_over_slots(&self, slots: impl Iterator<Item = u16>, new_owner: NodeId) {
        {
            let mut owners = self.slots.write();
            let mut migrations = self.migrations.write();
            for slot in slots {
                owners[slot as usize] = Some(new_owner);
                migrations.remove(&slot);
            }
        }
        // The caller saves, once writes are no longer paused.
        self.refresh_state();
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
