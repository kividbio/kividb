use clap::Parser;

/// The values `tls-auth-clients` accepts.
pub const TLS_AUTH_CLIENTS: [&str; 3] = ["yes", "no", "optional"];

#[derive(clap::ValueEnum, Debug, Clone, Default, PartialEq)]
pub enum IoModel {
    #[default]
    Auto, // io_uring if available, tokio fallback
    Uring, // force io_uring, fail if unavailable
    Tokio, // always use tokio
}

#[derive(Parser, Debug, Clone)]
#[command(
    name = "kividb",
    about = "Fast Redis-compatible store",
    version = env!("CARGO_PKG_VERSION"),
    author = env!("CARGO_PKG_AUTHORS")
)]
pub struct Config {
    /// Port to listen
    #[arg(long, default_value_t = 6380)]
    pub port: u16,

    /// Address to listen
    #[arg(long, default_value = "0.0.0.0")]
    pub bind: String,

    /// While the default user has no password, accept clients from this
    /// machine only (Redis `protected-mode`). See `src/protected_mode.rs`.
    #[arg(long, default_value = "yes", value_parser = parse_yes_no, num_args = 1)]
    pub protected_mode: bool,

    /// Whether `bind` was set (on the command line or in the configuration
    /// file). Protected mode applies only while it was not, as in Redis
    /// before 7: listening on every address by choice is a choice.
    #[arg(skip)]
    pub bind_explicit: bool,

    /// Number of worker threads (0 = auto)
    #[arg(long, default_value_t = 0)]
    pub threads: usize,

    /// Enable append-only file persistence
    #[arg(long, default_value_t = false)]
    pub aof: bool,

    /// Start a rewritten AOF with a KDB image of the dataset, so recovery loads
    /// the bulk at snapshot speed and replays only what came after it.
    /// Disable to emit the plain command log older builds produce.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub aof_preamble: bool,

    /// Run scripts whose KEYS share a hash tag beside each other, each under
    /// its tag's lock, instead of one at a time for the whole server. A
    /// script run that way may not touch a key outside the tag. On unless
    /// switched off; it means something only in a build with Lua. See
    /// docs/SCRIPTS.md.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub lua_parallel_scripts: bool,

    /// Rewrite the AOF once it has grown this much past its post-rewrite size,
    /// as a percentage (0 = never rewrite automatically).
    #[arg(long, default_value_t = 100)]
    pub aof_rewrite_percentage: u64,

    /// Never rewrite automatically below this size, in bytes. Keeps a small
    /// log from rewriting constantly just because it doubled.
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    pub aof_rewrite_min_size: u64,

    /// Number of databases
    #[arg(long, default_value_t = 16)]
    pub databases: usize,

    /// Max memory (0 = unlimited): bytes, or with a unit as in Redis (`3gb`,
    /// `512mb`). Once the process uses more, writes are refused with an OOM
    /// error.
    #[arg(long, default_value_t = 0, value_parser = parse_memory_flag)]
    pub maxmemory: usize,

    /// Idle time before a private client query buffer is released. Hidden so
    /// integration tests can shorten the maintenance interval deterministically.
    #[arg(long, default_value_t = 2000, hide = true)]
    pub query_buffer_idle_resize_after_ms: u64,

    /// What to do when `maxmemory` is reached (Redis `maxmemory-policy`):
    ///   noeviction — refuse the write
    ///   allkeys-lru | allkeys-lfu | allkeys-lrm | allkeys-random — delete
    ///            keys to make room: the least recently used, least
    ///            frequently used, least recently modified, or any
    ///   volatile-lru | volatile-lfu | volatile-lrm | volatile-random |
    ///   volatile-ttl — the same, among keys with a TTL only; or the one
    ///            nearest to expiring
    #[arg(long, default_value = "noeviction")]
    pub maxmemory_policy: String,

    /// How many keys are looked at to choose the one to evict (Redis
    /// `maxmemory-samples`).
    #[arg(long, default_value_t = 5)]
    pub maxmemory_samples: usize,

    /// How slowly a key's use count grows, for the LFU policies (Redis
    /// `lfu-log-factor`).
    #[arg(long, default_value_t = 10)]
    pub lfu_log_factor: u32,

    /// Record every key's use whatever the eviction policy, so that `OBJECT
    /// IDLETIME` and `OBJECT FREQ` report it under `noeviction` too. Costs
    /// every command and every key: see `eviction::key_access_tracking`.
    #[arg(long, default_value = "no", value_parser = parse_yes_no, num_args = 1)]
    pub key_access_tracking: bool,

    /// Minutes for a key's use count to fall by one, for the LFU policies
    /// (Redis `lfu-decay-time`). 0 = it never falls.
    #[arg(long, default_value_t = 1)]
    pub lfu_decay_time: u32,

    /// Working directory: where `dump.kdb`, `appendonly.aof`, backups and
    /// `nodes.conf` live (Redis `dir`). Empty = the directory the server was
    /// started in.
    #[arg(long, default_value = "")]
    pub dir: String,

    /// Log level: debug, verbose, notice (or info), warning, nothing.
    /// docs/LOGGING.md says what each shows.
    #[arg(long, default_value = "info")]
    pub loglevel: String,

    /// Write the log to this file, appending, instead of standard error.
    #[arg(long, default_value = "")]
    pub logfile: String,

    /// Log line format: `text`, or `json` — one object per line, for log
    /// pipelines.
    #[arg(long, default_value = "text", value_parser = ["text", "json"])]
    pub log_format: String,

    /// I/O model: auto, uring, tokio
    #[arg(long, default_value = "auto", value_enum)]
    pub io_model: IoModel,

    /// Replicate from master: "host port" (e.g. "127.0.0.1 6379")
    /// Pass as two args: --replicaof 127.0.0.1 6379
    #[arg(long, num_args = 2, value_names = ["HOST", "PORT"])]
    pub replicaof: Option<Vec<String>>,

    /// This replica's announced port (for REPLCONF listening-port)
    /// Defaults to the server's own port.
    #[arg(long, default_value_t = 0)]
    pub replica_announced_port: u16,

    /// Password required for AUTH (empty = no authentication required)
    #[arg(long, default_value = "")]
    pub requirepass: String,

    /// Path to ACL file (empty = no ACL file, use inline SETUSER rules only)
    #[arg(long, default_value = "")]
    pub aclfile: String,

    /// Slowlog threshold in microseconds (0 = log everything)
    #[arg(long, default_value_t = u64::MAX)]
    pub slowlog_log_slower_than: u64,

    /// Maximum number of slowlog entries to keep
    #[arg(long, default_value_t = 128)]
    pub slowlog_max_len: usize,

    /// True iff the server should run as a cluster node.
    /// Activated by `--cluster-enabled yes`. Defaults to false (standalone).
    #[arg(long, default_value = "no", value_parser = parse_yes_no, num_args=1)]
    pub cluster_enabled: bool,

    /// Milliseconds without a PONG before a peer is marked PFAIL.
    /// Default 15000 matches Redis. Lower values detect failures faster
    /// but increase false positives under network jitter.
    #[arg(long, default_value_t = 15000)]
    pub cluster_node_timeout: u64,

    /// Path to a config file to load settings from.
    /// Equivalent to passing redis-server /etc/redis.conf.
    #[arg(long, default_value = "/etc/kividb/kividb.conf")]
    pub configfile: String,

    /// Port to accept TLS connections on. 0 = TLS disabled.
    /// Separate from `port` so the uring hot-path on `port` is untouched.
    #[arg(long, default_value_t = 0)]
    pub tls_port: u16,

    /// Path to the TLS certificate file (PEM format).
    #[arg(long, default_value_t = String::new())]
    pub tls_cert_file: String,

    /// Path to the TLS private key file (PEM format).
    #[arg(long, default_value_t = String::new())]
    pub tls_key_file: String,

    /// Path to the CA certificate file (PEM) that client certificates are
    /// verified against; see `tls-auth-clients`. Leave empty to not ask
    /// clients for a certificate.
    #[arg(long, default_value_t = String::new())]
    pub tls_ca_cert_file: String,

    /// Whether TLS clients must present a certificate signed by
    /// `tls-ca-cert-file` (Redis `tls-auth-clients`):
    ///   yes      — required; a client without a valid one is refused
    ///   optional — not required, but one that is presented must be valid
    ///   no       — not asked for
    /// Has no effect while `tls-ca-cert-file` is empty.
    #[arg(long, default_value = "yes")]
    pub tls_auth_clients: String,

    /// The cluster bus over TLS, and the TLS port announced as this node's
    /// port, so that what nodes open to each other is TLS (Redis
    /// `tls-cluster`). Needs `tls-port`, the certificate, key and CA files.
    #[arg(long, default_value = "no", value_parser = parse_yes_no, num_args = 1)]
    pub tls_cluster: bool,

    /// A replica connects to its master over TLS (Redis `tls-replication`).
    /// Implied by `tls-cluster` in a cluster.
    #[arg(long, default_value = "no", value_parser = parse_yes_no, num_args = 1)]
    pub tls_replication: bool,

    /// Certificate this node presents when it connects to another node
    /// (Redis `tls-client-cert-file`); `tls-cert-file` when empty.
    #[arg(long, default_value = "")]
    pub tls_client_cert_file: String,

    /// Key for `tls-client-cert-file`.
    #[arg(long, default_value = "")]
    pub tls_client_key_file: String,

    /// Whether to ask GitHub at startup for the latest release, and say so
    /// when it is newer than this build (see docs/UPDATE_CHECK.md).
    /// `--update-check no` turns it off, as does KIVIDB_NO_UPDATE_CHECK=1.
    #[arg(long, default_value = "yes", value_parser = parse_yes_no, num_args = 1)]
    pub update_check: bool,

    /// Where the update check asks: a releases/latest page that redirects to
    /// the newest release's tag (https:// only).
    #[arg(long, default_value = crate::update_check::DEFAULT_URL)]
    pub update_check_url: String,

    /// Keyspace notification events to publish (empty = disabled).
    #[arg(long, default_value = "")]
    pub notify_keyspace_events: String,

    /// Max bytes a master will queue for a single replica that isn't keeping up
    /// (analogous to Redis's `client-output-buffer-limit slave <hard-limit>`).
    /// Once exceeded the replica is disconnected — it must full-resync to catch
    /// back up — rather than letting the queue grow without bound.
    #[arg(long, default_value_t = 268_435_456)]
    pub repl_replica_max_backlog_bytes: u64,

    /// Bytes of the write stream a master keeps for replicas that reconnect
    /// (Redis `repl-backlog-size`). A replica that was away for less than
    /// this much writing is brought up to date without a full resync.
    #[arg(long, default_value_t = 1024 * 1024)]
    pub repl_backlog_size: u64,

    /// Seconds a master with no replicas keeps that backlog before releasing
    /// it (Redis `repl-backlog-ttl`). 0 = never release it.
    #[arg(long, default_value_t = 3600)]
    pub repl_backlog_ttl: u64,

    /// How a replica loads a full resync (Redis `repl-diskless-load`):
    ///   disabled | flushdb | on-empty-db — drop the local dataset first, then
    ///            load: memory for one dataset
    ///   swapdb — keep serving the local dataset until the new one is loaded,
    ///            then swap: memory for two
    #[arg(long, default_value = "disabled")]
    pub repl_diskless_load: String,

    /// Password this node presents when connecting to its master as a
    /// replica (Redis `masterauth`). Required when the master has
    /// `requirepass` or ACLs; without it the handshake is refused.
    #[arg(long, default_value = "")]
    pub masterauth: String,

    /// ACL user to authenticate as on the master (Redis `masteruser`).
    /// Empty = the `default` user.
    #[arg(long, default_value = "")]
    pub masteruser: String,

    /// Seconds without a byte from the master before a replica declares the
    /// link dead and reconnects (Redis `repl-timeout`). Also bounds each step
    /// of the handshake. This is what notices a master that was powered off or
    /// partitioned away: such a peer never refuses or resets the connection,
    /// so without a deadline the replica waits on the kernel's own TCP
    /// retransmission limit (~15 minutes) while reporting a healthy link.
    #[arg(long, default_value_t = 60)]
    pub repl_timeout: u64,

    /// Refuse writes while fewer than this many replicas are good: online,
    /// and heard from within `min-replicas-max-lag` seconds (Redis
    /// `min-replicas-to-write`). 0 is off. A master cut off from its
    /// replicas then refuses writes with NOREPLICAS instead of accepting
    /// ones that are lost when the partition heals.
    #[arg(long, default_value_t = 0)]
    pub min_replicas_to_write: u32,

    /// How recently, in seconds, a replica must have acknowledged to count
    /// as good for `min-replicas-to-write` (Redis `min-replicas-max-lag`).
    /// 0 turns `min-replicas-to-write` off.
    #[arg(long, default_value_t = 10)]
    pub min_replicas_max_lag: u64,

    /// Seconds between the PINGs a master writes into the replication stream
    /// (Redis `repl-ping-replica-period`). They give an idle link traffic, so
    /// a replica can tell "nothing to replicate" apart from "master is gone".
    #[arg(long, default_value_t = 10)]
    pub repl_ping_replica_period: u64,

    /// What a replica does when a full resync would replace its dataset with a
    /// smaller one:
    ///   off    — always accept (Redis behaviour)
    ///   empty  — refuse a master that has no keys while we hold some
    ///   shrink — additionally refuse a master holding fewer than
    ///            `replica-full-sync-guard-ratio` percent of our key count
    /// A refused sync keeps the local dataset, leaves the link down, and is
    /// retried; the operator overrides with `CONFIG SET replica-full-sync-guard off`.
    #[arg(long, default_value = "empty")]
    pub replica_full_sync_guard: String,

    /// Percentage used by `replica-full-sync-guard shrink`.
    #[arg(long, default_value_t = 50)]
    pub replica_full_sync_guard_ratio: u64,

    /// Address this node tells the rest of the cluster to reach it on (Redis
    /// `cluster-announce-ip`). Empty = let peers use the address they observe.
    #[arg(long, default_value = "")]
    pub cluster_announce_ip: String,

    /// Hostname this node announces to the cluster (Redis
    /// `cluster-announce-hostname`): letters, digits, `-` and `.`. Shown in
    /// CLUSTER NODES, SLOTS and SHARDS, and given to clients in place of the
    /// address when `cluster-preferred-endpoint-type` is `hostname`.
    #[arg(long, default_value = "")]
    pub cluster_announce_hostname: String,

    /// What clients are told to connect to, in MOVED, ASK and the topology
    /// replies: `ip`, `hostname` or `unknown-endpoint` (Redis
    /// `cluster-preferred-endpoint-type`).
    #[arg(long, default_value = "ip")]
    pub cluster_preferred_endpoint_type: String,

    /// Client port announced to the cluster. 0 = `port`.
    #[arg(long, default_value_t = 0)]
    pub cluster_announce_port: u16,

    /// Bus port announced to the cluster. 0 = `port` + 10000.
    #[arg(long, default_value_t = 0)]
    pub cluster_announce_bus_port: u16,

    /// A secret every node of the cluster shares. A node connecting to
    /// another's bus port must prove it knows it (a challenge and an
    /// HMAC-SHA256 answer; the secret itself is never sent) before anything
    /// it says is read. Empty: the bus is open to anything that can reach
    /// it. Set it on every node or none.
    #[arg(long, default_value = "")]
    pub cluster_bus_secret: String,

    /// Refuse to run as a cluster node while the bus port is unauthenticated
    /// (Redis `cluster-bus-port-protected-mode`): with this `yes`, a cluster
    /// node needs `cluster-bus-secret`. The default `no` starts without one
    /// and logs a warning.
    #[arg(long, default_value = "no", value_parser = parse_yes_no, num_args = 1)]
    pub cluster_bus_port_protected_mode: bool,

    /// Where cluster topology is persisted (Redis `cluster-config-file`).
    #[arg(long, default_value = "nodes.conf")]
    pub cluster_config_file: String,

    /// Stop serving keyed commands while any hash slot is unassigned or owned
    /// by a failed node (Redis `cluster-require-full-coverage`).
    #[arg(long, default_value = "yes", value_parser = parse_yes_no, num_args = 1)]
    pub cluster_require_full_coverage: bool,

    /// Arguments kept per slowlog entry (Redis 8.8 `slowlog-entry-max-argc`).
    #[arg(long, default_value_t = 32)]
    pub slowlog_entry_max_argc: usize,

    /// Bytes kept per argument in a slowlog entry (Redis 8.8
    /// `slowlog-entry-max-string-len`).
    #[arg(long, default_value_t = 128)]
    pub slowlog_entry_max_string_len: usize,

    /// Default seconds an `XADD ... IDMP` producer/id pair is remembered
    /// (Redis 8.6 `stream-idmp-duration`).
    #[arg(long, default_value_t = 100)]
    pub stream_idmp_duration: u64,

    /// Default idempotency ids remembered per producer (Redis 8.6
    /// `stream-idmp-maxsize`).
    #[arg(long, default_value_t = 100)]
    pub stream_idmp_maxsize: u64,

    /// Directory `BACKUP START` writes a backup into, under the working
    /// directory (Redis 8.10 `backupdirname`). Startup-only.
    #[arg(long, default_value = "backupdir")]
    pub backupdirname: String,

    /// Seconds a sealed backup is kept before it is cleaned up automatically;
    /// `0` keeps it until `BACKUP CLEANUP` (Redis 8.10 `backup-sealed-ttl`).
    #[arg(long, default_value_t = 0)]
    pub backup_sealed_ttl: u64,

    /// Load this file at startup instead of the usual `dump.kdb` and
    /// `appendonly.aof`: `aof:<manifest>` restores a sealed backup,
    /// `rdb:<file>` a single snapshot (Redis 8.10 `preload-file`).
    /// Startup-only; startup fails if the target cannot be loaded.
    #[arg(long, default_value = "")]
    pub preload_file: String,

    /// Atomic slot migration: once the source has less than this many bytes
    /// of the write stream left to send, it pauses writes and hands the slots
    /// over (Redis 8.4 `cluster-slot-migration-handoff-max-lag-bytes`).
    #[arg(long, default_value_t = 1024 * 1024)]
    pub cluster_slot_migration_handoff_max_lag_bytes: u64,

    /// Atomic slot migration: the longest the source keeps writes paused for
    /// a handoff, in milliseconds, before giving the migration up (Redis 8.4
    /// `cluster-slot-migration-write-pause-timeout`).
    #[arg(long, default_value_t = 10_000)]
    pub cluster_slot_migration_write_pause_timeout: u64,
}

impl Default for Config {
    /// Mirrors `#[arg(default_values)]` — `derive(Default)` would zero numeric fields
    /// (`databases = 0`), breaking integration tests and emptying DB/expiry stores.
    fn default() -> Self {
        Self {
            port: 6380,
            bind: "0.0.0.0".into(),
            threads: 0,
            aof: false,
            aof_preamble: true,
            lua_parallel_scripts: true,
            aof_rewrite_percentage: 100,
            aof_rewrite_min_size: 64 * 1024 * 1024,
            databases: 16,
            maxmemory: 0,
            query_buffer_idle_resize_after_ms: 2000,
            maxmemory_policy: "noeviction".into(),
            maxmemory_samples: 5,
            lfu_log_factor: 10,
            key_access_tracking: false,
            lfu_decay_time: 1,
            dir: String::new(),
            loglevel: "info".into(),
            logfile: String::new(),
            log_format: "text".into(),
            io_model: IoModel::default(),
            replicaof: None,
            replica_announced_port: 0,
            requirepass: "".into(),
            aclfile: "".into(),
            slowlog_log_slower_than: u64::MAX,
            slowlog_max_len: 128,
            cluster_enabled: false,
            cluster_node_timeout: 15000,
            configfile: "/etc/kividb/kividb.conf".into(),
            tls_port: 0,
            tls_cert_file: "".into(),
            tls_key_file: "".into(),
            tls_ca_cert_file: "".into(),
            tls_auth_clients: "yes".into(),
            tls_cluster: false,
            tls_replication: false,
            tls_client_cert_file: String::new(),
            tls_client_key_file: String::new(),
            update_check: true,
            update_check_url: crate::update_check::DEFAULT_URL.into(),
            notify_keyspace_events: String::new(),
            repl_replica_max_backlog_bytes: 268_435_456,
            repl_backlog_size: 1024 * 1024,
            repl_backlog_ttl: 3600,
            repl_diskless_load: "disabled".into(),
            masterauth: String::new(),
            masteruser: String::new(),
            repl_timeout: 60,
            min_replicas_to_write: 0,
            min_replicas_max_lag: 10,
            repl_ping_replica_period: 10,
            replica_full_sync_guard: "empty".into(),
            replica_full_sync_guard_ratio: 50,
            cluster_announce_ip: String::new(),
            cluster_announce_hostname: String::new(),
            cluster_preferred_endpoint_type: "ip".into(),
            cluster_announce_port: 0,
            cluster_announce_bus_port: 0,
            cluster_bus_port_protected_mode: false,
            protected_mode: true,
            bind_explicit: false,
            cluster_bus_secret: String::new(),
            cluster_config_file: "nodes.conf".into(),
            cluster_require_full_coverage: true,
            slowlog_entry_max_argc: 32,
            slowlog_entry_max_string_len: 128,
            stream_idmp_duration: 100,
            stream_idmp_maxsize: 100,
            backupdirname: "backupdir".into(),
            backup_sealed_ttl: 0,
            preload_file: String::new(),
            cluster_slot_migration_handoff_max_lag_bytes: 1024 * 1024,
            cluster_slot_migration_write_pause_timeout: 10_000,
        }
    }
}

impl Config {
    pub fn resolve_threads(&self) -> usize {
        if self.threads > 0 {
            return self.threads;
        }
        std::thread::available_parallelism()
            .map(|n| if n.get() < 4 { n.get() } else { n.get() - 2 })
            .unwrap_or(12)
    }

    pub fn addr(&self) -> String {
        format!("{}:{}", self.bind, self.port)
    }

    /// Returns (host, port) if this instance should run as a replica.
    pub fn replicaof_addr(&self) -> Option<(String, u16)> {
        self.replicaof.as_ref().and_then(|parts| {
            if parts.len() == 2 {
                parts[1]
                    .parse::<u16>()
                    .ok()
                    .map(|port| (parts[0].clone(), port))
            } else {
                None
            }
        })
    }

    /// Returns a minimal Config suitable for use inside the replica
    /// command applier — only the fields that execute_command reads.
    pub fn default_replica() -> Self {
        Self {
            port: 0,
            bind: String::new(),
            threads: 1,
            aof: false,
            aof_preamble: true,
            lua_parallel_scripts: true,
            aof_rewrite_percentage: 100,
            aof_rewrite_min_size: 64 * 1024 * 1024,
            databases: 16,
            maxmemory: 0,
            query_buffer_idle_resize_after_ms: 2000,
            maxmemory_policy: "noeviction".into(),
            maxmemory_samples: 5,
            lfu_log_factor: 10,
            key_access_tracking: false,
            lfu_decay_time: 1,
            dir: String::new(),
            loglevel: "warn".into(),
            logfile: String::new(),
            log_format: "text".into(),
            io_model: IoModel::Tokio,
            replicaof: None,
            replica_announced_port: 0,
            requirepass: "".to_string(),
            aclfile: "".to_string(),
            slowlog_log_slower_than: 10_000,
            slowlog_max_len: 128,
            cluster_enabled: false,
            cluster_node_timeout: 15000,
            configfile: "./kividb.conf".to_string(),
            tls_port: 0,
            tls_cert_file: String::new(),
            tls_key_file: String::new(),
            tls_ca_cert_file: String::new(),
            tls_auth_clients: "yes".into(),
            tls_cluster: false,
            tls_replication: false,
            tls_client_cert_file: String::new(),
            tls_client_key_file: String::new(),
            update_check: false,
            update_check_url: crate::update_check::DEFAULT_URL.into(),
            notify_keyspace_events: String::new(),
            repl_replica_max_backlog_bytes: 268_435_456,
            repl_backlog_size: 1024 * 1024,
            repl_backlog_ttl: 3600,
            repl_diskless_load: "disabled".into(),
            masterauth: String::new(),
            masteruser: String::new(),
            repl_timeout: 60,
            min_replicas_to_write: 0,
            min_replicas_max_lag: 10,
            repl_ping_replica_period: 10,
            replica_full_sync_guard: "empty".into(),
            replica_full_sync_guard_ratio: 50,
            cluster_announce_ip: String::new(),
            cluster_announce_hostname: String::new(),
            cluster_preferred_endpoint_type: "ip".into(),
            cluster_announce_port: 0,
            cluster_announce_bus_port: 0,
            cluster_bus_port_protected_mode: false,
            protected_mode: true,
            bind_explicit: false,
            cluster_bus_secret: String::new(),
            cluster_config_file: "nodes.conf".into(),
            cluster_require_full_coverage: true,
            slowlog_entry_max_argc: 32,
            slowlog_entry_max_string_len: 128,
            stream_idmp_duration: 100,
            stream_idmp_maxsize: 100,
            backupdirname: "backupdir".into(),
            backup_sealed_ttl: 0,
            preload_file: String::new(),
            cluster_slot_migration_handoff_max_lag_bytes: 1024 * 1024,
            cluster_slot_migration_write_pause_timeout: 10_000,
        }
    }

    /// Reject configurations that would start a server doing something other
    /// than what was asked for. `Err` is the message to print before exiting;
    /// `Ok` carries warnings to log.
    ///
    /// `tls_compiled` is whether this binary was built with the `tls` feature;
    /// it is a parameter so both outcomes are testable from one build.
    pub fn validate(&self, tls_compiled: bool) -> Result<Vec<String>, String> {
        let mut warnings = Vec::new();

        if !self.update_check_url.starts_with("https://") {
            return Err(format!(
                "--update-check-url must be an https:// URL, not '{}'",
                self.update_check_url
            ));
        }

        // A build without the `tls` feature has no TLS listener at all. It used
        // to accept these flags and ignore them, so a node configured for TLS
        // came up serving plaintext with nothing in its log. `--tls-port` is
        // the flag that asks for a listener, so that one is fatal; a cert or
        // key on its own configures nothing and only earns a warning.
        if !tls_compiled {
            if self.tls_port > 0 {
                return Err(format!(
                    "--tls-port {} was given, but this binary was built without TLS support \
                     (the `tls` feature). It would serve plaintext only. Use a `-tls` build, \
                     or remove --tls-port to run without TLS.",
                    self.tls_port
                ));
            }
            if !self.tls_cert_file.is_empty()
                || !self.tls_key_file.is_empty()
                || !self.tls_ca_cert_file.is_empty()
            {
                warnings.push(
                    "TLS certificate/key options were given, but this binary was built \
                     without TLS support (the `tls` feature) — they are ignored"
                        .to_string(),
                );
            }
        } else if self.tls_port > 0 && !self.tls_enabled() {
            return Err(format!(
                "--tls-port {} requires both --tls-cert-file and --tls-key-file",
                self.tls_port
            ));
        }

        if !TLS_AUTH_CLIENTS.contains(&self.tls_auth_clients.as_str()) {
            return Err(format!(
                "invalid tls-auth-clients '{}' (expected yes, no or optional)",
                self.tls_auth_clients
            ));
        }

        if self.tls_cluster || self.tls_replication {
            if !tls_compiled {
                return Err(
                    "tls-cluster and tls-replication need a build with TLS (the \
                            `tls` feature)"
                        .into(),
                );
            }
            if self.tls_cert_file.is_empty()
                || self.tls_key_file.is_empty()
                || self.tls_ca_cert_file.is_empty()
            {
                return Err("tls-cluster and tls-replication need tls-cert-file, \
                            tls-key-file and tls-ca-cert-file: nodes present their \
                            certificates to each other and check them against the CA"
                    .into());
            }
            if self.tls_client_cert_file.is_empty() != self.tls_client_key_file.is_empty() {
                return Err("tls-client-cert-file and tls-client-key-file go together".into());
            }
        }
        if self.tls_cluster && (!self.cluster_enabled || self.tls_port == 0) {
            return Err("tls-cluster needs cluster-enabled yes and a tls-port".into());
        }

        match self.replica_full_sync_guard.as_str() {
            "off" | "empty" | "shrink" => {}
            other => {
                return Err(format!(
                    "invalid replica-full-sync-guard '{other}' (expected off, empty or shrink)"
                ));
            }
        }
        if self.replica_full_sync_guard_ratio > 100 {
            return Err("replica-full-sync-guard-ratio must be between 0 and 100".into());
        }
        if self.repl_timeout == 0 {
            return Err("repl-timeout must be at least 1 second".into());
        }
        if crate::eviction::Policy::parse(&self.maxmemory_policy).is_none() {
            return Err(format!(
                "invalid maxmemory-policy '{}' (expected one of: {})",
                self.maxmemory_policy,
                crate::eviction::POLICY_NAMES
            ));
        }
        if !(1..=64).contains(&self.maxmemory_samples) {
            return Err("maxmemory-samples must be between 1 and 64".into());
        }
        if crate::replication::flush_before_load(&self.repl_diskless_load).is_none() {
            return Err(format!(
                "invalid repl-diskless-load '{}' (expected disabled, on-empty-db, swapdb or \
                 flushdb)",
                self.repl_diskless_load
            ));
        }

        if self.cluster_enabled {
            if self.replicaof.is_some() {
                return Err(
                    "replicaof directive not allowed in cluster mode — use CLUSTER REPLICATE"
                        .into(),
                );
            }
            if self.port > u16::MAX - 10000 {
                return Err(format!(
                    "port {} leaves no room for the cluster bus port (port + 10000)",
                    self.port
                ));
            }
            if self.cluster_bus_secret.is_empty() {
                if self.cluster_bus_port_protected_mode {
                    return Err(
                        "cluster-bus-port-protected-mode is yes, but the cluster bus is not \
                         authenticated: set cluster-bus-secret, the same on every node. \
                         Refusing to start a cluster node with an open bus port."
                            .into(),
                    );
                }
                warnings.push(format!(
                    "cluster bus port {} is not authenticated: any host that can reach it can \
                     join this cluster. Set cluster-bus-secret, or restrict the port to cluster \
                     members at the network layer.",
                    self.cluster_bus_port()
                ));
            }
        }
        Ok(warnings)
    }

    /// The port the cluster bus listens on.
    pub fn cluster_bus_port(&self) -> u16 {
        self.port.saturating_add(10000)
    }

    /// Returns true if TLS should be enabled.
    /// Requires tls_port > 0 AND both cert + key files configured.
    pub fn tls_enabled(&self) -> bool {
        self.tls_port > 0 && !self.tls_cert_file.is_empty() && !self.tls_key_file.is_empty()
    }

    /// This configuration with `values` applied over it: CONFIG GET's names
    /// and values, which are what is in effect now. CONFIG REWRITE writes
    /// that; it used to write the configuration the server started with,
    /// and lose whatever CONFIG SET had changed since. A value that does not
    /// read back as a directive leaves its setting as it was.
    pub fn with_values(&self, values: &[(String, String)]) -> Config {
        let mut config = self.clone();
        for (name, value) in values {
            if value == "**redacted**" {
                continue;
            }
            let args: Vec<String> = value.split_whitespace().map(String::from).collect();
            if args.is_empty() {
                continue;
            }
            let _ = config.apply_directive(&name.to_ascii_lowercase(), &args, &|_| false);
        }
        config
    }

    /// Write the current runtime config to `config_path`.
    ///
    /// Emits one `key value` pair per line in a format KiviDB can parse
    /// with `--key value` on restart. Comments are not preserved (that
    /// is a known limitation documented in COMMANDS.md).
    ///
    /// Returns the Redis-compatible error string if no path was set,
    /// or an io::Error if the write fails.
    pub fn rewrite_to_disk(&self) -> Result<(), RewriteError> {
        let path = std::path::Path::new(&self.configfile);

        // Create parent directories if they don't exist.
        // On a fresh install /etc/kividb/ won't exist yet.
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(RewriteError::Io)?;
        }

        let mut out = String::new();
        out.push_str(&format!("port {}\n", self.port));
        out.push_str(&format!("bind {}\n", self.bind));
        out.push_str(&format!(
            "protected-mode {}\n",
            if self.protected_mode { "yes" } else { "no" }
        ));
        out.push_str(&format!("threads {}\n", self.threads));
        out.push_str(&format!("aof {}\n", if self.aof { "yes" } else { "no" }));
        out.push_str(&format!(
            "aof-preamble {}\n",
            if self.aof_preamble { "yes" } else { "no" }
        ));
        out.push_str(&format!(
            "aof-rewrite-percentage {}\n",
            self.aof_rewrite_percentage
        ));
        out.push_str(&format!(
            "lua-parallel-scripts {}\n",
            if self.lua_parallel_scripts {
                "yes"
            } else {
                "no"
            }
        ));
        out.push_str(&format!(
            "aof-rewrite-min-size {}\n",
            self.aof_rewrite_min_size
        ));
        out.push_str(&format!("databases {}\n", self.databases));
        out.push_str(&format!("maxmemory {}\n", self.maxmemory));
        out.push_str(&format!("maxmemory-policy {}\n", self.maxmemory_policy));
        out.push_str(&format!("maxmemory-samples {}\n", self.maxmemory_samples));
        out.push_str(&format!("lfu-log-factor {}\n", self.lfu_log_factor));
        out.push_str(&format!("lfu-decay-time {}\n", self.lfu_decay_time));
        out.push_str(&format!(
            "key-access-tracking {}\n",
            if self.key_access_tracking {
                "yes"
            } else {
                "no"
            }
        ));
        out.push_str(&format!("loglevel {}\n", self.loglevel));
        if !self.logfile.is_empty() {
            out.push_str(&format!("logfile {}\n", quote(&self.logfile)));
        }
        if self.log_format != "text" {
            out.push_str(&format!("log-format {}\n", self.log_format));
        }
        // A directive with nothing after it is an error when the file is
        // read back, so an unset option is left out.
        if !self.requirepass.is_empty() {
            out.push_str(&format!("requirepass {}\n", quote(&self.requirepass)));
        }
        if !self.aclfile.is_empty() {
            out.push_str(&format!("aclfile {}\n", quote(&self.aclfile)));
        }
        // Off is written -1, as it is set.
        match self.slowlog_log_slower_than {
            u64::MAX => out.push_str("slowlog-log-slower-than -1\n"),
            us => out.push_str(&format!("slowlog-log-slower-than {us}\n")),
        }
        out.push_str(&format!("slowlog-max-len {}\n", self.slowlog_max_len));
        out.push_str(&format!(
            "cluster-enabled {}\n",
            if self.cluster_enabled { "yes" } else { "no" }
        ));
        out.push_str(&format!(
            "cluster-node-timeout {}\n",
            self.cluster_node_timeout
        ));
        out.push_str(&format!(
            "repl-replica-max-backlog-bytes {}\n",
            self.repl_replica_max_backlog_bytes
        ));
        if !self.masterauth.is_empty() {
            out.push_str(&format!("masterauth {}\n", quote(&self.masterauth)));
        }
        if !self.masteruser.is_empty() {
            out.push_str(&format!("masteruser {}\n", self.masteruser));
        }
        if !self.update_check {
            out.push_str("update-check no\n");
        }
        if self.update_check_url != crate::update_check::DEFAULT_URL {
            out.push_str(&format!("update-check-url {}\n", self.update_check_url));
        }
        out.push_str(&format!("repl-backlog-size {}\n", self.repl_backlog_size));
        out.push_str(&format!("repl-backlog-ttl {}\n", self.repl_backlog_ttl));
        out.push_str(&format!("repl-diskless-load {}\n", self.repl_diskless_load));
        out.push_str(&format!("repl-timeout {}\n", self.repl_timeout));
        out.push_str(&format!(
            "min-replicas-to-write {}\n",
            self.min_replicas_to_write
        ));
        out.push_str(&format!(
            "min-replicas-max-lag {}\n",
            self.min_replicas_max_lag
        ));
        out.push_str(&format!(
            "repl-ping-replica-period {}\n",
            self.repl_ping_replica_period
        ));
        out.push_str(&format!(
            "replica-full-sync-guard {}\n",
            self.replica_full_sync_guard
        ));
        out.push_str(&format!(
            "replica-full-sync-guard-ratio {}\n",
            self.replica_full_sync_guard_ratio
        ));
        if !self.cluster_announce_ip.is_empty() {
            out.push_str(&format!(
                "cluster-announce-ip {}\n",
                self.cluster_announce_ip
            ));
        }
        if !self.cluster_announce_hostname.is_empty() {
            out.push_str(&format!(
                "cluster-announce-hostname {}\n",
                self.cluster_announce_hostname
            ));
        }
        out.push_str(&format!(
            "cluster-preferred-endpoint-type {}\n",
            self.cluster_preferred_endpoint_type
        ));
        if self.cluster_announce_port != 0 {
            out.push_str(&format!(
                "cluster-announce-port {}\n",
                self.cluster_announce_port
            ));
        }
        if self.cluster_announce_bus_port != 0 {
            out.push_str(&format!(
                "cluster-announce-bus-port {}\n",
                self.cluster_announce_bus_port
            ));
        }
        if !self.cluster_bus_secret.is_empty() {
            out.push_str(&format!("cluster-bus-secret {}\n", self.cluster_bus_secret));
        }
        out.push_str(&format!(
            "cluster-bus-port-protected-mode {}\n",
            if self.cluster_bus_port_protected_mode {
                "yes"
            } else {
                "no"
            }
        ));
        out.push_str(&format!(
            "cluster-config-file {}\n",
            self.cluster_config_file
        ));
        out.push_str(&format!(
            "cluster-require-full-coverage {}\n",
            if self.cluster_require_full_coverage {
                "yes"
            } else {
                "no"
            }
        ));
        out.push_str(&format!(
            "slowlog-entry-max-argc {}\n",
            self.slowlog_entry_max_argc
        ));
        out.push_str(&format!(
            "slowlog-entry-max-string-len {}\n",
            self.slowlog_entry_max_string_len
        ));
        out.push_str(&format!(
            "stream-idmp-duration {}\n",
            self.stream_idmp_duration
        ));
        out.push_str(&format!(
            "stream-idmp-maxsize {}\n",
            self.stream_idmp_maxsize
        ));
        out.push_str(&format!("backupdirname {}\n", self.backupdirname));
        out.push_str(&format!("backup-sealed-ttl {}\n", self.backup_sealed_ttl));
        out.push_str(&format!(
            "cluster-slot-migration-handoff-max-lag-bytes {}\n",
            self.cluster_slot_migration_handoff_max_lag_bytes
        ));
        out.push_str(&format!(
            "cluster-slot-migration-write-pause-timeout {}\n",
            self.cluster_slot_migration_write_pause_timeout
        ));
        out.push_str(&format!(
            "notify-keyspace-events {}\n",
            quote(&crate::keyspace_events::flags_to_string(
                crate::keyspace_events::KS_FLAGS.load(std::sync::atomic::Ordering::Relaxed)
            ))
        ));
        if let Some(parts) = &self.replicaof
            && parts.len() == 2
        {
            out.push_str(&format!("replicaof {} {}\n", parts[0], parts[1]));
        }

        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, &out).map_err(RewriteError::Io)?;
        std::fs::rename(&tmp, path).map_err(RewriteError::Io)?;

        Ok(())
    }

    /// Load the config file and apply its directives.
    ///
    /// The precedence chain is: defaults < config file < CLI args.
    /// `from_cli` says whether an option (named as its field is) was given on
    /// the command line; a directive for such an option is still checked, and
    /// then left alone.
    ///
    /// `Ok` carries warnings to log. `Err` is the message to print before
    /// exiting: the file is read strictly. A directive that is not known, or a
    /// value that cannot be understood, used to be dropped without a word —
    /// `appendonly yes` did nothing, and the server ran without the
    /// durability its config file asked for.
    pub fn apply_config_file(
        &mut self,
        from_cli: &dyn Fn(&str) -> bool,
    ) -> Result<Vec<String>, String> {
        if self.configfile.is_empty() {
            return Ok(Vec::new());
        }
        let contents = match std::fs::read_to_string(&self.configfile) {
            Ok(s) => s,
            // The file doesn't exist yet — that's fine, REWRITE creates it.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => {
                return Err(format!("can't open config file '{}': {e}", self.configfile));
            }
        };
        let file = self.configfile.clone();
        self.apply_config_text(&contents, from_cli)
            .map(|warnings| {
                warnings
                    .into_iter()
                    .map(|w| format!("{file}: {w}"))
                    .collect()
            })
            .map_err(|e| format!("{file}: {e}"))
    }

    /// [`Config::apply_config_file`], given the text of the file.
    pub fn apply_config_text(
        &mut self,
        contents: &str,
        from_cli: &dyn Fn(&str) -> bool,
    ) -> Result<Vec<String>, String> {
        let mut warnings = Vec::new();
        for (index, line) in contents.lines().enumerate() {
            let number = index + 1;
            let words = split_config_line(line).map_err(|e| format!("line {number}: {e}"))?;
            let Some((key, args)) = words.split_first() else {
                continue;
            };
            let key = key.to_ascii_lowercase();
            match self.apply_directive(&key, args, from_cli) {
                Ok(Some(warning)) => warnings.push(format!("line {number}: {warning}")),
                Ok(None) => {}
                Err(e) => return Err(format!("line {number}: {e} (in: {})", line.trim())),
            }
        }
        Ok(warnings)
    }

    /// Apply one directive. `Ok(Some(_))` is a warning about it.
    fn apply_directive(
        &mut self,
        key: &str,
        args: &[String],
        from_cli: &dyn Fn(&str) -> bool,
    ) -> Result<Option<String>, String> {
        // The single value of a directive that takes one.
        let one = || -> Result<&str, String> {
            match args {
                [value] => Ok(value.as_str()),
                [] => Err(format!("'{key}' needs a value")),
                _ => Err(format!(
                    "'{key}' takes one value, {} were given",
                    args.len()
                )),
            }
        };
        fn number<T: std::str::FromStr>(key: &str, value: &str) -> Result<T, String> {
            value
                .parse()
                .map_err(|_| format!("invalid value '{value}' for '{key}': not a number in range"))
        }
        fn yes_no(key: &str, value: &str) -> Result<bool, String> {
            parse_yes_no(value).map_err(|_| {
                format!("invalid value '{value}' for '{key}': argument must be 'yes' or 'no'")
            })
        }
        // Checked and, unless the command line already said otherwise, stored.
        macro_rules! set {
            ($field:ident, $value:expr) => {{
                let value = $value;
                if !from_cli(stringify!($field)) {
                    self.$field = value;
                }
            }};
        }
        let no_effect = |why: &str| {
            Ok(Some(format!(
                "'{key}' is accepted but has no effect: {why}"
            )))
        };

        match key {
            "port" => set!(port, number(key, one()?)?),
            "bind" => {
                let Some(first) = args.first() else {
                    return Err("'bind' needs an address".into());
                };
                self.bind_explicit = true;
                set!(bind, first.trim_start_matches('-').to_string());
                if args.len() > 1 {
                    return Ok(Some(format!(
                        "'bind' lists {} addresses; KiviDB listens on one, and uses '{}'",
                        args.len(),
                        self.bind
                    )));
                }
            }
            "threads" => set!(threads, number(key, one()?)?),
            "dir" => set!(dir, one()?.to_string()),
            "aof" | "appendonly" => set!(aof, yes_no(key, one()?)?),
            "aof-preamble" | "aof-use-rdb-preamble" => set!(aof_preamble, yes_no(key, one()?)?),
            "lua-parallel-scripts" => set!(lua_parallel_scripts, yes_no(key, one()?)?),
            "aof-rewrite-percentage" | "auto-aof-rewrite-percentage" => {
                set!(aof_rewrite_percentage, number(key, one()?)?)
            }
            "aof-rewrite-min-size" | "auto-aof-rewrite-min-size" => {
                set!(aof_rewrite_min_size, parse_memory(key, one()?)?)
            }
            "databases" => {
                let n: usize = number(key, one()?)?;
                if n == 0 {
                    return Err("'databases' must be at least 1".into());
                }
                set!(databases, n)
            }
            "maxmemory" => set!(maxmemory, parse_memory(key, one()?)? as usize),
            "loglevel" => {
                let level = match one()?.to_ascii_lowercase().as_str() {
                    level @ ("debug" | "verbose" | "notice" | "info" | "warn" | "warning"
                    | "nothing" | "error") => level.to_string(),
                    other => {
                        return Err(format!(
                            "invalid value '{other}' for 'loglevel': expected debug, verbose, \
                             notice, warning or nothing"
                        ));
                    }
                };
                set!(loglevel, level)
            }
            "io-model" => {
                let model = match one()?.to_ascii_lowercase().as_str() {
                    "auto" => IoModel::Auto,
                    "uring" => IoModel::Uring,
                    "tokio" => IoModel::Tokio,
                    other => {
                        return Err(format!(
                            "invalid value '{other}' for 'io-model': expected auto, uring or tokio"
                        ));
                    }
                };
                set!(io_model, model)
            }
            "requirepass" => set!(requirepass, one()?.to_string()),
            "aclfile" => set!(aclfile, one()?.to_string()),
            "slowlog-log-slower-than" => {
                // A negative value turns the slow log off.
                let us: i64 = number(key, one()?)?;
                set!(
                    slowlog_log_slower_than,
                    u64::try_from(us).unwrap_or(u64::MAX)
                )
            }
            "slowlog-max-len" => set!(slowlog_max_len, number(key, one()?)?),
            "slowlog-entry-max-argc" => set!(slowlog_entry_max_argc, number(key, one()?)?),
            "slowlog-entry-max-string-len" => {
                set!(slowlog_entry_max_string_len, number(key, one()?)?)
            }
            "cluster-enabled" => set!(cluster_enabled, yes_no(key, one()?)?),
            "update-check" => set!(update_check, yes_no(key, one()?)?),
            "update-check-url" => {
                let url = one()?;
                if !url.starts_with("https://") {
                    return Err(format!(
                        "'update-check-url' must be an https:// URL, not '{url}'"
                    ));
                }
                set!(update_check_url, url.to_string())
            }
            "cluster-node-timeout" => set!(cluster_node_timeout, number(key, one()?)?),
            "cluster-announce-ip" => set!(cluster_announce_ip, one()?.to_string()),
            "cluster-announce-hostname" => {
                let name = one()?;
                if !crate::cluster::valid_hostname(name) {
                    return Err(format!(
                        "'cluster-announce-hostname' may only have letters, digits, '-' and \
                         '.', not '{name}'"
                    ));
                }
                set!(cluster_announce_hostname, name.to_string())
            }
            "cluster-preferred-endpoint-type" => {
                let kind = one()?;
                if crate::cluster::Endpoint::parse(kind).is_none() {
                    return Err(format!(
                        "'cluster-preferred-endpoint-type' must be ip, hostname or \
                         unknown-endpoint, not '{kind}'"
                    ));
                }
                set!(cluster_preferred_endpoint_type, kind.to_ascii_lowercase())
            }
            "cluster-announce-port" => set!(cluster_announce_port, number(key, one()?)?),
            "cluster-announce-bus-port" => set!(cluster_announce_bus_port, number(key, one()?)?),
            "cluster-bus-secret" => set!(cluster_bus_secret, one()?.to_string()),
            "cluster-bus-port-protected-mode" => {
                set!(cluster_bus_port_protected_mode, yes_no(key, one()?)?)
            }
            "cluster-config-file" => set!(cluster_config_file, one()?.to_string()),
            "cluster-require-full-coverage" => {
                set!(cluster_require_full_coverage, yes_no(key, one()?)?)
            }
            "cluster-slot-migration-handoff-max-lag-bytes" => set!(
                cluster_slot_migration_handoff_max_lag_bytes,
                parse_memory(key, one()?)?
            ),
            "cluster-slot-migration-write-pause-timeout" => set!(
                cluster_slot_migration_write_pause_timeout,
                number(key, one()?)?
            ),
            "repl-replica-max-backlog-bytes" => {
                set!(repl_replica_max_backlog_bytes, parse_memory(key, one()?)?)
            }
            "repl-backlog-size" => set!(repl_backlog_size, parse_memory(key, one()?)?),
            "repl-backlog-ttl" => set!(repl_backlog_ttl, number(key, one()?)?),
            "repl-diskless-load" => {
                let value = one()?.to_ascii_lowercase();
                if crate::replication::flush_before_load(&value).is_none() {
                    return Err(format!(
                        "invalid value '{value}' for 'repl-diskless-load': expected disabled, \
                         on-empty-db, swapdb or flushdb"
                    ));
                }
                set!(repl_diskless_load, value)
            }
            "masterauth" | "primaryauth" => set!(masterauth, one()?.to_string()),
            "masteruser" | "primaryuser" => set!(masteruser, one()?.to_string()),
            "repl-timeout" => set!(repl_timeout, number(key, one()?)?),
            "min-replicas-to-write" | "min-slaves-to-write" => {
                set!(min_replicas_to_write, number(key, one()?)?)
            }
            "min-replicas-max-lag" | "min-slaves-max-lag" => {
                set!(min_replicas_max_lag, number(key, one()?)?)
            }
            "repl-ping-replica-period" | "repl-ping-slave-period" => {
                set!(repl_ping_replica_period, number(key, one()?)?)
            }
            "replica-full-sync-guard" => {
                set!(replica_full_sync_guard, one()?.to_ascii_lowercase())
            }
            "replica-full-sync-guard-ratio" => {
                set!(replica_full_sync_guard_ratio, number(key, one()?)?)
            }
            "replica-announced-port" | "replica-announce-port" | "slave-announce-port" => {
                set!(replica_announced_port, number(key, one()?)?)
            }
            "replicaof" | "slaveof" => match args {
                [host, port] => {
                    let _: u16 = number(key, port)?;
                    set!(replicaof, Some(vec![host.clone(), port.clone()]))
                }
                _ => return Err(format!("'{key}' takes a host and a port")),
            },
            "stream-idmp-duration" => set!(stream_idmp_duration, number(key, one()?)?),
            "stream-idmp-maxsize" => set!(stream_idmp_maxsize, number(key, one()?)?),
            "backupdirname" => {
                let name = one()?;
                if name.is_empty() {
                    return Err("'backupdirname' cannot be empty".into());
                }
                set!(backupdirname, name.to_string())
            }
            "backup-sealed-ttl" => set!(backup_sealed_ttl, number(key, one()?)?),
            "preload-file" => set!(preload_file, one()?.to_string()),
            "tls-port" => set!(tls_port, number(key, one()?)?),
            "tls-cert-file" => set!(tls_cert_file, one()?.to_string()),
            "tls-key-file" => set!(tls_key_file, one()?.to_string()),
            "tls-ca-cert-file" => set!(tls_ca_cert_file, one()?.to_string()),
            "protected-mode" => set!(protected_mode, yes_no(key, one()?)?),
            "tls-cluster" => set!(tls_cluster, yes_no(key, one()?)?),
            "tls-replication" => set!(tls_replication, yes_no(key, one()?)?),
            "tls-client-cert-file" => set!(tls_client_cert_file, one()?.to_string()),
            "tls-client-key-file" => set!(tls_client_key_file, one()?.to_string()),
            "tls-auth-clients" => {
                let value = one()?.to_ascii_lowercase();
                if !TLS_AUTH_CLIENTS.contains(&value.as_str()) {
                    return Err(format!(
                        "invalid value '{value}' for 'tls-auth-clients': expected yes, no or \
                         optional"
                    ));
                }
                set!(tls_auth_clients, value)
            }
            "notify-keyspace-events" => set!(notify_keyspace_events, one()?.to_string()),

            // ── Redis directives that name something KiviDB has one way of
            // doing. The value KiviDB already behaves as is accepted quietly;
            // any other would be a promise it does not keep.
            "appendfilename" => {
                if one()? != crate::aof::AOF_PATH {
                    return Err(format!(
                        "'appendfilename' cannot be changed: the append-only file is always \
                         '{}'",
                        crate::aof::AOF_PATH
                    ));
                }
            }
            "dbfilename" => {
                if !matches!(one()?, "dump.kdb" | "dump.rdb") {
                    return Err(
                        "'dbfilename' cannot be changed: the snapshot is always 'dump.kdb'".into(),
                    );
                }
            }
            "daemonize" => {
                if yes_no(key, one()?)? {
                    return Err(
                        "'daemonize yes' is not supported: KiviDB runs in the foreground".into(),
                    );
                }
            }
            "appendfsync" => match one()? {
                "everysec" => {}
                "always" | "no" => {
                    return no_effect("the append-only file is synced once a second");
                }
                other => return Err(format!("invalid value '{other}' for 'appendfsync'")),
            },
            "save" => {
                if args.iter().any(|a| !a.is_empty()) {
                    return no_effect(
                        "KiviDB does not snapshot on a schedule — use `appendonly yes`, or \
                         BGSAVE",
                    );
                }
            }
            "replica-read-only" | "slave-read-only" => {
                if !yes_no(key, one()?)? {
                    return no_effect("a replica is always read-only");
                }
            }
            "aof-load-truncated" => {
                if !yes_no(key, one()?)? {
                    return no_effect("a truncated append-only file is always loaded");
                }
            }
            "logfile" => set!(logfile, one()?.to_string()),
            "log-format" => {
                let format = one()?.to_ascii_lowercase();
                if format != "text" && format != "json" {
                    return Err(format!(
                        "invalid value '{format}' for 'log-format': expected text or json"
                    ));
                }
                set!(log_format, format)
            }
            "pidfile" | "unixsocket" => {
                if !one()?.is_empty() {
                    return no_effect("not supported");
                }
            }
            "maxmemory-policy" => {
                let value = one()?.to_ascii_lowercase();
                if crate::eviction::Policy::parse(&value).is_none() {
                    return Err(format!(
                        "invalid value '{value}' for 'maxmemory-policy': expected one of {}",
                        crate::eviction::POLICY_NAMES
                    ));
                }
                set!(maxmemory_policy, value)
            }
            "maxmemory-samples" => {
                let n: usize = number(key, one()?)?;
                if !(1..=64).contains(&n) {
                    return Err("'maxmemory-samples' must be between 1 and 64".into());
                }
                set!(maxmemory_samples, n)
            }
            "lfu-log-factor" => set!(lfu_log_factor, number(key, one()?)?),
            "lfu-decay-time" => set!(lfu_decay_time, number(key, one()?)?),
            "key-access-tracking" => set!(key_access_tracking, yes_no(key, one()?)?),
            // Tuning knobs for machinery KiviDB does not have, or does not
            // expose. They change nothing a client could observe.
            "activerehashing"
            | "always-show-logo"
            | "appenddirname"
            | "dynamic-hz"
            | "hz"
            | "io-threads"
            | "io-threads-do-reads"
            | "lazyfree-lazy-eviction"
            | "lazyfree-lazy-expire"
            | "lazyfree-lazy-server-del"
            | "lazyfree-lazy-user-del"
            | "no-appendfsync-on-rewrite"
            | "proc-title-template"
            | "rdbchecksum"
            | "rdbcompression"
            | "repl-disable-tcp-nodelay"
            | "repl-diskless-sync"
            | "repl-diskless-sync-delay"
            | "replica-serve-stale-data"
            | "set-proc-title"
            | "stop-writes-on-bgsave-error"
            | "supervised"
            | "tcp-backlog"
            | "tcp-keepalive"
            | "timeout" => {
                if args.is_empty() {
                    return Err(format!("'{key}' needs a value"));
                }
                return no_effect("KiviDB has no such setting");
            }

            _ => return Err(format!("unknown directive '{key}'")),
        }
        Ok(None)
    }
}

/// Split a config line into words. A word may be quoted — `"…"`, with `\"`,
/// `\\`, `\n`, `\r` and `\t` — or `'…'`; a `#` where a word would start
/// begins a comment.
fn split_config_line(line: &str) -> Result<Vec<String>, String> {
    let mut words = Vec::new();
    let mut chars = line.chars().peekable();
    loop {
        while chars.next_if(|c| c.is_whitespace()).is_some() {}
        let Some(&first) = chars.peek() else { break };
        if first == '#' {
            break;
        }
        let mut word = String::new();
        if first == '"' || first == '\'' {
            chars.next();
            let mut closed = false;
            while let Some(c) = chars.next() {
                if c == first {
                    closed = true;
                    break;
                }
                if c == '\\' && first == '"' {
                    match chars.next() {
                        Some('n') => word.push('\n'),
                        Some('r') => word.push('\r'),
                        Some('t') => word.push('\t'),
                        Some(other) => word.push(other),
                        None => break,
                    }
                } else {
                    word.push(c);
                }
            }
            if !closed {
                return Err("unbalanced quotes".into());
            }
            if chars.peek().is_some_and(|c| !c.is_whitespace()) {
                return Err("a closing quote must be followed by a space".into());
            }
        } else {
            while let Some(c) = chars.next_if(|c| !c.is_whitespace()) {
                word.push(c);
            }
        }
        words.push(word);
    }
    Ok(words)
}

/// A value as [`split_config_line`] reads it back: quoted if it has to be.
fn quote(value: &str) -> String {
    if !value.is_empty()
        && !value.starts_with(['#', '"', '\''])
        && !value.contains(|c: char| c.is_whitespace() || c == '\\')
    {
        return value.to_string();
    }
    let mut out = String::from("\"");
    for c in value.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            other => out.push(other),
        }
    }
    out.push('"');
    out
}

/// A size as Redis config files write them: `1048576`, `1mb`, `512k`, `2GB`.
/// `k`, `m` and `g` are powers of 1000; `kb`, `mb` and `gb` powers of 1024.
pub(crate) fn parse_memory(key: &str, value: &str) -> Result<u64, String> {
    let lower = value.to_ascii_lowercase();
    let digits = lower.trim_end_matches(|c: char| c.is_ascii_alphabetic());
    let unit: u64 = match &lower[digits.len()..] {
        "" | "b" => 1,
        "k" => 1000,
        "kb" => 1024,
        "m" => 1000 * 1000,
        "mb" => 1024 * 1024,
        "g" => 1000 * 1000 * 1000,
        "gb" => 1024 * 1024 * 1024,
        _ => {
            return Err(format!(
                "invalid value '{value}' for '{key}': not a memory size"
            ));
        }
    };
    digits
        .parse::<u64>()
        .ok()
        .and_then(|n| n.checked_mul(unit))
        .ok_or_else(|| format!("invalid value '{value}' for '{key}': not a memory size"))
}

/// [`parse_memory`] for a command-line flag, which Redis takes with units too
/// (`--maxmemory 3gb`).
fn parse_memory_flag(value: &str) -> Result<usize, String> {
    parse_memory("--maxmemory", value)
        .and_then(|n| usize::try_from(n).map_err(|_| format!("invalid value '{value}': too large")))
}

/// Parse Redis-style `yes` / `no` into a bool. Used by `--cluster-enabled`
/// and any future flags that want to mirror redis.conf phrasing rather than
/// clap's default `true` / `false`.
fn parse_yes_no(s: &str) -> Result<bool, String> {
    match s.to_ascii_lowercase().as_str() {
        "yes" | "true" | "1" => Ok(true),
        "no" | "false" | "0" => Ok(false),
        other => Err(format!("expected 'yes' or 'no', got '{other}'")),
    }
}

/// Error returned by `Config::rewrite_to_disk`.
pub enum RewriteError {
    Io(std::io::Error),
}

#[cfg(test)]
mod config_file_tests {
    use super::*;

    fn load(text: &str) -> Result<(Config, Vec<String>), String> {
        let mut config = Config::default();
        let warnings = config.apply_config_text(text, &|_| false)?;
        Ok((config, warnings))
    }

    #[test]
    fn tls_auth_clients_takes_yes_no_or_optional() {
        assert_eq!(Config::default().tls_auth_clients, "yes");
        let (c, _) = load("tls-auth-clients OPTIONAL\n").unwrap();
        assert_eq!(c.tls_auth_clients, "optional");
        let err = load("tls-auth-clients maybe\n").unwrap_err();
        assert!(err.contains("tls-auth-clients"), "{err}");
    }

    #[test]
    fn maxmemory_on_the_command_line_takes_units_as_in_redis() {
        use clap::Parser;
        let at =
            |v: &str| Config::try_parse_from(["kividb", "--maxmemory", v]).map(|c| c.maxmemory);
        assert_eq!(at("3gb").unwrap(), 3 * 1024 * 1024 * 1024);
        assert_eq!(at("512mb").unwrap(), 512 * 1024 * 1024);
        assert_eq!(at("1000").unwrap(), 1000);
        assert!(at("lots").is_err());
    }

    #[test]
    fn protected_mode_is_on_until_turned_off_or_a_bind_address_is_given() {
        let c = Config::default();
        assert!(c.protected_mode && !c.bind_explicit);
        assert!(!load("protected-mode no\n").unwrap().0.protected_mode);
        assert!(load("protected-mode yes\n").unwrap().0.protected_mode);
        assert!(load("protected-mode maybe\n").is_err());
        assert!(load("bind 0.0.0.0\n").unwrap().0.bind_explicit);
    }

    /// `appendonly`, Redis's spelling of the AOF switch, must work in a config file.
    #[test]
    fn the_redis_spelling_of_the_aof_switch_is_understood() {
        assert!(load("appendonly yes\n").unwrap().0.aof);
        assert!(load("aof yes\n").unwrap().0.aof);
        assert!(!load("appendonly no\n").unwrap().0.aof);
    }

    #[test]
    fn an_unknown_directive_is_an_error_that_names_its_line() {
        let err = load("port 7000\n\n# comment\nthis-directive-does-not-exist 42\n").unwrap_err();
        assert!(err.contains("line 4"), "{err}");
        assert!(
            err.contains("unknown directive 'this-directive-does-not-exist'"),
            "{err}"
        );
    }

    #[test]
    fn a_value_that_cannot_be_understood_is_an_error() {
        for bad in [
            "maxmemory notanumber",
            "port seventy",
            "port 70000",
            "appendonly maybe",
            "databases 0",
            "loglevel shouting",
            "replicaof onlyhost",
            "port",
            "port 1 2",
            "requirepass \"unterminated",
            "maxmemory-policy allkeys-mru",
            "maxmemory-samples 0",
            "daemonize yes",
            "dbfilename other.rdb",
        ] {
            let err = load(bad).unwrap_err();
            assert!(err.starts_with("line 1: "), "{bad}: {err}");
        }
    }

    #[test]
    fn memory_sizes_take_units() {
        assert_eq!(
            load("maxmemory 100mb").unwrap().0.maxmemory,
            100 * 1024 * 1024
        );
        assert_eq!(load("maxmemory 2G").unwrap().0.maxmemory, 2_000_000_000);
        assert_eq!(load("maxmemory 4096").unwrap().0.maxmemory, 4096);
        assert_eq!(
            load("auto-aof-rewrite-min-size 64mb")
                .unwrap()
                .0
                .aof_rewrite_min_size,
            64 * 1024 * 1024
        );
    }

    #[test]
    fn quotes_and_comments() {
        let (config, _) = load("requirepass \"pa ss#word\" # the password\n").unwrap();
        assert_eq!(config.requirepass, "pa ss#word");
        let (config, _) = load("requirepass 'a\"b'\n").unwrap();
        assert_eq!(config.requirepass, "a\"b");
        let (config, _) = load("requirepass p#ss\n").unwrap();
        assert_eq!(config.requirepass, "p#ss");
        for value in ["plain", "has space", "#hash", "quo\"te", "back\\slash", ""] {
            let words = split_config_line(&format!("k {}", quote(value))).unwrap();
            assert_eq!(words, ["k", value]);
        }
    }

    #[test]
    fn the_command_line_wins_over_the_file() {
        let mut config = Config {
            port: 7000,
            ..Config::default()
        };
        config
            .apply_config_text("port 8000\nthreads 3\n", &|id| id == "port")
            .unwrap();
        assert_eq!(config.port, 7000);
        assert_eq!(config.threads, 3);
        // A value the command line overrides is still checked.
        assert!(
            config
                .apply_config_text("port x\n", &|id| id == "port")
                .is_err()
        );
    }

    #[test]
    fn redis_directives_with_no_effect_are_reported_and_not_fatal() {
        let (_, warnings) = load("save 900 1\ntcp-keepalive 300\nappendfsync always\n").unwrap();
        assert_eq!(warnings.len(), 3, "{warnings:?}");
        assert!(warnings[0].contains("line 1") && warnings[0].contains("'save'"));
        // The values KiviDB already behaves as are not worth a warning.
        let (_, warnings) =
            load("save \"\"\nappendfsync everysec\ndaemonize no\ndbfilename dump.kdb\n").unwrap();
        assert!(warnings.is_empty(), "{warnings:?}");
    }

    /// Every option the file can set is asked about by the name clap knows
    /// it under — a wrong name there would panic at startup.
    #[test]
    fn every_directive_asks_about_an_option_that_exists() {
        use clap::CommandFactory;
        let command = Config::command();
        let known: Vec<String> = command
            .get_arguments()
            .map(|a| a.get_id().to_string())
            .collect();
        let everything = "port 1\nbind a\nthreads 1\ndir d\naof yes\naof-preamble yes\n\
            aof-rewrite-percentage 1\naof-rewrite-min-size 1\ndatabases 1\nmaxmemory 1\n\
            loglevel info\nio-model tokio\nrequirepass p\naclfile f\nslowlog-log-slower-than 1\n\
            slowlog-max-len 1\nslowlog-entry-max-argc 1\nslowlog-entry-max-string-len 1\n\
            cluster-enabled yes\ncluster-node-timeout 1\ncluster-announce-ip a\n\
            cluster-announce-hostname h\ncluster-preferred-endpoint-type ip\n\
            cluster-announce-port 1\ncluster-announce-bus-port 1\n\
            cluster-bus-port-protected-mode no\ncluster-bus-secret s\n\
            cluster-config-file f\n\
            cluster-require-full-coverage no\ncluster-slot-migration-handoff-max-lag-bytes 1\n\
            cluster-slot-migration-write-pause-timeout 1\nrepl-replica-max-backlog-bytes 1\n\
            masterauth a\nmasteruser u\nrepl-timeout 1\nrepl-ping-replica-period 1\n\
            min-replicas-to-write 1\nmin-replicas-max-lag 1\n\
            repl-backlog-size 1mb\nrepl-backlog-ttl 1\nrepl-diskless-load swapdb\n\
            replica-full-sync-guard off\nreplica-full-sync-guard-ratio 1\n\
            replica-announced-port 1\nreplicaof h 1\nstream-idmp-duration 1\n\
            stream-idmp-maxsize 1\nbackupdirname b\nbackup-sealed-ttl 1\npreload-file f\n\
            tls-port 1\ntls-cert-file f\ntls-key-file f\ntls-ca-cert-file f\n\
            tls-cluster no\ntls-replication no\ntls-client-cert-file f\ntls-client-key-file f\n\
            tls-auth-clients optional\n\
            notify-keyspace-events KEA\n";
        let asked = std::cell::Cell::new(0);
        Config::default()
            .apply_config_text(everything, &|id| {
                assert!(known.iter().any(|k| k == id), "no option named {id}");
                asked.set(asked.get() + 1);
                false
            })
            .unwrap();
        assert_eq!(asked.get(), everything.lines().count());
    }
}

#[cfg(test)]
mod validate_tests {
    use super::Config;

    fn cfg() -> Config {
        Config::default()
    }

    /// Rebuilt nodes serving plaintext:
    /// a build with no TLS in it accepted `--tls-port` and silently served
    /// plaintext. It must refuse instead.
    #[test]
    fn tls_port_on_a_build_without_tls_is_fatal() {
        let c = Config {
            tls_port: 6380,
            tls_cert_file: "cert.pem".into(),
            tls_key_file: "key.pem".into(),
            ..cfg()
        };
        let err = c.validate(false).unwrap_err();
        assert!(err.contains("built without TLS support"), "{err}");
        // The same configuration is fine on a build that has TLS.
        assert!(c.validate(true).is_ok());
    }

    #[test]
    fn invalid_tls_auth_clients_is_fatal() {
        let c = Config {
            tls_auth_clients: "maybe".into(),
            ..cfg()
        };
        let err = c.validate(true).unwrap_err();
        assert!(err.contains("tls-auth-clients"), "{err}");
    }

    #[test]
    fn tls_cert_without_port_only_warns_on_a_build_without_tls() {
        let c = Config {
            tls_cert_file: "cert.pem".into(),
            ..cfg()
        };
        let warnings = c.validate(false).unwrap();
        assert!(warnings.iter().any(|w| w.contains("without TLS support")));
    }

    #[test]
    fn tls_port_without_cert_is_fatal_on_a_tls_build() {
        let c = Config {
            tls_port: 6380,
            ..cfg()
        };
        assert!(c.validate(true).is_err());
    }

    #[test]
    fn default_config_is_valid_and_quiet() {
        assert_eq!(cfg().validate(false).unwrap(), Vec::<String>::new());
        assert_eq!(cfg().validate(true).unwrap(), Vec::<String>::new());
    }

    #[test]
    fn full_sync_guard_value_is_checked() {
        for ok in ["off", "empty", "shrink"] {
            let c = Config {
                replica_full_sync_guard: ok.into(),
                ..cfg()
            };
            assert!(c.validate(true).is_ok(), "{ok} must be accepted");
        }
        let c = Config {
            replica_full_sync_guard: "sometimes".into(),
            ..cfg()
        };
        assert!(c.validate(true).is_err());
    }

    #[test]
    fn cluster_mode_warns_about_the_open_bus_and_can_refuse() {
        let c = Config {
            cluster_enabled: true,
            ..cfg()
        };
        let warnings = c.validate(true).unwrap();
        assert!(warnings.iter().any(|w| w.contains("not authenticated")));

        let c = Config {
            cluster_enabled: true,
            cluster_bus_port_protected_mode: true,
            ..cfg()
        };
        assert!(c.validate(true).is_err());
    }

    #[test]
    fn replicaof_is_rejected_in_cluster_mode() {
        let c = Config {
            cluster_enabled: true,
            replicaof: Some(vec!["127.0.0.1".into(), "6379".into()]),
            ..cfg()
        };
        assert!(c.validate(true).unwrap_err().contains("CLUSTER REPLICATE"));
    }
}
