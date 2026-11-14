use crate::commands::ctx::Ctx;
use crate::loglevel::LOG_LEVEL;
use crate::read_lock;
use crate::resp::write::*;
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// Redis's `avg_ttl`: how long the `expires` keys with a time to live have
/// left on average, in milliseconds, at `now` (milliseconds since the Unix
/// epoch), given the sum of when their times end (`ttl_deadline_sum`, kept
/// by each shard as times come, change and go).
///
/// It went over every key with a time to live, holding each shard while it
/// did: at 5 million such keys `INFO` took 45-70 ms, and writes to each
/// shard waited for it (#622). Now it is a sum and a count a shard. The
/// mean of the times, less `now`, is the mean of what is left, rounded down
/// as before. Keys past their time that expiry has not taken out yet (a
/// cycle or so) count as having less than nothing left, and the whole is
/// at least 0; the pass left them out.
fn avg_ttl_ms(expires: usize, deadlines: u128, now: u64) -> i64 {
    if expires == 0 {
        return 0;
    }
    let mean = deadlines / expires as u128;
    mean.saturating_sub(u128::from(now)).min(i64::MAX as u128) as i64
}

/// Why `CONFIG SET cluster-announce-ip` refuses `value`, if it does. As in
/// Redis: shorter than 46 characters (an IPv6 address's longest text), and
/// only letters, digits, '-', '.' and ':'. Empty unsets it.
fn announce_ip_error(value: &str) -> Option<&'static str> {
    if value.len() >= 46 {
        Some("Hostnames for cluster-announce-ip must be less than 46 characters")
    } else if !value
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b':'))
    {
        Some("Hostnames may only contain alphanumeric characters, hyphens or dots")
    } else {
        None
    }
}

/// The reply to a `CONFIG SET` of an announce setting: it is in effect
/// either way, but nodes.conf may not have been saved.
fn announce_reply(saved: std::io::Result<()>) -> Option<Vec<u8>> {
    match saved {
        Ok(()) => Some(resp_simple("OK")),
        Err(e) => Some(resp_error(&format!(
            "ERR CONFIG SET applied, but saving the cluster config file failed: {e}"
        ))),
    }
}

/// Replication, cluster and slowlog settings as `CONFIG GET` reports them.
///
/// Runtime-settable ones are read from where `CONFIG SET` writes them, so a
/// `GET` after a `SET` returns what was set rather than the startup value.
fn replication_and_cluster_config(ctx: &Ctx) -> Vec<(&'static str, String)> {
    use std::sync::atomic::Ordering::Relaxed;
    let yes_no = |b: bool| if b { "yes" } else { "no" }.to_string();
    let rs = ctx.repl_state;
    let (master_user, master_auth) = rs.master_credentials();
    // `CONFIG SET` changes it in a build with TLS.
    #[cfg(feature = "tls")]
    let tls_replication = crate::node_tls::replication_set();
    #[cfg(not(feature = "tls"))]
    let tls_replication = ctx.config.tls_replication;
    // This node's own hostname and client port, read under one guard that is
    // dropped here. Each used to be read inside the `vec!` below with a
    // `nodes.read()` of its own; a temporary lives to the end of its
    // statement, so the first guard was still held when the second was
    // taken. A read of a lock the thread already holds waits behind any
    // writer that queued in between (parking_lot lets no new reader past a
    // waiting writer), and the writer waits for the first guard: neither
    // ever moved again. The cluster bus writes `nodes` on every heartbeat,
    // and a `CONFIG GET` that met one stopped the node for good, every
    // command that reads the topology queued behind it.
    let (announce_ip, announce_port, announce_bus_port) = ctx.cluster.announce();
    let (announce_hostname, announce_client_port) = {
        let nodes = ctx.cluster.nodes.read();
        let me = nodes.get(&ctx.cluster.myself());
        (
            me.map(|me| me.hostname.clone())
                .unwrap_or_else(|| ctx.config.cluster_announce_hostname.clone()),
            me.map(|me| me.announced_client_port)
                .unwrap_or(ctx.config.cluster_announce_client_port),
        )
    };
    vec![
        (
            "repl-timeout",
            rs.repl_timeout_secs.load(Relaxed).to_string(),
        ),
        (
            "repl-ping-replica-period",
            rs.repl_ping_period_secs.load(Relaxed).to_string(),
        ),
        (
            "shutdown-timeout",
            rs.shutdown_timeout_secs.load(Relaxed).to_string(),
        ),
        // Under both of its names, as Redis lists it.
        (
            "replica-priority",
            rs.replica_priority.load(Relaxed).to_string(),
        ),
        (
            "slave-priority",
            rs.replica_priority.load(Relaxed).to_string(),
        ),
        (
            "replica-announced",
            yes_no(rs.replica_announced.load(Relaxed)),
        ),
        (
            "replica-full-sync-guard",
            rs.full_sync_guard().as_str().to_string(),
        ),
        (
            "replica-full-sync-guard-ratio",
            rs.full_sync_guard_ratio.load(Relaxed).to_string(),
        ),
        ("masteruser", master_user),
        // Never echo a credential back.
        (
            "masterauth",
            if master_auth.is_empty() {
                String::new()
            } else {
                "**redacted**".to_string()
            },
        ),
        (
            "repl-replica-max-backlog-bytes",
            rs.replica_max_backlog_bytes.load(Relaxed).to_string(),
        ),
        (
            "repl-replica-sync-max-backlog-bytes",
            rs.replica_sync_max_backlog_bytes.load(Relaxed).to_string(),
        ),
        ("cluster-enabled", yes_no(ctx.config.cluster_enabled)),
        (
            "cluster-config-file",
            ctx.config.cluster_config_file.clone(),
        ),
        (
            "cluster-node-timeout",
            ctx.config.cluster_node_timeout.to_string(),
        ),
        ("cluster-announce-ip", announce_ip),
        ("cluster-announce-hostname", announce_hostname),
        (
            "cluster-preferred-endpoint-type",
            if ctx.cluster.enabled {
                ctx.cluster.preferred_endpoint().name().to_string()
            } else {
                ctx.config.cluster_preferred_endpoint_type.clone()
            },
        ),
        ("cluster-announce-port", announce_port.to_string()),
        (
            "cluster-announce-client-port",
            announce_client_port.to_string(),
        ),
        ("cluster-announce-bus-port", announce_bus_port.to_string()),
        ("cluster-port", ctx.config.cluster_port.to_string()),
        (
            "cluster-bus-port-protected-mode",
            yes_no(ctx.config.cluster_bus_port_protected_mode),
        ),
        ("protected-mode", yes_no(crate::protected_mode::is_on())),
        (
            "key-access-tracking",
            yes_no(crate::eviction::key_access_tracking()),
        ),
        ("tls-cluster", yes_no(ctx.config.tls_cluster)),
        ("tls-replication", yes_no(tls_replication)),
        (
            "cluster-bus-secret",
            if ctx.config.cluster_bus_secret.is_empty() {
                String::new()
            } else {
                "**redacted**".to_string()
            },
        ),
        (
            "cluster-require-full-coverage",
            yes_no(ctx.cluster.require_full_coverage()),
        ),
        (
            "cluster-slot-migration-handoff-max-lag-bytes",
            crate::cluster::migration::handoff_max_lag_bytes(ctx.config).to_string(),
        ),
        (
            "cluster-slot-migration-write-pause-timeout",
            crate::cluster::migration::write_pause_timeout_ms(ctx.config).to_string(),
        ),
        (
            "cluster-replica-validity-factor",
            ctx.cluster.replica_validity_factor().to_string(),
        ),
        (
            "slowlog-entry-max-argc",
            ctx.slowlog.entry_max_argc().to_string(),
        ),
        (
            "slowlog-entry-max-string-len",
            ctx.slowlog.entry_max_string_len().to_string(),
        ),
        (
            "stream-idmp-duration",
            crate::commands::stream_extra::idmp_defaults(ctx.config)
                .0
                .to_string(),
        ),
        ("backupdirname", ctx.config.backupdirname.clone()),
        (
            "backup-sealed-ttl",
            crate::commands::backup::sealed_ttl(ctx.config).to_string(),
        ),
        ("preload-file", ctx.config.preload_file.clone()),
        (
            "stream-idmp-maxsize",
            crate::commands::stream_extra::idmp_defaults(ctx.config)
                .1
                .to_string(),
        ),
        (
            "stream-node-max-entries",
            crate::commands::stream_extra::NODE_MAX_ENTRIES
                .load(std::sync::atomic::Ordering::Relaxed)
                .to_string(),
        ),
        (
            "stream-node-max-bytes",
            crate::commands::stream_extra::NODE_MAX_BYTES
                .load(std::sync::atomic::Ordering::Relaxed)
                .to_string(),
        ),
        (
            "array-slice-size",
            crate::types::ARRAY_SLICE_SIZE
                .load(std::sync::atomic::Ordering::Relaxed)
                .to_string(),
        ),
        (
            "array-sparse-kmax",
            crate::types::ARRAY_SPARSE_KMAX
                .load(std::sync::atomic::Ordering::Relaxed)
                .to_string(),
        ),
        (
            "array-sparse-kmin",
            crate::types::ARRAY_SPARSE_KMIN
                .load(std::sync::atomic::Ordering::Relaxed)
                .to_string(),
        ),
    ]
    .into_iter()
    .chain(crate::commands::bf::config_rows())
    .collect()
}

/// `CONFIG SET` of the array settings, as Redis checks them: the slice size
/// a power of two from 256 to 65536; `kmax` and `kmin` from 0 to 256, `kmin`
/// below `kmax` unless `kmax` is 0 (sparse slices off).
fn config_set_array(param: &str, value: &str) -> Vec<u8> {
    use crate::types::{ARRAY_SLICE_SIZE, ARRAY_SPARSE_KMAX, ARRAY_SPARSE_KMIN};
    use std::sync::atomic::Ordering::Relaxed;
    let failed = |why: &str| {
        resp_error(&format!(
            "ERR CONFIG SET failed (possibly related to argument '{param}') - {why}"
        ))
    };
    let (min, max) = if param == "array-slice-size" {
        (256, 65536)
    } else {
        (0, 256)
    };
    let Some(v) = value
        .parse::<u32>()
        .ok()
        .filter(|v| (min..=max).contains(v))
    else {
        return failed(&format!(
            "argument must be between {min} and {max} inclusive"
        ));
    };
    match param {
        "array-slice-size" if !v.is_power_of_two() => {
            return failed("array-slice-size must be a power of two");
        }
        "array-slice-size" => ARRAY_SLICE_SIZE.store(v, Relaxed),
        "array-sparse-kmax" if v != 0 && v <= ARRAY_SPARSE_KMIN.load(Relaxed) => {
            return failed(
                "array-sparse-kmax must be greater than array-sparse-kmin when non-zero",
            );
        }
        "array-sparse-kmax" => ARRAY_SPARSE_KMAX.store(v, Relaxed),
        _ => {
            let kmax = ARRAY_SPARSE_KMAX.load(Relaxed);
            if kmax != 0 && v >= kmax {
                return failed("array-sparse-kmin must be less than array-sparse-kmax");
            }
            ARRAY_SPARSE_KMIN.store(v, Relaxed);
        }
    }
    resp_simple("OK")
}

const NOT_MEMORY_OR_PERCENT: &str = "ERR argument must be a memory or percent value";

/// The Redis release whose commands and replies this server follows: `INFO`'s
/// `redis_version`, and `HELLO`'s `version`. Clients read it to decide what
/// they may send; redis-py and Jedis turn on their client-side caches only
/// for a `server` of `redis` at 7.4 or later (#451).
pub(crate) const REDIS_VERSION: &str = "8.10.0";

/// The reply to a script command in a build without Lua. Job queues such as
/// BullMQ and Sidekiq show it to their users, so it names the build to use.
#[cfg(not(feature = "lua"))]
const NO_LUA: &str = concat!(
    "ERR this build of KiviDB has no Lua: EVAL, SCRIPT, FUNCTION and FCALL need the ",
    "lua or full variant (the kividb-full apt/dnf package, Docker image kividb/kividb:v",
    env!("CARGO_PKG_VERSION"),
    "-lua, or a kividb-lua release file)"
);

/// `FUNCTION` in a build without Lua. `FUNCTION FLUSH` drops the libraries
/// it kept from the data it loaded (`snapshot::flush_kept_functions`), so
/// they can be got rid of here too, from a client, the master or the AOF;
/// every other subcommand is refused. One refused from the master or the
/// AOF leaves this server with other libraries than the master had, so it
/// is logged, once for each: a replica without Lua keeps those of its last
/// full sync.
#[cfg(not(feature = "lua"))]
fn function_without_lua(ctx: &Ctx) -> Vec<u8> {
    use std::sync::atomic::AtomicBool;
    let sub = ctx.cmd.get(1).copied().unwrap_or_default();
    if sub.eq_ignore_ascii_case(b"FLUSH") {
        // As Redis answers these.
        match ctx.cmd.len() {
            2 => {}
            3 if ctx.cmd[2].eq_ignore_ascii_case(b"SYNC")
                || ctx.cmd[2].eq_ignore_ascii_case(b"ASYNC") => {}
            3 => return resp_error("ERR FUNCTION FLUSH only supports SYNC|ASYNC option"),
            _ => {
                return resp_error(
                    "ERR unknown subcommand or wrong number of arguments for 'flush'. Try \
                     FUNCTION HELP.",
                );
            }
        }
        crate::snapshot::flush_kept_functions();
        return resp_simple("OK");
    }
    if ctx.apply_replication {
        static FROM_MASTER: AtomicBool = AtomicBool::new(false);
        static FROM_AOF: AtomicBool = AtomicBool::new(false);
        // The AOF is replayed with the dataset counted as loading; the
        // master's stream is applied once it is loaded.
        let (source, warned) = if crate::expiry::loading() {
            ("the AOF", &FROM_AOF)
        } else {
            ("the master", &FROM_MASTER)
        };
        if !warned.swap(true, std::sync::atomic::Ordering::Relaxed) {
            log_warn!(
                "FUNCTION {} from {source} was refused: this build has no Lua, so its function \
                 libraries are not those the master has (those of the data it loaded, if any). \
                 Run the lua or full build on replicas of a master that uses them",
                String::from_utf8_lossy(sub).to_ascii_uppercase()
            );
        }
    }
    resp_error(NO_LUA)
}

/// A setting's own name, for one Redis also takes under an older one.
fn config_name(name: &str) -> &str {
    match name {
        "cluster-slave-no-failover" => "cluster-replica-no-failover",
        "slave-lazy-flush" => "replica-lazy-flush",
        "slave-serve-stale-data" => "replica-serve-stale-data",
        "slave-read-only" => "replica-read-only",
        "slave-ignore-maxmemory" => "replica-ignore-maxmemory",
        "slave-announce-ip" => "replica-announce-ip",
        "cluster-slave-validity-factor" => "cluster-replica-validity-factor",
        "list-max-ziplist-size" => "list-max-listpack-size",
        "slave-priority" => "replica-priority",
        "slave-announce-port" => "replica-announce-port",
        "repl-ping-slave-period" => "repl-ping-replica-period",
        "min-slaves-to-write" => "min-replicas-to-write",
        "min-slaves-max-lag" => "min-replicas-max-lag",
        "lua-time-limit" => "busy-reply-threshold",
        "hash-max-ziplist-entries" => "hash-max-listpack-entries",
        "zset-max-ziplist-entries" => "zset-max-listpack-entries",
        "hash-max-ziplist-value" => "hash-max-listpack-value",
        "zset-max-ziplist-value" => "zset-max-listpack-value",
        "slaveof" => "replicaof",
        other => other,
    }
}

/// `CONFIG SET appendonly yes` with the AOF off: the log made, and the
/// dataset written out under it, before the reply.
fn enable_aof(ctx: &Ctx) -> Vec<u8> {
    use std::sync::atomic::Ordering::Relaxed;
    // The checkpoint's turn, from before the log is made to the end of the
    // checkpoint: no other checkpoint, or other enable, between the two.
    let turn = match crate::aof::checkpoint_turn() {
        Ok(turn) => turn,
        Err(e) => return resp_error(&format!("ERR enabling AOF failed: {e}")),
    };
    if crate::aof::AOF_ENABLED.load(Relaxed) {
        return resp_simple("OK");
    }
    // The log, before the flag goes up: a new one, unless the one kept open
    // is still current (#350). One that cannot be made is an error and the
    // AOF stays off, as in Redis, not an exit (#47).
    if let Err(e) = crate::aof::open_log(&turn) {
        log_warn!("CONFIG SET appendonly yes: {e}");
        return resp_error(&format!(
            "ERR CONFIG SET failed (possibly related to argument 'appendonly') - Unable to turn on AOF: {e}"
        ));
    }
    crate::aof::AOF_ENABLED.store(true, Relaxed);
    crate::aof::failpoint("aof-enabling");
    // Redis CONFIG SET appendonly yes rewrites before OK. Returning first
    // left a window where the next write could miss both the dump and the
    // delta — and on io_uring the connection still carried the boot-time
    // aof_enabled=false, so that write was never logged at all.
    if let Err(e) = crate::aof::checkpoint_noting(&turn, ctx.store, ctx.index_store, &mut || {}) {
        crate::aof::AOF_ENABLED.store(false, Relaxed);
        return resp_error(&format!("ERR enabling AOF failed: {e}"));
    }
    // A `CONFIG SET appendonly no` took no turn, and may have come in the
    // meantime: then the AOF is off, and saying OK would say otherwise.
    if !crate::aof::AOF_ENABLED.load(Relaxed) {
        return resp_error(
            "ERR enabling AOF failed: the AOF was turned off while it was being turned on",
        );
    }
    resp_simple("OK")
}

/// `CONFIG SET` of one setting.
fn config_set_one(ctx: &mut Ctx, param: &str, value: &str) -> Vec<u8> {
    let param = param.to_string();
    let reply: Option<Vec<u8>> = (|| {
        match param.as_str() {
            "slowlog-log-slower-than" => match value.parse::<i64>() {
                // A negative value turns the slow log off.
                Ok(v) => {
                    ctx.slowlog
                        .set_threshold_us(u64::try_from(v).unwrap_or(u64::MAX));
                    Some(resp_simple("OK"))
                }
                Err(_) => Some(resp_error("ERR value is not an integer or out of range")),
            },
            "slowlog-max-len" => match value.parse::<usize>() {
                Ok(v) => {
                    ctx.slowlog.set_max_len(v);
                    Some(resp_simple("OK"))
                }
                _ => Some(resp_error("ERR value is not an integer or out of range")),
            },
            "acllog-max-len" => match value.parse::<usize>() {
                Ok(v) => {
                    ctx.acl_log.set_max_len(v);
                    Some(resp_simple("OK"))
                }
                _ => Some(resp_error("ERR value is not an integer or out of range")),
            },
            "maxmemory" => match memory_size(value) {
                Some(v) => {
                    crate::RUNTIME_MAXMEMORY.store(v, std::sync::atomic::Ordering::Relaxed);
                    crate::memory_use::limit_changed();
                    crate::eviction::limit_changed();
                    // A limit below what is in use is enforced
                    // now, not at the next write.
                    // Not while writes are paused: eviction waits then
                    // (#424).
                    if v != 0 && !crate::pause::expiry_deferred() {
                        // Off the runtime, as `SAVE` (#605): it may evict a
                        // great many keys.
                        crate::cluster::migration::off_the_runtime(|| {
                            // Each key ordered against the writes to it (#271).
                            let _evicting = crate::write_gate::enter();
                            crate::eviction::make_room(ctx, v, !ctx.apply_replication);
                        });
                    }
                    Some(resp_simple("OK"))
                }
                None => Some(resp_error("ERR argument must be a memory value")),
            },
            "maxmemory-policy" => match crate::eviction::Policy::parse(value) {
                Some(policy) => {
                    crate::eviction::set_policy(policy);
                    Some(resp_simple("OK"))
                }
                None => Some(resp_error(&format!(
                    "ERR CONFIG SET failed (possibly related to argument \
                                 'maxmemory-policy') - argument(s) must be one of the \
                                 following: {}",
                    crate::eviction::POLICY_NAMES
                ))),
            },
            "maxmemory-samples" => match value.parse::<usize>() {
                Ok(n) if (1..=64).contains(&n) => {
                    crate::eviction::set_samples(n);
                    Some(resp_simple("OK"))
                }
                _ => Some(resp_error(
                    "ERR CONFIG SET failed (possibly related to argument \
                                 'maxmemory-samples') - argument must be between 1 and 64 \
                                 inclusive",
                )),
            },
            "lfu-log-factor" => match value.parse::<u32>() {
                Ok(n) => {
                    crate::eviction::set_lfu_log_factor(n);
                    Some(resp_simple("OK"))
                }
                Err(_) => Some(resp_error("ERR value is not an integer or out of range")),
            },
            "lfu-decay-time" => match value.parse::<u32>() {
                Ok(n) => {
                    crate::eviction::set_lfu_decay_time(n);
                    Some(resp_simple("OK"))
                }
                Err(_) => Some(resp_error("ERR value is not an integer or out of range")),
            },
            // Active defrag (#113): see src/compaction.rs.
            "activedefrag" => match value.to_ascii_lowercase().as_str() {
                "yes" | "no" => {
                    crate::compaction::set_enabled(value.eq_ignore_ascii_case("yes"));
                    Some(resp_simple("OK"))
                }
                _ => Some(resp_error(
                    "ERR CONFIG SET failed (possibly related to argument 'activedefrag') - argument must be 'yes' or 'no'",
                )),
            },
            // Giving free memory back once idle (#753): see src/give_back.rs.
            "allocator-give-back" => match value.to_ascii_lowercase().as_str() {
                "yes" | "no" => {
                    crate::give_back::set_enabled(value.eq_ignore_ascii_case("yes"));
                    Some(resp_simple("OK"))
                }
                _ => Some(resp_error(
                    "ERR CONFIG SET failed (possibly related to argument 'allocator-give-back') - argument must be 'yes' or 'no'",
                )),
            },
            "active-defrag-ignore-bytes" => match memory_size(value) {
                Some(bytes) => {
                    crate::compaction::set_ignore_bytes(bytes as u64);
                    Some(resp_simple("OK"))
                }
                None => Some(resp_error(
                    "ERR CONFIG SET failed (possibly related to argument 'active-defrag-ignore-bytes') - argument must be a memory value",
                )),
            },
            "active-defrag-threshold-lower" => match value.parse::<u32>() {
                Ok(percent) if percent <= 1000 => {
                    crate::compaction::set_threshold_lower(percent);
                    Some(resp_simple("OK"))
                }
                _ => Some(resp_error(
                    "ERR CONFIG SET failed (possibly related to argument 'active-defrag-threshold-lower') - argument must be between 0 and 1000 inclusive",
                )),
            },
            "active-defrag-cycle-max" => match value.parse::<u32>() {
                Ok(percent) if (1..=99).contains(&percent) => {
                    crate::compaction::set_cycle_max(percent);
                    Some(resp_simple("OK"))
                }
                _ => Some(resp_error(
                    "ERR CONFIG SET failed (possibly related to argument 'active-defrag-cycle-max') - argument must be between 1 and 99 inclusive",
                )),
            },
            "requirepass" => {
                // Update the default user's password in acl_store — this is the
                // authoritative source for authentication, not config.requirepass.
                // Empty string means nopass (remove password requirement).
                let mut store = ctx.acl_store.write();
                if let Some(default_user) = store.get_mut("default") {
                    if value.is_empty() {
                        default_user.nopass = true;
                        default_user.passwords.clear();
                    } else {
                        default_user.nopass = false;
                        default_user.passwords = vec![crate::acl::sha256_hex(value)];
                    }
                }
                Some(resp_simple("OK"))
            }
            "appendonly" => match value {
                "yes" | "1" | "true" => {
                    if crate::aof::AOF_ENABLED.load(std::sync::atomic::Ordering::Relaxed) {
                        return Some(resp_simple("OK"));
                    }
                    // It waits for the checkpoint's turn, and then writes
                    // the whole dataset out: off the runtime, so that the
                    // other clients are read from meanwhile (as `MIGRATE`
                    // does, see `off_the_runtime`).
                    Some(crate::cluster::migration::off_the_runtime(|| {
                        enable_aof(ctx)
                    }))
                }
                "no" | "0" | "false" => {
                    crate::aof::AOF_ENABLED.store(false, std::sync::atomic::Ordering::Relaxed);
                    Some(resp_simple("OK"))
                }
                _ => Some(resp_error("ERR Invalid argument for CONFIG SET appendonly")),
            },
            "loglevel" => match crate::loglevel::log::parse_level(value) {
                Some(l) => {
                    LOG_LEVEL.store(l, std::sync::atomic::Ordering::Relaxed);
                    Some(resp_simple("OK"))
                }
                None => Some(resp_error(
                    "ERR Invalid log level. \
                                     Valid values: debug, verbose, notice, warning, nothing",
                )),
            },
            // Taken with the command gate held alone (`run_command`): no
            // command that went by the old setting is still running.
            "lua-parallel-scripts" => match value.to_ascii_lowercase().as_str() {
                "yes" => {
                    crate::tag_locks::set_enabled(true);
                    Some(resp_simple("OK"))
                }
                "no" => {
                    crate::tag_locks::set_enabled(false);
                    Some(resp_simple("OK"))
                }
                _ => Some(resp_error("ERR argument must be 'yes' or 'no'")),
            },
            "key-access-tracking" => match value.to_ascii_lowercase().as_str() {
                "yes" => {
                    crate::eviction::set_key_access_tracking(true);
                    Some(resp_simple("OK"))
                }
                "no" => {
                    crate::eviction::set_key_access_tracking(false);
                    Some(resp_simple("OK"))
                }
                _ => Some(resp_error("ERR argument must be 'yes' or 'no'")),
            },
            "protected-mode" => match value.to_ascii_lowercase().as_str() {
                "yes" => {
                    crate::protected_mode::set(true);
                    Some(resp_simple("OK"))
                }
                "no" => {
                    crate::protected_mode::set(false);
                    Some(resp_simple("OK"))
                }
                _ => Some(resp_error("ERR argument must be 'yes' or 'no'")),
            },
            "auto-aof-rewrite-percentage" | "aof-rewrite-percentage" => {
                match value.parse::<u64>() {
                    Ok(v) => {
                        crate::aof::AUTO_REWRITE_PERCENTAGE
                            .store(v, std::sync::atomic::Ordering::Relaxed);
                        Some(resp_simple("OK"))
                    }
                    Err(_) => Some(resp_error("ERR value is not an integer or out of range")),
                }
            }
            "auto-aof-rewrite-min-size" | "aof-rewrite-min-size" => {
                match crate::config::parse_memory(&param, value) {
                    Ok(v) => {
                        crate::aof::AUTO_REWRITE_MIN_SIZE
                            .store(v, std::sync::atomic::Ordering::Relaxed);
                        Some(resp_simple("OK"))
                    }
                    Err(e) => Some(resp_error(&format!("ERR {e}"))),
                }
            }
            // As Redis, both may change while the node runs: peers learn a
            // new hostname from its next heartbeat.
            "cluster-announce-hostname" => {
                if !crate::cluster::valid_hostname(value) {
                    Some(resp_error(
                        "ERR CONFIG SET failed (possibly related to argument \
                         'cluster-announce-hostname') - Hostnames must be less than 256 \
                         characters and only contain alphanumeric characters, '-' or '.'",
                    ))
                } else {
                    ctx.cluster.set_my_hostname(value);
                    let _ = ctx.cluster.save_to_disk();
                    Some(resp_simple("OK"))
                }
            }
            // What clients are told; nodes keep reaching each other at the
            // client port (#110).
            "cluster-announce-client-port" => match value.parse::<u16>() {
                Ok(port) => {
                    ctx.cluster.set_my_announced_client_port(port);
                    Some(resp_simple("OK"))
                }
                Err(_) => Some(resp_error(
                    "ERR argument must be between 0 and 65535 inclusive",
                )),
            },
            "cluster-preferred-endpoint-type" => match crate::cluster::Endpoint::parse(value) {
                Some(endpoint) => {
                    ctx.cluster.set_preferred_endpoint(endpoint);
                    Some(resp_simple("OK"))
                }
                None => Some(resp_error(
                    "ERR CONFIG SET failed (possibly related to argument \
                     'cluster-preferred-endpoint-type') - argument(s) must be one of the \
                     following: ip, hostname, unknown-endpoint",
                )),
            },
            "min-replicas-to-write" | "min-slaves-to-write" => match value.parse::<u32>() {
                Ok(v) => {
                    ctx.repl_state
                        .min_replicas_to_write
                        .store(v, std::sync::atomic::Ordering::Relaxed);
                    Some(resp_simple("OK"))
                }
                Err(_) => Some(resp_error("ERR value is not an integer or out of range")),
            },
            "min-replicas-max-lag" | "min-slaves-max-lag" => match value.parse::<u64>() {
                Ok(v) => {
                    ctx.repl_state
                        .min_replicas_max_lag
                        .store(v, std::sync::atomic::Ordering::Relaxed);
                    Some(resp_simple("OK"))
                }
                Err(_) => Some(resp_error("ERR value is not an integer or out of range")),
            },
            "notify-keyspace-events" => {
                match crate::keyspace_events::parse_ks_flags_strict(value) {
                    Some(new_flags) => {
                        crate::keyspace_events::KS_FLAGS
                            .store(new_flags, std::sync::atomic::Ordering::Relaxed);
                        Some(resp_simple("OK"))
                    }
                    None => Some(resp_error(&format!(
                        "ERR CONFIG SET failed (possibly related to argument \
                     'notify-keyspace-events') - {}",
                        crate::keyspace_events::BAD_FLAGS
                    ))),
                }
            }
            "repl-backlog-size" => match value.parse::<u64>() {
                Ok(v) if v >= 1 => {
                    ctx.repl_state.backlog_size.store(v, Ordering::Relaxed);
                    Some(resp_simple("OK"))
                }
                _ => Some(resp_error(
                    "ERR CONFIG SET failed (possibly related to argument \
                                 'repl-backlog-size') - argument must be a positive integer",
                )),
            },
            "repl-backlog-ttl" => match value.parse::<u64>() {
                Ok(v) => {
                    ctx.repl_state.backlog_ttl_secs.store(v, Ordering::Relaxed);
                    Some(resp_simple("OK"))
                }
                Err(_) => Some(resp_error(
                    "ERR CONFIG SET failed (possibly related to argument \
                                 'repl-backlog-ttl') - argument must be a non-negative integer",
                )),
            },
            "repl-diskless-load" => match crate::replication::flush_before_load(value) {
                Some(flush) => {
                    ctx.repl_state
                        .flush_before_load
                        .store(flush, Ordering::Relaxed);
                    Some(resp_simple("OK"))
                }
                None => Some(resp_error(
                    "ERR CONFIG SET failed (possibly related to argument \
                                 'repl-diskless-load') - argument must be one of: disabled, \
                                 on-empty-db, swapdb, flushdb",
                )),
            },
            // What `INFO replication` tells Sentinel (#443): it used to be
            // remembered for `CONFIG GET` only, and `INFO` said 100 whatever
            // was set, so a replica set to 0 could still be promoted.
            "replica-priority" | "slave-priority" => match value.parse::<i64>() {
                Ok(v) if (0..=i64::from(i32::MAX)).contains(&v) => {
                    ctx.repl_state
                        .replica_priority
                        .store(v as u32, std::sync::atomic::Ordering::Relaxed);
                    Some(resp_simple("OK"))
                }
                Ok(_) => Some(resp_error(
                    "ERR argument must be between 0 and 2147483647 inclusive",
                )),
                Err(_) => Some(resp_error(
                    "ERR argument couldn't be parsed into an integer",
                )),
            },
            "replica-announced" => match value.to_ascii_lowercase().as_str() {
                yes_no @ ("yes" | "no") => {
                    ctx.repl_state
                        .replica_announced
                        .store(yes_no == "yes", std::sync::atomic::Ordering::Relaxed);
                    Some(resp_simple("OK"))
                }
                _ => Some(resp_error("ERR argument must be 'yes' or 'no'")),
            },
            "repl-timeout" => match value.parse::<u64>() {
                Ok(v) if v >= 1 => {
                    ctx.repl_state
                        .repl_timeout_secs
                        .store(v, std::sync::atomic::Ordering::Relaxed);
                    Some(resp_simple("OK"))
                }
                _ => Some(resp_error(
                    "ERR CONFIG SET failed (possibly related to argument \
                                 'repl-timeout') - argument must be a positive integer",
                )),
            },
            // Read for each write the hub queues: a change applies to the
            // replicas already attached (#305). 0 is no limit, as
            // `client-output-buffer-limit replica 0 0 0` sets it; and any
            // value that sets, this takes back, so that `CONFIG GET` reads
            // back through `CONFIG SET` (and a failed `CONFIG SET` of several
            // settings puts it back) (#774).
            "repl-replica-max-backlog-bytes" | "repl-replica-sync-max-backlog-bytes" => {
                match memory_size(value) {
                    Some(bytes) => {
                        let limit = if param == "repl-replica-max-backlog-bytes" {
                            &ctx.repl_state.replica_max_backlog_bytes
                        } else {
                            &ctx.repl_state.replica_sync_max_backlog_bytes
                        };
                        limit.store(bytes as u64, Ordering::Relaxed);
                        Some(resp_simple("OK"))
                    }
                    None => Some(resp_error(&format!(
                        "ERR CONFIG SET failed (possibly related to argument '{param}') - \
                         argument must be a memory value"
                    ))),
                }
            }
            // `<class> <hard> <soft> <seconds>`, for one class or more (#774).
            // Normal and subscribed clients' limits are the hub's, read as
            // each reply or message is queued; a replica's are the
            // replication's, read for each write the hub queues, its hard
            // limit being `repl-replica-max-backlog-bytes`.
            "client-output-buffer-limit" => match crate::output_limits::parse(value) {
                Ok(classes) => {
                    for (class, limit) in classes {
                        if class == crate::output_limits::Class::Replica {
                            let rs = &ctx.repl_state;
                            rs.replica_max_backlog_bytes
                                .store(limit.hard, Ordering::Relaxed);
                            rs.replica_soft_limit_bytes
                                .store(limit.soft, Ordering::Relaxed);
                            rs.replica_soft_limit_secs
                                .store(limit.soft_seconds, Ordering::Relaxed);
                        } else {
                            ctx.pubsub.limits().set(class, limit);
                        }
                    }
                    Some(resp_simple("OK"))
                }
                Err(why) => Some(resp_error(&format!(
                    "ERR CONFIG SET failed (possibly related to argument \
                     'client-output-buffer-limit') - {why}"
                ))),
            },
            // Read when a shutdown begins to wait for the replicas.
            "shutdown-timeout" => match value.parse::<u64>() {
                Ok(v) if v <= i32::MAX as u64 => {
                    ctx.repl_state
                        .shutdown_timeout_secs
                        .store(v, std::sync::atomic::Ordering::Relaxed);
                    Some(resp_simple("OK"))
                }
                _ => Some(resp_error(
                    "ERR CONFIG SET failed (possibly related to argument \
                                 'shutdown-timeout') - argument must be between 0 and 2147483647 inclusive",
                )),
            },
            "repl-ping-replica-period" | "repl-ping-slave-period" => match value.parse::<u64>() {
                Ok(v) => {
                    ctx.repl_state
                        .repl_ping_period_secs
                        .store(v, std::sync::atomic::Ordering::Relaxed);
                    Some(resp_simple("OK"))
                }
                Err(_) => Some(resp_error("ERR value is not an integer or out of range")),
            },
            "replica-full-sync-guard" => match crate::replication::FullSyncGuard::parse(value) {
                Some(guard) => {
                    ctx.repl_state.set_full_sync_guard(guard);
                    log_info!(
                        "Replication: replica-full-sync-guard set to {}",
                        guard.as_str()
                    );
                    Some(resp_simple("OK"))
                }
                None => Some(resp_error(
                    "ERR CONFIG SET failed (possibly related to argument \
                                     'replica-full-sync-guard') - argument must be one of: \
                                     off, empty, shrink",
                )),
            },
            "replica-full-sync-guard-ratio" => match value.parse::<u64>() {
                Ok(v) if v <= 100 => {
                    ctx.repl_state
                        .full_sync_guard_ratio
                        .store(v, std::sync::atomic::Ordering::Relaxed);
                    Some(resp_simple("OK"))
                }
                _ => Some(resp_error(
                    "ERR CONFIG SET failed (possibly related to argument \
                                 'replica-full-sync-guard-ratio') - argument must be \
                                 between 0 and 100",
                )),
            },
            "masterauth" | "primaryauth" => {
                ctx.repl_state.set_master_auth(value);
                Some(resp_simple("OK"))
            }
            "masteruser" | "primaryuser" => {
                ctx.repl_state.set_master_user(value);
                Some(resp_simple("OK"))
            }
            // Redis's ranges: an entry keeps at least the command and one
            // argument, and at least one byte of each.
            "slowlog-entry-max-argc" => match value.parse::<i64>() {
                Ok(v) if (2..=i32::MAX as i64).contains(&v) => {
                    ctx.slowlog.set_entry_max_argc(v as usize);
                    Some(resp_simple("OK"))
                }
                Ok(_) => Some(resp_error(
                    "ERR argument must be between 2 and 2147483647 inclusive",
                )),
                Err(_) => Some(resp_error(
                    "ERR argument couldn't be parsed into an integer",
                )),
            },
            "slowlog-entry-max-string-len" => match value.parse::<i64>() {
                Ok(v) if v >= 1 => {
                    ctx.slowlog.set_entry_max_string_len(v as usize);
                    Some(resp_simple("OK"))
                }
                Ok(_) => Some(resp_error(
                    "ERR argument must be between 1 and 9223372036854775807 inclusive",
                )),
                Err(_) => Some(resp_error(
                    "ERR argument couldn't be parsed into an integer",
                )),
            },
            // As in Redis, this node's address may change while it runs:
            // peers take the new one from its next heartbeat, and
            // nodes.conf is saved (#391).
            "cluster-announce-ip" => match announce_ip_error(value) {
                Some(why) => Some(resp_error(&format!(
                    "ERR CONFIG SET failed (possibly related to argument \
                     'cluster-announce-ip') - {why}"
                ))),
                None => announce_reply(ctx.cluster.set_announce(Some(value), None, None)),
            },
            "cluster-announce-port" | "cluster-announce-bus-port" => {
                let port = match value.parse::<i64>() {
                    Ok(port) => u16::try_from(port)
                        .map_err(|_| "argument must be between 0 and 65535 inclusive"),
                    Err(_) => Err("argument couldn't be parsed into an integer"),
                };
                match port {
                    Ok(port) if param == "cluster-announce-port" => {
                        announce_reply(ctx.cluster.set_announce(None, Some(port), None))
                    }
                    Ok(port) => announce_reply(ctx.cluster.set_announce(None, None, Some(port))),
                    Err(why) => Some(resp_error(&format!(
                        "ERR CONFIG SET failed (possibly related to argument '{param}') - {why}"
                    ))),
                }
            }
            "cluster-require-full-coverage" => match value.to_ascii_lowercase().as_str() {
                "yes" => {
                    ctx.cluster.set_require_full_coverage(true);
                    Some(resp_simple("OK"))
                }
                "no" => {
                    ctx.cluster.set_require_full_coverage(false);
                    Some(resp_simple("OK"))
                }
                _ => Some(resp_error(
                    "ERR CONFIG SET failed (possibly related to argument \
                     'cluster-require-full-coverage') - argument must be 'yes' or 'no'",
                )),
            },
            // Fixed at startup: changing them under a running
            // cluster would desynchronise this node from what its
            // peers and its own nodes.conf already say.
            // `cluster-node-timeout` too, unlike in Redis: the failure
            // detector, elections and the bus's timers take it once,
            // at startup (docs/COMPATIBILITY.md).
            "cluster-enabled"
            | "cluster-config-file"
            | "cluster-port"
            | "cluster-bus-port-protected-mode"
            | "cluster-bus-secret"
            | "tls-cluster"
            | "cluster-node-timeout" => {
                // To what it already is, is no change.
                let unchanged = config_rows(ctx)
                    .iter()
                    .any(|(name, current)| *name == param && current == value);
                if unchanged {
                    Some(resp_simple("OK"))
                } else if param == "cluster-node-timeout" {
                    Some(resp_error(
                        "ERR CONFIG SET failed (possibly related to argument \
                         'cluster-node-timeout') - can't set immutable config: \
                         KiviDB reads cluster-node-timeout only at startup; set it in \
                         the config file or with --cluster-node-timeout and restart",
                    ))
                } else {
                    Some(resp_error(&format!(
                        "ERR CONFIG SET failed (possibly related to argument \
                                     '{param}') - can't set immutable config"
                    )))
                }
            }
            // Read at the start of each slot migration's handoff: a
            // change applies to the next one (#111).
            "cluster-slot-migration-handoff-max-lag-bytes" => match memory_size(value) {
                Some(bytes) if bytes <= i64::MAX as usize => {
                    crate::cluster::migration::set_handoff_max_lag_bytes(bytes as u64);
                    Some(resp_simple("OK"))
                }
                _ => Some(resp_error(
                    "ERR CONFIG SET failed (possibly related to argument \
                                 'cluster-slot-migration-handoff-max-lag-bytes') - argument must be a memory value",
                )),
            },
            "cluster-slot-migration-write-pause-timeout" => match value.parse::<i64>() {
                Ok(ms) if ms >= 0 => {
                    crate::cluster::migration::set_write_pause_timeout_ms(ms as u64);
                    Some(resp_simple("OK"))
                }
                _ => Some(resp_error(
                    "ERR CONFIG SET failed (possibly related to argument \
                                 'cluster-slot-migration-write-pause-timeout') - argument must be between 0 and 9223372036854775807 inclusive",
                )),
            },
            "cluster-replica-validity-factor" => match value.parse::<i64>() {
                Ok(factor) if factor >= 0 => {
                    ctx.cluster.set_replica_validity_factor(factor as u64);
                    Some(resp_simple("OK"))
                }
                _ => Some(resp_error(
                    "ERR argument must be between 0 and 9223372036854775807 inclusive",
                )),
            },
            "tracking-table-max-keys" => match value.parse::<usize>() {
                Ok(max_keys) => {
                    let dropped = ctx.tracking_table.set_max_keys(max_keys);
                    crate::tracking::tell_dropped(
                        ctx.client_registry,
                        dropped,
                        Some(ctx.client_id),
                    );
                    Some(resp_simple("OK"))
                }
                Err(_) => Some(resp_error("ERR value is not an integer or out of range")),
            },
            // Remembered, as below, but only a value Redis would take: a
            // `CONFIG SET` with several settings is refused whole for one
            // bad value (#24).
            // Listen there instead (`listen`): refused, with nothing
            // changed, if the port cannot be had.
            // 0 closes the plaintext port, as in Redis; connections already
            // open stay open, as they do in Redis. Redis is then reached on
            // its TLS port or a Unix socket; there is no Unix socket here, so
            // 0 needs the TLS port open.
            "port" => match value.parse::<u16>() {
                Ok(port) => {
                    let server = crate::listen::server_of(ctx.config);
                    let tls = crate::listen::current_port(server, crate::listen::Kind::Tls);
                    if port == 0 && tls == 0 {
                        Some(resp_error(
                            "ERR port 0 turns the plaintext port off, which leaves no port to \
                             reach this server on: open the TLS port (tls-port) first",
                        ))
                    } else if port == 0 && ctx.config.cluster_enabled && !ctx.config.tls_cluster {
                        Some(resp_error(
                            "ERR a cluster node without tls-cluster is known by its plaintext \
                             port: it cannot be closed",
                        ))
                    } else {
                        match crate::listen::move_to(server, crate::listen::Kind::Plain, port) {
                            Ok(()) => Some(resp_simple("OK")),
                            Err(e) => Some(resp_error(&format!(
                                "ERR Unable to listen on this port: {e}"
                            ))),
                        }
                    }
                }
                Err(_) => Some(resp_error(
                    "ERR argument must be between 0 and 65535 inclusive",
                )),
            },
            // Open, move or close the TLS port: see `config_set_tls_port`.
            "tls-port" => match value.parse::<u16>() {
                Ok(port) => Some(config_set_tls_port(ctx, port)),
                Err(_) => Some(resp_error(
                    "ERR argument must be between 0 and 65535 inclusive",
                )),
            },
            // From the replica's next connection to its master on; a link
            // that is up stays as it is. `tls-cluster` implies it, and is
            // fixed at startup.
            "tls-replication" => {
                let on = match value.to_ascii_lowercase().as_str() {
                    "yes" => true,
                    "no" => false,
                    _ => return Some(resp_error("ERR argument must be 'yes' or 'no'")),
                };
                Some(config_set_tls_replication(ctx, on))
            }
            // Redis's ranges: 1 second to a day, 1 to 10000 entries.
            "stream-idmp-duration" | "stream-idmp-maxsize" => {
                let (slot, max) = if param == "stream-idmp-duration" {
                    (
                        &crate::commands::stream_extra::IDMP_DEFAULT_DURATION,
                        86_400,
                    )
                } else {
                    (&crate::commands::stream_extra::IDMP_DEFAULT_MAXSIZE, 10_000)
                };
                match value.parse::<i64>() {
                    Ok(n) if (1..=max).contains(&n) => {
                        slot.store(n as u64, std::sync::atomic::Ordering::Relaxed);
                        Some(resp_simple("OK"))
                    }
                    Ok(_) => Some(resp_error(&format!(
                        "ERR argument must be between 1 and {max} inclusive"
                    ))),
                    Err(_) => Some(resp_error(
                        "ERR argument couldn't be parsed into an integer",
                    )),
                }
            }
            // Seconds; 0, the default, is off.
            "backup-sealed-ttl" => match value.parse::<i64>() {
                Ok(n) if n >= 0 => {
                    crate::commands::backup::set_sealed_ttl(n as u64);
                    Some(resp_simple("OK"))
                }
                Ok(_) => Some(resp_error(
                    "ERR argument must be between 0 and 9223372036854775807 inclusive",
                )),
                Err(_) => Some(resp_error(
                    "ERR argument couldn't be parsed into an integer",
                )),
            },
            // More clients only if the open-file limit can be raised for
            // them, as in Redis; set below in `config_set_global`.
            "maxclients" => {
                let n = value
                    .parse::<u32>()
                    .ok()
                    .filter(|&n| n as usize > crate::client_registry::max_clients())?;
                match crate::client_registry::clients_that_fit(n as usize) {
                    Ok(fits) if fits == n as usize => None,
                    Ok(fits) => Some(resp_error(&format!(
                        "ERR The operating system is not able to handle the specified \
                         number of clients, try with {fits}"
                    ))),
                    Err(e) => Some(resp_error(&format!(
                        "ERR Can't read the open-file limit: {e}"
                    ))),
                }
            }
            // A per-server setting, so it is handled here: `config_set_global`
            // has no context to store it in.
            "hll-write-format" => {
                let v = value.to_ascii_lowercase();
                if v != "legacy" && v != "redis" {
                    return Some(resp_error(
                        "ERR CONFIG SET failed (possibly related to argument 'hll-write-format') - argument(s) must be one of the following: legacy redis",
                    ));
                }
                ctx.repl_state.set_hll_write_redis(v == "redis");
                Some(resp_simple("OK"))
            }
            // What a running Redis cannot be told either.
            "daemonize" | "databases" | "pidfile" | "logfile" | "log-format" | "unixsocket"
            | "supervised" | "io-threads" | "tcp-backlog" | "set-proc-title" | "appendfilename"
            | "appenddirname" | "aclfile" => {
                let unchanged = config_rows(ctx)
                    .iter()
                    .any(|(name, current)| *name == param && current == value);
                if unchanged {
                    Some(resp_simple("OK"))
                } else {
                    Some(resp_error(&format!(
                        "ERR CONFIG SET failed (possibly related to argument \
                                     '{param}') - can't set immutable config"
                    )))
                }
            }
            _ => None,
        }
    })();
    let reply = reply
        .or_else(|| config_set_global(&param, value))
        .unwrap_or_else(|| config_set_other(ctx, &param, value));
    if reply.starts_with(b"+") {
        let secret = crate::slowlog::SECRET_SETTINGS.contains(&param.as_str());
        log_info!(
            "CONFIG SET {param} {}",
            if secret { "(hidden)" } else { value }
        );
    }
    reply
}

/// What `CONFIG SET latency-tracking-info-percentiles` refuses a value with
/// (#255). Redis's two messages, word for word: clients and tests compare
/// them.
const PERCENTILES_NOT_NUMBERS: &str = "Invalid latency-tracking-info-percentiles parameters";
const PERCENTILES_OUT_OF_RANGE: &str =
    "latency-tracking-info-percentiles parameters should sit between [0.0,100.0]";

/// One percentile as Redis reads it (with `strtod`): a decimal, with or
/// without an exponent (`99.9`, `1e1`), `inf`, or a hexadecimal integer
/// (`0x10`). Anything else is not a number, and neither is NaN or a value
/// too large for a double (`1e999`). A hexadecimal fraction or exponent
/// (`0x1p3`) is not read.
fn parse_percentile(word: &str) -> Option<f64> {
    let (negative, digits) = match word.as_bytes().first()? {
        b'-' => (true, &word[1..]),
        b'+' => (false, &word[1..]),
        _ => (false, word),
    };
    let magnitude = match digits
        .strip_prefix("0x")
        .or_else(|| digits.strip_prefix("0X"))
    {
        Some(hex) if !hex.is_empty() && hex.bytes().all(|b| b.is_ascii_hexdigit()) => {
            hex.bytes().fold(0.0, |sum, b| {
                sum * 16.0 + (b as char).to_digit(16).map_or(0.0, f64::from)
            })
        }
        Some(_) => return None,
        None => {
            // A second sign ("--5", "+-5") is not a number, though Rust's
            // parser would take one more.
            if digits.starts_with(['+', '-']) {
                return None;
            }
            let value: f64 = digits.parse().ok()?;
            if value.is_nan()
                || (value.is_infinite() && !digits.to_ascii_lowercase().contains("inf"))
            {
                return None;
            }
            // An underflow to zero ("1e-400") is refused too, as `string2d`
            // refuses it: a nonzero digit before the exponent, and zero.
            let mantissa = digits.split(['e', 'E']).next().unwrap_or_default();
            if value == 0.0 && mantissa.bytes().any(|b| (b'1'..=b'9').contains(&b)) {
                return None;
            }
            value
        }
    };
    Some(if negative { -magnitude } else { magnitude })
}

/// A list of percentiles as Redis's `CONFIG GET` writes it: each number with
/// `%f` (six decimals), then the trailing zeros and a trailing `.` taken off,
/// so `50.0` reads back as `50` and `99.90` as `99.9`.
pub(crate) fn format_latency_percentiles(percentiles: &[f64]) -> String {
    percentiles
        .iter()
        .map(|p| {
            let fixed = format!("{p:.6}");
            fixed
                .trim_end_matches('0')
                .trim_end_matches('.')
                .to_string()
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// The percentiles of a `latency-tracking-info-percentiles` value: numbers
/// from 0 to 100 between single spaces, or nothing at all. The words are
/// checked in order, each for being a number and then for its range, as in
/// Redis, so an extra space (an empty word) is a word that is not a number.
pub(crate) fn parse_latency_percentiles(value: &str) -> Result<Vec<f64>, &'static str> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    value
        .split(' ')
        .map(|word| {
            let percentile = parse_percentile(word).ok_or(PERCENTILES_NOT_NUMBERS)?;
            if (0.0..=100.0).contains(&percentile) {
                Ok(percentile)
            } else {
                Err(PERCENTILES_OUT_OF_RANGE)
            }
        })
        .collect()
}

/// `CONFIG SET` of a setting that is the server's as a whole, not of a
/// connection, a store or the replication: what a line of the configuration
/// file with it does too (#142). `None`: not one of these.
pub(crate) fn config_set_global(param: &str, value: &str) -> Option<Vec<u8>> {
    let param = param.to_string();
    let remembered = REMEMBERED_CONFIG.lock().contains_key(param.as_str())
        || crate::redis_settings::is_hidden(&param);
    (|| {
        match param.as_str() {
            "array-slice-size" | "array-sparse-kmax" | "array-sparse-kmin" => {
                Some(config_set_array(&param, value))
            }
            "bf-error-rate"
            | "bf-initial-size"
            | "bf-expansion-factor"
            | "bloom-max-filter-bytes" => Some(crate::commands::bf::config_set(&param, value)),
            "stream-node-max-entries" => match value.parse::<i64>() {
                Ok(v) if v >= 0 => {
                    crate::commands::stream_extra::NODE_MAX_ENTRIES
                        .store(v, std::sync::atomic::Ordering::Relaxed);
                    Some(resp_simple("OK"))
                }
                _ => Some(resp_error(
                    "ERR CONFIG SET failed (possibly related to argument \
                                 'stream-node-max-entries') - argument must be between 0 and \
                                 9223372036854775807 inclusive",
                )),
            },
            // When the AOF is synced (#513): under `always`, before each
            // reply to a write, as in Redis. `no` is taken as `everysec`.
            "appendfsync" => match value.to_ascii_lowercase().as_str() {
                policy @ ("always" | "everysec" | "no") => {
                    if policy == "no" {
                        log_warn!(
                            "'appendfsync no' syncs the append-only file once a second, as \
                             'everysec' does"
                        );
                    }
                    crate::aof::set_fsync_always(policy == "always");
                    REMEMBERED_CONFIG
                        .lock()
                        .insert(param.clone(), policy.to_string());
                    Some(resp_simple("OK"))
                }
                _ => Some(resp_error(
                    "ERR CONFIG SET failed (possibly related to argument 'appendfsync') - \
                     argument(s) must be one of the following: always, everysec, no",
                )),
            },
            // Whether a reply to a write waits for its AOF record (#147).
            "aof-ack-after-write" => match value.to_ascii_lowercase().as_str() {
                "yes" | "no" => {
                    crate::aof::set_ack_after_write(value.eq_ignore_ascii_case("yes"));
                    REMEMBERED_CONFIG
                        .lock()
                        .insert(param.clone(), value.to_ascii_lowercase());
                    Some(resp_simple("OK"))
                }
                _ => Some(resp_error("ERR argument must be 'yes' or 'no'")),
            },
            // Settings of Redis's that take yes or no: remembered, and in
            // effect where KiviDB has what they set (`lazyfree-lazy-expire`
            // decides whether a key that expires is propagated as `UNLINK`
            // or `DEL`; `lazyfree-lazy-user-del`, whether `DEL` frees a large
            // value in the background, as `UNLINK` does (#754);
            // `latency-tracking`, whether `INFO latencystats`
            // reports; `rdbchecksum no`, that an RDB file's checksum is not
            // checked as it is loaded). Anything else is refused, as Redis
            // refuses it.
            "lazyfree-lazy-eviction"
            | "lazyfree-lazy-expire"
            | "lazyfree-lazy-server-del"
            | "lazyfree-lazy-user-del"
            | "lazyfree-lazy-user-flush"
            | "latency-tracking"
            | "rdbchecksum" => match value.to_ascii_lowercase().as_str() {
                yes_no @ ("yes" | "no") => {
                    REMEMBERED_CONFIG
                        .lock()
                        .insert(param.clone(), yes_no.to_string());
                    Some(resp_simple("OK"))
                }
                _ => Some(resp_error(&format!(
                    "ERR CONFIG SET failed (possibly related to argument '{param}') - argument \
                     must be 'yes' or 'no'"
                ))),
            },
            // The policy of a series made without one (#127).
            "ts-duplicate-policy" => match crate::types::DuplicatePolicy::parse(value) {
                Some(policy) => {
                    crate::types::DuplicatePolicy::set_server_default(&policy);
                    REMEMBERED_CONFIG
                        .lock()
                        .insert(param.clone(), policy.as_str().to_ascii_lowercase());
                    Some(resp_simple("OK"))
                }
                None => Some(resp_error(
                    "ERR CONFIG SET failed (possibly related to argument 'ts-duplicate-policy') \
                     - argument(s) must be one of the following: block, first, last, min, max, sum",
                )),
            },
            // How long FT.SEARCH and FT.AGGREGATE may run, and what running
            // out means: `FT.CONFIG`'s `TIMEOUT` and `ON_TIMEOUT` under
            // Redis 8's names. One value, kept by `FT.CONFIG`, whichever
            // name sets it.
            "search-timeout" => match value.parse::<i64>() {
                Ok(ms)
                    if ms >= 0
                        && crate::commands::search_tools::set_config(
                            "TIMEOUT",
                            &ms.to_string(),
                        ) =>
                {
                    Some(resp_simple("OK"))
                }
                _ => Some(resp_error(
                    "ERR argument must be between 0 and 9223372036854775807 inclusive",
                )),
            },
            // The shortest word whose stem is indexed: `FT.CONFIG
            // MINSTEMLEN`.
            "search-min-stem-len" => match value.parse::<u64>() {
                Ok(len)
                    if (2..=u64::from(u32::MAX)).contains(&len)
                        && crate::commands::search_tools::set_config(
                            "MINSTEMLEN",
                            &len.to_string(),
                        ) =>
                {
                    Some(resp_simple("OK"))
                }
                _ => Some(resp_error(
                    "ERR CONFIG SET failed (possibly related to argument 'search-min-stem-len') \
                     - argument must be between 2 and 4294967295 inclusive",
                )),
            },
            "search-on-timeout" => {
                let policy = value.to_ascii_lowercase();
                if matches!(policy.as_str(), "return" | "fail" | "return_strict")
                    && crate::commands::search_tools::set_config("ON_TIMEOUT", &policy)
                {
                    Some(resp_simple("OK"))
                } else {
                    Some(resp_error(
                        "ERR CONFIG SET failed (possibly related to argument 'search-on-timeout') \
                         - argument(s) must be one of the following: return, fail, return_strict",
                    ))
                }
            }
            "latency-monitor-threshold" => match value.parse::<i64>() {
                Ok(ms) if ms >= 0 => {
                    crate::latency::set_threshold_ms(ms as u64);
                    REMEMBERED_CONFIG
                        .lock()
                        .insert(param.clone(), ms.to_string());
                    Some(resp_simple("OK"))
                }
                _ => Some(resp_error(
                    "ERR argument must be between 0 and 9223372036854775807 inclusive",
                )),
            },
            // The percentiles `INFO latencystats` reports (#255): remembered
            // when they are numbers from 0 to 100 (or none), refused with
            // Redis's message when not. The configuration file goes through
            // here too.
            "latency-tracking-info-percentiles" => match parse_latency_percentiles(value) {
                Ok(percentiles) => {
                    // Remembered as Redis's `CONFIG GET` gives it back, not as
                    // given: `0x10 1e1` reads back as `16 10`.
                    REMEMBERED_CONFIG
                        .lock()
                        .insert(param.clone(), format_latency_percentiles(&percentiles));
                    Some(resp_simple("OK"))
                }
                Err(why) => Some(resp_error(&format!(
                    "ERR CONFIG SET failed (possibly related to argument '{param}') - {why}"
                ))),
            },
            "stream-node-max-bytes" => match memory_size(value) {
                Some(v) if v <= i64::MAX as usize => {
                    crate::commands::stream_extra::NODE_MAX_BYTES
                        .store(v as i64, std::sync::atomic::Ordering::Relaxed);
                    Some(resp_simple("OK"))
                }
                _ => Some(resp_error(
                    "ERR CONFIG SET failed (possibly related to argument \
                                 'stream-node-max-bytes') - argument must be a memory value",
                )),
            },
            // Redis's name for it since 7.0; the old one is kept.
            "lua-time-limit" | "busy-reply-threshold" => {
                #[cfg(feature = "lua")]
                {
                    match value.parse::<u64>() {
                        Ok(v) => {
                            crate::commands::lua::lua_time_limit_ms_set(v);
                            Some(resp_simple("OK"))
                        }
                        Err(_) => Some(resp_error("ERR value is not an integer or out of range")),
                    }
                }
                // Nothing runs to a limit here: remembered, for `CONFIG
                // GET`.
                #[cfg(not(feature = "lua"))]
                {
                    match value.parse::<u64>() {
                        Ok(v) => {
                            REMEMBERED_CONFIG
                                .lock()
                                .insert("busy-reply-threshold".to_string(), v.to_string());
                            Some(resp_simple("OK"))
                        }
                        Err(_) => Some(resp_error("ERR value is not an integer or out of range")),
                    }
                }
            }
            "maxclients" => match value.parse::<u32>() {
                Ok(n) if n >= 1 => {
                    crate::client_registry::set_max_clients(n as usize);
                    REMEMBERED_CONFIG
                        .lock()
                        .insert(param.clone(), n.to_string());
                    Some(resp_simple("OK"))
                }
                _ => Some(resp_error(
                    "ERR argument must be between 1 and 4294967295 inclusive",
                )),
            },
            "maxmemory-clients" => {
                let value = match value.strip_suffix('%') {
                    Some(percent) => match percent.parse::<u64>() {
                        Ok(n) if n <= 100 => value.to_string(),
                        Ok(_) => {
                            return Some(resp_error(
                                "ERR percentage argument must be less or equal to 100",
                            ));
                        }
                        Err(_) => return Some(resp_error(NOT_MEMORY_OR_PERCENT)),
                    },
                    None => match memory_size(value) {
                        Some(bytes) => bytes.to_string(),
                        None => return Some(resp_error(NOT_MEMORY_OR_PERCENT)),
                    },
                };
                REMEMBERED_CONFIG.lock().insert(param.clone(), value);
                Some(resp_simple("OK"))
            }
            "client-query-buffer-limit" => match memory_size(value) {
                Some(bytes) if (1 << 20..=i64::MAX as usize).contains(&bytes) => {
                    REMEMBERED_CONFIG
                        .lock()
                        .insert(param.clone(), bytes.to_string());
                    Some(resp_simple("OK"))
                }
                Some(_) => Some(resp_error(
                    "ERR argument must be between 1048576 and 9223372036854775807 inclusive",
                )),
                None => Some(resp_error("ERR argument must be a memory value")),
            },
            // A setting of Redis's that means nothing here — how
            // it encodes small values, how it sizes its buffers.
            // It is accepted, so that a client or a tool that
            // sets it does not fail, and remembered, so that
            // `CONFIG GET` answers with what was set.
            _ if remembered => {
                REMEMBERED_CONFIG
                    .lock()
                    .insert(param.clone(), value.to_string());
                Some(resp_simple("OK"))
            }
            _ => None,
        }
    })()
}

/// `CONFIG SET` of a name no arm of [`config_set_one`] or
/// [`config_set_global`] takes (#143).
fn config_set_other(ctx: &Ctx, param: &str, value: &str) -> Vec<u8> {
    let immutable = || {
        resp_error(&format!(
            "ERR CONFIG SET failed (possibly related to argument '{param}') - can't set \
             immutable config"
        ))
    };
    // One `CONFIG GET` reports, read only at startup
    // (`repl-replica-max-backlog-bytes`, `backupdirname`, ...): to what it
    // already is, is no change.
    if let Some((_, current)) = config_rows(ctx).into_iter().find(|(name, _)| name == param) {
        return if current == value {
            resp_simple("OK")
        } else {
            immutable()
        };
    }
    // A command-line flag (`threads`, `io-model`, `aof`, ...).
    if is_startup_flag(param) {
        return immutable();
    }
    // A cluster setting this server does not have
    // (`cluster-allow-reads-when-down`, ...) would change how the cluster
    // behaves, so taking it and doing nothing would mislead whoever set it
    // (#111); a name that is not Redis's either is most likely misspelt.
    // Both are refused, as Redis refuses a setting it does not know.
    if param.starts_with("cluster-") || !crate::redis_settings::is_redis_setting(param) {
        return resp_error(&format!(
            "ERR Unknown option or number of arguments for CONFIG SET - '{param}'"
        ));
    }
    // Any other setting of Redis's: remembered, as above.
    remember_config(param, value);
    resp_simple("OK")
}

/// Whether `name` is one of the server's command-line flags.
fn is_startup_flag(name: &str) -> bool {
    use clap::CommandFactory;
    crate::config::Config::command()
        .get_arguments()
        .any(|arg| arg.get_long() == Some(name))
}

/// Remember a setting of Redis's that means nothing here (see
/// [`REMEMBERED_CONFIG`]).
pub(crate) fn remember_config(name: &str, value: &str) {
    REMEMBERED_CONFIG
        .lock()
        .insert(name.to_string(), value.to_string());
}

/// What TLS was set up from before a `CONFIG SET`, to put back.
#[cfg(feature = "tls")]
struct TlsBefore {
    listener: Option<crate::tls::acceptor::Files>,
    nodes: Option<crate::tls::acceptor::Files>,
}

/// `CONFIG SET` of TLS's files and `tls-auth-clients` (#156). The
/// certificate, key and CA files are read again, as Redis reads them, also
/// when a path is the same (a file renewed in place), and every handshake
/// from then on, from clients and between nodes, uses them; connections
/// already open keep theirs. Several are taken together, so that a
/// certificate and its key can change in one `CONFIG SET`. If they cannot be
/// loaded, nothing changes.
///
/// While the TLS port is closed they are loaded only once both the
/// certificate and the key are set, so that one can be set before the
/// other; `CONFIG SET tls-port` loads them when it opens the port.
#[cfg(feature = "tls")]
fn config_set_tls(ctx: &Ctx, changes: &[(String, String)]) -> Result<TlsBefore, Vec<u8>> {
    use crate::tls::acceptor;
    let first = &changes[0].0;
    let failed = |why: &str| {
        resp_error(&format!(
            "ERR CONFIG SET failed (possibly related to argument '{first}') - {why}"
        ))
    };
    if let Some((_, value)) = changes.iter().find(|(name, _)| name == "tls-auth-clients")
        && !crate::config::TLS_AUTH_CLIENTS.contains(&value.to_ascii_lowercase().as_str())
    {
        return Err(failed(
            "argument(s) must be one of the following: no, yes, optional",
        ));
    }
    // Written as `CONFIG GET` gives it back: lower case, one space apart.
    let allowed_names = match changes
        .iter()
        .find(|(name, _)| name == "tls-auth-clients-allowed-names")
    {
        Some((_, value)) => match crate::config::allowed_names(value) {
            Ok(names) => names.join(" "),
            Err(e) => {
                return Err(resp_error(&format!(
                    "ERR CONFIG SET failed (possibly related to argument \
                     'tls-auth-clients-allowed-names') - {e}"
                )));
            }
        },
        None => String::new(),
    };
    let server = crate::listen::server_of(ctx.config);
    let before = TlsBefore {
        listener: Some(acceptor::files(server).unwrap_or_else(|| acceptor::Files::of(ctx.config))),
        nodes: crate::node_tls::files(),
    };
    let changed = |files: &Option<acceptor::Files>| {
        files.clone().map(|mut files| {
            for (name, value) in changes {
                let value = match name.as_str() {
                    "tls-auth-clients" => value.to_ascii_lowercase(),
                    "tls-auth-clients-allowed-names" => allowed_names.clone(),
                    _ => value.clone(),
                };
                files.set(name, &value);
            }
            files
        })
    };
    let unable = |e: String| {
        log_warn!("TLS: CONFIG SET {first}: {e}");
        failed("Unable to update TLS configuration. Check server logs.")
    };
    if let Some(files) = changed(&before.listener) {
        let open = crate::listen::current_port(server, crate::listen::Kind::Tls) != 0;
        if open || (!files.cert.is_empty() && !files.key.is_empty()) {
            acceptor::install(server, &files).map_err(unable)?;
        } else {
            acceptor::keep(server, &files);
        }
    }
    if let Some(files) = changed(&before.nodes)
        && let Err(e) = crate::node_tls::reload(files)
    {
        if let Some(files) = &before.listener {
            restore_tls_files(server, files);
        }
        return Err(unable(e));
    }
    for (name, value) in changes {
        log_info!("CONFIG SET {name} {value}");
    }
    log_info!("TLS: certificates loaded again");
    Ok(before)
}

/// Put `files` back as what `server`'s TLS is set up from: loaded, if
/// they can be, as they were.
#[cfg(feature = "tls")]
fn restore_tls_files(server: crate::listen::Server, files: &crate::tls::acceptor::Files) {
    if crate::tls::acceptor::install(server, files).is_err() {
        crate::tls::acceptor::keep(server, files);
    }
}

/// Put TLS back as it was before [`config_set_tls`].
#[cfg(feature = "tls")]
fn config_set_tls_undo(ctx: &Ctx, before: TlsBefore) {
    if let Some(files) = before.listener {
        restore_tls_files(crate::listen::server_of(ctx.config), &files);
    }
    if let Some(files) = before.nodes {
        let _ = crate::node_tls::reload(files);
    }
}

/// `CONFIG SET tls-port`: open the TLS port, as Redis does, also on a
/// server that started without one; move it; or close it with 0. Opening
/// or moving it loads the certificate and key again first. Connections
/// already open stay open, as they do when `port` moves. Refused, with
/// nothing changed, if the files cannot be loaded or the port cannot be
/// had.
#[cfg(feature = "tls")]
fn config_set_tls_port(ctx: &Ctx, port: u16) -> Vec<u8> {
    use crate::listen::Kind;
    use crate::tls::acceptor;
    let server = crate::listen::server_of(ctx.config);
    if port == crate::listen::current_port(server, Kind::Tls) {
        return resp_simple("OK");
    }
    // The port a node of a TLS cluster is known by: the other nodes and
    // its nodes.conf would not follow.
    if ctx.config.tls_cluster {
        return resp_error(
            "ERR CONFIG SET failed (possibly related to argument 'tls-port') - can't set \
             immutable config",
        );
    }
    if port == 0 {
        if crate::listen::current_port(server, Kind::Plain) == 0 {
            return resp_error(
                "ERR the plaintext port is off (port 0), so the TLS port is the only one \
                 clients can reach: set port first",
            );
        }
        return match crate::listen::move_to(server, Kind::Tls, 0) {
            Ok(()) => resp_simple("OK"),
            Err(e) => resp_error(&format!("ERR {e}")),
        };
    }
    let files = acceptor::files(server).unwrap_or_else(|| acceptor::Files::of(ctx.config));
    if files.cert.is_empty() || files.key.is_empty() {
        return resp_error("ERR TLS needs tls-cert-file and tls-key-file: set them first");
    }
    if let Err(e) = acceptor::install(server, &files) {
        log_warn!("TLS: CONFIG SET tls-port {port}: {e}");
        return resp_error("ERR Unable to update TLS configuration. Check server logs.");
    }
    match crate::listen::move_to(server, Kind::Tls, port) {
        Ok(()) => resp_simple("OK"),
        Err(e) => resp_error(&format!("ERR Unable to listen on this port: {e}")),
    }
}

#[cfg(not(feature = "tls"))]
fn config_set_tls_port(_ctx: &Ctx, port: u16) -> Vec<u8> {
    if port == 0 {
        resp_simple("OK")
    } else {
        resp_error("ERR this build has no TLS (the `tls` feature)")
    }
}

/// `CONFIG SET tls-replication`: see `node_tls::set_replication`. It needs
/// the certificate, key and CA files, as at startup.
#[cfg(feature = "tls")]
fn config_set_tls_replication(ctx: &Ctx, on: bool) -> Vec<u8> {
    if on == crate::node_tls::replication_set() {
        return resp_simple("OK");
    }
    let server = crate::listen::server_of(ctx.config);
    let files = crate::tls::acceptor::files(server)
        .unwrap_or_else(|| crate::tls::acceptor::Files::of(ctx.config));
    match crate::node_tls::set_replication(on, files) {
        Ok(()) => resp_simple("OK"),
        Err(e) => {
            log_warn!("TLS: CONFIG SET tls-replication: {e}");
            resp_error("ERR Unable to update TLS configuration. Check server logs.")
        }
    }
}

#[cfg(not(feature = "tls"))]
fn config_set_tls_replication(_ctx: &Ctx, on: bool) -> Vec<u8> {
    if on {
        resp_error("ERR this build has no TLS (the `tls` feature)")
    } else {
        resp_simple("OK")
    }
}

/// The last `INFO keysizes` of each server (by its store's address): when
/// it was counted, what it said, and whether a count is under way.
struct KeysizesCount {
    /// The store counted, held weakly: while the count is kept, no other
    /// store can be at its address, which is the key (`Arc::as_ptr`). A
    /// server started after another has stopped, in the same process, could
    /// otherwise be given the stopped one's count.
    store: std::sync::Weak<Vec<crate::store::Db>>,
    /// When the pass that made `text` began.
    at: std::time::Instant,
    text: String,
    counting: bool,
}

static KEYSIZES_LAST: std::sync::LazyLock<
    parking_lot::Mutex<ahash::AHashMap<usize, KeysizesCount>>,
> = std::sync::LazyLock::new(Default::default);

/// `INFO keysizes` of every database of `store`: a pass over every key.
fn keysizes_text(store: &crate::store::Store) -> String {
    store
        .iter()
        .enumerate()
        .map(|(i, db)| keysizes(i, db))
        .collect()
}

/// `INFO keysizes`: a pass over every key. Asked for by name it is counted
/// now; as part of `INFO all` or `everything`, which exporters scrape every
/// few seconds, a count up to [`KEYSIZES_REUSED_FOR`] old is given again,
/// so that they cost one pass per that time and not one each (#158).
///
/// Nor does `INFO all` wait for that pass, but for the first (#622): an
/// older count starts a new one on a thread of its own, and is given
/// meanwhile. The pass took 100-200 ms at 5 million keys, on the thread
/// that serves the connection, and every connection that thread serves
/// waited for it.
///
/// A count older than twice that is not given: the keys may have changed
/// past recognition since (a `FLUSHALL`, a load), and `INFO all` waits for
/// a new one, as before.
fn keysizes_section(ctx: &Ctx, fresh: bool) -> String {
    keysizes_cached(ctx.store, fresh, keysizes_text)
}

/// What makes the text of a count: [`keysizes_text`], or a test's.
type KeysizesCounter = fn(&crate::store::Store) -> String;

/// [`keysizes_section`] for `store`, counted by `count`.
fn keysizes_cached(store: &crate::store::Store, fresh: bool, count: KeysizesCounter) -> String {
    if !fresh {
        let mut last = KEYSIZES_LAST.lock();
        if let Some(kept) = last.get_mut(&keysizes_key(store)) {
            let age = kept.at.elapsed();
            if age < 2 * KEYSIZES_REUSED_FOR {
                if age >= KEYSIZES_REUSED_FOR && !kept.counting {
                    // No thread: the count is tried again on the next ask.
                    kept.counting = keysizes_count_aside(store, count);
                }
                return kept.text.clone();
            }
        }
    }
    let began = std::time::Instant::now();
    let text = count(store);
    keysizes_keep(store, began, text.clone());
    text
}

/// The key of `store`'s count in [`KEYSIZES_LAST`].
fn keysizes_key(store: &crate::store::Store) -> usize {
    Arc::as_ptr(store) as *const () as usize
}

/// Keep `text`, the count of `store` begun at `began`, unless the one kept
/// was begun later. A count under way elsewhere stays under way. Counts
/// of stores that are gone are let go of.
fn keysizes_keep(store: &crate::store::Store, began: std::time::Instant, text: String) {
    let mut last = KEYSIZES_LAST.lock();
    last.retain(|_, kept| kept.store.strong_count() > 0);
    match last.get_mut(&keysizes_key(store)) {
        Some(kept) => {
            if began >= kept.at {
                kept.at = began;
                kept.text = text;
            }
        }
        None => {
            last.insert(
                keysizes_key(store),
                KeysizesCount {
                    store: Arc::downgrade(store),
                    at: began,
                    text,
                    counting: false,
                },
            );
        }
    }
}

/// Count `store` on a thread of its own. Returns whether the thread
/// started. However the count ends, a panic in it too, its `counting` is
/// put back, so that the next ask can start another.
fn keysizes_count_aside(store: &crate::store::Store, count: KeysizesCounter) -> bool {
    struct Done(usize);
    impl Drop for Done {
        fn drop(&mut self) {
            if let Some(kept) = KEYSIZES_LAST.lock().get_mut(&self.0) {
                kept.counting = false;
            }
        }
    }
    let store = Arc::clone(store);
    std::thread::Builder::new()
        .name("keysizes".to_string())
        .spawn(move || {
            // Dropped last, after the count is kept.
            let _done = Done(keysizes_key(&store));
            let began = std::time::Instant::now();
            let text = count(&store);
            keysizes_keep(&store, began, text);
        })
        .is_ok()
}

const KEYSIZES_REUSED_FOR: std::time::Duration = std::time::Duration::from_secs(10);

/// What a remembered setting is now (see [`REMEMBERED_CONFIG`]).
pub(crate) fn remembered_config(name: &str) -> Option<String> {
    REMEMBERED_CONFIG.lock().get(name).cloned()
}

/// Settings of Redis's that have no meaning here, with Redis's defaults and
/// whatever `CONFIG SET` has since given them. See `CONFIG SET`.
static REMEMBERED_CONFIG: std::sync::LazyLock<
    parking_lot::Mutex<std::collections::BTreeMap<String, String>>,
> = std::sync::LazyLock::new(|| {
    parking_lot::Mutex::new(
        [
            ("hash-max-listpack-entries", "512"),
            ("hash-max-listpack-value", "64"),
            ("hash-max-ziplist-entries", "512"),
            ("hash-max-ziplist-value", "64"),
            ("list-max-listpack-size", "-2"),
            ("list-max-ziplist-size", "-2"),
            ("list-compress-depth", "0"),
            ("set-max-intset-entries", "512"),
            ("set-max-listpack-entries", "128"),
            ("set-max-listpack-value", "64"),
            ("zset-max-listpack-entries", "128"),
            ("zset-max-listpack-value", "64"),
            ("zset-max-ziplist-entries", "128"),
            ("zset-max-ziplist-value", "64"),
            ("hll-sparse-max-bytes", "3000"),
            ("proto-max-bulk-len", "536870912"),
            ("client-query-buffer-limit", "1073741824"),
            ("latency-tracking", "yes"),
            ("latency-tracking-info-percentiles", "50 99 99.9"),
            ("maxclients", "10000"),
            ("maxmemory-clients", "0"),
            // No save points: nothing is saved on a timer.
            ("save", ""),
            ("appendfsync", "everysec"),
            // `DEL` frees what it deletes before it replies (#152).
            ("lazyfree-lazy-user-del", "no"),
            ("lazyfree-lazy-user-flush", "no"),
            ("lazyfree-lazy-expire", "yes"),
            ("lazyfree-lazy-server-del", "yes"),
            ("close-on-replica-write", "yes"),
            ("hz", "10"),
            ("activerehashing", "yes"),
            ("lazyfree-lazy-eviction", "no"),
            ("timeout", "0"),
            ("tcp-keepalive", "300"),
            ("latency-monitor-threshold", "0"),
            ("ts-duplicate-policy", "last"),
            ("aof-ack-after-write", "yes"),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect(),
    )
});

/// `client-output-buffer-limit` in effect now, as `CONFIG GET` gives it.
fn output_buffer_limits(ctx: &Ctx) -> String {
    use crate::output_limits::{Class, Limit, Limits};
    use std::sync::atomic::Ordering::Relaxed;
    let rs = &ctx.repl_state;
    let mut limits = Limits::default();
    for class in [Class::Normal, Class::PubSub] {
        limits.set(class, ctx.pubsub.limits().get(class));
    }
    limits.set(
        Class::Replica,
        Limit {
            hard: rs.replica_max_backlog_bytes.load(Relaxed),
            soft: rs.replica_soft_limit_bytes.load(Relaxed),
            soft_seconds: rs.replica_soft_limit_secs.load(Relaxed),
        },
    );
    limits.to_config_string()
}

/// Every setting `CONFIG GET` knows, with its value now: the ones that are
/// in effect, then the ones that are only remembered.
fn config_rows(ctx: &Ctx) -> Vec<(String, String)> {
    use std::sync::atomic::Ordering::Relaxed;
    let yes_no = |b: bool| if b { "yes" } else { "no" }.to_string();
    let mut rows: Vec<(String, String)> = replication_and_cluster_config(ctx)
        .into_iter()
        .map(|(name, value)| (name.to_string(), value))
        .collect();
    let requirepass = {
        let store = ctx.acl_store.read();
        let open = store.get("default").map(|u| u.nopass).unwrap_or(true);
        if open { "" } else { "**redacted**" }.to_string()
    };
    #[cfg(feature = "lua")]
    let lua_time_limit = crate::commands::lua::lua_time_limit_ms().to_string();
    #[cfg(not(feature = "lua"))]
    let lua_time_limit =
        remembered_config("busy-reply-threshold").unwrap_or_else(|| "5000".to_string());
    let in_effect: Vec<(&str, String)> = vec![
        ("bind", ctx.config.bind.clone()),
        (
            "port",
            crate::listen::current_port(
                crate::listen::server_of(ctx.config),
                crate::listen::Kind::Plain,
            )
            .to_string(),
        ),
        // The TLS port open now; 0 when there is none.
        (
            "tls-port",
            crate::listen::current_port(
                crate::listen::server_of(ctx.config),
                crate::listen::Kind::Tls,
            )
            .to_string(),
        ),
        ("logfile", ctx.config.logfile.clone()),
        ("log-format", ctx.config.log_format.clone()),
        ("databases", ctx.config.databases.to_string()),
        // All three classes, always, as Redis gives them (#774).
        ("client-output-buffer-limit", output_buffer_limits(ctx)),
        // As given with `--threads` (0: one per core), the value `CONFIG
        // REWRITE` keeps, not the count it resolved to on this machine.
        ("threads", ctx.config.threads.to_string()),
        // The value in force, not the one the server started with.
        (
            "maxmemory",
            crate::RUNTIME_MAXMEMORY.load(Relaxed).to_string(),
        ),
        (
            "maxmemory-policy",
            crate::eviction::policy().name().to_string(),
        ),
        ("maxmemory-samples", crate::eviction::samples().to_string()),
        (
            "lfu-log-factor",
            crate::eviction::lfu_log_factor().to_string(),
        ),
        (
            "lfu-decay-time",
            crate::eviction::lfu_decay_time().to_string(),
        ),
        (
            "activedefrag",
            if crate::compaction::enabled() {
                "yes"
            } else {
                "no"
            }
            .to_string(),
        ),
        (
            "allocator-give-back",
            if crate::give_back::enabled() {
                "yes"
            } else {
                "no"
            }
            .to_string(),
        ),
        (
            "active-defrag-ignore-bytes",
            crate::compaction::ignore_bytes().to_string(),
        ),
        (
            "active-defrag-threshold-lower",
            crate::compaction::threshold_lower().to_string(),
        ),
        (
            "active-defrag-cycle-max",
            crate::compaction::cycle_max().to_string(),
        ),
        ("requirepass", requirepass),
        (
            "dir",
            std::env::current_dir()
                .map(|d| d.to_string_lossy().into_owned())
                .unwrap_or_default(),
        ),
        ("aclfile", ctx.config.aclfile.clone()),
        (
            "slowlog-log-slower-than",
            // Disabled is written -1, as it is set.
            match ctx.slowlog.threshold_us() {
                u64::MAX => "-1".to_string(),
                us => us.to_string(),
            },
        ),
        ("slowlog-max-len", ctx.slowlog.len_cap().to_string()),
        ("acllog-max-len", ctx.acl_log.max_len().to_string()),
        ("appendonly", yes_no(crate::aof::AOF_ENABLED.load(Relaxed))),
        (
            "loglevel",
            crate::loglevel::log::level_str(LOG_LEVEL.load(Relaxed)).to_string(),
        ),
        (
            "repl-backlog-size",
            ctx.repl_state.backlog_size.load(Relaxed).to_string(),
        ),
        (
            "repl-backlog-ttl",
            ctx.repl_state.backlog_ttl_secs.load(Relaxed).to_string(),
        ),
        (
            "repl-diskless-load",
            if ctx.repl_state.flush_before_load.load(Relaxed) {
                "disabled"
            } else {
                "swapdb"
            }
            .to_string(),
        ),
        ("tcp-backlog", "511".to_string()),
        ("lua-time-limit", lua_time_limit.clone()),
        (
            "lua-parallel-scripts",
            if crate::tag_locks::enabled() {
                "yes"
            } else {
                "no"
            }
            .to_string(),
        ),
        ("busy-reply-threshold", lua_time_limit),
        (
            "auto-aof-rewrite-percentage",
            crate::aof::AUTO_REWRITE_PERCENTAGE
                .load(Relaxed)
                .to_string(),
        ),
        (
            "auto-aof-rewrite-min-size",
            crate::aof::AUTO_REWRITE_MIN_SIZE.load(Relaxed).to_string(),
        ),
        (
            "min-replicas-to-write",
            ctx.repl_state
                .min_replicas_to_write
                .load(Relaxed)
                .to_string(),
        ),
        (
            "min-replicas-max-lag",
            ctx.repl_state
                .min_replicas_max_lag
                .load(Relaxed)
                .to_string(),
        ),
        (
            "notify-keyspace-events",
            crate::keyspace_events::flags_to_string(crate::keyspace_events::KS_FLAGS.load(Relaxed)),
        ),
        (
            "tracking-table-max-keys",
            ctx.tracking_table.max_keys().to_string(),
        ),
        // What `FT.CONFIG` holds: see `config_set_global`.
        (
            "search-timeout",
            crate::commands::search_tools::config_value("TIMEOUT"),
        ),
        (
            "search-on-timeout",
            crate::commands::search_tools::config_value("ON_TIMEOUT").to_ascii_lowercase(),
        ),
        (
            "search-min-stem-len",
            crate::commands::search_tools::config_value("MINSTEMLEN"),
        ),
        (
            "hll-write-format",
            if ctx.repl_state.hll_write_redis() {
                "redis"
            } else {
                "legacy"
            }
            .to_string(),
        ),
    ];
    for (name, value) in in_effect {
        if !rows.iter().any(|(known, _)| known == name) {
            rows.push((name.to_string(), value));
        }
    }
    // TLS's files, as `CONFIG SET` last loaded them.
    #[cfg(feature = "tls")]
    {
        let files = crate::tls::acceptor::files(crate::listen::server_of(ctx.config))
            .or_else(crate::node_tls::files)
            .unwrap_or_else(|| crate::tls::acceptor::Files::of(ctx.config));
        for name in crate::tls::acceptor::SETTINGS {
            if !rows.iter().any(|(known, _)| known == name) {
                rows.push((name.to_string(), files.get(name).unwrap_or_default()));
            }
        }
    }
    // The master this server replicates, under both of the setting's names.
    let master = ctx
        .repl_state
        .master_addr()
        .filter(|_| ctx.repl_state.is_slave())
        .map(|(host, port)| format!("{host} {port}"))
        .unwrap_or_default();
    for name in ["replicaof", "slaveof"] {
        if !rows.iter().any(|(known, _)| known == name) {
            rows.push((name.to_string(), master.clone()));
        }
    }
    for (name, value) in REMEMBERED_CONFIG.lock().iter() {
        if !rows.iter().any(|(known, _)| known == name) && !crate::redis_settings::is_hidden(name) {
            rows.push((name.clone(), value.clone()));
        }
    }
    rows
}

/// Before a flush of `db` (`None`: of every database): the watched keys
/// that are about to go are modified by it. One that was not there is not —
/// it is as absent after the flush as it was before. One whose time had run
/// out when it was watched is not either; one whose time ran out after is
/// (Redis's `touchAllWatchedKeysInDb`, with nothing to replace the keys).
fn touch_watched_keys(ctx: &Ctx, db: Option<usize>) {
    if !ctx.watch_registry.has_watchers() {
        return;
    }
    ctx.watch_registry.notify_replaced(db, &|db, key| {
        (
            crate::watch::presence(ctx.store, db, key),
            crate::watch::Presence::Absent,
        )
    });
}

/// The processor time this process has used, in seconds: in its own code,
/// and in the kernel on its behalf.
fn cpu_seconds() -> (f64, f64) {
    #[cfg(unix)]
    {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        // SAFETY: `getrusage` fills the struct it is given a pointer to, and
        // returns 0 when it has.
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } == 0 {
            // SAFETY: filled in, by the call that just succeeded.
            let usage = unsafe { usage.assume_init() };
            let seconds = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1_000_000.0;
            return (seconds(usage.ru_utime), seconds(usage.ru_stime));
        }
    }
    (0.0, 0.0)
}

/// The reply to `<COMMAND> HELP`: what the command's subcommands are.
pub(crate) fn help(command: &str, subcommands: &[(&str, &str)]) -> Vec<u8> {
    let mut lines = vec![resp_simple(&format!(
        "{command} <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"
    ))];
    for (usage, what) in subcommands {
        lines.push(resp_simple(usage));
        lines.push(resp_simple(&format!("    {what}")));
    }
    lines.push(resp_simple("HELP"));
    lines.push(resp_simple("    Print this help."));
    resp_array(lines)
}

/// A number of bytes, as a configuration file writes one: `1048576`, `1mb`,
/// `1m` (a million), `2gb`, `512k`.
fn memory_size(text: &str) -> Option<usize> {
    let lower = text.trim().to_ascii_lowercase();
    let digits = lower.trim_end_matches(|c: char| c.is_ascii_alphabetic());
    let unit: usize = match &lower[digits.len()..] {
        "" | "b" => 1,
        "k" => 1000,
        "kb" => 1 << 10,
        "m" => 1000 * 1000,
        "mb" => 1 << 20,
        "g" => 1000 * 1000 * 1000,
        "gb" => 1 << 30,
        _ => return None,
    };
    digits.parse::<usize>().ok()?.checked_mul(unit)
}

/// `ON` or `OFF`.
fn on_or_off(word: &[u8]) -> Option<bool> {
    if word.eq_ignore_ascii_case(b"ON") {
        Some(true)
    } else if word.eq_ignore_ascii_case(b"OFF") {
        Some(false)
    } else {
        None
    }
}

/// Whether `raw` can stand as one word in a line of `CLIENT LIST`: printable
/// ASCII, and no spaces.
pub(crate) fn is_printable_word(raw: &[u8]) -> bool {
    raw.iter().all(|byte| (b'!'..=b'~').contains(byte))
}

/// The conditions of a `CLIENT LIST` or a `CLIENT KILL`.
fn client_filter(
    ctx: &Ctx,
    args: &[&[u8]],
    listing: bool,
) -> Result<crate::client_registry::ClientFilter, Vec<u8>> {
    use crate::client_registry::{ClientFilter, ClientKind};
    use crate::commands::helpers::parse_utf8;
    let syntax = || resp_error("ERR syntax error");
    let id = |raw: &[u8]| {
        parse_utf8(raw)
            .and_then(|s| s.parse::<i64>().ok())
            .filter(|id| *id > 0)
            .map(|id| id as u64)
    };
    let text = |raw: &[u8]| String::from_utf8_lossy(raw).into_owned();
    let mut filter = ClientFilter {
        // `KILL` spares the client that asks unless told not to; a listing
        // includes it.
        skip_me: !listing,
        ..Default::default()
    };
    let mut i = 0;
    while i < args.len() {
        let value = *args.get(i + 1).ok_or_else(syntax)?;
        let option = args[i].to_ascii_uppercase();
        i += 2;
        match option.as_slice() {
            b"ID" => {
                let first = id(value)
                    .ok_or_else(|| resp_error("ERR client-id should be greater than 0"))?;
                let ids = filter.ids.get_or_insert_with(Vec::new);
                ids.push(first);
                // `CLIENT LIST ID a b c`
                while listing && let Some(more) = args.get(i).and_then(|raw| id(raw)) {
                    ids.push(more);
                    i += 1;
                }
            }
            b"TYPE" => {
                filter.kind = Some(ClientKind::parse(value).ok_or_else(|| {
                    resp_error(&format!("ERR Unknown client type '{}'", text(value)))
                })?);
            }
            b"USER" => {
                let user = text(value);
                if !ctx.acl_store.read().contains_key(user.as_str()) {
                    return Err(resp_error(&format!("ERR No such user '{user}'")));
                }
                filter.user = Some(user);
            }
            b"ADDR" => filter.addr = Some(text(value)),
            b"LADDR" => filter.laddr = Some(text(value)),
            b"SKIPME" => {
                filter.skip_me = if value.eq_ignore_ascii_case(b"yes") {
                    true
                } else if value.eq_ignore_ascii_case(b"no") {
                    false
                } else {
                    return Err(syntax());
                };
            }
            b"MAXAGE" => {
                let age = parse_utf8(value)
                    .and_then(|s| s.parse::<i64>().ok())
                    .ok_or_else(|| resp_error("ERR value is not an integer or out of range"))?;
                if age <= 0 {
                    return Err(resp_error("ERR maxage should be greater than 0"));
                }
                filter.max_age = Some(age as u64);
            }
            _ => return Err(syntax()),
        }
    }
    Ok(filter)
}

/// `FLUSHALL ASYNC` / `FLUSHDB ASYNC`: the values are freed in the
/// background (`lazyfree`), after the reply. `SYNC`, or nothing, frees them
/// now; anything else, or more, is a syntax error, as in Redis (#561).
fn flush_is_async(cmd: &[&[u8]]) -> Result<bool, Vec<u8>> {
    match cmd {
        [_] => Ok(false),
        [_, mode] if mode.eq_ignore_ascii_case(b"ASYNC") => Ok(true),
        [_, mode] if mode.eq_ignore_ascii_case(b"SYNC") => Ok(false),
        _ => Err(resp_error("ERR syntax error")),
    }
}

/// A plain `FLUSHALL` or `FLUSHDB` is, in Redis, an `ASYNC` flush the
/// client waits for, and its keys count as lazily freed; not in a
/// transaction or a script, where nothing can wait. The keys were freed here
/// and the client did wait: the count is the same.
fn count_flushed_in_place(flushed: &crate::store::Flushed) {
    if crate::lazyfree::flush_counts() {
        crate::lazyfree::record_freed(flushed.keys as u64);
    }
}

/// One row per connected replica — `(ip, port, offset, lag_seconds)` — in
/// attach order, for `INFO replication` and `ROLE`.
///
/// `offset` is the master's offset minus the bytes this replica has been sent
/// but not yet acknowledged, so it equals `master_repl_offset` exactly when
/// the replica is caught up. `lag` is seconds since its last ACK, as in Redis
/// (it used to be a byte count).
fn replica_rows(
    repl_state: &crate::replication::ReplState,
    master_offset: u64,
) -> Vec<(String, u16, u64, u64, &'static str, u64)> {
    let mut peers: Vec<(u64, String, u16)> = repl_state.replica_peers.lock().clone();
    peers.sort_by_key(|(id, _, _)| *id);
    let progress = repl_state.replica_progress.lock();
    peers
        .into_iter()
        .map(|(id, ip, port)| match progress.get(&id) {
            Some(p) => (
                ip,
                port,
                master_offset.saturating_sub(p.behind_bytes()),
                p.lag_secs(),
                p.state().as_str(),
                p.queued(),
            ),
            None => (ip, port, 0, 0, "wait_bgsave", 0),
        })
        .collect()
}

const ALL_COMMANDS: &[&str] = &[
    "APPEND",
    // Arrays (Redis 8.8)
    "ARCOUNT",
    "ARDEL",
    "ARDELRANGE",
    "ARGET",
    "ARGETRANGE",
    "ARGREP",
    "ARINFO",
    "ARINSERT",
    "ARLASTITEMS",
    "ARLEN",
    "ARMGET",
    "ARMSET",
    "ARNEXT",
    "AROP",
    "ARRING",
    "ARSCAN",
    "ARSEEK",
    "ARSET",
    "AUTH",
    // Bloom filters
    "BF.ADD",
    "BF.CARD",
    "BF.DEBUG",
    "BF.EXISTS",
    "BF.INFO",
    "BF.INSERT",
    "BF.LOADCHUNK",
    "BF.MADD",
    "BF.MEXISTS",
    "BF.RESERVE",
    "BF.SCANDUMP",
    "BGSAVE",
    "BGREWRITEAOF",
    "BITCOUNT",
    "BITFIELD",
    "BITOP",
    "BITPOS",
    "BLMOVE",
    "BLMPOP",
    "BLPOP",
    "BRPOP",
    "BRPOPLPUSH",
    "BZMPOP",
    "BZPOPMAX",
    "BZPOPMIN",
    "CLIENT",
    "COMMAND",
    "COPY",
    "DBSIZE",
    "DEBUG",
    "DECR",
    "DECRBY",
    "DEL",
    "DELEX",
    "DIGEST",
    "DUMP",
    "ECHO",
    "EXISTS",
    "EXPIRE",
    "EXPIREAT",
    "EXPIRETIME",
    "FLUSHALL",
    "FLUSHDB",
    "GEOADD",
    "GEODIST",
    "GEOPOS",
    "GEORADIUS",
    "GEORADIUSBYMEMBER",
    "GEOSEARCH",
    "GEOSEARCHSTORE",
    "GET",
    "GETBIT",
    "GETDEL",
    "GETEX",
    "GETRANGE",
    "GETSET",
    "HDEL",
    "HEXISTS",
    "HEXPIRE",
    "HEXPIREAT",
    "HEXPIRETIME",
    "HGET",
    "HGETALL",
    "HGETDEL",
    "HGETEX",
    "HINCRBY",
    "HINCRBYFLOAT",
    "HKEYS",
    "HLEN",
    "HMGET",
    "HMSET",
    "HPEXPIRE",
    "HPEXPIREAT",
    "HPEXPIRETIME",
    "HPERSIST",
    "HRANDFIELD",
    "HSCAN",
    "HTTL",
    "HPTTL",
    "HSET",
    "HSETEX",
    "HSETNX",
    "HSTRLEN",
    "HVALS",
    "INCR",
    "INCRBY",
    "INCRBYFLOAT",
    "INCREX",
    "INFO",
    "KEYS",
    "LASTSAVE",
    "LCS",
    "LINDEX",
    "LINSERT",
    "LLEN",
    "LMOVE",
    "LMPOP",
    "LOLWUT",
    "LPOP",
    "LPOS",
    "LPUSH",
    "LPUSHX",
    "LRANGE",
    "LREM",
    "LSET",
    "LTRIM",
    "MEMORY",
    "MGET",
    "MIGRATE",
    "MOVE",
    "MSET",
    "MSETNX",
    "MSETEX",
    "OBJECT",
    "PERSIST",
    "PEXPIRE",
    "PEXPIREAT",
    "PEXPIRETIME",
    "PFADD",
    "PFCOUNT",
    "PFMERGE",
    "PING",
    "PSETEX",
    "PSUBSCRIBE",
    "PTTL",
    "PUBLISH",
    "PUNSUBSCRIBE",
    "QUIT",
    "RANDOMKEY",
    "RENAME",
    "RENAMENX",
    "RESET",
    "RESTORE",
    "ROLE",
    "RPOP",
    "RPOPLPUSH",
    "RPUSH",
    "RPUSHX",
    "SADD",
    "SAVE",
    "SCAN",
    "SCARD",
    "SDIFF",
    "SDIFFSTORE",
    "SELECT",
    "SET",
    "SETBIT",
    "SETEX",
    "SETRANGE",
    "SHUTDOWN",
    "SINTER",
    "SINTERCARD",
    "SINTERSTORE",
    "SISMEMBER",
    "SLAVEOF",
    "SLOWLOG",
    "SMEMBERS",
    "SMISMEMBER",
    "SMOVE",
    "SORT",
    "SORT_RO",
    "SPOP",
    "SRANDMEMBER",
    "SREM",
    "SSCAN",
    "SSUBSCRIBE",
    "SUBSCRIBE",
    "SUNION",
    "SUNIONSTORE",
    "SUNSUBSCRIBE",
    "SWAPDB",
    "TIME",
    "TOUCH",
    "TTL",
    "TYPE",
    "UNLINK",
    "UNSUBSCRIBE",
    "UNWATCH",
    "VADD",
    "VCARD",
    "VDIM",
    "VEMB",
    "VGETATTR",
    "VINFO",
    "VISMEMBER",
    "VLINKS",
    "VRANDMEMBER",
    "VRANGE",
    "VREM",
    "VSETATTR",
    "VSIM",
    "WAIT",
    "WAITAOF",
    "WATCH",
    "XACK",
    "XADD",
    "XAUTOCLAIM",
    "XCLAIM",
    "XDEL",
    "XDELEX",
    "XGROUP",
    "XINFO",
    "XLEN",
    "XNACK",
    "XPENDING",
    "XRANGE",
    "XREAD",
    "XREADGROUP",
    "XREVRANGE",
    "XSETID",
    "XTRIM",
    "ZADD",
    "ZCARD",
    "ZDIFF",
    "ZDIFFSTORE",
    "ZINCRBY",
    "ZINTER",
    "ZINTERCARD",
    "ZINTERSTORE",
    "ZLEXCOUNT",
    "ZMPOP",
    "ZMSCORE",
    "ZPOPMAX",
    "ZPOPMIN",
    "ZRANDMEMBER",
    "ZRANGE",
    "ZRANGEBYLEX",
    "ZRANGEBYSCORE",
    "ZRANGESTORE",
    "ZRANK",
    "ZREM",
    "ZREMRANGEBYLEX",
    "ZREMRANGEBYSCORE",
    "ZREMRANGEBYRANK",
    "ZREVRANGE",
    "ZREVRANGEBYLEX",
    "ZREVRANGEBYSCORE",
    "ZREVRANK",
    "ZSCAN",
    "ZSCORE",
    "ZUNION",
    "ZUNIONSTORE",
    // FT commands
    "FT.CREATE",
    "FT.SEARCH",
    "FT.AGGREGATE",
    "FT.DROPINDEX",
    "FT.INFO",
    "FT._LIST",
    "FT.ALTER",
    "FT.ALIASADD",
    "FT.ALIASDEL",
    "FT.ALIASUPDATE",
    "FT.SYNUPDATE",
    "FT.SYNDUMP",
    "FT.DICTADD",
    "FT.DICTDEL",
    "FT.DICTDUMP",
    "FT.EXPLAIN",
    "FT.EXPLAINCLI",
    "FT.PROFILE",
    "FT.SPELLCHECK",
    "FT.CONFIG",
    "FT.CURSOR",
    "FT.DEBUG",
    // JSON commands
    "JSON.SET",
    "JSON.GET",
    "JSON.MGET",
    "JSON.MSET",
    "JSON.DEL",
    "JSON.TYPE",
    "JSON.NUMINCRBY",
    "JSON.NUMMULTBY",
    "JSON.STRAPPEND",
    "JSON.STRLEN",
    "JSON.ARRAPPEND",
    "JSON.ARRINDEX",
    "JSON.ARRINSERT",
    "JSON.ARRLEN",
    "JSON.ARRPOP",
    "JSON.ARRTRIM",
    "JSON.OBJKEYS",
    "JSON.OBJLEN",
    "JSON.CLEAR",
    "JSON.TOGGLE",
    "JSON.MERGE",
    "JSON.RESP",
    "JSON.DEBUG",
    // TS commands
    "TS.CREATE",
    "TS.ALTER",
    "TS.ADD",
    "TS.MADD",
    "TS.INCRBY",
    "TS.DECRBY",
    "TS.DEL",
    "TS.GET",
    "TS.MGET",
    "TS.RANGE",
    "TS.MRANGE",
    "TS.REVRANGE",
    "TS.MREVRANGE",
    "TS.INFO",
    "TS.QUERYINDEX",
    "TS.CREATERULE",
    "TS.DELETERULE",
    // Redis 8.4 – 8.10
    "BACKUP",
    "BLMOVEM",
    "HIMPORT",
    "HOTKEYS",
    "LMOVEM",
    "SDIFFCARD",
    "SUNIONCARD",
    "TS.NRANGE",
    "TS.NREVRANGE",
    "TS.QUERYLABELS",
    "TS.READ",
    "XACKDEL",
    "XCFGSET",
    "XIDMPRECORD",
    "FT.ALIASLIST",
    "FT.HYBRID",
    "FT.TAGVALS",
    "JSON.FORGET",
    // Commands that were served but never listed
    "ASKING",
    "BITFIELD_RO",
    "CLUSTER",
    "CONFIG",
    "DISCARD",
    "EVAL",
    "EVALSHA",
    "EVALSHA_RO",
    "EVAL_RO",
    "EXEC",
    "FAILOVER",
    "FCALL",
    "FCALL_RO",
    "FUNCTION",
    "GEOHASH",
    "GEORADIUSBYMEMBER_RO",
    "GEORADIUS_RO",
    "HELLO",
    "MODULE",
    "MONITOR",
    "MULTI",
    "PFDEBUG",
    "PFSELFTEST",
    "PUBSUB",
    "READONLY",
    "READWRITE",
    "SCRIPT",
    "SETNX",
    "SPUBLISH",
    "STRLEN",
    "SUBSTR",
    "ZCOUNT",
    // Replication internals
    "REPLICAOF",
    "REPLCONF",
    "PSYNC",
    "SYNC",
    // ACL
    "ACL",
    // Latency / slowlog / memory (these will be added in later PRs
    // but listing them here ensures COMMAND LIST is always complete)
    "LATENCY",
];

/// Whether `name` appears in [`ALL_COMMANDS`] (used by Lua `redis.acl_check_cmd`, etc.).
#[allow(dead_code)]
#[inline]
pub(crate) fn is_registered_command(name: &str) -> bool {
    let upper = name.to_ascii_uppercase();
    ALL_COMMANDS.iter().any(|c| c.eq_ignore_ascii_case(&upper))
}

fn commands_with_prefix(prefix: &str) -> Vec<&'static str> {
    ALL_COMMANDS
        .iter()
        .copied()
        .filter(|c| c.starts_with(prefix))
        .collect()
}

/// The `admin` ACL category.
/// Whether `name` (upper case) is an administrative command.
///
/// `ACL` counts as one here, for MONITOR, which shows none of it: `ACL
/// SETUSER` carries passwords.
pub(crate) fn is_admin_command(name: &str) -> bool {
    name == "ACL" || ADMIN_COMMANDS.contains(&name)
}

/// Whether there is a command called `name` (upper case).
pub(crate) fn is_command(name: &str) -> bool {
    static NAMES: std::sync::LazyLock<ahash::AHashSet<&'static str>> =
        std::sync::LazyLock::new(|| ALL_COMMANDS.iter().copied().collect());
    NAMES.contains(name) || crate::commands::arity::is_known(name)
}

const ADMIN_COMMANDS: &[&str] = &[
    // Not `ACL` as a whole: `ACL WHOAMI`, `CAT`, `GENPASS` and `HELP` are
    // not administrative in Redis (#566).
    "ACL|DELUSER",
    "ACL|DRYRUN",
    "ACL|GETUSER",
    "ACL|LIST",
    "ACL|LOAD",
    "ACL|LOG",
    "ACL|SAVE",
    "ACL|SETUSER",
    "ACL|USERS",
    "BACKUP",
    "BGREWRITEAOF",
    "BGSAVE",
    "CLIENT|KILL",
    "CLIENT|LIST",
    "CLIENT|NO-EVICT",
    "CLIENT|PAUSE",
    "CLIENT|UNPAUSE",
    "CLUSTER|ADDSLOTS",
    "CLUSTER|ADDSLOTSRANGE",
    "CLUSTER|BUMPEPOCH",
    "CLUSTER|COUNT-FAILURE-REPORTS",
    "CLUSTER|DELSLOTS",
    "CLUSTER|DELSLOTSRANGE",
    "CLUSTER|DROPORPHANS",
    "CLUSTER|FAILOVER",
    "CLUSTER|FLUSHSLOTS",
    "CLUSTER|FORGET",
    "CLUSTER|LINKS",
    "CLUSTER|MEET",
    "CLUSTER|MIGRATION",
    "CLUSTER|ORPHANSLOTS",
    "CLUSTER|REPLICAS",
    "CLUSTER|REPLICATE",
    "CLUSTER|RESET",
    "CLUSTER|SAVECONFIG",
    "CLUSTER|SET-CONFIG-EPOCH",
    "CLUSTER|SETSLOT",
    "CLUSTER|SLAVES",
    "CLUSTER|SYNCSLOTS",
    "CONFIG",
    "DEBUG",
    "FAILOVER",
    "HOTKEYS",
    "LASTSAVE",
    "LATENCY",
    "MODULE",
    "MONITOR",
    "PFDEBUG",
    "PFSELFTEST",
    "PSYNC",
    "REPLCONF",
    "REPLICAOF",
    "SAVE",
    "SHUTDOWN",
    "SLAVEOF",
    "SLOWLOG",
    "SYNC",
];

/// The `fast` ACL category.
/// Whether `name` (upper case) is a command Redis flags `fast`: the latency
/// monitor's `fast-command` event, not `command`.
pub(crate) fn is_fast_command(name: &str) -> bool {
    FAST_COMMANDS.contains(&name)
}

const FAST_COMMANDS: &[&str] = &[
    "APPEND",
    "ASKING",
    "AUTH",
    "BF.CARD",
    "BF.INFO",
    "BF.RESERVE",
    "DBSIZE",
    "DECR",
    "DECRBY",
    "DISCARD",
    "ECHO",
    "EXISTS",
    "EXPIRE",
    "EXPIREAT",
    "EXPIRETIME",
    "GET",
    "GETBIT",
    "GETDEL",
    "GETEX",
    "GETSET",
    "HDEL",
    "HELLO",
    "HEXISTS",
    "HGET",
    "HINCRBY",
    "HINCRBYFLOAT",
    "HLEN",
    "HMGET",
    "HMSET",
    "HSET",
    "HSETNX",
    "HSTRLEN",
    "INCR",
    "INCRBY",
    "INCRBYFLOAT",
    "LASTSAVE",
    "LLEN",
    "LPOP",
    "LPUSH",
    "LPUSHX",
    "MGET",
    "MULTI",
    "PERSIST",
    "PEXPIRE",
    "PEXPIREAT",
    "PEXPIRETIME",
    "PFADD",
    "PING",
    "PTTL",
    "PUBLISH",
    "READONLY",
    "READWRITE",
    "ROLE",
    "RPOP",
    "RPUSH",
    "RPUSHX",
    "SADD",
    "SCARD",
    "SELECT",
    "SETNX",
    "SISMEMBER",
    "SMISMEMBER",
    "SMOVE",
    "SPOP",
    "SPUBLISH",
    "SREM",
    "STRLEN",
    "TIME",
    "TOUCH",
    "TTL",
    "TYPE",
    "UNLINK",
    "UNWATCH",
    "WATCH",
    "XACK",
    "XADD",
    "XLEN",
    "ZADD",
    "ZCARD",
    "ZCOUNT",
    "ZINCRBY",
    "ZLEXCOUNT",
    "ZMSCORE",
    "ZPOPMAX",
    "ZPOPMIN",
    "ZRANK",
    "ZREVRANK",
    "ZSCORE",
];

/// Whether `name` (upper case) is in `@write`, and flagged `write` by
/// `COMMAND`. That is what is sent to replicas
/// ([`crate::replication::is_write_command`]) but `SELECT`: replicas are
/// sent it to follow the database, and Redis has it in `@fast @connection`
/// only (#392).
fn is_acl_write(name: &str) -> bool {
    (crate::replication::is_write_command(name) && name != "SELECT")
        || WRITES_BY_ARGUMENT.contains(&name)
}

/// Writes only with some arguments, but Redis flags them `write` and puts
/// them in `@write` (not `@read`) regardless, as can-write; the per-key and
/// runtime checks look at the arguments.
const WRITES_BY_ARGUMENT: [&str; 3] = ["GEORADIUS", "GEORADIUSBYMEMBER", "MIGRATE"];

/// The categories in [`ACL_CATEGORIES`] that Redis does not have. They
/// can be granted like any other, and `COMMAND INFO` leaves them out.
const KIVIDB_ONLY_CATEGORIES: &[&str] = &["cluster", "generic", "server"];

/// The ACL categories, as `ACL CAT` lists them.
pub(crate) const ACL_CATEGORIES: &[&str] = &[
    "admin",
    "array",
    "bitmap",
    "blocking",
    "bloom",
    "cluster",
    "connection",
    "dangerous",
    "fast",
    "generic",
    "geo",
    "hash",
    "hyperloglog",
    "json",
    "keyspace",
    "list",
    "pubsub",
    "read",
    "scripting",
    "search",
    "server",
    "set",
    "slow",
    "sortedset",
    "stream",
    "string",
    "timeseries",
    "transaction",
    "vector",
    "write",
];

/// Every command as `COMMAND` describes it, by upper-case name:
///
/// ```text
/// [name, arity, [flag ...], first-key, last-key, key-step,
///  [@category ...], [tip ...], [key-spec ...], [subcommand ...]]
/// ```
///
/// What a client takes from this is where the keys are — redis-py's cluster
/// client routes every command by it, and can send nothing if the list is
/// empty. The three key numbers come from [`crate::keyspec::layout`], which
/// reads them off the table routing itself uses; a command whose keys are
/// not at fixed positions carries the `movablekeys` flag, which tells the
/// client to ask `COMMAND GETKEYS`.
///
/// The arity is a lower bound (negative, as Redis writes "at least"): KiviDB
/// has no arity table, and each handler checks its own arguments.
fn command_table() -> std::collections::BTreeMap<&'static str, Vec<u8>> {
    use crate::keyspec::Layout;

    let mut categories: ahash::AHashMap<&'static str, Vec<&'static str>> = ahash::AHashMap::new();
    // The categories Redis has: `cluster`, `generic` and `server` are
    // KiviDB's own, after Redis's command groups, and Redis reports no such
    // category for a command (`COMMAND INFO command` is `@slow @connection`).
    for category in ACL_CATEGORIES
        .iter()
        .filter(|c| !KIVIDB_ONLY_CATEGORIES.contains(*c))
    {
        for command in acl_cat_commands(category).unwrap_or_default() {
            // `CLIENT|KILL` and the like describe a subcommand, not a command.
            if !command.contains('|') {
                categories.entry(command).or_default().push(category);
            }
        }
    }
    let status = |words: Vec<String>| -> Vec<u8> {
        resp_array(words.iter().map(|word| resp_simple(word)).collect())
    };

    ALL_COMMANDS
        .iter()
        .map(|name| {
            let layout = crate::keyspec::layout(name);
            let (first, last, step, arity) = match layout {
                Layout::NoKeys => (0, 0, 0, -1),
                Layout::Fixed { first, last, step } => (first, last, step, -(first + 1)),
                // Redis reports the first key where there is one; a client
                // that honours `movablekeys` does not use these.
                Layout::Movable => (0, 0, 0, -2),
            };
            // One of Redis's commands has Redis's arity. The guess above is
            // for the rest.
            let arity = crate::commands::arity::of(name)
                .or_else(|| crate::commands::bf::arity(name))
                .or_else(|| module_arity(name))
                .map_or(arity, i64::from);
            // A command with subcommands has no keys of its own: they are
            // its subcommands'.
            let container = crate::commands::arity::is_container(name);
            let movable = !container
                && crate::commands::key_specs::movable(name).unwrap_or(layout == Layout::Movable);
            let in_category = |category: &str| {
                categories
                    .get(name)
                    .is_some_and(|list| list.contains(&category))
            };
            let mut flags: Vec<String> = Vec::new();
            if is_acl_write(name) {
                flags.push("write".into());
            } else if layout != Layout::NoKeys {
                flags.push("readonly".into());
            }
            for flag in ["admin", "pubsub", "blocking", "fast"] {
                if in_category(flag) {
                    flags.push(flag.into());
                }
            }
            if movable {
                flags.push("movablekeys".into());
            }
            let acl = categories
                .get(name)
                .map(|list| list.iter().map(|category| format!("@{category}")).collect())
                .unwrap_or_default();
            let entry = resp_array(vec![
                resp_bulk_str(&name.to_ascii_lowercase()),
                resp_integer(arity),
                status(flags),
                resp_integer(first),
                resp_integer(last),
                resp_integer(step),
                status(acl),
                resp_array(vec![]),
                resp_array(vec![]),
                resp_array(vec![]),
            ]);
            (*name, entry)
        })
        .collect()
}

/// The arity `COMMAND INFO` reports for a command of a module KiviDB has
/// built in, as Redis 8.10 reports it with that module loaded (#561). Only
/// reported: each handler checks its own arguments.
fn module_arity(name: &str) -> Option<i16> {
    Some(match name {
        "JSON.ARRAPPEND" | "JSON.STRAPPEND" | "JSON.MGET" => -3,
        "JSON.ARRINDEX" | "JSON.SET" | "JSON.MSET" | "JSON.MERGE" => -4,
        "JSON.ARRINSERT" => -5,
        "JSON.ARRTRIM" => 5,
        "JSON.NUMINCRBY" | "JSON.NUMMULTBY" | "JSON.NUMPOWBY" => 4,
        "JSON.TOGGLE" => 3,
        _ => return None,
    })
}

/// Returns the list of commands in an ACL category, or None for an unknown category.
fn acl_cat_commands(category: &str) -> Option<Vec<&'static str>> {
    let cmds = match category {
        "string" => vec![
            "APPEND",
            "DECR",
            "DECRBY",
            "GET",
            "GETDEL",
            "GETEX",
            "GETRANGE",
            "GETSET",
            "INCR",
            "INCRBY",
            "INCRBYFLOAT",
            "INCREX",
            "LCS",
            "MGET",
            "MSET",
            "MSETNX",
            "PSETEX",
            "SET",
            "SETEX",
            "SETNX",
            "SETRANGE",
            "STRLEN",
            "SUBSTR",
            "DELEX",
            "DIGEST",
            "MSETEX",
        ],
        "list" => vec![
            "BLMOVE",
            "BLMPOP",
            "BLPOP",
            "BRPOP",
            "BRPOPLPUSH",
            "LINDEX",
            "LINSERT",
            "LLEN",
            "LMOVE",
            "LMPOP",
            "LPOP",
            "LPOS",
            "LPUSH",
            "LPUSHX",
            "LRANGE",
            "LREM",
            "LSET",
            "LTRIM",
            "RPOP",
            "RPOPLPUSH",
            "RPUSH",
            "RPUSHX",
            "BLMOVEM",
            "LMOVEM",
        ],
        "set" => vec![
            "SADD",
            "SCARD",
            "SDIFF",
            "SDIFFSTORE",
            "SINTER",
            "SINTERCARD",
            "SINTERSTORE",
            "SISMEMBER",
            "SMEMBERS",
            "SMISMEMBER",
            "SMOVE",
            "SPOP",
            "SRANDMEMBER",
            "SREM",
            "SSCAN",
            "SUNION",
            "SUNIONSTORE",
            "SDIFFCARD",
            "SUNIONCARD",
        ],
        "sorted_set" | "sortedset" => vec![
            "BZMPOP",
            "BZPOPMAX",
            "BZPOPMIN",
            "ZADD",
            "ZCARD",
            "ZCOUNT",
            "ZDIFF",
            "ZDIFFSTORE",
            "ZINCRBY",
            "ZINTER",
            "ZINTERCARD",
            "ZINTERSTORE",
            "ZLEXCOUNT",
            "ZMPOP",
            "ZMSCORE",
            "ZPOPMAX",
            "ZPOPMIN",
            "ZRANDMEMBER",
            "ZRANGE",
            "ZRANGEBYLEX",
            "ZRANGEBYSCORE",
            "ZRANGESTORE",
            "ZRANK",
            "ZREM",
            "ZREMRANGEBYLEX",
            "ZREMRANGEBYSCORE",
            "ZREMRANGEBYRANK",
            "ZREVRANGE",
            "ZREVRANGEBYLEX",
            "ZREVRANGEBYSCORE",
            "ZREVRANK",
            "ZSCAN",
            "ZSCORE",
            "ZUNION",
            "ZUNIONSTORE",
        ],
        "hash" => vec![
            "HDEL",
            "HEXISTS",
            "HEXPIRE",
            "HEXPIREAT",
            "HEXPIRETIME",
            "HGET",
            "HGETALL",
            "HINCRBY",
            "HINCRBYFLOAT",
            "HKEYS",
            "HLEN",
            "HMGET",
            "HMSET",
            "HPERSIST",
            "HPEXPIRE",
            "HPEXPIREAT",
            "HPEXPIRETIME",
            "HPTTL",
            "HGETDEL",
            "HGETEX",
            "HSETEX",
            "HSTRLEN",
            "HRANDFIELD",
            "HSCAN",
            "HSET",
            "HTTL",
            "HSETNX",
            "HVALS",
            "HIMPORT",
        ],
        "bitmap" => vec![
            "BITCOUNT",
            "BITFIELD",
            "BITFIELD_RO",
            "BITOP",
            "BITPOS",
            "GETBIT",
            "SETBIT",
        ],
        "hyperloglog" => vec!["PFADD", "PFCOUNT", "PFDEBUG", "PFMERGE", "PFSELFTEST"],
        // Redis 8.8 array type. Members are split into @read/@write below by
        // `replication::is_write_command`, same as every other data category.
        "array" => vec![
            "ARCOUNT",
            "ARDEL",
            "ARDELRANGE",
            "ARGET",
            "ARGETRANGE",
            "ARGREP",
            "ARINFO",
            "ARINSERT",
            "ARLASTITEMS",
            "ARLEN",
            "ARMGET",
            "ARMSET",
            "ARNEXT",
            "AROP",
            "ARRING",
            "ARSCAN",
            "ARSEEK",
            "ARSET",
        ],
        "geo" => vec![
            "GEOADD",
            "GEODIST",
            "GEOPOS",
            "GEORADIUS",
            "GEORADIUSBYMEMBER",
            "GEOSEARCH",
            "GEOSEARCHSTORE",
            "GEOHASH",
            "GEORADIUSBYMEMBER_RO",
            "GEORADIUS_RO",
        ],
        "stream" => vec![
            "XACK",
            "XADD",
            "XAUTOCLAIM",
            "XCLAIM",
            "XDEL",
            "XDELEX",
            "XGROUP",
            "XINFO",
            "XLEN",
            "XNACK",
            "XPENDING",
            "XRANGE",
            "XREAD",
            "XREADGROUP",
            "XREVRANGE",
            "XSETID",
            "XTRIM",
            "XACKDEL",
            "XCFGSET",
            "XIDMPRECORD",
        ],
        "generic" => vec![
            "COPY",
            "DEL",
            "DUMP",
            "EXISTS",
            "EXPIRE",
            "EXPIREAT",
            "EXPIRETIME",
            "KEYS",
            "MIGRATE",
            "MOVE",
            "OBJECT",
            "PERSIST",
            "PEXPIRE",
            "PEXPIREAT",
            "PEXPIRETIME",
            "PTTL",
            "RANDOMKEY",
            "RENAME",
            "RENAMENX",
            "RESTORE",
            "SCAN",
            "SORT",
            "SORT_RO",
            "TOUCH",
            "TTL",
            "TYPE",
            "UNLINK",
        ],
        "server" => vec![
            "ACL",
            "BGREWRITEAOF",
            "BGSAVE",
            "COMMAND",
            "CONFIG",
            "DBSIZE",
            "DEBUG",
            "FAILOVER",
            "FLUSHALL",
            "FLUSHDB",
            "INFO",
            "LASTSAVE",
            "LATENCY",
            "LOLWUT",
            "MEMORY",
            "MODULE",
            "MONITOR",
            "PSYNC",
            "REPLICAOF",
            "REPLCONF",
            "ROLE",
            "SAVE",
            "SELECT",
            "SHUTDOWN",
            "SLAVEOF",
            "SLOWLOG",
            "SWAPDB",
            "TIME",
            "BACKUP",
            "HOTKEYS",
            "SYNC",
        ],
        // As Redis 7.4 to 8.10 and Valkey have it. redis-py's cluster client
        // runs `COMMAND` as it connects, so a user given `+@connection` and
        // nothing more for it must be able to (#392).
        "connection" => vec![
            "ASKING",
            "AUTH",
            "CLIENT",
            "COMMAND",
            "ECHO",
            "HELLO",
            "PING",
            "QUIT",
            "READONLY",
            "READWRITE",
            "RESET",
            "SELECT",
            "WAIT",
            "WAITAOF",
        ],
        // Redis calls it `transaction`; `transactions` is the older KiviDB name.
        "transaction" | "transactions" => vec!["DISCARD", "EXEC", "MULTI", "UNWATCH", "WATCH"],
        "pubsub" => vec![
            "PSUBSCRIBE",
            "PUBLISH",
            "PUBSUB",
            "PUNSUBSCRIBE",
            "RESET",
            "SPUBLISH",
            "SUBSCRIBE",
            "SUNSUBSCRIBE",
            "SSUBSCRIBE",
            "UNSUBSCRIBE",
        ],
        "scripting" => vec![
            "EVAL",
            "EVALSHA",
            "EVALSHA_RO",
            "EVAL_RO",
            "FCALL",
            "FCALL_RO",
            "FUNCTION",
            "SCRIPT",
        ],
        "cluster" => vec!["ASKING", "CLUSTER", "READONLY", "READWRITE"],
        "keyspace" => vec![
            "COPY",
            "DEL",
            "DUMP",
            "EXISTS",
            "EXPIRE",
            "EXPIREAT",
            "EXPIRETIME",
            "KEYS",
            "MIGRATE",
            "MOVE",
            "OBJECT",
            "PERSIST",
            "PEXPIRE",
            "PEXPIREAT",
            "PEXPIRETIME",
            "PTTL",
            "RANDOMKEY",
            "RENAME",
            "RENAMENX",
            "RESTORE",
            "SCAN",
            "SORT",
            "SORT_RO",
            "TOUCH",
            "TTL",
            "TYPE",
            "UNLINK",
        ],
        // The module families, taken from the command table by prefix so a new
        // command joins its category by being registered.
        "json" => commands_with_prefix("JSON."),
        "bloom" => commands_with_prefix("BF."),
        "search" => commands_with_prefix("FT."),
        "timeseries" => commands_with_prefix("TS."),
        "vector" => vec![
            "VADD",
            "VCARD",
            "VDIM",
            "VEMB",
            "VGETATTR",
            "VINFO",
            "VISMEMBER",
            "VLINKS",
            "VRANDMEMBER",
            "VRANGE",
            "VREM",
            "VSETATTR",
            "VSIM",
        ],
        // Commands that can park the connection.
        "blocking" => vec![
            "BLMOVE",
            "BLMOVEM",
            "BLMPOP",
            "BLPOP",
            "BRPOP",
            "BRPOPLPUSH",
            "BZMPOP",
            "BZPOPMAX",
            "BZPOPMIN",
            "TS.READ",
            "WAIT",
            "WAITAOF",
            "XREAD",
            "XREADGROUP",
        ],
        // Operating the server rather than using it. An entry of the form
        // `COMMAND|SUBCOMMAND` covers that subcommand only: clients need
        // `CLUSTER SLOTS` and `CLIENT SETNAME` even when they may not run
        // `CLUSTER RESET` or `CLIENT KILL`.
        "admin" => ADMIN_COMMANDS.to_vec(),
        // Everything in `admin`, plus what can destroy data, stall the server
        // or disclose how it is set up.
        "dangerous" => {
            let mut cmds = ADMIN_COMMANDS.to_vec();
            cmds.extend_from_slice(&[
                "FLUSHALL", "FLUSHDB", "INFO", "KEYS", "MIGRATE", "RESTORE", "ROLE", "SORT",
                "SWAPDB",
            ]);
            cmds
        }
        // Redis sorts every command into one of these two; `fast` is the
        // constant-time ones and `slow` is the rest.
        "fast" => FAST_COMMANDS.to_vec(),
        "slow" => ALL_COMMANDS
            .iter()
            .copied()
            .filter(|c| !FAST_COMMANDS.contains(c))
            .collect(),
        // Backed by the same authoritative write-command list already used to
        // gate replica read-only enforcement (`replication::is_write_command`)
        // — reusing it here keeps `-@write`/`+@write` consistent with that
        // check instead of maintaining a second, independently-drifting list.
        //
        // Rebuilt for every rule a command is checked against, so the
        // commands that are writes only by argument ([`is_acl_write`]) are
        // added once rather than tested for at every entry.
        "write" => {
            let mut cmds: Vec<&'static str> = Vec::with_capacity(ALL_COMMANDS.len() + 7);
            cmds.extend(
                ALL_COMMANDS
                    .iter()
                    .copied()
                    .filter(|c| crate::replication::is_write_command(c) && *c != "SELECT"),
            );
            cmds.extend_from_slice(&WRITES_BY_ARGUMENT);
            // FUNCTION's loading subcommands write (the container as a whole
            // names no key, and FUNCTION LIST / DUMP / STATS read), as Redis
            // places `function|load` etc. in `@write`.
            cmds.extend_from_slice(&[
                "FUNCTION|LOAD",
                "FUNCTION|DELETE",
                "FUNCTION|FLUSH",
                "FUNCTION|RESTORE",
            ]);
            cmds
        }
        // Union of the existing data-access categories, minus anything
        // classified as a write above.
        "read" => {
            let mut cmds: Vec<&'static str> = [
                "string",
                "list",
                "set",
                "sortedset",
                "hash",
                "bitmap",
                "bloom",
                "hyperloglog",
                "geo",
                "stream",
                "generic",
                "keyspace",
                "array",
                "json",
                "search",
                "timeseries",
                "vector",
            ]
            .iter()
            .flat_map(|cat| acl_cat_commands(cat).unwrap_or_default())
            .filter(|c| !crate::replication::is_write_command(c))
            .collect();
            cmds.sort_unstable();
            cmds.dedup();
            for write in WRITES_BY_ARGUMENT {
                if let Ok(at) = cmds.binary_search(&write) {
                    cmds.remove(at);
                }
            }
            cmds
        }
        _ => return None,
    };
    Some(cmds)
}

/// The module families KiviDB has built in, as Redis 8.10 names and numbers
/// them. No module can be loaded, but clients look for these before they use
/// the commands (redisvl 0.4, which LlamaIndex's vector store pins, refuses
/// to connect without `search`), so `MODULE LIST`, `HELLO` and
/// `COMMAND LIST FILTERBY MODULE` report them as Redis reports its own
/// built-in `vectorset`: an empty path and no args.
const BUILT_IN_MODULES: [(&str, i64); 5] = [
    ("search", 81001),
    ("ReJSON", 81000),
    ("timeseries", 81001),
    ("vectorset", 1),
    ("bf", 81001),
];

/// One entry per built-in module family, as `MODULE LIST` and `HELLO`'s
/// `modules` give them: a map under RESP3, a flat array under RESP2.
pub(crate) fn module_list_reply(resp: u8) -> Vec<u8> {
    resp_array(
        BUILT_IN_MODULES
            .iter()
            .map(|(name, ver)| {
                resp_map_for(
                    resp,
                    vec![
                        resp_bulk(b"name"),
                        resp_bulk(name.as_bytes()),
                        resp_bulk(b"ver"),
                        resp_integer(*ver),
                        resp_bulk(b"path"),
                        resp_bulk(b""),
                        resp_bulk(b"args"),
                        resp_array(vec![]),
                    ],
                )
            })
            .collect(),
    )
}

/// Whether the lower-cased command `name` belongs to the built-in module
/// family `module`, for `COMMAND LIST FILTERBY MODULE`.
fn in_module(module: &str, name: &str) -> bool {
    match module {
        "search" => name.starts_with("ft.") || name.starts_with("_ft."),
        "rejson" => name.starts_with("json."),
        "timeseries" => name.starts_with("ts."),
        // The probabilistic types: Redis 8's `bf` module.
        "bf" => ["bf.", "cf.", "cms.", "topk.", "tdigest."]
            .iter()
            .any(|prefix| name.starts_with(prefix)),
        // The vector set commands are the only ones that begin with `v`.
        "vectorset" => name.starts_with('v'),
        _ => false,
    }
}

/// `INFO keysizes` counts: by type, then by bin. Bin 0 is the empty, bin n
/// holds 2^(n-1) to 2^n - 1.
type KeysizeBins = [[u64; 65]; 5];

/// How many buckets of a shard `INFO keysizes` goes over in one hold of it.
const KEYSIZES_BUCKETS: usize = 1024;

/// `value` counted in its type's bin.
fn keysize_count(bins: &mut KeysizeBins, value: crate::store::ValueRef<'_>) {
    let (kind, len) = match value {
        crate::store::ValueRef::String(b) => (0, b.len()),
        crate::store::ValueRef::List(l) => (1, l.read().len()),
        crate::store::ValueRef::Set(s) => (2, s.read().len()),
        crate::store::ValueRef::SortedSet(z) => (3, z.read().len()),
        crate::store::ValueRef::Hash(h) => (4, h.read().len()),
        crate::store::ValueRef::HashEx(h) => (4, h.read().len()),
        _ => return,
    };
    let bin = (usize::BITS - len.leading_zeros()) as usize;
    bins[kind][bin] += 1;
}

/// One shard's `INFO keysizes` counts, gone over [`KEYSIZES_BUCKETS`]
/// buckets at a time and let go of in between. It was held for the whole
/// of its keys, and writes to it waited: tens of milliseconds a shard at
/// millions of keys, every time an exporter scraped `INFO all` (#622).
///
/// A shard nobody writes to meanwhile is counted exactly. Under writes the
/// count is approximate, as a `SCAN` is: a key written or deleted
/// meanwhile is counted or not, and keys move between buckets when the
/// table is rehashed. A resize changes how many buckets there are, and the
/// shard is then counted again (in one hold after a few tries); a rehash
/// in place, at the same size, which the table does when deleted keys
/// have used up its room to grow, cannot be seen from here, and in the
/// part not yet counted a key may then be counted twice or not at all.
///
/// One hold for the whole shard would be exact: about 20 ms for a shard
/// of a million keys (21 ns a key measured, 5 million keys on 8 vCPUs), a
/// wait for every write to it, every 10 s while `INFO all` is asked. A
/// count of rehashes kept by the table would let the walk start again on
/// one too, but needs a check on every insert, the `SET` path. For a
/// distribution refreshed every 10 seconds neither is worth it.
///
/// `between` is called with no lock held after each part (for tests).
fn keysizes_of_shard(shard: &crate::store::Shard, between: &mut dyn FnMut()) -> KeysizeBins {
    let mut tries = 0;
    'shard: loop {
        let mut bins: KeysizeBins = [[0u64; 65]; 5];
        let mut at = 0;
        let mut size = None;
        loop {
            let map = read_lock!(shard);
            let buckets = map.buckets();
            if *size.get_or_insert(buckets) != buckets {
                tries += 1;
                if tries < 3 {
                    continue 'shard;
                }
                let mut bins: KeysizeBins = [[0u64; 65]; 5];
                for value in map.values() {
                    keysize_count(&mut bins, value);
                }
                return bins;
            }
            let end = buckets.min(at + KEYSIZES_BUCKETS);
            for index in at..end {
                if let Some((_, value)) = map.bucket(index) {
                    keysize_count(&mut bins, value);
                }
            }
            at = end;
            if at >= buckets {
                return bins;
            }
            drop(map);
            between();
        }
    }
}

/// `INFO keysizes` for database `db`: for strings, lists, sets, sorted sets
/// and hashes, how many keys have a length (bytes, or items) in each
/// power-of-two bin, as Redis 8.10 writes it:
/// `db0_distrib_lists_items:1=2,4=1` (one to one, two or three; four to
/// seven). Nothing for a type with no keys.
fn keysizes(db: usize, shards: &crate::store::Db) -> String {
    const TYPES: [&str; 5] = [
        "distrib_strings_sizes",
        "distrib_lists_items",
        "distrib_sets_items",
        "distrib_zsets_items",
        "distrib_hashes_items",
    ];
    let mut bins: KeysizeBins = [[0u64; 65]; 5];
    for shard in shards.iter() {
        for (all, one) in bins
            .iter_mut()
            .zip(keysizes_of_shard(shard, &mut || {}).iter())
        {
            for (a, b) in all.iter_mut().zip(one.iter()) {
                *a += *b;
            }
        }
    }
    let mut out = String::new();
    for (kind, name) in TYPES.iter().enumerate() {
        let counts: Vec<String> = bins[kind]
            .iter()
            .enumerate()
            .filter(|(_, n)| **n > 0)
            .map(|(bin, n)| format!("{}={n}", keysizes_label(bin)))
            .collect();
        if !counts.is_empty() {
            out.push_str(&format!("db{db}_{name}:{}\r\n", counts.join(",")));
        }
    }
    out
}

/// The label of a `keysizes` bin: its least length, in 1024s past 1023.
fn keysizes_label(bin: usize) -> String {
    if bin == 0 {
        return "0".into();
    }
    let least = bin - 1;
    let suffix = ["", "K", "M", "G", "T", "P", "E"][least / 10];
    format!("{}{suffix}", 1u64 << (least % 10))
}

/// Given a command invocation (cmd[0] = command name, cmd[1..] = args),
/// return RESP-encoded key entries. Each entry is either a bulk string (GETKEYS)
/// or a 2-element array [key, [flags...]] (GETKEYSANDFLAGS).
pub(crate) fn extract_keys(args: &[&[u8]], with_flags: bool) -> Result<Vec<Vec<u8>>, String> {
    use crate::resp::write::{resp_array, resp_bulk, resp_bulk_str};

    // Helper: build a single key entry
    let make_entry = |key: &[u8], read: bool, write: bool| -> Vec<u8> {
        if !with_flags {
            return resp_bulk(key);
        }
        let mut flags = vec![];
        if read {
            flags.push(resp_bulk_str("read"));
        }
        if write {
            flags.push(resp_bulk_str("write"));
        }
        if !read && !write {
            flags.push(resp_bulk_str("read"));
        } // default read
        resp_array(vec![resp_bulk(key), resp_array(flags)])
    };

    if args.is_empty() {
        return Err("ERR Invalid arguments specified".into());
    }

    // Redis's own commands, by Redis's own description of them. What
    // follows is for the commands that are KiviDB's.
    if let Some(found) = crate::commands::key_specs::keys_of(args) {
        return match found {
            Ok(keys) => Ok(keys
                .into_iter()
                .map(|(at, flags)| {
                    if !with_flags {
                        return resp_bulk(args[at]);
                    }
                    let flags = flags
                        .split(' ')
                        .filter(|flag| !flag.is_empty())
                        .map(crate::resp::write::resp_simple)
                        .collect();
                    resp_array(vec![resp_bulk(args[at]), resp_array(flags)])
                })
                .collect()),
            Err(why) => Err(why.message().to_string()),
        };
    }

    let cmd_name = String::from_utf8_lossy(args[0]).to_ascii_uppercase();
    if !is_command(&cmd_name) {
        return Err(crate::commands::key_specs::NoKeys::NoSuchCommand
            .message()
            .to_string());
    }

    // Key position rules — (first_key, last_key, step) matching Redis's
    // COMMAND INFO format, or custom logic for variable-position commands.
    let keys: Vec<Vec<u8>> = match cmd_name.as_str() {
        // No keys
        "PING" | "ECHO" | "SELECT" | "DBSIZE" | "FLUSHDB" | "FLUSHALL" | "INFO" | "SAVE"
        | "BGSAVE" | "LASTSAVE" | "TIME" | "LOLWUT" | "COMMAND" | "CONFIG" | "DEBUG" | "QUIT"
        | "RESET" | "AUTH" | "WAIT" | "WAITAOF" | "ROLE" | "REPLICAOF" | "SLAVEOF" | "REPLCONF"
        | "PSYNC" | "LATENCY" | "SLOWLOG" | "MEMORY" | "ACL" => {
            vec![]
        }

        // Single key at position 1 (read)
        "GET" | "GETDEL" | "GETEX" | "STRLEN" | "TTL" | "PTTL" | "TYPE" | "PERSIST" | "DUMP"
        | "OBJECT" | "HGET" | "HMGET" | "HGETALL" | "HKEYS" | "HVALS" | "HLEN" | "HEXISTS"
        | "HRANDFIELD" | "HSCAN" | "HTTL" | "HPTTL" | "HEXPIRETIME" | "HPEXPIRETIME"
        | "HSTRLEN" | "LLEN" | "LRANGE" | "LINDEX" | "LPOS" | "SCARD" | "SMEMBERS"
        | "SRANDMEMBER" | "SISMEMBER" | "SMISMEMBER" | "SSCAN" | "ZCARD" | "ZRANGE"
        | "ZRANGEBYSCORE" | "ZRANGEBYLEX" | "ZREVRANGE" | "ZREVRANGEBYSCORE" | "ZREVRANGEBYLEX"
        | "ZRANK" | "ZREVRANK" | "ZSCORE" | "ZMSCORE" | "ZCOUNT" | "ZLEXCOUNT" | "ZRANDMEMBER"
        | "ZSCAN" | "XLEN" | "XRANGE" | "XREVRANGE" | "XINFO" | "XPENDING" | "KEYS" | "SCAN"
        | "RANDOMKEY" | "JSON.GET" | "JSON.TYPE" | "JSON.STRLEN" | "JSON.OBJKEYS"
        | "JSON.OBJLEN" | "JSON.ARRLEN" | "JSON.ARRINDEX" | "JSON.RESP" | "JSON.DEBUG"
        | "VCARD" | "VDIM" | "VINFO" | "VISMEMBER" | "VEMB" | "VGETATTR" | "VLINKS"
        | "VRANDMEMBER" | "VRANGE" | "VSIM" => {
            if args.len() < 2 {
                return Err("ERR Invalid arguments specified".into());
            }
            vec![make_entry(args[1], true, false)]
        }

        // Single key at position 1 (write)
        "SET" | "SETNX" | "SETEX" | "PSETEX" | "GETSET" | "APPEND" | "INCR" | "DECR" | "INCRBY"
        | "DECRBY" | "INCRBYFLOAT" | "SETRANGE" | "SETBIT" | "EXPIRE" | "PEXPIRE" | "EXPIREAT"
        | "PEXPIREAT" | "RESTORE" | "MOVE" | "SORT" | "HSET" | "HMSET" | "HSETNX" | "HDEL"
        | "HINCRBY" | "HINCRBYFLOAT" | "HEXPIRE" | "HPEXPIRE" | "HEXPIREAT" | "HPEXPIREAT"
        | "HPERSIST" | "HGETDEL" | "HGETEX" | "HSETEX" | "LPUSH" | "RPUSH" | "LPUSHX"
        | "RPUSHX" | "LPOP" | "RPOP" | "LSET" | "LINSERT" | "LREM" | "LTRIM" | "LMPOP" | "SADD"
        | "SREM" | "SPOP" | "ZADD" | "ZREM" | "ZINCRBY" | "ZPOPMIN" | "ZPOPMAX" | "ZMPOP"
        | "XADD" | "XDEL" | "XDELEX" | "XTRIM" | "XSETID" | "XGROUP" | "XACK" | "XAUTOCLAIM"
        | "XCLAIM" | "XREADGROUP" | "PFADD" | "BITOP" | "BITFIELD" | "GEOADD" | "JSON.SET"
        | "JSON.DEL" | "JSON.MSET" | "JSON.NUMINCRBY" | "JSON.NUMMULTBY" | "JSON.STRAPPEND"
        | "JSON.ARRAPPEND" | "JSON.ARRINSERT" | "JSON.ARRPOP" | "JSON.ARRTRIM" | "JSON.CLEAR"
        | "JSON.TOGGLE" | "JSON.MERGE" | "VADD" | "VREM" | "VSETATTR" | "TS.ADD" | "TS.INCRBY"
        | "TS.DECRBY" | "TS.CREATE" | "TS.DEL" | "TS.ALTER" => {
            if args.len() < 2 {
                return Err("ERR Invalid arguments specified".into());
            }
            vec![make_entry(args[1], false, true)]
        }

        // Two keys: first=src(read), second=dst(write)
        "RENAME" | "RENAMENX" | "COPY" | "LMOVE" | "RPOPLPUSH" | "SMOVE" | "BRPOPLPUSH"
        | "BLMOVE" => {
            if args.len() < 3 {
                return Err("ERR Invalid arguments specified".into());
            }
            vec![
                make_entry(args[1], true, false),
                make_entry(args[2], false, true),
            ]
        }

        // MGET: all args are keys (read)
        "MGET" | "DEL" | "EXISTS" | "UNLINK" | "TOUCH" | "PFCOUNT" => args[1..]
            .iter()
            .map(|k| make_entry(k, true, false))
            .collect(),

        // MSET / MSETNX: keys at odd positions (1,3,5...) — write
        "MSET" | "MSETNX" => args[1..]
            .iter()
            .step_by(2)
            .map(|k| make_entry(k, false, true))
            .collect(),

        // BLPOP/BRPOP: keys at 1..len-1, last arg is timeout
        "BLPOP" | "BRPOP" => {
            if args.len() < 3 {
                return Err("ERR Invalid arguments specified".into());
            }
            args[1..args.len() - 1]
                .iter()
                .map(|k| make_entry(k, false, true))
                .collect()
        }

        // LMPOP/ZMPOP: numkeys at [1], keys at [2..2+numkeys]
        "BLMPOP" | "BZMPOP" => {
            if args.len() < 4 {
                return Err("ERR Invalid arguments specified".into());
            }
            let n: usize = std::str::from_utf8(args[2])
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            args[3..3 + n.min(args.len() - 3)]
                .iter()
                .map(|k| make_entry(k, false, true))
                .collect()
        }

        // XREAD: STREAMS keyword divides keys from IDs
        "XREAD" => {
            // XREAD [COUNT n] [BLOCK ms] STREAMS key [key...] id [id...]
            let streams_pos = args.iter().position(|a| a.eq_ignore_ascii_case(b"STREAMS"));
            match streams_pos {
                Some(pos) => {
                    let remaining = args.len() - pos - 1;
                    let num_keys = remaining / 2;
                    args[pos + 1..pos + 1 + num_keys]
                        .iter()
                        .map(|k| make_entry(k, true, false))
                        .collect()
                }
                None => return Err("ERR Invalid arguments specified".into()),
            }
        }

        // SORT with STORE: key at [1], destination at STORE arg
        "SORT_RO" => {
            if args.len() < 2 {
                return Err("ERR Invalid arguments specified".into());
            }
            vec![make_entry(args[1], true, false)]
        }

        // GEORADIUS* — source key at [1], optional STORE at named position
        "GEORADIUS" | "GEORADIUSBYMEMBER" | "GEOSEARCH" | "GEOSEARCHSTORE" | "GEODIST"
        | "GEOPOS" => {
            if args.len() < 2 {
                return Err("ERR Invalid arguments specified".into());
            }
            let mut keys = vec![make_entry(args[1], true, false)];
            // Check for STORE/STOREDIST option
            for (i, a) in args.iter().enumerate() {
                if a.eq_ignore_ascii_case(b"STORE") || a.eq_ignore_ascii_case(b"STOREDIST") {
                    if let Some(dst) = args.get(i + 1) {
                        keys.push(make_entry(dst, false, true));
                    }
                    break;
                }
            }
            keys
        }

        // ZDIFFSTORE / ZUNIONSTORE / ZINTERSTORE / ZRANGESTORE: dst at [1], sources follow
        "ZDIFFSTORE" | "ZUNIONSTORE" | "ZINTERSTORE" => {
            if args.len() < 4 {
                return Err("ERR Invalid arguments specified".into());
            }
            let n: usize = std::str::from_utf8(args[2])
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let mut keys = vec![make_entry(args[1], false, true)];
            keys.extend(
                args[3..3 + n.min(args.len() - 3)]
                    .iter()
                    .map(|k| make_entry(k, true, false)),
            );
            keys
        }

        "ZRANGESTORE" => {
            if args.len() < 3 {
                return Err("ERR Invalid arguments specified".into());
            }
            vec![
                make_entry(args[1], false, true),
                make_entry(args[2], true, false),
            ]
        }

        // SDIFFSTORE / SUNIONSTORE / SINTERSTORE: dst at [1], rest are sources
        "SDIFFSTORE" | "SUNIONSTORE" | "SINTERSTORE" => {
            if args.len() < 2 {
                return Err("ERR Invalid arguments specified".into());
            }
            let mut keys = vec![make_entry(args[1], false, true)];
            keys.extend(args[2..].iter().map(|k| make_entry(k, true, false)));
            keys
        }

        // ZDIFF / ZUNION / ZINTER: numkeys at [1], keys follow
        "ZDIFF" | "ZUNION" | "ZINTER" | "SINTER" | "SUNION" | "SDIFF" => {
            // ZDIFF numkeys key [key ...]
            let start = if matches!(cmd_name.as_str(), "SINTER" | "SUNION" | "SDIFF") {
                1
            } else {
                2
            };
            let n: usize = if start == 2 {
                std::str::from_utf8(args[1])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0)
            } else {
                args.len() - 1
            };
            args[start..start + n.min(args.len() - start)]
                .iter()
                .map(|k| make_entry(k, true, false))
                .collect()
        }

        // PFMERGE: dst at [1], sources at [2..]
        "PFMERGE" => {
            if args.len() < 2 {
                return Err("ERR Invalid arguments specified".into());
            }
            let mut keys = vec![make_entry(args[1], false, true)];
            keys.extend(args[2..].iter().map(|k| make_entry(k, true, false)));
            keys
        }

        // LCS: two keys
        "LCS" => {
            if args.len() < 3 {
                return Err("ERR Invalid arguments specified".into());
            }
            vec![
                make_entry(args[1], true, false),
                make_entry(args[2], true, false),
            ]
        }

        // FCALL / FCALL_RO — same shape as EVAL: fname, numkeys, keys..., argv...
        "FCALL" => {
            if args.len() < 3 {
                return Err("ERR Invalid arguments specified".into());
            }
            let n: usize = std::str::from_utf8(args[2])
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            if n > args.len() - 3 {
                return Err("ERR Invalid arguments specified".into());
            }
            args[3..3 + n]
                .iter()
                .map(|k| make_entry(k, true, true))
                .collect()
        }

        "FCALL_RO" => {
            if args.len() < 3 {
                return Err("ERR Invalid arguments specified".into());
            }
            let n: usize = std::str::from_utf8(args[2])
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            if n > args.len() - 3 {
                return Err("ERR Invalid arguments specified".into());
            }
            args[3..3 + n]
                .iter()
                .map(|k| make_entry(k, true, false))
                .collect()
        }

        _ => {
            // Unknown command — return error matching Redis behaviour
            return Err("ERR Invalid arguments specified".to_string());
        }
    };

    Ok(keys)
}

pub fn handle(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;

    match name {
        "SAVE" => {
            // It blocks its own client, as in Redis, and no other: with
            // `--io-model tokio` the worker it runs on hands its place over
            // first, or no client is accepted or read until it is done
            // (#605, as `CONFIG SET appendonly yes` does).
            let result = crate::cluster::migration::off_the_runtime(|| {
                crate::replication::save_dataset(ctx.store, ctx.index_store, ctx.repl_state)
            });
            match result {
                Ok(_) => {
                    crate::stats::reset_rdb_changes();
                    Some(resp_simple("OK"))
                }
                Err(e) => Some(resp_error(&format!("ERR save failed : {e}"))),
            }
        }

        "BGSAVE" => {
            // One at a time, as in Redis — and so that "in progress" means
            // one particular save.
            if crate::stats::BGSAVE_IN_PROGRESS.swap(true, Ordering::AcqRel) {
                return Some(resp_error("ERR Background save already in progress"));
            }
            let store = Arc::clone(ctx.store);
            let index_store_snap = Arc::clone(ctx.index_store);
            let repl_state = Arc::clone(ctx.repl_state);

            tokio::spawn(async move {
                let started = std::time::Instant::now();
                let snap = tokio::task::spawn_blocking(move || {
                    crate::replication::save_dataset(&store, &index_store_snap, &repl_state)
                })
                .await;
                // `LASTSAVE` is the last save that *succeeded*. It used to
                // move on whatever happened, so a failed background save
                // looked like a finished one to anything waiting on it.
                let ok = matches!(snap, Ok(Ok(())));
                if ok {
                    crate::stats::reset_rdb_changes();
                    crate::stats::LAST_BGSAVE_SECS
                        .store(started.elapsed().as_secs() as i64, Ordering::Relaxed);
                } else {
                    let why = match snap {
                        Ok(Err(e)) => e.to_string(),
                        Err(e) => format!("the save task failed: {e}"),
                        Ok(Ok(())) => unreachable!(),
                    };
                    log_error!(
                        "Background save failed: {why}. The previous dump.kdb is kept; \
                         LASTSAVE does not move and INFO persistence shows \
                         rdb_last_bgsave_status:err. Check free disk space and write \
                         permission on the data directory"
                    );
                }
                crate::stats::LAST_BGSAVE_OK.store(ok, Ordering::Release);
                crate::stats::BGSAVE_IN_PROGRESS.store(false, Ordering::Release);
            });

            Some(resp_simple("Background saving started"))
        }

        "LASTSAVE" => Some(resp_integer(crate::stats::last_save())),

        "FLUSHDB" => {
            let lazy = match flush_is_async(cmd) {
                Ok(lazy) => lazy,
                Err(reply) => return Some(reply),
            };
            let db = *ctx.selected_db;
            touch_watched_keys(ctx, Some(db));
            let flushed = if lazy {
                let (values, flushed) = crate::store::take_db(&ctx.store[db]);
                crate::lazyfree::free_later(values, flushed.keys as u64);
                // Nothing to give back to the OS yet: the values are freed
                // in the background.
                crate::store::Flushed::default()
            } else {
                let flushed = crate::store::clear_db(&ctx.store[db]);
                count_flushed_in_place(&flushed);
                flushed
            };
            let indexes = crate::vector::drop_indexes_for_db(ctx.index_store, db);

            // Locks are all released by now — malloc_trim walks every arena and
            // must not run with shards held.
            if flushed.should_trim() || indexes > 0 {
                crate::commands::memory::purge_allocator();
            }

            Some(resp_simple("OK"))
        }

        "INFO" => {
            // INFO [section [section ...]]
            //
            // No section, or `default`: every section but the long ones that
            // are only given when asked for (`commandstats`,
            // `latencystats`). `all` and `everything`: those too.
            let sections: Vec<String> = cmd[1..]
                .iter()
                .map(|raw| String::from_utf8_lossy(raw).to_ascii_lowercase())
                .collect();
            let asked = |s: &str| sections.iter().any(|section| section == s);
            let all = asked("all") || asked("everything");
            let default = sections.is_empty() || asked("default");

            let mut out = String::new();

            // `keysizes` is in Redis's default INFO, kept as keys change. Here
            // it is counted when asked for, a pass over every key: not in the
            // default, which monitoring asks for every few seconds.
            let want = |s: &str| {
                all || asked(s)
                    || (default && !matches!(s, "commandstats" | "latencystats" | "keysizes"))
            };

            if want("server") {
                out.push_str("# Server\r\n");
                // The Redis release whose commands and replies this server
                // follows. Clients read it to decide what they may send.
                out.push_str(&format!("redis_version:{REDIS_VERSION}\r\n"));
                // There is no Redis build behind that version: these are
                // the fields a Redis build fills in, for the clients and
                // tools that read them.
                out.push_str("redis_git_sha1:00000000\r\n");
                out.push_str("redis_git_dirty:0\r\n");
                out.push_str(&format!(
                    "redis_build_id:{}\r\n",
                    env!("CARGO_PKG_VERSION").replace('.', "")
                ));
                out.push_str(if ctx.cluster.enabled {
                    "redis_mode:cluster\r\n"
                } else {
                    "redis_mode:standalone\r\n"
                });
                out.push_str(&format!("arch_bits:{}\r\n", usize::BITS));
                out.push_str(&format!("kividb_version:{}\r\n", env!("CARGO_PKG_VERSION")));
                // Which of the release's builds: default, lua, tls or full
                // (the `kividb` and `kividb-full` packages, #732).
                out.push_str(&format!(
                    "kividb_variant:{}\r\n",
                    crate::update_check::variant()
                ));
                // That a replica of this server need not expire keys
                // itself: each one expired here is sent to it as a `DEL`
                // (`replication::expiry_propagated_by`).
                out.push_str(crate::replication::EXPIRY_PROPAGATED_INFO);
                out.push_str("\r\n");
                // Not on a replica of a master that ignores the options.
                if ctx.repl_state.keeps_vset_quant() {
                    out.push_str(crate::replication::VSET_QUANT_INFO);
                    out.push_str("\r\n");
                }
                // And each hash field expired here, as an `HDEL`
                // (`replication::field_expiry_propagated_by`). Not said under
                // `KIVIDB_TEST_NO_FIELD_EXPIRY_CAPABILITY`, for the tests of a
                // replica of a master that does not say it.
                if !crate::replication::hides_field_expiry_capability() {
                    out.push_str(crate::replication::FIELD_EXPIRY_PROPAGATED_INFO);
                    out.push_str("\r\n");
                }
                out.push_str(&format!(
                    "os:{} {}\r\n",
                    std::env::consts::OS,
                    std::env::consts::ARCH
                ));
                out.push_str(&format!(
                    "tcp_port:{}\r\n",
                    crate::listen::current_port(
                        crate::listen::server_of(ctx.config),
                        crate::listen::Kind::Plain,
                    )
                ));
                out.push_str(&format!("process_id:{}\r\n", std::process::id()));
                out.push_str(&format!("run_id:{}\r\n", crate::stats::RUN_ID.as_str()));
                let uptime = ctx.started_at.elapsed().as_secs();
                out.push_str(&format!("uptime_in_seconds:{uptime}\r\n"));
                out.push_str(&format!("uptime_in_days:{}\r\n", uptime / 86400));
                out.push_str("hz:10\r\n");
                out.push_str("configured_hz:10\r\n");
                out.push_str(&format!(
                    "multiplexing_api:{}\r\n",
                    crate::stats::multiplexing_api()
                ));
                out.push_str("atomicvar_api:atomic-builtin\r\n");
                out.push_str("gcc_version:0.0.0\r\n");
                // The file it was started with, if there is one.
                let config_file = ctx
                    .config
                    .configfile_loaded
                    .then_some(std::path::Path::new(&ctx.config.configfile))
                    .and_then(|path| std::path::absolute(path).ok())
                    .map(|path| path.to_string_lossy().into_owned())
                    .unwrap_or_default();
                out.push_str(&format!("config_file:{config_file}\r\n"));
                // Off is -1, as `CONFIG GET` writes it.
                out.push_str(&format!(
                    "slowlog_log_slower_than:{}\r\n",
                    match ctx.slowlog.threshold_us() {
                        u64::MAX => -1,
                        us => us as i64,
                    }
                ));
                out.push_str(&format!("slowlog_max_len:{}\r\n", ctx.slowlog.len_cap()));
                out.push_str(&format!(
                    "executable:{}\r\n",
                    std::env::current_exe()
                        .map(|path| path.to_string_lossy().into_owned())
                        .unwrap_or_default()
                ));
                out.push_str("\r\n");
            }

            if want("tls") {
                let tls_section = {
                    // As they are now: `CONFIG SET tls-port` opens, moves
                    // and closes the port.
                    let tls_port = crate::listen::current_port(
                        crate::listen::server_of(ctx.config),
                        crate::listen::Kind::Tls,
                    );
                    let tls_enabled = u8::from(tls_port != 0);
                    let tls_replication = u8::from(crate::node_tls::replication());
                    let tls_cluster = u8::from(crate::node_tls::cluster());

                    format!(
                        "# tls\r\n\
                         tls_enabled:{tls_enabled}\r\n\
                         tls_port:{tls_port}\r\n\
                         tls_replication:{tls_replication}\r\n\
                         tls_cluster:{tls_cluster}\r\n\
                         \r\n"
                    )
                };
                out.push_str(&tls_section);
            }

            if want("cluster") {
                // As Redis: tools (redis-cli --cluster among them) and
                // client libraries read it to tell a cluster node from a
                // standalone server.
                out.push_str("# Cluster\r\n");
                out.push_str(&format!(
                    "cluster_enabled:{}\r\n",
                    u8::from(ctx.config.cluster_enabled)
                ));
                // KiviDB's own: the slots this master holds keys of but
                // does not serve, and those keys, as last counted (#517).
                if ctx.config.cluster_enabled {
                    let (slots, keys) = ctx.cluster.orphans.last_count();
                    out.push_str(&format!(
                        "cluster_orphan_slots:{slots}\r\ncluster_orphan_keys:{keys}\r\n"
                    ));
                }
                out.push_str("\r\n");
            }

            if want("keyspace") {
                out.push_str("# Keyspace\r\n");
                let now = crate::commands::helpers::unix_ms(std::time::SystemTime::now());
                for (i, db) in ctx.store.iter().enumerate() {
                    // Three numbers a shard, each kept as keys change.
                    let (keys, expires, deadlines) =
                        db.iter().fold((0usize, 0usize, 0u128), |(k, e, d), shard| {
                            let map = read_lock!(shard);
                            (k + map.len(), e + map.ttl_len(), d + map.ttl_deadline_sum())
                        });
                    if keys > 0 || expires > 0 {
                        let avg_ttl = avg_ttl_ms(expires, deadlines, now);
                        out.push_str(&format!(
                            "db{}:keys={},expires={},avg_ttl={}\r\n",
                            i, keys, expires, avg_ttl
                        ));
                    }
                }
                out.push_str("\r\n");
            }

            if want("keysizes") {
                out.push_str("# Keysizes\r\n");
                out.push_str(&keysizes_section(ctx, asked("keysizes")));
                out.push_str("\r\n");
            }

            if want("stats") {
                out.push_str("# Stats\r\n");
                out.push_str(&format!(
                    "total_connections_received:{}\r\n",
                    crate::stats::TOTAL_CONNECTIONS_RECEIVED.load(Ordering::Relaxed)
                ));
                out.push_str(&format!(
                    "total_commands_processed:{}\r\n",
                    crate::stats::TOTAL_COMMANDS_PROCESSED.total()
                ));
                out.push_str(&format!(
                    "instantaneous_ops_per_sec:{}\r\n",
                    crate::stats::INSTANTANEOUS_OPS.load(Ordering::Relaxed)
                ));
                // Full resyncs served, and partial ones served and refused.
                out.push_str(&format!(
                    "sync_full:{}\r\nsync_partial_ok:{}\r\nsync_partial_err:{}\r\n",
                    ctx.repl_state.sync_full.load(Ordering::Relaxed),
                    ctx.repl_state.sync_partial_ok.load(Ordering::Relaxed),
                    ctx.repl_state.sync_partial_err.load(Ordering::Relaxed)
                ));
                out.push_str(&format!(
                    "total_net_input_bytes:{}\r\n",
                    crate::stats::TOTAL_NET_INPUT_BYTES.load(Ordering::Relaxed)
                ));
                out.push_str(&format!(
                    "total_net_output_bytes:{}\r\n",
                    crate::stats::TOTAL_NET_OUTPUT_BYTES.load(Ordering::Relaxed)
                ));
                out.push_str(&format!(
                    "instantaneous_input_kbps:{:.2}\r\n",
                    crate::stats::INSTANTANEOUS_INPUT_BPS.load(Ordering::Relaxed) as f64 / 1024.0
                ));
                out.push_str(&format!(
                    "instantaneous_output_kbps:{:.2}\r\n",
                    crate::stats::INSTANTANEOUS_OUTPUT_BPS.load(Ordering::Relaxed) as f64 / 1024.0
                ));
                out.push_str(&format!(
                    "rejected_connections:{}\r\n",
                    crate::stats::REJECTED_CONNECTIONS.load(Ordering::Relaxed)
                ));
                out.push_str(&format!(
                    "total_reads_processed:{}\r\n",
                    crate::stats::TOTAL_READS_PROCESSED.total()
                ));
                out.push_str(&format!(
                    "total_writes_processed:{}\r\n",
                    crate::stats::TOTAL_WRITES_PROCESSED.total()
                ));
                out.push_str(&format!(
                    "keyspace_hits:{}\r\n",
                    crate::stats::KEYSPACE_HITS.total()
                ));
                out.push_str(&format!(
                    "keyspace_misses:{}\r\n",
                    crate::stats::KEYSPACE_MISSES.total()
                ));
                out.push_str(&format!(
                    "expired_keys:{}\r\n",
                    crate::stats::EXPIRED_KEYS.load(Ordering::Relaxed)
                ));
                out.push_str(&format!(
                    "expired_subkeys:{}\r\n",
                    crate::stats::EXPIRED_SUBKEYS.load(Ordering::Relaxed)
                ));
                out.push_str(&format!(
                    "expired_keys_active:{}\r\n",
                    crate::stats::EXPIRED_KEYS_ACTIVE.load(Ordering::Relaxed)
                ));
                out.push_str(&format!(
                    "evicted_keys:{}\r\n",
                    crate::stats::EVICTED_KEYS.load(Ordering::Relaxed)
                ));
                out.push_str(&format!(
                    "evicted_scripts:{}\r\n",
                    crate::stats::EVICTED_SCRIPTS.load(Ordering::Relaxed)
                ));
                // Slowlog totals (Redis 8.8). Milliseconds to two places,
                // as Redis writes them (`0.00`).
                let (slow_count, slow_us_sum, slow_us_max) = ctx.slowlog.totals();
                out.push_str(&format!("slowlog_commands_count:{slow_count}\r\n"));
                out.push_str(&format!(
                    "slowlog_commands_time_ms_sum:{:.2}\r\n",
                    slow_us_sum as f64 / 1000.0
                ));
                out.push_str(&format!(
                    "slowlog_commands_time_ms_max:{:.2}\r\n",
                    slow_us_max as f64 / 1000.0
                ));
                out.push_str(&format!(
                    "client_output_buffer_limit_disconnections:{}\r\n",
                    crate::stats::OUTPUT_BUFFER_LIMIT_DISCONNECTIONS.load(Ordering::Relaxed)
                ));
                out.push_str(&format!(
                    "total_error_replies:{}\r\n",
                    crate::stats::TOTAL_ERROR_REPLIES.load(Ordering::Relaxed)
                ));
                // Redis INFO field — KiviDB does not cache MIGRATE sockets; keep at 0 for Tcl parity.
                out.push_str("migrate_cached_sockets:0\r\n");
                out.push_str("\r\n");
            }

            if want("errorstats") {
                out.push_str(&crate::stats::format_info_errorstats());
            }

            if want("cpu") {
                let (user, system) = cpu_seconds();
                out.push_str("# CPU\r\n");
                out.push_str(&format!("used_cpu_sys:{system:.6}\r\n"));
                out.push_str(&format!("used_cpu_user:{user:.6}\r\n"));
                // Nothing is forked: there are no children to have used any.
                out.push_str("used_cpu_sys_children:0.000000\r\n");
                out.push_str("used_cpu_user_children:0.000000\r\n");
                out.push_str("\r\n");
            }

            // Only on request, as in Redis: `INFO commandstats`, `all` or
            // `everything`.
            if want("commandstats") {
                out.push_str("# Commandstats\r\n");
                let slow: ahash::AHashMap<String, (u64, u64, u64)> =
                    ctx.slowlog.per_command_totals().into_iter().collect();
                for (name, totals) in crate::cmdstats::totals() {
                    out.push_str(&format!(
                        "cmdstat_{name}:calls={},usec={},usec_per_call={:.2},rejected_calls={},\
                         failed_calls={}",
                        totals.calls,
                        totals.usec,
                        totals.usec as f64 / totals.calls.max(1) as f64,
                        totals.rejected,
                        totals.failed
                    ));
                    // The slowlog fields Redis 8.8 added to each command's
                    // line, for a command that has been in the slowlog.
                    if let Some((count, sum_us, max_us)) = slow.get(&name) {
                        out.push_str(&format!(
                            ",slowlog_count={count},slowlog_time_ms_sum={},\
                             slowlog_time_ms_max={}",
                            sum_us / 1000,
                            max_us / 1000
                        ));
                    }
                    out.push_str("\r\n");
                }
                out.push_str("\r\n");
            }

            // Each command's percentiles, as Redis 7 gives them
            // (`latency-tracking`, `latency-tracking-info-percentiles`): the
            // section was empty (#158).
            if want("latencystats")
                && remembered_config("latency-tracking").as_deref() != Some("no")
            {
                out.push_str("# Latencystats\r\n");
                let percentiles: Vec<f64> = parse_latency_percentiles(
                    &remembered_config("latency-tracking-info-percentiles").unwrap_or_default(),
                )
                .unwrap_or_default();
                for (name, totals) in crate::cmdstats::totals() {
                    let line: Vec<String> = percentiles
                        .iter()
                        .map(|p| {
                            format!(
                                "p{p}={:.3}",
                                crate::cmdstats::percentile_usec(&totals.buckets, *p)
                            )
                        })
                        .collect();
                    out.push_str(&format!(
                        "latency_percentiles_usec_{name}:{}\r\n",
                        line.join(",")
                    ));
                }
                out.push_str("\r\n");
            }

            if want("replication") {
                let role = ctx.repl_state.role();
                let offset = ctx
                    .repl_state
                    .repl_offset
                    .load(std::sync::atomic::Ordering::Relaxed);
                let connected = ctx
                    .repl_state
                    .connected_replicas
                    .load(std::sync::atomic::Ordering::Relaxed);
                let repl_id = &ctx.repl_state.repl_id();

                out.push_str("# Replication\r\n");
                out.push_str(&format!("role:{role}\r\n"));

                if role == "master" {
                    out.push_str(&format!("connected_slaves:{connected}\r\n"));
                    // As Redis: the count `min-replicas-to-write` goes by,
                    // shown while it is on.
                    let rs = &ctx.repl_state;
                    let max_lag = rs
                        .min_replicas_max_lag
                        .load(std::sync::atomic::Ordering::Relaxed);
                    let wanted = rs
                        .min_replicas_to_write
                        .load(std::sync::atomic::Ordering::Relaxed);
                    if wanted != 0 && max_lag != 0 {
                        out.push_str(&format!(
                            "min_slaves_good_slaves:{}\r\n",
                            rs.good_replica_count(max_lag)
                        ));
                    }
                    // `state` is where the replica really is: wait_bgsave
                    // while its dataset is prepared, send_bulk while it is
                    // sent, online once the replica has loaded it and
                    // acknowledged. (It used to read `online` from the moment
                    // the replica connected.)
                    // `queued` (KiviDB's): the bytes of the stream waiting
                    // to be sent to it.
                    for (idx, (ip, port, slave_off, lag, state, queued)) in
                        replica_rows(ctx.repl_state, offset).iter().enumerate()
                    {
                        out.push_str(&format!(
                            "slave{idx}:ip={ip},port={port},state={state},offset={slave_off},lag={lag},queued={queued}\r\n"
                        ));
                    }
                    let load = |v: &std::sync::atomic::AtomicU64| {
                        v.load(std::sync::atomic::Ordering::Relaxed)
                    };
                    // KiviDB extension: a cluster master that came back
                    // without its dataset, and serves none of its slots
                    // until a replica takes over (`LostDataset`).
                    if ctx.cluster.lost_dataset() {
                        out.push_str("master_lost_dataset:1\r\n");
                    }
                    out.push_str(&format!("master_replid:{repl_id}\r\n"));
                    let (replid2, second) = match ctx.repl_state.repl_id2.read().as_ref() {
                        Some((id, until)) => (id.clone(), *until as i64),
                        None => ("0".repeat(40), -1),
                    };
                    out.push_str(&format!("master_replid2:{replid2}\r\n"));
                    out.push_str(&format!("second_repl_offset:{second}\r\n"));
                    out.push_str(&format!("master_repl_offset:{offset}\r\n"));
                    out.push_str(&format!(
                        "repl_backlog_active:{}\r\n",
                        ctx.repl_state
                            .backlog_active
                            .load(std::sync::atomic::Ordering::Relaxed)
                            as u8
                    ));
                    out.push_str(&format!(
                        "repl_backlog_size:{}\r\n",
                        load(&ctx.repl_state.backlog_size)
                    ));
                    out.push_str(&format!(
                        "repl_backlog_first_byte_offset:{}\r\n",
                        load(&ctx.repl_state.backlog_first_offset)
                    ));
                    out.push_str(&format!(
                        "repl_backlog_histlen:{}\r\n",
                        load(&ctx.repl_state.backlog_histlen)
                    ));
                } else {
                    // We're a replica — prefer the live replication target over
                    // the boot-time `--replicaof`, which REPLICAOF never updates.
                    let (master_host, master_port) = ctx
                        .repl_state
                        .master_addr()
                        .or_else(|| ctx.config.replicaof_addr())
                        .unwrap_or_else(|| ("unknown".into(), 0));
                    // "Up" means synced and streaming. A link that is still
                    // connecting, or whose master has gone silent past
                    // `repl-timeout`, is down — see `ReplState::set_link_state`.
                    let link_up = ctx
                        .repl_state
                        .slave_link_up
                        .load(std::sync::atomic::Ordering::Relaxed);
                    out.push_str(&format!("master_host:{master_host}\r\n"));
                    out.push_str(&format!("master_port:{master_port}\r\n"));
                    out.push_str(if link_up {
                        "master_link_status:up\r\n"
                    } else {
                        "master_link_status:down\r\n"
                    });
                    // Seconds since a byte actually arrived from the master;
                    // -1 while the link is down.
                    out.push_str(&format!(
                        "master_last_io_seconds_ago:{}\r\n",
                        ctx.repl_state.master_last_io_seconds_ago()
                    ));
                    let sync_ip = ctx
                        .repl_state
                        .slave_sync_in_progress
                        .load(std::sync::atomic::Ordering::Relaxed)
                        || ctx.repl_state.link_state() == crate::replication::LinkState::Sync;
                    out.push_str(&format!(
                        "master_sync_in_progress:{}\r\n",
                        if sync_ip { 1 } else { 0 }
                    ));
                    out.push_str(&format!("slave_repl_offset:{offset}\r\n"));
                    if let Some(down_for) = ctx.repl_state.master_link_down_since_seconds() {
                        out.push_str(&format!("master_link_down_since_seconds:{down_for}\r\n"));
                    }
                    // Sentinel never promotes a replica of priority 0, and
                    // lists only one that is announced (#443).
                    out.push_str(&format!(
                        "slave_priority:{}\r\n",
                        ctx.repl_state
                            .replica_priority
                            .load(std::sync::atomic::Ordering::Relaxed)
                    ));
                    out.push_str("slave_read_only:1\r\n");
                    out.push_str(&format!(
                        "replica_announced:{}\r\n",
                        ctx.repl_state
                            .replica_announced
                            .load(std::sync::atomic::Ordering::Relaxed)
                            as u8
                    ));
                    // KiviDB extensions: the full-resync dataset guard.
                    out.push_str(&format!(
                        "replica_full_sync_guard:{}\r\n",
                        ctx.repl_state.full_sync_guard().as_str()
                    ));
                    out.push_str(&format!(
                        "replica_full_syncs_refused:{}\r\n",
                        ctx.repl_state
                            .full_syncs_refused
                            .load(std::sync::atomic::Ordering::Relaxed)
                    ));
                    if let Some(reason) = ctx.repl_state.last_sync_refusal.read().as_ref() {
                        // One line: INFO is line-oriented.
                        let reason = reason.replace(['\r', '\n'], " ");
                        out.push_str(&format!("replica_last_sync_refusal:{reason}\r\n"));
                    }
                }
                out.push_str("\r\n");
            }

            if want("memory") {
                let (rss, _, _) = crate::commands::memory::read_proc_status_pub();
                let maxmem = crate::RUNTIME_MAXMEMORY.load(std::sync::atomic::Ordering::Relaxed);
                // What the process has allocated — what `maxmemory` is
                // measured against — as distinct from what the operating
                // system has given it, which is the RSS below.
                let (used, peak) = crate::commands::memory::used_and_peak();
                let (frag_ratio, frag_bytes) = crate::commands::memory::fragmentation(rss, used);
                // What the server had allocated before any data; KiviDB
                // counts no other overhead apart from the dataset.
                let startup_memory = crate::commands::memory::startup_used().min(used);
                let overhead = startup_memory;
                let dataset = used.saturating_sub(overhead);
                let dataset_perc = if used > 0 {
                    dataset as f64 / used as f64 * 100.0
                } else {
                    0.0
                };

                out.push_str("# Memory\r\n");
                out.push_str(&format!("used_memory:{used}\r\n"));
                out.push_str(&format!(
                    "used_memory_human:{}\r\n",
                    crate::commands::memory::human_bytes_pub(used)
                ));
                out.push_str(&format!("used_memory_rss:{rss}\r\n"));
                // KiviDB's slabs of small string values that hold something
                // (#113): `used_memory` counts the blocks in them.
                let fill = crate::value_slab::fill();
                out.push_str(&format!("value_slab_bytes:{}\r\n", fill.slab_bytes));
                out.push_str(&format!("value_slab_live_bytes:{}\r\n", fill.live_bytes));
                // Times freed memory was given back to the system after a
                // large fall in what is allocated (#753).
                out.push_str(&format!(
                    "allocator_trims:{}\r\n",
                    crate::give_back::trims()
                ));
                // ... and times one was put off while the server was busy.
                out.push_str(&format!(
                    "allocator_trims_skipped_busy:{}\r\n",
                    crate::give_back::skipped_busy()
                ));
                out.push_str(&format!(
                    "used_memory_rss_human:{}\r\n",
                    crate::commands::memory::human_bytes_pub(rss)
                ));
                out.push_str(&format!("used_memory_peak:{peak}\r\n"));
                out.push_str(&format!(
                    "used_memory_peak_human:{}\r\n",
                    crate::commands::memory::human_bytes_pub(peak)
                ));
                out.push_str(&format!(
                    "used_memory_peak_perc:{:.2}%\r\n",
                    if peak > 0 {
                        used as f64 / peak as f64 * 100.0
                    } else {
                        100.0
                    }
                ));
                out.push_str(&format!("used_memory_overhead:{overhead}\r\n"));
                out.push_str(&format!("used_memory_startup:{startup_memory}\r\n"));
                out.push_str(&format!("used_memory_dataset:{dataset}\r\n"));
                out.push_str(&format!("used_memory_dataset_perc:{dataset_perc:.2}%\r\n"));
                // The VM `EVAL` scripts run in (`[s used_memory_lua]` in
                // unit/scripting.tcl).
                // `used_memory_vm_eval` is its name since Redis 7, and
                // `used_memory_lua` the old one.
                #[cfg(feature = "lua")]
                out.push_str(&format!(
                    "used_memory_lua:{eval}\r\nused_memory_vm_eval:{eval}\r\nused_memory_scripts_eval:{scripts}\r\n",
                    eval = crate::commands::lua::eval_vm_bytes(),
                    scripts = crate::commands::lua::cached_scripts_bytes()
                ));
                #[cfg(not(feature = "lua"))]
                out.push_str(
                    "used_memory_lua:0\r\nused_memory_vm_eval:0\r\nused_memory_scripts_eval:0\r\n",
                );
                // The VM functions are loaded into.
                #[cfg(feature = "lua")]
                out.push_str(&format!(
                    "used_memory_vm_functions:{}\r\n",
                    crate::commands::lua::functions_vm_bytes()
                ));
                #[cfg(not(feature = "lua"))]
                out.push_str("used_memory_vm_functions:0\r\n");
                #[cfg(feature = "lua")]
                out.push_str(&format!(
                    "used_memory_vm_total:{}\r\n",
                    crate::commands::lua::eval_vm_bytes()
                        + crate::commands::lua::functions_vm_bytes()
                ));
                #[cfg(not(feature = "lua"))]
                out.push_str("used_memory_vm_total:0\r\n");
                #[cfg(feature = "lua")]
                out.push_str(&format!(
                    "number_of_cached_scripts:{}\r\n",
                    crate::commands::lua::cached_script_count()
                ));
                #[cfg(not(feature = "lua"))]
                out.push_str("number_of_cached_scripts:0\r\n");
                out.push_str(&format!("allocator_allocated:{used}\r\n"));
                out.push_str(&format!("allocator_active:{rss}\r\n"));
                out.push_str(&format!("allocator_resident:{rss}\r\n"));
                out.push_str(&format!("maxmemory:{maxmem}\r\n"));
                out.push_str(&format!(
                    "maxmemory_human:{}\r\n",
                    crate::commands::memory::human_bytes_pub(maxmem)
                ));
                out.push_str(&format!(
                    "maxmemory_policy:{}\r\n",
                    crate::eviction::policy().name()
                ));
                out.push_str(&format!("mem_fragmentation_ratio:{frag_ratio:.4}\r\n"));
                out.push_str(&format!("mem_fragmentation_bytes:{frag_bytes}\r\n"));
                // What a snapshot being written keeps alive (#284): in
                // Redis, the forked child's, outside `used_memory` altogether.
                out.push_str(&format!(
                    "mem_not_counted_for_evict:{}\r\n",
                    crate::memory_use::not_counted()
                ));
                // The backlog's bytes, and the clients' query buffers: they
                // were 0 (#152).
                out.push_str(&format!(
                    "mem_replication_backlog:{}\r\n",
                    ctx.repl_state.backlog_histlen.load(Ordering::Relaxed)
                ));
                let (clients_normal, clients_replicas) = ctx.client_registry.buffer_totals();
                out.push_str(&format!("mem_clients_slaves:{clients_replicas}\r\n"));
                out.push_str(&format!("mem_clients_normal:{clients_normal}\r\n"));
                out.push_str("mem_aof_buffer:0\r\n");
                // `active_defrag_running`, `_hits`, `_misses`, `_panics`
                // and `_disabled_by_panics` (#358).
                out.push_str(&crate::compaction::info());
                out.push_str(&format!(
                    "lazyfree_pending_objects:{}\r\n",
                    crate::lazyfree::pending_objects()
                ));
                out.push_str(&format!(
                    "lazyfreed_objects:{}\r\n",
                    crate::lazyfree::freed_objects()
                ));
                out.push_str("\r\n");
            }

            if want("clients") {
                let count = ctx.client_registry.connected_clients();
                let blocked = crate::stats::BLOCKED_CLIENTS.load(Ordering::Relaxed);
                let tracking = crate::tracking::tracking_client_count();
                out.push_str("# Clients\r\n");
                out.push_str(&format!("connected_clients:{count}\r\n"));
                out.push_str(&format!("blocked_clients:{blocked}\r\n"));
                out.push_str(&format!("tracking_clients:{tracking}\r\n"));
                let (keys, items, prefixes) = ctx.tracking_table.totals();
                out.push_str(&format!("tracking_total_keys:{keys}\r\n"));
                out.push_str(&format!("tracking_total_items:{items}\r\n"));
                out.push_str(&format!("tracking_total_prefixes:{prefixes}\r\n"));
                out.push_str(&format!(
                    "clients_in_timeout_table:{}\r\n",
                    crate::client_registry::ClientRegistryInner::in_timeout_table()
                ));
                let (blocking_keys, on_nokey) = ctx.wait_table.blocking_keys();
                out.push_str(&format!("total_blocking_keys:{blocking_keys}\r\n"));
                out.push_str(&format!("total_blocking_keys_on_nokey:{on_nokey}\r\n"));
                out.push_str(&format!(
                    "maxclients:{}\r\n",
                    remembered_config("maxclients").unwrap_or_else(|| "10000".to_string())
                ));
                out.push_str("\r\n");
            }

            if want("persistence") {
                let lastsave = crate::stats::last_save();
                let chg = crate::stats::rdb_changes_since_save();
                out.push_str("# Persistence\r\n");
                // 1 on a replica while a full sync is being loaded: the
                // dataset is not there to be read.
                out.push_str(&format!(
                    "loading:{}\r\n",
                    ctx.repl_state
                        .slave_sync_in_progress
                        .load(Ordering::Relaxed) as u8
                ));
                out.push_str("async_loading:0\r\n");
                out.push_str(&format!(
                    "aof_enabled:{}\r\n",
                    if ctx.aof_enabled { 1 } else { 0 }
                ));
                out.push_str(&format!("rdb_last_save_time:{lastsave}\r\n"));
                out.push_str(&format!("rdb_changes_since_last_save:{chg}\r\n"));
                out.push_str(&format!(
                    "rdb_bgsave_in_progress:{}\r\n",
                    crate::stats::BGSAVE_IN_PROGRESS.load(Ordering::Acquire) as u8
                ));
                // Saves completed since startup. Unlike `rdb_last_save_time`,
                // it changes for every save, however close together.
                out.push_str(&format!(
                    "rdb_saves:{}\r\n",
                    crate::stats::RDB_SAVES.load(Ordering::Relaxed)
                ));
                out.push_str(&format!(
                    "rdb_last_bgsave_time_sec:{}\r\n",
                    crate::stats::LAST_BGSAVE_SECS.load(Ordering::Relaxed)
                ));
                out.push_str(&format!(
                    "rdb_last_bgsave_status:{}\r\n",
                    if crate::stats::LAST_BGSAVE_OK.load(Ordering::Acquire) {
                        "ok"
                    } else {
                        "err"
                    }
                ));
                out.push_str("rdb_last_cow_size:0\r\n");
                out.push_str(&format!(
                    "backup_in_progress:{}\r\n",
                    crate::commands::backup::in_progress() as u8
                ));

                // ── AOF ──────────────────────────────────────────────────────
                // These are the only external view of durability. A control
                // plane uses them to decide when to trigger BGREWRITEAOF, to
                // alert on unbounded growth, and to confirm a promoted replica
                // actually has something on disk — none of which was possible
                // while this block reported a hardcoded -1 and no size at all.
                let rewriting = crate::aof::rewrite_in_progress();
                out.push_str(&format!(
                    "aof_rewrite_in_progress:{}\r\n",
                    if rewriting { 1 } else { 0 }
                ));
                out.push_str("aof_rewrite_scheduled:0\r\n");
                match crate::aof::last_rewrite_duration() {
                    // Redis reports whole seconds, so keep that field
                    // wire-compatible for tools that already parse it.
                    Some(d) => {
                        out.push_str(&format!("aof_last_rewrite_time_sec:{}\r\n", d.as_secs()))
                    }
                    None => out.push_str("aof_last_rewrite_time_sec:-1\r\n"),
                }
                // ...and report milliseconds alongside it, because a rewrite
                // here is normally sub-second (16ms at 1M keys), which the
                // seconds field renders as an indistinguishable `0`.
                match crate::aof::last_rewrite_duration() {
                    Some(d) => {
                        out.push_str(&format!("aof_last_rewrite_time_ms:{}\r\n", d.as_millis()))
                    }
                    None => out.push_str("aof_last_rewrite_time_ms:-1\r\n"),
                }
                out.push_str(&format!(
                    "aof_last_bgrewrite_status:{}\r\n",
                    if crate::aof::last_rewrite_ok() {
                        "ok"
                    } else {
                        "err"
                    }
                ));
                // Only meaningful when AOF is on: with it off nothing is
                // written, and a stale size from a previous run would read as
                // durability that is not actually happening.
                if ctx.aof_enabled {
                    out.push_str(&format!(
                        "aof_current_size:{}\r\n",
                        crate::aof::current_size()
                    ));
                    out.push_str(&format!("aof_base_size:{}\r\n", crate::aof::base_size()));
                }
                // Non-zero means the writer could not keep a record and the AOF
                // has a hole — it can no longer reconstruct this dataset. This
                // is the single field that answers "is my durability intact?".
                out.push_str(&format!(
                    "aof_lost_records:{}\r\n",
                    crate::aof::AOF_LOST_RECORDS.load(Ordering::Relaxed)
                ));
                out.push_str(&format!(
                    "aof_last_write_status:{}\r\n",
                    if crate::aof::AOF_LOST_RECORDS.load(Ordering::Relaxed) == 0 {
                        "ok"
                    } else {
                        "err"
                    }
                ));
                // Replies to writes sent after 5 s without their records in
                // the file, because the disk fell that far behind: writes a
                // process killed then could lose. 0 on a disk that keeps up.
                out.push_str(&format!(
                    "aof_acks_before_write:{}\r\n",
                    crate::aof::AOF_ACKS_BEFORE_WRITE.load(Ordering::Relaxed)
                ));
                // Logs kept aside (`superseded.*` in `dir`): never replayed and
                // never removed by the server. Each may hold writes that are
                // in no dump; an operator removes them once they are not
                // needed, and these say whether any are left.
                let (kept, kept_bytes) = crate::aof::kept_logs_size();
                out.push_str(&format!(
                    "aof_superseded_files:{kept}\r\naof_superseded_bytes:{kept_bytes}\r\n"
                ));
                out.push_str("aof_last_cow_size:0\r\n");
                out.push_str("module_fork_in_progress:0\r\n");
                out.push_str("module_fork_last_cow_size:0\r\n");
                out.push_str("\r\n");
            }

            Some(resp_bulk_str(&out))
        }

        "DBSIZE" => {
            let db = &ctx.store[*ctx.selected_db];
            let count: usize = db.iter().map(|shard| read_lock!(shard).len()).sum();
            Some(resp_integer(count as i64))
        }

        "CONFIG" => {
            if cmd.len() < 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'config' command",
                ));
            }
            let subcmd = String::from_utf8_lossy(cmd[1]).to_ascii_uppercase();

            match subcmd.as_str() {
                "GET" => {
                    // CONFIG GET parameter [parameter ...]
                    //
                    // `CONFIG GET` with no parameter used to index cmd[2]
                    // unguarded and panic the connection task, dropping the
                    // client with no reply at all. Redis's arity for
                    // `config|get` is -3.
                    if cmd.len() < 3 {
                        return Some(resp_error(
                            "ERR wrong number of arguments for 'config|get' command",
                        ));
                    }
                    // Each parameter is a name or a glob; a setting that
                    // several of them match is given once. Alternating
                    // names and values, like real Redis.
                    let patterns: Vec<String> = cmd[2..]
                        .iter()
                        .map(|raw| String::from_utf8_lossy(raw).to_ascii_lowercase())
                        .collect();
                    let mut pairs: Vec<Vec<u8>> = Vec::new();
                    for (name, value) in config_rows(ctx) {
                        if patterns.iter().any(|pattern| {
                            *pattern == name || crate::commands::helpers::glob_match(pattern, &name)
                        }) {
                            pairs.push(resp_bulk_str(&name));
                            pairs.push(resp_bulk_str(&value));
                        }
                    }
                    // Redis's debugging settings: not listed by a pattern,
                    // and answered when asked for by name.
                    for (hidden, default) in crate::redis_settings::HIDDEN_SETTINGS {
                        if patterns.iter().any(|pattern| pattern == hidden) {
                            pairs.push(resp_bulk_str(hidden));
                            pairs.push(resp_bulk_str(
                                REMEMBERED_CONFIG
                                    .lock()
                                    .get(*hidden)
                                    .map_or(*default, |value| value.as_str()),
                            ));
                        }
                    }

                    // RESP3 replies with a map (`%`) rather than a flat array of
                    // alternating key/value bulk strings, so clients hand back a
                    // dict instead of a list. Same payload, different header.
                    Some(resp_map_for(
                        crate::commands::handler::effective_resp_for_reply(ctx),
                        pairs,
                    ))
                }
                "SET" => {
                    // CONFIG SET parameter value [parameter value ...]
                    if cmd.len() < 4 {
                        return Some(resp_error(
                            "ERR wrong number of arguments for 'config|set' command",
                        ));
                    }
                    // A name with no value to pair with it is Redis's syntax
                    // error, not an arity one: `CONFIG SET replicaof <host>
                    // <port>` gets the same reply as from Redis 8.10.
                    if !cmd.len().is_multiple_of(2) {
                        return Some(resp_error("ERR syntax error"));
                    }
                    let mut settings: Vec<(String, String)> = cmd[2..]
                        .chunks(2)
                        .map(|pair| {
                            (
                                String::from_utf8_lossy(pair[0]).to_ascii_lowercase(),
                                String::from_utf8_lossy(pair[1]).into_owned(),
                            )
                        })
                        .collect();
                    // The ports last: moving one is what can fail after the
                    // values are found good (the port is taken), so they
                    // are moved only once the others are set. A port closed
                    // (0) goes after one opened, so that `tls-port 6380
                    // port 0` and `port 6379 tls-port 0` always leave one
                    // to be reached on.
                    settings.sort_by_key(|(name, value)| match name.as_str() {
                        "port" | "tls-port" => 1 + u8::from(value.parse::<u16>() == Ok(0)),
                        _ => 0,
                    });
                    if let Some((_, (twice, _))) =
                        settings.iter().enumerate().find(|(i, (name, _))| {
                            settings[..*i]
                                .iter()
                                .any(|(seen, _)| config_name(seen) == config_name(name))
                        })
                    {
                        return Some(resp_error(&format!(
                            "ERR CONFIG SET failed (possibly related to argument '{twice}') -                              duplicate parameter"
                        )));
                    }
                    // TLS's files are loaded together, first: a
                    // certificate and its key are only good as a pair.
                    #[cfg(feature = "tls")]
                    let (tls, settings): (Vec<_>, Vec<_>) =
                        settings.into_iter().partition(|(name, _)| {
                            crate::tls::acceptor::SETTINGS.contains(&name.as_str())
                        });
                    #[cfg(feature = "tls")]
                    let mut tls_before = if tls.is_empty() {
                        None
                    } else {
                        match config_set_tls(ctx, &tls) {
                            Ok(before) => Some(before),
                            Err(reply) => return Some(reply),
                        }
                    };
                    // Several at once: all of them, or none. Each is set as
                    // it would be alone, in order; if one is refused, the
                    // ones before it are put back to what they were.
                    let before = if settings.len() > 1 {
                        config_rows(ctx)
                    } else {
                        Vec::new()
                    };
                    for (i, (param, value)) in settings.iter().enumerate() {
                        let mut reply = config_set_one(ctx, param, value);
                        // Redis's own reply to a setting it does not know
                        // is not wrapped either.
                        if reply.starts_with(b"-")
                            && !reply.starts_with(b"-ERR CONFIG SET failed")
                            && !reply.starts_with(b"-ERR Unknown option")
                        {
                            let why = String::from_utf8_lossy(&reply[1..]);
                            let why = why.trim_end().trim_start_matches("ERR ");
                            reply = resp_error(&format!(
                                "ERR CONFIG SET failed (possibly related to argument '{param}') - \
                                 {why}"
                            ));
                        }
                        if reply.starts_with(b"-") {
                            #[cfg(feature = "tls")]
                            if let Some(before) = tls_before.take() {
                                config_set_tls_undo(ctx, before);
                            }
                            for (param, _) in &settings[..i] {
                                match before.iter().find(|(known, _)| known == param) {
                                    Some((_, old)) => {
                                        config_set_one(ctx, param, old);
                                    }
                                    None => {
                                        REMEMBERED_CONFIG.lock().remove(param);
                                    }
                                }
                            }
                            return Some(reply);
                        }
                    }
                    Some(resp_simple("OK"))
                }
                "RESETSTAT" => {
                    crate::cmdstats::reset();
                    crate::stats::KEYSPACE_HITS.reset();
                    crate::stats::KEYSPACE_MISSES.reset();
                    crate::stats::TOTAL_COMMANDS_PROCESSED.reset();
                    crate::stats::TOTAL_READS_PROCESSED.reset();
                    crate::stats::TOTAL_WRITES_PROCESSED.reset();
                    crate::stats::TOTAL_CONNECTIONS_RECEIVED.store(0, Ordering::Relaxed);
                    crate::stats::TOTAL_ERROR_REPLIES.store(0, Ordering::Relaxed);
                    crate::stats::TOTAL_NET_INPUT_BYTES.store(0, Ordering::Relaxed);
                    crate::stats::TOTAL_NET_OUTPUT_BYTES.store(0, Ordering::Relaxed);
                    crate::stats::EXPIRED_KEYS.store(0, Ordering::Relaxed);
                    crate::stats::EXPIRED_SUBKEYS.store(0, Ordering::Relaxed);
                    crate::stats::EXPIRED_KEYS_ACTIVE.store(0, Ordering::Relaxed);
                    crate::lazyfree::reset_freed();
                    // `active_defrag_hits`, `_misses` and `_panics`, as Redis
                    // resets its `stat_active_defrag_*` (#370).
                    crate::compaction::reset_stats();
                    crate::stats::REJECTED_CONNECTIONS.store(0, Ordering::Relaxed);
                    crate::stats::OUTPUT_BUFFER_LIMIT_DISCONNECTIONS.store(0, Ordering::Relaxed);
                    // Not `rdb_changes_since_last_save`: that is not a
                    // statistic, and Redis keeps it (#566).
                    crate::stats::INSTANTANEOUS_OPS.store(0, Ordering::Relaxed);
                    crate::stats::LAST_SAMPLE_COMMANDS.store(0, Ordering::Relaxed);
                    crate::stats::reset_error_reply_stats();
                    ctx.slowlog.reset_totals();
                    Some(resp_simple("OK"))
                }
                "REWRITE" => {
                    use crate::config::RewriteError;
                    match ctx.config.with_values(&config_rows(ctx)).rewrite_to_disk() {
                        Ok(()) => Some(resp_simple("OK")),
                        Err(RewriteError::NoConfigFile) => Some(resp_error(
                            "ERR The server is running without a config file",
                        )),
                        Err(RewriteError::DefaultConfigFile(path)) => Some(resp_error(&format!(
                            "ERR The server read '{path}' only because it is the default \
                             config file, and will not rewrite it. Start the server with \
                             '--configfile {path}' to allow CONFIG REWRITE"
                        ))),
                        Err(RewriteError::Io(e)) => {
                            Some(resp_error(&format!("ERR Rewriting config file: {e}")))
                        }
                    }
                }
                "HELP" => Some(help(
                    "CONFIG",
                    &[
                        (
                            "GET <pattern>",
                            "Return parameters matching the glob-like <pattern> and their values.",
                        ),
                        (
                            "SET <directive> <value>",
                            "Set the configuration <directive> to <value>.",
                        ),
                        (
                            "RESETSTAT",
                            "Reset statistics reported by the INFO command.",
                        ),
                        ("REWRITE", "Rewrite the configuration file."),
                    ],
                )),
                _ => Some(resp_error(&format!(
                    "ERR unknown subcommand '{}'. Try CONFIG HELP.",
                    String::from_utf8_lossy(cmd[1])
                ))),
            }
        }

        "COMMAND" => {
            // redis-cli sends COMMAND DOCS or COMMAND COUNT on connect
            if cmd.len() >= 2 {
                let sub = String::from_utf8_lossy(cmd[1]).to_ascii_uppercase();
                match sub.as_str() {
                    "COUNT" => {
                        return Some(resp_integer(ALL_COMMANDS.len() as i64));
                    }
                    "DOCS" => {
                        // redis-cli sends COMMAND DOCS on connect and uses
                        // the summaries for HELP and the arguments for the
                        // typing hint. An empty array used to be enough for
                        // the client to start; the fields it reads are
                        // filled in now.
                        let resp = crate::commands::handler::effective_resp_for_reply(ctx);
                        return Some(super::command_docs::reply(resp, &cmd[2..], ALL_COMMANDS));
                    }
                    "INFO" => {
                        // COMMAND INFO [name ...]: nil for a name that is
                        // not a command.
                        let table = command_table();
                        if cmd.len() == 2 {
                            return Some(resp_array(table.into_values().collect()));
                        }
                        let resp = crate::commands::handler::effective_resp_for_reply(ctx);
                        let entries = cmd[2..]
                            .iter()
                            .map(|name| {
                                let name = String::from_utf8_lossy(name).to_ascii_uppercase();
                                table
                                    .get(name.as_str())
                                    .cloned()
                                    .unwrap_or_else(|| resp_null_for(resp))
                            })
                            .collect();
                        return Some(resp_array(entries));
                    }
                    "LIST" => {
                        // COMMAND LIST [FILTERBY MODULE module | ACLCAT category | PATTERN pattern]
                        //
                        // The names of the commands, in lower case, and of
                        // their subcommands as `container|subcommand`.
                        let names = || -> Vec<String> {
                            ALL_COMMANDS
                                .iter()
                                .copied()
                                .chain(crate::commands::arity::subcommands())
                                .map(str::to_ascii_lowercase)
                                .collect()
                        };
                        let listed: Vec<String> = match cmd.len() {
                            2 => names(),
                            5 if cmd[2].eq_ignore_ascii_case(b"FILTERBY") => {
                                let by = String::from_utf8_lossy(cmd[4]).to_ascii_lowercase();
                                if cmd[3].eq_ignore_ascii_case(b"MODULE") {
                                    names()
                                        .into_iter()
                                        .filter(|name| in_module(&by, name))
                                        .collect()
                                } else if cmd[3].eq_ignore_ascii_case(b"PATTERN") {
                                    names()
                                        .into_iter()
                                        .filter(|name| {
                                            crate::commands::helpers::glob_match(&by, name)
                                        })
                                        .collect()
                                } else if cmd[3].eq_ignore_ascii_case(b"ACLCAT") {
                                    // A command in the category, and — it
                                    // being theirs too — its subcommands.
                                    let members: Vec<String> = acl_cat_commands(&by)
                                        .unwrap_or_default()
                                        .iter()
                                        .map(|name| name.to_ascii_lowercase())
                                        .collect();
                                    names()
                                        .into_iter()
                                        .filter(|name| {
                                            members.contains(name)
                                                || name.split_once('|').is_some_and(
                                                    |(container, _)| {
                                                        members
                                                            .iter()
                                                            .any(|member| member == container)
                                                    },
                                                )
                                        })
                                        .collect()
                                } else {
                                    return Some(resp_error("ERR syntax error"));
                                }
                            }
                            _ => return Some(resp_error("ERR syntax error")),
                        };
                        return Some(resp_array(
                            listed.iter().map(|name| resp_bulk_str(name)).collect(),
                        ));
                    }

                    "HELP" => {
                        return Some(help(
                            "COMMAND",
                            &[
                                (
                                    "(no subcommand)",
                                    "Return details about all Redis commands.",
                                ),
                                (
                                    "COUNT",
                                    "Return the total number of commands in this Redis server.",
                                ),
                                (
                                    "LIST",
                                    "Return a list of all commands in this Redis server.",
                                ),
                                (
                                    "INFO [<command-name> ...]",
                                    "Return details about multiple Redis commands. If called without argument, return info about all commands.",
                                ),
                                (
                                    "DOCS [<command-name> ...]",
                                    "Return documentation details about multiple Redis commands. If called without argument, return documentation details about all commands.",
                                ),
                                (
                                    "GETKEYS <full-command>",
                                    "Return the keys from a full Redis command.",
                                ),
                                (
                                    "GETKEYSANDFLAGS <full-command>",
                                    "Return the keys and the access flags from a full Redis command.",
                                ),
                            ],
                        ));
                    }
                    "GETKEYS" | "GETKEYSANDFLAGS" => {
                        // COMMAND GETKEYS command [arg ...]
                        // Returns the key positions in the given command invocation.
                        // GETKEYSANDFLAGS additionally returns read/write flags per key.
                        let with_flags = sub == "GETKEYSANDFLAGS";
                        if cmd.len() < 3 {
                            return Some(resp_error("ERR Invalid arguments specified"));
                        }
                        // We pass the sub-command args starting from cmd[2] (the command name)
                        let sub_args = &cmd[2..];
                        return match extract_keys(sub_args, with_flags) {
                            Ok(result) => Some(resp_array(result)),
                            Err(e) => Some(resp_error(&e)),
                        };
                    }
                    // As the other containers answer, and Redis; it was
                    // nil (#134).
                    _ => {
                        return Some(resp_error(&format!(
                            "ERR unknown subcommand '{}'. Try COMMAND HELP.",
                            String::from_utf8_lossy(cmd[1])
                        )));
                    }
                }
            }
            Some(resp_array(command_table().into_values().collect()))
        }

        "REPLICAOF" | "SLAVEOF" => {
            if cmd.len() != 3 {
                return Some(resp_error("ERR syntax: REPLICAOF host port"));
            }

            let host = match std::str::from_utf8(cmd[1]) {
                Ok(h) => h,
                Err(_) => return Some(resp_error("ERR invalid host")),
            };

            // A cluster node's replication topology belongs to the cluster:
            // changing it behind the cluster's back leaves nodes.conf and
            // every peer's view describing a master this node no longer
            // follows. Redis refuses for the same reason.
            if ctx.cluster.enabled {
                return Some(resp_error(
                    "ERR REPLICAOF not allowed in cluster mode — use CLUSTER REPLICATE",
                ));
            }

            // REPLICAOF NO ONE — promote this instance to master
            if host.eq_ignore_ascii_case("no") {
                if !std::str::from_utf8(cmd[2]).is_ok_and(|s| s.eq_ignore_ascii_case("one")) {
                    return Some(resp_error("ERR syntax: REPLICAOF NO ONE"));
                }
                ctx.repl_state.become_master();
                log_info!("Replication: Promoted to master via REPLICAOF NO ONE");
                return Some(resp_simple("OK"));
            }

            let port: u16 = match std::str::from_utf8(cmd[2])
                .ok()
                .and_then(|s| s.parse().ok())
            {
                Some(p) => p,
                None => return Some(resp_error("ERR invalid port")),
            };

            // Already following exactly this master — no-op, matching real Redis
            // ("REPLICAOF would result into synchronization with the master we
            // are already connected with. No operation performed."). Operators
            // re-issue REPLICAOF on every reconcile pass; without this, each pass
            // would supersede the running task and force a fresh full resync.
            if ctx.repl_state.is_already_following(host, port) {
                log_info!(
                    "Replication: REPLICAOF {host}:{port} — already following this master, \
                     no operation performed"
                );
                return Some(resp_simple("OK"));
            }

            // Synchronous role change: on return this node is a read-only
            // replica of `host:port`, whether or not that master answers.
            crate::replication::replica_client::ReplicaStarter::from_ctx(ctx)
                .start(host.to_string(), port);

            Some(resp_simple("OK"))
        }

        "FLUSHALL" => {
            let lazy = match flush_is_async(cmd) {
                Ok(lazy) => lazy,
                Err(reply) => return Some(reply),
            };
            touch_watched_keys(ctx, None);
            let flushed = if lazy {
                let mut values = Vec::with_capacity(ctx.store.len());
                let mut keys = 0;
                for db in ctx.store.iter() {
                    let (taken, flushed) = crate::store::take_db(db);
                    values.push(taken);
                    keys += flushed.keys;
                }
                crate::lazyfree::free_later(values, keys as u64);
                crate::store::Flushed::default()
            } else {
                let flushed = crate::store::clear_store(ctx.store);
                count_flushed_in_place(&flushed);
                flushed
            };
            let indexes = crate::vector::drop_all_indexes(ctx.index_store);
            crate::vector::drop_all_dicts(ctx.dict_store);
            crate::store::clear_ts_label_index(ctx.ts_index);
            // Indexes hold HNSW graphs that are not Redis keys. Dropping them
            // is what actually returns vector-benchmark RSS; KV entry count
            // alone would miss that.
            if flushed.should_trim() || indexes > 0 {
                crate::commands::memory::purge_allocator();
            }
            Some(resp_simple("OK"))
        }

        "ROLE" => {
            let role = ctx.repl_state.role();
            let offset = ctx
                .repl_state
                .repl_offset
                .load(std::sync::atomic::Ordering::Relaxed);

            if role == "master" {
                // master: ["master", repl_offset, [[ip, port, offset], ...]]
                let replicas = replica_rows(ctx.repl_state, offset)
                    .into_iter()
                    .map(|(ip, port, slave_off, _lag, _state, _queued)| {
                        resp_array(vec![
                            resp_bulk_str(&ip),
                            resp_bulk_str(&port.to_string()),
                            resp_bulk_str(&slave_off.to_string()),
                        ])
                    })
                    .collect();
                Some(resp_array(vec![
                    resp_bulk_str("master"),
                    resp_integer(offset as i64),
                    resp_array(replicas),
                ]))
            } else {
                // slave: ["slave", master_host, master_port, link_status, replica_offset]
                // Live target first — the boot-time `--replicaof` is stale the
                // moment a REPLICAOF re-points this node.
                let (master_host, master_port) = ctx
                    .repl_state
                    .master_addr()
                    .or_else(|| ctx.config.replicaof_addr())
                    .unwrap_or_else(|| ("".into(), 0));
                // Report the actual link state — connect / connecting / sync /
                // connected. This used to be hardcoded "connected", so a
                // replica whose master was unreachable still looked healthy to
                // anything driving failover off ROLE.
                let link_status = ctx.repl_state.link_state().as_str();
                Some(resp_array(vec![
                    resp_bulk_str("slave"),
                    resp_bulk_str(&master_host),
                    resp_integer(master_port as i64),
                    resp_bulk_str(link_status),
                    resp_integer(offset as i64),
                ]))
            }
        }

        "REPLCONF" => {
            // Replica sends: REPLCONF listening-port <port>
            //                REPLCONF capa psync2
            //                REPLCONF ACK <offset>  (periodic heartbeat)
            // We accept all subcommands and respond OK. ACKs are read off the
            // replica link itself once the connection has been handed over.
            let mut i = 1;
            while i + 1 < cmd.len() {
                if cmd[i].eq_ignore_ascii_case(b"listening-port")
                    && let Some(port) = std::str::from_utf8(cmd[i + 1])
                        .ok()
                        .and_then(|s| s.parse::<u16>().ok())
                {
                    ctx.client_registry.update(ctx.client_id, |info| {
                        info.replica_listening_port = port;
                    });
                }
                if cmd[i].eq_ignore_ascii_case(b"capa")
                    && cmd[i + 1].eq_ignore_ascii_case(b"dataset-digest")
                {
                    ctx.client_registry.update(ctx.client_id, |info| {
                        info.replica_takes_digest = true;
                        info.replica_vset_quant.get_or_insert(false);
                    });
                }
                if cmd[i].eq_ignore_ascii_case(b"capa")
                    && cmd[i + 1].eq_ignore_ascii_case(b"lost-data")
                {
                    ctx.client_registry.update(ctx.client_id, |info| {
                        info.replica_lost_data = 1;
                        info.replica_vset_quant.get_or_insert(false);
                    });
                }
                if cmd[i].eq_ignore_ascii_case(b"capa")
                    && cmd[i + 1].eq_ignore_ascii_case(b"vset-quant")
                {
                    ctx.client_registry.update(ctx.client_id, |info| {
                        info.replica_vset_quant = Some(true);
                    });
                }
                i += 2;
            }
            Some(resp_simple("OK"))
        }

        "PSYNC" => {
            // Replica sends: PSYNC <repl_id> <offset>
            // We always respond with FULLRESYNC since we have no backlog yet.
            // Then we need to send an RDB dump followed by the replication stream.
            //
            // NOTE: The actual RDB sending must happen at the server/transport level,
            // not here in the command handler, because it requires writing raw bytes
            // to the socket outside of normal RESP framing.
            // We return a special marker that server.rs detects and handles.
            // For now return FULLRESYNC — the RDB send is wired in the next step.
            let repl_id = &ctx.repl_state.repl_id();
            let offset = ctx
                .repl_state
                .repl_offset
                .load(std::sync::atomic::Ordering::Relaxed);
            Some(resp_simple(&format!("FULLRESYNC {repl_id} {offset}")))
        }

        "CLIENT" => {
            if cmd.len() < 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'client' command",
                ));
            }
            let sub = String::from_utf8_lossy(cmd[1]).to_ascii_uppercase();

            match sub.as_str() {
                "SETNAME" => {
                    if cmd.len() != 3 {
                        return Some(resp_error(
                            "ERR wrong number of arguments for 'client|setname' command",
                        ));
                    }
                    // It is printed in `CLIENT LIST`, one client to a line
                    // and one field to a word.
                    if !is_printable_word(cmd[2]) {
                        return Some(resp_error(
                            "ERR Client names cannot contain spaces, newlines or special characters.",
                        ));
                    }
                    let name = String::from_utf8_lossy(cmd[2]).into_owned();
                    ctx.client_registry.update(ctx.client_id, |info| {
                        info.name = name.clone();
                    });
                    Some(resp_simple("OK"))
                }

                "GETNAME" => {
                    let name = ctx
                        .client_registry
                        .get(ctx.client_id)
                        .map(|i| i.name)
                        .unwrap_or_default();
                    if name.is_empty() {
                        Some(resp_null_bulk())
                    } else {
                        Some(resp_bulk_str(&name))
                    }
                }

                "ID" => Some(resp_integer(ctx.client_id as i64)),

                "INFO" => {
                    let info = ctx
                        .client_registry
                        .get(ctx.client_id)
                        .map(|i| i.to_list_line())
                        .unwrap_or_default();
                    Some(resp_bulk_str(&info))
                }

                "LIST" => {
                    // CLIENT LIST [TYPE …] [ID id …] [USER …] [ADDR …] [LADDR …]
                    //             [SKIPME yes|no] [MAXAGE seconds]
                    let filter = match client_filter(ctx, &cmd[2..], true) {
                        Ok(filter) => filter,
                        Err(e) => return Some(e),
                    };
                    Some(resp_bulk_str(
                        &ctx.client_registry.list_lines(&filter, ctx.client_id),
                    ))
                }

                "KILL" => {
                    // CLIENT KILL addr:port
                    // CLIENT KILL [ID id] [TYPE …] [USER …] [ADDR …] [LADDR …]
                    //             [SKIPME yes|no] [MAXAGE seconds]
                    if cmd.len() < 3 {
                        return Some(resp_error(
                            "ERR wrong number of arguments for 'client|kill' command",
                        ));
                    }
                    // The old form names one client, and may name the one
                    // asking: it gets its `+OK`, and then its connection
                    // closes.
                    if cmd.len() == 3 {
                        let filter = crate::client_registry::ClientFilter {
                            addr: Some(String::from_utf8_lossy(cmd[2]).into_owned()),
                            ..Default::default()
                        };
                        let (killed, _) = ctx.client_registry.kill_matching(&filter, ctx.client_id);
                        return Some(if killed == 0 {
                            resp_error("ERR No such client")
                        } else {
                            resp_simple("OK")
                        });
                    }
                    let filter = match client_filter(ctx, &cmd[2..], false) {
                        Ok(filter) => filter,
                        Err(e) => return Some(e),
                    };
                    let (killed, _) = ctx.client_registry.kill_matching(&filter, ctx.client_id);
                    Some(resp_integer(killed as i64))
                }

                "NO-EVICT" => {
                    // CLIENT NO-EVICT ON|OFF
                    if cmd.len() != 3 {
                        return Some(resp_error(
                            "ERR wrong number of arguments for CLIENT NO-EVICT",
                        ));
                    }
                    let on = match on_or_off(cmd[2]) {
                        Some(on) => on,
                        None => return Some(resp_error("ERR syntax error")),
                    };
                    ctx.client_registry.update(ctx.client_id, |info| {
                        info.no_evict = on;
                    });
                    Some(resp_simple("OK"))
                }

                // Accepted: what a client has touched is not kept per client.
                "NO-TOUCH" => match cmd.get(2).and_then(|word| on_or_off(word)) {
                    Some(_) if cmd.len() == 3 => Some(resp_simple("OK")),
                    _ => Some(resp_error("ERR syntax error")),
                },

                "PAUSE" => {
                    // CLIENT PAUSE timeout [WRITE | ALL]
                    let ms: i64 = match cmd
                        .get(2)
                        .and_then(|b| std::str::from_utf8(b).ok())
                        .and_then(|s| s.parse().ok())
                    {
                        Some(ms) if ms >= 0 => ms,
                        Some(_) => return Some(resp_error("ERR timeout is negative")),
                        None => {
                            return Some(resp_error(
                                "ERR timeout is not an integer or out of range",
                            ));
                        }
                    };
                    let write_only = match cmd.get(3) {
                        None => false,
                        Some(mode) if cmd.len() == 4 && mode.eq_ignore_ascii_case(b"WRITE") => true,
                        Some(mode) if cmd.len() == 4 && mode.eq_ignore_ascii_case(b"ALL") => false,
                        Some(_) => return Some(resp_error("ERR syntax error")),
                    };
                    // Once the writes on their way have been applied or
                    // will be held: see `pause_clients` (#629).
                    match crate::pause::pause_clients(ms, write_only) {
                        Ok(()) => Some(resp_simple("OK")),
                        Err(why) => Some(resp_error(why)),
                    }
                }

                "UNPAUSE" => {
                    crate::pause::clear_pause();
                    Some(resp_simple("OK"))
                }

                "CACHING" => {
                    // CLIENT CACHING yes|no
                    // Sets a single-shot flag for the next read command.
                    // Only meaningful in OPTIN or OPTOUT mode — accepted in all modes
                    // for compatibility but only acts when the mode requires it.
                    if cmd.len() < 3 {
                        return Some(resp_error(
                            "ERR wrong number of arguments for 'client|caching' command",
                        ));
                    }
                    // `YES` is what a client in `OPTIN` mode says, and `NO`
                    // one in `OPTOUT` mode; there is nothing to say otherwise.
                    let (optin, optout) = ctx
                        .client_registry
                        .get(ctx.client_id)
                        .filter(|info| info.tracking)
                        .map(|info| (info.tracking_optin, info.tracking_optout))
                        .unwrap_or((false, false));
                    if !optin && !optout {
                        return Some(resp_error(
                            "ERR CLIENT CACHING can be called only when the client is in \
                             tracking mode with OPTIN or OPTOUT mode enabled",
                        ));
                    }
                    let flag = if cmd[2].eq_ignore_ascii_case(b"yes") {
                        if !optin {
                            return Some(resp_error(
                                "ERR CLIENT CACHING YES is only valid when tracking is \
                                 enabled in OPTIN mode.",
                            ));
                        }
                        true
                    } else if cmd[2].eq_ignore_ascii_case(b"no") {
                        if !optout {
                            return Some(resp_error(
                                "ERR CLIENT CACHING NO is only valid when tracking is \
                                 enabled in OPTOUT mode.",
                            ));
                        }
                        false
                    } else {
                        return Some(resp_error("ERR syntax error"));
                    };
                    ctx.client_registry.update(ctx.client_id, |info| {
                        info.tracking_caching_next = Some(flag);
                    });
                    Some(resp_simple("OK"))
                }

                "TRACKING" => {
                    // CLIENT TRACKING ON|OFF [REDIRECT client-id]
                    //                        [PREFIX prefix [...]]
                    //                        [BCAST] [OPTIN] [OPTOUT] [NOLOOP]
                    if cmd.len() < 3 {
                        return Some(resp_error(
                            "ERR wrong number of arguments for 'client|tracking' command",
                        ));
                    }
                    let on = match on_or_off(cmd[2]) {
                        Some(on) => on,
                        None => return Some(resp_error("ERR syntax error")),
                    };
                    let (mut optin, mut optout, mut noloop, mut bcast) =
                        (false, false, false, false);
                    let mut redirect: Option<u64> = None;
                    let mut prefixes: Vec<Vec<u8>> = Vec::new();
                    let mut i = 3usize;
                    while i < cmd.len() {
                        let word = cmd[i].to_ascii_uppercase();
                        match (word.as_slice(), cmd.get(i + 1)) {
                            (b"REDIRECT", Some(id)) => {
                                redirect = match std::str::from_utf8(id)
                                    .ok()
                                    .and_then(|s| s.parse::<u64>().ok())
                                {
                                    Some(id) => Some(id),
                                    None => {
                                        return Some(resp_error(
                                            "ERR value is not an integer or out of range",
                                        ));
                                    }
                                };
                                i += 1;
                            }
                            (b"PREFIX", Some(prefix)) => {
                                prefixes.push(prefix.to_vec());
                                i += 1;
                            }
                            (b"BCAST", _) => bcast = true,
                            (b"OPTIN", _) => optin = true,
                            (b"OPTOUT", _) => optout = true,
                            (b"NOLOOP", _) => noloop = true,
                            _ => return Some(resp_error("ERR syntax error")),
                        }
                        i += 1;
                    }

                    if !on {
                        let mut was_tracking = false;
                        ctx.client_registry.update(ctx.client_id, |info| {
                            was_tracking = info.tracking;
                            info.tracking = false;
                            info.tracking_bcast = false;
                            info.tracking_optin = false;
                            info.tracking_optout = false;
                            info.tracking_noloop = false;
                            info.tracking_caching_next = None;
                            info.tracking_prefixes.clear();
                            info.tracking_redirect = None;
                            info.tracking_redirect_broken = false;
                        });
                        if was_tracking {
                            // Purge all key registrations for this client before
                            // decrementing the counter — otherwise keys read while
                            // tracking was on will still trigger invalidations.
                            ctx.tracking_table.unregister_client(ctx.client_id);
                            ctx.tracking_table.disable_tracking();
                        }
                        return Some(resp_simple("OK"));
                    }

                    // What is asked for has to make sense, with itself and
                    // with what the client has already turned on.
                    let now = ctx.client_registry.get(ctx.client_id);
                    let (was_tracking, was_bcast, was_optin, was_optout, had_prefixes) = now
                        .map(|info| {
                            (
                                info.tracking,
                                info.tracking_bcast,
                                info.tracking_optin,
                                info.tracking_optout,
                                info.tracking_prefixes,
                            )
                        })
                        .unwrap_or_default();
                    if let Some(redirect) = redirect {
                        if redirect == ctx.client_id {
                            return Some(resp_error(
                                "ERR A client can't redirect its own tracking invalidation \
                                 messages to itself",
                            ));
                        }
                        if ctx.client_registry.get(redirect).is_none() {
                            return Some(resp_error(
                                "ERR The client ID you want redirect to does not exist",
                            ));
                        }
                    }
                    if !bcast && !prefixes.is_empty() {
                        return Some(resp_error(
                            "ERR PREFIX option requires BCAST mode to be enabled",
                        ));
                    }
                    if was_tracking && was_bcast != bcast {
                        return Some(resp_error(
                            "ERR You can't switch BCAST mode on/off before disabling tracking \
                             for this client, and then re-enabling it with a different mode.",
                        ));
                    }
                    if bcast && (optin || optout) {
                        return Some(resp_error(
                            "ERR OPTIN and OPTOUT are not compatible with BCAST",
                        ));
                    }
                    if optin && optout {
                        return Some(resp_error(
                            "ERR You can't specify both OPTIN mode and OPTOUT mode",
                        ));
                    }
                    if (optin && was_optout) || (optout && was_optin) {
                        return Some(resp_error(
                            "ERR You can't switch OPTIN/OPTOUT mode before disabling tracking \
                             for this client, and then re-enabling it with a different mode.",
                        ));
                    }
                    if bcast {
                        // No prefix is the prefix of every key.
                        if prefixes.is_empty() && had_prefixes.is_empty() {
                            prefixes.push(Vec::new());
                        }
                        // One prefix of a client must not be the start of
                        // another: a key would be announced twice.
                        let overlap = |a: &[u8], b: &[u8]| a.starts_with(b) || b.starts_with(a);
                        for (n, prefix) in prefixes.iter().enumerate() {
                            if let Some(other) =
                                had_prefixes.iter().find(|other| overlap(prefix, other))
                            {
                                let (prefix, other) = (
                                    String::from_utf8_lossy(prefix),
                                    String::from_utf8_lossy(other),
                                );
                                return Some(resp_error(&format!(
                                    "ERR Prefix '{prefix}' overlaps with an existing prefix \
                                     '{other}'. Prefixes for a single client must not overlap."
                                )));
                            }
                            if let Some(other) = prefixes[n + 1..]
                                .iter()
                                .find(|other| overlap(prefix, other))
                            {
                                let (prefix, other) = (
                                    String::from_utf8_lossy(prefix),
                                    String::from_utf8_lossy(other),
                                );
                                return Some(resp_error(&format!(
                                    "ERR Prefix '{prefix}' overlaps with another provided \
                                     prefix '{other}'. Prefixes for a single client must not \
                                     overlap."
                                )));
                            }
                        }
                        // Given again, prefixes are added to the ones there.
                        let mut all = had_prefixes;
                        all.extend(prefixes);
                        prefixes = all;
                        ctx.tracking_table
                            .set_broadcast(ctx.client_id, prefixes.clone(), noloop);
                    }

                    ctx.client_registry.update(ctx.client_id, |info| {
                        info.tracking = true;
                        info.tracking_optin = optin;
                        info.tracking_optout = optout;
                        info.tracking_noloop = noloop;
                        info.tracking_bcast = bcast;
                        info.tracking_prefixes = prefixes;
                        info.tracking_redirect = redirect;
                        info.tracking_redirect_broken = false;
                    });
                    // Only increment if this client wasn't already tracking —
                    // re-sending TRACKING ON should not double-count.
                    if !was_tracking {
                        ctx.tracking_table.enable_tracking();
                    }
                    Some(resp_simple("OK"))
                }

                "TRACKINGINFO" => {
                    let info = ctx.client_registry.get(ctx.client_id);
                    let mut flags: Vec<&str> = Vec::new();
                    let mut redirect = -1;
                    let mut prefixes: Vec<Vec<u8>> = Vec::new();
                    match &info {
                        Some(info) if info.tracking => {
                            flags.push("on");
                            if info.tracking_bcast {
                                flags.push("bcast");
                            }
                            if info.tracking_optin {
                                flags.push("optin");
                                if info.tracking_caching_next == Some(true) {
                                    flags.push("caching-yes");
                                }
                            }
                            if info.tracking_optout {
                                flags.push("optout");
                                if info.tracking_caching_next == Some(false) {
                                    flags.push("caching-no");
                                }
                            }
                            if info.tracking_noloop {
                                flags.push("noloop");
                            }
                            if info.tracking_redirect_broken {
                                flags.push("broken_redirect");
                            }
                            // Zero: the messages come to this connection.
                            redirect = info.tracking_redirect.map_or(0, |id| id as i64);
                            prefixes = info.tracking_prefixes.clone();
                        }
                        _ => flags.push("off"),
                    }
                    Some(resp_map_for(
                        crate::commands::handler::effective_resp_for_reply(ctx),
                        vec![
                            resp_bulk_str("flags"),
                            resp_array(flags.iter().map(|flag| resp_bulk_str(flag)).collect()),
                            resp_bulk_str("redirect"),
                            resp_integer(redirect),
                            resp_bulk_str("prefixes"),
                            resp_array(prefixes.iter().map(|p| resp_bulk(p)).collect()),
                        ],
                    ))
                }

                // The connection loop does what this asks (see `ReplyGate`):
                // the reply here is the one `ON` gets.
                "REPLY" => match cmd.get(2) {
                    Some(mode)
                        if cmd.len() == 3
                            && [&b"ON"[..], b"OFF", b"SKIP"]
                                .iter()
                                .any(|word| mode.eq_ignore_ascii_case(word)) =>
                    {
                        Some(resp_simple("OK"))
                    }
                    _ => Some(resp_error("ERR syntax error")),
                },

                // CLIENT UNBLOCK client-id [TIMEOUT | ERROR]
                "UNBLOCK" => {
                    use crate::commands::blocking::Unblock;
                    let Some(id) = cmd
                        .get(2)
                        .and_then(|raw| std::str::from_utf8(raw).ok())
                        .and_then(|s| s.parse::<u64>().ok())
                    else {
                        return Some(resp_error("ERR value is not an integer or out of range"));
                    };
                    let how = match cmd.get(3) {
                        None => Unblock::Timeout,
                        Some(word) if word.eq_ignore_ascii_case(b"TIMEOUT") => Unblock::Timeout,
                        Some(word) if word.eq_ignore_ascii_case(b"ERROR") => Unblock::Error,
                        Some(_) => {
                            return Some(resp_error(
                                "ERR CLIENT UNBLOCK reason should be TIMEOUT or ERROR",
                            ));
                        }
                    };
                    Some(resp_integer(ctx.client_registry.unblock(id, how) as i64))
                }

                "SETINFO" => {
                    if cmd.len() < 4 {
                        return Some(resp_error(
                            "ERR wrong number of arguments for 'client|setinfo' command",
                        ));
                    }
                    let field = std::str::from_utf8(cmd[2])
                        .unwrap_or("")
                        .to_ascii_uppercase();
                    if matches!(field.as_str(), "LIB-NAME" | "LIB-VER")
                        && !is_printable_word(cmd[3])
                    {
                        return Some(resp_error(&format!(
                            "ERR {} cannot contain spaces, newlines or special characters.",
                            field.to_ascii_lowercase()
                        )));
                    }
                    let value = String::from_utf8_lossy(cmd[3]).into_owned();
                    match field.as_str() {
                        "LIB-NAME" => {
                            ctx.client_registry.update(ctx.client_id, |info| {
                                info.lib_name = value;
                            });
                            Some(resp_simple("OK"))
                        }
                        "LIB-VER" => {
                            ctx.client_registry.update(ctx.client_id, |info| {
                                info.lib_ver = value;
                            });
                            Some(resp_simple("OK"))
                        }
                        _ => Some(resp_error(&format!(
                            "ERR Unrecognized option '{}'",
                            String::from_utf8_lossy(cmd[2])
                        ))),
                    }
                }

                "GETREDIR" => {
                    // -1 with tracking off; 0 with the messages coming to
                    // this connection.
                    let redir = ctx
                        .client_registry
                        .get(ctx.client_id)
                        .filter(|info| info.tracking)
                        .map_or(-1, |info| info.tracking_redirect.map_or(0, |id| id as i64));
                    Some(resp_integer(redir))
                }

                "HELP" => Some(help(
                    "CLIENT",
                    &[
                        (
                            "CACHING (YES|NO)",
                            "Enable/disable tracking of the keys for next command in OPTIN/OPTOUT modes.",
                        ),
                        (
                            "GETREDIR",
                            "Return the client ID we are redirecting to when tracking is enabled.",
                        ),
                        ("GETNAME", "Return the name of the current connection."),
                        ("ID", "Return the ID of the current connection."),
                        (
                            "INFO",
                            "Return information about the current client connection.",
                        ),
                        ("KILL <ip:port>", "Kill connection made from <ip:port>."),
                        (
                            "KILL <option> <value> [<option> <value> [...]]",
                            "Kill connections. Options are: ID, TYPE, USER, ADDR, LADDR, SKIPME, MAXAGE.",
                        ),
                        (
                            "LIST [options ...]",
                            "Return information about client connections. Options: TYPE, ID.",
                        ),
                        (
                            "UNPAUSE",
                            "Stop the current client pause, resuming traffic.",
                        ),
                        (
                            "PAUSE <timeout> [WRITE|ALL]",
                            "Suspend all, or just write, clients for <timeout> milliseconds.",
                        ),
                        (
                            "REPLY (ON|OFF|SKIP)",
                            "Control the replies sent to the current connection.",
                        ),
                        (
                            "SETNAME <name>",
                            "Assign the name <name> to the current connection.",
                        ),
                        (
                            "SETINFO <option> <value>",
                            "Set client meta attr. Options are: LIB-NAME, LIB-VER.",
                        ),
                        (
                            "UNBLOCK <clientid> [TIMEOUT|ERROR]",
                            "Unblock the specified blocked client.",
                        ),
                        (
                            "TRACKING (ON|OFF) [REDIRECT <id>] [BCAST] [PREFIX <prefix> [...]] [OPTIN] [OPTOUT] [NOLOOP]",
                            "Control server assisted client side caching.",
                        ),
                        (
                            "TRACKINGINFO",
                            "Report tracking status for the current connection.",
                        ),
                        (
                            "NO-EVICT (ON|OFF)",
                            "Protect current client connection from eviction.",
                        ),
                        (
                            "NO-TOUCH (ON|OFF)",
                            "Will not touch LRU/LFU stats when this mode is on.",
                        ),
                    ],
                )),
                _ => Some(resp_error(&format!(
                    "ERR unknown subcommand '{}'. Try CLIENT HELP.",
                    String::from_utf8_lossy(cmd[1])
                ))),
            }
        }

        "HELLO" => {
            // HELLO is intercepted in the server loops before reaching here.
            // If it arrives here something went wrong — return a minimal response
            // rather than an error, so clients don't break.
            Some(resp_array(vec![
                resp_bulk_str("server"),
                resp_bulk_str("redis"),
                resp_bulk_str("version"),
                resp_bulk_str(REDIS_VERSION),
                resp_bulk_str("kividb_version"),
                resp_bulk_str(env!("CARGO_PKG_VERSION")),
                resp_bulk_str("proto"),
                resp_integer(2),
                resp_bulk_str("mode"),
                resp_bulk_str("standalone"),
                resp_bulk_str("role"),
                resp_bulk_str("master"),
                resp_bulk_str("modules"),
                module_list_reply(2),
            ]))
        }

        "TIME" => {
            // TIME — frozen inside Lua scripts (Redis execution unit).
            let (secs, micros) = crate::script_clock::time_command_wall_tuple();
            Some(resp_array(vec![
                resp_bulk_str(&secs.to_string()),
                resp_bulk_str(&micros.to_string()),
            ]))
        }

        "LOLWUT" => {
            // LOLWUT [VERSION version] [option ...]
            // Returns some art and the server version. KiviDB has one
            // picture, whatever the version; the arguments are checked as
            // Redis checks them (#561): VERSION takes an integer, and so do
            // the options of versions 5 (three) and 6 (two).
            let mut options = &cmd[1..];
            let mut asked = None;
            if options.len() >= 2 && options[0].eq_ignore_ascii_case(b"VERSION") {
                match std::str::from_utf8(options[1])
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                {
                    Some(v) => asked = Some(v),
                    None => {
                        return Some(resp_error("ERR value is not an integer or out of range"));
                    }
                }
                options = &options[2..];
            }
            let numeric = match asked {
                Some(5) => 3,
                Some(6) => 2,
                _ => 0,
            };
            if options.iter().take(numeric).any(|o| {
                std::str::from_utf8(o)
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                    .is_none()
            }) {
                return Some(resp_error("ERR value is not an integer or out of range"));
            }
            let version = env!("CARGO_PKG_VERSION");
            let art = format!(
                "

        ██╗  ██╗██╗         ██╗███████╗ ███████╗ 
        ██║ ██╔╝╚═╝         ╚═╝╚██╔══██╗╚██╔══██╗
        █████╔╝ ██╗██╗   ██╗██╗ ██║  ██║ ██████╔╝
        ██╔═██╗ ██║╚██╗ ██╔╝██║ ██║  ██║ ██╔══██╗
        ██║ ╚██╗██║ ╚████╔╝ ██║███████╔╝███████╔╝
        ╚═╝  ╚═╝╚═╝  ╚═══╝  ╚═╝╚══════╝  ╚══════╝ 

        KiviDB version {version}, bits 64

"
            );
            Some(resp_bulk_str(&art))
        }

        "BGREWRITEAOF" => {
            if !ctx.aof_enabled {
                return Some(resp_error("ERR AOF is not enabled"));
            }
            crate::aof::checkpoint_in_background(
                Arc::clone(ctx.store),
                Arc::clone(ctx.index_store),
            );
            Some(resp_simple("Background append only file rewriting started"))
        }

        "SHUTDOWN" => {
            // SHUTDOWN [NOSAVE | SAVE | NOW | FORCE | ABORT]
            //
            // We cannot write the snapshot here — execute_command is a plain fn
            // with no async executor.  Instead we set two global atomics and
            // return +OK.  The signal-watching thread in main.rs reads the flag
            // and drives the real shutdown sequence (snapshot + process::exit).
            //
            // ABORT clears the pending flag and returns OK immediately so the
            // caller knows the in-progress shutdown was cancelled.

            use crate::shutdown::{SHUTDOWN_MODE, SHUTDOWN_PENDING, mode};
            use std::sync::atomic::Ordering::Relaxed;

            let modifier = ctx
                .cmd
                .get(1)
                .and_then(|b| std::str::from_utf8(b).ok())
                .map(|s| s.to_ascii_uppercase());

            match modifier.as_deref() {
                // SHUTDOWN ABORT — cancel a pending shutdown.
                Some("ABORT") => {
                    SHUTDOWN_PENDING.store(false, Relaxed);
                    Some(resp_simple("OK"))
                }

                // SHUTDOWN NOSAVE — exit without writing snapshot.
                Some("NOSAVE") => {
                    SHUTDOWN_MODE.store(mode::NOSAVE, Relaxed);
                    SHUTDOWN_PENDING.store(true, Relaxed);
                    // Return OK before the process exits; the signal thread will
                    // call process::exit shortly after, so the client may or may
                    // not receive this response — that is Redis-compatible behaviour.
                    Some(resp_simple("OK"))
                }

                // SHUTDOWN NOW — skip AOF flush wait, write snapshot immediately.
                Some("NOW") => {
                    SHUTDOWN_MODE.store(mode::NOW, Relaxed);
                    SHUTDOWN_PENDING.store(true, Relaxed);
                    Some(resp_simple("OK"))
                }

                // SHUTDOWN FORCE — ignore replica/AOF checks.
                Some("FORCE") => {
                    SHUTDOWN_MODE.store(mode::FORCE, Relaxed);
                    SHUTDOWN_PENDING.store(true, Relaxed);
                    Some(resp_simple("OK"))
                }

                // SHUTDOWN / SHUTDOWN SAVE — default: write snapshot then exit.
                None | Some("SAVE") => {
                    SHUTDOWN_MODE.store(mode::SAVE, Relaxed);
                    SHUTDOWN_PENDING.store(true, Relaxed);
                    Some(resp_simple("OK"))
                }

                // Unknown modifier — match Redis's exact error.
                Some(other) => Some(resp_error(&format!(
                    "ERR SHUTDOWN requires NOSAVE, SAVE, NOW, FORCE, or ABORT, not '{other}'"
                ))),
            }
        }

        // ── Tier 3 rejections — Lua / Functions / Modules ───────────────────
        //
        // Without `--features lua`, SCRIPT/EVAL return a documented ERR here so clients
        // fail loudly. With `lua`, those commands fall through (`None`) to `commands::lua`.
        "EVAL" | "EVALSHA" | "EVAL_RO" | "EVALSHA_RO" => {
            #[cfg(feature = "lua")]
            {
                None
            }
            #[cfg(not(feature = "lua"))]
            {
                Some(resp_error(NO_LUA))
            }
        }

        "SCRIPT" => {
            #[cfg(feature = "lua")]
            {
                None
            }
            #[cfg(not(feature = "lua"))]
            {
                Some(resp_error(NO_LUA))
            }
        }

        "FCALL" | "FCALL_RO" => {
            #[cfg(feature = "lua")]
            {
                None
            }
            #[cfg(not(feature = "lua"))]
            {
                Some(resp_error(NO_LUA))
            }
        }

        "FUNCTION" => {
            #[cfg(feature = "lua")]
            {
                None
            }
            #[cfg(not(feature = "lua"))]
            {
                Some(function_without_lua(ctx))
            }
        }

        // The built-in module families; see `BUILT_IN_MODULES`.
        "MODULE"
            if cmd
                .get(1)
                .is_some_and(|sub| sub.eq_ignore_ascii_case(b"LIST")) =>
        {
            Some(module_list_reply(
                crate::commands::handler::effective_resp_for_reply(ctx),
            ))
        }
        "MODULE"
            if cmd
                .get(1)
                .is_some_and(|sub| sub.eq_ignore_ascii_case(b"HELP")) =>
        {
            Some(help(
                "MODULE",
                &[
                    ("LIST", "Return a list of loaded modules."),
                    (
                        "LOAD <path> [<arg> ...]",
                        "Load a module library from <path>, passing to it any optional arguments.",
                    ),
                    (
                        "LOADEX <path> [[CONFIG NAME VALUE] [CONFIG NAME VALUE]] [ARGS ...]",
                        "Load a module library from <path>, while passing it module configurations and optional arguments.",
                    ),
                    ("UNLOAD <name>", "Unload a module."),
                ],
            ))
        }
        "MODULE"
            if cmd.get(1).is_some_and(|sub| {
                [&b"LOAD"[..], b"LOADEX", b"UNLOAD"]
                    .iter()
                    .any(|known| sub.eq_ignore_ascii_case(known))
            }) =>
        {
            Some(resp_error(
                "ERR MODULE LOAD, LOADEX and UNLOAD are not supported: KiviDB does not \
                 load modules. Search, JSON, time series and vector \
                 sets are built in (MODULE LIST).",
            ))
        }
        "MODULE" => Some(resp_error(&format!(
            "ERR unknown subcommand '{}'. Try MODULE HELP.",
            String::from_utf8_lossy(cmd.get(1).copied().unwrap_or_default())
        ))),

        "FAILOVER" => Some(resp_error(
            "ERR FAILOVER is not supported by KiviDB. \
             This command is only meaningful in a Sentinel topology, \
             which KiviDB does not implement.",
        )),

        "MONITOR" => {
            // MONITOR is intercepted by server.rs before execute_command.
            // If we reach here it's inside a MULTI block — reject it.
            Some(resp_error(
                "ERR MONITOR is not allowed inside a MULTI/EXEC block",
            ))
        }

        _ => None, // ← tells the router this module doesn't handle it
    }
}

/// Every command name the server knows about — used by the replication
/// bucketing equivalence test to check both classifications agree.
#[cfg(test)]
pub(crate) fn all_commands_for_test() -> &'static [&'static str] {
    ALL_COMMANDS
}

pub fn acl_cat_commands_pub(category: &str) -> Vec<&'static str> {
    acl_cat_commands(category).unwrap_or_default()
}

pub fn acl_cat_commands_opt(category: &str) -> Option<Vec<&'static str>> {
    acl_cat_commands(category)
}

#[cfg(test)]
mod keysizes_tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn set(store: &crate::store::Store, key: &str, value: &str) {
        let _ = crate::store::get_shard(&store[0], key)
            .write()
            .set_str(key, value);
    }

    /// The kept count of `store` made `by` older.
    fn age(store: &crate::store::Store, by: Duration) {
        let mut last = KEYSIZES_LAST.lock();
        let kept = last.get_mut(&keysizes_key(store)).unwrap();
        kept.at = Instant::now().checked_sub(by).unwrap();
    }

    fn counting(store: &crate::store::Store) -> bool {
        KEYSIZES_LAST
            .lock()
            .get(&keysizes_key(store))
            .unwrap()
            .counting
    }

    /// Wait until no count of `store` is under way.
    fn settled(store: &crate::store::Store) {
        let until = Instant::now() + Duration::from_secs(10);
        while counting(store) {
            assert!(Instant::now() < until, "the count never ended");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    const ONE: &str = "db0_distrib_strings_sizes:1=1\r\n";

    /// A count is given again while fresh, counted aside once it is older
    /// than `KEYSIZES_REUSED_FOR`, and counted at once, waited for, once
    /// it is older than twice that. Each store has its own.
    #[test]
    fn a_count_is_given_again_counted_aside_or_waited_for_by_its_age() {
        let store = crate::store::new_store(1);
        let other = crate::store::new_store(1);
        set(&store, "k", "v");
        assert_eq!(keysizes_cached(&store, false, keysizes_text), ONE);
        assert_eq!(keysizes_cached(&other, false, keysizes_text), "");
        // Fresh: given again, though the keys have gone.
        let _ = crate::store::clear_db(&store[0]);
        assert_eq!(keysizes_cached(&store, false, keysizes_text), ONE);
        // Older: given again, and counted aside.
        age(&store, KEYSIZES_REUSED_FOR + Duration::from_secs(1));
        assert_eq!(keysizes_cached(&store, false, keysizes_text), ONE);
        settled(&store);
        assert_eq!(keysizes_cached(&store, false, keysizes_text), "");
        // Asked for by name: counted at once.
        set(&store, "k", "v");
        assert_eq!(keysizes_cached(&store, true, keysizes_text), ONE);
        // Much older, the keys gone since: waited for.
        let _ = crate::store::clear_db(&store[0]);
        age(&store, 2 * KEYSIZES_REUSED_FOR + Duration::from_secs(1));
        assert_eq!(keysizes_cached(&store, false, keysizes_text), "");
        assert_eq!(keysizes_cached(&other, false, keysizes_text), "");
    }

    fn panics(_: &crate::store::Store) -> String {
        panic!("a count that fails")
    }

    /// A count that panics on its thread lets the next one start.
    #[test]
    fn a_count_that_panics_does_not_stop_the_next() {
        let store = crate::store::new_store(1);
        set(&store, "k", "v");
        assert_eq!(keysizes_cached(&store, false, keysizes_text), ONE);
        age(&store, KEYSIZES_REUSED_FOR + Duration::from_secs(1));
        assert_eq!(keysizes_cached(&store, false, panics), ONE);
        settled(&store);
        // Still the old one, still older: counted aside again, and kept.
        set(&store, "j", "vv");
        assert_eq!(keysizes_cached(&store, false, keysizes_text), ONE);
        settled(&store);
        assert_eq!(
            keysizes_cached(&store, false, keysizes_text),
            "db0_distrib_strings_sizes:1=1,2=1\r\n"
        );
    }

    /// A count by name while one is under way aside leaves it under way.
    #[test]
    fn a_count_by_name_leaves_one_under_way_alone() {
        let store = crate::store::new_store(1);
        assert_eq!(keysizes_cached(&store, false, keysizes_text), "");
        KEYSIZES_LAST
            .lock()
            .get_mut(&keysizes_key(&store))
            .unwrap()
            .counting = true;
        set(&store, "k", "v");
        assert_eq!(keysizes_cached(&store, true, keysizes_text), ONE);
        assert!(counting(&store));
    }

    fn shard_of(keys: impl Iterator<Item = (String, &'static str)>) -> crate::store::Shard {
        let shard: crate::store::Shard = Arc::new(crate::store::ShardLock::default());
        {
            let mut map = shard.write();
            for (key, value) in keys {
                let _ = map.set_str(key, value);
            }
        }
        shard
    }

    /// Strings of `len` bytes counted.
    fn of_len(bins: &KeysizeBins, len: usize) -> u64 {
        bins[0][(usize::BITS - len.leading_zeros()) as usize]
    }

    /// The table grown between two parts: the shard is counted again, and
    /// the count is exact.
    #[test]
    fn the_walk_counts_a_resized_shard_again() {
        let shard = shard_of((0..5_000).map(|i| (format!("stay{i}"), "xxxxxxxxxx")));
        let before = shard.read().buckets();
        assert!(before > 2 * KEYSIZES_BUCKETS);
        let mut calls = 0;
        let bins = keysizes_of_shard(&shard, &mut || {
            calls += 1;
            if calls == 1 {
                let mut map = shard.write();
                for i in 0..20_000 {
                    let _ = map.set_str(format!("new{i}"), "x");
                }
            }
        });
        assert!(shard.read().buckets() > before);
        assert_eq!((of_len(&bins, 10), of_len(&bins, 1)), (5_000, 20_000));
    }

    /// Writes and deletes between the parts, with room in the table, so
    /// that it is neither resized nor rehashed: every key there all along
    /// is counted once.
    #[test]
    fn the_walk_counts_untouched_keys_once_under_writes() {
        let shard = shard_of(
            (0..5_000)
                .map(|i| (format!("stay{i}"), "xxxxxxxxxx"))
                .chain((0..500).map(|i| (format!("gone{i}"), "xxxxx"))),
        );
        let (buckets, room) = {
            let map = shard.read();
            (map.buckets(), map.capacity() - map.len())
        };
        let parts = buckets.div_ceil(KEYSIZES_BUCKETS);
        let per_part = (room / parts).min(100);
        assert!(per_part > 0);
        let mut call = 0;
        let bins = keysizes_of_shard(&shard, &mut || {
            let mut map = shard.write();
            for i in 0..per_part {
                let _ = map.set_str(format!("new{call}-{i}"), "x");
            }
            for i in 0..50 {
                drop(map.remove(&format!("gone{}", call * 50 + i)));
            }
            call += 1;
        });
        assert_eq!(shard.read().buckets(), buckets);
        assert_eq!(of_len(&bins, 10), 5_000);
        assert!(of_len(&bins, 1) <= (per_part * parts) as u64);
        assert!(of_len(&bins, 5) <= 500);
    }

    /// Heavy churn between the parts, many keys in and out, which can make
    /// the table rehash in place: the walk ends, and its count is the
    /// approximation documented, no more than twice the keys there all
    /// along.
    #[test]
    fn the_walk_ends_under_churn() {
        let shard = shard_of((0..5_000).map(|i| (format!("stay{i}"), "xxxxxxxxxx")));
        let mut call = 0;
        let bins = keysizes_of_shard(&shard, &mut || {
            let mut map = shard.write();
            for i in 0..2_000 {
                let _ = map.set_str(format!("churn{call}-{i}"), "x");
            }
            for i in 0..2_000 {
                drop(map.remove(&format!("churn{call}-{i}")));
            }
            call += 1;
        });
        assert!(of_len(&bins, 10) <= 10_000);
        assert_eq!(shard.read().len(), 5_000);
    }
}
