use crate::commands::ctx::Ctx;
use crate::loglevel::LOG_LEVEL;
use crate::read_lock;
use crate::resp::write::*;
use crate::store::ExpiryDb;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::SystemTime;

/// Average remaining TTL in milliseconds for keys with a future expiry (Redis `avg_ttl`).
fn avg_ttl_ms(expiry_db: &ExpiryDb) -> i64 {
    let now = SystemTime::now();
    let mut sum_ms: u128 = 0;
    let mut n = 0usize;
    for shard in expiry_db.iter() {
        let map = read_lock!(shard);
        for exp_at in map.values() {
            if let Ok(rem) = exp_at.duration_since(now) {
                sum_ms += rem.as_millis();
                n += 1;
            }
        }
    }
    if n == 0 {
        0
    } else {
        (sum_ms / n as u128) as i64
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
            ctx.config.repl_replica_max_backlog_bytes.to_string(),
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
        (
            "cluster-announce-ip",
            ctx.config.cluster_announce_ip.clone(),
        ),
        (
            "cluster-announce-hostname",
            ctx.cluster
                .nodes
                .read()
                .get(&ctx.cluster.myself())
                .map(|me| me.hostname.clone())
                .unwrap_or_else(|| ctx.config.cluster_announce_hostname.clone()),
        ),
        (
            "cluster-preferred-endpoint-type",
            if ctx.cluster.enabled {
                ctx.cluster.preferred_endpoint().name().to_string()
            } else {
                ctx.config.cluster_preferred_endpoint_type.clone()
            },
        ),
        (
            "cluster-announce-port",
            ctx.config.cluster_announce_port.to_string(),
        ),
        (
            "cluster-announce-bus-port",
            ctx.config.cluster_announce_bus_port.to_string(),
        ),
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
        ("tls-replication", yes_no(ctx.config.tls_replication)),
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
            yes_no(ctx.config.cluster_require_full_coverage),
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
            ctx.config.stream_idmp_duration.to_string(),
        ),
        ("backupdirname", ctx.config.backupdirname.clone()),
        (
            "backup-sealed-ttl",
            ctx.config.backup_sealed_ttl.to_string(),
        ),
        ("preload-file", ctx.config.preload_file.clone()),
        (
            "stream-idmp-maxsize",
            ctx.config.stream_idmp_maxsize.to_string(),
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
    ]
}

const NOT_MEMORY_OR_PERCENT: &str = "ERR argument must be a memory or percent value";

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
            "slowlog-max-len" => match value.parse::<usize>() {
                Ok(v) => {
                    ctx.slowlog.set_max_len(v);
                    Some(resp_simple("OK"))
                }
                _ => Some(resp_error("ERR value is not an integer or out of range")),
            },
            "maxmemory" => match memory_size(value) {
                Some(v) => {
                    crate::RUNTIME_MAXMEMORY.store(v, std::sync::atomic::Ordering::Relaxed);
                    // A limit below what is in use is enforced
                    // now, not at the next write.
                    if v != 0 {
                        let observed = crate::replication::write_is_observed(ctx);
                        let _evicting = crate::write_gate::enter();
                        crate::eviction::make_room(ctx, v, observed);
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
                    let was_on =
                        crate::aof::AOF_ENABLED.swap(true, std::sync::atomic::Ordering::Relaxed);
                    // Redis CONFIG SET appendonly yes rewrites before
                    // OK. Returning first left a window where the next
                    // write could miss both the dump and the delta —
                    // and on io_uring the connection still carried the
                    // boot-time aof_enabled=false, so that write was
                    // never logged at all.
                    if !was_on
                        && let Err(e) =
                            crate::aof::checkpoint(ctx.store, ctx.expiry_store, ctx.index_store)
                    {
                        crate::aof::AOF_ENABLED.store(false, std::sync::atomic::Ordering::Relaxed);
                        return Some(resp_error(&format!("ERR enabling AOF failed: {e}")));
                    }
                    Some(resp_simple("OK"))
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
                #[cfg(not(feature = "lua"))]
                {
                    Some(resp_simple("OK"))
                }
            }
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
                let new_flags = crate::keyspace_events::parse_ks_flags(value);
                crate::keyspace_events::KS_FLAGS
                    .store(new_flags, std::sync::atomic::Ordering::Relaxed);
                Some(resp_simple("OK"))
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
            // Fixed at startup: changing them under a running
            // cluster would desynchronise this node from what its
            // peers and its own nodes.conf already say.
            "cluster-enabled"
            | "cluster-config-file"
            | "cluster-announce-ip"
            | "cluster-announce-port"
            | "cluster-announce-bus-port"
            | "cluster-bus-port-protected-mode"
            | "cluster-bus-secret"
            | "tls-cluster"
            | "tls-replication"
            | "cluster-node-timeout"
            | "cluster-require-full-coverage" => {
                // To what it already is, is no change.
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
            "port" => match value.parse::<u16>() {
                Ok(port) if port > 0 => match crate::listen::move_to(ctx.config.port, port) {
                    Ok(()) => Some(resp_simple("OK")),
                    Err(e) => Some(resp_error(&format!(
                        "ERR Unable to listen on this port: {e}"
                    ))),
                },
                // Redis takes 0, for no TCP at all, with a Unix socket to be
                // reached on; there is none here.
                _ => Some(resp_error(
                    "ERR argument must be between 1 and 65535 inclusive",
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
            // A setting of Redis's that means nothing here — how
            // it encodes small values, how it sizes its buffers.
            // It is accepted, so that a client or a tool that
            // sets it does not fail, and remembered, so that
            // `CONFIG GET` answers with what was set.
            _ => {
                REMEMBERED_CONFIG
                    .lock()
                    .insert(param.clone(), value.to_string());
                Some(resp_simple("OK"))
            }
        }
    })();
    let reply = reply.unwrap_or_else(|| resp_simple("OK"));
    if reply.starts_with(b"+") {
        let secret = matches!(
            param.as_str(),
            "requirepass" | "masterauth" | "tls-key-file-pass"
        );
        log_info!(
            "CONFIG SET {param} {}",
            if secret { "(hidden)" } else { value }
        );
    }
    reply
}

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
            ("lazyfree-lazy-user-del", "yes"),
            ("lazyfree-lazy-user-flush", "no"),
            ("lazyfree-lazy-expire", "yes"),
            ("lazyfree-lazy-server-del", "yes"),
            ("activedefrag", "no"),
            ("close-on-replica-write", "yes"),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect(),
    )
});

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
    let lua_time_limit = "5000".to_string();
    let in_effect: Vec<(&str, String)> = vec![
        ("bind", ctx.config.bind.clone()),
        (
            "port",
            crate::listen::current_port(ctx.config.port).to_string(),
        ),
        ("logfile", ctx.config.logfile.clone()),
        ("log-format", ctx.config.log_format.clone()),
        ("databases", ctx.config.databases.to_string()),
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
        ("appendonly", yes_no(crate::aof::AOF_ENABLED.load(Relaxed))),
        (
            "loglevel",
            crate::loglevel::log::level_str(LOG_LEVEL.load(Relaxed)).to_string(),
        ),
        ("hz", "10".to_string()),
        ("activerehashing", "yes".to_string()),
        ("lazyfree-lazy-eviction", "no".to_string()),
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
        ("timeout", "0".to_string()),
        ("tcp-keepalive", "300".to_string()),
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
    ];
    for (name, value) in in_effect {
        if !rows.iter().any(|(known, _)| known == name) {
            rows.push((name.to_string(), value));
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
    const HIDDEN: [&str; 2] = ["key-load-delay", "rdb-key-save-delay"];
    for (name, value) in REMEMBERED_CONFIG.lock().iter() {
        if !rows.iter().any(|(known, _)| known == name) && !HIDDEN.contains(&name.as_str()) {
            rows.push((name.clone(), value.clone()));
        }
    }
    rows
}

/// Before a flush of `db` (`None`: of every database): the watched keys
/// that are about to go are modified by it. One that was not there is not —
/// it is as absent after the flush as it was before.
fn touch_watched_keys(ctx: &Ctx, db: Option<usize>) {
    if !ctx.watch_registry.has_watchers() {
        return;
    }
    ctx.watch_registry.notify_existing(db, &|db, key| {
        if crate::commands::helpers::is_expired(ctx.expiry_store, db, key) {
            return false;
        }
        ctx.store.get(db).is_some_and(|shards| {
            crate::read_lock!(crate::store::get_shard(shards, key)).contains_key(key)
        })
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
fn is_printable_word(raw: &[u8]) -> bool {
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
/// background (`lazyfree`), after the reply.
fn flush_is_async(cmd: &[&[u8]]) -> bool {
    cmd.get(1)
        .is_some_and(|arg| arg.eq_ignore_ascii_case(b"ASYNC"))
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
) -> Vec<(String, u16, u64, u64, &'static str)> {
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
            ),
            None => (ip, port, 0, 0, "wait_bgsave"),
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
    "GEODELBYMEMBER",
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
pub(crate) fn is_admin_command(name: &str) -> bool {
    ADMIN_COMMANDS.contains(&name)
}

/// Whether there is a command called `name` (upper case).
pub(crate) fn is_command(name: &str) -> bool {
    static NAMES: std::sync::LazyLock<ahash::AHashSet<&'static str>> =
        std::sync::LazyLock::new(|| ALL_COMMANDS.iter().copied().collect());
    NAMES.contains(name) || crate::commands::arity::is_known(name)
}

const ADMIN_COMMANDS: &[&str] = &[
    "ACL",
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
    "CLUSTER|FAILOVER",
    "CLUSTER|FLUSHSLOTS",
    "CLUSTER|FORGET",
    "CLUSTER|LINKS",
    "CLUSTER|MEET",
    "CLUSTER|MIGRATION",
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
const FAST_COMMANDS: &[&str] = &[
    "APPEND",
    "ASKING",
    "AUTH",
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

/// The ACL categories, as `ACL CAT` lists them.
pub(crate) const ACL_CATEGORIES: &[&str] = &[
    "admin",
    "array",
    "bitmap",
    "blocking",
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
    for category in ACL_CATEGORIES {
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
            let arity = crate::commands::arity::of(name).map_or(arity, i64::from);
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
            if crate::replication::is_write_command(name) {
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
            "GEODELBYMEMBER",
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
            "WAIT",
            "WAITAOF",
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
        "connection" => vec!["AUTH", "CLIENT", "ECHO", "HELLO", "PING", "QUIT", "RESET"],
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
            "WAIT",
            "WAITAOF",
        ],
        // The module families, taken from the command table by prefix so a new
        // command joins its category by being registered.
        "json" => commands_with_prefix("JSON."),
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
        "write" => ALL_COMMANDS
            .iter()
            .copied()
            .filter(|c| crate::replication::is_write_command(c))
            .collect(),
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
const BUILT_IN_MODULES: [(&str, i64); 4] = [
    ("search", 81001),
    ("ReJSON", 81000),
    ("timeseries", 81001),
    ("vectorset", 1),
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
        // The vector set commands are the only ones that begin with `v`.
        "vectorset" => name.starts_with('v'),
        _ => false,
    }
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
        | "XCLAIM" | "XREADGROUP" | "PFADD" | "BITOP" | "BITFIELD" | "GEOADD"
        | "GEODELBYMEMBER" | "JSON.SET" | "JSON.DEL" | "JSON.MSET" | "JSON.NUMINCRBY"
        | "JSON.NUMMULTBY" | "JSON.STRAPPEND" | "JSON.ARRAPPEND" | "JSON.ARRINSERT"
        | "JSON.ARRPOP" | "JSON.ARRTRIM" | "JSON.CLEAR" | "JSON.TOGGLE" | "JSON.MERGE" | "VADD"
        | "VREM" | "VSETATTR" | "TS.ADD" | "TS.INCRBY" | "TS.DECRBY" | "TS.CREATE" | "TS.DEL"
        | "TS.ALTER" => {
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
            let result = crate::replication::save_dataset(
                ctx.store,
                ctx.expiry_store,
                ctx.index_store,
                ctx.repl_state,
            );
            match result {
                Ok(_) => {
                    crate::stats::reset_rdb_changes();
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as i64;

                    *ctx.lastsave.write() = now;

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
            let expiry_store_snap = Arc::clone(ctx.expiry_store);
            let index_store_snap = Arc::clone(ctx.index_store);
            let repl_state = Arc::clone(ctx.repl_state);
            let lastsave = Arc::clone(ctx.lastsave);

            tokio::spawn(async move {
                let snap = tokio::task::spawn_blocking(move || {
                    crate::replication::save_dataset(
                        &store,
                        &expiry_store_snap,
                        &index_store_snap,
                        &repl_state,
                    )
                })
                .await;
                // `LASTSAVE` is the last save that *succeeded*. It used to
                // move on whatever happened, so a failed background save
                // looked like a finished one to anything waiting on it.
                let ok = matches!(snap, Ok(Ok(())));
                if ok {
                    crate::stats::reset_rdb_changes();
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as i64;
                    *lastsave.write() = now;
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

        "LASTSAVE" => {
            let t = *ctx.lastsave.read();
            Some(resp_integer(t))
        }

        "FLUSHDB" => {
            let db = *ctx.selected_db;
            touch_watched_keys(ctx, Some(db));
            let flushed = if flush_is_async(cmd) {
                let (values, flushed) = crate::store::take_db(&ctx.store[db]);
                crate::lazyfree::free_later(values, flushed.keys as u64);
                crate::store::clear_expiry_db(&ctx.expiry_store[db]);
                // Nothing to give back to the OS yet: the values are freed
                // in the background.
                crate::store::Flushed::default()
            } else {
                let mut flushed = crate::store::clear_db(&ctx.store[db]);
                flushed += crate::store::clear_expiry_db(&ctx.expiry_store[db]);
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

            let want = |s: &str| {
                all || asked(s) || (default && !matches!(s, "commandstats" | "latencystats"))
            };

            if want("server") {
                out.push_str("# Server\r\n");
                // The Redis release whose commands and replies this server
                // follows. Clients read it to decide what they may send.
                out.push_str("redis_version:8.10.0\r\n");
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
                out.push_str(&format!(
                    "os:{} {}\r\n",
                    std::env::consts::OS,
                    std::env::consts::ARCH
                ));
                out.push_str(&format!(
                    "tcp_port:{}\r\n",
                    crate::listen::current_port(ctx.config.port)
                ));
                out.push_str(&format!("process_id:{}\r\n", std::process::id()));
                out.push_str(&format!("run_id:{}\r\n", crate::stats::RUN_ID.as_str()));
                let uptime = ctx.started_at.elapsed().as_secs();
                out.push_str(&format!("uptime_in_seconds:{uptime}\r\n"));
                out.push_str(&format!("uptime_in_days:{}\r\n", uptime / 86400));
                out.push_str("hz:10\r\n");
                out.push_str("configured_hz:10\r\n");
                out.push_str("multiplexing_api:epoll\r\n");
                out.push_str("atomicvar_api:atomic-builtin\r\n");
                out.push_str("gcc_version:0.0.0\r\n");
                out.push_str("config_file:\r\n");
                out.push_str(&format!(
                    "slowlog_log_slower_than:{}\r\n",
                    ctx.slowlog.threshold_us()
                ));
                out.push_str(&format!("slowlog_max_len:{}\r\n", ctx.slowlog.len_cap()));
                out.push_str("executable:/usr/local/bin/kividb\r\n");
                out.push_str("\r\n");
            }

            if want("tls") {
                let tls_section = {
                    #[cfg(feature = "tls")]
                    let (tls_port, tls_enabled) = if ctx.config.tls_enabled() {
                        (ctx.config.tls_port, 1u8)
                    } else {
                        (0, 0u8)
                    };

                    #[cfg(not(feature = "tls"))]
                    let (tls_port, tls_enabled) = (0u16, 0u8);

                    format!(
                        "# tls\r\n\
                         tls_enabled:{tls_enabled}\r\n\
                         tls_port:{tls_port}\r\n\
                         tls_replication:0\r\n\
                         tls_cluster:0\r\n\
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
                    "cluster_enabled:{}\r\n\r\n",
                    u8::from(ctx.config.cluster_enabled)
                ));
            }

            if want("keyspace") {
                out.push_str("# Keyspace\r\n");
                for (i, (db, expiry_db)) in
                    ctx.store.iter().zip(ctx.expiry_store.iter()).enumerate()
                {
                    let keys: usize = db.iter().map(|shard| read_lock!(shard).len()).sum();
                    let expires: usize =
                        expiry_db.iter().map(|shard| read_lock!(shard).len()).sum();
                    if keys > 0 || expires > 0 {
                        let avg_ttl = if expires > 0 {
                            avg_ttl_ms(expiry_db)
                        } else {
                            0
                        };
                        out.push_str(&format!(
                            "db{}:keys={},expires={},avg_ttl={}\r\n",
                            i, keys, expires, avg_ttl
                        ));
                    }
                }
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
                    crate::stats::TOTAL_COMMANDS_PROCESSED.load(Ordering::Relaxed)
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
                out.push_str("instantaneous_input_kbps:0\r\n");
                out.push_str("instantaneous_output_kbps:0\r\n");
                out.push_str(&format!(
                    "rejected_connections:{}\r\n",
                    crate::stats::REJECTED_CONNECTIONS.load(Ordering::Relaxed)
                ));
                out.push_str("total_reads_processed:0\r\n");
                out.push_str("total_writes_processed:0\r\n");
                out.push_str(&format!(
                    "keyspace_hits:{}\r\n",
                    crate::stats::KEYSPACE_HITS.load(Ordering::Relaxed)
                ));
                out.push_str(&format!(
                    "keyspace_misses:{}\r\n",
                    crate::stats::KEYSPACE_MISSES.load(Ordering::Relaxed)
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
                    for (idx, (ip, port, slave_off, lag, state)) in
                        replica_rows(ctx.repl_state, offset).iter().enumerate()
                    {
                        out.push_str(&format!(
                            "slave{idx}:ip={ip},port={port},state={state},offset={slave_off},lag={lag}\r\n"
                        ));
                    }
                    let load = |v: &std::sync::atomic::AtomicU64| {
                        v.load(std::sync::atomic::Ordering::Relaxed)
                    };
                    out.push_str(&format!("master_replid:{repl_id}\r\n"));
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
                    out.push_str("slave_priority:100\r\n");
                    out.push_str("slave_read_only:1\r\n");
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
                let (rss, peak, vsize) = crate::commands::memory::read_proc_status_pub();
                // Only Linux's VmSize/VmRSS are comparable enough for this to
                // mean anything — see `memory::fragmentation`.
                let (frag_ratio, frag_bytes) = crate::commands::memory::fragmentation(rss, vsize);
                let maxmem = crate::RUNTIME_MAXMEMORY.load(std::sync::atomic::Ordering::Relaxed);
                // What the process has allocated — what `maxmemory` is
                // measured against — as distinct from what the operating
                // system has given it, which is the RSS below.
                let used = crate::memory_use::allocated();
                let startup_memory: usize = 1_000_000;
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
                        rss as f64 / peak as f64 * 100.0
                    } else {
                        100.0
                    }
                ));
                out.push_str(&format!("used_memory_overhead:{overhead}\r\n"));
                out.push_str(&format!("used_memory_startup:{startup_memory}\r\n"));
                out.push_str(&format!("used_memory_dataset:{dataset}\r\n"));
                out.push_str(&format!("used_memory_dataset_perc:{dataset_perc:.2}%\r\n"));
                // Redis scripting tests read this via `[s used_memory_lua]` (unit/scripting.tcl).
                out.push_str("used_memory_lua:0\r\n");
                // The VM functions are loaded into (EVAL scripts that run
                // alone share it).
                #[cfg(feature = "lua")]
                out.push_str(&format!(
                    "used_memory_vm_functions:{}\r\n",
                    crate::commands::lua::functions_vm_bytes()
                ));
                #[cfg(not(feature = "lua"))]
                out.push_str("used_memory_vm_functions:0\r\n");
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
                out.push_str("mem_not_counted_for_evict:0\r\n");
                out.push_str("mem_replication_backlog:0\r\n");
                out.push_str("mem_clients_slaves:0\r\n");
                out.push_str("mem_clients_normal:0\r\n");
                out.push_str("mem_aof_buffer:0\r\n");
                out.push_str("active_defrag_running:0\r\n");
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
                let count = ctx.client_registry.len();
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
                out.push_str("clients_in_timeout_table:0\r\n");
                let (blocking_keys, on_nokey) = ctx.wait_table.blocking_keys();
                out.push_str(&format!("total_blocking_keys:{blocking_keys}\r\n"));
                out.push_str(&format!("total_blocking_keys_on_nokey:{on_nokey}\r\n"));
                out.push_str("maxclients:10000\r\n");
                out.push_str("\r\n");
            }

            if want("persistence") {
                let lastsave = *ctx.lastsave.read();
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
                    if lastsave > 0 { lastsave } else { -1 }
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
                    for hidden in ["key-load-delay", "rdb-key-save-delay"] {
                        if patterns.iter().any(|pattern| pattern == hidden) {
                            pairs.push(resp_bulk_str(hidden));
                            pairs.push(resp_bulk_str(
                                REMEMBERED_CONFIG
                                    .lock()
                                    .get(hidden)
                                    .map_or("0", |value| value.as_str()),
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
                    if cmd.len() < 4 || !cmd.len().is_multiple_of(2) {
                        return Some(resp_error(
                            "ERR wrong number of arguments for 'config|set' command",
                        ));
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
                    // The port last: moving it is what can fail after the
                    // values are found good (the port is taken), and it is
                    // not to be moved back, so it is moved only once the
                    // others are set.
                    settings.sort_by_key(|(name, _)| name == "port");
                    if let Some((_, (twice, _))) = settings
                        .iter()
                        .enumerate()
                        .find(|(i, (name, _))| settings[..*i].iter().any(|(seen, _)| seen == name))
                    {
                        return Some(resp_error(&format!(
                            "ERR CONFIG SET failed (possibly related to argument '{twice}') -                              duplicate parameter"
                        )));
                    }
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
                        if reply.starts_with(b"-") && !reply.starts_with(b"-ERR CONFIG SET failed")
                        {
                            let why = String::from_utf8_lossy(&reply[1..]);
                            let why = why.trim_end().trim_start_matches("ERR ");
                            reply = resp_error(&format!(
                                "ERR CONFIG SET failed (possibly related to argument '{param}') - \
                                 {why}"
                            ));
                        }
                        if reply.starts_with(b"-") {
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
                    crate::stats::KEYSPACE_HITS.store(0, Ordering::Relaxed);
                    crate::stats::KEYSPACE_MISSES.store(0, Ordering::Relaxed);
                    crate::stats::TOTAL_COMMANDS_PROCESSED.store(0, Ordering::Relaxed);
                    crate::stats::TOTAL_CONNECTIONS_RECEIVED.store(0, Ordering::Relaxed);
                    crate::stats::TOTAL_ERROR_REPLIES.store(0, Ordering::Relaxed);
                    crate::stats::TOTAL_NET_INPUT_BYTES.store(0, Ordering::Relaxed);
                    crate::stats::TOTAL_NET_OUTPUT_BYTES.store(0, Ordering::Relaxed);
                    crate::stats::EXPIRED_KEYS.store(0, Ordering::Relaxed);
                    crate::stats::EXPIRED_SUBKEYS.store(0, Ordering::Relaxed);
                    crate::stats::EXPIRED_KEYS_ACTIVE.store(0, Ordering::Relaxed);
                    crate::lazyfree::reset_freed();
                    crate::stats::REJECTED_CONNECTIONS.store(0, Ordering::Relaxed);
                    crate::stats::reset_rdb_changes();
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
                        return Some(resp_array(vec![])); // empty is fine, cli degrades gracefully
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
                    _ => {
                        return Some(resp_null_array());
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
            touch_watched_keys(ctx, None);
            let flushed = if flush_is_async(cmd) {
                let mut values = Vec::with_capacity(ctx.store.len());
                let mut keys = 0;
                for db in ctx.store.iter() {
                    let (taken, flushed) = crate::store::take_db(db);
                    values.push(taken);
                    keys += flushed.keys;
                }
                crate::lazyfree::free_later(values, keys as u64);
                for db in ctx.expiry_store.iter() {
                    crate::store::clear_expiry_db(db);
                }
                crate::store::Flushed::default()
            } else {
                let flushed = crate::store::clear_store(ctx.store, ctx.expiry_store);
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
                    .map(|(ip, port, slave_off, _lag, _state)| {
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
                    crate::pause::set_pause(ms, write_only);
                    Some(resp_simple("OK"))
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
                    let mut prefixes: Vec<String> = Vec::new();
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
                                prefixes.push(String::from_utf8_lossy(prefix).into_owned());
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
                            prefixes.push(String::new());
                        }
                        // One prefix of a client must not be the start of
                        // another: a key would be announced twice.
                        let overlap = |a: &str, b: &str| a.starts_with(b) || b.starts_with(a);
                        for (n, prefix) in prefixes.iter().enumerate() {
                            if let Some(other) =
                                had_prefixes.iter().find(|other| overlap(prefix, other))
                            {
                                return Some(resp_error(&format!(
                                    "ERR Prefix '{prefix}' overlaps with an existing prefix \
                                     '{other}'. Prefixes for a single client must not overlap."
                                )));
                            }
                            if let Some(other) = prefixes[n + 1..]
                                .iter()
                                .find(|other| overlap(prefix, other))
                            {
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
                    let mut prefixes: Vec<String> = Vec::new();
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
                            resp_array(prefixes.iter().map(|p| resp_bulk_str(p)).collect()),
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
                resp_bulk_str("kividb"),
                resp_bulk_str("version"),
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
            // LOLWUT [VERSION version]
            // Returns some art and the server version. The VERSION arg selects
            // which art pattern to show — we have one pattern (version 6, the dragon).
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
                Arc::clone(ctx.expiry_store),
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
                Some(resp_error(
                    "ERR Lua scripting is not supported by KiviDB. \
                     See COMMANDS.md for the design rationale.",
                ))
            }
        }

        "SCRIPT" => {
            #[cfg(feature = "lua")]
            {
                None
            }
            #[cfg(not(feature = "lua"))]
            {
                Some(resp_error(
                    "ERR SCRIPT subsystem is not supported by KiviDB (requires Lua). \
                     See COMMANDS.md for the design rationale.",
                ))
            }
        }

        "FCALL" | "FCALL_RO" => {
            #[cfg(feature = "lua")]
            {
                None
            }
            #[cfg(not(feature = "lua"))]
            {
                Some(resp_error(
                    "ERR FUNCTION subsystem is not supported by KiviDB (requires Lua). \
                     See COMMANDS.md for the design rationale.",
                ))
            }
        }

        "FUNCTION" => {
            #[cfg(feature = "lua")]
            {
                None
            }
            #[cfg(not(feature = "lua"))]
            {
                Some(resp_error(
                    "ERR FUNCTION subsystem is not supported by KiviDB (requires Lua). \
                     See COMMANDS.md for the design rationale.",
                ))
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
        "MODULE" => Some(resp_error(
            "ERR MODULE subsystem is not supported by KiviDB. \
             Dynamic module loading requires dlopen and ABI versioning \
             which conflicts with KiviDB's safety model. \
             See COMMANDS.md for the design rationale.",
        )),

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
