// src/commands/handler.rs

use crate::commands::*;
use crate::replication;
use crate::resp::write::{resp_array, resp_error, resp_integer, resp_null_bulk};
use ctx::Ctx;
use std::sync::atomic::Ordering;

fn dispatch_command_handlers_fallback(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    strings::handle(ctx, name)
        .or_else(|| acl::handle(ctx, name))
        .or_else(|| json::handle(ctx, name))
        .or_else(|| lists::handle(ctx, name))
        .or_else(|| sets::handle(ctx, name))
        .or_else(|| generics::handle(ctx, name))
        .or_else(|| server::handle(ctx, name))
        .or_else(|| streams::handle(ctx, name))
        .or_else(|| connection::handle(ctx, name))
        .or_else(|| vector::handle(ctx, name))
        .or_else(|| vset::handle(ctx, name))
        .or_else(|| hash::handle(ctx, name))
        .or_else(|| zset::handle(ctx, name))
        .or_else(|| bitmap::handle(ctx, name))
        .or_else(|| hyperloglog::handle(ctx, name))
        .or_else(|| geo::handle(ctx, name))
        .or_else(|| ts::handle(ctx, name))
        .or_else(|| pubsub::handle(ctx, name))
        .or_else(|| slowlog::handle(ctx, name))
        .or_else(|| latency::handle(ctx, name))
        .or_else(|| memory::handle(ctx, name))
        .or_else(|| cluster::handle(ctx, name))
        .or_else(|| cluster::handle_migrate(ctx, name))
        .or_else(|| array::handle(ctx, name))
        .or_else(|| hotkeys::handle(ctx, name))
        .or_else(|| backup::handle(ctx, name))
}

/// The reply to a command there is no such command as, in Redis's words:
/// the name as it was sent, and the start of what was sent with it.
pub(crate) fn unknown_command(cmd: &[&[u8]]) -> Vec<u8> {
    // Line breaks cannot be part of an error reply.
    let shown = |raw: &[u8]| -> String {
        String::from_utf8_lossy(&raw[..raw.len().min(128)]).replace(['\r', '\n'], " ")
    };
    let mut args = String::new();
    for arg in cmd.iter().skip(1) {
        if args.len() >= 128 {
            break;
        }
        let room = 128 - args.len();
        let arg = shown(arg);
        let arg: String = arg.chars().take(room).collect();
        args.push_str(&format!("'{arg}' "));
    }
    resp_error(&format!(
        "ERR unknown command '{}', with args beginning with: {}",
        shown(cmd.first().copied().unwrap_or_default()),
        args
    ))
}

/// Command dispatch only — no Lua, no replication / MONITOR / tracking side effects.
/// Used by `redis.call` from Lua scripts (nested `EVAL` is impossible because Lua is not in this chain).
pub(crate) fn dispatch_command_handlers(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    let first = name.as_bytes().first().copied().unwrap_or(0) | 0x20;
    let fast = match first {
        b'g' | b's' | b'i' | b'd' | b'a' | b'm' => strings::handle(ctx, name)
            .or_else(|| generics::handle(ctx, name))
            .or_else(|| sets::handle(ctx, name))
            .or_else(|| server::handle(ctx, name))
            .or_else(|| {
                if first == b'g' {
                    geo::handle(ctx, name)
                } else {
                    None
                }
            })
            .or_else(|| {
                if first == b'a' {
                    acl::handle(ctx, name)
                } else {
                    None
                }
            })
            // AR* (Redis 8.8 arrays) — all 18 start with 'A', so they ride the
            // same first-byte bucket as APPEND/ACL. Checked after those two so
            // the more common commands in this bucket keep their short path.
            .or_else(|| {
                if first == b'a' {
                    array::handle(ctx, name)
                } else {
                    None
                }
            })
            .or_else(|| {
                if first == b'm' {
                    memory::handle(ctx, name)
                } else {
                    None
                }
            }),
        b'l' => lists::handle(ctx, name),
        b'h' => hash::handle(ctx, name).or_else(|| hyperloglog::handle(ctx, name)),
        b'z' => zset::handle(ctx, name),
        b'b' => bitmap::handle(ctx, name),
        b'f' => vector::handle(ctx, name),
        b'j' => json::handle(ctx, name),
        b'x' => streams::handle(ctx, name),
        b'v' => vset::handle(ctx, name).or_else(|| vector::handle(ctx, name)),
        b'p' => connection::handle(ctx, name)
            .or_else(|| generics::handle(ctx, name))
            .or_else(|| pubsub::handle(ctx, name)),
        b'u' => pubsub::handle(ctx, name).or_else(|| generics::handle(ctx, name)),
        b't' => ts::handle(ctx, name)
            .or_else(|| generics::handle(ctx, name))
            .or_else(|| server::handle(ctx, name)),
        b'c' => connection::handle(ctx, name)
            .or_else(|| cluster::handle(ctx, name))
            .or_else(|| cluster::handle_migrate(ctx, name)),
        b'r' | b'e' | b'w' | b'n' | b'o' | b'y' => server::handle(ctx, name)
            .or_else(|| generics::handle(ctx, name))
            .or_else(|| strings::handle(ctx, name)),
        _ => None,
    };
    fast.or_else(|| dispatch_command_handlers_fallback(ctx, name))
}

/// Run one Redis command from inside a Lua script (`redis.call` / `redis.pcall`).
/// `parts` are owned; switched into `ctx.cmd` for the duration of the call.
#[cfg_attr(not(feature = "lua"), allow(dead_code))]
///
/// `parts` are the arguments where the caller has them; they are in
/// `ctx.cmd` while the command runs and taken out again before this returns.
pub(crate) fn execute_command_from_script(
    ctx: &mut Ctx,
    mut parts: Vec<&[u8]>,
    read_only_script: bool,
) -> Vec<u8> {
    if parts.is_empty() {
        return resp_error("ERR wrong number of arguments for redis.call()");
    }
    // A plain FLUSHALL here is not counted as lazily freed.
    let _queued = crate::lazyfree::running_queued();

    // Compute name early (before cmd swap) so we can short-circuit blocking commands cleanly.
    // On the stack, for a name of ordinary length: a script calls this ten
    // or twenty times.
    let mut upper_first = [0u8; 32];
    let name_long;
    let name: &str = if parts[0].len() <= upper_first.len() {
        let upper = &mut upper_first[..parts[0].len()];
        upper.copy_from_slice(parts[0]);
        upper.make_ascii_uppercase();
        std::str::from_utf8(upper).unwrap_or("")
    } else {
        name_long = String::from_utf8_lossy(parts[0]).to_ascii_uppercase();
        name_long.as_str()
    };

    // Redis `CMD_NOSCRIPT` — must win over "cluster disabled" and other dispatch errors.
    if matches!(name, "CLUSTER" | "HOTKEYS" | "BACKUP") {
        return resp_error("ERR This Redis command is not allowed from script");
    }

    // Purely blocking commands — return fixed sentinel values instead of blocking the Lua thread.
    // Redis's behaviour: BLPOP/BRPOP/… → nil;  WAIT → 0;  WAITAOF → {0 0}.
    match name {
        "BLPOP" | "BRPOP" | "BLMOVE" | "BLMPOP" | "BZPOPMIN" | "BZPOPMAX" | "BZMPOP"
        | "BRPOPLPUSH" => return resp_null_bulk(),

        "WAIT" => return resp_integer(0),

        "WAITAOF" => return resp_array(vec![resp_integer(0), resp_integer(0)]),

        // XREAD / XREADGROUP with BLOCK: strip the BLOCK option so the normal handler runs
        // non-blockingly.  If the stream has data it's returned; otherwise null is returned.
        "XREAD" | "XREADGROUP" => {
            let has_block = parts.iter().any(|a| a.eq_ignore_ascii_case(b"BLOCK"));
            if has_block {
                let mut i = 1usize;
                while i < parts.len() {
                    if parts[i].eq_ignore_ascii_case(b"BLOCK") {
                        // Remove BLOCK and its timeout argument
                        parts.remove(i);
                        if i < parts.len() {
                            parts.remove(i);
                        }
                    } else {
                        i += 1;
                    }
                }
            }
        }

        _ => {}
    }

    // What `parts` point at outlives this call; `ctx.cmd` is restored before
    // it returns, on every path.
    let refs_erased: Vec<&'static [u8]> = unsafe { std::mem::transmute(parts) };
    let saved_cmd = std::mem::replace(&mut ctx.cmd, refs_erased);
    let reply = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run_from_script(ctx, read_only_script)
    }));
    // Restored on every path, a panic's too: what `ctx.cmd` points at now is
    // gone once this returns, and a panic in a command a script calls is
    // caught (`lua::bridge`) and the script goes on.
    ctx.cmd = saved_cmd;
    reply.unwrap_or_else(|panic| std::panic::resume_unwind(panic))
}

/// The rest of [`execute_command_from_script`], with the command in `ctx.cmd`.
fn run_from_script(ctx: &mut Ctx, read_only_script: bool) -> Vec<u8> {
    // Re-derive cmd0/name from ctx.cmd in case parts were modified (XREAD BLOCK stripping).
    let cmd0 = ctx.cmd[0];
    let mut upper_buf = [0u8; 32];
    let name_owned2;
    let name2: &str = if cmd0.len() <= 32 {
        for (i, &b) in cmd0.iter().enumerate() {
            upper_buf[i] = b.to_ascii_uppercase();
        }
        std::str::from_utf8(&upper_buf[..cmd0.len()]).unwrap_or("")
    } else {
        name_owned2 = String::from_utf8_lossy(cmd0).to_ascii_uppercase();
        name_owned2.as_str()
    };

    // A script running under its tag's lock keeps to its tag (`tag_locks`).
    if crate::tag_locks::in_script() {
        let outside = if crate::tag_locks::sees_the_whole_keyspace(name2) {
            Some(format!(
                "ERR Script attempted to run '{}', which looks at keys it does not name, while \
                 running under the hash tag '{}' of its KEYS (lua-parallel-scripts)",
                name2.to_ascii_lowercase(),
                crate::tag_locks::script_tag_for_message()
            ))
        } else {
            crate::keyspec::key_positions(name2, &ctx.cmd)
                .iter()
                .map(|pos| ctx.cmd[pos])
                .find(|key| !crate::tag_locks::script_may_touch(key))
                .map(|key| {
                    format!(
                        "ERR Script attempted to access the key '{}', which is outside the hash \
                         tag '{}' of its KEYS (lua-parallel-scripts)",
                        String::from_utf8_lossy(key),
                        crate::tag_locks::script_tag_for_message()
                    )
                })
        };
        if let Some(outside) = outside {
            return resp_error(&outside);
        }
    }

    if read_only_script && replication::inner_command_blocked_in_readonly_script(name2, &ctx.cmd) {
        return resp_error("ERR Write commands are not allowed from read-only scripts.");
    }

    let repl_is_slave = ctx.repl_state.is_slave();

    if repl_is_slave && !ctx.apply_replication {
        if replication::is_denied_on_readonly_replica(name2, &ctx.cmd) {
            return resp_error("READONLY You can't write against a read only replica.");
        }
        if ctx
            .repl_state
            .slave_sync_in_progress
            .load(Ordering::Relaxed)
            && !replication::slave_command_allowed_during_loading(name2)
        {
            return resp_error("LOADING KiviDB is loading the dataset into memory");
        }
    }

    #[cfg(feature = "lua")]
    let skip_mem = crate::commands::lua::script_allow_oom();
    #[cfg(not(feature = "lua"))]
    let skip_mem = false;

    if !repl_is_slave
        && check_memory_limit()
        && replication::is_write_command_bytes(cmd0)
        && !crate::eviction::allowed_when_out_of_memory(name2)
        && !skip_mem
    {
        log_throttled!(
            60,
            crate::loglevel::WARN,
            "maxmemory ({} bytes) reached and nothing more can be evicted under \
             maxmemory-policy {}: write commands are refused with OOM. Raise maxmemory, \
             use an evicting policy such as allkeys-lru, or delete data",
            crate::RUNTIME_MAXMEMORY.load(std::sync::atomic::Ordering::Relaxed),
            crate::eviction::policy().name()
        );
        return resp_error("OOM command not allowed when used memory > 'maxmemory'.");
    }

    #[cfg(feature = "lua")]
    if !repl_is_slave && let Some(rep) = crate::commands::lua::script_min_replicas_gate(ctx, cmd0) {
        return rep;
    }

    // A command a script calls is checked against the ACL of the user running
    // the script, as one that user sent would be. It was not checked at all:
    // a user allowed only EVAL could run any command on any key from a
    // script, `ACL SETUSER` on itself included.
    if !ctx.apply_replication
        && (crate::acl::RESTRICTED_CMD_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) > 0
            || crate::acl::RESTRICTED_KEY_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) > 0)
    {
        let denied = ctx
            .acl_store
            .read()
            .get(ctx.authenticated_user)
            .and_then(|user| crate::acl::check_permission(user, name2, ctx.cmd.as_slice()).err());
        if let Some(denied) = denied {
            ctx.acl_log.record(
                denied.reason(),
                denied.object(),
                ctx.authenticated_user,
                ctx.peer_addr,
            );
            let reply = resp_error(&denied.message(ctx.authenticated_user));
            return reply;
        }
    }

    // What a script calls is shown to MONITOR as called by `lua`.
    crate::monitor::feed(ctx.monitor_registry, &ctx.cmd, *ctx.selected_db, "lua");

    // Unknown-command message must match Redis's wording (Tcl tests use glob `*Unknown Redis*`).
    let started = std::time::Instant::now();
    let result = dispatch_command_handlers(ctx, name2)
        .unwrap_or_else(|| resp_error("ERR Unknown Redis command called from script"));
    // Counted for `INFO commandstats`, like a command a client sends.
    crate::cmdstats::record(name2, &ctx.cmd, &result, started.elapsed());

    #[cfg(feature = "lua")]
    if !result.starts_with(b"-") && replication::is_write_command_bytes(cmd0) {
        crate::commands::lua::script_note_successful_write();
    }

    // A write from a script leaves the same things to be done as one from a
    // client: a key it emptied goes, and clients blocked on a key it wrote
    // are woken.
    if !result.starts_with(b"-") && replication::is_write_command_bytes(cmd0) {
        settle_written_keys(ctx, name2);
    }
    // And clients caching a key it wrote are told; a key it read is one its
    // caller has read.
    {
        let is_write = replication::is_write_command_bytes(cmd0);
        track(ctx, name2, is_write && !result.starts_with(b"-"), is_write);
    }

    #[cfg(feature = "lua")]
    if !result.starts_with(b"-")
        && !repl_is_slave
        && !ctx.apply_replication
        && replication::command_mutates_for_replication(name2, ctx.cmd.as_slice())
    {
        let feeds = replication::script_feed::commands_for_propagation(
            ctx,
            name2,
            ctx.cmd.as_slice(),
            &result,
        );
        // Same guard as `apply_write_side_effects`. `record_rdb_change` still runs
        // per feed regardless — it tracks dirty-key count for RDB save, which is
        // independent of whether anything consumes the replication stream.
        let observed = replication::write_is_observed(ctx);
        for parts in feeds {
            if observed {
                replication::replicate_one_write(ctx, &parts);
            }
            crate::stats::record_rdb_change();
        }
        // A write made from a script dirties a WATCH like any other write.
        notify_watchers(ctx, name2);
    }

    rewrite_wrong_arity_error_for_script(result)
}

/// Redis replaces per-command arity errors with one stable message for scripts (script.c).
fn rewrite_wrong_arity_error_for_script(resp: Vec<u8>) -> Vec<u8> {
    if resp.starts_with(b"-ERR wrong number of arguments for redis.call") {
        return resp;
    }
    if resp.starts_with(b"-ERR wrong number of arguments") {
        return resp_error("ERR Wrong number of args calling Redis command from script");
    }
    resp
}

/// Effective RESP version for a command reply: `redis.setresp` overrides the connection when
/// inside a script; otherwise the protocol this connection negotiated via `HELLO`.
///
/// Reads the `ctx.resp` field rather than looking the client up in the registry. The two are
/// kept in lockstep — `HELLO` assigns the connection's `resp_version` and
/// `client_registry.resp` in the same match arm, in both the tokio (`server.rs`) and io_uring
/// (`server_uring.rs`) loops, and those are the only writers of either — so this is the same
/// value without the `RwLock` read and hash lookup `ClientRegistry::get_resp` costs. That
/// matters because RESP3 shaping is decided per reply across the whole command surface; a lock
/// acquisition per reply is not something the hot path should pay.
///
/// It is also strictly more correct inside scripts. A `redis.call` Ctx is built with `resp: 2`
/// on purpose (its result becomes a Lua value, not wire bytes), and Redis likewise defaults
/// scripts to RESP2 regardless of the calling client's protocol. The old registry lookup
/// ignored that and returned the *client's* protocol, so a client that had sent `HELLO 3` would
/// see RESP3-shaped replies inside a script that never called `redis.setresp 3`.
#[inline]
pub(crate) fn effective_resp_for_reply(ctx: &Ctx<'_>) -> u8 {
    #[cfg(feature = "lua")]
    {
        if let Some(r) = crate::commands::lua::script_eval_script_resp() {
            return r;
        }
    }
    ctx.resp
}

/// Negotiated wire protocol for the **connected client** (ignores `redis.setresp`).
#[allow(dead_code)]
#[inline]
pub(crate) fn client_connection_resp(ctx: &Ctx<'_>) -> u8 {
    ctx.client_registry.get_resp(ctx.client_id)
}

/// Whether the process is using more than `maxmemory`. Costs one atomic
/// load when there is no limit; with one, see `memory_use::over`.
pub fn check_memory_limit() -> bool {
    let limit = crate::RUNTIME_MAXMEMORY.load(std::sync::atomic::Ordering::Relaxed);
    limit != 0 && crate::memory_use::over(limit)
}

/// Run one command.
///
/// A blocking command that has to wait registers its waiters and parks them in
/// `Ctx::blocking` instead of returning a reply; the caller (a connection loop)
/// then awaits and re-dispatches. Callers that passed `blocking: None` — Lua,
/// `MULTI`/`EXEC`, the replica applier, tests — get the non-blocking reply
/// instead, which is what Redis does inside `MULTI`. See
/// `crate::commands::blocking`.
pub fn execute_command(mut ctx: Ctx) -> Vec<u8> {
    let ctx = &mut ctx;
    if ctx.cmd.is_empty() {
        return resp_error("ERR empty command");
    }
    ctx.skip_write_side_effects = false;
    crate::stats::record_command_processed();

    let cmd0 = &ctx.cmd[0];
    let mut upper_buf = [0u8; 32];
    let name_owned;
    let name: &str = if cmd0.len() <= 32 {
        // ASCII is checked as the bytes are copied, which is all the
        // validation a name needs: a second pass over it as UTF-8 was a
        // measurable part of every command.
        let mut ascii = true;
        for (i, &b) in cmd0.iter().enumerate() {
            ascii &= b.is_ascii();
            upper_buf[i] = b.to_ascii_uppercase();
        }
        if ascii {
            // SAFETY: every byte was just checked to be ASCII.
            unsafe { std::str::from_utf8_unchecked(&upper_buf[..cmd0.len()]) }
        } else {
            ""
        }
    } else {
        name_owned = String::from_utf8_lossy(cmd0).to_ascii_uppercase();
        name_owned.as_str()
    };

    // Counted and timed for `INFO commandstats` — unless it parked, in which
    // case it has not run yet and is counted when it is resumed.
    let started = std::time::Instant::now();
    let reply = run_command(ctx, name);
    let elapsed = started.elapsed();
    // For the connection loop's slow log: see `cmdstats::last_command_us`.
    crate::cmdstats::note_last_command(elapsed);
    if !crate::commands::blocking::parked(ctx) {
        crate::cmdstats::record(name, &ctx.cmd, &reply, elapsed);
    }
    // An index definition goes to the rest of the cluster once it is made
    // here, with nothing held: see `cluster::ddl`.
    if ctx.cluster.enabled && crate::cluster::ddl::is_definition_command(name) {
        return crate::cluster::ddl::spread(ctx, reply);
    }
    reply
}

/// [`execute_command`], once the command's name is known.
fn run_command(ctx: &mut Ctx, name: &str) -> Vec<u8> {
    // The wrong number of arguments is refused before anything else looks
    // at the command, as in Redis. See `arity`.
    if let Some(refusal) = crate::commands::arity::check(name, &ctx.cmd) {
        return refusal;
    }
    let cmd0 = &ctx.cmd[0];

    // ── Cluster routing check ────────────────────────────────────────
    //
    // Decide whether this command can run on this node. In standalone mode
    // or when bypass_cluster_check is true (replica applier), this is a
    // single boolean check and passes through.
    {
        use crate::cluster::routing::route_named;
        // Standalone-mode fast path: skip every cluster-related check entirely.
        // A single branch here keeps the asking-flag lookup and the closure
        // setup off the standalone hot path.
        if ctx.cluster.enabled && !ctx.bypass_cluster_check {
            // A cluster node has one keyspace. Letting SELECT move a
            // connection to another database would put keys where no slot
            // accounting, migration or replication check ever looks.
            if name == "SELECT" && ctx.cmd.get(1).is_some_and(|db| *db != &b"0"[..]) {
                return resp_error("ERR SELECT is not allowed in cluster mode");
            }
            // Pull asking flag from client registry.
            let asking_flag = ctx.client_registry.get_asking(ctx.client_id);
            // has_key_locally: bind to the store/db combination via closure.
            let store_for_check = ctx.store;
            let db = *ctx.selected_db;
            let has_key_locally = |key: &[u8]| -> bool {
                let key_str = match std::str::from_utf8(key) {
                    Ok(s) => s,
                    Err(_) => return false,
                };
                let shard = crate::store::get_shard(&store_for_check[db], key_str);
                crate::read_lock!(shard).contains_key(key_str)
            };
            let registry = ctx.client_registry;
            let client_id = ctx.client_id;
            let decision = route_named(
                name,
                ctx.cmd.as_slice(),
                ctx.cluster,
                asking_flag,
                || registry.get_readonly(client_id),
                has_key_locally,
            );
            // ASKING is single-shot: clear after this command regardless of outcome.
            if asking_flag {
                ctx.client_registry.update(ctx.client_id, |info| {
                    info.asking = false;
                });
            }
            if let Some(refusal) = decision.refusal() {
                return refusal;
            }
        }
    }

    #[cfg(feature = "lua")]
    if crate::commands::lua::lua_server_busy_for_command(ctx.cmd.as_slice()) {
        return resp_error(
            "BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.",
        );
    }

    // [`ReplState::is_slave`] is a relaxed atomic load — avoid `role()` here (it allocates).
    let repl_is_slave = ctx.repl_state.is_slave();

    // Masters skip `is_write_command` entirely here — hot path for standalone read benchmarks.
    if repl_is_slave
        && !ctx.apply_replication
        && replication::is_denied_on_readonly_replica(name, &ctx.cmd)
    {
        return resp_error("READONLY You can't write against a read only replica.");
    }
    // Too few good replicas: a write is refused, as Redis refuses it. Not
    // a command of a transaction (EXEC was refused whole, or let through
    // whole, before it began: see `transaction::exec_refusal`), nor a
    // script, whose writes are refused one by one as it makes them
    // (`lua::script_min_replicas_gate`). One relaxed load when it is off.
    if !ctx.apply_replication
        && ctx.repl_state.min_replicas_to_write.load(Ordering::Relaxed) != 0
        && !matches!(
            name,
            "EVAL" | "EVALSHA" | "EVAL_RO" | "EVALSHA_RO" | "FCALL" | "FCALL_RO"
        )
        && !crate::command_gate::alone_on_this_thread()
        && replication::is_write_command_bytes(name.as_bytes())
        && ctx.repl_state.lacks_good_replicas()
    {
        return resp_error(replication::NOREPLICAS);
    }
    let loading_replica = repl_is_slave
        && !ctx.apply_replication
        && ctx
            .repl_state
            .slave_sync_in_progress
            .load(Ordering::Relaxed);
    // `LOADING` is for clients. A command arriving from the master's stream
    // (or from AOF replay) has to be applied regardless: refusing it does not
    // make the master retry, it just drops the write, and the replica quietly
    // diverges for as long as the index rebuild takes.
    let loading_snapshot =
        !ctx.apply_replication && crate::vector::SNAPSHOT_INDEX_REBUILD.load(Ordering::Relaxed);
    if (loading_replica || loading_snapshot)
        && !replication::slave_command_allowed_during_loading(name)
    {
        return resp_error("LOADING KiviDB is loading the dataset into memory");
    }

    // ── ACL command & key pattern check ─────────────────────────────────────
    // Two Relaxed atomic loads — free when no restricted clients are connected.
    // No exemption for "default": it's a real ACL user like any other and can
    // carry its own restrictions (`ACL SETUSER default ... -flushall`) — the
    // counters above already only go non-zero when *some* connected user
    // (including a restricted "default") actually needs checking, so gating
    // on username here would just silently un-enforce that one username.
    if crate::acl::RESTRICTED_CMD_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) > 0
        || crate::acl::RESTRICTED_KEY_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) > 0
    {
        // The command, every key it names (`keyspec` knows where they are)
        // and every channel, under the user's rules or one of its selectors.
        let denied = ctx
            .acl_store
            .read()
            .get(ctx.authenticated_user)
            .and_then(|user| crate::acl::check_permission(user, name, ctx.cmd.as_slice()).err());
        if let Some(denied) = denied {
            ctx.acl_log.record(
                denied.reason(),
                denied.object(),
                ctx.authenticated_user,
                ctx.peer_addr,
            );
            return resp_error(&denied.message(ctx.authenticated_user));
        }
    }

    // Over `maxmemory`, a write that could add to what is in use has room
    // made for it — below, once the gates are held — or is refused.
    // `check_memory_limit` returns false immediately when maxmemory==0.
    //
    // Not a write that is being loaded — the append-only file replayed at
    // startup, a master's stream applied: the limit is on what clients may
    // add, not on what the server already holds. (It used to apply to a
    // replay too: a server whose dataset had outgrown `maxmemory` refused
    // every write in its own log and started empty.)
    //
    // Over the limit, room is made before any command runs — a read too,
    // as in Redis: memory is over the limit whoever notices. A command that
    // may add to it is refused if no room could be made.
    let over_limit = !repl_is_slave && !ctx.apply_replication && check_memory_limit();
    // A library of functions is memory too, though not in the keyspace.
    let loads_functions = name == "FUNCTION"
        && ctx.cmd.get(1).is_some_and(|sub| {
            sub.eq_ignore_ascii_case(b"LOAD") || sub.eq_ignore_ascii_case(b"RESTORE")
        });
    let needs_room = over_limit
        && (loads_functions
            || (replication::is_write_command_bytes(cmd0)
                && !crate::eviction::allowed_when_out_of_memory(name)));

    // Writes hold the write gate from before they touch the store until their
    // replication/AOF payload has been queued, so a full resync or checkpoint
    // can cut cleanly between whole writes. See `write_gate`.
    //
    // Scripts are gated as a unit: their writes propagate one `redis.call` at
    // a time, and a cut in the middle of one would be a cut in the middle of
    // something that is supposed to be atomic.
    //
    // MIGRATE is deliberately not gated. It blocks on the network for up to
    // its timeout, which would stall every write behind a cut, and it needs no
    // protection: it propagates as an idempotent `DEL`.
    let writes_operation = replication::command_mutates_for_replication(name, &ctx.cmd);
    let script = matches!(name, "EVAL" | "EVALSHA" | "FCALL");
    let gated = (writes_operation && name != "MIGRATE") || script;

    // And, while anything is listening to the write stream, a write is
    // ordered against the other writes to the same keys, so that the stream
    // carries them in the order the store took them. See `write_order`.
    //
    // A write its master sent is already in order: the stream is one
    // connection, applied by one task.
    let ordered = gated && !ctx.apply_replication && replication::write_is_observed(ctx);

    // Every command runs inside the command gate; a script — read-only or
    // not — and a flush that has to be ordered take it alone. So does
    // `EXEC`, in the server loop, around the commands it runs. Nothing else
    // runs while one of those does: see `command_gate`.
    let any_script = script || matches!(name, "EVAL_RO" | "EVALSHA_RO" | "FCALL_RO");
    // Not for ever, if what holds the gate is a script that has run past its
    // time limit: the command is told `BUSY`, as it would have been had it
    // arrived a moment later. See `command_gate::enter_unless`.
    #[cfg(feature = "lua")]
    let overrun = crate::commands::lua::running_script_is_over_its_time_limit;
    #[cfg(not(feature = "lua"))]
    let overrun = || false;
    const SLICE: std::time::Duration = std::time::Duration::from_millis(10);
    // Scripts by hash tag, when that is switched on (`tag_locks`). A script
    // whose keys share a tag is held to it, however it is run. One that its
    // caller has seen to be a quick one also runs beside other scripts: it
    // takes the gate as a command does, and its tag's lock below. Not inside
    // a transaction or another script: those hold the gate alone already.
    let tag_locks = crate::tag_locks::enabled();
    let allowed_beside = any_script && crate::tag_locks::take_next_script_allowed();
    let script_tag: Option<&[u8]> = if tag_locks && any_script {
        crate::tag_locks::script_tag(&ctx.cmd)
    } else {
        None
    };
    let beside_others =
        script_tag.is_some() && allowed_beside && !crate::command_gate::alone_on_this_thread();
    // And a command that looks at keys it does not name must not find a
    // script part-way through: it waits for all of them.
    let whole_keyspace = tag_locks && crate::tag_locks::sees_the_whole_keyspace(name);
    let gates = if crate::command_gate::skips(name, &ctx.cmd) {
        Some((None, None))
    } else if (any_script && !beside_others)
        || whole_keyspace
        || crate::tag_locks::is_switch(name, &ctx.cmd)
        || (ordered && matches!(name, "FLUSHALL" | "FLUSHDB" | "SWAPDB"))
    {
        crate::command_gate::enter_alone_unless(SLICE, overrun).map(|alone| (None, Some(alone)))
    } else {
        crate::command_gate::enter_unless(SLICE, overrun).map(|gate| (Some(gate), None))
    };
    let Some((_command_gate, _alone)) = gates else {
        return resp_error(
            "BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.",
        );
    };

    // A write under a script or an `EXEC` that already holds the gate is part
    // of that one: the handoff's cut waits for the whole of it.
    let nested = crate::write_gate::held_by_this_thread();
    let mut _write_gate = gated.then(crate::write_gate::enter);
    // A handoff under way — slots migrating away, or a manual failover — has
    // paused writes, and this one got past the pause before it was set. It
    // must not run behind the handoff's back (see `pause::HANDOFF`): out of
    // the gate, wait, and then answer as whoever owns the key now would.
    while gated && !nested && !ctx.apply_replication && crate::pause::handoff() {
        _write_gate = None;
        crate::pause::wait_out_handoff();
        if ctx.cluster.enabled {
            let decision = crate::cluster::routing::route_named(
                name,
                ctx.cmd.as_slice(),
                ctx.cluster,
                false,
                || false,
                |_| true,
            );
            if let Some(refusal) = decision.refusal() {
                return refusal;
            }
        }
        if ctx.repl_state.is_slave() && replication::is_denied_on_readonly_replica(name, &ctx.cmd) {
            return resp_error("READONLY You can't write against a read only replica.");
        }
        _write_gate = Some(crate::write_gate::enter());
    }

    if over_limit {
        // The keys it deletes are writes too, to be cut between and ordered
        // like any other; it orders them itself, so this comes before the
        // command takes the order of its own keys.
        let _evicting = crate::write_gate::enter();
        let limit = crate::RUNTIME_MAXMEMORY.load(std::sync::atomic::Ordering::Relaxed);
        let in_order = ordered || (!gated && replication::write_is_observed(ctx));
        if limit != 0 && !crate::eviction::make_room(ctx, limit, in_order) && needs_room {
            log_throttled!(
                60,
                crate::loglevel::WARN,
                "maxmemory ({} bytes) reached and nothing more can be evicted under \
                 maxmemory-policy {}: write commands are refused with OOM. Raise maxmemory, \
                 use an evicting policy such as allkeys-lru, or delete data",
                crate::RUNTIME_MAXMEMORY.load(std::sync::atomic::Ordering::Relaxed),
                crate::eviction::policy().name()
            );
            return crate::tracking::with_what_came_before(resp_error(
                "OOM command not allowed when used memory > 'maxmemory'.",
            ));
        }
    }

    // The tag's lock, for a script that runs under one; and for a command
    // from a client, the lock of every tagged key it names, so that it finds
    // those keys as they were before a script or as they are after it.
    // Before the order of writes below, for both, so that the two are always
    // taken the same way round.
    let _script_tag = script_tag.map(|tag| {
        if beside_others {
            crate::tag_locks::lock_for_script(tag)
        } else {
            crate::tag_locks::hold_script_to(tag)
        }
    });
    let _key_tags = if tag_locks
        && !any_script
        && !crate::command_gate::alone_on_this_thread()
        && !crate::tag_locks::in_script()
    {
        let keys = crate::keyspec::key_positions(name, &ctx.cmd);
        crate::tag_locks::lock_keys(keys.iter().map(|pos| ctx.cmd[pos]))
    } else {
        None
    };

    let _write_order = if crate::command_gate::alone_on_this_thread() {
        // No other command is running: there is nothing to be ordered
        // against, and the writes made from inside this one take nothing.
        Some(crate::write_order::held_exclusively())
    } else if ordered {
        let keys = crate::keyspec::key_positions(name, &ctx.cmd);
        Some(crate::write_order::lock_keys(
            *ctx.selected_db,
            keys.iter().map(|pos| ctx.cmd[pos]),
        ))
    } else {
        None
    };

    // HOTKEYS: one relaxed load when no session is running.
    let hotkeys_started = hotkeys::active().then(std::time::Instant::now);

    // MONITOR: a command is shown when it is called, before it runs — what a
    // script calls then follows the script, as it happened — and a command
    // that blocks is shown once, when it arrives, not again when it resumes.
    // The administrative commands are not shown, as in Redis.
    if crate::monitor::MONITOR_COUNT.load(std::sync::atomic::Ordering::Relaxed) > 0
        && !crate::commands::blocking::resuming(ctx)
        && !crate::commands::server::is_admin_command(name)
    {
        crate::monitor::feed(
            ctx.monitor_registry,
            &ctx.cmd,
            *ctx.selected_db,
            ctx.peer_addr,
        );
    }

    // `notify-keyspace-events n`: which of the keys the command names are
    // not there now, to see afterwards which of them it created.
    let announce_new = writes_operation
        && crate::keyspace_events::KS_FLAGS.load(std::sync::atomic::Ordering::Relaxed)
            & crate::keyspace_events::KS_NEW
            != 0;
    let exists = |ctx: &Ctx, key: &str| {
        crate::read_lock!(crate::store::get_shard(&ctx.store[*ctx.selected_db], key))
            .contains_key(key)
    };
    let absent: Vec<usize> = if announce_new {
        crate::keyspec::key_positions(name, &ctx.cmd)
            .iter()
            .filter(|pos| std::str::from_utf8(ctx.cmd[*pos]).is_ok_and(|key| !exists(ctx, key)))
            .collect()
    } else {
        Vec::new()
    };

    let result = dispatch_command_handlers(ctx, name)
        .or_else(|| lua::handle(ctx, name))
        .unwrap_or_else(|| unknown_command(&ctx.cmd));

    for pos in absent {
        if let Ok(key) = std::str::from_utf8(ctx.cmd[pos])
            && exists(ctx, key)
        {
            crate::keyspace_events::notify(
                ctx.pubsub,
                *ctx.selected_db,
                crate::keyspace_events::KS_NEW,
                "new",
                key,
            );
        }
    }

    // A parked command has not done its work yet — there is nothing to
    // propagate, invalidate or feed to MONITOR until it is resumed, and the
    // reply above is a discarded placeholder. The resume dispatch runs this.
    if crate::commands::blocking::parked(ctx) {
        return result;
    }

    if let Some(started) = hotkeys_started {
        hotkeys::record(ctx, name, result.len(), started.elapsed());
    }

    // An eviction policy that goes by how keys are used is told which were.
    // (Not by the commands that only ask about a key: Redis does not count
    // those as a use of it either.)
    if crate::eviction::recording()
        && !matches!(
            name,
            "OBJECT" | "TYPE" | "TTL" | "PTTL" | "EXPIRETIME" | "PEXPIRETIME" | "EXISTS" | "DEBUG"
        )
    {
        let wrote = writes_operation && !result.starts_with(b"-");
        for pos in crate::keyspec::key_positions(name, &ctx.cmd).iter() {
            crate::eviction::used(ctx.store, *ctx.selected_db, ctx.cmd[pos], wrote);
        }
    }

    write_side_effects(ctx, name, &result, writes_operation);

    // Command handlers hardcode RESP2's legacy null encoding (`$-1`/`*-1`); rewrite it to the
    // RESP3 null type (`_`) here, once, for RESP3 connections — see `resp3_normalize_null`.
    // `ctx.resp` is per-connection local state, not a client_registry lookup — see Ctx::resp.
    let result = if ctx.resp == 3 {
        crate::resp::write::resp3_normalize_null(result)
    } else {
        result
    };
    crate::tracking::with_what_came_before(result)
}

/// Replication/AOF propagation, WATCH notification, RDB-change tracking, MONITOR broadcast,
/// and client-side-cache invalidation for a command that just ran through `dispatch`.
///
/// Split out of `execute_command` so MULTI/EXEC's lock-held fast-apply path (see
/// `server_uring::handle_client`'s EXEC arm, which bypasses `dispatch_command_handlers` for a
/// narrow set of commands to close a WATCH lost-update race) gets these side effects from the
/// exact same code `execute_command` uses, rather than a hand-duplicated copy that could drift.
///
/// The caller must hold the write gate (`write_gate::enter`) across the store
/// mutation *and* this call; `execute_command` does so itself.
#[inline]
pub(crate) fn apply_write_side_effects(ctx: &Ctx, name: &str, result: &[u8]) {
    let writes_operation = replication::command_mutates_for_replication(name, &ctx.cmd);
    write_side_effects(ctx, name, result, writes_operation);
}

/// Mark every `WATCH` on a key this write touched as dirty.
///
/// It used to notify `cmd[1]` and nothing else. So `MSET a 1 b 2` left a
/// client watching `b` free to `EXEC`, as did `RENAME a b`, `LMOVE a b …`,
/// `SUNIONSTORE …` for their other keys — and for a command whose first
/// argument is not a key (`XGROUP CREATE s …`, `LMPOP 1 k …`) it notified a
/// "key" named `CREATE` or `1`. A flush did not notify anyone.
fn notify_watchers(ctx: &Ctx, name: &str) {
    if !ctx.watch_registry.has_watchers() {
        return;
    }
    let db = *ctx.selected_db;
    match name {
        // The flush has said which watched keys it removed, before it
        // removed them (`touch_watched_keys`).
        "FLUSHDB" | "FLUSHALL" => return,
        "SWAPDB" => return ctx.watch_registry.notify_db(None),
        _ => {}
    }
    let keys = crate::keyspec::key_positions(name, &ctx.cmd);
    if keys.is_empty() {
        // Not in the key table: keep the old first-argument guess.
        if let Some(Ok(key)) = ctx.cmd.get(1).map(|k| std::str::from_utf8(k)) {
            ctx.watch_registry.notify(db, key);
        }
        return;
    }
    for pos in keys.iter() {
        if let Ok(key) = std::str::from_utf8(ctx.cmd[pos])
            && !crate::watch::is_unchanged(db, key)
        {
            ctx.watch_registry.notify(db, key);
        }
    }
}

/// Commands that can take the last element out of a list, set, sorted set,
/// hash or vector set, or store a result that turns out to be empty.
fn can_leave_a_key_empty(name: &str) -> bool {
    matches!(
        name,
        "LPOP"
            | "RPOP"
            | "LMPOP"
            | "BLPOP"
            | "BRPOP"
            | "BLMPOP"
            | "LREM"
            | "LTRIM"
            | "RPOPLPUSH"
            | "LMOVE"
            | "BLMOVE"
            | "BRPOPLPUSH"
            | "LMOVEM"
            | "BLMOVEM"
            | "SPOP"
            | "SREM"
            | "SMOVE"
            | "SDIFFSTORE"
            | "SINTERSTORE"
            | "SUNIONSTORE"
            | "ZADD"
            | "ZINCRBY"
            | "ZREM"
            | "ZPOPMIN"
            | "ZPOPMAX"
            | "BZPOPMIN"
            | "BZPOPMAX"
            | "ZMPOP"
            | "BZMPOP"
            | "ZREMRANGEBYSCORE"
            | "ZREMRANGEBYRANK"
            | "ZREMRANGEBYLEX"
            | "ZRANGESTORE"
            | "ZDIFFSTORE"
            | "ZINTERSTORE"
            | "ZUNIONSTORE"
            | "GEOSEARCHSTORE"
            | "GEORADIUS"
            | "GEORADIUSBYMEMBER"
            | "GEODELBYMEMBER"
            | "SORT"
            | "HDEL"
            | "HGETDEL"
            | "HGETEX"
            | "HSETEX"
            | "HEXPIRE"
            | "HPEXPIRE"
            | "HEXPIREAT"
            | "HPEXPIREAT"
            | "VREM"
    )
}

/// Delete `key` if it holds a collection with nothing in it.
fn drop_if_empty(ctx: &Ctx, db: usize, key: &str) -> bool {
    let shard = crate::store::get_shard(&ctx.store[db], key);
    if !crate::read_lock!(shard)
        .get(key)
        .is_some_and(|value| value.is_empty_collection())
    {
        return false;
    }
    // Looked at again under the write lock: a push may have got in between.
    let removed = {
        let mut map = crate::write_lock!(shard);
        let still_empty = map
            .get(key)
            .is_some_and(|value| value.is_empty_collection());
        still_empty && map.remove(key).is_some()
    };
    if removed {
        helpers::clear_expiry(ctx.expiry_store, db, key);
        crate::vector::maybe_unindex(ctx.index_store, key);
        crate::keyspace_events::notify(
            ctx.pubsub,
            db,
            crate::keyspace_events::KS_GENERIC,
            "del",
            key,
        );
    }
    removed
}

/// What a write leaves to be done to the keys it named.
///
/// * A key whose last element it removed is deleted. Left in place it would
///   still answer `EXISTS`, `TYPE`, `KEYS` and `DBSIZE`, be saved, replicated
///   and counted in its slot — a queue drained by `LPOP` would never go away.
/// * Clients blocked on a key it wrote are woken to look at it again. It is
///   any write and not only a push, because a list can also arrive by
///   `RENAME`, `COPY`, `RESTORE` or `SORT … STORE`.
///
/// Both follow from the command's keys and run after it, rather than each
/// handler having to remember; on a replica and during AOF replay they run
/// for the same commands, so the two sides agree.
fn settle_written_keys(ctx: &Ctx, name: &str) {
    // A key restored whole is indexed as a whole: see `reindex_hash_key`.
    if name == "RESTORE" {
        for pos in crate::keyspec::key_positions(name, &ctx.cmd).iter() {
            if let Ok(key) = std::str::from_utf8(ctx.cmd[pos]) {
                let db = &ctx.store[*ctx.selected_db];
                crate::vector::reindex_hash_key(ctx.index_store, db, key);
                crate::vector::reindex_json_key(ctx.index_store, db, key);
            }
        }
    }
    // A command that can leave a hash with field TTLs under a key: the
    // background sweep has to know the key. See `field_expiry`.
    match name {
        "HEXPIRE" | "HPEXPIRE" | "HEXPIREAT" | "HPEXPIREAT" | "HSETEX" | "HGETEX" | "HIMPORT"
        | "RENAME" | "RENAMENX" | "RESTORE" => {
            for pos in crate::keyspec::key_positions(name, &ctx.cmd).iter() {
                if let Ok(key) = std::str::from_utf8(ctx.cmd[pos]) {
                    crate::field_expiry::track_key(ctx.store, *ctx.selected_db, key);
                }
            }
        }
        // These can put it in another database.
        "COPY" | "MOVE" => {
            for pos in crate::keyspec::key_positions(name, &ctx.cmd).iter() {
                if let Ok(key) = std::str::from_utf8(ctx.cmd[pos]) {
                    for db in 0..ctx.store.len() {
                        crate::field_expiry::track_key(ctx.store, db, key);
                    }
                }
            }
        }
        "SWAPDB" => crate::field_expiry::rescan(ctx.store),
        // A JSON document was written: the `ON JSON` indexes that cover it
        // are brought up to date. Here and not in each command, so that
        // every command that writes one is covered — and so is a replica,
        // and a replay.
        _ if name.starts_with("JSON.") => {
            for pos in crate::keyspec::key_positions(name, &ctx.cmd).iter() {
                if let Ok(key) = std::str::from_utf8(ctx.cmd[pos]) {
                    crate::vector::reindex_json_key(
                        ctx.index_store,
                        &ctx.store[*ctx.selected_db],
                        key,
                    );
                }
            }
        }
        _ => {}
    }

    let may_be_empty = can_leave_a_key_empty(name);
    let waiters = ctx.wait_table.has_signal_waiters();
    // A flush names no key and empties them all: a client blocked on a
    // stream's consumer group is waiting on a group that is no longer there.
    if waiters && matches!(name, "FLUSHALL" | "FLUSHDB" | "SWAPDB") {
        ctx.wait_table.notify_all_signals();
        return;
    }
    if !may_be_empty && !waiters {
        return;
    }
    let db = *ctx.selected_db;
    for pos in crate::keyspec::key_positions(name, &ctx.cmd).iter() {
        let Ok(key) = std::str::from_utf8(ctx.cmd[pos]) else {
            continue;
        };
        if may_be_empty && drop_if_empty(ctx, db, key) {
            continue;
        }
        if waiters {
            ctx.wait_table.notify_signal(key);
        }
    }
}

/// [`apply_write_side_effects`] for a caller that has already classified the
/// command, so the hot path does not classify it twice.
#[inline]
fn write_side_effects(ctx: &Ctx, name: &str, result: &[u8], writes_operation: bool) {
    side_effects(ctx, name, result, writes_operation);
    crate::watch::clear_unchanged();
}

fn side_effects(ctx: &Ctx, name: &str, result: &[u8], writes_operation: bool) {
    // Separate command kind from success: tracking invalidates only OK writes; register runs for
    // non-write commands regardless of `-ERR`.
    let is_mutating_ok_write = writes_operation && !result.starts_with(b"-");

    if is_mutating_ok_write {
        settle_written_keys(ctx, name);
        // A write of this node's own — not one its master sent — means its
        // dataset is no longer a position in that master's stream.
        if !ctx.apply_replication {
            ctx.repl_state.note_local_write();
        }
    }

    // Replication / AOF / watch / RDB — successful mutating writes only.
    if is_mutating_ok_write && !ctx.skip_write_side_effects {
        // Only pay for the payload when something downstream will read it.
        // `replicate_one_write` is a no-op with no replicas and AOF off (see
        // `write_is_observed`), but reaching that no-op used to cost a
        // `Vec<Vec<u8>>` clone of every argument here *plus* a full RESP encode
        // inside — allocations on every single write, discarded immediately.
        if replication::write_is_observed(ctx) {
            // Propagate the command's *effect*, not the bytes the client sent.
            // `SPOP` and `XADD … *` produce a different result each time they
            // run, so a replica re-executing them diverges (measured: a replica
            // kept a different 12 of 20 members; 4 of 5 stream IDs differed).
            // The AOF carries these same bytes, so replay depends on it too.
            //
            // Kept behind `write_is_observed` so an instance with no replicas
            // and AOF off never pays for it. In the observed case the verbatim
            // arm allocates exactly what the previous `ctx.cmd` clone did.
            for parts in
                replication::script_feed::commands_for_propagation(ctx, name, &ctx.cmd, result)
            {
                replication::replicate_one_write(ctx, &parts);
            }
        }

        notify_watchers(ctx, name);

        crate::stats::record_rdb_change();
    }

    // ── PUBLISH reaches replicas' subscribers too ───────────────────────────
    // Not a write — nothing is stored, and it never goes to the AOF — but
    // Redis propagates it so a client subscribed on a replica receives what
    // was published on the master. The byte count is in the offset like any
    // other stream content.
    // In cluster mode the bus forwards it to every node instead, replicas
    // included (see `commands::pubsub`), so it is one or the other.
    if !writes_operation
        && name == "PUBLISH"
        && !ctx.cluster.enabled
        && !ctx.apply_replication
        && !ctx.repl_state.is_slave()
        && !result.starts_with(b"-")
        && ctx.repl_state.connected_replicas.load(Ordering::Relaxed) > 0
    {
        let parts: Vec<Vec<u8>> = ctx.cmd.iter().map(|b| b.to_vec()).collect();
        let payload = crate::resp::write::encode_resp_command(&parts);
        ctx.repl_state
            .repl_offset
            .fetch_add(payload.len() as u64, Ordering::Relaxed);
        replication::propagate(ctx.hub_tx, payload);
    }

    track(ctx, name, is_mutating_ok_write, writes_operation);
}

/// Client-side caching: what a command that has just run means for the
/// clients tracking keys. A write that succeeded invalidates each of its
/// keys; a read by a tracking client is noted, for each of its keys, so that
/// the client is told when one changes.
///
/// One atomic load when no client has tracking on.
#[inline(always)]
fn track(ctx: &Ctx, name: &str, wrote: bool, is_write: bool) {
    if crate::tracking::TRACKING_CLIENT_COUNT.load(std::sync::atomic::Ordering::Relaxed) == 0 {
        return;
    }
    track_for_clients(ctx, name, wrote, is_write);
}

#[cold]
fn track_for_clients(ctx: &Ctx, name: &str, wrote: bool, is_write: bool) {
    // A script is tracked by what it calls, one command at a time. `CLIENT`
    // subcommands are not reads of data, and must not use up the one-shot
    // `CLIENT CACHING`.
    if matches!(
        name,
        "EVAL" | "EVALSHA" | "EVAL_RO" | "EVALSHA_RO" | "FCALL" | "FCALL_RO" | "CLIENT"
    ) {
        return;
    }
    if wrote {
        if matches!(name, "FLUSHALL" | "FLUSHDB") {
            crate::tracking::invalidate_everything(
                ctx.tracking_table,
                ctx.client_registry,
                Some(ctx.client_id),
            );
            return;
        }
        for pos in crate::keyspec::key_positions(name, &ctx.cmd).iter() {
            if let Ok(key) = std::str::from_utf8(ctx.cmd[pos]) {
                invalidate_tracked_key(ctx, key, true);
            }
        }
        return;
    }
    if is_write {
        return;
    }
    let keys = crate::keyspec::key_positions(name, &ctx.cmd);
    if keys.is_empty() {
        return;
    }
    let mut should_register = false;
    ctx.client_registry.update(ctx.client_id, |info| {
        // A `BCAST` client is told by prefix, whatever it has read.
        if !info.tracking || info.tracking_bcast {
            return;
        }
        let caching_next = info.tracking_caching_next.take();
        should_register = if info.tracking_optin {
            caching_next == Some(true)
        } else if info.tracking_optout {
            caching_next != Some(false)
        } else {
            true
        };
    });
    if !should_register {
        return;
    }
    for pos in keys.iter() {
        if let Ok(key) = std::str::from_utf8(ctx.cmd[pos]) {
            let dropped = ctx.tracking_table.register(key, ctx.client_id);
            crate::tracking::tell_dropped(ctx.client_registry, dropped, Some(ctx.client_id));
        }
    }
}

/// Tell the clients caching `key` that it has changed. `by_client` is false
/// when no client's command did it — the key was evicted — and then nobody
/// is spared the message for having asked for NOLOOP.
pub(crate) fn invalidate_tracked_key(ctx: &Ctx, key: &str, by_client: bool) {
    if crate::tracking::TRACKING_CLIENT_COUNT.load(std::sync::atomic::Ordering::Relaxed) == 0 {
        return;
    }
    let invalidate = || {
        crate::tracking::invalidate(
            ctx.tracking_table,
            ctx.client_registry,
            key,
            by_client.then_some(ctx.client_id),
            Some(ctx.client_id),
        );
    };
    if by_client {
        invalidate();
    } else {
        // Evicted to make room for the command that is about to run.
        crate::tracking::before_the_reply(invalidate);
    }
}

#[cfg(test)]
mod replica_loading_gate_tests {
    use super::execute_command;
    use crate::commands::ctx::{Ctx, NoopAofSender};
    use crate::config::Config;
    use crate::replication::ReplState;
    use parking_lot::RwLock;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Instant;

    fn run_as_slave(repl_state: &Arc<ReplState>, cmd: Vec<Vec<u8>>, apply_stream: bool) -> Vec<u8> {
        let store = crate::store::new_store(1);
        let expiry_store = crate::store::new_expiry_store(1);
        let lastsave = Arc::new(RwLock::new(0i64));
        let (hub_tx, mut hub_rx) =
            tokio::sync::mpsc::unbounded_channel::<crate::replication::HubMsg>();
        tokio::spawn(async move { while hub_rx.recv().await.is_some() {} });

        let config = Arc::new(Config::default_replica());
        let started_at = Instant::now();
        let index_store = crate::vector::new_index_store();
        let dict_store = crate::vector::new_dict_store();
        let wait_table = Arc::new(crate::wait_table::WaitTable::new());
        let ts_index = crate::store::new_ts_index();
        let pubsub = crate::pubsub::PubSubHub::new();
        let acl_store = crate::acl::new_acl_store("");
        let acl_log = crate::acl::new_acl_log();
        let slowlog = crate::slowlog::new_slowlog(u64::MAX, 0);
        let latency = crate::latency::new_latency_tracker();
        let (compaction_tx, compaction_rx) = crate::vector::new_compaction_channel();
        drop(compaction_rx);
        let cluster = Arc::new(crate::cluster::ClusterState::new_disabled());
        let client_registry = crate::client_registry::new_client_registry();
        let monitor_registry = crate::monitor::new_monitor_registry();
        let tracking_table = crate::tracking::new_tracking_table();
        let mut selected_db = 0usize;
        let peer = "127.0.0.1:1".to_string();

        execute_command(Ctx {
            cmd: cmd.iter().map(|v| v.as_slice()).collect(),
            store: &store,
            expiry_store: &expiry_store,
            lastsave: &lastsave,
            selected_db: &mut selected_db,
            aof_tx: &NoopAofSender,
            aof_enabled: false,
            config: &config,
            started_at: &started_at,
            index_store: &index_store,
            dict_store: &dict_store,
            wait_table: &wait_table,
            ts_index: &ts_index,
            pubsub: &pubsub,
            hub_tx: &hub_tx,
            repl_state,
            watch_registry: &crate::watch::NoopWatchRegistry,
            client_id: 0,
            client_registry: &client_registry,
            resp: 2,
            acl_store: &acl_store,
            acl_log: &acl_log,
            slowlog: &slowlog,
            latency: &latency,
            compaction_tx: &compaction_tx,
            cluster: &cluster,
            bypass_cluster_check: false,
            apply_replication: apply_stream,
            skip_write_side_effects: false,
            peer_addr: peer.as_str(),
            monitor_registry: &monitor_registry,
            authenticated_user: "default",
            tracking_table: &tracking_table,
            blocking: None,
        })
    }

    /// Regression: full-resync must not serve normal reads from an empty store (LOADING gate).
    #[tokio::test]
    async fn slave_get_returns_loading_while_slave_sync_in_progress() {
        let repl_state = Arc::new(ReplState::new_master());
        repl_state.become_slave();
        repl_state
            .slave_sync_in_progress
            .store(true, Ordering::Relaxed);

        let out = run_as_slave(&repl_state, vec![b"GET".to_vec(), b"x".to_vec()], false);
        assert!(
            String::from_utf8_lossy(&out).starts_with("-LOADING"),
            "got {}",
            String::from_utf8_lossy(&out)
        );
    }

    #[tokio::test]
    async fn slave_ping_allowed_while_slave_sync_in_progress() {
        let repl_state = Arc::new(ReplState::new_master());
        repl_state.become_slave();
        repl_state
            .slave_sync_in_progress
            .store(true, Ordering::Relaxed);

        let out = run_as_slave(&repl_state, vec![b"PING".to_vec()], false);
        assert!(
            String::from_utf8_lossy(&out).starts_with("+PONG")
                || String::from_utf8_lossy(&out).starts_with('$'),
            "expected PONG-compatible reply, got {}",
            String::from_utf8_lossy(&out)
        );
    }

    #[tokio::test]
    async fn apply_replication_bypasses_loading_gate_for_stream_applier() {
        let repl_state = Arc::new(ReplState::new_master());
        repl_state.become_slave();
        repl_state
            .slave_sync_in_progress
            .store(true, Ordering::Relaxed);

        let out = run_as_slave(
            &repl_state,
            vec![b"SET".to_vec(), b"stream-key".to_vec(), b"1".to_vec()],
            true,
        );
        assert!(
            !String::from_utf8_lossy(&out).starts_with("-LOADING"),
            "applier must not hit LOADING gate, got {}",
            String::from_utf8_lossy(&out)
        );
    }
}
