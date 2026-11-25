// src/commands/handler.rs

use crate::commands::*;
use crate::replication;
use crate::resp::write::{resp_array, resp_error, resp_integer};
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
        .or_else(|| bf::handle(ctx, name))
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
        b'b' => bitmap::handle(ctx, name).or_else(|| bf::handle(ctx, name)),
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

/// The values a command may replace whole (Redis's `setKey`), taken before
/// it runs, to announce `overwritten` and `type_changed` for those it did.
struct ReplaceWatch {
    /// A hold on each key's entry as it was: an entry is never changed
    /// while held, so the key's value is the same one exactly when the key
    /// still has this entry.
    before: Vec<(usize, usize, crate::store::EntryRef)>,
    /// The command's own events are held back, so that these go first.
    holding: bool,
}

#[cold]
#[inline(never)]
fn replace_watch_begin(ctx: &Ctx, name: &str) -> Option<ReplaceWatch> {
    let before: Vec<_> = crate::keyspace_events::whole_value_targets(name, &ctx.cmd)
        .into_iter()
        .filter_map(|(db, pos)| {
            let db = db.unwrap_or(*ctx.selected_db);
            let key = ctx.cmd[pos];
            let held =
                crate::read_lock!(crate::store::get_shard(ctx.store.get(db)?, key)).get_ref(key)?;
            Some((db, pos, held))
        })
        .collect();
    if before.is_empty() {
        return None;
    }
    let holding = crate::keyspace_events::overwritten_goes_first(name);
    if holding {
        crate::keyspace_events::hold();
    }
    Some(ReplaceWatch { before, holding })
}

#[cold]
#[inline(never)]
fn replace_watch_end(ctx: &Ctx, watch: ReplaceWatch) {
    let held = if watch.holding {
        crate::keyspace_events::release()
    } else {
        Vec::new()
    };
    for (db, pos, old) in watch.before {
        let key = ctx.cmd[pos];
        // Whether the key has another value now, and whether of another type.
        let replaced = {
            let map = crate::read_lock!(crate::store::get_shard(&ctx.store[db], key));
            match map.get(key) {
                Some(now) if !map.holds(key, &old) => {
                    Some(now.type_name() != old.value().type_name())
                }
                _ => None,
            }
        };
        if let Some(type_changed) = replaced {
            crate::keyspace_events::notify(
                ctx.pubsub,
                db,
                crate::keyspace_events::KS_OVERWRITTEN,
                "overwritten",
                key,
            );
            if type_changed {
                crate::keyspace_events::notify(
                    ctx.pubsub,
                    db,
                    crate::keyspace_events::KS_TYPE_CHANGED,
                    "type_changed",
                    key,
                );
            }
        }
    }
    crate::keyspace_events::replay(ctx.pubsub, held);
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
    if not_allowed_from_script(name, parts.get(1).copied()) {
        return resp_error("ERR This Redis command is not allowed from script");
    }

    // A script never waits. `BLPOP`, `BZPOPMIN`, `BLMOVE` and the other
    // blocking pops run as their non-blocking form, as in Redis, where a
    // script's client is `CLIENT_DENY_BLOCKING`: the element if there is
    // one, their empty reply if not. The `Ctx` a script runs commands with
    // has `blocking: None`, which is what makes them so
    // (`blocking::wait_for_keys`). They used to answer nil here without
    // running, whatever the key held (#558).
    // `WAIT` → 0 and `WAITAOF` → {0 0}, as in Redis.
    match name {
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

/// Whether a script may not run the command `name` (in capitals), whose
/// first argument is `sub`: the commands and subcommands flagged `NOSCRIPT`
/// in Valkey 9.1.3's command table (BSD-3-Clause, the same source as
/// COMMAND DOCS), which Redis refuses the same way, and KiviDB's `BACKUP`
/// and `HOTKEYS` subcommands that start or stop something, refused before
/// anything else is looked at. They change the connection (`MULTI`, `SUBSCRIBE`, `HELLO`, `CLIENT
/// SETNAME`, ...), the server (`CONFIG SET`, `ACL SETUSER`, `REPLICAOF`,
/// `SHUTDOWN`, ...), or wait for what waits for the script: a script holds a
/// slot of the write gate, and a `SAVE` in one, on a replica, waited for the
/// apply lock, whose holder waited for a backup's or a full sync's cut,
/// which waited for the gate.
///
/// One difference from Redis: of `CLUSTER`, a script may run only what
/// reads (`INFO`, `NODES`, `SLOTS`, `KEYSLOT`, ...). The subcommands that
/// change the cluster, which Redis flags `admin` but lets a script run,
/// are refused too: a slot's handoff here waits for the writes in flight,
/// and the script is one of them.
fn not_allowed_from_script(name: &str, sub: Option<&[u8]>) -> bool {
    let sub_is = |names: &[&str]| {
        sub.is_some_and(|sub| names.iter().any(|n| sub.eq_ignore_ascii_case(n.as_bytes())))
    };
    match name {
        "AUTH" | "BGREWRITEAOF" | "BGSAVE" | "DEBUG" | "DISCARD" | "EVAL" | "EVALSHA"
        | "EVALSHA_RO" | "EVAL_RO" | "EXEC" | "FAILOVER" | "FCALL" | "FCALL_RO" | "HELLO"
        | "MONITOR" | "MULTI" | "PSUBSCRIBE" | "PSYNC" | "PUNSUBSCRIBE" | "QUIT" | "REPLCONF"
        | "REPLICAOF" | "RESET" | "ROLE" | "SAVE" | "SHUTDOWN" | "SLAVEOF" | "SSUBSCRIBE"
        | "SUBSCRIBE" | "SUNSUBSCRIBE" | "SYNC" | "UNSUBSCRIBE" | "UNWATCH" | "WATCH" => true,
        "ACL" => sub_is(&[
            "CAT", "DELUSER", "DRYRUN", "GENPASS", "GETUSER", "LIST", "LOAD", "LOG", "SAVE",
            "SETUSER", "USERS", "WHOAMI",
        ]),
        "BACKUP" => sub_is(&["ABORT", "CLEANUP", "SEAL", "START"]),
        "CLIENT" => sub_is(&[
            "CACHING",
            "GETNAME",
            "GETREDIR",
            "ID",
            "INFO",
            "KILL",
            "LIST",
            "NO-EVICT",
            "NO-TOUCH",
            "PAUSE",
            "REPLY",
            "SETINFO",
            "SETNAME",
            "TRACKING",
            "TRACKINGINFO",
            "UNBLOCK",
            "UNPAUSE",
        ]),
        "CLUSTER" => !sub_is(&[
            "COUNTKEYSINSLOT",
            "GETKEYSINSLOT",
            "HELP",
            "INFO",
            "KEYSLOT",
            "LINKS",
            "MYID",
            "MYSHARDID",
            "NODES",
            "SHARDS",
            "SLOT-STATS",
            "SLOTS",
        ]),
        "CONFIG" => sub_is(&["GET", "RESETSTAT", "REWRITE", "SET"]),
        "FUNCTION" => sub_is(&[
            "DELETE", "DUMP", "FLUSH", "KILL", "LIST", "LOAD", "RESTORE", "STATS",
        ]),
        "HOTKEYS" => sub_is(&["GET", "RESET", "START", "STOP"]),
        "LATENCY" => sub_is(&["DOCTOR", "GRAPH", "HISTOGRAM", "HISTORY", "LATEST", "RESET"]),
        "MODULE" => sub_is(&["LIST", "LOAD", "LOADEX", "UNLOAD"]),
        "SCRIPT" => sub_is(&["DEBUG", "EXISTS", "FLUSH", "KILL", "LOAD"]),
        _ => false,
    }
}

/// The `BUSY` reply to a command refused while a script runs: it names
/// `FUNCTION KILL` while a function runs, as Redis's does (#562).
fn busy_reply() -> Vec<u8> {
    #[cfg(feature = "lua")]
    {
        crate::commands::lua::busy_reply()
    }
    #[cfg(not(feature = "lua"))]
    {
        resp_error(
            "BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.",
        )
    }
}

/// The rest of [`execute_command_from_script`], with the command in `ctx.cmd`.
/// Why a script on a cluster node may not run `ctx.cmd`, or `None` when it
/// may: as in Redis, its keys must be ones this node serves, and, unless the
/// script has `allow-cross-slot-keys`, in the script's slot.
///
/// The script's own slot, its first key's, was routed with the command that
/// runs it, ASKING and slots being imported included: a key in it is not
/// routed again while the slot stays where it is. While keys move, its
/// command on a key it did not declare is (see [`script_refusal_while_moving`]);
/// on its declared keys, never.
#[cfg(feature = "lua")]
fn script_key_refusal(ctx: &Ctx, name: &str) -> Option<Vec<u8>> {
    let (script_slot, allow_cross_slot) = crate::commands::lua::script_slot()?;
    let keys = crate::keyspec::key_positions(name, &ctx.cmd);
    let mut slots = keys
        .iter()
        .map(|pos| crate::cluster::slot::key_hash_slot(ctx.cmd[pos]));
    let first = slots.next()?;
    if slots.any(|slot| slot != first) {
        return Some(resp_error(
            "ERR Script attempted to access keys that do not hash to the same slot",
        ));
    }
    if script_slot == Some(first) {
        // Two atomic loads while no key or slot moves.
        if crate::pause::keys_moving() || ctx.cluster.slots_open() {
            return script_refusal_while_moving(ctx, name, first);
        }
        return None;
    }
    if script_slot.is_some() && !allow_cross_slot {
        return Some(resp_error(
            "ERR Script attempted to access keys that do not hash to the same slot",
        ));
    }
    if crate::pause::keys_moving()
        && let Some(refusal) = script_write_to_a_moving_key(ctx, name)
    {
        return Some(refusal);
    }
    let store = ctx.store;
    let db = *ctx.selected_db;
    let registry = ctx.client_registry;
    let client_id = ctx.client_id;
    let decision = crate::cluster::routing::route_named(
        name,
        ctx.cmd.as_slice(),
        ctx.cluster,
        false,
        || registry.get_readonly(client_id),
        |key| {
            let shard = crate::store::get_shard(&store[db], key);
            crate::read_lock!(shard).contains_key(key)
        },
    );
    match decision {
        crate::cluster::routing::RouteDecision::OwnedSlot(slot) => {
            crate::commands::lua::script_settle_slot(slot);
            None
        }
        crate::cluster::routing::RouteDecision::TryAgain => decision.refusal(false),
        crate::cluster::routing::RouteDecision::Passthrough => None,
        _ => Some(script_route_refusal(&decision)),
    }
}

/// The error a script's command gets when routing it would not run it here
/// (see [`script_key_refusal`]): Redis's for a cluster that is down
/// (`scriptVerifyClusterState`), the client's own `CLUSTERDOWN` for a slot
/// no node serves, and Redis's "non local key" for a key that is served
/// elsewhere, or not all here (`-ASK`, `-MOVED`, `-TRYAGAIN`).
#[cfg(feature = "lua")]
#[cold]
fn script_route_refusal(decision: &crate::cluster::routing::RouteDecision) -> Vec<u8> {
    use crate::cluster::routing::RouteDecision;
    match decision {
        RouteDecision::ClusterDown => {
            resp_error("ERR Script attempted to execute a command while the cluster is down")
        }
        RouteDecision::Unassigned { slot } => crate::cluster::routing::clusterdown_error(*slot),
        _ => resp_error("ERR Script attempted to access a non local key in a cluster node"),
    }
}

/// The keys `ctx.cmd` (a script's command) names that are not among the
/// script's `KEYS`.
#[cfg(feature = "lua")]
fn undeclared_keys<'a>(ctx: &Ctx<'a>, name: &str) -> Vec<&'a [u8]> {
    crate::keyspec::key_positions(name, &ctx.cmd)
        .iter()
        .map(|pos| ctx.cmd[pos])
        .filter(|key| !crate::commands::lua::script_declares(key))
        .collect()
}

/// A script's write to a key it did not declare that a `MIGRATE` is moving
/// (`key_moves`): refused with `TRYAGAIN`. A client's write to it waits in
/// the write gate until the key has moved (`checked_in_gate`); a script's,
/// in the gate already with the whole script, cannot wait there, and runs
/// beside the `MIGRATE`, which skips the command gate: run, it would change
/// the key after it was sent. The script's declared keys were waited for
/// before it ran (a `MIGRATE` that marks one after that waits for the
/// script in its cut, and has sent nothing), and are not looked at.
#[cfg(feature = "lua")]
#[cold]
#[inline(never)]
fn script_write_to_a_moving_key(ctx: &Ctx, name: &str) -> Option<Vec<u8>> {
    if !replication::command_mutates_for_replication(name, &ctx.cmd) {
        return None;
    }
    let keys = undeclared_keys(ctx, name);
    crate::key_moves::one_is_moving(*ctx.selected_db, &keys)
        .then(|| resp_error("TRYAGAIN a key of the script's command is being migrated"))
}

/// [`script_key_refusal`] for a command in the script's own slot, `slot`,
/// while keys or slots move here: its keys that the script did not declare
/// are looked at as a client's command's would be.
/// - A write to one that a `MIGRATE` is moving: `TRYAGAIN`
///   ([`script_write_to_a_moving_key`]).
/// - With the slot being migrated away or imported here, the command is
///   routed, as Redis routes every command of a script
///   (`scriptVerifyClusterState`), and refused with Redis's error if it
///   would be sent elsewhere (`-ASK`, `-MOVED`) or asked to try again: on
///   a node migrating the slot, a key that has left already, or one never
///   made, is the destination's; on the node importing it, a command over
///   several keys some of which have not arrived yet is not run. Its
///   declared keys count as here: they were routed with the script, which
///   may delete one and make it again.
///
/// The script was routed with `ASKING` if, and only if, its slot is being
/// imported here: without it, the script was sent to the slot's owner (and
/// a slot this node owns is not imported). So `ASKING` is as it was then.
/// A script refused part way through has run the commands before: as in
/// Redis, they stay.
#[cfg(feature = "lua")]
#[cold]
#[inline(never)]
fn script_refusal_while_moving(ctx: &Ctx, name: &str, slot: u16) -> Option<Vec<u8>> {
    use crate::cluster::MigrationState;
    use crate::cluster::routing::RouteDecision;
    if crate::pause::keys_moving()
        && let Some(refusal) = script_write_to_a_moving_key(ctx, name)
    {
        return Some(refusal);
    }
    let asking = match ctx.cluster.migration_state(slot)? {
        MigrationState::Importing { .. } => true,
        MigrationState::Migrating { .. } => false,
    };
    if undeclared_keys(ctx, name).is_empty() {
        return None;
    }
    let store = ctx.store;
    let db = *ctx.selected_db;
    let registry = ctx.client_registry;
    let client_id = ctx.client_id;
    let decision = crate::cluster::routing::route_named(
        name,
        ctx.cmd.as_slice(),
        ctx.cluster,
        asking,
        || registry.get_readonly(client_id),
        |key| {
            crate::commands::lua::script_declares(key) || {
                let shard = crate::store::get_shard(&store[db], key);
                crate::read_lock!(shard).contains_key(key)
            }
        },
    );
    match decision {
        RouteDecision::OwnedSlot(_) | RouteDecision::Passthrough => None,
        _ => Some(script_route_refusal(&decision)),
    }
}

/// Whether `cmd` is a write naming more than one key, which would be seen
/// half done by another client without the gate alone: a `DEL` of several
/// keys, `RENAME`, `SUNIONSTORE` and the other `*STORE` commands, `SORT ...
/// STORE`, `LMOVE`. In Redis every command is one step; here such a write
/// takes and lets go of its keys' shard locks one key at a time, so even
/// keys of one shard are seen half written (#159): taken alone only for
/// keys of several shards, a `DEL` of keys that fell in one shard was seen
/// by a reader with some of them gone and others not. `MSET` and `MSETNX`
/// lock all their keys together, and need no more.
#[inline(never)]
fn writes_several_keys(name: &str, cmd: &[&[u8]]) -> bool {
    // `key_positions` is a long match on the name: the writes that name one
    // key are told apart first.
    if cmd.len() < 3
        || matches!(name, "MSET" | "MSETNX")
        || one_key_write(name)
        // By the arguments: `GEORADIUS ... STORE` writes. (`MIGRATE` skips
        // the gate before this is asked: `command_gate::skips`.)
        || !replication::command_mutates_for_replication(name, cmd)
    {
        return false;
    }
    let keys = crate::keyspec::key_positions(name, cmd);
    let mut keys = keys.iter().map(|pos| cmd[pos]);
    let Some(first) = keys.next() else {
        return false;
    };
    keys.any(|key| key != first)
}

/// Whether a command takes the command gate alone: the one rule, asked by
/// [`run_command`] for a command from a client and by the replica's applier
/// for one from the master's stream (see [`applied_takes_the_gate_alone`]).
/// `script_alone`: a script that is not let run beside others by its tag.
/// `gated`: a write, or a script. A flush and `SWAPDB` are not taken alone
/// while the AOF is replayed, where nothing else runs.
#[inline(always)]
fn takes_the_gate_alone(name: &str, cmd: &[&[u8]], script_alone: bool, gated: bool) -> bool {
    gate_alone_rule(name, cmd, script_alone, gated, true)
}

/// [`takes_the_gate_alone`], with or without its look at the commands that
/// read keys they do not name (`readers`): `gate_alone_or_walk` asks the
/// rest of it alone.
#[inline(always)]
fn gate_alone_rule(
    name: &str,
    cmd: &[&[u8]],
    script_alone: bool,
    gated: bool,
    readers: bool,
) -> bool {
    script_alone
        || (gated
            && matches!(name, "FLUSHALL" | "FLUSHDB" | "SWAPDB")
            && !crate::expiry::replaying())
        // A command that reads or changes keys it does not name, with tag
        // locks on: alone, or, with Lua, the readers beside every command but
        // the scripts that run beside others (`gate_alone_or_walk`, #772).
        || (readers
            && crate::tag_locks::enabled()
            && crate::tag_locks::reads_keys_it_does_not_name(name))
        || crate::tag_locks::is_switch(name, cmd)
        // One argument is at most one key (`DEL k`, `INCR k`): asked here,
        // and the rest out of line, so that what every command runs through
        // stays small.
        || (cmd.len() >= 3 && writes_several_keys(name, cmd))
        // A `SORT` that reads fields of hashes it does not name, which may
        // remove one past its time (`generics::sort_reads_hash_fields`).
        // Not `SORT_RO`: it is no write, and takes the order of what it
        // removes as a read does.
        || (name == "SORT" && sort_reads_hash_fields(cmd))
}

/// [`takes_the_gate_alone`]'s question for `SORT`, out of line.
#[inline(never)]
fn sort_reads_hash_fields(cmd: &[&[u8]]) -> bool {
    crate::commands::generics::sort_reads_hash_fields(cmd)
}

/// The writes of one key, and so of one shard, made most: told apart by
/// name before `key_positions`, a long match, is asked for their keys.
#[inline]
fn one_key_write(name: &str) -> bool {
    matches!(
        name,
        "SET"
            | "SETEX"
            | "PSETEX"
            | "SETNX"
            | "GETSET"
            | "GETDEL"
            | "APPEND"
            | "INCR"
            | "DECR"
            | "INCRBY"
            | "DECRBY"
            | "INCRBYFLOAT"
            | "HSET"
            | "HMSET"
            | "HSETNX"
            | "HDEL"
            | "HINCRBY"
            | "HINCRBYFLOAT"
            | "LPUSH"
            | "RPUSH"
            | "LPOP"
            | "RPOP"
            | "SADD"
            | "SREM"
            | "ZADD"
            | "ZREM"
            | "ZINCRBY"
            | "XADD"
            | "PFADD"
            | "PEXPIREAT"
            | "HPEXPIREAT"
            | "PERSIST"
    )
}

/// Whether `cmd`, a command applied from the master's stream to database
/// `db`, takes the command gate alone ([`takes_the_gate_alone`]). The
/// replica's applier takes the gate first, before its own lock, as it does
/// for a transaction. Taken after that lock, the gate waited for a `SAVE`
/// that held it shared and was waiting for the lock: the replica answered
/// nothing and applied nothing more (#373). A script from the stream never
/// runs beside others. `run_command` then finds the gate held on its thread
/// and takes nothing of it.
pub(crate) fn applied_takes_the_gate_alone(cmd: &[&[u8]]) -> bool {
    let Some(&cmd0) = cmd.first() else {
        return false;
    };
    let mut upper = [0u8; 32];
    let owned;
    let name: &str = if cmd0.len() <= upper.len() {
        for (to, from) in upper.iter_mut().zip(cmd0) {
            *to = from.to_ascii_uppercase();
        }
        std::str::from_utf8(&upper[..cmd0.len()]).unwrap_or("")
    } else {
        owned = String::from_utf8_lossy(cmd0).to_ascii_uppercase();
        &owned
    };
    if one_key_write(name) || crate::command_gate::skips(name, cmd) {
        return false;
    }
    // A flush and `SWAPDB` take it alone from the stream too, as from a
    // client: a query on the replica beside one found a database half
    // flushed, its keys gone and its indexes not yet, or half swapped (#378).
    let script = matches!(
        name,
        "EVAL" | "EVALSHA" | "FCALL" | "EVAL_RO" | "EVALSHA_RO" | "FCALL_RO"
    );
    takes_the_gate_alone(name, cmd, script, true)
}

/// The order of the keys of an `MSET` or `MSETNX`, held shared (see
/// `write_order::lock_keys_shared`). Out of line: not on the way of the
/// one-key writes, which go through the same `run_command`.
#[inline(never)]
fn order_of_pairs_shared(ctx: &Ctx) -> crate::write_order::OrderGuard {
    crate::write_order::lock_keys_shared(*ctx.selected_db, ctx.cmd[1..].iter().step_by(2).copied())
}

/// How a script runs (`tag_locks`): the tags of its `KEYS`, if every one
/// carries a tag, which it is held to; and whether it runs beside others,
/// under those tags' locks, which it does once its caller has seen it to be
/// quick
/// (`tag_locks::allow_next_script`) — not inside a transaction or another
/// script, which hold the gate alone already. `applied`: it comes from the
/// master's stream.
///
/// Counted for `INFO` here, where it is decided (`stats::scripts`), unless it
/// is applied from a master or replayed from the AOF. Only a script comes
/// here: nothing is added to any other command's way.
#[inline]
fn script_mode<'a>(
    cmd: &[&'a [u8]],
    name: &str,
    tag_locks: bool,
    applied: bool,
) -> (Option<crate::tag_locks::ScriptTags<'a>>, bool) {
    let allowed_beside = crate::tag_locks::take_next_script_allowed();
    let script_tags = if tag_locks {
        crate::tag_locks::script_tags(cmd)
    } else {
        None
    };
    let in_another = crate::command_gate::alone_on_this_thread();
    let beside_others = script_tags.is_some() && allowed_beside && !in_another;
    #[cfg(feature = "lua")]
    if !applied {
        if beside_others {
            crate::stats::scripts::ran_beside_others();
        } else {
            count_script_alone(cmd, name, tag_locks, in_another);
        }
    }
    #[cfg(not(feature = "lua"))]
    let _ = (name, applied);
    (script_tags, beside_others)
}

/// Why a script runs alone, counted (`stats::scripts::Alone`): out of line,
/// off the way of the scripts that run beside others.
#[cfg(feature = "lua")]
#[cold]
#[inline(never)]
fn count_script_alone(cmd: &[&[u8]], name: &str, tag_locks: bool, in_another: bool) {
    use crate::stats::scripts::Alone;
    use crate::tag_locks::ScriptKeys;
    if crate::expiry::replaying() {
        return;
    }
    let why = if !tag_locks {
        Alone::ParallelOff
    } else if in_another {
        Alone::InTransaction
    } else if name.starts_with("FCALL") {
        Alone::Function
    } else {
        match crate::tag_locks::script_keys(cmd) {
            ScriptKeys::None => Alone::NoKeys,
            ScriptKeys::Untagged => Alone::NoTag,
            ScriptKeys::Tagged => Alone::NotQuick,
        }
    };
    crate::stats::scripts::ran_alone(why);
}

/// For a script held to a hash tag: the refusal of a `SORT` or `SORT_RO`
/// whose `BY` or `GET` pattern can name keys outside the tag
/// (`generics::sort_pattern_outside`). Asked before the command runs, so
/// nothing is read, and no field removed, before it is refused.
#[inline(never)]
fn sort_pattern_outside_the_tag(name: &str, cmd: &[&[u8]]) -> Option<String> {
    if !matches!(name, "SORT" | "SORT_RO") {
        return None;
    }
    let pattern =
        crate::commands::generics::sort_pattern_outside(cmd, crate::tag_locks::script_may_touch)?;
    Some(format!(
        "ERR Script attempted to run '{}' with the pattern '{}', which looks at keys it does \
         not name outside the {} of its KEYS (lua-parallel-scripts)",
        name.to_ascii_lowercase(),
        String::from_utf8_lossy(pattern),
        crate::tag_locks::script_tags_for_message()
    ))
}

/// For a command a script held to its tags calls, of those that read or
/// change keys they do not name (`tag_locks::reads_keys_it_does_not_name`):
/// whether it is refused (see `run_from_script`). Those of
/// `sees_the_whole_keyspace` always; the others when the script runs beside
/// others. `SORT` by a pattern is asked of `sort_pattern_outside_the_tag`:
/// one within the script's own tag reads keys only it holds. Out of line:
/// most commands a script calls are none of these.
#[cold]
#[inline(never)]
fn refused_for_unnamed_keys(name: &str, cmd: &[&[u8]]) -> bool {
    if crate::tag_locks::sees_the_whole_keyspace(name) {
        return true;
    }
    #[cfg(feature = "lua")]
    {
        crate::tag_locks::beside_others()
            && (crate::tag_locks::walks(name, cmd)
                || crate::tag_locks::changes_keys_it_does_not_name(name, cmd))
    }
    #[cfg(not(feature = "lua"))]
    {
        let _ = cmd;
        false
    }
}

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

    // A cluster node has one keyspace, for a script as for a client: as in
    // Redis, the `SELECT` command itself refuses (#428).
    if name2 == "SELECT"
        && ctx.cluster.enabled
        && !ctx.apply_replication
        && ctx.cmd.get(1).is_some_and(|db| *db != &b"0"[..])
    {
        return resp_error("ERR SELECT is not allowed in cluster mode");
    }

    // A script held to the tags of its KEYS keeps to them (`tag_locks`).
    // One that runs beside others does not call what reads or changes keys
    // it does not name either (`INFO keyspace`, `FT.SEARCH`, `TS.MGET`, ...):
    // such a command, sent by a client, keeps out the scripts running beside
    // others while it runs (`tag_locks::walk`), or waits for every script;
    // called from one of them, it could do neither. A script that runs
    // alone has the server to itself, and may.
    if crate::tag_locks::in_script() {
        let outside = if crate::tag_locks::reads_keys_it_does_not_name(name2)
            && refused_for_unnamed_keys(name2, &ctx.cmd)
        {
            Some(format!(
                "ERR Script attempted to run '{}', which looks at keys it does not name, while \
                 running under the {} of its KEYS (lua-parallel-scripts)",
                name2.to_ascii_lowercase(),
                crate::tag_locks::script_tags_for_message()
            ))
        } else {
            crate::keyspec::key_positions(name2, &ctx.cmd)
                .iter()
                .map(|pos| ctx.cmd[pos])
                .find(|key| !crate::tag_locks::script_may_touch(key))
                .map(|key| {
                    format!(
                        "ERR Script attempted to access the key '{}', which is outside the {} \
                         of its KEYS (lua-parallel-scripts)",
                        String::from_utf8_lossy(key),
                        crate::tag_locks::script_tags_for_message()
                    )
                })
                .or_else(|| sort_pattern_outside_the_tag(name2, &ctx.cmd))
        };
        if let Some(outside) = outside {
            return resp_error(&outside);
        }
        // Nor may it be the first to select a database other than 0. That
        // switches every write to carrying its own `SELECT`, and has to wait
        // out the writes on their way without one first: a cut, which a
        // script cannot take beside others, in the write gate. Without it, a
        // database-0 write could be sent after the script's `SELECT`, and be
        // applied in that database by a replica and a replay of the AOF
        // (#423). A client's `SELECT` makes the switch, once, before its gate.
        if name2 == "SELECT"
            && crate::tag_locks::beside_others()
            && let Some(db) = makes_the_switch(ctx)
        {
            return resp_error(&format!(
                "ERR Script attempted to SELECT {db}, the first database other than 0 \
                 selected on this server, while running beside other scripts under the {} \
                 of its KEYS (lua-parallel-scripts)",
                crate::tag_locks::script_tags_for_message()
            ));
        }
        // Nor may it `MIGRATE` (#340). A `MIGRATE` holds back writes to the
        // keys it moves until they have gone (`cluster::hold_while_moving`),
        // and the hold enters the write gate and takes the keys' order,
        // which a script beside others holds already, under its tags' locks:
        // taken there, it would be the other way round from every writer.
        // So such a script is refused. A script that runs alone holds
        // every other command back, and keeps its hold.
        if name2 == "MIGRATE" && crate::tag_locks::beside_others() {
            return resp_error(&format!(
                "ERR Script attempted to run 'migrate' while running beside other scripts \
                 under the {} of its KEYS (lua-parallel-scripts)",
                crate::tag_locks::script_tags_for_message()
            ));
        }
    }

    #[cfg(debug_assertions)]
    crate::pause::test_stall_in_script(ctx.cmd.as_slice());

    if read_only_script && replication::inner_command_blocked_in_readonly_script(name2, &ctx.cmd) {
        return resp_error("ERR Write commands are not allowed from read-only scripts.");
    }

    #[cfg(feature = "lua")]
    if ctx.cluster.enabled
        && !ctx.bypass_cluster_check
        && !ctx.apply_replication
        && let Some(refusal) = script_key_refusal(ctx, name2)
    {
        return refusal;
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

    // Whether the command writes, judged by its name and arguments, which it
    // does not change: asked once here, it was asked five times a command
    // (#901). A command that writes only with some arguments (GEORADIUS ...
    // STORE, MIGRATE, ...) counts as a write (`command_mutates_for_replication`).
    let inner_write = replication::command_mutates_for_replication(name2, ctx.cmd.as_slice());
    // A `SELECT` is an inner write but changes no data (`writes_the_dataset`).
    #[cfg_attr(not(feature = "lua"), allow(unused_variables))]
    let writes_data = inner_write && replication::writes_the_dataset(name2, ctx.cmd.as_slice());

    // Room for a command that allocates as much as its arguments say, as
    // for one from a client, where that can be made (`room_in_script`):
    // before the refusal of a write over the limit, which it may make room
    // for.
    if inner_write
        && !repl_is_slave
        && !ctx.apply_replication
        && !skip_mem
        && crate::RUNTIME_MAXMEMORY.load(std::sync::atomic::Ordering::Relaxed) != 0
        && let Some(refusal) = room_in_script(ctx, name2)
    {
        return refusal;
    }

    if !repl_is_slave
        && check_memory_limit()
        && inner_write
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
            let _lua = crate::acl::log_context("lua");
            ctx.acl_log.record(
                denied.reason(),
                denied.object(),
                ctx.authenticated_user,
                &crate::acl::log_client_info(ctx.client_registry, ctx.client_id, ctx.peer_addr),
            );
            let reply = resp_error(&denied.message(ctx.authenticated_user));
            return reply;
        }
    }

    // What a script calls is shown to MONITOR as called by `lua`.
    crate::monitor::feed(ctx.monitor_registry, &ctx.cmd, *ctx.selected_db, "lua");

    // Unknown-command message must match Redis's wording (Tcl tests use glob `*Unknown Redis*`).
    let started = std::time::Instant::now();
    // A write takes the keys it names that are past their time as gone,
    // and they go first, as from a client (`expire_named_keys_first`).
    if inner_write {
        expire_named_keys_first(ctx, name2, None);
    }
    // What the command records of its effects is read below whether or not
    // the write is observed (`script_feed::effects_begin_if_read`).
    let script_call = replication::script_feed::ScriptCall::enter();
    let result = dispatch_command_handlers(ctx, name2)
        .unwrap_or_else(|| resp_error("ERR Unknown Redis command called from script"));
    drop(script_call);
    // Counted for `INFO commandstats`, like a command a client sends.
    crate::cmdstats::record(name2, &ctx.cmd, &result, started.elapsed());

    // A command that writes only with some arguments (GEORADIUS ... STORE,
    // MIGRATE, ...) must count as a write here too, or a key it emptied is
    // not swept, a client blocked on a key it wrote is not woken, and a
    // client tracking that key is not told (`inner_write`, judged by the
    // arguments, as the propagation below judges it). A `SELECT` changes no
    // data: a script that has only selected a database can still be killed
    // (#424).
    #[cfg(feature = "lua")]
    if !result.starts_with(b"-") && writes_data {
        crate::commands::lua::script_note_successful_write();
    }

    // A write from a script leaves the same things to be done as one from a
    // client: a key it emptied goes, and clients blocked on a key it wrote
    // are woken.
    if !result.starts_with(b"-") && inner_write {
        settle_written_keys(ctx, name2);
    }
    // And clients caching a key it wrote are told; a key it read is one its
    // caller has read.
    {
        track(
            ctx,
            name2,
            &result,
            inner_write && !result.starts_with(b"-"),
            inner_write,
        );
    }

    // A write that changed nothing — a pop that took nothing, a `PERSIST`
    // that answered 0 — is not sent, not counted, and a `WATCH` on its keys
    // holds (`changed_nothing`).
    #[cfg(feature = "lua")]
    if !result.starts_with(b"-")
        && !repl_is_slave
        && !ctx.apply_replication
        && !propagates_nothing(name2, ctx.cmd.as_slice(), &result)
        // Not a `SELECT`: a write carries the one it needs, from the database
        // it ran in, and a script that only selected one sent nothing in
        // Redis, whose offset it must not move (#705).
        && writes_data
    {
        // Same guard as `apply_write_side_effects`. `record_rdb_change` still runs
        // per feed regardless — it tracks dirty-key count for RDB save, which is
        // independent of whether anything consumes the replication stream.
        // With nothing to read them, the feeds are only counted, not built.
        if replication::write_is_observed(ctx) {
            let feeds = replication::script_feed::commands_for_propagation(
                ctx,
                name2,
                ctx.cmd.as_slice(),
                &result,
            );
            for parts in feeds {
                replication::replicate_one_write(ctx, &parts);
                crate::stats::record_rdb_change();
            }
        } else {
            let feeds = replication::script_feed::propagation_count(
                ctx,
                name2,
                ctx.cmd.as_slice(),
                &result,
            );
            for _ in 0..feeds {
                crate::stats::record_rdb_change();
            }
        }
        // A write made from a script dirties a WATCH like any other write,
        // on the keys it changed.
        notify_watchers(ctx, name2, &result);
    }
    // What this command said it left unchanged is for this command only:
    // the next one a script calls may change the same key.
    crate::watch::clear_unchanged();

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
    ctx.resp & crate::commands::ctx::RESP_VERSION
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
    let mut reply = run_command(ctx, name, started);
    if sent_back_to_wait(&reply) {
        reply = dispatched_again_after_waiting(ctx, name, started, reply);
    }
    if crate::pause::is_held(&reply) {
        // Not run: a `CLIENT PAUSE` sent it back to be held, and it is
        // dispatched again (`blocking::hold_for_pause`). Counted and timed
        // then.
        crate::stats::unrecord_command_processed();
        return reply;
    }
    let elapsed = started.elapsed();
    // For the connection loop's slow log: see `cmdstats::last_command_us`.
    crate::cmdstats::note_last_command(elapsed);
    crate::latency::note_command(ctx.latency, name, elapsed);
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

/// Commands that only ask about keys: no use of a key, as in Redis, and
/// nothing they look up has its use written. `RESTORE` writes its own, with
/// what `IDLETIME` or `FREQ` said. Those that pick keys by what is in them
/// rather than by name (the query engine's, the time series' filters) name
/// no key, and their reads are no use of one either.
#[inline]
fn asks_only(name: &str) -> bool {
    matches!(
        name,
        "OBJECT"
            | "TYPE"
            | "TTL"
            | "PTTL"
            | "EXPIRETIME"
            | "PEXPIRETIME"
            | "EXISTS"
            | "DEBUG"
            | "RESTORE"
            | "TS.MGET"
            | "TS.MRANGE"
            | "TS.MREVRANGE"
            | "TS.QUERYINDEX"
    ) || name.starts_with("FT.")
}

/// A script: its declared keys have their use written after it, and
/// nothing its calls look up does (see `eviction`).
#[inline]
fn is_script(name: &str) -> bool {
    matches!(
        name,
        "EVAL" | "EVALSHA" | "EVAL_RO" | "EVALSHA_RO" | "FCALL" | "FCALL_RO"
    )
}

/// Whether a `CLIENT NO-TOUCH` client's command `name` records no use of
/// its keys: a read does not; `TOUCH`, a write and a script that may write
/// do (#566). A `SORT` without `STORE` is a read here, as it is in Redis,
/// though it counts as a write elsewhere. Out of line and cold: only such
/// a client's commands, under a policy that records uses, ask, and the test
/// on the way here is one byte.
#[cold]
#[inline(never)]
fn no_touch_skips(name: &str, cmd: &[&[u8]], writes_operation: bool) -> bool {
    let reads = !writes_operation
        || (name == "SORT"
            && !cmd
                .iter()
                .skip(2)
                .any(|arg| arg.eq_ignore_ascii_case(b"STORE")));
    reads && name != "TOUCH" && (!is_script(name) || name.ends_with("_RO"))
}

/// What the lookups of the command `name`, started at `now`, write to the
/// keys they find: see `store::AccessScope`. And whether the keys have their
/// use written after the command, by `eviction::used`: a script's declared
/// keys, and a modification under the LRM policies.
#[inline(always)]
fn access_scope(
    name: &str,
    now: std::time::Instant,
    cmd: &[&[u8]],
) -> (crate::store::AccessScope, bool) {
    use crate::store::AccessScope;
    // Names that start with another letter than these are neither, and are
    // not compared to them (GET, SET, ...).
    if matches!(
        name.as_bytes().first(),
        Some(b'O' | b'T' | b'P' | b'E' | b'D' | b'R' | b'F')
    ) {
        if asks_only(name) {
            return (AccessScope::none(), false);
        }
        if is_script(name) {
            return (AccessScope::none(), true);
        }
    }
    let stamp = crate::eviction::stamp(now);
    // A stamp of 0 under a policy that records: the LRM policies, where only
    // a modification is a use, and the command's outcome tells.
    let after = stamp == 0;
    (access_scope_counting(name, cmd, stamp), after)
}

/// [`access_scope`] once the stamp is known.
#[inline(always)]
fn access_scope_counting(name: &str, cmd: &[&[u8]], stamp: u32) -> crate::store::AccessScope {
    use crate::store::AccessScope;
    // Under LFU, a key the command names twice (`MGET a a`) is counted
    // twice, as Redis counts each lookup. Only a command of more than one
    // argument can, and the hot ones find their keys without the long
    // match (`keyspec::key_positions`).
    if !AccessScope::counts(stamp) || cmd.len() < 3 {
        return AccessScope::open(stamp);
    }
    AccessScope::open_naming(stamp, named_more_than_once(name, cmd))
}

/// The keys the command names more than once, and how many times.
fn named_more_than_once(name: &str, cmd: &[&[u8]]) -> Vec<(Box<[u8]>, u8)> {
    let positions = crate::keyspec::key_positions(name, cmd);
    let mut keys = positions.iter();
    let (Some(first), Some(second)) = (keys.next(), keys.next()) else {
        return Vec::new();
    };
    let mut keys: Vec<&[u8]> = [first, second]
        .into_iter()
        .chain(keys)
        .map(|pos| cmd[pos])
        .collect();
    keys.sort_unstable();
    let mut named = Vec::new();
    for run in keys.chunk_by(|a, b| a == b) {
        if run.len() > 1 {
            named.push((Box::from(run[0]), run.len().min(u8::MAX as usize) as u8));
        }
    }
    named
}

/// The keys the command `name` names, used under the access scope it runs
/// in (see `run_command`): for a command that answered with an error.
#[cold]
#[inline(never)]
fn used_on_error(ctx: &Ctx, name: &str) {
    let db = *ctx.selected_db;
    for pos in crate::keyspec::key_positions(name, &ctx.cmd).iter() {
        let key = ctx.cmd[pos];
        crate::read_lock!(crate::store::get_shard_of_bytes(&ctx.store[db], key)).use_key(key);
    }
}

/// The routing of [`run_command`], again, for a write inside the write gate
/// that was routed as `routed`, if its keys may have left since: a
/// `MIGRATE` deleted them here, or a slot changed state
/// (`pause::routes_moved`). `None` if it may run here (#331).
#[cold]
#[inline(never)]
fn routed_again_in_gate(
    ctx: &Ctx,
    name: &str,
    routed: crate::cluster::routing::Routed,
) -> Option<Vec<u8>> {
    if !ctx.cluster.enabled
        || ctx.bypass_cluster_check
        || !crate::pause::routes_moved_since(routed.at())
    {
        return None;
    }
    let store = ctx.store;
    let db = *ctx.selected_db;
    let registry = ctx.client_registry;
    let client_id = ctx.client_id;
    crate::cluster::routing::route_named(
        name,
        ctx.cmd.as_slice(),
        ctx.cluster,
        routed.asking(),
        || registry.get_readonly(client_id),
        |key| crate::read_lock!(crate::store::get_shard(&store[db], key)).contains_key(key),
    )
    .refusal(ctx.peer_ipv6)
}

// The parts of `run_command` that a plain command on a standalone master
// never runs are functions of their own, `#[cold]` and never inlined: the
// function that every command runs stays small, and what is added to one of
// them does not change how the compiler lays out the rest (#867). Each is
// called behind the cheap check (an atomic load, a flag) that it used to
// sit behind, in the same order.

/// Cluster routing of a command as it arrives, on a node with cluster mode
/// on (and not the replica's applier): the refusal (`-MOVED`, `-ASK`, ...),
/// or `None` to run it here, with `routed` set when it was routed.
#[cold]
#[inline(never)]
fn route_on_arrival(
    ctx: &mut Ctx,
    name: &str,
    routed: &mut crate::cluster::routing::Routed,
) -> Option<Vec<u8>> {
    use crate::cluster::routing::route_named;
    // A cluster node has one keyspace. Letting SELECT move a
    // connection to another database would put keys where no slot
    // accounting, migration or replication check ever looks.
    if name == "SELECT" && ctx.cmd.get(1).is_some_and(|db| *db != &b"0"[..]) {
        return Some(resp_error("ERR SELECT is not allowed in cluster mode"));
    }
    // A command `EXEC` runs was routed with the rest of its queue, once,
    // before any of it ran (`routing::exec_refusal`), and leaves
    // `ASKING` to the end of the transaction (`routing::transaction_ended`).
    // Routed again here, it was judged on the keys the commands before
    // it had made or deleted, and refused half way through (#339).
    // Scripts' commands do not come here; a command of the client's
    // takes the gate alone only after this.
    if !crate::command_gate::alone_on_this_thread() {
        // Pull asking flag from client registry.
        let asking_flag = ctx.client_registry.get_asking(ctx.client_id);
        // Before the keys are looked for: anything that moves them from
        // here after this, the write sees in the gate.
        *routed = crate::cluster::routing::Routed::now(asking_flag);
        // has_key_locally: bind to the store/db combination via closure.
        let store_for_check = ctx.store;
        let db = *ctx.selected_db;
        let has_key_locally = |key: &[u8]| -> bool {
            let shard = crate::store::get_shard(&store_for_check[db], key);
            crate::read_lock!(shard).contains_key(key)
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
        // ASKING is single-shot: clear after this command regardless of
        // outcome.
        if asking_flag {
            ctx.client_registry.update(ctx.client_id, |info| {
                info.asking = false;
            });
        }
        if let Some(refusal) = decision.refusal(ctx.peer_ipv6) {
            return Some(refusal);
        }
        crate::cluster::routing::stall_for_test(name, ctx.cmd.as_slice());
    }
    None
}

/// On a replica, for a client: whether `name` is a write it refuses.
#[cold]
#[inline(never)]
fn denied_on_readonly_replica(ctx: &Ctx, name: &str) -> bool {
    replication::is_denied_on_readonly_replica(name, &ctx.cmd)
}

/// With `min-replicas-to-write` set, for a client: whether the command is
/// refused for too few good replicas.
#[cold]
#[inline(never)]
fn refused_without_replicas(ctx: &Ctx, name: &str) -> bool {
    !matches!(
        name,
        "EVAL" | "EVALSHA" | "EVAL_RO" | "EVALSHA_RO" | "FCALL" | "FCALL_RO"
    ) && !crate::command_gate::alone_on_this_thread()
        && replication::refused_without_replicas(name, &ctx.cmd)
        && ctx.repl_state.lacks_good_replicas()
}

/// While the dataset is loading, for a client: whether `name` waits for it.
#[cold]
#[inline(never)]
fn refused_while_loading(name: &str) -> bool {
    !replication::slave_command_allowed_during_loading(name)
}

/// With a restricted user connected: the refusal of the command if its user
/// may not run it, or touch its keys or channels.
#[cold]
#[inline(never)]
fn acl_refusal(ctx: &Ctx, name: &str) -> Option<Vec<u8>> {
    // The command, every key it names (`keyspec` knows where they are)
    // and every channel, under the user's rules or one of its selectors.
    let denied = ctx
        .acl_store
        .read()
        .get(ctx.authenticated_user)
        .and_then(|user| crate::acl::check_permission(user, name, ctx.cmd.as_slice()).err())?;
    // As in Redis, a command there is no such command as is refused
    // for that, before the user's rules (and the ACL log). The
    // wrong number of arguments was refused above.
    if !crate::commands::server::is_command(name) {
        return Some(unknown_command(&ctx.cmd));
    }
    ctx.acl_log.record(
        denied.reason(),
        denied.object(),
        ctx.authenticated_user,
        &crate::acl::log_client_info(ctx.client_registry, ctx.client_id, ctx.peer_addr),
    );
    Some(resp_error(&denied.message(ctx.authenticated_user)))
}

/// For a client's write in the write gate, `write_gate`, when a handoff, a
/// `MIGRATE`, a move of its keys or a `CLIENT PAUSE` may have come since it
/// was routed as `routed` (`pause::writes_must_check`): see `run_command`.
/// The reply to give instead of running it, or `None` to run it; a write
/// that has to wait is sent back to wait outside its gates
/// ([`WAIT_OUT_HANDOFF`], [`WAIT_UNTIL_MOVED`]).
#[cold]
#[inline(never)]
fn checked_in_gate(
    ctx: &mut Ctx,
    name: &str,
    routed: &crate::cluster::routing::Routed,
    write_gate: &mut Option<crate::write_gate::WriteGuard>,
    beside_others: bool,
) -> Option<Vec<u8>> {
    // While the AOF cannot be written, what would be logged is refused, as
    // Redis refuses it (`MISCONF`): a write, or a script that may make some,
    // before it touches anything. Found by the one load that brought the
    // write here (`pause::writes_must_check`): nothing more for a write
    // otherwise, and nothing for a read.
    if crate::aof::write_failing()
        && crate::aof::refused_while_failing(ctx.cmd.as_slice(), name, ctx.apply_replication)
    {
        *write_gate = None;
        return Some(crate::aof::misconf());
    }
    // Not for `SCRIPT LOAD` or `SCRIPT FLUSH`: they change the script
    // cache, not the dataset, and are sent to nobody (#705), so a
    // handoff misses nothing of them.
    // The pause does not hold them (#518), and they used to wait the
    // handoff out here instead, asleep on their worker thread, with
    // every other connection of that thread (#528).
    let handoff = crate::pause::handoff() && name != "SCRIPT";
    if !handoff
        && crate::pause::write_held_in_gate(ctx.cmd.as_slice())
        && !crate::commands::blocking::client_gone(ctx)
    {
        *write_gate = None;
        // `ASKING` was used up when it was routed; the dispatch that
        // runs it is routed as this one was.
        if routed.asking() {
            ctx.client_registry
                .update(ctx.client_id, |info| info.asking = true);
        }
        return Some(crate::commands::blocking::hold_for_pause(ctx));
    }
    if !handoff
        && !(crate::pause::keys_moving()
            && crate::key_moves::names_a_moving_key(*ctx.selected_db, name, ctx.cmd.as_slice()))
    {
        return routed_again_in_gate(ctx, name, *routed);
    }
    *write_gate = None;
    // It waits outside every gate, and is then dispatched again from
    // the top ([`dispatched_again_after_waiting`], #340): it holds
    // nothing while it waits, and what it was checked against before
    // its gates (routing, ACL, `lua-parallel-scripts`, whether it runs
    // beside other scripts) is decided again. A script that runs alone
    // lets go of the command gate before it waits: waited out here, no
    // other command would run on any thread meanwhile. So does a run of
    // scripts that keeps the gate closed between them
    // (`command_gate::reopen_for_a_wait`).
    //
    // What its first dispatch used up is given back to the next, as the
    // pause's branch above does: `ASKING`, and a quick script's leave to run
    // beside others (`tag_locks::allow_next_script`), which is one-shot and
    // would otherwise make it run alone, holding every other command back
    // for that run. A leave the next dispatch does not take is cleared by
    // the caller that gave it (`lua_vm_blocking`).
    if routed.asking() {
        ctx.client_registry
            .update(ctx.client_id, |info| info.asking = true);
    }
    if beside_others {
        crate::tag_locks::allow_next_script();
    }
    Some(if handoff {
        WAIT_OUT_HANDOFF.to_vec()
    } else {
        WAIT_UNTIL_MOVED.to_vec()
    })
}

/// What [`checked_in_gate`] answers for a write that has to wait out a
/// handoff, or until a `MIGRATE` has moved its keys, outside its gates:
/// never sent. See [`dispatched_again_after_waiting`].
const WAIT_OUT_HANDOFF: &[u8] = b"\0wait out the handoff\0";
/// See [`WAIT_OUT_HANDOFF`].
const WAIT_UNTIL_MOVED: &[u8] = b"\0wait until its keys have moved\0";

/// Whether `reply` is a write sent back by [`checked_in_gate`] to wait. No
/// reply that is sent begins with a zero byte, so this is one compare for
/// every other.
#[inline(always)]
fn sent_back_to_wait(reply: &[u8]) -> bool {
    reply.first() == Some(&0) && (reply == WAIT_OUT_HANDOFF || reply == WAIT_UNTIL_MOVED)
}

/// For a write that [`checked_in_gate`] sent back as `reply`: wait, out of
/// every gate, for the handoff to end or its keys to have moved, then run
/// it again from the top (`run_command`), as many times as it is sent back.
/// The wait is on its worker thread, as it was in the gate
/// (`pause::wait_out_handoff`), with the command gate open: a run of scripts
/// that held it closed between them opens it first
/// (`command_gate::reopen_for_a_wait`). The dispatch again is not held at
/// the test hook `routed` a second time. On a thread that must not wait (a
/// queued script run on a worker thread, `pause::handing_back_waits`), it
/// does not: it is answered `pause::HELD_IN_GATE`, not run, and handed back.
#[cold]
#[inline(never)]
fn dispatched_again_after_waiting(
    ctx: &mut Ctx,
    name: &str,
    started: std::time::Instant,
    mut reply: Vec<u8>,
) -> Vec<u8> {
    while sent_back_to_wait(&reply) {
        crate::command_gate::reopen_for_a_wait();
        if crate::pause::hands_back_a_wait() {
            return crate::pause::HELD_IN_GATE.to_vec();
        }
        #[cfg(debug_assertions)]
        if crate::cluster::routing::test_stalls_on() {
            log_warn!("(test) {name} waits outside its gates");
        }
        if reply == WAIT_OUT_HANDOFF {
            crate::pause::wait_out_handoff();
        } else {
            crate::key_moves::wait_until_moved(*ctx.selected_db, name, ctx.cmd.as_slice());
        }
        reply = crate::cluster::routing::dispatched_again(|| run_command(ctx, name, started));
    }
    reply
}

/// Over `maxmemory`: room made for the command, by eviction in the order of
/// writes if `in_order`, or its `OOM` refusal if it `needs_room` and none
/// could be made. See `run_command`.
#[cold]
#[inline(never)]
fn make_room_or_refuse(ctx: &mut Ctx, in_order: bool, needs_room: bool) -> Option<Vec<u8>> {
    // The keys it deletes are writes too, to be cut between and ordered
    // like any other; it orders them itself, so this comes before the
    // command takes the order of its own keys.
    let _evicting = crate::write_gate::enter();
    let limit = crate::RUNTIME_MAXMEMORY.load(std::sync::atomic::Ordering::Relaxed);
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
        return Some(crate::tracking::with_what_came_before(resp_error(
            "OOM command not allowed when used memory > 'maxmemory'.",
        )));
    }
    None
}

/// For a command from a client that allocates as much as its arguments say
/// (`sketch::room`), where the arity check's cold path finds it
/// (`arity::check_dispatch`), before anything else in `run_command`: room
/// made for it ([`room_before_dispatch`]), or its refusal, unless something
/// before the gates refuses it. That is asked first, by the checks
/// `run_command` makes after this, and nothing is made or evicted for a
/// command they refuse: routing (`MOVED`, `ASK`, ...), `BUSY`, `NOREPLICAS`,
/// `LOADING`, the ACL. (A replica's client, a master's stream and the AOF
/// are not held to `maxmemory`.) Checked here rather than where
/// `run_command` checks them so that the common path pays as little as it
/// can for it. Measured (instructions per GET and SET, `scripts/perf`): a
/// flag carried down to there cost some ten; leaving the common path for
/// good, as a refusal does (a path of its own around the rest, or
/// `run_command` again from here), fifteen to twenty-five; this, room
/// made here and the common path taken up again, three to six.
/// Something that changes between the two (a slot moving, a script
/// starting) only means room made for a command then refused, or refused
/// `OOM` when it runs without the room.
#[cold]
#[inline(never)]
fn room_on_arrival(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    if crate::RUNTIME_MAXMEMORY.load(Ordering::Relaxed) == 0
        || ctx.apply_replication
        || ctx.repl_state.is_slave()
        || refused_before_the_gates(ctx, name)
    {
        return None;
    }
    // Eviction may wait for the order of keys a `MIGRATE` holds: not with
    // the gate closed for a run of scripts (`command_gate::for_a_run`).
    crate::command_gate::reopen_for_a_wait();
    room_before_dispatch(ctx, name)
}

/// Whether `run_command` refuses the command before its gates, by the
/// checks it makes there (for a master's client; see [`room_on_arrival`]).
/// Asks only: `ASKING` is left for the routing to take.
fn refused_before_the_gates(ctx: &Ctx, name: &str) -> bool {
    if ctx.cluster.enabled
        && !ctx.bypass_cluster_check
        && !crate::command_gate::alone_on_this_thread()
    {
        let asking = ctx.client_registry.get_asking(ctx.client_id);
        let (store, db) = (ctx.store, *ctx.selected_db);
        let (registry, client_id) = (ctx.client_registry, ctx.client_id);
        let decision = crate::cluster::routing::route_named(
            name,
            ctx.cmd.as_slice(),
            ctx.cluster,
            asking,
            || registry.get_readonly(client_id),
            |key: &[u8]| {
                crate::read_lock!(crate::store::get_shard(&store[db], key)).contains_key(key)
            },
        );
        if decision.refusal(ctx.peer_ipv6).is_some() {
            return true;
        }
    }
    #[cfg(feature = "lua")]
    if crate::commands::lua::lua_server_busy_for_command(ctx.cmd.as_slice()) {
        return true;
    }
    if ctx.repl_state.min_replicas_to_write.load(Ordering::Relaxed) != 0
        && refused_without_replicas(ctx, name)
    {
        return true;
    }
    if crate::vector::SNAPSHOT_INDEX_REBUILD.load(Ordering::Relaxed) && refused_while_loading(name)
    {
        return true;
    }
    (crate::acl::RESTRICTED_CMD_CLIENTS.load(Ordering::Relaxed) > 0
        || crate::acl::RESTRICTED_KEY_CLIENTS.load(Ordering::Relaxed) > 0)
        && ctx
            .acl_store
            .read()
            .get(ctx.authenticated_user)
            .is_some_and(|user| {
                crate::acl::check_permission(user, name, ctx.cmd.as_slice()).is_err()
            })
}

/// For a command that allocates as much as its arguments say (`sketch::room`),
/// under `maxmemory`, once nothing before the gates refuses it (routing,
/// `BUSY`, `READONLY`, `NOREPLICAS`, `LOADING`, the ACL): room made for what
/// it wants before it takes its keys' order ([`make_room_in_steps`]), or its
/// refusal. Not for a command
/// that is not held to `maxmemory` (on a replica, from a master or the
/// AOF), nor while writes are paused. A command its handler refuses before
/// its allocation (its arguments, its key's type, the per-command limit)
/// wants nothing, and nothing is evicted for it.
#[cold]
#[inline(never)]
fn room_before_dispatch(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    let limit = crate::RUNTIME_MAXMEMORY.load(std::sync::atomic::Ordering::Relaxed);
    if limit == 0
        || ctx.apply_replication
        || ctx.repl_state.is_slave()
        || crate::pause::expiry_deferred()
    {
        return None;
    }
    let wanted = match crate::sketch::room::wanted(ctx, name) {
        Ok(wanted) => wanted,
        Err(refusal) => return Some(refusal),
    };
    if wanted == 0 {
        return None;
    }
    // With what the command allocates besides on its way to its check
    // (its reply, a copy of its arguments, a decoded payload's pieces): its
    // check looks again, and must find the room still there.
    let wanted = wanted.saturating_add(ROOM_SLACK);
    if crate::sketch::room_for(wanted) {
        return None;
    }
    // More than evicting every key could free (what the server held before
    // it had any, `used_memory_startup`, is never freed): refused at once,
    // rather than after emptying the keyspace for nothing.
    let wanted_usize = usize::try_from(wanted).unwrap_or(usize::MAX);
    if wanted_usize > limit.saturating_sub(crate::commands::memory::startup_used()) {
        return Some(sketch_oom());
    }
    make_room_in_steps(ctx, wanted_usize)
}

/// Room made for a command beyond what it says it allocates
/// ([`room_before_dispatch`]).
const ROOM_SLACK: u64 = 128 << 10;

/// What eviction for one command frees at most in one hold of the gates
/// ([`make_room_in_steps`]): some tens of thousands of small keys, a few
/// milliseconds.
const EVICTION_STEP: usize = 4 << 20;

/// Evict until memory use is down to `target`, for a command that wants
/// `wanted` bytes: a step of [`EVICTION_STEP`] bytes at a time, each in the
/// gates in which every command's room is made (see `make_room_or_refuse`),
/// let go between steps. One command that wants a 1 GiB filter evicts all
/// it needs, but a cut (a full sync, a snapshot), a script, a transaction
/// or a flush waits for no more than one step of it; inside a script or a
/// transaction, which hold the gates already, it holds them throughout, as
/// they do. It stops, refused, after twice the steps its size takes (and a
/// few more), should writes elsewhere fill memory as fast as it frees it.
///
/// What it was asked under is looked at again in each step, in the gates:
/// writes paused meanwhile (`CLIENT PAUSE`, a failover, a slot handoff),
/// this node become a replica, or `maxmemory` changed. Paused or a replica,
/// it stops, room made or not, and the command goes on to what it meets
/// then (held by the pause, `READONLY`): dispatched again after a pause, it
/// is sized again from the start. The limit, and the target with it, are
/// the limit of the moment; none, it stops.
fn make_room_in_steps(ctx: &mut Ctx, wanted: usize) -> Option<Vec<u8>> {
    #[cfg(feature = "lua")]
    let overrun = crate::commands::lua::running_script_is_over_its_time_limit;
    #[cfg(not(feature = "lua"))]
    let overrun = || false;
    let steps = (wanted / EVICTION_STEP).saturating_mul(2).saturating_add(8);
    for step in 0..steps {
        #[cfg(debug_assertions)]
        if step > 0 {
            stall_between_steps_for_test();
        }
        let _ = step;
        let _gate =
            crate::command_gate::enter_unless(std::time::Duration::from_millis(10), overrun)?;
        let _evicting = crate::write_gate::enter();
        let limit = crate::RUNTIME_MAXMEMORY.load(Ordering::Relaxed);
        if limit == 0 || ctx.repl_state.is_slave() || crate::pause::expiry_deferred() {
            return None;
        }
        let Some(target) = limit.checked_sub(wanted) else {
            return Some(sketch_oom());
        };
        let used = crate::memory_use::counted();
        if used <= target {
            return None;
        }
        if !crate::eviction::make_room(ctx, target.max(used.saturating_sub(EVICTION_STEP)), true) {
            return Some(sketch_oom());
        }
    }
    let limit = crate::RUNTIME_MAXMEMORY.load(Ordering::Relaxed);
    (limit != 0 && crate::memory_use::counted().saturating_add(wanted) > limit).then(sketch_oom)
}

/// `KIVIDB_TEST_FAILPOINTS` names `sketch-room-step-stall` (debug builds):
/// a pause of 300 ms between two steps of [`make_room_in_steps`], out of
/// the gates, for a test to change what it was asked under meanwhile.
#[cfg(debug_assertions)]
fn stall_between_steps_for_test() {
    static STALL: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    if *STALL.get_or_init(|| crate::aof::injected("sketch-room-step-stall")) {
        std::thread::sleep(std::time::Duration::from_millis(300));
    }
}

/// The `OOM` refusal of a command there is no room for.
fn sketch_oom() -> Vec<u8> {
    crate::tracking::with_what_came_before(resp_error(
        "OOM command not allowed when used memory > 'maxmemory'.",
    ))
}

/// For a command a script calls that allocates as much as its arguments
/// say, under `maxmemory`: room made for it as for one from a client
/// ([`room_before_dispatch`]), when the script runs alone (as it holds the
/// gates, the eviction is a part of it, as in a transaction). Not for one
/// that runs beside other scripts under its tags (`lua-parallel-scripts`):
/// eviction would delete keys of theirs from under them. Such a command is
/// refused with `OOM` if what it allocates does not fit. Nor for one that
/// the script's checks after this refuse (too few replicas, the ACL), which
/// are asked first, as they would answer.
#[cold]
#[inline(never)]
fn room_in_script(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    if !crate::commands::arity::allocates(name) || crate::tag_locks::beside_others() {
        return None;
    }
    #[cfg(feature = "lua")]
    if crate::commands::lua::script_min_replicas_gate(ctx, ctx.cmd[0]).is_some() {
        return None;
    }
    if (crate::acl::RESTRICTED_CMD_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) > 0
        || crate::acl::RESTRICTED_KEY_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) > 0)
        && ctx
            .acl_store
            .read()
            .get(ctx.authenticated_user)
            .is_some_and(|user| {
                crate::acl::check_permission(user, name, ctx.cmd.as_slice()).is_err()
            })
    {
        return None;
    }
    room_before_dispatch(ctx, name)
}

/// With Lua, for a command `arity::fits_with_shape` did not let through:
/// the reply to the wrong number of arguments, or room made for a command
/// that allocates as much as its arguments say (`room_on_arrival`). Out of
/// line, off the way of every other command.
#[cfg(feature = "lua")]
#[cold]
#[inline(never)]
fn arity_refusal(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    let refusal = crate::commands::arity::check_dispatch(name, &ctx.cmd)?;
    // Empty: a command that allocates as much as its arguments say, given
    // room here if nothing below refuses it.
    if !refusal.is_empty() {
        return Some(refusal);
    }
    room_on_arrival(ctx, name)
}

/// [`execute_command`], once the command's name is known; it started at
/// `started`.
fn run_command(ctx: &mut Ctx, name: &str, started: std::time::Instant) -> Vec<u8> {
    // The wrong number of arguments is refused before anything else looks
    // at the command, as in Redis. See `arity`. With Lua, the same lookup
    // says how the command's keys are found, for the look for hash tags
    // below (#889).
    #[cfg(feature = "lua")]
    let key_shape = match crate::commands::arity::fits_with_shape(name, &ctx.cmd) {
        Some(shape) => shape,
        None => {
            if let Some(refusal) = arity_refusal(ctx, name) {
                return refusal;
            }
            None
        }
    };
    #[cfg(not(feature = "lua"))]
    if let Some(refusal) = crate::commands::arity::check_dispatch(name, &ctx.cmd) {
        // Empty: a command that allocates as much as its arguments say,
        // given room here if nothing below refuses it (`room_on_arrival`).
        if !refusal.is_empty() {
            return refusal;
        }
        if let Some(refusal) = room_on_arrival(ctx, name) {
            return refusal;
        }
    }

    // [`ReplState::is_slave`] is a relaxed atomic load — avoid `role()` here (it allocates).
    // Taken before the command is routed, as everything it is checked
    // against below may be: a write that passed these checks while this
    // node was a master can be held up before the write gate while the node
    // becomes a replica. The gate routes it again (#331, #337); so does the
    // test hook that holds it there (`routing::stall_for_test`).
    let repl_is_slave = ctx.repl_state.is_slave();

    // ── Cluster routing check ────────────────────────────────────────
    //
    // Decide whether this command can run on this node. In standalone mode
    // or when bypass_cluster_check is true (replica applier), this is a
    // single boolean check and passes through.
    //
    // When it was routed, for a write to compare inside the write gate:
    // its keys may have left meanwhile (#331).
    let mut routed = crate::cluster::routing::Routed::default();
    // Standalone-mode fast path: skip every cluster-related check entirely.
    // A single branch here keeps the asking-flag lookup and the closure
    // setup off the standalone hot path.
    if ctx.cluster.enabled
        && !ctx.bypass_cluster_check
        && let Some(refusal) = route_on_arrival(ctx, name, &mut routed)
    {
        return refusal;
    }

    #[cfg(feature = "lua")]
    if crate::commands::lua::lua_server_busy_for_command(ctx.cmd.as_slice()) {
        return busy_reply();
    }

    // Masters skip `is_write_command` entirely here — hot path for standalone read benchmarks.
    if repl_is_slave && !ctx.apply_replication && denied_on_readonly_replica(ctx, name) {
        return resp_error("READONLY You can't write against a read only replica.");
    }
    // Too few good replicas: a write is refused, as Redis refuses it. Not
    // a command of a transaction (EXEC was refused whole, or let through
    // whole, before it began: see `transaction::exec_refusal`), nor a
    // script, whose writes are refused one by one as it makes them
    // (`lua::script_min_replicas_gate`). One relaxed load when it is off.
    if !ctx.apply_replication
        && ctx.repl_state.min_replicas_to_write.load(Ordering::Relaxed) != 0
        && refused_without_replicas(ctx, name)
    {
        return resp_error(replication::NOREPLICAS);
    }
    // `LOADING` is for clients. A command arriving from the master's stream
    // (or from AOF replay) has to be applied regardless: refusing it does not
    // make the master retry, it just drops the write, and the replica quietly
    // diverges for as long as the index rebuild takes.
    if !ctx.apply_replication
        && ((repl_is_slave
            && ctx
                .repl_state
                .slave_sync_in_progress
                .load(Ordering::Relaxed))
            || crate::vector::SNAPSHOT_INDEX_REBUILD.load(Ordering::Relaxed))
        && refused_while_loading(name)
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
    if (crate::acl::RESTRICTED_CMD_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) > 0
        || crate::acl::RESTRICTED_KEY_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) > 0)
        && let Some(refusal) = acl_refusal(ctx, name)
    {
        return refusal;
    }

    // With NOOP=1, `scripts/perf/build.sh` adds a never-taken branch after
    // the next line, to show that a line added to this function moves no
    // instructions on the command path (`scripts/perf/stability.sh`, #867).
    // perf/stability.sh: no-op line

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
    //
    // Not while writes are paused (`CLIENT PAUSE`, a failover, a slot
    // handoff): the dataset stands still then, as in Redis, where eviction
    // waits like expiry (`PAUSE_ACTION_EVICT`). Its `DEL`s moved the
    // replication offset a paused master had reported (#424).
    let over_limit = !repl_is_slave
        && !ctx.apply_replication
        && check_memory_limit()
        && !crate::pause::expiry_deferred();
    // A library of functions is memory too, though not in the keyspace.
    let loads_functions = name == "FUNCTION"
        && ctx.cmd.get(1).is_some_and(|sub| {
            sub.eq_ignore_ascii_case(b"LOAD") || sub.eq_ignore_ascii_case(b"RESTORE")
        });
    let needs_room = over_limit
        && (loads_functions
            || (replication::command_mutates_for_replication(name, &ctx.cmd)
                && !crate::eviction::allowed_when_out_of_memory(name)));

    // Writes hold the write gate from before they touch the store until their
    // replication/AOF payload has been queued, so a full resync or checkpoint
    // can cut cleanly between whole writes. See `write_gate`.
    //
    // Scripts are gated as a unit: their writes propagate one `redis.call` at
    // a time, and a cut in the middle of one would be a cut in the middle of
    // something that is supposed to be atomic.
    //
    // MIGRATE is deliberately not gated here. It blocks on the network for up
    // to its timeout, and takes what it needs itself, for no longer than it
    // must (`commands::cluster::hold_while_moving`); it propagates as an
    // idempotent `DEL`.
    //
    // Nor is FT.DROPINDEX here: with `DD` it may delete millions of documents,
    // and it takes the gate, and the order of their keys, a few at a time
    // itself (`vector::ft_dropindex`). With tag locks on, a `DD` takes the
    // command gate alone (`gate_alone_or_walk`): it deletes keys whose tags
    // it does not lock.
    let writes_operation = replication::command_mutates_for_replication(name, &ctx.cmd);
    let script = matches!(name, "EVAL" | "EVALSHA" | "FCALL");
    let gated = (writes_operation && name != "MIGRATE" && name != "FT.DROPINDEX") || script;

    // The first `SELECT` of a database other than 0 switches every write to
    // carrying a `SELECT` of its own, and has to wait out the writes on
    // their way without one first (`ReplState::note_database_selected`). A
    // cut cannot be taken from inside the write gate, which a `SELECT`
    // enters as a write: there it was none, a database-0 write in flight
    // reached the stream and the AOF behind the plain `SELECT N`, and was
    // applied in N (#423). So it is taken here, before the gate. Once only:
    // `several_databases` stays set.
    if gated && name == "SELECT" {
        first_select_of_another_database(ctx);
    }

    // And a write is ordered against the other writes to the same keys: it
    // runs whole before or after each of them, and, while anything is
    // listening to the write stream, the stream carries them in the order the
    // store took them. See `write_order`. Whether anything is listening
    // decides only whether the write is propagated, never whether it is
    // ordered: a command that reads a key and writes it back under separate
    // locks of its shard (`PFADD`, `HINCRBY` on a new key, `BITFIELD`, the
    // `*STORE` commands) lost updates without it (#271).
    //
    // A write its master sent is in order with the others it sent: the
    // stream is one connection, applied by one task. It still takes the
    // order of its keys (`keys_ordered`), as a command does, against the
    // field-expiry sweep, which brought a hash's postings up to date over an
    // `HSET` applied meanwhile, or dropped its watch over an `HPEXPIREAT`
    // (#371). Not when nothing is watched and the command watches nothing
    // either: no sweep is then part way through a hash, nor can one start on
    // a key of this write before it is done, and the order cost every applied
    // write 4 to 8 per cent more instructions. A command that may watch a
    // hash (`tracks_field_ttls`) takes it whatever is watched: it watches the
    // hash before it brings the indexes up to date with it, and a sweep could
    // otherwise reach the hash in between, and be written over (#376). Only
    // the order, too: it takes the gate as it did, and never alone for a
    // flush. Not while the AOF is replayed: nothing else writes then.
    let ordered = gated && !ctx.apply_replication;
    let keys_ordered = ordered
        || (gated
            && !crate::expiry::replaying()
            && (crate::field_expiry::any_watched() || tracks_field_ttls(name)));

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
    // whose keys all carry a tag — one, or several — is held to those tags,
    // however it is run. One that its caller has seen to be a quick one also
    // runs beside other scripts: it takes the gate as a command does, and
    // its tags' locks below. Not inside a transaction or another script:
    // those hold the gate alone already.
    let tag_locks = crate::tag_locks::enabled();
    let (script_tags, beside_others) = if any_script {
        script_mode(&ctx.cmd, name, tag_locks, ctx.apply_replication)
    } else {
        (None, false)
    };
    // A command that changes keys it does not name must not find a script
    // part-way through: it waits for all of them (`takes_the_gate_alone`);
    // one that only reads them keeps such scripts out while it runs, below.
    // On a thread that holds the gate already, what either takes of it is
    // nothing: the replica's applier holds it for each turn of its master's
    // stream, alone for a command that takes it alone
    // (`applied_takes_the_gate_alone`, #373), and EXEC and a script hold it
    // alone for theirs.
    #[cfg(debug_assertions)]
    if gated && !crate::write_gate::held_by_this_thread() {
        crate::pause::test_stall_before_the_gate(ctx.cmd.as_slice());
    }
    //
    // With Lua, a command that reads keys it does not name (`KEYS`, `SCAN`,
    // `INFO`, `FT.SEARCH`, ...) holds the gate shared and keeps out the
    // scripts that run beside others while it reads, and nothing else
    // (`gate_alone_or_walk`, `tag_locks::walk`, #772): only it, and the
    // commands that take the gate alone, come that way.
    #[cfg(feature = "lua")]
    let (_command_gate, _alone, _walk) = if crate::command_gate::skips(name, &ctx.cmd) {
        (None, None, None)
    } else if takes_the_gate_alone(name, &ctx.cmd, any_script && !beside_others, gated) {
        let Some(gates) =
            gate_alone_or_walk(name, &ctx.cmd, any_script && !beside_others, gated, SLICE)
        else {
            return busy_reply();
        };
        gates
    } else {
        let Some(gate) = crate::command_gate::enter_unless(SLICE, overrun) else {
            return busy_reply();
        };
        (Some(gate), None, None)
    };
    #[cfg(not(feature = "lua"))]
    let gates = if crate::command_gate::skips(name, &ctx.cmd) {
        Some((None, None))
    } else if takes_the_gate_alone(name, &ctx.cmd, any_script && !beside_others, gated) {
        crate::command_gate::enter_alone_unless(SLICE, overrun).map(|alone| (None, Some(alone)))
    } else {
        crate::command_gate::enter_unless(SLICE, overrun).map(|gate| (Some(gate), None))
    };
    #[cfg(not(feature = "lua"))]
    let Some((_command_gate, _alone)) = gates else {
        return busy_reply();
    };

    // A write under a script or an `EXEC` that already holds the gate is part
    // of that one: the handoff's cut waits for the whole of it.
    let nested = crate::write_gate::held_by_this_thread();
    let mut _write_gate = gated.then(crate::write_gate::enter);
    // A handoff under way — slots migrating away, or a manual failover — has
    // paused writes, and this one got past the pause before it was set. It
    // must not run behind the handoff's back (see `pause::HANDOFF`): out of
    // the gate, wait, and then answer as whoever owns the key now would.
    //
    // The same one load finds a `MIGRATE` holding keys here (`key_moves`): a
    // write to one of them waits until it has gone, or stayed, and is then
    // answered as the slot's state says — `-ASK` for a key that left. A
    // write to other keys goes on.
    //
    // And it finds that keys have left here, or a slot has changed state,
    // since this write was routed: a whole `MIGRATE` of its key may have run
    // between its routing and the gate. It is routed again, here in the
    // gate, where nothing moves its keys before it is done (#331).
    //
    // And a `CLIENT PAUSE` set since it passed the pause check: it would be
    // applied, and acknowledged, after the pause had been answered and the
    // master's offset read, and lost by a switchover that promoted a replica
    // at that offset (#629). It goes back to its connection, out of the
    // gates, and is held there like any write that comes during the pause
    // (`pause::pause_clients`). Not a dispatch that resumes a command whose
    // client has gone, which writes nothing.
    if gated
        && !nested
        && !ctx.apply_replication
        && crate::pause::writes_must_check(routed.at())
        && let Some(reply) = checked_in_gate(ctx, name, &routed, &mut _write_gate, beside_others)
    {
        return reply;
    }

    if over_limit {
        let in_order = ordered || (!gated && !ctx.apply_replication);
        if let Some(refusal) = make_room_or_refuse(ctx, in_order, needs_room) {
            return refusal;
        }
    }

    // The tags' locks, for a script that runs under its tags; and for a
    // command from a client, the lock of every tagged key it names, so that
    // it finds those keys as they were before a script or as they are after
    // it. Both take several lowest first (`tag_locks`), inside the write
    // gate, and before the order of writes below, so that they are always
    // taken the same way round.
    let _script_tags = script_tags.map(|tags| {
        if beside_others {
            crate::tag_locks::lock_for_script(&tags)
        } else {
            crate::tag_locks::hold_script_to(&tags)
        }
    });
    //
    // A tag a script holds is not waited for here, where the thread would
    // wait with it and every other connection of the thread behind it: the
    // command is sent back, and its connection waits for the script off the
    // thread (#882).
    //
    // Nearly every command: none of its keys has a `{`, and nothing else is
    // looked at, not even whether this is a script's own call (#889).
    #[cfg(feature = "lua")]
    let _key_tags = if tag_locks
        && !any_script
        && !crate::command_gate::alone_on_this_thread()
        && let Some(keys) = crate::tag_locks::named_keys_of(key_shape, name, &ctx.cmd)
        && !crate::tag_locks::in_script()
    {
        match lock_tags_found(ctx, keys) {
            Ok(held) => held,
            Err(busy) => {
                // Sent back: not run now.
                crate::commands::blocking::hold_for_tags(ctx, busy);
                return crate::pause::HELD_IN_GATE.to_vec();
            }
        }
    } else {
        None
    };
    #[cfg(not(feature = "lua"))]
    let _key_tags = if tag_locks
        && !any_script
        && !crate::command_gate::alone_on_this_thread()
        && !crate::tag_locks::in_script()
    {
        lock_tags_of_named_keys(ctx, name)
    } else {
        None
    };

    // Whether a hash command took its key's order shared, having found no
    // search index: see `ask_again_for_an_index`.
    let mut hash_beside_others = false;
    // The keys it names, if they were looked for here: for
    // `expire_named_keys_first`, which would look for them again.
    let mut named: Option<crate::keyspec::Keys> = None;
    let mut _write_order = if crate::command_gate::alone_on_this_thread() {
        // No other command is running: there is nothing to be ordered
        // against, and the writes made from inside this one take nothing.
        Some(crate::write_order::held_exclusively())
    } else if keys_ordered
        && !replication::write_is_observed(ctx)
        && atomic_under_its_shard(ctx, name)
    {
        // Its key's order shared, beside the writes like it, which the shard
        // orders (see `write_order::lock_key_shared`). Not while anything
        // reads the write stream: the stream carries the writes to a key in
        // the order they take this lock whole.
        // `HSET`, `HMSET`, `HSETNX`, `HINCRBY`, `HINCRBYFLOAT`: the only
        // ones of them that start with an H.
        hash_beside_others = name.as_bytes()[0] == b'H';
        Some(crate::write_order::lock_key_shared(
            *ctx.selected_db,
            ctx.cmd[1],
        ))
    } else if keys_ordered
        && matches!(name, "MSET" | "MSETNX")
        && !replication::write_is_observed(ctx)
    {
        // Each key's order shared: both write every key under all of their
        // shards' locks at once (`WriteShards`), so the shards order them
        // against each other and against the writes above; see
        // `write_order::lock_keys_shared`. Whole while anything reads the
        // write stream, as every other write is.
        Some(order_of_pairs_shared(ctx))
    } else if keys_ordered {
        Some(order_of_named_keys(ctx, name, &mut named))
    } else {
        None
    };
    if hash_beside_others && !ctx.index_store.none() {
        ask_again_for_an_index(ctx, &mut _write_order);
    }
    // From here to the end, one time for every expiry check the command
    // makes: the look below for the keys it names that are past their time,
    // and its own lookups after it (`script_clock::command`, #632). Taken
    // once its gates and the order of its keys are held, where it may have
    // waited: the time is the one it runs at.
    let _clock = crate::script_clock::command();
    // The keys it names that are past their time go first, their deletions
    // sent ahead of it (`expire_named_keys_first`, #628). In the gate and
    // the order of its keys: the deletion falls where it did among the
    // writes to them.
    if gated && writes_operation {
        expire_named_keys_first(ctx, name, named);
    }

    // HOTKEYS: one relaxed load when no session is running.
    let hotkeys_started = hotkeys::active().then(std::time::Instant::now);

    // MONITOR: a command is shown when it is called, before it runs — what a
    // script calls then follows the script, as it happened — and a command
    // that blocks is shown once, when it arrives, not again when it resumes.
    // The administrative commands are not shown, as in Redis.
    if crate::monitor::MONITOR_COUNT.load(std::sync::atomic::Ordering::Relaxed) > 0 {
        feed_monitors(ctx, name);
    }

    // `notify-keyspace-events` `n`, `m`, `o` and `c`: what they need to know
    // of the keys before the command runs (see `KeyspaceWatch`). With none
    // of them on, which is the default, this is one load and a branch, and
    // one pointer kept across the command.
    let keyspace_watch = if crate::keyspace_events::KS_FLAGS
        .load(std::sync::atomic::Ordering::Relaxed)
        & KeyspaceWatch::FLAGS
        != 0
    {
        KeyspaceWatch::begin(ctx, name, writes_operation)
    } else {
        None
    };

    // Under an eviction policy that goes by use, the command's own lookups
    // write it (see `eviction`): one atomic load when there is none. A
    // `CLIENT NO-TOUCH` client's reads record no use of their keys: no
    // scope, and nothing written after them either. Its writes count as
    // uses — a key one makes is born used, not at the start of time — a
    // script that may write counts its declared keys the same, and `TOUCH`
    // still touches, as in Redis (#566).
    let (access, used_after) = if crate::eviction::recording()
        && !(ctx.resp & crate::commands::ctx::RESP_NO_TOUCH != 0
            && no_touch_skips(name, &ctx.cmd, writes_operation))
    {
        let (scope, after) = access_scope(name, started, &ctx.cmd);
        (Some(scope), after)
    } else {
        (None, false)
    };
    // A script command is none of the handlers': each was asked, and every
    // one of them said no, before it got to `lua::handle` (#901: 1,400
    // instructions an `EVALSHA`). With Lua it goes there at once.
    #[cfg(feature = "lua")]
    let script = matches!(
        name,
        "EVALSHA" | "EVAL" | "EVALSHA_RO" | "EVAL_RO" | "FCALL" | "FCALL_RO"
    );
    #[cfg(not(feature = "lua"))]
    let script = false;
    let dispatched = if script {
        None
    } else {
        dispatch_command_handlers(ctx, name)
    };
    let result = match dispatched {
        Some(result) => result,
        None => not_dispatched(ctx, name),
    };
    if let Some(scope) = access {
        // A command refused for what it was given (a syntax error, say) can be
        // refused before it looks its keys up; those keys are used all the
        // same, as they were when the use was written after every command. Only
        // on an error: a key it did look up is not counted again.
        if result.first() == Some(&b'-') {
            used_on_error(ctx, name);
        }
        // Under LFU, a key the command names more than once is counted as many
        // times as it is named, as when its use was written after the command,
        // a lookup for each place it is named. Its own lookups count up to that
        // many; these make up the rest.
        if scope.is_counting()
            && let Some(named) = crate::store::AccessScope::named_more_than_once()
        {
            use_keys_named_more_than_once(ctx, named);
        }
        drop(scope);
    }

    if let Some(watch) = keyspace_watch {
        watch.end(ctx);
    }
    // The events of the commands that announce none of their own (#160).
    if writes_operation
        && crate::keyspace_events::KS_FLAGS.load(std::sync::atomic::Ordering::Relaxed) != 0
    {
        announce_command_events(ctx, name, &result);
    }

    // A parked command has not done its work yet — there is nothing to
    // propagate, invalidate or feed to MONITOR until it is resumed, and the
    // reply above is a discarded placeholder. The resume dispatch runs this.
    // All but what it did before it parked: the consumer an `XREADGROUP`
    // created (#654).
    if crate::commands::blocking::parked(ctx) {
        if name == "XREADGROUP" {
            propagate_before_park(ctx, name);
        }
        return result;
    }

    if let Some(started) = hotkeys_started {
        hotkeys::record(ctx, name, result.len(), started.elapsed());
    }

    // The uses its lookups did not write: a modification under the LRM
    // policies, and the keys a script declares (see `eviction`). Not by the
    // commands that only ask about a key: Redis does not count those as a
    // use of it either.
    if used_after {
        use_named_keys(ctx, name, writes_operation && !result.starts_with(b"-"));
    }

    write_side_effects(ctx, name, &result, writes_operation);

    // Command handlers hardcode RESP2's legacy null encoding (`$-1`/`*-1`); rewrite it to the
    // RESP3 null type (`_`) here, once, for RESP3 connections — see `resp3_normalize_null`.
    // `ctx.resp` is per-connection local state, not a client_registry lookup — see Ctx::resp.
    // The protocol is 2 or 3: bit 0 says RESP3, whatever `RESP_NO_TOUCH` is.
    let result = if ctx.resp & 1 != 0 {
        crate::resp::write::resp3_normalize_null(result)
    } else {
        result
    };
    crate::tracking::with_what_came_before(result)
}

/// What a command holds of the command gate, shared or alone, and, for
/// one that reads keys it does not name, of the walk locks
/// (`tag_locks::walk`).
#[cfg(feature = "lua")]
type Gates = (
    Option<crate::command_gate::Guard>,
    Option<crate::command_gate::AloneGuard>,
    Option<crate::tag_locks::Walk>,
);

/// The gates of a command [`takes_the_gate_alone`] says takes the gate
/// alone. One that only reads keys it does not name, and that nothing else
/// makes take it alone, takes the walk locks instead, before the command
/// gate (as a script that runs beside others takes its own, before its
/// command), and the gate shared: what it reads is found with no such script
/// part-way through, and every other command runs beside it. Out of line:
/// only the commands that take the gate alone come here.
#[cfg(feature = "lua")]
#[inline(never)]
fn gate_alone_or_walk(
    name: &str,
    cmd: &[&[u8]],
    script_alone: bool,
    gated: bool,
    slice: std::time::Duration,
) -> Option<Gates> {
    let overrun = crate::commands::lua::running_script_is_over_its_time_limit;
    // What takes the gate alone whatever it reads: as before; and, with tag
    // locks on, what deletes or makes keys it does not name
    // (`FT.DROPINDEX … DD`, `DEBUG POPULATE`).
    let alone = gate_alone_rule(name, cmd, script_alone, gated, false)
        || (crate::tag_locks::enabled()
            && crate::tag_locks::sees_the_whole_keyspace(name)
            && !crate::tag_locks::walks(name, cmd))
        || (crate::tag_locks::enabled()
            && crate::tag_locks::changes_keys_it_does_not_name(name, cmd));
    let walks = !alone
        && crate::tag_locks::enabled()
        && crate::tag_locks::reads_keys_it_does_not_name(name)
        && !crate::command_gate::alone_on_this_thread()
        && match name {
            // By its patterns.
            "SORT" | "SORT_RO" => crate::commands::generics::sort_reads_keys_it_does_not_name(cmd),
            _ => crate::tag_locks::walks(name, cmd),
        };
    if !alone && !walks {
        // A reader of keys it names after all (`MEMORY USAGE`, `CLUSTER
        // NODES`, `SORT` by nothing): as any command.
        return crate::command_gate::enter_unless(slice, overrun)
            .map(|gate| (Some(gate), None, None));
    }
    if walks {
        let walk = crate::tag_locks::walk();
        return crate::command_gate::enter_unless(slice, overrun)
            .map(|gate| (Some(gate), None, Some(walk)));
    }
    crate::command_gate::enter_alone_unless(slice, overrun).map(|alone| (None, Some(alone), None))
}

/// The command to the clients in `MONITOR`, unless it is resuming or is an
/// administrative one. See `run_command`.
#[cold]
#[inline(never)]
fn feed_monitors(ctx: &Ctx, name: &str) {
    // Nor when it is run again off its connection's thread: it was shown
    // when it arrived (`deferred::run_elsewhere`).
    if !crate::commands::blocking::resuming(ctx)
        && !crate::deferred::running_elsewhere()
        && !crate::commands::server::is_admin_command(name)
    {
        crate::monitor::feed(
            ctx.monitor_registry,
            &ctx.cmd,
            *ctx.selected_db,
            ctx.peer_addr,
        );
    }
}

/// With scripts locked by hash tag (`tag_locks`), for a client's command:
/// the locks of the tags of the keys it names. A build without Lua has no
/// scripts that run beside others, and never comes here.
#[cfg(not(feature = "lua"))]
#[inline(never)]
fn lock_tags_of_named_keys(ctx: &Ctx, name: &str) -> Option<crate::tag_locks::KeysGuard> {
    crate::tag_locks::lock_named_keys(name, &ctx.cmd)
}

/// With scripts locked by hash tag (`tag_locks`), for a client's command
/// with a key that may carry a tag (`tag_locks::named_keys`): the locks of
/// the tags of the keys it names. Out of line: only a command with a `{` in
/// a key comes here.
///
/// `Err`: a script holds one of them, or is in line for it, and the
/// command's connection can wait for it without its thread: it is to be
/// sent back (`blocking::hold_for_tags`, #882), and not to run now. What cannot wait so waits on
/// the locks, as before: the replica's applier and the AOF's replay (which
/// never meet a script that runs beside others there: a script from the
/// stream or the log runs alone, so what they can find holding a tag is a
/// read-only script of a replica's client, for as long as it runs), a
/// command of a connection's slow path, and the dispatch of a command whose
/// client has gone, which only leaves the line it stood in. So does the
/// dispatch that runs a command again after its connection waited holds the
/// turns of the tags it waited for: no script holds those, and the others
/// are tried as on the first dispatch; the turns go once the locks are held,
/// or once it is sent back again, to wait for more of its tags
/// (`tag_locks::try_lock_found_keys`).
#[cfg(feature = "lua")]
#[inline(never)]
fn lock_tags_found(
    ctx: &mut Ctx,
    keys: crate::tag_locks::NamedKeys,
) -> Result<Option<crate::tag_locks::KeysGuard>, crate::tag_locks::TagsBusy> {
    let Some(slot) = ctx
        .blocking
        .as_deref_mut()
        .filter(|slot| slot.awaitable && !(slot.gone && slot.resume.is_some()))
    else {
        return Ok(crate::tag_locks::lock_found_keys(keys, &ctx.cmd));
    };
    if let Some(turns) = slot.tag_turns.take() {
        return lock_tags_holding_turns(&ctx.cmd, keys, turns);
    }
    crate::tag_locks::try_lock_found_keys(keys, &ctx.cmd, &[])
}

/// [`lock_tags_found`] for the dispatch that runs a command again after its
/// connection waited for the turns of some of its tags.
#[cfg(feature = "lua")]
#[cold]
#[inline(never)]
fn lock_tags_holding_turns(
    cmd: &[&[u8]],
    keys: crate::tag_locks::NamedKeys,
    turns: crate::tag_locks::TagTurns,
) -> Result<Option<crate::tag_locks::KeysGuard>, crate::tag_locks::TagsBusy> {
    let tried = crate::tag_locks::try_lock_found_keys(keys, cmd, turns.stripes());
    drop(turns);
    tried
}

/// The order of the keys a write names (see `write_order`), and where it
/// writes to another database, of the same keys there; the keys it names
/// are left in `named`. Out of line: the generic lockers are large, and a
/// `SET` or an `INCR` takes its key's order shared instead.
#[inline(never)]
fn order_of_named_keys(
    ctx: &Ctx,
    name: &str,
    named: &mut Option<crate::keyspec::Keys>,
) -> crate::write_order::OrderGuard {
    let keys = named.insert(crate::keyspec::key_positions(name, &ctx.cmd));
    let here = *ctx.selected_db;
    match database_written_too(ctx, name) {
        // The key's record is ordered where it goes as well: see
        // `write_order::lock_keys_in`.
        Some(there) if there != here => crate::write_order::lock_keys_in(
            keys.iter()
                .flat_map(|pos| [(here, ctx.cmd[pos]), (there, ctx.cmd[pos])]),
        ),
        _ => crate::write_order::lock_keys(here, keys.iter().map(|pos| ctx.cmd[pos])),
    }
}

/// Whether `key` is in the selected database.
fn key_exists(ctx: &Ctx, key: &[u8]) -> bool {
    crate::read_lock!(crate::store::get_shard(&ctx.store[*ctx.selected_db], key)).contains_key(key)
}

/// What `notify-keyspace-events` needs to know of the keys before a command
/// runs, to announce afterwards what it did to them. Built only when one
/// of those classes is on: off, it costs `run_command` a load and a branch,
/// and one value kept across the command instead of three (#867). Nothing
/// is allocated for it beyond what the vectors hold, and nothing at all
/// when there is nothing to watch.
struct KeyspaceWatch {
    /// `n`: the places of the keys a write names that are not there now,
    /// to see afterwards which of them it created.
    absent: Vec<usize>,
    /// `m`: the places of the keys a read looks for and does not find.
    missed: Vec<usize>,
    /// `o` / `c` (see `replace_watch_begin`).
    replacing: Option<ReplaceWatch>,
}

impl KeyspaceWatch {
    /// The classes of events that need a watch.
    const FLAGS: u32 = crate::keyspace_events::KS_NEW
        | crate::keyspace_events::KS_KEY_MISS
        | crate::keyspace_events::KS_OVERWRITTEN
        | crate::keyspace_events::KS_TYPE_CHANGED;

    /// The watch of the command `name`, if any of those classes is on and
    /// there is something to watch. Inline, as is `end`: with `m` on every
    /// read comes here, with `o` or `c` every write, and out of line (cold
    /// or not) the calls and the moves of the watch cost a read that
    /// missed 70 instructions and a write 50 more than the same code inline
    /// in `run_command`. Behind the one test of the flags in `run_command`,
    /// they cost a command nothing while the events are off.
    #[inline(always)]
    fn begin(ctx: &Ctx, name: &str, writes_operation: bool) -> Option<KeyspaceWatch> {
        let flags = || crate::keyspace_events::KS_FLAGS.load(std::sync::atomic::Ordering::Relaxed);
        if !writes_operation {
            // A read has only `m` to watch. It never fired (#160). `EXISTS`
            // and `TYPE` only ask, as in Redis.
            if flags() & crate::keyspace_events::KS_KEY_MISS == 0
                || matches!(name, "EXISTS" | "TYPE" | "TOUCH" | "OBJECT")
            {
                return None;
            }
            let missed = named_keys_absent(ctx, name);
            if missed.is_empty() {
                return None;
            }
            return Some(KeyspaceWatch {
                absent: Vec::new(),
                missed,
                replacing: None,
            });
        }
        let absent = if flags() & crate::keyspace_events::KS_NEW != 0 {
            named_keys_absent(ctx, name)
        } else {
            Vec::new()
        };
        let replacing = if flags()
            & (crate::keyspace_events::KS_OVERWRITTEN | crate::keyspace_events::KS_TYPE_CHANGED)
            != 0
        {
            replace_watch_begin(ctx, name)
        } else {
            None
        };
        if absent.is_empty() && replacing.is_none() {
            return None;
        }
        Some(KeyspaceWatch {
            absent,
            missed: Vec::new(),
            replacing,
        })
    }

    /// After the command: `overwritten` / `type_changed`, `new`, `keymiss`.
    #[inline(always)]
    fn end(self, ctx: &Ctx) {
        let KeyspaceWatch {
            absent,
            missed,
            replacing,
        } = self;
        if let Some(watch) = replacing {
            replace_watch_end(ctx, watch);
        }
        if !absent.is_empty() {
            announce_new_keys(ctx, absent);
        }
        if !missed.is_empty() {
            announce_missed_keys(ctx, missed);
        }
    }
}

/// The places of the keys the command names that are not there now: for
/// `notify-keyspace-events` `n` and `m`. See `KeyspaceWatch`. Inline,
/// as the code it replaces was.
#[inline(always)]
fn named_keys_absent(ctx: &Ctx, name: &str) -> Vec<usize> {
    crate::keyspec::key_positions(name, &ctx.cmd)
        .iter()
        .filter(|pos| !key_exists(ctx, ctx.cmd[*pos]))
        .collect()
}

/// `new` for each key at the places `absent` (absent before the command)
/// that is there now.
#[inline]
fn announce_new_keys(ctx: &Ctx, absent: Vec<usize>) {
    for pos in absent {
        let key = ctx.cmd[pos];
        if key_exists(ctx, key) {
            crate::keyspace_events::notify(
                ctx.pubsub,
                *ctx.selected_db,
                crate::keyspace_events::KS_NEW,
                "new",
                key,
            );
        }
    }
}

/// `keymiss` for each key at the places `missed`.
#[inline]
fn announce_missed_keys(ctx: &Ctx, missed: Vec<usize>) {
    for pos in missed {
        crate::keyspace_events::notify(
            ctx.pubsub,
            *ctx.selected_db,
            crate::keyspace_events::KS_KEY_MISS,
            "keymiss",
            ctx.cmd[pos],
        );
    }
}

/// With keyspace events on, after a write: the events of the commands
/// that announce none of their own (#160), unless it failed or parked.
/// Inline, as it was: with any class of events on, every write comes here.
#[inline(always)]
fn announce_command_events(ctx: &Ctx, name: &str, result: &[u8]) {
    if result.starts_with(b"-") || crate::commands::blocking::parked(ctx) {
        return;
    }
    // A BITFIELD that changed no field and grew nothing wrote nothing, and
    // Redis 8 announces nothing for it.
    if name == "BITFIELD" && ctx.skip_write_side_effects {
        return;
    }
    for event in crate::keyspace_events::command_events(name, &ctx.cmd, result) {
        if let Some(key) = ctx.cmd.get(event.key) {
            crate::keyspace_events::notify(
                ctx.pubsub,
                event.db.unwrap_or(*ctx.selected_db),
                event.class,
                &event.event,
                key,
            );
        }
    }
}

/// Under LFU: the keys the command named more than once (`named`, with how
/// many times) used as many more times as its own lookups did not count.
/// See `run_command`, which asks whether there are any.
#[cold]
#[inline(never)]
fn use_keys_named_more_than_once(ctx: &Ctx, named: Vec<(Box<[u8]>, u8)>) {
    let db = *ctx.selected_db;
    for (key, times) in named {
        let map = crate::read_lock!(crate::store::get_shard_of_bytes(&ctx.store[db], &key));
        for _ in 0..times {
            map.use_key(&key);
        }
    }
}

/// The use of each key the command names, written after it: a
/// modification (`wrote`) under the LRM policies, a script's declared keys.
/// Not cold: under an LRM policy every write comes here.
#[inline(never)]
fn use_named_keys(ctx: &Ctx, name: &str, wrote: bool) {
    for pos in crate::keyspec::key_positions(name, &ctx.cmd).iter() {
        crate::eviction::used(ctx.store, *ctx.selected_db, ctx.cmd[pos], wrote);
    }
}

/// A command no handler of `dispatch_command_handlers` knows: a script's,
/// or there is no such command. Out of line, but not cold: every `EVAL`,
/// `EVALSHA` and `FCALL` comes this way.
#[inline(never)]
fn not_dispatched(ctx: &mut Ctx, name: &str) -> Vec<u8> {
    lua::handle(ctx, name).unwrap_or_else(|| unknown_command(&ctx.cmd))
}

/// What an `XREADGROUP` that is about to wait has already changed: the
/// consumer it created, which stays whether anything is delivered or not.
/// Redis propagates it as `XGROUP CREATECONSUMER` when the command blocks;
/// waiting for the resume left it out of the AOF and the replicas
/// whenever the wait timed out, since by then the consumer was there to
/// be found (#654). Deliveries are not made before a park, so this is the
/// whole of it; a resumed command propagates what it delivers itself.
#[cold]
#[inline(never)]
fn propagate_before_park(ctx: &Ctx, name: &str) {
    let effects = replication::script_feed::effects_take();
    if effects.is_empty() {
        return;
    }
    if !ctx.apply_replication {
        ctx.repl_state.note_local_write_of(name);
    }
    if ctx.skip_write_side_effects {
        return;
    }
    if replication::write_is_observed(ctx) {
        for parts in &effects {
            replication::replicate_one_write(ctx, parts);
        }
    }
    if replication::script_feed::effects_changed_data() {
        crate::stats::record_rdb_change();
    }
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

/// The switch to writes that carry their own `SELECT`, for a `SELECT` of
/// a database other than 0 on a server that has never left database 0,
/// taken before the `SELECT` enters the write gate (#423). Nothing for any
/// other: a `SELECT 0`, one that is going to be refused, or once the switch
/// has been made.
#[cold]
#[inline(never)]
fn first_select_of_another_database(ctx: &Ctx) {
    if let Some(db) = makes_the_switch(ctx) {
        ctx.repl_state.note_database_selected(db);
    }
}

/// The database a `SELECT` (`ctx.cmd`) selects if it is the one that
/// switches the server to writes that carry their own `SELECT`: a valid
/// `SELECT` of a database other than 0, with that switch not made yet.
fn makes_the_switch(ctx: &Ctx) -> Option<usize> {
    if ctx.repl_state.several_databases.load(Ordering::Relaxed) || ctx.cmd.len() != 2 {
        return None;
    }
    std::str::from_utf8(ctx.cmd[1])
        .ok()
        .and_then(|db| db.parse::<usize>().ok())
        .filter(|db| *db != 0 && *db < ctx.config.databases)
}

/// Whether a write's reply says it changed nothing: `SET ... NX` on a key
/// that exists, `SADD` of members there already, `EXPIRE` of a missing key.
/// Redis does not touch such a key, and a `WATCH` on it holds (#159). Only
/// replies that cannot mean a change are listed: `ZADD` and `HSET` answer
/// `0` for updates too.
///
/// Nor is such a write propagated, as Redis does not propagate it (#628).
/// A replica, and a replay of the AOF, never take a key as gone because of
/// its time: a `PERSIST` that found its key past its time, answered 0 and
/// left the key for the sweep, took the key's time away on the replica, and
/// the key lived there with no time to live until the sweep's `UNLINK`
/// came. A `MOVE` or `COPY` that answered 0 moved the key on the replica,
/// where the sweep's `DEL` then deleted nothing, and the key stayed in the
/// other database for good.
fn changed_nothing(name: &str, cmd: &[&[u8]], result: &[u8]) -> bool {
    let nil = result == b"$-1\r\n" || result == b"_\r\n";
    let zero = result == b":0\r\n";
    match name {
        "SET" => nil && !set_wrote_answering_nil(cmd),
        // `SMOVE`: no source, the member not in it, or moved to where it
        // is and not there. `LPUSHX` / `RPUSHX`: no list (a push to one
        // answers its new length, at least 1).
        "SETNX" | "MSETNX" | "HSETNX" | "RENAMENX" | "PERSIST" | "EXPIRE" | "PEXPIRE"
        | "EXPIREAT" | "PEXPIREAT" | "SADD" | "SREM" | "ZREM" | "HDEL" | "LREM" | "DEL"
        | "UNLINK" | "COPY" | "MOVE" | "SMOVE" | "LPUSHX" | "RPUSHX" => zero,
        // No list (0), or no such pivot (-1).
        "LINSERT" => zero || result == b":-1\r\n",
        // A sample `IGNORE` left out, or a `TS.MADD` that added none: the
        // reply is a timestamp (an array of them), which does not say so.
        "TS.ADD" | "TS.MADD" => crate::commands::ts::ts_changed_nothing(),
        _ => popped_nothing(name, result),
    }
}

/// Whether a `SET` (`cmd`) that answered nil wrote all the same. Without
/// `GET` it did not: a `SET` that writes answers `OK`, and nil is a
/// condition (`NX`, `XX`, `IFEQ`, ...) not met. With `GET` nil is the old
/// value of a key that was not there, and the `SET` wrote unless its
/// condition needs the key (`XX`, `IFEQ`, `IFDEQ`): `SET k v NX GET` on a
/// missing key writes and answers nil. The options are read as `SET` reads
/// them: the argument of `EX`, `IFEQ`, ... is not an option.
fn set_wrote_answering_nil(cmd: &[&[u8]]) -> bool {
    let (mut get, mut needs_the_key) = (false, false);
    let mut i = 3;
    while i < cmd.len() {
        let option = cmd[i];
        let is = |name: &[u8]| option.eq_ignore_ascii_case(name);
        if is(b"GET") {
            get = true;
        } else if is(b"XX") {
            needs_the_key = true;
        } else if is(b"IFEQ") || is(b"IFDEQ") {
            needs_the_key = true;
            i += 1;
        } else if is(b"IFNE")
            || is(b"IFDNE")
            || is(b"EX")
            || is(b"PX")
            || is(b"EXAT")
            || is(b"PXAT")
        {
            i += 1;
        }
        i += 1;
    }
    get && !needs_the_key
}

/// Before a write runs on a master whose write stream something reads (the
/// AOF, a replica, a tap): each key it names that is past its time is
/// deleted, and its `UNLINK` (or `DEL`) sent, as a read that finds one
/// deletes it (`helpers::delete_key_if_expired`), and as Redis expires every
/// key a write looks up (`expireIfNeeded`).
///
/// A write takes such a key as gone. One that then wrote over it sent the
/// deletion first already (`expiry::written_over`), but one that did
/// nothing, or something that needed no new value, left the key there for
/// the sweep and was sent as it came (#628): `PERSIST` answered 0 and took
/// the key's time away on a replica, which still had the key, and `SET … XX`
/// gave the replica's key a new value with no time to live, where the
/// master had neither. Until the sweep's `UNLINK` came the replica had a key
/// the master did not, with no time to live, and a failover in between
/// brought it back for good. With the key deleted first, the replica
/// applies the write to what the master did.
///
/// Nothing when nothing reads the stream: the key stays for the sweep, or
/// for the read or write that finds it, as before. Under the write gate and
/// the order of the command's keys, which the deletion takes again for
/// nothing (both nest).
///
/// Not for a `SET` with no condition, the commonest write: it writes its
/// key whatever is there, over a key past its time too, and that sends the
/// deletion first (`expiry::written_over`). Looking first cost every such
/// `SET` about 200 instructions more (3%, with the AOF on).
#[inline]
fn expire_named_keys_first(ctx: &Ctx, name: &str, named: Option<crate::keyspec::Keys>) {
    if ctx.apply_replication || ctx.repl_state.is_slave() || !replication::write_is_observed(ctx) {
        return;
    }
    if name == "SET" && !set_has_a_condition(&ctx.cmd) {
        return;
    }
    let db = *ctx.selected_db;
    let named = named.unwrap_or_else(|| crate::keyspec::key_positions(name, &ctx.cmd));
    for pos in named.iter() {
        let key = ctx.cmd[pos];
        if crate::commands::helpers::is_expired(ctx.store, db, key) {
            crate::commands::helpers::delete_key_if_expired(ctx.store, db, key);
        }
    }
    // A test's window between this look and the command's own lookups.
    #[cfg(debug_assertions)]
    crate::pause::test_move_clock_after_the_expiry_check(ctx.cmd.as_slice());
}

/// Whether `SET` `cmd` writes only on a condition: `NX`, `XX`, `IFEQ`,
/// `IFNE`, `IFDEQ` or `IFDNE`. The argument of an option (`EX 10`, `IFEQ
/// NX`) is taken as an option too, which only makes a `SET` look first that
/// need not have.
fn set_has_a_condition(cmd: &[&[u8]]) -> bool {
    cmd.iter().skip(3).any(|arg| {
        arg.eq_ignore_ascii_case(b"NX")
            || arg.eq_ignore_ascii_case(b"XX")
            || (arg.len() >= 4 && arg[..2].eq_ignore_ascii_case(b"IF"))
    })
}

/// Whether a write's reply (`result`) says it changed nothing
/// ([`changed_nothing`]), and so is not propagated. A reply that could say
/// so is short and is a number, a null or an empty array: the common reply,
/// `+OK` or a value, is passed over without looking at the command.
#[inline]
fn propagates_nothing(name: &str, cmd: &[&[u8]], result: &[u8]) -> bool {
    match result.first() {
        Some(b':' | b'$' | b'_' | b'*' | b'~') if result.len() <= 5 => {
            changed_nothing(name, cmd, result)
        }
        // A longer number or array: only a time series sample `IGNORE`
        // left out says it changed nothing that way.
        Some(b':' | b'*') => ts_added_nothing(name),
        _ => false,
    }
}

/// Whether `name` is a `TS.ADD` or `TS.MADD` that added no sample
/// ([`crate::commands::ts::ts_changed_nothing`]). The mark is read for those
/// two alone: it is set by the command that just ran on this thread, and
/// cleared when the next of them starts, so for any other command it may be
/// one an earlier command left.
#[inline]
fn ts_added_nothing(name: &str) -> bool {
    name.as_bytes().first() == Some(&b'T')
        && matches!(name, "TS.ADD" | "TS.MADD")
        && crate::commands::ts::ts_changed_nothing()
}

/// Whether a pop answered that it took nothing: a null (a `BLPOP` that
/// timed out, or found nothing in `MULTI` or a script; an `LPOP` of a
/// missing key) or an empty array (`ZPOPMIN` of one). Redis neither
/// propagates such a pop nor counts it as a change, and a `WATCH` on its
/// keys holds. The reply is looked at first: it is rarely one of these.
#[inline]
fn popped_nothing(name: &str, result: &[u8]) -> bool {
    matches!(
        result,
        b"$-1\r\n" | b"*-1\r\n" | b"_\r\n" | b"*0\r\n" | b"~0\r\n"
    ) && matches!(
        name,
        "LPOP"
            | "RPOP"
            | "LMPOP"
            | "LMOVE"
            | "RPOPLPUSH"
            | "LMOVEM"
            | "ZPOPMIN"
            | "ZPOPMAX"
            | "ZMPOP"
            | "SPOP"
            | "BLPOP"
            | "BRPOP"
            | "BLMPOP"
            | "BLMOVE"
            | "BRPOPLPUSH"
            | "BLMOVEM"
            | "BZPOPMIN"
            | "BZPOPMAX"
            | "BZMPOP"
    )
}

/// After a `SWAPDB` (`ctx.cmd`): the watches on keys of the two swapped
/// databases are dirty, if the key is in either of them — its value changed
/// or it appeared or went. A key in neither, and every key of another
/// database, is unchanged; a database swapped with itself changes nothing
/// (Redis's `touchAllWatchedKeysInDb`). A watch on a key that had expired
/// when it was watched, swapped for none or for another expired one, holds,
/// as Redis keeps it by `wk->expired`; one on a key that was not there,
/// swapped for an expired one, is marked expired
/// ([`crate::watch::WatchNotifier::notify_replaced`]).
fn touch_swapped_watches(ctx: &Ctx) {
    let db = |i: usize| {
        ctx.cmd
            .get(i)
            .and_then(|arg| std::str::from_utf8(arg).ok())
            .and_then(|arg| arg.parse::<usize>().ok())
    };
    let (Some(db1), Some(db2)) = (db(1), db(2)) else {
        return;
    };
    if db1 == db2 {
        return;
    }
    // The swap has been made: what a watched key of `watched` was is now in
    // `other`, and what it is now is in `watched`.
    let presence = |db: usize, key: &[u8]| crate::watch::presence(ctx.store, db, key);
    for (watched, other) in [(db1, db2), (db2, db1)] {
        ctx.watch_registry
            .notify_replaced(Some(watched), &|_, key| {
                (presence(other, key), presence(watched, key))
            });
    }
}

/// Mark every `WATCH` on a key this write (`result`, its reply) changed as
/// dirty: the keys [`for_each_changed_key`] gives.
///
/// It used to notify `cmd[1]` and nothing else. So `MSET a 1 b 2` left a
/// client watching `b` free to `EXEC`, as did `RENAME a b`, `LMOVE a b …`,
/// `SUNIONSTORE …` for their other keys — and for a command whose first
/// argument is not a key (`XGROUP CREATE s …`, `LMPOP 1 k …`) it notified a
/// "key" named `CREATE` or `1`. A flush did not notify anyone. Then it
/// notified every key the command named, changed or not (#584).
fn notify_watchers(ctx: &Ctx, name: &str, result: &[u8]) {
    if !ctx.watch_registry.has_watchers() {
        return;
    }
    notify_watchers_of_changed_keys(ctx, name, result);
}

#[cold]
fn notify_watchers_of_changed_keys(ctx: &Ctx, name: &str, result: &[u8]) {
    let db = *ctx.selected_db;
    match name {
        // The flush has said which watched keys it removed, before it
        // removed them (`touch_watched_keys`).
        "FLUSHDB" | "FLUSHALL" => return,
        "SWAPDB" => return touch_swapped_watches(ctx),
        // The key, in this database and in the one it went to, as Redis
        // signals both.
        "MOVE" => {
            if result == b":1\r\n" {
                ctx.watch_registry.notify(db, ctx.cmd[1]);
                if let Some(to) = ctx.cmd.get(2).and_then(|to| parse_db(to)) {
                    ctx.watch_registry.notify(to, ctx.cmd[1]);
                }
            }
            return;
        }
        // `COPY … DB n`: the destination is in database `n`.
        "COPY" => {
            if let Some(to) = copy_db(&ctx.cmd) {
                if result == b":1\r\n" {
                    ctx.watch_registry.notify(to, ctx.cmd[2]);
                }
                return;
            }
        }
        _ => {}
    }
    let known = for_each_changed_key(ctx, name, result, |key| ctx.watch_registry.notify(db, key));
    if !known && !changed_nothing(name, &ctx.cmd, result) {
        // Not in the key table: keep the old first-argument guess.
        if let Some(key) = ctx.cmd.get(1) {
            ctx.watch_registry.notify(db, key);
        }
    }
}

fn parse_db(arg: &[u8]) -> Option<usize> {
    std::str::from_utf8(arg).ok()?.parse().ok()
}

/// The database of `COPY source destination DB n`, if it names one.
fn copy_db(cmd: &[&[u8]]) -> Option<usize> {
    let at = cmd
        .iter()
        .skip(3)
        .position(|arg| arg.eq_ignore_ascii_case(b"DB"))?;
    parse_db(cmd.get(3 + at + 1)?)
}

/// Run `changed` on each key of the current database that a successful
/// write (`result`, its reply) changed: the keys Redis signals as modified
/// (`signalModifiedKey`), and so those whose `WATCH` fails and whose
/// tracking clients are told. Not every key a command names:
///
/// * a write that changed nothing changes no key ([`changed_nothing`]);
/// * a key the command said it left as it was ([`crate::watch::unchanged`]:
///   a `DEL` of a key that is not there, `SMOVE` to a set that has the
///   member already);
/// * of a pop from the first of several keys that has something (`BLPOP`,
///   `BZPOPMIN`, `LMPOP`, ...), only the key it took from, which its reply
///   names;
/// * a key the command only reads: the sources of `SUNIONSTORE`, `COPY`,
///   `BITOP`, `PFMERGE`, `SORT … STORE`, `GEOSEARCHSTORE`, ..., those whose
///   key spec is `RO`;
/// * a stream whose consumer groups alone a command changes (`XREADGROUP`,
///   `XACK`, `XCLAIM`, `XAUTOCLAIM`, `XSETID`, `XGROUP` but a `CREATE …
///   MKSTREAM`): Redis does not signal it (#588).
///
/// Returns false, having done nothing, for a command the key table knows no
/// keys of. Only called while someone watches a key or tracks keys: the
/// common write pays nothing for it.
#[cold]
fn for_each_changed_key(
    ctx: &Ctx,
    name: &str,
    result: &[u8],
    mut changed: impl FnMut(&[u8]),
) -> bool {
    let keys = crate::keyspec::key_positions(name, &ctx.cmd);
    let Some(first) = keys.first() else {
        return false;
    };
    if changed_nothing(name, &ctx.cmd, result) || leaves_the_stream_signalled_as_is(name, &ctx.cmd)
    {
        return true;
    }
    let db = *ctx.selected_db;
    let mut tell = |key: &[u8]| {
        if !crate::watch::is_unchanged(db, key) {
            changed(key);
        }
    };
    if keys.iter().nth(1).is_none() {
        tell(ctx.cmd[first]);
        return true;
    }
    if let Some(popped) = popped_from(name, result)
        && let Some(pos) = keys.iter().find(|&pos| ctx.cmd[pos] == popped)
    {
        tell(ctx.cmd[pos]);
        return true;
    }
    let read_only = read_only_key_positions(&ctx.cmd);
    for pos in keys.iter() {
        if !read_only.contains(&pos) {
            tell(ctx.cmd[pos]);
        }
    }
    true
}

/// Whether `cmd` is a stream command that Redis does not signal the key of:
/// it changes a consumer group, or the stream's last ID, and calls no
/// `signalModifiedKey` (Redis 8.2's `t_stream.c`). `XGROUP CREATE …
/// MKSTREAM` may make the stream, and is taken as changing it.
fn leaves_the_stream_signalled_as_is(name: &str, cmd: &[&[u8]]) -> bool {
    match name {
        "XREADGROUP" | "XACK" | "XCLAIM" | "XAUTOCLAIM" | "XSETID" => true,
        "XGROUP" => {
            !(cmd
                .get(1)
                .is_some_and(|sub| sub.eq_ignore_ascii_case(b"CREATE"))
                && cmd.iter().any(|arg| arg.eq_ignore_ascii_case(b"MKSTREAM")))
        }
        _ => false,
    }
}

/// The key a pop from the first of several keys that has something took
/// from: the first element of its reply (`*2 $key ...`). `None` for any
/// other command, or a reply that is not that.
fn popped_from<'a>(name: &str, result: &'a [u8]) -> Option<&'a [u8]> {
    if !matches!(
        name,
        "BLPOP" | "BRPOP" | "BZPOPMIN" | "BZPOPMAX" | "LMPOP" | "ZMPOP" | "BLMPOP" | "BZMPOP"
    ) {
        return None;
    }
    let rest = result.strip_prefix(b"*")?;
    let rest = &rest[rest.iter().position(|&b| b == b'\n')? + 1..];
    let rest = rest.strip_prefix(b"$")?;
    let digits = rest.iter().position(|&b| b == b'\r')?;
    let len: usize = std::str::from_utf8(&rest[..digits]).ok()?.parse().ok()?;
    rest.get(digits + 2..digits + 2 + len)
}

/// The positions of the keys of `cmd` that its key specs say it only reads.
fn read_only_key_positions(cmd: &[&[u8]]) -> Vec<usize> {
    match crate::commands::key_specs::keys_of(cmd) {
        Some(Ok(keys)) => keys
            .into_iter()
            .filter(|(_, flags)| flags.starts_with("RO"))
            .map(|(pos, _)| pos)
            .collect(),
        _ => Vec::new(),
    }
}

/// Whether the probabilistic-type write `name` that just failed had found
/// its key (`BF.*`, `CF.*`, `CMS.*`, `TDIGEST.*`, `TOPK.*`).
#[cold]
#[inline(never)]
fn sketch_write_touched(name: &str) -> bool {
    (name.starts_with("BF.") && crate::commands::bf::take_touched())
        || (name.starts_with("CF.") && crate::commands::cf::take_touched())
        || (name.starts_with("CMS.") && crate::commands::cms::take_touched())
        || (name.starts_with("TDIGEST.") && crate::commands::tdigest::take_touched())
        || (name.starts_with("TOPK.") && crate::commands::topk::take_touched())
}

/// [`notify_watchers`] for a `BF.*` write that failed after finding its key,
/// which is always `cmd[1]`. Kept apart and out of line so `notify_watchers`
/// keeps its one call site in [`side_effects`], where it is inlined: a second
/// call site there cost every successful write about 20 instructions.
#[cold]
#[inline(never)]
fn notify_failed_bf_write(ctx: &Ctx) {
    if ctx.watch_registry.has_watchers()
        && let Some(key) = ctx.cmd.get(1)
    {
        ctx.watch_registry.notify(*ctx.selected_db, key);
    }
}

/// Whether the command is one that reads and writes its one key, `cmd[1]`,
/// under one hold of the key's shard lock: two of them on the key run one
/// before the other with nothing more, and while nothing reads the write
/// stream each takes its key's order shared (`write_order::lock_key_shared`).
///
/// Each was looked at for it, and is kept so (#271):
///
/// * `INCR` and the like: found, worked out and written under the shard's
///   write lock.
/// * `SET`, with any of its options: its condition (`NX`, `XX`, `IFEQ`,
///   ...), the old value for `GET` and the write, under the shard's write
///   lock. A document it writes over leaves its indexes after the shard is
///   let go of; every command that writes a document holds the order whole,
///   and so waits for that.
/// * `LPUSH`, `RPUSH`: the list found or made, and pushed to, under the
///   shard's write lock.
/// * `SADD`, `ZADD`, `ZINCRBY`, `HSET`, `HMSET`, `HSETNX`, `HINCRBY`,
///   `HINCRBYFLOAT`: the collection written with the shard held, for reading
///   when it is there (so that nothing can take it out of the keyspace
///   meanwhile), for writing when it is made — and made with what goes into
///   it, never empty.
/// * `PFADD`: the registers looked at under the shard's read lock; when one
///   is to change, the new value is made from them and put in their place
///   under the write lock only if they are still what is there, else made
///   again under it.
/// * `CMS.INCRBY`: the sketch found under the shard's read lock, and
///   checked and added to under one hold of its own write lock; two of
///   them on a key run one before the other. Its additions commute (each
///   counter is held at 2^32 − 1, the total wraps, and an item at the top
///   is answered with an error but added to all the same), so the sketch
///   ends as it would in either order.
///
/// The hash commands only while no search index is defined: they bring the
/// indexes up to date after the shard is let go of, and two of them beside
/// each other could leave an index with the earlier one's fields. Asked
/// again once the order is held ([`ask_again_for_an_index`], #570).
///
/// Never a command that removes what it finds (`SREM`, `HDEL`, `LPOP`, ...)
/// or writes back what it read under an earlier hold of the shard
/// (`SUNIONSTORE`, `BITFIELD`, `APPEND`, ...): those hold the order whole,
/// and these wait for them.
#[inline]
fn atomic_under_its_shard(ctx: &Ctx, name: &str) -> bool {
    if ctx.cmd.len() < 2 {
        return false;
    }
    match name {
        "SET" | "INCR" | "DECR" | "INCRBY" | "DECRBY" | "INCRBYFLOAT" | "LPUSH" | "RPUSH"
        | "SADD" | "ZADD" | "ZINCRBY" | "PFADD" => true,
        // Asked without the indexes' lock (#544): see `vector::Indexes`.
        "HSET" | "HMSET" | "HSETNX" | "HINCRBY" | "HINCRBYFLOAT" => ctx.index_store.none(),
        // Apart from the names above, which the commands on the common path
        // are among: a name more there moved their match.
        _ => name == "CMS.INCRBY",
    }
}

/// A hash command that [`atomic_under_its_shard`] let take its key's order
/// shared, and that finds a search index now that it holds it: it takes the
/// order whole instead, before it has touched the store.
///
/// It was let because there was no index when it asked, before it held the
/// order; one may have been added since. The first index is added only once
/// every write that holds an order is done, and after `none` has become
/// false (`vector::Indexes::write_to_add`): a write that holds the order
/// shared and still finds no index is over before that index exists, and
/// one that finds an index is ordered against its walk, as every other write
/// of a document is (#570).
#[cold]
#[inline(never)]
fn ask_again_for_an_index(ctx: &Ctx, order: &mut Option<crate::write_order::OrderGuard>) {
    // Let go of first: the stripe is the one taken next.
    *order = None;
    *order = Some(crate::write_order::lock_keys(
        *ctx.selected_db,
        std::iter::once(ctx.cmd[1]),
    ));
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
fn drop_if_empty(ctx: &Ctx, db: usize, key: &[u8]) -> bool {
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

/// `COPY` and `MOVE`: the key they wrote, in the database they wrote it
/// to, is indexed whole; the one `MOVE` took away leaves the indexes of its
/// database.
/// The other database a write puts a key in: `MOVE key db`'s, and
/// `COPY ... DB db`'s.
#[inline]
fn database_written_too(ctx: &Ctx, name: &str) -> Option<usize> {
    if !matches!(name, "MOVE" | "COPY") {
        return None;
    }
    let number = |i: usize| {
        ctx.cmd
            .get(i)
            .and_then(|a| std::str::from_utf8(a).ok())
            .and_then(|s| s.parse::<usize>().ok())
    };
    if name == "MOVE" {
        return number(2);
    }
    (3..ctx.cmd.len())
        .find(|&i| ctx.cmd[i].eq_ignore_ascii_case(b"DB"))
        .and_then(|i| number(i + 1))
}

fn reindex_copied(ctx: &Ctx, name: &str) {
    use crate::vector::Write;
    let here = *ctx.selected_db;
    let number = |i: usize| {
        ctx.cmd
            .get(i)
            .and_then(|a| std::str::from_utf8(a).ok())
            .and_then(|s| s.parse::<usize>().ok())
    };
    if name == "MOVE" {
        if let (Some(key), Some(to)) = (ctx.cmd.get(1), number(2)) {
            crate::vector::reindex_key(ctx.index_store, ctx.store, here, key, Write::Whole);
            crate::vector::reindex_key(ctx.index_store, ctx.store, to, key, Write::Whole);
        }
        return;
    }
    let Some(key) = ctx.cmd.get(2) else {
        return;
    };
    let mut to = here;
    let mut i = 3;
    while i < ctx.cmd.len() {
        if ctx.cmd[i].eq_ignore_ascii_case(b"DB") {
            to = number(i + 1).unwrap_or(here);
            i += 1;
        }
        i += 1;
    }
    crate::vector::reindex_key(ctx.index_store, ctx.store, to, key, Write::Whole);
}

/// Whether `name` is a command that may watch a hash with field TTLs, in
/// [`settle_written_keys`]: the first two arms there. Every write applied
/// from the stream asks: most are told by their first letter.
#[inline]
fn tracks_field_ttls(name: &str) -> bool {
    matches!(name.as_bytes().first(), Some(b'H' | b'R' | b'C' | b'M'))
        && matches!(
            name,
            "HEXPIRE"
                | "HPEXPIRE"
                | "HEXPIREAT"
                | "HPEXPIREAT"
                | "HSETEX"
                | "HGETEX"
                | "HIMPORT"
                | "RENAME"
                | "RENAMENX"
                | "RESTORE"
                | "COPY"
                | "MOVE"
        )
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
    use crate::vector::Write;
    // A command that can leave a hash with field TTLs under a key: the
    // background sweep has to know the key. See `field_expiry`.
    //
    // Each also changes a document other than by writing fields of it, or
    // puts one under a key, or takes one away: the indexes that cover the
    // key are brought up to date with what is there now (see
    // `vector::reindex_key`). A value put under a key whole (`RESTORE`, and
    // so `MIGRATE` and slot migration; `RENAME`) is indexed whole.
    //
    // Every write comes through here: the names below all begin with one of
    // a few letters, and `SET` and the like are let through on one compare.
    let first = name.as_bytes().first().copied().unwrap_or(0);
    let named_below = matches!(first, b'H' | b'R' | b'C' | b'M' | b'J') || name == "SWAPDB";
    match if named_below { name } else { "" } {
        "HEXPIRE" | "HPEXPIRE" | "HEXPIREAT" | "HPEXPIREAT" | "HSETEX" | "HGETEX" | "HIMPORT"
        | "RENAME" | "RENAMENX" | "RESTORE" => {
            let how = match name {
                "RENAME" | "RENAMENX" | "RESTORE" => Some(Write::Whole),
                "HIMPORT" => None,
                _ => Some(Write::Refresh),
            };
            let db = *ctx.selected_db;
            for pos in crate::keyspec::key_positions(name, &ctx.cmd).iter() {
                let key = ctx.cmd[pos];
                crate::field_expiry::track_key(ctx.store, db, key);
                if let Some(how) = how {
                    crate::vector::reindex_key(ctx.index_store, ctx.store, db, key, how);
                }
            }
        }
        // These can put it in another database.
        "COPY" | "MOVE" => {
            for pos in crate::keyspec::key_positions(name, &ctx.cmd).iter() {
                for db in 0..ctx.store.len() {
                    crate::field_expiry::track_key(ctx.store, db, ctx.cmd[pos]);
                }
            }
            reindex_copied(ctx, name);
        }
        "HDEL" | "HINCRBY" | "HINCRBYFLOAT" | "HSETNX" | "HGETDEL" => {
            for pos in crate::keyspec::key_positions(name, &ctx.cmd).iter() {
                let db = *ctx.selected_db;
                crate::vector::reindex_key(
                    ctx.index_store,
                    ctx.store,
                    db,
                    ctx.cmd[pos],
                    Write::Refresh,
                );
            }
        }
        "SWAPDB" => {
            crate::field_expiry::rescan(ctx.store);
            crate::vector::rebuild_indexes(ctx.index_store, ctx.store);
        }
        // A JSON document was written: the `ON JSON` indexes that cover it
        // are brought up to date. Here and not in each command, so that
        // every command that writes one is covered — and so is a replica,
        // and a replay.
        _ if name.starts_with("JSON.") => {
            for pos in crate::keyspec::key_positions(name, &ctx.cmd).iter() {
                crate::vector::reindex_json_key(
                    ctx.index_store,
                    ctx.store,
                    *ctx.selected_db,
                    ctx.cmd[pos],
                );
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
    // A blocking pop's own keys gained nothing from it: what it wakes there
    // is no client a later read waits for (#42). What it pushes (`BLMOVE`'s
    // destination) its handler wakes, and that is waited for.
    let quiet = waiters && pops_blocking(name);
    // One that took nothing — its time ran out, or `CLIENT UNBLOCK` ended
    // it — left its keys as they were. Woken by it, the client at the head
    // of the line would leave its wait only to find nothing and wait again,
    // and not be listed as blocked meanwhile: a `CLIENT UNBLOCK` sent for
    // it then was answered 0, and it stayed blocked.
    // `XREADGROUP` waits otherwise, and is not told.
    if quiet && crate::commands::blocking::took_nothing() && name != "XREADGROUP" {
        return;
    }
    for pos in crate::keyspec::key_positions(name, &ctx.cmd).iter() {
        let key = ctx.cmd[pos];
        if may_be_empty && drop_if_empty(ctx, db, key) {
            continue;
        }
        if quiet {
            crate::wait_table::without_collecting(|| ctx.wait_table.notify_signal(key));
        } else if waiters {
            ctx.wait_table.notify_signal(key);
        }
    }
}

/// The commands that wait for elements and take them.
#[cold]
fn pops_blocking(name: &str) -> bool {
    matches!(
        name,
        "BLPOP"
            | "BRPOP"
            | "BRPOPLPUSH"
            | "BLMOVE"
            | "BLMPOP"
            | "BZPOPMIN"
            | "BZPOPMAX"
            | "BZMPOP"
            | "XREADGROUP"
    )
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
        // dataset is no longer a position in that master's stream. Not a
        // `SELECT` or a `SCRIPT LOAD`, which change no data (#424).
        if !ctx.apply_replication {
            ctx.repl_state.note_local_write_of(name);
        }
    }

    // Replication / AOF / watch / RDB — successful mutating writes only.
    if is_mutating_ok_write && !ctx.skip_write_side_effects {
        // Only pay for the payload when something downstream will read it.
        // `replicate_one_write` is a no-op with no replicas and AOF off (see
        // `write_is_observed`), but reaching that no-op used to cost a
        // `Vec<Vec<u8>>` clone of every argument here *plus* a full RESP encode
        // inside — allocations on every single write, discarded immediately.
        //
        // Not a `SELECT` or a `SCRIPT LOAD` / `FLUSH`, which Redis sends to
        // nobody (#705): a write carries the `SELECT` it needs, and a
        // script's writes reach a replica as the commands it made.
        if replication::write_is_observed(ctx)
            && !propagates_nothing(name, &ctx.cmd, result)
            && !matches!(name, "SELECT" | "SCRIPT")
        {
            // Propagate the command's *effect*, not the bytes the client sent.
            // `SPOP` and `XADD … *` produce a different result each time they
            // run, so a replica re-executing them diverges (measured: a replica
            // kept a different 12 of 20 members; 4 of 5 stream IDs differed).
            // The AOF carries these same bytes, so replay depends on it too.
            //
            // Kept behind `write_is_observed` so an instance with no replicas
            // and AOF off never pays for it. In the observed case the verbatim
            // arm allocates exactly what the previous `ctx.cmd` clone did,
            // except for a write the hub alone reads that goes out as it came:
            // encoded from `ctx.cmd` itself (`write_as_it_came`, #894).
            if !replication::write_as_it_came(ctx, name, &ctx.cmd) {
                for parts in
                    replication::script_feed::commands_for_propagation(ctx, name, &ctx.cmd, result)
                {
                    replication::replicate_one_write(ctx, &parts);
                }
            }
        }

        notify_watchers(ctx, name, result);

        // An XREADGROUP that only re-read pending entries, or found nothing
        // new, changed nothing Redis counts (#27); nor did a pop that took
        // nothing, nor any write whose reply says it changed nothing
        // (`changed_nothing`), as from a script.
        if (name != "XREADGROUP" || replication::script_feed::effects_changed_data())
            && !propagates_nothing(name, &ctx.cmd, result)
        {
            crate::stats::record_rdb_change();
        }
    }

    // A `BF.*` write that failed once it had found its key (`BF.RESERVE` of
    // one there already, `BF.ADD` to a full filter) invalidates a `WATCH`
    // on it all the same, as Redis's do.
    if writes_operation && !is_mutating_ok_write && sketch_write_touched(name) {
        notify_failed_bf_write(ctx);
    }

    // ── PUBLISH reaches replicas' subscribers too ───────────────────────────
    // Not a write — nothing is stored, and it never goes to the AOF — but
    // Redis propagates it so a client subscribed on a replica receives what
    // was published on the master. The byte count is in the offset like any
    // other stream content.
    // In cluster mode the bus forwards it to every node instead, replicas
    // included (see `commands::pubsub`), so it is one or the other.
    // `SPUBLISH` as well: standalone, a shard channel is a channel (#161).
    if !writes_operation
        && matches!(name, "PUBLISH" | "SPUBLISH")
        && !ctx.cluster.enabled
        && !ctx.apply_replication
        && !ctx.repl_state.is_slave()
        && !result.starts_with(b"-")
        && ctx.repl_state.connected_replicas.load(Ordering::Relaxed) > 0
    {
        let parts: Vec<Vec<u8>> = ctx.cmd.iter().map(|b| b.to_vec()).collect();
        let payload = crate::resp::write::encode_resp_command(&parts);
        // In a transaction or a script, after the writes made before it.
        replication::propagate_in_order(ctx.repl_state, ctx.hub_tx, payload);
    }

    track(ctx, name, result, is_mutating_ok_write, writes_operation);
}

/// Client-side caching: what a command that has just run means for the
/// clients tracking keys. A write that succeeded invalidates each of its
/// keys; a read by a tracking client is noted, for each of its keys, so that
/// the client is told when one changes.
///
/// One atomic load when no client has tracking on.
#[inline(always)]
fn track(ctx: &Ctx, name: &str, result: &[u8], wrote: bool, is_write: bool) {
    if crate::tracking::TRACKING_CLIENT_COUNT.load(std::sync::atomic::Ordering::Relaxed) == 0 {
        return;
    }
    track_for_clients(ctx, name, result, wrote, is_write);
}

#[cold]
fn track_for_clients(ctx: &Ctx, name: &str, result: &[u8], wrote: bool, is_write: bool) {
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
        // The keys it changed, as for `WATCH`: Redis tells both from
        // `signalModifiedKey` (#584).
        for_each_changed_key(ctx, name, result, |key| {
            invalidate_tracked_key(ctx, key, true)
        });
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
        let dropped = ctx.tracking_table.register(ctx.cmd[pos], ctx.client_id);
        crate::tracking::tell_dropped(ctx.client_registry, dropped, Some(ctx.client_id));
    }
}

/// Tell the clients caching `key` that it has changed. `by_client` is false
/// when no client's command did it — the key was evicted — and then nobody
/// is spared the message for having asked for NOLOOP.
pub(crate) fn invalidate_tracked_key(ctx: &Ctx, key: &[u8], by_client: bool) {
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
mod applied_gate_tests {
    use super::applied_takes_the_gate_alone;

    fn takes(cmd: &[&str]) -> bool {
        let cmd: Vec<&[u8]> = cmd.iter().map(|a| a.as_bytes()).collect();
        applied_takes_the_gate_alone(&cmd)
    }

    /// What the replica's applier takes the gate alone for, before its own
    /// lock (#373): what `run_command` takes it alone for from a client, but
    /// what only a client asks for (a script beside others, by its tag). A
    /// flush and `SWAPDB` among them (#378).
    #[test]
    fn the_applier_takes_the_gate_alone_as_run_command_does() {
        assert!(!takes(&["SET", "k", "v"]));
        assert!(!takes(&["HSET", "h", "f", "v"]));
        assert!(!takes(&["MSET", "a", "1", "b", "2"]));
        assert!(!takes(&["PING"]));
        assert!(takes(&["EVAL", "return 1", "0"]));
        assert!(takes(&["EVAL_RO", "return 1", "0"]));
        assert!(takes(&["FLUSHDB"]));
        assert!(takes(&["flushall", "async"]));
        assert!(takes(&["SWAPDB", "0", "1"]));
        // Keys of two shards, and of one: a write of keys of one shard is
        // no more one step than of two.
        let shards = crate::store::new_store(1)[0].len();
        let of = |key: &str| crate::store::shard_index(shards, key);
        let other = (0..)
            .map(|i| format!("b{i}"))
            .find(|b| of(b) != of("a"))
            .unwrap();
        let same = (0..)
            .map(|i| format!("c{i}"))
            .find(|c| of(c) == of("a"))
            .unwrap();
        assert!(takes(&["COPY", "a", &other, "REPLACE"]));
        assert!(takes(&["COPY", "a", &same, "REPLACE"]));
        assert!(takes(&["DEL", "a", &same]));
        assert!(takes(&["RENAME", "{t}a", "{t}b"]));
        assert!(takes(&["SMOVE", "{t}a", "{t}b", "m"]));
        // One key named twice is one key.
        assert!(!takes(&["DEL", "a", "a"]));
        assert!(!takes(&["RENAME", "a", "a"]));
    }
}

#[cfg(all(test, feature = "lua"))]
mod script_route_tests {
    use super::script_route_refusal;
    use crate::cluster::routing::RouteDecision;

    fn text(reply: Vec<u8>) -> String {
        String::from_utf8(reply).unwrap()
    }

    /// A script's command that is not run here says why: the cluster is
    /// down, its slot is served by no node, or its key is another node's.
    #[test]
    fn a_scripts_refused_command_says_what_happened() {
        assert_eq!(
            text(script_route_refusal(&RouteDecision::ClusterDown)),
            "-ERR Script attempted to execute a command while the cluster is down\r\n"
        );
        assert_eq!(
            text(script_route_refusal(&RouteDecision::Unassigned { slot: 7 })),
            "-CLUSTERDOWN Hash slot 7 is not served\r\n"
        );
        for elsewhere in [
            RouteDecision::TryAgain,
            RouteDecision::Ask {
                slot: 7,
                target_ip: "127.0.0.1".into(),
                target_ip6: "::1".into(),
                target_port: 7000,
            },
        ] {
            assert_eq!(
                text(script_route_refusal(&elsewhere)),
                "-ERR Script attempted to access a non local key in a cluster node\r\n"
            );
        }
    }
}

#[cfg(test)]
mod noscript_tests {
    use super::not_allowed_from_script;

    /// Every command and subcommand that Valkey 9.1.3's `src/commands/*.json`
    /// flag `NOSCRIPT` and KiviDB serves, KiviDB's `BACKUP` and `HOTKEYS`
    /// subcommands that start or stop something, and some that are allowed.
    #[test]
    fn the_noscript_commands_are_redis_s() {
        let redis: &[(&str, &[&str])] = &[
            (
                "ACL",
                &[
                    "CAT", "DELUSER", "DRYRUN", "GENPASS", "GETUSER", "LIST", "LOAD", "LOG",
                    "SAVE", "SETUSER", "USERS", "WHOAMI",
                ],
            ),
            ("BACKUP", &["ABORT", "CLEANUP", "SEAL", "START"]),
            (
                "CLIENT",
                &[
                    "CACHING",
                    "GETNAME",
                    "GETREDIR",
                    "ID",
                    "INFO",
                    "KILL",
                    "LIST",
                    "NO-EVICT",
                    "NO-TOUCH",
                    "PAUSE",
                    "REPLY",
                    "SETINFO",
                    "SETNAME",
                    "TRACKING",
                    "TRACKINGINFO",
                    "UNBLOCK",
                    "UNPAUSE",
                ],
            ),
            ("CLUSTER", &["RESET"]),
            ("CONFIG", &["GET", "RESETSTAT", "REWRITE", "SET"]),
            (
                "FUNCTION",
                &[
                    "DELETE", "DUMP", "FLUSH", "KILL", "LIST", "LOAD", "RESTORE", "STATS",
                ],
            ),
            ("HOTKEYS", &["GET", "RESET", "START", "STOP"]),
            (
                "LATENCY",
                &["DOCTOR", "GRAPH", "HISTOGRAM", "HISTORY", "LATEST", "RESET"],
            ),
            ("MODULE", &["LIST", "LOAD", "LOADEX", "UNLOAD"]),
            ("SCRIPT", &["DEBUG", "EXISTS", "FLUSH", "KILL", "LOAD"]),
        ];
        for (name, subs) in redis {
            for sub in *subs {
                assert!(
                    not_allowed_from_script(name, Some(sub.as_bytes())),
                    "{name} {sub}"
                );
                let lower = sub.to_ascii_lowercase();
                assert!(not_allowed_from_script(name, Some(lower.as_bytes())));
            }
            assert!(!not_allowed_from_script(name, Some(b"HELP")), "{name} HELP");
        }
        for name in [
            "AUTH",
            "BGREWRITEAOF",
            "BGSAVE",
            "DEBUG",
            "DISCARD",
            "EVAL",
            "EVALSHA",
            "EVALSHA_RO",
            "EVAL_RO",
            "EXEC",
            "FAILOVER",
            "FCALL",
            "FCALL_RO",
            "HELLO",
            "MONITOR",
            "MULTI",
            "PSUBSCRIBE",
            "PSYNC",
            "PUNSUBSCRIBE",
            "QUIT",
            "REPLCONF",
            "REPLICAOF",
            "RESET",
            "ROLE",
            "SAVE",
            "SHUTDOWN",
            "SLAVEOF",
            "SSUBSCRIBE",
            "SUBSCRIBE",
            "SUNSUBSCRIBE",
            "SYNC",
            "UNSUBSCRIBE",
            "UNWATCH",
            "WATCH",
        ] {
            assert!(not_allowed_from_script(name, None), "{name}");
        }
        for (name, sub) in [
            ("GET", None),
            ("SET", None),
            ("PUBLISH", None),
            ("INFO", None),
            ("BACKUP", Some(&b"LIST"[..])),
            ("BACKUP", Some(b"STATUS")),
            ("CLUSTER", Some(b"KEYSLOT")),
            ("CLUSTER", Some(b"NODES")),
            ("OBJECT", Some(b"FREQ")),
            ("MEMORY", Some(b"USAGE")),
        ] {
            assert!(!not_allowed_from_script(name, sub), "{name} {sub:?}");
        }
        // KiviDB's own: what changes the cluster.
        for sub in [
            "SETSLOT",
            "FAILOVER",
            "MEET",
            "FORGET",
            "ADDSLOTS",
            "MIGRATION",
        ] {
            assert!(
                not_allowed_from_script("CLUSTER", Some(sub.as_bytes())),
                "{sub}"
            );
        }
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
        let lastsave = Arc::new(RwLock::new(0i64));
        let (hub_tx, mut hub_rx) = crate::replication::HubTx::channel();
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
            peer_ipv6: crate::cluster::is_ipv6_peer(peer.as_str()),
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

#[cfg(test)]
mod changed_nothing_tests {
    use super::*;

    fn cmd(args: &[&'static str]) -> Vec<&'static [u8]> {
        args.iter().map(|&a| a.as_bytes()).collect()
    }

    /// What a `SET` that answered nil did: with `GET` it wrote unless its
    /// condition needs the key; an option's argument is not an option.
    #[test]
    fn a_set_that_answered_nil_wrote_only_with_get_and_a_condition_a_missing_key_meets() {
        let wrote = |args: &[&'static str]| set_wrote_answering_nil(&cmd(args));
        // GET with no condition, or one a missing key meets: written.
        assert!(wrote(&["SET", "k", "v", "GET"]));
        assert!(wrote(&["SET", "k", "v", "NX", "GET"]));
        assert!(wrote(&["SET", "k", "v", "GET", "NX"]));
        assert!(wrote(&["SET", "k", "v", "IFNE", "x", "GET"]));
        assert!(wrote(&[
            "SET",
            "k",
            "v",
            "IFDNE",
            "0123456789abcdef",
            "GET"
        ]));
        assert!(wrote(&["SET", "k", "v", "EX", "10", "GET"]));
        // `XX` here is what IFNE compares with, not the option.
        assert!(wrote(&["SET", "k", "v", "IFNE", "XX", "GET"]));
        assert!(wrote(&["set", "k", "v", "ifne", "IFEQ", "get"]));
        // A condition a missing key does not meet: not written.
        assert!(!wrote(&["SET", "k", "v", "XX", "GET"]));
        assert!(!wrote(&["SET", "k", "v", "IFEQ", "x", "GET"]));
        assert!(!wrote(&[
            "SET",
            "k",
            "v",
            "IFDEQ",
            "0123456789abcdef",
            "GET"
        ]));
        // No GET: nil is a condition not met. `GET` as an argument is not
        // the option.
        assert!(!wrote(&["SET", "k", "v", "NX"]));
        assert!(!wrote(&["SET", "k", "v", "IFNE", "GET"]));
        assert!(!wrote(&["SET", "k", "v", "IFEQ", "GET"]));
        assert!(!wrote(&["SET", "k", "v", "PX", "100", "XX"]));
    }

    #[test]
    fn replies_that_change_nothing() {
        let nothing = |args: &[&'static str], reply: &[u8]| {
            let cmd = cmd(args);
            assert_eq!(
                changed_nothing(&args[0].to_ascii_uppercase(), &cmd, reply),
                propagates_nothing(&args[0].to_ascii_uppercase(), &cmd, reply),
                "{args:?}"
            );
            changed_nothing(&args[0].to_ascii_uppercase(), &cmd, reply)
        };
        assert!(nothing(&["SMOVE", "a", "b", "m"], b":0\r\n"));
        assert!(!nothing(&["SMOVE", "a", "b", "m"], b":1\r\n"));
        assert!(nothing(&["LPUSHX", "l", "x"], b":0\r\n"));
        assert!(nothing(&["RPUSHX", "l", "x"], b":0\r\n"));
        assert!(!nothing(&["RPUSHX", "l", "x"], b":1\r\n"));
        assert!(nothing(&["LINSERT", "l", "BEFORE", "p", "x"], b":0\r\n"));
        assert!(nothing(&["LINSERT", "l", "BEFORE", "p", "x"], b":-1\r\n"));
        assert!(!nothing(&["LINSERT", "l", "BEFORE", "p", "x"], b":3\r\n"));
        assert!(nothing(&["PERSIST", "k"], b":0\r\n"));
        assert!(nothing(&["SET", "k", "v", "XX"], b"$-1\r\n"));
        assert!(nothing(&["SET", "k", "v", "IFEQ", "x"], b"_\r\n"));
        assert!(!nothing(&["SET", "k", "v", "NX", "GET"], b"$-1\r\n"));
        assert!(!nothing(&["SET", "k", "v"], b"+OK\r\n"));
        assert!(nothing(&["LPOP", "l"], b"$-1\r\n"));
        // A count that is not 0 is a change; an INCR to 0 is one too.
        assert!(!nothing(&["SADD", "s", "a"], b":1\r\n"));
        assert!(!nothing(&["INCRBY", "n", "-1"], b":0\r\n"));
    }

    #[test]
    fn only_a_set_with_a_condition_looks_for_its_key_first() {
        let conditional = |args: &[&'static str]| set_has_a_condition(&cmd(args));
        assert!(!conditional(&["SET", "k", "v"]));
        assert!(!conditional(&["SET", "k", "v", "EX", "10"]));
        assert!(!conditional(&["SET", "k", "v", "KEEPTTL", "GET"]));
        // The value is not an option.
        assert!(!conditional(&["SET", "k", "NX"]));
        assert!(conditional(&["SET", "k", "v", "NX"]));
        assert!(conditional(&["SET", "k", "v", "xx", "PX", "5"]));
        assert!(conditional(&["SET", "k", "v", "IFEQ", "old"]));
        assert!(conditional(&["SET", "k", "v", "ifdne", "0123456789abcdef"]));
    }
}
