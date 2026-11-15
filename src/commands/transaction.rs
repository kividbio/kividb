// MULTI / EXEC / DISCARD / WATCH / UNWATCH
//
// Transaction state lives in server.rs on the connection stack.
// This module only handles the commands that don't need queuing —
// MULTI, DISCARD, WATCH, UNWATCH. EXEC is handled in server.rs
// because it needs to execute the queued commands directly.

use std::sync::Arc;
use std::sync::atomic::Ordering;

/// Held by an `EXEC` whose transaction watches keys, from before it checks
/// whether a watched key was modified until its last command has run.
///
/// `WATCH` is optimistic locking: read, queue, and let `EXEC` refuse if what
/// was read has changed. That only works if "has it changed?" and the
/// transaction's own writes are one step with respect to the other
/// transactions doing the same thing. They were not: two clients could both
/// pass the check, and then both write — ten buyers and a stock of five sold
/// six. With this held, the second `EXEC` makes its check after the first has
/// written, sees the watched key modified, and refuses.
///
/// Only transactions that watch something take it; the rest have asked for no
/// such guarantee. An async mutex, because `EXEC` awaits (a queued script
/// runs on the blocking pool) and because the io_uring server's threads each
/// run their own runtime.
///
/// What it does not cover: a write made outside any transaction, landing
/// between an `EXEC`'s check and its first command. See
/// `docs/EXEC_ATOMICITY.md`.
pub static WATCHED_EXEC: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const BUSY: &str =
    "BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.";

/// Whether another client's script has run past `lua-time-limit`, so that
/// `cmd` is to be refused. (Waits, as every command does, for a script that
/// has not.)
fn busy(cmd: &[&[u8]]) -> bool {
    #[cfg(feature = "lua")]
    {
        crate::commands::lua::lua_server_busy_for_command(cmd)
    }
    #[cfg(not(feature = "lua"))]
    {
        let _ = cmd;
        false
    }
}

/// The reply to a command sent inside `MULTI` that cannot be queued: there
/// is no such command, or it has the wrong number of arguments, or the
/// server is busy. The transaction it was meant for is then refused whole —
/// `EXEC` answers `-EXECABORT` — rather than run without it.
pub fn queue_refusal(name: &str, cmd: &[&[u8]]) -> Option<Vec<u8>> {
    if !crate::commands::server::is_command(name) {
        return Some(crate::commands::handler::unknown_command(cmd));
    }
    if let Some(wrong_number) = crate::commands::arity::check(name, cmd) {
        return Some(wrong_number);
    }
    // What cannot be one step of several: it ends the process, or takes
    // the connection over. (`MONITOR` is queued, as in Redis, and refused
    // when `EXEC` runs it: see `connection_reply_in_exec`.)
    if not_allowed_in_multi(name) {
        return Some(crate::resp::write::resp_error(NOT_IN_MULTI));
    }
    if busy(cmd) {
        return Some(crate::resp::write::resp_error(BUSY));
    }
    no_room(name, cmd).then(|| crate::resp::write::resp_error(OOM))
}

const OOM: &str = "OOM command not allowed when used memory > 'maxmemory'.";

/// The refusal of a command that may not be part of a transaction.
pub const NOT_IN_MULTI: &str = "ERR Command not allowed inside a transaction";

/// Whether `name` (in capitals) may not be part of a transaction: the
/// commands Redis flags `no-multi`. They end the process, write a
/// snapshot in the foreground, or take the connection over.
pub fn not_allowed_in_multi(name: &str) -> bool {
    matches!(name, "SAVE" | "SHUTDOWN" | "SYNC" | "PSYNC")
}

/// What a transaction's queued command is run with, when the connection loop
/// answers it itself rather than `execute_command`.
pub struct InExec<'a> {
    pub acl_store: &'a crate::acl::AclStore,
    pub acl_log: &'a crate::acl::SharedAclLog,
    pub user: &'a str,
    pub peer_addr: &'a str,
    pub client_registry: &'a crate::client_registry::ClientRegistry,
    pub client_id: u64,
    pub repl_state: &'a crate::replication::ReplState,
    /// For a queued `WAIT`: the replicas, and what each had been sent, as
    /// `EXEC` began (see `queues_wait`). None when nothing queued waits.
    pub wait_targets: Option<&'a [(Arc<crate::replication::ReplicaProgress>, u64)]>,
}

/// Whether a queued `WAIT` is among `queue`: `EXEC` then takes the replicas'
/// targets before it runs anything, as the `WAIT` it runs would have.
pub fn queues_wait(queue: &[Vec<Vec<u8>>]) -> bool {
    queue.iter().any(|queued| {
        queued
            .first()
            .is_some_and(|name| name.eq_ignore_ascii_case(b"WAIT"))
    })
}

/// The reply, inside `EXEC`, to a queued `UNWATCH`, `MONITOR`, `WAIT` or
/// `WAITAOF`: commands the connection loop answers itself outside a
/// transaction, and `execute_command` does not know. None for any other
/// command. (`AUTH` and `HELLO` are `login::Login`'s.)
///
/// They are queued inside `MULTI`, as in Redis, and answered here as Redis's
/// `EXEC` answers them, where nothing may block (#550, #499):
///
/// - `UNWATCH`: `OK`, and nothing else. `EXEC` has checked the watched keys
///   already, and drops the watches when it ends.
/// - `MONITOR`: refused; a connection cannot start monitoring part-way
///   through a transaction's replies.
/// - `WAIT`: how many replicas have everything written before `EXEC`, now,
///   without waiting.
/// - `WAITAOF`: whether what was written is on disk here, now, without
///   waiting; as outside a transaction, `0` replicas.
pub fn connection_reply_in_exec(name: &str, cmd: &[&[u8]], env: &InExec) -> Option<Vec<u8>> {
    if !matches!(name, "UNWATCH" | "MONITOR" | "WAIT" | "WAITAOF") {
        return None;
    }
    // Checked again when it runs, as every queued command is: the user's
    // rules may have changed since it was queued.
    if crate::acl::RESTRICTED_CMD_CLIENTS.load(Ordering::Relaxed) > 0
        || crate::acl::RESTRICTED_KEY_CLIENTS.load(Ordering::Relaxed) > 0
    {
        let denied = env
            .acl_store
            .read()
            .get(env.user)
            .and_then(|user| crate::acl::check_permission(user, name, cmd).err());
        if let Some(denied) = denied {
            env.acl_log.record(
                denied.reason(),
                denied.object(),
                env.user,
                &crate::acl::log_client_info(env.client_registry, env.client_id, env.peer_addr),
            );
            return Some(crate::resp::write::resp_error(&denied.message(env.user)));
        }
    }
    let number = |at: usize| {
        cmd.get(at)
            .and_then(|arg| std::str::from_utf8(arg).ok())
            .and_then(|arg| arg.parse::<i64>().ok())
            .unwrap_or(0)
    };
    Some(match name {
        "UNWATCH" => b"+OK\r\n".to_vec(),
        "MONITOR" => {
            crate::resp::write::resp_error("ERR MONITOR isn't allowed for DENY BLOCKING client")
        }
        // Its arguments checked as outside a transaction (#561), and refused
        // on a replica.
        "WAIT" => match crate::replication::wait_args(cmd, env.repl_state.is_slave()) {
            Err(refusal) => refusal,
            // Whatever the number asked for: the replicas that have what
            // was written before this `EXEC`, now, as Redis answers.
            Ok(_) => crate::resp::write::resp_integer(
                env.wait_targets
                    .map_or(0, crate::replication::ReplState::count_reached),
            ),
        },
        _ => {
            if !crate::aof::AOF_ENABLED.load(Ordering::Relaxed) {
                if number(1) != 0 {
                    crate::resp::write::resp_error(
                        "ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled.",
                    )
                } else {
                    b"*2\r\n:0\r\n:0\r\n".to_vec()
                }
            } else if crate::aof::AOF_FSYNCED.load(Ordering::Relaxed)
                >= crate::aof::AOF_WRITTEN.load(Ordering::Relaxed)
            {
                b"*2\r\n:1\r\n:0\r\n".to_vec()
            } else {
                b"*2\r\n:0\r\n:0\r\n".to_vec()
            }
        }
    })
}

/// A write, with the server over `maxmemory` and no policy that could make
/// room for it. (With a policy that evicts, room is made when the command
/// runs, as it is for a command outside a transaction.)
fn no_room(name: &str, cmd: &[&[u8]]) -> bool {
    crate::commands::handler::check_memory_limit()
        && crate::eviction::policy() == crate::eviction::Policy::NoEviction
        && crate::replication::command_mutates_for_replication(name, cmd)
        && !crate::eviction::allowed_when_out_of_memory(name)
}

/// The reply to an `EXEC` that cannot run its transaction: the server is
/// busy with a script, out of memory with a write among the commands
/// queued, or a master with too few good replicas (`min-replicas-to-write`)
/// with a write among them. The transaction is discarded whole, as Redis
/// discards it: none of it runs.
pub fn exec_refusal(
    cmd: &[&[u8]],
    queue: &[Vec<Vec<u8>>],
    repl_state: &crate::replication::ReplState,
) -> Option<Vec<u8>> {
    let writes = || {
        queue.iter().any(|queued| {
            let Some(cmd0) = queued.first() else {
                return false;
            };
            let parts: Vec<&[u8]> = queued.iter().map(Vec::as_slice).collect();
            let name = String::from_utf8_lossy(cmd0).to_ascii_uppercase();
            crate::replication::refused_without_replicas(&name, &parts)
        })
    };
    // `no_room` asks `check_memory_limit` first; asked here, before the
    // queue is looked at, an EXEC under the limit makes nothing per command.
    let why = if busy(cmd) {
        BUSY
    } else if crate::commands::handler::check_memory_limit()
        && queue.iter().any(|queued| {
            let Some(cmd0) = queued.first() else {
                return false;
            };
            let parts: Vec<&[u8]> = queued.iter().map(Vec::as_slice).collect();
            no_room(&String::from_utf8_lossy(cmd0).to_ascii_uppercase(), &parts)
        })
    {
        OOM
    } else if repl_state.lacks_good_replicas() && writes() {
        crate::replication::NOREPLICAS
    } else {
        return None;
    };
    Some(crate::resp::write::resp_error(&format!(
        "EXECABORT Transaction discarded because of: {why}"
    )))
}

/// Per-connection transaction state.
/// Lives on the stack in handle_client — zero heap cost when not in use.
pub struct TxState {
    /// True between MULTI and EXEC/DISCARD
    pub active: bool,
    /// Queued commands — each is the raw parsed Vec<Vec<u8>>
    pub queue: Vec<Vec<Vec<u8>>>,
    /// Keys being watched — if any are modified, EXEC returns nil
    pub watched_keys: Vec<(usize, crate::store::Key)>, // (db_index, key)
    /// For each of `watched_keys`, in the same order, whether it had already
    /// run out of time when it was watched: its time running out is then no
    /// change (Redis's `watchedKey.expired`). See [`watched_key_expired`].
    pub watched_expired: Vec<bool>,
    /// Set to true if a watched key was modified before EXEC
    pub dirty: bool,
    /// A command was refused when it was queued: EXEC answers -EXECABORT and
    /// runs nothing.
    pub aborted: bool,
}

impl TxState {
    pub fn new() -> Self {
        Self {
            active: false,
            queue: Vec::new(),
            watched_keys: Vec::new(),
            watched_expired: Vec::new(),
            dirty: false,
            aborted: false,
        }
    }

    pub fn reset(&mut self) {
        self.active = false;
        self.queue.clear();
        self.unwatch_keys();
        self.dirty = false;
        self.aborted = false;
    }

    /// Watch `key` in `db`; `expired`: whether its time had already run out.
    pub fn watch_key(&mut self, db: usize, key: &[u8], expired: bool) {
        self.watched_keys.push((db, crate::store::Key::new(key)));
        self.watched_expired.push(expired);
    }

    /// Forget the watched keys (the registry's watches are the caller's).
    pub fn unwatch_keys(&mut self) {
        self.watched_keys.clear();
        self.watched_expired.clear();
    }
}

/// Whether a watched key's time has run out since it was watched, by the
/// clock as it is now: inside `EXEC`, the transaction's frozen time
/// (`script_clock::freeze`). Such a key has been modified, whether or not
/// anything has removed it yet, and the transaction is refused, as Redis's
/// `execCommand` refuses it (`isWatchedKeyExpired`, which reads the same
/// `commandTimeSnapshot` the queued commands then read). Read by the time the
/// commands will run under, a key cannot be alive for the check and gone for
/// the commands: it is one or the other for both (#659).
///
/// A key that had already run out when it was watched is skipped, as Redis
/// skips it: on a replica it stays until its master's `DEL` comes. Nothing is
/// deleted here.
///
/// A key not past its time when it was watched may be marked so since: a
/// `SWAPDB` that put an expired key where it was absent marks it, as Redis's
/// `touchAllWatchedKeysInDb` does. The registry (`watches`, the client's
/// `flag`) is asked, only for a key found past its time. The marks the
/// registry clears are of keys that have gone, which are not past their
/// time here.
pub fn watched_key_expired(
    store: &crate::store::Store,
    tx: &TxState,
    watches: &dyn crate::watch::WatchNotifier,
    flag: &Arc<std::sync::atomic::AtomicBool>,
) -> bool {
    tx.watched_keys
        .iter()
        .zip(&tx.watched_expired)
        .any(|((db, key), &was)| {
            !was && crate::commands::helpers::is_expired(store, *db, key)
                && !watches.marked_expired(*db, key, flag)
        })
}

/// Where a refused `EXEC` is shown to MONITOR: see [`reject`].
pub struct Monitored<'a> {
    pub registry: &'a crate::monitor::SharedMonitorRegistry,
    pub db: usize,
    pub peer_addr: &'a str,
}

/// A command refused before it ran — no such command, the wrong number of
/// arguments, not allowed inside MULTI, not allowed to the user — as Redis's
/// `rejectCommand` refuses it. Inside MULTI the transaction is failed, and
/// `EXEC` will refuse it. A refused `EXEC` itself discards the transaction,
/// watches and all, and says why: `-EXECABORT Transaction discarded because
/// of: <the refusal>`, and MONITOR sees it, as it sees an `EXEC` that ran.
/// `refusal` is the error reply; what is returned is the reply to send.
pub fn reject(
    tx: &mut TxState,
    cmd: &[&[u8]],
    watches: &dyn crate::watch::WatchNotifier,
    dirty_flag: &Arc<std::sync::atomic::AtomicBool>,
    monitor: Monitored<'_>,
    refusal: Vec<u8>,
) -> Vec<u8> {
    let is_exec = cmd
        .first()
        .is_some_and(|name| name.eq_ignore_ascii_case(b"EXEC"));
    if !is_exec {
        if tx.active {
            tx.aborted = true;
        }
        return refusal;
    }
    watches.unwatch_all(
        &tx.watched_keys
            .iter()
            .map(|_| Arc::clone(dirty_flag))
            .collect::<Vec<_>>(),
    );
    tx.reset();
    dirty_flag.store(false, Ordering::Release);
    crate::monitor::feed(monitor.registry, cmd, monitor.db, monitor.peer_addr);
    let why = String::from_utf8_lossy(&refusal);
    crate::resp::write::resp_error(&format!(
        "EXECABORT Transaction discarded because of: {}",
        why.trim_start_matches('-').trim_end()
    ))
}

impl Default for TxState {
    fn default() -> Self {
        Self::new()
    }
}
