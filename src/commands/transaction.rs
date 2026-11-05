// MULTI / EXEC / DISCARD / WATCH / UNWATCH
//
// Transaction state lives in server.rs on the connection stack.
// This module only handles the commands that don't need queuing —
// MULTI, DISCARD, WATCH, UNWATCH. EXEC is handled in server.rs
// because it needs to execute the queued commands directly.
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
    // the connection over.
    if matches!(
        name,
        "SAVE" | "SHUTDOWN" | "BGREWRITEAOF" | "FAILOVER" | "SYNC" | "PSYNC" | "MONITOR"
    ) {
        return Some(crate::resp::write::resp_error(
            "ERR Command not allowed inside a transaction",
        ));
    }
    if busy(cmd) {
        return Some(crate::resp::write::resp_error(BUSY));
    }
    no_room(name, cmd[0]).then(|| crate::resp::write::resp_error(OOM))
}

const OOM: &str = "OOM command not allowed when used memory > 'maxmemory'.";

/// A write, with the server over `maxmemory` and no policy that could make
/// room for it. (With a policy that evicts, room is made when the command
/// runs, as it is for a command outside a transaction.)
fn no_room(name: &str, cmd0: &[u8]) -> bool {
    crate::commands::handler::check_memory_limit()
        && crate::eviction::policy() == crate::eviction::Policy::NoEviction
        && crate::replication::is_write_command_bytes(cmd0)
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
            queued
                .first()
                .is_some_and(|cmd0| crate::replication::is_write_command_bytes(cmd0))
        })
    };
    let why = if busy(cmd) {
        BUSY
    } else if queue.iter().any(|queued| {
        queued
            .first()
            .is_some_and(|cmd0| no_room(&String::from_utf8_lossy(cmd0).to_ascii_uppercase(), cmd0))
    }) {
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
    pub watched_keys: Vec<(usize, String)>, // (db_index, key)
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
            dirty: false,
            aborted: false,
        }
    }

    pub fn reset(&mut self) {
        self.active = false;
        self.queue.clear();
        self.watched_keys.clear();
        self.dirty = false;
        self.aborted = false;
    }
}

impl Default for TxState {
    fn default() -> Self {
        Self::new()
    }
}
