//! Room under `maxmemory` for the commands of the probabilistic types that
//! allocate as much as their arguments, or the value they grow, say.
//!
//! Eviction deletes keys as writes, in the order of writes, and so comes
//! before a command takes its own keys' order (`make_room_or_refuse` in
//! `commands::handler`): from inside a command it would delete keys whose
//! order it does not hold, and a write to one of them could reach replicas
//! on the other side of the `DEL`. So such a command is asked what it
//! wants before it runs ([`wanted`]): its handler is run with [`asking`]
//! set, as far as its call to [`check`], which keeps what the command would
//! allocate and stops it there, before it has taken a lock or changed
//! anything. Room is made for that where it is made for every command.
//! When the command runs, [`check`] only looks: a command whose room has
//! gone meanwhile is refused with `OOM`. Nothing in a handler evicts.

use std::cell::Cell;
use std::sync::atomic::Ordering;

use crate::commands::ctx::Ctx;

thread_local! {
    /// While a handler is asked what it wants: what it said, so far.
    static ASKED: Cell<Option<u64>> = const { Cell::new(None) };
}

/// Whether `name` is a command that may allocate as much as its arguments,
/// or the value it grows, say: one to be asked what it wants ([`wanted`]).
pub fn allocates(name: &str) -> bool {
    matches!(
        name,
        "BF.ADD"
            | "BF.INSERT"
            | "BF.LOADCHUNK"
            | "BF.MADD"
            | "BF.RESERVE"
            | "TDIGEST.CREATE"
            | "TDIGEST.MERGE"
    )
}

/// The bytes the command in `ctx`, named `name` (one that [`allocates`]),
/// would allocate: 0 if it would be refused before it got that far. Asked
/// by the dispatch, under `maxmemory`, before the command takes its keys'
/// order.
#[cold]
#[inline(never)]
pub fn wanted(ctx: &mut Ctx, name: &str) -> u64 {
    #[cfg(debug_assertions)]
    if not_asked_for_test() {
        return 0;
    }
    ASKED.set(Some(0));
    let _ = crate::commands::bf::handle(ctx, name);
    ASKED.take().unwrap_or(0)
}

/// Whether the handler running is only being asked what it wants: it must
/// not lock or change anything.
pub fn asking() -> bool {
    ASKED.get().is_some()
}

/// Refused by [`check`].
pub struct NoRoom;

/// In a handler that [`allocates`], once it knows what it would allocate
/// and before it takes a lock: `Err` if it is only being asked (it has now
/// said what: `bytes`), or if it is a command from a client and `bytes` do
/// not fit under `maxmemory`. A command from a master or the AOF is never
/// held to `maxmemory`, nor is a replica, as no write on one is.
pub fn check(ctx: &Ctx, bytes: impl FnOnce(&Ctx) -> u64) -> Result<(), NoRoom> {
    if asking() {
        ASKED.set(Some(bytes(ctx)));
        return Err(NoRoom);
    }
    let from_client = !ctx.apply_replication && !ctx.repl_state.is_slave();
    if from_client
        && crate::RUNTIME_MAXMEMORY.load(Ordering::Relaxed) != 0
        && !super::room_for(bytes(ctx))
    {
        return Err(NoRoom);
    }
    Ok(())
}

/// `KIVIDB_TEST_NO_ROOM_BEFORE`, never set otherwise, and read in debug
/// builds only: commands are not asked what they want, so that a test can
/// see one find no room when it runs.
#[cfg(debug_assertions)]
fn not_asked_for_test() -> bool {
    static SET: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *SET.get_or_init(|| std::env::var_os("KIVIDB_TEST_NO_ROOM_BEFORE").is_some())
}
