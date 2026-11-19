//! Room under `maxmemory` for the commands of the probabilistic types that
//! allocate as much as their arguments, or the value they grow or copy, say.
//!
//! Eviction deletes keys as writes, in the order of writes, and so comes
//! before a command takes its own keys' order (`make_room_or_refuse` in
//! `commands::handler`): from inside a command it would delete keys whose
//! order it does not hold, and a write to one of them could reach replicas
//! on the other side of the `DEL`. So such a command is asked what it
//! wants before it runs ([`wanted`]), once it has passed every refusal a
//! command can meet before it runs (routing, `BUSY`, `NOREPLICAS`,
//! `LOADING`, the ACL): its handler is run with [`asking`] set, as far as
//! its call to [`check`], which keeps what the command would allocate and
//! stops it there, before it has taken a lock or changed anything. Room is
//! made for that where it is made for every command. When the command runs,
//! [`check`] only looks: a command whose room has gone meanwhile is refused
//! with `OOM`. Nothing in a handler evicts.
//!
//! A handler asked what it wants either reaches [`check`] or refuses the
//! command before it gets there, with an error, having changed nothing. One
//! that answers otherwise without reaching [`check`] could have written
//! outside the write gate and its keys' order, and nothing of it would reach
//! replicas or the AOF: [`wanted`] then refuses the command, logs it as a
//! bug, and fails a debug build.
//!
//! `RESTORE` of a probabilistic value is asked the same way: the size its
//! payload declares, read before anything of that size is allocated
//! ([`declared`]).

use std::cell::Cell;
use std::sync::atomic::Ordering;

use crate::commands::ctx::Ctx;

/// Where the handler being asked what it wants has got to.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Asked {
    /// Not asking.
    No,
    /// Asking, and [`check`] not reached yet.
    Asking,
    /// [`check`] reached: what it said.
    Said(u64),
}

thread_local! {
    static ASKED: Cell<Asked> = const { Cell::new(Asked::No) };
    /// For a client's `RESTORE` under `maxmemory`: [`declared`] sizes are
    /// held to it, and whether one was refused.
    static LOADING_FOR_CLIENT: Cell<Option<bool>> = const { Cell::new(None) };
}

/// The commands that may allocate as much as their arguments, or the value
/// they grow or copy, say: those to be asked what they want ([`wanted`]).
/// The dispatch finds them in its arity lookup (`arity::check_dispatch`).
/// `CF.DEL` and `CF.COMPACT` free memory, but copy what they change first
/// if a snapshot or a copy shares it.
pub const ALLOCATES: &[&str] = &[
    "BF.ADD",
    "BF.INSERT",
    "BF.LOADCHUNK",
    "BF.MADD",
    "BF.RESERVE",
    "CMS.INCRBY",
    "CMS.INITBYDIM",
    "CMS.INITBYPROB",
    "CMS.MERGE",
    "TOPK.ADD",
    "TOPK.INCRBY",
    "TOPK.RESERVE",
    "CF.ADD",
    "CF.ADDNX",
    "CF.COMPACT",
    "CF.DEL",
    "CF.INSERT",
    "CF.INSERTNX",
    "CF.LOADCHUNK",
    "CF.RESERVE",
    "TDIGEST.CREATE",
    "TDIGEST.MERGE",
    "RESTORE",
];

/// What the command in `ctx`, named `name` (one of [`ALLOCATES`]), would
/// allocate: 0 if it would be refused before it got that far. `Err` is the
/// reply to refuse it with: its handler answered without reaching
/// [`check`] (see the module's notes). Asked by the dispatch, under
/// `maxmemory`, before the command takes its keys' order.
#[cold]
#[inline(never)]
pub fn wanted(ctx: &mut Ctx, name: &str) -> Result<u64, Vec<u8>> {
    #[cfg(debug_assertions)]
    if not_asked_for_test() {
        return Ok(0);
    }
    let _asking = Asking::begin();
    if name == "RESTORE" {
        crate::commands::restore_wanted(ctx);
        return Ok(match ASKED.get() {
            Asked::Said(bytes) => bytes,
            _ => 0,
        });
    }
    let reply = crate::commands::bf::handle(ctx, name);
    match ASKED.get() {
        Asked::Said(bytes) => Ok(bytes),
        // Refused before it got to its allocation: nothing changed.
        _ if reply.as_ref().is_some_and(|r| r.first() == Some(&b'-')) => Ok(0),
        _ => {
            crate::log_error!(
                "{name} answered without being asked what it allocates (a bug in its handler): \
                 the command is refused"
            );
            debug_assert!(false, "{name} was not asked what it allocates");
            Err(crate::resp::write::resp_error(&format!(
                "ERR internal error: {name} could not be sized under maxmemory; the command was \
                 not run"
            )))
        }
    }
}

/// Asking, until dropped: however the handler asked leaves, a panic
/// included, the next command on this thread is not taken to be asked.
struct Asking;

impl Asking {
    fn begin() -> Asking {
        ASKED.set(Asked::Asking);
        Asking
    }
}

impl Drop for Asking {
    fn drop(&mut self) {
        ASKED.set(Asked::No);
    }
}

/// Whether the handler running is only being asked what it wants: it must
/// not lock or change anything.
pub fn asking() -> bool {
    ASKED.get() != Asked::No
}

/// Refused by [`check`].
pub struct NoRoom;

/// In a handler of one of [`ALLOCATES`], once it knows what it would allocate
/// and before it takes a lock: `Err` if it is only being asked (it has now
/// said what: `bytes`), or if it is a command from a client and `bytes` do
/// not fit under `maxmemory`. A command from a master or the AOF is never
/// held to `maxmemory`, nor is a replica, as no write on one is.
pub fn check(ctx: &Ctx, bytes: impl FnOnce(&Ctx) -> u64) -> Result<(), NoRoom> {
    if asking() {
        ASKED.set(Asked::Said(bytes(ctx)));
        return Err(NoRoom);
    }
    if fits(ctx, bytes) {
        Ok(())
    } else {
        Err(NoRoom)
    }
}

/// Whether `bytes` more fit under `maxmemory` for the command in `ctx`
/// (always for one that is not held to it), looked at as it runs: for an
/// allocation a command makes past what it was given room for, which is
/// then answered for that item alone.
///
/// Nothing (0 bytes) always fits: a command that allocates nothing, such as
/// `CF.DEL` of a filter nothing shares, which frees, is not refused here
/// over the limit. (One that may add to memory use is refused over the
/// limit before it runs, by `make_room_or_refuse`, as before.)
pub fn fits(ctx: &Ctx, bytes: impl FnOnce(&Ctx) -> u64) -> bool {
    let from_client = !ctx.apply_replication && !ctx.repl_state.is_slave();
    if !from_client || crate::RUNTIME_MAXMEMORY.load(Ordering::Relaxed) == 0 {
        return true;
    }
    let bytes = bytes(ctx);
    bytes == 0 || super::room_for(bytes)
}

/// Run `f` (a client's `RESTORE`), holding the probabilistic values it
/// reads to `maxmemory` by the sizes they declare ([`declared`]); and
/// whether one was refused for that.
pub fn loading_for_client<T>(ctx: &Ctx, f: impl FnOnce() -> T) -> (T, bool) {
    let held = !ctx.apply_replication
        && !ctx.repl_state.is_slave()
        && crate::RUNTIME_MAXMEMORY.load(Ordering::Relaxed) != 0;
    if !held {
        return (f(), false);
    }
    struct Done;
    impl Drop for Done {
        fn drop(&mut self) {
            LOADING_FOR_CLIENT.set(None);
        }
    }
    let _done = Done;
    LOADING_FOR_CLIENT.set(Some(false));
    let out = f();
    (out, LOADING_FOR_CLIENT.get() == Some(true))
}

/// In a reader of a probabilistic value, once its shape says how much it
/// will allocate (`bytes`) and before it allocates any of it: an error if
/// it is a client's `RESTORE` only being asked what it wants (it has now
/// said), or one under `maxmemory` that `bytes` do not fit. Loading a file,
/// a full sync or the AOF is never held to it.
pub fn declared(bytes: u64) -> std::io::Result<()> {
    let refused = || {
        std::io::Error::new(
            std::io::ErrorKind::OutOfMemory,
            "no room under maxmemory for the value",
        )
    };
    if asking() {
        ASKED.set(Asked::Said(bytes));
        return Err(refused());
    }
    if LOADING_FOR_CLIENT.get().is_some() && !super::room_for(bytes) {
        LOADING_FOR_CLIENT.set(Some(true));
        return Err(refused());
    }
    Ok(())
}

/// [`declared`] for a value read in parts, each part's `bytes` as its
/// header gives them (a Bloom filter's layers): a client's `RESTORE` under
/// `maxmemory` is held to each part as it is read. Only asked what it
/// wants, it adds the part to what it has said and is to skip the part's
/// bytes (`Ok(true)`), so that it says the whole at the end
/// ([`declared_end`]).
pub fn declared_part(bytes: u64) -> std::io::Result<bool> {
    match ASKED.get() {
        Asked::No => declared(bytes).map(|()| false),
        Asked::Asking => {
            ASKED.set(Asked::Said(bytes));
            Ok(true)
        }
        Asked::Said(before) => {
            ASKED.set(Asked::Said(before.saturating_add(bytes)));
            Ok(true)
        }
    }
}

/// After the parts of a value read in parts ([`declared_part`]): if only
/// asked, the reading stops here, having said the whole.
pub fn declared_end() -> std::io::Result<()> {
    if asking() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::OutOfMemory,
            "no room under maxmemory for the value",
        ));
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Being asked is over once the asking is, however it ends: a panic in
    /// the handler asked included.
    #[test]
    fn asking_ends_with_a_panic_too() {
        let caught = std::panic::catch_unwind(|| {
            let _asking = Asking::begin();
            assert!(asking());
            panic!("in the handler");
        });
        assert!(caught.is_err());
        assert!(!asking());
    }

    /// A reader asked what it would allocate says so and stops; one not
    /// asked, outside a client's `RESTORE`, is never held to anything.
    #[test]
    fn declared_sizes() {
        assert!(declared(u64::MAX).is_ok());
        ASKED.set(Asked::Asking);
        assert!(declared(1234).is_err());
        assert_eq!(ASKED.get(), Asked::Said(1234));
        ASKED.set(Asked::No);
    }

    /// A value read in parts, asked: the parts add up, and the reading
    /// stops at the end; not asked, each part goes on.
    #[test]
    fn parts_add_up_when_asked() {
        assert!(!declared_part(10).unwrap());
        assert!(declared_end().is_ok());
        let _asking = Asking::begin();
        assert!(declared_part(10).unwrap());
        assert!(declared_part(32).unwrap());
        assert!(declared_end().is_err());
        assert_eq!(ASKED.get(), Asked::Said(42));
    }
}
