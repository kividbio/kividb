// src/expiry.rs
//
// What happens around a key whose time to live ends, beyond taking it out of
// the keyspace and the expiry store: who is told, and when a key's time alone
// does not make it gone.
//
// A key's expiry used to be nobody's business but its own node's. The master
// logged nothing and propagated nothing when it expired one, and every reader
// of its write stream, a replica or the AOF being replayed at startup, expired
// keys again by its own clock. That gave two different datasets:
//
// * `SET k 5 PXAT t` then `INCR k`, made while the key lived, replayed after
//   `t`: the replay's INCR found no key (it was past its time) and made one,
//   `1`, with no time to live, for good.
// * A replica expired keys when its own sweep came round to them, while its
//   master's writes to the same keys were still on their way, and the two
//   drifted apart.
//
// So, as in Redis (`deleteExpiredKeyAndPropagate`):
//
// * The master writes a `DEL` (`UNLINK` under `lazyfree-lazy-expire yes`, the
//   default) into its AOF and its replication stream for every key it
//   expires: one a read or a write finds past its time
//   (`helpers::delete_key_if_expired`; a write expires the keys it names
//   first, in `handler`), one active expiry takes out
//   (`helpers::expire_batch`). Each holds what a write holds while it does
//   (`order`), so the `DEL` falls where the deletion did among the writes to
//   the key, and a cut never falls between the two.
// * While a dataset is loaded (the AOF replayed; a snapshot under it, or one a
//   replica is sent) or a master's stream is applied, a key's time does not
//   make it gone (`held`). What expired there is said by the `DEL` that
//   follows. Active expiry waits while the server loads its dataset at
//   startup (`loading`).
// * A replica deletes no expired key itself. Its clients are answered as if
//   one were gone, and the key stays until its master's `DEL` arrives
//   (`on_replica`). Nor does it propagate anything: a replica's stream is its
//   master's.

use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::RwLock;

use crate::replication::{HubTx, ReplState};

/// Where a store's expired keys are told: its node's replication state and
/// hub. Registered by the server that owns the store.
#[derive(Clone)]
pub struct Target {
    repl_state: Arc<ReplState>,
    hub_tx: HubTx,
}

impl Target {
    /// This node is a replica: it deletes no expired key of its own.
    #[inline]
    pub fn on_replica(&self) -> bool {
        self.repl_state.is_slave()
    }

    /// Whether a key this node expires has to be told to anyone: it is a
    /// master, and something reads its write stream (the AOF, a replica or
    /// the backlog kept for one, a tap).
    #[inline]
    pub fn observed(&self) -> bool {
        !self.repl_state.is_slave()
            && (crate::aof::AOF_ENABLED.load(Ordering::Relaxed)
                || self.repl_state.feeds_hub()
                || self.repl_state.stream_taps.load(Ordering::Relaxed) > 0)
    }

    /// `keys`, which this master has just expired in database `db`, as a
    /// `DEL` (or `UNLINK`) each to replicas, taps and the AOF. Inside a
    /// script, its AOF records go with the script's. The caller holds what
    /// [`order`] takes.
    pub fn propagate<'a>(&self, db: usize, keys: impl IntoIterator<Item = &'a str>) {
        let command: &[u8] = if lazy_expire() { b"UNLINK" } else { b"DEL" };
        for key in keys {
            crate::replication::replicate_expired(
                &self.repl_state,
                &self.hub_tx,
                db,
                &[command.to_vec(), key.as_bytes().to_vec()],
            );
        }
    }
}

/// `lazyfree-lazy-expire`: an expired key is propagated as `UNLINK`.
fn lazy_expire() -> bool {
    crate::commands::server::remembered_config("lazyfree-lazy-expire")
        .is_some_and(|v| v.eq_ignore_ascii_case("yes"))
}

static TARGETS: RwLock<Vec<(usize, Target)>> = RwLock::new(Vec::new());

fn store_id(store: &crate::store::Store) -> usize {
    Arc::as_ptr(store) as *const () as usize
}

/// Make `repl_state` and `hub_tx` where `store`'s expired keys are told.
pub fn register(store: &crate::store::Store, repl_state: &Arc<ReplState>, hub_tx: &HubTx) {
    let id = store_id(store);
    let mut targets = TARGETS.write();
    targets.retain(|(known, _)| *known != id);
    targets.push((
        id,
        Target {
            repl_state: Arc::clone(repl_state),
            hub_tx: hub_tx.clone(),
        },
    ));
}

/// Where `store`'s expired keys are told. `None` for a store no server
/// registered (one a unit test made, a replica's staging store): nobody to
/// tell.
pub fn of(store: &crate::store::Store) -> Option<Target> {
    let id = store_id(store);
    TARGETS
        .read()
        .iter()
        .find(|(known, _)| *known == id)
        .map(|(_, target)| target.clone())
}

/// What a write holds while it changes the store and queues its record:
/// the write gate, so that a cut (a full resync, a checkpoint) falls before
/// or after both; and the order of its keys, so that its record falls where
/// its change did among the other writes to them. Both nest: a command that
/// holds them already takes nothing more.
pub struct Ordered {
    _order: crate::write_order::OrderGuard,
    _gate: crate::write_gate::WriteGuard,
}

/// Take what a write to `keys` of database `db` holds ([`Ordered`]).
pub fn order<'a>(db: usize, keys: impl Iterator<Item = &'a [u8]>) -> Ordered {
    let gate = crate::write_gate::enter();
    let order = if crate::command_gate::alone_on_this_thread() {
        crate::write_order::held_exclusively()
    } else {
        crate::write_order::lock_keys(db, keys)
    };
    Ordered {
        _order: order,
        _gate: gate,
    }
}

thread_local! {
    /// How many [`Hold`]s this thread has.
    static HELD: Cell<u32> = const { Cell::new(0) };
}

/// While this lives, no key expires on this thread ([`held`]).
pub struct Hold {
    _not_send: PhantomData<*const ()>,
}

impl Drop for Hold {
    fn drop(&mut self) {
        HELD.with(|held| held.set(held.get().saturating_sub(1)));
    }
}

/// From now until the [`Hold`] is dropped, a key's time does not make it
/// gone on this thread: what a thread loading a dataset, or applying its
/// master's stream, holds. Nests.
pub fn hold() -> Hold {
    HELD.with(|held| held.set(held.get() + 1));
    Hold {
        _not_send: PhantomData,
    }
}

/// Whether this thread is loading a dataset or applying its master's
/// stream: no key expires on it (see [`hold`]).
#[inline]
pub fn held() -> bool {
    HELD.with(|held| held.get() > 0)
}

/// Datasets being loaded into a live store: while there are any, active
/// expiry waits. The threads that expire keys are not the one loading, so a
/// [`Hold`] does not reach them: a key loaded past its time could be taken
/// out between the replay's `SET` of it and its `INCR`, and the `INCR` would
/// then make it again, with no time to live.
static LOADING: AtomicUsize = AtomicUsize::new(0);

/// While this lives, active expiry does nothing ([`loading`]), and no key
/// expires on this thread ([`Hold`]).
pub struct Loading {
    _hold: Hold,
}

impl Drop for Loading {
    fn drop(&mut self) {
        LOADING.fetch_sub(1, Ordering::Release);
    }
}

/// A dataset is being loaded into the server's store on this thread: from
/// its snapshot, its AOF. Nests.
pub fn loading_guard() -> Loading {
    LOADING.fetch_add(1, Ordering::Acquire);
    Loading { _hold: hold() }
}

/// Whether a dataset is being loaded into the server's store
/// ([`loading_guard`]): active expiry waits for it.
#[inline]
pub fn loading() -> bool {
    LOADING.load(Ordering::Acquire) > 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_hold_nests_and_is_this_threads() {
        assert!(!held());
        let outer = hold();
        let inner = hold();
        assert!(held());
        std::thread::spawn(|| assert!(!held())).join().unwrap();
        drop(inner);
        assert!(held());
        drop(outer);
        assert!(!held());
    }

    #[test]
    fn loading_holds_this_thread_and_is_seen_by_others() {
        let guard = loading_guard();
        assert!(held());
        assert!(std::thread::spawn(loading).join().unwrap());
        drop(guard);
        assert!(!held());
    }

    #[test]
    fn a_store_nobody_registered_has_nobody_to_tell() {
        let store = crate::store::new_store(1);
        assert!(of(&store).is_none());
    }
}
