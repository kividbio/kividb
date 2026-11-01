// src/expiry.rs
//
// What happens around a key whose time to live ends, beyond taking it out of
// its shard: who is told, and when a key's time alone does not make it gone.
//
// A key's expiry used to be nobody's business but its own node's. The master
// logged nothing and propagated nothing when it expired one, and every reader
// of its write stream, a replica or the AOF being replayed at startup, expired
// keys again by its own clock. That gave two different datasets:
//
// * `SET k 5 PXAT t` then `INCR k`, made while the key lived, replayed after
//   `t`: the replay's INCR found no key (it was past its time) and made one,
//   `1`, with no time to live, for good. 100 keys of 100 came back so.
// * A replica expired keys when its own sweep came round to them, while its
//   master's writes to the same keys were still on their way: 572 keys
//   differed after 15 s of writes to expiring keys, and 170 of 200 behind a
//   replica that lagged.
//
// So, as in Redis (`deleteExpiredKeyAndPropagate`):
//
// * The master writes a `DEL` (`UNLINK` under `lazyfree-lazy-expire yes`, the
//   default) into its AOF and its replication stream for every key it
//   expires: one a read finds past its time (`helpers::delete_key_if_expired`),
//   one a write puts a new value over (`written_over`, before the write's own
//   record), one active expiry takes out (`helpers::expire_batch`). Each holds
//   what a write holds while it does (`order`), so the `DEL` falls where the
//   deletion did among the writes to the key, and a cut never falls between
//   the two.
// * While a dataset is loaded (the AOF replayed; a snapshot under it, or one a
//   replica is sent) or a master's stream is applied, a key's time does not
//   make it gone (`held`). What expired there is said by the `DEL` that
//   follows.
// * Nor does active expiry run while a dataset is loaded (`Loading`): it
//   runs on threads of its own, started before the load, and it took a key
//   out between the replay's `SET k 5 PXAT t` and its `INCR k`, which then
//   made the key again. Nor in the middle of a transaction or a script,
//   which is one step: it takes the command gate, as a command does.
// * A replica deletes no expired key itself. Its clients are answered as if
//   one were gone, and the key stays until its master's `DEL` arrives
//   (`on_replica`). Nor does it propagate anything: a replica's stream is its
//   master's.
// * Except under a master that sends no such `DEL` (KiviDB 1.1.2 and
//   before, followed during an upgrade that does replicas first): there the
//   replica expires keys by its own clock, and applies its master's stream
//   without a hold, as every replica did before (`on_replica`, #231). And a
//   replica that goes on to a master that sends them, by a partial resync,
//   first deletes the keys whose time ended before that master started,
//   which nobody would send a `DEL` of
//   (`commands::helpers::expire_keys_due_before`).

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
    /// This node is a replica of a master that sends the deletion of every
    /// key it expires: it deletes no expired key of its own. A replica of a
    /// master that sends none (KiviDB before 1.1.3) expires keys itself, as
    /// every replica did then, and tells no one ([`Target::observed`]).
    #[inline]
    pub fn on_replica(&self) -> bool {
        self.repl_state.is_slave()
            && self
                .repl_state
                .master_propagates_expiry
                .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Whether a key this node expires has to be told to anyone: it is a
    /// master, and something reads its write stream (the AOF, a replica or
    /// the backlog kept for one, a tap). As `replication::write_is_observed`
    /// for a master's own write.
    #[inline]
    pub fn observed(&self) -> bool {
        !self.repl_state.is_slave()
            && (crate::aof::AOF_ENABLED.load(std::sync::atomic::Ordering::Relaxed)
                || self.repl_state.feeds_hub()
                || self
                    .repl_state
                    .stream_taps
                    .load(std::sync::atomic::Ordering::Relaxed)
                    > 0)
    }

    /// `keys`, which this master has just expired in database `db`, as a
    /// `DEL` (or `UNLINK`) each to replicas, taps and the AOF. Inside a
    /// transaction or a script, among its writes. The caller holds what
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
/// registered (one a unit test made): nobody to tell.
pub fn of(store: &crate::store::Store) -> Option<Target> {
    of_id(store_id(store))
}

/// [`of`], by the store's address.
pub fn of_id(id: usize) -> Option<Target> {
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
///
/// Never entered for the first time with a hash tag's lock held: whoever
/// takes one holds the write gate already (`tag_locks`), a read too. A
/// read that took a tag and then entered the gate here, to delete a key it
/// found past its time, hung behind a checkpoint that waited for a writer
/// waiting for the tag.
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

/// Keys of database `db` of the store at `store` that writes put new values
/// over, which were past their time: each expired, as Redis expires a key a
/// write finds so, before the write's own effects. Counted already (by the
/// shard, as it found them); here announced, the clients watching or
/// caching them told, their documents taken out of the indexes, and the
/// `DEL` of each propagated. Called once the shard is let go of, by the
/// write still holding what it holds for its own record ([`order`]).
#[cold]
#[inline(never)]
pub fn written_over(store: usize, db: usize, keys: &[String]) {
    crate::keyspace_events::written_over_expired(store, db, keys);
    for key in keys {
        crate::watch::expired_in(store, db, key);
        crate::tracking::expired_in(store, key);
        crate::vector::unindex_expired_in(store, db, key);
    }
    if let Some(target) = of_id(store).filter(Target::observed) {
        let _ordered = order(db, keys.iter().map(|key| key.as_bytes()));
        target.propagate(db, keys.iter().map(String::as_str));
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

/// Datasets being loaded, on any thread: active expiry deletes nothing
/// meanwhile ([`loading`]).
static LOADING: AtomicUsize = AtomicUsize::new(0);

/// While this lives, a dataset is being loaded: no key is swept, on any
/// thread. [`hold`] is the loading thread's own; the sweep runs on threads
/// of its own, from before the load.
pub struct Loading(());

impl Loading {
    /// Count a load in until the guard is dropped. Nests.
    pub fn start() -> Loading {
        LOADING.fetch_add(1, Ordering::SeqCst);
        Loading(())
    }
}

impl Drop for Loading {
    fn drop(&mut self) {
        LOADING.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Whether a dataset is being loaded ([`Loading`]).
#[inline]
pub fn loading() -> bool {
    LOADING.load(Ordering::SeqCst) > 0
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
    fn loading_nests_and_is_every_threads() {
        let outer = Loading::start();
        let inner = Loading::start();
        assert!(std::thread::spawn(loading).join().unwrap());
        drop(inner);
        assert!(loading());
        drop(outer);
    }

    #[test]
    fn a_store_nobody_registered_has_nobody_to_tell() {
        let store = crate::store::new_store(1);
        assert!(of(&store).is_none());
    }
}

/// What a master tells for a key that goes for its time, and what a
/// replica, or a thread loading a dataset or applying its master's stream,
/// does with one: through the hub's queue, as a replica's stream gets it.
///
/// Adapted from Parikalp Bhardwaj's tests of his design of the same fix
/// (his `replication::expiry`): his `Applying` is [`hold`] here, his
/// `store_follows_master` is [`Target::on_replica`], and the sweep is given
/// its [`Target`] as the expiry threads give it.
#[cfg(test)]
mod propagation_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use tokio::sync::mpsc;

    use crate::commands::helpers::{
        ExpireCursor, active_expire_cycle, delete_key_if_expired, expire_on_read, is_expired,
        now_ms,
    };
    use crate::replication::{HubMsg, ReplState};
    use crate::shared_bytes::SharedBytes;
    use crate::store::{self, Store, ValueRef};
    use crate::types::KiviDBValue;
    use crate::vector::IndexStore;

    /// A store whose expired keys are told to a hub queue, as with a
    /// replica attached. Never freed: a store is found by its address
    /// (`of`), which a store freed here could hand to another test's.
    fn server() -> (Store, Arc<ReplState>, mpsc::UnboundedReceiver<HubMsg>) {
        let store = store::new_store(2);
        std::mem::forget(Arc::clone(&store));
        let repl_state = Arc::new(ReplState::new_master());
        repl_state.backlog_active.store(true, Ordering::Relaxed);
        let (hub_tx, hub_rx) = mpsc::unbounded_channel();
        super::register(&store, &repl_state, &hub_tx);
        (store, repl_state, hub_rx)
    }

    /// Everything in the stream so far, as one text.
    fn stream(rx: &mut mpsc::UnboundedReceiver<HubMsg>) -> String {
        let mut out = String::new();
        while let Ok(msg) = rx.try_recv() {
            if let HubMsg::Propagate(payload) = msg {
                out.push_str(&String::from_utf8_lossy(&payload));
            }
        }
        out
    }

    fn command(parts: &[&str]) -> String {
        let parts: Vec<Vec<u8>> = parts.iter().map(|p| p.as_bytes().to_vec()).collect();
        String::from_utf8(crate::resp::write::encode_resp_command(&parts)).unwrap()
    }

    /// `key` = `5`, long past its time.
    fn stale(store: &Store, db: usize, key: &str) {
        drop(store::get_shard(&store[db], key).write().insert_expiring(
            key,
            KiviDBValue::String(SharedBytes::from(&b"5"[..])),
            Some(1),
        ));
    }

    /// What `key` holds, past its time or not.
    fn text(store: &Store, db: usize, key: &str) -> Option<Vec<u8>> {
        match store::get_shard(&store[db], key).read().peek_expiring(key) {
            Some((ValueRef::String(b), _)) => Some(b.to_vec()),
            _ => None,
        }
    }

    fn with_ttl(store: &Store) -> usize {
        store[0].iter().map(|s| s.read().ttl_len()).sum()
    }

    #[test]
    fn a_read_that_finds_a_key_past_its_time_sends_its_deletion_once() {
        let (store, _repl, mut rx) = server();
        stale(&store, 0, "k");
        assert!(delete_key_if_expired(&store, 0, "k"));
        assert!(!delete_key_if_expired(&store, 0, "k"), "gone already");
        let sent = stream(&mut rx);
        assert_eq!(
            sent.matches(&command(&["UNLINK", "k"])).count(),
            1,
            "{sent:?}"
        );
        assert!(!sent.contains("DEL\r\n"), "{sent:?}");
    }

    /// Told once the shard is let go of, and before anything after the
    /// write: with the write still holding the key's order, so that its
    /// own record follows the `DEL` (`written_over`).
    #[test]
    fn a_write_over_a_key_past_its_time_sends_its_deletion_once_it_lets_go() {
        let (store, _repl, mut rx) = server();
        stale(&store, 0, "k");
        let shard = store::get_shard(&store[0], "k");
        let mut map = shard.write();
        drop(map.insert_str("k", b"1"));
        assert!(stream(&mut rx).is_empty(), "not before the lock lets go");
        drop(map);
        // After the `SELECT 0` that opens the stream.
        assert_eq!(
            stream(&mut rx),
            format!("{}{}", command(&["SELECT", "0"]), command(&["UNLINK", "k"]))
        );
        assert_eq!(text(&store, 0, "k"), Some(b"1".to_vec()));
        assert_eq!(shard.read().expires("k"), None);
        // A live key written over sends nothing.
        drop(shard.write().insert_expiring(
            "k",
            KiviDBValue::String(SharedBytes::from(&b"2"[..])),
            Some(now_ms() + 3_600_000),
        ));
        drop(shard.write().insert_str("k", b"3"));
        assert!(stream(&mut rx).is_empty());
    }

    #[test]
    fn the_sweep_sends_each_deletion_once() {
        let (store, _repl, mut rx) = server();
        let index = IndexStore::default();
        let keys: Vec<String> = (0..50).map(|i| format!("gone:{i}")).collect();
        for key in &keys {
            stale(&store, 0, key);
        }
        let target = super::of(&store);
        let mut cursor = ExpireCursor::default();
        let mut cycles = 0;
        while with_ttl(&store) > 0 {
            active_expire_cycle(&store, &index, &mut cursor, target.as_ref());
            cycles += 1;
            assert!(cycles < 1000, "the sweep never got to them");
        }
        // A read after the sweep finds nothing to delete.
        for key in &keys {
            assert!(!delete_key_if_expired(&store, 0, key));
        }
        let sent = stream(&mut rx);
        for key in &keys {
            assert_eq!(sent.matches(&command(&["UNLINK", key])).count(), 1, "{key}");
        }
    }

    /// While no client has left database 0 its writes carry no `SELECT`:
    /// a deletion in another database goes back to 0 in the same piece,
    /// and leaves the stream there (`replication::expired_for_stream`).
    /// Once one has, it goes with its `SELECT`, as every write then does.
    #[test]
    fn a_key_of_another_database_is_deleted_there_and_the_stream_goes_back() {
        let (store, repl, mut rx) = server();
        stale(&store, 1, "k");
        assert!(delete_key_if_expired(&store, 1, "k"));
        assert_eq!(
            stream(&mut rx),
            format!(
                "{}{}{}",
                command(&["SELECT", "1"]),
                command(&["UNLINK", "k"]),
                command(&["SELECT", "0"])
            )
        );
        assert_eq!(repl.repl_stream_last_db.load(Ordering::Relaxed), 0);

        repl.several_databases.store(true, Ordering::Relaxed);
        stale(&store, 1, "j");
        assert!(delete_key_if_expired(&store, 1, "j"));
        assert_eq!(
            stream(&mut rx),
            format!("{}{}", command(&["SELECT", "1"]), command(&["UNLINK", "j"]))
        );
        assert_eq!(repl.repl_stream_last_db.load(Ordering::Relaxed), 1);
    }

    /// Loading a dataset, or applying the master's stream, every key is
    /// live: a write keeps the old value's time, and nothing is deleted or
    /// sent.
    #[test]
    fn under_a_hold_nothing_is_past_its_time() {
        let (store, _repl, mut rx) = server();
        stale(&store, 0, "k");
        let shard = store::get_shard(&store[0], "k");
        {
            let _hold = super::hold();
            assert!(!is_expired(&store, 0, "k"));
            assert!(!delete_key_if_expired(&store, 0, "k"));
            assert!(shard.read().get("k").is_some());
            // INCR as the master ran it, with `k` alive.
            drop(shard.write().insert_str("k", b"6"));
            assert_eq!(shard.read().expires("k"), Some(1));
        }
        assert!(stream(&mut rx).is_empty());
        assert_eq!(text(&store, 0, "k"), Some(b"6".to_vec()));
        // Past its time again, now that nothing holds it.
        assert!(is_expired(&store, 0, "k"));
    }

    /// A write that has changed its key and not yet sent itself on holds
    /// the key's order (`write_order`): a deletion of the key for its time
    /// waits for it, so that the write's record is not after the deletion
    /// in the stream when its change was before it in the store.
    #[test]
    fn a_deletion_waits_for_a_write_to_its_key_still_on_its_way() {
        let (store, _repl, mut rx) = server();
        stale(&store, 0, "k");
        let writing = crate::write_order::lock_keys(0, std::iter::once(&b"k"[..]));
        let deleted = Arc::new(AtomicBool::new(false));
        let deleter = {
            let (store, deleted) = (Arc::clone(&store), Arc::clone(&deleted));
            std::thread::spawn(move || {
                assert!(delete_key_if_expired(&store, 0, "k"));
                deleted.store(true, Ordering::SeqCst);
            })
        };
        std::thread::sleep(Duration::from_millis(100));
        assert!(!deleted.load(Ordering::SeqCst), "it did not wait");
        drop(writing);
        deleter.join().unwrap();
        assert!(stream(&mut rx).contains(&command(&["UNLINK", "k"])));
    }

    /// A replica takes a key past its time as gone, and leaves it, to reads
    /// and to the sweep, for its master's deletion.
    #[test]
    fn a_replica_neither_deletes_nor_sends() {
        let (store, repl, mut rx) = server();
        repl.become_slave();
        stale(&store, 0, "k");
        assert!(delete_key_if_expired(&store, 0, "k"), "gone, to a read");
        assert!(expire_on_read(&store, 0, "k"));
        let target = super::of(&store);
        assert!(target.as_ref().is_some_and(super::Target::on_replica));
        let mut cursor = ExpireCursor::default();
        let index = IndexStore::default();
        assert!(!active_expire_cycle(
            &store,
            &index,
            &mut cursor,
            target.as_ref()
        ));
        assert_eq!(text(&store, 0, "k"), Some(b"5".to_vec()), "still there");
        assert!(stream(&mut rx).is_empty());
    }

    /// A replica of a master that sends no deletions (before 1.1.3) expires
    /// keys itself, to reads and to the sweep, and sends nothing.
    #[test]
    fn a_replica_of_a_master_that_sends_no_deletions_deletes_but_does_not_send() {
        let (store, repl, mut rx) = server();
        repl.become_slave();
        repl.master_propagates_expiry
            .store(false, Ordering::Relaxed);
        let target = super::of(&store);
        assert!(!target.as_ref().is_some_and(super::Target::on_replica));
        stale(&store, 0, "read");
        assert!(delete_key_if_expired(&store, 0, "read"));
        assert_eq!(text(&store, 0, "read"), None, "deleted by the read");
        stale(&store, 0, "swept");
        let mut cursor = ExpireCursor::default();
        let index = IndexStore::default();
        let mut cycles = 0;
        while with_ttl(&store) > 0 {
            active_expire_cycle(&store, &index, &mut cursor, target.as_ref());
            cycles += 1;
            assert!(cycles < 1000, "the sweep never got to it");
        }
        assert_eq!(text(&store, 0, "swept"), None);
        assert!(stream(&mut rx).is_empty());
    }

    /// The sweep at a partial resync deletes the keys whose time ended
    /// before the time it is given, and no others, and sends nothing.
    #[test]
    fn keys_due_before_a_time_are_deleted_and_no_others() {
        let (store, repl, mut rx) = server();
        repl.become_slave();
        let index = IndexStore::default();
        let now = now_ms();
        for (key, at) in [("long", 1), ("recent", now - 500), ("live", now + 60_000)] {
            drop(store::get_shard(&store[1], key).write().insert_expiring(
                key,
                KiviDBValue::String(SharedBytes::from(&b"v"[..])),
                Some(at),
            ));
        }
        drop(
            store::get_shard(&store[0], "forever")
                .write()
                .insert_str("forever", b"v"),
        );
        assert_eq!(
            crate::commands::helpers::expire_keys_due_before(&store, &index, now - 1_000),
            1
        );
        assert_eq!(text(&store, 1, "long"), None);
        for (db, key) in [(1, "recent"), (1, "live"), (0, "forever")] {
            assert_eq!(text(&store, db, key), Some(b"v".to_vec()), "{key}");
        }
        assert!(stream(&mut rx).is_empty());
    }
}
