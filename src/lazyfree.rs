//! Freeing in the background what takes long to free, as Redis's lazyfree
//! does: a large value `UNLINK` removes, the keyspace `FLUSHALL ASYNC` and
//! `FLUSHDB ASYNC` remove, the libraries `FUNCTION FLUSH ASYNC` removes.
//!
//! `INFO` reports the count: `lazyfree_pending_objects`, handed over and
//! not yet freed, and `lazyfreed_objects`, freed so far (reset by `CONFIG
//! RESETSTAT`). Monitoring reads them, and the Redis suite checks them
//! (#19). They used to be `0` always, and everything was freed on the thread
//! that removed it, under the shard lock.

use std::cell::Cell;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{SendError, Sender, channel};

use crate::types::KiviDBValue;

/// Redis's `LAZYFREE_THRESHOLD`: a value that takes more than this much work
/// to free (see [`effort`]) is freed in the background.
pub const THRESHOLD: u64 = 64;

/// Objects handed to the background thread and not yet freed.
static PENDING: AtomicU64 = AtomicU64::new(0);
/// Objects freed in the background since start, or since `CONFIG RESETSTAT`.
static FREED: AtomicU64 = AtomicU64::new(0);

type Job = (Box<dyn Send>, u64);

/// The thread that frees, started the first time there is something for it.
fn worker() -> &'static Sender<Job> {
    static WORKER: OnceLock<Sender<Job>> = OnceLock::new();
    WORKER.get_or_init(|| {
        let (tx, rx) = channel::<Job>();
        // Should it fail to start, the receiver goes with it and
        // `free_later` frees in place.
        let started = std::thread::Builder::new()
            .name("kividb-lazyfree".into())
            .spawn(move || {
                for (what, objects) in rx {
                    drop(what);
                    done(objects);
                }
            });
        if let Err(e) = started {
            crate::log_warn!("lazyfree: could not start the thread, freeing in place: {e}");
        }
        tx
    })
}

/// Counted freed before no longer pending, so whoever sees nothing pending
/// sees all of it freed.
fn done(objects: u64) {
    FREED.fetch_add(objects, Ordering::SeqCst);
    PENDING.fetch_sub(objects, Ordering::SeqCst);
}

/// Free `what`, which holds `objects` objects, on the background thread.
pub fn free_later<T: Send + 'static>(what: T, objects: u64) {
    PENDING.fetch_add(objects, Ordering::SeqCst);
    if let Err(SendError((what, objects))) = worker().send((Box::new(what), objects)) {
        drop(what);
        done(objects);
    }
}

/// `objects` objects a command freed itself, waiting until they were: what a
/// plain `FLUSHALL` or `FLUSHDB` does outside a transaction or a script. Redis
/// runs those as an `ASYNC` flush the client waits for, and counts them.
pub fn record_freed(objects: u64) {
    FREED.fetch_add(objects, Ordering::SeqCst);
}

pub fn pending_objects() -> u64 {
    PENDING.load(Ordering::SeqCst)
}

pub fn freed_objects() -> u64 {
    FREED.load(Ordering::SeqCst)
}

/// `CONFIG RESETSTAT`.
pub fn reset_freed() {
    FREED.store(0, Ordering::SeqCst);
}

thread_local! {
    static QUEUED_DEPTH: Cell<u32> = const { Cell::new(0) };
}

/// Held while `EXEC` runs a transaction's commands, and while a script's
/// `redis.call` runs one. A client cannot wait in either, so Redis does not
/// make a plain `FLUSHALL` there a background flush it waits for: it frees
/// in place and counts nothing.
pub struct RunningQueued(());

pub fn running_queued() -> RunningQueued {
    QUEUED_DEPTH.with(|d| d.set(d.get() + 1));
    RunningQueued(())
}

impl Drop for RunningQueued {
    fn drop(&mut self) {
        QUEUED_DEPTH.with(|d| d.set(d.get() - 1));
    }
}

/// Whether a plain flush now counts its keys as lazily freed: outside a
/// transaction and a script.
pub fn flush_counts() -> bool {
    QUEUED_DEPTH.with(|d| d.get() == 0)
}

/// How much work freeing `value` is, as Redis's `lazyfreeGetFreeEffort`
/// measures it: the elements of a collection, the nodes of a stream and of
/// its groups' pending lists. A string is one allocation.
pub fn effort(value: &KiviDBValue) -> u64 {
    let n = match value {
        KiviDBValue::String(_) => 1,
        KiviDBValue::List(list) => list.read().len(),
        KiviDBValue::Hash(hash) => hash.read().len(),
        KiviDBValue::Json(json) => match &*json.read() {
            serde_json::Value::Array(items) => items.len(),
            serde_json::Value::Object(fields) => fields.len(),
            _ => 1,
        },
        KiviDBValue::Set(set) => set.read().len(),
        KiviDBValue::VectorSet(set) => set.read().entries.len(),
        KiviDBValue::SortedSet(zset) => zset.read().len(),
        KiviDBValue::Stream(stream) => {
            let stream = stream.read();
            // A stream loaded from disk has not recorded its nodes: take them
            // to be of the default `stream-node-max-entries`.
            let nodes = if stream.node_starts.is_empty() {
                stream.entries.len().div_ceil(100)
            } else {
                stream.node_starts.len()
            };
            let groups: usize = stream.groups.values().map(|g| 1 + g.pel.len()).sum();
            nodes + groups
        }
        KiviDBValue::TimeSeries(series) => series.read().samples.len(),
        KiviDBValue::HashEx(hash) => hash.read().len(),
        KiviDBValue::Array(array) => array.read().count() as usize,
    };
    n as u64
}

/// Free a value `UNLINK` removed: in the background if that is much work.
pub fn release(value: KiviDBValue) {
    if effort(&value) > THRESHOLD {
        free_later(value, 1);
    }
}
