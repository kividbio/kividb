use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc;

pub static AOF_WRITTEN: AtomicU64 = AtomicU64::new(0);
pub static AOF_FSYNCED: AtomicU64 = AtomicU64::new(0);
/// Records that never reached the writer. Should stay 0 outside shutdown; a
/// non-zero value means the AOF has a hole and cannot reconstruct the dataset.
pub static AOF_LOST_RECORDS: AtomicU64 = AtomicU64::new(0);

/// Count a record that could not be queued, and say so once.
///
/// Only reachable when the writer's channel is closed, which normally means
/// shutdown. Silence here was the original bug: loss must be visible, and the
/// log line is rate-limited to the first occurrence so a torn-down server does
/// not spam a line per in-flight write.
pub fn record_lost_aof_record() {
    if AOF_LOST_RECORDS.fetch_add(1, Ordering::Relaxed) == 0 {
        log_error!(
            "AOF writer unavailable — write records are being lost; \
             the AOF can no longer reconstruct this dataset"
        );
    }
}
/// Runtime AOF toggle. Set by CONFIG SET appendonly.
/// The AOF writer task is always running — this flag gates whether
/// commands are sent to it. Flipping to false drains the channel
/// naturally; flipping to true immediately starts routing writes.
pub static AOF_ENABLED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

// ── Preamble framing ─────────────────────────────────────────────────────────
//
// A preamble-bearing AOF opens with this header, then the KDB image, then the
// ordinary RESP command stream. See docs/PERSISTENCE.md.
//
//   "KIVIAOF1"   8 bytes, magic
//   <u8>         flags (bit 0: preamble present)
//   <u64 LE>     preamble length in bytes
//
// A legacy AOF opens with `*`, the first byte of a RESP array, so one byte
// distinguishes the two and files written by older builds keep loading.

/// Magic opening a preamble-bearing AOF.
pub const AOF_MAGIC: &[u8; 8] = b"KIVIAOF1";

/// Bytes before the preamble: magic, flags, length.
pub const AOF_HEADER_LEN: usize = 8 + 1 + 8;

const AOF_FLAG_PREAMBLE: u8 = 1;

/// A delta AOF — commands since the last `dump.kdb`, not a full history.
///
/// Recovery loads the snapshot first, then this tail. That is the cloud volume
/// snapshot path: the dump holds most of the data, and writes that landed while
/// the volume was being copied are in the AOF. A legacy log opens with `*` or
/// [`AOF_MAGIC`]; this opens with [`AOF_DELTA_MAGIC`] so the two cannot be
/// confused.
pub const AOF_DELTA_MAGIC: &[u8; 8] = b"KIVIDLT1";
pub const AOF_DELTA_HEADER_LEN: usize = 16;
pub const AOF_GEN_PATH: &str = "dump.kdb.gen";

// ── Rewrite telemetry ────────────────────────────────────────────────────────
//
// Everything below exists so durability is observable from *outside* the
// process. A control plane deciding when to trigger `BGREWRITEAOF`, or alerting
// on AOF growth, or confirming that a freshly promoted replica actually has
// something on disk, can only do that through `INFO persistence` — and until
// now that section reported a hardcoded `-1` and no size at all.

/// Wall-clock duration of the last *completed* rewrite, in milliseconds.
/// `u64::MAX` is the sentinel for "no rewrite has run in this process", which
/// `INFO` renders as Redis's `-1`.
static AOF_LAST_REWRITE_MS: AtomicU64 = AtomicU64::new(u64::MAX);

/// AOF size in bytes immediately after the last rewrite swap — or, if none has
/// run, the size the writer found at startup.
///
/// This is the denominator for the growth ratio (`current / base`) that decides
/// when a rewrite is worth doing. Without it a control plane can only threshold
/// on absolute size, which says nothing about whether a rewrite would actually
/// shrink anything.
static AOF_BASE_SIZE: AtomicU64 = AtomicU64::new(0);

/// Whether the last rewrite attempt swapped successfully. A failed rewrite
/// keeps the existing AOF (so durability is intact) but leaves growth
/// unbounded, which is worth alerting on rather than discovering later.
static AOF_LAST_REWRITE_OK: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(true);

/// Size of the AOF on disk, read fresh rather than tracked.
///
/// Deliberately a `stat` instead of a counter: a counter drifts the moment
/// anything touches the file outside the writer's accounting (a rewrite swap,
/// an operator truncating it, a restart), and a *wrong* durability metric is
/// worse than a slightly more expensive one. `INFO` is not a hot path.
pub fn current_size() -> u64 {
    std::fs::metadata(AOF_PATH).map(|m| m.len()).unwrap_or(0)
}

/// Bytes the AOF held at the last rewrite (or at startup). See [`AOF_BASE_SIZE`].
pub fn base_size() -> u64 {
    AOF_BASE_SIZE.load(Ordering::Relaxed)
}

/// Latch the current on-disk size as the new rewrite baseline.
pub fn record_base_size() {
    AOF_BASE_SIZE.store(current_size(), Ordering::Relaxed);
}

/// Duration of the last completed rewrite, or `None` if none has run.
pub fn last_rewrite_duration() -> Option<std::time::Duration> {
    match AOF_LAST_REWRITE_MS.load(Ordering::Relaxed) {
        u64::MAX => None,
        ms => Some(std::time::Duration::from_millis(ms)),
    }
}

/// Did the last rewrite attempt succeed? True when none has run yet.
pub fn last_rewrite_ok() -> bool {
    AOF_LAST_REWRITE_OK.load(Ordering::Relaxed)
}

/// Is a rewrite walking the store right now?
pub fn rewrite_in_progress() -> bool {
    REWRITE_ACTIVE.load(Ordering::Relaxed) || REWRITES_ASKED_FOR.load(Ordering::Relaxed) > 0
}

/// Checkpoints that `BGREWRITEAOF` has started and that have not finished —
/// whether or not they have got as far as running.
static REWRITES_ASKED_FOR: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// `BGREWRITEAOF`: a checkpoint, on a thread of its own.
///
/// It is in progress from this moment, not from the moment that thread gets
/// to it: a client that asks for a rewrite and then waits for
/// `aof_rewrite_in_progress` to read 0 used to be told 0 at once, before the
/// rewrite had begun, and go on as if it were done.
pub fn checkpoint_in_background(
    store: crate::store::Store,
    index_store: crate::vector::IndexStore,
) {
    struct Asked;
    impl Drop for Asked {
        fn drop(&mut self) {
            REWRITES_ASKED_FOR.fetch_sub(1, Ordering::Relaxed);
        }
    }
    REWRITES_ASKED_FOR.fetch_add(1, Ordering::Relaxed);
    let asked = Asked;
    tokio::spawn(async move {
        tokio::task::spawn_blocking(move || {
            let _asked = asked;
            checkpoint(&store, &index_store)
        })
        .await
        .ok();
    });
}

// Global sender for requesting immediate fsync
static FLUSH_TX: std::sync::OnceLock<tokio::sync::mpsc::Sender<()>> = std::sync::OnceLock::new();

/// Hand a freshly-written AOF to the writer so *it* performs the swap.
///
/// The writer owns the file handle for the process lifetime, and that is the
/// whole problem the swap has to solve: `rewrite_aof` used to `rename` a new
/// file over `appendonly.aof` from another thread, leaving the writer appending
/// to the old, now-unlinked inode. Every write after a `BGREWRITEAOF` went to a
/// file with no name and was gone on restart — verified: the AOF froze at 41
/// bytes while the server happily served the newer values from memory.
///
/// Doing the rename inside the writer means the handle is closed and reopened
/// by the one task that owns it, with no window where a write can land on the
/// wrong inode.
///
/// The request is a mark in the record queue itself (see [`turn_over_mark`]):
/// the records queued ahead of it are the old log's, the ones after it the
/// new log's, whenever the writer gets to them. A checkpoint holds writes
/// only to place the mark, not while the writer writes out and syncs all
/// that is queued ahead of it. With the disk behind, that was most of the
/// hold: 15 to 46 seconds on nodes of a cluster that checkpointed together
/// while loading data.
static WRITER_RUNNING: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Who waits for each turn-over the writer has yet to reach, by save number.
/// std mpsc, not tokio oneshot: the waiter is often `SAVE` / `CONFIG SET`
/// running on a connection worker, where `oneshot::blocking_recv` panics.
static TURN_OVER_WAITERS: parking_lot::Mutex<
    Vec<(u64, std::sync::mpsc::SyncSender<std::io::Result<()>>)>,
> = parking_lot::Mutex::new(Vec::new());

/// The start of a turn-over mark. No record begins with a zero byte.
const TURN_OVER_MARK: &[u8] = b"\0kividb-aof-turn-over\0";

fn turn_over_mark(generation: u64) -> Vec<u8> {
    let mut mark = TURN_OVER_MARK.to_vec();
    mark.extend_from_slice(&generation.to_le_bytes());
    mark
}

/// The save number in a turn-over mark; `None` for a record.
fn as_turn_over_mark(item: &[u8]) -> Option<u64> {
    let rest = item.strip_prefix(TURN_OVER_MARK)?;
    Some(u64::from_le_bytes(rest.try_into().ok()?))
}

// ── Written before it is acknowledged ────────────────────────────────────────
//
// A write was acknowledged as soon as its record was queued for the writer:
// a process killed then (`kill -9`, the OOM killer, a container stopped
// without a grace period) lost writes its clients had been told were made, a
// few in a test, more under a burst (#147). As in Redis, a reply now waits
// until the records of the commands it answers are in the file (the page
// cache; `appendfsync everysec` syncs it within the second, as before).
//
// A connection whose replies answer a write sends a barrier down its own AOF
// queue, behind its records, and then writes what is queued itself, if no
// one else is writing: no thread to wake and wait for. Otherwise whoever is
// writing (another connection, or the writer thread) takes its barrier with
// what it writes. Every barrier is answered once what came before it is
// written: one write, for as many connections as were waiting, is a group
// commit. Waking the writer thread for each write and waiting to be woken
// back halved the writes a second.

/// The queue and the file, shared by the writer thread and the connections
/// that write what is queued themselves (see [`written`]).
struct Log {
    rx: mpsc::UnboundedReceiver<Vec<u8>>,
    file: std::fs::File,
}

static LOG: parking_lot::Mutex<Option<Log>> = parking_lot::const_mutex(None);

/// Wakes the writer thread: a record or a barrier has been queued. Kept as a
/// permit if it is busy, so nothing queued is left behind.
static QUEUED: std::sync::LazyLock<tokio::sync::Notify> =
    std::sync::LazyLock::new(tokio::sync::Notify::new);

/// Something has been sent down the AOF's queue. Every sender calls it.
///
/// While replies wait for their writes, the writer thread is not woken: the
/// connection that sent the record writes it before it replies (see
/// [`written`]), and woken, the thread took the lock first and left the
/// connection waiting to be woken in turn — a quarter of the writes a
/// second. What no reply waits for (an expiry's delete, a replica's stream)
/// is written with the next connection's, or by the writer's next sync, a
/// second at most.
#[inline]
pub fn records_queued() {
    if !acks_wait() {
        QUEUED.notify_one();
    }
}

/// Write whatever is queued, and answer the barriers it covers. Whether a
/// barrier was answered; `None` if someone else is writing now.
fn write_what_is_queued(block: bool) -> Option<bool> {
    let mut log = if block { LOG.lock() } else { LOG.try_lock()? };
    let log = log.as_mut()?;
    Some(write_queued(&mut log.file, &mut log.rx, None).1)
}

/// A barrier in the queue, followed by its number.
const BARRIER_MARK: &[u8] = b"\0kividb-aof-barrier\0";

static BARRIERS: std::sync::LazyLock<
    parking_lot::Mutex<ahash::AHashMap<u64, tokio::sync::oneshot::Sender<()>>>,
> = std::sync::LazyLock::new(Default::default);
static NEXT_BARRIER: AtomicU64 = AtomicU64::new(1);

/// The number in a barrier; `None` for anything else.
fn as_barrier(item: &[u8]) -> Option<u64> {
    let rest = item.strip_prefix(BARRIER_MARK)?;
    Some(u64::from_le_bytes(rest.try_into().ok()?))
}

/// `aof-ack-after-write`: whether a reply to a write waits for its record
/// to be in the file. `yes`, the default, is Redis's guarantee; `no` is
/// 1.1.2's behaviour, faster for writes, where a killed process can lose
/// the last few acknowledged writes.
static ACK_AFTER_WRITE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);

pub fn set_ack_after_write(on: bool) {
    ACK_AFTER_WRITE.store(on, Ordering::Relaxed);
}

/// Whether a connection's replies may have to wait for the AOF: it is on,
/// and acknowledges after writing.
#[inline]
pub fn acks_wait() -> bool {
    AOF_ENABLED.load(Ordering::Relaxed) && ACK_AFTER_WRITE.load(Ordering::Relaxed)
}

/// Whether a command may write a record its reply should wait for: a write,
/// or a script or a transaction that may make some.
#[inline]
pub fn may_log(name: &[u8]) -> bool {
    crate::replication::is_write_command_bytes(name)
        || name.eq_ignore_ascii_case(b"EVAL")
        || name.eq_ignore_ascii_case(b"EVALSHA")
        || name.eq_ignore_ascii_case(b"FCALL")
        || name.eq_ignore_ascii_case(b"EXEC")
}

/// Wait until every record queued on `tx` before now is in the file.
///
/// Bounded: should the writer be gone, a reply is late, not lost. With the
/// AOF off, or no writer, nothing to wait for.
pub async fn written(tx: &tokio::sync::mpsc::UnboundedSender<Vec<u8>>) {
    if !acks_wait() || !WRITER_RUNNING.load(Ordering::Acquire) {
        return;
    }
    let id = NEXT_BARRIER.fetch_add(1, Ordering::Relaxed);
    let (done, wait) = tokio::sync::oneshot::channel();
    BARRIERS.lock().insert(id, done);
    let mut mark = BARRIER_MARK.to_vec();
    mark.extend_from_slice(&id.to_le_bytes());
    if tx.send(mark).is_err() {
        BARRIERS.lock().remove(&id);
        return;
    }
    // Write it ourselves, unless someone else is writing. They may have
    // taken all there was before our barrier came: then the writer thread
    // takes it.
    if write_what_is_queued(false).is_none() {
        QUEUED.notify_one();
    }
    if tokio::time::timeout(std::time::Duration::from_secs(5), wait)
        .await
        .is_err()
    {
        BARRIERS.lock().remove(&id);
        log_throttled!(
            10,
            crate::loglevel::WARN,
            "AOF: a reply waited 5 s for its write to reach {AOF_PATH} and was sent without it"
        );
    }
}

/// Answer the barriers taken: what came before them has been written.
fn release_barriers(barriers: &mut Vec<u64>) {
    if barriers.is_empty() {
        return;
    }
    let mut waiting = BARRIERS.lock();
    for id in barriers.drain(..) {
        if let Some(done) = waiting.remove(&id) {
            let _ = done.send(());
        }
    }
}

/// The AOF queue, reachable from code that was never handed one.
///
/// Installed once at startup by whichever server path is running (see
/// `install_global_aof_sender`). Only consumer today is the replication
/// applier, which runs several frames below the point where the channel exists
/// — see `commands::ctx::GlobalAofSender`.
static GLOBAL_AOF_TX: std::sync::OnceLock<Box<dyn crate::commands::ctx::AofSender>> =
    std::sync::OnceLock::new();

/// Publish the AOF queue for [`crate::commands::ctx::GlobalAofSender`].
///
/// Call once, from the same place the writer task is spawned, before any
/// replica session can start. First caller wins.
///
/// A process running several servers in-process (the test harness does) leaves
/// them sharing the first one's queue. That is not a new constraint: `AOF_PATH`,
/// `AOF_ENABLED`, `FLUSH_TX` and the turn-over waiters are all already process-global, so
/// the AOF has only ever supported one server per process — and `set`-and-ignore
/// is what the other two channels do.
pub fn install_global_aof_sender(tx: Box<dyn crate::commands::ctx::AofSender>) {
    let _ = GLOBAL_AOF_TX.set(tx);
}

thread_local! {
    /// The records of the script or transaction this thread is running, held
    /// back until it ends (`RecordGroup`).
    static RECORD_GROUP: std::cell::RefCell<Option<Vec<u8>>> =
        const { std::cell::RefCell::new(None) };
}

/// Queue a record for the AOF: at once — or, while this thread is running a
/// script or a transaction, with everything else it writes.
pub fn queue_record(tx: &dyn crate::commands::ctx::AofSender, data: Vec<u8>) {
    let data = RECORD_GROUP.with(|group| match group.borrow_mut().as_mut() {
        Some(held) => {
            held.extend_from_slice(&data);
            None
        }
        None => Some(data),
    });
    if let Some(data) = data {
        tx.send_aof(data);
    }
}

/// Put `data` in front of the records this thread's group holds — the
/// `MULTI` of a group that turns out to be a transaction, at its second
/// write. False if no group is open.
pub(crate) fn put_in_front_of_group(data: &[u8]) -> bool {
    RECORD_GROUP.with(|group| match group.borrow_mut().as_mut() {
        Some(held) => {
            held.splice(0..0, data.iter().copied());
            true
        }
        None => false,
    })
}

/// Everything a script, or a transaction, writes, as one piece of the AOF.
///
/// Each `redis.call` that writes makes a record. Queued one by one, they
/// reach the file in whatever groups the writer happens to take them, and a
/// server killed between two of those leaves a script half done in its log:
/// replayed, a job's hash without its place in the queue. A script is one
/// step to every client, and it is one to the log too: its records are held
/// while this lives, and queued together when it is dropped — which the
/// script does while it still holds the write gate, so a checkpoint or a
/// replica's cut cannot fall between the script's effect and its record.
///
/// One message is one `write`, short of the kernel cutting that write at a
/// page boundary as the process dies: far rarer than a kill between two
/// messages, and not excluded. So the group is marked in the file: it is
/// opened by a [`crate::replication::PropagationGroup`], which puts a
/// `MULTI` before its records and an `EXEC` after them, and a replay drops a
/// `MULTI` whose `EXEC` is not there (#172). A transaction's records are
/// grouped the same way.
pub struct RecordGroup<'a> {
    tx: &'a dyn crate::commands::ctx::AofSender,
    /// False for a script called by a script: the outer one's group holds.
    outermost: bool,
}

impl<'a> RecordGroup<'a> {
    pub fn begin(tx: &'a dyn crate::commands::ctx::AofSender) -> Self {
        let outermost = RECORD_GROUP.with(|group| {
            let mut group = group.borrow_mut();
            if group.is_some() {
                false
            } else {
                *group = Some(Vec::new());
                true
            }
        });
        RecordGroup { tx, outermost }
    }
}

impl Drop for RecordGroup<'_> {
    fn drop(&mut self) {
        if !self.outermost {
            return;
        }
        if let Some(held) = RECORD_GROUP.with(|group| group.borrow_mut().take())
            && !held.is_empty()
        {
            self.tx.send_aof(held);
        }
    }
}

/// Queue a record through the globally-installed sender.
///
/// A missing sender is counted as loss rather than dropped silently: reaching
/// here at all means a caller believed AOF was on, so "no writer installed" is
/// a hole in the log, not a no-op.
pub fn send_to_global_writer(data: Vec<u8>) {
    match GLOBAL_AOF_TX.get() {
        Some(tx) => tx.send_aof(data),
        None => record_lost_aof_record(),
    }
}

pub fn request_flush() {
    match FLUSH_TX.get() {
        Some(tx) => {
            let r = tx.try_send(());
            log_debug!("AOF: request_flush result: {r:?}");
        }
        None => log_debug!("AOF: request_flush: FLUSH_TX not initialized"),
    }
}

pub async fn start_aof_writer(rx: mpsc::UnboundedReceiver<Vec<u8>>) {
    let (flush_tx, mut flush_rx) = tokio::sync::mpsc::channel::<()>(1);
    let _ = FLUSH_TX.set(flush_tx);

    // A server asked to keep an append-only file that cannot open one must
    // not run: it would accept writes and keep none of them. (This used to be
    // a panic in this task alone — the server carried on, serving, with
    // nothing being logged.)
    let file = match open_aof() {
        Ok(file) => file,
        Err(e) => fatal_persistence(&format!("{AOF_PATH} cannot be opened for appending: {e}")),
    };
    *LOG.lock() = Some(Log { rx, file });
    WRITER_RUNNING.store(true, Ordering::Release);

    // Whatever survived startup (an AOF that was just replayed, or nothing) is
    // the baseline the first growth ratio is measured against. The writer opens
    // *after* recovery on every startup path, so this sees the post-replay size.
    record_base_size();

    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = interval.tick() => {
                do_fsync();
            }
            Some(()) = flush_rx.recv() => {
                do_fsync();
            }
            () = QUEUED.notified() => {
                let released = write_what_is_queued(true).unwrap_or(false);
                // Not straight back to waiting. A sender wakes a writer
                // that is waiting, with a system call, and under load the
                // writer was back to waiting before the next record came:
                // one wake-up a record, a tenth of the server's time when
                // scripts on several threads were what it was doing. What
                // arrives in this pause is taken together, with no wake-up
                // at all. A record that arrives alone is still written at
                // once: the pause is after it, not before. Not when it has
                // answered barriers: their clients wait on it, and send
                // nothing until they have their replies. Nor while replies
                // wait for writes at all: a connection's records come before
                // its barrier, and a pause after them made the barrier wait
                // it out, halving the writes a second (#147).
                if !released && !acks_wait() {
                    std::thread::sleep(WRITER_PAUSE);
                }
            }
        }
    }
}

/// The file, written by the writer itself.
///
/// The writer has a thread of its own (every server path runs it on a
/// runtime that hosts nothing else), so it can simply write. Through
/// `tokio::fs` each write was handed to yet another thread and waited for:
/// two more wake-ups for every record that arrived alone, which is how a
/// script's record arrives. On a one-CPU machine each of those is the
/// connection's thread put aside.
fn open_aof() -> std::io::Result<std::fs::File> {
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(AOF_PATH)
}

/// The log a checkpoint was turning over from, kept until the snapshot it
/// goes with is safely on disk. See [`checkpoint`].
pub const AOF_PREV_PATH: &str = "appendonly.aof.prev";

/// Turn the log over: what is in it, and everything still queued for it,
/// becomes `appendonly.aof.prev`, and a new, empty delta for save number
/// `generation` takes its place.
///
/// Called with writes held (see [`checkpoint`]), so everything queued came
/// from a write made before the dataset was captured: it belongs with the
/// old log, which with the old snapshot describes the dataset as captured.
/// Nothing is dropped.
fn perform_turn_over(file: &mut std::fs::File, generation: u64) -> std::io::Result<()> {
    file.sync_data()?;
    turn_over_files(generation)?;
    *file = open_aof()?;
    Ok(())
}

/// Write `first` and whatever else is queued, turning the log over at each
/// mark on the way (see [`turn_over_mark`]), and answering the barriers
/// taken once what was before them is written. Returns whether anything was
/// written, and whether any barrier was answered.
fn write_queued(
    file: &mut std::fs::File,
    rx: &mut mpsc::UnboundedReceiver<Vec<u8>>,
    mut first: Option<Vec<u8>>,
) -> (bool, bool) {
    let mut wrote = false;
    let mut released = false;
    let mut barriers = Vec::new();
    loop {
        let mut batch = Vec::new();
        let mark = match first.take() {
            Some(item) => match (as_turn_over_mark(&item), as_barrier(&item)) {
                (Some(generation), _) => Some(generation),
                (None, Some(id)) => {
                    barriers.push(id);
                    drain_into(&mut batch, rx, &mut barriers)
                }
                (None, None) => {
                    batch = item;
                    drain_into(&mut batch, rx, &mut barriers)
                }
            },
            None => drain_into(&mut batch, rx, &mut barriers),
        };
        wrote |= !batch.is_empty();
        write_batch(file, &batch);
        released |= !barriers.is_empty();
        release_barriers(&mut barriers);
        let Some(generation) = mark else {
            return (wrote, released);
        };
        let result = perform_turn_over(file, generation);
        let mut waiters = TURN_OVER_WAITERS.lock();
        if let Some(at) = waiters.iter().position(|(g, _)| *g == generation) {
            let (_, done) = waiters.swap_remove(at);
            let _ = done.send(result);
        }
    }
}

/// The files' part of [`perform_turn_over`], for when no writer has the log
/// open as well.
fn turn_over_files(generation: u64) -> std::io::Result<()> {
    use std::io::Write;
    let prev = std::path::Path::new(AOF_PREV_PATH);
    if prev.exists() {
        // A turn-over before this one whose snapshot never reached the disk:
        // its old log is still needed, and this log goes on from it.
        let current = std::fs::read(AOF_PATH).unwrap_or_default();
        let records = if aof_delta_generation(&current).is_some() {
            &current[AOF_DELTA_HEADER_LEN..]
        } else {
            &current[..]
        };
        let mut old = std::fs::OpenOptions::new().append(true).open(prev)?;
        old.write_all(records)?;
        old.sync_data()?;
    } else if std::path::Path::new(AOF_PATH).exists() {
        std::fs::rename(AOF_PATH, prev)?;
    }
    let tmp_path = "appendonly.aof.rewrite.tmp";
    {
        let mut fresh = std::fs::File::create(tmp_path)?;
        fresh.write_all(AOF_DELTA_MAGIC)?;
        fresh.write_all(&generation.to_le_bytes())?;
        fresh.sync_all()?;
    }
    std::fs::rename(tmp_path, AOF_PATH)
}

/// One write syscall per record is what made the AOF the bottleneck: each
/// `tokio::fs` write hops to the blocking pool, so the writer sustained only
/// ~13k records/s against a client doing 200k/s. That backlog is what used to
/// overflow the old bounded queue and silently discard records; it now merely
/// delays them, but a delayed record is still not a durable one — a 1.1s burst
/// took ~15s to reach disk, stretching the 1s fsync guarantee to ~15s.
///
/// Appending everything already queued into one buffer collapses that into a
/// single write. Capped so one burst cannot hand the kernel an arbitrarily
/// large buffer; whatever is left stays queued and goes out on the next pass.
const MAX_AOF_BATCH_BYTES: usize = 4 * 1024 * 1024;

/// How long the writer stays away from its queue after a write. It adds this
/// much, at most, to how long a record written under load waits to reach
/// the file — next to the second it may wait for the `fsync`.
const WRITER_PAUSE: std::time::Duration = std::time::Duration::from_micros(150);

/// Take queued records into `batch`, up to a turn-over mark, whose save
/// number is returned; the barriers met on the way go to `barriers`.
fn drain_into(
    batch: &mut Vec<u8>,
    rx: &mut mpsc::UnboundedReceiver<Vec<u8>>,
    barriers: &mut Vec<u64>,
) -> Option<u64> {
    while batch.len() < MAX_AOF_BATCH_BYTES {
        match rx.try_recv() {
            Ok(more) => match (as_turn_over_mark(&more), as_barrier(&more)) {
                (Some(generation), _) => return Some(generation),
                (None, Some(id)) => barriers.push(id),
                (None, None) => batch.extend_from_slice(&more),
            },
            Err(_) => break,
        }
    }
    None
}

fn write_batch(file: &mut std::fs::File, batch: &[u8]) {
    use std::io::Write;
    if batch.is_empty() {
        return;
    }
    if let Err(e) = file.write_all(batch) {
        log_throttled!(
            10,
            crate::loglevel::ERROR,
            "AOF: write to {AOF_PATH} failed: {e} — writes are not being made durable; check free disk space and permissions on the data directory"
        );
    }
}

/// Write what is queued and sync the file — on a handle of its own, outside
/// the lock, so that connections writing their records do not wait out the
/// disk.
fn do_fsync() {
    let (had_data, written, file) = {
        let mut log = LOG.lock();
        let Some(log) = log.as_mut() else {
            return;
        };
        // Drain pending writes before fsyncing, coalesced into one write.
        let had_data = write_queued(&mut log.file, &mut log.rx, None).0;
        // What the sync covers: what is on this file now. Records others
        // write once the lock is let go may miss it, so they are not counted.
        let written = AOF_WRITTEN.load(Ordering::Relaxed);
        (had_data, written, log.file.try_clone())
    };
    let Ok(file) = file else {
        return;
    };

    // Skip the syscall only when there is nothing *unsynced*.
    //
    // Records land on the file as they are queued, without an fsync.
    // `WAITAOF` then `request_flush()`s; the drain here is often empty
    // because the write already happened. Skipping `sync_data` in that case
    // leaves `AOF_FSYNCED` behind `AOF_WRITTEN` and WAITAOF times out at `[0,0]`.
    //
    // The idle case we actually want to skip is a quiet `--aof` process whose
    // last write is already durable (`AOF_WRITTEN == AOF_FSYNCED`). That used
    // to hop to tokio's blocking pool every second on an empty fd — the same
    // overhead the AOF-off path already avoided, and the RSS ratchet on a
    // quiet box (`used_memory` is process RSS).
    if !had_data && written == AOF_FSYNCED.load(Ordering::Relaxed) {
        return;
    }

    if let Err(e) = file.sync_data() {
        log_throttled!(
            10,
            crate::loglevel::ERROR,
            "AOF: fsync of {AOF_PATH} failed: {e} — writes acknowledged since the last good fsync may be lost on power failure; check the disk"
        );
    } else {
        AOF_FSYNCED.fetch_max(written, Ordering::Relaxed);
    }
}

/// Ask the writer to turn the log over, and wait for it.
///
/// The waiter may be an async connection worker (`SAVE`, `CONFIG SET
/// appendonly yes`). A tokio oneshot `blocking_recv` panics in that context;
/// a std channel `recv` just blocks the worker until the dedicated writer
/// thread is done.
/// Place the turn-over: call with writes held. What is returned waits, once
/// they are no longer held, for the writer to have done it.
fn turn_over_aof(generation: u64) -> impl FnOnce() -> std::io::Result<()> {
    let queued = if WRITER_RUNNING.load(Ordering::Acquire) {
        let (done_tx, done_rx) = std::sync::mpsc::sync_channel(1);
        TURN_OVER_WAITERS.lock().push((generation, done_tx));
        send_to_global_writer(turn_over_mark(generation));
        // Waited for: never left to the next sync.
        QUEUED.notify_one();
        Some(done_rx)
    } else {
        None
    };
    move || match queued {
        // No writer running: nothing else has the file open.
        None => turn_over_files(generation),
        Some(done) => done
            .recv()
            .map_err(|_| std::io::Error::other("AOF writer dropped the turn-over"))?,
    }
}

/// Fold the live dataset into `dump.kdb` and start a new AOF delta.
///
/// After this, recovery is: load the dump, then replay the AOF. The AOF holds
/// only writes since this checkpoint.
///
/// Writes are held only while the dataset is captured and the log turned
/// over: a reference to every string value (they are never changed in
/// place), the other values laid out, and the old log closed with everything
/// queued for it. The snapshot is written to disk afterwards, with nothing
/// held. It used to be written with every shard of every database locked
/// for writing, so that reads and writes alike waited for the whole of a
/// copy of the dataset and its write to disk: seconds, on a large one, every
/// time the log doubled.
///
/// Between the turn-over and the snapshot reaching the disk, the old log is
/// kept as `appendonly.aof.prev`, and recovery replays it before the new log
/// if the snapshot did not get there (see `checkpoint_completed`).
pub fn checkpoint(
    store: &crate::store::Store,
    index_store: &crate::vector::IndexStore,
) -> std::io::Result<()> {
    checkpoint_noting(store, index_store, &mut || {})
}

/// [`checkpoint`], calling `at_cut` while writes are held for the capture:
/// what it reads is as of the dataset written.
pub fn checkpoint_noting(
    store: &crate::store::Store,
    index_store: &crate::vector::IndexStore,
    at_cut: &mut dyn FnMut(),
) -> std::io::Result<()> {
    REWRITE_ACTIVE.store(true, Ordering::Release);
    let aof_on = AOF_ENABLED.load(Ordering::Relaxed);
    let started = std::time::Instant::now();
    let result = checkpoint_inner(store, index_store, at_cut);
    // With the AOF on, a checkpoint is its rewrite (#149): these were never
    // set.
    if aof_on {
        AOF_LAST_REWRITE_OK.store(result.is_ok(), Ordering::Relaxed);
        if result.is_ok() {
            AOF_LAST_REWRITE_MS.store(started.elapsed().as_millis() as u64, Ordering::Relaxed);
        }
    }
    if result.is_ok() {
        crate::stats::record_save();
    }
    REWRITE_ACTIVE.store(false, Ordering::Release);
    result
}

fn checkpoint_inner(
    store: &crate::store::Store,
    index_store: &crate::vector::IndexStore,
    at_cut: &mut dyn FnMut(),
) -> std::io::Result<()> {
    let aof_on = AOF_ENABLED.load(Ordering::Relaxed);
    let generation = next_generation();

    // Between whole writes: a write is either in what is captured, with its
    // record in the old log, or after it, with its record in the new one.
    let cut = crate::write_gate::cut();
    let started = std::time::Instant::now();
    at_cut();
    let frozen = crate::snapshot::freeze_dataset_for_disk(store, Some(index_store));
    let turn_over = match (&frozen, aof_on) {
        (Ok(_), true) => Some(turn_over_aof(generation)),
        _ => None,
    };
    drop(cut);
    let held = started.elapsed();
    let frozen = frozen?;
    if let Some(turned) = turn_over {
        turned()?;
    }
    if held > std::time::Duration::from_millis(100) {
        log_info!(
            "AOF: the checkpoint held writes for {} ms",
            held.as_millis()
        );
    }

    static TMP_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let tmp_path = format!(
        "dump.kdb.{}.{}.tmp",
        std::process::id(),
        TMP_SEQ.fetch_add(1, Ordering::Relaxed)
    );
    let written = (|| -> std::io::Result<()> {
        frozen.write_file(&tmp_path)?;
        // The save number, and which dump it is the number of, take their
        // name before the dump takes its own: a crash between the two leaves
        // a number that does not describe the dump there, which recovery
        // reads as a checkpoint that did not finish.
        let (len, hash) = file_fingerprint(&tmp_path)?;
        let gen_tmp = format!("{AOF_GEN_PATH}.tmp");
        {
            use std::io::Write;
            let mut file = std::fs::File::create(&gen_tmp)?;
            file.write_all(&generation.to_le_bytes())?;
            file.write_all(&len.to_le_bytes())?;
            file.write_all(&hash.to_le_bytes())?;
            file.sync_all()?;
        }
        std::fs::rename(&gen_tmp, AOF_GEN_PATH)?;
        std::fs::rename(&tmp_path, "dump.kdb")
    })();
    if let Err(e) = written {
        let _ = std::fs::remove_file(&tmp_path);
        log_warn!(
            "AOF: the snapshot of the checkpoint could not be written: {e} — {AOF_PREV_PATH} is \
             kept and replayed before the log on the next start"
        );
        return Err(e);
    }
    crate::stats::reset_rdb_changes();
    if aof_on {
        if let Err(e) = std::fs::remove_file(AOF_PREV_PATH)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            log_warn!("AOF: {AOF_PREV_PATH} could not be removed: {e}");
        }
        record_base_size();
        log_info!("AOF: rebased onto dump.kdb (generation {generation}) — log is now the delta");
    }
    Ok(())
}

/// The length of a file and a hash of its first and last 64 KiB: enough to
/// tell one dump from another.
fn file_fingerprint(path: &str) -> std::io::Result<(u64, u64)> {
    use std::io::{Read, Seek, SeekFrom};
    const EDGE: u64 = 64 * 1024;
    let mut file = std::fs::File::open(path)?;
    let len = file.metadata()?.len();
    let mut sample = Vec::with_capacity((2 * EDGE) as usize);
    (&mut file).take(EDGE).read_to_end(&mut sample)?;
    if len > EDGE {
        file.seek(SeekFrom::Start(len - EDGE.min(len - EDGE)))?;
        file.read_to_end(&mut sample)?;
    }
    Ok((len, xxhash_rust::xxh3::xxh3_64(&sample)))
}

/// Whether the checkpoint that left `appendonly.aof.prev` behind finished:
/// the save number on disk is the one the log goes on from, and it was
/// written for the `dump.kdb` that is there.
fn checkpoint_completed(log: &[u8]) -> bool {
    let Ok(saved) = std::fs::read(AOF_GEN_PATH) else {
        return false;
    };
    if saved.len() < 24 {
        return false;
    }
    let number = u64::from_le_bytes(saved[0..8].try_into().unwrap_or_default());
    let len = u64::from_le_bytes(saved[8..16].try_into().unwrap_or_default());
    let hash = u64::from_le_bytes(saved[16..24].try_into().unwrap_or_default());
    aof_delta_generation(log) == Some(number)
        && file_fingerprint("dump.kdb").is_ok_and(|found| found == (len, hash))
}

fn next_generation() -> u64 {
    read_generation().saturating_add(1)
}

fn read_generation() -> u64 {
    std::fs::read(AOF_GEN_PATH)
        .ok()
        .and_then(|b| b.get(..8).and_then(|s| s.try_into().ok()))
        .map(u64::from_le_bytes)
        .unwrap_or(0)
}

fn aof_delta_generation(buf: &[u8]) -> Option<u64> {
    if buf.len() < AOF_DELTA_HEADER_LEN || !buf.starts_with(AOF_DELTA_MAGIC) {
        return None;
    }
    buf[8..16].try_into().ok().map(u64::from_le_bytes)
}

/// The dump is newer than this AOF delta (SAVE wrote dump, crashed before the
/// log was reset). Replaying the old delta would double-apply. Skip it.
fn aof_delta_is_stale(buf: &[u8]) -> bool {
    match aof_delta_generation(buf) {
        Some(aof_gen) => {
            let dump_gen = read_generation();
            dump_gen > 0 && aof_gen < dump_gen
        }
        None => false,
    }
}

// ── Automatic rewrite ────────────────────────────────────────────────────────

/// How often the growth ratio is checked. One `stat` per tick, so the interval
/// only needs to be short relative to how fast a log can grow, not to anything
/// a client waits on.
const AUTO_REWRITE_CHECK: std::time::Duration = std::time::Duration::from_secs(10);

/// Has the log grown enough past its last rewrite to be worth rewriting again?
///
/// `percentage` of 0 disables this entirely. `min_size` keeps a small log from
/// rewriting every time it doubles, which for a log of a few kilobytes is
/// constant work for no benefit.
///
/// A zero base means no rewrite has happened in this process and the log was
/// empty at startup, so any log past `min_size` is all growth.
pub fn should_auto_rewrite(percentage: u64, min_size: u64) -> bool {
    if percentage == 0 || !AOF_ENABLED.load(Ordering::Relaxed) || rewrite_in_progress() {
        return false;
    }
    let current = current_size();
    if current < min_size {
        return false;
    }
    let base = base_size();
    if base == 0 {
        return true;
    }
    current
        .saturating_sub(base)
        .saturating_mul(100)
        .checked_div(base)
        .is_some_and(|growth| growth >= percentage)
}

/// `auto-aof-rewrite-percentage`, as in effect; 0 turns automatic rewrites off.
pub static AUTO_REWRITE_PERCENTAGE: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(100);
/// `auto-aof-rewrite-min-size`, in bytes, as in effect.
pub static AUTO_REWRITE_MIN_SIZE: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(64 * 1024 * 1024);

/// Watch AOF growth and rewrite when it crosses the threshold.
///
/// Without this the log is bounded only by an operator remembering to run
/// `BGREWRITEAOF`, and startup cost tracks total write history rather than live
/// data — a benchmark that overwrites its keyspace pays for every overwrite on
/// the next start. The preamble does not help a log nothing ever compacts.
///
/// A plain thread rather than a task: `rewrite_aof` quiesces every shard and
/// then blocks waiting for the writer to swap the file in, which is exactly
/// what must not sit on a runtime worker. Being off-runtime also means both the
/// tokio and io_uring paths start it the same way.
///
/// `percentage` and `min_size` are where [`AUTO_REWRITE_PERCENTAGE`] and
/// [`AUTO_REWRITE_MIN_SIZE`] start; `CONFIG SET` changes them while it runs.
/// They used to be read once, here: `CONFIG SET auto-aof-rewrite-percentage 0`
/// answered OK and rewrites went on.
pub fn spawn_auto_rewrite_thread(
    store: crate::store::Store,
    index_store: crate::vector::IndexStore,
    percentage: u64,
    min_size: u64,
) {
    AUTO_REWRITE_PERCENTAGE.store(percentage, Ordering::Relaxed);
    AUTO_REWRITE_MIN_SIZE.store(min_size, Ordering::Relaxed);
    if percentage == 0 {
        log_info!("AOF: automatic rewrite disabled (aof-rewrite-percentage 0)");
    }
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(AUTO_REWRITE_CHECK);
            let percentage = AUTO_REWRITE_PERCENTAGE.load(Ordering::Relaxed);
            let min_size = AUTO_REWRITE_MIN_SIZE.load(Ordering::Relaxed);
            if !should_auto_rewrite(percentage, min_size) {
                continue;
            }
            log_info!(
                "AOF: auto-checkpoint: {:.2} MB is {}%+ above the {:.2} MB baseline — \
                 folding into dump.kdb",
                current_size() as f64 / (1024.0 * 1024.0),
                percentage,
                base_size() as f64 / (1024.0 * 1024.0),
            );
            if let Err(e) = checkpoint(&store, &index_store) {
                log_warn!("AOF: auto-checkpoint failed: {e}");
            }
        }
    });
}

// ── Replay ───────────────────────────────────────────────────────────────────

/// Outcome of reading an AOF back into the store.
#[derive(Debug, Default)]
pub struct ReplayStats {
    pub commands: u64,
    pub errors: u64,
    /// A record was cut short — the process died mid-write. The prefix before
    /// it is still applied; everything from the tear on is discarded.
    pub truncated_tail: bool,
    /// Length of the file up to and including its last whole record. Shorter
    /// than `bytes` exactly when `truncated_tail` is set.
    pub valid_len: u64,
    pub bytes: u64,
    /// Records a later `FLUSHDB` / `FLUSHALL` made dead, so they were never
    /// applied. See [`live_after_flushes`].
    pub skipped: u64,
    /// Keys restored from the KDB preamble, before a single record was
    /// replayed. Zero for a log with no preamble.
    pub preamble_keys: u64,
    /// Records of a transaction whose `EXEC` is not in the file, its `MULTI`
    /// included: dropped, and cut off with the tail (`truncated_tail`).
    pub reverted: u64,
}

/// Split a raw AOF into its optional KDB image and the command stream.
///
/// A legacy log opens with `*` and is all stream. A preamble-bearing one opens
/// with [`AOF_MAGIC`]; see the framing notes at the top of this file.
///
/// A short preamble is a hard error, unlike a torn *tail*. The tail is the
/// normal casualty of a crash mid-append and truncating it costs the last few
/// writes. The preamble arrives by atomic rename and cannot be half-written by
/// any ordinary failure, so a short one means the file is damaged — and
/// starting from a fraction of a dataset, silently, is far worse than not
/// starting at all.
fn split_aof(buf: &[u8]) -> std::io::Result<(Option<&[u8]>, &[u8])> {
    if buf.starts_with(AOF_DELTA_MAGIC) {
        if buf.len() < AOF_DELTA_HEADER_LEN {
            return Ok((None, &[]));
        }
        return Ok((None, &buf[AOF_DELTA_HEADER_LEN..]));
    }
    if !buf.starts_with(AOF_MAGIC) {
        return Ok((None, buf));
    }
    if buf.len() < AOF_HEADER_LEN {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "AOF header is truncated",
        ));
    }

    let flags = buf[8];
    let body = &buf[AOF_HEADER_LEN..];
    if flags & AOF_FLAG_PREAMBLE == 0 {
        return Ok((None, body));
    }

    let len = u64::from_le_bytes(buf[9..17].try_into().expect("8 bytes")) as usize;
    let end = AOF_HEADER_LEN.checked_add(len).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "AOF preamble length overflows the file",
        )
    })?;
    if buf.len() < end {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "AOF preamble is truncated: header claims {len} bytes, file holds {}",
                buf.len().saturating_sub(AOF_HEADER_LEN)
            ),
        ));
    }
    Ok((Some(&buf[AOF_HEADER_LEN..end]), &buf[end..]))
}

/// Mark the records a later flush renders pointless.
///
/// An AOF that ends in `FLUSHDB` otherwise rebuilds the whole dataset in order
/// to throw it away — 1.18M vectors and their index, for nothing. Everything
/// before the last `FLUSHALL`, and everything before the last `FLUSHDB` *for
/// the database that flush targeted*, is dead by construction.
///
/// `SELECT` is always kept: it carries the database cursor that decides which
/// flush applies to which record, and re-applying it is free. The flush records
/// themselves are kept too — replaying one against an already-empty database is
/// a no-op, and keeping them avoids reasoning about whether the cut landed
/// before or after the boundary.
fn live_after_flushes(commands: &[Vec<Vec<u8>>]) -> Vec<bool> {
    let is = |cmd: &[Vec<u8>], name: &[u8]| {
        cmd.first().map(|c| c.eq_ignore_ascii_case(name)) == Some(true)
    };

    // Pass 1: the database each record lands in, and where the last flush of
    // each kind sits.
    let mut db_of = Vec::with_capacity(commands.len());
    let mut last_flushall: Option<usize> = None;
    let mut last_flushdb: std::collections::HashMap<usize, usize> =
        std::collections::HashMap::new();
    let mut db = 0usize;

    for (i, cmd) in commands.iter().enumerate() {
        if is(cmd, b"SELECT")
            && let Some(n) = cmd
                .get(1)
                .and_then(|a| std::str::from_utf8(a).ok())
                .and_then(|s| s.parse::<usize>().ok())
        {
            db = n;
        }
        db_of.push(db);
        if is(cmd, b"FLUSHALL") {
            last_flushall = Some(i);
        } else if is(cmd, b"FLUSHDB") {
            last_flushdb.insert(db, i);
        }
    }

    // Pass 2: a record survives only if it sits at or after every flush that
    // could have wiped it.
    commands
        .iter()
        .enumerate()
        .map(|(i, cmd)| {
            if is(cmd, b"SELECT") {
                return true;
            }
            if last_flushall.is_some_and(|f| i < f) {
                return false;
            }
            last_flushdb.get(&db_of[i]).is_none_or(|&f| i >= f)
        })
        .collect()
}

/// Split an AOF buffer into commands.
///
/// Returns the commands parsed and how many bytes were consumed. A trailing
/// partial record leaves `consumed < buf.len()`: a crash between `write` and
/// the record being complete is normal, and Redis truncates such a tail rather
/// than refusing to start. Anything malformed *before* the final record is a
/// different matter and stops the parse at that point, so a corrupt middle
/// cannot be silently skipped over into unrelated data.
fn parse_aof_commands(buf: &[u8]) -> (Vec<Vec<Vec<u8>>>, usize) {
    let mut out = Vec::new();
    let mut pos = 0usize;

    while let Some(rest) = buf.get(pos..).filter(|r| !r.is_empty()) {
        match parse_one_command(rest) {
            Some((cmd, used)) => {
                out.push(cmd);
                pos += used;
            }
            // Incomplete or malformed: stop here and report `pos` so the caller
            // can tell the tail was not consumed.
            None => break,
        }
    }
    (out, pos)
}

/// `MULTI` or `EXEC`, the marks around a transaction's records.
fn is_transaction_mark(cmd: &[Vec<u8>]) -> bool {
    cmd.len() == 1
        && (cmd[0].eq_ignore_ascii_case(b"MULTI") || cmd[0].eq_ignore_ascii_case(b"EXEC"))
}

/// The index of the `MULTI` of a transaction that has no `EXEC` after it,
/// if the log ends inside one.
///
/// As in Redis, a `MULTI` inside a transaction is not a second one, and an
/// `EXEC` with no transaction open closes nothing.
fn unfinished_transaction(commands: &[Vec<Vec<u8>>]) -> Option<usize> {
    let mut open = None;
    for (i, cmd) in commands.iter().enumerate() {
        if cmd.len() != 1 {
            continue;
        }
        if cmd[0].eq_ignore_ascii_case(b"MULTI") {
            open = open.or(Some(i));
        } else if cmd[0].eq_ignore_ascii_case(b"EXEC") {
            open = None;
        }
    }
    open
}

/// Where record `index` starts in `buf`, whose first `index` records are
/// whole.
fn record_offset(buf: &[u8], index: usize) -> usize {
    let mut pos = 0;
    for _ in 0..index {
        match parse_one_command(&buf[pos..]) {
            Some((_, used)) => pos += used,
            None => break,
        }
    }
    pos
}

/// What the bytes after the last whole record are.
#[derive(Debug, PartialEq, Eq)]
enum Tail {
    /// The beginning of a record that was never finished: the process died
    /// while appending. Harmless — the record was never acknowledged as
    /// durable — and it is cut off.
    Truncated,
    /// Not the beginning of a record at all. The file is damaged, and
    /// whatever follows cannot be trusted either.
    Corrupt,
}

/// Decide which of the two the unparsed remainder of an AOF is. `rest` is
/// not empty and is not a whole record.
fn classify_tail(rest: &[u8]) -> Tail {
    // The digits of a count or a length, up to its CRLF; `None` if the buffer
    // ends first. A lone `\r` at the very end is half of that CRLF.
    fn number(buf: &[u8], from: usize) -> Result<Option<(usize, usize)>, Tail> {
        match find_crlf(buf, from) {
            Some(end) => std::str::from_utf8(&buf[from..end])
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
                .map(|n| Some((n, end + 2)))
                .ok_or(Tail::Corrupt),
            None => {
                let digits = buf[from..].strip_suffix(b"\r").unwrap_or(&buf[from..]);
                if digits.len() <= 20 && digits.iter().all(u8::is_ascii_digit) {
                    Ok(None)
                } else {
                    Err(Tail::Corrupt)
                }
            }
        }
    }

    if rest.first() != Some(&b'*') {
        return Tail::Corrupt;
    }
    let (argc, mut pos) = match number(rest, 1) {
        Ok(Some(found)) => found,
        Ok(None) => return Tail::Truncated,
        Err(tail) => return tail,
    };
    for _ in 0..argc {
        match rest.get(pos) {
            None => return Tail::Truncated,
            Some(b'$') => {}
            Some(_) => return Tail::Corrupt,
        }
        let (len, data) = match number(rest, pos + 1) {
            Ok(Some(found)) => found,
            Ok(None) => return Tail::Truncated,
            Err(tail) => return tail,
        };
        match data.checked_add(len).and_then(|end| end.checked_add(2)) {
            Some(end) if end <= rest.len() => pos = end,
            Some(_) => return Tail::Truncated,
            None => return Tail::Corrupt,
        }
    }
    // Every argument is there: it is a whole record, and the caller's parser
    // refused it for some other reason.
    Tail::Corrupt
}

/// Stop the server because what is on disk cannot be loaded.
///
/// A persistence file that is *there* and cannot be read is not the same as
/// no file. Starting empty in that case looks healthy and is not: the node
/// accepts writes, its replicas copy the empty dataset, and the next save
/// replaces the file it could not read. Redis refuses to start, and so does
/// KiviDB.
pub(crate) fn fatal_persistence(what: &str) -> ! {
    log_fatal!("{what}");
    log_fatal!(
        "Refusing to start with an empty dataset in place of one that could not be \
         loaded. Fix the file (or its permissions), or move it out of the working directory \
         to start empty on purpose."
    );
    std::process::exit(1);
}

/// One `*N\r\n$len\r\n<arg>\r\n…` record. `None` ⇒ incomplete or malformed.
pub(crate) fn parse_one_command(buf: &[u8]) -> Option<(Vec<Vec<u8>>, usize)> {
    if *buf.first()? != b'*' {
        return None;
    }
    let mut pos = 0usize;
    let line_end = find_crlf(buf, pos)?;
    let argc: usize = std::str::from_utf8(&buf[pos + 1..line_end])
        .ok()?
        .parse()
        .ok()?;
    pos = line_end + 2;

    let mut args = Vec::with_capacity(argc);
    for _ in 0..argc {
        if *buf.get(pos)? != b'$' {
            return None;
        }
        let line_end = find_crlf(buf, pos)?;
        let len: usize = std::str::from_utf8(&buf[pos + 1..line_end])
            .ok()?
            .parse()
            .ok()?;
        pos = line_end + 2;
        let end = pos.checked_add(len)?;
        if buf.len() < end + 2 {
            return None;
        }
        args.push(buf[pos..end].to_vec());
        pos = end + 2;
    }
    Some((args, pos))
}

fn find_crlf(buf: &[u8], from: usize) -> Option<usize> {
    buf.get(from..)?
        .windows(2)
        .position(|w| w == b"\r\n")
        .map(|i| from + i)
}

/// Holds [`crate::vector::DEFER_INDEXING`] for a replay and clears it on the
/// way out.
///
/// A guard rather than two bare stores because leaking the flag set would
/// silently stop *all* index maintenance for the rest of the process — a
/// failure that would surface much later as an index quietly missing writes,
/// with nothing pointing back here.
struct DeferIndexing;

impl DeferIndexing {
    fn start() -> Self {
        crate::vector::DEFER_INDEXING.store(true, Ordering::Release);
        Self
    }
}

impl Drop for DeferIndexing {
    fn drop(&mut self) {
        crate::vector::DEFER_INDEXING.store(false, Ordering::Release);
    }
}

/// Rebuild the dataset by re-executing the AOF.
///
/// Commands are applied through the same environment the replica applier uses,
/// with `apply_replication: true` — so a replayed write does not propagate
/// onward or get appended back into the AOF it just came from.
///
/// A command that errors is counted and skipped rather than aborting the
/// replay: refusing to start because one record is unloadable would turn a
/// small problem into a total outage, and the rest of the log is still good.
///
/// Two things are deliberately *not* done per record. Records a later flush
/// invalidates are never applied ([`live_after_flushes`]), and index
/// maintenance is suppressed ([`DeferIndexing`]) so the caller can rebuild the
/// indexes in one pass — see [`recover_from_aof`], which must run that pass.
#[allow(clippy::too_many_arguments)]
pub fn replay_aof(
    path: &str,
    store: &crate::store::Store,
    index_store: &crate::vector::IndexStore,
    dict_store: &crate::vector::DictStore,
    wait_table: &std::sync::Arc<crate::wait_table::WaitTable>,
    ts_index: &crate::store::SharedTsIndex,
    config: &std::sync::Arc<crate::config::Config>,
    compaction_tx: &crate::vector::CompactionTx,
    repl_state: &std::sync::Arc<crate::replication::ReplState>,
    skip_preamble: bool,
) -> std::io::Result<ReplayStats> {
    let buf = std::fs::read(path)?;
    let mut stats = ReplayStats {
        bytes: buf.len() as u64,
        ..Default::default()
    };
    if buf.is_empty() {
        return Ok(stats);
    }

    let (image, tail) = split_aof(&buf)?;
    // The dump is already in the store. A KDB preamble is a second copy of the
    // same dataset; loading it would overwrite the snapshot we just restored.
    let image = if skip_preamble { None } else { image };

    let (commands, consumed) = parse_aof_commands(tail);
    stats.valid_len = (buf.len() - tail.len() + consumed) as u64;
    if consumed < tail.len() {
        match classify_tail(&tail[consumed..]) {
            Tail::Truncated => stats.truncated_tail = true,
            // Before anything is applied: a damaged log is not half-loaded.
            Tail::Corrupt => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "bad file format at byte offset {} of {} — what follows is not a \
                         command record",
                        stats.valid_len,
                        buf.len()
                    ),
                ));
            }
        }
    }
    // A transaction whose `EXEC` is not in the file — the process died while
    // it was being written — is dropped whole, and the file cut before its
    // `MULTI`, as Redis does ("Revert incomplete MULTI/EXEC transaction in AOF
    // file"). Half of it is a dataset no client ever saw. Everything after
    // that `MULTI` is in it: one whose `EXEC` never came is unfinished however
    // much follows, in Redis as here.
    let mut commands = commands;
    if let Some(multi) = unfinished_transaction(&commands) {
        stats.reverted = (commands.len() - multi) as u64;
        stats.valid_len = (buf.len() - tail.len() + record_offset(tail, multi)) as u64;
        stats.truncated_tail = true;
        commands.truncate(multi);
    }
    let live = live_after_flushes(&commands);

    // A `FLUSHALL` in the tail discards the image along with everything else
    // before it, so loading it would be pure cost. `live_after_flushes` has
    // already dropped the records on the same side of that line.
    let image = match image {
        Some(_)
            if commands.iter().any(|c| {
                c.first()
                    .is_some_and(|n| n.eq_ignore_ascii_case(b"FLUSHALL"))
            }) =>
        {
            log_info!("AOF preamble skipped — a FLUSHALL later in the log discards it anyway");
            None
        }
        other => other,
    };

    if let Some(image) = image {
        let loaded = crate::snapshot::load_snapshot_from(image)?;
        let crate::snapshot::LoadedSnapshot {
            dbs,
            index_schemas,
            dictionaries,
            hnsw_graphs,
            expiries,
            functions,
        } = loaded;
        crate::vector::restore_dictionaries(index_store, dictionaries);
        stats.preamble_keys = crate::snapshot::restore_into_store(dbs, store) as u64;
        crate::snapshot::restore_expiries_into_store(expiries, store);
        // Schemas and graphs travel inside the image, so the replay never has
        // to re-execute an `FT.CREATE` or rebuild a graph it already has.
        crate::vector::register_index_schemas(index_store, &index_schemas, &hnsw_graphs);
        crate::snapshot::restore_functions(functions)?;
    }

    let env = crate::replication::replica_client::ReplicationApplyEnv::new_inert();
    let watch_registry = crate::watch::NoopWatchRegistry;
    let mut selected_db = 0usize;

    let _indexing = DeferIndexing::start();

    for (cmd, live) in commands.into_iter().zip(live) {
        // What is left of a transaction here is whole: its commands are
        // applied in order, and nothing runs beside a replay to see them
        // half done. Its `MULTI` and `EXEC` have nothing more to do.
        if cmd.is_empty() || is_transaction_mark(&cmd) {
            continue;
        }
        if !live {
            stats.skipped += 1;
            continue;
        }
        stats.commands += 1;
        if env
            .apply_command(
                cmd,
                store,
                repl_state,
                index_store,
                dict_store,
                wait_table,
                ts_index,
                config,
                compaction_tx,
                &mut selected_db,
                &watch_registry,
                "aof-replay",
            )
            .is_err()
        {
            stats.errors += 1;
        }
    }

    Ok(stats)
}

pub const AOF_PATH: &str = "appendonly.aof";

/// Should startup rebuild from the AOF instead of a snapshot?
///
/// Only when AOF is enabled *and* a non-empty AOF exists. An enabled-but-empty
/// AOF means this is a fresh start with `--aof`, where the snapshot (if any) is
/// still the right source.
pub fn should_recover_from_aof(aof_enabled: bool) -> bool {
    if !aof_enabled {
        return false;
    }
    match std::path::Path::new(AOF_PATH).metadata() {
        Ok(meta) => meta.len() > 0,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
        Err(e) => fatal_persistence(&format!("{AOF_PATH} exists but cannot be examined: {e}")),
    }
}

/// What local recovery restored, so the caller can bind and backfill indexes.
pub struct LocalRecovery {
    pub index_names: Vec<String>,
    pub hnsw_loaded: bool,
    /// The dataset in memory is `dump.kdb` and nothing else: no append-only
    /// file was replayed on top of it. Only then does a resync point saved
    /// beside the dump describe what was loaded.
    pub exact_dump: bool,
}

/// Load `dump.kdb` (or `dump.rdb`) first, then replay the AOF on top.
///
/// The dump is the bulk of the dataset. The AOF is the delta since the last
/// checkpoint — including writes that landed while a cloud volume snapshot was
/// copying the disk. A non-empty AOF no longer skips the dump.
#[allow(clippy::too_many_arguments)]
pub fn recover_local(
    aof_enabled: bool,
    store: &crate::store::Store,
    index_store: &crate::vector::IndexStore,
    dict_store: &crate::vector::DictStore,
    wait_table: &std::sync::Arc<crate::wait_table::WaitTable>,
    ts_index: &crate::store::SharedTsIndex,
    config: &std::sync::Arc<crate::config::Config>,
    compaction_tx: &crate::vector::CompactionTx,
    repl_state: &std::sync::Arc<crate::replication::ReplState>,
) -> LocalRecovery {
    let mut recovery = LocalRecovery {
        index_names: Vec::new(),
        hnsw_loaded: false,
        exact_dump: false,
    };

    // `preload-file`: load exactly this, and nothing else. An operator who
    // names a file to restore from must not come up on some other dataset —
    // or an empty one — because the file was missing or damaged.
    if !config.preload_file.is_empty() {
        match preload(
            &config.preload_file,
            store,
            index_store,
            dict_store,
            wait_table,
            ts_index,
            config,
            compaction_tx,
            repl_state,
        ) {
            Ok(names) => {
                recovery.index_names = names;
                return recovery;
            }
            Err(e) => {
                log_fatal!(
                    "preload-file '{}' could not be loaded: {e}",
                    config.preload_file
                );
                std::process::exit(1);
            }
        }
    }

    let kdb_path = std::path::Path::new("dump.kdb");
    let rdb_path = std::path::Path::new("dump.rdb");
    let mut dump_loaded = false;

    if kdb_path.exists() {
        log_info!("Found dump.kdb — loading native snapshot");
        match crate::snapshot::load_snapshot("dump.kdb") {
            Ok(loaded) => {
                recovery.exact_dump = true;
                let crate::snapshot::LoadedSnapshot {
                    dbs,
                    index_schemas,
                    dictionaries,
                    hnsw_graphs,
                    expiries,
                    functions,
                } = loaded;
                if let Err(e) = crate::snapshot::restore_functions(functions) {
                    fatal_persistence(&format!("dump.kdb cannot be loaded: {e}"));
                }
                crate::vector::restore_dictionaries(index_store, dictionaries);
                if !dbs.is_empty() {
                    let total = crate::snapshot::restore_into_store(dbs, store);
                    let with_ttl = crate::snapshot::restore_expiries_into_store(expiries, store);
                    log_info!("Loaded {total} keys from snapshot ({with_ttl} with a TTL)");
                    dump_loaded = true;
                }
                if !index_schemas.is_empty() {
                    recovery.hnsw_loaded = !hnsw_graphs.is_empty();
                    crate::vector::register_index_schemas(
                        index_store,
                        &index_schemas,
                        &hnsw_graphs,
                    );
                    recovery.index_names = index_schemas.iter().map(|s| s.name.clone()).collect();
                    if recovery.hnsw_loaded {
                        log_info!("HNSW graph(s) loaded from snapshot — precision preserved");
                    }
                }
            }
            Err(e) => fatal_persistence(&format!("dump.kdb cannot be loaded: {e}")),
        }
    } else if rdb_path.exists() {
        log_info!("Found dump.rdb — loading Redis RDB snapshot");
        match crate::snapshot::load_redis_rdb_with_expiry("dump.rdb") {
            Ok((dbs, expiries, functions)) if !dbs.is_empty() => {
                if let Err(e) = crate::snapshot::restore_functions(functions) {
                    fatal_persistence(&format!("dump.rdb cannot be loaded: {e}"));
                }
                let total = crate::snapshot::restore_into_store(dbs, store);
                let with_ttl = crate::snapshot::restore_expiries_into_store(expiries, store);
                log_info!("Loaded {total} keys from snapshot ({with_ttl} with a TTL)");
                dump_loaded = true;
            }
            Ok(_) => {}
            Err(e) => fatal_persistence(&format!("dump.rdb cannot be loaded: {e}")),
        }
    }

    if should_recover_from_aof(aof_enabled) {
        let buf = match std::fs::read(AOF_PATH) {
            Ok(buf) => buf,
            Err(e) => fatal_persistence(&format!("{AOF_PATH} cannot be read: {e}")),
        };
        // A checkpoint turned the log over and was stopped before its
        // snapshot reached the disk: the old log, then the new one, on top of
        // the old snapshot. One that did finish only left the old log behind.
        let mut resumed_checkpoint = false;
        if std::path::Path::new(AOF_PREV_PATH).exists() {
            if checkpoint_completed(&buf) {
                let _ = std::fs::remove_file(AOF_PREV_PATH);
            } else {
                log_warn!(
                    "AOF: a checkpoint did not finish — replaying {AOF_PREV_PATH} and then \
                     {AOF_PATH} on top of the snapshot it started from"
                );
                resumed_checkpoint = true;
                recovery.exact_dump = false;
                if let Err(e) = replay_aof(
                    AOF_PREV_PATH,
                    store,
                    index_store,
                    dict_store,
                    wait_table,
                    ts_index,
                    config,
                    compaction_tx,
                    repl_state,
                    dump_loaded,
                ) {
                    fatal_persistence(&format!("{AOF_PREV_PATH} cannot be replayed: {e}"));
                }
            }
        }
        // A delta written on top of a *later* dump than the one here: the two
        // files are not from the same moment — copied separately, with a save
        // in between. What was written between the two dumps is in neither.
        if let (Some(aof_generation), dump_generation) =
            (aof_delta_generation(&buf), read_generation())
            && !resumed_checkpoint
            && dump_generation > 0
            && aof_generation > dump_generation
        {
            fatal_persistence(&format!(
                "{AOF_PATH} continues from save number {aof_generation}, but dump.kdb is save \
                 number {dump_generation}: the two files do not belong together, and the writes \
                 made between those saves are in neither"
            ));
        }
        if dump_loaded && aof_delta_is_stale(&buf) {
            log_info!(
                "AOF delta is older than dump.kdb — skipping it so commands \
                 already in the snapshot are not applied twice"
            );
        } else {
            if dump_loaded {
                log_info!("Replaying {AOF_PATH} on top of the snapshot (writes since the dump)");
                if aof_delta_generation(&buf).is_none() {
                    log_warn!(
                        "{AOF_PATH} is a full-history log, not a delta. Replaying it on \
                         top of dump.kdb can double-apply INCR/RPUSH/APPEND. SAVE will rebase \
                         the AOF to a delta."
                    );
                }
            }
            let empty_delta =
                aof_delta_generation(&buf).is_some() && buf.len() <= AOF_DELTA_HEADER_LEN;
            if empty_delta {
                // dump.kdb is the whole dataset; the log has nothing to add.
            } else {
                recovery.exact_dump = false;
                let names = recover_from_aof(
                    store,
                    index_store,
                    dict_store,
                    wait_table,
                    ts_index,
                    config,
                    compaction_tx,
                    repl_state,
                    dump_loaded,
                );
                if recovery.index_names.is_empty() {
                    recovery.index_names = names;
                }
                // Delta writes are not in the snapshot's HNSW graph.
                if dump_loaded {
                    recovery.hnsw_loaded = false;
                }
            }
        }
    }

    recovery
}

/// Load a `preload-file` target: `aof:<manifest>` (a sealed backup: its base
/// snapshot, then its incremental file replayed on top) or `rdb:<file>` (one
/// snapshot, in KiviDB's format or Redis's). Returns the names of the indexes
/// it brought along.
#[allow(clippy::too_many_arguments)]
fn preload(
    target: &str,
    store: &crate::store::Store,
    index_store: &crate::vector::IndexStore,
    dict_store: &crate::vector::DictStore,
    wait_table: &std::sync::Arc<crate::wait_table::WaitTable>,
    ts_index: &crate::store::SharedTsIndex,
    config: &std::sync::Arc<crate::config::Config>,
    compaction_tx: &crate::vector::CompactionTx,
    repl_state: &std::sync::Arc<crate::replication::ReplState>,
) -> std::io::Result<Vec<String>> {
    let invalid = |msg: String| std::io::Error::new(std::io::ErrorKind::InvalidInput, msg);
    let (kind, path) = target
        .split_once(':')
        .ok_or_else(|| invalid("expected aof:<manifest> or rdb:<file>".into()))?;
    let path = std::path::Path::new(path);

    let (base, incr) = match kind.to_ascii_lowercase().as_str() {
        "rdb" => (path.to_path_buf(), None),
        "aof" => {
            let (base, incr) =
                crate::commands::backup::parse_manifest(&std::fs::read_to_string(path)?)?;
            // The manifest names its files relative to itself.
            let dir = path.parent().unwrap_or_else(|| std::path::Path::new(""));
            (dir.join(base), incr.map(|incr| dir.join(incr)))
        }
        other => return Err(invalid(format!("unknown preload type '{other}'"))),
    };

    log_info!("preload-file: loading {}", base.display());
    let base_str = base.to_string_lossy();
    let is_redis_rdb = std::fs::File::open(&base)
        .and_then(|mut f| {
            let mut magic = [0u8; 5];
            std::io::Read::read_exact(&mut f, &mut magic).map(|()| magic)
        })
        .is_ok_and(|magic| &magic == b"REDIS");
    let mut index_names = Vec::new();
    let keys = if is_redis_rdb {
        let (dbs, expiries, functions) = crate::snapshot::load_redis_rdb_with_expiry(&base_str)?;
        crate::snapshot::restore_functions(functions)?;
        let keys = crate::snapshot::restore_into_store(dbs, store);
        crate::snapshot::restore_expiries_into_store(expiries, store);
        keys
    } else {
        let crate::snapshot::LoadedSnapshot {
            dbs,
            index_schemas,
            dictionaries,
            hnsw_graphs,
            expiries,
            functions,
        } = crate::snapshot::load_snapshot(&base_str)?;
        crate::snapshot::restore_functions(functions)?;
        crate::vector::restore_dictionaries(index_store, dictionaries);
        let keys = crate::snapshot::restore_into_store(dbs, store);
        crate::snapshot::restore_expiries_into_store(expiries, store);
        crate::vector::register_index_schemas(index_store, &index_schemas, &hnsw_graphs);
        index_names = index_schemas.into_iter().map(|s| s.name).collect();
        keys
    };
    crate::store::rebuild_ts_label_index(store, ts_index);
    log_info!("preload-file: {keys} keys from the base snapshot");

    if let Some(incr) = incr {
        let stats = replay_aof(
            &incr.to_string_lossy(),
            store,
            index_store,
            dict_store,
            wait_table,
            ts_index,
            config,
            compaction_tx,
            repl_state,
            false,
        )?;
        log_info!(
            "preload-file: {} commands replayed from {}",
            stats.commands,
            incr.display()
        );
        if stats.truncated_tail {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "the incremental file ends in a partial record — a sealed backup never does",
            ));
        }
        // An index created during the backup window is in the replayed
        // commands, not in the base snapshot.
        index_names = index_store.read().keys().cloned().collect();
    }
    Ok(index_names)
}

/// Replay the AOF into the store, logging the outcome.
///
/// When `dump_already_loaded` is set, a KDB preamble in the AOF is skipped —
/// the dump is the base, the command stream is the delta.
#[allow(clippy::too_many_arguments)]
#[must_use = "indexes are left empty by the replay and must be backfilled"]
pub fn recover_from_aof(
    store: &crate::store::Store,
    index_store: &crate::vector::IndexStore,
    dict_store: &crate::vector::DictStore,
    wait_table: &std::sync::Arc<crate::wait_table::WaitTable>,
    ts_index: &crate::store::SharedTsIndex,
    config: &std::sync::Arc<crate::config::Config>,
    compaction_tx: &crate::vector::CompactionTx,
    repl_state: &std::sync::Arc<crate::replication::ReplState>,
    dump_already_loaded: bool,
) -> Vec<String> {
    log_info!("Found {AOF_PATH} — recovering from the append-only file");
    let started = std::time::Instant::now();
    match replay_aof(
        AOF_PATH,
        store,
        index_store,
        dict_store,
        wait_table,
        ts_index,
        config,
        compaction_tx,
        repl_state,
        dump_already_loaded,
    ) {
        Ok(stats) => {
            let keys: usize = store
                .iter()
                .map(|db| {
                    db.iter()
                        .map(|shard| crate::read_lock!(shard).len())
                        .sum::<usize>()
                })
                .sum();
            log_info!(
                "AOF replay: {} commands, {} keys, {:.2} MB in {:.2}s",
                stats.commands,
                keys,
                stats.bytes as f64 / (1024.0 * 1024.0),
                started.elapsed().as_secs_f64()
            );
            if stats.preamble_keys > 0 {
                log_info!(
                    "AOF preamble: {} of those keys came from the KDB image, \
                     not from replayed records",
                    stats.preamble_keys
                );
            }
            if stats.skipped > 0 {
                log_info!(
                    "AOF replay: {} record(s) skipped — a later FLUSHDB/FLUSHALL \
                     had already discarded them",
                    stats.skipped
                );
            }
            if stats.errors > 0 {
                log_warn!(
                    "{} AOF command(s) failed to apply and were skipped",
                    stats.errors
                );
            }
            if stats.truncated_tail {
                // Normal after a hard kill: the process died part-way through
                // appending. Say so explicitly rather than leaving a silently
                // shorter dataset to be discovered later.
                if stats.reverted > 0 {
                    log_warn!(
                        "Revert incomplete MULTI/EXEC transaction in AOF file: its EXEC is \
                         not in the file, and its {} record(s) are dropped",
                        stats.reverted
                    );
                }
                log_warn!(
                    "AOF ended with a partial record or transaction (likely an unclean \
                     shutdown); the incomplete tail ({} bytes) is cut off",
                    stats.bytes - stats.valid_len
                );
                // Cut it off the file, too. Left there, the next record
                // appended would follow half a record — and from then on the
                // log is damaged in the middle, not at the end.
                let trimmed = std::fs::OpenOptions::new()
                    .write(true)
                    .open(AOF_PATH)
                    .and_then(|file| {
                        file.set_len(stats.valid_len)?;
                        file.sync_all()
                    });
                if let Err(e) = trimmed {
                    fatal_persistence(&format!(
                        "{AOF_PATH} ends in a partial record that could not be cut off: {e}"
                    ));
                }
            }
        }
        Err(e) => fatal_persistence(&format!("{AOF_PATH} cannot be loaded: {e}")),
    }

    index_store.read().keys().cloned().collect()
}

// ── Checkpoint in progress ───────────────────────────────────────────────────

/// True while [`checkpoint`] holds the store. `INFO persistence` reports it as
/// `aof_rewrite_in_progress`, and the auto-checkpoint thread skips overlapping
/// runs.
pub static REWRITE_ACTIVE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

#[cfg(test)]
mod replay_parse_tests {
    use super::*;

    #[test]
    fn a_record_cut_short_is_a_truncation_and_anything_else_is_corruption() {
        let whole = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nvalue\r\n";
        assert!(parse_one_command(whole).is_some());
        // Every proper prefix of a record is a truncation.
        for cut in 1..whole.len() {
            assert_eq!(
                classify_tail(&whole[..cut]),
                Tail::Truncated,
                "cut at {cut}: {:?}",
                String::from_utf8_lossy(&whole[..cut])
            );
        }
        for damaged in [
            &b"this is not a record"[..],
            b"\x00\x01\x02",
            b"*x\r\n",
            b"*2\r\nSET\r\n",
            b"*2\r\n$z\r\n",
            b"*1\r\n$3junk-with-no-line-end",
            b"KIVI-something",
        ] {
            assert_eq!(classify_tail(damaged), Tail::Corrupt, "{damaged:?}");
        }
    }

    fn enc(parts: &[&str]) -> Vec<u8> {
        let mut v = format!("*{}\r\n", parts.len()).into_bytes();
        for p in parts {
            v.extend_from_slice(format!("${}\r\n{}\r\n", p.len(), p).as_bytes());
        }
        v
    }

    #[test]
    fn parses_a_sequence_of_commands() {
        let mut buf = enc(&["SELECT", "0"]);
        buf.extend_from_slice(&enc(&["SET", "a", "1"]));
        buf.extend_from_slice(&enc(&["RPUSH", "l", "x", "y"]));
        let (cmds, used) = parse_aof_commands(&buf);
        assert_eq!(used, buf.len(), "whole buffer should be consumed");
        assert_eq!(cmds.len(), 3);
        assert_eq!(cmds[1], vec![b"SET".to_vec(), b"a".to_vec(), b"1".to_vec()]);
        assert_eq!(cmds[2].len(), 4);
    }

    #[test]
    fn binary_values_survive_the_round_trip() {
        // Values are arbitrary bytes, including CRLF — a line-oriented parse
        // would split this record in the middle of its payload.
        let mut buf = Vec::new();
        let val = b"bin\r\n\x00\xffdata";
        buf.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n");
        buf.extend_from_slice(format!("${}\r\n", val.len()).as_bytes());
        buf.extend_from_slice(val);
        buf.extend_from_slice(b"\r\n");
        let (cmds, used) = parse_aof_commands(&buf);
        assert_eq!(used, buf.len());
        assert_eq!(cmds.len(), 1);
        assert_eq!(cmds[0][2], val.to_vec());
    }

    #[test]
    fn a_torn_final_record_is_dropped_but_the_prefix_is_kept() {
        // Exactly what a crash mid-write leaves behind.
        let mut buf = enc(&["SET", "a", "1"]);
        let good = buf.len();
        buf.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$5\r\nunfin");
        let (cmds, used) = parse_aof_commands(&buf);
        assert_eq!(cmds.len(), 1, "only the complete record applies");
        assert_eq!(used, good, "the torn tail is not consumed");
        assert!(used < buf.len());
    }

    #[test]
    fn an_empty_file_yields_nothing() {
        let (cmds, used) = parse_aof_commands(b"");
        assert!(cmds.is_empty());
        assert_eq!(used, 0);
    }

    #[test]
    fn a_truncated_length_header_is_not_consumed() {
        let buf = b"*3\r\n$3\r\nSET\r\n$1";
        let (cmds, used) = parse_aof_commands(buf);
        assert!(cmds.is_empty());
        assert_eq!(used, 0);
    }

    #[test]
    fn a_transaction_without_its_exec_is_found_where_it_starts() {
        let mut buf = enc(&["SET", "a", "1"]);
        buf.extend_from_slice(&enc(&["MULTI"]));
        buf.extend_from_slice(&enc(&["SET", "b", "1"]));
        buf.extend_from_slice(&enc(&["SET", "c", "1"]));
        buf.extend_from_slice(&enc(&["EXEC"]));
        let whole = enc(&["SET", "a", "1"]).len();
        let first_multi = buf.len() - whole;
        buf.extend_from_slice(&enc(&["MULTI"]));
        buf.extend_from_slice(&enc(&["SET", "d", "1"]));
        let (cmds, _) = parse_aof_commands(&buf);
        assert_eq!(
            unfinished_transaction(&cmds[..5]),
            None,
            "closed by its EXEC"
        );
        assert_eq!(unfinished_transaction(&cmds), Some(5));
        assert_eq!(record_offset(&buf, 5), whole + first_multi);
        assert_eq!(record_offset(&buf, 1), whole);
        // A `MULTI` inside one is not a second: the first is where it starts.
        let mut nested = cmds.clone();
        nested.push(vec![b"MULTI".to_vec()]);
        assert_eq!(unfinished_transaction(&nested), Some(5));
        // An `EXEC` with nothing open closes nothing.
        assert_eq!(unfinished_transaction(&[vec![b"EXEC".to_vec()]]), None);
        assert!(is_transaction_mark(&cmds[1]) && is_transaction_mark(&cmds[4]));
        assert!(!is_transaction_mark(&cmds[0]));
    }
}

#[cfg(test)]
mod flush_skip_tests {
    use super::*;

    fn cmds(rows: &[&[&str]]) -> Vec<Vec<Vec<u8>>> {
        rows.iter()
            .map(|r| r.iter().map(|s| s.as_bytes().to_vec()).collect())
            .collect()
    }

    #[test]
    fn everything_before_a_flushall_is_dead() {
        let c = cmds(&[
            &["SET", "gone", "1"],
            &["INCR", "gone"],
            &["FLUSHALL"],
            &["SET", "kept", "1"],
        ]);
        assert_eq!(live_after_flushes(&c), vec![false, false, true, true]);
    }

    #[test]
    fn a_flushdb_only_kills_its_own_database() {
        // db 0 is flushed; the db 1 write straddling it must survive, or the
        // skip would silently drop data the flush never touched.
        let c = cmds(&[
            &["SET", "db0-gone", "1"],
            &["SELECT", "1"],
            &["SET", "db1-kept", "1"],
            &["SELECT", "0"],
            &["FLUSHDB"],
            &["SET", "db0-kept", "1"],
        ]);
        assert_eq!(
            live_after_flushes(&c),
            vec![false, true, true, true, true, true]
        );
    }

    #[test]
    fn select_is_never_dropped() {
        // The cursor decides which flush applies to which record, so dropping a
        // SELECT would misattribute every record after it.
        let c = cmds(&[&["SELECT", "3"], &["SET", "a", "1"], &["FLUSHALL"]]);
        let live = live_after_flushes(&c);
        assert!(live[0], "SELECT survives even ahead of a FLUSHALL");
        assert!(!live[1]);
    }

    #[test]
    fn a_log_with_no_flush_keeps_everything() {
        let c = cmds(&[&["SET", "a", "1"], &["INCR", "a"], &["DEL", "a"]]);
        assert_eq!(live_after_flushes(&c), vec![true, true, true]);
    }

    #[test]
    fn the_last_flush_wins_when_there_are_several() {
        let c = cmds(&[
            &["SET", "a", "1"],
            &["FLUSHALL"],
            &["SET", "b", "1"],
            &["FLUSHALL"],
            &["SET", "c", "1"],
        ]);
        assert_eq!(
            live_after_flushes(&c),
            vec![false, false, false, true, true]
        );
    }

    #[test]
    fn case_does_not_matter() {
        let c = cmds(&[&["set", "a", "1"], &["flushall"], &["set", "b", "1"]]);
        assert_eq!(live_after_flushes(&c), vec![false, true, true]);
    }
}

#[cfg(test)]
mod preamble_framing_tests {
    use super::*;

    fn framed(image: &[u8], tail: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(AOF_MAGIC);
        buf.push(AOF_FLAG_PREAMBLE);
        buf.extend_from_slice(&(image.len() as u64).to_le_bytes());
        buf.extend_from_slice(image);
        buf.extend_from_slice(tail);
        buf
    }

    #[test]
    fn a_legacy_log_is_all_tail() {
        // Files written before the preamble existed must keep loading, and they
        // are recognised by opening with a RESP array rather than the magic.
        let buf = b"*1\r\n$4\r\nPING\r\n";
        let (image, tail) = split_aof(buf).expect("legacy log parses");
        assert!(image.is_none());
        assert_eq!(tail, buf);
    }

    #[test]
    fn an_image_and_its_tail_are_separated() {
        let buf = framed(b"KIVIDB01-image-bytes", b"*1\r\n$4\r\nPING\r\n");
        let (image, tail) = split_aof(&buf).expect("framed log parses");
        assert_eq!(image, Some(&b"KIVIDB01-image-bytes"[..]));
        assert_eq!(tail, b"*1\r\n$4\r\nPING\r\n");
    }

    #[test]
    fn an_empty_tail_is_fine() {
        let buf = framed(b"image", b"");
        let (image, tail) = split_aof(&buf).expect("framed log parses");
        assert_eq!(image, Some(&b"image"[..]));
        assert!(tail.is_empty());
    }

    #[test]
    fn a_truncated_image_refuses_to_load() {
        // Unlike a torn tail, this cannot happen from an ordinary crash — the
        // image arrives by atomic rename. Loading a fraction of the dataset and
        // calling it recovered is the failure this guards against.
        let mut buf = framed(b"the-whole-image", b"");
        buf.truncate(buf.len() - 4);
        let err = split_aof(&buf).expect_err("a short image must not load");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn a_delta_header_is_not_an_image() {
        let mut buf = Vec::new();
        buf.extend_from_slice(AOF_DELTA_MAGIC);
        buf.extend_from_slice(&7u64.to_le_bytes());
        buf.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");
        let (image, tail) = split_aof(&buf).expect("delta log parses");
        assert!(image.is_none());
        assert_eq!(tail, b"*1\r\n$4\r\nPING\r\n");
    }

    #[test]
    fn an_empty_delta_has_no_tail() {
        let mut buf = Vec::new();
        buf.extend_from_slice(AOF_DELTA_MAGIC);
        buf.extend_from_slice(&1u64.to_le_bytes());
        let (image, tail) = split_aof(&buf).expect("empty delta parses");
        assert!(image.is_none());
        assert!(tail.is_empty());
    }

    #[test]
    fn a_header_without_the_preamble_flag_is_all_tail() {
        let mut buf = Vec::new();
        buf.extend_from_slice(AOF_MAGIC);
        buf.push(0);
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");
        let (image, tail) = split_aof(&buf).expect("flagless header parses");
        assert!(image.is_none());
        assert_eq!(tail, b"*1\r\n$4\r\nPING\r\n");
    }

    #[test]
    fn a_header_cut_off_mid_way_is_rejected() {
        let buf = &AOF_MAGIC[..];
        let err = split_aof(buf).expect_err("a partial header must not load");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }
}

#[cfg(test)]
mod auto_rewrite_tests {
    use super::*;

    /// `should_auto_rewrite` reads process-global state, so these cases drive
    /// the ratio directly rather than through the file.
    fn ratio_triggers(current: u64, base: u64, percentage: u64) -> bool {
        if base == 0 {
            return true;
        }
        current
            .saturating_sub(base)
            .saturating_mul(100)
            .checked_div(base)
            .is_some_and(|growth| growth >= percentage)
    }

    #[test]
    fn doubling_hits_the_default_threshold() {
        assert!(ratio_triggers(200, 100, 100));
        assert!(!ratio_triggers(199, 100, 100));
    }

    #[test]
    fn an_empty_baseline_always_triggers() {
        // Nothing has been rewritten in this process, so the whole log is
        // growth — there is no ratio to compute.
        assert!(ratio_triggers(1, 0, 100));
    }

    #[test]
    fn a_disabled_percentage_never_triggers() {
        assert!(!should_auto_rewrite(0, 0));
    }

    #[test]
    fn a_log_below_the_floor_never_triggers() {
        // Guards the case the floor exists for: a tiny log doubling constantly.
        assert!(!should_auto_rewrite(100, u64::MAX));
    }
}

#[cfg(test)]
mod record_group_tests {
    use super::*;

    #[derive(Default)]
    struct Sent(parking_lot::Mutex<Vec<Vec<u8>>>);

    impl crate::commands::ctx::AofSender for Sent {
        fn send_aof(&self, data: Vec<u8>) {
            self.0.lock().push(data);
        }
    }

    #[test]
    fn a_scripts_records_are_queued_together_when_it_ends() {
        let sent = Sent::default();
        queue_record(&sent, b"before".to_vec());
        {
            let _script = RecordGroup::begin(&sent);
            queue_record(&sent, b"one,".to_vec());
            {
                // A script called by the script: still the one group.
                let _inner = RecordGroup::begin(&sent);
                queue_record(&sent, b"two,".to_vec());
            }
            queue_record(&sent, b"three".to_vec());
            assert_eq!(
                *sent.0.lock(),
                [b"before".to_vec()],
                "held until the script ends"
            );
        }
        queue_record(&sent, b"after".to_vec());
        assert_eq!(
            *sent.0.lock(),
            [
                b"before".to_vec(),
                b"one,two,three".to_vec(),
                b"after".to_vec()
            ]
        );

        // A script that writes nothing queues nothing.
        drop(RecordGroup::begin(&sent));
        assert_eq!(sent.0.lock().len(), 3);
    }
}
