use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc;

pub static AOF_WRITTEN: AtomicU64 = AtomicU64::new(0);
pub static AOF_FSYNCED: AtomicU64 = AtomicU64::new(0);
/// Records that never reached the writer. Should stay 0 outside shutdown; a
/// non-zero value means the AOF has a hole and cannot reconstruct the dataset.
pub static AOF_LOST_RECORDS: AtomicU64 = AtomicU64::new(0);
/// Replies to writes sent after waiting [`ACK_WAIT_LIMIT`] for their records
/// to reach the file, without them (`aof_acks_before_write` in `INFO`). Each
/// is a write a process killed right then could lose: the disk had fallen
/// that far behind.
pub static AOF_ACKS_BEFORE_WRITE: AtomicU64 = AtomicU64::new(0);

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
/// naturally; flipping to true immediately starts routing writes, so
/// the log is opened first (see [`open_log`]).
///
/// The rule: while this is on, the log is open (`LOG` holds it). It is
/// opened before the flag first goes up, at startup or by `CONFIG SET
/// appendonly yes`, and never closed. A checkpoint's turn-over waits for
/// whoever holds the log to reach its mark: with no log, it would wait for
/// ever.
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
    REWRITE_ACTIVE.load(Ordering::Relaxed) || REWRITE_ASKED.load(Ordering::Relaxed)
}

/// `BGREWRITEAOF`'s thread: whether it is running, and whether another
/// rewrite was asked for while it ran.
struct BackgroundRewrite {
    running: bool,
    again: bool,
}

static BACKGROUND_REWRITE: parking_lot::Mutex<BackgroundRewrite> =
    parking_lot::Mutex::new(BackgroundRewrite {
        running: false,
        again: false,
    });

/// Set while `BGREWRITEAOF`'s thread runs, for [`rewrite_in_progress`].
static REWRITE_ASKED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// `BGREWRITEAOF`: a checkpoint, on a thread of its own.
///
/// One thread, however many are asked for. Each used to have a thread of its
/// own, and nothing kept them apart: a client asking every few milliseconds
/// had hundreds of checkpoints running at once, holding writes back in turn,
/// sharing a save number, and finishing in any order — an older snapshot
/// could replace a newer one. Asked while one runs, this notes that another
/// is wanted, and the thread runs exactly one more when the one it is on
/// has finished. That one starts after this call, so what was acknowledged
/// before the call is in a snapshot that finishes after it; every call made
/// in the meantime is served by the same one.
///
/// It is in progress from this moment, not from the moment that thread gets
/// to it: a client that asks for a rewrite and then waits for
/// `aof_rewrite_in_progress` to read 0 used to be told 0 at once, before the
/// rewrite had begun, and go on as if it were done.
pub fn checkpoint_in_background(
    store: crate::store::Store,
    index_store: crate::vector::IndexStore,
) {
    {
        let mut rewrite = BACKGROUND_REWRITE.lock();
        if rewrite.running {
            rewrite.again = true;
            return;
        }
        rewrite.running = true;
        REWRITE_ASKED.store(true, Ordering::Release);
    }
    fn stopped() {
        let mut rewrite = BACKGROUND_REWRITE.lock();
        rewrite.running = false;
        rewrite.again = false;
        REWRITE_ASKED.store(false, Ordering::Release);
    }
    /// A thread that panics stops being the one that runs: the next
    /// `BGREWRITEAOF` starts another.
    struct Unwinding;
    impl Drop for Unwinding {
        fn drop(&mut self) {
            if std::thread::panicking() {
                stopped();
            }
        }
    }
    let spawned = std::thread::Builder::new()
        .name("kividb-bgrewrite".into())
        .spawn(move || {
            let _unwinding = Unwinding;
            loop {
                BACKGROUND_REWRITE.lock().again = false;
                if let Err(e) = checkpoint(&store, &index_store) {
                    log_warn!("AOF: BGREWRITEAOF failed: {e}");
                }
                // Decided under the lock a caller takes to ask for one more:
                // one asked for after this starts a thread of its own.
                let mut rewrite = BACKGROUND_REWRITE.lock();
                if !rewrite.again {
                    rewrite.running = false;
                    REWRITE_ASKED.store(false, Ordering::Release);
                    return;
                }
            }
        });
    if let Err(e) = spawned {
        stopped();
        log_error!("AOF: BGREWRITEAOF could not start its thread: {e}");
    }
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
/// What each is sent is the old log, still to be synced, if it has one.
static TURN_OVER_WAITERS: parking_lot::Mutex<
    Vec<(u64, std::sync::mpsc::SyncSender<std::io::Result<OldLog>>)>,
> = parking_lot::Mutex::new(Vec::new());

/// The log a turn-over moved aside, as the writer leaves it to the
/// checkpoint, with the number of the turn-over (see [`OLD_LOG_UNSYNCED`]):
/// `None` if it needs nothing more.
type OldLog = Option<(std::fs::File, u64)>;

/// Set from a turn-over until the old log it moved aside is synced: the
/// writer does not sync the new log before then. Records in the new log
/// come after those in the old one, and a power failure must not keep them
/// and lose the old one's last records.
///
/// The number of the turn-over (see [`TURN_OVERS`]), 0 for none, and cleared
/// only by a sync made after that turn-over: one that found an earlier
/// turn-over's number, and synced that one's old log, must not clear the
/// mark of a turn-over made while it synced. The writer did, and reported
/// the records after the second synced with its renames not on the disk
/// (#529).
static OLD_LOG_UNSYNCED: AtomicU64 = AtomicU64::new(0);

/// Turn-overs made in this process, numbering them for [`OLD_LOG_UNSYNCED`].
static TURN_OVERS: AtomicU64 = AtomicU64::new(0);

/// Clear [`OLD_LOG_UNSYNCED`] after a sync of the old log begun when it read
/// `seen`: only if no turn-over has been made since.
fn old_log_synced(seen: u64) {
    let _ = OLD_LOG_UNSYNCED.compare_exchange(seen, 0, Ordering::AcqRel, Ordering::Acquire);
}

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

/// The queue, while no log is open: the AOF has been off since the start.
/// See [`open_log`].
static UNOPENED: parking_lot::Mutex<Option<mpsc::UnboundedReceiver<Vec<u8>>>> =
    parking_lot::const_mutex(None);

/// Set when the dataset has moved on without the open log: a dump was saved
/// with the AOF off (see [`dump_saved_without_log`]), or a replica's full
/// sync replaced the dataset. Cleared when a checkpoint turns the log over,
/// or a new log is started. While it is set, the AOF turned on again starts
/// a new log (see [`open_log`]).
static LOG_SUPERSEDED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// The dataset has moved on without the log. See [`LOG_SUPERSEDED`].
pub fn log_superseded() {
    LOG_SUPERSEDED.store(true, Ordering::Relaxed);
}

/// Get the log ready for `CONFIG SET appendonly yes`, with the turn of the
/// checkpoint that follows held. Call before the flag goes up, so that a
/// record logged once it is up has a file to go to. A file that cannot be
/// made is an error for the caller, not an exit: the AOF stays off.
///
/// The log starts again, empty (see [`start_new_log`]), unless it is the one
/// this process has had open all along and nothing has moved on without it
/// (#350). An `appendonly.aof` left by an earlier run with the AOF on is not
/// the dataset this process serves, and the log kept through `appendonly no`
/// is older than a `SAVE` made since. Appended to, either became the enable's
/// `appendonly.aof.prev`, and a crash before the enable's snapshot was on
/// disk replayed it on top of the newer data: overwritten values, and keys
/// deleted since, came back. Otherwise the open log is kept: with the dump it
/// continues from, it holds what was written while the AOF was on. Kept, it
/// must also be one recovery would replay over the dump on disk (see
/// [`DumpOnDisk`]).
pub fn open_log(_turn: &CheckpointTurn) -> std::io::Result<()> {
    let made = |e: std::io::Error| {
        std::io::Error::new(e.kind(), format!("a new {AOF_PATH} cannot be made: {e}"))
    };
    let mut log = LOG.lock();
    if let Some(log) = log.as_mut() {
        let on_disk = DumpOnDisk::read(std::path::Path::new("dump.kdb").exists());
        if LOG_SUPERSEDED.load(Ordering::Relaxed) || on_disk.age_of(AOF_PATH) != LogAge::GoesOn {
            // What is still queued goes to the old log, and the barriers in
            // it are answered. No turn-over mark is: the turn is held.
            write_queued(&mut log.file, &mut log.rx, None);
            log.file = start_new_log(NewLogWhy::Enable, &[]).map_err(made)?;
        }
        return Ok(());
    }
    let mut unopened = UNOPENED.lock();
    let Some(rx) = unopened.take() else {
        return Err(std::io::Error::other("the AOF writer has not started yet"));
    };
    match start_new_log(NewLogWhy::EnableOverEarlierRun, &[]) {
        Ok(file) => {
            *log = Some(Log { rx, file });
            Ok(())
        }
        Err(e) => {
            *unopened = Some(rx);
            Err(made(e))
        }
    }
}

/// A new, empty log in place of `appendonly.aof`, with nothing an earlier
/// log left beside it: the log of an AOF turned on at runtime (see
/// [`open_log`]).
///
/// It is a delta on the save number of the dump on disk, with no records:
/// from here, `dump.kdb` alone is what a restart loads, and the records
/// logged from now on go on top of it. A dump whose save number on disk is
/// not its own (a save with the AOF off in 1.2.0 wrote none) is given one
/// first, or recovery would read the new log as older than the dump and
/// leave it out (see [`DumpOnDisk`]). It is written and synced under a name
/// of its own and
/// then takes the log's name, so a crash leaves the old log or the whole new
/// one. Cut to nothing in place instead, a crash right after the cut left an
/// empty log, which recovery reads as a turn-over stopped part-way: with an
/// `appendonly.aof.prev` from an earlier run there, that was replayed in the
/// log's place.
///
/// Only once the new log has the name are an old log and a turn-over's
/// leftovers removed. Stopped in between, an `appendonly.aof.prev` still
/// there goes with the dump on disk: recovery removes it, as the leftover of
/// a checkpoint that finished. Removed first, a crash in between left the old
/// log without the records it continues from. The turn-over's notes go
/// before `appendonly.aof.prev`: left without it, they stopped the next start
/// (see [`settle_turn_over`]).
///
/// Before anything is replaced or removed, the old logs that hold records
/// are kept under names of their own (see [`keep_aside`]), and the directory
/// synced: they may hold writes that are nowhere else, and the server never
/// deletes them. `why` decides what the warning says (see [`kept_warning`]).
///
/// `added` is what the new log starts with, after its header: the records
/// of an old log that go on from the dump (see [`LogAge::AddedToFrom`]);
/// usually nothing.
fn start_new_log(why: NewLogWhy, added: &[u8]) -> std::io::Result<std::fs::File> {
    let mut kept = Vec::new();
    // A turn-over's new log only ever holds a header (see
    // [`prepare_new_log`]), and is not kept then; one that holds more is.
    for path in [AOF_PREV_PATH, AOF_PATH, TURN_OVER_TMP] {
        if let Some(name) = keep_aside(path)? {
            kept.push((path, name));
        }
    }
    if !kept.is_empty() {
        sync_dir()?;
    }
    for (path, name) in &kept {
        // Never loaded: what `why` says of the logs is not true of it.
        let why = if *path == TURN_OVER_TMP {
            NewLogWhy::Older
        } else {
            why
        };
        log_warn!("{}", kept_warning(why, path, name));
    }
    let file = write_new_log(added)?;
    remove_log_leftovers()?;
    LOG_SUPERSEDED.store(false, Ordering::Relaxed);
    Ok(file)
}

/// Why a log is kept aside and a new one started (see [`start_new_log`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NewLogWhy {
    /// `CONFIG SET appendonly yes`, over the log this process has had open,
    /// which the dataset has moved on from: its writes were applied here.
    Enable,
    /// `CONFIG SET appendonly yes`, with the AOF off since the start, over a
    /// log an earlier run left: it was never loaded.
    EnableOverEarlierRun,
    /// At a start: older than `dump.kdb`, and not replayed.
    Older,
    /// At a start: older than `dump.kdb`, but `appendonly.aof` (and the old
    /// log, with `prev`) was added to after the dump was saved with the AOF
    /// off, and what was added has been replayed. With `log_whole`, all of
    /// `appendonly.aof` was: a log made, or written to, after that save
    /// (#363).
    AddedTo { prev: bool, log_whole: bool },
    /// At a start, or a turn-over: the old log of a checkpoint that
    /// finished, removed, with `dump.kdb.gen` that of a save with the AOF
    /// off. Its records may be in the dump, or in the log, or in neither
    /// (#368).
    FinishedBesideSaveWithAofOff,
}

/// The warning for the log at `path`, kept as `name`. Whether its writes
/// are in the dataset decides what it says to do with them. It used to say
/// they were not, whatever the case, and to send the file to the server:
/// done with a log whose writes had been replayed, or were made by this very
/// server, it applied them twice (#359).
fn kept_warning(why: NewLogWhy, path: &str, name: &str) -> String {
    let kept = format!("It is kept as {name}, which the server never removes.");
    let apply = format!(
        "Writes in it that are not in dump.kdb are not in the dataset. To apply those, send them \
         to the server (`tail -c +17 {name} | redis-cli --pipe` for a file that starts with \
         KIVIDLT1, `redis-cli --pipe < {name}` for one that starts with `*`), leaving out what \
         dump.kdb holds, which would be applied twice; remove the file once it is not needed"
    );
    match why {
        NewLogWhy::Enable => format!(
            "AOF: {path} no longer goes on from dump.kdb (a save with the AOF off, or a full \
             sync, came since), and the AOF turned on starts a new log. {kept} Its writes are \
             already in the dataset: this server made them or replayed them (or a full sync \
             replaced them with its master's dataset). Do not send them to the server, which \
             would apply them twice; remove the file once it is not needed"
        ),
        NewLogWhy::EnableOverEarlierRun => format!(
            "AOF: {path} was left by an earlier run and is not part of this dataset, and the AOF \
             turned on starts a new log. {kept} {apply}"
        ),
        NewLogWhy::AddedTo {
            log_whole: true, ..
        } if path == AOF_PATH => format!(
            "AOF: {path} was written to after dump.kdb was saved with the AOF off, and the log \
             starts again on dump.kdb with its records. {kept} All of its records have been \
             replayed, and are in the dataset: do not send the file to the server, which would \
             apply them twice. Remove the file once it is not needed"
        ),
        NewLogWhy::AddedTo { prev, .. } if path != AOF_PREV_PATH || prev => format!(
            "AOF: {path} was added to after dump.kdb was saved with the AOF off, and the log \
             starts again on dump.kdb with what was added. {kept} What was added to it since that \
             save has been replayed, and is in the dataset: do not send the file to the server, \
             which would apply those writes twice. What it held before that save is older than \
             dump.kdb and was not replayed: if writes from then are missing, read the file (RESP, \
             after a 16-byte header if it starts with KIVIDLT1) and send only those. Remove the \
             file once it is not needed"
        ),
        NewLogWhy::FinishedBesideSaveWithAofOff => format!(
            "AOF: {path} was left by a checkpoint, beside a log that goes on from dump.kdb, and \
             dump.kdb.gen is that of a save with the AOF off. {kept} Its records may be in \
             dump.kdb or in the log already, or in neither: do not send the whole file to the \
             server, which could apply writes twice. If writes from it are missing, read it \
             (RESP, after a 16-byte header if it starts with KIVIDLT1) and send only those; \
             remove the file once it is not needed"
        ),
        NewLogWhy::Older | NewLogWhy::AddedTo { .. } => {
            format!("AOF: {path} is older than dump.kdb and is not replayed. {kept} {apply}")
        }
    }
}

/// What the names of the logs kept aside start with (see [`keep_aside`]).
const KEPT_PREFIX: &str = "superseded.";

/// Keep the log at `path` under a name of its own, `superseded.<ms>.<n>.<path>`,
/// before it is replaced or removed. A log that recovery leaves out, or that
/// an enable starts again from, may hold acknowledged writes that are in no
/// dump: of a run whose dataset the next one did not load, or made after a
/// save the log cannot be told apart from. It used to be overwritten, and
/// they were gone. A hard link, so nothing is copied; a copy, synced, where
/// links cannot be made. One with no record in it (empty, or a header alone)
/// is not kept. The caller syncs the directory.
fn keep_aside(path: &str) -> std::io::Result<Option<String>> {
    let len = match std::fs::metadata(path) {
        Ok(meta) => meta.len(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let header = if aof_delta_generation(&read_head(path)).is_some() {
        AOF_DELTA_HEADER_LEN as u64
    } else {
        0
    };
    if len <= header {
        return Ok(None);
    }
    // Kept already, by an attempt that did not get to remove it (its removal
    // failed, or the process stopped): not kept again. Each start or
    // checkpoint that could not remove it added another copy (#375). Only a
    // copy of the same name with the very same bytes counts; the caller
    // syncs the directory before anything is removed, as for a new one.
    let suffix = format!(".{path}");
    for (kept, kept_len) in kept_logs() {
        if kept_len == len && kept.ends_with(&suffix) && same_bytes(path, &kept).unwrap_or(false) {
            // Synced first: a copy stopped between its write and its sync
            // reads whole from the page cache, but may not be on disk.
            std::fs::File::open(&kept)?.sync_all()?;
            return Ok(Some(kept));
        }
    }
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |at| at.as_millis());
    let name = format!(
        "{KEPT_PREFIX}{ms}.{}.{path}",
        SEQ.fetch_add(1, Ordering::Relaxed)
    );
    if std::fs::hard_link(path, &name).is_err() {
        copy_synced(path, &name, |from, to| std::fs::copy(from, to))?;
    }
    Ok(Some(name))
}

/// Whether the files `a` and `b` hold the same bytes, read in full.
fn same_bytes(a: &str, b: &str) -> std::io::Result<bool> {
    use std::io::Read;
    fn fill(file: &mut std::fs::File, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut filled = 0;
        while filled < buf.len() {
            match file.read(&mut buf[filled..])? {
                0 => break,
                n => filled += n,
            }
        }
        Ok(filled)
    }
    let (mut fa, mut fb) = (std::fs::File::open(a)?, std::fs::File::open(b)?);
    if fa.metadata()?.len() != fb.metadata()?.len() {
        return Ok(false);
    }
    let (mut ba, mut bb) = (vec![0u8; 64 * 1024], vec![0u8; 64 * 1024]);
    loop {
        let (n, m) = (fill(&mut fa, &mut ba)?, fill(&mut fb, &mut bb)?);
        if ba[..n] != bb[..m] {
            return Ok(false);
        }
        if n == 0 {
            return Ok(true);
        }
    }
}

/// Copy `from` to `to` with `copy` ([`std::fs::copy`], or a test's) and sync
/// the copy. If either fails, what was copied is removed: named like a log
/// kept aside, a partial copy was listed at every start as one, and each
/// failed attempt left another (#359).
fn copy_synced(
    from: &str,
    to: &str,
    copy: impl FnOnce(&str, &str) -> std::io::Result<u64>,
) -> std::io::Result<()> {
    let copied = copy(from, to).and_then(|_| std::fs::File::open(to)?.sync_all());
    if copied.is_err() {
        let _ = std::fs::remove_file(to);
    }
    copied
}

/// The logs kept aside (see [`keep_aside`]) in the data directory, by name,
/// with their sizes.
fn kept_logs() -> Vec<(String, u64)> {
    let Ok(entries) = std::fs::read_dir(".") else {
        return Vec::new();
    };
    let mut kept: Vec<(String, u64)> = entries
        .flatten()
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().into_owned();
            (name.starts_with(KEPT_PREFIX) && name.contains(AOF_PATH)).then(|| {
                let len = entry.metadata().map_or(0, |meta| meta.len());
                (name, len)
            })
        })
        .collect();
    kept.sort();
    kept
}

/// How many logs kept aside are in the data directory, and their bytes:
/// `aof_superseded_files` and `aof_superseded_bytes` in `INFO persistence`.
/// A kept log that is a hard link to a log still in use shares its blocks
/// with it until that log is replaced.
pub fn kept_logs_size() -> (usize, u64) {
    let kept = kept_logs();
    (kept.len(), kept.iter().map(|(_, len)| len).sum())
}

/// Say, at every start, which logs kept aside are still in the data
/// directory: the server never removes them, and they are not to pile up
/// unnoticed.
fn warn_of_kept_logs() {
    let kept: Vec<String> = kept_logs().into_iter().map(|(name, _)| name).collect();
    if kept.is_empty() {
        return;
    }
    log_warn!(
        "AOF: {} log(s) kept aside, never replayed and never removed by the server, are in the \
         data directory: {}. Each may hold writes that are not in dump.kdb (see the warning \
         logged when it was kept); remove them once they are not needed",
        kept.len(),
        kept.join(", ")
    );
}

/// A new log, a delta on the dump on disk with `added` as its records
/// (usually none), written and synced under a name of its own and then given
/// the log's.
fn write_new_log(added: &[u8]) -> std::io::Result<std::fs::File> {
    use std::io::Write;
    let generation = generation_of_dump_on_disk()?;
    let tmp = format!("{AOF_PATH}.tmp");
    let made = (|| -> std::io::Result<std::fs::File> {
        remove_if_there(&tmp)?;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .create_new(true)
            .open(&tmp)?;
        file.write_all(&delta_header(generation))?;
        file.write_all(added)?;
        file.sync_all()?;
        std::fs::rename(&tmp, AOF_PATH)?;
        Ok(file)
    })();
    let file = made.inspect_err(|_| {
        let _ = std::fs::remove_file(&tmp);
    })?;
    sync_dir()?;
    Ok(file)
}

/// The log of a start with the AOF on. One that is not there, or empty, is
/// made as a delta on the dump on disk: opened with no header, as it was, it
/// was a log of an age no recovery could tell, and replayed over a dump saved
/// later with the AOF off, it applied its writes twice. Anything else at the
/// log's name (a link, a directory) is opened as it is, and fails as it would.
fn open_log_at_start() -> std::io::Result<std::fs::File> {
    let make = match std::fs::symlink_metadata(AOF_PATH) {
        Ok(meta) => meta.is_file() && meta.len() == 0,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => true,
        Err(e) => return Err(e),
    };
    if make {
        return write_new_log(&[]);
    }
    open_aof()
}

/// Remove what an old log left beside the log: a turn-over's notes, then
/// the old log itself.
fn remove_log_leftovers() -> std::io::Result<()> {
    for left in [TURN_OVER_TMP, TURN_OVER_APPENDING, AOF_PREV_PATH] {
        failpoint("new-log-removing");
        remove_if_there(left)?;
    }
    sync_dir()
}

fn remove_if_there(path: &str) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}

/// The save number of the `dump.kdb` on disk, given it one if the number on
/// disk is not its own: past every number on disk, logs included, so that no
/// log left there reads as going with it.
fn generation_of_dump_on_disk() -> std::io::Result<u64> {
    if !std::path::Path::new("dump.kdb").exists() {
        return Ok(read_generation());
    }
    let found = file_fingerprint("dump.kdb")?;
    match read_saved() {
        Some(saved) if saved.dump == found => Ok(saved.number),
        _ => {
            let generation = next_generation();
            // No log on disk goes with it. The logs are as they are: the
            // caller is about to replace them, and may hold the open one.
            write_saved(generation, found, Some(logs_on_disk()?))?;
            sync_dir()?;
            log_warn!(
                "AOF: {AOF_GEN_PATH} did not describe dump.kdb (saved with the AOF off by an \
                 earlier release, or replaced) — dump.kdb is now save number {generation}"
            );
            Ok(generation)
        }
    }
}

/// A dump was put in place with the AOF off, by a save or a checkpoint that
/// found the AOF off: the open log no longer goes on from it, and the next
/// enable starts a new one (#350). The files are left alone, as with the AOF
/// off they always were: the dump's save number, past every log's, and its
/// mark that no log goes with it (see [`install_dump`]) are what keep a
/// start with the AOF on from replaying them over it (#354).
pub(crate) fn dump_saved_without_log() {
    LOG_SUPERSEDED.store(true, Ordering::Relaxed);
}

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
/// second. What no reply waits for (an expiry's delete) is written with the
/// next connection's, or by the writer's next sync, a second at most. A
/// replica's stream has no connection's to go with: its applier wakes the
/// writer itself, once a batch ([`stream_applied`]).
#[inline]
pub fn records_queued() {
    if !acks_wait() {
        QUEUED.notify_one();
    }
}

/// A replica has applied a batch of its master's stream. Its records have no
/// reply waiting for them to write them, and a read-only replica takes no
/// writes of its own whose replies would: the writer thread writes them. Left to the
/// writer's sync once a second, a replica under sustained writes fell behind
/// its master, its queue (and with it its memory) grew by the second, and
/// its checkpoints, queued behind the backlog, did not finish for minutes.
#[inline]
pub fn stream_applied() {
    if WRITER_RUNNING.load(Ordering::Acquire) {
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

/// `appendfsync always`: whether a reply to a write waits for its record to
/// be synced to disk, not only written to the file (#513). It was accepted,
/// and the file synced once a second all the same: a power failure could
/// lose a second of writes their clients had been told were made.
static FSYNC_ALWAYS: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

pub fn set_fsync_always(on: bool) {
    FSYNC_ALWAYS.store(on, Ordering::Relaxed);
    // Replies already waiting for a sync get it, whatever the setting now;
    // the writer is woken to look at the setting again (see
    // [`run_aof_writer`]).
    SYNC_WANTED.notify_one();
    QUEUED.notify_one();
}

#[inline]
fn fsync_always() -> bool {
    FSYNC_ALWAYS.load(Ordering::Relaxed)
}

/// Under `appendfsync always`: the barriers whose records are in the file
/// and wait for it to be synced. Added to with the log locked, and taken
/// with it locked too, so that what the barriers taken stand for is in the
/// file then. The writer thread syncs for them ([`sync_for_replies`]).
static UNSYNCED_BARRIERS: parking_lot::Mutex<Vec<u64>> = parking_lot::const_mutex(Vec::new());

/// Wakes the writer thread to sync for [`UNSYNCED_BARRIERS`].
static SYNC_WANTED: tokio::sync::Notify = tokio::sync::Notify::const_new();

/// Whether the writer thread has to listen for [`SYNC_WANTED`]: under
/// `appendfsync always`, or while barriers handed to it under `always` are
/// still waiting. Not otherwise: `everysec` costs what it did.
fn syncs_for_replies() -> bool {
    fsync_always() || !UNSYNCED_BARRIERS.lock().is_empty()
}

/// A sync of the AOF failed under `appendfsync always`: the server stops
/// rather than acknowledge writes that may not be on disk, as Redis does.
/// Never carried on from. Linux reports a failed writeback once to each open
/// file, and every handle of the log shares one: a failure logged and
/// passed over would leave the next sync to return success and its replies
/// to go out with their writes lost.
fn sync_failed_under_always(what: &str, e: &std::io::Error) -> ! {
    log_fatal!(
        "AOF: {what} failed under appendfsync always: {e} — exiting rather than acknowledge writes that may not be on disk"
    );
    std::process::exit(1);
}

/// Whether a connection's replies may have to wait for the AOF: it is on,
/// and acknowledges after writing — always under `appendfsync always`,
/// whatever `aof-ack-after-write` says.
#[inline]
pub fn acks_wait() -> bool {
    AOF_ENABLED.load(Ordering::Relaxed)
        && (ACK_AFTER_WRITE.load(Ordering::Relaxed) || fsync_always())
}

/// Whether a command may write a record its reply should wait for: a write,
/// or a script or a transaction that may make some.
#[inline]
pub fn may_log(cmd: &[&[u8]]) -> bool {
    let Some(&cmd0) = cmd.first() else {
        return false;
    };
    // A write by name, the most of them, is told by its name alone.
    if crate::replication::is_write_command_bytes(cmd0) {
        return true;
    }
    // The rest, told apart by length first: most commands are none of them.
    let is = |name: &str| cmd0.eq_ignore_ascii_case(name.as_bytes());
    // Writes only with some arguments (`GEORADIUS ... STORE`, `MIGRATE`,
    // `FUNCTION LOAD`): judged by them, so the reply waits for the record
    // like any other write's.
    let by_arguments =
        |name: &str| is(name) && crate::replication::command_mutates_for_replication(name, cmd);
    // EVAL, EVALSHA, FCALL and EXEC may write from what they run.
    match cmd0.len() {
        4 => is("EVAL") || is("EXEC"),
        5 => is("FCALL"),
        6 => by_arguments("SCRIPT"),
        7 => is("EVALSHA") || by_arguments("MIGRATE"),
        8 => by_arguments("FUNCTION"),
        9 => by_arguments("GEORADIUS"),
        17 => by_arguments("GEORADIUSBYMEMBER"),
        _ => false,
    }
}

/// How long a reply to a write waits for its records to reach the file
/// before it is sent without them. Counted in [`AOF_ACKS_BEFORE_WRITE`].
const ACK_WAIT_LIMIT: std::time::Duration = std::time::Duration::from_secs(5);

/// Wait until every record queued on `tx` before now is in the file.
///
/// Bounded: should the writer be gone, or the disk fall more than
/// [`ACK_WAIT_LIMIT`] behind, a reply is late, not lost — and is sent
/// without its write, which a process killed then can lose. With the AOF
/// off, or no writer, nothing to wait for.
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
    let mut wait = wait;
    loop {
        if tokio::time::timeout(ACK_WAIT_LIMIT, &mut wait)
            .await
            .is_ok()
        {
            return;
        }
        // Under `appendfsync always` a reply is never sent ahead of its
        // write: it waits for as long as the disk takes.
        if fsync_always() {
            log_throttled!(
                10,
                crate::loglevel::WARN,
                "AOF: a reply has waited over 5 s for its write to be synced to {AOF_PATH} (appendfsync always): the disk is behind"
            );
            continue;
        }
        BARRIERS.lock().remove(&id);
        AOF_ACKS_BEFORE_WRITE.fetch_add(1, Ordering::Relaxed);
        log_throttled!(
            10,
            crate::loglevel::WARN,
            "AOF: a reply waited 5 s for its write to reach {AOF_PATH} and was sent without it"
        );
        return;
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

/// Set the AOF's queue up, and with the AOF on its log, and return the
/// writer for a thread of its own to run. Called before that thread is
/// spawned: a `CONFIG SET appendonly yes` that came before the thread got
/// going found no queue to open a log for ("the AOF writer has not started
/// yet"), and a log that cannot be opened stops the server before it serves.
pub fn start_aof_writer(
    rx: mpsc::UnboundedReceiver<Vec<u8>>,
) -> impl std::future::Future<Output = ()> + Send + 'static {
    let (flush_tx, flush_rx) = tokio::sync::mpsc::channel::<()>(1);
    let _ = FLUSH_TX.set(flush_tx);

    if AOF_ENABLED.load(Ordering::Relaxed) {
        // A server asked to keep an append-only file that cannot open one must
        // not run: it would accept writes and keep none of them. (This used to be
        // a panic in this task alone — the server carried on, serving, with
        // nothing being logged.)
        let file = match open_log_at_start() {
            Ok(file) => file,
            Err(e) => fatal_persistence(&format!("{AOF_PATH} cannot be opened for appending: {e}")),
        };
        *LOG.lock() = Some(Log { rx, file });
        *UNOPENED.lock() = None;
    } else {
        // With the AOF off, no file is opened or needed until `CONFIG SET
        // appendonly yes` (see [`open_log`]). Opened whatever the setting, it
        // was made in every data directory, and one that could not be written
        // stopped a server that was never going to write it (#47). A log
        // already open is another server's, in a process that runs several
        // (the tests do): left alone.
        *UNOPENED.lock() = Some(rx);
    }
    WRITER_RUNNING.store(true, Ordering::Release);

    // Whatever survived startup (an AOF that was just replayed, or nothing) is
    // the baseline the first growth ratio is measured against. The writer opens
    // *after* recovery on every startup path, so this sees the post-replay size.
    record_base_size();
    run_aof_writer(flush_rx)
}

/// The writer's loop: the second's sync, a sync asked for, and what is
/// queued that no connection writes.
async fn run_aof_writer(mut flush_rx: tokio::sync::mpsc::Receiver<()>) {
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
            () = SYNC_WANTED.notified(), if syncs_for_replies() => {
                sync_for_replies();
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
/// Everything queued ahead of the mark came from a write made before the
/// dataset was captured: it belongs with the old log, which with the old
/// snapshot describes the dataset as captured. Nothing is dropped.
///
/// It runs with the log locked, in whichever thread reaches the mark — often
/// a connection's — and every reply waiting for the AOF waits for it: so it
/// only renames. The new log was written and synced before the mark was
/// placed (see [`prepare_new_log`]), and the old one is handed back to be
/// synced by the checkpoint, with the lock let go (see [`finish_turn_over`]).
/// Synced here, on a disk running behind, it held every write for seconds
/// (#243).
fn perform_turn_over(file: &mut std::fs::File) -> std::io::Result<OldLog> {
    let moved = move_log_aside();
    // Whether or not it got all the way: the writer goes on in whatever file
    // has the log's name now, never in one that has been moved aside.
    let old = std::mem::replace(file, open_aof()?);
    match moved? {
        MovedAside::ToSync => {
            // With the log locked: turn-overs are numbered in the order made.
            let number = TURN_OVERS.fetch_add(1, Ordering::Relaxed) + 1;
            OLD_LOG_UNSYNCED.store(number, Ordering::Release);
            Ok(Some((old, number)))
        }
        MovedAside::Synced => Ok(None),
    }
}

/// Make durable what a turn-over left to sync: the old log, and the
/// directory that has both logs under their new names. The writer syncs the
/// new log again once they are.
fn finish_turn_over(old: OldLog) -> std::io::Result<()> {
    let Some((old, number)) = old else {
        return Ok(());
    };
    failpoint("turn-over-syncing");
    let synced = if injected_failure("turn-over-sync-fails") {
        Err(std::io::Error::other("failure injected by a test"))
    } else {
        old.sync_data().and_then(|()| sync_dir())
    };
    match &synced {
        Ok(()) => old_log_synced(number),
        Err(e) if fsync_always() => {
            sync_failed_under_always(&format!("the sync of {AOF_PREV_PATH}"), e)
        }
        // Left set: the new log is not synced ahead of the old one, and the
        // second's sync tries the old one again (see [`do_fsync`]).
        Err(_) => {}
    }
    synced
}

/// Sync the old log a turn-over moved aside, and the directory.
fn sync_old_log() -> std::io::Result<()> {
    match std::fs::File::open(AOF_PREV_PATH) {
        Ok(old) => old.sync_data()?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    sync_dir()
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
    let mut drained = 0usize;
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
        // A batch that stopped at the cap left more queued. Going on with it
        // here, not on the next pass: what no reply waits for (a replica's
        // stream) may have no next pass but the second's sync, and one batch
        // a second fell behind 40 MB/s of writes, with the queue, and the
        // turn-over of a checkpoint behind it, growing without end. Not for
        // ever, though: with writes coming faster than the disk takes them
        // the queue never empties, and whoever holds the log would keep it,
        // the second's sync waiting behind. Past a budget, the writer thread
        // is woken to go on, and the sync gets its turn.
        let more_queued = batch.len() >= MAX_AOF_BATCH_BYTES;
        drained += batch.len();
        wrote |= !batch.is_empty();
        write_batch(file, &batch);
        released |= !barriers.is_empty();
        // Under `appendfsync always`, the replies the barriers stand for go
        // out once what is before them is on the disk. The writer thread
        // syncs, not the connection's: a sync takes milliseconds, and a
        // connection's thread serves others meanwhile. Every barrier that
        // comes in while it syncs is answered by the next sync: a group
        // commit.
        if !barriers.is_empty() && fsync_always() {
            UNSYNCED_BARRIERS.lock().append(&mut barriers);
            SYNC_WANTED.notify_one();
        } else {
            release_barriers(&mut barriers);
        }
        let Some(generation) = mark else {
            if more_queued {
                if drained < MAX_AOF_PASS_BYTES {
                    continue;
                }
                QUEUED.notify_one();
            }
            return (wrote, released);
        };
        let result = perform_turn_over(file);
        let done = {
            let mut waiters = TURN_OVER_WAITERS.lock();
            waiters
                .iter()
                .position(|(g, _)| *g == generation)
                .map(|at| waiters.swap_remove(at).1)
        };
        let undelivered = match done {
            Some(done) => done.send(result).err().map(|unsent| unsent.0),
            None => Some(result),
        };
        // No one to sync the old log: synced here, so that the new one is
        // not left unsynced for good.
        if let Some(Ok(old)) = undelivered {
            let _ = finish_turn_over(old);
        }
    }
}

/// The new log of a turn-over, under a name of its own until it takes the
/// log's.
const TURN_OVER_TMP: &str = "appendonly.aof.rewrite.tmp";

/// There while a turn-over adds the log's records to an old log that is
/// still needed: how long that old log was before. See [`settle_turn_over`].
const TURN_OVER_APPENDING: &str = "appendonly.aof.prev.len";

// ── The files of a turn-over ─────────────────────────────────────────────────
//
// Stopped at any point, a turn-over leaves files that recovery reads as they
// were before it or as they are after it (see [`recover_turn_over`]). The
// new, empty log is written in full first, under a name of its own; then the
// log is moved aside, and the new one takes its name. It used to be the
// other way round: a crash after the log was moved aside left
// `appendonly.aof.prev` and no log at all, and a start that found no log
// replayed nothing, `.prev` included — every write since the snapshot lost.
//
// Three steps: [`prepare_new_log`], [`move_log_aside`], and the syncs
// ([`finish_turn_over`]). Only the second runs with the log locked.

/// The first step of a turn-over: the new, empty log for save number
/// `generation`, written in full and synced under a name of its own.
///
/// Done by the checkpoint before it places its mark, with nothing locked:
/// it waits for the disk, and on a disk running behind that is seconds.
/// Stopped after this, the files read as they did before it: recovery
/// removes the new log (see [`recover_turn_over`]).
fn prepare_new_log(generation: u64) -> std::io::Result<()> {
    use std::io::Write;
    settle_turn_over()?;
    // The old log of a checkpoint that finished is removed by the turn-over
    // (see [`move_log_aside`]), and may have to be kept aside first: that
    // links or copies it, and syncs the directory, so it is done here, with
    // nothing locked (#375). The turn is held from here to the turn-over, and
    // nothing else changes the save number, the dump or the old log: the
    // turn-over sees what this saw.
    FINISHED_OLD_LOG_KEPT.store(false, Ordering::Relaxed);
    if std::path::Path::new(AOF_PREV_PATH).exists() && checkpoint_completed(&read_head(AOF_PATH)) {
        keep_finished_old_log()?;
        FINISHED_OLD_LOG_KEPT.store(true, Ordering::Relaxed);
    }
    let mut fresh = std::fs::File::create(TURN_OVER_TMP)?;
    fresh.write_all(AOF_DELTA_MAGIC)?;
    fresh.write_all(&generation.to_le_bytes())?;
    fresh.sync_all()
}

/// What [`move_log_aside`] left to sync.
enum MovedAside {
    /// The old log, now `appendonly.aof.prev`, and the directory.
    ToSync,
    /// Nothing: the old log's records were added to an older one, synced.
    Synced,
}

/// The rest of a turn-over, once [`prepare_new_log`] has run: the log is
/// moved aside, and the new one takes its name.
///
/// Neither the old log nor the directory is synced in the usual case: that
/// is left to the caller ([`MovedAside::ToSync`]), so that a writer holding
/// the log's lock does not wait for the disk.
fn move_log_aside() -> std::io::Result<MovedAside> {
    use std::io::Write;
    let prev = std::path::Path::new(AOF_PREV_PATH);
    let mut current = Vec::new();
    if prev.exists() {
        current = match std::fs::read(AOF_PATH) {
            Ok(current) => current,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e),
        };
        if checkpoint_completed(&current) {
            // Left by a checkpoint that finished, all but removing it: what
            // it holds is in the snapshot. Added to, it would be replayed
            // on top of that snapshot, should this checkpoint not finish.
            // Kept aside first if it had to be, by [`prepare_new_log`]: only
            // removed here. Not kept when it had to be, it is not removed,
            // and the turn-over fails with nothing lost.
            if !FINISHED_OLD_LOG_KEPT.swap(false, Ordering::Relaxed)
                && read_saved().is_some_and(|saved| saved.alone)
            {
                return Err(std::io::Error::other(format!(
                    "{AOF_PREV_PATH}, left by a checkpoint that finished, was not kept aside"
                )));
            }
            remove_if_there(AOF_PREV_PATH)?;
        }
    }
    if prev.exists() {
        // A turn-over before this one whose snapshot never reached the disk:
        // its old log is still needed, and this log goes on from it. Until
        // the new log has the name, these records are in both; the old log's
        // length is noted first, so that they are cut off it again rather
        // than replayed twice if this stops before then.
        let records = if aof_delta_generation(&current).is_some() {
            &current[AOF_DELTA_HEADER_LEN..]
        } else {
            &current[..]
        };
        let mut old = std::fs::OpenOptions::new().append(true).open(prev)?;
        let before = old.metadata()?.len();
        write_file_durably(TURN_OVER_APPENDING, &before.to_le_bytes())?;
        old.write_all(records)?;
        old.sync_data()?;
        failpoint("turn-over-appended");
        std::fs::rename(TURN_OVER_TMP, AOF_PATH)?;
        sync_dir()?;
        std::fs::remove_file(TURN_OVER_APPENDING)?;
        Ok(MovedAside::Synced)
    } else {
        if std::path::Path::new(AOF_PATH).exists() {
            std::fs::rename(AOF_PATH, prev)?;
            failpoint("turn-over-moved-aside");
        }
        std::fs::rename(TURN_OVER_TMP, AOF_PATH)?;
        Ok(MovedAside::ToSync)
    }
}

/// Put right what a turn-over that stopped while adding the log's records
/// to the old log left (see [`move_log_aside`]). If the new log had not yet
/// taken the log's name, those records are still in the log, and are cut off
/// the old log again; if it had, the old log is whole and the note is all
/// that is left.
fn settle_turn_over() -> std::io::Result<()> {
    let noted = match std::fs::read(TURN_OVER_APPENDING) {
        Ok(noted) => noted,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    if std::path::Path::new(TURN_OVER_TMP).exists() {
        let before = noted
            .get(..8)
            .and_then(|b| b.try_into().ok())
            .map(u64::from_le_bytes)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("{TURN_OVER_APPENDING} is damaged"),
                )
            })?;
        let old = std::fs::OpenOptions::new()
            .write(true)
            .open(AOF_PREV_PATH)?;
        if old.metadata()?.len() > before {
            old.set_len(before)?;
            old.sync_all()?;
        }
    }
    std::fs::remove_file(TURN_OVER_APPENDING)
}

/// Before recovery reads the logs: put right what a checkpoint's turn-over
/// left, if the process stopped part-way through it.
fn recover_turn_over() {
    if let Err(e) = settle_turn_over() {
        fatal_persistence(&format!(
            "a turn-over of {AOF_PATH} that was stopped part-way cannot be put right: {e}"
        ));
    }
    let log_len = match std::fs::metadata(AOF_PATH) {
        Ok(meta) => meta.len(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
        Err(e) => fatal_persistence(&format!("{AOF_PATH} exists but cannot be examined: {e}")),
    };
    if std::path::Path::new(AOF_PREV_PATH).exists() && log_len == 0 {
        // Stopped after the log was moved aside and before the new one took
        // its name (or, in older releases, with no new one written at all). The
        // old log holds writes that are in no snapshot: the new log takes
        // the name (a new one goes on from the next save number if there is
        // none), and the old log is then replayed like that of any
        // checkpoint that did not finish.
        log_warn!(
            "AOF: a checkpoint was stopped while turning {AOF_PATH} over — completing the \
             turn-over, and replaying {AOF_PREV_PATH}"
        );
        let fresh = std::fs::read(TURN_OVER_TMP)
            .is_ok_and(|b| b.len() == AOF_DELTA_HEADER_LEN && aof_delta_generation(&b).is_some());
        let done = if fresh {
            std::fs::rename(TURN_OVER_TMP, AOF_PATH)
        } else {
            let mut header = AOF_DELTA_MAGIC.to_vec();
            header.extend_from_slice(&read_generation().saturating_add(1).to_le_bytes());
            write_file_durably(AOF_PATH, &header)
        };
        if let Err(e) = done.and_then(|()| sync_dir()) {
            fatal_persistence(&format!(
                "{AOF_PATH} could not be put back after an interrupted checkpoint: {e}"
            ));
        }
    } else {
        // A turn-over's new log, never put in place: only ever a header (see
        // [`prepare_new_log`]). One that holds more is kept aside first: no
        // log is removed with records that may be nowhere else.
        let kept = keep_aside(TURN_OVER_TMP).and_then(|kept| {
            if let Some(name) = &kept {
                sync_dir()?;
                log_warn!("{}", kept_warning(NewLogWhy::Older, TURN_OVER_TMP, name));
            }
            Ok(kept)
        });
        let removed = kept.and_then(|_| remove_if_there(TURN_OVER_TMP));
        if let Err(e) = removed {
            log_warn!("AOF: {TURN_OVER_TMP} could not be removed: {e}");
        }
    }
}

/// Set by [`prepare_new_log`] once the old log of a checkpoint that
/// finished, which the turn-over removes, has been kept aside if it had to
/// be (see [`keep_finished_old_log`]).
static FINISHED_OLD_LOG_KEPT: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Before `appendonly.aof.prev`, left by a checkpoint that finished (see
/// [`checkpoint_completed`]), is removed: the dump holds its records, unless
/// `dump.kdb.gen` is that of a save with the AOF off, which did not turn the
/// log over. Then it is kept aside first (#368). A save with the AOF off
/// that took the number of a turn-over's new log (`next_generation` did not
/// count it) made the old log read as that of a finished checkpoint, and it
/// was deleted with writes a run with the AOF off had never loaded. With
/// any other `dump.kdb.gen`, nothing is done.
fn keep_finished_old_log() -> std::io::Result<()> {
    if read_saved().is_some_and(|saved| saved.alone)
        && let Some(name) = keep_aside(AOF_PREV_PATH)?
    {
        sync_dir()?;
        log_warn!(
            "{}",
            kept_warning(
                NewLogWhy::FinishedBesideSaveWithAofOff,
                AOF_PREV_PATH,
                &name
            )
        );
    }
    Ok(())
}

/// Write `bytes` to `path` so that a crash leaves the old file or the whole
/// new one.
fn write_file_durably(path: &str, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let tmp = format!("{path}.tmp");
    {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    sync_dir()
}

/// Make the renames in the data directory durable before what depends on
/// them: a power cut must not keep a later step and lose an earlier one.
fn sync_dir() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::fs::File::open(".")?.sync_all()?;
    }
    Ok(())
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

/// How much one pass over the queue writes, at most, before it lets the log
/// go (see [`write_queued`]): about a second of a fast disk's writes.
const MAX_AOF_PASS_BYTES: usize = 64 * 1024 * 1024;

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
        // Replies wait for this write and its sync: none can be sent
        // without it, nor with it lost. Redis exits too.
        if fsync_always() {
            log_fatal!(
                "AOF: write to {AOF_PATH} failed under appendfsync always: {e} — exiting rather than acknowledge writes that are not on disk"
            );
            std::process::exit(1);
        }
        log_throttled!(
            10,
            crate::loglevel::ERROR,
            "AOF: write to {AOF_PATH} failed: {e} — writes are not being made durable; check free disk space and permissions on the data directory"
        );
    }
}

/// `appendfsync always`: sync the file for the replies waiting for it, and
/// then answer them (#513). On a handle of its own, outside the lock:
/// connections go on writing meanwhile, and what they write is synced next.
fn sync_for_replies() {
    let (mut barriers, written, file) = {
        let log = LOG.lock();
        let barriers = std::mem::take(&mut *UNSYNCED_BARRIERS.lock());
        // What the sync covers, as in [`do_fsync`]: `WAITAOF` and the
        // second's sync see it synced.
        let written = AOF_WRITTEN.load(Ordering::Relaxed);
        match log.as_ref() {
            Some(log) => (barriers, written, Some(log.file.try_clone())),
            // The AOF turned off since: nothing to sync.
            None => (barriers, written, None),
        }
    };
    if barriers.is_empty() {
        return;
    }
    match file {
        Some(Ok(file)) => {
            // A test holds the sync for these replies here: only here, not
            // in the second's sync (`do_fsync`), which answers no reply. Held
            // there too, a tick that came first took the hit the test
            // waited for, and the replies' sync then waited at the next one,
            // which the test never let go of (#515).
            failpoint("aof-fsync-before-reply");
            sync_before_reply(&file);
            AOF_FSYNCED.fetch_max(written, Ordering::Relaxed);
        }
        Some(Err(e)) => sync_failed_under_always(&format!("opening {AOF_PATH} to sync"), &e),
        None => {}
    }
    release_barriers(&mut barriers);
}

/// `appendfsync always`: sync what has been written, before the replies
/// that wait for it are sent — and first the old log of a turn-over the
/// checkpoint has yet to sync, with the directory: the records after a
/// turn-over must not be on the disk without those before it, nor in a
/// new log whose name may not be. A sync that fails ends the server, as in
/// Redis: it can neither reply nor keep the promise.
fn sync_before_reply(file: &std::fs::File) {
    let synced = (|| {
        if OLD_LOG_UNSYNCED.load(Ordering::Acquire) != 0 {
            sync_old_log()?;
        }
        file.sync_data()
    })();
    if let Err(e) = synced {
        sync_failed_under_always(&format!("the fsync of {AOF_PATH}"), &e);
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
    // A test holds the sync here, as a slow disk would: `WAITAOF` waits.
    failpoint("fsync");
    // Under `appendfsync always`, a sync that fails stops the server, here
    // as for a reply: logged and passed over, its error would be gone for
    // the next sync, which would answer replies with their writes lost.
    if fsync_always() {
        sync_before_reply(&file);
        AOF_FSYNCED.fetch_max(written, Ordering::Relaxed);
        return;
    }
    // Not ahead of the old log a turn-over has moved aside. The checkpoint
    // syncs that one; if its sync failed, or has yet to be made, it is made
    // here. Marked synced only if no turn-over was made meanwhile: what this
    // synced may not be the old log of that one, nor its renames.
    let seen = OLD_LOG_UNSYNCED.load(Ordering::Acquire);
    if seen != 0 {
        match sync_old_log() {
            Ok(()) => {
                // A test holds the writer here, as a slow disk would.
                failpoint("old-log-synced");
                old_log_synced(seen);
            }
            Err(e) => {
                log_throttled!(
                    10,
                    crate::loglevel::ERROR,
                    "AOF: fsync of {AOF_PREV_PATH} failed: {e} — writes acknowledged since the last good fsync may be lost on power failure; check the disk"
                );
                return;
            }
        }
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
        // Whoever holds the log takes the mark: the AOF being on, it is open
        // (see [`AOF_ENABLED`]). With none, nothing would take it, and the
        // wait below would never end.
        debug_assert!(
            LOG.try_lock().is_none_or(|log| log.is_some()),
            "a turn-over with no log open"
        );
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
        None => match move_log_aside()? {
            MovedAside::ToSync => {
                if let Ok(old) = std::fs::File::open(AOF_PREV_PATH) {
                    old.sync_data()?;
                }
                sync_dir()
            }
            MovedAside::Synced => Ok(()),
        },
        Some(done) => finish_turn_over(
            done.recv()
                .map_err(|_| std::io::Error::other("AOF writer dropped the turn-over"))??,
        ),
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
    let turn = checkpoint_turn()?;
    checkpoint_noting(&turn, store, index_store, &mut || {})
}

/// Held by whatever writes `dump.kdb`, from a checkpoint's choice of save
/// number until it has removed the old log: one at a time.
///
/// Nothing kept checkpoints apart. Two at once took the same save number
/// (it is read from `dump.kdb.gen`, which only a finished one writes), each
/// put its snapshot in place whenever it finished — an older one last undid
/// a newer one, and keys deleted in between came back — and each removed
/// `appendonly.aof.prev` when it finished, while the other still needed it.
static CHECKPOINT_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

/// The last save number chosen in this process. See [`next_generation`].
static LAST_GENERATION: AtomicU64 = AtomicU64::new(0);

/// A checkpoint's turn (see [`CHECKPOINT_LOCK`]): while it is held, nothing
/// else writes `dump.kdb`, its save number, or turns the log over.
pub struct CheckpointTurn {
    _held: parking_lot::MutexGuard<'static, ()>,
}

/// Wait for a checkpoint's turn.
///
/// The order is the turn, then the write gate: a checkpoint has its turn
/// while it waits for the gate's cut, and the cut waits for every write in
/// flight. A caller inside a write, holding a slot of the gate (a `SAVE` in
/// a `MULTI`, say), must not wait here, then: the checkpoint that has the
/// turn may be waiting for that very slot. It is told that a checkpoint is
/// in progress instead, as Redis tells a `SAVE` during a `BGSAVE`.
pub fn checkpoint_turn() -> std::io::Result<CheckpointTurn> {
    let held = if crate::write_gate::held_by_this_thread() {
        CHECKPOINT_LOCK
            .try_lock()
            .ok_or_else(|| std::io::Error::other("another checkpoint is in progress"))?
    } else {
        CHECKPOINT_LOCK.lock()
    };
    Ok(CheckpointTurn { _held: held })
}

/// [`checkpoint`], with its turn already taken, calling `at_cut` while
/// writes are held for the capture: what it reads is as of the dataset
/// written.
pub fn checkpoint_noting(
    _turn: &CheckpointTurn,
    store: &crate::store::Store,
    index_store: &crate::vector::IndexStore,
    at_cut: &mut dyn FnMut(),
) -> std::io::Result<()> {
    /// Cleared however the checkpoint ends, a panic included.
    struct Active;
    impl Drop for Active {
        fn drop(&mut self) {
            REWRITE_ACTIVE.store(false, Ordering::Release);
        }
    }
    REWRITE_ACTIVE.store(true, Ordering::Release);
    let _active = Active;
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
    result
}

fn checkpoint_inner(
    store: &crate::store::Store,
    index_store: &crate::vector::IndexStore,
    at_cut: &mut dyn FnMut(),
) -> std::io::Result<()> {
    let aof_on = AOF_ENABLED.load(Ordering::Relaxed);
    let generation = next_generation();
    failpoint("checkpoint-cutting");
    if aof_on {
        // Before writes are held, and before the log is locked to turn it
        // over: it waits for the disk.
        prepare_new_log(generation)?;
    }

    // Between whole writes: a write is either in what is captured, with its
    // record in the old log, or after it, with its record in the new one.
    let cut = crate::write_gate::cut();
    let started = std::time::Instant::now();
    at_cut();
    let frozen = crate::snapshot::freeze_dataset_for_disk(store, Some(index_store));
    let turn_over = match (&frozen, aof_on) {
        (Ok(_), true) => {
            // The new log goes on from the dataset captured: cleared here,
            // with writes held, so that a full sync's swap after the cut
            // keeps its mark.
            LOG_SUPERSEDED.store(false, Ordering::Relaxed);
            Some(turn_over_aof(generation))
        }
        _ => None,
    };
    drop(cut);
    let held = started.elapsed();
    let frozen = frozen?;
    if let Some(turned) = turn_over
        && let Err(e) = turned()
    {
        LOG_SUPERSEDED.store(true, Ordering::Relaxed);
        return Err(e);
    }
    failpoint("checkpoint-turned-over");
    if held > std::time::Duration::from_millis(100) {
        log_info!(
            "AOF: the checkpoint held writes for {} ms",
            held.as_millis()
        );
    }

    let tmp_path = temp_dump_name();
    let written = frozen
        .write_file(&tmp_path)
        .and_then(|()| install_dump(&tmp_path, generation, !aof_on));
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
    } else {
        // A `CONFIG SET appendonly no` came before this checkpoint read the
        // setting: its dump is one saved with the AOF off.
        dump_saved_without_log();
    }
    failpoint("checkpoint-finished");
    Ok(())
}

/// The length of a file and a hash of its first and last 64 KiB: enough to
/// tell one dump from another.
fn file_fingerprint(path: &str) -> std::io::Result<(u64, u64)> {
    let file = std::fs::File::open(path)?;
    let len = file.metadata()?.len();
    fingerprint_of_first(&file, len)
}

/// How much of each end of a file [`file_fingerprint`] hashes.
const EDGE: u64 = 64 * 1024;

/// [`file_fingerprint`] of a file that holds `buf`.
fn fingerprint_of_bytes(buf: &[u8]) -> (u64, u64) {
    let len = buf.len() as u64;
    let mut sample = buf[..EDGE.min(len) as usize].to_vec();
    if len > EDGE {
        let from = len - EDGE.min(len - EDGE);
        sample.extend_from_slice(&buf[from as usize..]);
    }
    (len, xxhash_rust::xxh3::xxh3_64(&sample))
}

/// The header of a delta log for save number `generation`.
fn delta_header(generation: u64) -> [u8; AOF_DELTA_HEADER_LEN] {
    let mut header = [0; AOF_DELTA_HEADER_LEN];
    header[..8].copy_from_slice(AOF_DELTA_MAGIC);
    header[8..].copy_from_slice(&generation.to_le_bytes());
    header
}

/// [`file_fingerprint`] of the first `len` bytes of `file`: its fingerprint
/// when it was that long, if it has only been added to since. From the
/// start of the file, wherever it was read up to: called again on the same
/// file, for the next length recorded, it went on from where the last call
/// stopped, and a log added to after a save matched only the first length
/// tried.
fn fingerprint_of_first(mut file: &std::fs::File, len: u64) -> std::io::Result<(u64, u64)> {
    use std::io::{Read, Seek, SeekFrom};
    file.seek(SeekFrom::Start(0))?;
    let mut sample = Vec::with_capacity((2 * EDGE) as usize);
    (&mut file).take(EDGE.min(len)).read_to_end(&mut sample)?;
    if len > EDGE {
        let from = len - EDGE.min(len - EDGE);
        file.seek(SeekFrom::Start(from))?;
        (&mut file).take(len - from).read_to_end(&mut sample)?;
    }
    Ok((len, xxhash_rust::xxh3::xxh3_64(&sample)))
}

/// [`file_fingerprint`] of a log; one that is not there reads as empty.
fn log_fingerprint(path: &str) -> std::io::Result<(u64, u64)> {
    match file_fingerprint(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Ok((0, xxhash_rust::xxh3::xxh3_64(&[])))
        }
        found => found,
    }
}

/// `appendonly.aof` and `appendonly.aof.prev` as recorded beside a dump
/// saved with the AOF off: their fingerprints, and the save numbers in their
/// headers ([`NO_HEADER`] for one with none, or not there).
type LogsAtSave = ([(u64, u64); 2], [u64; 2]);

/// The save number recorded for a log with no header, or none at all.
const NO_HEADER: u64 = u64::MAX;

/// `appendonly.aof` and `appendonly.aof.prev` as they are on disk now.
fn logs_on_disk() -> std::io::Result<LogsAtSave> {
    let number = |path| aof_delta_generation(&read_head(path)).unwrap_or(NO_HEADER);
    Ok((
        [log_fingerprint(AOF_PATH)?, log_fingerprint(AOF_PREV_PATH)?],
        [number(AOF_PATH), number(AOF_PREV_PATH)],
    ))
}

/// The logs as a save with the AOF off leaves them, to be recorded beside
/// its dump (see [`DumpOnDisk`]). What was queued for the open log before
/// the AOF went off is written first, and the log synced: records of writes
/// the dump holds, which would otherwise reach the log after it was
/// recorded, and read as added to it since.
fn logs_as_saved() -> std::io::Result<LogsAtSave> {
    if let Some(log) = LOG.lock().as_mut() {
        write_queued(&mut log.file, &mut log.rx, None);
        // Not synced, it is shorter after a power cut than recorded: read
        // as changed, it is replayed, as when nothing was recorded. Under
        // `appendfsync always` a failure is not passed over (see
        // [`sync_failed_under_always`]).
        if let Err(e) = log.file.sync_data()
            && fsync_always()
        {
            sync_failed_under_always(&format!("the fsync of {AOF_PATH}"), &e);
        }
    }
    logs_on_disk()
}

/// The name a dump is written under before [`install_dump`] puts it in
/// place, `dump.kdb.<pid>.<n>.tmp`: one of its own for each writer, since a
/// checkpoint and a save with the AOF off can overlap, and two writing one
/// file would interleave.
pub(crate) fn temp_dump_name() -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    format!(
        "dump.kdb.{}.{}.tmp",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

/// Whether `name` is one of [`temp_dump_name`]'s, of any process.
fn is_temp_dump(name: &str) -> bool {
    let number = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    name.strip_prefix("dump.kdb.")
        .and_then(|rest| rest.strip_suffix(".tmp"))
        .and_then(|rest| rest.split_once('.'))
        .is_some_and(|(pid, n)| number(pid) && number(n))
}

/// Remove the dumps that saves cut short (a kill -9, the OOM killer, a
/// Kubernetes liveness probe) left in the data directory, naming each in
/// the log. Each is as big as the dataset, and nothing reads it: recovery
/// goes by `dump.kdb` and the save number beside it. Left there, each crash
/// during a save added one, until the volume was full (#545).
///
/// Only at start, by the process holding the data directory's lock, and
/// before it saves anything: every such file is then another process's, one
/// with this pid included, as every start in a container has the same.
pub fn remove_cut_short_dumps() {
    let Ok(entries) = std::fs::read_dir(".") else {
        return;
    };
    let mut cut_short: Vec<(String, u64)> = entries
        .flatten()
        .filter_map(|entry| {
            let name = entry.file_name().into_string().ok()?;
            is_temp_dump(&name).then(|| (name, entry.metadata().map_or(0, |meta| meta.len())))
        })
        .collect();
    cut_short.sort();
    for (name, len) in cut_short {
        match std::fs::remove_file(&name) {
            Ok(()) => log_warn!(
                "Removed {name} ({len} bytes), the dump of a save that did not finish: nothing \
                 reads it"
            ),
            Err(e) => log_warn!(
                "Can't remove {name}, the dump of a save that did not finish ({e}): nothing \
                 reads it, remove it by hand"
            ),
        }
    }
}

/// Put the dump written to `tmp` in place as save number `generation`, with
/// the checkpoint's turn held. The save number, and which dump it is the
/// number of, take their name before the dump takes its own: a crash between
/// the two leaves a number that does not describe the dump there, which
/// recovery reads as a checkpoint that did not finish. `alone`: saved with
/// the AOF off, no log on disk goes with it.
///
/// The directory is synced between the two renames (#359). A power cut
/// could otherwise keep the dump's rename and lose the number's: an old
/// number beside a new dump, which recovery reads as a dump replaced since,
/// and leaves out the logs that go on from it.
pub(crate) fn install_dump(tmp: &str, generation: u64, alone: bool) -> std::io::Result<()> {
    let logs = if alone { Some(logs_as_saved()?) } else { None };
    write_saved(generation, file_fingerprint(tmp)?, logs)?;
    sync_dir()?;
    failpoint("dump-installing");
    std::fs::rename(tmp, "dump.kdb")?;
    // On disk before the old log goes.
    sync_dir()
}

/// Write `dump.kdb.gen`, synced (the directory is not):
///
/// | bytes | what |
/// |---|---|
/// | 0..8 | save number `generation` |
/// | 8..24 | the fingerprint of its dump, `(len, hash)` |
/// | 24 | 1 if no log on disk goes with that dump (`logs` given), else 0 |
/// | 25..41, 41..57 | with 1: the fingerprints of `appendonly.aof` and `appendonly.aof.prev` at that save, an absent one as empty |
/// | 57..65, 65..73 | with 1: the save numbers in their headers, [`NO_HEADER`] for none |
///
/// Releases before 1.2.1 read the first 24 bytes only.
fn write_saved(
    generation: u64,
    (len, hash): (u64, u64),
    logs: Option<LogsAtSave>,
) -> std::io::Result<()> {
    use std::io::Write;
    let gen_tmp = format!("{AOF_GEN_PATH}.tmp");
    {
        let mut file = std::fs::File::create(&gen_tmp)?;
        file.write_all(&generation.to_le_bytes())?;
        file.write_all(&len.to_le_bytes())?;
        file.write_all(&hash.to_le_bytes())?;
        file.write_all(&[u8::from(logs.is_some())])?;
        if let Some((fingerprints, numbers)) = logs {
            for (log_len, log_hash) in fingerprints {
                file.write_all(&log_len.to_le_bytes())?;
                file.write_all(&log_hash.to_le_bytes())?;
            }
            for number in numbers {
                file.write_all(&number.to_le_bytes())?;
            }
        }
        file.sync_all()?;
    }
    std::fs::rename(&gen_tmp, AOF_GEN_PATH)
}

/// `dump.kdb.gen` (see [`write_saved`]).
struct Saved {
    number: u64,
    /// The fingerprint of the dump it was written for.
    dump: (u64, u64),
    /// No log on disk goes with that dump: it was saved with the AOF off.
    alone: bool,
    /// With `alone`, the logs as they were at that save. Not in the file of
    /// a save made before they were recorded.
    logs: Option<[(u64, u64); 2]>,
    /// With `logs`, the save numbers in their headers then. Not in the file
    /// of a save made before they were recorded.
    numbers: Option<[u64; 2]>,
}

fn read_saved() -> Option<Saved> {
    let saved = std::fs::read(AOF_GEN_PATH).ok()?;
    let field = |at: usize| Some(u64::from_le_bytes(saved.get(at..at + 8)?.try_into().ok()?));
    let pair = |at: usize| Some((field(at)?, field(at + 8)?));
    let alone = saved.get(24) == Some(&1);
    let logs = if alone {
        pair(25).zip(pair(41)).map(|(log, prev)| [log, prev])
    } else {
        None
    };
    let numbers = logs.and(pair(57)).map(|(log, prev)| [log, prev]);
    Some(Saved {
        number: field(0)?,
        dump: pair(8)?,
        alone,
        logs,
        numbers,
    })
}

/// The first bytes of a log, enough for its header; empty if there is none.
fn read_head(path: &str) -> Vec<u8> {
    use std::io::Read;
    let mut head = Vec::new();
    if let Ok(file) = std::fs::File::open(path) {
        let _ = file
            .take(AOF_DELTA_HEADER_LEN as u64)
            .read_to_end(&mut head);
    }
    head
}

/// Whether the checkpoint that left `appendonly.aof.prev` behind finished:
/// the save number on disk is the one the log goes on from, and it was
/// written for the `dump.kdb` that is there.
fn checkpoint_completed(log: &[u8]) -> bool {
    let Some(saved) = read_saved() else {
        return false;
    };
    aof_delta_generation(log) == Some(saved.number)
        && file_fingerprint("dump.kdb").is_ok_and(|found| found == saved.dump)
}

/// The save number for a checkpoint or a save that has its turn: past the
/// one on disk, past any this process chose before (a checkpoint that failed
/// wrote none, and the next does not take its number again), and past the
/// number of any log on disk, so that a new dump never shares its number
/// with a log that does not go with it.
pub(crate) fn next_generation() -> u64 {
    // A turn-over's new log too, left by one that stopped: the next start
    // with the AOF on puts it in place. A save that took its number made
    // the old log beside it read as that of a finished checkpoint (#368).
    let logs = [AOF_PATH, AOF_PREV_PATH, TURN_OVER_TMP]
        .into_iter()
        .filter_map(|path| aof_delta_generation(&read_head(path)))
        .max()
        .unwrap_or(0);
    let next = read_generation()
        .max(LAST_GENERATION.load(Ordering::Relaxed))
        .max(logs)
        .saturating_add(1);
    LAST_GENERATION.store(next, Ordering::Relaxed);
    next
}

/// The `dump.kdb` on disk, as recovery sees it next to a log: which logs it
/// is newer than, and so must not have replayed on top of it (#354).
///
/// A log is a delta on the dump of its save number: `dump.kdb.gen` holds the
/// number of the dump on disk and its fingerprint. Saves with the AOF off in
/// 1.2.0 wrote a new `dump.kdb` and left `dump.kdb.gen` as it was: its number
/// is then the old log's, and its fingerprint not the dump's. Comparing the
/// numbers alone, recovery took the log for one going on from the dump and
/// replayed it on top of the newer data: writes applied twice, deletions
/// undone.
///
/// The fingerprint is what tells them apart, and where it does not match,
/// the number still says something. A checkpoint stopped between writing its
/// number and putting its dump in place leaves the number of its new log,
/// beside the old log, which goes with the dump that is there: both are
/// replayed. A number that is a log's own while its dump is not there means
/// that dump was replaced since: the log is older. A dump saved with the AOF
/// off is marked as one no log on disk goes with, and the logs as they were
/// at that save are recorded beside it: a log with no header (made by 1.2.0)
/// that is still as it was then is older too. When in doubt (no number, a
/// log with no header beside a dump not so marked, or changed since), a log
/// is replayed, as before.
///
/// A log can be added to after such a save: 1.2.0, started with the AOF on,
/// adds to the log it finds, replaying it first if it has no header and
/// leaving it out if its number is older (#359). What was added goes on from
/// the dump, and is replayed: with a log with no header, the whole log, as
/// 1.2.0 did; with an older number, only what was added.
///
/// A log left out is never lost: it is kept aside (see [`keep_aside`]).
struct DumpOnDisk {
    /// `dump.kdb` was there and was loaded.
    kdb: bool,
    /// `dump.kdb.gen`, and whether it was written for the `dump.kdb` there.
    saved: Option<(Saved, bool)>,
}

/// What recovery makes of a log beside the dump on disk (see [`DumpOnDisk`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LogAge {
    /// It goes on from the dump, or nothing says otherwise: replayed.
    GoesOn,
    /// It goes with a dump that has been replaced since: left out.
    Older,
    /// Older, but added to since the dump was saved with the AOF off, from
    /// this byte on: what follows goes on from the dump.
    AddedToFrom(u64),
}

impl DumpOnDisk {
    fn read(kdb: bool) -> Self {
        let saved = read_saved().map(|saved| {
            let own = file_fingerprint("dump.kdb").is_ok_and(|found| found == saved.dump);
            (saved, own)
        });
        DumpOnDisk { kdb, saved }
    }

    /// The log at `path` (the log, or the old log), against the dump.
    fn age_of(&self, path: &str) -> LogAge {
        let Some((saved, own)) = self.saved.as_ref().filter(|_| self.kdb) else {
            return LogAge::GoesOn;
        };
        let head = read_head(path);
        match (aof_delta_generation(&head), *own) {
            // The number is this dump's: an older log goes with an older
            // one, unless it was added to after this dump was saved.
            (Some(log), true) if log < saved.number => {
                if written_since_a_save_with_no_log(path, saved) {
                    LogAge::AddedToFrom(AOF_DELTA_HEADER_LEN as u64)
                } else {
                    match added_to_since(path, saved) {
                        Some(from) => LogAge::AddedToFrom(from),
                        None => LogAge::Older,
                    }
                }
            }
            // No log goes with this dump: one with no header is older, if it
            // is as it was when the dump was saved.
            (None, true) if saved.alone && !head.is_empty() => {
                let as_saved = saved.logs.is_some_and(|logs| {
                    file_fingerprint(path).is_ok_and(|found| logs.contains(&found))
                });
                if as_saved {
                    LogAge::Older
                } else {
                    LogAge::GoesOn
                }
            }
            // The number is the log's own, and its dump is gone.
            (Some(log), false) if log == saved.number => LogAge::Older,
            _ => LogAge::GoesOn,
        }
    }
}

impl DumpOnDisk {
    /// Whether the log `buf`, which [`DumpOnDisk::age_of`] says goes on from
    /// the dump, is replayed, on its own, beside an old log that is older
    /// than the dump (#363). Left out with the old log, its records were only
    /// in the copy kept aside. It is when it holds records, the dump is the
    /// one `dump.kdb.gen` was written for, and it is a log with no header (as
    /// without the old log, it is replayed whole), or one made after the
    /// save, when there was no log: 1.2.0 makes a delta on the next number
    /// then, for an old log with no log beside it, and adds to it.
    fn replayed_beside_an_older_old_log(&self, buf: &[u8]) -> bool {
        let Some((saved, own)) = self.saved.as_ref().filter(|_| self.kdb) else {
            return false;
        };
        if !*own || buf.len() <= records_start(buf) || buf.starts_with(AOF_MAGIC) {
            return false;
        }
        // A delta on this dump's number is never here: `checkpoint_completed`
        // is true for it, and the old log is not judged at all. One on a
        // number below it, made after the save, is added to (see
        // [`written_since_a_save_with_no_log`]); this is one above it.
        match aof_delta_generation(buf) {
            None => true,
            Some(_) => saved.logs.is_some_and(|logs| logs[0].0 == 0),
        }
    }
}

/// Whether the delta at `path`, holding records, was made after the dump of
/// `saved` was saved with the AOF off, when there was no such log: all its
/// records were written since, whatever the number in its header. 1.2.0 puts
/// a turn-over's new log left from before the save in place, a delta on a
/// number below the save's, and adds to it; read as older than the dump, the
/// log was left out with those writes.
///
/// Only for the log. An old log made after such a save, a delta below its
/// number, is not reached: 1.2.0 starts a log with no header where there is
/// none, and its turn-overs give the new log a number past the save's.
fn written_since_a_save_with_no_log(path: &str, saved: &Saved) -> bool {
    let Some(logs) = saved.logs.filter(|_| saved.alone && path == AOF_PATH) else {
        return false;
    };
    logs[0].0 == 0
        && std::fs::metadata(path).is_ok_and(|meta| meta.len() > AOF_DELTA_HEADER_LEN as u64)
}

/// Where what was added to the log at `path` since the dump of `saved` was
/// saved with the AOF off starts, if it has only been added to since: it is
/// longer now, and its first bytes are those recorded at that save. Either
/// name counts, as a checkpoint renames the log to the old log's.
///
/// An old log that was there at that save, as an old log, has been added to
/// by a turn-over: 1.2.0's adds the log's records to an old log it still
/// needs, the log's records from before the save among them, which the dump
/// holds. Those come first, and are skipped: only once they are found to be
/// what the log held at the save. If that cannot be told (the save did not
/// record the log's header, or they are not), `None`: the old log is older
/// than the dump, and kept aside whole. Replayed from where it was, it
/// applied the log's records a second time.
fn added_to_since(path: &str, saved: &Saved) -> Option<u64> {
    use std::io::{Read, Seek, SeekFrom};
    let logs = saved.logs?;
    let mut file = std::fs::File::open(path).ok()?;
    let len = file.metadata().ok()?.len();
    let (which, was) = logs
        .into_iter()
        .enumerate()
        .find_map(|(which, (was, hash))| {
            let added_to = was >= AOF_DELTA_HEADER_LEN as u64
                && was < len
                && fingerprint_of_first(&file, was).is_ok_and(|found| found == (was, hash));
            added_to.then_some((which, was))
        })?;
    if path != AOF_PREV_PATH || which != 1 {
        return Some(was);
    }
    let (log_was, log_hash) = logs[0];
    if log_was == 0 {
        // No log then: what was added came after.
        return Some(was);
    }
    let mut then = match saved.numbers?[0] {
        NO_HEADER => Vec::new(),
        number => delta_header(number).to_vec(),
    };
    let records = log_was.checked_sub(then.len() as u64)?;
    file.seek(SeekFrom::Start(was)).ok()?;
    (&mut file).take(records).read_to_end(&mut then).ok()?;
    (fingerprint_of_bytes(&then) == (log_was, log_hash)).then_some(was + records)
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

/// Where the records of a log start: after its delta header, if it has one.
fn records_start(buf: &[u8]) -> usize {
    if aof_delta_generation(buf).is_some() {
        AOF_DELTA_HEADER_LEN
    } else {
        0
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

    // Zeros to the end of the file: what a power loss leaves where the last
    // writes were. The file system recorded the file's new length, and the
    // data never reached the disk (ext4 commits the size first). Those
    // writes were never synced, so cut off with whatever record they end;
    // a server refused to start over them. Zeros with anything after them
    // are not a tail: a hole in the middle of the log is damage.
    let end = rest
        .iter()
        .rposition(|&b| b != 0)
        .map_or(0, |last| last + 1);
    if end < rest.len() {
        return if end == 0 {
            Tail::Truncated
        } else {
            classify_tail(&rest[..end])
        };
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
    // Keys outside database 0 of a cluster node: the file is fine, the
    // node cannot serve them (#484).
    if what.contains(crate::snapshot::CLUSTER_KEYS_OUTSIDE_DB0) {
        log_fatal!("{}.", crate::snapshot::CLUSTER_KEYS_OUTSIDE_DB0_AT_START);
        std::process::exit(1);
    }
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
    // Each record is applied to the keys as they were when it was written:
    // a key the master expired since has a `DEL` further on. Expired by the
    // clock now, `SET k 5 PXAT t` and an `INCR k` made while it lived
    // replayed after `t` as an INCR of a missing key, `1` with no time to
    // live, for good. The preamble's keys too, past their time or not. And no key is
    // swept meanwhile, by the expiry threads, which run from before the load.
    // Nor does a `PEXPIREAT` whose moment has come delete its key: it sets
    // it, and the writes after it go into the key (`expiry::replay`).
    let _hold = crate::expiry::hold();
    let _loading = crate::expiry::Loading::start();
    let _replay = crate::expiry::replay();
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
        let loaded = crate::snapshot::load_snapshot_bytes(image)
            .map_err(|e| crate::snapshot::refusal_of_file(e, "the AOF's preamble"))?;
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
        if let Some(delay) = test_replay_delay() {
            std::thread::sleep(delay);
        }
    }

    Ok(stats)
}

/// For the tests of what runs beside a replay, and never set otherwise:
/// `KIVIDB_TEST_REPLAY_DELAY_MS`, how long each command of a replay of the
/// AOF takes, after it is applied.
fn test_replay_delay() -> Option<std::time::Duration> {
    static DELAY: std::sync::OnceLock<Option<std::time::Duration>> = std::sync::OnceLock::new();
    *DELAY.get_or_init(|| {
        let ms: u64 = std::env::var("KIVIDB_TEST_REPLAY_DELAY_MS")
            .ok()?
            .parse()
            .ok()?;
        Some(std::time::Duration::from_millis(ms))
    })
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
    /// The fingerprint of the `dump.kdb` that was loaded, if one was: what a
    /// point or marker saved beside it must match (#303).
    pub dump_fingerprint: Option<crate::replication::DumpFingerprint>,
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
    // Keys past their time are loaded too, from the snapshot as from the
    // log: the log's writes to them were made while they lived, and a key
    // that went since has its `DEL` in the log. Expiry takes the rest out
    // once the server is up, and logs their `DEL`s: not before both are in,
    // though the expiry threads are running already.
    let _hold = crate::expiry::hold();
    let _loading = crate::expiry::Loading::start();
    let mut recovery = LocalRecovery {
        index_names: Vec::new(),
        hnsw_loaded: false,
        exact_dump: false,
        dump_fingerprint: None,
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

    warn_of_kept_logs();
    let kdb_path = std::path::Path::new("dump.kdb");
    let rdb_path = std::path::Path::new("dump.rdb");
    let mut dump_loaded = false;
    // `dump.kdb` was loaded, even with nothing in it: what the logs are
    // checked against (see [`DumpOnDisk`]).
    let mut kdb_loaded = false;

    if kdb_path.exists() {
        log_info!("Found dump.kdb — loading native snapshot");
        match crate::replication::load_dump("dump.kdb") {
            Ok((loaded, fingerprint)) => {
                kdb_loaded = true;
                recovery.exact_dump = true;
                recovery.dump_fingerprint = Some(fingerprint);
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

    if aof_enabled {
        recover_turn_over();
    }
    if should_recover_from_aof(aof_enabled) {
        let mut buf = match std::fs::read(AOF_PATH) {
            Ok(buf) => buf,
            Err(e) => fatal_persistence(&format!("{AOF_PATH} cannot be read: {e}")),
        };
        // The logs are replayed only over the dump they go on from (#354).
        let on_disk = DumpOnDisk::read(kdb_loaded);
        let older_than_dump = |what: &str| {
            log_warn!(
                "AOF: {what} older than dump.kdb (saved since, with the AOF off, or replaced) — \
                 not replayed: replayed on top of it, writes the dump holds would be applied \
                 twice. The old log is kept aside, and {AOF_PATH} starts again on dump.kdb"
            );
        };
        // Left out: it starts again on the dump, empty, before anything is
        // logged. Left as it was, what is written from now on would go after
        // the old records, and be left out with them at the next start.
        let mut left_out = false;
        // A checkpoint turned the log over and was stopped before its
        // snapshot reached the disk: the old log, then the new one, on top of
        // the old snapshot. One that did finish only left the old log behind.
        let mut resumed_checkpoint = false;
        // What an older log had added to it after the dump was saved with the
        // AOF off, its name, and whether it is all of the log: it goes on
        // from the dump (see [`LogAge::AddedToFrom`]).
        let mut added: Option<(&str, bool, Vec<u8>)> = None;
        if std::path::Path::new(AOF_PREV_PATH).exists() {
            // None: the checkpoint that left it behind finished.
            let prev_age = (!checkpoint_completed(&buf)).then(|| on_disk.age_of(AOF_PREV_PATH));
            if prev_age.is_none() {
                // With a dump saved with the AOF off, the old log may hold
                // writes it does not: it is not removed without a copy, and
                // the server does not start without one. Otherwise the dump
                // holds them, and an old log that cannot be removed is left
                // in place (#375). Until it is removed, each turn-over fails
                // on it, and so does each checkpoint: the warning says so.
                if let Err(e) = keep_finished_old_log() {
                    log_fatal!(
                        "{AOF_PREV_PATH}, left by a checkpoint that finished, cannot be kept \
                         aside: {e}. dump.kdb was saved with the AOF off since, and may not hold \
                         its writes, so it is not removed without a copy. Free disk space in the \
                         data directory, or move {AOF_PREV_PATH} out of it, and start again"
                    );
                    std::process::exit(1);
                }
                if let Err(e) = remove_if_there(AOF_PREV_PATH) {
                    log_warn!(
                        "AOF: {AOF_PREV_PATH}, left by a checkpoint that finished, could not be \
                         removed: {e} — it is left in place. dump.kdb holds its writes. Until it is \
                         removed, every checkpoint fails (INFO persistence shows \
                         aof_last_bgrewrite_status:err) and {AOF_PATH} keeps growing: fix the \
                         permissions of the data directory, and the next checkpoint removes it, or \
                         remove {AOF_PREV_PATH} by hand"
                    );
                }
            } else if prev_age == Some(LogAge::Older) {
                // The log is judged on its own. What was added to it since
                // the dump was saved goes on from the dump (#359), and so
                // does a log that, alone, would be replayed (#363): left out
                // with the old log, their records were only in the copies
                // kept aside.
                match on_disk.age_of(AOF_PATH) {
                    LogAge::AddedToFrom(from) => {
                        added = Some((
                            AOF_PATH,
                            false,
                            buf.get(from as usize..).unwrap_or_default().to_vec(),
                        ));
                    }
                    LogAge::GoesOn if on_disk.replayed_beside_an_older_old_log(&buf) => {
                        added = Some((AOF_PATH, true, buf[records_start(&buf)..].to_vec()));
                    }
                    _ => {
                        older_than_dump(&format!("{AOF_PREV_PATH} and {AOF_PATH} are"));
                        left_out = true;
                    }
                }
            } else if let Some(LogAge::AddedToFrom(from)) = prev_age {
                // And the log goes on from what was added.
                let prev = match std::fs::read(AOF_PREV_PATH) {
                    Ok(prev) => prev,
                    Err(e) => fatal_persistence(&format!("{AOF_PREV_PATH} cannot be read: {e}")),
                };
                let mut both = prev.get(from as usize..).unwrap_or_default().to_vec();
                both.extend_from_slice(&buf[records_start(&buf)..]);
                added = Some((AOF_PREV_PATH, true, both));
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
            && !left_out
            && added.is_none()
            && dump_generation > 0
            && aof_generation > dump_generation
        {
            fatal_persistence(&format!(
                "{AOF_PATH} continues from save number {aof_generation}, but dump.kdb is save \
                 number {dump_generation}: the two files do not belong together, and the writes \
                 made between those saves are in neither"
            ));
        }
        if !left_out && !resumed_checkpoint && added.is_none() {
            match on_disk.age_of(AOF_PATH) {
                LogAge::Older => {
                    older_than_dump(&format!("{AOF_PATH} is"));
                    left_out = true;
                }
                LogAge::AddedToFrom(from) => {
                    added = Some((
                        AOF_PATH,
                        false,
                        buf.get(from as usize..).unwrap_or_default().to_vec(),
                    ));
                }
                LogAge::GoesOn => {}
            }
        }
        // The records added go on from the dump: the log starts again on it
        // with them, and is replayed. The old logs are kept aside, whole.
        if let Some((name, log_whole, added)) = added {
            if name == AOF_PATH && log_whole {
                log_warn!(
                    "AOF: {AOF_PATH} was written to after dump.kdb was saved with the AOF off (by \
                     an earlier release started with the AOF on) — it is replayed on top of \
                     dump.kdb, all of it, as that release replayed it; {AOF_PREV_PATH}, older \
                     than dump.kdb, is not"
                );
            } else {
                log_warn!(
                    "AOF: {name} was added to after dump.kdb was saved with the AOF off (by an \
                     earlier release started with the AOF on) — what was added is replayed on \
                     top of dump.kdb; the records from before that save, which dump.kdb holds, \
                     are not"
                );
            }
            let why = NewLogWhy::AddedTo {
                prev: name == AOF_PREV_PATH,
                log_whole,
            };
            if let Err(e) = start_new_log(why, &added) {
                fatal_persistence(&format!(
                    "{AOF_PATH} cannot be started again on dump.kdb with what was added to it: {e}"
                ));
            }
            buf = match std::fs::read(AOF_PATH) {
                Ok(buf) => buf,
                Err(e) => fatal_persistence(&format!("{AOF_PATH} cannot be read: {e}")),
            };
        }
        if !left_out {
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
                // Delta writes are not in the snapshot's HNSW graph: the
                // backfill after this brings it up to date with the keys.
                if dump_loaded {
                    recovery.hnsw_loaded = false;
                }
            }
        }
        if left_out && let Err(e) = start_new_log(NewLogWhy::Older, &[]) {
            fatal_persistence(&format!(
                "{AOF_PATH} is older than dump.kdb and cannot be started again on it: {e}"
            ));
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
    // A file of a Dragonfly snapshot in Dragonfly's own format, by its name:
    // `dump-<time>-summary.dfs` and a `dump-<time>-NNNN.dfs` for each shard.
    // Each is an RDB file, and before Dragonfly 1.32 nothing in them says
    // what they are: the summary loaded as an empty dataset, a shard's file
    // as that shard's keys (#444).
    if base
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("dfs"))
    {
        return Err(invalid(format!(
            "{} is a file of a Dragonfly snapshot in Dragonfly's own format (.dfs), which \
             keeps each shard's keys in a file of its own; save one in RDB format \
             (Dragonfly's SAVE RDB) and load that",
            base.display()
        )));
    }

    log_info!("preload-file: loading {}", base.display());
    let base_str = base.to_string_lossy();
    // Redis's header or Valkey 9's (`VALKEY080`).
    let is_redis_rdb = std::fs::File::open(&base)
        .and_then(|mut f| {
            let mut magic = [0u8; 6];
            std::io::Read::read_exact(&mut f, &mut magic).map(|()| magic)
        })
        .is_ok_and(|magic| crate::snapshot::is_rdb_header(&magic));
    let mut index_names = Vec::new();
    let keys = if is_redis_rdb {
        let (dbs, expiries, functions) = crate::snapshot::load_redis_rdb_with_expiry(&base_str)?;
        // A Dragonfly file with no keys at all is what the summary file of a
        // snapshot in Dragonfly's own format is, renamed; up to Dragonfly
        // 1.31 nothing else tells them apart. Either way there is nothing to
        // start from.
        if dbs.iter().all(|db| db.is_empty())
            && crate::snapshot::rdb_written_by_dragonfly(&base_str)?
        {
            return Err(invalid(format!(
                "{} was written by Dragonfly and holds no keys: it is the summary file of \
                 a snapshot in Dragonfly's own format (.dfs), or a snapshot of an empty \
                 Dragonfly. Save the data in RDB format (Dragonfly's SAVE RDB) and load that",
                base.display()
            )));
        }
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

// ── Test hooks ───────────────────────────────────────────────────────────────

/// `KIVIDB_TEST_FAILPOINTS`: names of steps (of a checkpoint, of a
/// `MIGRATE`, of an index-wide walk) at which the process waits for a test,
/// which can then start something else or kill it at exactly that step. At
/// each step named it creates `failpoint.<name>.<n>` in the data directory
/// (`n` counts from 1 for each name) and waits while the file is there. Read
/// once; unset, a step costs a load and a branch.
pub(crate) fn failpoint(name: &'static str) {
    if !failpoint_named(name) {
        return;
    }
    static HITS: parking_lot::Mutex<Vec<(&'static str, u64)>> = parking_lot::Mutex::new(Vec::new());
    let hit = {
        let mut hits = HITS.lock();
        match hits.iter_mut().find(|(n, _)| *n == name) {
            Some((_, count)) => {
                *count += 1;
                *count
            }
            None => {
                hits.push((name, 1));
                1
            }
        }
    };
    let path = format!("failpoint.{name}.{hit}");
    if std::fs::write(&path, b"").is_err() {
        return;
    }
    log_warn!("Test failpoint {path} reached — waiting for it to be removed");
    // Off the runtime: a tokio worker held here for as long as a test likes
    // must not take the polling of every other client with it.
    crate::cluster::migration::off_the_runtime(|| {
        while std::path::Path::new(&path).exists() {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    });
}

/// Whether `KIVIDB_TEST_FAILPOINTS` names `name`. Read once.
fn failpoint_named(name: &str) -> bool {
    static NAMES: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();
    NAMES
        .get_or_init(|| {
            std::env::var("KIVIDB_TEST_FAILPOINTS")
                .map(|list| {
                    list.split(',')
                        .map(|name| name.trim().to_string())
                        .filter(|name| !name.is_empty())
                        .collect()
                })
                .unwrap_or_default()
        })
        .iter()
        .any(|n| n == name)
}

/// A step a test makes fail, named in `KIVIDB_TEST_FAILPOINTS` as a
/// [`failpoint`] is: whether it is to fail.
fn injected_failure(name: &str) -> bool {
    failpoint_named(name)
}

/// What a node did before, that a test has it do again, named in
/// `KIVIDB_TEST_FAILPOINTS` as a [`failpoint`] is: whether it is to.
pub(crate) fn test_behaves_as_before(name: &str) -> bool {
    failpoint_named(name)
}

// ── Checkpoint in progress ───────────────────────────────────────────────────

/// True while [`checkpoint`] holds the store. `INFO persistence` reports it as
/// `aof_rewrite_in_progress`, and the auto-checkpoint thread skips overlapping
/// runs.
pub static REWRITE_ACTIVE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

#[cfg(test)]
mod kept_file_tests {
    use super::*;

    fn in_temp(name: &str) -> (tempfile::TempDir, String) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(name).to_str().unwrap().to_string();
        (dir, path)
    }

    /// A copy that fails part-way leaves nothing named like a kept log
    /// behind (#359): every start listed it, and each attempt added one.
    #[test]
    fn a_copy_that_fails_leaves_no_partial_file() {
        let (dir, from) = in_temp("appendonly.aof");
        std::fs::write(&from, b"*1\r\n$4\r\nPING\r\n").unwrap();
        let to = dir
            .path()
            .join("superseded.1.0.appendonly.aof")
            .to_str()
            .unwrap()
            .to_string();
        let failed = copy_synced(&from, &to, |_, to| {
            std::fs::write(to, b"*1\r\n$4")?;
            Err(std::io::Error::other("disk full"))
        });
        assert!(failed.is_err());
        assert!(
            !std::path::Path::new(&to).exists(),
            "the partial copy was left"
        );
        // And one that works is kept, whole.
        copy_synced(&from, &to, |from, to| std::fs::copy(from, to)).unwrap();
        assert_eq!(std::fs::read(&to).unwrap(), std::fs::read(&from).unwrap());
    }

    /// Same bytes, read in full: not just the same length, or the same ends.
    #[test]
    fn same_bytes_reads_all_of_both_files() {
        let (dir, a) = in_temp("a");
        let b = dir.path().join("b").to_str().unwrap().to_string();
        let bytes: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
        std::fs::write(&a, &bytes).unwrap();
        std::fs::write(&b, &bytes).unwrap();
        assert!(same_bytes(&a, &b).unwrap());
        let mut middle = bytes.clone();
        middle[100_000] ^= 1;
        std::fs::write(&b, &middle).unwrap();
        assert!(!same_bytes(&a, &b).unwrap());
        std::fs::write(&b, &bytes[..199_999]).unwrap();
        assert!(!same_bytes(&a, &b).unwrap());
    }

    /// The fingerprint of a log's first bytes is the one it had when it was
    /// that long, at every size around the sampled edges.
    #[test]
    fn a_log_added_to_has_its_old_fingerprint_at_its_old_length() {
        let (_dir, path) = in_temp("appendonly.aof");
        let bytes: Vec<u8> = (0..300_000u32).map(|i| (i * 7 % 251) as u8).collect();
        for was in [0, 16, 1000, 65_536, 65_537, 131_072, 131_073, 200_000] {
            std::fs::write(&path, &bytes[..was]).unwrap();
            let then = file_fingerprint(&path).unwrap();
            std::fs::write(&path, &bytes).unwrap();
            let file = std::fs::File::open(&path).unwrap();
            assert_eq!(
                fingerprint_of_first(&file, was as u64).unwrap(),
                then,
                "{was}"
            );
        }
        // One handle, several lengths, in any order.
        std::fs::write(&path, &bytes).unwrap();
        let file = std::fs::File::open(&path).unwrap();
        for was in [1000u64, 16, 200_000, 16] {
            assert_eq!(
                fingerprint_of_first(&file, was).unwrap(),
                fingerprint_of_bytes(&bytes[..was as usize]),
                "{was}"
            );
        }
        // And a file's fingerprint is that of what it holds.
        for len in [0, 16, 65_536, 65_537, 131_073, 300_000] {
            std::fs::write(&path, &bytes[..len]).unwrap();
            assert_eq!(
                fingerprint_of_bytes(&bytes[..len]),
                file_fingerprint(&path).unwrap(),
                "{len}"
            );
        }
        // An absent log is recorded as an empty one.
        std::fs::write(&path, b"").unwrap();
        let empty = file_fingerprint(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        assert_eq!(log_fingerprint(&path).unwrap(), empty);
    }
}

#[cfg(test)]
mod kept_warning_tests {
    use super::*;

    const NAME: &str = "superseded.1.0.appendonly.aof";

    fn says_to_apply(warning: &str) -> bool {
        warning.contains("redis-cli --pipe")
    }

    /// A kept log whose writes are in the dataset is never to be sent to
    /// the server again: its warning says so, and gives no command for it.
    #[test]
    fn a_log_whose_writes_are_in_the_dataset_is_not_to_be_applied() {
        let enable = kept_warning(NewLogWhy::Enable, AOF_PATH, NAME);
        assert!(enable.contains("already in the dataset"), "{enable}");
        assert!(!says_to_apply(&enable), "{enable}");
        let added = kept_warning(
            NewLogWhy::AddedTo {
                prev: false,
                log_whole: false,
            },
            AOF_PATH,
            NAME,
        );
        assert!(added.contains("has been replayed"), "{added}");
        assert!(!says_to_apply(&added), "{added}");
        // All of the log replayed (#363): with the old log added to, or the
        // log judged on its own.
        for prev in [false, true] {
            let whole = kept_warning(
                NewLogWhy::AddedTo {
                    prev,
                    log_whole: true,
                },
                AOF_PATH,
                NAME,
            );
            assert!(
                whole.contains("All of its records have been replayed"),
                "{whole}"
            );
            assert!(!whole.contains("was not replayed"), "{whole}");
            assert!(!says_to_apply(&whole), "{whole}");
        }
        let prev_added = kept_warning(
            NewLogWhy::AddedTo {
                prev: true,
                log_whole: true,
            },
            AOF_PREV_PATH,
            NAME,
        );
        assert!(prev_added.contains("has been replayed"), "{prev_added}");
        assert!(!says_to_apply(&prev_added), "{prev_added}");
    }

    /// The old log of a finished checkpoint beside a save with the AOF off
    /// may hold records the dataset has: no command to send it whole (#368).
    #[test]
    fn an_old_log_kept_beside_a_save_with_the_aof_off_is_not_to_be_sent_whole() {
        let warning = kept_warning(NewLogWhy::FinishedBesideSaveWithAofOff, AOF_PREV_PATH, NAME);
        assert!(!says_to_apply(&warning), "{warning}");
        assert!(warning.contains("send only those"), "{warning}");
        assert!(warning.contains(NAME), "{warning}");
    }

    /// One whose writes were never loaded says how to apply them.
    #[test]
    fn a_log_not_loaded_says_how_to_apply_its_writes() {
        for (why, path) in [
            (NewLogWhy::Older, AOF_PATH),
            (NewLogWhy::Older, AOF_PREV_PATH),
            (NewLogWhy::EnableOverEarlierRun, AOF_PATH),
            // The old log beside a log added to, itself not added to.
            (
                NewLogWhy::AddedTo {
                    prev: false,
                    log_whole: false,
                },
                AOF_PREV_PATH,
            ),
            (
                NewLogWhy::AddedTo {
                    prev: false,
                    log_whole: true,
                },
                AOF_PREV_PATH,
            ),
        ] {
            let warning = kept_warning(why, path, NAME);
            assert!(says_to_apply(&warning), "{why:?} {path}: {warning}");
            assert!(warning.contains("not in the dataset"), "{warning}");
            assert!(warning.contains(NAME), "{warning}");
        }
    }
}

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

    /// A power loss leaves zeros where the last, unsynced, writes were: the
    /// file's length reached the disk and its data did not. That is a
    /// truncation, after a whole record or in the middle of one; zeros with
    /// something after them are a hole, and damage.
    #[test]
    fn zeros_to_the_end_of_the_file_are_a_truncation() {
        let whole = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nvalue\r\n";
        assert_eq!(classify_tail(&[0u8; 316]), Tail::Truncated);
        for cut in 1..whole.len() {
            let mut torn = whole[..cut].to_vec();
            torn.extend_from_slice(&[0u8; 64]);
            assert_eq!(classify_tail(&torn), Tail::Truncated, "cut at {cut}");
        }
        let mut hole = vec![0u8; 64];
        hole.extend_from_slice(whole);
        assert_eq!(classify_tail(&hole), Tail::Corrupt);
        let mut damaged = b"not a record".to_vec();
        damaged.extend_from_slice(&[0u8; 64]);
        assert_eq!(classify_tail(&damaged), Tail::Corrupt);
    }

    /// Replayed, a log ending in zeros loads every whole record and is cut
    /// where the zeros start.
    #[test]
    fn a_log_ending_in_zeros_is_cut_where_they_start() {
        let mut buf = enc(&["SET", "a", "1"]);
        buf.extend_from_slice(&enc(&["SET", "b", "2"]));
        let valid = buf.len();
        buf.extend_from_slice(b"*3\r\n$3\r\nSE");
        buf.extend_from_slice(&[0u8; 4096]);
        let (commands, consumed) = parse_aof_commands(&buf);
        assert_eq!(commands.len(), 2);
        assert_eq!(consumed, valid);
        assert_eq!(classify_tail(&buf[consumed..]), Tail::Truncated);
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

#[cfg(test)]
mod backlog_tests {
    use super::*;

    /// A queue holding more than one batch is written whole in one call: a
    /// replica's stream, with no reply to write it, has only the writer's
    /// passes, and one batch a pass fell behind sustained writes.
    #[test]
    fn everything_queued_is_written_not_one_batch_of_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("appendonly.aof");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .expect("open");
        let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let record = vec![b'x'; 64 * 1024];
        let records = 3 * MAX_AOF_BATCH_BYTES / record.len() + 7;
        for _ in 0..records {
            tx.send(record.clone()).expect("send");
        }
        let (wrote, released) = write_queued(&mut file, &mut rx, None);
        assert!(wrote);
        assert!(!released);
        assert!(rx.try_recv().is_err(), "nothing is left queued");
        let len = std::fs::metadata(&path).expect("metadata").len();
        assert_eq!(len, (records * record.len()) as u64);
    }

    /// One pass lets the log go after its budget, so that a queue fed
    /// faster than the disk takes it does not keep the second's sync out;
    /// the rest goes on the next.
    #[test]
    fn a_pass_stops_at_its_budget_and_the_next_takes_the_rest() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("appendonly.aof");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .expect("open");
        let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let record = vec![b'x'; MAX_AOF_BATCH_BYTES];
        let records = MAX_AOF_PASS_BYTES / record.len() + 3;
        for _ in 0..records {
            tx.send(record.clone()).expect("send");
        }
        write_queued(&mut file, &mut rx, None);
        let len = std::fs::metadata(&path).expect("metadata").len();
        assert_eq!(len, MAX_AOF_PASS_BYTES as u64, "one pass writes its budget");
        write_queued(&mut file, &mut rx, None);
        assert!(rx.try_recv().is_err(), "the next pass takes the rest");
        let len = std::fs::metadata(&path).expect("metadata").len();
        assert_eq!(len, (records * record.len()) as u64);
    }
}

/// Time series commands in an AOF: those written by 1.2.0, with what that
/// version accepted and 1.2.1 refuses from a client (#565), a chunk size
/// out of bounds, `IGNORE`, replayed rather than the series lost, and its
/// `TS.INCRBY` records read as it read them; and 1.2.1's, replayed as they
/// ran.
#[cfg(test)]
mod ts_option_replay_tests {
    use super::past_deadline_replay_tests::{replayed, replayed_with_index};
    use crate::store::ValueRef;

    /// `TS.INCRBY` records as 1.2.0 wrote them, `TIMESTAMP <time>` at the
    /// end, read as 1.2.0 read them: each sample at its time, the options
    /// passed over, no trim to the retention, and the records after them
    /// applied. A word before the time that takes it when read with the
    /// rest changes nothing. A record of 1.2.1, behind its mark, is read
    /// with its options and trims.
    #[test]
    fn incrby_records_of_1_2_0_are_read_as_1_2_0_read_them() {
        let store = replayed(&[
            vec!["TS.INCRBY", "a", "1", "LABELS", "x", "TIMESTAMP", "42"],
            vec!["TS.INCRBY", "a", "1", "TIMESTAMP", "50"],
            vec!["TS.INCRBY", "b", "1", "RETENTION", "TIMESTAMP", "42"],
            vec!["TS.INCRBY", "c", "1", "ENCODING", "TIMESTAMP", "42"],
            vec![
                "TS.INCRBY",
                "d",
                "1",
                "RETENTION",
                "100",
                "LABELS",
                "x",
                "y",
                "z",
                "TIMESTAMP",
                "42",
            ],
            vec!["TS.INCRBY", "e", "1", "DUPLICATE_POLICY", "TIMESTAMP", "42"],
            vec!["TS.INCRBY", "f", "1", "LABELS", "a", "b", "TIMESTAMP", "42"],
            // Read cleanly too, these were trimmed and given options.
            vec!["TS.CREATE", "r", "RETENTION", "10"],
            vec!["TS.INCRBY", "r", "1", "TIMESTAMP", "1"],
            vec!["TS.INCRBY", "r", "1", "RETENTION", "10", "TIMESTAMP", "100"],
            // 1.2.1's.
            vec![
                "TS.INCRBY",
                "m",
                "1",
                "timestamp",
                "5",
                "RETENTION",
                "10",
                "LABELS",
                "a",
                "b",
            ],
            vec!["TS.INCRBY", "m", "1", "timestamp", "100"],
        ]);
        let series = |key: &str| {
            let shard = crate::store::get_shard(&store[0], key);
            let map = shard.read();
            match map.get(key.as_bytes()) {
                Some(ValueRef::TimeSeries(inner)) => inner.read().clone(),
                _ => panic!("{key} is not a series"),
            }
        };
        let samples = |key: &str| {
            series(key)
                .samples
                .iter()
                .map(|(t, v)| (*t, *v))
                .collect::<Vec<_>>()
        };
        assert_eq!(samples("a"), [(42, 1.0), (50, 2.0)]);
        for key in ["a", "b", "c", "d", "e", "f"] {
            let s = series(key);
            assert!(s.labels.is_empty(), "{key}");
            assert_eq!(s.retention, 0, "{key}");
            assert_eq!(s.samples.get(&42), Some(&1.0), "{key}");
        }
        assert_eq!(samples("r"), [(1, 1.0), (100, 2.0)]);
        let m = series("m");
        assert_eq!(m.retention, 10);
        assert_eq!(m.labels.get("a").map(String::as_str), Some("b"));
        assert_eq!(samples("m"), [(100, 2.0)]);
    }

    /// `ON_DUPLICATE` for one sample, labels a `TS.ALTER` took away, and a
    /// `TS.INCRBY` as the feed writes it, with a word left over from
    /// `LABELS`: replayed as they ran.
    /// A time above `i64::MAX`, which 1.2.0 took: in its log as given
    /// (`TS.ADD`, `TS.MADD`), or, from `TS.INCRBY`, as the negative number
    /// it replied. Replayed, each sample at the time 1.2.0 kept (#592).
    #[test]
    fn times_above_i64_max_of_1_2_0_replay() {
        let max = u64::MAX.to_string();
        let below = (u64::MAX - 1).to_string();
        let store = replayed(&[
            vec!["TS.ADD", "h", below.as_str(), "1"],
            vec!["TS.MADD", "h", max.as_str(), "2"],
            vec!["TS.INCRBY", "i", "1", "TIMESTAMP", "-1"],
        ]);
        let samples = |key: &str| {
            let shard = crate::store::get_shard(&store[0], key);
            let map = shard.read();
            match map.get(key.as_bytes()) {
                Some(ValueRef::TimeSeries(inner)) => inner
                    .read()
                    .samples
                    .iter()
                    .map(|(t, v)| (*t, *v))
                    .collect::<Vec<_>>(),
                _ => panic!("{key} is not a series"),
            }
        };
        assert_eq!(samples("h"), [(u64::MAX - 1, 1.0), (u64::MAX, 2.0)]);
        assert_eq!(samples("i"), [(u64::MAX, 1.0)]);
    }

    /// A compaction's destination past its time (its `DEL` not yet in the
    /// log), deleted, made again and given its source again; and a source
    /// renamed. Replayed, the links are as the master had them (#593).
    #[test]
    fn compaction_links_replay_as_they_ran() {
        let store = replayed(&[
            vec!["TS.CREATE", "s"],
            vec!["TS.CREATE", "d"],
            vec!["TS.CREATERULE", "s", "d", "AGGREGATION", "sum", "10"],
            vec!["TS.ADD", "s", "1", "1"],
            vec!["PEXPIREAT", "d", "1"],
            vec!["TS.ADD", "s", "15", "1"],
            vec!["DEL", "d"],
            vec!["TS.CREATE", "d"],
            vec!["TS.CREATERULE", "s", "d", "AGGREGATION", "sum", "10"],
            vec!["TS.ADD", "s", "25", "1"],
            vec!["RENAME", "s", "s2"],
            vec!["TS.ADD", "s2", "35", "1"],
            // A destination renamed onto another of its source's.
            vec!["TS.CREATE", "w"],
            vec!["TS.CREATE", "a"],
            vec!["TS.CREATE", "b"],
            vec!["TS.CREATERULE", "w", "a", "AGGREGATION", "sum", "10"],
            vec!["TS.CREATERULE", "w", "b", "AGGREGATION", "sum", "10"],
            vec!["RENAME", "a", "b"],
            vec!["TS.ADD", "w", "1", "1"],
            vec!["TS.ADD", "w", "15", "1"],
        ]);
        let series = |key: &str| {
            let shard = crate::store::get_shard(&store[0], key);
            let map = shard.read();
            match map.get(key.as_bytes()) {
                Some(ValueRef::TimeSeries(inner)) => inner.read().clone(),
                _ => panic!("{key} is not a series"),
            }
        };
        let s = series("s2");
        assert_eq!(s.rules.len(), 1);
        assert_eq!(s.rules[0].dest_key, "d");
        let d = series("d");
        assert_eq!(d.source.as_deref(), Some("s2"));
        assert_eq!(
            d.samples.iter().map(|(t, v)| (*t, *v)).collect::<Vec<_>>(),
            [(10, 1.0), (20, 1.0)]
        );
        let w = series("w");
        assert_eq!(w.rules.len(), 1);
        assert_eq!(w.rules[0].dest_key, "b");
        assert_eq!(series("b").source.as_deref(), Some("w"));
        assert_eq!(series("b").samples.get(&0), Some(&1.0));
    }

    #[test]
    fn options_replay_as_they_ran() {
        let (store, index) = replayed_with_index(&[
            vec!["TS.CREATE", "k", "LABELS", "a", "b"],
            vec!["TS.ALTER", "k", "LABELS"],
            vec!["TS.CREATE", "d", "DUPLICATE_POLICY", "BLOCK"],
            vec!["TS.ADD", "d", "1", "1"],
            vec!["TS.ADD", "d", "1", "5", "ON_DUPLICATE", "SUM"],
            vec!["TS.INCRBY", "c", "2", "timestamp", "9", "LABELS", "x"],
        ]);
        let series = |key: &str| {
            let shard = crate::store::get_shard(&store[0], key);
            let map = shard.read();
            match map.get(key.as_bytes()) {
                Some(ValueRef::TimeSeries(inner)) => inner.read().clone(),
                _ => panic!("{key} is not a series"),
            }
        };
        assert!(series("k").labels.is_empty());
        let indexed = |entry: &str| {
            crate::read_lock!(index)
                .get(entry)
                .is_some_and(|keys| !keys.is_empty())
        };
        assert!(!indexed("a=b"));
        let d = series("d");
        assert_eq!(d.samples.get(&1), Some(&6.0));
        assert_eq!(d.duplicate_policy, crate::types::DuplicatePolicy::Block);
        let c = series("c");
        assert_eq!(c.samples.get(&9), Some(&2.0));
        assert!(c.labels.is_empty());
    }

    #[test]
    fn words_an_older_version_passed_over_are_passed_over_again() {
        let store = replayed(&[
            vec![
                "TS.CREATE",
                "k",
                "RETENTION",
                "5000",
                "FOO",
                "LABELS",
                "a",
                "b",
            ],
            vec!["TS.ALTER", "k", "CHUNK", "SIZE", "100"],
            vec!["TS.INCRBY", "c", "1", "BOGUS", "TIMESTAMP", "7"],
            vec!["TS.ADD", "k", "10", "1.5", "IGNORE", "5", "0.5"],
        ]);
        let series = |key: &str| {
            let shard = crate::store::get_shard(&store[0], key);
            let map = shard.read();
            match map.get(key.as_bytes()) {
                Some(ValueRef::TimeSeries(inner)) => inner.read().clone(),
                _ => panic!("{key} is not a series"),
            }
        };
        let k = series("k");
        assert_eq!(k.retention, 5000);
        assert_eq!(k.chunk_size, 100);
        assert_eq!(k.labels.get("a").map(String::as_str), Some("b"));
        assert_eq!(k.samples.get(&10), Some(&1.5));
        let c = series("c");
        assert_eq!(c.samples.get(&7), Some(&1.0));
    }
}

/// A deadline the log gave a key, or a hash field, and the writes made to
/// it after that while it lived: replayed once the deadline has passed.
#[cfg(test)]
mod past_deadline_replay_tests {
    use super::*;
    use crate::commands::helpers::{delete_key_if_expired, is_expired, now_ms};
    use crate::store::{Store, ValueRef};

    fn record(parts: &[&str]) -> Vec<u8> {
        let parts: Vec<Vec<u8>> = parts.iter().map(|p| p.as_bytes().to_vec()).collect();
        crate::resp::write::encode_resp_command(&parts)
    }

    /// `records`, written to an AOF and replayed into a new store.
    pub(super) fn replayed(records: &[Vec<&str>]) -> Store {
        replayed_with_index(records).0
    }

    /// [`replayed`], and the time series label index the replay built.
    pub(super) fn replayed_with_index(
        records: &[Vec<&str>],
    ) -> (Store, crate::store::SharedTsIndex) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("appendonly.aof");
        let log: Vec<u8> = records.iter().flat_map(|r| record(r)).collect();
        std::fs::write(&path, log).expect("write");
        let store = crate::store::new_store(1);
        let index_store = crate::vector::new_index_store();
        let dict_store = crate::vector::new_dict_store();
        let wait_table = std::sync::Arc::new(crate::wait_table::WaitTable::new());
        let ts_index = crate::store::new_ts_index();
        let config = std::sync::Arc::new(crate::config::Config::default());
        let (compaction_tx, _compaction_rx) = crate::vector::new_compaction_channel();
        let repl_state = std::sync::Arc::new(crate::replication::ReplState::new_master());
        let stats = replay_aof(
            &path.to_string_lossy(),
            &store,
            &index_store,
            &dict_store,
            &wait_table,
            &ts_index,
            &config,
            &compaction_tx,
            &repl_state,
            false,
        )
        .expect("replay");
        assert_eq!(stats.errors, 0, "every record applied");
        (store, ts_index)
    }

    fn expires(store: &Store, key: &str) -> Option<u64> {
        crate::store::get_shard(&store[0], key).read().expires(key)
    }

    fn hash_fields(store: &Store, key: &str) -> Vec<String> {
        let shard = crate::store::get_shard(&store[0], key);
        let map = shard.read();
        let mut fields: Vec<String> = match map.peek_expiring(key) {
            Some((ValueRef::Hash(h), _)) => h.read().keys().cloned().collect(),
            Some((ValueRef::HashEx(h), _)) => h.read().fields.keys().cloned().collect(),
            other => panic!(
                "{key} is not a hash: {:?}",
                other.map(|(v, _)| v.type_name())
            ),
        };
        fields.sort();
        fields
    }

    fn string(store: &Store, key: &str) -> Vec<u8> {
        let shard = crate::store::get_shard(&store[0], key);
        let map = shard.read();
        match map.peek_expiring(key) {
            Some((ValueRef::String(b), _)) => b.to_vec(),
            _ => panic!("{key} is not a string"),
        }
    }

    fn list_len(store: &Store, key: &str) -> usize {
        let shard = crate::store::get_shard(&store[0], key);
        let map = shard.read();
        match map.peek_expiring(key) {
            Some((ValueRef::List(l), _)) => l.read().len(),
            _ => panic!("{key} is not a list"),
        }
    }

    /// `HSET h f0 v0`, `PEXPIREAT h t`, `HSET h f1 v1`, `t` gone: the
    /// replay deleted `h` at its `PEXPIREAT`, and the `HSET` made it again
    /// of `f1` alone, with no time to live, for good. The same of a string
    /// and a list. Now each keeps its deadline and every write, and is past
    /// its time once the replay is over.
    #[test]
    fn a_deadline_that_has_passed_is_set_and_the_writes_after_it_go_into_the_key() {
        let past = (now_ms() - 5_000).to_string();
        let store = replayed(&[
            vec!["SELECT", "0"],
            vec!["HSET", "h", "f0", "v0"],
            vec!["PEXPIREAT", "h", &past],
            vec!["HSET", "h", "f1", "v1"],
            vec!["SET", "s", "v"],
            vec!["PEXPIREAT", "s", &past],
            vec!["APPEND", "s", "x"],
            vec!["RPUSH", "l", "a"],
            vec!["PEXPIREAT", "l", &past],
            vec!["RPUSH", "l", "b"],
        ]);
        let at: u64 = past.parse().unwrap();
        for key in ["h", "s", "l"] {
            assert_eq!(expires(&store, key), Some(at), "{key} kept its deadline");
            assert!(is_expired(&store, 0, key), "{key} is past it");
        }
        assert_eq!(hash_fields(&store, "h"), ["f0", "f1"]);
        assert_eq!(string(&store, "s"), b"vx");
        assert_eq!(list_len(&store, "l"), 2);
        // And it goes the way any key past its time goes.
        for key in ["h", "s", "l"] {
            assert!(delete_key_if_expired(&store, 0, key), "{key}");
            assert_eq!(expires(&store, key), None);
            assert!(
                crate::store::get_shard(&store[0], key)
                    .read()
                    .peek_expiring(key)
                    .is_none()
            );
        }
    }

    /// The same with a deadline still ahead: kept, with every write.
    #[test]
    fn a_deadline_still_ahead_is_kept_with_every_write() {
        let ahead = (now_ms() + 3_600_000).to_string();
        let store = replayed(&[
            vec!["HSET", "h", "f0", "v0"],
            vec!["PEXPIREAT", "h", &ahead],
            vec!["HSET", "h", "f1", "v1"],
            vec!["SET", "s", "v"],
            vec!["PEXPIREAT", "s", &ahead],
            vec!["APPEND", "s", "x"],
            vec!["RPUSH", "l", "a"],
            vec!["PEXPIREAT", "l", &ahead],
            vec!["RPUSH", "l", "b"],
        ]);
        let at: u64 = ahead.parse().unwrap();
        for key in ["h", "s", "l"] {
            assert_eq!(expires(&store, key), Some(at), "{key}");
            assert!(!is_expired(&store, 0, key), "{key}");
        }
        assert_eq!(hash_fields(&store, "h"), ["f0", "f1"]);
        assert_eq!(string(&store, "s"), b"vx");
        assert_eq!(list_len(&store, "l"), 2);
    }

    /// The field of a hash, its value and whether it has a deadline, past
    /// its time or not.
    fn field(store: &Store, key: &str, field: &str) -> (Option<Vec<u8>>, Option<bool>) {
        let shard = crate::store::get_shard(&store[0], key);
        let map = shard.read();
        match map.peek_expiring(key) {
            Some((ValueRef::HashEx(h), _)) => {
                let h = h.read();
                (
                    h.fields.get(field).map(|v| v.to_vec()),
                    h.expiries.get(field).map(|_| h.is_field_expired(field)),
                )
            }
            Some((ValueRef::Hash(h), _)) => (h.read().get(field).map(|v| v.to_vec()), None),
            _ => (None, None),
        }
    }

    /// `HPEXPIREAT h t FIELDS 1 g` and an `HINCRBY h g 1` made while `g`
    /// lived, `t` gone: the replay deleted `g` at the `HPEXPIREAT`, and the
    /// `HINCRBY` made it again, `1` with no deadline, for good. So with
    /// `HSETEX ... PXAT t` and `HGETEX ... PXAT t`. Now `g` keeps its deadline
    /// and the increment, and is past its time once the replay is over. An
    /// `HSET` after it takes the deadline away, as it did on the master.
    ///
    /// A field the master expired is in the log as its `HDEL`, before the
    /// write that found it gone: `z`, incremented after its deadline, is `1`
    /// with no deadline, as on the master.
    #[test]
    fn a_field_deadline_that_has_passed_is_set_and_the_writes_after_it_go_into_the_field() {
        let past = (now_ms() - 5_000).to_string();
        let store = replayed(&[
            vec!["HSET", "h", "keep", "1", "g", "5", "f", "v0"],
            vec!["HPEXPIREAT", "h", &past, "FIELDS", "2", "g", "f"],
            vec!["HINCRBY", "h", "g", "1"],
            vec!["HSET", "h", "f", "v1"],
            vec!["HSETEX", "x", "PXAT", &past, "FIELDS", "1", "n", "1.5"],
            vec!["HINCRBYFLOAT", "x", "n", "1"],
            vec!["HSET", "y", "keep", "1", "m", "7"],
            vec!["HGETEX", "y", "PXAT", &past, "FIELDS", "1", "m"],
            vec!["HINCRBY", "y", "m", "1"],
            vec!["HSET", "h", "z", "5"],
            vec!["HPEXPIREAT", "h", &past, "FIELDS", "1", "z"],
            vec!["HDEL", "h", "z"],
            vec!["HINCRBY", "h", "z", "1"],
        ]);
        assert_eq!(field(&store, "h", "z"), (Some(b"1".to_vec()), None));
        assert_eq!(field(&store, "h", "g"), (Some(b"6".to_vec()), Some(true)));
        assert_eq!(field(&store, "h", "f").0, Some(b"v1".to_vec()));
        assert_eq!(
            field(&store, "h", "f").1,
            None,
            "HSET took f's deadline away"
        );
        assert_eq!(field(&store, "h", "keep"), (Some(b"1".to_vec()), None));
        assert_eq!(field(&store, "x", "n"), (Some(b"2.5".to_vec()), Some(true)));
        assert_eq!(field(&store, "y", "m"), (Some(b"8".to_vec()), Some(true)));
        // Outside the replay the field is past its time, and a sweep of the
        // hash takes it.
        for (key, gone) in [("h", "g"), ("x", "n"), ("y", "m")] {
            let shard = crate::store::get_shard(&store[0], key);
            let map = shard.read();
            if let Some((ValueRef::HashEx(h), _)) = map.peek_expiring(key) {
                let mut h = h.write();
                assert_eq!(h.sweep_expired(), 1, "{key}");
                assert!(!h.fields.contains_key(gone), "{key}");
            } else {
                panic!("{key} is not a hash with field deadlines");
            }
        }
    }

    /// Outside a replay a field deadline that has passed still deletes the
    /// field on the spot.
    #[test]
    fn outside_a_replay_a_field_deadline_that_has_passed_deletes_the_field() {
        let mut inner = crate::types::HashInner::new();
        inner
            .fields
            .insert("g".to_string(), std::sync::Arc::from(&b"5"[..]));
        inner
            .expiries
            .insert("g".to_string(), crate::types::field_now() - 1);
        {
            let _replay = crate::expiry::replay();
            assert!(!inner.is_field_expired("g"));
            assert_eq!(inner.sweep_expired(), 0);
        }
        assert!(inner.is_field_expired("g"));
        assert_eq!(inner.sweep_expired(), 1);
    }

    /// A `DEL` in the log still deletes: what the master expired is said
    /// by it, and a write after it makes a new key, as on the master.
    #[test]
    fn a_del_after_the_deadline_still_deletes() {
        let past = (now_ms() - 5_000).to_string();
        let store = replayed(&[
            vec!["HSET", "h", "f0", "v0"],
            vec!["PEXPIREAT", "h", &past],
            vec!["UNLINK", "h"],
            vec!["HSET", "h", "f1", "v1"],
        ]);
        assert_eq!(expires(&store, "h"), None);
        assert_eq!(hash_fields(&store, "h"), ["f1"]);
    }
}

#[cfg(test)]
mod may_log_tests {
    use super::may_log;

    fn logs(cmd: &[&str]) -> bool {
        let cmd: Vec<&[u8]> = cmd.iter().map(|a| a.as_bytes()).collect();
        may_log(&cmd)
    }

    /// A reply waits for the AOF for a write by name, a script or a
    /// transaction, and a write by argument only with the arguments that
    /// write; never for a read.
    #[test]
    fn writes_are_told_by_name_or_by_their_arguments() {
        for write in [
            &["SET", "k", "v"][..],
            &["incr", "k"],
            &["eval", "return 1", "0"],
            &["EVALSHA", "x", "0"],
            &["fcall", "f", "0"],
            &["EXEC"],
            &["GEORADIUS", "k", "15", "37", "200", "km", "STORE", "d"],
            &["georadiusbymember", "k", "m", "200", "km", "storedist", "d"],
            &["MIGRATE", "h", "1", "k", "0", "100"],
            &["SCRIPT", "LOAD", "return 1"],
            &["FUNCTION", "FLUSH"],
        ] {
            assert!(logs(write), "{write:?}");
        }
        for read in [
            &["GET", "k"][..],
            &["PING"],
            &["GEORADIUS", "k", "15", "37", "200", "km"],
            &["GEORADIUSBYMEMBER", "k", "store", "200", "km"],
            &["FUNCTION", "LIST"],
            &["SCRIPT", "EXISTS", "x"],
            &["EVAL_RO", "return 1", "0"],
        ] {
            assert!(!logs(read), "{read:?}");
        }
    }
}
