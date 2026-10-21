// src/stats.rs
//
// Hot-path stat counters use thread-local accumulators to avoid cache-line contention on
// high-core-count machines (e.g. 48-core Graviton3).  Each Tokio worker thread keeps a local
// u32 bucket and only flushes to the shared AtomicU64 every FLUSH_BATCH increments.
// execute_command is synchronous (no await points), so a task never migrates threads mid-batch.
// The globals may lag by at most FLUSH_BATCH per worker thread — acceptable for monitoring.
use std::cell::Cell;
use std::collections::BTreeMap;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use parking_lot::Mutex;

pub static TOTAL_COMMANDS_PROCESSED: AtomicU64 = AtomicU64::new(0);
pub static TOTAL_CONNECTIONS_RECEIVED: AtomicU64 = AtomicU64::new(0);
pub static KEYSPACE_HITS: AtomicU64 = AtomicU64::new(0);
pub static KEYSPACE_MISSES: AtomicU64 = AtomicU64::new(0);
pub static EXPIRED_KEYS: AtomicU64 = AtomicU64::new(0);
/// Hash fields removed because their TTL ran out — `expired_subkeys`.
pub static EXPIRED_SUBKEYS: AtomicU64 = AtomicU64::new(0);
/// Of `expired_keys`, those the background cycle took (Redis 8.10's
/// `expired_keys_active`), not a read that found them past their time.
pub static EXPIRED_KEYS_ACTIVE: AtomicU64 = AtomicU64::new(0);
/// Keys deleted to get under `maxmemory` — `evicted_keys`.
pub static EVICTED_KEYS: AtomicU64 = AtomicU64::new(0);
pub static TOTAL_NET_INPUT_BYTES: AtomicU64 = AtomicU64::new(0);
pub static TOTAL_NET_OUTPUT_BYTES: AtomicU64 = AtomicU64::new(0);
pub static TOTAL_ERROR_REPLIES: AtomicU64 = AtomicU64::new(0);
pub static REJECTED_CONNECTIONS: AtomicU64 = AtomicU64::new(0);
/// Changes to the dataset since it was last saved, a counter to each thread
/// that has made one. See [`record_rdb_change`].
static RDB_CHANGES: parking_lot::Mutex<Vec<std::sync::Arc<AtomicU64>>> =
    parking_lot::Mutex::new(Vec::new());
/// Saves of the dataset completed since the server started — `rdb_saves` in
/// `INFO persistence`. `LASTSAVE` counts in seconds, so two saves within one
/// second look like one; this does not.
pub static RDB_SAVES: AtomicU64 = AtomicU64::new(0);
/// A `BGSAVE` is running.
pub static BGSAVE_IN_PROGRESS: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
/// Whether the last `BGSAVE` to finish succeeded.
pub static LAST_BGSAVE_OK: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);
pub static BLOCKED_CLIENTS: AtomicI64 = AtomicI64::new(0);

/// INFO `errorstat_<prefix>` counters — keyed by first Redis error token (`ERR`, `OOM`, …).
pub static ERROR_REPLY_BY_CATEGORY: LazyLock<Mutex<BTreeMap<String, u64>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));

#[inline]
pub fn error_category_from_message(msg: &str) -> String {
    let t = msg.trim();
    if t.is_empty() {
        return "ERR".into();
    }
    let first = t.split_whitespace().next().unwrap();
    if first.eq_ignore_ascii_case("ERR") {
        return "ERR".into();
    }
    first.to_string()
}

/// One client error reply: bumps [`TOTAL_ERROR_REPLIES`] and per-category INFO stats.
#[inline]
pub fn record_error_reply_stats(msg: &str) {
    // Direct atomic increment so `CONFIG RESETSTAT` does not fight thread-local error batches.
    TOTAL_ERROR_REPLIES.fetch_add(1, Ordering::Relaxed);
    let cat = error_category_from_message(msg);
    let mut g = ERROR_REPLY_BY_CATEGORY.lock();
    *g.entry(cat).or_insert(0) += 1;
}

#[inline]
pub fn reset_error_reply_stats() {
    ERROR_REPLY_BY_CATEGORY.lock().clear();
}

pub fn format_info_errorstats() -> String {
    let g = ERROR_REPLY_BY_CATEGORY.lock();
    let mut out = String::from("# Errorstats\r\n");
    for (k, n) in g.iter() {
        out.push_str(&format!("errorstat_{k}:count={n}\r\n"));
    }
    out.push_str("\r\n");
    out
}

/// Flush every N increments — reduces global atomic traffic by ~128×.
const FLUSH_BATCH: u32 = 128;

thread_local! {
    static TL_COMMANDS:    Cell<u32> = const { Cell::new(0) };
    static TL_HITS:        Cell<u32> = const { Cell::new(0) };
    static TL_MISSES:      Cell<u32> = const { Cell::new(0) };
    static TL_RDB_CHANGES: std::sync::Arc<AtomicU64> = {
        let mine = std::sync::Arc::new(AtomicU64::new(0));
        RDB_CHANGES.lock().push(std::sync::Arc::clone(&mine));
        mine
    };
}

macro_rules! tl_bump {
    ($tl:ident, $global:ident) => {{
        $tl.with(|c| {
            let n = c.get() + 1;
            if n >= FLUSH_BATCH {
                $global.fetch_add(n as u64, Ordering::Relaxed);
                c.set(0);
            } else {
                c.set(n);
            }
        });
    }};
}

/// Stable for this process lifetime — used in INFO `run_id`.
pub static RUN_ID: LazyLock<String> = LazyLock::new(|| {
    use std::time::{SystemTime, UNIX_EPOCH};
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    let pid = std::process::id();
    format!(
        "{:016x}{:016x}{:08x}",
        t as u64,
        pid as u64 ^ 0xdeadbeef,
        t ^ pid
    )
    .chars()
    .take(40)
    .collect()
});

// Snapshot pair for instantaneous_ops_per_sec — updated by a 1Hz background task
pub static INSTANTANEOUS_OPS: AtomicU64 = AtomicU64::new(0);
// Previous sample of TOTAL_COMMANDS_PROCESSED
pub static LAST_SAMPLE_COMMANDS: AtomicU64 = AtomicU64::new(0);

#[inline(always)]
pub fn record_command_processed() {
    tl_bump!(TL_COMMANDS, TOTAL_COMMANDS_PROCESSED);
}

#[inline(always)]
pub fn record_hit() {
    tl_bump!(TL_HITS, KEYSPACE_HITS);
}

#[inline(always)]
pub fn record_miss() {
    tl_bump!(TL_MISSES, KEYSPACE_MISSES);
}

#[inline]
pub fn record_expired_subkey() {
    EXPIRED_SUBKEYS.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn record_evicted() {
    EVICTED_KEYS.fetch_add(1, Ordering::Relaxed);
}

/// Counted straight into the total, not batched per thread as commands
/// are: keys expire far less often than commands run, and a batched count
/// could read 0 after the first few expiries (the Redis suite's pause test).
#[inline]
pub fn record_expired() {
    EXPIRED_KEYS.fetch_add(1, Ordering::Relaxed);
}

/// One more change to the dataset since it was last saved.
///
/// Counted where the thread that made it counts, and nowhere else: no other
/// thread writes there, so it costs a load and a store. It used to be kept
/// in a thread-local batch and added to one shared counter every second
/// time — so a change was not counted until the same thread made another,
/// and two changes made on two threads were not counted at all.
#[inline(always)]
pub fn record_rdb_change() {
    TL_RDB_CHANGES.with(|mine| mine.store(mine.load(Ordering::Relaxed) + 1, Ordering::Relaxed));
}

/// `rdb_changes_since_last_save`.
pub fn rdb_changes_since_save() -> u64 {
    RDB_CHANGES
        .lock()
        .iter()
        .map(|counter| counter.load(Ordering::Relaxed))
        .sum()
}

/// The dataset has been saved. (A change a thread is counting at this very
/// moment may put its counter back where it was: the count is then a save
/// too high until the next one.)
pub fn reset_rdb_changes() {
    for counter in RDB_CHANGES.lock().iter() {
        counter.store(0, Ordering::Relaxed);
    }
}

/// 1 Hz sampling for `instantaneous_ops_per_sec` in INFO — safe from any startup path.
pub fn spawn_instantaneous_ops_sampler() {
    std::thread::spawn(|| {
        loop {
            std::thread::sleep(std::time::Duration::from_secs(1));
            let current = TOTAL_COMMANDS_PROCESSED.load(Ordering::Relaxed);
            let last = LAST_SAMPLE_COMMANDS.swap(current, Ordering::Relaxed);
            INSTANTANEOUS_OPS.store(current.saturating_sub(last), Ordering::Relaxed);
        }
    });
}
