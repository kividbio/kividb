// src/stats.rs
//
// The counters of INFO stats. Those counted on every command (commands,
// keyspace hits and misses, reads and writes) are kept per thread, where the
// thread that counts writes and no other thread does, and added up when INFO
// reads them (see `PerThread`): no cache line is shared between threads on
// the hot path.
use std::collections::BTreeMap;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use parking_lot::Mutex;

/// A count kept where each thread counts, and added up when it is read.
///
/// No other thread writes a thread's counter, so counting costs a load and a
/// store, with no shared cache line. These used to be kept in a thread-local
/// batch and added to a shared total every 128th time: a server that had run
/// a few commands reported none (#136).
pub struct PerThread {
    threads: parking_lot::Mutex<Vec<std::sync::Arc<Slot>>>,
    /// The total at the last `CONFIG RESETSTAT`.
    base: AtomicU64,
}

/// One thread's count, alone on its cache line: two threads' counts side by
/// side would have their writes contend as one shared counter's do.
#[repr(align(64))]
#[derive(Default)]
pub struct Slot(AtomicU64);

impl PerThread {
    pub const fn new() -> Self {
        PerThread {
            threads: parking_lot::const_mutex(Vec::new()),
            base: AtomicU64::new(0),
        }
    }

    /// A counter for the calling thread. It stays counted after the thread
    /// ends.
    fn register(&self) -> std::sync::Arc<Slot> {
        let mine = std::sync::Arc::new(Slot::default());
        self.threads.lock().push(std::sync::Arc::clone(&mine));
        mine
    }

    fn sum(&self) -> u64 {
        self.threads
            .lock()
            .iter()
            .map(|counter| counter.0.load(Ordering::Relaxed))
            .sum()
    }

    /// The count since the server started, or since `CONFIG RESETSTAT`.
    pub fn total(&self) -> u64 {
        self.sum().saturating_sub(self.base.load(Ordering::Relaxed))
    }

    /// `CONFIG RESETSTAT`: count from 0 again.
    pub fn reset(&self) {
        self.base.store(self.sum(), Ordering::Relaxed);
    }
}

impl Default for PerThread {
    fn default() -> Self {
        Self::new()
    }
}

pub static TOTAL_COMMANDS_PROCESSED: PerThread = PerThread::new();
pub static TOTAL_CONNECTIONS_RECEIVED: AtomicU64 = AtomicU64::new(0);
pub static KEYSPACE_HITS: PerThread = PerThread::new();
pub static KEYSPACE_MISSES: PerThread = PerThread::new();
/// Reads from client connections — `total_reads_processed`.
pub static TOTAL_READS_PROCESSED: PerThread = PerThread::new();
/// Writes to client connections — `total_writes_processed`.
pub static TOTAL_WRITES_PROCESSED: PerThread = PerThread::new();
pub static EXPIRED_KEYS: AtomicU64 = AtomicU64::new(0);
/// Hash fields removed because their TTL ran out — `expired_subkeys`.
pub static EXPIRED_SUBKEYS: AtomicU64 = AtomicU64::new(0);
/// Of `expired_keys`, those the background cycle took (Redis 8.10's
/// `expired_keys_active`), not a read that found them past their time.
pub static EXPIRED_KEYS_ACTIVE: AtomicU64 = AtomicU64::new(0);
/// Scripts sent only with `EVAL` that the script cache let go to stay
/// within its limit — `evicted_scripts` (#155).
pub static EVICTED_SCRIPTS: AtomicU64 = AtomicU64::new(0);
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
/// When the dataset was last written to disk whole, in Unix seconds:
/// `LASTSAVE` and `rdb_last_save_time` (#149). A `SAVE` or `BGSAVE`, the
/// save at shutdown, a replica's after a full sync, and a checkpoint of the
/// AOF (`BGREWRITEAOF`, an automatic rewrite, `CONFIG SET appendonly yes`),
/// which writes `dump.kdb` too. Until the first, the server's start, as in
/// Redis.
static LAST_SAVE: AtomicI64 = AtomicI64::new(0);

/// How long the last `BGSAVE` took, in seconds; -1 before the first —
/// `rdb_last_bgsave_time_sec`.
pub static LAST_BGSAVE_SECS: AtomicI64 = AtomicI64::new(-1);

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_secs() as i64)
}

/// The dataset has been written to disk whole, or the server has started.
pub fn record_save() {
    LAST_SAVE.store(unix_now(), Ordering::Relaxed);
}

/// See [`LAST_SAVE`].
pub fn last_save() -> i64 {
    LAST_SAVE.load(Ordering::Relaxed)
}

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

thread_local! {
    static TL_COMMANDS: std::sync::Arc<Slot> = TOTAL_COMMANDS_PROCESSED.register();
    static TL_HITS: std::sync::Arc<Slot> = KEYSPACE_HITS.register();
    static TL_MISSES: std::sync::Arc<Slot> = KEYSPACE_MISSES.register();
    static TL_READS: std::sync::Arc<Slot> = TOTAL_READS_PROCESSED.register();
    static TL_WRITES: std::sync::Arc<Slot> = TOTAL_WRITES_PROCESSED.register();
    static TL_RDB_CHANGES: std::sync::Arc<AtomicU64> = {
        let mine = std::sync::Arc::new(AtomicU64::new(0));
        RDB_CHANGES.lock().push(std::sync::Arc::clone(&mine));
        mine
    };
}

macro_rules! tl_bump {
    ($tl:ident) => {{
        $tl.with(|mine| {
            mine.0
                .store(mine.0.load(Ordering::Relaxed) + 1, Ordering::Relaxed)
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

/// Bytes read from and written to clients in the last second:
/// `instantaneous_input_kbps` and `instantaneous_output_kbps` (#158).
pub static INSTANTANEOUS_INPUT_BPS: AtomicU64 = AtomicU64::new(0);
pub static INSTANTANEOUS_OUTPUT_BPS: AtomicU64 = AtomicU64::new(0);

/// The server runs its connections on io_uring.
pub static IO_URING: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// `multiplexing_api`: what the server waits on its connections with. It
/// said `epoll` whatever it was.
pub fn multiplexing_api() -> &'static str {
    if IO_URING.load(Ordering::Relaxed) {
        "io_uring"
    } else if cfg!(target_os = "linux") {
        "epoll"
    } else if cfg!(target_os = "windows") {
        "iocp"
    } else {
        "kqueue"
    }
}

// Snapshot pair for instantaneous_ops_per_sec — updated by a 1Hz background task
pub static INSTANTANEOUS_OPS: AtomicU64 = AtomicU64::new(0);
// Previous sample of TOTAL_COMMANDS_PROCESSED
pub static LAST_SAMPLE_COMMANDS: AtomicU64 = AtomicU64::new(0);

#[inline(always)]
pub fn record_command_processed() {
    tl_bump!(TL_COMMANDS);
}

#[inline(always)]
pub fn record_hit() {
    tl_bump!(TL_HITS);
}

#[inline(always)]
pub fn record_miss() {
    tl_bump!(TL_MISSES);
}

/// One read from a client's connection.
#[inline(always)]
pub fn record_read() {
    tl_bump!(TL_READS);
}

/// One write to a client's connection.
#[inline(always)]
pub fn record_write() {
    tl_bump!(TL_WRITES);
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

/// 1 Hz sampling for `instantaneous_ops_per_sec` in INFO, and `used_memory`'s
/// peak every 100 ms — safe from any startup path.
pub fn spawn_instantaneous_ops_sampler() {
    std::thread::spawn(|| {
        let (mut last_in, mut last_out) = (0u64, 0u64);
        let mut tick = 0u64;
        loop {
            tick += 1;
            std::thread::sleep(std::time::Duration::from_millis(100));
            crate::commands::memory::used_and_peak();
            if !tick.is_multiple_of(10) {
                continue;
            }
            let current = TOTAL_COMMANDS_PROCESSED.total();
            let last = LAST_SAMPLE_COMMANDS.swap(current, Ordering::Relaxed);
            INSTANTANEOUS_OPS.store(current.saturating_sub(last), Ordering::Relaxed);
            let bytes_in = TOTAL_NET_INPUT_BYTES.load(Ordering::Relaxed);
            let bytes_out = TOTAL_NET_OUTPUT_BYTES.load(Ordering::Relaxed);
            INSTANTANEOUS_INPUT_BPS.store(bytes_in.saturating_sub(last_in), Ordering::Relaxed);
            INSTANTANEOUS_OUTPUT_BPS.store(bytes_out.saturating_sub(last_out), Ordering::Relaxed);
            (last_in, last_out) = (bytes_in, bytes_out);
        }
    });
}
