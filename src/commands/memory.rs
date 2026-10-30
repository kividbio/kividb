use crate::commands::ctx::Ctx;
use crate::commands::helpers::parse_utf8;
use crate::read_lock;
use crate::resp::write::*;
use crate::store;
use crate::store::ValueRef;

pub fn handle(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    if name != "MEMORY" {
        return None;
    }
    let cmd = &ctx.cmd;
    if cmd.len() < 2 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'memory' command",
        ));
    }
    let sub = String::from_utf8_lossy(cmd[1]).to_ascii_uppercase();
    match sub.as_str() {
        "USAGE" => memory_usage(ctx),
        "STATS" => Some(memory_stats()),
        "DOCTOR" => Some(memory_doctor()),
        "PURGE" => Some(memory_purge()),
        "MALLOC-STATS" => Some(memory_malloc_stats()),
        "HELP" => Some(resp_array(
            [
                "MEMORY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "DOCTOR",
                "    Return memory problems reports.",
                "MALLOC-STATS",
                "    Return internal statistics report from the memory allocator.",
                "PURGE",
                "    Attempt to purge dirty pages for reclamation by the allocator.",
                "STATS",
                "    Return information about the memory usage of the server.",
                "USAGE <key> [SAMPLES <count>]",
                "    Return memory in bytes used by <key> and its value. Nested values are",
                "    sampled up to <count> times (default: 5, 0 means sample all).",
                "HELP",
                "    Print this help.",
            ]
            .iter()
            .map(|line| resp_simple(line))
            .collect(),
        )),
        _ => Some(resp_error(&format!(
            "ERR unknown subcommand '{}'. Try MEMORY HELP.",
            String::from_utf8_lossy(ctx.cmd.get(1).copied().unwrap_or_default())
        ))),
    }
}

/// The size the allocator gives a request of `n` bytes, in jemalloc's
/// classes (Redis's): 8, then steps of 16 to 128, then four to each
/// doubling.
fn size_class(n: usize) -> usize {
    if n <= 8 {
        return 8;
    }
    if n <= 128 {
        return n.div_ceil(16) * 16;
    }
    let step = 1usize << ((usize::BITS - (n - 1).leading_zeros() - 1) - 2);
    n.div_ceil(step) * step
}

fn memory_usage(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() < 3 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'memory|usage' command",
        ));
    }
    let key = match parse_utf8(cmd[2]) {
        Some(k) => k,
        None => return Some(resp_error("ERR key must be valid UTF-8")),
    };

    // `SAMPLES count`, as Redis reads it: how many elements of a collection
    // are looked at, 5 by default; 0 is all of them. It divided by zero for
    // a hash, set or sorted set, and the worker panicked (#151).
    let samples = match &cmd[3..] {
        [] => 5,
        [opt, count] if opt.eq_ignore_ascii_case(b"SAMPLES") => {
            match std::str::from_utf8(count)
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
            {
                None => return Some(resp_error("ERR value is not an integer or out of range")),
                Some(n) if n < 0 => return Some(resp_error("ERR syntax error")),
                Some(0) => usize::MAX,
                Some(n) => n as usize,
            }
        }
        _ => return Some(resp_error("ERR syntax error")),
    };

    let db = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db, key);
    let map = read_lock!(shard);

    let value = match map.get(key) {
        Some(v) => v,
        None => return Some(resp_null_bulk()),
    };

    // What the key's entry takes: forty bytes of a table that is, on
    // average, two thirds full. The key's own bytes are inside the entry
    // unless there are more than twenty-four of them (`store::Key`).
    let key_overhead = 64 + if key.len() > 24 { key.len() } else { 0 };

    let bytes: usize = match value {
        // As Redis 8 counts a string: one object holding its header, the
        // key and the value, as the allocator rounds it. Redis's suite
        // holds it to that (at least the three, at most twice them or 64),
        // which a share of the table's entries does not fit.
        ValueRef::String(b) => size_class(16 + key.len() + 2 + b.len() + 2),

        ValueRef::List(arc) => {
            let inner = arc.read();
            let len = inner.len();
            let avg = if len == 0 || samples == 0 {
                0
            } else {
                let sample_n = samples.min(len);
                let step = (len / sample_n).max(1);
                let total: usize = inner
                    .iter()
                    .step_by(step)
                    .take(sample_n)
                    .map(|v| v.len())
                    .sum();
                total / sample_n
            };
            key_overhead + avg * len + 80
        }

        ValueRef::Hash(arc) => {
            let inner = arc.read();
            let len = inner.len();
            let (avg_k, avg_v) = if len == 0 {
                (0, 0)
            } else {
                let sample_n = samples.min(len);
                let step = (len / sample_n).max(1);
                let (tk, tv): (usize, usize) = inner
                    .iter()
                    .step_by(step)
                    .take(sample_n)
                    .map(|(k, v)| (k.len(), v.len()))
                    .fold((0, 0), |(ak, av), (k, v)| (ak + k, av + v));
                (tk / sample_n.max(1), tv / sample_n.max(1))
            };
            key_overhead + (avg_k + avg_v) * len + 120
        }

        ValueRef::Set(arc) => {
            let inner = arc.read();
            let len = inner.len();
            let avg = if len == 0 {
                0
            } else {
                let sample_n = samples.min(len);
                let step = (len / sample_n).max(1);
                let total: usize = inner
                    .iter()
                    .step_by(step)
                    .take(sample_n)
                    .map(|m| m.len())
                    .sum();
                total / sample_n.max(1)
            };
            key_overhead + avg * len + 80
        }

        ValueRef::SortedSet(arc) => {
            let inner = arc.read();
            let len = inner.members.len();
            let avg = if len == 0 {
                0
            } else {
                let sample_n = samples.min(len);
                let step = (len / sample_n).max(1);
                let total: usize = inner
                    .members
                    .keys()
                    .step_by(step)
                    .take(sample_n)
                    .map(|m| m.len())
                    .sum();
                total / sample_n.max(1)
            };
            // dual-indexed: BTreeMap + AHashMap, so *2
            key_overhead + (avg + 8) * len * 2 + 120
        }

        ValueRef::Stream(arc) => {
            let inner = arc.read();
            let len = inner.entries.len();
            let avg = if len == 0 {
                0
            } else {
                let sample_n = samples.min(len);
                let step = (len / sample_n).max(1);
                let total: usize = inner
                    .entries
                    .values()
                    .step_by(step)
                    .take(sample_n)
                    .map(|e| {
                        e.fields
                            .iter()
                            .map(|(k, v)| k.len() + v.len())
                            .sum::<usize>()
                    })
                    .sum();
                total / sample_n.max(1)
            };
            key_overhead + avg * len + 200
        }

        ValueRef::Json(arc) => {
            let s = arc.read().to_string();
            key_overhead + s.len() + 64
        }

        ValueRef::TimeSeries(arc) => {
            let inner = arc.read();
            // Each sample: 8 bytes timestamp + 8 bytes f64
            key_overhead + inner.samples.len() * 16 + 200
        }

        ValueRef::VectorSet(arc) => {
            let inner = arc.read();
            // Each entry: dim * 4 bytes (f32) + attrs string
            let count = inner.entries.len();
            let dim = inner.dim;
            key_overhead + count * dim * 4 + 200
        }

        ValueRef::Array(arc) => {
            let inner = arc.read();
            let len = inner.entries.len();
            // Sample values the same way the other collection types do —
            // arrays are sparse, so cost tracks stored elements, not `ARLEN`.
            let avg = if len == 0 || samples == 0 {
                0
            } else {
                let sample_n = samples.min(len);
                let step = (len / sample_n).max(1);
                let total: usize = inner
                    .entries
                    .values()
                    .step_by(step)
                    .take(sample_n)
                    .map(|v| v.len())
                    .sum();
                total / sample_n.max(1)
            };
            // Per entry: 8-byte index key + value bytes + BTreeMap node share.
            key_overhead + len * (8 + avg + 32) + 100
        }
        ValueRef::HashEx(arc) => {
            let inner = arc.read();
            let len = inner.fields.len();
            let (avg_k, avg_v) = if len == 0 {
                (0, 0)
            } else {
                let sample_n = samples.min(len);
                let step = (len / sample_n).max(1);
                let (tk, tv): (usize, usize) = inner
                    .fields
                    .iter()
                    .step_by(step)
                    .take(sample_n)
                    .map(|(k, v)| (k.len(), v.len()))
                    .fold((0, 0), |(ak, av), (k, v)| (ak + k, av + v));
                (tk / sample_n.max(1), tv / sample_n.max(1))
            };
            // Each expiry entry: ~32 bytes (String + Instant on 64-bit).
            // We add the expiries map size on top of the regular hash cost.
            let expiry_overhead = inner.expiries.len() * 32;
            key_overhead + (avg_k + avg_v) * len + 120 + expiry_overhead
        }
    };

    Some(resp_integer(bytes as i64))
}

fn memory_stats() -> Vec<u8> {
    let (rss, _, vsize) = read_proc_status();
    // Allocated, as in `INFO memory`: see `memory_use`.
    let (used, peak) = used_and_peak();

    let rss_human = human_bytes(rss);
    let peak_human = human_bytes(peak);
    let vsize_human = human_bytes(vsize);

    let (frag_ratio, frag_bytes) = fragmentation(rss, used);

    // Flat key-value array matching Redis field names
    let mut pairs: Vec<Vec<u8>> = Vec::new();
    macro_rules! kv {
        ($k:expr, $v:expr) => {
            pairs.push(resp_bulk_str($k));
            pairs.push(resp_bulk_str(&$v.to_string()));
        };
    }

    kv!("used_memory", used);
    kv!("used_memory_human", human_bytes(used));
    kv!("used_memory_rss", rss);
    kv!("used_memory_rss_human", rss_human);
    kv!("used_memory_peak", peak);
    kv!("used_memory_peak_human", peak_human);
    kv!("used_memory_vsize", vsize);
    kv!("used_memory_vsize_human", vsize_human);
    kv!(
        "used_memory_peak_perc",
        format!(
            "{:.2}%",
            if peak > 0 {
                used as f64 / peak as f64 * 100.0
            } else {
                100.0
            }
        )
    );
    kv!("mem_fragmentation_ratio", format!("{:.4}", frag_ratio));
    kv!("mem_fragmentation_bytes", frag_bytes);
    kv!("allocator_frag_ratio", "1.0");
    kv!("allocator_frag_bytes", "0");
    kv!("allocator_rss_ratio", "1.0");
    kv!("allocator_rss_bytes", "0");
    kv!("rss_overhead_ratio", "1.0");
    kv!("rss_overhead_bytes", "0");
    kv!(
        "active_defrag_running",
        u8::from(crate::compaction::running()).to_string()
    );
    kv!(
        "lazyfree_pending_objects",
        crate::lazyfree::pending_objects().to_string()
    );
    kv!(
        "lazyfreed_objects",
        crate::lazyfree::freed_objects().to_string()
    );

    resp_array(pairs)
}

/// High-water mark of observed RSS.
///
/// Linux's `VmPeak` is peak *virtual* address space, not peak resident memory.
/// Reporting it as `used_memory_peak` is wildly misleading — tokio's thread
/// stacks and io_uring's reservations alone put an idle, empty server at
/// `used_memory_peak:1.4G` — and it makes `used_memory_peak_perc` meaningless.
/// Tracking the largest RSS we have actually sampled is the honest analogue of
/// what Redis reports.
static PEAK_RSS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Record `rss` and return the high-water mark including it.
fn observe_peak_rss(rss: usize) -> usize {
    use std::sync::atomic::Ordering;
    PEAK_RSS.fetch_max(rss, Ordering::Relaxed).max(rss)
}

pub(crate) fn read_proc_status() -> (usize, usize, usize) {
    #[cfg(target_os = "linux")]
    {
        if let Ok(content) = std::fs::read_to_string("/proc/self/status") {
            let mut rss = 0usize;
            let mut vsize = 0usize;
            for line in content.lines() {
                if let Some(rest) = line.strip_prefix("VmRSS:") {
                    rss = rest
                        .split_whitespace()
                        .next()
                        .and_then(|s| s.parse::<usize>().ok())
                        .unwrap_or(0)
                        * 1024;
                } else if let Some(rest) = line.strip_prefix("VmSize:") {
                    vsize = rest
                        .split_whitespace()
                        .next()
                        .and_then(|s| s.parse::<usize>().ok())
                        .unwrap_or(0)
                        * 1024;
                }
            }
            return (rss, observe_peak_rss(rss), vsize);
        }
        (0, 0, 0)
    }
    #[cfg(target_os = "macos")]
    {
        // ps -o rss= -p <pid> returns RSS in kilobytes
        let pid = std::process::id();
        let rss = std::process::Command::new("ps")
            .args(["-o", "rss=", "-p", &pid.to_string()])
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .and_then(|s| s.trim().parse::<usize>().ok())
            .unwrap_or(0)
            * 1024; // ps reports KB

        let vsize = std::process::Command::new("ps")
            .args(["-o", "vsz=", "-p", &pid.to_string()])
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .and_then(|s| s.trim().parse::<usize>().ok())
            .unwrap_or(0)
            * 1024;

        // macOS exposes no peak-RSS counter, so the tracked high-water mark is
        // all there is.
        (rss, observe_peak_rss(rss), vsize)
    }
    // Windows has no /proc and no `ps`, so this used to fall through to the
    // catch-all below and report zero. That is not a cosmetic gap: `INFO
    // memory` reported `used_memory:0`, and — worse — `check_memory_limit`
    // compares `used >= limit`, so `maxmemory` was silently unenforced. A dev
    // box would accept `CONFIG SET maxmemory 1` and happily keep writing.
    //
    // `K32GetProcessMemoryInfo` lives in kernel32 on Windows 7+ (the
    // K32-prefixed form specifically, so no psapi.dll import library is
    // needed), which keeps this to a bare `extern` block rather than a new
    // dependency for one call.
    #[cfg(target_os = "windows")]
    {
        #[repr(C)]
        #[derive(Default)]
        struct ProcessMemoryCounters {
            cb: u32,
            page_fault_count: u32,
            peak_working_set_size: usize,
            working_set_size: usize,
            quota_peak_paged_pool_usage: usize,
            quota_paged_pool_usage: usize,
            quota_peak_non_paged_pool_usage: usize,
            quota_non_paged_pool_usage: usize,
            pagefile_usage: usize,
            peak_pagefile_usage: usize,
        }

        unsafe extern "system" {
            fn GetCurrentProcess() -> isize;
            fn K32GetProcessMemoryInfo(
                process: isize,
                counters: *mut ProcessMemoryCounters,
                cb: u32,
            ) -> i32;
        }

        let mut pmc = ProcessMemoryCounters {
            cb: std::mem::size_of::<ProcessMemoryCounters>() as u32,
            ..Default::default()
        };
        // SAFETY: `pmc` is a live, correctly-sized, repr(C) allocation and `cb`
        // describes it; the pseudo-handle from GetCurrentProcess needs no
        // closing. A non-zero return means the struct was filled.
        let ok = unsafe {
            K32GetProcessMemoryInfo(
                GetCurrentProcess(),
                &mut pmc,
                std::mem::size_of::<ProcessMemoryCounters>() as u32,
            )
        };
        // Working set is the closest analogue to VmRSS, and peak working set is
        // a true peak-resident counter (unlike Linux's VmPeak), so it is used
        // directly. `PagefileUsage` (private commit) stands in for VmSize, but
        // it is NOT comparable the way Linux's is — see `fragmentation`, which
        // declines to derive a ratio from it.
        if ok == 0 {
            (0, 0, 0)
        } else {
            (
                pmc.working_set_size,
                observe_peak_rss(pmc.working_set_size).max(pmc.peak_working_set_size),
                pmc.pagefile_usage,
            )
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    {
        (0, 0, 0)
    }
}

fn human_bytes(bytes: usize) -> String {
    if bytes >= 1 << 30 {
        format!("{:.2}G", bytes as f64 / (1u64 << 30) as f64)
    } else if bytes >= 1 << 20 {
        format!("{:.2}M", bytes as f64 / (1u64 << 20) as f64)
    } else if bytes >= 1 << 10 {
        format!("{:.2}K", bytes as f64 / (1u64 << 10) as f64)
    } else {
        format!("{}B", bytes)
    }
}

pub fn read_proc_status_pub() -> (usize, usize, usize) {
    read_proc_status()
}

/// Redis's `(mem_fragmentation_ratio, mem_fragmentation_bytes)`: what the
/// operating system holds for the process (`rss`) over what it has allocated
/// (`used`, `used_memory`), and the difference (#152). Above 1, memory freed
/// is held in pages that something still uses, or by the allocator.
///
/// It was the virtual size over the RSS: 176 on an empty server and 56 with
/// 100,000 keys, where RSS over `used_memory` was about 1.8, so that
/// `MEMORY DOCTOR` always warned and alerts above 1.5 always fired.
pub(crate) fn fragmentation(rss: usize, used: usize) -> (f64, i64) {
    let ratio = if used > 0 {
        rss as f64 / used as f64
    } else {
        1.0
    };
    (ratio, rss as i64 - used as i64)
}

/// The most `used_memory` has been: Redis's `used_memory_peak` (#152). It
/// was the most RSS seen, and only when `INFO` or `MEMORY` ran.
static PEAK_USED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// `used_memory` now, and its peak including now. Sampled every 100 ms (see
/// `stats::spawn_instantaneous_ops_sampler`) and whenever it is read, as
/// Redis samples it in its cron and its commands.
pub(crate) fn used_and_peak() -> (usize, usize) {
    use std::sync::atomic::Ordering;
    let used = crate::memory_use::allocated();
    (used, PEAK_USED.fetch_max(used, Ordering::Relaxed).max(used))
}

/// `used_memory` once the server has started, before any data:
/// `used_memory_startup`.
static STARTUP_USED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// The server has started: what it has allocated is its own, not data's.
pub(crate) fn note_startup() {
    let _ = STARTUP_USED.compare_exchange(
        0,
        crate::memory_use::allocated(),
        std::sync::atomic::Ordering::Relaxed,
        std::sync::atomic::Ordering::Relaxed,
    );
}

/// See [`STARTUP_USED`].
pub(crate) fn startup_used() -> usize {
    STARTUP_USED.load(std::sync::atomic::Ordering::Relaxed)
}

pub fn human_bytes_pub(bytes: usize) -> String {
    human_bytes(bytes)
}

/// As Redis's: nothing to say about an almost empty instance, whose RSS is
/// mostly the program's own; fragmentation above 1.4 that is more than
/// 10 MB; a peak half as large again as now.
fn memory_doctor() -> Vec<u8> {
    let (rss, _, _) = read_proc_status();
    let (used, peak) = used_and_peak();
    let (frag_ratio, frag_bytes) = fragmentation(rss, used);

    let msg = if used < 5 << 20 {
        "Hi Sam, this instance is empty or is using very little memory, my issues detector \
         can't be used in these conditions."
    } else if frag_ratio > 1.4 && frag_bytes > 10 << 20 && crate::compaction::enabled() {
        // Active defrag is on (the default since 1.2): it told the reader to
        // turn it on (#173).
        "High fragmentation: RSS fragmentation ratio above 1.4 (the process holds over 40% more \
         than it has allocated). Active defrag is on and works through it as the server runs; \
         MEMORY PURGE gives freed memory back to the system at once."
    } else if frag_ratio > 1.4 && frag_bytes > 10 << 20 {
        "High fragmentation: RSS fragmentation ratio above 1.4 (the process holds over 40% more \
         than it has allocated). Consider MEMORY PURGE, or turn on activedefrag."
    } else if peak > used / 2 * 3 {
        "Peak memory: in the past this instance used more than 150% of the memory it is using \
         now. The allocator keeps some of it, which shows as fragmentation."
    } else {
        "Sam, I love your taste in T-shirts, but your memory is just fine."
    };
    resp_bulk_str(msg)
}

/// Hand free pages back to the OS.
///
/// Freeing a Rust allocation only returns it to the allocator. glibc keeps the
/// pages mapped in its arena free lists until asked, so without this a flush
/// shows up nowhere in `used_memory` (which is RSS). Used by `MEMORY PURGE` and
/// by flushes big enough to be worth the walk over every arena.
pub(crate) fn purge_allocator() {
    #[cfg(target_os = "linux")]
    unsafe {
        libc::malloc_trim(0);
    }
}

fn memory_purge() -> Vec<u8> {
    purge_allocator();
    resp_simple("OK")
}

fn memory_malloc_stats() -> Vec<u8> {
    // What Redis answers when built without jemalloc, as KiviDB is: it was
    // a fixed piece of XML that looked like statistics (#152).
    resp_bulk_str("Stats not supported for the current allocator")
}

#[cfg(test)]
mod tests {
    use super::size_class;

    #[test]
    fn size_classes_are_jemallocs() {
        let classes: Vec<usize> = [1, 8, 9, 17, 128, 129, 160, 161, 256, 257, 1000, 4097]
            .iter()
            .map(|&n| size_class(n))
            .collect();
        assert_eq!(
            classes,
            [8, 8, 16, 32, 128, 160, 160, 192, 256, 320, 1024, 5120]
        );
    }
}
