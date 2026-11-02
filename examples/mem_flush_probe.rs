//! Isolates where memory goes after a FLUSHDB-style `clear()` on the real store type.
//!
//! Run: `cargo run --example mem_flush_probe [num_keys] [key_len] [val_len]`
//!
//! Prints kernel RSS plus the summed hash-table capacity of every shard, so the
//! retained bytes can be split into "still allocated by us" (bucket arrays that
//! `clear()` keeps) versus "freed to the allocator but not returned to the OS".

use kividb::store::{self, Store};
use kividb::types::KiviDBValue;

/// Same quantity `INFO memory` reports as `used_memory`: process resident set.
#[cfg(target_os = "linux")]
fn rss() -> usize {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find_map(|l| l.strip_prefix("VmRSS:"))
                .and_then(|r| r.split_whitespace().next()?.parse::<usize>().ok())
        })
        .map(|kb| kb * 1024)
        .unwrap_or(0)
}

#[cfg(target_os = "windows")]
fn rss() -> usize {
    #[repr(C)]
    #[derive(Default)]
    struct Pmc {
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
        fn K32GetProcessMemoryInfo(p: isize, c: *mut Pmc, cb: u32) -> i32;
    }
    let mut pmc = Pmc {
        cb: size_of::<Pmc>() as u32,
        ..Default::default()
    };
    let ok =
        unsafe { K32GetProcessMemoryInfo(GetCurrentProcess(), &mut pmc, size_of::<Pmc>() as u32) };
    if ok == 0 { 0 } else { pmc.working_set_size }
}

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
fn rss() -> usize {
    0
}

fn mb(bytes: usize) -> String {
    format!("{:>8.1} MB", bytes as f64 / (1024.0 * 1024.0))
}

/// (live entries, allocated bucket slots) across every shard of every db.
fn table_stats(store: &Store) -> (usize, usize) {
    let mut len = 0;
    let mut cap = 0;
    for db in store.iter() {
        for shard in db.iter() {
            let m = shard.read();
            len += m.len();
            cap += m.capacity();
        }
    }
    (len, cap)
}

fn trim() {
    #[cfg(target_os = "linux")]
    unsafe {
        libc::malloc_trim(0);
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let n: usize = args
        .next()
        .and_then(|a| a.parse().ok())
        .unwrap_or(1_714_739);
    let key_len: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(15);
    let val_len: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(32);

    println!("size_of::<KiviDBValue>() = {}", size_of::<KiviDBValue>());
    println!(
        "size_of::<(String, KiviDBValue)>() = {}",
        size_of::<(String, KiviDBValue)>()
    );
    println!("shards per db = {}", store::num_shards());
    println!("keys = {n}, key_len = {key_len}, val_len = {val_len}\n");

    let store = store::new_store(16);
    let base = rss();
    println!("{:<34} rss={}", "empty store", mb(base));

    let val_src = kividb::types::SharedBytes::from(vec![b'v'; val_len].as_slice());
    for i in 0..n {
        let key = format!("{:0width$}", i, width = key_len);
        let shard = store::get_shard(&store[0], &key);
        shard
            .write()
            .insert(key.as_str(), KiviDBValue::String(val_src.clone()));
    }

    let (len, cap) = table_stats(&store);
    let loaded = rss();
    println!(
        "{:<34} rss={}  entries={len:<10} bucket_slots={cap}",
        "after load",
        mb(loaded)
    );

    // Exactly what FLUSHDB / FLUSHALL do today.
    for db in store.iter() {
        for shard in db.iter() {
            shard.write().clear();
        }
    }
    let (len, cap) = table_stats(&store);
    let cleared = rss();
    println!(
        "{:<34} rss={}  entries={len:<10} bucket_slots={cap}",
        "after clear() [what FLUSHDB does]",
        mb(cleared)
    );

    trim();
    let trimmed = rss();
    println!(
        "{:<34} rss={}",
        "after malloc_trim [MEMORY PURGE]",
        mb(trimmed)
    );

    // What FLUSHDB would have to do to actually release the tables.
    for db in store.iter() {
        for shard in db.iter() {
            *shard.write() = store::ShardMap::new();
        }
    }
    let (len, cap) = table_stats(&store);
    let dropped = rss();
    println!(
        "{:<34} rss={}  entries={len:<10} bucket_slots={cap}",
        "after replacing maps",
        mb(dropped)
    );

    trim();
    let final_rss = rss();
    println!(
        "{:<34} rss={}\n",
        "after replace + malloc_trim",
        mb(final_rss)
    );

    println!("--- accounting (relative to empty store) ---");
    println!(
        "{:<40}{}",
        "peak after load:",
        mb(loaded.saturating_sub(base))
    );
    println!(
        "{:<40}{}",
        "retained by clear():",
        mb(cleared.saturating_sub(base))
    );
    println!(
        "{:<40}{}",
        "retained by clear() + malloc_trim:",
        mb(trimmed.saturating_sub(base))
    );
    println!(
        "{:<40}{}",
        "retained by drop + malloc_trim:",
        mb(final_rss.saturating_sub(base))
    );
    println!(
        "{:<40}{}",
        "bucket arrays clear() held onto:",
        mb(trimmed.saturating_sub(dropped.min(trimmed)))
    );
}
