//! Measurements of `collection_table::CollectionTable` (#764 step 1) against
//! the structures hashes and sets use today: `IndexMap<String, Arc<[u8]>>`
//! for a hash, `hashbrown::HashTable<Arc<[u8]>>` for a set.
//!
//! ```text
//! cargo run --release --example collection_table_bench -- mem
//! cargo run --release --example collection_table_bench -- latency [N]
//! cargo run --release --example collection_table_bench -- scan [N]
//! cargo run --release --example collection_table_bench -- fill
//! valgrind --tool=callgrind --toggle-collect='*measured*' --cache-sim=yes \
//!   target/release/examples/collection_table_bench ops STRUCT WORKLOAD N OP
//! ```
//!
//! - `mem`: bytes per element, as `used_memory` counts them, for #764's
//!   hash (fields `f0..` = `v`), suite 19's (14-byte fields, 32-byte
//!   values) and suite 17's sets (14-byte members; integers), at 1k, 100k
//!   and 3M and just before and after the table's doublings near each.
//! - `ops`: one operation `OP` (`hit`, `miss`, `insert`, `remove`, `scan`)
//!   on `STRUCT` (`table` or `today`) holding `N` elements of `WORKLOAD`
//!   (`h764`, `s17`, `s17int`), for callgrind: everything outside the
//!   `measured_*` function is excluded. Prints the operations done, to
//!   divide the counts by. `FILL=750` sets the table's maximum fill.
//! - `latency`: the slowest single inserts while filling to N (3M).
//! - `scan`: a whole `scan` at COUNT 10 of N (1M) elements, per page.
//! - `fill`: maximum fill 0.75 / 0.82 / 0.875: overflow share, groups read
//!   per lookup and bytes per element (open question 1).

use std::alloc::Layout;
use std::hint::black_box;
use std::ptr::NonNull;
use std::sync::Arc;
use std::time::Instant;

use kividb::collection_table::{
    CollectionTable, Fields, Members, Memory, SlabMemory, SystemMemory,
};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

type IndexHash = indexmap::IndexMap<String, Arc<[u8]>, ahash::RandomState>;

/// Today's large set: `member_set::Table`.
struct TodaySet {
    table: hashbrown::HashTable<Arc<[u8]>>,
    hasher: ahash::RandomState,
}

impl TodaySet {
    fn new() -> Self {
        TodaySet {
            table: hashbrown::HashTable::new(),
            hasher: ahash::RandomState::new(),
        }
    }

    #[inline]
    fn contains(&self, member: &[u8]) -> bool {
        self.table
            .find(self.hasher.hash_one(member), |have| &**have == member)
            .is_some()
    }

    #[inline]
    fn insert(&mut self, member: &[u8]) -> bool {
        let hash = self.hasher.hash_one(member);
        let hasher = &self.hasher;
        match self.table.entry(
            hash,
            |have| &**have == member,
            |have| hasher.hash_one(&**have),
        ) {
            hashbrown::hash_table::Entry::Occupied(_) => false,
            hashbrown::hash_table::Entry::Vacant(place) => {
                place.insert(Arc::from(member));
                true
            }
        }
    }

    #[inline]
    fn remove(&mut self, member: &[u8]) -> bool {
        let hash = self.hasher.hash_one(member);
        match self.table.find_entry(hash, |have| &**have == member) {
            Ok(found) => {
                found.remove();
                let buckets = self.table.num_buckets();
                if buckets > 64 && self.table.len() * 8 < buckets {
                    let hasher = &self.hasher;
                    self.table
                        .shrink_to(self.table.len() * 2, |have| hasher.hash_one(&**have));
                }
                true
            }
            Err(_) => false,
        }
    }
}

/// The system's memory, charged what glibc's malloc really takes for it: a
/// chunk of the size plus 8, rounded up to 16, at least 32.
struct GlibcMemory;

// SAFETY: `SystemMemory`'s blocks; only what is charged differs.
unsafe impl Memory for GlibcMemory {
    fn alloc(layout: Layout, zeroed: bool) -> NonNull<u8> {
        SystemMemory::alloc(layout, zeroed)
    }
    unsafe fn free(block: NonNull<u8>, layout: Layout) {
        // SAFETY: the caller's promise.
        unsafe { SystemMemory::free(block, layout) }
    }
    fn charged(layout: Layout) -> usize {
        if layout.align() <= 16 {
            ((layout.size() + 8 + 15) & !15).max(32)
        } else {
            layout.size()
        }
    }
}

fn counted() -> isize {
    kividb::memory_use::allocated_by_this_thread().expect("this thread's counter")
}

#[derive(Clone, Copy, PartialEq)]
enum Workload {
    H764,
    H19,
    S17,
    S17Int,
}

impl Workload {
    fn parse(s: &str) -> Self {
        match s {
            "h764" => Workload::H764,
            "h19" => Workload::H19,
            "s17" => Workload::S17,
            "s17int" => Workload::S17Int,
            _ => panic!("workload: h764, h19, s17 or s17int"),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Workload::H764 => "#764 hash (f<i> = v)",
            Workload::H19 => "suite 19 hash (14 B field, 32 B value)",
            Workload::S17 => "suite 17 set (14 B members)",
            Workload::S17Int => "suite 17 set (integers)",
        }
    }

    fn is_hash(self) -> bool {
        matches!(self, Workload::H764 | Workload::H19)
    }

    fn key(self, i: usize) -> String {
        match self {
            Workload::H764 => format!("f{i}"),
            Workload::H19 => format!("field-{i:08}"),
            Workload::S17 => format!("member-{i:07}"),
            Workload::S17Int => i.to_string(),
        }
    }

    /// A key of the same shape that no element has.
    fn absent(self, i: usize) -> String {
        match self {
            Workload::H764 => format!("g{i}"),
            Workload::H19 => format!("fiell-{i:08}"),
            Workload::S17 => format!("absent-{i:07}"),
            Workload::S17Int => format!("-{i}"),
        }
    }

    fn value(self, i: usize) -> Vec<u8> {
        match self {
            Workload::H19 => format!("value-{i:026}").into_bytes(),
            _ => b"v".to_vec(),
        }
    }
}

// ---------------------------------------------------------------------------
// mem
// ---------------------------------------------------------------------------

/// Where the table doubles: the length past which an array of `2^bits`
/// groups grows, at the default fill.
fn doubling(bits: u32, fill: usize) -> usize {
    (7usize << bits) * fill / 1000
}

fn table_bytes<A: Memory>(w: Workload, n: usize) -> (usize, isize) {
    let before = counted();
    if w.is_hash() {
        let mut t = CollectionTable::<Fields, A>::with_capacity_and_seed(0, 1);
        for i in 0..n {
            t.insert(w.key(i).as_bytes(), &w.value(i), None);
        }
        while !t.step_resize(usize::MAX) {}
        (t.allocation_size(), counted() - before)
    } else {
        let mut t = CollectionTable::<Members, A>::with_capacity_and_seed(0, 1);
        for i in 0..n {
            t.insert(w.key(i).as_bytes());
        }
        while !t.step_resize(usize::MAX) {}
        (t.allocation_size(), counted() - before)
    }
}

fn today_bytes(w: Workload, n: usize) -> isize {
    let before = counted();
    if w.is_hash() {
        let mut t = IndexHash::default();
        for i in 0..n {
            t.insert(w.key(i), Arc::from(w.value(i)));
        }
        let bytes = counted() - before;
        drop(black_box(t));
        bytes
    } else {
        let mut t = TodaySet::new();
        for i in 0..n {
            t.insert(w.key(i).as_bytes());
        }
        let bytes = counted() - before;
        drop(black_box(t));
        bytes
    }
}

fn mem() {
    let workloads = [
        Workload::H764,
        Workload::H19,
        Workload::S17,
        Workload::S17Int,
    ];
    let mut sizes: Vec<(String, usize)> = vec![
        ("1k".into(), 1_000),
        ("100k".into(), 100_000),
        ("3M".into(), 3_000_000),
    ];
    // The doublings next to 1k, 100k and 3M: 784 -> 1568 (b 7 -> 8),
    // 100,352 (b 14 -> 15), 3,211,264 (b 19 -> 20).
    for bits in [7u32, 14, 19] {
        let at = doubling(bits, 875);
        sizes.push((format!("{at} (before doubling)"), at));
        sizes.push((format!("{} (after)", at + 1), at + 1));
    }
    println!("bytes per element (used_memory): table with entries from the system allocator");
    println!(
        "as counted (requested size) / as glibc really takes them / from value_slab classes; today's structure as counted"
    );
    println!();
    for w in workloads {
        println!("### {}", w.name());
        println!();
        println!(
            "| elements | today | table, system (counted) | table, system (glibc real) | table, value_slab |"
        );
        println!("|---|---|---|---|---|");
        for (label, n) in &sizes {
            let n = *n;
            let (sys, sys_counted) = table_bytes::<SystemMemory>(w, n);
            assert_eq!(
                sys as isize, sys_counted,
                "allocation_size is what is counted"
            );
            let (glibc, _) = table_bytes::<GlibcMemory>(w, n);
            let (slab, _) = table_bytes::<SlabMemory>(w, n);
            let today = today_bytes(w, n);
            let per = |b: f64| b / n as f64;
            println!(
                "| {label} | {:.1} | {:.1} | {:.1} | {:.1} |",
                per(today as f64),
                per(sys as f64),
                per(glibc as f64),
                per(slab as f64)
            );
        }
        println!();
    }
}

// ---------------------------------------------------------------------------
// ops (for callgrind)
// ---------------------------------------------------------------------------

#[inline(never)]
fn measured_table_hash_get(t: &CollectionTable<Fields>, probes: &[String]) -> usize {
    let mut n = 0;
    for p in probes {
        if let Some(e) = t.get(black_box(p.as_bytes())) {
            n += e.value.len();
        }
    }
    n
}

#[inline(never)]
fn measured_today_hash_get(t: &IndexHash, probes: &[String]) -> usize {
    let mut n = 0;
    for p in probes {
        if let Some(v) = t.get(black_box(p.as_str())) {
            n += v.len();
        }
    }
    n
}

#[inline(never)]
fn measured_table_hash_insert(t: &mut CollectionTable<Fields>, probes: &[String]) -> usize {
    probes
        .iter()
        .filter(|p| t.insert(black_box(p.as_bytes()), b"v", None))
        .count()
}

#[inline(never)]
fn measured_today_hash_insert(t: &mut IndexHash, probes: &[String]) -> usize {
    probes
        .iter()
        .filter(|p| {
            t.insert(black_box(p.to_string()), Arc::from(&b"v"[..]))
                .is_none()
        })
        .count()
}

#[inline(never)]
fn measured_table_hash_remove(t: &mut CollectionTable<Fields>, probes: &[String]) -> usize {
    probes
        .iter()
        .filter(|p| t.remove(black_box(p.as_bytes())))
        .count()
}

#[inline(never)]
fn measured_today_hash_remove(t: &mut IndexHash, probes: &[String]) -> usize {
    probes
        .iter()
        .filter(|p| t.swap_remove(black_box(p.as_str())).is_some())
        .count()
}

#[inline(never)]
fn measured_table_set_contains(t: &CollectionTable<Members>, probes: &[String]) -> usize {
    probes
        .iter()
        .filter(|p| t.contains(black_box(p.as_bytes())))
        .count()
}

#[inline(never)]
fn measured_today_set_contains(t: &TodaySet, probes: &[String]) -> usize {
    probes
        .iter()
        .filter(|p| t.contains(black_box(p.as_bytes())))
        .count()
}

#[inline(never)]
fn measured_table_set_insert(t: &mut CollectionTable<Members>, probes: &[String]) -> usize {
    probes
        .iter()
        .filter(|p| t.insert(black_box(p.as_bytes())))
        .count()
}

#[inline(never)]
fn measured_today_set_insert(t: &mut TodaySet, probes: &[String]) -> usize {
    probes
        .iter()
        .filter(|p| t.insert(black_box(p.as_bytes())))
        .count()
}

#[inline(never)]
fn measured_table_set_remove(t: &mut CollectionTable<Members>, probes: &[String]) -> usize {
    probes
        .iter()
        .filter(|p| t.remove(black_box(p.as_bytes())))
        .count()
}

#[inline(never)]
fn measured_today_set_remove(t: &mut TodaySet, probes: &[String]) -> usize {
    probes
        .iter()
        .filter(|p| t.remove(black_box(p.as_bytes())))
        .count()
}

#[inline(never)]
fn measured_table_set_scan(t: &CollectionTable<Members>, pages: usize) -> usize {
    let mut cursor = 0;
    let mut seen = 0;
    for _ in 0..pages {
        cursor = t.scan(cursor, 10, |m| seen += m.len());
        if cursor == 0 {
            break;
        }
    }
    seen
}

#[inline(never)]
fn measured_table_hash_scan(t: &CollectionTable<Fields>, pages: usize) -> usize {
    let mut cursor = 0;
    let mut seen = 0;
    for _ in 0..pages {
        cursor = t.scan(cursor, 10, |e| seen += e.value.len());
        if cursor == 0 {
            break;
        }
    }
    seen
}

fn fill_from_env() -> Option<u16> {
    std::env::var("FILL")
        .ok()
        .map(|f| f.parse().expect("FILL in per mille"))
}

fn ops(args: &[String]) {
    let structure = args
        .first()
        .map(String::as_str)
        .expect("STRUCT: table or today");
    let w = Workload::parse(args.get(1).expect("WORKLOAD"));
    let n: usize = args.get(2).expect("N").parse().expect("N a number");
    let op = args.get(3).map(String::as_str).expect("OP");
    let lookups = 200_000;
    let changes = (n / 20).max(50);
    let mut rng = StdRng::seed_from_u64(3);
    // Keys made before the table, the probes after it, in the order they
    // are used: their reading is the same for both structures.
    let keys: Vec<String> = (0..n).map(|i| w.key(i)).collect();
    let fill = fill_from_env();
    let mut table_hash = CollectionTable::<Fields>::with_capacity_and_seed(0, 9);
    let mut table_set = CollectionTable::<Members>::with_capacity_and_seed(0, 9);
    if let Some(f) = fill {
        table_hash.set_max_fill(f);
        table_set.set_max_fill(f);
    }
    let mut today_hash = IndexHash::default();
    let mut today_set = TodaySet::new();
    let table = structure == "table";
    for (i, k) in keys.iter().enumerate() {
        match (table, w.is_hash()) {
            (true, true) => {
                table_hash.insert(k.as_bytes(), &w.value(i), None);
            }
            (true, false) => {
                table_set.insert(k.as_bytes());
            }
            (false, true) => {
                today_hash.insert(k.clone(), Arc::from(w.value(i)));
            }
            (false, false) => {
                today_set.insert(k.as_bytes());
            }
        }
    }
    while !table_hash.step_resize(usize::MAX) {}
    while !table_set.step_resize(usize::MAX) {}
    let probes: Vec<String> = match op {
        "hit" => (0..lookups)
            .map(|_| keys[rng.gen_range(0..n)].clone())
            .collect(),
        "miss" => (0..lookups).map(|i| w.absent(i % n.max(1))).collect(),
        "insert" => (n..n + changes).map(|i| w.key(i)).collect(),
        "remove" => {
            let mut chosen: Vec<String> =
                keys.choose_multiple(&mut rng, changes).cloned().collect();
            chosen.shuffle(&mut rng);
            chosen
        }
        "scan" => Vec::new(),
        _ => panic!("OP: hit, miss, insert, remove or scan"),
    };
    let pages = n / 10;
    let done = match (table, w.is_hash(), op) {
        (true, true, "hit" | "miss") => measured_table_hash_get(&table_hash, &probes),
        (false, true, "hit" | "miss") => measured_today_hash_get(&today_hash, &probes),
        (true, true, "insert") => measured_table_hash_insert(&mut table_hash, &probes),
        (false, true, "insert") => measured_today_hash_insert(&mut today_hash, &probes),
        (true, true, "remove") => measured_table_hash_remove(&mut table_hash, &probes),
        (false, true, "remove") => measured_today_hash_remove(&mut today_hash, &probes),
        (true, false, "hit" | "miss") => measured_table_set_contains(&table_set, &probes),
        (false, false, "hit" | "miss") => measured_today_set_contains(&today_set, &probes),
        (true, false, "insert") => measured_table_set_insert(&mut table_set, &probes),
        (false, false, "insert") => measured_today_set_insert(&mut today_set, &probes),
        (true, false, "remove") => measured_table_set_remove(&mut table_set, &probes),
        (false, false, "remove") => measured_today_set_remove(&mut today_set, &probes),
        (true, false, "scan") => measured_table_set_scan(&table_set, pages),
        (true, true, "scan") => measured_table_hash_scan(&table_hash, pages),
        _ => panic!("no such measurement"),
    };
    black_box(done);
    let operations = if op == "scan" { pages } else { probes.len() };
    println!(
        "OPS {operations} (result {done}, table resizing {})",
        table_hash.resizing() || table_set.resizing()
    );
}

// ---------------------------------------------------------------------------
// latency
// ---------------------------------------------------------------------------

fn percentile(sorted: &[u64], p: f64) -> u64 {
    sorted[((sorted.len() as f64 * p) as usize).min(sorted.len() - 1)]
}

fn report(label: &str, mut ns: Vec<u64>, total: f64) {
    ns.sort_unstable();
    println!(
        "| {label} | {:.2} | {} | {} | {} | {:.1} |",
        total,
        percentile(&ns, 0.999),
        percentile(&ns, 0.9999),
        ns[ns.len() - 1],
        ns.iter().sum::<u64>() as f64 / ns.len() as f64
    );
}

fn latency(n: usize) {
    println!("| structure | fill s | p99.9 ns | p99.99 ns | max ns | mean ns |");
    println!("|---|---|---|---|---|---|");
    for w in [Workload::H764, Workload::S17] {
        let keys: Vec<String> = (0..n).map(|i| w.key(i)).collect();
        for table in [true, false] {
            let mut ns = Vec::with_capacity(n);
            let start = Instant::now();
            let mut th = CollectionTable::<Fields>::new();
            let mut ts = CollectionTable::<Members>::new();
            let mut ih = IndexHash::default();
            let mut is = TodaySet::new();
            for k in &keys {
                let t = Instant::now();
                match (table, w.is_hash()) {
                    (true, true) => {
                        th.insert(k.as_bytes(), b"v", None);
                    }
                    (true, false) => {
                        ts.insert(k.as_bytes());
                    }
                    (false, true) => {
                        ih.insert(k.clone(), Arc::from(&b"v"[..]));
                    }
                    (false, false) => {
                        is.insert(k.as_bytes());
                    }
                }
                ns.push(t.elapsed().as_nanos() as u64);
            }
            let total = start.elapsed().as_secs_f64();
            let label = match (table, w.is_hash()) {
                (true, true) => "table, #764 hash",
                (true, false) => "table, 14 B set",
                (false, true) => "IndexMap, #764 hash",
                (false, false) => "HashTable<Arc>, 14 B set",
            };
            report(label, ns, total);
            drop(black_box((th, ts, ih, is)));
        }
    }
}

// ---------------------------------------------------------------------------
// scan
// ---------------------------------------------------------------------------

fn scan(n: usize) {
    let mut t = CollectionTable::<Members>::with_capacity_and_seed(0, 5);
    for i in 0..n {
        t.insert(Workload::S17.key(i).as_bytes());
    }
    while !t.step_resize(usize::MAX) {}
    let mut elements = Vec::new();
    let mut groups = Vec::new();
    let mut ns = Vec::new();
    let mut cursor = 0;
    loop {
        let mut page = 0usize;
        let start = Instant::now();
        let (next, g) = t.scan_counted(cursor, 10, |m| {
            black_box(&m);
            page += 1;
        });
        ns.push(start.elapsed().as_nanos() as u64);
        elements.push(page as u64);
        groups.push(g as u64);
        if next == 0 {
            break;
        }
        cursor = next;
    }
    let pages = ns.len();
    let total: u64 = elements.iter().sum();
    assert_eq!(total as usize, n, "no resize: each element once");
    for (name, mut v) in [("elements", elements), ("groups", groups), ("ns", ns)] {
        v.sort_unstable();
        println!(
            "{name} per page: mean {:.2}, p50 {}, p99 {}, max {}",
            v.iter().sum::<u64>() as f64 / pages as f64,
            percentile(&v, 0.5),
            percentile(&v, 0.99),
            v[v.len() - 1]
        );
    }
    println!("pages {pages} for {n} elements at COUNT 10");
}

// ---------------------------------------------------------------------------
// fill
// ---------------------------------------------------------------------------

fn fill() {
    println!(
        "| max fill | workload | at doubling (b=19, n) | overflow groups / main | groups read per hit | per miss | longest chain | B/elem at doubling | B/elem just after | B/elem mean 1M-3.3M |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|");
    for f in [750u16, 820, 875] {
        for w in [Workload::H764, Workload::S17, Workload::S17Int] {
            let top = doubling(19, usize::from(f));
            let mut th = CollectionTable::<Fields>::with_capacity_and_seed(0, 21);
            let mut ts = CollectionTable::<Members>::with_capacity_and_seed(0, 21);
            th.set_max_fill(f);
            ts.set_max_fill(f);
            // Log-spaced sample points from 1M to past the doubling.
            let mut points = Vec::new();
            let mut p = 1_000_000f64;
            while (p as usize) < top + top / 10 {
                points.push(p as usize);
                p *= 1.005;
            }
            let mut next_point = 0;
            let mut sum = 0f64;
            let mut at_doubling = None;
            let mut after = 0f64;
            for i in 0..top + top / 10 {
                if w.is_hash() {
                    th.insert(w.key(i).as_bytes(), &w.value(i), None);
                } else {
                    ts.insert(w.key(i).as_bytes());
                }
                let len = i + 1;
                let bytes = |th: &CollectionTable<Fields>, ts: &CollectionTable<Members>| {
                    if w.is_hash() {
                        th.allocation_size()
                    } else {
                        ts.allocation_size()
                    }
                };
                if len == top {
                    let stats = if w.is_hash() { th.stats() } else { ts.stats() };
                    at_doubling = Some((stats, bytes(&th, &ts) as f64 / len as f64));
                }
                if len == top + 1 {
                    // The doubling's moves finished, as the background task
                    // would: the old array is gone.
                    while !(th.step_resize(usize::MAX) && ts.step_resize(usize::MAX)) {}
                    after = bytes(&th, &ts) as f64 / len as f64;
                }
                if next_point < points.len() && len == points[next_point] {
                    sum += bytes(&th, &ts) as f64 / len as f64;
                    next_point += 1;
                }
            }
            let (s, b) = at_doubling.expect("reached the doubling");
            println!(
                "| {:.3} | {} | {} | {:.3} | {:.3} | {:.3} | {} | {:.1} | {:.1} | {:.1} |",
                f64::from(f) / 1000.0,
                w.name(),
                top,
                s.overflow_groups as f64 / s.groups as f64,
                s.hit_groups as f64 / top as f64,
                s.miss_groups as f64 / s.groups as f64,
                s.max_chain,
                b,
                after,
                sum / points.len() as f64
            );
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let number = |i: usize, default: usize| {
        args.get(i)
            .map_or(default, |s| s.parse().expect("a number"))
    };
    match args.first().map(String::as_str) {
        Some("mem") => mem(),
        Some("ops") => ops(&args[1..]),
        Some("latency") => latency(number(1, 3_000_000)),
        Some("scan") => scan(number(1, 1_000_000)),
        Some("fill") => fill(),
        _ => eprintln!(
            "usage: collection_table_bench mem | ops STRUCT WORKLOAD N OP | latency [N] | scan [N] | fill"
        ),
    }
}
