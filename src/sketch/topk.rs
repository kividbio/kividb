//! Top-K (`TOPK.*`): the items seen most often, as Redis 8 keeps them
//! (formerly RedisBloom): a HeavyKeeper sketch and a heap of the `k` items
//! with the largest counts.
//!
//! The sketch is `depth` rows of `width` buckets, each a fingerprint and a
//! count. An item adds to one bucket in each row, the one at
//! `murmur2(item, row) mod width`, its fingerprint being `murmur2(item,
//! 1919)`: an empty bucket or one with its fingerprint takes the increment;
//! another item's count is decayed instead, one step for each unit of the
//! increment with probability `decay^count`, and the bucket is the item's
//! once it reaches zero. The heap keeps the `k` items whose counts were
//! largest when they were added.
//!
//! Everything but the decay's chances is Redis's, established by checking a
//! model against Redis 8.10.2 with `decay` 1, where no chance is involved:
//! 12,000 random `TOPK.INCRBY`s on sketches of up to 6 × 6 × 3 gave the
//! same buckets (read off `DUMP`), the same items expelled, and the same
//! `TOPK.LIST`, `TOPK.COUNT` and `TOPK.QUERY`, quirks included (`TOPK.COUNT`
//! answers 0 for an item in the heap whose buckets count less than the
//! heap's least; `TOPK.LIST` leaves out items counted 0). HeavyKeeper is
//! Gong et al.'s published algorithm; MurmurHash2 Austin Appleby's public-
//! domain one; the seeds and the decay's lookup table came from general
//! knowledge of how RedisBloom works. No Redis or RedisBloom source code
//! was opened or copied.
//!
//! The chances are drawn from a generator kept in the sketch (and saved
//! with it), not from the process: so a replica, and a replay of the AOF,
//! make exactly the choices the master made, and end with the same sketch.
//! Redis's format has no place for it: a sketch read from it (an RDB file,
//! `RESTORE`, and so `MIGRATE`) starts at [`RNG_START`], as a new one does.
//! Answers under a decay below 1 are therefore not Redis's bit for bit,
//! only as good: see the checks in `scripts/checks/sketch/topk/`.
//!
//! Size limits, which Redis does not have, checked before anything is
//! allocated, whether a sketch is made or loaded: [`MAX_K`],
//! [`MAX_BUCKETS`], [`MAX_DEPTH`]. A command is held to a per-command
//! amount of work ([`BUDGET`]), reckoned before it starts.

use std::io;
use std::sync::Arc;

use super::murmur::murmur2_32;

/// The seed of an item's fingerprint.
pub const FINGERPRINT_SEED: u32 = 1919;
/// The most items a sketch keeps (`k`).
pub const MAX_K: u64 = 1 << 16;
/// The most buckets (`width × depth`): 2^26, 512 MiB.
pub const MAX_BUCKETS: u64 = 1 << 26;
/// The most rows.
pub const MAX_DEPTH: u64 = 1 << 16;
/// The largest increment `TOPK.INCRBY` takes, as in Redis.
pub const MAX_INCREMENT: i64 = 100_000;
/// What one `TOPK.ADD` or `TOPK.INCRBY` may cost at most, reckoned before it
/// starts, in steps of some 2.5 ns: for each item, its rows times 64 (a
/// row's hashing and bucket) and its increment (the most decay steps it
/// can take there), and a sixteenth of `k` (the heap is searched for it).
/// Some 17,000 items of `TOPK.ADD` at the default size, or eleven
/// increments of 100,000 at depth 7: a few tens of milliseconds at most.
pub const BUDGET: u64 = 8_000_000;

/// Entries of the decay's lookup table: `decay^i` for `i` below it.
const LOOKUP: usize = 256;

/// Why a sketch could not be made or written to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopKError {
    TooLarge,
    NoMemory,
}

/// One bucket: a fingerprint and its count.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(C)]
pub struct Bucket {
    pub fp: u32,
    pub count: u32,
}

// SAFETY: two u32s, no padding; all zero is an empty bucket (the default).
unsafe impl super::Zeroed for Bucket {}

/// One place in the heap: empty, or an item with its fingerprint and count.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Slot {
    pub fp: u32,
    pub count: u32,
    pub item: Option<Arc<[u8]>>,
}

/// A copy of `v` in an allocation that is asked for, not assumed.
fn copy_of<T: Clone>(v: &[T]) -> Result<Vec<T>, TopKError> {
    let mut out = Vec::new();
    out.try_reserve_exact(v.len())
        .map_err(|_| TopKError::NoMemory)?;
    out.extend_from_slice(v);
    Ok(out)
}

/// A Top-K sketch.
#[derive(Clone, Debug)]
pub struct TopK {
    pub k: u32,
    pub width: u32,
    pub depth: u32,
    pub decay: f64,
    /// Row by row. Shared between copies until one is written to.
    pub buckets: Arc<Vec<Bucket>>,
    /// A min-heap by count, as Redis keeps it. Shared likewise.
    pub heap: Arc<Vec<Slot>>,
    /// The generator the decay's chances are drawn from.
    pub rng: u64,
    lookup: Box<[f64; LOOKUP]>,
}

impl PartialEq for TopK {
    fn eq(&self, o: &TopK) -> bool {
        self.k == o.k
            && self.width == o.width
            && self.depth == o.depth
            && self.decay.to_bits() == o.decay.to_bits()
            && self.buckets == o.buckets
            && self.heap == o.heap
            && self.rng == o.rng
    }
}

/// The generator's state for a new sketch: the same for every one, so that
/// a replica makes the sketch the master made.
pub const RNG_START: u64 = 0x9e37_79b9_7f4a_7c15;

/// Whether a sketch of these dimensions is within the limits.
pub fn fits(k: u64, width: u64, depth: u64) -> bool {
    (1..=MAX_K).contains(&k)
        && (1..=MAX_DEPTH).contains(&depth)
        && width >= 1
        && width.checked_mul(depth).is_some_and(|n| n <= MAX_BUCKETS)
}

fn lookup_for(decay: f64) -> Box<[f64; LOOKUP]> {
    let mut t = Box::new([0f64; LOOKUP]);
    for (i, v) in t.iter_mut().enumerate() {
        *v = decay.powi(i as i32);
    }
    t
}

/// What one item costs a command (see [`BUDGET`]).
pub fn cost(k: u32, depth: u32, increment: u64) -> u64 {
    u64::from(depth)
        .saturating_mul(increment.saturating_add(64))
        .saturating_add(u64::from(k) / 16)
}

impl TopK {
    /// An empty sketch, if within the limits and to be had.
    pub fn new(k: u64, width: u64, depth: u64, decay: f64) -> Result<TopK, TopKError> {
        if !fits(k, width, depth) {
            return Err(TopKError::TooLarge);
        }
        let buckets: Vec<Bucket> =
            super::zeroed((width * depth) as usize).ok_or(TopKError::NoMemory)?;
        let mut heap = Vec::new();
        heap.try_reserve_exact(k as usize)
            .map_err(|_| TopKError::NoMemory)?;
        heap.resize(k as usize, Slot::default());
        Ok(Self::from_parts(
            k as u32,
            width as u32,
            depth as u32,
            decay,
            buckets,
            heap,
            RNG_START,
        ))
    }

    /// A sketch of these parts, checked by the caller.
    pub fn from_parts(
        k: u32,
        width: u32,
        depth: u32,
        decay: f64,
        buckets: Vec<Bucket>,
        heap: Vec<Slot>,
        rng: u64,
    ) -> TopK {
        TopK {
            k,
            width,
            depth,
            decay,
            buckets: Arc::new(buckets),
            heap: Arc::new(heap),
            rng,
            lookup: lookup_for(decay),
        }
    }

    /// The bytes a sketch of these dimensions takes, about.
    pub fn bytes_for(k: u64, width: u64, depth: u64) -> u64 {
        width
            .saturating_mul(depth)
            .saturating_mul(8)
            .saturating_add(k.saturating_mul(std::mem::size_of::<Slot>() as u64))
    }

    /// What writing to it may allocate: copies of what another copy shares.
    pub fn copy_bytes(&self) -> u64 {
        let mut n = 0;
        if Arc::strong_count(&self.buckets) > 1 {
            n += self.buckets.len() as u64 * 8;
        }
        if Arc::strong_count(&self.heap) > 1 {
            n += self.heap.len() as u64 * std::mem::size_of::<Slot>() as u64;
        }
        n
    }

    /// The next chance, in [0, 1): xorshift64*.
    fn chance(&mut self) -> f64 {
        let mut x = self.rng;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.rng = x;
        (x.wrapping_mul(0x2545_f491_4f6c_dd1d) >> 11) as f64 / (1u64 << 53) as f64
    }

    /// The probability that a bucket counting `count` is decayed a step.
    fn decay_at(&self, count: u32) -> f64 {
        let count = count as usize;
        if count < LOOKUP {
            self.lookup[count]
        } else {
            self.lookup[LOOKUP - 1].powi((count / (LOOKUP - 1)) as i32)
                * self.lookup[count % (LOOKUP - 1)]
        }
    }

    fn find(&self, fp: u32, item: &[u8]) -> Option<usize> {
        self.heap
            .iter()
            .position(|s| s.fp == fp && s.item.as_deref() == Some(item))
    }

    /// Restore the heap's order below `start`, as Redis does.
    fn sift_down(heap: &mut [Slot], mut start: usize) {
        let n = heap.len();
        if n < 2 || (n - 2) / 2 < start {
            return;
        }
        let mut child = 2 * start + 1;
        if child + 1 < n && heap[child].count > heap[child + 1].count {
            child += 1;
        }
        if heap[child].count > heap[start].count {
            return;
        }
        let top = std::mem::take(&mut heap[start]);
        loop {
            heap[start] = std::mem::take(&mut heap[child]);
            start = child;
            if (n - 2) / 2 < child {
                break;
            }
            child = 2 * child + 1;
            if child + 1 < n && heap[child].count > heap[child + 1].count {
                child += 1;
            }
            if heap[child].count >= top.count {
                break;
            }
        }
        heap[start] = top;
    }

    /// Add `increment` to `item`: the item the heap let go of to make room
    /// for it, if any.
    pub fn incr_by(&mut self, item: &[u8], increment: u32) -> Result<Option<Arc<[u8]>>, TopKError> {
        if Arc::get_mut(&mut self.buckets).is_none() {
            self.buckets = Arc::new(copy_of(&self.buckets)?);
        }
        if Arc::get_mut(&mut self.heap).is_none() {
            self.heap = Arc::new(copy_of(&self.heap)?);
        }
        let fp = murmur2_32(item, FINGERPRINT_SEED);
        let heap_min = self.heap[0].count;
        let width = self.width as usize;
        let mut max_count = 0u32;
        for row in 0..self.depth {
            let at = row as usize * width + murmur2_32(item, row) as usize % width;
            let b = Arc::get_mut(&mut self.buckets).expect("own")[at];
            if b.count == 0 {
                self.set_bucket(at, fp, increment);
                max_count = max_count.max(increment);
            } else if b.fp == fp {
                let count = b.count.wrapping_add(increment);
                self.set_bucket(at, fp, count);
                max_count = max_count.max(count);
            } else {
                // Decayed one step for each unit, at the chance its count
                // had when the item came.
                let p = self.decay_at(b.count);
                let mut count = b.count;
                let mut owner = b.fp;
                for left in (1..=increment).rev() {
                    if self.chance() < p {
                        count -= 1;
                        if count == 0 {
                            owner = fp;
                            count = left;
                            max_count = max_count.max(count);
                            break;
                        }
                    }
                }
                self.set_bucket(at, owner, count);
            }
        }
        if max_count < heap_min {
            return Ok(None);
        }
        let heap = Arc::get_mut(&mut self.heap).expect("own");
        let at = heap
            .iter()
            .position(|s| s.fp == fp && s.item.as_deref() == Some(item));
        Ok(match at {
            Some(at) => {
                heap[at].count = max_count;
                Self::sift_down(heap, at);
                None
            }
            None => {
                let expelled = heap[0].item.take();
                heap[0] = Slot {
                    fp,
                    count: max_count,
                    item: Some(Arc::from(item)),
                };
                Self::sift_down(heap, 0);
                expelled
            }
        })
    }

    fn set_bucket(&mut self, at: usize, fp: u32, count: u32) {
        Arc::get_mut(&mut self.buckets).expect("own")[at] = Bucket { fp, count };
    }

    /// Whether `item` is in the heap.
    pub fn query(&self, item: &[u8]) -> bool {
        self.find(murmur2_32(item, FINGERPRINT_SEED), item)
            .is_some()
    }

    /// `TOPK.COUNT`: the largest count of a bucket with the item's
    /// fingerprint; but 0 for an item in the heap whose count that is
    /// below the heap's least, as Redis answers.
    pub fn count(&self, item: &[u8]) -> u32 {
        let fp = murmur2_32(item, FINGERPRINT_SEED);
        let width = self.width as usize;
        let mut best = 0;
        for row in 0..self.depth {
            let at = row as usize * width + murmur2_32(item, row) as usize % width;
            let b = self.buckets[at];
            if b.fp == fp {
                best = best.max(b.count);
            }
        }
        if best < self.heap[0].count && self.find(fp, item).is_some() {
            return 0;
        }
        best
    }

    /// The heap's items counted above 0, the largest count first, those of
    /// one count in the heap's order.
    pub fn list(&self) -> Vec<(Arc<[u8]>, u32)> {
        let mut out: Vec<(Arc<[u8]>, u32)> = self
            .heap
            .iter()
            .filter(|s| s.count > 0)
            .filter_map(|s| s.item.clone().map(|i| (i, s.count)))
            .collect();
        out.sort_by_key(|a| std::cmp::Reverse(a.1));
        out
    }

    pub fn memory_bytes(&self) -> usize {
        self.buckets.len() * 8
            + self.heap.len() * std::mem::size_of::<Slot>()
            + self
                .heap
                .iter()
                .filter_map(|s| s.item.as_ref().map(|i| i.len() + 16))
                .sum::<usize>()
            + LOOKUP * 8
    }

    pub fn data_bytes(&self) -> u64 {
        self.memory_bytes() as u64
    }
}

// ── KDB encoding ─────────────────────────────────────────────────────────

/// `k`, `width`, `depth` (u32), `decay` (f64), the generator (u64), the
/// buckets (fingerprint and count, u32 each), then each place in the heap:
/// fingerprint, count, then 0 for an empty one or 1 and the item (a u32
/// length and its bytes). Little-endian.
pub fn write_kdb(w: &mut impl io::Write, t: &TopK) -> io::Result<()> {
    w.write_all(&t.k.to_le_bytes())?;
    w.write_all(&t.width.to_le_bytes())?;
    w.write_all(&t.depth.to_le_bytes())?;
    w.write_all(&t.decay.to_le_bytes())?;
    w.write_all(&t.rng.to_le_bytes())?;
    let mut buf = Vec::with_capacity(64 * 1024);
    for chunk in t.buckets.chunks(8 * 1024) {
        buf.clear();
        for b in chunk {
            buf.extend_from_slice(&b.fp.to_le_bytes());
            buf.extend_from_slice(&b.count.to_le_bytes());
        }
        w.write_all(&buf)?;
    }
    for s in t.heap.iter() {
        w.write_all(&s.fp.to_le_bytes())?;
        w.write_all(&s.count.to_le_bytes())?;
        match &s.item {
            None => w.write_all(&[0])?,
            Some(item) => {
                w.write_all(&[1])?;
                w.write_all(&(item.len() as u32).to_le_bytes())?;
                w.write_all(item)?;
            }
        }
    }
    Ok(())
}

pub fn kdb_len(t: &TopK) -> u64 {
    28 + t.buckets.len() as u64 * 8
        + t.heap
            .iter()
            .map(|s| 9 + s.item.as_ref().map_or(0, |i| 4 + i.len() as u64))
            .sum::<u64>()
}

/// Whether `decay` is one a sketch may have: in (0, 1].
pub fn decay_ok(decay: f64) -> bool {
    decay > 0.0 && decay <= 1.0
}

/// A sketch [`write_kdb`] wrote: its shape checked against the limits
/// before anything sized by it is allocated; what it holds read as it
/// arrives.
pub fn read_kdb(r: &mut impl io::Read) -> io::Result<TopK> {
    let k = super::read_u32(r)?;
    let width = super::read_u32(r)?;
    let depth = super::read_u32(r)?;
    let decay = f64::from_bits(super::read_u64(r)?);
    let rng = super::read_u64(r)?;
    if !fits(u64::from(k), u64::from(width), u64::from(depth)) || !decay_ok(decay) {
        return Err(super::invalid(
            "a Top-K sketch over the size limits is not supported",
        ));
    }
    let raw = super::read_vec(r, width as usize * depth as usize * 8)?;
    let buckets = buckets_from_bytes(&raw)?;
    drop(raw);
    let mut heap = Vec::with_capacity((k as usize).min(1024));
    for _ in 0..k {
        let fp = super::read_u32(r)?;
        let count = super::read_u32(r)?;
        let mut present = [0u8];
        r.read_exact(&mut present)?;
        let item = match present[0] {
            0 => None,
            1 => {
                let len = super::read_u32(r)? as usize;
                Some(Arc::from(super::read_vec(r, len)?.into_boxed_slice()))
            }
            _ => return Err(super::invalid("a Top-K heap entry is malformed")),
        };
        heap.push(Slot { fp, count, item });
    }
    Ok(TopK::from_parts(k, width, depth, decay, buckets, heap, rng))
}

/// Buckets from their bytes (fingerprint and count, little-endian), into
/// an allocation that is asked for.
pub fn buckets_from_bytes(raw: &[u8]) -> io::Result<Vec<Bucket>> {
    let mut v = Vec::new();
    v.try_reserve_exact(raw.len() / 8)
        .map_err(|_| io::Error::new(io::ErrorKind::OutOfMemory, "no memory for a Top-K sketch"))?;
    let (words, _) = raw.as_chunks::<8>();
    v.extend(words.iter().map(|w| Bucket {
        fp: u32::from_le_bytes([w[0], w[1], w[2], w[3]]),
        count: u32::from_le_bytes([w[4], w[5], w[6], w[7]]),
    }));
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rng(seed: &mut u64) -> u64 {
        *seed ^= *seed << 13;
        *seed ^= *seed >> 7;
        *seed ^= *seed << 17;
        *seed
    }

    /// A Zipf stream: the heavy hitters are found.
    #[test]
    fn finds_the_heavy_hitters() {
        let mut t = TopK::new(10, 1000, 5, 0.9).unwrap();
        let mut seed = 99u64;
        let mut truth = std::collections::HashMap::new();
        for _ in 0..200_000 {
            // P(rank r) ~ 1/r over 5,000 ranks.
            let u = (rng(&mut seed) % 1_000_000) as f64 / 1_000_000.0;
            let r = (5000f64.powf(u)) as u64;
            let item = format!("z{r}");
            t.incr_by(item.as_bytes(), 1).unwrap();
            *truth.entry(item).or_insert(0u64) += 1;
        }
        let mut top: Vec<_> = truth.into_iter().collect();
        top.sort_by_key(|a| std::cmp::Reverse(a.1));
        let listed: Vec<String> = t
            .list()
            .iter()
            .map(|(i, _)| String::from_utf8(i.to_vec()).unwrap())
            .collect();
        let found = top[..10].iter().filter(|(i, _)| listed.contains(i)).count();
        assert!(found >= 9, "{found} of the top 10: {listed:?}");
    }

    /// With `decay` 1 nothing is left to chance: a bucket of another item
    /// is decayed one step for each unit.
    #[test]
    fn decay_one_is_certain() {
        let mut t = TopK::new(1, 1, 1, 1.0).unwrap();
        assert_eq!(t.incr_by(b"a", 5).unwrap(), None);
        assert_eq!(t.incr_by(b"b", 3).unwrap(), None);
        assert_eq!(t.buckets[0].count, 2);
        // Taken over with what is left of the increment; the kept item is
        // not let go of until the newcomer counts as much.
        assert_eq!(t.incr_by(b"b", 4).unwrap(), None);
        assert_eq!(t.buckets[0].count, 3);
        assert_eq!(t.list()[0].1, 5);
        assert_eq!(t.incr_by(b"b", 3).unwrap().as_deref(), Some(&b"a"[..]));
        assert_eq!(t.list()[0].1, 6);
    }

    /// Two sketches given the same commands end the same, chances and all:
    /// what a replica and a replay depend on.
    #[test]
    fn the_same_commands_make_the_same_sketch() {
        let mut a = TopK::new(5, 20, 3, 0.8).unwrap();
        let mut b = a.clone();
        let mut seed = 5u64;
        for _ in 0..5000 {
            let item = format!("i{}", rng(&mut seed) % 60);
            let inc = (rng(&mut seed) % 4) as u32;
            assert_eq!(
                a.incr_by(item.as_bytes(), inc).unwrap(),
                b.incr_by(item.as_bytes(), inc).unwrap()
            );
        }
        assert_eq!(a, b);
        assert!(!Arc::ptr_eq(&a.buckets, &b.buckets));
    }

    #[test]
    fn limits_before_allocating() {
        assert_eq!(TopK::new(0, 8, 7, 0.9), Err(TopKError::TooLarge));
        assert_eq!(TopK::new(MAX_K + 1, 8, 7, 0.9), Err(TopKError::TooLarge));
        assert_eq!(
            TopK::new(1, MAX_BUCKETS + 1, 1, 0.9),
            Err(TopKError::TooLarge)
        );
        assert_eq!(
            TopK::new(1, 1, MAX_DEPTH + 1, 0.9),
            Err(TopKError::TooLarge)
        );
        assert_eq!(
            TopK::new(1, u64::MAX, u64::MAX, 0.9),
            Err(TopKError::TooLarge)
        );
        assert!(TopK::new(MAX_K, 8, 7, 0.9).is_ok());
    }

    #[test]
    fn kdb_round_trip() {
        let mut t = TopK::new(4, 10, 3, 0.9).unwrap();
        for i in 0..300 {
            t.incr_by(format!("{}", i % 17).as_bytes(), 1).unwrap();
        }
        t.incr_by(b"", 2).unwrap();
        let mut out = Vec::new();
        write_kdb(&mut out, &t).unwrap();
        assert_eq!(out.len() as u64, kdb_len(&t));
        assert_eq!(read_kdb(&mut &out[..]).unwrap(), t);
    }

    #[test]
    fn corrupt_kdb_is_refused_cleanly() {
        let mut t = TopK::new(3, 6, 2, 0.9).unwrap();
        for i in 0..40 {
            t.incr_by(format!("{i}").as_bytes(), 1).unwrap();
        }
        let mut good = Vec::new();
        write_kdb(&mut good, &t).unwrap();
        let mut seed = 77u64;
        for _ in 0..4000 {
            let mut bad = good.clone();
            match rng(&mut seed) % 3 {
                0 => {
                    let at = rng(&mut seed) as usize % bad.len();
                    bad[at] = rng(&mut seed) as u8;
                }
                1 => bad.truncate(rng(&mut seed) as usize % bad.len()),
                _ => {
                    let at = (rng(&mut seed) as usize % 3) * 4;
                    bad[at..at + 4].copy_from_slice(&(rng(&mut seed) as u32).to_le_bytes());
                }
            }
            if let Ok(read) = read_kdb(&mut &bad[..]) {
                assert!(fits(
                    u64::from(read.k),
                    u64::from(read.width),
                    u64::from(read.depth)
                ));
                assert_eq!(read.heap.len(), read.k as usize);
            }
        }
    }
}
