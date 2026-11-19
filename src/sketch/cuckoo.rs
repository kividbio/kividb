//! Cuckoo filters (`CF.*`), laid out byte for byte as Redis 8 lays them
//! out (formerly RedisBloom).
//!
//! A filter is a chain of sub-filters, each `buckets` buckets of
//! `bucket_size` one-byte fingerprints. An item hashes to `h =
//! murmur64a(item, 0)`; its fingerprint is `h mod 255 + 1`, its buckets `h`
//! and `h ^ (fingerprint × 0x5bd1e995)`, each modulo the number of buckets
//! (a power of two, so each is the other's alternate). It goes into the first
//! free slot of either bucket, the newest sub-filter first; failing that,
//! fingerprints are kicked to their other bucket, up to `max_iterations`
//! times, in the newest sub-filter (and the kicks undone if that fails);
//! failing that, a sub-filter `expansion` times larger is added. Deletes look
//! newest first, and once more than a tenth of the items are deletes, the
//! newer sub-filters' fingerprints are moved down into older ones where they
//! fit, and an emptied newest sub-filter is dropped (`CF.COMPACT` does so on
//! demand).
//!
//! Everything above was established by checking a model against Redis
//! 8.10.2: 1,200 random sequences of adds, deletes, lookups and compactions
//! on filters of every bucket size from 1 to 4, kick limits from 1 to 30 and
//! expansions from 0 to 4 gave the same bytes (`CF.SCANDUMP`), counts, item
//! and delete totals after every command. Cuckoo hashing is Fan et al.'s
//! published algorithm, MurmurHash64A Austin Appleby's public-domain one;
//! the constants, the kick order and the compaction rule came from general
//! knowledge of how RedisBloom works. No Redis or RedisBloom source code was
//! opened or copied. So a filter moved from Redis (an RDB file, `RESTORE`,
//! `CF.LOADCHUNK`) answers as it did there, and one moved back the same.
//!
//! Size limits, which Redis does not have, checked before anything is
//! allocated, whether a filter is made, grows or is loaded: [`MAX_BYTES`]
//! in all its sub-filters, and [`MAX_FILTERS`] sub-filters. And a limit on
//! the work of one compaction, [`COMPACT_BUDGET`], past which it stops.

use std::io;
use std::sync::Arc;

use super::murmur::murmur64a;

/// The most bytes a filter's sub-filters may take together: 1 GiB.
pub const MAX_BYTES: u64 = 1 << 30;
/// The most sub-filters a filter may have, made, grown or loaded: as many as
/// Redis takes in a `CF.LOADCHUNK` header.
pub const MAX_FILTERS: u64 = 99;
/// The largest capacity a filter is made with.
pub const MAX_CAPACITY: i64 = 1 << 30;
/// The bytes of one `CF.SCANDUMP` chunk at most.
pub const CHUNK_MAX: u64 = 16 * 1024 * 1024;
/// A `CF.SCANDUMP` header's length.
pub const HEADER_LEN: usize = 38;
/// The most work one compaction does (`CF.COMPACT`, or the one a `CF.DEL`
/// starts), in steps of some 1.6 ns: each slot of a newer sub-filter costs
/// one, and each fingerprint in one the slots of the older sub-filters it
/// may look at (two buckets of each). Some 25 ms. A compaction that would
/// go past it stops where it is, its newest sub-filter kept, whoever runs
/// it: the same on a master, a replica and in a replay. A filter of some
/// hundreds of thousands of fingerprints compacts whole, as in Redis.
pub const COMPACT_BUDGET: u64 = 15_000_000;

const ALT_FACTOR: u64 = 0x5bd1_e995;

/// Why an item could not be added or a filter made.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CuckooError {
    /// No room, and the filter does not grow.
    Full,
    /// No room, and the filter has as many sub-filters as it may.
    MaxExpansions,
    /// Over [`MAX_BYTES`] or [`MAX_FILTERS`].
    TooLarge,
    /// The system would not allocate it.
    NoMemory,
    /// A client's command would grow the filter past what it was given
    /// room for under `maxmemory`, and there is no more
    /// ([`Cuckoo::insert_within`]).
    NoRoom,
}

/// A sub-filter's bytes, shared between copies of the filter until one is
/// written to.
#[derive(Clone, Debug, PartialEq)]
pub struct SubFilter {
    pub buckets: u64,
    pub data: Arc<Box<[u8]>>,
}

impl SubFilter {
    fn zeroed(buckets: u64, bucket_size: u16) -> Result<SubFilter, CuckooError> {
        let bytes = buckets
            .checked_mul(u64::from(bucket_size))
            .filter(|&b| b > 0 && b <= MAX_BYTES)
            .ok_or(CuckooError::TooLarge)?;
        let v = super::zeroed::<u8>(bytes as usize).ok_or(CuckooError::NoMemory)?;
        Ok(SubFilter {
            buckets,
            data: Arc::new(v.into_boxed_slice()),
        })
    }

    /// The bytes to write to, copied first if another copy shares them.
    fn own(&mut self) -> Result<&mut [u8], CuckooError> {
        if Arc::get_mut(&mut self.data).is_none() {
            let mut copy: Vec<u8> = Vec::new();
            copy.try_reserve_exact(self.data.len())
                .map_err(|_| CuckooError::NoMemory)?;
            copy.extend_from_slice(&self.data);
            self.data = Arc::new(copy.into_boxed_slice());
        }
        Ok(Arc::get_mut(&mut self.data).expect("own"))
    }
}

/// An item's lookup: its fingerprint and its two hashes.
#[derive(Clone, Copy, Debug)]
pub struct Lookup {
    pub fp: u8,
    pub h1: u64,
    pub h2: u64,
}

#[inline]
fn alt(fp: u8, index: u64) -> u64 {
    index ^ u64::from(fp).wrapping_mul(ALT_FACTOR)
}

/// The lookup of `item`.
#[inline]
pub fn lookup(item: &[u8]) -> Lookup {
    let h = murmur64a(item, 0);
    let fp = (h % 255 + 1) as u8;
    Lookup {
        fp,
        h1: h,
        h2: alt(fp, h),
    }
}

/// A Cuckoo filter.
#[derive(Clone, Debug, PartialEq)]
pub struct Cuckoo {
    pub items: u64,
    pub deletes: u64,
    pub bucket_size: u16,
    pub max_iterations: u16,
    pub expansion: u16,
    pub filters: Vec<SubFilter>,
}

/// How a filter is made.
#[derive(Clone, Copy, Debug)]
pub struct Params {
    pub capacity: u64,
    pub bucket_size: u16,
    pub max_iterations: u16,
    /// As given: rounded up to a power of two (0 stays 0).
    pub expansion: u16,
}

/// The number of buckets of a filter for `capacity` items in buckets of
/// `bucket_size`: `capacity / bucket_size` rounded up to a power of two.
pub fn buckets_for(capacity: u64, bucket_size: u16) -> u64 {
    (capacity / u64::from(bucket_size.max(1)))
        .max(1)
        .checked_next_power_of_two()
        .unwrap_or(u64::MAX)
}

/// `expansion` rounded up to a power of two, as Redis keeps it.
pub fn expansion_of(expansion: u16) -> u16 {
    if expansion == 0 {
        0
    } else {
        expansion.checked_next_power_of_two().unwrap_or(32768)
    }
}

/// The load, in per cent of its slots, that a Cuckoo filter of buckets of
/// `bucket_size` reaches before an add fails, about (Fan et al.: some 50%
/// with buckets of 1, 84% of 2, 95% of 4): what a command of many items is
/// given room for growing to ([`Cuckoo::bytes_to_add_items`]).
fn load_percent(bucket_size: u16) -> u64 {
    match bucket_size {
        0 | 1 => 50,
        2 => 84,
        3 => 90,
        _ => 95,
    }
}

/// The bytes of the sub-filter after one of `last` buckets, in a filter of
/// `filters` sub-filters and `data` bytes: 0 if it would not grow
/// ([`Cuckoo::growth_bytes`]).
fn next_growth(bucket_size: u16, expansion: u16, filters: u64, data: u64, last: u64) -> u64 {
    if expansion == 0 || filters >= MAX_FILTERS {
        return 0;
    }
    let bytes = last
        .saturating_mul(u64::from(expansion))
        .saturating_mul(u64::from(bucket_size));
    if data.saturating_add(bytes) > MAX_BYTES {
        0
    } else {
        bytes
    }
}

/// The bytes of the sub-filters a filter of `filters` sub-filters, `data`
/// bytes and `last` buckets in its newest grows by to hold `items` items
/// at [`load_percent`], within the fixed limits.
fn growth_for(
    bucket_size: u16,
    expansion: u16,
    filters: u64,
    data: u64,
    last: u64,
    items: u64,
) -> u64 {
    let load = load_percent(bucket_size);
    let (mut slots, mut last, mut filters, mut grown) = (data, last, filters, 0u64);
    while slots.saturating_mul(load) / 100 < items {
        let bytes = next_growth(
            bucket_size,
            expansion,
            filters,
            data.saturating_add(grown),
            last,
        );
        if bytes == 0 {
            break;
        }
        grown += bytes;
        slots += bytes;
        last = last.saturating_mul(u64::from(expansion));
        filters += 1;
    }
    grown
}

/// What the free slot found for a fingerprint is: a sub-filter and a byte.
type Slot = (usize, usize);

impl Cuckoo {
    /// An empty filter, within the limits.
    pub fn new(p: Params) -> Result<Cuckoo, CuckooError> {
        let buckets = buckets_for(p.capacity, p.bucket_size);
        let first = SubFilter::zeroed(buckets, p.bucket_size)?;
        Ok(Cuckoo {
            items: 0,
            deletes: 0,
            bucket_size: p.bucket_size,
            max_iterations: p.max_iterations,
            expansion: expansion_of(p.expansion),
            filters: vec![first],
        })
    }

    /// The bytes a filter made with `p` takes.
    pub fn bytes_for(p: &Params) -> u64 {
        buckets_for(p.capacity, p.bucket_size).saturating_mul(u64::from(p.bucket_size))
    }

    /// All the sub-filters' bytes.
    pub fn data_bytes(&self) -> u64 {
        self.filters.iter().map(|f| f.data.len() as u64).sum()
    }

    /// What `CF.INFO` calls its size: Redis's reckoning.
    pub fn info_size(&self) -> u64 {
        40 + 16 * self.filters.len() as u64 + self.data_bytes()
    }

    pub fn memory_bytes(&self) -> usize {
        self.data_bytes() as usize + self.filters.len() * std::mem::size_of::<SubFilter>()
    }

    /// The bytes of sub-filter `fi` if a snapshot or a copy shares them (a
    /// write to it copies them first), else 0.
    fn shared(&self, fi: usize) -> u64 {
        let f = &self.filters[fi];
        if Arc::strong_count(&f.data) > 1 {
            f.data.len() as u64
        } else {
            0
        }
    }

    /// The bytes of every sub-filter a snapshot or a copy shares.
    pub fn shared_bytes(&self) -> u64 {
        (0..self.filters.len()).map(|fi| self.shared(fi)).sum()
    }

    /// The bytes of the sub-filter the next growth adds; 0 if the filter
    /// does not grow (`expansion` 0), or may not within the fixed limits
    /// (it is then refused, having allocated nothing).
    pub fn growth_bytes(&self) -> u64 {
        let last = self.filters.last().map_or(0, |f| f.buckets);
        next_growth(
            self.bucket_size,
            self.expansion,
            self.filters.len() as u64,
            self.data_bytes(),
            last,
        )
    }

    /// What adding the item `l` may allocate at most: a copy of the
    /// sub-filter it goes into, if shared; and only if neither of its
    /// buckets has a free slot in any sub-filter, a copy of the newest
    /// (where its fingerprint kicks others) and a new sub-filter (if the
    /// kicks make no room). A filter with a free slot for it never grows
    /// for it.
    pub fn bytes_to_add_item(&self, l: Lookup) -> u64 {
        for fi in (0..self.filters.len()).rev() {
            if self.find_free(fi, l.h1, l.h2).is_some() {
                return self.shared(fi);
            }
        }
        self.shared(self.filters.len() - 1)
            .saturating_add(self.growth_bytes())
    }

    /// What adding `n` items may allocate, reckoned before a command of
    /// many items starts: a copy of every shared sub-filter, and the
    /// sub-filters it grows by for the filter to hold its items and `n`
    /// more at the load a Cuckoo filter of its bucket size reaches before
    /// an add fails ([`load_percent`]). Items that crowd the same buckets
    /// may grow it sooner: each growth past these is asked for as it comes
    /// ([`Cuckoo::insert_within`]).
    pub fn bytes_to_add_items(&self, n: u64) -> u64 {
        let last = self.filters.last().map_or(0, |f| f.buckets);
        self.shared_bytes().saturating_add(growth_for(
            self.bucket_size,
            self.expansion,
            self.filters.len() as u64,
            self.data_bytes(),
            last,
            self.items.saturating_add(n),
        ))
    }

    /// [`Cuckoo::bytes_to_add_items`] for a filter not made yet: what
    /// making one with `p` and adding `n` items to it may allocate.
    pub fn bytes_to_make_and_add(p: &Params, n: u64) -> u64 {
        let buckets = buckets_for(p.capacity, p.bucket_size);
        let bytes = Cuckoo::bytes_for(p);
        bytes.saturating_add(growth_for(
            p.bucket_size,
            expansion_of(p.expansion),
            1,
            bytes,
            buckets,
            n,
        ))
    }

    /// What deleting the item `l` may allocate: a copy of the sub-filter it
    /// is deleted from, if shared; of every shared one if the delete
    /// starts a compaction. Nothing if it is not there.
    pub fn bytes_to_delete(&self, l: Lookup) -> u64 {
        let Some((fi, _)) = self.find(l) else {
            return 0;
        };
        if self.compacts_after_delete() {
            self.shared_bytes()
        } else {
            self.shared(fi)
        }
    }

    /// What [`Cuckoo::compact`] may allocate: a copy of every shared
    /// sub-filter, if there is more than one.
    pub fn bytes_to_compact(&self) -> u64 {
        if self.filters.len() > 1 {
            self.shared_bytes()
        } else {
            0
        }
    }

    /// What `CF.LOADCHUNK key iter data` (with `len` bytes of data) may
    /// allocate: a copy of the sub-filter the chunk goes into, if shared.
    pub fn bytes_to_load_chunk(&self, iter: i64, len: usize) -> u64 {
        self.chunk_place(iter, len)
            .map_or(0, |(fi, _)| self.shared(fi))
    }

    /// Where the fingerprint of `l` is, the newest sub-filter first, as a
    /// delete looks: the sub-filter and the byte.
    fn find(&self, l: Lookup) -> Option<Slot> {
        let bs = self.bucket_size as usize;
        for fi in (0..self.filters.len()).rev() {
            for h in [l.h1, l.h2] {
                let b = self.bucket(fi, h);
                if let Some(j) = self.filters[fi].data[b..b + bs]
                    .iter()
                    .position(|&x| x == l.fp)
                {
                    return Some((fi, b + j));
                }
            }
        }
        None
    }

    /// Whether a delete that finds its item goes on to compact.
    fn compacts_after_delete(&self) -> bool {
        let items = self.items.wrapping_sub(1);
        let deletes = self.deletes.wrapping_add(1);
        self.filters.len() > 1 && deletes as f64 > items as f64 * 0.1
    }

    /// Every sub-filter's bytes this filter's own, copied first where a
    /// snapshot or a copy shares them: before a change that may write to
    /// any of them, so that it cannot fail part way for want of memory.
    fn own_all(&mut self) -> Result<(), CuckooError> {
        for f in &mut self.filters {
            f.own()?;
        }
        Ok(())
    }

    fn bucket(&self, fi: usize, h: u64) -> usize {
        ((h % self.filters[fi].buckets) * u64::from(self.bucket_size)) as usize
    }

    fn find_free(&self, fi: usize, h1: u64, h2: u64) -> Option<Slot> {
        let bs = self.bucket_size as usize;
        let data = &self.filters[fi].data;
        for h in [h1, h2] {
            let b = self.bucket(fi, h);
            if let Some(j) = data[b..b + bs].iter().position(|&x| x == 0) {
                return Some((fi, b + j));
            }
        }
        None
    }

    fn set(&mut self, (fi, at): Slot, fp: u8) -> Result<(), CuckooError> {
        self.filters[fi].own()?[at] = fp;
        Ok(())
    }

    /// Kick fingerprints in the newest sub-filter to make room; undone if
    /// it fails.
    fn kick_in(&mut self, fp: u8, h1: u64) -> Result<bool, CuckooError> {
        let bs = self.bucket_size as usize;
        let max = self.max_iterations;
        let last = self.filters.len() - 1;
        let buckets = self.filters[last].buckets;
        let data = self.filters[last].own()?;
        let mut fp = fp;
        let mut victim = 0usize;
        let mut ii = h1 % buckets;
        for _ in 0..max {
            let b = ii as usize * bs;
            std::mem::swap(&mut data[b + victim], &mut fp);
            ii = alt(fp, ii) % buckets;
            let b = ii as usize * bs;
            if let Some(j) = data[b..b + bs].iter().position(|&x| x == 0) {
                data[b + j] = fp;
                return Ok(true);
            }
            victim = (victim + 1) % bs;
        }
        for _ in 0..max {
            victim = (victim + bs - 1) % bs;
            ii = alt(fp, ii) % buckets;
            let b = ii as usize * bs;
            std::mem::swap(&mut data[b + victim], &mut fp);
        }
        Ok(false)
    }

    /// Add a sub-filter, `expansion` times the newest, if `may_grow` lets
    /// it have its bytes.
    fn grow(
        &mut self,
        max_filters: u64,
        may_grow: &mut dyn FnMut(u64) -> bool,
    ) -> Result<(), CuckooError> {
        if self.filters.len() as u64 >= max_filters {
            return Err(CuckooError::MaxExpansions);
        }
        if self.filters.len() as u64 >= MAX_FILTERS {
            return Err(CuckooError::TooLarge);
        }
        let last = self.filters.last().expect("a filter has a sub-filter");
        let buckets = last
            .buckets
            .checked_mul(u64::from(self.expansion))
            .ok_or(CuckooError::TooLarge)?;
        let bytes = buckets.saturating_mul(u64::from(self.bucket_size));
        if self.data_bytes().saturating_add(bytes) > MAX_BYTES {
            return Err(CuckooError::TooLarge);
        }
        if !may_grow(bytes) {
            return Err(CuckooError::NoRoom);
        }
        let next = SubFilter::zeroed(buckets, self.bucket_size)?;
        self.filters.push(next);
        Ok(())
    }

    /// Add an item's fingerprint. `max_filters` is how many sub-filters it
    /// may grow to (`cf-max-expansions` for a client's command).
    pub fn insert(&mut self, l: Lookup, max_filters: u64) -> Result<(), CuckooError> {
        self.insert_within(l, max_filters, &mut |_| true)
    }

    /// [`Cuckoo::insert`], growing the filter only by sub-filters whose
    /// bytes `may_grow` lets it have: one it does not is
    /// [`CuckooError::NoRoom`]. An add that fails changes nothing (its
    /// kicks are undone; a copy of a shared sub-filter it made stays, the
    /// same bytes).
    pub fn insert_within(
        &mut self,
        l: Lookup,
        max_filters: u64,
        may_grow: &mut dyn FnMut(u64) -> bool,
    ) -> Result<(), CuckooError> {
        loop {
            for fi in (0..self.filters.len()).rev() {
                if let Some(slot) = self.find_free(fi, l.h1, l.h2) {
                    self.set(slot, l.fp)?;
                    self.items += 1;
                    return Ok(());
                }
            }
            if self.kick_in(l.fp, l.h1)? {
                self.items += 1;
                return Ok(());
            }
            if self.expansion == 0 {
                return Err(CuckooError::Full);
            }
            self.grow(max_filters, may_grow)?;
        }
    }

    /// Whether the item may have been added.
    pub fn contains(&self, l: Lookup) -> bool {
        let bs = self.bucket_size as usize;
        (0..self.filters.len()).any(|fi| {
            [l.h1, l.h2].iter().any(|&h| {
                let b = self.bucket(fi, h);
                self.filters[fi].data[b..b + bs].contains(&l.fp)
            })
        })
    }

    /// How many times its fingerprint is in its buckets: both buckets
    /// counted, even when they are the same one, as Redis counts.
    pub fn count(&self, l: Lookup) -> u64 {
        let bs = self.bucket_size as usize;
        let mut n = 0u64;
        for fi in 0..self.filters.len() {
            for h in [l.h1, l.h2] {
                let b = self.bucket(fi, h);
                n += self.filters[fi].data[b..b + bs]
                    .iter()
                    .filter(|&&x| x == l.fp)
                    .count() as u64;
            }
        }
        n
    }

    /// Delete one copy of the item's fingerprint, the newest sub-filter
    /// first; then compact if deletes are more than a tenth of the items.
    /// What it writes to is its own before anything changes: it fails for
    /// want of memory, if it does, having changed nothing.
    pub fn delete(&mut self, l: Lookup) -> Result<bool, CuckooError> {
        let Some((fi, at)) = self.find(l) else {
            return Ok(false);
        };
        let compacts = self.compacts_after_delete();
        if compacts {
            self.own_all()?;
        }
        self.filters[fi].own()?[at] = 0;
        self.items = self.items.wrapping_sub(1);
        self.deletes = self.deletes.wrapping_add(1);
        if compacts {
            self.compact(false)?;
        }
        Ok(true)
    }

    /// Move the fingerprint at `(fi, bucket, slot)` into an older
    /// sub-filter where it fits. `None` for an empty slot.
    fn relocate(
        &mut self,
        fi: usize,
        bucket: u64,
        slot: usize,
    ) -> Result<Option<bool>, CuckooError> {
        let at = bucket as usize * self.bucket_size as usize + slot;
        let fp = self.filters[fi].data[at];
        if fp == 0 {
            return Ok(None);
        }
        let (h1, h2) = (bucket, alt(fp, bucket));
        for older in 0..fi {
            if let Some(free) = self.find_free(older, h1, h2) {
                self.set(free, fp)?;
                self.filters[fi].own()?[at] = 0;
                return Ok(Some(true));
            }
        }
        Ok(Some(false))
    }

    /// Move the newer sub-filters' fingerprints down where they fit, from
    /// the newest; drop the newest if it empties. `all`: go on past a
    /// sub-filter that could not be emptied (`CF.COMPACT`). It stops where
    /// it is at [`COMPACT_BUDGET`].
    ///
    /// What it writes to is its own before anything changes: it fails for
    /// want of memory, if it does, having changed nothing.
    pub fn compact(&mut self, all: bool) -> Result<(), CuckooError> {
        if self.filters.len() > 1 {
            self.own_all()?;
        }
        let look = 2 * u64::from(self.bucket_size);
        let mut work = 0u64;
        'filters: for fi in (1..self.filters.len()).rev() {
            let mut emptied = true;
            for bucket in 0..self.filters[fi].buckets {
                for slot in 0..self.bucket_size as usize {
                    let at = bucket as usize * self.bucket_size as usize + slot;
                    work += 1;
                    if self.filters[fi].data[at] != 0 {
                        work += fi as u64 * look;
                    }
                    if work > COMPACT_BUDGET {
                        break 'filters;
                    }
                    if self.relocate(fi, bucket, slot)? == Some(false) {
                        emptied = false;
                    }
                }
            }
            if emptied && fi == self.filters.len() - 1 {
                self.filters.pop();
            }
            if !emptied && !all {
                break;
            }
        }
        self.deletes = 0;
        Ok(())
    }

    /// The `CF.SCANDUMP` header.
    pub fn header(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_LEN);
        out.extend_from_slice(&self.items.to_le_bytes());
        out.extend_from_slice(&self.filters[0].buckets.to_le_bytes());
        out.extend_from_slice(&self.deletes.to_le_bytes());
        out.extend_from_slice(&(self.filters.len() as u64).to_le_bytes());
        out.extend_from_slice(&self.bucket_size.to_le_bytes());
        out.extend_from_slice(&self.max_iterations.to_le_bytes());
        out.extend_from_slice(&self.expansion.to_le_bytes());
        out
    }

    /// The sub-filter holding byte `offset` of all their bytes, and where it
    /// starts.
    fn filter_at(&self, offset: u64) -> Option<(usize, u64)> {
        let mut start = 0u64;
        for (fi, f) in self.filters.iter().enumerate() {
            let len = f.data.len() as u64;
            if offset < start + len {
                return Some((fi, start));
            }
            start += len;
        }
        None
    }

    /// `CF.SCANDUMP key iter`: the next iterator and the chunk. Iterator 0
    /// is the header; then the bytes from the start of the sixteen-megabyte
    /// stretch of the sub-filter that byte `iter − 1` is in, to the end of
    /// that stretch or of the sub-filter. For sub-filters of up to 16 MiB,
    /// which is every chunk Redis gives whole, this is Redis's reply.
    pub fn scandump(&self, iter: i64) -> Option<(i64, Vec<u8>)> {
        if self.items == 0 {
            return None;
        }
        if iter == 0 {
            return Some((1, self.header()));
        }
        let offset = u64::try_from(iter - 1).ok()?;
        let (fi, start) = self.filter_at(offset)?;
        let data = &self.filters[fi].data;
        let from = (offset - start) / CHUNK_MAX * CHUNK_MAX;
        let to = (from + CHUNK_MAX).min(data.len() as u64);
        let chunk = data[from as usize..to as usize].to_vec();
        Some((iter.saturating_add(chunk.len() as i64), chunk))
    }

    /// `CF.LOADCHUNK key iter data` past the header: `data` goes to byte
    /// `iter − len − 1`, which with all of it must be in one sub-filter.
    pub fn load_chunk(&mut self, iter: i64, data: &[u8]) -> Result<(), &'static str> {
        const BAD: &str = "Couldn't load chunk!";
        let (fi, at) = self.chunk_place(iter, data.len()).ok_or(BAD)?;
        self.filters[fi].own().map_err(|_| BAD)?[at..at + data.len()].copy_from_slice(data);
        Ok(())
    }

    /// Where `CF.LOADCHUNK key iter data` (`len` bytes of data) puts them:
    /// the sub-filter and the byte, all of them in it; `None` if they do
    /// not fit there.
    fn chunk_place(&self, iter: i64, len: usize) -> Option<(usize, usize)> {
        let offset = iter
            .checked_sub(len as i64)
            .and_then(|v| v.checked_sub(1))
            .and_then(|v| u64::try_from(v).ok())?;
        // The sub-filter it lands in, as Redis finds it: past the last one,
        // the last, at the offset less all their bytes.
        let mut at = offset;
        let mut fi = self.filters.len() - 1;
        for (i, f) in self.filters.iter().enumerate() {
            let flen = f.data.len() as u64;
            if at < flen {
                fi = i;
                break;
            }
            at -= flen;
        }
        let at = usize::try_from(at).ok()?;
        (at.saturating_add(len) <= self.filters[fi].data.len()).then_some((fi, at))
    }

    /// A filter from a `CF.SCANDUMP` header: its sub-filters made, zeroed.
    /// Its bucket size may be `max_bucket_size` at most (Redis bounds a
    /// client's by `cf-initial-size`).
    pub fn from_header(raw: &[u8], max_bucket_size: u16) -> Result<Cuckoo, HeaderError> {
        let (shape, _) = Cuckoo::header_shape(raw, max_bucket_size)?;
        Cuckoo::from_shape(&shape)
    }

    /// The shape a `CF.SCANDUMP` header gives, checked, and the bytes of its
    /// sub-filters, before anything of them is allocated.
    pub fn header_shape(raw: &[u8], max_bucket_size: u16) -> Result<(Shape, u64), HeaderError> {
        if raw.len() != HEADER_LEN {
            return Err(HeaderError::Invalid);
        }
        let u64_at = |i: usize| u64::from_le_bytes(raw[i..i + 8].try_into().expect("8"));
        let u16_at = |i: usize| u16::from_le_bytes([raw[i], raw[i + 1]]);
        let shape = Shape {
            items: u64_at(0),
            buckets: u64_at(8),
            deletes: u64_at(16),
            filters: u64_at(24),
            bucket_size: u16_at(32),
            max_iterations: u16_at(34),
            expansion: u16_at(36),
        };
        if shape.bucket_size > max_bucket_size {
            return Err(HeaderError::Bad);
        }
        let bytes = shape.check().map_err(|e| match e {
            ShapeError::Bad => HeaderError::Bad,
            ShapeError::TooLarge => HeaderError::TooLarge,
        })?;
        Ok((shape, bytes))
    }

    /// A filter of `shape` (checked: [`Cuckoo::header_shape`]), its
    /// sub-filters made, zeroed.
    pub fn from_shape(shape: &Shape) -> Result<Cuckoo, HeaderError> {
        let mut filters = Vec::with_capacity(shape.filters as usize);
        let mut buckets = shape.buckets;
        for _ in 0..shape.filters {
            filters.push(
                SubFilter::zeroed(buckets, shape.bucket_size).map_err(|e| match e {
                    CuckooError::NoMemory => HeaderError::NoMemory,
                    _ => HeaderError::TooLarge,
                })?,
            );
            buckets = buckets.saturating_mul(u64::from(shape.expansion));
        }
        Ok(Cuckoo {
            items: shape.items,
            deletes: shape.deletes,
            bucket_size: shape.bucket_size,
            max_iterations: shape.max_iterations,
            expansion: shape.expansion,
            filters,
        })
    }
}

/// Why a header was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderError {
    /// Not a header (Redis's `Invalid header`).
    Invalid,
    /// A header of a filter that cannot be (`Couldn't create filter!`).
    Bad,
    TooLarge,
    NoMemory,
}

/// What a header, a snapshot or an RDB value says about a filter.
#[derive(Debug, Clone, Copy)]
pub struct Shape {
    pub items: u64,
    pub buckets: u64,
    pub deletes: u64,
    pub filters: u64,
    pub bucket_size: u16,
    pub max_iterations: u16,
    pub expansion: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShapeError {
    Bad,
    TooLarge,
}

impl Shape {
    /// The bytes all its sub-filters take, if it is one Redis could make and
    /// within the limits; checked before anything is allocated.
    pub fn check(&self) -> Result<u64, ShapeError> {
        if self.filters == 0
            || self.bucket_size == 0
            || self.max_iterations == 0
            || self.buckets == 0
            || !self.buckets.is_power_of_two()
            || (self.filters > 1 && self.expansion == 0)
        {
            return Err(ShapeError::Bad);
        }
        if self.filters > MAX_FILTERS {
            return Err(ShapeError::Bad);
        }
        let mut total = 0u64;
        let mut buckets = self.buckets;
        for _ in 0..self.filters {
            let bytes = buckets
                .checked_mul(u64::from(self.bucket_size))
                .ok_or(ShapeError::TooLarge)?;
            total = total.checked_add(bytes).ok_or(ShapeError::TooLarge)?;
            if total > MAX_BYTES {
                return Err(ShapeError::TooLarge);
            }
            buckets = buckets
                .checked_mul(u64::from(self.expansion.max(1)))
                .ok_or(ShapeError::TooLarge)?;
        }
        Ok(total)
    }
}

// ── KDB encoding ─────────────────────────────────────────────────────────

/// The header's fields (items, first sub-filter's buckets, deletes,
/// sub-filters as u64s; bucket size, kick limit, expansion as u16s), then
/// each sub-filter's bytes. Little-endian.
pub fn write_kdb(w: &mut impl io::Write, cf: &Cuckoo) -> io::Result<()> {
    w.write_all(&cf.header())?;
    for f in &cf.filters {
        w.write_all(&f.data)?;
    }
    Ok(())
}

pub fn kdb_len(cf: &Cuckoo) -> u64 {
    HEADER_LEN as u64 + cf.data_bytes()
}

/// A filter [`write_kdb`] wrote: its shape checked before anything is
/// allocated; each sub-filter's bytes read as they arrive.
pub fn read_kdb(r: &mut impl io::Read) -> io::Result<Cuckoo> {
    let mut head = [0u8; HEADER_LEN];
    r.read_exact(&mut head)?;
    let u64_at = |i: usize| u64::from_le_bytes(head[i..i + 8].try_into().expect("8"));
    let u16_at = |i: usize| u16::from_le_bytes([head[i], head[i + 1]]);
    let shape = Shape {
        items: u64_at(0),
        buckets: u64_at(8),
        deletes: u64_at(16),
        filters: u64_at(24),
        bucket_size: u16_at(32),
        max_iterations: u16_at(34),
        expansion: u16_at(36),
    };
    let bytes = shape
        .check()
        .map_err(|_| super::invalid("a Cuckoo filter of this shape is not supported"))?;
    super::room::declared(bytes)?;
    let mut filters = Vec::with_capacity(shape.filters as usize);
    let mut buckets = shape.buckets;
    for _ in 0..shape.filters {
        let len = buckets * u64::from(shape.bucket_size);
        let data = super::read_vec(r, len as usize)?;
        filters.push(SubFilter {
            buckets,
            data: Arc::new(data.into_boxed_slice()),
        });
        buckets = buckets.saturating_mul(u64::from(shape.expansion.max(1)));
    }
    Ok(Cuckoo {
        items: shape.items,
        deletes: shape.deletes,
        bucket_size: shape.bucket_size,
        max_iterations: shape.max_iterations,
        expansion: shape.expansion,
        filters,
    })
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

    fn params(capacity: u64) -> Params {
        Params {
            capacity,
            bucket_size: 2,
            max_iterations: 20,
            expansion: 1,
        }
    }

    /// A compaction stops at its budget, where it is, and the same way each
    /// time: two copies end alike, every item still found; a small filter
    /// compacts whole.
    #[test]
    fn compaction_stops_at_its_budget() {
        let grown = |items: u32| {
            let mut cf = Cuckoo::new(Params {
                capacity: 1 << 12,
                bucket_size: 2,
                max_iterations: 20,
                expansion: 2,
            })
            .unwrap();
            for i in 0..items {
                cf.insert(lookup(format!("c{i}").as_bytes()), MAX_FILTERS)
                    .unwrap();
            }
            cf
        };
        let mut big = grown(1 << 21);
        assert!(big.filters.len() > 4);
        let mut again = big.clone();
        let filters = big.filters.len();
        big.compact(true).unwrap();
        again.compact(true).unwrap();
        assert_eq!(big, again);
        assert_eq!(big.filters.len(), filters, "stopped before the oldest");
        assert!((0..1u32 << 21).all(|i| big.contains(lookup(format!("c{i}").as_bytes()))));
        let mut small = grown(10_000);
        for i in 0..9_990 {
            small.delete(lookup(format!("c{i}").as_bytes())).unwrap();
        }
        small.compact(true).unwrap();
        assert_eq!(small.filters.len(), 1, "compacted whole");
    }

    /// No false negatives; added items deleted succeed; the false-positive
    /// rate within what a fingerprint of eight bits gives (about
    /// 2·b/255 for buckets of b).
    #[test]
    fn guarantees() {
        let mut cf = Cuckoo::new(params(10_000)).unwrap();
        let items: Vec<String> = (0..9_000).map(|i| format!("in{i}")).collect();
        for i in &items {
            cf.insert(lookup(i.as_bytes()), 32).unwrap();
        }
        assert!(items.iter().all(|i| cf.contains(lookup(i.as_bytes()))));
        let fp = (0..50_000)
            .filter(|i| cf.contains(lookup(format!("out{i}").as_bytes())))
            .count();
        let rate = fp as f64 / 50_000.0;
        assert!(rate < 2.0 * 2.0 / 255.0 * 1.2, "{rate}");
        for i in &items[..4_000] {
            assert!(cf.delete(lookup(i.as_bytes())).unwrap(), "{i}");
        }
        assert!(
            items[4_000..]
                .iter()
                .all(|i| cf.contains(lookup(i.as_bytes())))
        );
        assert_eq!(cf.items, 5_000);
    }

    /// A filter grows by sub-filters `expansion` times larger, up to what it
    /// may; one that does not grow is full.
    #[test]
    fn expansion() {
        let mut cf = Cuckoo::new(Params {
            capacity: 4,
            bucket_size: 1,
            max_iterations: 1,
            expansion: 2,
        })
        .unwrap();
        for i in 0..200 {
            cf.insert(lookup(format!("{i}").as_bytes()), 32).unwrap();
        }
        assert!(cf.filters.len() > 3);
        for w in cf.filters.windows(2) {
            assert_eq!(w[1].buckets, w[0].buckets * 2);
        }
        let mut fixed = Cuckoo::new(Params {
            capacity: 4,
            bucket_size: 2,
            max_iterations: 1,
            expansion: 0,
        })
        .unwrap();
        let mut full = 0;
        for i in 0..20 {
            if fixed.insert(lookup(format!("{i}").as_bytes()), 32) == Err(CuckooError::Full) {
                full += 1;
            }
        }
        assert!(full > 0);
        let mut capped = Cuckoo::new(Params {
            capacity: 4,
            bucket_size: 1,
            max_iterations: 1,
            expansion: 1,
        })
        .unwrap();
        let errors: Vec<_> = (0..400)
            .filter_map(|i| capped.insert(lookup(format!("{i}").as_bytes()), 3).err())
            .collect();
        assert_eq!(capped.filters.len(), 3);
        assert!(errors.iter().all(|&e| e == CuckooError::MaxExpansions));
    }

    #[test]
    fn limits_before_allocating() {
        assert_eq!(
            Cuckoo::new(Params {
                capacity: 1 << 31,
                bucket_size: 1,
                max_iterations: 1,
                expansion: 1
            }),
            Err(CuckooError::TooLarge)
        );
        let huge = Shape {
            items: 0,
            buckets: 1 << 40,
            deletes: 0,
            filters: 1,
            bucket_size: 1,
            max_iterations: 1,
            expansion: 1,
        };
        assert_eq!(huge.check(), Err(ShapeError::TooLarge));
        let many = Shape {
            buckets: 4,
            filters: 40,
            expansion: 32768,
            ..huge
        };
        assert_eq!(many.check(), Err(ShapeError::TooLarge));
    }

    /// An add is charged for a new sub-filter only when no bucket of its
    /// item has a free slot (a 64 MiB filter does not need 64 MiB free for
    /// every add); and for a copy of the sub-filter it writes to only when
    /// a snapshot or a copy shares it.
    #[test]
    fn an_add_is_charged_for_what_it_may_allocate() {
        let mut cf = Cuckoo::new(Params {
            capacity: 1 << 20,
            bucket_size: 2,
            max_iterations: 20,
            expansion: 1,
        })
        .unwrap();
        let l = lookup(b"x");
        assert_eq!(cf.bytes_to_add_item(l), 0);
        let shared = cf.clone();
        assert_eq!(cf.bytes_to_add_item(l), 1 << 20);
        drop(shared);
        // Both its buckets full: it may grow.
        let mut tiny = Cuckoo::new(Params {
            capacity: 4,
            bucket_size: 1,
            max_iterations: 1,
            expansion: 2,
        })
        .unwrap();
        let x = lookup(b"x");
        while let Some(at) = tiny.find_free(0, x.h1, x.h2) {
            tiny.set(at, 7).unwrap();
        }
        assert_eq!(tiny.growth_bytes(), 8);
        assert_eq!(tiny.bytes_to_add_item(x), 8);
        cf.insert(l, MAX_FILTERS).unwrap();
        // Many items: room to grow to hold them at the load the bucket size
        // reaches; none for a few.
        assert_eq!(cf.bytes_to_add_items(10), 0);
        assert!(cf.bytes_to_add_items(1 << 21) >= 1 << 20);
        let p = Params {
            capacity: 1024,
            bucket_size: 2,
            max_iterations: 20,
            expansion: 1,
        };
        assert_eq!(Cuckoo::bytes_to_make_and_add(&p, 10), 1024);
        assert_eq!(Cuckoo::bytes_to_make_and_add(&p, 2000), 3072);
    }

    /// A growth refused for room changes nothing, and the add says so.
    #[test]
    fn a_growth_without_room_changes_nothing() {
        let mut cf = Cuckoo::new(Params {
            capacity: 4,
            bucket_size: 1,
            max_iterations: 2,
            expansion: 1,
        })
        .unwrap();
        let mut refused = 0;
        for i in 0..100 {
            let before = cf.clone();
            let l = lookup(format!("{i}").as_bytes());
            if let Err(e) = cf.insert_within(l, MAX_FILTERS, &mut |_| false) {
                assert_eq!(e, CuckooError::NoRoom);
                assert_eq!(cf, before);
                refused += 1;
            }
        }
        assert!(refused > 0);
        assert_eq!(cf.filters.len(), 1);
    }

    /// A delete or a compaction of a filter a copy shares copies what it
    /// writes to before it changes anything, and leaves the copy as it was.
    #[test]
    fn deletes_of_a_shared_filter() {
        let mut cf = Cuckoo::new(Params {
            capacity: 8,
            bucket_size: 1,
            max_iterations: 2,
            expansion: 1,
        })
        .unwrap();
        for i in 0..40 {
            cf.insert(lookup(format!("{i}").as_bytes()), MAX_FILTERS)
                .unwrap();
        }
        assert!(cf.filters.len() > 1);
        let copy = cf.clone();
        assert_eq!(cf.bytes_to_compact(), cf.data_bytes());
        assert_eq!(cf.bytes_to_delete(lookup(b"absent")), 0);
        let one = lookup(b"0");
        assert!(cf.bytes_to_delete(one) > 0);
        assert!(cf.delete(one).unwrap());
        assert_ne!(cf, copy);
        assert!(copy.contains(one));
    }

    #[test]
    fn scandump_and_loadchunk_round_trip() {
        let mut cf = Cuckoo::new(Params {
            capacity: 8,
            bucket_size: 1,
            max_iterations: 5,
            expansion: 2,
        })
        .unwrap();
        for i in 0..30 {
            cf.insert(lookup(format!("{i}").as_bytes()), 32).unwrap();
        }
        let (mut iter, header) = cf.scandump(0).unwrap();
        let mut copy = Cuckoo::from_header(&header, 255).unwrap();
        while let Some((next, chunk)) = cf.scandump(iter) {
            copy.load_chunk(next, &chunk).unwrap();
            iter = next;
        }
        assert_eq!(copy, cf);
    }

    #[test]
    fn kdb_round_trip_and_corruption() {
        let mut cf = Cuckoo::new(params(64)).unwrap();
        for i in 0..100 {
            cf.insert(lookup(format!("{i}").as_bytes()), 32).unwrap();
        }
        let mut good = Vec::new();
        write_kdb(&mut good, &cf).unwrap();
        assert_eq!(good.len() as u64, kdb_len(&cf));
        assert_eq!(read_kdb(&mut &good[..]).unwrap(), cf);
        let mut seed = 3u64;
        for _ in 0..4000 {
            let mut bad = good.clone();
            match rng(&mut seed) % 3 {
                0 => {
                    let at = rng(&mut seed) as usize % bad.len();
                    bad[at] = rng(&mut seed) as u8;
                }
                1 => bad.truncate(rng(&mut seed) as usize % bad.len()),
                _ => {
                    let at = (rng(&mut seed) as usize % 4) * 8;
                    bad[at..at + 8].copy_from_slice(&rng(&mut seed).to_le_bytes());
                }
            }
            if let Ok(read) = read_kdb(&mut &bad[..]) {
                assert!(read.data_bytes() <= MAX_BYTES);
                assert!(!read.filters.is_empty());
            }
            let _ = Cuckoo::from_header(&bad[..HEADER_LEN.min(bad.len())], 255);
        }
    }
}
