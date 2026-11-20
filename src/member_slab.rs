//! Sorted-set member bytes, stored once and named by generation-checked handles.

use std::fmt;

const UNIT: usize = 8;
const HEADER: usize = 8;
pub const PAGE: usize = 64 << 10;
const UNIT_BITS: u32 = 13;
const _: () = assert!(PAGE / UNIT == 1 << UNIT_BITS);
// Small first pages keep a set of a few members from holding 64 KiB.
const FIRST_PAGE: usize = 256;
const SMALL_RECORD_MAX: usize = 4096;
pub const SMALL_MAX: usize = SMALL_RECORD_MAX - HEADER;
/// Location flag for a member stored in its own allocation.
const LARGE: u32 = 1 << 31;
// The location bits left beside the flag and the unit: 16 GiB of pages.
const MAX_PAGES: usize = 1 << (31 - UNIT_BITS);
const MAX_LARGE: usize = LARGE as usize;

/// Valid only in the slab that issued it; another slab may resolve it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct MemberHandle {
    /// `page << UNIT_BITS | unit`, or `LARGE | index` for a large member.
    loc: u32,
    /// Odd: the record's generation when this member was stored.
    generation: u32,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct SlabFull;

impl fmt::Display for SlabFull {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("member slab is full")
    }
}

impl std::error::Error for SlabFull {}

#[derive(Clone)]
struct Page {
    bytes: Box<[u8]>,
    /// Units handed out; only the last page grows.
    carved: u16,
}

#[derive(Clone)]
struct Large {
    generation: u32,
    bytes: Box<[u8]>,
}

/// Bytes held and requested from the allocator, as `memory_use` counts them.
#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
pub struct SlabUsage {
    pub members: usize,
    /// Member bytes without headers or padding.
    pub member_bytes: usize,
    pub page_bytes: usize,
    /// Live small records, headers and padding included.
    pub record_bytes: usize,
    pub large_bytes: usize,
    /// The slab's own vectors at their capacity.
    pub table_bytes: usize,
}

impl SlabUsage {
    pub fn heap_bytes(&self) -> usize {
        self.page_bytes + self.large_bytes + self.table_bytes
    }
}

#[derive(Clone)]
pub struct MemberSlab {
    pages: Vec<Page>,
    /// Freed records by size in units, sorted; sparse so small sets stay small.
    free: Vec<(usize, Vec<u32>)>,
    large: Vec<Large>,
    large_free: Vec<u32>,
    members: usize,
    member_bytes: usize,
    page_bytes: usize,
    record_bytes: usize,
    large_bytes: usize,
    /// Past this many pages, small members take large entries. Lowered by tests.
    page_limit: u32,
    /// Refusing by count alone keeps room independent of history. Lowered by tests.
    member_limit: u32,
}

#[inline]
fn units(len: usize) -> usize {
    (HEADER + len).div_ceil(UNIT)
}

#[inline]
fn split(loc: u32) -> (usize, usize) {
    (
        (loc >> UNIT_BITS) as usize,
        (loc & ((1 << UNIT_BITS) - 1)) as usize * UNIT,
    )
}

#[inline]
fn read_u32(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]])
}

#[inline]
fn write_u32(bytes: &mut [u8], at: usize, value: u32) {
    bytes[at..at + 4].copy_from_slice(&value.to_le_bytes());
}

impl MemberSlab {
    pub fn new() -> Self {
        Self {
            pages: Vec::new(),
            free: Vec::new(),
            large: Vec::new(),
            large_free: Vec::new(),
            members: 0,
            member_bytes: 0,
            page_bytes: 0,
            record_bytes: 0,
            large_bytes: 0,
            page_limit: MAX_PAGES as u32,
            member_limit: MAX_LARGE as u32,
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.members
    }

    /// Copies `member` in; the index, not the slab, keeps members unique.
    pub fn insert(&mut self, member: &[u8]) -> Result<MemberHandle, SlabFull> {
        if self.members >= self.member_limit as usize {
            return Err(SlabFull);
        }
        let handle = if member.len() <= SMALL_MAX {
            self.insert_small(member)
                .or_else(|SlabFull| self.insert_large(member))?
        } else {
            self.insert_large(member)?
        };
        self.members += 1;
        self.member_bytes += member.len();
        Ok(handle)
    }

    fn insert_small(&mut self, member: &[u8]) -> Result<MemberHandle, SlabFull> {
        let size = units(member.len());
        let listed = self.free.binary_search_by_key(&size, |&(units, _)| units);
        let loc = match listed.ok().and_then(|i| self.free[i].1.pop()) {
            Some(loc) => loc,
            None => self.carve(size)?,
        };
        let (page, at) = split(loc);
        let bytes = &mut self.pages[page].bytes;
        // Free records are even and below u32::MAX, so this is odd and cannot wrap.
        let generation = read_u32(bytes, at) + 1;
        write_u32(bytes, at, generation);
        write_u32(bytes, at + 4, member.len() as u32);
        bytes[at + HEADER..at + HEADER + member.len()].copy_from_slice(member);
        self.record_bytes += size * UNIT;
        Ok(MemberHandle { loc, generation })
    }

    fn carve(&mut self, size: usize) -> Result<u32, SlabFull> {
        let fits = self
            .pages
            .last()
            .is_some_and(|page| page.carved as usize + size <= page.bytes.len() / UNIT);
        if !fits {
            if self.pages.len() >= self.page_limit as usize {
                return Err(SlabFull);
            }
            let next = self
                .pages
                .last()
                .map_or(FIRST_PAGE, |page| (page.bytes.len() * 2).min(PAGE));
            let len = next.max((size * UNIT).next_power_of_two());
            self.pages.push(Page {
                bytes: vec![0; len].into_boxed_slice(),
                carved: 0,
            });
            self.page_bytes += len;
        }
        let number = self.pages.len() - 1;
        let page = &mut self.pages[number];
        let unit = page.carved as u32;
        page.carved += size as u16;
        Ok(((number as u32) << UNIT_BITS) | unit)
    }

    fn insert_large(&mut self, member: &[u8]) -> Result<MemberHandle, SlabFull> {
        let index = match self.large_free.pop() {
            Some(index) => index as usize,
            None => {
                if self.large.len() >= MAX_LARGE {
                    return Err(SlabFull);
                }
                self.large.push(Large {
                    generation: 0,
                    bytes: Box::default(),
                });
                self.large.len() - 1
            }
        };
        let entry = &mut self.large[index];
        entry.generation += 1;
        entry.bytes = member.into();
        self.large_bytes += member.len();
        Ok(MemberHandle {
            loc: LARGE | index as u32,
            generation: entry.generation,
        })
    }

    /// Members surely storable: each takes at most one large entry.
    pub fn spare(&self) -> usize {
        let members = (self.member_limit as usize).saturating_sub(self.members);
        members.min(self.large_free.len() + MAX_LARGE - self.large.len())
    }

    #[cfg(test)]
    pub(crate) fn set_member_limit(&mut self, members: u32) {
        self.member_limit = members;
    }

    /// The member's bytes, or `None` once it has been removed.
    pub fn get(&self, handle: MemberHandle) -> Option<&[u8]> {
        // An even generation belongs to a free record, never a live member.
        if handle.generation & 1 == 0 {
            return None;
        }
        if handle.loc & LARGE != 0 {
            let entry = self.large.get((handle.loc & !LARGE) as usize)?;
            return (entry.generation == handle.generation).then_some(&entry.bytes[..]);
        }
        let (page, at) = split(handle.loc);
        // Uncarved bytes stay zero, which never matches a handle's odd generation.
        let records = &self.pages.get(page)?.bytes;
        // One bounds check for both header fields; tied tree comparisons call this.
        let header: [u8; HEADER] = records.get(at..at + HEADER)?.try_into().ok()?;
        let [g0, g1, g2, g3, l0, l1, l2, l3] = header;
        if u32::from_le_bytes([g0, g1, g2, g3]) != handle.generation {
            return None;
        }
        let len = u32::from_le_bytes([l0, l1, l2, l3]) as usize;
        records.get(at + HEADER..)?.get(..len)
    }

    /// Returns false, changing nothing, if the member is already gone.
    pub fn remove(&mut self, handle: MemberHandle) -> bool {
        let Some(len) = self.get(handle).map(<[u8]>::len) else {
            return false;
        };
        if handle.loc & LARGE != 0 {
            let index = handle.loc & !LARGE;
            let entry = &mut self.large[index as usize];
            entry.bytes = Box::default();
            entry.generation = entry.generation.wrapping_add(1);
            // Generation 0 retires the entry, so no old handle can match again.
            if entry.generation != 0 {
                self.large_free.push(index);
            }
            self.large_bytes -= len;
        } else {
            let (page, at) = split(handle.loc);
            let size = units(len);
            // Wrapping to 0 retires the record, so no old handle can match again.
            let generation = handle.generation.wrapping_add(1);
            write_u32(&mut self.pages[page].bytes, at, generation);
            if generation != 0 {
                match self.free.binary_search_by_key(&size, |&(units, _)| units) {
                    Ok(i) => self.free[i].1.push(handle.loc),
                    Err(i) => self.free.insert(i, (size, vec![handle.loc])),
                }
            }
            self.record_bytes -= size * UNIT;
        }
        self.members -= 1;
        self.member_bytes -= len;
        true
    }

    pub fn usage(&self) -> SlabUsage {
        let free_lists = self.free.capacity() * size_of::<(usize, Vec<u32>)>()
            + self
                .free
                .iter()
                .map(|(_, list)| list.capacity() * size_of::<u32>())
                .sum::<usize>();
        SlabUsage {
            members: self.members,
            member_bytes: self.member_bytes,
            page_bytes: self.page_bytes,
            record_bytes: self.record_bytes,
            large_bytes: self.large_bytes,
            table_bytes: self.pages.capacity() * size_of::<Page>()
                + self.large.capacity() * size_of::<Large>()
                + self.large_free.capacity() * size_of::<u32>()
                + free_lists,
        }
    }
}

impl Default for MemberSlab {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for MemberSlab {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemberSlab")
            .field("pages", &self.pages.len())
            .field("large", &self.large.len())
            .field("usage", &self.usage())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::*;

    /// Recomputes every counter and free list from the pages and tables.
    fn check(slab: &MemberSlab) {
        let mut listed = HashSet::new();
        assert!(slab.free.windows(2).all(|w| w[0].0 < w[1].0), "unsorted");
        for (size, list) in &slab.free {
            for &loc in list {
                assert!(listed.insert(loc), "{loc:#x} listed twice");
                assert_eq!(loc & LARGE, 0);
                let (page, at) = split(loc);
                let len = read_u32(&slab.pages[page].bytes, at + 4) as usize;
                assert_eq!(units(len), *size, "{loc:#x} on the wrong list");
            }
        }
        let (mut members, mut member_bytes, mut record_bytes, mut page_bytes) = (0, 0, 0, 0);
        let mut free_seen = 0;
        for (number, page) in slab.pages.iter().enumerate() {
            let len = page.bytes.len();
            assert!(len.is_power_of_two() && (FIRST_PAGE..=PAGE).contains(&len));
            page_bytes += len;
            let carved = page.carved as usize * UNIT;
            assert!(carved <= len);
            let mut at = 0;
            while at < carved {
                let loc = ((number as u32) << UNIT_BITS) | (at / UNIT) as u32;
                let generation = read_u32(&page.bytes, at);
                let member = read_u32(&page.bytes, at + 4) as usize;
                let size = units(member) * UNIT;
                assert!(size <= SMALL_RECORD_MAX && at + size <= carved, "{loc:#x}");
                if generation & 1 == 1 {
                    members += 1;
                    member_bytes += member;
                    record_bytes += size;
                } else if generation != 0 {
                    assert!(listed.contains(&loc), "removed {loc:#x} not listed");
                    free_seen += 1;
                } else {
                    assert!(!listed.contains(&loc), "retired {loc:#x} listed");
                }
                at += size;
            }
            assert_eq!(at, carved, "records of page {number} overrun");
            assert!(
                page.bytes[carved..].iter().all(|&b| b == 0),
                "beyond carved"
            );
        }
        assert_eq!(free_seen, listed.len(), "a listed location is no record");

        let large_listed: HashSet<u32> = slab.large_free.iter().copied().collect();
        assert_eq!(large_listed.len(), slab.large_free.len(), "listed twice");
        let mut large_bytes = 0;
        for (index, entry) in slab.large.iter().enumerate() {
            let index = index as u32;
            if entry.generation & 1 == 1 {
                members += 1;
                member_bytes += entry.bytes.len();
                large_bytes += entry.bytes.len();
                let spilled = slab.pages.len() >= slab.page_limit as usize;
                assert!(
                    entry.bytes.len() > SMALL_MAX || spilled,
                    "small member spilled early"
                );
            } else {
                assert!(entry.bytes.is_empty(), "removed large member kept");
                assert_eq!(large_listed.contains(&index), entry.generation != 0);
            }
        }
        assert!(
            large_listed
                .iter()
                .all(|&i| (i as usize) < slab.large.len())
        );

        let usage = slab.usage();
        assert_eq!(usage.members, members);
        assert_eq!(usage.member_bytes, member_bytes);
        assert_eq!(usage.record_bytes, record_bytes);
        assert_eq!(usage.page_bytes, page_bytes);
        assert_eq!(usage.large_bytes, large_bytes);
        assert_eq!(slab.len(), members);
        assert!(members <= slab.member_limit as usize);
    }

    fn is_large(handle: MemberHandle) -> bool {
        handle.loc & LARGE != 0
    }

    #[test]
    fn empty_and_binary_members_come_back_byte_for_byte() {
        let every_byte: Vec<u8> = (0..=255).collect();
        let members: [&[u8]; 8] = [
            b"",
            b"\0",
            b"\xff",
            b"\xff\xff",
            b"\0\0\0\0\0\0\0\0",
            // Bytes that look like a record header: generation 1, length 8.
            b"\x01\0\0\0\x08\0\0\0",
            "caf\u{e9}".as_bytes(),
            &every_byte,
        ];
        let mut slab = MemberSlab::new();
        let handles: Vec<_> = members.iter().map(|m| slab.insert(m).unwrap()).collect();
        for (handle, member) in handles.iter().zip(members) {
            assert_eq!(slab.get(*handle), Some(member));
        }
        assert_eq!(slab.len(), members.len());
        check(&slab);

        // An empty member still has a record and is removed once.
        assert!(slab.remove(handles[0]));
        assert_eq!(slab.get(handles[0]), None);
        assert!(!slab.remove(handles[0]));
        for (handle, member) in handles.iter().zip(members).skip(1) {
            assert_eq!(slab.get(*handle), Some(member));
        }
        check(&slab);
    }

    #[test]
    fn members_of_every_length_around_the_limits_are_kept_whole() {
        let lengths: Vec<usize> = (0..=40)
            .chain(SMALL_MAX - 9..=SMALL_MAX + 9)
            .chain([PAGE - 1, PAGE, PAGE + 1, 1 << 20])
            .collect();
        let mut slab = MemberSlab::new();
        let mut held = Vec::new();
        for &len in &lengths {
            let member: Vec<u8> = (0..len).map(|i| (i * 31 + len) as u8).collect();
            let handle = slab.insert(&member).unwrap();
            assert_eq!(is_large(handle), len > SMALL_MAX, "length {len}");
            held.push((handle, member));
        }
        check(&slab);
        for (handle, member) in &held {
            assert_eq!(slab.get(*handle), Some(&member[..]));
        }
        // Every other one removed: the rest are untouched.
        for (handle, _) in held.iter().step_by(2) {
            assert!(slab.remove(*handle));
        }
        check(&slab);
        for (i, (handle, member)) in held.iter().enumerate() {
            let want = (i % 2 == 1).then_some(&member[..]);
            assert_eq!(slab.get(*handle), want);
        }
    }

    #[test]
    fn a_removed_members_handle_matches_nothing_again() {
        let mut slab = MemberSlab::new();
        let a = slab.insert(b"alpha").unwrap();
        let keep = slab.insert(b"kept").unwrap();
        assert!(slab.remove(a));
        assert_eq!(slab.get(a), None, "removed");

        // Same-size members reuse the record under a new generation.
        let b = slab.insert(b"bravo").unwrap();
        assert_eq!(b.loc, a.loc, "the record is reused");
        assert_ne!(b.generation, a.generation);
        assert_eq!(slab.get(a), None, "stale, though the record is in use");
        assert!(!slab.remove(a), "a stale handle removes nothing");
        assert_eq!(slab.get(b), Some(&b"bravo"[..]));
        assert_eq!(slab.get(keep), Some(&b"kept"[..]));

        let mut stale = vec![a];
        let mut current = b;
        for round in 0..100u32 {
            assert!(slab.remove(current));
            let member = round.to_le_bytes();
            let next = slab.insert(&member).unwrap();
            assert_eq!(next.loc, a.loc);
            stale.push(current);
            for &old in &stale {
                assert_eq!(slab.get(old), None);
            }
            assert_eq!(slab.get(next), Some(&member[..]));
            current = next;
        }
        check(&slab);
    }

    #[test]
    fn removing_twice_frees_the_record_once() {
        let mut slab = MemberSlab::new();
        let a = slab.insert(b"same").unwrap();
        assert!(slab.remove(a));
        assert!(!slab.remove(a));
        assert!(!slab.remove(a));
        check(&slab);
        // Listed once, so two new members of its size get two records.
        let b = slab.insert(b"size").unwrap();
        let c = slab.insert(b"also").unwrap();
        assert_ne!(b.loc, c.loc);
        assert_eq!(slab.get(b), Some(&b"size"[..]));
        assert_eq!(slab.get(c), Some(&b"also"[..]));
        assert_eq!(slab.len(), 2);
        check(&slab);
    }

    #[test]
    fn a_freed_record_is_reused_only_by_a_member_of_its_size() {
        let mut slab = MemberSlab::new();
        let short = slab.insert(b"ab").unwrap(); // 1 + 1 units
        assert!(slab.remove(short));
        let longer = slab.insert(&[7; 20]).unwrap(); // 4 units
        assert_ne!(longer.loc, short.loc, "too short for it");
        let same = slab.insert(b"xyz").unwrap(); // 2 units, as "ab"
        assert_eq!(same.loc, short.loc);
        assert_eq!(slab.get(longer), Some(&[7; 20][..]));
        check(&slab);
    }

    #[test]
    fn removing_a_long_member_grows_the_free_table_as_a_short_one_does() {
        let table_after_removing = |len: usize| {
            let mut slab = MemberSlab::new();
            let handle = slab.insert(&vec![1; len]).unwrap();
            assert!(slab.remove(handle));
            check(&slab);
            slab.usage().table_bytes
        };
        assert_eq!(table_after_removing(SMALL_MAX), table_after_removing(1));
    }

    #[test]
    fn a_large_member_is_given_back_when_removed_and_its_entry_reused() {
        let mut slab = MemberSlab::new();
        let big = vec![0xab; 100_000];
        let a = slab.insert(&big).unwrap();
        assert!(is_large(a));
        assert_eq!(slab.usage().large_bytes, 100_000);
        assert!(slab.remove(a));
        assert_eq!(slab.usage().large_bytes, 0, "its bytes are given back");
        assert_eq!(slab.get(a), None);
        assert!(!slab.remove(a));

        let other = vec![0xcd; SMALL_MAX + 1];
        let b = slab.insert(&other).unwrap();
        assert_eq!(b.loc, a.loc, "the entry is reused, whatever the size");
        assert_ne!(b.generation, a.generation);
        assert_eq!(slab.get(a), None);
        assert_eq!(slab.get(b), Some(&other[..]));
        check(&slab);
    }

    /// Simulates 2^31 reuses of one record by setting its generation directly.
    #[test]
    fn a_record_is_retired_before_its_generation_wraps() {
        let mut slab = MemberSlab::new();
        let first = slab.insert(b"one").unwrap();
        assert_eq!(first.generation, 1);
        assert!(slab.remove(first));
        let (page, at) = split(first.loc);
        write_u32(&mut slab.pages[page].bytes, at, u32::MAX - 1);
        check(&slab);

        let last = slab.insert(b"two").unwrap();
        assert_eq!((last.loc, last.generation), (first.loc, u32::MAX));
        assert!(slab.remove(last));
        assert!(
            slab.free.iter().all(|(_, list)| list.is_empty()),
            "retired, not freed"
        );
        check(&slab);
        let next = slab.insert(b"six").unwrap();
        assert_ne!(next.loc, first.loc, "a retired record is not reused");
        assert_eq!(slab.get(first), None);
        assert_eq!(slab.get(last), None);

        // The same for a large member's entry.
        let first = slab.insert(&[1; SMALL_MAX + 1]).unwrap();
        assert!(slab.remove(first));
        slab.large[(first.loc & !LARGE) as usize].generation = u32::MAX - 1;
        let last = slab.insert(&[2; SMALL_MAX + 1]).unwrap();
        assert_eq!((last.loc, last.generation), (first.loc, u32::MAX));
        assert!(slab.remove(last));
        assert!(slab.large_free.is_empty());
        let next = slab.insert(&[3; SMALL_MAX + 1]).unwrap();
        assert_ne!(next.loc, first.loc);
        assert_eq!(slab.get(last), None);
        check(&slab);
    }

    /// Forged handles never panic; out-of-range or even-generation ones never match.
    #[test]
    fn handles_never_given_out_are_safe() {
        let mut slab = MemberSlab::new();
        let mut kept = Vec::new();
        for len in [0, 3, 13, 200, SMALL_MAX, SMALL_MAX + 1] {
            kept.push(slab.insert(&vec![9; len]).unwrap());
        }
        let past_pages = (slab.pages.len() as u32) << UNIT_BITS;
        let past_large = LARGE | slab.large.len() as u32;
        for generation in [1, 3, u32::MAX] {
            for loc in [past_pages, past_large, LARGE - 1, u32::MAX] {
                let handle = MemberHandle { loc, generation };
                assert_eq!(slab.get(handle), None);
                assert!(!slab.remove(handle));
            }
        }
        for handle in &kept {
            let even = MemberHandle {
                generation: handle.generation + 1,
                ..*handle
            };
            assert_eq!(slab.get(even), None);
            assert!(!slab.remove(even));
        }
        // Random handles: any read must stay inside the pages.
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        for _ in 0..100_000 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let handle = MemberHandle {
                loc: (seed as u32 & !LARGE) | ((seed >> 63) as u32 * LARGE),
                generation: (seed >> 32) as u32 % 8,
            };
            let _ = slab.get(handle);
        }
        check(&slab);
        // Inside a page but past its carved records: zeros, never a live header.
        let mut small = MemberSlab::new();
        small.insert(b"a").unwrap();
        let carved = small.pages[0].carved;
        assert!(carved as usize * UNIT < small.pages[0].bytes.len());
        let past_carved = MemberHandle {
            loc: carved as u32,
            generation: 1,
        };
        assert_eq!(small.get(past_carved), None);
        assert!(!small.remove(past_carved));
        check(&small);
    }

    #[test]
    fn usage_counts_what_is_asked_for_and_what_is_used() {
        let mut slab = MemberSlab::new();
        assert_eq!(slab.usage(), SlabUsage::default());
        assert_eq!(slab.usage().heap_bytes(), 0, "an empty slab holds nothing");

        let a = slab.insert(&[1; 14]).unwrap();
        let usage = slab.usage();
        assert_eq!((usage.members, usage.member_bytes), (1, 14));
        assert_eq!(usage.record_bytes, 24, "8 of header, 14, 2 of padding");
        assert_eq!(usage.page_bytes, FIRST_PAGE);
        assert_eq!(usage.large_bytes, 0);

        let big = slab.insert(&[2; 10_000]).unwrap();
        assert_eq!(slab.usage().large_bytes, 10_000);
        assert_eq!(slab.usage().member_bytes, 10_014);
        assert!(slab.usage().table_bytes > 0);
        let usage = slab.usage();
        assert_eq!(
            usage.heap_bytes(),
            usage.page_bytes + usage.large_bytes + usage.table_bytes
        );

        assert!(slab.remove(a));
        assert!(slab.remove(big));
        let usage = slab.usage();
        assert_eq!((usage.members, usage.member_bytes), (0, 0));
        assert_eq!(usage.record_bytes, 0);
        assert_eq!(usage.large_bytes, 0, "large members are given back");
        assert_eq!(usage.page_bytes, FIRST_PAGE, "pages are kept");
        check(&slab);
    }

    #[test]
    fn pages_start_small_and_double_up_to_64_kib() {
        let mut slab = MemberSlab::new();
        for i in 0..20_000u32 {
            slab.insert(&i.to_le_bytes().repeat(4)).unwrap(); // 24-byte records
        }
        let sizes: Vec<usize> = slab.pages.iter().map(|p| p.bytes.len()).collect();
        let doubling: Vec<usize> = (0..9).map(|k| FIRST_PAGE << k).collect();
        assert_eq!(sizes[..9], doubling[..]);
        assert!(sizes[9..].iter().all(|&len| len == PAGE));
        // A record larger than the next page gets a page that fits it.
        let mut slab = MemberSlab::new();
        let h = slab.insert(&[5; SMALL_MAX]).unwrap();
        assert_eq!(slab.pages[0].bytes.len(), SMALL_RECORD_MAX);
        assert_eq!(slab.get(h), Some(&[5; SMALL_MAX][..]));
        check(&slab);
    }

    #[test]
    fn a_full_slab_says_so_and_loses_nothing() {
        let mut slab = MemberSlab::new();
        assert_eq!(slab.spare(), 1 << 31, "the production limit");
        slab.page_limit = 1;
        slab.member_limit = 40;
        let mut held = Vec::new();
        let full = loop {
            let member = (held.len() as u64).to_le_bytes();
            match slab.insert(&member) {
                Ok(handle) => held.push((handle, member)),
                Err(full) => break full,
            }
        };
        assert_eq!(full, SlabFull);
        assert_eq!(held.len(), 40);
        let in_page = held.iter().take_while(|(handle, _)| !is_large(*handle));
        assert_eq!(
            in_page.count(),
            FIRST_PAGE / 16,
            "16-byte records in one page"
        );
        assert!(
            held[FIRST_PAGE / 16..]
                .iter()
                .all(|(handle, _)| is_large(*handle))
        );
        for (handle, member) in &held {
            assert_eq!(slab.get(*handle), Some(&member[..]));
        }
        assert_eq!(slab.insert(b""), Err(SlabFull), "whatever its size");
        assert_eq!(slab.spare(), 0);
        // Room again once any member is removed.
        assert!(slab.remove(held[30].0));
        assert_eq!(slab.spare(), 1);
        assert!(slab.insert(&[1; 200]).is_ok());
        assert_eq!(slab.insert(b"88888888"), Err(SlabFull));

        // Even when the removal retires the entry: 2^31 reuses, simulated.
        let (handle, _) = held[31];
        assert!(slab.remove(handle));
        slab.large[(handle.loc & !LARGE) as usize].generation = u32::MAX - 1;
        let last = slab.insert(b"last").unwrap();
        assert_eq!((last.loc, last.generation), (handle.loc, u32::MAX));
        assert!(slab.remove(last));
        assert!(slab.large_free.is_empty(), "retired, not freed");
        assert_eq!(slab.spare(), 1);
        assert_ne!(slab.insert(b"").unwrap().loc, handle.loc);
        assert_eq!(slab.insert(b""), Err(SlabFull));
        check(&slab);
    }

    /// Past the page limit small members take large entries, and give them back.
    #[test]
    fn past_the_page_limit_small_members_take_large_entries() {
        let mut slab = MemberSlab::new();
        slab.page_limit = 1;
        let paged: Vec<_> = (0..16u64)
            .map(|i| slab.insert(&i.to_le_bytes()).unwrap())
            .collect();
        assert!(!paged.iter().any(|&handle| is_large(handle)));
        let members: [&[u8]; 4] = [b"", b"\0\xff", b"\x01\0\0\0\x08\0\0\0", &[7; SMALL_MAX]];
        let spilled: Vec<_> = members.iter().map(|m| slab.insert(m).unwrap()).collect();
        assert!(spilled.iter().all(|&handle| is_large(handle)));
        for (handle, member) in spilled.iter().zip(members) {
            assert_eq!(slab.get(*handle), Some(member));
        }
        let usage = slab.usage();
        assert_eq!(usage.page_bytes, FIRST_PAGE, "no page past the limit");
        assert_eq!(usage.large_bytes, 2 + 8 + SMALL_MAX);
        assert_eq!(
            (usage.members, usage.member_bytes),
            (20, 16 * 8 + 10 + SMALL_MAX)
        );
        check(&slab);

        // A clone keeps its own entries.
        let copy = slab.clone();
        assert!(slab.remove(spilled[3]));
        assert!(!slab.remove(spilled[3]), "removed twice");
        assert_eq!(slab.get(spilled[3]), None);
        assert_eq!(slab.usage().large_bytes, 10, "its bytes are given back");
        assert_eq!(copy.get(spilled[3]), Some(&[7; SMALL_MAX][..]));

        // The entry serves the next member that finds no record; old handles stay dead.
        let next = slab.insert(b"after").unwrap();
        assert_eq!(
            (next.loc, next.generation),
            (spilled[3].loc, spilled[3].generation + 2)
        );
        assert_eq!(slab.get(spilled[3]), None);
        assert_eq!(copy.get(next), None);
        // A freed record of the size is still taken before an entry.
        assert!(slab.remove(paged[5]));
        let again = slab.insert(b"88888888").unwrap();
        assert_eq!(again.loc, paged[5].loc);
        check(&slab);
        check(&copy);
    }

    #[test]
    fn spare_is_what_storing_in_order_finds() {
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = |n: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            (seed % n) as usize
        };
        let (mut fitted, mut refused) = (0, 0);
        for _ in 0..3_000 {
            // A few small pages, partly freed: free records of several sizes.
            let mut slab = MemberSlab::new();
            slab.page_limit = 1 + next(4) as u32;
            slab.member_limit = next(60) as u32;
            let mut held = Vec::new();
            for _ in 0..next(60) {
                if let Ok(handle) = slab.insert(&vec![next(256) as u8; next(40)]) {
                    held.push(handle);
                }
            }
            for &handle in &held {
                if next(3) == 0 {
                    assert!(slab.remove(handle));
                }
            }
            let batch: Vec<Vec<u8>> = (0..next(12))
                .map(|_| match next(10) {
                    0 => vec![1; SMALL_MAX + 1 + next(50)],
                    1 => vec![2; next(SMALL_MAX as u64 + 1)],
                    _ => vec![next(256) as u8; next(40)],
                })
                .collect();
            let batch: Vec<&[u8]> = batch.iter().map(Vec::as_slice).collect();

            check(&slab);
            let fits = batch.len() <= slab.spare();
            let mut stored = slab.clone();
            assert_eq!(batch.iter().all(|m| stored.insert(m).is_ok()), fits);
            // Whatever pages and free records hold, only the count decides.
            let counted = slab.len() + batch.len() <= slab.member_limit as usize;
            assert_eq!(fits, counted);
            if fits {
                fitted += 1;
            } else {
                refused += 1;
            }
        }
        assert!(fitted > 300 && refused > 300, "{fitted} fit, {refused} not");
    }

    #[test]
    fn a_clone_keeps_every_handle() {
        let mut slab = MemberSlab::new();
        let a = slab.insert(b"a").unwrap();
        let big = slab.insert(&[4; SMALL_MAX + 100]).unwrap();
        let copy = slab.clone();
        assert!(slab.remove(a));
        assert!(slab.remove(big));
        let b = slab.insert(b"b").unwrap();
        assert_eq!(b.loc, a.loc);
        assert_eq!(copy.get(a), Some(&b"a"[..]));
        assert_eq!(copy.get(big), Some(&[4; SMALL_MAX + 100][..]));
        assert_eq!(copy.get(b), None);
        check(&copy);
        check(&slab);
    }

    /// Random inserts, removals and stale lookups checked against a handle-to-bytes map.
    #[test]
    fn random_inserts_and_removals_agree_with_a_map() {
        let mut seed = 0x2545_f491_4f6c_dd1du64;
        let mut next = |n: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % n
        };
        let mut slab = MemberSlab::new();
        let mut live: HashMap<MemberHandle, Vec<u8>> = HashMap::new();
        let mut order: Vec<MemberHandle> = Vec::new();
        let mut dead: Vec<MemberHandle> = Vec::new();
        let mut ever: HashSet<MemberHandle> = HashSet::new();
        for step in 0..200_000u32 {
            match next(100) {
                0..=44 => {
                    let len = match next(20) {
                        0..=11 => next(24),
                        12..=16 => next(300),
                        17 | 18 => (SMALL_MAX - 4) as u64 + next(9),
                        _ => SMALL_MAX as u64 + 1 + next(20_000),
                    } as usize;
                    let member: Vec<u8> = (0..len).map(|_| next(256) as u8).collect();
                    let handle = slab.insert(&member).unwrap();
                    // Never a handle given out before, live or removed.
                    assert!(ever.insert(handle), "{handle:?} given twice");
                    live.insert(handle, member);
                    order.push(handle);
                }
                45..=79 if !order.is_empty() => {
                    let handle = order.swap_remove(next(order.len() as u64) as usize);
                    assert!(slab.remove(handle));
                    live.remove(&handle);
                    dead.push(handle);
                }
                80..=89 if !dead.is_empty() => {
                    let handle = dead[next(dead.len() as u64) as usize];
                    assert_eq!(slab.get(handle), None);
                    assert!(!slab.remove(handle), "removed twice");
                }
                _ if !order.is_empty() => {
                    let handle = order[next(order.len() as u64) as usize];
                    assert_eq!(slab.get(handle), Some(&live[&handle][..]));
                }
                _ => {}
            }
            if step % 20_000 == 0 {
                check(&slab);
                for (handle, member) in &live {
                    assert_eq!(slab.get(*handle), Some(&member[..]));
                }
            }
        }
        check(&slab);
        for (handle, member) in &live {
            assert_eq!(slab.get(*handle), Some(&member[..]));
        }
        for handle in &dead {
            assert_eq!(slab.get(*handle), None);
        }
        // Everything removed: nothing held, pages kept for reuse.
        for handle in order {
            assert!(slab.remove(handle));
        }
        let usage = slab.usage();
        assert_eq!((usage.members, usage.member_bytes), (0, 0));
        assert_eq!((usage.record_bytes, usage.large_bytes), (0, 0));
        check(&slab);
    }
}
