//! `HSCAN` and `SSCAN` over a hash or set held as a [`CollectionTable`]
//! (#770, step 4 of the design on #764).
//!
//! The packed forms are still returned whole, and the current large forms
//! still go through `helpers::scan_page`, which reads the whole collection
//! on every call to find one page. A table gives the cursor itself
//! ([`CollectionTable::scan`]): a page reads its own groups and no others,
//! and a resize between two calls misses nothing (a growth repeats nothing
//! either; a shrink may repeat). So there is no `len / 1024` page floor
//! here: `COUNT` is the hint it is in Redis.
//!
//! What a page holds, as in Redis:
//! - `COUNT` counts the elements visited, and `MATCH` filters them after:
//!   a page can be short, or empty, with a cursor that is not 0, and a
//!   page also stops after `10 × COUNT` groups, so a long run of empty
//!   groups costs a bounded walk;
//! - the cursor is a group boundary, 48 bits wide like
//!   `helpers::scan_position`'s positions. A cursor from today's form, or
//!   from another server, is still accepted and goes on from that position,
//!   but without the guarantee, as a cursor from another Redis would. One
//!   past 48 bits ends the scan;
//! - a hash field whose TTL is due is skipped where readers do not see it
//!   (`hide_due_at`), from the TTL in its entry: the hash is not copied.

// Nothing calls it yet: sets (#764 step 2) and hashes (step 3) wire it in
// as they move onto the table.
#![allow(dead_code)]

use crate::collection_table::{CollectionTable, Fields, Members, Memory};
use crate::commands::glob_redis::Glob;
use crate::resp::write::{resp_array, resp_bulk, resp_bulk_str};
use crate::types::{FieldDeadline, field_due};

/// The `SSCAN` reply for a set held as a table: the next cursor, then the
/// members of the page that match `pattern`.
pub fn sscan_reply<A: Memory>(
    set: &CollectionTable<Members, A>,
    cursor: u64,
    count: usize,
    pattern: Option<&Glob>,
) -> Vec<u8> {
    let mut items = Vec::new();
    let next = scan_members(set, cursor, count, pattern, |member| {
        items.push(resp_bulk(member));
    });
    page_reply(next, items)
}

/// The `HSCAN` reply for a hash held as a table: the next cursor, then the
/// fields of the page that match `pattern`, each followed by its value
/// unless `no_values`. A field due at `hide_due_at` is left out.
pub fn hscan_reply<A: Memory>(
    hash: &CollectionTable<Fields, A>,
    cursor: u64,
    count: usize,
    pattern: Option<&Glob>,
    no_values: bool,
    hide_due_at: Option<FieldDeadline>,
) -> Vec<u8> {
    let mut items = Vec::new();
    let next = scan_fields(hash, cursor, count, pattern, hide_due_at, |field, value| {
        items.push(resp_bulk(field));
        if !no_values {
            items.push(resp_bulk(value));
        }
    });
    page_reply(next, items)
}

fn page_reply(next: u64, items: Vec<Vec<u8>>) -> Vec<u8> {
    resp_array(vec![resp_bulk_str(&next.to_string()), resp_array(items)])
}

/// One page of `set` from `cursor`: `out` is given each member that matches
/// `pattern`; returns the next cursor, 0 when the scan is over.
fn scan_members<A: Memory>(
    set: &CollectionTable<Members, A>,
    cursor: u64,
    count: usize,
    pattern: Option<&Glob>,
    mut out: impl FnMut(&[u8]),
) -> u64 {
    set.scan(cursor, count, |member| {
        let member = member.as_bytes();
        if pattern.is_none_or(|glob| glob.matches(member)) {
            out(member);
        }
    })
}

/// One page of `hash` from `cursor`: `out` is given each field that matches
/// `pattern` and is not due at `hide_due_at`, with its value; returns the
/// next cursor, 0 when the scan is over.
fn scan_fields<A: Memory>(
    hash: &CollectionTable<Fields, A>,
    cursor: u64,
    count: usize,
    pattern: Option<&Glob>,
    hide_due_at: Option<FieldDeadline>,
    mut out: impl FnMut(&[u8], &[u8]),
) -> u64 {
    hash.scan(cursor, count, |entry| {
        let due = hide_due_at.is_some_and(|now| entry.ttl.is_some_and(|at| field_due(at, now)));
        if !due && pattern.is_none_or(|glob| glob.matches(entry.field)) {
            out(entry.field, entry.value);
        }
    })
}

#[cfg(test)]
// Fixed seeds are for tests (clippy.toml forbids them elsewhere).
#[allow(clippy::disallowed_methods)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

    type Set = CollectionTable<Members>;
    type Hash = CollectionTable<Fields>;

    fn member(i: u64) -> Vec<u8> {
        // Both forms: up to seven bytes in the slot, longer in a block.
        if i.is_multiple_of(2) {
            format!("{i}").into_bytes()
        } else {
            format!("member:{i:08}").into_bytes()
        }
    }

    fn set_of(n: u64) -> Set {
        let mut set = Set::with_capacity_and_seed(0, 770);
        for i in 0..n {
            assert!(set.insert(&member(i)).unwrap());
        }
        set
    }

    /// Every member a full scan returns, page by page, at `count`.
    fn full_scan(set: &Set, count: usize) -> (Vec<Vec<u8>>, usize) {
        let (mut seen, mut pages, mut cursor) = (Vec::new(), 0, 0);
        loop {
            cursor = scan_members(set, cursor, count, None, |m| seen.push(m.to_vec()));
            pages += 1;
            if cursor == 0 {
                return (seen, pages);
            }
        }
    }

    #[test]
    fn a_full_scan_returns_every_member_once() {
        for n in [0, 1, 7, 100, 5_000] {
            let set = set_of(n);
            for count in [1, 10, 1_000] {
                let (seen, _) = full_scan(&set, count);
                assert_eq!(seen.len() as u64, n, "n {n} count {count}");
                let distinct: HashSet<_> = seen.into_iter().collect();
                assert_eq!(distinct, (0..n).map(member).collect(), "n {n}");
            }
        }
    }

    /// The SCAN guarantee across resizes: inserts that double the table
    /// several times, then deletes that shrink it, between pages. Every
    /// member there for the whole scan is returned; one removed before the
    /// scan started is not.
    #[test]
    fn members_there_for_the_whole_scan_are_returned_across_growth_and_shrink() {
        for count in [1, 10, 100] {
            let mut set = set_of(2_000);
            // Not there when the scan starts, so never owed.
            for i in 1_000..1_200 {
                assert!(set.remove(&member(i)));
            }
            let lasting: HashSet<Vec<u8>> = (0..1_000).map(member).collect();
            let mut seen = HashSet::new();
            let mut next_new = 1_000_000;
            let mut cursor = 0;
            let mut growing = true;
            let first_size = set.allocation_size();
            let mut largest = first_size;
            loop {
                cursor = scan_members(&set, cursor, count, None, |m| {
                    seen.insert(m.to_vec());
                });
                if cursor == 0 {
                    break;
                }
                // Grow to 16,000 members, then shrink by deleting the ones
                // not owed (the late ones and 1_200..2_000), so both kinds
                // of resize fall between pages.
                if growing {
                    for _ in 0..1_000 {
                        set.insert(&member(next_new)).unwrap();
                        next_new += 1;
                    }
                    growing = set.len() < 16_000;
                } else {
                    let gone: Vec<Vec<u8>> = set
                        .iter()
                        .map(|m| m.as_bytes().to_vec())
                        .filter(|m| !lasting.contains(m))
                        .take(1_000)
                        .collect();
                    for m in gone {
                        set.remove(&m);
                    }
                }
                largest = largest.max(set.allocation_size());
            }
            assert!(largest > 4 * first_size, "count {count}: it grew");
            assert!(
                set.allocation_size() < largest / 2,
                "count {count}: it shrank"
            );
            let missed: Vec<_> = lasting.difference(&seen).collect();
            assert!(missed.is_empty(), "count {count}: missed {missed:?}");
            for i in 1_000..1_200 {
                assert!(!seen.contains(&member(i)), "count {count}: {i}");
            }
        }
    }

    /// `COUNT` is a hint again: a page of a large set at `COUNT 10` holds
    /// about ten members, not `len / 1024` of them, and a full scan takes
    /// about `len / 10` pages.
    #[test]
    fn a_page_at_count_10_of_a_large_set_is_about_ten_members() {
        let n: u64 = 1_000_000;
        let mut set = Set::with_capacity_and_seed(n as usize, 770);
        for i in 0..n {
            set.insert(&i.to_be_bytes()).unwrap();
        }
        let mut first = 0;
        let cursor = scan_members(&set, 0, 10, None, |_| first += 1);
        assert_ne!(cursor, 0);
        assert!((1..=40).contains(&first), "{first} members on one page");
        let (seen, pages) = full_scan(&set, 10);
        assert_eq!(seen.len() as u64, n);
        assert!(
            (n as usize / 40..=n as usize / 5).contains(&pages),
            "{pages} pages"
        );
    }

    /// `MATCH` filters what a page visited: pages can be empty with the
    /// scan not over, and the scan still finds every match.
    #[test]
    fn match_filters_each_page_and_the_scan_finds_every_match() {
        let set = set_of(3_000);
        let glob = Glob::new(b"member:*7");
        let (mut seen, mut cursor, mut empty_pages) = (HashSet::new(), 0, 0);
        loop {
            let before = seen.len();
            cursor = scan_members(&set, cursor, 10, Some(&glob), |m| {
                seen.insert(m.to_vec());
            });
            if seen.len() == before && cursor != 0 {
                empty_pages += 1;
            }
            if cursor == 0 {
                break;
            }
        }
        let want: HashSet<Vec<u8>> = (0..3_000).map(member).filter(|m| glob.matches(m)).collect();
        assert_eq!(seen, want);
        assert!(empty_pages > 0);
    }

    #[test]
    fn a_cursor_past_48_bits_ends_the_scan_and_an_old_one_goes_on() {
        let set = set_of(1_000);
        assert_eq!(
            sscan_reply(&set, 1 << 48, 10, None),
            b"*2\r\n$1\r\n0\r\n*0\r\n"
        );
        // A cursor from today's form is a 48-bit position: the scan goes on
        // from there to the end, and from 0 up to it gives the rest.
        let middle = 1u64 << 47;
        let mut after = Vec::new();
        let mut cursor = middle;
        loop {
            cursor = scan_members(&set, cursor, 10, None, |m| after.push(m.to_vec()));
            if cursor == 0 {
                break;
            }
        }
        assert!(!after.is_empty() && after.len() < 1_000);
    }

    fn hash_of(n: u64, due_every: u64) -> Hash {
        let mut hash = Hash::with_capacity_and_seed(0, 770);
        for i in 0..n {
            let ttl = (due_every > 0 && i.is_multiple_of(due_every)).then_some(1_000);
            hash.insert(&member(i), format!("v{i}").as_bytes(), ttl)
                .unwrap();
        }
        hash
    }

    fn full_hash_scan(
        hash: &Hash,
        hide_due_at: Option<FieldDeadline>,
    ) -> HashMap<Vec<u8>, Vec<u8>> {
        let (mut seen, mut cursor) = (HashMap::new(), 0);
        loop {
            cursor = scan_fields(hash, cursor, 10, None, hide_due_at, |f, v| {
                assert!(seen.insert(f.to_vec(), v.to_vec()).is_none());
            });
            if cursor == 0 {
                return seen;
            }
        }
    }

    #[test]
    fn a_full_hscan_returns_every_field_with_its_value() {
        let seen = full_hash_scan(&hash_of(2_000, 0), None);
        assert_eq!(seen.len(), 2_000);
        for i in 0..2_000 {
            assert_eq!(seen[&member(i)], format!("v{i}").into_bytes());
        }
    }

    /// A due field is skipped where readers do not see it, from the TTL in
    /// its entry, and shown where they do (a load, a replayed AOF).
    #[test]
    fn due_fields_are_skipped_only_where_they_are_hidden() {
        let hash = hash_of(1_000, 3);
        assert_eq!(full_hash_scan(&hash, None).len(), 1_000);
        // Not yet due at their deadline: due after it.
        assert_eq!(full_hash_scan(&hash, Some(1_000)).len(), 1_000);
        let live = full_hash_scan(&hash, Some(1_001));
        assert_eq!(live.len(), 1_000 - 334);
        assert!(live.keys().all(|f| !f.is_empty()));
        assert!(!live.contains_key(&member(0)) && live.contains_key(&member(1)));
    }

    #[test]
    fn hscan_reply_has_values_unless_novalues() {
        let mut hash = Hash::new();
        hash.insert(b"f", b"v", None).unwrap();
        assert_eq!(
            hscan_reply(&hash, 0, 10, None, false, None),
            b"*2\r\n$1\r\n0\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n"
        );
        assert_eq!(
            hscan_reply(&hash, 0, 10, None, true, None),
            b"*2\r\n$1\r\n0\r\n*1\r\n$1\r\nf\r\n"
        );
        let glob = Glob::new(b"x*");
        assert_eq!(
            hscan_reply(&hash, 0, 10, Some(&glob), false, None),
            b"*2\r\n$1\r\n0\r\n*0\r\n"
        );
    }
}
