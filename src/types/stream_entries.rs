//! The entries of a stream, in ID order.
//!
//! Commands, persistence and memory accounting reach a stream's entries only
//! through [`StreamEntries`]: IDs come back by value and fields as borrowed
//! slices ([`EntryRef`]), so the way entries are stored can change without
//! touching them.

use super::{StreamEntry, StreamId};
use std::collections::BTreeMap;
use std::ops::RangeBounds;

/// A stream's entries, ordered by ID.
#[derive(Debug, Clone, Default)]
pub struct StreamEntries {
    map: BTreeMap<StreamId, StreamEntry>,
}

/// One entry's field-value pairs, borrowed from the stream.
#[derive(Debug, Clone, Copy)]
pub struct EntryRef<'a>(&'a StreamEntry);

impl<'a> EntryRef<'a> {
    /// The field-value pairs in the order they were added.
    pub fn fields(self) -> impl ExactSizeIterator<Item = (&'a [u8], &'a [u8])> + 'a {
        self.0.fields.iter().map(|(f, v)| (f.as_ref(), v.as_ref()))
    }

    /// How many field-value pairs the entry has.
    pub fn field_count(self) -> usize {
        self.0.fields.len()
    }
}

type Item<'a> = (StreamId, EntryRef<'a>);

fn item<'a>((id, entry): (&StreamId, &'a StreamEntry)) -> Item<'a> {
    (id.clone(), EntryRef(entry))
}

impl StreamEntries {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn contains(&self, id: &StreamId) -> bool {
        self.map.contains_key(id)
    }

    pub fn get(&self, id: &StreamId) -> Option<EntryRef<'_>> {
        self.map.get(id).map(EntryRef)
    }

    /// The ID of the oldest entry.
    pub fn first_id(&self) -> Option<StreamId> {
        self.map.keys().next().cloned()
    }

    /// The ID of the newest entry.
    pub fn last_id(&self) -> Option<StreamId> {
        self.map.keys().next_back().cloned()
    }

    /// All entries, oldest first.
    pub fn iter(&self) -> impl Iterator<Item = Item<'_>> {
        self.map.iter().map(item)
    }

    /// The entries with IDs in `range`, oldest first.
    ///
    /// The range must not start after it ends; callers check that first.
    pub fn range<R: RangeBounds<StreamId>>(&self, range: R) -> impl Iterator<Item = Item<'_>> {
        self.map.range(range).map(item)
    }

    /// The entries with IDs in `range`, newest first.
    pub fn range_rev<R: RangeBounds<StreamId>>(&self, range: R) -> impl Iterator<Item = Item<'_>> {
        self.map.range(range).rev().map(item)
    }

    /// Add an entry from `XADD`, which has checked its ID against the
    /// stream's `last_id`. That is normally newer than every entry, but a
    /// loaded stream can have a `last_id` below its newest entry: the entry
    /// then goes in ID order, replacing one with the same ID.
    pub fn push_tail(&mut self, id: StreamId, entry: StreamEntry) {
        self.map.insert(id, entry);
    }

    /// Add an entry read from a file or a payload. IDs may come in any
    /// order, and a repeated ID replaces the entry added before it.
    pub fn insert_loaded(&mut self, id: StreamId, entry: StreamEntry) {
        self.map.insert(id, entry);
    }

    /// Remove the entry `id`; false if there is none.
    pub fn remove(&mut self, id: &StreamId) -> bool {
        self.map.remove(id).is_some()
    }

    /// Remove the oldest entry, returning its ID.
    pub fn pop_first(&mut self) -> Option<StreamId> {
        self.map.pop_first().map(|(id, _)| id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Bound::{Excluded, Included, Unbounded};
    use std::sync::Arc;

    fn id(ms: u64, seq: u64) -> StreamId {
        StreamId::new(ms, seq)
    }

    fn entry(pairs: &[(&[u8], &[u8])]) -> StreamEntry {
        StreamEntry {
            fields: pairs
                .iter()
                .map(|(f, v)| (Arc::from(*f), Arc::from(*v)))
                .collect(),
        }
    }

    fn ids<'a>(it: impl Iterator<Item = Item<'a>>) -> Vec<StreamId> {
        it.map(|(id, _)| id).collect()
    }

    /// 1-0, 1-1, 2-0, 5-7
    fn sample() -> StreamEntries {
        let mut s = StreamEntries::new();
        for (ms, seq) in [(1, 0), (1, 1), (2, 0), (5, 7)] {
            s.push_tail(id(ms, seq), entry(&[(b"f", b"v")]));
        }
        s
    }

    #[test]
    fn an_empty_stream_has_nothing_in_any_range() {
        let s = StreamEntries::new();
        assert!(s.is_empty());
        assert_eq!(s.len(), 0);
        assert_eq!((s.first_id(), s.last_id()), (None, None));
        assert!(s.get(&id(1, 0)).is_none());
        assert_eq!(s.iter().count(), 0);
        assert_eq!(s.range(..).count(), 0);
        assert_eq!(s.range_rev(..).count(), 0);
    }

    #[test]
    fn entries_come_back_in_id_order_both_ways() {
        let s = sample();
        let all = vec![id(1, 0), id(1, 1), id(2, 0), id(5, 7)];
        assert_eq!(ids(s.iter()), all);
        assert_eq!(ids(s.range(..)), all);
        let mut back = all.clone();
        back.reverse();
        assert_eq!(ids(s.range_rev(..)), back);
        assert_eq!(
            (s.first_id(), s.last_id()),
            (Some(id(1, 0)), Some(id(5, 7)))
        );
    }

    #[test]
    fn ranges_honour_inclusive_and_exclusive_bounds() {
        let s = sample();
        assert_eq!(ids(s.range(id(1, 1)..=id(2, 0))), [id(1, 1), id(2, 0)]);
        assert_eq!(ids(s.range(id(1, 1)..id(2, 0))), [id(1, 1)]);
        assert_eq!(
            ids(s.range((Excluded(id(1, 1)), Unbounded))),
            [id(2, 0), id(5, 7)]
        );
        assert_eq!(
            ids(s.range_rev(..=id(2, 0))),
            [id(2, 0), id(1, 1), id(1, 0)]
        );
        assert_eq!(
            ids(s.range_rev((Excluded(id(1, 0)), Included(id(5, 7))))),
            [id(5, 7), id(2, 0), id(1, 1)]
        );
        // Bounds between entries, and past either end.
        assert_eq!(ids(s.range(id(1, 2)..=id(4, 0))), [id(2, 0)]);
        assert_eq!(s.range(id(6, 0)..).count(), 0);
        assert_eq!(s.range_rev(..id(1, 0)).count(), 0);
    }

    #[test]
    fn fields_are_returned_byte_exact_and_in_order() {
        let mut s = StreamEntries::new();
        let pairs: &[(&[u8], &[u8])] = &[(b"a", b"1"), (b"", b"\0\xff"), (b"a", b"")];
        s.push_tail(id(1, 0), entry(pairs));
        let e = s.get(&id(1, 0)).unwrap();
        assert_eq!(e.field_count(), 3);
        assert_eq!(e.fields().collect::<Vec<_>>(), pairs);
        assert!(s.get(&id(1, 1)).is_none());
        assert!(!s.contains(&id(0, 9)));
    }

    #[test]
    fn remove_and_pop_first_report_what_they_removed() {
        let mut s = sample();
        assert!(s.remove(&id(1, 1)));
        assert!(!s.remove(&id(1, 1)));
        assert!(!s.remove(&id(3, 0)));
        assert_eq!(s.pop_first(), Some(id(1, 0)));
        assert_eq!(ids(s.iter()), [id(2, 0), id(5, 7)]);
        assert_eq!(s.pop_first(), Some(id(2, 0)));
        assert_eq!(s.pop_first(), Some(id(5, 7)));
        assert_eq!(s.pop_first(), None);
        assert!(s.is_empty());
    }

    #[test]
    fn loading_takes_ids_in_any_order_and_a_repeat_replaces() {
        let mut s = StreamEntries::new();
        s.insert_loaded(id(5, 0), entry(&[(b"f", b"first")]));
        s.insert_loaded(id(2, 0), entry(&[(b"f", b"x")]));
        s.insert_loaded(id(5, 0), entry(&[(b"f", b"second")]));
        assert_eq!(ids(s.iter()), [id(2, 0), id(5, 0)]);
        let fields: Vec<_> = s.get(&id(5, 0)).unwrap().fields().collect();
        assert_eq!(fields, [(&b"f"[..], &b"second"[..])]);
    }

    #[test]
    fn appending_below_the_newest_loaded_entry_keeps_id_order() {
        // A loaded stream whose `last_id` (2-0) is below its newest entry
        // lets XADD through with 3-0 and 5-0.
        let mut s = StreamEntries::new();
        s.insert_loaded(id(1, 0), entry(&[(b"f", b"a")]));
        s.insert_loaded(id(5, 0), entry(&[(b"f", b"loaded")]));
        s.push_tail(id(3, 0), entry(&[(b"f", b"between")]));
        assert_eq!(ids(s.iter()), [id(1, 0), id(3, 0), id(5, 0)]);
        assert_eq!(ids(s.range_rev(..)), [id(5, 0), id(3, 0), id(1, 0)]);
        assert_eq!(s.last_id(), Some(id(5, 0)));

        s.push_tail(id(5, 0), entry(&[(b"f", b"added")]));
        assert_eq!(s.len(), 3);
        let fields: Vec<_> = s.get(&id(5, 0)).unwrap().fields().collect();
        assert_eq!(fields, [(&b"f"[..], &b"added"[..])]);
    }
}
