//! A consumer group's pending entries, indexed by consumer as well as by ID.

use super::{StreamId, StreamName};
use ahash::AHashMap;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::{Deref, RangeBounds};

/// A pending entry: delivered to a consumer and not yet acknowledged.
#[derive(Debug, Clone)]
pub struct PelEntry {
    /// Changed only through [`Pel`], which indexes entries by it.
    consumer: StreamName,
    pub delivered_at: u64, // unix ms
    pub delivery_count: u64,
}

impl PelEntry {
    pub fn new(consumer: StreamName, delivered_at: u64, delivery_count: u64) -> Self {
        PelEntry {
            consumer,
            delivered_at,
            delivery_count,
        }
    }

    /// The consumer it was delivered to; empty once `XNACK` released it.
    pub fn consumer(&self) -> &StreamName {
        &self.consumer
    }
}

/// A pending entry open to change: when it was delivered and how often, but
/// not to whom, which only [`Pel::set_consumer`] changes.
///
/// So an entry can be neither handed over nor replaced through it:
///
/// ```compile_fail
/// # use kividb::types::{Pel, PelEntry, StreamId};
/// let mut pel = Pel::new();
/// pel.insert(StreamId::new(1, 0), PelEntry::new("a".into(), 0, 1));
/// let e = pel.get_mut(&StreamId::new(1, 0)).unwrap();
/// *e = PelEntry::new("b".into(), 0, 1);
/// ```
///
/// ```compile_fail
/// # use kividb::types::{Pel, PelEntry, StreamId};
/// let mut pel = Pel::new();
/// pel.insert(StreamId::new(1, 0), PelEntry::new("a".into(), 0, 1));
/// *pel.get_mut(&StreamId::new(1, 0)).unwrap().consumer = "b".into();
/// ```
#[derive(Debug)]
pub struct PendingMut<'a> {
    pub consumer: &'a StreamName,
    pub delivered_at: &'a mut u64,
    pub delivery_count: &'a mut u64,
}

impl<'a> PendingMut<'a> {
    fn new(entry: &'a mut PelEntry) -> Self {
        PendingMut {
            consumer: &entry.consumer,
            delivered_at: &mut entry.delivered_at,
            delivery_count: &mut entry.delivery_count,
        }
    }
}

/// A group's pending entries by ID, and the IDs each consumer has pending.
///
/// Reads go straight to the map by ID (through `Deref`). Every change goes
/// through the methods here, which keep `by_consumer` in step: it lists a
/// consumer exactly when the consumer has an entry pending, with the IDs of
/// those entries.
#[derive(Debug, Clone, Default)]
pub struct Pel {
    entries: BTreeMap<StreamId, PelEntry>,
    by_consumer: AHashMap<StreamName, BTreeSet<StreamId>>,
}

impl Deref for Pel {
    type Target = BTreeMap<StreamId, PelEntry>;

    fn deref(&self) -> &Self::Target {
        &self.entries
    }
}

fn link(index: &mut AHashMap<StreamName, BTreeSet<StreamId>>, consumer: &StreamName, id: StreamId) {
    match index.get_mut(consumer.as_bytes()) {
        Some(ids) => {
            ids.insert(id);
        }
        None => {
            index.insert(consumer.clone(), BTreeSet::from([id]));
        }
    }
}

fn unlink(index: &mut AHashMap<StreamName, BTreeSet<StreamId>>, consumer: &[u8], id: &StreamId) {
    if let Some(ids) = index.get_mut(consumer) {
        ids.remove(id);
        if ids.is_empty() {
            index.remove(consumer);
        }
    }
}

impl Pel {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add the entry `id`, or replace it, moving it to the new entry's
    /// consumer. Returns the entry it replaced.
    pub fn insert(&mut self, id: StreamId, entry: PelEntry) -> Option<PelEntry> {
        match self.entries.entry(id) {
            Entry::Vacant(slot) => {
                link(&mut self.by_consumer, &entry.consumer, slot.key().clone());
                slot.insert(entry);
                None
            }
            Entry::Occupied(mut slot) => {
                let old = &slot.get().consumer;
                if *old != entry.consumer {
                    unlink(&mut self.by_consumer, old.as_bytes(), slot.key());
                    link(&mut self.by_consumer, &entry.consumer, slot.key().clone());
                }
                Some(slot.insert(entry))
            }
        }
    }

    /// Remove the entry `id`, returning it.
    pub fn remove(&mut self, id: &StreamId) -> Option<PelEntry> {
        let old = self.entries.remove(id)?;
        unlink(&mut self.by_consumer, old.consumer.as_bytes(), id);
        Some(old)
    }

    /// The entry `id`, to change when it was delivered and how often.
    pub fn get_mut(&mut self, id: &StreamId) -> Option<PendingMut<'_>> {
        self.entries.get_mut(id).map(PendingMut::new)
    }

    /// Hand the entry `id` to `consumer`. None if it is not pending.
    pub fn set_consumer(&mut self, id: &StreamId, consumer: &StreamName) -> Option<PendingMut<'_>> {
        let entry = self.entries.get_mut(id)?;
        if entry.consumer != *consumer {
            unlink(&mut self.by_consumer, entry.consumer.as_bytes(), id);
            link(&mut self.by_consumer, consumer, id.clone());
            entry.consumer = consumer.clone();
        }
        Some(PendingMut::new(entry))
    }

    /// Remove every entry `consumer` has pending, returning how many.
    pub fn remove_consumer(&mut self, consumer: &[u8]) -> usize {
        let Some(ids) = self.by_consumer.remove(consumer) else {
            return 0;
        };
        for id in &ids {
            self.entries.remove(id);
        }
        ids.len()
    }

    /// How many entries `consumer` has pending.
    pub fn count_of(&self, consumer: &[u8]) -> usize {
        self.by_consumer.get(consumer).map_or(0, BTreeSet::len)
    }

    /// The IDs `consumer` has pending within `range`, in order.
    pub fn ids_of<R: RangeBounds<StreamId>>(
        &self,
        consumer: &[u8],
        range: R,
    ) -> impl DoubleEndedIterator<Item = &StreamId> {
        self.by_consumer
            .get(consumer)
            .map(|ids| ids.range(range))
            .into_iter()
            .flatten()
    }

    /// Each consumer with entries pending, and how many: in no set order.
    pub fn counts(&self) -> impl Iterator<Item = (&StreamName, usize)> {
        self.by_consumer.iter().map(|(name, ids)| (name, ids.len()))
    }
}

impl FromIterator<(StreamId, PelEntry)> for Pel {
    fn from_iter<I: IntoIterator<Item = (StreamId, PelEntry)>>(iter: I) -> Self {
        let mut pel = Pel::new();
        for (id, entry) in iter {
            pel.insert(id, entry);
        }
        pel
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(ms: u64) -> StreamId {
        StreamId::new(ms, 0)
    }

    fn entry(consumer: &str) -> PelEntry {
        PelEntry::new(consumer.into(), 1, 1)
    }

    /// The index matches a scan of the entries.
    fn check(pel: &Pel) {
        let mut scanned: AHashMap<&[u8], Vec<StreamId>> = AHashMap::new();
        for (id, e) in pel.iter() {
            scanned
                .entry(e.consumer.as_bytes())
                .or_default()
                .push(id.clone());
        }
        assert_eq!(pel.by_consumer.len(), scanned.len(), "consumers listed");
        for (name, ids) in &pel.by_consumer {
            assert!(!ids.is_empty(), "an empty set is kept for {name:?}");
            let want = &scanned[name.as_bytes()];
            assert_eq!(ids.iter().cloned().collect::<Vec<_>>(), *want);
            assert_eq!(pel.count_of(name.as_bytes()), want.len());
        }
    }

    fn owned(pel: &Pel, consumer: &str) -> Vec<StreamId> {
        pel.ids_of(consumer.as_bytes(), ..).cloned().collect()
    }

    #[test]
    fn each_consumer_has_its_own_entries_in_order() {
        let pel: Pel = [(3, "a"), (1, "b"), (2, "a"), (5, "b"), (4, "")]
            .into_iter()
            .map(|(ms, c)| (id(ms), entry(c)))
            .collect();
        check(&pel);
        assert_eq!(owned(&pel, "a"), [id(2), id(3)]);
        assert_eq!(owned(&pel, "b"), [id(1), id(5)]);
        assert_eq!(owned(&pel, ""), [id(4)]);
        assert_eq!(owned(&pel, "nobody"), []);
        assert_eq!(pel.count_of(b"nobody"), 0);
        let after_1: Vec<_> = pel
            .ids_of(
                b"b",
                (std::ops::Bound::Excluded(id(1)), std::ops::Bound::Unbounded),
            )
            .cloned()
            .collect();
        assert_eq!(after_1, [id(5)]);
        let mut counts: Vec<_> = pel
            .counts()
            .map(|(n, c)| (n.as_bytes().to_vec(), c))
            .collect();
        counts.sort();
        assert_eq!(
            counts,
            [(b"".to_vec(), 1), (b"a".to_vec(), 2), (b"b".to_vec(), 2)]
        );
    }

    #[test]
    fn replacing_an_entry_moves_it_to_the_new_consumer() {
        let mut pel = Pel::new();
        assert!(pel.insert(id(1), entry("a")).is_none());
        let old = pel.insert(id(1), entry("b")).unwrap();
        assert_eq!(old.consumer(), "a");
        check(&pel);
        assert_eq!(owned(&pel, "a"), []);
        assert_eq!(owned(&pel, "b"), [id(1)]);
        // The same consumer again: still listed once.
        pel.insert(id(1), entry("b"));
        check(&pel);
        assert_eq!(pel.count_of(b"b"), 1);
    }

    #[test]
    fn set_consumer_moves_only_pending_entries() {
        let mut pel: Pel = [(1, "a"), (2, "a")]
            .into_iter()
            .map(|(ms, c)| (id(ms), entry(c)))
            .collect();
        let e = pel.set_consumer(&id(2), &"b".into()).unwrap();
        assert_eq!(e.consumer, "b");
        *e.delivery_count = 7;
        assert!(pel.set_consumer(&id(9), &"b".into()).is_none());
        // To the consumer that already has it: nothing moves.
        pel.set_consumer(&id(1), &"a".into()).unwrap();
        check(&pel);
        assert_eq!(owned(&pel, "a"), [id(1)]);
        assert_eq!(owned(&pel, "b"), [id(2)]);
        assert_eq!(pel[&id(2)].delivery_count, 7);
        assert_eq!(pel[&id(2)].consumer(), "b");
        // Released (`XNACK`): no one's.
        pel.set_consumer(&id(1), &StreamName::default()).unwrap();
        check(&pel);
        assert_eq!(owned(&pel, ""), [id(1)]);
        assert_eq!(pel.count_of(b"a"), 0);
    }

    #[test]
    fn removing_entries_and_consumers_keeps_the_index_in_step() {
        let mut pel: Pel = (1..=6)
            .map(|ms| (id(ms), entry(if ms % 2 == 0 { "even" } else { "odd" })))
            .collect();
        assert_eq!(pel.remove(&id(2)).unwrap().consumer(), "even");
        assert!(pel.remove(&id(2)).is_none());
        check(&pel);
        assert_eq!(pel.remove_consumer(b"odd"), 3);
        assert_eq!(pel.remove_consumer(b"odd"), 0);
        check(&pel);
        assert_eq!(pel.keys().cloned().collect::<Vec<_>>(), [id(4), id(6)]);
        pel.remove(&id(4));
        pel.remove(&id(6));
        check(&pel);
        assert!(pel.is_empty());
        assert_eq!(pel.counts().count(), 0);
    }
}
