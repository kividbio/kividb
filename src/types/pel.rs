//! A consumer group's pending entries list (PEL), indexed by consumer.
//!
//! Every pending entry is in the group's list, ordered by ID, and in the
//! set of IDs of the consumer that owns it. The second lets the commands
//! that ask about one consumer (`XINFO CONSUMERS`, `XPENDING` with a
//! consumer, `XREADGROUP` of a consumer's history, `XGROUP DELCONSUMER`)
//! touch only that consumer's entries, not the whole list (#637).
//!
//! The two are kept in step here, by the only methods that add, move or
//! remove an entry: an entry's owner is private, so no caller can change
//! it behind the index's back. The index is not saved: loading a group
//! inserts its entries one by one, which builds it.

use super::{StreamId, StreamName};
use ahash::AHashMap;
use std::collections::{BTreeMap, BTreeSet, btree_map};
use std::ops::RangeBounds;
use std::sync::Arc;

/// One pending entry: who has it, since when, and how often it was
/// delivered.
#[derive(Debug, Clone)]
pub struct PelEntry {
    /// Shared with the consumer's index, so the name is stored once per
    /// consumer rather than once per entry. Empty for an entry `XNACK`
    /// released, which no one owns.
    consumer: Arc<StreamName>,
    /// Unix ms.
    pub delivered_at: u64,
    pub delivery_count: u64,
}

impl PelEntry {
    /// The consumer that owns the entry; empty if no one does.
    pub fn consumer(&self) -> &StreamName {
        &self.consumer
    }
}

/// The IDs one consumer owns.
#[derive(Debug, Clone)]
struct Owned {
    name: Arc<StreamName>,
    ids: BTreeSet<StreamId>,
}

/// A group's pending entries, by ID and by consumer.
#[derive(Debug, Clone, Default)]
pub struct Pel {
    entries: BTreeMap<StreamId, PelEntry>,
    /// Consumer name → the IDs it owns. A consumer with none has no entry.
    owners: Owners,
}

type Owners = AHashMap<StreamName, Owned>;

/// `consumer`'s index entry, made if it has none. One lookup: a consumer
/// name is short, and copying it is cheaper than looking it up twice.
fn owner_mut<'a>(owners: &'a mut Owners, consumer: &[u8]) -> &'a mut Owned {
    owners
        .entry(StreamName::from(consumer))
        .or_insert_with_key(|name| Owned {
            name: Arc::new(name.clone()),
            ids: BTreeSet::new(),
        })
}

/// Takes `id` out of `consumer`'s index, and the consumer out of the index
/// once it owns nothing.
fn unlink(owners: &mut Owners, consumer: &[u8], id: &StreamId) {
    if let Some(owned) = owners.get_mut(consumer) {
        owned.ids.remove(id);
        if owned.ids.is_empty() {
            owners.remove(consumer);
        }
    }
}

impl Pel {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn get(&self, id: &StreamId) -> Option<&PelEntry> {
        self.entries.get(id)
    }

    /// The entry, to change its delivery time or count. Its owner changes
    /// only through [`Pel::assign`] or [`Pel::insert`].
    pub fn get_mut(&mut self, id: &StreamId) -> Option<&mut PelEntry> {
        self.entries.get_mut(id)
    }

    pub fn contains_key(&self, id: &StreamId) -> bool {
        self.entries.contains_key(id)
    }

    /// Every entry, by ID.
    pub fn iter(&self) -> btree_map::Iter<'_, StreamId, PelEntry> {
        self.entries.iter()
    }

    pub fn keys(&self) -> btree_map::Keys<'_, StreamId, PelEntry> {
        self.entries.keys()
    }

    pub fn values(&self) -> btree_map::Values<'_, StreamId, PelEntry> {
        self.entries.values()
    }

    pub fn range<R: RangeBounds<StreamId>>(
        &self,
        range: R,
    ) -> btree_map::Range<'_, StreamId, PelEntry> {
        self.entries.range(range)
    }

    /// The lowest pending ID.
    pub fn first_id(&self) -> Option<&StreamId> {
        self.entries.keys().next()
    }

    /// The highest pending ID.
    pub fn last_id(&self) -> Option<&StreamId> {
        self.entries.keys().next_back()
    }

    /// Makes `id` pending for `consumer`, with these delivery details. An
    /// entry already pending is replaced, and moves to `consumer` if someone
    /// else had it.
    pub fn insert(
        &mut self,
        id: StreamId,
        consumer: &[u8],
        delivered_at: u64,
        delivery_count: u64,
    ) -> &mut PelEntry {
        match self.entries.entry(id) {
            btree_map::Entry::Vacant(slot) => {
                let owned = owner_mut(&mut self.owners, consumer);
                owned.ids.insert(slot.key().clone());
                slot.insert(PelEntry {
                    consumer: Arc::clone(&owned.name),
                    delivered_at,
                    delivery_count,
                })
            }
            btree_map::Entry::Occupied(slot) => {
                let id = slot.key().clone();
                let entry = slot.into_mut();
                if entry.consumer.as_bytes() != consumer {
                    unlink(&mut self.owners, entry.consumer.as_bytes(), &id);
                    let owned = owner_mut(&mut self.owners, consumer);
                    owned.ids.insert(id);
                    entry.consumer = Arc::clone(&owned.name);
                }
                entry.delivered_at = delivered_at;
                entry.delivery_count = delivery_count;
                entry
            }
        }
    }

    /// Hands the pending entry `id` to `consumer`. None if it is not
    /// pending.
    pub fn assign(&mut self, id: &StreamId, consumer: &[u8]) -> Option<&mut PelEntry> {
        let entry = self.entries.get_mut(id)?;
        if entry.consumer.as_bytes() != consumer {
            unlink(&mut self.owners, entry.consumer.as_bytes(), id);
            let owned = owner_mut(&mut self.owners, consumer);
            owned.ids.insert(id.clone());
            entry.consumer = Arc::clone(&owned.name);
        }
        Some(entry)
    }

    /// Acknowledges `id`: it is no longer pending.
    pub fn remove(&mut self, id: &StreamId) -> Option<PelEntry> {
        let entry = self.entries.remove(id)?;
        unlink(&mut self.owners, entry.consumer.as_bytes(), id);
        Some(entry)
    }

    /// Drops every entry `consumer` owns. Returns how many there were.
    pub fn remove_consumer(&mut self, consumer: &[u8]) -> usize {
        let Some(owned) = self.owners.remove(consumer) else {
            return 0;
        };
        for id in &owned.ids {
            self.entries.remove(id);
        }
        owned.ids.len()
    }

    /// How many entries `consumer` owns.
    pub fn consumer_len(&self, consumer: &[u8]) -> usize {
        self.owners.get(consumer).map_or(0, |owned| owned.ids.len())
    }

    /// The entries `consumer` owns with an ID in `range`, by ID.
    pub fn consumer_range<R: RangeBounds<StreamId>>(
        &self,
        consumer: &[u8],
        range: R,
    ) -> impl Iterator<Item = (&StreamId, &PelEntry)> {
        let ids = self
            .owners
            .get(consumer)
            .map(|owned| owned.ids.range(range));
        ids.into_iter().flatten().map(|id| {
            let entry = self.entries.get(id).expect("indexed entry is pending");
            (id, entry)
        })
    }

    /// Every consumer that owns entries, with how many, in no order. The
    /// unnamed one, if listed, holds the entries `XNACK` released.
    pub fn owners(&self) -> impl Iterator<Item = (&StreamName, usize)> {
        self.owners
            .iter()
            .map(|(name, owned)| (name, owned.ids.len()))
    }

    /// Checks the index against a scan of the whole list: every entry is
    /// in its owner's set, every set holds only entries the consumer owns,
    /// and no set is empty. For tests and `DEBUG`; it walks everything.
    pub fn check(&self) -> Result<(), String> {
        let mut counts: AHashMap<&[u8], usize> = AHashMap::new();
        for (id, entry) in &self.entries {
            let owner = entry.consumer.as_bytes();
            *counts.entry(owner).or_insert(0) += 1;
            match self.owners.get(owner) {
                Some(owned) if owned.ids.contains(id) => {
                    if !Arc::ptr_eq(&owned.name, &entry.consumer) {
                        return Err(format!("{id}: owner name not shared with the index"));
                    }
                }
                _ => return Err(format!("{id}: not in its consumer's index")),
            }
        }
        for (name, owned) in &self.owners {
            if owned.ids.is_empty() {
                return Err(format!("{name:?}: empty index entry"));
            }
            if counts.get(name.as_bytes()).copied() != Some(owned.ids.len()) {
                return Err(format!(
                    "{name:?}: index holds {} entries, the list {}",
                    owned.ids.len(),
                    counts.get(name.as_bytes()).copied().unwrap_or(0)
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u64) -> StreamId {
        StreamId::new(n, 0)
    }

    /// What the index must agree with: a plain map of ID to owner.
    fn owned_by(model: &BTreeMap<StreamId, String>, consumer: &str) -> Vec<StreamId> {
        model
            .iter()
            .filter(|(_, c)| c.as_str() == consumer)
            .map(|(id, _)| id.clone())
            .collect()
    }

    #[test]
    fn insert_assign_remove_keep_the_index() {
        let mut pel = Pel::new();
        pel.insert(id(1), b"a", 10, 1);
        pel.insert(id(2), b"a", 10, 1);
        pel.insert(id(3), b"b", 10, 1);
        assert_eq!(pel.consumer_len(b"a"), 2);
        assert_eq!(pel.consumer_len(b"b"), 1);
        // Re-inserted for someone else: moves, with the new details.
        let e = pel.insert(id(2), b"b", 20, 5);
        assert_eq!(
            (e.consumer().as_bytes(), e.delivered_at, e.delivery_count),
            (&b"b"[..], 20, 5)
        );
        assert_eq!(pel.consumer_len(b"a"), 1);
        assert_eq!(pel.consumer_len(b"b"), 2);
        // Assigned: moves, details kept.
        assert_eq!(pel.assign(&id(1), b"c").unwrap().delivered_at, 10);
        assert_eq!(pel.consumer_len(b"a"), 0);
        assert_eq!(pel.owners().count(), 2, "a owns nothing, so is not listed");
        assert!(pel.assign(&id(9), b"c").is_none());
        assert_eq!(pel.remove(&id(3)).unwrap().consumer(), "b");
        assert!(pel.remove(&id(3)).is_none());
        let b: Vec<_> = pel
            .consumer_range(b"b", ..)
            .map(|(id, _)| id.clone())
            .collect();
        assert_eq!(b, vec![id(2)]);
        assert_eq!(pel.remove_consumer(b"c"), 1);
        assert_eq!(pel.remove_consumer(b"c"), 0);
        assert_eq!(pel.len(), 1);
        pel.check().unwrap();
    }

    #[test]
    fn random_operations_match_a_full_scan() {
        let consumers = ["", "a", "b", "c", "d"];
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = move |n: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % n
        };
        let mut pel = Pel::new();
        let mut model: BTreeMap<StreamId, String> = BTreeMap::new();
        for step in 0..20_000 {
            let i = id(next(300));
            let c = consumers[next(consumers.len() as u64) as usize];
            match next(6) {
                0 | 1 => {
                    pel.insert(i.clone(), c.as_bytes(), step, 1);
                    model.insert(i, c.to_string());
                }
                2 => {
                    let moved = pel.assign(&i, c.as_bytes()).is_some();
                    assert_eq!(moved, model.contains_key(&i));
                    if moved {
                        model.insert(i, c.to_string());
                    }
                }
                3 | 4 => {
                    assert_eq!(pel.remove(&i).is_some(), model.remove(&i).is_some());
                }
                _ => {
                    let gone = owned_by(&model, c);
                    assert_eq!(pel.remove_consumer(c.as_bytes()), gone.len());
                    model.retain(|_, owner| owner != c);
                }
            }
            if step % 97 == 0 {
                pel.check().unwrap();
                assert_eq!(pel.len(), model.len());
                for c in consumers {
                    let want = owned_by(&model, c);
                    assert_eq!(pel.consumer_len(c.as_bytes()), want.len());
                    let lo = id(100);
                    let got: Vec<_> = pel
                        .consumer_range(c.as_bytes(), lo.clone()..)
                        .map(|(id, e)| {
                            assert_eq!(e.consumer(), c);
                            id.clone()
                        })
                        .collect();
                    let want: Vec<_> = want.into_iter().filter(|id| *id >= lo).collect();
                    assert_eq!(got, want);
                }
            }
        }
        pel.check().unwrap();
    }
}
