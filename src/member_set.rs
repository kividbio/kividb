//! The members of a set (`SADD`): a hash table that can also hand out a
//! member at random without looking at the others.
//!
//! The standard library's `HashSet` cannot: the only way to a random member
//! is to walk the table. `SPOP` and `SRANDMEMBER` did, cloning every member
//! on the way, and on a set of a hundred thousand members answered a few
//! thousand times a second where every other command answered a million.
//!
//! This is the same table (hashbrown's, as the standard library's is), held
//! directly so that a bucket can be asked for by its number: a random member
//! is a random bucket, tried again while it is empty, as Redis picks one. A
//! table is never less than a quarter full for long (it is shrunk below
//! that: see [`MemberSet::remove`]), so that is a handful of tries.

use std::sync::Arc;

use hashbrown::HashTable;

/// A set of byte strings. Each member is shared (`Arc`), as the commands that
/// copy members between sets and into replies want it.
#[derive(Clone, Default)]
pub struct MemberSet {
    table: HashTable<Arc<[u8]>>,
    hasher: ahash::RandomState,
}

impl MemberSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(members: usize) -> Self {
        Self {
            table: HashTable::with_capacity(members),
            hasher: ahash::RandomState::new(),
        }
    }

    #[inline]
    fn hash(&self, member: &[u8]) -> u64 {
        self.hasher.hash_one(member)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.table.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.table.capacity()
    }

    #[inline]
    pub fn contains(&self, member: &[u8]) -> bool {
        self.get(member).is_some()
    }

    #[inline]
    pub fn get(&self, member: &[u8]) -> Option<&Arc<[u8]>> {
        self.table.find(self.hash(member), |have| &**have == member)
    }

    /// Adds `member`. False if it was there already.
    pub fn insert(&mut self, member: Arc<[u8]>) -> bool {
        let hash = self.hash(&member);
        let hasher = &self.hasher;
        match self.table.entry(
            hash,
            |have| **have == *member,
            |have| hasher.hash_one(&**have),
        ) {
            hashbrown::hash_table::Entry::Occupied(_) => false,
            hashbrown::hash_table::Entry::Vacant(place) => {
                place.insert(member);
                true
            }
        }
    }

    /// Adds `member`, making the shared copy of it only if it is new.
    /// False if it was there already.
    pub fn insert_bytes(&mut self, member: &[u8]) -> bool {
        let hash = self.hash(member);
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

    /// Removes `member`. False if it was not there.
    pub fn remove(&mut self, member: &[u8]) -> bool {
        self.take(member).is_some()
    }

    /// Removes `member` and hands it back.
    pub fn take(&mut self, member: &[u8]) -> Option<Arc<[u8]>> {
        let hash = self.hash(member);
        let found = self.table.find_entry(hash, |have| &**have == member).ok()?;
        let (taken, _) = found.remove();
        self.shrink_if_sparse();
        Some(taken)
    }

    /// A table that has lost most of its members is made smaller: it would
    /// otherwise keep the memory of its largest size for ever, and a random
    /// bucket of it would nearly always be an empty one.
    fn shrink_if_sparse(&mut self) {
        let buckets = self.table.num_buckets();
        if buckets > 64 && self.table.len() * 8 < buckets {
            let hasher = &self.hasher;
            self.table
                .shrink_to(self.table.len() * 2, |have| hasher.hash_one(&**have));
        }
    }

    pub fn clear(&mut self) {
        self.table.clear();
    }

    pub fn reserve(&mut self, more: usize) {
        let hasher = &self.hasher;
        self.table.reserve(more, |have| hasher.hash_one(&**have));
    }

    pub fn retain(&mut self, mut keep: impl FnMut(&Arc<[u8]>) -> bool) {
        self.table.retain(|have| keep(have));
        self.shrink_if_sparse();
    }

    pub fn iter(&self) -> hashbrown::hash_table::Iter<'_, Arc<[u8]>> {
        self.table.iter()
    }

    /// The bucket a random member is in, if there is a member at all.
    fn random_bucket(&self, rng: &mut impl rand::Rng) -> Option<usize> {
        if self.table.is_empty() {
            return None;
        }
        let buckets = self.table.num_buckets();
        for _ in 0..64 {
            let at = rng.gen_range(0..buckets);
            if self.table.get_bucket(at).is_some() {
                return Some(at);
            }
        }
        // A table made large and filled little (`with_capacity`): sixty-four
        // empty buckets in a row. Walk to a member instead.
        use rand::seq::IteratorRandom;
        let member = self.table.iter().choose(rng)?;
        self.table
            .find_bucket_index(self.hash(member), |have| Arc::ptr_eq(have, member))
    }

    /// A member, any one as likely as another.
    pub fn random(&self, rng: &mut impl rand::Rng) -> Option<&Arc<[u8]>> {
        self.table.get_bucket(self.random_bucket(rng)?)
    }

    /// Removes a member, any one as likely as another.
    pub fn pop_random(&mut self, rng: &mut impl rand::Rng) -> Option<Arc<[u8]>> {
        let at = self.random_bucket(rng)?;
        let (taken, _) = self.table.get_bucket_entry(at).ok()?.remove();
        self.shrink_if_sparse();
        Some(taken)
    }

    /// Up to `count` different members, chosen at random, without removing
    /// them.
    ///
    /// A few of many are picked one at a time, picking again when one comes
    /// up twice. Most of them, or all, are found by walking the table once,
    /// which is then no more work than the answer is long.
    pub fn random_distinct(&self, count: usize, rng: &mut impl rand::Rng) -> Vec<Arc<[u8]>> {
        use rand::seq::IteratorRandom;
        let len = self.len();
        if count >= len {
            return self.iter().cloned().collect();
        }
        if count * 3 > len {
            return self
                .iter()
                .choose_multiple(rng, count)
                .into_iter()
                .cloned()
                .collect();
        }
        let mut seen: ahash::AHashSet<usize> = ahash::AHashSet::with_capacity(count);
        let mut picked = Vec::with_capacity(count);
        while picked.len() < count {
            let Some(at) = self.random_bucket(rng) else {
                break;
            };
            if seen.insert(at)
                && let Some(member) = self.table.get_bucket(at)
            {
                picked.push(Arc::clone(member));
            }
        }
        picked
    }
}

impl<'a> IntoIterator for &'a MemberSet {
    type Item = &'a Arc<[u8]>;
    type IntoIter = hashbrown::hash_table::Iter<'a, Arc<[u8]>>;
    fn into_iter(self) -> Self::IntoIter {
        self.table.iter()
    }
}

impl IntoIterator for MemberSet {
    type Item = Arc<[u8]>;
    type IntoIter = hashbrown::hash_table::IntoIter<Arc<[u8]>>;
    fn into_iter(self) -> Self::IntoIter {
        self.table.into_iter()
    }
}

impl Extend<Arc<[u8]>> for MemberSet {
    fn extend<I: IntoIterator<Item = Arc<[u8]>>>(&mut self, members: I) {
        for member in members {
            self.insert(member);
        }
    }
}

impl FromIterator<Arc<[u8]>> for MemberSet {
    fn from_iter<I: IntoIterator<Item = Arc<[u8]>>>(members: I) -> Self {
        let mut set = Self::new();
        set.extend(members);
        set
    }
}

impl std::fmt::Debug for MemberSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_set().entries(self.iter()).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(i: usize) -> Arc<[u8]> {
        Arc::from(format!("member-{i}").as_bytes())
    }

    #[test]
    fn it_is_a_set() {
        let mut set = MemberSet::new();
        assert!(set.is_empty());
        for i in 0..1000 {
            assert!(set.insert(member(i)));
        }
        for i in 0..1000 {
            assert!(!set.insert(member(i)), "{i} was added twice");
            assert!(set.contains(&member(i)));
        }
        assert_eq!(set.len(), 1000);
        assert!(!set.insert_bytes(&member(3)));
        assert!(set.insert_bytes(b"from bytes"));
        assert!(!set.insert_bytes(b"from bytes"));
        assert!(set.remove(b"from bytes"));
        assert!(!set.contains(b"nobody"));
        assert!(set.remove(&member(7)));
        assert!(!set.remove(&member(7)));
        assert_eq!(set.take(&member(8)).as_deref(), Some(&member(8)[..]));
        assert_eq!(set.len(), 998);
        set.retain(|m| m.ends_with(b"0"));
        assert_eq!(set.len(), 100);
        let copy: MemberSet = set.iter().cloned().collect();
        assert_eq!(copy.len(), 100);
        assert!(set.iter().all(|m| copy.contains(m)));
    }

    #[test]
    fn popping_at_random_empties_it_and_gives_each_member_once() {
        let mut rng = rand::thread_rng();
        let mut set: MemberSet = (0..5000).map(member).collect();
        let mut popped = std::collections::HashSet::new();
        while let Some(m) = set.pop_random(&mut rng) {
            assert!(popped.insert(m), "a member was popped twice");
        }
        assert_eq!(popped.len(), 5000);
        assert!(set.is_empty());
        assert!(set.random(&mut rng).is_none());
        // What it grew to was given back as it emptied.
        assert!(set.capacity() < 256, "{}", set.capacity());
    }

    #[test]
    fn a_random_member_is_any_of_them() {
        let mut rng = rand::thread_rng();
        let set: MemberSet = (0..10).map(member).collect();
        let mut times = std::collections::HashMap::new();
        for _ in 0..20_000 {
            *times
                .entry(set.random(&mut rng).unwrap().clone())
                .or_insert(0u32) += 1;
        }
        assert_eq!(times.len(), 10);
        // Two thousand each, give or take: none is left out or favoured.
        assert!(
            times.values().all(|n| (1500..2500).contains(n)),
            "{times:?}"
        );
    }

    #[test]
    fn several_at_random_are_different_and_no_more_than_there_are() {
        let mut rng = rand::thread_rng();
        let set: MemberSet = (0..1000).map(member).collect();
        for count in [0, 1, 5, 300, 400, 999, 1000, 5000] {
            let picked = set.random_distinct(count, &mut rng);
            assert_eq!(picked.len(), count.min(1000), "asked for {count}");
            let different: std::collections::HashSet<_> = picked.iter().collect();
            assert_eq!(different.len(), picked.len(), "asked for {count}");
            assert!(picked.iter().all(|m| set.contains(m)));
        }
    }
}
