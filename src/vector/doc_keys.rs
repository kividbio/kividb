// The documents of an index that gives them no ids: which keys they are
// under, for the index to know how many it holds.
//
// An index with a TEXT, TAG or NUMERIC field gives each document an id
// (`postings`), and the ids in use are its count. An index of vectors alone
// gives none, and each vector index holds only the documents with a vector
// in its field, so nothing knew how many documents the index covered:
// `FT.INFO`'s `num_docs` walked every key of the database to count them
// (#590). Giving such an index ids as well would count them, at the price
// of the postings' one lock for every write of a document and of their
// lists in key order: writes of documents to an index of vectors alone were
// 12% to 16% slower for it, and each document took 112 bytes more.
//
// So each key is kept here as 96 bits of its hash, with its cluster hash
// slot, in a few tables with a lock each: a write takes the lock of its
// key's table and touches nothing the writes of other keys do. Two keys
// with the same 96 bits would be counted as one: with a billion documents
// the chance that any two are is about 1 in 10^11.

use parking_lot::Mutex;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hash, Hasher};

/// How many sets the keys are spread over: writes of different keys seldom
/// wait for each other.
const STRIPES: usize = 64;

/// 96 bits of a key's hash: 64 that its table goes by, and 32 of those
/// that chose the table. Packed, so that an entry is 14 bytes, not 24.
#[derive(PartialEq, Eq, Clone, Copy)]
#[repr(C, packed)]
struct Bits(u64, u32);

impl Hash for Bits {
    /// They are a hash already: the table goes by the 64 as they are.
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.0);
    }
}

#[derive(Default)]
struct Half(u64);

impl Hasher for Half {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, _: &[u8]) {
        unreachable!("only `Bits` are hashed")
    }

    fn write_u64(&mut self, half: u64) {
        self.0 = half;
    }
}

/// One of the tables, by a key's bits its cluster hash slot, on cache lines
/// of its own: a write to one does not take a line another thread's table
/// is on.
#[derive(Default)]
#[repr(align(128))]
struct Stripe(Mutex<HashMap<Bits, u16, BuildHasherDefault<Half>>>);

/// Which keys an index's documents are under.
pub struct DocKeys {
    stripes: Box<[Stripe]>,
    /// What chooses a key's table, and what the table goes by.
    hashers: [ahash::RandomState; 2],
}

impl std::fmt::Debug for DocKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DocKeys")
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

impl Default for DocKeys {
    fn default() -> Self {
        Self {
            stripes: (0..STRIPES).map(|_| Stripe::default()).collect(),
            hashers: [ahash::RandomState::new(), ahash::RandomState::new()],
        }
    }
}

impl DocKeys {
    /// The key's 96 bits, and the table they are kept in.
    fn place(&self, key: &str) -> (Bits, &Stripe) {
        let [chooses, goes_by] = [&self.hashers[0], &self.hashers[1]].map(|h| h.hash_one(key));
        (
            Bits(goes_by, (chooses >> 32) as u32),
            &self.stripes[chooses as usize % STRIPES],
        )
    }

    /// The document at `key` is in the index: once, however often it is
    /// written.
    pub fn add(&self, key: &str) {
        let (bits, stripe) = self.place(key);
        stripe
            .0
            .lock()
            .entry(bits)
            .or_insert_with(|| crate::cluster::key_hash_slot(key.as_bytes()));
    }

    /// The document at `key` has left the index, if it was in it.
    pub fn remove(&self, key: &str) {
        let (bits, stripe) = self.place(key);
        stripe.0.lock().remove(&bits);
    }

    /// Forget every key: the index gives its documents ids from here on
    /// (`FT.ALTER` added a field that needs them), and counts those.
    pub fn clear(&self) {
        for stripe in self.stripes.iter() {
            *stripe.0.lock() = HashMap::default();
        }
    }

    /// How many documents the index holds.
    pub fn len(&self) -> usize {
        self.stripes
            .iter()
            .map(|stripe| stripe.0.lock().len())
            .sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// How many of them are in the slots of `served`, a bit a slot
    /// (`cluster::search::served_slots`): each one's slot is looked at, as
    /// it is for an index that gives its documents ids.
    pub fn len_in(&self, served: &[u64]) -> usize {
        self.stripes
            .iter()
            .map(|stripe| {
                stripe
                    .0
                    .lock()
                    .values()
                    .filter(|&&slot| {
                        served
                            .get(slot as usize / 64)
                            .is_some_and(|bits| bits & (1 << (slot % 64)) != 0)
                    })
                    .count()
            })
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_is_counted_once_and_by_its_slot() {
        let docs = DocKeys::default();
        for key in ["doc:1", "doc:2", "doc:1"] {
            docs.add(key);
        }
        assert_eq!(docs.len(), 2);
        let slot = crate::cluster::key_hash_slot(b"doc:1") as usize;
        let mut served = vec![0u64; crate::cluster::slot::CLUSTER_SLOTS.div_ceil(64)];
        assert_eq!(docs.len_in(&served), 0);
        served[slot / 64] |= 1 << (slot % 64);
        assert_eq!(docs.len_in(&served), 1);

        docs.remove("doc:1");
        docs.remove("doc:1");
        docs.remove("doc:3");
        assert_eq!((docs.len(), docs.len_in(&served)), (1, 0));

        docs.clear();
        assert!(docs.is_empty());
        assert_eq!(docs.len_in(&vec![u64::MAX; served.len()]), 0);
    }

    #[test]
    fn many_keys_are_each_counted() {
        let docs = DocKeys::default();
        for i in 0..100_000 {
            docs.add(&format!("doc:{i}"));
        }
        assert_eq!(docs.len(), 100_000);
        let every = vec![u64::MAX; crate::cluster::slot::CLUSTER_SLOTS.div_ceil(64)];
        assert_eq!(docs.len_in(&every), 100_000);
        for i in 0..100_000 {
            docs.remove(&format!("doc:{i}"));
        }
        assert_eq!((docs.len(), docs.len_in(&every)), (0, 0));
    }
}
