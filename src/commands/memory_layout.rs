//! What the blocks of a collection take, from its structure: what
//! `MEMORY USAGE` adds up for a set, a hash and a sorted set.
//!
//! It is counted as `used_memory` counts (`memory_use`): the size each
//! allocation asks for, not what the system allocator rounds it up to. So
//! the estimate of a key is what deleting it gives back to `used_memory`,
//! which is what `maxmemory` is measured against.
//!
//! A table's allocation is known from its capacity, which costs nothing to
//! read; what its elements point to (each member, field and value is a
//! block of its own) is sampled, as Redis samples a collection's elements
//! (`SAMPLES`, 5 by default, 0 for all of them).
//!
//! A member's or value's block can be shared by more than one key (`COPY`
//! and the commands that store a set made of others share them rather than
//! copy them). Each key counts the blocks it holds in full, as Redis, whose
//! keys never share them, does: deleting one of the keys gives back only
//! what no other holds.

use std::alloc::Layout;
use std::sync::Arc;

use crate::types::HashFields;

/// Bytes of control a hashbrown table has past its buckets: one group, so
/// that a probe that starts near the end can read a whole group. The group
/// is hashbrown's choice of SIMD, made as here (and the standard library's
/// tables are hashbrown's).
const GROUP_WIDTH: usize = if cfg!(all(
    target_feature = "sse2",
    any(target_arch = "x86", target_arch = "x86_64"),
    not(miri)
)) {
    16
} else if cfg!(all(
    target_arch = "aarch64",
    target_feature = "neon",
    target_endian = "little",
    not(miri)
)) {
    8
} else {
    std::mem::size_of::<usize>()
};

/// The buckets of a hashbrown table that has room for `capacity` elements:
/// a power of two, a seventh to an eighth of them kept empty. The inverse of
/// what a table reports as its capacity, and what a table made with room for
/// `capacity` gets.
pub fn buckets_for(capacity: usize) -> usize {
    match capacity {
        0 => 0,
        1..=3 => 4,
        4..=7 => 8,
        8..=14 => 16,
        _ => (capacity.saturating_mul(8) / 7).next_power_of_two(),
    }
}

/// The one allocation of a hashbrown table of elements of `size` bytes,
/// aligned to `align`, with room for `capacity`: the buckets, then a control
/// byte for each and one group more. An empty table has none.
pub fn hash_table(capacity: usize, size: usize, align: usize) -> usize {
    let buckets = buckets_for(capacity);
    if buckets == 0 {
        return 0;
    }
    let ctrl_align = align.max(GROUP_WIDTH);
    size.saturating_mul(buckets).next_multiple_of(ctrl_align) + buckets + GROUP_WIDTH
}

/// A standard library (or `ahash`) map of `K` to `V` with room for `capacity`.
pub fn std_hash_map<K, V>(capacity: usize) -> usize {
    hash_table(
        capacity,
        std::mem::size_of::<(K, V)>(),
        std::mem::align_of::<(K, V)>(),
    )
}

/// The block of an `Arc<T>`: the two counts, then `T`.
pub fn arc_of<T>() -> usize {
    arc_layout(Layout::new::<T>())
}

/// The block of an `Arc<[u8]>` of `len` bytes: the two counts, then the
/// bytes, padded to the counts' alignment. An empty one has a block too.
/// Plain arithmetic, as it is done for every member `SAMPLES 0` looks at;
/// `an_arc_is_its_counts_and_its_value` holds it to what is allocated. A
/// slice is at most `isize::MAX` bytes, so the sum cannot overflow.
#[inline]
pub fn arc_slice(len: usize) -> usize {
    const COUNTS: usize = 2 * std::mem::size_of::<usize>();
    const ALIGN: usize = std::mem::align_of::<usize>();
    (COUNTS + len + (ALIGN - 1)) & !(ALIGN - 1)
}

fn arc_layout(value: Layout) -> usize {
    Layout::new::<[usize; 2]>()
        .extend(value)
        .expect("an Arc's layout")
        .0
        .pad_to_align()
        .size()
}

/// What `len` elements take, from the first of `samples` of them (all of
/// them for `usize::MAX`, `SAMPLES 0`): `each` gives every element's bytes,
/// in the collection's own order. As Redis does, the first ones are looked
/// at: in a hash table they are in no order of their own, so they are as
/// good a sample as any, and getting to them is no walk of the rest.
pub fn sampled<I, F>(len: usize, samples: usize, each: F) -> usize
where
    F: FnOnce() -> I,
    I: Iterator<Item = usize>,
{
    if len == 0 || samples == 0 {
        return 0;
    }
    if samples >= len {
        return each().sum();
    }
    let total: usize = each().take(samples).sum();
    (total as u128 * len as u128 / samples as u128) as usize
}

/// A hash's fields (`IndexMap`): its table of indexes, its vector of
/// entries (hash, name, value), both with room for as many as it reports,
/// and each name's and each value's block, sampled.
///
/// What it reports is the lesser room of the two. That is the vector's for
/// a hash made with room for its fields (as a snapshot loads one) and both's
/// for one that grew; but deleting fields can leave the table fewer free
/// buckets (hashbrown keeps a deleted bucket marked until it is rebuilt),
/// and then part of the vector's room is not seen and not counted.
pub fn hash_fields(fields: &HashFields, samples: usize) -> usize {
    let capacity = fields.capacity();
    let indexes = hash_table(
        capacity,
        std::mem::size_of::<usize>(),
        std::mem::align_of::<usize>(),
    );
    let entries = capacity.saturating_mul(std::mem::size_of::<(usize, String, Arc<[u8]>)>());
    let len = fields.len();
    // The fields are in the order they were set. An `IndexMap` gets to its
    // n-th entry at once, so the sample is spread over the hash rather than
    // taken from its first fields only.
    let bytes = |(name, value): (&String, &Arc<[u8]>)| name.capacity() + arc_slice(value.len());
    let each = if samples >= len {
        sampled(len, samples, || fields.iter().map(bytes))
    } else {
        let step = (len / samples.max(1)).max(1);
        sampled(len, samples, || fields.iter().step_by(step).map(bytes))
    };
    indexes + entries + each
}

/// Keys a node of the standard library's `BTreeMap` has room for (its B is
/// 6): a leaf is made with room for this many, however many it holds.
const BTREE_KEYS: usize = 11;

/// A leaf of a `BTreeMap<K, ()>`: its parent, its place in the parent and
/// its length, and room for its keys.
fn btree_leaf<K>() -> Layout {
    Layout::new::<(*const (), u16, u16, [K; BTREE_KEYS])>()
}

/// A sorted set's score index (`BTreeMap<K, ()>`) of `len` keys: one leaf
/// up to eleven of them, and past that leaves and the nodes above them.
///
/// How full the nodes are is not seen from outside the map (and is not
/// worth a walk of it): it depends on the order the keys came in. Keys that
/// come in order (ascending or descending scores, and every sorted set a
/// snapshot loads) leave nodes about 6/11 full, 7 children a node; keys
/// that come at random about 7.6/11, 8.6 children. This counts 7.75, as
/// many nodes as the keys need (fractionally), so that either order is
/// within 12% from about a thousand keys up. Fewer keys come to whole
/// nodes, and the tree can be a leaf off: measured from -18% to +42% at
/// the worst sizes (a few dozen keys, at random).
pub fn score_index<K>(len: usize) -> usize {
    const CHILDREN: f64 = 7.75;
    let leaf = btree_leaf::<K>();
    let internal = leaf
        .extend(Layout::new::<[*const (); BTREE_KEYS + 1]>())
        .expect("a B-tree node's layout")
        .0
        .pad_to_align()
        .size();
    let leaf = leaf.pad_to_align().size();
    match len {
        0 => 0,
        1..=BTREE_KEYS => leaf,
        _ => {
            let leaves = (len as f64 + 1.0) / CHILDREN;
            let leaves = leaves.max(2.0);
            let internals = (leaves / (CHILDREN - 1.0)).max(1.0);
            (leaves * leaf as f64 + internals * internal as f64) as usize
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::member_set::MemberSet;
    use crate::memory_use::allocated_by_this_thread;

    /// Runs `check` where this thread has an allocation counter of its own:
    /// here if it has, or else in a new process of this test binary running
    /// only test `name`. A process gives its first threads counters of their
    /// own (`memory_use`), and a test binary that has run many tests already
    /// has none left to give.
    fn with_own_counter(name: &str, check: impl FnOnce()) {
        const CHILD: &str = "KIVIDB_LAYOUT_TEST_CHILD";
        if allocated_by_this_thread().is_some() || std::env::var_os(CHILD).is_some() {
            return check();
        }
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                &format!("commands::memory_layout::tests::{name}"),
                "--exact",
            ])
            .args(["--test-threads", "1"])
            .env(CHILD, "1")
            .status()
            .unwrap();
        assert!(status.success(), "{name} failed in a process of its own");
    }

    /// What `make` allocates on this thread and keeps, as `used_memory`
    /// counts it.
    fn allocated<T>(make: impl FnOnce() -> T) -> (T, usize) {
        // ahash's first random state allocates its seed, once a process.
        let _ = ahash::RandomState::new();
        let before = allocated_by_this_thread().expect("a counter of its own");
        let made = make();
        let after = allocated_by_this_thread().expect("a counter of its own");
        (made, (after - before) as usize)
    }

    #[test]
    fn a_table_is_counted_as_hashbrown_allocates_it() {
        for capacity in [
            0, 1, 3, 4, 7, 8, 14, 15, 28, 29, 56, 57, 64, 65, 112, 113, 1000, 70_000,
        ] {
            let t: hashbrown::HashTable<Arc<[u8]>> = hashbrown::HashTable::with_capacity(capacity);
            assert_eq!(
                hash_table(t.capacity(), 16, 8),
                t.allocation_size(),
                "Arc<[u8]>, room for {capacity}"
            );
            let t: hashbrown::HashTable<usize> = hashbrown::HashTable::with_capacity(capacity);
            assert_eq!(
                hash_table(t.capacity(), 8, 8),
                t.allocation_size(),
                "usize, room for {capacity}"
            );
            // A table made with room for `capacity` and one that grew to hold
            // that many have as many buckets.
            assert_eq!(
                hash_table(capacity, 8, 8),
                t.allocation_size(),
                "with_capacity({capacity})"
            );
        }
    }

    #[test]
    fn a_table_that_grew_is_counted_from_its_capacity() {
        let mut t: hashbrown::HashTable<usize> = hashbrown::HashTable::new();
        for i in 0..5000usize {
            t.insert_unique(i as u64, i, |&v| v as u64);
            assert_eq!(
                hash_table(t.capacity(), 8, 8),
                t.allocation_size(),
                "{} elements",
                i + 1
            );
        }
    }

    #[test]
    fn a_set_is_what_it_allocates() {
        with_own_counter("a_set_is_what_it_allocates", || {
            for members in [1usize, 7, 8, 64, 65, 1000] {
                let (set, bytes) = allocated(|| {
                    let mut set = MemberSet::new();
                    for i in 0..members {
                        set.insert_bytes(format!("member:{i}").as_bytes());
                    }
                    set
                });
                let estimate = set.allocation_size()
                    + sampled(set.len(), usize::MAX, || {
                        set.iter().map(|m| arc_slice(m.len()))
                    });
                assert_eq!(estimate, bytes, "{members} members");
            }
        });
    }

    #[test]
    fn a_hash_is_what_it_allocates() {
        with_own_counter("a_hash_is_what_it_allocates", || {
            for fields in [1usize, 3, 4, 8, 15, 64, 65, 513, 1000] {
                let (hash, bytes) = allocated(|| {
                    let mut hash = HashFields::default();
                    for i in 0..fields {
                        hash.insert(
                            format!("field:{i}"),
                            Arc::from(format!("value:{i}").as_bytes()),
                        );
                    }
                    hash
                });
                assert_eq!(hash_fields(&hash, usize::MAX), bytes, "{fields} fields");
            }
        });
    }

    #[test]
    fn a_std_map_is_what_it_allocates() {
        with_own_counter("a_std_map_is_what_it_allocates", || {
            for len in [1usize, 3, 4, 14, 15, 100, 1000] {
                let (map, bytes) = allocated(|| {
                    let mut map: ahash::AHashMap<u64, std::time::Instant> = ahash::AHashMap::new();
                    for i in 0..len as u64 {
                        map.insert(i, std::time::Instant::now());
                    }
                    map
                });
                assert_eq!(
                    std_hash_map::<u64, std::time::Instant>(map.capacity()),
                    bytes,
                    "{len}"
                );
            }
        });
    }

    #[test]
    fn an_arc_is_its_counts_and_its_value() {
        with_own_counter("an_arc_is_its_counts_and_its_value", || {
            for len in [0usize, 1, 7, 8, 9, 100] {
                let (arc, bytes) = allocated(|| Arc::<[u8]>::from(vec![0u8; len].as_slice()));
                // The vector it was made from is freed again.
                assert_eq!(arc_slice(arc.len()), bytes, "{len} bytes");
            }
            let (_arc, bytes) = allocated(|| Arc::new(parking_lot::RwLock::new(MemberSet::new())));
            assert_eq!(arc_of::<parking_lot::RwLock<MemberSet>>(), bytes);
        });
    }

    #[test]
    fn a_sample_is_scaled_to_the_whole() {
        assert_eq!(sampled(0, 5, || std::iter::repeat(9)), 0);
        assert_eq!(sampled(10, usize::MAX, || 1..=10), 55);
        // The first five of a hundred, each 4 bytes: four hundred.
        assert_eq!(sampled(100, 5, || std::iter::repeat(4)), 400);
        // More samples than elements: all of them, once.
        assert_eq!(sampled(3, 100, || [1, 2, 3].into_iter()), 6);
    }

    #[test]
    fn a_score_index_is_within_its_bounds_of_what_it_allocates() {
        use std::collections::BTreeMap;
        type Key = (ordered_float::OrderedFloat<f64>, Arc<[u8]>);
        with_own_counter(
            "a_score_index_is_within_its_bounds_of_what_it_allocates",
            || {
                let member: Arc<[u8]> = Arc::from(&b"m"[..]);
                let key = |score: u64| (ordered_float::OrderedFloat(score as f64), member.clone());
                let mut seed = 0x9E37_79B9_7F4A_7C15u64;
                let mut random = move || {
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    seed
                };
                for len in [
                    1usize, 5, 11, 12, 13, 20, 50, 65, 100, 128, 500, 1000, 5000, 50_000,
                ] {
                    let orders: [(&str, Vec<u64>); 3] = [
                        ("ascending", (0..len as u64).collect()),
                        ("descending", (0..len as u64).rev().collect()),
                        ("random", (0..len).map(|_| random()).collect()),
                    ];
                    for (order, scores) in orders {
                        let (_tree, bytes) = allocated(|| {
                            let mut tree: BTreeMap<Key, ()> = BTreeMap::new();
                            for &s in &scores {
                                tree.insert(key(s), ());
                            }
                            tree
                        });
                        let estimate = score_index::<Key>(len);
                        let error = (estimate as f64 - bytes as f64) / bytes as f64;
                        let (under, over) = match len {
                            // One leaf, or two and a root: exact.
                            0..=13 => (0.0, 0.0),
                            14..=999 => (0.20, 0.45),
                            _ => (0.13, 0.13),
                        };
                        assert!(
                            (-under..=over).contains(&error),
                            "{len} keys, {order}: estimate {estimate}, allocated {bytes}"
                        );
                    }
                }
            },
        );
    }
}
