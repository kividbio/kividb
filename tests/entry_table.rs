// tests/entry_table.rs
//
// A shard's table of entries (src/store/entry.rs, src/store/shard_map.rs):
// each key and its value in one block, behind an eight-byte slot. The
// entries are unsafe code, so this file is also what Miri runs:
//   cargo +nightly miri test --test entry_table
// A binary of its own, so the count of slabs in use means what these tests
// did. The tests run one at a time, and make entries on threads of their
// own, which let go of their slabs when they end.
// Run with: cargo test --test entry_table

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::thread;

use kividb::shared_bytes::SharedBytes;
use kividb::store::{NewEntry, Relocated, ShardMap, ValueRef};
use kividb::types::KiviDBValue;
use kividb::value_slab;
use parking_lot::RwLock;

static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

fn one_at_a_time() -> std::sync::MutexGuard<'static, ()> {
    ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner())
}

/// `n`, or a fraction of it under Miri, which runs code a thousand times
/// slower: enough there to look for undefined behaviour.
fn scaled(n: usize) -> usize {
    if cfg!(miri) { (n / 50).max(20) } else { n }
}

fn bytes(len: usize, seed: usize) -> Vec<u8> {
    (0..len)
        .map(|i| ((i * 31 + seed * 7) % 251) as u8)
        .collect()
}

fn key(len: usize, seed: usize) -> String {
    let mut k = format!("k{seed}-");
    while k.len() < len {
        k.push((b'a' + (k.len() % 26) as u8) as char);
    }
    k
}

fn text(value: Option<ValueRef<'_>>) -> Option<Vec<u8>> {
    match value {
        Some(ValueRef::String(b)) => Some(b.to_vec()),
        _ => None,
    }
}

fn list(items: &[&[u8]]) -> KiviDBValue {
    KiviDBValue::List(Arc::new(RwLock::new(
        items
            .iter()
            .map(|i| Arc::<[u8]>::from(*i))
            .collect::<VecDeque<_>>(),
    )))
}

/// Run `f` on a thread of its own, then check every slab it took was given
/// back.
fn on_a_thread<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    let before = value_slab::slabs_in_use();
    let out = thread::spawn(f).join().unwrap();
    assert_eq!(value_slab::slabs_in_use(), before, "a slab not given back");
    out
}

#[test]
fn strings_of_every_shape_read_back() {
    let _one = one_at_a_time();
    on_a_thread(|| {
        let mut map = ShardMap::new();
        // Inline at every class's edge, at the largest slab block, past it
        // (a SharedBytes of its own), and keys short, at the long-key edge
        // and long.
        let value_lens: Vec<usize> = if cfg!(miri) {
            vec![0, 1, 100, 500, 1000, 4000, 4084, 4085, 5000]
        } else {
            (0..=1100)
                .chain([2000, 4000, 4080, 4084, 4085, 4096, 5000, 70_000])
                .collect()
        };
        let key_lens = [1usize, 15, 254, 255, 300, 1000];
        let mut want = Vec::new();
        for (i, &vlen) in value_lens.iter().enumerate() {
            for (j, &klen) in key_lens.iter().enumerate() {
                let seed = i * 10 + j;
                let k = key(klen, seed);
                let v = bytes(vlen, seed);
                if seed.is_multiple_of(2) {
                    assert!(map.insert_str(&k, &v).is_none());
                } else {
                    let value = KiviDBValue::String(SharedBytes::from(&v[..]));
                    assert!(map.insert(&k, value).is_none());
                }
                want.push((k, v));
            }
        }
        assert_eq!(map.len(), want.len());
        for (k, v) in &want {
            assert_eq!(
                text(map.get(k)).as_ref(),
                Some(v),
                "{} / {}",
                k.len(),
                v.len()
            );
            let found = map.keys().filter(|key| *key == k.as_str()).count();
            assert_eq!(found, 1);
        }
        let mut seen = 0;
        for (k, v) in map.iter() {
            assert!(matches!(v, ValueRef::String(_)));
            assert!(!k.is_empty());
            seen += 1;
        }
        assert_eq!(seen, want.len());
        // Half removed, their values taken whole; the rest dropped with the
        // map.
        for (k, v) in want.iter().step_by(2) {
            let taken = map.remove(k).expect("there");
            assert_eq!(taken.key(), k);
            match taken.into_value() {
                KiviDBValue::String(b) => assert_eq!(&b[..], &v[..]),
                _ => panic!("not a string"),
            }
            assert!(map.get(k).is_none());
        }
    });
}

#[test]
fn collections_are_kept_whole_and_dropped_once() {
    let _one = one_at_a_time();
    on_a_thread(|| {
        let handle = list(&[b"a", b"b"]);
        let KiviDBValue::List(arc) = &handle else {
            unreachable!()
        };
        let arc = Arc::clone(arc);
        let mut map = ShardMap::new();
        map.insert("l", handle);
        assert_eq!(Arc::strong_count(&arc), 2);
        match map.get("l") {
            Some(ValueRef::List(l)) => {
                assert!(Arc::ptr_eq(l, &arc));
                l.write().push_back(Arc::from(&b"c"[..]));
            }
            _ => panic!("not a list"),
        }
        assert_eq!(arc.read().len(), 3);
        // Replaced by a string: the list goes with what insert gave back.
        let old = map.insert_str("l", b"now a string").expect("had one");
        assert!(matches!(old.value(), ValueRef::List(_)));
        assert_eq!(Arc::strong_count(&arc), 2);
        drop(old);
        assert_eq!(Arc::strong_count(&arc), 1);
        // Taken out whole: the same handle, not a copy.
        map.insert("m", KiviDBValue::List(Arc::clone(&arc)));
        let back = map.remove("m").unwrap().into_value();
        match &back {
            KiviDBValue::List(l) => assert!(Arc::ptr_eq(l, &arc)),
            _ => panic!("not a list"),
        }
        assert_eq!(Arc::strong_count(&arc), 2);
        drop(back);
        // Dropped with the map.
        map.insert(key(400, 1), KiviDBValue::List(Arc::clone(&arc)));
        assert_eq!(Arc::strong_count(&arc), 2);
        drop(map);
        assert_eq!(Arc::strong_count(&arc), 1);
    });
}

#[test]
fn a_held_entry_outlives_its_key() {
    let _one = one_at_a_time();
    on_a_thread(|| {
        let mut map = ShardMap::new();
        let v = bytes(300, 3);
        map.insert_str("k", &v);
        let held = map.get_ref("k").unwrap();
        assert!(map.holds("k", &held));
        let again = held.clone();
        // Overwritten: the held entry is not the key's any more, and has not
        // changed.
        drop(map.insert_str("k", b"new"));
        assert!(!map.holds("k", &held));
        assert_eq!(held.key(), "k");
        assert!(matches!(held.value(), ValueRef::String(b) if b.as_bytes() == &v[..]));
        // Gone from the table altogether, and from the map.
        drop(map.remove("k"));
        drop(map);
        drop(held);
        assert!(matches!(again.value(), ValueRef::String(b) if b.as_bytes() == &v[..]));
    });
}

#[test]
fn holds_are_counted_across_threads() {
    let _one = one_at_a_time();
    on_a_thread(|| {
        // The standard library's lock: Miri does not take parking_lot's
        // futex call when threads wait.
        let map = Arc::new(std::sync::RwLock::new(ShardMap::new()));
        let rounds = scaled(2_000);
        map.write().unwrap().insert_str("k", bytes(100, 0));
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let map = Arc::clone(&map);
                thread::spawn(move || {
                    let mut kept = Vec::new();
                    for i in 0..rounds {
                        let held = map.read().unwrap().get_ref("k");
                        if let Some(held) = held {
                            assert_eq!(held.key(), "k");
                            if let ValueRef::String(b) = held.value() {
                                assert_eq!(b.len(), 100);
                            }
                            if i.is_multiple_of(3) {
                                kept.push(held.clone());
                            }
                        }
                        if kept.len() > 8 {
                            kept.clear();
                        }
                    }
                })
            })
            .collect();
        for i in 0..rounds {
            let old = map.write().unwrap().insert_str("k", bytes(100, i));
            drop(old);
        }
        for r in readers {
            r.join().unwrap();
        }
    });
}

#[test]
fn an_entry_made_unlocked_and_never_put_is_freed() {
    let _one = one_at_a_time();
    on_a_thread(|| {
        let start = kividb::memory_use::allocated() as isize;
        for len in [10usize, 1000, 10_000] {
            drop(NewEntry::string("k", &bytes(len, 1)));
            drop(NewEntry::new("k", list(&[b"x"])));
        }
        let mut map = ShardMap::new();
        map.insert_entry(NewEntry::string("k", b"v"));
        assert_eq!(text(map.get("k")), Some(b"v".to_vec()));
        drop(map);
        let left = kividb::memory_use::allocated() as isize - start;
        // The count is the process's: another thread can be letting go of a
        // little of its own.
        assert!(left.abs() < 4_096, "{left} bytes left counted");
    });
}

#[test]
fn compaction_moves_entries_and_their_strings_out_of_sparse_slabs() {
    let _one = one_at_a_time();
    on_a_thread(|| {
        let n = scaled(6_000);
        // Made on a thread that ends, so that its slabs are nobody's.
        let mut map = thread::spawn(move || {
            let mut map = ShardMap::new();
            for i in 0..n {
                if i.is_multiple_of(50) {
                    // Too long together to be inline, short enough for the
                    // string's own block to be in a slab.
                    map.insert_str(key(1000, i), bytes(3000, i));
                } else {
                    map.insert_str(key(15, i), bytes(100, i));
                }
            }
            map
        })
        .join()
        .unwrap();
        let klen = |i: usize| if i.is_multiple_of(50) { 1000 } else { 15 };
        let vlen = |i: usize| if i.is_multiple_of(50) { 3000 } else { 100 };
        // Most deleted at random places: sparse slabs.
        for i in (0..n).filter(|i| !i.is_multiple_of(5)) {
            map.remove(&key(klen(i), i));
        }
        let held = map.get_ref(&key(15, 5)).unwrap();
        // Each key's access word, to see that it moves with its entry.
        for i in (0..n).filter(|i| i.is_multiple_of(5)) {
            map.touch(key(klen(i), i).as_bytes(), |_| i as u32 + 1);
        }
        let chosen = value_slab::plan_evacuation(usize::MAX);
        assert!(chosen > 0);
        let regions = value_slab::regions_snapshot();
        let (mut moved, mut kept) = (Vec::new(), 0);
        for index in 0..map.buckets() {
            if !map.evacuating_at(index, &regions) {
                continue;
            }
            match map.relocate_at(index, &regions) {
                Relocated::Moved(left) => moved.push(left),
                Relocated::Held => kept += 1,
                Relocated::Stays => panic!("evacuating, yet stays"),
            }
        }
        assert!(!moved.is_empty());
        assert!(kept <= 1, "{kept} held");
        drop(moved);
        for i in (0..n).filter(|i| i.is_multiple_of(5)) {
            let k = key(klen(i), i);
            assert_eq!(text(map.get(&k)), Some(bytes(vlen(i), i)), "{i}");
            assert_eq!(map.access(&k), Some(i as u32 + 1), "{i}");
        }
        assert!(matches!(held.value(), ValueRef::String(b) if b.as_bytes() == &bytes(100, 5)[..]));
        drop(held);
        value_slab::finish_evacuation();
        drop(map);
    });
}

/// A key's access word (when it was last used, or how often) stays with
/// the key through every write that makes it a new entry, and is written
/// only when it changes.
#[test]
fn the_access_word_follows_the_key() {
    let _one = one_at_a_time();
    on_a_thread(|| {
        let mut map = ShardMap::new();
        assert_eq!(map.access("k"), None);
        map.touch(b"k", |_| panic!("no such key"));
        map.insert_str("k", b"v");
        assert_eq!(map.access("k"), Some(0), "a new entry's word");
        map.touch(b"k", |w| w + 7);
        assert_eq!(map.access("k"), Some(7));
        // Written over: by a string inline, a long one, a collection, an
        // entry made unlocked.
        drop(map.insert_str("k", b"w"));
        assert_eq!(map.access("k"), Some(7));
        drop(map.insert_str("k", bytes(10_000, 1)));
        assert_eq!(map.access("k"), Some(7));
        drop(map.insert("k", list(&[b"a", b"b"])));
        assert_eq!(map.access("k"), Some(7));
        drop(map.insert_entry(NewEntry::string("k", b"x")));
        assert_eq!(map.access("k"), Some(7));
        // A time to live given (a new entry), changed in place, changed
        // while the entry is held (a new entry again), taken away.
        assert!(map.set_expires("k", 1_000));
        assert_eq!(map.access("k"), Some(7));
        assert!(map.set_expires("k", 2_000));
        let held = map.get_ref("k").unwrap();
        assert!(map.set_expires("k", 3_000));
        assert_eq!(map.access("k"), Some(7));
        drop(map.insert_str_expiring("k", b"y", Some(4_000)));
        assert_eq!(map.access("k"), Some(7));
        assert!(map.persist("k"));
        assert_eq!(map.access("k"), Some(7));
        drop(held);
        // `touch` sees what is there, and the same word is no write.
        map.touch(b"k", |w| {
            assert_eq!(w, 7);
            w
        });
        drop(map.remove("k"));
        assert_eq!(map.access("k"), None);
        // A key made anew starts from nothing.
        map.insert_str("k", b"v");
        assert_eq!(map.access("k"), Some(0));
    });
}

/// The access word is the one thing written in a held entry, and it is
/// written with the shard locked only for reading: readers touching a key
/// while others read its held entries, unlocked, is no data race (Miri
/// checks).
#[test]
fn touching_a_held_entry_changes_nothing_a_holder_reads() {
    let _one = one_at_a_time();
    on_a_thread(|| {
        let map = Arc::new(std::sync::RwLock::new(ShardMap::new()));
        let rounds = scaled(2_000);
        let v = bytes(100, 9);
        map.write().unwrap().insert_str("k", &v);
        let held = map.read().unwrap().get_ref("k").unwrap();
        let toucher = {
            let map = Arc::clone(&map);
            thread::spawn(move || {
                for i in 0..rounds {
                    map.read().unwrap().touch(b"k", |_| i as u32);
                }
            })
        };
        let reader = {
            let held = held.clone();
            let v = v.clone();
            thread::spawn(move || {
                for _ in 0..rounds {
                    assert_eq!(held.key(), "k");
                    assert!(matches!(held.value(), ValueRef::String(b) if b.as_bytes() == &v[..]));
                }
            })
        };
        for i in 0..rounds {
            if i.is_multiple_of(10) {
                // A write: a new entry, the held one left as it was.
                drop(map.write().unwrap().insert_str("k", &v));
            }
            let _ = map.read().unwrap().access("k");
        }
        toucher.join().unwrap();
        reader.join().unwrap();
        assert!(matches!(held.value(), ValueRef::String(b) if b.as_bytes() == &v[..]));
    });
}

#[test]
fn growing_and_shrinking_keep_every_entry() {
    let _one = one_at_a_time();
    on_a_thread(|| {
        let n = scaled(20_000);
        let mut map = ShardMap::with_capacity(4);
        for i in 0..n {
            map.insert_str(key(20, i), bytes(i % 200, i));
        }
        for i in (0..n).filter(|i| !i.is_multiple_of(10)) {
            assert!(map.remove(&key(20, i)).is_some());
        }
        map.shrink_to(0);
        assert_eq!(map.len(), n.div_ceil(10));
        for i in (0..n).step_by(10) {
            assert_eq!(text(map.get(&key(20, i))), Some(bytes(i % 200, i)));
        }
        let mut rng = 12345usize;
        let (k, _) = map
            .random_entry(|| {
                rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                rng >> 7
            })
            .unwrap();
        assert!(map.contains_key(k));
        map.clear();
        assert!(map.is_empty());
    });
}

#[test]
fn every_size_up_to_4_kib_has_the_smallest_class_that_holds_it() {
    let mut last = None;
    for size in 1..=value_slab::MAX_BLOCK {
        let class = value_slab::class_of(size).expect("a slab's");
        let block = value_slab::block_size(class);
        assert!(block >= size, "{size} in {block}");
        assert_eq!(block % 16, 0);
        if class > 0 {
            assert!(value_slab::block_size(class - 1) < size, "{size}");
        }
        assert!(last.is_none_or(|l| l == class || l + 1 == class));
        last = Some(class);
    }
    assert!(value_slab::class_of(value_slab::MAX_BLOCK + 1).is_none());
    // A memtier key and a 1 KB value, inline.
    assert_eq!(
        value_slab::block_size(value_slab::class_of(12 + 15 + 1024).unwrap()),
        1056
    );
}
