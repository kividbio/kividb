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
use kividb::store::{AccessScope, Relocated, ShardMap, ValueRef, Written};
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

/// A string written over an inline string goes into the same block when the
/// table alone holds it and its length keeps the block in its slab class;
/// nothing is allocated for it. Otherwise it is a new entry, as before.
#[test]
fn strings_are_written_in_place_when_the_block_still_fits() {
    let _one = one_at_a_time();
    on_a_thread(|| {
        let mut map = ShardMap::new();
        // Key "k" (1 byte) and the 12-byte header: a 100-byte string is a
        // block of 113, in the 128-byte class, which holds 113 to 128.
        assert!(matches!(map.set_str("k", bytes(100, 1)), Written::New));
        let before = kividb::memory_use::allocated() as isize;
        for (len, seed) in [(100, 2), (115, 3), (101, 4), (100, 5)] {
            assert!(
                matches!(map.set_str("k", bytes(len, seed)), Written::InPlace),
                "{len}"
            );
            assert_eq!(text(map.get("k")), Some(bytes(len, seed)));
        }
        let after = kividb::memory_use::allocated() as isize;
        assert!((after - before).abs() < 512, "{} bytes", after - before);
        // Past the class, either way: a new entry.
        assert!(matches!(
            map.set_str("k", bytes(116, 6)),
            Written::Replaced(_)
        ));
        assert_eq!(text(map.get("k")), Some(bytes(116, 6)));
        assert!(matches!(
            map.set_str("k", bytes(90, 7)),
            Written::Replaced(_)
        ));
        assert_eq!(text(map.get("k")), Some(bytes(90, 7)));
        // Not over a value of another kind, or one too long to be inline.
        map.insert("l", list(&[b"x"]));
        assert!(matches!(map.set_str("l", b"v"), Written::Replaced(_)));
        assert!(matches!(
            map.set_str("l", bytes(5_000, 1)),
            Written::Replaced(_)
        ));
        assert!(matches!(
            map.set_str("l", bytes(5_000, 2)),
            Written::Replaced(_)
        ));
        assert_eq!(text(map.get("l")), Some(bytes(5_000, 2)));

        // A time to live: kept, changed in place, or (given or taken away)
        // a new block, since it is room before the key.
        assert!(matches!(map.set_str("t", b"aaaa"), Written::New));
        assert!(matches!(
            map.set_str_expiring("t", b"bbbb", Some(5_000)),
            Written::Replaced(_)
        ));
        assert!(matches!(map.set_str("t", b"cccc"), Written::InPlace));
        assert_eq!(map.expires("t"), Some(5_000));
        assert!(matches!(
            map.set_str_expiring("t", b"dddd", Some(6_000)),
            Written::InPlace
        ));
        assert_eq!(map.expires("t"), Some(6_000));
        assert_eq!(map.ttl_len(), 1);
        assert!(matches!(
            map.set_str_expiring("t", b"eeee", None),
            Written::Replaced(_)
        ));
        assert_eq!(map.expires("t"), None);
        assert_eq!(map.ttl_len(), 0);
        assert_eq!(text(map.get("t")), Some(b"eeee".to_vec()));
    });
}

/// An entry somebody holds (a snapshot being written) is never written in
/// place: the holder keeps reading what it held.
#[test]
fn a_held_string_is_never_written_in_place() {
    let _one = one_at_a_time();
    on_a_thread(|| {
        let mut map = ShardMap::new();
        map.set_str("k", bytes(100, 1));
        let held = map.get_ref("k").unwrap();
        assert!(matches!(
            map.set_str("k", bytes(100, 2)),
            Written::Replaced(_)
        ));
        assert_eq!(map.append_in_place("k", b"x"), Some(101), "the new entry");
        let held2 = map.get_ref("k").unwrap();
        assert_eq!(map.append_in_place("k", b"y"), None);
        assert_eq!(map.set_range_in_place("k", 0, b"zz"), None);
        assert!(matches!(held.value(), ValueRef::String(b) if b.as_bytes() == &bytes(100, 1)[..]));
        let mut was = bytes(100, 2);
        was.push(b'x');
        assert!(matches!(held2.value(), ValueRef::String(b) if b.as_bytes() == &was[..]));
        assert_eq!(text(map.get("k")), Some(was));
    });
}

/// APPEND and SETRANGE in the string's own block, and where they cannot be.
#[test]
fn appends_and_ranges_in_place() {
    let _one = one_at_a_time();
    on_a_thread(|| {
        let mut map = ShardMap::new();
        assert_eq!(map.append_in_place("k", b"x"), None, "no such key");
        map.set_str("k", b"hello");
        // "k" and 5 bytes: an 18-byte block, in the class of 17 to 32.
        assert_eq!(map.append_in_place("k", b" world"), Some(11));
        assert_eq!(text(map.get("k")), Some(b"hello world".to_vec()));
        assert_eq!(map.set_range_in_place("k", 6, b"WORLD"), Some(11));
        assert_eq!(text(map.get("k")), Some(b"hello WORLD".to_vec()));
        // Past the end: the gap is zeros.
        assert_eq!(map.set_range_in_place("k", 13, b"!"), Some(14));
        assert_eq!(text(map.get("k")), Some(b"hello WORLD\0\0!".to_vec()));
        // Out of the class: left as it was.
        assert_eq!(map.append_in_place("k", &[b'.'; 10]), None);
        assert_eq!(map.set_range_in_place("k", 100, b"!"), None);
        assert_eq!(map.set_range_in_place("k", usize::MAX, b"!"), None);
        assert_eq!(text(map.get("k")), Some(b"hello WORLD\0\0!".to_vec()));
        // Not a string kept inline.
        map.insert("l", list(&[b"x"]));
        assert_eq!(map.append_in_place("l", b"x"), None);
        map.set_str("big", bytes(5_000, 1));
        assert_eq!(map.append_in_place("big", b"x"), None);
        assert_eq!(map.set_range_in_place("big", 0, b"x"), None);
    });
}

/// Strings written in place while other threads take holds on the entry
/// and read it unlocked: a held entry never changes under its holder, and
/// nothing is a data race (Miri checks).
#[test]
fn writes_in_place_beside_holders_on_other_threads() {
    let _one = one_at_a_time();
    on_a_thread(|| {
        let map = Arc::new(std::sync::RwLock::new(ShardMap::new()));
        let rounds = scaled(2_000);
        map.write().unwrap().set_str("k", bytes(100, 0));
        let readers: Vec<_> = (0..3)
            .map(|_| {
                let map = Arc::clone(&map);
                thread::spawn(move || {
                    for _ in 0..rounds {
                        let held = map.read().unwrap().get_ref("k").unwrap();
                        let first = match held.value() {
                            ValueRef::String(b) => b.to_vec(),
                            _ => panic!("a string"),
                        };
                        for _ in 0..3 {
                            assert!(matches!(held.value(), ValueRef::String(b) if b.as_bytes() == &first[..]));
                        }
                    }
                })
            })
            .collect();
        let mut in_place = 0;
        for i in 0..rounds {
            let mut m = map.write().unwrap();
            match m.set_str("k", bytes(100 + i % 10, i)) {
                Written::InPlace => in_place += 1,
                Written::Replaced(old) => drop(old),
                Written::New => panic!("the key is there"),
            }
            if i % 7 == 0 {
                m.append_in_place("k", b"z");
            }
        }
        for r in readers {
            r.join().unwrap();
        }
        // Under Miri the readers can be holding it every time.
        assert!(cfg!(miri) || in_place > 0);
    });
}

/// Inside an access scope, the lookups that use a key write the scope's
/// stamp to its word; those that only look do not; outside one, nothing
/// does. Scopes nest.
#[test]
fn lookups_write_a_keys_use_inside_an_access_scope() {
    let _one = one_at_a_time();
    on_a_thread(|| {
        let mut map = ShardMap::new();
        map.set_str("k", b"v");
        map.set_str("t", b"v");
        map.set_expires("t", 1 << 40);
        assert_eq!(map.access("k"), Some(0));
        // A clock word (top bits 01): written as it is.
        let stamp = 0x4000_0123;
        {
            let _scope = AccessScope::open(stamp);
            let _ = map.peek("k");
            let _ = map.expires("k");
            let _ = map.access("k");
            assert_eq!(map.access("k"), Some(0), "looking is no use");
            let _ = map.get("k");
            assert_eq!(map.access("k"), Some(stamp));
            {
                let _quiet = AccessScope::none();
                let _ = map.get("t");
                assert_eq!(map.access("t"), Some(0));
            }
            assert!(map.contains_key("t"));
            assert_eq!(map.access("t"), Some(stamp), "the outer scope again");
            // Writes: in place, new entries, a new key, times to live.
            map.set_str("n", b"new");
            assert_eq!(map.access("n"), Some(stamp));
            assert!(map.persist("t"));
            assert_eq!(map.access("t"), Some(stamp));
        }
        let _ = map.get("n");
        drop(map.insert_str("k", b"w"));
        assert_eq!(map.access("k"), Some(stamp), "no scope: nothing written");
        let next = 0x4000_0200;
        {
            let _scope = AccessScope::open(next);
            map.set_str("k", b"x");
            assert_eq!(map.access("k"), Some(next));
            map.insert("k", list(&[b"a"]));
            assert_eq!(map.access("k"), Some(next));
            assert!(map.set_expires("k", 1 << 40));
            let _ = map.get_expiring("k");
            assert_eq!(map.access("k"), Some(next));
            assert_eq!(map.append_in_place("x", b"y"), None);
        }
    });
}

/// Under LFU, a key is counted once a command however many times the
/// command looks it up (INCR reads it, then writes it).
#[test]
fn an_lfu_count_is_one_a_command() {
    let _one = one_at_a_time();
    on_a_thread(|| {
        use kividb::eviction::{self, Policy};
        eviction::set_policy(Policy::AllKeysLfu);
        eviction::set_lfu_log_factor(0);
        eviction::set_lfu_decay_time(0);
        let mut map = ShardMap::new();
        let now = std::time::Instant::now();
        let stamp = eviction::stamp(now);
        assert!(stamp >> 31 == 1, "an LFU stamp");
        {
            let _command = AccessScope::open(stamp);
            map.set_str("k", b"1");
        }
        let made = map.access("k").unwrap();
        assert_eq!(eviction::frequency(made), 5, "a new key is not counted");
        for round in 1..=3u8 {
            let _command = AccessScope::open(stamp);
            let _ = map.get("k");
            map.set_str("k", b"2");
            let _ = map.contains_key("k");
            map.insert("k", list(&[b"x"]));
            assert_eq!(eviction::frequency(map.access("k").unwrap()), 5 + round);
        }
        // Other keys in the same command are counted, each once.
        {
            let _command = AccessScope::open(stamp);
            map.set_str("a", b"1");
            map.set_str("b", b"1");
        }
        {
            let _command = AccessScope::open(stamp);
            for _ in 0..3 {
                let _ = map.get("a");
                let _ = map.get("b");
            }
        }
        assert_eq!(eviction::frequency(map.access("a").unwrap()), 6);
        assert_eq!(eviction::frequency(map.access("b").unwrap()), 6);
        eviction::set_lfu_log_factor(10);
        eviction::set_lfu_decay_time(1);
        eviction::set_policy(Policy::NoEviction);
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
        assert!(matches!(map.set_str("k", b"x"), Written::Replaced(_)));
        assert_eq!(map.access("k"), Some(7));
        assert!(matches!(map.set_str("k", b"y"), Written::InPlace));
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
