// tests/value_slab.rs
//
// The slabs that hold small string values (#113, src/value_slab.rs), through
// `SharedBytes`. A binary of its own: nothing else here makes values, so the
// count of slabs in use means what these tests did. The tests run one at a
// time, and make values only on threads of their own, which let go of their
// slabs when they end.
// Run with: cargo test --test value_slab

use std::collections::HashSet;
use std::sync::Mutex;
use std::sync::mpsc;
use std::thread;

use kividb::shared_bytes::SharedBytes;
use kividb::value_slab::{self, SLAB};

static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

/// `n`, or a few hundredth of it under Miri, which runs code a thousand times
/// slower: enough there to look for undefined behaviour, not to measure.
fn scaled(n: usize) -> usize {
    if cfg!(miri) { (n / 300).max(20) } else { n }
}

fn bytes(len: usize, seed: usize) -> Vec<u8> {
    (0..len)
        .map(|i| ((i * 31 + seed * 7) % 251) as u8)
        .collect()
}

fn slab_of(value: &SharedBytes) -> usize {
    value.as_slice().as_ptr() as usize & !(SLAB - 1)
}

/// A small, fast, reproducible sequence: no crate needed.
struct Lcg(u64);
impl Lcg {
    fn next(&mut self, below: usize) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.0 >> 33) as usize) % below
    }
}

#[test]
fn values_of_every_size_hold_their_bytes() {
    let _one = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    let before = value_slab::slabs_in_use();
    thread::spawn(|| {
        // Every class, both sides of the largest, and the system's sizes.
        let values: Vec<(Vec<u8>, SharedBytes)> = (0..=600)
            .chain([4096, 70_000])
            .map(|len| {
                let want = bytes(len, len);
                let value = SharedBytes::from(&want[..]);
                (want, value)
            })
            .collect();
        for (want, value) in &values {
            assert_eq!(&value[..], &want[..], "{} bytes", want.len());
        }
    })
    .join()
    .unwrap();
    assert_eq!(value_slab::slabs_in_use(), before);
}

#[test]
fn a_slab_emptied_by_another_thread_is_given_back() {
    let _one = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    let before = value_slab::slabs_in_use();
    // Made on one thread, which ends; freed on another.
    let values = thread::spawn(|| {
        (0..20_000)
            .map(|i| SharedBytes::from(&bytes(100, i)[..]))
            .collect::<Vec<_>>()
    })
    .join()
    .unwrap();
    let slabs: HashSet<usize> = values.iter().map(slab_of).collect();
    // 20,000 blocks of 112 bytes, 585 a slab.
    assert!(slabs.len() >= 34, "{} slabs", slabs.len());
    let generations: Vec<(usize, u32)> = slabs
        .iter()
        .map(|&slab| (slab, value_slab::generation_of(slab as *const u8)))
        .collect();
    assert_eq!(value_slab::slabs_in_use(), before + slabs.len());
    for (i, value) in values.iter().enumerate() {
        assert_eq!(&value[..], &bytes(100, i)[..]);
    }
    drop(values);
    for (slab, generation) in generations {
        assert_ne!(
            value_slab::generation_of(slab as *const u8),
            generation,
            "a slab of freed values was not emptied"
        );
    }
    assert_eq!(value_slab::slabs_in_use(), before);
}

#[test]
fn a_thread_that_makes_and_drops_values_uses_one_slab() {
    let _one = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    let slabs = thread::spawn(|| {
        let mut slabs = HashSet::new();
        for i in 0..scaled(200_000) {
            let value = SharedBytes::from(&bytes(100, i)[..]);
            slabs.insert(slab_of(&value));
        }
        slabs.len()
    })
    .join()
    .unwrap();
    assert_eq!(slabs, 1);
}

#[test]
fn values_counted_in_used_memory_by_their_block() {
    let _one = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    let (made, sent) = mpsc::channel();
    let (done, finished) = mpsc::channel::<()>();
    let maker = thread::spawn(move || {
        let start = kividb::memory_use::allocated();
        let values: Vec<SharedBytes> = (0..10_000)
            .map(|i| SharedBytes::from(&bytes(100, i)[..]))
            .collect();
        let held = kividb::memory_use::allocated() - start;
        made.send(held).unwrap();
        finished.recv().unwrap();
        drop(values);
        kividb::memory_use::allocated() as isize - start as isize
    });
    let held = sent.recv().unwrap();
    // 10,000 blocks of 112 bytes, and the Vec of 80,000 that holds them.
    assert!(
        (10_000 * 112 + 80_000..=10_000 * 112 + 100_000).contains(&held),
        "{held}"
    );
    done.send(()).unwrap();
    let left = maker.join().unwrap();
    assert!(left.abs() < 4_096, "{left} bytes left counted");
}

#[test]
fn many_threads_hand_values_to_each_other() {
    let _one = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    let before = value_slab::slabs_in_use();
    const THREADS: usize = 8;
    let (senders, receivers): (Vec<_>, Vec<_>) = (0..THREADS)
        .map(|_| mpsc::channel::<(usize, usize, SharedBytes)>())
        .unzip();
    let threads: Vec<_> = receivers
        .into_iter()
        .enumerate()
        .map(|(me, from_previous)| {
            let to_next = senders[(me + 1) % THREADS].clone();
            thread::spawn(move || {
                let mut rnd = Lcg(me as u64 + 1);
                let mut kept: Vec<(usize, usize, SharedBytes)> = Vec::new();
                let check = |(len, seed, value): &(usize, usize, SharedBytes)| {
                    assert_eq!(&value[..], &bytes(*len, *seed)[..]);
                };
                for i in 0..scaled(60_000) {
                    let len = rnd.next(600);
                    let seed = me * 1_000_000 + i;
                    let value = (len, seed, SharedBytes::from(&bytes(len, seed)[..]));
                    match rnd.next(3) {
                        0 => to_next.send(value).unwrap(),
                        1 => kept.push(value),
                        _ => drop(value),
                    }
                    if kept.len() > 500 {
                        let at = rnd.next(kept.len());
                        check(&kept.swap_remove(at));
                    }
                    while let Ok(theirs) = from_previous.try_recv() {
                        check(&theirs);
                        if rnd.next(2) == 0 {
                            kept.push(theirs);
                        }
                    }
                }
                drop(to_next);
                kept.iter().for_each(check);
                // What is still on the way, once every thread has stopped
                // sending.
                from_previous
            })
        })
        .collect();
    drop(senders);
    let left: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    for from_previous in left {
        while let Ok(theirs) = from_previous.try_recv() {
            assert_eq!(&theirs.2[..], &bytes(theirs.0, theirs.1)[..]);
        }
    }
    assert_eq!(value_slab::slabs_in_use(), before);
}
