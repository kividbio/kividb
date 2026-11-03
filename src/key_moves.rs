// src/key_moves.rs
//
// The keys a `MIGRATE` is moving, and the writes that wait for it.
//
// `MIGRATE` without `COPY` reads its keys, sends them to the destination,
// waits for the destination to confirm them, and deletes them here. A write
// to one of them in that time used to be applied here and then either lost
// with the delete (#274) or, since, refused to `MIGRATE` as `TRYAGAIN`. A
// resharding tool stops on `TRYAGAIN`, and `redis-cli --cluster fix` cannot
// close the slot it leaves open while clients go on writing (#295).
//
// So from before `MIGRATE` reads a batch until its keys are deleted here, a
// write to one of them waits; then it is answered as the slot's state says:
// `-ASK` for a key that left. Redis gets the same by blocking the server for
// the round trip.
//
// The check costs ordinary commands nothing. A write already looks, inside
// the write gate, at one flag before it runs (`pause::writes_must_check`):
// it is how a handoff stops the writes that got past its pause. A
// transaction, whose commands run alone and cannot wait, looks at the same
// flag inside the gate and is refused with `TRYAGAIN`. `MIGRATE`
// sets that flag too while it holds keys here, and only then does a write
// look at the keys named below, in its slow path. To be sure that no write
// that looked before the flag was set is still running, `MIGRATE` waits
// for the writes in the gate once the flag is up (`write_gate::cut`), before
// it reads anything. See `commands::cluster::hold_while_moving`.

use parking_lot::{Condvar, Mutex};
use std::time::Duration;

/// The keys of one `MIGRATE`, in database `db`.
struct Batch {
    id: u64,
    db: usize,
    keys: Vec<Vec<u8>>,
}

struct Moving {
    next_id: u64,
    batches: Vec<Batch>,
}

static MOVING: Mutex<Moving> = parking_lot::const_mutex(Moving {
    next_id: 0,
    batches: Vec::new(),
});

/// Signalled when a batch is let go of.
static LET_GO: Condvar = Condvar::new();

/// The keys of one `MIGRATE` marked as moving, until this is dropped.
pub struct Marked {
    id: u64,
}

impl Drop for Marked {
    fn drop(&mut self) {
        let mut moving = MOVING.lock();
        moving.batches.retain(|batch| batch.id != self.id);
        if moving.batches.is_empty() {
            crate::pause::set_keys_moving(false);
        }
        drop(moving);
        LET_GO.notify_all();
    }
}

/// Mark `keys`, of database `db`, as being moved: writes to them wait from
/// the next one that enters the write gate (see the module).
pub fn mark(db: usize, keys: Vec<Vec<u8>>) -> Marked {
    let mut moving = MOVING.lock();
    let id = moving.next_id;
    moving.next_id += 1;
    moving.batches.push(Batch { id, db, keys });
    // Under the lock, as it is cleared: the flag is up while any batch is.
    crate::pause::set_keys_moving(true);
    Marked { id }
}

fn names_one(moving: &Moving, db: usize, keys: &[&[u8]]) -> bool {
    moving.batches.iter().any(|batch| {
        batch.db == db
            && keys
                .iter()
                .any(|key| batch.keys.iter().any(|moved| moved.as_slice() == *key))
    })
}

fn keys_of<'a>(name: &str, cmd: &[&'a [u8]]) -> Vec<&'a [u8]> {
    crate::keyspec::key_positions(name, cmd)
        .iter()
        .map(|pos| cmd[pos])
        .collect()
}

/// Whether the command `cmd` (named `name`), in database `db`, names a key
/// that a `MIGRATE` is moving.
#[cold]
#[inline(never)]
pub fn names_a_moving_key(db: usize, name: &str, cmd: &[&[u8]]) -> bool {
    let keys = keys_of(name, cmd);
    !keys.is_empty() && names_one(&MOVING.lock(), db, &keys)
}

/// Whether a command of the transaction `queue`, in database `db`, names a
/// key that a `MIGRATE` is moving.
#[cold]
#[inline(never)]
pub fn queue_names_a_moving_key(db: usize, queue: &[Vec<Vec<u8>>]) -> bool {
    let moving = MOVING.lock();
    queue.iter().any(|cmd| {
        let Some(first) = cmd.first() else {
            return false;
        };
        let name = String::from_utf8_lossy(first).to_ascii_uppercase();
        let args: Vec<&[u8]> = cmd.iter().map(Vec::as_slice).collect();
        let keys = keys_of(&name, &args);
        !keys.is_empty() && names_one(&moving, db, &keys)
    })
}

/// Wait until no `MIGRATE` is moving a key `cmd` names: for as long as the
/// `MIGRATE` that holds it takes, whose every step waits on the network for
/// at most its timeout. Called by a write outside the write gate.
///
/// On a worker of the multi-threaded tokio runtime the worker's other tasks
/// go to another thread first, as in `pause::wait_out_handoff`.
#[cold]
#[inline(never)]
pub fn wait_until_moved(db: usize, name: &str, cmd: &[&[u8]]) {
    let keys = keys_of(name, cmd);
    let wait = || {
        let mut moving = MOVING.lock();
        while names_one(&moving, db, &keys) {
            LET_GO.wait_for(&mut moving, Duration::from_millis(10));
        }
    };
    let on_multi_thread_runtime = tokio::runtime::Handle::try_current().is_ok_and(|runtime| {
        runtime.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread
    });
    if on_multi_thread_runtime {
        tokio::task::block_in_place(wait);
    } else {
        wait();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_is_moving_while_it_is_marked() {
        let set = |args: &[&'static str]| -> Vec<&'static [u8]> {
            args.iter().map(|a| a.as_bytes()).collect()
        };
        let marked = mark(3, vec![b"km:a".to_vec(), b"km:b".to_vec()]);
        assert!(names_a_moving_key(3, "SET", &set(&["SET", "km:a", "1"])));
        assert!(names_a_moving_key(
            3,
            "MSET",
            &set(&["MSET", "km:x", "1", "km:b", "2"])
        ));
        // Another database, another key.
        assert!(!names_a_moving_key(4, "SET", &set(&["SET", "km:a", "1"])));
        assert!(!names_a_moving_key(3, "SET", &set(&["SET", "km:c", "1"])));
        let waiter = std::thread::spawn(move || {
            wait_until_moved(3, "INCR", &set(&["INCR", "km:b"]));
        });
        std::thread::sleep(Duration::from_millis(50));
        assert!(!waiter.is_finished(), "a write to a moving key waits");
        drop(marked);
        waiter.join().unwrap();
        assert!(!names_a_moving_key(3, "SET", &set(&["SET", "km:a", "1"])));
    }
}
