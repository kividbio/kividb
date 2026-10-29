// src/tag_locks.rs
//
// Scripts on different hash tags, at the same time.
//
// A script is one step to every other client. KiviDB gets that by having a
// script take the command gate alone (`command_gate`): nothing else runs
// while it does. On a machine with many cores that leaves all but one of
// them idle whenever scripts are what the server is doing — a job queue is
// nearly all scripts, and eight queues that share no key ran no faster on
// eight cores than one queue did on one.
//
// Which keys a script will touch cannot be known before it runs: it is told
// some (`KEYS`), and builds others itself. What can be known is the *hash
// tag* — the part of a key between `{` and `}` — that the keys it was told
// share, and a script can be held to it. So, when this is switched on
// (`lua-parallel-scripts`, on unless switched off, in a build with Lua):
//
// * A script whose `KEYS` all carry the same hash tag is held to that tag:
//   a key outside it is refused, as a key in another slot is in a cluster.
//   This is the one thing a script may do today that it may not do then,
//   and it is so however the script comes to be run — the first time or the
//   thousandth, inside a transaction or not — so that a script never works
//   until the day it is run another way.
// * Such a script, once it has been seen to be a quick one, takes its tag's
//   lock, and the command gate only as any command does. Scripts on other
//   tags run beside it. Until then, and inside a transaction, it takes the
//   gate alone, as every script does today.
// * Scripts with any other `KEYS` — none, untagged, of two tags — take the
//   gate alone and may touch what they like, as before. So do functions.
// * A command from a client takes the lock of every tagged key it names, so
//   it finds a script's keys as they were before the script or as they are
//   after it, never between. Keys without a tag belong to no script and
//   cost a look for `{`.
// * A command that reads or changes keys it does not name — `KEYS`, `SCAN`,
//   `DBSIZE`, `FLUSHALL` — takes the gate alone, which waits for every
//   script (a script holds the gate as a command for as long as it runs).
//
// The locks are stripes: a fixed table indexed by a hash of the tag. Two
// tags that land on one stripe wait for each other when they need not, and
// nothing else.

use parking_lot::{Mutex, MutexGuard};
use std::cell::{Cell, RefCell};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};

static ENABLED: AtomicBool = AtomicBool::new(false);

/// Whether scripts are locked by hash tag. One load.
#[inline]
pub fn enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

/// `CONFIG SET lua-parallel-scripts` and the same at startup. Switching it
/// is done with the command gate held alone (see the caller), so no command
/// that went by the old setting is running beside one that goes by the new.
pub fn set_enabled(on: bool) {
    ENABLED.store(on, Ordering::Relaxed);
}

/// A power of two.
const STRIPES: usize = 1024;

#[repr(align(64))]
struct Stripe(Mutex<()>);

static STRIPE: [Stripe; STRIPES] = [const { Stripe(parking_lot::const_mutex(())) }; STRIPES];

/// The hash tag of `key`: what is between the first `{` and the next `}`,
/// if that is not empty. (As a cluster reads it, except that a key without
/// one has none, where a cluster hashes the whole key.)
#[inline]
pub fn tag_of(key: &[u8]) -> Option<&[u8]> {
    let open = memchr::memchr(b'{', key)?;
    let rest = &key[open + 1..];
    let close = memchr::memchr(b'}', rest)?;
    (close > 0).then(|| &rest[..close])
}

#[inline]
fn stripe_of(tag: &[u8]) -> usize {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in tag {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    // The low bits of FNV-1a of short strings are its weakest.
    ((hash ^ (hash >> 32)) as usize) & (STRIPES - 1)
}

thread_local! {
    /// The tag the script this thread is running is held to.
    static SCRIPT_TAG: RefCell<Option<Vec<u8>>> = const { RefCell::new(None) };
    /// That script holds its tag's lock, and runs beside scripts of other
    /// threads — not the command gate alone.
    static BESIDE_OTHERS: Cell<bool> = const { Cell::new(false) };
    /// Set for the script about to be run on this thread: it has been seen to
    /// be quick, and may run under its tag (see `allow_next_script`).
    static NEXT_SCRIPT_ALLOWED: Cell<bool> = const { Cell::new(false) };
}

/// The tag a script command's `KEYS` share: `cmd` is `EVAL`/`EVALSHA` (or the
/// `_RO` forms) with its arguments. `None` if it has no keys, a key has no
/// tag, or two differ.
pub fn script_tag<'a>(cmd: &[&'a [u8]]) -> Option<&'a [u8]> {
    let name = cmd.first()?;
    if !(name.eq_ignore_ascii_case(b"EVAL")
        || name.eq_ignore_ascii_case(b"EVALSHA")
        || name.eq_ignore_ascii_case(b"EVAL_RO")
        || name.eq_ignore_ascii_case(b"EVALSHA_RO"))
    {
        return None;
    }
    let numkeys: usize = std::str::from_utf8(cmd.get(2)?).ok()?.parse().ok()?;
    let keys = cmd.get(3..3usize.checked_add(numkeys)?)?;
    let (first, rest) = keys.split_first()?;
    let tag = tag_of(first)?;
    rest.iter()
        .all(|key| tag_of(key) == Some(tag))
        .then_some(tag)
}

/// Say that the script this thread runs next may run under its tag. Asked
/// for by the caller that knows the script to be a quick one; taken by the
/// command path, once.
pub fn allow_next_script() {
    NEXT_SCRIPT_ALLOWED.with(|allowed| allowed.set(true));
}

/// Whether [`allow_next_script`] was called for the command now starting.
/// Cleared by asking.
pub fn take_next_script_allowed() -> bool {
    NEXT_SCRIPT_ALLOWED.with(|allowed| allowed.replace(false))
}

/// Held by a tagged script for as long as it runs: the note that this
/// thread's commands are that script's and keep to its tag, and — for a
/// script running beside others — its tag's lock.
pub struct ScriptGuard {
    _stripe: Option<MutexGuard<'static, ()>>,
    /// Whether this guard is the one that set the tag: a tagged script
    /// called by a tagged script leaves the outer one's in place.
    outermost: bool,
    _not_send: PhantomData<*const ()>,
}

impl Drop for ScriptGuard {
    fn drop(&mut self) {
        if self.outermost {
            SCRIPT_TAG.with(|tag| *tag.borrow_mut() = None);
            BESIDE_OTHERS.with(|beside| beside.set(false));
        }
    }
}

fn hold(tag: &[u8], stripe: Option<MutexGuard<'static, ()>>) -> ScriptGuard {
    let outermost = SCRIPT_TAG.with(|held| {
        let mut held = held.borrow_mut();
        if held.is_some() {
            false
        } else {
            *held = Some(tag.to_vec());
            true
        }
    });
    if outermost {
        BESIDE_OTHERS.with(|beside| beside.set(stripe.is_some()));
    }
    ScriptGuard {
        _stripe: stripe,
        outermost,
        _not_send: PhantomData,
    }
}

/// Take `tag`'s lock for a script that runs beside others.
///
/// The script has had its turn among the scripts of its tag already
/// ([`script_turn`]), so what this waits for, if anything, is a client's
/// command on a key of the tag: a moment.
pub fn lock_for_script(tag: &[u8]) -> ScriptGuard {
    hold(tag, Some(STRIPE[stripe_of(tag)].0.lock()))
}

/// The turns of the scripts of each stripe: see [`script_turn`].
static TURN: [tokio::sync::Mutex<()>; STRIPES] =
    [const { tokio::sync::Mutex::const_new(()) }; STRIPES];

/// Wait for `tag`'s turn among scripts, and hold it while the script runs.
///
/// A script waits for the scripts ahead of it on its tag here, asleep and
/// in the order they came — not inside the command, on the lock, where the
/// connection's thread would wait with it and every other connection of
/// that thread behind it, most of them on tags nobody holds.
pub async fn script_turn(tag: &[u8]) -> tokio::sync::MutexGuard<'static, ()> {
    TURN[stripe_of(tag)].lock().await
}

/// Hold a script to `tag` without the lock: one that has the command gate
/// alone, and so has nothing to be locked against.
pub fn hold_script_to(tag: &[u8]) -> ScriptGuard {
    hold(tag, None)
}

/// Whether this thread is running a script that is held to a tag.
#[inline]
pub fn in_script() -> bool {
    SCRIPT_TAG.with(|tag| tag.borrow().is_some())
}

/// Whether this thread is running a script beside the scripts of other
/// threads, under its tag's lock.
#[inline]
pub fn beside_others() -> bool {
    BESIDE_OTHERS.with(|beside| beside.get())
}

/// For a command the script on this thread calls: whether `key` is one the
/// script may touch. True when no script is running under a tag here.
pub fn script_may_touch(key: &[u8]) -> bool {
    SCRIPT_TAG.with(|tag| match tag.borrow().as_deref() {
        None => true,
        Some(tag) => tag_of(key) == Some(tag),
    })
}

/// The tag of the script on this thread, for a message.
pub fn script_tag_for_message() -> String {
    SCRIPT_TAG.with(|tag| {
        tag.borrow()
            .as_deref()
            .map(|tag| String::from_utf8_lossy(tag).into_owned())
            .unwrap_or_default()
    })
}

/// Held by a command from a client: the lock of every tagged key it names.
pub struct KeysGuard {
    _one: Option<MutexGuard<'static, ()>>,
    _more: Vec<MutexGuard<'static, ()>>,
}

/// Take the lock of each tagged key in `keys`. `None` — nothing taken, and
/// nothing allocated — when no key has a tag, which is the usual case.
pub fn lock_keys<'a>(keys: impl Iterator<Item = &'a [u8]>) -> Option<KeysGuard> {
    let mut first: Option<usize> = None;
    let mut more: Vec<usize> = Vec::new();
    for key in keys {
        let Some(tag) = tag_of(key) else { continue };
        let stripe = stripe_of(tag);
        match first {
            None => first = Some(stripe),
            Some(one) if one == stripe => {}
            Some(_) => more.push(stripe),
        }
    }
    let first = first?;
    if more.is_empty() {
        return Some(KeysGuard {
            _one: Some(STRIPE[first].0.lock()),
            _more: Vec::new(),
        });
    }
    // Several: in the order of the table, so that two commands naming the
    // same tags the other way round do not wait for each other.
    more.push(first);
    more.sort_unstable();
    more.dedup();
    Some(KeysGuard {
        _one: None,
        _more: more
            .into_iter()
            .map(|stripe| STRIPE[stripe].0.lock())
            .collect(),
    })
}

/// Commands that read or change keys they do not name. With tag locks on
/// they take the command gate alone: no script is then part-way through.
pub fn sees_the_whole_keyspace(name: &str) -> bool {
    matches!(
        name,
        "KEYS" | "SCAN" | "DBSIZE" | "RANDOMKEY" | "FLUSHALL" | "FLUSHDB" | "SWAPDB"
    )
}

/// Whether `cmd` is the command that switches this on or off. It is run
/// with the command gate held alone.
#[inline(always)]
pub fn is_switch(name: &str, cmd: &[&[u8]]) -> bool {
    name == "CONFIG"
        && cmd
            .get(1)
            .is_some_and(|sub| sub.eq_ignore_ascii_case(b"SET"))
        && cmd
            .iter()
            .skip(2)
            .step_by(2)
            .any(|parameter| parameter.eq_ignore_ascii_case(b"lua-parallel-scripts"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_tag_is_what_is_between_the_first_braces() {
        assert_eq!(tag_of(b"{q1}:wait"), Some(&b"q1"[..]));
        assert_eq!(tag_of(b"bull:{q1}:wait"), Some(&b"q1"[..]));
        assert_eq!(tag_of(b"{a}{b}"), Some(&b"a"[..]));
        assert_eq!(tag_of(b"plain"), None);
        assert_eq!(tag_of(b"{}x"), None);
        assert_eq!(tag_of(b"{open"), None);
        assert_eq!(tag_of(b"close}"), None);
    }

    #[test]
    fn a_script_has_a_tag_when_all_its_keys_share_one() {
        let tag = |cmd: &[&[u8]]| script_tag(cmd).map(|tag| tag.to_vec());
        assert_eq!(
            tag(&[b"EVALSHA", b"abc", b"2", b"{q}:a", b"x:{q}:b", b"arg"]),
            Some(b"q".to_vec())
        );
        assert_eq!(
            tag(&[b"eval", b"return 1", b"1", b"{q}:a"]),
            Some(b"q".to_vec())
        );
        // No keys, an untagged key, two tags, a function, a count that lies.
        assert_eq!(tag(&[b"EVALSHA", b"abc", b"0", b"{q}:a"]), None);
        assert_eq!(tag(&[b"EVALSHA", b"abc", b"2", b"{q}:a", b"b"]), None);
        assert_eq!(tag(&[b"EVALSHA", b"abc", b"2", b"{q}:a", b"{r}:b"]), None);
        assert_eq!(tag(&[b"FCALL", b"f", b"1", b"{q}:a"]), None);
        assert_eq!(tag(&[b"EVALSHA", b"abc", b"5", b"{q}:a"]), None);
        assert_eq!(tag(&[b"EVALSHA", b"abc", b"x", b"{q}:a"]), None);
    }

    #[test]
    fn a_script_is_held_to_its_tag() {
        assert!(script_may_touch(b"anything"), "no script here");
        let guard = lock_for_script(b"q1");
        assert!(in_script());
        assert!(script_may_touch(b"{q1}:jobs"));
        assert!(script_may_touch(b"bull:{q1}:7"));
        assert!(!script_may_touch(b"{q2}:jobs"));
        assert!(!script_may_touch(b"untagged"));
        drop(guard);
        assert!(!in_script());
        assert!(script_may_touch(b"{q2}:jobs"));
    }

    #[test]
    fn a_command_waits_for_the_script_on_its_tag_and_no_other() {
        let script = lock_for_script(b"busy-queue");
        // Another tag, and no tag: neither waits. (A different stripe is
        // found by looking, so that the test does not depend on the hash.)
        let other = (0..)
            .map(|i| format!("other-{i}"))
            .find(|tag| stripe_of(tag.as_bytes()) != stripe_of(b"busy-queue"))
            .unwrap();
        let key = format!("{{{other}}}:k");
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    assert!(lock_keys([key.as_bytes(), b"plain".as_slice()].into_iter()).is_some());
                    assert!(lock_keys([b"plain".as_slice()].into_iter()).is_none());
                })
                .join()
                .unwrap();
        });
        // The same tag waits until the script lets go.
        let (waiting_tx, waiting_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let waiter = std::thread::spawn(move || {
            waiting_tx.send(()).unwrap();
            let _held = lock_keys([b"{busy-queue}:k".as_slice()].into_iter());
            done_tx.send(()).unwrap();
        });
        waiting_rx.recv().unwrap();
        assert!(
            done_rx
                .recv_timeout(std::time::Duration::from_millis(100))
                .is_err(),
            "it did not wait for the script"
        );
        drop(script);
        done_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("it was let through when the script ended");
        waiter.join().unwrap();
    }

    #[test]
    fn several_tags_are_taken_whatever_order_they_are_named_in() {
        let a = lock_keys(
            [
                b"{t1}:a".as_slice(),
                b"{t2}:b".as_slice(),
                b"{t1}:c".as_slice(),
            ]
            .into_iter(),
        );
        assert!(a.is_some());
        drop(a);
        // The other way round, from two threads at once, many times: a
        // deadlock would hang the test.
        std::thread::scope(|scope| {
            for flip in [false, true] {
                scope.spawn(move || {
                    for _ in 0..2000 {
                        let keys: [&[u8]; 2] = if flip {
                            [b"{t2}:b", b"{t1}:a"]
                        } else {
                            [b"{t1}:a", b"{t2}:b"]
                        };
                        let _held = lock_keys(keys.into_iter());
                    }
                });
            }
        });
    }
}
