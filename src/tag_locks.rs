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
// * A script whose `KEYS` all carry a hash tag — one, or several different
//   ones — is held to those tags: a key outside all of them is refused, as a
//   key in another slot is in a cluster. This is the one thing a script may
//   do today that it may not do then, and it is so however the script comes
//   to be run — the first time or the thousandth, inside a transaction or
//   not — so that a script never works until the day it is run another way.
// * Such a script, once it has been seen to be a quick one, takes the lock
//   of each of its tags, in the order of the table (as a client's command
//   naming keys of several tags does, `lock_keys`), and the command gate
//   only as any command does. Scripts on other tags run beside it. Until
//   then, and inside a transaction, it takes the gate alone, as every
//   script does today.
// * Scripts with any other `KEYS` — none, or one without a tag — take the
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
// nothing else; a script whose tags land on one stripe takes it once.
//
// Whoever holds more than one stripe took them in the order of the table,
// lowest first, and takes no stripe while it holds a later one: a client's
// command (`lock_keys`), a script (`lock_for_script`) and active expiry
// (`try_lock_keys`, which waits a bounded time at most). So no two of them
// wait for each other. A script's turns among the scripts of its tags
// (`script_turn`) are taken the same way, lowest stripe first, before any
// gate or stripe, and are taken by nothing else.
//
// Whoever takes a tag's lock holds the write gate (`write_gate`) first, a
// read as a write does: the gate, then the tags' locks, then the keys'
// order. A read under a tag's lock that finds a key past its time deletes
// it, and its `DEL` is logged and propagated under the write gate
// (`expiry::order`). Entering the gate there, with the tag held, hung the
// server: a write to the tag, or active expiry, held a slot of the gate and
// waited for the tag, and a checkpoint, holding the read's slot already,
// waited for theirs.

use parking_lot::{Mutex, MutexGuard};
use std::cell::{Cell, RefCell};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

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
/// one has none, where a cluster hashes the whole key.) Always inline: a
/// call of it cost a client's command on a tagged key more than its look.
#[inline(always)]
pub fn tag_of(key: &[u8]) -> Option<&[u8]> {
    let open = position_of(b'{', key)?;
    let rest = &key[open + 1..];
    let close = position_of(b'}', rest)?;
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

/// The tags the script on a thread is held to: their bytes end to end, and
/// where each ends. The room is kept from one script to the next, so that
/// holding a script to its tags allocates nothing once warm.
struct HeldTags {
    held: bool,
    bytes: Vec<u8>,
    ends: Vec<usize>,
}

impl HeldTags {
    const fn new() -> Self {
        HeldTags {
            held: false,
            bytes: Vec::new(),
            ends: Vec::new(),
        }
    }

    fn set(&mut self, tags: &ScriptTags) {
        self.bytes.clear();
        self.ends.clear();
        for tag in tags.as_slice() {
            self.bytes.extend_from_slice(tag);
            self.ends.push(self.bytes.len());
        }
        self.held = true;
    }

    /// Whether `tag` is one of them.
    #[inline]
    fn holds(&self, tag: &[u8]) -> bool {
        if self.ends.len() == 1 {
            return self.bytes == tag;
        }
        self.iter().any(|held| held == tag)
    }

    fn clear(&mut self) {
        self.held = false;
        // Not kept, the room of a script of a great many tags.
        if self.bytes.capacity() > 64 * 1024 {
            self.bytes = Vec::new();
            self.ends = Vec::new();
        }
    }

    fn iter(&self) -> impl Iterator<Item = &[u8]> {
        let mut start = 0;
        self.ends.iter().map(move |&end| {
            let tag = &self.bytes[start..end];
            start = end;
            tag
        })
    }
}

thread_local! {
    /// The tags the script this thread is running is held to.
    static SCRIPT_TAGS: RefCell<HeldTags> = const { RefCell::new(HeldTags::new()) };
    /// That script holds its tag's lock, and runs beside scripts of other
    /// threads — not the command gate alone.
    static BESIDE_OTHERS: Cell<bool> = const { Cell::new(false) };
    /// Set for the script about to be run on this thread: it has been seen to
    /// be quick, and may run under its tag (see `allow_next_script`).
    static NEXT_SCRIPT_ALLOWED: Cell<bool> = const { Cell::new(false) };
}

/// The hash tags of a script's `KEYS`, each once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScriptTags<'a> {
    /// One, the usual case: nothing allocated.
    One(&'a [u8]),
    /// Several: the first key's, then the others in the order of their
    /// bytes.
    Several(Vec<&'a [u8]>),
}

impl<'a> ScriptTags<'a> {
    pub fn as_slice(&self) -> &[&'a [u8]] {
        match self {
            ScriptTags::One(tag) => std::slice::from_ref(tag),
            ScriptTags::Several(tags) => tags,
        }
    }

    /// The stripes of the tags, each once, lowest first: the order every
    /// holder of several takes them in.
    fn stripes(&self) -> Vec<usize> {
        let mut stripes: Vec<usize> = self.as_slice().iter().map(|tag| stripe_of(tag)).collect();
        stripes.sort_unstable();
        stripes.dedup();
        stripes
    }
}

/// The hash tags of a script command's `KEYS`: `cmd` is `EVAL`/`EVALSHA` (or
/// the `_RO` forms) with its arguments. `None` if it has no keys or a key
/// has no tag. Keys of one tag, the usual case, allocate nothing.
pub fn script_tags<'a>(cmd: &[&'a [u8]]) -> Option<ScriptTags<'a>> {
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
    let first = tag_of(first)?;
    let mut others = Vec::new();
    for key in rest {
        let tag = tag_of(key)?;
        if tag != first {
            others.push(tag);
        }
    }
    if others.is_empty() {
        return Some(ScriptTags::One(first));
    }
    others.sort_unstable();
    others.dedup();
    others.insert(0, first);
    Some(ScriptTags::Several(others))
}

/// What a script command's `KEYS` are, as far as running under a tag goes:
/// for `INFO`'s counts of the scripts that ran alone, and why
/// (`stats::scripts`). Asked only of a script that runs alone.
#[cfg(feature = "lua")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScriptKeys {
    /// None, or a count of them that is not one.
    None,
    /// At least one has no hash tag.
    Untagged,
    /// All have one.
    Tagged,
}

/// See [`ScriptKeys`]. `cmd` is a script command with its arguments.
#[cfg(feature = "lua")]
pub fn script_keys(cmd: &[&[u8]]) -> ScriptKeys {
    let keys = std::str::from_utf8(cmd.get(2).copied().unwrap_or_default())
        .ok()
        .and_then(|n| n.parse::<usize>().ok())
        .and_then(|n| cmd.get(3..3usize.checked_add(n)?));
    let Some((first, rest)) = keys.and_then(|keys| keys.split_first()) else {
        return ScriptKeys::None;
    };
    if std::iter::once(first)
        .chain(rest)
        .all(|key| tag_of(key).is_some())
    {
        ScriptKeys::Tagged
    } else {
        ScriptKeys::Untagged
    }
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
/// thread's commands are that script's and keep to its tags, and — for a
/// script running beside others — its tags' locks.
///
/// What it holds is let go of out of line ([`Drop`]): the guard is an
/// `Option` in the way of every command (`handler::run_command`), and the
/// code to let go of several stripes has no business there.
pub struct ScriptGuard {
    held: std::mem::ManuallyDrop<Held>,
    /// Whether this guard is the one that set the tag: a tagged script
    /// called by a tagged script leaves the outer one's in place.
    outermost: bool,
    _not_send: PhantomData<*const ()>,
}

/// What a [`ScriptGuard`] holds, let go of in this order.
struct Held {
    _stripes: Stripes,
    /// Entered before the stripes are taken, and let go of after them: a
    /// read-only script (`EVAL_RO`) holds no write gate otherwise.
    _gate: Option<crate::write_gate::WriteGuard>,
}

/// The stripes a script holds: none (it runs alone), its one, or its
/// several, taken lowest first.
enum Stripes {
    None,
    One { _held: MutexGuard<'static, ()> },
    Several { _held: Vec<MutexGuard<'static, ()>> },
}

impl Drop for ScriptGuard {
    #[inline(never)]
    fn drop(&mut self) {
        if self.outermost {
            SCRIPT_TAGS.with(|tags| tags.borrow_mut().clear());
            BESIDE_OTHERS.with(|beside| beside.set(false));
        }
        // SAFETY: dropped here once; the field is not used again.
        unsafe { std::mem::ManuallyDrop::drop(&mut self.held) }
    }
}

fn hold(
    tags: &ScriptTags,
    stripes: Stripes,
    gate: Option<crate::write_gate::WriteGuard>,
) -> ScriptGuard {
    let outermost = SCRIPT_TAGS.with(|held| {
        let mut held = held.borrow_mut();
        if held.held {
            false
        } else {
            held.set(tags);
            true
        }
    });
    if outermost {
        BESIDE_OTHERS.with(|beside| beside.set(gate.is_some()));
    }
    ScriptGuard {
        held: std::mem::ManuallyDrop::new(Held {
            _stripes: stripes,
            _gate: gate,
        }),
        outermost,
        _not_send: PhantomData,
    }
}

/// Take the lock of each of `tags` for a script that runs beside others,
/// and hold it until the script is done: each stripe once, lowest first, as
/// a client's command naming keys of several tags takes them
/// ([`lock_keys`]), so that neither waits for the other in a cycle.
///
/// The script has had its turn among the scripts of its tags already
/// ([`script_turn`]), so what this waits for, if anything, is a client's
/// command on a key of one of them: a moment.
///
/// The write gate first, as for any tag's lock (see the top): a read-only
/// script holds none otherwise, and one that finds a key past its time
/// enters it to delete the key.
pub fn lock_for_script(tags: &ScriptTags) -> ScriptGuard {
    let gate = crate::write_gate::enter();
    let stripes = match tags {
        ScriptTags::One(tag) => Stripes::One {
            _held: STRIPE[stripe_of(tag)].0.lock(),
        },
        ScriptTags::Several(_) => Stripes::Several {
            _held: tags
                .stripes()
                .into_iter()
                .map(|stripe| STRIPE[stripe].0.lock())
                .collect(),
        },
    };
    hold(tags, stripes, Some(gate))
}

/// The turns of the scripts of each stripe: see [`script_turn`].
static TURN: [tokio::sync::Mutex<()>; STRIPES] =
    [const { tokio::sync::Mutex::const_new(()) }; STRIPES];

/// A script's turns among the scripts of its tags: see [`script_turn`].
pub enum ScriptTurn {
    One {
        _turn: tokio::sync::MutexGuard<'static, ()>,
    },
    Several {
        _turns: Vec<tokio::sync::MutexGuard<'static, ()>>,
    },
}

/// Wait for the turn of each of `tags` among scripts, and hold them while
/// the script runs.
///
/// A script waits for the scripts ahead of it on its tags here, asleep and
/// in the order they came — not inside the command, on the lock, where the
/// connection's thread would wait with it and every other connection of
/// that thread behind it, most of them on tags nobody holds. Several are
/// taken lowest stripe first, as the locks are: two scripts that share two
/// tags never each hold the turn the other waits for.
pub async fn script_turn(tags: &ScriptTags<'_>) -> ScriptTurn {
    match tags {
        ScriptTags::One(tag) => ScriptTurn::One {
            _turn: TURN[stripe_of(tag)].lock().await,
        },
        ScriptTags::Several(_) => {
            let mut turns = Vec::new();
            for stripe in tags.stripes() {
                turns.push(TURN[stripe].lock().await);
            }
            ScriptTurn::Several { _turns: turns }
        }
    }
}

/// Hold a script to `tags` without their locks: one that has the command
/// gate alone, and so has nothing to be locked against.
pub fn hold_script_to(tags: &ScriptTags) -> ScriptGuard {
    hold(tags, Stripes::None, None)
}

/// Whether this thread is running a script that is held to its tags.
#[inline]
pub fn in_script() -> bool {
    SCRIPT_TAGS.with(|tags| tags.borrow().held)
}

/// Whether this thread is running a script beside the scripts of other
/// threads, under its tags' locks.
#[inline]
pub fn beside_others() -> bool {
    BESIDE_OTHERS.with(|beside| beside.get())
}

/// For a command the script on this thread calls: whether `key` is one the
/// script may touch, a key of one of its tags. True when no script is
/// running under its tags here.
pub fn script_may_touch(key: &[u8]) -> bool {
    SCRIPT_TAGS.with(|tags| {
        let tags = tags.borrow();
        !tags.held || tag_of(key).is_some_and(|tag| tags.holds(tag))
    })
}

/// The tags of the script on this thread, for a message: `hash tag 'q'`,
/// or `hash tags 'a', 'b'`.
pub fn script_tags_for_message() -> String {
    SCRIPT_TAGS.with(|tags| {
        let tags = tags.borrow();
        let quoted: Vec<String> = tags
            .iter()
            .map(|tag| format!("'{}'", String::from_utf8_lossy(tag)))
            .collect();
        match quoted.len() {
            1 => format!("hash tag {}", quoted[0]),
            _ => format!("hash tags {}", quoted.join(", ")),
        }
    })
}

/// Held by a command from a client: the lock of every tagged key it names,
/// and the write gate, entered before them and let go of after them.
pub struct KeysGuard {
    _one: Option<MutexGuard<'static, ()>>,
    _more: Vec<MutexGuard<'static, ()>>,
    _gate: crate::write_gate::WriteGuard,
}

/// Take the lock of each tagged key in `keys`. `None` — nothing taken, and
/// nothing allocated — when no key has a tag, which is the usual case.
///
/// The write gate first (see the top), for a read too: one that finds a key
/// past its time deletes it under the gate, and must not enter it with a
/// tag held. A write holds the gate already, and this costs it a count.
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
    let gate = crate::write_gate::enter();
    if more.is_empty() {
        return Some(KeysGuard {
            _one: Some(STRIPE[first].0.lock()),
            _more: Vec::new(),
            _gate: gate,
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
        _gate: gate,
    })
}

/// Whether `key` may carry a hash tag: whether it has a `{` at all. One
/// without has no tag ([`tag_of`]); one with may not either (`{}`, no `}`).
/// Eight bytes at a time up to 32, which on the command path costs a short
/// key less than a call of `memchr` (#889).
#[inline]
pub fn may_carry_tag(key: &[u8]) -> bool {
    position_of(b'{', key).is_some()
}

/// Where `byte` is first in `hay`: eight bytes at a time up to 32, which
/// costs a key of a few dozen bytes less than a call of `memchr` (#889);
/// `memchr` past that.
#[inline(always)]
fn position_of(byte: u8, hay: &[u8]) -> Option<usize> {
    const ONES: u64 = u64::from_ne_bytes([1; 8]);
    const HIGHS: u64 = ONES << 7;
    let pattern = ONES * byte as u64;
    // The bytes of `word` that are `byte` are the zero bytes of `x`. The
    // lowest bit this sets marks the first of them: it sets a bit that is
    // not one only above a byte that is (where the subtraction borrowed).
    let first = |word: &[u8]| {
        let x = u64::from_le_bytes(word.try_into().unwrap()) ^ pattern;
        let found = x.wrapping_sub(ONES) & !x & HIGHS;
        (found != 0).then(|| found.trailing_zeros() as usize / 8)
    };
    let len = hay.len();
    match len {
        0..8 => hay.iter().position(|&each| each == byte),
        8..=32 => {
            let mut at = 0;
            while at + 8 < len {
                if let Some(within) = first(&hay[at..at + 8]) {
                    return Some(at + within);
                }
                at += 8;
            }
            // The last eight, over bytes looked at already if need be: none
            // of those was `byte`.
            first(&hay[len - 8..]).map(|within| len - 8 + within)
        }
        _ => memchr::memchr(byte, hay),
    }
}

/// Where [`lock_named_keys`] looks for the tags of the keys a client's
/// command names.
#[derive(Debug)]
pub enum NamedKeys {
    /// Every `step`-th argument from `from` to `last`: the first key that
    /// may carry a tag, and those after it (`keyspec::regular_key_positions`).
    Regular {
        from: usize,
        last: usize,
        step: usize,
    },
    /// The keys `keyspec::key_positions` finds.
    Other(crate::keyspec::Keys),
}

/// The keys of `cmd` (a command from a client, `name` upper case) that
/// [`lock_keys`] has to look at: `None` when none can carry a tag, which is
/// what `lock_keys` would find, and then nothing is taken. Most commands
/// name their keys at regular places and most keys have no `{`, so this
/// is a look for `{` in each key, without building the general list of
/// positions (#889).
#[inline]
pub fn named_keys(name: &str, cmd: &[&[u8]]) -> Option<NamedKeys> {
    // A key is an argument: a command of one argument (`GET k`, `INCR k`)
    // names that one at most, whatever the command, and the table need not
    // be asked when it has no `{`.
    let one_argument = cmd.len() == 2;
    if one_argument && !may_carry_tag(cmd[1]) {
        return None;
    }
    let Some((first, last, step)) = crate::keyspec::regular_key_positions(name, cmd) else {
        return Some(NamedKeys::Other(crate::keyspec::key_positions(name, cmd)));
    };
    // That one has a `{`: whether it is a key is all that is left to know.
    if one_argument {
        return (first <= last).then_some(NamedKeys::Regular {
            from: first,
            last,
            step,
        });
    }
    let mut at = first;
    while at <= last {
        if may_carry_tag(cmd[at]) {
            return Some(NamedKeys::Regular {
                from: at,
                last,
                step,
            });
        }
        at += step;
    }
    None
}

/// [`lock_keys`] over the keys a client's command names: the lock of each
/// tagged one, and `None`, with nothing taken or entered, when none has a
/// tag. A key before the first that may carry a tag has none, and
/// `lock_keys` would have passed it by.
#[inline]
pub fn lock_named_keys(name: &str, cmd: &[&[u8]]) -> Option<KeysGuard> {
    let keys = named_keys(name, cmd)?;
    lock_found_keys(keys, cmd)
}

/// The rest of [`lock_named_keys`], for a command with a key that may carry
/// a tag. Out of line, so that the look for one stays small; one iterator
/// for both kinds, the one `key_positions`' keys give, so that `lock_keys`
/// is made once here.
#[inline(never)]
fn lock_found_keys(keys: NamedKeys, cmd: &[&[u8]]) -> Option<KeysGuard> {
    let at = match &keys {
        NamedKeys::Regular { from, last, step } => crate::keyspec::KeyIter::Range {
            next: *from,
            last: *last,
            step: *step,
        },
        NamedKeys::Other(keys) => keys.iter(),
    };
    lock_keys(at.map(|at| cmd[at]))
}

/// What active expiry holds of the tags of a batch of keys: the locks it
/// could take, and which it could not ([`try_lock_keys`]).
pub struct TriedKeys {
    _held: Vec<MutexGuard<'static, ()>>,
    /// The stripes that were busy: their keys are left for another time.
    busy: Vec<usize>,
}

impl TriedKeys {
    /// Whether `key` may be taken: it has no tag, or its tag's lock is held.
    #[inline]
    pub fn may_take(&self, key: &[u8]) -> bool {
        self.busy.is_empty() || tag_of(key).is_none_or(|tag| !self.busy.contains(&stripe_of(tag)))
    }

    /// Whether some key's tag was busy, and is to be left.
    #[inline]
    pub fn left_some(&self) -> bool {
        !self.busy.is_empty()
    }
}

/// How many times in a row a thread may find a stripe busy in
/// [`try_lock_keys`] before it waits for it, and how long it then waits at
/// most.
const PATIENT_AFTER: u8 = 3;
const PATIENCE: Duration = Duration::from_millis(5);

thread_local! {
    /// For each stripe, how many times in a row [`try_lock_keys`] found it
    /// busy on this thread.
    static MISSES: [Cell<u8>; STRIPES] = const { [const { Cell::new(0) }; STRIPES] };
}

/// The lock of each tagged key in `keys`, for active expiry, which holds the
/// write gate already: without waiting for a lock that is held, or not for
/// long. `None` when no key has a tag.
///
/// Waiting there, under the gate, is how it hung (with the sweep holding
/// a slot of the write gate and waiting for a tag, a read under that tag
/// waiting for the same slot to delete an expired key, and a checkpoint
/// waiting for the sweep's slot and holding back the read). So a stripe
/// that is held is not waited for: its keys are left (`TriedKeys::may_take`),
/// and found again on the next cycle. A stripe found busy
/// [`PATIENT_AFTER`] times in a row, as a hash tag whose queue is never idle
/// is, is waited for, [`PATIENCE`] at most: a script's tag is held for a
/// moment at a time, and a `parking_lot` lock is handed to a thread that
/// waits for it within half a millisecond or so. The wait is bounded, so
/// nothing waits on the sweep for longer than that.
pub fn try_lock_keys<'a>(keys: impl Iterator<Item = &'a [u8]>) -> Option<TriedKeys> {
    let mut stripes: Vec<usize> = keys.filter_map(tag_of).map(stripe_of).collect();
    if stripes.is_empty() {
        return None;
    }
    // In the order of the table, as `lock_keys` takes them.
    stripes.sort_unstable();
    stripes.dedup();
    let mut held = Vec::with_capacity(stripes.len());
    let mut busy = Vec::new();
    MISSES.with(|misses| {
        for stripe in stripes {
            let missed = &misses[stripe];
            let lock = &STRIPE[stripe].0;
            let guard = if missed.get() >= PATIENT_AFTER {
                lock.try_lock_for(PATIENCE)
            } else {
                lock.try_lock()
            };
            match guard {
                Some(guard) => {
                    missed.set(0);
                    held.push(guard);
                }
                None => {
                    missed.set(missed.get().saturating_add(1));
                    busy.push(stripe);
                }
            }
        }
    });
    Some(TriedKeys { _held: held, busy })
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
    fn a_script_has_tags_when_all_its_keys_carry_one() {
        let tags = |cmd: &[&[u8]]| {
            script_tags(cmd).map(|tags| {
                tags.as_slice()
                    .iter()
                    .map(|tag| tag.to_vec())
                    .collect::<Vec<_>>()
            })
        };
        assert_eq!(
            tags(&[b"EVALSHA", b"abc", b"2", b"{q}:a", b"x:{q}:b", b"arg"]),
            Some(vec![b"q".to_vec()])
        );
        assert_eq!(
            tags(&[b"eval", b"return 1", b"1", b"{q}:a"]),
            Some(vec![b"q".to_vec()])
        );
        // Several: each once, the first key's first.
        assert_eq!(
            tags(&[
                b"EVALSHA", b"abc", b"5", b"{t}:a", b"{s}:b", b"{t}:c", b"{r}:d", b"{s}:e"
            ]),
            Some(vec![b"t".to_vec(), b"r".to_vec(), b"s".to_vec()])
        );
        assert!(
            matches!(
                script_tags(&[b"EVALSHA", b"abc", b"2", b"{q}:a", b"{q}:b"]),
                Some(ScriptTags::One(_))
            ),
            "one tag allocates nothing"
        );
        // No keys, an untagged key, a function, a count that lies.
        assert_eq!(tags(&[b"EVALSHA", b"abc", b"0", b"{q}:a"]), None);
        assert_eq!(tags(&[b"EVALSHA", b"abc", b"2", b"{q}:a", b"b"]), None);
        assert_eq!(tags(&[b"EVALSHA", b"abc", b"2", b"b", b"{q}:a"]), None);
        assert_eq!(
            tags(&[b"EVALSHA", b"abc", b"3", b"{q}:a", b"{r}:b", b"c"]),
            None
        );
        assert_eq!(tags(&[b"FCALL", b"f", b"1", b"{q}:a"]), None);
        assert_eq!(tags(&[b"EVALSHA", b"abc", b"5", b"{q}:a"]), None);
        assert_eq!(tags(&[b"EVALSHA", b"abc", b"x", b"{q}:a"]), None);
    }

    #[test]
    fn a_script_is_held_to_its_tags() {
        let two = ScriptTags::Several(vec![b"t1", b"t2"]);
        let guard = lock_for_script(&two);
        assert!(in_script());
        assert!(beside_others());
        assert!(script_may_touch(b"{t1}:a"));
        assert!(script_may_touch(b"x:{t2}:b"));
        assert!(!script_may_touch(b"{t3}:c"));
        assert!(!script_may_touch(b"plain"));
        assert_eq!(script_tags_for_message(), "hash tags 't1', 't2'");
        drop(guard);
        assert!(!in_script());
        assert!(!beside_others());
        assert!(script_may_touch(b"{t3}:c"));
        let alone = hold_script_to(&two);
        assert!(in_script());
        assert!(!beside_others());
        assert!(!script_may_touch(b"{t3}:c"));
        drop(alone);
        let one = lock_for_script(&ScriptTags::One(b"t1"));
        assert_eq!(script_tags_for_message(), "hash tag 't1'");
        assert!(!script_may_touch(b"{t2}:b"));
        drop(one);
    }

    /// Tags that land on one stripe are taken once: taken twice, the lock
    /// would wait for itself.
    #[test]
    fn two_tags_on_one_stripe_are_one_lock() {
        let mut same = None;
        'found: for i in 0..100_000 {
            for j in 0..i {
                let (a, b) = (format!("x{i}"), format!("x{j}"));
                if stripe_of(a.as_bytes()) == stripe_of(b.as_bytes()) {
                    same = Some((a, b));
                    break 'found;
                }
            }
        }
        let (a, b) = same.expect("two tags of one stripe");
        let tags = ScriptTags::Several(vec![a.as_bytes(), b.as_bytes()]);
        assert_eq!(tags.stripes().len(), 1);
        let guard = lock_for_script(&tags);
        assert!(script_may_touch(format!("{{{a}}}:k").as_bytes()));
        assert!(script_may_touch(format!("{{{b}}}:k").as_bytes()));
        drop(guard);
    }

    /// Scripts and client commands taking the same tags, named the other way
    /// round, from several threads at once, many times: a deadlock would
    /// hang the test. And while a script holds two tags, a command on
    /// either waits, and one on neither does not.
    #[test]
    fn scripts_of_several_tags_and_commands_never_wait_for_each_other_in_a_cycle() {
        let pick = |prefix: &str, avoid: &[usize]| {
            (0..)
                .map(|i| format!("{prefix}{i}"))
                .find(|tag| !avoid.contains(&stripe_of(tag.as_bytes())))
                .unwrap()
        };
        let a = pick("a", &[]);
        let b = pick("b", &[stripe_of(a.as_bytes())]);
        let c = pick("c", &[stripe_of(a.as_bytes()), stripe_of(b.as_bytes())]);
        std::thread::scope(|scope| {
            for flip in 0..4 {
                let (a, b, c) = (&a, &b, &c);
                scope.spawn(move || {
                    for _ in 0..2000 {
                        let (x, y) = if flip % 2 == 0 { (a, b) } else { (b, a) };
                        if flip < 2 {
                            let tags =
                                ScriptTags::Several(vec![x.as_bytes(), y.as_bytes(), c.as_bytes()]);
                            let _held = lock_for_script(&tags);
                        } else {
                            let keys = [
                                format!("{{{y}}}:k"),
                                format!("{{{c}}}:k"),
                                format!("{{{x}}}:k"),
                            ];
                            let _held = lock_keys(keys.iter().map(|k| k.as_bytes()));
                        }
                    }
                });
            }
        });

        let tags = ScriptTags::Several(vec![a.as_bytes(), b.as_bytes()]);
        let script = lock_for_script(&tags);
        let neither = format!("{{{c}}}:k");
        std::thread::scope(|scope| {
            scope
                .spawn(|| assert!(lock_keys([neither.as_bytes()].into_iter()).is_some()))
                .join()
                .unwrap();
        });
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let waiter = crate::cpus::spawn({
            let key = format!("{{{b}}}:k");
            move || {
                let _held = lock_keys([key.as_bytes()].into_iter());
                done_tx.send(()).unwrap();
            }
        });
        assert!(
            done_rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "a command on the script's second tag did not wait"
        );
        drop(script);
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("it was let through when the script ended");
        waiter.join().unwrap();
    }

    /// Turns of several tags are taken lowest stripe first: two scripts
    /// sharing two tags, named the other way round, never each hold the
    /// turn the other waits for.
    #[test]
    fn turns_of_several_tags_never_wait_for_each_other_in_a_cycle() {
        let a = b"turn-a".as_slice();
        let b = (0..)
            .map(|i| format!("turn-b{i}"))
            .find(|tag| stripe_of(tag.as_bytes()) != stripe_of(a))
            .unwrap();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut tasks = Vec::new();
            for flip in 0..8 {
                let b = b.clone();
                tasks.push(tokio::spawn(async move {
                    for _ in 0..500 {
                        let tags = if flip % 2 == 0 {
                            ScriptTags::Several(vec![a, b.as_bytes()])
                        } else {
                            ScriptTags::Several(vec![b.as_bytes(), a])
                        };
                        let _turn = script_turn(&tags).await;
                        tokio::task::yield_now().await;
                    }
                }));
            }
            for task in tasks {
                tokio::time::timeout(Duration::from_secs(30), task)
                    .await
                    .expect("the turns deadlocked")
                    .unwrap();
            }
        });
    }

    #[cfg(feature = "lua")]
    #[test]
    fn a_scripts_keys_are_told_apart_for_the_counters() {
        let kind = |cmd: &[&[u8]]| script_keys(cmd);
        assert_eq!(
            kind(&[b"EVALSHA", b"abc", b"2", b"{q}:a", b"x:{q}:b"]),
            ScriptKeys::Tagged
        );
        assert_eq!(kind(&[b"EVALSHA", b"abc", b"0"]), ScriptKeys::None);
        assert_eq!(
            kind(&[b"EVALSHA", b"abc", b"3", b"{q}:a"]),
            ScriptKeys::None
        );
        assert_eq!(kind(&[b"EVALSHA", b"abc", b"x"]), ScriptKeys::None);
        assert_eq!(
            kind(&[b"EVALSHA", b"abc", b"2", b"{q}:a", b"b"]),
            ScriptKeys::Untagged
        );
        assert_eq!(
            kind(&[b"EVALSHA", b"abc", b"2", b"a", b"{q}:b"]),
            ScriptKeys::Untagged
        );
        assert_eq!(
            kind(&[b"EVALSHA", b"abc", b"3", b"{q}:a", b"{r}:b", b"c"]),
            ScriptKeys::Untagged
        );
        assert_eq!(
            kind(&[b"EVALSHA", b"abc", b"2", b"{q}:a", b"{r}:b"]),
            ScriptKeys::Tagged
        );
    }

    #[test]
    fn a_script_is_held_to_its_tag() {
        assert!(script_may_touch(b"anything"), "no script here");
        let guard = lock_for_script(&ScriptTags::One(b"q1"));
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
        let script = lock_for_script(&ScriptTags::One(b"busy-queue"));
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
        let waiter = crate::cpus::spawn(move || {
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

    /// Active expiry's way: a held tag is not waited for, and its keys are
    /// left; the others, and keys without a tag, are taken.
    #[test]
    fn a_tag_that_is_held_is_left_and_not_waited_for() {
        let other = (0..)
            .map(|i| format!("free-{i}"))
            .find(|tag| stripe_of(tag.as_bytes()) != stripe_of(b"held-tag"))
            .unwrap();
        let free_key = format!("{{{other}}}:k");
        let held = lock_for_script(&ScriptTags::One(b"held-tag"));
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let started = std::time::Instant::now();
                    let tried = try_lock_keys(
                        [b"{held-tag}:a".as_slice(), free_key.as_bytes(), b"plain"].into_iter(),
                    )
                    .expect("two keys have a tag");
                    assert!(started.elapsed() < Duration::from_secs(1), "it waited");
                    assert!(tried.left_some());
                    assert!(!tried.may_take(b"{held-tag}:a"));
                    assert!(tried.may_take(free_key.as_bytes()));
                    assert!(tried.may_take(b"plain"));
                    assert!(try_lock_keys([b"plain".as_slice()].into_iter()).is_none());
                })
                .join()
                .unwrap();
        });
        drop(held);
    }

    /// A tag held for ever is waited for a bounded time at most, however
    /// often it is tried; and a tag that is busy nearly all the time, but
    /// let go of now and then, is taken in the end: its keys do not wait
    /// for ever.
    #[test]
    fn a_busy_tag_is_waited_for_a_little_and_not_for_ever() {
        let forever = lock_for_script(&ScriptTags::One(b"never-let-go"));
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    for _ in 0..10 {
                        let started = std::time::Instant::now();
                        let tried =
                            try_lock_keys([b"{never-let-go}:k".as_slice()].into_iter()).unwrap();
                        assert!(tried.left_some());
                        assert!(started.elapsed() < Duration::from_secs(1));
                    }
                })
                .join()
                .unwrap();
        });
        drop(forever);

        // Held by another thread all the time but for a moment between
        // holds, as a queue's scripts hold their tag.
        let stop = std::sync::atomic::AtomicBool::new(false);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                while !stop.load(Ordering::Relaxed) {
                    let _held = lock_for_script(&ScriptTags::One(b"busy-queue-tag"));
                    std::thread::sleep(Duration::from_micros(300));
                }
            });
            scope
                .spawn(|| {
                    std::thread::sleep(Duration::from_millis(20));
                    let taken = (0..200).any(|_| {
                        let tried =
                            try_lock_keys([b"{busy-queue-tag}:k".as_slice()].into_iter()).unwrap();
                        !tried.left_some()
                    });
                    stop.store(true, Ordering::Relaxed);
                    assert!(taken, "the busy tag was never taken");
                })
                .join()
                .unwrap();
        });
    }

    /// A tag's lock is never held without the write gate: whoever takes
    /// one enters the gate first, and lets go of it after.
    #[test]
    fn a_tag_is_held_inside_the_write_gate() {
        assert!(!crate::write_gate::held_by_this_thread());
        let keys = lock_keys([b"{gated}:k".as_slice()].into_iter());
        assert!(crate::write_gate::held_by_this_thread());
        drop(keys);
        assert!(!crate::write_gate::held_by_this_thread());
        assert!(lock_keys([b"untagged".as_slice()].into_iter()).is_none());
        assert!(!crate::write_gate::held_by_this_thread(), "no tag, no gate");
        let script = lock_for_script(&ScriptTags::One(b"gated"));
        assert!(crate::write_gate::held_by_this_thread());
        drop(script);
        assert!(!crate::write_gate::held_by_this_thread());
        let held = hold_script_to(&ScriptTags::One(b"gated"));
        assert!(
            !crate::write_gate::held_by_this_thread(),
            "no lock, no gate"
        );
        drop(held);
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

    /// `may_carry_tag` is a look for `{`, eight bytes at a time: every
    /// length up to 40, every byte in every place, among fillers that a
    /// borrow between bytes would trip over.
    #[test]
    fn a_key_may_carry_a_tag_when_it_has_a_brace() {
        // `tag_of` as it was, with `memchr`.
        let reference = |key: &[u8]| -> Option<Vec<u8>> {
            let open = memchr::memchr(b'{', key)?;
            let rest = &key[open + 1..];
            let close = memchr::memchr(b'}', rest)?;
            (close > 0).then(|| rest[..close].to_vec())
        };
        let check = |key: &[u8]| {
            assert_eq!(may_carry_tag(key), key.contains(&b'{'), "{key:?}");
            for byte in *b"{}" {
                assert_eq!(
                    position_of(byte, key),
                    memchr::memchr(byte, key),
                    "{byte} {key:?}"
                );
            }
            assert_eq!(tag_of(key).map(<[u8]>::to_vec), reference(key), "{key:?}");
            if tag_of(key).is_some() {
                assert!(may_carry_tag(key), "{key:?}");
            }
        };
        for len in 0..=40usize {
            for filler in [b'a', 0x00, 0x7a, 0x7c, 0xfb, 0xff, 0x80, 0x01] {
                let mut key = vec![filler; len];
                check(&key);
                for at in 0..len {
                    let bytes: Vec<u8> = if filler == b'a' {
                        (0..=255).collect()
                    } else {
                        vec![b'{', b'{' ^ 0x80, b'{' - 1, b'{' + 1, 0, 0xff, b'}']
                    };
                    for byte in bytes {
                        key[at] = byte;
                        check(&key);
                    }
                    key[at] = filler;
                }
            }
        }
        // Two braces in every pair of places: the first of two is found,
        // and the tag between them.
        for len in 0..=40usize {
            for open in 0..len {
                for other in 0..len {
                    for second in *b"}{" {
                        let mut key = vec![b'k'; len];
                        key[open] = b'{';
                        key[other] = second;
                        check(&key);
                    }
                }
            }
        }
        check(&[b'x'; 4096]);
        let mut long = vec![b'x'; 4096];
        long[4000] = b'{';
        check(&long);
    }

    /// For every command of the key table, and some without keys: the
    /// stripes `lock_named_keys` would take (through `named_keys`) are the
    /// ones `lock_keys` takes over everything `key_positions` finds, with a
    /// tagged or an untagged key, an empty tag, a `{` without its `}`,
    /// binary keys and keys longer than a word in each place (#889).
    #[test]
    fn named_keys_find_the_stripes_key_positions_finds() {
        use std::collections::BTreeSet;
        let mut names = crate::keyspec::table_names();
        names.extend(["PING", "MULTI", "EXEC", "EVALSHA", "FCALL", "SCAN", "X"]);
        let keys: &[&[u8]] = &[
            b"k",
            b"{t}:k",
            b"x:{u}",
            b"{}",
            b"{}k{v}",
            b"{k",
            b"k}",
            b"{",
            b"}{w}",
            b"\xff{\x00}\x80",
            b"\x00{t\x01}",
            b"1234567{",
            b"12345678{t}",
            b"key:000000001234",
            b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa{long}",
            b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ];
        // What the shapes with counts and options look for.
        let words: &[&[u8]] = &[
            b"0",
            b"1",
            b"2",
            b"3",
            b"-1",
            b"STREAMS",
            b"STORE",
            b"STOREDIST",
            b"KEYS",
            b"SET",
            b"USAGE",
            b"HELP",
            b"GROUP",
            b"$",
            b"",
        ];
        let stripes_of = |cmd: &[&[u8]], at: &mut dyn Iterator<Item = usize>| -> BTreeSet<usize> {
            at.filter_map(|at| tag_of(cmd[at]).map(stripe_of)).collect()
        };
        let compare = |name: &str, cmd: &[&[u8]]| -> bool {
            let general = stripes_of(cmd, &mut crate::keyspec::key_positions(name, cmd).iter());
            let fast = match named_keys(name, cmd) {
                None => BTreeSet::new(),
                Some(NamedKeys::Regular { from, last, step }) => {
                    stripes_of(cmd, &mut (from..=last).step_by(step))
                }
                Some(NamedKeys::Other(found)) => stripes_of(cmd, &mut found.iter()),
            };
            assert_eq!(fast, general, "{name} {cmd:?}");
            named_keys(name, cmd).is_none()
        };
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move |below: usize| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state % below as u64) as usize
        };
        let (mut cases, mut passed_by) = (0usize, 0usize);
        for name in &names {
            // One key of each kind in each place, the rest plain keys or
            // words.
            for len in 2..=7 {
                for filler in [&b"k"[..], b"2", b"STREAMS", b"STORE", b"KEYS"] {
                    for at in 1..len {
                        for key in keys {
                            let mut cmd: Vec<&[u8]> = vec![filler; len];
                            cmd[0] = name.as_bytes();
                            cmd[at] = key;
                            cases += 1;
                            passed_by += compare(name, &cmd) as usize;
                        }
                    }
                }
            }
            // Mixed at random.
            for len in 1..=10 {
                for _ in 0..40 {
                    let mut cmd: Vec<&[u8]> = (0..len)
                        .map(|_| {
                            if next(2) == 0 {
                                keys[next(keys.len())]
                            } else {
                                words[next(words.len())]
                            }
                        })
                        .collect();
                    cmd[0] = name.as_bytes();
                    cases += 1;
                    passed_by += compare(name, &cmd) as usize;
                }
            }
        }
        assert!(cases > 500_000, "{cases}");
        assert!(passed_by > cases / 10, "{passed_by} of {cases}");
    }

    /// What `lock_named_keys` takes: the stripe of each tagged key and the
    /// write gate, or nothing at all.
    #[test]
    fn a_command_takes_the_stripes_of_its_tagged_keys_and_nothing_else() {
        let held = |tag: &str| STRIPE[stripe_of(tag.as_bytes())].0.is_locked();
        let cmd = |parts: &[&str]| -> Vec<Vec<u8>> {
            parts.iter().map(|part| part.as_bytes().to_vec()).collect()
        };
        let lock = |parts: &[Vec<u8>]| {
            let args: Vec<&[u8]> = parts.iter().map(|part| part.as_slice()).collect();
            let name = String::from_utf8(parts[0].clone()).unwrap();
            lock_named_keys(&name, &args)
        };
        for untagged in [
            cmd(&["GET", "plain"]),
            cmd(&["SET", "plain", "{not-a-key}"]),
            cmd(&["GET", "{}"]),
            cmd(&["GET", "{open"]),
            cmd(&["MGET", "a", "b", "c{"]),
            cmd(&["MSET", "a", "{v}", "b", "{w}"]),
            cmd(&["PING"]),
            cmd(&["SORT", "plain", "BY", "{x}:*"]),
        ] {
            assert!(lock(&untagged).is_none(), "{untagged:?}");
            assert!(!crate::write_gate::held_by_this_thread());
        }
        let guard = lock(&cmd(&["GET", "{nk889-a}:k"])).expect("a tagged key");
        assert!(crate::write_gate::held_by_this_thread());
        assert!(held("nk889-a"));
        drop(guard);
        assert!(!crate::write_gate::held_by_this_thread());
        let guard = lock(&cmd(&[
            "MSET",
            "plain",
            "1",
            "{nk889-b}:k",
            "2",
            "x{nk889-c}",
            "3",
        ]))
        .expect("tagged keys");
        assert!(held("nk889-b") && held("nk889-c"));
        drop(guard);
        let guard = lock(&cmd(&["SORT", "plain", "STORE", "{nk889-d}:dst"])).expect("STORE");
        assert!(held("nk889-d"));
        drop(guard);
        assert!(!crate::write_gate::held_by_this_thread());
    }
}
