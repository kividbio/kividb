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
// * Except a script whose `KEYS` all lack a tag and that keeps to them: one
//   whose source names no key but `KEYS[n]`, and calls only commands whose
//   keys are known (`lua_keys_only`). It is held to its keys, as a tagged
//   script is to its tags, each key standing for its own tag, and runs
//   beside others under their locks — once keys without a tag are locked
//   too. That starts with the first such script, run alone
//   ([`lock_untagged_keys_for`]); until then a command on a key without a
//   `{` makes one load more. Not for `EVAL_RO`/`EVALSHA_RO`: those run
//   alone. Eviction and active expiry leave a key whose lock a script
//   holds. Keys without a tag stop being locked once no such script has
//   run for a while (`maybe_clear_untagged`).
// * Whether a client's command locks its keys is decided inside the command
//   gate, where nothing switches: tags always, in a build with Lua
//   ([`enabled`]); keys without a tag, by a switch only ever made with the
//   gate held alone. A script decides before the gate, after it is counted
//   among those that run beside others, and a switch that stops locking keys
//   without a tag makes sure no such script is counted
//   ([`try_clear_untagged`]).
// * A command from a client takes the lock of every tagged key it names, so
//   it finds a script's keys as they were before the script or as they are
//   after it, never between. Keys without a tag belong to no script and
//   cost a look for `{`, and a load — until keys without a tag are locked
//   too: then each is locked as its own tag.
// * A command that changes keys it does not name — `FLUSHALL`, `FLUSHDB`,
//   `SWAPDB`, `FT.DROPINDEX … DD`, `DEBUG POPULATE` — takes the gate alone, which waits for every script (a script
//   holds the gate as a command for as long as it runs). One that only reads
//   them — `KEYS`, `SCAN`, `INFO`, a search of an index, `SORT` by a pattern
//   — keeps the scripts that run beside others out while it runs, and
//   nothing else (`walks`, `walk`, #772); `DBSIZE` and `RANDOMKEY` take the
//   gate alone.
//
// The locks are stripes: a fixed table indexed by a hash of the tag. Two
// tags that land on one stripe wait for each other when they need not, and
// nothing else; a script whose tags land on one stripe takes it once.
//
// Whoever holds more than one stripe took them in the order of the table,
// lowest first, and takes no stripe while it holds a later one: a client's
// command (`lock_found_keys`), a script (`lock_for_script`) and active expiry
// (`try_lock_keys`, which waits a bounded time at most). So no two of them
// wait for each other. A script's turns among the scripts of its tags
// (`script_turn`) are taken the same way, lowest stripe first, before any
// gate or stripe. A client's command that finds a script on some of its
// tags takes the turns of those tags too, the same way, holding nothing
// else: its connection waits for them off its thread, and the command is
// dispatched again holding them (`try_lock_found_keys`, `command_turns`,
// #882). Should it find a script on another of its tags then, it lets go and
// waits for the turns of both, so it waits at most once per tag.
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
#[cfg(feature = "lua")]
use parking_lot::{RwLock, RwLockWriteGuard};
use std::cell::{Cell, RefCell};
use std::marker::PhantomData;
#[cfg(feature = "lua")]
use std::sync::atomic::AtomicBool;
#[cfg(feature = "lua")]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

/// `lua-parallel-scripts`: which scripts may run beside others, as
/// [`NO`], [`TAGS`] or [`YES`].
static PARALLEL: AtomicU8 = AtomicU8::new(NO);

const NO: u8 = 0;
const TAGS: u8 = 1;
const YES: u8 = 2;

/// Whether a client's command locks the hash tags of the keys it names (and,
/// once those are locked, its keys without one): always in a build with Lua,
/// whatever `lua-parallel-scripts` says. A command asks this before it is in
/// the command gate (`handler::run_command`), where a setting could change
/// before it gets in; a constant cannot. A build without Lua runs no script
/// beside others, and goes by the setting as it always has: keys without a
/// tag are never locked there.
#[inline(always)]
pub fn enabled() -> bool {
    #[cfg(feature = "lua")]
    {
        true
    }
    #[cfg(not(feature = "lua"))]
    {
        PARALLEL.load(Ordering::Relaxed) != NO
    }
}

/// `lua-parallel-scripts`, as it is now: whether a script may run beside
/// others (`handler::script_mode`, `lua_vm_blocking`). A script asks it
/// after it is counted among the scripts that run beside others
/// ([`run_beside_others`]), so a switch of keys without a tag
/// ([`try_clear_untagged`]) sees every script that may have read the old
/// value. `SeqCst`, as the count is.
#[inline]
pub fn parallel_scripts() -> bool {
    PARALLEL.load(Ordering::SeqCst) != NO
}

/// Whether a script that keeps to its keys, none of which has a tag, may
/// run beside others under its keys' locks: `lua-parallel-scripts yes`.
/// Read as [`parallel_scripts`] is.
#[cfg(feature = "lua")]
#[inline]
fn keys_beside() -> bool {
    PARALLEL.load(Ordering::SeqCst) == YES
}

/// `lua-parallel-scripts`, for `CONFIG GET`.
pub fn mode() -> crate::config::ParallelScripts {
    match PARALLEL.load(Ordering::SeqCst) {
        NO => crate::config::ParallelScripts::No,
        TAGS => crate::config::ParallelScripts::Tags,
        _ => crate::config::ParallelScripts::Yes,
    }
}

/// `CONFIG SET lua-parallel-scripts` and the same at startup. `CONFIG SET`
/// holds the command gate alone (`tag_locks::is_switch`). A script that read
/// the old setting before the gate may still run after it: a script that
/// runs beside others then finds the tags of its keys locked by every
/// command all the same ([`enabled`]), and keys without a tag stay locked
/// until no script that keeps to such keys runs beside others
/// ([`try_clear_untagged`], tried here and again later). In a build without
/// Lua it switches the locks of tags (`tags` and `yes` alike), as it always
/// has; keys without a tag are never locked there.
pub fn set_mode(mode: crate::config::ParallelScripts) {
    let value = match mode {
        crate::config::ParallelScripts::No => NO,
        crate::config::ParallelScripts::Tags => TAGS,
        crate::config::ParallelScripts::Yes => YES,
    };
    PARALLEL.store(value, Ordering::SeqCst);
    #[cfg(feature = "lua")]
    if value != YES && crate::command_gate::alone_on_this_thread() {
        try_clear_untagged();
    }
}

/// [`set_mode`]: `yes` or `no`.
pub fn set_enabled(on: bool) {
    set_mode(if on {
        crate::config::ParallelScripts::Yes
    } else {
        crate::config::ParallelScripts::No
    });
}

/// Keys without a hash tag are locked too, each as its own tag: from the
/// first script that keeps to its keys and has none with a tag, run alone
/// ([`lock_untagged_keys_for`]), until no such script has run for
/// [`quiet_for`] or `lua-parallel-scripts` is switched off and no script
/// runs beside others ([`try_clear_untagged`]). Only ever switched with the
/// command gate held alone.
#[cfg(feature = "lua")]
static UNTAGGED: AtomicBool = AtomicBool::new(false);

/// How many scripts under their keys' locks ([`ScriptTags::Key`],
/// [`ScriptTags::Keys`]) run beside others, or are about to: see
/// [`try_clear_untagged`].
#[cfg(feature = "lua")]
static KEYS_ONLY_BESIDE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Held by a script under its keys' locks that runs beside others, from
/// before it reads whether keys are locked ([`whole_keys`], again inside the
/// command) until it has run: counted in [`KEYS_ONLY_BESIDE`].
#[cfg(feature = "lua")]
pub struct KeysOnlyBeside(());

#[cfg(feature = "lua")]
impl Drop for KeysOnlyBeside {
    #[inline]
    fn drop(&mut self) {
        KEYS_ONLY_BESIDE.fetch_sub(1, Ordering::SeqCst);
    }
}

/// For a script about to run beside others (`lua_vm_blocking`): counted in
/// [`KEYS_ONLY_BESIDE`] if it is to run under its keys' locks, before the
/// command reads again whether keys are locked. Nothing for one under its
/// tags' locks.
#[cfg(feature = "lua")]
#[inline]
pub fn count_keys_only(tags: &ScriptTags) -> Option<KeysOnlyBeside> {
    tags.whole_keys().then(|| {
        KEYS_ONLY_BESIDE.fetch_add(1, Ordering::SeqCst);
        KeysOnlyBeside(())
    })
}

/// Times [`UNTAGGED`] was switched on, and off, for `INFO`.
#[cfg(feature = "lua")]
static SWITCHED_ON: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "lua")]
static SWITCHED_OFF: AtomicU64 = AtomicU64::new(0);

/// When a script that keeps to its keys last ran, in milliseconds since
/// [`STARTED`].
#[cfg(feature = "lua")]
static LAST_KEYS_ONLY: AtomicU64 = AtomicU64::new(0);

#[cfg(feature = "lua")]
static STARTED: std::sync::LazyLock<std::time::Instant> =
    std::sync::LazyLock::new(std::time::Instant::now);

#[cfg(feature = "lua")]
fn now_ms() -> u64 {
    STARTED.elapsed().as_millis() as u64
}

/// How long without a script that keeps to its keys before keys without a
/// tag stop being locked: a minute, or in debug builds
/// `KIVIDB_TEST_KEY_LOCKS_QUIET_MS` (never set otherwise).
#[cfg(feature = "lua")]
fn quiet_for() -> Duration {
    #[cfg(debug_assertions)]
    {
        static FOR: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
        *FOR.get_or_init(|| {
            std::env::var("KIVIDB_TEST_KEY_LOCKS_QUIET_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .map_or(Duration::from_secs(60), Duration::from_millis)
        })
    }
    #[cfg(not(debug_assertions))]
    {
        Duration::from_secs(60)
    }
}

/// Whether keys without a hash tag are locked too ([`UNTAGGED`]), for a
/// client's command, inside the command gate: the switch is made with the
/// gate held alone, so what this reads cannot change while the command
/// runs. One load; none in a build without Lua.
#[inline(always)]
pub fn untagged_keys_locked() -> bool {
    #[cfg(feature = "lua")]
    {
        UNTAGGED.load(Ordering::Relaxed)
    }
    #[cfg(not(feature = "lua"))]
    {
        false
    }
}

/// Whether the client's command `cmd` (its name as sent) locks keys without
/// a tag that it names: once those are locked ([`untagged_keys_locked`]),
/// every command but `MIGRATE`. A `MIGRATE` holds back writes to the keys it
/// moves itself, and takes their order, from before it reads them until they
/// have gone or stayed (`cluster::hold_while_moving`): a script that runs
/// beside others holds the order of its keys for its whole run, so the
/// `MIGRATE` reads them before or after it, and a script on a key being
/// moved waits for the move before it starts. A key's lock held by a
/// `MIGRATE` would hold its reads back for as long as the move takes, and
/// the `MIGRATE` would take the write gate with it, which its hold takes as
/// a sign that it runs inside a script or a transaction.
#[inline]
fn command_locks_untagged_keys(cmd: &[&[u8]]) -> bool {
    untagged_keys_locked()
        && cmd
            .first()
            .is_none_or(|name| !name.eq_ignore_ascii_case(b"MIGRATE"))
}

/// The lock `key` is held by, if any: its hash tag's ([`STRIPE`], below
/// [`STRIPES`]), or â€” when keys without a tag are locked (`untagged`) â€” the
/// whole key's ([`KEY_STRIPE`], [`STRIPES`] and up).
#[inline(always)]
fn lock_id(key: &[u8], untagged: bool) -> Option<usize> {
    match tag_of(key) {
        Some(tag) => Some(stripe_of(tag)),
        None => untagged.then(|| STRIPES + stripe_of(key)),
    }
}

/// For a script command (`cmd`, with its arguments) run with the command
/// gate held alone: if its `KEYS` all lack a hash tag, and it may run beside
/// others under them ([`keys_only_script_may_run_beside`]), keys without a
/// tag are locked from now on ([`UNTAGGED`]), so that the next such script
/// can run beside others. With the gate alone no command is in it, and a
/// client's command asks whether to lock its keys only inside it: every
/// command after the switch finds it made, before any script that reads it
/// is in the gate. Not from a master's stream or the AOF.
#[cfg(feature = "lua")]
#[cold]
#[inline(never)]
pub fn lock_untagged_keys_for(cmd: &[&[u8]]) {
    if crate::expiry::replaying() || !crate::command_gate::alone_on_this_thread() || !keys_beside()
    {
        return;
    }
    let keys = std::str::from_utf8(cmd.get(2).copied().unwrap_or_default())
        .ok()
        .and_then(|n| n.parse::<usize>().ok())
        .and_then(|n| cmd.get(3..3usize.checked_add(n)?));
    let Some(keys) = keys.filter(|keys| !keys.is_empty()) else {
        return;
    };
    if keys.iter().any(|key| tag_of(key).is_some()) || !keys_only_script_may_run_beside(cmd) {
        return;
    }
    LAST_KEYS_ONLY.store(now_ms(), Ordering::Relaxed);
    if !UNTAGGED.swap(true, Ordering::SeqCst) {
        SWITCHED_ON.fetch_add(1, Ordering::Relaxed);
        watch_for_quiet();
    }
}

/// With the command gate held alone: keys without a tag stop being locked,
/// unless a script under its keys' locks runs beside others, or is counted
/// to (it may have read that they are): [`KEYS_ONLY_BESIDE`], which scripts
/// under their tags' locks are not counted in, so that a stream of those
/// does not keep keys locked. Which of the two the script sees is settled by
/// the order of two steps on each side, all `SeqCst`: this clears the switch
/// and then reads the count; a script is counted ([`count_keys_only`]) and
/// then reads the switch ([`whole_keys`]). Either this sees the script, and puts
/// the switch back, or the script sees the switch off, and runs alone. No
/// client's command is in the gate meanwhile, and each one after it reads
/// the switch as it is then. True if keys without a tag are not locked now.
#[cfg(feature = "lua")]
fn try_clear_untagged() -> bool {
    if !UNTAGGED.load(Ordering::SeqCst) {
        return true;
    }
    UNTAGGED.store(false, Ordering::SeqCst);
    if KEYS_ONLY_BESIDE.load(Ordering::SeqCst) != 0 {
        UNTAGGED.store(true, Ordering::SeqCst);
        return false;
    }
    SWITCHED_OFF.fetch_add(1, Ordering::Relaxed);
    true
}

/// With the command gate held alone (a script that runs alone, the thread of
/// [`watch_for_quiet`]): keys without a tag stop being locked if no script
/// that keeps to its keys has run for [`quiet_for`], or scripts may not run
/// beside others at all ([`try_clear_untagged`]). One load while they are
/// not locked.
#[cfg(feature = "lua")]
#[inline]
pub fn maybe_clear_untagged() {
    if UNTAGGED.load(Ordering::Relaxed) {
        clear_untagged_if_quiet();
    }
}

#[cfg(feature = "lua")]
#[cold]
#[inline(never)]
fn clear_untagged_if_quiet() {
    if !crate::command_gate::alone_on_this_thread() {
        return;
    }
    let quiet = now_ms().saturating_sub(LAST_KEYS_ONLY.load(Ordering::Relaxed))
        >= quiet_for().as_millis() as u64;
    if quiet || !keys_beside() {
        try_clear_untagged();
    }
}

/// How long [`watch_for_quiet`] tries for the command gate alone in a round,
/// at most, before it leaves it for the next round.
#[cfg(feature = "lua")]
const WATCH_TRY: Duration = Duration::from_millis(2);

/// A thread that, while keys without a tag are locked and no script that
/// keeps to its keys has run for [`quiet_for`], holds the command gate
/// alone for a moment to stop locking them ([`maybe_clear_untagged`]), so
/// that a server where such scripts ran once does not lock every command's
/// keys for good. Started once, with the first switch.
///
/// It never waits long for the gate: it tries for [`WATCH_TRY`] at most and,
/// if something holds the gate longer (a script, a long command), leaves it
/// for the next round. Nor does it try while clients are paused or a
/// handoff pauses writes: the commands held back then are in the gate or
/// waiting to get in, and the round after the pause does it.
#[cfg(feature = "lua")]
fn watch_for_quiet() {
    static STARTED_WATCH: std::sync::Once = std::sync::Once::new();
    STARTED_WATCH.call_once(|| {
        use crate::cpus::BuilderExt as _;
        let _ = std::thread::Builder::new()
            .name("key-locks-watch".into())
            .spawn_released(|| {
                loop {
                    let every = (quiet_for() / 4).max(Duration::from_millis(20));
                    std::thread::sleep(every);
                    if !UNTAGGED.load(Ordering::Relaxed) {
                        continue;
                    }
                    let since = now_ms().saturating_sub(LAST_KEYS_ONLY.load(Ordering::Relaxed));
                    if since < quiet_for().as_millis() as u64 && keys_beside() {
                        continue;
                    }
                    if crate::pause::active().is_some() || crate::pause::handoff() {
                        continue;
                    }
                    let until = std::time::Instant::now() + WATCH_TRY;
                    let Some(_alone) =
                        crate::command_gate::enter_alone_unless(Duration::from_micros(500), || {
                            std::time::Instant::now() >= until
                        })
                    else {
                        WATCH_GAVE_UP.fetch_add(1, Ordering::Relaxed);
                        continue;
                    };
                    maybe_clear_untagged();
                }
            });
    });
}

/// Rounds of [`watch_for_quiet`] that left the gate for the next one, for
/// `INFO`.
#[cfg(feature = "lua")]
static WATCH_GAVE_UP: AtomicU64 = AtomicU64::new(0);

/// For `INFO stats`: whether keys without a tag are locked now, how many
/// times that was switched on and off, and how many times the switch off
/// was left for later because the command gate was busy.
#[cfg(feature = "lua")]
pub fn key_locks_info(out: &mut String) {
    out.push_str(&format!(
        "untagged_key_locks:{}\r\nuntagged_key_locks_on:{}\r\nuntagged_key_locks_off:{}\r\nuntagged_key_locks_off_later:{}\r\n",
        u8::from(UNTAGGED.load(Ordering::Relaxed)),
        SWITCHED_ON.load(Ordering::Relaxed),
        SWITCHED_OFF.load(Ordering::Relaxed),
        WATCH_GAVE_UP.load(Ordering::Relaxed),
    ));
}

/// Whether a script none of whose keys has a tag may run beside others under
/// its keys' locks: it keeps to them (`lua_keys_only`), and it is sent by
/// `EVAL` or `EVALSHA`.
///
/// `EVAL` and `EVALSHA`, and not their `_RO` forms: a script of those holds
/// the order of its keys (`write_order`) for its whole run, as every write
/// does, and that is what keeps a `MIGRATE` of one of them, which takes no
/// key's lock ([`command_locks_untagged_keys`]), to before or after the
/// script. Eviction and active expiry leave a key whose lock a script holds
/// ([`try_hold_for_eviction`], [`try_lock_keys`]).
#[cfg(feature = "lua")]
fn keys_only_script_may_run_beside(cmd: &[&[u8]]) -> bool {
    let writes = cmd.first().is_some_and(|name| {
        name.eq_ignore_ascii_case(b"EVAL") || name.eq_ignore_ascii_case(b"EVALSHA")
    });
    writes && crate::commands::script_keeps_to_its_keys(cmd)
}

/// A power of two.
const STRIPES: usize = 1024;

#[repr(align(64))]
struct Stripe(Mutex<()>);

/// The locks of hash tags.
static STRIPE: [Stripe; STRIPES] = [const { Stripe(parking_lot::const_mutex(())) }; STRIPES];

/// The locks of keys without a tag, once those are locked ([`UNTAGGED`]):
/// held whole by a script that runs beside others under them, shared by
/// everything else (a client's command, active expiry, eviction), which
/// then waits only for such a script, not for each other.
#[cfg(feature = "lua")]
#[repr(align(64))]
struct KeyStripe(RwLock<()>);

#[cfg(feature = "lua")]
static KEY_STRIPE: [KeyStripe; STRIPES] =
    [const { KeyStripe(parking_lot::const_rwlock(())) }; STRIPES];

/// A client's hold of lock `id` ([`lock_id`]): a tag's whole, as a tagged
/// script takes it; a key's shared. Kept as the id, the guard forgotten, and
/// let go of by [`release`]: a command's locks are then a short list of
/// numbers, with nothing to allocate for up to [`INLINE`] of them.
#[cfg(feature = "lua")]
#[inline]
fn take(id: usize) {
    if id < STRIPES {
        std::mem::forget(STRIPE[id].0.lock());
    } else {
        #[cfg(feature = "lua")]
        std::mem::forget(KEY_STRIPE[id - STRIPES].0.read());
    }
}

/// [`take`], if nothing holds the lock whole.
#[inline]
fn try_take(id: usize) -> bool {
    if id < STRIPES {
        STRIPE[id].0.try_lock().map(std::mem::forget).is_some()
    } else {
        #[cfg(feature = "lua")]
        {
            KEY_STRIPE[id - STRIPES]
                .0
                .try_read()
                .map(std::mem::forget)
                .is_some()
        }
        #[cfg(not(feature = "lua"))]
        {
            true
        }
    }
}

/// [`try_take`], waiting up to `wait`.
#[cfg(feature = "lua")]
fn try_take_for(id: usize, wait: Duration) -> bool {
    if id < STRIPES {
        STRIPE[id]
            .0
            .try_lock_for(wait)
            .map(std::mem::forget)
            .is_some()
    } else {
        KEY_STRIPE[id - STRIPES]
            .0
            .try_read_for(wait)
            .map(std::mem::forget)
            .is_some()
    }
}

/// Let go of a hold [`take`] or one of the tries made.
///
/// # Safety
///
/// `id` must be held by this caller through [`take`], [`try_take`] or
/// [`try_take_for`], and not let go of yet.
#[inline]
unsafe fn release(id: usize) {
    if id < STRIPES {
        // SAFETY: held, as the caller says, its guard forgotten.
        unsafe { STRIPE[id].0.force_unlock() }
    } else {
        #[cfg(feature = "lua")]
        // SAFETY: as above, a shared hold.
        unsafe {
            KEY_STRIPE[id - STRIPES].0.force_unlock_read()
        }
    }
}

/// How many locks a list of them ([`Ids`]) keeps without allocating: the
/// keys of nearly every command.
const INLINE: usize = 16;

/// The locks a client's command holds, or is about to take: each once, and,
/// once [`Ids::sorted`], lowest first â€” the order every holder of several
/// takes them in, the tags' (below [`STRIPES`]) before the keys'.
struct Ids {
    len: usize,
    inline: [u16; INLINE],
    more: Vec<u16>,
}

impl Ids {
    #[cfg(feature = "lua")]
    #[inline]
    fn one(id: usize) -> Self {
        let mut inline = [0; INLINE];
        inline[0] = id as u16;
        Ids {
            len: 1,
            inline,
            more: Vec::new(),
        }
    }

    #[inline]
    fn new() -> Self {
        Ids {
            len: 0,
            inline: [0; INLINE],
            more: Vec::new(),
        }
    }

    /// Add `id`, if it is not there yet (up to [`INLINE`]; past that, made
    /// unique by [`Ids::sorted`]).
    #[inline]
    fn add(&mut self, id: usize) {
        let id = id as u16;
        if self.len <= INLINE {
            if self.inline[..self.len].contains(&id) {
                return;
            }
            if self.len < INLINE {
                self.inline[self.len] = id;
                self.len += 1;
                return;
            }
            self.more.extend_from_slice(&self.inline);
            self.len = INLINE + 1;
        }
        self.more.push(id);
    }

    /// Lowest first, each once.
    fn sorted(&mut self) {
        if self.len > INLINE {
            self.more.sort_unstable();
            self.more.dedup();
        } else {
            self.inline[..self.len].sort_unstable();
        }
    }

    #[inline]
    fn as_slice(&self) -> &[u16] {
        if self.len > INLINE {
            &self.more
        } else {
            &self.inline[..self.len]
        }
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.len == 0
    }
}

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
    /// They are whole keys, not tags ([`ScriptTags::Key`]).
    whole_keys: bool,
    bytes: Vec<u8>,
    ends: Vec<usize>,
}

impl HeldTags {
    const fn new() -> Self {
        HeldTags {
            held: false,
            whole_keys: false,
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
        self.whole_keys = tags.whole_keys();
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
        self.whole_keys = false;
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
    /// The one key, without a tag, of a script that keeps to its `KEYS`
    /// (`lua_keys_only`): locked as its own tag, once keys without a tag
    /// are ([`untagged_keys_locked`]).
    #[cfg(feature = "lua")]
    Key(&'a [u8]),
    /// Several such keys, in the order of [`ScriptTags::Several`].
    #[cfg(feature = "lua")]
    Keys(Vec<&'a [u8]>),
}

impl<'a> ScriptTags<'a> {
    pub fn as_slice(&self) -> &[&'a [u8]] {
        match self {
            ScriptTags::One(tag) => std::slice::from_ref(tag),
            ScriptTags::Several(tags) => tags,
            #[cfg(feature = "lua")]
            ScriptTags::Key(key) => std::slice::from_ref(key),
            #[cfg(feature = "lua")]
            ScriptTags::Keys(keys) => keys,
        }
    }

    /// Whether these are whole keys rather than tags.
    pub fn whole_keys(&self) -> bool {
        #[cfg(feature = "lua")]
        {
            matches!(self, ScriptTags::Key(_) | ScriptTags::Keys(_))
        }
        #[cfg(not(feature = "lua"))]
        {
            false
        }
    }

    /// The locks of the tags (or keys), each once, lowest first: the order
    /// every holder of several takes them in ([`lock_id`]).
    fn stripes(&self) -> Vec<usize> {
        let offset = if self.whole_keys() { STRIPES } else { 0 };
        let mut stripes: Vec<usize> = self
            .as_slice()
            .iter()
            .map(|tag| offset + stripe_of(tag))
            .collect();
        stripes.sort_unstable();
        stripes.dedup();
        stripes
    }
}

/// The hash tags of a script command's `KEYS`: `cmd` is `EVAL`/`EVALSHA` (or
/// the `_RO` forms) with its arguments. `None` if it has no keys or a key
/// has no tag — unless no key has one, the script keeps to its keys, and
/// keys without a tag are locked: then the keys themselves
/// ([`ScriptTags::Key`], [`whole_keys`]). Keys of one tag, the usual case,
/// allocate nothing; nor does one key without a tag.
#[cfg_attr(feature = "lua", inline(never))]
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
    let Some(first) = tag_of(first) else {
        return whole_keys(cmd, keys);
    };
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

/// [`script_tags`] for a script whose first key has no tag: its keys, if
/// none has a tag, keys without one are locked ([`untagged_keys_locked`]),
/// and it may run beside others under them
/// ([`keys_only_script_may_run_beside`]).
#[cfg(feature = "lua")]
#[inline(never)]
fn whole_keys<'a>(cmd: &[&'a [u8]], keys: &[&'a [u8]]) -> Option<ScriptTags<'a>> {
    // `SeqCst`, after the script was counted (`count_keys_only`): see
    // `try_clear_untagged`. And not once `lua-parallel-scripts` says
    // otherwise, so that keys stop being locked soon after.
    if !UNTAGGED.load(Ordering::SeqCst)
        || !keys_beside()
        || keys.iter().any(|key| tag_of(key).is_some())
        || !keys_only_script_may_run_beside(cmd)
    {
        return None;
    }
    LAST_KEYS_ONLY.store(now_ms(), Ordering::Relaxed);
    let (&first, rest) = keys.split_first()?;
    let mut others: Vec<&[u8]> = rest.iter().copied().filter(|&key| key != first).collect();
    if others.is_empty() {
        return Some(ScriptTags::Key(first));
    }
    others.sort_unstable();
    others.dedup();
    others.insert(0, first);
    Some(ScriptTags::Keys(others))
}

#[cfg(not(feature = "lua"))]
#[inline(always)]
fn whole_keys<'a>(_cmd: &[&'a [u8]], _keys: &[&'a [u8]]) -> Option<ScriptTags<'a>> {
    None
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
    One {
        _held: MutexGuard<'static, ()>,
    },
    Several {
        _held: Vec<MutexGuard<'static, ()>>,
    },
    /// Keys without a tag ([`ScriptTags::Key`]): their locks, whole.
    #[cfg(feature = "lua")]
    Keys {
        _held: Vec<RwLockWriteGuard<'static, ()>>,
    },
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
#[cfg_attr(feature = "lua", inline(never))]
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
        #[cfg(feature = "lua")]
        ScriptTags::Key(_) | ScriptTags::Keys(_) => Stripes::Keys {
            _held: tags
                .stripes()
                .into_iter()
                .map(|id| KEY_STRIPE[id - STRIPES].0.write())
                .collect(),
        },
    };
    hold(tags, stripes, Some(gate))
}

/// The turns of the scripts of each stripe: see [`script_turn`].
static TURN: [tokio::sync::Mutex<()>; 2 * STRIPES] =
    [const { tokio::sync::Mutex::const_new(()) }; 2 * STRIPES];

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
        #[cfg(feature = "lua")]
        ScriptTags::Key(key) => ScriptTurn::One {
            _turn: TURN[STRIPES + stripe_of(key)].lock().await,
        },
        #[cfg(feature = "lua")]
        ScriptTags::Keys(_) => several_turns(tags).await,
        ScriptTags::Several(_) => several_turns(tags).await,
    }
}

/// [`script_turn`] for several tags or keys: each, lowest first.
async fn several_turns(tags: &ScriptTags<'_>) -> ScriptTurn {
    let mut turns = Vec::new();
    for stripe in tags.stripes() {
        turns.push(TURN[stripe].lock().await);
    }
    ScriptTurn::Several { _turns: turns }
}

/// Hold a script to `tags` without their locks: one that has the command
/// gate alone, and so has nothing to be locked against.
#[cfg_attr(feature = "lua", inline(never))]
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
/// script may touch, a key of one of its tags, or one of its keys if it is
/// held to those ([`ScriptTags::Key`]). True when no script is running
/// under its tags here.
pub fn script_may_touch(key: &[u8]) -> bool {
    SCRIPT_TAGS.with(|tags| {
        let tags = tags.borrow();
        if !tags.held {
            return true;
        }
        if tags.whole_keys {
            return tags.holds(key);
        }
        tag_of(key).is_some_and(|tag| tags.holds(tag))
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
        match (tags.whole_keys, quoted.len()) {
            (true, 1) => format!("key {}", quoted[0]),
            (true, _) => format!("keys {}", quoted.join(", ")),
            (false, 1) => format!("hash tag {}", quoted[0]),
            (false, _) => format!("hash tags {}", quoted.join(", ")),
        }
    })
}

/// Held by a command from a client: the lock of every tagged key it names
/// (and of each of its keys without a tag, once those are locked), and the
/// write gate, entered before them and let go of after them.
///
/// What it holds is let go of out of line ([`Drop`]), as a [`ScriptGuard`]'s
/// is: the guard is an `Option` in the way of every command
/// (`handler::run_command`), and letting go of its locks has no business
/// there.
#[cfg(feature = "lua")]
pub struct KeysGuard {
    held: std::mem::ManuallyDrop<KeysHeld>,
}

/// In a build without Lua, which locks no key without a tag: the locks of
/// the tags, as guards.
#[cfg(not(feature = "lua"))]
pub struct KeysGuard {
    _one: Option<MutexGuard<'static, ()>>,
    _more: Vec<MutexGuard<'static, ()>>,
    _gate: crate::write_gate::WriteGuard,
}

/// What a [`KeysGuard`] holds: its locks, let go of first, then the gate.
#[cfg(feature = "lua")]
struct KeysHeld {
    locks: Locks,
    _gate: crate::write_gate::WriteGuard,
}

/// A command's locks: one, as nearly every command on a tagged key holds,
/// or a list of them.
#[cfg(feature = "lua")]
enum Locks {
    One(u16),
    Many(Ids),
}

#[cfg(feature = "lua")]
impl KeysGuard {
    #[inline]
    fn new(ids: Ids, gate: crate::write_gate::WriteGuard) -> Self {
        KeysGuard {
            held: std::mem::ManuallyDrop::new(KeysHeld {
                locks: Locks::Many(ids),
                _gate: gate,
            }),
        }
    }

    #[inline]
    fn one(id: usize, gate: crate::write_gate::WriteGuard) -> Self {
        KeysGuard {
            held: std::mem::ManuallyDrop::new(KeysHeld {
                locks: Locks::One(id as u16),
                _gate: gate,
            }),
        }
    }
}

#[cfg(feature = "lua")]
impl Drop for KeysGuard {
    #[inline(never)]
    fn drop(&mut self) {
        match &self.held.locks {
            // SAFETY: taken once by whoever made this guard, and not let go
            // of since; the same for each of a list.
            Locks::One(id) => unsafe { release(*id as usize) },
            Locks::Many(ids) => {
                for &id in ids.as_slice() {
                    // SAFETY: as above.
                    unsafe { release(id as usize) }
                }
            }
        }
        // SAFETY: dropped here once; the field is not used again.
        unsafe { std::mem::ManuallyDrop::drop(&mut self.held) }
    }
}

/// Take the lock of each tagged key in `keys`. `None` â€” nothing taken, and
/// nothing allocated â€” when no key has a tag, which is the usual case.
#[cfg(test)]
fn lock_keys<'a>(keys: impl Iterator<Item = &'a [u8]>) -> Option<KeysGuard> {
    lock_keys_as(keys, untagged_keys_locked())
}

/// The locks of `keys`, keys without a tag among them if `untagged`.
///
/// The write gate first (see the top), for a read too: one that finds a key
/// past its time deletes it under the gate, and must not enter it with a
/// tag held. A write holds the gate already, and this costs it a count.
#[cfg(feature = "lua")]
fn lock_keys_as<'a>(keys: impl Iterator<Item = &'a [u8]>, untagged: bool) -> Option<KeysGuard> {
    let mut ids = Ids::new();
    for key in keys {
        if let Some(id) = lock_id(key, untagged) {
            ids.add(id);
        }
    }
    if ids.is_empty() {
        return None;
    }
    let gate = crate::write_gate::enter();
    // Several: in the order of the table, so that two commands naming the
    // same tags the other way round do not wait for each other.
    ids.sorted();
    for &id in ids.as_slice() {
        take(id as usize);
    }
    Some(KeysGuard::new(ids, gate))
}

/// [`lock_keys_as`] in a build without Lua: the tags' locks only.
#[cfg(not(feature = "lua"))]
fn lock_keys_as<'a>(keys: impl Iterator<Item = &'a [u8]>, _untagged: bool) -> Option<KeysGuard> {
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
///
/// Every command from a client asks this of its keys (#889), so it only
/// says whether, not where: up to 32 bytes, two or four words that may
/// overlap, looked at together with no branch between them; `memchr` past
/// that. A word's bytes that are `{` are the zero bytes of its `^` with
/// `{{{{{{{{`, and `(x - 0x01…) & !x & 0x80…` is not 0 exactly when `x`
/// has a zero byte.
#[inline]
pub fn may_carry_tag(key: &[u8]) -> bool {
    const ONES: u64 = u64::from_ne_bytes([1; 8]);
    const HIGHS: u64 = ONES << 7;
    const BRACES: u64 = ONES * b'{' as u64;
    // A word of eight bytes, or of none (0, which has no `{`): the length
    // is known where it is asked, and the `None` side is compiled away.
    let has = |bytes: Option<&[u8; 8]>| {
        let x = bytes.map_or(0, |bytes| u64::from_le_bytes(*bytes)) ^ BRACES;
        x.wrapping_sub(ONES) & !x & HIGHS
    };
    let len = key.len();
    match len {
        8..=16 => has(key.first_chunk()) | has(key.last_chunk()) != 0,
        17..=32 => {
            let (head, tail) = (&key[..16], &key[len - 16..]);
            has(head.first_chunk())
                | has(head.last_chunk())
                | has(tail.first_chunk())
                | has(tail.last_chunk())
                != 0
        }
        0..8 => {
            const ONES4: u32 = u32::from_ne_bytes([1; 4]);
            let has4 = |bytes: Option<&[u8; 4]>| {
                let x = bytes.map_or(0, |bytes| u32::from_le_bytes(*bytes)) ^ (ONES4 * b'{' as u32);
                x.wrapping_sub(ONES4) & !x & (ONES4 << 7)
            };
            if len < 4 {
                return key.contains(&b'{');
            }
            has4(key.first_chunk()) | has4(key.last_chunk()) != 0
        }
        _ => memchr::memchr(b'{', key).is_some(),
    }
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
    named_keys_of(None, name, cmd)
}

/// [`named_keys`], told how the command's keys are found when the caller
/// has it already (`arity::fits_with_shape`, from the lookup every
/// command makes anyway): the table is then not looked up again (#889).
///
/// Once keys without a tag are locked, every key the command names is,
/// from its first, tagged or not ([`untagged_named_keys`]): a key without a
/// tag before its first tagged one too. Until then that costs a command one
/// load and one branch.
#[cfg(feature = "lua")]
#[inline(always)]
pub(crate) fn named_keys_of(
    shape: Option<crate::keyspec::Shape>,
    name: &str,
    cmd: &[&[u8]],
) -> Option<NamedKeys> {
    named_keys_as(untagged_keys_locked(), shape, name, cmd)
}

/// [`named_keys_of`], told whether keys without a tag are locked.
#[cfg(feature = "lua")]
#[inline(always)]
fn named_keys_as(
    untagged: bool,
    shape: Option<crate::keyspec::Shape>,
    name: &str,
    cmd: &[&[u8]],
) -> Option<NamedKeys> {
    if untagged {
        return untagged_named_keys(shape, name, cmd);
    }
    tagged_named_keys_of(shape, name, cmd)
}

/// [`named_keys_of`] in a build without Lua, which locks no key without a
/// tag.
#[cfg(not(feature = "lua"))]
#[inline(always)]
pub(crate) fn named_keys_of(
    shape: Option<crate::keyspec::Shape>,
    name: &str,
    cmd: &[&[u8]],
) -> Option<NamedKeys> {
    tagged_named_keys_of(shape, name, cmd)
}

/// [`named_keys_of`] for keys that may carry a tag.
#[inline(always)]
fn tagged_named_keys_of(
    shape: Option<crate::keyspec::Shape>,
    name: &str,
    cmd: &[&[u8]],
) -> Option<NamedKeys> {
    // A key is an argument: a command of one argument (`GET k`, `INCR k`)
    // names that one at most, whatever the command, and the table need not
    // be asked when it has no `{`.
    let one_argument = cmd.len() == 2;
    if one_argument && !may_carry_tag(cmd[1]) {
        return None;
    }
    let regular = match shape {
        Some(shape) => crate::keyspec::regular_positions_of(shape, cmd),
        None => crate::keyspec::regular_key_positions(name, cmd),
    };
    let Some((first, last, step)) = regular else {
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

/// [`named_keys_of`] once keys without a tag are locked too: every key
/// the command names, from its first, as they are
/// ([`command_locks_untagged_keys`]). `MIGRATE` locks only its tagged keys,
/// as before (`script_may_touch` keeps a script off a key it moves).
#[cfg(feature = "lua")]
#[cold]
#[inline(never)]
fn untagged_named_keys(
    shape: Option<crate::keyspec::Shape>,
    name: &str,
    cmd: &[&[u8]],
) -> Option<NamedKeys> {
    if name == "MIGRATE" {
        return tagged_named_keys_of(shape, name, cmd);
    }
    let regular = match shape {
        Some(shape) => crate::keyspec::regular_positions_of(shape, cmd),
        None => crate::keyspec::regular_key_positions(name, cmd),
    };
    let Some((first, last, step)) = regular else {
        return Some(NamedKeys::Other(crate::keyspec::key_positions(name, cmd)));
    };
    (first <= last).then_some(NamedKeys::Regular {
        from: first,
        last,
        step,
    })
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
pub fn lock_found_keys(keys: NamedKeys, cmd: &[&[u8]]) -> Option<KeysGuard> {
    let at = match &keys {
        NamedKeys::Regular { from, last, step } => crate::keyspec::KeyIter::Range {
            next: *from,
            last: *last,
            step: *step,
        },
        NamedKeys::Other(keys) => keys.iter(),
    };
    lock_keys_as(at.map(|at| cmd[at]), command_locks_untagged_keys(cmd))
}

/// The stripes of a client's command that a script holds, or has its turn
/// for ([`script_turn`]): what the command waits for, off its thread
/// ([`command_turns`]). Lowest first, each once; and the stripes whose turns
/// it held already, if it had waited before ([`try_lock_found_keys`]).
#[cfg(feature = "lua")]
#[derive(Debug)]
pub struct TagsBusy(Vec<usize>);

/// How long [`try_lock_found_keys`] waits at a time on a tag's lock that a
/// client's command holds, before it looks again at whether a script has
/// come for the tag meanwhile.
#[cfg(feature = "lua")]
const SPELL: Duration = Duration::from_micros(50);

/// [`lock_found_keys`] for a command whose connection can wait for a script
/// without its thread (#882): the lock of each tagged key it names
/// ([`named_keys`]), or [`TagsBusy`] â€” with nothing taken or entered â€”
/// when a script holds one of them or has its turn for it. A lock a
/// client's command holds is waited for here, as in [`lock_found_keys`]: a
/// command holds it for a moment.
///
/// All or none: a lock taken is let go of before it says busy, so the
/// command holds no tag while its connection waits, and nothing waits for it
/// meanwhile. Taken lowest first, as [`lock_found_keys`] takes them. The
/// first look is a `try_lock`: what a command on a tag nobody holds costs is
/// what it cost before. One key takes nothing more than its lock: nothing
/// allocated, nothing sorted.
///
/// `mine`: the stripes whose turns the command holds ([`command_turns`]),
/// dispatched again after it waited. No script holds those, and what holds
/// them is waited for. A busy answer then names them too: the command lets
/// its turns go and waits for all of them together, lowest first, so each
/// time it waits it waits for more of its tags, and it waits once per tag
/// at most.
#[cfg(feature = "lua")]
#[inline(never)]
pub fn try_lock_found_keys(
    keys: NamedKeys,
    cmd: &[&[u8]],
    mine: &[usize],
) -> Result<Option<KeysGuard>, TagsBusy> {
    // One key, as most commands on a tagged key name (`GET {t}:k`): the
    // same steps as below for one stripe, without the walk over positions.
    if let NamedKeys::Regular { from, last, .. } = keys
        && from == last
    {
        let key = cmd[from];
        let id = match tag_of(key) {
            Some(tag) => stripe_of(tag),
            None if command_locks_untagged_keys(cmd) => STRIPES + stripe_of(key),
            None => return Ok(None),
        };
        let gate = crate::write_gate::enter();
        if try_take(id) {
            return Ok(Some(KeysGuard::one(id, gate)));
        }
        return lock_unless_a_script_has_them(Ids::one(id), gate, mine);
    }
    let untagged = command_locks_untagged_keys(cmd);
    let at = match &keys {
        NamedKeys::Regular { from, last, step } => crate::keyspec::KeyIter::Range {
            next: *from,
            last: *last,
            step: *step,
        },
        NamedKeys::Other(keys) => keys.iter(),
    };
    let mut ids = Ids::new();
    for key in at.map(|at| cmd[at]) {
        if let Some(id) = lock_id(key, untagged) {
            ids.add(id);
        }
    }
    if ids.is_empty() {
        return Ok(None);
    }
    let gate = crate::write_gate::enter();
    if let [id] = ids.as_slice()
        && try_take(*id as usize)
    {
        return Ok(Some(KeysGuard::new(ids, gate)));
    }
    ids.sorted();
    lock_unless_a_script_has_them(ids, gate, mine)
}

/// The locks `ids`, lowest first, each waited for while a client's command
/// holds it, and none of them if a script has the turn of one: it may hold
/// the lock for as long as it runs. Busy, it says which of them have a
/// script's turn, with `mine` (see [`try_lock_found_keys`]).
#[cfg(feature = "lua")]
#[cold]
#[inline(never)]
fn lock_unless_a_script_has_them(
    ids: Ids,
    gate: crate::write_gate::WriteGuard,
    mine: &[usize],
) -> Result<Option<KeysGuard>, TagsBusy> {
    let all = ids.as_slice();
    for (taken, &id) in all.iter().enumerate() {
        let id = id as usize;
        loop {
            if try_take_for(id, SPELL) {
                break;
            }
            if !mine.contains(&id) && script_has_the_turn(id) {
                for &held in &all[..taken] {
                    // SAFETY: taken above, in this loop, and not let go of.
                    unsafe { release(held as usize) }
                }
                drop(gate);
                // The others a script has come for too, so that the command
                // waits for them together: one is, at least, unless its
                // script has ended since, which waiting finds at once.
                let busy: Vec<usize> = all
                    .iter()
                    .map(|&other| other as usize)
                    .filter(|&other| {
                        other == id || mine.contains(&other) || script_has_the_turn(other)
                    })
                    .collect();
                return Err(TagsBusy(busy));
            }
        }
    }
    Ok(Some(KeysGuard::new(ids, gate)))
}

/// Whether a script holds lock `id`'s turn ([`script_turn`]), or a command
/// that waited for one does ([`command_turns`]). A script takes its turn
/// before its stripes and lets go of it after them: one that holds a stripe
/// holds the turn.
#[cfg(feature = "lua")]
fn script_has_the_turn(id: usize) -> bool {
    TURN[id].try_lock().is_err()
}

/// The turns of `busy`'s stripes, lowest first as a script takes them
/// ([`script_turn`]), for a command that found a script on its tags: it
/// waits for them here, asleep, in line with the scripts of those tags and
/// the commands that came before it, and is dispatched again holding them,
/// so that no script that came after it on those tags runs first. Only
/// those tags': a script on another tag of the command is not held up by it.
/// It takes its stripes with [`try_lock_found_keys`] then — what holds
/// those by then is a client's command, for a moment — and lets the turns
/// go ([`TagTurns`]).
#[cfg(feature = "lua")]
pub async fn command_turns(busy: &TagsBusy) -> TagTurns {
    let mut turns = Vec::with_capacity(busy.0.len());
    for &stripe in &busy.0 {
        turns.push(TURN[stripe].lock().await);
    }
    TagTurns {
        stripes: busy.0.clone(),
        _turns: turns,
    }
}

/// What [`command_turns`] holds: kept by the connection until the command
/// has taken its tags' locks, or has been answered without them.
#[cfg(feature = "lua")]
pub struct TagTurns {
    stripes: Vec<usize>,
    _turns: Vec<tokio::sync::MutexGuard<'static, ()>>,
}

#[cfg(feature = "lua")]
impl TagTurns {
    /// The stripes whose turns these are.
    pub fn stripes(&self) -> &[usize] {
        &self.stripes
    }
}

/// What active expiry holds of the locks of a batch of keys: the ones it
/// could take, and which it could not ([`try_lock_keys`]). A tag's lock is
/// held whole, as on a client's command; a key's without a tag, shared, so
/// that only a script that runs beside others under it keeps expiry out.
pub struct TriedKeys {
    held: Ids,
    /// The locks that were busy: their keys are left for another time.
    busy: Vec<usize>,
}

impl Drop for TriedKeys {
    fn drop(&mut self) {
        for &id in self.held.as_slice() {
            // SAFETY: taken once by `try_lock_keys`, and not let go of since.
            unsafe { release(id as usize) }
        }
    }
}

impl TriedKeys {
    /// Whether `key` may be taken: it has no lock, or its lock is held.
    #[inline]
    pub fn may_take(&self, key: &[u8]) -> bool {
        self.busy.is_empty()
            || lock_id(key, untagged_keys_locked()).is_none_or(|id| !self.busy.contains(&id))
    }

    /// Whether some key's lock was busy, and is to be left.
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
    /// For each lock, how many times in a row [`try_lock_keys`] found it
    /// busy on this thread.
    static MISSES: [Cell<u8>; 2 * STRIPES] = const { [const { Cell::new(0) }; 2 * STRIPES] };
}

/// The lock of each key in `keys` that has one, for active expiry, which
/// holds the write gate already: without waiting for a lock that is held, or
/// not for long. `None` when no key has one.
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
/// nothing waits on the sweep for longer than that. A key's lock without a
/// tag is taken shared: what it waits for is a script, never a read.
pub fn try_lock_keys<'a>(keys: impl Iterator<Item = &'a [u8]>) -> Option<TriedKeys> {
    let untagged = untagged_keys_locked();
    let mut ids = Ids::new();
    for key in keys {
        if let Some(id) = lock_id(key, untagged) {
            ids.add(id);
        }
    }
    if ids.is_empty() {
        return None;
    }
    // In the order of the table, as a client's command takes them.
    ids.sorted();
    let mut held = Ids::new();
    let mut busy = Vec::new();
    MISSES.with(|misses| {
        for &id in ids.as_slice() {
            let id = id as usize;
            let missed = &misses[id];
            let got = if missed.get() >= PATIENT_AFTER {
                try_take_patiently(id)
            } else {
                try_take(id)
            };
            if got {
                missed.set(0);
                held.add(id);
            } else {
                missed.set(missed.get().saturating_add(1));
                busy.push(id);
            }
        }
    });
    Some(TriedKeys { held, busy })
}

/// [`try_take`], waiting [`PATIENCE`] at most.
fn try_take_patiently(id: usize) -> bool {
    if id < STRIPES {
        return STRIPE[id]
            .0
            .try_lock_for(PATIENCE)
            .map(std::mem::forget)
            .is_some();
    }
    #[cfg(feature = "lua")]
    {
        KEY_STRIPE[id - STRIPES]
            .0
            .try_read_for(PATIENCE)
            .map(std::mem::forget)
            .is_some()
    }
    #[cfg(not(feature = "lua"))]
    {
        true
    }
}

/// Held by eviction while it deletes a key: the turn of the key's lock among
/// scripts ([`script_turn`]), if it was free (see [`try_hold_for_eviction`]).
pub struct EvictionHold(#[allow(dead_code)] Option<tokio::sync::MutexGuard<'static, ()>>);

/// For eviction, about to delete `key`: `None` if a script that runs beside
/// others holds the key's lock (its tag's, or its own once keys without a
/// tag are locked), or has its turn for it ([`script_turn`]) — the key is
/// then left, and another chosen. Otherwise the turn, held until the key
/// has gone, so that no such script starts on it meanwhile. Eviction makes
/// room from inside a write, a script's `redis.call` too, where it takes no
/// key's order: so no key a script that runs beside others holds goes from
/// under it. A client's command on the key's tag is never a reason to leave
/// a key: eviction is ordered against it as on any key ([`write_order`]).
/// With no script running, a free turn taken and let go of: the key goes as
/// it would with no locks at all. (A command that waited for a script holds
/// the turns it waited for until it has its locks: a key of one of those is
/// left too, for that moment.) Whatever `lua-parallel-scripts` says now: a
/// script that read it before a switch may still be running. Not waited
/// for: the writer that makes room holds the write gate.
///
/// [`write_order`]: crate::write_order
#[inline]
pub fn try_hold_for_eviction(key: &[u8]) -> Option<EvictionHold> {
    #[cfg(feature = "lua")]
    {
        let Some(id) = lock_id(key, untagged_keys_locked()) else {
            return Some(EvictionHold(None));
        };
        TURN[id]
            .try_lock()
            .ok()
            .map(|turn| EvictionHold(Some(turn)))
    }
    #[cfg(not(feature = "lua"))]
    {
        let _ = key;
        Some(EvictionHold(None))
    }
}

/// Commands that read or change keys they do not name: refused to a script
/// held to its tags. With tag locks on, the ones that change them take the
/// command gate alone, and the ones that only read them keep the scripts
/// that run beside others out ([`walks`]): either way, no script
/// is part-way through while one runs.
pub fn sees_the_whole_keyspace(name: &str) -> bool {
    matches!(
        name,
        "KEYS" | "SCAN" | "DBSIZE" | "RANDOMKEY" | "FLUSHALL" | "FLUSHDB" | "SWAPDB"
    )
}

/// Whether `name` is a command that reads, or may read, keys it does not
/// name: those of [`sees_the_whole_keyspace`], and the ones that read the
/// keyspace through something else — an index (`FT.*`), the labels of time
/// series (`TS.M*`, `TS.QUERY*`), a slot (`CLUSTER`), counts over keys
/// (`INFO`, `MEMORY`) or patterns (`SORT`); and two that change keys they
/// do not name, `FT.DROPINDEX … DD` and `DEBUG POPULATE`
/// ([`changes_keys_it_does_not_name`]). Every command with no key in
/// the key table, and every one whose keys there do not cover what it
/// reads, was looked at for this list. One match on the name, on the way
/// of every command with tag locks on; which of them keep scripts out
/// ([`walks`]) is asked of them only.
#[inline]
pub fn reads_keys_it_does_not_name(name: &str) -> bool {
    // A build without Lua runs no script beside others: the commands of
    // `sees_the_whole_keyspace` there, as before.
    if !cfg!(feature = "lua") {
        return sees_the_whole_keyspace(name);
    }
    matches!(
        name,
        "KEYS"
            | "SCAN"
            | "DBSIZE"
            | "RANDOMKEY"
            | "FLUSHALL"
            | "FLUSHDB"
            | "SWAPDB"
            | "INFO"
            | "MEMORY"
            | "CLUSTER"
            | "SORT"
            | "SORT_RO"
            | "FT.SEARCH"
            | "FT.AGGREGATE"
            | "FT.HYBRID"
            | "FT.CURSOR"
            | "FT.PROFILE"
            | "FT.INFO"
            | "FT.TAGVALS"
            | "FT.SPELLCHECK"
            | "TS.MGET"
            | "TS.MRANGE"
            | "TS.MREVRANGE"
            | "TS.QUERYINDEX"
            | "TS.QUERYLABELS"
            | "FT.DROPINDEX"
            | "DEBUG"
    )
}

/// Of [`reads_keys_it_does_not_name`], the commands that keep the scripts
/// that run beside others out while they read ([`walk`]), and run beside
/// every other command and each other: `cmd` finds every such script's
/// keys as they were before it or as they are after it, never between, as
/// in Redis, where a script is one step. They took the command gate alone
/// (`KEYS`, `SCAN`), which stopped every connection for as long as one ran
/// (a `KEYS` of two million keys, ~280 ms, #772).
///
/// Not `DBSIZE` and `RANDOMKEY`, which take the gate alone as before. They
/// are quick, and the gate alone also waits for what is in flight, as a
/// command in Redis comes after the whole of the one before it: active
/// expiry removing a large key is then seen whole by a `DBSIZE` that no
/// longer counts the key, its `expired_keys` and its `LATENCY` event with
/// it (Redis's `unit/latency-monitor` waits for `DBSIZE` 0 and then asks
/// for the event). Nor the flushes, `SWAPDB` and the others that write
/// ([`changes_keys_it_does_not_name`]). `SORT` only with a `BY` or `GET`
/// pattern that names keys; `INFO` only for the sections that count keys
/// (none named, `keyspace`, `default`, `all`, `everything`); `MEMORY` and
/// `CLUSTER` only for the subcommands that count or list keys.
#[cfg(feature = "lua")]
#[cold]
pub fn walks(name: &str, cmd: &[&[u8]]) -> bool {
    let sub = |names: &[&[u8]]| {
        cmd.get(1)
            .is_some_and(|sub| names.iter().any(|name| sub.eq_ignore_ascii_case(name)))
    };
    match name {
        "DBSIZE" | "RANDOMKEY" | "FLUSHALL" | "FLUSHDB" | "SWAPDB" | "FT.DROPINDEX" | "DEBUG" => {
            false
        }
        "INFO" => {
            cmd.len() == 1
                || cmd[1..].iter().any(|section| {
                    [&b"keyspace"[..], b"default", b"all", b"everything"]
                        .iter()
                        .any(|counts| section.eq_ignore_ascii_case(counts))
                })
        }
        "MEMORY" => sub(&[b"STATS", b"DOCTOR"]),
        "CLUSTER" => sub(&[b"COUNTKEYSINSLOT", b"GETKEYSINSLOT", b"SLOT-STATS"]),
        // By its patterns, which the command path asks of it itself.
        "SORT" | "SORT_RO" => false,
        _ => true,
    }
}

/// Of [`reads_keys_it_does_not_name`], besides the flushes and `SWAPDB`,
/// the commands that change keys they do not name: `FT.DROPINDEX … DD`,
/// which deletes the documents of an index, and `DEBUG POPULATE`. With tag
/// locks on, they take the command gate alone, which waits for every
/// script; and a script that runs beside others may not call them.
#[cfg(feature = "lua")]
#[cold]
pub fn changes_keys_it_does_not_name(name: &str, cmd: &[&[u8]]) -> bool {
    match name {
        "FT.DROPINDEX" => cmd
            .get(2)
            .is_some_and(|arg| arg.eq_ignore_ascii_case(b"DD")),
        "DEBUG" => cmd
            .get(1)
            .is_some_and(|sub| sub.eq_ignore_ascii_case(b"POPULATE")),
        _ => false,
    }
}

/// The locks a walk of the keyspace ([`walk`]) and the scripts that run
/// beside others ([`keep_walks_out`]) keep each other out with: one per
/// thread a script runs on, as the command gate has (`command_gate`). A
/// script holds its thread's alone, which nothing else on that thread wants
/// while it runs (a thread runs one script at a time), so what it costs is
/// one uncontended lock on a line of its own. A walk holds every one of them
/// shared, beside other walks: one call of `SCAN` or `KEYS` then finds every
/// script's keys as they were before it or as they are after it, never
/// between, as in Redis, where a script is one step. Every one, and not
/// only the slots handed out so far: a thread given its slot while a walk
/// was counting would run its script beside the walk.
#[cfg(feature = "lua")]
const WALK_SLOTS: usize = 128;

#[cfg(feature = "lua")]
#[repr(align(128))]
struct WalkSlot(parking_lot::RwLock<()>);

#[cfg(feature = "lua")]
static WALK_SLOT: [WalkSlot; WALK_SLOTS] =
    [const { WalkSlot(parking_lot::const_rwlock(())) }; WALK_SLOTS];

/// The slots handed to threads so far: the next is this, modulo their
/// number. Threads beyond that many share, and their scripts wait for each
/// other, which is correct.
#[cfg(feature = "lua")]
static WALK_SLOTS_GIVEN: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

#[cfg(feature = "lua")]
thread_local! {
    /// This thread's slot of [`WALK_SLOT`], once it has run a script beside
    /// others.
    static MY_WALK_SLOT: Cell<usize> = const { Cell::new(usize::MAX) };
}

/// Held by a script that runs beside others for as long as it runs: no walk
/// of the keyspace is under way meanwhile ([`walk`]).
#[cfg(feature = "lua")]
pub struct WalksKeptOut {
    _slot: parking_lot::RwLockWriteGuard<'static, ()>,
}

/// For a script about to run beside others: wait for the walks of the
/// keyspace under way, if any, and keep new ones out until the script is
/// done. Taken before the script's command, and so before the command
/// gate, the write gate and the tags' locks, and let go of after them, as a
/// walk takes its locks before the command gate: a script waiting here for
/// a walk holds nothing the walk, or anything the walk waits for, wants.
/// Nothing else takes it: a command that is neither a script nor a walk
/// pays nothing for it.
#[cfg(feature = "lua")]
#[inline]
pub fn keep_walks_out() -> WalksKeptOut {
    let mut slot = MY_WALK_SLOT.with(Cell::get);
    if slot == usize::MAX {
        slot = WALK_SLOTS_GIVEN.fetch_add(1, Ordering::Relaxed) % WALK_SLOTS;
        MY_WALK_SLOT.with(|mine| mine.set(slot));
    }
    WalksKeptOut {
        _slot: WALK_SLOT[slot].0.write(),
    }
}

/// Held by a walk of the keyspace ([`walks`]) for as long as
/// it runs: no script runs beside others meanwhile. A walk may end before
/// its command does ([`end_walk_here`]).
#[cfg(feature = "lua")]
pub struct Walk {
    _not_send: PhantomData<*const ()>,
}

#[cfg(feature = "lua")]
thread_local! {
    /// Whether this thread's command holds a [`Walk`]'s slots now.
    static WALKING: Cell<bool> = const { Cell::new(false) };
}

/// For a walk that has read what it reads here, and goes on to wait for
/// something else — a coordinator of a search across a cluster, which then
/// waits for the other masters' parts (`cluster::search::fan_out`): let go
/// of the walk's slots now, not when the command ends. Each of those parts
/// takes a walk on its own node; held across the wait, two nodes
/// coordinating at once could each hold a walk the other's part waits for,
/// behind a script waiting for its slot ([`keep_walks_out`]). Nothing when
/// no walk is held.
#[cfg(feature = "lua")]
pub fn end_walk_here() {
    if WALKING.with(|walking| walking.replace(false)) {
        let_go_of_walk();
    }
}

/// What [`Walk`] holds, let go of.
#[cfg(feature = "lua")]
fn let_go_of_walk() {
    for slot in &WALK_SLOT {
        // SAFETY: `walk` took every slot shared, once, and this walk has
        // not let go of them yet (`WALKING` was set).
        unsafe { slot.0.force_unlock_read() };
    }
    if WALKS.fetch_sub(1, Ordering::SeqCst) == 1 {
        WALKS_ENDED.notify_waiters();
    }
}

/// How many walks are under way: counted from before a walk takes its first
/// slot until it has let go of the last. A script about to run beside
/// others waits while there are any, off its thread ([`walks_done`]).
#[cfg(feature = "lua")]
static WALKS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// How many walks have started, ever: for a script waiting for walks to
/// tell one long walk from a stream of them ([`walks_done`]).
#[cfg(feature = "lua")]
static WALKS_STARTED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Woken when the last walk under way has ended.
#[cfg(feature = "lua")]
static WALKS_ENDED: tokio::sync::Notify = tokio::sync::Notify::const_new();

/// For a script about to run beside others, before it runs: wait, asleep,
/// while a walk of the keyspace is under way. A walk can last as long as a
/// `KEYS` over millions of keys does, and a script that waited for it in
/// [`keep_walks_out`], inside the command, would hold its connection's
/// thread, and every other connection of that thread, for as long. One load
/// when no walk is under way. A walk that starts after this has looked is
/// waited for in [`keep_walks_out`]: one walk at most.
///
/// Not for ever: a script that has seen more than [`WALK_STREAM`] walks
/// start while it waited stops waiting here and waits in [`keep_walks_out`]
/// instead, where it holds the walks that
/// come after it back from its thread's slot — a stream of `SCAN`s, each
/// starting before the last has ended, would otherwise keep it out for
/// ever. (Walks do not wait for such scripts themselves: a walk waits on its
/// thread, and the script it would wait for may be waiting on that very
/// thread's loop.)
#[cfg(feature = "lua")]
#[inline]
pub async fn walks_done() {
    if WALKS.load(Ordering::SeqCst) != 0 {
        wait_for_walks().await;
    }
}

/// How many walks may start while a script waits asleep for walks to end:
/// see [`walks_done`].
#[cfg(feature = "lua")]
const WALK_STREAM: usize = 3;

#[cfg(feature = "lua")]
#[cold]
async fn wait_for_walks() {
    let first = WALKS_STARTED.load(Ordering::SeqCst);
    loop {
        let ended = WALKS_ENDED.notified();
        tokio::pin!(ended);
        // Enabled before the count is looked at: a walk that ends after the
        // look wakes it.
        ended.as_mut().enable();
        if WALKS.load(Ordering::SeqCst) == 0
            || WALKS_STARTED.load(Ordering::SeqCst).wrapping_sub(first) > WALK_STREAM
        {
            return;
        }
        // Walks that overlap never bring the count to 0: they are counted
        // as they start, looked at every millisecond.
        tokio::select! {
            () = &mut ended => {}
            () = tokio::time::sleep(Duration::from_millis(1)) => {}
        }
    }
}

/// Wait for the scripts running beside others, and keep new ones out, for
/// as long as the walk is held. Each slot shared, lowest first: a walk and
/// the scripts it waits for never wait for each other in a cycle, for a
/// script holds one slot and waits for nothing a walk holds; and a script
/// waiting for its slot keeps the walks that come after it out of it, so a
/// stream of walks does not keep it from running.
#[cfg(feature = "lua")]
#[cold]
#[inline(never)]
pub fn walk() -> Walk {
    WALKS.fetch_add(1, Ordering::SeqCst);
    WALKS_STARTED.fetch_add(1, Ordering::SeqCst);
    // Each let go of in `Drop`, or in `end_walk_here`. Those free now are
    // taken here. Should one be held, by a script running beside others,
    // the walk waits for it, and for the rest, off the runtime: on a worker
    // of the Tokio runtime, the worker's other tasks go to another thread
    // first, the one it had queued to run next among them (tokio's LIFO
    // slot, which no other worker takes). That task would wait for the
    // script with the walk, for as long as the script runs: under load it
    // can be what takes the server's new connections, and every one of them
    // would wait too (tests/keyspace_walks.rs).
    let free = WALK_SLOT
        .iter()
        .take_while(|slot| slot.0.try_read().map(std::mem::forget).is_some())
        .count();
    if free < WALK_SLOTS {
        crate::cluster::migration::off_the_runtime(|| {
            for slot in &WALK_SLOT[free..] {
                std::mem::forget(slot.0.read());
            }
        });
    }
    WALKING.with(|walking| walking.set(true));
    Walk {
        _not_send: PhantomData,
    }
}

#[cfg(feature = "lua")]
impl Drop for Walk {
    fn drop(&mut self) {
        end_walk_here();
    }
}

// An `EXEC` and the scripts that run beside others (#882).
//
// An `EXEC` takes the command gate alone, so it waits for every script that
// runs beside others: each holds the gate shared for as long as it runs.
// Waiting for that inside the gate would hold the slots of the gate taken
// already, and the other connections of every thread whose slot it held
// would wait with it, for as long as the script ran. So an `EXEC` that
// finds such a script running does not wait there: its connection waits
// asleep for those scripts to end ([`scripts_beside_done`]), with what the
// client sent behind it waiting behind it, and dispatches it again; and
// while it waits, a script about to run beside others waits for it, asleep
// too ([`run_beside_others`]), so that a stream of scripts does not keep
// it out. What the scripts and the `EXEC` wait for here is only where they
// wait: what keeps them apart is the gate.

/// How many scripts are running beside others: counted before a script
/// takes the command gate, and until it has let go of it.
#[cfg(feature = "lua")]
static BESIDE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Woken when the last script running beside others has ended while an
/// `EXEC` waits.
#[cfg(feature = "lua")]
static BESIDE_ENDED: tokio::sync::Notify = tokio::sync::Notify::const_new();

/// How many `EXEC`s wait, asleep, for the scripts running beside others.
#[cfg(feature = "lua")]
static EXECS_WAITING: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// How many `EXEC`s have started to wait, ever: for a script waiting for
/// them to tell one from a stream of them ([`run_beside_others`]).
#[cfg(feature = "lua")]
static EXECS_STARTED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Woken when the last `EXEC` waiting for scripts has stopped waiting.
#[cfg(feature = "lua")]
static EXECS_ENDED: tokio::sync::Notify = tokio::sync::Notify::const_new();

/// How many `EXEC`s may start to wait while a script waits for them, before
/// it goes on regardless: see [`run_beside_others`].
#[cfg(feature = "lua")]
const EXEC_STREAM: usize = 3;

/// How long an `EXEC` waits for a slot of the command gate at a time,
/// inside the gate, before it looks whether a script running beside others
/// is what it waits for ([`exec_alone`]).
#[cfg(feature = "lua")]
const EXEC_SLICE: Duration = Duration::from_millis(1);

/// Held by a script that runs beside others for as long as it runs: it is
/// counted in [`BESIDE`].
#[cfg(feature = "lua")]
pub struct RunningBeside(());

#[cfg(feature = "lua")]
impl RunningBeside {
    #[inline]
    fn count() -> Self {
        BESIDE.fetch_add(1, Ordering::SeqCst);
        RunningBeside(())
    }
}

#[cfg(feature = "lua")]
impl Drop for RunningBeside {
    #[inline]
    fn drop(&mut self) {
        // Let go of, and then the `EXEC`s looked for: an `EXEC` says it
        // waits and then looks at the count, so one of the two sees the
        // other.
        if BESIDE.fetch_sub(1, Ordering::SeqCst) == 1 && EXECS_WAITING.load(Ordering::SeqCst) != 0 {
            BESIDE_ENDED.notify_waiters();
        }
    }
}

/// For a script about to run beside others, after its turns and the walks:
/// count it among those running, unless an `EXEC` waits for them to end.
/// Then it waits, asleep, until no `EXEC` waits, and is counted then. One
/// count and one load when none waits.
///
/// Not for ever: a script that has seen more than [`EXEC_STREAM`] `EXEC`s
/// start to wait while it waited goes on regardless, and an `EXEC` that
/// comes then waits for it as it waits for the others.
#[cfg(feature = "lua")]
#[inline]
pub async fn run_beside_others() -> RunningBeside {
    let running = RunningBeside::count();
    if EXECS_WAITING.load(Ordering::SeqCst) == 0 {
        return running;
    }
    wait_for_execs(running).await
}

#[cfg(feature = "lua")]
#[cold]
async fn wait_for_execs(running: RunningBeside) -> RunningBeside {
    let first = EXECS_STARTED.load(Ordering::SeqCst);
    let stream = || EXECS_STARTED.load(Ordering::SeqCst).wrapping_sub(first) > EXEC_STREAM;
    // Not counted while it waits: the `EXEC`s wait for the count.
    drop(running);
    loop {
        if stream() {
            break;
        }
        let ended = EXECS_ENDED.notified();
        tokio::pin!(ended);
        // Enabled before the look: an `EXEC` that stops waiting after it
        // wakes this one.
        ended.as_mut().enable();
        if EXECS_WAITING.load(Ordering::SeqCst) != 0 {
            // Looked at again without counting itself: a count let go of
            // at every wake would wake the waiting `EXEC`s for nothing, and
            // one that came in between would wait again.
            tokio::select! {
                () = &mut ended => {}
                () = tokio::time::sleep(Duration::from_millis(20)) => {}
            }
            continue;
        }
        // None waits: counted, and then looked at again, so that an `EXEC`
        // that starts to wait in between sees the count.
        let running = RunningBeside::count();
        if EXECS_WAITING.load(Ordering::SeqCst) == 0 {
            return running;
        }
        drop(running);
    }
    RunningBeside::count()
}

/// Held by an `EXEC` that waits for the scripts running beside others, from
/// its first look until it has run: counted in [`EXECS_WAITING`], which
/// holds back the scripts that come meanwhile ([`run_beside_others`]).
#[cfg(feature = "lua")]
pub struct ExecWaits(());

#[cfg(feature = "lua")]
impl ExecWaits {
    fn begin() -> Self {
        EXECS_STARTED.fetch_add(1, Ordering::SeqCst);
        EXECS_WAITING.fetch_add(1, Ordering::SeqCst);
        ExecWaits(())
    }
}

#[cfg(feature = "lua")]
impl Drop for ExecWaits {
    fn drop(&mut self) {
        if EXECS_WAITING.fetch_sub(1, Ordering::SeqCst) == 1 {
            EXECS_ENDED.notify_waiters();
        }
    }
}

/// The command gate alone, for an `EXEC`: taken at once, waiting only for
/// the commands in flight, when no script runs beside others. Otherwise
/// nothing is held, and the `EXEC` is to wait for them asleep
/// ([`scripts_beside_done`]) holding the [`ExecWaits`] this returns, and
/// to ask again with it; on success the `ExecWaits`, if any, is to be held
/// until the `EXEC` has run. A script that starts as the `EXEC` takes the
/// gate makes it let go of what it has taken within [`EXEC_SLICE`], and
/// wait asleep.
///
/// With tag locks off, the gate alone, as it is taken there.
#[cfg(feature = "lua")]
pub fn exec_alone(
    waits: Option<ExecWaits>,
) -> Result<(crate::command_gate::AloneGuard, Option<ExecWaits>), ExecWaits> {
    if !enabled() && waits.is_none() {
        return Ok((crate::command_gate::enter_alone(), None));
    }
    if BESIDE.load(Ordering::SeqCst) == 0
        && let Some(alone) = crate::command_gate::enter_alone_unless(EXEC_SLICE, || {
            BESIDE.load(Ordering::SeqCst) != 0
        })
    {
        return Ok((alone, waits));
    }
    Err(waits.unwrap_or_else(ExecWaits::begin))
}

/// For an `EXEC` that found scripts running beside others
/// ([`exec_alone`]): wait, asleep, until none does. New ones wait for the
/// `EXEC` meanwhile ([`run_beside_others`]), so the count comes to 0.
#[cfg(feature = "lua")]
pub async fn scripts_beside_done(_waits: &ExecWaits) {
    loop {
        let ended = BESIDE_ENDED.notified();
        tokio::pin!(ended);
        // Enabled before the look: the last script that ends after it
        // wakes this.
        ended.as_mut().enable();
        if BESIDE.load(Ordering::SeqCst) == 0 {
            return;
        }
        tokio::select! {
            () = &mut ended => {}
            () = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
    }
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

    /// In a build without Lua, keys without a tag are never locked, whatever
    /// `lua-parallel-scripts` says.
    #[cfg(not(feature = "lua"))]
    #[test]
    fn without_lua_keys_without_a_tag_are_never_locked() {
        set_enabled(true);
        assert!(!untagged_keys_locked());
        assert!(lock_keys([b"plain".as_slice()].into_iter()).is_none());
        assert!(try_hold_for_eviction(b"plain").is_some_and(|hold| hold.0.is_none()));
        set_enabled(false);
    }

    /// Once keys without a tag are locked, the keys a client's command
    /// locks ([`named_keys_of`]'s dispatch) are every key it names, as the
    /// test spells them, tagged or not, wherever its first tagged key is: for every way commands name their keys (one key,
    /// several, at steps, counted, a `STORE` destination, more than fit
    /// inline), with no tag, a tag first, a tag later and a tag only.
    #[cfg(feature = "lua")]
    #[test]
    fn with_keys_locked_every_key_a_command_names_is_locked() {
        let shapes: &[&[&str]] = &[
            &["GET", "A"],
            &["SET", "A", "v"],
            &["INCR", "A"],
            &["MGET", "A", "B", "C"],
            &["MSET", "A", "1", "B", "2", "C", "3"],
            &["MSETNX", "A", "1", "B", "2"],
            &["DEL", "A", "B", "C"],
            &["EXISTS", "A", "B"],
            &["RENAME", "A", "B"],
            &["COPY", "A", "B"],
            &["SMOVE", "A", "B", "m"],
            &["LMOVE", "A", "B", "LEFT", "RIGHT"],
            &["BLMOVE", "A", "B", "LEFT", "RIGHT", "0"],
            &["SINTERSTORE", "A", "B", "C"],
            &["SINTERCARD", "2", "A", "B"],
            &["ZUNIONSTORE", "A", "2", "B", "C"],
            &["ZINTER", "2", "A", "B"],
            &["LMPOP", "2", "A", "B", "LEFT"],
            &["BLMPOP", "0", "2", "A", "B", "LEFT"],
            &["ZMPOP", "2", "A", "B", "MIN"],
            &["XREAD", "STREAMS", "A", "B", "0", "0"],
            &["SORT", "A", "STORE", "B"],
            &["GEORADIUS", "A", "0", "0", "1", "km", "STORE", "B"],
            &["LCS", "A", "B"],
            &["PFMERGE", "A", "B", "C"],
            &["BITOP", "AND", "A", "B", "C"],
            &["OBJECT", "ENCODING", "A"],
        ];
        // The keys of a shape: none tagged, the first, the last, all.
        let spellings = |at: usize, keys: usize| -> [String; 4] {
            let plain = format!("k{at}");
            let tagged = format!("{{t}}:k{at}");
            [
                plain.clone(),
                if at == 0 {
                    tagged.clone()
                } else {
                    plain.clone()
                },
                if at + 1 == keys {
                    tagged.clone()
                } else {
                    plain
                },
                tagged,
            ]
        };
        let mut many = vec!["MGET".to_string()];
        many.extend((0..20).map(|at| {
            if at == 19 {
                "{t}:last".to_string()
            } else {
                format!("m{at}")
            }
        }));
        let many: Vec<&str> = many.iter().map(String::as_str).collect();
        // Each command with the keys it names, as spelled here: what is
        // expected does not come from the key table.
        let mut all: Vec<(Vec<String>, std::collections::BTreeSet<String>)> = Vec::new();
        for shape in shapes {
            let keys = shape
                .iter()
                .filter(|word| word.len() == 1 && word.as_bytes()[0].is_ascii_uppercase())
                .count();
            for way in 0..4 {
                let mut at = 0;
                let mut named = std::collections::BTreeSet::new();
                let words = shape
                    .iter()
                    .map(|word| {
                        if word.len() == 1 && word.as_bytes()[0].is_ascii_uppercase() {
                            at += 1;
                            let key = spellings(at - 1, keys)[way].clone();
                            named.insert(key.clone());
                            key
                        } else {
                            word.to_string()
                        }
                    })
                    .collect();
                all.push((words, named));
            }
        }
        all.push((
            many.iter().map(|word| word.to_string()).collect(),
            many[1..].iter().map(|word| word.to_string()).collect(),
        ));
        for (words, named) in &all {
            let cmd: Vec<&[u8]> = words.iter().map(|word| word.as_bytes()).collect();
            let name = words[0].to_ascii_uppercase();
            let named: std::collections::BTreeSet<&[u8]> =
                named.iter().map(|key| key.as_bytes()).collect();
            // Through the dispatch a client's command takes, with keys
            // without a tag locked.
            let found = named_keys_as(true, None, &name, &cmd).expect("keys found");
            let at = match &found {
                NamedKeys::Regular { from, last, step } => crate::keyspec::KeyIter::Range {
                    next: *from,
                    last: *last,
                    step: *step,
                },
                NamedKeys::Other(keys) => keys.iter(),
            };
            let locked: std::collections::BTreeSet<&[u8]> = at.map(|at| cmd[at]).collect();
            assert_eq!(locked, named, "{words:?}");
            // And each of those has a lock: its tag's or its own.
            for key in &locked {
                assert!(
                    lock_id(key, true).is_some(),
                    "{words:?}: {:?}",
                    String::from_utf8_lossy(key)
                );
            }
        }
    }

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

    /// #772: walks of the keyspace run beside each other, and not beside a
    /// script that runs beside others; a script waits for the walks under
    /// way, and a walk for the scripts. A walk that comes after a script
    /// waiting for one does not go ahead of it.
    #[cfg(feature = "lua")]
    #[test]
    fn walks_and_scripts_keep_each_other_out() {
        use std::sync::mpsc::channel;
        let first = walk();
        // Another walk beside it, on another thread.
        std::thread::scope(|scope| {
            scope.spawn(|| drop(walk())).join().unwrap();
        });
        // A script waits for it.
        let (ran_tx, ran_rx) = channel();
        let script = std::thread::spawn(move || {
            let held = keep_walks_out();
            ran_tx.send(()).unwrap();
            std::thread::sleep(Duration::from_millis(50));
            drop(held);
        });
        assert!(ran_rx.recv_timeout(Duration::from_millis(100)).is_err());
        // A walk that comes now waits behind the script.
        let (walked_tx, walked_rx) = channel();
        let later = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            let _walk = walk();
            walked_tx.send(std::time::Instant::now()).unwrap();
        });
        std::thread::sleep(Duration::from_millis(60));
        drop(first);
        ran_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the script ran once the walk was done");
        let script_started = std::time::Instant::now();
        let walked = walked_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the later walk ran");
        assert!(
            walked >= script_started + Duration::from_millis(30),
            "the later walk went ahead of the script that waited before it"
        );
        script.join().unwrap();
        later.join().unwrap();
        // Scripts of different threads run beside each other.
        std::thread::scope(|scope| {
            let one = keep_walks_out();
            scope.spawn(|| drop(keep_walks_out())).join().unwrap();
            drop(one);
        });
    }

    /// #772: a script waits for the walks under way off its thread
    /// (`walks_done`), and runs once they have ended.
    #[cfg(feature = "lua")]
    #[test]
    fn a_script_waits_for_walks_asleep() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        // No walk: at once.
        runtime.block_on(walks_done());
        let first = walk();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let script = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(walks_done());
            // Keeps walks out from here, as a script does in its command.
            let kept = keep_walks_out();
            done_tx.send(std::time::Instant::now()).unwrap();
            std::thread::sleep(Duration::from_millis(50));
            drop(kept);
        });
        assert!(done_rx.recv_timeout(Duration::from_millis(100)).is_err());
        let ended = std::time::Instant::now();
        drop(first);
        assert!(done_rx.recv_timeout(Duration::from_secs(5)).unwrap() >= ended);
        script.join().unwrap();
    }

    /// #772: a script woken by a walk ending, and finding another under
    /// way each time, stops waiting asleep after a few and waits for its
    /// thread's slot instead, which holds the later walks back: a stream of
    /// overlapping walks does not keep it out.
    #[cfg(feature = "lua")]
    #[test]
    fn a_stream_of_walks_does_not_keep_a_script_out() {
        let stop = std::sync::atomic::AtomicBool::new(false);
        std::thread::scope(|scope| {
            // Two walkers, each starting before the other has ended.
            for _ in 0..2 {
                scope.spawn(|| {
                    while !stop.load(Ordering::Relaxed) {
                        let _walk = walk();
                        std::thread::sleep(Duration::from_millis(2));
                    }
                });
            }
            let ran = scope.spawn(|| {
                std::thread::sleep(Duration::from_millis(20));
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                let started = std::time::Instant::now();
                runtime.block_on(walks_done());
                drop(keep_walks_out());
                started.elapsed()
            });
            let took = ran.join().unwrap();
            stop.store(true, Ordering::Relaxed);
            assert!(took < Duration::from_secs(2), "{took:?}");
        });
    }

    /// #882: an `EXEC` that finds a script running beside others takes
    /// nothing and waits for it asleep; a script that comes meanwhile waits
    /// for the `EXEC`, asleep too, and runs once it has run. A stream of
    /// `EXEC`s does not keep a script out. (One test: the counts are the
    /// process's.)
    #[cfg(feature = "lua")]
    #[test]
    fn an_exec_and_the_scripts_beside_others_wait_for_each_other_asleep() {
        use std::sync::mpsc::channel;
        use std::time::Instant;
        let runtime = || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
        };
        let running = runtime().block_on(run_beside_others());
        let Err(waits) = exec_alone(Some(ExecWaits::begin())) else {
            panic!("an EXEC took the gate beside a script running beside others")
        };
        assert!(!crate::command_gate::alone_on_this_thread());
        // A script that comes now waits for the EXEC.
        let (ran_tx, ran_rx) = channel();
        let later = std::thread::spawn(move || {
            let counted = runtime().block_on(run_beside_others());
            ran_tx.send(Instant::now()).unwrap();
            drop(counted);
        });
        assert!(ran_rx.recv_timeout(Duration::from_millis(100)).is_err());
        let exec = std::thread::spawn(move || {
            runtime().block_on(scripts_beside_done(&waits));
            let Ok((alone, waits)) = exec_alone(Some(waits)) else {
                panic!("no script runs beside others, and the EXEC was turned away")
            };
            std::thread::sleep(Duration::from_millis(30));
            let done = Instant::now();
            drop(alone);
            drop(waits);
            done
        });
        std::thread::sleep(Duration::from_millis(50));
        assert!(!exec.is_finished(), "the EXEC ran beside a running script");
        drop(running);
        let exec_done = exec.join().unwrap();
        let later_ran = ran_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(
            later_ran >= exec_done,
            "a script ran before the EXEC it came after"
        );
        later.join().unwrap();

        // A stream of EXECs, each waiting before the last has stopped.
        let stop = std::sync::atomic::AtomicBool::new(false);
        std::thread::scope(|scope| {
            for _ in 0..2 {
                scope.spawn(|| {
                    while !stop.load(Ordering::Relaxed) {
                        let waits = ExecWaits::begin();
                        std::thread::sleep(Duration::from_millis(2));
                        drop(waits);
                    }
                });
            }
            std::thread::sleep(Duration::from_millis(20));
            let started = Instant::now();
            drop(runtime().block_on(run_beside_others()));
            stop.store(true, Ordering::Relaxed);
            assert!(started.elapsed() < Duration::from_secs(2));
        });
    }

    /// #889: `may_carry_tag` says whether a key has a `{` for every length
    /// up to past `memchr`'s, wherever the `{` is, and not for bytes next to
    /// it in value (`z`, `|`, `{` + 128).
    #[test]
    fn may_carry_tag_finds_a_brace_at_any_place() {
        for len in 0..80 {
            for filler in [b'a', b'z', b'|', b'{' | 0x80, 0x01, 0xff] {
                let key = vec![filler; len];
                assert!(!may_carry_tag(&key), "len {len} filler {filler}");
                for at in 0..len {
                    let mut key = key.clone();
                    key[at] = b'{';
                    assert!(may_carry_tag(&key), "len {len} at {at} filler {filler}");
                }
            }
        }
    }

    /// #889: the shape the arity lookup carries is `keyspec`'s, for every
    /// name it has, so `named_keys_of` with it finds what `named_keys`
    /// finds.
    #[cfg(feature = "lua")]
    #[test]
    fn the_shape_from_the_arity_lookup_is_keyspecs() {
        let tagged: &[&[u8]] = &[b"{t}:a", b"{t}:b", b"{t}:c", b"{t}:d"];
        for name in crate::keyspec::table_names() {
            for len in 2..=5 {
                let mut cmd: Vec<&[u8]> = vec![name.as_bytes()];
                cmd.extend_from_slice(&tagged[..len - 1]);
                let shape = crate::commands::arity::fits_with_shape(name, &cmd).flatten();
                if let Some(shape) = shape {
                    assert_eq!(shape, crate::keyspec::shape_of(name), "{name}");
                }
                let of = format!("{:?}", named_keys_of(shape, name, &cmd));
                let plain = format!("{:?}", named_keys(name, &cmd));
                assert_eq!(of, plain, "{name} with {} arguments", len - 1);
            }
        }
    }

    /// `try_lock_found_keys` over the keys `named_keys` finds, as the command
    /// path asks it.
    #[cfg(feature = "lua")]
    fn try_lock_named(name: &str, cmd: &[&[u8]]) -> Result<Option<KeysGuard>, TagsBusy> {
        match named_keys(name, cmd) {
            None => Ok(None),
            Some(keys) => try_lock_found_keys(keys, cmd, &[]),
        }
    }

    /// #882: what `try_lock_found_keys` takes. A tag a script holds is busy,
    /// and nothing at all is taken then — not the other tags the command
    /// names, nor the write gate; a tag nobody holds is taken; a key with no
    /// tag takes nothing. Once the script is done, all are taken.
    #[cfg(feature = "lua")]
    #[test]
    fn a_command_finds_a_scripts_tag_busy_and_takes_nothing() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let held = |tag: &str| STRIPE[stripe_of(tag.as_bytes())].0.is_locked();
        let script_tag = "tw882-script";
        let other = (0..)
            .map(|i| format!("tw882-other-{i}"))
            .find(|tag| stripe_of(tag.as_bytes()) != stripe_of(script_tag.as_bytes()))
            .unwrap();
        let tags = ScriptTags::One(script_tag.as_bytes());
        let turn = runtime.block_on(script_turn(&tags));
        let script = lock_for_script(&tags);
        let both = [
            b"MGET".to_vec(),
            format!("{{{other}}}:k").into_bytes(),
            format!("{{{script_tag}}}:k").into_bytes(),
        ];
        let both: Vec<&[u8]> = both.iter().map(Vec::as_slice).collect();
        let other_key = format!("{{{other}}}:k");
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let busy = try_lock_named("MGET", &both).err().expect("busy");
                    assert_eq!(
                        busy.0,
                        vec![stripe_of(script_tag.as_bytes())],
                        "the script's stripe only"
                    );
                    assert!(!held(&other), "the free tag was kept");
                    assert!(!crate::write_gate::held_by_this_thread());
                    let one = try_lock_named("GET", &[b"GET", other_key.as_bytes()]).expect("free");
                    assert!(one.is_some() && held(&other));
                    drop(one);
                    assert!(matches!(
                        try_lock_named("GET", &[b"GET", b"plain"]),
                        Ok(None)
                    ));
                })
                .join()
                .unwrap();
        });
        drop(script);
        drop(turn);
        let all = try_lock_named("MGET", &both).expect("all free");
        assert!(all.is_some() && held(&other) && held(script_tag));
    }

    /// #882: a tag a client's command holds — no script's turn — is waited
    /// for, as `lock_keys` waits, and taken once it is let go of.
    #[cfg(feature = "lua")]
    #[test]
    fn a_command_waits_a_moment_for_another_commands_tag() {
        let key = b"{tw882-command}:k".as_slice();
        let first = lock_keys([key].into_iter());
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let waiter = std::thread::spawn(move || {
            let taken = try_lock_named("GET", &[b"GET", key]);
            done_tx.send(matches!(taken, Ok(Some(_)))).unwrap();
        });
        assert!(done_rx.recv_timeout(Duration::from_millis(50)).is_err());
        drop(first);
        assert!(done_rx.recv_timeout(Duration::from_secs(5)).unwrap());
        waiter.join().unwrap();
    }

    /// #882: a command that finds a script on one of its tags waits for that
    /// tag's turn only: a script on another of its tags is not held up by it.
    /// Dispatched again holding that turn, and finding a script on the other
    /// tag now, it is told to wait for both.
    #[cfg(feature = "lua")]
    #[test]
    fn a_waiting_command_holds_only_the_busy_tags_turns() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let a = "tw882-busy-a";
        let b = (0..)
            .map(|i| format!("tw882-busy-b-{i}"))
            .find(|tag| stripe_of(tag.as_bytes()) != stripe_of(a.as_bytes()))
            .unwrap();
        let mget = [
            b"MGET".to_vec(),
            format!("{{{a}}}:k").into_bytes(),
            format!("{{{b}}}:k").into_bytes(),
        ];
        let mget: Vec<&[u8]> = mget.iter().map(Vec::as_slice).collect();
        let on_another_thread = |mine: &[usize]| {
            std::thread::scope(|scope| {
                scope
                    .spawn(|| {
                        let keys = named_keys("MGET", &mget).expect("tagged");
                        try_lock_found_keys(keys, &mget, mine).map(|held| held.is_some())
                    })
                    .join()
                    .unwrap()
            })
        };
        runtime.block_on(async {
            let tags_a = ScriptTags::One(a.as_bytes());
            let turn_a = script_turn(&tags_a).await;
            let script_a = lock_for_script(&tags_a);
            let busy = on_another_thread(&[]).expect_err("busy");
            assert_eq!(busy.0, vec![stripe_of(a.as_bytes())]);
            drop(script_a);
            drop(turn_a);
            let turns = command_turns(&busy).await;
            let tags_b = ScriptTags::One(b.as_bytes());
            let turn_b = tokio::time::timeout(Duration::from_millis(200), script_turn(&tags_b))
                .await
                .expect("a script on the other tag was held up");
            let script_b = lock_for_script(&tags_b);
            let again = on_another_thread(turns.stripes()).expect_err("busy");
            let mut both = vec![stripe_of(a.as_bytes()), stripe_of(b.as_bytes())];
            both.sort_unstable();
            assert_eq!(again.0, both);
            drop(script_b);
            drop(turn_b);
            // And with nothing on either, it takes both.
            assert_eq!(on_another_thread(turns.stripes()).ok(), Some(true));
            drop(turns);
        });
    }

    /// #882: a command that found a script waits for the tag's turn in line
    /// with scripts: after the script that holds it, before one that comes
    /// after it.
    #[cfg(feature = "lua")]
    #[test]
    fn a_waiting_command_is_in_line_with_scripts() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let tag = b"tw882-line".as_slice();
        let tags = ScriptTags::One(tag);
        runtime.block_on(async {
            let running = script_turn(&tags).await;
            let busy = TagsBusy(vec![stripe_of(tag)]);
            let order = std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
            let command = tokio::spawn({
                let order = order.clone();
                async move {
                    let turns = command_turns(&busy).await;
                    order.lock().push("command");
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    drop(turns);
                }
            });
            tokio::time::sleep(Duration::from_millis(20)).await;
            let later = tokio::spawn({
                let order = order.clone();
                async move {
                    let _turn = script_turn(&ScriptTags::One(b"tw882-line")).await;
                    order.lock().push("later script");
                }
            });
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert!(
                order.lock().is_empty(),
                "nothing goes before the running script"
            );
            drop(running);
            command.await.unwrap();
            later.await.unwrap();
            assert_eq!(*order.lock(), vec!["command", "later script"]);
        });
    }
}
