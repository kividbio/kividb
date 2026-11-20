//! Logical clock frozen for the duration of an execution unit, as Redis's
//! `commandTimeSnapshot`: a Lua script, an `FCALL`, or a transaction's
//! `EXEC` (#659). TTL math, the expiry of keys and hash fields, and `TIME`
//! use the snapshot taken when the unit starts. See [`freeze`].
//!
//! `TIME` stops holding it once a script runs past `busy-reply-threshold`
//! (#676). See [`let_time_move`].
//!
//! Every other command reads the clock once too (#632): the first time it
//! asks, and that time holds until it ends. A key is live for the whole
//! command or expired for the whole of it. See [`command`].

use std::cell::RefCell;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// [`COMMAND_MS`] in a command that has not read the clock yet.
const PENDING: u64 = u64::MAX;

thread_local! {
    /// The time of the command running on this thread ([`command`]), in
    /// milliseconds: [`PENDING`] until it first reads the clock, 0 outside
    /// a command, where every reading is the clock's own. A unit's time
    /// ([`FROZEN_MS`]) comes first.
    static COMMAND_MS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(debug_assertions)]
thread_local! {
    /// Debug builds only: milliseconds added to the clock on this thread
    /// until the command running on it ends. A test's way to move the clock
    /// between two expiry checks of one command (see
    /// `handler::expire_named_keys_first`).
    static TEST_CLOCK_AHEAD_MS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Debug builds only: moves this thread's clock `ms` ahead until the
/// command running on it ends ([`command`]).
#[cfg(debug_assertions)]
pub fn test_move_clock_ahead(ms: u64) {
    TEST_CLOCK_AHEAD_MS.with(|a| a.set(ms));
}

thread_local! {
    /// How deep this thread is in execution units, and the frozen time
    /// (Unix seconds and microseconds) while it is in one.
    static SCRIPT_STATE: RefCell<(u32, Option<(u64, u32)>)> =
        const { RefCell::new((0, None)) };
    /// The script's frozen time in milliseconds, as [`ttl_wall_ms_now`]
    /// gives it; 0 outside a script. What [`wall_ms_now`] reads, without
    /// borrowing `SCRIPT_STATE`.
    static FROZEN_MS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Whether `TIME` reads the wall clock while the rest stays frozen:
    /// set by [`let_time_move`] until the outermost unit ends.
    static TIME_MOVES: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Used only when `--features lua` (EVAL/FCALL); harmless no-op visibility otherwise.
#[allow(dead_code)]
#[inline]
pub fn enter_script_execution() {
    SCRIPT_STATE.with(|st| {
        let mut st = st.borrow_mut();
        if st.0 == 0 {
            let wall = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default();
            st.1 = Some((wall.as_secs(), wall.subsec_micros()));
            let ms = wall
                .as_secs()
                .saturating_mul(1000)
                .saturating_add(u64::from(wall.subsec_millis()))
                .max(1);
            // Inside a command (`EVAL`, `FCALL`): the time the command read
            // when it looked at the keys it names holds for its script too;
            // and a command that had not read the clock yet keeps the
            // script's time for the rest of it.
            let ms = COMMAND_MS.with(|c| match c.get() {
                0 => ms,
                PENDING => {
                    c.set(ms);
                    ms
                }
                command => command,
            });
            FROZEN_MS.with(|f| f.set(ms));
        }
        st.0 += 1;
    });
}

#[allow(dead_code)]
#[inline]
pub fn exit_script_execution() {
    SCRIPT_STATE.with(|st| {
        let mut st = st.borrow_mut();
        st.0 = st.0.saturating_sub(1);
        if st.0 == 0 {
            st.1 = None;
            FROZEN_MS.with(|f| f.set(0));
            TIME_MOVES.with(|t| t.set(false));
        }
    });
}

/// The clock held at one time on this thread for as long as the guard
/// lives: what a script, a function and a transaction's `EXEC` run under.
/// Every key and hash field in the unit is there, or gone, for all of its
/// commands at once, as in Redis, where `call()` takes `cmd_time_snapshot`
/// at the outermost level only (`enterExecutionUnit`) and every expiry
/// check, TTL reply and `TIME` reads it.
///
/// Before #659 only scripts froze the clock, and an `EXEC` read it for each
/// queued command: a key or field whose deadline fell inside the
/// transaction was found by its first commands and not by its later ones.
///
/// Units nest: one entered inside another (a script run by `EXEC`) keeps
/// the outer one's time. The guard is not `Send`: the time is this
/// thread's, so it cannot be held across an `.await` that could move the
/// task to another thread.
#[must_use = "the clock is frozen only while the guard lives"]
#[inline]
pub fn freeze() -> Frozen {
    enter_script_execution();
    Frozen {
        _this_thread: std::marker::PhantomData,
    }
}

/// See [`freeze`].
pub struct Frozen {
    _this_thread: std::marker::PhantomData<*const ()>,
}

impl Drop for Frozen {
    #[inline]
    fn drop(&mut self) {
        exit_script_execution();
    }
}

/// One command's time, as Redis's `commandTimeSnapshot` (#632): the clock
/// is read the first time the command asks for it ([`wall_ms_now`]), and
/// that reading answers every later ask until the guard is dropped. Every
/// expiry check the command makes (the look for keys past their time
/// before a write, `handler::expire_named_keys_first`, then its own
/// lookups and those of the shards it locks, its hash fields, the times to
/// live it sets and reports) goes by the same time, so a key is live for
/// the whole command or expired for the whole of it.
///
/// Read lazily: a command that looks at no key with a time to live reads no
/// clock. Taken by `handler::run_command` once the command holds its gates,
/// and after any room made for it under `maxmemory`, which goes by the
/// clock as the background expiry and eviction do. Inside a unit
/// ([`freeze`]: a script, an `EXEC`) the unit's time comes first. Commands
/// do not nest outside a unit: a script's calls and a transaction's queued
/// commands run inside one.
///
/// Two plain thread-local stores and nothing kept on the stack: this is on
/// every command's path.
#[must_use = "the command's time holds only while the guard lives"]
#[inline(always)]
pub fn command() -> Command {
    COMMAND_MS.with(|c| c.set(PENDING));
    Command {
        _this_thread: std::marker::PhantomData,
    }
}

/// See [`command`]. Not `Send`, as [`Frozen`] is not.
pub struct Command {
    _this_thread: std::marker::PhantomData<*const ()>,
}

impl Drop for Command {
    #[inline(always)]
    fn drop(&mut self) {
        COMMAND_MS.with(|c| c.set(0));
        #[cfg(debug_assertions)]
        TEST_CLOCK_AHEAD_MS.with(|a| a.set(0));
    }
}

/// Milliseconds since UNIX epoch, as [`wall_ms_now`] gives them.
#[allow(dead_code)]
#[inline]
pub fn ttl_wall_ms_now() -> u128 {
    u128::from(wall_ms_now())
}

/// [`ttl_wall_ms_now`] as a `u64`: what a key's entry keeps a time to live
/// as, read on every write lock that looks at a key with one, and by every
/// lazy expiry check. One thread-local load, and outside a script the
/// clock read straight into milliseconds: through `SystemTime`, a
/// `RefCell` borrow and `duration_since` it cost about 75 instructions.
///
/// In a unit ([`freeze`]), the unit's time; in a command ([`command`]),
/// the command's; elsewhere (the background expiry, eviction, timers) the
/// clock's own.
#[inline]
pub fn wall_ms_now() -> u64 {
    let frozen = FROZEN_MS.with(|f| f.get());
    if frozen != 0 {
        return frozen;
    }
    match COMMAND_MS.with(|c| c.get()) {
        0 => clock_ms(),
        PENDING => first_reading_of_the_command(),
        command => command,
    }
}

/// The clock, read for the command running on this thread and kept for the
/// rest of it ([`command`]).
#[inline]
fn first_reading_of_the_command() -> u64 {
    let now = clock_ms().max(1);
    COMMAND_MS.with(|c| c.set(now));
    now
}

/// The wall clock, in whole milliseconds since the Unix epoch (0 before it).
#[cfg(not(debug_assertions))]
#[inline(always)]
fn clock_ms() -> u64 {
    os_clock_ms()
}

/// The wall clock, and a test's move of it ([`test_move_clock_ahead`]).
#[cfg(debug_assertions)]
#[inline]
fn clock_ms() -> u64 {
    os_clock_ms().saturating_add(TEST_CLOCK_AHEAD_MS.with(|a| a.get()))
}

#[cfg(unix)]
#[inline]
fn os_clock_ms() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is a valid place for the call to write the time to.
    unsafe { libc::clock_gettime(libc::CLOCK_REALTIME, &mut ts) };
    if ts.tv_sec < 0 {
        return 0;
    }
    (ts.tv_sec as u64)
        .saturating_mul(1000)
        .saturating_add(ts.tv_nsec as u64 / 1_000_000)
}

#[cfg(not(unix))]
#[inline]
fn os_clock_ms() -> u64 {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    d.as_secs()
        .saturating_mul(1000)
        .saturating_add(u64::from(d.subsec_millis()))
}

/// The time by which a time to live is set, told and ended: the wall clock
/// in whole milliseconds, and during a script the moment the script began.
///
/// Whole milliseconds, as Redis's clock for this is, because a key's time
/// to live is a number of them and everything that handles one must agree
/// on where a millisecond starts. With the fraction kept, a key set to live
/// 250 ms at 10.9 ms was set (by a clock that dropped the fraction) to end
/// at 260, was said a moment later to have 249 left, and was gone at 260.0,
/// 249.1 ms after it was set. A client that waits as long as it is told
/// (BullMQ's rate limiter does) came back a millisecond early each time.
#[inline]
pub fn wall_system_time_now() -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ttl_wall_ms_now() as u64)
}

/// Lets `TIME` read the wall clock for the rest of this thread's unit,
/// while expiry stays at the unit's start: a script past
/// `busy-reply-threshold`. Redis refreshes its cached time there
/// (`scriptInterrupt` calls `processEventsWhileBlocked`, which calls
/// `updateCachedTime`), and `TIME` reads that time, but expiry reads
/// `cmd_time_snapshot`, which stays where the script began. Without it a
/// script waiting for `TIME` to move never ended.
#[allow(dead_code)]
#[inline]
pub fn let_time_move() {
    TIME_MOVES.with(|t| t.set(true));
}

/// `TIME` command: `(seconds, microseconds_within_second)`.
#[inline]
pub fn time_command_wall_tuple() -> (u64, u32) {
    SCRIPT_STATE.with(|s| {
        if let Some((sec, us)) = s.borrow().1
            && !TIME_MOVES.with(|t| t.get())
        {
            return (sec, us);
        }
        let d = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        (d.as_secs(), d.subsec_micros())
    })
}

#[cfg(test)]
mod wall_ms_tests {
    use super::*;

    #[test]
    fn the_fast_clock_agrees_with_the_slow_one() {
        let before = ttl_wall_ms_now() as u64;
        let fast = wall_ms_now();
        let after = ttl_wall_ms_now() as u64;
        assert!(before <= fast && fast <= after, "{before} {fast} {after}");
    }

    #[test]
    fn a_script_sees_its_frozen_time() {
        enter_script_execution();
        let frozen = ttl_wall_ms_now() as u64;
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(wall_ms_now(), frozen);
        enter_script_execution();
        assert_eq!(wall_ms_now(), frozen);
        exit_script_execution();
        assert_eq!(wall_ms_now(), frozen);
        exit_script_execution();
        std::thread::sleep(Duration::from_millis(2));
        assert!(wall_ms_now() > frozen);
    }

    #[test]
    fn a_frozen_unit_keeps_one_time_and_a_nested_one_keeps_the_outer_time() {
        let outer = freeze();
        let frozen = wall_ms_now();
        let time = time_command_wall_tuple();
        std::thread::sleep(Duration::from_millis(5));
        {
            // A script run by an `EXEC`: the transaction's time.
            let _inner = freeze();
            std::thread::sleep(Duration::from_millis(2));
            assert_eq!(wall_ms_now(), frozen);
        }
        assert_eq!(wall_ms_now(), frozen);
        assert_eq!(ttl_wall_ms_now() as u64, frozen);
        assert_eq!(time_command_wall_tuple(), time);
        drop(outer);
        std::thread::sleep(Duration::from_millis(2));
        assert!(wall_ms_now() > frozen);
    }

    #[test]
    fn a_command_keeps_the_time_it_first_read_until_it_ends() {
        let command = command();
        let first = wall_ms_now();
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(wall_ms_now(), first);
        assert_eq!(ttl_wall_ms_now() as u64, first);
        assert_eq!(
            wall_system_time_now(),
            UNIX_EPOCH + Duration::from_millis(first)
        );
        {
            // Its script: the command's time, and the command keeps it.
            let _script = freeze();
            std::thread::sleep(Duration::from_millis(2));
            assert_eq!(wall_ms_now(), first);
        }
        assert_eq!(wall_ms_now(), first);
        drop(command);
        std::thread::sleep(Duration::from_millis(2));
        assert!(wall_ms_now() > first);
    }

    #[test]
    fn a_command_that_has_not_read_the_clock_keeps_its_scripts_time() {
        let command = command();
        let script = freeze();
        let frozen = wall_ms_now();
        drop(script);
        std::thread::sleep(Duration::from_millis(2));
        assert_eq!(wall_ms_now(), frozen);
        drop(command);
        std::thread::sleep(Duration::from_millis(2));
        assert!(wall_ms_now() > frozen);
    }

    #[test]
    fn a_command_inside_a_unit_goes_by_the_units_time() {
        // A transaction's queued command.
        let unit = freeze();
        let frozen = wall_ms_now();
        std::thread::sleep(Duration::from_millis(2));
        {
            let _command = command();
            assert_eq!(wall_ms_now(), frozen);
        }
        std::thread::sleep(Duration::from_millis(2));
        assert_eq!(wall_ms_now(), frozen);
        drop(unit);
        assert!(wall_ms_now() > frozen);
    }

    #[cfg(debug_assertions)]
    #[test]
    fn a_command_reads_no_clock_until_asked_and_a_clock_moved_ahead_is_not_seen_after_that() {
        let command = command();
        assert_eq!(COMMAND_MS.with(|c| c.get()), PENDING);
        let first = wall_ms_now();
        // The window a test opens between two checks of one command.
        test_move_clock_ahead(60_000);
        assert_eq!(wall_ms_now(), first);
        drop(command);
        // Back with the command's end.
        let after = wall_ms_now();
        assert!(after < first + 60_000, "{first} {after}");
    }

    #[test]
    fn time_moves_once_let_while_expiry_stays_until_the_unit_ends() {
        let unit = freeze();
        let frozen = wall_ms_now();
        let time = time_command_wall_tuple();
        let_time_move();
        std::thread::sleep(Duration::from_millis(5));
        assert!(time_command_wall_tuple() > time);
        assert_eq!(wall_ms_now(), frozen);
        assert_eq!(ttl_wall_ms_now() as u64, frozen);
        drop(unit);

        // The next unit holds `TIME` again.
        let _next = freeze();
        let time = time_command_wall_tuple();
        std::thread::sleep(Duration::from_millis(2));
        assert_eq!(time_command_wall_tuple(), time);
    }
}
