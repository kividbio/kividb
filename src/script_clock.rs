//! Logical clock frozen for the duration of an execution unit, as Redis's
//! `commandTimeSnapshot`: a Lua script, an `FCALL`, or a transaction's
//! `EXEC` (#659). TTL math, the expiry of keys and hash fields, and `TIME`
//! use the snapshot taken when the unit starts. See [`freeze`].
//!
//! `TIME` stops holding it once a script runs past `busy-reply-threshold`
//! (#676). See [`let_time_move`].

use std::cell::RefCell;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
                .saturating_add(u64::from(wall.subsec_millis()));
            FROZEN_MS.with(|f| f.set(ms.max(1)));
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

/// Milliseconds since UNIX epoch; frozen during scripts.
#[allow(dead_code)]
#[inline]
pub fn ttl_wall_ms_now() -> u128 {
    SCRIPT_STATE.with(|s| {
        if let Some((sec, us)) = s.borrow().1 {
            return sec as u128 * 1000 + (us / 1000) as u128;
        }
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    })
}

/// [`ttl_wall_ms_now`] as a `u64`: what a key's entry keeps a time to live
/// as, read on every write lock that looks at a key with one, and by every
/// lazy expiry check. One thread-local load, and outside a script the
/// clock read straight into milliseconds: through `SystemTime`, a
/// `RefCell` borrow and `duration_since` it cost about 75 instructions.
#[inline]
pub fn wall_ms_now() -> u64 {
    let frozen = FROZEN_MS.with(|f| f.get());
    if frozen != 0 {
        return frozen;
    }
    clock_ms()
}

/// The wall clock, in whole milliseconds since the Unix epoch (0 before it).
#[cfg(unix)]
#[inline]
fn clock_ms() -> u64 {
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
fn clock_ms() -> u64 {
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
