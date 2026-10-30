//! Logical clock frozen for the duration of a Lua script / FCALL (Redis `execution unit`).
//! TTL math and `TIME` use the snapshot taken when script execution starts.

use std::cell::RefCell;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

thread_local! {
    #[allow(clippy::type_complexity)]
    static SCRIPT_STATE: RefCell<(u32, Option<(Instant, u64, u32)>)> =
        const { RefCell::new((0, None)) };
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
            st.1 = Some((Instant::now(), wall.as_secs(), wall.subsec_micros()));
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
        }
    });
}

/// Monotonic “now” for TTL (`Instant` comparisons). Frozen during scripts.
#[allow(dead_code)]
#[inline]
pub fn ttl_clock_now() -> Instant {
    SCRIPT_STATE.with(|s| s.borrow().1.map(|(a, _, _)| a).unwrap_or_else(Instant::now))
}

/// Wall-clock seconds (UNIX) for `EXAT`-style math. Frozen during scripts.
#[allow(dead_code)]
#[inline]
pub fn ttl_wall_secs_now() -> u64 {
    SCRIPT_STATE.with(|s| {
        s.borrow().1.map(|(_, sec, _)| sec).unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        })
    })
}

/// Milliseconds since UNIX epoch; frozen during scripts.
#[allow(dead_code)]
#[inline]
pub fn ttl_wall_ms_now() -> u128 {
    SCRIPT_STATE.with(|s| {
        if let Some((_, sec, us)) = s.borrow().1 {
            return sec as u128 * 1000 + (us / 1000) as u128;
        }
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    })
}

/// [`ttl_wall_ms_now`] as a `u64`, in 64-bit arithmetic: what a key's
/// entry keeps a time to live as, read on every write lock that looks at a
/// key with one.
#[inline]
pub fn wall_ms_now() -> u64 {
    SCRIPT_STATE.with(|s| {
        if let Some((_, sec, us)) = s.borrow().1 {
            return sec.saturating_mul(1000).saturating_add(u64::from(us / 1000));
        }
        let d = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        d.as_secs()
            .saturating_mul(1000)
            .saturating_add(u64::from(d.subsec_millis()))
    })
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

/// `TIME` command: `(seconds, microseconds_within_second)`.
#[inline]
pub fn time_command_wall_tuple() -> (u64, u32) {
    SCRIPT_STATE.with(|s| {
        if let Some((_, sec, us)) = s.borrow().1 {
            return (sec, us);
        }
        let d = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        (d.as_secs(), d.subsec_micros())
    })
}
