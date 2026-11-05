// src/pause.rs
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};

/// Global pause expiry in Unix milliseconds.
/// 0 = not paused. Positive = writes are paused until this timestamp.
/// [`ENDING`]: ended, and the clients it held are still being let go.
pub static PAUSE_UNTIL_MS: AtomicI64 = AtomicI64::new(0);

/// `PAUSE_UNTIL_MS` from `CLIENT UNPAUSE` until the last client the pause
/// held has gone on. Nothing is paused, but every other command waits for
/// them (see [`gate`]): in Redis the clients a pause held run before the
/// next command is read, so a `CLIENT UNBLOCK` sent straight after
/// `CLIENT UNPAUSE` finds the `BLPOP` the pause held blocked on its key.
const ENDING: i64 = -1;

/// Clients held by the pause now, and the ones an ended pause let go
/// whose command has not yet run or blocked ([`LETTING_GO`]).
static HELD: AtomicUsize = AtomicUsize::new(0);

/// The ids of the clients an ended pause let go, until their command has
/// run or blocked.
static LETTING_GO: parking_lot::Mutex<Vec<u64>> = parking_lot::const_mutex(Vec::new());

/// How many there are in [`LETTING_GO`]: one load for a client that blocks.
static LETTING_GO_LEN: AtomicUsize = AtomicUsize::new(0);

// 0 = ALL (default), 1 = WRITE only
static PAUSE_MODE_WRITE_ONLY: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Set while the pause in force is a *handoff*: this node is giving slots to
/// another (atomic slot migration) or its role to its replica (manual
/// failover), and has paused writes to do it.
///
/// The pause is checked in the connection loop, long before a write enters
/// the write gate. A handoff sets the pause, waits out the writes in flight
/// (`write_gate::cut`) and then takes what has been written as everything
/// there is. A write that passed the pause check an instant before the pause
/// was set, and reaches the gate after the cut, would run behind the
/// handoff's back: applied here, acknowledged, and never sent to the node
/// taking over. So a write looks at this flag *inside* the gate
/// (`handoff`), steps out again and waits for the handoff to end
/// (`wait_out_handoff`) — and is then answered by whoever owns the key.
///
/// Not set for `CLIENT PAUSE`: nothing is handed over, so a write that slips
/// past it is merely a moment late in being paused, and waiting for it in
/// the gate would hold a worker thread for as long as the client asked.
///
/// The same flag, a second bit of it, is up while a `MIGRATE` holds keys
/// here ([`KEYS_MOVING`], see `key_moves`): one load still tells a write
/// whether it has anything to look at ([`writes_must_check`]).
///
/// Above those two bits, in a cluster, it counts the times keys have left
/// this node or a slot here has changed state ([`routes_moved`]): a
/// `MIGRATE` deleted what it moved, or `CLUSTER SETSLOT` ran. A command is
/// routed ("is its key here?") before it enters the write gate, and a whole
/// `MIGRATE` of its key can run in between. It then ran here, on a key that
/// had left, and made it again (#331). So a command routed in a cluster
/// takes the count before it looks for its keys ([`routes_now`]), and a
/// write compares the whole flag with that inside the gate: the same one
/// load, against its own number instead of 0. Outside a cluster the count
/// stays 0.
static HANDOFF: AtomicU64 = AtomicU64::new(0);

/// [`HANDOFF`]: a handoff's pause is in force.
const HANDOFF_PAUSE: u64 = 1;
/// [`HANDOFF`]: a `MIGRATE` is moving keys (`key_moves`).
const KEYS_MOVING: u64 = 2;
/// [`HANDOFF`]: one more time keys left here or a slot changed state.
const ROUTES_MOVED: u64 = 4;
/// [`HANDOFF`]: the bits below the count.
const BITS: u64 = ROUTES_MOVED - 1;

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Whether clients are paused, and if they are, whether it is only the ones
/// that write (`Some(true)`) or all of them (`Some(false)`).
pub fn active() -> Option<bool> {
    let until = PAUSE_UNTIL_MS.load(Ordering::Relaxed);
    if until <= 0 {
        return None;
    }
    if now_ms() >= until {
        if PAUSE_UNTIL_MS
            .compare_exchange(until, 0, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            // A handoff that ran out of time is over, like its pause.
            end_handoff_flag();
        }
        return None;
    }
    Some(PAUSE_MODE_WRITE_ONLY.load(Ordering::Relaxed))
}

/// `CLIENT PAUSE timeout [WRITE | ALL]`.
///
/// A pause given while clients are already paused does not shorten the
/// pause or narrow it: the later of the two ends holds, and all clients stay
/// paused if either pause was of all of them.
/// Whether keys whose time has passed are to stay where they are for now:
/// while writes are paused, by `CLIENT PAUSE` (`WRITE` or `ALL`), a slot
/// handoff or a manual failover. Redis does the same (`PAUSE_ACTION_EXPIRE`):
/// a read takes such a key as gone, but it is not deleted, counted in
/// `expired_keys` or announced until the pause is over, and the background
/// sweeps of keys and of hash fields wait as well. One load when nothing is
/// paused.
#[inline]
pub fn expiry_deferred() -> bool {
    PAUSE_UNTIL_MS.load(Ordering::Relaxed) != 0 && active().is_some()
}

pub fn set_pause(duration_ms: i64, write_only: bool) {
    let end = now_ms().saturating_add(duration_ms);
    let (end, write_only) = match active() {
        Some(was_write_only) => (
            end.max(PAUSE_UNTIL_MS.load(Ordering::Relaxed)),
            write_only && was_write_only,
        ),
        None => (end, write_only),
    };
    PAUSE_MODE_WRITE_ONLY.store(write_only, Ordering::Relaxed);
    PAUSE_UNTIL_MS.store(end, Ordering::Relaxed); // gate last
}

pub fn clear_pause() {
    PAUSE_MODE_WRITE_ONLY.store(false, Ordering::Relaxed);
    let held = HELD.load(Ordering::SeqCst) > 0;
    PAUSE_UNTIL_MS.store(if held { ENDING } else { 0 }, Ordering::SeqCst);
    // The last of them may have gone between the two.
    if held && HELD.load(Ordering::SeqCst) == 0 {
        let _ = PAUSE_UNTIL_MS.compare_exchange(ENDING, 0, Ordering::SeqCst, Ordering::SeqCst);
    }
    end_handoff_flag();
    CHANGED.notify_waiters();
}

/// Take the flag of a handoff's pause down, and if it was up, move the
/// routes ([`routes_moved`]) in the same change. A write that finds the
/// flag down then finds the routes moved too, and is routed again in the
/// gate, however the pause ended: the handoff's end, a hold let go,
/// `CLIENT UNPAUSE`, or its time running out. A handoff that moved slots
/// still moves the routes itself first, before `PAUSE_UNTIL_MS` is cleared,
/// for a write that sees the pause over with the flag still up
/// ([`handoff_pause_in_force`]). The flag is up only in a cluster, so the
/// count still stays 0 outside one.
fn end_handoff_flag() {
    let mut now = HANDOFF.load(Ordering::Relaxed);
    while now & HANDOFF_PAUSE != 0 {
        let ended = (now & !HANDOFF_PAUSE).wrapping_add(ROUTES_MOVED);
        match HANDOFF.compare_exchange_weak(now, ended, Ordering::SeqCst, Ordering::Relaxed) {
            Ok(_) => return,
            Err(seen) => now = seen,
        }
    }
}

/// Woken when a pause is ended, and when the last client it held goes on.
static CHANGED: tokio::sync::Notify = tokio::sync::Notify::const_new();

/// What client `id`'s command does when `PAUSE_UNTIL_MS` is not 0, before
/// it runs: wait for the clients an ended pause held to go on (at most
/// 100 ms: they are a step from it), then wait while `cmd` is held (see
/// [`holds`]). What it returns is to be kept until the command has run.
///
/// `None`: the client went while its command was held — `gone`, made only
/// then, resolved — and the command is not to run, as in Redis, which goes on
/// reading a client a pause holds and frees it (#384). Only while it is held:
/// a command the pause does not hold runs, though its client has gone, as a
/// fire-and-forget `LPUSH` sent just as a pause ends runs in Redis.
pub async fn gate<Q: AsRef<[u8]>, G: std::future::Future<Output = ()>>(
    cmd: &[&[u8]],
    queued: &[Vec<Q>],
    id: u64,
    gone: impl FnOnce() -> G,
) -> Option<Option<Ran>> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(100);
    while PAUSE_UNTIL_MS.load(Ordering::SeqCst) == ENDING {
        if std::time::Instant::now() >= deadline {
            // Not to wait on them again: whatever kept them, nothing is
            // paused.
            let _ = PAUSE_UNTIL_MS.compare_exchange(ENDING, 0, Ordering::SeqCst, Ordering::SeqCst);
            break;
        }
        wait_for_change(|| PAUSE_UNTIL_MS.load(Ordering::SeqCst) == ENDING).await;
    }
    if !holds(cmd, queued) {
        return Some(None);
    }
    let held = Held::new();
    HELD.fetch_add(1, Ordering::SeqCst);
    // Counted off however the wait ends, the connection going with it.
    let counted = Counted;
    let gone = gone();
    let mut gone = std::pin::pin!(gone);
    while holds(cmd, queued) {
        tokio::select! {
            biased;
            () = wait_for_change(|| holds(cmd, queued)) => {}
            // Let go with its client; `held` and `counted` go with it.
            () = &mut gone => return None,
        }
    }
    drop(held);
    if PAUSE_UNTIL_MS.load(Ordering::SeqCst) == ENDING {
        // Ended: it counts until its command has run or blocked.
        std::mem::forget(counted);
        LETTING_GO.lock().push(id);
        LETTING_GO_LEN.fetch_add(1, Ordering::SeqCst);
        return Some(Some(Ran(id)));
    }
    Some(None)
}

struct Counted;

impl Drop for Counted {
    fn drop(&mut self) {
        let_go();
    }
}

/// Until [`CHANGED`] or a millisecond, whichever is first, if `still` holds
/// once the wait is set up. The millisecond is for a pause that runs out.
async fn wait_for_change(still: impl Fn() -> bool) {
    let changed = CHANGED.notified();
    tokio::pin!(changed);
    changed.as_mut().enable();
    if still() {
        let _ = tokio::time::timeout(std::time::Duration::from_millis(1), changed).await;
    }
}

/// One client fewer held: the last one ends an ending pause.
fn let_go() {
    if HELD.fetch_sub(1, Ordering::SeqCst) == 1
        && PAUSE_UNTIL_MS
            .compare_exchange(ENDING, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    {
        CHANGED.notify_waiters();
    }
}

/// Client `id` has begun to wait in a blocking command, or its command has
/// run: if an ended pause let it go, it has gone on. (Whatever the pause is
/// now: another may have begun since.)
pub fn went_on(id: u64) {
    if LETTING_GO_LEN.load(Ordering::SeqCst) == 0 {
        return;
    }
    let mut letting_go = LETTING_GO.lock();
    if let Some(at) = letting_go.iter().position(|&other| other == id) {
        letting_go.swap_remove(at);
        LETTING_GO_LEN.fetch_sub(1, Ordering::SeqCst);
        drop(letting_go);
        let_go();
    }
}

/// Kept while the command an ended pause let go runs (see [`gate`]).
pub struct Ran(u64);

impl Drop for Ran {
    fn drop(&mut self) {
        went_on(self.0);
    }
}

/// Pause writes for a handoff (see `HANDOFF`). The caller takes its cut
/// through the write stream *after* this returns; `clear_pause` ends it.
pub fn set_handoff_pause(duration_ms: i64) {
    // The flag first, and before the caller's cut: a write that enters the
    // gate after that cut is certain to see it.
    HANDOFF.fetch_or(HANDOFF_PAUSE, Ordering::SeqCst);
    set_pause(duration_ms, true);
}

/// How long [`hold_writes`] pauses writes for: until the pause is ended, in
/// effect (a century).
const UNTIL_ENDED_MS: i64 = 100 * 365 * 24 * 3600 * 1000;

/// Pause writes as a handoff does (see `HANDOFF`), with no end of its own:
/// until `clear_pause`. For a master that is not sure it still owns its
/// slots (`ClusterState::hold_writes_until_confirmed`), and for a slot
/// migration's handoff until the source knows whether the destination took
/// the slots over (`migration::await_takeover`). A pause that ran out by
/// itself let it take writes again while its replica, or the migration's
/// destination, could be taking the slots over, and what it acknowledged
/// before it heard of that was lost (#242, #278).
pub fn hold_writes() {
    set_handoff_pause(UNTIL_ENDED_MS);
}

/// Whether a write that has just entered the write gate has to step out
/// again: a handoff is under way. One atomic load when none is.
#[inline]
pub fn handoff() -> bool {
    HANDOFF.load(Ordering::Acquire) & HANDOFF_PAUSE != 0 && handoff_pause_in_force()
}

/// Whether the pause of a handoff whose flag a write has just seen is still
/// in force, and then an acquire fence.
///
/// A handoff that ends moves the count of routes first ([`routes_moved`]),
/// then ends the pause (`clear_pause`: `PAUSE_UNTIL_MS`, then the flag). A
/// write can load the flag before that, still up, and then find the pause
/// ended: [`active`] loads `PAUSE_UNTIL_MS` relaxed. The write then asks
/// whether its routes have moved ([`routes_moved_since`]), a third load,
/// which nothing made see the count moved before the pause ended: x86 does,
/// aarch64 need not, and the write ran here, on a slot it had been handed
/// away from (#337). The fence after a load that saw the pause ended makes
/// everything done before it was ended visible. Only here, where the flag
/// was up: an ordinary write never comes this way.
#[cold]
#[inline(never)]
fn handoff_pause_in_force() -> bool {
    let in_force = active().is_some();
    std::sync::atomic::fence(Ordering::Acquire);
    in_force
}

/// Whether a write that has just entered the write gate has anything to
/// look at: a handoff ([`handoff`]), a `MIGRATE` moving keys
/// (`key_moves::names_a_moving_key`), or keys that left here, or a slot
/// that changed state, since the write was routed. `routed` is what
/// [`routes_now`] said before it was routed; 0 if it was not, outside a
/// cluster, where the count stays 0. One atomic load when none of these is
/// so.
#[inline]
pub fn writes_must_check(routed: u64) -> bool {
    HANDOFF.load(Ordering::Acquire) != routed
}

/// Whether a `MIGRATE` holds keys here (`key_moves`).
#[inline]
pub fn keys_moving() -> bool {
    HANDOFF.load(Ordering::Acquire) & KEYS_MOVING != 0
}

/// How many times keys have left here or a slot has changed state (see
/// [`HANDOFF`]). Taken by a command in a cluster before it looks for its
/// keys: whatever moves them after this, it sees in the write gate.
#[inline]
pub fn routes_now() -> u64 {
    HANDOFF.load(Ordering::Acquire) & !BITS
}

/// Whether keys have left here, or a slot has changed state, since
/// [`routes_now`] said `routed`.
pub fn routes_moved_since(routed: u64) -> bool {
    routes_now() != routed
}

/// Keys have left this node, or a slot here has changed state: a write
/// routed before this, and not yet in the write gate, is routed again there
/// (#331). In a cluster only.
pub(crate) fn routes_moved() {
    HANDOFF.fetch_add(ROUTES_MOVED, Ordering::SeqCst);
}

/// `MIGRATE` holds keys here, or no longer does (`key_moves`, under its
/// lock). Before `MIGRATE` waits out the writes in the gate: a write that
/// enters the gate after that is certain to see it.
pub(crate) fn set_keys_moving(on: bool) {
    if on {
        HANDOFF.fetch_or(KEYS_MOVING, Ordering::SeqCst);
    } else {
        HANDOFF.fetch_and(!KEYS_MOVING, Ordering::SeqCst);
    }
}

/// Wait for the handoff to end. Called by a write command, outside the write
/// gate, on its worker thread: a handoff lasts as long as it takes (a manual
/// failover: until the replica has caught up and won its election; a slot
/// migration: until the source knows whether the destination took the
/// slots), or until `CLIENT UNPAUSE`.
///
/// On a worker of the multi-threaded tokio runtime (`--io-model tokio`) the
/// wait gives the worker's other tasks to another thread first
/// (`block_in_place`). A task the worker had queued to run next (tokio's
/// LIFO slot, which no other worker takes) otherwise waited with it: the
/// replication hub, woken by the write before, did, and the master's
/// handoff, which waits for the hub to have sent everything to the replica,
/// waited for the pause to run out (10 s). By then the replica had given the
/// failover up (it waits 5 s), and `CLUSTER FAILOVER` did nothing.
pub fn wait_out_handoff() {
    let wait = || {
        while handoff() {
            std::thread::sleep(std::time::Duration::from_micros(200));
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

/// Whether `cmd` has to wait for the pause to end. `queued` is the
/// connection's transaction, which is what an `EXEC` would run.
///
/// With all clients paused, every command waits — but for `CLIENT UNPAUSE`,
/// which Redis holds too: there, a pause of everything can only run out.
/// With only writers paused, the commands that wait are the ones that may
/// change the dataset or the replication stream: writes, `PFCOUNT` (which
/// may rewrite its key), `PUBLISH`, a script that has not declared itself
/// free of writes, and an `EXEC` of a transaction holding any of those.
///
/// A command with the wrong number of arguments is not held: it is going to
/// be refused whenever it runs, and is refused now.
pub fn holds<Q: AsRef<[u8]>>(cmd: &[&[u8]], queued: &[Vec<Q>]) -> bool {
    let Some(write_only) = active() else {
        return false;
    };
    let name = String::from_utf8_lossy(cmd[0]).to_ascii_uppercase();
    if crate::commands::arity::check(&name, cmd).is_some() {
        return false;
    }
    if !write_only {
        return !(name == "CLIENT"
            && cmd
                .get(1)
                .is_some_and(|sub| sub.eq_ignore_ascii_case(b"UNPAUSE")));
    }
    if name == "EXEC" {
        return queued.iter().any(|command| {
            command.first().is_some_and(|cmd0| {
                may_replicate(
                    &String::from_utf8_lossy(cmd0.as_ref()).to_ascii_uppercase(),
                    command.get(1).map(|arg| arg.as_ref()),
                )
            })
        });
    }
    may_replicate(&name, cmd.get(1).copied())
}

/// Whether running `name` — with `first`, its first argument — may write.
fn may_replicate(name: &str, first: Option<&[u8]>) -> bool {
    #[cfg(feature = "lua")]
    let declared_read_only = |name: &str, first: &[u8]| match name {
        "EVAL" => crate::commands::script_declares_no_writes(first),
        "EVALSHA" => crate::commands::cached_script_declares_no_writes(first),
        _ => crate::commands::function_declares_no_writes(first),
    };
    #[cfg(not(feature = "lua"))]
    let declared_read_only = |_: &str, _: &[u8]| false;
    match name {
        "PFCOUNT" | "PUBLISH" | "SPUBLISH" => true,
        "EVAL" | "EVALSHA" | "FCALL" => !first.is_some_and(|first| declared_read_only(name, first)),
        "EVAL_RO" | "EVALSHA_RO" | "FCALL_RO" => false,
        _ => crate::replication::is_write_command(name),
    }
}

/// A client held by the pause is a blocked client, for as long as it is.
pub struct Held;

impl Held {
    pub fn new() -> Self {
        crate::stats::BLOCKED_CLIENTS.fetch_add(1, Ordering::Relaxed);
        Held
    }
}

impl Default for Held {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        crate::stats::BLOCKED_CLIENTS.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Held by a unit test that sets or ends the pause, which is the process's.
#[cfg(test)]
pub(crate) static TEST_LOCK: parking_lot::Mutex<()> = parking_lot::const_mutex(());

#[cfg(test)]
mod tests {
    use super::*;

    // One test, because the pause is the process's.
    #[test]
    fn the_longer_and_the_wider_pause_holds() {
        let _pause = TEST_LOCK.lock();
        clear_pause();
        assert_eq!(active(), None);
        set_pause(60_000, true);
        assert_eq!(active(), Some(true));
        // A shorter pause does not end it sooner.
        let end = PAUSE_UNTIL_MS.load(Ordering::Relaxed);
        set_pause(10, true);
        assert_eq!(PAUSE_UNTIL_MS.load(Ordering::Relaxed), end);
        // A pause of everybody widens it, and a later one of writers does not
        // narrow it again.
        set_pause(10, false);
        assert_eq!(active(), Some(false));
        set_pause(10, true);
        assert_eq!(active(), Some(false));

        let none: &[Vec<Vec<u8>>] = &[];
        assert!(holds(&[b"GET", b"k"], none));
        assert!(!holds(&[b"CLIENT", b"UNPAUSE"], none));
        clear_pause();
        set_pause(60_000, true);
        assert!(!holds(&[b"GET", b"k"], none));
        assert!(holds(&[b"SET", b"k", b"v"], none));
        assert!(holds(&[b"PUBLISH", b"c", b"m"], none));
        // Refused at once, not after the pause.
        assert!(!holds(&[b"SET", b"k"], none));
        // An EXEC waits if what it would run does.
        let reads = vec![vec![b"GET".to_vec(), b"k".to_vec()]];
        let writes = vec![vec![b"SET".to_vec(), b"k".to_vec(), b"v".to_vec()]];
        assert!(!holds(&[b"EXEC"], &reads));
        assert!(holds(&[b"EXEC"], &writes));
        clear_pause();
        assert!(!holds(&[b"SET", b"k", b"v"], none));

        // A handoff is a pause of writers that writes in the gate can see;
        // CLIENT PAUSE is not one, and it ends with its pause.
        assert!(!handoff());
        set_pause(60_000, true);
        assert!(!handoff());
        clear_pause();
        set_handoff_pause(60_000);
        assert!(handoff() && holds(&[b"SET", b"k", b"v"], none));
        clear_pause();
        assert!(!handoff());
        set_handoff_pause(1);
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(!handoff());
        // ... and a later CLIENT PAUSE is not mistaken for one.
        set_pause(60_000, true);
        assert!(!handoff());
        clear_pause();
        // Writes held until further notice are a handoff, and end with it.
        hold_writes();
        assert!(handoff() && holds(&[b"SET", b"k", b"v"], none));
        assert!(!holds(&[b"GET", b"k"], none));
        clear_pause();
        assert!(!handoff());

        // A write that waits out a handoff on a worker of the multi-threaded
        // runtime leaves that worker's other tasks to run. Here the one that
        // ends the handoff, which the worker would have run next (its LIFO
        // slot, which no other worker takes): it used to wait with the
        // write, until the pause ran out.
        set_handoff_pause(20_000);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let started = std::time::Instant::now();
        runtime.block_on(async {
            tokio::spawn(async {
                let ender = tokio::spawn(async { clear_pause() });
                wait_out_handoff();
                ender.await.unwrap();
            })
            .await
            .unwrap();
        });
        assert!(
            started.elapsed() < std::time::Duration::from_secs(10),
            "the handoff was waited out for {:?}",
            started.elapsed()
        );
        assert!(!handoff());
    }
}
