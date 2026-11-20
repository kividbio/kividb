// src/shutdown.rs
//
// Global shutdown-intent signals.  These are set by the SHUTDOWN command
// handler (synchronous, inside execute_command) and read by the signal-
// watching thread in main.rs which already owns the store reference needed
// to write the snapshot.
//
// Why atomics instead of a channel?
//   • The command handler is a plain `fn` — no async executor available.
//   • A channel would require Arc<Mutex<Sender>> or similar, adding overhead
//     to every command dispatch even when SHUTDOWN is never called.
//   • Two atomics (pending flag + mode byte) give us lock-free, zero-cost
//     reads in the happy path.

use std::sync::atomic::{AtomicBool, AtomicU8};

/// Set to `true` by SHUTDOWN (any mode except ABORT).
/// The signal-watching thread polls this and triggers the real shutdown path.
pub static SHUTDOWN_PENDING: AtomicBool = AtomicBool::new(false);

/// Encodes the desired shutdown mode so the signal thread knows what to do:
/// `SAVE` (no bits), or a combination of the bits below, as the command's
/// modifiers combine (`NOSAVE NOW`). `FORCE` is [`SHUTDOWN_FORCE`].
pub mod mode {
    pub const SAVE: u8 = 0; // default — write snapshot then exit
    pub const NOSAVE: u8 = 1; // bit: exit without writing snapshot
    pub const NOW: u8 = 2; // bit: do not wait for the replicas
}

/// The mode last written by the SHUTDOWN command handler.
/// Only meaningful when SHUTDOWN_PENDING is true.
pub static SHUTDOWN_MODE: AtomicU8 = AtomicU8::new(mode::SAVE);

// ── SHUTDOWN as a command: no reply, an error if it fails ────────────────
//
// Redis answers a successful SHUTDOWN with nothing: the connection closes
// when the process exits, after the final save. A shutdown that fails (the
// save cannot be written) or is cancelled (`SHUTDOWN ABORT`) is answered
// with an error and the server runs on.
//
// The command handler only validates, and asks for the shutdown here
// ([`request`]). The connection loop then flushes the replies that came
// before it, and waits ([`failed_after`]) for the shutdown to fail, or for
// the process to end: the sequence itself runs on the signal thread, off the
// workers, which go on serving the replicas the shutdown waits for.

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};

/// What a failed or cancelled `SHUTDOWN` is answered with.
pub const FAILED_REPLY: &str = "ERR Errors trying to SHUTDOWN. Check logs.";

/// What `SHUTDOWN ABORT` is answered with when there is nothing to abort.
pub const NO_SHUTDOWN_IN_PROGRESS: &str = "ERR No shutdown in progress.";

/// The sequence has begun (it is past the signal thread's poll).
static RUNNING: AtomicBool = AtomicBool::new(false);

/// `SHUTDOWN ABORT` was given while a shutdown was in progress.
static ABORT: AtomicBool = AtomicBool::new(false);

/// How many shutdowns have failed or been aborted: a connection that asked
/// for one is answered when this moves.
static FAILED: AtomicU64 = AtomicU64::new(0);

/// `SHUTDOWN FORCE`: a final save that fails does not keep the server
/// running.
pub static SHUTDOWN_FORCE: AtomicBool = AtomicBool::new(false);

/// Held by [`request`], [`abort`] and [`failed`]: a `SHUTDOWN` that reads
/// the count of failures and finds no shutdown in progress does so either
/// wholly before a failure, and joins the shutdown that fails, or wholly
/// after it, and starts one of its own. Between the two, it started one and
/// was then answered with the failure of the one before, or joined one that
/// had already failed and waited for ever.
static LOCK: parking_lot::Mutex<()> = parking_lot::const_mutex(());

thread_local! {
    /// The `SHUTDOWN` this thread's handler has just accepted, for the
    /// connection loop that called it (one command at a time on a thread):
    /// the number of shutdowns that had failed before it.
    static REQUESTED: Cell<Option<u64>> = const { Cell::new(None) };
}

/// A shutdown is waiting for the signal thread, or running.
pub fn in_progress() -> bool {
    SHUTDOWN_PENDING.load(Ordering::SeqCst) || RUNNING.load(Ordering::SeqCst)
}

/// The `SHUTDOWN` handler: ask for a shutdown in `mode`, and mark this
/// thread's command as the one that asked. One that asks while another is
/// in progress waits with it, as in Redis.
pub fn request(mode: u8, force: bool) {
    let _lock = LOCK.lock();
    // Before the shutdown can fail: what the connection waits for is a
    // failure after this.
    REQUESTED.with(|r| r.set(Some(FAILED.load(Ordering::SeqCst))));
    if in_progress() {
        return;
    }
    SHUTDOWN_MODE.store(mode, Ordering::SeqCst);
    SHUTDOWN_FORCE.store(force, Ordering::SeqCst);
    ABORT.store(false, Ordering::SeqCst);
    SHUTDOWN_PENDING.store(true, Ordering::SeqCst);
}

/// The connection loop, after a command: if it was a `SHUTDOWN` that was
/// accepted, the number of failed shutdowns before it. The loop is then to
/// wait with [`failed_after`].
pub fn take_requested() -> Option<u64> {
    REQUESTED.with(Cell::take)
}

/// Wait until a shutdown fails or is aborted after `seen` ([`failures`]).
/// A shutdown that succeeds ends the process, so this does not return then.
pub async fn failed_after(seen: u64) {
    while FAILED.load(Ordering::SeqCst) == seen {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

/// `SHUTDOWN ABORT`: cancel the shutdown that is in progress, if there is
/// one. Whoever asked for it is answered with [`FAILED_REPLY`].
pub fn abort() -> bool {
    let _lock = LOCK.lock();
    if !in_progress() {
        return false;
    }
    ABORT.store(true, Ordering::SeqCst);
    true
}

/// The shutdown in progress is to be given up.
pub fn abort_requested() -> bool {
    ABORT.load(Ordering::SeqCst)
}

/// The shutdown has been given up, its effects undone: the next may begin.
fn failed() {
    let _lock = LOCK.lock();
    // Back to what a signal asks for: a signal reads the mode, and one after
    // an aborted `SHUTDOWN NOSAVE` wrote no final save.
    SHUTDOWN_MODE.store(mode::SAVE, Ordering::SeqCst);
    SHUTDOWN_FORCE.store(false, Ordering::SeqCst);
    SHUTDOWN_PENDING.store(false, Ordering::SeqCst);
    ABORT.store(false, Ordering::SeqCst);
    RUNNING.store(false, Ordering::SeqCst);
    FAILED.fetch_add(1, Ordering::SeqCst);
}

/// A script has run past its time limit (`lua-time-limit`): other clients
/// are told `BUSY`, and it holds the write gate until it ends, if it ever
/// does. `SHUTDOWN NOSAVE` is the way out Redis gives (`BUSY ... You can
/// only call SCRIPT KILL or SHUTDOWN NOSAVE.`), so it must not wait for it.
pub fn script_busy() -> bool {
    #[cfg(feature = "lua")]
    {
        crate::commands::running_script_is_over_its_time_limit()
    }
    #[cfg(not(feature = "lua"))]
    {
        false
    }
}

/// How the final sequence ended.
#[derive(Debug, PartialEq, Eq)]
pub enum Ended {
    /// The process is to exit now.
    Exit,
    /// Aborted, or failed at a client's request: undone, and the server
    /// runs on.
    KeepRunning,
}

/// What the final sequence needs.
pub struct Final<'a> {
    pub store: &'a crate::store::Store,
    pub index_store: &'a crate::vector::IndexStore,
    pub repl_state: &'a crate::replication::ReplState,
    pub hub_tx: &'a crate::replication::HubTx,
}

impl Final<'_> {
    /// The sequence of a shutdown in `m` (a [`mode`]): wait for the replicas,
    /// then close the command gate, and write the final save under it.
    ///
    /// [`Ended::Exit`] when the process is to exit now: the connections stay open until
    /// it does, and the gate stays closed, so that nothing is acknowledged
    /// after the save's cut. [`Ended::KeepRunning`] when the shutdown was aborted, or failed
    /// at the request of a client (`by_command`): everything it did is
    /// undone, the server runs on, and [`failed_after`] is let go. A
    /// shutdown from a signal that fails ends the process with status 1, as
    /// it always did.
    pub fn run(&self, m: u8, by_command: bool) -> Ended {
        use crate::replication as repl;
        let (nosave, now) = (m & mode::NOSAVE != 0, m & mode::NOW != 0);
        RUNNING.store(true, Ordering::SeqCst);
        // First, before anything that can take long: a node stopped with a
        // grace period is killed at its end.
        crate::cluster::save_at_shutdown();
        // `SHUTDOWN NOSAVE` while a script runs past its time limit: it is
        // the way to stop that script, and the wait below for the writes in
        // flight would be for the script to end, which it may never do.
        let script_in_the_way = || nosave && script_busy();
        // Before the gate below: the clients are paused, the write stream
        // ends here, and the replicas are given the time to receive all of
        // it, so that the restart can continue them from the offset saved
        // below (#306). The worker threads go on serving the replicas
        // meanwhile. `SHUTDOWN NOW` does not wait.
        let started = std::time::Instant::now();
        let (paused, deadline) = if !now {
            let (paused, deadline) =
                repl::drain_replicas_for_shutdown(self.repl_state, self.hub_tx, &|| {
                    abort_requested() || script_in_the_way()
                });
            (Some(paused), deadline)
        } else {
            (None, started)
        };
        if abort_requested() {
            return self.give_up(by_command, "SHUTDOWN ABORT", paused);
        }
        if nosave {
            // As in Redis, NOSAVE waits for the replicas too, but writes
            // nothing: the pause stays, to the exit.
            crate::aof::sync_for_shutdown();
            // Records the disk would not take are in memory only: not a
            // clean exit.
            if crate::aof::lost_at_exit() {
                std::process::exit(1);
            }
            log_info!("SHUTDOWN NOSAVE — skipping snapshot. Goodbye.");
            return Ended::Exit;
        }
        // No command runs from here to the exit: the snapshot is cut at one
        // moment (#148), and a write a connection already open made after
        // that moment was acknowledged and then lost with the process. Taken
        // alone, the gate waits for the commands running now and lets no
        // other start; it is never let go, unless the shutdown fails. What
        // the final save takes is taken before the gate: a replica's applier
        // holds it while it waits at the gate (#308).
        let hold = match repl::hold_for_final_save(self.repl_state) {
            Ok(hold) => hold,
            Err(e) => {
                log_error!("Final snapshot failed: {e}");
                return self.give_up(by_command, "the final save could not begin", paused);
            }
        };
        repl::end_stream_for_shutdown(self.repl_state);
        if !now {
            repl::report_replicas_at_shutdown(self.repl_state, self.hub_tx, started, deadline);
        }
        if abort_requested() {
            drop(hold);
            return self.give_up(by_command, "SHUTDOWN ABORT", paused);
        }
        // Not over the last good dump with a dataset a full sync is still
        // replacing.
        let Some(settled) = repl::dataset_for_final_save(self.repl_state) else {
            log_warn!("{}", repl::FINAL_SAVE_SKIPPED);
            std::process::exit(0);
        };
        log_info!("Writing final snapshot...");
        // With the replica's position in its master's stream beside it, so
        // that the restart can ask to continue.
        let result = if crate::aof::injected_failure("final-save") {
            Err(std::io::Error::other("injected failure of the final save"))
        } else {
            repl::save_dataset_held(&hold, self.store, self.index_store, self.repl_state)
        };
        match result {
            Ok(()) => {
                repl::note_clean_exit();
                log_info!("Goodbye.");
                // Forgotten, not kept in a binding: one dropped at the end
                // of this function opened the gate again before the exit,
                // and the writes that ran in between were acknowledged and
                // lost.
                std::mem::forget(hold);
                std::mem::forget(settled);
                Ended::Exit
            }
            Err(e) => {
                if crate::aof::write_failing() {
                    log_error!(
                        "Final snapshot failed: {e}. Writes since the last successful save are \
                         not in dump.kdb, and {} bytes of them could not be written to the AOF \
                         either: they are in memory only. Check free disk space and write \
                         permission on the data directory",
                        crate::aof::unwritten_bytes()
                    );
                } else {
                    log_error!(
                        "Final snapshot failed: {e}. Writes since the last successful save are \
                         not in dump.kdb (they are in the AOF, if appendonly is on). Check free \
                         disk space and write permission on the data directory"
                    );
                }
                if by_command {
                    // Not a clean exit, but there is none: the server runs
                    // on, and the gate opens.
                    drop(settled);
                    drop(hold);
                    self.undo(paused);
                    Ended::KeepRunning
                } else {
                    // Not a clean exit: whatever restarts this node should
                    // know its last writes are not on disk.
                    crate::aof::sync_for_shutdown();
                    crate::aof::lost_at_exit();
                    std::process::exit(1)
                }
            }
        }
    }

    /// Before the save: the shutdown is given up (aborted, or its gate could
    /// not be taken).
    fn give_up(&self, by_command: bool, why: &str, paused: Option<crate::pause::Undo>) -> Ended {
        if by_command || abort_requested() {
            log_warn!("Shutdown given up: {why}. The server keeps running");
            self.undo(paused);
            Ended::KeepRunning
        } else {
            crate::aof::sync_for_shutdown();
            crate::aof::lost_at_exit();
            std::process::exit(1)
        }
    }

    /// Take back what the sequence did before the save: its own pause of
    /// the clients (`paused`), and the end of the stream, and let the next
    /// shutdown begin. A pause that was in force before it stays: a
    /// `CLIENT PAUSE`, or the hold of writes of a failover or a slot
    /// migration, whose writes would be acknowledged here and lost with the
    /// slots. The gate and the save's locks are let go by the caller, by
    /// dropping them.
    fn undo(&self, paused: Option<crate::pause::Undo>) {
        self.repl_state
            .stream_closed
            .store(false, std::sync::atomic::Ordering::SeqCst);
        if let Some(paused) = paused {
            crate::pause::undo_pause(paused);
        }
        failed();
    }
}
