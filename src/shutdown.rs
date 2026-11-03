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

/// Encodes the desired shutdown mode so the signal thread knows what to do.
pub mod mode {
    pub const SAVE: u8 = 0; // default — write snapshot then exit
    pub const NOSAVE: u8 = 1; // exit without writing snapshot
    pub const NOW: u8 = 2; // skip AOF flush wait, write snapshot, exit
    pub const FORCE: u8 = 3; // ignore replica/AOF state checks, write snapshot, exit
}

/// The mode last written by the SHUTDOWN command handler.
/// Only meaningful when SHUTDOWN_PENDING is true.
pub static SHUTDOWN_MODE: AtomicU8 = AtomicU8::new(mode::SAVE);
