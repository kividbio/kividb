//! Parking and resuming blocking commands (`BLPOP`, `BZPOPMIN`, `XREAD BLOCK`, …).
//!
//! # Why this exists
//!
//! Blocking commands used to wait by calling `tokio::task::block_in_place` and
//! then `Handle::current().block_on(...)` from inside the *synchronous* command
//! dispatch path. That has two problems:
//!
//!   1. `block_in_place` **panics on a current-thread runtime**, and
//!      `tokio_uring` is inherently thread-per-core current-thread. Every
//!      blocking command therefore killed the connection under
//!      `--io-model uring` — see `docs/IO_URING_BLOCKING_COMMANDS.md`.
//!   2. Even on the multi-thread runtime where it *worked*, `block_in_place`
//!      forces tokio to hand the worker's queue to a replacement thread for the
//!      duration of the block. A thousand clients parked in `BLPOP key 0` cost a
//!      thousand OS threads.
//!
//! # How it works instead
//!
//! The wait is moved out of the sync handler and into the connection loop,
//! which is already `async` (this is the same place `WAIT`/`WAITAOF` have always
//! done their waiting). The handler is split across two invocations:
//!
//! * **Park.** The handler runs its normal fast path, registers its waiters on
//!   the [`crate::wait_table::WaitTable`], re-checks to close the TOCTOU window,
//!   and then — instead of blocking — hands the registered receivers to
//!   [`park`], which stashes them in [`BlockingCtl::parked`]. The reply it
//!   returns in that case is a discarded placeholder.
//! * **Await.** The loop sees the parked slot via [`BlockingSlot::take_parked`],
//!   flushes any pipelined replies it is holding, then `.await`s
//!   [`BlockingWait::wait`]. A plain `.await` behaves identically on both
//!   runtimes and costs a task, not a thread.
//! * **Resume.** The loop re-dispatches the *same* command — it simply does not
//!   advance its parse offset, so the parse loop re-reads the same bytes — with
//!   the delivery in [`BlockingCtl::resume`]. The handler sees it, skips the
//!   fast path and the registration entirely, and runs only its tail.
//!
//! Re-dispatching rather than running a stored continuation is what keeps the
//! tail logic next to the parsing that produced it — `XREADGROUP`'s tail, for
//! instance, needs to re-run consumer-group delivery against `&mut Ctx`, which a
//! detached closure could not borrow. Re-reading the bytes rather than cloning
//! the command keeps the non-blocking path allocation-free.
//!
//! # Callers that cannot await
//!
//! Lua, `MULTI`/`EXEC` and the replica applier build their `Ctx` with
//! `blocking: None`. [`park`] then returns [`ParkOutcome::CannotWait`] and the
//! handler replies as if the block had timed out immediately. That is exactly
//! what Redis does: a blocking command inside `MULTI` never blocks, it returns
//! its empty-handed reply.

use crate::wait_table::Value;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

/// A blocking command whose waiters are registered but not yet consumed.
///
/// Held by the connection loop across the `.await`. Dropping it without
/// awaiting cancels every registration (the `WaitTable` skips waiters whose
/// receiver has gone away), which is what makes it safe for the loop to bail
/// out on a write error while a command is parked.
pub struct BlockingWait {
    /// One registered receiver per key the command is waiting on. Raced against
    /// each other; the first delivery wins and the rest are cancelled.
    rxs: Vec<(String, oneshot::Receiver<Value>)>,
    /// `None` means "wait forever" (`timeout 0`).
    timeout: Option<Duration>,
    /// Carried across the park and handed back on resume. See [`BlockState`].
    state: Option<BlockState>,
}

/// Command state captured at park time and handed back verbatim on resume,
/// for the cases where re-deriving it from the command would be *wrong*.
pub enum BlockState {
    /// `XREAD BLOCK`'s resolved start IDs.
    ///
    /// `$` means "entries added after this command arrived", and is resolved to
    /// the stream's last ID at that moment. Re-resolving it on resume would
    /// point *past* the very entry that woke us, so `XREAD BLOCK $` — the most
    /// common form there is — would return nil every time. Resolve once, reuse.
    ///
    /// And its deadline (`None` = wait forever): the reader is woken when its
    /// stream is written, which is not always by something it can read — the
    /// entry was added and deleted again inside one `MULTI` — and then it
    /// goes on waiting for what is left of its time.
    StreamIds {
        ids: Vec<crate::types::StreamId>,
        deadline: Option<std::time::Instant>,
        /// Its registrations: dropping this, however the wait ends, removes them.
        registered: crate::wait_table::StreamWait,
    },
    /// A command waiting in line for its keys (the blocking list and
    /// sorted-set commands): its deadline (`None` = wait forever) and its
    /// place in the line.
    ///
    /// Such a command is woken because its key was written, not by being
    /// handed what it asked for, so it can wake to find nothing for it —
    /// another client got there first, or the list is still too short for
    /// `BLMOVEM … EXACTLY n` — and have to wait again: against the original
    /// deadline, not a fresh timeout, and in its original place, not at the
    /// back. A wait carrying this is allowed to park from a resume dispatch;
    /// see [`BlockingWait::may_repark`].
    Queued {
        deadline: Option<std::time::Instant>,
        ticket: std::sync::Arc<crate::wait_table::Ticket>,
    },
    /// `TS.READ … BLOCK`: its deadline and its cursor. It too is woken by a
    /// signal and may have to wait again; and like `XREAD $` its
    /// cursor (`+`, `$`) is resolved once, when the command arrives.
    TsRead {
        deadline: Option<std::time::Instant>,
        cursor: u64,
        /// Whether the series was there when the client began to wait: if it
        /// was and no longer is, the wait is over.
        existed: bool,
    },
    /// `XREADGROUP … BLOCK`: its deadline, and its place in the line of the
    /// clients reading the stream's groups. It is woken when the stream is
    /// written — and, with `CLAIM`, when a pending entry has been idle for
    /// long enough to be claimed: a timer, set when it parks — and waits
    /// again if that has produced nothing for it.
    GroupRead {
        deadline: Option<std::time::Instant>,
        ticket: std::sync::Arc<crate::wait_table::Ticket>,
    },
}

/// `CLIENT UNBLOCK id [TIMEOUT | ERROR]`: how the blocked client is to be
/// told that its wait is over.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unblock {
    /// As if its timeout had passed.
    Timeout,
    /// With `-UNBLOCKED`.
    Error,
}

/// The reply to a client unblocked with `ERROR`.
pub const UNBLOCKED: &[u8] = b"-UNBLOCKED client unblocked via CLIENT UNBLOCK\r\n";

/// How a wait ended, when it was not by what was waited for or by its
/// timeout. Kept by the connection loop across the dispatch that resumes
/// the command, which is where the command tidies up after itself.
#[derive(Default)]
pub struct Interrupted {
    /// The client has gone — closed its connection, or was killed. The
    /// connection is closed once the command has left the line it was in.
    pub gone: bool,
    /// `CLIENT UNBLOCK … ERROR`: the reply is [`UNBLOCKED`].
    pub unblocked_with_error: bool,
}

/// The timeout of a blocking command, in seconds, as Redis reads one: any
/// float (hexadecimal included), not negative, and not so large that the
/// deadline in milliseconds would overflow.
pub fn parse_timeout(raw: &[u8]) -> Result<f64, Vec<u8>> {
    use crate::resp::write::resp_error;
    let Some(secs) = crate::commands::zset::parse_score(raw).filter(|t| t.is_finite()) else {
        return Err(resp_error("ERR timeout is not a float or out of range"));
    };
    if secs < 0.0 {
        return Err(resp_error("ERR timeout is negative"));
    }
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_millis() as f64);
    if secs * 1000.0 + now_ms >= i64::MAX as f64 {
        return Err(resp_error("ERR timeout is out of range"));
    }
    Ok(secs)
}

/// Mark an error reply as one a woken client is told even though it says
/// WRONGTYPE: see [`wait_for_keys`].
pub fn definite(mut reply: Vec<u8>) -> Vec<u8> {
    reply.insert(0, DEFINITE);
    reply
}

const DEFINITE: u8 = 0x01;

/// What a resumed blocking command was handed: the key that fired and the value
/// delivered through the handoff, or `None` if the wait timed out.
///
/// `XREAD`/`XREADGROUP` use a zero-length sentinel value (see
/// `WaitTable::notify_stream_waiters`) and only care whether this is `Some`.
pub type Delivery = Option<(String, Value)>;

/// Per-command blocking state, carried on [`crate::commands::ctx::Ctx`].
#[derive(Default)]
pub struct BlockingCtl {
    /// Whether the caller is able to `.await` a park. False for Lua, `EXEC`
    /// and the replica applier, which makes blocking commands non-blocking.
    pub awaitable: bool,
    /// Set by a handler that parked; read by `execute_command`.
    pub parked: Option<BlockingWait>,
    /// Set by the connection loop on the resume dispatch; taken by the handler.
    pub resume: Option<Delivery>,
    /// State the handler stashed at park time, returned to it on resume.
    pub state: Option<BlockState>,
}

/// The connection loop's end of the parking mechanism.
///
/// Lives for the whole connection and is handed to each dispatch as
/// `Ctx::blocking`. Holding it here rather than rebuilding it per command is
/// what keeps the hot path free of the struct's ~160 bytes — see the note on
/// [`crate::commands::ctx::Ctx::blocking`].
#[derive(Default)]
pub struct BlockingSlot {
    ctl: BlockingCtl,
    /// Delivery waiting to be handed to the resume dispatch.
    pending: Option<(Delivery, Option<BlockState>)>,
    /// True while the current dispatch *is* a resume. Guards against a handler
    /// that parks again after being resumed, which would spin the loop forever.
    in_resume: bool,
}

impl BlockingSlot {
    /// Prepare the slot for the next dispatch and hand out the borrow that
    /// goes into `Ctx`.
    pub fn prime(&mut self) -> &mut BlockingCtl {
        self.ctl.awaitable = true;
        self.ctl.parked = None;
        match self.pending.take() {
            Some((delivery, state)) => {
                self.in_resume = true;
                self.ctl.resume = Some(delivery);
                self.ctl.state = state;
            }
            None => {
                self.in_resume = false;
                self.ctl.resume = None;
                self.ctl.state = None;
            }
        }
        &mut self.ctl
    }

    /// Take the wait a handler parked during the dispatch just completed.
    pub fn take_parked(&mut self) -> Option<BlockingWait> {
        self.ctl.parked.take()
    }

    /// Whether the dispatch just completed was a resume.
    pub fn in_resume(&self) -> bool {
        self.in_resume
    }

    /// Whether the next dispatch is a resume: what the wait produced is
    /// waiting for it.
    pub fn resuming(&self) -> bool {
        self.pending.is_some()
    }

    /// Clear the resume flag after the guard has fired.
    pub fn clear_resume(&mut self) {
        self.in_resume = false;
    }

    /// Store what the wait produced, for the resume dispatch to pick up.
    pub fn set_pending(&mut self, delivery: Delivery, state: Option<BlockState>) {
        self.pending = Some((delivery, state));
    }
}

/// Outcome of asking to park.
pub enum ParkOutcome {
    /// Parked. The handler must return immediately; its real reply is produced
    /// by the resume dispatch. The value it returns is discarded.
    Parked,
    /// The caller cannot await (Lua / `EXEC` / replica applier). The handler
    /// must return its timed-out reply now, exactly as Redis does.
    CannotWait,
}

/// Convert a `timeout` in seconds — where `0` means "forever", per Redis — into
/// the optional `Duration` [`BlockingWait`] wants.
#[inline]
pub fn timeout_from_secs(secs: f64) -> Option<Duration> {
    if secs <= 0.0 {
        None
    } else {
        Some(Duration::from_secs_f64(secs))
    }
}

/// Same, for the millisecond timeouts `XREAD BLOCK` / `XREADGROUP BLOCK` take.
#[inline]
pub fn timeout_from_ms(ms: u64) -> Option<Duration> {
    if ms == 0 {
        None
    } else {
        Some(Duration::from_millis(ms))
    }
}

/// Hand registered waiters to the connection loop, or report that this caller
/// cannot wait — with state to hand back on resume, if the command has any.
/// See [`BlockState`].
///
/// On [`ParkOutcome::CannotWait`] the receivers are dropped here, which cancels
/// the registrations — the handler must not try to use them.
pub fn park_with_state(
    ctx: &mut crate::commands::ctx::Ctx,
    rxs: Vec<(String, oneshot::Receiver<Value>)>,
    timeout: Option<Duration>,
    state: Option<BlockState>,
) -> ParkOutcome {
    match ctx.blocking.as_mut() {
        Some(slot) if slot.awaitable => {
            slot.parked = Some(BlockingWait {
                rxs,
                timeout,
                state,
            });
            ParkOutcome::Parked
        }
        _ => {
            drop(rxs);
            ParkOutcome::CannotWait
        }
    }
}

/// Run a command that waits until one of `keys` has something for it.
///
/// `attempt` is the command's non-blocking form: `Some(reply)` once it has
/// done its work (or failed), `None` while there is nothing for it. `empty`
/// is the reply for a wait that ends with nothing.
///
/// The command is woken by a *signal* that one of its keys was written, and
/// then looks for itself, exactly as it looked the first time. It used to be
/// handed the element instead, by the push that was adding it — which meant:
///
///   * it got whichever element was pushed first rather than the one its own
///     rules pick, so `BRPOP` against `RPUSH k a b` returned `a`, and
///     `BZPOPMIN` the first member added rather than the lowest;
///   * the element never entered the list on the master, while the replica —
///     sent the push and then the pop — removed a different one, and the two
///     ended up holding different data;
///   * only a push could wake it: a list that arrived by `RENAME`, `COPY`,
///     `RESTORE` or `SORT … STORE` left the client blocked.
///
/// Clients blocked on the same key are woken one at a time, in the order
/// they blocked (see [`crate::wait_table::WaitTable::enqueue`]), and each
/// passes the turn to the one behind it once it has looked. A wake-up is not
/// a promise — a client that was not blocked may get there first — so the
/// command may wait again: against its original deadline, and in its
/// original place.
///
/// `attempt` is given the keys it may take from, in the command's order:
/// all of them, but for those whose elements belong to a client that has
/// been waiting longer (see below).
pub fn wait_for_keys(
    ctx: &mut crate::commands::ctx::Ctx,
    keys: &[String],
    timeout: Option<Duration>,
    mut attempt: impl FnMut(&mut crate::commands::ctx::Ctx, &[String]) -> Option<Vec<u8>>,
    empty: impl Fn() -> Vec<u8>,
) -> Vec<u8> {
    let resumed = take_resume(ctx);
    let (deadline, ticket) = match take_state(ctx) {
        Some(BlockState::Queued { deadline, ticket }) => (deadline, ticket),
        _ => (
            timeout.map(|timeout| Instant::now() + timeout),
            ctx.wait_table.new_ticket(),
        ),
    };
    let timed_out = matches!(resumed, Some(None));
    // The key this client was woken for, if it was.
    let woken_for: Option<String> = match &resumed {
        Some(Some((key, _))) => Some(key.clone()),
        _ => None,
    };

    // Stand in line before looking, so a write that lands between the look
    // and the park still wakes us.
    let rxs: Vec<(String, oneshot::Receiver<Value>)> = if timed_out {
        Vec::new()
    } else {
        keys.iter()
            .map(|key| (key.clone(), ctx.wait_table.enqueue(key, &ticket)))
            .collect()
    };
    // On the first look a key of the wrong type is an error. Found on
    // being woken — the key it was waiting on has been replaced by
    // something else — it is not: Redis leaves such a client blocked, and
    // so does this.
    //
    // That is the key it is waiting on. An error about another key — the
    // destination of a `BLMOVE` — is an error whenever it is found, and the
    // command says so by marking the reply (`definite`).
    let woken = resumed.is_some();
    // A key that has elements *and* a client that has been waiting for them
    // longer than this one is not this client's to take from: the elements
    // are theirs. The push has woken the one at the head of the line, which
    // has not got to them yet, and taking them now would be jumping it.
    //
    // That is so for a client that arrives — `LPUSH k v` and `BLPOP k 0`
    // sent together, to a key another client is blocked on — and for one
    // that is woken for *another* of its keys: blocked on `k2 k3`, and woken
    // because `k3` was pushed to, it used to look at `k2` first, find the
    // element meant for the client ahead of it there, and take it — leaving
    // that client blocked on an empty list, and the element in `k3` with
    // nobody to take it.
    //
    // The key it was woken for is its own to look at: it is at the head of
    // that line, or those ahead of it have looked and passed the turn on.
    // On the others it waits its turn, which comes when those ahead have
    // looked — and they are made to look.
    let can_wait = ctx.blocking.is_some() && !timed_out && remaining_time(deadline).is_some();
    let open: Vec<String> = keys
        .iter()
        .filter(|key| {
            let theirs = can_wait
                && woken_for.as_deref() != Some(key.as_str())
                && ctx.wait_table.someone_ahead(key, &ticket)
                && has_elements(ctx, key);
            if theirs {
                ctx.wait_table.wake_head(key);
            }
            !theirs
        })
        .cloned()
        .collect();
    let attempted = if open.is_empty() {
        None
    } else {
        attempt(ctx, &open)
    };
    let reply = match attempted {
        Some(mut reply) if reply.first() == Some(&DEFINITE) => {
            reply.remove(0);
            Some(reply)
        }
        Some(reply) if woken && reply.starts_with(b"-WRONGTYPE") => None,
        reply => reply,
    };

    // `Some(None)`: wait with no deadline. `None`: the deadline has passed.
    let remaining = remaining_time(deadline);
    let outcome = match (reply, remaining) {
        (Some(reply), _) => Ok(reply),
        (None, Some(remaining)) if !timed_out => Err(remaining),
        (None, _) => {
            crate::stats::record_miss();
            Ok(empty())
        }
    };

    // The clients behind this one get their turn when it leaves the line —
    // there may be more than it took — and when it stays but leaves something
    // behind that it could not use (`BLMOVEM … EXACTLY 3` with two elements
    // there). Not when it stays and there is nothing: waking the next client
    // to look at nothing would put two of them in a race for whatever
    // arrives next, and the order they blocked in would be lost.
    let leaving = outcome.is_ok();
    let pass_on = |ctx: &crate::commands::ctx::Ctx, leaving: bool| {
        for key in keys {
            if leaving {
                ctx.wait_table.dequeue(key, &ticket);
                ctx.wait_table.pass_on(key, &ticket);
            } else if has_elements(ctx, key) {
                ctx.wait_table.pass_on(key, &ticket);
            }
        }
    };
    let remaining = match outcome {
        Ok(reply) => {
            drop(rxs);
            pass_on(ctx, leaving);
            return reply;
        }
        Err(remaining) => remaining,
    };
    pass_on(ctx, leaving);
    let state = BlockState::Queued {
        deadline,
        ticket: std::sync::Arc::clone(&ticket),
    };
    match park_with_state(ctx, rxs, remaining, Some(state)) {
        // The reply comes from the dispatch that resumes the command.
        ParkOutcome::Parked => Vec::new(),
        ParkOutcome::CannotWait => {
            for key in keys {
                ctx.wait_table.dequeue(key, &ticket);
            }
            crate::stats::record_miss();
            empty()
        }
    }
}

/// The time left until `deadline`. `Some(None)`: there is no deadline.
/// `None`: it has passed.
pub(crate) fn remaining_time(deadline: Option<Instant>) -> Option<Option<Duration>> {
    match deadline {
        None => Some(None),
        Some(deadline) => {
            let left = deadline.saturating_duration_since(Instant::now());
            (!left.is_zero()).then_some(Some(left))
        }
    }
}

/// Whether `key` holds anything a blocked client could take.
fn has_elements(ctx: &crate::commands::ctx::Ctx, key: &str) -> bool {
    let shard = crate::store::get_shard(&ctx.store[*ctx.selected_db], key);
    crate::read_lock!(shard)
        .get(key)
        .is_some_and(|value| !value.is_empty_collection())
}

/// Take the delivery on a resume dispatch, if this is one.
///
/// A handler calls this immediately after parsing its arguments and *before*
/// its fast path: on resume the fast path must be skipped entirely, because the
/// delivered element was handed off out-of-band and is not in the keyspace.
/// Running the fast path first could return a *different* element and silently
/// drop the delivered one.
#[inline]
pub fn take_resume(ctx: &mut crate::commands::ctx::Ctx) -> Option<Delivery> {
    ctx.blocking.as_mut().and_then(|slot| slot.resume.take())
}

/// Whether this dispatch is the one that resumes a command that waited.
#[inline]
pub fn resuming(ctx: &crate::commands::ctx::Ctx) -> bool {
    ctx.blocking
        .as_ref()
        .is_some_and(|slot| slot.resume.is_some())
}

/// Take back whatever this command stashed with [`park_with_state`].
#[inline]
pub fn take_state(ctx: &mut crate::commands::ctx::Ctx) -> Option<BlockState> {
    ctx.blocking.as_mut().and_then(|slot| slot.state.take())
}

/// Whether the handler asked to park during this dispatch.
#[inline]
pub fn parked(ctx: &crate::commands::ctx::Ctx) -> bool {
    ctx.blocking
        .as_ref()
        .is_some_and(|slot| slot.parked.is_some())
}

impl BlockingWait {
    /// Take the state stashed at park time, so the loop can hand it to the
    /// resume dispatch. Called before [`wait`](Self::wait), which consumes self.
    pub fn take_state(&mut self) -> Option<BlockState> {
        self.state.take()
    }

    /// Whether it ends by itself at a time: `clients_in_timeout_table`.
    pub fn has_timeout(&self) -> bool {
        self.timeout.is_some()
    }

    /// Whether parking this from a resume dispatch is intended rather than a
    /// handler that forgot its resume check. Each such park awaits a new
    /// signal, so it cannot spin.
    pub fn may_repark(&self) -> bool {
        matches!(
            self.state,
            Some(
                BlockState::Queued { .. }
                    | BlockState::TsRead { .. }
                    | BlockState::GroupRead { .. }
                    | BlockState::StreamIds { .. }
            )
        )
    }

    /// Await the first delivery, or the timeout.
    ///
    /// This is the code that used to live inside `block_in_place(|| rt.block_on(…))`
    /// at ten separate call sites, moved verbatim into async context: each
    /// receiver is raced in its own task and the first to fire wins. Keeping the
    /// mechanism identical is deliberate — the change being made here is *where*
    /// the wait happens, not how it resolves.
    pub async fn wait(self) -> Delivery {
        // Counted for as long as this future lives, and uncounted when it is
        // dropped — which it is, without finishing, when the client goes
        // away or is unblocked while it waits.
        struct Counted;
        impl Drop for Counted {
            fn drop(&mut self) {
                crate::stats::BLOCKED_CLIENTS.fetch_sub(1, Ordering::Relaxed);
            }
        }
        crate::stats::BLOCKED_CLIENTS.fetch_add(1, Ordering::Relaxed);
        let _counted = Counted;
        self.race().await
    }

    async fn race(self) -> Delivery {
        let timeout = self.timeout;
        let (winner_tx, mut winner_rx) = tokio::sync::mpsc::channel(1);
        let mut handles = Vec::with_capacity(self.rxs.len());

        for (key, rx) in self.rxs {
            let tx = winner_tx.clone();
            handles.push(tokio::spawn(async move {
                let got = match timeout {
                    None => rx.await.ok(),
                    Some(d) => tokio::time::timeout(d, rx).await.ok().and_then(|r| r.ok()),
                };
                if let Some(val) = got {
                    let _ = tx.send((key, val)).await;
                }
            }));
        }
        // Dropping our own sender is what lets `recv()` return `None` once every
        // racing task has finished — which, when they all timed out, is the
        // timeout signal itself.
        drop(winner_tx);

        // Stopped when this future ends, however it ends: a task left
        // running would go on holding its place among a key's waiters for a
        // client that is no longer waiting.
        struct Racers(Vec<tokio::task::JoinHandle<()>>);
        impl Drop for Racers {
            fn drop(&mut self) {
                for handle in &self.0 {
                    handle.abort();
                }
            }
        }
        let _racers = Racers(handles);
        winner_rx.recv().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn wait_on(
        rxs: Vec<(String, oneshot::Receiver<Value>)>,
        timeout: Option<Duration>,
    ) -> Delivery {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(
            BlockingWait {
                rxs,
                timeout,
                state: None,
            }
            .wait(),
        )
    }

    /// The whole point of the refactor: the wait must resolve on a
    /// **current-thread** runtime, which is what `tokio_uring` gives us and what
    /// `block_in_place` panicked on.
    #[test]
    fn resolves_on_a_current_thread_runtime() {
        let (tx, rx) = oneshot::channel();
        let val: Value = Arc::from(b"hello".as_slice());
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            let _ = tx.send(val);
        });
        let got = wait_on(vec![("k".to_string(), rx)], None);
        let (key, val) = got.expect("delivery must arrive, not time out");
        assert_eq!(key, "k");
        assert_eq!(val.as_ref(), b"hello");
    }

    #[test]
    fn timeout_yields_no_delivery() {
        let (_tx, rx) = oneshot::channel();
        // `_tx` is held alive, so the only way out is the timeout.
        let got = wait_on(vec![("k".to_string(), rx)], Some(Duration::from_millis(30)));
        assert!(got.is_none(), "an elapsed timeout must produce no delivery");
    }

    /// Multi-key `BLPOP k1 k2 k3` must report *which* key fired, since that key
    /// is the first element of the reply.
    #[test]
    fn first_key_to_fire_wins_and_is_reported() {
        let (_tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();
        let (_tx3, rx3) = oneshot::channel();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            let _ = tx2.send(Arc::from(b"v".as_slice()) as Value);
        });
        let got = wait_on(
            vec![
                ("k1".to_string(), rx1),
                ("k2".to_string(), rx2),
                ("k3".to_string(), rx3),
            ],
            None,
        );
        assert_eq!(got.expect("k2 fired").0, "k2");
    }

    /// Every registration being cancelled must not hang a `timeout 0` wait
    /// forever with no way out.
    #[test]
    fn all_senders_dropped_resolves_rather_than_hanging() {
        let (tx, rx) = oneshot::channel::<Value>();
        drop(tx);
        let got = wait_on(vec![("k".to_string(), rx)], None);
        assert!(got.is_none());
    }
}
