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

use crate::resp::write::resp_error;
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
    rxs: Vec<(Vec<u8>, oneshot::Receiver<Value>)>,
    /// `None` means "wait forever" (`timeout 0`).
    timeout: Option<Duration>,
    /// Carried across the park and handed back on resume. See [`BlockState`].
    state: Option<BlockState>,
    /// [`role_changes`] when the command parked: see
    /// [`ended_by_role_change`].
    role_epoch: u64,
    /// Not a wait: a write that got past a `CLIENT PAUSE` before it was set
    /// is to be read again and held by the pause ([`hold_for_pause`]).
    held_by_pause: bool,
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
    /// This node has become a replica (`REPLICAOF`, `CLUSTER REPLICATE`, a
    /// failover that demotes it): with [`ROLE_CHANGED`], and the connection
    /// is then closed, as Redis's `disconnectAllBlockedClients` ends every
    /// blocked client.
    RoleChange,
}

/// The reply to a client unblocked with `ERROR`.
pub const UNBLOCKED: &[u8] = b"-UNBLOCKED client unblocked via CLIENT UNBLOCK\r\n";

/// The reply to a client whose wait is ended because this node became a
/// replica. Its connection is closed once this is written (#591). The
/// same reply `WAIT` gives then.
pub const ROLE_CHANGED: &[u8] = crate::replication::UNBLOCKED_ROLE_CHANGE;

/// This node has become a replica (or been pointed at another master):
/// its count of role changes (`ReplState::role_changes`) goes up. Called
/// with the role already changed, before the blocked clients are swept.
///
/// The count is the node's own, not the process's: several servers run in
/// one test process, and a role change of one must end nothing on another.
pub fn note_role_change(repl_state: &crate::replication::ReplState) {
    repl_state.role_changes.fetch_add(1, Ordering::SeqCst);
}

/// How many times this node has become a replica, or been pointed at
/// another master. Read when a command parks, and again once its client is
/// listed as blocked: a role change between the two, which the sweep that
/// ends the blocked clients (`ClientRegistry::end_blocked_for_role_change`)
/// cannot have seen, ends the wait as the sweep would have.
#[inline]
pub fn role_changes(repl_state: &crate::replication::ReplState) -> u64 {
    repl_state.role_changes.load(Ordering::Relaxed)
}

/// Whether a parked wait ends because this node became a replica: the
/// role changed since the command parked (`park_epoch`), or the command is
/// one a replica refuses and this node is a replica now (it was let through
/// by the look at the role just before a demotion). Asked once the client
/// is listed as blocked, and again when the wait ends, however it ended: a
/// wake-up at the moment of the change must not hand the client what it
/// waited for and leave it connected.
pub fn ended_by_role_change(
    park_epoch: u64,
    refused_on_replica: bool,
    repl_state: &crate::replication::ReplState,
) -> bool {
    role_changes(repl_state) != park_epoch || (refused_on_replica && repl_state.is_slave())
}

/// Test builds only: `KIVIDB_TEST_PARK_DELAY_MS` holds every parked command
/// this long before its client is listed as blocked, so that a test can
/// change the role in between.
#[cfg(debug_assertions)]
pub fn test_park_delay() -> Option<Duration> {
    static DELAY: std::sync::OnceLock<Option<Duration>> = std::sync::OnceLock::new();
    *DELAY.get_or_init(|| {
        let ms: u64 = std::env::var("KIVIDB_TEST_PARK_DELAY_MS")
            .ok()?
            .parse()
            .ok()?;
        Some(Duration::from_millis(ms))
    })
}

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
    /// This node became a replica while the command waited: the reply is
    /// [`ROLE_CHANGED`], and the connection is closed after it.
    pub role_changed: bool,
}

/// The timeout of a blocking command, in seconds, as Redis reads one: any
/// float (hexadecimal included), not negative, and not so large that the
/// deadline in milliseconds would overflow.
///
/// Redis reads the number as a `long double` and rounds it up to whole
/// milliseconds, so a timeout too small for an `f64` (`1e-343`) is still a
/// real one and waits 1 ms; only a text that no `long double` can hold
/// (`1e-5000`, `1e5000`) is "not a float". `parse_score` refuses such texts
/// and [`parse_rounded_away_timeout`] sorts them out.
pub fn parse_timeout(raw: &[u8]) -> Result<f64, Vec<u8>> {
    let parsed = crate::commands::zset::parse_score(raw);
    // `parse_score` reads `1e-330` as 0.0 (as `fast_float` does), which is
    // no zero as written: such a text goes the same way as one it refuses.
    let secs = match parsed {
        Some(secs) if secs != 0.0 => secs,
        _ => return parse_rounded_away_timeout(raw, parsed),
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

/// The cut-offs of a `long double`, as a power of ten and the first 22
/// digits: half the smallest subnormal (2^-16446, below which a text rounds
/// to 0 and is no number) and the value where `strtold` overflows (2^16384 -
/// 2^16319). Texts are compared with these to 22 significant digits, not
/// exactly: a text that differs only past the 22nd digit may land on the
/// wrong side.
const LONG_DOUBLE_TINY: (i64, &[u8; DIGITS]) = (-4951, b"1822599765941237301264");
const LONG_DOUBLE_HUGE: (i64, &[u8; DIGITS]) = (4932, b"1189731495357231765053");
const DIGITS: usize = 22;

/// The power of ten of a number's first non-zero digit, and its digits.
type Magnitude = (i64, [u8; DIGITS]);

/// A timeout that `parse_score` did not take or read as 0.0: one that an
/// `f64` rounds to 0 or to infinity, or that is no number at all. `parsed` is
/// what `parse_score` made of it.
fn parse_rounded_away_timeout(raw: &[u8], parsed: Option<f64>) -> Result<f64, Vec<u8>> {
    let not_a_float = || resp_error("ERR timeout is not a float or out of range");
    let Some((negative, magnitude)) = std::str::from_utf8(raw).ok().and_then(split_timeout_parts)
    else {
        // A hexadecimal zero (`0x0`) is a zero all the same.
        return if parsed.is_some() {
            Ok(0.0)
        } else {
            Err(not_a_float())
        };
    };
    // A zero as written is a timeout that never ends, and `parse_score`
    // has read it already.
    let Some(magnitude) = magnitude else {
        return Ok(0.0);
    };
    // Redis tells these apart from a number of any size before it looks at
    // the sign: `-1e-5000` is not a float, but `-1e-343` is negative.
    if magnitude <= (LONG_DOUBLE_TINY.0, *LONG_DOUBLE_TINY.1)
        || magnitude >= (LONG_DOUBLE_HUGE.0, *LONG_DOUBLE_HUGE.1)
    {
        return Err(not_a_float());
    }
    if negative {
        return Err(resp_error("ERR timeout is negative"));
    }
    if magnitude.0 > 0 {
        return Err(resp_error("ERR timeout is out of range"));
    }
    // A tiny timeout, but not zero: the shortest wait there is.
    Ok(0.001)
}

/// Split a decimal number, `[+-]digits[.digits][e[+-]digits]` with at least
/// one digit before the `e`, into its sign and its magnitude: the power of
/// ten of its first non-zero digit and its first [`DIGITS`] digits from
/// there (the rest zeros), or `None` for the magnitude of a zero. `None` if
/// the text is no number. The power saturates, so `1e-99999999999999999999`
/// is read at once.
fn split_timeout_parts(text: &str) -> Option<(bool, Option<Magnitude>)> {
    let (negative, unsigned) = match text.as_bytes().first()? {
        b'-' => (true, &text[1..]),
        b'+' => (false, &text[1..]),
        _ => (false, text),
    };
    let (mantissa, exponent) = match unsigned.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => (mantissa, Some(exponent)),
        None => (unsigned, None),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let mut all = whole.bytes().chain(fraction.bytes()).peekable();
    if all.peek().is_none() || !all.clone().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let exponent = match exponent {
        None => 0,
        Some(exponent) => {
            let (negative, digits) = match exponent.as_bytes().first() {
                Some(b'-') => (true, &exponent[1..]),
                Some(b'+') => (false, &exponent[1..]),
                _ => (false, exponent),
            };
            if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            let value = digits.bytes().fold(0i64, |acc, b| {
                acc.saturating_mul(10)
                    .saturating_add(i64::from(b - b'0'))
                    .min(1 << 40)
            });
            if negative { -value } else { value }
        }
    };
    let zeros = all.clone().take_while(|&b| b == b'0').count();
    if zeros == whole.len() + fraction.len() {
        return Some((negative, None));
    }
    let power = whole.len() as i64 - 1 - zeros as i64 + exponent;
    let mut digits = [b'0'; DIGITS];
    for (slot, b) in digits.iter_mut().zip(all.skip(zeros)) {
        *slot = b;
    }
    Some((negative, Some((power, digits))))
}

/// Mark an error reply as one a woken client is told even though it says
/// WRONGTYPE: see [`wait_for_keys`].
pub fn definite(mut reply: Vec<u8>) -> Vec<u8> {
    reply.insert(0, DEFINITE);
    reply
}

const DEFINITE: u8 = 0x01;

/// How long a reply waits on the clients its command woke. See
/// [`BlockingSlot::settle`].
const WOKEN_BOUND: Duration = Duration::from_millis(100);

/// The wait [`BlockingSlot::settle`] gives a reply.
pub type Settling = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

/// What a resumed blocking command was handed: the key that fired and the value
/// delivered through the handoff, or `None` if the wait timed out.
///
/// `XREAD`/`XREADGROUP` use a zero-length sentinel value (see
/// `WaitTable::notify_stream_waiters`) and only care whether this is `Some`.
pub type Delivery = Option<(Vec<u8>, Value)>;

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
    /// Set with `resume` when the client of the command being resumed went
    /// while it waited: see [`client_gone`]. Only read on a resume.
    pub gone: bool,
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
    /// Delivery waiting to be handed to the resume dispatch, and whether its
    /// client has gone.
    pending: Option<(Delivery, Option<BlockState>, bool)>,
    /// True while the current dispatch *is* a resume. Guards against a handler
    /// that parks again after being resumed, which would spin the loop forever.
    in_resume: bool,
    /// The looks this client owes whoever woke it, from the end of its wait
    /// until it has looked at its keys and either replied or waited again:
    /// they hold their replies until then. Done at the end of the dispatch
    /// that looks. See [`crate::wait_table::Look`].
    looked: Vec<std::sync::Arc<crate::wait_table::Look>>,
    /// The looks of the clients the last dispatch woke. See
    /// [`settle`](Self::settle).
    woken: Vec<std::sync::Arc<crate::wait_table::Look>>,
    /// This connection, to the looks it waits for: one per woken client.
    owner: u64,
    /// Whether the dispatch under way collects the clients it wakes.
    collecting: bool,
}

impl Drop for BlockingSlot {
    /// A connection that ends before its resume dispatch has run still owes
    /// its looks: they are marked done here, so that whoever woke it does
    /// not wait out the bound for a client that has gone.
    fn drop(&mut self) {
        if !self.looked.is_empty() {
            crate::wait_table::all_done(std::mem::take(&mut self.looked), &[]);
        }
    }
}

impl BlockingSlot {
    /// Prepare the slot for the next dispatch and hand out the borrow that
    /// goes into `Ctx`.
    pub fn prime(&mut self) -> &mut BlockingCtl {
        // Nobody stands in any line (nearly always): nothing this command
        // does can wake anyone, and there is nothing to collect.
        self.collecting = crate::wait_table::anyone_in_line();
        if self.collecting {
            self.collect();
        }
        self.ctl.awaitable = true;
        self.ctl.parked = None;
        match self.pending.take() {
            Some((delivery, state, gone)) => {
                self.in_resume = true;
                self.ctl.resume = Some(delivery);
                self.ctl.state = state;
                self.ctl.gone = gone;
            }
            None => {
                self.in_resume = false;
                self.ctl.resume = None;
                self.ctl.state = None;
            }
        }
        &mut self.ctl
    }

    /// Collect what the dispatch about to run wakes, for this connection.
    #[inline(never)]
    fn collect(&mut self) {
        if self.owner == 0 {
            self.owner = new_owner();
        }
        crate::wait_table::collect_woken_for(self.owner);
    }

    /// Take the wait a handler parked during the dispatch just completed.
    ///
    /// Called straight after every dispatch and before anything is awaited:
    /// what it woke is in a thread-local, which another connection on this
    /// thread would overwrite.
    #[inline]
    pub fn take_parked(&mut self) -> Option<BlockingWait> {
        // Looked at in place: moving the wait out of the slot to look at it
        // copied it on every command.
        if self.ctl.parked.is_none() {
            if self.in_resume {
                self.resume_over();
            }
            // Nearly every command: ran while nobody stood in any line,
            // owes nobody a look, and did not wait.
            if !self.collecting && self.looked.is_empty() {
                return None;
            }
            return self.looked_or_waits(None);
        }
        let parked = self.ctl.parked.take();
        self.looked_or_waits(parked)
    }

    /// [`take_parked`](Self::take_parked) for a command that may have woken
    /// someone, owes a look, or waits.
    #[cold]
    fn looked_or_waits(&mut self, parked: Option<BlockingWait>) -> Option<BlockingWait> {
        // After `resume_over`: a refused resume's ticket, dropped there,
        // passes its turn on, and the client that wakes is this dispatch's to
        // wait for, as the client a handler passes the turn to is.
        let collected = if self.collecting {
            crate::wait_table::take_woken()
        } else {
            None
        };
        let mut woken = Vec::new();
        if let Some(collected) = collected {
            woken = collected.woken;
            self.looked.extend(collected.owed);
        }
        if let Some(wait) = &parked {
            // It looked, found nothing and waits again: whoever woke it has
            // its look, unless a write woke it again once it stood back in
            // line (that one it has not seen yet). A command that waits has
            // no reply to hold for those it woke.
            let owed = std::mem::take(&mut self.looked);
            match &wait.state {
                Some(BlockState::Queued { ticket, .. } | BlockState::GroupRead { ticket, .. }) => {
                    ticket.looked_and_waits(owed, &woken)
                }
                _ => crate::wait_table::all_done(owed, &woken),
            }
        } else {
            if !self.looked.is_empty() {
                // It looked: done, with the looks of those it woke, which
                // whoever woke it waits for too. Now, not after its own wait
                // for them, so that two clients that woke each other never
                // wait for each other.
                crate::wait_table::all_done(std::mem::take(&mut self.looked), &woken);
            }
            // Waited for before the next command of this client that reads
            // (`settles_before`), with those of the commands before it.
            self.woken.append(&mut woken);
        }
        parked
    }

    /// Whether the clients the commands before `cmd0` woke must have looked
    /// before it runs: it reads (or is not known to write), and so would see
    /// what they are yet to take. Writes do not wait, and replies are not
    /// held: a producer pipelining `LPUSH`es to queues with blocked workers
    /// would otherwise wait for every worker it woke, push by push or batch
    /// by batch, and push at the rate the workers pop (#823: 1,960 jobs/s
    /// down to 1,240 at 1,000 workers x 270 queues waiting per command, and
    /// 1,400 waiting once per batch). What a client observes after its own
    /// push, with `LLEN`, `INFO` or anything else that reads, comes after
    /// the pops that push caused, as in Redis.
    #[inline]
    pub fn settles_before(&self, cmd0: &[u8]) -> bool {
        !self.woken.is_empty() && !crate::replication::is_write_command_bytes(cmd0)
    }

    /// Forget the looks of the clients woken before that are done: kept
    /// only for the next command that reads, they must not pile up on a
    /// connection that only writes.
    #[inline]
    pub fn forget_looked(&mut self) {
        if !self.woken.is_empty() {
            self.woken.retain(|look| !look.is_done());
        }
    }

    /// Wait for the clients the commands before this one woke to have
    /// looked at their keys, so that what it reads comes after their pops,
    /// as in Redis (#42); a chain (`BRPOPLPUSH a b` woken by a push to `a`,
    /// whose push to `b` wakes `BRPOPLPUSH b c`) is waited for whole.
    ///
    /// Bounded: a woken client cannot always look at once — a paused server
    /// holds its resume dispatch, and `CLIENT KILL` or a timeout can take it
    /// away mid-flight. At the bound the command runs without waiting any
    /// longer, and may not see that client's pop. A hundred milliseconds is
    /// far longer than a dispatch, which only has to finish what it was
    /// already running, and far shorter than any `CLIENT PAUSE` someone
    /// would set. The bound is for all of them together, not for each.
    ///
    /// A client that has gone does not hold it: the looks it owes are done
    /// when its ticket goes, or with the dispatch that leaves its lines.
    ///
    /// `None`, nothing to wait for, is what nearly every command gets. The
    /// wait is boxed, so that it is polled out of line, as `until_gone` is:
    /// the connection's own future is compiled as it was without it.
    #[inline]
    pub fn settle(&mut self) -> Option<Settling> {
        if self.woken.is_empty() {
            return None;
        }
        self.settle_woken()
    }

    #[cold]
    fn settle_woken(&mut self) -> Option<Settling> {
        let woken = std::mem::take(&mut self.woken);
        let deadline = tokio::time::Instant::now() + WOKEN_BOUND;
        Some(Box::pin(crate::wait_table::all_looked(woken, deadline)))
    }

    /// A dispatch that resumed a command is over and did not park again:
    /// what it carried goes now. Its handler took it if it ran; if the
    /// command was refused before that (`OOM`, `READONLY` after a failover),
    /// the place in line it holds goes here, and passes the turn on (see
    /// `wait_table::Ticket`), rather than at the next command or the end of
    /// the batch, which a `WAIT` pipelined behind it can put off for ever.
    #[cold]
    fn resume_over(&mut self) {
        self.ctl.state = None;
        self.ctl.resume = None;
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

    /// The dispatch just completed was sent back to be held by a pause
    /// ([`hold_for_pause`]): the command is dispatched again once the pause
    /// lets it, as it was dispatched this time. A resume is a resume again,
    /// with what its wait produced: the handler had not yet taken it.
    pub fn held_by_pause(&mut self) {
        if self.in_resume {
            if let Some(delivery) = self.ctl.resume.take() {
                self.pending = Some((delivery, self.ctl.state.take(), self.ctl.gone));
            }
            self.in_resume = false;
        }
    }

    /// Clear the resume flag after the guard has fired.
    pub fn clear_resume(&mut self) {
        self.in_resume = false;
    }

    /// Store what the wait produced, for the resume dispatch to pick up.
    pub fn set_pending(&mut self, delivery: Delivery, state: Option<BlockState>) {
        if let Some(BlockState::Queued { ticket, .. } | BlockState::GroupRead { ticket, .. }) =
            &state
        {
            self.looked.extend(ticket.take_looked());
        }
        self.pending = Some((delivery, state, false));
    }

    /// The client went while its command waited. The command is dispatched
    /// once more, but only to leave the line it stood in: see
    /// [`client_gone`]. Whoever woke it, if anyone did, waits for that
    /// dispatch as for any other: it passes the turn on, and the client it
    /// passes it to is the one that takes the element.
    pub fn set_gone(&mut self, state: Option<BlockState>) {
        if let Some(BlockState::Queued { ticket, .. } | BlockState::GroupRead { ticket, .. }) =
            &state
        {
            self.looked.extend(ticket.take_looked());
        }
        self.pending = Some((None, state, true));
    }

    /// The client went before the dispatch that resumes its command, which a
    /// pause held: that dispatch is now only to leave the line (see
    /// [`set_gone`](Self::set_gone)).
    pub fn make_gone(&mut self) {
        if let Some((delivery, _, gone)) = self.pending.as_mut() {
            *delivery = None;
            *gone = true;
        }
    }
}

/// A number for a connection, to the looks it waits for
/// (`wait_table::collect_woken_for`).
#[cold]
fn new_owner() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
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
    rxs: Vec<(Vec<u8>, oneshot::Receiver<Value>)>,
    timeout: Option<Duration>,
    state: Option<BlockState>,
) -> ParkOutcome {
    match ctx.blocking.as_mut() {
        Some(slot) if slot.awaitable => {
            slot.parked = Some(BlockingWait {
                rxs,
                timeout,
                state,
                role_epoch: role_changes(ctx.repl_state),
                held_by_pause: false,
            });
            ParkOutcome::Parked
        }
        _ => {
            drop(rxs);
            ParkOutcome::CannotWait
        }
    }
}

/// The reply of a write that has found, inside the write gate, that a
/// `CLIENT PAUSE` was set after it passed the pause check
/// (`pause::held_in_gate`): it does not run now. A connection that can
/// park a command is asked to read it again, its pause check holding it
/// until the pause ends: parked, with nothing to wait for
/// ([`BlockingWait::held_by_pause`]). Any other caller takes the reply,
/// `pause::HELD_IN_GATE`, the same way: a script, a command on the slow
/// path. It is never sent.
pub fn hold_for_pause(ctx: &mut crate::commands::ctx::Ctx) -> Vec<u8> {
    if let Some(slot) = ctx.blocking.as_mut()
        && slot.awaitable
    {
        slot.parked = Some(BlockingWait {
            rxs: Vec::new(),
            timeout: None,
            state: None,
            role_epoch: 0,
            held_by_pause: true,
        });
    }
    crate::pause::HELD_IN_GATE.to_vec()
}

/// Run a command that waits until one of `keys` has something for it.
///
/// `attempt` is the command's non-blocking form: `Some(reply)` once it has
/// done its work (or failed), `None` while there is nothing for it. `empty`
/// is the reply when the caller cannot wait — inside `MULTI` or a script —
/// and there is nothing: Redis answers `BRPOPLPUSH` and `BLMOVE` there with
/// a null bulk string and the others with a null array. A wait that ends
/// with nothing — its timeout, or `CLIENT UNBLOCK … TIMEOUT` — is answered
/// with a null array whatever the command, as Redis answers every blocking
/// command that times out (#385).
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
    keys: &[Vec<u8>],
    timeout: Option<Duration>,
    mut attempt: impl FnMut(&mut crate::commands::ctx::Ctx, &[Vec<u8>]) -> Option<Vec<u8>>,
    empty: impl Fn() -> Vec<u8>,
) -> Vec<u8> {
    let gone = client_gone(ctx);
    let resumed = take_resume(ctx);
    let (deadline, ticket) = match take_state(ctx) {
        Some(BlockState::Queued { deadline, ticket }) => (deadline, ticket),
        _ => (
            timeout.map(|timeout| Instant::now() + timeout),
            ctx.wait_table.new_ticket(),
        ),
    };
    if gone {
        // Nobody is there to be given anything: out of the line, taking
        // nothing, and the turn to the client behind, which may have been
        // this one's.
        leave_lines(ctx, keys, &ticket);
        return empty();
    }
    let timed_out = matches!(resumed, Some(None));
    // The key this client was woken for, if it was: what the line's
    // receiver is sent (`WaitTable::enqueue_all`).
    let woken_for: Option<Vec<u8>> = match &resumed {
        Some(Some((_, key))) => Some(key.to_vec()),
        _ => None,
    };

    // Stand in line before looking, so a write that lands between the look
    // and the park still wakes us. One receiver for all the keys, which the
    // first of them to be written wakes (#758).
    //
    // Not a caller that cannot wait (`MULTI`, a script, the replica
    // applier): it only looks. Standing in line, it could be the head a
    // push wakes, and then leave without passing the turn on, leaving the
    // client behind it blocked with the element there.
    let may_park = ctx.blocking.as_ref().is_some_and(|slot| slot.awaitable);
    let rxs: Vec<(Vec<u8>, oneshot::Receiver<Value>)> = if timed_out || !may_park {
        Vec::new()
    } else {
        vec![(Vec::new(), ctx.wait_table.enqueue_all(keys, &ticket))]
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
    //
    // Whether a key has elements is asked first: it is the cheap question,
    // and of a client blocked on many keys nearly all are empty.
    let theirs = |ctx: &crate::commands::ctx::Ctx, key: &Vec<u8>| {
        let theirs = can_wait
            && woken_for.as_deref() != Some(key.as_slice())
            && has_elements(ctx, key)
            && ctx.wait_table.someone_ahead(key, &ticket);
        if theirs {
            ctx.wait_table.wake_head(key);
        }
        theirs
    };
    let attempted = match keys.iter().position(|key| theirs(&*ctx, key)) {
        // All of them are open to it: no copy of the keys.
        None => attempt(ctx, keys),
        Some(first) => {
            let mut open: Vec<Vec<u8>> = keys[..first].to_vec();
            open.extend(
                keys[first + 1..]
                    .iter()
                    .filter(|key| !theirs(&*ctx, key))
                    .cloned(),
            );
            if open.is_empty() {
                None
            } else {
                attempt(ctx, &open)
            }
        }
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
            // A wait that ran out is answered with a null array. A caller
            // that cannot wait (`MULTI`, a script, the replica applier) whose
            // tiny timeout passed while it looked never waited: it gets the
            // command's own reply, as it does below and as in Redis, which
            // answers there by `CLIENT_DENY_BLOCKING` whatever the timeout.
            Ok(if ctx.blocking.is_some() {
                crate::resp::write::resp_null_array()
            } else {
                empty()
            })
        }
    };

    // The clients behind this one get their turn when it leaves the line —
    // there may be more than it took — and when it stays but leaves something
    // behind that it could not use (`BLMOVEM … EXACTLY 3` with two elements
    // there). Not when it stays and there is nothing: waking the next client
    // to look at nothing would put two of them in a race for whatever
    // arrives next, and the order they blocked in would be lost.
    let remaining = match outcome {
        Ok(reply) => {
            drop(rxs);
            leave_lines(ctx, keys, &ticket);
            return reply;
        }
        Err(remaining) => remaining,
    };
    for key in keys {
        if has_elements(ctx, key) {
            ctx.wait_table.pass_on(key, &ticket);
        }
    }
    let state = BlockState::Queued {
        deadline,
        ticket: std::sync::Arc::clone(&ticket),
    };
    match park_with_state(ctx, rxs, remaining, Some(state)) {
        // The reply comes from the dispatch that resumes the command.
        ParkOutcome::Parked => {
            ticket.parks_in(keys, false);
            Vec::new()
        }
        // It never stood in line (`may_park` above).
        ParkOutcome::CannotWait => {
            crate::stats::record_miss();
            empty()
        }
    }
}

/// A command that waited in the lines for `keys` is over: out of all of
/// them, under one lock, and the turn to the client behind it where the
/// turn may have been this one's — it had been woken there and had not
/// looked, or the key has elements it has left behind.
///
/// Not where the key is empty and the turn was not with it: the client
/// behind would only look, find nothing and wait again. Every client that
/// timed out used to wake the next in each of its lines that way, and with
/// a thousand clients blocked on the same 270 keys each timeout set off a
/// round of a few hundred pointless wake-ups (#758).
fn leave_lines(
    ctx: &crate::commands::ctx::Ctx,
    keys: &[Vec<u8>],
    ticket: &crate::wait_table::Ticket,
) {
    let woken = ctx.wait_table.dequeue_all(keys, ticket);
    ctx.wait_table.left_all(ticket);
    for (key, woken) in keys.iter().zip(woken) {
        if woken || has_elements(ctx, key) {
            ctx.wait_table.pass_on(key, ticket);
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
fn has_elements(ctx: &crate::commands::ctx::Ctx, key: &[u8]) -> bool {
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

/// Whether this dispatch resumes a command whose client went while it
/// waited — closed its connection, or was killed. Asked before
/// [`take_resume`].
///
/// Such a command must take nothing: what it popped, or delivered to its
/// consumer, would be for a client that is not there to be told, and lost
/// (#384). It leaves the line it stood in and gives the turn to the client
/// behind it, and the connection is then closed.
#[inline]
pub fn client_gone(ctx: &crate::commands::ctx::Ctx) -> bool {
    ctx.blocking
        .as_ref()
        .is_some_and(|slot| slot.resume.is_some() && slot.gone)
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

    /// [`role_changes`] when the command parked.
    pub fn role_epoch(&self) -> u64 {
        self.role_epoch
    }

    /// Whether this is no wait, but a write sent back to be held by a
    /// pause ([`hold_for_pause`]): the connection reads the command again,
    /// as it is, and its pause check holds it.
    pub fn held_by_pause(&self) -> bool {
        self.held_by_pause
    }

    /// Whether the command is one a replica refuses (`READONLY`): the
    /// blocking list and sorted-set commands and `XREADGROUP`, which write.
    /// `XREAD BLOCK` and `TS.READ BLOCK` may wait on a replica. Asked before
    /// [`take_state`](Self::take_state).
    pub fn refused_on_replica(&self) -> bool {
        matches!(
            self.state,
            Some(BlockState::Queued { .. } | BlockState::GroupRead { .. })
        )
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

    /// The receivers are raced in this future, under one timer: the first
    /// to be sent something wins, and a receiver whose sender has gone drops
    /// out. With none left, or the time up, there is no delivery. Each
    /// receiver used to be raced by a task of its own, with a timer of its
    /// own, which for `XREAD BLOCK` over many streams (and, before #758 gave
    /// a client in the lines for many keys a single receiver, `BRPOP` over
    /// many keys) was a task and a timer per key per wait.
    async fn race(self) -> Delivery {
        let mut rxs: Vec<(Vec<u8>, Option<oneshot::Receiver<Value>>)> = self
            .rxs
            .into_iter()
            .map(|(key, rx)| (key, Some(rx)))
            .collect();
        let first = std::future::poll_fn(move |cx| {
            let mut waiting = false;
            for (key, slot) in rxs.iter_mut() {
                let Some(rx) = slot.as_mut() else {
                    continue;
                };
                match std::future::Future::poll(std::pin::Pin::new(rx), cx) {
                    std::task::Poll::Ready(Ok(value)) => {
                        return std::task::Poll::Ready(Some((std::mem::take(key), value)));
                    }
                    std::task::Poll::Ready(Err(_)) => *slot = None,
                    std::task::Poll::Pending => waiting = true,
                }
            }
            if waiting {
                std::task::Poll::Pending
            } else {
                std::task::Poll::Ready(None)
            }
        });
        match self.timeout {
            None => first.await,
            Some(timeout) => tokio::time::timeout(timeout, first).await.ok().flatten(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn wait_on(
        rxs: Vec<(Vec<u8>, oneshot::Receiver<Value>)>,
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
                role_epoch: 0,
                held_by_pause: false,
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
        let got = wait_on(vec![(b"k".to_vec(), rx)], None);
        let (key, val) = got.expect("delivery must arrive, not time out");
        assert_eq!(key, b"k");
        assert_eq!(val.as_ref(), b"hello");
    }

    #[test]
    fn timeout_yields_no_delivery() {
        let (_tx, rx) = oneshot::channel();
        // `_tx` is held alive, so the only way out is the timeout.
        let got = wait_on(vec![(b"k".to_vec(), rx)], Some(Duration::from_millis(30)));
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
                (b"k1".to_vec(), rx1),
                (b"k2".to_vec(), rx2),
                (b"k3".to_vec(), rx3),
            ],
            None,
        );
        assert_eq!(got.expect("k2 fired").0, b"k2");
    }

    /// A woken client that never looks at its keys — paused, killed, or gone —
    /// must not hold the reply past the bound: for all of them together.
    /// Looks that name each other end.
    #[test]
    fn a_woken_client_that_never_looks_holds_the_reply_only_to_the_bound() {
        use crate::wait_table::Look;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let woken = Look::new_for_tests(5);
        // The slot has a `Drop`, so it cannot be built with `..Default::default()`.
        #[allow(clippy::field_reassign_with_default)]
        let mut slot = BlockingSlot::default();
        slot.woken = woken.clone();
        let started = Instant::now();
        rt.block_on(slot.settle().expect("something to wait for"));
        let waited = started.elapsed();
        assert!(waited >= WOKEN_BOUND, "gave up early: {waited:?}");
        assert!(
            waited < WOKEN_BOUND * 3,
            "waited past the bound: {waited:?}"
        );
        // Done, and done with each other: no wait.
        woken[0].done(&woken[1..]);
        for (n, look) in woken.iter().enumerate().skip(1) {
            look.done(&woken[..n]);
        }
        // The slot has a `Drop`, so it cannot be built with `..Default::default()`.
        #[allow(clippy::field_reassign_with_default)]
        let mut slot = BlockingSlot::default();
        slot.woken = woken;
        let started = Instant::now();
        rt.block_on(slot.settle().expect("something to wait for"));
        assert!(started.elapsed() < WOKEN_BOUND / 2);
        // Nothing woken, nothing to wait for.
        assert!(BlockingSlot::default().settle().is_none());
    }

    /// Every registration being cancelled must not hang a `timeout 0` wait
    /// forever with no way out.
    #[test]
    fn all_senders_dropped_resolves_rather_than_hanging() {
        let (tx, rx) = oneshot::channel::<Value>();
        drop(tx);
        let got = wait_on(vec![(b"k".to_vec(), rx)], None);
        assert!(got.is_none());
    }
}

/// A command resumed for a client that went while it waited, with what it
/// was woken for there (#384): it takes nothing — the element stays, nothing
/// is delivered to the consumer — and it passes its turn to the client
/// behind it. Run through `execute_command`, as the connection loops run it.
#[cfg(test)]
mod gone_resume_tests {
    use super::*;
    use crate::commands::ctx::{Ctx, NoopAofSender};
    use std::sync::Arc;

    type Run<'r> = &'r dyn Fn(&[&str], Option<&mut BlockingCtl>) -> Vec<u8>;
    /// What gives the key an element, the command, what reads the key, and
    /// what it must read.
    type Case<'c> = (&'c [&'c str], &'c [&'c str], &'c [&'c str], &'c [u8]);

    /// A server's shared state, and `run`, which dispatches one command on it.
    fn with_server(test: impl FnOnce(Run<'_>, &Arc<crate::wait_table::WaitTable>)) {
        // The pause is the process's: a unit test pausing writes would hold
        // these commands back (`pause::held_in_gate`).
        let _pause = crate::pause::TEST_LOCK.lock();
        let store = crate::store::new_store(1);
        let lastsave = Arc::new(parking_lot::RwLock::new(0i64));
        let (hub_tx, _hub_rx) = tokio::sync::mpsc::unbounded_channel();
        let config = Arc::new(crate::config::Config::default());
        let started_at = Instant::now();
        let index_store = crate::vector::new_index_store();
        let dict_store = crate::vector::new_dict_store();
        let wait_table = Arc::new(crate::wait_table::WaitTable::new());
        let ts_index = crate::store::new_ts_index();
        let pubsub = crate::pubsub::PubSubHub::new();
        let repl_state = Arc::new(crate::replication::ReplState::new_master());
        let acl_store = crate::acl::new_acl_store("");
        let acl_log = crate::acl::new_acl_log();
        let slowlog = crate::slowlog::new_slowlog(u64::MAX, 0);
        let latency = crate::latency::new_latency_tracker();
        let (compaction_tx, _compaction_rx) = crate::vector::new_compaction_channel();
        let cluster = Arc::new(crate::cluster::ClusterState::new_disabled());
        let client_registry = crate::client_registry::new_client_registry();
        let monitor_registry = crate::monitor::new_monitor_registry();
        let tracking_table = crate::tracking::new_tracking_table();
        let run = |args: &[&str], blocking: Option<&mut BlockingCtl>| -> Vec<u8> {
            let mut selected_db = 0usize;
            crate::commands::execute_command(Ctx {
                cmd: args.iter().map(|a| a.as_bytes()).collect(),
                store: &store,
                lastsave: &lastsave,
                selected_db: &mut selected_db,
                aof_tx: &NoopAofSender,
                aof_enabled: false,
                config: &config,
                started_at: &started_at,
                index_store: &index_store,
                dict_store: &dict_store,
                wait_table: &wait_table,
                ts_index: &ts_index,
                pubsub: &pubsub,
                hub_tx: &hub_tx,
                repl_state: &repl_state,
                watch_registry: &crate::watch::NoopWatchRegistry,
                client_id: 0,
                client_registry: &client_registry,
                resp: 2,
                acl_store: &acl_store,
                acl_log: &acl_log,
                slowlog: &slowlog,
                latency: &latency,
                compaction_tx: &compaction_tx,
                cluster: &cluster,
                bypass_cluster_check: false,
                apply_replication: false,
                skip_write_side_effects: false,
                peer_addr: "127.0.0.1:1",
                monitor_registry: &monitor_registry,
                authenticated_user: "default",
                tracking_table: &tracking_table,
                blocking,
            })
        };
        test(&run, &wait_table);
    }

    fn with_ctx(test: impl FnOnce(&mut Ctx<'_>)) {
        // `with_server`, handing the test a `Ctx` with no blocking slot, as
        // `MULTI`, a script and the replica applier have.
        // The pause is the process's: a unit test pausing writes would hold
        // these commands back (`pause::held_in_gate`).
        let _pause = crate::pause::TEST_LOCK.lock();
        let store = crate::store::new_store(1);
        let lastsave = Arc::new(parking_lot::RwLock::new(0i64));
        let (hub_tx, _hub_rx) = tokio::sync::mpsc::unbounded_channel();
        let config = Arc::new(crate::config::Config::default());
        let started_at = Instant::now();
        let index_store = crate::vector::new_index_store();
        let dict_store = crate::vector::new_dict_store();
        let wait_table = Arc::new(crate::wait_table::WaitTable::new());
        let ts_index = crate::store::new_ts_index();
        let pubsub = crate::pubsub::PubSubHub::new();
        let repl_state = Arc::new(crate::replication::ReplState::new_master());
        let acl_store = crate::acl::new_acl_store("");
        let acl_log = crate::acl::new_acl_log();
        let slowlog = crate::slowlog::new_slowlog(u64::MAX, 0);
        let latency = crate::latency::new_latency_tracker();
        let (compaction_tx, _compaction_rx) = crate::vector::new_compaction_channel();
        let cluster = Arc::new(crate::cluster::ClusterState::new_disabled());
        let client_registry = crate::client_registry::new_client_registry();
        let monitor_registry = crate::monitor::new_monitor_registry();
        let tracking_table = crate::tracking::new_tracking_table();
        let mut selected_db = 0usize;
        {
            let mut ctx = Ctx {
                cmd: Vec::new(),
                store: &store,
                lastsave: &lastsave,
                selected_db: &mut selected_db,
                aof_tx: &NoopAofSender,
                aof_enabled: false,
                config: &config,
                started_at: &started_at,
                index_store: &index_store,
                dict_store: &dict_store,
                wait_table: &wait_table,
                ts_index: &ts_index,
                pubsub: &pubsub,
                hub_tx: &hub_tx,
                repl_state: &repl_state,
                watch_registry: &crate::watch::NoopWatchRegistry,
                client_id: 0,
                client_registry: &client_registry,
                resp: 2,
                acl_store: &acl_store,
                acl_log: &acl_log,
                slowlog: &slowlog,
                latency: &latency,
                compaction_tx: &compaction_tx,
                cluster: &cluster,
                bypass_cluster_check: false,
                apply_replication: false,
                skip_write_side_effects: false,
                peer_addr: "127.0.0.1:1",
                monitor_registry: &monitor_registry,
                authenticated_user: "default",
                tracking_table: &tracking_table,
                blocking: None,
            };
            test(&mut ctx);
        }
    }

    /// A blocking command that cannot wait (`MULTI`, a script, the replica
    /// applier) only looks: it does not stand in line, so a push that lands
    /// while it looks wakes the client blocked behind it, not it. It used to
    /// stand in line, be the head the push woke, and leave without passing
    /// the turn on: the client behind stayed blocked with the element there.
    #[test]
    fn a_command_that_cannot_wait_does_not_take_a_turn() {
        with_ctx(|ctx| {
            let keys = vec![b"k".to_vec()];
            let table = Arc::clone(ctx.wait_table);
            let mut behind = None;
            let reply = wait_for_keys(
                ctx,
                &keys,
                None,
                |ctx, _| {
                    // A client blocks on the key, and a push to it lands,
                    // while this command looks.
                    let ticket = table.new_ticket();
                    let rx = table.enqueue_all(&keys, &ticket);
                    ticket.parks_in(&keys, false);
                    ctx.wait_table.notify_signal("k");
                    behind = Some((ticket, rx));
                    None
                },
                crate::resp::write::resp_null_array,
            );
            assert_eq!(reply, crate::resp::write::resp_null_array());
            let (_ticket, mut rx) = behind.expect("looked");
            assert_eq!(
                rx.try_recv()
                    .expect("the client behind was not woken")
                    .as_ref(),
                b"k"
            );
        });
    }

    #[tokio::test]
    async fn a_list_or_zset_command_whose_client_went_takes_nothing_and_passes_the_turn() {
        with_server(|run, wait_table| {
            let cases: [Case<'_>; 4] = [
                (
                    &["RPUSH", "l1", "e"],
                    &["BRPOPLPUSH", "l1", "d1", "0"],
                    &["LRANGE", "l1", "0", "-1"],
                    b"*1\r\n$1\r\ne\r\n",
                ),
                (
                    &["RPUSH", "l2", "e"],
                    &["BLMOVE", "l2", "d2", "LEFT", "RIGHT", "0"],
                    &["LRANGE", "l2", "0", "-1"],
                    b"*1\r\n$1\r\ne\r\n",
                ),
                (
                    &["RPUSH", "l3", "e"],
                    &["BLPOP", "l3", "0"],
                    &["LRANGE", "l3", "0", "-1"],
                    b"*1\r\n$1\r\ne\r\n",
                ),
                (
                    &["ZADD", "z", "1", "e"],
                    &["BZPOPMIN", "z", "0"],
                    &["ZRANGE", "z", "0", "-1"],
                    b"*1\r\n$1\r\ne\r\n",
                ),
            ];
            for (push, block, left, want) in cases {
                let key = block[1];
                run(push, None);
                // Two clients in line; the first is woken for the element,
                // and has gone.
                let gone = wait_table.new_ticket();
                let behind = wait_table.new_ticket();
                let _gone_rx = wait_table.enqueue(key, &gone);
                let mut behind_rx = wait_table.enqueue(key, &behind);
                wait_table.notify_signal(key);
                assert!(behind_rx.try_recv().is_err());

                let mut slot = BlockingSlot::default();
                slot.set_gone(Some(BlockState::Queued {
                    deadline: None,
                    ticket: gone,
                }));
                run(block, Some(slot.prime()));
                assert!(slot.take_parked().is_none(), "{block:?} waited again");
                assert_eq!(
                    String::from_utf8_lossy(&run(left, None)),
                    String::from_utf8_lossy(want),
                    "{block:?} took the element for a client that had gone"
                );
                assert!(
                    behind_rx.try_recv().is_ok(),
                    "{block:?} did not pass the turn on"
                );
            }
            assert_eq!(run(&["EXISTS", "d1", "d2"], None), b":0\r\n");
        });
    }

    /// The client woken for an element has gone: whoever woke it has its
    /// look once its gone dispatch has left the line and passed the turn
    /// on, and that look carries the look of the client it passed the turn
    /// to, which takes the element.
    #[tokio::test]
    async fn a_gone_resume_hands_its_waker_the_client_it_passes_to() {
        with_server(|run, wait_table| {
            run(&["RPUSH", "g", "e"], None);
            let gone = wait_table.new_ticket();
            let behind = wait_table.new_ticket();
            let _gone_rx = wait_table.enqueue("g", &gone);
            let mut behind_rx = wait_table.enqueue("g", &behind);
            gone.parks_in(&["g".to_string()], false);
            behind.parks_in(&["g".to_string()], false);
            // The push's dispatch, which wakes the client at the head.
            crate::wait_table::collect_woken();
            wait_table.notify_signal("g");
            let waker = crate::wait_table::take_woken().expect("woke one").woken;
            assert_eq!(waker.len(), 1);
            assert!(!waker[0].is_done());

            let mut slot = BlockingSlot::default();
            slot.set_gone(Some(BlockState::Queued {
                deadline: None,
                ticket: gone,
            }));
            run(&["BLPOP", "g", "0"], Some(slot.prime()));
            assert!(slot.take_parked().is_none());
            assert!(behind_rx.try_recv().is_ok(), "the turn was not passed on");
            assert!(waker[0].is_done(), "the gone client's look is not done");
            // The one behind is waited for (twice, perhaps: the dispatch's
            // write to `g` wakes the head of its line again, and that is the
            // same client, yet to look).
            assert!(!slot.woken.is_empty(), "the one behind is not waited for");
            assert!(slot.woken.iter().all(|look| !look.is_done()));
            drop(behind);
            assert!(
                slot.woken.iter().all(|look| look.is_done()),
                "a client that went owes nothing"
            );
        });
    }

    #[tokio::test]
    async fn an_xreadgroup_whose_client_went_delivers_nothing_and_passes_the_turn() {
        with_server(|run, wait_table| {
            run(&["XGROUP", "CREATE", "s", "g", "$", "MKSTREAM"], None);
            run(&["XADD", "s", "*", "f", "v"], None);
            let gone = wait_table.new_ticket();
            let behind = wait_table.new_ticket();
            let _gone_rx = wait_table.enqueue_reader("s", &gone);
            let mut behind_rx = wait_table.enqueue_reader("s", &behind);
            wait_table.notify_signal("s");
            assert!(behind_rx.try_recv().is_err());

            let mut slot = BlockingSlot::default();
            slot.set_gone(Some(BlockState::GroupRead {
                deadline: None,
                ticket: gone,
            }));
            let reply = run(
                &[
                    "XREADGROUP",
                    "GROUP",
                    "g",
                    "c",
                    "BLOCK",
                    "0",
                    "STREAMS",
                    "s",
                    ">",
                ],
                Some(slot.prime()),
            );
            assert_eq!(reply, b"*-1\r\n");
            assert!(slot.take_parked().is_none());
            let pending = run(&["XPENDING", "s", "g"], None);
            assert!(
                pending.starts_with(b"*4\r\n:0\r\n"),
                "delivered to a consumer that had gone: {}",
                String::from_utf8_lossy(&pending)
            );
            assert!(behind_rx.try_recv().is_ok(), "the turn was not passed on");
        });
    }

    #[test]
    fn parse_timeout_reads_a_long_double() {
        let err = |raw: &[u8]| String::from_utf8(parse_timeout(raw).unwrap_err()).unwrap();
        // Too small for an f64 but not for a long double: the shortest wait.
        for raw in [
            "1e-343",
            "1e-400",
            "+1e-343",
            ".5e-343",
            "5.e-343",
            "1e-4950",
            "1000e-4953",
            "1.83e-4951",
            "3e-4951",
            "1e-330",
            "1e-325",
            "2e-324",
            "0.000001e-320",
        ] {
            assert_eq!(parse_timeout(raw.as_bytes()).unwrap(), 0.001, "{raw}");
        }
        // Zero in any spelling never ends.
        for raw in ["0", "0.0", "0e10", "-0", "-0.0", "-0e10", "0x0"] {
            assert_eq!(parse_timeout(raw.as_bytes()).unwrap(), 0.0, "{raw}");
        }
        for raw in [
            "1e-4951",
            "1.8e-4951",
            "0.001e-4948",
            "1e-5000",
            "-1e-5000",
            "1e-99999999999999999999",
            "1e99999999999999999999",
            "1.2e4932",
            "1e4933",
            "xe-5",
            "1.2.3e-400",
            "1e2e3",
            "e999",
            "1e",
            "1e-",
            ".",
            "",
            " 1",
            "1 ",
            "1abc",
            "--1",
            "nan",
        ] {
            assert_eq!(
                err(raw.as_bytes()),
                "-ERR timeout is not a float or out of range\r\n",
                "{raw}"
            );
        }
        for raw in [
            "inf",
            "+inf",
            "2e308",
            "1e309",
            "1e4000",
            "1.1e4932",
            "1.18973149535723176503e4932",
        ] {
            assert_eq!(
                err(raw.as_bytes()),
                "-ERR timeout is out of range\r\n",
                "{raw}"
            );
        }
        for raw in ["-1", "-inf", "-2e308", "-1e-343", "-1e-4950", "-1e-330"] {
            assert_eq!(err(raw.as_bytes()), "-ERR timeout is negative\r\n", "{raw}");
        }
    }
}
