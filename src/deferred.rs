//! Long work moved off an io_uring worker, its connection waiting for it (#762).
//!
//! On the io_uring I/O model each worker thread has its own ring and serves
//! its own connections. A command that runs for a long time on it (`KEYS`
//! over millions of keys, the free of a hash of millions of fields after
//! `DEL`) used to hold up every other connection of that thread for as
//! long: about one connection in N. `helpers::long_work` hands the Tokio
//! runtime's other tasks to another thread meanwhile (#754); a ring thread
//! has nobody to hand its connections to.
//!
//! So the work is moved instead, to a small pool of threads of its own, and
//! the connection waits for it, asleep (`server_uring`), while its thread
//! serves the others. The connection reads its next command, and writes the
//! reply, only once the work is done: each connection's replies keep their
//! order. Two kinds of work move:
//!
//! - A command that only reads, and finds it is long before it has done
//!   anything ([`run_elsewhere`]): `KEYS`, `SCAN` with a huge `COUNT`,
//!   `HGETALL`, `SMEMBERS`, `LRANGE` and `SORT` without `STORE` over
//!   millions of elements. It stops, and is run again whole on the pool,
//!   with its connection's state, as a script that is sent to a thread of
//!   its own runs. What it is counted and timed as (`INFO commandstats`,
//!   `LATENCY`) is that run; the slow log has the whole wait.
//! - The free of a big value `DEL` let go of, of one key or several
//!   ([`free_weighed`]). The write is done, and
//!   propagated, where it ran, as before: only where the memory is freed
//!   changed. The reply waits for the free, as Redis frees before it
//!   replies (`lazyfree-lazy-user-del no`). On the Tokio I/O model the free
//!   runs the same way, out of the command's gates, as other long work does
//!   (`helpers::long_work`).
//!
//! Neither moves inside `MULTI`/`EXEC`, a script, or a command that holds
//! the command gate alone (a read that does), which have no connection to
//! wait (`Ctx::blocking` is `None`, or the gate is held): those run where
//! they are, as before. A read that names a key does not move in cluster
//! mode: it would be routed again, its `ASKING` already used up.

use crate::commands::blocking::BlockingCtl;
use crate::commands::ctx::{AofSender, Ctx};
use crate::cpus::BuilderExt as _;
use crate::types::KiviDBValue;
use std::cell::Cell;
use std::collections::VecDeque;
use std::time::Duration;

thread_local! {
    /// Whether this thread is an io_uring worker: its connections wait for
    /// work sent to the pool.
    static RING: Cell<bool> = const { Cell::new(false) };
    /// Whether this thread is running a command sent to the pool.
    static ELSEWHERE: Cell<bool> = const { Cell::new(false) };
}

/// Called once by each io_uring worker thread, before it serves anyone.
pub fn mark_ring_thread() {
    RING.with(|ring| ring.set(true));
}

fn on_ring_thread() -> bool {
    RING.with(|ring| ring.get())
}

/// Whether the command this thread runs was sent here to be run again
/// ([`run_elsewhere`]): it was shown to `MONITOR` when it arrived.
pub fn running_elsewhere() -> bool {
    ELSEWHERE.with(|elsewhere| elsewhere.get())
}

// ── The pool ────────────────────────────────────────────────────────────────

type Job = Box<dyn FnOnce() + Send>;

struct Pool {
    queue: parking_lot::Mutex<Queue>,
    ready: parking_lot::Condvar,
}

struct Queue {
    jobs: VecDeque<Job>,
    /// Threads started, and of them those waiting for a job.
    threads: usize,
    idle: usize,
}

/// At most one thread for each CPU, started as they are needed and kept:
/// a thread that runs commands takes a slot of the command gate and of the
/// write gate for good (#757), so they are not made anew.
fn max_threads() -> usize {
    std::thread::available_parallelism().map_or(2, |n| n.get().max(2))
}

fn pool() -> &'static Pool {
    static POOL: std::sync::OnceLock<Pool> = std::sync::OnceLock::new();
    POOL.get_or_init(|| Pool {
        queue: parking_lot::Mutex::new(Queue {
            jobs: VecDeque::new(),
            threads: 0,
            idle: 0,
        }),
        ready: parking_lot::Condvar::new(),
    })
}

impl Pool {
    fn submit(&'static self, job: Job) {
        let mut queue = self.queue.lock();
        queue.jobs.push_back(job);
        if queue.idle > 0 {
            drop(queue);
            self.ready.notify_one();
            return;
        }
        if queue.threads < max_threads() {
            let started = std::thread::Builder::new()
                .name("kividb-long".into())
                .spawn_released(move || self.work());
            match started {
                Ok(_) => queue.threads += 1,
                // The job waits for a thread that is busy, if there is one.
                Err(e) if queue.threads > 0 => {
                    crate::log_warn!("could not start a thread for long commands: {e}");
                }
                // None: it runs here, as it would have without the pool.
                Err(e) => {
                    crate::log_warn!("could not start a thread for long commands: {e}");
                    let job = queue.jobs.pop_back().expect("pushed above");
                    drop(queue);
                    job();
                }
            }
        }
    }

    fn work(&self) {
        let mut queue = self.queue.lock();
        loop {
            match queue.jobs.pop_front() {
                Some(job) => {
                    drop(queue);
                    // A panic is the command's, and its connection is told
                    // (the reply's sender is dropped): the thread goes on.
                    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
                    queue = self.queue.lock();
                }
                None => {
                    queue.idle += 1;
                    self.ready.wait(&mut queue);
                    queue.idle -= 1;
                }
            }
        }
    }
}

/// Run `f` on the pool. What it returns comes back through the receiver;
/// if it panicked, the receiver says so.
pub fn spawn<R: Send + 'static>(
    f: impl FnOnce() -> R + Send + 'static,
) -> tokio::sync::oneshot::Receiver<R> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    pool().submit(Box::new(move || {
        let _ = tx.send(f());
    }));
    rx
}

// ── What a command hands its connection ────────────────────────────────────

/// Work a command left for its connection to have done elsewhere, and to
/// wait for, before its reply (`BlockingWait::offload`).
pub enum Offload {
    /// Run the command again, whole, on the pool: its reply is that run's.
    /// The command as it was sent.
    Rerun(Vec<Vec<u8>>),
    /// Free these values on the pool: the reply is the command's own.
    Free(Freeing),
}

/// Values a write let go of, to be freed before its reply.
pub struct Freeing {
    values: Vec<KiviDBValue>,
    effort: u64,
    /// What the command is counted as, once the free is done.
    counted: Option<Counted>,
}

/// What a command whose free was waited for is counted as in `INFO
/// commandstats` and timed as for `LATENCY`, with the free: its name, its
/// first argument (for a command with subcommands), and how long it took
/// itself.
pub struct Counted {
    name: String,
    cmd: Vec<Vec<u8>>,
    pub own: Duration,
}

impl Freeing {
    /// Free here and now: on Tokio, as long work (`helpers::long_work`).
    pub fn free_here(self) -> Option<Counted> {
        let Freeing {
            values,
            effort,
            counted,
        } = self;
        crate::commands::helpers::long_work(effort as usize, move || drop(values));
        counted
    }

    /// Free on the pool; the receiver says when it is done.
    pub fn free_elsewhere(self) -> (tokio::sync::oneshot::Receiver<()>, Option<Counted>) {
        let Freeing {
            values, counted, ..
        } = self;
        (spawn(move || drop(values)), counted)
    }
}

/// On the Tokio I/O model, what a command left for its connection: values
/// to free, freed here as long work (`helpers::long_work`), and the command
/// counted with the free. A command is sent to be run again only from an
/// io_uring worker ([`run_elsewhere`]); should one come here, it is told it
/// could not be run.
pub fn finish_on_tokio(
    offload: Offload,
    latency: &crate::latency::LatencyTracker,
    response: &mut Vec<u8>,
) {
    match offload {
        Offload::Free(freeing) => {
            let started = std::time::Instant::now();
            let counted = freeing.free_here();
            let own = counted
                .as_ref()
                .map_or(Duration::ZERO, |counted| counted.own);
            count_freed(latency, counted, response, own + started.elapsed());
        }
        Offload::Rerun(_) => {
            debug_assert!(false, "a command sent to be run again on Tokio");
            *response = crate::resp::write::resp_error("ERR the command could not be run");
        }
    }
}

/// Count a command whose free was waited for ([`Freeing`]), now that it is
/// done: `elapsed` is its own time and the free's.
pub fn count_freed(
    latency: &crate::latency::LatencyTracker,
    counted: Option<Counted>,
    reply: &[u8],
    elapsed: Duration,
) {
    let Some(Counted { name, cmd, .. }) = counted else {
        return;
    };
    let cmd: Vec<&[u8]> = cmd.iter().map(Vec::as_slice).collect();
    crate::latency::note_command(latency, &name, elapsed);
    crate::cmdstats::record(&name, &cmd, reply, elapsed);
}

/// For `execute_command`: the command parked something. If it is only
/// values to free, what it is counted as waits for the free (its name,
/// and how long it took itself, here); true then.
pub(crate) fn counted_after_free(ctx: &mut Ctx, name: &str, elapsed: Duration) -> bool {
    let Some(Offload::Free(freeing)) = ctx
        .blocking
        .as_mut()
        .and_then(|slot| slot.parked.as_mut())
        .and_then(|wait| wait.offload_mut())
    else {
        return false;
    };
    let first = ctx.cmd.iter().take(2).map(|arg| arg.to_vec()).collect();
    freeing.counted = Some(Counted {
        name: name.to_string(),
        cmd: first,
        own: elapsed,
    });
    true
}

/// Whether the command running with `blocking` can be sent to the pool,
/// its connection waiting for it: on an io_uring worker, for a client
/// connection's own command (not `EXEC`'s, a script's, a replica's), that
/// does not hold the command gate alone and has parked nothing.
#[inline]
pub fn may_run_elsewhere(blocking: &Option<&mut BlockingCtl>) -> bool {
    blocking
        .as_ref()
        .is_some_and(|slot| slot.awaitable && slot.parked.is_none())
        && on_ring_thread()
        && !crate::command_gate::alone_on_this_thread()
}

/// For a command that only reads, whose work over `units` keys or elements
/// is long (`helpers::LONG_WORK`), and has done nothing yet: whether it is
/// to stop here, to be run again on the pool ([`Offload::Rerun`]). If so,
/// it returns `pause::HELD_IN_GATE`, which `execute_command` takes for a
/// command that has not run: not counted, nor timed. The connection runs it
/// again, and replies with what that run replies.
#[cold]
#[inline(never)]
pub fn run_elsewhere(blocking: &mut Option<&mut BlockingCtl>, cmd: &[&[u8]], units: usize) -> bool {
    if units < crate::commands::helpers::LONG_WORK || !may_run_elsewhere(blocking) {
        return false;
    }
    let Some(slot) = blocking.as_mut() else {
        return false;
    };
    let cmd = cmd.iter().map(|arg| arg.to_vec()).collect();
    slot.parked = Some(crate::commands::blocking::BlockingWait::offload(
        Offload::Rerun(cmd),
    ));
    true
}

/// Free `value`, which a write has let go of, and whose freeing is
/// `effort` ([`crate::lazyfree::effort`]): before the reply, as Redis frees
/// it. If it is long work and the command's connection can wait, the
/// connection frees it after the command, out of its gates — on the pool on
/// io_uring — before it replies ([`Offload::Free`]); else here.
pub fn free_weighed(blocking: &mut Option<&mut BlockingCtl>, value: KiviDBValue, effort: u64) {
    if effort < crate::commands::helpers::LONG_WORK as u64 {
        drop(value);
        return;
    }
    if let Some(slot) = blocking.as_mut().filter(|slot| slot.awaitable) {
        match slot.parked.as_mut() {
            None => {
                slot.parked = Some(crate::commands::blocking::BlockingWait::offload(
                    Offload::Free(Freeing {
                        values: vec![value],
                        effort,
                        counted: None,
                    }),
                ));
                return;
            }
            Some(wait) => {
                if let Some(Offload::Free(freeing)) = wait.offload_mut() {
                    freeing.values.push(value);
                    freeing.effort += effort;
                    return;
                }
            }
        }
    }
    crate::commands::helpers::long_work(effort as usize, move || drop(value));
}

// ── What a command run elsewhere runs with ─────────────────────────────────

/// The AOF queue, as a script run on another thread holds it. Both server
/// paths queue on a tokio channel.
#[derive(Clone)]
pub(crate) enum AofBridge {
    Tokio(tokio::sync::mpsc::UnboundedSender<Vec<u8>>),
}

impl AofSender for AofBridge {
    // Same contract as the inherent impls in `commands::ctx`: a record handed
    // here is never dropped. Writes issued from Lua are as durable as any
    // other; discarding them would tear a hole in the AOF that is invisible
    // until a recovery silently comes back short.
    fn send_aof(&self, data: Vec<u8>) {
        let failed = match self {
            Self::Tokio(tx) => tx.send(data).is_err(),
        };
        if failed {
            crate::aof::record_lost_aof_record();
        } else {
            crate::aof::records_queued();
        }
    }
}

/// What a connection's scripts run with, made once for the connection, and
/// what a command of its sent to the pool runs with ([`run_elsewhere`]).
///
/// Shared, not copied: a script handed to the runner thread takes the
/// connection's env with it, and copying it was twenty-odd reference counts
/// moved, and two strings made, on one thread and let go of on another, for
/// every script (#901).
#[derive(Clone)]
pub(crate) struct ClientExecEnv(std::sync::Arc<ClientExecEnvParts>);

impl std::ops::Deref for ClientExecEnv {
    type Target = ClientExecEnvParts;
    fn deref(&self) -> &ClientExecEnvParts {
        &self.0
    }
}

pub(crate) struct ClientExecEnvParts {
    pub store: crate::store::Store,
    pub lastsave: std::sync::Arc<parking_lot::RwLock<i64>>,
    pub aof_bridge: AofBridge,
    pub config: std::sync::Arc<crate::config::Config>,
    pub started_at: std::sync::Arc<std::time::Instant>,
    pub index_store: crate::vector::IndexStore,
    pub dict_store: crate::vector::DictStore,
    pub wait_table: std::sync::Arc<crate::wait_table::WaitTable>,
    pub ts_index: crate::store::SharedTsIndex,
    pub pubsub: crate::pubsub::PubSubHub,
    pub hub_tx: crate::replication::HubTx,
    pub repl_state: std::sync::Arc<crate::replication::ReplState>,
    pub watch_registry: crate::watch::WatchRegistry,
    pub client_registry: crate::client_registry::ClientRegistry,
    pub acl_store: crate::acl::AclStore,
    pub acl_log: crate::acl::SharedAclLog,
    pub slowlog: crate::slowlog::SharedSlowLog,
    pub latency: crate::latency::SharedLatencyTracker,
    pub compaction_tx: crate::vector::CompactionTx,
    pub cluster_state: std::sync::Arc<crate::cluster::ClusterState>,
    pub monitor_registry: crate::monitor::SharedMonitorRegistry,
    pub tracking_table: crate::tracking::TrackingTable,
    pub peer_addr: String,
    /// Whether the client is connected over IPv6 (#847), worked out once.
    pub peer_ipv6: bool,
    pub client_id: u64,
    /// The connection's `CLIENT NO-TOUCH` flag when this env was made: a
    /// script's own commands take it as their client's flag (`Ctx::resp`'s `RESP_NO_TOUCH`).
    /// A copy, not a borrow: the env outlives the loop local, the flag
    /// cannot be set from inside a script anyway, and the loop remakes the
    /// env when the flag changes (#566).
    #[cfg_attr(not(feature = "lua"), allow(dead_code))]
    pub no_touch: bool,
    pub authenticated_user: String,
}

impl ClientExecEnv {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn snapshot(
        store: &crate::store::Store,
        lastsave: &std::sync::Arc<parking_lot::RwLock<i64>>,
        aof_bridge: AofBridge,
        config: &std::sync::Arc<crate::config::Config>,
        started_at: &std::sync::Arc<std::time::Instant>,
        index_store: &crate::vector::IndexStore,
        dict_store: &crate::vector::DictStore,
        wait_table: &std::sync::Arc<crate::wait_table::WaitTable>,
        ts_index: &crate::store::SharedTsIndex,
        pubsub: &crate::pubsub::PubSubHub,
        hub_tx: &crate::replication::HubTx,
        repl_state: &std::sync::Arc<crate::replication::ReplState>,
        watch_registry: &crate::watch::WatchRegistry,
        client_registry: &crate::client_registry::ClientRegistry,
        acl_store: &crate::acl::AclStore,
        acl_log: &crate::acl::SharedAclLog,
        slowlog: &crate::slowlog::SharedSlowLog,
        latency: &crate::latency::SharedLatencyTracker,
        compaction_tx: &crate::vector::CompactionTx,
        cluster_state: &std::sync::Arc<crate::cluster::ClusterState>,
        monitor_registry: &crate::monitor::SharedMonitorRegistry,
        tracking_table: &crate::tracking::TrackingTable,
        peer_addr: &str,
        client_id: u64,
        no_touch: bool,
        authenticated_user: &str,
    ) -> Self {
        Self(std::sync::Arc::new(ClientExecEnvParts {
            store: store.clone(),
            lastsave: lastsave.clone(),
            aof_bridge,
            config: config.clone(),
            started_at: started_at.clone(),
            index_store: index_store.clone(),
            dict_store: dict_store.clone(),
            wait_table: wait_table.clone(),
            ts_index: ts_index.clone(),
            pubsub: pubsub.clone(),
            hub_tx: hub_tx.clone(),
            repl_state: repl_state.clone(),
            watch_registry: watch_registry.clone(),
            client_registry: client_registry.clone(),
            acl_store: acl_store.clone(),
            acl_log: acl_log.clone(),
            slowlog: slowlog.clone(),
            latency: latency.clone(),
            compaction_tx: compaction_tx.clone(),
            cluster_state: cluster_state.clone(),
            monitor_registry: monitor_registry.clone(),
            tracking_table: tracking_table.clone(),
            peer_addr: peer_addr.to_string(),
            peer_ipv6: crate::cluster::is_ipv6_peer(peer_addr),
            client_id,
            no_touch,
            authenticated_user: authenticated_user.to_string(),
        }))
    }
}

impl ClientExecEnv {
    /// Run `cmd`, which stopped on its connection's thread to be run here
    /// ([`run_elsewhere`]), as that connection would have run it: with its
    /// database, its protocol (`resp`, flags included), and nothing to park
    /// on, so that it runs whole, here. Its reply.
    pub(crate) fn run_sent_here(
        &self,
        cmd: Vec<Vec<u8>>,
        selected_db: usize,
        aof_enabled: bool,
        resp: u8,
    ) -> Vec<u8> {
        struct Back;
        impl Drop for Back {
            fn drop(&mut self) {
                ELSEWHERE.with(|elsewhere| elsewhere.set(false));
            }
        }
        ELSEWHERE.with(|elsewhere| elsewhere.set(true));
        let _back = Back;
        let mut db = selected_db;
        crate::commands::execute_command(Ctx {
            cmd: cmd.iter().map(Vec::as_slice).collect(),
            store: &self.store,
            lastsave: &self.lastsave,
            selected_db: &mut db,
            aof_tx: &self.aof_bridge,
            aof_enabled,
            config: &self.config,
            started_at: &self.started_at,
            index_store: &self.index_store,
            dict_store: &self.dict_store,
            wait_table: &self.wait_table,
            ts_index: &self.ts_index,
            pubsub: &self.pubsub,
            hub_tx: &self.hub_tx,
            repl_state: &self.repl_state,
            watch_registry: self.watch_registry.as_ref(),
            client_id: self.client_id,
            client_registry: &self.client_registry,
            resp,
            acl_store: &self.acl_store,
            acl_log: &self.acl_log,
            slowlog: &self.slowlog,
            latency: &self.latency,
            compaction_tx: &self.compaction_tx,
            cluster: &self.cluster_state,
            bypass_cluster_check: false,
            apply_replication: false,
            skip_write_side_effects: false,
            peer_addr: &self.peer_addr,
            peer_ipv6: self.peer_ipv6,
            monitor_registry: &self.monitor_registry,
            authenticated_user: &self.authenticated_user,
            tracking_table: &self.tracking_table,
            blocking: None,
        })
    }
}
