//! Run Lua VM commands on Tokio's blocking thread pool so core runtime workers keep polling
//! other TCP connections (`SCRIPT KILL`, `-BUSY`, etc.). Long-running `EVAL` on a worker would
//! otherwise starve every command scheduled on that same worker — including other connections'
//! control commands — matching Redis single-threaded behaviour only when Lua does not block async.
//!
//! That protection costs two thread hand-offs a script: to the pool, and back to the worker when
//! it is done. For the script that runs for seconds it is nothing. For the scripts a job queue
//! runs by the thousand — a few `redis.call`s, tens of microseconds — it was most of the time:
//! scripts ran at about a sixteenth of Redis's rate. So a script is offloaded only until it has
//! shown itself to be quick (`FAST_SCRIPTS`): after one run under `PROMOTE_US` it runs on the
//! worker, like any other command, and one run over `DEMOTE_US` there sends it back to the pool.

#[cfg(feature = "lua")]
use crate::commands::ctx::AofSender;
#[cfg(feature = "lua")]
use crate::commands::ctx::Ctx;
#[cfg(feature = "lua")]
use crate::commands::execute_command;

/// The AOF queue, as a script run on another thread holds it. Both server
/// paths queue on a tokio channel.
#[cfg(feature = "lua")]
#[derive(Clone)]
pub(crate) enum AofBridge {
    Tokio(tokio::sync::mpsc::UnboundedSender<Vec<u8>>),
}

#[cfg(feature = "lua")]
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

#[cfg(feature = "lua")]
#[derive(Clone)]
pub(crate) struct ClientExecEnv {
    pub store: crate::store::Store,
    pub expiry_store: crate::store::ExpiryStore,
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
    pub client_id: u64,
    pub authenticated_user: String,
}

#[cfg(feature = "lua")]
impl ClientExecEnv {
    pub(crate) fn run_execute(
        self,
        owned_cmd: Vec<Vec<u8>>,
        selected_db: usize,
        aof_enabled: bool,
    ) -> (Vec<u8>, usize) {
        let cmd_refs: Vec<&[u8]> = owned_cmd.iter().map(|v| v.as_slice()).collect();
        self.run_execute_borrowed(cmd_refs, selected_db, aof_enabled)
    }

    /// [`Self::run_execute`] on the caller's own thread, where the command
    /// can be used where it lies.
    pub(crate) fn run_execute_borrowed(
        &self,
        cmd_refs: Vec<&[u8]>,
        selected_db: usize,
        aof_enabled: bool,
    ) -> (Vec<u8>, usize) {
        let mut db = selected_db;
        let out = execute_command(Ctx {
            cmd: cmd_refs,
            store: &self.store,
            expiry_store: &self.expiry_store,
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
            // Result is converted to a Lua value, never sent over the wire as-is.
            resp: 2,
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
            monitor_registry: &self.monitor_registry,
            authenticated_user: &self.authenticated_user,
            tracking_table: &self.tracking_table,
            blocking: None,
        });
        (out, db)
    }
}

#[cfg(feature = "lua")]
impl ClientExecEnv {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn snapshot(
        store: &crate::store::Store,
        expiry_store: &crate::store::ExpiryStore,
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
        authenticated_user: &str,
    ) -> Self {
        Self {
            store: store.clone(),
            expiry_store: expiry_store.clone(),
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
            client_id,
            authenticated_user: authenticated_user.to_string(),
        }
    }
}

/// Scripts that ran quickly the last time they were offloaded, and so run on
/// the worker thread: by a hash of what names them — the body for `EVAL`, the
/// digest for `EVALSHA`, the function for `FCALL`.
#[cfg(feature = "lua")]
static FAST_SCRIPTS: std::sync::LazyLock<parking_lot::RwLock<ahash::AHashSet<u64>>> =
    std::sync::LazyLock::new(Default::default);

/// An offloaded run that spent less than this in the VM makes its script one
/// that runs inline.
#[cfg(feature = "lua")]
const PROMOTE_US: u64 = 1_000;
/// A run that spent longer than this in the VM sends its script back to the
/// pool: a worker thread held that long is other connections kept waiting.
#[cfg(feature = "lua")]
const DEMOTE_US: u64 = 5_000;
/// Enough for every script of a large application; scripts written out anew
/// for each call (`EVAL` with its arguments pasted into the body) would
/// otherwise grow the set without end, so past this it starts again.
#[cfg(feature = "lua")]
const FAST_SCRIPTS_MAX: usize = 16_384;

/// What a script command is remembered by, or `None` for one that always
/// goes to the pool (`FUNCTION LOAD`).
#[cfg(feature = "lua")]
fn script_key(cmd: &[&[u8]]) -> Option<u64> {
    use std::hash::{BuildHasher, Hash, Hasher};
    static HASHER: std::sync::LazyLock<ahash::RandomState> =
        std::sync::LazyLock::new(ahash::RandomState::new);
    let name = cmd.first()?;
    if name.eq_ignore_ascii_case(b"FUNCTION") {
        return None;
    }
    let mut h = HASHER.build_hasher();
    // EVAL of a body and EVALSHA of its digest are remembered apart, as are
    // a script and a function of one name: each is learned on its own.
    name.len().hash(&mut h);
    cmd.get(1)?.hash(&mut h);
    Some(h.finish())
}

/// One quick script, ready to run on whichever thread gets to it.
#[cfg(feature = "lua")]
struct Job {
    env: ClientExecEnv,
    cmd: Vec<Vec<u8>>,
    db: usize,
    aof_enabled: bool,
    key: u64,
}

/// The reply, the database selected afterwards, and how long it took (µs).
#[cfg(feature = "lua")]
type Done = (Vec<u8>, usize, u64);

#[cfg(feature = "lua")]
fn resp_error(message: &str) -> Vec<u8> {
    format!("-{message}\r\n").into_bytes()
}

#[cfg(feature = "lua")]
impl Job {
    fn run(self) -> Done {
        let (key, db, aof_enabled) = (self.key, self.db, self.aof_enabled);
        let (env, cmd) = (self.env, self.cmd);
        ran_quick(key, db, move || env.run_execute(cmd, db, aof_enabled))
    }
}

/// Run a quick script, by `run`, and see whether it was one.
#[cfg(feature = "lua")]
fn ran_quick(key: u64, db: usize, run: impl FnOnce() -> (Vec<u8>, usize)) -> Done {
    {
        let t0 = std::time::Instant::now();
        // A panic must not take the runner thread, or a worker, with it.
        let ran = std::panic::catch_unwind(std::panic::AssertUnwindSafe(run));
        let elapsed_us = t0.elapsed().as_micros() as u64;
        // By the time it ran for, not the time it took: a quick script that
        // waited for the gate behind other commands is still a quick script.
        // Sent to the pool for that, it would wait there with a thread of its
        // own, next to others doing the same — and the scripts of a busy
        // server would never be quick again.
        if crate::commands::take_last_vm_run_us() > DEMOTE_US {
            FAST_SCRIPTS.write().remove(&key);
        }
        match ran {
            Ok((resp, db)) => (resp, db, elapsed_us),
            Err(_) => (
                resp_error("ERR the script failed unexpectedly"),
                db,
                elapsed_us,
            ),
        }
    }
}

/// Whether a quick script is running, here or on the runner thread: they take
/// turns, so that the command gate — which each of them takes alone — is
/// never fought over by several threads at once.
#[cfg(feature = "lua")]
static SCRIPT_RUNNING: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// The turn to run a quick script, given up when dropped.
#[cfg(feature = "lua")]
struct Turn;

#[cfg(feature = "lua")]
impl Turn {
    fn take() -> Option<Turn> {
        use std::sync::atomic::Ordering;
        SCRIPT_RUNNING
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
            // Not `then_some(Turn)`: that would make a `Turn` either way, and
            // dropping it gives up a turn — somebody else's.
            .then(|| Turn)
    }
}

#[cfg(feature = "lua")]
impl Drop for Turn {
    fn drop(&mut self) {
        SCRIPT_RUNNING.store(false, std::sync::atomic::Ordering::Release);
        // Scripts that arrived meanwhile are the runner's.
        if let Some(runner) = RUNNER.get()
            && runner.waiting.load(std::sync::atomic::Ordering::Acquire) > 0
        {
            runner.thread.unpark();
        }
    }
}

/// The thread that runs quick scripts when more than one is asked for at
/// once: one after another, with nothing to wait for between them.
#[cfg(feature = "lua")]
struct Runner {
    queue:
        parking_lot::Mutex<std::collections::VecDeque<(Job, tokio::sync::oneshot::Sender<Done>)>>,
    /// How many are queued, for the runner to look at without the lock.
    waiting: std::sync::atomic::AtomicUsize,
    thread: std::thread::Thread,
}

#[cfg(feature = "lua")]
static RUNNER: std::sync::OnceLock<&'static Runner> = std::sync::OnceLock::new();

#[cfg(feature = "lua")]
fn runner() -> &'static Runner {
    RUNNER.get_or_init(|| {
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<&'static Runner>();
        let handle = std::thread::Builder::new()
            .name("script-runner".into())
            .spawn(move || {
                let runner = ready_rx.recv().expect("the script runner is given itself");
                runner.run();
            })
            .expect("could not start the script runner thread");
        let runner: &'static Runner = Box::leak(Box::new(Runner {
            queue: Default::default(),
            waiting: Default::default(),
            thread: handle.thread().clone(),
        }));
        let _ = ready_tx.send(runner);
        runner
    })
}

#[cfg(feature = "lua")]
impl Runner {
    fn submit(&self, job: Job, reply: tokio::sync::oneshot::Sender<Done>) {
        use std::sync::atomic::Ordering;
        self.queue.lock().push_back((job, reply));
        self.waiting.fetch_add(1, Ordering::Release);
        self.thread.unpark();
    }

    fn run(&self) {
        use std::sync::atomic::Ordering;
        /// How long to keep looking for work before sleeping: under load
        /// the next script is microseconds away, and a sleeping thread takes
        /// far longer than that to wake.
        const SPIN: std::time::Duration = std::time::Duration::from_micros(100);
        let mut idle_since = std::time::Instant::now();
        loop {
            if self.waiting.load(Ordering::Acquire) == 0 {
                if idle_since.elapsed() < SPIN {
                    std::hint::spin_loop();
                } else {
                    // `unpark` before this `park` makes it return at once.
                    std::thread::park();
                    idle_since = std::time::Instant::now();
                }
                continue;
            }
            // A script running on a worker thread is nearly done: wait for it.
            let Some(turn) = Turn::take() else {
                std::hint::spin_loop();
                continue;
            };
            loop {
                let next = self.queue.lock().pop_front();
                let Some((job, reply)) = next else { break };
                self.waiting.fetch_sub(1, Ordering::Release);
                // The client may have gone; its script has run all the same.
                let _ = reply.send(job.run());
            }
            // Not through `Drop`: that would wake this very thread.
            std::mem::forget(turn);
            SCRIPT_RUNNING.store(false, Ordering::Release);
            idle_since = std::time::Instant::now();
        }
    }
}

#[cfg(feature = "lua")]
pub(crate) async fn run_lua_vm_blocking(
    cmd: &[&[u8]],
    selected_db: &mut usize,
    env: &ClientExecEnv,
    aof_enabled: bool,
) -> (Vec<u8>, u64) {
    let key = script_key(cmd);
    let sel = *selected_db;
    // Copied only to be handed to another thread.
    let owned = || -> Vec<Vec<u8>> { cmd.iter().map(|c| c.to_vec()).collect() };

    if let Some(key) = key
        && FAST_SCRIPTS.read().contains(&key)
    {
        // A script that will run under its tag's lock (`tag_locks`) runs
        // here and now, beside whatever scripts other threads are running:
        // it has no turn to wait for.
        if crate::tag_locks::enabled()
            && let Some(tag) = crate::tag_locks::script_tag(cmd)
        {
            // Its turn among the scripts of its tag is waited for here, by
            // the connection, and not inside the command: while another
            // thread's script runs, this thread serves its other
            // connections — which may be on tags nobody holds.
            let _turn = crate::tag_locks::script_turn(tag).await;
            crate::tag_locks::allow_next_script();
            let (resp, sel_out, elapsed_us) = ran_quick(key, sel, || {
                env.run_execute_borrowed(cmd.to_vec(), sel, aof_enabled)
            });
            // Not left for another script if this one was turned away
            // before it got as far as asking.
            crate::tag_locks::take_next_script_allowed();
            *selected_db = sel_out;
            return (resp, elapsed_us);
        }
        let (resp, sel_out, elapsed_us) = match Turn::take() {
            // No other script is running: run this one here, on the command
            // where it lies.
            Some(turn) => {
                let done = ran_quick(key, sel, || {
                    env.run_execute_borrowed(cmd.to_vec(), sel, aof_enabled)
                });
                drop(turn);
                done
            }
            // One is. Scripts run one at a time, and a worker thread that
            // waited for its turn would be woken for it — a thread hand-off
            // for every script, which is what made scripts slow under load.
            // The runner thread takes them one after another instead, and
            // this connection waits without holding its worker.
            None => {
                let job = Job {
                    env: env.clone(),
                    cmd: owned(),
                    db: sel,
                    aof_enabled,
                    key,
                };
                let (reply, done) = tokio::sync::oneshot::channel();
                runner().submit(job, reply);
                done.await
                    .unwrap_or_else(|_| (resp_error("ERR the script could not be run"), sel, 0))
            }
        };
        *selected_db = sel_out;
        return (resp, elapsed_us);
    }

    let owned = owned();
    let env = env.clone();
    let (resp, sel_out, elapsed_us, ran_us) = tokio::task::spawn_blocking(move || {
        let t0 = std::time::Instant::now();
        let (r, db) = env.run_execute(owned, sel, aof_enabled);
        let elapsed_us = t0.elapsed().as_micros() as u64;
        (r, db, elapsed_us, crate::commands::take_last_vm_run_us())
    })
    .await
    .expect("Lua VM offload thread panicked");
    if ran_us < PROMOTE_US
        && let Some(key) = key
    {
        let mut fast = FAST_SCRIPTS.write();
        if fast.len() >= FAST_SCRIPTS_MAX {
            fast.clear();
        }
        fast.insert(key);
    }
    *selected_db = sel_out;
    (resp, elapsed_us)
}

#[cfg(all(test, feature = "lua"))]
mod tests {
    use super::*;

    #[test]
    fn a_script_is_remembered_by_what_names_it() {
        let key = |cmd: &[&[u8]]| script_key(cmd);
        let eval = key(&[b"EVAL", b"return 1", b"0"]);
        assert!(eval.is_some());
        // The same script, whatever its keys and arguments.
        assert_eq!(eval, key(&[b"eval", b"return 1", b"1", b"k"]));
        assert_ne!(eval, key(&[b"EVAL", b"return 2", b"0"]));
        // A digest is not a body, and a function is not a script.
        assert_ne!(
            key(&[b"EVALSHA", b"abc", b"0"]),
            key(&[b"FCALL", b"abc", b"0"])
        );
        assert_eq!(key(&[b"FUNCTION", b"LOAD", b"#!lua name=x"]), None);
        assert_eq!(key(&[b"EVAL"]), None);
    }
}
