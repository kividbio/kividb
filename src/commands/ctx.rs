use crate::config::Config;
use crate::pubsub::PubSubHub;
use crate::replication::{HubTx, ReplState};
use crate::store::{ExpiryStore, SharedTsIndex, Store};
use crate::vector::{DictStore, IndexStore};
use crate::wait_table::WaitTable;
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Instant;

/// Minimal trait so both the tokio and std AOF queues can be stored in Ctx
/// without making Ctx generic (which would infect every call site).
///
/// **A record handed to this trait must not be dropped.** It used to be
/// `try_send_aof`, implemented as `let _ = self.try_send(data)` on a bounded
/// channel — so once writes outran the AOF writer, records were discarded with
/// no error, no counter and no log line. A durability log that silently skips
/// entries under exactly the load you would want it for is worse than none,
/// because it still looks like it is working.
///
/// The two implementations differ because `Sync` is required here and
/// `std::sync::mpsc::Sender` is not `Sync`, so the std side cannot simply
/// become unbounded:
///
/// * tokio → unbounded; `send` cannot block or fail except on a closed channel.
/// * std → stays a bounded `SyncSender` but blocks instead of dropping. Its
///   consumer is a tight loop forwarding into the unbounded tokio queue, so it
///   drains at memory speed and this should never actually block; if it ever
///   does, throttling the writer is the correct answer for a durability log.
///
/// Either way the only unrecoverable case is a closed channel (shutdown), which
/// is counted in `aof::AOF_LOST_RECORDS` and logged once rather than ignored.
pub trait AofSender: Send + Sync {
    fn send_aof(&self, data: Vec<u8>);
}

impl AofSender for tokio::sync::mpsc::UnboundedSender<Vec<u8>> {
    fn send_aof(&self, data: Vec<u8>) {
        if self.send(data).is_err() {
            crate::aof::record_lost_aof_record();
        } else {
            crate::aof::records_queued();
        }
    }
}

impl AofSender for std::sync::mpsc::SyncSender<Vec<u8>> {
    fn send_aof(&self, data: Vec<u8>) {
        if self.send(data).is_err() {
            crate::aof::record_lost_aof_record();
        } else {
            crate::aof::records_queued();
        }
    }
}

/// Sentinel for contexts that must never append — currently AOF replay, which
/// re-executes the log and would otherwise write every record straight back
/// into the file it is reading.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopAofSender;

impl AofSender for NoopAofSender {
    #[inline(always)]
    fn send_aof(&self, _data: Vec<u8>) {}
}

/// Routes to whichever queue the server installed at startup.
///
/// The replication applier is built deep inside a task that never saw the AOF
/// channel, and which of the two channel types it would be depends on the I/O
/// model. Threading a sender down through `start_replica` →
/// `run_replication_session` → `ReplicationApplyEnv` would mean changing every
/// call site on both paths to fix one field; the writer's other control
/// channels (`FLUSH_TX`, `SWAP_TX`) are already process-global for the same
/// reason, so this follows the pattern already in `aof.rs`.
#[derive(Clone, Copy, Debug, Default)]
pub struct GlobalAofSender;

impl AofSender for GlobalAofSender {
    fn send_aof(&self, data: Vec<u8>) {
        crate::aof::send_to_global_writer(data);
    }
}

pub struct Ctx<'a> {
    pub cmd: Vec<&'a [u8]>,
    pub store: &'a Store,
    pub expiry_store: &'a ExpiryStore,
    /// Not read: `LASTSAVE` is `stats::last_save()`, which every save
    /// moves (#149). Still passed around by the connection loops.
    pub lastsave: &'a Arc<RwLock<i64>>,
    pub selected_db: &'a mut usize,
    pub aof_tx: &'a dyn AofSender,
    pub aof_enabled: bool,
    pub config: &'a Arc<Config>,
    pub started_at: &'a Instant,
    pub index_store: &'a IndexStore,
    pub dict_store: &'a DictStore,
    pub wait_table: &'a Arc<WaitTable>,
    pub ts_index: &'a SharedTsIndex,
    pub pubsub: &'a PubSubHub,
    pub hub_tx: &'a HubTx,
    pub repl_state: &'a Arc<ReplState>,
    pub watch_registry: &'a dyn crate::watch::WatchNotifier,
    pub client_id: u64,
    pub client_registry: &'a crate::client_registry::ClientRegistry,
    /// Negotiated RESP protocol version (2 or 3) for the connection this command
    /// came in on. Threaded in directly by the caller's per-connection loop state
    /// rather than looked up from `client_registry` here: this field is read on
    /// every command (see `execute_command`'s null-encoding rewrite), and
    /// `client_registry` is one lock shared by every worker thread — a per-command
    /// lookup there is cheap in isolation but adds real cross-core cache-line
    /// traffic at full core count. `client_registry` still holds the value too
    /// (kept in sync by the caller) for introspection commands like CLIENT INFO.
    pub resp: u8,
    pub acl_store: &'a crate::acl::AclStore,
    pub acl_log: &'a crate::acl::SharedAclLog,
    pub slowlog: &'a crate::slowlog::SharedSlowLog,
    pub latency: &'a crate::latency::SharedLatencyTracker,
    pub compaction_tx: &'a crate::vector::CompactionTx,
    pub cluster: &'a std::sync::Arc<crate::cluster::ClusterState>,
    /// When true, skip the cluster slot ownership check. Set to true only
    /// by the replica applier — incoming writes from the master are
    /// already-validated, so the replica must apply them regardless of
    /// what it locally thinks the slot ownership is.
    pub bypass_cluster_check: bool,
    /// True only when applying replicated commands from the master stream.
    /// Normal clients attached to a replica must not mutate data (read-only replica).
    pub apply_replication: bool,
    /// SETBIT / BITFIELD no-op: skip replication feed and `rdb_changes_since_last_save` bump.
    pub skip_write_side_effects: bool,
    /// Address of the connected client — used for MONITOR line formatting.
    /// Already available in handle_client as `peer_addr`.
    pub peer_addr: &'a str,
    /// Monitor registry — broadcast target when MONITOR_COUNT > 0.
    pub monitor_registry: &'a crate::monitor::SharedMonitorRegistry,
    /// The username of the currently authenticated user.
    pub authenticated_user: &'a str,
    pub tracking_table: &'a crate::tracking::TrackingTable,
    /// Blocking-command parking slot, owned by the connection loop.
    ///
    /// A *borrow*, not a value: `Ctx` is built and moved on every single
    /// command, and `BlockingCtl` is ~160 bytes (three `Option`s, one holding a
    /// `Vec` of registered waiters) that only blocking commands ever read.
    /// Passing a pointer costs 8 bytes instead (`size_of::<Ctx>()` 280 → 288;
    /// by value it was 440).
    ///
    /// Measured honestly, that size difference is *not* worth a throughput
    /// claim: a controlled A/B — same binary, `Ctx` padded back to 440 bytes,
    /// one server alive at a time, alternating order — put `SET`/`GET` within
    /// ±2%, i.e. noise. The borrow is the better shape on principle (no copy of
    /// state the command will not touch), not because it rescued a benchmark.
    ///
    /// `None` means the caller cannot await a park (Lua, `MULTI`/`EXEC`, the
    /// replica applier), so blocking commands behave as their non-blocking
    /// variants — which is what Redis does inside `MULTI`.
    pub blocking: Option<&'a mut crate::commands::blocking::BlockingCtl>,
}

#[cfg(test)]
mod ctx_size_tests {
    use super::Ctx;

    /// `Ctx` is built and moved on every single command, so its size is a cost
    /// `SET`/`GET` pay as much as anything else. This is a hygiene bound, not a
    /// cliff: a controlled A/B (this struct padded to 440 bytes, one server
    /// alive at a time, alternating order) moved `SET`/`GET` by under 2% —
    /// noise. So the bound is here to stop `Ctx` quietly accumulating
    /// pass-by-value state, not because 320 is a measured threshold.
    ///
    /// If it fails, the useful question is whether the new field can be
    /// borrowed instead of copied — most of `Ctx` is already `&`-references for
    /// exactly that reason. Raising the bound is fine if the field genuinely
    /// belongs here; just don't do it without looking.
    #[test]
    fn ctx_stays_small_enough_for_the_hot_path() {
        let size = std::mem::size_of::<Ctx>();
        assert!(
            size <= 320,
            "Ctx grew to {size} bytes; it is copied per command — see this test's comment"
        );
    }
}

#[cfg(test)]
mod noop_aof_tests {
    use super::{AofSender, NoopAofSender};

    #[test]
    fn noop_aof_sender_drops_payload_without_panic() {
        let n = NoopAofSender;
        n.send_aof(vec![1, 2, 3, 9]);
        n.send_aof(Vec::with_capacity(1_048_576));
    }
}
