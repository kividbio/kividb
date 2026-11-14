// src/cluster/orphans.rs
//
// Keys a master holds of slots it does not serve (#517).
//
// A slot's keys can stay on a master that no longer serves the slot. The
// case that showed it: a master hands slots over in an atomic slot
// migration, and is killed before its deletion of their keys has reached
// its replica. The replica, promoted, has every key of the slots that moved
// away, and nothing ever deletes them. No client is served them (every
// command for them is answered `-MOVED`), but they take memory, count in
// `DBSIZE`, `SCAN` and `INFO keyspace`, and are saved and replicated.
//
// Nothing deletes them on its own. Deleting them automatically was tried
// (#739), and every round of review found a way it could delete the only
// copy of a slot's keys: old-style `SETSLOT` / `MIGRATE` resharding, late
// heartbeats, restarts part way through a handover. So this node reports
// them, and an operator, who can see where the slot's data is, deletes them
// with `CLUSTER DROPORPHANS`:
//
// * `orphan_counts` counts them, per slot: the keys of the slots that are
//   not this node's, nor being imported into it, nor migrating away from
//   it, nor in an atomic slot migration here, nor having their keys deleted
//   already. The slots a master does not serve have keys in every shard,
//   so a full count is O(N) in the node's keys, a few thousand buckets per
//   hold of a shard's read lock.
// * A thread of its own counts them in full at startup and after a
//   promotion, and after the slots this node serves change counts only the
//   slots that became orphans (once they have stayed the same for a moment,
//   and no more than every few seconds). It logs a warning when it finds
//   any. `INFO cluster` reports the last count.
// * `CLUSTER ORPHANSLOTS` lists them, counted afresh, with each slot's
//   owner as this node's slot table names it.
// * `CLUSTER DROPORPHANS` deletes them, on a master only, and only for
//   slots another master owns, that this node sees as up, with no slot
//   migration or handover of them under way that this node knows of
//   (`check_droppable`). This node's slot table can be out of date (a
//   destination that crashed before its claim reached this node, a
//   resharding half done, a restart before the table was saved), so the
//   owner it names is asked as well (`CLUSTER SYNCSLOTS ORPHANCHECK`): it
//   must own the slot in its own table, not be moving it, and hold keys of
//   it (`confirm_with_owners`). Anything else refuses, with no way round.

use super::migration::{self, Env, TaskState, in_ranges};
use super::{
    CLUSTER_SLOTS, ClusterState, HANDOVER_FAILOVER_GRACE, MigrationState, NodeFlags, NodeId,
    key_hash_slot, node_id_str,
};
use crate::replication::ReplState;
use parking_lot::{Condvar, Mutex};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

/// Why `CLUSTER DROPORPHANS` refuses on a replica.
pub(crate) const ON_A_REPLICA: &str = "CLUSTER DROPORPHANS is refused on a replica: its keys \
                                       are its master's, and what the master deletes is \
                                       deleted here";

/// How often the watcher looks at the slots this node serves.
const POLL: Duration = Duration::from_secs(1);
/// How long they stay the same before it counts again: not in the middle
/// of a resharding.
const SETTLE: Duration = Duration::from_secs(2);
/// The least time between two counts for a change of the slots.
const MIN_GAP: Duration = Duration::from_secs(10);
/// How long after startup the first count is made: the slot table read
/// from nodes.conf is brought up to date by the first heartbeats.
const STARTUP_DELAY: Duration = Duration::from_secs(3);
/// How old a count that found keys may be before `INFO` asks for another.
const STALE: Duration = Duration::from_secs(60);
/// How often keys found are counted again in full, the slots unchanged.
const RECOUNT: Duration = Duration::from_secs(300);
/// How often the same finding is logged again.
const RELOG_AFTER: Duration = Duration::from_secs(600);
/// How many slot ranges a log line names.
const RANGES_LOGGED: usize = 8;

/// Per slot with any, the keys this node holds of a slot it does not
/// serve, by slot.
pub(crate) type Found = Vec<(u16, u64)>;

/// What was last found, and the watcher's wake-up.
#[derive(Default)]
pub struct Orphans {
    last: Mutex<Last>,
    /// The last `CLUSTER DROPORPHANS`.
    pub(crate) drop_status: Mutex<Option<DropStatus>>,
    /// A count asked for, and why (to log); `None` when none is.
    asked: Mutex<Option<Option<&'static str>>>,
    wake: Condvar,
}

#[derive(Default)]
struct Last {
    slots: u64,
    keys: u64,
    at: Option<Instant>,
    /// What was logged last, and when.
    logged: Option<(u64, u64, Instant)>,
}

impl Orphans {
    /// The slots and keys the last count found, for `INFO`. A count that
    /// found some and is old asks for another: they change without the
    /// slots changing (`FLUSHALL`, say).
    pub(crate) fn last_count(&self) -> (u64, u64) {
        let last = self.last.lock();
        if last.keys > 0 && last.at.is_some_and(|at| at.elapsed() >= STALE) {
            self.ask(None);
        }
        (last.slots, last.keys)
    }

    /// The end of a `CLUSTER DROPORPHANS`.
    pub(crate) fn finish_drop(&self, state: &'static str, deleted: usize, kept: usize, why: &str) {
        let mut status = self.drop_status.lock();
        let slots = status.as_ref().map_or(0, |s| s.slots);
        *status = Some(DropStatus {
            state,
            slots,
            deleted,
            kept,
            reason: why.to_string(),
        });
    }

    /// Ask the watcher to count now; with `why`, the count is logged if it
    /// finds any.
    pub(crate) fn ask(&self, why: Option<&'static str>) {
        let mut asked = self.asked.lock();
        *asked = Some(why.or((*asked).flatten()));
        self.wake.notify_one();
    }

    /// Wait up to `timeout` for a count to be asked for.
    fn wait(&self, timeout: Duration) -> Option<Option<&'static str>> {
        let mut asked = self.asked.lock();
        if asked.is_none() {
            self.wake.wait_for(&mut asked, timeout);
        }
        asked.take()
    }

    /// Note what a count found; `why` it was made, if it is to be logged.
    pub(crate) fn record(&self, found: &[(u16, u64)], why: Option<&str>) {
        let slots = found.len() as u64;
        let keys: u64 = found.iter().map(|&(_, n)| n).sum();
        let mut last = self.last.lock();
        last.slots = slots;
        last.keys = keys;
        last.at = Some(Instant::now());
        let Some(why) = why else {
            return;
        };
        if keys == 0 {
            if last.logged.take().is_some() {
                log_info!("Cluster: {why}, this node holds no keys of slots it does not serve");
            }
            return;
        }
        let again = match last.logged {
            Some((s, k, at)) => (s, k) != (slots, keys) || at.elapsed() >= RELOG_AFTER,
            None => true,
        };
        if !again {
            return;
        }
        last.logged = Some((slots, keys, Instant::now()));
        drop(last);
        log_warn!(
            "Cluster: {why}, this node holds {keys} key(s) of {slots} slot(s) it does not serve \
             ({}). No client is sent to them here (they get -MOVED), but they take memory, \
             count in DBSIZE, SCAN and INFO keyspace, and are saved and replicated. CLUSTER \
             ORPHANSLOTS lists them with their owners as this node's slot table names them. \
             CLUSTER DROPORPHANS deletes those of them their owner says it has, and keeps the \
             others. Nothing deletes them on its own (#517)",
            ranges_logged(found.iter().map(|&(slot, _)| slot))
        );
    }
}

/// Per slot, whether keys this node holds of it are orphans: it is not this
/// node's, nor being imported into it or migrating away from it (`SETSLOT`),
/// nor in an atomic slot migration here, nor having its keys deleted
/// already. Each lock taken in `ClusterState`'s order, none while a shard
/// is held.
fn unserved(cluster: &ClusterState) -> Vec<bool> {
    let me = cluster.myself();
    let deleting = cluster.slot_deletions.running_slots();
    cluster.slot_migrations.with_tasks(|tasks| {
        let owners = cluster.slots.read();
        let migrations = cluster.migrations.read();
        (0..CLUSTER_SLOTS)
            .map(|i| {
                let slot = i as u16;
                owners[i] != Some(me)
                    && !migrations.contains_key(&slot)
                    && !deleting[i]
                    && !tasks
                        .iter()
                        .any(|t| t.state == TaskState::InProgress && in_ranges(&t.ranges, slot))
            })
            .collect()
    })
}

/// Whether this node's dataset is its own: a master's.
fn is_master(cluster: &ClusterState, repl_state: &ReplState) -> bool {
    cluster.enabled && cluster.my_master_id().is_none() && !repl_state.is_slave()
}

/// Count the keys this master holds of slots it does not serve
/// ([`unserved`]); nothing on a replica, whose keys are its master's.
pub(crate) fn orphan_counts(
    cluster: &ClusterState,
    store: &crate::store::Store,
    repl_state: &ReplState,
) -> Found {
    found_of(&count_all(cluster, store, repl_state).counts)
}

/// What a count saw: the slots this node did not serve then, and per slot
/// its keys of them.
struct Counted {
    master: bool,
    generation: u64,
    unserved: Vec<bool>,
    counts: Vec<u64>,
}

/// Whether this node is a master, its replication generation, and per slot
/// whether it does not serve it.
type View = (bool, u64, Vec<bool>);

/// The slots this node does not serve now, as [`Counted`] has them, before
/// any key is counted.
fn view(cluster: &ClusterState, repl_state: &ReplState) -> View {
    let master = is_master(cluster, repl_state);
    let unserved = if master {
        unserved(cluster)
    } else {
        vec![false; CLUSTER_SLOTS]
    };
    (master, repl_state.generation(), unserved)
}

/// Count every orphan key: O(N) in this node's keys, as the slots it does
/// not serve have keys in every shard.
fn count_all(
    cluster: &ClusterState,
    store: &crate::store::Store,
    repl_state: &ReplState,
) -> Counted {
    let (master, generation, unserved) = view(cluster, repl_state);
    let counts = count_slots(store, &unserved);
    Counted {
        master,
        generation,
        unserved,
        counts,
    }
}

/// Count again after the slots this node serves changed, but for the
/// change alone: the keys of the slots that were already orphans are as
/// `before` counted them (nothing writes to them; `CLUSTER DROPORPHANS`
/// and an old count in `INFO` ask for a full count), and only the shards of
/// the slots that became orphans since are walked. A change of role is
/// counted in full.
fn count_change(
    cluster: &ClusterState,
    store: &crate::store::Store,
    repl_state: &ReplState,
    before: &Counted,
) -> Counted {
    let (master, generation, unserved) = view(cluster, repl_state);
    if (master, generation) != (before.master, before.generation) {
        return count_all(cluster, store, repl_state);
    }
    let new: Vec<bool> = unserved
        .iter()
        .zip(&before.unserved)
        .map(|(&now, &then)| now && !then)
        .collect();
    let mut counts = count_slots(store, &new);
    for (slot, count) in counts.iter_mut().enumerate() {
        if unserved[slot] && before.unserved[slot] {
            *count = before.counts[slot];
        }
    }
    Counted {
        master,
        generation,
        unserved,
        counts,
    }
}

/// The slots with keys of `counts`, with their counts.
fn found_of(counts: &[u64]) -> Found {
    counts
        .iter()
        .enumerate()
        .filter(|&(_, &n)| n > 0)
        .map(|(slot, &n)| (slot as u16, n))
        .collect()
}

/// Per slot, this node's keys of the slots `wanted` picks (0 for the
/// others). Walks the shards of those slots only.
fn count_slots(store: &crate::store::Store, wanted: &[bool]) -> Vec<u64> {
    let mut counts = vec![0u64; CLUSTER_SLOTS];
    let Some(db) = store.first() else {
        return counts;
    };
    if !wanted.iter().any(|&w| w) {
        return counts;
    }
    let holds = crate::store::shards_of_slots(db.len(), |slot| wanted[slot as usize]);
    for (shard, _) in db.iter().zip(holds).filter(|(_, holds)| *holds) {
        count_shard(shard, wanted, &mut counts);
    }
    counts
}

/// How many buckets of a shard's table a count looks at per hold of the
/// shard's read lock: writes to the shard wait for no more than that.
const BUCKETS_PER_HOLD: usize = 4096;

/// Add to `counts` the keys of `shard` of the slots `unserved` picks. A few
/// thousand buckets at a time, letting the lock go in between, so that a
/// shard of millions of keys does not hold its writers for long. Keys stay
/// in their buckets until the table is resized; if it is meanwhile, the
/// shard is counted again, and after a few tries in one hold.
fn count_shard(shard: &crate::store::Shard, unserved: &[bool], counts: &mut [u64]) {
    let mut seen: Vec<u16> = Vec::new();
    for _ in 0..3 {
        seen.clear();
        let mut size = None;
        let mut at = 0;
        let complete = loop {
            let map = crate::read_lock!(shard);
            let buckets = map.buckets();
            if *size.get_or_insert(buckets) != buckets {
                break false;
            }
            let end = (at + BUCKETS_PER_HOLD).min(buckets);
            for index in at..end {
                if let Some((key, _)) = map.bucket(index) {
                    let slot = key_hash_slot(key);
                    if unserved[slot as usize] {
                        seen.push(slot);
                    }
                }
            }
            if end == buckets {
                break true;
            }
            at = end;
        };
        if complete {
            for &slot in &seen {
                counts[slot as usize] += 1;
            }
            return;
        }
    }
    let map = crate::read_lock!(shard);
    for key in map.keys() {
        let slot = key_hash_slot(key) as usize;
        if unserved[slot] {
            counts[slot] += 1;
        }
    }
}

/// Whether `CLUSTER DROPORPHANS` may delete this node's keys of `slots`,
/// or why not. Only those of slots another master owns, that this node
/// sees as up; none while this node may yet hand slots over or take them,
/// or while the slot is migrating anywhere this node knows of.
pub(crate) fn check_droppable(cluster: &ClusterState, slots: &[u16]) -> Result<(), String> {
    let me = cluster.myself();
    // This node as a whole: a handover away from it whose outcome its
    // replicas, or the slot table here, may not reflect yet.
    if cluster.handovers.load(Ordering::SeqCst) > 0 {
        return Err(
            "an atomic slot migration away from this node is handing slots over; \
                    send it again once it has ended"
                .into(),
        );
    }
    if let Some(at) = *cluster.last_handover_end.lock()
        && at.elapsed() < HANDOVER_FAILOVER_GRACE
    {
        return Err(format!(
            "an atomic slot migration away from this node handed slots over {} ms ago, and \
             the cluster may not have settled which node owns them; send it again in a few \
             seconds",
            at.elapsed().as_millis()
        ));
    }
    if cluster.held_writes.lock().is_some() {
        return Err(
            "this node does not know yet whether a replica of it has taken its slots over".into(),
        );
    }
    if cluster.marked_failed.load(Ordering::SeqCst) {
        return Err(
            "this node was marked as failed, and has not yet made sure which slots are its own"
                .into(),
        );
    }
    let deleting = cluster.slot_deletions.running_slots();
    // Peers not heard from since this node started: until a node timeout
    // has passed, nothing marks one that is down as failing.
    let unheard = cluster.unconfirmed_addresses.read().clone();
    cluster.slot_migrations.with_tasks(|tasks| {
        let nodes = cluster.nodes.read();
        let owners = cluster.slots.read();
        let migrations = cluster.migrations.read();
        for &slot in slots {
            match migrations.get(&slot) {
                Some(MigrationState::Migrating { .. }) => {
                    return Err(format!(
                        "slot {slot} is migrating away from this node (CLUSTER SETSLOT \
                         MIGRATING)"
                    ));
                }
                Some(MigrationState::Importing { .. }) => {
                    return Err(format!(
                        "slot {slot} is being imported into this node (CLUSTER SETSLOT \
                         IMPORTING)"
                    ));
                }
                None => {}
            }
            for task in tasks.iter().filter(|t| in_ranges(&t.ranges, slot)) {
                if task.state == TaskState::InProgress {
                    return Err(format!(
                        "an atomic slot migration of slot {slot} is in progress (task {})",
                        task.id
                    ));
                }
                if let Some(ended) = task.ended_at
                    && ended.elapsed() < HANDOVER_FAILOVER_GRACE
                {
                    return Err(format!(
                        "an atomic slot migration of slot {slot} ended {} ms ago (task {}), and \
                         the cluster may not have settled which node owns it; send it again \
                         in a few seconds",
                        ended.elapsed().as_millis(),
                        task.id
                    ));
                }
            }
            if deleting[slot as usize] {
                return Err(format!(
                    "the keys of slot {slot} are being deleted already, as the slot left this \
                     node"
                ));
            }
            let Some(owner) = owners[slot as usize] else {
                return Err(format!(
                    "slot {slot} has no owner: no other master is known to hold its keys"
                ));
            };
            if owner == me {
                return Err(format!("slot {slot} is this node's own"));
            }
            let up = match owner_is_up(&nodes, owner) {
                Ok(()) if unheard.contains(&owner) => {
                    Err("has not been heard from since this node started")
                }
                up => up,
            };
            up.map_err(|why| {
                format!(
                    "the owner of slot {slot}, {}, {why}: it is not known to hold the slot's keys",
                    node_id_str(&owner)
                )
            })?;
        }
        Ok(())
    })
}

/// Whether `owner` is a master this node sees as up, or why not.
fn owner_is_up(
    nodes: &ahash::AHashMap<NodeId, super::ClusterNode>,
    owner: NodeId,
) -> Result<(), &'static str> {
    let Some(node) = nodes.get(&owner) else {
        return Err("is not in this node's view of the cluster");
    };
    if node.master_id.is_some()
        || node.flags.contains(NodeFlags::REPLICA)
        || !node.flags.contains(NodeFlags::MASTER)
    {
        return Err("is not a master in this node's view");
    }
    if node.flags.contains(NodeFlags::FAIL) {
        return Err("is failed in this node's view");
    }
    if node.flags.contains(NodeFlags::PFAIL) {
        return Err("may be failing in this node's view (no answer from it lately)");
    }
    if node
        .flags
        .intersects(NodeFlags::HANDSHAKE | NodeFlags::NOADDR)
    {
        return Err("is not reachable from this node yet");
    }
    Ok(())
}

/// What the last `CLUSTER DROPORPHANS` did, or is doing: `CLUSTER
/// DROPORPHANS STATUS`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DropStatus {
    /// `running`, `done` (every key looked at), `stopped` (part way, or
    /// before it began: `reason` says why).
    pub state: &'static str,
    pub slots: usize,
    /// Keys deleted: each one its owner said it has.
    pub deleted: usize,
    /// Keys left because their owner said it does not have them.
    pub kept: usize,
    pub reason: String,
}

/// `CLUSTER DROPORPHANS [slots]`: begin deleting the keys this master holds
/// of `wanted`, or of every slot it holds orphan keys of: all of them or,
/// if one may not be, none (see [`check_droppable`]). Returns how many keys
/// it found to look at.
///
/// Everything that asks another node runs on a thread of its own, never on
/// the command's (an I/O thread under io_uring): first each owner this
/// node's slot table names is asked whether it owns the slot in its own
/// table and is not moving it (`CLUSTER SYNCSLOTS ORPHANCHECK`); then each
/// batch of keys, before it is deleted, which of them it has (`CLUSTER
/// SYNCSLOTS ORPHANKEYS`). Only keys it has are deleted: a key it does not
/// have may be the only copy (written after the owner took the slot, or
/// left behind by a resharding that stopped part way), and is kept, and
/// counted. An owner that stops owning the slot, starts moving it, is no
/// longer the slot's owner here, or cannot be asked, stops the deletion,
/// and the keys not reached stay. `CLUSTER DROPORPHANS STATUS` and the log
/// say how it went.
pub(crate) fn drop_orphans(env: &Env, wanted: Option<Vec<u16>>) -> Result<u64, String> {
    let cluster = &env.cluster;
    if !is_master(cluster, &env.repl_state) {
        return Err(ON_A_REPLICA.into());
    }
    if cluster
        .orphans
        .drop_status
        .lock()
        .as_ref()
        .is_some_and(|s| s.state == "running")
    {
        return Err(
            "a CLUSTER DROPORPHANS is running already: see CLUSTER DROPORPHANS STATUS".into(),
        );
    }
    let found = orphan_counts(cluster, &env.store, &env.repl_state);
    cluster.orphans.record(&found, None);
    let slots = match wanted {
        Some(mut wanted) => {
            wanted.sort_unstable();
            wanted.dedup();
            wanted
        }
        None => found.iter().map(|&(slot, _)| slot).collect(),
    };
    if slots.is_empty() {
        return Ok(0);
    }
    check_droppable(cluster, &slots)?;
    // The owner of each slot, as the checks found it: if it changes, the
    // deletion stops.
    let owners: Vec<(u16, NodeId)> = {
        let table = cluster.slots.read();
        slots
            .iter()
            .filter_map(|&slot| table[slot as usize].map(|owner| (slot, owner)))
            .collect()
    };
    let keys: u64 = found
        .iter()
        .filter(|(slot, _)| slots.binary_search(slot).is_ok())
        .map(|&(_, n)| n)
        .sum();
    {
        let mut status = cluster.orphans.drop_status.lock();
        if status.as_ref().is_some_and(|s| s.state == "running") {
            return Err(
                "a CLUSTER DROPORPHANS is running already: see CLUSTER DROPORPHANS STATUS".into(),
            );
        }
        *status = Some(DropStatus {
            state: "running",
            slots: slots.len(),
            deleted: 0,
            kept: 0,
            reason: String::new(),
        });
    }
    log_info!(
        "Cluster: CLUSTER DROPORPHANS: asking the owners of {} slot(s) ({}) about the {keys} \
         key(s) this node holds of them",
        slots.len(),
        ranges_logged(slots.iter().copied())
    );
    let env = env.clone();
    let spawned = std::thread::Builder::new()
        .name("kividb-drop-orphans".into())
        .spawn(move || run_drop(&env, &slots, &owners));
    if let Err(e) = spawned {
        let why = format!("could not start the thread that deletes the keys: {e}");
        cluster.orphans.finish_drop("stopped", 0, 0, &why);
        return Err(why);
    }
    Ok(keys)
}

/// The thread of a `CLUSTER DROPORPHANS`.
fn run_drop(env: &Env, slots: &[u16], owners: &[(u16, NodeId)]) {
    let cluster = &env.cluster;
    let owner_of: ahash::AHashMap<u16, NodeId> = owners.iter().copied().collect();
    let outcome = (|| {
        confirm_with_owners(env, owners)?;
        // The asking took a moment: the checks here once more.
        check_droppable(cluster, slots)?;
        same_owners(cluster, owners)?;
        let mut confirm = |keys: &[crate::store::Key]| -> Result<Vec<bool>, String> {
            confirm_keys(env, &owner_of, keys)
        };
        migration::drop_orphans_confirmed(
            cluster,
            &env.store,
            &env.repl_state,
            &env.hub_tx,
            slots,
            &mut confirm,
        )
    })();
    match outcome {
        Ok(dropped) => {
            let (state, reason) = match &dropped.halted {
                Some(why) => ("stopped", why.clone()),
                None => ("done", String::new()),
            };
            if dropped.kept > 0 {
                log_warn!(
                    "Cluster: CLUSTER DROPORPHANS kept {} key(s) of slots owned by other \
                     masters: their owner does not have them, and they may be the only copy. \
                     CLUSTER ORPHANSLOTS still lists them",
                    dropped.kept
                );
            }
            log_info!(
                "Cluster: CLUSTER DROPORPHANS {state}: {} key(s) deleted, {} kept as their owner \
                 does not have them{}",
                dropped.deleted,
                dropped.kept,
                if reason.is_empty() {
                    String::new()
                } else {
                    format!("; stopped: {reason}")
                }
            );
            cluster
                .orphans
                .finish_drop(state, dropped.deleted, dropped.kept, &reason);
        }
        Err(why) => {
            log_warn!("Cluster: CLUSTER DROPORPHANS deleted nothing: {why}");
            cluster.orphans.finish_drop("stopped", 0, 0, &why);
        }
    }
    cluster.orphans.ask(Some("after CLUSTER DROPORPHANS"));
}

/// Whether each slot of `owners` still has that owner in this node's slot
/// table.
fn same_owners(cluster: &ClusterState, owners: &[(u16, NodeId)]) -> Result<(), String> {
    let table = cluster.slots.read();
    for &(slot, owner) in owners {
        if table[slot as usize] != Some(owner) {
            return Err(format!(
                "the owner of slot {slot} has changed in this node's slot table since {} was \
                 asked",
                node_id_str(&owner)
            ));
        }
    }
    Ok(())
}

/// What `CLUSTER SYNCSLOTS ORPHANCHECK` asks and is told: per slot, whether
/// the node asked owns it in its own slot table (and is a master), whether
/// it is importing or migrating it (`SETSLOT`, or an atomic slot migration
/// running or ended a moment ago), and how many keys of it it holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OwnerSays {
    pub owned: bool,
    pub moving: bool,
    pub keys: u64,
}

/// The owner's side of `CLUSTER SYNCSLOTS ORPHANCHECK <asker> slot ...`:
/// this node's id, then one word per slot, `slot:owned:moving:keys` (0 or 1
/// for the flags).
pub(crate) fn orphan_check_answer(
    cluster: &ClusterState,
    store: &crate::store::Store,
    repl_state: &ReplState,
    slots: &[u16],
) -> String {
    let says = owner_says(cluster, store, repl_state, slots);
    let mut words = vec![node_id_str(&cluster.myself()).to_string()];
    words.extend(slots.iter().zip(says).map(|(slot, s)| {
        format!(
            "{slot}:{}:{}:{}",
            u8::from(s.owned),
            u8::from(s.moving),
            s.keys
        )
    }));
    words.join(" ")
}

/// Per slot, whether this node owns it (as a master) and whether it is
/// moving it.
fn owner_flags(cluster: &ClusterState, repl_state: &ReplState, slots: &[u16]) -> Vec<(bool, bool)> {
    let me = cluster.myself();
    let master = is_master(cluster, repl_state);
    cluster.slot_migrations.with_tasks(|tasks| {
        let owners = cluster.slots.read();
        let migrations = cluster.migrations.read();
        slots
            .iter()
            .map(|&slot| {
                let owned = master && owners[slot as usize] == Some(me);
                let moving = migrations.contains_key(&slot)
                    || tasks.iter().any(|t| {
                        in_ranges(&t.ranges, slot)
                            && (t.state == TaskState::InProgress
                                || t.ended_at
                                    .is_some_and(|e| e.elapsed() < HANDOVER_FAILOVER_GRACE))
                    });
                (owned, moving)
            })
            .collect()
    })
}

fn owner_says(
    cluster: &ClusterState,
    store: &crate::store::Store,
    repl_state: &ReplState,
    slots: &[u16],
) -> Vec<OwnerSays> {
    let flags = owner_flags(cluster, repl_state, slots);
    let mut wanted = vec![false; CLUSTER_SLOTS];
    for &slot in slots {
        wanted[slot as usize] = true;
    }
    let counts = count_slots(store, &wanted);
    slots
        .iter()
        .zip(flags)
        .map(|(&slot, (owned, moving))| OwnerSays {
            owned,
            moving,
            keys: counts[slot as usize],
        })
        .collect()
}

/// The owner's side of `CLUSTER SYNCSLOTS ORPHANKEYS <asker> key ...`: this
/// node's id; `k` and one digit per key, 1 if it has the key (looked at
/// without using it: no expiry, LRU or keyspace event; a key past its time
/// counts as absent); then the slots of those keys it does not own, or is
/// moving, if any.
pub(crate) fn orphan_keys_answer(
    cluster: &ClusterState,
    store: &crate::store::Store,
    repl_state: &ReplState,
    keys: &[&[u8]],
) -> String {
    let mut slots: Vec<u16> = keys.iter().map(|k| key_hash_slot(k)).collect();
    slots.sort_unstable();
    slots.dedup();
    let flags = owner_flags(cluster, repl_state, &slots);
    let bits: String = match store.first() {
        Some(db) => keys
            .iter()
            .map(|key| {
                let has = crate::store::get_shard_of_bytes(db, key)
                    .read()
                    .peek(key)
                    .is_some();
                if has { '1' } else { '0' }
            })
            .collect(),
        None => "0".repeat(keys.len()),
    };
    let mut words = vec![
        node_id_str(&cluster.myself()).to_string(),
        format!("k{bits}"),
    ];
    words.extend(
        slots
            .iter()
            .zip(flags)
            .filter(|(_, (owned, moving))| !owned || *moving)
            .map(|(slot, _)| slot.to_string()),
    );
    words.join(" ")
}

/// Parse an `ORPHANCHECK` answer: who answered, and per slot what it says.
fn parse_answer(text: &str) -> Option<(String, Vec<(u16, OwnerSays)>)> {
    let mut words = text.split_ascii_whitespace();
    let id = words.next()?.to_string();
    let says = words
        .map(|word| {
            let mut parts = word.split(':');
            let slot = parts.next()?.parse().ok()?;
            let owned = parts.next()? == "1";
            let moving = parts.next()? == "1";
            let keys = parts.next()?.parse().ok()?;
            Some((
                slot,
                OwnerSays {
                    owned,
                    moving,
                    keys,
                },
            ))
        })
        .collect::<Option<Vec<_>>>()?;
    Some((id, says))
}

/// Parse an `ORPHANKEYS` answer for `n` keys: who answered, per key whether
/// it has it, and the slots it no longer owns or is moving.
fn parse_keys_answer(text: &str, n: usize) -> Option<(String, Vec<bool>, Vec<u16>)> {
    let mut words = text.split_ascii_whitespace();
    let id = words.next()?.to_string();
    let bits = words.next()?.strip_prefix('k')?;
    if bits.len() != n || !bits.bytes().all(|b| b == b'0' || b == b'1') {
        return None;
    }
    let has = bits.bytes().map(|b| b == b'1').collect();
    let bad = words
        .map(|w| w.parse().ok())
        .collect::<Option<Vec<u16>>>()?;
    Some((id, has, bad))
}

/// Whether `says`, the owner's answer for `slot`, lets this node go on to
/// ask about its keys of it, or why not.
fn owner_allows(slot: u16, owner: &str, says: Option<OwnerSays>) -> Result<(), String> {
    let Some(says) = says else {
        return Err(format!(
            "the owner of slot {slot}, {owner}, did not answer for it"
        ));
    };
    if says.moving {
        return Err(format!(
            "the owner of slot {slot} in this node's slot table, {owner}, is importing or \
             migrating it, or just has: send it again once the migration has settled"
        ));
    }
    if !says.owned {
        return Err(format!(
            "slot {slot} is {owner}'s in this node's slot table, but {owner} says it does not \
             own it: this node's slot table may be out of date, and these keys may be the only \
             copy of the slot's"
        ));
    }
    Ok(())
}

/// The node that answered must be the one asked: an address can be stale,
/// or another node's now.
fn answered_by(owner: &NodeId, id: &str) -> Result<(), String> {
    if id.as_bytes() == &owner[..] {
        Ok(())
    } else {
        Err(format!(
            "{id} answered for {}: this node has a stale address for it",
            node_id_str(owner)
        ))
    }
}

/// The owners of `owners`' slots, each with its slots.
fn by_owner<T: Copy>(items: impl IntoIterator<Item = (NodeId, T)>) -> Vec<(NodeId, Vec<T>)> {
    let mut grouped: std::collections::BTreeMap<NodeId, Vec<T>> = Default::default();
    for (owner, item) in items {
        grouped.entry(owner).or_default().push(item);
    }
    grouped.into_iter().collect()
}

/// Ask the owner of each slot of `owners`, as this node's slot table names
/// it, over a node-to-node connection (`CLUSTER SYNCSLOTS ORPHANCHECK`)
/// whether it owns the slot in its own table and is not moving it. Any slot
/// it does not, or an owner that cannot be asked (unreachable, or of a
/// version without the command), refuses the lot.
fn confirm_with_owners(env: &Env, owners: &[(u16, NodeId)]) -> Result<(), String> {
    let me = env.cluster.myself();
    for (owner, slots) in by_owner(owners.iter().map(|&(slot, owner)| (owner, slot))) {
        let owner_id = node_id_str(&owner).to_string();
        let words: Vec<String> = slots.iter().map(u16::to_string).collect();
        let mut parts: Vec<&[u8]> = vec![
            &b"CLUSTER"[..],
            &b"SYNCSLOTS"[..],
            &b"ORPHANCHECK"[..],
            &me[..],
        ];
        parts.extend(words.iter().map(|w| w.as_bytes()));
        let answer = migration::ask_node(env, &owner, &parts).map_err(|e| {
            format!(
                "could not ask the owner of slot {}, {owner_id}, whether it owns the slot ({e})",
                slots[0]
            )
        })?;
        let (id, says) = parse_answer(&answer).ok_or_else(|| {
            format!("the owner {owner_id} gave an answer this node cannot read: {answer}")
        })?;
        answered_by(&owner, &id)?;
        for &slot in &slots {
            let said = says.iter().find(|(s, _)| *s == slot).map(|&(_, s)| s);
            owner_allows(slot, &owner_id, said)?;
        }
    }
    same_owners(&env.cluster, owners)
}

/// Of `keys`, which their owner has (`CLUSTER SYNCSLOTS ORPHANKEYS`): only
/// those may be deleted here. `Err`, and the deletion stops, when a slot's
/// owner in this node's slot table is no longer the one asked at the start
/// (`owner_of`), when the owner no longer owns one of the slots or is
/// moving it, or cannot be asked.
fn confirm_keys(
    env: &Env,
    owner_of: &ahash::AHashMap<u16, NodeId>,
    keys: &[crate::store::Key],
) -> Result<Vec<bool>, String> {
    let owners: Vec<(u16, NodeId)> = {
        let mut slots: Vec<u16> = keys.iter().map(|k| key_hash_slot(k.as_bytes())).collect();
        slots.sort_unstable();
        slots.dedup();
        slots
            .into_iter()
            .map(|slot| {
                owner_of
                    .get(&slot)
                    .map(|&owner| (slot, owner))
                    .ok_or_else(|| format!("slot {slot} was not to be dropped"))
            })
            .collect::<Result<_, _>>()?
    };
    same_owners(&env.cluster, &owners)?;
    let me = env.cluster.myself();
    let mut has = vec![false; keys.len()];
    let indexed = keys.iter().enumerate().map(|(i, key)| {
        let owner = owner_of[&key_hash_slot(key.as_bytes())];
        (owner, i)
    });
    for (owner, indexes) in by_owner(indexed) {
        let owner_id = node_id_str(&owner).to_string();
        let mut parts: Vec<&[u8]> = vec![
            &b"CLUSTER"[..],
            &b"SYNCSLOTS"[..],
            &b"ORPHANKEYS"[..],
            &me[..],
        ];
        parts.extend(indexes.iter().map(|&i| keys[i].as_bytes()));
        let answer = migration::ask_node(env, &owner, &parts).map_err(|e| {
            format!("could not ask {owner_id} which of this node's keys it has ({e})")
        })?;
        let (id, owner_has, bad) = parse_keys_answer(&answer, indexes.len())
            .ok_or_else(|| format!("{owner_id} gave an answer this node cannot read: {answer}"))?;
        answered_by(&owner, &id)?;
        if let Some(slot) = bad.first() {
            return Err(format!(
                "{owner_id} no longer owns slot {slot}, or is moving it"
            ));
        }
        for (&i, owned) in indexes.iter().zip(owner_has) {
            has[i] = owned;
        }
    }
    // And after asking: the answers are the owners'.
    same_owners(&env.cluster, &owners)?;
    Ok(has)
}

/// Runs of consecutive slots of `slots` (ascending), as `(first, last)`.
pub(crate) fn runs(slots: impl IntoIterator<Item = u16>) -> Vec<(u16, u16)> {
    let mut runs: Vec<(u16, u16)> = Vec::new();
    for slot in slots {
        match runs.last_mut() {
            Some((_, last)) if *last + 1 == slot => *last = slot,
            _ => runs.push((slot, slot)),
        }
    }
    runs
}

/// `slots` for a log line: the first few runs.
fn ranges_logged(slots: impl IntoIterator<Item = u16>) -> String {
    let runs = runs(slots);
    let mut text: Vec<String> = runs
        .iter()
        .take(RANGES_LOGGED)
        .map(|&(a, b)| {
            if a == b {
                a.to_string()
            } else {
                format!("{a}-{b}")
            }
        })
        .collect();
    if runs.len() > RANGES_LOGGED {
        text.push(format!("and {} more", runs.len() - RANGES_LOGGED));
    }
    format!("slots {}", text.join(" "))
}

/// Start the thread that counts the orphan keys when something may have
/// left some ([`Orphans`]). Once, from `ClusterState::start`.
pub(crate) fn start_watching(cluster: &Arc<ClusterState>) {
    let weak = Arc::downgrade(cluster);
    let orphans = Arc::clone(&cluster.orphans);
    let spawned = std::thread::Builder::new()
        .name("kividb-orphans".into())
        .spawn(move || watch(&weak, &orphans));
    if let Err(e) = spawned {
        log_error!(
            "Cluster: could not start the thread that looks for keys of slots this node does \
             not serve: {e}; CLUSTER ORPHANSLOTS still lists them"
        );
    }
}

fn watch(cluster: &Weak<ClusterState>, orphans: &Orphans) {
    let started = Instant::now();
    // The last count, and when it was made.
    let mut counted: Option<(Counted, Instant)> = None;
    // A view unlike the last count's, and since when it has stayed so.
    let mut changed: Option<(View, Instant)> = None;
    loop {
        let asked = orphans.wait(POLL);
        let Some(cluster) = cluster.upgrade() else {
            return;
        };
        let Some(hooks) = cluster.hooks.get() else {
            continue;
        };
        let starter = &hooks.replica_starter;
        let (store, repl_state) = (&starter.store, &starter.repl_state);
        let now = view(&cluster, repl_state);
        let next = match (asked, &counted) {
            (Some(why), _) => Some((count_all(&cluster, store, repl_state), why)),
            (None, None) => (started.elapsed() >= STARTUP_DELAY)
                .then(|| (count_all(&cluster, store, repl_state), Some("at startup"))),
            (None, Some((last, at)))
                if (now.0, now.1, &now.2) != (last.master, last.generation, &last.unserved) =>
            {
                let since = match &changed {
                    Some((seen, since)) if *seen == now => *since,
                    _ => {
                        changed = Some((now, Instant::now()));
                        continue;
                    }
                };
                (since.elapsed() >= SETTLE && at.elapsed() >= MIN_GAP).then(|| {
                    (
                        count_change(&cluster, store, repl_state, last),
                        Some("after the slots it serves changed"),
                    )
                })
            }
            // Keys found, counted in full again now and then: they can go
            // without the slots changing (FLUSHALL, a reload), and the
            // counts of a change keep what was counted before.
            (None, Some((last, at)))
                if at.elapsed() >= RECOUNT && last.counts.iter().any(|&n| n > 0) =>
            {
                Some((count_all(&cluster, store, repl_state), None))
            }
            (None, Some(_)) => None,
        };
        let Some((count, why)) = next else {
            continue;
        };
        orphans.record(&found_of(&count.counts), why);
        counted = Some((count, Instant::now()));
        changed = None;
    }
}

#[cfg(test)]
mod tests {
    use super::super::{ClusterNode, generate_node_id};
    use super::*;

    fn add_node(cs: &ClusterState, flags: NodeFlags, master: Option<NodeId>) -> NodeId {
        let id = generate_node_id();
        let mut node = ClusterNode::new(id, "10.0.0.1".into(), 7000, 17000);
        node.flags |= flags;
        node.master_id = master;
        cs.nodes.write().insert(id, node);
        id
    }

    /// A master owning slots 0-99, and another one, `other`, 100-199.
    fn two_masters() -> (ClusterState, NodeId) {
        let cs = ClusterState::new_enabled(None, 15000);
        cs.claim_slot_range(0, 99).unwrap();
        let other = add_node(&cs, NodeFlags::MASTER, None);
        for slot in 100..200 {
            cs.slots.write()[slot] = Some(other);
        }
        (cs, other)
    }

    fn refused(cs: &ClusterState, slot: u16) -> String {
        check_droppable(cs, &[slot]).unwrap_err()
    }

    #[test]
    fn only_slots_another_master_owns_and_is_up_for_may_be_dropped() {
        let (cs, other) = two_masters();
        assert_eq!(check_droppable(&cs, &[100, 150, 199]), Ok(()));
        assert!(refused(&cs, 5).contains("is this node's own"));
        assert!(refused(&cs, 300).contains("has no owner"));
        // All or nothing.
        assert!(check_droppable(&cs, &[100, 5]).is_err());

        // The owner failing, or suspected.
        cs.nodes.write().get_mut(&other).unwrap().flags |= NodeFlags::PFAIL;
        assert!(
            refused(&cs, 100).contains("may be failing"),
            "{}",
            refused(&cs, 100)
        );
        cs.nodes.write().get_mut(&other).unwrap().flags |= NodeFlags::FAIL;
        assert!(refused(&cs, 100).contains("is failed"));
        cs.nodes
            .write()
            .get_mut(&other)
            .unwrap()
            .flags
            .remove(NodeFlags::FAIL | NodeFlags::PFAIL);
        assert_eq!(check_droppable(&cs, &[100]), Ok(()));

        // A replica, as the slot table names it.
        let replica = add_node(&cs, NodeFlags::REPLICA, Some(other));
        cs.slots.write()[100] = Some(replica);
        assert!(refused(&cs, 100).contains("is not a master"));
        // A node this one does not know.
        cs.slots.write()[100] = Some(generate_node_id());
        assert!(refused(&cs, 100).contains("is not in this node's view"));
        // One it is still meeting.
        let meeting = add_node(&cs, NodeFlags::MASTER | NodeFlags::HANDSHAKE, None);
        cs.slots.write()[100] = Some(meeting);
        assert!(refused(&cs, 100).contains("not reachable"));
        // One not heard from since this node started.
        cs.slots.write()[100] = Some(other);
        cs.unconfirmed_addresses.write().insert(other);
        assert!(refused(&cs, 100).contains("not been heard from"));
        cs.unconfirmed_addresses.write().clear();
        assert_eq!(check_droppable(&cs, &[100]), Ok(()));
    }

    #[test]
    fn a_slot_migrating_here_or_away_may_not_be_dropped() {
        let (cs, other) = two_masters();
        cs.migrations
            .write()
            .insert(100, MigrationState::Importing { src_id: other });
        assert!(refused(&cs, 100).contains("being imported"));
        cs.migrations
            .write()
            .insert(100, MigrationState::Migrating { dest_id: other });
        assert!(refused(&cs, 100).contains("migrating away"));
        cs.migrations.write().clear();
        assert_eq!(check_droppable(&cs, &[100]), Ok(()));
    }

    #[test]
    fn nothing_may_be_dropped_while_a_handover_is_unresolved() {
        let (cs, _) = two_masters();
        cs.handovers.store(1, Ordering::SeqCst);
        assert!(refused(&cs, 100).contains("is handing slots over"));
        cs.handovers.store(0, Ordering::SeqCst);
        *cs.last_handover_end.lock() = Some(Instant::now());
        assert!(refused(&cs, 100).contains("handed slots over"));
        *cs.last_handover_end.lock() = Some(Instant::now() - HANDOVER_FAILOVER_GRACE);
        assert_eq!(check_droppable(&cs, &[100]), Ok(()));
        cs.marked_failed.store(true, Ordering::SeqCst);
        assert!(refused(&cs, 100).contains("marked as failed"));
    }

    /// A key of `slot`.
    fn key_of(slot: u16) -> String {
        (0..)
            .map(|i| format!("k{i}"))
            .find(|k| key_hash_slot(k.as_bytes()) == slot)
            .unwrap()
    }

    fn put(store: &crate::store::Store, key: &str) {
        let _ = crate::store::get_shard(&store[0], key)
            .write()
            .insert_str(key, "v");
    }

    /// Unless the owner says that it owns the slot and is not moving it,
    /// nothing is asked of its keys. No answer refuses too, and so does an
    /// answer from another node than the one asked.
    #[test]
    fn the_owner_must_own_the_slot_and_not_move_it() {
        let ok = OwnerSays {
            owned: true,
            moving: false,
            keys: 3,
        };
        assert_eq!(owner_allows(5, "x", Some(ok)), Ok(()));
        let refused = |says| owner_allows(5, "x", says).unwrap_err();
        assert!(refused(None).contains("did not answer"));
        let not_owned = OwnerSays { owned: false, ..ok };
        assert!(refused(Some(not_owned)).contains("says it does not own it"));
        let moving = OwnerSays { moving: true, ..ok };
        assert!(refused(Some(moving)).contains("importing or migrating"));
        // No keys of it there is no refusal: each key is asked about.
        assert_eq!(
            owner_allows(5, "x", Some(OwnerSays { keys: 0, ..ok })),
            Ok(())
        );

        assert_eq!(
            parse_answer("abc 5:1:0:3 7:0:1:0"),
            Some((
                "abc".to_string(),
                vec![
                    (5, ok),
                    (
                        7,
                        OwnerSays {
                            owned: false,
                            moving: true,
                            keys: 0
                        }
                    )
                ]
            ))
        );
        assert_eq!(parse_answer("abc 5:1"), None);
        assert_eq!(parse_answer("abc x:1:0:0"), None);
        assert_eq!(parse_answer(""), None);

        assert_eq!(
            parse_keys_answer("abc k101 7", 3),
            Some(("abc".to_string(), vec![true, false, true], vec![7]))
        );
        assert_eq!(parse_keys_answer("abc k10", 3), None);
        assert_eq!(parse_keys_answer("abc 101", 3), None);
        assert_eq!(parse_keys_answer("abc k1x1", 3), None);

        let owner = generate_node_id();
        assert_eq!(answered_by(&owner, node_id_str(&owner)), Ok(()));
        assert!(
            answered_by(&owner, "someone")
                .unwrap_err()
                .contains("stale address")
        );
    }

    /// Which keys an owner says it has: looked at without using them, a key
    /// past its time absent; and the slots it no longer owns or is moving.
    #[test]
    fn an_owner_says_which_keys_it_has() {
        let (cs, other) = two_masters();
        let store = crate::store::new_store(1);
        let repl = ReplState::new_master();
        let gone = (0..)
            .map(|i| format!("g{i}"))
            .find(|k| key_hash_slot(k.as_bytes()) == 5)
            .unwrap();
        let (here, expired) = (key_of(5), key_of(6));
        put(&store, &here);
        let _ = crate::store::get_shard(&store[0], &expired)
            .write()
            .insert_str_expiring(&expired, "v", Some(1));
        let id = node_id_str(&cs.myself()).to_string();
        let keys: Vec<&[u8]> = vec![here.as_bytes(), gone.as_bytes(), expired.as_bytes()];
        let answer = orphan_keys_answer(&cs, &store, &repl, &keys);
        assert_eq!(answer, format!("{id} k100"));
        // A key of a slot it does not own names that slot.
        let theirs = key_of(150);
        let answer = orphan_keys_answer(&cs, &store, &repl, &[theirs.as_bytes()]);
        assert_eq!(answer, format!("{id} k0 150"));
        cs.migrations
            .write()
            .insert(5, MigrationState::Migrating { dest_id: other });
        let answer = orphan_keys_answer(&cs, &store, &repl, &[here.as_bytes()]);
        assert_eq!(answer, format!("{id} k1 5"));
    }

    /// What an owner answers: a slot of its own, on a master, not moving,
    /// with its keys counted.
    #[test]
    fn an_owner_says_what_it_owns_moves_and_holds() {
        let (cs, other) = two_masters();
        let store = crate::store::new_store(1);
        put(&store, &key_of(5));
        let repl = ReplState::new_master();
        let says = owner_says(&cs, &store, &repl, &[5, 6, 150]);
        let mine = |keys| OwnerSays {
            owned: true,
            moving: false,
            keys,
        };
        assert_eq!(says[0], mine(1));
        assert_eq!(says[1], mine(0));
        assert!(!says[2].owned);
        cs.migrations
            .write()
            .insert(6, MigrationState::Migrating { dest_id: other });
        assert!(owner_says(&cs, &store, &repl, &[6])[0].moving);
        assert_eq!(
            orphan_check_answer(&cs, &store, &repl, &[6]),
            format!("{} 6:1:1:0", node_id_str(&cs.myself()))
        );
        // A replica owns nothing.
        repl.become_slave();
        assert!(!owner_says(&cs, &store, &repl, &[5])[0].owned);
    }

    /// After the slots it serves change, only the slots that became
    /// orphans are counted; those that were stay as counted.
    #[test]
    fn a_change_counts_the_new_orphans_alone() {
        let (cs, other) = two_masters();
        let store = crate::store::new_store(1);
        let repl = ReplState::new_master();
        put(&store, &key_of(150));
        put(&store, &key_of(50));
        let first = count_all(&cs, &store, &repl);
        assert_eq!(found_of(&first.counts), vec![(150, 1)]);
        // Slot 50 goes to the other master, its key left here.
        cs.slots.write()[50] = Some(other);
        let next = count_change(&cs, &store, &repl, &first);
        assert_eq!(found_of(&next.counts), vec![(50, 1), (150, 1)]);
        // Back here: no longer an orphan.
        cs.slots.write()[50] = Some(cs.myself());
        let back = count_change(&cs, &store, &repl, &next);
        assert_eq!(found_of(&back.counts), vec![(150, 1)]);
        // A replica's are its master's: counted in full, and none.
        repl.become_slave();
        assert!(found_of(&count_change(&cs, &store, &repl, &back).counts).is_empty());
    }

    #[test]
    fn runs_of_slots() {
        assert_eq!(runs([1, 2, 3, 7, 9, 10]), vec![(1, 3), (7, 7), (9, 10)]);
        assert_eq!(runs([]), vec![]);
        assert_eq!(
            ranges_logged((0..40).map(|i| i * 2)),
            "slots 0 2 4 6 8 10 12 14 and 32 more"
        );
    }

    #[test]
    fn a_count_asked_for_is_logged_once_per_finding() {
        let orphans = Orphans::default();
        orphans.record(&[(5, 2)], Some("at startup"));
        let logged = orphans.last.lock().logged;
        assert!(logged.is_some());
        // The same again, a moment later: not logged again.
        orphans.record(&[(5, 2)], Some("after the slots it serves changed"));
        assert_eq!(orphans.last.lock().logged.map(|l| l.2), logged.map(|l| l.2));
        assert_eq!(orphans.last_count(), (1, 2));
        orphans.record(&[], Some("after CLUSTER DROPORPHANS"));
        assert!(orphans.last.lock().logged.is_none());
        assert_eq!(orphans.last_count(), (0, 0));
    }
}
