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
//   already. Keys are placed by slot in cluster mode, so only the shards of
//   those slots are walked, a few thousand buckets per hold of a shard's
//   read lock.
// * A thread of its own counts them at startup, after a promotion, and
//   after the slots this node serves change (once they have stayed the same
//   for a moment, and no more than every few seconds), and logs a warning
//   when it finds any. `INFO cluster` reports the last count.
// * `CLUSTER ORPHANSLOTS` lists them, counted afresh, with each slot's
//   owner.
// * `CLUSTER DROPORPHANS` deletes them, on a master only, and only for
//   slots another master owns, that this node sees as up, with no slot
//   migration or handover of them under way that this node knows of
//   (`check_droppable`).

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
             ORPHANSLOTS lists them with their owners; once the owner is known to hold the \
             slots' data, CLUSTER DROPORPHANS deletes them here. Nothing deletes them on its \
             own (#517)",
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
    if !is_master(cluster, repl_state) {
        return Vec::new();
    }
    let unserved = unserved(cluster);
    let Some(db) = store.first() else {
        return Vec::new();
    };
    let mut counts = vec![0u64; CLUSTER_SLOTS];
    let holds = crate::store::shards_of_slots(db.len(), |slot| unserved[slot as usize]);
    for (shard, _) in db.iter().zip(holds).filter(|(_, holds)| *holds) {
        count_shard(shard, &unserved, &mut counts);
    }
    counts
        .into_iter()
        .enumerate()
        .filter(|&(_, n)| n > 0)
        .map(|(slot, n)| (slot as u16, n))
        .collect()
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

/// `CLUSTER DROPORPHANS [slots]`: begin deleting the keys this master holds
/// of `wanted`, or of every slot it holds orphan keys of, all of them or,
/// if one may not be, none (see [`check_droppable`]). Returns how many keys
/// it found to delete; they are deleted in the background.
pub(crate) fn drop_orphans(env: &Env, wanted: Option<Vec<u16>>) -> Result<u64, String> {
    let cluster = &env.cluster;
    if !is_master(cluster, &env.repl_state) {
        return Err(ON_A_REPLICA.into());
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
    let keys: u64 = found
        .iter()
        .filter(|(slot, _)| slots.binary_search(slot).is_ok())
        .map(|&(_, n)| n)
        .sum();
    log_info!(
        "Cluster: CLUSTER DROPORPHANS: deleting the {keys} key(s) this node holds of {} slot(s) \
         owned by other masters ({})",
        slots.len(),
        ranges_logged(slots.iter().copied())
    );
    let orphans = Arc::clone(&cluster.orphans);
    migration::start_dropping_orphans(
        cluster,
        &env.store,
        &env.repl_state,
        &env.hub_tx,
        slots,
        move || orphans.ask(Some("after CLUSTER DROPORPHANS")),
    )?;
    Ok(keys)
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

/// What the slots this node serves look like now, as a number that changes
/// when they do, or when it becomes a master or a replica.
fn fingerprint(cluster: &ClusterState, repl_state: &ReplState) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    let master = is_master(cluster, repl_state);
    (master, repl_state.generation()).hash(&mut hasher);
    if master {
        unserved(cluster).hash(&mut hasher);
    }
    hasher.finish()
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
    // The fingerprint of the last count, and when it was made.
    let mut counted: Option<(u64, Instant)> = None;
    // A fingerprint unlike it, and since when it has been the same.
    let mut changed: Option<(u64, Instant)> = None;
    loop {
        let asked = orphans.wait(POLL);
        let Some(cluster) = cluster.upgrade() else {
            return;
        };
        let Some(hooks) = cluster.hooks.get() else {
            continue;
        };
        let starter = &hooks.replica_starter;
        let print = fingerprint(&cluster, &starter.repl_state);
        let why = match (asked, counted) {
            (Some(why), _) => Some(why),
            (None, None) => (started.elapsed() >= STARTUP_DELAY).then_some(Some("at startup")),
            (None, Some((seen, at))) if seen != print => {
                let since = match changed {
                    Some((p, since)) if p == print => since,
                    _ => {
                        changed = Some((print, Instant::now()));
                        continue;
                    }
                };
                (since.elapsed() >= SETTLE && at.elapsed() >= MIN_GAP)
                    .then_some(Some("after the slots it serves changed"))
            }
            (None, Some(_)) => None,
        };
        let Some(why) = why else {
            continue;
        };
        let found = orphan_counts(&cluster, &starter.store, &starter.repl_state);
        orphans.record(&found, why);
        counted = Some((print, Instant::now()));
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
