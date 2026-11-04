// src/cluster/routing.rs
//
// Cluster routing: given a command, decide whether this node may execute it.
//
// Which arguments are keys comes from `crate::keyspec`, the one table shared
// with the ACL key check and `COMMAND GETKEYS`. This module turns those keys
// into a hash slot and the slot into a decision.

use crate::cluster::slot::key_hash_slot;
use crate::cluster::{MigrationState, NodeFlags};
use crate::keyspec::key_positions;

/// What the routing layer wants the dispatcher to do with a command.
pub enum RouteDecision {
    /// No keys, or cluster mode disabled — pass through unmodified.
    Passthrough,
    /// All keys hash to the same slot, and we may serve it — pass through.
    OwnedSlot(u16),
    /// All keys hash to the same slot, but another node owns it.
    /// The dispatcher should respond with -MOVED instead of executing.
    Moved {
        slot: u16,
        target_ip: String,
        target_port: u16,
    },
    /// Slot is being migrated away from us. Key isn't here anymore — tell
    /// the client to retry against dest with ASKING.
    Ask {
        slot: u16,
        target_ip: String,
        target_port: u16,
    },
    /// Multi-key command spanning slots — respond with -CROSSSLOT.
    CrossSlot,
    /// Slot is unassigned in the cluster (no node owns it).
    /// Respond with -CLUSTERDOWN.
    Unassigned { slot: u16 },
    /// The cluster is not serving: a slot is uncovered or its owner has
    /// failed, or this node cannot see a majority of masters. Respond with
    /// -CLUSTERDOWN.
    ClusterDown,
    /// A multi-key command hit a slot mid-migration with only some of its
    /// keys on this node. Neither node can run it yet — respond with
    /// -TRYAGAIN.
    TryAgain,
}

impl RouteDecision {
    /// The reply that turns the command away, or `None` when it may run here.
    pub fn refusal(&self) -> Option<Vec<u8>> {
        match self {
            RouteDecision::Passthrough | RouteDecision::OwnedSlot(_) => None,
            RouteDecision::Moved {
                slot,
                target_ip,
                target_port,
            } => Some(moved_error(*slot, target_ip, *target_port)),
            RouteDecision::Ask {
                slot,
                target_ip,
                target_port,
            } => Some(ask_error(*slot, target_ip, *target_port)),
            RouteDecision::CrossSlot => Some(crossslot_error()),
            RouteDecision::Unassigned { slot } => Some(clusterdown_error(*slot)),
            RouteDecision::ClusterDown => Some(clusterdown_state_error()),
            RouteDecision::TryAgain => Some(tryagain_error()),
        }
    }
}

/// When and how a command was routed in a cluster: what a write compares
/// inside the write gate, to tell whether its keys may have left since it
/// was routed (`pause::writes_must_check`, #331). `Default` for a command
/// that was not routed.
#[derive(Clone, Copy, Default)]
pub struct Routed {
    at: u64,
    asking: bool,
}

impl Routed {
    /// Now, before the command's keys are looked for, for a client that had
    /// sent `ASKING` or not.
    #[inline]
    pub fn now(asking: bool) -> Self {
        Routed {
            at: crate::pause::routes_now(),
            asking,
        }
    }

    /// What `pause::routes_now` said.
    #[inline]
    pub fn at(self) -> u64 {
        self.at
    }

    /// Whether the client had sent `ASKING`: the command is routed again
    /// as it was routed the first time.
    pub fn asking(self) -> bool {
        self.asking
    }
}

/// Who is asking, and where the keys would be: what a routing decision made
/// outside the command dispatcher needs.
pub struct Caller<'a> {
    pub cluster: &'a crate::cluster::ClusterState,
    pub registry: &'a crate::client_registry::ClientRegistry,
    pub client_id: u64,
    pub store: &'a crate::store::Store,
    pub db: usize,
}

impl Caller<'_> {
    /// Route one command. ASKING is single-shot, so the caller says whether
    /// the client had issued it.
    fn route(&self, cmd: &[&[u8]], asking: bool) -> RouteDecision {
        let name_upper = cmd[0].to_ascii_uppercase();
        let name = std::str::from_utf8(&name_upper).unwrap_or("");
        route_named(
            name,
            cmd,
            self.cluster,
            asking,
            || self.registry.get_readonly(self.client_id),
            |key| {
                std::str::from_utf8(key).is_ok_and(|key| {
                    let shard = crate::store::get_shard(&self.store[self.db], key);
                    crate::read_lock!(shard).contains_key(key)
                })
            },
        )
    }

    /// The client's ASKING flag, cleared: it covers one command.
    fn take_asking(&self) -> bool {
        let asking = self.registry.get_asking(self.client_id);
        if asking {
            self.registry
                .update(self.client_id, |info| info.asking = false);
        }
        asking
    }
}

/// The reply that refuses to queue `cmd` inside MULTI, or `None` when it can
/// be queued.
///
/// A command is routed when it is queued, not when EXEC runs it: a
/// transaction is all-or-nothing, and one that found out at EXEC that a
/// command belongs to another node would have run the others already.
pub fn queue_refusal(caller: &Caller, cmd: &[&[u8]]) -> Option<Vec<u8>> {
    if !caller.cluster.enabled || cmd.is_empty() {
        return None;
    }
    let asking = caller.take_asking();
    caller.route(cmd, asking).refusal()
}

/// The reply that refuses to run a queued transaction, or `None` when EXEC
/// may go ahead.
///
/// Slots can change hands between a command being queued and EXEC, so every
/// command is routed again; and the commands of one transaction must all
/// address one slot, as the keys of one command must.
///
/// `routed` is set to when it was routed: inside its gates, `EXEC` asks
/// [`exec_refusal_in_gate`] whether its keys have moved since.
pub fn exec_refusal(
    caller: &Caller,
    queue: &[Vec<Vec<u8>>],
    routed: &mut Routed,
) -> Option<Vec<u8>> {
    if !caller.cluster.enabled {
        return None;
    }
    let asking = caller.take_asking();
    *routed = Routed::now(asking);
    if let Some(refusal) = route_queue(caller, queue, asking) {
        return Some(refusal);
    }
    if test_stall_keys().is_some() {
        for queued in queue {
            let cmd: Vec<&[u8]> = queued.iter().map(|arg| arg.as_slice()).collect();
            if let Some(first) = cmd.first() {
                let name = String::from_utf8_lossy(first).to_ascii_uppercase();
                stall_for_test(&name, &cmd);
            }
        }
    }
    None
}

/// Inside `EXEC`'s gates, why its transaction may not run after all, routed
/// by [`exec_refusal`] as `routed`:
/// - a `MIGRATE` is moving one of its keys (`key_moves`): it cannot wait for
///   it, holding every other command back, nor run, writing a key the
///   `MIGRATE` is about to delete. It is given up whole with `TRYAGAIN`, as
///   Redis answers a command whose keys are being moved; the client's retry
///   is sent on with `-ASK`;
/// - a handoff is under way (`pause::HANDOFF`: slots migrating away, or a
///   manual failover), and the transaction writes: it got past the pause
///   before the pause was set. Its writes would miss what the handoff sends
///   on, and be lost with the slots, and it cannot wait for the handoff,
///   holding every other command back. It is given up whole with
///   `TRYAGAIN`; the client's retry waits out the pause and is answered by
///   whoever owns the keys then. Its commands, run inside it, did not look;
/// - its keys have left here, or a slot has changed state, since it was
///   routed: it is routed again, here, and answered as whoever has them now
///   would answer it. It used to run here, on keys that had left, and make
///   them again (#331).
///
/// One load when none of these is so.
#[inline]
pub fn exec_refusal_in_gate(
    caller: &Caller,
    queue: &[Vec<Vec<u8>>],
    routed: Routed,
) -> Option<Vec<u8>> {
    if !crate::pause::writes_must_check(routed.at) {
        return None;
    }
    exec_refusal_again(caller, queue, routed)
}

#[cold]
#[inline(never)]
fn exec_refusal_again(caller: &Caller, queue: &[Vec<Vec<u8>>], routed: Routed) -> Option<Vec<u8>> {
    if crate::pause::keys_moving() && crate::key_moves::queue_names_a_moving_key(caller.db, queue) {
        return Some(b"-TRYAGAIN a key of the transaction is being migrated\r\n".to_vec());
    }
    if crate::pause::handoff() && queue.iter().any(|cmd| writes(cmd)) {
        return Some(b"-TRYAGAIN writes are paused for a handoff\r\n".to_vec());
    }
    // `pause::handoff` fenced, if it saw a handoff's flag: a handoff that
    // has ended is seen to have moved the routes, on aarch64 too (#337).
    if caller.cluster.enabled && crate::pause::routes_moved_since(routed.at) {
        return route_queue(caller, queue, routed.asking);
    }
    None
}

/// Whether the queued command `cmd` writes, or may: a script may.
fn writes(cmd: &[Vec<u8>]) -> bool {
    cmd.first().is_some_and(|name| {
        crate::replication::is_write_command_bytes(name)
            || [&b"EVAL"[..], b"EVALSHA", b"FCALL"]
                .iter()
                .any(|script| name.eq_ignore_ascii_case(script))
    })
}

/// Route every command of `queue`, for a client that had sent `ASKING` or
/// not: the refusal of the first that may not run here, or of commands that
/// address more than one slot.
fn route_queue(caller: &Caller, queue: &[Vec<Vec<u8>>], asking: bool) -> Option<Vec<u8>> {
    let mut slot: Option<u16> = None;
    for queued in queue {
        let cmd: Vec<&[u8]> = queued.iter().map(|arg| arg.as_slice()).collect();
        if cmd.is_empty() {
            continue;
        }
        match caller.route(&cmd, asking) {
            RouteDecision::OwnedSlot(this) => {
                if slot.is_some_and(|seen| seen != this) {
                    return Some(crossslot_error());
                }
                slot = Some(this);
            }
            other => {
                if let Some(refusal) = other.refusal() {
                    return Some(refusal);
                }
            }
        }
    }
    None
}

// ── For the tests of a command routed before its keys moved (#331) ──────────

/// `KIVIDB_TEST_STALL_KEYS`, never set otherwise: keys, separated by commas.
/// A command routed here that names one of them waits, once it has been
/// routed, at the failpoint `routed` (`aof::failpoint`, which
/// `KIVIDB_TEST_FAILPOINTS` has to name too), as a command can be held up
/// between its routing and the write gate on a busy machine while a
/// `MIGRATE` runs. So does `EXEC`, for the commands it has queued. Not
/// `MIGRATE` itself, which moves them, nor a command run inside a
/// transaction or a script, which was routed with it. Read once; called in
/// cluster mode only, where it costs a load and a branch.
pub fn stall_for_test(name: &str, cmd: &[&[u8]]) {
    if let Some(keys) = test_stall_keys() {
        stall_if_named(keys, name, cmd);
    }
}

fn test_stall_keys() -> Option<&'static [Vec<u8>]> {
    static KEYS: std::sync::OnceLock<Vec<Vec<u8>>> = std::sync::OnceLock::new();
    let keys = KEYS.get_or_init(|| {
        std::env::var("KIVIDB_TEST_STALL_KEYS")
            .map(|list| {
                list.split(',')
                    .filter(|key| !key.is_empty())
                    .map(|key| key.as_bytes().to_vec())
                    .collect()
            })
            .unwrap_or_default()
    });
    (!keys.is_empty()).then_some(keys.as_slice())
}

#[cold]
#[inline(never)]
fn stall_if_named(keys: &[Vec<u8>], name: &str, cmd: &[&[u8]]) {
    let named = key_positions(name, cmd)
        .iter()
        .any(|pos| keys.iter().any(|key| key.as_slice() == cmd[pos]));
    if name == "MIGRATE" || !named || crate::write_gate::held_by_this_thread() {
        return;
    }
    // Leaving the worker's other tasks to another thread of the
    // multi-threaded runtime, as `pause::wait_out_handoff` does: the test's
    // `MIGRATE` comes on a connection of its own.
    let stall = || crate::aof::failpoint("routed");
    let on_multi_thread_runtime = tokio::runtime::Handle::try_current().is_ok_and(|runtime| {
        runtime.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread
    });
    if on_multi_thread_runtime {
        tokio::task::block_in_place(stall);
    } else {
        stall();
    }
}

/// Compute the routing decision for a command.
///
/// The `cluster` ref is `Arc<ClusterState>`. We read it but don't mutate.
pub fn route_command(
    cmd: &[&[u8]],
    cluster: &crate::cluster::ClusterState,
    bypass: bool,
    asking_flag: bool,
    has_key_locally: impl Fn(&[u8]) -> bool,
) -> RouteDecision {
    if bypass || !cluster.enabled || cmd.is_empty() {
        return RouteDecision::Passthrough;
    }
    let name_upper = cmd[0].to_ascii_uppercase();
    let name = std::str::from_utf8(&name_upper).unwrap_or("");
    route_named(name, cmd, cluster, asking_flag, || false, has_key_locally)
}

/// [`route_command`] for a caller that already has the uppercased command
/// name and knows how to ask whether the client issued `READONLY`.
///
/// `readonly_client` is only consulted when it can change the outcome — a
/// read addressed to a slot this node replicates — so the common case does
/// not pay for the lookup.
pub fn route_named(
    name: &str,
    cmd: &[&[u8]],
    cluster: &crate::cluster::ClusterState,
    asking_flag: bool,
    readonly_client: impl Fn() -> bool,
    has_key_locally: impl Fn(&[u8]) -> bool,
) -> RouteDecision {
    if !cluster.enabled || cmd.is_empty() {
        return RouteDecision::Passthrough;
    }

    let keys = key_positions(name, cmd);
    let mut positions = keys.iter();
    let Some(first) = positions.next() else {
        return RouteDecision::Passthrough;
    };

    // Every key must hash to the same slot.
    let first_slot = key_hash_slot(cmd[first]);
    for pos in positions {
        if key_hash_slot(cmd[pos]) != first_slot {
            return RouteDecision::CrossSlot;
        }
    }

    let owner = cluster.slot_owner(first_slot);
    let migration = cluster.migration_state(first_slot);

    // A slot nobody owns is reported as such, ahead of the general "cluster
    // is down" answer it also implies — it says which slot.
    if owner.is_none() && !matches!(migration, Some(MigrationState::Importing { .. })) {
        return RouteDecision::Unassigned { slot: first_slot };
    }
    if !cluster.is_serving() {
        return RouteDecision::ClusterDown;
    }

    let me = cluster.myself();
    let node_addr = |id: &crate::cluster::NodeId| {
        cluster
            .nodes
            .read()
            .get(id)
            .map(|n| (cluster.endpoint_of(n), n.client_facing_port()))
    };
    let moved_to = |id: &crate::cluster::NodeId| match node_addr(id) {
        Some((ip, port)) => RouteDecision::Moved {
            slot: first_slot,
            target_ip: ip,
            target_port: port,
        },
        None => RouteDecision::Unassigned { slot: first_slot },
    };
    // How many of the command's keys this node holds, and how many it names.
    let local_keys = || {
        let (mut present, mut total) = (0usize, 0usize);
        for pos in keys.iter() {
            total += 1;
            if has_key_locally(cmd[pos]) {
                present += 1;
            }
        }
        (present, total)
    };

    match (owner, migration) {
        // We are importing this slot. A client that was sent here with -ASK
        // says so with ASKING; anyone else is sent to the slot's owner.
        (_, Some(MigrationState::Importing { src_id })) => {
            // MIGRATE runs here too, as in Redis while a slot is open:
            // `redis-cli --cluster fix` moves the keys of a node it set
            // importing back to the slot's owner with it (#295).
            if name == "MIGRATE" {
                return RouteDecision::OwnedSlot(first_slot);
            }
            if !asking_flag {
                return moved_to(&src_id);
            }
            // A multi-key command needs all of its keys to have arrived.
            let (present, total) = local_keys();
            if total > 1 && present != 0 && present != total {
                RouteDecision::TryAgain
            } else {
                RouteDecision::OwnedSlot(first_slot)
            }
        }

        // We own the slot and are migrating it away. Keys still here are
        // served here; keys already moved are served by the destination.
        (Some(owner_id), Some(MigrationState::Migrating { dest_id })) if owner_id == me => {
            // MIGRATE is how the keys leave: it always runs here, whichever
            // of its keys are still present (the missing ones are skipped).
            if name == "MIGRATE" {
                return RouteDecision::OwnedSlot(first_slot);
            }
            let (present, total) = local_keys();
            if present == total {
                RouteDecision::OwnedSlot(first_slot)
            } else if present == 0 {
                // None here: either all moved or never existed. The
                // destination can answer either way.
                match node_addr(&dest_id) {
                    Some((ip, port)) => RouteDecision::Ask {
                        slot: first_slot,
                        target_ip: ip,
                        target_port: port,
                    },
                    None => RouteDecision::Unassigned { slot: first_slot },
                }
            } else {
                // Split across the two nodes: nobody can run it until the
                // migration of these keys finishes.
                RouteDecision::TryAgain
            }
        }

        // Normal: we own the slot.
        (Some(owner_id), _) if owner_id == me => RouteDecision::OwnedSlot(first_slot),

        // Someone else owns it. If that someone is our master, the client has
        // opted into replica reads with READONLY, and the command only reads,
        // we serve it; otherwise redirect.
        (Some(owner_id), _) => {
            let i_replicate_owner = cluster.nodes.read().get(&me).is_some_and(|n| {
                n.flags.contains(NodeFlags::REPLICA) && n.master_id == Some(owner_id)
            });
            if i_replicate_owner
                && !crate::replication::is_denied_on_readonly_replica(name, cmd)
                && readonly_client()
            {
                RouteDecision::OwnedSlot(first_slot)
            } else {
                moved_to(&owner_id)
            }
        }

        (None, _) => RouteDecision::Unassigned { slot: first_slot },
    }
}

pub fn ask_error(slot: u16, ip: &str, port: u16) -> Vec<u8> {
    let s = format!("ASK {slot} {ip}:{port}");
    crate::resp::write::resp_error(&s)
}

/// Format a -CLUSTERDOWN error for a cluster that is not serving.
pub fn clusterdown_state_error() -> Vec<u8> {
    crate::resp::write::resp_error("CLUSTERDOWN The cluster is down")
}

/// Format a -TRYAGAIN error for a multi-key command split by a migration.
pub fn tryagain_error() -> Vec<u8> {
    crate::resp::write::resp_error("TRYAGAIN Multiple keys request during rehashing of slot")
}

/// Format a -MOVED error reply. Redis wire format:
///   -MOVED <slot> <ip>:<port>\r\n
pub fn moved_error(slot: u16, ip: &str, port: u16) -> Vec<u8> {
    let s = format!("MOVED {slot} {ip}:{port}");
    crate::resp::write::resp_error(&s)
}

/// Format a -CROSSSLOT error reply.
pub fn crossslot_error() -> Vec<u8> {
    crate::resp::write::resp_error("CROSSSLOT Keys in request don't hash to the same slot")
}

/// Format a -CLUSTERDOWN error for an unassigned slot.
pub fn clusterdown_error(slot: u16) -> Vec<u8> {
    crate::resp::write::resp_error(&format!("CLUSTERDOWN Hash slot {slot} is not served"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::ClusterState;

    fn make_cmd(parts: &[&str]) -> Vec<Vec<u8>> {
        parts.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    fn cmd_slices(cmd: &[Vec<u8>]) -> Vec<&[u8]> {
        cmd.iter().map(|b| b.as_slice()).collect()
    }

    #[test]
    fn passthrough_when_disabled() {
        let cs = ClusterState::new_disabled();
        let cmd = make_cmd(&["GET", "foo"]);
        assert!(matches!(
            route_command(&cmd_slices(&cmd), &cs, false, false, |_| false),
            RouteDecision::Passthrough
        ));
    }

    #[test]
    fn passthrough_when_bypassed() {
        let cs = ClusterState::new_enabled(None, 15000);
        let cmd = make_cmd(&["GET", "foo"]);
        assert!(matches!(
            route_command(&cmd_slices(&cmd), &cs, true, false, |_| false),
            RouteDecision::Passthrough
        ));
    }

    #[test]
    fn keyless_command_passthrough() {
        let cs = ClusterState::new_enabled(None, 15000);
        let cmd = make_cmd(&["PING"]);
        assert!(matches!(
            route_command(&cmd_slices(&cmd), &cs, false, false, |_| false),
            RouteDecision::Passthrough
        ));
    }

    #[test]
    fn unassigned_slot() {
        let cs = ClusterState::new_enabled(None, 15000); // no slots claimed
        let cmd = make_cmd(&["GET", "foo"]);
        match route_command(&cmd_slices(&cmd), &cs, false, false, |_| false) {
            RouteDecision::Unassigned { slot } => assert_eq!(slot, 12182),
            other => panic!(
                "expected Unassigned, got {:?}",
                std::mem::discriminant(&other)
            ),
        }
    }

    /// A cluster owning every slot, all on this node.
    fn full_cluster() -> ClusterState {
        let cs = ClusterState::new_enabled(None, 15000);
        let me = cs.myself();
        for s in cs.slots.write().iter_mut() {
            *s = Some(me);
        }
        cs.refresh_state();
        cs
    }

    #[test]
    fn owned_slot() {
        let cs = full_cluster();
        let cmd = make_cmd(&["GET", "foo"]);
        assert!(matches!(
            route_command(&cmd_slices(&cmd), &cs, false, false, |_| false),
            RouteDecision::OwnedSlot(12182)
        ));
    }

    /// With full coverage required (the default), an incomplete cluster does
    /// not serve even the slots it does have.
    #[test]
    fn partial_coverage_is_cluster_down_unless_allowed() {
        let cs = ClusterState::new_enabled(None, 15000);
        let me = cs.myself();
        cs.slots.write()[12182] = Some(me);
        cs.refresh_state();
        let cmd = make_cmd(&["GET", "foo"]);
        assert!(matches!(
            route_command(&cmd_slices(&cmd), &cs, false, false, |_| false),
            RouteDecision::ClusterDown
        ));

        cs.set_require_full_coverage(false);
        assert!(matches!(
            route_command(&cmd_slices(&cmd), &cs, false, false, |_| false),
            RouteDecision::OwnedSlot(12182)
        ));
    }

    /// A slot whose owner has been declared failed is not served: the client
    /// is told the cluster is down rather than redirected to a dead node.
    #[test]
    fn failed_owner_means_cluster_down() {
        let cs = full_cluster();
        let dead = crate::cluster::generate_node_id();
        let mut node = crate::cluster::ClusterNode::new(dead, "10.0.0.5".into(), 7000, 17000);
        node.flags |= crate::cluster::NodeFlags::MASTER;
        node.mark_failed(1);
        cs.nodes.write().insert(dead, node);
        cs.slots.write()[12182] = Some(dead);
        cs.refresh_state();

        let cmd = make_cmd(&["GET", "foo"]);
        assert!(matches!(
            route_command(&cmd_slices(&cmd), &cs, false, false, |_| false),
            RouteDecision::ClusterDown
        ));
        assert_eq!(cs.cluster_state_str(), "fail");
        assert_eq!(cs.slot_health(), (16384, 16383, 0, 1));
    }

    /// The commands whose first argument is not the key used to be hashed on
    /// that argument and redirected to whoever owns the slot of "2" or
    /// "CREATE".
    #[test]
    fn slot_is_computed_from_the_key_not_the_first_argument() {
        let cs = full_cluster();
        let slot_of = |parts: &[&str]| {
            let cmd = make_cmd(parts);
            match route_command(&cmd_slices(&cmd), &cs, false, false, |_| false) {
                RouteDecision::OwnedSlot(s) => s,
                _ => panic!("{parts:?} was not routed to a slot"),
            }
        };
        let foo = key_hash_slot(b"foo");
        assert_eq!(slot_of(&["LMPOP", "1", "foo", "LEFT"]), foo);
        assert_eq!(slot_of(&["XGROUP", "CREATE", "foo", "g", "$"]), foo);
        assert_eq!(slot_of(&["XINFO", "STREAM", "foo"]), foo);
        assert_eq!(slot_of(&["OBJECT", "ENCODING", "foo"]), foo);
        assert_eq!(slot_of(&["SINTERCARD", "1", "foo"]), foo);
        assert_eq!(slot_of(&["JSON.MGET", "foo", "$"]), foo);
        // Commands that were missing from the table ran unrouted.
        assert_eq!(slot_of(&["DELEX", "foo"]), foo);
        assert_eq!(slot_of(&["MSETEX", "1", "foo", "v", "EX", "10"]), foo);
        assert_eq!(slot_of(&["ZREMRANGEBYSCORE", "foo", "0", "1"]), foo);
        assert_eq!(slot_of(&["ARSET", "foo", "0", "v"]), foo);
        assert_eq!(slot_of(&["EVAL", "return 1", "1", "foo"]), foo);
    }

    /// A slot owned elsewhere is a MOVED — unless this node replicates the
    /// owner, the client said READONLY, and the command only reads.
    #[test]
    fn replica_serves_reads_for_its_masters_slots_only_after_readonly() {
        let cs = full_cluster();
        let me = cs.myself();
        let master = crate::cluster::generate_node_id();
        {
            let mut nodes = cs.nodes.write();
            let mut m = crate::cluster::ClusterNode::new(master, "10.0.0.5".into(), 7000, 17000);
            m.flags |= crate::cluster::NodeFlags::MASTER;
            nodes.insert(master, m);
            let my_node = nodes.get_mut(&me).unwrap();
            my_node.flags.remove(crate::cluster::NodeFlags::MASTER);
            my_node.flags |= crate::cluster::NodeFlags::REPLICA;
            my_node.master_id = Some(master);
        }
        for s in cs.slots.write().iter_mut() {
            *s = Some(master);
        }
        cs.refresh_state();

        let get = make_cmd(&["GET", "foo"]);
        let set = make_cmd(&["SET", "foo", "v"]);
        let route = |cmd: &[Vec<u8>], readonly: bool| {
            route_named(
                std::str::from_utf8(&cmd[0]).unwrap(),
                &cmd_slices(cmd),
                &cs,
                false,
                || readonly,
                |_| false,
            )
        };
        assert!(matches!(route(&get, false), RouteDecision::Moved { .. }));
        assert!(matches!(route(&get, true), RouteDecision::OwnedSlot(_)));
        // Writes always go to the master.
        assert!(matches!(route(&set, true), RouteDecision::Moved { .. }));
    }

    /// Mid-migration, a multi-key command with only some of its keys moved
    /// cannot run on either node yet.
    #[test]
    fn migrating_slot_with_split_keys_is_tryagain() {
        let cs = full_cluster();
        let dest = crate::cluster::generate_node_id();
        let mut d = crate::cluster::ClusterNode::new(dest, "10.0.0.6".into(), 7001, 17001);
        d.flags |= crate::cluster::NodeFlags::MASTER;
        cs.nodes.write().insert(dest, d);
        let slot = key_hash_slot(b"{t}a");
        cs.set_migrating(slot, dest).unwrap();

        let cmd = make_cmd(&["MGET", "{t}a", "{t}b"]);
        let route = |present: &'static [&'static [u8]]| {
            route_command(&cmd_slices(&cmd), &cs, false, false, move |k| {
                present.contains(&k)
            })
        };
        assert!(matches!(
            route(&[b"{t}a", b"{t}b"]),
            RouteDecision::OwnedSlot(_)
        ));
        assert!(matches!(route(&[]), RouteDecision::Ask { .. }));
        assert!(matches!(route(&[b"{t}a"]), RouteDecision::TryAgain));

        // MIGRATE itself always runs on the node the keys are leaving, even
        // when some (or all) of the keys it names are not there.
        let migrate = make_cmd(&[
            "MIGRATE", "10.0.0.6", "7001", "", "0", "1000", "KEYS", "{t}a", "{t}b",
        ]);
        for present in [&[b"{t}a" as &[u8]][..], &[][..]] {
            let decision = route_command(&cmd_slices(&migrate), &cs, false, false, |k| {
                present.contains(&k)
            });
            assert!(matches!(decision, RouteDecision::OwnedSlot(_)));
        }
    }

    /// On a node importing the slot, MIGRATE runs here as well, as in Redis,
    /// for `redis-cli --cluster fix` (#295). Anything else without ASKING
    /// is still sent to the slot's owner.
    #[test]
    fn migrate_runs_on_a_node_importing_the_slot() {
        let cs = full_cluster();
        let src = crate::cluster::generate_node_id();
        let mut s = crate::cluster::ClusterNode::new(src, "10.0.0.6".into(), 7001, 17001);
        s.flags |= crate::cluster::NodeFlags::MASTER;
        cs.nodes.write().insert(src, s);
        let slot = key_hash_slot(b"{t}a");
        cs.slots.write()[slot as usize] = Some(src);
        cs.set_importing(slot, src).unwrap();

        let migrate = make_cmd(&[
            "MIGRATE", "10.0.0.6", "7001", "", "0", "1000", "KEYS", "{t}a", "{t}b",
        ]);
        let decision = route_command(&cmd_slices(&migrate), &cs, false, false, |_| true);
        assert!(matches!(decision, RouteDecision::OwnedSlot(_)));
        let get = make_cmd(&["GET", "{t}a"]);
        let decision = route_command(&cmd_slices(&get), &cs, false, false, |_| true);
        assert!(matches!(decision, RouteDecision::Moved { .. }));
    }

    #[test]
    fn crossslot_detection() {
        // We own every slot, so the only failure mode for MGET below is if
        // the keys hash to different slots.
        let cs = full_cluster();
        // foo and bar hash to different slots (not collocated by hash tag).
        let cmd = make_cmd(&["MGET", "foo", "bar"]);
        assert!(matches!(
            route_command(&cmd_slices(&cmd), &cs, false, false, |_| false),
            RouteDecision::CrossSlot
        ));
    }

    #[test]
    fn hash_tag_keeps_keys_together() {
        let cs = full_cluster();
        // Both keys have the same {tag}, so they collocate — should pass.
        let cmd = make_cmd(&["MGET", "{user1}.a", "{user1}.b"]);
        assert!(matches!(
            route_command(&cmd_slices(&cmd), &cs, false, false, |_| false),
            RouteDecision::OwnedSlot(_)
        ));
    }
}
