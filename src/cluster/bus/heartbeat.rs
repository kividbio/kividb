// src/cluster/bus/heartbeat.rs
//
// The cluster's periodic work: heartbeats, failure detection, elections, and
// keeping the replication client pointed where the cluster says it should be.
//
// Every HEARTBEAT_INTERVAL we walk every known peer and send them a PING. The
// PING carries gossip and our slot claims; the receiver merges both into its
// routing table.
//
// Why every peer every tick rather than a sample: in small clusters
// (2-10 nodes) the bandwidth is negligible (~2 KiB per ping), and
// pinging every peer keeps the failure-detection signal (ping_sent_ms /
// pong_received_ms) fresh on every node.

use crate::cluster::ClusterState;
use crate::cluster::bus::BusMessage;
use crate::cluster::bus::dialer;
use crate::cluster::node::{NodeFlags, NodeId, node_id_str};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

/// Run the heartbeat loop forever. Spawned once at startup if cluster
/// mode is enabled.
pub async fn run_heartbeat_loop(state: Arc<ClusterState>) {
    log_info!("Cluster: Heartbeat loop started (interval {HEARTBEAT_INTERVAL:?})");
    let mut ticker = tokio::time::interval(HEARTBEAT_INTERVAL);
    // Skip the immediate first tick — give the bus listener a moment to bind.
    ticker.tick().await;

    loop {
        ticker.tick().await;
        send_one_round(&state).await;
    }
}

/// PING every peer once.
///
/// Failed nodes are pinged too. They used to be skipped, on the reasoning
/// that there was no point — but a node only stops being FAIL when it is
/// heard from, and a node that restarted without its nodes.conf, or whose
/// only route back is our reaching out, was never heard from again.
pub(crate) async fn send_one_round(state: &Arc<ClusterState>) {
    // Snapshot the peer list so we don't hold the lock during async I/O.
    let peers: Vec<(NodeId, String, u16)> = {
        let nodes = state.nodes.read();
        nodes
            .values()
            .filter(|n| {
                n.id != state.myself()
                    // Skip handshake-only entries — we don't know their real
                    // id yet, and the dialer task is already mid-conversation.
                    && !n.flags.contains(NodeFlags::HANDSHAKE)
                    && !n.ip.is_empty()
                    && n.cluster_port != 0
            })
            .map(|n| (n.id, n.ip.clone(), n.cluster_port))
            .collect()
    };

    let pinged: Vec<String> = peers
        .iter()
        .map(|(_, ip, port)| format!("{ip}:{port}"))
        .collect();
    dialer::prune_ping_links(state, &pinged);
    for (peer_id, ip, bus_port) in peers {
        let state = Arc::clone(state);
        tokio::spawn(async move {
            if let Err(e) = dialer::send_ping(state, ip.clone(), bus_port).await {
                // Only log if this isn't a routine "connection refused" from
                // a peer that's between heartbeats and being marked PFAIL.
                // The failure-detection loop is the canonical place to learn
                // a peer is down — heartbeat errors are just symptoms.
                if !e.contains("refused") && !e.contains("timed out") {
                    log_verbose!(
                        "Cluster: Heartbeat to {} ({ip}:{bus_port}) failed: {e}",
                        node_id_str(&peer_id)
                    );
                }
            }
        });
    }
}

pub const FAILURE_DETECTION_INTERVAL: Duration = Duration::from_millis(100);

/// Periodic task that checks for stale peers (no PONG in node_timeout_ms)
/// and applies PFAIL / FAIL flags. Runs ~10x per second so failures are
/// detected promptly once the timeout window has elapsed.
pub async fn run_failure_detection_loop(
    state: Arc<ClusterState>,
    repl_state: Arc<crate::replication::ReplState>,
) {
    log_info!(
        "Cluster: Failure detection started (interval {FAILURE_DETECTION_INTERVAL:?}, node_timeout {}ms)",
        state.node_timeout_ms
    );
    let mut ticker = tokio::time::interval(FAILURE_DETECTION_INTERVAL);
    ticker.tick().await;
    loop {
        ticker.tick().await;
        let newly_failed = state.run_failure_detection_tick();
        for id in newly_failed {
            log_warn!("Cluster: Node {} marked FAIL by quorum", node_id_str(&id));
            // Tell everyone now. Replicas of the failed master start their
            // election on this; left to gossip they would wait for the
            // verdict to reach them a few random entries at a time.
            broadcast(
                &state,
                &BusMessage::Fail {
                    sender_id: state.myself(),
                    failed_id: id,
                },
                |_| true,
            );
        }
        check_election(&state, &repl_state).await;
        state.reconcile_replication();
    }
}

/// Queue `msg` for every member that `wanted` selects, over the long-lived
/// links. Fire-and-forget.
pub(crate) fn broadcast(
    state: &Arc<ClusterState>,
    msg: &BusMessage,
    wanted: impl Fn(&crate::cluster::ClusterNode) -> bool,
) {
    let me = state.myself();
    let targets: Vec<String> = state
        .nodes
        .read()
        .values()
        .filter(|n| {
            n.id != me
                && !n.flags.contains(NodeFlags::HANDSHAKE)
                && !n.ip.is_empty()
                && n.cluster_port != 0
                && wanted(n)
        })
        .map(|n| format!("{}:{}", n.ip, n.cluster_port))
        .collect();
    if targets.is_empty() {
        return;
    }
    let frame = crate::cluster::bus::message::encode(msg);
    for addr in targets {
        dialer::send_frame(state, &addr, frame.clone());
    }
}

/// Forward a `PUBLISH` (or `SPUBLISH`) issued on this node to the rest of the
/// cluster, so subscribers connected to other nodes receive it.
///
/// A plain channel goes to every node. A shard channel goes only to the nodes
/// serving its slot — the owner and that owner's replicas.
pub fn forward_publish(state: &Arc<ClusterState>, channel: &[u8], payload: &[u8], sharded: bool) {
    if !state.enabled {
        return;
    }
    let msg = BusMessage::Publish {
        sender_id: state.myself(),
        channel: channel.to_vec(),
        payload: payload.to_vec(),
        sharded,
    };
    if sharded {
        let owner = state.slot_owner(crate::cluster::key_hash_slot(channel));
        broadcast(state, &msg, |n| {
            owner.is_some_and(|o| n.id == o || n.master_id == Some(o))
        });
    } else {
        // A failed node is not listening; do not queue for it.
        broadcast(state, &msg, |n| !n.flags.contains(NodeFlags::FAIL));
    }
}

/// Check if we should run for election (we're a replica and our master is FAIL).
/// If we should, drive the election state machine through one tick.
async fn check_election(
    state: &Arc<ClusterState>,
    repl_state: &Arc<crate::replication::ReplState>,
) {
    if !state.enabled {
        return;
    }

    // Are we a replica? What's our master?
    let (im_replica, master_id) = {
        let nodes = state.nodes.read();
        match nodes.get(&state.myself()) {
            Some(me) => (me.flags.contains(NodeFlags::REPLICA), me.master_id),
            None => (false, None),
        }
    };
    if !im_replica {
        return;
    }
    let master_id = match master_id {
        Some(id) => id,
        None => return,
    };

    // Is our master FAIL'd?
    let master_failed = {
        let nodes = state.nodes.read();
        nodes
            .get(&master_id)
            .map(|m| m.flags.contains(NodeFlags::FAIL))
            .unwrap_or(false)
    };

    let active_election = state.election.lock().clone();

    match (master_failed, active_election) {
        // Master alive, no election running — nothing to do.
        (false, None) => (),

        // Master alive and an election is running. An automatic one is stale —
        // the master came back — so abandon it. A manual one is the whole
        // point: the master is healthy and is being replaced deliberately.
        // This used to abandon both, on the next tick, so `CLUSTER FAILOVER`
        // against a healthy master never completed.
        (false, Some(election)) if !election.manual => {
            state.end_election();
        }

        // Master FAIL'd, no election yet — start one (after delay).
        (true, None) => {
            // A replica whose data is too old does not stand: it would win
            // the slots and lose whatever it never received (#107).
            if let Some(why) = too_stale_to_stand(state, repl_state) {
                let now = now_ms_local();
                if now.saturating_sub(LAST_STALE_LOG_MS.swap(now, Ordering::Relaxed)) >= 10_000 {
                    log_warn!(
                        "Cluster: Not standing for election for failed master {}: {why}",
                        node_id_str(&master_id)
                    );
                }
                return;
            }
            // Make sure we don't kick off concurrent election attempts.
            if state
                .election_pending
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
            {
                return; // Another tick is already in the delay phase.
            }

            // The delay is spent in a task of its own: this loop also detects
            // failures and keeps replication pointed right, every 100 ms, and
            // a ranked delay can be seconds.
            let state = Arc::clone(state);
            let repl_state = Arc::clone(repl_state);
            tokio::spawn(async move {
                // The replicas that have read further go first, a second
                // apart, as in Redis: so the most up-to-date one normally
                // wins, and the others' writes are not lost to it (#107).
                let rank = replica_rank(&state, &repl_state, master_id).await;
                let delay_ms = 500 + (rand_u32() % 500) as u64 + 1000 * rank;
                if rank > 0 {
                    log_info!(
                        "Cluster: {rank} replica(s) of failed master {} have read further; \
                         standing for election in {delay_ms} ms",
                        node_id_str(&master_id)
                    );
                }
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;

                // Re-check: still a replica of that master, it is still FAIL,
                // and no other replica has taken over meanwhile.
                let still_failed = {
                    let nodes = state.nodes.read();
                    let still_its_replica = nodes
                        .get(&state.myself())
                        .is_some_and(|me| me.master_id == Some(master_id));
                    still_its_replica
                        && nodes
                            .get(&master_id)
                            .map(|m| m.flags.contains(NodeFlags::FAIL))
                            .unwrap_or(false)
                };
                if !still_failed {
                    state.election_pending.store(false, Ordering::SeqCst);
                    return;
                }

                let epoch = state.start_election(master_id, false);
                state.election_pending.store(false, Ordering::SeqCst);
                log_warn!(
                    "Cluster: Starting election for epoch {epoch} (failed master: {})",
                    node_id_str(&master_id)
                );
                broadcast_auth_request(&state, master_id, epoch, false).await;
            });
        }

        // Election running — check progress.
        (_, Some(election)) => {
            if election.completed {
                return;
            }
            let quorum = state.election_quorum(master_id);
            if election.votes_received.len() >= quorum {
                log_info!(
                    "Cluster: Won election at epoch {} ({} votes, quorum {})",
                    election.epoch,
                    election.votes_received.len(),
                    quorum
                );
                promote(state, repl_state, master_id, election.epoch).await;
                return;
            }
            // Election timeout: 10 seconds. After that, abandon and re-start
            // next tick if master is still FAIL.
            let now = now_ms_local();
            if now.saturating_sub(election.started_at_ms) > 10_000 {
                log_warn!(
                    "Cluster: Election at epoch {} timed out with {} of {} votes — abandoning",
                    election.epoch,
                    election.votes_received.len(),
                    quorum
                );
                state.end_election();
            }
        }
    }
}

/// When a too-stale replica last said it would not stand (logged at most
/// every 10 s; the check runs every 100 ms).
static LAST_STALE_LOG_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Why this replica's data is too old for it to stand for election, if it
/// is: its link to the master has been down longer than
/// `node timeout × cluster-replica-validity-factor + repl-ping-replica-period`,
/// counting from when the master stopped answering (Redis's rule). A factor
/// of 0 means always stand.
fn too_stale_to_stand(
    state: &ClusterState,
    repl_state: &crate::replication::ReplState,
) -> Option<String> {
    let factor = state.replica_validity_factor();
    if factor == 0 || repl_state.slave_link_up.load(Ordering::Acquire) {
        return None;
    }
    let down_since = repl_state.master_link_down_since_ms.load(Ordering::Relaxed);
    if down_since == 0 {
        return None;
    }
    let ping_period_ms = repl_state.repl_ping_period_secs.load(Ordering::Relaxed) * 1000;
    let down_for = now_ms_local().saturating_sub(down_since);
    data_too_old(down_for, state.node_timeout_ms, ping_period_ms, factor).map(|(age, limit)| {
        format!(
            "its data is {} s old, more than the {} s cluster-replica-validity-factor {factor} allows",
            age / 1000,
            limit / 1000
        )
    })
}

/// Redis's rule, in milliseconds: with the link to the master down for
/// `down_for`, the data's age and the limit it is over, if it is. The link
/// was down for at least the node timeout before the master could be marked
/// FAIL; that much does not count against the data.
fn data_too_old(
    down_for: u64,
    node_timeout: u64,
    ping_period: u64,
    factor: u64,
) -> Option<(u64, u64)> {
    let age = if down_for > node_timeout {
        down_for - node_timeout
    } else {
        down_for
    };
    let limit = node_timeout
        .saturating_mul(factor)
        .saturating_add(ping_period);
    (factor != 0 && age > limit).then_some((age, limit))
}

/// How many other replicas of `master_id` have read further than we have.
/// Each is asked (`OFFSET`); one that does not answer, an older version
/// among them, is not counted, so it never holds us back.
async fn replica_rank(
    state: &Arc<ClusterState>,
    repl_state: &crate::replication::ReplState,
    master_id: NodeId,
) -> u64 {
    let mine = repl_state.repl_offset.load(Ordering::Relaxed);
    let me = state.myself();
    let siblings: Vec<String> = {
        let nodes = state.nodes.read();
        nodes
            .values()
            .filter(|n| {
                n.id != me
                    && n.master_id == Some(master_id)
                    && !n.flags.contains(NodeFlags::FAIL)
                    && !n.flags.contains(NodeFlags::HANDSHAKE)
                    && !n.ip.is_empty()
                    && n.cluster_port != 0
            })
            .map(|n| format!("{}:{}", n.ip, n.cluster_port))
            .collect()
    };
    let mut asked = tokio::task::JoinSet::new();
    for addr in siblings {
        let state = Arc::clone(state);
        asked.spawn(async move {
            let ask = BusMessage::OffsetQuery { sender_id: me };
            dialer::exchange(&state, &addr, &ask, Duration::from_millis(500)).await
        });
    }
    let mut ahead = 0;
    while let Some(answer) = asked.join_next().await {
        if let Ok(Ok(Some(BusMessage::OffsetAck { offset, .. }))) = answer
            && offset > mine
        {
            ahead += 1;
        }
    }
    ahead
}

/// Take over from `old_master` at `epoch`: claim its slots, switch role in
/// both the cluster view and the replication layer, and tell everyone at once.
async fn promote(
    state: &Arc<ClusterState>,
    repl_state: &Arc<crate::replication::ReplState>,
    old_master: NodeId,
    epoch: u64,
) {
    state.promote_self(old_master, epoch);
    // Flip our INFO replication role too — the cluster sees us as master,
    // repl_state needs to agree.
    repl_state.become_master();
    state.end_election();
    // Announce the new ownership now rather than on the next heartbeat tick:
    // until the others hear it they keep redirecting clients to the old
    // master, and its other replicas keep following a node that is gone.
    send_one_round(state).await;
}

/// Broadcast a FailoverAuthRequest to every known master.
pub(crate) async fn broadcast_auth_request(
    state: &Arc<ClusterState>,
    master_id: NodeId,
    epoch: u64,
    force: bool,
) {
    let masters: Vec<(NodeId, String, u16)> = {
        let nodes = state.nodes.read();
        nodes
            .values()
            .filter(|n| {
                n.flags.contains(NodeFlags::MASTER)
                    && !n.flags.contains(NodeFlags::FAIL)
                    && n.id != state.myself()
                    && !n.ip.is_empty()
            })
            .map(|n| (n.id, n.ip.clone(), n.cluster_port))
            .collect()
    };

    let me = {
        let nodes = state.nodes.read();
        nodes.get(&state.myself()).cloned().unwrap_or_else(|| {
            crate::cluster::ClusterNode::new(state.myself(), String::new(), 0, 0)
        })
    };

    let msg = BusMessage::FailoverAuthRequest {
        sender_id: state.myself(),
        sender_ip: state.advertised_ip(&me.ip),
        sender_client_port: me.client_port,
        sender_cluster_port: me.cluster_port,
        request_epoch: epoch,
        master_id,
        force,
    };

    for (peer_id, ip, bus_port) in masters {
        let msg = msg.clone();
        let state = Arc::clone(state);
        tokio::spawn(async move {
            let addr = format!("{ip}:{bus_port}");
            // A master saves its vote, synced, before it answers: on a busy
            // disk that took longer than the two seconds this used to wait,
            // and the vote was lost though it had been given.
            let wait = Duration::from_millis(state.node_timeout_ms.max(2_000));
            match dialer::exchange(&state, &addr, &msg, wait).await {
                Ok(Some(BusMessage::FailoverAuthAck {
                    sender_id,
                    request_epoch,
                })) => {
                    let count = state.record_vote(sender_id, request_epoch);
                    log_verbose!(
                        "Cluster: Got vote from {} for epoch {} (total: {})",
                        node_id_str(&sender_id),
                        request_epoch,
                        count,
                    );
                }
                // No reply is a declined vote, not a failure.
                Ok(other) => log_info!(
                    "Cluster: Election: {} gave no vote at epoch {epoch}: {}",
                    node_id_str(&peer_id),
                    match other {
                        None => "no answer".to_string(),
                        Some(m) => format!("answered {m:?}"),
                    }
                ),
                Err(e) => log_warn!(
                    "Cluster: Election: failed to ask {} ({addr}) for vote: {e}",
                    node_id_str(&peer_id)
                ),
            }
        });
    }
}

// ── Manual failover ──────────────────────────────────────────────────────────

/// How long a master holds writes for a replica that asked to take over, and
/// how long that replica waits to catch up before giving up.
pub const MANUAL_FAILOVER_TIMEOUT: Duration = Duration::from_secs(5);

/// The master's side of `CLUSTER FAILOVER`: stop accepting writes, and tell
/// the replica how far it has to read to have everything.
///
/// The offset is exact. Writes are paused, the write gate is cycled so every
/// write already in flight has queued its payload, and a hub barrier confirms
/// those payloads have been handed to the replica's link — so "what this
/// replica has been sent" is final when it is read.
pub(crate) async fn answer_mfstart(
    state: &Arc<ClusterState>,
    replica_id: NodeId,
) -> Option<BusMessage> {
    let hooks = state.hooks.get()?;
    let starter = &hooks.replica_starter;
    let repl_state = &starter.repl_state;

    // Only our own replica may ask, and only while we are a master.
    let replica_addr = {
        let nodes = state.nodes.read();
        let me = state.myself();
        if !nodes
            .get(&me)
            .is_some_and(|n| n.flags.contains(NodeFlags::MASTER))
        {
            return None;
        }
        let replica = nodes.get(&replica_id)?;
        if !replica.flags.contains(NodeFlags::REPLICA) || replica.master_id != Some(me) {
            return None;
        }
        (replica.ip.clone(), replica.client_port)
    };

    crate::pause::set_handoff_pause(MANUAL_FAILOVER_TIMEOUT.as_millis() as i64);
    // Let writes that were already in the gate finish and queue. One that is
    // past the pause check but not yet in the gate waits for the failover
    // there, and is redirected (`pause::HANDOFF`).
    let _ = tokio::task::spawn_blocking(|| drop(crate::write_gate::cut())).await;
    let (barrier_tx, barrier_rx) = tokio::sync::oneshot::channel();
    if starter
        .hub_tx
        .send(crate::replication::HubMsg::Barrier(barrier_tx))
        .is_ok()
    {
        let _ = barrier_rx.await;
    }

    // Find this replica's replication link. The master lists a replica by the
    // IP its connection came from and the port it announced; the cluster
    // knows it by the address it advertises. Those agree on the port, and
    // usually on the IP — but not behind NAT or an announce address, so an
    // exact match is preferred and a port match accepted.
    let target = {
        let peers: Vec<(u64, String, u16)> = repl_state.replica_peers.lock().clone();
        let progress = repl_state.replica_progress.lock();
        let same_port = || peers.iter().filter(|(_, _, port)| *port == replica_addr.1);
        same_port()
            .find(|(_, ip, _)| *ip == replica_addr.0)
            .or_else(|| same_port().next())
            .and_then(|(id, _, _)| progress.get(id))
            .map(|p| p.base + p.sent.load(Ordering::Relaxed))
    };
    let Some(target_offset) = target else {
        // The replica is not attached; there is nothing to hand over.
        crate::pause::clear_pause();
        return None;
    };
    log_info!(
        "Cluster: Manual failover requested by {} — writes paused, replica must reach offset {target_offset}",
        node_id_str(&replica_id)
    );
    Some(BusMessage::MfAck {
        sender_id: state.myself(),
        target_offset,
    })
}

/// The replica's side of `CLUSTER FAILOVER` (without `FORCE`): have the master
/// pause, catch up to it, then run a forced election.
///
/// Returns an error describing why the failover did not start; on success the
/// election is under way and `check_election` drives it from here.
pub async fn run_manual_failover(
    state: Arc<ClusterState>,
    master_id: NodeId,
) -> Result<(), String> {
    let hooks = state.hooks.get().ok_or("cluster hooks are not installed")?;
    let repl_state = Arc::clone(&hooks.replica_starter.repl_state);

    let master_addr = {
        let nodes = state.nodes.read();
        let m = nodes
            .get(&master_id)
            .ok_or("master is not in the node table")?;
        format!("{}:{}", m.ip, m.cluster_port)
    };

    let reply = dialer::exchange(
        &state,
        &master_addr,
        &BusMessage::MfStart {
            sender_id: state.myself(),
        },
        MANUAL_FAILOVER_TIMEOUT,
    )
    .await?;
    let target_offset = match reply {
        Some(BusMessage::MfAck { target_offset, .. }) => target_offset,
        _ => return Err("master did not acknowledge the failover request".into()),
    };

    // Catch up to the point the master stopped at.
    let deadline = tokio::time::Instant::now() + MANUAL_FAILOVER_TIMEOUT;
    while repl_state.repl_offset.load(Ordering::Relaxed) < target_offset {
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "did not catch up with the master in time (at {} of {target_offset})",
                repl_state.repl_offset.load(Ordering::Relaxed)
            ));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    start_forced_election(&state, master_id).await;
    Ok(())
}

/// Start an election that does not require the master to be FAIL.
pub async fn start_forced_election(state: &Arc<ClusterState>, master_id: NodeId) {
    let epoch = state.start_election(master_id, true);
    log_info!(
        "Cluster: Manual failover: starting election at epoch {epoch} to replace {}",
        node_id_str(&master_id)
    );
    broadcast_auth_request(state, master_id, epoch, true).await;
}

/// For the delay before a replica stands for election. It has to differ
/// between the replicas of one master, or they split the vote; the clock's
/// nanoseconds, which this used to be, come in steps of 100 on some systems.
fn rand_u32() -> u32 {
    rand::random()
}

fn now_ms_local() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::data_too_old;

    #[test]
    fn a_replica_too_long_without_its_master_does_not_stand() {
        // Node timeout 5 s, ping period 10 s, factor 10: 60 s of data age.
        let (timeout, ping) = (5_000, 10_000);
        // Just failed: down for about the node timeout.
        assert_eq!(data_too_old(6_000, timeout, ping, 10), None);
        // The node timeout itself does not count.
        assert_eq!(data_too_old(65_000, timeout, ping, 10), None);
        assert_eq!(
            data_too_old(65_001, timeout, ping, 10),
            Some((60_001, 60_000))
        );
        // Factor 0: always stand.
        assert_eq!(data_too_old(10_000_000, timeout, ping, 0), None);
        // Factor 1: 15 s.
        assert_eq!(
            data_too_old(21_000, timeout, ping, 1),
            Some((16_000, 15_000))
        );
    }
}
