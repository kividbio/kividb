// src/cluster/bus/listener.rs
//
// Bus listener: accepts incoming peer connections on the cluster port
// (typically client_port + 10000) and processes their messages.
//
// One tokio task per accepted connection. The task reads bus messages in a
// loop, dispatches them, and may write replies (PONG for MEET/PING).

use crate::cluster::ClusterState;
use crate::cluster::bus::message::{BusMessage, GossipEntry, decode, encode, exceeds_buffer_limit};
use crate::cluster::node::{ClientIps, ClusterNode, NodeFlags, NodeId, node_id_str};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Start the bus listener on `bind:port`. Returns when the listener fails
/// to bind; otherwise loops forever spawning per-connection tasks.
pub async fn run_bus_listener(
    bind: String,
    bus_port: u16,
    state: Arc<ClusterState>,
) -> std::io::Result<()> {
    let addr: SocketAddr = crate::config::host_port(&bind, bus_port)
        .parse()
        .map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("bad bind: {e}"))
        })?;
    let listener = TcpListener::bind(addr).await?;
    log_info!("Cluster: Bus listener bound on {addr}");

    loop {
        let (sock, peer) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                log_warn!("Cluster: Bus accept error: {e}");
                // A persistent accept error (fd exhaustion) would otherwise
                // spin this loop at full speed.
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                continue;
            }
        };
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            let _ = sock.set_nodelay(true);
            // The address the peer reached us on is, by construction, one of ours.
            if let Ok(local) = sock.local_addr() {
                state.learn_self_ip(local.ip());
            }
            let sock: Box<dyn crate::node_tls::AsyncStream> = if crate::node_tls::cluster() {
                match crate::node_tls::accept_bus(sock).await {
                    Ok(tls) => Box::new(tls),
                    Err(e) => {
                        // Most often a certificate the node may not use as a
                        // client (no `clientAuth`), or one another CA signed.
                        note_refused(peer, &format!("TLS handshake failed: {e}"));
                        return;
                    }
                }
            } else {
                Box::new(sock)
            };
            if let Err(e) = handle_bus_connection(sock, peer, state).await {
                let s = e.to_string();
                if !s.contains("unexpected end of file") && !s.contains("Connection reset") {
                    log_verbose!("Cluster: Bus connection error from {peer}: {e}");
                }
            }
        });
    }
}

async fn handle_bus_connection(
    mut sock: Box<dyn crate::node_tls::AsyncStream>,
    peer: SocketAddr,
    state: Arc<ClusterState>,
) -> std::io::Result<()> {
    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    if let Some(secret) = state.bus_secret.get() {
        match crate::cluster::bus::auth::challenge(&mut sock, secret).await {
            Ok(rest) => buf.extend_from_slice(&rest),
            Err(e) => {
                note_refused(peer, &e);
                return Ok(());
            }
        }
    }
    let mut tmp = [0u8; 16 * 1024];

    loop {
        // Parse and process every complete message in the buffer, before
        // reading more: what came in with the answer to the challenge may
        // already be a whole message, and its sender is waiting on the reply.
        loop {
            match decode(&buf) {
                Ok(Some((msg, consumed))) => {
                    state
                        .stats_messages_received
                        .fetch_add(1, Ordering::Relaxed);
                    if let BusMessage::Ping {
                        sender_id,
                        ref sender_ip,
                        sender_cluster_port,
                        ..
                    } = msg
                        && !state.meets_given_up.lock().is_empty()
                        && !sender_is_member(&state, &sender_id)
                    {
                        let ip = if sender_ip.is_empty() || sender_ip == "0.0.0.0" {
                            peer.ip().to_string()
                        } else {
                            sender_ip.clone()
                        };
                        crate::cluster::bus::dialer::resume_given_up_meet(
                            &state,
                            &ip,
                            sender_cluster_port,
                        );
                    }
                    let reply = match msg {
                        // The one message whose answer has to wait on the
                        // rest of the server; everything else is synchronous.
                        BusMessage::MfStart { sender_id } => {
                            crate::cluster::bus::heartbeat::answer_mfstart(&state, sender_id)
                                .await
                                .map(|m| encode(&m))
                        }
                        // Answered once the sender is saved as a member, on a
                        // thread that may wait on the disk (`admit_met_node`).
                        meet @ BusMessage::Meet { .. } => {
                            let state = Arc::clone(&state);
                            let reply = crate::cpus::spawn_blocking(move || {
                                process_message(&state, meet, peer)
                            })
                            .await
                            .unwrap_or(None);
                            if reply.is_none() {
                                // Not saved: not answered, and the sender,
                                // seeing the connection close, tries again.
                                return Ok(());
                            }
                            reply
                        }
                        // Voted once the vote is saved: the same.
                        request @ BusMessage::FailoverAuthRequest { .. } => {
                            let state = Arc::clone(&state);
                            crate::cpus::spawn_blocking(move || {
                                process_message(&state, request, peer)
                            })
                            .await
                            .unwrap_or(None)
                        }
                        other => process_message(&state, other, peer),
                    };
                    buf.drain(0..consumed);
                    if let Some(bytes) = reply
                        && !crate::cluster::bus::heartbeat::test_bus_muted()
                    {
                        sock.write_all(&bytes).await?;
                        state.stats_messages_sent.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Ok(None) => {
                    // Need more bytes — but not without limit.
                    if exceeds_buffer_limit(&buf) {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "bus message exceeds size limit",
                        ));
                    }
                    break;
                }
                Err(e) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("bus protocol error: {e}"),
                    ));
                }
            }
        }

        let n = sock.read(&mut tmp).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&tmp[..n]);
    }
}

/// Log a connection turned away for want of the bus secret: once a minute
/// at most, since a misconfigured peer retries every heartbeat.
fn note_refused(peer: SocketAddr, why: &str) {
    static LAST: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let now = now_ms();
    let last = LAST.load(Ordering::Relaxed);
    if now.saturating_sub(last) >= 60_000
        && LAST
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    {
        log_warn!("Cluster: Refused a bus connection from {peer}: {why}");
    }
}

/// Whether `id` is a node this one already counts as a member.
///
/// Only members' heartbeats, verdicts and publishes are acted on. A node that
/// is not one has to be introduced — by `CLUSTER MEET`, or by gossip from a
/// member — rather than being able to enrol itself by sending messages.
fn sender_is_member(state: &ClusterState, id: &NodeId) -> bool {
    state
        .nodes
        .read()
        .get(id)
        .is_some_and(|n| !n.flags.contains(NodeFlags::HANDSHAKE))
}

/// Apply an incoming bus message to our local cluster state.
/// Returns Some(bytes) if we should reply to the peer.
///
/// `MFSTART` is handled by the connection task instead (it has to await the
/// replication hub); it is a no-op here.
pub fn process_message(
    state: &ClusterState,
    msg: BusMessage,
    peer_addr: SocketAddr,
) -> Option<Vec<u8>> {
    // The address a peer claims for itself, or — when it has none to claim
    // (bound to a wildcard, no announce address) — the one it connected from.
    let resolve_ip = |claimed: String| {
        if claimed.is_empty() || claimed == "0.0.0.0" {
            peer_addr.ip().to_string()
        } else {
            claimed
        }
    };

    match msg {
        BusMessage::Meet {
            sender_id,
            sender_ip,
            sender_client_port,
            sender_cluster_port,
            sender_config_epoch,
        } => {
            // MEET is the operator saying "this node belongs": it overrides a
            // FORGET ban, which exists to stop gossip undoing a removal, not
            // to stop the operator reversing it.
            state.forgotten.write().remove(&sender_id);
            let ip = resolve_ip(sender_ip);
            let mut node = ClusterNode::new(
                sender_id,
                ip.clone(),
                sender_client_port,
                sender_cluster_port,
            );
            node.flags = NodeFlags::MASTER;
            node.config_epoch = sender_config_epoch;
            // Saved, so that the new peer survives a restart, before it is
            // a member, and before it is answered (#808).
            let admitted = state.admit_met_node(node, || {
                upsert_node(
                    state,
                    sender_id,
                    ip,
                    sender_client_port,
                    sender_cluster_port,
                    sender_config_epoch,
                    NodeFlags::MASTER,
                    None,
                    true,
                )
            });
            if !admitted {
                // Not saved, so not a member, and not answered: the sender
                // tries again (`send_meet`).
                return None;
            }
            state.save_soon();

            // Reply with our own PONG so the peer learns OUR id.
            Some(encode(&our_pong(state, None)))
        }
        BusMessage::Ping {
            sender_id,
            sender_ip,
            sender_client_port,
            sender_cluster_port,
            sender_config_epoch,
            gossip,
            slot_claims,
            sender_flags,
            sender_master_id,
            sender_hostname,
            sender_announced_client_port,
            sender_client_ips,
            sender_shard_id,
        } => {
            // A PING from a node we do not know is answered — so it can see
            // we are alive — but it does not get itself added by pinging.
            // Otherwise a node removed with CLUSTER FORGET, which still has
            // us in *its* table, would re-add itself on its next heartbeat.
            let member = sender_is_member(state, &sender_id);
            if member {
                if sender_flags.contains(NodeFlags::MASTER) {
                    state.note_says_master(sender_id);
                }
                apply_heartbeat(
                    state,
                    sender_id,
                    resolve_ip(sender_ip),
                    sender_client_port,
                    sender_cluster_port,
                    sender_config_epoch,
                    sender_flags,
                    sender_master_id,
                    sender_hostname,
                    sender_announced_client_port,
                    sender_client_ips,
                    sender_shard_id,
                    gossip,
                    &slot_claims,
                );
            }
            // Made after the PING is taken in: if the sender says it is a
            // replica and still owns slots here, the PONG says which.
            Some(encode(&our_pong(state, member.then_some(sender_id))))
        }
        BusMessage::Pong {
            sender_id,
            sender_ip,
            sender_client_port,
            sender_cluster_port,
            sender_config_epoch,
            gossip,
            slot_claims,
            sender_flags,
            sender_master_id,
            sender_hostname,
            sender_announced_client_port,
            sender_client_ips,
            sender_shard_id,
            held_as_owner,
        } => {
            // A PONG is a reply to something we sent. From a stranger it is
            // only acceptable as the answer to our own MEET — which the
            // dialer handles through `accept_meet_reply`, not through here.
            if sender_is_member(state, &sender_id) {
                if sender_flags.contains(NodeFlags::MASTER) {
                    state.note_says_master(sender_id);
                }
                apply_heartbeat(
                    state,
                    sender_id,
                    resolve_ip(sender_ip),
                    sender_client_port,
                    sender_cluster_port,
                    sender_config_epoch,
                    sender_flags,
                    sender_master_id,
                    sender_hostname,
                    sender_announced_client_port,
                    sender_client_ips,
                    sender_shard_id,
                    gossip,
                    &slot_claims,
                );
                // What the sender holds of this node, in its own words:
                // never relayed, so never another node's stale view.
                state.note_held_as_owner(sender_id, held_as_owner);
                state.note_pong_heard(sender_id);
            }
            None
        }
        BusMessage::FailoverAuthRequest {
            sender_id,
            request_epoch,
            master_id,
            force,
            claims,
            claims_unreadable,
            ..
        } => {
            // Claims that do not read as claims are not taken for none: the
            // vote would be decided with no check at all (#742).
            if claims_unreadable {
                log_warn!(
                    "Cluster: Not voting for {} to take over from {}: its request's slot \
                     claims do not read as valid claims",
                    node_id_str(&sender_id),
                    node_id_str(&master_id)
                );
                return None;
            }
            if state.cast_vote(
                sender_id,
                master_id,
                request_epoch,
                force,
                claims.as_deref(),
            ) {
                // A vote is sent once the file that records it is on disk,
                // so that a restart cannot repeat it. Not saved, it is not
                // sent, and this node does not vote in that epoch (it has
                // recorded it as voted).
                // Saved as a node met is: by the first save begun after it,
                // one already under way included, not one save a vote.
                let from = state.saves.written.load(Ordering::SeqCst) + 1;
                let deadline = std::time::Instant::now() + crate::cluster::ELECTION_LIFETIME;
                if let Err(e) = crate::cluster::persist::wait_for_save(state, from, deadline) {
                    log_warn!(
                        "Cluster: Did not vote for {} at epoch {}: the vote could not be saved ({e})",
                        node_id_str(&sender_id),
                        request_epoch,
                    );
                    // Saved again, in the background: the save that failed
                    // may be the file in place, its rename not synced (it
                    // failed at the directory's sync).
                    state.save_soon();
                    return None;
                }
                log_info!(
                    "Cluster: Voted for {} to take over from {} at epoch {}{}",
                    node_id_str(&sender_id),
                    node_id_str(&master_id),
                    request_epoch,
                    if force { " (manual failover)" } else { "" },
                );
                Some(encode(&BusMessage::FailoverAuthAck {
                    sender_id: state.myself(),
                    request_epoch,
                }))
            } else {
                // Refused because this node knows a newer owner of slots the
                // candidate claimed: say which, so that it learns them even
                // if it cannot hear those owners itself, and stands again
                // for what its master still has (#742). Only to a candidate
                // that sent claims, which no earlier version does: one of
                // those drops a connection on a message it does not know.
                // Every other refusal is silent, as before.
                let mut owners = claims
                    .as_deref()
                    .map(|c| state.newer_owners_of_claimed(master_id, c))
                    .unwrap_or_default();
                // And its master's own claims, if its request left out some
                // of them: it stands for those too, and counts the masters
                // that own slots as this node does.
                if let Some(master) = claims
                    .as_deref()
                    .and_then(|c| state.master_update_for_left_out(master_id, c))
                    && !owners.iter().any(|o| o.id == master.id)
                {
                    owners.push(master);
                }
                // And the slots it claimed that came to its master after the
                // epoch it claimed them at (`came_at`), at the epoch each
                // came at: it stands for the others.
                let moved = if crate::cluster::came_at::test_voter_skips() {
                    Default::default()
                } else {
                    claims
                        .as_deref()
                        .map(|c| state.moved_claims_of(master_id, c))
                        .unwrap_or_default()
                };
                if !moved.is_empty() {
                    return Some(encode(&BusMessage::UpdateMoved {
                        sender_id: state.myself(),
                        owners,
                        moved: moved.decisive,
                        fallback: moved.fallback,
                    }));
                }
                if owners.is_empty() {
                    return None;
                }
                Some(encode(&BusMessage::Update {
                    sender_id: state.myself(),
                    owners,
                }))
            }
        }
        // Normally read by the candidate that asked, on the connection it
        // asked on (`heartbeat::broadcast_auth_request`); taken in the same
        // way if one comes here.
        BusMessage::Update { sender_id, owners } => {
            if sender_is_member(state, &sender_id) {
                apply_update(state, sender_id, owners);
            }
            None
        }
        BusMessage::UpdateMoved {
            sender_id,
            owners,
            moved,
            fallback,
        } => {
            if sender_is_member(state, &sender_id) {
                apply_update(state, sender_id, owners);
                apply_moved(state, sender_id, &moved, &fallback);
            }
            None
        }
        BusMessage::FailoverAuthAck {
            sender_id,
            request_epoch,
        } => {
            let count = state.record_vote(sender_id, request_epoch);
            log_verbose!(
                "Cluster: Got vote from {} for epoch {} (total: {})",
                node_id_str(&sender_id),
                request_epoch,
                count,
            );
            None
        }
        BusMessage::Fail {
            sender_id,
            failed_id,
        } => {
            // Only a member's verdict counts.
            if !sender_is_member(state, &sender_id) {
                return None;
            }
            if failed_id == state.myself() {
                // We know we are up, but the cluster has failed us: our
                // replicas are about to stand for election (#242).
                state.note_marked_failed(sender_id);
            } else if state.mark_node_failed(failed_id) {
                log_warn!(
                    "Cluster: Node {} marked FAIL (reported by {})",
                    node_id_str(&failed_id),
                    node_id_str(&sender_id)
                );
            }
            None
        }
        BusMessage::Publish {
            sender_id,
            channel,
            payload,
            sharded,
        } => {
            if sender_is_member(state, &sender_id)
                && let Some(hooks) = state.hooks.get()
            {
                // The name as it was published, bytes and all (#161).
                let payload: Arc<[u8]> = Arc::from(payload.as_slice());
                if sharded {
                    hooks.pubsub.spublish(&channel[..], payload);
                } else {
                    hooks.pubsub.publish(&channel[..], payload);
                }
            }
            None
        }
        // Answered by the connection task; see `handle_bus_connection`.
        BusMessage::MfStart { .. } | BusMessage::MfAck { .. } => None,
        // A fellow member asks how far we have read (an election's rank).
        BusMessage::OffsetQuery { sender_id } => {
            if !sender_is_member(state, &sender_id) {
                return None;
            }
            let offset = state
                .hooks
                .get()?
                .replica_starter
                .repl_state
                .repl_offset
                .load(Ordering::Relaxed);
            Some(encode(&BusMessage::OffsetAck {
                sender_id: state.myself(),
                offset,
            }))
        }
        BusMessage::OffsetAck { .. } => None,
    }
}

/// Accept the PONG that answers our own `MEET`: the one case where a message
/// from a node we do not know yet is taken at its word. The node is saved
/// before it is a member (`ClusterState::admit_met_node`): this blocks on
/// the disk. False if it could not be saved, or `msg` is not a PONG.
pub fn accept_meet_reply(state: &ClusterState, msg: BusMessage, peer_addr: SocketAddr) -> bool {
    if let BusMessage::Pong {
        sender_id,
        sender_ip,
        sender_client_port,
        sender_cluster_port,
        sender_config_epoch,
        gossip,
        slot_claims,
        sender_flags,
        sender_master_id,
        sender_hostname,
        sender_announced_client_port,
        sender_client_ips,
        sender_shard_id,
        // A node met has not been this node's peer: it holds it as the
        // owner of nothing.
        held_as_owner: _,
    } = msg
    {
        state.forgotten.write().remove(&sender_id);
        let ip = if sender_ip.is_empty() || sender_ip == "0.0.0.0" {
            peer_addr.ip().to_string()
        } else {
            sender_ip
        };
        let mut node = ClusterNode::new(
            sender_id,
            ip.clone(),
            sender_client_port,
            sender_cluster_port,
        );
        node.flags = sender_flags & (NodeFlags::MASTER | NodeFlags::REPLICA);
        node.master_id = sender_master_id;
        node.config_epoch = sender_config_epoch;
        node.hostname = sender_hostname.clone();
        node.client_ips = sender_client_ips.clone();
        if let Some(shard_id) = sender_shard_id {
            node.shard_id = shard_id;
        }
        return state.admit_met_node(node, || {
            apply_heartbeat(
                state,
                sender_id,
                ip,
                sender_client_port,
                sender_cluster_port,
                sender_config_epoch,
                sender_flags,
                sender_master_id,
                sender_hostname,
                sender_announced_client_port,
                sender_client_ips,
                sender_shard_id,
                gossip,
                &slot_claims,
            )
        });
    }
    false
}

/// Everything a PING or PONG tells us: the sender is alive and where it is,
/// what it thinks of other nodes, and which slots it owns.
#[allow(clippy::too_many_arguments)]
fn apply_heartbeat(
    state: &ClusterState,
    sender_id: NodeId,
    ip: String,
    client_port: u16,
    cluster_port: u16,
    config_epoch: u64,
    sender_flags: NodeFlags,
    sender_master_id: Option<NodeId>,
    sender_hostname: String,
    sender_announced_client_port: u16,
    sender_client_ips: ClientIps,
    sender_shard_id: Option<NodeId>,
    gossip: Vec<GossipEntry>,
    slot_claims: &[crate::cluster::bus::SlotClaim],
) {
    // Hold this read guard through the whole merge. RESET takes the write side
    // before clearing topology, so a heartbeat admitted before the reset must
    // finish before the reset returns and cannot restore the old cluster after.
    let forgotten = state.forgotten.read();
    if forgotten
        .get(&sender_id)
        .is_some_and(|until| *until > now_ms())
    {
        return;
    }
    // The master the sender replicated here, if this heartbeat is the one
    // that makes it a master here: its claims of that master's slots at
    // that master's own epoch are a takeover at a colliding epoch (#743).
    let former_master = if sender_flags.contains(NodeFlags::MASTER) {
        state.nodes.read().get(&sender_id).and_then(|n| {
            n.flags
                .contains(NodeFlags::REPLICA)
                .then_some(n.master_id)
                .flatten()
        })
    } else {
        None
    };
    // What it says of itself only, never relayed (#742).
    state.note_vote_claims(sender_id, sender_flags.contains(NodeFlags::VOTE_CLAIMS));
    // The sender's view of *itself* never includes "I have failed".
    upsert_node(
        state,
        sender_id,
        ip,
        client_port,
        cluster_port,
        config_epoch,
        sender_flags & !(NodeFlags::PFAIL | NodeFlags::FAIL),
        sender_master_id,
        true,
    );
    state.observe_epoch(config_epoch);
    // What a node says of its own hostname stands, none included.
    state.set_peer_hostname(sender_id, &sender_hostname, true);
    state.set_peer_client_port(sender_id, sender_announced_client_port, true);
    // Its client addresses only from its own word, never from gossip (#847).
    state.set_peer_client_ips(sender_id, &sender_client_ips);
    // Its shard: what a master says of its own is its shard's, replicas
    // included (this node too, if it is one); a replica is in its master's
    // (#563). A node of an earlier version says none.
    state.take_shard_id(sender_id, sender_shard_id);
    // It reached us, so whatever we concluded about it deserves a second look.
    state.maybe_clear_fail_on_contact(sender_id);

    for g in gossip {
        merge_gossip_entry(state, sender_id, g, &forgotten);
    }
    // A replica announces no slots; claims only ever come from masters.
    if sender_flags.contains(NodeFlags::MASTER) && !test_ignores_claims_of(&sender_id) {
        // What it claims in its own words: all an `UPDATE` relays of it.
        state.note_own_claims(sender_id, slot_claims);
        state.apply_peer_slot_claims_from(sender_id, slot_claims, former_master);
    }
    // After the claims, as Redis does (`clusterHandleConfigEpochCollision`
    // comes last): a master whose replica took its slots over at its own
    // epoch has given them up by now, and does not move above it and take
    // them back (#743).
    if sender_flags.contains(NodeFlags::MASTER) {
        state.resolve_epoch_collision(sender_id, config_epoch);
    }
    // And the links of a takeover with it let go once they no longer apply.
    state.settle_takeover_links(
        sender_id,
        sender_flags.contains(NodeFlags::MASTER),
        config_epoch,
    );
    state.save_soon();
}

/// Take in an `UPDATE` from `sender_id`: the newer owners of slots this
/// node claimed for its master when it asked for a vote, as the voter knows
/// them (#742). Each owner's slots are taken in as its own PING's claims
/// would be (`apply_peer_slot_claims`: a slot moves only to a claim newer
/// than its owner's here), and its config epoch is raised to the one given,
/// as gossip raises it; an owner this node does not know yet is added as
/// gossip adds one. The claims are what that owner itself claimed at that
/// epoch, relayed: taking them in is taking in that owner's heartbeat late,
/// which the epochs already make safe. If its master is left with no slots,
/// this node follows their new owner and stands for nothing more; else it
/// asks again, for the slots its master still has.
pub(crate) fn apply_update(
    state: &ClusterState,
    sender_id: NodeId,
    owners: Vec<crate::cluster::bus::OwnerUpdate>,
) {
    let forgotten = state.forgotten.read();
    for owner in owners {
        if owner.id == state.myself()
            || forgotten
                .get(&owner.id)
                .is_some_and(|until| *until > now_ms())
        {
            continue;
        }
        upsert_node(
            state,
            owner.id,
            owner.ip,
            owner.client_port,
            owner.cluster_port,
            owner.config_epoch,
            NodeFlags::MASTER,
            None,
            false,
        );
        state.observe_epoch(owner.config_epoch);
        log_info!(
            "Cluster: {} answered this node's vote request with the newer owner of slots it \
             claimed: {} owns {} at config epoch {}",
            node_id_str(&sender_id),
            node_id_str(&owner.id),
            owner
                .claims
                .iter()
                .map(|c| if c.start == c.end {
                    c.start.to_string()
                } else {
                    format!("{}-{}", c.start, c.end)
                })
                .collect::<Vec<_>>()
                .join(","),
            owner.config_epoch
        );
        // Of this node's own master, only the slots it owns here already,
        // and those whose data this node holds as of the claim, as its
        // master's stream told it (`master_claims_arrived`): a replica that
        // does not hold them may not hold their keys.
        if state.my_master_id() == Some(owner.id) {
            let claims = state.master_claims_arrived(owner.id, &owner.claims);
            state.apply_peer_slot_claims(owner.id, &claims);
        } else {
            state.apply_peer_slot_claims(owner.id, &owner.claims);
        }
    }
    drop(forgotten);
    state.save_soon();
}

/// Take in the slots of this node's master that a voter says came to it
/// after the epoch this node claimed them at, each at the epoch it came at
/// (`came_at`): `moved`, whose coming the voter knows, are given up at
/// once; `fallback`, which the voter takes to have come at their owner's
/// epoch, once the masters that refused them leave this node no quorum for
/// them (`ClusterState::moved_refusals_give_up`). A slot given up is a
/// gain of the master at that epoch, held only if the master's stream
/// confirms it (a `TAKEN` at that epoch or later that lists it, or an
/// `ARRIVED` at that very epoch; `followed_imports::FollowedImports::note_gains`);
/// this node stands for the others.
pub(crate) fn apply_moved(
    state: &ClusterState,
    sender_id: NodeId,
    moved: &[crate::cluster::bus::SlotClaim],
    fallback: &[crate::cluster::bus::SlotClaim],
) {
    let Some(master) = state.my_master_id() else {
        return;
    };
    let of_master = |claims: &[crate::cluster::bus::SlotClaim]| -> Vec<(u16, u64)> {
        let slots = state.slots.read();
        claims
            .iter()
            .flat_map(|c| {
                (c.start..=c.end.min((crate::cluster::CLUSTER_SLOTS - 1) as u16))
                    .map(|s| (s, c.epoch))
            })
            .filter(|&(slot, _)| slots[slot as usize] == Some(master))
            .collect()
    };
    let (decisive, fallback_gains) = (of_master(moved), of_master(fallback));
    if decisive.is_empty() && fallback_gains.is_empty() {
        return;
    }
    let mut given_up = decisive;
    if !fallback_gains.is_empty() {
        given_up.extend(state.moved_refusals_give_up(sender_id, master, &fallback_gains));
    }
    let all: Vec<crate::cluster::bus::SlotClaim> = moved.iter().chain(fallback).cloned().collect();
    log_info!(
        "Cluster: {} answered this node's vote request: slots {} came to its master after the \
         epoch this node claimed them at; {} given up, held only if its master's stream \
         confirms them",
        node_id_str(&sender_id),
        crate::cluster::came_at::claims_text(&all),
        if given_up.is_empty() {
            "none".to_string()
        } else {
            crate::cluster::migration::ranges_text(&crate::cluster::migration::slot_runs(
                given_up.iter().map(|(s, _)| *s),
            ))
        }
    );
    if !given_up.is_empty() {
        state.followed_imports.note_gains(master, &given_up);
        state.save_soon();
    }
}

/// For the tests of a node that has not heard of a slot's new owner, and
/// never set otherwise: `KIVIDB_TEST_IGNORE_CLAIMS`, a file that, while it
/// holds a node's id, has this node take in none of that node's slot
/// claims.
fn test_ignores_claims_of(id: &NodeId) -> bool {
    static PATH: std::sync::OnceLock<Option<std::path::PathBuf>> = std::sync::OnceLock::new();
    let Some(path) = PATH
        .get_or_init(|| std::env::var_os("KIVIDB_TEST_IGNORE_CLAIMS").map(Into::into))
        .as_deref()
    else {
        return false;
    };
    std::fs::read_to_string(path).is_ok_and(|s| s.trim() == node_id_str(id))
}

/// Insert or update a node in the routing table. Clears HANDSHAKE flag if set.
///
/// `flags` carries role only (MASTER / REPLICA). Failure verdicts are never
/// copied from another node's view: they are reached locally, by quorum, or
/// arrive as an explicit FAIL broadcast.
#[allow(clippy::too_many_arguments)]
fn upsert_node(
    state: &ClusterState,
    id: NodeId,
    ip: String,
    client_port: u16,
    cluster_port: u16,
    config_epoch: u64,
    flags: NodeFlags,
    master_id: Option<NodeId>,
    direct_contact: bool,
) {
    if id == state.myself() {
        return;
    }
    let role = flags & (NodeFlags::MASTER | NodeFlags::REPLICA);

    {
        let mut nodes = state.nodes.write();

        // A handshake placeholder for this address has served its purpose.
        nodes.retain(|placeholder_id, n| {
            !(n.flags.contains(NodeFlags::HANDSHAKE)
                && n.ip == ip
                && n.client_port == client_port
                && *placeholder_id != id)
        });

        let is_new = !nodes.contains_key(&id);
        let entry = nodes
            .entry(id)
            .or_insert_with(|| ClusterNode::new(id, ip.clone(), client_port, cluster_port));
        // What a node says about its own address beats what a third party
        // relays, which may be stale. A relayed address only fills a gap,
        // or replaces one we cannot reach the node at, from a node that
        // can: the node has moved. Two nodes restarted together with new
        // addresses (pods rescheduled) can only learn each other's this
        // way, each having dialled the other's old one; they kept those
        // for ever, each seeing the other as failed, and a shard left
        // without a master the cluster could see went down with it.
        // So does one we have not heard from since we started, whose
        // address is still nodes.conf's: the start grace otherwise keeps
        // it unsuspected for two node timeouts, and the moment it ends
        // our suspicion and a peer's report from before the node came
        // back make a quorum, and a healthy master is failed over.
        let suspect = NodeFlags::PFAIL | NodeFlags::FAIL;
        let unconfirmed = !direct_contact && state.unconfirmed_addresses.read().contains(&id);
        let moved = !direct_contact
            && !ip.is_empty()
            && (entry.flags.intersects(suspect) || unconfirmed)
            && !flags.intersects(suspect | NodeFlags::NOADDR)
            && (entry.ip != ip
                || entry.client_port != client_port
                || entry.cluster_port != cluster_port);
        if direct_contact || is_new || entry.ip.is_empty() || moved {
            if !ip.is_empty() {
                entry.ip = ip;
            }
            entry.client_port = client_port;
            entry.cluster_port = cluster_port;
        }
        // Take the higher epoch — never overwrite a higher claim with a lower one.
        if config_epoch > entry.config_epoch {
            entry.config_epoch = config_epoch;
        }
        entry.flags.remove(NodeFlags::HANDSHAKE);
        // Likewise for role: a node is the authority on its own. Second-hand,
        // it is only used to describe a node we are hearing of for the first
        // time.
        if direct_contact || is_new {
            if role.contains(NodeFlags::MASTER) {
                entry.flags.remove(NodeFlags::REPLICA);
                entry.flags |= NodeFlags::MASTER;
                entry.master_id = None;
            } else if role.contains(NodeFlags::REPLICA) {
                entry.flags.remove(NodeFlags::MASTER);
                entry.flags |= NodeFlags::REPLICA;
                if master_id.is_some() {
                    entry.master_id = master_id;
                }
            }
        }

        // Critical distinction: only refresh pong_received_ms when this peer
        // is the one that sent us a message. A gossip mention about this
        // peer (from a third party) tells us nothing about whether the peer
        // is reachable from US — so we don't touch the timestamp.
        if direct_contact {
            entry.pong_received_ms = now_ms();
            entry.flags.remove(NodeFlags::PFAIL);
            state.unconfirmed_addresses.write().remove(&id);
        } else if is_new {
            // Give a node we have only heard about a full timeout before
            // suspecting it, the same grace a freshly met one gets.
            entry.ping_sent_ms = now_ms();
        }
    }
    // Lock on `nodes` released — now safe to take the failure_reports lock.
    if direct_contact {
        state.clear_failure_reports(id);
    }
}

fn merge_gossip_entry(
    state: &ClusterState,
    sender_id: NodeId,
    g: GossipEntry,
    forgotten: &ahash::AHashMap<NodeId, u64>,
) {
    if g.id == state.myself() {
        return;
    }
    // Gossip is how a removed node would otherwise come straight back.
    if forgotten.get(&g.id).is_some_and(|until| *until > now_ms()) {
        return;
    }

    // "I cannot reach it" from a master is a vote toward FAIL. It is recorded
    // as a report; the flag itself is only ever set by quorum.
    if g.flags.contains(NodeFlags::PFAIL) || g.flags.contains(NodeFlags::FAIL) {
        state.record_failure_report(g.id, sender_id);
    }

    upsert_node(
        state,
        g.id,
        g.ip,
        g.client_port,
        g.cluster_port,
        g.config_epoch,
        g.flags,
        g.master_id,
        false, // gossip mention — NOT direct contact, do not refresh pong_received_ms
    );
    // Second-hand, a hostname only fills in: the sender may be a node that
    // does not carry them.
    state.set_peer_hostname(g.id, &g.hostname, false);
    state.set_peer_client_port(g.id, g.announced_client_port, false);
}

/// Build a PONG describing ourselves. Includes our slot claims and a gossip
/// section — this is the gossip propagation mechanism, piggybacking on every
/// reply.
///
/// `to`, the member it answers: if this node, a master, holds it as the
/// owner of slots while it says it is a replica, the PONG says which slots,
/// at its config epoch here, and how many masters own slots here
/// (`held_as_owner`), unless this node's vote for a replacement of it may
/// still be won (`ClusterState::owner_report_for`). That is a node
/// restarted before the save of its promotion was done: see
/// `ClusterState::promotion_to_complete`.
fn our_pong(state: &ClusterState, to: Option<NodeId>) -> BusMessage {
    let nodes = state.nodes.read();
    let me = nodes
        .get(&state.myself())
        .cloned()
        .unwrap_or_else(|| ClusterNode::new(state.myself(), String::new(), 0, 0));

    let gossip = sample_gossip(&nodes, state.myself(), GOSSIP_SAMPLE);
    let to_is_replica = me.flags.contains(NodeFlags::MASTER)
        && to.is_some_and(|id| {
            nodes
                .get(&id)
                .is_some_and(|n| n.flags.contains(NodeFlags::REPLICA))
        });
    drop(nodes);
    let held_as_owner = match to {
        Some(id) if to_is_replica => state.owner_report_for(id),
        _ => None,
    };

    BusMessage::Pong {
        sender_id: state.myself(),
        sender_ip: state.advertised_ip(&me.ip),
        sender_client_port: me.client_port,
        sender_cluster_port: me.cluster_port,
        sender_config_epoch: me.config_epoch,
        // A token an earlier version skips: see `ClusterState::note_vote_claims`.
        sender_flags: me.flags & (NodeFlags::MASTER | NodeFlags::REPLICA) | NodeFlags::VOTE_CLAIMS,
        sender_master_id: me.master_id,
        sender_hostname: me.hostname.clone(),
        sender_announced_client_port: me.announced_client_port,
        sender_client_ips: me.client_ips.clone(),
        sender_shard_id: Some(me.shard_id),
        gossip,
        slot_claims: state.my_slot_claims(),
        held_as_owner,
    }
}

/// Healthy nodes mentioned per heartbeat, on top of every suspect one.
pub(super) const GOSSIP_SAMPLE: usize = 3;

/// Pick the nodes to mention in a heartbeat's gossip section: every node we
/// currently suspect or consider failed, plus up to `limit` others at random.
///
/// Suspects are always included because the FAIL quorum is assembled from
/// these mentions. Sampling them like everything else meant a master's opinion
/// of a dead node reached a given peer only now and then, and reports expire
/// after two node timeouts — so in anything but a tiny cluster a quorum could
/// take a long time to form, or never form at all.
///
/// Excludes ourselves and any HANDSHAKE-state placeholders (those don't have
/// real ids yet).
pub(super) fn sample_gossip(
    nodes: &ahash::AHashMap<NodeId, ClusterNode>,
    myself: NodeId,
    limit: usize,
) -> Vec<crate::cluster::bus::GossipEntry> {
    let entry = |n: &ClusterNode| crate::cluster::bus::GossipEntry {
        id: n.id,
        ip: n.ip.clone(),
        client_port: n.client_port,
        cluster_port: n.cluster_port,
        config_epoch: n.config_epoch,
        flags: n.flags
            & (NodeFlags::MASTER | NodeFlags::REPLICA | NodeFlags::PFAIL | NodeFlags::FAIL),
        master_id: n.master_id,
        hostname: n.hostname.clone(),
        announced_client_port: n.announced_client_port,
    };

    let mut out = Vec::new();
    let mut healthy: Vec<&ClusterNode> = Vec::new();
    for n in nodes.values() {
        if n.id == myself || n.flags.contains(NodeFlags::HANDSHAKE) {
            continue;
        }
        if n.flags.intersects(NodeFlags::PFAIL | NodeFlags::FAIL) {
            out.push(entry(n));
        } else {
            healthy.push(n);
        }
    }

    // A different few each time, so that every node is mentioned to every
    // peer before long: this is how a node learns of the ones it was never
    // introduced to. The choice used to be a window starting at the clock's
    // nanoseconds modulo the count — and where the clock ticks in 100 ns
    // steps (Windows), that is zero for a count of 4 or 5, so the same three
    // nodes were mentioned for ever and a cluster of six or more never
    // finished forming.
    use rand::seq::SliceRandom;
    out.extend(
        healthy
            .choose_multiple(&mut rand::thread_rng(), limit)
            .map(|n| entry(n)),
    );
    out
}

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::bus::{OwnerReport, SlotClaim};
    use crate::cluster::node::generate_node_id;

    fn peer() -> SocketAddr {
        "10.0.0.9:50000".parse().unwrap()
    }

    fn ping(sender: NodeId, flags: NodeFlags, gossip: Vec<GossipEntry>) -> BusMessage {
        BusMessage::Ping {
            sender_id: sender,
            sender_ip: String::new(),
            sender_client_port: 7000,
            sender_cluster_port: 17000,
            sender_config_epoch: 1,
            sender_flags: flags,
            sender_master_id: None,
            sender_hostname: String::new(),
            sender_announced_client_port: 0,
            sender_client_ips: ClientIps::default(),
            sender_shard_id: None,
            gossip,
            slot_claims: vec![],
        }
    }

    fn meet(sender: NodeId) -> BusMessage {
        BusMessage::Meet {
            sender_id: sender,
            sender_ip: String::new(),
            sender_client_port: 7000,
            sender_cluster_port: 17000,
            sender_config_epoch: 0,
        }
    }

    /// A heartbeat of `sender`, with its role, master, config epoch and
    /// claims (each range at that epoch).
    fn heartbeat(
        sender: NodeId,
        flags: NodeFlags,
        master: Option<NodeId>,
        epoch: u64,
        claims: &[(u16, u16)],
    ) -> BusMessage {
        let mut msg = ping(sender, flags, vec![]);
        if let BusMessage::Ping {
            sender_master_id,
            sender_config_epoch,
            slot_claims,
            ..
        } = &mut msg
        {
            *sender_master_id = master;
            *sender_config_epoch = epoch;
            *slot_claims = claims
                .iter()
                .map(|&(start, end)| SlotClaim { start, end, epoch })
                .collect();
        }
        msg
    }

    /// A member `id` of `cs`, as a master with `claims` at `epoch`, or as a
    /// replica of `master`.
    fn member(
        cs: &ClusterState,
        master: Option<NodeId>,
        epoch: u64,
        claims: &[(u16, u16)],
    ) -> NodeId {
        member_as(cs, generate_node_id(), master, epoch, claims)
    }

    /// [`member`], with the id given.
    fn member_as(
        cs: &ClusterState,
        id: NodeId,
        master: Option<NodeId>,
        epoch: u64,
        claims: &[(u16, u16)],
    ) -> NodeId {
        process_message(cs, meet(id), peer());
        let flags = if master.is_some() {
            NodeFlags::REPLICA
        } else {
            NodeFlags::MASTER
        };
        process_message(cs, heartbeat(id, flags, master, epoch, claims), peer());
        id
    }

    /// An `UPDATE` relays an owner only as that owner claimed itself, at
    /// the epoch it claimed at: never this node's slot table for it at the
    /// config epoch gossip raised it to (#742). The voter heard b claim
    /// 50-60 at 9; b has since given slot 50 to c at 10 and moved to 12,
    /// and the voter heard only b's new epoch, by gossip, not c's claim
    /// nor b's own new one. Relayed at 12, slot 50 went to b on a candidate
    /// that held it as c's at 10, and c's claim never won it back there.
    #[test]
    fn an_update_relays_what_the_owner_claimed_itself_not_a_gossiped_epoch() {
        let voter = ClusterState::new_enabled(None, 15000);
        voter.claim_slot_range(200, 299).unwrap();
        set_my_epoch(&voter, 2);
        let a = member(&voter, None, 3, &[(0, 99)]);
        let b = member(&voter, None, 9, &[(50, 60)]);
        // As gossip raises it.
        voter.nodes.write().get_mut(&b).unwrap().config_epoch = 12;
        let owners = voter.newer_owners_of_claimed(
            a,
            &[SlotClaim {
                start: 0,
                end: 99,
                epoch: 3,
            }],
        );
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0].id, b);
        assert_eq!(owners[0].config_epoch, 9);
        let got: Vec<(u16, u16, u64)> = owners[0]
            .claims
            .iter()
            .map(|c| (c.start, c.end, c.epoch))
            .collect();
        assert_eq!(got, vec![(50, 60, 9)]);

        // The candidate holds slot 50 as c's, at 10, from c itself.
        let cand = ClusterState::new_enabled(None, 15000);
        member_as(&cand, a, None, 3, &[(0, 99)]);
        cand.set_replica_of(a).unwrap();
        let c = member(&cand, None, 10, &[(50, 50)]);
        assert_eq!(cand.slot_owner(50), Some(c));
        apply_update(&cand, voter.myself(), owners);
        assert_eq!(cand.slot_owner(50), Some(c), "a relayed claim overrode c's");
        assert_eq!(cand.slot_owner(51), Some(b));
        assert_eq!(cand.slot_owner(61), Some(a));
    }

    fn set_my_epoch(cs: &ClusterState, epoch: u64) {
        let me = cs.myself();
        cs.nodes.write().get_mut(&me).unwrap().config_epoch = epoch;
        cs.observe_epoch(epoch);
    }

    /// A replica of this node that took its slots over (`TAKEOVER`) at
    /// this node's own config epoch (one it took unannounced) keeps them:
    /// at its first heartbeat as a master, this node gives them up at the
    /// tie, and does not move to a newer epoch on the collision, which
    /// would have its next claim take them back whatever the ids (#743).
    #[test]
    fn a_takeover_at_the_masters_own_epoch_keeps_the_slots_on_the_master() {
        // Left with none, the master follows it.
        let cs = ClusterState::new_enabled(None, 15000);
        cs.claim_slot_range(0, 99).unwrap();
        set_my_epoch(&cs, 7);
        let r = member(&cs, Some(cs.myself()), 0, &[]);
        process_message(
            &cs,
            heartbeat(r, NodeFlags::MASTER, None, 7, &[(0, 99)]),
            peer(),
        );
        assert_eq!(cs.slot_owner(50), Some(r));
        assert_eq!(cs.my_master_id(), Some(r));

        // Left with some, it keeps them, at its epoch, whatever the ids.
        for _ in 0..8 {
            let cs = ClusterState::new_enabled(None, 15000);
            cs.claim_slot_range(0, 199).unwrap();
            set_my_epoch(&cs, 7);
            let r = member(&cs, Some(cs.myself()), 0, &[]);
            for _ in 0..3 {
                process_message(
                    &cs,
                    heartbeat(r, NodeFlags::MASTER, None, 7, &[(0, 99)]),
                    peer(),
                );
            }
            assert_eq!(cs.slot_owner(50), Some(r));
            assert_eq!(cs.slot_owner(150), Some(cs.myself()));
            assert_eq!(cs.my_master_id(), None);
            assert_eq!(
                cs.nodes.read()[&cs.myself()].config_epoch,
                7,
                "moved above it"
            );
        }

        // A master that was never its replica, claiming at the same epoch,
        // takes nothing: equal epochs keep the owner.
        let cs = ClusterState::new_enabled(None, 15000);
        cs.claim_slot_range(0, 99).unwrap();
        set_my_epoch(&cs, 7);
        let x = member(&cs, None, 7, &[]);
        process_message(
            &cs,
            heartbeat(x, NodeFlags::MASTER, None, 7, &[(0, 99)]),
            peer(),
        );
        assert_eq!(cs.slot_owner(50), Some(cs.myself()));
    }

    /// The same at a node that had heard the master take that epoch: the
    /// replica's first heartbeat as a master takes the slots at the tie.
    #[test]
    fn a_takeover_at_the_masters_own_epoch_wins_on_every_node_that_knew_it_as_a_replica() {
        let cs = ClusterState::new_enabled(None, 15000);
        cs.claim_slot_range(200, 299).unwrap();
        set_my_epoch(&cs, 2);
        let a = member(&cs, None, 7, &[(0, 99)]);
        let r = member(&cs, Some(a), 0, &[]);
        assert_eq!(cs.slot_owner(50), Some(a));
        process_message(
            &cs,
            heartbeat(r, NodeFlags::MASTER, None, 7, &[(0, 99)]),
            peer(),
        );
        assert_eq!(cs.slot_owner(50), Some(r));
        // The old master's own claim at that epoch does not take them back.
        process_message(
            &cs,
            heartbeat(a, NodeFlags::MASTER, None, 7, &[(0, 99)]),
            peer(),
        );
        assert_eq!(cs.slot_owner(50), Some(r));
    }

    /// A master that refuses a vote because it knows a newer owner of slots
    /// the candidate claims answers with an `UPDATE` naming that owner, its
    /// epoch and its slots; the vote is not spent (#742). A request whose
    /// claims do not read as claims is refused, and nothing is answered.
    #[test]
    fn a_vote_refused_for_a_newer_owner_is_answered_with_it() {
        let cs = ClusterState::new_enabled(None, 15000);
        cs.claim_slot_range(200, 299).unwrap();
        set_my_epoch(&cs, 2);
        let a = member(&cs, None, 3, &[(0, 99)]);
        let b = member(&cs, None, 9, &[(50, 60), (300, 310)]);
        let r = member(&cs, Some(a), 0, &[]);
        cs.nodes.write().get_mut(&a).unwrap().flags |= NodeFlags::FAIL;
        let request =
            |claims: Option<Vec<SlotClaim>>, unreadable: bool| BusMessage::FailoverAuthRequest {
                sender_id: r,
                sender_ip: "10.0.0.9".into(),
                sender_client_port: 7000,
                sender_cluster_port: 17000,
                request_epoch: 20,
                master_id: a,
                force: false,
                claims,
                claims_unreadable: unreadable,
            };
        let stale = vec![SlotClaim {
            start: 0,
            end: 99,
            epoch: 3,
        }];
        let reply = process_message(&cs, request(Some(stale.clone()), false), peer()).unwrap();
        match crate::cluster::bus::message::decode(&reply)
            .unwrap()
            .unwrap()
            .0
        {
            BusMessage::Update { sender_id, owners } => {
                assert_eq!(sender_id, cs.myself());
                assert_eq!(owners.len(), 1);
                assert_eq!(owners[0].id, b);
                assert_eq!(owners[0].config_epoch, 9);
                let got: Vec<(u16, u16, u64)> = owners[0]
                    .claims
                    .iter()
                    .map(|c| (c.start, c.end, c.epoch))
                    .collect();
                assert_eq!(got, vec![(50, 60, 9), (300, 310, 9)]);
            }
            other => panic!("expected an UPDATE, got {other:?}"),
        }
        assert_eq!(cs.last_vote_epoch.load(Ordering::SeqCst), 0);
        // A forced request with the same claims, the same.
        let mut forced = request(Some(stale), false);
        if let BusMessage::FailoverAuthRequest { force, .. } = &mut forced {
            *force = true;
        }
        assert!(process_message(&cs, forced, peer()).is_some());
        assert_eq!(cs.last_vote_epoch.load(Ordering::SeqCst), 0);
        // Unreadable claims: refused, unanswered.
        assert!(process_message(&cs, request(None, true), peer()).is_none());
        assert_eq!(cs.last_vote_epoch.load(Ordering::SeqCst), 0);
        // Without claims (an earlier version): refused for nothing here,
        // and never answered with an UPDATE.
        cs.nodes
            .write()
            .get_mut(&a)
            .unwrap()
            .flags
            .remove(NodeFlags::FAIL);
        assert!(process_message(&cs, request(None, false), peer()).is_none());
    }

    /// A candidate takes an `UPDATE` in as the owner's own claims: its
    /// master's slots in its view, and so its next request's claims, are
    /// what the master still has (#742). An owner it did not know is added.
    /// Left with nothing, it follows the owner.
    #[test]
    fn an_update_puts_the_candidates_claims_right() {
        let cs = ClusterState::new_enabled(None, 15000);
        let a = member(&cs, None, 3, &[(0, 99)]);
        cs.set_replica_of(a).unwrap();
        let b = generate_node_id();
        let owner = |claims: Vec<(u16, u16)>| crate::cluster::bus::OwnerUpdate {
            id: b,
            ip: "10.0.0.7".into(),
            client_port: 7007,
            cluster_port: 17007,
            config_epoch: 9,
            claims: claims
                .into_iter()
                .map(|(start, end)| SlotClaim {
                    start,
                    end,
                    epoch: 9,
                })
                .collect(),
        };
        let voter = generate_node_id();
        apply_update(&cs, voter, vec![owner(vec![(50, 60), (300, 310)])]);
        let left: Vec<(u16, u16)> = cs
            .slot_claims_of(a)
            .iter()
            .map(|c| (c.start, c.end))
            .collect();
        assert_eq!(left, vec![(0, 49), (61, 99)]);
        assert_eq!(cs.nodes.read()[&b].config_epoch, 9);
        assert_eq!(cs.my_master_id(), Some(a));
        // An older claim changes nothing.
        let mut old = owner(vec![(0, 10)]);
        old.config_epoch = 2;
        old.claims[0].epoch = 2;
        apply_update(&cs, voter, vec![old]);
        assert_eq!(cs.slot_owner(5), Some(a));
        // The rest: the master has nothing left, and this node follows b.
        apply_update(&cs, voter, vec![owner(vec![(0, 99)])]);
        assert_eq!(cs.my_master_id(), Some(b));
    }

    /// A node advertises `vote-claims` in its own heartbeats, and its peers
    /// note it from those only (#742).
    #[test]
    fn the_vote_claims_capability_is_learnt_from_a_nodes_own_heartbeat() {
        let cs = ClusterState::new_enabled(None, 15000);
        let x = member(&cs, None, 1, &[]);
        assert!(!cs.decides_on_claims(&x));
        process_message(
            &cs,
            heartbeat(x, NodeFlags::MASTER | NodeFlags::VOTE_CLAIMS, None, 1, &[]),
            peer(),
        );
        assert!(cs.decides_on_claims(&x));
        // An earlier version's heartbeat, after a downgrade: forgotten.
        process_message(&cs, heartbeat(x, NodeFlags::MASTER, None, 1, &[]), peer());
        assert!(!cs.decides_on_claims(&x));
        let BusMessage::Pong { sender_flags, .. } = our_pong(&cs, None) else {
            panic!()
        };
        assert!(sender_flags.contains(NodeFlags::VOTE_CLAIMS));
        // Never in the node table.
        process_message(
            &cs,
            heartbeat(x, NodeFlags::MASTER | NodeFlags::VOTE_CLAIMS, None, 1, &[]),
            peer(),
        );
        assert!(!cs.nodes.read()[&x].flags.contains(NodeFlags::VOTE_CLAIMS));
    }

    /// A node that sends no address of its own is recorded at the address it
    /// connected from — not at a loopback placeholder.
    #[test]
    fn empty_sender_address_falls_back_to_the_observed_one() {
        let cs = ClusterState::new_enabled(None, 15000);
        let other = generate_node_id();
        process_message(&cs, meet(other), peer());
        assert_eq!(cs.nodes.read().get(&other).unwrap().ip, "10.0.0.9");
    }

    /// A stranger's PING is answered but does not enrol it.
    #[test]
    fn ping_from_an_unknown_node_does_not_add_it() {
        let cs = ClusterState::new_enabled(None, 15000);
        let stranger = generate_node_id();
        let reply = process_message(&cs, ping(stranger, NodeFlags::MASTER, vec![]), peer());
        assert!(reply.is_some(), "a PING is always answered");
        assert!(!cs.nodes.read().contains_key(&stranger));
    }

    /// CLUSTER FORGET has to stick: neither the node's own heartbeat nor a
    /// third party's gossip may bring it back while the ban lasts.
    #[test]
    fn a_forgotten_node_is_not_readmitted_by_ping_or_gossip() {
        let cs = ClusterState::new_enabled(None, 15000);
        let (member, gone) = (generate_node_id(), generate_node_id());
        process_message(&cs, meet(member), peer());
        process_message(&cs, meet(gone), peer());
        assert!(cs.forget_node(gone));

        process_message(&cs, ping(gone, NodeFlags::MASTER, vec![]), peer());
        assert!(
            !cs.nodes.read().contains_key(&gone),
            "re-added by its own PING"
        );

        let about_gone = GossipEntry {
            hostname: String::new(),
            announced_client_port: 0,
            id: gone,
            ip: "10.0.0.7".into(),
            client_port: 7001,
            cluster_port: 17001,
            config_epoch: 0,
            flags: NodeFlags::MASTER,
            master_id: None,
        };
        process_message(
            &cs,
            ping(member, NodeFlags::MASTER, vec![about_gone]),
            peer(),
        );
        assert!(!cs.nodes.read().contains_key(&gone), "re-added by gossip");

        // An explicit MEET is the operator changing their mind.
        process_message(&cs, meet(gone), peer());
        assert!(cs.nodes.read().contains_key(&gone));
    }

    /// A heartbeat can pass the membership check just before RESET HARD and
    /// reach the merge step just after it. The old cluster must stay gone.
    #[test]
    fn hard_reset_rejects_an_in_flight_heartbeat_from_the_old_cluster() {
        let cs = ClusterState::new_enabled(None, 15000);
        let old_peer = generate_node_id();
        process_message(&cs, meet(old_peer), peer());

        cs.hard_reset();
        apply_heartbeat(
            &cs,
            old_peer,
            "10.0.0.9".into(),
            7000,
            17000,
            1,
            NodeFlags::MASTER,
            None,
            String::new(),
            0,
            ClientIps::default(),
            None,
            vec![],
            &[SlotClaim {
                start: 0,
                end: 8191,
                epoch: 1,
            }],
        );

        assert!(!cs.nodes.read().contains_key(&old_peer));
        assert_eq!(cs.assigned_slot_count(), 0);
    }

    /// One node's opinion that another has failed is a report, not a verdict.
    #[test]
    fn gossiped_failure_flags_are_reports_not_verdicts() {
        let cs = ClusterState::new_enabled(None, 15000);
        let (a, b) = (generate_node_id(), generate_node_id());
        process_message(&cs, meet(a), peer());
        process_message(&cs, meet(b), peer());

        let b_failed = GossipEntry {
            hostname: String::new(),
            announced_client_port: 0,
            id: b,
            ip: "10.0.0.8".into(),
            client_port: 7002,
            cluster_port: 17002,
            config_epoch: 0,
            flags: NodeFlags::MASTER | NodeFlags::FAIL,
            master_id: None,
        };
        process_message(&cs, ping(a, NodeFlags::MASTER, vec![b_failed]), peer());

        let nodes = cs.nodes.read();
        assert!(
            !nodes.get(&b).unwrap().flags.contains(NodeFlags::FAIL),
            "a single node's gossip must not mark another FAIL"
        );
        drop(nodes);
        assert_eq!(cs.failure_report_count(b), 1);
    }

    fn gossip_of(id: NodeId, ip: &str, port: u16, flags: NodeFlags) -> GossipEntry {
        GossipEntry {
            hostname: String::new(),
            announced_client_port: 0,
            id,
            ip: ip.into(),
            client_port: port,
            cluster_port: port + 10000,
            config_epoch: 0,
            flags,
            master_id: None,
        }
    }

    /// A node we cannot reach, relayed at a new address by one that can
    /// reach it, has moved: we dial it there. Two pods restarted together
    /// with new IPs otherwise kept each other's old ones for ever.
    #[test]
    fn a_node_we_cannot_reach_takes_the_address_a_healthy_relay_gives() {
        let cs = ClusterState::new_enabled(None, 15000);
        let (a, b) = (generate_node_id(), generate_node_id());
        process_message(&cs, meet(a), peer());
        process_message(
            &cs,
            ping(
                a,
                NodeFlags::MASTER,
                vec![gossip_of(b, "10.0.0.8", 7002, NodeFlags::MASTER)],
            ),
            peer(),
        );
        cs.nodes.write().get_mut(&b).unwrap().flags |= NodeFlags::PFAIL;

        let moved = gossip_of(b, "10.0.0.88", 7002, NodeFlags::MASTER);
        process_message(&cs, ping(a, NodeFlags::MASTER, vec![moved]), peer());
        let nodes = cs.nodes.read();
        let node = nodes.get(&b).unwrap();
        assert_eq!(node.ip, "10.0.0.88");
        assert!(
            node.flags.contains(NodeFlags::PFAIL),
            "only a reply from the node itself clears our suspicion"
        );
    }

    /// After a restart, a peer we have not heard from yet has the address
    /// nodes.conf gave it, which may be stale: a healthy relay's address
    /// replaces it at once, before the start grace would let us suspect it.
    #[test]
    fn after_a_restart_a_peer_not_yet_heard_from_takes_a_healthy_relays_address() {
        let cs = ClusterState::new_enabled(None, 15000);
        let (a, b) = (generate_node_id(), generate_node_id());
        process_message(&cs, meet(a), peer());
        let known = gossip_of(b, "10.0.0.8", 7002, NodeFlags::MASTER);
        process_message(&cs, ping(a, NodeFlags::MASTER, vec![known]), peer());
        cs.start_the_clock_on_peers();

        let moved = gossip_of(b, "10.0.0.88", 7002, NodeFlags::MASTER);
        process_message(&cs, ping(a, NodeFlags::MASTER, vec![moved]), peer());
        let nodes = cs.nodes.read();
        let node = nodes.get(&b).unwrap();
        assert_eq!(node.ip, "10.0.0.88");
        assert!(!node.flags.intersects(NodeFlags::PFAIL | NodeFlags::FAIL));
    }

    /// A node we reach keeps the address it gives itself, whatever a third
    /// party says; and one the relay cannot reach either keeps the one we
    /// have.
    #[test]
    fn a_relayed_address_does_not_replace_a_reachable_one_or_come_from_a_suspect() {
        let cs = ClusterState::new_enabled(None, 15000);
        let (a, b) = (generate_node_id(), generate_node_id());
        process_message(&cs, meet(a), peer());
        process_message(
            &cs,
            ping(
                a,
                NodeFlags::MASTER,
                vec![gossip_of(b, "10.0.0.8", 7002, NodeFlags::MASTER)],
            ),
            peer(),
        );

        let stale = gossip_of(b, "10.0.0.99", 7002, NodeFlags::MASTER);
        process_message(&cs, ping(a, NodeFlags::MASTER, vec![stale]), peer());
        assert_eq!(cs.nodes.read().get(&b).unwrap().ip, "10.0.0.8");

        cs.nodes.write().get_mut(&b).unwrap().flags |= NodeFlags::PFAIL;
        let suspect = gossip_of(b, "10.0.0.99", 7002, NodeFlags::MASTER | NodeFlags::PFAIL);
        process_message(&cs, ping(a, NodeFlags::MASTER, vec![suspect]), peer());
        assert_eq!(cs.nodes.read().get(&b).unwrap().ip, "10.0.0.8");
    }

    /// A replica's suspicion is not a vote.
    #[test]
    fn failure_reports_from_replicas_do_not_count() {
        let cs = ClusterState::new_enabled(None, 15000);
        let (master, replica, target) =
            (generate_node_id(), generate_node_id(), generate_node_id());
        process_message(&cs, meet(master), peer());
        process_message(&cs, meet(replica), peer());
        process_message(&cs, meet(target), peer());

        let target_pfail = || GossipEntry {
            hostname: String::new(),
            announced_client_port: 0,
            id: target,
            ip: "10.0.0.8".into(),
            client_port: 7002,
            cluster_port: 17002,
            config_epoch: 0,
            flags: NodeFlags::MASTER | NodeFlags::PFAIL,
            master_id: None,
        };
        // The replica announces its role in its own PING, then reports.
        let mut from_replica = ping(replica, NodeFlags::REPLICA, vec![target_pfail()]);
        if let BusMessage::Ping {
            sender_master_id, ..
        } = &mut from_replica
        {
            *sender_master_id = Some(master);
        }
        process_message(&cs, from_replica, peer());
        assert_eq!(cs.failure_report_count(target), 0);

        process_message(
            &cs,
            ping(master, NodeFlags::MASTER, vec![target_pfail()]),
            peer(),
        );
        assert_eq!(cs.failure_report_count(target), 1);
    }

    /// The failover completes on the old master's side too: when it sees its
    /// slots claimed at a higher epoch, it steps down and follows the winner.
    /// A write routed here before, and not yet in the write gate, is routed
    /// again there (#337).
    #[test]
    fn a_master_that_lost_all_its_slots_becomes_a_replica_of_the_new_owner() {
        // Following the winner ends a handoff's pause, which is the process's.
        let _pause = crate::pause::TEST_LOCK.lock();
        let cs = ClusterState::new_enabled(None, 15000);
        let me = cs.myself();
        cs.claim_slot_range(0, 99).unwrap();
        let winner = generate_node_id();
        process_message(&cs, meet(winner), peer());

        let mut takeover = ping(winner, NodeFlags::MASTER, vec![]);
        if let BusMessage::Ping {
            slot_claims,
            sender_config_epoch,
            ..
        } = &mut takeover
        {
            *sender_config_epoch = 5;
            *slot_claims = vec![SlotClaim {
                start: 0,
                end: 99,
                epoch: 5,
            }];
        }
        let routed = crate::pause::routes_now();
        process_message(&cs, takeover, peer());
        assert!(crate::pause::routes_moved_since(routed));

        assert_eq!(cs.my_slot_count(), 0);
        let nodes = cs.nodes.read();
        let my_node = nodes.get(&me).unwrap();
        assert!(my_node.flags.contains(NodeFlags::REPLICA));
        assert!(!my_node.flags.contains(NodeFlags::MASTER));
        assert_eq!(my_node.master_id, Some(winner));
    }

    /// A PONG to a node held here as the owner of slots that says it is a
    /// replica says which slots, at its config epoch here; to a master that
    /// owns them, or a replica that owns none, it says nothing. What a
    /// master's PONG says of this node is noted, and withdrawn by one that
    /// says nothing.
    #[test]
    fn a_pong_tells_a_replica_the_slots_it_is_held_as_the_owner_of() {
        let cs = ClusterState::new_enabled(None, 15000);
        let restarted = generate_node_id();
        process_message(&cs, meet(restarted), peer());
        let mut announced = ping(restarted, NodeFlags::MASTER, vec![]);
        if let BusMessage::Ping {
            slot_claims,
            sender_config_epoch,
            ..
        } = &mut announced
        {
            *sender_config_epoch = 7;
            *slot_claims = vec![SlotClaim {
                start: 0,
                end: 99,
                epoch: 7,
            }];
        }
        let held = |reply: Option<Vec<u8>>| {
            let (msg, _) = crate::cluster::bus::message::decode(&reply.unwrap())
                .unwrap()
                .unwrap();
            let BusMessage::Pong { held_as_owner, .. } = msg else {
                panic!("expected a PONG");
            };
            held_as_owner
                .map(|r| {
                    assert_eq!(r.masters, 1, "the masters that own slots here");
                    r.claims
                        .iter()
                        .map(|c| (c.start, c.end, c.epoch))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        };
        // A master owning the slots: nothing.
        assert!(held(process_message(&cs, announced, peer())).is_empty());
        // The same node, restarted as a replica: the slots, at epoch 7.
        let mut as_replica = ping(restarted, NodeFlags::REPLICA, vec![]);
        if let BusMessage::Ping {
            sender_master_id, ..
        } = &mut as_replica
        {
            *sender_master_id = Some(generate_node_id());
        }
        assert_eq!(
            held(process_message(&cs, as_replica.clone(), peer())),
            [(0, 99, 7)]
        );
        // A replica that owns nothing: nothing.
        let other = generate_node_id();
        process_message(&cs, meet(other), peer());
        assert!(
            held(process_message(
                &cs,
                ping(other, NodeFlags::REPLICA, vec![]),
                peer()
            ))
            .is_empty()
        );

        // On the other side: noted from a PONG, and withdrawn.
        let master = generate_node_id();
        process_message(&cs, meet(master), peer());
        let pong = |held_as_owner: Option<OwnerReport>| BusMessage::Pong {
            sender_id: master,
            sender_ip: String::new(),
            sender_client_port: 7000,
            sender_cluster_port: 17000,
            sender_config_epoch: 1,
            sender_flags: NodeFlags::MASTER,
            sender_master_id: None,
            sender_hostname: String::new(),
            sender_announced_client_port: 0,
            sender_client_ips: ClientIps::default(),
            sender_shard_id: None,
            gossip: vec![],
            slot_claims: vec![],
            held_as_owner,
        };
        process_message(
            &cs,
            pong(Some(OwnerReport {
                masters: 3,
                claims: vec![SlotClaim {
                    start: 0,
                    end: 9,
                    epoch: 3,
                }],
            })),
            peer(),
        );
        assert_eq!(cs.held_as_owner_by(master), Some(vec![(0, 9, 3)]));
        process_message(&cs, pong(None), peer());
        assert_eq!(cs.held_as_owner_by(master), None);
    }

    /// A master `r` that owns 0-99 at config epoch 7 in `cs`'s view, which
    /// owns 100-199, and `r`'s replica `q`; `r` FAIL. Returns the PING of
    /// `r` restarted as a replica (of another node).
    fn a_failed_master_and_its_replica(cs: &ClusterState) -> (NodeId, NodeId, BusMessage) {
        cs.claim_slot_range(100, 199).unwrap();
        let r = generate_node_id();
        process_message(cs, meet(r), peer());
        let mut announced = ping(r, NodeFlags::MASTER, vec![]);
        if let BusMessage::Ping {
            slot_claims,
            sender_config_epoch,
            ..
        } = &mut announced
        {
            *sender_config_epoch = 7;
            *slot_claims = vec![SlotClaim {
                start: 0,
                end: 99,
                epoch: 7,
            }];
        }
        process_message(cs, announced, peer());
        let q = generate_node_id();
        process_message(cs, meet(q), peer());
        let mut follows = ping(q, NodeFlags::REPLICA, vec![]);
        if let BusMessage::Ping {
            sender_master_id, ..
        } = &mut follows
        {
            *sender_master_id = Some(r);
        }
        process_message(cs, follows, peer());
        cs.nodes.write().get_mut(&r).unwrap().flags |= NodeFlags::FAIL;
        let mut as_replica = ping(r, NodeFlags::REPLICA, vec![]);
        if let BusMessage::Ping {
            sender_master_id, ..
        } = &mut as_replica
        {
            *sender_master_id = Some(generate_node_id());
        }
        (r, q, as_replica)
    }

    fn report_in(reply: Option<Vec<u8>>) -> Option<OwnerReport> {
        let (msg, _) = crate::cluster::bus::message::decode(&reply.unwrap())
            .unwrap()
            .unwrap();
        let BusMessage::Pong { held_as_owner, .. } = msg else {
            panic!("expected a PONG");
        };
        held_as_owner
    }

    /// A master that voted for a replacement of a node sends that node no
    /// report while the vote may still be won and announced: the node is not
    /// to complete a promotion of its own while the winner's newer claim is
    /// on its way.
    #[test]
    fn a_master_that_voted_for_a_replacement_sends_no_report() {
        let cs = ClusterState::new_enabled(None, 15000);
        let (r, q, as_replica) = a_failed_master_and_its_replica(&cs);
        assert!(cs.cast_vote(q, r, 100, false, None));
        assert_eq!(report_in(process_message(&cs, as_replica, peer())), None);
        // Another master, that gave no vote, reports.
        let other = ClusterState::new_enabled(None, 15000);
        let (_, _, as_replica) = a_failed_master_and_its_replica(&other);
        let report = report_in(process_message(&other, as_replica, peer())).unwrap();
        assert_eq!(report.masters, 2);
        assert_eq!(
            report
                .claims
                .iter()
                .map(|c| (c.start, c.end, c.epoch))
                .collect::<Vec<_>>(),
            [(0, 99, 7)]
        );
    }

    /// A master that reported to a node that says it is a replica votes
    /// for no replacement of it, forced or not, while the node may be
    /// completing its promotion on that report; once the node says it is a
    /// master again, it does.
    #[test]
    fn a_master_that_reported_votes_for_no_replacement() {
        let cs = ClusterState::new_enabled(None, 15000);
        let (r, q, as_replica) = a_failed_master_and_its_replica(&cs);
        assert!(report_in(process_message(&cs, as_replica, peer())).is_some());
        cs.nodes.write().get_mut(&r).unwrap().flags |= NodeFlags::FAIL;
        assert!(!cs.cast_vote(q, r, 100, false, None));
        assert!(!cs.cast_vote(q, r, 101, true, None));
        // Completed: it says it is a master.
        let mut completed = ping(r, NodeFlags::MASTER, vec![]);
        if let BusMessage::Ping {
            sender_config_epoch,
            ..
        } = &mut completed
        {
            *sender_config_epoch = 7;
        }
        process_message(&cs, completed, peer());
        assert!(cs.cast_vote(q, r, 102, true, None));
    }

    /// A vote decided while the node's first PING as a replica comes in, in
    /// between the vote's look at the node table and its taking of
    /// `last_vote_for`: the PING is answered with a report, and the vote,
    /// which reads the node's role under the lock, is refused. This node
    /// never both reports to the node and votes for its replacement.
    #[test]
    fn a_report_made_while_a_vote_is_decided_refuses_the_vote() {
        let cs = std::sync::Arc::new(ClusterState::new_enabled(None, 15000));
        let (r, q, as_replica) = a_failed_master_and_its_replica(&cs);
        let reported = std::rc::Rc::new(std::cell::Cell::new(false));
        {
            let (cs, reported) = (std::sync::Arc::clone(&cs), std::rc::Rc::clone(&reported));
            crate::cluster::run_before_vote_lock(move || {
                let report = report_in(process_message(&cs, as_replica, peer()));
                reported.set(report.is_some());
            });
        }
        let voted = cs.cast_vote(q, r, 100, false, None);
        assert!(reported.get(), "the PING was answered with a report");
        assert!(!voted, "voted for a replacement of a node it reported to");
    }

    /// Reports to a node that says it is a replica end three node timeouts
    /// after the first, if it has not said it is a master by then; votes for
    /// a replacement of it are refused until three node timeouts after the
    /// last report, and given after. A restart of the node (more PINGs as a
    /// replica) does not start the reports again; its saying it is a master
    /// does.
    #[test]
    fn reports_to_a_node_that_does_not_complete_end_and_votes_follow() {
        let node_timeout = 40;
        let cs = ClusterState::new_enabled(None, node_timeout);
        let (r, q, as_replica) = a_failed_master_and_its_replica(&cs);
        let began = std::time::Instant::now();
        assert!(report_in(process_message(&cs, as_replica.clone(), peer())).is_some());
        assert!(!cs.cast_vote(q, r, 100, true, None));
        // Reported on every PING, until the limit.
        let mut last_report = began;
        loop {
            std::thread::sleep(std::time::Duration::from_millis(5));
            if report_in(process_message(&cs, as_replica.clone(), peer())).is_none() {
                break;
            }
            last_report = std::time::Instant::now();
            assert!(
                began.elapsed() < std::time::Duration::from_millis(node_timeout * 3 + 30),
                "still reporting after {:?}",
                began.elapsed()
            );
        }
        assert!(began.elapsed() >= std::time::Duration::from_millis(node_timeout * 3));
        // Refused while the last report may still count, given after.
        if last_report.elapsed() < std::time::Duration::from_millis(node_timeout * 3 - 20) {
            assert!(!cs.cast_vote(q, r, 101, true, None));
        }
        // Restarted, as a replica again: still no report.
        process_message(&cs, meet(r), peer());
        assert!(report_in(process_message(&cs, as_replica.clone(), peer())).is_none());
        std::thread::sleep(std::time::Duration::from_millis(node_timeout * 3 + 10));
        assert!(report_in(process_message(&cs, as_replica.clone(), peer())).is_none());
        assert!(cs.cast_vote(q, r, 102, true, None));

        // Its saying it is a master starts the reports afresh.
        let cs = ClusterState::new_enabled(None, node_timeout);
        let (r, _q, as_replica) = a_failed_master_and_its_replica(&cs);
        assert!(report_in(process_message(&cs, as_replica.clone(), peer())).is_some());
        std::thread::sleep(std::time::Duration::from_millis(node_timeout * 3 + 10));
        assert!(report_in(process_message(&cs, as_replica.clone(), peer())).is_none());
        let mut master = ping(r, NodeFlags::MASTER, vec![]);
        if let BusMessage::Ping {
            sender_config_epoch,
            ..
        } = &mut master
        {
            *sender_config_epoch = 7;
        }
        process_message(&cs, master, peer());
        assert!(report_in(process_message(&cs, as_replica, peer())).is_some());
    }

    /// Losing *some* slots is a resharding, not a failover.
    #[test]
    fn a_master_that_keeps_some_slots_stays_a_master() {
        let cs = ClusterState::new_enabled(None, 15000);
        let me = cs.myself();
        cs.claim_slot_range(0, 99).unwrap();
        let other = generate_node_id();
        process_message(&cs, meet(other), peer());

        let mut partial = ping(other, NodeFlags::MASTER, vec![]);
        if let BusMessage::Ping {
            slot_claims,
            sender_config_epoch,
            ..
        } = &mut partial
        {
            *sender_config_epoch = 5;
            *slot_claims = vec![SlotClaim {
                start: 0,
                end: 49,
                epoch: 5,
            }];
        }
        let routed = crate::pause::routes_now();
        process_message(&cs, partial, peer());
        assert!(crate::pause::routes_moved_since(routed));

        assert_eq!(cs.my_slot_count(), 50);
        assert!(
            cs.nodes
                .read()
                .get(&me)
                .unwrap()
                .flags
                .contains(NodeFlags::MASTER)
        );
    }

    /// Slots assigned at config epoch 0 must still be learned by peers.
    #[test]
    fn claims_for_unowned_slots_are_accepted_at_epoch_zero() {
        let cs = ClusterState::new_enabled(None, 15000);
        let other = generate_node_id();
        process_message(&cs, meet(other), peer());

        let mut claim = ping(other, NodeFlags::MASTER, vec![]);
        if let BusMessage::Ping {
            slot_claims,
            sender_config_epoch,
            ..
        } = &mut claim
        {
            *sender_config_epoch = 0;
            *slot_claims = vec![SlotClaim {
                start: 100,
                end: 199,
                epoch: 0,
            }];
        }
        process_message(&cs, claim, peer());
        assert_eq!(cs.slot_owner(150), Some(other));
    }

    /// Suspects are always in the gossip section, however many healthy nodes
    /// there are to choose from.
    #[test]
    fn suspect_nodes_are_always_gossiped() {
        let me = generate_node_id();
        let mut nodes = ahash::AHashMap::new();
        for _ in 0..20 {
            let id = generate_node_id();
            nodes.insert(id, ClusterNode::new(id, "10.0.0.1".into(), 7000, 17000));
        }
        let suspect = generate_node_id();
        let mut n = ClusterNode::new(suspect, "10.0.0.2".into(), 7000, 17000);
        n.flags |= NodeFlags::PFAIL;
        nodes.insert(suspect, n);

        for _ in 0..50 {
            let sample = sample_gossip(&nodes, me, GOSSIP_SAMPLE);
            assert!(sample.iter().any(|g| g.id == suspect));
            assert_eq!(sample.len(), GOSSIP_SAMPLE + 1);
        }
    }

    /// Whatever the number of nodes, each of them is mentioned sooner or
    /// later, and no heartbeat mentions one twice.
    #[test]
    fn gossip_gets_round_to_every_node() {
        let me = generate_node_id();
        for count in 1..=12 {
            let mut nodes: ahash::AHashMap<NodeId, ClusterNode> = ahash::AHashMap::new();
            nodes.insert(me, ClusterNode::new(me, "10.0.0.9".into(), 7000, 17000));
            for _ in 0..count {
                let id = generate_node_id();
                nodes.insert(id, ClusterNode::new(id, "10.0.0.1".into(), 7000, 17000));
            }
            let mut mentioned = std::collections::HashSet::new();
            for _ in 0..400 {
                let sample = sample_gossip(&nodes, me, GOSSIP_SAMPLE);
                assert_eq!(sample.len(), GOSSIP_SAMPLE.min(count));
                let ids: std::collections::HashSet<NodeId> = sample.iter().map(|g| g.id).collect();
                assert_eq!(ids.len(), sample.len(), "a node mentioned twice");
                assert!(!ids.contains(&me));
                mentioned.extend(ids);
            }
            assert_eq!(mentioned.len(), count, "with {count} other nodes");
        }
    }
}
