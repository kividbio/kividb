// src/cluster/bus/listener.rs
//
// Bus listener: accepts incoming peer connections on the cluster port
// (typically client_port + 10000) and processes their messages.
//
// One tokio task per accepted connection. The task reads bus messages in a
// loop, dispatches them, and may write replies (PONG for MEET/PING).

use crate::cluster::ClusterState;
use crate::cluster::bus::message::{BusMessage, GossipEntry, decode, encode, exceeds_buffer_limit};
use crate::cluster::node::{ClusterNode, NodeFlags, NodeId, node_id_str};
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
    let addr: SocketAddr = format!("{bind}:{bus_port}").parse().map_err(|e| {
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
                    let reply = match msg {
                        // The one message whose answer has to wait on the
                        // rest of the server; everything else is synchronous.
                        BusMessage::MfStart { sender_id } => {
                            crate::cluster::bus::heartbeat::answer_mfstart(&state, sender_id)
                                .await
                                .map(|m| encode(&m))
                        }
                        other => process_message(&state, other, peer),
                    };
                    buf.drain(0..consumed);
                    if let Some(bytes) = reply {
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
            upsert_node(
                state,
                sender_id,
                resolve_ip(sender_ip),
                sender_client_port,
                sender_cluster_port,
                sender_config_epoch,
                NodeFlags::MASTER,
                None,
                true,
            );
            // Save nodes.conf so the new peer survives restart.
            state.save_soon();

            // Reply with our own PONG so the peer learns OUR id.
            Some(encode(&our_pong(state)))
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
        } => {
            // A PING from a node we do not know is answered — so it can see
            // we are alive — but it does not get itself added by pinging.
            // Otherwise a node removed with CLUSTER FORGET, which still has
            // us in *its* table, would re-add itself on its next heartbeat.
            if sender_is_member(state, &sender_id) {
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
                    gossip,
                    &slot_claims,
                );
            }
            Some(encode(&our_pong(state)))
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
        } => {
            // A PONG is a reply to something we sent. From a stranger it is
            // only acceptable as the answer to our own MEET — which the
            // dialer handles through `accept_meet_reply`, not through here.
            if sender_is_member(state, &sender_id) {
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
                    gossip,
                    &slot_claims,
                );
            }
            None
        }
        BusMessage::FailoverAuthRequest {
            sender_id,
            request_epoch,
            master_id,
            force,
            ..
        } => {
            if state.cast_vote(sender_id, master_id, request_epoch, force) {
                log_info!(
                    "Cluster: Voted for {} to take over from {} at epoch {}{}",
                    node_id_str(&sender_id),
                    node_id_str(&master_id),
                    request_epoch,
                    if force { " (manual failover)" } else { "" },
                );
                let _ = state.save_to_disk();
                Some(encode(&BusMessage::FailoverAuthAck {
                    sender_id: state.myself(),
                    request_epoch,
                }))
            } else {
                None // silently reject
            }
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
            // Only a member's verdict counts, and never one about ourselves:
            // we know we are up.
            if sender_is_member(state, &sender_id) && state.mark_node_failed(failed_id) {
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
/// from a node we do not know yet is taken at its word.
pub fn accept_meet_reply(state: &ClusterState, msg: BusMessage, peer_addr: SocketAddr) {
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
    } = msg
    {
        state.forgotten.write().remove(&sender_id);
        let ip = if sender_ip.is_empty() || sender_ip == "0.0.0.0" {
            peer_addr.ip().to_string()
        } else {
            sender_ip
        };
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
            gossip,
            &slot_claims,
        );
    }
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
    if sender_flags.contains(NodeFlags::MASTER) {
        state.resolve_epoch_collision(sender_id, config_epoch);
    }
    // What a node says of its own hostname stands, none included.
    state.set_peer_hostname(sender_id, &sender_hostname, true);
    state.set_peer_client_port(sender_id, sender_announced_client_port, true);
    // It reached us, so whatever we concluded about it deserves a second look.
    state.maybe_clear_fail_on_contact(sender_id);

    for g in gossip {
        merge_gossip_entry(state, sender_id, g, &forgotten);
    }
    // A replica announces no slots; claims only ever come from masters.
    if sender_flags.contains(NodeFlags::MASTER) {
        state.apply_peer_slot_claims(sender_id, slot_claims);
    }
    state.save_soon();
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
        // relays, which may be stale. A relayed address only fills a gap.
        if direct_contact || is_new || entry.ip.is_empty() {
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
fn our_pong(state: &ClusterState) -> BusMessage {
    let nodes = state.nodes.read();
    let me = nodes
        .get(&state.myself())
        .cloned()
        .unwrap_or_else(|| ClusterNode::new(state.myself(), String::new(), 0, 0));

    let gossip = sample_gossip(&nodes, state.myself(), GOSSIP_SAMPLE);
    drop(nodes);

    BusMessage::Pong {
        sender_id: state.myself(),
        sender_ip: state.advertised_ip(&me.ip),
        sender_client_port: me.client_port,
        sender_cluster_port: me.cluster_port,
        sender_config_epoch: me.config_epoch,
        sender_flags: me.flags & (NodeFlags::MASTER | NodeFlags::REPLICA),
        sender_master_id: me.master_id,
        sender_hostname: me.hostname.clone(),
        sender_announced_client_port: me.announced_client_port,
        gossip,
        slot_claims: state.my_slot_claims(),
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
    use crate::cluster::bus::SlotClaim;
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
    #[test]
    fn a_master_that_lost_all_its_slots_becomes_a_replica_of_the_new_owner() {
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
        process_message(&cs, takeover, peer());

        assert_eq!(cs.my_slot_count(), 0);
        let nodes = cs.nodes.read();
        let my_node = nodes.get(&me).unwrap();
        assert!(my_node.flags.contains(NodeFlags::REPLICA));
        assert!(!my_node.flags.contains(NodeFlags::MASTER));
        assert_eq!(my_node.master_id, Some(winner));
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
        process_message(&cs, partial, peer());

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
