// src/cluster/bus/message.rs
//
// Wire format for cluster bus messages. RESP-based for simplicity.
//
// Message types:
//   MEET <sender_id> <sender_endpoint>
//     "I don't know you yet, please add me to your routing table."
//
//   PING <sender_id> <sender_endpoint> <gossip_array>
//     "I'm alive. Here are some peers I know about, in case you don't."
//
//   PONG <sender_id> <sender_endpoint> <gossip_array>
//     "Reply to your PING/MEET. Same gossip payload as PING."
//
//   FAILOVER_AUTH_REQUEST <sender_id> <endpoint> <master_id> [force]
//   FAILOVER_AUTH_ACK <sender_id> <epoch>
//     Replica election. `force` marks a manual failover.
//
//   FAIL <sender_id> <failed_id>
//     "A majority of masters agree this node is down."
//
//   PUBLISH <sender_id> <channel> <payload> <c|s>
//     A message published on another node, for local subscribers.
//
//   MFSTART <sender_id>  /  MFACK <sender_id> <offset>
//     Manual-failover handshake between a replica and its master.
//
//   OFFSET <sender_id>  /  OFFSET_ACK <sender_id> <offset>
//     A replica whose master failed asks its fellow replicas how far each has
//     read, to rank itself before it stands for election. A node that does not
//     know OFFSET drops the connection: the asker counts it as unknown.
//
// <sender_endpoint> is a colon-packed string:
//     "<ip>:<client_port>:<cluster_port>:<config_epoch>"
// Yes, this means an IPv6 address would break the parser. We use IPv4 only
// for now — adding [::1]-style bracketing is a one-line fix when needed.
//
// <gossip_array> is a RESP array of 4-element subarrays:
//     [node_id, endpoint, flags_string, master_id_or_empty]
//
// A node's hostname (`cluster-announce-hostname`) travels in its flags
// string, as a `host=<name>` token: nodes that do not know it ignore an
// unknown token, where a new field would have them drop the message. A
// hostname has no commas (see `valid_hostname`). Decoders take at least the
// fields they know and skip any after them, so that a later version can add
// some.
// Up to 3 entries per heartbeat; receiver merges them into its routing table.

use crate::cluster::node::{NodeFlags, NodeId, parse_node_id};
use crate::resp::write::{resp_array, resp_bulk, resp_bulk_str};

/// One node's entry inside a gossip payload.
#[derive(Debug, Clone)]
pub struct GossipEntry {
    pub id: NodeId,
    pub ip: String,
    pub client_port: u16,
    pub cluster_port: u16,
    pub config_epoch: u64,
    pub flags: NodeFlags,
    pub master_id: Option<NodeId>,
    /// Empty when the node announces none, or the sender does not know it.
    pub hostname: String,
    /// The node's `cluster-announce-client-port`; 0 when none, or unknown.
    /// Travels in the flags string as `cport=<port>`, as the hostname does.
    pub announced_client_port: u16,
}

/// One slot range the sender claims to own, with the epoch at which the
/// claim was made. Receivers compare against their own ownership and
/// keep the higher epoch's claim.
#[derive(Debug, Clone)]
pub struct SlotClaim {
    pub start: u16,
    pub end: u16,
    pub epoch: u64,
}

#[derive(Debug, Clone)]
pub enum BusMessage {
    Meet {
        sender_id: NodeId,
        sender_ip: String,
        sender_client_port: u16,
        sender_cluster_port: u16,
        sender_config_epoch: u64,
    },
    Ping {
        sender_id: NodeId,
        sender_ip: String,
        sender_client_port: u16,
        sender_cluster_port: u16,
        sender_config_epoch: u64,
        sender_flags: NodeFlags,
        sender_master_id: Option<NodeId>,
        /// Empty when the sender announces none.
        sender_hostname: String,
        /// Its `cluster-announce-client-port`; 0 when it announces none.
        sender_announced_client_port: u16,
        gossip: Vec<GossipEntry>,
        slot_claims: Vec<SlotClaim>,
    },
    Pong {
        sender_id: NodeId,
        sender_ip: String,
        sender_client_port: u16,
        sender_cluster_port: u16,
        sender_config_epoch: u64,
        sender_flags: NodeFlags,
        sender_master_id: Option<NodeId>,
        /// Empty when the sender announces none.
        sender_hostname: String,
        /// Its `cluster-announce-client-port`; 0 when it announces none.
        sender_announced_client_port: u16,
        gossip: Vec<GossipEntry>,
        slot_claims: Vec<SlotClaim>,
    },
    /// "I'm a replica of a FAIL'd master, and I want your vote to take over."
    /// Sent by a replica during election. The receiver (a master) replies
    /// with FailoverAuthAck if it agrees.
    FailoverAuthRequest {
        sender_id: NodeId,
        sender_ip: String,
        sender_client_port: u16,
        sender_cluster_port: u16,
        /// The epoch the replica is claiming for itself.
        request_epoch: u64,
        /// The replica's master, currently FAIL'd.
        master_id: NodeId,
        /// A manual failover: grant the vote although the master is healthy.
        force: bool,
    },

    /// "Yes, you can take over." Sent by a master in response to a valid
    /// FailoverAuthRequest. The replying master records the epoch as its
    /// last_vote_epoch to prevent voting twice in the same epoch.
    FailoverAuthAck {
        sender_id: NodeId,
        request_epoch: u64,
    },

    /// "A majority of masters agree `failed_id` is down." Sent to every node
    /// by whichever one reaches quorum, so the verdict does not have to
    /// trickle out three gossip entries at a time while replicas wait on it.
    Fail {
        sender_id: NodeId,
        failed_id: NodeId,
    },

    /// A `PUBLISH` (or `SPUBLISH`, when `sharded`) issued on another node,
    /// forwarded so subscribers connected to this one receive it.
    Publish {
        sender_id: NodeId,
        channel: Vec<u8>,
        payload: Vec<u8>,
        sharded: bool,
    },

    /// Manual failover, step one: a replica asks its master to stop taking
    /// writes and say how far the replica has to catch up.
    MfStart { sender_id: NodeId },

    /// The master's answer: writes are paused, and the replica is caught up
    /// once its replication offset reaches `target_offset`.
    MfAck {
        sender_id: NodeId,
        target_offset: u64,
    },

    /// A replica asks another replica of the same master for its replication
    /// offset, to rank itself before an election.
    OffsetQuery { sender_id: NodeId },

    /// The answer: the replica's replication offset.
    OffsetAck { sender_id: NodeId, offset: u64 },
}

impl BusMessage {
    pub fn sender_id(&self) -> &NodeId {
        match self {
            BusMessage::Meet { sender_id, .. }
            | BusMessage::Ping { sender_id, .. }
            | BusMessage::Pong { sender_id, .. }
            | BusMessage::FailoverAuthRequest { sender_id, .. }
            | BusMessage::FailoverAuthAck { sender_id, .. }
            | BusMessage::Fail { sender_id, .. }
            | BusMessage::Publish { sender_id, .. }
            | BusMessage::MfStart { sender_id }
            | BusMessage::MfAck { sender_id, .. }
            | BusMessage::OffsetQuery { sender_id }
            | BusMessage::OffsetAck { sender_id, .. } => sender_id,
        }
    }
}

// ── Encoding ──────────────────────────────────────────────────────────────────

pub fn encode(msg: &BusMessage) -> Vec<u8> {
    match msg {
        BusMessage::Meet {
            sender_id,
            sender_ip,
            sender_client_port,
            sender_cluster_port,
            sender_config_epoch,
        } => resp_array(vec![
            resp_bulk_str("MEET"),
            resp_bulk(sender_id),
            resp_bulk_str(&format_endpoint(
                sender_ip,
                *sender_client_port,
                *sender_cluster_port,
                *sender_config_epoch,
            )),
        ]),
        BusMessage::Ping {
            sender_id,
            sender_ip,
            sender_client_port,
            sender_cluster_port,
            sender_config_epoch,
            sender_flags,
            sender_master_id,
            sender_hostname,
            sender_announced_client_port,
            gossip,
            slot_claims,
        } => resp_array(vec![
            resp_bulk_str("PING"),
            resp_bulk(sender_id),
            resp_bulk_str(&format_endpoint(
                sender_ip,
                *sender_client_port,
                *sender_cluster_port,
                *sender_config_epoch,
            )),
            resp_bulk_str(&flags_with_host(
                *sender_flags,
                sender_hostname,
                *sender_announced_client_port,
            )),
            match sender_master_id {
                Some(m) => resp_bulk(m),
                None => resp_bulk(b""),
            },
            encode_gossip(gossip),
            encode_slot_claims(slot_claims),
        ]),
        BusMessage::Pong {
            sender_id,
            sender_ip,
            sender_client_port,
            sender_cluster_port,
            sender_config_epoch,
            sender_flags,
            sender_master_id,
            sender_hostname,
            sender_announced_client_port,
            gossip,
            slot_claims,
        } => resp_array(vec![
            resp_bulk_str("PONG"),
            resp_bulk(sender_id),
            resp_bulk_str(&format_endpoint(
                sender_ip,
                *sender_client_port,
                *sender_cluster_port,
                *sender_config_epoch,
            )),
            resp_bulk_str(&flags_with_host(
                *sender_flags,
                sender_hostname,
                *sender_announced_client_port,
            )),
            match sender_master_id {
                Some(m) => resp_bulk(m),
                None => resp_bulk(b""),
            },
            encode_gossip(gossip),
            encode_slot_claims(slot_claims),
        ]),
        BusMessage::FailoverAuthRequest {
            sender_id,
            sender_ip,
            sender_client_port,
            sender_cluster_port,
            request_epoch,
            master_id,
            force,
        } => {
            let mut fields = vec![
                resp_bulk_str("FAILOVER_AUTH_REQUEST"),
                resp_bulk(sender_id),
                resp_bulk_str(&format_endpoint(
                    sender_ip,
                    *sender_client_port,
                    *sender_cluster_port,
                    *request_epoch,
                )),
                resp_bulk(master_id),
            ];
            // Only present when set, so an ordinary request stays in the
            // four-field form that nodes running an older build accept.
            if *force {
                fields.push(resp_bulk_str("force"));
            }
            resp_array(fields)
        }
        BusMessage::FailoverAuthAck {
            sender_id,
            request_epoch,
        } => resp_array(vec![
            resp_bulk_str("FAILOVER_AUTH_ACK"),
            resp_bulk(sender_id),
            resp_bulk_str(&request_epoch.to_string()),
        ]),
        BusMessage::Fail {
            sender_id,
            failed_id,
        } => resp_array(vec![
            resp_bulk_str("FAIL"),
            resp_bulk(sender_id),
            resp_bulk(failed_id),
        ]),
        BusMessage::Publish {
            sender_id,
            channel,
            payload,
            sharded,
        } => resp_array(vec![
            resp_bulk_str("PUBLISH"),
            resp_bulk(sender_id),
            resp_bulk(channel),
            resp_bulk(payload),
            resp_bulk_str(if *sharded { "s" } else { "c" }),
        ]),
        BusMessage::MfStart { sender_id } => {
            resp_array(vec![resp_bulk_str("MFSTART"), resp_bulk(sender_id)])
        }
        BusMessage::MfAck {
            sender_id,
            target_offset,
        } => resp_array(vec![
            resp_bulk_str("MFACK"),
            resp_bulk(sender_id),
            resp_bulk_str(&target_offset.to_string()),
        ]),
        BusMessage::OffsetQuery { sender_id } => {
            resp_array(vec![resp_bulk_str("OFFSET"), resp_bulk(sender_id)])
        }
        BusMessage::OffsetAck { sender_id, offset } => resp_array(vec![
            resp_bulk_str("OFFSET_ACK"),
            resp_bulk(sender_id),
            resp_bulk_str(&offset.to_string()),
        ]),
    }
}

fn encode_slot_claims(claims: &[SlotClaim]) -> Vec<u8> {
    let entries: Vec<Vec<u8>> = claims
        .iter()
        .map(|c| {
            // Each claim is a 3-element array: [start, end, epoch] as bulks.
            // Yes, integers as bulks — keeps the parser uniform.
            resp_array(vec![
                resp_bulk_str(&c.start.to_string()),
                resp_bulk_str(&c.end.to_string()),
                resp_bulk_str(&c.epoch.to_string()),
            ])
        })
        .collect();
    resp_array(entries)
}

fn format_endpoint(ip: &str, client_port: u16, cluster_port: u16, epoch: u64) -> String {
    format!("{ip}:{client_port}:{cluster_port}:{epoch}")
}

fn encode_gossip(gossip: &[GossipEntry]) -> Vec<u8> {
    let entries: Vec<Vec<u8>> = gossip
        .iter()
        .map(|g| {
            resp_array(vec![
                resp_bulk(&g.id),
                resp_bulk_str(&format_endpoint(
                    &g.ip,
                    g.client_port,
                    g.cluster_port,
                    g.config_epoch,
                )),
                resp_bulk_str(&flags_with_host(
                    g.flags,
                    &g.hostname,
                    g.announced_client_port,
                )),
                match g.master_id {
                    Some(m) => resp_bulk(&m),
                    None => resp_bulk(b""),
                },
            ])
        })
        .collect();
    resp_array(entries)
}

fn format_flags(f: NodeFlags) -> String {
    let mut parts: Vec<&str> = Vec::new();
    if f.contains(NodeFlags::MASTER) {
        parts.push("master");
    }
    if f.contains(NodeFlags::REPLICA) {
        parts.push("replica");
    }
    if f.contains(NodeFlags::PFAIL) {
        parts.push("pfail");
    }
    if f.contains(NodeFlags::FAIL) {
        parts.push("fail");
    }
    parts.join(",")
}

/// The flags string with the node's hostname and announced client port in
/// it, where it has them.
fn flags_with_host(flags: NodeFlags, hostname: &str, client_port: u16) -> String {
    let mut s = format_flags(flags);
    if !hostname.is_empty() {
        if !s.is_empty() {
            s.push(',');
        }
        s.push_str("host=");
        s.push_str(hostname);
    }
    if client_port != 0 {
        if !s.is_empty() {
            s.push(',');
        }
        s.push_str("cport=");
        s.push_str(&client_port.to_string());
    }
    s
}

/// The announced client port in a flags string; 0 if there is none.
fn cport_of(flags: &str) -> u16 {
    flags
        .split(',')
        .find_map(|tok| tok.strip_prefix("cport="))
        .and_then(|p| p.parse().ok())
        .unwrap_or(0)
}

/// The hostname in a flags string; empty if there is none.
fn host_of(flags: &str) -> String {
    flags
        .split(',')
        .find_map(|tok| tok.strip_prefix("host="))
        .unwrap_or("")
        .to_string()
}

fn parse_flags(s: &str) -> NodeFlags {
    let mut f = NodeFlags::empty();
    for tok in s.split(',') {
        match tok {
            "master" => f |= NodeFlags::MASTER,
            "replica" | "slave" => f |= NodeFlags::REPLICA,
            "pfail" | "fail?" => f |= NodeFlags::PFAIL,
            "fail" => f |= NodeFlags::FAIL,
            _ => {}
        }
    }
    f
}

// ── Decoding ──────────────────────────────────────────────────────────────────

/// Parse a single bus message. Returns None if the buffer doesn't yet
/// contain a complete message — callers should read more and retry.
/// Returns Err if the buffer contains malformed data — callers should
/// drop the connection.
pub fn decode(buf: &[u8]) -> Result<Option<(BusMessage, usize)>, String> {
    let (parts, consumed) = match parse_resp_array(buf) {
        Ok(Some(v)) => v,
        Ok(None) => return Ok(None),
        Err(e) => return Err(e),
    };
    if parts.is_empty() {
        return Err("empty bus message".into());
    }

    let msg_type = std::str::from_utf8(&parts[0]).map_err(|_| "bad msg type")?;
    match msg_type {
        "MEET" => {
            if parts.len() != 3 {
                return Err(format!("MEET expects 3 fields, got {}", parts.len()));
            }
            let sender_id = parse_node_id(&parts[1]).ok_or("bad sender id")?;
            let endpoint = std::str::from_utf8(&parts[2]).map_err(|_| "bad endpoint utf8")?;
            let (ip, cp, busp, ep) = parse_endpoint(endpoint)?;
            Ok(Some((
                BusMessage::Meet {
                    sender_id,
                    sender_ip: ip,
                    sender_client_port: cp,
                    sender_cluster_port: busp,
                    sender_config_epoch: ep,
                },
                consumed,
            )))
        }
        "PING" | "PONG" => {
            if parts.len() < 7 {
                return Err(format!("{msg_type} expects 7 fields, got {}", parts.len()));
            }
            let sender_id = parse_node_id(&parts[1]).ok_or("bad sender id")?;
            let endpoint = std::str::from_utf8(&parts[2]).map_err(|_| "bad endpoint utf8")?;
            let (sender_ip, sender_client_port, sender_cluster_port, sender_config_epoch) =
                parse_endpoint(endpoint)?;
            let flags_str = std::str::from_utf8(&parts[3]).map_err(|_| "bad flags utf8")?;
            let sender_flags = parse_flags(flags_str);
            let sender_hostname = host_of(flags_str);
            let sender_announced_client_port = cport_of(flags_str);
            let sender_master_id = if parts[4].is_empty() {
                None
            } else {
                parse_node_id(&parts[4])
            };
            let gossip = parse_gossip(&parts[5])?;
            let slot_claims = parse_slot_claims(&parts[6])?;
            let msg = if msg_type == "PING" {
                BusMessage::Ping {
                    sender_id,
                    sender_ip,
                    sender_client_port,
                    sender_cluster_port,
                    sender_config_epoch,
                    sender_flags,
                    sender_master_id,
                    sender_hostname,
                    sender_announced_client_port,
                    gossip,
                    slot_claims,
                }
            } else {
                BusMessage::Pong {
                    sender_id,
                    sender_ip,
                    sender_client_port,
                    sender_cluster_port,
                    sender_config_epoch,
                    sender_flags,
                    sender_master_id,
                    sender_hostname,
                    sender_announced_client_port,
                    gossip,
                    slot_claims,
                }
            };
            Ok(Some((msg, consumed)))
        }
        "FAILOVER_AUTH_REQUEST" => {
            if parts.len() != 4 && parts.len() != 5 {
                return Err(format!(
                    "FAILOVER_AUTH_REQUEST expects 4 or 5 fields, got {}",
                    parts.len()
                ));
            }
            let force = parts.get(4).is_some_and(|f| f.as_slice() == b"force");
            let sender_id = parse_node_id(&parts[1]).ok_or("bad sender id")?;
            let endpoint = std::str::from_utf8(&parts[2]).map_err(|_| "bad endpoint utf8")?;
            let (ip, cp, busp, ep) = parse_endpoint(endpoint)?;
            let master_id = parse_node_id(&parts[3]).ok_or("bad master id")?;
            Ok(Some((
                BusMessage::FailoverAuthRequest {
                    sender_id,
                    sender_ip: ip,
                    sender_client_port: cp,
                    sender_cluster_port: busp,
                    request_epoch: ep,
                    master_id,
                    force,
                },
                consumed,
            )))
        }
        "FAILOVER_AUTH_ACK" => {
            if parts.len() != 3 {
                return Err(format!(
                    "FAILOVER_AUTH_ACK expects 3 fields, got {}",
                    parts.len()
                ));
            }
            let sender_id = parse_node_id(&parts[1]).ok_or("bad sender id")?;
            let request_epoch: u64 = std::str::from_utf8(&parts[2])
                .map_err(|_| "bad epoch utf8")?
                .parse()
                .map_err(|_| "bad epoch parse")?;
            Ok(Some((
                BusMessage::FailoverAuthAck {
                    sender_id,
                    request_epoch,
                },
                consumed,
            )))
        }
        "FAIL" => {
            if parts.len() != 3 {
                return Err(format!("FAIL expects 3 fields, got {}", parts.len()));
            }
            let sender_id = parse_node_id(&parts[1]).ok_or("bad sender id")?;
            let failed_id = parse_node_id(&parts[2]).ok_or("bad failed id")?;
            Ok(Some((
                BusMessage::Fail {
                    sender_id,
                    failed_id,
                },
                consumed,
            )))
        }
        "PUBLISH" => {
            if parts.len() != 5 {
                return Err(format!("PUBLISH expects 5 fields, got {}", parts.len()));
            }
            let sender_id = parse_node_id(&parts[1]).ok_or("bad sender id")?;
            let sharded = match parts[4].as_slice() {
                b"s" => true,
                b"c" => false,
                _ => return Err("bad PUBLISH kind".into()),
            };
            let mut parts = parts;
            let payload = std::mem::take(&mut parts[3]);
            let channel = std::mem::take(&mut parts[2]);
            Ok(Some((
                BusMessage::Publish {
                    sender_id,
                    channel,
                    payload,
                    sharded,
                },
                consumed,
            )))
        }
        "MFSTART" => {
            if parts.len() != 2 {
                return Err(format!("MFSTART expects 2 fields, got {}", parts.len()));
            }
            let sender_id = parse_node_id(&parts[1]).ok_or("bad sender id")?;
            Ok(Some((BusMessage::MfStart { sender_id }, consumed)))
        }
        "MFACK" => {
            if parts.len() != 3 {
                return Err(format!("MFACK expects 3 fields, got {}", parts.len()));
            }
            let sender_id = parse_node_id(&parts[1]).ok_or("bad sender id")?;
            let target_offset: u64 = std::str::from_utf8(&parts[2])
                .map_err(|_| "bad offset utf8")?
                .parse()
                .map_err(|_| "bad offset parse")?;
            Ok(Some((
                BusMessage::MfAck {
                    sender_id,
                    target_offset,
                },
                consumed,
            )))
        }
        "OFFSET" => {
            if parts.len() < 2 {
                return Err(format!("OFFSET expects 2 fields, got {}", parts.len()));
            }
            let sender_id = parse_node_id(&parts[1]).ok_or("bad sender id")?;
            Ok(Some((BusMessage::OffsetQuery { sender_id }, consumed)))
        }
        "OFFSET_ACK" => {
            if parts.len() < 3 {
                return Err(format!("OFFSET_ACK expects 3 fields, got {}", parts.len()));
            }
            let sender_id = parse_node_id(&parts[1]).ok_or("bad sender id")?;
            let offset: u64 = std::str::from_utf8(&parts[2])
                .map_err(|_| "bad offset utf8")?
                .parse()
                .map_err(|_| "bad offset parse")?;
            Ok(Some((
                BusMessage::OffsetAck { sender_id, offset },
                consumed,
            )))
        }
        other => Err(format!("unknown bus message type: {other}")),
    }
}

fn parse_endpoint(s: &str) -> Result<(String, u16, u16, u64), String> {
    // Format: "ip:client_port:cluster_port:config_epoch"
    // We use rsplitn so the IP can contain colons (it can't here since we're
    // IPv4 only, but the code is structured to be safe).
    let parts: Vec<&str> = s.splitn(4, ':').collect();
    if parts.len() != 4 {
        return Err(format!("bad endpoint: {s}"));
    }
    let ip = parts[0].to_string();
    let cp: u16 = parts[1]
        .parse()
        .map_err(|_| format!("bad client port: {}", parts[1]))?;
    let busp: u16 = parts[2]
        .parse()
        .map_err(|_| format!("bad cluster port: {}", parts[2]))?;
    let ep: u64 = parts[3]
        .parse()
        .map_err(|_| format!("bad epoch: {}", parts[3]))?;
    Ok((ip, cp, busp, ep))
}

fn parse_gossip(buf: &[u8]) -> Result<Vec<GossipEntry>, String> {
    // The outer array contains N nested 4-element arrays. parse_resp_array
    // returned each nested array as a single raw blob — we now re-parse
    // each blob to get its four fields out.
    let (raw_entries, _) = parse_resp_array(buf)
        .map_err(|e| format!("gossip array: {e}"))?
        .ok_or_else(|| "gossip array incomplete".to_string())?;

    let mut out = Vec::with_capacity(raw_entries.len());
    for raw in raw_entries {
        let (fields, _) = parse_resp_array(&raw)
            .map_err(|e| format!("gossip entry: {e}"))?
            .ok_or_else(|| "gossip entry incomplete".to_string())?;
        if fields.len() < 4 {
            return Err(format!(
                "gossip entry expects 4 fields, got {}",
                fields.len()
            ));
        }
        let id = parse_node_id(&fields[0]).ok_or("bad gossip node id")?;
        let endpoint = std::str::from_utf8(&fields[1]).map_err(|_| "bad gossip endpoint utf8")?;
        let (ip, cp, busp, ep) = parse_endpoint(endpoint)?;
        let flags_str = std::str::from_utf8(&fields[2]).map_err(|_| "bad flags utf8")?;
        let flags = parse_flags(flags_str);
        let master_id = if fields[3].is_empty() {
            None
        } else {
            parse_node_id(&fields[3])
        };
        out.push(GossipEntry {
            id,
            ip,
            client_port: cp,
            cluster_port: busp,
            config_epoch: ep,
            flags,
            master_id,
            hostname: host_of(flags_str),
            announced_client_port: cport_of(flags_str),
        });
    }
    Ok(out)
}

fn parse_slot_claims(buf: &[u8]) -> Result<Vec<SlotClaim>, String> {
    let (raw_entries, _) = parse_resp_array(buf)
        .map_err(|e| format!("slot_claims array: {e}"))?
        .ok_or_else(|| "slot_claims array incomplete".to_string())?;

    let mut out = Vec::with_capacity(raw_entries.len());
    for raw in raw_entries {
        let (fields, _) = parse_resp_array(&raw)
            .map_err(|e| format!("slot_claim entry: {e}"))?
            .ok_or_else(|| "slot_claim entry incomplete".to_string())?;
        if fields.len() != 3 {
            return Err(format!("slot_claim expects 3 fields, got {}", fields.len()));
        }
        let start: u16 = std::str::from_utf8(&fields[0])
            .map_err(|_| "slot_claim start utf8")?
            .parse()
            .map_err(|_| "slot_claim start parse")?;
        let end: u16 = std::str::from_utf8(&fields[1])
            .map_err(|_| "slot_claim end utf8")?
            .parse()
            .map_err(|_| "slot_claim end parse")?;
        let epoch: u64 = std::str::from_utf8(&fields[2])
            .map_err(|_| "slot_claim epoch utf8")?
            .parse()
            .map_err(|_| "slot_claim epoch parse")?;
        out.push(SlotClaim { start, end, epoch });
    }
    Ok(out)
}

// ── Minimal RESP array parser ─────────────────────────────────────────────────
//
// We need RESP parsing here. The full RESP parser in src/resp/read.rs is
// async and works against an AsyncRead. The bus uses simple framed reads:
// we accumulate bytes until we have a complete message, then decode. So we
// roll a tiny synchronous parser tailored to the bus.

type RespArrayResponse = Result<Option<(Vec<Vec<u8>>, usize)>, String>;

// The bus port is unauthenticated, so this parser is fed by whoever can reach
// it. Each of these bounds closes a way one small hostile message could take
// the node down:
//
//   * An element count is a promise, not a fact. It used to size an
//     allocation directly — `*4000000000` asked for tens of gigabytes before
//     a single element had arrived.
//   * A bulk length was added to an offset unchecked; a length near
//     `usize::MAX` wrapped the sum and indexed out of bounds.
//   * Arrays nest by recursion. `*1\r\n*1\r\n*1\r\n...` recursed once per
//     level until the stack ran out, which is not a catchable error.
//
// The limits are far above anything a real message contains: the widest array
// is a node's slot ranges (at most 8192, if it owns every other slot), and
// the deepest nesting is message → section → entry.

/// Most elements any one array may declare.
const MAX_ARRAY_ELEMENTS: usize = 20_000;
/// Deepest array nesting accepted.
const MAX_ARRAY_DEPTH: usize = 4;
/// Largest single bulk string: a forwarded PUBLISH payload is the only large
/// one, and this is the bus's cap on it.
pub const MAX_BULK_LEN: usize = 64 * 1024 * 1024;

pub(super) fn parse_resp_array(buf: &[u8]) -> RespArrayResponse {
    parse_resp_array_at_depth(buf, 0)
}

fn parse_resp_array_at_depth(buf: &[u8], depth: usize) -> RespArrayResponse {
    if depth >= MAX_ARRAY_DEPTH {
        return Err("bus message nests too deeply".into());
    }
    if buf.is_empty() {
        return Ok(None);
    }
    if buf[0] != b'*' {
        return Err(format!("expected '*', got {:?}", buf[0] as char));
    }
    let line_end = match find_crlf(buf, 1) {
        Some(p) => p,
        None => return Ok(None),
    };
    let count: usize = std::str::from_utf8(&buf[1..line_end])
        .map_err(|_| "array count utf8")?
        .parse()
        .map_err(|_| "array count parse")?;
    if count > MAX_ARRAY_ELEMENTS {
        return Err(format!("array of {count} elements exceeds the bus limit"));
    }
    let mut pos = line_end + 2;
    // Sized by what has arrived, not by what was announced.
    let mut out: Vec<Vec<u8>> = Vec::with_capacity(count.min(64));

    for _ in 0..count {
        if pos >= buf.len() {
            return Ok(None);
        }
        match buf[pos] {
            b'$' => {
                let len_end = match find_crlf(buf, pos + 1) {
                    Some(p) => p,
                    None => return Ok(None),
                };
                let len: usize = std::str::from_utf8(&buf[pos + 1..len_end])
                    .map_err(|_| "bulk len utf8")?
                    .parse()
                    .map_err(|_| "bulk len parse")?;
                if len > MAX_BULK_LEN {
                    return Err(format!("bulk of {len} bytes exceeds the bus limit"));
                }
                let body_start = len_end + 2;
                let body_end = body_start + len;
                if buf.len() < body_end + 2 {
                    return Ok(None);
                }
                out.push(buf[body_start..body_end].to_vec());
                pos = body_end + 2;
            }
            b'*' => {
                // Nested array — return its raw bytes as a single "bulk"
                // so the caller can re-parse it. This is how we handle
                // the gossip subarray inside PING/PONG.
                let nested_start = pos;
                let (_, nested_consumed) = match parse_resp_array_at_depth(&buf[pos..], depth + 1)?
                {
                    Some(v) => v,
                    None => return Ok(None),
                };
                let nested_end = nested_start + nested_consumed;
                out.push(buf[nested_start..nested_end].to_vec());
                pos = nested_end;
            }
            other => {
                return Err(format!(
                    "expected '$' or '*' at element start, got {:?}",
                    other as char
                ));
            }
        }
    }

    Ok(Some((out, pos)))
}

fn find_crlf(buf: &[u8], start: usize) -> Option<usize> {
    (start..buf.len().saturating_sub(1)).find(|&i| buf[i] == b'\r' && buf[i + 1] == b'\n')
}

/// Whether `buf` can still become a valid message with more bytes. A sender
/// that never completes one — an endless header line, say — would otherwise
/// have its bytes buffered without limit while `decode` keeps answering "need
/// more".
pub fn exceeds_buffer_limit(buf: &[u8]) -> bool {
    // A header line is a few digits; anything long with no line break is not
    // a bus message.
    if buf.len() > 64 && !buf[..64].contains(&b'\n') {
        return true;
    }
    buf.len() > MAX_BULK_LEN + 1024 * 1024
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::node::generate_node_id;

    #[test]
    fn meet_round_trip() {
        let id = generate_node_id();
        let msg = BusMessage::Meet {
            sender_id: id,
            sender_ip: "127.0.0.1".into(),
            sender_client_port: 6380,
            sender_cluster_port: 16380,
            sender_config_epoch: 5,
        };
        let bytes = encode(&msg);
        let (decoded, _) = decode(&bytes).unwrap().unwrap();
        match decoded {
            BusMessage::Meet {
                sender_id,
                sender_ip,
                sender_client_port,
                ..
            } => {
                assert_eq!(sender_id, id);
                assert_eq!(sender_ip, "127.0.0.1");
                assert_eq!(sender_client_port, 6380);
            }
            other => panic!("expected Meet, got {:?}", other),
        }
    }

    #[test]
    fn ping_with_empty_gossip() {
        let id = generate_node_id();
        let msg = BusMessage::Ping {
            sender_id: id,
            sender_ip: "10.0.0.1".into(),
            sender_client_port: 6380,
            sender_cluster_port: 16380,
            sender_config_epoch: 0,
            gossip: vec![],
            slot_claims: vec![],
            sender_flags: NodeFlags::MASTER,
            sender_master_id: None,
            sender_hostname: String::new(),
            sender_announced_client_port: 0,
        };
        let bytes = encode(&msg);
        let (decoded, consumed) = decode(&bytes).unwrap().unwrap();
        assert_eq!(consumed, bytes.len());
        match decoded {
            BusMessage::Ping { gossip, .. } => assert!(gossip.is_empty()),
            other => panic!("expected Ping, got {:?}", other),
        }
    }

    #[test]
    fn ping_with_gossip_entries() {
        let me = generate_node_id();
        let other = generate_node_id();
        let msg = BusMessage::Ping {
            sender_id: me,
            sender_ip: "10.0.0.1".into(),
            sender_client_port: 6380,
            sender_cluster_port: 16380,
            sender_config_epoch: 0,
            gossip: vec![GossipEntry {
                hostname: String::new(),
                announced_client_port: 0,
                id: other,
                ip: "10.0.0.2".into(),
                client_port: 6381,
                cluster_port: 16381,
                config_epoch: 0,
                flags: NodeFlags::MASTER,
                master_id: None,
            }],
            slot_claims: vec![],
            sender_flags: NodeFlags::MASTER,
            sender_master_id: None,
            sender_hostname: String::new(),
            sender_announced_client_port: 0,
        };
        let bytes = encode(&msg);
        let (decoded, _) = decode(&bytes).unwrap().unwrap();
        match decoded {
            BusMessage::Ping { gossip, .. } => {
                assert_eq!(gossip.len(), 1);
                assert_eq!(gossip[0].id, other);
                assert_eq!(gossip[0].ip, "10.0.0.2");
            }
            other => panic!("expected Ping, got {:?}", other),
        }
    }

    /// An announced client port goes in the flags string too (#110), as
    /// `cport=<port>`: the sender's own, and each gossiped node's. A node
    /// that does not know the token skips it, and reads the flags as before.
    #[test]
    fn announced_client_ports_travel_in_the_flags() {
        let me = generate_node_id();
        let other = generate_node_id();
        let msg = BusMessage::Ping {
            sender_id: me,
            sender_ip: "10.0.0.1".into(),
            sender_client_port: 6380,
            sender_cluster_port: 16380,
            sender_config_epoch: 3,
            sender_flags: NodeFlags::MASTER,
            sender_master_id: None,
            sender_hostname: "a.example".into(),
            sender_announced_client_port: 30001,
            gossip: vec![GossipEntry {
                id: other,
                ip: "10.0.0.2".into(),
                client_port: 6380,
                cluster_port: 16380,
                config_epoch: 2,
                flags: NodeFlags::REPLICA,
                master_id: Some(me),
                hostname: String::new(),
                announced_client_port: 30002,
            }],
            slot_claims: vec![],
        };
        let (decoded, _) = decode(&encode(&msg)).unwrap().unwrap();
        match decoded {
            BusMessage::Ping {
                sender_hostname,
                sender_announced_client_port,
                sender_flags,
                gossip,
                ..
            } => {
                assert_eq!(sender_hostname, "a.example");
                assert_eq!(sender_announced_client_port, 30001);
                assert_eq!(sender_flags, NodeFlags::MASTER);
                assert_eq!(gossip[0].announced_client_port, 30002);
                assert_eq!(gossip[0].flags, NodeFlags::REPLICA);
            }
            other => panic!("expected Ping, got {:?}", other),
        }
        // What an older node's parser makes of it: the flags, and nothing else.
        assert_eq!(
            parse_flags("master,host=a.example,cport=30001"),
            NodeFlags::MASTER
        );
        assert_eq!(cport_of("master,host=a.example,cport=30001"), 30001);
        assert_eq!(cport_of("master,host=a.example"), 0);
        assert_eq!(cport_of("master,cport=nonsense"), 0);
    }

    /// Hostnames go in the flags strings, where a node that does not know
    /// them finds only a token it skips; and a field added after the ones a
    /// node knows does not make it drop the message.
    #[test]
    fn hostnames_travel_where_older_nodes_ignore_them() {
        let me = generate_node_id();
        let other = generate_node_id();
        let msg = BusMessage::Pong {
            sender_id: me,
            sender_ip: "10.0.0.1".into(),
            sender_client_port: 6380,
            sender_cluster_port: 16380,
            sender_config_epoch: 3,
            gossip: vec![GossipEntry {
                id: other,
                ip: "10.0.0.2".into(),
                client_port: 6381,
                cluster_port: 16381,
                config_epoch: 0,
                flags: NodeFlags::REPLICA,
                master_id: Some(me),
                hostname: "shard-1b.db.example".into(),
                announced_client_port: 0,
            }],
            slot_claims: vec![],
            sender_flags: NodeFlags::MASTER,
            sender_master_id: None,
            sender_hostname: "shard-1a.db.example".into(),
            sender_announced_client_port: 0,
        };
        let bytes = encode(&msg);
        let (decoded, _) = decode(&bytes).unwrap().unwrap();
        let BusMessage::Pong {
            sender_hostname,
            sender_flags,
            gossip,
            ..
        } = decoded
        else {
            panic!("expected Pong");
        };
        assert_eq!(sender_hostname, "shard-1a.db.example");
        assert_eq!(sender_flags, NodeFlags::MASTER);
        assert_eq!(gossip[0].hostname, "shard-1b.db.example");
        assert_eq!(gossip[0].flags, NodeFlags::REPLICA);
        // What a node without hostnames makes of those flags.
        assert_eq!(
            parse_flags("master,host=shard-1a.db.example"),
            NodeFlags::MASTER
        );

        // One more field at the end, as a later version might send. The
        // parser hands nested arrays (gossip, claims) back raw.
        let (parts, _) = parse_resp_array(&bytes).unwrap().unwrap();
        let mut longer = format!("*{}\r\n", parts.len() + 1).into_bytes();
        for (i, part) in parts.iter().enumerate() {
            if i == 5 || i == 6 {
                longer.extend_from_slice(part);
            } else {
                longer.extend_from_slice(&resp_bulk(part));
            }
        }
        longer.extend_from_slice(&resp_bulk(b"later"));
        assert!(matches!(
            decode(&longer),
            Ok(Some((BusMessage::Pong { .. }, _)))
        ));
    }

    #[test]
    fn partial_buffer_returns_none() {
        let msg = BusMessage::Meet {
            sender_id: generate_node_id(),
            sender_ip: "127.0.0.1".into(),
            sender_client_port: 6380,
            sender_cluster_port: 16380,
            sender_config_epoch: 0,
        };
        let bytes = encode(&msg);
        // Truncated buffer: should return None (need more data), not Err.
        for cut in 0..bytes.len() {
            assert!(
                decode(&bytes[..cut]).unwrap().is_none(),
                "cut={cut} should be incomplete",
            );
        }
        // Full buffer: should decode.
        assert!(decode(&bytes).unwrap().is_some());
    }

    #[test]
    fn ping_with_multiple_gossip_entries() {
        let me = generate_node_id();
        let other1 = generate_node_id();
        let other2 = generate_node_id();
        let other3 = generate_node_id();
        let msg = BusMessage::Ping {
            sender_id: me,
            sender_ip: "10.0.0.1".into(),
            sender_client_port: 6380,
            sender_cluster_port: 16380,
            sender_config_epoch: 0,
            gossip: vec![
                GossipEntry {
                    hostname: String::new(),
                    announced_client_port: 0,
                    id: other1,
                    ip: "10.0.0.2".into(),
                    client_port: 6381,
                    cluster_port: 16381,
                    config_epoch: 1,
                    flags: NodeFlags::MASTER,
                    master_id: None,
                },
                GossipEntry {
                    hostname: String::new(),
                    announced_client_port: 0,
                    id: other2,
                    ip: "10.0.0.3".into(),
                    client_port: 6382,
                    cluster_port: 16382,
                    config_epoch: 2,
                    flags: NodeFlags::REPLICA,
                    master_id: Some(other1),
                },
                GossipEntry {
                    hostname: String::new(),
                    announced_client_port: 0,
                    id: other3,
                    ip: "10.0.0.4".into(),
                    client_port: 6383,
                    cluster_port: 16383,
                    config_epoch: 3,
                    flags: NodeFlags::MASTER | NodeFlags::PFAIL,
                    master_id: None,
                },
            ],
            slot_claims: vec![],
            sender_flags: NodeFlags::MASTER,
            sender_master_id: None,
            sender_hostname: String::new(),
            sender_announced_client_port: 0,
        };
        let bytes = encode(&msg);
        let (decoded, consumed) = decode(&bytes).unwrap().unwrap();
        assert_eq!(consumed, bytes.len(), "must consume entire buffer");
        match decoded {
            BusMessage::Ping {
                gossip, sender_id, ..
            } => {
                assert_eq!(sender_id, me);
                assert_eq!(gossip.len(), 3);
                assert_eq!(gossip[0].id, other1);
                assert_eq!(gossip[0].config_epoch, 1);
                assert!(gossip[0].flags.contains(NodeFlags::MASTER));
                assert_eq!(gossip[1].id, other2);
                assert_eq!(gossip[1].master_id, Some(other1));
                assert!(gossip[1].flags.contains(NodeFlags::REPLICA));
                assert_eq!(gossip[2].id, other3);
                assert!(gossip[2].flags.contains(NodeFlags::PFAIL));
            }
            other => panic!("expected Ping, got {:?}", other),
        }
    }

    #[test]
    fn ping_with_slot_claims_round_trip() {
        let me = generate_node_id();
        let msg = BusMessage::Ping {
            sender_id: me,
            sender_ip: "127.0.0.1".into(),
            sender_client_port: 6380,
            sender_cluster_port: 16380,
            sender_config_epoch: 7,
            gossip: vec![],
            slot_claims: vec![
                SlotClaim {
                    start: 0,
                    end: 8191,
                    epoch: 7,
                },
                SlotClaim {
                    start: 9000,
                    end: 9000,
                    epoch: 7,
                },
            ],
            sender_flags: NodeFlags::MASTER,
            sender_master_id: None,
            sender_hostname: String::new(),
            sender_announced_client_port: 0,
        };
        let bytes = encode(&msg);
        let (decoded, _) = decode(&bytes).unwrap().unwrap();
        match decoded {
            BusMessage::Ping { slot_claims, .. } => {
                assert_eq!(slot_claims.len(), 2);
                assert_eq!(slot_claims[0].start, 0);
                assert_eq!(slot_claims[0].end, 8191);
                assert_eq!(slot_claims[0].epoch, 7);
                assert_eq!(slot_claims[1].start, 9000);
                assert_eq!(slot_claims[1].end, 9000);
            }
            other => panic!("expected Ping, got {:?}", other),
        }
    }
    #[test]
    fn failover_auth_request_round_trip() {
        let me = generate_node_id();
        let master = generate_node_id();
        let msg = BusMessage::FailoverAuthRequest {
            sender_id: me,
            sender_ip: "127.0.0.1".into(),
            sender_client_port: 6390,
            sender_cluster_port: 16390,
            request_epoch: 7,
            master_id: master,
            force: false,
        };
        let bytes = encode(&msg);
        let (decoded, _) = decode(&bytes).unwrap().unwrap();
        match decoded {
            BusMessage::FailoverAuthRequest {
                sender_id,
                request_epoch,
                master_id: m,
                force,
                ..
            } => {
                assert_eq!(sender_id, me);
                assert_eq!(request_epoch, 7);
                assert_eq!(m, master);
                assert!(!force);
            }
            other => panic!("expected FailoverAuthRequest, got {:?}", other),
        }
    }

    #[test]
    fn forced_auth_request_round_trip() {
        let msg = BusMessage::FailoverAuthRequest {
            sender_id: generate_node_id(),
            sender_ip: "127.0.0.1".into(),
            sender_client_port: 6390,
            sender_cluster_port: 16390,
            request_epoch: 9,
            master_id: generate_node_id(),
            force: true,
        };
        let (decoded, _) = decode(&encode(&msg)).unwrap().unwrap();
        assert!(matches!(
            decoded,
            BusMessage::FailoverAuthRequest {
                force: true,
                request_epoch: 9,
                ..
            }
        ));
    }

    #[test]
    fn fail_round_trip() {
        let (me, dead) = (generate_node_id(), generate_node_id());
        let msg = BusMessage::Fail {
            sender_id: me,
            failed_id: dead,
        };
        let (decoded, consumed) = decode(&encode(&msg)).unwrap().unwrap();
        assert_eq!(consumed, encode(&msg).len());
        assert!(matches!(
            decoded,
            BusMessage::Fail { sender_id, failed_id } if sender_id == me && failed_id == dead
        ));
    }

    /// Channels and payloads are arbitrary bytes, CRLF included.
    #[test]
    fn publish_round_trip_is_binary_safe() {
        let msg = BusMessage::Publish {
            sender_id: generate_node_id(),
            channel: b"news\r\n.\x00x".to_vec(),
            payload: vec![0, 1, 2, b'\r', b'\n', 255],
            sharded: true,
        };
        let (decoded, _) = decode(&encode(&msg)).unwrap().unwrap();
        match decoded {
            BusMessage::Publish {
                channel,
                payload,
                sharded,
                ..
            } => {
                assert_eq!(channel, b"news\r\n.\x00x");
                assert_eq!(payload, vec![0, 1, 2, b'\r', b'\n', 255]);
                assert!(sharded);
            }
            other => panic!("expected Publish, got {:?}", other),
        }
    }

    #[test]
    fn manual_failover_handshake_round_trip() {
        let me = generate_node_id();
        let (decoded, _) = decode(&encode(&BusMessage::MfStart { sender_id: me }))
            .unwrap()
            .unwrap();
        assert!(matches!(decoded, BusMessage::MfStart { sender_id } if sender_id == me));

        let ack = BusMessage::MfAck {
            sender_id: me,
            target_offset: 123_456,
        };
        let (decoded, _) = decode(&encode(&ack)).unwrap().unwrap();
        assert!(matches!(
            decoded,
            BusMessage::MfAck {
                target_offset: 123_456,
                ..
            }
        ));
    }

    // ── Hostile input ────────────────────────────────────────────────────
    //
    // The bus port has no authentication. None of these may panic, allocate
    // by the attacker's say-so, or recurse without bound.

    #[test]
    fn an_absurd_element_count_is_rejected_not_allocated() {
        assert!(decode(b"*4000000000\r\n").is_err());
        assert!(decode(b"*18446744073709551615\r\n").is_err());
    }

    #[test]
    fn an_absurd_bulk_length_is_rejected_without_overflow() {
        assert!(decode(b"*1\r\n$18446744073709551615\r\nx").is_err());
        assert!(decode(b"*1\r\n$99999999999\r\nx").is_err());
    }

    #[test]
    fn deep_nesting_is_rejected_without_recursing() {
        let mut bomb = Vec::new();
        for _ in 0..100_000 {
            bomb.extend_from_slice(b"*1\r\n");
        }
        assert!(decode(&bomb).is_err());
    }

    #[test]
    fn garbage_is_an_error_and_never_a_panic() {
        for input in [
            &b"hello\r\n"[..],
            b"*-1\r\n",
            b"*x\r\n",
            b"*1\r\n$-5\r\n",
            b"*1\r\n:5\r\n",
            b"*2\r\n$4\r\nPING\r\n$2\r\nxx\r\n",
            b"*3\r\n$4\r\nMEET\r\n$1\r\nx\r\n$1\r\ny\r\n",
        ] {
            assert!(
                decode(input).is_err(),
                "{:?}",
                String::from_utf8_lossy(input)
            );
        }
    }

    #[test]
    fn a_header_line_that_never_ends_trips_the_buffer_limit() {
        assert!(exceeds_buffer_limit(&[b'*'; 100]));
        assert!(exceeds_buffer_limit(&[b'9'; 4096]));
        // A real message's prefix does not.
        let msg = encode(&BusMessage::MfStart {
            sender_id: generate_node_id(),
        });
        assert!(!exceeds_buffer_limit(&msg));
        assert!(!exceeds_buffer_limit(&msg[..10]));
    }

    #[test]
    fn failover_auth_ack_round_trip() {
        let me = generate_node_id();
        let msg = BusMessage::FailoverAuthAck {
            sender_id: me,
            request_epoch: 11,
        };
        let bytes = encode(&msg);
        let (decoded, _) = decode(&bytes).unwrap().unwrap();
        match decoded {
            BusMessage::FailoverAuthAck {
                sender_id,
                request_epoch,
            } => {
                assert_eq!(sender_id, me);
                assert_eq!(request_epoch, 11);
            }
            other => panic!("expected FailoverAuthAck, got {:?}", other),
        }
    }

    #[test]
    fn offset_query_and_answer_round_trip() {
        let me = generate_node_id();
        let ask = BusMessage::OffsetQuery { sender_id: me };
        let (decoded, consumed) = decode(&encode(&ask)).unwrap().unwrap();
        assert_eq!(consumed, encode(&ask).len());
        assert!(matches!(decoded, BusMessage::OffsetQuery { sender_id } if sender_id == me));

        let ack = BusMessage::OffsetAck {
            sender_id: me,
            offset: u64::MAX - 1,
        };
        let (decoded, _) = decode(&encode(&ack)).unwrap().unwrap();
        assert!(matches!(
            decoded,
            BusMessage::OffsetAck { sender_id, offset } if sender_id == me && offset == u64::MAX - 1
        ));
    }
}
