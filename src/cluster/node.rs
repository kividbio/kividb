// src/cluster/node.rs
//
// ClusterNode and related types.
//
// Design choices worth understanding:
//
//   NodeId is [u8; 40] (not String):
//     Redis node IDs are exactly 40 hex chars. Fixing the size makes NodeId
//     Copy, eliminates heap allocations on every gossip message, and lets us
//     use it as a HashMap key without boxing. The trade-off is a tiny bit
//     of conversion code at the parse/format boundary.
//
//   config_epoch is u64:
//     This is the version number for slot ownership claims. In a failover,
//     the replica with the highest epoch wins. Redis uses uint64_t — don't
//     shrink it to u32 even though current values rarely exceed 2^16.
//
//   Timestamps are u64 ms since UNIX epoch:
//     Matches what Redis reports in CLUSTER NODES. Avoids std::time::Instant
//     which can't be compared across process restarts.

use bitflags::bitflags;

/// 40 hex characters, stored as raw bytes (e.g. b"a1b2..." — 40 ASCII chars).
pub type NodeId = [u8; 40];

/// All-zero node ID, used as a placeholder/sentinel.
pub const NODE_ID_ZERO: NodeId = [b'0'; 40];

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct NodeFlags: u32 {
        const MYSELF    = 1 << 0;
        const MASTER    = 1 << 1;
        const REPLICA   = 1 << 2;
        const PFAIL     = 1 << 3;  // possibly failing — local view only
        const FAIL      = 1 << 4;  // confirmed failed — quorum agreed
        const HANDSHAKE = 1 << 5;  // mid-MEET, ID not yet known
        const NOADDR    = 1 << 6;  // address unknown
    }
}

#[derive(Debug, Clone)]
pub struct ClusterNode {
    pub id: NodeId,
    pub ip: String,
    pub client_port: u16,
    pub cluster_port: u16,
    pub flags: NodeFlags,
    pub master_id: Option<NodeId>,
    pub config_epoch: u64,
    pub ping_sent_ms: u64,
    pub pong_received_ms: u64,
    /// When this node was marked FAIL (Unix ms), 0 if it is not. A failed
    /// master that owns slots is only un-failed once this is old enough that
    /// its replicas have had their chance to take over.
    pub fail_since_ms: u64,
    /// The node's `cluster-announce-hostname`; empty if it announces none.
    pub hostname: String,
    /// The node's `cluster-announce-client-port`: the port clients are told
    /// to reach it on, where that is not `client_port` (a NodePort, a load
    /// balancer that maps ports); 0 if it announces none. Nodes reach each
    /// other at `client_port` regardless (#110).
    pub announced_client_port: u16,
}

impl ClusterNode {
    /// Construct a fresh node entry for a peer we just learned about.
    pub fn new(id: NodeId, ip: String, client_port: u16, cluster_port: u16) -> Self {
        Self {
            id,
            ip,
            client_port,
            cluster_port,
            flags: NodeFlags::empty(),
            master_id: None,
            config_epoch: 0,
            ping_sent_ms: 0,
            pong_received_ms: 0,
            fail_since_ms: 0,
            hostname: String::new(),
            announced_client_port: 0,
        }
    }

    /// The port clients are told, in redirects and in `CLUSTER SLOTS`,
    /// `SHARDS` and `NODES`.
    pub fn client_facing_port(&self) -> u16 {
        if self.announced_client_port != 0 {
            self.announced_client_port
        } else {
            self.client_port
        }
    }

    /// Mark this node failed, stamping when.
    pub fn mark_failed(&mut self, now_ms: u64) {
        if !self.flags.contains(NodeFlags::FAIL) {
            self.fail_since_ms = now_ms;
        }
        self.flags |= NodeFlags::FAIL;
        self.flags.remove(NodeFlags::PFAIL);
    }

    pub fn clear_failed(&mut self) {
        self.flags.remove(NodeFlags::FAIL);
        self.fail_since_ms = 0;
    }

    pub fn is_master(&self) -> bool {
        self.flags.contains(NodeFlags::MASTER)
    }

    pub fn is_replica(&self) -> bool {
        self.flags.contains(NodeFlags::REPLICA)
    }
}

// ── NodeId helpers ────────────────────────────────────────────────────────────
//
// Two operations we need everywhere: parse a hex string into a NodeId, and
// format a NodeId back to a string for CLUSTER NODES output and nodes.conf.

/// Parse a 40-char hex string into a NodeId. Returns None on length mismatch
/// or non-hex bytes. We don't decode the hex — we store the ASCII as-is,
/// matching Redis's wire format.
pub fn parse_node_id(s: &[u8]) -> Option<NodeId> {
    if s.len() != 40 {
        return None;
    }
    for &b in s {
        if !b.is_ascii_hexdigit() {
            return None;
        }
    }
    let mut id = [0u8; 40];
    id.copy_from_slice(s);
    // Normalize to lowercase to match Redis behavior on display.
    for b in id.iter_mut() {
        b.make_ascii_lowercase();
    }
    Some(id)
}

/// Format a NodeId as &str without allocating. NodeId is already ASCII hex.
pub fn node_id_str(id: &NodeId) -> &str {
    // Safe: parse_node_id and generate_node_id both guarantee ASCII.
    std::str::from_utf8(id).expect("NodeId is ASCII")
}

/// Generate a fresh random 40-hex-char node ID for this server instance.
/// Called once at startup if no nodes.conf is found.
pub fn generate_node_id() -> NodeId {
    // 160 random bits. Two nodes with the same ID are one node as far as the
    // cluster is concerned, so this is not left to the clock and the PID —
    // which in a container is 1 for everybody.
    let bytes: [u8; 20] = rand::random();
    let mut out = [0u8; 40];
    let hex = b"0123456789abcdef";
    for (chunk, byte) in out.chunks_mut(2).zip(bytes) {
        chunk[0] = hex[(byte >> 4) as usize];
        chunk[1] = hex[(byte & 0x0F) as usize];
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_id() {
        let s = b"a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4";
        let id = parse_node_id(s).expect("valid id");
        assert_eq!(&id[..], s);
    }

    #[test]
    fn parse_uppercase_normalizes() {
        let s = b"A1B2C3D4E5F60718293A4B5C6D7E8F90A1B2C3D4";
        let id = parse_node_id(s).expect("valid id");
        // Should be lowercased
        assert_eq!(&id[..], b"a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4");
    }

    #[test]
    fn parse_invalid_length() {
        assert!(parse_node_id(b"too-short").is_none());
        assert!(parse_node_id(b"a".repeat(41).as_slice()).is_none());
    }

    #[test]
    fn parse_invalid_hex() {
        // 40 chars but contains a 'g'
        let s = b"g1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4";
        assert!(parse_node_id(s).is_none());
    }

    #[test]
    fn generate_returns_valid_id() {
        let id = generate_node_id();
        // Round-trip through parse_node_id
        assert!(parse_node_id(&id).is_some());
        assert_eq!(id.len(), 40);
    }

    #[test]
    fn generated_ids_differ() {
        // Two consecutive calls should produce different IDs.
        // (Theoretically possible to collide, but with ~10ns between calls
        // and 2^160 space, never happens in practice.)
        let a = generate_node_id();
        let b = generate_node_id();
        assert_ne!(a, b);
    }
}
