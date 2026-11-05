#!/usr/bin/env python3
"""Proposed follow-up on top of 69c2bef2 (+ cargo fmt): a restarted node takes a healthy relay's
address for a peer it has not heard from directly since it started, without first waiting for
the 2x node-timeout start grace to expire and suspecting the peer. CRLF-preserving, exact
count==1 replacements. usage: proposal_patch.py TREE [DRY=1 env]"""
import os
import sys

TREE = sys.argv[1]
DRY = os.environ.get("DRY") == "1"


def patch(rel, pairs):
    p = os.path.join(TREE, rel)
    raw = open(p, newline="").read()
    crlf = "\r\n" in raw
    text = raw.replace("\r\n", "\n")
    for old, new in pairs:
        n = text.count(old)
        if n != 1:
            raise SystemExit(f"{rel}: anchor found {n} times:\n{old}")
        text = text.replace(old, new)
    if crlf:
        text = text.replace("\n", "\r\n")
    if not DRY:
        open(p, "w", newline="").write(text)
    print(f"{rel}: ok ({len(pairs)} edits){' [dry]' if DRY else ''}")


patch("src/cluster/mod.rs", [
    (
        "    pub failure_reports: RwLock<AHashMap<NodeId, AHashMap<NodeId, u64>>>,\n",
        "    pub failure_reports: RwLock<AHashMap<NodeId, AHashMap<NodeId, u64>>>,\n"
        "    /// Peers whose address is still the one nodes.conf gave us: none of\n"
        "    /// them has reached us since this process started. A node that moved\n"
        "    /// while we were down is only reachable at an address a peer relays.\n"
        "    pub(crate) unconfirmed_addresses: RwLock<ahash::AHashSet<NodeId>>,\n",
    ),
    (
        "            failure_reports: RwLock::new(AHashMap::new()),\n",
        "            failure_reports: RwLock::new(AHashMap::new()),\n"
        "            unconfirmed_addresses: RwLock::new(ahash::AHashSet::new()),\n",
    ),
    (
        "        let now = now_ms() + self.node_timeout_ms;\n"
        "        let me = self.myself();\n"
        "        for node in self.nodes.write().values_mut() {\n"
        "            if node.id == me {\n"
        "                continue;\n"
        "            }\n"
        "            node.ping_sent_ms = 0;\n"
        "            node.pong_received_ms = now;\n"
        "            node.flags.remove(NodeFlags::PFAIL);\n"
        "        }\n",
        "        let now = now_ms() + self.node_timeout_ms;\n"
        "        let me = self.myself();\n"
        "        let mut unconfirmed = ahash::AHashSet::new();\n"
        "        for node in self.nodes.write().values_mut() {\n"
        "            if node.id == me {\n"
        "                continue;\n"
        "            }\n"
        "            node.ping_sent_ms = 0;\n"
        "            node.pong_received_ms = now;\n"
        "            node.flags.remove(NodeFlags::PFAIL);\n"
        "            unconfirmed.insert(node.id);\n"
        "        }\n"
        "        *self.unconfirmed_addresses.write() = unconfirmed;\n",
    ),
])

patch("src/cluster/bus/listener.rs", [
    (
        "        let suspect = NodeFlags::PFAIL | NodeFlags::FAIL;\n"
        "        let moved = !direct_contact\n"
        "            && !ip.is_empty()\n"
        "            && entry.flags.intersects(suspect)\n",
        "        // So does one we have not heard from since we started, whose\n"
        "        // address is still nodes.conf's: the start grace otherwise keeps\n"
        "        // it unsuspected for two node timeouts, and the moment it ends\n"
        "        // our suspicion and a peer's report from before the node came\n"
        "        // back make a quorum, and a healthy master is failed over.\n"
        "        let suspect = NodeFlags::PFAIL | NodeFlags::FAIL;\n"
        "        let unconfirmed = !direct_contact && state.unconfirmed_addresses.read().contains(&id);\n"
        "        let moved = !direct_contact\n"
        "            && !ip.is_empty()\n"
        "            && (entry.flags.intersects(suspect) || unconfirmed)\n",
    ),
    (
        "        if direct_contact {\n"
        "            entry.pong_received_ms = now_ms();\n"
        "            entry.flags.remove(NodeFlags::PFAIL);\n",
        "        if direct_contact {\n"
        "            entry.pong_received_ms = now_ms();\n"
        "            entry.flags.remove(NodeFlags::PFAIL);\n"
        "            state.unconfirmed_addresses.write().remove(&id);\n",
    ),
    (
        "    /// A node we reach keeps the address it gives itself, whatever a third\n",
        "    /// After a restart, a peer we have not heard from yet has the address\n"
        "    /// nodes.conf gave it, which may be stale: a healthy relay's address\n"
        "    /// replaces it at once, before the start grace would let us suspect it.\n"
        "    #[test]\n"
        "    fn after_a_restart_a_peer_not_yet_heard_from_takes_a_healthy_relays_address() {\n"
        "        let cs = ClusterState::new_enabled(None, 15000);\n"
        "        let (a, b) = (generate_node_id(), generate_node_id());\n"
        "        process_message(&cs, meet(a), peer());\n"
        "        let known = gossip_of(b, \"10.0.0.8\", 7002, NodeFlags::MASTER);\n"
        "        process_message(&cs, ping(a, NodeFlags::MASTER, vec![known]), peer());\n"
        "        cs.start_the_clock_on_peers();\n"
        "\n"
        "        let moved = gossip_of(b, \"10.0.0.88\", 7002, NodeFlags::MASTER);\n"
        "        process_message(&cs, ping(a, NodeFlags::MASTER, vec![moved]), peer());\n"
        "        let nodes = cs.nodes.read();\n"
        "        let node = nodes.get(&b).unwrap();\n"
        "        assert_eq!(node.ip, \"10.0.0.88\");\n"
        "        assert!(!node.flags.intersects(NodeFlags::PFAIL | NodeFlags::FAIL));\n"
        "    }\n"
        "\n"
        "    /// A node we reach keeps the address it gives itself, whatever a third\n",
    ),
])
