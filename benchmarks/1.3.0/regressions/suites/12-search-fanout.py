#!/usr/bin/env python3
"""#723: a cluster search keeps its connections to the other masters (#725).

A 3-master cluster on this VM (three servers of the engine, own ports, slots
split evenly; lib/nodes.py). FT.CREATE docs (TEXT t, prefix doc:) on node 1,
30 documents written to the masters that own them. Then 2,000 FT.SEARCH docs
hello NOCONTENT LIMIT 0 0 sent to node 1 by 8 client processes (250 each),
which node 1 fans out to the other two. As #725's test: how much each other
master's total_connections_received grows (v1.2.2: one per search, 2,001),
and how many `kividb-cluster-search` clients it keeps afterwards.

KiviDB only: Redis has no cluster-wide search.

Gates: the most connections either other master received at most 20 (#725:
16 kept + 4); every search counted 30 documents; searches a second within
25 %, shown beside v1.2.2's.
"""
import multiprocessing as mp
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, EXACT, rel, bound, info  # noqa: E402
from nodes import Nodes, slot  # noqa: E402

S = Suite("12-search-fanout", [Config("cluster3", engines=("v1_2_2", "candidate"), self_hosted=True)])
CLIENTS, EACH = 8, 250


def searcher(port, q):
    import redis
    c = redis.Redis(port=port, socket_timeout=60)
    bad = 0
    for _ in range(EACH):
        r = c.execute_command("FT.SEARCH", "docs", "hello", "NOCONTENT", "LIMIT", "0", "0")
        bad += int(r[0] != 30)
    q.put(bad)


def owner(nodes, key):
    s = slot(key)
    return next(n for n in nodes if n.slots[0] <= s <= n.slots[1])


@S.run
def run(ctx):
    with Nodes(ctx) as ns:
        nodes = ns.cluster(3)
        ctx.raw["server"] = ctx.server_info(nodes[0].port)
        c0 = nodes[0].client()
        c0.execute_command("FT.CREATE", "docs", "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA", "t", "TEXT")
        on = [0, 0, 0]
        for i in range(30):
            k = "doc:%d" % i
            n = owner(nodes, k)
            n.client().hset(k, "t", "hello")
            on[nodes.index(n)] += 1
        ctx.raw["docs_per_master"] = on
        r = None
        for _ in range(100):
            r = c0.execute_command("FT.SEARCH", "docs", "hello", "NOCONTENT")
            if r[0] == 30:
                break
            time.sleep(0.1)
        assert r[0] == 30, r
        peers = [n.client() for n in nodes[1:]]
        before = [p.info("stats")["total_connections_received"] for p in peers]
        mpc = mp.get_context("fork")
        q = mpc.Queue()
        procs = [mpc.Process(target=searcher, args=(nodes[0].port, q)) for _ in range(CLIENTS)]
        t = time.perf_counter()
        for p in procs:
            p.start()
        bad = sum(q.get(timeout=600) for _ in procs)
        secs = time.perf_counter() - t
        for p in procs:
            p.join(10)
        grew = [p.info("stats")["total_connections_received"] - b for p, b in zip(peers, before)]
        kept = []
        for p in peers:
            kept.append(sum(1 for cl in p.client_list() if cl.get("name") == "kividb-cluster-search"))
        ctx.raw["connections_per_peer"] = grew
        ctx.raw["kept_per_peer"] = kept
        ctx.emit("peer_connections_max", max(grew), "connections", "lower", gate=bound(max=20),
                 samples=grew)
        ctx.emit("peer_kept_clients_max", max(kept), "clients", "lower", gate=info(), samples=kept)
        ctx.emit("searches_wrong_count", bad, "searches", "lower", gate=EXACT)
        ctx.emit("searches_per_s", CLIENTS * EACH / secs, "searches/s", "higher", gate=rel(25))


if __name__ == "__main__":
    sys.exit(S.main())
