#!/usr/bin/env python3
"""#726: MIGRATE keeps its connection to a target (#733).

The run-all.sh server is the source; the suite starts a second server of the
same engine as the target (lib/nodes.py). 2,000 times: SET mk:<i> on the
source, then MIGRATE 127.0.0.1 <target> mk:<i> 0 5000. The target's
total_connections_received grows by one per MIGRATE on v1.2.2 and by 1 on
Redis 8.10.2 and on 1.3.0 (#733 against redis-cli and Redis 8.10.2).

Gates: the target's new connections at most 3 (as #733's test); every key
arrived (exact). The median MIGRATE time is shown.
"""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, EXACT, bound, info, median  # noqa: E402
from nodes import Nodes  # noqa: E402

S = Suite("11-migrate-conns", [Config("migrate")])


@S.run
def run(ctx):
    src = ctx.client()
    src.flushall()
    with Nodes(ctx) as ns:
        tgt_node = ns.start()
        tgt = tgt_node.client()
        before = tgt.info("stats")["total_connections_received"]
        n = 2000
        xs = []
        for i in range(n):
            k = "mk:%d" % i
            src.set(k, "v%d" % i)
            t = time.perf_counter()
            r = src.execute_command("MIGRATE", "127.0.0.1", tgt_node.port, k, 0, 5000)
            xs.append((time.perf_counter() - t) * 1000)
            assert r in (b"OK", "OK", True), r
        after = tgt.info("stats")["total_connections_received"]
        ctx.raw["target_server"] = ctx.server_info(tgt_node.port)
        ctx.emit("target_connections", after - before, "connections", "lower", gate=bound(max=3))
        ctx.emit("keys_on_target", tgt.dbsize(), "keys", "higher", gate=EXACT)
        ctx.emit("keys_left_on_source", src.dbsize(), "keys", "lower", gate=EXACT)
        ctx.emit("migrate_ms", median(xs), "ms", "lower", gate=info(), samples=[median(xs), max(xs)])


if __name__ == "__main__":
    sys.exit(S.main())
