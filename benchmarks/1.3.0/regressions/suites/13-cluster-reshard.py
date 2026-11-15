#!/usr/bin/env python3
"""#294: a slot's keys live in one store shard, so GETKEYSINSLOT and
COUNTKEYSINSLOT look at one shard (#724).

#724's setup on one VM: three masters (three servers of the engine on their
own ports, `--threads 1` for KiviDB, slots split evenly; lib/nodes.py),
3,000,000 keys key:<n> = xxx written to the masters that own them (about 1M a
node), then

    redis-cli --cluster reshard <node 1> --cluster-from <node 1> --cluster-to <node 2>
        --cluster-slots 1000 --cluster-pipeline 100 --cluster-yes

with redis-cli 7.0.15 (lib/nodes.py redis_cli_7(): the GETKEYSINSLOT + MIGRATE
path; redis-cli 8.x uses CLUSTER MIGRATION IMPORT against KiviDB and never
calls these commands). About 183,000 keys move. The time of the reshard;
afterwards every key is still there and `redis-cli --cluster check` passes.

Gates: reshard time within 15 % and keeping 60 % of the gain over v1.2.2;
keys after the reshard and keys moved exact. Redis 8.10.2 is shown beside.
"""
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, EXACT, KEEP_IMPROVEMENT, rel, bound, info, scaled  # noqa: E402
from nodes import Nodes, slot, redis_cli_7  # noqa: E402

S = Suite("13-cluster-reshard", [Config("reshard1000", threads=1, self_hosted=True)])


@S.run
def run(ctx):
    cli = redis_cli_7()
    ctx.raw["redis_cli"] = subprocess.run([cli, "--version"], capture_output=True, text=True).stdout.strip()
    with Nodes(ctx) as ns:
        nodes = ns.cluster(3)
        ctx.raw["server"] = ctx.server_info(nodes[0].port)
        n = scaled(3_000_000)
        pipes = [nd.client().pipeline(transaction=False) for nd in nodes]
        bounds = [nd.slots for nd in nodes]
        per = 16384 // 3
        t = time.perf_counter()
        for i in range(n):
            k = "key:%d" % i
            j = min(slot(k) // per, 2)
            assert bounds[j][0] <= slot(k) <= bounds[j][1]
            pipes[j].set(k, "xxx")
            if len(pipes[j]) >= 1000:
                pipes[j].execute()
        for p in pipes:
            p.execute()
        ctx.emit("load_s", time.perf_counter() - t, "s", "lower", gate=info())
        sizes = [nd.client().dbsize() for nd in nodes]
        assert sum(sizes) == n, sizes
        ids = [nd.client().execute_command("CLUSTER", "MYID") for nd in nodes]
        ids = [x.decode() if isinstance(x, bytes) else x for x in ids]
        time.sleep(1)
        cmd = [cli, "--cluster", "reshard", "127.0.0.1:%d" % nodes[0].port, "--cluster-from", ids[0],
               "--cluster-to", ids[1], "--cluster-slots", "1000", "--cluster-pipeline", "100", "--cluster-yes"]
        t = time.perf_counter()
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=1500)
        secs = time.perf_counter() - t
        ctx.raw["reshard_rc"] = p.returncode
        ctx.raw["reshard_tail"] = (p.stdout[-3000:], p.stderr[-3000:])
        assert p.returncode == 0, p.stderr[-2000:]
        after = [nd.client().dbsize() for nd in nodes]
        chk = subprocess.run([cli, "--cluster", "check", "127.0.0.1:%d" % nodes[0].port],
                             capture_output=True, text=True, timeout=300)
        ctx.raw["check_tail"] = chk.stdout[-3000:]
        ctx.raw["dbsize_before_after"] = (sizes, after)
        ctx.emit("reshard_s", secs, "s", "lower", gate=[rel(15), KEEP_IMPROVEMENT])
        ctx.emit("keys_moved", after[1] - sizes[1], "keys", "higher", gate=EXACT)
        ctx.emit("keys_after", sum(after), "keys", "higher", gate=EXACT)
        ctx.emit("cluster_check_failed", int(chk.returncode != 0), "-", "lower", gate=bound(max=0))


if __name__ == "__main__":
    sys.exit(S.main())
