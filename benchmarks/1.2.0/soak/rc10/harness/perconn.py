#!/usr/bin/env python3
"""perconn.py <run dir> <probe a|b|c> <round>: how the stall of one reshard round spreads over a shard probe's 8
connections (#310). For each connection: operations scheduled inside the round, the share over 10 ms and over 100 ms,
and the worst. With io_uring and `threads 4`, a worker that blocks stalls the connections it holds (about a quarter);
a stall shared by every connection points at something else (a lock, the CPU)."""
import collections
import json
import re
import sys

d, probe, rnd = sys.argv[1], sys.argv[2], sys.argv[3]
a = b = None
for line in open(d + "/events"):
    m = re.match(r"(\S+) reshard-start round %s " % rnd, line)
    if m:
        a = float(m.group(1))
    m = re.match(r"(\S+) reshard-end round %s " % rnd, line)
    if m:
        b = float(m.group(1))
c = collections.defaultdict(lambda: [0, 0, 0, 0])
for line in open(d + "/probe-%s.raw" % probe):
    f = line.split()
    if len(f) != 4:
        continue
    s, lat, conn = int(f[0]) / 1e6, int(f[1]), int(f[2])
    if a <= s < b:
        x = c[conn]
        x[0] += 1
        x[1] += lat > 10_000
        x[2] += lat > 100_000
        x[3] = max(x[3], lat)
out = {"round": int(rnd), "probe": probe, "seconds": round(b - a, 1), "conns": []}
print("probe %s, round %s, %.1f s" % (probe, rnd, b - a))
for k in sorted(c):
    n, s10, s100, mx = c[k]
    out["conns"].append({"conn": k, "ops": n, "over_10ms_pct": round(100 * s10 / n, 1), "over_100ms_pct": round(100 * s100 / n, 1), "worst_ms": round(mx / 1000)})
    print("conn %d: ops %d, over 10 ms %.1f %%, over 100 ms %.1f %%, worst %.0f ms" % (k, n, 100 * s10 / n, 100 * s100 / n, mx / 1000))
json.dump(out, open("%s/perconn-%s-%s.json" % (d, probe, rnd), "w"), indent=1)
