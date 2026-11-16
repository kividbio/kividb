#!/usr/bin/env python3
"""wlmem.py host:port: how much of used_memory is the write log's keys (wl:*), which only ever grow, and how much is
the soak mix's bounded key spaces. SCANs every key, counts each class, and samples MEMORY USAGE of 1 key in 20."""
import json
import random
import sys

import redis

h, p = sys.argv[1].rsplit(":", 1)
c = redis.Redis(host=h, port=int(p), socket_timeout=60)
cnt = {"wl": 0, "mix": 0}
samp = {"wl": [], "mix": []}
cur = 0
rnd = random.Random(1)
while True:
    cur, keys = c.scan(cur, count=2000)
    pick = []
    for k in keys:
        cls = "wl" if k.startswith(b"wl:") else "mix"
        cnt[cls] += 1
        if rnd.random() < 0.05:
            pick.append((cls, k))
    if pick:
        pp = c.pipeline(transaction=False)
        for _, k in pick:
            pp.execute_command("MEMORY", "USAGE", k)
        for (cls, _), v in zip(pick, pp.execute(raise_on_error=False)):
            if isinstance(v, int):
                samp[cls].append(v)
    if cur == 0:
        break
info = c.info("memory")
out = {"used_memory": info["used_memory"], "used_memory_rss": info["used_memory_rss"], "keys": cnt}
for cls in cnt:
    s = samp[cls]
    out[cls + "_memory_usage_mean_bytes"] = round(sum(s) / len(s), 1) if s else None
    out[cls + "_estimated_bytes"] = round(sum(s) / len(s) * cnt[cls]) if s else 0
    out[cls + "_sampled"] = len(s)
print(json.dumps(out, indent=1))
