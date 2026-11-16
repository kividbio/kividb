#!/usr/bin/env python3
"""slowdump.py <host:port> <out.json>: the node's whole SLOWLOG (rc10 soak, #310), as a list of
{"id", "ts", "us", "cmd"} (cmd: the first two words), plus a summary by command printed on stdout."""
import json
import sys

import redis

h, p = sys.argv[1].rsplit(":", 1)
r = redis.Redis(host=h, port=int(p), socket_timeout=10)
try:
    raw = r.execute_command("SLOWLOG", "GET", "100000")
except Exception as e:  # noqa: BLE001
    json.dump({"error": str(e)}, open(sys.argv[2], "w"))
    sys.exit(0)
out = []
for e in raw:
    args = [a.decode(errors="replace") if isinstance(a, bytes) else str(a) for a in e[3]]
    name = args[0].upper() if args else "?"
    if name in ("CLUSTER", "CONFIG", "CLIENT", "OBJECT") and len(args) > 1:
        name += " " + args[1].upper()
    out.append({"id": e[0], "ts": e[1], "us": e[2], "cmd": name})
json.dump(out, open(sys.argv[2], "w"))
by = {}
for x in out:
    by.setdefault(x["cmd"], []).append(x["us"])
for k, v in sorted(by.items(), key=lambda kv: -len(kv[1])):
    v.sort()
    print(f"{k}: n={len(v)} median={v[len(v) // 2]}us max={v[-1]}us")
