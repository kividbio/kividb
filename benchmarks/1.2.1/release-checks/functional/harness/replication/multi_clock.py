#!/usr/bin/env python3
"""multi_clock.py <port> [tries]: is a hash field's deadline judged at one moment inside MULTI/EXEC?
Each try: HSET h f 1, HPEXPIREAT h <now+30 ms> FIELDS 1 f, then (busy-wait to ~1 ms before the deadline) one
MULTI with TIME, HGETALL h, 3000 x HEXISTS h f, HGETALL h, TIME, EXEC. Mismatch = the answers inside one EXEC
disagree about f. Also the same for a key: SET k v PXAT, MULTI EXISTS k x3000, EXEC."""
import sys, time, redis
port = int(sys.argv[1]); tries = int(sys.argv[2]) if len(sys.argv) > 2 else 50
r = redis.Redis(port=port)
fm = km = 0; spans = []
for i in range(tries):
    r.delete("h", "k")
    r.hset("h", "f", "1"); t = int(time.time() * 1000) + 30
    r.execute_command("HPEXPIREAT", "h", t, "FIELDS", 1, "f")
    r.set("k", "v", pxat=t)
    while time.time() * 1000 < t - 1.5:
        pass
    p = r.pipeline(transaction=True)
    p.time(); p.hgetall("h")
    for _ in range(3000):
        p.hexists("h", "f")
    p.hgetall("h"); p.time()
    for _ in range(3000):
        p.exists("k")
    res = p.execute()
    seen = [bool(x) for x in res[2:3002]]
    first, last = b"f" in res[1], b"f" in res[3002]
    (s0, u0), (s1, u1) = res[0], res[3003]
    spans.append((s1 * 1e6 + u1 - s0 * 1e6 - u0) / 1000)
    if len(set(seen + [first, last])) > 1:
        fm += 1
    ks = [bool(x) for x in res[3004:]]
    if len(set(ks)) > 1:
        km += 1
print("tries=%d field_mixed_inside_exec=%d key_mixed_inside_exec=%d exec_time_ms_median=%.2f" % (
    tries, fm, km, sorted(spans)[len(spans) // 2]))
