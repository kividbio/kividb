#!/usr/bin/env python3
"""fexp_race.py MASTER REPLICA [n]: a field TTL extended on the master right at its deadline (#415 class).

For each i: HSET rx:i f v keep k; HPEXPIRE rx:i 100 FIELDS 1 f; sleep 100 ms +- a few ms; HPEXPIRE rx:i 600000 FIELDS 1 f.
When the master still has f (reply 1) it propagates HPEXPIREAT; the replica, which expires fields by its own clock, may
already have dropped f. Then master and replica differ for the next 10 minutes. Also the mirror case with HGETEX PX.
"""
import random
import sys
import time

import redis

m = redis.Redis(host=sys.argv[1]); r = redis.Redis(host=sys.argv[2])
n = int(sys.argv[3]) if len(sys.argv) > 3 else 300
res = {"HPEXPIRE": [0, 0], "HGETEX": [0, 0]}
for cmd in res:
    keys = []
    for i in range(n):
        k = f"rx:{cmd}:{i}"
        m.delete(k)
        m.hset(k, mapping={"f": "v", "keep": "k"})
        m.execute_command("HPEXPIRE", k, 100, "FIELDS", 1, "f")
        time.sleep(0.100 + random.uniform(-0.004, 0.004))
        if cmd == "HPEXPIRE":
            rep = m.execute_command("HPEXPIRE", k, 600000, "FIELDS", 1, "f")
            alive = rep and rep[0] == 1
        else:
            rep = m.execute_command("HGETEX", k, "PX", 600000, "FIELDS", 1, "f")
            alive = rep and rep[0] is not None
        if alive:
            res[cmd][0] += 1
            keys.append(k)
    off = m.info("replication")["master_repl_offset"]
    for _ in range(100):
        if r.info("replication")["slave_repl_offset"] >= off:
            break
        time.sleep(0.05)
    time.sleep(0.5)
    for k in keys:
        if m.hget(k, "f") != r.hget(k, "f"):
            res[cmd][1] += 1
            if res[cmd][1] <= 3:
                print(f"  {k}: master f={m.hget(k, 'f')} pttl={m.execute_command('HPTTL', k, 'FIELDS', 1, 'f')} | "
                      f"replica f={r.hget(k, 'f')} pttl={r.execute_command('HPTTL', k, 'FIELDS', 1, 'f')}")
for cmd, (ext, bad) in res.items():
    print(f"{cmd}: {n} tries, master extended the field in {ext}, replica lacks it afterwards in {bad}")
