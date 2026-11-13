#!/usr/bin/env python3
"""knnload.py HOST PORT N DIM: FT.CREATE vidx (HNSW, FLOAT32, L2) on hashes v:<i>; N vectors whose float32 bytes are all
ASCII letters (so a query vector can be written on memtier's command line); prints one such query blob."""
import random, sys, redis
h, p, n, dim = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
c = redis.Redis(host=h, port=p)
try: c.execute_command("FT.DROPINDEX", "vidx", "DD")
except Exception: pass
c.execute_command("FT.CREATE", "vidx", "ON", "HASH", "PREFIX", 1, "v:", "SCHEMA", "vec", "VECTOR", "HNSW", 6,
                  "TYPE", "FLOAT32", "DIM", dim, "DISTANCE_METRIC", "L2", "tag", "TAG")
r = random.Random(1)
L = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
def blob(): return bytes(r.choice(L) for _ in range(4 * dim))
pl = c.pipeline(transaction=False)
for i in range(n):
    pl.hset(f"v:{i}", mapping={"vec": blob(), "tag": f"t{i % 10}"})
    if i % 1000 == 999: pl.execute()
pl.execute()
print(blob().decode())
