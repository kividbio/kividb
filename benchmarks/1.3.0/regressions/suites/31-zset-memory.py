#!/usr/bin/env python3
"""Collection storage step 4: small sorted sets in one block, and large ones
on the collection table (#875's storage half, #825).

Bytes per member is the growth of INFO used_memory over the load divided by
the members, as the earlier steps take it (`17-set-memory`, `19-hash-memory`,
`21-list-memory`): TOTAL members (1,000,000) in sorted sets of N members each
(TOTAL / N keys), N = 10, 100, 128, 129 and 1,000, then one set each of
100,000 and 3,000,000 members, loaded with ZADD of up to 250 pairs a command
in pipelines of about 5,000 members. Keys are `k:zst:<i>` (16 bytes);
members `membr:<i>` (14 bytes, i zero-padded to 8 digits), the size the
redesign was measured with. used_memory is read once it has stopped falling
(a large set's table finishes a resize in the background).

  int    scores 0..N-1: whole numbers, which a block keeps in one to three
         bytes
  float  scores i + 0.5: kept as the eight bytes of the float

At Redis's limits (zset-max-listpack-entries 128, zset-max-listpack-value 64)
the members make a block up to 128, and the large form (a table of entries
and an index of their order) from 129.
`encoding_<n>` records what OBJECT ENCODING says of the first key (2
listpack, 5 skiplist) and must not change.

Gates: memory per member within 5 % (or 1 B) of recorded at every size, and
60 % of the gain over v1.2.2 measured in the same session kept at every size
(the block at 10 to 128 members, the table from 129).
"""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, KEEP_IMPROVEMENT, EXACT, rel, info, scaled  # noqa: E402

S = Suite("31-zset-memory", [Config("int", threads=4), Config("float", threads=4)])
SIZES = [10, 100, 128, 129, 1000, 100_000, 3_000_000]
TOTAL = 1_000_000
BATCH = 250
ENCODINGS = {"listpack": 2, "skiplist": 5}


def pairs(start, n, half):
    out = []
    for i in range(start, start + n):
        out += [("%d.5" % i) if half else ("%d" % i), "membr:%08d" % i]
    return out


def settled(c, limit=60.0):
    """used_memory once it has stopped falling for 2 s."""
    low = c.info("memory")["used_memory"]
    end = time.perf_counter() + limit
    since = time.perf_counter()
    while time.perf_counter() < end and time.perf_counter() - since < 2.0:
        time.sleep(0.1)
        now = c.info("memory")["used_memory"]
        if now < low - 4096:
            since = time.perf_counter()
        low = min(low, now)
    return low


def load(c, n, total, half):
    keys = max(1, total // n)
    p = c.pipeline(transaction=False)
    pending = 0
    for k in range(keys):
        key = "k:zst:%010d" % k
        done = 0
        while done < n:
            m = min(BATCH, n - done)
            p.execute_command("ZADD", key, *pairs(done, m, half))
            done += m
            pending += m
            if pending >= 5000:
                p.execute()
                pending = 0
    p.execute()
    return keys


@S.run
def run(ctx):
    c = ctx.client()
    half = ctx.config.name == "float"
    for n in SIZES:
        # One set of each size past TOTAL.
        total = scaled(max(TOTAL, n))
        n_here = min(n, total)
        c.flushall()
        time.sleep(0.5)
        u0 = settled(c)
        t = time.perf_counter()
        keys = load(c, n_here, total, half)
        secs = time.perf_counter() - t
        u1 = settled(c)
        first = "k:zst:%010d" % 0
        assert c.zcard(first) == n_here, "ZCARD %d != %d" % (c.zcard(first), n_here)
        per = (u1 - u0) / float(keys * n_here)
        gate = [rel(5, abs=1), KEEP_IMPROVEMENT]
        ctx.emit("bytes_per_member_%d" % n, per, "B", "lower", gate=gate)
        enc = c.object("encoding", first)
        enc = enc.decode() if isinstance(enc, bytes) else enc
        ctx.emit("encoding_%d" % n, ENCODINGS.get(enc, 0), "code", "lower", gate=EXACT)
        ctx.emit("load_%d_s" % n, secs, "s", "lower", gate=info())
    c.flushall()


if __name__ == "__main__":
    sys.exit(S.main())
