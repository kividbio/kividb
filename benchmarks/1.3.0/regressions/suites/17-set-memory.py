#!/usr/bin/env python3
"""#297 step 1: small sets in one block, small sets of integers in a sorted array.

Bytes per member is the growth of INFO used_memory over the load divided by
the members, as the collection-memory measurements of #297 took it: TOTAL
members (1,000,000) in sets of N members each (TOTAL / N keys), N = 10, 100,
1,000 and 100,000, loaded with SADD of up to 500 members a command in
pipelines of about 5,000 members. Keys are `k:<kind>:<i>` (16 bytes).

  strings  members `membr:<i>` (14 bytes, i zero-padded to 8 digits)
  ints     members `<i>`, 0 to N - 1, as an application's ids would be

At Redis's limits (set-max-listpack-entries 128, set-max-listpack-value 64,
set-max-intset-entries 512) the strings make a block at 10 and 100 members
(`listpack`) and a table at 1,000 and 100,000; the integers an array
(`intset`) at 10 and 100 and a table past 512. `encoding_<n>` records what
OBJECT ENCODING says of the first key (1 intset, 2 listpack, 3 hashtable)
and must not change.

Gates: memory per member within 5 % (or 1 B) of recorded at every size; at
10 and 100 members, where the block and the array replaced the table, it must
also keep 60 % of the gain over v1.2.2 measured in the same session. The
tables (1,000 and 100,000) are guard rows: the change must not cost them.
"""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, KEEP_IMPROVEMENT, EXACT, rel, info, scaled  # noqa: E402

S = Suite("17-set-memory", [Config("strings"), Config("ints")])
SIZES = [10, 100, 1000, 100_000]
TOTAL = 1_000_000
BATCH = 500
ENCODINGS = {"intset": 1, "listpack": 2, "hashtable": 3}


def members(kind, start, n):
    if kind == "strings":
        return ["membr:%08d" % i for i in range(start, start + n)]
    return ["%d" % i for i in range(start, start + n)]


def load(c, kind, n, total):
    keys = max(1, total // n)
    p = c.pipeline(transaction=False)
    pending = 0
    for k in range(keys):
        key = "k:%s:%010d" % (kind[:3], k)
        done = 0
        while done < n:
            m = min(BATCH, n - done)
            p.execute_command("SADD", key, *members(kind, done, m))
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
    kind = ctx.config.name
    total = scaled(TOTAL)
    for n in SIZES:
        n_here = min(n, total)
        c.flushall()
        time.sleep(0.5)
        u0 = c.info("memory")["used_memory"]
        t = time.perf_counter()
        keys = load(c, kind, n_here, total)
        secs = time.perf_counter() - t
        u1 = c.info("memory")["used_memory"]
        first = "k:%s:%010d" % (kind[:3], 0)
        assert c.scard(first) == n_here, "SCARD %d != %d" % (c.scard(first), n_here)
        per = (u1 - u0) / float(keys * n_here)
        gate = [rel(5, abs=1)]
        if n <= 100:
            gate.append(KEEP_IMPROVEMENT)
        ctx.emit("bytes_per_member_%d" % n, per, "B", "lower", gate=gate)
        enc = c.object("encoding", first)
        enc = enc.decode() if isinstance(enc, bytes) else enc
        ctx.emit("encoding_%d" % n, ENCODINGS.get(enc, 0), "code", "lower", gate=EXACT)
        ctx.emit("load_%d_s" % n, secs, "s", "lower", gate=info())
    c.flushall()


if __name__ == "__main__":
    sys.exit(S.main())
