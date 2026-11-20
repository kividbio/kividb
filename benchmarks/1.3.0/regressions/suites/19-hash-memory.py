#!/usr/bin/env python3
"""Collection storage step 2: small hashes in one block.

Bytes per field is the growth of INFO used_memory over the load divided by
the fields, as step 1 (`17-set-memory`) takes it: TOTAL fields (1,000,000) in
hashes of N fields each (TOTAL / N keys), N = 10, 100, 1,000 and 100,000,
loaded with HSET of up to 250 fields a command in pipelines of about 5,000
fields. Keys are `k:has:<i>` (16 bytes); fields `field:<i>` (14 bytes, i
zero-padded to 8 digits) with 32-byte values, the sizes the redesign was
measured with.

  plain  the hashes as loaded
  ttl    the same, then one field of each hash given a deadline far off
         (HPEXPIREAT): a block with field TTLs (`listpackex`)

At Redis's limits (hash-max-listpack-entries 512, hash-max-listpack-value 64)
the fields make a block at 10 and 100 fields (`listpack`, `listpackex` with a
TTL) and a table at 1,000 and 100,000. `encoding_<n>` records what OBJECT
ENCODING says of the first key (2 listpack, 3 hashtable, 4 listpackex) and
must not change.

Gates: memory per field within 5 % (or 1 B) of recorded at every size; at 10
and 100 fields, where the block replaced the table, it must also keep 60 % of
the gain over v1.2.2 measured in the same session. The tables (1,000 and
100,000) are guard rows: the change must not cost them.

#764 step 3 (hashes on `CollectionTable`) re-recorded the table rows and
added one hash each of 3,000,000 fields, and of 3,211,264 and 3,211,265:
just before and just after the table doubles from 2^19 to 2^20 groups (it
grows past 0.875 of 7 x 2^19 slots), its fullest and its emptiest point, the
bounds of what a large hash costs. A hash this big is one key of that many
fields. Before `used_memory` is read the server is left until it stops
falling (a resize that the background, or Redis's rehashing, is still
finishing; at most 60 s).
"""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, KEEP_IMPROVEMENT, EXACT, rel, info, scaled  # noqa: E402

S = Suite("19-hash-memory", [Config("plain", threads=2), Config("ttl", threads=2)])
SIZES = [10, 100, 1000, 100_000, 3_000_000, 3_211_264, 3_211_265]
TOTAL = 1_000_000
BATCH = 250
ENCODINGS = {"listpack": 2, "hashtable": 3, "listpackex": 4}
FAR = 4_000_000_000_000  # a deadline in 2096


def fields(start, n):
    out = []
    for i in range(start, start + n):
        out += ["field:%08d" % i, "value:%026d" % i]
    return out


def load(c, n, total, ttl):
    keys = max(1, total // n)
    p = c.pipeline(transaction=False)
    pending = 0
    for k in range(keys):
        key = "k:has:%010d" % k
        done = 0
        while done < n:
            m = min(BATCH, n - done)
            p.execute_command("HSET", key, *fields(done, m))
            done += m
            pending += m
            if pending >= 5000:
                p.execute()
                pending = 0
        if ttl:
            p.execute_command("HPEXPIREAT", key, FAR, "FIELDS", 1, "field:00000000")
    p.execute()
    return keys


def settled(c, limit=60.0):
    """used_memory once it has stopped falling: a resize still being finished
    in the background holds two arrays until it is done, and gives the old
    one back all at once when it is, so this waits until no lower value has
    been seen for 2 s."""
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


@S.run
def run(ctx):
    c = ctx.client()
    ttl = ctx.config.name == "ttl"
    for n in SIZES:
        # Past TOTAL, one hash of n fields.
        total = scaled(max(TOTAL, n))
        n_here = min(n, total)
        c.flushall()
        time.sleep(0.5)
        u0 = c.info("memory")["used_memory"]
        t = time.perf_counter()
        keys = load(c, n_here, total, ttl)
        secs = time.perf_counter() - t
        u1 = settled(c)
        first = "k:has:%010d" % 0
        assert c.hlen(first) == n_here, "HLEN %d != %d" % (c.hlen(first), n_here)
        per = (u1 - u0) / float(keys * n_here)
        gate = [rel(5, abs=1)]
        if n <= 100:
            gate.append(KEEP_IMPROVEMENT)
        ctx.emit("bytes_per_field_%d" % n, per, "B", "lower", gate=gate)
        enc = c.object("encoding", first)
        enc = enc.decode() if isinstance(enc, bytes) else enc
        ctx.emit("encoding_%d" % n, ENCODINGS.get(enc, 0), "code", "lower", gate=EXACT)
        ctx.emit("load_%d_s" % n, secs, "s", "lower", gate=info())
    c.flushall()


if __name__ == "__main__":
    sys.exit(S.main())
