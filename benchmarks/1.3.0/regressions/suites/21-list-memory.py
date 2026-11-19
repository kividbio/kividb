#!/usr/bin/env python3
"""Collection storage step 3: lists in chunks of packed elements.

Bytes per element is the growth of INFO used_memory over the load divided by
the elements, as steps 1 and 2 (`17-set-memory`, `19-hash-memory`) take it:
TOTAL elements (1,000,000) in lists of N elements each (TOTAL / N keys), N =
10, 100, 1,000 and 100,000, loaded with RPUSH of up to 500 elements a command
in pipelines of about 5,000 elements. Keys are `k:lst:<i>` (16 bytes);
elements `valuevaluevaluevaluevalue<i>` (32 bytes, i zero-padded to 7
digits), the size the redesign was measured with.

  packed  the lists as loaded: every element 32 bytes, so every list is kept
          in chunks (`listpack` while it is one chunk of up to 64 elements,
          `quicklist` beyond)
  plain   the same, with a first element of 1,025 bytes pushed and popped:
          the list keeps its elements apart from then on, as 1.2 kept every
          list (a guard: the change must not cost these)

`encoding_<n>` records what OBJECT ENCODING says of the first key (2
listpack, 5 quicklist) and must not change.

Gates: memory per element within 5 % (or 1 B) of recorded at every size; for
`packed` it must also keep 60 % of the gain over v1.2.2 measured in the same
session.
"""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, KEEP_IMPROVEMENT, EXACT, rel, info, scaled  # noqa: E402

S = Suite("21-list-memory", [Config("packed", threads=2), Config("plain", threads=2)])
SIZES = [10, 100, 1000, 100_000]
TOTAL = 1_000_000
BATCH = 500
ENCODINGS = {"listpack": 2, "quicklist": 5}
LONG = "L" * 1025


def elements(start, n):
    return ["valuevaluevaluevaluevalue%07d" % i for i in range(start, start + n)]


def load(c, n, total, plain):
    keys = max(1, total // n)
    p = c.pipeline(transaction=False)
    pending = 0
    for k in range(keys):
        key = "k:lst:%010d" % k
        if plain:
            p.execute_command("RPUSH", key, LONG)
        done = 0
        while done < n:
            m = min(BATCH, n - done)
            p.execute_command("RPUSH", key, *elements(done, m))
            done += m
            pending += m
            if pending >= 5000:
                p.execute()
                pending = 0
        if plain:
            p.execute_command("LPOP", key)
    p.execute()
    return keys


@S.run
def run(ctx):
    c = ctx.client()
    plain = ctx.config.name == "plain"
    total = scaled(TOTAL)
    for n in SIZES:
        n_here = min(n, total)
        c.flushall()
        time.sleep(0.5)
        u0 = c.info("memory")["used_memory"]
        t = time.perf_counter()
        keys = load(c, n_here, total, plain)
        secs = time.perf_counter() - t
        u1 = c.info("memory")["used_memory"]
        first = "k:lst:%010d" % 0
        assert c.llen(first) == n_here, "LLEN %d != %d" % (c.llen(first), n_here)
        per = (u1 - u0) / float(keys * n_here)
        gate = [rel(5, abs=1)]
        if not plain:
            gate.append(KEEP_IMPROVEMENT)
        ctx.emit("bytes_per_element_%d" % n, per, "B", "lower", gate=gate)
        enc = c.object("encoding", first)
        enc = enc.decode() if isinstance(enc, bytes) else enc
        ctx.emit("encoding_%d" % n, ENCODINGS.get(enc, 0), "code", "lower", gate=EXACT)
        ctx.emit("load_%d_s" % n, secs, "s", "lower", gate=info())
    c.flushall()


if __name__ == "__main__":
    sys.exit(S.main())
