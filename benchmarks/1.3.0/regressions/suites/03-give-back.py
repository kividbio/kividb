#!/usr/bin/env python3
"""#753: RSS given back to the system after a large delete (fixed in #763).

Three scenarios, each on a fresh server (default build: glibc malloc, where
the give-back thread runs; Redis uses its jemalloc). A candidate that links
mimalloc (a lua or full build) fails this suite on purpose: run-all.sh detects
it and the suite refuses to measure it.

  hashdel  HSET of 3,000,000 fields f0.. = v (1,000 per HSET), then one DEL
  hdel     HSET of 1,000,000 fields, then HDEL of all of them, 100 per HDEL
  churn    300,000 strings of 32-4,096 random bytes and 50 hashes of 20,000
           fields; then about half the strings deleted, a quarter rewritten
           with other sizes, and 25 hashes deleted (random.Random(7))

After the last command the harness sends nothing (the give-back waits for an
idle server) and reads VmRSS 1.5, 5 and 15 s later, plus a timeline every
0.5 s for the raw log. INFO memory is read only after the 15 s mark.
"""
import os
import random
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import (Suite, Config, MEMORY, GIVE_BACK, KEEP_IMPROVEMENT, info, scaled)  # noqa: E402
from rss import vmrss_mib, used_memory_mib, sample_at, smaps_mib  # noqa: E402

S = Suite("03-give-back", [Config("hashdel"), Config("hdel"), Config("churn")])
MARKS = (1.5, 5, 15)


def hashdel(c):
    p = c.pipeline(transaction=False)
    for i in range(0, scaled(3_000_000), 1000):
        p.hset("bighash", mapping={"f%d" % j: "v" for j in range(i, i + 1000)})
        if i % 100_000 == 0:
            p.execute()
    p.execute()
    yield "loaded"
    c.delete("bighash")


def hdel(c):
    p = c.pipeline(transaction=False)
    n = scaled(1_000_000)
    for i in range(0, n, 1000):
        p.hset("h", mapping={"f%d" % j: "v" for j in range(i, i + 1000)})
        if i % 100_000 == 0:
            p.execute()
    p.execute()
    yield "loaded"
    for i in range(0, n, 100):
        p.hdel("h", *["f%d" % j for j in range(i, i + 100)])
        if i % 100_000 == 0:
            p.execute()
    p.execute()


def churn(c):
    rnd = random.Random(7)
    p = c.pipeline(transaction=False)
    n = scaled(300_000)
    for i in range(n):
        p.set("k%d" % i, b"x" * rnd.randint(32, 4096))
        if i % 1000 == 0:
            p.execute()
    p.execute()
    for i in range(50):
        p.hset("hh%d" % i, mapping={"f%d" % j: "v" * rnd.randint(1, 64) for j in range(20_000)})
        p.execute()
    yield "loaded"
    for i in range(n):
        if rnd.random() < 0.5:
            p.delete("k%d" % i)
        elif rnd.random() < 0.5:
            p.set("k%d" % i, b"y" * rnd.randint(32, 4096))
        if i % 1000 == 0:
            p.execute()
    for i in range(0, 50, 2):
        p.delete("hh%d" % i)
    p.execute()


SCEN = {"hashdel": hashdel, "hdel": hdel, "churn": churn}


@S.run
def run(ctx):
    if ctx.engine == "candidate" and os.environ.get("REGRESS_CANDIDATE_ALLOCATOR") == "mimalloc":
        raise RuntimeError("03-give-back measures glibc malloc's give-back, and this candidate links "
                           "mimalloc (a lua or full build): use a default build, or --only the other suites")
    c = ctx.client()
    c.flushall()
    pid = ctx.pid
    ctx.emit("rss_start_mib", vmrss_mib(pid), "MiB", "lower", gate=info())
    steps = SCEN[ctx.config.name](c)
    next(steps)  # load
    time.sleep(1.0)
    ctx.emit("rss_loaded_mib", vmrss_mib(pid), "MiB", "lower", gate=MEMORY)
    ctx.emit("used_loaded_mib", used_memory_mib(c), "MiB", "lower", gate=MEMORY)
    t = time.perf_counter()
    for _ in steps:  # the delete
        pass
    t0 = time.perf_counter()
    ctx.emit("delete_ms", (t0 - t) * 1000, "ms", "lower", gate=info())
    at, timeline = sample_at(pid, t0, MARKS)
    ctx.raw["rss_timeline"] = timeline
    ctx.raw["smaps_after"] = smaps_mib(pid)
    improving = ctx.config.name in ("hashdel", "hdel")
    for m in MARKS:
        name = "rss_after_%ss_mib" % str(m).replace(".", "_")
        gates = [GIVE_BACK] + ([KEEP_IMPROVEMENT] if improving and m == 1.5 else [])
        ctx.emit(name, at[m], "MiB", "lower", gate=gates)
    ctx.emit("used_after_mib", used_memory_mib(c), "MiB", "lower", gate=info())
    if ctx.kind == "kividb":
        mem = c.info("memory")
        ctx.raw["allocator_trims"] = mem.get("allocator_trims")
        ctx.raw["allocator_trims_skipped_busy"] = mem.get("allocator_trims_skipped_busy")


if __name__ == "__main__":
    sys.exit(S.main())
