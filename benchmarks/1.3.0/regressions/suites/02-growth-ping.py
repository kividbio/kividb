#!/usr/bin/env python3
"""#752: PING latency while the keyspace grows (fixed in #780).

One loader connection (redis-py) SETs 8,000,000 keys key:N with 16-byte values
in pipelines of 2,000; one probe connection, in its own process, PINGs every
1 ms the whole time. Configurations:

  tokio    --io-model tokio, default threads (what Docker's seccomp gives you)
  uring1   --io-model uring --threads 1: the probe shares the loader's ring
  ex17m    FULL=1 only: 17,500,000 keys with SET ... EX 86400 on Tokio (the TTL
           sweep's lock hand-off, the second half of #780)

Redis runs once per configuration it is listed in (its I/O settings are its
defaults). One load per server: the load is the measurement, and a load of
8M keys takes ~50 s.

The probe runs in its own process, not in a thread of the loader as in the
issue's harness, so its p99 is not inflated by the loader's GIL: compare this
suite's numbers with each other, not with the issue's.
"""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import (Suite, Config, THROUGHPUT, MEMORY, WORST_PING, KEEP_IMPROVEMENT, EXACT, info, scaled)  # noqa: E402
from probe import Probe  # noqa: E402
from rss import vmrss_mib, used_memory_mib  # noqa: E402

KIVI = ("v1_2_2", "candidate")
S = Suite("02-growth-ping", [
    Config("tokio", io="tokio"),
    Config("uring1", io="uring", threads=1, engines=KIVI),
    Config("ex17m", io="tokio", full_only=True),
])
SIZES = {"tokio": (8_000_000, None), "uring1": (8_000_000, None), "ex17m": (17_500_000, 86400)}


@S.run
def run(ctx):
    n, ex = SIZES[ctx.config.name]
    n = scaled(n)
    c = ctx.client()
    c.flushall()
    v = b"x" * 16
    with Probe(ctx.port, n=1) as pr:
        time.sleep(0.5)  # the probe's own warm-up
        t0 = time.perf_counter()
        p = c.pipeline(transaction=False)
        for i in range(n):
            if ex:
                p.set("key:%d" % i, v, ex=ex)
            else:
                p.set("key:%d" % i, v)
            if i % 2000 == 1999:
                p.execute()
        p.execute()
        load_s = time.perf_counter() - t0
        time.sleep(0.2)
    ctx.raw["top_pings"] = pr.top(40)
    ctx.raw["t0"] = t0
    ctx.emit("load_s", load_s, "s", "lower", gate=THROUGHPUT)
    ctx.emit("ping_max_ms", pr.max(), "ms", "lower",
             gate=[WORST_PING] + ([KEEP_IMPROVEMENT] if ctx.config.name in ("tokio", "uring1") else []))
    ctx.emit("ping_p99_ms", pr.p99(), "ms", "lower", gate=WORST_PING)
    ctx.emit("pings_over_10ms", pr.over(10), "pings", "lower", gate=info())
    ctx.emit("pings", pr.count(), "pings", "higher", gate=info())
    ctx.emit("dbsize", c.dbsize(), "keys", "higher", gate=EXACT)
    ctx.emit("rss_loaded_mib", vmrss_mib(ctx.pid), "MiB", "lower", gate=MEMORY)
    ctx.emit("used_memory_mib", used_memory_mib(c), "MiB", "lower", gate=MEMORY)


if __name__ == "__main__":
    sys.exit(S.main())
