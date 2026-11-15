#!/usr/bin/env python3
"""#751: a full SCAN MATCH iteration over 2M keys (fixed in #769).

Dataset (the issue's): 2,000,000 keys user:N and 2,000 keys schedule:N, 16-byte
values, plus a 200,000-field hash h, set s and zset z with 200 extra
sched:N members each (the guard rows for #770).

Each case is one full iteration (cursor 0 back to 0), run 3 times after one
warm-up iteration; the metric is the median. Distinct keys returned are
counted in the warm-up iteration (the timed ones only count, with Python's
garbage collector off) and must match exactly.

HSCAN/SSCAN/ZSCAN are still 10-20x slower than Redis (#770). Their rows are
gated only against getting slower than today's recorded value, not against
Redis, so the fix for #770 shows up as an improvement and no gate fails today.
"""
import gc
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import (Suite, Config, THROUGHPUT, KEEP_IMPROVEMENT, EXACT, rel, vs_redis, info, median, scaled)  # noqa: E402

RUNS = 3
S = Suite("01-scan-match", [Config("default")])


def load(c):
    c.flushall()
    p = c.pipeline(transaction=False)
    v = b"x" * 16
    for i in range(scaled(2_000_000)):
        p.set("user:%d" % i, v)
        if i % 2000 == 1999:
            p.execute()
    for i in range(2_000):
        p.set("schedule:%d" % i, v)
    p.execute()
    for i in range(0, scaled(200_000), 2000):
        p.hset("h", mapping={"f:%d" % j: b"v" for j in range(i, i + 2000)})
        p.sadd("s", *["m:%d" % j for j in range(i, i + 2000)])
        p.zadd("z", {"m:%d" % j: j for j in range(i, i + 2000)})
        p.execute()
    for j in range(200):
        p.hset("h", "sched:%d" % j, b"v")
        p.sadd("s", "sched:%d" % j)
        p.zadd("z", {"sched:%d" % j: j})
    p.execute()


def full_scan(c, cmd, key=None, match=None, count=1000, collect=False):
    """One full iteration -> (ms, calls, items returned, distinct names or None).

    Timed runs only count; the warm-up run also collects the distinct names,
    so building a Python set of 2M names is not part of the time."""
    cursor, calls, n = 0, 0, 0
    seen = set() if collect else None
    t0 = time.perf_counter()
    while True:
        args = [cmd] + ([key] if key else []) + [cursor]
        if match:
            args += ["MATCH", match]
        args += ["COUNT", count]
        cur, items = c.execute_command(*args)
        calls += 1
        n += len(items)
        if collect:
            if isinstance(items, dict):  # redis-py parses HSCAN into a dict
                seen.update(items.keys())
            elif items and isinstance(items[0], tuple):  # and ZSCAN into (member, score)
                seen.update(x[0] for x in items)
            else:
                seen.update(items)
        cursor = int(cur)
        if cursor == 0:
            break
    return (time.perf_counter() - t0) * 1000, calls, n, (len(seen) if collect else None)


# metric, command, key, match, count, gates
CASES = [
    ("scan_match_c1000", "SCAN", None, "schedule:*", 1000, [THROUGHPUT, KEEP_IMPROVEMENT, vs_redis(1.3)]),
    ("scan_all_c1000", "SCAN", None, None, 1000, [THROUGHPUT, KEEP_IMPROVEMENT, vs_redis(1.3)]),
    ("scan_infix_c1000", "SCAN", None, "*:1*", 1000, [THROUGHPUT, KEEP_IMPROVEMENT, vs_redis(1.3)]),
    ("scan_match_c10000", "SCAN", None, "schedule:*", 10000, [THROUGHPUT, KEEP_IMPROVEMENT, vs_redis(1.3)]),
    # #770 guard rows: today's value, no Redis or improvement gate yet. 35 %: two runs of the
    # same build differed by up to 28 % (SSCAN 1,093 vs 783 ms).
    ("hscan_match_c1000", "HSCAN", "h", "sched:*", 1000, [rel(35)]),
    ("sscan_match_c1000", "SSCAN", "s", "sched:*", 1000, [rel(35)]),
    ("zscan_match_c1000", "ZSCAN", "z", "sched:*", 1000, [rel(35)]),
]


@S.run
def run(ctx):
    c = ctx.client()
    t0 = time.perf_counter()
    load(c)
    ctx.raw["load_s"] = round(time.perf_counter() - t0, 1)
    ctx.emit("dbsize", c.dbsize(), "keys", "higher", gate=EXACT)
    ctx.raw["cases"] = {}
    for name, cmd, key, match, count, gates in CASES:
        warm = full_scan(c, cmd, key, match, count, collect=True)
        gc.disable()
        try:
            res = [full_scan(c, cmd, key, match, count) for _ in range(RUNS)]
        finally:
            gc.enable()
        ms = [r[0] for r in res]
        ctx.raw["cases"][name] = {"ms": ms, "calls": [r[1] for r in res], "returned": [r[2] for r in res],
                                  "distinct": warm[3]}
        ctx.emit(name + "_ms", median(ms), "ms", "lower", gate=gates, samples=ms)
        ctx.emit(name + "_distinct", warm[3], "keys", "higher", gate=EXACT)
        ctx.emit(name + "_calls", median([r[1] for r in res]), "calls", "lower", gate=info())
    # KEYS over the same keyspace (#754/#757 made it match before copying).
    ms = []
    n = None
    for i in range(RUNS + 1):
        t = time.perf_counter()
        n = len(c.keys("schedule:*"))
        if i:
            ms.append((time.perf_counter() - t) * 1000)
    ctx.emit("keys_schedule_ms", median(ms), "ms", "lower", gate=[THROUGHPUT], samples=ms)
    ctx.emit("keys_schedule_returned", n, "keys", "higher", gate=EXACT)


if __name__ == "__main__":
    sys.exit(S.main())
