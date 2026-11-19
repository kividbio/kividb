#!/usr/bin/env python3
"""#839: ZRANDMEMBER and HRANDFIELD cost the sample, not the collection.

v1.2.2 copied every member of the sorted set (or built a vector of every
field of the hash and one of every index) before it picked, so that
`ZRANDMEMBER z` over a million members took 2.4 ms where Redis takes 0.09
ms. The candidate reads a hash's fields by position (O(count)) and a sorted
set's members in one walk of its member table up to the last position picked:
no copy, and no more of the table than that, but still a walk, because the
table has no index by position (see docs/PERFORMANCE.md).

Dataset, one server per configuration: a sorted set `z` of N members (score i
for member `m<i>`) and a hash `h` of N fields (`f<i>` = `v<i>`), N = 100, 10,000
and 1,000,000.

Commands, each timed as the mean of CALLS calls over one connection (3 rounds,
median of the rounds), after one warm-up round:

  zrandmember_none / hrandfield_none   ZRANDMEMBER z / HRANDFIELD h
  zrandmember_1, _10                   ZRANDMEMBER z 1 / 10
  zrandmember_m1, _m10                 ZRANDMEMBER z -1 / -10
  hrandfield_1, _10, _m1, _m10         likewise for the hash
  zrandmember_10_ws, hrandfield_10_wv  ZRANDMEMBER z 10 WITHSCORES / HRANDFIELD h 10 WITHVALUES

`bad_replies` counts replies of the wrong length or, for a positive count,
with a member twice: it must be 0 on every engine.

Gates: the times are a few tens to hundreds of microseconds, most of it the
client's round trip (about 50 us here, from Python), so `rel` is 50 % or 20 us,
whichever is larger. At 1,000,000, where v1.2.2 paid for the size, every row
but `HRANDFIELD h` (which never copied) must also keep 60 % of the gain over
v1.2.2 measured in the same session, and the hash rows must stay within 2x of
Redis (a hash is O(count) on both). The sorted set's rows are not held to
Redis: the candidate still walks the member table (about half of it on
average, nearly all of it for a count of 10) where Redis indexes by rank, so
they take about a millisecond at 1,000,000 against Redis's 50 us. The gap is
shown, not gated; it goes away with a storage that can be indexed by
position.
"""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import (Suite, Config, KEEP_IMPROVEMENT, EXACT, rel, vs_redis, median, scaled)  # noqa: E402

S = Suite("16-random-sample", [
    Config("n100", threads=2),
    Config("n10k", threads=2),
    Config("n1m", threads=2),
])
SIZES = {"n100": 100, "n10k": 10_000, "n1m": 1_000_000}
# Calls per timed round: the dearer the call, the fewer.
CALLS = {"n100": 3000, "n10k": 1000, "n1m": 150}
ROUNDS = 3

# name, args, expected reply length, distinct (positive count)
CASES = [
    ("zrandmember_none", ("ZRANDMEMBER", "z"), None, False),
    ("zrandmember_1", ("ZRANDMEMBER", "z", "1"), 1, True),
    ("zrandmember_10", ("ZRANDMEMBER", "z", "10"), 10, True),
    ("zrandmember_m1", ("ZRANDMEMBER", "z", "-1"), 1, False),
    ("zrandmember_m10", ("ZRANDMEMBER", "z", "-10"), 10, False),
    ("zrandmember_10_ws", ("ZRANDMEMBER", "z", "10", "WITHSCORES"), 20, False),
    ("hrandfield_none", ("HRANDFIELD", "h"), None, False),
    ("hrandfield_1", ("HRANDFIELD", "h", "1"), 1, True),
    ("hrandfield_10", ("HRANDFIELD", "h", "10"), 10, True),
    ("hrandfield_m1", ("HRANDFIELD", "h", "-1"), 1, False),
    ("hrandfield_m10", ("HRANDFIELD", "h", "-10"), 10, False),
    ("hrandfield_10_wv", ("HRANDFIELD", "h", "10", "WITHVALUES"), 20, False),
]


def load(c, n):
    for lo in range(1, n + 1, 1000):
        hi = min(lo + 1000, n + 1)
        p = c.pipeline(transaction=False)
        p.zadd("z", {"m%d" % i: i for i in range(lo, hi)})
        p.hset("h", mapping={"f%d" % i: "v%d" % i for i in range(lo, hi)})
        p.execute()


def gates_for(name, cfg):
    g = [rel(50, abs=20)]
    if cfg == "n1m":
        # The size where v1.2.2 paid for it: the old cost must not come back.
        # `HRANDFIELD h` without a count never copied, so it is a guard only.
        if name != "hrandfield_none":
            g.append(KEEP_IMPROVEMENT)
        # A hash is O(count) on both engines.
        if name.startswith("hrandfield"):
            g.append(vs_redis(2))
    return g


@S.run
def run(ctx):
    c = ctx.client()
    c.flushall()
    cfg = ctx.config.name
    n = scaled(SIZES[cfg], step=100)
    t0 = time.perf_counter()
    load(c, n)
    ctx.raw["load_s"] = round(time.perf_counter() - t0, 1)
    ctx.emit("zcard", c.zcard("z"), "members", "higher", gate=EXACT)
    ctx.emit("hlen", c.hlen("h"), "fields", "higher", gate=EXACT)
    calls = CALLS[cfg] if n == SIZES[cfg] else 200
    bad = 0
    ctx.raw["cases"] = {}
    for name, args, want, distinct in CASES:
        for _ in range(min(calls, 100)):
            c.execute_command(*args)
        rounds = []
        for _ in range(ROUNDS):
            t = time.perf_counter()
            for _ in range(calls):
                c.execute_command(*args)
            rounds.append((time.perf_counter() - t) / calls * 1e6)
        # Correctness, on a handful of replies.
        for _ in range(20):
            r = c.execute_command(*args)
            if want is None:
                bad += 0 if isinstance(r, bytes) else 1
                continue
            if len(r) != want:
                bad += 1
                continue
            if distinct and len(set(r)) != len(r):
                bad += 1
        ctx.raw["cases"][name] = rounds
        ctx.emit(name + "_us", median(rounds), "us", "lower", gate=gates_for(name, cfg), samples=rounds)
    ctx.emit("bad_replies", bad, "replies", "lower", gate=EXACT)


if __name__ == "__main__":
    sys.exit(S.main())
