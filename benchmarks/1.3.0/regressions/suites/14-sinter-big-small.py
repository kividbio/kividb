#!/usr/bin/env python3
"""#759: set algebra of a big set with a small one costs the small set's size.

Dataset (the issue's): `big` holds the integers 0..N-1 and `small` 4,096
integers sampled from 0..2N-1 with random.Random(7), so about half of them
are in `big`. N is 1M and 5M in the default configuration (`big` grows from
1M to 5M between the two) and 20M in `n20m` (FULL=1).

Commands, each run once to warm up and then 5 times (median):

  sinter_big_small     SINTER big small
  sinter_small_big     SINTER small big
  sintercard           SINTERCARD 2 big small
  sintercard_limit     SINTERCARD 2 big small LIMIT 100
  sinterstore          SINTERSTORE dst big small
  sdiff_small_big      SDIFF small big

v1.2.2 copied every set out of the store before intersecting, so its time
grew with N (1.9 s at 20M against Redis's 3.7 ms in the issue); the
candidate walks the smallest set and looks its members up in the others. The
replies (counts) must equal the recorded ones exactly, on every engine.

Gates: the times are a few milliseconds or less, most of it the round trip
and the client parsing the reply, so the `rel` gate is 50 % or 2 ms,
whichever is larger (v1.2.2 took 80 ms and more at 1M, so the old cost cannot
come back unnoticed). The commands that reply with the members (SINTER,
SDIFF: about 2 ms on every engine) must also keep 60 % of the gain over
v1.2.2 and stay within 2x of Redis, both measured in the same session. The
count-only ones (SINTERCARD, SINTERSTORE) take 0.05-0.3 ms, where a ratio is
mostly the noise of the round trip, so they have only the `rel` gate.
"""
import os
import random
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import (Suite, Config, KEEP_IMPROVEMENT, EXACT, rel, vs_redis, median, scaled)  # noqa: E402

RUNS = 5
S = Suite("14-sinter-big-small", [
    Config("default"),
    Config("n20m", full_only=True),
])
SIZES = {"default": (1_000_000, 5_000_000), "n20m": (20_000_000,)}
GATES = [rel(50, abs=2), KEEP_IMPROVEMENT, vs_redis(2)]  # replies of ~2,000 members
SMALL_GATES = [rel(50, abs=2)]                         # count-only replies, sub-ms


def grow(c, frm, to):
    """SADD big frm..to-1, 1,000 members per command."""
    p = c.pipeline(transaction=False)
    for i in range(frm, to, 1000):
        p.sadd("big", *range(i, min(i + 1000, to)))
        if i % 200_000 == 0:
            p.execute()
    p.execute()


def timed(c, args):
    """(median ms, size of the reply) over RUNS runs after one warm-up."""
    ms, n = [], None
    for i in range(RUNS + 1):
        t = time.perf_counter()
        r = c.execute_command(*args)
        took = (time.perf_counter() - t) * 1000
        n = r if isinstance(r, int) else len(r)
        if i:
            ms.append(took)
    return median(ms), ms, n


CASES = [
    ("sinter_big_small", ("SINTER", "big", "{small}"), GATES),
    ("sinter_small_big", ("SINTER", "{small}", "big"), GATES),
    ("sintercard", ("SINTERCARD", "2", "big", "{small}"), SMALL_GATES),
    ("sintercard_limit", ("SINTERCARD", "2", "big", "{small}", "LIMIT", "100"), SMALL_GATES),
    ("sinterstore", ("SINTERSTORE", "dst", "big", "{small}"), SMALL_GATES),
    ("sdiff_small_big", ("SDIFF", "{small}", "big"), GATES),
]


@S.run
def run(ctx):
    c = ctx.client()
    c.flushall()
    have = 0
    ctx.raw["cases"] = {}
    for n in SIZES[ctx.config.name]:
        n = scaled(n)
        t0 = time.perf_counter()
        grow(c, have, n)
        have = n
        small = "small%d" % n
        c.sadd(small, *random.Random(7).sample(range(2 * n), 4096))
        ctx.raw["load_s_%d" % n] = round(time.perf_counter() - t0, 1)
        tag = "%dm" % (n // 1_000_000) if n >= 1_000_000 else str(n)
        ctx.emit("%s_big_card" % tag, c.scard("big"), "members", "higher", gate=EXACT)
        for name, args, gates in CASES:
            args = [a.format(small=small) for a in args]
            med, ms, size = timed(c, args)
            ctx.raw["cases"]["%s_%s" % (tag, name)] = {"ms": ms, "reply": size}
            ctx.emit("%s_%s_ms" % (tag, name), med, "ms", "lower", gate=gates, samples=ms)
            ctx.emit("%s_%s_reply" % (tag, name), size, "members", "higher", gate=EXACT)


if __name__ == "__main__":
    sys.exit(S.main())
