#!/usr/bin/env python3
"""#862: ZRANDMEMBER costs the count asked for, not the size of the set.

Until #862 a large sorted set (past `zset-max-listpack-entries`) found its
random members by walking its member table up to the last position picked:
about half the table for one member, nearly all of it for ten, so that
`ZRANDMEMBER z 10` over a million members took about 1.8 ms where Redis takes
0.08 ms (suite 16 has those rows). The table is now asked for a bucket by its
number: a random bucket, tried again while it is empty, for each member
picked (a distinct sample of up to a third of the set picks again a bucket it
already took; more than that is one walk of the table, no longer than the
answer). Small sets (one block) were never slow and are not changed.

Dataset, one server per configuration: a sorted set `z` of N members (score
i for member `m<i>`), N = 1,000, 100,000 and 1,000,000.

Commands, each timed over one connection (3 rounds, median of the rounds),
after one warm-up round:

  none                ZRANDMEMBER z
  c1, c10, c1000      ZRANDMEMBER z 1 / 10 / 1000
  m10, m1000          ZRANDMEMBER z -10 / -1000
  *_ws                the same with WITHSCORES (not for `none`)

Two numbers a command: `<case>_us`, the client's wall time per call (the
round trip and redis-py's parsing of the reply included: about 50 us, and
about 1 ms to parse a thousand members), and `<case>_srv_us`, the server's
own time per call (INFO commandstats `usec` over `calls`, reset before each
case), which is the number this suite is about. At 1,000 members a count of
1000 is the whole set (from the highest score down).

`bad_replies` counts replies of the wrong length, or with a member twice for
a positive count: it must be 0 on every engine.

Gates: wall time 50 % or 20 us, server time 100 % or 3 us, whichever is
larger (see SERVER below). At 100,000 and 1,000,000 members every wall-time row must keep 60 %
of the gain over v1.2.2 measured in the same session (not the server time:
for one member it is a microsecond or less, too coarse to divide by), and
the server time of a count of 10 or 1000, either sign, must stay within 3x
of Redis's (both are O(count) now).
"""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import (Suite, Config, KEEP_IMPROVEMENT, EXACT, rel, vs_redis, median, scaled)  # noqa: E402

S = Suite("37-zrandmember-by-count", [
    Config("n1k", threads=2),
    Config("n100k", threads=2),
    Config("n1m", threads=2),
])
SIZES = {"n1k": 1000, "n100k": 100_000, "n1m": 1_000_000}
# Calls per timed round: the dearer the call, the fewer.
CALLS = {"n1k": 1000, "n100k": 300, "n1m": 150}
ROUNDS = 3

# name, count (None: no count)
COUNTS = [("none", None), ("c1", 1), ("c10", 10), ("c1000", 1000), ("m10", -10), ("m1000", -1000)]
CASES = []
for _name, _count in COUNTS:
    CASES.append((_name, _count, False))
    if _count is not None:
        CASES.append((_name + "_ws", _count, True))


def load(c, n):
    for lo in range(1, n + 1, 1000):
        hi = min(lo + 1000, n + 1)
        c.zadd("z", {"m%d" % i: i for i in range(lo, hi)})


def args_of(count, ws):
    args = ["ZRANDMEMBER", "z"]
    if count is not None:
        args.append(str(count))
    if ws:
        args.append("WITHSCORES")
    return args


def server_usec(c):
    stats = c.info("commandstats").get("cmdstat_zrandmember", {})
    return stats.get("calls", 0), stats.get("usec", 0)


# The server time of the same build moves by up to 1.7x between two VMs of
# the same size and CPU (`n1k` `m1000_srv_us`: 66 us recorded, 110-115 us on
# another D4s_v5, the code before and after #969's review alike), so it is
# held to twice its recorded value; the gains it guards are larger (2-10x
# master at 100,000 members and more, hundreds of times at the counts of 1
# and 10).
SERVER = rel(100, abs=3)


def gates_for(cfg, name, server):
    if cfg == "n1k":
        return [SERVER if server else rel(50, abs=20)]
    if not server:
        # Where v1.2.2 and the walk paid for the size: that cost must stay
        # gone. Judged on the wall time: a server time of a microsecond or
        # less is too coarse a denominator for a ratio (the first run's
        # `none` was 0.06 us, its rerun 0.89 us).
        return [rel(50, abs=20), KEEP_IMPROVEMENT]
    g = [SERVER]
    if name.startswith(("c10", "m10")):
        # Ten members or more: several microseconds on both engines, enough
        # to hold to Redis's (both are O(count) now).
        g.append(vs_redis(3))
    return g


def bad_reply(r, n, count, ws):
    if count is None:
        return not isinstance(r, bytes)
    want = min(count, n) if count >= 0 else -count
    per = 2 if ws else 1
    if len(r) != want * per:
        return True
    members = r[::per]
    return count >= 0 and len(set(members)) != len(members)


@S.run
def run(ctx):
    c = ctx.client()
    c.flushall()
    cfg = ctx.config.name
    n = scaled(SIZES[cfg], step=100)
    t0 = time.perf_counter()
    load(c, n)
    ctx.raw["load_s"] = round(time.perf_counter() - t0, 1)
    encoding = c.object("encoding", "z")
    ctx.raw["encoding"] = encoding.decode() if isinstance(encoding, bytes) else encoding
    ctx.emit("zcard", c.zcard("z"), "members", "higher", gate=EXACT)
    calls = CALLS[cfg] if n == SIZES[cfg] else 100
    bad = 0
    ctx.raw["cases"] = {}
    for name, count, ws in CASES:
        args = args_of(count, ws)
        for _ in range(min(calls, 100)):
            c.execute_command(*args)
        rounds, server = [], []
        for _ in range(ROUNDS):
            c.config_resetstat()
            t = time.perf_counter()
            for _ in range(calls):
                c.execute_command(*args)
            rounds.append((time.perf_counter() - t) / calls * 1e6)
            done, usec = server_usec(c)
            server.append(usec / done if done else 0.0)
        for _ in range(20):
            if bad_reply(c.execute_command(*args), n, count, ws):
                bad += 1
        ctx.raw["cases"][name] = {"wall_us": rounds, "server_us": server}
        ctx.emit(name + "_us", median(rounds), "us", "lower", gate=gates_for(cfg, name, False), samples=rounds)
        ctx.emit(name + "_srv_us", median(server), "us", "lower", gate=gates_for(cfg, name, True),
                 samples=server)
    ctx.emit("bad_replies", bad, "replies", "lower", gate=EXACT)


if __name__ == "__main__":
    sys.exit(S.main())
