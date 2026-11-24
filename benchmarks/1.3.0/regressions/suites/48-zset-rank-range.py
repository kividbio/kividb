#!/usr/bin/env python3
"""#875: a large sorted set's rank and range-by-rank commands walk one path
of its index, whatever the rank, and its writes cost what v1.2.2's do.

A sorted set past `zset-max-listpack-entries` keeps its order in a B+tree of
up to 32 members a leaf whose internal nodes count the members under each
child (`src/counted_btree.rs`): a member's rank, the members from a rank and
the members between two scores are found from those counts. In v1.2.2 the
index counted the members before a rank one by one: `ZRANK` of a member in
the middle of a million cost about 3 ms.

Dataset, one server per configuration: a sorted set `z` of N members
(`membr:<8 digits>`, score i for member i, loaded by ZADDs of 1,000),
N = 1,000, 100,000 and 1,000,000.

Commands, each timed over one connection, one call at a time (reads: 3
rounds; writes: 2,000 pairs a round, 7 rounds; the median of the rounds),
after a warm-up; a member or a rank picked from the middle half, the same
ones on every engine:

  zrank, zrevrank     ZRANK / ZREVRANK z <member>
  zrange_deep         ZRANGE z <rank> <rank + 9>
  zrangebyscore_off   ZRANGEBYSCORE z -inf +inf LIMIT <rank> 10
  zcount              ZCOUNT z <score> <score + N/4>
  zrangebyscore_10    ZRANGEBYSCORE z <score> +inf LIMIT 0 10
  zadd_new, zrem      ZADD z <random score> new<i>, then ZREM z new<i>
  zincrby_move        ZINCRBY z <N> <member> (to past the highest score),
                      then ZINCRBY z -<N> <member> (back): both counted

Each emits `<case>_srv_us`, the server's own microseconds a call (INFO
commandstats, reset before each round), which is what the suite is about,
and `<case>_us`, the client's wall time a call (round trip included, shown
for the reads). `bad_replies` counts replies that are not what the dataset
makes them: in the timed rounds a rank, a range's first member, a count;
and, after each case's rounds and outside the timing, the first 100 calls
of the case once more, each reply compared whole with one the suite works
out from the dataset (every member of a range, and its scores, the range
asked again `WITHSCORES`; `ZADD` and `ZREM` of a new member 1; each
`ZINCRBY`'s new score): it must be 0.

Gates: server time 100 % or 3 us of the recorded value (suite 37 measured
the same build 1.7x apart on two D4s_v5s), and at 100,000 and 1,000,000
members the reads keep 60 % of their gain over v1.2.2 in the same session,
and the writes (`zadd_new`, `zrem`, `zincrby_move`) are at most 1.25x
v1.2.2's server time. A write takes about a microsecond, and the server
adds each call's time in whole microseconds, so a write's mean moves with
how many calls cross a microsecond: on a D4s_v5 some rounds ran at 1.0-1.5
us a call and others at 2-3.5, on v1.2.2 and on the candidate (and
master) alike, while callgrind counts the candidate's `ZINCRBY` that moves
a member within 1 % of master's and below v1.2.2's. 2,000 pairs a round
and 7 rounds (300 and 3 before) narrow that; they do not remove it, and a
write gate past 1.25x is to be read against `scripts/perf/run.sh`'s
counts before it is taken as a cost.
"""
import os
import random
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import (Suite, Config, KEEP_IMPROVEMENT, EXACT, rel, vs_v1_2_2, median, scaled)  # noqa: E402

S = Suite("48-zset-rank-range", [
    Config("n1k", threads=2),
    Config("n100k", threads=2),
    Config("n1m", threads=2),
])
SIZES = {"n1k": 1000, "n100k": 100_000, "n1m": 1_000_000}
# Calls a timed round: v1.2.2's reads at a million members cost milliseconds.
CALLS = {"n1k": 1000, "n100k": 300, "n1m": 100}
ROUNDS = 3
# Writes: pairs a round, and rounds (see the notes above).
WRITE_CALLS = 2000
WRITE_ROUNDS = 7
READS = ["zrank", "zrevrank", "zrange_deep", "zrangebyscore_off", "zcount", "zrangebyscore_10"]
WRITES = ["zadd_new", "zrem", "zincrby_move"]
SERVER = rel(100, abs=3)


def member(i):
    return "membr:%08d" % i


def load(c, n):
    for lo in range(0, n, 1000):
        hi = min(lo + 1000, n)
        c.zadd("z", {member(i): i for i in range(lo, hi)})


def server_usec(c, *names):
    stats = c.info("commandstats")
    calls = usec = 0
    for name in names:
        s = stats.get("cmdstat_" + name, {})
        calls += s.get("calls", 0)
        usec += s.get("usec", 0)
    return calls, usec


def gates_for(cfg, case, server):
    if not server:
        return [rel(50, abs=20)]
    g = [SERVER]
    if cfg != "n1k":
        g.append(KEEP_IMPROVEMENT if case in READS else vs_v1_2_2(1.25))
    return g


def commands(case, n, picks, rng):
    """The calls of one round of `case`, and the reply each should get
    (None: not checked); the command names INFO commandstats counts."""
    calls = []
    if case in ("zrank", "zrevrank"):
        for i in picks:
            want = i if case == "zrank" else n - 1 - i
            calls.append(([case.upper(), "z", member(i)], want))
        return calls, [case]
    if case == "zrange_deep":
        return [(["ZRANGE", "z", i, i + 9], member(i)) for i in picks], ["zrange"]
    if case == "zrangebyscore_off":
        return [(["ZRANGEBYSCORE", "z", "-inf", "+inf", "LIMIT", i, 10], member(i)) for i in picks], \
            ["zrangebyscore"]
    if case == "zrangebyscore_10":
        return [(["ZRANGEBYSCORE", "z", i, "+inf", "LIMIT", 0, 10], member(i)) for i in picks], \
            ["zrangebyscore"]
    if case == "zcount":
        q = n // 4
        return [(["ZCOUNT", "z", i, i + q], min(q + 1, n - i)) for i in picks], ["zcount"]
    if case in ("zadd_new", "zrem"):
        # Both commands of a pair either way; the case names the one counted.
        for k, _ in enumerate(picks):
            name = "new%08d" % k
            calls.append((["ZADD", "z", "%.4f" % (rng.random() * n), name], None))
            calls.append((["ZREM", "z", name], None))
        return calls, ["zadd" if case == "zadd_new" else "zrem"]
    if case == "zincrby_move":
        for i in picks:
            calls.append((["ZINCRBY", "z", n, member(i)], None))
            calls.append((["ZINCRBY", "z", -n, member(i)], None))
        return calls, ["zincrby"]
    raise ValueError(case)


def text(value):
    return value.decode() if isinstance(value, bytes) else value


def range_pairs(reply):
    """A range reply `WITHSCORES` as [(member, score)]: flat (RESP2) or in
    pairs (RESP3)."""
    if reply and isinstance(reply[0], (list, tuple)):
        return [(text(m), float(text(sc))) for m, sc in reply]
    return [(text(reply[k]), float(text(reply[k + 1]))) for k in range(0, len(reply), 2)]


def reference(args, n, scores):
    """The whole reply `args` gets from the dataset (score i for member i),
    and, for a range, the same range asked `WITHSCORES` and its pairs.
    `scores` holds the scores `ZINCRBY` has moved members to."""
    name = args[0]
    if name in ("ZRANK", "ZREVRANK"):
        i = int(args[2][6:])
        return i if name == "ZRANK" else n - 1 - i, None
    if name == "ZCOUNT":
        lo, hi = int(args[2]), int(args[3])
        return min(hi, n - 1) - lo + 1, None
    if name in ("ZRANGE", "ZRANGEBYSCORE"):
        if name == "ZRANGE":
            first, last = int(args[2]), int(args[3])
            scored = args + ["WITHSCORES"]
        else:
            first = int(args[5]) if args[2] == "-inf" else int(args[2]) + int(args[5])
            last = first + int(args[6]) - 1
            scored = args[:4] + ["WITHSCORES"] + args[4:]
        ranks = range(first, min(last, n - 1) + 1)
        return [member(i) for i in ranks], (scored, [(member(i), float(i)) for i in ranks])
    if name in ("ZADD", "ZREM"):
        return 1, None
    if name == "ZINCRBY":
        m = args[3]
        scores[m] = scores.get(m, float(int(m[6:]))) + float(args[2])
        return scores[m], None
    raise ValueError(name)


def validate(c, cmds, n):
    """The first 100 calls of a case once, each reply compared whole with
    `reference`: how many differ. Not timed; leaves the set as it was."""
    bad, scores = 0, {}
    for args, _ in cmds[:100]:
        args = list(args)
        want, scored = reference(args, n, scores)
        got = c.execute_command(*args)
        if isinstance(got, list):
            ok = [text(m) for m in got] == want
        elif args[0] == "ZINCRBY":
            ok = float(text(got)) == want
        else:
            ok = got == want
        if ok and scored:
            ok = range_pairs(c.execute_command(*scored[0])) == scored[1]
        bad += not ok
    return bad


def check(reply, want):
    if want is None:
        return False
    if isinstance(reply, list):
        reply = reply[0] if reply else None
    if isinstance(reply, bytes):
        reply = reply.decode()
    return reply != want


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
    rng = random.Random(875)
    # Ranks in the middle half, the same ones on every engine.
    picks = [n // 4 + rng.randrange(max(n // 2, 1)) for _ in range(calls)]
    wrng = random.Random(1875)
    wcalls = WRITE_CALLS if n == SIZES[cfg] else 100
    wpicks = [n // 4 + wrng.randrange(max(n // 2, 1)) for _ in range(wcalls)]
    bad = 0
    ctx.raw["cases"] = {}
    for case in READS + WRITES:
        write = case in WRITES
        cmds, names = commands(case, n, wpicks if write else picks, rng)
        for args, _ in cmds[:min(len(cmds), 50)]:
            c.execute_command(*args)
        rounds, server = [], []
        for _ in range(WRITE_ROUNDS if write else ROUNDS):
            c.config_resetstat()
            t = time.perf_counter()
            for args, want in cmds:
                if check(c.execute_command(*args), want):
                    bad += 1
            rounds.append((time.perf_counter() - t) / len(cmds) * 1e6)
            done, usec = server_usec(c, *names)
            server.append(usec / done if done else 0.0)
        wrong = validate(c, cmds, n)
        bad += wrong
        ctx.raw["cases"][case] = {"wall_us": rounds, "server_us": server, "validated_bad": wrong}
        if case in READS:
            ctx.emit(case + "_us", median(rounds), "us", "lower", gate=gates_for(cfg, case, False),
                     samples=rounds)
        ctx.emit(case + "_srv_us", median(server), "us", "lower", gate=gates_for(cfg, case, True),
                 samples=server)
    ctx.emit("zcard_after", c.zcard("z"), "members", "higher", gate=EXACT)
    ctx.emit("bad_replies", bad, "replies", "lower", gate=EXACT)


if __name__ == "__main__":
    sys.exit(S.main())
