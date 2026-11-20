#!/usr/bin/env python3
"""#754: a long command must not hold up other connections (fixed on Tokio in #757,
on io_uring in #762).

Dataset: DEBUG POPULATE 2000000 user (keys user:0..user:1999999), and for the
DEL cases a 3,000,000-field hash loaded again before each run. Four probe
connections, each in its own process, PING every 1 ms from 0.6 s before the
command until 1 s after it. Commands, 3 runs each:

  keys       KEYS user:99*                      (11,111 matches)
  scan       SCAN 0 MATCH user:99* COUNT 2000000
  del        DEL big                            (3M-field hash)
  del_multi  DEL big user:1                     (uring1 only: takes the command gate alone)

Per run: the command's own time, and the worst PING in flight while it ran,
over all four probes ("worst"). Metrics are medians over the runs, except
worst_max, the largest worst of the 3 runs, which is what the bound checks:
no PING should wait for a long command at all.

On Tokio a long command runs off the I/O driver (#757). On io_uring (one ring
per thread) the work is sent to a thread of its own and the connection waits
for it, while its ring serves its other connections (#762). `uring` runs two
rings, so which probes share the command's ring is chance (the worst over all
probes is gated, and "least_worst", the probe that waited least, is kept);
`uring1` runs one ring, so every probe shares it: v1.2.2 stalls all four for
the whole command. Redis is single-threaded and stalls every probe; it is
measured in the tokio configuration only.
"""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import (Suite, Config, THROUGHPUT, EXACT, rel, bound, info, jitter, vs_v1_2_2, median, scaled)  # noqa: E402
from probe import Probe  # noqa: E402

RUNS = 3
PROBES = 4
S = Suite("04-long-commands", [
    Config("tokio", io="tokio", threads=2),
    Config("uring", io="uring", engines=("v1_2_2", "candidate"), threads=2),
    # One ring: every probe is on the long command's thread (#762).
    Config("uring1", io="uring", engines=("v1_2_2", "candidate"), threads=1),
])
CMDS = {
    "keys": ("KEYS", "user:99*"),
    "scan": ("SCAN", "0", "MATCH", "user:99*", "COUNT", "2000000"),
    "del": ("DEL", "big"),
    "del_multi": ("DEL", "big", "user:1"),
}
# The free of #762 beyond `DEL` of one key: on the one-ring configuration.
SAME_THREAD_ONLY = ("del_multi",)


def load_keys(c):
    c.flushall()
    try:
        c.execute_command("DEBUG", "POPULATE", str(scaled(2_000_000)), "user")
    except Exception:  # an engine without DEBUG POPULATE: same keys by SET
        p = c.pipeline(transaction=False)
        for i in range(scaled(2_000_000)):
            p.set("user:%d" % i, "value:%d" % i)
            if i % 2000 == 1999:
                p.execute()
        p.execute()


def load_hash(c):
    c.delete("big")
    p = c.pipeline(transaction=False)
    for i in range(0, scaled(3_000_000), 1000):
        p.hset("big", mapping={"f%d" % j: "v%d" % j for j in range(i, i + 1000)})
        if i % 32_000 == 0:
            p.execute()
    p.execute()


def describe(case, rep):
    if case == "keys":
        return len(rep)
    if case == "scan":
        return len(rep[1])
    return int(rep)


def one(ctx, c, case):
    if case in ("del", "del_multi"):
        load_hash(c)
    if case == "del_multi":
        c.set("user:1", "value:1")
    c.ping()
    with Probe(ctx.port, n=PROBES) as pr:
        time.sleep(0.6)
        t0 = time.perf_counter()
        rep = c.execute_command(*CMDS[case])
        t1 = time.perf_counter()
        time.sleep(1.0)
    each = pr.worst_each(t0, t1)
    return {"cmd_ms": (t1 - t0) * 1000, "worst_ms": max(each), "each_ms": [round(x, 2) for x in each],
            "stalled": sum(1 for x in each if x > (t1 - t0) * 500), "n": describe(case, rep),
            "probe_p99_ms": pr.p99()}


@S.run
def run(ctx):
    c = ctx.client()
    load_keys(c)
    ctx.emit("dbsize", c.dbsize(), "keys", "higher", gate=EXACT)
    name = ctx.config.name
    ctx.raw["runs"] = {}
    for case in CMDS:
        if case in SAME_THREAD_ONLY and name != "uring1":
            continue
        runs = [one(ctx, c, case) for _ in range(RUNS)]
        ctx.raw["runs"][case] = runs
        cmd = [r["cmd_ms"] for r in runs]
        worst = [r["worst_ms"] for r in runs]
        # KEYS was 15 % slower than v1.2.2 before #789 and is about a third faster since:
        # it must stay at least 1/0.85 times faster than v1.2.2 measured in the same session.
        gate = [THROUGHPUT, vs_v1_2_2(0.85)] if case == "keys" else THROUGHPUT
        ctx.emit(case + "_cmd_ms", median(cmd), "ms", "lower", gate=gate, samples=cmd)
        # Off the driver (Tokio), off the ring (io_uring): no PING may wait more than 20 ms
        # (the command takes 130-300 ms).
        ctx.emit(case + "_worst_ping_ms", median(worst), "ms", "lower", gate=jitter(bound(max=20)), samples=worst)
        ctx.emit(case + "_worst_ping_max_ms", max(worst), "ms", "lower", gate=jitter(bound(max=20)))
        if name == "uring":
            # Which of the 4 probes share the command's ring is chance (0 to 4): the probe
            # that waited least, kept as it was gated before #762.
            least = [min(r["each_ms"]) for r in runs]
            ctx.emit(case + "_least_worst_ping_ms", median(least), "ms", "lower", gate=jitter(rel(50, abs=10)),
                     samples=least)
        ctx.emit(case + "_probes_stalled", median([r["stalled"] for r in runs]), "probes", "lower", gate=info())
        gate = EXACT if case in ("keys", "del", "del_multi") else info()
        ctx.emit(case + "_reply", runs[-1]["n"], "items", "higher", gate=gate)


if __name__ == "__main__":
    sys.exit(S.main())
