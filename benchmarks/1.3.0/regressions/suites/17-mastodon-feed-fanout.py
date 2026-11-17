#!/usr/bin/env python3
"""#849: Mastodon's home-feed fan-out (FeedManager), the mix published in
mastodon/mastodon#41702: 1.7x Redis at 200 connections, 2.0x at 500, timeline
read p50 halved.

Dataset: N = 20,000 home feeds `feed:home:<n>`, sorted sets of 800 members
each: an 18-digit status id, score below 1, so every status the run adds is
newer and the trim takes the loaded ones first.

Load, memtier_benchmark (4 threads, 200 and 500 connections, keys 1..N
uniformly), one command per FeedManager call (MIX below):

  ZADD feed:home:<n> <id> <id>                       a status pushed to a feed
  ZREMRANGEBYRANK feed:home:<n> 0 -801               the trim to 800 after it
  ZREVRANGE feed:home:<n> 40 40 WITHSCORES           the reblog fall-off check
  ZSCORE feed:home:<n> <id>                          is this status in the feed
  EXISTS feed:home:<n>                               is the feed there at all
  ZREVRANGEBYSCORE feed:home:<n> +inf -inf LIMIT 0 20 WITHSCORES   a timeline page
  ZRANGEBYSCORE feed:home:<n> (<id> +inf LIMIT 0 20 WITHSCORES     newer than min_id

Each connection count: one warm-up run, then RUNS runs of SECONDS; the metric
is the median. Rows per connection count: ops/s for the whole mix, the
timeline read's (ZREVRANGEBYSCORE) p50, and p99 of the mix and of the read,
shown next to them (several are worse than Redis at p99 today). Memory:
used_memory after the load, and its bytes per member. `feeds` (DBSIZE after
the runs) must stay N: the trim never empties a feed.

Gates, against Redis in the same session, ~15 % under what was published:
ops/s at least 1.5x Redis at 200 connections and 1.7x at 500, the read p50 at
most 0.6x Redis's; and THROUGHPUT / MEMORY against the recorded candidate. The
p99 rows are shown, not gated (`jitter`: client and server share the VM).

Needs memtier_benchmark 2.x: `$MEMTIER`, else on PATH.
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import (Suite, Config, THROUGHPUT, MEMORY, EXACT, info, jitter, vs_redis, median, scaled)  # noqa: E402
from rss import used_memory_mib  # noqa: E402

S = Suite("17-mastodon-feed-fanout", [Config("default")])
FEEDS = 20_000
MEMBERS = 800
THREADS = 4
SECONDS = 20
WARMUP_SECONDS = 10
RUNS = 3
# Connections -> the floor over Redis for the mix's ops/s (published 1.7x / 2.0x).
CONNS = {200: 1.5, 500: 1.7}
READ = "Zrevrangebyscores"  # memtier's name for the timeline read's stats

# command, ratio. `__key__` is a feed number, drawn anew for each placeholder.
MIX = [
    ("ZADD feed:home:__key__ __key__ __key__", 3),
    ("ZREMRANGEBYRANK feed:home:__key__ 0 -801", 3),
    ("ZREVRANGE feed:home:__key__ 40 40 WITHSCORES", 1),
    ("ZSCORE feed:home:__key__ __key__", 1),
    ("EXISTS feed:home:__key__", 1),
    ("ZREVRANGEBYSCORE feed:home:__key__ +inf -inf LIMIT 0 20 WITHSCORES", 2),
    ("ZRANGEBYSCORE feed:home:__key__ (__key__ +inf LIMIT 0 20 WITHSCORES", 1),
]


def memtier_bin():
    for p in (os.environ.get("MEMTIER", ""), shutil.which("memtier_benchmark") or ""):
        if p and os.access(p, os.X_OK):
            return p
    raise RuntimeError("memtier_benchmark not found ($MEMTIER or PATH)")


def load(c, feeds):
    base = 113_000_000_000_000_000
    for lo in range(1, feeds + 1, 50):
        p = c.pipeline(transaction=False)
        for n in range(lo, min(lo + 50, feeds + 1)):
            p.zadd("feed:home:%d" % n, {str(base + n * MEMBERS + j): j / 1000.0 for j in range(MEMBERS)})
        p.execute()


def memtier(port, conns, seconds, feeds):
    """One run -> (ops/s, mix p99, read p50, read p99), latencies in ms."""
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, "out.json")
        cmd = [memtier_bin(), "-s", "127.0.0.1", "-p", str(port), "--protocol=redis",
               "-t", str(THREADS), "-c", str(conns // THREADS), "--test-time", str(seconds),
               "--key-prefix=", "--key-minimum=1", "--key-maximum=%d" % feeds,
               "--distinct-client-seed", "--hide-histogram", "--json-out-file", out]
        for command, ratio in MIX:
            # --command-key-pattern must follow its --command.
            cmd += ["--command=" + command, "--command-ratio=%d" % ratio, "--command-key-pattern=R"]
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True,
                       timeout=seconds + 120)
        with open(out) as f:
            stats = json.load(f)["ALL STATS"]
    total, read = stats["Totals"], stats[READ]
    return (total["Ops/sec"], total["Percentile Latencies"]["p99.00"],
            read["Percentile Latencies"]["p50.00"], read["Percentile Latencies"]["p99.00"])


@S.run
def run(ctx):
    c = ctx.client()
    c.flushall()
    feeds = scaled(FEEDS, step=100)
    t0 = time.perf_counter()
    load(c, feeds)
    ctx.raw["load_s"] = round(time.perf_counter() - t0, 1)
    members = sum(c.zcard("feed:home:%d" % n) for n in range(1, feeds + 1, max(1, feeds // 100)))
    mib = used_memory_mib(c)
    ctx.emit("used_memory_mib", mib, "MiB", "lower", gate=MEMORY)
    ctx.emit("bytes_per_member", mib * 1048576 / (feeds * MEMBERS), "bytes", "lower", gate=info())
    ctx.raw["sampled_members"] = members
    ctx.raw["runs"] = {}
    for conns, floor in CONNS.items():
        memtier(ctx.port, conns, WARMUP_SECONDS, feeds)
        runs = [memtier(ctx.port, conns, SECONDS, feeds) for _ in range(RUNS)]
        ctx.raw["runs"][conns] = runs
        ops = [r[0] for r in runs]
        ctx.emit("ops_c%d" % conns, median(ops), "ops/s", "higher",
                 gate=[THROUGHPUT, vs_redis(round(1 / floor, 3))], samples=ops)
        read_p50 = [r[2] for r in runs]
        ctx.emit("read_p50_ms_c%d" % conns, median(read_p50), "ms", "lower",
                 gate=jitter(vs_redis(0.6)), samples=read_p50)
        p99 = [r[1] for r in runs]
        ctx.emit("p99_ms_c%d" % conns, median(p99), "ms", "lower", gate=jitter(info()), samples=p99)
        read_p99 = [r[3] for r in runs]
        ctx.emit("read_p99_ms_c%d" % conns, median(read_p99), "ms", "lower", gate=jitter(info()),
                 samples=read_p99)
    ctx.emit("feeds", c.dbsize(), "keys", "higher", gate=EXACT)


if __name__ == "__main__":
    sys.exit(S.main())
