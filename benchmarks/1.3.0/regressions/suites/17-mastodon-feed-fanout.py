#!/usr/bin/env python3
"""#849: Mastodon's home-feed fan-out (FeedManager), the mix published in
mastodon/mastodon#41702: 1.7x Redis at 200 connections, 2.0x at 500, timeline
read p50 halved.

Dataset: N = 20,000 home feeds `feed:home:<n>`, n = 10000 .. 10000 + N - 1 (five
digits, so an id made from n below has a fixed length), each a sorted set of
800 statuses whose score is their id, as FeedManager stores them: member j
(0..799) of feed n is the id (1000 + j) * 10^11 + n, from 100000000000000 + n
to 179900000000000 + n. Every id is below 2^53, so a score is the id exactly.

Load: memtier_benchmark, 4 threads, 200 and 500 connections, `--transaction`,
which resolves every `__key__` of one pass through the commands to the same
number n, drawn uniformly from the feeds. So one pass sends, for one feed n
(MIX below, with its weights):

  ZADD feed:home:n 9000000000n 9000000000n            a status pushed: score = id,
                                                      newer than every loaded one
  ZREMRANGEBYRANK feed:home:n 0 -801                  the trim to 800 after it
  ZREVRANGE feed:home:n 40 40 WITHSCORES              the reblog fall-off check
  ZSCORE feed:home:n 1799000000n                      a status that is in the feed
                                                      (its newest loaded one)
  EXISTS feed:home:n                                  is the feed there
  ZREVRANGEBYSCORE feed:home:n +inf -inf LIMIT 0 20 WITHSCORES   a timeline page
  ZRANGEBYSCORE feed:home:n (1000000000n +inf LIMIT 0 20 WITHSCORES   newer than
                                                      min_id (its oldest loaded one)

`9000000000n` is the digits of n after `9000000000`, e.g. 900000000012345: memtier
puts n in place of `__key__` inside an argument. Checked with MONITOR on
memtier 2.6.1. One id per feed is all a pass can make, so the first ZADD to a
feed adds its id (801 members, and the trim takes the oldest); every later
one to that feed updates it in place, and its trim finds nothing to take.

Each connection count: one warm-up run, then RUNS runs of SECONDS; the metric
is the median. Rows per connection count: ops/s for the whole mix, the
timeline read's (ZREVRANGEBYSCORE) p50, and p99 of the mix and of the read,
shown next to them (several are worse than Redis at p99 today). Memory:
used_memory after the load, and its bytes per member. `feeds` (DBSIZE after
the runs) must stay N: the trim never empties a feed. The load is checked
(800 members in every sampled feed) before anything is measured, and a run
fails on any error reply memtier reports.

Gates, against Redis in the same session, ~15 % under what was published:
ops/s at least 1.5x Redis at 200 connections and 1.7x at 500, the read p50 at
most 0.6x Redis's; and THROUGHPUT / MEMORY against the recorded candidate. The
p99 rows are shown, not gated (`jitter`: client and server share the VM).

Needs memtier_benchmark 2.x with `--transaction`: `$MEMTIER`, else on PATH.
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
FIRST_FEED = 10_000  # feed numbers have five digits
MEMBERS = 800
THREADS = 4
SECONDS = 20
WARMUP_SECONDS = 10
RUNS = 3
# Connections -> the floor over Redis for the mix's ops/s (published 1.7x / 2.0x).
CONNS = {200: 1.5, 500: 1.7}
READ = "Zrevrangebyscores"  # memtier's name for the timeline read's stats

# ── The mix: the issue's seven commands. The WEIGHTS ARE A PLACEHOLDER, a
# reading of FeedManager; replace them with the published run's when
# recording. `__key__` is the feed number n, the same in every command of a
# pass (`--transaction`).
MIX = [
    ("ZADD feed:home:__key__ 9000000000__key__ 9000000000__key__", 3),
    ("ZREMRANGEBYRANK feed:home:__key__ 0 -801", 3),
    ("ZREVRANGE feed:home:__key__ 40 40 WITHSCORES", 1),
    ("ZSCORE feed:home:__key__ 1799000000__key__", 1),
    ("EXISTS feed:home:__key__", 1),
    ("ZREVRANGEBYSCORE feed:home:__key__ +inf -inf LIMIT 0 20 WITHSCORES", 2),
    ("ZRANGEBYSCORE feed:home:__key__ (1000000000__key__ +inf LIMIT 0 20 WITHSCORES", 1),
]


def memtier_bin():
    for p in (os.environ.get("MEMTIER", ""), shutil.which("memtier_benchmark") or ""):
        if p and os.access(p, os.X_OK):
            return p
    raise RuntimeError("memtier_benchmark not found ($MEMTIER or PATH)")


def status_id(j, n):
    """Member j of feed n, as the MIX's affixes make them."""
    return (1000 + j) * 10**11 + n


def load(c, feeds):
    for lo in range(FIRST_FEED, FIRST_FEED + feeds, 50):
        p = c.pipeline(transaction=False)
        for n in range(lo, min(lo + 50, FIRST_FEED + feeds)):
            p.zadd("feed:home:%d" % n, {str(status_id(j, n)): status_id(j, n) for j in range(MEMBERS)})
        p.execute()


def memtier(memtier_path, port, conns, seconds, feeds):
    """One run -> (ops/s, mix p99, read p50, read p99), latencies in ms."""
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, "out.json")
        cmd = [memtier_path, "-s", "127.0.0.1", "-p", str(port), "--protocol=redis",
               "-t", str(THREADS), "-c", str(conns // THREADS), "--test-time", str(seconds),
               "--transaction", "--key-prefix=", "--key-minimum=%d" % FIRST_FEED,
               "--key-maximum=%d" % (FIRST_FEED + feeds - 1),
               "--distinct-client-seed", "--hide-histogram", "--json-out-file", out]
        for command, ratio in MIX:
            # --command-key-pattern must follow its --command.
            cmd += ["--command=" + command, "--command-ratio=%d" % ratio, "--command-key-pattern=R"]
        res = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
                             timeout=seconds + 120)
        tail = "\n".join(res.stderr.strip().splitlines()[-10:])
        if res.returncode != 0:
            raise RuntimeError("memtier_benchmark exited %d:\n%s" % (res.returncode, tail))
        # memtier answers error replies with a line on stderr, and exits 0.
        errors = [line for line in res.stderr.splitlines() if "error response" in line]
        if errors:
            raise RuntimeError("memtier_benchmark got %d error replies, e.g.:\n%s"
                               % (len(errors), "\n".join(errors[:5])))
        with open(out) as f:
            stats = json.load(f)["ALL STATS"]
    total, read = stats["Totals"], stats[READ]
    return (total["Ops/sec"], total["Percentile Latencies"]["p99.00"],
            read["Percentile Latencies"]["p50.00"], read["Percentile Latencies"]["p99.00"])


@S.run
def run(ctx):
    memtier_path = memtier_bin()  # before the load, not after it
    c = ctx.client()
    c.flushall()
    feeds = scaled(FEEDS, step=100)
    t0 = time.perf_counter()
    load(c, feeds)
    ctx.raw["load_s"] = round(time.perf_counter() - t0, 1)
    sampled = range(FIRST_FEED, FIRST_FEED + feeds, max(1, feeds // 100))
    short = {n: c.zcard("feed:home:%d" % n) for n in sampled}
    short = {n: card for n, card in short.items() if card != MEMBERS}
    if short:
        raise RuntimeError("feeds not loaded with %d members: %r" % (MEMBERS, dict(list(short.items())[:5])))
    mib = used_memory_mib(c)
    ctx.emit("used_memory_mib", mib, "MiB", "lower", gate=MEMORY)
    ctx.emit("bytes_per_member", mib * 1048576 / (feeds * MEMBERS), "bytes", "lower", gate=info())
    ctx.raw["runs"] = {}
    for conns, floor in CONNS.items():
        memtier(memtier_path, ctx.port, conns, WARMUP_SECONDS, feeds)
        runs = [memtier(memtier_path, ctx.port, conns, SECONDS, feeds) for _ in range(RUNS)]
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
