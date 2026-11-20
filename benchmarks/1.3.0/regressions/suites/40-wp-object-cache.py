#!/usr/bin/env python3
"""#849: a WordPress-shaped object cache (Redis Object Cache and the like),
the case published in kividb.io/blog/wordpress-redis-object-cache-slow-under-load-how-to-speed-it-up:
1.4-1.75x Redis's `GET`/`SET` throughput, a third to a half of its median
latency at 100 clients, and 7-25 % less memory for plain strings.

Dataset: 1,000,000 string keys `key:000000000000` .. `key:000000999999`, the
names `redis-benchmark -r 1000000` draws, each a value of D bytes (`x`
repeated, as `redis-benchmark -d` makes it). Loaded with `SET` in pipelines of
500, so the benchmark's `GET`s all hit. Two configurations: `d512` and
`d2048` (D = 512 and 2,048).

Load: `redis-benchmark -c 100 -d D -r 1000000 -t get` (then `-t set`) with
`--threads 4`, 2,000,000 requests a run (500,000 for `SET`); one warm-up run,
then RUNS runs, the metric is the median. The issue's command has no
`--threads`, and with one client thread this machine's client is the limit at
about 131,000 requests a second for every server (measured 2026-11-20 on the
recording VM: Redis 8.10.2 and the candidate within 2 %, 2 KB, 100
connections), so it cannot tell them apart: `--threads 4` puts the limit on
the servers. Rows per configuration and
command: requests a second, the median (p50) and p99 latency; and, once the
dataset is loaded, `rss_mib` (`VmRSS` of the server) and `used_memory_mib`
(`INFO`), with `bytes_per_key` (used_memory over the keys, shown).

Gates (the candidate): `GET` at 2 KB at least 1.3x Redis in the same session
(the issue's floor, 15 % under the published figure); the memory at most 1.0x
Redis's RSS, for both sizes; every throughput row within 10 % of recorded
(scaled by the host speed), memory within 5 %. The other ratios (`GET` at
512 B, `SET`, the latencies) are recorded and shown next to Redis's in
`summary.md`; the latency rows are `jitter`, shown, not gated.

CPU: as suites 20 and 34: on Linux the server is pinned to the first half of
the CPUs (`--threads` as many) and this process, and with it redis-benchmark
and the loader, to the other half (`REGRESS_SERVER_CPUS` / `REGRESS_CLIENT_CPUS`
choose others). Needs 8 vCPUs (`min_cpus=8`): `run-all.sh --min-cpus-only` on
a `Standard_D8s_v5`.

redis-benchmark is taken from the directory of the Redis binary (8.10.x).
"""
import csv
import io
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, THROUGHPUT, MEMORY, info, jitter, vs_redis, median, scaled  # noqa: E402
from nodes import redis_tool  # noqa: E402
from rss import vmrss_mib  # noqa: E402

KEYS = 1_000_000
CLIENTS = 100
BENCH_THREADS = 4
RUNS = 3
REQUESTS = {"get": 2_000_000, "set": 500_000}
GET_FLOOR_2K = 1.3  # published 1.4-1.75x


def parse_cpus(text):
    """`0-3,6` -> [0, 1, 2, 3, 6]."""
    cpus = []
    for part in text.split(","):
        if "-" in part:
            lo, hi = part.split("-")
            cpus.extend(range(int(lo), int(hi) + 1))
        elif part.strip():
            cpus.append(int(part))
    return cpus


def cpu_split():
    """(server CPUs, client CPUs), or (None, None) where nothing is pinned."""
    if not hasattr(os, "sched_getaffinity"):
        return None, None
    server, client = os.environ.get("REGRESS_SERVER_CPUS"), os.environ.get("REGRESS_CLIENT_CPUS")
    if server and client:
        return parse_cpus(server), parse_cpus(client)
    cpus = sorted(os.sched_getaffinity(0))
    if len(cpus) < 2:
        return None, None
    half = len(cpus) // 2
    return cpus[:half], cpus[half:]


SERVER_CPUS, CLIENT_CPUS = cpu_split()
THREADS = len(SERVER_CPUS) if SERVER_CPUS else 0
S = Suite("40-wp-object-cache", [Config("d512", threads=THREADS), Config("d2048", threads=THREADS)], min_cpus=8)


def pin_server(pid):
    for tid in os.listdir("/proc/%d/task" % pid):
        os.sched_setaffinity(int(tid), SERVER_CPUS)


def load(c, keys, size):
    value = b"x" * size
    p = c.pipeline(transaction=False)
    for i in range(keys):
        p.set("key:%012d" % i, value)
        if i % 500 == 499:
            p.execute()
    p.execute()


def bench(port, command, size, keys, requests):
    """One redis-benchmark run -> (requests/s, p50 ms, p99 ms)."""
    cmd = [redis_tool("redis-benchmark"), "-p", str(port), "-c", str(CLIENTS), "-d", str(size),
           "-r", str(keys), "-n", str(requests), "--threads", str(BENCH_THREADS),
           "-t", command, "--csv"]
    res = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    if res.returncode != 0:
        raise RuntimeError("redis-benchmark exited %d: %s" % (res.returncode, res.stderr[-500:]))
    rows = list(csv.DictReader(io.StringIO(res.stdout)))
    if not rows:
        raise RuntimeError("redis-benchmark printed no result: %r" % res.stdout[-300:])
    row = rows[0]
    rps = float(row["rps"])
    if not rps > 0:
        raise RuntimeError("redis-benchmark did no work: %r" % row)
    return rps, float(row["p50_latency_ms"]), float(row["p99_latency_ms"])


@S.run
def run(ctx):
    size = int(ctx.config.name[1:])
    c = ctx.client()
    c.flushall()
    keys = scaled(KEYS, step=1000)
    pinned = bool(SERVER_CPUS and ctx.pid)
    if pinned:
        pin_server(ctx.pid)
        os.sched_setaffinity(0, CLIENT_CPUS)  # the loader and redis-benchmark inherit it
    ctx.raw["cpus"] = {"pinned": pinned, "server": SERVER_CPUS, "client": CLIENT_CPUS,
                       "os_cpus": os.cpu_count()}
    base = c.info("memory")["used_memory"]
    load(c, keys, size)
    assert c.dbsize() == keys, (c.dbsize(), keys)
    assert len(c.get("key:%012d" % (keys - 1))) == size
    time.sleep(3)
    used = c.info("memory")["used_memory"] - base
    rss = vmrss_mib(ctx.pid) if ctx.pid else None
    ctx.emit("keys", keys, "keys", "higher", gate=info())
    if rss is not None:
        ctx.emit("rss_mib", rss, "MiB", "lower", gate=[vs_redis(1.0), MEMORY])
    ctx.emit("used_memory_mib", c.info("memory")["used_memory"] / 1048576.0, "MiB", "lower", gate=info())
    ctx.emit("bytes_per_key", used / float(keys), "B", "lower", gate=info())
    ctx.raw["runs"] = {}
    for command in ("get", "set"):
        n = scaled(REQUESTS[command], step=10000)
        bench(ctx.port, command, size, keys, n)  # warm-up
        runs = [bench(ctx.port, command, size, keys, n) for _ in range(RUNS)]
        ctx.raw["runs"][command] = runs
        rps = [r[0] for r in runs]
        gate = [THROUGHPUT]
        if command == "get" and size == 2048:
            gate.append(vs_redis(round(1 / GET_FLOOR_2K, 3)))
        ctx.emit("%s_rps" % command, median(rps), "ops/s", "higher", gate=gate, samples=rps)
        p50 = [r[1] for r in runs]
        p99 = [r[2] for r in runs]
        ctx.emit("%s_p50_ms" % command, median(p50), "ms", "lower", gate=jitter(info()), samples=p50)
        ctx.emit("%s_p99_ms" % command, median(p99), "ms", "lower", gate=jitter(info()), samples=p99)


if __name__ == "__main__":
    sys.exit(S.main())
