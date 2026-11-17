#!/usr/bin/env python3
"""#849: envoyproxy/ratelimit's Redis traffic, the pipelined `INCRBY` +
`EXPIRE` pair published in envoyproxy/ratelimit#1315: 1.8x Redis at 200
connections, 2.0x at 500.

For each hit, the limiter sends, pipelined, on one key:

  INCRBY rl:<n> 1
  EXPIRE rl:<n> 60

Load: memtier_benchmark, 4 threads, 200 and 500 connections, `--pipeline 2`,
keys rl:1 .. rl:100000 uniformly, `--transaction`, which resolves every
`__key__` of one pass through the two commands to the same key, so the
EXPIRE lands on the key the INCRBY counted, as the limiter's does (without it
memtier draws a new key for each). Checked with MONITOR on memtier 2.6.1:

  "INCRBY" "rl:74738" "1"
  "EXPIRE" "rl:74738" "60"

(12,754 pairs in a one-second run, every EXPIRE on its INCRBY's key.) The
suite checks it each time, before anything is measured: a one-second run
under MONITOR, which fails the suite if any pair is on two keys.

Each connection count: one warm-up run, then RUNS runs of SECONDS; the metric
is the median. Rows per connection count: ops/s, and p99 shown beside it.
`keys_without_ttl`, counted over every key after the runs, must be 0: a key
the INCRBY made and no EXPIRE reached would be a counter that never resets.
The suite fails if the scan sees under 90 % of the keys, or a run does no
work, rather than count nothing and pass.

CPU: on Linux the server's threads (all of them, `/proc/<pid>/task`) are
pinned to the first half of the CPUs this process may use and memtier to the
other half, the same for KiviDB and Redis; KiviDB runs with as many threads
as the server has CPUs. `REGRESS_SERVER_CPUS` / `REGRESS_CLIENT_CPUS`
(`0-3`, `4,5`) choose others. Elsewhere (macOS) nothing is pinned, and the
run says so in `raw`.

Gates, against Redis in the same session, ~15 % under what was published:
ops/s at least 1.5x Redis at 200 connections and 1.7x at 500; and THROUGHPUT
against the recorded candidate. The p99 rows are shown, not gated (`jitter`).

Needs memtier_benchmark 2.6.1 or newer (`--transaction`, and the stderr line
it logs for an error reply): `$MEMTIER`, else on PATH.
"""
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import (Suite, Config, THROUGHPUT, bound, info, jitter, vs_redis, median, scaled)  # noqa: E402

KEYS = 100_000
TTL = 60
THREADS = 4
SECONDS = 20
WARMUP_SECONDS = 10
RUNS = 3
# Connections -> the floor over Redis for ops/s (published 1.8x / 2.0x).
CONNS = {200: 1.5, 500: 1.7}
COMMANDS = ["INCRBY __key__ 1", "EXPIRE __key__ %d" % TTL]


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
S = Suite("20-envoy-ratelimit", [Config("default", threads=len(SERVER_CPUS) if SERVER_CPUS else 0)])


def pin_server(pid):
    """Every thread of the server onto SERVER_CPUS; threads it starts later
    inherit it from the one that starts them."""
    for tid in os.listdir("/proc/%d/task" % pid):
        os.sched_setaffinity(int(tid), SERVER_CPUS)


def memtier_bin():
    for p in (os.environ.get("MEMTIER", ""), shutil.which("memtier_benchmark") or ""):
        if p and os.access(p, os.X_OK):
            return p
    raise RuntimeError("memtier_benchmark not found ($MEMTIER or PATH)")


def memtier(memtier_path, port, conns, seconds, keys, threads=THREADS):
    """One run -> (ops/s, p99 ms)."""
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, "out.json")
        cmd = [memtier_path, "-s", "127.0.0.1", "-p", str(port), "--protocol=redis",
               "-t", str(threads), "-c", str(max(1, conns // threads)), "--test-time", str(seconds),
               "--pipeline", "2", "--transaction", "--key-prefix=rl:",
               "--key-minimum=1", "--key-maximum=%d" % keys,
               "--distinct-client-seed", "--hide-histogram", "--json-out-file", out]
        for command in COMMANDS:
            # --command-key-pattern must follow its --command.
            cmd += ["--command=" + command, "--command-key-pattern=R"]
        pin = (lambda: os.sched_setaffinity(0, CLIENT_CPUS)) if CLIENT_CPUS else None
        res = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
                             timeout=seconds + 120, preexec_fn=pin)
        tail = "\n".join(res.stderr.strip().splitlines()[-10:])
        if res.returncode != 0:
            raise RuntimeError("memtier_benchmark exited %d:\n%s" % (res.returncode, tail))
        # memtier answers error replies with a line on stderr, and exits 0.
        errors = [line for line in res.stderr.splitlines() if "error response" in line]
        if errors:
            raise RuntimeError("memtier_benchmark got %d error replies, e.g.:\n%s"
                               % (len(errors), "\n".join(errors[:5])))
        with open(out) as f:
            total = json.load(f)["ALL STATS"]["Totals"]
    if not total["Ops/sec"] > 0:
        raise RuntimeError("memtier_benchmark did no work: %r" % total)
    return total["Ops/sec"], total["Percentile Latencies"]["p99.00"]


MONITOR_LINE = re.compile(r'\[\d+ ([^\]]+)\] "(INCRBY|EXPIRE)" "([^"]*)"')


def check_one_key(memtier_path, port, keys):
    """A one-second run under MONITOR: every EXPIRE is on the key of the
    INCRBY its connection sent before it. -> (pairs, pairs on two keys).
    Raises if there were none, or any was on two keys: a memtier whose
    `--transaction` no longer keeps one key a pass would otherwise measure
    another workload, and leave counters that never reset."""
    sock = socket.create_connection(("127.0.0.1", port))
    sock.sendall(b"MONITOR\r\n")
    chunks, stop = [], threading.Event()

    def read():
        sock.settimeout(0.2)
        while not stop.is_set():
            try:
                data = sock.recv(1 << 16)
            except socket.timeout:
                continue
            if not data:
                return
            chunks.append(data)

    reader = threading.Thread(target=read)
    reader.start()
    try:
        time.sleep(0.2)
        memtier(memtier_path, port, 4, 1, keys, threads=1)
        time.sleep(0.5)
    finally:
        stop.set()
        reader.join()
        sock.close()
    last, pairs, apart = {}, 0, 0
    for line in b"".join(chunks).decode("utf-8", "replace").splitlines():
        m = MONITOR_LINE.search(line)
        if not m:
            continue
        client, command, key = m.groups()
        if command == "INCRBY":
            last[client] = key
        else:
            pairs += 1
            apart += last.get(client) != key
    if pairs == 0 or apart:
        raise RuntimeError("MONITOR: %d of %d INCRBY + EXPIRE pairs on two keys" % (apart, pairs))
    return pairs, apart


def keys_without_ttl(c):
    """Keys with no time to live: an INCRBY that no EXPIRE reached. -> (keys
    without one, keys seen)."""
    missing, seen = 0, 0
    cursor = 0
    while True:
        cursor, keys = c.scan(cursor, match="rl:*", count=1000)
        if keys:
            p = c.pipeline(transaction=False)
            for k in keys:
                p.ttl(k)
            ttls = p.execute()
            seen += len(keys)
            missing += sum(1 for t in ttls if t == -1)
        if cursor == 0:
            return missing, seen


@S.run
def run(ctx):
    memtier_path = memtier_bin()  # before anything else
    c = ctx.client()
    c.flushall()
    keys = scaled(KEYS, step=1000)
    pinned = bool(SERVER_CPUS and ctx.pid)
    if pinned:
        pin_server(ctx.pid)
    ctx.raw["cpus"] = {"pinned": pinned, "server": SERVER_CPUS, "client": CLIENT_CPUS,
                       "os_cpus": os.cpu_count()}
    pairs, _ = check_one_key(memtier_path, ctx.port, keys)
    ctx.raw["monitor_pairs"] = pairs
    ctx.raw["runs"] = {}
    for conns, floor in CONNS.items():
        memtier(memtier_path, ctx.port, conns, WARMUP_SECONDS, keys)
        runs = [memtier(memtier_path, ctx.port, conns, SECONDS, keys) for _ in range(RUNS)]
        ctx.raw["runs"][conns] = runs
        ops = [r[0] for r in runs]
        ctx.emit("ops_c%d" % conns, median(ops), "ops/s", "higher",
                 gate=[THROUGHPUT, vs_redis(round(1 / floor, 3))], samples=ops)
        p99 = [r[1] for r in runs]
        ctx.emit("p99_ms_c%d" % conns, median(p99), "ms", "lower", gate=jitter(info()), samples=p99)
    missing, seen = keys_without_ttl(c)
    ctx.raw["keys_seen"] = seen
    # Counted over the keys the runs made, or it says nothing: a wrong
    # prefix, or runs that did no work, would leave none to count.
    if seen < 0.9 * keys:
        raise RuntimeError("the scan saw %d rl:* keys of %d" % (seen, keys))
    ctx.emit("keys_without_ttl", missing, "keys", "lower", gate=bound(max=0))


if __name__ == "__main__":
    sys.exit(S.main())
