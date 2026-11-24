#!/usr/bin/env python3
"""`KEYS` with a leading and a trailing `*` over a large keyspace, alone and
beside GETs from many connections (#1018).

Where it comes from: Plane invalidates its Django response cache with
django-redis's `cache.keys(f"*{key}*")`, which sends `KEYS *<path>*` whenever
labels or workspace membership change. The issue's dataset: 500,000 keys of
2 KB named `:1:/api/workspaces/other-ws/labels/:user-N`, and 5 that match
`*/api/workspaces/acme/labels/*`.

Rows:
- `keys_ms`: one `KEYS` with nothing else running, the client's wall time,
  median of RUNS (after one warm-up); its reply must hold the 5 keys.
- `get_ops_noscan`, `get_p999_noscan_ms`: GETs of random keys from CONNS
  connections (this suite's own client, one asyncio process a client CPU,
  at most 4, each connection one GET at a time) for WINDOW_S.
- `get_ops_scan`, `get_p99_scan_ms`, `get_p999_scan_ms`, `get_max_scan_ms`:
  the same, with another connection sending the `KEYS` every 250 ms;
  `keys_scan_ms` its median time meanwhile.

One server an engine, `--threads 4` (Redis `--io-threads 4`), pinned to the
first half of the CPUs this process may use, the clients to the other half
(`REGRESS_SERVER_CPUS` / `REGRESS_CLIENT_CPUS`, as suite 49).

Gates, in the same session: `keys_ms` at most 0.2x v1.2.2's and at most
0.2x Redis's (master before #1018 measured 0.21-0.23x and 0.23-0.25x: they hold
the gain of reading the entries ahead); `get_p999_scan_ms` and
`get_max_scan_ms` at most 0.25x v1.2.2's and 0.25x Redis's (on both, about
one `KEYS` long or more: on Redis every GET that arrives during a scan
waits for it, on v1.2.2 those of the connections on the worker that runs
it); `get_ops_scan` within 10 % of recorded. A run on a Standard_D8s_v5
measured, for the candidate, v1.2.2 and Redis 8.10.2, `keys_ms` 17.6, 202
and 184, `get_p999_scan_ms` 3.6, 311 and 191, and `get_max_scan_ms` 6.1,
1,000 (the suite's ceiling) and 192 (see README, "Recorded run, suite 50").
Every other row is shown, not gated.
`min_cpus=8`: needs a Standard_D8s_v5 (`run-all.sh --min-cpus-only`).
Linux only (it pins CPUs).
"""
import array
import asyncio
import multiprocessing
import os
import random
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, THROUGHPUT, vs_redis, vs_v1_2_2, info, median, scaled  # noqa: E402
from nodes import Nodes  # noqa: E402

S = Suite("50-keys-wildcard", [
    Config("uring4", io="uring", threads=4, self_hosted=True),
], min_cpus=8)
KEYS = scaled(500_000)
VALUE = b"v" * 2048
PATTERN = b"*/api/workspaces/acme/labels/*"
MATCHING = 5
RUNS = 20
CONNS = 500
WINDOW_S = 10.0
MAX_CLIENT_PROCS = 4
BUCKETS = 1_000_000  # 1 us buckets up to 1 s


def parse_cpus(text):
    out = set()
    for part in text.split(","):
        part = part.strip()
        if "-" in part:
            a, b = part.split("-")
            out.update(range(int(a), int(b) + 1))
        elif part:
            out.add(int(part))
    return out


def cpu_split():
    server, client = os.environ.get("REGRESS_SERVER_CPUS"), os.environ.get("REGRESS_CLIENT_CPUS")
    cpus = sorted(os.sched_getaffinity(0))
    half = len(cpus) // 2
    return (parse_cpus(server) if server else set(cpus[:half]),
            parse_cpus(client) if client else set(cpus[half:]))


def pin(pid, cpus):
    for tid in os.listdir("/proc/%d/task" % pid):
        try:
            os.sched_setaffinity(int(tid), cpus)
        except OSError:
            pass  # a thread that ended meanwhile


def key_for(i):
    return b":1:/api/workspaces/other-ws/labels/:user-%d" % i


def preload(c):
    for start in range(0, KEYS, 2000):
        p = c.pipeline(transaction=False)
        for i in range(start, min(start + 2000, KEYS)):
            p.set(key_for(i), VALUE)
        p.execute()
    for i in range(MATCHING):
        c.set(b":1:/api/workspaces/acme/labels/:user-%d" % i, VALUE)


def keys_once(c):
    t0 = time.perf_counter()
    got = c.execute_command("KEYS", PATTERN)
    ms = (time.perf_counter() - t0) * 1000
    if len(got) != MATCHING:
        raise RuntimeError("KEYS returned %d keys, not %d" % (len(got), MATCHING))
    return ms


async def one_connection(port, deadline, hist):
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    perf = time.perf_counter
    try:
        while perf() < deadline:
            k = key_for(random.randrange(KEYS))
            t0 = perf()
            writer.write(b"*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n" % (len(k), k))
            head = await reader.readline()
            if head[:1] != b"$" or int(head[1:-2]) != len(VALUE):
                raise RuntimeError("unexpected reply %r" % head[:40])
            await reader.readexactly(len(VALUE) + 2)
            us = int((perf() - t0) * 1e6)
            hist[us if us < BUCKETS else BUCKETS - 1] += 1
    finally:
        writer.close()


def client_proc(port, per, cpus, start_at, seconds, out):
    try:
        os.sched_setaffinity(0, cpus)

        async def main():
            hist = array.array("I", bytes(4 * BUCKETS))
            await asyncio.sleep(max(0.0, start_at - time.time()))
            deadline = time.perf_counter() + seconds
            await asyncio.gather(*(one_connection(port, deadline, hist) for _ in range(per)))
            return hist

        out.put((asyncio.run(main()).tobytes(), None))
    except Exception as e:  # reported by the parent
        out.put((b"", repr(e)))


def percentile(hist, total, p):
    want, acc = total * p, 0
    for us, n in enumerate(hist):
        acc += n
        if acc >= want:
            return us
    return len(hist)


def gets(port, client_cpus, scan_client=None):
    """GETs from CONNS connections for WINDOW_S; with scan_client, a KEYS every 250 ms meanwhile."""
    cpus = sorted(client_cpus)
    procs = min(MAX_CLIENT_PROCS, len(cpus))
    mpc = multiprocessing.get_context("fork")
    out = mpc.Queue()
    start_at = time.time() + 1.5
    ps = [mpc.Process(target=client_proc, args=(port, CONNS // procs, {cpus[i % len(cpus)]}, start_at,
                                                WINDOW_S, out))
          for i in range(procs)]
    for p in ps:
        p.start()
    scans = []
    stop = threading.Event()

    def scanner():
        time.sleep(max(0.0, start_at - time.time()))
        while not stop.is_set():
            t0 = time.perf_counter()
            scans.append(keys_once(scan_client))
            stop.wait(max(0.0, 0.25 - (time.perf_counter() - t0)))

    th = threading.Thread(target=scanner) if scan_client is not None else None
    if th:
        th.start()
    results = [out.get(timeout=WINDOW_S + 120) for _ in ps]
    stop.set()
    if th:
        th.join(60)
    for p in ps:
        p.join(30)
    errors = [r[1] for r in results if r[1]]
    if errors:
        raise RuntimeError("client: %s" % errors[0])
    hist = [0] * BUCKETS
    top = 0
    for r in results:
        for i, n in enumerate(array.array("I", r[0])):
            if n:
                hist[i] += n
                top = max(top, i)
    ops = sum(hist)
    if ops == 0:
        raise RuntimeError("no GET was answered")
    return {"ops": ops / WINDOW_S, "p99_ms": percentile(hist, ops, 0.99) / 1000,
            "p999_ms": percentile(hist, ops, 0.999) / 1000, "max_ms": top / 1000, "scans": scans}


@S.run
def run(ctx):
    if not hasattr(os, "sched_getaffinity"):
        raise RuntimeError("Linux only: the suite pins CPUs")
    server_cpus, client_cpus = cpu_split()
    ctx.raw["server_cpus"], ctx.raw["client_cpus"] = sorted(server_cpus), sorted(client_cpus)
    os.sched_setaffinity(0, client_cpus)
    args = ["--io-threads", "4"] if ctx.kind == "redis" else []
    with Nodes(ctx) as ns:
        node = ns.start(args=args)
        pin(node.pid, server_cpus)
        c = node.client()
        t0 = time.perf_counter()
        preload(c)
        ctx.raw["load_s"] = round(time.perf_counter() - t0, 1)
        pin(node.pid, server_cpus)  # threads started since
        keys_once(c)
        ms = [keys_once(c) for _ in range(RUNS)]
        ctx.raw["keys_ms"] = ms
        ctx.emit("keys_ms", median(ms), "ms", "lower", gate=[vs_v1_2_2(0.2), vs_redis(0.2)], samples=ms)
        alone = gets(node.port, client_cpus)
        scan = gets(node.port, client_cpus, scan_client=node.client())
        ctx.raw["gets_alone"] = {k: v for k, v in alone.items() if k != "scans"}
        ctx.raw["gets_scan"] = scan
        if len(scan["scans"]) < WINDOW_S * 2:
            raise RuntimeError("only %d KEYS during the window" % len(scan["scans"]))
        ctx.emit("get_ops_noscan", alone["ops"], "ops/s", "higher", gate=info())
        ctx.emit("get_p999_noscan_ms", alone["p999_ms"], "ms", "lower", gate=info())
        ctx.emit("get_ops_scan", scan["ops"], "ops/s", "higher", gate=THROUGHPUT)
        ctx.emit("get_p99_scan_ms", scan["p99_ms"], "ms", "lower", gate=info())
        ctx.emit("get_p999_scan_ms", scan["p999_ms"], "ms", "lower", gate=[vs_v1_2_2(0.25), vs_redis(0.25)])
        ctx.emit("get_max_scan_ms", scan["max_ms"], "ms", "lower", gate=[vs_v1_2_2(0.25), vs_redis(0.25)])
        ctx.emit("keys_scan_ms", median(scan["scans"]), "ms", "lower", gate=info(),
                 samples=scan["scans"])


if __name__ == "__main__":
    sys.exit(S.main())
