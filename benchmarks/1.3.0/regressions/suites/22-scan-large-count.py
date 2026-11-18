#!/usr/bin/env python3
"""#884: SCAN with a large COUNT and a pattern that matches nothing.

Argo CD's API server sends `SCAN 0 MATCH revoked-token|* COUNT 10000` on
every replica, over a keyspace of mostly 50 KB values (its gzipped resource
trees) where no key matches. On 1.2.2 that call cost about 3x what it costs
Valkey 8.1, and every connection served by the thread running it waited for
it.

Dataset, one server per configuration (`--threads 4` for KiviDB, Redis with
its defaults): 40,000 keys `argocd|mfst|<i>` of 50 KB of random bytes.

Metrics:

  scan_call_us      mean time of one `SCAN <cursor> MATCH revoked-token|*
                    COUNT 10000` call, client side, over full iterations
                    (median of 3 rounds of 10 iterations, after one warm-up)
  scan_server_us    the server's own time per call (INFO commandstats
                    usec_per_call for scan, over the same rounds)
  scan_calls        calls one full iteration takes (4 at 40,000 keys)
  scan_returned     keys returned over one full iteration: 0, exactly
  ping_p99_ms       p99 of 4 probe connections' PINGs (one every 1 ms, each
                    in its own process) while 4 other connections loop the
                    same SCAN for 5 s
  ping_max_ms       the longest of those PINGs (shown, not gated)

Gates: the call times are a few hundred microseconds, a Python round trip
(about 50 us) included, so `rel` is 25 % or 30 us; scan_server_us must also
keep 60 % of the gain over v1.2.2 measured in the same session. The probes'
p99 is scheduling on a 4-vCPU VM shared with the clients: jitter, 50 % or
1 ms.
"""
import multiprocessing as mp
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import (Suite, Config, KEEP_IMPROVEMENT, EXACT, rel, jitter, info, median, scaled)  # noqa: E402
from probe import Probe, percentile  # noqa: E402

S = Suite("22-scan-large-count", [Config("t4", threads=4)])
KEYS = 40_000
VALUE = 50 * 1024
PATTERN = "revoked-token|*"
COUNT = 10_000
ROUNDS = 3
ITERATIONS = 10
LOOPERS = 4
LOOP_S = 5.0


def load(c, n):
    p = c.pipeline(transaction=False)
    for i in range(1, n + 1):
        p.set("argocd|mfst|%d" % i, os.urandom(VALUE))
        if i % 100 == 0:
            p.execute()
    p.execute()


def full_scan(c):
    """One full iteration: (calls, keys returned)."""
    cursor, calls, returned = 0, 0, 0
    while True:
        cursor, keys = c.scan(cursor, match=PATTERN, count=COUNT)
        calls += 1
        returned += len(keys)
        if cursor == 0:
            return calls, returned


def scan_usec(c):
    stats = c.info("commandstats").get("cmdstat_scan", {})
    return stats.get("calls", 0), stats.get("usec", 0)


def _looper(port, stop):
    import redis
    c = redis.Redis(host="127.0.0.1", port=port, socket_timeout=600)
    while not stop.is_set():
        full_scan(c)


@S.run
def run(ctx):
    c = ctx.client()
    c.flushall()
    n = scaled(KEYS, step=1000)
    t0 = time.perf_counter()
    load(c, n)
    ctx.raw["load_s"] = round(time.perf_counter() - t0, 1)
    ctx.emit("dbsize", c.dbsize(), "keys", "higher", gate=EXACT)

    calls, returned = full_scan(c)  # warm-up
    ctx.emit("scan_calls", calls, "calls", "lower", gate=info())
    ctx.emit("scan_returned", returned, "keys", "lower", gate=EXACT)

    client_us, server_us = [], []
    for _ in range(ROUNDS):
        c0, u0 = scan_usec(c)
        t = time.perf_counter()
        made = 0
        for _ in range(ITERATIONS):
            made += full_scan(c)[0]
        client_us.append((time.perf_counter() - t) / made * 1e6)
        c1, u1 = scan_usec(c)
        server_us.append((u1 - u0) / max(1, c1 - c0))
    ctx.emit("scan_call_us", median(client_us), "us", "lower", gate=rel(25, abs=30), samples=client_us)
    ctx.emit("scan_server_us", median(server_us), "us", "lower",
             gate=[rel(25, abs=30), KEEP_IMPROVEMENT], samples=server_us)

    mpc = mp.get_context("fork")
    stop = mpc.Event()
    loopers = [mpc.Process(target=_looper, args=(ctx.port, stop), daemon=True) for _ in range(LOOPERS)]
    with Probe(ctx.port, n=4) as pr:
        for p in loopers:
            p.start()
        time.sleep(0.5)
        t0 = time.perf_counter()
        time.sleep(LOOP_S)
        t1 = time.perf_counter()
        stop.set()
        for p in loopers:
            p.join(timeout=60)
    rtts = [rtt for probe in pr.samples for (sent, rtt) in probe if t0 <= sent <= t1]
    ctx.raw["probe_pings"] = len(rtts)
    ctx.emit("ping_p99_ms", percentile(rtts, 0.99) * 1e3, "ms", "lower", gate=jitter(rel(50, abs=1)))
    ctx.emit("ping_max_ms", (max(rtts) if rtts else float("nan")) * 1e3, "ms", "lower", gate=info())


if __name__ == "__main__":
    sys.exit(S.main())
