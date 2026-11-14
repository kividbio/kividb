#!/usr/bin/env python3
"""#637: each consumer's pending entries indexed (#736).

The issue's scenario: stream `polled` of 200,000 one-field entries, group `g`,
50 consumers c1..c50 that each read 4,000 with XREADGROUP ... COUNT 4000 >,
so 200,000 entries are pending (c1 owns the first 4,000, c50 the last).

  memory   used_memory growth per pending entry over the 50 reads (the cost
           #736 accepted: 122 -> 129 B)
  latency  from one client, 20 calls each (median, ms): XINFO CONSUMERS,
           the XPENDING summary, XPENDING - + 10 c50, and XREADGROUP of c50's
           history (ID 0, COUNT 10)
  polled   redis-benchmark -c 20: 200,000 XADDs to `polled` and then 200,000
           to another stream, each while a second client sends XINFO
           CONSUMERS polled g every 50 ms; requests a second and the longest
           XADD

Client and server share the VM and the server is not pinned (the PR pinned it
to two cores), so absolute numbers differ from #736's; the ratios hold.
"""
import os
import re
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, KEEP_IMPROVEMENT, rel, info, scaled, median  # noqa: E402
from nodes import redis_tool  # noqa: E402

S = Suite("06-stream-consumers", [Config("pel")])
LAT = [rel(50, abs=0.5), KEEP_IMPROVEMENT]


def timed(c, args, n=20):
    xs = []
    for _ in range(n):
        t = time.perf_counter()
        c.execute_command(*args)
        xs.append((time.perf_counter() - t) * 1000)
    return median(xs), xs


def bench_under_poll(port, key, n):
    """XADDs from redis-benchmark while XINFO CONSUMERS is polled every 50 ms."""
    import redis
    stop = threading.Event()
    polls = []

    def poll():
        c = redis.Redis(port=port)
        while not stop.is_set():
            t = time.perf_counter()
            c.execute_command("XINFO", "CONSUMERS", "polled", "g")
            polls.append((time.perf_counter() - t) * 1000)
            stop.wait(0.05)

    th = threading.Thread(target=poll, daemon=True)
    th.start()
    time.sleep(0.2)
    out = subprocess.run([redis_tool("redis-benchmark"), "-p", str(port), "-n", str(n), "-c", "20",
                          "XADD", key, "*", "f", "v"], capture_output=True, text=True, timeout=900).stdout
    stop.set()
    th.join()
    out = out.replace("\r", "\n")
    rps = float(re.findall(r"throughput summary: ([\d.]+) requests per second", out)[-1])
    lat = re.search(r"latency summary \(msec\):\s*\n\s*avg\s+min\s+p50\s+p95\s+p99\s+max\s*\n\s*([\d.\s]+)", out)
    avg, mn, p50, p95, p99, mx = (float(x) for x in lat.group(1).split()[:6])
    return {"rps": rps, "avg": avg, "p99": p99, "max": mx, "polls": len(polls),
            "poll_ms_median": median(polls)}


@S.run
def run(ctx):
    c = ctx.client()
    c.flushall()
    n = scaled(200_000)
    per = n // 50
    p = c.pipeline(transaction=False)
    for i in range(n):
        p.execute_command("XADD", "polled", "*", "f", "v")
        if i % 1000 == 999:
            p.execute()
    p.execute()
    c.execute_command("XGROUP", "CREATE", "polled", "g", "0")
    time.sleep(0.5)
    u0 = c.info("memory")["used_memory"]
    for i in range(1, 51):
        c.execute_command("XREADGROUP", "GROUP", "g", "c%d" % i, "COUNT", per, "STREAMS", "polled", ">")
    pending = c.xpending("polled", "g")["pending"]
    assert pending == per * 50, pending
    ctx.emit("pending_bytes_per_entry", (c.info("memory")["used_memory"] - u0) / float(pending), "B", "lower",
             gate=rel(5, abs=2))
    raw = ctx.client()
    raw.response_callbacks.clear()  # time the command, not redis-py's parsing of the reply
    for name, args in (("xinfo_consumers_ms", ("XINFO", "CONSUMERS", "polled", "g")),
                       ("xpending_summary_ms", ("XPENDING", "polled", "g")),
                       ("xpending_c50_ms", ("XPENDING", "polled", "g", "-", "+", "10", "c50")),
                       ("xreadgroup_c50_history_ms",
                        ("XREADGROUP", "GROUP", "g", "c50", "COUNT", "10", "STREAMS", "polled", "0"))):
        v, xs = timed(raw, args)
        ctx.emit(name, v, "ms", "lower", gate=LAT, samples=xs)
    nb = scaled(200_000)
    r = bench_under_poll(ctx.port, "polled", nb)
    ctx.raw["polled"] = r
    ctx.emit("xadd_polled_stream_rps", r["rps"], "req/s", "higher", gate=[rel(25), KEEP_IMPROVEMENT])
    ctx.emit("xadd_polled_stream_max_ms", r["max"], "ms", "lower", gate=info())
    r = bench_under_poll(ctx.port, "other", nb)
    ctx.raw["other"] = r
    ctx.emit("xadd_other_stream_rps", r["rps"], "req/s", "higher", gate=rel(25))
    ctx.emit("xadd_other_stream_max_ms", r["max"], "ms", "lower", gate=info())
    ctx.emit("xinfo_polls", r["polls"], "calls", "higher", gate=info())


if __name__ == "__main__":
    sys.exit(S.main())
