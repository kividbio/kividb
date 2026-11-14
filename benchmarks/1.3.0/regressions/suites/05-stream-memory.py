#!/usr/bin/env python3
"""#639: stream entries stored in packed blocks (#683, landed by #713).

Memory per entry is the growth of INFO used_memory over the load divided by
the entries, as #683 measured it; each stream gets auto IDs (XADD key * ...),
loaded in pipelines of 1,000 from one client.

  one1m    1,000,000 entries of one field f=v; also VmRSS after the load
  shapes   one server, four streams one after the other (FLUSHALL between):
           shared5   200,000 entries of five fields f1..f5 = v
           unique5   200,000 entries of five fields whose names differ in
                     every entry (a<i> b<i> c<i> d<i> e<i>)
           val200    200,000 entries of one field with a 200-byte value
           then the cost side of #683 with redis-benchmark (-c 4 -P 16):
           XADD of five fields, XRANGE - + COUNT 10 over five-field entries
           and XRANGE of one ID, in requests a second.

Gates: memory per entry within 5 % (or 1 B) of recorded and keeping 60 % of
the gain over v1.2.2; RSS within 5 % or 5 MiB. The throughput rows are the
costs #683 accepted (-2 to -7 % on five-field reads); they are gated at 15 %
(their run-to-run spread with client and server on one 4-vCPU VM is up to
about 8 %), so a large loss shows and the measured trade-off does not.
"""
import os
import re
import subprocess
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, MEMORY, KEEP_IMPROVEMENT, rel, info, scaled, median  # noqa: E402
from rss import vmrss_mib, used_memory_mib  # noqa: E402
from nodes import redis_tool  # noqa: E402

S = Suite("05-stream-memory", [Config("one1m"), Config("shapes")])
PER_ENTRY = [rel(5, abs=1), KEEP_IMPROVEMENT]
RATE = rel(15)


def used(c):
    return c.info("memory")["used_memory"]


def load(c, key, n, fields):
    """fields(i) -> flat [name, value, ...] of entry i."""
    p = c.pipeline(transaction=False)
    for i in range(n):
        p.execute_command("XADD", key, "*", *fields(i))
        if i % 1000 == 999:
            p.execute()
    p.execute()


def per_entry(ctx, c, name, n, fields):
    c.flushall()
    time.sleep(0.5)
    u0 = used(c)
    t = time.perf_counter()
    load(c, "s", n, fields)
    secs = time.perf_counter() - t
    assert c.xlen("s") == n, "XLEN %d != %d" % (c.xlen("s"), n)
    b = (used(c) - u0) / float(n)
    ctx.emit(name + "_bytes_per_entry", b, "B", "lower", gate=PER_ENTRY)
    ctx.emit(name + "_load_s", secs, "s", "lower", gate=info())
    return b


def bench(ctx, port, args, n):
    """redis-benchmark requests a second (median of 3)."""
    rb = redis_tool("redis-benchmark")
    rates = []
    for _ in range(3):
        out = subprocess.run([rb, "-p", str(port), "-n", str(n), "-c", "4", "-P", "16", "-q"] + args,
                             capture_output=True, text=True, timeout=600).stdout
        m = re.findall(r"([\d.]+) requests per second", out.replace("\r", "\n"))
        rates.append(float(m[-1]))
    return median(rates), rates


@S.run
def run(ctx):
    c = ctx.client()
    if ctx.config.name == "one1m":
        n = scaled(1_000_000)
        per_entry(ctx, c, "one_field", n, lambda i: ("f", "v"))
        time.sleep(1.0)
        ctx.emit("one_field_rss_mib", vmrss_mib(ctx.pid), "MiB", "lower", gate=[rel(5, abs=5), KEEP_IMPROVEMENT])
        ctx.emit("one_field_used_mib", used_memory_mib(c), "MiB", "lower", gate=info())
        return
    n = scaled(200_000)
    per_entry(ctx, c, "shared5", n, lambda i: ("f1", "v", "f2", "v", "f3", "v", "f4", "v", "f5", "v"))
    per_entry(ctx, c, "unique5", n, lambda i: ("a%d" % i, "v", "b%d" % i, "v", "c%d" % i, "v",
                                               "d%d" % i, "v", "e%d" % i, "v"))
    val = "x" * 200
    per_entry(ctx, c, "val200", n, lambda i: ("f", val))
    # Costs: five-field writes and reads.
    c.flushall()
    load(c, "s5", scaled(100_000), lambda i: ("f1", "v1", "f2", "v2", "f3", "v3", "f4", "v4", "f5", "v5"))
    one_id = c.xrange("s5", count=1)[0][0].decode()
    r, s = bench(ctx, ctx.port, ["XADD", "w5", "*", "f1", "v1", "f2", "v2", "f3", "v3", "f4", "v4", "f5", "v5"],
                 scaled(500_000))
    ctx.emit("xadd5_rps", r, "req/s", "higher", gate=RATE, samples=s)
    r, s = bench(ctx, ctx.port, ["XRANGE", "s5", "-", "+", "COUNT", "10"], scaled(300_000))
    ctx.emit("xrange5_count10_rps", r, "req/s", "higher", gate=RATE, samples=s)
    r, s = bench(ctx, ctx.port, ["XRANGE", "s5", one_id, one_id], scaled(500_000))
    ctx.emit("xrange_one_id_rps", r, "req/s", "higher", gate=RATE, samples=s)


if __name__ == "__main__":
    sys.exit(S.main())
