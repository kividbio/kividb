#!/usr/bin/env python3
"""#590: FT.INFO reads an index's document count off the index (#727).

KiviDB only: the Redis 8.10.2 on the VM image is built without the Query
Engine module (no FT.*), and #727 did not measure Redis.

On one server, in order:

  1. cost side (#727 accepted ~5 % on HSET of a new document into an index
     of vectors alone, and ~31 B a document): `idx_mem` (FLAT, FLOAT32,
     DIM 2, prefix mem:) and 200,000 documents: used_memory growth per
     document; then FT.DROPINDEX idx_mem DD. `idx_hv` (the same, prefix hv:):
     redis-benchmark -c 8 of 300,000 HSET hv:<rand> v <8 bytes>, a second.
  2. DEBUG POPULATE 5,400,000 other keys (key:N), as in #590.
  3. `idx_text` (TEXT, prefix doc:) and `idx_vec` (vectors alone, prefix
     vec:), 1,000 documents each.
  4. FT.INFO of each, 30 calls from one client: median ms.
  5. SETs a second (redis-benchmark -c 8 over 1M keys, for 10 s: on v1.2.2
     a fixed count takes many minutes) while 4 other processes send FT.INFO
     back to back (2 on each index), and the FT.INFO calls they got through.

Gates: FT.INFO median within 50 % or 0.5 ms, keeping 60 % of the gain; SET
rate beside the pollers within 20 % (it shares 4 vCPUs with 4 pollers and
redis-benchmark), with no ratio: v1.2.2 manages 30-115 SETs a second here, too
noisy a denominator; the costs within 5 % (or 4 B) and 15 %.
"""
import multiprocessing as mp
import os
import re
import subprocess
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, KEEP_IMPROVEMENT, rel, info, scaled, median  # noqa: E402
from nodes import redis_tool, redis_benchmark_for  # noqa: E402

S = Suite("08-ft-info", [Config("ftinfo", engines=("v1_2_2", "candidate"))])
VEC = b"\x00\x00\x80\x3f\x00\x00\x00\x40"  # [1.0, 2.0] as FLOAT32


def vec_index(c, name, prefix):
    c.execute_command("FT.CREATE", name, "ON", "HASH", "PREFIX", "1", prefix, "SCHEMA",
                      "v", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2", "DISTANCE_METRIC", "L2")


def num_docs(c, name):
    r = c.execute_command("FT.INFO", name)
    d = dict(zip(r[::2], r[1::2]))
    return int(d.get(b"num_docs", d.get("num_docs")))


def rb(port, args):
    out = subprocess.run([redis_tool("redis-benchmark"), "-p", str(port), "-q"] + args,
                         capture_output=True, text=True, timeout=900).stdout
    return float(re.findall(r"([\d.]+) requests per second", out.replace("\r", "\n"))[-1])


def poller(port, names, stop, q):
    import redis
    c = redis.Redis(port=port)
    n = 0
    while not stop.is_set():
        c.execute_command("FT.INFO", names[n % len(names)])
        n += 1
    q.put(n)


@S.run
def run(ctx):
    c = ctx.client()
    c.flushall()
    # 1. costs
    vec_index(c, "idx_mem", "mem:")
    time.sleep(0.5)
    u0 = c.info("memory")["used_memory"]
    n = scaled(200_000)
    p = c.pipeline(transaction=False)
    for i in range(n):
        p.hset("mem:%d" % i, "v", VEC)
        if i % 1000 == 999:
            p.execute()
    p.execute()
    assert num_docs(c, "idx_mem") == n
    ctx.emit("vec_doc_bytes", (c.info("memory")["used_memory"] - u0) / float(n), "B", "lower",
             gate=rel(5, abs=4))
    c.execute_command("FT.DROPINDEX", "idx_mem", "DD")
    vec_index(c, "idx_hv", "hv:")
    hs = [rb(ctx.port, ["-c", "8", "-n", str(scaled(300_000)), "-r", "1000000",
                        "HSET", "hv:__rand_int__", "v", "aaaaaaaa"]) for _ in range(3)]
    ctx.emit("hset_vec_doc_rps", median(hs), "req/s", "higher", gate=rel(15), samples=hs)
    c.flushall()
    time.sleep(1)
    # 2. other keys
    t = time.perf_counter()
    other = scaled(5_400_000)
    c.execute_command("DEBUG", "POPULATE", other)
    ctx.emit("populate_s", time.perf_counter() - t, "s", "lower", gate=info())
    # 3. two small indexes
    c.execute_command("FT.CREATE", "idx_text", "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA", "t", "TEXT")
    vec_index(c, "idx_vec", "vec:")
    p = c.pipeline(transaction=False)
    for i in range(1000):
        p.hset("doc:%d" % i, "t", "hello world %d" % i)
        p.hset("vec:%d" % i, "v", VEC)
    p.execute()
    for _ in range(600):
        if num_docs(c, "idx_text") == 1000 and num_docs(c, "idx_vec") == 1000:
            break
        time.sleep(0.1)
    assert num_docs(c, "idx_text") == 1000 and num_docs(c, "idx_vec") == 1000
    assert c.dbsize() == other + 2000, c.dbsize()
    # 4. FT.INFO latency
    for name in ("idx_text", "idx_vec"):
        xs = []
        for _ in range(30):
            t = time.perf_counter()
            c.execute_command("FT.INFO", name)
            xs.append((time.perf_counter() - t) * 1000)
        ctx.emit("ft_info_%s_ms" % name[4:], median(xs), "ms", "lower",
                 gate=[rel(50, abs=0.5), KEEP_IMPROVEMENT], samples=xs)
    # 5. SETs beside 4 pollers
    mpc = mp.get_context("fork")
    stop, q = mpc.Event(), mpc.Queue()
    procs = [mpc.Process(target=poller, args=(ctx.port, names, stop, q), daemon=True)
             for names in (["idx_text"], ["idx_text"], ["idx_vec"], ["idx_vec"])]
    for pr in procs:
        pr.start()
    time.sleep(0.5)
    t = time.perf_counter()
    rate = redis_benchmark_for(ctx.port, 10, ["-c", "8", "-r", "1000000", "SET", "key:__rand_int__", "x"])
    secs = time.perf_counter() - t
    stop.set()
    polls = sum(q.get(timeout=30) for _ in procs)
    for pr in procs:
        pr.join(5)
    ctx.emit("set_rps_beside_ft_info", rate, "req/s", "higher", gate=rel(20))
    ctx.emit("ft_info_polls_per_s", polls / secs, "calls/s", "higher", gate=info())


if __name__ == "__main__":
    sys.exit(S.main())
