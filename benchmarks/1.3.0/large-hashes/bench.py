#!/usr/bin/env python3
"""Large hashes on CollectionTable (#764 step 3): the measurements of the PR,
against one running server (KiviDB master, the branch, or Redis).

    bench.py <test> --port P [--pid PID] [--out results.jsonl]

Tests (each prints one JSON object, and appends it to --out):

  fill      the worst single HSET while one hash is filled to 3M fields, one
            field a command, one connection (redis-benchmark -c 1 -P 1), and
            PING latency from a second connection meanwhile (p50, p99, p99.9,
            max), in a process of its own
  mem764    #764's load: one hash of 3,000,000 fields f0..f2999999, each = v,
            loaded pipelined: used_memory it added once settled, bytes a
            field and MiB; and the same at 3,211,264 and 3,211,265 fields
            (just before and just after the table doubles)
  ttl       one hash of 1,000,000 fields (field:<i>, 32-byte values): bytes a
            field with no field TTL, and with every field given one
            (HPEXPIREAT far off)
  hscan     a full HSCAN of a 1M-field hash at COUNT 10, 100 and 1000:
            calls, fields returned (and distinct), wall time, server CPU
  rand      HRANDFIELD h <count> on a 1M-field hash, count 10, 1,000 and
            500,000: median of 5
  copy      COPY of a 1M-field hash: time and the used_memory it adds
  free      a 3M-field hash removed by DEL and UNLINK: the command's time and
            the worst PING from a second connection
  background  a hash filled (pipelined) to 3,211,265 fields, then left
            alone: MEMORY USAGE until it stops falling
  digest    DEBUG DIGEST-VALUE of a hash of 1,000,000 known fields (needs
            --enable-debug-command yes): to compare servers
  ingest    FT.CREATE on HASH then 200,000 documents by HSET (pipelined):
            small ones (listpack) and ones with a 100-byte body (a table);
            seconds, until FT.INFO says indexing is done. KiviDB only

--pid is the server's process, for its CPU time (/proc/<pid>/stat).
"""
import argparse
import json
import multiprocessing as mp
import os
import socket
import statistics
import subprocess
import sys
import time

import redis

BENCH = os.environ.get("REDIS_BENCHMARK", "redis-benchmark")
TICK = os.sysconf("SC_CLK_TCK")


def cpu_s(pid):
    if not pid:
        return None
    with open("/proc/%d/stat" % pid) as f:
        parts = f.read().rsplit(")", 1)[1].split()
    return (int(parts[11]) + int(parts[12])) / TICK


def load(c, key, n, name=lambda i: "field:%08d" % i, value=lambda i: "value:%026d" % i,
         start=0, batch=1000):
    p = c.pipeline(transaction=False)
    pending = 0
    for i in range(start, start + n, batch):
        hi = min(start + n, i + batch)
        args = []
        for j in range(i, hi):
            args += [name(j), value(j)]
        p.execute_command("HSET", key, *args)
        pending += 1
        if pending >= 20:
            p.execute()
            pending = 0
    p.execute()


def settled(c, limit=60.0):
    """used_memory once it has stopped falling for 2 s (a resize finished in
    the background gives its old array back at its end)."""
    low = c.info("memory")["used_memory"]
    end = time.perf_counter() + limit
    since = time.perf_counter()
    while time.perf_counter() < end and time.perf_counter() - since < 2.0:
        time.sleep(0.1)
        now = c.info("memory")["used_memory"]
        if now < low - 4096:
            since = time.perf_counter()
        low = min(low, now)
    return low


def pinger(port, stop, q):
    s = socket.create_connection(("127.0.0.1", port))
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    lat = []
    while not stop.is_set():
        t = time.perf_counter_ns()
        s.sendall(b"*1\r\n$4\r\nPING\r\n")
        got = b""
        while not got.endswith(b"\r\n"):
            got += s.recv(64)
        lat.append((time.perf_counter_ns() - t) / 1000.0)
    lat.sort()
    q.put(lat)


class Pings:
    def __init__(self, port):
        self.stop = mp.Event()
        self.q = mp.Queue()
        self.p = mp.Process(target=pinger, args=(port, self.stop, self.q))
        self.p.start()
        time.sleep(0.3)

    def done(self):
        self.stop.set()
        lat = self.q.get()
        self.p.join()
        if not lat:
            return {}

        def pct(p):
            return round(lat[min(len(lat) - 1, int(len(lat) * p))] / 1000.0, 3)
        return {"pings": len(lat), "ping_p50_ms": pct(0.5), "ping_p99_ms": pct(0.99),
                "ping_p999_ms": pct(0.999), "ping_max_ms": round(lat[-1] / 1000.0, 3)}


def t_fill(c, a):
    c.flushall()
    n = 3_000_000
    pings = Pings(a.port)
    t = time.perf_counter()
    out = subprocess.run([BENCH, "-p", str(a.port), "-c", "1", "-P", "1", "-n", str(n),
                          "-r", "1000000000", "--precision", "3", "HSET", "h", "f:__rand_int__", "v"],
                         capture_output=True, text=True).stdout
    secs = time.perf_counter() - t
    res = pings.done()
    lines = out.splitlines()
    for i, line in enumerate(lines):
        if "latency summary" in line and i + 2 < len(lines):
            names = ["hset_avg_ms", "hset_min_ms", "hset_p50_ms", "hset_p95_ms", "hset_p99_ms",
                     "hset_max_ms"]
            res.update({k: float(v) for k, v in zip(names, lines[i + 2].split())})
    res.update({"fields": c.hlen("h"), "fill_s": round(secs, 2)})
    return res


def t_mem764(c, a):
    res = {}
    for n in (3_000_000, 3_211_264, 3_211_265):
        c.flushall()
        time.sleep(1)
        u0 = settled(c)
        load(c, "h", n, name=lambda i: "f%d" % i, value=lambda i: "v")
        u1 = settled(c)
        assert c.hlen("h") == n
        res["n%d" % n] = {"bytes_per_field": round((u1 - u0) / n, 2),
                          "mib": round((u1 - u0) / 2**20, 1),
                          "memory_usage": c.memory_usage("h")}
    c.flushall()
    return res


def t_ttl(c, a):
    res = {}
    n = 1_000_000
    for ttl in (False, True):
        c.flushall()
        time.sleep(1)
        u0 = settled(c)
        load(c, "h", n)
        if ttl:
            p = c.pipeline(transaction=False)
            for i in range(0, n, 1000):
                names = ["field:%08d" % j for j in range(i, i + 1000)]
                p.execute_command("HPEXPIREAT", "h", 4_000_000_000_000, "FIELDS", len(names), *names)
                if i % 20000 == 0:
                    p.execute()
            p.execute()
        u1 = settled(c)
        res["ttl" if ttl else "plain"] = {"bytes_per_field": round((u1 - u0) / n, 2),
                                          "memory_usage": c.memory_usage("h")}
    c.flushall()
    return res


def t_hscan(c, a):
    c.flushall()
    n = 1_000_000
    load(c, "h", n)
    time.sleep(2)
    res = {}
    for count in (10, 100, 1000):
        cursor, calls, got, seen = 0, 0, 0, set()
        c0 = cpu_s(a.pid)
        t = time.perf_counter()
        while True:
            cursor, page = c.hscan("h", cursor, count=count)
            calls += 1
            got += len(page)
            seen.update(page)
            if cursor == 0:
                break
        wall = time.perf_counter() - t
        c1 = cpu_s(a.pid)
        res["count_%d" % count] = {
            "calls": calls, "returned": got, "distinct": len(seen), "wall_s": round(wall, 3),
            "server_cpu_s": None if c0 is None else round(c1 - c0, 3)}
        assert len(seen) == n, "HSCAN missed fields: %d of %d" % (len(seen), n)
    return res


def timed(f, reps=5):
    times = []
    for _ in range(reps):
        t = time.perf_counter()
        f()
        times.append(time.perf_counter() - t)
    return round(statistics.median(times) * 1000, 3)


def t_rand(c, a):
    c.flushall()
    load(c, "h", 1_000_000)
    time.sleep(2)
    res = {}
    for count in (10, 1000, 500_000):
        res["hrandfield_%d_ms" % count] = timed(lambda: c.hrandfield("h", count))
        res["hrandfield_neg_%d_ms" % count] = timed(lambda: c.hrandfield("h", -count))
    return res


def used(c):
    time.sleep(1.0)
    return settled(c)


def t_copy(c, a):
    c.flushall()
    load(c, "h", 1_000_000)
    u0 = used(c)
    t = time.perf_counter()
    c.execute_command("COPY", "h", "c")
    copy_ms = (time.perf_counter() - t) * 1000
    u1 = used(c)
    return {"copy_ms": round(copy_ms, 2), "copy_bytes_per_field": round((u1 - u0) / 1e6, 2)}


def t_free(c, a):
    res = {}
    for how in ("DEL", "UNLINK"):
        c.flushall()
        load(c, "big", 3_000_000)
        time.sleep(2)
        pings = Pings(a.port)
        t = time.perf_counter()
        if how == "DEL":
            c.delete("big")
        else:
            c.unlink("big")
        took = (time.perf_counter() - t) * 1000
        time.sleep(1.0)
        res[how.lower()] = dict(command_ms=round(took, 2), **pings.done())
    return res


def t_background(c, a):
    c.flushall()
    load(c, "h", 3_211_265)
    t = time.perf_counter()
    first = c.memory_usage("h")
    samples = []
    while time.perf_counter() - t < 60:
        time.sleep(0.05)
        samples.append(c.memory_usage("h"))
        if len(samples) >= 10 and len(set(samples[-10:])) == 1:
            break
    return {"fields": c.hlen("h"), "memory_usage_first": first, "memory_usage_settled": samples[-1],
            "settled_after_s": round(time.perf_counter() - t - 0.5, 2)}


def t_digest(c, a):
    c.flushall()
    load(c, "h", 1_000_000)
    c.hset("small", mapping={"a": "1", "b": "2"})
    return {"digest_value_h": c.execute_command("DEBUG", "DIGEST-VALUE", "h"),
            "digest_value_small": c.execute_command("DEBUG", "DIGEST-VALUE", "small")}


def t_ingest(c, a):
    res = {}
    n = 200_000
    for kind, body in (("small", ""), ("table", "b" * 100)):
        c.flushall()
        time.sleep(1)
        try:
            c.execute_command("FT.CREATE", "idx", "ON", "HASH", "PREFIX", 1, "doc:", "SCHEMA",
                              "title", "TEXT", "tag", "TAG", "n", "NUMERIC", "body", "TEXT")
        except redis.ResponseError as e:
            return {"skipped": str(e)}
        t = time.perf_counter()
        p = c.pipeline(transaction=False)
        for i in range(n):
            args = ["title", "word%d other%d" % (i % 1000, i % 37), "tag", "t%d" % (i % 50),
                    "n", i]
            if body:
                args += ["body", body]
            p.execute_command("HSET", "doc:%d" % i, *args)
            if i % 1000 == 999:
                p.execute()
        p.execute()
        while True:
            info = c.execute_command("FT.INFO", "idx")
            d = info if isinstance(info, dict) else dict(zip(info[::2], info[1::2]))
            if str(d.get("indexing", 0)) == "0":
                break
            time.sleep(0.05)
        secs = time.perf_counter() - t
        hits = c.execute_command("FT.SEARCH", "idx", "@tag:{t7}", "LIMIT", 0, 0)
        hits = hits.get("total_results") if isinstance(hits, dict) else hits[0]
        res[kind] = {"seconds": round(secs, 3), "docs_per_s": round(n / secs), "tag_hits": hits,
                     "encoding": c.object("encoding", "doc:1")}
    return res


TESTS = {"fill": t_fill, "mem764": t_mem764, "ttl": t_ttl, "hscan": t_hscan, "rand": t_rand,
         "copy": t_copy, "free": t_free, "background": t_background, "digest": t_digest,
         "ingest": t_ingest}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("test", choices=sorted(TESTS))
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--pid", type=int)
    ap.add_argument("--label", default="")
    ap.add_argument("--out")
    a = ap.parse_args()
    c = redis.Redis(port=a.port, decode_responses=True, socket_timeout=600)
    res = {"test": a.test, "label": a.label, **TESTS[a.test](c, a)}
    line = json.dumps(res, default=str)
    print(line)
    if a.out:
        with open(a.out, "a") as f:
            f.write(line + "\n")


if __name__ == "__main__":
    sys.exit(main())
