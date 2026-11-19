#!/usr/bin/env python3
"""Large sets on CollectionTable (#764 step 2): the measurements of the PR,
against one running server (KiviDB master, the branch, or Redis).

    bench.py <test> --port P [--pid PID] [--out results.json]

Tests (each prints one JSON object, and appends it to --out):

  fill      the worst single SADD while one set is filled to 3M members, one
            member a command, one connection (redis-benchmark -c 1 -P 1),
            and PING latency from a second connection meanwhile (p50, p99,
            p99.9, max), in a process of its own
  sscan     a full SSCAN of a 1M-member set at COUNT 10, 100 and 1000: calls,
            members returned (and distinct), wall time, server CPU
  rand      SRANDMEMBER s <count> and SPOP s <count> on a 1M-member set,
            count 10, 1,000 and 500,000: the command's time, median of 5
            (SPOP pops from a fresh COPY each time; the COPY is not timed)
  copy      COPY of a 1M-member set and SUNIONSTORE of it with a set of 10:
            the time and the used_memory the copy adds
  free      a 3M-member set removed by DEL, UNLINK, an expiry, eviction and
            FLUSHALL: the command's own time, and the worst PING from a
            second connection while it and the second after it run
  background  a set filled (pipelined) to 3,211,265 members, just past a
            doubling, then left alone: MEMORY USAGE until it stops falling
            (the background finishing the resize), and how long that took
  digest    DEBUG DIGEST-VALUE and DEBUG DIGEST of a set of 1,000,000 known
            members (needs --enable-debug-command yes): to compare servers

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


def member(i):
    return "membr:%08d" % i


def load(c, key, n, start=0, batch=1000):
    p = c.pipeline(transaction=False)
    pending = 0
    for i in range(start, start + n, batch):
        hi = min(start + n, i + batch)
        p.execute_command("SADD", key, *[member(j) for j in range(i, hi)])
        pending += 1
        if pending >= 20:
            p.execute()
            pending = 0
    p.execute()


def pinger(port, stop, q):
    """PING one at a time on a connection of its own until `stop`; every
    round trip in microseconds."""
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
                          "-r", "1000000000", "--precision", "3", "SADD", "s", "membr:__rand_int__"],
                         capture_output=True, text=True).stdout
    secs = time.perf_counter() - t
    res = pings.done()
    summary = None
    lines = out.splitlines()
    for i, line in enumerate(lines):
        if "latency summary" in line and i + 2 < len(lines):
            summary = lines[i + 2].split()
    if summary:
        names = ["sadd_avg_ms", "sadd_min_ms", "sadd_p50_ms", "sadd_p95_ms", "sadd_p99_ms", "sadd_max_ms"]
        res.update({k: float(v) for k, v in zip(names, summary)})
    res.update({"members": c.scard("s"), "fill_s": round(secs, 2)})
    return res


def t_sscan(c, a):
    c.flushall()
    n = 1_000_000
    load(c, "s", n)
    time.sleep(2)  # any resize the load left is finished
    res = {}
    for count in (10, 100, 1000):
        cursor, calls, got, seen = 0, 0, 0, set()
        c0 = cpu_s(a.pid)
        t = time.perf_counter()
        while True:
            cursor, page = c.sscan("s", cursor, count=count)
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
        assert len(seen) == n, "SSCAN missed members: %d of %d" % (len(seen), n)
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
    load(c, "s", 1_000_000)
    time.sleep(2)
    res = {}
    for count in (10, 1000, 500_000):
        res["srandmember_%d_ms" % count] = timed(lambda: c.srandmember("s", count))
        times = []
        for _ in range(5):
            c.delete("w")
            c.execute_command("COPY", "s", "w")
            time.sleep(0.5)
            t = time.perf_counter()
            c.spop("w", count)
            times.append(time.perf_counter() - t)
        res["spop_%d_ms" % count] = round(statistics.median(times) * 1000, 3)
    return res


def used(c):
    time.sleep(1.0)
    return c.info("memory")["used_memory"]


def t_copy(c, a):
    c.flushall()
    load(c, "s", 1_000_000)
    c.sadd("small", *[member(i * 7) for i in range(10)])
    u0 = used(c)
    t = time.perf_counter()
    c.execute_command("COPY", "s", "c")
    copy_ms = (time.perf_counter() - t) * 1000
    u1 = used(c)
    t = time.perf_counter()
    c.sunionstore("u", "s", "small")
    union_ms = (time.perf_counter() - t) * 1000
    u2 = used(c)
    return {"copy_ms": round(copy_ms, 2), "copy_bytes_per_member": round((u1 - u0) / 1e6, 2),
            "sunionstore_ms": round(union_ms, 2),
            "sunionstore_bytes_per_member": round((u2 - u1) / 1e6, 2)}


def t_free(c, a):
    res = {}
    for how in ("DEL", "UNLINK", "EXPIRE", "EVICT", "FLUSHALL"):
        c.config_set("maxmemory", 0)
        c.flushall()
        load(c, "big", 3_000_000)
        c.set("other", "x")
        time.sleep(2)
        pings = Pings(a.port)
        t = time.perf_counter()
        if how == "DEL":
            c.delete("big")
        elif how == "UNLINK":
            c.unlink("big")
        elif how == "EXPIRE":
            c.pexpire("big", 1)
            while c.exists("big"):
                time.sleep(0.001)
        elif how == "EVICT":
            c.config_set("maxmemory-policy", "allkeys-random")
            c.config_set("maxmemory", c.info("memory")["used_memory"] - 50_000_000)
            for i in range(200):
                c.set("filler:%d" % i, "y" * 100)
                if not c.exists("big"):
                    break
            assert not c.exists("big"), "the big set was not evicted"
        else:
            c.flushall()
        took = (time.perf_counter() - t) * 1000
        time.sleep(1.0)
        res[how.lower()] = dict(command_ms=round(took, 2), **pings.done())
        c.config_set("maxmemory", 0)
        c.config_set("maxmemory-policy", "noeviction")
    return res


def t_background(c, a):
    c.flushall()
    load(c, "s", 3_211_265)
    t = time.perf_counter()
    first = c.memory_usage("s")
    last, samples = first, []
    while time.perf_counter() - t < 60:
        time.sleep(0.05)
        now = c.memory_usage("s")
        samples.append(now)
        if len(samples) >= 10 and len(set(samples[-10:])) == 1:
            break
        last = now
    return {"members": c.scard("s"), "memory_usage_first": first, "memory_usage_settled": samples[-1],
            "settled_after_s": round(time.perf_counter() - t - 0.5, 2)}


def t_digest(c, a):
    c.flushall()
    load(c, "s", 1_000_000)
    return {"digest_value": c.execute_command("DEBUG", "DIGEST-VALUE", "s"),
            "digest": c.execute_command("DEBUG", "DIGEST")}


TESTS = {"fill": t_fill, "sscan": t_sscan, "rand": t_rand, "copy": t_copy, "free": t_free,
         "background": t_background, "digest": t_digest}


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
