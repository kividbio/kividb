#!/usr/bin/env python3
"""Large sorted sets on CollectionTable (#875's storage half, #825, #770):
the measurements of the PR, against one running server (KiviDB master, the
branch, or Redis).

    bench.py <test> --port P [--pid PID] [--out results.jsonl]

Tests (each prints one JSON object, and appends it to --out):

  mem       one sorted set of N members (1,000, 10,000, 3,000,000) of L bytes
            (8, 16, 32), loaded by ZADDs of 1,000 pairs, pipelined: in order
            (member i at score i), at random (members in a random order, each
            at a random score), and the random one again after a reload
            (a load gives the members in order; KiviDB has no DEBUG RELOAD,
            so DUMP, DEL and RESTORE, which loads it as a snapshot does).
            used_memory and
            used_memory_rss each added, once settled, in bytes a member, and
            MEMORY USAGE
  mastodon  #825's shape: 20,000 sorted sets of 800 members each, a member
            an 18-digit status id and its score that id (as Mastodon's home
            feeds), loaded pipelined: bytes a member, used and RSS
  shrink    one set of 3,000,000 members (16 bytes, at random) taken down to
            1,000 by ZREM (1,000 members a command) and, freshly loaded, by
            one ZREMRANGEBYRANK: used_memory and RSS kept, the command's time
  fill      the worst single ZADD while one set is filled to 3M members, one
            member a command, one connection (redis-benchmark -c 1 -P 1,
            random scores and members), and PING latency from a second
            connection meanwhile (p50, p99, p99.9, max)
  zscan     a full ZSCAN of a 1M-member set at COUNT 10, 100 and 1000: calls,
            members returned (and distinct), wall time, server CPU
  digest    DEBUG DIGEST-VALUE of a set of 1,000,000 known members and of a
            small one (needs --enable-debug-command yes): to compare servers

--pid is the server's process, for its CPU time (/proc/<pid>/stat).
"""
import argparse
import json
import multiprocessing as mp
import os
import random
import socket
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


def member(i, length):
    """Member i, exactly `length` bytes: `m` and i zero-padded."""
    return "m%0*d" % (length - 1, i)


def zadd_pairs(c, key, pairs, batch=1000):
    """ZADD of (score, member) pairs, `batch` a command, pipelined."""
    p = c.pipeline(transaction=False)
    pending = 0
    for i in range(0, len(pairs), batch):
        args = []
        for score, m in pairs[i:i + batch]:
            args += [score, m]
        p.execute_command("ZADD", key, *args)
        pending += 1
        if pending >= 20:
            p.execute()
            pending = 0
    p.execute()


def settled(c, limit=60.0):
    """used_memory and used_memory_rss once used_memory has stopped falling
    for 2 s (a resize finished in the background gives its old array back
    at its end)."""
    low = c.info("memory")["used_memory"]
    end = time.perf_counter() + limit
    since = time.perf_counter()
    while time.perf_counter() < end and time.perf_counter() - since < 2.0:
        time.sleep(0.1)
        now = c.info("memory")["used_memory"]
        if now < low - 4096:
            since = time.perf_counter()
        low = min(low, now)
    return low, c.info("memory")["used_memory_rss"]


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


def per_member(before, after, n):
    return {"used_b": round((after[0] - before[0]) / n, 2),
            "rss_b": round((after[1] - before[1]) / n, 2),
            "used_mib": round((after[0] - before[0]) / 2**20, 2)}


def t_mem(c, a):
    res = {}
    rng = random.Random(825)
    # A first load of a fresh server pays for things made once (the
    # keyspace's table, buffers, the entry pool's first slabs), which would
    # be charged to the first row: one set loaded and dropped first.
    zadd_pairs(c, "warm", [(i, member(i, 16)) for i in range(20_000)])
    c.flushall()
    for n in (1_000, 10_000, 3_000_000):
        for length in (8, 16, 32):
            row = {}
            for order in ("in_order", "random"):
                c.flushall()
                time.sleep(1)
                u0 = settled(c)
                if order == "in_order":
                    pairs = [(i, member(i, length)) for i in range(n)]
                else:
                    ids = list(range(n))
                    rng.shuffle(ids)
                    pairs = [(rng.random() * 1e6, member(i, length)) for i in ids]
                zadd_pairs(c, "z", pairs)
                del pairs
                u1 = settled(c)
                assert c.zcard("z") == n
                row[order] = dict(per_member(u0, u1, n), memory_usage=c.memory_usage("z"))
                if order == "random":
                    # Built again from its serialized form, in order, as a
                    # load builds it: KiviDB has no DEBUG RELOAD, and
                    # RESTORE goes through the same loader as a snapshot
                    # (and Redis's through rdbLoadObject, as its load).
                    dump = c.dump("z")
                    c.delete("z")
                    settled(c)
                    c.restore("z", 0, dump)
                    del dump
                    u2 = settled(c)
                    assert c.zcard("z") == n
                    row["reloaded"] = dict(per_member(u0, u2, n),
                                           memory_usage=c.memory_usage("z"))
            res["n%d_len%d" % (n, length)] = row
    c.flushall()
    return res


def t_mastodon(c, a):
    keys, per = 20_000, 800
    rng = random.Random(825)
    c.flushall()
    time.sleep(1)
    u0 = settled(c)
    p = c.pipeline(transaction=False)
    base = 109_500_000_000_000_000
    for k in range(keys):
        ids = sorted(base + rng.randrange(10**15) for _ in range(per))
        args = []
        for i in ids:
            args += [i, i]
        p.execute_command("ZADD", "feed:home:%d" % k, *args)
        if k % 50 == 49:
            p.execute()
    p.execute()
    u1 = settled(c)
    total = sum(c.zcard("feed:home:%d" % k) for k in range(0, keys, 1000)) * 1000
    return dict(per_member(u0, u1, keys * per), sampled_members=total,
                encoding=c.object("encoding", "feed:home:0"),
                memory_usage_one=c.memory_usage("feed:home:0"))


def t_shrink(c, a):
    res = {}
    n, keep = 3_000_000, 1_000
    rng = random.Random(77)
    for how in ("zrem", "zremrangebyrank"):
        c.flushall()
        time.sleep(1)
        u0 = settled(c)
        ids = list(range(n))
        rng.shuffle(ids)
        zadd_pairs(c, "z", [(rng.random() * 1e6, member(i, 16)) for i in ids])
        del ids
        full = settled(c)
        t = time.perf_counter()
        if how == "zrem":
            gone = [member(i, 16) for i in range(keep, n)]
            p = c.pipeline(transaction=False)
            for i in range(0, len(gone), 1000):
                p.execute_command("ZREM", "z", *gone[i:i + 1000])
                if i % 20000 == 0:
                    p.execute()
            p.execute()
            del gone
        else:
            c.execute_command("ZREMRANGEBYRANK", "z", 0, n - keep - 1)
        took = time.perf_counter() - t
        after = settled(c)
        assert c.zcard("z") == keep
        res[how] = {"full_used_mib": round((full[0] - u0[0]) / 2**20, 2),
                    "kept_used_mib": round((after[0] - u0[0]) / 2**20, 3),
                    "kept_rss_mib": round((after[1] - u0[1]) / 2**20, 2),
                    "memory_usage": c.memory_usage("z"),
                    "command_s": round(took, 3)}
    c.flushall()
    return res


def t_fill(c, a):
    c.flushall()
    n = 3_000_000
    pings = Pings(a.port)
    t = time.perf_counter()
    out = subprocess.run([BENCH, "-p", str(a.port), "-c", "1", "-P", "1", "-n", str(n),
                          "-r", "1000000000", "--precision", "3", "ZADD", "z", "__rand_int__",
                          "m:__rand_int__"],
                         capture_output=True, text=True).stdout
    secs = time.perf_counter() - t
    res = pings.done()
    lines = out.splitlines()
    for i, line in enumerate(lines):
        if "latency summary" in line and i + 2 < len(lines):
            names = ["zadd_avg_ms", "zadd_min_ms", "zadd_p50_ms", "zadd_p95_ms", "zadd_p99_ms",
                     "zadd_max_ms"]
            res.update({k: float(v) for k, v in zip(names, lines[i + 2].split())})
    res.update({"members": c.zcard("z"), "fill_s": round(secs, 2)})
    return res


def t_zscan(c, a):
    c.flushall()
    n = 1_000_000
    zadd_pairs(c, "z", [(i, member(i, 16)) for i in range(n)])
    time.sleep(2)
    res = {}
    for count in (10, 100, 1000):
        cursor, calls, got, seen = 0, 0, 0, set()
        c0 = cpu_s(a.pid)
        t = time.perf_counter()
        while True:
            cursor, page = c.zscan("z", cursor, count=count)
            calls += 1
            got += len(page)
            seen.update(m for m, _ in page)
            if cursor == 0:
                break
        wall = time.perf_counter() - t
        c1 = cpu_s(a.pid)
        res["count_%d" % count] = {
            "calls": calls, "returned": got, "distinct": len(seen), "wall_s": round(wall, 3),
            "server_cpu_s": None if c0 is None else round(c1 - c0, 3)}
        assert len(seen) == n, "ZSCAN missed members: %d of %d" % (len(seen), n)
    return res


def t_digest(c, a):
    c.flushall()
    rng = random.Random(5)
    zadd_pairs(c, "z", [(rng.random() * 1e6, member(i, 16)) for i in range(1_000_000)])
    c.zadd("small", {"a": 1, "b": 2.5})
    return {"digest_value_z": c.execute_command("DEBUG", "DIGEST-VALUE", "z"),
            "digest_value_small": c.execute_command("DEBUG", "DIGEST-VALUE", "small")}


TESTS = {"mem": t_mem, "mastodon": t_mastodon, "shrink": t_shrink, "fill": t_fill,
         "zscan": t_zscan, "digest": t_digest}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("test", choices=sorted(TESTS))
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--pid", type=int)
    ap.add_argument("--label", default="")
    ap.add_argument("--out")
    a = ap.parse_args()
    c = redis.Redis(port=a.port, decode_responses=True, socket_timeout=1200)
    res = {"test": a.test, "label": a.label, **TESTS[a.test](c, a)}
    line = json.dumps(res, default=str)
    print(line)
    if a.out:
        with open(a.out, "a") as f:
            f.write(line + "\n")


if __name__ == "__main__":
    sys.exit(main())
