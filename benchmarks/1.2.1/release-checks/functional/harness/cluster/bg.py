#!/usr/bin/env python3
"""bg.py: background mix for the cluster tests (replaces redis-benchmark --cluster, which puts all of a node's keys
in ONE slot through a hash tag, so one slot held ~400k keys). Keys spread over all slots:
SET bg:<n> (100 B), INCR bgc:<n>, HSET bgh:<n%20000> f<n%50>, LPUSH+LTRIM bgl:<n%5000> 100. Cluster pipelines of 20.
  bg.py --seeds h:p,... --procs 4 --stop-file F
  bg.py clean --seeds ...   (delete redis-benchmark's key:{..}, counter:{..}, mylist:{..}, myhash:{..} keys)
"""
import argparse, os, random, sys, time
from multiprocessing import Process
import redis
from redis.cluster import RedisCluster, ClusterNode


def cluster(seeds, t=2):
    nodes = [ClusterNode(h, int(p)) for h, p in (s.rsplit(":", 1) for s in seeds.split(","))]
    return RedisCluster(startup_nodes=nodes, socket_timeout=t, socket_connect_timeout=t, require_full_coverage=False)


def run(i, a):
    random.seed(1000 + i)
    rc = None
    val = b"v" * 100
    while not os.path.exists(a.stop_file):
        try:
            if rc is None:
                rc = cluster(a.seeds)
            p = rc.pipeline()
            for _ in range(20):
                n = random.randrange(a.keys)
                op = n % 4
                if op == 0:
                    p.set(b"bg:%d" % n, val)
                elif op == 1:
                    p.incr(b"bgc:%d" % n)
                elif op == 2:
                    p.hset(b"bgh:%d" % (n % 20000), b"f%d" % (n % 50), val)
                else:
                    k = b"bgl:%d" % (n % 5000)
                    p.lpush(k, val)
                    p.ltrim(k, 0, 99)
            p.execute(raise_on_error=False)
            if a.sleep:
                time.sleep(a.sleep)
        except Exception:
            rc = None
            time.sleep(0.2)


def clean(a):
    import re
    nodes = cluster(a.seeds, 30).get_primaries()
    tot = 0
    for n in nodes:
        c = redis.Redis(host=n.host, port=n.port, socket_timeout=60)
        for pat in (b"key:{*", b"counter:{*", b"mylist:{*", b"myhash:{*"):
            cur = 0
            while True:
                cur, ks = c.scan(cur, match=pat, count=5000)
                if ks:
                    p = c.pipeline(transaction=False)
                    for k in ks:
                        p.unlink(k)
                    p.execute()
                    tot += len(ks)
                if cur == 0:
                    break
        print(n.name, "dbsize now", c.dbsize(), flush=True)
    print("deleted", tot)


ap = argparse.ArgumentParser()
ap.add_argument("cmd", nargs="?", default="run")
ap.add_argument("--seeds")
ap.add_argument("--procs", type=int, default=4)
ap.add_argument("--keys", type=int, default=400000)
ap.add_argument("--sleep", type=float, default=0.0)
ap.add_argument("--stop-file")
a = ap.parse_args()
if a.cmd == "clean":
    clean(a)
else:
    ps = [Process(target=run, args=(i, a)) for i in range(a.procs)]
    [p.start() for p in ps]
    [p.join() for p in ps]
