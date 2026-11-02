#!/usr/bin/env python3
"""WAIT under concurrent writers: how often WAIT 1 <timeout> answers 0, and how long it takes.

  waitprobe.py host:port [--writers 16] [--seconds 20] [--timeout-ms 200]
"""
import argparse
import collections
import threading
import time

import redis

ap = argparse.ArgumentParser()
ap.add_argument("addr")
ap.add_argument("--writers", type=int, default=16)
ap.add_argument("--seconds", type=float, default=20)
ap.add_argument("--timeout-ms", type=int, default=200)
a = ap.parse_args()
h, p = a.addr.rsplit(":", 1)
res = collections.Counter()
lat = {0: [], 1: []}
lock = threading.Lock()
stop = time.time() + a.seconds


def w(i):
    c = redis.Redis(host=h, port=int(p))
    n = 0
    while time.time() < stop:
        c.set("wp:%d:%d" % (i, n), "x" * 32)
        s = time.perf_counter()
        r = int(c.execute_command("WAIT", 1, a.timeout_ms))
        d = (time.perf_counter() - s) * 1000
        with lock:
            res[min(r, 1)] += 1
            lat[min(r, 1)].append(d)
        n += 1


ts = [threading.Thread(target=w, args=(i,)) for i in range(a.writers)]
[t.start() for t in ts]
[t.join() for t in ts]
for k in (0, 1):
    v = sorted(lat[k])
    if v:
        print("WAIT ->%d: %d times, ms p50 %.2f p99 %.2f max %.2f" % (k, len(v), v[len(v) // 2], v[int(len(v) * .99)], v[-1]))
