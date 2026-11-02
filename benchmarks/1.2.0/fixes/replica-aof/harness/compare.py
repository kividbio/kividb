#!/usr/bin/env python3
"""Full key/value comparison of two servers (a master and its replica).

Scans every key on A, reads its type and value on both (type-aware, order-
insensitive where the type is unordered) plus whether it has a time to live,
and does the same for keys only B has. Keys that differ are read again after
a pause, so that a write still in flight does not count; a key that is
missing on one side and expires within a few seconds on the other is counted
as "expiring", not as a difference.

  compare.py A_host:port B_host:port [--out result.json]
"""
import argparse
import hashlib
import json
import sys
import time

import redis


def canon(pipe_results, kind):
    t, v = kind, pipe_results
    if v is None:
        return None
    if t == "string":
        return hashlib.sha1(v).hexdigest()
    if t == "hash":
        return hashlib.sha1(repr(sorted(v.items())).encode()).hexdigest()
    if t == "list":
        return hashlib.sha1(repr(v).encode()).hexdigest()
    if t == "set":
        return hashlib.sha1(repr(sorted(v)).encode()).hexdigest()
    if t == "zset":
        return hashlib.sha1(repr(sorted(v)).encode()).hexdigest()
    return repr(v)


def fetch(c, keys):
    p = c.pipeline(transaction=False)
    for k in keys:
        p.type(k)
        p.pttl(k)
    tt = p.execute()
    types = [x.decode() if isinstance(x, bytes) else x for x in tt[0::2]]
    ttls = tt[1::2]
    p = c.pipeline(transaction=False)
    for k, t in zip(keys, types):
        if t == "string":
            p.get(k)
        elif t == "hash":
            p.hgetall(k)
        elif t == "list":
            p.lrange(k, 0, -1)
        elif t == "set":
            p.smembers(k)
        elif t == "zset":
            p.zrange(k, 0, -1, withscores=True)
        else:
            p.exists(k)
    vals = p.execute()
    out = {}
    for k, t, ttl, v in zip(keys, types, ttls, vals):
        out[k] = (t, canon(v, t) if t != "none" else None, ttl)
    return out


def scan_all(c):
    keys = []
    cur = 0
    while True:
        cur, ks = c.scan(cur, count=5000)
        keys.extend(ks)
        if cur == 0:
            break
    return keys


def diff_keys(a, b, keys):
    da, db = {}, {}
    for i in range(0, len(keys), 2000):
        ch = keys[i:i + 2000]
        da.update(fetch(a, ch))
        db.update(fetch(b, ch))
    bad = []
    for k in keys:
        ta, va, la = da[k]
        tb, vb, lb = db[k]
        if (ta, va) != (tb, vb) or ((la == -1) != (lb == -1) and ta != "none" and tb != "none"):
            bad.append(k)
    return bad, da, db


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("a")
    ap.add_argument("b")
    ap.add_argument("--out")
    args = ap.parse_args()
    ha, pa = args.a.rsplit(":", 1)
    hb, pb = args.b.rsplit(":", 1)
    a = redis.Redis(host=ha, port=int(pa), socket_timeout=60)
    b = redis.Redis(host=hb, port=int(pb), socket_timeout=60)
    t0 = time.time()
    ka = scan_all(a)
    kb = scan_all(b)
    union = list(set(ka) | set(kb))
    bad, da, db = diff_keys(a, b, union)
    first = len(bad)
    time.sleep(3)
    bad2, da2, db2 = diff_keys(a, b, bad) if bad else ([], {}, {})
    expiring = 0
    real = []
    for k in bad2:
        ta, va, la = da2[k]
        tb, vb, lb = db2[k]
        if (ta == "none" and 0 <= lb < 5000) or (tb == "none" and 0 <= la < 5000):
            expiring += 1
        else:
            real.append(k)
    res = {
        "dbsize_a": a.dbsize(), "dbsize_b": b.dbsize(),
        "scanned_a": len(ka), "scanned_b": len(kb), "compared": len(union),
        "differ_first_pass": first, "differ_after_recheck": len(bad2),
        "expiring_at_compare": expiring, "real_differences": len(real),
        "sample": [k.decode(errors="replace") for k in real[:20]],
        "seconds": round(time.time() - t0, 1),
    }
    print(json.dumps(res))
    if args.out:
        json.dump(res, open(args.out, "w"), indent=1)
    sys.exit(1 if real else 0)


if __name__ == "__main__":
    main()
