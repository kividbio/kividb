#!/usr/bin/env python3
"""Full key/value comparison of two servers (a master and its replica).

Scans every key on A, reads its type and value on both (type-aware, order-
insensitive where the type is unordered) plus whether it has a time to live,
and does the same for keys only B has. Keys that differ are read again after
a pause, so that a write still in flight does not count; a key that is
missing on one side and expires within a few seconds on the other is counted
as "expiring", not as a difference. rc8 soak: a hash that still differs is read field by field,
and counts as "hash_field_expiring" if every differing field has a field TTL under 5 s (or has just expired).

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
    # rc8 soak: a key in a slot this node does not own (a stale copy a failed `--cluster fix` left behind) answers
    # MOVED; it is recorded as type "not-owned" instead of failing the whole compare
    tt = p.execute(raise_on_error=False)
    tt = ["not-owned" if isinstance(x, redis.exceptions.MovedError) else x for x in tt]
    for x in tt:
        if isinstance(x, Exception):
            raise x
    types = [x.decode() if isinstance(x, bytes) else x for x in tt[0::2]]
    ttls = [None if x == "not-owned" else x for x in tt[1::2]]
    p = c.pipeline(transaction=False)
    for k, t in zip(keys, types):
        if t == "not-owned":
            p.echo("not-owned")
        elif t == "string":
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
        out[k] = (t, canon(v, t) if t not in ("none", "not-owned") else None, ttl)
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


def field_check(a, b, k):
    """rc8 soak: the hash-field TTL allowance. A hash that still differs after the recheck is read field by field.
    "field_expiring": every field that differs exists on one side only (or differs) and has a field TTL under 5 s
    there, or has just expired (HPTTL -2). "settled": the two sides now agree (reported separately, not hidden).
    Anything else is "real"."""
    fa, fb = a.hgetall(k), b.hgetall(k)
    diff = [f for f in set(fa) | set(fb) if fa.get(f) != fb.get(f)]
    if not diff:
        return "settled"
    for f in diff:
        for side, fs in ((a, fa), (b, fb)):
            if f in fs:
                t = side.execute_command("HPTTL", k, "FIELDS", 1, f)
                t = t[0] if isinstance(t, list) else t
                if not (0 <= t < 5000 or t == -2):
                    return "real"
    return "field_expiring"


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
    not_owned = (sum(1 for v in da.values() if v[0] == "not-owned"), sum(1 for v in db.values() if v[0] == "not-owned"))
    time.sleep(3)
    bad2, da2, db2 = diff_keys(a, b, bad) if bad else ([], {}, {})
    expiring = 0
    real = []
    field_expiring, settled = 0, 0
    for k in bad2:
        ta, va, la = da2[k]
        tb, vb, lb = db2[k]
        if (ta == "none" and 0 <= lb < 5000) or (tb == "none" and 0 <= la < 5000):
            expiring += 1
        elif ta == "hash" and tb == "hash":
            r = field_check(a, b, k)
            if r == "field_expiring":
                field_expiring += 1
            elif r == "settled":
                settled += 1
            else:
                real.append(k)
        else:
            real.append(k)
    res = {
        "dbsize_a": a.dbsize(), "dbsize_b": b.dbsize(),
        "scanned_a": len(ka), "scanned_b": len(kb), "compared": len(union),
        "differ_first_pass": first, "differ_after_recheck": len(bad2),
        "expiring_at_compare": expiring, "hash_field_expiring": field_expiring,
        "hash_settled_on_field_recheck": settled, "not_owned_keys_a_b": not_owned, "real_differences": len(real),
        "sample": [k.decode(errors="replace") for k in real[:20]],
        "seconds": round(time.time() - t0, 1),
    }
    print(json.dumps(res))
    if args.out:
        json.dump(res, open(args.out, "w"), indent=1)
    sys.exit(1 if real else 0)


if __name__ == "__main__":
    main()
