#!/usr/bin/env python3
"""r121 replication: key-by-key comparison of two servers, every database, keys as bytes.

KiviDB has no whole-database DEBUG DIGEST, so this computes one: for each key, its type, a type-aware
canonical value (strings, hashes with each field's HPEXPIRETIME, lists, sets, zsets, streams with their
consumer groups), its PEXPIRETIME, and the server's own DEBUG DIGEST-VALUE. The "db digest" of a side is a
SHA-1 over the sorted (db, key, type, canonical value) of all its keys; a second one over (db, key,
DIGEST-VALUE). Absolute expiry times are compared with a tolerance (--ttl-tol ms), since the two reads
are not at the same instant and the two hosts' clocks differ a little. Keys that differ are read again
after a pause (a write still in flight); a key missing on one side that expires within 5 s on the other
counts as "expiring", not a difference.

  kvdiff.py A_host:port B_host:port [--out result.json] [--ttl-tol 1500] [--no-digest-value]
Exit 1 when a real difference remains.
"""
import argparse
import hashlib
import json
import sys
import time

import redis

TYPES_PLAIN = {"string", "hash", "list", "set", "zset", "stream"}


def dbs_of(c):
    ks = c.info("keyspace")
    return sorted(int(k[2:]) for k in ks if k.startswith("db"))


def scan_all(c):
    out = []
    cur = 0
    while True:
        cur, ks = c.scan(cur, count=2000)
        out.extend(ks)
        if cur == 0:
            return out


def h(x):
    return hashlib.sha1(repr(x).encode()).hexdigest()


def fetch(c, keys, digest_value):
    p = c.pipeline(transaction=False)
    for k in keys:
        p.type(k)
        p.execute_command("PEXPIRETIME", k)
    tt = p.execute(raise_on_error=False)
    types = [x.decode() if isinstance(x, bytes) else str(x) for x in tt[0::2]]
    exps = tt[1::2]
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
        elif t == "stream":
            p.execute_command("XRANGE", k, "-", "+")
        else:
            p.exists(k)
    vals = p.execute(raise_on_error=False)
    # hash field expiry times, and stream groups
    p = c.pipeline(transaction=False)
    extra_idx = []
    for i, (k, t, v) in enumerate(zip(keys, types, vals)):
        if t == "hash" and isinstance(v, dict) and v:
            fs = sorted(v.keys())
            p.execute_command("HPEXPIRETIME", k, "FIELDS", len(fs), *fs)
            extra_idx.append((i, fs))
        elif t == "stream":
            p.execute_command("XINFO", "GROUPS", k)
            extra_idx.append((i, None))
    extras = p.execute(raise_on_error=False) if extra_idx else []
    extra = {}
    for (i, fs), r in zip(extra_idx, extras):
        if fs is None:
            gs = []
            if isinstance(r, list):
                for g in r:
                    if isinstance(g, list):
                        d = dict(zip(g[0::2], g[1::2]))
                        gs.append((d.get(b"name"), d.get(b"last-delivered-id"), d.get(b"pending"),
                                   d.get(b"consumers")))
            extra[i] = sorted(gs, key=repr)
        else:
            extra[i] = dict(zip(fs, r)) if isinstance(r, list) else repr(r)
    dv = {}
    if digest_value:
        for j in range(0, len(keys), 500):
            ch = keys[j:j + 500]
            try:
                r = c.execute_command("DEBUG", "DIGEST-VALUE", *ch)
                for k, d in zip(ch, r):
                    dv[k] = d
            except Exception as e:
                for k in ch:
                    dv[k] = "ERR:" + str(e)[:40]
    out = {}
    for i, (k, t, e, v) in enumerate(zip(keys, types, exps, vals)):
        fttl = {}
        if t == "string":
            cv = v
        elif t == "hash" and isinstance(v, dict):
            cv = sorted(v.items())
            fttl = extra.get(i, {})
        elif t in ("set",) and isinstance(v, set):
            cv = sorted(v)
        elif t == "stream":
            cv = (v, extra.get(i))
        elif t == "none":
            cv = None
        else:
            cv = v
        out[k] = {"type": t, "val": h(cv), "exp": e if isinstance(e, int) else repr(e), "fttl": fttl,
                  "dv": dv.get(k)}
    return out


def close(a, b, tol):
    if a == b:
        return True
    if isinstance(a, int) and isinstance(b, int) and a > 0 and b > 0:
        return abs(a - b) <= tol
    return False


def differs(x, y, tol):
    """list of reasons"""
    r = []
    if x["type"] != y["type"]:
        return ["type %s/%s" % (x["type"], y["type"])]
    if x["val"] != y["val"]:
        r.append("value")
    if not close(x["exp"], y["exp"], tol):
        r.append("expire %r/%r" % (x["exp"], y["exp"]))
    fx, fy = x["fttl"], y["fttl"]
    if isinstance(fx, dict) and isinstance(fy, dict):
        bad = [f for f in set(fx) | set(fy) if not close(fx.get(f), fy.get(f), tol)]
        if bad:
            r.append("field-ttl %d fields e.g. %r: %r/%r" % (len(bad), bad[0], fx.get(bad[0]), fy.get(bad[0])))
    elif fx != fy:
        r.append("field-ttl %r/%r" % (fx, fy))
    return r


def side_digest(rows):
    a = hashlib.sha1()
    b = hashlib.sha1()
    for (db, k) in sorted(rows):
        v = rows[(db, k)]
        a.update(repr((db, k, v["type"], v["val"])).encode())
        b.update(repr((db, k, v["dv"])).encode())
    return a.hexdigest(), b.hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("a")
    ap.add_argument("b")
    ap.add_argument("--out")
    ap.add_argument("--ttl-tol", type=int, default=1500)
    ap.add_argument("--no-digest-value", action="store_true")
    ap.add_argument("--recheck-pause", type=float, default=3.0)
    args = ap.parse_args()
    t0 = time.time()
    conns = {}

    def conn(side, db):
        hp = args.a if side == "a" else args.b
        if (side, db) not in conns:
            host, port = hp.rsplit(":", 1)
            conns[(side, db)] = redis.Redis(host=host, port=int(port), db=db, socket_timeout=120)
        return conns[(side, db)]

    dbs = sorted(set(dbs_of(conn("a", 0))) | set(dbs_of(conn("b", 0))))
    rows_a, rows_b, bad = {}, {}, {}
    nkeys = 0
    for db in dbs:
        ka, kb = scan_all(conn("a", db)), scan_all(conn("b", db))
        union = sorted(set(ka) | set(kb))
        nkeys += len(union)
        for j in range(0, len(union), 2000):
            ch = union[j:j + 2000]
            fa = fetch(conn("a", db), ch, not args.no_digest_value)
            fb = fetch(conn("b", db), ch, not args.no_digest_value)
            for k in ch:
                if fa[k]["type"] != "none":
                    rows_a[(db, k)] = fa[k]
                if fb[k]["type"] != "none":
                    rows_b[(db, k)] = fb[k]
                d = differs(fa[k], fb[k], args.ttl_tol)
                if d:
                    bad[(db, k)] = d
    first = len(bad)
    real, expiring, dv_only = {}, 0, 0
    if bad:
        time.sleep(args.recheck_pause)
        bydb = {}
        for (db, k) in bad:
            bydb.setdefault(db, []).append(k)
        for db, ks in bydb.items():
            fa = fetch(conn("a", db), ks, not args.no_digest_value)
            fb = fetch(conn("b", db), ks, not args.no_digest_value)
            for k in ks:
                for side, f, rows in (("a", fa, rows_a), ("b", fb, rows_b)):
                    if f[k]["type"] != "none":
                        rows[(db, k)] = f[k]
                    else:
                        rows.pop((db, k), None)
                d = differs(fa[k], fb[k], args.ttl_tol)
                if not d:
                    continue
                x, y = fa[k], fb[k]
                if (x["type"] == "none" and isinstance(y["exp"], int) and 0 < y["exp"] - time.time() * 1000 < 5000) or \
                   (y["type"] == "none" and isinstance(x["exp"], int) and 0 < x["exp"] - time.time() * 1000 < 5000):
                    expiring += 1
                else:
                    real[(db, k)] = d
    # DIGEST-VALUE differences (only meaningful between the same version)
    dv_types = {}
    dv_sample = []
    for key in sorted(set(rows_a) & set(rows_b)):
        if rows_a[key]["dv"] != rows_b[key]["dv"] and key not in real:
            dv_only += 1
            t = rows_a[key]["type"]
            if t == "hash":
                t = "hash-with-field-ttl" if any(isinstance(v, int) and v > 0 for v in rows_a[key]["fttl"].values())                     else "hash-no-field-ttl"
            dv_types[t] = dv_types.get(t, 0) + 1
            if len(dv_sample) < 10:
                dv_sample.append([key[0], key[1].decode("latin-1"), t])
    da, dva = side_digest(rows_a)
    db_, dvb = side_digest(rows_b)
    res = {
        "a": args.a, "b": args.b, "dbs": dbs, "keys_compared": nkeys,
        "keys_a": len(rows_a), "keys_b": len(rows_b),
        "differ_first_pass": first, "expiring_at_compare": expiring, "real_differences": len(real),
        "digest_a": da, "digest_b": db_, "digest_equal": da == db_,
        "digestvalue_a": dva, "digestvalue_b": dvb, "digestvalue_equal": dva == dvb,
        "digestvalue_only_differences": dv_only, "digestvalue_only_by_type": dv_types,
        "digestvalue_only_sample": dv_sample,
        "sample": [[db, k.decode("latin-1"), why] for (db, k), why in list(real.items())[:25]],
        "seconds": round(time.time() - t0, 1),
    }
    print(json.dumps(res))
    if args.out:
        json.dump(res, open(args.out, "w"), indent=1)
    sys.exit(1 if real else 0)


if __name__ == "__main__":
    main()
