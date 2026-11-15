#!/usr/bin/env python3
"""Snapshot compatibility across versions: dataset loader and exact fingerprint.

  snapdata.py load  <port> [--empty-key] [--nonutf8] [--part all|a|b] [--dbs 0,5,9]
  snapdata.py fp    <port> <out.json>          fingerprint of every key in dbs 0-15
  snapdata.py diff  <a.json> <b.json>          prints the differences, exit 1 if any
  snapdata.py mutate <port>                    a few writes of every kind (for the AOF tail / the replication stream)

Dataset (text keys only, so 1.2.0 takes it; --nonutf8 adds keys 1.2.0 refuses), in each db of --dbs:
strings (plain, int, empty value, every byte value, 1 MB, absolute and relative TTLs), hashes (small, 1,000 fields,
empty field name, field TTLs set with HPEXPIREAT and HPEXPIRE), lists (small, 100,000 elements, empty elements),
sets (ints, strings, 2,000 members, empty member), sorted sets (small, 1,000, +inf/-inf, empty member), streams
(consumer groups, pending entries, consumers, deleted entries, trimmed), JSON, time series (labels, retention, a
compaction rule), vector sets (attributes), HyperLogLog, bitmap, geo, and an FT index over doc:* in db 0.
--empty-key: the empty key "" in db 0 (a string) and in db 5 (a hash with a field TTL).
--part a / b: the first / second half (for "snapshot, then more writes in the AOF").
The fingerprint holds per key: type, PEXPIRETIME, and the full value (hash fields with HPEXPIRETIME; stream
entries, groups, PEL ids/consumers/delivery counts, consumers' pending counts; TS samples, labels, rules; vector-set
elements with VEMB and attributes); plus DBSIZE per db and each FT index's name and num_docs.
"""
import hashlib
import json
import sys

import redis

FAR = 4102444800000  # 2100-01-01 in ms


def c(port, db=0):
    return redis.Redis(port=int(port), db=db, socket_timeout=120)


ERRS = []


def x(r, *a):
    try:
        return r.execute_command(*a)
    except redis.ResponseError as e:
        ERRS.append("%s -> %s" % (" ".join(str(v)[:40] for v in a[:3]), e))
        return None


def load_db(r, db, part, empty_key, nonutf8):
    A = part in ("all", "a")
    B = part in ("all", "b")
    p = "d%d:" % db
    if A:
        x(r, "SET", p + "s:plain", "hello")
        x(r, "SET", p + "s:int", "12345")
        x(r, "SET", p + "s:emptyval", "")
        x(r, "SET", p + "s:bytes", bytes(range(256)))
        x(r, "SET", p + "s:big", b"x" * (1 << 20))
        x(r, "SET", p + "s:ttl-abs", "a", "PXAT", FAR)
        x(r, "SET", p + "s:ttl-rel", "r", "EX", 86400 * 30)
        for i in range(500):
            x(r, "SET", p + "s:n:%d" % i, "v%d" % i)
        x(r, "HSET", p + "h:small", "a", "1", "b", "2", "", "emptyname")
        x(r, "HSET", p + "h:big", *sum([["f%d" % i, "v%d" % i] for i in range(1000)], []))
        x(r, "HSET", p + "h:fttl", "a", "1", "b", "2", "c", "3", "keep", "k")
        x(r, "HPEXPIREAT", p + "h:fttl", FAR, "FIELDS", 1, "a")
        x(r, "HPEXPIRE", p + "h:fttl", 86400000 * 20, "FIELDS", 1, "b")
        x(r, "RPUSH", p + "l:small", "a", "b", "", "")
        x(r, "RPUSH", p + "l:big", *["e%d" % i for i in range(100000)])
        x(r, "SADD", p + "set:int", *range(100))
        x(r, "SADD", p + "set:str", "a", "b", "c", "")
        x(r, "SADD", p + "set:big", *["m%d" % i for i in range(2000)])
        x(r, "ZADD", p + "z:small", 1, "a", 2.5, "b", 0, "")
        x(r, "ZADD", p + "z:big", *sum([[i * 0.5, "m%d" % i] for i in range(1000)], []))
        x(r, "ZADD", p + "z:inf", "+inf", "pinf", "-inf", "ninf", 0, "zero")
        sk = p + "x:s"
        for i in range(1, 51):
            x(r, "XADD", sk, "%d-1" % i, "f", "v%d" % i, "g", "")
        x(r, "XGROUP", "CREATE", sk, "g1", "0")
        x(r, "XGROUP", "CREATE", sk, "g2", "$")
        x(r, "XREADGROUP", "GROUP", "g1", "c1", "COUNT", 5, "STREAMS", sk, ">")
        x(r, "XREADGROUP", "GROUP", "g1", "c2", "COUNT", 3, "STREAMS", sk, ">")
        x(r, "XACK", sk, "g1", "2-1")
        x(r, "XDEL", sk, "10-1", "11-1")
        x(r, "XADD", p + "x:trim", "MAXLEN", "5", "*", "a", "1")
        for i in range(20):
            x(r, "XADD", p + "x:trim", "MAXLEN", "=", "5", "*", "a", str(i))
        x(r, "JSON.SET", p + "j:doc", "$", json.dumps({"a": 1, "b": [1, 2.5, "x", None, True], "c": {"d": "e"}, "": ""}))
        x(r, "TS.CREATE", p + "ts:src", "RETENTION", 0, "LABELS", "sensor", "t1", "db", str(db))
        x(r, "TS.CREATE", p + "ts:dst", "LABELS", "agg", "avg")
        x(r, "TS.CREATERULE", p + "ts:src", p + "ts:dst", "AGGREGATION", "avg", 60000)
        for i in range(200):
            x(r, "TS.ADD", p + "ts:src", 1700000000000 + i * 1000, i * 1.5)
        for i in range(30):
            x(r, "VADD", p + "v:set", "VALUES", 4, 0.1 * i, 0.2, -0.3 * (i % 3), 1.0, "e%d" % i)
        x(r, "VSETATTR", p + "v:set", "e1", json.dumps({"color": "red", "n": 1}))
        x(r, "PFADD", p + "hll", *["u%d" % i for i in range(5000)])
        for b in (1, 7, 100, 10000):
            x(r, "SETBIT", p + "bits", b, 1)
        x(r, "GEOADD", p + "geo", 13.361389, 38.115556, "Palermo", 15.087269, 37.502669, "Catania")
        if empty_key and db == 0:
            x(r, "SET", "", "empty-key-db0")
    if B:
        for i in range(500, 1000):
            x(r, "SET", p + "s:n:%d" % i, "v%d" % i)
        x(r, "HSET", p + "h:b2", "x", "1", "y", "2")
        x(r, "HPEXPIREAT", p + "h:b2", FAR - 5, "FIELDS", 1, "x")
        x(r, "RPUSH", p + "l:b2", *["b%d" % i for i in range(1000)])
        x(r, "ZADD", p + "z:b2", 1, "one", 2, "two")
        x(r, "SADD", p + "set:b2", "p", "q")
        x(r, "XADD", p + "x:b2", "1-1", "k", "v")
        x(r, "XGROUP", "CREATE", p + "x:b2", "gb", "0")
        x(r, "XREADGROUP", "GROUP", "gb", "cb", "STREAMS", p + "x:b2", ">")
        x(r, "JSON.SET", p + "j:b2", "$", '{"k":[1,2,3]}')
        x(r, "SET", p + "s:b2ttl", "t", "PXAT", FAR - 7)
        if empty_key and db == 5:
            x(r, "HSET", "", "f", "empty-key-db5", "g", "2")
            x(r, "HPEXPIREAT", "", FAR - 9, "FIELDS", 1, "g")
    if nonutf8 and A:
        for j, k in enumerate([b"\xff", b"\xfe\xff\x00k", b"\xc3\x28", b"a\xffb"]):
            x(r, "SET", k, b"nonutf8-%d" % j)
        x(r, "HSET", b"h:\xffbin", b"f\xff", b"v\xfe")
        x(r, "SADD", b"set:\xfe", b"\xff", b"\x00")


def load(port, args):
    part = args[args.index("--part") + 1] if "--part" in args else "all"
    dbs = [int(d) for d in (args[args.index("--dbs") + 1] if "--dbs" in args else "0,5,9").split(",")]
    for db in dbs:
        load_db(c(port, db), db, part, "--empty-key" in args, "--nonutf8" in args)
    if part in ("all", "a"):
        r = c(port, 0)
        for i in range(200):
            x(r, "HSET", "doc:%d" % i, "title", "hello world %d" % i, "n", str(i))
        x(r, "FT.CREATE", "idx0", "ON", "HASH", "PREFIX", 1, "doc:", "SCHEMA", "title", "TEXT", "n", "NUMERIC")
    print(json.dumps({"loaded": {db: c(port, db).dbsize() for db in dbs}, "errors": ERRS}))


def mutate(port):
    for db in (0, 5, 9):
        r = c(port, db)
        p = "d%d:" % db
        x(r, "INCRBY", p + "s:int", 7)
        x(r, "APPEND", p + "s:plain", "-more")
        x(r, "DEL", p + "s:n:3")
        x(r, "HSET", p + "h:small", "c", "3")
        x(r, "HDEL", p + "h:big", "f5")
        x(r, "LPUSH", p + "l:small", "head")
        x(r, "SREM", p + "set:str", "a")
        x(r, "ZINCRBY", p + "z:small", 10, "a")
        x(r, "XADD", p + "x:s", "100-1", "late", "1")
        x(r, "XCLAIM", p + "x:s", "g1", "c3", 0, "1-1")
        x(r, "JSON.NUMINCRBY", p + "j:doc", "$.a", 2)
        x(r, "TS.ADD", p + "ts:src", 1700000300000, 99)
        x(r, "VADD", p + "v:set", "VALUES", 4, 0.9, 0.9, 0.9, 0.9, "late")
        x(r, "PFADD", p + "hll", "late1", "late2")
        x(r, "PEXPIREAT", p + "s:n:4", FAR - 11)
        x(r, "HPEXPIREAT", p + "h:small", FAR - 13, "FIELDS", 1, "c")
    x(c(port, 0), "APPEND", "", "+tail")
    x(c(port, 5), "HSET", "", "h", "tail")
    print(json.dumps({"mutated": True, "errors": ERRS}))


def sv(v):
    if isinstance(v, bytes):
        return v.hex() if any(b > 126 or b < 32 for b in v) else "s:" + v.decode()
    if isinstance(v, (list, tuple)):
        return [sv(i) for i in v]
    if isinstance(v, dict):
        return {sv(k): sv(w) for k, w in v.items()}
    return v


def pairs(v):
    return {v[i]: v[i + 1] for i in range(0, len(v) - 1, 2)} if isinstance(v, list) else v


def raw(r):
    """The same connection settings, but replies unparsed (lists, not redis-py's dicts)."""
    k = r.connection_pool.connection_kwargs
    rr = redis.Redis(port=k["port"], db=k["db"], socket_timeout=120)
    rr.response_callbacks = {}
    return rr


def fp_key(r, k, rr):
    t = r.type(k).decode()
    e = {"type": t, "pexpiretime": rr.execute_command("PEXPIRETIME", k)}
    if t == "string":
        e["v"] = hashlib.sha1(r.get(k)).hexdigest() + ":%d" % len(r.get(k))
    elif t == "hash":
        h = r.hgetall(k)
        fs = sorted(h)
        ex = rr.execute_command("HPEXPIRETIME", k, "FIELDS", len(fs), *fs) if fs else []
        e["v"] = [[sv(f), sv(h[f]), ex[i]] for i, f in enumerate(fs)]
    elif t == "list":
        e["v"] = hashlib.sha1(repr(r.lrange(k, 0, -1)).encode()).hexdigest() + ":%d" % r.llen(k)
    elif t == "set":
        e["v"] = hashlib.sha1(repr(sorted(r.smembers(k))).encode()).hexdigest() + ":%d" % r.scard(k)
    elif t == "zset":
        e["v"] = hashlib.sha1(repr(r.zrange(k, 0, -1, withscores=True)).encode()).hexdigest() + ":%d" % r.zcard(k)
    elif t == "stream":
        e["entries"] = sv(rr.execute_command("XRANGE", k, "-", "+"))
        info = pairs(rr.execute_command("XINFO", "STREAM", k))
        e["info"] = {sv(a): sv(info.get(a)) for a in (b"length", b"last-generated-id", b"max-deleted-entry-id",
                                                         b"entries-added", b"groups")}
        groups = []
        for g in rr.execute_command("XINFO", "GROUPS", k):
            g = pairs(g)
            name = g[b"name"]
            pel = rr.execute_command("XPENDING", k, name, "-", "+", 10000)
            cons = [pairs(cc) for cc in rr.execute_command("XINFO", "CONSUMERS", k, name)]
            groups.append({"name": sv(name), "last-delivered-id": sv(g.get(b"last-delivered-id")),
                           "entries-read": g.get(b"entries-read"), "pending": g.get(b"pending"),
                           "pel": [[sv(p[0]), sv(p[1]), p[3]] for p in pel],
                           "consumers": sorted([sv(cc[b"name"]), cc[b"pending"]] for cc in cons)})
        e["groups"] = groups
    elif t == "ReJSON-RL":
        e["v"] = sv(rr.execute_command("JSON.GET", k, "$"))
    elif t == "TSDB-TYPE":
        e["samples"] = sv(rr.execute_command("TS.RANGE", k, "-", "+"))
        info = pairs(rr.execute_command("TS.INFO", k))
        e["info"] = {sv(a): sv(info.get(a)) for a in (b"retentionTime", b"labels", b"rules", b"sourceKey",
                                                         b"duplicatePolicy", b"totalSamples", b"firstTimestamp",
                                                         b"lastTimestamp")}
    elif t == "vectorset":
        n = rr.execute_command("VCARD", k)
        els = sorted(rr.execute_command("VRANDMEMBER", k, n + 10))
        e["dim"] = rr.execute_command("VDIM", k)
        e["v"] = [[sv(el), sv(rr.execute_command("VEMB", k, el)), sv(rr.execute_command("VGETATTR", k, el))]
                  for el in els]
    else:
        e["dump"] = "unhandled type"
    return e


def fp(port, out):
    res = {"dbs": {}, "ft": {}}
    for db in range(16):
        r = c(port, db)
        n = r.dbsize()
        if not n:
            continue
        keys = {}
        rr = raw(r)
        for k in r.scan_iter(count=1000):
            keys[sv(k) if k else "(empty key)"] = fp_key(r, k, rr)
        res["dbs"][str(db)] = {"dbsize": n, "keys": keys}
    r = c(port, 0)
    try:
        rr = raw(r)
        for name in rr.execute_command("FT._LIST"):
            info = pairs(rr.execute_command("FT.INFO", name))
            res["ft"][sv(name)] = {"num_docs": sv(info.get(b"num_docs"))}
    except redis.ResponseError as e:
        res["ft"] = "FT._LIST: %s" % e
    json.dump(res, open(out, "w"), sort_keys=True)
    print(json.dumps({"fp": out, "dbsize": {d: v["dbsize"] for d, v in res["dbs"].items()},
                      "empty_key": {d: "(empty key)" in v["keys"] for d, v in res["dbs"].items()}, "ft": res["ft"]}))


def classify(ka, kb):
    """'' (equal), 'ts_label_order' (#612: only the order of a time series' labels), 'field_ttl_1ms' (only hash-field
    deadlines, each within 1 ms; the -1 ms of a 1.2.0-written file, #594), or 'real'."""
    if ka == kb:
        return ""
    if ka and kb and ka.get("type") == kb.get("type") == "TSDB-TYPE":
        na, nb = json.loads(json.dumps(ka)), json.loads(json.dumps(kb))
        for n in (na, nb):
            n["info"]["s:labels"] = sorted(n["info"].get("s:labels") or [])
        if na == nb:
            return "ts_label_order"
    if ka and kb and ka.get("type") == kb.get("type") == "hash" and ka["pexpiretime"] == kb["pexpiretime"]             and len(ka["v"]) == len(kb["v"]):
        ok = True
        for (fa, va, ta), (fb, vb, tb) in zip(ka["v"], kb["v"]):
            if fa != fb or va != vb or (ta == -1) != (tb == -1) or abs(ta - tb) > 1:
                ok = False
        if ok:
            return "field_ttl_1ms"
    return "real"


def diff(a, b):
    A, B = json.load(open(a)), json.load(open(b))
    out, cls = [], {"real": 0, "ts_label_order": 0, "field_ttl_1ms": 0}
    deltas = {}
    if A["ft"] != B["ft"]:
        out.append("ft: %s != %s" % (A["ft"], B["ft"])); cls["real"] += 1
    for db in sorted(set(A["dbs"]) | set(B["dbs"])):
        da, dbb = A["dbs"].get(db, {"dbsize": 0, "keys": {}}), B["dbs"].get(db, {"dbsize": 0, "keys": {}})
        if da["dbsize"] != dbb["dbsize"]:
            out.append("db%s dbsize %s != %s" % (db, da["dbsize"], dbb["dbsize"])); cls["real"] += 1
        for k in sorted(set(da["keys"]) | set(dbb["keys"])):
            ka, kb = da["keys"].get(k), dbb["keys"].get(k)
            c = classify(ka, kb)
            if not c:
                continue
            cls[c] += 1
            if c == "field_ttl_1ms":
                for (fa, va, ta), (fb, vb, tb) in zip(ka["v"], kb["v"]):
                    if ta != tb:
                        deltas[tb - ta] = deltas.get(tb - ta, 0) + 1
            out.append("[%s] db%s key %s: %s\n    != %s" % (c, db, k, json.dumps(ka)[:400], json.dumps(kb)[:400]))
    nkeys = sum(len(v["keys"]) for v in A["dbs"].values())
    print("DIFF %s vs %s: %d keys compared; real differences %d; ts_label_order %d; field_ttl_1ms %d keys "
          "(field deadline deltas ms: %s)" % (a, b, nkeys, cls["real"], cls["ts_label_order"], cls["field_ttl_1ms"],
                                              json.dumps(deltas, sort_keys=True)))
    for o in sorted(out, key=lambda o: not o.startswith("[real]"))[:60]:
        print("  " + o)
    sys.exit(1 if cls["real"] else 0)


if __name__ == "__main__":
    m = sys.argv[1]
    if m == "load":
        load(sys.argv[2], sys.argv[3:])
    elif m == "fp":
        fp(sys.argv[2], sys.argv[3])
    elif m == "diff":
        diff(sys.argv[2], sys.argv[3])
    elif m == "mutate":
        mutate(sys.argv[2])
