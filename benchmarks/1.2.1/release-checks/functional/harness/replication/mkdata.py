#!/usr/bin/env python3
"""r121 replication: a varied dataset for master/replica equality and mixed-version tests.

  mkdata.py host:port load   [--n 2000] [--dbs 3] [--text-only] [--empty-key] [--nonutf8] [--ft]
                             [--short-field-ttl] [--prefix p]
  mkdata.py host:port stream --seconds 30 [--rate 2000] [same flags]   (writes of every type, as a stream)

Types: strings (with and without TTL), hashes (fields with long HPEXPIRE TTLs; with --short-field-ttl also
fields that expire within seconds: hash-field expiry propagation is #415, a known issue), lists, sets,
zsets (incl. +inf/-inf), streams with a consumer group and pending entries, keys in several databases,
binary values. --nonutf8: keys and members that are not UTF-8 (incl. \\xff, \\x00, \\xc3\\x28). --empty-key:
the key "" in every database used. --ft: an FT index over hash keys doc:* in db 0.
"""
import argparse
import random
import time

import redis

NONUTF8 = [b"\xff", b"\xfe\xff\x00k", b"\xc3\x28", b"bin:\x80\x81\x82", b"\x00", b"a\xffb"]


def conn(hp, db):
    h, p = hp.rsplit(":", 1)
    return redis.Redis(host=h, port=int(p), db=db, socket_timeout=30)


def one(p, i, pre, args, rnd):
    k = ("%s%d" % (pre, i)).encode()
    kind = i % 7
    if kind == 0:
        p.set(b"s:" + k, ("v%d:" % i).encode() + bytes(rnd.randrange(256) for _ in range(8)) if not args.text_only
              else ("v%d:%s" % (i, "x" * 8)).encode())
    elif kind == 1:
        p.set(b"st:" + k, b"ttl%d" % i, ex=3600 + i % 600)
    elif kind == 2:
        hk = b"h:" + k
        p.hset(hk, mapping={b"a": b"1", b"b": b"%d" % i, b"c": b"x" * (i % 20), b"d": b"keep"})
        p.execute_command("HEXPIRE", hk, 7200 + i % 100, "FIELDS", 2, "a", "b")
        if args.short_field_ttl and i % 3 == 0:
            p.hset(hk, b"short", b"1")
            p.execute_command("HPEXPIRE", hk, 2000 + (i % 50) * 100, "FIELDS", 1, "short")
    elif kind == 3:
        p.rpush(b"l:" + k, *[b"e%d" % j for j in range(1 + i % 10)])
    elif kind == 4:
        p.sadd(b"set:" + k, *[b"m%d" % j for j in range(1 + i % 12)])
    elif kind == 5:
        z = {b"m%d" % j: j * 1.5 for j in range(1 + i % 9)}
        if i % 10 == 0:
            z[b"pinf"] = float("inf")
            z[b"ninf"] = float("-inf")
        p.zadd(b"z:" + k, z)
    else:
        sk = b"x:" + k
        for j in range(1 + i % 4):
            p.xadd(sk, {b"f": b"%d" % j, b"g": b"v"})
        if i % 5 == 0:
            p.execute_command("XGROUP", "CREATE", sk, "g1", "0", "MKSTREAM")
            p.execute_command("XREADGROUP", "GROUP", "g1", "c1", "COUNT", 1, "STREAMS", sk, ">")


def specials(hp, db, args):
    c = conn(hp, db)
    if args.empty_key:
        c.set(b"", b"empty-key-db%d" % db)
    if args.nonutf8:
        for j, k in enumerate(NONUTF8):
            c.set(k, b"nonutf8-%d" % j)
        c.hset(b"h:\xffbin", mapping={b"f\xff": b"v\xfe", b"g": b"1"})
        c.sadd(b"set:\xfe", b"\xff", b"\x00")
        c.zadd(b"z:\xc3\x28", {b"\xff": 1})
        c.rpush(b"l:\x80", b"\xff")
    c.hset(b"h:emptyfield", mapping={b"": b"", b"x": b""})
    c.sadd(b"set:emptymember", b"")
    c.zadd(b"z:emptymember", {b"": 0})
    c.rpush(b"l:emptyelem", b"", b"")


def load(args):
    rnd = random.Random(1)
    for db in range(args.dbs):
        c = conn(args.hp, db)
        p = c.pipeline(transaction=False)
        for i in range(args.n):
            one(p, i, "%sd%d:" % (args.prefix, db), args, rnd)
            if i % 500 == 499:
                p.execute()
        p.execute()
        specials(args.hp, db, args)
    if args.ft:
        c = conn(args.hp, 0)
        for i in range(200):
            c.hset(b"doc:%d" % i, mapping={b"title": b"hello world %d" % i, b"n": b"%d" % i})
        try:
            c.execute_command("FT.CREATE", "idx0", "ON", "HASH", "PREFIX", 1, "doc:", "SCHEMA", "title", "TEXT",
                              "n", "NUMERIC")
        except redis.ResponseError as e:
            print("FT.CREATE:", e)
    print("loaded", {db: conn(args.hp, db).dbsize() for db in range(args.dbs)})


def stream(args):
    rnd = random.Random(2)
    cs = [conn(args.hp, db) for db in range(args.dbs)]
    t_end = time.time() + args.seconds
    i = 0
    n = 0
    while time.time() < t_end:
        db = i % args.dbs
        p = cs[db].pipeline(transaction=False)
        for _ in range(50):
            one(p, i, "%sw:" % args.prefix, args, rnd)
            # also mutate earlier keys
            j = rnd.randrange(max(1, args.n))
            p.incr(b"cnt:%d" % (j % 100))
            p.hincrby(b"h:%sd%d:%d" % (args.prefix.encode(), db, (j // 7) * 7 + 2), b"b", 1)
            p.lpush(b"l:%sd%d:%d" % (args.prefix.encode(), db, (j // 7) * 7 + 3), b"new")
            if i % 13 == 0:
                p.set(b"sx:%d" % i, b"short", px=1000 + i % 3000)
            if args.nonutf8 and i % 97 == 0:
                p.set(b"wbin:\xff%d" % i, b"\xfe")
            i += 1
        p.execute()
        n += 50
        if args.rate:
            time.sleep(max(0, 50.0 / args.rate - 0.001))
    print("streamed", n, "batches of writes")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("hp")
    ap.add_argument("mode", choices=["load", "stream"])
    ap.add_argument("--n", type=int, default=2000)
    ap.add_argument("--dbs", type=int, default=3)
    ap.add_argument("--text-only", action="store_true")
    ap.add_argument("--empty-key", action="store_true")
    ap.add_argument("--nonutf8", action="store_true")
    ap.add_argument("--ft", action="store_true")
    ap.add_argument("--short-field-ttl", action="store_true")
    ap.add_argument("--prefix", default="")
    ap.add_argument("--seconds", type=float, default=30)
    ap.add_argument("--rate", type=float, default=2000)
    args = ap.parse_args()
    (load if args.mode == "load" else stream)(args)


if __name__ == "__main__":
    main()
