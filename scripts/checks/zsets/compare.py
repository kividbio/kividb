#!/usr/bin/env python3
"""compare.py <redis-port> <kividb-port>: one sequence of sorted-set commands against both,
replies compared. A sorted set's order is its own, so almost every reply is compared exactly,
in order, whatever the form; ZSCAN of a skiplist is compared as a set of pairs (it pages),
random replies by shape. OBJECT ENCODING after every step. Prints every difference and a
summary line."""
import socket, sys

class C:
    def __init__(self, port):
        self.s = socket.create_connection(("127.0.0.1", port))
        self.f = self.s.makefile("rb")
    def cmd(self, *args):
        args = [a if isinstance(a, bytes) else str(a).encode() for a in args]
        out = b"*%d\r\n" % len(args) + b"".join(b"$%d\r\n%s\r\n" % (len(a), a) for a in args)
        self.s.sendall(out)
        return self.read()
    def read(self):
        line = self.f.readline()
        t, rest = line[:1], line[1:-2]
        if t == b"+": return rest.decode()
        if t == b"-": return ("ERR", rest.decode())
        if t == b":": return int(rest)
        if t == b"$":
            n = int(rest)
            return None if n < 0 else self.f.read(n + 2)[:-2]
        if t in (b"*", b"~"):
            n = int(rest)
            return None if n < 0 else [self.read() for _ in range(n)]
        raise RuntimeError(line)

r, k = C(int(sys.argv[1])), C(int(sys.argv[2]))
diffs, total = [], 0
enc_diffs = []

def scan_all(c, key):
    """Every (member, score) a full ZSCAN gives, and whether it came whole at once."""
    cursor, seen, calls = b"0", [], 0
    while True:
        cursor, items = c.cmd("ZSCAN", key, cursor, "COUNT", 3)
        calls += 1
        seen += [(items[i], items[i + 1]) for i in range(0, len(items), 2)]
        if cursor == b"0":
            return sorted(set(seen)), calls

def both(*args, kind="exact"):
    global total
    total += 1
    a, b = r.cmd(*args), k.cmd(*args)
    if kind == "exact":
        ok = a == b
    elif kind == "sorted":
        ok = sorted(a) == sorted(b) if isinstance(a, list) and isinstance(b, list) else a == b
    elif kind == "shape":  # same type and length
        ok = type(a) == type(b) and (not isinstance(a, list) or len(a) == len(b))
    elif kind == "error":  # both an error, or both the same
        ok = (isinstance(a, tuple) and isinstance(b, tuple)) or a == b
    if not ok:
        diffs.append((args, a, b))
    return a, b

def enc(key):
    a, b = r.cmd("OBJECT", "ENCODING", key), k.cmd("OBJECT", "ENCODING", key)
    global total
    total += 1
    if a != b:
        enc_diffs.append((key, a, b))
    return a

def reads(key, probe=None, deep=True):
    """Every read of a sorted set."""
    e = enc(key)
    both("ZCARD", key)
    both("ZRANGE", key, 0, -1, "WITHSCORES")
    if deep:
        both("ZRANGE", key, 0, 9)
        both("ZRANGE", key, -3, -1, "WITHSCORES")
        both("ZRANGE", key, 2, 1)
        both("ZREVRANGE", key, 0, 4, "WITHSCORES")
        both("ZRANGE", key, "-inf", "+inf", "BYSCORE", "LIMIT", 1, 3)
        both("ZRANGE", key, "(1", "5", "BYSCORE", "WITHSCORES")
        both("ZRANGE", key, "+inf", "-inf", "BYSCORE", "REV", "LIMIT", 0, 2, "WITHSCORES")
        both("ZRANGEBYSCORE", key, "-inf", "(3")
        both("ZREVRANGEBYSCORE", key, "10", "-10", "WITHSCORES", "LIMIT", 1, 100)
        both("ZRANGE", key, "-", "+", "BYLEX", "LIMIT", 0, 5)
        both("ZRANGE", key, "[m1", "(m5", "BYLEX")
        both("ZREVRANGEBYLEX", key, "+", "[a")
        both("ZCOUNT", key, "-inf", "+inf")
        both("ZCOUNT", key, "(0", "3")
        both("ZLEXCOUNT", key, "-", "+")
        both("ZLEXCOUNT", key, "[m", "(n")
        both("ZRANDMEMBER", key, kind="shape")
        both("ZRANDMEMBER", key, 3, kind="shape")
        both("ZRANDMEMBER", key, -5, "WITHSCORES", kind="shape")
        both("ZRANDMEMBER", key, 1000, "WITHSCORES", kind="sorted")
        # ZSCAN: a listpack whole, in order, cursor 0; a skiplist paged.
        if e == b"listpack":
            both("ZSCAN", key, 0, "COUNT", 2)
            both("ZSCAN", key, 0, "MATCH", "*1*")
        else:
            global total
            total += 1
            a, b = scan_all(r, key), scan_all(k, key)
            if a[0] != b[0]:
                diffs.append((("ZSCAN-all", key), a, b))
        # On a copy: Redis turns a listpack it sorts into a skiplist, which
        # would show in every OBJECT ENCODING after (see COMPATIBILITY).
        both("COPY", key, "sort-copy", "REPLACE")
        both("SORT", "sort-copy", "BY", "nosort")
        both("SORT", "sort-copy", "ALPHA", "LIMIT", 0, 5, kind="error")
    if probe is not None:
        both("ZSCORE", key, probe)
        both("ZRANK", key, probe, "WITHSCORE")
        both("ZREVRANK", key, probe)
        both("ZMSCORE", key, probe, "nope", probe)

def m(i, width=0):
    s = "m%d" % i
    return s + "." * max(0, width - len(s))

def fresh(*keys):
    for key in keys:
        both("DEL", key)

# 1. Growth one member at a time across 128 -> 129, scores mixed.
fresh("g")
for i in range(1, 131):
    score = [i, -i, i / 4, 1e9 + i, 0][i % 5]
    both("ZADD", "g", score, m(i))
    reads("g", m(i // 2), deep=(i % 16 == 0 or i in (127, 128, 129, 130)))

# 2. Shrinking a large set back below the limit: stays a skiplist (Redis does not go back).
for i in range(1, 60):
    both("ZREM", "g", m(i))
reads("g", m(70))
both("ZREMRANGEBYRANK", "g", 0, 40)
reads("g", m(110))

# 3. A member of 64 bytes stays packed, 65 converts; also through ZINCRBY.
for n in (64, 65):
    fresh("l%d" % n, "li%d" % n)
    both("ZADD", "l%d" % n, 1, "a", 2, "x" * n, 3, "b")
    reads("l%d" % n, "x" * n)
    both("ZADD", "li%d" % n, 1, "a")
    both("ZINCRBY", "li%d" % n, 5, "y" * n)
    reads("li%d" % n, "y" * n)

# 4. Scores at every edge, members binary and empty: order and ties.
fresh("e")
edges = ["-inf", "+inf", "-0", "0", "0.0", "1", "-1", "1.5", "-1.5", "1e308", "-1e308",
         "5e-324", "-5e-324", "247", "248", "255", "256", "-128", "-129", "65536",
         "1700000000", "-1700000000123", "36028797018963967", "36028797018963968",
         "9007199254740993", "113000000000001000", "0x1p+4", "1e-300", "3.141592653589793"]
for i, s in enumerate(edges):
    both("ZADD", "e", s, "e%02d" % i)
both("ZADD", "e", 1, "", 1, b"\xff\x00\x01", 1, "a", 1, "A", 1, "a\x00")
reads("e", "")
reads("e", b"\xff\x00\x01")
both("ZSCORE", "e", "e02")
both("ZADD", "e", "nan", "x", kind="error")
both("ZINCRBY", "e", "-inf", "e01", kind="error")
both("ZINCRBY", "e", "+inf", "e01")
both("ZINCRBY", "e", "-inf", "e01", kind="error")
reads("e", "e01")

# 5. ZADD flags on a packed set and on a large one.
for key, n in (("fp", 10), ("fl", 140)):
    fresh(key)
    both("ZADD", key, *sum(([i, m(i)] for i in range(n)), []))
    both("ZADD", key, "NX", 100, m(1), 100, "new1")
    both("ZADD", key, "XX", 100, m(2), 100, "new2")
    both("ZADD", key, "XX", "CH", 100, m(3), 3, m(4))
    both("ZADD", key, "GT", "CH", 1, m(5), 50, m(6), 7, "new3")
    both("ZADD", key, "LT", "CH", 1, m(7), 50, m(8))
    both("ZADD", key, "INCR", 2.5, m(9))
    both("ZADD", key, "INCR", "NX", 2.5, m(9))
    both("ZADD", key, "INCR", "XX", 2.5, "new4")
    both("ZADD", key, "GT", "INCR", -1, m(9))
    both("ZADD", key, "CH", 0, m(0), "-0", m(0))
    both("ZADD", key, "NX", "XX", 1, "a", kind="error")
    both("ZADD", key, "GT", "LT", 1, "a", kind="error")
    both("ZADD", key, 1, "a", 2, kind="error")
    both("ZADD", key, "x", "a", kind="error")
    both("ZINCRBY", key, 3, m(3))
    both("ZINCRBY", key, -1e9, "new5")
    reads(key, m(5))

# 6. ZADD's own count decides the form (Redis judges by the command's pairs).
fresh("h1", "h2", "h3")
both("ZADD", "h1", *sum(([1, "same"] for _ in range(129)), []))
enc("h1")
both("ZADD", "h2", 1, "a")
both("ZADD", "h2", *sum(([1, "a"] for _ in range(129)), []))
enc("h2")
both("ZADD", "h3", "XX", 1, "x" * 65)
both("EXISTS", "h3")
both("ZADD", "h3", 1, "a")
both("ZADD", "h3", "XX", 2, "x" * 65)
enc("h3")
fresh("h4")
both("GEOADD", "h4", 13.36, 38.11, "x" * 65)
enc("h4")

# 7. Pops and removals on each form.
for key, n in (("pp", 20), ("pl", 200)):
    fresh(key)
    both("ZADD", key, *sum(([i % 7, m(i)] for i in range(n)), []))
    both("ZPOPMIN", key)
    both("ZPOPMAX", key, 3)
    both("ZPOPMIN", key, 0)
    both("ZMPOP", 2, "nokey", key, "MIN", "COUNT", 4)
    both("ZMPOP", 1, key, "MAX")
    both("BZPOPMIN", key, 0.1)
    both("BZPOPMAX", "nokey", key, 0.1)
    both("BZMPOP", 0.1, 1, key, "MAX", "COUNT", 2)
    both("ZREM", key, m(10), m(11), "nope")
    both("ZREMRANGEBYSCORE", key, "(2", "4")
    both("ZREMRANGEBYLEX", key, "[m15", "[m19")
    both("ZREMRANGEBYRANK", key, -3, -1)
    reads(key, m(13))
both("ZPOPMIN", "pp", 1000)
both("EXISTS", "pp")
both("ZREMRANGEBYRANK", "pl", 0, -1)
both("EXISTS", "pl")

# 8. Lex ranges on equal scores.
fresh("lex")
both("ZADD", "lex", *sum(([0, w] for w in ["a", "b", "c", "d", "e", "f", "g", "aa", "", "zz"]), []))
for lo, hi in (("-", "+"), ("[b", "[e"), ("(b", "(e"), ("[aa", "+"), ("-", "(c"), ("+", "-"), ("[", "[")):
    both("ZRANGEBYLEX", "lex", lo, hi)
    both("ZREVRANGEBYLEX", "lex", hi, lo)
    both("ZLEXCOUNT", "lex", lo, hi)
both("ZRANGEBYLEX", "lex", "-", "+", "LIMIT", 2, 3)
both("ZRANGEBYLEX", "lex", "b", "c", kind="error")
both("ZREMRANGEBYLEX", "lex", "[b", "(d")
reads("lex", "c")

# 9. Set operations: results and their destinations' forms, inputs of each form.
fresh("a1", "a2", "b1", "s1")
both("ZADD", "a1", *sum(([i, m(i)] for i in range(0, 40)), []))
both("ZADD", "a2", *sum(([i * 2, m(i)] for i in range(20, 60)), []))
both("ZADD", "b1", *sum(([i, m(i)] for i in range(0, 300)), []))
both("SADD", "s1", m(1), m(2), m(25), "zz")
for keys in (["a1", "a2"], ["a1", "b1"], ["b1", "a2", "s1"], ["a1", "s1"], ["a1"], ["b1"]):
    n = len(keys)
    both("ZUNION", n, *keys, "WITHSCORES")
    both("ZINTER", n, *keys, "WITHSCORES")
    both("ZDIFF", n, *keys, "WITHSCORES")
    both("ZINTERCARD", n, *keys)
    both("ZINTERCARD", n, *keys, "LIMIT", 3)
    for op in ("ZUNIONSTORE", "ZINTERSTORE", "ZDIFFSTORE"):
        both(op, "dst", n, *keys)
        reads("dst", m(25))
    both("ZUNIONSTORE", "dst", n, *keys, "WEIGHTS", *([2] * n), "AGGREGATE", "MAX")
    reads("dst", m(25), deep=False)
    both("ZINTERSTORE", "dst", n, *keys, "AGGREGATE", "MIN")
    reads("dst", m(25), deep=False)
both("ZUNION", 2, "a1", "a2", "AGGREGATE", "COUNT", "WITHSCORES", kind="error")
both("ZUNIONSTORE", "dst", 2, "a1", "nokey")
reads("dst", None)
both("ZINTERSTORE", "dst", 2, "a1", "nokey")
both("EXISTS", "dst")

# 10. ZRANGESTORE: destinations of each size.
for args in ((0, 9), (0, -1), ("-inf", "+inf", "BYSCORE"), ("[m1", "+", "BYLEX"), (10, 2, "BYSCORE", "REV", "LIMIT", 0, 3), (0, 200)):
    for src in ("a1", "b1"):
        both("ZRANGESTORE", "rs", src, *args)
        reads("rs", m(5), deep=False)
both("ZRANGESTORE", "rs", "b1", 0, 127)
reads("rs", m(5))
both("ZRANGESTORE", "rs", "b1", 0, 128)
reads("rs", m(5))
both("ZRANGESTORE", "rs", "a1", 5, 1)
both("EXISTS", "rs")

# 11. COPY, RENAME keep the form; SORT on each form.
for src in ("a1", "b1", "e"):
    both("COPY", src, "cp", "REPLACE")
    reads("cp", m(3), deep=False)
    both("ZADD", "cp", 1000, "added")
    reads("cp", "added", deep=False)
    reads(src, "added", deep=False)
    both("RENAME", "cp", "cp2")
    reads("cp2", "added", deep=False)
    both("SORT", "cp2", "BY", "nosort", "DESC", "LIMIT", 0, 5)
    both("SORT", "cp2", "ALPHA", "DESC", "LIMIT", 1, 5)
    both("SORT", "cp2", "BY", "nosort", "STORE", "sorted")
    both("LRANGE", "sorted", 0, -1)

# 12. The settings: lowered, raised, the older names; effect at the next new member.
both("CONFIG", "GET", "zset-max-*")
both("CONFIG", "SET", "zset-max-listpack-entries", 10)
both("CONFIG", "GET", "zset-max-ziplist-entries")
fresh("c1")
for i in range(12):
    both("ZADD", "c1", i, m(i))
    enc("c1")
both("ZADD", "a1", 100, m(1))  # an update of a block over the lowered limit
enc("a1")
both("ZADD", "a1", 100, "brand-new")
enc("a1")
both("CONFIG", "SET", "zset-max-ziplist-value", 3)
both("CONFIG", "GET", "zset-max-listpack-value")
fresh("c2")
both("ZADD", "c2", 1, "abc")
enc("c2")
both("ZADD", "c2", 1, "abcd")
enc("c2")
both("CONFIG", "SET", "zset-max-listpack-entries", 0)
fresh("c3")
both("ZADD", "c3", 1, "a")
enc("c3")
both("CONFIG", "SET", "zset-max-listpack-entries", 128)
both("CONFIG", "SET", "zset-max-listpack-value", 64)
both("CONFIG", "SET", "zset-max-listpack-entries", -1, kind="error")
both("CONFIG", "SET", "zset-max-listpack-entries", "x", kind="error")
fresh("c4")
both("ZADD", "c4", 1, "a")
enc("c4")

# 13. GEO: packed and large sets of points.
for key, n in (("geo-small", 20), ("geo-large", 300)):
    fresh(key, "gs")
    pts = []
    for i in range(n):
        pts += [round(-10 + (i * 0.37) % 20, 5), round(35 + (i * 0.23) % 15, 5), "p%d" % i]
    both("GEOADD", key, *pts)
    both("GEOADD", key, "CH", "XX", 1, 40, "p1", 2, 41, "zzz")
    both("GEOADD", key, "NX", 1, 40, "p2", 2, 41, "new")
    enc(key)
    both("GEOPOS", key, "p1", "p2", "nope")
    both("GEODIST", key, "p1", "p3", "km")
    both("GEOHASH", key, "p1", "p4")
    both("GEOSEARCH", key, "FROMLONLAT", 0, 40, "BYRADIUS", 500, "km", "ASC", "WITHCOORD", "WITHDIST")
    both("GEOSEARCH", key, "FROMMEMBER", "p3", "BYBOX", 800, 800, "km", "DESC", "COUNT", 5)
    both("GEORADIUS", key, 0, 40, 300, "km", "WITHDIST", "ASC")
    both("GEORADIUSBYMEMBER", key, "p5", 200, "km", "ASC", "COUNT", 3)
    both("GEOSEARCHSTORE", "gs", key, "FROMLONLAT", 0, 40, "BYRADIUS", 800, "km", "ASC")
    reads("gs", "p1", deep=False)
    both("GEOSEARCHSTORE", "gs", key, "FROMLONLAT", 0, 40, "BYRADIUS", 800, "km", "ASC", "STOREDIST")
    reads("gs", "p1", deep=False)
    reads(key, "p7", deep=False)

# 14. DUMP/RESTORE within each server: the form comes back.
for src in ("a1", "b1", "e", "geo-small"):
    for c in (r, k):
        payload = c.cmd("DUMP", src)
        c.cmd("RESTORE", "rst", 0, payload, "REPLACE")
    reads("rst", m(3), deep=False)

# 15. Types and arity.
both("SET", "str", "x")
both("ZADD", "str", 1, "a", kind="error")
both("ZRANGE", "str", 0, -1, kind="error")
both("ZCARD", "nokey")
both("ZSCORE", "nokey", "a")
both("ZRANGE", "a1", "x", 1, kind="error")
both("ZRANGE", "a1", 0, 1, "BYLEX", kind="error")

for args, a, b in diffs:
    print("DIFF", [x if isinstance(x, (int, float)) else (x.decode("latin1") if isinstance(x, bytes) else x) for x in args][:10])
    print("   redis :", repr(a)[:400])
    print("   kividb:", repr(b)[:400])
for key, a, b in enc_diffs:
    print("ENCODING", key, "redis", a, "kividb", b)
print(f"COMPARED {total} commands, {len(diffs)} reply differences, {len(enc_diffs)} encoding differences")
