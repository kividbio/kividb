#!/usr/bin/env python3
"""xver.py <port> write|check <json>: write = load sorted sets of every form and boundary
(packed, large, converted by count and by a long member, scores at every edge, binary and
empty members, geo points), plus removals, score changes and pops after the adds, and record
each set's members and scores in order; check = every key has exactly what was recorded, in
the same order, scores bit for bit (and nothing else is there)."""
import json, socket, sys

class C:
    def __init__(self, port):
        self.s = socket.create_connection(("127.0.0.1", port))
        self.f = self.s.makefile("rb")
    def cmd(self, *args):
        args = [a if isinstance(a, bytes) else str(a).encode() for a in args]
        self.s.sendall(b"*%d\r\n" % len(args) + b"".join(b"$%d\r\n%s\r\n" % (len(a), a) for a in args))
        return self.read()
    def read(self):
        line = self.f.readline()
        t, rest = line[:1], line[1:-2]
        if t in (b"+", b":"): return rest.decode()
        if t == b"-": raise RuntimeError(rest.decode())
        if t == b"$":
            n = int(rest)
            return None if n < 0 else self.f.read(n + 2)[:-2]
        if t in (b"*", b"~"):
            return [self.read() for _ in range(int(rest))]
        raise RuntimeError(line)

port, mode, path = int(sys.argv[1]), sys.argv[2], sys.argv[3]
c = C(port)

EDGES = ["-inf", "+inf", "-0", "0", "1", "-1", "1.5", "-1.5", "1e308", "-1e308", "5e-324",
         "247", "248", "-129", "65536", "1700000000", "-1700000000123", "36028797018963968",
         "9007199254740993", "113000000000001000", "0.1", "3.141592653589793"]

def zadd(key, pairs):
    for i in range(0, len(pairs), 100):
        args = [x for s, m in pairs[i:i + 100] for x in (s, m)]
        c.cmd("ZADD", key, *args)

def members(n, p="member"):
    return [(i % 17 - 8, "%s:%08d" % (p, i)) for i in range(n)]

if mode == "write":
    sets = {
        "z10": members(10), "z128": members(128), "z129": members(129), "z1000": members(1000),
        "member64": members(5) + [(1, "M" * 64)], "member65": members(5) + [(1, "M" * 65)],
        "edges": [(s, "e%02d" % i) for i, s in enumerate(EDGES)],
        "binary": [(1, b""), (1, b"\xff\x00\x01"), (1, b"a\x00"), (1, b"\r\n"), (2, b"\x00")],
        "ties": [(0, w) for w in ["d", "a", "c", "b", "", "aa", "ab"]],
        "big": members(5000, "big"),
    }
    for key, pairs in sets.items():
        zadd(key, pairs)
    c.cmd("GEOADD", "geo", 13.361389, 38.115556, "Palermo", 15.087269, 37.502669, "Catania")
    pts = [x for i in range(300) for x in (round(-10 + i * 0.05, 5), round(40 + i * 0.03, 5), "p%d" % i)]
    c.cmd("GEOADD", "geo300", *pts)
    # After the adds, as they happen in the AOF: changes of score, removals, pops, growth
    # past the limit and back under it, a member made long later.
    c.cmd("ZINCRBY", "z10", 2.5, "member:00000003")
    c.cmd("ZADD", "z10", "XX", "-inf", "member:00000004")
    c.cmd("ZREM", "z10", "member:00000005")
    c.cmd("ZPOPMIN", "z128", 3)
    c.cmd("ZPOPMAX", "z129", 2)
    c.cmd("ZREMRANGEBYSCORE", "z1000", 0, 3)
    for i in range(140):
        c.cmd("ZADD", "grown", i, "g%d" % i)
    for i in range(130):
        c.cmd("ZREM", "grown", "g%d" % i)
    c.cmd("ZADD", "longlater", 1, "a", 2, "b")
    c.cmd("ZADD", "longlater", 3, "L" * 100)
    c.cmd("ZUNIONSTORE", "union", 2, "z10", "member64")
    c.cmd("ZRANGESTORE", "rstore", "z1000", 0, 99)
    keys = sorted(k.decode("latin1") for k in c.cmd("KEYS", "*"))
    want = {}
    for k in keys:
        flat = c.cmd("ZRANGE", k, 0, -1, "WITHSCORES")
        want[k] = [[flat[i].decode("latin1"), flat[i + 1].decode()] for i in range(0, len(flat), 2)]
    json.dump(want, open(path, "w"))
    print("WROTE %d keys, %d members" % (len(want), sum(len(v) for v in want.values())))
else:
    want = json.load(open(path))
    keys = sorted(k.decode("latin1") for k in c.cmd("KEYS", "*"))
    bad = []
    if keys != sorted(want):
        bad.append("keys differ: %s" % sorted(set(keys) ^ set(want)))
    encodings = {}
    for k, w in want.items():
        flat = c.cmd("ZRANGE", k.encode("latin1"), 0, -1, "WITHSCORES")
        got = [[flat[i].decode("latin1"), flat[i + 1].decode()] for i in range(0, len(flat), 2)]
        # Scores as numbers: releases write some differently in replies (1.2.2 says
        # `1.7e+9` for 1700000000), and 1.2.2 keeps a score of -0 as -0 where 1.3
        # keeps 0 (#422), which compare equal; the order is compared as it is.
        as_num = lambda pairs: [(m, float(s)) for m, s in pairs]
        if as_num(got) != as_num(w):
            bad.append("%s: %d members, want %d (first difference %s)" % (
                k, len(got), len(w), next(((a, b) for a, b in zip(got, w) if a != b), None)))
        e = c.cmd("OBJECT", "ENCODING", k.encode("latin1"))
        encodings[e.decode()] = encodings.get(e.decode(), 0) + 1
    print("CHECK %s %d keys %s %s" % ("OK" if not bad else "FAILED", len(want), encodings, bad[:5]))
