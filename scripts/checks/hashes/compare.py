#!/usr/bin/env python3
"""compare.py <redis-port> <kividb-port>: one sequence of hash commands against both,
replies compared. While Redis keeps a hash as a listpack its order is compared exactly
(HGETALL, HKEYS, HVALS, HSCAN); a hashtable's replies are compared sorted; random
replies by shape. OBJECT ENCODING after every step. Prints every difference and a
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

def pairs_of(flat):
    return sorted((flat[i], flat[i + 1]) for i in range(0, len(flat), 2))

def both(*args, kind="exact"):
    global total
    total += 1
    a, b = r.cmd(*args), k.cmd(*args)
    if kind == "exact":
        ok = a == b
    elif kind == "sorted":
        ok = sorted(a) == sorted(b) if isinstance(a, list) and isinstance(b, list) else a == b
    elif kind == "pairs":  # HGETALL of a table: the same pairs, in any order
        ok = pairs_of(a) == pairs_of(b) if isinstance(a, list) and isinstance(b, list) else a == b
    elif kind == "scan":  # whole, cursor 0, in order
        ok = a == b
    elif kind == "paged":  # a table: paged, or whole at once (Redis visits ten buckets
        # a COUNT, so a small table comes back whole); never a part with cursor 0
        whole = lambda x: x[0] != b"0" or len(x[1]) == len(r.cmd("HKEYS", args[1])) * (1 if "NOVALUES" in args else 2) or "MATCH" in args
        ok = whole(a) and whole(b)
    elif kind == "shape":  # same type and length
        ok = type(a) == type(b) and (not isinstance(a, list) or len(a) == len(b))
    if not ok:
        diffs.append((args, a, b))
    return a, b

def enc(key):
    return both("OBJECT", "ENCODING", key)[0]

def listpack(key):
    return r.cmd("OBJECT", "ENCODING", key) in (b"listpack", b"listpackex")

def reads(key, probe=None):
    """Every read of a hash, by what Redis keeps it as."""
    lp = listpack(key)
    enc(key)
    both("HLEN", key)
    both("HGETALL", key, kind="exact" if lp else "pairs")
    both("HKEYS", key, kind="exact" if lp else "sorted")
    both("HVALS", key, kind="exact" if lp else "sorted")
    both("HSCAN", key, 0, "COUNT", 2, kind="scan" if lp else "paged")
    both("HSCAN", key, 0, "MATCH", "*1*", "COUNT", 1, kind="scan" if lp else "paged")
    both("HSCAN", key, 0, "COUNT", 2, "NOVALUES", kind="scan" if lp else "paged")
    if probe is not None:
        both("HGET", key, probe)
        both("HEXISTS", key, probe)
        both("HSTRLEN", key, probe)
        both("HMGET", key, probe, "absent", probe)
    both("HGET", key, "absent")
    both("HEXISTS", key, "absent")
    both("HRANDFIELD", key, kind="shape")
    both("HRANDFIELD", key, 5, kind="shape")
    both("HRANDFIELD", key, -20, "WITHVALUES", kind="shape")
    both("HRANDFIELD", key, 100000, kind="exact" if lp else "sorted")
    both("MEMORY", "USAGE", key, kind="shape")

def pairs(n, p="field"):
    return [("%s:%08d" % (p, i), "value:%026d" % i) for i in range(n)]

for c in (r, k):
    c.cmd("FLUSHALL")
hashes = {
    "h10": pairs(10), "h512": pairs(512), "h513": pairs(513),
    "field64": pairs(5) + [("F" * 64, "v")], "field65": pairs(5) + [("F" * 65, "v")],
    "value64": pairs(5) + [("f", "V" * 64)], "value65": pairs(5) + [("f", "V" * 65)],
    "empties": [("", ""), ("e", "")],
    "numbers": [("a", "1"), ("b", "-7"), ("c", "3.5")],
    "table": pairs(1000, "t"),
}
for key, fv in hashes.items():
    for i in range(0, len(fv), 50):
        args = [x for f, v in fv[i:i + 50] for x in (f, v)]
        both("HSET", key, *args)
    reads(key, fv[len(fv) // 2][0])
# Writes inside a block: in place, order kept.
both("HSET", "h10", "field:00000002", "a longer value than before, still short")
both("HSET", "h10", "field:00000007", "x")
both("HDEL", "h10", "field:00000000", "absent")
both("HSETNX", "h10", "field:00000001", "no")
both("HSETNX", "h10", "new", "yes")
both("HINCRBY", "numbers", "a", 41)
both("HINCRBY", "numbers", "n", 5)
both("HINCRBYFLOAT", "numbers", "c", "0.25")
both("HINCRBY", "numbers", "c", 1)
both("HINCRBYFLOAT", "numbers", "b", "1e3")
for key in ("h10", "numbers"):
    reads(key, "field:00000002")
# Conversions, one field at a time, with the encoding after each.
for c in (r, k):
    c.cmd("DEL", "grow")
for i in range(515):
    both("HSET", "grow", "f%d" % i, "v")
    if i % 64 == 0 or i >= 509:
        enc("grow")
reads("grow", "f3")
# By a long field, a long value, a value set again too long, HINCRBYFLOAT's text.
for key, field, value in [("lf", "L" * 65, "v"), ("lv", "f", "V" * 65), ("again", "a", "1")]:
    both("HSET", key, "x", "1", "y", "2")
    enc(key)
    both("HSET", key, field, value)
    enc(key)
    reads(key, "x")
both("HSET", "again", "a", "A" * 70)
enc("again")
# Shrinking never converts back.
for i in range(510):
    both("HDEL", "grow", "f%d" % i)
reads("grow", "f512")
both("HDEL", "lv", "f")
enc("lv")
# HGETDEL, HGETEX, HSETEX, field TTLs on each form (absolute deadlines: exact).
FAR = 4_000_000_000_000
for key in ("h10", "h513", "value65"):
    both("HPEXPIREAT", key, FAR, "FIELDS", 2, "field:00000003", "absent")
    enc(key)
    both("HPEXPIRETIME", key, "FIELDS", 2, "field:00000003", "field:00000004")
    both("HPERSIST", key, "FIELDS", 1, "field:00000003")
    both("HPEXPIRETIME", key, "FIELDS", 1, "field:00000003")
    both("HPEXPIREAT", key, FAR + 5, "NX", "FIELDS", 1, "field:00000004")
    reads(key, "field:00000004")
    both("HGETDEL", key, "FIELDS", 2, "field:00000005", "absent")
    both("HGETEX", key, "PXAT", FAR + 9, "FIELDS", 1, "field:00000006")
    both("HPEXPIRETIME", key, "FIELDS", 1, "field:00000006")
    both("HSETEX", key, "PXAT", FAR + 11, "FIELDS", 1, "field:00000007", "set-with-a-ttl")
    both("HPEXPIRETIME", key, "FIELDS", 1, "field:00000007")
    reads(key, "field:00000007")
# A deadline that has come deletes the field at once; the last one deletes the hash.
both("HSET", "gone", "a", "1", "b", "2")
both("HPEXPIREAT", "gone", 1, "FIELDS", 1, "a")
reads("gone", "b")
both("HPEXPIREAT", "gone", 1, "FIELDS", 1, "b")
both("EXISTS", "gone")
# SORT by hash fields.
both("RPUSH", "ids", "1", "2", "3")
for i in (1, 2, 3):
    both("HSET", "obj:%d" % i, "w", str(10 - i), "name", "n%d" % i)
both("SORT", "ids", "BY", "obj:*->w", "GET", "obj:*->name")
# COPY / RENAME / DEL / UNLINK keep forms.
for key in list(hashes):
    both("COPY", key, key + ":c")
    enc(key + ":c")
    both("RENAME", key + ":c", key + ":r")
    reads(key + ":r")
    both("UNLINK", key + ":r")
both("DEL", "h512")
both("EXISTS", "h512")
# Settings: clamping, aliases, and lower limits from the next write that crosses them.
for name, val in [("hash-max-listpack-entries", 1000), ("hash-max-listpack-value", 100),
                  ("hash-max-ziplist-entries", 300), ("hash-max-ziplist-value", 40)]:
    both("CONFIG", "SET", name, val)
    both("CONFIG", "GET", name)
    both("CONFIG", "GET", name.replace("ziplist", "listpack") if "ziplist" in name else name.replace("listpack", "ziplist"))
for c in (r, k):
    c.cmd("CONFIG", "SET", "hash-max-listpack-entries", 4)
    c.cmd("CONFIG", "SET", "hash-max-listpack-value", 64)
both("HSET", "low", "a", "1", "b", "2", "c", "3", "d", "4")
enc("low")
both("HSET", "low", "a", "again")
enc("low")
both("HSET", "low", "e", "5")
enc("low")
both("HSET", "low2", "a", "1", "b", "2", "c", "3", "d", "4", "e", "5")
enc("low2")
for c in (r, k):
    c.cmd("CONFIG", "SET", "hash-max-listpack-entries", 0)
both("HSET", "never", "a", "1")
enc("never")
for c in (r, k):
    c.cmd("CONFIG", "SET", "hash-max-listpack-entries", 512)
    c.cmd("CONFIG", "SET", "hash-max-listpack-value", 64)

print("COMPARED %d commands, %d differences" % (total, len(diffs)))
for args, a, b in diffs[:80]:
    sh = lambda x: (repr(x)[:160])
    print("DIFF", [x if isinstance(x, int) else (x[:40] if isinstance(x, str) else x) for x in args][:7], "redis=", sh(a), "kividb=", sh(b))
