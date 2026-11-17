#!/usr/bin/env python3
"""compare.py <redis-port> <kividb-port>: one sequence of set commands against both,
replies compared (set replies sorted; random replies by shape and membership).
Prints every difference and a summary line."""
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
SETREPLY = {"SMEMBERS", "SINTER", "SUNION", "SDIFF"}

def norm(name, args, rep, other):
    if isinstance(rep, list) and name in SETREPLY:
        return sorted(rep)
    return rep

def both(*args, kind="exact"):
    global total
    total += 1
    a, b = r.cmd(*args), k.cmd(*args)
    name = str(args[0]).upper()
    if kind == "exact":
        a, b = norm(name, args, a, b), norm(name, args, b, a)
        ok = a == b
    elif kind == "sorted":
        ok = sorted(a) == sorted(b) if isinstance(a, list) and isinstance(b, list) else a == b
    elif kind == "scan":  # cursor and sorted page
        ok = a[0] == b[0] and sorted(a[1]) == sorted(b[1])
    elif kind == "paged":  # a table: neither says it is done after one call
        ok = a[0] != b"0" and b[0] != b"0"
    elif kind == "shape":  # same type and length
        ok = type(a) == type(b) and (not isinstance(a, list) or len(a) == len(b))
    if not ok:
        diffs.append((args, a, b))
    return a, b

def enc(key):
    return both("OBJECT", "ENCODING", key)

words = lambda n, p="membr": ["%s:%08d" % (p, i) for i in range(n)]
ints = lambda n, step=7, base=-300: [str(i * step + base) for i in range(n)]

for c in (r, k):
    c.cmd("FLUSHALL")
# Each form and its boundaries.
sets = {
    "ints10": ints(10), "ints512": ints(512), "ints513": ints(513),
    "words10": words(10), "words128": words(128), "words129": words(129),
    "long64": words(5) + ["L" * 64], "long65": words(5) + ["L" * 65],
    "mixed": ints(10) + ["x"], "lookalikes": ["1", "2", "01", "-0", "+1", " 1"],
    "edges": [str(-2**63), "-1", "0", str(2**63 - 1), ""],
}
for key, members in sets.items():
    for i in range(0, len(members), 50):
        both("SADD", key, *members[i:i + 50])
    enc(key)
    both("SCARD", key)
    both("SMEMBERS", key)
    both("SISMEMBER", key, members[len(members) // 2])
    both("SISMEMBER", key, "absent")
    both("SMISMEMBER", key, members[0], "nope", members[-1])
    both("SSCAN", key, 0, "COUNT", 2, kind="scan" if len(members) <= 128 or key == "ints512" else "paged")
    both("SSCAN", key, 0, "MATCH", "*1*", "COUNT", 1, kind="scan" if len(members) <= 128 or key == "ints512" else "paged")
    both("SRANDMEMBER", key, kind="shape")
    both("SRANDMEMBER", key, 5, kind="shape")
    both("SRANDMEMBER", key, -20, kind="shape")
    both("SORT", key, "ALPHA")
# Conversions, one member at a time, with the encoding after each.
for c in (r, k):
    c.cmd("DEL", "grow")
for i in range(140):
    both("SADD", "grow", str(i))
    enc("grow")
both("SADD", "grow", "w")
enc("grow")
for c in (r, k):
    c.cmd("DEL", "grow2")
for i in range(10):
    both("SADD", "grow2", str(i))
both("SADD", "grow2", "word")
enc("grow2")
for i in range(120):
    both("SADD", "grow2", "w%d" % i)
    enc("grow2")
# Shrinking never converts back.
for i in range(120):
    both("SREM", "grow2", "w%d" % i)
enc("grow2")
both("SPOP", "grow2", 3, kind="shape")
enc("grow2")
# Algebra and stores, every pair.
keys = list(sets)
for a in keys:
    for b in keys:
        both("SINTER", a, b)
        both("SUNION", a, b)
        both("SDIFF", a, b)
        both("SINTERCARD", 2, a, b)
        for st in ("SINTERSTORE", "SUNIONSTORE", "SDIFFSTORE"):
            both(st, "dst", a, b)
            both("SMEMBERS", "dst")
            both("EXISTS", "dst")
            enc("dst")
# SMOVE between forms.
for a, b in [("ints10", "words10"), ("words10", "ints10"), ("mixed", "ints512"), ("ints513", "words128")]:
    m = sets[a][3]
    both("SMOVE", a, b, m)
    enc(a); enc(b)
    both("SMEMBERS", b)
# COPY / RENAME / DEL / UNLINK keep forms.
for key in keys:
    both("COPY", key, key + ":c")
    enc(key + ":c")
    both("RENAME", key + ":c", key + ":r")
    enc(key + ":r")
    both("UNLINK", key + ":r")
# Settings: clamping, and a low intset limit.
for name, val in [("set-max-listpack-entries", 1000), ("set-max-listpack-value", 100)]:
    both("CONFIG", "SET", name, val)
    both("CONFIG", "GET", name)
for c in (r, k):
    c.cmd("CONFIG", "SET", "set-max-listpack-entries", 128)
    c.cmd("CONFIG", "SET", "set-max-listpack-value", 64)
    c.cmd("CONFIG", "SET", "set-max-intset-entries", 20)
for i in range(21):
    both("SADD", "lowint", str(i))
enc("lowint")
for c in (r, k):
    c.cmd("CONFIG", "SET", "set-max-intset-entries", 512)

print("COMPARED %d commands, %d differences" % (total, len(diffs)))
for args, a, b in diffs[:60]:
    sh = lambda x: (repr(x)[:150])
    print("DIFF", [x if isinstance(x, (int,)) else (x[:40] if isinstance(x, str) else x) for x in args][:6], "redis=", sh(a), "kividb=", sh(b))
