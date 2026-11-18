#!/usr/bin/env python3
"""xver.py <port> write|check <json>: write = load hashes of every form and boundary,
with and without field TTLs, plus removals and conversions, and record their fields,
values and deadlines; check = every key has what was recorded (and nothing else is there)."""
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

def pairs(n, p="field"):
    return [("%s:%08d" % (p, i), "value:%026d" % i) for i in range(n)]

FAR = 4_000_000_000_000  # a deadline in 2096: never due during the check
if mode == "write":
    hashes = {
        "h10": pairs(10), "h512": pairs(512), "h513": pairs(513),
        "field64": pairs(5) + [("F" * 64, "v")], "field65": pairs(5) + [("F" * 65, "v")],
        "value64": pairs(5) + [("f", "V" * 64)], "value65": pairs(5) + [("f", "V" * 65)],
        "empties": [("", ""), ("e", ""), ("", "again")],
        "numbers": [("a", "1"), ("b", "-7"), ("c", "3.5")],
        "big": pairs(5000, "big"),
        "ttl10": pairs(10), "ttl600": pairs(600),
    }
    for key, fv in hashes.items():
        for i in range(0, len(fv), 100):
            args = [x for f, v in fv[i:i + 100] for x in (f, v)]
            c.cmd("HSET", key, *args)
    # Field TTLs: a packed hash and a table with some of their fields' deadlines.
    c.cmd("HPEXPIREAT", "ttl10", FAR, "FIELDS", 3, "field:00000000", "field:00000004", "field:00000009")
    c.cmd("HPEXPIREAT", "ttl600", FAR + 1, "FIELDS", 2, "field:00000001", "field:00000599")
    # Removals and conversions after the adds: in the AOF as they happened.
    c.cmd("HDEL", "h10", "field:00000003")
    c.cmd("HSET", "h10", "field:00000005", "a value set again, of another length")
    c.cmd("HINCRBY", "numbers", "a", 41)
    c.cmd("HINCRBYFLOAT", "numbers", "c", "0.25")
    for i in range(530):
        c.cmd("HSET", "grown", "g%d" % i, "v%d" % i)
    for i in range(500):
        c.cmd("HDEL", "grown", "g%d" % i)
    c.cmd("HSET", "longlater", "a", "1", "b", "2")
    c.cmd("HSET", "longlater", "b", "L" * 100)
    keys = sorted(k.decode() for k in c.cmd("KEYS", "*"))
    want = {}
    for k in keys:
        flat = c.cmd("HGETALL", k)
        fields = sorted([flat[i].decode("latin1"), flat[i + 1].decode("latin1")] for i in range(0, len(flat), 2))
        names = [f for f, _ in fields]
        times = c.cmd("HPEXPIRETIME", k, "FIELDS", len(names), *names) if names else []
        want[k] = {"fields": fields, "times": [int(t) for t in times]}
    json.dump(want, open(path, "w"))
    print("WROTE %d keys, %d fields" % (len(want), sum(len(v["fields"]) for v in want.values())))
else:
    want = json.load(open(path))
    keys = sorted(k.decode() for k in c.cmd("KEYS", "*"))
    bad = []
    if keys != sorted(want):
        bad.append("keys differ: %s" % sorted(set(keys) ^ set(want)))
    for k, w in want.items():
        flat = c.cmd("HGETALL", k)
        fields = sorted([flat[i].decode("latin1"), flat[i + 1].decode("latin1")] for i in range(0, len(flat), 2))
        if fields != w["fields"]:
            bad.append("%s: %d fields, want %d" % (k, len(fields), len(w["fields"])))
            continue
        names = [f for f, _ in fields]
        times = [int(t) for t in c.cmd("HPEXPIRETIME", k, "FIELDS", len(names), *names)] if names else []
        if times != w["times"]:
            bad.append("%s: deadlines differ" % k)
    print("CHECK %s %d keys %s" % ("OK" if not bad else "FAILED", len(want), bad[:5]))
