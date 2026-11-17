#!/usr/bin/env python3
"""xver.py <port> write|check <json>: write = load sets of every form and boundary,
plus removals and conversions, record their members; check = every key has the
recorded members (and nothing else is there)."""
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
words = lambda n, p="membr": ["%s:%08d" % (p, i) for i in range(n)]
ints = lambda n: [str(i * 7 - 300) for i in range(n)]
if mode == "write":
    sets = {
        "ints10": ints(10), "ints512": ints(512), "ints513": ints(513),
        "words10": words(10), "words128": words(128), "words129": words(129),
        "long64": words(5) + ["L" * 64], "long65": words(5) + ["L" * 65],
        "mixed": ints(10) + ["x"], "lookalikes": ["1", "2", "01", "-0", "+1"],
        "edges": [str(-2**63), "-1", "0", str(2**63 - 1), ""],
        "big": words(5000, "big"),
    }
    for key, members in sets.items():
        for i in range(0, len(members), 100):
            c.cmd("SADD", key, *members[i:i + 100])
    # Removals and conversions after the adds: in the AOF as they happened.
    c.cmd("SREM", "words10", "membr:00000003")
    c.cmd("SADD", "ints10", "now-a-word")
    c.cmd("SREM", "ints512", "-300", "-293")
    for i in range(130):
        c.cmd("SADD", "grown", "g%d" % i)
    for i in range(100):
        c.cmd("SREM", "grown", "g%d" % i)
    c.cmd("SINTERSTORE", "inter", "words128", "words129")
    c.cmd("SUNIONSTORE", "union", "ints10", "mixed")
    keys = sorted(k.decode() for k in c.cmd("KEYS", "*"))
    want = {}
    for k in keys:
        want[k] = sorted(m.decode("latin1") for m in c.cmd("SMEMBERS", k))
    json.dump(want, open(path, "w"))
    print("WROTE %d keys, %d members" % (len(want), sum(map(len, want.values()))))
else:
    want = json.load(open(path))
    keys = sorted(k.decode() for k in c.cmd("KEYS", "*"))
    bad = []
    if keys != sorted(want):
        bad.append("keys differ: %s" % sorted(set(keys) ^ set(want)))
    for k, members in want.items():
        got = sorted(m.decode("latin1") for m in c.cmd("SMEMBERS", k))
        if got != members:
            bad.append("%s: %d members, want %d" % (k, len(got), len(members)))
    print("CHECK %s %d keys %s" % ("OK" if not bad else "FAILED", len(want), bad[:5]))
