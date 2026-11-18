#!/usr/bin/env python3
"""xver.py <port> write|check <json>: write = load lists of every shape (one chunk,
many chunks, irregular after inserts and removals in the middle, an element over 1,024
bytes, the longest that packs, empty and binary elements, one of 300,000 elements) with
pushes, pops, trims, sets, inserts and removals at both ends, and record their elements;
check = every key holds exactly what was recorded, in order (and nothing else is there)."""
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
        if t in (b"+", b":"):
            return rest.decode()
        if t == b"-":
            raise RuntimeError(rest.decode())
        if t == b"$":
            n = int(rest)
            return None if n < 0 else self.f.read(n + 2)[:-2]
        if t in (b"*", b"~"):
            return [self.read() for _ in range(int(rest))]
        raise RuntimeError(line)


port, mode, path = int(sys.argv[1]), sys.argv[2], sys.argv[3]
c = C(port)


def el(i, p="element"):
    return "%s:%024d" % (p, i)


def push(key, items, cmd="RPUSH"):
    for i in range(0, len(items), 200):
        c.cmd(cmd, key, *items[i:i + 200])


if mode == "write":
    push("one", [el(i) for i in range(10)])
    push("sixty-four", [el(i) for i in range(64)])
    push("sixty-five", [el(i) for i in range(65)])
    push("many", [el(i) for i in range(1000)])
    push("lmany", [el(i) for i in range(1000)], "LPUSH")
    push("irregular", [el(i) for i in range(1000)])
    for j in range(0, 1000, 97):
        c.cmd("LINSERT", "irregular", "BEFORE", el(j), "ins%d" % j)
    c.cmd("LREM", "irregular", 0, el(500))
    for j in range(200, 260):
        c.cmd("LREM", "irregular", 1, el(j))
    push("plain", [el(i) for i in range(50)] + ["L" * 2000] + [el(i) for i in range(50, 100)])
    push("plain-after", ["L" * 5000] + [el(i) for i in range(30)])
    c.cmd("LPOP", "plain-after")
    push("longest", [el(i) for i in range(5)] + ["l" * 1024, "", "o" * 1023])
    push("binary", [bytes([i, 0, 255 - i, 13, 10]) for i in range(256)])
    push("empties", ["", "", "x", ""])
    push("big", [el(i, "big") for i in range(300_000)])
    # Writes after the loads: in the AOF as they happened.
    for i in range(500):
        c.cmd("LPUSH", "capped", el(i))
        c.cmd("LTRIM", "capped", 0, 99)
    for i in range(400):
        c.cmd("RPUSH", "queue", el(i))
        if i >= 30:
            c.cmd("LPOP", "queue")
    c.cmd("LSET", "many", 500, "set in the middle, longer than before")
    c.cmd("LSET", "many", 0, "s")
    c.cmd("LINSERT", "one", "AFTER", el(4), "after four")
    c.cmd("LMOVE", "many", "one", "RIGHT", "LEFT")
    c.cmd("RPOPLPUSH", "lmany", "lmany")
    c.cmd("LSET", "sixty-five", 10, "S" * 1500)  # a conversion, as it happened
    c.cmd("LTRIM", "big", 100, -100)
    c.cmd("RPUSH", "gone", "a")
    c.cmd("LPOP", "gone")
    keys = sorted(k.decode() for k in c.cmd("KEYS", "*"))
    want = {}
    for k in keys:
        want[k] = [e.decode("latin1") for e in c.cmd("LRANGE", k, 0, -1)]
    json.dump(want, open(path, "w"))
    print("WROTE %d keys, %d elements" % (len(want), sum(len(v) for v in want.values())))
else:
    want = json.load(open(path))
    keys = sorted(k.decode() for k in c.cmd("KEYS", "*"))
    bad = []
    if keys != sorted(want):
        bad.append("keys differ: %s" % sorted(set(keys) ^ set(want)))
    for k, w in want.items():
        got = [e.decode("latin1") for e in (c.cmd("LRANGE", k, 0, -1) or [])]
        if got != w:
            bad.append("%s: %d elements, want %d" % (k, len(got), len(w)))
    print("CHECK %s %d keys %s" % ("OK" if not bad else "FAILED", len(want), bad[:5]))
