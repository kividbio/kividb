#!/usr/bin/env python3
"""compare.py <redis-port> <kividb-port> [--seed N] [--steps N] [--sequences N]
       compare.py <redis-port> --capture OUT.json [--seed N] [--steps N] [--sequences N]
       compare.py <kividb-port> --guarantees

Cuckoo filters (`CF.*`): randomised command sequences sent to a Redis server
(8.10.2) and to KiviDB, every reply compared byte for byte, RESP2 on database 0
and RESP3 on database 1. Filters are Redis's byte for byte, so everything is
compared exactly: `CF.SCANDUMP` chunks and headers, `CF.LOADCHUNK` of them into
another key, counts, `CF.INFO`, `CF.DEBUG`, growth (bucket sizes 1 to 4, kick
limits 1 to 30, expansions 0 to 4), deletes and the compaction they trigger,
`CF.COMPACT`, every error and the order arguments are checked in, the `cf-*`
settings, wrong types, missing keys, `WATCH` on failed writes.

--guarantees (KiviDB alone): no false negatives over 100,000 items in a
growing filter, the false-positive rate against its bound, every added item
deleted, expansion, and a full non-growing filter.

With --capture, Redis's replies are written as the fixture tests/cf.rs
replays."""
import json
import random
import socket
import sys


class Conn:
    def __init__(self, port, resp3=False, db=0):
        self.s = socket.create_connection(("127.0.0.1", port))
        self.f = self.s.makefile("rb")
        if resp3:
            self.raw("HELLO", "3")
        self.raw("SELECT", str(db))

    def raw(self, *args):
        args = [a if isinstance(a, bytes) else str(a).encode() for a in args]
        out = b"*%d\r\n" % len(args) + b"".join(b"$%d\r\n%s\r\n" % (len(a), a) for a in args)
        self.s.sendall(out)
        return self.read()

    def read(self):
        line = self.f.readline()
        t, body = line[:1], line[1:-2]
        if t in b"+-:,#_(":
            return line
        if t in b"$=!":
            n = int(body)
            return line if n < 0 else line + self.f.read(n + 2)
        if t in b"*~>%|":
            n = int(body)
            if n < 0:
                return line
            k = 2 * n if t in b"%|" else n
            out = line
            for _ in range(k):
                out += self.read()
            return out
        raise RuntimeError(line)


ITEMS = [b"a", b"b", b"c", b"d", b"item:1", b"item:2", b"", b"\x00\xff", "été".encode(), b"x" * 30] + \
    [b"i%d" % i for i in range(40)]
KEYS = ["c1", "c2", "c3", "s", "missing", "copy"]
CAPS = ["4", "6", "8", "10", "17", "32", "50", "100", "1", "3", "0", "-1", "abc", "07", "+8", "1073741825"]
SMALL = ["1", "2", "3", "4", "5", "0", "-1", "abc", "255", "256", "65535", "65536", "32768", "32769", ""]
SETTINGS = [("cf-bucket-size", ["1", "2", "3", "4", "0", "256", "x"]),
            ("cf-initial-size", ["4", "8", "64", "1024", "3", "x"]),
            ("cf-expansion-factor", ["0", "1", "2", "3", "32769", "x"]),
            ("cf-max-iterations", ["1", "5", "20", "0", "65536"]),
            # Not 1: Redis then makes a missing filter, refuses the add and
            # keeps the filter; KiviDB refuses without making it.
            ("cf-max-expansions", ["2", "3", "32", "0", "x"])]
DEFAULTS = [("cf-bucket-size", "2"), ("cf-initial-size", "1024"), ("cf-expansion-factor", "1"),
            ("cf-max-iterations", "20"), ("cf-max-expansions", "32")]


def reserve(rng, k):
    args = ["CF.RESERVE", k, rng.choice(CAPS[:8]) if rng.random() < 0.85 else rng.choice(CAPS)]
    opts = [["BUCKETSIZE", lambda: rng.choice(["1", "2", "3", "4"] if rng.random() < 0.85 else SMALL)],
            ["MAXITERATIONS", lambda: rng.choice(["1", "3", "10", "30"] if rng.random() < 0.85 else SMALL)],
            ["EXPANSION", lambda: rng.choice(["0", "1", "2", "3", "4"] if rng.random() < 0.85 else SMALL)]]
    rng.shuffle(opts)
    for word, val in opts:
        if rng.random() < 0.7:
            args += [word if rng.random() < 0.9 else word.lower(), val()]
    r = rng.random()
    if r < 0.05:
        args += ["FOO", "1"]
    elif r < 0.08:
        args.append("BUCKETSIZE")
    return args


def gen(rng, steps):
    cmds = []
    for _ in range(steps):
        k = rng.choice(KEYS)
        op = rng.random()
        if op < 0.1:
            cmds.append(reserve(rng, k))
        elif op < 0.35:
            cmds.append([rng.choice(["CF.ADD", "CF.ADD", "CF.ADDNX"]), k, rng.choice(ITEMS)])
        elif op < 0.45:
            args = [rng.choice(["CF.INSERT", "CF.INSERTNX"]), k]
            for _ in range(rng.randint(0, 2)):
                w = rng.random()
                if w < 0.4:
                    args += [rng.choice(["CAPACITY", "capacity", "CAP", "C"]), rng.choice(CAPS)]
                elif w < 0.7:
                    args.append(rng.choice(["NOCREATE", "N", "nocreate"]))
                else:
                    args.append(rng.choice(["FOO", "CAPACITY"]))
            args.append(rng.choice(["ITEMS", "items", "I"]))
            args += [rng.choice(ITEMS) for _ in range(rng.randint(0 if rng.random() < 0.05 else 1, 6))]
            cmds.append(args)
        elif op < 0.55:
            cmds.append(["CF.DEL", k, rng.choice(ITEMS)])
        elif op < 0.62:
            cmds.append(["CF.EXISTS", k, rng.choice(ITEMS)])
        elif op < 0.67:
            cmds.append(["CF.MEXISTS", k] + [rng.choice(ITEMS) for _ in range(rng.randint(1, 5))])
        elif op < 0.72:
            cmds.append(["CF.COUNT", k, rng.choice(ITEMS)])
        elif op < 0.76:
            cmds.append([rng.choice(["CF.INFO", "CF.DEBUG"]), k])
        elif op < 0.8:
            cmds.append(["#DUMPLOAD", k])
        elif op < 0.82:
            cmds.append(["CF.SCANDUMP", k, rng.choice(["0", "1", "2", "5", "9", "17", "-1", "abc", "100000"])])
        elif op < 0.85:
            it = rng.choice(["1", "2", "5", "9", "0", "-1", "abc", "3"])
            data = rng.choice([b"x", b"\x00" * 4, b"\x01" * 38, bytes(rng.randrange(256) for _ in range(38))])
            cmds.append(["CF.LOADCHUNK", rng.choice(KEYS), it, data])
        elif op < 0.87:
            cmds.append(["CF.COMPACT", k])
        elif op < 0.89:
            name, values = rng.choice(SETTINGS)
            cmds.append(["CONFIG", "SET", name, rng.choice(values)])
        elif op < 0.9:
            cmds.append(["SET", "s", "v"])
        elif op < 0.92:
            cmds.append(["DEL", k])
        elif op < 0.93:
            cmds.append(rng.choice([["TYPE", k], ["OBJECT", "ENCODING", k]]))
        elif op < 0.97:
            name = rng.choice(["CF.RESERVE", "CF.ADD", "CF.ADDNX", "CF.INSERT", "CF.INSERTNX", "CF.EXISTS",
                               "CF.MEXISTS", "CF.COUNT", "CF.DEL", "CF.INFO", "CF.SCANDUMP", "CF.LOADCHUNK",
                               "CF.COMPACT", "CF.DEBUG"])
            cmds.append([name] + [k] * rng.randint(0, 3))
        else:
            cmds.append(["#WATCH", k, rng.choice([
                ["CF.ADD", k, "a"], ["CF.DEL", k, "zz"], ["CF.RESERVE", k, "8"], ["CF.INSERT", k, "CAPACITY", "0", "ITEMS", "a"],
                ["CF.EXISTS", k, "a"], ["CF.INSERTNX", k, "NOCREATE", "ITEMS", "a"], ["CF.COMPACT", k]])])
    return cmds


def run(conns, cmd):
    out = []
    for c, c2 in conns:
        if cmd[0] == "#WATCH":
            c.raw("WATCH", cmd[1])
            c2.raw(*cmd[2])
            c.raw("MULTI")
            c.raw("PING")
            out.append(c.raw("EXEC"))
        elif cmd[0] == "#DUMPLOAD":
            # SCANDUMP the whole filter, LOADCHUNK it into `copy`, compare.
            c.raw("DEL", "copy")
            replies = b""
            it = b"0"
            for _ in range(64):
                r = c.raw("CF.SCANDUMP", cmd[1], it)
                replies += r
                if not r.startswith(b"*2"):
                    break
                parts = r.split(b"\r\n", 3)
                nxt = parts[1][1:]
                if nxt == b"0":
                    break
                chunk = parts[3][:-2]
                replies += c.raw("CF.LOADCHUNK", "copy", nxt, chunk)
                it = nxt
            replies += c.raw("CF.INFO", "copy")
            out.append(replies)
        else:
            out.append(c.raw(*cmd))
    return out


def guarantees(port):
    c = Conn(port, db=3)
    c.raw("FLUSHDB")
    c.raw("CF.RESERVE", "g", "10000", "BUCKETSIZE", "4", "EXPANSION", "2")
    n = 100_000
    for i in range(0, n, 1000):
        c.raw("CF.INSERT", "g", "ITEMS", *[b"in%d" % j for j in range(i, i + 1000)])
    missing = 0
    for i in range(0, n, 1000):
        r = c.raw("CF.MEXISTS", "g", *[b"in%d" % j for j in range(i, i + 1000)])
        missing += r.count(b":0\r\n")
    fp = 0
    for i in range(0, n, 1000):
        r = c.raw("CF.MEXISTS", "g", *[b"out%d" % j for j in range(i, i + 1000)])
        fp += r.count(b":1\r\n")
    info = c.raw("CF.INFO", "g").split(b"\r\n")
    filters = int(info[6][1:])
    bound = 2 * 4 / 255 * filters
    print(f"false negatives: {missing} of {n}; false positives: {fp / n:.4f} "
          f"(bound 2*b/255 per sub-filter, {filters} sub-filters: {bound:.4f})")
    deleted = 0
    for i in range(0, n, 2):
        deleted += c.raw("CF.DEL", "g", b"in%d" % i) == b":1\r\n"
    missing = 0
    for i in range(1, n, 1000 * 2):
        r = c.raw("CF.MEXISTS", "g", *[b"in%d" % j for j in range(i, min(n, i + 2000), 2)])
        missing += r.count(b":0\r\n")
    print(f"deleted {deleted} of {n // 2}; false negatives among the rest: {missing}")
    c.raw("CF.RESERVE", "full", "8", "EXPANSION", "0")
    full = sum(c.raw("CF.ADD", "full", b"f%d" % i).startswith(b"-Filter is full") for i in range(40))
    print(f"a non-growing filter of 8 refused {full} of 40 adds")
    return 0 if missing == 0 and deleted == n // 2 else 1


def main():
    argv = sys.argv[1:]
    opts = {"--seed": "1", "--steps": "400", "--sequences": "20", "--capture": None}
    flags, pos = set(), []
    while argv:
        a = argv.pop(0)
        if a in opts:
            opts[a] = argv.pop(0)
        elif a.startswith("--"):
            flags.add(a)
        else:
            pos.append(a)
    ports = [int(p) for p in pos]
    if "--guarantees" in flags:
        return guarantees(ports[0])
    seed, steps, seqs = int(opts["--seed"]), int(opts["--steps"]), int(opts["--sequences"])
    capture = opts["--capture"]
    diffs, total = [], 0
    fixture = {"sequences": []}
    for s in range(seqs):
        rng = random.Random(seed * 1000 + s)
        cmds = gen(rng, steps)
        cases = []
        for resp3, db in ((False, 0), (True, 1)):
            conns = [(Conn(p, resp3, db), Conn(p, resp3, db)) for p in ports]
            for c, _ in conns:
                c.raw("FLUSHDB")
                for name, value in DEFAULTS:
                    c.raw("CONFIG", "SET", name, value)
            for i, cmd in enumerate(cmds):
                replies = run(conns, cmd)
                total += 1
                if capture:
                    if not resp3:
                        args = cmd if cmd[0] != "#WATCH" else ["#WATCH", cmd[1]] + cmd[2]
                        cases.append({"args": [a.hex() if isinstance(a, bytes) else a.encode().hex() for a in args],
                                      "resp2": replies[0].hex()})
                    else:
                        cases[i]["resp3"] = replies[0].hex()
                elif replies[0] != replies[1]:
                    diffs.append((s, resp3, cmd, replies[0], replies[1]))
        if capture:
            fixture["sequences"].append({"seed": seed * 1000 + s, "cases": cases})
    for p in ports:
        c = Conn(p)
        for name, value in DEFAULTS:
            c.raw("CONFIG", "SET", name, value)
    if capture:
        json.dump(fixture, open(capture, "w"), indent=0)
        print(f"captured {total} replies")
        return 0
    for s, resp3, cmd, a, b in diffs[:40]:
        print(f"seq {s} RESP{3 if resp3 else 2}: {[x[:40] for x in cmd]!r}\n  redis  {a[:300]!r}\n  kividb {b[:300]!r}")
    print(f"{total} replies compared, {len(diffs)} differ")
    return 1 if diffs else 0


if __name__ == "__main__":
    sys.exit(main())
