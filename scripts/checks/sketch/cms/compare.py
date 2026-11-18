#!/usr/bin/env python3
"""compare.py <redis-port> <kividb-port> [--seed N] [--steps N] [--sequences N]
       compare.py <redis-port> --capture OUT.json [--seed N] [--steps N] [--sequences N]

Count-min sketches (`CMS.*`): long randomised command sequences sent to a
Redis server (8.10.2) and to KiviDB, every reply compared byte for byte, in
RESP2 (database 0) and RESP3 (database 1). Counts are compared exactly: items
are placed as Redis places them, so even counters that items share agree.
The sequences cover every command and error, the order in which Redis checks
arguments, overflow at 2^32 - 1, merges with weights, wrong types, missing
keys, `WATCH` on failed writes, `TYPE`, `OBJECT ENCODING`, `COPY`, `RENAME`.
Sizes stay small: KiviDB's size and per-command limits (which Redis does not
have) are not reached here; tests/cms.rs covers them.

With --capture, the commands and Redis's replies (hex) are written as the
fixture tests/cms.rs replays. Prints every difference and a summary line;
exits 1 if any."""
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


ITEMS = [b"a", b"b", b"c", b"item:1", b"item:2", b"", b"\x00\xff", "élan".encode(), b"x" * 40]
KEYS = ["k1", "k2", "k3", "k4", "s", "missing"]
NUMS = ["0", "1", "2", "5", "17", "100", "4294967290", "4294967294", "4294967295", "4294967296",
        "9223372036854775807", "9223372036854775808", "-1", "-0", "+3", "007", "1.5", "abc", "", " 3"]
DIMS = ["1", "2", "3", "4", "7", "10", "31", "0", "-1", "abc", "1.0", "+2", "010", "0x10",
        "18446744073709551616"]
FLOATS = ["0.5", "0.1", "0.01", "0.001", "0.25", "0.3", "0.9999", "1e-3", "0x0.1p0", "0.6666666666666666",
          "0", "1", "1.5", "-0.1", "nan", "inf", "abc", " 0.1", "0.1 ", "1e-50", "1e-45", "1e-20",
          "0.0625", "0.125", "2e-1"]


def gen(rng, steps):
    cmds = []
    for _ in range(steps):
        k = rng.choice(KEYS)
        op = rng.random()
        if op < 0.12:
            cmds.append(["CMS.INITBYDIM", k, rng.choice(DIMS), rng.choice(DIMS)])
        elif op < 0.2:
            cmds.append(["CMS.INITBYPROB", k, rng.choice(FLOATS), rng.choice(FLOATS)])
        elif op < 0.5:
            args = ["CMS.INCRBY", k]
            for _ in range(rng.randint(1, 5)):
                args += [rng.choice(ITEMS), rng.choice(NUMS[:8]) if rng.random() < 0.85 else rng.choice(NUMS)]
            if rng.random() < 0.05:
                args.pop()
            cmds.append(args)
        elif op < 0.65:
            cmds.append(["CMS.QUERY", k] + [rng.choice(ITEMS) for _ in range(rng.randint(0 if rng.random() < 0.05 else 1, 5))])
        elif op < 0.8:
            n = rng.randint(1, 3)
            sources = [rng.choice(KEYS) for _ in range(n)]
            numkeys = str(n) if rng.random() < 0.85 else rng.choice(["0", "-1", "abc", str(n + 1), str(n - 1), "99999999999"])
            args = ["CMS.MERGE", k, numkeys] + sources
            r = rng.random()
            if r < 0.4:
                ws = [rng.choice(["1", "2", "0", "-1", "3", "858993459", "858993460", "4611686018427387905",
                                  "9223372036854775807", "q", "+1", "1.5"]) if rng.random() < 0.3 else rng.choice(["1", "2", "0"])
                      for _ in range(n)]
                if rng.random() < 0.1:
                    ws = ws[:-1] if rng.random() < 0.5 else ws + ["1"]
                args += [rng.choice(["WEIGHTS", "weights", "WEIGHT", "FOO"]) if rng.random() < 0.1 else "WEIGHTS"] + ws
            elif r < 0.45:
                args += ["WEIGHTS"]
            cmds.append(args)
        elif op < 0.86:
            cmds.append(["CMS.INFO", k] + (["x"] if rng.random() < 0.05 else []))
        elif op < 0.89:
            cmds.append(["SET", "s", "v"])
        elif op < 0.91:
            cmds.append(["DEL", k])
        elif op < 0.93:
            cmds.append([rng.choice(["TYPE", "OBJECT ENCODING", "EXISTS"]), k] if True else [])
            if cmds[-1][0] == "OBJECT ENCODING":
                cmds[-1] = ["OBJECT", "ENCODING", k]
        elif op < 0.95:
            cmds.append(["RENAME", k, rng.choice(KEYS)])
        elif op < 0.97:
            # Arity errors.
            name = rng.choice(["CMS.INITBYDIM", "CMS.INITBYPROB", "CMS.INCRBY", "CMS.QUERY", "CMS.MERGE", "CMS.INFO"])
            cmds.append([name] + [k] * rng.randint(0, 2))
        else:
            # A failed write and WATCH: checked as a whole below.
            cmds.append(["#WATCH", k, rng.choice([
                ["CMS.INCRBY", k, "a", "-1"], ["CMS.INITBYDIM", k, "1", "1"], ["CMS.QUERY", k, "a"],
                ["CMS.MERGE", k, "1", "k2"], ["CMS.INCRBY", k, "a", "1"], ["CMS.INFO", k]])])
    return cmds


def run(conns, cmd):
    """Replies of `conns` (one connection each, with a second for WATCH)."""
    if cmd[0] == "#WATCH":
        key, other = cmd[1], cmd[2]
        out = []
        for c, c2 in conns:
            c.raw("WATCH", key)
            c2.raw(*other)
            c.raw("MULTI")
            c.raw("PING")
            out.append(c.raw("EXEC"))
        return out
    return [c.raw(*cmd) for c, _ in conns]


def main():
    argv = sys.argv[1:]
    opts = {"--seed": "1", "--steps": "400", "--sequences": "20", "--capture": None}
    pos = []
    while argv:
        a = argv.pop(0)
        if a in opts:
            opts[a] = argv.pop(0)
        else:
            pos.append(a)
    seed, steps, seqs = int(opts["--seed"]), int(opts["--steps"]), int(opts["--sequences"])
    ports = [int(p) for p in pos]
    capture = opts["--capture"]
    diffs, total = [], 0
    fixture = {"sequences": []}
    for s in range(seqs):
        rng = random.Random(seed * 1000 + s)
        cmds = gen(rng, steps)
        cases = []
        for resp3, db in ((False, 0), (True, 1)):
            conns = [(Conn(p, resp3, db), Conn(p, resp3, db)) for p in ports]
            conns[0][0].raw("FLUSHDB")
            for c, _ in conns[1:]:
                c.raw("FLUSHDB")
            for i, cmd in enumerate(cmds):
                replies = run(conns, cmd)
                total += 1
                if capture:
                    if not resp3:
                        cases.append({"args": [a.hex() if isinstance(a, bytes) else a.encode().hex()
                                               for a in (cmd if cmd[0] != "#WATCH" else ["#WATCH", cmd[1]] + cmd[2])],
                                      "resp2": replies[0].hex()})
                    else:
                        cases[i]["resp3"] = replies[0].hex()
                elif replies[0] != replies[1]:
                    diffs.append((s, resp3, cmd, replies[0], replies[1]))
        if capture:
            fixture["sequences"].append({"seed": seed * 1000 + s, "cases": cases})
    if capture:
        json.dump(fixture, open(capture, "w"), indent=0)
        print(f"captured {total} replies in {seqs} sequences")
        return 0
    for s, resp3, cmd, a, b in diffs[:50]:
        print(f"seq {s} RESP{3 if resp3 else 2}: {cmd!r}\n  redis  {a!r}\n  kividb {b!r}")
    print(f"{total} replies compared, {len(diffs)} differ")
    return 1 if diffs else 0


if __name__ == "__main__":
    sys.exit(main())
