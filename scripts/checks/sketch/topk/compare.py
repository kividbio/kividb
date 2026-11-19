#!/usr/bin/env python3
"""compare.py <redis-port> <kividb-port> [--seed N] [--steps N] [--sequences N]
       compare.py <redis-port> --capture OUT.json [--seed N] [--steps N] [--sequences N]
       compare.py <redis-port> <kividb-port> --quality

Top-K (`TOPK.*`): randomised command sequences sent to a Redis server (8.10.2)
and to KiviDB, replies compared byte for byte, RESP2 on database 0 and RESP3 on
database 1. Most sketches are made with a decay of 1, where nothing is left to
chance and every answer (items expelled, `TOPK.LIST`, `TOPK.COUNT`,
`TOPK.QUERY`) must be Redis's exactly; sketches with a decay below 1 (keys
`r:*`) draw their chances from different generators on each side, so their
replies are compared by shape (type and length) only. Errors, arity, wrong
types, missing keys, `WATCH` on failed writes, `INFO`, `TYPE`, `OBJECT
ENCODING` are compared exactly.

Redis 8.10.2 never finishes its reply to a `TOPK.INCRBY` whose bad increment is
not the last one (the client waits for ever), so the sequences put a bad
increment only last.

After the sequences, counts at the top of 32 bits: 42,949 `TOPK.INCRBY key x
100000` on sketches of k 1 and 2, decays 1 and 0.9, then more; every reply
compared exactly (a count past 2^32 - 1 wraps, in Redis as here).

--quality: Zipf streams (s = 0.8 to 1.2, 200,000 items over 20,000 distinct)
added to both with the default decay of 0.9, and the share of the true top k
each lists (recall), and how close its counts are, printed side by side.

With --capture, Redis's replies are written as the fixture tests/topk.rs
replays (exact cases only)."""
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


def shape(reply):
    """Type and length of a reply, not its contents."""
    t = reply[:1]
    if t in b"*%~":
        return t + reply.split(b"\r\n", 1)[0][1:]
    return t


# Items with a zero byte are left out: Redis cuts an item at its first zero
# byte when it names it in a reply (it keeps C strings); KiviDB does not.
ITEMS = [b"a", b"b", b"c", b"d", b"e", b"item:1", b"item:2", b"", "été".encode(), b"x" * 30]
KEYS = ["t1", "t2", "t3", "r:1", "s", "missing"]
GOOD_INC = ["0", "1", "1", "2", "3", "5", "100000"]
BAD_INC = ["-1", "100001", "abc", "1.5", "+1", "01", "4294967296", ""]
INTS = ["1", "2", "3", "4", "7", "8", "0", "-1", "abc", "4294967295x", "+2", "02", "4294967296"]
# Valid decays below 1 go only to the `r:` keys, compared by shape.
DECAYS = ["1", "1.0", "0x1p0", "0", "-0.5", "1.0000001", "nan", "inf", "abc", " 1", "1e999"]


def gen(rng, steps):
    cmds = []
    for _ in range(steps):
        k = rng.choice(KEYS)
        op = rng.random()
        if op < 0.12:
            if k.startswith("r:"):
                decay = rng.choice(["0.9", "0.5", "0.99"])
            elif rng.random() < 0.85:
                decay = "1"
            else:
                decay = rng.choice(DECAYS)
            if rng.random() < 0.15:
                # The defaults have a decay of 0.9: only for the `r:` keys.
                bare = rng.choice(INTS) if k.startswith("r:") else rng.choice(["0", "-1", "abc", "+2", "02", "4294967296"])
                cmds.append(["TOPK.RESERVE", k, bare] + ([] if rng.random() < 0.5 else [rng.choice(INTS)] * rng.randint(1, 4)))
            else:
                cmds.append(["TOPK.RESERVE", k, str(rng.randint(1, 5)), str(rng.randint(1, 6)), str(rng.randint(1, 3)), decay])
        elif op < 0.4:
            cmds.append(["TOPK.ADD", k] + [rng.choice(ITEMS) for _ in range(rng.randint(1, 6))])
        elif op < 0.6:
            args = ["TOPK.INCRBY", k]
            for _ in range(rng.randint(1, 4)):
                args += [rng.choice(ITEMS), rng.choice(GOOD_INC[:6])]
            if rng.random() < 0.1:
                args[-1] = rng.choice(BAD_INC)
            if rng.random() < 0.03:
                args.pop()
            cmds.append(args)
        elif op < 0.7:
            cmds.append(["TOPK.QUERY", k] + [rng.choice(ITEMS) for _ in range(rng.randint(1, 5))])
        elif op < 0.8:
            cmds.append(["TOPK.COUNT", k] + [rng.choice(ITEMS) for _ in range(rng.randint(1, 5))])
        elif op < 0.88:
            cmds.append(["TOPK.LIST", k] + rng.choice([[], ["WITHCOUNT"], ["withcount"], ["FOO"], ["WITHCOUNT", "x"]]))
        elif op < 0.91:
            cmds.append(["TOPK.INFO", k] + (["x"] if rng.random() < 0.1 else []))
        elif op < 0.93:
            cmds.append(["SET", "s", "v"])
        elif op < 0.95:
            cmds.append(["DEL", k])
        elif op < 0.96:
            cmds.append(rng.choice([["TYPE", k], ["OBJECT", "ENCODING", k], ["EXISTS", k]]))
        elif op < 0.98:
            name = rng.choice(["TOPK.RESERVE", "TOPK.ADD", "TOPK.INCRBY", "TOPK.QUERY", "TOPK.COUNT", "TOPK.LIST", "TOPK.INFO"])
            cmds.append([name] + [k] * rng.randint(0, 1))
        else:
            cmds.append(["#WATCH", k, rng.choice([
                ["TOPK.ADD", k, "a"], ["TOPK.INCRBY", k, "a", "-1"], ["TOPK.RESERVE", k, "3", "8", "7", "1"],
                ["TOPK.QUERY", k, "a"], ["TOPK.LIST", k], ["TOPK.ADD", "missing", "a"]])])
    return cmds


def exact(cmd):
    """Whether the reply cannot depend on chance."""
    if cmd[0] == "#WATCH":
        return True
    key = cmd[1] if len(cmd) > 1 else ""
    if not key.startswith("r:"):
        return True
    return cmd[0] in ("TOPK.RESERVE", "TOPK.INFO", "TYPE", "OBJECT", "EXISTS", "DEL")


def loose(cmd):
    """Whether only the reply's type can be compared: a list of a sketch left
    to chance."""
    return cmd[0] == "TOPK.LIST" and len(cmd) > 1 and cmd[1].startswith("r:")


def run(conns, cmd):
    if cmd[0] == "#WATCH":
        out = []
        for c, c2 in conns:
            c.raw("WATCH", cmd[1])
            c2.raw(*cmd[2])
            c.raw("MULTI")
            c.raw("PING")
            out.append(c.raw("EXEC"))
        return out
    return [c.raw(*cmd) for c, _ in conns]


def zipf_stream(rng, s, n, distinct):
    weights = [1 / (r ** s) for r in range(1, distinct + 1)]
    return rng.choices(range(distinct), weights=weights, k=n)


def quality(ports):
    rng = random.Random(42)
    print("s     k   redis recall  kividb recall  redis mean err  kividb mean err")
    for s in (0.8, 1.0, 1.2):
        stream = zipf_stream(rng, s, 200_000, 20_000)
        truth = {}
        for x in stream:
            truth[x] = truth.get(x, 0) + 1
        for k in (10, 50, 100):
            top = sorted(truth, key=lambda x: -truth[x])[:k]
            row = []
            errs = []
            for p in ports:
                c = Conn(p, db=2)
                c.raw("DEL", "q")
                c.raw("TOPK.RESERVE", "q", k, max(8, k * 8), 7, "0.9")
                for i in range(0, len(stream), 1000):
                    c.raw("TOPK.ADD", "q", *[b"z%d" % x for x in stream[i:i + 1000]])
                listed = c.raw("TOPK.LIST", "q", "WITHCOUNT").split(b"\r\n")[1:-1]
                names = [int(listed[i + 1][1:]) for i in range(0, len(listed), 3)]
                counts = [int(listed[i + 2][1:]) for i in range(0, len(listed), 3)]
                row.append(len(set(names) & set(top)) / k)
                errs.append(sum(abs(cn - truth[n]) / truth[n] for n, cn in zip(names, counts)) / max(1, len(names)))
            print(f"{s:<4}  {k:<3} {row[0]:>12.3f} {row[1]:>14.3f} {errs[0]:>15.4f} {errs[1]:>16.4f}")


def at_the_top(ports, diffs):
    """Counts taken past 2^32 - 1; the replies compared exactly, appended to
    `diffs` where they differ. Returns how many were compared."""
    compared = 0
    for key, shape in (("top1", (1, 1, 1, 1)), ("top2", (2, 1, 1, 1)), ("top3", (2, 2, 3, "0.9"))):
        conns = [Conn(p, db=3) for p in ports]
        for c in conns:
            c.raw("DEL", key)
            c.raw("TOPK.RESERVE", key, *shape)
            one = b"*4\r\n$11\r\nTOPK.INCRBY\r\n$%d\r\n%s\r\n$1\r\nx\r\n$6\r\n100000\r\n" % (len(key), key.encode())
            c.s.sendall(one * 42_949)
            for _ in range(42_949):
                c.read()
        for cmd in (["TOPK.LIST", key, "WITHCOUNT"], ["TOPK.COUNT", key, "x"],
                    ["TOPK.INCRBY", key, "x", "100000"], ["TOPK.LIST", key, "WITHCOUNT"],
                    ["TOPK.COUNT", key, "x"], ["TOPK.QUERY", key, "x"],
                    ["TOPK.INCRBY", key, "y", "5", "x", "70000"], ["TOPK.LIST", key, "WITHCOUNT"],
                    ["TOPK.COUNT", key, "x", "y"]):
            replies = [c.raw(*cmd) for c in conns]
            compared += 1
            if replies[0] != replies[1]:
                diffs.append(("top", False, cmd, replies[0], replies[1]))
    return compared


def main():
    argv = sys.argv[1:]
    opts = {"--seed": "1", "--steps": "400", "--sequences": "20", "--capture": None}
    flags = set()
    pos = []
    while argv:
        a = argv.pop(0)
        if a in opts:
            opts[a] = argv.pop(0)
        elif a.startswith("--"):
            flags.add(a)
        else:
            pos.append(a)
    ports = [int(p) for p in pos]
    if "--quality" in flags:
        quality(ports)
        return 0
    seed, steps, seqs = int(opts["--seed"]), int(opts["--steps"]), int(opts["--sequences"])
    capture = opts["--capture"]
    diffs, total, by_shape = [], 0, 0
    fixture = {"sequences": []}
    for s in range(seqs):
        rng = random.Random(seed * 1000 + s)
        cmds = gen(rng, steps)
        cases = []
        for resp3, db in ((False, 0), (True, 1)):
            conns = [(Conn(p, resp3, db), Conn(p, resp3, db)) for p in ports]
            for c, _ in conns:
                c.raw("FLUSHDB")
            kept = 0
            for cmd in cmds:
                replies = run(conns, cmd)
                total += 1
                if capture:
                    if not exact(cmd):
                        continue
                    if not resp3:
                        args = cmd if cmd[0] != "#WATCH" else ["#WATCH", cmd[1]] + cmd[2]
                        cases.append({"args": [a.hex() if isinstance(a, bytes) else a.encode().hex() for a in args],
                                      "resp2": replies[0].hex()})
                    else:
                        cases[kept]["resp3"] = replies[0].hex()
                    kept += 1
                elif exact(cmd):
                    if replies[0] != replies[1]:
                        diffs.append((s, resp3, cmd, replies[0], replies[1]))
                else:
                    by_shape += 1
                    a, b = (r[:1] for r in replies) if loose(cmd) else map(shape, replies)
                    if a != b:
                        diffs.append((s, resp3, cmd, replies[0], replies[1]))
        if capture:
            fixture["sequences"].append({"seed": seed * 1000 + s, "cases": cases})
    if capture:
        json.dump(fixture, open(capture, "w"), indent=0)
        print(f"captured {sum(len(q['cases']) for q in fixture['sequences'])} cases")
        return 0
    total += at_the_top(ports, diffs)
    for s, resp3, cmd, a, b in diffs[:40]:
        print(f"seq {s} RESP{3 if resp3 else 2}: {cmd!r}\n  redis  {a!r}\n  kividb {b!r}")
    print(f"{total} replies compared ({by_shape} by shape), {len(diffs)} differ")
    return 1 if diffs else 0


if __name__ == "__main__":
    sys.exit(main())
