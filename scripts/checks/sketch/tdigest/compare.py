#!/usr/bin/env python3
"""compare.py <redis-port> <kividb-port> [--seed N] [--steps N] [--sequences N]
       compare.py <redis-port> <kividb-port> --quality
       compare.py <redis-port> --capture OUT.json [--seed N] [--steps N] [--sequences N]

t-digests (`TDIGEST.*`): randomised command sequences sent to a Redis server
(8.10.2) and to KiviDB, RESP2 on database 0 and RESP3 on database 1. Errors,
arity, wrong types, missing keys, `WATCH`, `TYPE`, `OBJECT ENCODING`, and the
shape of every reply are compared exactly; so are the numbers of a digest
whose every centroid is still one value (few values: there is nothing to
approximate, and `nan`, `inf` and `-inf` must be spelled the same). Numbers
of larger digests are approximations on both sides and are compared within
the error t-digests make: quantiles and trimmed means by where they fall in
the data, CDFs within 0.02, ranks within 2% of the count. `TDIGEST.INFO`'s
merged and unmerged node counts and weights and its compressions count are
left out (Redis merges on reads; KiviDB never changes a digest on a read).

--quality: streams of 200,000 values (normal, uniform, lognormal, and sorted
ascending: the adversarial case) into digests of compression 100 on both;
for quantiles from 0.001 to 0.999, the error in rank of each side's answer
against the true quantile, side by side.

With --capture, Redis's replies to exact cases are written as the fixture
tests/tdigest.rs replays."""
import json
import math
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


def parse(b, i=0):
    """A reply as Python values: (value, next index)."""
    end = b.index(b"\r\n", i)
    t, body = b[i:i + 1], b[i + 1:end]
    i = end + 2
    if t == b"+":
        return ("s", body), i
    if t == b"-":
        return ("e", body), i
    if t == b":":
        return int(body), i
    if t == b",":
        return float(body), i
    if t == b"$":
        n = int(body)
        if n < 0:
            return None, i
        s = b[i:i + n]
        try:
            return float(s), i + n + 2
        except ValueError:
            return ("b", s), i + n + 2
    if t == b"_":
        return None, i
    if t in b"*%":
        n = int(body) * (2 if t == b"%" else 1)
        out = []
        for _ in range(n):
            v, i = parse(b, i)
            out.append(v)
        return out, i
    raise ValueError(b)


NUMS = ["1", "2", "3", "0", "-1", "2.5", "1e10", "-1e10", "1e-10", "100", "7", "0.5", "-3.25"]
BAD = ["abc", "nan", "inf", "-inf", "", "1e400", "0x10", " 1"]
QS = ["0", "0.1", "0.25", "0.5", "0.75", "0.9", "0.99", "1", "1.5", "-0.1", "abc", "nan"]
KEYS = ["d1", "d2", "d3", "s", "missing"]


def gen(rng, steps):
    cmds = []
    for _ in range(steps):
        k = rng.choice(KEYS)
        op = rng.random()
        if op < 0.08:
            args = ["TDIGEST.CREATE", k]
            r = rng.random()
            if r < 0.6:
                args += [rng.choice(["COMPRESSION", "compression"]), rng.choice(["20", "50", "100", "200", "1"])]
            elif r < 0.7:
                args += [rng.choice(["COMPRESSION", "FOO"]), rng.choice(["0", "-1", "abc", "1.5", "100", "1000000"])]
            elif r < 0.75:
                args += ["COMPRESSION"]
            cmds.append(args)
        elif op < 0.4:
            vals = [rng.choice(NUMS) if rng.random() < 0.3 else repr(round(rng.gauss(0, 10), 3)) for _ in range(rng.randint(1, 12))]
            if rng.random() < 0.05:
                vals[rng.randrange(len(vals))] = rng.choice(BAD)
            cmds.append(["TDIGEST.ADD", k] + vals)
        elif op < 0.48:
            cmds.append(["TDIGEST.QUANTILE", k] + [rng.choice(QS[:8]) if rng.random() < 0.95 else rng.choice(QS) for _ in range(rng.randint(1, 4))])
        elif op < 0.55:
            cmds.append(["TDIGEST.CDF", k] + [rng.choice(NUMS + ["5", "-5", "1e300"]) if rng.random() < 0.97 else "abc" for _ in range(rng.randint(1, 4))])
        elif op < 0.62:
            cmds.append([rng.choice(["TDIGEST.RANK", "TDIGEST.REVRANK"]), k] + [rng.choice(NUMS) if rng.random() < 0.97 else "abc" for _ in range(rng.randint(1, 4))])
        elif op < 0.68:
            cmds.append([rng.choice(["TDIGEST.BYRANK", "TDIGEST.BYREVRANK"]), k] + [rng.choice(["0", "1", "2", "5", "10", "100", "1000"]) if rng.random() < 0.95 else rng.choice(["-1", "1.5", "abc"]) for _ in range(rng.randint(1, 4))])
        elif op < 0.72:
            lo, hi = rng.choice([("0", "1"), ("0.1", "0.9"), ("0.25", "0.75"), ("0.5", "0.5"), ("0.9", "0.1"), ("-1", "1"), ("a", "b"), ("0", "b"), ("0.2", "1.1")])
            cmds.append(["TDIGEST.TRIMMED_MEAN", k, lo, hi])
        elif op < 0.76:
            cmds.append([rng.choice(["TDIGEST.MIN", "TDIGEST.MAX"]), k])
        elif op < 0.8:
            cmds.append(["TDIGEST.INFO", k])
        elif op < 0.84:
            n = rng.randint(1, 3)
            srcs = [rng.choice(KEYS) for _ in range(n)]
            numkeys = str(n) if rng.random() < 0.9 else rng.choice(["0", "-1", "abc", str(n + 3)])
            args = ["TDIGEST.MERGE", rng.choice(KEYS + ["new"]), numkeys] + srcs
            r = rng.random()
            if r < 0.2:
                args += ["COMPRESSION", rng.choice(["30", "100", "0", "abc"])]
            if rng.random() < 0.3:
                args += ["OVERRIDE"]
            if rng.random() < 0.03:
                args += ["FOO"]
            cmds.append(args)
        elif op < 0.86:
            cmds.append(["TDIGEST.RESET", k])
        elif op < 0.88:
            cmds.append(["SET", "s", "v"])
        elif op < 0.9:
            cmds.append(["DEL", k])
        elif op < 0.91:
            cmds.append(rng.choice([["TYPE", k], ["OBJECT", "ENCODING", k]]))
        elif op < 0.96:
            name = rng.choice(["TDIGEST.CREATE", "TDIGEST.ADD", "TDIGEST.MERGE", "TDIGEST.RESET", "TDIGEST.QUANTILE",
                               "TDIGEST.CDF", "TDIGEST.RANK", "TDIGEST.REVRANK", "TDIGEST.BYRANK", "TDIGEST.BYREVRANK",
                               "TDIGEST.MIN", "TDIGEST.MAX", "TDIGEST.TRIMMED_MEAN", "TDIGEST.INFO"])
            cmds.append([name] + [k] * rng.randint(0, 3))
        else:
            cmds.append(["#WATCH", k, rng.choice([["TDIGEST.ADD", k, "1"], ["TDIGEST.ADD", k, "x"], ["TDIGEST.CREATE", k],
                                                  ["TDIGEST.RESET", k], ["TDIGEST.MIN", k], ["TDIGEST.MERGE", k, "1", "d1"]])])
    return cmds


INFO_LOOSE = {b"Merged nodes", b"Unmerged nodes", b"Merged weight", b"Unmerged weight", b"Total compressions"}


import bisect


def truth_rank(srt, x):
    """Values below `x`, and half of those equal: what a rank means."""
    lo = bisect.bisect_left(srt, x)
    hi = bisect.bisect_right(srt, x)
    return lo + (hi - lo) / 2


def judge(cmd, va, vb, srt):
    """Whether KiviDB's numbers (`vb`) are as good as Redis's (`va`) against
    the true values `srt` (sorted): each error within 1.5 times Redis's plus
    a little, or within a small bound outright."""
    n = len(srt)
    if not n:
        return va == vb
    name = cmd[0]
    va = va if isinstance(va, list) else [va]
    vb = vb if isinstance(vb, list) else [vb]
    if len(va) != len(vb):
        return False
    args = cmd[2:]
    spread = (srt[-1] - srt[0]) or 1.0
    for i, (x, y) in enumerate(zip(va, vb)):
        if isinstance(x, tuple) or isinstance(y, tuple) or x is None or y is None:
            if x != y:
                return False
            continue
        if isinstance(x, float) and (math.isnan(x) or math.isinf(x)) or isinstance(y, float) and (math.isnan(y) or math.isinf(y)):
            if not (x == y or (math.isnan(x) and math.isnan(y))):
                return False
            continue
        if name in ("TDIGEST.QUANTILE", "TDIGEST.BYRANK", "TDIGEST.BYREVRANK"):
            a = float(args[i])
            q = {"TDIGEST.QUANTILE": a, "TDIGEST.BYRANK": a / n, "TDIGEST.BYREVRANK": 1 - (a + 1) / n}[name]
            ex, ey = (abs(truth_rank(srt, v) / n - q) for v in (x, y))
        elif name == "TDIGEST.CDF":
            t = truth_rank(srt, float(args[i])) / n
            ex, ey = abs(x - t), abs(y - t)
        elif name in ("TDIGEST.RANK", "TDIGEST.REVRANK"):
            t = truth_rank(srt, float(args[i]))
            if name == "TDIGEST.REVRANK":
                t = n - t
            ex, ey = abs(x - t) / n, abs(y - t) / n
        elif name == "TDIGEST.TRIMMED_MEAN":
            lo, hi = float(args[0]), float(args[1])
            part = srt[math.floor(lo * n):math.ceil(hi * n)]
            t = sum(part) / len(part) if part else float("nan")
            ex, ey = abs(x - t) / spread, abs(y - t) / spread
        elif name in ("TDIGEST.MIN", "TDIGEST.MAX"):
            ex, ey = 0.0, abs(x - y)
        else:
            return x == y
        if ey > max(1.5 * ex + 0.02, 0.05):
            return False
    return True


def same(cmd, a, b, n, exact=True, srt=None):
    """Whether replies `a` and `b` (raw) agree for `cmd` on a digest of `n`
    values (`n` small: exact)."""
    if a == b:
        return True
    try:
        va, _ = parse(a)
        vb, _ = parse(b)
    except Exception:
        return False
    name = cmd[0]
    if name == "TDIGEST.INFO" and isinstance(va, list) and isinstance(vb, list) and len(va) == len(vb):
        for i in range(0, len(va), 2):
            if va[i] != vb[i]:
                return False
            if va[i][1] not in INFO_LOOSE and va[i + 1] != vb[i + 1]:
                return False
        return True
    if exact:
        return False

    def close(x, y, kind):
        if isinstance(x, tuple) or isinstance(y, tuple) or x is None or y is None:
            return x == y
        if isinstance(x, float) and math.isnan(x):
            return isinstance(y, float) and math.isnan(y)
        if math.isinf(x) or math.isinf(y):
            return x == y
        if kind == "cdf":
            return abs(x - y) <= 0.02
        if kind == "rank":
            return abs(x - y) <= 0.02 * n + 1
        return True  # value answers: checked by --quality, where truth is known
    if srt is not None:
        return judge(cmd, va, vb, srt)
    kind = {"TDIGEST.CDF": "cdf", "TDIGEST.RANK": "rank", "TDIGEST.REVRANK": "rank"}.get(name, "value")
    if isinstance(va, list) and isinstance(vb, list):
        return len(va) == len(vb) and all(close(x, y, kind) for x, y in zip(va, vb))
    return close(va, vb, kind)


def track(truth, cmd, reply):
    """Each digest's values, as Redis's replies say they changed."""
    name = cmd[0]
    ok = reply.startswith(b"+OK")
    if name == "TDIGEST.CREATE" and ok:
        truth[cmd[1]] = []
    elif name == "TDIGEST.ADD" and ok:
        def num(v):
            try:
                return float(v)
            except ValueError:
                return float.fromhex(v)
        truth.setdefault(cmd[1], []).extend(num(v) for v in cmd[2:])
    elif name == "TDIGEST.RESET" and ok:
        truth[cmd[1]] = []
    elif name == "TDIGEST.MERGE" and ok:
        n = int(cmd[2])
        added = [v for k in cmd[3:3 + n] for v in truth.get(k) or []]
        if "OVERRIDE" in cmd[3 + n:] or truth.get(cmd[1]) is None:
            truth[cmd[1]] = added
        else:
            truth[cmd[1]] = truth[cmd[1]] + added
    elif name == "DEL":
        truth.pop(cmd[1], None)
    elif name == "SET":
        truth.pop(cmd[1], None)


def run(conns, cmd):
    out = []
    for c, c2 in conns:
        if cmd[0] == "#WATCH":
            c.raw("WATCH", cmd[1])
            c2.raw(*cmd[2])
            c.raw("MULTI")
            c.raw("PING")
            out.append(c.raw("EXEC"))
        else:
            out.append(c.raw(*cmd))
    return out


def count_of(conn, key):
    r = conn.raw("TDIGEST.INFO", key)
    try:
        v, _ = parse(r)
        d = {v[i][1]: v[i + 1] for i in range(0, len(v), 2)}
        # Every centroid one value, and few enough values for both sides to
        # keep them so when they merge: at most 20, and a fifth of the
        # compression (Redis merges a compression of 20 down to 11 centroids
        # from 20 values).
        n = d[b"Observations"]
        return n, d[b"Merged nodes"] + d[b"Unmerged nodes"] == n and n <= min(20, d[b"Compression"] // 5)
    except Exception:
        return None


def size_of(conns, key):
    """The digest's count, and whether on both sides every centroid is still
    one value (then nothing is approximated): `(n, exact)`; None for no
    digest."""
    got = [count_of(c2, key) for _, c2 in conns]
    if any(g is None for g in got):
        return None
    return got[0][0], all(g[1] for g in got)


def quality(ports):
    rng = random.Random(5)
    n = 200_000
    dists = {
        "normal": lambda i: rng.gauss(0, 1),
        "uniform": lambda i: rng.random(),
        "lognormal": lambda i: math.exp(rng.gauss(0, 1)),
        "sorted": lambda i: float(i),
    }
    qs = [0.001, 0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99, 0.999]
    print("dist       q       redis rank error  kividb rank error")
    worst = [0.0, 0.0]
    for name, f in dists.items():
        values = [f(i) for i in range(n)]
        srt = sorted(values)
        res = []
        for p in ports:
            c = Conn(p, db=2)
            c.raw("DEL", "q")
            c.raw("TDIGEST.CREATE", "q", "COMPRESSION", "100")
            for i in range(0, n, 2000):
                c.raw("TDIGEST.ADD", "q", *[repr(v) for v in values[i:i + 2000]])
            v, _ = parse(c.raw("TDIGEST.QUANTILE", "q", *[str(q) for q in qs]))
            res.append(v)
        import bisect
        for j, q in enumerate(qs):
            errs = [abs(bisect.bisect_left(srt, res[k][j]) / n - q) for k in range(2)]
            worst = [max(worst[k], errs[k] / min(q, 1 - q)) for k in range(2)]
            print(f"{name:<10} {q:<7} {errs[0]:>16.6f} {errs[1]:>18.6f}")
    print(f"worst relative rank error (error / min(q, 1-q)): redis {worst[0]:.4f}, kividb {worst[1]:.4f}")
    return 0


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
    if "--quality" in flags:
        return quality(ports)
    seed, steps, seqs = int(opts["--seed"]), int(opts["--steps"]), int(opts["--sequences"])
    capture = opts["--capture"]
    diffs, total, loose = [], 0, 0
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
            truth = {}
            for cmd in cmds:
                key = cmd[1] if len(cmd) > 1 and cmd[0] != "#WATCH" else None
                size = size_of(conns, key) if key and cmd[0].startswith("TDIGEST.") else None
                n = None if size is None else size[0]
                replies = run(conns, cmd)
                total += 1
                exact = size is None or size[1]
                track(truth, cmd, replies[0])
                if capture:
                    # Every write is kept (the replay must reach the same
                    # state); a read only where nothing is approximated.
                    approx = cmd[0] in ("TDIGEST.QUANTILE", "TDIGEST.CDF", "TDIGEST.RANK", "TDIGEST.REVRANK",
                                        "TDIGEST.BYRANK", "TDIGEST.BYREVRANK", "TDIGEST.TRIMMED_MEAN")
                    if (approx and not exact) or cmd[0] == "TDIGEST.INFO":
                        continue
                    if not resp3:
                        args = cmd if cmd[0] != "#WATCH" else ["#WATCH", cmd[1]] + cmd[2]
                        cases.append({"args": [a.encode().hex() for a in args], "resp2": replies[0].hex()})
                    else:
                        cases[kept]["resp3"] = replies[0].hex()
                    kept += 1
                    continue
                if not exact:
                    loose += 1
                srt = sorted(truth[key]) if key in truth and truth[key] is not None else None
                if not same(cmd, replies[0], replies[1], n, exact, srt):
                    diffs.append((s, resp3, cmd, replies[0], replies[1]))
        if capture:
            fixture["sequences"].append({"seed": seed * 1000 + s, "cases": cases})
    if capture:
        json.dump(fixture, open(capture, "w"), indent=0)
        print(f"captured {sum(len(q['cases']) for q in fixture['sequences'])} cases")
        return 0
    for s, resp3, cmd, a, b in diffs[:40]:
        print(f"seq {s} RESP{3 if resp3 else 2}: {cmd!r}\n  redis  {a[:300]!r}\n  kividb {b[:300]!r}")
    print(f"{total} replies compared ({loose} of digests whose centroids hold several values, within bounds), {len(diffs)} differ")
    return 1 if diffs else 0


if __name__ == "__main__":
    sys.exit(main())
