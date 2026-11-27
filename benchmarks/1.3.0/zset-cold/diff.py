#!/usr/bin/env python3
"""Differential test: every sorted-set command against master and the
prototype, same random sequence; replies must match (ZRANDMEMBER: sizes and
membership; ZSCAN: the union of a full scan). DEBUG DIGEST-VALUE, DIGEST and
DUMP payloads compared too.
usage: diff.py <master-bin> <proto-bin> <seed> <steps>"""
import os, random, subprocess, sys, time
sys.path.insert(0, os.path.expanduser("~/venv/lib/python3.12/site-packages"))
import redis

def start(binary, port):
    d = "/tmp/diff-%d" % port
    os.makedirs(d, exist_ok=True)
    open(d + "/k.conf", "w").write("enable-debug-command yes\n")
    p = subprocess.Popen([binary, "--port", str(port), "--bind", "127.0.0.1", "--protected-mode", "no", "--dir", d,
                          "--configfile", d + "/k.conf", "--save", "", "--appendonly", "no"],
                         stdout=open(d + "/log", "w"), stderr=subprocess.STDOUT, cwd=d,
                         env=dict(os.environ, KIVIDB_NO_UPDATE_CHECK="1"))
    c = redis.Redis(port=port)
    for _ in range(100):
        try:
            c.ping(); return p, c
        except Exception:
            time.sleep(0.1)
    raise SystemExit("no server")

SCORES = ["0", "-0", "1", "-1", "inf", "-inf", "+inf", "1e-310", "-4.9e-324", "2.5", "1e300", "0.1"]

def member(r):
    i = r.randrange(3000)
    n = r.choice([0, 1, 3, 8, 15, 20, 64, 65, 120, 121, 128, 200, 300]) if r.random() < 0.3 else (i * 7) % 24
    m = (b"%d:" % i)
    m = (m + bytes([97 + i % 26]) * n)[:max(n, len(m))] if n else (b"" if i % 7 == 0 else m)
    return m

def score(r):
    if r.random() < 0.4:
        return r.choice(SCORES)
    return repr(r.randrange(-2000, 2000) / 4)

def rng_lex(r):
    a, b = sorted([member(r), member(r)])
    return r.choice([b"-", b"[" + a, b"(" + a]), r.choice([b"+", b"[" + b, b"(" + b])

def rng_score(r):
    a = r.choice(["-inf", "+inf", "(1", "0", "(-5", "100", "(2.5"] + [repr(r.randrange(-500, 500))])
    b = r.choice(["+inf", "-inf", "(400", "300", "(0"] + [repr(r.randrange(-500, 500))])
    return a, b

def gen(r, keys):
    k = r.choice(keys)
    op = r.randrange(30)
    if op < 8:
        n = r.choice([1, 1, 2, 5, 50])
        flags = r.choice([[], [], ["NX"], ["XX"], ["GT"], ["LT"], ["CH"], ["INCR"]])
        if "INCR" in flags: n = 1
        args = ["ZADD", k] + flags
        for _ in range(n): args += [score(r), member(r)]
        return args
    if op < 11: return ["ZREM", k] + [member(r) for _ in range(r.choice([1, 1, 3]))]
    if op == 11: return ["ZINCRBY", k, score(r) if r.random() < 0.2 else str(r.randrange(-100, 100)), member(r)]
    if op == 12: return ["ZSCORE", k, member(r)]
    if op == 13: return ["ZMSCORE", k, member(r), member(r)]
    if op == 14: return [r.choice(["ZRANK", "ZREVRANK"]), k, member(r)] + (["WITHSCORE"] if r.random() < 0.3 else [])
    if op == 15:
        a = r.randrange(-300, 300); b = r.randrange(-300, 300)
        return [r.choice(["ZRANGE", "ZREVRANGE"]), k, str(a), str(b)] + (["WITHSCORES"] if r.random() < 0.5 else [])
    if op == 16:
        a, b = rng_score(r)
        cmd = r.choice(["ZRANGEBYSCORE", "ZREVRANGEBYSCORE"])
        if cmd == "ZREVRANGEBYSCORE": a, b = b, a
        extra = ["LIMIT", str(r.randrange(0, 50)), str(r.randrange(-1, 30))] if r.random() < 0.5 else []
        return [cmd, k, a, b] + (["WITHSCORES"] if r.random() < 0.5 else []) + extra
    if op == 17: a, b = rng_score(r); return ["ZCOUNT", k, a, b]
    if op == 18: a, b = rng_lex(r); return ["ZLEXCOUNT", k, a, b]
    if op == 19:
        a, b = rng_lex(r)
        cmd = r.choice(["ZRANGEBYLEX", "ZREVRANGEBYLEX"])
        if cmd == "ZREVRANGEBYLEX": a, b = b, a
        return [cmd, k, a, b] + (["LIMIT", str(r.randrange(0, 20)), str(r.randrange(-1, 30))] if r.random() < 0.5 else [])
    if op == 20: return [r.choice(["ZPOPMIN", "ZPOPMAX"]), k] + ([str(r.randrange(1, 5))] if r.random() < 0.5 else [])
    if op == 21: a = r.randrange(-300, 300); return ["ZREMRANGEBYRANK", k, str(a), str(a + r.randrange(0, 4))]
    if op == 22: a, b = rng_score(r); return ["ZREMRANGEBYSCORE", k, a, b] if r.random() < 0.3 else ["ZCARD", k]
    if op == 23: a, b = rng_lex(r); return ["ZREMRANGEBYLEX", k, a, b] if r.random() < 0.3 else ["ZCARD", k]
    if op == 24: return [r.choice(["ZUNIONSTORE", "ZINTERSTORE", "ZDIFFSTORE"]), "dst", "2", k, r.choice(keys)]
    if op == 25: return [r.choice(["ZUNION", "ZINTER", "ZDIFF"]), "2", k, r.choice(keys), "WITHSCORES"]
    if op == 26: return ["ZRANGESTORE", "dst2", k, str(r.randrange(0, 100)), str(r.randrange(0, 300))]
    if op == 27: return ["ZCARD", k]
    if op == 28: return ["DEBUG", "DIGEST-VALUE", k]
    return ["ZRANDMEMBER", k, str(r.choice([1, 5, -5, 300, -300])), "WITHSCORES"]

def norm(x):
    if isinstance(x, list): return [norm(y) for y in x]
    return x

def call(c, args):
    try:
        return c.execute_command(*args)
    except redis.ResponseError as e:
        return "ERR:" + str(e)

def scan_all(c, k):
    seen = {}
    cur = 0
    while True:
        cur, items = c.zscan(k, cur, count=r.choice([1, 10, 100]))
        for m, s in items: seen[m] = s
        if cur == 0: return seen

def pairs(x):
    return sorted(zip(x[::2], x[1::2])) if isinstance(x, list) else x

master, proto, seed, steps = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
pa, a = start(master, 7101)
pb, b = start(proto, 7102)
r = random.Random(seed)
keys = ["z%d" % i for i in range(6)]
# Large from the start: a few hundred members each.
for k in keys:
    for _ in range(4):
        args = ["ZADD", k]
        for _ in range(100): args += [score(r), member(r)]
        assert call(a, args) == call(b, args)
bad = 0
for step in range(steps):
    args = gen(r, keys)
    x, y = call(a, args), call(b, args)
    if args[0] == "ZRANDMEMBER":
        cnt = int(args[2])
        def flat(v):
            out = []
            for e in v or []:
                out += list(e) if isinstance(e, (list, tuple)) else [e]
            return out
        x, y = flat(x), flat(y)
        ok = len(x) == len(y)
        if ok:
            ys = list(zip(y[::2], y[1::2]))
            # Every pick a member of the set, at its score (master's).
            ok = all(call(a, ["ZSCORE", args[1], m]) == sc for m, sc in ys[:50])
            if ok and cnt > 0:
                ok = len({m for m, _ in ys}) == len(ys)
                if ok and cnt >= a.zcard(args[1]):
                    ok = sorted(ys) == sorted(zip(x[::2], x[1::2]))
        if not ok:
            bad += 1; print("DIFF", step, args[:3], len(x), len(y))
        continue
    if x != y:
        bad += 1
        print("DIFF", step, args, repr(x)[:300], repr(y)[:300])
        if bad > 20: break
    if step % 500 == 0:
        for k in keys:
            if call(a, ["DEBUG", "DIGEST-VALUE", k]) != call(b, ["DEBUG", "DIGEST-VALUE", k]):
                bad += 1; print("DIGEST-VALUE", step, k)
            if a.exists(k) and scan_all(a, k) != scan_all(b, k):
                bad += 1; print("ZSCAN", step, k)
            if call(a, ["DUMP", k]) != call(b, ["DUMP", k]):
                bad += 1; print("DUMP", step, k)
            if a.exists(k) and a.object("encoding", k) != b.object("encoding", k):
                bad += 1; print("ENCODING", step, k)
        if call(a, ["DEBUG", "DIGEST"]) != call(b, ["DEBUG", "DIGEST"]):
            bad += 1; print("DIGEST", step)
        # RESTORE of the prototype's payload into master and back.
        k = r.choice(keys)
        d = call(b, ["DUMP", k])
        if d:
            call(a, ["RESTORE", "rt", "0", d, "REPLACE"]); call(b, ["RESTORE", "rt", "0", d, "REPLACE"])
            if call(a, ["ZRANGE", "rt", "0", "-1", "WITHSCORES"]) != call(b, ["ZRANGE", "rt", "0", "-1", "WITHSCORES"]):
                bad += 1; print("RESTORE", step)
sizes = [a.zcard(k) for k in keys]
print("steps", steps, "seed", seed, "sizes", sizes, "differences", bad)
for c in (a, b):
    try: c.shutdown(nosave=True)
    except Exception: pass
sys.exit(1 if bad else 0)
