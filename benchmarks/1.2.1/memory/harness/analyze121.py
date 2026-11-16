#!/usr/bin/env python3
"""Tables for the 1.2.1 memory round (1.2.1 against 1.2.0), from a copy of the client's /var/tmp/mem and /var/tmp/wf.

    python3 -I analyze121.py <raw-mem-dir> <raw-wf-dir> > results/tables.md

growth: bytes per key = (RSS or used_memory at the checkpoint - the idle engine's, 5 s after start) / keys.
        RSS is the sum of VmRSS over the container's processes (rss_all_mb), as in the 1.2.0 suite.
hfe:    bytes per field = (value at the checkpoint - idle) / (hashes x fields); TTL cost = after HPEXPIRE - after HSET.
warefeats: their runner's bytesPerKey* metrics, unchanged.
"""
import csv, glob, json, os, statistics, sys
from collections import defaultdict

RA, WF = sys.argv[1], sys.argv[2]
MiB = 1024 * 1024
B = ["kividb-1.2.0", "kividb-1.2.1"]
LAB = {"kividb-1.2.0": "1.2.0 (release binary)", "kividb-1.2.1": "1.2.1 (878746e4)"}
COLS = "t,anon_mb,file_mb,rss_main_mb,rss_all_mb,nproc,used_mb,keys".split(",")


def med(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None


def mr(xs, d=1):
    xs = [x for x in xs if x is not None]
    if not xs:
        return "-"
    m = med(xs)
    if len(xs) == 1:
        return f"{m:,.{d}f}"
    return f"{m:,.{d}f} ({min(xs):,.{d}f}-{max(xs):,.{d}f})"


def chg(a, b):
    a, b = med(a), med(b)
    if a is None or b is None or b == 0:
        return "-"
    return f"{(a - b) / b * 100:+.2f}%"


def row(s):
    return {k: (float(v) if v not in ("", None) else None) for k, v in zip(COLS, s)}


# ------------------------------------------------------------------ events
EV = defaultdict(dict)
p = os.path.join(RA, "events.csv")
if os.path.exists(p):
    for r in csv.reader(open(p)):
        if len(r) >= 3:
            EV[r[1]][r[2]] = float(r[0])

# ------------------------------------------------------------------ growth
ck = defaultdict(lambda: defaultdict(dict))  # (e, v) -> round -> keys -> (rss_bpk, used_bpk, rss_mb, used_mb)
idle = defaultdict(list)  # e -> [(idle used bytes, idle rss MB)]
p = os.path.join(RA, "checkpoints.csv")
if os.path.exists(p):
    for r in csv.reader(open(p)):
        e, v, kind, c, rnd = r[0], int(r[1]), r[2], int(r[3]), r[6]
        s = row(r[7:])
        run = f"growth-{e}-{v}-{kind}-{rnd}"
        g = os.path.join(RA, run + ".csv")
        rows = [row(x) for x in list(csv.reader(open(g)))[1:]] if os.path.exists(g) else []
        t0 = EV[run].get("start")
        if rows and t0:
            b = min(rows, key=lambda x: abs(x["t"] - t0))
        else:
            b = {"rss_all_mb": float(r[4]), "used_mb": float(r[5])}
        ck[(e, v)][rnd][c] = ((s["rss_all_mb"] - b["rss_all_mb"]) * MiB / c, (s["used_mb"] - b["used_mb"]) * MiB / c,
                              s["rss_all_mb"], s["used_mb"])


def load_s(e, v, rnd):
    ev = EV.get(f"growth-{e}-{v}-random-{rnd}", {})
    tot = sum(ev[f"loaded-{k[8:]}"] - t for k, t in ev.items() if k.startswith("load-to-") and f"loaded-{k[8:]}" in ev)
    return tot or None


SUMMARY = []
for v in sorted({k[1] for k in ck}):
    cks = sorted({c for e in B for rr in ck[(e, v)].values() for c in rr})
    print(f"## Growth, {v} B random values\n")
    print("RSS bytes per key (used_memory bytes per key); median (min-max) over rounds.\n")
    print("| build | rounds | " + " | ".join(f"{c / 1e6:g}M" for c in cks) + " | RSS at largest GiB | load s (all stages) |")
    print("|---|---|" + "---|" * (len(cks) + 2))
    for e in B:
        rs = ck[(e, v)]
        if not rs:
            continue
        cells = []
        for c in cks:
            a = [rr[c][0] for rr in rs.values() if c in rr]
            u = [rr[c][1] for rr in rs.values() if c in rr]
            cells.append(f"{mr(a)} ({mr(u)})")
        last = max(cks)
        print(f"| {LAB[e]} | {len(rs)} | " + " | ".join(cells) +
              f" | {mr([rr[last][2] / 1024 for rr in rs.values() if last in rr], 2)} | {mr([load_s(e, v, k) for k in rs])} |")
    print()
    print("| metric | 1.2.1 | 1.2.0 | change |\n|---|---|---|---|")
    for c in cks:
        for i, n in ((0, "RSS"), (1, "used")):
            a = [rr[c][i] for rr in ck[("kividb-1.2.1", v)].values() if c in rr]
            b = [rr[c][i] for rr in ck[("kividb-1.2.0", v)].values() if c in rr]
            print(f"| {n} B/key at {c / 1e6:g}M | {mr(a, 2)} | {mr(b, 2)} | {chg(a, b)} |")
    a = [load_s("kividb-1.2.1", v, k) for k in ck[("kividb-1.2.1", v)]]
    b = [load_s("kividb-1.2.0", v, k) for k in ck[("kividb-1.2.0", v)]]
    print(f"| load s, all stages | {mr(a)} | {mr(b)} | {chg(a, b)} |\n")

# idle used_memory: the sampler row nearest the 'start' event of every growth and hfe run
print("## Idle engine (empty, 2 s after it answers PING; the `start-memory` line of every run's *.engine.txt)\n")
print("| build | starts | used_memory B | used_memory_rss MB |\n|---|---|---|---|")
idle = {}
for e in B:
    us, rs = [], []
    for g in sorted(glob.glob(os.path.join(RA, f"*-{e}-*.engine.txt"))):
        for line in open(g):
            if line.startswith("start-memory:"):
                d = dict(x.split(":", 1) for x in line.split()[1:])
                us.append(int(d["used_memory"]))
                rs.append(int(d["used_memory_rss"]) / MiB)
    idle[e] = us
    print(f"| {LAB[e]} | {len(us)} | {mr(us, 0)} | {mr(rs, 1)} |")
if all(idle.get(e) for e in B):
    print(f"\n1.2.1 against 1.2.0: {med(idle['kividb-1.2.1']) - med(idle['kividb-1.2.0']):+,.0f} B ({chg(idle['kividb-1.2.1'], idle['kividb-1.2.0'])}).\n")

# ------------------------------------------------------------------ hfe
hf = defaultdict(lambda: defaultdict(dict))  # e -> round -> stage -> row
p = os.path.join(RA, "hfe-checkpoints.csv")
if os.path.exists(p):
    print("## Hashes with field TTLs (1M hashes x 10 fields x 16 B, then HPEXPIRE on every field)\n")
    nf = None
    for r in csv.reader(open(p)):
        e, n, f, v, rnd, st = r[0], int(r[1]), int(r[2]), int(r[3]), r[4], r[5]
        nf = n * f
        hf[e][rnd][st] = row(r[6:])
    print("Bytes per field; median (min-max) over rounds.\n")
    print("| build | rounds | used B/field, no TTL | used B/field, with TTL | used B per TTL | RSS B/field, no TTL | RSS B/field, with TTL | RSS B per TTL |")
    print("|---|---|---|---|---|---|---|---|")
    vals = {}
    for e in B:
        rs = hf[e]
        if not rs:
            continue
        m = defaultdict(list)
        for d in rs.values():
            if not all(k in d for k in ("idle", "hset", "hpexpire")):
                continue
            for k, col in (("used", "used_mb"), ("rss", "rss_all_mb")):
                i, h, x = d["idle"][col], d["hset"][col], d["hpexpire"][col]
                m[k + "0"].append((h - i) * MiB / nf)
                m[k + "1"].append((x - i) * MiB / nf)
                m[k + "t"].append((x - h) * MiB / nf)
        vals[e] = m
        print(f"| {LAB[e]} | {len(m['used0'])} | " + " | ".join(mr(m[k], 1) for k in ("used0", "used1", "usedt", "rss0", "rss1", "rsst")) + " |")
    if len(vals) == 2:
        print("\n| metric | 1.2.1 | 1.2.0 | change |\n|---|---|---|---|")
        for k, n in (("used0", "used B/field, no TTL"), ("used1", "used B/field, with TTL"), ("usedt", "used B per field TTL"),
                     ("rss0", "RSS B/field, no TTL"), ("rss1", "RSS B/field, with TTL"), ("rsst", "RSS B per field TTL")):
            print(f"| {n} | {mr(vals['kividb-1.2.1'][k], 2)} | {mr(vals['kividb-1.2.0'][k], 2)} | {chg(vals['kividb-1.2.1'][k], vals['kividb-1.2.0'][k])} |")
    print()

# ------------------------------------------------------------------ warefeats
wf = defaultdict(list)  # (build, workload) -> [metrics]
for p in sorted(glob.glob(os.path.join(WF, "*.json"))):
    v = os.path.basename(p)[:-5]
    e = next((b for b in B if v.startswith(b)), None)
    if not e:
        continue
    try:
        for r in json.load(open(p))["results"]:
            wf[(e, r["workload"])].append(r["metrics"])
    except (ValueError, KeyError):
        continue
if wf:
    MEMK = [("bytesPerKeyRss", "1st M RSS B/key"), ("bytesPerKeyUsed", "1st M used B/key"),
            ("bytesPerKeyRssSecond", "2nd M RSS B/key"), ("bytesPerKeyUsedSecond", "2nd M used B/key")]
    print("## warefeats/kv-stores memory cells (a324d29): 1M keys x 100 B, then a second million\n")
    for wl in ("memory-load", "memory-load-random"):
        print(f"### {wl} ({'memtier ASCII values' if wl == 'memory-load' else 'random bytes'})\n")
        print("| build | runs | " + " | ".join(n for _, n in MEMK) + " |\n|---|---|" + "---|" * len(MEMK))
        for e in B:
            ms = wf.get((e, wl), [])
            if ms:
                print(f"| {LAB[e]} | {len(ms)} | " + " | ".join(mr([m.get(k, {}).get("value") for m in ms]) for k, _ in MEMK) + " |")
        print()
