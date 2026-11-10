"""Aggregate results/*/summary.csv into all.csv and a markdown table.

    python aggregate.py <results-dir> <out-dir>
"""
import csv, glob, os, statistics, sys

src, out = sys.argv[1], sys.argv[2]
rows = []
for path in sorted(glob.glob(os.path.join(src, "*", "summary.csv"))):
    rows += list(csv.DictReader(open(path)))
cols = list(rows[0].keys())
with open(os.path.join(out, "all.csv"), "w", newline="") as f:
    w = csv.DictWriter(f, cols, lineterminator="\n")
    w.writeheader()
    w.writerows(rows)

def engine(label):
    return label.rsplit("-r", 1)[0]

def rnd(label):
    return int(label.rsplit("-r", 1)[1])

cells = {}
for r in rows:
    key = (engine(r["label"]), r["pipeline"], r["workload"])
    cells.setdefault(key, []).append(r)

def stat(eng, p, wl, field, rounds=None):
    vals = [float(r[field]) for r in cells.get((eng, p, wl), [])
            if r[field] and (rounds is None or rnd(r["label"]) in rounds)]
    return vals

def fmt_m(v):
    return f"{v/1e6:.2f}M"

engines = ["kividb121", "kividb120", "garnet", "dragonfly", "redis"]
scen = [("GET, pipeline 16", "16", "0:1"), ("SET, pipeline 16", "16", "1:0"),
        ("GET, no pipelining", "1", "0:1"), ("SET, no pipelining", "1", "1:0")]
lines = []
for title, rounds in [("rounds 1-2 (the published method: mean of two rounds)", {1, 2}),
                      ("all rounds (KiviDB: 4 rounds, ABBA order)", None)]:
    lines.append(f"### Ops/s, {title}\n")
    lines.append("| | " + " | ".join(engines) + " |")
    lines.append("|---|" + "---:|" * len(engines))
    for name, p, wl in scen:
        cellsfmt = []
        for e in engines:
            v = stat(e, p, wl, "ops_per_sec", rounds)
            cellsfmt.append(f"{fmt_m(statistics.mean(v))} ({fmt_m(min(v))}-{fmt_m(max(v))})" if v else "")
        lines.append(f"| {name} | " + " | ".join(cellsfmt) + " |")
    for name, wl in [("p99 GET p16 (ms)", "0:1"), ("p99 SET p16 (ms)", "1:0")]:
        cellsfmt = []
        for e in engines:
            v = stat(e, "16", wl, "p99_ms", rounds)
            cellsfmt.append(f"{statistics.mean(v):.2f} ({min(v):.2f}-{max(v):.2f})" if v else "")
        lines.append(f"| {name} | " + " | ".join(cellsfmt) + " |")
    cellsfmt = []
    for e in engines:
        v = stat(e, "16", "0:1", "bytes_per_key", rounds)
        cellsfmt.append(f"{statistics.mean(v):.1f}" if v else "not reported")
    lines.append("| Bytes a key | " + " | ".join(cellsfmt) + " |")
    lines.append("")

lines.append("### KiviDB 1.2.1 vs 1.2.0 (all four rounds each)\n")
lines.append("| | 1.2.1 | 1.2.0 | delta |")
lines.append("|---|---:|---:|---:|")
for name, p, wl in scen:
    a = statistics.mean(stat("kividb121", p, wl, "ops_per_sec"))
    b = statistics.mean(stat("kividb120", p, wl, "ops_per_sec"))
    lines.append(f"| {name} | {fmt_m(a)} | {fmt_m(b)} | {100*(a/b-1):+.1f}% |")
for name, wl in [("p99 GET p16 (ms)", "0:1"), ("p99 SET p16 (ms)", "1:0")]:
    a = statistics.mean(stat("kividb121", "16", wl, "p99_ms"))
    b = statistics.mean(stat("kividb120", "16", wl, "p99_ms"))
    lines.append(f"| {name} | {a:.3f} | {b:.3f} | {100*(a/b-1):+.1f}% |")
print("\n".join(lines))
