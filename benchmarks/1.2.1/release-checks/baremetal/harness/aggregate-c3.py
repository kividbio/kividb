"""Aggregate this session's KiviDB rounds plus the reused 2026-11-10 rows into all.csv and tables.md.

    python aggregate-c3.py <this-session-results-dir> <reused-dir> <out-dir>

Labels: kividb121 = 1.2.1 (35a413c0), kividb120 = 1.2.0 release, both this session (4 rounds, ABBA).
prev121 = 1.2.1 build e7669a1f and prev120 = 1.2.0, garnet, dragonfly, redis: the 2026-11-10 session (same hardware type).
"""
import csv, glob, os, statistics, sys

src, reused, out = sys.argv[1:4]
rows = []
for d in (src, reused):
    for path in sorted(glob.glob(os.path.join(d, "*", "summary.csv"))):
        rows += list(csv.DictReader(open(path)))
cols = list(rows[0].keys())
with open(os.path.join(out, "all.csv"), "w", newline="") as f:
    w = csv.DictWriter(f, cols, lineterminator="\n"); w.writeheader(); w.writerows(rows)

eng = lambda l: l.rsplit("-r", 1)[0]
rnd = lambda l: int(l.rsplit("-r", 1)[1])
cells = {}
for r in rows:
    cells.setdefault((eng(r["label"]), r["pipeline"], r["workload"]), []).append(r)

def stat(e, p, wl, field, rounds=None):
    return [float(r[field]) for r in cells.get((e, p, wl), []) if r[field] and (rounds is None or rnd(r["label"]) in rounds)]

M = lambda v: f"{v/1e6:.2f}M"
engines = ["kividb121", "kividb120", "prev121", "prev120", "garnet", "dragonfly", "redis"]
scen = [("GET, pipeline 16", "16", "0:1"), ("SET, pipeline 16", "16", "1:0"),
        ("GET, no pipelining", "1", "0:1"), ("SET, no pipelining", "1", "1:0")]
L = []
for title, rounds in [("rounds 1-2 (published method)", {1, 2}), ("all rounds", None)]:
    L += [f"### Ops/s, {title}\n", "| | " + " | ".join(engines) + " |", "|---|" + "---:|" * len(engines)]
    for name, p, wl in scen:
        L.append(f"| {name} | " + " | ".join(
            (f"{M(statistics.mean(v))} ({M(min(v))}-{M(max(v))})" if (v := stat(e, p, wl, "ops_per_sec", rounds)) else "") for e in engines) + " |")
    for name, wl in [("p99 GET p16 (ms)", "0:1"), ("p99 SET p16 (ms)", "1:0")]:
        L.append(f"| {name} | " + " | ".join(
            (f"{statistics.mean(v):.3f} ({min(v):.2f}-{max(v):.2f})" if (v := stat(e, "16", wl, "p99_ms", rounds)) else "") for e in engines) + " |")
    L.append("| Bytes a key | " + " | ".join(
        (f"{statistics.mean(v):.1f}" if (v := stat(e, "16", "0:1", "bytes_per_key", rounds)) else "n/a") for e in engines) + " |")
    L.append("")
for a_, b_, t in [("kividb121", "kividb120", "1.2.1 (35a413c0) vs 1.2.0, this session, 4 rounds each, ABBA"),
                  ("kividb121", "prev121", "1.2.1 (35a413c0) this session vs 1.2.1 (e7669a1f) 2026-11-10, 4 rounds each")]:
    L += [f"### {t}\n", "| | A | B | delta |", "|---|---:|---:|---:|"]
    for name, p, wl in scen:
        a = statistics.mean(stat(a_, p, wl, "ops_per_sec")); b = statistics.mean(stat(b_, p, wl, "ops_per_sec"))
        L.append(f"| {name} | {M(a)} | {M(b)} | {100*(a/b-1):+.1f}% |")
    for name, wl in [("p99 GET p16 (ms)", "0:1"), ("p99 SET p16 (ms)", "1:0")]:
        a = statistics.mean(stat(a_, "16", wl, "p99_ms")); b = statistics.mean(stat(b_, "16", wl, "p99_ms"))
        L.append(f"| {name} | {a:.3f} | {b:.3f} | {100*(a/b-1):+.1f}% |")
    L.append("")
open(os.path.join(out, "tables.md"), "w").write("\n".join(L) + "\n")
print("\n".join(L))
