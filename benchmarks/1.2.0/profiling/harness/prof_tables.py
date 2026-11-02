"""prof_tables.py RESULTS_DIR: Markdown tables for the profiling README.

Prints three blocks separated by lines '=== name': cpu (CPU per request per
engine/workload, with the busiest thread), split (categorize.py shares in
microseconds per request) and cg (callgrind instructions per command)."""
import csv
import os
import sys

RES = sys.argv[1]
NAMES = {"kividb-rc4-fp": "KiviDB rc4", "kividb-1.1.3-fp": "KiviDB 1.1.3", "redis": "Redis 8.10.3",
         "kividb-rc4": "KiviDB rc4, release binary"}
ORDER = ["kividb-rc4-fp", "redis", "kividb-1.1.3-fp", "kividb-rc4"]
WL = ["get-p1", "get-p16", "set-p1", "set-p16"]


def wname(w):
    c, p = w.split("-p")
    return f"{c.upper()}, pipeline {p}"


print("=== cpu")
for e in ORDER:
    p = os.path.join(RES, e, "cpu.csv")
    if not os.path.exists(p):
        continue
    for r in csv.DictReader(open(p)):
        tag = f"{r['workload']}-p{r['pipeline']}"
        busiest = ""
        tp = os.path.join(RES, e, f"{tag}.threads.csv")
        if os.path.exists(tp):
            th = [t for t in csv.DictReader(open(tp)) if t.get("cores", "").replace(".", "").isdigit()]
            th.sort(key=lambda t: -float(t["cores"]))
            if th:
                busy = [t for t in th if float(t["cores"]) >= 0.05]
                busiest = (f"{th[0]['thread'].replace('kividb-rc4-fp', 'worker').replace('kividb-1.1.3-fp', 'worker').replace('kividb-rc4', 'worker')}"
                           f" {float(th[0]['cores']):.2f} cores; {len(busy)} threads busy, "
                           f"{sum(float(t['cores']) for t in busy):.1f} cores in all")
        print(f"| {NAMES[e]}, {wname(tag)} | {float(r['ops_per_sec']) / 1e6:.2f}M | {float(r['total_us_per_op']):.2f} | "
              f"{float(r['user_us_per_op']):.2f} | {float(r['sys_us_per_op']):.2f} | {float(r['syscalls_per_op']):.2f} | {busiest} |")

print("=== split")
sp = os.path.join(RES, "cpu-split.csv")
if os.path.exists(sp):
    rows = list(csv.DictReader(open(sp)))
    cats = []
    for r in rows:
        if r["category"] not in cats:
            cats.append(r["category"])
    short = {c: c.split(" (")[0] for c in cats}
    keys = [(e, w) for e in ["kividb-rc4-fp", "redis", "kividb-1.1.3-fp"] for w in WL
            if any(r["engine"] == e and r["workload"] == w for r in rows)]
    print("| microseconds of CPU per request | " + " | ".join(f"{NAMES[e]}<br>{wname(w)}" for e, w in keys) + " |")
    print("|---|" + "---|" * len(keys))
    for c in cats:
        vals = []
        for e, w in keys:
            m = [r for r in rows if r["engine"] == e and r["workload"] == w and r["category"] == c]
            vals.append(f"{float(m[0]['us_per_op']):.2f} ({float(m[0]['share_pct']):.0f}%)" if m else "-")
        print(f"| {short[c]} | " + " | ".join(vals) + " |")
    tot = []
    for e, w in keys:
        m = [r for r in rows if r["engine"] == e and r["workload"] == w]
        tot.append(f"**{float(m[0]['total_us_per_op']):.2f}**")
    print("| **total** | " + " | ".join(tot) + " |")

print("=== cg")
cg = os.path.join(RES, "callgrind-table.csv")
if os.path.exists(cg):
    rows = list(csv.DictReader(open(cg)))
    print("| workload | KiviDB 1.1.3 | KiviDB 1.2.0-rc4 | rc4 vs 1.1.3 | Redis 8.10.3 | rc4 vs Redis |")
    print("|---|---|---|---|---|---|")
    for r in rows:
        a, b, c = r.get("v113", ""), r.get("rc4", ""), r.get("redis-8.10.3", "")
        d1 = f"{(int(b) - int(a)) * 100.0 / int(a):+.1f}%" if a and b else ""
        d2 = f"{(int(b) - int(c)) * 100.0 / int(c):+.1f}%" if b and c else "-"
        print(f"| {r['workload']} | {a} | {b} | {d1} | {c or '-'} | {d2} |")
