#!/usr/bin/env python3
"""rc10 quick core check: rc10 against rc8, 100 B rows.

    python3 analyze-rc10.py <results-dir> > tables.md

Reads summarize.py's summary.csv and runs.csv in <results-dir>.
"""
import csv
import os
import sys

ROOT = sys.argv[1]
S = {(r["engine"], r["size"], r["scenario"]): r for r in csv.DictReader(open(os.path.join(ROOT, "summary.csv")))}
RUNS = [r for r in csv.DictReader(open(os.path.join(ROOT, "runs.csv"))) if not r.get("error")]
SCS = ["get_p1", "get_p16", "set_p1", "set_p16", "mget10_p1", "mset10_p1"]
NEW, OLD = "kividb-rc10", "kividb-rc8"
FLOOR = float(os.environ.get("FLOOR", "1"))


def m(v):
    v = float(v)
    return f"{v / 1e6:.2f}M" if v >= 1e6 else f"{v / 1e3:.0f}K"


def name(sc):
    op, p = sc.split("_p")
    return f"100 B {op.upper().replace('10', ' 10')} p{p}"


def flag(r):
    return {"client": " C", "nic": " N", "client+nic": " C N"}.get(r.get("limit", ""), "")


print("### rc10 against rc8 (same session, alternating rounds)\n")
print("Median [min - max] of the rounds. Spread is half the min-max range of each build, as % of its median.")
print(f"Verdict: **fail** only if every rc10 round is below every rc8 round and the medians differ by more than {FLOOR:.0f}%.\n")
print("| Scenario | rc8 | rc10 | Change | Spread rc8 / rc10 | Verdict | p99 ms rc8 → rc10 | p99.9 ms rc8 → rc10 | server µs/req rc8 → rc10 |")
print("|---|---:|---:|---:|---:|---|---:|---:|---:|")
for sc in SCS:
    o, n = S.get((OLD, "100", sc)), S.get((NEW, "100", sc))
    if not (o and n):
        print(f"| {name(sc)} | missing | | | | | | | |")
        continue
    ch = 100 * (float(n["ops_median"]) / float(o["ops_median"]) - 1)
    below = float(n["ops_max"]) < float(o["ops_min"])
    above = float(n["ops_min"]) > float(o["ops_max"])
    v = "**fail**" if below and ch < -FLOOR else "pass (faster)" if above and ch > FLOOR else "pass (level)"
    if (below or above) and v == "pass (level)":
        v = f"pass (rounds apart, under {FLOOR:.0f}%)"
    if "client" in n["limit"] and v.startswith("pass ("):
        v = v[:-1] + ", clients)"
    print(f"| {name(sc)} | {m(o['ops_median'])} [{m(o['ops_min'])} - {m(o['ops_max'])}]{flag(o)} | "
          f"{m(n['ops_median'])} [{m(n['ops_min'])} - {m(n['ops_max'])}]{flag(n)} | {ch:+.1f}% | "
          f"±{float(o['spread_pct']) / 2:.1f}% / ±{float(n['spread_pct']) / 2:.1f}% | {v} | "
          f"{float(o['p99_ms']):.2f} → {float(n['p99_ms']):.2f} | {float(o['p999_ms']):.2f} → {float(n['p999_ms']):.2f} | "
          f"{float(o['us_per_op']):.2f} → {float(n['us_per_op']):.2f} |")

print("\n### Round by round\n")
print("| Scenario | Build | round | ops/s | p50 / p99 / p99.9 ms | server busy | server µs/req (user + kernel) | load generators busy |")
print("|---|---|---|---:|---:|---:|---:|---:|")
for sc in SCS:
    for e in (OLD, NEW):
        for r in sorted([r for r in RUNS if r["engine"] == e and r["size"] == "100" and r["scenario"] == sc],
                        key=lambda r: r["round"]):
            print(f"| {name(sc)} | {e.split('-')[1]} | {r['round']} | {m(r['ops'])} | "
                  f"{float(r['p50_ms']):.2f} / {float(r['p99_ms']):.2f} / {float(r['p999_ms']):.2f} | {r['srv_busy']}% | "
                  f"{r['us_per_op']} ({r['us_user_per_op']} + {r['us_kernel_per_op']}) | {r['client_busy_max']}% |")
