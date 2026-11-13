#!/usr/bin/env python3
"""1.2.1 core check: tables from summarize.py's summary.csv and runs.csv.

    python3 analyze-121.py <results-dir> > tables.md

The rule (the 1.2.0 suite's last round): a row fails only if every NEW round is below every
OLD round AND the medians differ by more than the spread. Spread here is the larger of the two
builds' half min-max ranges, as % of the median, and never less than FLOOR (1%).
"""
import csv
import os
import sys

ROOT = sys.argv[1]
S = {(r["engine"], r["size"], r["scenario"]): r for r in csv.DictReader(open(os.path.join(ROOT, "summary.csv")))}
RUNS = [r for r in csv.DictReader(open(os.path.join(ROOT, "runs.csv"))) if not r.get("error")]
SIZES = ["100", "1024", "2048"]
SCS = ["get_p1", "get_p16", "set_p1", "set_p16", "mget10_p1", "mset10_p1"]
LABEL = {"kividb-121": "1.2.1", "kividb-120rel": "1.2.0 rel", "kividb-120src": "1.2.0 built"}
FLOOR = float(os.environ.get("FLOOR", "1"))


def m(v):
    v = float(v)
    return f"{v / 1e6:.2f}M" if v >= 1e6 else f"{v / 1e3:.0f}K"


def name(size, sc):
    op, p = sc.split("_p")
    sz = {"100": "100 B", "1024": "1 KB", "2048": "2 KB"}[size]
    return f"{sz} {op.upper().replace('10', ' 10')} p{p}"


def flag(r):
    return {"client": " C", "nic": " N", "client+nic": " C N"}.get(r.get("limit", ""), "")


def table(new, old):
    nl, ol = LABEL[new], LABEL[old]
    print(f"### {nl} against {ol}\n")
    print("Median [min - max] of the rounds. **C**: load generators >90% CPU (they are the limit). "
          f"Verdict **fail** only if every {nl} round is below every {ol} round and the medians differ "
          f"by more than the spread (larger half-range, at least {FLOOR:.0f}%).\n")
    print(f"| Row | {nl} | {ol} | Δ | spread {nl} / {ol} | Verdict | p50 ms | p99 ms | p99.9 ms | server µs/req | load gen. CPU % |")
    print("|---|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|")
    for size in SIZES:
        for sc in SCS:
            if sc.startswith("m") and size != "100":
                continue
            o, n = S.get((old, size, sc)), S.get((new, size, sc))
            if not (o and n):
                if o or n:
                    print(f"| {name(size, sc)} | missing | | | | | | | | | |")
                continue
            ch = 100 * (float(n["ops_median"]) / float(o["ops_median"]) - 1)
            sn, so = float(n["spread_pct"]) / 2, float(o["spread_pct"]) / 2
            spread = max(FLOOR, sn, so)
            below = float(n["ops_max"]) < float(o["ops_min"])
            above = float(n["ops_min"]) > float(o["ops_max"])
            if below and -ch > spread:
                v = "**fail**"
            elif above and ch > spread:
                v = "pass (faster)"
            elif below or above:
                v = "pass (rounds apart, within spread)"
            else:
                v = "pass (level)"
            if "client" in n["limit"] and v.startswith("pass ("):
                v = v[:-1] + ", clients)"
            print(f"| {name(size, sc)} | {m(n['ops_median'])} [{m(n['ops_min'])} - {m(n['ops_max'])}]{flag(n)} | "
                  f"{m(o['ops_median'])} [{m(o['ops_min'])} - {m(o['ops_max'])}]{flag(o)} | {ch:+.1f}% | "
                  f"±{sn:.1f}% / ±{so:.1f}% | {v} | "
                  f"{float(n['p50_ms']):.2f} / {float(o['p50_ms']):.2f} | "
                  f"{float(n['p99_ms']):.2f} / {float(o['p99_ms']):.2f} | {float(n['p999_ms']):.2f} / {float(o['p999_ms']):.2f} | "
                  f"{float(n['us_per_op']):.2f} / {float(o['us_per_op']):.2f} | "
                  f"{float(n['client_busy_max']):.0f} / {float(o['client_busy_max']):.0f} |")
    print()


table("kividb-121", "kividb-120rel")
table("kividb-121", "kividb-120src")
table("kividb-120src", "kividb-120rel")

print("### Round by round\n")
print("| Row | Build | round | ops/s | p50 / p99 / p99.9 ms | server busy % | server µs/req (user + kernel) | load gen. busy % |")
print("|---|---|---|---:|---:|---:|---:|---:|")
for size in SIZES:
    for sc in SCS:
        for e in ("kividb-121", "kividb-120rel", "kividb-120src"):
            for r in sorted([r for r in RUNS if r["engine"] == e and r["size"] == size and r["scenario"] == sc],
                            key=lambda r: r["round"]):
                print(f"| {name(size, sc)} | {LABEL[e]} | {r['round']} | {m(r['ops'])} | "
                      f"{float(r['p50_ms']):.2f} / {float(r['p99_ms']):.2f} / {float(r['p999_ms']):.2f} | {r['srv_busy']} | "
                      f"{r['us_per_op']} ({r['us_user_per_op']} + {r['us_kernel_per_op']}) | {r['client_busy_max']} |")
