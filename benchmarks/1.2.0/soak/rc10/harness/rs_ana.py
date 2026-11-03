#!/usr/bin/env python3
"""rs_ana.py <rs dir>: write latency of the reshard run (rs_run.sh), from the writes-only soakgen (wsoak.csv) and the
mixed one (soak.csv): per-second p99 and max, in microseconds, split into the 50 s of baseline before the first
reshard, the seconds a reshard was running, and the seconds between reshards. Prints a table and writes latency.json.
soakgen's t_s is seconds since it started, which is within ~0.1 s of the `load-start` event."""
import csv
import json
import os
import statistics
import sys

d = sys.argv[1]
ev = {}
for line in open(os.path.join(d, "events")):
    parts = line.split()
    if len(parts) >= 2:
        ev.setdefault(parts[1], float(parts[0]))
t0 = ev["load-start"]
rounds = []
r = 1
while f"round-{r}-start" in ev:
    rounds.append((ev[f"round-{r}-start"] - t0, ev[f"round-{r}-end"] - t0))
    r += 1
base_end = ev["baseline-end"] - t0
last = ev.get("reshards-done", t0) - t0


def phase(t):
    # an interval [t-1, t]
    if 10 <= t <= base_end:
        return "baseline"
    for a, b in rounds:
        if t > a and t - 1 < b:
            return "reshard"
    if base_end < t <= last:
        return "between"
    return None


out = {}
for name in ("wsoak", "soak"):
    rows = list(csv.DictReader(open(os.path.join(d, name + ".csv"))))
    by = {}
    for row in rows:
        p = phase(float(row["t_s"]))
        if p:
            by.setdefault(p, []).append(row)
    res = {}
    for p, rs in by.items():
        p99 = [int(x["p99_us"]) for x in rs]
        mx = [int(x["max_us"]) for x in rs]
        res[p] = {
            "seconds": len(rs),
            "ops_per_s_median": statistics.median(int(x["ops_per_s"]) for x in rs),
            "errors": sum(int(x["errors"]) for x in rs),
            "p99_us_median": statistics.median(p99),
            "p99_us_max": max(p99),
            "max_us_max": max(mx),
            "p50_us_median": statistics.median(int(x["p50_us"]) for x in rs),
        }
    out[name] = res
out["rounds_s"] = [round(b - a, 1) for a, b in rounds]
json.dump(out, open(os.path.join(d, "latency.json"), "w"), indent=1)
for name, label in (("wsoak", "writes only, 10k/s"), ("soak", "mixed, 90k/s")):
    print(f"{label}:")
    print(f"  {'phase':<9} {'secs':>5} {'ops/s':>7} {'err':>5} {'p50 med':>8} {'p99 med':>8} {'p99 max':>8} {'worst':>8}  (us)")
    for p in ("baseline", "reshard", "between"):
        x = out[name].get(p)
        if x:
            print(f"  {p:<9} {x['seconds']:>5} {x['ops_per_s_median']:>7.0f} {x['errors']:>5} {x['p50_us_median']:>8.0f} "
                  f"{x['p99_us_median']:>8.0f} {x['p99_us_max']:>8} {x['max_us_max']:>8}")
print("reshard durations (s):", out["rounds_s"])
