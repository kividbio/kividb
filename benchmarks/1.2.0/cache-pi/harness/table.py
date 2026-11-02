#!/usr/bin/env python3
"""table.py <results-dir> -- markdown tables (median of repeats, [min-max]) from results/runs.csv"""
import csv, os, statistics, sys

R = sys.argv[1]
rows = [r for r in csv.DictReader(open(os.path.join(R, "runs.csv"))) if r.get("gets")]
ORDER = ["kividb-rc4", "kividb-master", "kividb-1.1.3", "kividb-48280b7", "redis-8.10.3", "redis-6bf6224"]
NAME = {"kividb-rc4": "KiviDB 1.2.0-rc4", "kividb-master": "KiviDB master 348cc2d (dry run)",
        "kividb-1.1.3": "KiviDB 1.1.3", "kividb-48280b7": "KiviDB 48280b7 (1.2.0-rc2, reporter's)",
        "redis-8.10.3": "Redis 8.10.3", "redis-6bf6224": "Redis unstable 6bf6224 (reporter's)"}
LEN = {"F": "33,554,432 GETs (the reporter's -benchtime)", "S": "8,388,608 GETs"}


def cell(v, fmt, spread=True):
    med = statistics.median(v)
    s = fmt.format(med)
    if spread and len(v) > 1:
        s += f" [{fmt.format(min(v))}-{fmt.format(max(v))}]"
    return s


for length in ("F", "S"):
    for pol in ("allkeys-lfu", "allkeys-lru"):
        g = [r for r in rows if r["length"] == length and r["policy"] == pol]
        if not g:
            continue
        print(f"\n#### {pol}, {LEN[length]}\n")
        print("| engine | runs | hit rate % | per-memory hit rate % | VmHWM MiB | gets/s | hits/s | hit/s/mem "
              "| P50 / P90 / P99 / P99.9 us | server CPU % | client CPU % of 3 cores | errors |")
        print("|---|---|---|---|---|---|---|---|---|---|---|---|")
        for e in ORDER:
            x = [r for r in g if r["engine"] == e]
            if not x:
                continue
            f = lambda m: [float(r[m]) for r in x if r.get(m) not in ("", None)]
            lat = " / ".join(f"{statistics.median(f(m)):,.0f}" for m in ("p50_us", "p90_us", "p99_us", "p999_us"))
            err = sum(int(r["errors"]) for r in x)
            miss = [int(r["server_gets_missing"] or 0) for r in x]
            errs = f"{err} timeouts in {sum(1 for r in x if int(r['errors']))} runs" if err else "0"
            cut = [m for m in miss if m > 0]
            if cut:
                errs += f"; {len(cut)} runs ended warmup early (~{statistics.median(cut) / 1e6:.1f}M GETs unsent)"
            extra = [-m for m in miss if m < 0]
            if extra:
                errs += f"; {len(extra)} runs had client retries ({max(extra)} extra GETs at most)"
            print(f"| {NAME[e]} | {len(x)} | {cell(f('hit_rate'), '{:.2f}')} | {cell(f('per_mem_hit_rate'), '{:.2f}')} "
                  f"| {cell([v / 1024 for v in f('vmhwm_kb')], '{:,.0f}')} | {cell(f('gets_per_s'), '{:,.0f}')} "
                  f"| {cell(f('hits_per_s'), '{:,.0f}')} | {cell(f('hit_per_s_mem'), '{:,.0f}')} | {lat} "
                  f"| {statistics.median(f('server_cpu_pct')):.0f} | {statistics.median(f('client_cpu_pct_of_300')) / 3:.0f} | {errs} |")
