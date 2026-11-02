#!/usr/bin/env python3
"""Summarise run-suite.sh output.

    python3 summarize.py <results-dir>

Reads <results-dir>/<engine>/r<round>/d<size>/<scenario>.{c1,c2}.json (memtier),
*.cpu (sample.sh) and memory.txt, and writes into <results-dir>:

  runs.csv     one row per engine x round x size x scenario. Throughput is the
               sum of both load generators; p50/p99/p99.9 come from the two
               generators' latency histograms merged (HdrHistogram, exact),
               not from averaging their percentiles.
  summary.csv  one row per engine x size x scenario: median throughput over
               the rounds with min and max, median of each percentile, server
               CPU, CPU microseconds per request (user vs kernel), NIC rate,
               and flags for rows limited by the load generators or the NIC.

Needs the `hdrhistogram` package (pip install hdrhistogram).
"""
import csv
import glob
import json
import os
import re
import statistics
import sys

from hdrh.histogram import HdrHistogram

ROOT = sys.argv[1]
NIC_MBPS = float(os.environ.get("NIC_MBPS", "2000"))  # D32ps_v5: 16 Gbps = 2000 MB/s
CLIENT_BUSY = float(os.environ.get("CLIENT_BUSY", "90"))


def cpu(path):
    try:
        f = open(path).read().strip().split(",")
        return dict(zip(["busy", "user", "sys", "softirq", "rx", "tx", "ncpu"], map(float, f)))
    except (OSError, ValueError):
        return None


def load(path):
    try:
        return json.load(open(path))["ALL STATS"]
    except (OSError, ValueError, KeyError):
        return None


rows = []
for c1 in sorted(glob.glob(os.path.join(ROOT, "*", "r*", "d*", "*.c1.json"))):
    d = os.path.dirname(c1)
    scen = os.path.basename(c1)[: -len(".c1.json")]
    if scen == "warm":
        continue
    size = os.path.basename(d)[1:]
    rnd = os.path.basename(os.path.dirname(d))[1:]
    engine = os.path.basename(os.path.dirname(os.path.dirname(d)))
    parts = [load(c1), load(c1.replace(".c1.json", ".c2.json"))]
    if not all(parts):
        rows.append(dict(engine=engine, round=rnd, size=size, scenario=scen, ops="", error="missing client output"))
        continue
    ops = sum(p["Totals"]["Ops/sec"] for p in parts)
    hits = sum(p["Totals"].get("Hits/sec", 0) for p in parts)
    misses = sum(p["Totals"].get("Misses/sec", 0) for p in parts)
    merged = None
    for p in parts:
        h = HdrHistogram.decode(p["Totals"]["Percentile Latencies"]["Histogram log format"]["Compressed Histogram"])
        if merged is None:
            merged = h
        else:
            merged.add(h)
    # memtier records microseconds; check the scale against its own p50.
    own = parts[0]["Totals"]["Percentile Latencies"]["p50.00"]
    single = HdrHistogram.decode(parts[0]["Totals"]["Percentile Latencies"]["Histogram log format"]["Compressed Histogram"])
    scale = 1000.0 if abs(single.get_value_at_percentile(50) / 1000.0 - own) <= max(0.05 * own, 0.002) else 1.0
    pct = {q: merged.get_value_at_percentile(q) / scale for q in (50, 99, 99.9)}
    srv = cpu(c1.replace(".c1.json", ".srv.cpu"))
    cl = [cpu(c1.replace(".c1.json", f".{c}.cpu")) for c in ("c1", "c2")]
    row = dict(engine=engine, round=rnd, size=size, scenario=scen, ops=round(ops),
               miss_pct=round(100 * misses / (hits + misses), 2) if hits + misses else "",
               p50_ms=round(pct[50], 3), p99_ms=round(pct[99], 3), p999_ms=round(pct[99.9], 3),
               avg_ms=round(statistics.mean(p["Totals"]["Average Latency"] for p in parts), 3), error="")
    if srv:
        n = srv["ncpu"]
        row.update(srv_busy=srv["busy"], srv_user=srv["user"], srv_kernel=round(srv["sys"] + srv["softirq"], 1),
                   srv_rx_MBps=srv["rx"], srv_tx_MBps=srv["tx"],
                   us_per_op=round(srv["busy"] / 100 * n * 1e6 / ops, 2) if ops else "",
                   us_user_per_op=round(srv["user"] / 100 * n * 1e6 / ops, 2) if ops else "",
                   us_kernel_per_op=round((srv["sys"] + srv["softirq"]) / 100 * n * 1e6 / ops, 2) if ops else "")
    if all(cl):
        row.update(client_busy_max=max(c["busy"] for c in cl))
    rows.append(row)

cols = ["engine", "round", "size", "scenario", "ops", "miss_pct", "p50_ms", "p99_ms", "p999_ms", "avg_ms",
        "srv_busy", "srv_user", "srv_kernel", "srv_rx_MBps", "srv_tx_MBps", "us_per_op", "us_user_per_op",
        "us_kernel_per_op", "client_busy_max", "error"]
with open(os.path.join(ROOT, "runs.csv"), "w", newline="\n") as f:
    w = csv.DictWriter(f, fieldnames=cols, lineterminator="\n")
    w.writeheader()
    for r in rows:
        w.writerow({c: r.get(c, "") for c in cols})

groups = {}
for r in rows:
    if r.get("ops") == "":
        continue
    groups.setdefault((r["engine"], r["size"], r["scenario"]), []).append(r)


def med(rs, k):
    v = [r[k] for r in rs if r.get(k, "") != ""]
    return round(statistics.median(v), 3) if v else ""


scols = ["engine", "size", "scenario", "rounds", "ops_median", "ops_min", "ops_max", "spread_pct",
         "p50_ms", "p99_ms", "p999_ms", "miss_pct", "srv_busy", "us_per_op", "us_user_per_op", "us_kernel_per_op",
         "srv_rx_MBps", "srv_tx_MBps", "client_busy_max", "limit"]
with open(os.path.join(ROOT, "summary.csv"), "w", newline="\n") as f:
    w = csv.DictWriter(f, fieldnames=scols, lineterminator="\n")
    w.writeheader()
    for (e, s, sc), rs in sorted(groups.items(), key=lambda kv: (kv[0][0], int(kv[0][1]), kv[0][2])):
        ops = [r["ops"] for r in rs]
        m = statistics.median(ops)
        limit = []
        cb = med(rs, "client_busy_max")
        if cb != "" and cb >= CLIENT_BUSY:
            limit.append("client")
        nic = max(med(rs, "srv_rx_MBps") or 0, med(rs, "srv_tx_MBps") or 0)
        if nic >= 0.85 * NIC_MBPS:
            limit.append("nic")
        w.writerow(dict(engine=e, size=s, scenario=sc, rounds=len(rs), ops_median=round(m), ops_min=min(ops),
                        ops_max=max(ops), spread_pct=round(100 * (max(ops) - min(ops)) / m, 1) if m else "",
                        p50_ms=med(rs, "p50_ms"), p99_ms=med(rs, "p99_ms"), p999_ms=med(rs, "p999_ms"),
                        miss_pct=med(rs, "miss_pct"), srv_busy=med(rs, "srv_busy"), us_per_op=med(rs, "us_per_op"),
                        us_user_per_op=med(rs, "us_user_per_op"), us_kernel_per_op=med(rs, "us_kernel_per_op"),
                        srv_rx_MBps=med(rs, "srv_rx_MBps"), srv_tx_MBps=med(rs, "srv_tx_MBps"),
                        client_busy_max=cb, limit="+".join(limit)))
print(open(os.path.join(ROOT, "summary.csv")).read())
