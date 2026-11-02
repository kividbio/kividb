"""summarize.py RAW_DIR OUT_DIR

RAW_DIR/<engine>/<label>.json are memtier_benchmark JSON outputs (labels
<cmd>-<V>-p<P>-r<R>, or incr-p<P>-r<R>), RAW_DIR/<engine>/server-cpu.csv the
server's CPU seconds per run and RAW_DIR/<engine>/memory.csv the RSS after
loading each group. Writes OUT_DIR/runs.csv (one row per measured run),
OUT_DIR/summary.csv (median of the repeats, with min and max) and
OUT_DIR/memory.csv.
"""
import csv
import json
import os
import re
import statistics
import sys

RAW, OUT = sys.argv[1], sys.argv[2]
LABEL = re.compile(r"^(?P<cmd>[a-z0-9]+?)(?:-(?P<v>\d+))?-p(?P<p>\d+)-r(?P<r>\d+)$")


def load_cpu(path):
    out = {}
    if os.path.exists(path):
        for row in csv.DictReader(open(path)):
            try:
                out[row["label"]] = (float(row["server_cpu_s"]), float(row["wall_s"]))
            except (KeyError, ValueError):
                pass
    return out


runs = []
memory = []
for engine in sorted(os.listdir(RAW)):
    d = os.path.join(RAW, engine)
    if not os.path.isdir(d):
        continue
    cpu = load_cpu(os.path.join(d, "server-cpu.csv"))
    mpath = os.path.join(d, "memory.csv")
    if os.path.exists(mpath):
        for row in csv.DictReader(open(mpath)):
            memory.append({"engine": engine, **row})
    for f in sorted(os.listdir(d)):
        if not f.endswith(".json"):
            continue
        label = f[:-5]
        m = LABEL.match(label)
        if not m:
            continue
        try:
            j = json.load(open(os.path.join(d, f)))
        except (OSError, ValueError):
            continue
        a = j.get("ALL STATS", {})
        t = a.get("Totals", {})
        pl = t.get("Percentile Latencies", {})
        c = a.get("CPU", {})
        txt = os.path.join(d, label + ".txt")
        errors = 0
        if os.path.exists(txt):
            errors = sum(1 for line in open(txt, errors="replace") if "error" in line.lower())
        scpu, wall = cpu.get(label, (0.0, 0.0))
        rt = a.get("Runtime", {}).get("Total duration", 0) / 1000.0
        runs.append({
            "engine": engine,
            "command": m["cmd"].upper().replace("LRANGE100", "LRANGE_100"),
            "value_bytes": m["v"] or "",
            "pipeline": int(m["p"]),
            "repeat": int(m["r"]),
            "requests": t.get("Count", 0),
            "ops_per_sec": round(t.get("Ops/sec", 0.0)),
            "p50_ms": pl.get("p50.00", ""),
            "p99_ms": pl.get("p99.00", ""),
            "p999_ms": pl.get("p99.90", ""),
            "avg_ms": t.get("Average Latency", ""),
            "rx_MBps": round(t.get("KB/sec RX", 0.0) / 1024, 1),
            "tx_MBps": round(t.get("KB/sec TX", 0.0) / 1024, 1),
            "duration_s": round(rt, 2),
            "client_cpu_pct": c.get("avg_cpu_utilization_pct", ""),
            "server_cores": round(scpu / wall, 2) if wall else "",
            "error_lines": errors,
        })

os.makedirs(OUT, exist_ok=True)
if runs:
    with open(os.path.join(OUT, "runs.csv"), "w", newline="\n") as fh:
        w = csv.DictWriter(fh, fieldnames=list(runs[0].keys()), lineterminator="\n")
        w.writeheader()
        w.writerows(runs)

groups = {}
for r in runs:
    groups.setdefault((r["engine"], r["command"], r["value_bytes"], r["pipeline"]), []).append(r)
summary = []
for (engine, cmd, v, p), rs in sorted(groups.items()):
    ops = [r["ops_per_sec"] for r in rs]
    med = lambda k: statistics.median([float(r[k]) for r in rs if r[k] != ""]) if any(r[k] != "" for r in rs) else ""
    summary.append({
        "engine": engine, "command": cmd, "value_bytes": v, "pipeline": p, "repeats": len(rs),
        "ops_per_sec_median": round(statistics.median(ops)),
        "ops_per_sec_min": min(ops), "ops_per_sec_max": max(ops),
        "spread_pct": round((max(ops) - min(ops)) * 100.0 / statistics.median(ops), 1) if statistics.median(ops) else "",
        "p50_ms": med("p50_ms"), "p99_ms": med("p99_ms"), "p999_ms": med("p999_ms"),
        "rx_MBps": med("rx_MBps"), "tx_MBps": med("tx_MBps"),
        "client_cpu_pct_max": max(float(r["client_cpu_pct"] or 0) for r in rs),
        "server_cores": med("server_cores"),
        "error_lines": sum(r["error_lines"] for r in rs),
    })
if summary:
    with open(os.path.join(OUT, "summary.csv"), "w", newline="\n") as fh:
        w = csv.DictWriter(fh, fieldnames=list(summary[0].keys()), lineterminator="\n")
        w.writeheader()
        w.writerows(summary)
if memory:
    with open(os.path.join(OUT, "memory.csv"), "w", newline="\n") as fh:
        w = csv.DictWriter(fh, fieldnames=["engine", "label", "rss_kb", "disk_kb"], lineterminator="\n")
        w.writeheader()
        w.writerows(memory)
print(f"{len(runs)} runs, {len(summary)} cells, {len(memory)} memory rows")
