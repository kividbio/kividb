#!/usr/bin/env python3
"""ra_report.py <ra-out dir> <out-dir>: replica-with-AOF runs -> results/replica-aof/summary.csv + charts/replica-aof-memory.svg"""
import csv, datetime as dt, json, os, re, statistics as st, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

plt.rcParams.update({"svg.fonttype": "none", "font.family": "DejaVu Sans", "font.size": 10, "figure.facecolor": "white",
                     "axes.facecolor": "white", "savefig.facecolor": "white", "axes.grid": True, "grid.color": "#e3e3e3",
                     "axes.spines.top": False, "axes.spines.right": False})
D, O = sys.argv[1], sys.argv[2]
RUNS = [("rc7-aof-15m", "KiviDB 1.2.0-rc7", "#0072B2"), ("rc4-aof-240s", "KiviDB 1.2.0-rc4 (control)", "#56B4E9")]


def ts(line):
    m = re.match(r"(\d{4}-\d\d-\d\dT[\d:.]+)Z", line)
    return dt.datetime.fromisoformat(m.group(1)) if m else None


def checkpoints(path):
    out, start = [], None
    if not os.path.exists(path):
        return out, False
    for line in open(path, errors="replace"):
        if "auto-checkpoint:" in line and "folding" in line:
            start = ts(line)
        elif "rebased onto" in line and start is not None:
            out.append((ts(line) - start).total_seconds()); start = None
    return out, start is not None


rows = []
fig, ax = plt.subplots(figsize=(9, 4))
for name, label, col in RUNS:
    p = os.path.join(D, name + ".csv")
    if not os.path.exists(p):
        continue
    rs = list(csv.DictReader(open(p)))
    t = [int(r["t_s"]) / 60 for r in rs if r["replica_used"]]
    ru = [int(r["replica_used"]) / 1e9 for r in rs if r["replica_used"]]
    mu = [int(r["master_used"]) / 1e9 for r in rs if r["replica_used"]]
    rss = [int(r["replica_rss"]) / 1e9 for r in rs if r["replica_rss"]]
    rw = [r["replica_aof_rewrite_in_progress"] for r in rs]
    cps, open_end = checkpoints(os.path.join(D, name + ".server-replica.log"))
    mcps, _ = checkpoints(os.path.join(D, name + ".server-master.log"))
    load = list(csv.DictReader(open(os.path.join(D, name + ".load.csv"))))
    ops = [float(x["ops_per_s"]) for x in load if x.get("ops_per_s")]
    errs = sum(int(x.get("errors", 0) or 0) for x in load)
    cmp_ = {}
    cj = os.path.join(D, name + ".compare.json")
    if os.path.exists(cj):
        cmp_ = json.load(open(cj))
    # slope of replica used_memory over the second half of the run (MB/min)
    h = len(t) // 2
    slope = (ru[-1] - ru[h]) * 1000 / (t[-1] - t[h]) if len(t) > 3 and t[-1] > t[h] else 0
    rows.append(dict(run=name, minutes=round(t[-1], 1), replica_used_first_GB=round(ru[0], 2), replica_used_max_GB=round(max(ru), 2),
                     replica_used_last_GB=round(ru[-1], 2), replica_used_slope_2nd_half_MB_per_min=round(slope, 1),
                     replica_rss_max_GB=round(max(rss), 2) if rss else "", master_used_last_GB=round(mu[-1], 2),
                     replica_checkpoints_done=len(cps), replica_checkpoint_open_at_end=open_end,
                     replica_checkpoint_s_median=round(st.median(cps), 2) if cps else "",
                     replica_checkpoint_s_max=round(max(cps), 2) if cps else "",
                     master_checkpoints_done=len(mcps), rewrite_in_progress_samples=f"{rw.count('1')}/{len(rw)}",
                     load_ops_per_s_median=round(st.median(ops)) if ops else "", load_errors=errs,
                     compared_keys=cmp_.get("compared", cmp_.get("keys_compared", "")),
                     real_differences=cmp_.get("real_differences", "")))
    ax.plot(t, ru, color=col, lw=2, label=label + ": replica used_memory")
    ax.plot(t, mu, color=col, lw=1, ls=(0, (3, 2)), label=label + ": master used_memory")
ax.set_xlabel("minutes of load (100k ops/s, both nodes appendonly yes, everysec)")
ax.set_ylabel("GB")
ax.set_ylim(0, None)
ax.set_title("Replica with its own AOF under sustained writes: used_memory", loc="left", fontsize=11)
ax.legend(loc="upper left", fontsize=8, frameon=False)
fig.tight_layout()
os.makedirs(os.path.join(O, "charts"), exist_ok=True)
os.makedirs(os.path.join(O, "results", "replica-aof"), exist_ok=True)
fig.savefig(os.path.join(O, "charts", "replica-aof-memory.svg"), bbox_inches="tight")
with open(os.path.join(O, "results", "replica-aof", "summary.csv"), "w", newline="\n") as f:
    w = csv.DictWriter(f, list(rows[0].keys()), lineterminator="\n"); w.writeheader(); w.writerows(rows)
for r in rows:
    print(json.dumps(r))
