"""analyze.py <verify-dir>: summary numbers, checkpoint durations, chart."""
import csv
import datetime as dt
import re
import statistics as st
import sys
from pathlib import Path

D = Path(sys.argv[1])
RUNS = ["rc4-aof", "fixed-aof", "fixed-noaof", "fixed-aof-15m"]


def rows(p):
    with open(p, newline="") as f:
        return list(csv.DictReader(f))


def ts(line):
    m = re.match(r"(\d{4}-\d\d-\d\dT[\d:.]+)Z", line)
    return dt.datetime.fromisoformat(m.group(1)) if m else None


def checkpoints(log):
    out, start = [], None
    for line in log.read_text(errors="replace").splitlines():
        if "auto-checkpoint:" in line and "folding" in line:
            start = ts(line)
        elif "rebased onto" in line and start is not None:
            out.append((ts(line) - start).total_seconds())
            start = None
    return out, start is not None


summary = []
for r in RUNS:
    p = D / f"{r}.csv"
    if not p.exists():
        continue
    rs = rows(p)
    ru = [int(x["replica_used"]) for x in rs if x["replica_used"]]
    mu = [int(x["master_used"]) for x in rs if x["master_used"]]
    rw = [x["replica_aof_rewrite_in_progress"] for x in rs]
    cps, open_at_end = checkpoints(D / f"{r}.server-replica.log")
    mcps, _ = checkpoints(D / f"{r}.server-master.log")
    sm = D / f"{r}.samples-master.csv"
    lags = []
    if sm.exists():
        lags = [int(x["replica_lag_bytes"]) for x in rows(sm) if x["replica_lag_bytes"] not in ("", None)]
    load = rows(D / f"{r}.load.csv")
    ops = [float(x["ops_per_s"]) for x in load]
    summary.append({
        "run": r,
        "samples": len(rs),
        "replica_used_first_MB": round(ru[0] / 1e6) if ru else "",
        "replica_used_max_MB": round(max(ru) / 1e6) if ru else "",
        "replica_used_last_MB": round(ru[-1] / 1e6) if ru else "",
        "master_used_last_MB": round(mu[-1] / 1e6) if mu else "",
        "rewrite_in_progress_samples": f"{rw.count('1')}/{len(rw)}",
        "replica_checkpoints_done": len(cps),
        "replica_checkpoint_s_median": round(st.median(cps), 2) if cps else "",
        "replica_checkpoint_s_max": round(max(cps), 2) if cps else "",
        "replica_checkpoint_open_at_end": open_at_end,
        "master_checkpoints_done": len(mcps),
        "master_checkpoint_s_median": round(st.median(mcps), 2) if mcps else "",
        "lag_bytes_median": int(st.median(lags)) if lags else "",
        "lag_bytes_max": max(lags) if lags else "",
        "load_ops_per_s_median": round(st.median(ops)) if ops else "",
    })
    with open(D / f"{r}.replica-checkpoints.csv", "w", newline="\n") as f:
        f.write("n,seconds\n")
        for i, c in enumerate(cps, 1):
            f.write(f"{i},{c:.3f}\n")

with open(D / "summary.csv", "w", newline="\n") as f:
    w = csv.DictWriter(f, fieldnames=list(summary[0].keys()), lineterminator="\n")
    w.writeheader()
    w.writerows(summary)
for s in summary:
    print(s)

# Chart: replica used_memory over time, the three 240 s runs.
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

plt.rcParams.update({"svg.fonttype": "none", "font.family": "sans-serif", "font.size": 11})
series = [("rc4-aof", "rc4, replica AOF on", "#D55E00", "-", 4),
          ("fixed-noaof", "fixed, replica AOF off (control)", "#009E73", "-", 7),
          ("fixed-aof", "KiviDB fixed, replica AOF on", "#0072B2", "--", 4)]
fig, ax = plt.subplots(figsize=(9, 5.2), facecolor="white")
ax.set_facecolor("white")
last = {}
for key, label, color, ls, ms in series:
    p = D / f"{key}.csv"
    if not p.exists():
        continue
    rs = rows(p)
    t = [int(x["t_s"]) for x in rs if x["replica_used"]]
    y = [int(x["replica_used"]) / 1e9 for x in rs if x["replica_used"]]
    ax.plot(t, y, color=color, lw=2, ls=ls, label=label, marker="o", ms=ms)
    last[key] = (t[-1], y[-1], label, color)
for key, (x, y, label, color) in last.items():
    if key == "fixed-noaof":
        continue
    text = f"{y:.2f} GB" if key == "rc4-aof" else f"{y:.2f} GB (fixed and control)"
    ax.annotate(text, (x, y), xytext=(6, 10 if key == "fixed-aof" else 0), textcoords="offset points",
                va="center", fontsize=10, color="#333333")
rc4 = last.get("rc4-aof", (0, 0))[1]
fx = last.get("fixed-aof", (0, 0))[1]
ax.set_title(f"With the fix, the replica's memory stays flat: {fx:.2f} GB after 240 s, vs {rc4:.1f} GB on rc4",
             loc="left", fontsize=12.5, color="#111111")
ax.set_xlabel("seconds of load (100k ops/s mixed writes, ~40 MB/s)", color="#444444")
ax.set_ylabel("replica used_memory (GB)", color="#444444")
ax.grid(axis="y", color="#e5e5e5", lw=0.8)
for sp in ("top", "right"):
    ax.spines[sp].set_visible(False)
for sp in ("left", "bottom"):
    ax.spines[sp].set_color("#999999")
ax.tick_params(colors="#555555")
ax.set_ylim(bottom=0)
ax.set_xlim(0, 330)
ax.legend(frameon=False, loc="upper left")
fig.tight_layout()
fig.savefig(D / "replica-memory.svg", facecolor="white")
fig.savefig(Path(__file__).parent / "preview.png", facecolor="white", dpi=100)
print("chart written")
