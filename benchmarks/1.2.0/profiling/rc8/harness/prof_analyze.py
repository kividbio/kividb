#!/usr/bin/env python3
"""rc8 round of the profiling suite: callgrind instructions per command.

    python3 prof_analyze.py <results.txt> <published results.txt> <charts-dir> > table.md
"""
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

NOW, PUB, OUT = sys.argv[1:4]
WL = ["GET-100", "SET-100", "GET-1K", "SET-1K", "INCR", "DEL", "MIX-100"]
REDIS = {"GET-100": 4139, "SET-100": 5359, "GET-1K": 4250, "SET-1K": 5498, "MIX-100": 9559, "INCR": 4489, "DEL": 4483}


def read(path):
    d = {}
    for line in open(path):
        p = line.split()
        if len(p) == 3 and p[2].isdigit():
            d[(p[0], p[1])] = int(p[2])
    return d


now, pub = read(NOW), read(PUB)
print("| workload | rc7 | rc8 | rc8 vs rc7 | published 1.2.0 (75482abb) | rc8 vs published | Redis 8.10.3 (published) | rc8 vs Redis |")
print("|---|---:|---:|---:|---:|---:|---:|---:|")
for w in WL:
    a, b, c = now[(w, "rc7")], now[(w, "rc8")], pub.get((w, "cand"))
    print(f"| {w} | {a} | {b} | {100 * (b / a - 1):+.1f}% | {c} | {100 * (b / c - 1):+.1f}% | {REDIS[w]} | {100 * (b / REDIS[w] - 1):+.1f}% |")
print("\nOther workloads of WORKLOADS=full (measured in the same runs, not asked for):\n")
print("| workload | rc7 | rc8 | change |")
print("|---|---:|---:|---:|")
for w in sorted({k[0] for k in now} - set(WL), key=lambda x: [k[0] for k in now].index(x)):
    a, b = now[(w, "rc7")], now[(w, "rc8")]
    print(f"| {w} | {a} | {b} | {100 * (b / a - 1):+.1f}% |")

plt.rcParams.update({
    "svg.fonttype": "none", "font.family": "DejaVu Sans", "font.size": 10,
    "figure.facecolor": "white", "axes.facecolor": "white", "savefig.facecolor": "white",
    "text.color": "#222222", "axes.labelcolor": "#222222", "xtick.color": "#222222", "ytick.color": "#222222",
    "axes.edgecolor": "#888888", "axes.grid": True, "grid.color": "#e3e3e3", "grid.linewidth": 0.8,
    "axes.axisbelow": True, "axes.spines.top": False, "axes.spines.right": False,
})
fig, ax = plt.subplots(figsize=(10, 4.4))
x = range(len(WL))
wd = 0.27
for i, (lab, col, vals) in enumerate((
        ("KiviDB 1.2.0-rc7 (598b74d)", "#8c8c8c", [now[(w, "rc7")] for w in WL]),
        ("KiviDB 1.2.0-rc8 (082dab3)", "#0072B2", [now[(w, "rc8")] for w in WL]),
        ("Redis 8.10.3 (published run)", "#D55E00", [REDIS[w] for w in WL]))):
    xs = [j + (i - 1) * wd for j in x]
    ax.bar(xs, vals, wd, color=col, label=lab)
    if i == 1:
        for j, w in enumerate(WL):
            ch = 100 * (now[(w, "rc8")] / now[(w, "rc7")] - 1)
            lab_ch = "0.0%" if abs(ch) < 0.05 else f"{ch:+.1f}%"
            ax.annotate(lab_ch, (xs[j], vals[j]), xytext=(0, 3), textcoords="offset points",
                        ha="center", fontsize=8, color="#0072B2", fontweight="bold")
ax.set_xticks(list(x), WL)
ax.set_ylabel("instructions per command (one thread, callgrind)")
ax.grid(axis="x", visible=False)
ax.legend(frameon=False, loc="upper left", ncol=3, bbox_to_anchor=(0, -0.1))
ax.set_title("Instructions per command, KiviDB 1.2.0-rc8 vs rc7 (labels: rc8 vs rc7)\n"
             "whole server path, io_uring loop, pipeline 16; deterministic, one run each",
             loc="left", fontsize=11, fontweight="bold")
fig.tight_layout()
os.makedirs(OUT, exist_ok=True)
fig.savefig(os.path.join(OUT, "instructions-rc7-vs-rc8.svg"), format="svg", bbox_inches="tight")
if os.environ.get("PREVIEW"):
    fig.savefig(os.environ["PREVIEW"], dpi=80, bbox_inches="tight")
