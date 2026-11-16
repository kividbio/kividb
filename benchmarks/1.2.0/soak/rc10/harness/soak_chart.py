#!/usr/bin/env python3
"""soak_chart.py <cl dir> <chart dir>: cl-throughput.svg, the mix's completed ops/s per second through the soak (and the
tail load after 120 min), with the reshard windows shaded and the other events marked, and errors/s below.
It replaces clreport.py's latency chart for rc10: during the reshards soakgen's latencies went past its 60 s histogram,
so those intervals record 0 and a latency plot would show gaps where the worst was. Throughput is not capped.
Static SVG; colours: the dataviz reference palette's slots 1-2."""
import csv
import os
import re
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

D, C = sys.argv[1], sys.argv[2]
BLUE, ORANGE, INK, INK2, GRID, SHADE = "#2a78d6", "#eb6834", "#0b0b0b", "#52514e", "#d9d8d4", "#efeeea"
plt.rcParams.update({"svg.fonttype": "none", "font.size": 10, "axes.edgecolor": INK2, "xtick.color": INK2,
                     "ytick.color": INK2, "text.color": INK, "figure.facecolor": "white", "savefig.facecolor": "white"})
t0 = float(open(os.path.join(D, "soak.start")).read())
rows = []
for r in csv.DictReader(open(os.path.join(D, "soak.csv"))):
    rows.append((float(r["t_s"]) / 60, int(r["ops_per_s"]), int(r["errors"])))
tail = os.path.join(D, "tail")
if os.path.exists(os.path.join(tail, "soak.csv")):
    ts = float(open(os.path.join(tail, "start")).read())
    for r in csv.DictReader(open(os.path.join(tail, "soak.csv"))):
        rows.append(((ts - t0 + float(r["t_s"])) / 60, int(r["ops_per_s"]), int(r["errors"])))
ev = [(float(l.split()[0]), l.split()[1], l) for l in open(os.path.join(D, "events")) if len(l.split()) > 1]
resh = {}
for t, n, l in ev:
    m = re.search(r"reshard-(start|end) round (\d+)", l)
    if m:
        resh.setdefault(m.group(2), {})[m.group(1)] = (t - t0) / 60
marks = [((t - t0) / 60, n.split("-")[0]) for t, n, l in ev if n in ("failover-start", "kill9-start", "sigterm-start", "import-start")]
short = {"failover": "failover", "kill9": "kill -9", "sigterm": "SIGTERM", "import": "import"}

fig, (ax, ax2) = plt.subplots(2, 1, figsize=(10, 5.4), sharex=True, gridspec_kw={"height_ratios": [3, 1.1]})
for k, w in sorted(resh.items()):
    if "end" in w:
        for a in (ax, ax2):
            a.axvspan(w["start"], w["end"], color=SHADE, lw=0)
        ax.text((w["start"] + w["end"]) / 2, 60, "redis-cli reshard %s\n%.0f s" % (k, (w["end"] - w["start"]) * 60),
                ha="center", va="top", fontsize=8, color=INK2)
ax.plot([r[0] for r in rows], [r[1] / 1000 for r in rows], color=BLUE, lw=1)
for i, (x, k) in enumerate(marks):
    ax.axvline(x, color=INK2, lw=0.8, ls="--")
    ax.text(x + 0.4, 6 + (i % 3) * 7, short[k], fontsize=7, color=INK2, rotation=90, va="bottom")
ax.axvline((float(open(os.path.join(D, "soak.end")).read()) - t0) / 60, color=INK, lw=1)
ax.set_ylim(0, 115)
ax.set_ylabel("mix, thousand ops/s")
ax.set_title("KiviDB 1.2.0-rc10 cluster soak: the 100k ops/s mix through the events (shaded: redis-cli reshards)", fontsize=10)
ax.grid(color=GRID, lw=0.6)
ax2.bar([r[0] for r in rows], [r[2] for r in rows], width=1 / 60, color=ORANGE)
ax2.set_ylabel("errors / s")
ax2.set_xlabel("minutes since the soak started (solid line: end of cl_soak.sh's load; after it, the tail load)")
ax2.grid(color=GRID, lw=0.6)
for a in (ax, ax2):
    for s in ("top", "right"):
        a.spines[s].set_visible(False)
fig.tight_layout()
fig.savefig(os.path.join(C, "cl-throughput.svg"))
if os.environ.get("PREVIEW"):
    fig.savefig(os.path.join(os.environ["PREVIEW"], "cl-throughput.png"), dpi=90)
print("chart written")
