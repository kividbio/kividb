#!/usr/bin/env python3
"""reshard_charts.py <chart dir> <label>=<run dir> ...: #310 charts from cl_soak.sh runs with reshards.
  reshard-source-p99.svg      the source shard probe's 1 s p99 around reshard round 1 of each run (time 0 = start)
  reshard-per-connection.svg  share of each source-probe connection's operations over 100 ms during round 1
Static SVG for a Markdown page (no hover layer); colours: the dataviz reference palette's first three categorical
slots, which validate on all pairs."""
import csv
import json
import os
import re
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

SERIES = ["#2a78d6", "#eb6834", "#1baf7a"]
INK, INK2, GRID = "#0b0b0b", "#52514e", "#d9d8d4"
plt.rcParams.update({"svg.fonttype": "none", "font.size": 10, "axes.edgecolor": INK2, "axes.labelcolor": INK,
                     "xtick.color": INK2, "ytick.color": INK2, "text.color": INK, "figure.facecolor": "white",
                     "axes.facecolor": "white", "savefig.facecolor": "white"})
C = sys.argv[1]
PREVIEW = os.environ.get("PREVIEW")  # also write PNGs there, to look at
_save = plt.Figure.savefig


def _savefig(self, fname, *a, **k):
    _save(self, fname, *a, **k)
    if PREVIEW:
        _save(self, os.path.join(PREVIEW, os.path.basename(fname).replace(".svg", ".png")), dpi=90)


plt.Figure.savefig = _savefig
runs = [a.split("=", 1) for a in sys.argv[2:]]
PROBE = {"3000": "a", "8000": "b", "13000": "c"}


def round1(d):
    a = b = src = None
    for line in open(os.path.join(d, "events")):
        m = re.match(r"(\S+) reshard-start round 1 src-shard=(\d+)", line)
        if m:
            a, src = float(m.group(1)), m.group(2)
        m = re.match(r"(\S+) reshard-end round 1 ", line)
        if m:
            b = float(m.group(1))
    return a, b, PROBE[src]


fig, ax = plt.subplots(figsize=(10, 4.2))
for i, (label, d) in enumerate(runs):
    a, b, p = round1(d)
    xs, ys = [], []
    for r in csv.DictReader(open(os.path.join(d, "probe-%s.csv" % p))):
        t = float(r["unix_ts"]) - a
        if -60 <= t <= (b - a) + 60:
            xs.append(t)
            ys.append(max(int(r["p99_us"]), 1) / 1000)
    ax.plot(xs, ys, color=SERIES[i], lw=2, label="%s (reshard %.0f s)" % (label, b - a))
    ax.axvline(b - a, color=SERIES[i], lw=1, ls="--")
    ax.text(b - a + 2, 0.6 if i == 0 else 0.3, "%s: redis-cli done" % label, color=INK2, fontsize=8)
ax.axvline(0, color=INK2, lw=1, ls="--")
ax.text(2, 0.6, "redis-cli --cluster reshard starts", color=INK2, fontsize=8)
ax.set_yscale("log")
ax.set_ylim(0.2, None)
ax.set_xlabel("seconds since the reshard started")
ax.set_ylabel("write p99 per 1 s, ms (log)")
ax.set_title("Source shard's writes during a 128-slot redis-cli reshard at 100k ops/s (keys in slots that stay)", fontsize=10)
ax.grid(color=GRID, lw=0.6, which="major")
for s in ("top", "right"):
    ax.spines[s].set_visible(False)
ax.legend(frameon=False, loc="upper right")
fig.tight_layout()
fig.savefig(os.path.join(C, "reshard-source-p99.svg"))

fig, axs = plt.subplots(1, len(runs), figsize=(4.2 * len(runs), 3.4), sharey=True)
axs = axs if len(runs) > 1 else [axs]
for i, ((label, d), ax) in enumerate(zip(runs, axs)):
    a, b, p = round1(d)
    pc = json.load(open(os.path.join(d, "perconn-%s-1.json" % p)))
    xs = [c["conn"] for c in pc["conns"]]
    ys = [c["over_100ms_pct"] for c in pc["conns"]]
    ax.bar(xs, ys, color=SERIES[i], width=0.7)
    for x, y in zip(xs, ys):
        ax.text(x, y + 1.5, "%.0f" % y, ha="center", fontsize=8, color=INK2)
    ax.set_title(label, fontsize=10)
    ax.set_xticks(xs)
    ax.set_xlabel("probe connection")
    ax.set_ylim(0, 105)
    ax.grid(axis="y", color=GRID, lw=0.6)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
axs[0].set_ylabel("% of its writes over 100 ms")
fig.suptitle("Where the stall falls: each source-probe connection during the reshard (8 connections, threads 4)", fontsize=10)
fig.tight_layout()
fig.savefig(os.path.join(C, "reshard-per-connection.svg"))
print("charts written")
