#!/usr/bin/env python3
"""Charts for the replica-memory finding.

  replica_mem_report.py <results-dir> <charts-dir>

<results-dir>/replica-memory/<name>.csv from replica_mem.sh, and <results-dir>/run1-replica-aof/ (the soak
run whose replica was OOM-killed: samples-master.csv, samples-replica.csv, soak.start).
"""
import csv
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

plt.rcParams.update({
    "svg.fonttype": "none", "font.family": "DejaVu Sans", "font.size": 10,
    "figure.facecolor": "white", "axes.facecolor": "white", "savefig.facecolor": "white",
    "axes.edgecolor": "#444444", "text.color": "#222222", "axes.grid": True, "grid.color": "#e3e3e3",
    "axes.spines.top": False, "axes.spines.right": False,
})
GB = 2 ** 30


def save(fig, path):
    fig.savefig(path)
    if os.environ.get("PNG"):
        fig.savefig(path[:-4] + ".png", dpi=80)


def main():
    res, ch = sys.argv[1], sys.argv[2]
    os.makedirs(ch, exist_ok=True)
    # 1. the OOM in the soak
    d = os.path.join(res, "run1-replica-aof")
    t0 = float(open(os.path.join(d, "soak.start")).read())

    def load(name, col):
        xs, ys = [], []
        for r in csv.DictReader(open(os.path.join(d, name))):
            try:
                xv = (float(r["ts"]) - t0) / 60
                yv = float(r[col]) * (1024 if col == "rss_kb" else 1) / GB
            except (ValueError, TypeError):
                continue
            xs.append(xv)
            ys.append(yv)
        return xs, ys

    fig, ax = plt.subplots(figsize=(9, 3.8))
    x, y = load("samples-replica.csv", "rss_kb")
    ax.plot(x, y, color="#0072B2", lw=2, label="replica RSS (AOF on)")
    x2, y2 = load("samples-master.csv", "rss_kb")
    x2, y2 = zip(*[(a, b) for a, b in zip(x2, y2) if a <= 40])
    ax.plot(x2, y2, color="#0072B2", lw=1.6, ls="--", label="master RSS (AOF on)")
    kill = max(x)
    ax.axvline(kill, color="#222222", lw=1, ls=":")
    ax.text(kill - 0.5, 31, "replica OOM-killed\n(32 GB VM)", ha="right", va="top", fontsize=9)
    ax.set_xlim(0, 40)
    ax.set_ylim(0, 34)
    ax.set_xlabel("minutes into the soak (100k ops/s)")
    ax.set_ylabel("GiB")
    ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1), frameon=False)
    ax.set_title("KiviDB 1.2.0-rc4: a replica with the AOF on grows ~1.3 GB a minute until the OOM killer stops it",
                 loc="left", fontsize=10)
    fig.tight_layout()
    save(fig, os.path.join(ch, "replica-aof-oom.svg"))
    plt.close(fig)

    # 2. the version matrix
    runs = [("rc4-aof", "1.2.0-rc4, replica AOF on, ~200k ops/s", "#0072B2", "-"),
            ("rc4-aof-20k", "1.2.0-rc4, replica AOF on, ~120k ops/s", "#0072B2", "-."),
            ("rc3-aof", "1.2.0-rc3, replica AOF on, ~200k ops/s", "#0072B2", "--"),
            ("rc4-noaof", "1.2.0-rc4, replica AOF off, ~200k ops/s", "#0072B2", ":"),
            ("113-aof", "1.1.3, replica AOF on, ~200k ops/s", "#56B4E9", "-"),
            ("release-1.2.0-93524a1b-fix-aof", "release/1.2.0 93524a1b (fix), AOF on, 100k ops/s", "#0072B2", (0, (6, 1, 1, 1)))]
    fig, ax = plt.subplots(figsize=(9, 4.2))
    top = 0
    for name, label, col, ls in runs:
        p = os.path.join(res, "replica-memory", name + ".csv")
        if not os.path.exists(p):
            continue
        rows = list(csv.DictReader(open(p)))
        t = [float(r["t_s"]) for r in rows if r["replica_used"]]
        u = [float(r["replica_used"]) / GB for r in rows if r["replica_used"]]
        ax.plot(t, u, color=col, ls=ls, lw=2, label=label)
        ax.text(t[-1] + 4, u[-1], f"{u[-1]:.1f}", va="center", fontsize=8)
        top = max(top, max(u))
    ax.set_ylim(0, top * 1.15)
    ax.set_xlim(0, 330)
    ax.set_xlabel("seconds of load (the soak's mix); the master stays at ~1.0 GB in every run")
    ax.set_ylabel("replica used_memory (GiB)")
    ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1), frameon=False, fontsize=8)
    ax.set_title("Replica memory: unbounded with the replica's AOF on in rc3 and rc4; flat on 1.1.3, with it off, or with the fix",
                 loc="left", fontsize=10)
    fig.tight_layout()
    save(fig, os.path.join(ch, "replica-memory-versions.svg"))
    plt.close(fig)


if __name__ == "__main__":
    main()
