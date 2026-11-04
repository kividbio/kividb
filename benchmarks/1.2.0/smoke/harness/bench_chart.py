#!/usr/bin/env python3
"""Bar chart of the short redis-benchmark runs in extra_checks.sh (SET and GET, plain TCP), per artifact.

  bench_chart.py results/amd64/extra/bench.tsv results/arm64/extra/bench.tsv charts/bench-set-get.svg
A smoke number, not a benchmark: 4-vCPU VM, one redis-benchmark client on the same VM, -c 50, no pipelining.
"""
import csv
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

plt.rcParams["svg.fonttype"] = "none"
KIVI = "#0072B2"
LIGHT = "#56B4E9"


def load(path, arch):
    rows = []
    with open(path) as f:
        for r in csv.DictReader(f, delimiter="\t"):
            if r["transport"] != "plain":
                continue
            rows.append((f"{r['artifact']}", arch, int(r["SET"]), int(r["GET"])))
    return rows


def main():
    *tsvs, out = sys.argv[1:]
    fig, axes = plt.subplots(1, len(tsvs), figsize=(7 * len(tsvs), 6), facecolor="white", sharey=True)
    if len(tsvs) == 1:
        axes = [axes]
    for ax, p in zip(axes, tsvs):
        arch = "amd64" if "amd64" in p else "arm64"
        rows = load(p, arch)
        names = [r[0].replace(f"-{arch}", "") for r in rows]
        y = range(len(rows))
        ax.barh([i + 0.2 for i in y], [r[2] / 1000 for r in rows], height=0.4, color=KIVI, label="SET")
        ax.barh([i - 0.2 for i in y], [r[3] / 1000 for r in rows], height=0.4, color=LIGHT, label="GET")
        ax.set_yticks(list(y))
        ax.set_yticklabels(names, fontsize=8)
        ax.set_xlabel("thousand requests/s (redis-benchmark -c 50 -n 100000 -d 64)")
        ax.set_title(f"KiviDB 1.2.0-rc7, {arch} (Azure D4{'p' if arch == 'arm64' else ''}s_v5)")
        ax.set_facecolor("white")
        ax.grid(axis="x", alpha=0.3)
        ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.1), ncol=2, frameon=False)
    fig.suptitle("Every published artifact serves: short redis-benchmark, SET and GET (smoke, not a benchmark)")
    fig.tight_layout()
    fig.savefig(out, facecolor="white")


if __name__ == "__main__":
    main()
