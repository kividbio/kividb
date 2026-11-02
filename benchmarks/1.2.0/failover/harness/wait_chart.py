#!/usr/bin/env python3
"""Chart of results/wait-probe.txt (16 writers): share of WAIT 1 200 calls answered 0, and WAITs per second.

  wait_chart.py results/wait-probe.txt charts/wait-under-load.svg
"""
import os
import re
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

ROWS = [  # (header in wait-probe.txt, label, color)
    ("KiviDB rc4, master io_uring (Linux default), replica io_uring", "KiviDB rc4, io_uring master\n(Linux default)", "#0072B2"),
    ("KiviDB rc4, master io_uring, replica tokio", "KiviDB rc4, io_uring master,\ntokio replica", "#0072B2"),
    ("KiviDB rc4, master tokio, replica io_uring", "KiviDB rc4, tokio master,\nio_uring replica", "#0072B2"),
    ("KiviDB rc4, master tokio, replica tokio", "KiviDB rc4, tokio master\nand replica", "#0072B2"),
    ("Redis 8.10.3", "Redis 8.10.3", "#D55E00"),
]


def parse(path):
    blocks, cur = {}, None
    for line in open(path):
        if line.strip() and not line.startswith(" "):
            cur = line.strip()
            blocks[cur] = {}
            continue
        m = re.search(r"(?:writers=(\d+))?\s+->(\d): (\d+) times", line)
        if cur and m:
            if m.group(1):
                blocks[cur]["_w"] = int(m.group(1))
            w = blocks[cur].get("_w")
            if w == 16:
                blocks[cur][int(m.group(2))] = int(m.group(3))
    return blocks


def main():
    b = parse(sys.argv[1])
    labels, zero, rate, cols = [], [], [], []
    for head, lab, col in ROWS:
        d = b.get(head, {})
        n0, n1 = d.get(0, 0), d.get(1, 0)
        if n0 + n1 == 0:
            continue
        labels.append(lab)
        zero.append(100.0 * n0 / (n0 + n1))
        rate.append((n0 + n1) / 10.0)
        cols.append(col)
    fig, axes = plt.subplots(1, 2, figsize=(10, 3.8), sharey=True)
    y = range(len(labels))
    axes[0].barh(y, zero, color=cols, height=0.6)
    for i, v in enumerate(zero):
        axes[0].text(v + 0.5, i, f"{v:.0f}%", va="center", fontsize=9)
    axes[0].set_xlim(0, max(zero + [1]) * 1.25)
    axes[0].set_xlabel("WAIT 1 200 answered 0 after its timeout (% of calls)")
    axes[1].barh(y, rate, color=cols, height=0.6)
    for i, v in enumerate(rate):
        axes[1].text(v + 50, i, f"{v:,.0f}", va="center", fontsize=9)
    axes[1].set_xlim(0, max(rate) * 1.25)
    axes[1].set_xlabel("SET + WAIT pairs completed per second (16 writers)")
    axes[0].set_yticks(list(y))
    axes[0].set_yticklabels(labels, fontsize=8)
    axes[0].invert_yaxis()
    fig.suptitle("WAIT on an io_uring master misses replica acknowledgements under concurrency (1.1.3 too)",
                 x=0.01, ha="left", fontsize=11, fontweight="bold")
    fig.tight_layout()
    fig.savefig(sys.argv[2])
    if os.environ.get("PNG"):
        fig.savefig(sys.argv[2][:-4] + ".png", dpi=80)


if __name__ == "__main__":
    main()
