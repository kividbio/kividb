#!/usr/bin/env python3
"""Failed writes per second after two masters move together: unpatched vs patched (3 runs each)."""
import csv
import json
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

R = sys.argv[1]  # .../e2e
OUT = sys.argv[2]
plt.rcParams.update({"svg.fonttype": "none", "font.family": "DejaVu Sans", "font.size": 10})
COL = {"base": "#D55E00", "patched": "#0072B2", "proposal": "#009E73"}
NAME = {"base": "unpatched a6b1bf38", "patched": "patched 69c2bef2", "proposal": "69c2bef2 + proposed follow-up"}

fig, axes = plt.subplots(3, 1, figsize=(9, 7.6), sharex=True, sharey=True, facecolor="white")
xmax = 0
for ax, label in zip(axes, ("base", "patched", "proposal")):
    ax.set_facecolor("white")
    conv = []
    restart = None
    for rep in (1, 2, 3):
        d = f"{R}/two-masters/{label}-{rep}"
        try:
            rows = list(csv.DictReader(open(f"{d}/writes.csv")))
            s = json.load(open(f"{d}/summary.json"))
        except FileNotFoundError:
            continue
        restart = s["restart_after_kill_s"]
        conv.append(s["all_views_and_state_ok_s"])
        xs = [int(r["t_rel_kill"]) for r in rows]
        ys = [int(r["failed"]) for r in rows]
        xmax = max(xmax, max(xs))
        ax.plot(xs, ys, color=COL[label], lw=1.6, alpha=0.9 if rep == 1 else 0.55, drawstyle="steps-post",
                label=f"{NAME[label]}, 3 runs" if rep == 1 else None)
    ax.axvline(0, color="#555555", lw=1, ls=":")
    ax.axvline(restart, color="#333333", lw=1, ls="--")
    if all(c is not None for c in conv):
        t = restart + max(conv)
        ax.axvline(t, color=COL[label], lw=1, ls="-.")
        lo, hi = f"{min(conv):.1f}", f"{max(conv):.1f}"
        span = lo if lo == hi else f"{lo}-{hi}"
        ax.text(t + 2, 40, f"all 6 views converged\n{span} s after restart",
                color="#333333", fontsize=9, va="center")
    else:
        ax.text(60, 40, "moved nodes never see each other's new address\n(stale CLUSTER SLOTS keeps sending writes to dead IPs)",
                color="#333333", fontsize=9, va="center")
    ax.set_yscale("symlog", linthresh=1)
    ax.set_yticks([0, 1, 10, 100])
    ax.set_yticklabels(["0", "1", "10", "100"])
    ax.grid(axis="y", color="#e5e5e5", lw=0.8)
    for sp in ("top", "right"):
        ax.spines[sp].set_visible(False)
    ax.legend(frameon=False, loc="upper right")
    ax.set_ylabel("failed SETs / s")
axes[0].text(0.8, 300, "n1+n2 stopped", color="#333333", fontsize=9)
axes[0].text(restart + 0.8, 160, "restarted on new IPs", color="#333333", fontsize=9)
axes[0].set_ylim(-0.2, 600)
axes[2].set_xlim(-10, xmax)
axes[2].set_xlabel("seconds since the two masters were stopped (node timeout 5 s, writer ~150 SET/s)")
fig.suptitle("Two masters move to new IPs at once: failed writes per second", x=0.01, ha="left", fontsize=12)
fig.text(0.01, 0.005, "The ~100/s spike near 21 s is CLUSTERDOWN during a needless failover of a restarted master (unpatched and 69c2bef2); the follow-up avoids it.",
         fontsize=8.5, color="#555555")
fig.tight_layout(rect=(0, 0.03, 1, 1))
fig.savefig(OUT, facecolor="white", dpi=110)
print("wrote", OUT)
