#!/usr/bin/env python3
"""pl_chart.py <out.svg> <label=run-dir>...: power-off timelines, one panel per run, events marked."""
import csv, json, os, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

plt.rcParams.update({"svg.fonttype": "none", "font.family": "DejaVu Sans", "font.size": 10, "figure.facecolor": "white",
                     "axes.facecolor": "white", "savefig.facecolor": "white", "axes.grid": True, "grid.color": "#e3e3e3",
                     "axes.spines.top": False, "axes.spines.right": False})
out, runs = sys.argv[1], [a.split("=", 1) for a in sys.argv[2:]]
fig, axes = plt.subplots(len(runs), 1, figsize=(9, 2.3 * len(runs) + 0.6), sharex=True, squeeze=False)
for ax, (label, d) in zip(axes[:, 0], runs):
    s = json.load(open(os.path.join(d, "summary.json")))
    rows = list(csv.DictReader(open(os.path.join(d, "timeline.csv"))))
    t = [float(r["t"]) for r in rows]; ok = [int(r["ok"]) for r in rows]; err = [int(r["err"]) for r in rows]
    tb = t[::5]; okb = [sum(ok[i:i + 5]) * 2 for i in range(0, len(ok), 5)]; erb = [sum(err[i:i + 5]) * 2 for i in range(0, len(err), 5)]
    ax.plot(tb, okb, color="#0072B2", lw=1.8, label="acknowledged SETs/s")
    ax.plot(tb, erb, color="#555555", lw=1, ls=(0, (3, 2)), label="failed SETs/s")
    top = max(okb) * 1.55
    for i, (et, en) in enumerate(s["events"]):
        ax.axvline(et, color="#222222", lw=1, ls=":")
        ax.text(et + 1, top * (0.95 - 0.13 * i), en[:40], fontsize=7.5, va="top",
                bbox=dict(facecolor="white", edgecolor="none", pad=0.5, alpha=0.85))
    ax.set_ylim(0, top); ax.set_xlim(0, 120); ax.set_ylabel("SETs / s")
    ax.set_title(f"{label}: {s['lost_acked']:,} of {s['acked']:,} acknowledged writes lost, {s['wrong_value']} wrong",
                 loc="left", fontsize=9.5)
axes[-1, 0].set_xlabel("seconds since the writers started (the run continues to 330 s at the same rate)")
axes[0, 0].legend(loc="upper left", bbox_to_anchor=(1.01, 1.0), frameon=False, fontsize=8)
fig.suptitle("KiviDB 1.2.0-rc8: the master's VM powered off under write load (AOF everysec, aof-ack-after-write yes)",
             x=0.01, ha="left", fontsize=11, fontweight="bold")
fig.tight_layout()
fig.savefig(out, bbox_inches="tight")
