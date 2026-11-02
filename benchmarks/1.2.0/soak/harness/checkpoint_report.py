#!/usr/bin/env python3
"""Chart and table for checkpoint_probe.sh: p99.9 / max latency per second with checkpoints marked.

  checkpoint_report.py <probe-dir> <charts-dir>
"""
import csv
import datetime as dt
import json
import os
import re
import statistics
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def _save(fig, path, **kw):
    fig.savefig(path, **kw)
    if os.environ.get("PNG"):  # a PNG beside each SVG, for a quick look; not published
        fig.savefig(path[:-4] + ".png", dpi=80, **kw)

plt.rcParams.update({
    "svg.fonttype": "none", "font.family": "DejaVu Sans", "font.size": 10,
    "figure.facecolor": "white", "axes.facecolor": "white", "savefig.facecolor": "white",
    "axes.edgecolor": "#444444", "text.color": "#222222", "axes.grid": True, "grid.color": "#e3e3e3",
    "axes.spines.top": False, "axes.spines.right": False,
})
SETS = [("kividb-default", "KiviDB 1.2.0-rc4, default checkpoint settings", "#0072B2"),
        ("kividb-min1gb", "KiviDB 1.2.0-rc4, auto-aof-rewrite-min-size 1 GB", "#0072B2"),
        ("redis", "Redis 8.10.3, default auto-aof-rewrite settings", "#D55E00")]


def times(log):
    out = []
    for line in open(log):
        m = re.match(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d+)Z", line)
        if m and "rebased onto" in line:
            out.append(dt.datetime.fromisoformat(m.group(1)).replace(tzinfo=dt.timezone.utc).timestamp())
            continue
        m = re.search(r"(\d\d \w\w\w \d{4} \d\d:\d\d:\d\d\.\d+) \* Background append only file rewriting started", line)
        if m:
            out.append(dt.datetime.strptime(m.group(1), "%d %b %Y %H:%M:%S.%f").replace(tzinfo=dt.timezone.utc).timestamp())
    return out


def main():
    d, ch = sys.argv[1], sys.argv[2]
    os.makedirs(ch, exist_ok=True)
    summ = {}
    top = 0
    data = []
    for name, label, col in SETS:
        if not os.path.exists(os.path.join(d, name + ".csv")):
            continue
        t0 = float(open(os.path.join(d, name + ".start")).read())
        rows = list(csv.DictReader(open(os.path.join(d, name + ".csv"))))
        t = [float(r["t_s"]) for r in rows]
        p999 = [float(r["p999_us"]) / 1000 for r in rows]
        mx = [float(r["max_us"]) / 1000 for r in rows]
        p99 = [float(r["p99_us"]) / 1000 for r in rows]
        cps = [x - t0 for x in times(os.path.join(d, name + ".log")) if 0 <= x - t0 <= t[-1]]
        data.append((name, label, col, t, p99, p999, mx, cps))
        top = max(top, max(mx))
        summ[name] = {"checkpoints_in_180s": len(cps), "p99_ms_median": round(statistics.median(p99), 2),
                      "p999_ms_median": round(statistics.median(p999), 2), "p999_ms_max": round(max(p999), 1),
                      "max_ms": round(max(mx), 1),
                      "seconds_with_p999_over_10ms": sum(1 for v in p999 if v > 10),
                      "seconds_with_p999_over_30ms": sum(1 for v in p999 if v > 30),
                      "seconds_with_max_over_50ms": sum(1 for v in mx if v > 50)}
    fig, axes = plt.subplots(len(data), 1, figsize=(9, 2.6 * len(data) + 0.8), sharex=True, squeeze=False)
    axes = axes[:, 0]
    for ax, (name, label, col, t, p99, p999, mx, cps) in zip(axes, data):
        ax.plot(t, mx, color=col, lw=1, alpha=0.45, label="max")
        ax.plot(t, p999, color=col, lw=1.8, label="p99.9")
        for c in cps:
            ax.axvline(c, color="#222222", lw=0.7, ls=":")
        ax.set_yscale("log")
        ax.set_ylim(0.1, top * 1.5)
        ax.set_ylabel("ms (log)")
        ax.set_title(f"{label}: {len(cps)} checkpoints/rewrites (dotted), p99.9 > 10 ms in "
                     f"{summ[name]['seconds_with_p999_over_10ms']} of {len(t)} s", loc="left", fontsize=9)
    axes[0].legend(loc="upper left", bbox_to_anchor=(1.01, 1), frameon=False)
    axes[-1].set_xlabel("seconds (100k ops/s, the soak's mix, 128 connections, 1 s intervals)")
    fig.suptitle("Both rewrite the AOF every ~11 s under this load; KiviDB's checkpoint stalls writes longer than Redis's fork",
                 x=0.01, ha="left", fontsize=11, fontweight="bold")
    fig.tight_layout()
    _save(fig, os.path.join(ch, "checkpoint-latency.svg"))
    json.dump(summ, open(os.path.join(d, "checkpoint-summary.json"), "w"), indent=1)
    print(json.dumps(summ, indent=1))


if __name__ == "__main__":
    main()
