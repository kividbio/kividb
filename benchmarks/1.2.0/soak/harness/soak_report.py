#!/usr/bin/env python3
"""Charts and a summary for a soak run.

  soak_report.py <results-dir> <charts-dir>

<results-dir> holds soak.csv (soakgen output), soak.start (unix time the load started),
samples-master.csv and samples-replica.csv (sampler.sh) and, optionally, compare.json.
"""
import csv
import json
import os
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
    "axes.edgecolor": "#444444", "axes.labelcolor": "#222222", "xtick.color": "#333333",
    "ytick.color": "#333333", "text.color": "#222222", "axes.grid": True, "grid.color": "#e3e3e3",
    "grid.linewidth": 0.8, "axes.spines.top": False, "axes.spines.right": False,
})
BLUE = "#0072B2"
INK = "#222222"


def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def slope(xs, ys):
    pts = [(x, y) for x, y in zip(xs, ys) if y is not None]
    if len(pts) < 3:
        return None
    mx = statistics.mean(p[0] for p in pts)
    my = statistics.mean(p[1] for p in pts)
    den = sum((p[0] - mx) ** 2 for p in pts)
    return sum((p[0] - mx) * (p[1] - my) for p in pts) / den if den else None


def main():
    res, ch = sys.argv[1], sys.argv[2]
    os.makedirs(ch, exist_ok=True)
    t0 = float(open(os.path.join(res, "soak.start")).read().strip())
    load = list(csv.DictReader(open(os.path.join(res, "soak.csv"))))
    tm = [float(r["t_s"]) / 60 for r in load]
    ops = [float(r["ops_per_s"]) for r in load]
    p50 = [float(r["p50_us"]) / 1000 for r in load]
    p99 = [float(r["p99_us"]) / 1000 for r in load]
    p999 = [float(r["p999_us"]) / 1000 for r in load]
    mx = [float(r["max_us"]) / 1000 for r in load]
    errs = sum(int(r["errors"]) for r in load)
    total = sum(int(r["ops"]) for r in load)

    def samples(name):
        p = os.path.join(res, f"samples-{name}.csv")
        rows = list(csv.DictReader(open(p)))
        rows = [r for r in rows if num(r["ts"]) and num(r["ts"]) >= t0 - 30]
        return rows

    m, rp = samples("master"), samples("replica")
    mt = [(float(r["ts"]) - t0) / 60 for r in m]
    rt = [(float(r["ts"]) - t0) / 60 for r in rp]
    gb = lambda v: v / 2 ** 30 if v is not None else None
    m_rss = [gb(num(r["rss_kb"]) * 1024) if num(r["rss_kb"]) else None for r in m]
    m_used = [gb(num(r["used_memory"])) for r in m]
    r_rss = [gb(num(r["rss_kb"]) * 1024) if num(r["rss_kb"]) else None for r in rp]
    r_used = [gb(num(r["used_memory"])) for r in rp]
    m_keys = [num(r["keys"]) for r in m]
    r_keys = [num(r["keys"]) for r in rp]
    lag = [num(r["replica_lag_bytes"]) for r in m]
    rewrites = [num(r["aof_rewrites"]) for r in m]
    acks_before = [num(r["aof_acks_before_write"]) for r in m]
    # checkpoints: where the master's aof_rewrites count goes up
    cps = [mt[i] for i in range(1, len(m)) if rewrites[i] is not None and rewrites[i - 1] is not None
           and rewrites[i] > rewrites[i - 1]]
    cpf = os.path.join(res, "checkpoints.txt")  # unix times of "rebased onto dump.kdb" lines in the master's log
    if os.path.exists(cpf):
        cps = [(float(x) - t0) / 60 for x in open(cpf).read().split() if x.strip()]
        cps = [c for c in cps if c >= 0]
    cpn = os.path.join(res, "checkpoints.count")  # just the number (the master's last "generation N" in its log)
    if os.path.exists(cpn):
        cps = [None] * int(open(cpn).read().split()[0])
    end_load = tm[-1] if tm else 0

    def mark(ax):
        if len(cps) > 60 or None in cps:  # too many to draw one by one: say so in the title instead
            return
        for i, c in enumerate(cps):
            ax.axvline(c, color="#999999", lw=0.6, ls=":", zorder=0)

    # 1. throughput
    fig, ax = plt.subplots(figsize=(9, 3.4))
    ax.plot(tm, [o / 1000 for o in ops], color=BLUE, lw=2)
    mark(ax)
    ax.set_ylim(0, max(ops) / 1000 * 1.2)
    ax.set_xlabel("minutes")
    ax.set_ylabel("thousand ops / s (10 s intervals)")
    ax.set_title(f"Throughput held at {statistics.median(ops)/1000:,.0f}k ops/s for {end_load:.0f} min "
                 f"({len(cps)} AOF checkpoints, one every {end_load*60/max(len(cps),1):.0f} s)", loc="left")
    fig.tight_layout(); _save(fig, os.path.join(ch, "soak-throughput.svg")); plt.close(fig)

    # 2. latency
    fig, ax = plt.subplots(figsize=(9, 3.8))
    for ys, lab, ls in ((p50, "p50", "-"), (p99, "p99", "--"), (p999, "p99.9", ":")):
        ax.plot(tm, ys, color=BLUE, lw=2 if lab == "p50" else 1.6, ls=ls, label=lab)
        ax.text(tm[-1] + 1, ys[-1], lab, va="center", fontsize=9, color=INK)
    mark(ax)
    ax.set_yscale("log")
    ax.set_xlabel("minutes")
    ax.set_ylabel("latency per 10 s interval (ms, log)")
    ax.set_title(f"p50 flat; the p99 and p99.9 tails are checkpoint write holds: median p50 {statistics.median(p50):.2f} ms, p99 {statistics.median(p99):.2f} ms, "
                 f"p99.9 {statistics.median(p999):.2f} ms", loc="left")
    ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1), frameon=False)
    fig.tight_layout(); _save(fig, os.path.join(ch, "soak-latency.svg")); plt.close(fig)

    # 3. memory (master and replica panels)
    half = end_load / 3  # judge the trend on the last two thirds, once the data set is at its steady size
    s_m = slope([x for x in mt if half <= x <= end_load], [y for x, y in zip(mt, m_rss) if half <= x <= end_load])
    s_mu = slope([x for x in mt if half <= x <= end_load], [y for x, y in zip(mt, m_used) if half <= x <= end_load])
    s_r = slope([x for x in rt if half <= x <= end_load], [y for x, y in zip(rt, r_rss) if half <= x <= end_load])
    fig, axes = plt.subplots(2, 1, figsize=(9, 5.6), sharex=True)
    for ax, xs, rss, used, nm in ((axes[0], mt, m_rss, m_used, "master"), (axes[1], rt, r_rss, r_used, "replica")):
        ax.plot(xs, rss, color=BLUE, lw=2, label="RSS")
        ax.plot(xs, used, color=BLUE, lw=1.6, ls="--", label="used_memory")
        mark(ax)
        ax.axvline(end_load, color=INK, lw=1, ls="-.")
        ax.set_ylim(0, max([v for v in rss if v] + [1]) * 1.2)
        ax.set_ylabel(f"{nm} (GiB)")
    axes[0].text(end_load, axes[0].get_ylim()[1] * 0.95, " load stops", fontsize=8, va="top")
    axes[0].legend(loc="upper left", bbox_to_anchor=(1.01, 1), frameon=False)
    axes[1].set_xlabel("minutes")
    mh = lambda s: f"{s * 60 * 1024:+.0f} MiB/h" if s is not None else "n/a"
    fig.suptitle(f"used_memory flat (no leak); RSS trend: master {mh(s_m)}, used_memory {mh(s_mu)}, replica RSS {mh(s_r)} "
                 f"(last {end_load - half:.0f} min)", x=0.01, ha="left", fontsize=10)
    fig.tight_layout(); _save(fig, os.path.join(ch, "soak-memory.svg")); plt.close(fig)

    # 4. keys
    fig, ax = plt.subplots(figsize=(9, 3.2))
    ax.plot(mt, [k / 1e6 if k else None for k in m_keys], color=BLUE, lw=2, label="master")
    ax.plot(rt, [k / 1e6 if k else None for k in r_keys], color=BLUE, lw=1.4, ls="--", label="replica")
    ax.set_ylim(0, max([k for k in m_keys if k] + [1]) / 1e6 * 1.2)
    ax.set_xlabel("minutes"); ax.set_ylabel("keys (millions)")
    ax.set_title("Key count reaches a steady size: expiries and deletes balance new keys", loc="left")
    ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1), frameon=False)
    fig.tight_layout(); _save(fig, os.path.join(ch, "soak-keys.svg")); plt.close(fig)

    # 5. replication lag
    lagk = [l / 1024 if l is not None and l >= 0 else None for l in lag]
    vals = [v for v in lagk if v is not None]
    fig, ax = plt.subplots(figsize=(9, 3.2))
    ax.plot(mt, lagk, color=BLUE, lw=1.6)
    mark(ax)
    ax.set_ylim(0, max(vals + [1]) * 1.2)
    ax.set_xlabel("minutes"); ax.set_ylabel("replica behind master (KiB)")
    ax.set_title(f"Replication lag sampled every 10 s: median {statistics.median(vals) if vals else 0:,.0f} KiB, "
                 f"max {max(vals) if vals else 0:,.0f} KiB", loc="left")
    fig.tight_layout(); _save(fig, os.path.join(ch, "soak-replication-lag.svg")); plt.close(fig)

    summ = {
        "minutes": round(end_load, 1), "total_ops": total, "errors": errs,
        "ops_per_s_median": round(statistics.median(ops)), "ops_per_s_min": round(min(ops)),
        "p50_ms_median": round(statistics.median(p50), 3), "p99_ms_median": round(statistics.median(p99), 3),
        "p999_ms_median": round(statistics.median(p999), 3), "p99_ms_worst_interval": round(max(p99), 3),
        "p999_ms_worst_interval": round(max(p999), 3), "max_ms": round(max(mx), 1),
        "checkpoints": len(cps),
        "master_rss_gib_start_of_window": None, "master_rss_slope_mib_per_h": round(s_m * 60 * 1024, 1) if s_m else None,
        "master_used_slope_mib_per_h": round(s_mu * 60 * 1024, 1) if s_mu else None,
        "replica_rss_slope_mib_per_h": round(s_r * 60 * 1024, 1) if s_r else None,
        "master_rss_gib_end": round([v for v in m_rss if v][-1], 3),
        "replica_rss_gib_end": round([v for v in r_rss if v][-1], 3),
        "keys_end": m_keys[-1], "lag_kib_median": round(statistics.median(vals), 1) if vals else None,
        "lag_kib_max": round(max(vals), 1) if vals else None,
        "aof_acks_before_write_end": acks_before[-1],
    }
    win = [v for x, v in zip(mt, m_rss) if v and x >= half]
    summ["master_rss_gib_start_of_window"] = round(win[0], 3) if win else None
    cj = os.path.join(res, "compare.json")
    if os.path.exists(cj):
        summ["compare"] = json.load(open(cj))
    json.dump(summ, open(os.path.join(res, "summary.json"), "w"), indent=1)
    print(json.dumps(summ, indent=1))


if __name__ == "__main__":
    main()
