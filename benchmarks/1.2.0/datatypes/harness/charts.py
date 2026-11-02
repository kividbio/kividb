"""charts.py RESULTS_DIR CHARTS_DIR: SVG charts from summary.csv and memory.csv."""
import csv
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

plt.rcParams.update({
    "svg.fonttype": "none", "font.family": "DejaVu Sans", "font.size": 10,
    "axes.facecolor": "white", "figure.facecolor": "white", "savefig.facecolor": "white",
    "axes.edgecolor": "#444444", "axes.labelcolor": "#222222", "text.color": "#222222",
    "xtick.color": "#333333", "ytick.color": "#333333",
    "axes.grid": True, "grid.color": "#e3e3e3", "grid.linewidth": 0.6, "axes.axisbelow": True,
    "axes.spines.top": False, "axes.spines.right": False,
})
RES, OUT = sys.argv[1], sys.argv[2]
os.makedirs(OUT, exist_ok=True)

COLORS = {"kividb-rc4": "#0072B2", "kividb-1.1.3": "#56B4E9", "redis": "#D55E00", "redis-cluster": "#E69F00",
          "valkey": "#009E73", "dragonfly": "#CC79A7", "kvrocks": "#F0E442"}
EDGES = {"kvrocks": "#8a7f00"}
NAMES = {"kividb-rc4": "KiviDB 1.2.0-rc4", "kividb-1.1.3": "KiviDB 1.1.3", "redis": "Redis 8.10.3",
         "redis-cluster": "Redis 8.10.3 Cluster (16 shards)", "valkey": "Valkey 9.1.3", "dragonfly": "Dragonfly 2.0.2",
         "kvrocks": "kvrocks 2.17.0"}
INMEM = ["kividb-rc4", "kividb-1.1.3", "redis", "redis-cluster", "valkey", "dragonfly"]
CMDS = ["LPUSH", "LPOP", "LRANGE_100", "HSET", "HGET", "HGETALL", "SADD", "SISMEMBER", "ZADD", "ZRANGE", "INCR"]

rows = list(csv.DictReader(open(os.path.join(RES, "summary.csv"))))
S = {}
for r in rows:
    S[(r["engine"], r["command"], r["value_bytes"], int(r["pipeline"]))] = r


def cell(e, c, v, p):
    return S.get((e, c, "" if c == "INCR" else v, p))


def fmt_rate(x):
    return f"{x / 1e6:.2f}M" if x >= 1e6 else f"{x / 1e3:.0f}k"


def grid(engines, v, p, metric, fname, title, ylabel, scale=1e6):
    cmds = [c for c in CMDS if any(cell(e, c, v, p) for e in engines)]
    engines = [e for e in engines if any(cell(e, c, v, p) for c in cmds)]
    if not cmds or not engines:
        return
    ncol = 4
    nrow = (len(cmds) + ncol - 1) // ncol
    fig, axes = plt.subplots(nrow, ncol, figsize=(13, 2.7 * nrow + 1.2), squeeze=False)
    for i, c in enumerate(cmds):
        ax = axes[i // ncol][i % ncol]
        xs, hs, lo, hi, cols, eds = [], [], [], [], [], []
        for j, e in enumerate(engines):
            r = cell(e, c, v, p)
            if not r:
                continue
            val = float(r[metric]) / scale
            xs.append(j); hs.append(val); cols.append(COLORS[e]); eds.append(EDGES.get(e, COLORS[e]))
            if metric == "ops_per_sec_median":
                lo.append(val - float(r["ops_per_sec_min"]) / scale)
                hi.append(float(r["ops_per_sec_max"]) / scale - val)
        ax.bar(xs, hs, color=cols, edgecolor=eds, width=0.72, linewidth=0.8)
        if lo:
            ax.errorbar(xs, hs, yerr=[lo, hi], fmt="none", ecolor="#222222", elinewidth=0.8, capsize=2)
        label = c if c == "INCR" else f"{c}, {v} B"
        ax.set_title(label, fontsize=10, loc="left")
        ax.set_xticks([])
        ax.set_ylim(bottom=0)
        ax.set_ylabel(ylabel, fontsize=8)
        for x, h in zip(xs, hs):
            ax.text(x, h, f"{h:.2f}" if h < 10 else f"{h:.1f}", ha="center", va="bottom", fontsize=7, color="#333333")
    for k in range(len(cmds), nrow * ncol):
        axes[k // ncol][k % ncol].axis("off")
    handles = [plt.Rectangle((0, 0), 1, 1, color=COLORS[e], ec=EDGES.get(e, COLORS[e])) for e in engines]
    fig.legend(handles, [NAMES[e] for e in engines], loc="lower center", ncol=min(6, len(engines)), frameon=False,
               bbox_to_anchor=(0.5, 0.0))
    fig.suptitle(title, fontsize=13, x=0.01, ha="left")
    fig.tight_layout(rect=(0, 0.06, 1, 0.93))
    fig.savefig(os.path.join(OUT, fname))
    plt.close(fig)


def leader_title(engines, v, p, what):
    cmds = [c for c in CMDS if any(cell(e, c, v, p) for e in engines)]
    wins = 0
    best_other = []
    for c in cmds:
        vals = {e: float(cell(e, c, v, p)["ops_per_sec_median"]) for e in engines if cell(e, c, v, p)}
        if not vals or "kividb-rc4" not in vals:
            continue
        others_max = max([x for e, x in vals.items() if not e.startswith("kividb")], default=0)
        if vals["kividb-rc4"] >= others_max:
            wins += 1
        others = [x for e, x in vals.items() if not e.startswith("kividb")]
        if others:
            best_other.append(vals["kividb-rc4"] / max(others))
    if not best_other:
        return what
    lo, hi = min(best_other), max(best_other)
    return (f"{what}: KiviDB rc4 is ahead of the other engines on {wins} of {len(cmds)} commands "
            f"\n({lo:.2f}x to {hi:.2f}x the best of them; median of 3 runs, whiskers min-max)")


for v in ["100", "2048"]:
    for p in [1, 16]:
        vb = "100 B" if v == "100" else "2 KB"
        grid(INMEM, v, p, "ops_per_sec_median", f"throughput-{v}B-p{p}.svg",
             leader_title(INMEM, v, p, f"{vb} values, pipeline {p}"), "M ops/s")
        grid(INMEM, v, p, "p99_ms", f"p99-{v}B-p{p}.svg",
             f"p99 latency, {vb} values, pipeline {p}, lower is better" + ("; the Redis Cluster rows include a saturated client" if p == 16 else ""), "p99 ms", scale=1)
    # kvrocks: its own class (disk-backed), against KiviDB rc4 and Redis
    for p in [1, 16]:
        vb = "100 B" if v == "100" else "2 KB"
        grid(["kividb-rc4", "redis", "kvrocks"], v, p, "ops_per_sec_median", f"kvrocks-{v}B-p{p}.svg",
             f"kvrocks (RocksDB on local NVMe, a disk-backed store) vs in-memory engines, {vb}, pipeline {p}",
             "M ops/s")

# memory after loading each group
mem = list(csv.DictReader(open(os.path.join(RES, "memory.csv")))) if os.path.exists(os.path.join(RES, "memory.csv")) else []
if mem:
    labels = ["hash-100", "set-100", "zset-100", "hash-2048", "set-2048", "zset-2048"]
    nice = {"hash-100": "100k hashes x 10 fields\n100 B values", "set-100": "100k sets x 10\n100 B members",
            "zset-100": "100k zsets x 10\n100 B members", "hash-2048": "100k hashes x 10 fields\n2 KB values",
            "set-2048": "100k sets x 10\n2 KB members", "zset-2048": "100k zsets x 10\n2 KB members"}
    engines = [e for e in INMEM + ["kvrocks"] if any(m["engine"] == e for m in mem)]
    M = {(m["engine"], m["label"]): m for m in mem}
    fig, axes = plt.subplots(1, 2, figsize=(13, 4.6))
    for ax, group in zip(axes, [labels[:3], labels[3:]]):
        w = 0.8 / len(engines)
        for j, e in enumerate(engines):
            base = float(M[(e, "empty")]["rss_kb"]) if (e, "empty") in M else 0.0
            xs, hs, ds = [], [], []
            for i, lab in enumerate(group):
                if (e, lab) in M:
                    xs.append(i + (j - (len(engines) - 1) / 2) * w)
                    hs.append((float(M[(e, lab)]["rss_kb"]) - base) / 1024 / 1024)
                    ds.append(float(M[(e, lab)]["disk_kb"] or 0) / 1024 / 1024)
            ax.bar(xs, hs, width=w * 0.92, color=COLORS[e], edgecolor=EDGES.get(e, COLORS[e]), linewidth=0.8,
                   label=NAMES[e])
            if e == "kvrocks" and any(ds):
                ax.bar(xs, ds, bottom=hs, width=w * 0.92, color="white", edgecolor=EDGES["kvrocks"], hatch="///",
                       linewidth=0.8, label="kvrocks data on disk")
        ax.set_xticks(range(len(group)))
        ax.set_xticklabels([nice[g] for g in group], fontsize=8)
        ax.set_ylabel("GiB (RSS after load, minus empty)")
        ax.set_ylim(bottom=0)
    h, lab = axes[0].get_legend_handles_labels()
    fig.legend(h, lab, loc="lower center", ncol=4, frameon=False)
    fig.suptitle("Memory after loading: KiviDB needs the least with 2 KB values, but 1.8x Redis for 100 B hash fields", x=0.01, ha="left",
                 fontsize=13)
    fig.tight_layout(rect=(0, 0.1, 1, 0.94))
    fig.savefig(os.path.join(OUT, "memory-by-type.svg"))
    plt.close(fig)
print("charts in", OUT)
