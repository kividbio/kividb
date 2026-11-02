#!/usr/bin/env python3
"""SVG charts from summarize.py's summary.csv.

    python3 charts.py <results-dir> <charts-dir>

One idea per chart; fixed engine colours shared by every 1.2.0 suite.
"""
import csv
import math
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

plt.rcParams.update({
    "svg.fonttype": "none", "font.family": "DejaVu Sans", "font.size": 10,
    "figure.facecolor": "white", "axes.facecolor": "white", "savefig.facecolor": "white",
    "text.color": "#222222", "axes.labelcolor": "#222222", "xtick.color": "#222222", "ytick.color": "#222222",
    "axes.edgecolor": "#888888", "axes.grid": True, "grid.color": "#e3e3e3", "grid.linewidth": 0.8,
    "axes.axisbelow": True, "axes.spines.top": False, "axes.spines.right": False,
})

ROOT, OUT = sys.argv[1], sys.argv[2]
os.makedirs(OUT, exist_ok=True)

ENGINES = [  # key prefix, label, colour
    ("kividb-rc4", "KiviDB 1.2.0-rc4", "#0072B2"),
    ("kividb-113", "KiviDB 1.1.3", "#56B4E9"),
    ("redis-cluster", "Redis 8.10.3 Cluster (32 shards)", "#E69F00"),
    ("redis", "Redis 8.10.3 (io-threads {io})", "#D55E00"),
    ("valkey", "Valkey 9.1.3 (io-threads {io})", "#009E73"),
    ("dragonfly", "Dragonfly 2.0.2", "#CC79A7"),
    ("garnet", "Garnet 2.2.5", "#999999"),
]
ORDER = ["kividb-rc4", "kividb-113", "redis", "redis-cluster", "valkey", "dragonfly", "garnet"]
MAIN = os.environ.get("MAIN_ENGINES", "").split()  # e.g. "kividb-rc4 kividb-113 redis@8 ..."

rows = list(csv.DictReader(open(os.path.join(ROOT, "summary.csv"))))
data = {(r["engine"], r["size"], r["scenario"]): r for r in rows}
engines = MAIN or sorted({r["engine"] for r in rows}, key=lambda e: ORDER.index(e.split("@")[0]) if e.split("@")[0] in ORDER else 99)


def style(engine):
    base, _, io = engine.partition("@")
    for key, label, colour in ENGINES:
        if base == key:
            return label.format(io=io or "8"), colour
    return engine, "#444444"


def get(e, s, sc, k="ops_median"):
    r = data.get((e, s, sc))
    if not r or r.get(k, "") == "":
        return None
    return float(r[k])


def limited(e, s, sc):
    r = data.get((e, s, sc))
    return r["limit"] if r else ""


def save(fig, name):
    fig.savefig(os.path.join(OUT, name), format="svg", bbox_inches="tight")
    if os.environ.get("PREVIEW"):  # PNG copies for a quick look; not published
        fig.savefig(os.path.join(os.environ["PREVIEW"], name[:-4] + ".png"), dpi=80, bbox_inches="tight")
    plt.close(fig)
    print("wrote", name)


SIZE_NAME = {"100": "100 B", "1024": "1 KB", "2048": "2 KB"}
WL = [("get_p1", "GET\npipeline 1"), ("get_p16", "GET\npipeline 16"), ("set_p1", "SET\npipeline 1"),
      ("set_p16", "SET\npipeline 16")]


def grouped(scenarios, size, title, name, es=None):
    es = es or engines
    es = [e for e in es if any(get(e, size, sc) for sc, _ in scenarios)]
    n = len(es)
    width = 0.8 / n
    fig, ax = plt.subplots(figsize=(10, 4.8))
    for i, e in enumerate(es):
        label, colour = style(e)
        for j, (sc, _) in enumerate(scenarios):
            v = get(e, size, sc)
            if v is None:
                continue
            lo, hi = get(e, size, sc, "ops_min"), get(e, size, sc, "ops_max")
            x = j - 0.4 + width * (i + 0.5)
            lim = limited(e, size, sc)
            ax.bar(x, v / 1e6, width * 0.92, color=colour, hatch="///" if lim else None, edgecolor="white" if not lim else "#333333", linewidth=0.4)
            ax.errorbar(x, v / 1e6, yerr=[[(v - lo) / 1e6], [(hi - v) / 1e6]], color="#333333", lw=0.8, capsize=1.5)
            ax.text(x, hi / 1e6, f" {v / 1e6:.2f}", rotation=90, ha="center", va="bottom", fontsize=7, color="#222222")
    handles = [plt.Rectangle((0, 0), 1, 1, color=style(e)[1]) for e in es]
    labels = [style(e)[0] for e in es]
    if any(limited(e, size, sc) for e in es for sc, _ in scenarios):
        handles.append(plt.Rectangle((0, 0), 1, 1, facecolor="white", edgecolor="#333333", hatch="///"))
        labels.append("hatched: load generators or NIC were the limit")
    ax.legend(handles, labels, loc="upper left", bbox_to_anchor=(1.01, 1), frameon=False, fontsize=9)
    ax.set_xticks(range(len(scenarios)), [l for _, l in scenarios])
    ax.set_ylabel("million requests / s (median of rounds; whiskers min-max)")
    ax.set_ylim(0, ax.get_ylim()[1] * 1.12)
    ax.set_title(title, loc="left", fontsize=12, fontweight="bold")
    save(fig, name)


def best_other(size, sc):
    others = [(get(e, size, sc) or 0, e) for e in engines if not e.startswith("kividb")]
    return max(others) if others else (0, None)


def short(e):
    return style(e)[0].split(" (")[0]


def vs(size, sc):
    """'0.97x Garnet 2.2.5': KiviDB rc4 against the fastest other engine."""
    k = get("kividb-rc4", size, sc)
    b, be = best_other(size, sc)
    return f"{k / b:.2f}x {short(be)}" if k and b else "n/a"


for size in ("100", "1024", "2048"):
    p16 = [get(e, size, "get_p16") for e in engines if get(e, size, "get_p16")]
    if "nic" in limited("kividb-rc4", size, "get_p16"):
        t = (f"{SIZE_NAME[size]} values: at pipeline 16 the fast engines all hit the server's 16 Gbps NIC "
             f"(~{max(p16) / 1e6:.2f}M GET/s)")
    else:
        t = (f"{SIZE_NAME[size]} values, pipeline 16: KiviDB rc4 GET is {vs(size, 'get_p16')}, "
             f"SET is {vs(size, 'set_p16')}")
    grouped(WL, size, t, f"throughput-{size}.svg")

mk = [("mget10_p1", "MGET of 10 keys\npipeline 1"), ("mset10_p1", "MSET of 10 keys\npipeline 1")]
grouped(mk, "100", f"10-key commands: KiviDB rc4 MGET is {vs('100', 'mget10_p1')}, "
        f"MSET only {vs('100', 'mset10_p1')} (commands/s)",
        "multikey.svg", [e for e in engines if e != "redis-cluster"])

# Latency: p50, p99, p99.9 per engine, one panel per workload, log scale.
for size in ("100", "1024", "2048"):
    es = [e for e in engines if get(e, size, "get_p16")]
    fig, axes = plt.subplots(1, 4, figsize=(12, 0.45 * len(es) + 1.6), sharey=True)
    for ax, (sc, lab) in zip(axes, WL):
        for i, e in enumerate(es):
            y = len(es) - 1 - i
            p50, p99, p999 = (get(e, size, sc, k) for k in ("p50_ms", "p99_ms", "p999_ms"))
            if p50 is None:
                continue
            c = style(e)[1]
            ax.plot([p50, p999], [y, y], color=c, lw=2, solid_capstyle="round")
            ax.plot(p50, y, "o", color=c, ms=6, mec="white", mew=1)
            ax.plot(p99, y, "D", color=c, ms=7, mec="#222222", mew=0.8)
            ax.plot(p999, y, "|", color="#222222", ms=10)
            ax.text(p99, y - 0.38, f"{p99:.2f}", fontsize=7, ha="center", va="center")
        ax.set_ylim(-0.7, len(es) - 0.5)
        ax.set_xscale("log")
        ax.set_title(lab.replace("\n", ", "), fontsize=10)
        ax.set_xlabel("latency, ms (log)")
        ax.grid(axis="y", visible=False)
    axes[0].set_yticks(range(len(es)), [style(e)[0] for e in reversed(es)])
    handles = [plt.Line2D([], [], marker="o", ls="", color="#555555", label="p50"),
               plt.Line2D([], [], marker="D", ls="", color="#555555", mec="#222222", label="p99 (labelled)"),
               plt.Line2D([], [], marker="|", ls="", color="#222222", ms=10, label="p99.9")]
    fig.legend(handles=handles, loc="lower center", ncol=3, frameon=False, bbox_to_anchor=(0.5, -0.08))
    k99, b99 = get("kividb-rc4", size, "get_p16", "p99_ms"), None
    fig.suptitle(f"{SIZE_NAME[size]} values: request latency at each engine's own throughput (p50, p99, p99.9)",
                 x=0.01, ha="left", fontsize=12, fontweight="bold")
    fig.tight_layout()
    save(fig, f"latency-{size}.svg")

# Throughput against pipeline depth, GET 100 B, 300 connections.
fig, ax = plt.subplots(figsize=(8.5, 4.6))
depths = [1, 4, 16, 64]
ends = []
for e in engines:
    pts = [(d, get(e, "100", "get_p16" if d == 16 else f"sweep_get_p{d}")) for d in depths]
    pts = [(d, v) for d, v in pts if v]
    if len(pts) < 2:
        continue
    label, c = style(e)
    ax.plot([d for d, _ in pts], [v / 1e6 for _, v in pts], "-o", color=c, lw=2, ms=6, mec="white", label=label)
    ends.append([pts[-1][1] / 1e6, pts[-1][0]])
ax.set_ylim(0, max([v for v, _ in ends] + [1]) * 1.1)
# End labels, nudged apart so close values do not overprint.
gap = ax.get_ylim()[1] * 0.045
ends.sort()
for i, end in enumerate(ends):
    end.append(end[0] if i == 0 else max(end[0], ends[i - 1][2] + gap))
for v, d, ypos in ends:
    ax.text(d * 1.1, ypos, f"{v:.1f}M", fontsize=8, va="center")
ax.set_xscale("log", base=2)
ax.set_xticks(depths, [str(d) for d in depths])
ax.set_xlim(0.8, 110)
ax.set_xlabel("pipeline depth (requests in flight per connection; 300 connections)")
ax.set_ylabel("million GETs / s, 100 B")
ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1), frameon=False, fontsize=9)
ax.set_title("Batching is where the multi-threaded engines pull away: GET throughput by pipeline depth",
             loc="left", fontsize=11, fontweight="bold")
save(fig, "pipeline-depth.svg")

# 1.1.3 against rc4.
scs = [(s, sc) for s in ("100", "1024", "2048") for sc, _ in WL] + [("100", "mget10_p1"), ("100", "mset10_p1")]
pairs = [(s, sc, get("kividb-113", s, sc), get("kividb-rc4", s, sc)) for s, sc in scs]
pairs = [p for p in pairs if p[2] and p[3]]
if pairs:
    fig, ax = plt.subplots(figsize=(8.5, 0.32 * len(pairs) + 1.4))
    ys = list(range(len(pairs)))[::-1]
    ch = [round(100 * (n / o - 1), 1) + 0.0 for _, _, o, n in pairs]
    # Slower beyond run-to-run noise: rc4's best round below 1.1.3's worst.
    beyond = [get("kividb-rc4", s, sc, "ops_max") < get("kividb-113", s, sc, "ops_min") for s, sc, _, _ in pairs]
    ax.barh(ys, ch, color=["#0072B2" if c >= 0 else "#56B4E9" for c in ch], height=0.65,
            hatch=["///" if b else None for b in beyond], edgecolor="white")
    for y, c, b, (s, sc, o, n) in zip(ys, ch, beyond, pairs):
        ax.text(max(c, 0) + 0.4, y,
                f"{c:+.1f}%  ({o / 1e6:.2f}M -> {n / 1e6:.2f}M){'  outside run-to-run spread' if b else ''}",
                va="center", ha="left", fontsize=8)
    ax.axvline(0, color="#444444", lw=1)
    ax.set_yticks(ys, [f"{SIZE_NAME[s]} {sc.replace('_p', ' p').upper().replace(' P', ', pipeline ')}" for s, sc, _, _ in pairs])
    lo, hi = min(ch + [0]), max(ch + [0])
    pad = max(hi - lo, 10) * 0.6
    ax.set_xlim(lo - (pad if lo < 0 else 2), hi + pad)
    ax.set_xlabel("change in median throughput, 1.2.0-rc4 vs 1.1.3 (%)")
    ax.grid(axis="y", visible=False)
    slow = [(p, c) for p, c, b in zip(pairs, ch, beyond) if b]
    note = ("; slower beyond the spread: " + ", ".join(
        f"{SIZE_NAME[p[0]]} {p[1].split('_')[0].upper()} p{p[1].split('_p')[1]} {c:+.1f}%" for p, c in slow)
        if slow else "; nothing slower beyond the spread")
    ax.set_title(f"KiviDB 1.2.0-rc4 vs 1.1.3, interleaved rounds{note}",
                 loc="left", fontsize=11, fontweight="bold")
    save(fig, "regression-113-vs-rc4.svg")
