#!/usr/bin/env python3
"""charts.py <results-dir> <charts-dir> -- SVG charts from results/runs.csv (median of the repeats,
whiskers = min..max)."""
import csv, os, statistics, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker
from matplotlib.patches import Patch

R = sys.argv[1]; C = sys.argv[2]
os.makedirs(C, exist_ok=True)
plt.rcParams.update({"svg.fonttype": "none", "font.family": "DejaVu Sans", "font.size": 10,
                     "axes.facecolor": "white", "figure.facecolor": "white", "axes.edgecolor": "#444",
                     "axes.labelcolor": "#222", "xtick.color": "#222", "ytick.color": "#222",
                     "text.color": "#222", "axes.spines.top": False, "axes.spines.right": False})

# fixed engine colours from the shared brief; older builds of the same engine are hatched
ENG = [  # key, label, colour, hatch
    ("kividb-rc4", "KiviDB 1.2.0-rc4", "#0072B2", ""),
    ("kividb-1.1.3", "KiviDB 1.1.3", "#56B4E9", ""),
    ("kividb-48280b7", "KiviDB 48280b7\n(=1.2.0-rc2, reporter's)", "#56B4E9", "///"),
    ("redis-8.10.3", "Redis 8.10.3", "#D55E00", ""),
    ("redis-6bf6224", "Redis unstable 6bf6224\n(reporter's)", "#D55E00", "///"),
]
POL = ["allkeys-lfu", "allkeys-lru"]
rows = [r for r in csv.DictReader(open(os.path.join(R, "runs.csv"))) if r.get("gets")]


def vals(length, pol, eng, m):
    return [float(r[m]) for r in rows if r["length"] == length and r["policy"] == pol and r["engine"] == eng
            and r.get(m) not in ("", None)]


def bars(fname, metric, length, title, xlabel, engines=None, scale=1.0, fmt="{:,.0f}", vline=None):
    engines = [e for e in ENG if engines is None or e[0] in engines]
    fig, axes = plt.subplots(1, 2, figsize=(10, 0.55 * len(engines) + 1.6), sharey=True)
    for ax, pol in zip(axes, POL):
        top = 0
        for i, (k, label, col, hatch) in enumerate(engines):
            v = [x * scale for x in vals(length, pol, k, metric)]
            if not v:
                ax.text(0, i, "  not run", va="center", fontsize=8, color="#666")
                continue
            med = statistics.median(v)
            top = max(top, max(v))
            ax.barh(i, med, 0.62, color=col, hatch=hatch, edgecolor="white", linewidth=0, zorder=2)
            if len(v) > 1:
                ax.errorbar(med, i, xerr=[[med - min(v)], [max(v) - med]], fmt="none", ecolor="#222",
                            capsize=3, lw=1, zorder=3)
            ax.annotate(fmt.format(med), (max(v), i), xytext=(6, 0), textcoords="offset points",
                        va="center", ha="left", fontsize=8.5, color="#222")
        if vline:
            ax.axvline(vline[0], color="#666", lw=1, ls="--", zorder=1)
            ax.text(vline[0], -0.45, " " + vline[1], ha="left", va="top", fontsize=8, color="#444")
        ax.set_yticks(range(len(engines)))
        ax.set_yticklabels([e[1].replace("\n", " ") for e in engines], fontsize=8.5)
        if ax is axes[0]:
            ax.invert_yaxis()  # shared y: inverting once flips both panels
        ax.set_title(pol, fontsize=10)
        ax.grid(axis="x", color="#ddd", lw=0.6, zorder=0)
        ax.set_xlim(0, max(top, vline[0] if vline else 0) * 1.22)
        ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
        ax.set_xlabel(xlabel, fontsize=9)
    fig.suptitle(title, fontsize=11.5, x=0.01, ha="left")
    fig.tight_layout()
    fig.savefig(os.path.join(C, fname))
    plt.close(fig)


def latency(fname, length, title, engines=None):
    engines = [e for e in ENG if engines is None or e[0] in engines]
    pcts = [("p50_us", "P50"), ("p90_us", "P90"), ("p99_us", "P99"), ("p999_us", "P99.9")]
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.4), sharey=True)
    for ax, pol in zip(axes, POL):
        for k, label, col, hatch in engines:
            meds = []
            for m, _ in pcts:
                v = vals(length, pol, k, m)
                meds.append(statistics.median(v) / 1000 if v else None)
            if None in meds:
                continue
            ax.plot(range(len(pcts)), meds, color=col, lw=2, ls="--" if hatch else "-",
                    marker="o", ms=6, label=label.replace("\n", " "), zorder=3)
        ax.set_xticks(range(len(pcts)))
        ax.set_xticklabels([p[1] for p in pcts])
        ax.set_yscale("log")
        ax.set_yticks([0.5, 1, 2, 5, 10])
        ax.set_yticklabels(["0.5", "1", "2", "5", "10"])
        ax.set_title(pol, fontsize=10)
        ax.grid(axis="y", color="#ddd", lw=0.6, zorder=0)
        ax.axhline(10, color="#999", lw=0.8, ls=":")
    axes[1].text(3, 10, "harness histogram tops out at 10 ms ", ha="right", va="bottom", fontsize=7.5, color="#555")
    axes[0].set_ylabel("time per GET-or-SET, ms (log scale)")
    seen = {}
    for ax in axes:
        for h, l in zip(*ax.get_legend_handles_labels()):
            seen.setdefault(l, h)
    order = [e[1].replace("\n", " ") for e in ENG if e[1].replace("\n", " ") in seen]
    fig.legend([seen[l] for l in order], order, loc="lower center", ncol=3, fontsize=8, frameon=False, handlelength=3.5)
    fig.suptitle(title, fontsize=12, x=0.01, ha="left")
    fig.tight_layout(rect=(0, 0.07, 1, 1))
    fig.savefig(os.path.join(C, fname))
    plt.close(fig)


def hitrate(fname, length, title, engines=None):
    engines = [e for e in ENG if engines is None or e[0] in engines]
    fig, axes = plt.subplots(1, 2, figsize=(10, 0.55 * len(engines) + 2.0), sharey=True)
    for ax, pol in zip(axes, POL):
        for i, (k, label, col, hatch) in enumerate(engines):
            for m, mk, dy in (("hit_rate", "o", -0.14), ("per_mem_hit_rate", "s", 0.14)):
                v = vals(length, pol, k, m)
                if not v:
                    continue
                med = statistics.median(v)
                ax.errorbar(med, i + dy, xerr=[[med - min(v)], [max(v) - med]], fmt=mk, ms=7, color=col,
                            mfc=col if m == "hit_rate" else "white", mew=2, ecolor="#222", capsize=2, zorder=3)
                ax.annotate(f"{med:.1f}", (max(v), i + dy), xytext=(6, 0), textcoords="offset points",
                            fontsize=7.5, va="center", color="#222")
        ax.set_yticks(range(len(engines)))
        ax.set_yticklabels([e[1].replace("\n", " ") for e in engines], fontsize=8.5)
        if ax is axes[0]:
            ax.set_ylim(len(engines) - 0.5, -0.5)
        ax.set_xlim(40, 64)
        ax.set_title(pol, fontsize=10)
        ax.grid(axis="x", color="#ddd", lw=0.6, zorder=0)
        ax.set_xlabel("percent of GETs that hit (axis starts at 40%)", fontsize=9)
    leg = [plt.Line2D([], [], marker="o", color="#444", ls="", ms=8, label="hit rate"),
           plt.Line2D([], [], marker="s", color="#444", mfc="white", mew=2, ls="", ms=8,
                      label="per-memory hit rate (x 1 GiB / VmHWM)")]
    fig.legend(handles=leg, loc="lower center", ncol=2, fontsize=8.5, frameon=False)
    fig.suptitle(title, fontsize=12, x=0.01, ha="left")
    fig.tight_layout(rect=(0, 0.07, 1, 1))
    fig.savefig(os.path.join(C, fname))
    plt.close(fig)


def diag(fname, title):
    """diag-<engine>.log from diag-sampler.sh: epoch, total commands, connected clients, max idle s, pid"""
    series = [("kividb-1.1.3", "KiviDB 1.1.3", "#56B4E9"), ("kividb-rc4", "KiviDB 1.2.0-rc4", "#0072B2")]
    fig, axes = plt.subplots(2, 1, figsize=(10, 5.4), sharex=True, gridspec_kw={"height_ratios": [2, 1]})
    for k, label, col in series:
        p = os.path.join(R, "raw", f"diag-{k}.log")
        if not os.path.exists(p):
            continue
        pts = []
        for line in open(p):
            f = line.split()
            if len(f) >= 4 and f[3] != "NA" and f[1] != "NA":
                pts.append((float(f[0]), int(f[1]), int(f[2]), int(f[3])))
        t0 = pts[0][0]
        t = [(x[0] - t0) / 60 for x in pts]
        axes[0].plot(t, [x[3] for x in pts], color=col, lw=2, label=label)
        axes[1].plot(t, [x[2] - 1 for x in pts], color=col, lw=2, label=label)  # minus the sampler itself
        if k == "kividb-1.1.3":
            peak = max(range(len(pts)), key=lambda i: pts[i][3])
            axes[0].annotate(f"one connection unanswered for {pts[peak][3]} s\nwhile others are served; then the "
                             "client times out\nand drops its connections", (t[peak], pts[peak][3]),
                             xytext=(25, -10), textcoords="offset points", fontsize=8, color="#222",
                             arrowprops=dict(arrowstyle="-", color="#666", lw=0.8))
    axes[0].set_ylabel("longest wait of any\nconnection, s (CLIENT LIST idle)")
    axes[1].set_ylabel("client connections")
    axes[1].set_xlabel("minutes since the server started (warmup, then the measured 8,388,608 GETs)")
    for ax in axes:
        ax.grid(axis="y", color="#ddd", lw=0.6)
        ax.set_ylim(bottom=0)
    axes[0].legend(loc="upper right", frameon=False, fontsize=8.5)
    fig.suptitle(title, fontsize=11.5, x=0.01, ha="left")
    fig.tight_layout()
    fig.savefig(os.path.join(C, fname))
    plt.close(fig)


if __name__ == "__main__":
    T = dict(l.split("=", 1) for l in sys.argv[3:])  # optional title overrides key=text
    bars("hits-per-s.svg", "hits_per_s", "S",
         T.get("hits", "rc4 serves 7-8x the hits/s of 1.1.3 and about 8% fewer than Redis 8 (8,388,608 GETs per run)"),
         "hits per second (median of 3 runs; whiskers min-max)")
    bars("hits-per-s-full.svg", "hits_per_s", "F",
         T.get("hitsfull", "At the reporter's full length (33,554,432 GETs) rc4 is 6-8% behind Redis 8 on hits/s"),
         "hits per second (median of 3 runs; whiskers min-max)", engines={"kividb-rc4", "redis-8.10.3", "redis-6bf6224"})
    bars("vmhwm.svg", "vmhwm_kb", "S", T.get("mem", "rc4 peaks within 2% of the 1 GiB limit, like Redis; 1.1.3 and 48280b7 overshoot by 20-30%"),
         "VmHWM, MiB", scale=1 / 1024, vline=(1024, "maxmemory"))
    hitrate("hit-rate.svg", "S", T.get("hitrate", "rc4 hit rate matches Redis; 1.1.3 and 48280b7 lose it per byte of RAM (8,388,608 GETs)"))
    latency("latency.svg", "S", T.get("lat", "rc4 latency tracks Redis 8; old builds are 5-10x slower (8,388,608 GETs)"))
    print("charts written to", C)
    diag("connection-idle.svg", T.get("diag", "KiviDB 1.1.3 starves some connections under this load; 1.2.0-rc4 does not"))
