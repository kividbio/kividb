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
    ("kividb-75482ab", "KiviDB 1.2.0\n(candidate 75482ab)", "#0072B2", ""),
    ("kividb-1.1.3", "KiviDB 1.1.3", "#56B4E9", ""),
    ("redis-8.10.3", "Redis 8.10.3", "#D55E00", ""),
    ("redis-6bf6224", "Redis unstable 6bf6224\n(reporter's)", "#D55E00", "///"),
    ("kividb-rc4", "KiviDB 1.2.0-rc4", "#56B4E9", ""),  # regression chart only
]
CAND, RC4 = "kividb-75482ab", "kividb-rc4"
S_ENG = {CAND, "kividb-1.1.3", "redis-8.10.3"}  # the engines run at the quarter length
F_ENG = {CAND, "redis-8.10.3", "redis-6bf6224"}
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


def paired(fname, title):
    """candidate GETs/s / rc4 GETs/s for every run of the two on the same VM pair, back to back"""
    by = {}
    for r in rows:
        if r["engine"] in (CAND, RC4) and r["length"] in ("F", "X"):
            key = (r["length"], r["policy"], r["pair"].split()[0], r["rep"].rstrip("ab"), r["rep"][-1:] if r["length"] == "X" else "")
            by.setdefault(key, {})[r["engine"]] = float(r["gets_per_s"])
    groups = [("F", "allkeys-lfu", "allkeys-lfu, 33,554,432 GETs"), ("F", "allkeys-lru", "allkeys-lru, 33,554,432 GETs"),
              ("X", "allkeys-lfu", "allkeys-lfu, 8,388,608 GETs (extra rounds)")]
    fig, ax = plt.subplots(figsize=(10, 2.9))
    for i, (L, pol, label) in enumerate(groups):
        v = [d[CAND] / d[RC4] for k, d in by.items() if k[0] == L and k[1] == pol and CAND in d and RC4 in d]
        if not v:
            continue
        ax.scatter(v, [i] * len(v), s=36, color="#0072B2", edgecolor="white", lw=0.6, zorder=3)
        med = statistics.median(v)
        ax.plot([med, med], [i - 0.28, i + 0.28], color="#222", lw=2, zorder=4)
        ax.annotate(f"median {med:.3f} ({len(v)} paired runs)", (max(v), i), xytext=(8, 0), textcoords="offset points",
                    va="center", fontsize=8.5, color="#222")
    ax.axvline(1.0, color="#56B4E9", lw=2, zorder=1)
    ax.text(1.0, len(groups) - 0.45, " rc4 = 1.0", color="#222", fontsize=8.5, va="bottom")
    ax.set_yticks(range(len(groups)))
    ax.set_yticklabels([g[2] for g in groups], fontsize=8.5)
    ax.set_ylim(-0.6, len(groups) - 0.2)
    ax.invert_yaxis()
    ax.set_xlim(0.95, 1.05)
    ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, _: f"{x:.2f}"))
    ax.grid(axis="x", color="#ddd", lw=0.6, zorder=0)
    ax.set_xlabel("candidate GETs/s divided by rc4 GETs/s on the same VM pair (dots: one pair of runs each; bar: median)", fontsize=9)
    fig.suptitle(title, fontsize=12, x=0.01, ha="left")
    fig.tight_layout()
    fig.savefig(os.path.join(C, fname))
    plt.close(fig)


def diag(fname, title):
    """diag-<engine>.log from diag-sampler.sh: epoch, total commands, connected clients, max idle s, pid"""
    series = [("kividb-1.1.3", "KiviDB 1.1.3", "#56B4E9"), (CAND, "KiviDB 1.2.0 (candidate 75482ab)", "#0072B2")]
    fig, axes = plt.subplots(3, 1, figsize=(10, 7.2), sharex=True, gridspec_kw={"height_ratios": [2, 1.4, 1]})
    for k, label, col in series:
        p = os.path.join(R, "raw", f"diag-{k}.log")
        if not os.path.exists(p):
            continue
        pts, na = [], []
        for line in open(p):
            f = line.split()
            if len(f) < 4:
                continue
            if f[1] == "NA":
                na.append(float(f[0]))  # the sampler's own INFO got no answer within 2 s
            elif f[3] != "NA" and f[1] != "NA":
                pts.append((float(f[0]), int(f[1]), int(f[2]), int(f[3])))
        na = [x for x in na if pts[0][0] < x < pts[-1][0]]  # not before the start or after the stop
        t0 = pts[0][0]
        t = [(x[0] - t0) / 60 for x in pts]
        rate = [(b[1] - a[1]) / (b[0] - a[0]) for a, b in zip(pts, pts[1:])]
        axes[0].plot(t[1:], rate, color=col, lw=1.2, label=label)
        if na:
            axes[0].plot([(x - t0) / 60 for x in na], [800] * len(na), ls="", marker="|", ms=10, mew=1.5, color="#D55E00",
                         label=f"{label}: sampler's INFO unanswered for 2 s ({len(na)}x)")
        axes[1].plot(t, [x[3] for x in pts], color=col, lw=2, label=label)
        axes[2].plot(t, [x[2] - 1 for x in pts], color=col, lw=2, label=label)  # minus the sampler itself
    axes[0].set_yscale("log")
    axes[0].set_ylim(500, 300000)
    axes[0].yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    axes[0].set_ylabel("commands/s\n(INFO, log scale)")
    axes[1].set_ylabel("longest wait of any\nconnection, s (CLIENT LIST idle)")
    axes[2].set_ylabel("client connections")
    axes[2].set_xlabel("minutes since the server started (warmup, then the measured 8,388,608 GETs)")
    for ax in axes:
        ax.grid(axis="y", color="#ddd", lw=0.6)
    for ax in axes[1:]:
        ax.set_ylim(bottom=0)
    axes[1].set_ylim(top=max(5, axes[1].get_ylim()[1]))
    axes[0].legend(loc="lower right", frameon=False, fontsize=8)
    fig.suptitle(title, fontsize=11.5, x=0.01, ha="left")
    fig.tight_layout()
    fig.savefig(os.path.join(C, fname))
    plt.close(fig)


if __name__ == "__main__":
    T = dict(l.split("=", 1) for l in sys.argv[3:])  # optional title overrides key=text
    bars("hits-per-s.svg", "hits_per_s", "S",
         T.get("hits", "1.2.0 serves about 8x the hits/s of 1.1.3 (8,388,608 GETs per run)"),
         "hits per second (median of 3 runs; whiskers min-max)", engines=S_ENG)
    bars("hits-per-s-full.svg", "hits_per_s", "F",
         T.get("hitsfull", "At the reporter's full length (33,554,432 GETs), 1.2.0 against Redis on hits/s"),
         "hits per second (median of 3 runs; whiskers min-max)", engines=F_ENG)
    bars("vmhwm.svg", "vmhwm_kb", "S", T.get("mem", "1.2.0 peaks close to the 1 GiB limit, like Redis; 1.1.3 overshoots by 20-30%"),
         "VmHWM, MiB", scale=1 / 1024, vline=(1024, "maxmemory"), engines=S_ENG)
    hitrate("hit-rate.svg", "S", T.get("hitrate", "1.2.0 hit rate matches Redis; 1.1.3 loses it per byte of RAM (8,388,608 GETs)"),
            engines=S_ENG)
    latency("latency.svg", "F", T.get("lat", "1.2.0 latency against Redis (33,554,432 GETs)"), engines=F_ENG)
    bars("vs-rc4.svg", "gets_per_s", "F",
         T.get("rc4", "1.2.0 candidate against rc4, same VM pairs (33,554,432 GETs per run)"),
         "GETs per second (median of 3 runs; whiskers min-max)", engines={CAND, RC4})
    paired("vs-rc4-paired.svg", T.get("paired", "1.2.0 candidate against rc4, run by run on the same VM pair"))
    print("charts written to", C)
    diag("connection-idle.svg", T.get("diag", "Under this load KiviDB 1.1.3 stalls for seconds at a time and the client drops its connections; 1.2.0 does not"))
