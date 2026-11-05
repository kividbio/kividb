#!/usr/bin/env python3
"""rc8 round of the core suite: tables and the regression chart.

    python3 analyze.py <results-dir> <published-summary.csv> <charts-dir> > tables.md

Reads summarize.py's summary.csv and runs.csv in <results-dir>.
"""
import csv
import os
import statistics
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

ROOT, PUB, OUT = sys.argv[1], sys.argv[2], sys.argv[3]
os.makedirs(OUT, exist_ok=True)
S = {(r["engine"], r["size"], r["scenario"]): r for r in csv.DictReader(open(os.path.join(ROOT, "summary.csv")))}
RUNS = [r for r in csv.DictReader(open(os.path.join(ROOT, "runs.csv"))) if not r.get("error")]
P = {(r["engine"], r["size"], r["scenario"]): r for r in csv.DictReader(open(PUB))}
SIZE = {"100": "100 B", "1024": "1 KB", "2048": "2 KB"}
WL = ["get_p1", "get_p16", "set_p1", "set_p16"]
SCS = [(s, w) for s in ("100", "1024", "2048") for w in WL] + [("100", "mget10_p1"), ("100", "mset10_p1")]
NEW, OLD = "kividb-rc8", "kividb-rc7"
FLOOR = float(os.environ.get("FLOOR", "1"))  # % change below which two builds count as level


def m(v):
    v = float(v)
    return f"{v / 1e6:.2f}M" if v >= 1e6 else f"{v / 1e3:.0f}K"


def name(s, sc):
    op, p = sc.split("_p")
    return f"{SIZE[s]} {op.upper().replace('10', ' 10')} p{p}"


def per_round(e, s, sc, k):
    return [float(r[k]) for r in RUNS if r["engine"] == e and r["size"] == s and r["scenario"] == sc
            and r.get(k, "") not in ("", None)]


def flag(r):
    return {"client": " C", "nic": " N", "client+nic": " C N"}.get(r.get("limit", ""), "")


print("### rc8 against rc7 (same session, alternating rounds)\n")
print("Median [min - max] of the rounds. Spread is half the min-max range of each build, as % of its median.")
print(f"Verdict: *slower* if rc8's best round is below rc7's worst and the medians differ by more than {FLOOR:.0f}% "
      "(the run-to-run spread of the published six-round session), *faster* the other way round.\n")
print("| Scenario | rc7 | rc8 | Change | Spread rc7 / rc8 | Verdict | p99 ms rc7 → rc8 | p99.9 ms rc7 → rc8 | server µs/req rc7 → rc8 |")
print("|---|---:|---:|---:|---:|---|---:|---:|---:|")
for s, sc in SCS:
    o, n = S.get((OLD, s, sc)), S.get((NEW, s, sc))
    if not (o and n):
        continue
    ch = 100 * (float(n["ops_median"]) / float(o["ops_median"]) - 1)
    below = float(n["ops_max"]) < float(o["ops_min"])
    above = float(n["ops_min"]) > float(o["ops_max"])
    worse, better = below and ch < -FLOOR, above and ch > FLOOR
    v = "**slower**" if worse else "faster" if better else "level"
    if (below or above) and not (worse or better):
        v += f" (rounds apart, under {FLOOR:.0f}%)"
    if "nic" in n["limit"] and not (worse or better):
        v += " (NIC)"
    elif "client" in n["limit"] and not (worse or better):
        v += " (clients)"
    print(f"| {name(s, sc)} | {m(o['ops_median'])} [{m(o['ops_min'])} - {m(o['ops_max'])}]{flag(o)} | "
          f"{m(n['ops_median'])} [{m(n['ops_min'])} - {m(n['ops_max'])}]{flag(n)} | {ch:+.1f}% | "
          f"±{float(o['spread_pct']) / 2:.1f}% / ±{float(n['spread_pct']) / 2:.1f}% | {v} | "
          f"{float(o['p99_ms']):.2f} → {float(n['p99_ms']):.2f} | {float(o['p999_ms']):.2f} → {float(n['p999_ms']):.2f} | "
          f"{float(o['us_per_op']):.2f} → {float(n['us_per_op']):.2f} |")

print("\n### MSET and MGET of 10 keys in detail (100 B, pipeline 1, 1,200 connections)\n")
print("| Build | round | commands/s | p50 / p99 / p99.9 ms | server CPU busy | server µs/cmd (user + kernel) | load generators busy |")
print("|---|---|---:|---:|---:|---:|---:|")
for sc in ("mset10_p1", "mget10_p1"):
    for e in (OLD, NEW):
        for r in [r for r in RUNS if r["engine"] == e and r["size"] == "100" and r["scenario"] == sc]:
            print(f"| {e.split('-')[1]} {sc.split('_')[0].upper()} | {r['round']} | {m(r['ops'])} | "
                  f"{float(r['p50_ms']):.2f} / {float(r['p99_ms']):.2f} / {float(r['p999_ms']):.2f} | {r['srv_busy']}% | "
                  f"{r['us_per_op']} ({r['us_user_per_op']} + {r['us_kernel_per_op']}) | {r['client_busy_max']}% |")

print("\n### rc8 against the published 1.2.0 numbers (75482ab, 2026-11-02, eastasia; indicative only)\n")
print("| Scenario | published 75482ab | rc8 today | Change | p99 ms published → today |")
print("|---|---:|---:|---:|---:|")
for s, sc in SCS:
    o, n = P.get(("kividb-cand", s, sc)), S.get((NEW, s, sc))
    if not (o and n):
        continue
    ch = 100 * (float(n["ops_median"]) / float(o["ops_median"]) - 1)
    print(f"| {name(s, sc)} | {m(o['ops_median'])}{flag(o)} | {m(n['ops_median'])}{flag(n)} | {ch:+.1f}% | "
          f"{float(o['p99_ms']):.2f} → {float(n['p99_ms']):.2f} |")

print("\n### Competitors, one round, against their published medians\n")
print("| Engine | Scenario | published | today | Change | p99 ms published → today | rc8 today | rc8 / engine today |")
print("|---|---|---:|---:|---:|---:|---:|---:|")
for e, lab in (("garnet", "Garnet 2.2.5"), ("dragonfly", "Dragonfly 2.0.2"), ("redis@24", "Redis 8.10.3, io-threads 24")):
    for sc in ("get_p16", "set_p16", "mget10_p1", "mset10_p1"):
        o, n, k = P.get((e, "100", sc)), S.get((e, "100", sc)), S.get((NEW, "100", sc))
        if not (o and n):
            continue
        ch = 100 * (float(n["ops_median"]) / float(o["ops_median"]) - 1)
        print(f"| {lab} | {name('100', sc)} | {m(o['ops_median'])}{flag(o)} | {m(n['ops_median'])}{flag(n)} | {ch:+.1f}% | "
              f"{float(o['p99_ms']):.2f} → {float(n['p99_ms']):.2f} | {m(k['ops_median']) if k else '-'} | "
              f"{float(k['ops_median']) / float(n['ops_median']):.2f}× |")

# Regression chart.
plt.rcParams.update({
    "svg.fonttype": "none", "font.family": "DejaVu Sans", "font.size": 10,
    "figure.facecolor": "white", "axes.facecolor": "white", "savefig.facecolor": "white",
    "text.color": "#222222", "axes.labelcolor": "#222222", "xtick.color": "#222222", "ytick.color": "#222222",
    "axes.edgecolor": "#888888", "axes.grid": True, "grid.color": "#e3e3e3", "grid.linewidth": 0.8,
    "axes.axisbelow": True, "axes.spines.top": False, "axes.spines.right": False,
})
C_NEW, C_OLD = "#0072B2", "#8c8c8c"
pairs = [(s, sc) for s, sc in SCS if per_round(NEW, s, sc, "ops") and per_round(OLD, s, sc, "ops")]
LIM = {"ops": (90, 110), "p99_ms": (70, 130)}
fig, axes = plt.subplots(1, 2, figsize=(11, 0.36 * len(pairs) + 1.9), sharey=True)
ys = list(range(len(pairs)))[::-1]
slow = []
for ax, key, lab in ((axes[0], "ops", "throughput, % of rc7 median (higher is better)"),
                     (axes[1], "p99_ms", "p99 latency, % of rc7 median (lower is better)")):
    lo, hi = LIM[key]
    for y, (s, sc) in zip(ys, pairs):
        base = statistics.median(per_round(OLD, s, sc, key))
        for e, dy, c in ((OLD, 0.16, C_OLD), (NEW, -0.16, C_NEW)):
            v = [100 * x / base for x in per_round(e, s, sc, key)]
            out = [x for x in v if x > hi or x < lo]
            if out:
                med = statistics.median(v)
                edge = hi if med > hi else lo
                ax.annotate((f"{med:.0f}% of rc7 →" if edge == hi else f"← {med:.0f}% of rc7"), (edge, y + dy), xytext=(-9 if edge == hi else 9, 0),
                            textcoords="offset points", ha="right" if edge == hi else "left", va="center",
                            fontsize=8, color=c, fontweight="bold")
                v = [min(max(x, lo), hi) for x in v]
                ax.plot(edge, y + dy, ">" if edge == hi else "<", color=c, ms=7)
                continue
            ax.plot([min(v), max(v)], [y + dy, y + dy], color=c, lw=2, solid_capstyle="round")
            ax.plot(v, [y + dy] * len(v), "|", color=c, ms=7)
            ax.plot(statistics.median(v), y + dy, "o", color=c, ms=6, mec="white", mew=1)
        if key == "ops":
            cv, rv = per_round(NEW, s, sc, key), per_round(OLD, s, sc, key)
            if max(cv) < min(rv) and statistics.median(cv) / statistics.median(rv) - 1 < -FLOOR / 100:
                slow.append(f"{name(s, sc)} {100 * (statistics.median(cv) / statistics.median(rv) - 1):+.1f}%")
    ax.set_xlim(lo, hi)
    ax.axvline(100, color="#444444", lw=1)
    ax.set_xlabel(lab)
    ax.grid(axis="y", visible=False)
axes[0].set_yticks(ys, [name(s, sc) for s, sc in pairs])
handles = [plt.Line2D([], [], marker="o", color=c, lw=2, mec="white", label=l)
           for l, c in (("KiviDB 1.2.0-rc7 (598b74d)", C_OLD), ("KiviDB 1.2.0-rc8 (082dab3)", C_NEW))]
fig.legend(handles=handles, loc="lower center", ncol=2, frameon=False, bbox_to_anchor=(0.5, -0.04))
note = ("slower beyond the run-to-run spread: " + ", ".join(slow)) if slow else f"nothing slower beyond the run-to-run spread (> {FLOOR:.0f}% and every round apart)"
fig.suptitle(f"KiviDB 1.2.0-rc8 vs rc7, alternating rounds (dot: median, ticks: each round)\n{note}",
             x=0.01, ha="left", fontsize=11, fontweight="bold")
fig.tight_layout()
fig.savefig(os.path.join(OUT, "regression-rc7-vs-rc8.svg"), format="svg", bbox_inches="tight")
if os.environ.get("PREVIEW"):
    fig.savefig(os.environ["PREVIEW"], dpi=80, bbox_inches="tight")
