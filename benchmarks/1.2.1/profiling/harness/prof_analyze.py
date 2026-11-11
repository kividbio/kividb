#!/usr/bin/env python3
"""1.2.1 round of the profiling suite: tables and the chart.

    python3 prof_analyze.py <results.txt> <split.txt> <repeat split.txt> <1.2.0 suite's results.txt> <charts-dir> > table.md

results.txt: measure.sh's `workload tag instructions` (whole process, rounded down).
split.txt:   cg_split.py's `<tag>-<workload> total memcmp nomemcmp own own+memcmp libc_other other own_noexp`.
repeat split.txt: the same for `<tag>-<workload>-r<round>` (cg-repeat.sh).
The 1.2.0 suite's results.txt: its last round's counts; the column used is the tag given as PUBTAG
(environment), the tag under which that round measured the 1.2.0 code.
"""
import os
import re
import statistics
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

RES, SPLIT, REP, PUB, OUT = sys.argv[1:6]
A, B = "v120", "v121"
NA, NB = "1.2.0", "1.2.1"
PUBTAG = os.environ["PUBTAG"]
LIMIT = 0.5
REDIS = {"GET-100": 4139, "SET-100": 5359, "GET-1K": 4250, "SET-1K": 5498, "MIX-100": 9559, "INCR": 4489,
         "DEL": 4483}
COLS = ("total", "memcmp", "nomemcmp", "own", "ownm", "libc", "other", "noexp")


def read_res(path):
    d, order = {}, []
    for line in open(path):
        p = line.split()
        if len(p) == 3 and p[2].isdigit():
            d[(p[0], p[1])] = int(p[2])
            if p[0] not in order:
                order.append(p[0])
    return d, order


def read_split(path):
    d = {}
    if not os.path.exists(path):
        return d
    for line in open(path):
        if line.startswith("#") or not line.strip():
            continue
        p = line.split()
        tag, wl = p[0].split("-", 1)
        d[(wl, tag)] = dict(zip(COLS, map(float, p[1:9])))
    return d


def pct(a, b):
    return 100 * (b / a - 1)


res, WL = read_res(RES)
sp = read_split(SPLIT)
pub, _ = read_res(PUB)
runs = {}
for (wl, tag), v in read_split(REP).items():
    m = re.match(r"(.*)-r(\d+)$", wl)
    runs.setdefault((m.group(1), tag), []).append(v)
for (w, tag), v in list(runs.items()):
    v.append(sp[(w, tag)])
REPW = [w for w in WL if (w, A) in runs and (w, B) in runs]


def med(w, tag, col):
    return statistics.median(r[col] for r in runs[(w, tag)])


def rng(w, tag, col):
    v = [r[col] for r in runs[(w, tag)]]
    return min(v), max(v)


print(f"## {NB} against {NA}, all {len(WL)} workloads, one run each (instructions per command)\n")
print(f"| workload | whole {NA} | whole {NB} | change | glibc memcmp {NA} → {NB} | own + memcmp, change "
      f"| own {NA} | own {NB} | **own, change** | own without active expiry, change | verdict |")
print("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|")
rows = []
for w in WL:
    a, b = sp[(w, A)], sp[(w, B)]
    ch_own = pct(a["own"], b["own"])
    if w in REPW:
        n = len(runs[(w, A)])
        mo = pct(med(w, A, "own"), med(w, B, "own"))
        mn = pct(med(w, A, "noexp"), med(w, B, "noexp"))
        if abs(mo) <= LIMIT:
            verdict = f"within ±0.5% on the medians of {n} runs each ({mo:+.2f}%)"
        elif abs(mn) <= LIMIT:
            verdict = f"medians of {n} runs {mo:+.2f}%; without active expiry {mn:+.2f}%"
        else:
            verdict = f"**moves {mo:+.2f}% on the medians of {n} runs** (below)"
    else:
        verdict = "within ±0.5%" if abs(ch_own) <= LIMIT else "**moves** (not repeated)"
    rows.append((w, pct(a["total"], b["total"]), ch_own))
    print(f"| {w} | {res[(w, A)]} | {res[(w, B)]} | {pct(res[(w, A)], res[(w, B)]):+.2f}% "
          f"| {a['memcmp']:.1f} → {b['memcmp']:.1f} | {pct(a['ownm'], b['ownm']):+.2f}% "
          f"| {a['own']:.1f} | {b['own']:.1f} | **{ch_own:+.2f}%** | {pct(a['noexp'], b['noexp']):+.2f}% | {verdict} |")

if REPW:
    print("\n## Repeats: the main run plus the repeat rounds, per build (instructions per command)\n")
    print(f"| workload | measure | {NA} median (min-max) | {NB} median (min-max) | change of medians "
          f"| {NA} spread | {NB} spread |")
    print("|---|---|---:|---:|---:|---:|---:|")
    for w in REPW:
        for col, lab in (("own", "own"), ("noexp", "own without active expiry"), ("ownm", "own + memcmp"),
                         ("total", "whole process")):
            a, b = med(w, A, col), med(w, B, col)
            (a0, a1), (b0, b1) = rng(w, A, col), rng(w, B, col)
            print(f"| {w} | {lab} | {a:.1f} ({a0:.1f}-{a1:.1f}) | {b:.1f} ({b0:.1f}-{b1:.1f}) | {pct(a, b):+.2f}% "
                  f"| {100 * (a1 - a0) / a:.2f}% | {100 * (b1 - b0) / b:.2f}% |")

print(f"\n## Where each {NB} count is (instructions per command, main run)\n")
print("| workload | whole | own (kividb binary) | of which active expiry | glibc memcmp "
      "| glibc, other (malloc, free, memcpy, clock_gettime, ...) | other objects |")
print("|---|---:|---:|---:|---:|---:|---:|")
for w in WL:
    b = sp[(w, B)]
    print(f"| {w} | {b['total']:.1f} | {b['own']:.1f} | {b['own'] - b['noexp']:.1f} | {b['memcmp']:.1f} "
          f"| {b['libc']:.1f} | {b['other']:.1f} |")

print(f"\n## Against the 1.2.0 suite's published counts of the same 1.2.0 code (different session: indicative)\n")
print(f"| workload | 1.2.0 suite (published) | {NA} today | {NB} today | {NB} vs published | Redis 8.10.3 (published) "
      f"| {NB} vs Redis |")
print("|---|---:|---:|---:|---:|---:|---:|")
for w in WL:
    p = pub.get((w, PUBTAG))
    r = REDIS.get(w)
    rs = f"{r} | {pct(r, res[(w, B)]):+.1f}%" if r else "- | -"
    ps = f"{p} | {res[(w, A)]} | {res[(w, B)]} | {pct(p, res[(w, B)]):+.1f}%" if p else \
        f"- | {res[(w, A)]} | {res[(w, B)]} | -"
    print(f"| {w} | {ps} | {rs} |")

plt.rcParams.update({
    "svg.fonttype": "none", "font.family": "DejaVu Sans", "font.size": 10,
    "figure.facecolor": "white", "axes.facecolor": "white", "savefig.facecolor": "white",
    "text.color": "#222222", "axes.labelcolor": "#222222", "xtick.color": "#222222", "ytick.color": "#222222",
    "axes.edgecolor": "#888888", "axes.grid": True, "grid.color": "#e3e3e3", "grid.linewidth": 0.8,
    "axes.axisbelow": True, "axes.spines.top": False, "axes.spines.right": False,
})
fig, ax = plt.subplots(figsize=(13, 5))
x = list(range(len(WL)))
wd = 0.38
ax.axhspan(-LIMIT, LIMIT, color="#0072B2", alpha=0.07, lw=0)
ax.axhline(0, color="#888888", lw=0.8)
for lim in (-LIMIT, LIMIT):
    ax.axhline(lim, color="#0072B2", lw=0.8, ls="--")
whole = [r[1] for r in rows]
own = [r[2] for r in rows]
ax.bar([i - wd / 2 for i in x], whole, wd, color="#8c8c8c", label="whole process, one run (as measure.sh counts it)")
ax.bar([i + wd / 2 for i in x], own, wd, color="#0072B2", label="KiviDB's own code (kividb binary), one run")
for i, v in enumerate(own):
    ax.annotate(f"{v:+.2f}", (i + wd / 2, v), xytext=(0, 3 if v >= 0 else -10), textcoords="offset points",
                ha="center", fontsize=7, color="#0072B2", fontweight="bold")
ry = []
if REPW:
    rx = [WL.index(w) + wd / 2 for w in REPW]
    ry = [pct(med(w, A, "own"), med(w, B, "own")) for w in REPW]
    ax.scatter(rx, ry, marker="D", s=28, color="white", edgecolor="#222222", linewidth=1.2, zorder=5,
               label="own code, median of the repeats")
lo = min(min(whole), min(own), min(ry or [0]), -LIMIT) - 0.4
hi = max(max(whole), max(own), max(ry or [0]), LIMIT) + 0.4
ax.set_ylim(lo, hi)
ax.set_xticks(x, WL, rotation=40, ha="right")
ax.set_ylabel(f"{NB} vs {NA}, % instructions per command")
ax.grid(axis="x", visible=False)
ax.text(len(WL) - 0.5, LIMIT, "±0.5% band", color="#0072B2", fontsize=8, ha="right", va="bottom")
ax.legend(frameon=False, loc="upper left", ncol=3, bbox_to_anchor=(0, -0.28), fontsize=9)
ax.set_title(f"Instructions per command, KiviDB {NB} (878746e4) vs {NA} (labels: own code)\n"
             "callgrind, whole server path, io_uring loop, one thread, pipeline 16",
             loc="left", fontsize=11, fontweight="bold")
fig.tight_layout()
os.makedirs(OUT, exist_ok=True)
fig.savefig(os.path.join(OUT, "instructions-1.2.0-vs-1.2.1.svg"), format="svg", bbox_inches="tight")
if os.environ.get("PREVIEW"):
    fig.savefig(os.environ["PREVIEW"], dpi=80, bbox_inches="tight")
