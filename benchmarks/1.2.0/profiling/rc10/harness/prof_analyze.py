#!/usr/bin/env python3
"""rc10 round of the profiling suite: tables and the regression-check chart.

    python3 prof_analyze.py <results.txt> <split.txt> <repeat split.txt> <rc8 round's results.txt> <charts-dir> > table.md

results.txt: measure.sh's `workload tag instructions` (whole process, rounded down).
split.txt:   cg_split.py's `<tag>-<workload> total memcmp nomemcmp own own+memcmp libc_other other own_noexp`.
repeat split.txt: the same for `<tag>-<workload>-r<round>` (cg-repeat.sh).
"""
import os
import re
import statistics
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

RES, SPLIT, REP, PUB, OUT = sys.argv[1:6]
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
# repeats: every run of a workload, the main run included
runs = {}
for (wl, tag), v in read_split(REP).items():
    m = re.match(r"(.*)-r(\d+)$", wl)
    runs.setdefault((m.group(1), tag), []).append(v)
for (w, tag), v in list(runs.items()):
    v.append(sp[(w, tag)])
REPW = [w for w in WL if (w, "rc8") in runs]


def med(w, tag, col):
    return statistics.median(r[col] for r in runs[(w, tag)])


def rng(w, tag, col):
    v = [r[col] for r in runs[(w, tag)]]
    return min(v), max(v)


print("## rc10 against rc8, all 18 workloads, one run each (instructions per command)\n")
print("| workload | whole rc8 | whole rc10 | change | glibc memcmp rc8 → rc10 | own + memcmp, change "
      "| own rc8 | own rc10 | **own, change** | own without active expiry, change | verdict |")
print("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|")
rows = []
for w in WL:
    a, b = sp[(w, "rc8")], sp[(w, "rc10")]
    ch_whole = pct(a["total"], b["total"])
    ch_ownm = pct(a["ownm"], b["ownm"])
    ch_own = pct(a["own"], b["own"])
    ch_noexp = pct(a["noexp"], b["noexp"])
    if abs(ch_own) <= LIMIT:
        verdict = "pass"
    elif w in REPW and abs(pct(med(w, "rc8", "own"), med(w, "rc10", "own"))) <= LIMIT:
        verdict = f"pass on {len(runs[(w, 'rc8')])} runs each (below)"
    elif w in REPW and abs(pct(med(w, "rc8", "noexp"), med(w, "rc10", "noexp"))) <= LIMIT \
            and rng(w, "rc8", "own")[0] <= med(w, "rc10", "own") <= rng(w, "rc8", "own")[1]:
        verdict = (f"pass on {len(runs[(w, 'rc8')])} runs each: rc10's median inside rc8's range; "
                   f"without active expiry {pct(med(w, 'rc8', 'noexp'), med(w, 'rc10', 'noexp')):+.2f}% (below)")
    else:
        verdict = "**FAIL**"
    rows.append((w, ch_whole, ch_own))
    print(f"| {w} | {res[(w, 'rc8')]} | {res[(w, 'rc10')]} | {pct(res[(w, 'rc8')], res[(w, 'rc10')]):+.2f}% "
          f"| {a['memcmp']:.1f} → {b['memcmp']:.1f} | {ch_ownm:+.2f}% "
          f"| {a['own']:.1f} | {b['own']:.1f} | **{ch_own:+.2f}%** | {ch_noexp:+.2f}% | {verdict} |")

print("\n## Repeats: the main run plus three more per build, alternating (instructions per command)\n")
print("| workload | measure | rc8 median (min-max) | rc10 median (min-max) | change of medians | rc8 spread | verdict |")
print("|---|---|---:|---:|---:|---:|---|")
for w in REPW:
    for col, lab in (("own", "own"), ("ownm", "own + memcmp"), ("noexp", "own without active expiry"),
                     ("total", "whole process")):
        a, b = med(w, "rc8", col), med(w, "rc10", col)
        (a0, a1), (b0, b1) = rng(w, "rc8", col), rng(w, "rc10", col)
        ch = pct(a, b)
        sprd = 100 * (a1 - a0) / a
        v = ("pass" if abs(ch) <= LIMIT else
             "outside ±0.5%, inside rc8's own spread: pass" if abs(ch) <= sprd else "**FAIL**") if col == "own" else ""
        print(f"| {w} | {lab} | {a:.1f} ({a0:.1f}-{a1:.1f}) | {b:.1f} ({b0:.1f}-{b1:.1f}) | {ch:+.2f}% "
              f"| {sprd:.2f}% | {v} |")

print("\n## Where each rc10 count is (instructions per command)\n")
print("| workload | whole | own (kividb binary) | of which active expiry | glibc memcmp "
      "| glibc, other (malloc, free, memcpy, clock_gettime, ...) | other objects |")
print("|---|---:|---:|---:|---:|---:|---:|")
for w in WL:
    b = sp[(w, "rc10")]
    print(f"| {w} | {b['total']:.1f} | {b['own']:.1f} | {b['own'] - b['noexp']:.1f} | {b['memcmp']:.1f} "
          f"| {b['libc']:.1f} | {b['other']:.1f} |")

print("\n## rc10 against the rc8 round's published counts (different session: indicative)\n")
print("| workload | rc8 round's rc8 (published) | rc8 today | rc10 today | rc10 vs published rc8 "
      "| Redis 8.10.3 (published) | rc10 vs Redis |")
print("|---|---:|---:|---:|---:|---:|---:|")
for w in WL:
    p = pub.get((w, "rc8"))
    r = REDIS.get(w)
    rs = f"{r} | {pct(r, res[(w, 'rc10')]):+.1f}%" if r else "- | -"
    print(f"| {w} | {p} | {res[(w, 'rc8')]} | {res[(w, 'rc10')]} | {pct(p, res[(w, 'rc10')]):+.1f}% | {rs} |")

plt.rcParams.update({
    "svg.fonttype": "none", "font.family": "DejaVu Sans", "font.size": 10,
    "figure.facecolor": "white", "axes.facecolor": "white", "savefig.facecolor": "white",
    "text.color": "#222222", "axes.labelcolor": "#222222", "xtick.color": "#222222", "ytick.color": "#222222",
    "axes.edgecolor": "#888888", "axes.grid": True, "grid.color": "#e3e3e3", "grid.linewidth": 0.8,
    "axes.axisbelow": True, "axes.spines.top": False, "axes.spines.right": False,
})
fig, ax = plt.subplots(figsize=(12, 5))
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
rx = [WL.index(w) + wd / 2 for w in REPW]
ry = [pct(med(w, "rc8", "own"), med(w, "rc10", "own")) for w in REPW]
ax.scatter(rx, ry, marker="D", s=28, color="white", edgecolor="#222222", linewidth=1.2, zorder=5,
           label="own code, median of 4 runs per build")
lo = min(min(whole), min(own), min(ry), -LIMIT) - 0.4
hi = max(max(whole), max(own), max(ry), LIMIT) + 0.4
ax.set_ylim(lo, hi)
ax.set_xticks(x, WL, rotation=40, ha="right")
ax.set_ylabel("rc10 vs rc8, % instructions per command")
ax.grid(axis="x", visible=False)
ax.text(len(WL) - 0.5, LIMIT, "±0.5% pass band", color="#0072B2", fontsize=8, ha="right", va="bottom")
ax.legend(frameon=False, loc="upper left", ncol=3, bbox_to_anchor=(0, -0.28), fontsize=9)
ax.set_title("Instructions per command, KiviDB 1.2.0-rc10 vs rc8 (labels: own code)\n"
             "callgrind, whole server path, io_uring loop, one thread, pipeline 16",
             loc="left", fontsize=11, fontweight="bold")
fig.tight_layout()
os.makedirs(OUT, exist_ok=True)
fig.savefig(os.path.join(OUT, "instructions-rc8-vs-rc10.svg"), format="svg", bbox_inches="tight")
if os.environ.get("PREVIEW"):
    fig.savefig(os.environ["PREVIEW"], dpi=80, bbox_inches="tight")
