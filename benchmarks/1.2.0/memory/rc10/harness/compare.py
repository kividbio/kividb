#!/usr/bin/env python3
"""rc10 against rc8 (this session) and against the rc8 round's published numbers (rc8/).

    python3 compare.py <reg.json from analyze.py> <rc8 round results/compare.md> <rc8 round results/regression-rows.json> <out.md> <chart.svg>

With one round per build there is no spread of our own, so a row uses the spread the rc8 round judged the same
metric against (the larger of the published 75482abb run's 3-5-round spread and the rc8 round's own), or our own
if larger. A row is a regression when rc10 is worse than rc8 by more than that spread and by at least 1%.
"""
import json, re, statistics as st, sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

REGJ, PUBCMP, PUBREG, OUT, SVG = sys.argv[1:6]
FLOOR = 1.0
rows = json.load(open(REGJ))

# the rc8 round's compare.md, first table: section, metric -> spread % it judged the row against
pubsp = {}
for line in open(PUBCMP, encoding="utf-8"):
    if line.startswith("### "):
        break
    if line.startswith("| ") and not line.startswith("| section") and not line.startswith("|---"):
        c = [x.strip() for x in line.strip().strip("|").split("|")]
        if len(c) < 7:
            continue
        sp = re.match(r"^(-?[\d.]+)%", c[5])
        pubsp[(c[0], c[1])] = float(sp.group(1)) if sp else None
# the rc8 round's values of rc8 (median over its rounds)
pubv = {(r["section"], r["metric"]): st.median(r["rc8"]) for r in json.load(open(PUBREG)) if r["rc8"]}


def med(x): return st.median(x)


def fmt(x, d):
    if x is None:
        return "-"
    if abs(x) >= 10000:
        return f"{x:,.0f}"
    return f"{x:.{d}f}"


def mr(xs, d):
    if len(xs) == 1:
        return fmt(xs[0], d)
    return f"{fmt(med(xs), d)} ({fmt(min(xs), d)}-{fmt(max(xs), d)})"


out = []
out.append("| section | metric | rc10 | rc8 | change | spread | verdict |")
out.append("|---|---|---|---|---|---|---|")
res = []
flagged = []
for r in rows:
    s, m, b, cv, rv, d = r["section"], r["metric"], r["better"], r["rc10"], r["rc8"], r["digits"]
    if not cv or not rv:
        continue
    cm, rm = med(cv), med(rv)
    ch = 100 * (cm - rm) / abs(rm) if rm else None
    own = max(((max(x) - min(x)) / abs(med(x)) * 100) if med(x) else 0 for x in (cv, rv))
    ps = pubsp.get((s, m))
    sp = max(own, ps or 0)
    src = "rc8 round" if (ps is not None and ps >= own) else "own"
    worse = ch is not None and ((ch > 0) if b == "lower" else (ch < 0))
    if ch is None:
        v = "-"
    elif abs(cm - rm) < 1e-9:
        v = "level"
    elif worse and abs(ch) > sp and abs(ch) >= FLOOR:
        v = "**worse beyond spread**"
        flagged.append((s, m, ch, sp))
    elif worse and abs(ch) > sp:
        v = f"level (under {FLOOR:g}%)"
    elif not worse and abs(ch) > sp and abs(ch) >= FLOOR:
        v = "better"
    else:
        v = "level"
    res.append((s, m, b, cm, rm, ch, sp, v))
    out.append(f"| {s} | {m} | {mr(cv, d)} | {mr(rv, d)} | {fmt(ch, 1) if ch is not None else '-'}% | {sp:.1f}% ({src}) | {v} |")
out.append("")
out.append("Flagged: " + ("; ".join(f"{s} {m} ({c:+.1f}%, spread {p:.1f}%)" for s, m, c, p in flagged) if flagged else "none") + "")
out.append("")
out.append("### rc10 against the rc8 round's published rc8 numbers (other session, other VMs of the same sizes)")
out.append("")
out.append("| section | metric | rc10 (this run) | rc8 (rc8 round, median) | change |")
out.append("|---|---|---|---|---|")
for r in rows:
    s, m, d = r["section"], r["metric"], r["digits"]
    if not r["rc10"]:
        continue
    pm = pubv.get((s, m))
    cm = med(r["rc10"])
    ch = 100 * (cm - pm) / abs(pm) if pm else None
    out.append(f"| {s} | {m} | {fmt(cm, d)} | {fmt(pm, d)} | {fmt(ch, 1) if ch is not None else '-'}% |")
open(OUT, "w", newline="\n").write("\n".join(out) + "\n")

# chart: rc10 / rc8 per metric, with the spread band each row is judged against
plt.rcParams.update({
    "svg.fonttype": "none", "font.family": "DejaVu Sans", "font.size": 10,
    "figure.facecolor": "white", "axes.facecolor": "white", "savefig.facecolor": "white",
    "axes.grid": True, "grid.color": "#e3e3e3", "axes.spines.top": False, "axes.spines.right": False,
    "legend.frameon": False,
})
sel = [x for x in res if x[4]][::-1]
fig, ax = plt.subplots(figsize=(9.5, 0.26 * len(sel) + 1.4))
for i, (s, m, b, cm, rm, ch, sp, v) in enumerate(sel):
    ax.plot([1 - sp / 100, 1 + sp / 100], [i, i], color="#cfcfcf", lw=5, solid_capstyle="butt", zorder=1,
            label="spread the row is judged against" if i == 0 else None)
    ax.scatter([1.0], [i], color="#8C8C8C", s=16, zorder=2, label="KiviDB 1.2.0-rc8 (082dab3d)" if i == 0 else None)
    ax.scatter([cm / rm], [i], color="#0072B2", marker="X" if v.startswith("**") else "o", s=30 if v.startswith("**") else 22, zorder=3,
               label="KiviDB 1.2.0-rc10 (39bfacd2)" if i == 0 else None)
ax.axvline(1.0, color="#555555", lw=1)
ax.set_yticks(range(len(sel)), [f"{s}: {m}" + (" (higher is better)" if b == "higher" else "") for s, m, b, *_ in sel],
              fontsize=7)
ax.set_xlabel("rc10 / rc8 (1 round each, same VMs, alternating); X = worse beyond the spread")
ax.grid(axis="y", visible=False)
ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0), fontsize=8)
ax.set_title("Memory suite: rc10 against rc8, every metric (lower is better unless marked)", loc="left",
             fontweight="bold", fontsize=11)
fig.savefig(SVG, format="svg", bbox_inches="tight")
print("\n".join(out))
