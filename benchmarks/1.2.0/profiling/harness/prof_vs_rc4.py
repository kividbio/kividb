"""prof_vs_rc4.py RESULTS_DIR CHARTS_DIR: the candidate against rc4 in the profiling suite.

CPU per request: RESULTS_DIR/kividb-{cand,rc4}-r{1,2,3}/cpu.csv (release binaries, three
alternating rounds of profile.sh) and the frame-pointer builds kividb-{cand,rc4}-fp.
Instructions per command: RESULTS_DIR/callgrind-table.csv (columns rc4, cand).
Prints two Markdown tables and draws CHARTS_DIR/vs-rc4-cpu.svg and vs-rc4-instructions.svg.
"""
import csv
import os
import statistics
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
NEW, OLD = "#0072B2", "#56B4E9"
WL = ["get-p1", "get-p16", "set-p1", "set-p16"]


def cpu(run):
    p = os.path.join(RES, run, "cpu.csv")
    if not os.path.exists(p):
        return {}
    return {f"{r['workload']}-p{r['pipeline']}": r for r in csv.DictReader(open(p))}


def rounds(build):
    return [cpu(f"kividb-{build}-r{i}") for i in (1, 2, 3)]


new_r, old_r = rounds("cand"), rounds("rc4")
new_fp, old_fp = cpu("kividb-cand-fp"), cpu("kividb-rc4-fp")
print("| workload | 1.2.0 ops/s | rc4 ops/s | 1.2.0 CPU us/request (user + kernel) | rc4 CPU us/request | change | per round | frame-pointer builds: 1.2.0 / rc4 us |")
print("|---|---|---|---|---|---|---|---|")
chart = []
for w in WL:
    a = [r[w] for r in new_r if w in r]
    b = [r[w] for r in old_r if w in r]
    if not a or not b:
        continue
    ta = [float(r["total_us_per_op"]) for r in a]
    tb = [float(r["total_us_per_op"]) for r in b]
    ua = statistics.median(float(r["user_us_per_op"]) for r in a)
    ka = statistics.median(float(r["sys_us_per_op"]) for r in a)
    ub = statistics.median(float(r["user_us_per_op"]) for r in b)
    kb = statistics.median(float(r["sys_us_per_op"]) for r in b)
    oa = statistics.median(float(r["ops_per_sec"]) for r in a)
    ob = statistics.median(float(r["ops_per_sec"]) for r in b)
    ma, mb = statistics.median(ta), statistics.median(tb)
    per = " ".join(f"{(x / y - 1) * 100:+.1f}%" for x, y in zip(ta, tb))
    fp = (f"{float(new_fp[w]['total_us_per_op']):.2f} / {float(old_fp[w]['total_us_per_op']):.2f}"
          if w in new_fp and w in old_fp else "-")
    print(f"| {w.replace('-p', ', pipeline ').upper().replace('PIPELINE', 'pipeline')} | {oa / 1e6:.2f}M | {ob / 1e6:.2f}M | "
          f"{ma:.2f} ({ua:.2f} + {ka:.2f}), {min(ta):.2f}-{max(ta):.2f} | {mb:.2f} ({ub:.2f} + {kb:.2f}), {min(tb):.2f}-{max(tb):.2f} | "
          f"{(ma / mb - 1) * 100:+.1f}% | {per} | {fp} |")
    chart.append((w, ta, tb))

if chart:
    fig, ax = plt.subplots(figsize=(10, 4.2))
    for i, (w, ta, tb) in enumerate(chart):
        for off, vals, col in [(-0.19, tb, OLD), (0.19, ta, NEW)]:
            m = statistics.median(vals)
            ax.bar(i + off, m, width=0.36, color=col)
            ax.errorbar(i + off, m, yerr=[[m - min(vals)], [max(vals) - m]], fmt="none", ecolor="#222222", capsize=2, elinewidth=0.8)
            ax.text(i + off, m, f"{m:.2f}", ha="center", va="bottom", fontsize=8)
    ax.set_xticks(range(len(chart)))
    ax.set_xticklabels([w.replace("-p", ", pipeline ").upper().replace("PIPELINE", "pipeline") for w, _, _ in chart])
    ax.set_ylabel("server CPU us per request")
    ax.set_ylim(bottom=0)
    ax.bar(0, 0, color=OLD, label="KiviDB 1.2.0-rc4")
    ax.bar(0, 0, color=NEW, label="KiviDB 1.2.0 (75482abb)")
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.12), ncol=2, frameon=False)
    ax.set_title(os.environ.get("VSCPU_TITLE", "CPU per request, 1.2.0 against rc4 (release binaries, median of 3 alternating rounds, whiskers min-max)"),
                 loc="left", fontsize=11)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "vs-rc4-cpu.svg"))
    plt.close(fig)

cg = os.path.join(RES, "callgrind-table.csv")
if os.path.exists(cg):
    rows = [r for r in csv.DictReader(open(cg)) if r.get("rc4") and r.get("cand")]
    print()
    print("| workload | rc4 | 1.2.0 (75482abb) | change |")
    print("|---|---|---|---|")
    for r in rows:
        print(f"| {r['workload']} | {r['rc4']} | {r['cand']} | {(int(r['cand']) / int(r['rc4']) - 1) * 100:+.1f}% |")
    fig, ax = plt.subplots(figsize=(12, 4.4))
    xs = range(len(rows))
    ax.bar([x - 0.19 for x in xs], [int(r["rc4"]) for r in rows], width=0.36, color=OLD, label="KiviDB 1.2.0-rc4")
    ax.bar([x + 0.19 for x in xs], [int(r["cand"]) for r in rows], width=0.36, color=NEW, label="KiviDB 1.2.0 (75482abb)")
    for x, r in zip(xs, rows):
        d = (int(r["cand"]) / int(r["rc4"]) - 1) * 100
        ax.text(x + 0.19, int(r["cand"]), f"{d:+.1f}%", ha="center", va="bottom", fontsize=7)
    ax.set_xticks(list(xs))
    ax.set_xticklabels([r["workload"] for r in rows], rotation=30, ha="right", fontsize=8)
    ax.set_ylabel("instructions per command (callgrind)")
    ax.set_ylim(bottom=0)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.25), ncol=2, frameon=False)
    ax.set_title(os.environ.get("VSCG_TITLE", "Instructions per command, 1.2.0 against rc4 (one thread, pipeline 16, callgrind)"),
                 loc="left", fontsize=11)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "vs-rc4-instructions.svg"))
    plt.close(fig)
