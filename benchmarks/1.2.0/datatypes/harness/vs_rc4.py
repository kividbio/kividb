"""vs_rc4.py RESULTS_DIR CHARTS_DIR [NEW OLD]: the candidate against rc4.

Reads RESULTS_DIR/runs.csv (summarize.py; the KiviDB builds ran in three
alternating rounds of three runs each). For every command, value size and
pipeline: the median ops/s of each build over all its runs, the spread
((max-min)/median of those runs), the median of each round, and p50/p99/p99.9
(medians over the runs). A row is a regression when the new build's median is
below the old one's by more than the larger of the two spreads, and faster
when above it by more than that. Writes RESULTS_DIR/vs-rc4.csv, prints a
Markdown table, and draws CHARTS_DIR/vs-rc4.svg (both builds as % of rc4).
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
NEW, OLD = (sys.argv[3], sys.argv[4]) if len(sys.argv) > 4 else ("kividb-cand", "kividb-rc4")
NEW_NAME, OLD_NAME = "KiviDB 1.2.0 (75482abb)", "KiviDB 1.2.0-rc4"
CMDS = ["LPUSH", "LPOP", "LRANGE_100", "HSET", "HGET", "HGETALL", "SADD", "SISMEMBER", "ZADD", "ZRANGE", "INCR"]

runs = [r for r in csv.DictReader(open(os.path.join(RES, "runs.csv"))) if r["engine"] in (NEW, OLD)]
G = {}
for r in runs:
    G.setdefault((r["engine"], r["command"], r["value_bytes"], r["pipeline"]), []).append(r)


def stats(rs):
    ops = [float(r["ops_per_sec"]) for r in rs]
    med = statistics.median(ops)
    rounds = {}
    for r in rs:
        rounds.setdefault(int(r["round"]), []).append(float(r["ops_per_sec"]))
    return {
        "n": len(ops), "med": med, "min": min(ops), "max": max(ops),
        "spread": (max(ops) - min(ops)) / med * 100 if med else 0.0,
        "rounds": [statistics.median(rounds[k]) for k in sorted(rounds)],
        "p50": statistics.median(float(r["p50_ms"]) for r in rs),
        "p99": statistics.median(float(r["p99_ms"]) for r in rs),
        "p999": statistics.median(float(r["p999_ms"]) for r in rs),
    }


def fmt(x):
    return f"{x / 1e6:.2f}M" if x >= 1e6 else f"{x / 1e3:.0f}k"


rows = []
for v in ["100", "2048"]:
    for p in ["1", "16"]:
        for c in CMDS:
            vv = "" if c == "INCR" else v
            if c == "INCR" and v != "100":
                continue
            a, b = G.get((NEW, c, vv, p)), G.get((OLD, c, vv, p))
            if not a or not b:
                continue
            A, B = stats(a), stats(b)
            delta = (A["med"] / B["med"] - 1) * 100
            noise = max(A["spread"], B["spread"])
            per_round = [(x / y - 1) * 100 for x, y in zip(A["rounds"], B["rounds"])]
            verdict = "regression" if delta < -noise else ("faster" if delta > noise else "same")
            rows.append({
                "command": c, "value_bytes": vv, "pipeline": p,
                "new_median": round(A["med"]), "new_min": round(A["min"]), "new_max": round(A["max"]),
                "new_spread_pct": round(A["spread"], 1), "new_runs": A["n"],
                "old_median": round(B["med"]), "old_min": round(B["min"]), "old_max": round(B["max"]),
                "old_spread_pct": round(B["spread"], 1), "old_runs": B["n"],
                "delta_pct": round(delta, 1), "round_deltas_pct": " ".join(f"{d:+.1f}" for d in per_round),
                "new_p50_ms": A["p50"], "new_p99_ms": A["p99"], "new_p999_ms": A["p999"],
                "old_p50_ms": B["p50"], "old_p99_ms": B["p99"], "old_p999_ms": B["p999"],
                "verdict": verdict,
            })

with open(os.path.join(RES, "vs-rc4.csv"), "w", newline="\n") as fh:
    w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()), lineterminator="\n")
    w.writeheader()
    w.writerows(rows)

print(f"| values, pipeline | command | {NEW_NAME} | {OLD_NAME} | change | per round | p99 new / rc4 (ms) | p99.9 new / rc4 (ms) | verdict |")
print("|---|---|---|---|---|---|---|---|---|")
for r in rows:
    vb = "100 B" if r["value_bytes"] in ("100", "") else "2 KB"
    print(f"| {vb}, p{r['pipeline']} | {r['command']} | {fmt(r['new_median'])} ({r['new_spread_pct']}%) | "
          f"{fmt(r['old_median'])} ({r['old_spread_pct']}%) | {r['delta_pct']:+.1f}% | {r['round_deltas_pct']} | "
          f"{r['new_p99_ms']:.2f} / {r['old_p99_ms']:.2f} | {r['new_p999_ms']:.2f} / {r['old_p999_ms']:.2f} | "
          f"{'**regression**' if r['verdict'] == 'regression' else r['verdict']} |")

# chart: both builds as % of rc4's median, whiskers min-max of all runs
os.makedirs(OUT, exist_ok=True)
fig, axes = plt.subplots(2, 2, figsize=(13, 7.6), squeeze=False)
for k, (v, p) in enumerate([("100", "1"), ("100", "16"), ("2048", "1"), ("2048", "16")]):
    ax = axes[k // 2][k % 2]
    sel = [r for r in rows if r["pipeline"] == p and (r["value_bytes"] == v or (r["value_bytes"] == "" and v == "100"))]
    xs = range(len(sel))
    for off, who, col in [(-0.19, "old", "#56B4E9"), (0.19, "new", "#0072B2")]:
        base = [r["old_median"] for r in sel]
        h = [r[f"{who}_median"] / b * 100 for r, b in zip(sel, base)]
        lo = [hh - r[f"{who}_min"] / b * 100 for r, b, hh in zip(sel, base, h)]
        hi = [r[f"{who}_max"] / b * 100 - hh for r, b, hh in zip(sel, base, h)]
        ax.bar([x + off for x in xs], h, width=0.36, color=col, label=OLD_NAME if who == "old" else NEW_NAME)
        ax.errorbar([x + off for x in xs], h, yerr=[lo, hi], fmt="none", ecolor="#222222", elinewidth=0.7, capsize=2)
    for x, r in zip(xs, sel):
        if r["verdict"] != "same":
            ax.text(x, 4, "slower" if r["verdict"] == "regression" else "faster", ha="center", fontsize=7,
                    color="#B00020" if r["verdict"] == "regression" else "#1b5e20", rotation=90)
    ax.axhline(100, color="#555555", linewidth=0.8)
    ax.set_xticks(list(xs))
    ax.set_xticklabels([r["command"] for r in sel], rotation=40, ha="right", fontsize=8)
    ax.set_ylim(0, 135)
    ax.set_ylabel("% of rc4's median ops/s")
    ax.set_title(f"{'100 B' if v == '100' else '2 KB'} values, pipeline {p}", loc="left", fontsize=10)
h, lab = axes[0][0].get_legend_handles_labels()
fig.legend(h, lab, loc="lower center", ncol=2, frameon=False)
nreg = sum(r["verdict"] == "regression" for r in rows)
fig.suptitle(os.environ.get("VS_TITLE", f"1.2.0 (75482abb) against rc4: {nreg} of {len(rows)} rows slower by more than "
             "the run-to-run spread\n(9 runs each in 3 alternating rounds; bars = median, whiskers = min-max)"),
             x=0.01, ha="left", fontsize=12)
fig.tight_layout(rect=(0, 0.05, 1, 0.92))
fig.savefig(os.path.join(OUT, "vs-rc4.svg"))
plt.close(fig)
