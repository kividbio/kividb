"""vs_rc8.py RESULTS_DIR CHARTS_DIR RC8_ROUND_VS_RC7_CSV: rc10 against rc8 (adapted from vs_rc7.py).

Reads RESULTS_DIR/runs.csv (summarize.py; the two KiviDB builds ran one round of three runs
each, rc10 first) and the rc8 round's vs-rc7.csv, whose per-row spreads ((max-min)/median over
6 runs, for rc8 and for rc7) are the run-to-run noise that round measured. For every command,
value size and pipeline: each build's median ops/s, min, max and spread over its runs, and
p50/p99/p99.9 (medians over the runs). The threshold of a row is the largest of the rc8 round's
two spreads and today's two spreads. A row is a regression when rc10's median is below rc8's by
more than the threshold, and faster when above it by more than that. Writes
RESULTS_DIR/vs-rc8.csv, prints a Markdown table, and draws CHARTS_DIR/vs-rc8.svg.
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
RES, OUT, PRIOR = sys.argv[1], sys.argv[2], sys.argv[3]
NEW, OLD = "kividb-rc10", "kividb-rc8"
NEW_NAME, OLD_NAME = "KiviDB 1.2.0-rc10 (39bfacd2)", "KiviDB 1.2.0-rc8 (082dab3d)"
CMDS = ["LPUSH", "LPOP", "LRANGE_100", "HSET", "HGET", "HGETALL", "SADD", "SISMEMBER", "ZADD", "ZRANGE", "INCR"]

prior = {}
for r in csv.DictReader(open(PRIOR)):
    prior[(r["command"], r["value_bytes"], r["pipeline"])] = max(float(r["new_spread_pct"]), float(r["old_spread_pct"]))

runs = [r for r in csv.DictReader(open(os.path.join(RES, "runs.csv"))) if r["engine"] in (NEW, OLD)]
G = {}
for r in runs:
    G.setdefault((r["engine"], r["command"], r["value_bytes"], r["pipeline"]), []).append(r)


def stats(rs):
    ops = [float(r["ops_per_sec"]) for r in rs]
    med = statistics.median(ops)
    return {
        "n": len(ops), "med": med, "min": min(ops), "max": max(ops),
        "spread": (max(ops) - min(ops)) / med * 100 if med else 0.0,
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
            pr = prior.get((c, vv, p), 0.0)
            noise = max(pr, A["spread"], B["spread"])
            verdict = "regression" if delta < -noise else ("faster" if delta > noise else "same")
            rows.append({
                "command": c, "value_bytes": vv, "pipeline": p,
                "new_median": round(A["med"]), "new_min": round(A["min"]), "new_max": round(A["max"]),
                "new_spread_pct": round(A["spread"], 1), "new_runs": A["n"],
                "old_median": round(B["med"]), "old_min": round(B["min"]), "old_max": round(B["max"]),
                "old_spread_pct": round(B["spread"], 1), "old_runs": B["n"],
                "rc8_round_spread_pct": round(pr, 1), "threshold_pct": round(noise, 1),
                "delta_pct": round(delta, 1),
                "new_p50_ms": A["p50"], "new_p99_ms": A["p99"], "new_p999_ms": A["p999"],
                "old_p50_ms": B["p50"], "old_p99_ms": B["p99"], "old_p999_ms": B["p999"],
                "verdict": verdict,
            })

with open(os.path.join(RES, "vs-rc8.csv"), "w", newline="\n") as fh:
    w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()), lineterminator="\n")
    w.writeheader()
    w.writerows(rows)

print(f"| values, pipeline | command | {NEW_NAME}: median (min-max) | {OLD_NAME}: median (min-max) | change | "
      "spread today rc10 / rc8 | spread, rc8 round | p99 rc10 / rc8 (ms) | p99.9 rc10 / rc8 (ms) | verdict |")
print("|---|---|---|---|---|---|---|---|---|---|")
for r in rows:
    vb = "100 B" if r["value_bytes"] in ("100", "") else "2 KB"
    print(f"| {vb}, p{r['pipeline']} | {r['command']} | {fmt(r['new_median'])} ({fmt(r['new_min'])}-{fmt(r['new_max'])}) | "
          f"{fmt(r['old_median'])} ({fmt(r['old_min'])}-{fmt(r['old_max'])}) | {r['delta_pct']:+.1f}% | "
          f"{r['new_spread_pct']}% / {r['old_spread_pct']}% | {r['rc8_round_spread_pct']}% | "
          f"{r['new_p99_ms']:.2f} / {r['old_p99_ms']:.2f} | {r['new_p999_ms']:.2f} / {r['old_p999_ms']:.2f} | "
          f"{'**regression**' if r['verdict'] == 'regression' else r['verdict']} |")

# chart: both builds as % of rc8's median, whiskers min-max of all runs, orange tick = 100 - threshold
os.makedirs(OUT, exist_ok=True)
fig, axes = plt.subplots(2, 2, figsize=(13, 7.6), squeeze=False)
for k, (v, p) in enumerate([("100", "1"), ("100", "16"), ("2048", "1"), ("2048", "16")]):
    ax = axes[k // 2][k % 2]
    sel = [r for r in rows if r["pipeline"] == p and (r["value_bytes"] == v or (r["value_bytes"] == "" and v == "100"))]
    xs = range(len(sel))
    for off, who, col in [(-0.19, "old", "#9E9E9E"), (0.19, "new", "#0072B2")]:
        base = [r["old_median"] for r in sel]
        h = [r[f"{who}_median"] / b * 100 for r, b in zip(sel, base)]
        lo = [hh - r[f"{who}_min"] / b * 100 for r, b, hh in zip(sel, base, h)]
        hi = [r[f"{who}_max"] / b * 100 - hh for r, b, hh in zip(sel, base, h)]
        ax.bar([x + off for x in xs], h, width=0.36, color=col, label=OLD_NAME if who == "old" else NEW_NAME)
        ax.errorbar([x + off for x in xs], h, yerr=[lo, hi], fmt="none", ecolor="#222222", elinewidth=0.7, capsize=2)
    ax.hlines([100 - r["threshold_pct"] for r in sel], [x - 0.42 for x in xs], [x + 0.42 for x in xs],
              colors="#D55E00", linewidth=1.2, label="regression line (100% minus the spread)" if k == 0 else None)
    for x, r in zip(xs, sel):
        if r["verdict"] != "same":
            ax.text(x, 4, "slower" if r["verdict"] == "regression" else "faster", ha="center", fontsize=7,
                    color="#B00020" if r["verdict"] == "regression" else "#1b5e20", rotation=90)
    ax.axhline(100, color="#555555", linewidth=0.8)
    ax.set_xticks(list(xs))
    ax.set_xticklabels([r["command"] for r in sel], rotation=40, ha="right", fontsize=8)
    ax.set_ylim(0, 135)
    ax.set_ylabel("% of rc8's median ops/s")
    ax.set_title(f"{'100 B' if v == '100' else '2 KB'} values, pipeline {p}", loc="left", fontsize=10)
h, lab = axes[0][0].get_legend_handles_labels()
fig.legend(h, lab, loc="lower center", ncol=3, frameon=False)
nreg = sum(r["verdict"] == "regression" for r in rows)
fig.suptitle(f"rc10 (39bfacd2) against rc8 (082dab3d): {nreg} of {len(rows)} rows slower by more than "
             "the run-to-run spread\n(3 runs each, one round each, alternating; bars = median, whiskers = min-max; "
             "orange tick = 100% minus the larger of the rc8 round's and today's spreads)",
             x=0.01, ha="left", fontsize=11)
fig.tight_layout(rect=(0, 0.05, 1, 0.91))
fig.savefig(os.path.join(OUT, "vs-rc8.svg"))
plt.close(fig)
