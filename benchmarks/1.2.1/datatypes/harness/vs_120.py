"""vs_120.py RESULTS_DIR CHARTS_DIR PUBLISHED_SUMMARY_CSV RAW_DIR: 1.2.1 (878746e4) against 1.2.0 (release binary).

Adapted from the 1.2.0 suite's regression scripts. Reads RESULTS_DIR/runs.csv (summarize.py; two
alternating rounds of three runs per build: round 1 = 1.2.1 then 1.2.0, round 2 = 1.2.0 then 1.2.1)
and the 1.2.0 suite's published summary.csv, whose 1.2.0 rows (engine kividb-cand, 9 runs) give the
run-to-run spread that suite measured. For every row: each build's median ops/s, min, max and spread
((max-min)/median) over its 6 runs, and p50/p99/p99.9 (medians over the runs). The threshold of a row
is the largest of today's two spreads and the published spread (field-TTL rows: today's only). A row
is a regression when 1.2.1's median is below 1.2.0's by more than the threshold, "faster" when above
it by more than that. "limit" marks rows where the client (memtier CPU >= 75% in any run) or the
network (the rows the 1.2.0 suite found NIC-capped) is the limit, not the server.
Also reads RAW_DIR/<run>/expiry.csv (INFO expired_subkeys after each run) for the fields that
expired during each run of the expiring rows.
Writes RESULTS_DIR/vs-120.csv, RESULTS_DIR/expiry.csv, prints Markdown tables, draws CHARTS_DIR/vs-120.svg.
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
RES, OUT, PUB, RAW = sys.argv[1:5]
NEW, OLD = "kividb-121", "kividb-120"
CMDS = ["LPUSH", "LPOP", "LRANGE_100", "HSET", "HGET", "HGETALL", "SADD", "SISMEMBER", "ZADD", "ZRANGE", "INCR"]
HFE = ["HGETTTL", "HSETEX", "HPEXPIRE", "HSETPEXP", "HSETEXPX50", "HEXPMIX"]
NAMES = {
    "HGETTTL": "HGET (fields with TTLs)", "HSETEX": "HSETEX PX 1h", "HPEXPIRE": "HPEXPIRE 1h",
    "HSETPEXP": "HSET + HPEXPIRE (1:1)", "HSETEXPX50": "HSETEX PX 50 (expiring)",
    "HEXPMIX": "HSETEX PX 50 + HGET (1:1, expiring)",
}
# rows the 1.2.0 suite found capped by the network (server egress, or the client's egress on 2 KB writes at p16)
NET = {("LRANGE_100", "100", "1"), ("LRANGE_100", "100", "16"), ("HGETALL", "100", "16"), ("ZRANGE", "100", "16")}
for p in ("1", "16"):
    for c in ("LRANGE_100", "HGET", "HGETALL", "ZRANGE", "HGETTTL"):
        NET.add((c, "2048", p))
for c in ("LPUSH", "LPOP", "HSET", "SADD", "SISMEMBER", "ZADD", "HSETEX", "HPEXPIRE", "HSETPEXP", "HSETEXPX50", "HEXPMIX"):
    NET.add((c, "2048", "16"))

pub = {}
for r in csv.DictReader(open(PUB)):
    if r["engine"] == "kividb-cand":
        pub[(r["command"], r["value_bytes"], r["pipeline"])] = float(r["spread_pct"])

runs = [r for r in csv.DictReader(open(os.path.join(RES, "runs.csv"))) if r["engine"] in (NEW, OLD)]
G = {}
for r in runs:
    G.setdefault((r["engine"], r["command"], r["value_bytes"], r["pipeline"]), []).append(r)


def stats(rs):
    ops = [float(r["ops_per_sec"]) for r in rs]
    med = statistics.median(ops)
    per_round = {}
    for r in rs:
        per_round.setdefault(int(r["round"]), []).append(float(r["ops_per_sec"]))
    return {
        "n": len(ops), "med": med, "min": min(ops), "max": max(ops),
        "spread": (max(ops) - min(ops)) / med * 100 if med else 0.0,
        "p50": statistics.median(float(r["p50_ms"]) for r in rs),
        "p99": statistics.median(float(r["p99_ms"]) for r in rs),
        "p999": statistics.median(float(r["p999_ms"]) for r in rs),
        "p999_min": min(float(r["p999_ms"]) for r in rs), "p999_max": max(float(r["p999_ms"]) for r in rs),
        "cli": max(float(r["client_cpu_pct"] or 0) for r in rs),
        "cores": statistics.median(float(r["server_cores"]) for r in rs if r["server_cores"] != ""),
        "rounds": {k: statistics.median(v) for k, v in per_round.items()},
    }


def fmt(x):
    return f"{x / 1e6:.2f}M" if x >= 1e6 else f"{x / 1e3:.0f}k"


rows = []
for v in ["100", "2048"]:
    for p in ["1", "16"]:
        for c in CMDS + HFE:
            vv = "" if c == "INCR" else v
            if c == "INCR" and v != "100":
                continue
            a, b = G.get((NEW, c, vv, p)), G.get((OLD, c, vv, p))
            if not a or not b:
                continue
            A, B = stats(a), stats(b)
            delta = (A["med"] / B["med"] - 1) * 100
            pr = pub.get((c, vv, p), 0.0)
            noise = max(pr, A["spread"], B["spread"])
            verdict = "regression" if delta < -noise else ("faster" if delta > noise else "same")
            limit = []
            if max(A["cli"], B["cli"]) >= 75:
                limit.append("client")
            if (c, v, p) in NET:
                limit.append("network")
            rnd = " ".join(f"{(A['rounds'][k] / B['rounds'][k] - 1) * 100:+.1f}" for k in sorted(A["rounds"]) if k in B["rounds"])
            rows.append({
                "group": "field-ttl" if c in HFE else "matrix",
                "command": c, "name": NAMES.get(c, c), "value_bytes": vv, "pipeline": p,
                "new_median": round(A["med"]), "new_min": round(A["min"]), "new_max": round(A["max"]),
                "new_spread_pct": round(A["spread"], 1), "new_runs": A["n"],
                "old_median": round(B["med"]), "old_min": round(B["min"]), "old_max": round(B["max"]),
                "old_spread_pct": round(B["spread"], 1), "old_runs": B["n"],
                "published_spread_pct": round(pr, 1), "threshold_pct": round(noise, 1),
                "delta_pct": round(delta, 1), "per_round_pct": rnd,
                "new_p50_ms": A["p50"], "new_p99_ms": A["p99"], "new_p999_ms": A["p999"],
                "old_p50_ms": B["p50"], "old_p99_ms": B["p99"], "old_p999_ms": B["p999"],
                "new_p999_range": f"{A['p999_min']:.2f}-{A['p999_max']:.2f}",
                "old_p999_range": f"{B['p999_min']:.2f}-{B['p999_max']:.2f}",
                "client_cpu_pct_max": round(max(A["cli"], B["cli"]), 1),
                "new_server_cores": round(A["cores"], 2), "old_server_cores": round(B["cores"], 2),
                "limit": "+".join(limit), "verdict": verdict,
            })

with open(os.path.join(RES, "vs-120.csv"), "w", newline="\n") as fh:
    w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()), lineterminator="\n")
    w.writeheader()
    w.writerows(rows)


def table(group):
    print("| values, pipeline | command | 1.2.1 (878746e4): median (min-max) | 1.2.0 (release): median (min-max) | change | "
          "per round | threshold | p50 1.2.1 / 1.2.0 | p99 1.2.1 / 1.2.0 | p99.9 1.2.1 / 1.2.0 | limit | verdict |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|")
    for r in rows:
        if r["group"] != group:
            continue
        vb = "100 B" if r["value_bytes"] in ("100", "") else "2 KB"
        print(f"| {vb}, p{r['pipeline']} | {r['name']} | {fmt(r['new_median'])} ({fmt(r['new_min'])}-{fmt(r['new_max'])}) | "
              f"{fmt(r['old_median'])} ({fmt(r['old_min'])}-{fmt(r['old_max'])}) | {r['delta_pct']:+.1f}% | {r['per_round_pct']} | "
              f"{r['threshold_pct']}% | {r['new_p50_ms']:.2f} / {r['old_p50_ms']:.2f} | {r['new_p99_ms']:.2f} / {r['old_p99_ms']:.2f} | "
              f"{r['new_p999_ms']:.2f} / {r['old_p999_ms']:.2f} | {r['limit'] or '-'} | "
              f"{'**regression**' if r['verdict'] == 'regression' else r['verdict']} |")


print("## matrix\n")
table("matrix")
print("\n## field-ttl\n")
table("field-ttl")

# fields expired during each measured run of the expiring rows
exp_rows = []
for folder in sorted(os.listdir(RAW)):
    path = os.path.join(RAW, folder, "expiry.csv")
    if not os.path.exists(path):
        continue
    prev = None
    for r in csv.DictReader(open(path)):
        val = int(r["expired_subkeys"]) if r["expired_subkeys"].strip() else None
        lab = r["label"]
        if prev is not None and val is not None and (lab.startswith("hsetexpx50") or lab.startswith("hexpmix")):
            exp_rows.append({"run": folder, "label": lab, "expired_fields": val - prev})
        prev = val if val is not None else prev
with open(os.path.join(RES, "expiry.csv"), "w", newline="\n") as fh:
    w = csv.DictWriter(fh, fieldnames=["run", "label", "expired_fields"], lineterminator="\n")
    w.writeheader()
    w.writerows(exp_rows)
print("\n## fields expired per measured run (median over runs)\n")
agg = {}
for e in exp_rows:
    if e["label"].startswith("warm"):
        continue
    eng = "1.2.1" if e["run"].startswith(NEW) else "1.2.0"
    key = (eng, e["label"].rsplit("-r", 1)[0])
    agg.setdefault(key, []).append(e["expired_fields"])
for (eng, lab), vals in sorted(agg.items()):
    print(f"| {eng} | {lab} | {statistics.median(vals):,.0f} | {min(vals):,}-{max(vals):,} |")

# chart: 1.2.1 as % of 1.2.0's median, whiskers min-max, orange tick = 100 - threshold
os.makedirs(OUT, exist_ok=True)
panels = [("100", "1"), ("100", "16"), ("2048", "1"), ("2048", "16")]
fig, axes = plt.subplots(4, 1, figsize=(13, 16), squeeze=False)
for k, (v, p) in enumerate(panels):
    ax = axes[k][0]
    sel = [r for r in rows if r["pipeline"] == p and (r["value_bytes"] == v or (r["value_bytes"] == "" and v == "100"))]
    xs = range(len(sel))
    for off, who, col, nm in [(-0.19, "old", "#9E9E9E", "1.2.0 (release binary)"), (0.19, "new", "#0072B2", "1.2.1 (878746e4)")]:
        base = [r["old_median"] for r in sel]
        h = [r[f"{who}_median"] / b * 100 for r, b in zip(sel, base)]
        lo = [hh - r[f"{who}_min"] / b * 100 for r, b, hh in zip(sel, base, h)]
        hi = [r[f"{who}_max"] / b * 100 - hh for r, b, hh in zip(sel, base, h)]
        ax.bar([x + off for x in xs], h, width=0.36, color=col, label=nm if k == 0 else None)
        ax.errorbar([x + off for x in xs], h, yerr=[lo, hi], fmt="none", ecolor="#222222", elinewidth=0.7, capsize=2)
    ax.hlines([100 - r["threshold_pct"] for r in sel], [x - 0.42 for x in xs], [x + 0.42 for x in xs],
              colors="#D55E00", linewidth=1.2, label="regression line (100% minus the spread)" if k == 0 else None)
    for x, r in zip(xs, sel):
        if r["verdict"] != "same":
            ax.text(x, 4, "slower" if r["verdict"] == "regression" else "faster", ha="center", fontsize=7,
                    color="#B00020" if r["verdict"] == "regression" else "#1b5e20", rotation=90)
    ax.axhline(100, color="#555555", linewidth=0.8)
    ax.axvline(len([r for r in sel if r["group"] == "matrix"]) - 0.5, color="#888888", linewidth=0.8, linestyle=":")
    ax.set_xticks(list(xs))
    ax.set_xticklabels([r["command"] for r in sel], rotation=35, ha="right", fontsize=8)
    ax.set_ylim(0, 140)
    ax.set_ylabel("% of 1.2.0's median ops/s")
    ax.set_title(f"{'100 B' if v == '100' else '2 KB'} values, pipeline {p} (right of the dotted line: field-TTL rows)",
                 loc="left", fontsize=10)
h, lab = axes[0][0].get_legend_handles_labels()
fig.legend(h, lab, loc="lower center", ncol=3, frameon=False)
nreg = sum(r["verdict"] == "regression" for r in rows)
fig.suptitle(f"1.2.1 (878746e4) against 1.2.0 (release binary): {nreg} of {len(rows)} rows slower by more than the "
             "run-to-run spread\n(6 runs each, two alternating rounds; bars = median, whiskers = min-max)",
             x=0.01, ha="left", fontsize=11)
fig.tight_layout(rect=(0, 0.03, 1, 0.95))
fig.savefig(os.path.join(OUT, "vs-120.svg"))
plt.close(fig)
