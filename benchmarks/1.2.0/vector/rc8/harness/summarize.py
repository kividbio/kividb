"""rc8 round: tables and the regression chart from parse.py's summary JSON.

Usage: python summarize.py <summary.json> <tables.md> <chart.svg>
A pass's q/s for a point is the median of its three repetitions; the tables
give the median over passes, the min-max, the pass-to-pass spread
((max-min)/median) and the verdict: regression when rc8 is slower than rc7 by
more than the noise: the larger of the two builds' pass-to-pass spreads (the
published suite's rule) and, since there are only two passes here, the largest
spread of three repetitions inside one pass.
"""
import json, statistics, sys
import matplotlib
import matplotlib.ticker
matplotlib.use("Agg")
import matplotlib.pyplot as plt

plt.rcParams.update({
    "svg.fonttype": "none", "font.family": "DejaVu Sans", "font.size": 10,
    "axes.facecolor": "white", "figure.facecolor": "white", "savefig.facecolor": "white",
    "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.color": "#e3e3e3", "axes.axisbelow": True,
})

RC8, RC7 = "kividb-rc8", "kividb-rc7"
DS = [("h-and-m-2048-angular-filters", "h-and-m"), ("dbpedia-openai-100K-1536-angular", "dbpedia-100K"),
      ("glove-25-angular", "glove-25"), ("dbpedia-openai-1M-1536-angular", "dbpedia-1M")]
# Published 75482abb (KiviDB 1.2.0) and competitor numbers from benchmarks/1.2.0/vector/README.md.
PUB_Q = {("h-and-m", 1): 252, ("h-and-m", 16): 3092, ("h-and-m", 100): 4478,
         ("dbpedia-100K", 1): 609, ("dbpedia-100K", 16): 7163, ("dbpedia-100K", 100): 10255,
         ("glove-25", 100): 33891,
         ("dbpedia-1M", 1): 341, ("dbpedia-1M", 16): 4509, ("dbpedia-1M", 100): 6255}
PUB_R = {"h-and-m": 0.9984, "dbpedia-100K": 0.9864, "glove-25": 0.9647, "dbpedia-1M": 0.9604}
PUB_ING = {"h-and-m": 14.9, "dbpedia-100K": 11.7, "glove-25": 30.2, "dbpedia-1M": 221.0}
PUB_BPV = {"h-and-m": 21936, "dbpedia-100K": 13265, "glove-25": 1126, "dbpedia-1M": 13202}
PUB_COMP = {("redis", "h-and-m"): (1802, 0.9986, 15.1, 22785), ("dragonfly", "h-and-m"): (811, 0.9981, 9.5, 14233),
            ("redis", "dbpedia-100K"): (8198, 0.9813, 10.1, 14451), ("dragonfly", "dbpedia-100K"): (9399, 0.9801, 9.2, 8281)}
PUB_KIVI_100 = {"h-and-m": 4478, "dbpedia-100K": 10255}


def med(xs):
    return statistics.median(xs) if xs else None


def spread(xs):
    return 100 * (max(xs) - min(xs)) / statistics.median(xs) if len(xs) > 1 else 0.0


def point(runs, eng, ds, ef, par):
    out = []
    for r in runs:
        if r["engine"] == eng and r["dataset"] == ds and r["pass"] in ("p1", "p2", "p3"):
            for s in r["searches"]:
                if s["ef"] == ef and s["parallel"] == par:
                    out.append((r["pass"], s))
    return sorted(out, key=lambda x: x[0])


def bpv(r):
    if r.get("post_rss") and r.get("idle_rss") and r.get("post_keys"):
        return (r["post_rss"] - r["idle_rss"]) / r["post_keys"]
    return None


def main(src, tables, chart):
    runs = json.load(open(src))
    L = []
    w = L.append
    rows = []
    w("## rc8 against rc7 (same VMs, same session)\n")
    w("| dataset | clients | rc7 q/s median (min-max) | rc8 q/s median (min-max) | rc8/rc7 | pass spread rc7 / rc8 | largest rep spread | verdict | recall rc7 / rc8 | p99 ms rc7 / rc8 |")
    w("|---|---:|---:|---:|---:|---:|---:|---|---:|---:|")
    for ds, short in DS:
        pars = [100] if short == "glove-25" else [1, 16, 100]
        for par in pars:
            a = point(runs, RC7, ds, 128, par)
            b = point(runs, RC8, ds, 128, par)
            if not a or not b:
                w(f"| {short} | {par} | missing | missing | | | | | | |")
                continue
            qa = [s["median_rps"] for _, s in a]
            qb = [s["median_rps"] for _, s in b]
            ma, mb = med(qa), med(qb)
            sa, sb = spread(qa), spread(qb)
            rep = max(s["spread_pct"] or 0 for _, s in a + b)
            ratio = mb / ma
            tol = max(sa, sb, rep)
            if ratio < 1 - tol / 100:
                v = "REGRESSION"
            elif ratio > 1 + tol / 100:
                v = "faster"
            else:
                v = "same"
            ra = med([s["recall"] for _, s in a]); rb = med([s["recall"] for _, s in b])
            pa = med([s["p99_ms"] for _, s in a if s["p99_ms"]]); pb = med([s["p99_ms"] for _, s in b if s["p99_ms"]])
            sat = any(s.get("client_saturated") for _, s in a + b)
            rows.append({"ds": short, "par": par, "rc7": qa, "rc8": qb, "ratio": ratio, "tol": tol, "verdict": v,
                         "rc8_med": mb, "rc8_recall": rb})
            w(f"| {short} | {par} | {ma:,.0f} ({min(qa):,.0f}-{max(qa):,.0f}) | {mb:,.0f} ({min(qb):,.0f}-{max(qb):,.0f}) | "
              f"{ratio:.3f} | {sa:.1f}% / {sb:.1f}% | {rep:.1f}% | {v}{' (client-bound!)' if sat else ''} | "
              f"{ra:.4f} / {rb:.4f} | {pa:.2f} / {pb:.2f} |")
    w("")
    w("### Ingest and memory (one load per pass)\n")
    w("| dataset | rc7 upload+index s (loads) | rc8 upload+index s (loads) | rc8/rc7 | rc7 RSS B/vector | rc8 RSS B/vector | rc8/rc7 | verdict |")
    w("|---|---:|---:|---:|---:|---:|---:|---|")
    ing = {}
    for ds, short in DS:
        ok = lambda p: p in ("p1", "p2", "p3") or p.startswith("xg")
        ra = [r for r in runs if r["engine"] == RC7 and r["dataset"] == ds and ok(r["pass"])]
        rb = [r for r in runs if r["engine"] == RC8 and r["dataset"] == ds and ok(r["pass"])]
        ra.sort(key=lambda r: r["pass"]); rb.sort(key=lambda r: r["pass"])
        ia = [r["ingest_s"] for r in ra if r.get("ingest_s")]; ib = [r["ingest_s"] for r in rb if r.get("ingest_s")]
        ra = [r for r in ra if r["pass"] in ("p1", "p2", "p3")]; rb = [r for r in rb if r["pass"] in ("p1", "p2", "p3")]
        ba = [bpv(r) for r in ra if bpv(r)]; bb = [bpv(r) for r in rb if bpv(r)]
        if not ia or not ib:
            continue
        ratio = med(ib) / med(ia)
        tol = max(spread(ia), spread(ib), 1.0)
        v = "REGRESSION?" if ratio > 1 + tol / 100 else ("faster" if ratio < 1 - tol / 100 else "same")
        ing[short] = (ia, ib, ba, bb)
        w(f"| {short} | {med(ia):.1f} ({', '.join(f'{x:.1f}' for x in ia)}) | {med(ib):.1f} ({', '.join(f'{x:.1f}' for x in ib)}) | "
          f"{ratio:.3f} | {med(ba):,.0f} | {med(bb):,.0f} | {med(bb)/med(ba):.3f} | {v} |")
    w("")
    w("## rc8 against the published 75482abb numbers (different session: indicative only)\n")
    w("| dataset | clients | published q/s | rc8 q/s | rc8/published | published recall | rc8 recall |")
    w("|---|---:|---:|---:|---:|---:|---:|")
    for r in rows:
        p = PUB_Q[(r["ds"], r["par"])]
        w(f"| {r['ds']} | {r['par']} | {p:,} | {r['rc8_med']:,.0f} | {r['rc8_med']/p:.3f} | {PUB_R[r['ds']]:.4f} | {r['rc8_recall']:.4f} |")
    w("")
    w("| dataset | published ingest s | rc8 ingest s | ratio | published RSS B/vector | rc8 RSS B/vector |")
    w("|---|---:|---:|---:|---:|---:|")
    for short, (ia, ib, ba, bb) in ing.items():
        w(f"| {short} | {PUB_ING[short]:.1f} | {med(ib):.1f} | {med(ib)/PUB_ING[short]:.3f} | {PUB_BPV[short]:,} | {med(bb):,.0f} |")
    w("")
    w("## Competitors (one pass, ef 128, 100 clients) against their published numbers\n")
    w("| dataset | engine | published q/s | q/s now | now/published | recall now | ingest s published / now | RSS B/vector published / now | rc8 q/s now / engine |")
    w("|---|---|---:|---:|---:|---:|---:|---:|---:|")
    for ds, short in DS[:2]:
        k8 = next((r["rc8_med"] for r in rows if r["ds"] == short and r["par"] == 100), None)
        for eng, name in (("redis", "Redis 8.10.2"), ("dragonfly", "Dragonfly 2.0.2")):
            rr = [r for r in runs if r["engine"] == eng and r["dataset"] == ds and r["pass"] == "comp"]
            if not rr:
                continue
            r = rr[0]
            s = next((s for s in r["searches"] if s["ef"] == 128 and s["parallel"] == 100), None)
            pq, prc, pi, pb = PUB_COMP[(eng, short)]
            q = s["median_rps"] if s else float("nan")
            w(f"| {short} | {name} | {pq:,} | {q:,.0f} | {q/pq:.3f} | {s['recall'] if s else float('nan'):.4f} | "
              f"{pi:.1f} / {r['ingest_s']:.1f} | {pb:,} / {bpv(r) or 0:,.0f} | {k8/q:.2f}x (published {PUB_KIVI_100[short]/pq:.2f}x) |")
    w("")
    pr = [r for r in runs if r["pass"] == "purge"]
    if pr:
        r = pr[0]
        ser = r["mem_series"]
        w("## Memory after the drop (rc8, dbpedia-1M, upload only)\n")
        w(f"- peak RSS {r['peak_rss']/2**30:.2f} GiB; RSS after load {r.get('post_rss', 0)/2**30:.2f} GiB")
        if r.get("cleanup_rel") is not None:
            c = r["cleanup_rel"]
            for lag in (5, 30, 55):
                near = [m for m in ser if m[0] >= c + lag and m[1]]
                if near:
                    m = near[0]
                    w(f"- {lag} s after the tool's delete started: RSS {m[1]/2**30:.2f} GiB, used_memory {(m[2] or 0)/2**20:.1f} MiB, keys {m[3]}")
        w(f"- last sample: RSS {ser[-1][1]/2**30:.2f} GiB, used_memory {(ser[-1][2] or 0)/2**20:.1f} MiB")
    open(tables, "w", newline="\n").write("\n".join(L) + "\n")
    print("\n".join(L))

    # Regression-check chart: q/s and ingest of each build, normalised to rc7's median.
    labels, r7, r8, e7, e8 = [], [], [], [], []
    for r in rows:
        base = med(r["rc7"])
        labels.append(f"{r['ds']}, {r['par']} client{'s' if r['par'] > 1 else ''}")
        r7.append(1.0); r8.append(med(r["rc8"]) / base)
        e7.append((1 - min(r["rc7"]) / base, max(r["rc7"]) / base - 1))
        e8.append((med(r["rc8"]) / base - min(r["rc8"]) / base, max(r["rc8"]) / base - med(r["rc8"]) / base))
    for short, (ia, ib, ba, bb) in ing.items():
        base = med(ia)  # ingest: speed = 1/time, so >1 is faster like q/s
        labels.append(f"{short}, ingest")
        r7.append(1.0); r8.append(base / med(ib))
        e7.append((1 - base / max(ia), base / min(ia) - 1))
        e8.append((base / med(ib) - base / max(ib), base / min(ib) - base / med(ib)))
    import numpy as np
    n = len(labels)
    y = np.arange(n)[::-1]
    fig, ax = plt.subplots(figsize=(8.5, 0.42 * n + 1.4))
    ax.errorbar(r7, y + 0.15, xerr=np.array(e7).T, fmt="s", color="#999999", ecolor="#999999",
                capsize=3, markersize=6, label="KiviDB 1.2.0-rc7 (v1.2.0-rc7)")
    ax.errorbar(r8, y - 0.15, xerr=np.array(e8).T, fmt="o", color="#0072B2", ecolor="#0072B2",
                capsize=3, markersize=6, label="KiviDB 1.2.0-rc8 (082dab3d)")
    ax.axvline(1.0, color="#444444", linewidth=0.8)
    ax.set_yticks(y); ax.set_yticklabels([l for l in labels])
    ax.set_xlim(0.9, 1.1)
    ax.set_xticks(np.arange(0.90, 1.1001, 0.02))
    ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:.2f}x"))
    ax.set_xlabel("relative to rc7's median (right is faster)")
    ax.grid(axis="y", visible=False)
    ax.set_title("Vector search, ef 128: rc8 against rc7 on the same VMs, same session\n"
                 "q/s per point and ingest speed (2-3 passes; glove-25 ingest 7 loads); whiskers: min-max",
                 fontsize=10, loc="left")
    ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0), frameon=False)
    fig.savefig(chart, format="svg", bbox_inches="tight")


if __name__ == "__main__":
    main(*sys.argv[1:4])
