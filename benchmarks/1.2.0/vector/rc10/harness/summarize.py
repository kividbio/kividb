"""rc10 round: tables and the regression chart from parse.py's summary JSON.

Usage: python summarize.py <summary.json> <tables.md> <chart.svg> <rc8 round's summary.json>

This round is one pass of each build (rc10 and rc8 alternating), so it has no
pass-to-pass spread of its own. A point's q/s is the median of its three
repetitions. The verdict tolerance is the noise the rc8 round measured on the
same harness, VM types and region, widened by this round's own repetition
spread: the larger of
  - the rc8 round's pass-to-pass spreads ((max-min)/median) of both its builds,
  - the rc8 round's largest spread of three repetitions inside one pass,
  - this round's largest spread of three repetitions (rc10's or rc8's).
"regression" when rc10 is slower than rc8 by more than that. The column
"within rc8-round pass spread" says whether the gap is also inside the
narrower published rule (the pass spreads alone).
Ingest: one load per build in the pass (dbpedia-1M: plus extra upload-only
loads, passes xg*); the medians of the loads are compared; tolerance is the
larger of the rc8 round's load-to-load spreads (both builds), this round's,
and 1%.
"""
import json, statistics, sys
import matplotlib
import matplotlib.ticker
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

plt.rcParams.update({
    "svg.fonttype": "none", "font.family": "DejaVu Sans", "font.size": 10,
    "axes.facecolor": "white", "figure.facecolor": "white", "savefig.facecolor": "white",
    "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.color": "#e3e3e3", "axes.axisbelow": True,
})

NEW, REF = "kividb-rc10", "kividb-rc8"
OLD_A, OLD_B = "kividb-rc7", "kividb-rc8"   # the two builds of the rc8 round
DS = [("h-and-m-2048-angular-filters", "h-and-m"), ("dbpedia-openai-100K-1536-angular", "dbpedia-100K"),
      ("glove-25-angular", "glove-25"), ("dbpedia-openai-1M-1536-angular", "dbpedia-1M")]


def med(xs):
    return statistics.median(xs) if xs else None


def spread(xs):
    return 100 * (max(xs) - min(xs)) / statistics.median(xs) if len(xs) > 1 else 0.0


def point(runs, eng, ds, ef, par, passes):
    out = []
    for r in runs:
        if r["engine"] == eng and r["dataset"] == ds and r["pass"] in passes:
            for s in r["searches"]:
                if s["ef"] == ef and s["parallel"] == par:
                    out.append((r["pass"], s))
    return sorted(out, key=lambda x: x[0])


def loads(runs, eng, ds, extra=True):
    ok = lambda p: p in ("p1", "p2", "p3") or (extra and p.startswith("xg"))
    rr = sorted([r for r in runs if r["engine"] == eng and r["dataset"] == ds and ok(r["pass"])], key=lambda r: r["pass"])
    return [r["ingest_s"] for r in rr if r.get("ingest_s")]


def bpv(r):
    if r.get("post_rss") and r.get("idle_rss") and r.get("post_keys"):
        return (r["post_rss"] - r["idle_rss"]) / r["post_keys"]
    return None


def main(src, tables, chart, old_src):
    runs = json.load(open(src))
    old = json.load(open(old_src))
    L = []
    w = L.append
    rows = []
    w("## rc10 against rc8 (same VMs, same session, one pass each)\n")
    w("| dataset | clients | first | rc8 q/s (reps min-max) | rc10 q/s (reps min-max) | rc10/rc8 | rep spread rc8 / rc10 | "
      "rc8-round noise (pass rc7 / rc8; reps) | tolerance | verdict | within rc8-round pass spread | recall rc8 / rc10 | p99 ms rc8 / rc10 |")
    w("|---|---:|---|---:|---:|---:|---:|---:|---:|---|---|---:|---:|")
    for ds, short in DS:
        pars = [100] if short == "glove-25" else [1, 16, 100]
        first = next((r["engine"] for r in sorted(runs, key=lambda r: r.get("mem_t0", 0))
                      if r["dataset"] == ds and r["engine"] in (NEW, REF)), "")
        for par in pars:
            a = point(runs, REF, ds, 128, par, ("p1",))
            b = point(runs, NEW, ds, 128, par, ("p1",))
            if not a or not b:
                w(f"| {short} | {par} | | missing | missing | | | | | | | | |")
                continue
            sa_, sb_ = a[0][1], b[0][1]
            qa, qb = sa_["median_rps"], sb_["median_rps"]
            ratio = qb / qa
            rep_now = max(sa_["spread_pct"] or 0, sb_["spread_pct"] or 0)
            oa = point(old, OLD_A, ds, 128, par, ("p1", "p2", "p3"))
            ob = point(old, OLD_B, ds, 128, par, ("p1", "p2", "p3"))
            ps_a = spread([s["median_rps"] for _, s in oa]); ps_b = spread([s["median_rps"] for _, s in ob])
            rep_old = max(s["spread_pct"] or 0 for _, s in oa + ob)
            tol = max(ps_a, ps_b, rep_old, rep_now)
            if ratio < 1 - tol / 100:
                v = "REGRESSION"
            elif ratio > 1 + tol / 100:
                v = "faster"
            else:
                v = "same"
            narrow = max(ps_a, ps_b)
            within = "yes" if abs(ratio - 1) <= narrow / 100 else ("no (slower)" if ratio < 1 else "no (faster)")
            sat = sa_.get("client_saturated") or sb_.get("client_saturated")
            old_b_med = med([s["median_rps"] for _, s in ob])
            old_b_rec = med([s["recall"] for _, s in ob])
            rows.append({"ds": short, "par": par, "ref": sa_, "new": sb_, "ratio": ratio, "tol": tol, "verdict": v,
                         "old_rc8": old_b_med, "old_rc8_recall": old_b_rec})
            w(f"| {short} | {par} | {first.replace('kividb-', '')} | {qa:,.0f} ({min(sa_['reps']):,.0f}-{max(sa_['reps']):,.0f}) | "
              f"{qb:,.0f} ({min(sb_['reps']):,.0f}-{max(sb_['reps']):,.0f}) | {ratio:.3f} | "
              f"{sa_['spread_pct'] or 0:.1f}% / {sb_['spread_pct'] or 0:.1f}% | {ps_a:.1f}% / {ps_b:.1f}%; {rep_old:.1f}% | "
              f"{tol:.1f}% | {v}{' (client-bound!)' if sat else ''} | {within} | "
              f"{sa_['recall']:.4f} / {sb_['recall']:.4f} | {sa_['p99_ms']:.2f} / {sb_['p99_ms']:.2f} |")
    w("")
    w("### Ingest and memory (one load each in the pass; dbpedia-1M also two extra load pairs)\n")
    w("| dataset | rc8 upload+index s, median (loads) | rc10 upload+index s, median (loads) | rc10/rc8 | "
      "rc8-round load spread rc7 / rc8 | load spread now rc8 / rc10 | tolerance | verdict | "
      "rc8 RSS B/vector | rc10 RSS B/vector | rc10/rc8 | peak RSS GiB rc8 / rc10 |")
    w("|---|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|")
    ing = {}
    for ds, short in DS:
        ra = [r for r in runs if r["engine"] == REF and r["dataset"] == ds and r["pass"] == "p1"]
        rb = [r for r in runs if r["engine"] == NEW and r["dataset"] == ds and r["pass"] == "p1"]
        if not ra or not rb or not ra[0].get("ingest_s") or not rb[0].get("ingest_s"):
            continue
        ra, rb = ra[0], rb[0]
        xa, xb = loads(runs, REF, ds), loads(runs, NEW, ds)   # p1 plus any xg loads
        ia, ib = med(xa), med(xb)
        la, lb = loads(old, OLD_A, ds), loads(old, OLD_B, ds)
        tol = max(spread(la), spread(lb), spread(xa), spread(xb), 1.0)
        ratio = ib / ia
        v = "REGRESSION?" if ratio > 1 + tol / 100 else ("faster" if ratio < 1 - tol / 100 else "same")
        ba, bb = bpv(ra), bpv(rb)
        ing[short] = (xa, xb, tol, ba, bb, med(lb), [bpv(r) for r in old if r["engine"] == OLD_B and r["dataset"] == ds
                                                     and r["pass"] in ("p1", "p2", "p3") and bpv(r)])
        fl = lambda xs: ", ".join(f"{x:.1f}" for x in xs)
        w(f"| {short} | {ia:.1f} ({fl(xa)}) | {ib:.1f} ({fl(xb)}) | {ratio:.3f} | {spread(la):.1f}% / {spread(lb):.1f}% | "
          f"{spread(xa):.1f}% / {spread(xb):.1f}% | {tol:.1f}% | {v} | "
          f"{ba:,.0f} | {bb:,.0f} | {bb/ba:.3f} | {ra['peak_rss']/2**30:.2f} / {rb['peak_rss']/2**30:.2f} |")
    w("")
    xg_ds = [(ds, short) for ds, short in DS if any(r["dataset"] == ds and r["pass"].startswith("xg") for r in runs)]
    if xg_ds:
        w("Loads in the order they ran:\n")
        w("| dataset | pass | first | s | second | s |")
        w("|---|---|---|---:|---|---:|")
        for ds, short in xg_ds:
            for ps in sorted({r["pass"] for r in runs if r["dataset"] == ds}):
                rr = sorted([r for r in runs if r["dataset"] == ds and r["pass"] == ps and r.get("ingest_s")],
                            key=lambda r: r.get("mem_t0", 0))
                if len(rr) == 2:
                    w(f"| {short} | {ps} | {rr[0]['engine'].replace('kividb-', '')} | {rr[0]['ingest_s']:.1f} | "
                      f"{rr[1]['engine'].replace('kividb-', '')} | {rr[1]['ingest_s']:.1f} |")
        w("")
    w("## rc10 against the rc8 round's rc8 numbers (different session: indicative only)\n")
    w("| dataset | clients | rc8 round: rc8 q/s | rc10 q/s now | ratio | rc8 now / rc8 round | rc8 round recall | rc10 recall |")
    w("|---|---:|---:|---:|---:|---:|---:|---:|")
    for r in rows:
        w(f"| {r['ds']} | {r['par']} | {r['old_rc8']:,.0f} | {r['new']['median_rps']:,.0f} | {r['new']['median_rps']/r['old_rc8']:.3f} | "
          f"{r['ref']['median_rps']/r['old_rc8']:.3f} | {r['old_rc8_recall']:.4f} | {r['new']['recall']:.4f} |")
    w("")
    w("| dataset | rc8 round: rc8 ingest s | rc10 ingest s now | ratio | rc8 round: rc8 RSS B/vector | rc10 RSS B/vector now |")
    w("|---|---:|---:|---:|---:|---:|")
    for short, (xa, xb, tol, ba, bb, olb, obpv) in ing.items():
        w(f"| {short} | {olb:.1f} | {med(xb):.1f} | {med(xb)/olb:.3f} | {med(obpv):,.0f} | {bb:,.0f} |")
    w("")
    w("### Latency (rc8 / rc10, ms, the tool's reported repetition)\n")
    w("| dataset | clients | p50 | p95 | p99 | p99.9 |")
    w("|---|---:|---:|---:|---:|---:|")
    for r in rows:
        f = lambda k: f"{r['ref'][k] or 0:.2f} / {r['new'][k] or 0:.2f}"
        w(f"| {r['ds']} | {r['par']} | {f('p50_ms')} | {f('p95_ms')} | {f('p99_ms')} | {f('p999_ms')} |")
    w("")
    cpu = [r.get("client_cpu_peak") for r in runs if r.get("client_cpu_peak") is not None]
    w(f"Client: busiest second {max(cpu):.0f}% of 16 cores; `client_saturated` set on "
      f"{sum(1 for r in runs for s in r['searches'] if s.get('client_saturated'))} search points.")
    open(tables, "w", newline="\n").write("\n".join(L) + "\n")
    print("\n".join(L))

    # Regression-check chart: rc10 relative to rc8 (same session), with the
    # verdict tolerance as a band and each point's repetition min-max as whiskers.
    labels, r8, r10, e8, e10, tols = [], [], [], [], [], []
    for r in rows:
        base = r["ref"]["median_rps"]
        labels.append(f"{r['ds']}, {r['par']} client{'s' if r['par'] > 1 else ''}")
        r8.append(1.0); r10.append(r["new"]["median_rps"] / base)
        e8.append((1 - min(r["ref"]["reps"]) / base, max(r["ref"]["reps"]) / base - 1))
        e10.append((r10[-1] - min(r["new"]["reps"]) / base, max(r["new"]["reps"]) / base - r10[-1]))
        tols.append(r["tol"])
    for short, (xa, xb, tol, ba, bb, olb, obpv) in ing.items():
        base = med(xa)  # speed = 1/time, so >1 is faster like q/s; whiskers: load min-max
        labels.append(f"{short}, ingest" + (f" ({len(xb)} loads)" if len(xb) > 1 else ""))
        r8.append(1.0); r10.append(base / med(xb))
        e8.append((1 - base / max(xa), base / min(xa) - 1))
        e10.append((r10[-1] - base / max(xb), base / min(xb) - r10[-1])); tols.append(tol)
    n = len(labels)
    y = np.arange(n)[::-1]
    fig, ax = plt.subplots(figsize=(8.5, 0.42 * n + 1.5))
    for yi, t in zip(y, tols):
        ax.fill_betweenx([yi - 0.38, yi + 0.38], 1 - t / 100, 1 + t / 100, color="#eeeeee", linewidth=0, zorder=0)
    ax.errorbar(r8, y + 0.15, xerr=np.array(e8).T, fmt="s", color="#999999", ecolor="#999999",
                capsize=3, markersize=6, label="KiviDB 1.2.0-rc8 (082dab3d)")
    ax.errorbar(r10, y - 0.15, xerr=np.array(e10).T, fmt="o", color="#0072B2", ecolor="#0072B2",
                capsize=3, markersize=6, label="KiviDB 1.2.0-rc10 (39bfacd2)")
    ax.fill_between([], [], color="#eeeeee", label="tolerance (rc8 round's noise)")
    ax.axvline(1.0, color="#444444", linewidth=0.8)
    ax.set_yticks(y); ax.set_yticklabels(labels)
    ax.set_xlim(0.9, 1.1)
    ax.set_xticks(np.arange(0.90, 1.1001, 0.02))
    ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:.2f}x"))
    ax.set_xlabel("relative to rc8 (right is faster)")
    ax.grid(axis="y", visible=False)
    ax.set_title("Vector search, ef 128: rc10 against rc8 on the same VMs, same session\n"
                 "q/s per point (median of 3 reps; whiskers: rep min-max), ingest speed (median of loads); band: tolerance",
                 fontsize=10, loc="left")
    ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0), frameon=False)
    fig.savefig(chart, format="svg", bbox_inches="tight")


if __name__ == "__main__":
    main(*sys.argv[1:5])
