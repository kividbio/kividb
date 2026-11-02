"""Charts and tables from parse.py's summary JSON.

Usage: python charts.py <summary.json> <charts dir> <tables.md>
Search numbers come from the full pass (p1); ingest time and memory use every
pass (median, with min-max whiskers). One idea per chart, SVG, white
background, the suite-wide engine colors.
"""
import json, os, statistics, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

plt.rcParams.update({
    "svg.fonttype": "none", "font.family": "DejaVu Sans", "font.size": 10,
    "axes.facecolor": "white", "figure.facecolor": "white", "savefig.facecolor": "white",
    "axes.edgecolor": "#444444", "axes.labelcolor": "#222222", "text.color": "#222222",
    "xtick.color": "#333333", "ytick.color": "#333333",
    "axes.grid": True, "grid.color": "#e3e3e3", "grid.linewidth": 0.8, "axes.axisbelow": True,
    "axes.spines.top": False, "axes.spines.right": False,
})

ENGINES = {
    # label: (legend name, color, linestyle, marker, hatch)
    "kividb-rc4": ("KiviDB 1.2.0-rc4", "#0072B2", "-", "o", None),
    "kividb-master": ("KiviDB master (dry run)", "#0072B2", "-", "o", None),
    "kividb-rc3": ("KiviDB 1.2.0-rc3", "#56B4E9", "-", "s", None),
    "redis": ("Redis 8.10.2 (workflow flags)", "#D55E00", "-", "^", None),
    "redis-mt": ("Redis 8.10.2 + io-threads 8", "#D55E00", "--", "v", "///"),
    "dragonfly": ("Dragonfly 2.0.2", "#CC79A7", "-", "D", None),
}
ORDER = ["kividb-rc4", "kividb-master", "kividb-rc3", "redis", "redis-mt", "dragonfly"]
DS = {
    "h-and-m-2048-angular-filters": "h-and-m 2048-d, filtered (105K)",
    "dbpedia-openai-100K-1536-angular": "dbpedia 1536-d (100K)",
    "glove-25-angular": "glove 25-d (1.18M)",
    "dbpedia-openai-1M-1536-angular": "dbpedia 1536-d (1M, text-embedding-3-large)",
}
# Takeaway lines for the summary charts, set from the final numbers.
TITLES = {
    "ingest": "Dragonfly loads fastest; KiviDB loads glove-25 2x faster than Redis but 1536-d dbpedia 16-21% slower.\n",
    "bpv": "KiviDB needs 5-10% less memory per vector than Redis at 1536/2048-d, 13% more at 25-d; Dragonfly 36-38% less than KiviDB.\n",
    "after_delete": "dbpedia 1M: 45 s after deleting every vector, Redis and Dragonfly are back under 0.25 GiB; KiviDB still holds 10.6 GiB",
}

SHORT = {"h-and-m-2048-angular-filters": "h-and-m", "dbpedia-openai-100K-1536-angular": "dbpedia-100K",
         "glove-25-angular": "glove-25", "dbpedia-openai-1M-1536-angular": "dbpedia-1M"}


def engines_in(runs):
    present = {r["engine"] for r in runs}
    return [e for e in ORDER if e in present]


def legend_out(ax, **kw):
    ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0), frameon=False, **kw)


def save(fig, out, name):
    fig.savefig(os.path.join(out, name), format="svg", bbox_inches="tight")
    if os.environ.get("PNG_PREVIEW"):  # local preview only, not published
        fig.savefig(os.path.join(os.environ["PNG_PREVIEW"], name[:-4] + ".png"), dpi=80, bbox_inches="tight")
    plt.close(fig)


def full_runs(runs):
    return [r for r in runs if r["searches"]]


def pick(runs, e, ds):
    return [r for r in runs if r["engine"] == e and r["dataset"] == ds]


def lead(vals):
    """takeaway helper: (engine label, value) with the max value."""
    vals = [(k, v) for k, v in vals if v]
    return max(vals, key=lambda kv: kv[1]) if vals else (None, None)


def interp(curve, t):
    """q/s at recall t on a (recall, q/s) curve, linear between measured points."""
    for (r1, q1), (r2, q2) in zip(curve, curve[1:]):
        if r1 <= t <= r2:
            return q1 + (q2 - q1) * (t - r1) / (r2 - r1) if r2 > r1 else max(q1, q2)
    return None


def chart_qps_recall(runs, out, ds):
    fr = full_runs(runs)
    fig, ax = plt.subplots(figsize=(7.2, 4.2))
    best = []
    curves = {}
    par = 100 if ds == "glove-25-angular" else 1
    for e in engines_in(fr):
        rr = pick(fr, e, ds)
        if not rr:
            continue
        pts = sorted([(s["recall"], s["median_rps"], s["ef"]) for s in rr[0]["searches"]
                      if s["parallel"] == par and s["recall"] is not None], key=lambda p: p[2])
        if not pts:
            continue
        name, c, ls, mk, _ = ENGINES[e]
        ax.plot([p[0] for p in pts], [p[1] for p in pts], ls=ls, color=c, marker=mk, ms=7, lw=2.6 if e == "kividb-rc4" else 1.8, label=name, zorder=5 if e == "kividb-rc4" else 2)
        for p in pts:
            if e in ("kividb-rc4", "kividb-master"):
                ax.annotate(f"ef {p[2]}", (p[0], p[1]), textcoords="offset points", xytext=(6, 4), fontsize=8, color="#555555")
        best.append((name, max(p[1] for p in pts)))
        curves[e] = sorted((p[0], p[1]) for p in pts)
    w, v = lead(best)
    sub = f"highest q/s: {w} ({v:,.0f} q/s)"
    # Where the main engines' recall ranges overlap, compare them at one recall.
    main = [e for e in ("kividb-rc4", "redis", "dragonfly") if e in curves]
    if len(main) == 3:
        lo = max(min(p[0] for p in curves[e]) for e in main)
        hi = min(max(p[0] for p in curves[e]) for e in main)
        if hi - lo > 0.002:
            t = round((lo + hi) / 2, 3)
            vals = [(ENGINES[e][0].split(" (")[0], interp(curves[e], t)) for e in main]
            sub = f"at recall {t}: " + ", ".join(f"{n} {q:,.0f} q/s" for n, q in vals)
    ax.set_title(f"{DS[ds]}: queries/s vs recall@10 at {par} client{'s' if par > 1 else ''}\n" + sub,
                 fontsize=11, loc="left")
    ax.set_xlabel("recall@10 (higher is better)")
    ax.set_ylabel("queries per second")
    ax.set_ylim(bottom=0)
    legend_out(ax)
    save(fig, out, f"qps-vs-recall-{SHORT[ds]}.svg")


def chart_qps_parallel(runs, out, ds):
    fr = full_runs(runs)
    pars = [1, 16, 100]
    eng = [e for e in engines_in(fr) if pick(fr, e, ds)]
    if not eng:
        return
    fig, axes = plt.subplots(1, 3, figsize=(10, 3.8))
    for ax, par in zip(axes, pars):
        for i, e in enumerate(eng):
            s = [x for x in pick(fr, e, ds)[0]["searches"] if x["ef"] == 128 and x["parallel"] == par]
            if not s:
                continue
            s = s[0]
            name, c, _, _, hatch = ENGINES[e]
            lo, hi = min(s["reps"]), max(s["reps"])
            ax.bar(i, s["median_rps"], color=c if not hatch else "white", edgecolor=c, hatch=hatch, lw=1.5,
                   label=name, yerr=[[s["median_rps"] - lo], [hi - s["median_rps"]]], capsize=3,
                   error_kw={"ecolor": "#333333", "lw": 1})
            ax.text(i, hi, f"{s['median_rps']:,.0f}", ha="center", va="bottom", fontsize=7.5,
                    rotation=90 if len(eng) > 4 else 0, color="#222222")
        ax.set_title(f"{par} client{'s' if par > 1 else ''}", fontsize=10)
        ax.set_xticks([])
        ax.set_ylim(bottom=0)
        ax.margins(y=0.25)
    axes[0].set_ylabel("queries per second (ef 128)")
    h, l = axes[0].get_legend_handles_labels()
    fig.legend(h, l, loc="upper left", bbox_to_anchor=(0.99, 0.9), frameon=False)
    fig.suptitle(f"{DS[ds]}: queries/s by client count, ef 128 (median of 3 repetitions, whiskers min-max)",
                 x=0.02, ha="left", fontsize=11)
    fig.tight_layout()
    save(fig, out, f"qps-by-parallel-{SHORT[ds]}.svg")


def agg(runs, e, ds, key):
    v = [r[key] for r in pick(runs, e, ds) if r.get(key) is not None]
    if not v:
        return None
    return statistics.median(v), min(v), max(v), len(v)


def chart_ingest(runs, out):
    dss = [d for d in DS if any(r["dataset"] == d for r in runs)]
    eng = engines_in(runs)
    fig, axes = plt.subplots(1, len(dss), figsize=(3.1 * len(dss) + 1.5, 3.9))
    if len(dss) == 1:
        axes = [axes]
    for ax, ds in zip(axes, dss):
        for i, e in enumerate(eng):
            a = agg(runs, e, ds, "ingest_s")
            if not a:
                continue
            name, c, _, _, hatch = ENGINES[e]
            ax.bar(i, a[0], color=c if not hatch else "white", edgecolor=c, hatch=hatch, lw=1.5, label=name,
                   yerr=[[a[0] - a[1]], [a[2] - a[0]]], capsize=3, error_kw={"ecolor": "#333333", "lw": 1})
            ax.text(i, a[2], f"{a[0]:.1f}" if a[0] < 100 else f"{a[0]:.0f}", ha="center", va="bottom", fontsize=8)
        ax.set_title(SHORT[ds], fontsize=10)
        ax.set_xticks([])
        ax.set_ylim(bottom=0)
        ax.margins(y=0.15)
    axes[0].set_ylabel("seconds to load + index (lower is better)")
    h, l = axes[0].get_legend_handles_labels()
    fig.legend(h, l, loc="upper left", bbox_to_anchor=(0.99, 0.9), frameon=False)
    fig.suptitle(TITLES.get("ingest", "") + "Ingest: seconds to upload every vector and finish indexing (median of 3 loads, whiskers min-max)",
                 x=0.02, ha="left", fontsize=11)
    fig.tight_layout()
    save(fig, out, "ingest-time.svg")


def first_pass(runs):
    ps = sorted({r["pass"] for r in runs})
    return "p1" if "p1" in ps else ps[0]


def chart_mem_time(runs, out, ds, pas=None):
    pas = pas or first_pass(runs)
    eng = [e for e in engines_in(runs) if any(r["pass"] == pas for r in pick(runs, e, ds))]
    if not eng:
        return
    fig, ax = plt.subplots(figsize=(8, 4.3))
    peaks = []
    for e in eng:
        r = [x for x in pick(runs, e, ds) if x["pass"] == pas][0]
        if not r.get("mem_series") or r.get("load_start_rel") is None:
            continue
        name, c, ls, mk, _ = ENGINES[e]
        t0 = r["load_start_rel"]
        end = r["load_end_rel"] - t0
        stop = (r["cleanup_rel"] - t0) if r.get("cleanup_rel") else end * 1.25 + 10
        stop = min(stop, end * 1.25 + 10)  # until the tool deletes the data
        ser = [m for m in r["mem_series"] if m[1] and -5 <= m[0] - t0 <= stop]
        xs = [m[0] - t0 for m in ser]
        ys = [m[1] / 2**30 for m in ser]
        ax.plot(xs, ys, color=c, ls=ls, lw=2.6 if e == "kividb-rc4" else 1.8, label=name, zorder=5 if e == "kividb-rc4" else 2)
        yend = [m[1] for m in ser if m[0] - t0 <= end]
        if yend:
            ax.plot([end], [yend[-1] / 2**30], marker=mk, ms=9, color=c, mec="white", mew=1.5, ls="none")
            if e in ("kividb-rc4", "kividb-master"):
                ax.annotate("load end", (end, yend[-1] / 2**30), textcoords="offset points", xytext=(5, -12),
                            fontsize=7.5, color="#555555")
        peaks.append((name, max(ys)))
    ax.axvline(0, color="#777777", lw=1, ls=":")
    ax.text(0, ax.get_ylim()[1] * 0.97, " load start", fontsize=8, color="#555555", va="top")
    if not peaks:
        plt.close(fig)
        return
    lo = min(peaks, key=lambda p: p[1])
    ax.set_title(f"{DS[ds]}: server RSS while loading (sampled every 0.5 s)\n"
                 f"lowest peak: {lo[0]} ({lo[1]:.2f} GiB); marker = that engine's load end",
                 fontsize=11, loc="left")
    ax.set_xlabel("seconds since the client started uploading")
    ax.set_ylabel("resident memory (GiB)")
    ax.set_ylim(bottom=0)
    legend_out(ax)
    save(fig, out, f"memory-over-time-{SHORT[ds]}.svg")


def chart_mem_vs_vectors(runs, out, ds, pas=None):
    pas = pas or first_pass(runs)
    eng = [e for e in engines_in(runs) if any(r["pass"] == pas for r in pick(runs, e, ds))]
    if not eng:
        return
    fig, ax = plt.subplots(figsize=(8, 4.3))
    for e in eng:
        r = [x for x in pick(runs, e, ds) if x["pass"] == pas][0]
        if not r.get("mem_series"):
            continue
        name, c, ls, _, _ = ENGINES[e]
        end = r["load_end_rel"]
        ser = [m for m in r["mem_series"] if m[1] and m[3] is not None and m[0] <= end + 0.6]
        # keep the rising part only (the tool deletes the data after the run)
        pts, kmax = [], -1
        for m in ser:
            if m[3] >= kmax:
                kmax = m[3]
                pts.append(m)
        ax.plot([m[3] / 1e3 for m in pts], [m[1] / 2**30 for m in pts], color=c, ls=ls, lw=2.6 if e == "kividb-rc4" else 1.8, label=name, zorder=5 if e == "kividb-rc4" else 2)
    ax.set_title(f"{DS[ds]}: memory as the dataset grows\n"
                 "server RSS against vectors stored so far, sampled during the load", fontsize=11, loc="left")
    ax.set_xlabel("vectors stored (thousands)")
    ax.set_ylabel("resident memory (GiB)")
    ax.set_xlim(left=0)
    ax.set_ylim(bottom=0)
    legend_out(ax)
    save(fig, out, f"memory-vs-vectors-{SHORT[ds]}.svg")


def chart_after_delete(runs, out, ds, pas="p3"):
    """RSS after the tool deleted the index and every document (pass 3 lingers 45 s)."""
    eng = [e for e in engines_in(runs) if any(r["pass"] == pas and r.get("cleanup_rel") for r in pick(runs, e, ds))]
    if not eng:
        return
    fig, ax = plt.subplots(figsize=(8, 4.3))
    for e in eng:
        r = [x for x in pick(runs, e, ds) if x["pass"] == pas][0]
        name, c, ls, _, _ = ENGINES[e]
        c0 = r["cleanup_rel"]
        ser = [m for m in r["mem_series"] if m[1] and m[0] >= c0 - 5]
        ax.plot([m[0] - c0 for m in ser], [m[1] / 2**30 for m in ser], color=c, ls=ls,
                lw=2.6 if e == "kividb-rc4" else 1.8, label=name, zorder=5 if e == "kividb-rc4" else 2)
    ax.axvline(0, color="#777777", lw=1, ls=":")
    ax.text(0, ax.get_ylim()[1] * 0.97, " tool deletes index + documents", fontsize=8, color="#555555", va="top")
    ax.set_title(TITLES.get("after_delete", f"{DS[ds]}: memory given back after deleting everything") +
                 "\nserver RSS from the moment the client drops the index and deletes every document", fontsize=11, loc="left")
    ax.set_xlabel("seconds since the delete started")
    ax.set_ylabel("resident memory (GiB)")
    ax.set_ylim(bottom=0)
    legend_out(ax)
    save(fig, out, f"memory-after-delete-{SHORT[ds]}.svg")


def chart_bytes_per_vector(runs, out):
    dss = [d for d in DS if any(r["dataset"] == d for r in runs)]
    eng = engines_in(runs)
    fig, axes = plt.subplots(1, len(dss), figsize=(3.1 * len(dss) + 1.5, 3.9))
    if len(dss) == 1:
        axes = [axes]
    for ax, ds in zip(axes, dss):
        dim = {"h-and-m-2048-angular-filters": 2048, "glove-25-angular": 25}.get(ds, 1536)
        for i, e in enumerate(eng):
            v = [(r["post_rss"] - r["idle_rss"]) / r["post_keys"] for r in pick(runs, e, ds)
                 if r.get("post_rss") and r.get("post_keys")]
            if not v:
                continue
            m = statistics.median(v)
            name, c, _, _, hatch = ENGINES[e]
            ax.bar(i, m, color=c if not hatch else "white", edgecolor=c, hatch=hatch, lw=1.5, label=name)
            ax.text(i, m, f"{m:,.0f}", ha="center", va="bottom", fontsize=7.5)
        ax.axhline(dim * 4, color="#555555", lw=1, ls=":")
        ax.text(-0.45, dim * 4, f"one float32 copy: {dim * 4:,} B", fontsize=7, ha="left", va="bottom",
                color="#333333", bbox={"facecolor": "white", "edgecolor": "none", "pad": 1}, zorder=6)
        ax.set_title(SHORT[ds], fontsize=10)
        ax.set_xticks([])
        ax.set_ylim(bottom=0)
        ax.margins(y=0.15)
    axes[0].set_ylabel("bytes of RSS per stored vector")
    h, l = axes[0].get_legend_handles_labels()
    fig.legend(h, l, loc="upper left", bbox_to_anchor=(0.99, 0.9), frameon=False)
    fig.suptitle(TITLES.get("bpv", "") + "Memory per vector after loading: (RSS after load - idle RSS) / vectors, median of 3 loads",
                 x=0.02, ha="left", fontsize=11)
    fig.tight_layout()
    save(fig, out, "memory-per-vector.svg")


def tables(runs, path):
    fr = full_runs(runs)
    L = []
    for ds in DS:
        if not any(r["dataset"] == ds for r in runs):
            continue
        L.append(f"\n### {DS[ds]}\n")
        L.append("| engine | ef | clients | q/s median (3 reps) | spread | recall@10 | p50 ms | p95 ms | p99 ms | client-bound? |")
        L.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---|")
        for e in engines_in(fr):
            for r in pick(fr, e, ds)[:1]:
                for s in r["searches"]:
                    f = lambda x, d=2: "-" if x is None else f"{x:,.{d}f}"
                    L.append(f"| {ENGINES[e][0]} | {s['ef']} | {s['parallel']} | {f(s['median_rps'], 0)} | "
                             f"{f(s['spread_pct'], 1)}% | {f(s['recall'], 4)} | {f(s['p50_ms'])} | {f(s['p95_ms'])} | "
                             f"{f(s['p99_ms'])} | {'yes' if s.get('client_saturated') else 'no'} |")
        L.append("")
        L.append("| engine | loads | upload s (median, min-max) | index wait s | upload+index s | peak RSS GiB | RSS after load GiB | used_memory after load GiB | RSS B/vector | used_memory B/vector |")
        L.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
        for e in engines_in(runs):
            rr = [r for r in pick(runs, e, ds) if r.get("upload_s")]
            if not rr:
                continue
            md = lambda k: statistics.median([r[k] for r in rr if r.get(k) is not None]) if any(r.get(k) is not None for r in rr) else None
            up = [r["upload_s"] for r in rr]
            bpv = statistics.median([(r["post_rss"] - r["idle_rss"]) / r["post_keys"] for r in rr if r.get("post_keys")])
            upv = statistics.median([(r["post_used"] - r["idle_used"]) / r["post_keys"] for r in rr if r.get("post_keys") and r.get("post_used")])
            L.append(f"| {ENGINES[e][0]} | {len(rr)} | {statistics.median(up):.1f} ({min(up):.1f}-{max(up):.1f}) | "
                     f"{md('index_wait_s'):.2f} | {md('ingest_s'):.1f} | {md('peak_rss') / 2**30:.2f} | "
                     f"{md('post_rss') / 2**30:.2f} | {md('post_used') / 2**30:.2f} | {bpv:,.0f} | {upv:,.0f} |")
    # rc4 against rc3, measured on the same machines in this run
    if any(r["engine"] == "kividb-rc4" for r in fr):
        L.append("\n### KiviDB 1.2.0-rc4 against 1.2.0-rc3 (same VMs, same run)\n")
        L.append("| dataset | ef | clients | rc3 q/s | rc4 q/s | rc4/rc3 | rc3 recall | rc4 recall | rc3 p99 ms | rc4 p99 ms |")
        L.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
        for ds in DS:
            a = pick(fr, "kividb-rc3", ds)
            b = pick(fr, "kividb-rc4", ds)
            if not a or not b:
                continue
            for sa in a[0]["searches"]:
                sb = [x for x in b[0]["searches"] if x["ef"] == sa["ef"] and x["parallel"] == sa["parallel"]]
                if not sb:
                    continue
                sb = sb[0]
                L.append(f"| {SHORT[ds]} | {sa['ef']} | {sa['parallel']} | {sa['median_rps']:,.0f} | {sb['median_rps']:,.0f} | "
                         f"{sb['median_rps'] / sa['median_rps']:.2f} | {sa['recall']:.4f} | {sb['recall']:.4f} | "
                         f"{sa['p99_ms']:.2f} | {sb['p99_ms']:.2f} |")
        L.append("")
        L.append("| dataset | rc3 upload+index s (median) | rc4 | rc4/rc3 | rc3 RSS B/vector | rc4 RSS B/vector |")
        L.append("|---|---:|---:|---:|---:|---:|")
        for ds in DS:
            a = [r for r in pick(runs, "kividb-rc3", ds) if r.get("ingest_s")]
            b = [r for r in pick(runs, "kividb-rc4", ds) if r.get("ingest_s")]
            if not a or not b:
                continue
            ta = statistics.median(r["ingest_s"] for r in a)
            tb = statistics.median(r["ingest_s"] for r in b)
            ba = statistics.median((r["post_rss"] - r["idle_rss"]) / r["post_keys"] for r in a if r.get("post_keys"))
            bb = statistics.median((r["post_rss"] - r["idle_rss"]) / r["post_keys"] for r in b if r.get("post_keys"))
            L.append(f"| {SHORT[ds]} | {ta:.1f} | {tb:.1f} | {tb / ta:.2f} | {ba:,.0f} | {bb:,.0f} |")
    open(path, "w", newline="\n").write("\n".join(L) + "\n")


def main(summary, out, tables_md):
    runs = json.load(open(summary))
    os.makedirs(out, exist_ok=True)
    for ds in DS:
        if not any(r["dataset"] == ds for r in runs):
            continue
        chart_qps_recall(runs, out, ds)
        if ds != "glove-25-angular":
            chart_qps_parallel(runs, out, ds)
        chart_mem_time(runs, out, ds)
        chart_mem_vs_vectors(runs, out, ds)
    chart_after_delete(runs, out, "dbpedia-openai-1M-1536-angular")
    chart_ingest(runs, out)
    chart_bytes_per_vector(runs, out)
    tables(runs, tables_md)


if __name__ == "__main__":
    main(*sys.argv[1:4])
