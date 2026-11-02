#!/usr/bin/env python3
"""Turn the raw outputs of the memory suite into tables (Markdown) and SVG charts.

    python3 analyze.py <raw-dir> <warefeats-dir> <charts-dir> > tables.md

<raw-dir>        copy of the client's /var/tmp/mem (sampler CSVs, events.csv, checkpoints.csv, memtier JSON)
<warefeats-dir>  copy of the client's /var/tmp/wf (warefeats runner JSON per variant)
"""
import csv, glob, json, os, statistics as st, sys
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

RAW, WF, CH = sys.argv[1], sys.argv[2], sys.argv[3]
TAG = os.environ.get("TAG", "")  # e.g. "-pair-b": suffix for the mixed charts of a second VM pair
PAIR = os.environ.get("TITLE_SUFFIX", "")  # e.g. " (pair B, E32ds_v6)": added to the mixed chart titles
os.makedirs(CH, exist_ok=True)
plt.rcParams.update({
    "svg.fonttype": "none", "font.family": "DejaVu Sans", "font.size": 10,
    "figure.facecolor": "white", "axes.facecolor": "white", "savefig.facecolor": "white",
    "text.color": "#222222", "axes.labelcolor": "#222222", "xtick.color": "#333333", "ytick.color": "#333333",
    "axes.edgecolor": "#888888", "axes.grid": True, "grid.color": "#e3e3e3", "grid.linewidth": 0.8,
    "axes.spines.top": False, "axes.spines.right": False, "lines.linewidth": 2, "axes.titleweight": "bold",
    "axes.titlesize": 11, "legend.frameon": False,
})
ENG = {  # key: (label, colour)
    "kividb-rc4": ("KiviDB 1.2.0-rc4", "#0072B2"),
    "kividb-1.1.3": ("KiviDB 1.1.3", "#56B4E9"),
    "redis": ("Redis 8.10.3", "#D55E00"),
    "valkey": ("Valkey 9.1.3", "#009E73"),
    "dragonfly": ("Dragonfly 2.0.2", "#CC79A7"),
}
ORDER = list(ENG)
MiB, GiB = 1024 ** 2, 1024 ** 3


def lab(e): return ENG[e][0]
def col(e): return ENG[e][1]


def save(fig, name):
    fig.savefig(os.path.join(CH, name), format="svg", bbox_inches="tight")
    if os.environ.get("PREVIEW"):
        fig.savefig(os.path.join(os.environ["PREVIEW"], name[:-4] + ".png"), dpi=80, bbox_inches="tight")
    plt.close(fig)


def legend_right(ax_or_fig, handles=None, **kw):
    target = ax_or_fig
    if handles is None:
        handles, labels = target.get_legend_handles_labels()
    return target.legend(handles=handles, loc="upper left", bbox_to_anchor=(1.01, 1.0), **kw)


def read_sampler(path):
    rows = []
    with open(path) as f:
        for r in csv.DictReader(f):
            def num(k):
                try:
                    return float(r[k])
                except (ValueError, KeyError):
                    return None
            rows.append({k: num(k) for k in r})
    return rows


EVENTS = defaultdict(dict)
if os.path.exists(os.path.join(RAW, "events.csv")):
    for line in open(os.path.join(RAW, "events.csv")):
        t, run, ev = line.strip().split(",", 2)
        EVENTS[run][ev] = float(t)  # a rerun of the same run name overwrites: last one wins


def memtier_series(path, op="Sets"):
    """per-second rows (t_unix, p50, p99, p99.9, max) from a memtier JSON"""
    j = json.load(open(path))
    a = j["ALL STATS"]
    t0 = a["Runtime"]["Start time"] / 1000.0
    out = []
    for k, v in sorted(a[op]["Time-Serie"].items(), key=lambda kv: int(kv[0])):
        if v.get("Count", 0) == 0:
            continue
        out.append((t0 + int(k), v["p50.00"], v["p99.00"], v["p99.90"], v["Max Latency"], v["Count"]))
    return out, a


def pct(a, op, p):
    return a[op]["Percentile Latencies"].get(p)


print("# memory suite: generated tables\n")

# ---------------------------------------------------------------------------------------------- warefeats
wf = {}
for p in sorted(glob.glob(os.path.join(WF, "*.json"))):
    v = os.path.basename(p)[:-5]
    for r in json.load(open(p))["results"]:
        wf[(v, r["workload"])] = r
wf_variants = [v for v in ["kividb-rc4", "kividb-rc4-run2", "kividb-1.1.3", "dragonfly", "redis", "valkey"]
               if any(k[0] == v for k in wf)]


def wf_eng(v):
    return "kividb-rc4" if v.startswith("kividb-rc4") else v


if wf:
    print("## warefeats/kv-stores harness (a324d29)\n")
    for wl in ["memory-load", "memory-load-random"]:
        print(f"### {wl}: 1M keys x 100 B, then a second million on the same engine\n")
        print("| engine | 1st M RSS B/key | 1st M used B/key | 2nd M RSS B/key | 2nd M used B/key | RSS MB after 1st M | baseline RSS MB |")
        print("|---|---|---|---|---|---|---|")
        for v in wf_variants:
            r = wf.get((v, wl))
            if not r:
                continue
            m = r["metrics"]
            print(f"| {v} | {m['bytesPerKeyRss']['value']} | {m['bytesPerKeyUsed']['value']} | "
                  f"{m['bytesPerKeyRssSecond']['value']} | {m['bytesPerKeyUsedSecond']['value']} | "
                  f"{m['absoluteRssMb']['value']} | {m['baselineRssMb']['value']} |")
        print()
    for wl in ["set-get-small", "set-get-large", "pipeline-30"]:
        print(f"### {wl}\n")
        print("| engine | ops/s (median of passes) | min-max ops/s | p50 ms | p99 ms | p99.9 ms | client CPU % |")
        print("|---|---|---|---|---|---|---|")
        for v in wf_variants:
            r = wf.get((v, wl))
            if not r:
                continue
            s = r["samplesOpsPerSec"]
            cpu = r["metrics"].get("clientCpu", {}).get("value", "")
            print(f"| {v} | {r['opsPerSec']:,.0f} | {min(s):,.0f}-{max(s):,.0f} | {r['p50Ms']:.3f} | "
                  f"{r['p99Ms']:.3f} | {r.get('p999Ms', 0):.3f} | {cpu} |")
        print()
    # chart: bytes per key, both data kinds
    fig, axes = plt.subplots(1, 2, figsize=(10, 3.4), sharex=True)
    for ax, wl, title in zip(axes, ["memory-load", "memory-load-random"],
                             ["memtier default values (ASCII 'x')", "random bytes"]):
        vs = [v for v in wf_variants if (v, wl) in wf and not v.endswith("run2")]
        ys = list(range(len(vs)))[::-1]
        vals = [wf[(v, wl)]["metrics"]["bytesPerKeyRss"]["value"] for v in vs]
        ax.barh(ys, vals, color=[col(wf_eng(v)) for v in vs], height=0.6)
        for y, x in zip(ys, vals):
            ax.text(x + 3, y, f"{x:.0f}", va="center", fontsize=9)
        ax.set_yticks(ys, [lab(wf_eng(v)) for v in vs])
        ax.set_title(title, fontweight="normal")
        ax.set_xlabel("RSS bytes per key (first million, 100 B values)")
        ax.grid(axis="y", visible=False)
    fig.suptitle("warefeats memory cell: KiviDB 1.2 cut bytes per key from 267 to ~150", fontweight="bold", x=0.02, ha="left")
    fig.tight_layout()
    save(fig, "warefeats-bytes-per-key.svg")
    # chart: throughput workloads
    fig, axes = plt.subplots(1, 3, figsize=(12, 3.4))
    for ax, wl, title in zip(axes, ["set-get-small", "set-get-large", "pipeline-30"],
                             ["SET:GET 1:10, 100 B, pipeline 1", "SET:GET 1:10, 10 KB, pipeline 1", "SET:GET 1:10, 100 B, pipeline 30"]):
        vs = [v for v in wf_variants if (v, wl) in wf and not v.endswith("run2")]
        ys = list(range(len(vs)))[::-1]
        vals = [wf[(v, wl)]["opsPerSec"] / 1e6 for v in vs]
        ax.barh(ys, vals, color=[col(wf_eng(v)) for v in vs], height=0.6)
        for y, x, v in zip(ys, vals, vs):
            ax.text(x, y, f" {x:.2f}M  p99 {wf[(v, wl)]['p99Ms']:.2f} ms", va="center", fontsize=8)
        ax.set_yticks(ys, [lab(wf_eng(v)) for v in vs])
        ax.set_xlim(0, max(vals) * 1.6)
        ax.set_title(title, fontweight="normal", fontsize=10)
        ax.set_xlabel("million ops/s")
        ax.grid(axis="y", visible=False)
    fig.suptitle("warefeats throughput workloads (32-vCPU client; see README for client-bound rows)", fontweight="bold", x=0.02, ha="left")
    fig.tight_layout()
    save(fig, "warefeats-throughput.svg")

# ---------------------------------------------------------------------------------------------- growth
ck = defaultdict(list)  # (engine, size, kind) -> [(keys, rss_bpk, used_bpk, rss_mb, used_mb)]
cp = os.path.join(RAW, "checkpoints.csv")
if os.path.exists(cp):
    for r in csv.reader(open(cp)):
        e, v, kind, c, brss, bused = r[0], int(r[1]), r[2], int(r[3]), float(r[4]), float(r[5])
        s = r[6:]
        # RSS = sum over the container's processes (Valkey's and Dragonfly's images start the server under a
        # small init process); no fork happens in these runs, so nothing is counted twice. The baseline is the
        # same sum on the idle engine just before the load (from the sampler file).
        g = os.path.join(RAW, f"growth-{e}-{v}-{kind}.csv")
        if os.path.exists(g):
            first = read_sampler(g)
            t0 = EVENTS[f"growth-{e}-{v}-{kind}"].get("start", first[0]["t"])
            brss = min(first, key=lambda x: abs(x["t"] - t0))["rss_all_mb"]
        rss, used, keys = float(s[4]), float(s[6] or 0), int(s[7] or 0)
        ck[(e, v, kind)].append((c, (rss - brss) * MiB / c, (used - bused) * MiB / c, rss, used, keys))
if ck:
    print("## Memory as the dataset grows (random values)\n")
    for v in sorted({k[1] for k in ck}):
        for kind in sorted({k[2] for k in ck if k[1] == v}):
            es = [e for e in ORDER if (e, v, kind) in ck]
            cks = sorted({c for e in es for c, *_ in ck[(e, v, kind)]})
            print(f"### {v} B values ({kind}): RSS bytes per key (used_memory bytes per key)\n")
            print("| engine | " + " | ".join(f"{c / 1e6:g}M keys" for c in cks) + " | RSS at largest |")
            print("|---|" + "---|" * (len(cks) + 1))
            for e in es:
                d = {c: (a, b, rss) for c, a, b, rss, *_ in ck[(e, v, kind)]}
                last = max(d)
                print(f"| {lab(e)} | " + " | ".join(f"{d[c][0]:.1f} ({d[c][1]:.1f})" if c in d else "-" for c in cks)
                      + f" | {d[last][2] / 1024:.2f} GiB |")
            print()
    for kind in sorted({k[2] for k in ck}):
        sizes = sorted({k[1] for k in ck if k[2] == kind})
        fig, axes = plt.subplots(1, len(sizes), figsize=(5.2 * len(sizes), 3.6), squeeze=False)
        for ax, v in zip(axes[0], sizes):
            for e in ORDER:
                pts = ck.get((e, v, kind))
                if not pts:
                    continue
                pts.sort()
                ax.plot([p[0] / 1e6 for p in pts], [p[1] for p in pts], marker="o", ms=5, color=col(e), label=lab(e))
            ax.set_xscale("log")
            ax.set_xticks([1, 5, 10, 25, 50], ["1M", "5M", "10M", "25M", "50M"])
            ax.minorticks_off()
            ax.set_ylim(0, None)
            ax.set_xlabel("keys loaded")
            ax.set_ylabel("RSS bytes per key")
            ax.set_title(f"{v} B values", fontweight="normal")
        legend_right(axes[0][-1])
        fig.suptitle("Bytes per key as the dataset grows (RSS growth / keys, random values)", fontweight="bold", x=0.02, ha="left")
        fig.tight_layout()
        save(fig, f"growth-bytes-per-key-{kind}.svg")
    # RSS over time during the load
    for (v, kind) in sorted({(k[1], k[2]) for k in ck}):
        fig, ax = plt.subplots(figsize=(8.5, 3.8))
        for e in ORDER:
            run = f"growth-{e}-{v}-{kind}"
            p = os.path.join(RAW, run + ".csv")
            if not os.path.exists(p):
                continue
            rows = read_sampler(p)
            t0 = EVENTS[run].get("start", rows[0]["t"])
            ax.plot([(r["t"] - t0) for r in rows], [r["rss_all_mb"] / 1024 for r in rows], color=col(e), label=lab(e))
            for c, *_ in ck[(e, v, kind)]:
                te = EVENTS[run].get(f"loaded-{c}")
                if te:
                    rr = min(rows, key=lambda r: abs(r["t"] - te))
                    ax.plot([te - t0], [rr["rss_all_mb"] / 1024], "o", color=col(e), ms=4)
        ax.set_xlabel("seconds since the load started (dots: 1M, 5M, 10M, 25M, 50M keys written)")
        ax.set_ylabel("RSS (GiB)")
        ax.set_ylim(0, None)
        ax.set_xlim(0, None)
        legend_right(ax)
        ax.set_title(f"RSS while loading 50M keys of {v} B, sampled every second", loc="left")
        save(fig, f"growth-rss-over-time-{v}-{kind}.svg")

# ---------------------------------------------------------------------------------------------- mixed
mixed = [e for e in ORDER if os.path.exists(os.path.join(RAW, f"mixed-{e}.csv"))]
if mixed:
    print("## Mixed workload: 10M keys, 150k ops/s, 80% GET / 20% SET (64-1024 B, TTL 60-600 s), 20 min\n")
    print("| engine | RSS at start GiB | RSS peak GiB | RSS at end GiB | used at end GiB | RSS/used at end | keys at end (M) | ops/s | GET p50/p99/p99.9 ms | SET p50/p99/p99.9 ms |")
    print("|---|---|---|---|---|---|---|---|---|---|")
    figm, axm = plt.subplots(figsize=(8.5, 3.8))
    figk, axk = plt.subplots(figsize=(8.5, 3.4))
    for e in mixed:
        run = f"mixed-{e}"
        rows = read_sampler(os.path.join(RAW, run + ".csv"))
        ts, te = EVENTS[run]["mixed-start"], EVENTS[run]["mixed-end"]
        win = [r for r in rows if ts <= r["t"] <= te]
        if not win:
            continue
        x = [(r["t"] - ts) / 60 for r in rows if r["t"] >= ts - 30]
        axm.plot(x, [r["rss_all_mb"] / 1024 for r in rows if r["t"] >= ts - 30], color=col(e), label=lab(e))
        axk.plot(x, [r["keys"] / 1e6 for r in rows if r["t"] >= ts - 30], color=col(e), label=lab(e))
        jp = os.path.join(RAW, run + ".json")
        lat = ops = ""
        if os.path.exists(jp):
            _, a = memtier_series(jp, "Gets")
            ops = f"{a['Totals']['Ops/sec']:,.0f}"
            g = "/".join(f"{pct(a, 'Gets', p):.2f}" for p in ["p50.00", "p99.00", "p99.90"])
            s = "/".join(f"{pct(a, 'Sets', p):.2f}" for p in ["p50.00", "p99.00", "p99.90"])
            lat = f"{g} | {s}"
        end = win[-1]
        print(f"| {lab(e)} | {win[0]['rss_all_mb'] / 1024:.2f} | {max(r['rss_all_mb'] for r in win) / 1024:.2f} | "
              f"{end['rss_all_mb'] / 1024:.2f} | {end['used_mb'] / 1024:.2f} | {end['rss_all_mb'] / end['used_mb']:.2f} | "
              f"{end['keys'] / 1e6:.2f} | {ops} | {lat} |")
    print()
    for ax, fig, yl, name, title in [
            (axm, figm, "RSS (GiB)", f"mixed-rss-over-time{TAG}.svg", f"RSS under 20 minutes of mixed reads, overwrites and expiries{PAIR}"),
            (axk, figk, "keys (millions)", f"mixed-keys-over-time{TAG}.svg", f"Live keys during the mixed run (TTLs expire, SETs re-create){PAIR}")]:
        ax.axvline(0, color="#555555", lw=1, ls="--")
        ax.axvline(20, color="#555555", lw=1, ls="--")
        ax.text(0.2, ax.get_ylim()[1] * 0.98, "mixed load on", va="top", fontsize=8, color="#555555")
        ax.set_xlabel("minutes since the mixed load started")
        ax.set_ylabel(yl)
        ax.set_ylim(0, None)
        legend_right(ax)
        ax.set_title(title, loc="left")
        save(fig, name)

# ---------------------------------------------------------------------------------------------- snapshot
snaps = [e for e in ORDER if os.path.exists(os.path.join(RAW, f"snap-{e}.csv"))
         and sum(1 for _ in open(os.path.join(RAW, f"snap-{e}.csv"))) > 2]
summary = {}
if snaps:
    print("## Snapshot under a steady 100k SET/s: 30M keys x 1 KB, random values\n")
    print("| engine | event | memory before GiB | peak during GiB | added GiB | added % | duration s | max write latency ms | worst 1-s p99 ms | write p99.9 over window ms | baseline 1-s p99 ms |")
    print("|---|---|---|---|---|---|---|---|---|---|---|")
    for e in snaps:
        run = f"snap-{e}"
        rows = read_sampler(os.path.join(RAW, run + ".csv"))
        evs = EVENTS[run]
        fig, (a1, a2) = plt.subplots(2, 1, figsize=(9.5, 5.6), sharex=True, gridspec_kw={"height_ratios": [3, 2]})
        tw = evs.get("bgsave-window-start", rows[0]["t"])
        tend = evs.get("aofrw-window-end", evs.get("bgsave-window-end", rows[-1]["t"]))
        sel = [r for r in rows if tw - 10 <= r["t"] <= tend + 10]
        a1.plot([r["t"] - tw for r in sel], [r["anon_mb"] / 1024 for r in sel], color=col(e), label="memory (cgroup anon)")
        for w, name in [("bgsave", "BGSAVE"), ("aofrw", "BGREWRITEAOF")]:
            if f"{w}-issued" not in evs:
                continue
            t0, t1 = evs[f"{w}-issued"], evs.get(f"{w}-done", evs[f"{w}-issued"])
            for ax in (a1, a2):
                ax.axvspan(t0 - tw, t1 - tw, color="#bbbbbb", alpha=0.35, lw=0)
            a1.text(t0 - tw, 1.0, f" {name}", transform=a1.get_xaxis_transform(), va="top", fontsize=8, color="#444444")
            before = [r["anon_mb"] for r in rows if t0 - 10 <= r["t"] < t0]
            during = [r["anon_mb"] for r in rows if t0 <= r["t"] <= t1 + 2]
            jp = os.path.join(RAW, f"{run}-{w}.json")
            mx = p99w = p999 = base = ""
            if os.path.exists(jp):
                ser, a = memtier_series(jp, "Sets")
                inw = [s for s in ser if t0 - 1 <= s[0] <= t1 + 1]
                bas = [s for s in ser if s[0] < t0 - 1]
                if inw:
                    mx = max(s[4] for s in inw)
                    p99w = max(s[2] for s in inw)
                if bas:
                    base = st.median(s[2] for s in bas)
                p999 = pct(a, "Sets", "p99.90")
            b = st.median(before) / 1024 if before else float("nan")
            pk = max(during) / 1024 if during else float("nan")
            summary[(e, w)] = dict(before=b, peak=pk, add=pk - b, dur=t1 - t0, max=mx, p99w=p99w)
            f = lambda x: f"{x:.2f}" if isinstance(x, float) else str(x)
            print(f"| {lab(e)} | {name} | {b:.2f} | {pk:.2f} | {pk - b:.2f} | {100 * (pk - b) / b:.0f}% | {t1 - t0:.1f} | "
                  f"{f(mx)} | {f(p99w)} | {f(p999)} | {f(base)} |")
        for w in ["bgsave", "aofrw"]:
            jp = os.path.join(RAW, f"{run}-{w}.json")
            if os.path.exists(jp):
                ser, _ = memtier_series(jp, "Sets")
                a2.plot([s[0] - tw for s in ser], [s[4] for s in ser], color=col(e), lw=1, alpha=0.55,
                        label="max per second" if w == "bgsave" else None)
                a2.plot([s[0] - tw for s in ser], [s[2] for s in ser], color=col(e), lw=2,
                        label="p99 per second" if w == "bgsave" else None)
        a2.set_yscale("log")
        a2.set_ylabel("SET latency (ms, log)")
        a2.set_xlabel("seconds since the write load started (grey: snapshot running)")
        a1.set_ylabel("memory (GiB)")
        a1.set_ylim(0, None)
        legend_right(a1)
        legend_right(a2)
        a1.set_title(f"{lab(e)}: memory and write latency through BGSAVE and BGREWRITEAOF{PAIR}", loc="left")
        fig.tight_layout()
        save(fig, f"snapshot-{e}{TAG}.svg")
    print()
    # summary bars: memory added by the snapshot, and max write latency
    for metric, name, xl, title in [
            ("add", f"snapshot-memory-added{TAG}.svg", "memory added during the snapshot (GiB)", "Extra memory a snapshot costs under 100k writes/s (30M x 1 KB)"),
            ("max", f"snapshot-max-latency{TAG}.svg", "worst SET latency during the snapshot (ms)", "Worst write latency while the snapshot runs")]:
        fig, axes = plt.subplots(1, 2, figsize=(10, 3.2), sharex=True)
        for ax, w, wn in zip(axes, ["bgsave", "aofrw"], ["BGSAVE", "BGREWRITEAOF"]):
            es = [e for e in snaps if (e, w) in summary and summary[(e, w)][metric] != ""]
            ys = list(range(len(es)))[::-1]
            vals = [float(summary[(e, w)][metric]) for e in es]
            ax.barh(ys, vals, color=[col(e) for e in es], height=0.6)
            for y, x in zip(ys, vals):
                ax.text(x, y, f" {x:.2f}", va="center", fontsize=9)
            ax.set_yticks(ys, [lab(e) for e in es])
            ax.set_title(wn if es else f"{wn} (no AOF in Dragonfly)", fontweight="normal")
            ax.set_xlabel(xl)
            ax.set_xlim(0, None)
            ax.grid(axis="y", visible=False)
        fig.suptitle(title + PAIR, fontweight="bold", x=0.02, ha="left")
        fig.tight_layout()
        save(fig, name)

# ---------------------------------------------------------------------------------------------- evict (#284)
ev = [e for e in ORDER if os.path.exists(os.path.join(RAW, f"evict-{e}.csv"))]
if ev:
    print("## maxmemory + BGSAVE (issue #284): 10M keys x 1 KB, maxmemory = used + 10%, allkeys-lru, 100k same-size overwrites/s\n")
    print("| engine | keys before | keys after | evicted keys | maxmemory | lowest key count during run |")
    print("|---|---|---|---|---|---|")
    fig, ax = plt.subplots(figsize=(8.5, 3.6))
    for e in ev:
        run = f"evict-{e}"
        lim = {}
        for line in open(os.path.join(RAW, run + ".limits.txt")):
            for part in line.strip().split():
                if "=" in part:
                    k, v = part.split("=", 1); lim.setdefault(k, v)
                elif ":" in part:
                    k, v = part.split(":", 1); lim[k + ("_after" if k in lim else "")] = v
        rows = read_sampler(os.path.join(RAW, run + ".csv"))
        t0 = EVENTS[run]["writes-start"]
        sel = [r for r in rows if r["t"] >= t0 - 5 and r["keys"]]
        ax.plot([r["t"] - t0 for r in sel], [r["keys"] / 1e6 for r in sel], color=col(e), label=lab(e))
        evk = lim.get("evicted_keys_after", lim.get("evicted_keys", "?"))
        print(f"| {lab(e)} | {int(lim.get('dbsize_before', 0)):,} | {int(lim.get('dbsize_after', 0)):,} | {int(evk):,} | "
              f"{int(lim.get('maxmemory', 0)) / GiB:.2f} GiB | {min(r['keys'] for r in sel) / 1e6:.2f}M |")
        if "bgsave-issued" in EVENTS[run]:
            ax.axvspan(EVENTS[run]["bgsave-issued"] - t0, EVENTS[run].get("bgsave-done", EVENTS[run]["bgsave-issued"]) - t0,
                       color="#bbbbbb", alpha=0.35, lw=0)
    print()
    ax.set_ylim(0, None)
    ax.set_xlabel("seconds since the overwrites started (grey: BGSAVE running)")
    ax.set_ylabel("keys (millions)")
    legend_right(ax)
    ax.set_title("Keys kept at maxmemory while a snapshot runs (#284)", loc="left")
    save(fig, "evict-keys-during-bgsave.svg")
