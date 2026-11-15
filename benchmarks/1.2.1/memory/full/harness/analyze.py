#!/usr/bin/env python3
"""Turn the raw outputs of the memory suite into tables (Markdown) and SVG charts.

    python3 analyze.py <raw-pair-a> <raw-pair-b> <warefeats-dir> <charts-dir> > tables.md

<raw-pair-*>     copy of a client's /var/tmp/mem (sampler CSVs, events.csv, checkpoints.csv, memtier JSON)
<warefeats-dir>  copy of pair A's client /var/tmp/wf (warefeats runner JSON per variant)

Runs are named <test>-<engine>[-<tag>][-r<round>]. 1.2.1 and 1.2.0 ran three alternating rounds;
the tables give their median and the min-max over rounds, the other engines one run each.
"""
import csv, glob, json, os, re, statistics as st, sys
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

RA, RB, WF, CH = sys.argv[1:5]
os.makedirs(CH, exist_ok=True)
plt.rcParams.update({
    "svg.fonttype": "none", "font.family": "DejaVu Sans", "font.size": 10,
    "figure.facecolor": "white", "axes.facecolor": "white", "savefig.facecolor": "white",
    "text.color": "#222222", "axes.labelcolor": "#222222", "xtick.color": "#333333", "ytick.color": "#333333",
    "axes.edgecolor": "#888888", "axes.grid": True, "grid.color": "#e3e3e3", "grid.linewidth": 0.8,
    "axes.spines.top": False, "axes.spines.right": False, "lines.linewidth": 2, "axes.titleweight": "bold",
    "axes.titlesize": 11, "legend.frameon": False,
})
CAND_LABEL = os.environ.get("CAND_LABEL", "KiviDB 1.2.1 (ccba20e2)")
BASE_LABEL = os.environ.get("BASE_LABEL", "KiviDB 1.2.0")
ENG = {  # key: (label, colour). rc4 appears in the regression charts only.
    "kividb-1.2.1": (CAND_LABEL, "#0072B2"),
    "kividb-1.1.3": ("KiviDB 1.1.3", "#56B4E9"),
    "redis": ("Redis 8.10.3", "#D55E00"),
    "valkey": ("Valkey 9.1.3", "#009E73"),
    "dragonfly": ("Dragonfly 2.0.2", "#CC79A7"),
    "kividb-1.2.0": (BASE_LABEL, "#56B4E9"),
}
MAIN = ["kividb-1.2.1", "kividb-1.1.3", "redis", "valkey", "dragonfly"]
MiB, GiB = 1024 ** 2, 1024 ** 3


def lab(e): return ENG[e][0]
def col(e): return ENG[e][1]


def save(fig, name):
    fig.savefig(os.path.join(CH, name), format="svg", bbox_inches="tight")
    if os.environ.get("PREVIEW"):
        fig.savefig(os.path.join(os.environ["PREVIEW"], name[:-4] + ".png"), dpi=80, bbox_inches="tight")
    plt.close(fig)


def legend_right(ax, **kw):
    return ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0), **kw)


def read_sampler(path):
    rows = []
    with open(path) as f:
        for r in csv.DictReader(f):
            def num(k):
                try:
                    return float(r[k])
                except (ValueError, KeyError, TypeError):
                    return None
            rows.append({k: num(k) for k in r})
    return rows


def events(raw):
    E = defaultdict(dict)
    p = os.path.join(raw, "events.csv")
    if os.path.exists(p):
        for line in open(p):
            t, run, ev = line.strip().split(",", 2)
            E[run][ev] = float(t)  # a rerun of the same run name overwrites: last one wins
    return E


EV = {RA: events(RA), RB: events(RB)}


def parse_run(prefix, name):
    """'snap-kividb-1.2.1-r2' -> ('kividb-1.2.1', '', 'r2')"""
    rest = name[len(prefix) + 1:]
    for e in sorted(ENG, key=len, reverse=True):
        if rest == e or rest.startswith(e + "-"):
            tail = rest[len(e) + 1:]
            m = re.match(r"^(?:(.*?)-)?(r\d+)$", tail)
            if m:
                return e, m.group(1) or "", m.group(2)
            return e, tail, "r1"
    return None


def runs(raw, prefix):
    """{(engine, tag, round): run-name} for every sampler CSV <prefix>-*.csv in raw"""
    out = {}
    for p in glob.glob(os.path.join(raw, prefix + "-*.csv")):
        name = os.path.basename(p)[:-4]
        if sum(1 for _ in open(p)) < 3:
            continue
        k = parse_run(prefix, name)
        if k:
            out[k] = name
    return out


def memtier_series(path, op="Sets"):
    """per-second rows (t_unix, p50, p99, p99.9, max, count) from a memtier JSON"""
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


def med(xs):
    xs = [x for x in xs if x is not None]
    return st.median(xs) if xs else None


def rng(xs):
    xs = [x for x in xs if x is not None]
    return (min(xs), max(xs)) if xs else (None, None)


def fmt(x, d=1):
    if x is None or x == "":
        return "-"
    if isinstance(x, str):
        return x
    if abs(x) >= 10000:
        return f"{x:,.0f}"
    return f"{x:.{d}f}"


def mr(xs, d=1):
    """'median (min-max)' over rounds, or the one value"""
    xs = [x for x in xs if x is not None]
    if not xs:
        return "-"
    if len(xs) == 1:
        return fmt(xs[0], d)
    lo, hi = rng(xs)
    return f"{fmt(med(xs), d)} ({fmt(lo, d)}-{fmt(hi, d)})"


# Regression rows: (section, metric, better, cand values, rc4 values). better: "lower" or "higher".
REG = []
FLOOR = 1.0  # percent: a difference smaller than this is not called a regression even beyond the spread


def reg(section, metric, better, cv, rv, d=1):
    cv = [x for x in cv if x is not None]
    rv = [x for x in rv if x is not None]
    if cv and rv:
        REG.append((section, metric, better, cv, rv, d))


print("# memory suite: generated tables\n")

# ---------------------------------------------------------------------------------------------- warefeats
wf = {}
for p in sorted(glob.glob(os.path.join(WF, "*.json"))):
    v = os.path.basename(p)[:-5]
    try:
        for r in json.load(open(p))["results"]:
            wf[(v, r["workload"])] = r
    except (ValueError, KeyError):
        continue


def wf_eng(v):
    for e in ("kividb-1.2.1", "kividb-1.2.0"):
        if v.startswith(e):
            return e
    return v


def wf_vals(e, wl, f):
    return [f(r) for (v, w), r in sorted(wf.items()) if w == wl and wf_eng(v) == e]


MEMK = [("bytesPerKeyRss", "1st M RSS B/key"), ("bytesPerKeyUsed", "1st M used B/key"),
        ("bytesPerKeyRssSecond", "2nd M RSS B/key"), ("bytesPerKeyUsedSecond", "2nd M used B/key")]
TPUT = [("set-get-small", "SET:GET 1:10, 100 B, pipeline 1"), ("set-get-large", "SET:GET 1:10, 10 KB, pipeline 1"),
        ("pipeline-30", "SET:GET 1:10, 100 B, pipeline 30")]
WFE = ["kividb-1.2.1", "kividb-1.2.0", "kividb-1.1.3", "dragonfly", "redis", "valkey"]
if wf:
    print("## warefeats/kv-stores harness (a324d29)\n")
    print("1.2.1 and 1.2.0: median (min-max) over their rounds; one run for the others.\n")
    for wl in ["memory-load", "memory-load-random"]:
        print(f"### {wl}: 1M keys x 100 B, then a second million on the same engine\n")
        print("| engine | runs | " + " | ".join(n for _, n in MEMK) + " |")
        print("|---|---|" + "---|" * len(MEMK))
        for e in WFE:
            rs = wf_vals(e, wl, lambda r: r)
            if not rs:
                continue
            print(f"| {lab(e)} | {len(rs)} | " + " | ".join(mr([r["metrics"][k]["value"] for r in rs]) for k, _ in MEMK) + " |")
        print()
        for k, n in MEMK[:3]:
            reg("warefeats " + ("ASCII" if wl == "memory-load" else "random"), n, "lower",
                wf_vals("kividb-1.2.1", wl, lambda r: r["metrics"][k]["value"]),
                wf_vals("kividb-1.2.0", wl, lambda r: r["metrics"][k]["value"]))
    for wl, title in TPUT:
        print(f"### {wl}: {title}\n")
        print("| engine | runs | ops/s (median of passes) | p50 ms | p99 ms | p99.9 ms | client CPU % |")
        print("|---|---|---|---|---|---|---|")
        for e in WFE:
            rs = wf_vals(e, wl, lambda r: r)
            if not rs:
                continue
            print(f"| {lab(e)} | {len(rs)} | {mr([r['opsPerSec'] for r in rs], 0)} | {mr([r['p50Ms'] for r in rs], 3)} | "
                  f"{mr([r['p99Ms'] for r in rs], 3)} | {mr([r.get('p999Ms') for r in rs], 3)} | "
                  f"{mr([r['metrics'].get('clientCpu', {}).get('value') for r in rs], 1)} |")
        print()
        reg("warefeats " + wl, "ops/s", "higher", wf_vals("kividb-1.2.1", wl, lambda r: r["opsPerSec"]),
            wf_vals("kividb-1.2.0", wl, lambda r: r["opsPerSec"]), 0)
        reg("warefeats " + wl, "p99 ms", "lower", wf_vals("kividb-1.2.1", wl, lambda r: r["p99Ms"]),
            wf_vals("kividb-1.2.0", wl, lambda r: r["p99Ms"]), 3)
        reg("warefeats " + wl, "p99.9 ms", "lower", wf_vals("kividb-1.2.1", wl, lambda r: r.get("p999Ms")),
            wf_vals("kividb-1.2.0", wl, lambda r: r.get("p999Ms")), 3)

    def wf_bar(wl, e, f):
        xs = wf_vals(e, wl, f)
        return med(xs)

    fig, axes = plt.subplots(1, 2, figsize=(10, 3.4), sharex=True)
    for ax, wl, title in zip(axes, ["memory-load", "memory-load-random"],
                             ["memtier default values (ASCII 'x')", "random bytes"]):
        vs = [e for e in MAIN if wf_vals(e, wl, lambda r: r)]
        ys = list(range(len(vs)))[::-1]
        vals = [wf_bar(wl, e, lambda r: r["metrics"]["bytesPerKeyRss"]["value"]) for e in vs]
        ax.barh(ys, vals, color=[col(e) for e in vs], height=0.6)
        for y, x in zip(ys, vals):
            ax.text(x + 3, y, f"{x:.0f}", va="center", fontsize=9)
        ax.set_yticks(ys, [lab(e) for e in vs])
        ax.set_title(title, fontweight="normal")
        ax.set_xlabel("RSS bytes per key (first million, 100 B values)")
        ax.grid(axis="y", visible=False)
    fig.suptitle("warefeats memory cell: KiviDB 1.2 cut bytes per key from 267 to ~150", fontweight="bold", x=0.02, ha="left")
    fig.tight_layout()
    save(fig, "warefeats-bytes-per-key.svg")
    fig, axes = plt.subplots(1, 3, figsize=(12, 3.4))
    for ax, (wl, title) in zip(axes, TPUT):
        vs = [e for e in MAIN if wf_vals(e, wl, lambda r: r)]
        ys = list(range(len(vs)))[::-1]
        vals = [wf_bar(wl, e, lambda r: r["opsPerSec"]) / 1e6 for e in vs]
        p99 = [wf_bar(wl, e, lambda r: r["p99Ms"]) for e in vs]
        ax.barh(ys, vals, color=[col(e) for e in vs], height=0.6)
        for y, x, p in zip(ys, vals, p99):
            ax.text(x, y, f" {x:.2f}M  p99 {p:.2f} ms", va="center", fontsize=8)
        ax.set_yticks(ys, [lab(e) for e in vs])
        ax.set_xlim(0, max(vals) * 1.6)
        ax.set_title(title, fontweight="normal", fontsize=10)
        ax.set_xlabel("million ops/s")
        ax.grid(axis="y", visible=False)
    fig.suptitle("warefeats throughput workloads (32-vCPU client; see README for client-bound rows)", fontweight="bold", x=0.02, ha="left")
    fig.tight_layout()
    save(fig, "warefeats-throughput.svg")

# ---------------------------------------------------------------------------------------------- growth
ck = defaultdict(lambda: defaultdict(list))  # (engine, size, kind) -> round -> [(keys, rss_bpk, used_bpk, rss_mb, used_mb, keys)]
loadt = defaultdict(list)  # (engine, size, kind) -> [seconds to load the last stage... total load seconds per round]
cp = os.path.join(RA, "checkpoints.csv")
if os.path.exists(cp):
    for r in csv.reader(open(cp)):
        e, v, kind, c, brss, bused, rnd = r[0], int(r[1]), r[2], int(r[3]), float(r[4]), float(r[5]), r[6]
        s = r[7:]
        run = f"growth-{e}-{v}-{kind}-{rnd}"
        g = os.path.join(RA, run + ".csv")
        if os.path.exists(g):
            first = read_sampler(g)
            t0 = EV[RA][run].get("start", first[0]["t"])
            brss = min(first, key=lambda x: abs(x["t"] - t0))["rss_all_mb"]
        rss, used, keys = float(s[4]), float(s[6] or 0), int(s[7] or 0)
        ck[(e, v, kind)][rnd].append((c, (rss - brss) * MiB / c, (used - bused) * MiB / c, rss, used, keys))


def growth_load_seconds(e, v, kind, rnd):
    evs = EV[RA].get(f"growth-{e}-{v}-{kind}-{rnd}", {})
    tot = 0.0
    for k, t in evs.items():
        if k.startswith("load-to-"):
            c = k[len("load-to-"):]
            if f"loaded-{c}" in evs:
                tot += evs[f"loaded-{c}"] - t
    return tot or None


if ck:
    print("## Memory as the dataset grows (random values)\n")
    print("RSS bytes per key (used_memory bytes per key). 1.2.1 and 1.2.0: median over rounds.\n")
    for v in sorted({k[1] for k in ck}):
        for kind in sorted({k[2] for k in ck if k[1] == v}):
            es = [e for e in MAIN + ["kividb-1.2.0"] if (e, v, kind) in ck]
            cks = sorted({p[0] for e in es for rr in ck[(e, v, kind)].values() for p in rr})
            print(f"### {v} B values ({kind})\n")
            print("| engine | runs | " + " | ".join(f"{c / 1e6:g}M keys" for c in cks) + " | RSS at largest | load s (all stages) |")
            print("|---|---|" + "---|" * (len(cks) + 2))
            for e in es:
                rounds = ck[(e, v, kind)]
                cells = []
                for c in cks:
                    a = [p[1] for rr in rounds.values() for p in rr if p[0] == c]
                    b = [p[2] for rr in rounds.values() for p in rr if p[0] == c]
                    cells.append(f"{fmt(med(a))} ({fmt(med(b))})" if a else "-")
                last = max(cks)
                rl = [p[3] for rr in rounds.values() for p in rr if p[0] == last]
                lt = [growth_load_seconds(e, v, kind, rnd) for rnd in rounds]
                print(f"| {lab(e)} | {len(rounds)} | " + " | ".join(cells) + f" | {fmt(med(rl) / 1024 if rl else None, 2)} GiB | {mr(lt)} |")
            print()
            for c in cks:
                f = lambda e: [p[1] for rr in ck[(e, v, kind)].values() for p in rr if p[0] == c] if (e, v, kind) in ck else []
                reg(f"growth {v} B", f"RSS B/key at {c / 1e6:g}M", "lower", f("kividb-1.2.1"), f("kividb-1.2.0"))
            if ("kividb-1.2.1", v, kind) in ck and ("kividb-1.2.0", v, kind) in ck:
                reg(f"growth {v} B", "load seconds, 50M keys", "lower",
                    [growth_load_seconds("kividb-1.2.1", v, kind, r) for r in ck[("kividb-1.2.1", v, kind)]],
                    [growth_load_seconds("kividb-1.2.0", v, kind, r) for r in ck[("kividb-1.2.0", v, kind)]])
    for kind in sorted({k[2] for k in ck}):
        sizes = sorted({k[1] for k in ck if k[2] == kind})
        fig, axes = plt.subplots(1, len(sizes), figsize=(5.2 * len(sizes), 3.6), squeeze=False)
        for ax, v in zip(axes[0], sizes):
            for e in MAIN:
                if (e, v, kind) not in ck:
                    continue
                rounds = ck[(e, v, kind)]
                cs = sorted({p[0] for rr in rounds.values() for p in rr})
                ys = [med([p[1] for rr in rounds.values() for p in rr if p[0] == c]) for c in cs]
                ax.plot([c / 1e6 for c in cs], ys, marker="o", ms=5, color=col(e), label=lab(e))
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
    for (v, kind) in sorted({(k[1], k[2]) for k in ck}):
        fig, ax = plt.subplots(figsize=(8.5, 3.8))
        for e in MAIN:
            run = f"growth-{e}-{v}-{kind}-r1"
            p = os.path.join(RA, run + ".csv")
            if not os.path.exists(p):
                continue
            rows = read_sampler(p)
            t0 = EV[RA][run].get("start", rows[0]["t"])
            ax.plot([(r["t"] - t0) for r in rows], [r["rss_all_mb"] / 1024 for r in rows], color=col(e), label=lab(e))
            for c, *_ in ck[(e, v, kind)]["r1"]:
                te = EV[RA][run].get(f"loaded-{c}")
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

# ---------------------------------------------------------------------------------------------- mixed (pair B)
MX = runs(RB, "mixed")
mixed_sum = {}
if MX:
    print("## Mixed workload (pair B): 10M keys, 150k ops/s, 80% GET / 20% SET (64-1024 B, TTL 60-600 s), 20 min, distinct client seeds\n")
    print("| engine | round | RSS at start GiB | RSS peak GiB | RSS at end GiB | used at end GiB | RSS/used at end | keys at end (M) | ops/s | GET p50/p99/p99.9 ms | SET p50/p99/p99.9 ms |")
    print("|---|---|---|---|---|---|---|---|---|---|---|")
    for (e, tag, rnd), run in sorted(MX.items(), key=lambda kv: (list(ENG).index(kv[0][0]), kv[0][2])):
        rows = read_sampler(os.path.join(RB, run + ".csv"))
        evs = EV[RB][run]
        if "mixed-start" not in evs or "mixed-end" not in evs:
            continue
        ts, te = evs["mixed-start"], evs["mixed-end"]
        win = [r for r in rows if ts <= r["t"] <= te]
        if not win:
            continue
        jp = os.path.join(RB, run + ".json")
        d = dict(start=win[0]["rss_all_mb"] / 1024, peak=max(r["rss_all_mb"] for r in win) / 1024,
                 end=win[-1]["rss_all_mb"] / 1024, used=win[-1]["used_mb"] / 1024,
                 ratio=win[-1]["rss_all_mb"] / win[-1]["used_mb"], keys=win[-1]["keys"] / 1e6)
        lat = "- | -"
        if os.path.exists(jp):
            try:
                _, a = memtier_series(jp, "Gets")
                d["ops"] = a["Totals"]["Ops/sec"]
                for op in ("Gets", "Sets"):
                    for p in ("p50.00", "p99.00", "p99.90"):
                        d[f"{op}{p}"] = pct(a, op, p)
                g = "/".join(f"{d['Gets' + p]:.2f}" for p in ["p50.00", "p99.00", "p99.90"])
                s = "/".join(f"{d['Sets' + p]:.2f}" for p in ["p50.00", "p99.00", "p99.90"])
                lat = f"{g} | {s}"
            except (ValueError, KeyError):
                pass
        mixed_sum[(e, rnd)] = d
        print(f"| {lab(e)} | {rnd} | {d['start']:.2f} | {d['peak']:.2f} | {d['end']:.2f} | {d['used']:.2f} | {d['ratio']:.2f} | "
              f"{d['keys']:.2f} | {fmt(d.get('ops'), 0)} | {lat} |")
    print()

    def mv(e, k):
        return [d.get(k) for (ee, _), d in mixed_sum.items() if ee == e]
    for k, n, b, dd in [("end", "RSS at end GiB", "lower", 2), ("peak", "RSS peak GiB", "lower", 2), ("ratio", "RSS/used at end", "lower", 2),
                        ("Getsp99.00", "GET p99 ms", "lower", 2), ("Getsp99.90", "GET p99.9 ms", "lower", 2),
                        ("Setsp99.00", "SET p99 ms", "lower", 2), ("Setsp99.90", "SET p99.9 ms", "lower", 2)]:
        reg("mixed 20 min", n, b, mv("kividb-1.2.1", k), mv("kividb-1.2.0", k), dd)
    for key, yl, name, title in [
            ("rss_all_mb", "RSS (GiB)", "mixed-rss-over-time.svg", "RSS under 20 minutes of mixed reads, overwrites and expiries (pair B)"),
            ("keys", "keys (millions)", "mixed-keys-over-time.svg", "Live keys during the mixed run (TTLs expire, SETs re-create; pair B)")]:
        fig, ax = plt.subplots(figsize=(8.5, 3.8))
        for e in MAIN:
            run = MX.get((e, "", "r1"))
            if not run or (e, "r1") not in mixed_sum:
                continue
            rows = read_sampler(os.path.join(RB, run + ".csv"))
            ts = EV[RB][run]["mixed-start"]
            sel = [r for r in rows if r["t"] >= ts - 30 and r[key] is not None]
            div = 1024 if key == "rss_all_mb" else 1e6
            ax.plot([(r["t"] - ts) / 60 for r in sel], [r[key] / div for r in sel], color=col(e), label=lab(e))
        ax.axvline(0, color="#555555", lw=1, ls="--")
        ax.axvline(20, color="#555555", lw=1, ls="--")
        ax.set_xlabel("minutes since the mixed load started")
        ax.set_ylabel(yl)
        ax.set_ylim(0, None)
        legend_right(ax)
        ax.set_title(title, loc="left")
        save(fig, name)

# ---------------------------------------------------------------------------------------------- snapshot (pair A)
SN = runs(RA, "snap")
snap_sum = {}
if SN:
    print("## Snapshot under a steady 100k SET/s (uniform random keys): 30M keys x 1 KB, random values\n")
    print("| engine | round | event | memory before GiB | peak during GiB | added GiB | added % | duration s | worst write ms | worst 1-s p99 ms | write p99.9 over window ms | p99 before ms | memory 60 s after GiB | used after GiB |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for (e, tag, rnd), run in sorted(SN.items(), key=lambda kv: (list(ENG).index(kv[0][0]), kv[0][2])):
        rows = read_sampler(os.path.join(RA, run + ".csv"))
        evs = EV[RA][run]
        for w, name in [("bgsave", "BGSAVE"), ("aofrw", "BGREWRITEAOF")]:
            if f"{w}-issued" not in evs:
                continue
            t0, t1 = evs[f"{w}-issued"], evs.get(f"{w}-done", evs[f"{w}-issued"])
            before = [r["anon_mb"] for r in rows if t0 - 10 <= r["t"] < t0]
            during = [r["anon_mb"] for r in rows if t0 <= r["t"] <= t1 + 2]
            after = [r for r in rows if t1 + 55 <= r["t"] <= t1 + 65]
            jp = os.path.join(RA, f"{run}-{w}.json")
            mx = p99w = p999 = base = None
            if os.path.exists(jp):
                try:
                    ser, a = memtier_series(jp, "Sets")
                    inw = [s for s in ser if t0 - 1 <= s[0] <= t1 + 1]
                    bas = [s for s in ser if s[0] < t0 - 1]
                    if inw:
                        mx = max(s[4] for s in inw)
                        p99w = max(s[2] for s in inw)
                    if bas:
                        base = st.median(s[2] for s in bas)
                    p999 = pct(a, "Sets", "p99.90")
                except (ValueError, KeyError):
                    pass
            b = st.median(before) / 1024 if before else None
            pk = max(during) / 1024 if during else None
            d = dict(before=b, peak=pk, add=(pk - b) if b and pk else None, addp=100 * (pk - b) / b if b and pk else None,
                     dur=t1 - t0, max=mx, p99w=p99w, p999=p999, base=base,
                     after=med([r["anon_mb"] for r in after]) / 1024 if after else None,
                     used_after=med([r["used_mb"] for r in after if r["used_mb"]]) / 1024 if after and any(r["used_mb"] for r in after) else None)
            snap_sum[(e, rnd, w)] = d
            print(f"| {lab(e)} | {rnd} | {name} | {fmt(d['before'], 2)} | {fmt(d['peak'], 2)} | {fmt(d['add'], 2)} | {fmt(d['addp'], 0)}% | "
                  f"{fmt(d['dur'])} | {fmt(mx, 0)} | {fmt(p99w, 2)} | {fmt(p999, 2)} | {fmt(base, 2)} | {fmt(d['after'], 2)} | {fmt(d['used_after'], 2)} |")
    print()
    print("### Snapshot, per engine: median over rounds\n")
    print("| engine | runs | event | added GiB | added % | duration s | worst write ms | write p99.9 ms | p99 before ms | memory 60 s after GiB |")
    print("|---|---|---|---|---|---|---|---|---|---|")
    for e in MAIN + ["kividb-1.2.0"]:
        for w, name in [("bgsave", "BGSAVE"), ("aofrw", "BGREWRITEAOF")]:
            ds = [d for (ee, _, ww), d in snap_sum.items() if ee == e and ww == w]
            if not ds:
                continue
            g = lambda k, dd=1: mr([d[k] for d in ds], dd)
            print(f"| {lab(e)} | {len(ds)} | {name} | {g('add', 2)} | {g('addp', 0)} | {g('dur')} | {g('max', 0)} | {g('p999', 2)} | {g('base', 2)} | {g('after', 2)} |")
    print()
    for w, name in [("bgsave", "BGSAVE"), ("aofrw", "BGREWRITEAOF")]:
        sv = lambda e, k: [d[k] for (ee, _, ww), d in snap_sum.items() if ee == e and ww == w]
        for k, n, dd in [("add", "memory added GiB", 2), ("dur", "duration s", 1), ("max", "worst write ms", 0),
                         ("p999", "write p99.9 ms", 2), ("base", "write p99 before ms", 2), ("after", "memory 60 s after GiB", 2)]:
            reg(f"snapshot {name}", n, "lower", sv("kividb-1.2.1", k), sv("kividb-1.2.0", k), dd)
    # per-engine time series (round 1)
    for e in MAIN:
        run = SN.get((e, "", "r1"))
        if not run:
            continue
        rows = read_sampler(os.path.join(RA, run + ".csv"))
        evs = EV[RA][run]
        fig, (a1, a2) = plt.subplots(2, 1, figsize=(9.5, 5.6), sharex=True, gridspec_kw={"height_ratios": [3, 2]})
        tw = evs.get("bgsave-window-start", rows[0]["t"])
        tend = evs.get("aofrw-window-end", evs.get("bgsave-window-end", rows[-1]["t"]))
        sel = [r for r in rows if tw - 10 <= r["t"] <= tend + 10]
        a1.plot([r["t"] - tw for r in sel], [r["anon_mb"] / 1024 for r in sel], color=col(e), label="memory (cgroup anon)")
        if any(r["used_mb"] for r in sel):
            a1.plot([r["t"] - tw for r in sel if r["used_mb"]], [r["used_mb"] / 1024 for r in sel if r["used_mb"]], color=col(e),
                    lw=1, ls="--", label="used_memory")
        for w, name in [("bgsave", "BGSAVE"), ("aofrw", "BGREWRITEAOF")]:
            if f"{w}-issued" not in evs:
                continue
            t0, t1 = evs[f"{w}-issued"], evs.get(f"{w}-done", evs[f"{w}-issued"])
            for ax in (a1, a2):
                ax.axvspan(t0 - tw, t1 - tw, color="#bbbbbb", alpha=0.35, lw=0)
            a1.text(t0 - tw, 1.0, f" {name}", transform=a1.get_xaxis_transform(), va="top", fontsize=8, color="#444444")
            jp = os.path.join(RA, f"{run}-{w}.json")
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
        a1.set_title(f"{lab(e)}: memory and write latency through BGSAVE and BGREWRITEAOF", loc="left")
        fig.tight_layout()
        save(fig, f"snapshot-{e}.svg")
    for metric, name, xl, title in [
            ("add", "snapshot-memory-added.svg", "memory added during the snapshot (GiB)", "Extra memory a snapshot costs under 100k uniformly random writes/s (30M x 1 KB)"),
            ("max", "snapshot-max-latency.svg", "worst SET latency during the snapshot (ms)", "Worst write latency while the snapshot runs")]:
        fig, axes = plt.subplots(1, 2, figsize=(10, 3.2), sharex=True)
        for ax, w, wn in zip(axes, ["bgsave", "aofrw"], ["BGSAVE", "BGREWRITEAOF"]):
            es = [e for e in MAIN if med([d[metric] for (ee, _, ww), d in snap_sum.items() if ee == e and ww == w]) is not None]
            ys = list(range(len(es)))[::-1]
            vals = [med([d[metric] for (ee, _, ww), d in snap_sum.items() if ee == e and ww == w]) for e in es]
            ax.barh(ys, vals, color=[col(e) for e in es], height=0.6)
            for y, x in zip(ys, vals):
                ax.text(x, y, f" {x:.2f}" if metric == "add" else f" {x:.0f}", va="center", fontsize=9)
            ax.set_yticks(ys, [lab(e) for e in es])
            ax.set_title(wn if w == "bgsave" else f"{wn} (Dragonfly has no AOF)", fontweight="normal")
            ax.set_xlabel(xl)
            ax.set_xlim(0, (max(vals) * 1.25) if vals else None)
            ax.grid(axis="y", visible=False)
        fig.suptitle(title, fontweight="bold", x=0.02, ha="left")
        fig.tight_layout()
        save(fig, name)

# ---------------------------------------------------------------------------------------------- evict (#284)
EVR = runs(RA, "evict")
ev_sum = {}
if EVR:
    print("## maxmemory + BGSAVE (issue #284): 10M keys x 1 KB, maxmemory = used + 10%, allkeys-lru, 100k same-size overwrites/s (uniform)\n")
    print("| engine | round | keys before | keys after | evicted keys | maxmemory | lowest key count during run | SET p99 / p99.9 ms |")
    print("|---|---|---|---|---|---|---|---|")
    fig, ax = plt.subplots(figsize=(8.5, 3.6))
    for (e, tag, rnd), run in sorted(EVR.items(), key=lambda kv: (list(ENG).index(kv[0][0]), kv[0][2])):
        lim = {}
        lp = os.path.join(RA, run + ".limits.txt")
        if not os.path.exists(lp):
            continue
        for line in open(lp):
            for part in line.strip().split():
                if "=" in part:
                    k, v = part.split("=", 1); lim.setdefault(k, v)
                elif ":" in part:
                    k, v = part.split(":", 1); lim[k + ("_after" if k in lim else "")] = v
        rows = read_sampler(os.path.join(RA, run + ".csv"))
        evs = EV[RA][run]
        if "writes-start" not in evs:
            continue
        t0 = evs["writes-start"]
        sel = [r for r in rows if r["t"] >= t0 - 5 and r["keys"]]
        if rnd == "r1" and e in MAIN:
            ax.plot([r["t"] - t0 for r in sel], [r["keys"] / 1e6 for r in sel], color=col(e), label=lab(e))
            if "bgsave-issued" in evs:
                ax.axvspan(evs["bgsave-issued"] - t0, evs.get("bgsave-done", evs["bgsave-issued"]) - t0, color="#bbbbbb", alpha=0.35, lw=0)
        evk = int(lim.get("evicted_keys_after", lim.get("evicted_keys", 0)))
        lat = "-"
        jp = os.path.join(RA, run + ".json")
        if os.path.exists(jp):
            try:
                _, a = memtier_series(jp, "Sets")
                lat = f"{pct(a, 'Sets', 'p99.00'):.2f} / {pct(a, 'Sets', 'p99.90'):.2f}"
            except (ValueError, KeyError):
                pass
        ev_sum[(e, rnd)] = evk
        print(f"| {lab(e)} | {rnd} | {int(lim.get('dbsize_before', 0)):,} | {int(lim.get('dbsize_after', 0)):,} | {evk:,} | "
              f"{int(lim.get('maxmemory', 0)) / GiB:.2f} GiB | {min(r['keys'] for r in sel) / 1e6:.2f}M | {lat} |")
    print()
    ax.set_ylim(0, None)
    ax.set_xlabel("seconds since the overwrites started (grey: BGSAVE running)")
    ax.set_ylabel("keys (millions)")
    legend_right(ax)
    ax.set_title("Keys kept at maxmemory while a snapshot runs (#284)", loc="left")
    save(fig, "evict-keys-during-bgsave.svg")
    reg("#284 eviction", "keys evicted", "lower", [v for (e, _), v in ev_sum.items() if e == "kividb-1.2.1"],
        [v for (e, _), v in ev_sum.items() if e == "kividb-1.2.0"], 0)

# ---------------------------------------------------------------------------------------------- active defrag (pair B)
DF = runs(RB, "defrag")
df_sum = {}


def window_lat(ser, t0, t1):
    w = [s for s in ser if t0 <= s[0] < t1]
    if not w:
        return None
    return dict(p99_med=med([s[2] for s in w]), p99_max=max(s[2] for s in w), p999_med=med([s[3] for s in w]),
                p999_max=max(s[3] for s in w), max=max(s[4] for s in w), n=len(w))


if DF:
    print("## Active defrag after deleting a random half: 5M keys x 100 B, probe load 80k GET/s + 20k SET/s (pair B)\n")
    print("Slab = INFO value_slab_bytes (live = value_slab_live_bytes). *Give-back time*: from the end of the delete until RSS has made 90% of its "
          "final drop. *Compaction*: seconds with active_defrag_running = 1. Latency: per-second p99 / p99.9 from memtier, median over the "
          "seconds of the window (worst second in brackets); *before* = the 50 s before the delete, *during* = while compaction runs.\n")
    print("| build | round | RSS after load MB | slab / live after delete MB | RSS after delete MB | RSS final MB | slab / live final MB | RSS returned MB | give-back s | compaction s | hits | GET p99 before / during ms | GET p99.9 before / during ms | SET p99 before / during ms | SET p99.9 before / during ms | max ms during |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for (e, tag, rnd), run in sorted(DF.items(), key=lambda kv: (list(ENG).index(kv[0][0]), kv[0][1], kv[0][2])):
        rows = read_sampler(os.path.join(RB, run + ".csv"))
        evs = EV[RB][run]
        if "delete-done" not in evs:
            continue
        tl, td0, td = evs["loaded"], evs["delete-start"], evs["delete-done"]
        r_load = [r for r in rows if tl + 5 <= r["t"] < td0]
        r_after = [r for r in rows if td <= r["t"] <= td + 3]
        end = rows[-5:]
        rss_load = med([r["rss_all_mb"] for r in r_load[-10:]])
        rss_del = max(r["rss_all_mb"] for r in rows if td0 <= r["t"] <= td + 3)
        rss_fin = med([r["rss_all_mb"] for r in end])
        slab_del = r_after[-1]["slab_mb"] if r_after else None
        live_del = r_after[-1]["slab_live_mb"] if r_after else None
        slab_fin, live_fin = end[-1]["slab_mb"], end[-1]["slab_live_mb"]
        drop = rss_del - rss_fin
        gb = None
        if drop > 16:
            tgt = rss_fin + 0.1 * drop
            for r in rows:
                if r["t"] >= td and r["rss_all_mb"] <= tgt:
                    gb = r["t"] - td
                    break
        runs_on = [r["t"] for r in rows if r["t"] >= td0 and r.get("defrag_running") == 1]
        comp = (runs_on[-1] - runs_on[0] + 1) if runs_on else 0
        t_c0 = runs_on[0] if runs_on else td
        t_c1 = runs_on[-1] + 1 if runs_on else td + 60
        hits = end[-1].get("defrag_hits")
        d = dict(rss_load=rss_load, rss_del=rss_del, rss_fin=rss_fin, ret=drop, gb=gb, comp=comp, hits=hits,
                 slab_del=slab_del, live_del=live_del, slab_fin=slab_fin, live_fin=live_fin, run=run)
        lat_cells = []
        for op, fn in [("Gets", "get"), ("Sets", "set")]:
            jp = os.path.join(RB, f"{run}-{fn}.json")
            if not os.path.exists(jp):
                lat_cells += ["-", "-"]
                continue
            ser, a = memtier_series(jp, op)
            b = window_lat(ser, td0 - 50, td0)
            du = window_lat(ser, t_c0, t_c1) if runs_on else None
            d[op + "_before"], d[op + "_during"] = b, du
            def cell(k):
                if not b:
                    return "-"
                s = f"{b[k + '_med']:.2f} / "
                s += f"{du[k + '_med']:.2f} ({du[k + '_max']:.2f})" if du else "-"
                return s
            lat_cells += [cell("p99"), cell("p999")]
            d[op + "_maxdur"] = du["max"] if du else None
        df_sum[(e, tag, rnd)] = d
        name = lab(e) + (f" ({tag.replace('off', 'activedefrag no')})" if tag else "")
        mxd = max([x for x in (d.get("Gets_maxdur"), d.get("Sets_maxdur")) if x is not None], default=None)
        print(f"| {name} | {rnd} | {fmt(rss_load, 0)} | {fmt(slab_del, 0)} / {fmt(live_del, 0)} | {fmt(rss_del, 0)} | {fmt(rss_fin, 0)} | "
              f"{fmt(slab_fin, 0)} / {fmt(live_fin, 0)} | {fmt(drop, 0)} | {fmt(gb)} | {comp:.0f} | {fmt(hits, 0)} | "
              f"{lat_cells[0]} | {lat_cells[1]} | {lat_cells[2]} | {lat_cells[3]} | {fmt(mxd, 1)} |")
    print()

    def dv(e, k, sub=None, tag=""):
        out = []
        for (ee, tg, _), d in df_sum.items():
            if ee != e or tg != tag:
                continue
            x = d.get(k)
            if sub is not None:
                x = x.get(sub) if x else None
            out.append(x)
        return out
    for k, n, b, dd in [("rss_fin", "RSS final MB", "lower", 0), ("ret", "RSS returned MB", "higher", 0), ("gb", "give-back s", "lower", 1),
                        ("comp", "compaction s", "lower", 0), ("slab_fin", "slab final MB", "lower", 0)]:
        reg("active defrag", n, b, dv("kividb-1.2.1", k), dv("kividb-1.2.0", k), dd)
    for op in ("Gets", "Sets"):
        for sub, n in [("p99_med", "p99"), ("p999_med", "p99.9")]:
            reg("active defrag", f"{op[:-1].upper()} {n} during compaction ms", "lower",
                dv("kividb-1.2.1", op + "_during", sub), dv("kividb-1.2.0", op + "_during", sub), 2)

    # the probe's latency level moves from run to run (both builds, with compaction on or off); what compaction
    # itself adds is the ratio of the same run's seconds during compaction to its seconds before the delete
    def ratio(e, op, sub):
        out = []
        for (ee, tg, _), d in df_sum.items():
            b, du = d.get(op + "_before"), d.get(op + "_during")
            if ee == e and tg == "" and b and du and b[sub]:
                out.append(du[sub] / b[sub])
        return out
    for op in ("Gets", "Sets"):
        for sub, n in [("p99_med", "p99"), ("p999_med", "p99.9")]:
            reg("active defrag", f"{op[:-1].upper()} {n} during / before (same run)", "lower",
                ratio("kividb-1.2.1", op, sub), ratio("kividb-1.2.0", op, sub), 2)
    # chart: RSS, slab bytes and live bytes over time, candidate vs rc4 (one round in which both ran the same
    # 300-s probe), plus the activedefrag-off control; per-second p99.9 of the probe GETs and SETs below
    CR = os.environ.get("DEFRAG_CHART_ROUND", "r2")
    fig, (a1, a2, a3) = plt.subplots(3, 1, figsize=(9.5, 7.6), sharex=True, gridspec_kw={"height_ratios": [3, 1.6, 1.6]})
    for (e, tag, rnd), ls in [(("kividb-1.2.1", "", CR), "-"), (("kividb-1.2.0", "", CR), "-"), (("kividb-1.2.1", "off", "r1"), ":")]:
        key = (e, tag, rnd)
        if key not in df_sum:
            continue
        run = df_sum[key]["run"]
        rows = read_sampler(os.path.join(RB, run + ".csv"))
        evs = EV[RB][run]
        td = evs["delete-done"]
        sel = [r for r in rows if r["t"] >= evs["loaded"] - 5]
        name = lab(e) + (" (activedefrag no)" if tag else "")
        a1.plot([r["t"] - td for r in sel], [r["rss_all_mb"] for r in sel], color=col(e), ls=ls, label=f"{name}: RSS")
        if not tag:
            a1.plot([r["t"] - td for r in sel if r["slab_mb"] is not None], [r["slab_mb"] for r in sel if r["slab_mb"] is not None],
                    color=col(e), ls="-", lw=1, alpha=0.8, label=f"{name}: slab bytes")
            a1.plot([r["t"] - td for r in sel if r["slab_live_mb"] is not None],
                    [r["slab_live_mb"] for r in sel if r["slab_live_mb"] is not None], color=col(e), ls="--", lw=1, alpha=0.8,
                    label=f"{name}: slab live bytes")
            on = [r["t"] - td for r in sel if r.get("defrag_running") == 1]
            if on:
                a1.axvspan(on[0], on[-1] + 1, color=col(e), alpha=0.10, lw=0)
        for ax, op, fn in [(a2, "Gets", "get"), (a3, "Sets", "set")]:
            jp = os.path.join(RB, f"{run}-{fn}.json")
            if os.path.exists(jp):
                ser, _ = memtier_series(jp, op)
                ser = [s for s in ser if s[5] > 1000]  # drop the last, partial second
                ax.plot([s[0] - td for s in ser], [s[3] for s in ser], color=col(e), ls=ls, lw=1.1, label=name)
    a1.set_ylabel("MB")
    a1.set_ylim(0, None)
    a1.set_title(f"Active defrag after deleting a random half of 5M x 100 B (round {CR[1:]}; shaded: compaction running)", loc="left")
    for ax, n in [(a2, "GET"), (a3, "SET")]:
        ax.set_yscale("log")
        ax.set_ylabel(f"{n} p99.9 ms\n(per second, log)")
        legend_right(ax, fontsize=8)
    for ax in (a1, a2, a3):
        ax.axvline(0, color="#555555", lw=1, ls="--")
    legend_right(a1, fontsize=8)
    a3.set_xlim(-60, 240)
    a3.set_xlabel("seconds since the delete finished (probe load: 80k GET/s + 20k SET/s throughout)")
    fig.tight_layout()
    save(fig, "defrag-vs-1.2.0.svg")

# ---------------------------------------------------------------------------------------------- AOF-on write latency (pair B)
AL = runs(RB, "aoflat")
if AL:
    print("## Write latency with the AOF on, no snapshot (pair B): 2M x 1 KB, 100k uniformly random SET/s, 180 s\n")
    print("The check of the snapshot runs' steady-state write p99 (`harness/aoflat.sh`). Rounds r1-r4 alternate the builds; "
          "p1 is the round with `perf record` running for 30 s.\n")
    print("| build | round | ops/s | SET p50 ms | p99 ms | p99.9 ms | worst write ms | median of 1-s p99 ms | engine CPU s (load + run) |")
    print("|---|---|---|---|---|---|---|---|---|")
    al_sum = {}
    for (e, tag, rnd), run in sorted(AL.items(), key=lambda kv: (list(ENG).index(kv[0][0]), kv[0][1], kv[0][2])):
        rnd = tag or rnd  # "aoflat-<engine>-p1": the profiled round
        jp = os.path.join(RB, run + ".json")
        if not os.path.exists(jp):
            continue
        ser, a = memtier_series(jp, "Sets")
        ser = [s for s in ser if s[5] > 1000][5:]
        cpu = None
        cp_ = os.path.join(RB, run + ".cpu.txt")
        if os.path.exists(cp_):
            try:
                cpu = float(open(cp_).read().split()[0])
            except (ValueError, IndexError):
                pass
        d = dict(ops=a["Totals"]["Ops/sec"], p50=pct(a, "Sets", "p50.00"), p99=pct(a, "Sets", "p99.00"),
                 p999=pct(a, "Sets", "p99.90"), mx=max(s[4] for s in ser), m99=med([s[2] for s in ser]), cpu=cpu)
        if not rnd.startswith("p"):
            al_sum[(e, rnd)] = d
        print(f"| {lab(e)} | {rnd} | {fmt(d['ops'], 0)} | {d['p50']:.3f} | {d['p99']:.3f} | {d['p999']:.3f} | {d['mx']:.0f} | "
              f"{d['m99']:.3f} | {fmt(cpu, 0)} |")
    print()
    for k, n, dd in [("p50", "SET p50 ms", 3), ("p99", "SET p99 ms", 3), ("p999", "SET p99.9 ms", 3), ("mx", "worst write ms", 0),
                     ("cpu", "engine CPU s", 0)]:
        reg("AOF-on writes", n, "lower", [d[k] for (e, _), d in al_sum.items() if e == "kividb-1.2.1"],
            [d[k] for (e, _), d in al_sum.items() if e == "kividb-1.2.0"], dd)

# ---------------------------------------------------------------------------------------------- against rc4
if REG:
    print("## 1.2.1 against 1.2.0\n")
    print("Median (min-max) over rounds. *Change* is the 1.2.1 median vs the 1.2.0 median; *spread* is the larger of the two builds' "
          f"(max-min)/median. A row is flagged when 1.2.1 is worse by more than the spread and by at least {FLOOR:g}% "
          "(memory counts repeat to a few tenths of a percent, so a spread near zero would otherwise flag a 0.2% "
          "difference).\n")
    print("| section | metric | 1.2.1 | 1.2.0 | change | spread | verdict |")
    print("|---|---|---|---|---|---|---|")
    flagged = []
    for section, metric, better, cv, rv, d in REG:
        cm, rm = med(cv), med(rv)
        if not rm:
            ch = None
        else:
            ch = 100 * (cm - rm) / abs(rm)
        sp = max(((max(x) - min(x)) / abs(med(x)) * 100) if med(x) else 0 for x in (cv, rv))
        worse = ch is not None and ((ch > 0) if better == "lower" else (ch < 0))
        if ch is None:
            verdict = "-"
        elif worse and abs(ch) > sp and abs(ch) < FLOOR:
            verdict = f"level (beyond spread, under {FLOOR:g}%)"
        elif worse and abs(ch) > sp and abs(cm - rm) > 1e-9:
            verdict = "**worse beyond spread**"
            flagged.append((section, metric, ch, sp))
        elif abs(ch) <= sp or abs(cm - rm) < 1e-9:
            verdict = "level"
        else:
            verdict = "better" if not worse else "level"
        print(f"| {section} | {metric} | {mr(cv, d)} | {mr(rv, d)} | {fmt(ch, 1) if ch is not None else '-'}% | {sp:.1f}% | {verdict} |")
    print()
    if flagged:
        print("Flagged: " + "; ".join(f"{s} {m} ({c:+.1f}%, spread {p:.1f}%)" for s, m, c, p in flagged) + "\n")
    # chart: each round of each build, normalised to rc4's median
    sel = [r for r in REG if med(r[4])]
    fig, ax = plt.subplots(figsize=(9, 0.28 * len(sel) + 1.2))
    for i, (section, metric, better, cv, rv, d) in enumerate(sel[::-1]):
        rm = med(rv)
        ax.scatter([x / rm for x in rv], [i + 0.12] * len(rv), color=ENG["kividb-1.2.0"][1], s=18, zorder=3,
                   label="1.2.0, each round" if i == 0 else None)
        ax.scatter([x / rm for x in cv], [i - 0.12] * len(cv), color=ENG["kividb-1.2.1"][1], s=18, zorder=3,
                   label=f"{CAND_LABEL}, each round" if i == 0 else None)
    ax.axvline(1.0, color="#555555", lw=1)
    ax.set_yticks(range(len(sel)), [f"{s}: {m}" + (" (higher is better)" if b == "higher" else "") for s, m, b, *_ in sel[::-1]], fontsize=7)
    ax.set_xlabel("value / 1.2.0 median")
    ax.grid(axis="y", visible=False)
    legend_right(ax, fontsize=8)
    ax.set_title("1.2.1 against 1.2.0, every metric of the suite (lower is better unless marked)", loc="left")
    save(fig, "regression-vs-1.2.0.svg")
