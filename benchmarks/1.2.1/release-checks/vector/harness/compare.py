"""Side-by-side tables: 1.2.1 (this run), 1.2.0 (this run) and the earlier 1.2.1 build's
numbers published in benchmarks/1.2.1/vector/README.md (its "1.2.1 against 1.2.0" tables).
Usage: python compare.py <summary.json> <earlier README.md> <out.md>
Run from the harness directory (it imports charts.py's aggregation)."""
import json, re, statistics, sys

import charts as C

summary, readme, outp = sys.argv[1:4]
runs = json.load(open(summary))
SH = {"h-and-m": "h-and-m-2048-angular-filters", "dbpedia-100K": "dbpedia-openai-100K-1536-angular",
      "glove-25": "glove-25-angular", "dbpedia-1M": "dbpedia-openai-1M-1536-angular"}

num = lambda s: float(s.replace(",", ""))
old_pts, old_ing = {}, {}
for line in open(readme, encoding="utf-8"):
    c = [x.strip() for x in line.strip().strip("|").split("|")]
    if len(c) == 12 and c[0] in SH and c[1].isdigit():
        p99a = num(c[10].split("/")[1]); p99b = num(c[11].split("/")[1])
        old_pts[(SH[c[0]], int(c[1]), int(c[2]))] = dict(q120=num(c[3]), q121=num(c[4]), spread=c[6],
                                                          r120=num(c[8]), r121=num(c[9]), p99_120=p99a, p99_121=p99b)
    if len(c) == 7 and c[0] in SH and "(" in c[1]:
        old_ing[SH[c[0]]] = dict(i120=num(c[1].split()[0]), i121=num(c[2].split()[0]),
                                 b120=num(c[4]), b121=num(c[5]))

full = C.full_runs(runs)
L = ["## 1.2.1 (35a413c0) vs the earlier 1.2.1 build (ccba20e2) vs 1.2.0", "",
     "q/s = median of 3 passes (each pass: the median of 3 repetitions). `new` = this session; `prev` = the session of"
     " benchmarks/1.2.1/vector/README.md. Spread = the larger of the two builds' pass-to-pass spreads this session.", "",
     "| dataset | ef | clients | 1.2.1 new q/s | 1.2.0 new q/s | new 1.2.1/1.2.0 | spread | 1.2.1 prev q/s | 1.2.0 prev q/s | 1.2.1 new/prev | "
     "recall 1.2.1 new / prev / 1.2.0 new | p99 ms 1.2.1 new / prev / 1.2.0 new |",
     "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
inv = {v: k for k, v in SH.items()}
for ds in SH.values():
    a = C.search_agg(full, C.BASE, ds); b = C.search_agg(full, C.CAND, ds)
    for k in sorted(b, key=lambda k: (k[1], k[0])):
        if k not in a:
            continue
        sa, sb = a[k], b[k]
        o = old_pts.get((ds, k[0], k[1]), {})
        sp = max(sa["spread_pct"] or 0, sb["spread_pct"] or 0)
        f = lambda v, d=0: "-" if v is None else f"{v:,.{d}f}"
        L.append(f"| {inv[ds]} | {k[0]} | {k[1]} | {f(sb['median_rps'])} | {f(sa['median_rps'])} | "
                 f"{sb['median_rps'] / sa['median_rps']:.3f} | {sp:.1f}% | {f(o.get('q121'))} | {f(o.get('q120'))} | "
                 f"{(sb['median_rps'] / o['q121']) if o else float('nan'):.3f} | "
                 f"{f(sb['recall'], 4)} / {f(o.get('r121'), 4)} / {f(sa['recall'], 4)} | "
                 f"{f(sb['p99_ms'], 2)} / {f(o.get('p99_121'), 2)} / {f(sa['p99_ms'], 2)} |")

L += ["", "| dataset | ingest s 1.2.1 new (min-max) | 1.2.0 new (min-max) | new 1.2.1/1.2.0 | 1.2.1 prev | 1.2.0 prev | "
      "RSS B/vector 1.2.1 new / 1.2.0 new / 1.2.1 prev / 1.2.0 prev |", "|---|---:|---:|---:|---:|---:|---:|"]
for ds in SH.values():
    ing = {}
    for e in (C.CAND, C.BASE):
        v = [r["ingest_s"] for r in C.pick(full, e, ds) if r.get("ingest_s")]
        ing[e] = (statistics.median(v), min(v), max(v)) if v else (None, None, None)
    bb = {e: statistics.median(C.bpv_list(full, e, ds)) for e in (C.CAND, C.BASE)}
    o = old_ing.get(ds, {})
    c, b = ing[C.CAND], ing[C.BASE]
    L.append(f"| {inv[ds]} | {c[0]:.1f} ({c[1]:.1f}-{c[2]:.1f}) | {b[0]:.1f} ({b[1]:.1f}-{b[2]:.1f}) | {c[0] / b[0]:.3f} | "
             f"{o.get('i121', '-')} | {o.get('i120', '-')} | {bb[C.CAND]:,.0f} / {bb[C.BASE]:,.0f} / "
             f"{o.get('b121', 0):,.0f} / {o.get('b120', 0):,.0f} |")

L += ["", "Memory after deleting everything (dbpedia-1M, pass 3, 45 s linger; purge runs):", ""]
import csv, os
raw = os.path.join(os.path.dirname(summary), "raw")
for r in runs:
    if r["dataset"] == SH["dbpedia-1M"] and r.get("after_delete_rss") is not None and r["pass"] in ("p3", "purge"):
        um = r.get("after_delete_used")
        L.append(f"- {r['pass']} {r['engine']}: peak RSS {r['peak_rss'] / 2**30:.2f} GiB, RSS after delete "
                 f"{r['after_delete_rss'] / 2**30:.2f} GiB, used_memory after delete "
                 f"{'-' if um is None else f'{um / 2**20:.1f} MiB'}")
    if r["pass"] == "purge":
        d = os.path.join(raw, "purge", r["engine"], r["dataset"])
        try:
            tp = float(open(os.path.join(d, "purge.log")).read().split()[0])
            rows = [x for x in csv.DictReader(open(os.path.join(d, "mem.csv"))) if x["rss_kb"]]
            before = [x for x in rows if float(x["t"]) < tp][-1]
            after = [x for x in rows if float(x["t"]) >= tp + 1.0]
            a1 = after[0] if after else None
            L.append(f"  - MEMORY PURGE ({r['engine']}): RSS just before {int(before['rss_kb']) / 2**20:.2f} GiB"
                     f" (used_memory {int(before['used_memory'] or 0) / 2**20:.1f} MiB), >=1 s after "
                     f"{int(a1['rss_kb']) / 2**20:.2f} GiB" if a1 else "  - MEMORY PURGE: no sample after")
        except (OSError, IndexError, ValueError) as ex:
            L.append(f"  - purge parse failed for {r['engine']}: {ex}")
open(outp, "w", encoding="utf-8").write("\n".join(L) + "\n")
print("\n".join(L))
