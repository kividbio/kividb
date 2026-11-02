#!/usr/bin/env python3
"""Markdown tables for the README from summary.csv and runs.csv.

    python3 tables.py <results-dir>  > tables.md
"""
import csv
import os
import statistics
import sys

ROOT = sys.argv[1]
rows = list(csv.DictReader(open(os.path.join(ROOT, "summary.csv"))))
runs = [r for r in csv.DictReader(open(os.path.join(ROOT, "runs.csv"))) if not r.get("error")]
D = {(r["engine"], r["size"], r["scenario"]): r for r in rows}
CAND_LABEL = os.environ.get("CAND_LABEL", "KiviDB 1.2.0 (75482ab)")
ENG = [("kividb-cand", CAND_LABEL), ("kividb-113", "KiviDB 1.1.3"), ("redis@24", "Redis 8.10.3, io-threads 24"),
       ("redis-cluster", "Redis 8.10.3 Cluster, 32 shards"), ("valkey@24", "Valkey 9.1.3, io-threads 24"),
       ("dragonfly", "Dragonfly 2.0.2"), ("garnet", "Garnet 2.2.5")]
SIZE = {"100": "100 B", "1024": "1 KB", "2048": "2 KB"}
FLAG = {"client": "C", "nic": "N", "client+nic": "C N"}


def m(v):
    v = float(v)
    return f"{v / 1e6:.2f}M" if v >= 1e6 else f"{v / 1e3:.0f}K"


def cell(r):
    if not r:
        return "—"
    f = FLAG.get(r["limit"], "")
    return (f"{m(r['ops_median'])} ±{float(r['spread_pct']) / 2:.1f}%{' ' + f if f else ''}<br>"
            f"{float(r['p50_ms']):.2f} / {float(r['p99_ms']):.2f} / {float(r['p999_ms']):.2f}")


WL = [("get_p1", "GET p1"), ("get_p16", "GET p16"), ("set_p1", "SET p1"), ("set_p16", "SET p16")]
for size in ("100", "1024", "2048"):
    print(f"\n#### {SIZE[size]} values\n")
    print("| Engine | " + " | ".join(l for _, l in WL) + " |")
    print("|---|" + "---:|" * len(WL))
    for e, label in ENG:
        print(f"| {label} | " + " | ".join(cell(D.get((e, size, sc))) for sc, _ in WL) + " |")

print("\n#### MGET / MSET of 10 keys, 100 B, pipeline 1 (commands/s; keys/s is ten times that)\n")
print("| Engine | MGET 10 keys | MSET 10 keys |")
print("|---|---:|---:|")
for e, label in ENG:
    if e == "redis-cluster":
        continue
    print(f"| {label} | {cell(D.get((e, '100', 'mget10_p1')))} | {cell(D.get((e, '100', 'mset10_p1')))} |")

print("\n#### GET throughput by pipeline depth, 100 B, 300 connections\n")
print("| Engine | depth 1 | depth 4 | depth 16 | depth 64 |")
print("|---|---:|---:|---:|---:|")
for e, label in ENG:
    vals = []
    for d in (1, 4, 16, 64):
        r = D.get((e, "100", "get_p16" if d == 16 else f"sweep_get_p{d}"))
        vals.append(m(r["ops_median"]) + (" " + FLAG[r["limit"]] if r and r["limit"] else "") if r else "—")
    print(f"| {label} | " + " | ".join(vals) + " |")

print("\n#### Server CPU per request, GET 100 B, 300 connections (µs, user + kernel)\n")
print("| Engine | depth 1 | depth 4 | depth 16 | depth 64 |")
print("|---|---:|---:|---:|---:|")
for e, label in ENG:
    if e in ("kividb-113",):
        continue
    vals = []
    for d in (1, 4, 16, 64):
        r = D.get((e, "100", "get_p16" if d == 16 else f"sweep_get_p{d}"))
        vals.append(f"{float(r['us_user_per_op']):.2f} + {float(r['us_kernel_per_op']):.2f}" if r else "—")
    print(f"| {label} | " + " | ".join(vals) + " |")


def rr(e, s, sc, k):
    return [float(r[k]) for r in runs if r["engine"] == e and r["size"] == s and r["scenario"] == sc and r[k] != ""]


def against(old, old_label, new, new_label, p99=True):
    print(f"\n#### {new_label} against {old_label} (median, [min - max] of the rounds)\n")
    head = f"| Scenario | {old_label} | {new_label} | Change | Beyond spread? |"
    if p99:
        head += " p99 ms, old → new | server µs/req, old → new |"
    print(head)
    print("|---|---:|---:|---:|---|" + ("---:|---:|" if p99 else ""))
    scs = [(s, sc) for s in ("100", "1024", "2048") for sc, _ in WL] + [("100", "mget10_p1"), ("100", "mset10_p1")]
    for s, sc in scs:
        o, n = D.get((old, s, sc)), D.get((new, s, sc))
        if not (o and n):
            continue
        ch = 100 * (float(n["ops_median"]) / float(o["ops_median"]) - 1)
        worse = float(n["ops_max"]) < float(o["ops_min"])
        better = float(n["ops_min"]) > float(o["ops_max"])
        name = f"{SIZE[s]} {sc.split('_')[0].upper()} p{sc.split('_p')[1]}"
        nic = " (NIC)" if "nic" in n["limit"] and not (worse or better) else ""
        line = (f"| {name} | {m(o['ops_median'])} [{m(o['ops_min'])} - {m(o['ops_max'])}] ({o['rounds']}) | "
                f"{m(n['ops_median'])} [{m(n['ops_min'])} - {m(n['ops_max'])}] ({n['rounds']}) | {ch:+.1f}% | "
                f"{'**slower**' if worse else 'faster' if better else 'no'}{nic} |")
        if p99:
            po, pn = rr(old, s, sc, "p99_ms"), rr(new, s, sc, "p99_ms")
            line += (f" {statistics.median(po):.2f} → {statistics.median(pn):.2f} |"
                     f" {float(o['us_per_op']):.2f} → {float(n['us_per_op']):.2f} |")
        print(line)


against("kividb-rc4", "1.2.0-rc4", "kividb-cand", "1.2.0 (75482ab)")
against("kividb-113", "1.1.3", "kividb-cand", "1.2.0 (75482ab)", p99=False)
