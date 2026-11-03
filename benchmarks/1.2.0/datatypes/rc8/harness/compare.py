"""compare.py SUMMARY PUBLISHED_SUMMARY: this session's medians against the published ones.

SUMMARY is this session's summary.csv (summarize.py), PUBLISHED_SUMMARY the suite's
results/summary.csv from the 2026-11-02 session (KiviDB 1.2.0 at 75482abb = engine
kividb-cand). Prints two Markdown tables:
  1. rc8 (this session) against the published 75482abb numbers, every row;
  2. the competitors at 100 B, pipeline 16, against their published rows, with rc8's ratio.
"""
import csv
import sys

CMDS = ["LPUSH", "LPOP", "LRANGE_100", "HSET", "HGET", "HGETALL", "SADD", "SISMEMBER", "ZADD", "ZRANGE", "INCR"]
COMP = [("redis", "Redis 8.10.3 (io-threads 8)"), ("redis-cluster", "Redis Cluster x16"),
        ("valkey", "Valkey 9.1.3 (io-threads 6)"), ("dragonfly", "Dragonfly 2.0.2")]


def load(path):
    out = {}
    for r in csv.DictReader(open(path)):
        out[(r["engine"], r["command"], r["value_bytes"], r["pipeline"])] = r
    return out


def fmt(x):
    x = float(x)
    return f"{x / 1e6:.2f}M" if x >= 1e6 else f"{x / 1e3:.0f}k"


def ms(x):
    return f"{float(x):.2f}"


cur, pub = load(sys.argv[1]), load(sys.argv[2])

print("### rc8 against the published 75482abb numbers\n")
print("| values, pipeline | command | rc8, this session (6 runs) | 75482abb, published (9 runs) | change | p99 rc8 / published (ms) | p99.9 rc8 / published (ms) |")
print("|---|---|---|---|---|---|---|")
for v in ["100", "2048"]:
    for p in ["1", "16"]:
        for c in CMDS:
            if c == "INCR" and v != "100":
                continue
            vv = "" if c == "INCR" else v
            a, b = cur.get(("kividb-rc8", c, vv, p)), pub.get(("kividb-cand", c, vv, p))
            if not a or not b:
                continue
            d = (float(a["ops_per_sec_median"]) / float(b["ops_per_sec_median"]) - 1) * 100
            print(f"| {'100 B' if v == '100' else '2 KB'}, p{p} | {c} | {fmt(a['ops_per_sec_median'])} ({a['spread_pct']}%) | "
                  f"{fmt(b['ops_per_sec_median'])} ({b['spread_pct']}%) | {d:+.1f}% | {ms(a['p99_ms'])} / {ms(b['p99_ms'])} | "
                  f"{ms(a['p999_ms'])} / {ms(b['p999_ms'])} |")

print("\n### Competitors at 100 B, pipeline 16: this session (3 runs) against published (3 runs)\n")
print("| command | " + " | ".join(n for _, n in COMP) + " | rc8 here | rc8 / Redis | rc8 / Valkey | rc8 / Dragonfly | rc8 / Cluster |")
print("|---|" + "---|" * (len(COMP) + 5))
for c in CMDS:
    vv = "" if c == "INCR" else "100"
    cells = []
    for e, _ in COMP:
        a, b = cur.get((e, c, vv, "16")), pub.get((e, c, vv, "16"))
        if not a:
            cells.append("-")
            continue
        s = f"{fmt(a['ops_per_sec_median'])} ({a['spread_pct']}%)"
        if b:
            d = (float(a["ops_per_sec_median"]) / float(b["ops_per_sec_median"]) - 1) * 100
            s += f"<br>pub {fmt(b['ops_per_sec_median'])}, {d:+.1f}%"
        cells.append(s)
    k = cur.get(("kividb-rc8", c, vv, "16"))
    ratios = []
    for e in ["redis", "valkey", "dragonfly", "redis-cluster"]:
        o = cur.get((e, c, vv, "16"))
        ratios.append(f"{float(k['ops_per_sec_median']) / float(o['ops_per_sec_median']):.2f}x" if k and o else "-")
    print(f"| {c} | " + " | ".join(cells) + f" | {fmt(k['ops_per_sec_median']) if k else '-'} | " + " | ".join(ratios) + " |")
