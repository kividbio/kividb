"""tables.py SUMMARY_CSV [ENGINE...]: Markdown tables from summary.csv, one per
(value size, pipeline): ops/s median (spread) and p50/p99/p99.9 per engine."""
import csv
import sys

rows = list(csv.DictReader(open(sys.argv[1])))
engines = sys.argv[2:] or ["kividb-rc4", "kividb-1.1.3", "redis", "redis-cluster", "valkey", "dragonfly"]
NAMES = {"kividb-rc4": "KiviDB rc4", "kividb-1.1.3": "KiviDB 1.1.3", "redis": "Redis 8.10.3",
         "redis-cluster": "Redis Cluster x16", "valkey": "Valkey 9.1.3", "dragonfly": "Dragonfly 2.0.2",
         "kvrocks": "kvrocks 2.17.0"}
CMDS = ["LPUSH", "LPOP", "LRANGE_100", "HSET", "HGET", "HGETALL", "SADD", "SISMEMBER", "ZADD", "ZRANGE", "INCR"]
S = {(r["engine"], r["command"], r["value_bytes"], r["pipeline"]): r for r in rows}
engines = [e for e in engines if any(k[0] == e for k in S)]


def fmt(x):
    x = float(x)
    return f"{x / 1e6:.2f}M" if x >= 1e6 else f"{x / 1e3:.0f}k"


for v in ["100", "2048"]:
    for p in ["1", "16"]:
        print(f"\n#### {'100 B' if v == '100' else '2 KB'} values, pipeline {p}\n")
        print("ops/s: median of 3 runs (spread = (max-min)/median). Latency: p50 / p99 / p99.9 in ms. "
              "`*` = best ops/s in the row.\n")
        print("| command | " + " | ".join(NAMES[e] for e in engines) + " |")
        print("|---|" + "---|" * len(engines))
        for c in CMDS:
            vv = "" if c == "INCR" else v
            if c == "INCR" and v != "100":
                continue
            cells = [S.get((e, c, vv, p)) for e in engines]
            if not any(cells):
                continue
            best = max(float(x["ops_per_sec_median"]) for x in cells if x)
            out = []
            for x in cells:
                if not x:
                    out.append("-")
                    continue
                star = "*" if float(x["ops_per_sec_median"]) == best else ""
                out.append(f"{star}{fmt(x['ops_per_sec_median'])}{star} ({x['spread_pct']}%)<br>"
                           f"{float(x['p50_ms']):.2f} / {float(x['p99_ms']):.2f} / {float(x['p999_ms']):.2f}")
            print(f"| {c} | " + " | ".join(out) + " |")
