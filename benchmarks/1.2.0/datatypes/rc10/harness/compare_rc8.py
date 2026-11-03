"""compare_rc8.py SUMMARY RC8_ROUND_SUMMARY: rc10 (this session) against the rc8 round's published rc8 rows.

SUMMARY is this session's summary.csv (summarize.py), RC8_ROUND_SUMMARY the summary.csv of
benchmarks/1.2.0/datatypes/rc8/results (engine kividb-rc8, 6 runs, 2026-11-03 session). A
different session, so indicative only. Prints one Markdown table.
"""
import csv
import sys

CMDS = ["LPUSH", "LPOP", "LRANGE_100", "HSET", "HGET", "HGETALL", "SADD", "SISMEMBER", "ZADD", "ZRANGE", "INCR"]


def load(path):
    return {(r["engine"], r["command"], r["value_bytes"], r["pipeline"]): r for r in csv.DictReader(open(path))}


def fmt(x):
    x = float(x)
    return f"{x / 1e6:.2f}M" if x >= 1e6 else f"{x / 1e3:.0f}k"


cur, pub = load(sys.argv[1]), load(sys.argv[2])
print("| values, pipeline | command | rc10, this session (3 runs) | rc8, rc8 round (6 runs) | change | larger spread "
      "| p99 rc10 / rc8 round (ms) | p99.9 rc10 / rc8 round (ms) | outside spread |")
print("|---|---|---|---|---|---|---|---|---|")
for v in ["100", "2048"]:
    for p in ["1", "16"]:
        for c in CMDS:
            if c == "INCR" and v != "100":
                continue
            vv = "" if c == "INCR" else v
            a, b = cur.get(("kividb-rc10", c, vv, p)), pub.get(("kividb-rc8", c, vv, p))
            if not a or not b:
                continue
            d = (float(a["ops_per_sec_median"]) / float(b["ops_per_sec_median"]) - 1) * 100
            sp = max(float(a["spread_pct"]), float(b["spread_pct"]))
            print(f"| {'100 B' if v == '100' else '2 KB'}, p{p} | {c} | {fmt(a['ops_per_sec_median'])} ({a['spread_pct']}%) | "
                  f"{fmt(b['ops_per_sec_median'])} ({b['spread_pct']}%) | {d:+.1f}% | {sp:.1f}% | "
                  f"{float(a['p99_ms']):.2f} / {float(b['p99_ms']):.2f} | {float(a['p999_ms']):.2f} / {float(b['p999_ms']):.2f} | "
                  f"{'yes' if abs(d) > sp else 'no'} |")
