import json, statistics, sys
# usage: summ.py base.jsonl cand.jsonl
CASES = ["bytes_per_entry", "zscore_hit_ns", "zscore_miss_ns", "zrank_ns", "zrange_ns", "zrevrange_ns",
         "zincrby_ns", "zadd_zrem_ns", "feed_ns", "bgsave_s", "load_s"]

def load(p):
    d = {}
    for line in open(p):
        r = json.loads(line)
        for c in CASES:
            d.setdefault((r["n"], c), []).append(r[c])
    return d

a = load(sys.argv[1])
b = load(sys.argv[2]) if len(sys.argv) > 2 else None
sizes = sorted({n for n, _ in a})
print("| n | case | base median (spread) | cand median (spread) | cand/base |")
print("|---|---|---|---|---|")
for n in sizes:
    for c in CASES:
        x = a.get((n, c))
        if not x:
            continue
        mx = statistics.median(x); sx = (max(x) - min(x)) / mx * 100
        row = "| %d | %s | %.1f (%.1f%%) |" % (n, c.replace("_ns", ""), mx, sx)
        y = b.get((n, c)) if b else None
        if y:
            my = statistics.median(y); sy = (max(y) - min(y)) / my * 100
            row += " %.1f (%.1f%%) | %+.1f%% |" % (my, sy, (my / mx - 1) * 100)
        else:
            row += " | |"
        print(row)
