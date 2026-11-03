"""syncdiff.py <before> <after>: per node and in total, the change in sync_full / sync_partial_ok / sync_partial_err
(master side) between two syncs.sh listings. A node whose uptime went down restarted: its 'after' counts are since then."""
import re
import sys


def load(p):
    d = {}
    for line in open(p):
        m = re.match(r"(\S+-\d-\d) (.*)", line)
        if m:
            d[m.group(1)] = dict(re.findall(r"(\w+):(\S+)", m.group(2)))
    return d


b, a = load(sys.argv[1]), load(sys.argv[2])
tot = {"sync_full": 0, "sync_partial_ok": 0, "sync_partial_err": 0}
for n in sorted(a):
    x, y = b.get(n, {}), a[n]
    restarted = int(y.get("uptime_in_seconds", 0)) < int(x.get("uptime_in_seconds", 0))
    dd = {k: int(y.get(k, 0)) - (0 if restarted else int(x.get(k, 0))) for k in tot}
    for k in tot:
        tot[k] += dd[k]
    print(n, x.get("role"), "->", y.get("role"), " ".join(f"{k}+{v}" for k, v in dd.items()), "(restarted)" if restarted else "")
print("TOTAL", " ".join(f"{k}+{v}" for k, v in tot.items()))
