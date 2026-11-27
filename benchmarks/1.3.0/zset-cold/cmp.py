import json, sys
# cmp.py master/summary.json proto/summary.json
a = {r["key"]: r for r in json.load(open(sys.argv[1]))["rows"]}
b = {r["key"]: r for r in json.load(open(sys.argv[2]))["rows"]}
print("| metric | master | prototype | change | better |")
print("|---|---|---|---|---|")
for k in a:
    if k not in b:
        continue
    x = a[k]["values"].get("candidate"); y = b[k]["values"].get("candidate")
    if not isinstance(x, (int, float)) or not isinstance(y, (int, float)):
        continue
    ch = (y / x - 1) * 100 if x else 0.0
    sa = a[k].get("samples", {}).get("candidate")
    spread = ""
    if isinstance(sa, list) and len(sa) > 1 and min(sa) > 0:
        spread = " (%.1f%%)" % ((max(sa) - min(sa)) / sorted(sa)[len(sa) // 2] * 100)
    print("| %s | %.4g%s | %.4g | %+.1f%% | %s |" % (k, x, spread, y, ch, a[k]["better"]))
