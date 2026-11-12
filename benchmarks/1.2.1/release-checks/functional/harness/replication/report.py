#!/usr/bin/env python3
"""report.py <results dir>...: one CSV row per run (fo_run.sh / sen_run.sh / eq.sh layouts) on stdout.

unavail_s: 0.1 s buckets, from the first event on, in which acknowledged writes ran below half the median
rate of the 10 s before it (the failover README's definition), counted up to the end of the run.
"""
import csv
import glob
import json
import os
import sys


def unavail(d, ev):
    rows = list(csv.DictReader(open(os.path.join(d, "timeline.csv"))))
    ok = [int(r["ok"]) for r in rows]
    eb = int(ev / 0.1)
    pre = sorted(ok[max(0, eb - 100):eb]) or [0]
    base = pre[len(pre) // 2]
    # ignore the last 0.5 s (writers stopping)
    return round(sum(1 for x in ok[eb:len(ok) - 5] if x < 0.5 * base) * 0.1, 1), base


def jl(p):
    try:
        return json.load(open(p))
    except Exception:
        return None


w = csv.writer(sys.stdout)
w.writerow(["suite", "engine", "scenario", "rep", "acked", "lost_acked", "wrong", "wait_acked", "wait_acked_lost",
            "unavail_s", "recovery_s", "base_per_100ms", "converge_real_diff", "kvdiff_real_diff", "digest_equal",
            "digestvalue_equal", "extra"])
for root in sys.argv[1:]:
    for s in sorted(glob.glob(os.path.join(root, "*", "*", "*", "summary.json"))):
        d = os.path.dirname(s)
        rep = os.path.basename(d)
        scen = os.path.basename(os.path.dirname(d))
        eng = os.path.basename(os.path.dirname(os.path.dirname(d)))
        j = jl(s)
        ev = j["events"][0][0] if j["events"] else 0
        u, base = unavail(d, ev)
        conv = jl(os.path.join(d, "converge.json"))
        kvs = [jl(p) for p in sorted(glob.glob(os.path.join(d, "kvdiff*.json")))]
        kvs = [k for k in kvs if k]
        extra = ""
        if os.path.exists(os.path.join(d, "new-master.txt")):
            extra = open(os.path.join(d, "new-master.txt")).read().strip().replace("master by Sentinel after: ", "new=")
            pr = os.path.join(d, "priorities.txt")
            if os.path.exists(pr):
                extra += " | " + " ".join(l.split()[0] + ":" + l.split()[1] for l in open(pr) if l.strip())
        w.writerow(["sentinel" if "sen-" in root else "fo", eng, scen, rep, j["acked"], j["lost_acked"], j["wrong_value"],
                    j["wait_acked"], j["wait_acked_lost"], u, (j.get("recovery") or {}).get("recovery_s"), base,
                    conv["real_differences"] if conv else "",
                    "/".join(str(k["real_differences"]) for k in kvs),
                    "/".join("y" if k["digest_equal"] else "n" for k in kvs),
                    "/".join("y" if k["digestvalue_equal"] else "n" for k in kvs), extra])
