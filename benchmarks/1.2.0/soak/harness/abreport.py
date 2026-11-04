#!/usr/bin/env python3
"""abreport.py <dir with one sub-directory per run>: median p50/p99/p99.9 of the 10 s intervals (first 60 s dropped),
checkpoint count and median duration (master log), master CPU and used_memory, per run."""
import csv, datetime as dt, glob, os, re, statistics as st, sys
print("| run | ops/s | p50 ms | p99 ms | p99.9 ms | checkpoints | checkpoint median s | master CPU % | master used_memory GB (end) |")
print("|---|---|---|---|---|---|---|---|---|")
for d in sorted(glob.glob(os.path.join(sys.argv[1], "*"))):
    if not os.path.isdir(d) or not os.path.exists(d + "/soak.csv"):
        continue
    soak = [r for r in csv.DictReader(open(d + "/soak.csv"))][6:]
    if not soak:
        continue
    med = lambda k: st.median(float(r[k]) for r in soak)
    ck, cur = [], None
    for l in open(d + "/server-master.log", errors="replace"):
        m = re.match(r"(\S+)Z \d+ \[\w+\] (.*)", l)
        if not m:
            continue
        t = dt.datetime.fromisoformat(m.group(1)).timestamp()
        if "auto-checkpoint:" in m.group(2):
            cur = t
        elif "rebased onto" in m.group(2) and cur:
            ck.append(t - cur)
            cur = None
    sm = [r for r in csv.DictReader(open(d + "/samples-master.csv"))] if os.path.exists(d + "/samples-master.csv") else []
    cpu = st.median(float(r["cpu_pct"]) for r in sm[3:] if r["cpu_pct"]) if len(sm) > 4 else None
    used = int(sm[-1]["used_memory"]) / 1e9 if sm and sm[-1]["used_memory"] else None
    print("| %s | %.0f | %.3f | %.1f | %.1f | %d | %s | %s | %s |" % (os.path.basename(d), med("ops_per_s"), med("p50_us") / 1000,
          med("p99_us") / 1000, med("p999_us") / 1000, len(ck), "%.2f" % st.median(ck) if ck else "-", "%.0f" % cpu if cpu else "-",
          "%.3f" % used if used else "-"))
