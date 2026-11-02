#!/usr/bin/env python3
"""parse.py <results-dir> -- one CSV row per run (results/runs.csv) and a median/spread summary
(results/summary.csv) from the client's bench-<tag>.log and the server's final-<tag>.txt.

Tags are <engine>_<lfu|lru>_<F|S>_<rep>: F = the reporter's -benchtime=33554432x, S = 8388608x.
Like the reporter, per_memory_hit_rate and hit/s/mem are scaled by 1 GiB / the server's VmHWM
(the client cannot read a remote VmHWM and prints 0 kB)."""
import csv, os, re, statistics, sys

D = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "results")
RAW = os.path.join(D, "raw")
rows = []
for fn in sorted(os.listdir(RAW)):
    m = re.match(r"bench-(.+)\.log$", fn)
    if not m:
        continue
    tag = m[1]
    parts = tag.split("_")
    if len(parts) != 4:
        continue
    engine, pol, length, rep = parts
    b = open(os.path.join(RAW, fn)).read()
    fpath = os.path.join(RAW, f"final-{tag}.txt")
    f = open(fpath).read() if os.path.exists(fpath) else ""
    r = dict(tag=tag, engine=engine, policy="allkeys-" + pol, length=length, rep=int(rep))
    r["pair"] = (re.search(r"pair: (\S+)", b) or [None, ""])[1]
    r["exit"] = (re.search(r"exit=(\d+)", b) or [None, ""])[1]
    r["errors"] = b.count("got error")
    m = re.search(r"get:\s+(\d+)\s+hit:\s+(\d+)", b)
    if not m:
        r["status"] = "no result"
        rows.append(r)
        continue
    n, hit = int(m[1]), int(m[2])
    hwm = int(re.search(r"VmHWM:\s+(\d+) kB", f)[1])
    el = float(re.search(r"\n([\d.]+)s\s+output", b)[1])
    lat = [int(x) for x in re.search(
        r"P1:\s*(\d+) us\s+P50:\s*(\d+) us\s+P90:\s*(\d+) us\s+P99:\s*(\d+) us\s+P99.9:\s*(\d+) us", b).groups()]
    k = 1048576 / hwm
    r.update(status="ok" if r["exit"] == "0" and r["errors"] == 0 else "errors",
             gets=n, hits=hit, hit_rate=round(100 * hit / n, 3), vmhwm_kb=hwm,
             per_mem_hit_rate=round(100 * hit / n * k, 3), phase_s=el,
             gets_per_s=round(n / el), hits_per_s=round(hit / el), hit_per_s_mem=round(hit / el * k),
             p1_us=lat[0], p50_us=lat[1], p90_us=lat[2], p99_us=lat[3], p999_us=lat[4])
    um = re.search(r"^used_memory:(\d+)", f, re.M)
    rss = re.search(r"^used_memory_rss:(\d+)", f, re.M)
    r["used_memory_mib"] = round(int(um[1]) / 2**20) if um else ""
    r["rss_end_mib"] = round(int(rss[1]) / 2**20) if rss else ""
    ev = re.search(r"^evicted_keys:(\d+)", f, re.M)
    r["evicted_keys"] = int(ev[1]) if ev else ""
    kh = re.search(r"^keyspace_hits:(\d+)", f, re.M)
    km = re.search(r"^keyspace_misses:(\d+)", f, re.M)
    if kh and km:
        # warmup + measured phase each issue exactly N GETs; fewer means a phase ended early
        # (the harness stops a worker on its first error, e.g. a 30 s i/o timeout)
        r["server_gets"] = int(kh[1]) + int(km[1])
        r["server_gets_missing"] = 2 * n - r["server_gets"]
    st = re.search(r"stat: (\d+) (\d+)\n.*?age_s:\s*(\d+)", f, re.S)
    tck = re.search(r"CLK_TCK=(\d+)", f)
    if st and tck and int(st[3]) > 0:
        r["server_cpu_pct"] = round(100 * (int(st[1]) + int(st[2])) / int(tck[1]) / int(st[3]), 1)
    cpu = re.search(r"Percent of CPU this job got: (\d+)%", b)
    r["client_cpu_pct_of_300"] = int(cpu[1]) if cpu else ""
    rows.append(r)

cols = ["tag", "engine", "policy", "length", "rep", "pair", "status", "exit", "errors", "gets", "hits",
        "hit_rate", "per_mem_hit_rate", "vmhwm_kb", "phase_s", "gets_per_s", "hits_per_s", "hit_per_s_mem",
        "p1_us", "p50_us", "p90_us", "p99_us", "p999_us", "used_memory_mib", "rss_end_mib", "evicted_keys",
        "server_cpu_pct", "client_cpu_pct_of_300", "server_gets", "server_gets_missing"]
with open(os.path.join(D, "runs.csv"), "w", newline="\n") as fh:
    w = csv.DictWriter(fh, cols, extrasaction="ignore", lineterminator="\n")
    w.writeheader()
    for r in sorted(rows, key=lambda r: (r["length"], r["policy"], r["engine"], r["rep"])):
        w.writerow(r)

metrics = ["hit_rate", "per_mem_hit_rate", "vmhwm_kb", "gets_per_s", "hits_per_s", "hit_per_s_mem",
           "p50_us", "p90_us", "p99_us", "p999_us", "server_cpu_pct", "client_cpu_pct_of_300", "server_gets", "server_gets_missing"]
groups = {}
for r in rows:
    if "gets" in r:
        groups.setdefault((r["length"], r["policy"], r["engine"]), []).append(r)
with open(os.path.join(D, "summary.csv"), "w", newline="\n") as fh:
    w = csv.writer(fh, lineterminator="\n")
    w.writerow(["length", "policy", "engine", "runs", "runs_with_errors"] +
               [f"{m}_{s}" for m in metrics for s in ("median", "min", "max")])
    for key in sorted(groups):
        g = groups[key]
        out = list(key) + [len(g), sum(1 for r in g if r["status"] != "ok")]
        for m in metrics:
            v = [r[m] for r in g if r.get(m) not in (None, "")]
            out += [statistics.median(v), min(v), max(v)] if v else ["", "", ""]
        w.writerow(out)
print(f"{len(rows)} runs -> runs.csv, {len(groups)} groups -> summary.csv")
