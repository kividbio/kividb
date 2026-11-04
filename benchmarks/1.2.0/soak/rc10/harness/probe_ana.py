#!/usr/bin/env python3
"""probe_ana.py <run dir>: write latency during each `redis-cli --cluster reshard` of a cl_soak.sh run (#310).

For each reshard round (events "reshard-start round N src-shard=S dst-shard=D" ... "reshard-end round N"):
  - the SOURCE shard's write probe (shardprobe, 500 SET/s on 8 connections, keys in slots no event moves): every
    operation scheduled inside the reshard, from the raw log: count, errors, p50 / p99 / p99.9 / worst; the same for
    the 60 s before the reshard (baseline); the same for the service time (each request from when its connection
    could send it to its reply: the open-loop latency without the queue behind a stalled request); and the median and worst of the 1 s p99s (the measure #310 quotes);
    seconds with a stalled probe connection (one operation over 100 ms) and the most connections stalled at once;
  - the destination's probe and the third shard's probe over the same window, as controls;
  - the mixed soakgen (all shards, 100k ops/s): median ops/s and median 1 s p99 inside the reshard;
  - MIGRATE / CLUSTER SETSLOT on the source: calls and mean time from INFO commandstats before/after; SLOWLOG
    entries (over 10 ms) by command.
Writes <run dir>/reshard-latency.json and prints a table. Probes: a = shard of slot 3000, b = 8000, c = 13000.
"""
import csv
import json
import os
import re
import statistics
import sys

d = sys.argv[1]
PROBE = {"3000": "a", "8000": "b", "13000": "c"}

rounds = {}
for line in open(os.path.join(d, "events")):
    m = re.match(r"(\S+) reshard-start round (\d+) src-shard=(\d+) dst-shard=(\d+)", line)
    if m:
        rounds[int(m.group(2))] = {"start": float(m.group(1)), "src": m.group(3), "dst": m.group(4)}
    m = re.match(r"(\S+) reshard-end round (\d+) exit=(\d+) after (\S+) s", line)
    if m:
        rounds[int(m.group(2))].update(end=float(m.group(1)), exit=int(m.group(3)))


def pct(v, q):
    if not v:
        return None
    i = min(len(v) - 1, max(0, int(round(q / 100 * len(v) + 0.5)) - 1))
    return v[i]


raw = {}


def load_raw(p):
    if p not in raw:
        rows = []
        for line in open(os.path.join(d, "probe-%s.raw" % p)):
            f = line.split()
            if len(f) == 4:
                rows.append((int(f[0]) / 1e6, int(f[1]), int(f[3]), int(f[2])))
        # service time: what the request itself took, from when the connection could send it (its scheduled start,
        # or the previous request's reply, whichever is later) to its reply; open-loop latency minus queueing
        last = {}
        svc = []
        for sched, lat, err, conn in sorted(rows, key=lambda x: (x[3], x[0])):
            done = sched + lat / 1e6
            svc.append((sched, max(1, int((done - max(sched, last.get(conn, 0))) * 1e6)), err, conn))
            last[conn] = done
        raw[p] = rows
        raw[p + "-svc"] = svc
    return raw[p]


def window(p, a, b, svc=False):
    load_raw(p)
    rows = raw[p + "-svc"] if svc else raw[p]
    lat = sorted(x[1] for x in rows if a <= x[0] < b)
    err = sum(x[2] for x in rows if a <= x[0] < b)
    return {"ops": len(lat), "errors": err, "p50_us": pct(lat, 50), "p99_us": pct(lat, 99), "p999_us": pct(lat, 99.9),
            "worst_us": lat[-1] if lat else None}


def per_second(p, a, b):
    rows = [r for r in csv.DictReader(open(os.path.join(d, "probe-%s.csv" % p))) if a < float(r["unix_ts"]) <= b + 1]
    if not rows:
        return {}
    p99 = [int(r["p99_us"]) for r in rows]
    st = [int(r["stalled_conns"]) for r in rows]
    return {"seconds": len(rows), "p99_us_median_1s": statistics.median(p99), "p99_us_worst_1s": max(p99),
            "seconds_with_stall": sum(1 for x in st if x > 0), "max_stalled_conns": max(st)}


def soak(a, b):
    t0 = float(open(os.path.join(d, "soak.start")).read())
    rows = [r for r in csv.DictReader(open(os.path.join(d, "soak.csv"))) if a < t0 + float(r["t_s"]) <= b + 1]
    if not rows:
        return {}
    return {"ops_per_s_median": statistics.median(int(r["ops_per_s"]) for r in rows),
            "p99_us_median_1s": statistics.median(int(r["p99_us"]) for r in rows),
            "max_us": max(int(r["max_us"]) for r in rows), "errors": sum(int(r["errors"]) for r in rows)}


def cmdstats(path):
    out = {}
    if not os.path.exists(path):
        return out
    for line in open(path):
        m = re.match(r"cmdstat_([^:]+):calls=(\d+),usec=(\d+)", line.strip())
        if m:
            out[m.group(1)] = (int(m.group(2)), int(m.group(3)))
    return out


def cmd_delta(rd, side):
    a, b = cmdstats(os.path.join(rd, "cmdstats-%s-before.txt" % side)), cmdstats(os.path.join(rd, "cmdstats-%s-after.txt" % side))
    out = {}
    for k, (c, u) in b.items():
        c0, u0 = a.get(k, (0, 0))
        if c - c0 > 0:
            out[k] = {"calls": c - c0, "mean_us": round((u - u0) / (c - c0), 1)}
    return out


def slow(rd, side):
    p = os.path.join(rd, "slowlog-%s.json" % side)
    if not os.path.exists(p):
        return {}
    xs = json.load(open(p))
    if isinstance(xs, dict):
        return xs
    by = {}
    for x in xs:
        by.setdefault(x["cmd"], []).append(x["us"])
    return {k: {"n": len(v), "median_us": sorted(v)[len(v) // 2], "max_us": max(v)} for k, v in by.items()}


res = []
for n in sorted(rounds):
    r = rounds[n]
    if "end" not in r:
        continue
    a, b = r["start"], r["end"]
    src, dst = PROBE[r["src"]], PROBE[r["dst"]]
    other = ({"a", "b", "c"} - {src, dst}).pop()
    rd = os.path.join(d, "reshard-%d" % n)
    x = {"round": n, "src_shard": r["src"], "dst_shard": r["dst"], "exit": r["exit"], "seconds": round(b - a, 1),
         "source_baseline_60s": window(src, a - 60, a), "source": window(src, a, b),
         "source_service": window(src, a, b, svc=True), "source_service_baseline_60s": window(src, a - 60, a, svc=True), "source_1s": per_second(src, a, b),
         "destination": window(dst, a, b), "destination_1s": per_second(dst, a, b),
         "third_shard": window(other, a, b), "third_shard_1s": per_second(other, a, b),
         "mixed": soak(a, b), "cmd_source": cmd_delta(rd, "src"), "cmd_destination": cmd_delta(rd, "dst"),
         "slowlog_source": slow(rd, "src"), "slowlog_destination": slow(rd, "dst")}
    res.append(x)
json.dump(res, open(os.path.join(d, "reshard-latency.json"), "w"), indent=1)


def ms(v):
    return "-" if v is None else ("%.2f" % (v / 1000) if v < 100_000 else "%.0f" % (v / 1000))


print("round src->dst exit secs | source probe: base p99 | p50 p99 p99.9 worst (ms) | 1s-p99 median worst | stall s, max conns | dest p99 worst | third p99 worst | mixed ops/s p99med")
for x in res:
    s, s1, b0 = x["source"], x["source_1s"], x["source_baseline_60s"]
    print("%d %s->%s %d %.0f | %s | %s %s %s %s | %s %s | %s %s | %s %s | %s %s | %s %s" % (
        x["round"], x["src_shard"], x["dst_shard"], x["exit"], x["seconds"], ms(b0["p99_us"]),
        ms(s["p50_us"]), ms(s["p99_us"]), ms(s["p999_us"]), ms(s["worst_us"]),
        ms(s1.get("p99_us_median_1s")), ms(s1.get("p99_us_worst_1s")), s1.get("seconds_with_stall"), s1.get("max_stalled_conns"),
        ms(x["destination"]["p99_us"]), ms(x["destination"]["worst_us"]), ms(x["third_shard"]["p99_us"]), ms(x["third_shard"]["worst_us"]),
        x["mixed"].get("ops_per_s_median"), ms(x["mixed"].get("p99_us_median_1s"))))
    sv = x["source_service"]
    print("   source service time p50 %s p99 %s p99.9 %s worst %s ms (baseline p99 %s)" % (ms(sv["p50_us"]), ms(sv["p99_us"]), ms(sv["p999_us"]), ms(sv["worst_us"]), ms(x["source_service_baseline_60s"]["p99_us"])))
    print("   source cmds:", json.dumps(x["cmd_source"]), " slowlog:", json.dumps(x["slowlog_source"]))
