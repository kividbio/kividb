#!/usr/bin/env python3
"""Soak 2 (cluster) report: clreport.py <cl dir> <chart dir>  ->  <cl dir>/cl-summary.json, charts."""
import bisect
import csv
import glob
import json
import os
import re
import statistics as st
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

PREVIEW = os.environ.get("PREVIEW")  # also write PNGs there, to look at
_save = plt.Figure.savefig


def _savefig(self, fname, *a, **k):
    _save(self, fname, *a, **k)
    if PREVIEW and str(fname).endswith(".svg"):
        _save(self, os.path.join(PREVIEW, os.path.basename(str(fname))[:-4] + ".png"), dpi=80)


plt.Figure.savefig = _savefig

plt.rcParams.update({"svg.fonttype": "none", "figure.facecolor": "white", "axes.facecolor": "white",
                     "savefig.facecolor": "white", "font.size": 10})
KV, ORANGE, GREEN, GREY = "#0072B2", "#D55E00", "#009E73", "#666666"
D, C = sys.argv[1], sys.argv[2]
os.makedirs(C, exist_ok=True)
start = float(open(f"{D}/soak.start").read())
end = float(open(f"{D}/soak.end").read())
HOSTS = {"10.70.1.9": "c1", "10.70.1.10": "c2", "10.70.1.7": "c3"}
out = {"soak_minutes": round((end - start) / 60, 1)}

soak = []
for r in csv.DictReader(open(f"{D}/soak.csv")):
    soak.append({k: float(v) for k, v in r.items()})
out["ops_total"] = int(sum(r["ops"] for r in soak))
out["errors_total"] = int(sum(r["errors"] for r in soak))
out["ops_per_s_median"] = st.median(r["ops_per_s"] for r in soak)
for k in ("p50_us", "p99_us", "p999_us"):
    out[k + "_median_of_1s"] = st.median(r[k] for r in soak)
out["max_us"] = max(r["max_us"] for r in soak)

errlog = []
for line in open(f"{D}/soak-errs.log"):
    f = line.split()
    errlog.append((float(f[0]), {k: int(v) for k, v in (x.rsplit("=", 1) for x in f[1:])}))
kinds_total = {}
for _, d in errlog:
    for k, v in d.items():
        kinds_total[k] = kinds_total.get(k, 0) + v
out["error_kinds_total"] = kinds_total

events = []
for line in open(f"{D}/events"):
    ts, name = line.split(" ", 1)
    events.append((float(ts), name.strip()))
out["events_raw"] = [("%.1f min" % ((t - start) / 60), n) for t, n in events]

state = {}
for r in csv.reader(open(f"{D}/state.csv")):
    if len(r) < 4:
        continue
    state.setdefault(r[1], []).append((float(r[0]), r[2], r[3]))

ack = json.load(open(f"{D}/ack/summary.json"))
ack_t0 = float(open(f"{D}/ack/t0").read())
lost = json.load(open(f"{D}/ack/lost.json"))
tl = [r for r in csv.DictReader(open(f"{D}/ack/timeline.csv"))]
out["ack"] = {k: ack[k] for k in ("writers", "acked", "unacked_errors", "lost_acked", "wrong_value", "lost_ack_time_first", "lost_ack_time_last", "error_kinds")}

# group the events
groups = []
cur = None
for t, n in events:
    kind = n.split(" ", 1)[0]
    if kind.endswith("-start"):
        cur = {"type": kind[:-6], "start": t, "lines": [n]}
        groups.append(cur)
    elif cur is not None and kind.startswith(cur["type"]):
        cur["lines"].append(n)
        if kind.endswith("-end"):
            cur["end"] = t
        if kind in ("kill9-killed",):
            cur["kill_t"] = t
lost_abs = [(ack_t0 + x[2], x) for x in lost["missing"] + lost["wrong"]]
attributed = 0
rows = []
for g in groups:
    a, b = g["start"], g.get("end", g["start"] + 120) + 30
    srows = [r for r in soak if a <= start + r["t_s"] <= b]
    esecs = [start + r["t_s"] for r in srows if r["errors"] > 0]
    kinds = {}
    for t, d in errlog:
        if a <= t <= b:
            for k, v in d.items():
                kinds[k] = kinds.get(k, 0) + v
    abk = [(ack_t0 + float(r["t"]), int(r["err"])) for r in tl if a <= ack_t0 + float(r["t"]) <= b]
    aerr = [t for t, e in abk if e > 0]
    notok = {}
    for hp, seq in state.items():
        bad = [t for t, s, _ in seq if a <= t <= b and s != "ok"]
        if bad:
            notok[hp] = (round(min(bad) - a, 1), round(max(bad) - a, 1), len(bad))
    lw = [x for t, x in lost_abs if a - 15 <= t <= b]
    attributed += len(lw)
    info = {}
    for line in g["lines"]:
        m = re.search(r"after ([\d.]+|timeout) s", line)
        if m:
            info[line.split(" ", 1)[0]] = m.group(1)
    rec = {"event": g["type"], "at_min": round((a - start) / 60, 1), "detail": g["lines"][0], "timings_s": info,
           "duration_to_cluster_ok_s": round(g["end"] - a, 1) if "end" in g else None,
           "soakgen_ops_in_window": int(sum(r["ops"] for r in srows)), "soakgen_errors": int(sum(r["errors"] for r in srows)),
           "soakgen_error_kinds": kinds, "soakgen_error_seconds": len(esecs),
           "soakgen_first_last_error_s_after_start": (round(min(esecs) - a, 1), round(max(esecs) - a, 1)) if esecs else None,
           "ackcheck_errors": sum(e for _, e in abk), "ackcheck_first_last_error_s_after_start": (round(min(aerr) - a, 1), round(max(aerr) - a, 1)) if aerr else None,
           "cluster_state_not_ok_per_node": notok,
           "p999_ms_worst_1s": round(max((r["p999_us"] for r in srows), default=0) / 1000, 1),
           "lost_acked_in_window": len(lw)}
    if lw and "kill_t" in g:
        rel = sorted(round(t - g["kill_t"], 3) for t, x in lost_abs if a - 15 <= t <= b)
        rec["lost_ack_time_vs_kill_s"] = {"earliest": rel[0], "latest": rel[-1]}
    rows.append(rec)
out["events"] = rows
out["lost_outside_event_windows"] = len(lost_abs) - attributed
out["lost_outside_sample"] = [x for t, x in lost_abs if not any(g["start"] - 15 <= t <= g.get("end", g["start"] + 120) + 30 for g in groups)][:20]

# compare + checks
out["compare"] = {}
for p in sorted(glob.glob(f"{D}/compare/shard-*.out")):
    out["compare"][os.path.basename(p)] = open(p).read().strip().splitlines()[-1:]
try:
    out["compare_offsets"] = open(f"{D}/compare/offsets.txt").read().splitlines()
    out["check_end"] = [l for l in open(f"{D}/check-end.txt").read().splitlines() if l.startswith("[")]
except Exception as e:
    out["check_end"] = repr(e)

# memory per node
mem = {}
for p in sorted(glob.glob(f"{D}/10.*-samples-*.csv")):
    ip = os.path.basename(p).split("-")[0]
    port = p.rsplit("-", 1)[1][:4]
    rows_ = []
    for r in csv.DictReader(open(p)):
        try:
            rows_.append((float(r["ts"]) - start, int(r["used_memory"]), int(r["used_memory_rss"] or 0), r["role"], int(r["keys"] or 0),
                          int(r.get("value_slab_bytes") or 0), int(r.get("value_slab_live_bytes") or 0)))
        except (ValueError, TypeError):
            pass
    mem[f"{HOSTS.get(ip, ip)}:{port}"] = rows_
L = end - start
out["memory"] = {}
for n, rr in mem.items():
    load = [x for x in rr if 600 <= x[0] <= L]
    if not load:
        continue
    out["memory"][n] = {"used_gb_min": round(min(x[1] for x in load) / 1e9, 3), "used_gb_max": round(max(x[1] for x in load) / 1e9, 3),
                        "used_gb_end": round(load[-1][1] / 1e9, 3), "rss_gb_max": round(max(x[2] for x in load) / 1e9, 3),
                        "rss_gb_end": round(load[-1][2] / 1e9, 3), "rss_gb_at_10min": round(load[0][2] / 1e9, 3),
                        "slab_gb_end": round(load[-1][5] / 1e9, 3), "slab_live_gb_end": round(load[-1][6] / 1e9, 3), "roles_seen": sorted({x[3] for x in rr if x[3]}),
                        "keys_at_10min": load[0][4], "keys_end": load[-1][4],
                        "used_bytes_per_key_at_10min": round(load[0][1] / max(load[0][4], 1)),
                        "used_bytes_per_key_end": round(load[-1][1] / max(load[-1][4], 1))}
logs_bad = {}
for p in glob.glob(f"{D}/server-*.log"):
    bad = [l.strip() for l in open(p, errors="replace") if re.search(r"panic|\[ERROR\]", l)]
    logs_bad[os.path.basename(p)] = {"count": len(bad), "first": bad[:5]}
out["server_log_error_lines"] = logs_bad
json.dump(out, open(f"{D}/cl-summary.json", "w"), indent=1)


def evmarks(ax, y, labels=True):
    short = {"failover": "CLUSTER\nFAILOVER", "kill9": "kill -9\nmaster", "replica-restart": "replica\nrestart",
             "reshard": "redis-cli\nreshard", "import": "MIGRATION\nIMPORT"}
    for g in groups:
        if g["type"] not in short:
            continue
        ax.axvline((g["start"] - start) / 60, color=GREY, lw=1, ls="--")
        if labels:
            yy = y if g["type"] != "import" else (y * 0.004 if ax.get_yscale() == "log" else y * 0.7)
            ax.text((g["start"] - start) / 60 + 0.6, yy, short.get(g["type"], g["type"]), fontsize=8, color=GREY, va="top")


fig, (ax, ax2) = plt.subplots(2, 1, figsize=(10, 6), sharex=True, gridspec_kw={"height_ratios": [3, 1.2]})
tt = [r["t_s"] / 60 for r in soak]
ax.plot(tt, [r["p50_us"] / 1000 for r in soak], color=KV, lw=1, label="p50")
ax.plot(tt, [r["p99_us"] / 1000 for r in soak], color=ORANGE, lw=0.8, label="p99")
ax.plot(tt, [r["p999_us"] / 1000 for r in soak], color=GREEN, lw=0.6, label="p99.9")
ax.set_yscale("log")
top = max(r["p999_us"] for r in soak) / 1000 * 5
ax.set_ylim(0.05, top)
evmarks(ax, top * 0.9)
ax.set_ylabel("ms per 1 s interval (log)")
ax.set_title("KiviDB 1.2.0-rc7, cluster soak (3 shards x 2, AOF everysec, 100,000 ops/s): latency and errors")
ax.legend(loc="lower right", frameon=False, ncol=3)
ax.grid(alpha=.3, which="both")
ax2.bar(tt, [r["errors"] for r in soak], width=1 / 60, color=KV)
evmarks(ax2, 0, labels=False)
ax2.set_ylabel("errors / s")
ax2.set_xlabel("minutes since soak start")
ax2.grid(alpha=.3)
fig.tight_layout()
fig.savefig(f"{C}/cl-latency-errors.svg")

fig, ax = plt.subplots(figsize=(10, 4.6))
cols = {"c1": KV, "c2": ORANGE, "c3": GREEN}
for n, rr in sorted(mem.items()):
    ax.plot([x[0] / 60 for x in rr], [x[1] / 1e9 for x in rr], color=cols.get(n[:2], GREY), lw=1.6 if n.endswith("7001") else 1.1,
            ls="-" if n.endswith("7001") else "--", label=f"{n} used_memory")
    ax.plot([x[0] / 60 for x in rr], [x[2] / 1e9 for x in rr], color=cols.get(n[:2], GREY), lw=0.8, ls=":")
top = max(x[2] for rr in mem.values() for x in rr) / 1e9 * 1.35
ax.set_ylim(0, top)
evmarks(ax, top * 0.98)
ax.axvline(L / 60, color=GREY, lw=1)
ax.set_xlabel("minutes since soak start (each node changes role at failovers; :7001 started as masters)")
ax.set_ylabel("GB")
ax.set_title("KiviDB 1.2.0-rc7, cluster soak: used_memory (solid) and RSS (dotted) per node")
ax.legend(loc="lower right", frameon=False, ncol=3, fontsize=8)
ax.grid(alpha=.3)
fig.tight_layout()
fig.savefig(f"{C}/cl-memory.svg")
print(json.dumps(out, indent=1, default=str)[:8000])
