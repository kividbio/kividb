#!/usr/bin/env python3
"""Soak 1 report: s1report.py <s1 dir> <chart dir>  ->  <s1 dir>/s1-summary.json and the charts.

Reads soak.csv (soakgen, 10 s), samples-master/replica.csv (sampler.sh, 10 s), events, wl/ (writelog.py),
compare.json, restart/, and the two server logs (checkpoint durations)."""
import csv
import datetime as dt
import glob
import json
import os
import re
import statistics as st
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

PREVIEW = os.environ.get("PREVIEW")
_save = plt.Figure.savefig


def _savefig(self, fname, *a, **k):
    _save(self, fname, *a, **k)
    if PREVIEW and str(fname).endswith(".svg"):
        _save(self, os.path.join(PREVIEW, os.path.basename(str(fname))[:-4] + ".png"), dpi=80)


plt.Figure.savefig = _savefig
plt.rcParams.update({"svg.fonttype": "none", "figure.facecolor": "white", "axes.facecolor": "white",
                     "savefig.facecolor": "white", "font.size": 10})
KV, ORANGE, GREEN, PURPLE, GREY = "#0072B2", "#D55E00", "#009E73", "#CC79A7", "#666666"
D, C = sys.argv[1], sys.argv[2]
os.makedirs(C, exist_ok=True)
start = float(open(f"{D}/soak.start").read())
end = float(open(f"{D}/soak.end").read())
L = end - start
out = {"soak_minutes": round(L / 60, 1)}
TITLE = "KiviDB 1.2.0-rc7, standalone soak (master + replica, AOF everysec, 100,000 ops/s)"

# ---- events
events = []
for line in open(f"{D}/events"):
    ts, name = line.split(" ", 1)
    events.append((float(ts) - start, name.strip()))
out["events"] = [("%.2f min" % (t / 60), n) for t, n in events]
MARKS = [(t, lab) for t, n in events for key, lab in (("master-restart-start", "master\nrestart\n(SIGTERM)"),
                                                     ("replica-kill9-start", "kill -9\nreplica")) if n.split()[0] == key]


# the replica's full-sync loop after the master restart (link flapping): from the restart to the end of the load
_rl = next((t for t, n in events if n.startswith("master-restart-replica-linked")), None)
LOOP = (_rl, L) if _rl is not None and any(n.startswith("replica-sync-loop") for _, n in events) else None


def evmarks(ax, ytext, inside=True):
    if LOOP:
        ax.axvspan(LOOP[0] / 60, LOOP[1] / 60, color=GREY, alpha=0.08, lw=0)
        if inside:
            ax.text((LOOP[0] + LOOP[1]) / 120, ytext * 0.55 if ax.get_yscale() != "log" else ytext * 0.25,
                    "replica in a full-sync loop\n(evicted at the 256 MB limit)", fontsize=8, color=GREY, ha="center", va="top")
    ax.axvline(L / 60, color=GREY, lw=0.8, ls=":")
    for t, lab in MARKS:
        if t > L + 60:
            continue
        ax.axvline(t / 60, color=GREY, lw=1, ls="--")
        ax.text(t / 60 + 0.8, ytext, lab, fontsize=8, color=GREY, va="top")


# ---- client latency / throughput (10 s)
soak = [{k: float(v) for k, v in r.items()} for r in csv.DictReader(open(f"{D}/soak.csv"))]
out["ops_total"] = int(sum(r["ops"] for r in soak))
out["errors_total"] = int(sum(r["errors"] for r in soak))
out["ops_per_s_median"] = st.median(r["ops_per_s"] for r in soak)
out["ops_per_s_min_interval"] = min(r["ops_per_s"] for r in soak)
ev_windows = [(t - 10, t + 120) for t, _ in MARKS]


def in_ev(t):
    return any(a <= t <= b for a, b in ev_windows)


for k in ("p50_us", "p99_us", "p999_us"):
    v = [r[k] for r in soak]
    out[k + "_median"] = st.median(v)
    out[k + "_worst"] = max(v)
    out[k + "_median_outside_events"] = st.median(r[k] for r in soak if not in_ev(r["t_s"]))
    out[k + "_worst_outside_events"] = max(r[k] for r in soak if not in_ev(r["t_s"]))
out["max_us"] = max(r["max_us"] for r in soak)
out["by_half_hour_median_p50_p99_p999_ms"] = {}
for a in range(0, int(L // 60), 30):
    rr = [r for r in soak if a * 60 < r["t_s"] <= (a + 30) * 60]
    if rr:
        out["by_half_hour_median_p50_p99_p999_ms"]["%d-%d" % (a, a + 30)] = [round(st.median(r[k] for r in rr) / 1000, 3) for k in ("p50_us", "p99_us", "p999_us")]
errlog = []
if os.path.exists(f"{D}/soak-errs.log"):
    for line in open(f"{D}/soak-errs.log"):
        f = line.split()
        errlog.append((float(f[0]) - start, {k: int(v) for k, v in (x.rsplit("=", 1) for x in f[1:])}))
out["soakgen_error_windows"] = []
for t, lab in MARKS:
    e = [(x, d) for x, d in errlog if t - 5 <= x <= t + 180]
    kinds = {}
    for _, d in e:
        for k, v in d.items():
            kinds[k] = kinds.get(k, 0) + v
    out["soakgen_error_windows"].append({"event": lab.replace("\n", " "), "at_min": round(t / 60, 2), "errors": sum(kinds.values()),
                                         "kinds": kinds, "first_last_s_after": (round(e[0][0] - t, 1), round(e[-1][0] - t, 1)) if e else None})
out["soakgen_errors_outside_events"] = sum(sum(d.values()) for x, d in errlog if not any(t - 5 <= x <= t + 180 for t, _ in MARKS))


# ---- server samples
def samples(path):
    rows = []
    for r in csv.DictReader(open(path)):
        try:
            g = lambda k: int(r[k]) if r.get(k) not in (None, "") else None
            rows.append({"t": float(r["ts"]) - start, "used": g("used_memory"), "rss": g("used_memory_rss") or (g("rss_kb") or 0) * 1024,
                         "cpu": float(r["cpu_pct"]) if r["cpu_pct"] else None, "keys": g("keys") or 0, "lag": g("replica_lag_bytes"),
                         "aof": g("aof_size"), "slab": g("value_slab_bytes"), "slab_live": g("value_slab_live_bytes"),
                         "link": r.get("master_link_status"), "role": r.get("role"), "frag": r.get("mem_fragmentation_ratio"),
                         "lost": g("aof_lost_records"), "aofst": r.get("aof_last_write_status")})
        except (ValueError, TypeError):
            pass
    return [x for x in rows if x["used"] is not None]


sm, sr = samples(f"{D}/samples-master.csv"), samples(f"{D}/samples-replica.csv")


def slope(rows, key, a, b):
    xs = [(r["t"], r[key]) for r in rows if a <= r["t"] <= b and r[key] is not None]
    n = len(xs)
    if n < 3:
        return None
    mx = sum(x for x, _ in xs) / n
    my = sum(y for _, y in xs) / n
    return round(sum((x - mx) * (y - my) for x, y in xs) / sum((x - mx) ** 2 for x, _ in xs) * 3600 / 2 ** 20, 1)


def near(rows, t, key):
    r = min(rows, key=lambda r: abs(r["t"] - t))
    return r[key]


T_RESTART = next((t for t, n in events if n.startswith("master-restart-start")), None)
for name, rows in (("master", sm), ("replica", sr)):
    load = [r for r in rows if 0 <= r["t"] <= L]
    o = {}
    for key in ("used", "rss", "slab", "slab_live"):
        o[key + "_gb_at_5min"] = round(near(load, 300, key) / 1e9, 3) if near(load, 300, key) is not None else None
        o[key + "_gb_at_end"] = round(near(load, L - 30, key) / 1e9, 3) if near(load, L - 30, key) is not None else None
        o[key + "_gb_max"] = round(max(r[key] for r in load if r[key] is not None) / 1e9, 3)
        o[key + "_mib_per_h_30_to_end"] = slope(load, key, 1800, L)
        o[key + "_mib_per_h_last_hour"] = slope(load, key, L - 3600, L)
        if T_RESTART:
            o[key + "_mib_per_h_after_restart"] = slope(load, key, T_RESTART + 600, L)
    o["keys_at_5min"] = near(load, 300, "keys")
    o["keys_at_end"] = near(load, L - 30, "keys")
    o["cpu_median_pct"] = st.median(r["cpu"] for r in load if r["cpu"] is not None)
    o["aof_size_mb_max"] = round(max(r["aof"] or 0 for r in load) / 1e6, 1)
    o["aof_size_mb_median"] = round(st.median(r["aof"] or 0 for r in load) / 1e6, 1)
    o["aof_lost_records_max"] = max((r["lost"] or 0) for r in rows)
    o["aof_last_write_status_seen"] = sorted({r["aofst"] for r in rows if r["aofst"]})
    out[name] = o
lag = [r["lag"] for r in sm if r["lag"] is not None and 0 <= r["t"] <= L and r["role"] == "master"]
out["replica_lag_bytes"] = {"median": st.median(lag) if lag else None, "max": max(lag) if lag else None, "samples": len(lag)}
out["replica_link_down_samples"] = sum(1 for r in sr if 0 <= r["t"] <= L and r["link"] not in ("up",))

# ---- checkpoints from the server logs
def ckpts(path):
    res, cur, pid_cur = [], None, None
    for line in open(path, errors="replace"):
        m = re.match(r"(\d{4}-\d\d-\d\dT[\d:.]+)Z (\d+) \[(\w+)\] (.*)", line)
        if not m:
            continue
        ts = dt.datetime.fromisoformat(m.group(1)).replace(tzinfo=dt.timezone.utc).timestamp() - start
        msg = m.group(4)
        if "auto-checkpoint:" in msg and "folding" in msg:
            cur = ts
        elif "rebased onto dump.kdb" in msg and cur is not None:
            res.append((cur, ts - cur))
            cur = None
    return res


logs = {r: f"{D}/server-{r}.log" for r in ("master", "replica") if os.path.exists(f"{D}/server-{r}.log")}
cp = {}
for role, p in logs.items():
    c = ckpts(p)
    d = sorted(x[1] for x in c if 0 <= x[0] <= L)
    cp[role] = c
    out["checkpoints_" + role] = {"count": len(d), "median_s": round(st.median(d), 2) if d else None,
                                   "p90_s": round(d[int(len(d) * .9)], 2) if d else None, "max_s": round(max(d), 2) if d else None}
    bad = [l.strip()[:300] for l in open(p, errors="replace") if re.search(r"panic|\[ERROR\]|\[WARN\]", l)]
    out["log_warn_error_" + role] = {"count": len(bad), "lines": bad[:40]}

# ---- write log and compares
wl = {}
for p in sorted(glob.glob(f"{D}/wl/verify-*.json")):
    v = json.load(open(p))
    wl[v["node"]] = {"acked": v["acked_total"], "unknown": v["unknown_total"], "bad": v["bad_total"],
                     "per_kind": v["per_kind"], "problems_sample": v["problems"][:10], "checked_at_s": v["checked_at_s"]}
out["writelog"] = wl
tl = [r for r in csv.DictReader(open(f"{D}/wl/timeline.csv"))]
out["writelog_error_windows"] = []
for t, lab in MARKS:
    e = [(int(r["t"]), int(r["err"])) for r in tl if t - 5 <= int(r["t"]) <= t + 120 and int(r["err"]) > 0]
    out["writelog_error_windows"].append({"event": lab.replace("\n", " "), "errors": sum(x for _, x in e),
                                          "first_last_s_after": (e[0][0] - round(t), e[-1][0] - round(t)) if e else None})
try:
    out["writelog_error_kinds"] = json.load(open(f"{D}/wl/error_kinds.json"))
except Exception:
    pass
for name in ("compare.json",):
    try:
        out["compare_end"] = json.load(open(f"{D}/{name}"))
    except Exception as e:
        out["compare_end"] = repr(e)
out["compare_end_tail"] = open(f"{D}/compare.out").read().strip().splitlines()[-1:] if os.path.exists(f"{D}/compare.out") else None
out["offsets"] = open(f"{D}/offsets.txt").read().strip() if os.path.exists(f"{D}/offsets.txt") else None
if os.path.exists(f"{D}/restart/steps.txt"):
    out["restart_steps"] = open(f"{D}/restart/steps.txt").read().splitlines()
for k in list(out.get("compare_end", {}) if isinstance(out.get("compare_end"), dict) else []):
    if k in ("diff_sample", "differences") and isinstance(out["compare_end"][k], list):
        out["compare_end"][k] = out["compare_end"][k][:10]
json.dump(out, open(f"{D}/s1-summary.json", "w"), indent=1)

# ---- charts
x = lambda rows: [r["t"] / 60 for r in rows]
fig, (a1, a2) = plt.subplots(2, 1, figsize=(10, 7), sharex=True)
for rows, nm, ls in ((sm, "master", "-"), (sr, "replica", "--")):
    a1.plot(x(rows), [r["rss"] / 1e9 for r in rows], color=ORANGE, ls=ls, lw=1.2, label=f"RSS, {nm}")
    a1.plot(x(rows), [r["used"] / 1e9 for r in rows], color=KV, ls=ls, lw=1.2, label=f"used_memory, {nm}")
top = max(r["rss"] for r in sm + sr) / 1e9 * 1.25
a1.set_ylim(0, top)
evmarks(a1, top * 0.97)
a1.set_ylabel("GB")
a1.set_title(TITLE + ": memory", fontsize=10)
a1.legend(loc="lower right", frameon=False, ncol=2, fontsize=8)
a1.grid(alpha=.3)
for rows, nm, ls in ((sm, "master", "-"), (sr, "replica", "--")):
    a2.plot(x(rows), [(r["slab"] or 0) / 1e9 for r in rows], color=GREEN, ls=ls, lw=1.2, label=f"value_slab_bytes, {nm}")
    a2.plot(x(rows), [(r["slab_live"] or 0) / 1e9 for r in rows], color=PURPLE, ls=ls, lw=1.2, label=f"value_slab_live_bytes, {nm}")
top2 = max((r["slab"] or 0) for r in sm + sr) / 1e9 * 1.3 or 1
a2.set_ylim(0, top2)
evmarks(a2, top2 * 0.97)
a2.set_ylabel("GB")
a2.set_xlabel("minutes since the load started")
a2.legend(loc="lower right", frameon=False, ncol=2, fontsize=8)
a2.grid(alpha=.3)
a2.set_xlim(-2, L / 60 + 12)
fig.tight_layout()
fig.savefig(f"{C}/s1-memory.svg")
plt.close(fig)

fig, (a1, a2) = plt.subplots(2, 1, figsize=(10, 6.5), sharex=True, gridspec_kw={"height_ratios": [3, 1.2]})
tt = [r["t_s"] / 60 for r in soak]
a1.plot(tt, [r["p50_us"] / 1000 for r in soak], color=KV, lw=1.1, label="p50")
a1.plot(tt, [r["p99_us"] / 1000 for r in soak], color=ORANGE, lw=0.9, label="p99")
a1.plot(tt, [r["p999_us"] / 1000 for r in soak], color=GREEN, lw=0.8, label="p99.9")
a1.set_yscale("log")
top = max(r["p999_us"] for r in soak) / 1000 * 4
a1.set_ylim(0.05, top)
evmarks(a1, top * 0.8)
a1.set_ylabel("ms per 10 s interval (log)")
a1.set_title(TITLE + ": client latency", fontsize=10)
a1.legend(loc="lower right", frameon=False, ncol=3)
a1.grid(alpha=.3, which="both")
wt = [int(r["t"]) / 60 for r in tl]
a2.plot(wt, [int(r["err"]) for r in tl], color=ORANGE, lw=0.8, label="write-log errors per s (not acknowledged)")
a2.plot(tt, [r["errors"] / 10 for r in soak], color=KV, lw=1, label="soakgen errors per s (10 s mean)")
a2.set_yscale("symlog")
evmarks(a2, 1, False)
a2.set_ylabel("errors / s")
a2.set_xlabel("minutes since the load started")
a2.legend(loc="upper left", frameon=False, fontsize=8)
a2.grid(alpha=.3)
a2.set_xlim(-2, L / 60 + 12)
fig.tight_layout()
fig.savefig(f"{C}/s1-latency.svg")
plt.close(fig)

fig, (a1, a2, a3) = plt.subplots(3, 1, figsize=(10, 8), sharex=True)
lr = [r for r in sm if r["lag"] is not None and r["role"] == "master"]
a1.plot(x(lr), [max(r["lag"], 1) / 2 ** 20 for r in lr], color=KV, lw=1)
a1.set_yscale("log")
a1.set_ylabel("replica lag, MiB (log)")
a1.set_title(TITLE + ": replication, AOF and checkpoints", fontsize=10)
evmarks(a1, max((r["lag"] for r in lr), default=1) / 2 ** 20 * 0.8)
a1.grid(alpha=.3, which="both")
for rows, nm, ls in ((sm, "master", "-"), (sr, "replica", "--")):
    a2.plot(x(rows), [(r["aof"] or 0) / 1e6 for r in rows], color=KV if nm == "master" else ORANGE, ls=ls, lw=1, label=f"aof_current_size, {nm}")
a2.set_ylabel("MB")
a2.legend(loc="upper right", frameon=False, fontsize=8)
evmarks(a2, max((r["aof"] or 0) for r in sm) / 1e6 * 0.95)
a2.grid(alpha=.3)
for role, col in (("master", KV), ("replica", ORANGE)):
    c = cp.get(role, [])
    a3.plot([t / 60 for t, _ in c], [d for _, d in c], ".", ms=2.5, color=col, label=f"checkpoint duration, {role}")
a3.set_ylabel("seconds")
a3.set_xlabel("minutes since the load started")
a3.legend(loc="upper right", frameon=False, fontsize=8)
evmarks(a3, max((d for c in cp.values() for _, d in c), default=1) * 0.95)
a3.grid(alpha=.3)
a3.set_xlim(-2, L / 60 + 12)
fig.tight_layout()
fig.savefig(f"{C}/s1-replication-aof.svg")
plt.close(fig)
print(json.dumps({k: out[k] for k in ("ops_total", "errors_total", "p50_us_median", "p99_us_median", "p999_us_median")}))
print(json.dumps(out["writelog"] and {k: (v["acked"], v["bad"]) for k, v in out["writelog"].items()}))
