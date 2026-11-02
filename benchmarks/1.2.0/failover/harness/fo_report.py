#!/usr/bin/env python3
"""Tables and charts from fo_run.sh results.

  fo_report.py <results-dir> <out-dir>

Writes <out-dir>/results/summary.csv (one row per engine/scenario/rep), <out-dir>/results/table.md
(median over reps, with min-max) and <out-dir>/charts/*.svg.

Write unavailability and recovery: see outage().
"""
import csv
import glob
import json
import os
import statistics
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def _save(fig, path, **kw):
    fig.savefig(path, **kw)
    if os.environ.get("PNG"):  # a PNG beside each SVG, for a quick look; not published
        fig.savefig(path[:-4] + ".png", dpi=80, **kw)

plt.rcParams.update({
    "svg.fonttype": "none", "font.family": "DejaVu Sans", "font.size": 10,
    "figure.facecolor": "white", "axes.facecolor": "white", "savefig.facecolor": "white",
    "axes.edgecolor": "#444444", "axes.labelcolor": "#222222", "xtick.color": "#333333",
    "ytick.color": "#333333", "text.color": "#222222", "axes.grid": True, "grid.color": "#e3e3e3",
    "grid.linewidth": 0.8, "axes.spines.top": False, "axes.spines.right": False,
})
COL = {"kividb": "#0072B2", "redis": "#D55E00"}
NAME = {"kividb": "KiviDB 1.2.0-rc4", "redis": "Redis 8.10.3"}

SCEN = [
    ("promote_graceful", "Planned switchover (pause, wait, promote, move clients)"),
    ("promote_abrupt", "Replica promoted while master still takes writes"),
    ("kill9_everysec", "kill -9 master, AOF everysec, restart"),
    ("kill9_always", "kill -9 master, appendfsync always, restart"),
    ("kill9_noack", "kill -9 master, aof-ack-after-write no (KiviDB only)"),
    ("kill9_checkpoint", "kill -9 master as an AOF checkpoint starts, 3 times (KiviDB only)"),
    ("power_loss_everysec", "Power-off of the master's VM, AOF everysec, restart"),
    ("power_loss_always", "Power-off of the master's VM, appendfsync always, restart"),
    ("sigstop", "SIGSTOP master 30 s"),
    ("partition", "Partition master <-> replica 30 s (no promotion)"),
    ("partition_promote", "Partition + promote replica (split brain, 15 s)"),
    ("partition_promote_minrep", "Same, min-replicas-to-write 1, max-lag 2"),
    ("wait_kill9_promote", "WAIT 1 after each SET; kill -9 master, promote"),
    ("c_failover", "Cluster: CLUSTER FAILOVER"),
    ("c_failover_force", "Cluster: CLUSTER FAILOVER FORCE"),
    ("c_kill9", "Cluster: kill -9 a master"),
    ("c_wait_kill9", "Cluster: WAIT 1 after each SET; kill -9 a master"),
    ("c_sigstop", "Cluster: SIGSTOP a master 30 s"),
    ("c_partition", "Cluster: master's host cut off from other nodes 20 s"),
    ("c_partition_minrep", "Same, min-replicas-to-write 1, max-lag 2"),
]


def load_timeline(d):
    rows = list(csv.DictReader(open(os.path.join(d, "timeline.csv"))))
    t = [float(r["t"]) for r in rows]
    ok = [int(r["ok"]) for r in rows]
    err = [int(r["err"]) for r in rows]
    return t, ok, err


def outage(t, ok, events, duration):
    """(seconds below half rate, recovery seconds) for the outage the first event causes.

    The outage starts at the first 100 ms interval, within 10 s of the event, in which fewer than half
    the usual writes succeeded (the usual: the median of the 10 s before the event). It ends where
    2 s in a row run at half the usual rate or more. Recovery is from the event to that end.
    """
    if not events:
        return 0.0, 0.0
    ev = events[0][0]
    eb = int(ev / 0.1)
    pre = sorted(ok[max(0, eb - 100):eb]) or [0]
    base = pre[len(pre) // 2]
    end = min(len(ok), int((duration - 1.0) / 0.1))  # the last second is cut short by the stop
    low = lambda b: ok[b] < base * 0.5
    start = next((b for b in range(eb, min(end, eb + 100)) if low(b)), None)
    if start is None:
        return 0.0, 0.0
    b = start
    while b < end and any(low(x) for x in range(b, min(end, b + 20))):
        b += 1
    unav = sum(1 for x in range(start, b) if low(x))
    return round(unav * 0.1, 1), round(b * 0.1 - ev, 1)


def main():
    res, out = sys.argv[1], sys.argv[2]
    os.makedirs(os.path.join(out, "results"), exist_ok=True)
    os.makedirs(os.path.join(out, "charts"), exist_ok=True)
    rows = []
    for d in sorted(glob.glob(os.path.join(res, "*", "*", "*"))):
        if not os.path.exists(os.path.join(d, "summary.json")):
            continue
        eng, scen, rep = d.split(os.sep)[-3:]
        s = json.load(open(os.path.join(d, "summary.json")))
        t, ok, err = load_timeline(d)
        unav, rec = outage(t, ok, s["events"], s["duration_s"])
        conv = None
        cj = os.path.join(d, "converge.json")
        if os.path.exists(cj):
            conv = json.load(open(cj)).get("real_differences")
        rows.append(dict(engine=eng, scenario=scen, rep=int(rep), acked=s["acked"], lost_acked=s["lost_acked"],
                         wrong_value=s["wrong_value"], wait_acked=s["wait_acked"], wait_acked_lost=s["wait_acked_lost"],
                         unacked_errors=s["unacked_errors"], unavailable_s=unav, recovery_s=rec,
                         replica_differences=conv if conv is not None else "",
                         lost_first_t=s.get("lost_ack_time_first") or "", lost_last_t=s.get("lost_ack_time_last") or "",
                         first_event_t=s["events"][0][0] if s["events"] else ""))
    keys = list(rows[0].keys())
    with open(os.path.join(out, "results", "summary.csv"), "w", newline="\n") as f:
        w = csv.DictWriter(f, keys, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)

    def agg(eng, scen, k):
        v = [r[k] for r in rows if r["engine"] == eng and r["scenario"] == scen and r[k] != ""]
        if not v:
            return None
        return statistics.median(v), min(v), max(v), len(v)

    def fmt(a, digits=0):
        if a is None:
            return "–"
        m, lo, hi, n = a
        f = (lambda x: f"{x:,.0f}") if digits == 0 else (lambda x: f"{x:.1f}")
        return f(m) if lo == hi else f"{f(m)} ({f(lo)}–{f(hi)})"

    with open(os.path.join(out, "results", "table.md"), "w", newline="\n") as f:
        f.write("| Scenario | Engine | Runs | Acked writes | Lost acked | WAIT-acked lost | Writes unavailable (s) | Recovery (s) | Replica differs after |\n")
        f.write("|---|---|---|---|---|---|---|---|---|\n")
        for scen, label in SCEN:
            for eng in ("kividb", "redis"):
                a = agg(eng, scen, "acked")
                if a is None:
                    continue
                wl = agg(eng, scen, "wait_acked_lost") if (agg(eng, scen, "wait_acked") or (0,))[0] else None
                f.write(f"| {label} | {NAME[eng]} | {a[3]} | {fmt(a)} | {fmt(agg(eng, scen, 'lost_acked'))} | "
                        f"{fmt(wl) if wl else '–'} | {fmt(agg(eng, scen, 'unavailable_s'), 1)} | "
                        f"{fmt(agg(eng, scen, 'recovery_s'), 1)} | {fmt(agg(eng, scen, 'replica_differences'))} |\n")

    # ---- timelines: one chart per scenario, KiviDB above Redis, rep 1
    for scen, label in SCEN:
        engs = [e for e in ("kividb", "redis") if os.path.exists(os.path.join(res, e, scen, "1", "summary.json"))]
        if not engs:
            continue
        fig, axes = plt.subplots(len(engs), 1, figsize=(9, 2.6 * len(engs) + 0.6), sharex=True, squeeze=False)
        for ax, eng in zip(axes[:, 0], engs):
            d = os.path.join(res, eng, scen, "1")
            s = json.load(open(os.path.join(d, "summary.json")))
            t, ok, err = load_timeline(d)
            # 0.5 s bins, in writes per second
            tb = [t[i] for i in range(0, len(t), 5)]
            okb = [sum(ok[i:i + 5]) * 2 for i in range(0, len(ok), 5)]
            erb = [sum(err[i:i + 5]) * 2 for i in range(0, len(err), 5)]
            ax.plot(tb, okb, color=COL[eng], lw=2, label="acknowledged SETs/s")
            ax.plot(tb, erb, color="#555555", lw=1.2, ls=(0, (3, 2)), label="failed SETs/s (errors, timeouts)")
            top = max(okb + [1]) * 1.6
            for i, (et, en) in enumerate(s["events"]):
                ax.axvline(et, color="#222222", lw=1, ls=":")
                lab = en if len(en) <= 46 else en[:44] + "..."
                ax.text(et + 0.4, top * (0.95 - 0.1 * (i % 5)), lab, fontsize=7.5, va="top", color="#222222",
                        bbox=dict(facecolor="white", edgecolor="none", pad=0.5, alpha=0.85))
            ax.set_ylim(0, top)
            lost = s["lost_acked"]
            extra = f", WAIT-acked lost {s['wait_acked_lost']} of {s['wait_acked']:,}" if s["wait_n"] else ""
            ax.set_title(f"{NAME[eng]}: {lost:,} acknowledged writes lost of {s['acked']:,}{extra}", fontsize=10, loc="left")
            ax.set_ylabel("SETs / s")
        axes[-1, 0].set_xlabel("seconds since the writers started")
        axes[0, 0].legend(loc="upper left", bbox_to_anchor=(1.01, 1.0), frameon=False, fontsize=8)
        fig.suptitle(label, x=0.01, ha="left", fontsize=12, fontweight="bold")
        fig.tight_layout()
        _save(fig, os.path.join(out, "charts", f"timeline-{scen}.svg"), bbox_inches="tight")
        plt.close(fig)

    # ---- lost acked writes, all scenarios
    present = [(s, l) for s, l in SCEN if agg("kividb", s, "acked") or agg("redis", s, "acked")]
    fig, ax = plt.subplots(figsize=(9, 0.5 * len(present) + 1.4))
    y = list(range(len(present)))
    for j, eng in enumerate(("kividb", "redis")):
        vals = [(agg(eng, s, "lost_acked") or (None,))[0] for s, _ in present]
        ys = [i + (j - 0.5) * 0.38 for i in y]
        ax.barh([yy for yy, v in zip(ys, vals) if v is not None], [v + 0.0 for v in vals if v is not None], height=0.34,
                color=COL[eng], label=NAME[eng])
        for yy, v in zip(ys, vals):
            if v is not None:
                ax.text(max(v, 0.6) * 1.15 if v > 0 else 0.6, yy, f"{v:,.0f}", va="center", fontsize=8, color="#222222")
    ax.set_xscale("symlog", linthresh=1)
    ax.set_yticks(y)
    ax.set_yticklabels([l for _, l in present], fontsize=8)
    ax.invert_yaxis()
    ax.set_xlim(0, None)
    ax.set_xlabel("acknowledged writes lost (median of runs; symmetric log scale, 0 at the axis)")
    ax.set_title("Acknowledged writes lost: none without a promotion or a power cut; asynchronous-replication cases lose "
                 "about the same on both", loc="left", fontsize=10)
    ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0), frameon=False)
    fig.tight_layout()
    _save(fig, os.path.join(out, "charts", "lost-acked-writes.svg"), bbox_inches="tight")
    plt.close(fig)

    # ---- write unavailability
    fig, ax = plt.subplots(figsize=(9, 0.5 * len(present) + 1.4))
    for j, eng in enumerate(("kividb", "redis")):
        vals = [(agg(eng, s, "unavailable_s") or (None,))[0] for s, _ in present]
        ys = [i + (j - 0.5) * 0.38 for i in y]
        ax.barh([yy for yy, v in zip(ys, vals) if v is not None], [v for v in vals if v is not None], height=0.34,
                color=COL[eng], label=NAME[eng])
        for yy, v in zip(ys, vals):
            if v is not None:
                ax.text(v + 0.3, yy, f"{v:.1f}", va="center", fontsize=8, color="#222222")
    ax.set_yticks(y)
    ax.set_yticklabels([l for _, l in present], fontsize=8)
    ax.invert_yaxis()
    ax.set_xlim(0, None)
    ax.set_xlabel("seconds in which writes ran below half their normal rate (median of runs)")
    ax.set_title("Write unavailability per scenario", loc="left", fontsize=10)
    ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0), frameon=False)
    fig.tight_layout()
    _save(fig, os.path.join(out, "charts", "write-unavailability.svg"), bbox_inches="tight")
    plt.close(fig)
    print("rows", len(rows))


if __name__ == "__main__":
    main()
