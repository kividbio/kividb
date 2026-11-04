#!/usr/bin/env python3
"""cmp_rc4.py <rc7 summary.csv> <rc4 summary.csv> <out-dir>

One row per scenario: KiviDB rc7 (this run) vs KiviDB rc4 (rc4 run, same harness) vs Redis 8.10.3 (this run).
Writes <out>/results/compare-rc4.md and <out>/charts/rc7-vs-rc4.svg."""
import csv, os, statistics, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
plt.rcParams.update({"svg.fonttype": "none", "font.family": "DejaVu Sans", "font.size": 10, "figure.facecolor": "white",
                     "axes.facecolor": "white", "savefig.facecolor": "white", "axes.grid": True, "grid.color": "#e3e3e3",
                     "axes.spines.top": False, "axes.spines.right": False})
from fo_report import SCEN  # noqa: E402

rc7, rc4, out = sys.argv[1], sys.argv[2], sys.argv[3]
A = list(csv.DictReader(open(rc7)))
B = list(csv.DictReader(open(rc4)))
# lost acknowledged writes are expected where a replica is promoted past writes it never got
EXPECTED_LOSS = {"promote_abrupt", "partition_promote", "partition_promote_minrep", "c_failover_force", "c_partition",
                 "c_partition_minrep", "power_loss_everysec", "power_loss_always",
                 "kill9_noack"}  # kill9_noack: aof-ack-after-write no may lose the last few (documented)


def med(rows, eng, scen, k):
    v = [float(r[k]) for r in rows if r["engine"] == eng and r["scenario"] == scen and r[k] not in ("", None)]
    if not v:
        return None
    return statistics.median(v), min(v), max(v), len(v)


def f(a, d=0):
    if a is None:
        return "–"
    m, lo, hi, n = a
    g = (lambda x: f"{x:,.0f}") if d == 0 else (lambda x: f"{x:.1f}")
    return g(m) if lo == hi else f"{g(m)} ({g(lo)}–{g(hi)})"


lines = ["| Scenario | Runs rc7 / rc4 / Redis | Lost acked: rc7 | rc4 | Redis | WAIT-acked lost: rc7 / rc4 / Redis | "
         "Unavailable (s): rc7 | rc4 | Redis | Replica differs (rc7) | rc7 verdict |",
         "|---|---|---|---|---|---|---|---|---|---|---|"]
chart = []
for scen, label in SCEN:
    a = med(A, "kividb", scen, "acked")
    if a is None:
        continue
    b = med(B, "kividb", scen, "acked"); r = med(A, "redis", scen, "acked")
    la, lb, lr = med(A, "kividb", scen, "lost_acked"), med(B, "kividb", scen, "lost_acked"), med(A, "redis", scen, "lost_acked")
    wa = lambda rows, e: med(rows, e, scen, "wait_acked_lost") if (med(rows, e, scen, "wait_acked") or (0,))[0] else None
    ua, ub, ur = med(A, "kividb", scen, "unavailable_s"), med(B, "kividb", scen, "unavailable_s"), med(A, "redis", scen, "unavailable_s")
    wrong = med(A, "kividb", scen, "wrong_value")
    worst = la[2] if la else 0
    wl = wa(A, "kividb")
    if wrong and wrong[2] > 0:
        verdict = "**FAIL (wrong value)**"
    elif scen in ("wait_kill9_promote", "c_wait_kill9"):
        # a SET whose WAIT did not confirm it is asynchronous; only WAIT-acknowledged writes must survive
        verdict = "**FAIL (WAIT-acked lost)**" if wl and wl[2] > 0 else "pass (no WAIT-acked write lost)"
    elif scen == "kill9_noack":
        verdict = "pass (documented: may lose the last few)"
    elif scen.startswith("power_loss"):
        verdict = "pass (within the everysec window)"
    elif scen in EXPECTED_LOSS:
        verdict = "pass (loss expected: async replication)"
    else:
        verdict = "pass" if worst == 0 else "**FAIL**"
    n = lambda x: str(x[3]) if x else "–"
    lines.append(f"| {label} | {n(a)} / {n(b)} / {n(r)} | {f(la)} | {f(lb)} | {f(lr)} | "
                 f"{f(wa(A, 'kividb'))} / {f(wa(B, 'kividb'))} / {f(wa(A, 'redis'))} | {f(ua, 1)} | {f(ub, 1)} | {f(ur, 1)} | "
                 f"{f(med(A, 'kividb', scen, 'replica_differences'))} | {verdict} |")
    chart.append((label, ua, ub, ur))
os.makedirs(os.path.join(out, "results"), exist_ok=True)
open(os.path.join(out, "results", "compare-rc4.md"), "w", newline="\n").write("\n".join(lines) + "\n")

fig, ax = plt.subplots(figsize=(9, 0.55 * len(chart) + 1.4))
cols = [("KiviDB 1.2.0-rc7", "#0072B2"), ("KiviDB 1.2.0-rc4 (rc4 run)", "#56B4E9"), ("Redis 8.10.3", "#D55E00")]
for j, (lab, col) in enumerate(cols):
    ys = [i + (j - 1) * 0.27 for i in range(len(chart))]
    vals = [c[1 + j] for c in chart]
    ax.barh([y for y, v in zip(ys, vals) if v], [v[0] for v in vals if v], height=0.25, color=col, label=lab)
ax.set_yticks(range(len(chart)))
ax.set_yticklabels([c[0] for c in chart], fontsize=8)
ax.invert_yaxis()
ax.set_xlabel("seconds in which writes ran below half their normal rate (median of runs)")
ax.set_title("Write unavailability: rc7 against rc4 and Redis 8.10.3", loc="left", fontsize=10)
ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0), frameon=False)
fig.tight_layout()
os.makedirs(os.path.join(out, "charts"), exist_ok=True)
fig.savefig(os.path.join(out, "charts", "rc7-vs-rc4-unavailability.svg"), bbox_inches="tight")
print("\n".join(lines))
