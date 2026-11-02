"""prof_charts.py RESULTS_DIR CHARTS_DIR: profiling charts.

RESULTS_DIR/cpu-split.csv (categorize.py), RESULTS_DIR/cpu-<engine>.csv
(profile.sh's cpu.csv per engine), RESULTS_DIR/callgrind-table.csv.
"""
import csv
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

plt.rcParams.update({
    "svg.fonttype": "none", "font.family": "DejaVu Sans", "font.size": 10,
    "axes.facecolor": "white", "figure.facecolor": "white", "savefig.facecolor": "white",
    "axes.edgecolor": "#444444", "axes.labelcolor": "#222222", "text.color": "#222222",
    "xtick.color": "#333333", "ytick.color": "#333333",
    "axes.grid": True, "grid.color": "#e3e3e3", "grid.linewidth": 0.6, "axes.axisbelow": True,
    "axes.spines.top": False, "axes.spines.right": False,
})
RES, OUT = sys.argv[1], sys.argv[2]
os.makedirs(OUT, exist_ok=True)
NAMES = {"kividb-rc4-fp": "KiviDB 1.2.0-rc4", "kividb-1.1.3-fp": "KiviDB 1.1.3", "redis": "Redis 8.10.3 (io-threads 8)"}
COLORS = {"kividb-rc4-fp": "#0072B2", "kividb-1.1.3-fp": "#56B4E9", "redis": "#D55E00"}

# Categories: one sequential-ish neutral set (identity of a category, not of an engine).
CATS = [
    ("kernel: network I/O (syscalls, io_uring, TCP, softirq)", "#4d4d4d", "kernel: network I/O"),
    ("kernel: thread sleep/wake (futex, scheduler)", "#9a9a9a", "kernel: sleep/wake"),
    ("event loop and connection handling (user space)", "#7fb8d8", "event loop (user)"),
    ("parsing requests", "#e6c27a", "parsing"),
    ("command execution (lookup, read/write the value)", "#2a7f62", "command execution"),
    ("building replies", "#b5d99c", "building replies"),
    ("memory allocation", "#c58fb0", "allocation"),
    ("locks and atomics", "#d4775a", "locks/atomics"),
    ("other user space", "#dddddd", "other"),
]

split = list(csv.DictReader(open(os.path.join(RES, "cpu-split.csv"))))
order = [(e, w) for e in ["kividb-rc4-fp", "kividb-1.1.3-fp", "redis"] for w in ["get-p1", "get-p16", "set-p1", "set-p16"]
         if any(r["engine"] == e and r["workload"] == w for r in split)]
if order:
    fig, ax = plt.subplots(figsize=(12, 0.5 * len(order) + 2.2))
    ys = list(range(len(order)))[::-1]
    for cat, color, short in CATS:
        left = []
        vals = []
        for (e, w) in order:
            acc = 0.0
            for c2, _, _ in CATS:
                if c2 == cat:
                    break
                acc += sum(float(r["us_per_op"]) for r in split if r["engine"] == e and r["workload"] == w and r["category"] == c2)
            left.append(acc)
            vals.append(sum(float(r["us_per_op"]) for r in split if r["engine"] == e and r["workload"] == w and r["category"] == cat))
        ax.barh(ys, vals, left=left, color=color, edgecolor="white", linewidth=1.0, height=0.7, label=short)
    for y, (e, w) in zip(ys, order):
        tot = sum(float(r["us_per_op"]) for r in split if r["engine"] == e and r["workload"] == w)
        ax.text(tot, y, f" {tot:.2f} us", va="center", fontsize=8)
    ax.set_yticks(ys)
    ax.set_yticklabels([f"{NAMES[e].split(' (')[0]}  {w.replace('-p', ', pipeline ').upper().replace('PIPELINE', 'pipeline')}"
                        for e, w in order], fontsize=9)
    ax.set_xlabel("server CPU microseconds per request (all threads, user + kernel)")
    ax.set_xlim(left=0)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.12), ncol=5, frameon=False, fontsize=8)
    ax.set_title(os.environ.get("SPLIT_TITLE", "CPU per request: KiviDB and Redis about equal; pipelining cuts it ~5x for both, mostly kernel I/O"),
                 loc="left", fontsize=12)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "cpu-per-request-split.svg"))
    plt.close(fig)

# callgrind: instructions per command
cg = os.path.join(RES, "callgrind-table.csv")
if os.path.exists(cg):
    rows = list(csv.DictReader(open(cg)))
    tags = [k for k in rows[0].keys() if k != "workload"]
    tcol = {"v113": "#56B4E9", "rc4": "#0072B2", "redis-8.10.3": "#D55E00"}
    tname = {"v113": "KiviDB 1.1.3", "rc4": "KiviDB 1.2.0-rc4", "redis-8.10.3": "Redis 8.10.3"}
    rows = [r for r in rows if all(r.get(t) for t in tags)]
    fig, ax = plt.subplots(figsize=(12, 4.6))
    w = 0.8 / len(tags)
    for j, t in enumerate(tags):
        xs = [i + (j - (len(tags) - 1) / 2) * w for i in range(len(rows))]
        hs = [float(r[t]) for r in rows]
        ax.bar(xs, hs, width=w * 0.92, color=tcol.get(t, "#999999"), label=tname.get(t, t))
    ax.set_xticks(range(len(rows)))
    ax.set_xticklabels([r["workload"].replace("MIX-100", "MIX-100 (SET+GET)") for r in rows], fontsize=8, rotation=20)
    ax.set_ylabel("instructions per command (callgrind)")
    ax.set_ylim(bottom=0)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.2), ncol=3, frameon=False)
    ax.set_title(os.environ.get("CG_TITLE", "One thread, callgrind: KiviDB rc4 runs GET and SET in 18-19% fewer instructions than Redis;\nrc4 writes cost ~8% more than 1.1.3 (pipeline 16, whole user-space path per command)"),
                 loc="left", fontsize=12)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "instructions-per-command.svg"))
    plt.close(fig)
print("charts in", OUT)

# per-thread CPU at pipeline 16: is one thread the ceiling?
for wl in ["get-p16", "set-p16"]:
    series = []
    for e in ["kividb-rc4-fp", "redis"]:
        p = os.path.join(RES, e, f"{wl}.threads.csv")
        if not os.path.exists(p):
            continue
        rows = [r for r in csv.DictReader(open(p)) if float(r["cores"]) >= 0.02]
        series.append((e, rows))
    if not series:
        continue
    fig, axes = plt.subplots(1, len(series), figsize=(12, 4.2), sharey=True, squeeze=False)
    for ax, (e, rows) in zip(axes[0], series):
        xs = list(range(len(rows)))
        ax.bar(xs, [float(r["cores"]) for r in rows], color=COLORS[e], width=0.75)
        ax.set_xticks(xs)
        ax.set_xticklabels([r["thread"].replace("kividb-rc4-fp", "worker").replace("redis-server", "main")
                            for r in rows], rotation=60, fontsize=7)
        ax.axhline(1.0, color="#222222", linewidth=0.8, linestyle="--")
        ax.set_ylim(0, 1.1)
        ax.set_title(NAMES[e], loc="left", fontsize=10)
        ax.set_ylabel("CPU cores used by the thread")
    wname = wl.replace("-p16", "").upper()
    fig.suptitle(os.environ.get("THREADS_TITLE_" + wname,
                                f"{wname} 100 B, pipeline 16: Redis's main thread is pinned at one core; KiviDB's 14 workers all run\n(CPU of each server thread; dashed line = one full core)"),
                 x=0.01, ha="left", fontsize=12)
    fig.tight_layout(rect=(0, 0, 1, 0.86))
    fig.savefig(os.path.join(OUT, f"threads-{wl}.svg"))
    plt.close(fig)
print("thread charts done")
