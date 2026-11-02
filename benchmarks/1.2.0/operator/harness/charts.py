"""Charts for the operator e2e suite.

Usage: python charts.py <results dir> <charts dir>

Reads, from <results dir>:
  <scenario>-timeline.json   loader /timeline rows: [unix, ok, errors, max ms, slow]
  marks.csv                  scenario,unix,label   (events marked during the run)
  scenarios.json             [{"id", "title", "loader", "t0", "t1"}]  (window and takeaway title)
  summary.csv                one row per scenario (for the summary chart)
Writes one SVG per scenario plus summary charts into <charts dir>.
"""
import csv
import json
import os
import sys
import textwrap

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

plt.rcParams.update({
    "svg.fonttype": "none",
    "font.family": "DejaVu Sans",
    "font.size": 10,
    "axes.facecolor": "white",
    "figure.facecolor": "white",
    "savefig.facecolor": "white",
    "axes.edgecolor": "#888888",
    "axes.labelcolor": "#222222",
    "xtick.color": "#444444",
    "ytick.color": "#444444",
    "text.color": "#222222",
    "axes.grid": True,
    "grid.color": "#e6e6e6",
    "grid.linewidth": 0.8,
    "axes.spines.top": False,
    "axes.spines.right": False,
})

KIVIDB = "#0072B2"
ERR = "#333333"
EVENT = "#7a7a7a"
SUMMARY_TITLE = os.environ.get("SUMMARY_TITLE", "Write impact per scenario, 1.1.3 vs 1.2.0-rc4")


def load_marks(path):
    marks = {}
    if not os.path.exists(path):
        return marks
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            marks.setdefault(row["scenario"], []).append((int(row["unix"]), row["label"]))
    return marks


def timeline_chart(res, out, sc, marks):
    path = os.path.join(res, f"{sc['id']}-timeline.json")
    if not os.path.exists(path):
        print("no timeline for", sc["id"])
        return
    rows = json.load(open(path))
    t0, t1 = sc["t0"], sc["t1"]
    rows = [r for r in rows if t0 <= r[0] <= t1]
    if not rows:
        print("empty window for", sc["id"])
        return
    # fill missing seconds with zeros (no completions in that second)
    by = {r[0]: r for r in rows}
    xs = list(range(t0, t1 + 1))
    ok = [by[s][1] if s in by else 0 for s in xs]
    er = [by[s][2] if s in by else 0 for s in xs]
    mx = [by[s][3] if s in by else None for s in xs]
    rel = [s - t0 for s in xs]

    fig, axes = plt.subplots(3, 1, figsize=(10, 6.2), sharex=True,
                             gridspec_kw={"height_ratios": [3, 1.4, 1.6]})
    a0, a1, a2 = axes
    a0.plot(rel, ok, color=KIVIDB, linewidth=1.4)
    a0.set_ylabel("acked SET/s")
    a0.set_ylim(0, max(max(ok) * 1.15, 10))
    a1.bar(rel, er, width=1.0, color=ERR, linewidth=0)
    a1.set_ylabel("failed SET/s")
    a1.set_ylim(0, max(max(er) * 1.2, 5))
    mxr = [(x, m) for x, m in zip(rel, mx) if m is not None and m > 0]
    if mxr:
        a2.plot([x for x, _ in mxr], [m for _, m in mxr], color=KIVIDB, linewidth=1.0,
                marker="o", markersize=2.5)
    a2.set_yscale("log")
    a2.set_ylim(0.5, max([m for _, m in mxr] + [1000]) * 2)
    a2.set_ylabel("slowest SET\nin second (ms)")
    a2.set_xlabel(f"seconds since scenario start (t=0 at {sc.get('t0s', '')} UTC)")
    a2.set_xlim(0, t1 - t0)

    ev = sorted(marks.get(sc["id"], []))
    lane = 0
    for t, label in ev:
        if not (t0 <= t <= t1):
            continue
        x = t - t0
        for a in axes:
            a.axvline(x, color=EVENT, linestyle="--", linewidth=0.9)
        a0.annotate(label, xy=(x, 1.0), xycoords=("data", "axes fraction"),
                    xytext=(3, -12 - 12 * (lane % 4)), textcoords="offset points",
                    fontsize=8, color="#222222", ha="left", va="top",
                    bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none", alpha=0.85))
        lane += 1
    title = "\n".join(textwrap.wrap(sc["title"], 95))
    fig.suptitle(title, fontsize=12, x=0.01, ha="left", fontweight="bold")
    sub = sc.get("subtitle")
    if sub:
        fig.text(0.01, 0.935, sub, fontsize=9, color="#444444", ha="left")
    top = 0.96 - (0.04 if sub else 0) - 0.035 * title.count("\n")
    fig.tight_layout(rect=(0, 0, 1, top))
    fig.savefig(os.path.join(out, f"timeline-{sc['id']}.svg"))
    if os.environ.get("PREVIEW"):
        fig.savefig(os.path.join(os.environ["PREVIEW"], f"timeline-{sc['id']}.png"), dpi=80)
    plt.close(fig)


def summary_charts(res, out):
    """Grouped bars: impact seconds per kind of scenario, 1.1.3 vs 1.2.0-rc4."""
    path = os.path.join(res, "summary.csv")
    if not os.path.exists(path):
        return
    rows = list(csv.DictReader(open(path, newline="")))
    colors = {"1.1.3": "#56B4E9", "1.2.0-rc4": KIVIDB}
    whats = []
    for r in rows:
        if r["what"] and r["what"] not in whats:
            whats.append(r["what"])
    val = {(r["what"], r["engine"]): r for r in rows}
    fig, ax = plt.subplots(figsize=(10, 0.55 * len(whats) + 1.6))
    h = 0.38
    top = 0
    for k, w in enumerate(whats):
        y = len(whats) - 1 - k
        for j, eng in enumerate(["1.1.3", "1.2.0-rc4"]):
            r = val.get((w, eng))
            if r is None:
                continue
            yy = y + (h / 2 if j == 0 else -h / 2)
            v = float(r["impact_s"])
            top = max(top, v)
            ax.barh(yy, v, height=h * 0.92, color=colors[eng], label=eng)
            note = f" ({r['note']})" if r.get("note") else ""
            ax.text(v + 0.4, yy, f"{v:g} s, {int(r['errors']):,} failed, {int(r['lost']):,} lost{note}", va="center", fontsize=8)
    ax.set_yticks(range(len(whats)))
    ax.set_yticklabels(list(reversed(whats)), fontsize=9)
    ax.set_xlim(0, top * 1.9 + 1)
    ax.set_xlabel("seconds of write impact seen by the loader (failed SETs or < 50% of normal rate)")
    ax.grid(axis="y", visible=False)
    hs, ls = ax.get_legend_handles_labels()
    seen = {}
    for hh, ll in zip(hs, ls):
        seen.setdefault(ll, hh)
    ax.legend(seen.values(), seen.keys(), loc="upper left", bbox_to_anchor=(1.0, 1.0), frameon=False)
    fig.suptitle(SUMMARY_TITLE, x=0.01, ha="left", fontsize=12, fontweight="bold")
    fig.tight_layout(rect=(0, 0, 1, 0.97))
    fig.savefig(os.path.join(out, "summary-impact.svg"))
    if os.environ.get("PREVIEW"):
        fig.savefig(os.path.join(os.environ["PREVIEW"], "summary-impact.png"), dpi=80)
    plt.close(fig)


def main():
    res, out = sys.argv[1], sys.argv[2]
    os.makedirs(out, exist_ok=True)
    marks = load_marks(os.path.join(res, "marks.csv"))
    for sc in json.load(open(os.path.join(res, "scenarios.json"))):
        timeline_chart(res, out, sc, marks)
    summary_charts(res, out)


if __name__ == "__main__":
    main()
