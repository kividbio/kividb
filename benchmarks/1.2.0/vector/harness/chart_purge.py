"""Chart for purge_test.sh: KiviDB's RSS after the tool deleted every vector,
with the MEMORY PURGE sent 60 s later marked.

Usage: python chart_purge.py <run dir with mem.csv, tool.log, purge.log> <out.svg>
"""
import csv, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

plt.rcParams.update({
    "svg.fonttype": "none", "font.family": "DejaVu Sans", "font.size": 10,
    "axes.facecolor": "white", "figure.facecolor": "white", "savefig.facecolor": "white",
    "axes.grid": True, "grid.color": "#e3e3e3", "axes.axisbelow": True,
    "axes.spines.top": False, "axes.spines.right": False,
})

d, out = sys.argv[1], sys.argv[2]
rows = [r for r in csv.DictReader(open(f"{d}/mem.csv")) if r["rss_kb"]]
log = [l.split(" ", 1) for l in open(f"{d}/tool.log", encoding="utf-8")]
cleanup = float([t for t, l in log if "Experiment stage: Cleanup" in l][0])
purge = float(open(f"{d}/purge.log").read().split()[0])
xs = [float(r["t"]) - cleanup for r in rows]
ys = [int(r["rss_kb"]) / 2**20 for r in rows]
keep = [(x, y) for x, y in zip(xs, ys) if x >= -30]
before = [y for x, y in keep if x < 0][-1]
after_del = [y for x, y in keep if 0 < x < purge - cleanup][-1]
after_purge = keep[-1][1]

fig, ax = plt.subplots(figsize=(8, 4.2))
ax.plot([k[0] for k in keep], [k[1] for k in keep], color="#0072B2", lw=2.4, label="KiviDB 1.2.0-rc4")
for x, label in [(0, " tool deletes index + 999K documents"), (purge - cleanup, " MEMORY PURGE")]:
    ax.axvline(x, color="#777777", lw=1, ls=":")
    ax.text(x, before * 0.55 if x else before * 1.02, label, fontsize=8, color="#333333", va="bottom")
ax.set_title(f"dbpedia 1M: KiviDB keeps {after_del:.1f} GiB of RSS after every vector is deleted,\n"
             f"until MEMORY PURGE gives it back ({after_purge:.2f} GiB)", fontsize=11, loc="left")
ax.set_xlabel("seconds since the delete started")
ax.set_ylabel("resident memory (GiB)")
ax.set_ylim(0, before * 1.12)
ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0), frameon=False)
fig.savefig(out, format="svg", bbox_inches="tight")
if len(sys.argv) > 3:
    fig.savefig(sys.argv[3], dpi=80, bbox_inches="tight")
print(f"before delete {before:.2f} GiB, 60 s after delete {after_del:.2f} GiB, after purge {after_purge:.2f} GiB")
