"""README media for KiviDB 1.2.0: light and dark PNG charts, and an MP4 + animated WebP.

  python -I render_media.py <benchmarks/1.2.0 dir> <out dir> [--no-video]

Every number comes from the 1.2.0 result files (core/results/summary.csv) or is
copied from the suites' tables (memory, vector, core/rc8), as noted beside it.
"""
import csv
import os
import sys

import imageio_ffmpeg
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
from PIL import Image  # noqa: E402

B, OUT = sys.argv[1], sys.argv[2]
VIDEO = "--no-video" not in sys.argv
os.makedirs(OUT, exist_ok=True)

THEME = {
    "light": dict(bg="#fcfcfb", ink="#0b0b0b", ink2="#52514e", muted="#898781", grid="#e1e0d9",
                  axis="#c3c2b7", accent="#2a78d6", other="#b9b7b0", s2="#eb6834", s3="#1baf7a"),
    "dark": dict(bg="#1a1a19", ink="#f0efec", ink2="#c3c2b7", muted="#898781", grid="#2c2c2a",
                 axis="#383835", accent="#3987e5", other="#5d5c58", s2="#d95926", s3="#199e70"),
}
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 13})

# ── data ──────────────────────────────────────────────────────────────────────
core = {(r["engine"], r["size"], r["scenario"]): r
        for r in csv.DictReader(open(os.path.join(B, "core/results/summary.csv"), encoding="utf-8"))}
CORE = [("kividb-cand", "KiviDB 1.2.0"), ("garnet", "Garnet 2.2.5"), ("dragonfly", "Dragonfly 2.0.2"),
        ("redis-cluster", "Redis Cluster (32 shards)"), ("redis@24", "Redis 8.10 (io-threads 24)"),
        ("valkey@24", "Valkey 9.1 (io-threads 24)")]


def core_rows(scenario, field, scale):
    rows = []
    for key, name in CORE:
        r = core[(key, "100", scenario)]
        rows.append((name, float(r[field]) * scale, bool(r["limit"]) and field == "ops_median"))
    return rows


CORE_NOTE = "Azure D32ps_v5 server (32 vCPU arm64), two D16ps_v5 load generators; 10M keys, 100-byte values; median of 3+ rounds"
SCENES = [
    dict(name="throughput-pipelined", title="Throughput, pipeline 16, 100-byte values", unit="M ops/s",
         panels=[("GET", core_rows("get_p16", "ops_median", 1e-6)), ("SET", core_rows("set_p16", "ops_median", 1e-6))],
         fmt="{:.2f}M", note=CORE_NOTE + "; * the load generators were the limit"),
    dict(name="throughput-unpipelined", title="Throughput, no pipelining, 100-byte values", unit="M ops/s",
         panels=[("GET", core_rows("get_p1", "ops_median", 1e-6)), ("SET", core_rows("set_p1", "ops_median", 1e-6))],
         fmt="{:.2f}M", note=CORE_NOTE + "; * the load generators were the limit"),
    dict(name="latency-p99-pipelined", title="p99 latency, pipeline 16 (ms, lower is better)", unit="ms",
         panels=[("GET", core_rows("get_p16", "p99_ms", 1)), ("SET", core_rows("set_p16", "p99_ms", 1))],
         fmt="{:.2f} ms", note=CORE_NOTE),
    # core/rc8/README.md: the shipped MSET code, competitors in the same session (one round each).
    dict(name="mset", title="MSET of 10 keys, pipeline 1 (thousand commands/s)", unit="K/s",
         panels=[("", [("KiviDB 1.2.0", 569, False), ("Garnet 2.2.5", 800, False), ("Dragonfly 2.0.2", 561, False),
                       ("Redis 8.10 (io-threads 24)", 144, False)])],
         fmt="{:.0f}K", note="Same machines; 1,200 connections; keys/s is ten times this. Valkey and Redis Cluster were not in this round"),
    # memory/results/tables.md, "Memory as the dataset grows", 100 B values, 50M keys.
    dict(name="memory-per-key", title="Memory per key, 50M keys of 100 bytes (bytes, lower is better)", unit="B",
         panels=[("", [("KiviDB 1.2.0", 140.7, False), ("Dragonfly 2.0.2", 141.5, False), ("Valkey 9.1", 166.6, False),
                       ("Redis 8.10", 174.4, False)])],
         fmt="{:.1f} B", note="RSS per key while loading random 100-byte values, Azure E32ds_v7"),
    # memory/results/tables.md, "Snapshot, per engine", BGSAVE added %.
    dict(name="snapshot-memory", title="Memory added by BGSAVE under 100k writes/s (%, lower is better)", unit="%",
         panels=[("", [("KiviDB 1.2.0", 10, False), ("Dragonfly 2.0.2", 0.4, False), ("Valkey 9.1", 78, False),
                       ("Redis 8.10", 89, False)])],
         fmt="+{:g}%", note="30M keys of 1 KB, uniform random SETs during the snapshot; Redis and Valkey fork, copy-on-write"),
    # vector/results/tables.md, h-and-m 2048-d filtered, 100 clients, ef 128.
    dict(name="vector-filtered", title="Filtered vector search, 2048 dimensions, 100 clients (queries/s)", unit="q/s",
         panels=[("", [("KiviDB 1.2.0", 4478, False), ("Redis 8.10 (io-threads 8)", 1842, False), ("Dragonfly 2.0.2", 811, False)])],
         fmt="{:,.0f}", note="vector-db-benchmark h-and-m-2048-angular-filters, ef 128, recall@10 0.998 on all three"),
    # vector/results/tables.md, glove 25-d, upload + index.
    dict(name="vector-ingest", title="Vector ingest and index build, 1.18M vectors (s, lower is better)", unit="s",
         panels=[("", [("KiviDB 1.2.0", 30.2, False), ("Dragonfly 2.0.2", 27.8, False), ("Redis 8.10 (io-threads 8)", 59.8, False)])],
         fmt="{:.1f} s", note="glove-25-angular, HNSW M=16, EF_CONSTRUCTION=256, 100 clients uploading in batches of 64"),
]
# vector/results/tables.md, glove 25-d, 100 clients, ef 64/128/256/512: (recall, q/s).
QPS = dict(title="Vector search: queries per second against recall, 1.18M vectors",
           note="glove-25-angular, HNSW M=16, ef 64-512, 100 clients; Azure D32ps_v5 server, D16ps_v5 client",
           series=[("KiviDB 1.2.0", "accent", [(0.9478, 38217), (0.9647, 33891), (0.9908, 23451), (0.9983, 14992)]),
                   ("Dragonfly 2.0.2", "s2", [(0.9116, 25582), (0.9366, 25089), (0.9791, 21412), (0.9951, 15452)]),
                   ("Redis 8.10 (io-threads 8)", "s3", [(0.9116, 21198), (0.9366, 19310), (0.9791, 13677), (0.9951, 8794)])])


def ease(t):
    t = min(max(t, 0.0), 1.0)
    return 1 - (1 - t) ** 3


# ── drawing ───────────────────────────────────────────────────────────────────
def style(ax, th):
    ax.set_facecolor(th["bg"])
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(th["axis"])
    ax.tick_params(colors=th["ink2"], length=0)
    ax.grid(axis="x", color=th["grid"], linewidth=1)
    ax.set_axisbelow(True)


def draw_bars(fig, scene, th, t=1.0, video=False):
    panels = scene["panels"]
    n = len(panels)
    vmax = max(v for _, rows in panels for _, v, _ in rows) * 1.22
    axes = fig.subplots(1, n, sharey=True) if n > 1 else [fig.add_subplot(1, 1, 1)]
    for ax, (ptitle, rows) in zip(np.atleast_1d(axes), panels):
        style(ax, th)
        names = [r[0] for r in rows]
        y = np.arange(len(rows))[::-1]
        vals = [r[1] * ease(t) for r in rows]
        colors = [th["accent"] if nm.startswith("KiviDB") else th["other"] for nm in names]
        ax.barh(y, vals, height=0.68, color=colors, edgecolor=th["bg"], linewidth=2)
        ax.set_yticks(y)
        ax.set_yticklabels(names, color=th["ink"])
        ax.set_xlim(0, vmax)
        ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:g}"))
        if ptitle:
            ax.set_title(ptitle, color=th["ink"], fontsize=15, pad=8)
        if t > 0.85:
            for yi, (nm, v, limited) in zip(y, rows):
                label = scene["fmt"].format(v) + (" *" if limited else "")
                ax.text(v + vmax * 0.012, yi, label, va="center", ha="left", color=th["ink"], fontsize=12,
                        fontweight="bold" if nm.startswith("KiviDB") else "normal")
    fig.suptitle(scene["title"], color=th["ink"], fontsize=19 if video else 18, fontweight="bold", y=0.965)
    fig.text(0.5, 0.025, scene["note"], ha="center", color=th["muted"], fontsize=10.5)
    fig.subplots_adjust(left=0.2, right=0.97, top=0.84 if n > 1 else 0.88, bottom=0.12, wspace=0.12)


def draw_qps(fig, th, t=1.0, video=False):
    ax = fig.add_subplot(1, 1, 1)
    style(ax, th)
    ax.grid(axis="y", color=th["grid"], linewidth=1)
    frac = ease(t)
    for name, key, pts in QPS["series"]:
        xs = [p[0] for p in pts]
        ys = [p[1] / 1000 for p in pts]
        k = max(1, int(np.ceil(frac * len(xs))))
        ax.plot(xs[:k], ys[:k], color=th[key], linewidth=2.5, marker="o", markersize=8,
                markeredgecolor=th["bg"], markeredgewidth=2, label=name)
        if t > 0.85:
            ax.text(xs[0] - 0.0015, ys[0], name, ha="right", va="center", color=th["ink"], fontsize=12)
    ax.set_xlim(0.875, 1.002)
    ax.set_ylim(0, 42)
    ax.set_xlabel("recall@10 (higher is better)", color=th["ink2"])
    ax.set_ylabel("thousand queries per second", color=th["ink2"])
    leg = ax.legend(loc="lower left", frameon=False, labelcolor=th["ink"], fontsize=12)
    fig.suptitle(QPS["title"], color=th["ink"], fontsize=19 if video else 18, fontweight="bold", y=0.965)
    fig.text(0.5, 0.025, QPS["note"], ha="center", color=th["muted"], fontsize=10.5)
    fig.subplots_adjust(left=0.08, right=0.97, top=0.88, bottom=0.14)
    return leg


def card(fig, th, lines):
    fig.patch.set_facecolor(th["bg"])
    big, small = lines[0], lines[1:]
    fig.text(0.5, 0.58, big, ha="center", va="center", color=th["ink"], fontsize=34, fontweight="bold")
    for i, s in enumerate(small):
        fig.text(0.5, 0.44 - i * 0.075, s, ha="center", va="center", color=th["ink2"], fontsize=17)


def new_fig(th, w, h, dpi):
    fig = plt.figure(figsize=(w / dpi, h / dpi), dpi=dpi)
    fig.patch.set_facecolor(th["bg"])
    return fig


# ── PNGs ──────────────────────────────────────────────────────────────────────
for mode, th in THEME.items():
    for sc in SCENES:
        fig = new_fig(th, 1310, 800 if len(sc["panels"]) > 1 else 640, 100)
        draw_bars(fig, sc, th)
        fig.savefig(os.path.join(OUT, f"{sc['name']}-{mode}.png"), facecolor=th["bg"])
        plt.close(fig)
    fig = new_fig(th, 1310, 800, 100)
    draw_qps(fig, th)
    fig.savefig(os.path.join(OUT, f"vector-qps-recall-{mode}.png"), facecolor=th["bg"])
    plt.close(fig)
print("PNGs written")

# ── video ─────────────────────────────────────────────────────────────────────
if VIDEO:
    th, W, H, DPI, FPS = THEME["dark"], 1280, 720, 100, 24
    GROW, HOLD = 0.9, 2.8
    plan = [("card", ["KiviDB 1.2.0", "against Garnet, Dragonfly, Redis, Redis Cluster and Valkey",
                      "Azure VMs, November 2026"], 2.6)]
    plan += [("bars", sc, GROW + HOLD) for sc in SCENES[:6]]
    plan += [("qps", None, GROW + HOLD), ("bars", SCENES[6], GROW + HOLD), ("bars", SCENES[7], GROW + HOLD)]
    plan += [("card", ["Every number, every engine", "benchmarks/1.2.0/REPORT.md",
                       "github.com/kividbio/kividb"], 2.6)]

    mp4 = os.path.join(OUT, "kividb-1.2.0-benchmarks.mp4")
    writer = imageio_ffmpeg.write_frames(mp4, (W, H), fps=FPS, codec="libx264", quality=None,
                                         pix_fmt_out="yuv420p", ffmpeg_log_level="error",
                                         output_params=["-crf", "26", "-preset", "slow", "-threads", "2",
                                                        "-movflags", "+faststart"])
    writer.send(None)
    webp_frames = []
    frame_no = 0
    for kind, arg, dur in plan:
        for i in range(int(dur * FPS)):
            t = (i / FPS) / GROW
            fig = new_fig(th, W, H, DPI)
            if kind == "card":
                card(fig, th, arg)
            elif kind == "bars":
                draw_bars(fig, arg, th, t, video=True)
            else:
                draw_qps(fig, th, t, video=True)
            fig.canvas.draw()
            rgb = np.asarray(fig.canvas.buffer_rgba())[:, :, :3].copy()
            plt.close(fig)
            writer.send(rgb)
            if frame_no % 2 == 0:  # the WebP at 12 fps, 960x540
                webp_frames.append(Image.fromarray(rgb).resize((960, 540), Image.LANCZOS))
            frame_no += 1
    writer.close()
    webp = os.path.join(OUT, "kividb-1.2.0-benchmarks.webp")
    webp_frames[0].save(webp, save_all=True, append_images=webp_frames[1:], duration=int(1000 / 12),
                        loop=0, quality=70, method=4)
    print(f"video: {frame_no} frames, {frame_no / FPS:.1f} s; mp4 {os.path.getsize(mp4):,} B, webp {os.path.getsize(webp):,} B")
