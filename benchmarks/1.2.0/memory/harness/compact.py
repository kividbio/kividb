#!/usr/bin/env python3
"""Shrink a raw output directory for publishing: memtier JSON loses its HDR histograms (kept: configuration,
totals, percentiles, per-second series), queue logs lose memtier's progress lines. Everything else is copied.

    python3 compact.py <raw-dir> <out-dir>
"""
import json, os, re, shutil, sys

src, dst = sys.argv[1], sys.argv[2]
os.makedirs(dst, exist_ok=True)


def strip(o):
    if isinstance(o, dict):
        return {k: strip(v) for k, v in o.items()
                if k not in ("Histogram log format", "Bytes RX", "Bytes TX", "Accumulated Latency", "Waits")}
    if isinstance(o, list):
        return [strip(v) for v in o]
    return o


for name in sorted(os.listdir(src)):
    p = os.path.join(src, name)
    if not os.path.isfile(p) or name in ("queue.txt", "done.txt") or name.endswith(".sampler.log"):
        continue
    q = os.path.join(dst, name)
    if name.endswith(".json"):
        try:
            j = json.load(open(p))
        except ValueError:
            print("skipped (incomplete, run was stopped):", name)
            continue
        json.dump(strip(j), open(q, "w"), separators=(",", ":"))
    elif name.endswith(".log") or name.endswith(".memtier.txt"):
        text = open(p, errors="replace").read()
        text = re.sub(r"\[RUN #\d+[^\n\r]*?(?=\[RUN #|\r|\n|$)", "", text)
        open(q, "w", newline="\n").write(text)
    else:
        shutil.copyfile(p, q)
