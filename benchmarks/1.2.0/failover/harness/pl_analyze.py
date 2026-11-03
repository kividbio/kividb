#!/usr/bin/env python3
"""pl_analyze.py <run-dir>...: for each power-off run, where the lost acknowledged writes fall.

The cut is located from the writes themselves: the last acknowledgement before the longest gap in
acknowledgements (the VM was off). For the lost writes: how long before that last acknowledgement
the first of them was acknowledged (the window that never reached the disk), and whether, per writer,
the lost writes are exactly the tail of what it had acknowledged before the cut (nothing older is missing).
"""
import json, os, sys

for d in sys.argv[1:]:
    a = json.load(open(os.path.join(d, "acked.json")))
    s = json.load(open(os.path.join(d, "summary.json")))
    lost = json.load(open(os.path.join(d, "lost.json"))) if os.path.exists(os.path.join(d, "lost.json")) else {"missing": [], "wrong": []}
    times = sorted(t for v in a["acked"].values() for _, t in v)
    gaps = [(times[i + 1] - times[i], i) for i in range(len(times) - 1)]
    g, i = max(gaps)
    cut_last, back_first = times[i], times[i + 1]
    lw = lost["missing"] + lost["wrong"]
    lt = sorted(t for _, _, t in lw)
    tail_ok = True
    for w, v in a["acked"].items():
        before = [seq for seq, t in v if t <= cut_last + 0.0015]
        lost_w = sorted(seq for ww, seq, _ in lw if str(ww) == w)
        if lost_w and lost_w != before[len(before) - len(lost_w):]:
            tail_ok = False
    after_cut = sum(1 for t in lt if t > cut_last + 0.0015)   # lost.json rounds times to 1 ms
    out = {
        "run": d, "acked": s["acked"], "lost_acked": s["lost_acked"], "wrong_value": s["wrong_value"],
        "last_ack_before_cut_t": round(cut_last, 3), "first_ack_after_restart_t": round(back_first, 3),
        "acks_stopped_for_s": round(g, 1),
        "lost_window_s": round(cut_last - lt[0], 3) if lt else 0.0,
        "lost_acked_after_restart": after_cut,
        "lost_are_each_writers_last_writes": tail_ok,
    }
    print(json.dumps(out))
