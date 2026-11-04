#!/usr/bin/env python3
"""Markdown results table + results.csv from e2e/*/*/summary.json (+ addr.json from addr.py)."""
import csv
import glob
import json
import os
import sys

R = sys.argv[1]
rows = []
for p in sorted(glob.glob(f"{R}/*/*/summary.json")):
    s = json.load(open(p))
    a = os.path.join(os.path.dirname(p), "addr.json")
    s.update(json.load(open(a)) if os.path.exists(a) else {})
    s["fail_quorum_events"] = 0
    for log in glob.glob(os.path.join(os.path.dirname(p), "server-n*.log")):
        s["fail_quorum_events"] += sum("marked FAIL by quorum" in line for line in open(log, errors="replace"))
    rows.append(s)
order = {"two-masters": 0, "two-masters-down3s": 1, "master-plus-other-replica": 2, "one-master": 3,
         "rapid-move": 4, "rapid-move-two-masters": 5}
lab = {"base": 0, "patched": 1, "proposal": 2}
rows.sort(key=lambda s: (order.get(s["scenario"], 9), lab[s["label"]], s["rep"]))
cols = ["scenario", "label", "rep", "moving", "down_s", "addr_converged_s", "first_all_healthy_s",
        "all_views_and_state_ok_s", "cluster_state_ok_everywhere_s", "flaps_after_restart", "addr_regressions",
        "fail_quorum_events", "observed_after_restart_s", "writes_attempted_after_kill", "writes_failed_after_kill",
        "writes_clusterdown_after_kill", "last_failed_write_s_after_kill", "last_clusterdown_s_after_kill",
        "failed_seconds_after_restart"]
with open(f"{R}/results.csv", "w", newline="") as f:
    w = csv.writer(f, lineterminator="\n")
    w.writerow(cols)
    for s in rows:
        w.writerow(["+".join(s[c]) if c == "moving" else s.get(c) for c in cols])

NAME = {"base": "unpatched", "patched": "69c2bef2", "proposal": "69c2bef2 + follow-up"}


def fmt(v, cap):
    return f"never (> {cap:.0f})" if v is None else f"{v:.1f}"


print("| scenario | binary | run | addresses right everywhere (s after restart) | all 6 views healthy + cluster_state ok, for good (s) | FAIL-by-quorum events | failed / attempted SETs after stop | of which CLUSTERDOWN | last failed SET (s after stop) |")
print("|---|---|---|---|---|---|---|---|---|")
for s in rows:
    cap = s["observed_after_restart_s"] or 0
    print(f"| {s['scenario']} | {NAME[s['label']]} | {s['rep']} | {fmt(s.get('addr_converged_s'), cap)} | "
          f"{fmt(s['all_views_and_state_ok_s'], cap)} | {s['fail_quorum_events']} | "
          f"{s['writes_failed_after_kill']} / {s['writes_attempted_after_kill']} | "
          f"{s['writes_clusterdown_after_kill']} | {s['last_failed_write_s_after_kill']} |")
