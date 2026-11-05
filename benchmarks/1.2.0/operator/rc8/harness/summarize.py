"""Summarise each scenario from the loader's stats, verify and timeline output.

Usage: python summarize.py <results dir> <titles.json>

Per scenario (scenarios.json gives the chart window):
  checked     acknowledged writes read back by /verify (cumulative for that loader)
  lost        acknowledged writes missing on read-back that were not already missing at
              the previous check of the same loader (the loaders keep every ack for the
              whole run, so /verify's count is cumulative)
  errors      failed SETs during the scenario
  impact_s    seconds from the first to the last "impacted" second, where a second is
              impacted if it had a failed SET or fewer than half the median acked SET/s
  max_set_ms  slowest single SET in the window
Writes results/summary.csv and prints a Markdown table.
"""
import csv
import json
import os
import statistics
import sys


def main():
    res, titles_path = sys.argv[1], sys.argv[2]
    titles = json.load(open(titles_path))
    scen = json.load(open(os.path.join(res, "scenarios.json")))
    rows = []
    prev_missing = {}
    for sc in scen:
        sid = sc["id"]
        t = titles[sid]
        v = json.load(open(os.path.join(res, f"{sid}-verify.json")))
        ld = t.get("loader", "loader-shd")
        lost = v["missing"] - prev_missing.get(ld, 0)
        prev_missing[ld] = v["missing"]
        s = json.load(open(os.path.join(res, f"{sid}-stats.json")))
        tl = [r for r in json.load(open(os.path.join(res, f"{sid}-timeline.json")))
              if sc["t0"] <= r[0] <= sc["t1"]]
        by = {r[0]: r for r in tl}
        # the last second is cut short by the loader stop: leave it out
        secs = list(range(sc["t0"], sc["t1"]))
        oks = [by[x][1] if x in by else 0 for x in secs]
        # the loader may still be ramping up from the previous check: skip leading empty seconds
        while oks and oks[0] == 0:
            oks.pop(0)
            secs.pop(0)
        med = statistics.median(oks) if oks else 0
        hit = [x for x, o in zip(secs, oks) if o < 0.5 * med or (x in by and by[x][2] > 0)]
        impact = (hit[-1] - hit[0] + 1) if hit else 0
        mx = max([r[3] for r in tl] or [0])
        rows.append({
            "scenario": sid, "engine": t.get("engine", ""), "what": t.get("what", ""),
            "checked": v["checked"], "lost": lost, "wrong": v["wrong"],
            "errors": s["errors"], "impact_s": impact, "max_set_ms": mx,
            "median_set_per_s": int(med), "note": t.get("note", ""),
        })
    with open(os.path.join(res, "summary.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]), lineterminator="\n")
        w.writeheader()
        w.writerows(rows)
    print("| Scenario | Engine | What | Acked writes checked | Lost | Failed SETs | Impact (s) | Slowest SET (ms) |")
    print("|---|---|---|---:|---:|---:|---:|---:|")
    for r in rows:
        print(f"| {r['scenario']} | {r['engine']} | {r['what']} | {r['checked']:,} | {r['lost']} | "
              f"{r['errors']} | {r['impact_s']} | {r['max_set_ms']:,} |")


if __name__ == "__main__":
    main()
