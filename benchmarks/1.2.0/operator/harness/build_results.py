"""Turn the run's marks.log into results/marks.csv and results/scenarios.json.

Usage: python build_results.py <marks.log> <titles.json> <results dir> [events.log]

marks.log lines: "<label ...> HH:MM:SS <unix>". "BEGIN <id>" opens a scenario,
"STOP <id>" is the moment the loader was stopped for verification (end of the
chart window); any other line whose first word is a scenario id is an event
marker on that scenario's chart.
titles.json: {"<id>": {"title": ..., "subtitle": ..., "loader": ...}}
events.log (optional): `kubectl get events -w` lines (lastTimestamp, type, reason,
object, message); operator handover/failover Events inside a scenario's window
become markers too.
"""
import calendar
import csv
import json
import os
import re
import sys
import time

OPERATOR_REASONS = {"Switchover", "Failover", "ShardFailover", "MasterRestartedEmpty",
                    "MasterRestartedEmptyResolved", "RebalanceStarted", "RebalanceCompleted",
                    "PasswordChanged"}


def operator_events(path):
    out = []
    for line in open(path, errors="replace"):
        m = re.match(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ)\s+(\w+)\s+(\w+)\s+(\S+)\s+(.*)", line)
        if not m:
            continue
        t, _typ, reason, _obj, msg = m.groups()
        if reason == "ShardSwitchover" and "master moved" in msg:
            reason = "ShardSwitchover"
        elif reason not in OPERATOR_REASONS:
            continue
        sh = re.match(r"shard (\d+):", msg)
        label = "op: " + reason + (f" (shard {sh.group(1)})" if sh else "")
        unix = calendar.timegm(time.strptime(t, "%Y-%m-%dT%H:%M:%SZ"))
        out.append((unix, label))
    return sorted(set(out))


def main():
    marks_log, titles_path, res = sys.argv[1:4]
    titles = json.load(open(titles_path))
    begins, stops, events = {}, {}, []
    for line in open(marks_log):
        parts = line.split()
        if len(parts) < 3 or not parts[-1].isdigit():
            continue
        unix = int(parts[-1])
        words = parts[:-2]
        if words[0] == "BEGIN":
            begins[words[1]] = unix
        elif words[0] == "STOP":
            stops[words[1]] = unix
        elif words[0] == "END":
            continue
        else:
            events.append((words[0], unix, " ".join(words[1:])))
    # "<id>-rep" is the replicated cluster's loader checked after scenario <id> (drains): same start
    for sid in list(stops):
        if sid not in begins and sid.endswith("-rep") and sid[:-4] in begins:
            begins[sid] = begins[sid[:-4]]
    scenarios = []
    for sid, t0 in begins.items():
        if sid not in stops or sid not in titles:
            continue
        t = titles[sid]
        scenarios.append({
            "id": sid, "title": t["title"], "subtitle": t.get("subtitle", ""),
            "loader": t.get("loader", ""), "t0": t0, "t1": stops[sid],
            "t0s": time.strftime("%H:%M:%S", time.gmtime(t0)),
        })
    scenarios.sort(key=lambda s: s["t0"])
    json.dump(scenarios, open(os.path.join(res, "scenarios.json"), "w"), indent=1)
    if len(sys.argv) > 4:
        for unix, label in operator_events(sys.argv[4]):
            for sc in scenarios:
                if sc["t0"] <= unix <= sc["t1"]:
                    events.append((sc["id"], unix, label))
    with open(os.path.join(res, "marks.csv"), "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["scenario", "unix", "label"])
        for sid, unix, label in events:
            if label:
                w.writerow([sid, unix, label])
    print(len(scenarios), "scenarios,", len(events), "marks")


if __name__ == "__main__":
    main()
