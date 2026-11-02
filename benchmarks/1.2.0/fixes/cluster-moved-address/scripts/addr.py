#!/usr/bin/env python3
"""Per run, from monitor.csv: when every observer first showed the moved nodes at their new
address (and kept it to the end), when the moved nodes were first all healthy everywhere, and
whether any observer showed a moved node at an address other than its current one after that.
Writes addr.json beside each summary.json and prints a table."""
import csv
import glob
import json
import sys

R = sys.argv[1]
NAMES = [f"n{i}" for i in range(1, 7)]
for p in sorted(glob.glob(f"{R}/*/*/summary.json")):
    d = p.rsplit("/", 1)[0]
    s = json.load(open(p))
    moving = s["moving"]
    restarts = [t for t, e in s["events"] if e.startswith("restarted")]
    t_restart = restarts[-1]
    new = s["new_ips"]
    samples = {}
    for row in csv.DictReader(open(f"{d}/monitor.csv")):
        samples.setdefault(float(row["t"]), {})[row["observer"]] = row
    addr_since = None
    first_healthy = None
    wrong_after = 0
    for t in sorted(samples):
        if t < t_restart:
            continue
        rows = samples[t]
        ok_addr = True
        healthy = True
        for o in NAMES:
            r = rows.get(o)
            if r is None or r["cluster_state"] == "unreachable":
                ok_addr = healthy = False
                continue
            if r["cluster_state"] != "ok":
                healthy = False
            for n in moving:
                v = r[f"{n}_view"]
                ip, flags, link = (v.split("|") + ["", "", ""])[:3] if v != "absent" else ("", "", "")
                if ip != new[n]:
                    ok_addr = False
                if link != "connected" or set(flags.split(",")) & {"fail", "fail?", "handshake", "noaddr"}:
                    healthy = False
        if ok_addr:
            addr_since = addr_since if addr_since is not None else t
        else:
            if addr_since is not None:
                wrong_after += 1
            addr_since = None
        if ok_addr and healthy and first_healthy is None:
            first_healthy = t
    out = {
        "addr_converged_s": None if addr_since is None else round(addr_since - t_restart, 2),
        "first_all_healthy_s": None if first_healthy is None else round(first_healthy - t_restart, 2),
        "addr_regressions": wrong_after,
    }
    json.dump(out, open(f"{d}/addr.json", "w"), indent=1)
    print(d.split("/e2e/")[1], out)
