"""synccount.py <marks.log> <elogs dir> [out.csv]: per scenario window (BEGIN .. STOP in marks.log), from every engine
pod's followed log (enginelogs.sh), count
  full      replica-side "Full resync from" (a full sync started)
  partial   replica-side "Partial resync: continuing" (continued, no dataset transfer)
  shut_ok   rc9 shutdown: "replica ... has the whole stream ... (N ms)"; the N values are listed (the shutdown wait)
  shut_bad  rc9 shutdown: "did not acknowledge" / "still being sent the dataset"
  skipped   rc9: final save skipped (a replica stopped mid full sync keeps its last good dump)
  loaded0   "Loaded 0 keys from snapshot" (an empty dump loaded)
Lines are de-duplicated (the followers re-attach with --since)."""
import collections
import csv
import glob
import os
import re
import sys
import calendar
import time


def scen_windows(path):
    b, s = {}, {}
    for line in open(path):
        p = line.split()
        if len(p) < 3 or not p[-1].isdigit():
            continue
        if p[0] == "BEGIN":
            b[p[1]] = int(p[-1])
        elif p[0] == "STOP":
            s[p[1]] = int(p[-1])
    return sorted(((b[k], s[k], k) for k in b if k in s))


def main():
    wins = scen_windows(sys.argv[1])
    rows = collections.OrderedDict((k, collections.defaultdict(list)) for _, _, k in wins)
    for f in sorted(glob.glob(os.path.join(sys.argv[2], "*.log"))):
        pod = os.path.basename(f)[:-4]
        seen = set()
        for line in open(f, errors="replace"):
            if line in seen:
                continue
            seen.add(line)
            m = re.match(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)\.\d+Z (.*)", line)
            if not m:
                continue
            t = calendar.timegm(time.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S"))
            msg = m.group(2)
            sc = next((k for a, z, k in wins if a <= t <= z), None)
            if sc is None:
                continue
            r = rows[sc]
            if "Full resync from" in msg:
                r["full"].append(pod)
            elif "Partial resync: continuing" in msg:
                r["partial"].append(pod)
            elif "has the whole stream" in msg:
                w = re.search(r"\((\d+) ms\)", msg)
                r["shut_ok"].append(f"{pod}:{w.group(1) if w else '?'}ms")
            elif "did not acknowledge the end of the stream" in msg or "still being sent the dataset" in msg:
                r["shut_bad"].append(pod)
            elif "the final save is skipped" in msg:
                r["skipped"].append(pod)
            elif "Loaded 0 keys from snapshot" in msg:
                r["loaded0"].append(pod)
    cols = ["full", "partial", "shut_ok", "shut_bad", "skipped", "loaded0"]
    out = [["scenario"] + cols + ["full_pods", "shutdown_waits"]]
    for k, r in rows.items():
        out.append([k] + [len(r[c]) for c in cols] + [" ".join(r["full"]), " ".join(r["shut_ok"])])
    w = csv.writer(open(sys.argv[3], "w", newline="") if len(sys.argv) > 3 else sys.stdout, lineterminator="\n")
    w.writerows(out)
    if len(sys.argv) > 3:
        for row in out:
            print(*row, sep=" | ")


if __name__ == "__main__":
    main()
