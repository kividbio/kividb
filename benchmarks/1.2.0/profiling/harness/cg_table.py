"""cg_table.py OUT_CSV RAW...: 'LABEL tag instructions' lines (scripts/perf/run.sh's
results.txt and callgrind-redis.sh's output) -> one CSV row per workload."""
import csv
import sys

rows, tags = {}, []
for path in sys.argv[2:]:
    for line in open(path):
        p = line.split()
        if len(p) != 3 or not p[2].isdigit():
            continue
        rows.setdefault(p[0], {})[p[1]] = int(p[2])
        if p[1] not in tags:
            tags.append(p[1])
with open(sys.argv[1], "w", newline="\n") as fh:
    w = csv.writer(fh, lineterminator="\n")
    w.writerow(["workload"] + tags)
    for label, v in rows.items():
        w.writerow([label] + [v.get(t, "") for t in tags])
print(open(sys.argv[1]).read())
