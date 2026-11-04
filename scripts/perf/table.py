"""raw.txt lines 'LABEL tag Ir' -> a table, first tag as the baseline."""
import sys

rows, tags = {}, []
for line in open(sys.argv[1]):
    p = line.split()
    if len(p) != 3 or not p[2].isdigit():
        continue
    rows.setdefault(p[0], {})[p[1]] = int(p[2])
    if p[1] not in tags:
        tags.append(p[1])
print(f"{'workload':10}" + "".join(f"{t:>14}" for t in tags))
for label, v in rows.items():
    line = f"{label:10}" + "".join(f"{v.get(t, 0):>14}" for t in tags)
    base = v.get(tags[0], 0)
    if len(tags) > 1 and base:
        line += f"   {(v.get(tags[-1], 0) - base) * 100.0 / base:+6.1f}%"
    print(line)
