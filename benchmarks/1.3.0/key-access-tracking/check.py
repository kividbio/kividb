#!/usr/bin/env python3
"""The regression gate: tracking on against tracking off, the same binary.

    check.py raw.txt [raw.txt ...]     one raw.txt per run (WORKLOADS=access)

For each run, the cost of tracking on over off, in percent, for the pipelined
GET and SET cells (100 B and 1 KB, "-p16"); the median over all those cells
of all the runs is compared with THRESHOLD (2.5%). Exits 1 above it.
"""
import statistics
import sys

THRESHOLD = 2.5
costs = []
for path in sys.argv[1:]:
    cells = {}
    for line in open(path):
        parts = line.split()
        if len(parts) == 3 and "-p16-" in parts[0]:
            command, mode = parts[0].rsplit("-", 1)
            cells.setdefault(command, {})[mode] = int(parts[2])
    for command, modes in sorted(cells.items()):
        if "no" in modes and "yes" in modes:
            cost = 100 * (modes["yes"] - modes["no"]) / modes["no"]
            costs.append(cost)
            print(f"{path} {command}: {cost:+.2f}%")
if not costs:
    sys.exit("no pipelined cells found")
median = statistics.median(costs)
print(f"median {median:+.2f}% (limit {THRESHOLD}%)")
sys.exit(1 if median > THRESHOLD else 0)
