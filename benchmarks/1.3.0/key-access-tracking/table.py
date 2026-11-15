#!/usr/bin/env python3
"""Instructions per command with key-access-tracking off and on.

    table.py raw.txt        the raw.txt scripts/perf/measure.sh wrote
                            (WORKLOADS=access): lines "<command>-<no|yes> <tag> <Ir>"
"""
import sys

rows = {}
for line in open(sys.argv[1]):
    parts = line.split()
    if len(parts) != 3:
        continue
    command, mode = parts[0].rsplit("-", 1)
    rows.setdefault(command, {})[mode] = int(parts[2])

print("| command | tracking off | tracking on | difference | % |")
print("|---|---:|---:|---:|---:|")
for command, modes in rows.items():
    if "no" in modes and "yes" in modes:
        off, on = modes["no"], modes["yes"]
        print(f"| {command} | {off} | {on} | {on - off:+d} | {100 * (on - off) / off:+.2f}% |")
