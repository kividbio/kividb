#!/usr/bin/env python3
"""One CSV row per scenario, from memtier's JSON output in a results directory.

    summarize.py <results-dir>  > summary.csv
"""
import glob
import json
import os
import re
import sys

directory = sys.argv[1]
label = os.path.basename(os.path.normpath(directory))
print("label,value_bytes,pipeline,workload,ops_per_sec,avg_latency_ms,p50_ms,p99_ms,p99_9_ms,"
      "used_memory_bytes,keys,bytes_per_key")


def memory(size):
    used = keys = ""
    try:
        for line in open(os.path.join(directory, f"memory-{size}.txt")):
            name, _, value = line.strip().partition(":")
            if name == "used_memory":
                used = value
            elif name == "keys":
                keys = value
    except OSError:
        pass
    per_key = ""
    if used.isdigit() and keys.isdigit() and int(keys) > 0:
        per_key = f"{int(used) / int(keys):.1f}"
    return used, keys, per_key


def scenarios():
    """(path, value size, pipeline, workload) for every result file."""
    for path in sorted(glob.glob(os.path.join(directory, "*.json"))):
        name = os.path.basename(path)
        match = re.match(r"d(\d+)_p(\d+)_r(\d+)-(\d+)\.json$", name)
        if match:
            size, pipeline, sets, gets = match.groups()
            yield path, size, pipeline, f"{sets}:{gets}"
            continue
        # Multi-key commands: mset10_d100.json, mget10_d100.json
        match = re.match(r"(m[sg]et\d+)_d(\d+)\.json$", name)
        if match:
            yield path, match.group(2), "1", match.group(1)


for path, size, pipeline, workload in scenarios():
    try:
        totals = json.load(open(path))["ALL STATS"]["Totals"]
    except (OSError, ValueError, KeyError) as error:
        print(f"{label},{size},{pipeline},{workload},ERROR {error},,,,,,,")
        continue
    percentiles = totals.get("Percentile Latencies", {})
    used, keys, per_key = memory(size)
    print(",".join([
        label, size, pipeline, workload,
        f"{totals.get('Ops/sec', 0):.0f}",
        f"{totals.get('Average Latency', totals.get('Latency', 0)):.3f}",
        f"{percentiles.get('p50.00', 0):.3f}",
        f"{percentiles.get('p99.00', 0):.3f}",
        f"{percentiles.get('p99.90', 0):.3f}",
        used, keys, per_key,
    ]))
