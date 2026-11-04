#!/usr/bin/env python3
"""Adds the outcome of n8n's own Redis integration tests (a vitest --reporter=json file) to a
results-<label>.json written by n8n_queue_e2e.py.

  python merge_results.py results-kividb.json vitest-kividb.json
"""
import json
import sys

results_path, vitest_path = sys.argv[1], sys.argv[2]
with open(results_path) as f:
    out = json.load(f)
with open(vitest_path) as f:
    v = json.load(f)

tests = []
for file in v["testResults"]:
    name = file["name"].split("packages/cli/", 1)[-1]
    for a in file["assertionResults"]:
        tests.append({
            "file": name,
            "test": a["fullName"],
            "status": a["status"],
            "failure": "\n".join(a.get("failureMessages") or [])[:400],
        })
counts = {}
for t in tests:
    counts[t["status"]] = counts.get(t["status"], 0) + 1
out["n8n_integration_tests"] = {"counts": counts, "tests": tests}
with open(results_path, "w") as f:
    json.dump(out, f, indent=2)
print(results_path, counts)
