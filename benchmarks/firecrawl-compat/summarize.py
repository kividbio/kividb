#!/usr/bin/env python3
"""Builds results-<label>.json from the junit files of one or more runs of run_snips.sh, the
e2e-<label>.json of firecrawl_e2e.py and the server's INFO commandstats.

  python summarize.py kividb --junit run1/junit-kividb.xml run2/junit-kividb.xml ... \
      --e2e e2e-kividb.json --commandstats commandstats-kividb.txt --image kividb/kividb:v1.2.0-lua
"""
import argparse
import json
import xml.etree.ElementTree as ET

ap = argparse.ArgumentParser()
ap.add_argument("label")
ap.add_argument("--junit", nargs="+", required=True)
ap.add_argument("--e2e")
ap.add_argument("--commandstats")
ap.add_argument("--image", required=True)
args = ap.parse_args()

runs = []
for i, path in enumerate(args.junit, 1):
    counts = {"passed": 0, "failed": 0, "skipped": 0}
    failures = []
    for tc in ET.parse(path).iter("testcase"):
        name = f"{tc.get('classname')} > {tc.get('name')}"
        if tc.find("skipped") is not None:
            counts["skipped"] += 1
            continue
        f = tc.find("failure")
        if f is None:
            f = tc.find("error")
        if f is not None:
            counts["failed"] += 1
            failures.append({"test": name, "message": (f.get("message") or "")[:300]})
        else:
            counts["passed"] += 1
    runs.append({"run": i, "counts": counts, "failures": failures})

out = {
    "label": args.label,
    "server_image": args.image,
    "firecrawl": "v2.11.0",
    "suite": "pnpm harness pnpm test:snips (fetch engine, no proxy, no AI key, no search engine)",
    "runs": runs,
}
if args.e2e:
    with open(args.e2e) as f:
        e2e = json.load(f)["checks"]
    out["e2e"] = {"counts": {s: sum(1 for c in e2e if c["status"] == s) for s in ("PASS", "FAIL")}, "checks": e2e}
if args.commandstats:
    with open(args.commandstats) as f:
        out["commands_seen"] = sorted(
            l.split(":", 1)[0].replace("cmdstat_", "").upper() for l in f if l.startswith("cmdstat_"))
path = f"results-{args.label}.json"
with open(path, "w") as f:
    json.dump(out, f, indent=2)
print(path, [r["counts"] for r in runs], out.get("e2e", {}).get("counts"))
