#!/usr/bin/env python3
"""Folds run.sh's logs for one server into results-<label>.json: per suite, the counts and the
names of the tests that failed or errored.

  python3 summarize.py <label> "<version string>" <openviking ref>
"""
import json
import os
import re
import sys
import xml.etree.ElementTree as ET

HERE = os.path.dirname(os.path.abspath(__file__))
LOGS = os.path.join(HERE, "logs")


def junit(path):
    out = {"passed": 0, "failed": 0, "errors": 0, "skipped": 0, "not_passed": []}
    if not os.path.exists(path):
        out["missing"] = True
        return out
    for case in ET.parse(path).getroot().iter("testcase"):
        name = f"{case.get('classname')}::{case.get('name')}"
        if case.find("failure") is not None:
            out["failed"] += 1
            out["not_passed"].append(f"FAILED {name}")
        elif case.find("error") is not None:
            out["errors"] += 1
            out["not_passed"].append(f"ERROR {name}")
        elif case.find("skipped") is not None:
            out["skipped"] += 1
        else:
            out["passed"] += 1
    return out


def cargo(path):
    out = {"passed": 0, "failed": 0, "ignored": 0, "not_passed": []}
    if not os.path.exists(path):
        out["missing"] = True
        return out
    text = open(path, errors="replace").read()
    for m in re.finditer(r"test result: \w+\. (\d+) passed; (\d+) failed; (\d+) ignored", text):
        out["passed"] += int(m.group(1))
        out["failed"] += int(m.group(2))
        out["ignored"] += int(m.group(3))
    out["not_passed"] = sorted(set(re.findall(r"^test (\S+) \.\.\. FAILED", text, re.M)))
    return out


def main():
    label, version, ref = sys.argv[1], sys.argv[2], sys.argv[3]
    expiry_path = os.path.join(HERE, f"expiry-{label}.json")
    expiry = json.load(open(expiry_path)) if os.path.exists(expiry_path) else {"missing": True}
    result = {
        "label": label,
        "server": version.strip(),
        "openviking": ref,
        "suites": {
            "ragfs cargo test (REDIS_URL, cluster and password tests on)": cargo(os.path.join(LOGS, f"ragfs-{label}.txt")),
            "gateway pytest tests/gateway (standalone + 3-node cluster, ACL user)": junit(os.path.join(LOGS, f"gateway-{label}.xml")),
            "pathlock migration pytest (REDIS_URL)": junit(os.path.join(LOGS, f"pathlock-{label}.xml")),
        },
        "expiry_scheduler": {"counts": expiry.get("counts"), "checks": expiry.get("checks")},
    }
    plus = os.path.join(LOGS, f"gateway-{label}-plus-command.xml")
    if os.path.exists(plus):
        result["suites"]["gateway again, ACL user with +command (kividbio/kividb#392)"] = junit(plus)
    out = os.path.join(HERE, f"results-{label}.json")
    with open(out, "w") as f:
        json.dump(result, f, indent=2)
        f.write("\n")
    for name, s in result["suites"].items():
        print(f"   {name}: {s.get('passed')} passed, {s.get('failed')} failed, {s.get('errors', 0)} errors")
    print(f"   expiry scheduler: {expiry.get('counts')}")
    print(f"   wrote {out}")


if __name__ == "__main__":
    main()
