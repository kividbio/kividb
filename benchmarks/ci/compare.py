#!/usr/bin/env python3
"""Compare one harness's results on Redis and on KiviDB, check by check.

usage: compare.py NAME REDIS.xml KIVIDB.xml

Each file is the JUnit XML a harness's `ci.sh run` wrote (see
benchmarks/README.md). A check is a <testcase>, named classname::name: a
<failure> or <error> in it is a fail, <skipped> a skip, anything else a pass.

Prints a Markdown section for the job summary. Exits 1 when a check passes
on Redis and fails on KiviDB, the only thing that fails the job; a check
Redis passes that KiviDB's results lack (its run stopped early) counts as
failed there. Exits 0 otherwise, including when Redis's results are
missing: then there is nothing to compare with, and the section says so.
"""
import sys
import xml.etree.ElementTree as ET

MAX_ROWS = 40


def results(path):
    """{check: "pass" | "fail" | "skip"}, or None when the file is missing or unreadable."""
    try:
        root = ET.parse(path).getroot()
    except (OSError, ET.ParseError):
        return None
    out = {}
    for tc in root.iter("testcase"):
        key = f"{tc.get('classname', '')}::{tc.get('name', '')}".strip(":")
        if tc.find("failure") is not None or tc.find("error") is not None:
            out[key] = "fail"
        elif tc.find("skipped") is not None:
            out[key] = "skip"
        else:
            out[key] = "pass"
    return out


def tally(res):
    return ", ".join(f"{sum(1 for s in res.values() if s == k)} {k}" for k in ("pass", "fail", "skip"))


def main(name, redis_path, kividb_path):
    redis, kividb = results(redis_path), results(kividb_path)
    print(f"### {name}\n")
    if redis is None:
        print("No results from the run against Redis, so nothing to compare with: see the step's log.\n")
        return 0
    if kividb is None:
        kividb = {}
        print("No results from the run against KiviDB: every check Redis passes counts as failed on KiviDB.\n")

    regressions = [c for c, s in redis.items() if s == "pass" and kividb.get(c, "missing") in ("fail", "missing")]
    differ = sorted(c for c in set(redis) | set(kividb) if redis.get(c, "missing") != kividb.get(c, "missing"))

    print(f"Redis: {tally(redis)}. KiviDB: {tally(kividb)}.\n")
    if differ:
        print("| Check | Redis | KiviDB |")
        print("| --- | --- | --- |")
        for c in differ[:MAX_ROWS]:
            r, k = redis.get(c, "missing"), kividb.get(c, "missing")
            if c in regressions:
                k = f"**{k}**"
            print(f"| `{c}` | {r} | {k} |")
        if len(differ) > MAX_ROWS:
            print(f"\n{len(differ) - MAX_ROWS} more differences; the full results are in the artifact.")
        print()
    else:
        print("Same result for every check on both servers.\n")

    if regressions:
        print(f"**{len(regressions)} check(s) pass on Redis and fail on KiviDB.**\n")
        return 1
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 4:
        sys.exit(__doc__)
    sys.exit(main(*sys.argv[1:]))
