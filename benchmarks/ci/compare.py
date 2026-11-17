#!/usr/bin/env python3
"""Compare one harness's results on Redis and on KiviDB, check by check.

usage: compare.py NAME REDIS.xml KIVIDB.xml [KIVIDB-PASSES.txt]

Each file is the JUnit XML a harness's `ci.sh run` wrote (see
benchmarks/README.md). A check is a <testcase>, named classname::name: a
<failure> or <error> in it is a fail, <skipped> a skip, anything else a pass.
KIVIDB-PASSES.txt, when given, lists checks KiviDB passes (one
classname::name a line, # for comments), for a harness whose checks Redis
largely fails, so that comparing with Redis alone would miss most of them.

Prints a Markdown section for the job summary. Exits 1 when a check passes
on Redis and fails on KiviDB, or a listed check does not pass on KiviDB; a
check KiviDB's results lack (its run stopped early) counts as failed there.
Exits 1 too when either file is missing: the harness could not run there,
which the section says, rather than reading it as KiviDB failing.
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


def listed(path):
    """The checks a KIVIDB-PASSES file lists, in its order."""
    with open(path, encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip() and not line.lstrip().startswith("#")]


def tally(res):
    return ", ".join(f"{sum(1 for s in res.values() if s == k)} {k}" for k in ("pass", "fail", "skip"))


def main(name, redis_path, kividb_path, passes_path=None):
    redis, kividb = results(redis_path), results(kividb_path)
    print(f"### {name}\n")
    missing = [side for side, res in (("Redis", redis), ("KiviDB", kividb)) if res is None]
    if missing:
        print(f"Could not run against {' or '.join(missing)}: no results, see the step's log.\n")
        return 1

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

    lost = [c for c in listed(passes_path) if kividb.get(c, "missing") != "pass"] if passes_path else []
    if lost:
        print(f"**{len(lost)} check(s) listed as passing on KiviDB do not pass now:**\n")
        print("| Check | Redis | KiviDB |")
        print("| --- | --- | --- |")
        for c in lost[:MAX_ROWS]:
            print(f"| `{c}` | {redis.get(c, 'missing')} | **{kividb.get(c, 'missing')}** |")
        if len(lost) > MAX_ROWS:
            print(f"\n{len(lost) - MAX_ROWS} more; the full results are in the artifact.")
        print()

    if regressions:
        print(f"**{len(regressions)} check(s) pass on Redis and fail on KiviDB.**\n")
    return 1 if regressions or lost else 0


if __name__ == "__main__":
    if len(sys.argv) not in (4, 5):
        sys.exit(__doc__)
    sys.exit(main(*sys.argv[1:]))
