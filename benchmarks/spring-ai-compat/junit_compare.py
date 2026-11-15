#!/usr/bin/env python3
"""Compare JUnit XML files test by test.

usage: junit_compare.py LABEL=FILE [LABEL=FILE ...] [--json OUT]
Prints counts per file and every test whose outcome differs between files, with the first line of
the failure message.
"""
import json
import sys
import xml.etree.ElementTree as ET


def load(path):
    import glob
    import os
    out = {}
    files = sorted(glob.glob(os.path.join(path, "TEST-*.xml"))) if os.path.isdir(path) else [path]
    cases = [tc for f in files for tc in ET.parse(f).getroot().iter("testcase")]
    for tc in cases:
        name = f"{tc.get('classname')}::{tc.get('name')}"
        status, msg = "passed", ""
        for tag in ("failure", "error", "skipped"):
            el = tc.find(tag)
            if el is not None:
                status = {"failure": "failed", "error": "error", "skipped": "skipped"}[tag]
                msg = (el.get("message") or el.text or "").strip().splitlines()[:1]
                msg = msg[0][:300] if msg else ""
                break
        out[name] = (status, msg)
    return out


def main():
    args = sys.argv[1:]
    jout = None
    if "--json" in args:
        i = args.index("--json"); jout = args[i + 1]; del args[i:i + 2]
    runs = {}
    for a in args:
        label, path = a.split("=", 1)
        runs[label] = load(path)
    labels = list(runs)
    summary = {}
    for l in labels:
        c = {}
        for s, _ in runs[l].values():
            c[s] = c.get(s, 0) + 1
        summary[l] = c
        print(f"{l}: {sum(c.values())} tests {c}")
    names = sorted(set().union(*[set(r) for r in runs.values()]))
    diffs = []
    for n in names:
        st = {l: runs[l].get(n, ("absent", ""))[0] for l in labels}
        if len(set(st.values())) > 1:
            d = {"test": n, **{l: {"status": runs[l].get(n, ("absent", ""))[0],
                                   "message": runs[l].get(n, ("", ""))[1]} for l in labels}}
            diffs.append(d)
            print(f"DIFF {n}")
            for l in labels:
                s, m = runs[l].get(n, ("absent", ""))
                print(f"   {l:12} {s:8} {m}")
    failing_everywhere = [n for n in names if all(runs[l].get(n, ("absent",))[0] in ("failed", "error") for l in labels)]
    print(f"failing on every server: {len(failing_everywhere)}")
    for n in failing_everywhere:
        print(f"   {n}  ({runs[labels[0]][n][1][:160]})")
    if jout:
        json.dump({"summary": summary, "differences": diffs, "failing_everywhere": failing_everywhere,
                   "tests": {l: {n: s for n, (s, _) in r.items()} for l, r in runs.items()}},
                  open(jout, "w"), indent=1)


if __name__ == "__main__":
    main()
