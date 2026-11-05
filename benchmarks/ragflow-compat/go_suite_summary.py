#!/usr/bin/env python3
"""Summarise `go test -json` output of RAGFlow's test/api suite: one status per test, and the last
lines of output of each test that failed.

  python go_suite_summary.py go-suite.raw.json suite-1.0-<label>.json <label> <image>
  python go_suite_summary.py --compare suite-1.0-kvrocks.json suite-1.0-kividb.json

Subtest names that carry generated IDs (hex) differ from run to run; they are compared with the
IDs replaced by <id>, and a name that then covers several subtests gets their statuses together.
"""
import json
import re
import sys
from collections import Counter
from datetime import datetime, timezone

HEX = re.compile(r"[0-9a-fA-F]{16,}")


def normalised(tests):
    by = {}
    for name, st in tests.items():
        by.setdefault(HEX.sub("<id>", name), Counter())[st] += 1
    return {k: ", ".join(f"{s} x{n}" if n > 1 else s for s, n in sorted(c.items())) for k, c in sorted(by.items())}


if sys.argv[1] == "--compare":
    a, b = (json.load(open(p, encoding="utf-8")) for p in sys.argv[2:4])
    na, nb = normalised(a["tests"]), normalised(b["tests"])
    diff = {k: (na.get(k), nb.get(k)) for k in sorted(set(na) | set(nb)) if na.get(k) != nb.get(k)}
    print(f"{a['label']}: {a['counts']}   {b['label']}: {b['counts']}")
    for k, (x, y) in diff.items():
        print(f"  {k}: {a['label']} {x} / {b['label']} {y}")
    print("no difference" if not diff else f"{len(diff)} difference(s)")
    sys.exit(0)

raw, out, label, image = sys.argv[1:5]
status, output, pkg = {}, {}, {}
with open(raw, encoding="utf-8") as f:
    for line in f:
        try:
            ev = json.loads(line)
        except ValueError:
            continue
        test, action = ev.get("Test"), ev.get("Action")
        if action == "output":
            output.setdefault(test or "", []).append(ev.get("Output", ""))
        elif action in ("pass", "fail", "skip"):
            if test:
                status[test] = action.upper()
            else:
                pkg = {"status": action.upper(), "elapsed_s": ev.get("Elapsed")}
counts = {s: sum(1 for v in status.values() if v == s) for s in ("PASS", "FAIL", "SKIP")}
failed = {t: "".join(output.get(t, []))[-1500:] for t, s in status.items() if s == "FAIL"}
with open(out, "w", encoding="utf-8") as f:
    json.dump({"label": label, "image": image, "suite": "ragflow test/api (go test -tags integration ./test/api)",
               "package": pkg, "counts": counts, "tests": dict(sorted(status.items())),
               "tests_normalised": normalised(status), "failed_output": failed,
               "setup_output": "".join(output.get("", []))[-3000:],
               "when": datetime.now(timezone.utc).isoformat(timespec="seconds")}, f, indent=1)
print(f"go suite {label}: {counts} -> {out}")
