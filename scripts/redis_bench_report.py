#!/usr/bin/env python3
"""
Render redis-benchmark CSV output as a markdown table, optionally comparing
against a baseline run and gating on regression.

Without --baseline the output is purely informational. That is deliberate:
absolute throughput on a shared GitHub-hosted runner is far too noisy to
threshold against a fixed number, and a benchmark that fails randomly is a
benchmark people learn to ignore. A pass/fail verdict is only produced when
there is a baseline measured on the same machine in the same run.
Output is plain ASCII: this gets printed to a GitHub step summary, but also to
developer terminals whose stdout encoding is not UTF-8 (Windows cp1252 raises
UnicodeEncodeError on emoji), and a benchmark reporter that crashes on the
machine you are debugging from is worse than useless.
"""
from __future__ import annotations

import argparse
import csv
import sys


def load(path: str) -> dict[str, float]:
    """test name -> requests/sec."""
    out: dict[str, float] = {}
    with open(path, newline="") as fh:
        for row in csv.reader(fh):
            if not row or row[0] == "test":
                continue
            try:
                out[row[0].strip('"')] = float(row[1])
            except (IndexError, ValueError):
                continue
    return out


def fmt(n: float) -> str:
    return f"{n:,.0f}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--current", required=True)
    ap.add_argument("--baseline")
    ap.add_argument("--label", default="")
    ap.add_argument("--max-regression", type=float, default=15.0)
    ap.add_argument(
        "--baseline-label",
        default="baseline",
        help="Column header for the comparison arm, e.g. 'redis'.",
    )
    ap.add_argument(
        "--current-label",
        default="current",
        help="Column header for the arm under test, e.g. 'kividb'.",
    )
    ap.add_argument(
        "--no-gate",
        action="store_true",
        help="Report the comparison without a pass/fail verdict. Used for the "
        "kividb-vs-redis table, where being slower at some command is a fact "
        "to look at, not a build failure.",
    )
    args = ap.parse_args()

    cur = load(args.current)
    if not cur:
        print(f"**No benchmark rows parsed from `{args.current}`** — treating as failure.")
        return 1

    title = "redis-benchmark"
    if args.label:
        title += f" — {args.label}"
    print(f"### {title}\n")

    if not args.baseline:
        print("| command | requests/sec |")
        print("|---|---:|")
        for name in sorted(cur):
            print(f"| `{name}` | {fmt(cur[name])} |")
        print(
            "\n_No baseline supplied — reported for the record only. "
            "Absolute numbers on a shared hosted runner are not stable enough "
            "to gate on; re-run with `baseline_ref` for a pass/fail comparison._"
        )
        return 0

    base = load(args.baseline)
    bl, cl = args.baseline_label, args.current_label
    ratio_col = f"{cl} / {bl}" if args.no_gate else "delta"
    print(f"| command | {bl} | {cl} | {ratio_col} |")
    print("|---|---:|---:|---:|")

    regressions: list[str] = []
    faster = slower = 0
    for name in sorted(cur):
        c = cur[name]
        b = base.get(name)
        if not b:
            print(f"| `{name}` | - | {fmt(c)} | new |")
            continue
        delta = (c - b) / b * 100.0
        if args.no_gate:
            # A ratio reads better than a percentage when the two arms are
            # different servers rather than two builds of the same one.
            if c >= b:
                faster += 1
            else:
                slower += 1
            print(f"| `{name}` | {fmt(b)} | {fmt(c)} | {c / b:.2f}x |")
            continue
        flag = ""
        if delta < -args.max_regression:
            flag = "  **REGRESSION**"
            regressions.append(f"{name}: {delta:+.1f}% ({fmt(b)} -> {fmt(c)})")
        print(f"| `{name}` | {fmt(b)} | {fmt(c)} | {delta:+.1f}%{flag} |")

    missing = sorted(set(base) - set(cur))
    if missing:
        print(f"\n**Missing from {cl} run:** {', '.join(f'`{m}`' for m in missing)}")

    if args.no_gate:
        total = faster + slower
        if total:
            print(
                f"\n{cl} is at least as fast on {faster}/{total} commands, "
                f"slower on {slower}/{total}."
            )
        print(
            f"\n_Same runner, same moment, identical workload — but not a like-for-like "
            f"engine comparison: {bl} is single-threaded per instance while kividb uses "
            f"all available cores, and this runs one server at a time on a shared hosted "
            f"runner. Treat it as a shape check across the command surface (which families "
            f"are unexpectedly slow), not as a benchmark result to quote._"
        )
        return 0

    if regressions:
        print(f"\n**Regressions beyond {args.max_regression:.0f}%:**\n")
        for r in regressions:
            print(f"- {r}")
        print(
            "\n_Hosted runners are noisy; confirm by re-running before treating "
            "a single flagged command as real._"
        )
        return 1

    print(f"\nNo command regressed by more than {args.max_regression:.0f}%.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
