#!/usr/bin/env python3
"""Markdown table from smoke_all.sh's summary.tsv files.

  smoke_table.py results/amd64/summary.tsv results/arm64/summary.tsv > results/table.md
"""
import sys

KIND = {"image": "container image", "bin": "release tarball", "deb": ".deb package (systemd)", "rpm": ".rpm package"}


def main():
    rows = []
    for p in sys.argv[1:]:
        for line in open(p):
            f = line.rstrip("\n").split("\t")
            if len(f) < 6:
                continue
            rows.append(f)
    print("| Artifact | Architecture | Kind | Variant | Checks passed | Failed | Result |")
    print("|---|---|---|---|---|---|---|")
    tp = tf = 0
    for label, kind, ref, variant, p, fl, *rest in rows:
        arch = label.rsplit("-", 1)[1]
        p, fl = int(p), int(fl) if fl.isdigit() else 1
        tp += p
        tf += fl
        name = ref.split("/")[-1] if kind in ("bin", "deb", "rpm") else ref
        if kind == "bin":
            name = ("kividb" if variant == "default" else f"kividb-{variant}") + \
                   ("-linux-x86_64.tar.gz" if arch == "amd64" else "-linux-aarch64.tar.gz")
        res = "PASS" if fl == 0 and p > 0 else "FAIL " + " ".join(rest)
        print(f"| `{name}` | {arch} | {KIND.get(kind, kind)} | {variant} | {p} | {fl} | {res} |")
    print(f"\n{len(rows)} artifact runs, {tp} checks passed, {tf} failed.")


if __name__ == "__main__":
    main()
