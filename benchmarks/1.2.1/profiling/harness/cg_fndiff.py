#!/usr/bin/env python3
"""Per-function difference between two callgrind files (self cost and calls, per command).

    python3 cg_fndiff.py <a.out[.gz]> <b.out[.gz]> [N=60000] [rows=25]

Functions are matched by object basename + function name (the two binaries live
in different paths). `calls` is how many times the function was called, from
the callers' calls= lines, divided by N.
"""
import gzip
import os
import re
import sys
from collections import defaultdict

NAMEREF = re.compile(r"^\((\d+)\)(?: (.*))?$")


def parse(path):
    names = {k: {} for k in ("ob", "fl", "fn")}
    cur_ob = cur_fn = ""
    cfn = cob = ""
    self_cost = defaultdict(int)
    calls = defaultdict(int)
    skip_next = False
    op = gzip.open if path.endswith(".gz") else open
    with op(path, "rt", errors="replace") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line:
                continue
            c = line[0]
            if c.isdigit() or c in "+-*":
                if skip_next:
                    skip_next = False
                    continue
                parts = line.split()
                if len(parts) >= 2:
                    self_cost[(cur_ob, cur_fn)] += int(parts[1])
                continue
            if "=" not in line:
                continue
            key, val = line.split("=", 1)
            if key in ("ob", "fn", "cob", "cfn", "fl", "fi", "fe", "cfi", "cfl"):
                kind = {"cob": "ob", "cfn": "fn", "fi": "fl", "fe": "fl", "cfi": "fl", "cfl": "fl"}.get(key, key)
                m = NAMEREF.match(val)
                if m:
                    if m.group(2) is not None:
                        names[kind][m.group(1)] = m.group(2)
                    name = names[kind].get(m.group(1), val)
                else:
                    name = val
                if key == "ob":
                    cur_ob = name
                    cob = name
                elif key == "fn":
                    cur_fn = name
                    cob = cur_ob
                elif key == "cob":
                    cob = name
                elif key == "cfn":
                    cfn = name
            elif key == "calls":
                skip_next = True
                calls[(cob, cfn)] += int(val.split()[0])
                cob = cur_ob
    return self_cost, calls


def key(ob, fn):
    return os.path.basename(ob) + ":" + fn


def norm(d):
    out = defaultdict(int)
    for (ob, fn), v in d.items():
        out[key(ob, fn)] += v
    return out


a_s, a_c = parse(sys.argv[1])
b_s, b_c = parse(sys.argv[2])
N = int(sys.argv[3]) if len(sys.argv) > 3 else 60000
R = int(sys.argv[4]) if len(sys.argv) > 4 else 25
a_s, b_s, a_c, b_c = norm(a_s), norm(b_s), norm(a_c), norm(b_c)
ta, tb = sum(a_s.values()), sum(b_s.values())
print(f"# {os.path.basename(sys.argv[1])} -> {os.path.basename(sys.argv[2])}: "
      f"{ta / N:.2f} -> {tb / N:.2f} per command ({tb / N - ta / N:+.2f})")
same = sum(1 for k in set(a_s) | set(b_s) if a_s.get(k, 0) == b_s.get(k, 0))
print(f"# functions with the same self cost: {same} of {len(set(a_s) | set(b_s))}")
print("# self Ir per command a -> b (diff) | calls per command a -> b | object:function")
ks = sorted(set(a_s) | set(b_s), key=lambda k: -abs(b_s.get(k, 0) - a_s.get(k, 0)))
for k in ks[:R]:
    da, db = a_s.get(k, 0) / N, b_s.get(k, 0) / N
    if abs(db - da) < 0.005:
        break
    print(f"{da:9.2f} -> {db:9.2f} ({db - da:+8.2f}) | {a_c.get(k, 0) / N:7.3f} -> {b_c.get(k, 0) / N:7.3f} | {k[:150]}")
