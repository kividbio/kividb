#!/usr/bin/env python3
"""Split each kept callgrind file's Ir by object and function (self cost).

    python3 cg_split.py <dir of <tag>-<workload>.out.gz> [N=60000]

Prints, per file, instructions per command:
  total      the whole process (what measure.sh reports, before rounding down)
  memcmp     glibc's __memcmp_* (the ifunc variant callgrind saw)
  nomemcmp   total - memcmp
  own        functions whose object is the kividb binary (KiviDB's own code,
             with everything Rust inlined into it; not glibc, ld.so or the vDSO)
  own+memcmp own plus glibc memcmp
  libc_other glibc without memcmp; other = anything else (ld.so, vdso, ???)
  own_noexp  own without the active-expire thread's functions
             (spawn_active_expire_task's closures, expire_pass, ShardMap::due):
             that thread wakes every 100 ms of wall-clock time, so how much of
             it falls in the measured window is not a per-command cost
and checks that the per-function self costs add up to the file's totals.
"""
import gzip
import os
import re
import sys
from collections import defaultdict

D = sys.argv[1]
N = int(sys.argv[2]) if len(sys.argv) > 2 else 60000
EXP = re.compile(r"spawn_active_expire_task|expire_pass|ShardMap>::due$")
NAMEREF = re.compile(r"^\((\d+)\)(?: (.*))?$")


def parse(path):
    names = {k: {} for k in ("ob", "fl", "fn")}
    cur_ob = cur_fn = ""
    self_cost = defaultdict(int)
    total = None
    skip_next = False
    op = gzip.open if path.endswith(".gz") else open
    with op(path, "rt", errors="replace") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line:
                continue
            c = line[0]
            if c.isdigit() or c in "+-*":
                if skip_next:  # the inclusive cost of a call, not self cost
                    skip_next = False
                    continue
                parts = line.split()
                if len(parts) >= 2:
                    self_cost[(cur_ob, cur_fn)] += int(parts[1])
                continue
            if "=" in line:
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
                    elif key == "fn":
                        cur_fn = name
                    continue
                if key == "calls":
                    skip_next = True
                    continue
                continue
            if line.startswith("totals:") or line.startswith("summary:"):
                total = int(line.split()[1])
    return self_cost, total


def main():
    files = sorted(f for f in os.listdir(D) if f.endswith(".out.gz") or f.endswith(".out"))
    print("# file total memcmp nomemcmp own own+memcmp libc_other other own_noexp  (instructions per command, N=%d)" % N)
    for f in files:
        sc, total = parse(os.path.join(D, f))
        s = sum(sc.values())
        memcmp = sum(v for (ob, fn), v in sc.items() if "libc" in ob and fn.startswith("__memcmp"))
        own = sum(v for (ob, fn), v in sc.items() if ob.endswith("/release/kividb"))
        libc = sum(v for (ob, fn), v in sc.items() if "libc" in ob)
        other = s - own - libc
        exp = sum(v for (ob, fn), v in sc.items() if ob.endswith("/release/kividb") and EXP.search(fn))
        if total is not None and total != s:
            print(f"# WARNING {f}: functions add up to {s}, file says {total}")
        name = re.sub(r"\.out(\.gz)?$", "", f)
        print(f"{name} {s / N:.2f} {memcmp / N:.2f} {(s - memcmp) / N:.2f} {own / N:.2f} "
              f"{(own + memcmp) / N:.2f} {(libc - memcmp) / N:.2f} {other / N:.2f} {(own - exp) / N:.2f}")


main()
