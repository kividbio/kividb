#!/usr/bin/env bash
# The no-op-line check (#867): does a line added to `run_command` move the
# instructions per command? It should move them by the line's own cost (a
# compare and a branch) and no more. When it moves them by tens, the
# compiler's choices inside the function have flipped (a call no longer
# inlined, a closure made a real call), and every PR that touches the
# function pays, or is credited with, a cost that is not its own.
#
#   LTO=fat scripts/perf/stability.sh <tag>
#
# Builds the working tree twice with run.sh, as <tag> and as <tag>-noop (the
# same tree with a never-taken branch added at the "perf/stability.sh:
# no-op line" marker in src/commands/handler.rs; see build.sh), measures
# both on each I/O loop, and prints the difference per workload.
#
# The verdict is on the -EXEC rows (EXEC=1 in measure.sh): what
# `execute_command` costs a command with all it calls, which is the same
# from run to run of one binary. It fails if one moved by more than LIMIT
# (default 3). The whole-server rows are printed too, for information: on
# io_uring they move by tens between two runs of the same binary, with how
# many commands each wake-up finds.
#
# Environment: as run.sh.
# - LTO=fat: the release profile as shipped. Use it for a verdict: what
#   moves only with LTO off is not in the binary that ships, and the two
#   inline differently (#867: a flip seen with LTO off was not there with
#   fat LTO).
# - FEATURES=lua,tls checks the other variant; MODELS="uring tokio" by
#   default.
# - CMP=plain by default (see plaincmp.c): with glibc's memcmp, a change of
#   stack layout alone moves a command by about 40 instructions (#877), which
#   is not what this checks. CMP= measures with glibc's.
#
# Measured for #867 (fat LTO, plain memcmp), the probe moved only
# run_command's own count: +6 on master, and after the change -8 (default)
# and -2 to -4 (lua,tls), which is register allocation inside the function,
# not a call flipping. A row that moved by more: rerun both tags with
# PROFILE=1 and compare target/perf/out-<tag>/profile-<workload>.txt with
# the -noop one function by function, to see which call stopped (or started)
# being inlined.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
TAG=${1:?usage: stability.sh <tag>}
# results.txt keeps every run: this run's rows carry a tag of their own, so
# that a row left from an earlier run is never read as this one's.
TAG="$TAG-$(date +%Y%m%d-%H%M%S)"
LIMIT=${LIMIT:-3}
export CMP=${CMP-plain} EXEC=1
OUT="$(git rev-parse --show-toplevel)/target/perf"
status=0
for model in ${MODELS:-uring tokio}; do
  MODEL=$model bash "$HERE/run.sh" "$TAG-$model" > /dev/null
  MODEL=$model NOOP=1 bash "$HERE/run.sh" "$TAG-$model-noop" > /dev/null
  echo "== $model: $TAG-$model -> $TAG-$model-noop (in $OUT/results.txt)"
  awk -v a="$TAG-$model" -v b="$TAG-$model-noop" -v limit="$LIMIT" '
    $2 == a { if (!($1 in base)) order[++n] = $1; base[$1] = $3 }
    $2 == b { noop[$1] = $3 }
    END {
      bad = 0
      # No -EXEC row at all (a build or a run that failed) is no verdict.
      exec = 0
      for (i = 1; i <= n; i++) if (order[i] ~ /-EXEC$/) exec++
      if (exec == 0) { print "no -EXEC rows for " a; exit 1 }
      for (i = 1; i <= n; i++) {
        w = order[i]
        if (!(w in noop)) { printf "%-16s %8d  (no row for %s)  MISSING\n", w, base[w], b; bad = 1; continue }
        d = noop[w] - base[w]; flag = ""
        if (w ~ /-EXEC$/ && (d > limit || d < -limit)) { flag = "  MOVED"; bad = 1 }
        printf "%-16s %8d %8d %+6d%s\n", w, base[w], noop[w], d, flag
      }
      exit bad
    }' "$OUT/results.txt" || status=1
done
exit $status
