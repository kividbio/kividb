#!/usr/bin/env bash
# Instructions per command, whole server, pipelined: build a tree in the
# kividb-ci-valgrind image and measure it under callgrind.
#
#   scripts/perf/run.sh <tag>            the working tree as it is
#   REV=v1.0.5 scripts/perf/run.sh base  a commit or a tag
#
# Each run appends to target/perf/results.txt and prints the table of every
# tag measured so far, the first as the baseline. Wall-clock throughput on a
# laptop differs by a third from run to run; instruction counts do not.
#
# Environment:
#   MODEL=uring|tokio   I/O loop (default uring)
#   N=60000             commands measured per workload
#   LTO=fat             the release profile as shipped (slow to build);
#                       default off, which is enough to compare two trees
#   WORKLOADS=full      also INCR and DEL, and each of GET, SET, INCR, DEL
#                       with a TTL on every key and under allkeys-lru, and
#                       GET, SET and INCR under allkeys-lfu
#   PROFILE=1           keep the hottest functions of each workload in
#                       target/perf/out-<tag>/profile-*.txt
#   DNS=1 OFFLINE=      let cargo fetch a new dependency (the image's DNS is
#                       not always usable; this points it at 1.1.1.1)
#
# What it does not see: cache misses and anything in the kernel. A change to
# how keys and values are laid out has to be judged on real hardware too.
set -u
REPO=$(git rev-parse --show-toplevel)
HERE="$REPO/scripts/perf"
OUT="$REPO/target/perf"
TAG=${1:?usage: run.sh <tag>} MODEL=${MODEL:-uring}
mkdir -p "$OUT/out-$TAG"
MSYS_NO_PATHCONV=1 docker run --rm --security-opt seccomp=unconfined ${DNS:+--dns 1.1.1.1} \
  -e MODEL="$MODEL" -e N="${N:-60000}" -e WORKLOADS="${WORKLOADS:-}" -e LTO="${LTO:-off}" ${OFFLINE+-e OFFLINE="$OFFLINE"} ${PROFILE:+-e PROFILE="$TAG"} \
  -v "$(cygpath -w "$REPO" 2>/dev/null || echo "$REPO"):/src:ro" \
  -v "$(cygpath -w "$HERE" 2>/dev/null || echo "$HERE"):/perf:ro" \
  -v "$(cygpath -w "$OUT/out-$TAG" 2>/dev/null || echo "$OUT/out-$TAG"):/out" \
  -v kividb-cargo-registry:/usr/local/cargo/registry -v kividb-ci-target:/target \
  kividb-ci-valgrind bash -c "bash /perf/build.sh $TAG ${REV:-WORK} && bash /perf/measure.sh $TAG" 2>&1 | grep -vE "^done$" | tail -12
grep -E "^[A-Z0-9-]+ $TAG [0-9]+$" "$OUT/out-$TAG/raw.txt" >> "$OUT/results.txt"
python "$HERE/table.py" "$OUT/results.txt"
