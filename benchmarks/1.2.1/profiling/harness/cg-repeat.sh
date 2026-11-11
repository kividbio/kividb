#!/usr/bin/env bash
# cg-repeat.sh FIRST_ROUND LAST_ROUND WORKLOAD...   (on the VM, after callgrind-keep.sh)
# Repeats single workloads of the patched measure.sh (ONLY=...) on the two
# binaries callgrind-keep.sh built (already in the kividb-ci-target volume).
# Each round runs both builds at the same time, as the main run did, so the
# same workloads are the same runs as in the main run.
# Output: ~/out/callgrind/repeat/<tag>-<workload>-r<round>.out.gz, repeat.txt, split.txt
set -eu
R0=$1 R1=$2; shift 2
TAGS=${TAGS:-"v120 v121"}
OUTD=~/out/callgrind/repeat
mkdir -p "$OUTD"
for r in $(seq "$R0" "$R1"); do
  pids=""
  for tag in $TAGS; do
    d=~/rep/r$r-$tag
    rm -rf "$d"; mkdir -p "$d"
    sudo docker run --rm --security-opt seccomp=unconfined \
      -e MODEL=uring -e N=60000 -e WORKLOADS=full -e ONLY="$*" \
      -v ~/perf-keep:/perf:ro -v "$d:/out" -v kividb-ci-target:/target \
      kividb-ci-valgrind bash -c "umask 022; bash /perf/measure.sh $tag; chmod -R a+r /out" \
      > "$d.log" 2>&1 &
    pids="$pids $!"
  done
  for p in $pids; do wait "$p"; done
  for tag in $TAGS; do
    d=~/rep/r$r-$tag
    sed "s/\$/ r$r/" "$d/raw.txt" | tee -a "$OUTD/repeat.txt"
    for f in "$d"/cg-*.out; do
      w=$(basename "$f" .out | sed 's/^cg-//')
      gzip -c "$f" > "$OUTD/$tag-$w-r$r.out.gz"
    done
  done
done
python3 ~/p/cg_split.py "$OUTD" > "$OUTD/split.txt"
cat "$OUTD/split.txt"
echo REPEAT-DONE
