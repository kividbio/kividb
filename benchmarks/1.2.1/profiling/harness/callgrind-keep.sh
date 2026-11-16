#!/usr/bin/env bash
# callgrind-keep.sh TAG:REV TAG:REV    e.g.  callgrind-keep.sh v120:v1.2.0 v121:878746e4
# The 1.2.0 suite's callgrind-keep.sh (scripts/perf build.sh + measure.sh in the
# kividb-ci-valgrind image pinned to rust:1.99.1-bookworm; LTO=fat, debug=1,
# WORKLOADS=full, PROFILE, N=60000, io_uring, one worker thread, pipeline 16),
# with these differences:
#  - measure.sh is patched by patch_measure.py: it keeps each callgrind file,
#    takes ONLY= and PRELOAD2=, and has five hash workloads after the 18 of
#    WORKLOADS=full (three with field TTLs). The 18 run exactly as before.
#  - the two trees are built at the same time, then measured at the same time
#    (two containers, each its own network namespace, port and /tmp); valgrind
#    runs a process's threads one at a time, so a run uses about one core and
#    the counts do not depend on the other run.
# Output: ~/out/callgrind/{results.txt,table.txt,split.txt,<tag>-profile-*.txt,
#         run-<tag>.log,build-<tag>.log,cg/<tag>-<workload>.out.gz,sha256.txt,
#         toolchain.txt,revs.txt}
set -eu
LTO=${LTO:-fat}
OUTD=~/out/callgrind
mkdir -p "$OUTD/cg" ~/cg-image
cat > ~/cg-image/Dockerfile << 'EOF'
FROM rust:1.99.1-bookworm
RUN apt-get update -qq && apt-get install -y -qq valgrind redis-tools cmake clang git python3 > /dev/null \
 && rm -rf /var/lib/apt/lists/*
EOF
sudo docker build -q -t kividb-ci-valgrind ~/cg-image > /dev/null
sudo docker run --rm kividb-ci-valgrind bash -c 'rustc --version; cargo --version; valgrind --version; redis-benchmark --version' \
  | tee "$OUTD/toolchain.txt"
cd ~/kividb
REPO=$(pwd)
# scripts/perf of the 1.2.1 tree (identical in v1.2.0), measure.sh patched
rm -rf ~/perf-keep && cp -r "$REPO/scripts/perf" ~/perf-keep
python3 ~/p/patch_measure.py ~/perf-keep/measure.sh
diff "$REPO/scripts/perf/measure.sh" ~/perf-keep/measure.sh > "$OUTD/measure.diff" || true
: > "$OUTD/revs.txt"
for spec in "$@"; do
  tag=${spec%%:*} rev=${spec#*:}
  echo "$tag $rev $(git rev-parse "$rev^{commit}")" >> "$OUTD/revs.txt"
done
# build both at once
pids=""
for spec in "$@"; do
  tag=${spec%%:*} rev=${spec#*:}
  sudo docker run --rm --dns 1.1.1.1 -e LTO="$LTO" -e OFFLINE= \
    -v "$REPO:/src:ro" -v ~/perf-keep:/perf:ro \
    -v kividb-cargo-registry:/usr/local/cargo/registry -v kividb-ci-target:/target \
    kividb-ci-valgrind bash -c "bash /perf/build.sh $tag $rev && sha256sum /target/perf-$tag/release/kividb && timeout 10 /target/perf-$tag/release/kividb --version" \
    > "$OUTD/build-$tag.log" 2>&1 &
  pids="$pids $!"
  sleep 20   # let the first fetch the registry index before the second starts
done
for p in $pids; do wait "$p"; done
for spec in "$@"; do
  tag=${spec%%:*}
  tail -4 "$OUTD/build-$tag.log"
  grep -E "^[0-9a-f]{64}  /target/perf-$tag/release/kividb$" "$OUTD/build-$tag.log" | sed "s#^#$tag #" >> "$OUTD/sha256.txt"
done
# measure both at once
pids=""
for spec in "$@"; do
  tag=${spec%%:*}
  rm -rf "$REPO/target/perf/out-$tag"; mkdir -p "$REPO/target/perf/out-$tag"
  sudo docker run --rm --security-opt seccomp=unconfined \
    -e MODEL=uring -e N=60000 -e WORKLOADS=full -e PROFILE="$tag" \
    -v ~/perf-keep:/perf:ro -v "$REPO/target/perf/out-$tag:/out" -v kividb-ci-target:/target \
    kividb-ci-valgrind bash -c "umask 022; bash /perf/measure.sh $tag; chmod -R a+r /out" \
    > "$OUTD/run-$tag.log" 2>&1 &
  pids="$pids $!"
done
for p in $pids; do wait "$p"; done
: > "$REPO/target/perf/results.txt"
for spec in "$@"; do
  tag=${spec%%:*}
  cat "$OUTD/run-$tag.log"
  grep -E "^[A-Z0-9-]+ $tag [0-9]+$" "$REPO/target/perf/out-$tag/raw.txt" >> "$REPO/target/perf/results.txt"
  for f in "$REPO/target/perf/out-$tag"/profile-*.txt; do [ -f "$f" ] && cp "$f" "$OUTD/$tag-$(basename "$f")"; done
  for f in "$REPO/target/perf/out-$tag"/cg-*.out; do
    [ -f "$f" ] && gzip -c "$f" > "$OUTD/cg/$tag-$(basename "$f" .out | sed 's/^cg-//').out.gz"
  done
done
cp "$REPO/target/perf/results.txt" "$OUTD/results.txt"
python3 scripts/perf/table.py "$REPO/target/perf/results.txt" | tee "$OUTD/table.txt"
python3 ~/p/cg_split.py "$OUTD/cg" > "$OUTD/split.txt"
cat "$OUTD/split.txt"
echo CALLGRIND-DONE
