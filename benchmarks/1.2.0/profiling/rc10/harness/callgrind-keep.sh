#!/usr/bin/env bash
# callgrind-keep.sh TAG:REV...   e.g.  callgrind-keep.sh rc8:082dab3d rc10:39bfacd2
# The suite's callgrind.sh (scripts/perf/run.sh: build.sh + measure.sh in the
# kividb-ci-valgrind image; LTO=fat, WORKLOADS=full, PROFILE=1, N=60000, uring,
# one worker thread), with two differences that do not change what is counted:
#  - the image is pinned to rust:1.99.1-bookworm (the toolchain of the published
#    and rc8 runs) instead of rust:1-bookworm;
#  - measure.sh also keeps each workload's callgrind output file
#    (/out/cg-<workload>.out), so the count can be split by object and function
#    afterwards (cg_split.py): the whole process, glibc memcmp, and KiviDB's own
#    binary.
# The docker command line is run.sh's.
# Output: ~/out/callgrind/{results.txt,table.txt,<tag>-profile-*.txt,run-<tag>.log,
#         cg/<tag>-<workload>.out.gz,sha256.txt,toolchain.txt,revs.txt}
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
# scripts/perf as of the checked-out tree, measure.sh patched to keep the file
rm -rf ~/perf-keep && cp -r "$REPO/scripts/perf" ~/perf-keep
python3 - ~/perf-keep/measure.sh << 'PY'
import sys
p = sys.argv[1]
s = open(p).read()
anchor = '  echo "$label $tag $(( ${total:-0} / N ))"\n'
assert s.count(anchor) == 1, "anchor not found"
s = s.replace(anchor, anchor + '  cp "$f" "/out/cg-$label.out"\n')
open(p, "w").write(s)
PY
diff "$REPO/scripts/perf/measure.sh" ~/perf-keep/measure.sh || true
for spec in "$@"; do
  tag=${spec%%:*} rev=${spec#*:}
  echo "$tag $rev $(git rev-parse "$rev^{commit}")" >> "$OUTD/revs.txt"
  mkdir -p "$REPO/target/perf/out-$tag"
  sudo docker run --rm --security-opt seccomp=unconfined --dns 1.1.1.1 \
    -e MODEL=uring -e N=60000 -e WORKLOADS=full -e LTO="$LTO" -e OFFLINE= -e PROFILE="$tag" \
    -v "$REPO:/src:ro" -v ~/perf-keep:/perf:ro -v "$REPO/target/perf/out-$tag:/out" \
    -v kividb-cargo-registry:/usr/local/cargo/registry -v kividb-ci-target:/target \
    kividb-ci-valgrind bash -c "bash /perf/build.sh $tag $rev && sha256sum /target/perf-$tag/release/kividb && timeout 10 /target/perf-$tag/release/kividb --version; bash /perf/measure.sh $tag" \
    > "$OUTD/run-$tag.log" 2>&1
  tail -25 "$OUTD/run-$tag.log"
  grep -E "^[A-Z0-9-]+ $tag [0-9]+$" "$REPO/target/perf/out-$tag/raw.txt" >> "$REPO/target/perf/results.txt"
  grep -E "^[0-9a-f]{64}  /target/perf-$tag/release/kividb$" "$OUTD/run-$tag.log" | sed "s#^#$tag #" >> "$OUTD/sha256.txt" || true
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
