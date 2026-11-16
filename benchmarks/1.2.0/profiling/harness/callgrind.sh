#!/usr/bin/env bash
# callgrind.sh TAG:REV...  e.g.  callgrind.sh v113:v1.1.3 rc4:v1.2.0-rc4
# Instructions per command, whole server, one worker thread, pipeline 16,
# with the repo's own scripts/perf/run.sh (LTO=fat: the shipped release
# profile; WORKLOADS=full; PROFILE=1 keeps each workload's hottest functions).
# run.sh needs a Docker image called kividb-ci-valgrind (a Rust toolchain plus
# valgrind and redis-benchmark); this builds one from rust:1-bookworm.
# REDIS=1 then measures Redis 8.10.3 the same way (callgrind-redis.sh).
# Output: ~/out/callgrind/{results.txt,table.txt,<tag>-profile-*.txt,run-<tag>.log}
set -eu
LTO=${LTO:-fat}
HERE=$(cd "$(dirname "$0")" && pwd)
mkdir -p ~/out/callgrind ~/cg-image
cat > ~/cg-image/Dockerfile << 'EOF'
FROM rust:1-bookworm
RUN apt-get update -qq && apt-get install -y -qq valgrind redis-tools cmake clang git python3 > /dev/null \
 && rm -rf /var/lib/apt/lists/*
EOF
sudo docker build -q -t kividb-ci-valgrind ~/cg-image > /dev/null
sudo docker run --rm kividb-ci-valgrind bash -c 'rustc --version; valgrind --version; redis-benchmark --version' \
  | tee ~/out/callgrind/toolchain.txt
cd ~/kividb
git fetch -q --tags origin
for spec in "$@"; do
  tag=${spec%%:*} rev=${spec#*:}
  echo "$tag $rev $(git rev-parse "$rev^{commit}")" >> ~/out/callgrind/revs.txt
  # run.sh calls docker; run it in the docker group
  sg docker -c "REV=$rev LTO=$LTO OFFLINE= DNS=1 PROFILE=1 WORKLOADS=full bash scripts/perf/run.sh $tag" \
    > ~/out/callgrind/run-"$tag".log 2>&1
  tail -25 ~/out/callgrind/run-"$tag".log
  for f in target/perf/out-"$tag"/profile-*.txt; do [ -f "$f" ] && cp "$f" ~/out/callgrind/"$tag-$(basename "$f")"; done
done
cp target/perf/results.txt ~/out/callgrind/results.txt
python3 scripts/perf/table.py target/perf/results.txt | tee ~/out/callgrind/table.txt
if [ "${REDIS:-0}" = 1 ]; then bash "$HERE/callgrind-redis.sh" | tee ~/out/callgrind/redis.txt; fi
echo CALLGRIND-DONE
