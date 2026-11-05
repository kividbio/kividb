#!/usr/bin/env bash
# run_rc10.sh <tag>: everything this suite runs on one host (run on amd64 and on arm64).
#   0. rc10_checks.sh first (what changed since rc7: kividb.lock, systemd restart/stop, new defaults, rpm deps)
#   1. the rc4 harness unchanged, from the repo (benchmarks/1.2.0/smoke/harness at the tag)
#   2. the repo's tests/e2e command-surface suite against the published default image (RESP2, RESP3)
#   3. extra_checks.sh (arch of every tag, redis-benchmark, precise variant checks, deb/rpm under systemd)
set -u
TAG=${1:?tag}
H=$(cd "$(dirname "$0")" && pwd)
ARCH=$(dpkg --print-architecture)
R=$HOME/smoke-results/$ARCH; mkdir -p "$R"
SH=$HOME/src/benchmarks/1.2.0/smoke/harness
date -u +"start %FT%TZ" > "$R/timing.txt"
sg docker -c "bash $H/rc10_checks.sh $TAG" > "$R/rc10.out" 2>&1   # first: the new checks
date -u +"rc10 done %FT%TZ" >> "$R/timing.txt"
sg docker -c "bash $SH/smoke_all.sh $TAG $HOME/src" > "$R/smoke_all.out" 2>&1
date -u +"smoke_all done %FT%TZ" >> "$R/timing.txt"
sg docker -c "bash $SH/e2e_published.sh $TAG $HOME/src $R" > "$R/e2e.out" 2>&1
date -u +"e2e done %FT%TZ" >> "$R/timing.txt"
sg docker -c "bash $H/extra_checks.sh $TAG" > "$R/extra.out" 2>&1
date -u +"extra done %FT%TZ" >> "$R/timing.txt"
echo RUN-RC10-DONE >> "$R/timing.txt"
