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
#                       for a change in what the code does, not for a
#                       difference of a few dozen instructions: inlining
#                       differs between the two (#867)
#   WORKLOADS=full      also INCR and DEL, and each of GET, SET, INCR, DEL
#                       with a TTL on every key and under allkeys-lru, and
#                       GET, SET and INCR under allkeys-lfu
#   WORKLOADS=tags      what the hash-tag locks cost a command (#889):
#                       GET/SET of untagged and tagged keys, MGET of 10,
#                       a tagged EVALSHA, MULTI + 2 SET + EXEC (see
#                       measure.sh); with EXEC=1 and CMP=plain
#   WORKLOADS=stream    what feeding the replication stream costs a write
#                       (#894): SET-100 with no backlog, with a kept one
#                       (started from a snapshot), with a replica attached,
#                       with the AOF on; and GET-100
#   WORKLOADS=sets      set commands on one set of each form and size
#                       (#764): SADD, SISMEMBER hit and miss, SMISMEMBER,
#                       SCARD, SRANDMEMBER, SREM + SADD and SPOP + SADD
#                       pairs at 10, 128 (block), 512 (integer array),
#                       1k, 100k, 3M (table); SMEMBERS, SUNIONSTORE,
#                       SINTER and COPY of big sets (see measure.sh;
#                       SIZES limits the sizes)
#   WORKLOADS=zsets     sorted-set commands on one set of 128 (block),
#                       1k, 100k and 1M members (#875): ZADD of a member
#                       there and (with ZREM) of a new one, ZINCRBY,
#                       ZSCORE, ZRANK, ZRANGE 0 9 and from a random rank,
#                       ZRANGEBYSCORE, ZPOPMIN and ZREMRANGEBYRANK (each
#                       with a ZADD), ZRANDMEMBER (no count and 10),
#                       GEOSEARCH, and equal-score rows (see measure.sh;
#                       SIZES limits the sizes; at 1M members ZRANK and the
#                       range from a random rank run NDEEP (600) commands)
#   WORKLOADS=hashes    hash commands on one hash of each form and size
#                       (#764 step 3): HGET hit and miss, HSET of a field
#                       there, HDEL + HSET pairs, HINCRBY, HMGET, HLEN,
#                       HEXISTS, HSTRLEN, HGETALL at 10, 128, 512 (block),
#                       1k, 100k, 3M (table) (see measure.sh; SIZES limits
#                       the sizes, e.g. SIZES="10 1000")
#   ONLY=<regex>        measure only the workloads whose label matches
#   PROFILE=1           keep the hottest functions of each workload in
#                       target/perf/out-<tag>/profile-*.txt
#   WORKLOADS=zrand     ZRANDMEMBER on a sorted set of about 95,000
#                       members (#862): no count, 1, 10, 1000, -10
#                       WITHSCORES, -1000, and ZSCORE beside them; run it
#                       with N=2000 (a walk of the table is dear)
#   DNS=1 OFFLINE=      let cargo fetch a new dependency (the image's DNS is
#                       not always usable; this points it at 1.1.1.1)
#   FEATURES=lua,tls    build that variant instead of the default one
#   NOOP=1              add a no-op line to `run_command` first (see
#                       stability.sh, which uses it)
#   CMP=plain           count with a memcmp whose cost does not depend on
#                       where its operands lie (see plaincmp.c): two builds
#                       of the same code then count the same
#   EXEC=1              also a <workload>-EXEC row: what execute_command
#                       costs a command, all it calls included, without
#                       the I/O loop (whose count moves between two runs of
#                       one binary on io_uring)
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
  -e MODEL="$MODEL" -e ONLY="${ONLY:-}" -e N="${N:-60000}" -e WORKLOADS="${WORKLOADS:-}" -e LTO="${LTO:-off}" ${OFFLINE+-e OFFLINE="$OFFLINE"} ${PROFILE:+-e PROFILE="$TAG"} \
  -e FEATURES="${FEATURES:-}" -e NOOP="${NOOP:-}" -e CMP="${CMP:-}" -e EXEC="${EXEC:-}" -e SIZES="${SIZES:-}" -e NBIG="${NBIG:-}" \
  -v "$(cygpath -w "$REPO" 2>/dev/null || echo "$REPO"):/src:ro" \
  -v "$(cygpath -w "$HERE" 2>/dev/null || echo "$HERE"):/perf:ro" \
  -v "$(cygpath -w "$OUT/out-$TAG" 2>/dev/null || echo "$OUT/out-$TAG"):/out" \
  -v kividb-cargo-registry:/usr/local/cargo/registry -v kividb-ci-target:/target \
  kividb-ci-valgrind bash -c "bash /perf/build.sh $TAG ${REV:-WORK} && bash /perf/measure.sh $TAG" 2>&1 | grep -vE "^done$" | tail -12
grep -E "^[A-Z0-9-]+ $TAG [0-9]+$" "$OUT/out-$TAG/raw.txt" >> "$OUT/results.txt"
python "$HERE/table.py" "$OUT/results.txt"
