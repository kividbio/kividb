#!/usr/bin/env bash
# hset-recheck.sh: on the server after session.sh. The matrix had 1.2.1's HSET at 100 B, pipeline 16
# 10% below 1.2.0's (inside the published spread, but p50 higher on all 6 runs), so the hash group
# alone (HGET, HGETALL, HSET at 100 B, pipeline 16: a fresh server, a fresh load, 1 warm-up + 3 runs
# each) ran 4 more times per build, alternating 1.2.1, 1.2.0, 1.2.0, 1.2.1, 1.2.1, 1.2.0, 1.2.0, 1.2.1.
# Output: ~/out/recheck/hrc-<build>-r<N>/ (summarize.py reads it as engines hrc-121 / hrc-120).
set -u
cd ~
mkdir -p out/recheck
n=0
for e in 121 120 120 121 121 120 120 121; do
  n=$((n + 1))
  r=hrc-$e-r$(( (n + 1) / 2 ))
  env RUN="$r" MEMPASS=0 GROUPS_=hash SIZES=100 ONLY_P=16 bash harness/datatypes.sh kividb-$e > out/log-$r.txt 2>&1
  mv out/datatypes/$r out/recheck/
  echo "$(date -u +%T) $r $(tail -1 out/log-$r.txt)" | tee -a ~/recheck.log
done
python3 harness/summarize.py out/recheck out/recheck-summary | tee -a ~/recheck.log
grep -E "HSET|HGET" out/recheck-summary/summary.csv | tee -a ~/recheck.log
echo RECHECK-DONE | tee -a ~/recheck.log
