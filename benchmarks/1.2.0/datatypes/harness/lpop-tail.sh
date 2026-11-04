#!/usr/bin/env bash
# lpop-tail.sh ENGINE: repro for the LPOP tail seen in the matrix (KiviDB rc4,
# 100 B, pipeline 16: p99.9 46-68 ms in 2 of 3 runs). Fill 10,000 lists with
# 40M LPUSHes (100 B), then three LPOP runs of 12M each at pipeline 16, once
# with activedefrag yes (the default) and once with no. Prints ops/s and
# p50/p99/p99.9/max per run.
set -u
ENGINE=$1 CLI=${CLI:-10.0.0.5} SRV=${SRV:-$(hostname -I | awk '{print $1}')} PORT=7001
HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/engines.sh"
MT="memtier_benchmark -s $SRV -p $PORT --hide-histogram --key-prefix=q: --key-minimum=1 --key-maximum=10000 \
  --distinct-client-seed --print-percentiles=50,99,99.9 -t 32 -c 4 --pipeline 16"
for defrag in ${DEFRAG:-yes no}; do
  down "$ENGINE" "$PORT" 2> /dev/null
  up "$ENGINE" "$PORT" || exit 1
  echo "activedefrag $defrag: $(redis-cli -p "$PORT" config set activedefrag "$defrag" 2>&1)"
  ssh "$CLI" "$MT -n 312500 '--command=LPUSH __key__ __data__' --command-key-pattern=R -d 100 -R > /tmp/lpush.txt 2>&1; grep -E '^Totals' /tmp/lpush.txt"
  for r in 1 2 3; do
    ssh "$CLI" "$MT -n 93750 '--command=LPOP __key__' --command-key-pattern=R --json-out-file=/tmp/lpop.json > /tmp/lpop.txt 2>&1;
      python3 -c \"import json; t=json.load(open('/tmp/lpop.json'))['ALL STATS']['Totals']; p=t['Percentile Latencies']; \
print('activedefrag=$defrag run $r: %.0f ops/s p50 %.3f p99 %.3f p99.9 %.3f max %.3f ms' % (t['Ops/sec'], p['p50.00'], p['p99.00'], p['p99.90'], t['Max Latency']))\""
  done
  down "$ENGINE" "$PORT"
done
