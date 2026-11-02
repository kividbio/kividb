#!/usr/bin/env bash
# sustained-2k.sh ENGINE...: the 2 KB rows of the matrix run a fixed number of
# requests, which the fast engines finish in 1-2 s; Azure lets short bursts
# through above the VM's sustained bandwidth. This re-measures HSET (writes,
# 2 KB in) and HGET (reads, 2 KB out) on the same 100,000 hashes for 20 s
# each instead, pipeline 1 and 16, so the rate is the sustained one.
# Output: ~/out/sustained-2k/ENGINE/{hset,hget}-p{1,16}.json
set -u
CLI=${CLI:-10.0.0.5} SRV=${SRV:-$(hostname -I | awk '{print $1}')} PORT=7001
HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/engines.sh"
for ENGINE in "$@"; do
  OUT=out/sustained-2k/$ENGINE
  mkdir -p ~/"$OUT"; ssh "$CLI" "mkdir -p $OUT"
  CL=""; [ "$ENGINE" = redis-cluster ] && CL="--cluster-mode"
  down "$ENGINE" "$PORT" 2> /dev/null
  up "$ENGINE" "$PORT" || continue
  ssh "$CLI" "memtier_benchmark -s $SRV -p $PORT $CL --hide-histogram --key-prefix=h: -t 1 -c 1 --pipeline 32 -n allkeys \
    --key-minimum=1 --key-maximum=100001 '--command=HSET __key__ f0 __data__ f1 __data__ f2 __data__ f3 __data__ f4 __data__ f5 __data__ f6 __data__ f7 __data__ f8 __data__ f9 __data__' \
    -d 2048 --command-key-pattern=P > /dev/null 2>&1"
  for cmd in "HSET __key__ f0 __data__" "HGET __key__ f5"; do
    name=$(echo "$cmd" | awk '{print tolower($1)}')
    for p in 1 16; do
      c=16; [ "$p" = 16 ] && c=4
      ssh "$CLI" "ulimit -n 65535; memtier_benchmark -s $SRV -p $PORT $CL --hide-histogram --key-prefix=h: -t 32 -c $c --pipeline $p \
        --test-time 20 --distinct-client-seed --print-percentiles=50,99,99.9 '--command=$cmd' -d 2048 -R \
        --key-minimum=1 --key-maximum=100000 --command-key-pattern=R --json-out-file=$OUT/$name-p$p.json > $OUT/$name-p$p.txt 2>&1"
      scp -q "$CLI:$OUT/$name-p$p.json" ~/"$OUT"/
      python3 -c "import json,sys; t=json.load(open(sys.argv[1]))['ALL STATS']['Totals']; p=t['Percentile Latencies']; print(sys.argv[2], '%.0f ops/s %.0f MB/s p50 %.3f p99 %.3f p99.9 %.3f' % (t['Ops/sec'], t['Ops/sec']*2048/1e6, p['p50.00'], p['p99.00'], p['p99.90']))" ~/"$OUT/$name-p$p.json" "$ENGINE $name p$p"
    done
  done
  down "$ENGINE" "$PORT"
done
