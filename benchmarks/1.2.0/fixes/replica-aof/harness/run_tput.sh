#!/usr/bin/env bash
# run_tput.sh <master-ip> <replica-ip> <out>   (on the load VM)
# redis-benchmark SET, 100 B values, 50 clients, pipeline 1 and 16, AOF on, everysec.
# Master alone, then master with a replica (AOF on too); 3 rounds, rc4 and fixed alternating.
set -u
M=$1 R=$2 OUT=$3
mkdir -p "$OUT"
s() { ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR "$@"; }
CSV="$OUT/throughput.csv"
echo "setup,round,build,pipeline,requests,rps,p50_ms,p99_ms" > "$CSV"
for setup in solo replica; do
  for round in 1 2 3; do
    if [ $((round % 2)) = 1 ]; then order="rc4 fixed"; else order="fixed rc4"; fi
    for b in $order; do
      eng=$([ $b = rc4 ] && echo kividb || echo kividbfix)
      s $R 'bash server.sh stop' >/dev/null 2>&1
      s $M "ENGINE=$eng bash server.sh start-master" >/dev/null
      if [ $setup = replica ]; then
        s $R "ENGINE=$eng bash server.sh start-replica $M" >/dev/null
        for _ in $(seq 1 60); do redis-cli -h $R INFO replication | grep -q 'master_link_status:up' && break; sleep 1; done
      fi
      sleep 2
      for P in 1 16; do
        n=$([ $P = 1 ] && echo 6000000 || echo 30000000)
        line=$(redis-benchmark -h $M -p 6379 -t set -d 100 -c 50 -P $P -n $n -r 1000000 --threads 4 --csv 2>/dev/null | grep '"SET"')
        # "test","rps","avg_latency_ms","min_latency_ms","p50_latency_ms","p95_latency_ms","p99_latency_ms","max_latency_ms"
        rps=$(echo "$line" | awk -F'","' '{print $2}'); p50=$(echo "$line" | awk -F'","' '{print $5}'); p99=$(echo "$line" | awk -F'","' '{print $7}')
        echo "$setup,$round,$b,$P,$n,$rps,$p50,${p99//\"/}" >> "$CSV"
        echo "$setup r$round $b P$P rps=$rps"
      done
      redis-cli -h $M INFO persistence | tr -d '\r' | grep -E 'aof_acks|aof_current_size' | sed "s/^/$setup r$round $b /" >> "$OUT/throughput-aof-info.txt"
      [ $setup = replica ] && redis-cli -h $R INFO memory | tr -d '\r' | grep '^used_memory:' | sed "s/^/$setup r$round $b replica /" >> "$OUT/throughput-aof-info.txt"
    done
  done
done
s $M 'bash server.sh stop'; s $R 'bash server.sh stop'
echo TPUT-DONE
