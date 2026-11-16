#!/bin/bash
# Snapshot load time and memory at load, for a dump written by each KiviDB build. Client-side driver.
#   [R=<round>] [REPS=3] loadtime.sh <build-a> <build-b> [keys=30000000] [value-bytes=1024]
# 1. For each build: fresh engine (no AOF), load <keys> x <value-bytes> random values exactly as snapshot.sh does,
#    BGSAVE, wait for it, record INFO persistence and the dump's size, keep the dump as /mnt/nvme/dumps/<build>.kdb.
# 2. REPS times, readers and writers in alternating order: each build cold-starts on each dump (loadone.sh):
#    page cache dropped, time from `docker run` to the first PONG, DBSIZE, used_memory, RSS at the first PONG,
#    the process's peak RSS (VmHWM) during the load, and RSS 10 s later. One CSV row per start in loadtime.csv.
set -uo pipefail
. "$(dirname "$0")/lib.sh"
A=$1; B=$2; N=${3:-30000000}; V=${4:-1024}; REPS=${REPS:-3}
RUN=loadtime${R:+-$R}
CSV=$OUT/loadtime.csv
[ -s "$CSV" ] || echo "round,label,reader,writer,start_to_pong_s,dbsize,used_memory,rss_at_pong_kb,vmhwm_kb,max_sampled_rss_kb,rss_10s_kb,used_10s,log_load_s" > "$CSV"
srv "sudo mkdir -p /mnt/nvme/dumps && sudo rm -f /mnt/nvme/dumps/*"
for Wr in $A $B; do
  RUN=loadtime-write-$Wr${R:+-$R}
  srv /opt/mem/eng.sh stop
  srv /opt/mem/eng.sh start "$Wr" plain | tee "$OUT/$RUN.engine.txt"
  ev load
  memtier --protocol=redis --ratio=1:0 --random-data --data-size="$V" --key-pattern=P:P --key-minimum=1 --key-maximum="$N" \
    --requests=allkeys --threads=8 --clients=8 --pipeline=32 --hide-histogram > "$OUT/$RUN-load.memtier.txt" 2>&1
  ev loaded
  sleep 10
  rcli info memory | tr -d '\r' | grep -E '^(used_memory|used_memory_rss):' > "$OUT/$RUN.memory.txt"
  echo "dbsize=$(rcli DBSIZE)" >> "$OUT/$RUN.memory.txt"
  ev bgsave-issued; rcli BGSAVE | tee -a "$OUT/$RUN.replies.txt"
  wait_persist_idle
  ev bgsave-done
  rcli info persistence | tr -d '\r' > "$OUT/$RUN.persistence.txt"
  srv "ls -la /mnt/nvme/data; sudo ln /mnt/nvme/data/dump.kdb /mnt/nvme/dumps/$Wr.kdb && ls -la /mnt/nvme/dumps" > "$OUT/$RUN.files.txt" 2>&1
  srv /opt/mem/eng.sh stop
done
RUN=loadtime${R:+-$R}
for rep in $(seq 1 "$REPS"); do
  if [ $((rep % 2)) = 1 ]; then ORD="$A $B"; else ORD="$B $A"; fi
  for Rd in $ORD; do for Wr in $ORD; do
    ev "load-$Rd-from-$Wr-$rep"
    L=$(srv /opt/mem/loadone.sh "$Rd" "$Wr" "rep$rep" 2>> "$OUT/$RUN.stderr.txt")
    echo "${R:-r1},$L" | tee -a "$CSV"
  done; done
done
srv "ls -la /mnt/nvme/dumps" >> "$OUT/$RUN.files.txt" 2>&1
srv "sudo rm -f /mnt/nvme/dumps/*"
srv /opt/mem/eng.sh stop
ev end
