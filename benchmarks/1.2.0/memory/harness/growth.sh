#!/bin/bash
# Memory while the dataset grows (HN: kingcauchy). Client-side driver.
#   growth.sh <engine> <value-bytes> <checkpoint,...> [random|ascii]   e.g. growth.sh redis 100 1000000,5000000,10000000,25000000,50000000
# Fresh engine, no persistence. Keys memtier-1..memtier-N (memtier's default key names, 9-16 bytes) are written
# once each, in stages up to each checkpoint. Values are random bytes (memtier --random-data) by default, or
# memtier's ASCII filler with "ascii" (Dragonfly packs ASCII strings, which favours it). The sampler records memory every second through the whole load; after each stage the
# load pauses 10 s and a checkpoint row (keys, used_memory, RSS) is written to checkpoints.csv.
set -uo pipefail
. "$(dirname "$0")/lib.sh"
E=$1; V=$2; CKPTS=$3; KIND=${4:-random}
DFLAG=""; [ "$KIND" = random ] && DFLAG=--random-data
RUN=growth-$E-$V-$KIND
srv /opt/mem/eng.sh stop
srv /opt/mem/eng.sh start "$E" plain | tee "$OUT/$RUN.engine.txt"
srv /opt/mem/mon.sh start "$RUN"
sleep 5
ev start
read -r B_RSS B_USED <<< "$(srv "tail -1 /var/tmp/mem/$RUN.csv" | awk -F, '{print $4, $7}')"
prev=0
for C in ${CKPTS//,/ }; do
  ev "load-to-$C"
  memtier --protocol=redis --ratio=1:0 $DFLAG --data-size="$V" --key-pattern=P:P --key-minimum=$((prev + 1)) \
    --key-maximum="$C" --requests=allkeys --threads=8 --clients=8 --pipeline=32 --hide-histogram \
    > "$OUT/$RUN-to-$C.memtier.txt" 2>&1
  ev "loaded-$C"
  sleep 10
  L=$(srv "tail -1 /var/tmp/mem/$RUN.csv")
  echo "$E,$V,$KIND,$C,$B_RSS,$B_USED,$L" >> "$OUT/checkpoints.csv"
  echo "  $C keys: $(echo "$L" | awk -F, -v br="$B_RSS" -v bu="$B_USED" -v n="$C" '{printf "RSS %.1f MB (%.1f B/key) used %.1f MB (%.1f B/key) keys %s", $4, ($4-br)*1048576/n, $7, ($7-bu)*1048576/n, $8}')"
  prev=$C
done
ev end
srv /opt/mem/mon.sh stop
fetch "$RUN"
srv /opt/mem/eng.sh stop
