# Sourced by the client-side drivers (growth.sh, mixed.sh, snapshot.sh, evict.sh).
# SRV is the engine VM's private address; the client reaches it over the VNet, and ssh's into it to
# start engines and the sampler (key ~/.ssh/id_mem, installed by setup).
SRV=${SRV:-10.30.0.4}
OUT=${OUT:-/var/tmp/mem}
mkdir -p "$OUT"
SSH="ssh -i $HOME/.ssh/id_mem -o StrictHostKeyChecking=no -o ServerAliveInterval=30 azureuser@$SRV"
MT_IMG=ghcr.io/warefeats/memtier-benchmark:2.5.1

srv() { $SSH "$@"; }
# memtier_benchmark 2.5.1 (the warefeats image) against the engine VM
memtier() { docker run --rm --network host -v "$OUT:$OUT" "$MT_IMG" memtier_benchmark -s "$SRV" -p 6379 "$@"; }
rcli() { redis-cli -h "$SRV" -p 6379 "$@"; }
# event log: unix time, run, event
ev() { echo "$(date +%s.%N),$RUN,$1" >> "$OUT/events.csv"; echo "[$(date -u +%T)] $RUN $1"; }
# copy the sampler's csv back from the engine VM
fetch() { scp -q -i "$HOME/.ssh/id_mem" -o StrictHostKeyChecking=no "azureuser@$SRV:/var/tmp/mem/$1.csv" "$OUT/" ; }
# wait until neither a snapshot nor an AOF rewrite is in progress (checked for 2 polls in a row)
wait_persist_idle() {
  local ok=0
  sleep 1
  for _ in $(seq 1 7200); do
    local s; s=$(rcli info persistence 2>/dev/null | tr -d '\r')
    if echo "$s" | grep -qE '^(rdb_bgsave_in_progress|aof_rewrite_in_progress|saving):1' ; then ok=0
    else ok=$((ok + 1)); [ $ok -ge 2 ] && return 0; fi
    sleep 0.5
  done
}
