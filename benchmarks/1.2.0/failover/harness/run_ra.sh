#!/usr/bin/env bash
# run_ra.sh <master-ip> <replica-ip> <out>   (on the load VM) -- replica with its own AOF under sustained writes.
# The fixes/replica-aof harness (replica_mem.sh, server.sh, sampler.sh, compare.py, soakgen) unchanged except that
# server.sh's spare engine slot `kividbrc3` is pointed at the rc4 binary (/opt/kvrc4) for the control run.
#   rc7, both nodes appendonly yes / everysec, 15 min at 100k ops/s, then a full key-by-key compare;
#   rc4, the same, 240 s (control: the replica's memory must grow there).
set -u
M=$1 R=$2 OUT=$3
mkdir -p "$OUT"
s() { ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR "$@"; }
info() { redis-cli -h "$1" -p 6379 INFO "$2" | tr -d '\r' | awk -F: -v k="$3" '$1==k{print $2}'; }
moff() { info $M replication master_repl_offset; }
roff() { redis-cli -h $M -p 6379 INFO replication | tr -d '\r' | awk -F'[:,=]' '/^slave0:/{for(i=1;i<=NF;i++) if($i=="offset") print $(i+1)}'; }
post() { # name compare(0/1)
  local N=$1 CMP=$2 F="$OUT/$1.post.txt"
  {
    echo "== load stopped $(date -u +%T)"
    local t0=$(date +%s) mo ro
    while :; do
      mo=$(moff); ro=$(roff)
      echo "$(( $(date +%s) - t0 ))s master_repl_offset=$mo replica_offset=$ro lag=$(( ${mo:-0} - ${ro:-0} ))"
      [ -n "$mo" ] && [ "$mo" = "$ro" ] && break
      [ $(( $(date +%s) - t0 )) -gt 180 ] && { echo "replica did not catch up in 180 s"; break; }
      sleep 2
    done
    echo "== replica INFO persistence"; redis-cli -h $R INFO persistence | tr -d '\r' | grep -E 'aof_|rdb_bgsave_in'
    echo "== replica INFO server"; redis-cli -h $R INFO server | tr -d '\r' | grep -E 'kividb_version|redis_version'
    echo "== DBSIZE master $(redis-cli -h $M DBSIZE) replica $(redis-cli -h $R DBSIZE)"
    echo "== AOF/data files master"; s $M 'du -sb /mnt/kv/* 2>/dev/null'
    echo "== AOF/data files replica"; s $R 'du -sb /mnt/kv/* 2>/dev/null'
    if [ "$CMP" = 1 ]; then
      echo "== compare.py"; timeout 1200 ~/venv/bin/python3 ~/compare.py $M:6379 $R:6379 --out "$OUT/$N.compare.json" 2>&1 | tail -15
      echo "== DBSIZE after compare master $(redis-cli -h $M DBSIZE) replica $(redis-cli -h $R DBSIZE)"
    fi
  } > "$F" 2>&1
  s $M 'cat ~/samples-master.csv' > "$OUT/$N.samples-master.csv"
  s $R 'cat ~/samples-replica.csv' > "$OUT/$N.samples-replica.csv"
  s $M 'cat /mnt/kv/server.log' > "$OUT/$N.server-master.log"
  s $R 'cat /mnt/kv/server.log' > "$OUT/$N.server-replica.log"
  s $R 'cat /proc/$(cat /mnt/kv/pid)/status | grep -E "VmRSS|VmHWM"' > "$OUT/$N.replica-proc.txt" 2>&1
}
run() { # name engine rx secs compare
  echo "=== $1 start $(date -u +%T)"
  bash ~/replica_mem.sh $M $R "$OUT" "$1" "$2" "$3" "$4" 100000
  post "$1" "$5"
  echo "=== $1 done $(date -u +%T)"
}
run rc7-aof-15m kividb "" 900 1
run rc4-aof-240s kividbrc3 "" 240 0
s $M 'bash server.sh stop'; s $R 'bash server.sh stop'
echo RA-DONE
