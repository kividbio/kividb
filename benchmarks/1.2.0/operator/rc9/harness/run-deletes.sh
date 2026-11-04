# run-deletes.sh: on shd, 2 graceful deletes of a shard master (handover, shutdown wait, the demoted master's data on
# disk), then 3 force deletes (--grace-period=0 --force) of a shard master with the replacement on the same node (#303),
# each after a BGSAVE so an older dump is on disk. Targets: shards 0, 1 (graceful); 0, 1, 2 (force).
cd "$(dirname "$0")"; source ./env.sh
for n in 1 2; do s=$(( n - 1 )); sleep 30; M=$(smaster $s); mark "graceful try $n: $M"
  SAMENODE=1 bash scen-delete.sh graceful$n $M graceful > "$D/graceful$n.out" 2>&1
  echo "== graceful $n ($M)"; grep -E "^checked" "$D/graceful$n.out"; head -3 "$D/evidence/graceful$n/summary.txt"; done
for n in 1 2 3; do s=$(( (n-1) % 3 )); sleep 30; M=$(smaster $s); mark "forcedel try $n: BGSAVE on $M"
  k exec cli -- redis-cli -h $M.shd-$s-headless.e2e.svc -p 6380 --user default --pass shd-pw-1111 --no-auth-warning bgsave
  sleep 30; SAMENODE=1 bash scen-delete.sh forcedel$n $M force > "$D/forcedel$n.out" 2>&1
  echo "== forcedel $n ($M)"; grep -E "^checked" "$D/forcedel$n.out"; head -3 "$D/evidence/forcedel$n/summary.txt"
  grep -hE "in use by another|is free after" "$D/evidence/forcedel$n/$M-new.log" | cut -c1-200; done
echo DELETES_DONE
