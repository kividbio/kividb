# run-fxloss.sh [tries]: the forcedel-rc7 (#303) sequence on fx, every try run to the end (no stop on a loss):
# steady load, a BGSAVE on the target shard master (an older dump on disk), more load, then a force delete
# (--grace-period=0 --force) whose replacement lands on the same node (SAMENODE=1 cordons the others).
# The target rotates over shards 0, 1, 2. Then one graceful delete, same node, for comparison.
# Per try: two processes on one volume (procs.txt), the new engine's "in use by another process" / "free after"
# lines, and every acknowledged write of loader-fx read back.
cd "$(dirname "$0")"; source ./env.sh
N=${1:-6}
fxm() { for r in 0 1; do [ "$(k exec cli -- redis-cli -h fx-$1-$r.fx-$1-headless.e2e.svc -p 6380 --user default --pass shd-pw-1111 --no-auth-warning role 2>/dev/null | head -1 | tr -d '\r')" = master ] && echo fx-$1-$r; done; }
for n in $(seq 1 $N); do
  s=$(( (n-1) % 3 ))
  sleep 40; M=$(fxm $s); mark "fxloss try $n: BGSAVE on $M"
  k exec cli -- redis-cli -h $M.fx-$s-headless.e2e.svc -p 6380 --user default --pass shd-pw-1111 --no-auth-warning bgsave
  sleep 40; SAMENODE=1 bash scen-fxrepro.sh fx-loss$n $M force > "$D/fx-loss$n.out" 2>&1
  echo "== try $n ($M)"; grep -E "^checked|two engines" "$D/fx-loss$n.out" "$D/evidence/fx-loss$n/summary.txt" | cut -c1-200
  grep -hE "in use by another|is free after" "$D/evidence/fx-loss$n/$M-new.log" | cut -c1-200
  grep -q "missing [1-9]" "$D/fx-loss$n.out" && mark "fxloss try $n: LOST writes"
done
sleep 40; M=$(fxm 1); SAMENODE=1 bash scen-fxrepro.sh fx-graceful $M graceful > "$D/fx-graceful.out" 2>&1
echo "== graceful ($M)"; grep -E "^checked|two engines" "$D/fx-graceful.out" "$D/evidence/fx-graceful/summary.txt" | cut -c1-200
grep -hE "in use by another|is free after" "$D/evidence/fx-graceful/$M-new.log" | cut -c1-200
echo FXLOSS_DONE
