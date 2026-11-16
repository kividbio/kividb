# run-fxloss.sh: up to 4 tries of the forcedel-rc7 sequence on fx with a small dataset: steady load, a BGSAVE on
# shard 0's master (an older dump), more load, then a force delete whose replacement lands on the same node.
cd "$(dirname "$0")"; source ./env.sh
fxm0() { for r in 0 1; do [ "$(k exec cli -- redis-cli -h fx-0-$r.fx-0-headless.e2e.svc -p 6380 --user default --pass shd-pw-1111 --no-auth-warning role 2>/dev/null | head -1 | tr -d '\r')" = master ] && echo fx-0-$r; done; }
for n in 1 2 3 4; do
  sleep 45; M=$(fxm0); mark "fxloss try $n: BGSAVE on $M"; k exec cli -- redis-cli -h $M.fx-0-headless.e2e.svc -p 6380 --user default --pass shd-pw-1111 --no-auth-warning bgsave
  sleep 45; SAMENODE=1 bash scen-fxrepro.sh fx-loss$n $M force > "$D/fx-loss$n.out" 2>&1
  grep -E "^checked|two engines" "$D/fx-loss$n.out" "$D/evidence/fx-loss$n/summary.txt" | cut -c1-160
  grep -q "missing [1-9]" "$D/fx-loss$n.out" && { mark "fxloss try $n: LOST writes reproduced"; break; }
done
echo FXLOSS_DONE
