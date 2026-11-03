# syncs.sh <cluster> (cli pod): per node of a sharded cluster, role, uptime, and the master-side sync counters
C=$1; pw=$(cat /tmp/pw-$C)
for s in 0 1 2 3; do for r in 0 1; do h=$C-$s-$r.$C-$s-headless.e2e.svc; getent hosts $h >/dev/null 2>&1 || continue
  x=$(timeout 3 redis-cli -h $h -p 6380 --user default --pass $pw --no-auth-warning info everything 2>&1 | tr -d '\r' | grep -E '^(role|uptime_in_seconds|sync_full|sync_partial_ok|sync_partial_err|master_repl_offset):' | tr '\n' ' ')
  echo "$C-$s-$r $x"; done; done
