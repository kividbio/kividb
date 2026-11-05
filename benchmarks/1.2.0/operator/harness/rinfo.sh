# rinfo.sh <cluster> (cli pod): per pod of a replicated cluster, role, stream id, offset, link, keys, engine version,
# AOF on, aof-ack-after-write. Password in /tmp/pw.
C=$1; pw=$(cat /tmp/pw)
for i in 0 1 2 3; do h=$C-$i.$C-headless.e2e.svc
  getent hosts $h >/dev/null 2>&1 || continue
  A="-h $h -p 6380 --user default --pass $pw --no-auth-warning"
  x=$(timeout 3 redis-cli $A info everything 2>&1 | tr -d '\r' | grep -E '^(role|master_replid|master_repl_offset|master_link_status|master_host|db0|kividb_version|aof_enabled|run_id):' | sed 's/^db0:keys=\([0-9]*\).*/keys=\1/' | tr '\n' ' ')
  ack=$(timeout 3 redis-cli $A config get aof-ack-after-write 2>&1 | tr '\n' ' ')
  echo "$C-$i $x ack=[$ack]"
done
