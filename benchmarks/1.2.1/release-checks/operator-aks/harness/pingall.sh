# $1 cluster name; password in /tmp/pw-$1. PING every node every 5 s; also role, dbsize, master_lost_dataset
C=$1
while true; do
  pw=$(cat /tmp/pw-$C)
  l="$(date -u +%H:%M:%S)"; bad=""; info=""
  for s in 0 1 2 3; do for r in 0 1 2; do h=$C-$s-$r.$C-$s-headless.e2e.svc
    getent hosts $h >/dev/null 2>&1 || continue
    o=$(timeout 3 redis-cli -h $h -p 6380 --user default --pass $pw --no-auth-warning ping 2>&1 | tr -d '\r')
    [ "$o" = PONG ] || bad="$bad $C-$s-$r=${o:-TIMEOUT}"
    x=$(timeout 2 redis-cli -h $h -p 6380 --user default --pass $pw --no-auth-warning info everything 2>/dev/null | tr -d '\r' | grep -E '^(role|master_lost_dataset|master_link_status|db0|cluster_state|loading):' | sed 's/^db0:keys=\([0-9]*\).*/k=\1/;s/master_link_status/link/;s/master_lost_dataset/LOST/;s/role://;s/loading/ld/' | tr '\n' ',')
    info="$info $s$r[$x]"
  done; done
  echo "$l${bad:- all PONG} |$info"
  sleep 5
done
