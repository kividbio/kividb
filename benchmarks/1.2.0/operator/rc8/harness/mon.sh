# replication monitor (runs in the cli pod): role/link/offset/version of rep-0..2; password in /tmp/pw
while true; do
  l="$(date -u +%H:%M:%S)"
  for p in rep-0 rep-1 rep-2; do
    pw=$(cat /tmp/pw 2>/dev/null)
    if [ -n "$pw" ]; then A="--user default --pass $pw"; else A=""; fi
    r=$(timeout 2 redis-cli -h $p.rep-headless.e2e.svc -p 6380 $A --no-auth-warning info replication 2>&1 | tr -d '\r' | grep -E '^(role|master_link_status|master_repl_offset|connected_slaves):' | sed 's/master_link_status/link/;s/master_repl_offset/off/;s/connected_slaves/sl/' | tr '\n' ',')
    v=$(timeout 2 redis-cli -h $p.rep-headless.e2e.svc -p 6380 $A --no-auth-warning info server 2>&1 | tr -d '\r' | grep kividb_version | cut -d: -f2)
    l="$l | $p $v $r"
  done
  echo "$l"
  sleep 2
done
