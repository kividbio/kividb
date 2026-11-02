# $1 cluster, password /tmp/pw-$1: per shard role, offset, dbsize
C=$1; pw=$(cat /tmp/pw-$C)
for s in 0 1 2 3; do l="shard $s:"; for r in 0 1 2; do h=$C-$s-$r.$C-$s-headless.e2e.svc; getent hosts $h >/dev/null 2>&1 || continue
 A="-h $h -p 6380 --user default --pass $pw --no-auth-warning"
 i=$(redis-cli $A info replication | tr -d '\r'); role=$(echo "$i" | grep ^role: | cut -d: -f2); off=$(echo "$i" | grep -E '^(slave_repl_offset|master_repl_offset):' | head -1 | cut -d: -f2)
 l="$l $C-$s-$r $role off=$off n=$(redis-cli $A dbsize);"; done; echo "$l"; done
