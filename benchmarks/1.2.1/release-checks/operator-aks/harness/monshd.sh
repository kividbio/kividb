# cluster monitor (cli pod): $1 cluster name; password in /tmp/pw-$1
C=$1
while true; do
  pw=$(cat /tmp/pw-$C)
  n=$(timeout 2 redis-cli -h $C -p 6380 --user default --pass $pw --no-auth-warning cluster nodes 2>&1 | tr -d '\r' | awk '{split($2,a,","); h=a[2]; sub(/\..*/,"",h); r=$3; sub(/myself,/,"",r); s=""; for(i=9;i<=NF;i++) s=s" "$i; printf "%s:%s:%s%s; ", h, r, $8, s}')
  st=$(timeout 2 redis-cli -h $C -p 6380 --user default --pass $pw --no-auth-warning cluster info 2>&1 | tr -d '\r' | grep -E 'cluster_state|cluster_size' | tr '\n' ' ')
  echo "$(date -u +%H:%M:%S) $st| $n"
  sleep 2
done
