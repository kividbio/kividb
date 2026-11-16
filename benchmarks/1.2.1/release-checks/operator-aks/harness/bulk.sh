# bulk.sh <requests>: 1 KB SETs over a 5 M key space through the cluster, from the cli pod
source "$(dirname "$0")/env.sh"
mark "BULK start $1"
k exec cli -- sh -c "redis-benchmark -h shd -p 6380 --user default -a \$(cat /tmp/pw-shd) --cluster -t set -n $1 -d 1024 -r 5000000 -c 50 -P 16 -q 2>&1 | tail -3"
mark "BULK done"
