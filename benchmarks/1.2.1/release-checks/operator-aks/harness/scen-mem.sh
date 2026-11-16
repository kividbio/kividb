# scen-mem.sh <label>: B9, memory of every sharded node (role, keys, used_memory, RSS, value slabs, dataset). (c3 run)
source "$(dirname "$0")/env.sh"
mkdir -p "$D/res"; out="$D/res/mem-$1.txt"
k exec cli -- sh -c 'P=$(cat /tmp/pw-shd); for s in 0 1 2 3; do for r in 0 1; do h=shd-$s-$r.shd-$s-headless.e2e.svc; getent hosts $h >/dev/null 2>&1 || continue; x=$(timeout 3 redis-cli -h $h -p 6380 --user default --pass $P --no-auth-warning info everything | tr -d "\r" | grep -E "^(role|db0|used_memory|used_memory_rss|used_memory_dataset|value_slab_bytes|mem_fragmentation_ratio):" | sed "s/^db0:keys=\([0-9]*\).*/keys=\1/" | tr "\n" " "); echo "shd-$s-$r $x"; done; done' | tee "$out"
