#!/usr/bin/env bash
# on cg: DEL and DEL-TTL again, rc4 and cand alternating, 3 times (is DEL's +1.8% real?)
set -u
until grep -q CG-REDIS-DONE ~/cg-redis.log; do sleep 30; done
# measure.sh with only the DEL workloads: cut its loop body down
sed -n '1,/^LRU=/p' ~/kividb/scripts/perf/measure.sh > ~/del-measure.sh
cat >> ~/del-measure.sh <<'X'
for tag in "$@"; do
  PRELOAD="-d 100 -t set" PRELOAD_R=200000 PRELOAD_N=400000 DSIZE=100 one "$tag" DEL -r 200000 DEL key:__rand_int__
  PRELOAD="SET key:__rand_int__ $V100 EX 1000000" PRELOAD_R=200000 PRELOAD_N=400000 DSIZE=100 one "$tag" DEL-TTL -r 200000 DEL key:__rand_int__
  DSIZE=100; one "$tag" SET-100 -d 100 -t set
done
X
for i in 1 2 3; do
  sudo docker run --rm --security-opt seccomp=unconfined -e MODEL=uring -e N=60000 -v $HOME/del-measure.sh:/m.sh:ro \
    -v kividb-ci-target:/target kividb-ci-valgrind bash /m.sh rc4 cand cand rc4
done | tee ~/out/callgrind/del-repeat.txt
echo CG-DEL-DONE
