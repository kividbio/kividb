#!/usr/bin/env bash
# cg-repeat.sh ROUNDS WORKLOAD...   (on the VM, after callgrind-keep.sh)
# Repeats single workloads of measure.sh on the binaries callgrind-keep.sh
# built (rc8 and rc10, already in the kividb-ci-target volume), alternating
# rc8, rc10 (rc10 first in even rounds), and keeps each callgrind file.
# Uses measure.sh's own `one` function (its text up to the workload list) and
# its workload definitions, so the runs are the same as in the main run.
# Output: ~/out/callgrind/repeat/<tag>-<workload>-r<round>.out.gz, repeat.txt
set -eu
ROUNDS=$1; shift
OUTD=~/out/callgrind/repeat
mkdir -p "$OUTD" ~/rep
M=~/perf-keep/measure.sh
# the function definitions and setup, without the workload loop
sed -n '1,/^V100=/p' "$M" | sed '$d' > ~/rep/lib.sh
cat >> ~/rep/lib.sh << 'EOF'
V100=$(printf 'x%.0s' $(seq 100))
LRU="--maxmemory 64gb --maxmemory-policy allkeys-lru"
LFU="--maxmemory 64gb --maxmemory-policy allkeys-lfu"
wl() { # tag workload: the same arguments as measure.sh's list
  local tag=$1
  case $2 in
    GET-100) DSIZE=100 one "$tag" GET-100 -d 100 -t get ;;
    SET-100) DSIZE=100 one "$tag" SET-100 -d 100 -t set ;;
    GET-1K) DSIZE=1024 one "$tag" GET-1K -d 1024 -t get ;;
    SET-1K) DSIZE=1024 one "$tag" SET-1K -d 1024 -t set ;;
    MIX-100) DSIZE=100 one "$tag" MIX-100 -d 100 -t set,get ;;
    INCR) PRELOAD="SET counter:__rand_int__ 0" DSIZE=100 one "$tag" INCR -t incr ;;
    DEL) PRELOAD="-d 100 -t set" PRELOAD_R=200000 PRELOAD_N=400000 DSIZE=100 one "$tag" DEL -r 200000 DEL key:__rand_int__ ;;
    GET-100-TTL) PRELOAD="SET key:__rand_int__ $V100 EX 1000000" DSIZE=100 one "$tag" GET-100-TTL -t get ;;
    SET-100-TTL) PRELOAD="SET key:__rand_int__ $V100 EX 1000000" DSIZE=100 one "$tag" SET-100-TTL SET key:__rand_int__ "$V100" EX 1000000 ;;
    INCR-TTL) PRELOAD="SET counter:__rand_int__ 0 EX 1000000" DSIZE=100 one "$tag" INCR-TTL -t incr ;;
    DEL-TTL) PRELOAD="SET key:__rand_int__ $V100 EX 1000000" PRELOAD_R=200000 PRELOAD_N=400000 DSIZE=100 one "$tag" DEL-TTL -r 200000 DEL key:__rand_int__ ;;
    GET-100-LRU) SERVER_EXTRA=$LRU DSIZE=100 one "$tag" GET-100-LRU -d 100 -t get ;;
    SET-100-LRU) SERVER_EXTRA=$LRU DSIZE=100 one "$tag" SET-100-LRU -d 100 -t set ;;
    INCR-LRU) SERVER_EXTRA=$LRU PRELOAD="SET counter:__rand_int__ 0" DSIZE=100 one "$tag" INCR-LRU -t incr ;;
    DEL-LRU) SERVER_EXTRA=$LRU PRELOAD="-d 100 -t set" PRELOAD_R=200000 PRELOAD_N=400000 DSIZE=100 one "$tag" DEL-LRU -r 200000 DEL key:__rand_int__ ;;
    GET-100-LFU) SERVER_EXTRA=$LFU DSIZE=100 one "$tag" GET-100-LFU -d 100 -t get ;;
    SET-100-LFU) SERVER_EXTRA=$LFU DSIZE=100 one "$tag" SET-100-LFU -d 100 -t set ;;
    INCR-LFU) SERVER_EXTRA=$LFU PRELOAD="SET counter:__rand_int__ 0" DSIZE=100 one "$tag" INCR-LFU -t incr ;;
  esac
}
EOF
cat > ~/rep/run.sh << 'EOF'
set -u
. /rep/lib.sh
ROUNDS=$1; shift
for r in $(seq "$ROUNDS"); do
  order="rc8 rc10"; [ $((r % 2)) = 0 ] && order="rc10 rc8"
  for w in "$@"; do
    for tag in $order; do
      line=$(wl "$tag" "$w")
      echo "$line r$r"
      cp "/out/cg-$w.out" "/out/rep-$tag-$w-r$r.out"
    done
  done
done
EOF
grep -q 'cp "$f" "/out/cg-$label.out"' ~/rep/lib.sh
rm -rf ~/rep/out && mkdir -p ~/rep/out
sudo docker run --rm --security-opt seccomp=unconfined -e MODEL=uring -e N=60000 \
  -v ~/rep:/rep:ro -v ~/rep/out:/out -v kividb-ci-target:/target \
  kividb-ci-valgrind bash -c "umask 022; bash /rep/run.sh $ROUNDS $*; chmod -R a+r /out" | tee -a "$OUTD/repeat.txt"
for f in ~/rep/out/rep-*.out; do gzip -c "$f" > "$OUTD/$(basename "$f" .out | sed 's/^rep-//').out.gz"; done
python3 ~/p/cg_split.py "$OUTD" > "$OUTD/split.txt"
cat "$OUTD/split.txt"
echo REPEAT-DONE
