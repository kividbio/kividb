#!/usr/bin/env bash
# abq.sh <server-ip> [dur]: quick rc6/rc7 A/B on a spare D4s_v5 pair, master only, AOF everysec, RATE (50k) ops/s open loop,
# 128 conns, the rc7 and the rc6 soak mixes, order alternated, two rounds.
set -u
M=$1; DUR=${2:-5m}; OUT=$HOME/abq; mkdir -p $OUT
s() { for i in 1 2 3 4 5 6; do ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR "$@"; rc=$?; [ $rc != 255 ] && return $rc; sleep 3; done; return 255; }
run() { # round engine-label engine mix-label soakgen
  local d=$OUT/r$1-$2-$4; rm -rf $d; mkdir -p $d
  s $M "ENGINE=$3 bash server.sh start-master" > $d/start.txt
  $HOME/soakgen -addr $M:6379 -conns 64 -prefill
  $5 -addr $M:6379 -conns 128 -rate ${RATE:-50000} -duration $DUR -op-stats > $d/soak.csv 2> $d/opstats.txt
  s $M 'cat /mnt/kv/server.log' > $d/server.log
  awk -F, 'NR>1{print $5, $6, $7}' $d/soak.csv | sort -n -k2 | awk '{a[NR]=$0} END{print "median-by-p99 interval:", a[int(NR/2)+1]}' > $d/summary.txt
  echo "$(date -u +%T) done r$1 $2 $4: $(cat $d/summary.txt)"
}
for r in 1 2; do
  run $r rc7 kividb rc7mix $HOME/soakgen
  run $r rc6 kividbrc6 rc7mix $HOME/soakgen
  run $r rc6 kividbrc6 rc6mix $HOME/soakgen-rc6mix
  run $r rc7 kividb rc6mix $HOME/soakgen-rc6mix
done
s $M 'bash server.sh stop'
echo ABQ-DONE
