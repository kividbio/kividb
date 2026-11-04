#!/usr/bin/env bash
# ab.sh: rc6 against rc7 under soak 1's set-up (master + replica, AOF everysec, defaults, 100k ops/s open loop,
# 128 conns), 12 min per run, with the rc7 soak mix (soakgen) and the rc6 soak mix (soakgen-rc6mix, no field TTLs
# and no MULTI/EXEC). Order alternated. Each run: fresh servers, prefill, soak.csv, both server logs, samples.
set -u
. $HOME/hosts.env
OUT=$HOME/ab; mkdir -p $OUT
DUR=${DUR:-12m}
s() { for i in 1 2 3 4 5 6; do ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR "$@"; rc=$?; [ $rc != 255 ] && return $rc; sleep 3; done; return 255; }
run() { # engine-label engine mix-label soakgen
  local d=$OUT/$1-$3; rm -rf $d; mkdir -p $d
  s $M "ENGINE=$2 bash server.sh start-master" > $d/start.txt
  s $R "ENGINE=$2 bash server.sh start-replica $M" >> $d/start.txt
  sleep 3
  redis-cli -h $M INFO server | grep -E 'kividb_version|redis_version' > $d/version.txt
  $HOME/soakgen -addr $M:6379 -conns 64 -prefill
  date +%s.%N > $d/soak.start
  $4 -addr $M:6379 -conns 128 -rate 100000 -duration $DUR -op-stats > $d/soak.csv 2> $d/opstats.txt
  date +%s.%N > $d/soak.end
  s $M 'cat /mnt/kv/server.log' > $d/server-master.log
  s $R 'cat /mnt/kv/server.log' > $d/server-replica.log
  s $M 'pkill -f "[s]ampler.sh"; cat samples-master.csv' > $d/samples-master.csv
  s $R 'pkill -f "[s]ampler.sh"; cat samples-replica.csv' > $d/samples-replica.csv
  echo "done $1 $3"
}
run rc6 kividbrc6 rc7mix $HOME/soakgen
run rc7 kividb rc7mix $HOME/soakgen
run rc7 kividb rc6mix $HOME/soakgen-rc6mix
run rc6 kividbrc6 rc6mix $HOME/soakgen-rc6mix
s $M 'bash server.sh stop'; s $R 'bash server.sh stop'
echo AB-DONE
