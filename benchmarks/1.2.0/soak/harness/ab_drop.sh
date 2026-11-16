#!/usr/bin/env bash
# ab_drop.sh: which of the rc7 soak's new ops raises p99 (rc7 only, ab.sh's set-up, 10 min per run):
# the rc7 mix without MULTI/EXEC (-drop 18), and without the field-TTL ops (-drop 16,17).
set -u
. $HOME/hosts.env
OUT=$HOME/ab; DUR=${DUR:-6m}
s() { for i in 1 2 3 4 5 6; do ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR "$@"; rc=$?; [ $rc != 255 ] && return $rc; sleep 3; done; return 255; }
run() { # label drop
  local d=$OUT/$1; rm -rf $d; mkdir -p $d
  s $M "bash server.sh start-master" > $d/start.txt; s $R "bash server.sh start-replica $M" >> $d/start.txt; sleep 3
  $HOME/soakgen -addr $M:6379 -conns 64 -prefill
  $HOME/soakgen-drop -addr $M:6379 -conns 128 -rate 100000 -duration $DUR -op-stats -drop "$2" > $d/soak.csv 2> $d/opstats.txt
  s $M 'cat /mnt/kv/server.log' > $d/server-master.log
  s $M 'pkill -f "[s]ampler.sh"; cat samples-master.csv' > $d/samples-master.csv; s $R 'pkill -f "[s]ampler.sh"'
  echo "done $1"
}
run rc7-rc7mix-without-multi 18
run rc7-rc7mix-without-fieldttl 16,17
s $M 'bash server.sh stop'; s $R 'bash server.sh stop'
echo DROP-DONE
