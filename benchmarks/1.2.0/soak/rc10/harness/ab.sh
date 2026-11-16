#!/usr/bin/env bash
# ab.sh <label>...: #310 A/B on the soak VMs, after the soak. Each label is one run of cl_soak.sh on a fresh cluster
# (wipe, 1 M keys prefilled, the soak load at 100k ops/s with ackcheck, owcheck and the three shard probes) with ONE
# event: a plain `redis-cli --cluster reshard` of 128 slots from the shard of slot 3000 to the shard of slot 8000,
# 3 min in; 8 min in all. Labels starting with "tokio" run every node with `io-model tokio`, the others with the
# default (io_uring on these VMs). Then probe_ana.py and perconn.py (source probe a, round 1).
set -u
for x in "$@"; do
  case $x in tokio*) M=tokio ;; *) M="" ;; esac
  OUT=$HOME/ab-$x DUR=8m DURS=480 IOMODEL=$M SCHED="3 reshard 3000 8000 128" bash $HOME/cl_soak.sh > $HOME/ab-$x.out 2>&1
  $HOME/venv/bin/python $HOME/probe_ana.py $HOME/ab-$x > $HOME/ab-$x/probe_ana.txt 2>&1
  $HOME/venv/bin/python $HOME/perconn.py $HOME/ab-$x a 1 > $HOME/ab-$x/perconn.txt 2>&1
  echo "AB-DONE $x"
done
