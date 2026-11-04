#!/usr/bin/env bash
# setA.sh, on tc (master t1, replica t2, tc the client and the third cluster host): items 1-5 in order.
cd ~
ulimit -n 65535
EXTRA="repl-backlog-size 128mb" OUT=$HOME/r306-128mb bash r306.sh kividb 10 > r306-128mb.log 2>&1
OUT=$HOME/r306-default bash r306.sh kividb 3 > r306-default.log 2>&1
KEYS=3400000 RUNS="rc9:kividb:100000" OUT=$HOME/abfs bash ab_fullsync2.sh > abfs.log 2>&1
scp -q 10.91.1.11:samples-master.csv ~/abfs/rc9/samples-master.csv; scp -q 10.91.1.12:samples-replica.csv ~/abfs/rc9/samples-replica.csv
OUT=$HOME/r5 bash r5.sh > r5.log 2>&1
OUT=$HOME/c4 bash c4.sh kividb 10 > c4.log 2>&1
OUT=$HOME/c3 bash c3.sh kividb 3000000 > c3.log 2>&1
echo SETA-DONE > setA-done.txt
