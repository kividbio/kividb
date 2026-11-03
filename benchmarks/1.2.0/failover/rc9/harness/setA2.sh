#!/usr/bin/env bash
# setA2.sh, on tc after setA.sh: item 4 again with ackcheck (the first c3 run had none), then item 5 with the clean stop
# taken with the load stopped (r5.sh; the first try hung at that stop under load: r5-try1/).
cd ~
ulimit -n 65535
until [ -f setA-done.txt ]; do sleep 5; done
OUT=$HOME/c3-ack bash c3.sh kividb 3000000 > c3-ack.log 2>&1
OUT=$HOME/r5 bash r5.sh > r5.log 2>&1
echo SETA2-DONE > setA2-done.txt
