#!/usr/bin/env bash
# setB.sh, on sc (master sm, replica sr): item 6.
cd ~
ulimit -n 65535
OUT=$HOME/s1 bash s1_30.sh > s1.log 2>&1
echo SETB-DONE > setB-done.txt
