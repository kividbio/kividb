#!/usr/bin/env bash
# setAB3.sh <A|B>: more rounds. A (tc): item 5 again with three more delays (r5b). B (sc): the clean-stop repro,
# 10 rounds under load for rc8 and for rc9 (rs10-*).
cd ~
ulimit -n 65535
case $1 in
A) until [ -f setA2-done.txt ]; do sleep 5; done
   OUT=$HOME/r5b DELAYS="8 5 1" bash r5.sh > r5b.log 2>&1; echo SETA3-DONE > setA3-done.txt ;;
B) until [ -f setB2-done.txt ]; do sleep 5; done
   OUT=$HOME/rs10-rc8 bash rs.sh rc8 10 > rs10-rc8.log 2>&1
   OUT=$HOME/rs10-rc9 bash rs.sh kividb 10 > rs10-rc9.log 2>&1; echo SETB3-DONE > setB3-done.txt ;;
esac
