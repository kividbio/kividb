#!/usr/bin/env bash
# setB2.sh, on sc after the soak: the replica clean-stop repro (rs.sh), rc9 then rc8, 3 rounds under load + 1 control each.
cd ~
ulimit -n 65535
until [ -f setB-done.txt ]; do sleep 5; done
bash rs.sh kividb 3 > rs-rc9.log 2>&1
bash rs.sh rc8 3 > rs-rc8.log 2>&1
echo SETB2-DONE > setB2-done.txt
