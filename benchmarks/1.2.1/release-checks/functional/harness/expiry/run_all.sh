#!/usr/bin/env bash
# On the VM, after setup.sh: everything, in sequence (the idle CPU step needs a quiet machine).
set -u
ulimit -n 65535
R=~/res; mkdir -p $R
bash ~/tests.sh > $R/tests.console.log 2>&1
for io in uring tokio; do
  pkill -9 -x kividb 2>/dev/null; pkill -9 -x kividb-full 2>/dev/null; sleep 2
  ~/venv/bin/python ~/idle.py ~/bin/kividb $io $R/idle-$io > $R/idle-$io.console.log 2>&1
  rm -rf $R/idle-$io/data
done
for io in uring tokio; do
  for mode in repl crash; do
    pkill -9 -x kividb 2>/dev/null; sleep 2
    ~/venv/bin/python ~/load.py ~/bin/kividb $io $R/load-$mode-$io $mode > $R/load-$mode-$io.console.log 2>&1
  done
done
echo ALL-DONE > $R/ALL-DONE
