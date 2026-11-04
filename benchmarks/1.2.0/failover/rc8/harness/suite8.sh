#!/usr/bin/env bash
# suite8.sh <rep>: the rc8 failover subset, KiviDB only, one rep, on this client's server set (hosts.env).
# Set A (foc: fo1-fo3) runs rep 1, set B (fod: fo4-fo6) runs rep 2, in parallel.
H=$HOME/fo/harness
for s in kill9_everysec kill9_always kill9_checkpoint sigstop partition c_failover c_kill9 c_sigstop; do
  timeout 400 bash "$H/fo_run.sh" kividb "$s" "$1" 2>&1 | tail -1
done
echo SUITE-DONE
