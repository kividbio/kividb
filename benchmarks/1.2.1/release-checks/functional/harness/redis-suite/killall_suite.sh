#!/usr/bin/env bash
# Stops any running suite and server (patterns can't match the ssh command that runs this file).
for p in $(pgrep -f 'run_suite\.sh|followup.*\.sh|redis_compat\.sh|repro_'); do [ $p != $$ ] && kill -9 $p; done
pkill -9 -x tclsh8.6; pkill -9 -x kividb-lua[+]tls; pkill -9 -x kividb; sleep 2
pgrep -a 'kividb|tclsh|redis_compat' || echo clean
