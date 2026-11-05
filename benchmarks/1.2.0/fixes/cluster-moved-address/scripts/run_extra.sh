#!/usr/bin/env bash
# After run_all: two masters move with a 3 s downtime (shorter than the node timeout: no failover). Run with sudo.
set -u
H=/home/azureuser
until grep -q E2E-DONE $H/e2e.log; do sleep 5; done
PY=$H/venv/bin/python
R=$H/out/e2e
for l in base patched; do
  d=$R/two-masters-down3s/$l-1; mkdir -p $d
  DOWN=3 timeout 420 $PY $H/e2e.py $H/tgt-$l/release/kividb $l two-masters 1 $d > $d/run.log 2>&1
  python3 -c "import json;s=json.load(open('$d/summary.json'));s['scenario']='two-masters-down3s';json.dump(s,open('$d/summary.json','w'),indent=1);print('$l', s['all_views_and_state_ok_s'], s['writes_failed_after_kill'], s['writes_clusterdown_after_kill'])"
done
echo EXTRA-DONE
