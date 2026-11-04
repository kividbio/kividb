#!/usr/bin/env bash
# After run_extra: two masters move together, then again 3 s after restarting (relay path, stale relays). Run with sudo.
set -u
H=/home/azureuser
until grep -q EXTRA-DONE $H/extra.log; do sleep 5; done
PY=$H/venv/bin/python
R=$H/out/e2e
for rep in 1 2; do
for l in base patched; do
  d=$R/rapid-move-two-masters/$l-$rep; mkdir -p $d
  timeout 420 $PY $H/e2e.py $H/tgt-$l/release/kividb $l rapid-move-two-masters $rep $d > $d/run.log 2>&1
  python3 -c "import json;s=json.load(open('$d/summary.json'));print('$l $rep', s['all_views_and_state_ok_s'], s['writes_failed_after_kill'], s['writes_clusterdown_after_kill'], s['flaps_after_restart'])"
done
done
echo EXTRA2-DONE
