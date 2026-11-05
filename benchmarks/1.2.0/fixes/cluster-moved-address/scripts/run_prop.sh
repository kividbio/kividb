#!/usr/bin/env bash
# e2e for the proposed follow-up, once its binary exists and the other runs are over. Run with sudo.
set -u
H=/home/azureuser
until grep -q EXTRA2-DONE $H/extra2.log && grep -q "^build: exit 0" $H/out/proposal/result.txt 2>/dev/null; do sleep 5; done
mkdir -p $H/bin-proposal && cp $H/tgt-proposal/release/kividb $H/bin-proposal/kividb
PY=$H/venv/bin/python
R=$H/out/e2e
one() { # scenario rep [DOWN]
  local d=$R/$1/proposal-$2; mkdir -p $d
  DOWN=${3:-10} timeout 420 $PY $H/e2e.py $H/bin-proposal/kividb proposal $1 $2 $d > $d/run.log 2>&1
  python3 -c "import json;s=json.load(open('$d/summary.json'));print('$1 $2', s['all_views_and_state_ok_s'], s['writes_failed_after_kill'], s['writes_clusterdown_after_kill'], s['flaps_after_restart'])"
}
for rep in 1 2 3; do one two-masters $rep; done
one master-plus-other-replica 1
one one-master 1
one rapid-move 1
one rapid-move-two-masters 1
one rapid-move-two-masters 2
d=$R/two-masters-down3s/proposal-1; mkdir -p $d
DOWN=3 timeout 420 $PY $H/e2e.py $H/bin-proposal/kividb proposal two-masters 1 $d > $d/run.log 2>&1
python3 -c "import json;s=json.load(open('$d/summary.json'));s['scenario']='two-masters-down3s';json.dump(s,open('$d/summary.json','w'),indent=1);print('down3s', s['all_views_and_state_ok_s'], s['writes_failed_after_kill'], s['writes_clusterdown_after_kill'])"
echo PROP-E2E-DONE
