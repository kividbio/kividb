#!/usr/bin/env bash
# All e2e runs, unpatched and patched interleaved. Run with sudo.
set -u
H=/home/azureuser
PY=$H/venv/bin/python
R=$H/out/e2e
mkdir -p $R
one() { # label scenario rep
  local bin=$H/tgt-$1/release/kividb d=$R/$2/$1-$3
  mkdir -p $d
  timeout 420 $PY $H/e2e.py $bin $1 $2 $3 $d > $d/run.log 2>&1
  echo "$(date -u +%T) $1 $2 $3 exit $?: $(python3 -c "import json;s=json.load(open('$d/summary.json'));print(s['all_views_and_state_ok_s'], s['writes_failed_after_kill'], s['writes_clusterdown_after_kill'])" 2>&1)"
}
for rep in 1 2 3; do
  one base two-masters $rep
  one patched two-masters $rep
done
for s in master-plus-other-replica one-master rapid-move; do
  one base $s 1
  one patched $s 1
done
echo E2E-DONE
