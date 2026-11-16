#!/usr/bin/env bash
# run_all.sh <phase>: the r121 FINAL replication suite (878746e4), one phase at a time, on the client VM.
# Phases use FO1-FO3, so they run one after another (chain: run_all.sh chain). Log: ~/run-<phase>.log
set -u
cd "$(dirname "$0")"
P=$1
if [ "$P" = chain ]; then for p in fx mixed c1 c2 c3 c4; do bash "$0" $p; done; exit 0; fi
exec >> ~/run-$P.log 2>&1
echo "=== phase $P start $(date -u +%FT%TZ)"
case $P in
fx)  # check 4: hash-field TTLs, 1.2.1 <-> 1.2.1
  for rep in 1 2 3; do timeout 900 bash fexp_scen.sh race $rep; done
  for s in lag_stop lag_netem fullsync; do for rep in 1 2 3; do timeout 900 bash fexp_scen.sh $s $rep; done; done
  timeout 900 bash fexp_scen.sh chain 1
  timeout 900 bash fexp_scen.sh promote 1
  timeout 900 bash fexp_scen.sh lag_stop 1 kv120 kv120     # control: 1.2.0 both sides
  timeout 900 bash fexp_scen.sh race 1 kv120 kv120 ;;
pair)  # the earlier #415 reproduction, unchanged (one host, :6501 -> :6502), 3 x 300 tries, plus 1.2.0 once
  mkdir -p ~/pair-results
  for rep in 1 2 3; do bash pair.sh /opt/kv/kividb; timeout 300 ~/venv/bin/python fexp_race_pair.py 127.0.0.1 127.0.0.1 300 > ~/pair-results/kividb-$rep.txt 2>&1; cat ~/pair-results/kividb-$rep.txt; done
  bash pair.sh /opt/kv120/kividb; timeout 300 ~/venv/bin/python fexp_race_pair.py 127.0.0.1 127.0.0.1 300 > ~/pair-results/kv120-1.txt 2>&1; cat ~/pair-results/kv120-1.txt
  bash pair.sh stop ;;
mixed)  # check 5
  for t in a a2 b back; do timeout 900 bash mixed.sh $t; done
  timeout 1500 bash mixed_fexp.sh ;;
c1)  # check 1: kill -9 the master, promote, rejoin; 3 reps each
  for rep in 1 2 3; do for s in kill9_promote_rejoin wait_kill9_promote_rejoin minrep_kill9_promote_rejoin; do
    timeout 600 bash fo_run.sh kividb $s $rep
  done; done ;;
c2)  # check 2: Sentinel
  for rep in 1 2 3; do timeout 600 bash sen_run.sh kividb sen_kill9 $rep; done
  timeout 600 bash sen_run.sh kividb sen_prio0 1 ;;
c3)  for rep in 1 2 3; do timeout 900 bash eq.sh $rep; done ;;
c4)  for rep in 1 2 3; do timeout 600 bash fo_run.sh kv120 rolling_upgrade $rep; done ;;
esac
echo "=== phase $P end $(date -u +%FT%TZ)"
