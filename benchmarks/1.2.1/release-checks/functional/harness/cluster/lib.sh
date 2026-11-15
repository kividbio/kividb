# lib.sh: sourced by the phase scripts on cl-cc.
C1=10.82.1.11 C2=10.82.1.12 C3=10.82.1.13
export NODES="$C1:7001 $C2:7001 $C3:7001 $C1:7002 $C2:7002 $C3:7002"
SEEDS=$C1:7001,$C2:7001,$C3:7001
PY=$HOME/venv/bin/python
H=$HOME/h
R=$HOME/res
mkdir -p $R
s() { for i in 1 2 3 4 5 6; do ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "$@"; rc=$?; [ $rc != 255 ] && return $rc; sleep 3; done; return 255; }
ev() { echo "$(date +%s.%N) $*" >> $OUT/events; echo "$(date -u +%T) $*" | tee -a $OUT/events.log; }
hostof() { echo ${1%:*}; }
portof() { echo ${1#*:}; }
nodecmd() { s $(hostof $1) "bash node.sh $2 $(portof $1)"; }
# start: the acked-write checker, a background redis-benchmark mix, a cluster-state poller
start_load() {
  OUT=$1; mkdir -p $OUT; : > $OUT/events
  $PY $H/clw.py run --seeds $SEEDS --out $OUT --writers ${WRITERS:-16} --rate ${RATE:-250} --hot-frac ${HOTF:-0.4} --duration ${LDUR:-5400} > $OUT/clw.out 2>&1 &
  CLW=$!
  $PY $H/bg.py --seeds $SEEDS --procs ${BGP:-4} --sleep ${BGSLEEP:-0.002} --stop-file $OUT/stop > $OUT/bg.err 2>&1 &
  BG=$!
  ( while [ ! -e $OUT/stop ]; do for hp in $NODES; do echo "$(date +%s.%N),$hp,$(timeout 1 redis-cli -h ${hp%:*} -p ${hp#*:} CLUSTER INFO 2>/dev/null | tr -d '\r' | awk -F: '$1=="cluster_state"{print $2}'),$(timeout 1 redis-cli -h ${hp%:*} -p ${hp#*:} ROLE 2>/dev/null | head -1)"; done; sleep 0.5; done ) > $OUT/state.csv &
  POLL=$!
  sleep ${WARM:-20}
}
stop_load() {
  touch $OUT/stop
  wait $BG 2>/dev/null
  wait $POLL 2>/dev/null
  wait $CLW
  tail -1 $OUT/clw.out
}
check_all() { # label
  timeout 600 redis-cli --cluster check $C1:7001 > $OUT/check-$1.txt 2>&1
  grep -E '^\[(OK|ERR|WARNING)\]' $OUT/check-$1.txt | sort | uniq -c | tr '\n' ' '; echo
  timeout 900 $PY $H/clcheck.py topo > $OUT/topo-$1.json 2>&1; echo "topo rc=$? problems=$(jq -c .problems $OUT/topo-$1.json 2>/dev/null | head -c 600)"
}
