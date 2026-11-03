#!/usr/bin/env bash
# sigstop_probe.sh <reps> (on the client): c_sigstop on KiviDB, instrumented, until the reps are done.
# For each run: the replica's promotion offset and the old master's stream end (both from the servers' logs), the
# stopped old master's AOF size, and for every lost acknowledged write whether it is in that AOF.
export RES=$HOME/sig-results
H=$HOME/fo/harness
. $H/hosts.env
for rep in $(seq 1 "$1"); do
  timeout 400 bash $H/fo_run_sigstop_probe.sh kividb c_sigstop $rep > /dev/null 2>&1
  OUT=$RES/kividb/c_sigstop/$rep
  M=$(sed -n 's/^M=\([^ ]*\) R=.*/\1/p' $OUT/roles); R=$(sed -n 's/.* R=//p' $OUT/roles)
  mh=${M%:*}; mp=${M#*:}; rh=${R%:*}; rp=${R#*:}
  promo=$(ssh $rh "grep -a 'Promoted; the stream' /data/$rp/server.log | tail -1 | grep -o 'from offset [0-9]*' | grep -o '[0-9]*'")
  ended=$(ssh $mh "grep -a 'Backlog restarted' /data/$mp/server.log | tail -1 | grep -o 'it ended at [0-9]*' | grep -o '[0-9]*'")
  lost=$(python3 -c "import json;l=json.load(open('$OUT/lost.json'));print(' '.join('ack:%d:%d@%.3f'%(w,s,t) for w,s,t in l['missing']+l['wrong']))")
  inaof=""
  for k in $lost; do
    key=${k%@*}
    c=$(ssh $mh "tr -d '\r' < /data/aof-at-sigstop.aof | grep -a -c -x '$key'")
    inaof="$inaof $key=in_old_master_aof:$c"
  done
  ev=$(head -1 $OUT/events | awk -v t0=$(cat $OUT/t0) '{printf "%.3f", $1-t0}')
  echo "rep $rep: sigstop_logged_t=$ev replica_promoted_at=$promo old_master_stream_ended_at=$ended gap_bytes=$(( ${ended:-0} - ${promo:-0} )) old_master_aof_bytes=$(cat $OUT/aof-at-sigstop-size.txt) lost=[${lost}] ${inaof}" | tee -a $RES/probe.txt
done
echo PROBE-DONE | tee -a $RES/probe.txt
