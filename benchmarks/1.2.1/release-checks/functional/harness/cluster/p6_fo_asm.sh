#!/usr/bin/env bash
# p6_fo_asm.sh: check 1 on the 3-VM cluster (the #516 scenario, fixed by #523). Each round is its own clw run
# (70 % of the writes on the hot slots). CLUSTER MIGRATION IMPORT of slots A-B from the owner of slot 500 to another
# master, and a CLUSTER FAILOVER on the source's replica DELAY s after the IMPORT was sent. The delays sweep the
# import from its start to just past its handover (an import of 1200 slots takes ~0.3-0.45 s here, plus ~1 s when an attempt is fenced and retried).
#   env: DELAYS="0 0.05 ..." (one round per delay), TAG (results dir suffix), RANGE="0 1199"
# Per round it records: the import's state/retries/times, when the failover was sent relative to the import's
# start and end (ms), whether the replica was promoted (and in how long), "Manual failover ... refused" log lines
# on the source, then the clw verify (acked, lost, stale, wrong, list_lost).
set -u
. $HOME/h/lib.sh
export HOTF=${HOTF:-0.7}
RANGE=${RANGE:-"0 1199"}
DELAYS=${DELAYS:-"0 0.02 0.05 0.1 0.15 0.2 0.25 0.3 0.35 0.4 0.45 0.5"}
owner() { $PY $H/mig.py owner $1; }
other() { for m in $(for sl in 3000 8000 13000; do owner $sl; done | sort -u); do [ $m != $1 ] && echo $m && return; done; }
asm_retry() { # dst a b : ASM, retried until completed (max 5)
  for i in 1 2 3 4 5; do
    r=$(timeout 300 $PY $H/mig.py asm "$@" 2>&1 | tail -1); echo "$r" >> $OUT/asm.jsonl
    echo "$r" | grep -q '"completed"' && return 0
    ev "asm not completed (try $i): $r"; sleep 3
  done; return 1
}
SUM=$R/p6${TAG:-}.jsonl
n=0
for dl in $DELAYS; do
  n=$((n + 1))
  NODES="$NODES" timeout 300 $PY $H/cltool.py wait-ok > /dev/null
  S=$(owner 500); D=$(other $S)
  read _ _ SR _ <<< "$($PY $H/cltool.py shard 500)"
  WARM=8 start_load $R/p6${TAG:-}-r$n
  refused0=$(s $(hostof $S) "grep -c 'Manual failover requested by .* refused' /data/$(portof $S)/server.log")
  ev "round $n: asm $RANGE $S -> $D, failover on $SR after ${dl}s"
  # the IMPORT, and DELAY s after its reply the CLUSTER FAILOVER, both from one process (mig.py asmfo)
  t_imp=$(date +%s%3N)
  timeout 300 $PY $H/mig.py asmfo $D $SR $dl $RANGE > $OUT/asm1.json 2>&1
  t_fo=$(date +%s%3N)
  fo=$(tail -1 $OUT/asm1.json | jq -r .fo_reply 2>/dev/null)
  p=$(timeout 25 $PY $H/cltool.py wait-master $SR 2>/dev/null); promoted=yes; [ -z "$p" ] && promoted=no && p=null
  ev "failover reply '$fo', promoted=$promoted (waited $p s after the import task ended)"
  cat $OUT/asm1.json >> $OUT/asm.jsonl
  asmst=$(tail -1 $OUT/asm1.json)
  ev "asm: $asmst"
  [ $promoted = yes ] && { w=$(timeout 300 $PY $H/cltool.py wait-replica $S); ev "old source $S replica, link up after $w s"; }
  # if the import did not complete, run it again to the end (from whoever owns the slots now)
  retried=0
  if ! echo "$asmst" | grep -q '"completed"'; then sleep 3; ev "asm again"; asm_retry $D $RANGE; retried=1; fi
  refused1=$(s $(hostof $S) "grep -c 'Manual failover requested by .* refused' /data/$(portof $S)/server.log")
  sleep 10
  stop_load > /dev/null
  tail -1 $OUT/clw.out > $OUT/verify-summary.json
  NODES="$NODES" timeout 300 $PY $H/cltool.py wait-ok > /dev/null
  check_all after > $OUT/check-summary.txt
  python3 - "$n" "$dl" "$S" "$SR" "$D" "$t_imp" "$t_fo" "$promoted" "$p" "$fo" "$((refused1 - refused0))" "$retried" "$OUT" >> $SUM <<'EOF'
import json, sys
n, dl, S, SR, D, t_imp, t_fo, prom, p, fo, refused, retried, out = sys.argv[1:]
try:
    a = json.loads(open(out + "/asm1.json").read().strip().splitlines()[-1])
except Exception as e:
    a = {"error": open(out + "/asm1.json").read()[-200:]}
v = json.loads(open(out + "/verify-summary.json").read())
st, et, fs = a.get("create_time"), a.get("end_time"), a.get("fo_sent_ms")
print(json.dumps({"round": int(n), "delay_s": float(dl), "source": S, "replica": SR, "dest": D,
  "import_state": a.get("state", a.get("error")), "import_retries": a.get("retries"),
  "import_ms": (et - st) if st and et else None,
  "first_attempt_error": a.get("last_error"),
  "fo_after_import_created_ms": fs - st if st and fs else None,
  "fo_minus_import_end_ms": fs - et if et and fs else None,
  "failover_reply": fo, "promoted": prom == "yes", "promoted_s": None if p == "null" else float(p),
  "mf_refused_logs": int(refused), "import_rerun": retried == "1",
  "acked": v["acked_writes"], "unknown": v["unknown_writes"], "lost": v["lost"], "stale": v["stale"],
  "wrong": v["wrong"], "list_lost": v["list_lost"], "list_dup": v["list_dup"], "read_errors": v["read_errors"],
  "topo": open(out + "/check-summary.txt").read().strip().replace("\n", " | ")[-300:]}))
EOF
  tail -1 $SUM
done
echo P6-DONE
