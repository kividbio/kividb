# scen-failover.sh <scenario>: CLUSTER FAILOVER on every shard's replica in turn (rollfo), with every node's sync
# counters before and after (no engine restarts here, so the differences count this scenario's syncs) and each
# demoted master's log since the failover: "Partial resync: continuing" (rc9) or "Full resync from".
source "$(dirname "$0")/env.sh"
SC=$1; E="$D/evidence/$SC"; mkdir -p "$E"
syncs "$SC before" > "$E/syncs-before.txt"; masters > "$E/masters-before.txt"
begin $SC; sleep 15; rollfo $SC; sleep 15
syncs "$SC after" > "$E/syncs-after.txt"; masters > "$E/masters-after.txt"
since=$(( $(date +%s) - $(cat "$D/t0-$SC") + 5 ))
for p in $(awk '{print $1}' "$E/masters-before.txt"); do k logs $p -c kividb --timestamps --since=${since}s > "$E/$p.log" 2>&1
  echo "$p (demoted): $(grep -cE 'Partial resync: continuing' "$E/$p.log") partial, $(grep -cE 'Full resync from' "$E/$p.log") full | $(grep -hE 'Partial resync|Full resync|dataset is offset|continues' "$E/$p.log" | cut -c32-220 | tr '\n' '|')"; done | tee "$E/demoted.txt"
python "$(dirname "$0")/syncdiff.py" "$E/syncs-before.txt" "$E/syncs-after.txt" | tee "$E/syncs-diff.txt"
chk2 loader-shd $SC
