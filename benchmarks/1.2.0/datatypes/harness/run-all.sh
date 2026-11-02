#!/usr/bin/env bash
# run-all.sh SPEC...: the data-type matrix for each SPEC in turn (one engine on
# the server at a time), then the summary tables. SPEC is ENGINE or
# ENGINE:RUN; a RUN ending in -r<N> is a later round of the same engine (its
# own output folder, no memory pass: memory is measured in the first round).
#   run-all.sh kividb-cand kividb-rc4 redis kividb-rc4:kividb-rc4-r2 kividb-cand:kividb-cand-r2 ...
# IO_THREADS (Redis, Valkey) comes from iothreads-sweep.sh.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
for spec in "$@"; do
  e=${spec%%:*} r=${spec#*:} mp=1
  case $r in *-r[0-9]) mp=0 ;; esac
  echo "$(date -u +%T) START $r"
  RUN=$r MEMPASS=$mp bash "$HERE/datatypes.sh" "$e" > ~/out/log-"$r".txt 2>&1
  grep -E "WARN|failed|ALL-DONE" ~/out/log-"$r".txt
done
python3 "$HERE/summarize.py" ~/out/datatypes ~/out/datatypes-summary
echo "$(date -u +%T) RUN-ALL-DONE $*"
