#!/usr/bin/env bash
# run-all.sh ENGINE...: the data-type matrix for each engine in turn (one
# engine on the server at a time), then the summary tables.
# IO_THREADS (Redis, Valkey) comes from iothreads-sweep.sh.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
for e in "$@"; do
  echo "$(date -u +%T) START $e"
  bash "$HERE/datatypes.sh" "$e" > ~/out/log-"$e".txt 2>&1
  grep -E "WARN|failed|ALL-DONE" ~/out/log-"$e".txt
done
python3 "$HERE/summarize.py" ~/out/datatypes ~/out/datatypes-summary
echo "$(date -u +%T) RUN-ALL-DONE $*"
