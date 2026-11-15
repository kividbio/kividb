#!/usr/bin/env bash
# status.sh: one line per run that has reached its end marker (laptop side, short ssh calls).
cd "$(dirname "$0")"
ip() { awk -v n=$1 '$1==n{print $2}' ../hosts.txt; }
s() { TO=30 bash ssh.sh "$@" 2>/dev/null; }
s $(ip fp) 'grep -q DRIVE-DONE drive.log' && echo "persistence drive DONE"
[ -e ../results/persistence/powerloss/PL-DONE ] && echo "powerloss DONE"
[ -e ../results/persistence/powerloss/ABORTED ] && echo "powerloss ABORTED"
s $(ip fe) 'test -e res/ALL-DONE' && echo "expiry DONE"
s $(ip cl-cc) 'grep -q DRIVE35-DONE p4.log' && echo "cluster DONE"
s $(ip cl) 'grep -q "phase c4 end" run-c4.log' && echo "replication DONE"
