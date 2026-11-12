#!/usr/bin/env bash
# multi_clock_replica.sh <binary>: master :6621 + replica :6622 on this host, then multi_clock_replica.py.
B=$1; D=$(mktemp -d); mkdir -p $D/m $D/r
(cd $D/m && exec $B --port 6621 --dir $D/m > $D/m/log 2>&1) & MP=$!
echo "replicaof 127.0.0.1 6621" > $D/r/c.conf
(cd $D/r && exec $B --port 6622 --dir $D/r --configfile $D/r/c.conf > $D/r/log 2>&1) & RP=$!
for i in $(seq 100); do redis-cli -p 6622 INFO replication | grep -q 'master_link_status:up' && break; sleep 0.1; done
echo "$B ($($B --version)): $(timeout 120 ~/venv/bin/python multi_clock_replica.py 6621 6622 $MP 30)"
kill -9 $MP $RP; wait 2>/dev/null; rm -rf $D
