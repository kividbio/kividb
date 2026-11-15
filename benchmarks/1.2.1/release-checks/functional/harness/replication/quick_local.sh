#!/usr/bin/env bash
# quick_local.sh: harness sanity check on the client VM (local master 7500 + replica 7501)
stop() { for d in /tmp/t1 /tmp/t2; do [ -f $d/pid ] && kill -9 $(cat $d/pid) 2>/dev/null; done; }
stop; rm -rf /tmp/t1 /tmp/t2; mkdir -p /tmp/t1 /tmp/t2
printf "port 7500\ndir /tmp/t1\nupdate-check no\n" > /tmp/t1/c.conf
printf "port 7501\ndir /tmp/t2\nupdate-check no\nreplicaof 127.0.0.1 7500\n" > /tmp/t2/c.conf
for d in /tmp/t1 /tmp/t2; do (cd $d; exec /opt/kv/kividb --configfile $d/c.conf > $d/log 2>&1 < /dev/null) & echo $! > $d/pid; done
sleep 2; cd ~/h
~/venv/bin/python mkdata.py 127.0.0.1:7500 load --n 2000 --dbs 2 --empty-key --nonutf8 --short-field-ttl --ft
~/venv/bin/python mkdata.py 127.0.0.1:7500 stream --seconds 5 --dbs 2 --nonutf8 --short-field-ttl
sleep 9; ~/venv/bin/python kvdiff.py 127.0.0.1:7500 127.0.0.1:7501; grep WARN /tmp/t2/log | tail -3; stop
