#!/usr/bin/env bash
# aoftail.sh: on the server VM after a power-off, before the server is restarted: the AOF's size, how many zero bytes
# it ends in, and its last 64 KiB kept as /data/aof-tail-<n>.bin (evidence).
f=/data/6379/appendonly.aof
ls -l /data/6379/
python3 - "$f" <<'PY'
import sys
d = open(sys.argv[1], "rb").read()
z = len(d) - len(d.rstrip(b"\0"))
print("aof_size", len(d), "trailing_zero_bytes", z)
print("last 160 bytes:", d[-160:])
PY
n=$(ls /data/aof-tail-*.bin 2>/dev/null | wc -l); tail -c 65536 "$f" > /data/aof-tail-$((n+1)).bin
