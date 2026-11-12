#!/usr/bin/env bash
# persist_race.sh <binary> <io-model>: PERSIST racing its key's deadline, master (AOF on) + replica.
# 1M times: SET p:i v PX 2, then PERSIST p:i in the same pipeline, so many PERSISTs reach a key whose
# 2 ms are just over and that active expiry has not taken out yet. Then: persistent keys on each side.
B=$1 IO=${2:-uring}
pkill -9 -x kividb; sleep 0.5
rm -rf /tmp/pr; mkdir -p /tmp/pr/m /tmp/pr/r
(cd /tmp/pr/m && exec $B --port 7901 --dir /tmp/pr/m --aof --io-model $IO > log 2>&1 &)
(cd /tmp/pr/r && exec $B --port 7902 --dir /tmp/pr/r --io-model $IO > log 2>&1 &)
sleep 1
redis-cli -p 7902 REPLICAOF 127.0.0.1 7901 >/dev/null; sleep 2
python3 - <<'EOF'
out = bytearray()
for i in range(1000000):
    k = b"p:%d" % i
    out += b"*5\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$1\r\nv\r\n$2\r\nPX\r\n$1\r\n2\r\n" % (len(k), k)
    out += b"*2\r\n$7\r\nPERSIST\r\n$%d\r\n%s\r\n" % (len(k), k)
open("/tmp/pr/both.resp", "wb").write(out)
EOF
redis-cli -p 7901 --pipe < /tmp/pr/both.resp | tail -1
count() { # persistent keys among p:* on port $1
  redis-cli -p $1 --scan --pattern 'p:*' | awk '{print "PTTL " $0}' | redis-cli -p $1 | grep -c '^-1$'
}
sleep 1
echo "$($B --version) io=$IO"
echo "after 1 s: master dbsize=$(redis-cli -p 7901 dbsize) replica dbsize=$(redis-cli -p 7902 dbsize) master persistent=$(count 7901) replica persistent=$(count 7902)"
sleep 5
echo "after 6 s: master dbsize=$(redis-cli -p 7901 dbsize) replica dbsize=$(redis-cli -p 7902 dbsize) master persistent=$(count 7901) replica persistent=$(count 7902) $(redis-cli -p 7901 info replication | grep -a master_repl_offset | tr -d '\r') $(redis-cli -p 7902 info replication | grep -a slave_repl_offset | tr -d '\r')"
python3 - <<'EOF'
import glob
d = b"".join(open(f, "rb").read() for f in glob.glob("/tmp/pr/m/appendonly.aof*"))
# Each PERSIST record: was the key's UNLINK/DEL logged before it (after its SET)?
recs = d.split(b"*")
last = {}  # key -> last op
bare = 0
i = 0
toks = d.split(b"\r\n")
ops = []
j = 0
while j < len(toks):
    t = toks[j]
    if t.startswith(b"*") and t[1:].isdigit():
        n = int(t[1:]); args = [toks[j + 2 + 2 * a] for a in range(n)]; ops.append(args); j += 1 + 2 * n
    else:
        j += 1
for a in ops:
    if len(a) < 2:
        continue
    op, k = a[0].upper(), a[1]
    if op == b"PERSIST" and last.get(k) == b"SET":
        bare += 1
    last[k] = op
n_p = sum(1 for a in ops if a[0].upper() == b"PERSIST")
n_u = sum(1 for a in ops if a[0].upper() in (b"UNLINK", b"DEL"))
print("AOF: PERSIST records", n_p, "UNLINK/DEL records", n_u, "PERSIST right after the SET (no UNLINK between)", bare)
EOF
pkill -9 -x kividb
