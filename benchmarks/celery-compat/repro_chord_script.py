"""Celery 5.7.0's chord script (CHORD_PART_RETURN_LUA), called the way the Redis result backend calls
it, through redis-py over RESP3 and RESP2, with the raw reply bytes. usage: repro_chord_script.py PORT"""
import socket
import sys
import uuid

import redis
from celery.backends.redis import CHORD_PART_RETURN_LUA

port = int(sys.argv[1])
gid = "celery-taskset-meta-" + str(uuid.uuid4())
keys = [gid + ".j", gid + ".t", gid + ".s", gid + ".d", gid, gid + ".c"]


def args(i):
    value = '[1, "%s", "SUCCESS", %d]' % (uuid.uuid4(), 2 * i)
    # tid, encoded, ordered, group_index, chord_expires, result_expires, claim_expires, claim_token
    return [str(uuid.uuid4()), value, "1", str(i), "86400", "86400", "10", uuid.uuid4().hex]


setup = redis.Redis(port=port)
setup.set(gid + ".s", 3)
for proto in (3, 2):
    r = redis.Redis(port=port, protocol=proto)
    script = r.register_script(CHORD_PART_RETURN_LUA)
    for i in range(3):
        try:
            print(f"port {port} RESP{proto} part {i}: {script(keys=keys, args=args(i))!r}")
        except Exception as e:
            print(f"port {port} RESP{proto} part {i}: {type(e).__name__}: {e}")
    setup.delete(gid + ".j", gid + ".d", gid + ".c")

# The same call, raw, so the reply bytes show.
s = socket.create_connection(("127.0.0.1", port))


def raw(*parts):
    out = b"*%d\r\n" % len(parts) + b"".join(b"$%d\r\n%s\r\n" % (len(p.encode()), p.encode()) for p in parts)
    s.sendall(out)
    s.settimeout(0.5)
    data = b""
    try:
        while True:
            d = s.recv(65536)
            if not d:
                break
            data += d
    except socket.timeout:
        pass
    return data


print("HELLO 3 ->", raw("HELLO", "3")[:40], "...")
print("EVAL (RESP3) ->", raw("EVAL", CHORD_PART_RETURN_LUA, "6", *keys, *args(7)))
