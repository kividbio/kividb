#!/usr/bin/env python3
"""kv.py host:port db CMD [arg ...]: one command, arguments with Python escapes (\\xff, \\x00), reply as repr."""
import codecs
import sys

import redis

hp, db = sys.argv[1], int(sys.argv[2])
args = [codecs.escape_decode(a.encode())[0] for a in sys.argv[3:]]
h, p = hp.rsplit(":", 1)
c = redis.Redis(host=h, port=int(p), db=db, socket_timeout=30)
try:
    r = c.execute_command(*args)
except Exception as e:
    r = "%s: %s" % (type(e).__name__, e)
print(repr(r))
