"""The sequence of commands whose replies are compared with Redis 8.10.2: python3 compat.py <port>."""
import sys, time, redis
port = int(sys.argv[1])
r = redis.Redis(port=port, decode_responses=False)

def cmd(*a):
    try:
        return r.execute_command(*a)
    except Exception as e:
        return "ERR:" + str(e)[:60]

r.flushall()
cmd("CONFIG", "SET", "maxmemory-policy", "noeviction")
print("policy", cmd("CONFIG", "GET", "maxmemory-policy"))
cmd("SET", "foo", "bar")
payload = r.dump("foo")
cmd("DEL", "foo")
print("restore idletime 1000:", cmd("RESTORE", "foo", 0, payload, "IDLETIME", 1000))
print("idletime ~1000:", 1000 <= cmd("OBJECT", "IDLETIME", "foo") <= 1010)
print("freq under noeviction:", cmd("OBJECT", "FREQ", "foo"))
print("get:", cmd("GET", "foo"), "idletime after get:", cmd("OBJECT", "IDLETIME", "foo"))
cmd("DEL", "foo")
print("restore freq 100 under noeviction:", cmd("RESTORE", "foo", 0, payload, "FREQ", 100))
print("idletime after restore freq:", cmd("OBJECT", "IDLETIME", "foo"))
cmd("SET", "other", "x")
time.sleep(2.1)
for c in ("TTL", "PTTL", "TYPE", "EXISTS", "EXPIRETIME"):
    cmd(c, "other")
cmd("OBJECT", "ENCODING", "other")
print("idletime >=2 after TTL/TYPE/EXISTS/OBJECT:", cmd("OBJECT", "IDLETIME", "other") >= 2)
print("touch:", cmd("TOUCH", "other"), "idletime:", cmd("OBJECT", "IDLETIME", "other"))
print("idletime missing key:", cmd("OBJECT", "IDLETIME", "nokey"))
cmd("CONFIG", "SET", "maxmemory-policy", "allkeys-lfu")
cmd("DEL", "foo")
print("restore freq 100 lfu:", cmd("RESTORE", "foo", 0, payload, "FREQ", 100))
f = cmd("OBJECT", "FREQ", "foo")
print("freq 99..100:", 99 <= f <= 100)
print("idletime under lfu:", cmd("OBJECT", "IDLETIME", "foo"))
cmd("DEL", "foo")
print("restore idletime under lfu:", cmd("RESTORE", "foo", 0, payload, "IDLETIME", 1000))
print("freq after idletime restore:", cmd("OBJECT", "FREQ", "foo"))
cmd("CONFIG", "SET", "maxmemory-policy", "allkeys-lru")
cmd("DEL", "foo")
print("restore idletime lru:", cmd("RESTORE", "foo", 0, payload, "IDLETIME", 1000))
print("idletime lru ~1000:", 1000 <= cmd("OBJECT", "IDLETIME", "foo") <= 1010)
print("freq under lru:", cmd("OBJECT", "FREQ", "foo"))
cmd("DEL", "foo")
print("restore freq under lru:", cmd("RESTORE", "foo", 0, payload, "FREQ", 100))
print("idletime ~0:", cmd("OBJECT", "IDLETIME", "foo") < 2)
