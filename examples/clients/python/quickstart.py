"""KiviDB with redis-py: strings, expiry, hashes, a pipeline and a transaction.

    pip install -r requirements.txt
    python quickstart.py            # KiviDB on localhost:6380
    KIVIDB_URL=redis://:secret@host:6380/0 python quickstart.py
"""

import os

import redis

r = redis.Redis.from_url(
    os.environ.get("KIVIDB_URL", "redis://localhost:6380/0"), decode_responses=True
)

assert r.ping()

# Start clean: these are the keys this example uses.
r.delete("greeting", "session:42", "visits", "user:1", "balance")

# Strings, with and without an expiry.
r.set("greeting", "hello")
assert r.get("greeting") == "hello"
r.set("session:42", "alice", ex=60)
assert 0 < r.ttl("session:42") <= 60

# Counters.
assert r.incr("visits") == 1
assert r.incrby("visits", 10) == 11

# Hashes.
r.hset("user:1", mapping={"name": "Alice", "plan": "pro", "logins": 0})
r.hincrby("user:1", "logins", 1)
assert r.hgetall("user:1") == {"name": "Alice", "plan": "pro", "logins": "1"}

# A pipeline: many commands, one round trip.
with r.pipeline(transaction=False) as pipe:
    for i in range(100):
        pipe.set(f"item:{i}", i)
    pipe.execute()
assert r.mget("item:0", "item:99") == ["0", "99"]

# A transaction: MULTI/EXEC, with WATCH for optimistic locking.
r.set("balance", 100)
with r.pipeline() as tx:
    tx.watch("balance")
    balance = int(tx.get("balance"))
    tx.multi()
    tx.set("balance", balance - 30)
    tx.execute()
assert r.get("balance") == "70"

print("python quickstart: ok")
