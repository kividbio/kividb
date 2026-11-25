"""redis-py through Sentinel: writes to the master Sentinel names, and keeps
writing to the new one after a failover. The contract run.sh expects of
each client is in run.sh."""

import os
import sys
import time

from redis.exceptions import ConnectionError, ReadOnlyError, TimeoutError
from redis.sentinel import MasterNotFoundError, Sentinel

sentinels = [(h, int(p)) for h, p in (a.split(":") for a in os.environ["SENTINELS"].split(","))]
name, password, key = os.environ["MASTER"], os.environ["PASSWORD"], os.environ["KEY"]
sentinel = Sentinel(
    sentinels,
    socket_timeout=1,
    password=password,
    sentinel_kwargs={"socket_timeout": 1, "password": os.environ["SENTINEL_PASSWORD"]},
)
master = sentinel.master_for(name, socket_timeout=1, retry_on_timeout=False)

# Reads from a replica Sentinel lists.
replica = sentinel.slave_for(name, socket_timeout=1)
role = replica.execute_command("ROLE")[0]
if role not in (b"slave", "slave"):
    sys.exit(f"slave_for gave a {role!r}")

first, errors, after = None, 0, 0
deadline = time.monotonic() + float(os.environ.get("DEADLINE", "60"))
while time.monotonic() < deadline:
    try:
        n = master.incr(key)
        port = master.info("server")["tcp_port"]
    except (ConnectionError, TimeoutError, ReadOnlyError, MasterNotFoundError):
        errors += 1
        time.sleep(0.1)
        continue
    if first is None:
        first = port
        print(f"ready {port}", flush=True)
    elif port != first:
        after += 1
        if after >= 10:
            if int(master.get(key)) != n:
                sys.exit("the new master does not read back the last write")
            print(f"followed {first} -> {port} after {errors} failed writes", flush=True)
            sys.exit(0)
    time.sleep(0.1)
sys.exit(f"still on {first} at the deadline, {errors} failed writes")
