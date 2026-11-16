#!/usr/bin/env python3
"""multi_clock_replica.py <master-port> <replica-port> <master-pid> [tries]: the same question on a replica, with
the master stopped (SIGSTOP) just before the deadline, so the replica must hide the due field by its own clock.
Each try: on the master HSET h<i> f 1 + HPEXPIREAT now+300 ms; wait until the replica has it; SIGSTOP the master at
~deadline-2 ms; on the replica one MULTI: TIME, HGETALL, 3000 x HEXISTS, HGETALL, TIME; SIGCONT the master."""
import os, signal, sys, time, redis
mp, rp, pid = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]); tries = int(sys.argv[4]) if len(sys.argv) > 4 else 30
m, r = redis.Redis(port=mp), redis.Redis(port=rp)
mixed = inside = 0
for i in range(tries):
    k = "h%d" % i
    m.hset(k, "f", "1"); t = int(time.time() * 1000) + 300
    m.execute_command("HPEXPIREAT", k, t, "FIELDS", 1, "f")
    while not r.hexists(k, "f"):
        time.sleep(0.001)
    while time.time() * 1000 < t - 2:
        pass
    os.kill(pid, signal.SIGSTOP)
    try:
        p = r.pipeline(transaction=True)
        p.time(); p.hgetall(k)
        for _ in range(3000):
            p.hexists(k, "f")
        p.hgetall(k); p.time()
        res = p.execute()
    finally:
        os.kill(pid, signal.SIGCONT)
    (s0, u0), (s1, u1) = res[0], res[3003]
    a, b = s0 * 1000 + u0 / 1000, s1 * 1000 + u1 / 1000
    if a <= t <= b:
        inside += 1
    vals = [b"f" in res[1]] + [bool(x) for x in res[2:3002]] + [b"f" in res[3002]]
    if len(set(vals)) > 1:
        mixed += 1
print("tries=%d deadline_inside_exec(by TIME)=%d mixed_answers_inside_exec=%d" % (tries, inside, mixed))
