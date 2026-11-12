#!/usr/bin/env python3
"""Power-loss-like check, client side (runs on a second VM, so the acks survive the server's crash).

  powerloss.py write  <host> <port> <acks.json>   16 writers (8 SET, 8 INCR), every ack recorded, until
                                                  the server goes away (sysrq-b reboot of the server VM)
  powerloss.py verify <host> <port> <acks.json>   after the server VM is back and the server restarted
"""
import json, sys, threading, time
import redis

mode, host, port, path = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4]

if mode == "write":
    acks = {}; attempts = {}; t0 = time.time(); last_ack = {}
    def w(t):
        c = redis.Redis(host=host, port=port, socket_timeout=5, socket_connect_timeout=5)
        a = []; acks[t] = a; i = 0
        try:
            while True:
                attempts[t] = i + 1
                if t % 2:
                    c.set(f"pl:{t}:{i}", f"v{i}"); a.append(i)
                else:
                    a.append(c.incr(f"plc:{t}"))
                last_ack[t] = time.time()
                i += 1
        except Exception as e:
            pass
    ths = [threading.Thread(target=w, args=(t,)) for t in range(16)]
    for th in ths: th.start()
    for th in ths: th.join()
    json.dump({"acks": acks, "attempts": attempts, "last_ack": last_ack, "t0": t0, "t_end": time.time()}, open(path, "w"))
    print("acked", sum(len(a) for a in acks.values()), "in", round(max(last_ack.values()) - t0, 2), "s")
else:
    d = json.load(open(path)); acks = {int(k): v for k, v in d["acks"].items()}
    attempts = {int(k): v for k, v in d["attempts"].items()}
    r = redis.Redis(host=host, port=port, socket_timeout=30)
    lost = 0; total = 0; over = 0; lost_last_s = []
    for t, a in acks.items():
        total += len(a)
        if t % 2:
            vals = []
            for i in range(0, len(a), 5000):
                vals += r.mget([f"pl:{t}:{x}" for x in a[i:i + 5000]])
            miss = [x for x, v in zip(a, vals) if v != f"v{x}".encode()]
            lost += len(miss)
        else:
            got = int(r.get(f"plc:{t}") or 0); want = a[-1] if a else 0
            if got < want: lost += want - got
            if got > attempts[t]: over += 1
    # how much of the tail was lost, in acked writes per writer per second, for scale
    dur = max(d["last_ack"].values()) - d["t0"]
    print(json.dumps({"acked": total, "lost": lost, "counters_over_attempts": over, "write_seconds": round(dur, 2),
                      "acks_per_s": round(total / dur), "lost_as_seconds_of_writes": round(lost / (total / dur), 3) if total else 0,
                      "dbsize": r.dbsize()}))
