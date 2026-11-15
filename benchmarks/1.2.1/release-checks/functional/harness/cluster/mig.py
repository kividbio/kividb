#!/usr/bin/env python3
"""mig.py: slot moves for the 1.2.1 cluster tests (env NODES="h:p ...").

  mig.py owner <slot>                         -> host:port of the master that owns the slot
  mig.py asm <dst h:p> <a> <b> [<a> <b> ...]  CLUSTER MIGRATION IMPORT on dst; waits for the task (bounded 900 s)
  mig.py asmfo <dst> <replica h:p> <delay s> <a> <b>  the same, plus CLUSTER FAILOVER on the replica delay s
                                              after the IMPORT's reply (#516 scenario)
  mig.py finish <dst h:p> <slot>...         finish an open (MIGRATING/IMPORTING) slot: MIGRATE the rest, SETSLOT NODE
  mig.py hand <dst h:p> <slot> [<slot> ...]   by hand: SETSLOT IMPORTING / MIGRATING, GETKEYSINSLOT + MIGRATE
                                              (REPLACE, KEYS, binary-safe), SETSLOT NODE on dst, src and all others
Each prints one JSON line per task/slot.
"""
import json
import os
import sys
import time

import redis

BATCH = int(os.environ.get("BATCH", "50"))

NODES = os.environ["NODES"].split()


def conn(hp, t=60):
    h, p = hp.rsplit(":", 1)
    return redis.Redis(host=h, port=int(p), socket_timeout=t, socket_connect_timeout=5, decode_responses=False)


def nodes():
    for hp in NODES:
        try:
            txt = conn(hp).execute_command("CLUSTER", "NODES").decode()
            break
        except Exception:
            continue
    out = []
    for line in txt.splitlines():
        f = line.split()
        sl = []
        for s in f[8:]:
            if s.startswith("["):
                continue
            a, _, b = s.partition("-")
            sl.append((int(a), int(b or a)))
        out.append({"id": f[0], "addr": f[1].split("@")[0], "flags": f[2], "slots": sl})
    return out


def owner(slot):
    for n in nodes():
        if "master" in n["flags"] and "fail" not in n["flags"] and any(a <= slot <= b for a, b in n["slots"]):
            return n
    return None


def asm(dst, ranges, fo=None):
    c = conn(dst)
    src = owner(ranges[0])
    fod = {}
    if fo:  # (replica h:p, delay s): CLUSTER FAILOVER on it, delay s after the IMPORT's reply, from a thread
        import threading
        frc = conn(fo[0], 10)
        frc.ping()

        def failover():
            time.sleep(fo[1])
            fod["fo_sent_ms"] = int(time.time() * 1000)
            try:
                fod["fo_reply"] = frc.execute_command("CLUSTER", "FAILOVER").decode()
            except Exception as e:
                fod["fo_reply"] = "error: %s" % e
    t0 = time.time()
    tid = c.execute_command("CLUSTER", "MIGRATION", "IMPORT", *ranges)
    fod["import_reply_ms"] = int(time.time() * 1000)
    if fo:
        th = threading.Thread(target=failover)
        th.start()
    tid = tid.decode() if isinstance(tid, bytes) else tid
    st = {}
    for _ in range(9000):
        rows = c.execute_command("CLUSTER", "MIGRATION", "STATUS", "ID", tid)
        if rows:
            r = rows[0]
            st = {k.decode(): (v.decode() if isinstance(v, bytes) else v) for k, v in zip(r[0::2], r[1::2])}
            if st.get("state") in ("completed", "failed", "canceled"):
                break
        time.sleep(0.02 if fo else 0.1)
    if fo:
        th.join()
        st.update(fod)
    st["wall_s"] = round(time.time() - t0, 2)
    st["src_addr"] = src["addr"] if src else None
    st["dst_addr"] = dst
    st["ranges"] = ranges
    print(json.dumps(st))


def hand(dst, slots, finish=False):
    ns = nodes()
    dc = conn(dst)
    did = dc.execute_command("CLUSTER", "MYID").decode()
    dh, dp = dst.rsplit(":", 1)
    for s in slots:
        src = owner(s)
        if src["id"] == did:
            print(json.dumps({"slot": s, "skip": "already owned"}))
            continue
        sc = conn(src["addr"])
        t0 = time.time()
        r = {"slot": s, "src": src["addr"], "dst": dst, "migrate_calls": 0, "keys": 0, "retries": 0, "errors": []}
        if not finish:
            dc.execute_command("CLUSTER", "SETSLOT", s, "IMPORTING", src["id"])
            sc.execute_command("CLUSTER", "SETSLOT", s, "MIGRATING", did)
        for _ in range(100000):
            ks = sc.execute_command("CLUSTER", "GETKEYSINSLOT", s, BATCH)
            if not ks:
                break
            try:
                sc.execute_command("MIGRATE", dh, dp, b"", 0, 30000, "REPLACE", "KEYS", *ks)
                r["migrate_calls"] += 1
                r["keys"] += len(ks)
            except redis.ResponseError as e:
                r["retries"] += 1
                if len(r["errors"]) < 5:
                    r["errors"].append(str(e)[:80])
                time.sleep(0.05)
        r["move_s"] = round(time.time() - t0, 3)
        dc.execute_command("CLUSTER", "SETSLOT", s, "NODE", did)
        sc.execute_command("CLUSTER", "SETSLOT", s, "NODE", did)
        for n in ns:
            if "master" in n["flags"] and n["id"] not in (did, src["id"]) and "fail" not in n["flags"]:
                try:
                    conn(n["addr"]).execute_command("CLUSTER", "SETSLOT", s, "NODE", did)
                except Exception as e:
                    r["errors"].append("other %s: %s" % (n["addr"], str(e)[:60]))
        r["total_s"] = round(time.time() - t0, 3)
        print(json.dumps(r), flush=True)


def main():
    cmd = sys.argv[1]
    if cmd == "owner":
        print(owner(int(sys.argv[2]))["addr"])
    elif cmd == "asm":
        asm(sys.argv[2], [int(x) for x in sys.argv[3:]])
    elif cmd == "asmfo":  # asmfo <dst> <replica> <delay s> <a> <b> ...
        asm(sys.argv[2], [int(x) for x in sys.argv[5:]], fo=(sys.argv[3], float(sys.argv[4])))
    elif cmd == "finish":
        hand(sys.argv[2], [int(x) for x in sys.argv[3:]], finish=True)
    elif cmd == "hand":
        hand(sys.argv[2], [int(x) for x in sys.argv[3:]])


main()
