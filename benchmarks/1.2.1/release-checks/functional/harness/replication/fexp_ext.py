#!/usr/bin/env python3
"""fexp_ext.py MASTER:port REPLICA:port [--n 300] [--modes a,b] [--out file.json]

#415 race, extended: for each try a hash gets field f with a 100 ms TTL (plus a field keep), then right at
the deadline (100 ms +- 4 ms later) one command touches f on the master. Whether the master still had f
decides the result; a replica that expired f by its own clock would apply the command to a hash without it.
After the replica caught up, every key is compared on master and replica: all fields and values (HGETALL),
and each field's HPEXPIRETIME exactly (the deltas are counted: #489 is a 1 ms difference). Each mode runs
in its own thread with its own keys.

Race modes (command at the deadline):
  HPEXPIRE       HPEXPIRE k 600000 FIELDS 1 f
  HGETEX_PX      HGETEX k PX 600000 FIELDS 1 f
  HPERSIST       HPERSIST k FIELDS 1 f
  HGETEX_PERSIST HGETEX k PERSIST FIELDS 1 f
  HINCRBY        HINCRBY k f 1          (f starts at 10)
  HSETNX         HSETNX k f new
  HSETEX_FXX     HSETEX k FXX PX 600000 FIELDS 1 f v2
  HSETEX_KEEPTTL HSETEX k KEEPTTL FIELDS 1 f v2
Plain modes (no race; the field's deadline is far away; HPEXPIRETIME must be equal):
  P_HPEXPIRE P_HEXPIRE P_HPEXPIREAT P_HSETEX_PX P_HSETEX_EX P_HGETEX_PX P_HGETEX_EX
"""
import argparse
import collections
import json
import random
import threading
import time

import redis

RACE = ["HPEXPIRE", "HGETEX_PX", "HPERSIST", "HGETEX_PERSIST", "HINCRBY", "HSETNX", "HSETEX_FXX", "HSETEX_KEEPTTL"]
PLAIN = ["P_HPEXPIRE", "P_HEXPIRE", "P_HPEXPIREAT", "P_HSETEX_PX", "P_HSETEX_EX", "P_HGETEX_PX", "P_HGETEX_EX"]


def conn(hp):
    h, p = hp.rsplit(":", 1)
    return redis.Redis(host=h, port=int(p), socket_timeout=30)


def at_deadline(m, mode, k):
    """Returns whether the master still had f when the command ran."""
    x = m.execute_command
    if mode == "HPEXPIRE":
        r = x("HPEXPIRE", k, 600000, "FIELDS", 1, "f"); return r[0] == 1
    if mode == "HGETEX_PX":
        r = x("HGETEX", k, "PX", 600000, "FIELDS", 1, "f"); return r[0] is not None
    if mode == "HPERSIST":
        r = x("HPERSIST", k, "FIELDS", 1, "f"); return r[0] == 1
    if mode == "HGETEX_PERSIST":
        r = x("HGETEX", k, "PERSIST", "FIELDS", 1, "f"); return r[0] is not None
    if mode == "HINCRBY":
        return x("HINCRBY", k, "f", 1) == 11
    if mode == "HSETNX":
        return x("HSETNX", k, "f", "new") == 0
    if mode == "HSETEX_FXX":
        return x("HSETEX", k, "FXX", "PX", 600000, "FIELDS", 1, "f", "v2") == 1
    if mode == "HSETEX_KEEPTTL":
        alive = m.hexists(k, "f")  # (approximate; the command itself decides)
        x("HSETEX", k, "KEEPTTL", "FIELDS", 1, "f", "v2"); return bool(alive)
    raise ValueError(mode)


def plain(m, mode, k):
    x = m.execute_command
    m.hset(k, mapping={"f": "v", "g": "w", "keep": "k"})
    if mode == "P_HPEXPIRE":
        x("HPEXPIRE", k, 600000 + random.randrange(1000), "FIELDS", 2, "f", "g")
    elif mode == "P_HEXPIRE":
        x("HEXPIRE", k, 600 + random.randrange(100), "FIELDS", 2, "f", "g")
    elif mode == "P_HPEXPIREAT":
        x("HPEXPIREAT", k, int(time.time() * 1000) + 600000 + random.randrange(1000), "FIELDS", 2, "f", "g")
    elif mode == "P_HSETEX_PX":
        x("HSETEX", k, "PX", 600000 + random.randrange(1000), "FIELDS", 2, "f", "v", "g", "w")
    elif mode == "P_HSETEX_EX":
        x("HSETEX", k, "EX", 600 + random.randrange(100), "FIELDS", 2, "f", "v", "g", "w")
    elif mode == "P_HGETEX_PX":
        x("HGETEX", k, "PX", 600000 + random.randrange(1000), "FIELDS", 2, "f", "g")
    elif mode == "P_HGETEX_EX":
        x("HGETEX", k, "EX", 600 + random.randrange(100), "FIELDS", 2, "f", "g")


def run_mode(mhp, mode, n, out):
    m = conn(mhp)
    keys, alive, errs = [], 0, []
    for i in range(n):
        k = f"fx:{mode}:{i}"
        try:
            m.delete(k)
            if mode in PLAIN:
                plain(m, mode, k)
            else:
                m.hset(k, mapping={"f": "10" if mode == "HINCRBY" else "v", "keep": "k"})
                m.execute_command("HPEXPIRE", k, 100, "FIELDS", 1, "f")
                time.sleep(0.100 + random.uniform(-0.004, 0.004))
                if at_deadline(m, mode, k):
                    alive += 1
        except redis.RedisError as e:
            errs.append(str(e))
        keys.append(k)
    out[mode] = {"keys": keys, "alive_on_master": alive, "errors": len(errs), "first_error": errs[0] if errs else None}


def compare(m, r, keys):
    bad, samples, deltas = 0, [], collections.Counter()
    fields_cmp = 0
    for k in keys:
        a = m.hgetall(k); b = r.hgetall(k)
        fs = sorted(set(a) | set(b))
        ea = m.execute_command("HPEXPIRETIME", k, "FIELDS", len(fs), *fs) if fs else []
        eb = r.execute_command("HPEXPIRETIME", k, "FIELDS", len(fs), *fs) if fs else []
        ta = m.execute_command("HTTL", k, "FIELDS", len(fs), *fs) if fs else []
        tb = r.execute_command("HTTL", k, "FIELDS", len(fs), *fs) if fs else []
        diff = a != b
        for x, y in zip(ea, eb):
            fields_cmp += 1
            if x > 0 and y > 0:
                deltas[y - x] += 1
            elif x != y:
                diff = True
        httl_off = [(x, y) for x, y in zip(ta, tb) if (x < 0 or y < 0) and x != y or abs(x - y) > 1]
        if httl_off:
            diff = True
        if diff:
            bad += 1
            if len(samples) < 5:
                samples.append({"key": k, "master": {kk.decode(): vv.decode() for kk, vv in a.items()},
                                "replica": {kk.decode(): vv.decode() for kk, vv in b.items()},
                                "pexpiretime_m": ea, "pexpiretime_r": eb, "httl_m": ta, "httl_r": tb})
    return bad, samples, dict(sorted(deltas.items())), fields_cmp


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("master"); ap.add_argument("replica")
    ap.add_argument("--n", type=int, default=300)
    ap.add_argument("--modes", default=",".join(RACE + PLAIN))
    ap.add_argument("--out")
    a = ap.parse_args()
    modes = a.modes.split(",")
    res = {}
    th = [threading.Thread(target=run_mode, args=(a.master, md, a.n, res)) for md in modes]
    t0 = time.time()
    for t in th: t.start()
    for t in th: t.join()
    m, r = conn(a.master), conn(a.replica)
    off = m.info("replication")["master_repl_offset"]
    for _ in range(200):
        if r.info("replication").get("slave_repl_offset", -1) >= off:
            break
        time.sleep(0.05)
    time.sleep(1.0)
    summary = {"master": a.master, "replica": a.replica, "n": a.n, "seconds": round(time.time() - t0, 1),
               "master_kividb_version": m.info("server").get("kividb_version"),
               "master_expired_fields_propagated": m.info("server").get("kividb_expired_fields_propagated"),
               "modes": {}}
    tot = 0
    for md in modes:
        bad, samples, deltas, fc = compare(m, r, res[md]["keys"])
        tot += bad
        exact = deltas.get(0, 0)
        summary["modes"][md] = {"tries": a.n, "alive_on_master": res[md]["alive_on_master"], "keys_differ": bad,
                                "errors": res[md]["errors"], "first_error": res[md]["first_error"],
                                "fields_compared": fc, "pexpiretime_delta_ms_replica_minus_master": deltas,
                                "pexpiretime_exact": exact, "samples": samples}
        print(f"{md:15s} tries={a.n} alive_on_master={res[md]['alive_on_master']:4d} keys_differ={bad:4d} "
              f"errors={res[md]['errors']} pexpiretime deltas(r-m ms)={deltas}")
    summary["total_keys_differ"] = tot
    print(f"TOTAL keys differing: {tot}")
    if a.out:
        json.dump(summary, open(a.out, "w"), indent=1, default=str)


if __name__ == "__main__":
    main()
