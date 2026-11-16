#!/usr/bin/env python3
"""Helpers for redis-cli resharding under load (cluster soak).

  slotfix.py nslots <host:port>         -> number of slots the node owns, in its own view
  slotfix.py open <host:port>           -> the slots it has MIGRATING ("slot dest-id" per line)
  slotfix.py selftest                   -> checks that the handover refuses while the source holds keys
  slotfix.py repair <src h:p> <dst h:p> <slot>
      finish a half-moved slot by hand, as redis-cli --cluster fix would if CLUSTER DELSLOTS let it (rc6 finding C):
      SETSLOT IMPORTING on the destination, MIGRATING on the source, MIGRATE ... REPLACE of every key left on the
      source (again after a TRYAGAIN: KiviDB keeps a key written while it travelled, #274), then, only once the source
      holds no key of the slot, SETSLOT NODE on the destination, the source and every other node. Prints one JSON line.
      (The first version did not recognise redis-py's TryAgainError and handed slots over while the source still held
      keys; the source then deleted them. See README, "Resharding".)
Nodes: env NODES="h:p h:p ...".
"""
import json
import os
import sys
import time

import redis


def c(hp):
    h, p = hp.rsplit(":", 1)
    return redis.Redis(host=h, port=int(p), socket_timeout=30, decode_responses=True)


def myself(hp):
    for line in c(hp).execute_command("CLUSTER", "NODES").splitlines():
        f = line.split()
        if "myself" in f[2]:
            return f
    raise SystemExit("no myself line")


def nslots(hp):
    n = 0
    for s in myself(hp)[8:]:
        if s.startswith("["):
            continue
        a, _, b = s.partition("-")
        n += int(b or a) - int(a) + 1
    return n


def open_slots(hp):
    out = []
    for s in myself(hp)[8:]:
        if s.startswith("[") and "->-" in s:
            slot, dest = s[1:-1].split("->-")
            out.append((int(slot), dest))
    return out


class SourceHoldsKeys(Exception):
    pass


def hand_over(cs, slot, did, nodes, conn=None):
    """SETSLOT <slot> NODE <did> on every node in `nodes` (the destination first), but only when the source `cs`
    holds no key of the slot: CLUSTER COUNTKEYSINSLOT must be 0, checked right before. Otherwise raise
    SourceHoldsKeys and send nothing (a source that learns of the new owner while holding keys deletes them)."""
    left = cs.execute_command("CLUSTER", "COUNTKEYSINSLOT", slot)
    if left != 0:
        raise SourceHoldsKeys("source still holds %d keys of slot %d" % (left, slot))
    out = {}
    for hp in nodes:
        try:
            out[hp] = str((conn or c)(hp).execute_command("CLUSTER", "SETSLOT", slot, "NODE", did))
        except Exception as e:
            out[hp] = "error: %s" % e
    return out


def selftest():
    """The guard, against stand-in connections: no SETSLOT may be sent while COUNTKEYSINSLOT is not 0."""
    class Fake:
        def __init__(self, n, log):
            self.n, self.log = n, log

        def execute_command(self, *a):
            self.log.append(a)
            return self.n if a[:2] == ("CLUSTER", "COUNTKEYSINSLOT") else "OK"
    log = []
    try:
        hand_over(Fake(3, log), 7, "d" * 40, ["a:1", "b:2"], conn=lambda hp: Fake(0, log))
        raise SystemExit("FAIL: handed over a slot whose source holds 3 keys")
    except SourceHoldsKeys:
        pass
    assert not any(x[:2] == ("CLUSTER", "SETSLOT") for x in log), log
    log.clear()
    r = hand_over(Fake(0, log), 7, "d" * 40, ["a:1", "b:2"], conn=lambda hp: Fake(0, log))
    assert r == {"a:1": "OK", "b:2": "OK"} and sum(1 for x in log if x[:2] == ("CLUSTER", "SETSLOT")) == 2, (r, log)
    print("selftest OK: refused with 3 keys left (no SETSLOT sent), handed over with 0")


def repair(src, dst, slot):
    t0 = time.time()
    sid, did = myself(src)[0], myself(dst)[0]
    cs, cd = c(src), c(dst)
    res = {"slot": slot, "src_keys_before": cs.execute_command("CLUSTER", "COUNTKEYSINSLOT", slot),
           "dst_keys_before": cd.execute_command("CLUSTER", "COUNTKEYSINSLOT", slot)}
    res["importing"] = str(cd.execute_command("CLUSTER", "SETSLOT", slot, "IMPORTING", sid))
    res["migrating"] = str(cs.execute_command("CLUSTER", "SETSLOT", slot, "MIGRATING", did))
    dh, dp = dst.rsplit(":", 1)
    moved = tryagain = 0
    for _ in range(2000):
        keys = cs.execute_command("CLUSTER", "GETKEYSINSLOT", slot, 100)
        if not keys:
            break
        try:
            r = cs.execute_command("MIGRATE", dh, int(dp), "", 0, 10000, "REPLACE", "KEYS", *keys)
            moved += len(keys)
        except redis.ResponseError as e:
            # redis-py raises TryAgainError for a TRYAGAIN reply and strips the word from the message
            if isinstance(e, redis.exceptions.TryAgainError) or "written while being migrated" in str(e):
                tryagain += 1
                continue
            res["error"] = str(e)
            break
    res["migrated"], res["tryagain"] = moved, tryagain
    order = [dst, src] + [n for n in os.environ["NODES"].split() if n not in (dst, src)]
    try:
        res["setslot_node"] = hand_over(cs, slot, did, order)
    except SourceHoldsKeys as e:
        res["refused_setslot"] = str(e)
        print(json.dumps(res))
        sys.exit(1)
    res["src_keys_after"] = cs.execute_command("CLUSTER", "COUNTKEYSINSLOT", slot)
    res["dst_keys_after"] = cd.execute_command("CLUSTER", "COUNTKEYSINSLOT", slot)
    res["seconds"] = round(time.time() - t0, 2)
    print(json.dumps(res))


def heal(src, dst, slot):
    """A slot whose other nodes already name dst while src still holds keys of it (what the first slotfix.py,
    which did not recognise redis-py's TryAgainError, left): MIGRATE ... REPLACE everything src still holds to dst,
    again after each TRYAGAIN, then SETSLOT NODE dst on src and on every node."""
    t0 = time.time()
    did = myself(dst)[0]
    cs, cd = c(src), c(dst)
    dh, dp = dst.rsplit(":", 1)
    res = {"slot": slot, "src_keys_before": cs.execute_command("CLUSTER", "COUNTKEYSINSLOT", slot),
           "dst_keys_before": cd.execute_command("CLUSTER", "COUNTKEYSINSLOT", slot)}
    moved = tryagain = 0
    for _ in range(5000):
        keys = cs.execute_command("CLUSTER", "GETKEYSINSLOT", slot, 100)
        if not keys:
            break
        try:
            cs.execute_command("MIGRATE", dh, int(dp), "", 0, 10000, "REPLACE", "KEYS", *keys)
            moved += len(keys)
        except redis.ResponseError as e:
            if isinstance(e, redis.exceptions.TryAgainError) or "written while being migrated" in str(e):
                tryagain += 1
                continue
            res["error"] = str(e)
            break
    res["migrated"], res["tryagain"] = moved, tryagain
    try:
        res["setslot_node"] = hand_over(cs, slot, did, [src, dst] + [n for n in os.environ["NODES"].split() if n not in (dst, src)])
    except SourceHoldsKeys as e:
        res["refused_setslot"] = str(e)
        print(json.dumps(res))
        sys.exit(1)
    res["src_keys_after"] = cs.execute_command("CLUSTER", "COUNTKEYSINSLOT", slot)
    res["dst_keys_after"] = cd.execute_command("CLUSTER", "COUNTKEYSINSLOT", slot)
    res["seconds"] = round(time.time() - t0, 2)
    print(json.dumps(res))


cmd = sys.argv[1]
if cmd == "selftest":
    selftest()
elif cmd == "nslots":
    print(nslots(sys.argv[2]))
elif cmd == "open":
    for s, d in open_slots(sys.argv[2]):
        print(s, d)
elif cmd == "heal":
    heal(sys.argv[2], sys.argv[3], int(sys.argv[4]))
elif cmd == "repair":
    repair(sys.argv[2], sys.argv[3], int(sys.argv[4]))
