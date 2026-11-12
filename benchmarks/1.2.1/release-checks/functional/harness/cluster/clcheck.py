#!/usr/bin/env python3
"""clcheck.py: cluster consistency checks (env NODES="h:p ...").

  clcheck.py topo       every node's CLUSTER SLOTS / SHARDS view the same; no open (migrating/importing) slots;
                        all 16384 slots covered; each master: keys only in slots it owns (SCAN + client-side CRC16; COUNTKEYSINSLOT walks the keyspace, #294)
  clcheck.py replicas   wait until each replica's offset = its master's, then compare key by key
                        (SCAN both sides, DEBUG DIGEST-VALUE of every key on both)
Prints JSON; exit 1 on any difference.
"""
import json
import os
import sys
import time

import redis

NODES = os.environ["NODES"].split()

_T = []
for i in range(256):
    c = i << 8
    for _ in range(8):
        c = ((c << 1) ^ 0x1021) & 0xFFFF if c & 0x8000 else (c << 1) & 0xFFFF
    _T.append(c)


def key_slot(key):
    s = key.find(b"{")
    if s != -1:
        e = key.find(b"}", s + 1)
        if e != -1 and e != s + 1:
            key = key[s + 1:e]
    crc = 0
    for b in key:
        crc = ((crc << 8) & 0xFFFF) ^ _T[((crc >> 8) ^ b) & 0xFF]
    return crc & 16383


def conn(hp, t=30):
    h, p = hp.rsplit(":", 1)
    return redis.Redis(host=h, port=int(p), socket_timeout=t, socket_connect_timeout=5, decode_responses=False)


def parse_nodes(text):
    out = {}
    for line in text.decode().splitlines():
        f = line.split()
        if len(f) < 8:
            continue
        slots, opens = [], []
        for s in f[8:]:
            if s.startswith("["):
                opens.append(s)
                continue
            a, _, b = s.partition("-")
            slots.append((int(a), int(b or a)))
        out[f[0]] = {"addr": f[1].split("@")[0], "flags": f[2], "master": f[3], "slots": slots, "open": opens,
                     "link": f[7], "epoch": int(f[6])}
    return out


def slots_view(c):
    v = []
    for e in c.execute_command("CLUSTER", "SLOTS"):
        master = e[2]
        reps = sorted(r[2].decode() for r in e[3:])
        v.append((int(e[0]), int(e[1]), master[2].decode(), tuple(reps)))
    return sorted(v)


def shards_view(c):
    out = []
    for sh in c.execute_command("CLUSTER", "SHARDS"):
        d = dict(zip(sh[0::2], sh[1::2]))
        slots = [int(x) for x in d[b"slots"]]
        nodes = []
        for n in d[b"nodes"]:
            nd = dict(zip(n[0::2], n[1::2]))
            nodes.append((nd[b"id"].decode(), nd[b"role"].decode(), nd[b"health"].decode()))
        out.append((tuple(slots), tuple(sorted(nodes))))
    return sorted(out)


def topo():
    rep = {"problems": []}
    views, shviews, nodes_by = {}, {}, {}
    for hp in NODES:
        c = conn(hp)
        try:
            views[hp] = slots_view(c)
            shviews[hp] = shards_view(c)
            nodes_by[hp] = parse_nodes(c.execute_command("CLUSTER", "NODES"))
            info = c.execute_command("CLUSTER", "INFO").decode()
            if "cluster_state:ok" not in info:
                rep["problems"].append("%s cluster_state not ok" % hp)
        except Exception as e:
            rep["problems"].append("%s: %s" % (hp, e))
    ref = views[NODES[0]]
    for hp, v in views.items():
        if v != ref:
            rep["problems"].append("%s CLUSTER SLOTS differs from %s" % (hp, NODES[0]))
    for hp, v in shviews.items():
        if v != shviews[NODES[0]]:
            rep["problems"].append("%s CLUSTER SHARDS differs from %s" % (hp, NODES[0]))
    covered = sum(b - a + 1 for a, b, _, _ in ref)
    rep["slots_covered"] = covered
    if covered != 16384:
        rep["problems"].append("slots covered %d" % covered)
    for hp, ns in nodes_by.items():
        for nid, n in ns.items():
            if n["open"]:
                rep["problems"].append("%s sees open slots on %s: %s" % (hp, n["addr"], n["open"][:5]))
            if "fail" in n["flags"]:
                rep["problems"].append("%s sees %s %s" % (hp, n["addr"], n["flags"]))
    # keys only where they belong
    ns = nodes_by[NODES[0]]
    rep["masters"] = {}
    total = 0
    for nid, n in ns.items():
        if "master" not in n["flags"]:
            continue
        owned = set()
        for a, b in n["slots"]:
            owned.update(range(a, b + 1))
        c = conn(n["addr"], 120)
        misplaced = {}
        seen = set()
        cur = 0
        while True:
            cur, ks = c.scan(cur, count=5000)
            for k in ks:
                seen.add(k)
            if cur == 0:
                break
        for k in seen:
            s = key_slot(k)
            if s not in owned:
                misplaced[s] = misplaced.get(s, 0) + 1
        cnt = len(seen)
        db = int(c.dbsize())
        total += db
        rep["masters"][n["addr"]] = {"slots": len(owned), "keys": db, "keys_scanned": cnt,
                                     "misplaced": dict(list(misplaced.items())[:20]),
                                     "misplaced_keys": sum(misplaced.values())}
        if misplaced:
            rep["problems"].append("%s holds %d keys in %d slots it does not own" %
                                   (n["addr"], sum(misplaced.values()), len(misplaced)))
        if cnt != db:
            rep["problems"].append("%s dbsize %d != keys scanned %d" % (n["addr"], db, cnt))
    rep["total_master_keys"] = total
    return rep


def offsets_equal(m, r):
    a = m.info("replication")["master_repl_offset"]
    b = r.info("replication").get("slave_repl_offset")
    return a == b, a, b


def compare(m_hp, r_hp):
    m, r = conn(m_hp, 120), conn(r_hp, 120)
    r.execute_command("READONLY")
    for _ in range(300):
        eq, a, b = offsets_equal(m, r)
        if eq:
            break
        time.sleep(1)
    out = {"master": m_hp, "replica": r_hp, "offsets": [a, b], "offsets_equal": eq}
    mkeys = set()
    cur = 0
    while True:
        cur, ks = m.scan(cur, count=2000)
        mkeys.update(ks)
        if cur == 0:
            break
    rkeys = set()
    cur = 0
    while True:
        cur, ks = r.scan(cur, count=2000)
        rkeys.update(ks)
        if cur == 0:
            break
    out["master_keys"], out["replica_keys"] = len(mkeys), len(rkeys)
    out["only_master"] = len(mkeys - rkeys)
    out["only_replica"] = len(rkeys - mkeys)
    diff = 0
    sample = []
    ks = sorted(mkeys & rkeys)
    for i in range(0, len(ks), 1000):
        chunk = ks[i:i + 1000]
        res = []
        for c in (m, r):
            p = c.pipeline(transaction=False)
            for k in chunk:
                p.execute_command("DEBUG", "DIGEST-VALUE", k)
            res.append(p.execute())
        for k, a, b in zip(chunk, res[0], res[1]):
            if a != b:
                diff += 1
                if len(sample) < 10:
                    sample.append(k.hex())
    out["digest_differences"] = diff
    out["diff_sample"] = sample
    out["sample_only_master"] = [k.hex() for k in list(mkeys - rkeys)[:5]]
    out["sample_only_replica"] = [k.hex() for k in list(rkeys - mkeys)[:5]]
    return out


def replicas():
    ns = parse_nodes(conn(NODES[0]).execute_command("CLUSTER", "NODES"))
    pairs = []
    for nid, n in ns.items():
        if "slave" in n["flags"] and n["master"] in ns:
            pairs.append((ns[n["master"]]["addr"], n["addr"]))
    return {"pairs": [compare(a, b) for a, b in sorted(pairs)]}


def main():
    if sys.argv[1] == "topo":
        r = topo()
        print(json.dumps(r, indent=1))
        sys.exit(1 if r["problems"] else 0)
    r = replicas()
    print(json.dumps(r, indent=1))
    bad = any(p["only_master"] or p["only_replica"] or p["digest_differences"] or not p["offsets_equal"] for p in r["pairs"])
    sys.exit(1 if bad else 0)


main()
