#!/usr/bin/env python3
"""fexp_bulk.py: many hash fields whose deadlines pass while a replica lags or full-syncs (#415 / #531).

  fexp_bulk.py load   MASTER --hashes 20000 --fields 20 --min-ms 3000 --max-ms 25000 [--prefix fb]
      hashes fb:<i>, fields f0..f<F-1> with integer values. Of the fields, ~85% get a TTL uniform in
      [min,max] ms from now, spread over HPEXPIRE (relative), HPEXPIREAT (absolute) and HSETEX PX; the rest
      none. A few hashes have ALL their fields expiring (the master sends a DEL of the hash for those).
  fexp_bulk.py mutate MASTER --seconds 40 [--threads 4] [--rate 2000]
      writes at random fields while they expire: HINCRBY, HPERSIST, HPEXPIRE (extend), HSETNX,
      HSETEX KEEPTTL, HGETEX PX, HGETEX PERSIST, HDEL, and new fields with 200-3000 ms TTLs (so deadlines
      keep passing for the whole run). Each of these gives a different hash on a node that expired a
      field by its own clock earlier than the master did.
  fexp_bulk.py cmp    MASTER REPLICA [--out f.json]
      every fb:* key on both: key sets, HGETALL, and each field's HPEXPIRETIME exactly.
  fexp_bulk.py hide   REPLICA [--sample 3000] [--out f.json]
      a replica must hide a field that is past its deadline while it waits for the master's HDEL:
      counts fields that HGETALL returns with a deadline before the replica's own TIME, and HLEN / HEXISTS /
      HSTRLEN / HMGET answers that disagree with the HGETALL view.
"""
import argparse
import collections
import json
import random
import sys
import threading
import time

import redis

PRE = "fb"


def conn(hp):
    h, p = hp.rsplit(":", 1)
    return redis.Redis(host=h, port=int(p), socket_timeout=60)


def load(a):
    m = conn(a.master)
    rnd = random.Random(1)
    now = int(time.time() * 1000)
    p = m.pipeline(transaction=False)
    nall = 0
    for i in range(a.hashes):
        k = f"{PRE}:{i}"
        p.delete(k)
        p.hset(k, mapping={f"f{j}": str(j) for j in range(a.fields)})
        all_expire = (i % 97 == 0)
        nall += all_expire
        for j in range(a.fields):
            if not all_expire and rnd.random() < 0.15:
                continue
            ms = rnd.randint(a.min_ms, a.max_ms)
            w = rnd.randrange(3)
            if w == 0:
                p.execute_command("HPEXPIRE", k, ms, "FIELDS", 1, f"f{j}")
            elif w == 1:
                p.execute_command("HPEXPIREAT", k, now + ms, "FIELDS", 1, f"f{j}")
            else:
                p.execute_command("HSETEX", k, "PX", ms, "FIELDS", 1, f"f{j}", str(j))
        if i % 200 == 199:
            p.execute(); p = m.pipeline(transaction=False)
    p.execute()
    print(f"loaded {a.hashes} hashes x {a.fields} fields, deadlines {a.min_ms}-{a.max_ms} ms; {nall} hashes with every field expiring")


def mutate(a):
    stop = time.time() + a.seconds
    counts = collections.Counter()
    lock = threading.Lock()

    def worker(seed):
        m = conn(a.master)
        rnd = random.Random(seed)
        x = m.execute_command
        per = a.threads / a.rate
        c = collections.Counter()
        nxt = time.time()
        while time.time() < stop:
            k = f"{PRE}:{rnd.randrange(a.hashes)}"
            f = f"f{rnd.randrange(a.fields)}"
            op = rnd.randrange(10)
            try:
                if op == 0: x("HINCRBY", k, f, 1); c["HINCRBY"] += 1
                elif op == 1: x("HPERSIST", k, "FIELDS", 1, f); c["HPERSIST"] += 1
                elif op == 2: x("HPEXPIRE", k, rnd.randint(1000, 20000), "FIELDS", 1, f); c["HPEXPIRE"] += 1
                elif op == 3: x("HSETNX", k, f, "100"); c["HSETNX"] += 1
                elif op == 4: x("HSETEX", k, "KEEPTTL", "FIELDS", 1, f, "200"); c["HSETEX_KEEPTTL"] += 1
                elif op == 5: x("HGETEX", k, "PX", rnd.randint(500, 20000), "FIELDS", 1, f); c["HGETEX_PX"] += 1
                elif op == 6: x("HGETEX", k, "PERSIST", "FIELDS", 1, f); c["HGETEX_PERSIST"] += 1
                elif op == 7: x("HDEL", k, f); c["HDEL"] += 1
                else:
                    nf = f"n{rnd.randrange(50)}"
                    x("HSETEX", k, "PX", rnd.randint(200, 3000), "FIELDS", 1, nf, "7"); c["HSETEX_new_short"] += 1
            except redis.ConnectionError:
                time.sleep(0.2); m = conn(a.master); x = m.execute_command; c["conn_err"] += 1
            except redis.ResponseError as e:
                c["err:" + str(e)[:40]] += 1
            nxt += per
            d = nxt - time.time()
            if d > 0:
                time.sleep(d)
        with lock:
            counts.update(c)

    th = [threading.Thread(target=worker, args=(s,)) for s in range(a.threads)]
    for t in th: t.start()
    for t in th: t.join()
    print("mutate:", dict(counts), "total", sum(v for k, v in counts.items() if not k.startswith(("err", "conn"))))


def scan(c, pat):
    out, cur = set(), 0
    while True:
        cur, ks = c.scan(cur, match=pat, count=5000)
        out.update(ks)
        if cur == 0:
            return out


def cmp_(a):
    m, r = conn(a.master), conn(a.replica)
    km, kr = scan(m, PRE + ":*"), scan(r, PRE + ":*")
    only_m, only_r = km - kr, kr - km
    keys = sorted(km & kr)
    bad, val_diff, ttl_diff, fields = 0, 0, 0, 0
    deltas = collections.Counter()
    samples = []
    for i in range(0, len(keys), 500):
        chunk = keys[i:i + 500]
        pm, pr = m.pipeline(transaction=False), r.pipeline(transaction=False)
        for k in chunk:
            pm.hgetall(k); pr.hgetall(k)
        hm, hr = pm.execute(), pr.execute()
        pm, pr = m.pipeline(transaction=False), r.pipeline(transaction=False)
        fl = []
        for k, a1, b1 in zip(chunk, hm, hr):
            fs = sorted(set(a1) | set(b1)); fl.append(fs)
            pm.execute_command("HPEXPIRETIME", k, "FIELDS", len(fs), *fs)
            pr.execute_command("HPEXPIRETIME", k, "FIELDS", len(fs), *fs)
        em, er = pm.execute(), pr.execute()
        for k, a1, b1, fs, x1, y1 in zip(chunk, hm, hr, fl, em, er):
            d = False
            if a1 != b1:
                val_diff += 1; d = True
            for x, y in zip(x1, y1):
                fields += 1
                if x > 0 and y > 0:
                    deltas[y - x] += 1
                    if x != y:
                        ttl_diff += 1; d = True
                elif x != y:
                    ttl_diff += 1; d = True
            if d:
                bad += 1
                if len(samples) < 8:
                    dm = {f.decode(): (a1.get(f, b"-").decode(), t) for f, t in zip(fs, x1) if a1.get(f) != b1.get(f) or t != y1[fs.index(f)]}
                    dr = {f.decode(): (b1.get(f, b"-").decode(), t) for f, t in zip(fs, y1) if a1.get(f) != b1.get(f) or t != x1[fs.index(f)]}
                    samples.append({"key": k.decode(), "master": dm, "replica": dr})
    res = {"keys_master": len(km), "keys_replica": len(kr), "only_master": len(only_m), "only_replica": len(only_r),
           "only_samples": [x.decode() for x in list(only_m)[:5]] + [x.decode() for x in list(only_r)[:5]],
           "keys_differ": bad, "keys_value_differ": val_diff, "fields_ttl_differ": ttl_diff, "fields_compared": fields,
           "pexpiretime_delta_ms_replica_minus_master": dict(sorted(deltas.items())), "samples": samples,
           "master_expired_subkeys": m.info("stats").get("expired_subkeys"),
           "replica_expired_subkeys": r.info("stats").get("expired_subkeys")}
    res["equal"] = (bad == 0 and not only_m and not only_r)
    print(json.dumps({k: v for k, v in res.items() if k != "samples"}), "samples:", json.dumps(res["samples"][:3]))
    if a.out:
        json.dump(res, open(a.out, "w"), indent=1)
    sys.exit(0 if res["equal"] else 1)


def hide(a):
    """Each hash is read in one MULTI (TIME, HGETALL, HLEN, HMGET, HPEXPIRETIME, HEXISTS of every name), so the
    answers are of one moment and must agree with each other."""
    r = conn(a.replica)
    keys = list(scan(r, PRE + ":*"))
    random.shuffle(keys)
    keys = keys[:a.sample]
    allf = [f"f{j}" for j in range(20)] + [f"n{j}" for j in range(50)]
    leaked, hlen_off, hexists_off, hmget_off, checked, fields_seen = 0, 0, 0, 0, 0, 0
    samples = []
    for k in keys:
        p = r.pipeline(transaction=True)
        p.time(); p.hgetall(k); p.hlen(k); p.hmget(k, allf)
        p.execute_command("HPEXPIRETIME", k, "FIELDS", len(allf), *allf)
        for f in allf:
            p.hexists(k, f)
        res = p.execute()
        (s, us), h, hl, hv, et, exr = res[0], res[1], res[2], res[3], res[4], res[5:]
        now = s * 1000 + us // 1000
        if not h:
            continue
        checked += 1; fields_seen += len(h)
        for f, t, v, e in zip(allf, et, hv, exr):
            inh = f.encode() in h
            if inh and 0 < t < now:
                leaked += 1
                if len(samples) < 5:
                    samples.append({"key": k.decode(), "field": f, "deadline": t, "replica_time": now})
            if bool(e) != inh:
                hexists_off += 1
            if (v is not None) != inh:
                hmget_off += 1
        if hl != len(h):
            hlen_off += 1
    res = {"hashes_checked": checked, "fields_seen": fields_seen, "returned_past_deadline": leaked,
           "hlen_vs_hgetall_off": hlen_off, "hexists_vs_hgetall_off": hexists_off, "hmget_vs_hgetall_off": hmget_off,
           "replica_link": r.info("replication").get("master_link_status"), "samples": samples}
    print(json.dumps(res))
    if a.out:
        json.dump(res, open(a.out, "w"), indent=1)
def main():
    global PRE
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd")
    ap.add_argument("master")
    ap.add_argument("replica", nargs="?")
    ap.add_argument("--hashes", type=int, default=20000)
    ap.add_argument("--fields", type=int, default=20)
    ap.add_argument("--min-ms", type=int, default=3000)
    ap.add_argument("--max-ms", type=int, default=25000)
    ap.add_argument("--seconds", type=float, default=40)
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--rate", type=float, default=2000)
    ap.add_argument("--sample", type=int, default=3000)
    ap.add_argument("--prefix", default="fb")
    ap.add_argument("--out")
    a = ap.parse_args()
    PRE = a.prefix
    if a.cmd == "hide":
        a.replica = a.master
    {"load": load, "mutate": mutate, "cmp": cmp_, "hide": hide}[a.cmd](a)


if __name__ == "__main__":
    main()
