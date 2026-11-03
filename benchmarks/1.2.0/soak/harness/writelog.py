#!/usr/bin/env python3
"""Acknowledged-write log for the standalone soak, over several types, with MULTI/EXEC and hash field TTLs.

Each writer w writes a sequence seq = 0, 1, 2, ... ; the kind of write is seq % 8:

  0  SET wl:s:{w}:{seq} <v>                                 -> GET is <v>
  1  SET wl:e:{w}:{seq} <v> EX 86400                         -> GET is <v>, TTL > 0
  2  SET wl:x:{w}:{seq} <v> PX 20000                         -> gone (expired long before the check)
  3  HSET wl:h:{w}:{seq//64} f{seq} <v>                      -> HGET is <v>
  4  MULTI HSET wl:hf:{w}:{seq//64} f{seq} <v> ; HEXPIRE .. 86400 FIELDS 1 f{seq} EXEC  -> HGET is <v>, HTTL > 0
  5  MULTI HSET wl:hx:{w}:{seq//64} f{seq} <v> ; HPEXPIRE .. 20000 FIELDS 1 f{seq} EXEC -> field gone
  6  MULTI RPUSH wl:l:{w}:{seq//256} seq ; SADD wl:st:{w}:{seq//256} seq ; ZADD wl:z:{w}:{seq//256} seq seq ;
           INCR wl:c:{w} EXEC
        -> seq in the list, the set and the sorted set (score seq); the list is in increasing order and holds
           nothing that was not written; acked(6) <= counter <= acked(6) + unknown(6)
  7  SET wl:o:{w}:{seq % 1000} seq (overwrites)            -> the value is the last acknowledged seq for that key,
                                                               or a later write whose reply was lost

A write counts as acknowledged only when its reply came back as expected. Every write is logged as it completes,
to <out>/w<w>.log: "A <seq> <t>" (acknowledged) or "U <seq> <t>" (error / no reply: may or may not have happened),
t = seconds since the run's start. The check reads the logs, so it can be run again later against any node:

  writelog.py run    --target h:p --writers 8 --rate 100 --duration 7200 --out DIR
  writelog.py verify --addr h:p --out DIR --name master        -> DIR/verify-<name>.json
"""
import argparse
import json
import os
import sys
import threading
import time
from collections import defaultdict

import redis

KINDS = ["str", "str_ttl", "str_expired", "hash", "hash_field_ttl", "hash_field_expired", "multi_l_s_z_incr", "overwrite"]


def val(w, s):
    return "v%d:%d:%s" % (w, s, "x" * (16 + (s * 7) % 48))


def conn(addr, timeout):
    h, p = addr.rsplit(":", 1)
    return redis.Redis(host=h, port=int(p), socket_timeout=timeout, socket_connect_timeout=timeout, decode_responses=True)


def do_write(c, w, s):
    k = s % 8
    v = val(w, s)
    if k == 0:
        return c.set("wl:s:%d:%d" % (w, s), v) is True
    if k == 1:
        return c.set("wl:e:%d:%d" % (w, s), v, ex=86400) is True
    if k == 2:
        return c.set("wl:x:%d:%d" % (w, s), v, px=20000) is True
    if k == 3:
        return c.hset("wl:h:%d:%d" % (w, s // 64), "f%d" % s, v) in (0, 1)
    if k in (4, 5):
        key = ("wl:hf:%d:%d" if k == 4 else "wl:hx:%d:%d") % (w, s // 64)
        p = c.pipeline(transaction=True)
        p.hset(key, "f%d" % s, v)
        if k == 4:
            p.execute_command("HEXPIRE", key, 86400, "FIELDS", 1, "f%d" % s)
        else:
            p.execute_command("HPEXPIRE", key, 20000, "FIELDS", 1, "f%d" % s)
        r = p.execute()
        return r[0] in (0, 1) and list(r[1]) == [1]
    if k == 6:
        b = s // 256
        p = c.pipeline(transaction=True)
        p.rpush("wl:l:%d:%d" % (w, b), s)
        p.sadd("wl:st:%d:%d" % (w, b), s)
        p.zadd("wl:z:%d:%d" % (w, b), {str(s): s})
        p.incr("wl:c:%d" % w)
        r = p.execute()
        return r[1] == 1 and r[2] == 1 and isinstance(r[0], int) and isinstance(r[3], int)
    return c.set("wl:o:%d:%d" % (w, s % 1000), str(s)) is True


class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.ok = defaultdict(int)
        self.err = defaultdict(int)
        self.lat = defaultdict(list)
        self.kinds = defaultdict(int)


def writer(w, a, t0, stop, st):
    f = open(os.path.join(a.out, "w%d.log" % w), "w", buffering=1 << 16)
    c = None
    s = 0
    iv = 1.0 / a.rate if a.rate else 0
    nxt = time.monotonic()
    while not stop.is_set():
        if c is None:
            try:
                c = conn(a.target, a.timeout)
                c.ping()
            except Exception as e:
                c = None
                with st.lock:
                    st.err[int(time.monotonic() - t0)] += 1
                    st.kinds["connect:" + type(e).__name__] += 1
                time.sleep(0.05)
                continue
        if iv:
            nxt += iv
            d = nxt - time.monotonic()
            if d > 0:
                time.sleep(d)
            else:
                nxt = time.monotonic()
        b = time.monotonic()
        try:
            ok = do_write(c, w, s)
            e = time.monotonic()
            if not ok:
                raise redis.ResponseError("unexpected reply")
            f.write("A %d %.3f\n" % (s, e - t0))
            with st.lock:
                st.ok[int(e - t0)] += 1
                st.lat[int(e - t0)].append(int((e - b) * 1e6))
        except Exception as ex:
            e = time.monotonic()
            f.write("U %d %.3f\n" % (s, e - t0))
            with st.lock:
                st.err[int(e - t0)] += 1
                st.kinds[type(ex).__name__ + ":" + str(ex).split(" ")[0][:24]] += 1
            try:
                c.close()
            except Exception:
                pass
            c = None
            time.sleep(0.02)
        s += 1
    f.close()


def run(a):
    os.makedirs(a.out, exist_ok=True)
    t0 = time.monotonic()
    wall0 = time.time()
    json.dump({"t0_wall": wall0, "writers": a.writers, "rate": a.rate, "duration": a.duration, "target": a.target},
              open(os.path.join(a.out, "meta.json"), "w"))
    st = Stats()
    stop = threading.Event()
    th = [threading.Thread(target=writer, args=(w, a, t0, stop, st), daemon=True) for w in range(a.writers)]
    for t in th:
        t.start()
    while time.monotonic() - t0 < a.duration:
        time.sleep(0.5)
    stop.set()
    for t in th:
        t.join(timeout=10)
    with open(os.path.join(a.out, "timeline.csv"), "w") as f:
        f.write("t,ok,err,p50_us,p99_us\n")
        for sec in range(int(a.duration) + 1):
            l = sorted(st.lat.get(sec, []))
            f.write("%d,%d,%d,%s,%s\n" % (sec, st.ok.get(sec, 0), st.err.get(sec, 0),
                                          l[len(l) // 2] if l else "", l[min(len(l) - 1, int(len(l) * 0.99))] if l else ""))
    json.dump(dict(st.kinds), open(os.path.join(a.out, "error_kinds.json"), "w"), indent=1)
    print("RUN-DONE")


def load(out):
    meta = json.load(open(os.path.join(out, "meta.json")))
    logs = {}
    for w in range(meta["writers"]):
        acked, unknown = {}, {}
        for line in open(os.path.join(out, "w%d.log" % w)):
            p = line.split()
            if len(p) != 3:
                continue
            (acked if p[0] == "A" else unknown)[int(p[1])] = float(p[2])
        logs[w] = (acked, unknown)
    return meta, logs


def verify(a):
    meta, logs = load(a.out)
    c = conn(a.addr, 60)
    t_check = time.time() - meta["t0_wall"]
    res = {"node": a.name, "checked_at_s": round(t_check, 1), "per_kind": {}, "problems": []}
    cnt = {k: {"acked": 0, "unknown": 0, "ok": 0, "bad": 0} for k in KINDS}

    def bad(kind, w, s, what):
        cnt[kind]["bad"] += 1
        if len(res["problems"]) < 200:
            res["problems"].append({"kind": kind, "writer": w, "seq": s, "acked_at_s": logs[w][0].get(s), "what": what})

    def pipe(cmds, chunk=2000):
        out = []
        for i in range(0, len(cmds), chunk):
            p = c.pipeline(transaction=False)
            for cmd in cmds[i:i + chunk]:
                p.execute_command(*cmd)
            out.extend(p.execute(raise_on_error=False))
        return out

    for w, (acked, unknown) in logs.items():
        by = defaultdict(list)
        for s in acked:
            by[s % 8].append(s)
        unk = defaultdict(set)
        for s in unknown:
            unk[s % 8].add(s)
        for k in range(8):
            cnt[KINDS[k]]["acked"] += len(by[k])
            cnt[KINDS[k]]["unknown"] += len(unk[k])
        # 0, 1, 2: strings
        for k, fmt in ((0, "wl:s:%d:%d"), (1, "wl:e:%d:%d"), (2, "wl:x:%d:%d")):
            seqs = sorted(by[k])
            cmds = []
            for s in seqs:
                cmds.append(("GET", fmt % (w, s)))
                if k == 1:
                    cmds.append(("TTL", fmt % (w, s)))
            r = pipe(cmds)
            step = 2 if k == 1 else 1
            for i, s in enumerate(seqs):
                v = r[i * step]
                if k == 2:
                    if v is not None and acked[s] + 25 < t_check:
                        bad(KINDS[k], w, s, "expired key is back: %r ttl? " % (v,))
                    else:
                        cnt[KINDS[k]]["ok"] += 1
                    continue
                if v != val(w, s):
                    bad(KINDS[k], w, s, "GET %r" % (v,))
                elif k == 1 and not (isinstance(r[i * 2 + 1], int) and r[i * 2 + 1] > 0):
                    bad(KINDS[k], w, s, "TTL %r" % (r[i * 2 + 1],))
                else:
                    cnt[KINDS[k]]["ok"] += 1
        # 3, 4, 5: hashes, one HGETALL per bucket (+ HTTL of the fields for 4)
        for k, fmt in ((3, "wl:h:%d:%d"), (4, "wl:hf:%d:%d"), (5, "wl:hx:%d:%d")):
            buckets = defaultdict(list)
            for s in by[k]:
                buckets[s // 64].append(s)
            bl = sorted(buckets)
            cmds = []
            for b in bl:
                cmds.append(("HGETALL", fmt % (w, b)))
                if k == 4:
                    fs = ["f%d" % s for s in sorted(buckets[b])]
                    cmds.append(("HTTL", fmt % (w, b), "FIELDS", len(fs)) + tuple(fs))
            r = pipe(cmds, 500)
            step = 2 if k == 4 else 1
            for i, b in enumerate(bl):
                h = r[i * step]
                if isinstance(h, list):  # RESP2 flat list
                    h = dict(zip(h[0::2], h[1::2]))
                if not isinstance(h, dict):
                    h = {}
                ttls = list(r[i * 2 + 1]) if k == 4 and isinstance(r[i * 2 + 1], list) else None
                for j, s in enumerate(sorted(buckets[b])):
                    got = h.get("f%d" % s)
                    if k == 5:
                        if got is not None and acked[s] + 25 < t_check:
                            bad(KINDS[k], w, s, "expired field is back: %r" % (got,))
                        else:
                            cnt[KINDS[k]]["ok"] += 1
                        continue
                    if got != val(w, s):
                        bad(KINDS[k], w, s, "HGET %r" % (got,))
                    elif k == 4 and not (ttls and isinstance(ttls[j], int) and ttls[j] > 0):
                        bad(KINDS[k], w, s, "HTTL %r" % (ttls[j] if ttls else r[i * 2 + 1],))
                    else:
                        cnt[KINDS[k]]["ok"] += 1
        # 6: MULTI list + set + zset + counter
        buckets = defaultdict(list)
        for s in by[6]:
            buckets[s // 256].append(s)
        for s in unk[6]:
            buckets.setdefault(s // 256, [])
        bl = sorted(buckets)
        cmds = []
        for b in bl:
            cmds += [("LRANGE", "wl:l:%d:%d" % (w, b), 0, -1), ("SMEMBERS", "wl:st:%d:%d" % (w, b)),
                     ("ZRANGE", "wl:z:%d:%d" % (w, b), 0, -1, "WITHSCORES")]
        r = pipe(cmds, 300)
        for i, b in enumerate(bl):
            lst = [int(x) for x in (r[i * 3] or [])]
            sm = {int(x) for x in (r[i * 3 + 1] or [])}
            zr = r[i * 3 + 2] or []
            if zr and not isinstance(zr[0], (list, tuple)):
                zr = list(zip(zr[0::2], zr[1::2]))
            zs = {int(m): float(sc) for m, sc in zr}
            if any(lst[j] >= lst[j + 1] for j in range(len(lst) - 1)):
                bad(KINDS[6], w, b * 256, "list out of order or duplicated: bucket %d" % b)
            allowed = set(buckets[b]) | unk[6]
            extra = [x for x in lst if x not in allowed]
            if extra:
                bad(KINDS[6], w, extra[0], "list holds %d entries never written" % len(extra))
            ls = set(lst)
            for s in buckets[b]:
                if s not in ls or s not in sm or zs.get(s) != float(s):
                    bad(KINDS[6], w, s, "in list %s, set %s, zset %r" % (s in ls, s in sm, zs.get(s)))
                else:
                    cnt[KINDS[6]]["ok"] += 1
        ctr = c.get("wl:c:%d" % w)
        ctr = int(ctr) if ctr is not None else 0
        lo, hi = len(by[6]), len(by[6]) + len(unk[6])
        res.setdefault("counters", {})[w] = {"value": ctr, "acked": lo, "acked_plus_unknown": hi, "ok": lo <= ctr <= hi}
        if not lo <= ctr <= hi:
            res["problems"].append({"kind": "counter", "writer": w, "what": "counter %d outside [%d, %d]" % (ctr, lo, hi)})
        # 7: overwrites: last acknowledged seq per key, or a later unknown one
        last = {}
        for s in by[7]:
            kk = s % 1000
            if s > last.get(kk, -1):
                last[kk] = s
        later_unk = defaultdict(set)
        for s in unk[7]:
            later_unk[s % 1000].add(s)
        keys = sorted(last)
        r = pipe([("GET", "wl:o:%d:%d" % (w, kk)) for kk in keys])
        for kk, v in zip(keys, r):
            v = int(v) if v is not None else None
            if v == last[kk] or (v is not None and v > last[kk] and v in later_unk[kk]):
                cnt[KINDS[7]]["ok"] += 1
            else:
                bad(KINDS[7], w, last[kk], "key %d holds %r, last acked %d" % (kk, v, last[kk]))
    res["per_kind"] = cnt
    res["acked_total"] = sum(v["acked"] for v in cnt.values())
    res["unknown_total"] = sum(v["unknown"] for v in cnt.values())
    res["bad_total"] = sum(v["bad"] for v in cnt.values()) + sum(1 for x in res.get("counters", {}).values() if not x["ok"])
    json.dump(res, open(os.path.join(a.out, "verify-%s.json" % a.name), "w"), indent=1)
    print(json.dumps({"node": a.name, "acked": res["acked_total"], "unknown": res["unknown_total"], "bad": res["bad_total"],
                      "checked_keys_ok": sum(v["ok"] for v in cnt.values())}))
    return res["bad_total"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["run", "verify"])
    ap.add_argument("--target")
    ap.add_argument("--addr")
    ap.add_argument("--name", default="node")
    ap.add_argument("--writers", type=int, default=8)
    ap.add_argument("--rate", type=float, default=100)
    ap.add_argument("--duration", type=float, default=60)
    ap.add_argument("--timeout", type=float, default=2.0)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    if a.cmd == "run":
        run(a)
    else:
        sys.exit(1 if verify(a) else 0)


main()
