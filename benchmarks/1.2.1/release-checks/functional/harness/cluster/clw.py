#!/usr/bin/env python3
"""clw.py: acknowledged-write load + checker for the 1.2.1 cluster tests (cluster-aware, binary-safe).

  clw.py run    --seeds h:p,... --out DIR [--writers 16 --rate 250 --hot-frac 0.4 --duration 3600]
               runs until DIR/stop exists (or --duration), then verifies.
  clw.py verify --seeds h:p,... --out DIR    (re-verify the logs of an earlier run)
  clw.py bigfill --seeds h:p,...            (prefill the big collections in the hot slots)
  clw.py tags                                (print the hot slots and their tags)

Every key has exactly one writer, and each writer numbers its writes 1, 2, 3, ... so the latest acknowledged
value of each key/field is known. Key classes (all bytes, redis-py decode_responses=False):
  - cold keys: plain "k:w:i", hash-tagged with a non-UTF-8 tag "{\xff\xfe..}:..", raw binary "\xfe\xff\x00\r\n.."
  - hot keys: in HOT_SLOTS (the slots the tests move), tag = bytes found to hash to that slot (non-UTF-8)
  - the empty key b"" (slot 0), written by writer 0
  - big collections (prefilled by bigfill) in BIG_SLOTS: a list appended to (RPUSH) and a hash written to (HSET)
    by one writer each, plus a static zset and set whose DEBUG DIGEST-VALUE is recorded by bigfill
Kinds: s = SET, h = HSET field f0..f7, l = RPUSH "w:v".
Each writer process logs every result to DIR/w<N>.log:  A|U <kind> <keyhex> <fieldhex|-> <v> <t>
(A = acknowledged, U = error, outcome unknown). The verifier then reads every key at its owner and counts:
  lost  = acknowledged, but absent (string/hash field) / element missing (list)
  stale = value older than the last acknowledged one
  wrong = value never written (neither acked nor attempted)
  dup / disorder (lists) = an element twice / out of order
"""
import argparse
import glob
import json
import os
import random
import sys
import time
from multiprocessing import Process

import redis
from redis.cluster import RedisCluster, ClusterNode

HOT_SLOTS = [0, 100, 1000, 1199, 5500, 6000, 12000, 16383]
BIG_SLOTS = [100, 6000, 12000]
BUCKET = 0.1
SPAN = 10 ** 7  # versions of a run: int(t0) * SPAN + 1, 2, ...; later runs are always newer


def crc16(data):
    crc = 0
    for b in data:
        crc ^= b << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) if crc & 0x8000 else (crc << 1)
            crc &= 0xFFFF
    return crc


def key_slot(key):
    s = key.find(b"{")
    if s != -1:
        e = key.find(b"}", s + 1)
        if e != -1 and e != s + 1:
            key = key[s + 1:e]
    return crc16(key) % 16384


_tag_cache = {}


# CLW_TEXT=1: UTF-8 keys only (for the 1.2.0 control: 1.2.0 refuses keys that are not UTF-8)
TEXT = os.environ.get("CLW_TEXT") == "1"


def tag_for(slot):
    """A non-UTF-8 tag (starts with 0xff) whose CRC16 lands in `slot` (with CLW_TEXT=1: an ASCII one)."""
    if slot in _tag_cache:
        return _tag_cache[slot]
    n = 0
    while True:
        t = (b"t%d" % n) if TEXT else b"\xff" + n.to_bytes(3, "little")
        if b"{" not in t and b"}" not in t and crc16(t) % 16384 == slot:
            _tag_cache[slot] = t
            return t
        n += 1


def build_keys(w, writers, cold):
    """[(kind, key, hot)] for writer w."""
    out = []
    for i in range(cold):
        kind = "shl"[i % 3] if i % 7 else "s"
        style = 0 if TEXT else i % 3
        if style == 0:
            k = b"k:%d:%d" % (w, i)
        elif style == 1:
            k = b"{\xff\xfe" + bytes([w, i % 251]) + b"}:\x80\x81:%d:%d" % (w, i)
        else:
            k = b"\xfe\xff\x00\r\n %d \x00 %d" % (w, i)
        out.append((kind, k, False))
    for s in HOT_SLOTS:
        t = tag_for(s)
        for j, kind in enumerate("sshl"):
            out.append((kind, b"{" + t + (b"}:h:%d:%d" if TEXT else b"}\x00\xff:%d:%d") % (w, j), True))
    if w == 0:
        out.append(("s", b"", True))
    for j, s in enumerate(BIG_SLOTS):
        t = tag_for(s)
        if j % writers == w % writers:
            out.append(("l", b"{" + t + b"}:biglist", True))
        if (j + 1) % writers == w % writers:
            out.append(("h", b"{" + t + b"}:bighash", True))
    return out


def cluster(seeds, timeout=2.0):
    nodes = [ClusterNode(h, int(p)) for h, p in (s.rsplit(":", 1) for s in seeds.split(","))]
    return RedisCluster(startup_nodes=nodes, socket_timeout=timeout, socket_connect_timeout=timeout,
                        decode_responses=False, cluster_error_retry_attempts=0, require_full_coverage=False)


def writer(w, a):
    random.seed(w)
    keys = build_keys(w, a.writers, a.cold)
    hot = [k for k in keys if k[2]]
    cold = [k for k in keys if not k[2]]
    log = open(os.path.join(a.out, "w%d.log" % w), "w", buffering=1 << 16)
    t0 = float(open(os.path.join(a.out, "t0")).read())
    nb = int(a.duration / BUCKET) + 10
    ok = [0] * nb
    err = [0] * nb
    kinds = {}
    rc = None
    v = int(t0) * SPAN
    hi = ci = 0
    interval = 1.0 / a.rate
    nxt = time.monotonic()
    last_stop_check = 0
    stopf = os.path.join(a.out, "stop")
    fails = 0
    while True:
        now = time.monotonic()
        if now - last_stop_check > 0.2:
            last_stop_check = now
            if os.path.exists(stopf) or time.time() - t0 > a.duration:
                break
        nxt += interval
        d = nxt - time.monotonic()
        if d > 0:
            time.sleep(d)
        elif d < -1:
            nxt = time.monotonic()
        if rc is None:
            try:
                rc = cluster(a.seeds, a.timeout)
                fails = 0
            except Exception as e:
                b = int((time.time() - t0) / BUCKET)
                if 0 <= b < nb:
                    err[b] += 1
                k = "connect:" + type(e).__name__
                kinds[k] = kinds.get(k, 0) + 1
                time.sleep(0.1)
                continue
        if random.random() < a.hot_frac:
            kind, key, _ = hot[hi % len(hot)]
            hi += 1
        else:
            kind, key, _ = cold[ci % len(cold)]
            ci += 1
        v += 1
        field = b"f%d" % (v % 8) if kind == "h" else None
        try:
            if kind == "s":
                r = rc.set(key, b"%d:%d:\x00\xff" % (w, v))
                if r is not True:
                    raise redis.ResponseError("SET returned %r" % (r,))
            elif kind == "h":
                rc.hset(key, field, b"%d" % v)
            else:
                rc.rpush(key, b"%d:%d" % (w, v))
            st = "A"
            fails = 0
        except Exception as e:
            st = "U"
            fails += 1
            msg = str(e).split(" ")[0][:24]
            k = type(e).__name__ + ":" + msg
            kinds[k] = kinds.get(k, 0) + 1
            if fails >= 20:
                try:
                    rc.close()
                except Exception:
                    pass
                rc = None
                fails = 0
            time.sleep(0.01)
        t = time.time() - t0
        log.write("%s %s %s %s %d %.3f\n" % (st, kind, key.hex() or "-e-", field.hex() if field else "-", v, t))
        b = int(t / BUCKET)
        if 0 <= b < nb:
            if st == "A":
                ok[b] += 1
            else:
                err[b] += 1
    log.close()
    json.dump({"ok": ok, "err": err, "kinds": kinds}, open(os.path.join(a.out, "w%d.tl.json" % w), "w"))


def load_logs(out):
    """state[(kind,key,field)] = {"acked": max v, "ackt": t, "unk": set(v > ...)} ; lists: acked set, unk set."""
    st = {}
    n_ack = n_unk = 0
    for f in sorted(glob.glob(os.path.join(out, "w*.log"))):
        for line in open(f):
            p = line.split()
            if len(p) != 6:
                continue
            s, kind, kh, fh, v, t = p
            key = b"" if kh == "-e-" else bytes.fromhex(kh)
            field = None if fh == "-" else bytes.fromhex(fh)
            v = int(v)
            t = float(t)
            e = st.setdefault((kind, key, field), {"acked": 0, "ackt": None, "unk": set(), "aset": {}, "all": set()})
            e["all"].add(v)
            if s == "A":
                n_ack += 1
                if kind == "l":
                    e["aset"][v] = t
                elif v > e["acked"]:
                    e["acked"], e["ackt"] = v, t
            else:
                n_unk += 1
                e["unk"].add(v)
    return st, n_ack, n_unk


def verify(a):
    st, n_ack, n_unk = load_logs(a.out)
    base = int(float(open(os.path.join(a.out, "t0")).read())) * SPAN
    top = base + SPAN
    rc = None
    for _ in range(120):
        try:
            rc = cluster(a.seeds, 30)
            break
        except Exception:
            time.sleep(1)
    res = {"acked_writes": n_ack, "unknown_writes": n_unk, "keys": len(st), "lost": [], "stale": [], "wrong": [],
           "list_lost": [], "list_dup": 0, "list_disorder": 0, "list_extra": 0, "read_errors": 0}

    def rd(fn):
        for _ in range(60):
            try:
                return fn()
            except Exception:
                time.sleep(0.5)
        res["read_errors"] += 1
        return None

    hcache = {}
    for (kind, key, field), e in st.items():
        if kind == "s":
            val = rd(lambda: rc.get(key))
            cur = None if val is None else int(val.split(b":")[1])
        elif kind == "h":
            if key not in hcache:
                hcache[key] = rd(lambda: rc.hgetall(key)) or {}
            val = hcache[key].get(field)
            cur = None if val is None else int(val)
        else:
            lst = rd(lambda: rc.lrange(key, 0, -1)) or []
            w = None
            seq = []
            for x in lst:
                if x.startswith(b"p:"):
                    continue
                ww, vv = x.split(b":")
                vv = int(vv)
                if not a.allow_newer or vv < top:
                    seq.append(vv)
            s = set(seq)
            res["list_dup"] += len(seq) - len(s)
            res["list_disorder"] += sum(1 for i in range(1, len(seq)) if seq[i] < seq[i - 1])
            res["list_extra"] += len({x for x in s if base <= x < top} - set(e["aset"]) - e["unk"])
            for v, t in e["aset"].items():
                if v not in s:
                    res["list_lost"].append([key.hex(), v, t])
            continue
        if a.allow_newer and cur is not None and cur >= top:
            continue  # overwritten by a later run
        if e["acked"] == 0:
            if cur is not None and cur >= base and cur not in e["unk"]:
                res["wrong"].append([kind, key.hex(), (field or b"").hex(), cur, None])
            continue
        if cur is None:
            res["lost"].append([kind, key.hex(), (field or b"").hex(), e["acked"], e["ackt"]])
        elif cur < e["acked"]:
            res["stale"].append([kind, key.hex(), (field or b"").hex(), e["acked"], e["ackt"], cur])
        elif cur != e["acked"] and cur not in e["unk"]:
            res["wrong"].append([kind, key.hex(), (field or b"").hex(), e["acked"], e["ackt"], cur])
    # timeline
    tls = [json.load(open(f)) for f in glob.glob(os.path.join(a.out, "w*.tl.json"))]
    if tls:
        nb = max(len(t["ok"]) for t in tls)
        ok = [sum(t["ok"][i] for t in tls if i < len(t["ok"])) for i in range(nb)]
        err = [sum(t["err"][i] for t in tls if i < len(t["err"])) for i in range(nb)]
        last = max((i for i in range(nb) if ok[i] or err[i]), default=0)
        with open(os.path.join(a.out, "timeline.csv"), "w") as f:
            f.write("t,ok,err\n")
            for i in range(last + 1):
                f.write("%.1f,%d,%d\n" % (i * BUCKET, ok[i], err[i]))
        kinds = {}
        for t in tls:
            for k, n in t["kinds"].items():
                kinds[k] = kinds.get(k, 0) + n
        res["error_kinds"] = kinds
        res["error_buckets_100ms"] = sum(1 for x in err if x)
        res["zero_ok_buckets_100ms"] = sum(1 for i in range(last + 1) if ok[i] == 0)
    summ = {k: (len(v) if isinstance(v, list) else v) for k, v in res.items()}
    json.dump(res, open(os.path.join(a.out, "verify.json"), "w"), indent=1)
    json.dump(summ, open(os.path.join(a.out, "summary.json"), "w"), indent=1)
    print(json.dumps(summ))


def bigfill(a):
    rc = cluster(a.seeds, 30)
    dig = {}
    for s in BIG_SLOTS:
        t = b"{" + tag_for(s) + b"}"
        lk, hk, zk, sk = t + b":biglist", t + b":bighash", t + b":bigzset", t + b":bigset"
        for k in (lk, hk, zk, sk):
            rc.delete(k)
        n = a.big
        for i in range(0, n, 5000):
            rng = range(i, min(n, i + 5000))
            rc.rpush(lk, *[b"p:%d" % j for j in rng])
            rc.hset(hk, mapping={b"p\xff%d" % j: b"v%d" % j for j in rng})
            rc.zadd(zk, {b"m\x00\xfe%d" % j: j * 0.5 for j in rng})
            rc.sadd(sk, *[b"s\x80%d" % j for j in rng])
        for k in (zk, sk):
            node = rc.get_node_from_key(k)
            dig[k.hex()] = rc.execute_command("DEBUG", "DIGEST-VALUE", k, target_nodes=node)[0].decode()
        print("slot", s, "filled", n)
    json.dump(dig, open(a.digests, "w"), indent=1)


def bigcheck(a):
    rc = cluster(a.seeds, 30)
    dig = json.load(open(a.digests))
    bad = 0
    for kh, d in dig.items():
        k = bytes.fromhex(kh)
        node = rc.get_node_from_key(k)
        now = rc.execute_command("DEBUG", "DIGEST-VALUE", k, target_nodes=node)[0].decode()
        n = rc.zcard(k) if k.endswith(b"zset") else rc.scard(k)
        print(k, node.name, n, "same" if now == d else "DIFFERENT")
        bad += now != d
    print("bigcheck different:", bad)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd")
    ap.add_argument("--seeds")
    ap.add_argument("--out")
    ap.add_argument("--writers", type=int, default=16)
    ap.add_argument("--rate", type=float, default=250)
    ap.add_argument("--cold", type=int, default=600)
    ap.add_argument("--hot-frac", type=float, default=0.4)
    ap.add_argument("--duration", type=float, default=3600)
    ap.add_argument("--timeout", type=float, default=2.0)
    ap.add_argument("--big", type=int, default=100000)
    ap.add_argument("--allow-newer", action="store_true", help="re-verify an earlier run: values of later runs are fine")
    ap.add_argument("--digests", default=os.path.expanduser("~/bigdigests.json"))
    a = ap.parse_args()
    if a.cmd == "tags":
        for s in HOT_SLOTS:
            print(s, tag_for(s).hex(), key_slot(b"{" + tag_for(s) + b"}x"))
        print("empty key slot", key_slot(b""))
        return
    if a.cmd == "bigfill":
        return bigfill(a)
    if a.cmd == "bigcheck":
        return bigcheck(a)
    if a.cmd == "verify":
        return verify(a)
    os.makedirs(a.out, exist_ok=True)
    for f in glob.glob(os.path.join(a.out, "w*")) + [os.path.join(a.out, "stop")]:
        if os.path.exists(f):
            os.remove(f)
    with open(os.path.join(a.out, "t0"), "w") as f:
        f.write("%.3f\n" % time.time())
    ps = [Process(target=writer, args=(w, a)) for w in range(a.writers)]
    for p in ps:
        p.start()
    for p in ps:
        p.join()
    time.sleep(2)
    verify(a)


if __name__ == "__main__":
    main()
