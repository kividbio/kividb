#!/usr/bin/env python3
"""Overwrite checker for the cluster soak (rc8): does every key hold its LATEST acknowledged value?

ackcheck.py writes each key once, so it can find a missing key but not a stale one. A slot moved by
`redis-cli --cluster reshard` and closed by `redis-cli --cluster fix` (#295) can leave an older copy of a key
on one node and a newer one on the other; a fix that moves the wrong copy with REPLACE turns an acknowledged
overwrite back into an older value. This checker overwrites keys again and again and checks the last one.

  owcheck.py --seeds h:p,h:p --writers 8 --rate 200 --duration 7200 --out DIR --hot-file F

Keys are ow:{<tag>}:<writer>:<j>, with <tag> chosen so the key lands in a given slot. Each writer owns its keys
(so its writes to one key are in order) and writes values "<writer>:<seq>".
  - cold pool: one key per slot, slots split between the writers (every slot has keys);
  - hot pool: --hot-keys keys per writer in each slot listed in --hot-file (re-read every 0.5 s). The event driver
    writes there the slots a redis-cli reshard is about to move, so those slots are overwritten many times a
    second while they move and while `--cluster fix` closes them. --hot-frac of the writes go to the hot pool.
For each key: the last acknowledged seq, and the seqs that failed after it (error or no reply, so they may or
may not have been applied). At the end every key that was ever acknowledged is read back:
  ok      = the last acknowledged value, or one of the failed writes after it
  missing = no value
  stale   = an older value than the last acknowledged one (a lost acknowledged overwrite)
  other   = anything else
Output DIR: summary.json, bad.json (every bad key with its slot, expected and found), timeline.csv (per second).
"""
import argparse
import json
import os
import random
import threading
import time

import redis
from redis.cluster import RedisCluster, ClusterNode
from redis.crc import key_slot


def slot_tags():
    tags = [None] * 16384
    left, i = 16384, 0
    while left:
        t = "t%d" % i
        s = key_slot(t.encode())
        if tags[s] is None:
            tags[s] = t
            left -= 1
        i += 1
    return tags


def conn(seeds, timeout):
    nodes = [ClusterNode(h, int(p)) for h, p in (s.rsplit(":", 1) for s in seeds)]
    return RedisCluster(startup_nodes=nodes, socket_timeout=timeout, socket_connect_timeout=timeout,
                        decode_responses=True, cluster_error_retry_attempts=1, require_full_coverage=False)


class State:
    def __init__(self):
        self.lock = threading.Lock()
        self.hot = []
        self.ok = {}
        self.err = {}
        self.err_kinds = {}


def hot_reader(path, st, stop):
    while not stop.is_set():
        try:
            with open(path) as f:
                slots = [int(x) for x in f.read().split()]
        except Exception:
            slots = []
        st.hot = slots
        time.sleep(0.5)


def writer(wid, a, tags, st, stop, last, unknown, ack_t, t0):
    cold = [s for s in range(16384) if s % a.writers == wid]
    ci = 0
    seq = 0
    c = None
    interval = 1.0 / a.rate
    nxt = time.monotonic()
    while not stop.is_set():
        nxt += interval
        d = nxt - time.monotonic()
        if d > 0:
            time.sleep(d)
        else:
            nxt = time.monotonic()
        hot = st.hot
        if hot and random.random() < a.hot_frac:
            key = "ow:{%s}:%d:%d" % (tags[random.choice(hot)], wid, 1 + random.randrange(a.hot_keys))
        else:
            key = "ow:{%s}:%d:0" % (tags[cold[ci]], wid)
            ci = (ci + 1) % len(cold)
        seq += 1
        sec = int(time.monotonic() - t0)
        if c is None:
            try:
                c = conn(a.seeds.split(","), a.timeout)
            except Exception as e:
                with st.lock:
                    st.err[sec] = st.err.get(sec, 0) + 1
                    k = "connect:" + type(e).__name__
                    st.err_kinds[k] = st.err_kinds.get(k, 0) + 1
                time.sleep(0.05)
                continue
        try:
            r = c.set(key, "%d:%d" % (wid, seq))
            if r is not True:
                raise redis.ResponseError("SET returned %r" % (r,))
            last[key] = seq
            unknown.pop(key, None)
            ack_t[key] = round(time.time(), 3)
            with st.lock:
                st.ok[sec] = st.ok.get(sec, 0) + 1
        except Exception as e:
            unknown.setdefault(key, []).append(seq)
            with st.lock:
                st.err[sec] = st.err.get(sec, 0) + 1
                k = type(e).__name__ + ":" + str(e).split(" ")[0][:24]
                st.err_kinds[k] = st.err_kinds.get(k, 0) + 1
            try:
                c.close()
            except Exception:
                pass
            c = None
            time.sleep(0.02)


def verify(a, lasts, unknowns, ack_ts):
    c = conn(a.seeds.split(","), 30)
    res = {"ok": 0, "missing": 0, "stale": 0, "other": 0, "ok_unknown_later": 0}
    bad = []
    for wid in range(a.writers):
        keys = list(lasts[wid].keys())
        for i in range(0, len(keys), 1000):
            chunk = keys[i:i + 1000]
            for _ in range(240):
                try:
                    vals = c.mget_nonatomic(chunk)
                    break
                except Exception:
                    time.sleep(0.5)
            else:
                raise SystemExit("verify: cannot read back")
            for k, v in zip(chunk, vals):
                want = lasts[wid][k]
                unk = unknowns[wid].get(k, [])
                if v is None:
                    kind = "missing"
                else:
                    w, _, s = v.partition(":")
                    s = int(s) if s.isdigit() and w == str(wid) else None
                    if s == want:
                        kind = "ok"
                    elif s in unk:
                        kind = "ok"
                        res["ok_unknown_later"] += 1
                    elif s is not None and s < want:
                        kind = "stale"
                    else:
                        kind = "other"
                res[kind] += 1
                if kind != "ok":
                    bad.append({"key": k, "slot": key_slot(k.encode()), "kind": kind, "want_seq": want,
                                "unknown_after": unk, "found": v, "acked_at": ack_ts[wid].get(k)})
    return res, bad


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", required=True)
    ap.add_argument("--writers", type=int, default=8)
    ap.add_argument("--rate", type=float, default=200, help="SETs/s per writer")
    ap.add_argument("--duration", type=float, default=7200)
    ap.add_argument("--timeout", type=float, default=2.0)
    ap.add_argument("--hot-file", required=True)
    ap.add_argument("--hot-keys", type=int, default=8)
    ap.add_argument("--hot-frac", type=float, default=0.5)
    ap.add_argument("--settle", type=float, default=10)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    tags = slot_tags()
    st = State()
    stop = threading.Event()
    t0 = time.monotonic()
    with open(os.path.join(a.out, "t0"), "w") as f:
        f.write("%.3f\n" % time.time())
    lasts = [{} for _ in range(a.writers)]
    unknowns = [{} for _ in range(a.writers)]
    ack_ts = [{} for _ in range(a.writers)]
    threading.Thread(target=hot_reader, args=(a.hot_file, st, stop), daemon=True).start()
    ths = [threading.Thread(target=writer, args=(w, a, tags, st, stop, lasts[w], unknowns[w], ack_ts[w], t0),
                            daemon=True) for w in range(a.writers)]
    for t in ths:
        t.start()
    while time.monotonic() - t0 < a.duration:
        time.sleep(0.2)
    stop.set()
    for t in ths:
        t.join(timeout=5)
    time.sleep(a.settle)
    json.dump({"last": lasts, "unknown": unknowns}, open(os.path.join(a.out, "state.json"), "w"))
    res, bad = verify(a, lasts, unknowns, ack_ts)
    json.dump(bad, open(os.path.join(a.out, "bad.json"), "w"), indent=1)
    with open(os.path.join(a.out, "timeline.csv"), "w") as f:
        f.write("t,ok,err\n")
        for s in range(int(a.duration) + 1):
            f.write("%d,%d,%d\n" % (s, st.ok.get(s, 0), st.err.get(s, 0)))
    summary = {"keys_acked": sum(len(x) for x in lasts), "acked_writes": sum(st.ok.values()),
               "failed_writes": sum(st.err.values()), "error_kinds": st.err_kinds, **res,
               "bad_by_slot": {}}
    for b in bad:
        summary["bad_by_slot"][str(b["slot"])] = summary["bad_by_slot"].get(str(b["slot"]), 0) + 1
    json.dump(summary, open(os.path.join(a.out, "summary.json"), "w"), indent=1)
    print(json.dumps({k: summary[k] for k in ("keys_acked", "acked_writes", "failed_writes", "ok", "missing",
                                             "stale", "other", "ok_unknown_later")}))


if __name__ == "__main__":
    main()
