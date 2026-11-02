#!/usr/bin/env python3
"""Acknowledged-write checker for failover and durability scenarios.

Writers each write their own sequence of unique keys (ack:<writer>:<seq>) and
record every write the server acknowledged. A scenario script (run.sh) breaks
something meanwhile (kills, stops, partitions, fails over) and appends the
moment to an events file. When the run ends, the checker reads every
acknowledged key back from the final primary (or the cluster) and counts the
acknowledged writes that are missing or hold another value.

  ackcheck.py --mode standalone --target-file /tmp/target --duration 90 --out DIR
  ackcheck.py --mode cluster --seeds h1:7001,h2:7001 --duration 90 --out DIR [--wait 1]

--target-file holds "host:port" of the primary; the scenario rewrites it to
switch clients (standalone). In cluster mode redis-py's RedisCluster follows
MOVED and refreshes the topology itself.

--wait N: after each SET the writer sends WAIT N 200, and the write is
counted as "wait-acked" only if WAIT says N replicas have it.

Output (DIR): timeline.csv (per 100 ms: ok, err, p50/p99 latency in us of the
SETs that succeeded), summary.json (acked, lost, wrong, wait-acked lost,
first error, first success after it, recovery times against the events).
"""
import argparse
import json
import os
import random
import threading
import time

import redis
from redis.cluster import RedisCluster, ClusterNode

BUCKET = 0.1


class Shared:
    def __init__(self, n_buckets):
        self.lock = threading.Lock()
        self.ok = [0] * n_buckets
        self.err = [0] * n_buckets
        self.lat = [[] for _ in range(n_buckets)]
        self.err_kinds = {}


def read_target(path):
    with open(path) as f:
        h, p = f.read().strip().rsplit(":", 1)
    return h, int(p)


def standalone_conn(path, timeout):
    h, p = read_target(path)
    return (h, p), redis.Redis(host=h, port=p, socket_timeout=timeout, socket_connect_timeout=timeout,
                               decode_responses=True)


def cluster_conn(seeds, timeout):
    nodes = [ClusterNode(h, int(p)) for h, p in (s.rsplit(":", 1) for s in seeds)]
    return RedisCluster(startup_nodes=nodes, socket_timeout=timeout, socket_connect_timeout=timeout,
                        decode_responses=True, cluster_error_retry_attempts=1, require_full_coverage=False)


def writer(wid, args, t0, stop, shared, acked, wait_acked, pending_unknown):
    seq = 0
    conn = None
    cur = None
    last_check = 0.0
    interval = 1.0 / args.rate if args.rate else 0.0
    nxt = time.monotonic()
    while not stop.is_set():
        now = time.monotonic()
        if args.mode == "standalone" and now - last_check > 0.05:
            last_check = now
            try:
                tgt = read_target(args.target_file)
                if tgt != cur:
                    conn = None
            except Exception:
                pass
        if conn is None:
            try:
                if args.mode == "standalone":
                    cur, conn = standalone_conn(args.target_file, args.timeout)
                else:
                    conn = cluster_conn(args.seeds.split(","), args.timeout)
            except Exception as e:
                conn = None
                b = int((time.monotonic() - t0) / BUCKET)
                with shared.lock:
                    if b < len(shared.err):
                        shared.err[b] += 1
                    k = type(e).__name__
                    shared.err_kinds[k] = shared.err_kinds.get(k, 0) + 1
                time.sleep(0.05)
                continue
        if interval:
            nxt += interval
            d = nxt - time.monotonic()
            if d > 0:
                time.sleep(d)
            else:
                nxt = time.monotonic()
        key = "ack:%d:%d" % (wid, seq)
        val = "%d:%d:%s" % (wid, seq, args.payload)
        s = time.monotonic()
        try:
            r = conn.set(key, val)
            if r is not True:
                raise redis.ResponseError("SET returned %r" % (r,))
            e = time.monotonic()
            acked.append((seq, e - t0))
            if args.wait:
                n = conn.execute_command("WAIT", args.wait, args.wait_ms) \
                    if args.mode == "standalone" else _cluster_wait(conn, key, args.wait, args.wait_ms)
                if int(n) >= args.wait:
                    wait_acked.append(seq)
            b = int((e - t0) / BUCKET)
            with shared.lock:
                if b < len(shared.ok):
                    shared.ok[b] += 1
                    shared.lat[b].append(int((e - s) * 1e6))
        except Exception as ex:
            pending_unknown.append(seq)
            b = int((time.monotonic() - t0) / BUCKET)
            with shared.lock:
                if b < len(shared.err):
                    shared.err[b] += 1
                k = type(ex).__name__ + ":" + str(ex).split(" ")[0][:24]
                shared.err_kinds[k] = shared.err_kinds.get(k, 0) + 1
            if isinstance(ex, (redis.ConnectionError, redis.TimeoutError)) or args.mode == "cluster":
                try:
                    conn.close()
                except Exception:
                    pass
                conn = None
            time.sleep(0.02)
        seq += 1


def _cluster_wait(rc, key, n, ms):
    node = rc.get_node_from_key(key)
    return rc.execute_command("WAIT", n, ms, target_nodes=node)


def verify(args, per_writer):
    """Return (missing, wrong) lists of (wid, seq) among acknowledged writes."""
    if args.mode == "standalone":
        (h, p) = read_target(args.verify_target or args.target_file) if not args.verify_addr else \
            tuple([args.verify_addr.rsplit(":", 1)[0], int(args.verify_addr.rsplit(":", 1)[1])])
        c = redis.Redis(host=h, port=p, socket_timeout=30, decode_responses=True)
        mget = lambda keys: c.mget(keys)
    else:
        c = cluster_conn(args.seeds.split(","), 30)
        mget = lambda keys: c.mget_nonatomic(keys)
    missing, wrong = [], []
    for wid, seqs in per_writer.items():
        for i in range(0, len(seqs), 2000):
            chunk = seqs[i:i + 2000]
            keys = ["ack:%d:%d" % (wid, s) for s in chunk]
            for attempt in range(240):
                try:
                    vals = mget(keys)
                    break
                except Exception:
                    time.sleep(0.5)
            else:
                raise SystemExit("verify: could not read back (acknowledged writes are in acked.json; "
                                 "rerun with --verify-only once the server is up)")
            for s, v in zip(chunk, vals):
                if v is None:
                    missing.append((wid, s))
                elif v != "%d:%d:%s" % (wid, s, args.payload):
                    wrong.append((wid, s))
    return missing, wrong


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["standalone", "cluster"], default="standalone")
    ap.add_argument("--target-file")
    ap.add_argument("--verify-target", help="file with host:port to verify against (default: --target-file)")
    ap.add_argument("--verify-addr", help="host:port to verify against")
    ap.add_argument("--seeds")
    ap.add_argument("--writers", type=int, default=16)
    ap.add_argument("--rate", type=float, default=0, help="per-writer SETs/s cap (0 = as fast as it goes)")
    ap.add_argument("--duration", type=float, default=90)
    ap.add_argument("--timeout", type=float, default=1.0)
    ap.add_argument("--wait", type=int, default=0)
    ap.add_argument("--wait-ms", type=int, default=200)
    ap.add_argument("--payload", default="x" * 32)
    ap.add_argument("--out", required=True)
    ap.add_argument("--events", help="events file the scenario appends 'unix_ts name' lines to")
    ap.add_argument("--settle", type=float, default=3.0, help="seconds to wait after the run before verifying")
    ap.add_argument("--verify-only", action="store_true", help="read back the writes saved in <out>/acked.json")
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)

    n_buckets = int(args.duration / BUCKET) + 50
    shared = Shared(n_buckets)
    if args.verify_only:
        saved = json.load(open(os.path.join(args.out, "acked.json")))
        acked = {int(w): [tuple(x) for x in v] for w, v in saved["acked"].items()}
        wait_acked = {int(w): v for w, v in saved["wait_acked"].items()}
        unknown = {int(w): [None] * n for w, n in saved["unknown"].items()}
        shared.ok, shared.err, shared.lat = saved["timeline"]["ok"], saved["timeline"]["err"], saved["timeline"]["lat"]
        shared.err_kinds = saved["err_kinds"]
        wall0 = saved["t0_wall"]
        n_buckets = saved["n_buckets"]
        t0 = time.monotonic()
    else:
        stop = threading.Event()
        t0 = time.monotonic()
        wall0 = time.time()
        with open(os.path.join(args.out, "t0"), "w") as f:
            f.write("%.3f\n" % wall0)
        acked = {w: [] for w in range(args.writers)}
        wait_acked = {w: [] for w in range(args.writers)}
        unknown = {w: [] for w in range(args.writers)}
        ths = [threading.Thread(target=writer, args=(w, args, t0, stop, shared, acked[w], wait_acked[w], unknown[w]),
                                daemon=True) for w in range(args.writers)]
        for t in ths:
            t.start()
        while time.monotonic() - t0 < args.duration:
            time.sleep(0.2)
        stop.set()
        for t in ths:
            t.join(timeout=5)
        time.sleep(args.settle)

    per_writer = {w: [s for s, _ in acked[w]] for w in acked}
    ack_time = {(w, s): t for w in acked for s, t in acked[w]}
    if not args.verify_only:
        # kept on disk first, so that a failed read-back can be done again later (--verify-only)
        json.dump({"acked": {str(w): acked[w] for w in acked}, "wait_acked": {str(w): wait_acked[w] for w in wait_acked},
                   "unknown": {str(w): len(unknown[w]) for w in unknown}, "n_buckets": n_buckets,
                   "timeline": {"ok": shared.ok, "err": shared.err, "lat": shared.lat},
                   "err_kinds": shared.err_kinds, "t0_wall": wall0},
                  open(os.path.join(args.out, "acked.json"), "w"))
    missing, wrong = verify(args, per_writer)
    wset = {(w, s) for w in wait_acked for s in wait_acked[w]}
    wait_lost = [m for m in missing + wrong if m in wset]

    # timeline
    with open(os.path.join(args.out, "timeline.csv"), "w") as f:
        f.write("t,ok,err,p50_us,p99_us\n")
        last = int((time.monotonic() - t0) / BUCKET)
        for b in range(min(n_buckets, int(args.duration / BUCKET) + 1)):
            lat = sorted(shared.lat[b])
            p50 = lat[len(lat) // 2] if lat else ""
            p99 = lat[min(len(lat) - 1, int(len(lat) * 0.99))] if lat else ""
            f.write("%.1f,%d,%d,%s,%s\n" % (b * BUCKET, shared.ok[b], shared.err[b], p50, p99))

    # events relative to t0
    events = []
    if args.events and os.path.exists(args.events):
        for line in open(args.events):
            parts = line.split(None, 1)
            if len(parts) == 2:
                ts = float(parts[0]) - wall0
                if -5 < ts < args.duration + 60:
                    events.append((round(ts, 3), parts[1].strip()))

    # recovery: after the first event, first bucket with errors, then the first bucket after that
    # (and after the event) in which writes succeed and none fail, and when throughput is back to
    # 80% of its median before the event.
    rec = {}
    if events:
        ev = events[0][0]
        eb = int(max(ev, 0) / BUCKET)
        pre = sorted(shared.ok[max(0, eb - 100):eb]) or [0]
        base = pre[len(pre) // 2]
        first_err = next((b for b in range(eb, n_buckets) if shared.err[b] > 0), None)
        last_err = max((b for b in range(n_buckets) if shared.err[b] > 0 and b >= eb), default=None)
        # the outage: from the event to the first clean bucket after the last error burst that began
        # within 120 s of the event
        zero_from = None
        b = eb
        while b < n_buckets and shared.ok[b] >= max(1, base * 0.8):
            b += 1
        zero_from = b
        back = next((x for x in range(zero_from, n_buckets) if shared.ok[x] >= base * 0.8 and shared.err[x] == 0), None)
        rec = {
            "event_t": ev,
            "pre_event_ok_per_100ms_median": base,
            "first_error_t": first_err * BUCKET if first_err is not None else None,
            "last_error_t": last_err * BUCKET if last_err is not None else None,
            "writes_below_80pct_from_t": zero_from * BUCKET if zero_from < n_buckets else None,
            "throughput_back_t": back * BUCKET if back is not None else None,
            "recovery_s": round(back * BUCKET - ev, 2) if back is not None else None,
        }

    lost_times = sorted(round(ack_time[m], 3) for m in missing + wrong if m in ack_time)
    summary = {
        "mode": args.mode,
        "writers": args.writers,
        "duration_s": args.duration,
        "acked": sum(len(v) for v in per_writer.values()),
        "unacked_errors": sum(len(v) for v in unknown.values()),
        "lost_acked": len(missing),
        "wrong_value": len(wrong),
        "wait_n": args.wait,
        "wait_acked": len(wset),
        "wait_acked_lost": len(wait_lost),
        "lost_ack_time_first": lost_times[0] if lost_times else None,
        "lost_ack_time_last": lost_times[-1] if lost_times else None,
        "lost_sample": [("ack:%d:%d" % m) for m in (missing + wrong)[:20]],
        "events": events,
        "recovery": rec,
        "error_kinds": shared.err_kinds,
    }
    with open(os.path.join(args.out, "summary.json"), "w") as f:
        json.dump(summary, f, indent=1)
    print(json.dumps({k: summary[k] for k in ("acked", "lost_acked", "wrong_value", "wait_acked",
                                             "wait_acked_lost", "unacked_errors", "recovery")}))


if __name__ == "__main__":
    main()
