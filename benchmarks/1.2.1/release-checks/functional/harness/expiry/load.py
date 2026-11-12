#!/usr/bin/env python3
"""Expiry correctness under load, with a recorded oracle.

  load.py <kividb-binary> <io-model> <outdir> repl  [secs=300]   replica attached at 60 s, compared at the end
  load.py <kividb-binary> <io-model> <outdir> crash [secs=240]   --aof, kill -9 at 60/120/180 s, restart

Oracle workers (processes) each own a disjoint key range and are the only writers to it, so each knows
exactly what every one of its keys must be. For each acknowledged write a worker records the value and the
deadline window [send time + ttl, reply time + ttl] (client and server share the machine's clock), and:
  - checks every reply against the oracle (EXPIRE/PERSIST/DEL counts, GETEX values), when the key's state
    is determinate at both the send and the reply time (not within MARGIN of its deadline);
  - schedules a read-back of every key it gives a TTL at a random time BEFORE its deadline, and of every
    persistent key; the read must return the value written;
  - after a reconnect (crash mode) and at the end, reads back every key: GET and PTTL, where a key that must
    be alive must have its value and a PTTL inside its deadline window, and a key that must be gone is gone.
Ops: SET EX 1..30, SET (no TTL), EXPIRE 1..60, PEXPIRE 100..30000, PERSIST, GETEX EX 1..30, GETEX PERSIST, DEL.
Background: redis-benchmark loops on other keys (SET EX 7/25/3600, EXPIRE 3, PERSIST, GETEX EX 15, DEL).
"""
import heapq, json, multiprocessing as mp, os, random, signal, subprocess, sys, time
import redis
from redis.retry import Retry
from redis.backoff import NoBackoff


def conn(port):
    # no silent retries: an op in flight at a crash must surface, so its key is marked unknown
    return redis.Redis(port=port, socket_timeout=10, retry=Retry(NoBackoff(), 0), retry_on_timeout=False)

BIN, IO, OUT, MODE = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
SECS = int(sys.argv[5]) if len(sys.argv) > 5 else (300 if MODE == "repl" else 240)
PORT, RPORT = 7600, 7601
WORKERS, K = 6, 20000
MARGIN = 0.05  # s
os.makedirs(OUT, exist_ok=True)
logf = open(os.path.join(OUT, "load.log"), "a")


def say(*a):
    s = time.strftime("%H:%M:%S ") + " ".join(str(x) for x in a)
    print(s, flush=True); logf.write(s + "\n"); logf.flush()


class Server:
    def __init__(self, name, port, *args):
        self.d = os.path.join(OUT, name); os.makedirs(self.d, exist_ok=True)
        self.port, self.args, self.p = port, list(args), None

    def start(self):
        lf = open(os.path.join(self.d, "server.log"), "ab")
        self.p = subprocess.Popen([BIN, "--port", str(self.port), "--io-model", IO, "--dir", self.d,
                                   "--loglevel", "notice"] + self.args, cwd=self.d, stdout=lf, stderr=lf)
        r = redis.Redis(port=self.port, socket_timeout=60)
        for _ in range(1200):
            try:
                r.ping(); r.dbsize(); return r
            except (redis.ConnectionError, redis.BusyLoadingError, redis.ResponseError):
                time.sleep(0.1)
        raise RuntimeError("no start")

    def kill9(self):
        self.p.send_signal(signal.SIGKILL); self.p.wait()


# ---------------------------------------------------------------- worker
def worker(wid, port, end, qout):
    rnd = random.Random(1000 + wid)
    keys = [f"o{wid}:{i}" for i in range(K)]
    val = {}            # key -> value or None (absent)
    dl = {}             # key -> None (no TTL) or (lo, hi) seconds
    amb = set()         # keys whose state is unknown (op in flight at a crash)
    ver = {}
    heap = []
    st = {"ops": 0, "checks": 0, "reply_checks": 0, "errors": 0, "err": [], "reconnects": 0,
          "post_crash_verified": 0, "post_crash_lost": 0, "final_verified": 0, "ambiguous": 0}
    seq = [0]
    r = conn(port)

    def err(msg):
        st["errors"] += 1
        if len(st["err"]) < 40:
            st["err"].append(f"{time.time():.3f} {msg}")

    def state(k, t):
        """'present' / 'absent' / None (undetermined) at time t."""
        if k in amb:
            return None
        if val.get(k) is None:
            return "absent"
        d = dl.get(k)
        if d is None or d[0] > t + MARGIN:
            return "present"
        if d[1] < t - MARGIN:
            return "absent"
        return None

    def det(k, t0, t1):
        a, b = state(k, t0), state(k, t1)
        return a if a == b else None

    def newval():
        seq[0] += 1
        return f"{wid}-{seq[0]}"

    def bump(k, check_by=None):
        ver[k] = ver.get(k, 0) + 1
        if check_by is not None:
            heapq.heappush(heap, (check_by, k, ver[k]))

    def schedule_ttl(k, t1, lo):
        # a read-back at a random time before the deadline
        if lo - MARGIN * 2 > t1:
            bump(k, rnd.uniform(t1, lo - MARGIN * 2))
        else:
            bump(k)

    def verify_all(label):
        """GET + PTTL every determinate key; returns (verified, mismatches)."""
        ok = bad = 0
        for lo_i in range(0, K, 500):
            ks = keys[lo_i:lo_i + 500]
            p = r.pipeline(transaction=False)
            for k in ks:
                p.get(k); p.pttl(k)
            t0 = time.time(); out = p.execute(); t1 = time.time()
            for j, k in enumerate(ks):
                g, pt = out[2 * j], out[2 * j + 1]
                s = det(k, t0, t1)
                if s is None:
                    continue
                g = g.decode() if g is not None else None
                if s == "present":
                    d = dl.get(k)
                    good = g == val[k]
                    if good and d is None:
                        good = pt == -1
                    elif good:
                        good = (d[0] - t1 - MARGIN) * 1000 <= pt <= (d[1] - t0 + MARGIN) * 1000
                    if not good:
                        bad += 1; err(f"{label}: {k} want {val[k]!r} dl={d} t0={t0:.3f} got {g!r} pttl={pt}")
                    else:
                        ok += 1
                else:
                    if g is not None:
                        bad += 1; err(f"{label}: {k} must be gone (val={val.get(k)!r} dl={dl.get(k)}) got {g!r} pttl={pt}")
                    else:
                        ok += 1
        return ok, bad

    def reconnect():
        nonlocal r
        st["reconnects"] += 1
        for _ in range(1200):
            try:
                r = conn(port); r.ping(); r.dbsize(); break
            except (redis.ConnectionError, redis.TimeoutError, redis.BusyLoadingError, redis.ResponseError):
                time.sleep(0.1)
        ok, bad = verify_all("post-crash")
        st["post_crash_verified"] += ok; st["post_crash_lost"] += bad

    while time.time() < end:
        now = time.time()
        k = None
        try:
            if heap and heap[0][0] <= now:
                when, k, v = heapq.heappop(heap)
                if ver.get(k) != v:
                    continue
                t0 = time.time(); g = r.get(k); t1 = time.time()
                s = det(k, t0, t1)
                if s is not None:
                    st["checks"] += 1
                    g = g.decode() if g is not None else None
                    want = val[k] if s == "present" else None
                    if g != want:
                        err(f"read-back {k}: want {want!r} (dl={dl.get(k)}, scheduled {when:.3f}) at {t0:.3f} got {g!r}")
                continue
            k = rnd.choice(keys)
            op = rnd.random()
            st["ops"] += 1
            if op < 0.30:  # SET EX
                ttl = rnd.randint(1, 30); v = newval()
                t0 = time.time(); rep = r.set(k, v, ex=ttl); t1 = time.time()
                if rep is not True:
                    err(f"SET {k}: {rep!r}")
                val[k] = v; dl[k] = (t0 + ttl, t1 + ttl); amb.discard(k); schedule_ttl(k, t1, t0 + ttl)
            elif op < 0.40:  # SET persistent
                v = newval()
                t0 = time.time(); r.set(k, v); t1 = time.time()
                val[k] = v; dl[k] = None; amb.discard(k); bump(k, t1 + rnd.uniform(0, 10))
            elif op < 0.55 or op < 0.65:  # EXPIRE s / PEXPIRE ms
                if op < 0.55:
                    ttl = rnd.randint(1, 60); t0 = time.time(); rep = r.expire(k, ttl); t1 = time.time()
                else:
                    ttl = rnd.randint(100, 30000) / 1000.0; t0 = time.time(); rep = r.pexpire(k, int(ttl * 1000)); t1 = time.time()
                s = det(k, t0, t1)
                if s is not None:
                    st["reply_checks"] += 1
                    if bool(rep) != (s == "present"):
                        err(f"EXPIRE {k} {ttl}: reply {rep} but oracle {s} (val={val.get(k)!r} dl={dl.get(k)})")
                if rep:
                    dl[k] = (t0 + ttl, t1 + ttl)
                    if k not in amb and val.get(k) is None:
                        amb.add(k)  # already reported if determinate
                    schedule_ttl(k, t1, t0 + ttl)
                else:
                    val[k] = None; amb.discard(k)  # the server says it isn't there
                    bump(k)
            elif op < 0.75:  # PERSIST
                t0 = time.time(); rep = r.persist(k); t1 = time.time()
                s = det(k, t0, t1)
                if s is not None:
                    st["reply_checks"] += 1
                    want = 1 if (s == "present" and dl.get(k) is not None) else 0
                    if int(rep) != want:
                        err(f"PERSIST {k}: reply {rep} want {want} (val={val.get(k)!r} dl={dl.get(k)})")
                if rep:
                    dl[k] = None; bump(k, t1 + rnd.uniform(0, 10))
                elif s is None and k not in amb:
                    # undetermined means it had a TTL near its end: 0 says it had expired
                    val[k] = None
            elif op < 0.85:  # GETEX EX
                ttl = rnd.randint(1, 30)
                t0 = time.time(); g = r.getex(k, ex=ttl); t1 = time.time()
                s = det(k, t0, t1)
                g = g.decode() if g is not None else None
                if s is not None:
                    st["reply_checks"] += 1
                    want = val[k] if s == "present" else None
                    if g != want:
                        err(f"GETEX {k} EX {ttl}: got {g!r} want {want!r} (dl={dl.get(k)})")
                if g is not None:
                    if k in amb:
                        val[k] = g; amb.discard(k)
                    dl[k] = (t0 + ttl, t1 + ttl); schedule_ttl(k, t1, t0 + ttl)
                else:
                    val[k] = None; amb.discard(k)  # the server says it isn't there
                    bump(k)
            elif op < 0.90:  # GETEX PERSIST
                t0 = time.time(); g = r.getex(k, persist=True); t1 = time.time()
                s = det(k, t0, t1)
                g = g.decode() if g is not None else None
                if s is not None:
                    st["reply_checks"] += 1
                    want = val[k] if s == "present" else None
                    if g != want:
                        err(f"GETEX {k} PERSIST: got {g!r} want {want!r} (dl={dl.get(k)})")
                if g is not None:
                    if k in amb:
                        val[k] = g; amb.discard(k)
                    dl[k] = None; bump(k, t1 + rnd.uniform(0, 10))
                else:
                    val[k] = None; amb.discard(k)  # the server says it isn't there
                    bump(k)
            else:  # DEL
                t0 = time.time(); rep = r.delete(k); t1 = time.time()
                s = det(k, t0, t1)
                if s is not None:
                    st["reply_checks"] += 1
                    if int(rep) != (1 if s == "present" else 0):
                        err(f"DEL {k}: reply {rep} oracle {s} (val={val.get(k)!r} dl={dl.get(k)})")
                val[k] = None; dl[k] = None; amb.discard(k); bump(k, t1 + rnd.uniform(0, 5))
        except (redis.ConnectionError, redis.TimeoutError, redis.BusyLoadingError) as e:
            if k is not None:
                amb.add(k)
            reconnect()
    # final
    try:
        ok, bad = verify_all("final")
    except Exception as e:  # noqa
        ok, bad = 0, 0; err(f"final verify failed: {e!r}")
    st["final_verified"] = ok
    st["final_mismatch"] = bad
    st["ambiguous"] = len(amb)
    st["live_keys_at_end"] = sum(1 for kk in keys if state(kk, time.time()) == "present")
    qout.put((wid, st))


# ---------------------------------------------------------------- main
def background(end):
    cmds = [["SET", "bg:__rand_int__", "x", "EX", "7"], ["SET", "bg:__rand_int__", "x", "EX", "25"],
            ["SET", "bgp:__rand_int__", "x", "EX", "3600"], ["EXPIRE", "bg:__rand_int__", "3"],
            ["PERSIST", "bg:__rand_int__"], ["GETEX", "bg:__rand_int__", "EX", "15"], ["DEL", "bg:__rand_int__"]]
    procs = []
    for c in cmds:
        loop = (f"while [ $(date +%s) -lt {int(end)} ]; do timeout $(( {int(end)} - $(date +%s) + 1 )) "
                f"redis-benchmark -p {PORT} -c 4 -n 2000000000 -r 300000 -q {' '.join(c)} >/dev/null 2>&1; sleep 0.3; done")
        procs.append(subprocess.Popen(["bash", "-c", loop], start_new_session=True))
    return procs


def offsets(m, rep):
    return m.info("replication")["master_repl_offset"], rep.info("replication").get("slave_repl_offset")


def wait_synced(m, rep, label, timeout=120):
    t = time.time()
    while time.time() - t < timeout:
        a, b = offsets(m, rep)
        if a == b:
            time.sleep(0.5)
            a2, b2 = offsets(m, rep)
            if a2 == b2 == a:
                return True, a
        time.sleep(0.2)
    a, b = offsets(m, rep)
    say(f"{label}: offsets never matched: master {a} replica {b}")
    return False, (a, b)


def keyset(c):
    return {k for k in c.scan_iter(count=5000)}


def compare(m, rep, label, values=False):
    ks_m, ks_r = keyset(m), keyset(rep)
    only_m, only_r = ks_m - ks_r, ks_r - ks_m
    out = {"dbsize_master": m.dbsize(), "dbsize_replica": rep.dbsize(), "keys_master": len(ks_m),
           "keys_replica": len(ks_r), "only_master": len(only_m), "only_replica": len(only_r),
           "only_master_ex": [k.decode() for k in list(only_m)[:5]], "only_replica_ex": [k.decode() for k in list(only_r)[:5]]}
    out["_only_m"], out["_only_r"] = list(only_m), list(only_r)
    if values:
        diff, n = 0, 0
        ks = sorted(ks_m & ks_r)
        ex = []
        for i in range(0, len(ks), 2000):
            chunk = ks[i:i + 2000]
            pm, pr = m.pipeline(transaction=False), rep.pipeline(transaction=False)
            for k in chunk:
                pm.get(k); pm.pexpiretime(k); pr.get(k); pr.pexpiretime(k)
            a, b = pm.execute(), pr.execute()
            for j in range(len(chunk)):
                n += 1
                if a[2 * j] != b[2 * j] or a[2 * j + 1] != b[2 * j + 1]:
                    diff += 1
                    if len(ex) < 5: ex.append((chunk[j].decode(), a[2 * j:2 * j + 2], b[2 * j:2 * j + 2]))
        out["values_compared"] = n; out["value_or_ttl_diffs"] = diff; out["diff_ex"] = [str(x) for x in ex]
    om, orr = out.pop("_only_m"), out.pop("_only_r")
    say(label, json.dumps(out))
    out["_only_m"], out["_only_r"] = om, orr
    return out


def main():
    res = {"io": IO, "mode": MODE, "secs": SECS}
    margs = ["--aof"] if (MODE == "crash" and not os.environ.get("NOAOF")) or os.environ.get("MASTER_AOF") else []
    master = Server("master", PORT, *margs)
    m = master.start()
    say("master I/O model:", subprocess.run(f"grep -a -i 'I/O model' {master.d}/server.log | tail -1", shell=True, capture_output=True, text=True).stdout.strip())
    replica = None
    if MODE == "repl":
        replica = Server("replica", RPORT)
        rep = replica.start()
    start = time.time(); end = start + SECS
    bg = background(end)
    q = mp.Queue()
    ws = [mp.Process(target=worker, args=(i, PORT, end, q)) for i in range(WORKERS)]
    for w in ws:
        w.start()
    kills = []
    if MODE == "repl":
        time.sleep(int(os.environ.get("REPLICA_AT", "60")))
        say("REPLICAOF at", round(time.time() - start, 1), "s; master dbsize", m.dbsize())
        rep.execute_command("REPLICAOF", "127.0.0.1", str(PORT))
    else:
        for at in (60, 120, 180):
            if at >= SECS - 20:
                break
            time.sleep(max(0, start + at - time.time()))
            n = m.dbsize(); master.kill9(); kills.append({"at": round(time.time() - start, 1), "dbsize_before": n})
            say("kill -9 at", kills[-1])
            time.sleep(1)
            m = master.start(); kills[-1]["dbsize_after"] = m.dbsize()
            say("restarted", kills[-1])
    sts = {}
    for _ in ws:
        wid, st = q.get(timeout=SECS + 900)
        sts[wid] = st
    for w in ws:
        w.join()
    for p in bg:
        try:
            os.killpg(p.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    subprocess.run("pkill -9 -x redis-benchmark", shell=True)
    tot = {k: sum(s[k] for s in sts.values()) for k in ("ops", "checks", "reply_checks", "errors", "reconnects",
                                                         "post_crash_verified", "post_crash_lost", "final_verified",
                                                         "final_mismatch", "ambiguous", "live_keys_at_end")}
    res["oracle"] = tot
    res["errors"] = [e for s in sts.values() for e in s["err"]][:60]
    res["kills"] = kills
    st = m.info("stats")
    res["master_stats"] = {k: st.get(k) for k in ("expired_keys", "expired_keys_active", "total_commands_processed")}
    say("oracle totals", json.dumps(tot))
    if MODE == "repl":
        ri = rep.info("replication"); res["replica_link"] = ri.get("master_link_status")
        # 1) with TTLs still running: writes paused on the master, offsets equal
        try:
            m.execute_command("CLIENT", "PAUSE", "30000", "WRITE")
            ok, off = wait_synced(m, rep, "paused", timeout=20)
            c1 = compare(m, rep, "compare-paused")
            # SCAN skips keys past their time; a key the master listed and the replica didn't (or the other
            # way round) is only a difference if its time hasn't come by now.
            now_ms = int(time.time() * 1000)
            live_diff = []
            for k in c1.get("_only_m", []) + c1.get("_only_r", []):
                pm = m.pexpiretime(k); pr = rep.pexpiretime(k)
                if not ((0 <= pm < now_ms or pm == -2) and (0 <= pr < now_ms or pr == -2)):
                    live_diff.append((k.decode(), pm, pr))
                    det = {}
                    for nm, cl in (("master", m), ("replica", rep)):
                        for cmd in (["EXISTS", k], ["PTTL", k], ["GET", k], ["DEBUG", "OBJECT", k], ["OBJECT", "ENCODING", k]):
                            try:
                                det[f"{nm} {cmd[0]} {cmd[1] if len(cmd) > 2 else ''}"] = str(cl.execute_command(*cmd))
                            except Exception as e:
                                det[f"{nm} {cmd[0]}"] = "ERR " + str(e)
                    say("live diff detail", k.decode(), json.dumps(det))
            c1["diff_not_explained_by_expiry"] = len(live_diff); c1["live_diff_ex"] = [str(x) for x in live_diff[:5]]
            say("compare-paused live diffs", len(live_diff), live_diff[:5])
            # keys in only one side that have a deadline already past are an expiry still in flight
            c1["synced"] = ok
            c1.pop("_only_m"); c1.pop("_only_r")
            res["compare_paused"] = c1
        finally:
            m.execute_command("CLIENT", "UNPAUSE")
        # 2) quiet: every TTL of up to 60 s is over, the master expired them and the replica applied it
        say("quiet 70 s"); time.sleep(70)
        ok, off = wait_synced(m, rep, "quiet")
        c2 = compare(m, rep, "compare-quiet", values=True)
        c2["synced"] = ok
        c2.pop("_only_m"); c2.pop("_only_r")
        res["compare_quiet"] = c2
        replica.kill9()
    master.kill9()
    json.dump(res, open(os.path.join(OUT, "result.json"), "w"), indent=1)
    say("RESULT", json.dumps(res))
    say("LOAD-DONE")


if __name__ == "__main__":
    main()
