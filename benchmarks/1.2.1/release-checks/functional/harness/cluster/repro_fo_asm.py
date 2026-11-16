#!/usr/bin/env python3
"""repro_fo_asm.py BIN [--runs N] [--delay S] [--fill N]: CLUSTER FAILOVER on the source's replica while a
CLUSTER MIGRATION IMPORT from that source runs -> acknowledged writes to the moving slots are lost.

One host, six local nodes (ports 21001-21006), redis-cli --cluster create --cluster-replicas 1, --fill keys (default
200k, 200 B) so the import takes a few hundred ms. S = owner of slot 0, SR = its replica, D = owner of slot 8000.
4 writer threads RPUSH a counter to 200 lists in slots 0-1199 (redis-py RedisCluster, follows MOVED/ASK) and log
every acknowledged element. On D: CLUSTER MIGRATION IMPORT 0 1199; --delay s later: CLUSTER FAILOVER on SR.
5 s after both are done the writers stop and every list is read back: acknowledged elements missing = lost.
"""
import json
import os
import shutil
import subprocess
import sys
import threading
import time

import redis
from redis.cluster import RedisCluster, ClusterNode

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from clw import key_slot  # noqa: E402

BIN = sys.argv[1]
arg = lambda n, d: type(d)(sys.argv[sys.argv.index(n) + 1]) if n in sys.argv else d
RUNS, DELAY, FILL = arg("--runs", 1), arg("--delay", 0.0), arg("--fill", 200000)
PORTS = list(range(21001, 21007))
BASE = "/tmp/foasm"


def c(p, t=10):
    return redis.Redis(port=p, socket_timeout=t)


def nodes():
    out = {}
    for line in c(PORTS[0]).execute_command("CLUSTER", "NODES").decode().splitlines():
        f = line.split()
        out[f[0]] = {"port": int(f[1].split("@")[0].split(":")[1]), "flags": f[2], "master": f[3], "slots": f[8:]}
    return out


def owner(slot):
    for nid, n in nodes().items():
        if "master" in n["flags"]:
            for s in n["slots"]:
                if s.startswith("["):
                    continue
                a, _, b = s.partition("-")
                if int(a) <= slot <= int(b or a):
                    return nid, n["port"]


def start():
    subprocess.run(["pkill", "-9", "-f", "foasm/2100"], check=False)
    time.sleep(0.5)
    shutil.rmtree(BASE, ignore_errors=True)
    procs = {}
    for p in PORTS:
        d = "%s/%d" % (BASE, p)
        os.makedirs(d)
        open(d + "/node.conf", "w").write(
            "port %d\nbind 127.0.0.1\ndir %s\nappendonly yes\nappendfsync everysec\ncluster-enabled yes\n"
            "cluster-node-timeout 2000\ncluster-config-file nodes.conf\nupdate-check no\n" % (p, d))
        procs[p] = subprocess.Popen([BIN, "--configfile", d + "/node.conf"], cwd=d, stdout=open(d + "/server.log", "w"),
                                    stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
    for p in PORTS:
        for _ in range(100):
            try:
                c(p).ping()
                break
            except Exception:
                time.sleep(0.1)
    subprocess.run("yes yes | redis-cli --cluster create %s --cluster-replicas 1 --cluster-yes" %
                   " ".join("127.0.0.1:%d" % p for p in PORTS), shell=True, capture_output=True)
    for _ in range(300):
        try:
            ok = all(b"cluster_state:ok" in c(p).execute_command("CLUSTER", "INFO") for p in PORTS)
            ns = nodes()
            reps = [n for n in ns.values() if "slave" in n["flags"] and n["master"] in ns]
            if ok and len(reps) == 3 and all(c(n["port"]).info("replication").get("master_link_status") == "up"
                                             for n in reps):
                break
        except Exception:
            pass
        time.sleep(0.1)
    return procs


def writer(wid, keys, stop, acked):
    rc = RedisCluster(startup_nodes=[ClusterNode("127.0.0.1", PORTS[0])], socket_timeout=2,
                      cluster_error_retry_attempts=0)
    n = 0
    while not stop.is_set():
        k = keys[n % len(keys)]
        n += 1
        v = b"%d:%d" % (wid, n)
        try:
            rc.rpush(k, v)
            acked.append((k, v, time.time()))
        except Exception:
            time.sleep(0.005)
            try:
                rc = RedisCluster(startup_nodes=[ClusterNode("127.0.0.1", PORTS[0])], socket_timeout=2,
                                  cluster_error_retry_attempts=0)
            except Exception:
                pass


def one(run):
    procs = start()
    rc = RedisCluster(startup_nodes=[ClusterNode("127.0.0.1", PORTS[0])])
    val = b"x" * 200
    for i in range(0, FILL, 2000):
        p = rc.pipeline()
        for j in range(i, i + 2000):
            p.set(b"f:%d" % j, val)
        p.execute()
    keys, i = [], 0
    while len(keys) < 200:
        k = b"l:%d" % i
        i += 1
        if key_slot(k) < 1200:
            keys.append(k)
    sid, sp = owner(0)
    did, dp = owner(8000)
    srp = [n["port"] for n in nodes().values() if n["master"] == sid][0]
    stop = threading.Event()
    acked = [[] for _ in range(4)]
    ths = [threading.Thread(target=writer, args=(w, keys, stop, acked[w])) for w in range(4)]
    for t in ths:
        t.start()
    time.sleep(2)
    t_imp = time.time()
    tid = c(dp).execute_command("CLUSTER", "MIGRATION", "IMPORT", 0, 1199)
    time.sleep(DELAY)
    t_fo = time.time()
    fo = c(srp).execute_command("CLUSTER", "FAILOVER")
    st = {}
    for _ in range(3000):
        rows = c(dp).execute_command("CLUSTER", "MIGRATION", "STATUS", "ID", tid)
        if rows:
            st = {k.decode(): (v.decode() if isinstance(v, bytes) else v) for k, v in zip(rows[0][0::2], rows[0][1::2])}
            if st.get("state") in ("completed", "failed", "canceled"):
                break
        time.sleep(0.01)
    time.sleep(5)
    stop.set()
    for t in ths:
        t.join()
    time.sleep(1)
    rc = RedisCluster(startup_nodes=[ClusterNode("127.0.0.1", PORTS[0])])
    have = {k: set(rc.lrange(k, 0, -1)) for k in keys}
    lost = [(k, v, t) for a in acked for (k, v, t) in a if v not in have[k]]
    res = {"run": run, "delay_s": DELAY, "import_state": st.get("state"), "import_last_error": st.get("last_error"),
           "import_ms": (st.get("end_time", 0) - st.get("start_time", 0)) if st.get("end_time") else None,
           "failover_reply": fo.decode() if isinstance(fo, bytes) else fo,
           "acked": sum(len(a) for a in acked), "lost": len(lost),
           "lost_from_s": round(min(t for _, _, t in lost) - t_imp, 3) if lost else None,
           "lost_to_s": round(max(t for _, _, t in lost) - t_imp, 3) if lost else None,
           "failover_at_s": round(t_fo - t_imp, 3), "slot0_owner_after": owner(0)[1], "source": sp, "replica": srp,
           "dest": dp, "version": c(dp).info("server").get("redis_version")}
    print(json.dumps(res), flush=True)
    for pr in procs.values():
        pr.kill()


for r in range(RUNS):
    one(r)
