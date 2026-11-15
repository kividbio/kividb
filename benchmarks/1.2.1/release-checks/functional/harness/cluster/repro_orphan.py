#!/usr/bin/env python3
"""repro_orphan.py BIN [--kill|--no-kill] [--runs N]: a master killed right after an atomic slot migration (CLUSTER
MIGRATION IMPORT) handed its slots over -> its replica is promoted and keeps the keys of the slots that moved away.

One host, six local nodes (ports 21001-21006), redis-cli --cluster create --cluster-replicas 1, 60k keys.
S = owner of slot 0, SR = its replica, D = owner of slot 8000. On D: CLUSTER MIGRATION IMPORT 0 2999. The moment D's
task says completed, SIGKILL S. Wait for SR to be promoted, then count SR's keys in slots 0-2999 (it owns none of them).
"""
import json
import os
import shutil
import signal
import subprocess
import sys
import time

import redis
from redis.cluster import RedisCluster, ClusterNode

BIN = sys.argv[1]
KILL = "--no-kill" not in sys.argv
RUNS = int(sys.argv[sys.argv.index("--runs") + 1]) if "--runs" in sys.argv else 1
DELAY = float(sys.argv[sys.argv.index("--delay") + 1]) if "--delay" in sys.argv else 0.0
PORTS = list(range(21001, 21007))
BASE = "/tmp/orph"


def c(p, t=10):
    return redis.Redis(port=p, socket_timeout=t)


def start():
    subprocess.run(["pkill", "-9", "-f", "orph/2100"], check=False)
    time.sleep(0.5)
    shutil.rmtree(BASE, ignore_errors=True)
    pids = {}
    for p in PORTS:
        d = "%s/%d" % (BASE, p)
        os.makedirs(d)
        open(d + "/node.conf", "w").write(
            "port %d\nbind 127.0.0.1\ndir %s\nappendonly yes\nappendfsync everysec\ncluster-enabled yes\n"
            "cluster-node-timeout 2000\ncluster-config-file nodes.conf\nupdate-check no\n" % (p, d))
        pr = subprocess.Popen([BIN, "--configfile", d + "/node.conf"], cwd=d, stdout=open(d + "/server.log", "w"),
                              stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
        pids[p] = pr
    for p in PORTS:
        for _ in range(100):
            try:
                c(p).ping()
                break
            except Exception:
                time.sleep(0.1)
    r = subprocess.run("yes yes | redis-cli --cluster create %s --cluster-replicas 1 --cluster-yes" %
                       " ".join("127.0.0.1:%d" % p for p in PORTS), shell=True, capture_output=True, text=True)
    for _ in range(200):
        if all(b"cluster_state:ok" in c(p).execute_command("CLUSTER", "INFO") for p in PORTS):
            break
        time.sleep(0.1)
    for _ in range(300):
        ns = nodes()
        reps = [n for n in ns.values() if "slave" in n["flags"] and n["master"] in ns]
        if len(reps) == 3 and all(c(n["port"]).info("replication").get("master_link_status") == "up" for n in reps):
            break
        time.sleep(0.1)
    return pids


def nodes():
    out = {}
    for line in c(PORTS[0]).execute_command("CLUSTER", "NODES").decode().splitlines():
        f = line.split()
        out[f[0]] = {"port": int(f[1].split("@")[0].split(":")[1]), "flags": f[2], "master": f[3], "slots": f[8:]}
    return out


def owner_port(slot):
    for nid, n in nodes().items():
        if "master" in n["flags"]:
            for s in n["slots"]:
                if s.startswith("["):
                    continue
                a, _, b = s.partition("-")
                if int(a) <= slot <= int(b or a):
                    return nid, n["port"]


def one(run):
    pids = start()
    rc = RedisCluster(startup_nodes=[ClusterNode("127.0.0.1", PORTS[0])])
    for i in range(0, 60000, 1000):
        p = rc.pipeline()
        for j in range(i, i + 1000):
            p.set(b"k:%d" % j, b"v" * 50)
        p.execute()
    sid, sp = owner_port(0)
    did, dp = owner_port(8000)
    srp = [n["port"] for n in nodes().values() if n["master"] == sid][0]
    cnt = lambda port: sum(int(x) for x in _counts(port))
    before = cnt(sp)
    tid = c(dp).execute_command("CLUSTER", "MIGRATION", "IMPORT", 0, 2999)
    t0 = time.time()
    state = None
    while time.time() - t0 < 30:
        rows = c(dp).execute_command("CLUSTER", "MIGRATION", "STATUS", "ID", tid)
        if rows:
            st = dict(zip(rows[0][0::2], rows[0][1::2]))
            state = st.get(b"state")
            if state in (b"completed", b"failed", b"canceled"):
                break
        time.sleep(0.001)
    time.sleep(DELAY)
    tk = time.time()
    if KILL:
        pids[sp].send_signal(signal.SIGKILL)
    for _ in range(300):
        try:
            if c(srp).info("replication")["role"] == "master":
                break
        except Exception:
            pass
        time.sleep(0.1)
    promoted = c(srp).info("replication")["role"] == "master"
    time.sleep(5)
    res = {"run": run, "kill": KILL, "delay_s": DELAY, "task_state": state.decode() if state else None,
           "s_keys_in_0_2999_before": before, "kill_after_completed_ms": round((tk - t0) * 1000),
           "replica_promoted": promoted, "promoted_replica_keys_in_0_2999_after_5s": cnt(srp),
           "dst_keys_in_0_2999": cnt(dp)}
    if not KILL:
        res["source_keys_in_0_2999_after_5s"] = cnt(sp)
    res["version"] = c(dp).info("server").get("kividb_version") or c(dp).info("server").get("redis_version")
    print(json.dumps(res), flush=True)
    for pr in pids.values():
        pr.kill()


def _counts(port):
    p = c(port, 60).pipeline(transaction=False)
    for s in range(0, 3000):
        p.execute_command("CLUSTER", "COUNTKEYSINSLOT", s)
    return p.execute()


for r in range(RUNS):
    one(r)
