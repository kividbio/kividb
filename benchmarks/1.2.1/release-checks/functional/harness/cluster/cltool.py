#!/usr/bin/env python3
"""Cluster topology helper for the cluster soak (runs on the client VM).

  cltool.py shard <slot>               -> "<master host:port> <master id> <replica host:port> <replica id>"
  cltool.py wait-master <host:port>    -> waits until the node is a master that owns slots (prints seconds)
  cltool.py wait-replica <host:port>   -> waits until the node is a replica with master_link_status:up
  cltool.py wait-ok                    -> waits until all nodes say cluster_state:ok, no fail flags anywhere
  cltool.py nodes                      -> CLUSTER NODES of the first node that answers
Nodes: env NODES="h:p h:p ...".
"""
import os
import subprocess
import sys
import time

import redis

NODES = os.environ["NODES"].split()


def conn(hp, t=2):
    h, p = hp.rsplit(":", 1)
    return redis.Redis(host=h, port=int(p), socket_timeout=t, socket_connect_timeout=t, decode_responses=True)


def rcli(hp, *args):
    h, p = hp.rsplit(":", 1)
    r = subprocess.run(["timeout", "3", "redis-cli", "-h", h, "-p", p] + list(args), capture_output=True, text=True)
    if r.returncode != 0 or not r.stdout or r.stdout.startswith(("Could not connect", "ERR", "LOADING")):
        raise RuntimeError(r.stdout + r.stderr)
    return r.stdout


def raw_nodes():
    for hp in NODES:
        try:
            return rcli(hp, "CLUSTER", "NODES")
        except Exception:
            continue
    raise SystemExit("no node answers")


def parse(text):
    out = []
    for line in (text if isinstance(text, str) else text.decode()).splitlines():
        f = line.split()
        if len(f) < 8:
            continue
        addr = f[1].split("@")[0]
        slots = []
        for s in f[8:]:
            if s.startswith("["):
                continue
            a, _, b = s.partition("-")
            slots.append((int(a), int(b or a)))
        out.append({"id": f[0], "addr": addr, "flags": f[2], "master": f[3], "slots": slots, "link": f[7]})
    return out


def shard(slot):
    ns = parse(raw_nodes())
    for n in ns:
        if "master" in n["flags"] and "fail" not in n["flags"] and any(a <= slot <= b for a, b in n["slots"]):
            reps = [r for r in ns if r["master"] == n["id"] and "fail" not in r["flags"]]
            r = reps[0] if reps else {"addr": "-", "id": "-"}
            return "%s %s %s %s" % (n["addr"], n["id"], r["addr"], r["id"])
    raise SystemExit("no master for slot %d" % slot)


def info(hp, section):
    return conn(hp).info(section)


def main():
    cmd = sys.argv[1]
    t0 = time.time()
    if cmd == "shard":
        print(shard(int(sys.argv[2])))
    elif cmd == "nodes":
        print(raw_nodes())
    elif cmd == "wait-master":
        hp = sys.argv[2]
        while time.time() - t0 < 300:
            try:
                if info(hp, "replication").get("role") == "master":
                    me = [n for n in parse(rcli(hp, "CLUSTER", "NODES")) if "myself" in n["flags"]]
                    if me and me[0]["slots"]:
                        print("%.2f" % (time.time() - t0))
                        return
            except Exception:
                pass
            time.sleep(0.1)
        print("timeout")
    elif cmd == "wait-replica":
        hp = sys.argv[2]
        while time.time() - t0 < 600:
            try:
                i = info(hp, "replication")
                if i.get("role") == "slave" and i.get("master_link_status") == "up":
                    print("%.2f" % (time.time() - t0))
                    return
            except Exception:
                pass
            time.sleep(0.2)
        print("timeout")
    elif cmd == "wait-ok":
        while time.time() - t0 < 600:
            good = 0
            for hp in NODES:
                try:
                    st = rcli(hp, "CLUSTER", "INFO")
                    if "cluster_state:ok" in st and "fail" not in rcli(hp, "CLUSTER", "NODES"):
                        good += 1
                except Exception:
                    pass
            if good == len(NODES):
                print("%.2f" % (time.time() - t0))
                return
            time.sleep(0.5)
        print("timeout")


main()
