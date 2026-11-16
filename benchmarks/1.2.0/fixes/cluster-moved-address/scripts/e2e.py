#!/usr/bin/env python3
"""Moved-address end-to-end check for KiviDB cluster (run as root on the VM).

Six nodes, each in its own network namespace (a "pod") on a bridge, bound to
0.0.0.0 like the operator's pods (no cluster-announce-ip: peers learn each
other's address from the connection). A move = stop the node (SIGTERM), delete
its namespace (the old IP disappears), create a new namespace with a new IP,
restart the node with the same data dir / nodes.conf.

usage: e2e.py BINARY LABEL SCENARIO REP OUTDIR
scenarios: two-masters, master-plus-other-replica, one-master, rapid-move
"""
import csv
import json
import os
import random
import signal
import subprocess
import sys
import threading
import time

import redis
from redis.backoff import NoBackoff
from redis.crc import key_slot
from redis.exceptions import AskError, ClusterDownError, MovedError
from redis.retry import Retry

BIN, LABEL, SCEN, REP, OUT = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
NODE_TIMEOUT = int(os.environ.get("NODE_TIMEOUT", "5000"))
DOWN = float(os.environ.get("DOWN", "10"))  # seconds from stop to restart (pod reschedule)
CAP = float(os.environ.get("CAP", "180"))  # give up this long after the (last) restart
STABLE = float(os.environ.get("STABLE", "15"))  # converged must hold this long
PORT = 7000
BR = "clmbr"
ROOT = "/clm"
NAMES = [f"n{i}" for i in range(1, 7)]  # n1-n3 masters, n4->n1, n5->n2, n6->n3 replicas
os.makedirs(OUT, exist_ok=True)

ip_of = {f"n{i}": f"10.99.0.{10 + i}" for i in range(1, 7)}
procs = {}
lock = threading.Lock()
events = []  # (t, text)
T0 = time.time()


def now():
    return time.time() - T0


def ev(text):
    events.append((round(now(), 3), text))
    print(f"[{now():8.2f}] {text}", flush=True)


def sh(cmd, check=True):
    return subprocess.run(cmd, shell=True, check=check, capture_output=True, text=True)


def ns_create(name, ip):
    n = int(name[1:])
    sh(f"ip netns add {name}")
    for _ in range(100):  # the old namespace's veth goes away asynchronously
        if sh(f"ip link add vh{n} type veth peer name vn{n}", check=False).returncode == 0:
            break
        time.sleep(0.05)
    else:
        raise RuntimeError(f"veth vh{n} still exists")
    sh(f"ip link set vn{n} netns {name}")
    sh(f"ip link set vh{n} master {BR} up")
    sh(f"ip -n {name} link set lo up")
    sh(f"ip -n {name} link set vn{n} up")
    sh(f"ip -n {name} addr add {ip}/24 dev vn{n}")
    sh(f"ip -n {name} route add default via 10.99.0.1")


def ns_delete(name):
    # The namespace itself can outlive `ip netns del` (lingering sockets), with
    # its veth: delete the pair explicitly so the old IP really disappears.
    sh(f"ip link del vh{name[1:]}", check=False)
    sh(f"ip netns del {name}", check=False)


def cleanup():
    sh("pkill -9 -x kividb", check=False)
    time.sleep(0.5)
    for name in NAMES:
        ns_delete(name)
    sh(f"ip link del {BR}", check=False)
    sh(f"rm -rf {ROOT}")


def start(name):
    d = f"{ROOT}/{name}"
    os.makedirs(d, exist_ok=True)
    with open(f"{d}/kividb.conf", "w") as f:
        f.write(
            f"port {PORT}\nbind 0.0.0.0\ndir {d}\ncluster-enabled yes\n"
            f"cluster-config-file nodes.conf\ncluster-node-timeout {NODE_TIMEOUT}\n"
            "cluster-require-full-coverage yes\n"
        )
    log = open(f"{OUT}/server-{name}.log", "a")
    log.write(f"\n===== start at {now():.2f}s on {ip_of[name]}\n")
    log.flush()
    p = subprocess.Popen(
        ["ip", "netns", "exec", name, "taskset", "-c", "0-2", BIN, "--configfile", f"{d}/kividb.conf"],
        cwd=d, stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
    )
    procs[name] = p


def stop(names):
    for n in names:
        procs[n].send_signal(signal.SIGTERM)
    deadline = time.time() + 5
    for n in names:
        try:
            procs[n].wait(max(0.1, deadline - time.time()))
        except subprocess.TimeoutExpired:
            procs[n].kill()
            procs[n].wait()
            ev(f"{n} needed SIGKILL")


def conn(name, timeout=0.3):
    return redis.Redis(host=ip_of[name], port=PORT, socket_timeout=timeout,
                       socket_connect_timeout=timeout, retry=Retry(NoBackoff(), 0),
                       decode_responses=True)


def wait_up(name, t=20):
    end = time.time() + t
    while time.time() < end:
        try:
            if conn(name).ping():
                return
        except Exception:
            time.sleep(0.1)
    raise RuntimeError(f"{name} did not come up")


def parse_nodes(text):
    rows = {}
    for line in text.strip().splitlines():
        f = line.split()
        addr = f[1].split("@")[0]
        ip = addr.rsplit(":", 1)[0]
        rows[f[0]] = {"ip": ip, "flags": f[2], "link": f[7]}
    return rows


def form():
    for n in NAMES:
        wait_up(n)
    c1 = conn("n1", 2)
    for n in NAMES[1:]:
        c1.execute_command("CLUSTER", "MEET", ip_of[n], PORT)
    ids = {n: conn(n, 2).execute_command("CLUSTER", "MYID") for n in NAMES}
    end = time.time() + 60
    while time.time() < end:
        ok = True
        for n in NAMES:
            rows = parse_nodes(conn(n, 2).execute_command("CLUSTER", "NODES"))
            if len(rows) != 6 or any("handshake" in r["flags"] or "noaddr" in r["flags"] for r in rows.values()):
                ok = False
        if ok:
            break
        time.sleep(0.3)
    else:
        raise RuntimeError("cluster did not meet")
    ranges = [(0, 5460), (5461, 10922), (10923, 16383)]
    for n, (a, b) in zip(NAMES[:3], ranges):
        conn(n, 2).execute_command("CLUSTER", "ADDSLOTSRANGE", a, b)
    time.sleep(2)
    for r, m in zip(NAMES[3:], NAMES[:3]):
        conn(r, 2).execute_command("CLUSTER", "REPLICATE", ids[m])
    end = time.time() + 90
    while time.time() < end:
        if all(conn(n, 2).execute_command("CLUSTER", "INFO").find("cluster_state:ok") >= 0 for n in NAMES):
            if all("master_link_status:up" in str(conn(n, 2).execute_command("INFO", "replication")) or True for n in NAMES[3:]):
                break
        time.sleep(0.3)
    else:
        raise RuntimeError("cluster not ok")
    return ids


# ---------------------------------------------------------------- monitor
mon_rows = []
stop_flag = threading.Event()


def monitor(ids):
    name_of = {v: k for k, v in ids.items()}
    while not stop_flag.is_set():
        t = now()
        sample = {}
        for o in NAMES:
            try:
                c = conn(o, 0.3)
                info = c.execute_command("CLUSTER", "INFO")
                st = "ok" if "cluster_state:ok" in info else "fail"
                rows = parse_nodes(c.execute_command("CLUSTER", "NODES"))
                view = {name_of.get(i, i[:8]): r for i, r in rows.items()}
                sample[o] = (st, view)
            except Exception:
                sample[o] = None
        with lock:
            cur = dict(ip_of)
        mon_rows.append((t, cur, sample))
        time.sleep(max(0, 0.5 - (now() - t)))


def judge(cur, sample, subjects):
    """(all six answer and see every subject at its current address and healthy, all cluster_state ok)"""
    ok_view, ok_state = True, True
    for o in NAMES:
        s = sample.get(o)
        if s is None:
            return False, False
        st, view = s
        if st != "ok":
            ok_state = False
        for n in subjects:
            r = view.get(n)
            if (r is None or r["ip"] != cur[n] or r["link"] != "connected"
                    or any(x in r["flags"].split(",") for x in ("fail", "fail?", "handshake", "noaddr"))):
                ok_view = False
    return ok_view, ok_state


# ---------------------------------------------------------------- writer
buckets = {}


def writer():
    slots = {}
    clients = {}
    last_refresh = 0
    i = 0

    def client(ip):
        if ip not in clients:
            clients[ip] = redis.Redis(host=ip, port=PORT, socket_timeout=0.25, socket_connect_timeout=0.25,
                                      retry=Retry(NoBackoff(), 0), decode_responses=True)
        return clients[ip]

    def refresh():
        with lock:
            ips = list(ip_of.values())
        random.shuffle(ips)
        for ip in ips:
            try:
                m = {}
                for a, b, master, *_ in client(ip).execute_command("CLUSTER", "SLOTS"):
                    mip = master[0] or ip
                    for s in range(a, b + 1):
                        m[s] = mip
                slots.clear()
                slots.update(m)
                return
            except Exception:
                continue

    while not stop_flag.is_set():
        t0 = time.time()
        if t0 - last_refresh > 1.0 or not slots:
            refresh()
            last_refresh = time.time()
        sec = int(now())
        b = buckets.setdefault(sec, {"attempts": 0, "ok": 0, "failed": 0, "clusterdown": 0, "conn": 0, "other": 0, "moved": 0})
        key = f"k:{i}"
        i += 1
        b["attempts"] += 1
        target = slots.get(key_slot(key.encode()))
        try:
            if target is None:
                raise ConnectionError("no slot map")
            try:
                client(target).set(key, "v")
            except (MovedError, AskError) as e:
                # redis-py parses "MOVED slot host:port" into MovedError(host, port)
                b["moved"] += 1
                last_refresh = 0
                client(e.host).set(key, "v")
            b["ok"] += 1
        except redis.ResponseError as e:
            b["failed"] += 1
            # redis-py parses "CLUSTERDOWN ..." into ClusterDownError("The cluster is down")
            if isinstance(e, ClusterDownError) or "CLUSTERDOWN" in str(e):
                b["clusterdown"] += 1
                b.setdefault("cd_from", set()).add(target)
            else:
                b["other"] += 1
                b.setdefault("errs", set()).add(f"{type(e).__name__}: {str(e)[:60]}")
        except Exception:
            b["failed"] += 1
            b["conn"] += 1
            last_refresh = 0
        dt = time.time() - t0
        if dt < 0.005:
            time.sleep(0.005 - dt)


# ---------------------------------------------------------------- scenario
def move(names, new_ips, down):
    ev(f"stop {names}")
    t_stop = now()
    stop(names)
    for n, ip in zip(names, new_ips):
        ns_delete(n)
        with lock:
            ip_of[n] = ip
        ns_create(n, ip)
    ev(f"re-IP {dict(zip(names, new_ips))}")
    while now() < t_stop + down:
        time.sleep(0.05)
    for n in names:
        start(n)
    ev(f"restarted {names}")
    return t_stop, now()


def main():
    cleanup()
    sh(f"ip link add {BR} type bridge")
    sh(f"ip addr add 10.99.0.1/24 dev {BR}")
    sh(f"ip link set {BR} up")
    for n in NAMES:
        ns_create(n, ip_of[n])
        start(n)
    ids = form()
    ev(f"cluster formed {ids}")
    time.sleep(10)
    th_m = threading.Thread(target=monitor, args=(ids,), daemon=True)
    th_w = threading.Thread(target=writer, daemon=True)
    th_m.start()
    th_w.start()
    time.sleep(10)
    marks = []
    if SCEN == "two-masters":
        moving = ["n1", "n2"]
        marks.append(move(moving, ["10.99.0.21", "10.99.0.22"], DOWN))
    elif SCEN == "master-plus-other-replica":
        moving = ["n1", "n5"]  # n5 replicates n2: a different shard
        marks.append(move(moving, ["10.99.0.21", "10.99.0.25"], DOWN))
    elif SCEN == "one-master":
        moving = ["n1"]
        marks.append(move(moving, ["10.99.0.21"], DOWN))
    elif SCEN == "rapid-move":
        moving = ["n1"]
        marks.append(move(moving, ["10.99.0.21"], DOWN))
        time.sleep(3)
        marks.append(move(moving, ["10.99.0.31"], 2))
    elif SCEN == "rapid-move-two-masters":
        moving = ["n1", "n2"]
        marks.append(move(moving, ["10.99.0.21", "10.99.0.22"], DOWN))
        time.sleep(3)
        marks.append(move(moving, ["10.99.0.31", "10.99.0.32"], 2))
    else:
        raise SystemExit("unknown scenario")
    t_restart = marks[-1][1]
    t_kill = marks[0][0]
    # Wait for convergence held STABLE seconds, or CAP.
    good_since = None
    while now() < t_restart + CAP:
        time.sleep(0.5)
        if not mon_rows:
            continue
        t, cur, sample = mon_rows[-1]
        if t < t_restart:
            continue
        v, s = judge(cur, sample, NAMES)
        if v and s:
            good_since = good_since if good_since is not None else t
            if t - good_since >= STABLE:
                break
        else:
            good_since = None
    stop_flag.set()
    th_m.join(2)
    th_w.join(2)
    finals = {}
    for o in NAMES:
        try:
            finals[o] = conn(o, 1).execute_command("CLUSTER", "NODES")
        except Exception as e:
            finals[o] = f"ERR {e}"
    with open(f"{OUT}/final-cluster-nodes.txt", "w") as f:
        for o in NAMES:
            f.write(f"===== {o} ({ip_of[o]})\n{finals[o]}\n")
    stop(NAMES)

    # ------------------------------------------------------------ outputs
    with open(f"{OUT}/monitor.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["t", "observer", "cluster_state"] + [f"{n}_view" for n in NAMES])
        for t, cur, sample in mon_rows:
            for o in NAMES:
                s = sample.get(o)
                if s is None:
                    w.writerow([f"{t:.2f}", o, "unreachable"] + [""] * 6)
                else:
                    st, view = s
                    w.writerow([f"{t:.2f}", o, st] + [
                        (lambda r: f"{r['ip']}|{r['flags']}|{r['link']}")(view[n]) if n in view else "absent"
                        for n in NAMES])
    with open(f"{OUT}/writes.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["t_rel_kill", "attempts", "ok", "failed", "clusterdown", "conn_or_timeout", "other", "moved_followed"])
        for sec in sorted(buckets):
            b = buckets[sec]
            w.writerow([sec - int(t_kill), b["attempts"], b["ok"], b["failed"], b["clusterdown"], b["conn"], b["other"], b["moved"]])

    def first_stable(pred):
        """Time (rel. last restart) from which pred held for every later sample, None if not at the end."""
        since = None
        for t, cur, sample in mon_rows:
            if t < t_restart:
                continue
            if pred(cur, sample):
                since = since if since is not None else t
            else:
                since = None
        return None if since is None else round(since - t_restart, 2)

    def flaps(pred):
        prev, n = None, 0
        for t, cur, sample in mon_rows:
            if t < t_restart:
                continue
            p = pred(cur, sample)
            if prev is True and p is False:
                n += 1
            prev = p
        return n

    views_ok = lambda c, s: judge(c, s, moving)[0]
    all_ok = lambda c, s: all(judge(c, s, NAMES))
    state_ok = lambda c, s: judge(c, s, [])[1]
    per_obs = {}
    for o in NAMES:
        since = None
        for t, cur, sample in mon_rows:
            if t < t_restart:
                continue
            s = sample.get(o)
            good = s is not None and all(
                n in s[1] and s[1][n]["ip"] == cur[n] and s[1][n]["link"] == "connected"
                and not set(s[1][n]["flags"].split(",")) & {"fail", "fail?", "handshake", "noaddr"}
                for n in moving)
            since = (since if since is not None else t) if good else None
        per_obs[o] = None if since is None else round(since - t_restart, 2)
    # Addresses each observer showed for the moved nodes after the first restart (stale-flap check).
    addr_seq = {}
    for t, cur, sample in mon_rows:
        if t < marks[0][1]:
            continue
        for o in NAMES:
            s = sample.get(o)
            if s is None:
                continue
            for n in moving:
                ip = s[1].get(n, {}).get("ip")
                seq = addr_seq.setdefault(f"{o}->{n}", [])
                if ip and (not seq or seq[-1] != ip):
                    seq.append(ip)
    after = {s: b for s, b in buckets.items() if s >= int(t_kill)}
    fail_secs = [s for s, b in after.items() if b["failed"]]
    cd_secs = [s for s, b in after.items() if b["clusterdown"]]
    rel = lambda s: s - int(t_kill)
    summary = {
        "label": LABEL, "scenario": SCEN, "rep": REP, "node_timeout_ms": NODE_TIMEOUT, "down_s": DOWN,
        "moving": moving, "new_ips": {n: ip_of[n] for n in moving},
        "restart_after_kill_s": round(t_restart - t_kill, 2),
        "moved_views_converged_s": first_stable(views_ok),
        "all_views_and_state_ok_s": first_stable(all_ok),
        "cluster_state_ok_everywhere_s": first_stable(state_ok),
        "flaps_after_restart": flaps(all_ok),
        "per_observer_moved_view_ok_s": per_obs,
        "observed_after_restart_s": round(mon_rows[-1][0] - t_restart, 2) if mon_rows else None,
        "writes_failed_after_kill": sum(b["failed"] for b in after.values()),
        "writes_clusterdown_after_kill": sum(b["clusterdown"] for b in after.values()),
        "writes_attempted_after_kill": sum(b["attempts"] for b in after.values()),
        "last_failed_write_s_after_kill": rel(max(fail_secs)) if fail_secs else None,
        "last_clusterdown_s_after_kill": rel(max(cd_secs)) if cd_secs else None,
        "failed_seconds_after_restart": len([s for s in fail_secs if s >= int(t_restart)]),
        "clusterdown_seconds": len(cd_secs),
        "clusterdown_from": sorted({x for b in after.values() for x in b.get("cd_from", ())}),
        "other_errors": sorted({e for b in after.values() for e in b.get("errs", ())}),
        "address_sequences": {k: v for k, v in addr_seq.items() if len(v) > 1 or k.split("->")[0] not in moving},
        "events": events,
    }
    with open(f"{OUT}/summary.json", "w") as f:
        json.dump(summary, f, indent=1)
    print(json.dumps({k: summary[k] for k in list(summary)[:20]}, indent=1))
    cleanup()


if __name__ == "__main__":
    try:
        main()
    finally:
        sh("pkill -9 -x kividb", check=False)
