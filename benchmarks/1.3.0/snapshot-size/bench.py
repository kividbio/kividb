#!/usr/bin/env python3
"""#879: snapshot size, save, load, full sync and write latency during a save (runs on a VM).

Server on cores 0-1 (--threads 2), clients/replica on 2-3. One server at a time.

  bench.py seed <dataset>                 load the dataset into a plain master, SAVE, keep dump at ~/seed/<ds>/dump.kdb
  bench.py run <variant> <dataset> [--load] [--sync]   measure a variant on the seeded dataset
  bench.py redis <dataset> [--load] [--sync]           same for Redis 8.10.2 (loads via the generator)

Variants: v122 | master | yes | no | yes-inline (binaries: $V122, $MASTER, $CANDIDATE,
$INLINE, a build with MAX_HELPERS = 0 in src/snapshot/blocks.rs). `yes` runs the
candidate with `rdbcompression yes`; `no` as it starts (the 1.3 default). Redis runs from the redis:8.10.2 image (Docker).
Results are appended as JSON lines to $OUT/results.jsonl (default: this directory).
"""
import json
import os
import shutil
import signal
import statistics
import subprocess
import sys
import time

import redis

NL = chr(10)

HOME = os.path.expanduser("~")
V122 = os.environ.get("V122", HOME + "/v122/kividb/kividb")
HERE = os.path.dirname(os.path.abspath(__file__))
RES = os.path.join(os.environ.get("OUT", HERE), "results.jsonl")
RUN = os.environ.get("RUNDIR", HOME + "/runs")
PORT, RPORT = 7000, 7001
PROBE_DIR = os.path.join(HERE, "..", "regressions", "lib")
sys.path.insert(0, PROBE_DIR)
from probe import Probe  # noqa: E402

VARIANTS = {
    # name: (binary, extra config lines)
    "v122": (V122, ""),
    "master": (os.environ.get("MASTER", HOME + "/t-master/release/kividb"), ""),
    "yes": (os.environ.get("CANDIDATE", HOME + "/t-feat/release/kividb"), "rdbcompression yes" + NL),
    "no": (os.environ.get("CANDIDATE", HOME + "/t-feat/release/kividb"), ""),
    "yes-inline": (os.environ.get("INLINE", HOME + "/t-inline/release/kividb"), "rdbcompression yes" + NL),
}

DATASETS = {
    # name: (generator arg or "memtier", expected keys, memtier key prefix for the write load, data size)
    "issue": ("memtier", 6_000_000, "k:", 150),
    "rand6m": ("rand6m", 6_000_000, "key:", 40),
    "text6m": ("text6m", 6_000_000, "key:", 40),
    "coll": ("coll", 1_600_000, "user:", 0),
    "vec": ("vec", 200_000, "doc:", 0),
}


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


def record(row):
    with open(RES, "a") as f:
        f.write(json.dumps(row) + "\n")
    log("RESULT", json.dumps(row))


def client(port=PORT):
    return redis.Redis(port=port, socket_timeout=600)


def wait_ready(port, want=None, timeout=900):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            c = client(port)
            c.ping()
            n = c.dbsize()
            if want is None or n == want:
                return time.time() - t0
        except (redis.exceptions.ConnectionError, redis.exceptions.BusyLoadingError,
                redis.exceptions.ResponseError, redis.exceptions.TimeoutError):
            pass
        time.sleep(0.02)
    raise RuntimeError("server on %d not ready" % port)


CONF = os.path.join(os.environ.get("OUT", HERE), "kividb.conf")


def start_kividb(binary, env, d, port=PORT, cores="0,1", logname="server.log"):
    os.makedirs(d, exist_ok=True)
    with open(CONF, "w") as f:
        f.write('save ""' + NL + env)
    e = dict(os.environ)
    out = open(os.path.join(d, logname), "a")
    p = subprocess.Popen(["taskset", "-c", cores, binary, "--port", str(port), "--threads", "2",
                          "--dir", d, "--configfile", CONF, "--update-check", "no"],
                         env=e, stdout=out, stderr=subprocess.STDOUT)
    return p


def stop(p, port=PORT):
    try:
        client(port).execute_command("SHUTDOWN", "NOSAVE")
    except Exception:
        pass
    try:
        p.wait(timeout=60)
    except subprocess.TimeoutExpired:
        os.kill(p.pid, signal.SIGKILL)
        p.wait()


def proc_cpu(pid):
    """utime+stime+cutime+cstime in seconds."""
    with open("/proc/%d/stat" % pid) as f:
        parts = f.read().rsplit(")", 1)[1].split()
    t = sum(int(x) for x in parts[11:15])
    return t / os.sysconf("SC_CLK_TCK")


def status(pid, field):
    text = subprocess.run(["sudo", "cat", "/proc/%d/status" % pid], capture_output=True, text=True).stdout
    if True:
        for line in text.splitlines():
            if line.startswith(field + ":"):
                return int(line.split()[1]) * 1024
    return 0


def tree(pid):
    pids = [pid]
    try:
        for t in os.listdir("/proc/%d/task" % pid):
            with open("/proc/%d/task/%s/children" % (pid, t)) as f:
                for c in f.read().split():
                    pids += tree(int(c))
    except FileNotFoundError:
        pass
    return pids


def pss(pid):
    total = 0
    for q in tree(pid):
        try:
            try:
                with open("/proc/%d/smaps_rollup" % q) as f:
                    text = f.read()
            except PermissionError:
                text = subprocess.run(["sudo", "cat", "/proc/%d/smaps_rollup" % q],
                                      capture_output=True, text=True).stdout
            for line in text.splitlines():
                if line.startswith("Pss:"):
                    total += int(line.split()[1]) * 1024
        except (FileNotFoundError, ProcessLookupError):
            pass
    return total


def bgsave_and_wait(c, pid, sample=True):
    """BGSAVE; returns (wall s, cpu s, peak pss delta, peak hwm delta, rss before)."""
    try:
        with open("/proc/%d/clear_refs" % pid, "w") as f:
            f.write("5")
    except OSError:
        pass
    rss0 = status(pid, "VmRSS")
    pss0 = pss(pid)
    cpu0 = proc_cpu(pid)
    t0 = time.perf_counter()
    c.bgsave()
    peak = pss0
    while True:
        time.sleep(0.02)
        if sample:
            peak = max(peak, pss(pid))
        info = c.info("persistence")
        if not info.get("rdb_bgsave_in_progress", 0):
            break
    wall = time.perf_counter() - t0
    time.sleep(0.2)  # a forked child is reaped
    cpu = proc_cpu(pid) - cpu0
    hwm = status(pid, "VmHWM") - rss0
    st = c.info("persistence").get("rdb_last_bgsave_status")
    assert st == "ok", st
    return wall, cpu, peak - pss0, hwm, rss0


def memtier_load(port, ds):
    gen, n, prefix, size = DATASETS[ds]
    subprocess.run(["taskset", "-c", "2,3", "memtier_benchmark", "-s", "127.0.0.1", "-p", str(port),
                    "--ratio", "1:0", "--key-pattern", "P:P", "--key-prefix", prefix,
                    "--key-minimum", "1", "--key-maximum", str(n), "-n", "allkeys", "-c", "1",
                    "-t", "4", "--pipeline", "64", "--data-size", str(size), "--hide-histogram",
                    "--hide-progress" if False else "--hide-histogram"],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def gen_load(port, ds):
    gen = DATASETS[ds][0]
    if gen == "memtier":
        return memtier_load(port, ds)
    g = subprocess.Popen(["python3", os.path.join(HERE, "gen.py"), gen], stdout=subprocess.PIPE)
    r = subprocess.run(["redis-cli", "-p", str(port), "--pipe"], stdin=g.stdout,
                       capture_output=True, text=True)
    g.wait()
    log("pipe:", r.stdout.strip().splitlines()[-1] if r.stdout else r.stderr)
    if ds == "vec":
        client(port).execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA", "v", "VECTOR",
            "HNSW", "6", "TYPE", "FLOAT32", "DIM", "128", "DISTANCE_METRIC", "COSINE",
            "tag", "TAG", "title", "TEXT")
        # wait for the index to be built
        for _ in range(3000):
            inf = client(port).execute_command("FT.INFO", "idx")
            d = inf if isinstance(inf, dict) else dict(zip(inf[::2], inf[1::2]))
            idx = d.get(b"indexing", d.get("indexing", 0))
            if str(idx) in ("0", "b'0'"):
                break
            time.sleep(0.2)


def latency_under_load(c, pid, ds, port=PORT):
    """memtier 50% writes at a fixed rate for 30 s, BGSAVE at t=8 s; PING probe as suite 07."""
    gen, n, prefix, size = DATASETS[ds]
    size = size or 40
    jf = "/tmp/mt-%d.json" % os.getuid()
    if os.path.exists(jf):
        os.remove(jf)
    mt = subprocess.Popen(["taskset", "-c", "2,3", "memtier_benchmark", "-s", "127.0.0.1", "-p",
                           str(port), "--ratio", "1:1", "--key-pattern", "R:R", "--key-prefix",
                           prefix, "--key-minimum", "1", "--key-maximum", str(n), "-c", "25", "-t",
                           "2", "--rate-limiting", "1200", "--test-time", "30", "--data-size",
                           str(size), "--hide-histogram", "--json-out-file", jf,
                           "--print-percentiles", "50,99,99.9"],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    tstart = time.time()
    with Probe(port, n=2) as pr:
        time.sleep(8)
        ts0 = time.time()
        t0 = time.perf_counter()
        wall, cpu, _, _, _ = bgsave_and_wait(c, pid, sample=False)
        t1 = time.perf_counter()
        ts1 = time.time()
        mt.wait()
    worst = pr.worst(t0, t1)
    worst_before = pr.worst(t0 - 7, t0 - 1)
    with open(jf) as f:
        j = json.load(f)
    tot = j["ALL STATS"]["Totals"]
    series = tot.get("Time-Serie", {})
    # seconds since memtier start that overlap the save
    s0, s1 = int(ts0 - tstart), int(ts1 - tstart) + 1
    win = [series[str(s)] for s in range(s0, s1 + 1) if str(s) in series]
    ctl = [series[str(s)] for s in range(2, max(3, s0 - 1)) if str(s) in series]

    def mx(rows, k):
        v = [r.get(k) for r in rows if r.get(k) is not None]
        return max(v) if v else None

    def med(rows, k):
        v = [r.get(k) for r in rows if r.get(k) is not None]
        return statistics.median(v) if v else None
    return {
        "save_s_load": round(wall, 3),
        "p99_save_ms": mx(win, "p99.00"), "p999_save_ms": mx(win, "p99.90"),
        "p99_ctl_ms": med(ctl, "p99.00"), "p999_ctl_ms": med(ctl, "p99.90"),
        "ping_worst_save_ms": round(worst, 2) if worst == worst else None,
        "ping_worst_before_ms": round(worst_before, 2) if worst_before == worst_before else None,
        "ops_s": tot.get("Ops/sec"),
        "all_p99_ms": tot.get("Percentile Latencies", {}).get("p99.00"),
        "all_p999_ms": tot.get("Percentile Latencies", {}).get("p99.90"),
    }


def full_sync(start_replica, stop_replica, n, master_pid):
    try:
        with open("/proc/%d/clear_refs" % master_pid, "w") as f:
            f.write("5")
    except OSError:
        pass
    rss0 = status(master_pid, "VmRSS")
    rp = start_replica()
    wait_ready(RPORT)
    rc = client(RPORT)
    t0 = time.perf_counter()
    rc.execute_command("REPLICAOF", "127.0.0.1", str(PORT))
    while True:
        time.sleep(0.02)
        try:
            inf = rc.info("replication")
            if inf.get("master_link_status") == "up" and not inf.get("master_sync_in_progress", 0) \
                    and rc.dbsize() == n:
                break
        except (redis.exceptions.BusyLoadingError, redis.exceptions.ConnectionError,
                redis.exceptions.TimeoutError):
            pass
        if time.perf_counter() - t0 > 900:
            raise RuntimeError("full sync did not finish")
    t = time.perf_counter() - t0
    hwm = status(master_pid, "VmHWM") - rss0
    stop_replica(rp)
    return round(t, 3), hwm


def measure(kind, variant, ds, start_master, stop_master, start_replica, stop_replica, file_of,
            do_load, do_sync):
    n = DATASETS[ds][1]
    p, pid = start_master()
    wait_ready(PORT, n)
    assert getattr(p, "poll", lambda: None)() is None, "master exited"
    c = client(PORT)
    rss = status(pid, "VmRSS")
    used = c.info("memory").get("used_memory")
    # first save of a fresh process (memory not yet reused), then three measured saves (idle)
    first = bgsave_and_wait(c, pid)
    runs = [bgsave_and_wait(c, pid) for _ in range(3)]
    size = os.path.getsize(file_of())
    row = {"kind": kind, "variant": variant, "ds": ds, "keys": n, "file_bytes": size,
           "rss_bytes": rss, "used_memory": used,
           "save_s": round(statistics.median(r[0] for r in runs), 3),
           "save_s_all": [round(r[0], 3) for r in runs],
           "cpu_s": round(statistics.median(r[1] for r in runs), 2),
           "peak_pss_delta": int(statistics.median(r[2] for r in runs)),
           "peak_hwm_delta": int(statistics.median(r[3] for r in runs)),
           "first_save_s": round(first[0], 3), "first_save_hwm_delta": first[3],
           "first_save_pss_delta": first[2], "rundir": RUN}
    stop_master(p)
    # load time from this variant's own file, before anything changes the dataset
    loads = []
    for _ in range(2):
        t0 = time.perf_counter()
        p, pid = start_master()
        wait_ready(PORT, n)
        loads.append(time.perf_counter() - t0)
        if len(loads) < 2:
            stop_master(p)
    row["load_s"] = round(min(loads), 3)
    row["rss_after_load"] = status(pid, "VmRSS")
    c = client(PORT)
    if do_sync:
        row["sync_s"], row["sync_master_hwm_delta"] = full_sync(start_replica, stop_replica, n, pid)
    if do_load:
        row.update(latency_under_load(c, pid, ds))
    stop_master(p)
    record(row)


def kividb_run(variant, ds, do_load, do_sync):
    binary, env = VARIANTS[variant]
    d = RUN + "/m"
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
    shutil.copy(HOME + "/seed/%s/dump.kdb" % ds, d + "/dump.kdb")
    holder = {}

    def start_master():
        p = start_kividb(binary, env, d)
        holder["p"] = p
        return p, p.pid

    def start_replica():
        rd = RUN + "/r"
        shutil.rmtree(rd, ignore_errors=True)
        return start_kividb(binary, env, rd, port=RPORT, cores="2,3")

    def stop_replica(rp):
        stop(rp, RPORT)

    # the first start loads the seed (KIVIDB01); the first save writes the variant's format
    try:
        measure("kividb", variant, ds, start_master, lambda p: stop(p), start_replica,
                stop_replica, lambda: d + "/dump.kdb", do_load, do_sync)
    finally:
        for q in [holder.get("p")]:
            if q is not None and q.poll() is None:
                os.kill(q.pid, signal.SIGKILL)
    with open(d + "/server.log") as f:
        lines = [l.strip() for l in f if "Snapshot written" in l or "Loaded" in l or "full-sync" in l]
    log("\n".join(lines[-6:]))


def docker_rm(name):
    subprocess.run(["docker", "rm", "-f", name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def redis_start(name, d, port, cores, image="redis:8.10.2", server="redis-server"):
    docker_rm(name)
    os.makedirs(d, exist_ok=True)
    subprocess.run(["docker", "run", "-d", "--name", name, "--network", "host", "--cpuset-cpus",
                    cores, "-v", "%s:/data" % d, image, server, "--port", str(port), "--save", "",
                    "--appendonly", "no", "--dir", "/data", "--repl-diskless-sync-delay", "0",
                    "--enable-debug-command", "local", "--protected-mode", "no"],
                   check=True, stdout=subprocess.DEVNULL)
    pid = int(subprocess.check_output(["docker", "inspect", "-f", "{{.State.Pid}}", name]).strip())
    return name, pid


def redis_run(ds, do_load, do_sync, image="redis:8.10.2", server="redis-server", kind="redis"):
    d = RUN + "/redis"
    subprocess.run(["sudo", "rm", "-rf", d])
    n = DATASETS[ds][1]
    name, pid = redis_start("m879", d, PORT, "0,1", image, server)
    wait_ready(PORT)
    gen_load(PORT, ds)
    wait_ready(PORT, n)
    client(PORT).save()
    stop_container = lambda p: subprocess.run(["docker", "stop", "-t", "30", p],
                                              stdout=subprocess.DEVNULL)
    stop_container(name)
    first = [True]

    def start_master():
        if first[0]:
            first[0] = False
        subprocess.run(["docker", "start", name], check=True, stdout=subprocess.DEVNULL)
        pid = int(subprocess.check_output(["docker", "inspect", "-f", "{{.State.Pid}}", name]).strip())
        return name, pid

    def start_replica():
        subprocess.run(["sudo", "rm", "-rf", RUN + "/redis-r"])
        return redis_start("r879", RUN + "/redis-r", RPORT, "2,3", image, server)[0]

    def stop_replica(rp):
        docker_rm(rp)

    measure(kind, image, ds, start_master, stop_container, start_replica, stop_replica,
            lambda: d + "/dump.rdb", do_load, do_sync)
    if ds in ("issue", "text6m", "coll"):
        name, pid = start_master()
        wait_ready(PORT, n)
        c = client(PORT)
        keys = [k for k in c.scan_iter(count=1000)][:2000]
        sl = []
        for k in keys:
            o = c.execute_command("DEBUG", "OBJECT", k)
            o = o.decode() if isinstance(o, bytes) else o
            for part in o.split():
                if part.startswith("serializedlength:"):
                    sl.append(int(part.split(":")[1]))
        log("redis serializedlength mean %.1f over %d keys, key len mean %.1f" %
            (statistics.mean(sl), len(sl), statistics.mean(len(k) for k in keys)))
        record({"kind": "redis-objlen", "ds": ds, "serializedlength_mean": statistics.mean(sl),
                "keylen_mean": statistics.mean(len(k) for k in keys)})
        stop_container(name)
    docker_rm(name)


def seed(ds):
    d = HOME + "/seed/" + ds
    shutil.rmtree(d, ignore_errors=True)
    p = start_kividb(V122, "", d)
    wait_ready(PORT)
    t0 = time.time()
    gen_load(PORT, ds)
    n = DATASETS[ds][1]
    wait_ready(PORT, n)
    log("seeded %s in %.0f s" % (ds, time.time() - t0))
    client(PORT).save()
    stop(p)
    log("seed file", os.path.getsize(d + "/dump.kdb"))


if __name__ == "__main__":
    cmd = sys.argv[1]
    flags = sys.argv[2:]
    do_load = "--load" in flags
    do_sync = "--sync" in flags
    args = [a for a in flags if not a.startswith("--")]
    if cmd == "seed":
        seed(args[0])
    elif cmd == "run":
        kividb_run(args[0], args[1], do_load, do_sync)
    elif cmd == "redis":
        try:
            redis_run(args[0], do_load, do_sync)
        finally:
            docker_rm("m879"); docker_rm("r879")
    elif cmd == "valkey":
        try:
            redis_run(args[0], do_load, do_sync, image="valkey/valkey:8.1", server="valkey-server",
                      kind="valkey")
        finally:
            docker_rm("m879"); docker_rm("r879")
