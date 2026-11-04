#!/usr/bin/env python3
"""
n8n queue mode, end to end, on a Redis-protocol server (KiviDB or Redis 8).

  pip install -r requirements.txt
  python n8n_queue_e2e.py --server kividb     # kividb/kividb:v1.2.0-lua, --aof
  python n8n_queue_e2e.py --server redis8     # redis:8, --appendonly yes

Needs Docker on a Linux machine. It starts, on a private Docker network: Postgres (n8n's own
database), the server, the sidecar HTTP service (sidecar.py) and n8n 2.46.6 as one main instance
and two workers with EXECUTIONS_MODE=queue. It imports and publishes workflow.json:

  Webhook (POST, responds at once) -> Work (HTTP Request to the sidecar, retry on fail, 3 tries)
                                   -> Done (HTTP Request to the sidecar)

and then runs batches of 200 executions, one per webhook call. In each batch the sidecar fails ids
ending in 0 every time (the execution must end in error after 3 tries) and ids ending in 5 on the
first try only (n8n's retry must make them succeed). Every other id succeeds at once. Each try takes
WORK_MS (2 s), so a batch keeps the 2 x 10 worker slots busy for about 25 s, with jobs waiting in the
queue behind them.

  batch 1  no disruption
  batch 2  worker 1 restarted (docker restart, SIGTERM: n8n's graceful shutdown) 6 s in
  batch 3  the server restarted (docker restart) 6 s in, while jobs wait in the queue
  batch 4  worker 2 killed (SIGKILL) 6 s in, then started again

After each batch the harness waits for the queue to drain and checks, from n8n's Postgres and from
what reached the sidecar, that every webhook made exactly one execution, that each execution ended
as planned (success, or error for the always-failing ids), that no execution ran twice, and that the
queue is empty. Batch 4 is the exception: n8n gives up on a job whose worker was killed (Bull's
maxStalledCount is 0 in n8n), so there the check is that nothing ran twice and every execution ended,
the same as on Redis.

Writes results-<label>.json next to the script. Everything runs in containers that are removed at
the end (--keep leaves them up).
"""
import argparse
import concurrent.futures
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import redis

HERE = os.path.dirname(os.path.abspath(__file__))
N8N_IMAGE = "n8nio/n8n:2.46.6"
PG_IMAGE = "postgres:16"
SIDECAR_IMAGE = "python:3.12-slim"
NET = "n8nc"
BATCH = 200
WORKFLOW_ID = "KiviCompatFlow01"

SERVERS = {
    # The -lua variant: Bull and n8n itself (leader election, locks, instance registry, API rate
    # limit) run Lua scripts. seccomp=unconfined lets KiviDB use io_uring, as on a Linux host.
    "kividb": {
        "image": "kividb/kividb:v1.2.0-lua",
        "port": 6380,
        "args": ["--protected-mode", "no", "--aof"],
        "docker": ["--security-opt", "seccomp=unconfined"],
        "data": "/home/kividb",  # the image's working directory, owned by its user (1000)
    },
    "redis8": {
        "image": "redis:8",
        "port": 6379,
        "args": ["redis-server", "--appendonly", "yes", "--dir", "/data", "--protected-mode", "no"],
        "docker": [],
        "data": "/data",
    },
}

results = []


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def check(name, ok, detail=""):
    status = "PASS" if ok else "FAIL"
    results.append({"check": name, "status": status, "detail": detail})
    log(f"{status}  {name}{'  -- ' + detail if detail else ''}")
    return ok


def info(name, detail):
    results.append({"check": name, "status": "INFO", "detail": detail})
    log(f"INFO  {name}  -- {detail}")


def docker(*args, check_rc=True, capture=True):
    p = subprocess.run(["docker", *args], capture_output=capture, text=True)
    if check_rc and p.returncode != 0:
        raise RuntimeError(f"docker {' '.join(args[:3])}...: {p.stderr.strip()[:500]}")
    return p.stdout.strip() if capture else ""


def http(method, url, body=None, timeout=10):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except Exception as e:  # connection refused, reset, timeout
        return 0, str(e)


def wait_http(url, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        code, _ = http("GET", url, timeout=3)
        if code == 200:
            return True
        time.sleep(1)
    return False


def psql(sql):
    return docker("exec", "n8nc-pg", "psql", "-U", "n8n", "-d", "n8n", "-At", "-F", "|", "-c", sql)


# ---------------------------------------------------------------- the stack
def n8n_env(server):
    s = SERVERS[server]
    env = {
        "DB_TYPE": "postgresdb",
        "DB_POSTGRESDB_HOST": "n8nc-pg",
        "DB_POSTGRESDB_DATABASE": "n8n",
        "DB_POSTGRESDB_USER": "n8n",
        "DB_POSTGRESDB_PASSWORD": "n8n",
        "EXECUTIONS_MODE": "queue",
        "QUEUE_BULL_REDIS_HOST": "n8nc-redis",
        "QUEUE_BULL_REDIS_PORT": str(s["port"]),
        "QUEUE_HEALTH_CHECK_ACTIVE": "true",
        "N8N_ENCRYPTION_KEY": "kividb-compat-not-a-secret",
        "N8N_DIAGNOSTICS_ENABLED": "false",
        "N8N_VERSION_NOTIFICATIONS_ENABLED": "false",
        "N8N_TEMPLATES_ENABLED": "false",
        "N8N_GRACEFUL_SHUTDOWN_TIMEOUT": "60",
        "OFFLOAD_MANUAL_EXECUTIONS_TO_WORKERS": "true",
        "GENERIC_TIMEZONE": "UTC",
        "N8N_LOG_LEVEL": "info",
    }
    out = []
    for k, v in env.items():
        out += ["-e", f"{k}={v}"]
    return out


def start_server(server, image):
    s = SERVERS[server]
    docker("run", "-d", "--name", "n8nc-redis", "--network", NET, "-v", f"n8nc-data:{s['data']}",
           "-p", f"127.0.0.1:16379:{s['port']}", *s["docker"], image, *s["args"])


def start_n8n(name, server, cmd, host_port):
    docker("run", "-d", "--name", name, "--network", NET, "-p", f"127.0.0.1:{host_port}:5678",
           *n8n_env(server), N8N_IMAGE, *cmd)


def teardown():
    for c in ["n8nc-main", "n8nc-worker1", "n8nc-worker2", "n8nc-sidecar", "n8nc-redis", "n8nc-pg"]:
        docker("rm", "-f", c, check_rc=False)
    docker("volume", "rm", "-f", "n8nc-data", check_rc=False)
    docker("network", "rm", NET, check_rc=False)


def setup(server, image):
    teardown()
    docker("network", "create", NET)
    docker("run", "-d", "--name", "n8nc-pg", "--network", NET, "-e", "POSTGRES_USER=n8n",
           "-e", "POSTGRES_PASSWORD=n8n", "-e", "POSTGRES_DB=n8n", PG_IMAGE)
    start_server(server, image)
    docker("run", "-d", "--name", "n8nc-sidecar", "--network", NET, "-p", "127.0.0.1:8099:8099",
           "-e", "WORK_MS=2000", "-v", f"{HERE}/sidecar.py:/sidecar.py:ro", SIDECAR_IMAGE,
           "python", "/sidecar.py")
    for _ in range(60):
        if subprocess.run(["docker", "exec", "n8nc-pg", "pg_isready", "-U", "n8n"],
                          capture_output=True).returncode == 0:
            break
        time.sleep(1)
    time.sleep(2)
    # Import and publish the workflow with the n8n CLI before main starts, so main activates it.
    wf = ["-v", f"{HERE}/workflow.json:/wf/workflow.json:ro"]
    docker("run", "--rm", "--network", NET, *wf, *n8n_env(server), N8N_IMAGE,
           "import:workflow", "--input=/wf/workflow.json")
    docker("run", "--rm", "--network", NET, *n8n_env(server), N8N_IMAGE,
           "publish:workflow", f"--id={WORKFLOW_ID}")
    start_n8n("n8nc-main", server, ["start"], 5678)
    if not wait_http("http://127.0.0.1:5678/healthz/readiness", 240):
        raise RuntimeError("n8n main did not become ready:\n" + docker("logs", "--tail", "60", "n8nc-main"))
    start_n8n("n8nc-worker1", server, ["worker", "--concurrency=10"], 5681)
    start_n8n("n8nc-worker2", server, ["worker", "--concurrency=10"], 5682)
    for port in (5681, 5682):
        if not wait_http(f"http://127.0.0.1:{port}/healthz/readiness", 240):
            raise RuntimeError(f"worker on {port} did not become ready")
    # The webhook is registered once main has activated the workflow.
    for _ in range(60):
        code, _ = http("POST", "http://127.0.0.1:5678/webhook/kividb-compat", {"batch": 0, "id": 1})
        if code == 200:
            return
        time.sleep(1)
    raise RuntimeError("the webhook never answered 200:\n" + docker("logs", "--tail", "60", "n8nc-main"))


# ---------------------------------------------------------------- one batch
def queue_state(r):
    out = {}
    for k in ("wait", "active", "claiming", "delayed", "failed", "completed"):
        key = f"bull:jobs:{k}"
        t = r.type(key)
        if t == "list":
            out[k] = r.llen(key)
        elif t == "zset":
            out[k] = r.zcard(key)
        else:
            out[k] = 0
    return out


def run_batch(r, b, disruption, expect_exact=True):
    title = f"batch {b} ({disruption})"
    base = int(psql("select coalesce(max(id), 0) from execution_entity") or 0)
    t0 = time.time()

    def fire(i):
        return http("POST", "http://127.0.0.1:5678/webhook/kividb-compat", {"batch": b, "id": i}, timeout=30)[0]

    with concurrent.futures.ThreadPoolExecutor(20) as ex:
        codes = list(ex.map(fire, range(1, BATCH + 1)))
    check(f"{title}: all {BATCH} webhook calls accepted (HTTP 200)", codes.count(200) == BATCH,
          f"{codes.count(200)} x 200, others: {sorted(set(c for c in codes if c != 200))}")

    if disruption != "none":
        time.sleep(max(0, 6 - (time.time() - t0)))
        before = queue_state(r)
        td = time.time()
        if disruption == "worker restart":
            docker("restart", "-t", "90", "n8nc-worker1")
            ok = wait_http("http://127.0.0.1:5681/healthz/readiness", 180)
            check(f"{title}: worker 1 ready again after its restart", ok,
                  f"restart took {time.time() - td:.1f}s, queue before it: {before}")
        elif disruption == "server restart":
            docker("restart", "-t", "30", "n8nc-redis")
            r.connection_pool.disconnect()
            after = None
            for _ in range(60):
                try:
                    after = queue_state(r)
                    break
                except redis.exceptions.ConnectionError:
                    time.sleep(0.5)
            check(f"{title}: jobs waiting in the queue were still there after the server restart",
                  after is not None and before["wait"] > 0 and after["wait"] + after["active"] + after["claiming"] > 0,
                  f"before restart {before}, right after {after}, restart took {time.time() - td:.1f}s")
            ok = all(wait_http(f"http://127.0.0.1:{p}/healthz/readiness", 120) for p in (5678, 5681, 5682))
            check(f"{title}: main and both workers ready again after the server restart", ok)
        elif disruption == "worker kill":
            docker("kill", "-s", "KILL", "n8nc-worker2")
            time.sleep(1)
            docker("start", "n8nc-worker2")
            ok = wait_http("http://127.0.0.1:5682/healthz/readiness", 180)
            check(f"{title}: worker 2 ready again after SIGKILL and start", ok, f"queue before the kill: {before}")

    # Wait for every execution of the batch to end.
    end = time.time() + 600
    rows = []
    while time.time() < end:
        rows = [l.split("|") for l in psql(f"select id, status from execution_entity where id > {base}").splitlines() if l]
        open_ = [x for x in rows if x[1] in ("new", "running", "waiting", "unknown")]
        q = queue_state(r)
        if len(rows) >= BATCH and not open_ and q["wait"] == 0 and q["active"] == 0 and q["claiming"] == 0:
            break
        time.sleep(2)
    took = time.time() - t0
    time.sleep(3)  # let Done calls that are in flight land
    rows = [l.split("|") for l in psql(f"select id, status from execution_entity where id > {base}").splitlines() if l]
    statuses = {}
    for _, st in rows:
        statuses[st] = statuses.get(st, 0) + 1
    stats = json.loads(http("GET", "http://127.0.0.1:8099/stats")[1])
    work = {k: v for k, v in stats["work"].items() if k.startswith(f"{b}:")}
    done = {k: v for k, v in stats["done"].items() if k.startswith(f"{b}:")}
    fails = {i for i in range(1, BATCH + 1) if i % 10 == 0}
    flaky = {i for i in range(1, BATCH + 1) if i % 10 == 5}
    oks = set(range(1, BATCH + 1)) - fails
    done_counts = {i: len(done.get(f"{b}:{i}", [])) for i in range(1, BATCH + 1)}
    execs_per_id = {i: len({e["exec"] for e in work.get(f"{b}:{i}", [])}) for i in range(1, BATCH + 1)}
    dup_done = [i for i, n in done_counts.items() if n > 1]
    multi_exec = [i for i, n in execs_per_id.items() if n > 1]
    q = queue_state(r)
    summary = f"{len(rows)} executions in {took:.0f}s: {statuses}; queue after: {q}"

    check(f"{title}: no execution ran twice (no id reached Done twice, no id ran in two executions)",
          not dup_done and not multi_exec, f"Done twice: {dup_done[:10]}, two executions: {multi_exec[:10]}")
    check(f"{title}: every execution ended and the queue is empty",
          not [x for x in rows if x[1] in ("new", "running", "waiting", "unknown")]
          and q["wait"] == 0 and q["active"] == 0 and q["claiming"] == 0, summary)
    if expect_exact:
        check(f"{title}: exactly {BATCH} executions, {len(oks)} success and {len(fails)} error",
              len(rows) == BATCH and statuses.get("success", 0) == len(oks) and statuses.get("error", 0) == len(fails),
              summary)
        check(f"{title}: each id that should succeed reached Done exactly once, each failing id never",
              all(done_counts[i] == 1 for i in oks) and all(done_counts[i] == 0 for i in fails),
              f"ids with Done != 1: {[i for i in oks if done_counts[i] != 1][:10]}")
        tries_flaky = sorted({len(work.get(f'{b}:{i}', [])) for i in flaky})
        tries_fail = sorted({len(work.get(f'{b}:{i}', [])) for i in fails})
        check(f"{title}: n8n retried: ids failing once took 2 tries, ids always failing 3",
              tries_flaky == [2] and tries_fail == [3], f"tries seen: once-failing {tries_flaky}, always-failing {tries_fail}")
    else:
        crashed = len(rows) - statuses.get("success", 0) - statuses.get("error", 0)
        lost = [i for i in oks if done_counts[i] == 0]
        info(f"{title}: what the kill cost", f"{summary}; ids that should have succeeded but did not reach Done: "
             f"{len(lost)}; executions neither success nor error: {crashed}")
    return {"title": title, "executions": len(rows), "statuses": statuses, "seconds": round(took, 1), "queue_after": q}


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", choices=sorted(SERVERS), required=True)
    ap.add_argument("--image", help="override the server image (for example a build of master)")
    ap.add_argument("--label")
    ap.add_argument("--keep", action="store_true", help="leave the containers running")
    args = ap.parse_args()
    image = args.image or SERVERS[args.server]["image"]
    label = args.label or args.server
    batches = []
    r = redis.Redis(host="127.0.0.1", port=16379, decode_responses=True, socket_timeout=10)
    server_info = {}
    try:
        log(f"setting up n8n {N8N_IMAGE} on {image}")
        setup(args.server, image)
        server_info = {k: r.info("server").get(k) for k in ("redis_version", "kividb_version")}
        policy = r.config_get("maxmemory-policy").get("maxmemory-policy")
        check("health: main /healthz and /healthz/readiness answer 200",
              http("GET", "http://127.0.0.1:5678/healthz")[0] == 200
              and http("GET", "http://127.0.0.1:5678/healthz/readiness")[0] == 200)
        check("health: both workers' /healthz/readiness answer 200",
              all(http("GET", f"http://127.0.0.1:{p}/healthz/readiness")[0] == 200 for p in (5681, 5682)))
        info("server: maxmemory-policy as CONFIG GET reports it", str(policy))
        names = [c.get("name") for c in r.client_list()]
        info("server: connections from n8n", f"{len(names)} connections, named: {sorted(set(n for n in names if n))}")
        http("POST", "http://127.0.0.1:8099/reset", {})
        r.config_resetstat()

        batches.append(run_batch(r, 1, "none"))
        batches.append(run_batch(r, 2, "worker restart"))
        batches.append(run_batch(r, 3, "server restart"))
        batches.append(run_batch(r, 4, "worker kill", expect_exact=False))

        # The data must survive a restart once the queue is idle: Bull's job id counter, and the keys.
        idle_keys = sorted(r.scan_iter("bull:*", count=1000))
        job_id = r.get("bull:jobs:id")
        docker("restart", "-t", "30", "n8nc-redis")
        r.connection_pool.disconnect()
        for _ in range(60):
            try:
                r.ping()
                break
            except redis.exceptions.ConnectionError:
                time.sleep(0.5)
        check("persistence: Bull's job id counter and its keys are the same after a restart",
              r.get("bull:jobs:id") == job_id and sorted(r.scan_iter("bull:*", count=1000)) == idle_keys,
              f"bull:jobs:id {job_id} -> {r.get('bull:jobs:id')}, {len(idle_keys)} bull:* keys")
        ok = all(wait_http(f"http://127.0.0.1:{p}/healthz/readiness", 120) for p in (5678, 5681, 5682))
        check("persistence: main and both workers ready again after the idle restart", ok)
        last = run_batch(r, 5, "none")
        batches.append(last)
        cmds = r.info("commandstats")
        commands = sorted(k.replace("cmdstat_", "").upper() for k in cmds)
    except Exception as e:
        check("harness: the run finished", False, f"{type(e).__name__}: {e}"[:1500])
        commands = []
    finally:
        logs = {c: docker("logs", "--tail", "40", c, check_rc=False) for c in ("n8nc-main", "n8nc-worker1", "n8nc-worker2")}
        if not args.keep:
            teardown()

    counts = {s: sum(1 for x in results if x["status"] == s) for s in ("PASS", "FAIL", "INFO")}
    out = {
        "label": label,
        "server_image": image,
        "server": server_info,
        "n8n_image": N8N_IMAGE,
        "redis_py": redis.__version__,
        "run_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "counts": counts,
        "checks": results,
        "batches": batches,
        "commands_seen": commands,
        "log_tails": logs if counts["FAIL"] else {},
    }
    path = os.path.join(HERE, f"results-{label}.json")
    with open(path, "w") as f:
        json.dump(out, f, indent=2)
    log(f"{counts}  -> {path}")
    sys.exit(1 if counts["FAIL"] else 0)


if __name__ == "__main__":
    main()
