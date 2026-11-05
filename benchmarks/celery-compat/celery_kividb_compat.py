#!/usr/bin/env python3
"""
Celery <-> Redis-protocol server compatibility harness.

Runs a real Celery worker (a subprocess, prefork pool) with the server as both the broker (kombu's
Redis transport) and the result backend, and records PASS / FAIL / WARN per check. No external
services are involved.

  pip install -r requirements.txt
  python celery_kividb_compat.py --url redis://HOST:6380 --label kividb-lua \
      --restart-cmd "docker restart kividb"

Writes results-<label>.json next to the script. Run the same command against stock Redis (for
example the `redis:8` image) with a different --label to get a comparison. --restart-cmd is a shell
command that restarts the server; the restart checks are skipped without it, and they need the
server to persist its data (KiviDB `--aof`, Redis `--appendonly yes`). --backend-url sets a
different result backend URL, for example the same server with `?protocol=2` (redis-py over RESP2).

What it covers:
  * a task's round trip through the broker and the result backend
  * group, chain and chord (Celery 5.7 saves a chord's header results with a Lua script)
  * countdown (kombu's unacked sorted set), retries, revoke (a broadcast over pub/sub)
  * routing to a second queue, `inspect ping` (the pidbox fanout), result expiry
  * a worker killed with SIGKILL in the middle of an `acks_late` task: the task is redelivered
    to the next worker once the visibility timeout has passed
  * the server restarted while tasks wait in the queue: none are lost
  * the server restarted under a running worker: the worker reconnects and goes on
"""
import argparse
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from importlib.metadata import version

import redis

HERE = os.path.dirname(os.path.abspath(__file__))
results = []


def check(name, fn, level="FAIL"):
    t0 = time.time()
    try:
        detail = fn()
        status = "PASS"
    except SkipCheck as e:
        detail, status = str(e), "SKIP"
    except Exception as e:  # noqa
        detail, status = f"{type(e).__name__}: {str(e)[:300]}", level
    results.append({"check": name, "status": status, "detail": detail or "",
                    "ms": round((time.time() - t0) * 1000)})
    print(f"  [{status:4}] {name}" + (f"  ({detail})" if detail else ""), flush=True)


class SkipCheck(Exception):
    pass


def wait_until(fn, timeout, what):
    deadline = time.time() + timeout
    while time.time() < deadline:
        v = fn()
        if v:
            return v
        time.sleep(0.2)
    raise AssertionError(f"timed out after {timeout}s waiting for {what}")


class Worker:
    """One `celery worker` process group, so SIGKILL takes the pool's children too."""

    n = 0

    def __init__(self, env):
        Worker.n += 1
        self.name = f"compat{Worker.n}-{uuid.uuid4().hex[:4]}@localhost"
        self.log = open(os.path.join(HERE, f"worker-{env.get('CELERY_COMPAT_LABEL', 'x')}-{Worker.n}.log"), "w")
        self.p = subprocess.Popen(
            [sys.executable, "-m", "celery", "-A", "compat_tasks", "worker", "-Q", "celery,compat-other",
             "-c", "2", "-l", "info", "-n", self.name],
            cwd=HERE, env=env, stdout=self.log, stderr=subprocess.STDOUT, start_new_session=True)

    def wait_ready(self, app, timeout=60):
        def pong():
            # A new connection each time: a pooled one may predate a server restart.
            try:
                with app.connection_for_write() as conn:
                    replies = app.control.inspect(destination=[self.name], timeout=1, connection=conn).ping()
                return (replies or {}).get(self.name)
            except Exception:  # noqa: the server may still be coming back
                return None
        wait_until(pong, timeout, f"worker {self.name} to answer ping")

    def stop(self):
        if self.p.poll() is None:
            os.killpg(self.p.pid, signal.SIGTERM)
            try:
                self.p.wait(30)
            except subprocess.TimeoutExpired:
                os.killpg(self.p.pid, signal.SIGKILL)
                self.p.wait(10)
        self.log.close()

    def kill9(self):
        os.killpg(self.p.pid, signal.SIGKILL)
        self.p.wait(10)
        self.log.close()


def restart_server(cmd, r):
    subprocess.run(cmd, shell=True, check=True, timeout=120, stdout=subprocess.DEVNULL)

    def up():
        try:
            return r.ping()
        except redis.RedisError:
            return False
    wait_until(up, 60, "the server to answer PING after the restart")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--label", default="target")
    ap.add_argument("--restart-cmd", default="")
    ap.add_argument("--backend-url", default="")
    a = ap.parse_args()

    prefix = f"cc:{uuid.uuid4().hex[:8]}:"
    env = {**os.environ, "CELERY_COMPAT_URL": a.url, "CELERY_COMPAT_BACKEND_URL": a.backend_url or a.url,
           "CELERY_COMPAT_PREFIX": prefix, "CELERY_COMPAT_LABEL": a.label}
    os.environ.update(env)
    sys.path.insert(0, HERE)
    import compat_tasks as t  # noqa: E402

    r = redis.Redis.from_url(a.url)
    info = r.info("server")
    meta = {"label": a.label, "date": time.strftime("%Y-%m-%d"), "celery": version("celery"),
            "kombu": version("kombu"), "redis_py": version("redis"), "backend_url": (a.backend_url or a.url).split("@")[-1],
            "server": {k: info[k] for k in ("redis_version", "kividb_version", "valkey_version") if k in info}}
    print(f"== {a.label}: {a.url.split('@')[-1]}  {meta['server']}  celery {meta['celery']} kombu {meta['kombu']}")
    ran = lambda: [x.decode() for x in r.lrange(prefix + "ran", 0, -1)]

    w = Worker(env)
    try:
        check("worker starts and answers inspect ping (pidbox fanout over pub/sub)", lambda: (w.wait_ready(t.app), "pong")[1])

        def roundtrip():
            assert t.add.delay(2, 3).get(timeout=30) == 5
            return "add(2, 3) = 5"
        check("task round trip: broker, worker, result backend", roundtrip)

        def group():
            from celery import group as g
            res = g(t.add.s(i, i) for i in range(10))().get(timeout=30)
            assert res == [2 * i for i in range(10)], res
            return "10 results in order"
        check("group of 10", group)

        def chain():
            assert (t.add.s(1, 2) | t.add.s(3) | t.add.s(4))().get(timeout=30) == 10
            return "1+2+3+4 = 10"
        check("chain", chain)

        def chord():
            from celery import chord as c
            res = c(t.add.s(i, i) for i in range(10))(t.tsum.s()).get(timeout=30)
            assert res == 90, res
            return "sum = 90"
        check("chord (Celery 5.7 saves header results with a Lua script)", chord)

        def countdown():
            t0 = time.time()
            res = t.add.apply_async((5, 5), countdown=3).get(timeout=30)
            took = time.time() - t0
            assert res == 10 and took >= 2.9, (res, took)
            return f"ran after {took:.1f}s"
        check("countdown (kombu's unacked sorted set)", countdown)

        def retry():
            key = "flaky-" + uuid.uuid4().hex[:6]
            assert t.flaky.delay(key, 2).get(timeout=30) == 3
            return "failed twice, succeeded on the third try"
        check("retry", retry)

        def revoke():
            res = t.mark.apply_async(("revoked",), countdown=4)
            res.revoke()
            t.mark.delay("after-revoke").get(timeout=30)
            time.sleep(6)
            assert "revoked" not in ran(), ran()
            return "the revoked task did not run"
        check("revoke (broadcast to workers)", revoke)

        def other_queue():
            assert t.on_other_queue.delay(21).get(timeout=30) == 42
            return "routed to compat-other"
        check("task routed to a second queue", other_queue)

        def expiry():
            res = t.add.delay(1, 1)
            res.get(timeout=30)
            ttl = r.ttl(f"celery-task-meta-{res.id}")
            assert 0 < ttl <= 600, ttl
            return f"result key TTL {ttl}s"
        check("result expires (TTL on the result key)", expiry)

        def acks_late_crash():
            nonlocal w
            name = "crash-" + uuid.uuid4().hex[:6]
            t.slow_mark.delay(name, 6)
            wait_until(lambda: name.encode() in r.lrange(prefix + "started", 0, -1), 30, "the slow task to start")
            w.kill9()
            w = Worker(env)
            w.wait_ready(t.app)
            wait_until(lambda: name.encode() in r.lrange(prefix + "done", 0, -1), 90,
                       "the task to be redelivered and finish")
            return "redelivered after SIGKILL and finished"
        check("acks_late task redelivered after the worker is killed (SIGKILL)", acks_late_crash)

        def queued_survive_restart():
            nonlocal w
            if not a.restart_cmd:
                raise SkipCheck("no --restart-cmd")
            w.stop()
            names = [f"q{i}-{uuid.uuid4().hex[:4]}" for i in range(20)]
            for n in names:
                t.mark.delay(n)
            restart_server(a.restart_cmd, r)
            w = Worker(env)
            w.wait_ready(t.app)
            wait_until(lambda: set(names) <= set(ran()), 60, "all 20 queued tasks to run")
            return "20 of 20 ran after the restart"
        check("tasks queued across a server restart are not lost", queued_survive_restart)

        def worker_reconnects():
            if not a.restart_cmd:
                raise SkipCheck("no --restart-cmd")
            restart_server(a.restart_cmd, r)
            assert t.add.delay(20, 22).get(timeout=90) == 42
            return "the running worker reconnected and ran a new task"
        check("running worker reconnects after a server restart", worker_reconnects)
    finally:
        w.stop()
        for k in r.scan_iter(match=prefix + "*"):
            r.delete(k)

    json.dump({"meta": meta, "results": results}, open(os.path.join(HERE, f"results-{a.label}.json"), "w"), indent=2)
    n = lambda s: sum(1 for x in results if x["status"] == s)
    print(f"\nPASS {n('PASS')}  WARN {n('WARN')}  FAIL {n('FAIL')}  SKIP {n('SKIP')}  -> results-{a.label}.json")
    sys.exit(1 if n("FAIL") else 0)


if __name__ == "__main__":
    main()
