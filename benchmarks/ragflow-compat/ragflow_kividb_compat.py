#!/usr/bin/env python3
"""
RAGFlow 0.27 <-> Redis-protocol server compatibility harness, end to end (RAGFlow 1.0: ragflow1_kividb_compat.py).

Drives a running RAGFlow stack (its own docker-compose, with its `redis` service replaced by KiviDB,
by redis:8, or left as Valkey) through RAGFlow's HTTP API, the same calls its own HTTP API tests make
(test/testcases/test_http_api), and records PASS / FAIL / WARN / SKIP per check. No cloud model is
called: RAGFlow's OpenAI-API-compatible provider points at stub_openai.py, a local stand-in for the
chat and embedding models.

  pip install -r requirements.txt
  python ragflow_kividb_compat.py --label kividb --compose-dir ~/ragflow/docker \
      --compose-file ~/ragflow/docker/docker-compose.yml --compose-file compose.kividb.yaml

Writes results-<label>.json next to the script. run.sh brings the stack up and runs this.

What it covers (the harness reads the server only to look at the task queue and INFO):
  * start-up: the services up, and what RAGFlow and redis-py send at connect time
  * accounts: register, login, an API token
  * models: the OpenAI-API-compatible provider with the stub's chat and embedding models
  * a dataset: documents uploaded and parsed by the task executors, which take their tasks from
    a Redis stream through a consumer group; chunks; retrieval; every task acknowledged
  * chat: an assistant on the dataset, a session, a completion, streamed and not
  * restart: with the task executors paused, parse tasks wait in the stream; the server restarts
    twice; the executors resume and parse every queued document
  * no command RAGFlow sent was refused by the server

NOTE: the stub model proves the wiring, not a model's quality.
"""
import argparse
import json
import os
import re
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone

import redis
import requests

results = []
ARGS = None
STATE = {}
EMAIL = "qa@infiniflow.org"
PROVIDER, INSTANCE = "OpenAI-API-Compatible", "stub"


# ---------------------------------------------------------------- check runner
def check(name, fn, level="FAIL"):
    t0 = time.time()
    try:
        detail = fn()
        status = "PASS"
        if isinstance(detail, tuple) and detail and detail[0] in ("SKIP", "WARN"):
            status, detail = detail
    except Exception as e:  # noqa
        detail, status = f"{type(e).__name__}: {str(e)[:300]}", level
    results.append({"check": name, "status": status, "detail": detail or "",
                    "ms": round((time.time() - t0) * 1000)})
    print(f"  [{status:4}] {name}" + (f"  ({detail})" if detail else ""), flush=True)
    return status == "PASS"


def run_group(title, fn):
    print(f"\n== {title}", flush=True)
    try:
        return fn()
    except Exception as e:  # noqa
        detail = f"{type(e).__name__}: {str(e)[:300]}"
        results.append({"check": f"{title}: setup", "status": "FAIL", "detail": detail, "ms": 0})
        print(f"  [FAIL] {title}: setup  ({detail})", flush=True)
        return None


def need(cond, msg):
    if not cond:
        raise AssertionError(msg)


def wait_for(fn, timeout, every=1.0, what="condition"):
    end = time.time() + timeout
    last = None
    while time.time() < end:
        try:
            v = fn()
            if v:
                return v
        except Exception as e:  # noqa
            last = e
        time.sleep(every)
    raise TimeoutError(f"{what} not met in {timeout}s" + (f" (last error: {last})" if last else ""))


# ---------------------------------------------------------------- the stack
def compose(*args, check_rc=True, timeout=600):
    cmd = ["docker", "compose"]
    for f in ARGS.compose_file:
        cmd += ["-f", f]
    cmd += list(args)
    p = subprocess.run(cmd, cwd=ARGS.compose_dir, capture_output=True, text=True, timeout=timeout)
    if check_rc and p.returncode != 0:
        raise RuntimeError(f"{' '.join(args)}: {p.stderr.strip()[-300:]}")
    return p.stdout


def container(service):
    cid = compose("ps", "-q", service).strip()
    need(cid, f"no container for {service}")
    return cid


def server(db=1):
    ip = subprocess.run(["docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}",
                         container("redis")], capture_output=True, text=True, check=True).stdout.split()[0]
    return redis.Redis(host=ip, port=6379, password=ARGS.redis_password, db=db, socket_timeout=10)


def executors(signal):
    """Pause (STOP) or resume (CONT) every task executor in the RAGFlow container."""
    subprocess.run(["docker", "exec", container(ARGS.service), "pkill", f"-{signal}", "-f", "rag/svr/task_executor.py"],
                   check=True, capture_output=True)


# ---------------------------------------------------------------- HTTP helpers
def api(method, path, auth=None, **kw):
    headers = kw.pop("headers", {})
    tok = auth if auth is not None else STATE.get("token")
    if tok:
        headers["Authorization"] = tok if tok.startswith("Bearer ") or auth is not None else f"Bearer {tok}"
    r = requests.request(method, f"{ARGS.base}/api/v1{path}", headers=headers, timeout=kw.pop("timeout", 120), **kw)
    try:
        body = r.json()
    except ValueError:
        raise RuntimeError(f"{method} {path} -> {r.status_code}: {r.text[:300]}")
    if body.get("code") != 0:
        raise RuntimeError(f"{method} {path} -> code {body.get('code')}: {str(body.get('message'))[:300]}")
    return body.get("data")


def api_ready():
    r = requests.post(f"{ARGS.base}/api/v1/auth/login", json={}, timeout=10)
    return r.status_code < 500


def password():
    """The encrypted form of "123" that RAGFlow's own tests use (the web client RSA-encrypts it)."""
    src = open(os.path.join(ARGS.ragflow_src, "test", "testcases", "configs.py"), encoding="utf-8").read()
    return re.search(r'PASSWORD = """(.*?)"""', src, re.S).group(1)


def upload(ds, name, text):
    data = api("POST", f"/datasets/{ds}/documents", files=[("file", (name, text.encode(), "text/plain"))])
    return [d["id"] for d in data]


def docs(ds):
    return {d["id"]: d for d in api("GET", f"/datasets/{ds}/documents?page=1&page_size=100")["docs"]}


def wait_parsed(ds, ids, timeout=300):
    def done():
        d = docs(ds)
        st = {i: d[i]["run"] for i in ids}
        if any(s == "FAIL" for s in st.values()):
            raise RuntimeError(f"parse failed: {[(i, d[i].get('progress_msg', '')[-200:]) for i in ids]}")
        return all(s == "DONE" for s in st.values())
    wait_for(done, timeout, 2, "documents parsed")


def streams():
    r = server()
    return [k.decode() for k in r.scan_iter(count=1000) if r.type(k) == b"stream"]


def undelivered():
    """Entries in the task streams that no executor has been given yet."""
    r, total = server(), 0
    for s in streams():
        for g in r.xinfo_groups(s):
            last = g["last-delivered-id"]
            last = last.decode() if isinstance(last, bytes) else last
            total += len(r.xrange(s, f"({last}", "+"))
    return total


def pending():
    r, total = server(), 0
    for s in streams():
        for g in r.xinfo_groups(s):
            total += g["pending"]
    return total


DOC1 = ("KiviDB compatibility note for RAGFlow. The aurora borealis appears near the magnetic poles. "
        "Task executors parse this document from a Redis stream consumer group.")
DOC2 = "Quasar telescopes measure the redshift of distant galaxies and their spectral lines."


# ---------------------------------------------------------------- groups
def g_connection():
    def ping():
        r = server()
        need(r.ping(), "no PONG")
        STATE["server_version"] = r.info("server").get("redis_version")
        return f"redis_version {STATE['server_version']}"
    check("connection: PING through RAGFlow's password", ping)


def g_startup():
    def services_up():
        rows = [json.loads(line) for line in compose("ps", "-a", "--format", "json").splitlines() if line.strip()]
        bad = [f"{r['Service']}: {r['Status']}" for r in rows if not r["Status"].startswith("Up")]
        need(not bad, "; ".join(bad))
        return f"{len(rows)} containers"
    check("start-up: every RAGFlow service is running", services_up)

    def executors_registered():
        names = wait_for(lambda: server().smembers("TASKEXE"), 120, 3, "task executors registered")
        return f"{len(names)} task executor(s) in TASKEXE"
    check("start-up: the task executors register in the server (TASKEXE)", executors_registered)

    def startup_commands():
        stats = server().info("commandstats")
        STATE["startup_commandstats"] = stats
        conn = {k[len("cmdstat_"):] if k.startswith("cmdstat_") else k: v for k, v in stats.items()
                if re.match(r"(cmdstat_)?(client|info|config|hello|select|auth)", k)}
        # redis-py 7 probes CLIENT MAINT_NOTIFICATIONS and carries on when it is refused; Redis 8.10.3
        # refuses it too, so it says nothing about a server.
        bad = {k: v for k, v in conn.items() if (v.get("failed_calls") or v.get("rejected_calls"))
               and k != "client|maint_notifications"}
        summary = ", ".join(f"{k} x{v['calls']}" for k, v in sorted(conn.items()))
        need(not bad, "refused: " + ", ".join(bad) + f"; all: {summary}")
        return summary
    check("start-up: CLIENT / INFO / CONFIG / HELLO / SELECT / AUTH sent at connect are all accepted",
          startup_commands)


def g_account():
    def register():
        r = requests.post(f"{ARGS.base}/api/v1/users", json={"email": EMAIL, "nickname": "qa", "password": password()},
                          timeout=60).json()
        need(r.get("code") == 0 or "already registered" in str(r.get("message")), r)
    check("account: register", register)

    def login():
        r = requests.post(f"{ARGS.base}/api/v1/auth/login", json={"email": EMAIL, "password": password()}, timeout=60)
        need(r.json().get("code") == 0, r.text[:200])
        STATE["auth"] = r.headers["Authorization"]
    check("account: login", login)

    def token():
        STATE["token"] = api("POST", "/system/tokens", auth=STATE["auth"])["token"]
    check("account: create an API token", token)


def g_models():
    def provider():
        try:
            api("PUT", "/providers", json={"provider_name": PROVIDER})
        except RuntimeError as e:
            if "exist" not in str(e).lower() and "duplicat" not in str(e).lower():
                raise
        api("POST", f"/providers/{PROVIDER}/instances", json={
            "instance_name": INSTANCE, "api_key": "stub", "region": "default", "base_url": ARGS.stub_url,
            "model_info": [{"model_type": ["chat"], "model_name": "stub-chat", "max_tokens": 4096},
                           {"model_type": ["embedding"], "model_name": "stub-embed", "max_tokens": 512}]})
    check("models: add the OpenAI-API-compatible provider pointing at the stub", provider)

    def defaults():
        for t, m in (("chat", "stub-chat"), ("embedding", "stub-embed")):
            api("PATCH", "/models/default", json={"model_provider": PROVIDER, "model_instance": INSTANCE,
                                                  "model_type": t, "model_name": m})
    check("models: make the stub's chat and embedding models the defaults", defaults)


def g_dataset():
    ds = api("POST", "/datasets", json={"name": f"compat-{uuid.uuid4().hex[:6]}", "chunk_method": "naive"})["id"]
    STATE["ds"] = ds
    print(f"  (dataset {ds})")

    def parse():
        ids = upload(ds, "aurora.txt", DOC1) + upload(ds, "quasar.txt", DOC2)
        STATE["ids"] = ids
        api("POST", f"/datasets/{ds}/documents/parse", json={"document_ids": ids})
        wait_parsed(ds, ids)
        return f"{len(ids)} documents DONE"
    check("dataset: two uploaded documents are parsed by the task executors (Redis stream)", parse)

    def chunks():
        n = 0
        for i in STATE["ids"]:
            n += len(api("GET", f"/datasets/{ds}/documents/{i}/chunks")["chunks"])
        need(n >= 2, f"{n} chunks")
        return f"{n} chunks"
    check("dataset: the documents have chunks", chunks)

    def retrieval():
        d = api("POST", "/retrieval", json={"question": "aurora borealis magnetic poles", "dataset_ids": [ds]})
        hits = [c.get("content", "") for c in d.get("chunks", [])]
        need(any("aurora" in h for h in hits), f"chunks {hits[:2]}")
    check("dataset: retrieval finds the right chunk", retrieval)

    def queue_state():
        ss = streams()
        need(ss, "no task stream")
        groups = {s: [g["name"].decode() if isinstance(g["name"], bytes) else g["name"]
                      for g in server().xinfo_groups(s)] for s in ss}
        need(all(groups.values()), f"no consumer group: {groups}")
        p = wait_for(lambda: pending() == 0 or None, 60, 2, "no pending entries") and 0
        return f"streams {groups}, pending {p}"
    check("dataset: the task stream has its consumer group and every task is acknowledged", queue_state)


def g_chat():
    ds = STATE["ds"]
    chat = api("POST", "/chats", json={"name": f"compat-{uuid.uuid4().hex[:6]}", "dataset_ids": [ds]})["id"]
    sess = api("POST", f"/chats/{chat}/sessions", json={"name": "compat"})["id"]

    def completion():
        d = api("POST", "/chat/completions", json={"chat_id": chat, "session_id": sess, "stream": False,
                                                   "messages": [{"role": "user", "content": "tell me about the aurora"}]})
        need("stub answer" in d.get("answer", ""), f"answer {d.get('answer')!r}")
        need(d.get("session_id") == sess, "wrong session")
    check("chat: a completion in a session answers from the stub model", completion)

    def streamed():
        got = []
        with requests.post(f"{ARGS.base}/api/v1/chat/completions", headers={"Authorization": f"Bearer {STATE['token']}"},
                           json={"chat_id": chat, "session_id": sess, "stream": True,
                                 "messages": [{"role": "user", "content": "and the quasar?"}]},
                           stream=True, timeout=120) as r:
            for line in r.iter_lines(decode_unicode=True):
                if line and line.startswith("data:"):
                    try:
                        got.append(json.loads(line[5:]))
                    except ValueError:
                        pass
        text = json.dumps(got)
        need("stub answer" in text, f"{len(got)} events: {text[:300]}")
        return f"{len(got)} events"
    check("chat: a streamed completion", streamed)


def g_restart():
    ds = STATE["ds"]
    ids = []
    executors("STOP")
    try:
        def queue():
            compose("restart", "redis")  # drops the paused executors' blocked reads
            wait_for(lambda: server().ping(), 60, 1, "server back")
            wait_for(api_ready, 120, 2, "API back")
            for i in range(3):
                ids.extend(upload(ds, f"queued-{i}.txt", f"queued document {i} about nebula number {i}."))
            api("POST", f"/datasets/{ds}/documents/parse", json={"document_ids": ids})
            n = wait_for(lambda: undelivered() or None, 30, 1, "tasks waiting in the stream")
            need(n >= 3, f"{n} waiting")
            STATE["queued"] = n
            return f"{n} tasks waiting in the stream"
        check("restart: with the executors paused, 3 parse tasks wait in the stream", queue)

        def restart_server():
            compose("restart", "redis")
            wait_for(lambda: server().ping(), 60, 1, "server back")
            n = undelivered()
            need(n == STATE.get("queued"), f"{STATE.get('queued')} waiting before, {n} after")
            return f"{n} still waiting"
        check("restart: the server restarts and the waiting tasks are still in the stream", restart_server)
    finally:
        executors("CONT")

    def drained():
        wait_parsed(ds, ids, timeout=420)
        wait_for(lambda: pending() == 0, 60, 2, "no pending entries")
        return f"{len(ids)} documents DONE"
    check("restart: the executors resume and parse every queued document", drained)

    def after():
        d = api("POST", "/retrieval", json={"question": "nebula number 2", "dataset_ids": [ds]})
        need(any("nebula" in c.get("content", "") for c in d.get("chunks", [])), "not found")
    check("restart: retrieval finds a document parsed after the restarts", after)


def g_refused():
    def none_refused():
        stats = server().info("commandstats")
        noscript = server().info("errorstats").get("errorstat_NOSCRIPT", {}).get("count", 0)
        bad = {k: v for k, v in stats.items() if (v.get("failed_calls") or v.get("rejected_calls"))
               and "maint_notifications" not in k}
        # redis-py runs a script with EVALSHA first, and on NOSCRIPT (the server does not have it
        # cached yet, or no longer has it after a restart) loads it and runs it again. Those
        # EVALSHA replies are counted as failed calls on Redis too.
        ev = bad.get("cmdstat_evalsha")
        if ev and not ev.get("rejected_calls") and ev.get("failed_calls", 0) <= noscript:
            del bad["cmdstat_evalsha"]
        need(not bad, ", ".join(f"{k}: failed {v.get('failed_calls')}, rejected {v.get('rejected_calls')}"
                                for k, v in bad.items()))
        if noscript:
            return f"{noscript} EVALSHA answered NOSCRIPT, then run with the script, as redis-py does"
    check("server: no command RAGFlow sent was refused (INFO commandstats)", none_refused)


# ---------------------------------------------------------------- main
def main():
    global ARGS
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--label", required=True)
    ap.add_argument("--base", default="http://127.0.0.1:9380")
    ap.add_argument("--compose-dir", required=True)
    ap.add_argument("--compose-file", action="append", required=True)
    ap.add_argument("--service", default="ragflow-cpu")
    ap.add_argument("--ragflow-src", default=os.path.expanduser("~/ragflow"))
    ap.add_argument("--redis-password", default="infini_rag_flow")
    ap.add_argument("--stub-url", default="http://stub-llm:9999/v1")
    ap.add_argument("--image", default="")
    ap.add_argument("--groups", default="", help="comma-separated subset, for debugging (account always runs)")
    ARGS = ap.parse_args()

    groups = [("connection", g_connection), ("start-up", g_startup), ("account", g_account), ("models", g_models),
              ("dataset", g_dataset), ("chat", g_chat), ("restart", g_restart), ("server", g_refused)]
    only = set(filter(None, ARGS.groups.split(",")))
    wait_for(api_ready, 900, 5, "RAGFlow API")
    for title, fn in groups:
        if not only or title in only or title == "account":
            run_group(title, fn)

    stats, errs = {}, {}
    try:
        stats = server().info("commandstats")
        errs = server().info("errorstats")
    except Exception as e:  # noqa
        errs = {"error": str(e)}
    counts = {s: sum(1 for r in results if r["status"] == s) for s in ("PASS", "FAIL", "WARN", "SKIP")}
    ver = subprocess.run(["git", "-C", ARGS.ragflow_src, "describe", "--tags", "--always"],
                         capture_output=True, text=True).stdout.strip()
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"results-{ARGS.label}.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump({"label": ARGS.label, "image": ARGS.image, "redis_version": STATE.get("server_version"),
                   "ragflow": ver, "counts": counts, "checks": results,
                   "commands_at_startup": {k: {"calls": v.get("calls"), "failed": v.get("failed_calls"),
                                                "rejected": v.get("rejected_calls")}
                                            for k, v in sorted(STATE.get("startup_commandstats", {}).items())},
                   "commands_after_run": {k: {"calls": v.get("calls"), "failed": v.get("failed_calls"),
                                               "rejected": v.get("rejected_calls")} for k, v in sorted(stats.items())},
                   "errorstats": errs,
                   "when": datetime.now(timezone.utc).isoformat(timespec="seconds")}, f, indent=2)
    print(f"\n{counts}  -> {out}")
    sys.exit(0 if not counts["FAIL"] else 1)


if __name__ == "__main__":
    main()
