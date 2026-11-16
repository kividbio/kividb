#!/usr/bin/env python3
"""
Dify <-> Redis-protocol server compatibility harness, end to end.

Drives a running Dify stack (its own docker-compose, with its `redis` service replaced by KiviDB or
by redis:8) through Dify's HTTP APIs, and records PASS / FAIL / WARN / SKIP per check. No cloud
model is called: the workflows and chatflows come from Dify's own test fixtures and need no model,
and the one check group that does use a model points Dify at stub_openai.py, a local stand-in.

  pip install -r requirements.txt
  python dify_kividb_compat.py --label kividb --compose-dir ~/dify/docker \
      --compose-file ~/dify/docker/docker-compose.yaml --compose-file compose.kividb.yaml

Writes results-<label>.json next to the script. run.sh brings the stack up and runs this.

What it covers, all through Dify (the harness reads the server only to count queued tasks, find
leftover locks and read INFO):
  * start-up: every service up, Celery answering, and what Dify, Celery and redis-py send at
    connect time (CLIENT, INFO, CONFIG, HELLO)
  * accounts: set-up, password login, the refresh token (kept in Redis)
  * a workflow app (Dify's conditional_parallel_code_execution_workflow fixture): blocking runs in
    the API process, and streaming runs, which go to a Celery worker and come back over pub/sub
  * a chatflow (Dify's answer_end_with_text fixture), streaming and blocking, and its conversation
  * a knowledge base: documents indexed by Celery tasks, segments, keyword retrieval, file upload,
    delete, the Celery result backend, and the indexing lock being released
  * restart: with the worker stopped, indexing tasks queue up in the server; the server is
    restarted; the worker is started and every queued document is indexed. Then the API and the
    workers are restarted against a running server
  * a model through a plugin: Dify's OpenAI-API-compatible plugin, installed from the marketplace
    by the plugin daemon, pointed at the stub; a chatflow with an LLM node, and a high-quality
    knowledge base embedded with the stub's embedding model
  * no command Dify sent was refused by the server (INFO commandstats)

NOTE: the stub model proves the wiring, not a model's quality.
"""
import argparse
import base64
import json
import os
import re
import subprocess
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import redis
import requests
import yaml

results = []
ARGS = None
S = requests.Session()
STATE = {}
EMAIL, PASSWORD = "compat@example.com", "Kividb-compat-1"
USER = "compat-user"


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


def service_ip(service):
    out = subprocess.run(["docker", "inspect", "-f",
                          "{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}", container(service)],
                         capture_output=True, text=True, check=True).stdout.split()
    need(out, f"no IP for {service}")
    return out[0]


def server(db=0):
    return redis.Redis(host=service_ip("redis"), port=6379, password=ARGS.redis_password, db=db,
                       socket_timeout=10)


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


def worker_ping(timeout=300):
    """Dify's own worker health check (disabled in its compose file by default), run by hand: a
    broadcast over the broker that every worker answers."""
    def pong():
        out = compose("exec", "-T", "worker", "celery", "-A", "celery_healthcheck.celery", "inspect", "ping",
                      check_rc=False, timeout=120)
        return "pong" in out
    wait_for(pong, timeout, 5, "worker answers ping")


def api_ready():
    r = requests.get(f"{ARGS.base}/console/api/setup", timeout=10)
    return r.status_code == 200


# ---------------------------------------------------------------- HTTP helpers
def console(method, path, **kw):
    headers = kw.pop("headers", {})
    csrf = S.cookies.get("csrf_token")
    if csrf:
        headers["X-CSRF-Token"] = csrf
    r = S.request(method, f"{ARGS.base}/console/api{path}", headers=headers, timeout=kw.pop("timeout", 60), **kw)
    if r.status_code >= 400:
        raise RuntimeError(f"{method} {path} -> {r.status_code}: {r.text[:300]}")
    return r.json() if r.content and "json" in r.headers.get("Content-Type", "") else r


def service(method, path, key, **kw):
    r = requests.request(method, f"{ARGS.base}/v1{path}", headers={"Authorization": f"Bearer {key}"},
                         timeout=kw.pop("timeout", 120), **kw)
    if r.status_code >= 400:
        raise RuntimeError(f"{method} /v1{path} -> {r.status_code}: {r.text[:300]}")
    return r


def sse(path, key, payload, timeout=120):
    """POST a streaming request and return the list of events (dicts)."""
    events = []
    with requests.post(f"{ARGS.base}/v1{path}", headers={"Authorization": f"Bearer {key}"}, json=payload,
                       stream=True, timeout=timeout) as r:
        if r.status_code >= 400:
            raise RuntimeError(f"POST /v1{path} -> {r.status_code}: {r.text[:300]}")
        for line in r.iter_lines(decode_unicode=True):
            if line and line.startswith("data:"):
                body = line[5:].strip()
                if body:
                    try:
                        events.append(json.loads(body))
                    except ValueError:
                        pass
    return events


def fixture(name):
    with open(os.path.join(ARGS.dify_src, "api", "tests", "fixtures", "workflow", name), encoding="utf-8") as f:
        return f.read()


def import_app(yaml_text):
    res = console("POST", "/apps/imports", json={"mode": "yaml-content", "yaml_content": yaml_text})
    if res.get("status") == "pending":
        res = console("POST", f"/apps/imports/{res['id']}/confirm")
    need(res.get("status") in ("completed", "completed-with-warnings"), f"import status {res.get('status')}: {res}")
    app_id = res["app_id"]
    console("POST", f"/apps/{app_id}/workflows/publish", json={})
    key = console("POST", f"/apps/{app_id}/api-keys")["token"]
    return app_id, key


# ---------------------------------------------------------------- groups
def g_connection():
    def ping():
        r = server()
        need(r.ping(), "no PONG")
        info = r.info("server")
        STATE["server_version"] = info.get("redis_version")
        return f"redis_version {info.get('redis_version')}"
    check("connection: PING through Dify's password", ping)


def g_startup():
    def services_up():
        rows = [json.loads(line) for line in compose("ps", "-a", "--format", "json").splitlines() if line.strip()]
        bad = [f"{r['Service']}: {r['Status']}" for r in rows
               if r["Service"] != "init_permissions" and not r["Status"].startswith("Up")]
        need(not bad, "; ".join(bad))
        return f"{len(rows)} containers"
    check("start-up: every Dify service is running (none restarting)", services_up)

    def api_healthy():
        wait_for(lambda: subprocess.run(["docker", "inspect", "-f", "{{.State.Health.Status}}", container("api")],
                                        capture_output=True, text=True).stdout.strip() == "healthy",
                 300, 3, "API healthy")
    check("start-up: the API is healthy", api_healthy)
    check("start-up: the Celery worker answers a ping over the broker (celery inspect ping)", worker_ping)

    def startup_commands():
        stats = server().info("commandstats")
        STATE["startup_commandstats"] = stats
        conn = {k[len("cmdstat_"):] if k.startswith("cmdstat_") else k: v for k, v in stats.items()
                if re.match(r"(cmdstat_)?(client|info|config|hello|select|auth)", k)}
        # redis-py 7 probes CLIENT MAINT_NOTIFICATIONS at connect and carries on when it is refused.
        # Redis 8.10.3 refuses it too ("ERR unknown subcommand"), so it says nothing about a server.
        bad = {k: v for k, v in conn.items() if (v.get("failed_calls") or v.get("rejected_calls"))
               and k != "client|maint_notifications"}
        summary = ", ".join(f"{k} x{v['calls']}" for k, v in sorted(conn.items()))
        need(not bad, "refused: " + ", ".join(f"{k} (rejected {v.get('rejected_calls')}, failed "
                                              f"{v.get('failed_calls')})" for k, v in bad.items())
             + f"; all: {summary}")
        return summary
    check("start-up: CLIENT / INFO / CONFIG / HELLO / SELECT / AUTH sent at connect are all accepted",
          startup_commands, level="WARN")


def g_account():
    def setup():
        st = console("GET", "/setup")
        if st.get("step") == "finished":
            return "already set up"
        r = S.post(f"{ARGS.base}/console/api/setup", json={"email": EMAIL, "name": "compat", "password": PASSWORD},
                   timeout=60)
        need(r.status_code in (200, 201), f"{r.status_code}: {r.text[:200]}")
    check("account: set-up creates the admin", setup)

    def login():
        r = S.post(f"{ARGS.base}/console/api/login", timeout=60,
                   json={"email": EMAIL, "password": base64.b64encode(PASSWORD.encode()).decode(),
                         "remember_me": True})
        need(r.status_code == 200, f"{r.status_code}: {r.text[:200]}")
        need(S.cookies.get("access_token") and S.cookies.get("csrf_token"), f"cookies {list(S.cookies.keys())}")
        console("GET", "/account/profile")
    check("account: password login, then an authenticated call", login)

    def refresh():
        r = S.post(f"{ARGS.base}/console/api/refresh-token", headers={"X-CSRF-Token": S.cookies.get("csrf_token", "")},
                   timeout=60)
        need(r.status_code == 200, f"{r.status_code}: {r.text[:200]}")
        console("GET", "/account/profile")
    check("account: the refresh token (kept in Redis) gives a new session", refresh)


WF_EXPECT = {1: {"1": "Code 1", "2": "Code 2", "3": None}, 2: {"1": None, "2": "Code 2", "3": "Code 3"}}


def wf_outputs_ok(outputs, switch):
    exp = WF_EXPECT[switch]
    got = {k: (outputs or {}).get(k) for k in exp}
    need(got == exp, f"outputs {outputs}, expected {exp}")


def stream_workflow(key, switch):
    ev = sse("/workflows/run", key, {"inputs": {"switch": switch}, "response_mode": "streaming", "user": USER})
    fin = [e for e in ev if e.get("event") == "workflow_finished"]
    need(fin, f"no workflow_finished in {len(ev)} events: {[e.get('event') for e in ev][:12]}")
    d = fin[-1]["data"]
    need(d.get("status") == "succeeded", f"status {d.get('status')}: {d.get('error')}")
    wf_outputs_ok(d.get("outputs"), switch)
    return fin[-1]["workflow_run_id"], len(ev)


def g_workflow():
    app_id, key = import_app(fixture("conditional_parallel_code_execution_workflow.yml"))
    STATE["wf_key"] = key
    print(f"  (app {app_id})")

    def blocking(switch):
        def fn():
            r = service("POST", "/workflows/run", key,
                        json={"inputs": {"switch": switch}, "response_mode": "blocking", "user": USER}).json()
            d = r["data"]
            need(d.get("status") == "succeeded", f"status {d.get('status')}: {d.get('error')}")
            wf_outputs_ok(d.get("outputs"), switch)
            STATE["wf_run_id"] = r["workflow_run_id"]
        return fn
    check("workflow: blocking run, switch=1 (if-else, parallel code nodes)", blocking(1))
    check("workflow: blocking run, switch=2", blocking(2))

    def streaming():
        run_id, n = stream_workflow(key, 1)
        STATE["wf_stream_run_id"] = run_id
        return f"{n} events"
    check("workflow: streaming run, executed by the Celery worker and relayed over pub/sub", streaming)

    def detail():
        for rid in (STATE.get("wf_run_id"), STATE.get("wf_stream_run_id")):
            need(rid, "no run id from the runs above")
            d = service("GET", f"/workflows/run/{rid}", key).json()
            need(d.get("status") == "succeeded", f"run {rid}: {d.get('status')}")
    check("workflow: both runs are recorded as succeeded", detail)

    def concurrent():
        switches = [1, 2] * 4
        with ThreadPoolExecutor(len(switches)) as ex:
            outs = list(ex.map(lambda s: stream_workflow(key, s), switches))
        need(len({o[0] for o in outs}) == len(switches), "run ids not distinct")
        return f"{len(switches)} runs, each got its own outputs"
    check("workflow: 8 concurrent streaming runs each receive their own events", concurrent)


def g_chatflow():
    app_id, key = import_app(fixture("answer_end_with_text.yml"))
    print(f"  (app {app_id})")

    def streaming():
        ev = sse("/chat-messages", key, {"inputs": {}, "query": "hello kividb", "response_mode": "streaming",
                                         "user": USER})
        answer = "".join(e.get("answer", "") for e in ev if e.get("event") == "message")
        need(answer == "prefixhello kividbsuffix", f"answer {answer!r}; events {[e.get('event') for e in ev][:12]}")
        need(any(e.get("event") == "message_end" for e in ev), "no message_end")
        STATE["conv"] = next(e.get("conversation_id") for e in ev if e.get("conversation_id"))
        return f"{len(ev)} events"
    check("chatflow: streaming chat message (Celery worker, pub/sub)", streaming)

    def blocking():
        r = service("POST", "/chat-messages", key, json={"inputs": {}, "query": "second", "response_mode": "blocking",
                                                        "user": USER, "conversation_id": STATE.get("conv", "")}).json()
        need(r.get("answer") == "prefixsecondsuffix", f"answer {r.get('answer')!r}")
    check("chatflow: blocking chat message in the same conversation", blocking)

    def conversation():
        r = service("GET", f"/messages?user={USER}&conversation_id={STATE.get('conv')}", key).json()
        need(len(r.get("data", [])) == 2, f"{len(r.get('data', []))} messages")
    check("chatflow: the conversation has both messages", conversation)


DOC_TEXT = ("KiviDB compatibility note. The aurora borealis appears near the magnetic poles. "
            "Celery workers index this document through the Redis protocol broker.\n\n"
            "A second paragraph about quasar telescopes and spectral lines, so there is more than one segment.")


def wait_indexed(dkey, ds, batch, timeout=180):
    def done():
        rows = service("GET", f"/datasets/{ds}/documents/{batch}/indexing-status", dkey).json()["data"]
        st = [r["indexing_status"] for r in rows]
        if any(s in ("error", "paused") for s in st):
            raise RuntimeError(f"indexing status {rows}")
        return st and all(s == "completed" for s in st)
    wait_for(done, timeout, 1, "document indexed")


def lock_keys(dataset_id):
    found = []
    for db in (0, 1):
        found += [k.decode() for k in server(db).scan_iter(match=f"*{dataset_id}*lock*", count=1000)]
        found += [k.decode() for k in server(db).scan_iter(match=f"*lock*{dataset_id}*", count=1000)]
    return sorted(set(found))


def g_knowledge():
    dkey = console("POST", "/datasets/api-keys")["token"]
    STATE["dkey"] = dkey
    ds = service("POST", "/datasets", dkey, json={"name": f"compat-{uuid.uuid4().hex[:6]}",
                                                 "indexing_technique": "economy"}).json()["id"]
    STATE["ds"] = ds
    print(f"  (dataset {ds})")

    def by_text():
        r = service("POST", f"/datasets/{ds}/document/create-by-text", dkey,
                    json={"name": "aurora.txt", "text": DOC_TEXT, "indexing_technique": "economy",
                          "process_rule": {"mode": "automatic"}}).json()
        STATE["doc"] = r["document"]["id"]
        wait_indexed(dkey, ds, r["batch"])
    check("knowledge: a text document is indexed by the Celery worker (economy, keyword index)", by_text)

    def segments():
        r = service("GET", f"/datasets/{ds}/documents/{STATE['doc']}/segments", dkey).json()
        need(r.get("data"), "no segments")
        return f"{len(r['data'])} segments"
    check("knowledge: the document has segments", segments)

    def retrieve():
        r = service("POST", f"/datasets/{ds}/retrieve", dkey, json={"query": "aurora borealis"}).json()
        hits = [x["segment"]["content"] for x in r.get("records", [])]
        need(any("aurora" in h for h in hits), f"records {hits[:2]}")
    check("knowledge: keyword retrieval finds it", retrieve)

    def by_file():
        data = {"indexing_technique": "economy", "process_rule": {"mode": "automatic"}}
        files = {"file": ("quasar.md", b"# Quasars\n\nQuasar telescopes measure redshift of distant galaxies.\n",
                          "text/markdown")}
        r = service("POST", f"/datasets/{ds}/document/create-by-file", dkey, data={"data": json.dumps(data)},
                    files=files).json()
        STATE["doc2"] = r["document"]["id"]
        wait_indexed(dkey, ds, r["batch"])
    check("knowledge: an uploaded file is indexed", by_file)

    def lock_released():
        left = lock_keys(ds)
        need(not left, f"left behind: {left} (TTL {[server(0).ttl(k) for k in left]})")
    check("knowledge: the keyword-index lock (redis-py Lock) is released", lock_released)

    def delete():
        service("DELETE", f"/datasets/{ds}/documents/{STATE['doc2']}", dkey)
        r = service("GET", f"/datasets/{ds}/documents", dkey).json()
        need(STATE["doc2"] not in [d["id"] for d in r["data"]], "still listed")
    check("knowledge: delete a document", delete)

    def results_backend():
        counts = {}
        for k in server(1).scan_iter(match="celery-task-meta-*", count=1000):
            raw = server(1).get(k)
            if raw:
                st = json.loads(raw).get("status")
                counts[st] = counts.get(st, 0) + 1
        need(not counts.get("FAILURE"), f"task states {counts}")
        return f"task states in the result backend: {counts or 'none stored'}"
    check("celery: no task recorded as FAILURE in the result backend", results_backend)


def queued(db=1, names=("dataset", "priority_dataset")):
    total = 0
    r = server(db)
    for k in r.scan_iter(count=1000):
        name = k.decode(errors="replace")
        if any(name == n or name.startswith(n + "\x06\x16") for n in names) and r.type(k) == b"list":
            total += r.llen(k)
    return total


def g_restart():
    dkey, ds = STATE["dkey"], STATE["ds"]
    compose("stop", "worker")
    batches = []
    try:
        def enqueue():
            for i in range(5):
                r = service("POST", f"/datasets/{ds}/document/create-by-text", dkey,
                            json={"name": f"queued-{i}.txt", "text": f"queued document {i} about nebula {i}.",
                                  "indexing_technique": "economy", "process_rule": {"mode": "automatic"}}).json()
                batches.append(r["batch"])
            n = wait_for(lambda: queued() or None, 20, 1, "tasks in the queue")
            need(n >= 5, f"{n} tasks queued")
            return f"{n} tasks waiting in the broker"
        check("restart: with the worker stopped, 5 indexing tasks wait in the broker", enqueue)

        def restart_server():
            before = queued()
            compose("restart", "redis")
            wait_for(lambda: server().ping(), 60, 1, "server back")
            after = queued()
            need(after == before, f"{before} queued before the restart, {after} after")
            return f"{after} still queued"
        check("restart: the server restarts and the queued tasks are still there", restart_server)
    finally:
        compose("start", "worker")

    def drained():
        for b in batches:
            wait_indexed(dkey, ds, b, timeout=240)
        need(queued() == 0, f"{queued()} still queued")
        return f"{len(batches)} documents indexed"
    check("restart: the worker starts and indexes every queued document", drained)

    def session_kept():
        console("GET", "/account/profile")
        r = S.post(f"{ARGS.base}/console/api/refresh-token", headers={"X-CSRF-Token": S.cookies.get("csrf_token", "")},
                   timeout=60)
        need(r.status_code == 200, f"refresh {r.status_code}: {r.text[:200]}")
    check("restart: the login session and refresh token survive the server restart", session_kept)

    def services_restart():
        compose("restart", "api", "worker", "worker_beat", "plugin_daemon", timeout=900)
        wait_for(api_ready, 300, 3, "API back")
        worker_ping()
        stream_workflow(STATE["wf_key"], 2)
    check("restart: API and workers restart against the running server; a streaming run works", services_restart)


# ---------------------------------------------------------------- model through a plugin
PLUGIN = "langgenius/openai_api_compatible"
PROVIDER = f"{PLUGIN}/openai_api_compatible"


def g_model():
    if not ARGS.stub_url:
        check("model: set up the stub model", lambda: ("SKIP", "no --stub-url"))
        return

    def install():
        installed = console("GET", "/workspaces/current/plugin/list?page=1&page_size=100")
        if any(p.get("plugin_id") == PLUGIN for p in installed.get("plugins", [])):
            return "already installed"
        meta = requests.get(f"https://marketplace.dify.ai/api/v1/plugins/{PLUGIN}", timeout=30).json()
        ident = meta["data"]["plugin"]["latest_package_identifier"]
        r = console("POST", "/workspaces/current/plugin/install/marketplace",
                    json={"plugin_unique_identifiers": [ident]})
        if not r.get("all_installed"):
            tid = r["task_id"]

            def done():
                t = console("GET", f"/workspaces/current/plugin/tasks/{tid}")["task"]
                if t["status"] == "failed":
                    raise RuntimeError(f"install failed: {t}")
                return t["status"] == "success"
            wait_for(done, 600, 5, "plugin installed")
        return ident
    if not check("model: the plugin daemon installs the OpenAI-API-compatible plugin", install):
        return

    def creds(model, model_type, extra):
        console("POST", f"/workspaces/current/model-providers/{PROVIDER}/models/credentials",
                json={"model": model, "model_type": model_type, "name": f"stub-{model_type}",
                      "credentials": {"endpoint_url": ARGS.stub_url, **extra}})

    def add_models():
        creds("stub-chat", "llm", {"mode": "chat", "context_size": "4096", "max_tokens_to_sample": "512",
                                   "function_calling_type": "no_call", "stream_function_calling": "not_supported",
                                   "vision_support": "no_support", "structured_output_support": "not_supported",
                                   "stream_mode_delimiter": "\n\n"})
        creds("stub-embed", "text-embedding", {"context_size": "512", "max_chunks": "16"})
    if not check("model: add the stub's chat and embedding models", add_models):
        return

    def llm_chat():
        doc = yaml.safe_load(fixture("basic_chatflow.yml"))
        for n in doc["workflow"]["graph"]["nodes"]:
            if n["data"].get("type") == "llm":
                n["data"]["model"].update({"provider": PROVIDER, "name": "stub-chat"})
        app_id, key = import_app(yaml.safe_dump(doc, allow_unicode=True))
        ev = sse("/chat-messages", key, {"inputs": {}, "query": "hello from the harness",
                                         "response_mode": "streaming", "user": USER}, timeout=180)
        answer = "".join(e.get("answer", "") for e in ev if e.get("event") == "message")
        errs = [e for e in ev if e.get("event") == "error"]
        need(answer.startswith("stub answer: hello from the harness"), f"answer {answer!r} errors {errs[:1]}")
    check("model: a chatflow's LLM node streams the stub model's answer", llm_chat)

    def hq_dataset():
        dkey = STATE.get("dkey") or console("POST", "/datasets/api-keys")["token"]
        ds = service("POST", "/datasets", dkey, json={
            "name": f"hq-{uuid.uuid4().hex[:6]}", "indexing_technique": "high_quality",
            "embedding_model": "stub-embed", "embedding_model_provider": PROVIDER}).json()["id"]
        r = service("POST", f"/datasets/{ds}/document/create-by-text", dkey,
                    json={"name": "hq.txt", "text": DOC_TEXT, "indexing_technique": "high_quality",
                          "process_rule": {"mode": "automatic"}}).json()
        wait_indexed(dkey, ds, r["batch"], timeout=300)
        hits = service("POST", f"/datasets/{ds}/retrieve", dkey, json={"query": "aurora borealis poles"}).json()
        need(any("aurora" in x["segment"]["content"] for x in hits.get("records", [])), f"records {hits}")
    check("model: a high-quality knowledge base is embedded by Celery and searched", hq_dataset)


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
    check("server: no command Dify sent was refused (INFO commandstats)", none_refused)


# ---------------------------------------------------------------- main
def main():
    global ARGS
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--label", required=True)
    ap.add_argument("--base", default="http://127.0.0.1")
    ap.add_argument("--compose-dir", required=True)
    ap.add_argument("--compose-file", action="append", required=True)
    ap.add_argument("--dify-src", default=os.path.expanduser("~/dify"))
    ap.add_argument("--redis-password", default="difyai123456")
    ap.add_argument("--stub-url", default="", help="the stub model as the plugin daemon reaches it")
    ap.add_argument("--image", default="", help="the server image, recorded in the results")
    ap.add_argument("--groups", default="", help="comma-separated subset, for debugging (account always runs)")
    ARGS = ap.parse_args()

    groups = [("connection", g_connection), ("start-up", g_startup), ("account", g_account),
              ("workflow", g_workflow), ("chatflow", g_chatflow), ("knowledge", g_knowledge),
              ("restart", g_restart), ("model", g_model), ("server", g_refused)]
    only = set(filter(None, ARGS.groups.split(",")))
    wait_for(api_ready, 600, 5, "Dify API")
    for title, fn in groups:
        if not only or title in only or title == "account":
            run_group(title, fn)

    stats = {}
    try:
        stats = server().info("commandstats")
        errs = server().info("errorstats")
    except Exception as e:  # noqa
        errs = {"error": str(e)}
    counts = {s: sum(1 for r in results if r["status"] == s) for s in ("PASS", "FAIL", "WARN", "SKIP")}
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"results-{ARGS.label}.json")
    dify_version = ""
    try:
        dify_version = subprocess.run(["git", "-C", ARGS.dify_src, "describe", "--tags", "--always"],
                                      capture_output=True, text=True).stdout.strip()
    except Exception:  # noqa
        pass
    with open(out, "w", encoding="utf-8") as f:
        json.dump({"label": ARGS.label, "image": ARGS.image, "redis_version": STATE.get("server_version"),
                   "dify": dify_version, "counts": counts, "checks": results,
                   "commands_at_startup": {k: {"calls": v.get("calls"), "failed": v.get("failed_calls"),
                                                "rejected": v.get("rejected_calls")}
                                            for k, v in sorted(STATE.get("startup_commandstats", {}).items())},
                   "commands_after_run": {k: {"calls": v.get("calls"), "failed": v.get("failed_calls"),
                                               "rejected": v.get("rejected_calls")}
                                           for k, v in sorted(stats.items())},
                   "errorstats": errs,
                   "when": datetime.now(timezone.utc).isoformat(timespec="seconds")}, f, indent=2)
    print(f"\n{counts}  -> {out}")
    sys.exit(0 if not counts["FAIL"] else 1)


if __name__ == "__main__":
    main()
