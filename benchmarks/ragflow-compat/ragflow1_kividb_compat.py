#!/usr/bin/env python3
"""
RAGFlow 1.0 <-> Redis-protocol server compatibility harness, end to end.

RAGFlow 1.0 is a Go server. Its Docker deployment has one Redis-protocol service, `kvrocks`
(Apache Kvrocks 2.16.0), used as the cache, for locks and for the agent-run lease; its message
queue is NATS JetStream. This harness drives a running RAGFlow 1.0 stack (its own
docker-compose, with the `kvrocks` service left as Kvrocks or replaced by KiviDB) through the HTTP
API, and records PASS / FAIL / WARN / SKIP per check. No cloud model is called: RAGFlow's
OpenAI-API-compatible provider points at stub_openai.py, a local stand-in for the chat and
embedding models.

  pip install -r requirements.txt
  python ragflow1_kividb_compat.py --label kividb --compose-dir ~/ragflow/docker \
      --compose-file ~/ragflow/docker/docker-compose.yml --compose-file compose.kividb-kvrocks.yaml

Writes results-1.0-<label>.json next to the script. run1.sh brings the stack up and runs this.

What it covers (the harness reads the server directly only where it says so):
  * start-up: the services and the four Go server modes up, and what go-redis sends at connect time
  * accounts: register, login (the token is signed with a secret key RAGFlow keeps in the server),
    an API token
  * models: the OpenAI-API-compatible provider with the stub's chat and embedding models
  * a dataset: text documents uploaded and parsed by the ingestor; chunks; retrieval; an HTML
    document with an inline image, whose image goes through the media cache (a Lua script)
  * chat: an assistant on the dataset, a session, a completion, streamed and not
  * agent: an echo agent run, streamed and not; each run takes the session's lease (Lua scripts)
  * contract: RAGFlow's own Lua scripts and WATCH transaction, read from its source and run
    against the server: lease register / refresh / release / cancel, lease expiry on time (the
    TTL contract older Kvrocks broke), compare-and-delete locks, the token bucket, the media cache
  * restart: with the ingestor paused, parse tasks wait; the server restarts twice; the login
    session still works; the ingestor resumes and parses every queued document
  * no command RAGFlow sent was refused by the server, and RAGFlow logged no server errors

NOTE: the stub model proves the wiring, not a model's quality.
"""
import argparse
import base64
import json
import os
import re
import struct
import subprocess
import sys
import time
import uuid
import zlib
from datetime import datetime, timezone

import redis
import requests

results = []
ARGS = None
STATE = {}
EMAIL = "qa@infiniflow.org"
PROVIDER, INSTANCE = "OpenAI-API-Compatible", "stub"
# Errors a client sees while the server restarts; not refusals.
TRANSIENT = re.compile(r"connection refused|EOF|i/o timeout|broken pipe|connection reset|LOADING|"
                       r"no such host|context deadline|use of closed network connection", re.I)
# Logged at every start whatever the server: the synonym loader is built without a client.
BENIGN = re.compile(r"Realtime synonym is disabled, since no redis connection")


# ---------------------------------------------------------------- check runner
def check(name, fn, level="FAIL"):
    t0 = time.time()
    try:
        detail = fn()
        status = "PASS"
        if isinstance(detail, tuple) and detail and detail[0] in ("SKIP", "WARN"):
            status, detail = detail
    except Exception as e:  # noqa
        detail, status = f"{type(e).__name__}: {str(e)[:400]}", level
    results.append({"check": name, "status": status, "detail": detail or "",
                    "ms": round((time.time() - t0) * 1000)})
    print(f"  [{status:4}] {name}" + (f"  ({detail})" if detail else ""), flush=True)
    return status == "PASS"


def run_group(title, fn):
    print(f"\n== {title}", flush=True)
    try:
        return fn()
    except Exception as e:  # noqa
        detail = f"{type(e).__name__}: {str(e)[:400]}"
        results.append({"check": f"{title}: setup", "status": "FAIL", "detail": detail, "ms": 0})
        print(f"  [FAIL] {title}: setup  ({detail})", flush=True)
        return None


def need(cond, msg):
    if not cond:
        raise AssertionError(msg)


class Fatal(Exception):
    """Raised inside a wait_for condition to stop waiting at once (a document that FAILED)."""


def wait_for(fn, timeout, every=1.0, what="condition"):
    end = time.time() + timeout
    last = None
    while time.time() < end:
        try:
            v = fn()
            if v:
                return v
        except Fatal:
            raise
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


def server(db=None):
    ip = subprocess.run(["docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}",
                         container(ARGS.cache_service)], capture_output=True, text=True, check=True).stdout.split()[0]
    return redis.Redis(host=ip, port=6379, password=ARGS.password, db=ARGS.db if db is None else db,
                       socket_timeout=10)


def commandstats():
    """INFO commandstats as {command: {calls, failed, rejected}}. Kvrocks counts calls only."""
    out = {}
    for k, v in server().info("commandstats").items():
        name = k[len("cmdstat_"):] if k.startswith("cmdstat_") else k
        out[name] = {"calls": v.get("calls"), "failed": v.get("failed_calls"), "rejected": v.get("rejected_calls")}
    return out


def counts_failures(stats):
    return any(v.get("failed") is not None for v in stats.values())


def ingestors(signal):
    """Pause (STOP) or resume (CONT) the ingestor processes in the RAGFlow container."""
    subprocess.run(["docker", "exec", container(ARGS.service), "pkill", f"-{signal}", "-f", "ragflow_server --ingestor"],
                   check=True, capture_output=True)


def restart_server():
    compose("restart", ARGS.cache_service)
    wait_for(lambda: server().ping(), 60, 1, "server back")


def ragflow_logs(since):
    p = subprocess.run(["docker", "logs", "--since", since, container(ARGS.service)], capture_output=True, text=True)
    return (p.stdout + p.stderr).splitlines()


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


def upload(ds, name, data, mime="text/plain"):
    if isinstance(data, str):
        data = data.encode()
    out = api("POST", f"/datasets/{ds}/documents", files=[("file", (name, data, mime))])
    return [d["id"] for d in out]


def docs(ds):
    return {d["id"]: d for d in api("GET", f"/datasets/{ds}/documents?page=1&page_size=100")["docs"]}


def status(doc):
    """RAGFlow 1.0's ingestion_status: UNSTART, CREATED, SCHEDULED, RUNNING, STOPPING, STOPPED, COMPLETED, FAILED."""
    return doc.get("ingestion_status")


def wait_parsed(ds, ids, timeout=300):
    def done():
        d = docs(ds)
        st = {i: status(d[i]) for i in ids}
        if any(s in ("FAILED", "STOPPED") for s in st.values()):
            raise Fatal(f"parse failed: {[(d[i]['name'], status(d[i]), (d[i].get('latest_ingestion_event') or {}).get('message', '')[-300:]) for i in ids]}")
        return all(s == "COMPLETED" for s in st.values())
    wait_for(done, timeout, 2, "documents parsed")


def parse(ds, ids):
    api("POST", f"/datasets/{ds}/documents/parse", json={"document_ids": ids})


def chunks_of(ds, doc):
    return api("GET", f"/datasets/{ds}/documents/{doc}/chunks")["chunks"]


def png(w=48, h=48):
    """A small RGB PNG with a gradient, made without any imaging library."""
    raw = b"".join(b"\x00" + b"".join(bytes((x * 5 % 256, y * 5 % 256, 128)) for x in range(w)) for y in range(h))

    def chunk(t, d):
        return struct.pack(">I", len(d)) + t + d + struct.pack(">I", zlib.crc32(t + d) & 0xffffffff)
    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", w, h, 8, 2, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(raw, 9)) + chunk(b"IEND", b""))


DOC1 = ("KiviDB compatibility note for RAGFlow. The aurora borealis appears near the magnetic poles. "
        "The ingestor parses this document and RAGFlow caches its work in the Redis-protocol server.")
DOC2 = "Quasar telescopes measure the redshift of distant galaxies and their spectral lines."


def html_doc():
    img = base64.b64encode(png()).decode()
    return (f"<html><head><title>Comet</title></head><body><h1>Comet tails</h1>"
            f"<p>A comet tail always points away from the sun because of the solar wind.</p>"
            f"<p><img alt=\"comet diagram\" src=\"data:image/png;base64,{img}\"></p>"
            f"<p>The coma surrounds the nucleus of the comet.</p></body></html>")


# ---------------------------------------------------------------- RAGFlow's scripts, from its source
def go_scripts(rel):
    src = open(os.path.join(ARGS.ragflow_src, rel), encoding="utf-8").read()
    return [s for s in re.findall(r"`([^`]*)`", src) if "redis.call" in s]


def ragflow_scripts():
    tracker = go_scripts("internal/agent/canvas/run_tracker.go")
    need(len(tracker) == 4, f"expected 4 scripts in run_tracker.go, found {len(tracker)}")
    kv = go_scripts("internal/engine/kvrocks/redis.go")
    need(len(kv) == 3, f"expected 3 scripts in kvrocks/redis.go, found {len(kv)}")
    reg, refresh, release, cancel = tracker
    need("EXISTS" in reg and "PEXPIRE" in refresh and "DEL" in release and "PX" in cancel, "unexpected script order")
    return {"register": reg, "refresh": refresh, "release": release, "cancel": cancel,
            "delete_if_equal": kv[0], "token_bucket": kv[1], "media_put": kv[2]}


# ---------------------------------------------------------------- groups
def g_connection():
    def ping():
        r = server()
        need(r.ping(), "no PONG")
        STATE["server_version"] = r.info("server").get("redis_version") or r.info("server").get("version")
        return f"redis_version {STATE['server_version']}"
    check("connection: PING through RAGFlow's password, database 1", ping)


def g_startup():
    def services_up():
        rows = [json.loads(line) for line in compose("ps", "-a", "--format", "json").splitlines() if line.strip()]
        bad = [f"{r['Service']}: {r['Status']}" for r in rows if not r["Status"].startswith("Up")]
        need(not bad, "; ".join(bad))
        return f"{len(rows)} containers: " + ", ".join(sorted(r["Service"] for r in rows))
    check("start-up: every RAGFlow service is running", services_up)

    def modes():
        ps = subprocess.run(["docker", "exec", container(ARGS.service), "ps", "-eo", "args"],
                            capture_output=True, text=True).stdout
        got = sorted(set(re.findall(r"ragflow_server --(api|admin|syncer|ingestor)", ps)))
        need(got == ["admin", "api", "ingestor", "syncer"], f"running: {got}")
        return "api, admin, syncer, ingestor"
    check("start-up: the Go server's four modes are running (each connects to the server at start)", modes)

    def startup_commands():
        stats = commandstats()
        STATE["startup_commandstats"] = stats
        conn = {k: v for k, v in stats.items() if re.match(r"(client|info|config|hello|select|auth|ping)", k)}
        summary = ", ".join(f"{k} x{v['calls']}" for k, v in sorted(conn.items()))
        need(conn.get("select") or conn.get("hello"), f"no SELECT/HELLO seen: {summary}")
        if not counts_failures(stats):
            return ("WARN", "the server does not count refused calls; seen: " + summary)
        # go-redis 9.18 sends CLIENT MAINT_NOTIFICATIONS on a RESP3 connection and turns it off when
        # it is refused. Redis 8.10.3 refuses it too, as an unknown CLIENT subcommand (which Redis
        # counts in errorstats only, so it is not in Redis's commandstats); it says nothing about a
        # server.
        bad = {k: v for k, v in conn.items() if (v.get("failed") or v.get("rejected"))
               and k != "client|maint_notifications"}
        need(not bad, "refused: " + ", ".join(bad) + f"; all: {summary}")
        return summary
    check("start-up: HELLO / CLIENT / SELECT / AUTH / PING / INFO sent at connect are all accepted", startup_commands)


def g_account():
    def register():
        r = requests.post(f"{ARGS.base}/api/v1/users", json={"email": EMAIL, "nickname": "qa", "password": password()},
                          timeout=60).json()
        need(r.get("code") == 0 or "already" in str(r.get("message")).lower(), r)
    check("account: register", register)

    def login():
        r = requests.post(f"{ARGS.base}/api/v1/auth/login", json={"email": EMAIL, "password": password()}, timeout=60)
        need(r.json().get("code") == 0, r.text[:200])
        STATE["auth"] = r.headers["Authorization"]
        key = server().get("ragflow:system:secret_key")
        need(key, "no ragflow:system:secret_key in database 1")
        STATE["secret_key"] = key
        return "token signed with the secret key RAGFlow stored in the server"
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
    ds = api("POST", "/datasets", json={"name": f"compat-{uuid.uuid4().hex[:6]}", "parse_type": 1, "parser_id": "naive"})["id"]
    STATE["ds"] = ds
    print(f"  (dataset {ds})")

    def parse_text():
        ids = upload(ds, "aurora.txt", DOC1) + upload(ds, "quasar.txt", DOC2)
        STATE["ids"] = ids
        parse(ds, ids)
        wait_parsed(ds, ids)
        return f"{len(ids)} documents COMPLETED"
    check("dataset: two uploaded documents are parsed by the ingestor", parse_text)

    def chunks():
        n = sum(len(chunks_of(ds, i)) for i in STATE["ids"])
        need(n >= 2, f"{n} chunks")
        return f"{n} chunks"
    check("dataset: the documents have chunks", chunks)

    def retrieval():
        d = api("POST", "/retrieval", json={"question": "aurora borealis magnetic poles", "dataset_ids": [ds]})
        hits = [c.get("content", "") for c in d.get("chunks", [])]
        need(any("aurora" in h for h in hits), f"chunks {hits[:2]}")
    check("dataset: retrieval finds the right chunk", retrieval)

    def html_image():
        before = commandstats()
        ids = upload(ds, "comet.html", html_doc(), "text/html")
        parse(ds, ids)
        wait_parsed(ds, ids)
        after = commandstats()
        n = sum(len(chunks_of(ds, i)) for i in ids)
        need(n >= 1, f"{n} chunks")
        scripts = sum((after.get(c, {}).get("calls") or 0) - (before.get(c, {}).get("calls") or 0)
                      for c in ("eval", "evalsha"))
        need(scripts > 0, "no script ran: the image did not reach the media cache")
        return f"COMPLETED, {n} chunk(s), {scripts} script call(s) (the media cache's put)"
    check("dataset: an HTML document with an inline image is parsed (its image goes through the media cache)",
          html_image)


def g_chat():
    ds = STATE["ds"]
    chat = api("POST", "/chats", json={"name": f"compat-{uuid.uuid4().hex[:6]}", "dataset_ids": [ds]})["id"]
    sess = api("POST", f"/chats/{chat}/sessions", json={"name": "compat"})["id"]

    def completion():
        d = api("POST", "/chat/completions", json={"chat_id": chat, "session_id": sess, "stream": False,
                                                   "messages": [{"role": "user", "content": "tell me about the aurora"}]})
        need("stub answer" in json.dumps(d), f"answer {str(d)[:300]}")
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


def echo_dsl():
    return {"components": {
        "begin": {"obj": {"component_name": "Begin", "params": {}}, "downstream": ["message"], "upstream": []},
        "message": {"obj": {"component_name": "Message", "params": {"content": ["{sys.query}"]}},
                    "downstream": [], "upstream": ["begin"]}},
        "history": [], "retrieval": [], "path": [], "variables": {},
        "globals": {"sys.query": "", "sys.user_id": "", "sys.conversation_turns": 0, "sys.files": []}}


def g_agent():
    agent = api("POST", "/agents", json={"title": f"compat-{uuid.uuid4().hex[:6]}", "dsl": echo_dsl()})["id"]
    sess = api("POST", f"/agents/{agent}/sessions", json={"name": "compat"})["id"]
    STATE["agent"], STATE["agent_session"] = agent, sess

    def run():
        before = commandstats()
        q = f"echo-{uuid.uuid4().hex[:8]}"
        d = api("POST", "/agents/chat/completions", json={"agent_id": agent, "session_id": sess, "stream": False,
                                                          "query": q, "return_trace": True})
        need(json.dumps(d).count(q) >= 1 and d.get("event") == "message_end", f"answer {str(d)[:300]}")
        after = commandstats()
        scripts = sum((after.get(c, {}).get("calls") or 0) - (before.get(c, {}).get("calls") or 0)
                      for c in ("eval", "evalsha"))
        need(scripts > 0, "no script ran")
        need(not server().exists(f"agent:active-session:{sess}"), "the session's lease was not released")
        return f"echoed, {scripts} script call(s) (lease register / release), lease released"
    check("agent: an echo agent run answers, and takes and releases the session's lease", run)

    def streamed():
        q = f"echo-{uuid.uuid4().hex[:8]}"
        got = []
        with requests.post(f"{ARGS.base}/api/v1/agents/chat/completions",
                           headers={"Authorization": f"Bearer {STATE['token']}"},
                           json={"agent_id": agent, "session_id": sess, "stream": True, "query": q},
                           stream=True, timeout=120) as r:
            for line in r.iter_lines(decode_unicode=True):
                if line and line.startswith("data:"):
                    got.append(line[5:])
        need(any(q in g for g in got), f"{len(got)} events: {str(got)[:300]}")
        persisted = api("GET", f"/agents/{agent}/sessions/{sess}")
        msgs = persisted.get("message") or []
        need(msgs and msgs[-1].get("content") == q, f"last message {str(msgs[-1:])[:200]}")
        return f"{len(got)} events, the answer persisted in the session"
    check("agent: a streamed run, and the conversation is persisted", streamed)


def g_contract():
    """RAGFlow's own Lua scripts and WATCH transaction, run against the server the way its Go code runs them."""
    s = ragflow_scripts()
    r = server()
    p = f"compat:{uuid.uuid4().hex[:8]}:"
    lease, mark = p + "agent:active-session:s1", p + "agent:cancel:s1"

    def ev(name, keys, *args):
        return r.eval(s[name], len(keys), *keys, *args)

    def reg(token, ttl_ms):
        return ev("register", [lease, mark], "s1", token, "u1", "c1", "r1", str(int(time.time() * 1000)), str(ttl_ms))

    def lease_cas():
        need(reg("A", 30000) == 1, "first register refused")
        pttl = r.pttl(lease)
        need(0 < pttl <= 30000, f"PTTL {pttl}")
        need(reg("B", 30000) == 0, "a second owner registered while the lease was held")
        need(r.hget(lease, "token") == b"A", "token overwritten")
        need(ev("refresh", [lease], "B", "30000") == 0, "a wrong token refreshed the lease")
        r.pexpire(lease, 5000)
        need(ev("refresh", [lease], "A", "30000") == 1, "the owner could not refresh")
        need(r.pttl(lease) > 25000, f"refresh did not extend: PTTL {r.pttl(lease)}")
        need(ev("cancel", [lease, mark], "B", "86400000") == 0, "a wrong token set the cancel marker")
        need(ev("cancel", [lease, mark], "A", "86400000") == 1, "the owner could not set the cancel marker")
        need(r.pttl(mark) > 0, f"cancel marker PTTL {r.pttl(mark)}")
        need(ev("release", [lease, mark], "B") == 0 and r.exists(lease), "a wrong token released the lease")
        need(ev("release", [lease, mark], "A") == 1, "the owner could not release")
        need(not r.exists(lease) and not r.exists(mark), "release left keys behind")
        need(reg("B", 30000) == 1, "re-register after release refused")
        ev("release", [lease, mark], "B")
        return "register is exclusive; refresh, cancel and release only by the owner's token"
    check("contract: the agent-run lease (RunTracker's 4 scripts) is single-owner", lease_cas)

    def lease_expiry():
        need(reg("A", 300) == 1, "register refused")
        time.sleep(0.45)
        need(not r.exists(lease), "the lease still exists 150 ms after its expiry")
        need(reg("B", 30000) == 1, "a new owner could not take an expired lease")
        ev("release", [lease, mark], "B")
        k = p + "px"
        r.set(k, "x", px=300)
        time.sleep(0.45)
        need(r.get(k) is None, "SET PX 300 still readable after 450 ms")
        return "an expired lease is gone on time and can be taken (the TTL contract)"
    check("contract: a lease expires on time and a new owner can take it", lease_expiry)

    def lock():
        k = p + "lock"
        need(r.set(k, "owner-1", nx=True, px=60000), "SET NX PX refused")
        need(not r.set(k, "owner-2", nx=True, px=60000), "SET NX overwrote a held lock")
        need(ev("delete_if_equal", [k], "owner-2") == 0 and r.exists(k), "another owner deleted the lock")
        need(ev("delete_if_equal", [k], "owner-1") == 1 and not r.exists(k), "the owner could not delete the lock")
        return "SET NX PX, then compare-and-delete"
    check("contract: the distributed lock (SET NX PX + delete_if_equal)", lock)

    def bucket():
        k = p + "bucket"
        now = time.time()
        got = [ev("token_bucket", [k], "2", "0.001", repr(now), "1")[0] for _ in range(3)]
        need(got == [1, 1, 0], f"allowed {got}, want [1, 1, 0]")
        need(r.ttl(k) > 0, "no expiry on the bucket")
        return "capacity 2: allow, allow, deny; the bucket has an expiry"
    check("contract: the token-bucket rate limit (webhooks)", bucket)

    def media():
        mk, dk, sk, man = p + "media:d1", p + "media:digests", p + "media:state", p + "media:manifest"
        first = ev("media_put", [mk, dk, sk], "d1", "IMG1", "2", "100", "600", man, mk)
        again = ev("media_put", [mk, dk, sk], "d1", "IMG1", "2", "100", "600", man, mk)
        ev("media_put", [p + "media:d2", dk, sk], "d2", "IMG2", "2", "100", "600", man, p + "media:d2")
        full = ev("media_put", [p + "media:d3", dk, sk], "d3", "IMG3", "2", "100", "600", man, p + "media:d3")
        need(first == [2, 1] and again == [1, 0] and full == [0, 0], f"replies {first} {again} {full}")
        need(r.get(mk) == b"IMG1" and 0 < r.ttl(mk) <= 600 and r.hget(sk, "bytes") == b"8", "state wrong")
        need(r.smembers(man) == {mk.encode(), (p + "media:d2").encode()}, f"manifest {r.smembers(man)}")
        return "stored, deduplicated, and refused past the image quota"
    check("contract: the media cache put (images during ingestion)", media)

    def watch():
        k = p + "trace"
        other = server()
        with r.pipeline() as pipe:
            pipe.watch(k)
            pipe.get(k)
            other.set(k, "changed")
            pipe.multi()
            pipe.set(k, "mine", ex=60)
            try:
                pipe.execute()
                raise AssertionError("EXEC ran although the watched key changed")
            except redis.WatchError:
                pass
        with r.pipeline() as pipe:
            pipe.watch(k)
            pipe.get(k)
            pipe.multi()
            pipe.set(k, "mine", ex=60)
            pipe.execute()
        need(r.get(k) == b"mine", "second transaction did not apply")
        return "a changed key aborts EXEC; an unchanged one commits"
    check("contract: WATCH / MULTI / EXEC optimistic update (webhook traces)", watch)
    for k in r.scan_iter(match=p + "*", count=1000):
        r.delete(k)


def g_restart():
    ds = STATE["ds"]
    ids = []
    since = datetime.now(timezone.utc).isoformat()
    ingestors("STOP")
    try:
        def queue():
            restart_server()
            wait_for(api_ready, 120, 2, "API back")
            for i in range(3):
                ids.extend(upload(ds, f"queued-{i}.txt", f"queued document {i} about nebula number {i}."))
            wait_for(lambda: (parse(ds, ids) or True), 60, 3, "parse request accepted")
            time.sleep(5)
            st = {i: status(docs(ds)[i]) for i in ids}
            need(all(v != "COMPLETED" for v in st.values()), f"parsed while the ingestor was paused: {st}")
            return f"3 documents waiting: {sorted(set(st.values()))}"
        check("restart: with the ingestor paused, 3 parse tasks wait", queue)

        def restart_again():
            restart_server()
            wait_for(api_ready, 120, 2, "API back")
            return "restarted"
        check("restart: the server restarts again with the tasks waiting", restart_again)

        def session():
            key = server().get("ragflow:system:secret_key")
            need(key == STATE.get("secret_key"), "the secret key changed: login sessions were invalidated")
            tries = []

            def me():
                tries.append(time.time())
                return api("GET", "/users/me", auth=STATE["auth"])
            # The first request after a restart can meet a pooled connection the restart closed.
            wait_for(me, 30, 2, "the login token accepted")
            return (f"the secret key survived two restarts; the login token still works"
                    + (f" (accepted on try {len(tries)})" if len(tries) > 1 else ""))
        check("restart: the login session survives the restarts (the secret key is kept)", session)
    finally:
        ingestors("CONT")

    def drained():
        wait_parsed(ds, ids, timeout=420)
        return f"{len(ids)} documents COMPLETED"
    check("restart: the ingestor resumes and parses every queued document", drained)

    def after():
        d = api("POST", "/retrieval", json={"question": "nebula number 2", "dataset_ids": [ds]})
        need(any("nebula" in c.get("content", "") for c in d.get("chunks", [])), "not found")
    check("restart: retrieval finds a document parsed after the restarts", after)

    def agent_after():
        q = f"echo-{uuid.uuid4().hex[:8]}"
        d = api("POST", "/agents/chat/completions", json={"agent_id": STATE["agent"], "session_id": STATE["agent_session"],
                                                          "stream": False, "query": q})
        need(q in json.dumps(d), f"answer {str(d)[:200]}")
    check("restart: an agent run after the restarts", agent_after)
    STATE["restart_since"] = since


def g_refused():
    def none_refused():
        stats = commandstats()
        if not counts_failures(stats):
            return ("SKIP", "the server does not count refused calls (see the log check)")
        noscript = server().info("errorstats").get("errorstat_NOSCRIPT", {}).get("count", 0)
        bad = {k: v for k, v in stats.items() if (v.get("failed") or v.get("rejected"))
               and k != "client|maint_notifications"}
        # go-redis runs a script with EVALSHA first, and on NOSCRIPT (the server does not have it
        # cached yet, or no longer has it after a restart) runs it with EVAL. Those EVALSHA replies
        # are counted as failed calls on Redis too.
        ev = bad.get("evalsha")
        if ev and not ev.get("rejected") and (ev.get("failed") or 0) <= noscript:
            del bad["evalsha"]
        need(not bad, ", ".join(f"{k}: failed {v.get('failed')}, rejected {v.get('rejected')}" for k, v in bad.items()))
        if noscript:
            return f"{noscript} EVALSHA answered NOSCRIPT, then run with EVAL, as go-redis does"
    check("server: no command RAGFlow sent was refused (INFO commandstats)", none_refused)

    def logs():
        lines = ragflow_logs(STATE["t0"])
        pat = re.compile(r"(kvrocks|redis|lua|script|NOSCRIPT|unknown command|ERR )", re.I)
        lvl = re.compile(r"\t(warn|error|fatal|panic)\t|\b(WARN|ERROR|FATAL|PANIC)\b")
        hits = [l for l in lines if pat.search(l) and lvl.search(l) and not BENIGN.search(l)]
        transient = [l for l in hits if TRANSIENT.search(l)]
        real = [l for l in hits if not TRANSIENT.search(l)]
        STATE["log_hits"] = real[:40]
        need(not real, f"{len(real)} line(s), e.g. {real[0][-300:] if real else ''}")
        return f"none ({len(transient)} connection error(s) while the server restarted)"
    check("server: RAGFlow logged no server errors (warn/error lines about the cache)", logs)


# ---------------------------------------------------------------- main
def main():
    global ARGS
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--label", required=True)
    ap.add_argument("--base", default="http://127.0.0.1:9380")
    ap.add_argument("--compose-dir", required=True)
    ap.add_argument("--compose-file", action="append", required=True)
    ap.add_argument("--service", default="ragflow-cpu")
    ap.add_argument("--cache-service", default="kvrocks")
    ap.add_argument("--ragflow-src", default=os.path.expanduser("~/ragflow"))
    ap.add_argument("--password", default="infini_rag_flow")
    ap.add_argument("--db", type=int, default=1)
    ap.add_argument("--stub-url", default="http://stub-llm:9999/v1")
    ap.add_argument("--image", default="")
    ap.add_argument("--groups", default="", help="comma-separated subset, for debugging (account always runs)")
    ap.add_argument("--prepare", action="store_true",
                    help="only register the account and make the stub the default models (before RAGFlow's Go suite)")
    ARGS = ap.parse_args()
    STATE["t0"] = datetime.now(timezone.utc).isoformat()
    if ARGS.prepare:
        wait_for(api_ready, 900, 5, "RAGFlow API")
        g_account()
        g_models()
        sys.exit(0 if all(r["status"] == "PASS" for r in results) else 1)

    groups = [("connection", g_connection), ("start-up", g_startup), ("account", g_account), ("models", g_models),
              ("dataset", g_dataset), ("chat", g_chat), ("agent", g_agent), ("contract", g_contract),
              ("restart", g_restart), ("server", g_refused)]
    only = set(filter(None, ARGS.groups.split(",")))
    wait_for(api_ready, 900, 5, "RAGFlow API")
    for title, fn in groups:
        if not only or title in only or title == "account":
            run_group(title, fn)

    stats, errs, keys = {}, {}, {}
    try:
        # What RAGFlow left in database 1: key prefixes, and which have no expiry.
        r = server()
        for k in r.scan_iter(count=1000):
            name = k.decode(errors="replace")
            prefix = re.sub(r"[0-9a-f]{16,}|\d{6,}", "<id>", name)
            e = keys.setdefault(prefix, {"keys": 0, "no_expiry": 0, "type": r.type(k).decode()})
            e["keys"] += 1
            e["no_expiry"] += r.ttl(k) == -1
    except Exception as e:  # noqa
        keys = {"error": str(e)}
    try:
        stats = commandstats()
        errs = server().info("errorstats") if counts_failures(stats) else {}
    except Exception as e:  # noqa
        errs = {"error": str(e)}
    counts = {s: sum(1 for r in results if r["status"] == s) for s in ("PASS", "FAIL", "WARN", "SKIP")}
    ver = subprocess.run(["git", "-C", ARGS.ragflow_src, "describe", "--tags", "--always"],
                         capture_output=True, text=True).stdout.strip()
    sha = subprocess.run(["git", "-C", ARGS.ragflow_src, "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"results-1.0-{ARGS.label}.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump({"label": ARGS.label, "image": ARGS.image, "redis_version": STATE.get("server_version"),
                   "ragflow": ver, "ragflow_commit": sha, "counts": counts, "checks": results,
                   "commands_at_startup": STATE.get("startup_commandstats", {}),
                   "commands_after_run": stats, "errorstats": errs, "keys_after_run": keys,
                   "log_lines": STATE.get("log_hits", []),
                   "when": datetime.now(timezone.utc).isoformat(timespec="seconds")}, f, indent=2, sort_keys=False)
    print(f"\n{counts}  -> {out}")
    sys.exit(0 if not counts["FAIL"] else 1)


if __name__ == "__main__":
    main()
