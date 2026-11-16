#!/usr/bin/env python3
"""
cognee <-> Redis-protocol server compatibility harness.

Runs cognee's Redis cache adapter (cognee/infrastructure/databases/cache/redis/RedisAdapter.py)
against any server (KiviDB, Redis 8, Valkey) and records PASS / FAIL / WARN per check. No language
model or embedding model is involved: the adapter stores what it is given.

  pip install -r requirements.txt
  python cognee_kividb_compat.py --url redis://HOST:6380 --label kividb
  python cognee_kividb_compat.py --url redis://HOST:6379 --label redis8

Writes results-<label>.json next to the script. Run the same command against stock Redis (for
example the `redis:8` image) with a different --label to get a comparison.

What it covers. With CACHE_BACKEND=redis, cognee keeps these on the server:
  * the session cache: question/answer entries per user and session, their feedback, agent traces,
    session context entries, and the session TTL
  * plain values with an optional TTL (get_value / set_value / delete_value)
  * usage logs (USAGE_LOGGING=true)
  * the shared Ladybug (Kuzu) lock (SHARED_LADYBUG_LOCK=true, or SHARED_KUZU_LOCK): a redis-py Lock,
    whose release runs a Lua script, so it needs a server with Lua (KiviDB's -lua or -full image)
  * prune, which is FLUSHDB of the whole database

cognee's own tests that need a server are run separately: see REPORT.md.

Everything it writes uses random user, session, lock and log names and is removed at the end, except
for the last check, prune, which flushes the database: run it on a server of its own.
"""
import argparse
import asyncio
import json
import logging
import os
import sys
import time
import uuid
from importlib.metadata import version

import redis

logging.disable(logging.CRITICAL)
os.environ.setdefault("TELEMETRY_DISABLED", "1")
os.environ.setdefault("ENV", "dev")

from cognee.infrastructure.databases.cache.redis.RedisAdapter import RedisAdapter  # noqa: E402

results = []
PREFIXES = []
TTL = 604800  # cognee's default session_ttl_seconds


def check(name, fn, level="FAIL"):
    t0 = time.time()
    try:
        detail = fn()
        if asyncio.iscoroutine(detail):
            detail = LOOP.run_until_complete(detail)
        status = "PASS"
    except Exception as e:  # noqa
        detail, status = f"{type(e).__name__}: {str(e)[:220]}", level
    results.append({"check": name, "status": status, "detail": detail or "",
                    "ms": round((time.time() - t0) * 1000)})
    print(f"  [{status:4}] {name}" + (f"  ({detail})" if detail else ""))


def run_group(title, fn, *args):
    print(f"\n== {title}")
    try:
        return fn(*args)
    except Exception as e:  # noqa
        detail = f"{type(e).__name__}: {str(e)[:220]}"
        results.append({"check": f"{title}: setup", "status": "FAIL", "detail": detail, "ms": 0})
        print(f"  [FAIL] {title}: setup  ({detail})")
        return None


def unique(prefix):
    name = f"{prefix}{uuid.uuid4().hex[:8]}"
    PREFIXES.append(name)
    return name


def cleanup(r):
    for prefix in PREFIXES:
        for key in r.scan_iter(match=f"*{prefix}*", count=500):
            r.delete(key)


LOOP = asyncio.new_event_loop()
asyncio.set_event_loop(LOOP)


def adapter(host, port, **kw):
    return RedisAdapter(host=host, port=port, **kw)


# ---------------------------------------------------------------- values
def values(host, port, r):
    a = adapter(host, port)
    key, ttl_key = unique("cgv"), unique("cgvt")

    async def round_trip():
        await a.set_value(key, "hello")
        assert await a.get_value(key) == "hello"
        assert await a.get_value(unique("cgmissing")) is None

    async def with_ttl():
        await a.set_value(ttl_key, "short-lived", ttl=120)
        assert await a.get_value(ttl_key) == "short-lived"
        assert 110 <= r.ttl(ttl_key) <= 120, r.ttl(ttl_key)

    async def delete():
        await a.delete_value(key)
        assert await a.get_value(key) is None and not r.exists(key)

    check("values: set_value and get_value; a missing key is None", round_trip)
    check("values: set_value with a TTL", with_ttl)
    check("values: delete_value", delete)
    LOOP.run_until_complete(a.close())


# ---------------------------------------------------------------- the session cache
def sessions(host, port, r):
    a = adapter(host, port)
    user, other, sid = unique("cgu"), unique("cgo"), "s1"
    skey = f"agent_sessions:{user}:{sid}"
    ids = {}

    async def create():
        for n in (1, 2, 3):
            await a.create_qa_entry(user, sid, f"question {n}?", f"context {n}", f"answer {n}.",
                                    qa_id=f"qa-{n}")
        assert r.llen(skey) == 3, r.llen(skey)
        assert TTL - 5 <= r.ttl(skey) <= TTL, r.ttl(skey)

    async def latest():
        one = await a.get_latest_qa_entries(user, sid, last_n=1)
        assert [e.qa_id for e in one] == ["qa-3"], one
        two = await a.get_latest_qa_entries(user, sid, last_n=2)
        assert [e.qa_id for e in two] == ["qa-2", "qa-3"], two
        assert await a.get_latest_qa_entries(user, "no-such-session", last_n=1) == []

    async def all_and_by_id():
        got = await a.get_all_qa_entries(user, sid)
        assert [e.question for e in got] == ["question 1?", "question 2?", "question 3?"], got
        picked = await a.get_qa_entries_by_ids(user, sid, ["qa-3", "qa-1"])
        assert sorted(e.qa_id for e in picked) == ["qa-1", "qa-3"], picked

    async def update():
        await a.update_qa_entry(user, sid, "qa-2", feedback_text="helpful", feedback_score=5)
        got = {e.qa_id: e for e in await a.get_all_qa_entries(user, sid)}
        assert got["qa-2"].feedback_text == "helpful" and got["qa-2"].feedback_score == 5, got["qa-2"]
        assert got["qa-2"].answer == "answer 2." and len(got) == 3

    async def delete_feedback():
        assert await a.delete_feedback(user, sid, "qa-2")
        got = {e.qa_id: e for e in await a.get_all_qa_entries(user, sid)}
        assert got["qa-2"].feedback_text is None and got["qa-2"].feedback_score is None, got["qa-2"]

    async def delete_entry():
        assert await a.delete_qa_entry(user, sid, "qa-1")
        assert [e.qa_id for e in await a.get_all_qa_entries(user, sid)] == ["qa-2", "qa-3"]

    async def isolation():
        await a.create_qa_entry(other, sid, "their question?", "ctx", "their answer.")
        mine = [e.question for e in await a.get_all_qa_entries(user, sid)]
        assert "their question?" not in mine, mine
        assert len(await a.get_all_qa_entries(other, sid)) == 1

    async def traces():
        for n, status in ((1, "success"), (2, "error")):
            await a.append_agent_trace_step(user, sid, trace_id=f"t-{n}", origin_function="search",
                                            status=status, memory_query=f"query {n}",
                                            session_feedback=f"feedback {n}")
        assert await a.get_agent_trace_count(user, sid) == 2
        got = await a.get_agent_trace_session(user, sid)
        assert [t.status for t in got] == ["success", "error"], got
        assert await a.get_agent_trace_feedback(user, sid, last_n=1) == ["feedback 2"]
        assert TTL - 5 <= r.ttl(f"agent_traces:{user}:{sid}") <= TTL

    async def context():
        await a.create_session_context_entry(user, sid, {"id": "c1", "text": "the user likes tea"})
        await a.create_session_context_entry(user, sid, {"id": "c2", "text": "the user lives in Pune"})
        assert await a.update_session_context_entry(user, sid, "c1", {"text": "the user likes green tea"})
        assert not await a.update_session_context_entry(user, sid, "nope", {"text": "x"})
        got = await a.get_session_context_entries(user, sid)
        assert [e["text"] for e in got] == ["the user likes green tea", "the user lives in Pune"], got
        assert await a.delete_session_context_entry(user, sid, "c2")
        assert [e["id"] for e in await a.get_session_context_entries(user, sid)] == ["c1"]

    async def delete_session():
        assert await a.delete_session(user, sid)
        for k in (skey, f"agent_traces:{user}:{sid}", f"session_context:{user}:{sid}"):
            assert not r.exists(k), f"{k} left behind"
        assert len(await a.get_all_qa_entries(other, sid)) == 1, "another user's session was deleted"

    async def no_ttl():
        b = adapter(host, port, session_ttl_seconds=None)
        u = unique("cgn")
        await b.create_qa_entry(u, sid, "q?", "c", "a.")
        assert r.ttl(f"agent_sessions:{u}:{sid}") == -1, r.ttl(f"agent_sessions:{u}:{sid}")
        await b.close()

    check("sessions: create_qa_entry appends to the session list and sets the 7-day TTL", create)
    check("sessions: get_latest_qa_entries (last 1, last 2, an empty session)", latest)
    check("sessions: get_all_qa_entries keeps the order; get_qa_entries_by_ids", all_and_by_id)
    check("sessions: update_qa_entry adds feedback in place", update)
    check("sessions: delete_feedback", delete_feedback)
    check("sessions: delete_qa_entry", delete_entry)
    check("sessions: another user's session is separate", isolation)
    check("sessions: agent trace steps, count, feedback and TTL", traces)
    check("sessions: session context entries: create, update, list, delete one", context)
    check("sessions: delete_session removes the QA, trace and context lists only", delete_session)
    check("sessions: with session_ttl_seconds=None the session does not expire", no_ttl)
    LOOP.run_until_complete(a.close())


# ---------------------------------------------------------------- usage logs
def usage_logs(host, port, r):
    log_key = unique("cgl")
    a = adapter(host, port, log_key=log_key)
    user = unique("cglu")

    async def log_and_read():
        for n in range(3):
            await a.log_usage(user, {"function": "search", "n": n}, ttl=3600)
        got = await a.get_usage_logs(user, limit=2)
        assert [e["n"] for e in got] == [2, 1], got  # most recent first
        assert 3590 <= r.ttl(f"{log_key}:{user}") <= 3600, r.ttl(f"{log_key}:{user}")

    check("usage logs: log_usage and get_usage_logs (most recent first, with a TTL)", log_and_read)
    LOOP.run_until_complete(a.close())


# ---------------------------------------------------------------- the shared lock
def shared_lock(host, port, r):
    name = unique("cglock")
    holder = adapter(host, port, lock_name=name, timeout=30, blocking_timeout=2)
    waiter = adapter(host, port, lock_name=name, timeout=30, blocking_timeout=1)

    def acquire():
        holder.acquire_lock()
        assert r.exists(name), "the lock key is not on the server"
        assert 0 < r.pttl(name) <= 30000, r.pttl(name)

    def excludes():
        try:
            waiter.acquire_lock()
        except RuntimeError as e:
            assert "Could not acquire" in str(e), e
            return
        raise AssertionError("a second client acquired a held lock")

    def release():
        holder.release_lock()
        assert not r.exists(name), "the lock key is still there after release"

    def hand_over():
        with waiter.hold_lock():
            assert r.exists(name)
        assert not r.exists(name)

    check("lock: acquire_lock sets the key with the lock timeout", acquire)
    check("lock: a second client cannot acquire it while it is held", excludes)
    check("lock: release_lock frees it (redis-py runs a Lua script: EVALSHA)", release)
    check("lock: hold_lock acquires and releases (EVALSHA)", hand_over)
    r.delete(name)


# ---------------------------------------------------------------- prune
def prune(host, port, r):
    a = adapter(host, port)

    async def flush():
        await a.set_value(unique("cgp"), "x")
        await a.prune()
        assert r.dbsize() == 0, r.dbsize()

    check("prune: flushes the database (FLUSHDB)", flush)
    LOOP.run_until_complete(a.close())


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default="redis://localhost:6380")
    ap.add_argument("--label", default="kividb")
    args = ap.parse_args()

    r = redis.Redis.from_url(args.url)
    host, port = r.connection_pool.connection_kwargs["host"], r.connection_pool.connection_kwargs["port"]
    server_version = r.info("server").get("redis_version", "?")
    print(f"server {args.label} at {args.url} (redis_version {server_version})")
    check("connection: RedisAdapter connects (PING)", lambda: LOOP.run_until_complete(adapter(host, port).close()))

    run_group("Plain values", values, host, port, r)
    run_group("The session cache", sessions, host, port, r)
    run_group("Usage logs", usage_logs, host, port, r)
    run_group("The shared Ladybug lock", shared_lock, host, port, r)
    cleanup(r)
    run_group("Prune", prune, host, port, r)

    counts = {s: sum(1 for x in results if x["status"] == s) for s in ("PASS", "WARN", "FAIL")}
    print(f"\n{args.label}: {counts['PASS']} PASS, {counts['WARN']} WARN, {counts['FAIL']} FAIL "
          f"of {len(results)} checks")
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"results-{args.label}.json")
    with open(out, "w") as f:
        json.dump({"label": args.label, "url": args.url, "redis_version": server_version,
                   "cognee": version("cognee"), "redis_py": version("redis"),
                   "counts": counts, "checks": results}, f, indent=2)
    print(f"wrote {out}")
    return 1 if counts["FAIL"] else 0


if __name__ == "__main__":
    sys.exit(main())
