#!/usr/bin/env python3
"""
OpenViking's Redis expiry scheduler <-> Redis-protocol server compatibility harness.

OpenViking's own tests cover its other Redis parts against a real server (run.sh runs them), but
not the expiry scheduler (openviking/scheduling/expiry), which fires session, compile and TTL
callbacks when items fall due. Its store is Lua scripts over sorted sets and hashes. This drives the
scheduler's public API, ExpiryRuntime, the way OpenViking's service does, and records PASS / FAIL.

  pip install "openviking==0.5.3"
  python openviking_expiry_compat.py --url redis://HOST:6380 --label kividb

Writes expiry-<label>.json next to the script. The scheduler works in whole minutes (its
precision is fixed at 60 s), so a run takes three to five minutes.

Everything it writes is under a random service name (base64 in the key names), removed at the end.
"""
import argparse
import asyncio
import json
import os
import sys
import time
import uuid
from importlib.metadata import version

import redis

from openviking.scheduling.expiry import (
    CallbackResult,
    CallbackStatus,
    ExpiryRegistration,
    HandlerOptions,
    ItemIdentity,
)
from openviking.scheduling.expiry.api import TTL_ITEM_TYPE
from openviking.scheduling.expiry.config import ExpiryConfig
from openviking.scheduling.expiry.keys import encode_component
from openviking.scheduling.expiry.runtime import ExpiryRuntime, ExpiryRuntimeRoles

results = []


def record(name, ok, detail=""):
    status = "PASS" if ok else "FAIL"
    results.append({"check": name, "status": status, "detail": detail})
    print(f"  [{status}] {name}" + (f"  ({detail})" if detail else ""))


async def check(name, coro):
    try:
        detail = await coro
        record(name, True, detail or "")
    except Exception as e:  # noqa: BLE001
        record(name, False, f"{type(e).__name__}: {str(e)[:300]}")


class Recorder:
    """The callback handler: counts calls per item; RETRY once for items whose id says so."""

    def __init__(self, who):
        self.who = who
        self.calls = {}

    async def handle(self, request):
        local = request.identity.local_id
        self.calls[local] = self.calls.get(local, 0) + 1
        if local.startswith("retry-") and self.calls[local] == 1:
            return CallbackResult(status=CallbackStatus.RETRY, reason="harness: try again")
        return CallbackResult(status=CallbackStatus.COMPLETED, reason="harness: done")


def config(url, service):
    return ExpiryConfig(
        service_name=service,
        publisher_separate_process=False,
        shard_count=4,
        processing_timeout_seconds=10,
        callback_poll_seconds=0.2,
        shutdown_timeout_seconds=10,
        redis={"mode": "standalone", "endpoints": [url]},
    )


async def runtime(url, service, recorder):
    rt = ExpiryRuntime(config(url, service), roles=ExpiryRuntimeRoles(report=True, publish=True, consume=True))
    rt.scheduler.register_handler(TTL_ITEM_TYPE, recorder, options=HandlerOptions(concurrency=4, max_attempts=3))
    await rt.start_storage()
    await rt.start_roles()
    return rt


def ident(local):
    return ItemIdentity(item_type=TTL_ITEM_TYPE, account_id="compat", user_id="harness", local_id=local)


async def report(rt, local, in_ms=1000):
    return await rt.scheduler.report(ExpiryRegistration(
        identity=ident(local), due_at_ms=int(time.time() * 1000) + in_ms, timeout_seconds=60))


async def wait_until(pred, timeout_s):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if pred():
            return True
        await asyncio.sleep(0.5)
    return pred()


def total(recorders, local):
    return sum(r.calls.get(local, 0) for r in recorders)


async def main_async(url, label):
    run = uuid.uuid4().hex[:8]
    s1, s2 = f"compat-{run}-a", f"compat-{run}-b"
    r = redis.Redis.from_url(url)
    server_version = r.info("server").get("redis_version", "?")
    print(f"server {label} at {url} (redis_version {server_version}), service names {s1}, {s2}")

    # Two runtimes on one service name: the shard leases and claims decide who calls back.
    rec_a, rec_b = Recorder("a"), Recorder("b")
    a = b = None

    async def start_two():
        nonlocal a, b
        a = await runtime(url, s1, rec_a)
        b = await runtime(url, s1, rec_b)
        keys = [k.decode() for k in r.scan_iter(match=f"*{encode_component(s1)}*", count=1000)]
        assert keys, "no keys under the service name"
        return f"{len(keys)} keys"

    await check("two runtimes start on one service name (storage, publisher, consumer)", start_two())
    if a is None or b is None:
        return finish(label, url, server_version, r, [s1, s2])

    plain = [f"item-{i}" for i in range(10)]

    async def report_all():
        for local in plain + ["retry-1", "gone-1"]:
            res = await report(a if local.endswith(("0", "2", "4", "6", "8")) else b, local)
            assert res.accepted_due_at_ms > 0, res
        await a.scheduler.unregister(ident("gone-1"))

    await check("report 12 items due in 1 s, then unregister one of them", report_all())

    async def fired():
        recs = [rec_a, rec_b]
        ok = await wait_until(lambda: all(total(recs, x) >= 1 for x in plain) and total(recs, "retry-1") >= 2, 240)
        missing = [x for x in plain if total(recs, x) == 0]
        assert ok, f"not called back within 240 s: {missing}, retry-1 {total(recs, 'retry-1')} calls"
        await asyncio.sleep(5)  # anything late would be a second call
        twice = {x: total(recs, x) for x in plain if total(recs, x) != 1}
        assert not twice, f"called back more than once: {twice}"
        return f"runtime a: {sum(rec_a.calls.values())} calls, runtime b: {sum(rec_b.calls.values())} calls"

    await check("every item falls due and is called back exactly once, across the two runtimes", fired())

    async def retried():
        n = total([rec_a, rec_b], "retry-1")
        assert n == 2, f"{n} calls"

    await check("a callback that answers RETRY is called again, then completes", retried())

    async def unregistered():
        n = total([rec_a, rec_b], "gone-1")
        assert n == 0, f"called back {n} times"

    await check("an unregistered item is never called back", unregistered())

    async def stop_two():
        await a.close()
        await b.close()

    await check("both runtimes stop cleanly", stop_two())

    # Restart: items reported by one runtime, which then stops, are called back by the next one.
    rec_c, rec_d = Recorder("c"), Recorder("d")
    later = [f"later-{i}" for i in range(3)]

    async def before_restart():
        c = await runtime(url, s2, rec_c)
        for local in later:
            await report(c, local, in_ms=1000)
        await c.close()
        assert not rec_c.calls, f"called back before the restart: {rec_c.calls}"

    await check("a runtime reports 3 items and stops before they fall due", before_restart())

    async def after_restart():
        d = await runtime(url, s2, rec_d)
        try:
            ok = await wait_until(lambda: all(rec_d.calls.get(x, 0) >= 1 for x in later), 240)
            assert ok, f"not called back within 240 s: {rec_d.calls}"
            await asyncio.sleep(3)
            assert all(rec_d.calls[x] == 1 for x in later), rec_d.calls
        finally:
            await d.close()

    await check("a new runtime calls back the items the stopped one left", after_restart())
    return finish(label, url, server_version, r, [s1, s2])


def finish(label, url, server_version, r, services):
    for service in services:
        for key in r.scan_iter(match=f"*{encode_component(service)}*", count=1000):
            r.delete(key)
    counts = {s: sum(1 for x in results if x["status"] == s) for s in ("PASS", "FAIL")}
    print(f"\n{label}: {counts['PASS']} PASS, {counts['FAIL']} FAIL of {len(results)} checks")
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"expiry-{label}.json")
    with open(out, "w") as f:
        json.dump({"label": label, "url": url, "redis_version": server_version,
                   "openviking": version("openviking"), "redis_py": version("redis"),
                   "counts": counts, "checks": results}, f, indent=2)
        f.write("\n")
    print(f"wrote {out}")
    return 1 if counts["FAIL"] else 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default="redis://localhost:6380")
    ap.add_argument("--label", default="kividb")
    args = ap.parse_args()
    return asyncio.run(main_async(args.url, args.label))


if __name__ == "__main__":
    sys.exit(main())
