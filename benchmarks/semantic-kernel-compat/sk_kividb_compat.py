#!/usr/bin/env python3
"""
Semantic Kernel (Python) <-> Redis-protocol server compatibility harness.

Runs Semantic Kernel's Redis vector store connector (`semantic_kernel.connectors.redis`) in both of
its modes, JSON documents and hashes, against any server, and records PASS / FAIL / WARN per check.
Vectors are fixed in the harness, so no embedding model or API key is involved.

  pip install -r requirements.txt
  python sk_kividb_compat.py --url redis://HOST:6380 --label kividb --restart-cmd "docker restart kividb"

Writes results-<label>.json next to the script. --restart-cmd is optional: with it, the harness
restarts the server and searches again (the server must persist its data: KiviDB `--aof`, Redis
`--appendonly yes`).

What it covers, for RedisJsonCollection and RedisHashsetCollection:
  ensure_collection_exists / collection_exists (FT.CREATE, FT.INFO), upsert of four records, get by
  key and by several keys, vector search (KNN, nearest first, with scores), vector search with a tag
  filter, a delete, a search after a server restart, and ensure_collection_deleted (FT.DROPINDEX).
"""
import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from importlib.metadata import version
from typing import Annotated

import redis
from semantic_kernel.connectors.redis import RedisHashsetCollection, RedisJsonCollection
from semantic_kernel.data.vector import VectorStoreField, vectorstoremodel

HERE = os.path.dirname(os.path.abspath(__file__))
results = []


@vectorstoremodel
@dataclass
class Note:
    id: Annotated[str, VectorStoreField("key")]
    text: Annotated[str, VectorStoreField("data", is_full_text_indexed=True)]
    tag: Annotated[str, VectorStoreField("data", type="str", is_indexed=True)]
    embedding: Annotated[list[float] | None, VectorStoreField("vector", dimensions=4, type="float")] = None


RECORDS = [
    Note(id="n1", text="reset my password", tag="a", embedding=[1.0, 0.0, 0.0, 0.0]),
    Note(id="n2", text="change my password", tag="b", embedding=[0.9, 0.1, 0.0, 0.0]),
    Note(id="n3", text="track my order", tag="a", embedding=[0.0, 1.0, 0.0, 0.0]),
    Note(id="n4", text="capital of france", tag="b", embedding=[0.0, 0.0, 1.0, 0.0]),
]
QUERY = [1.0, 0.05, 0.0, 0.0]


async def check(name, fn, level="FAIL"):
    t0 = time.time()
    try:
        detail = await fn()
        status = "PASS"
    except Exception as e:  # noqa
        detail, status = f"{type(e).__name__}: {str(e)[:300]}", level
    results.append({"check": name, "status": status, "detail": detail or "",
                    "ms": round((time.time() - t0) * 1000)})
    print(f"  [{status:4}] {name}" + (f"  ({detail})" if detail else ""), flush=True)


def restart(cmd, url):
    subprocess.run(cmd, shell=True, check=True, timeout=120, stdout=subprocess.DEVNULL)
    r = redis.Redis.from_url(url)
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            if r.ping():
                return
        except redis.RedisError:
            time.sleep(0.3)
    raise AssertionError("server did not come back")


async def run_mode(mode, cls, url, restart_cmd):
    name = f"skc_{mode}_{uuid.uuid4().hex[:6]}"
    # Keys are `<collection>:<id>`, as the index's prefix is the collection name.
    mk = lambda: cls[str, Note](record_type=Note, collection_name=name, connection_string=url,
                                prefix_collection_name_to_key_names=True)
    coll = mk()

    async def create():
        await coll.ensure_collection_exists()
        assert await coll.collection_exists()
        return name
    await check(f"{mode}: ensure_collection_exists / collection_exists", create)

    async def upsert():
        keys = await coll.upsert(RECORDS)
        assert sorted(keys) == ["n1", "n2", "n3", "n4"], keys
        return "4 records"
    await check(f"{mode}: upsert", upsert)

    async def get_one():
        rec = await coll.get("n3")
        assert rec is not None and rec.text == "track my order" and rec.tag == "a", rec
        many = await coll.get(["n1", "n4"])
        assert sorted(x.id for x in many) == ["n1", "n4"], many
        return "n3, then n1 and n4"
    await check(f"{mode}: get by key and by keys", get_one)

    async def search(expect=("n1", "n2")):
        res = await coll.search(vector=QUERY, top=2)
        hits = [r async for r in res.results]
        ids = tuple(h.record.id for h in hits)
        assert ids == expect, f"nearest {ids}, expected {expect}"
        return " ".join(f"{h.record.id}={h.score:.4f}" for h in hits)
    await check(f"{mode}: vector search, top 2", search)

    async def filtered():
        res = await coll.search(vector=QUERY, top=2, filter=lambda x: x.tag == "a")
        ids = [r.record.id async for r in res.results]
        assert ids == ["n1", "n3"], ids
        return "tag == a: n1 n3"
    await check(f"{mode}: vector search with a tag filter", filtered)

    deleted = {"ok": False}

    async def delete():
        await coll.delete("n1")
        if await coll.get("n1") is not None:
            # semantic-kernel 1.47.0: RedisJsonCollection._inner_delete calls JSON.DEL on the bare id,
            # without the collection prefix that upsert and get add (connectors/redis.py line 755), so
            # the record stays. The same happens on Redis 8. Hashset mode adds the prefix.
            r = redis.Redis.from_url(url)
            assert r.exists(f"{name}:n1"), "record neither deleted nor at its key"
            raise AssertionError(f"SK left {name}:n1 in place: JSON.DEL went to the unprefixed key n1")
        deleted["ok"] = True
        res = await coll.search(vector=QUERY, top=1)
        ids = [r.record.id async for r in res.results]
        assert ids == ["n2"], ids
        return "n1 gone from get and search"
    await check(f"{mode}: delete", delete, level="WARN" if mode == "json" else "FAIL")

    async def after_restart():
        if not restart_cmd:
            return "skipped: no --restart-cmd"
        restart(restart_cmd, url)
        c2 = mk()
        assert await c2.collection_exists()
        res = await c2.search(vector=QUERY, top=2)
        ids = [r.record.id async for r in res.results]
        expect = ["n2", "n3"] if deleted["ok"] else ["n1", "n2"]
        assert ids == expect, (ids, expect)
        return f"index and records back; nearest {' '.join(ids)}"
    await check(f"{mode}: search after a server restart", after_restart)

    async def drop():
        c3 = mk()
        await c3.ensure_collection_deleted()
        assert not await c3.collection_exists()
        return "dropped"
    await check(f"{mode}: ensure_collection_deleted", drop)


async def amain(a):
    r = redis.Redis.from_url(a.url)
    info = r.info("server")
    meta = {"label": a.label, "date": time.strftime("%Y-%m-%d"), "semantic_kernel": version("semantic-kernel"),
            "redis_py": version("redis"), "redisvl": version("redisvl"),
            "server": {k: info[k] for k in ("redis_version", "kividb_version") if k in info}}
    print(f"== {a.label}: {meta['server']}  semantic-kernel {meta['semantic_kernel']} redisvl {meta['redisvl']}")
    await run_mode("json", RedisJsonCollection, a.url, a.restart_cmd)
    await run_mode("hashset", RedisHashsetCollection, a.url, a.restart_cmd)
    json.dump({"meta": meta, "results": results}, open(os.path.join(HERE, f"results-{a.label}.json"), "w"), indent=2)
    n = lambda s: sum(1 for x in results if x["status"] == s)
    print(f"\nPASS {n('PASS')}  WARN {n('WARN')}  FAIL {n('FAIL')}  -> results-{a.label}.json")
    return n("FAIL")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--label", default="target")
    ap.add_argument("--restart-cmd", default="")
    sys.exit(1 if asyncio.run(amain(ap.parse_args())) else 0)


if __name__ == "__main__":
    main()
