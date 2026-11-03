#!/usr/bin/env python3
"""
LiteLLM <-> Redis-protocol server compatibility harness.

Runs the Redis features LiteLLM relies on against any server (KiviDB, Redis, Valkey)
and records PASS / FAIL / WARN per check. A local stub plays the LLM provider, so no
real API keys or spend are involved.

  pip install "litellm[proxy]" redis redisvl numpy
  python litellm_kividb_compat.py --url redis://HOST:6379 --label kividb
  python litellm_kividb_compat.py --url redis://HOST:6379 --label kividb --bench

Writes results-<label>.json next to the script. Run the same command against stock
Redis (and Redis Stack) with a different --label to get a comparison.

NOTE: the stub embedding is a hashed bag of words. It proves the wiring (FT.CREATE,
KNN search, hit/miss logic), not semantic quality.
"""
import argparse, hashlib, json, math, os, re, statistics, sys, threading, time, uuid
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

import redis

DIM = 256
results = []


# ---------------------------------------------------------------- stub provider
def embed(text):
    v = [0.0] * DIM
    for tok in re.findall(r"[a-z0-9']+", text.lower()):
        v[int(hashlib.md5(tok.encode()).hexdigest(), 16) % DIM] += 1.0
    n = math.sqrt(sum(x * x for x in v)) or 1.0
    return [x / n for x in v]


class Stub(BaseHTTPRequestHandler):
    counts = {"chat": 0, "embed": 0}

    def log_message(self, *a):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get("content-length", 0))) or b"{}")
        if self.path.endswith("/chat/completions"):
            Stub.counts["chat"] += 1
            resp = {"id": "stub", "object": "chat.completion", "created": 0,
                    "model": body.get("model", "m"),
                    "choices": [{"index": 0, "finish_reason": "stop",
                                 "message": {"role": "assistant",
                                             "content": f"answer-{Stub.counts['chat']}"}}],
                    "usage": {"prompt_tokens": 5, "completion_tokens": 5, "total_tokens": 10}}
        elif self.path.endswith("/embeddings"):
            inp = body["input"]
            inp = [inp] if isinstance(inp, str) else inp
            Stub.counts["embed"] += len(inp)
            resp = {"object": "list", "model": body.get("model", "e"),
                    "data": [{"object": "embedding", "index": i, "embedding": embed(t)}
                             for i, t in enumerate(inp)],
                    "usage": {"prompt_tokens": 1, "total_tokens": 1}}
        else:
            self.send_response(404); self.end_headers(); return
        out = json.dumps(resp).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)


def start_stub():
    srv = ThreadingHTTPServer(("127.0.0.1", 0), Stub)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv.server_address[1]


# ---------------------------------------------------------------- check runner
def check(name, fn, level="FAIL"):
    t0 = time.time()
    try:
        detail = fn()
        status = "PASS"
    except Exception as e:  # noqa
        detail, status = f"{type(e).__name__}: {str(e)[:220]}", level
    results.append({"check": name, "status": status, "detail": detail or "",
                    "ms": round((time.time() - t0) * 1000)})
    print(f"  [{status:4}] {name}" + (f"  ({detail})" if detail else ""))


# ---------------------------------------------------------------- checks
def primitives(r, p):
    def ping():
        assert r.ping()

    def set_get_ex():
        r.set(p + "a", "v", ex=30); assert r.get(p + "a") == b"v"; assert 0 < r.ttl(p + "a") <= 30

    def incr_mget():
        for _ in range(3): r.incr(p + "c")
        r.expire(p + "c", 30)
        assert r.mget(p + "c", p + "missing") == [b"3", None]

    def pipeline_multi():
        pipe = r.pipeline(transaction=True)
        pipe.incr(p + "t"); pipe.expire(p + "t", 30)
        assert pipe.execute() == [1, True]

    def scan_match():
        for i in range(50): r.set(f"{p}scan:{i}", i, ex=30)
        assert len(list(r.scan_iter(match=p + "scan:*", count=100))) == 50

    def set_nx_lock():
        assert r.set(p + "lock", "1", nx=True, ex=10) is True
        assert r.set(p + "lock", "2", nx=True, ex=10) is None

    def hashes():
        r.hset(p + "h", mapping={"a": "1", "b": "2"}); r.expire(p + "h", 30)
        assert r.hgetall(p + "h") == {b"a": b"1", b"b": b"2"}

    def lists():
        r.rpush(p + "l", "x", "y", "z"); r.expire(p + "l", 30)
        assert r.lrange(p + "l", 0, -1) == [b"x", b"y", b"z"]; assert r.lpop(p + "l") == b"x"

    def lua():
        assert r.eval("return redis.call('INCR', KEYS[1])", 1, p + "lua") == 1
        s = r.register_script("return redis.call('INCR', KEYS[1])")
        assert s(keys=[p + "lua"]) == 2
        r.expire(p + "lua", 30)

    def info():
        i = r.info()
        # KiviDB reports redis_version 7.0.15 (compat level) alongside kividb_version.
        return " ".join(f"{k}={i[k]}" for k in ("redis_version", "kividb_version") if k in i) or "ok"

    for n, f in [("PING", ping), ("SET EX / GET / TTL", set_get_ex), ("INCR / EXPIRE / MGET", incr_mget),
                 ("MULTI/EXEC pipeline", pipeline_multi), ("SCAN MATCH", scan_match),
                 ("SET NX EX (locks)", set_nx_lock), ("HSET / HGETALL", hashes),
                 ("RPUSH / LRANGE / LPOP", lists), ("EVAL + EVALSHA (Lua)", lua)]:
        check(n, f)
    check("INFO", info, level="WARN")


def raw_vector(r, p):
    idx = "idx_" + uuid.uuid4().hex[:8]
    pre = f"{p}vec:"

    def create():
        r.execute_command("FT.CREATE", idx, "ON", "HASH", "PREFIX", "1", pre, "SCHEMA",
                          "scope", "TAG", "vec", "VECTOR", "HNSW", "6", "TYPE", "FLOAT32",
                          "DIM", str(DIM), "DISTANCE_METRIC", "COSINE")

    def knn():
        import numpy as np
        for i, t in enumerate(["reset my password", "capital of france", "track my order"]):
            r.hset(f"{pre}{i}", mapping={"scope": "s1", "text": t,
                                         "vec": np.asarray(embed(t), dtype=np.float32).tobytes()})
        q = np.asarray(embed("how do I reset my password"), dtype=np.float32).tobytes()
        res = r.execute_command("FT.SEARCH", idx, "(@scope:{s1})=>[KNN 1 @vec $q AS dist]",
                                "PARAMS", "2", "q", q, "SORTBY", "dist", "RETURN", "2", "text", "dist",
                                "DIALECT", "2")
        # redis-py >= 6 defaults to RESP3, where Redis Stack answers FT.SEARCH with a map;
        # RESP2 (and KiviDB under RESP3) answer with [total, id, [k, v, ...], ...].
        if isinstance(res, dict):
            res = [res[b"total_results"]] + [y for d in res[b"results"]
                                              for y in (d[b"id"], [z for kv in d[b"extra_attributes"].items() for z in kv])]
        flat = [x.decode() if isinstance(x, bytes) else x for x in res[1:] for x in (x if isinstance(x, list) else [x])]
        assert "reset my password" in flat, f"nearest neighbour wrong: {flat}"
        return "nearest=reset my password"

    def info():
        r.execute_command("FT.INFO", idx)

    def drop():
        r.execute_command("FT.DROPINDEX", idx)
        for k in r.scan_iter(match=pre + "*"): r.delete(k)

    check("FT.CREATE (HNSW + TAG, COSINE)", create)
    check("FT.SEARCH KNN with TAG pre-filter", knn)
    check("FT.INFO", info, level="WARN")
    check("FT.DROPINDEX", drop, level="WARN")


def litellm_checks(url, p, stub_port):
    import litellm
    from litellm import Router
    from litellm.caching.caching import Cache
    u = urlparse(url)
    host, port, pw = u.hostname, u.port or 6379, u.password
    os.environ["OPENAI_API_KEY"] = "sk-stub"
    os.environ["OPENAI_API_BASE"] = os.environ["OPENAI_BASE_URL"] = f"http://127.0.0.1:{stub_port}/v1"
    litellm.drop_params = True
    r = redis.Redis.from_url(url)

    def exact():
        litellm.cache = Cache(type="redis", host=host, port=str(port), password=pw, namespace=p + "exact", ttl=60)
        msgs = [{"role": "user", "content": "exact " + uuid.uuid4().hex}]
        c0 = Stub.counts["chat"]
        a = litellm.completion(model="gpt-4o-mini", messages=msgs, caching=True)
        b = litellm.completion(model="gpt-4o-mini", messages=msgs, caching=True)
        assert Stub.counts["chat"] - c0 == 1, f"provider hit {Stub.counts['chat'] - c0}x, expected 1"
        assert a.choices[0].message.content == b.choices[0].message.content

    def router_counters():
        # Counter keys look like <deployment-hash>:openai/gpt-4o-mini:rpm:HH-MM and live ~60s.
        # The hash is stable across runs, so a dbsize delta reads 0 when a previous run wrote
        # the same minute's keys. Compare counter values instead.
        def counters():
            ks = [k for k in r.scan_iter(match="*gpt-4o-mini:[rt]pm:*", count=1000)]
            return dict(zip(ks, r.mget(ks))) if ks else {}
        litellm.cache = None
        before = counters()
        rt = Router(model_list=[{"model_name": "m", "litellm_params": {"model": "openai/gpt-4o-mini"}}],
                    redis_host=host, redis_port=port, redis_password=pw,
                    routing_strategy="usage-based-routing-v2")
        for i in range(3):
            rt.completion(model="m", messages=[{"role": "user", "content": f"r{i} {uuid.uuid4().hex}"}])
        time.sleep(3)
        after = counters()
        grew = {k.decode(): int(v) - int(before.get(k) or 0) for k, v in after.items()
                if v is not None and int(v) > int(before.get(k) or 0)}
        rpm = sum(d for k, d in grew.items() if ":rpm:" in k)
        assert grew, "router incremented no rpm/tpm counters (may depend on LiteLLM version)"
        return f"{len(grew)} counter keys grew, rpm +{rpm}"

    def semantic():
        # Per-run index (RedisVL uses the index name as the key prefix, so entries land under
        # the run prefix and get cleaned up). With the shared default index, entries from a run
        # less than `ttl` seconds earlier are re-indexed and score 6/7 = 0.857 cosine against
        # this run's identical prompts, which makes the "unrelated" prompt hit.
        litellm.cache = Cache(type="redis-semantic", redis_url=url,
                              similarity_threshold=0.8, ttl=60,
                              redis_semantic_cache_index_name=p + "semcache",
                              redis_semantic_cache_embedding_model="text-embedding-ada-002")
        tag = uuid.uuid4().hex[:6]
        ask = lambda s: litellm.completion(model="gpt-4o-mini", messages=[{"role": "user", "content": s}],
                                           caching=True)
        try:
            c0 = Stub.counts["chat"]
            ask(f"how do I reset my password {tag}")
            ask(f"how can I reset my password {tag}")           # paraphrase: expect HIT
            paraphrase_calls = Stub.counts["chat"] - c0
            ask(f"what is the capital of france {tag}")           # unrelated: expect MISS
            total_calls = Stub.counts["chat"] - c0
            # LiteLLM logs and swallows cache errors, so a failed index build looks like a miss.
            # Touch the lazily-built RedisVL cache to surface the real error.
            try:
                litellm.cache.cache.llmcache
            except Exception as e:
                raise AssertionError(f"RedisVL index build failed: {type(e).__name__}: {e}") from e
            assert paraphrase_calls == 1, f"paraphrase missed the cache ({paraphrase_calls} provider calls)"
            assert total_calls == 2, f"unrelated prompt wrongly hit ({total_calls} provider calls)"
            return "paraphrase=hit, unrelated=miss"
        finally:
            litellm.cache = None
            try:
                r.execute_command("FT.DROPINDEX", p + "semcache")
            except redis.ResponseError:
                pass

    check("LiteLLM exact cache (type=redis)", exact)
    check("LiteLLM router TPM/RPM counters in Redis", router_counters, level="WARN")
    check("LiteLLM semantic cache (type=redis-semantic)", semantic)


def bench(url, p, threads=32, seconds=5):
    r = redis.Redis.from_url(url)
    lat, stop = [], time.time() + seconds

    def worker(i):
        local, k = [], f"{p}bench:{i}"
        while time.time() < stop:
            t = time.perf_counter()
            r.incr(k); r.mget(k, k + "x", k + "y", k + "z")
            local.append((time.perf_counter() - t) * 1000)
        return local

    with ThreadPoolExecutor(threads) as ex:
        for part in ex.map(worker, range(threads)): lat += part
    for i in range(threads): r.delete(f"{p}bench:{i}")
    lat.sort()
    out = {"threads": threads, "seconds": seconds, "pairs_per_sec": round(len(lat) / seconds),
           "p50_ms": round(lat[len(lat) // 2], 3), "p99_ms": round(lat[int(len(lat) * .99)], 3)}
    print("  bench (INCR+MGET pairs):", out)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--label", default="target")
    ap.add_argument("--bench", action="store_true")
    ap.add_argument("--skip-litellm", action="store_true")
    a = ap.parse_args()
    p = f"lkc:{uuid.uuid4().hex[:8]}:"
    r = redis.Redis.from_url(a.url)
    print(f"== {a.label}: {a.url.split('@')[-1]}  prefix={p}")
    print("-- Redis primitives LiteLLM relies on")
    primitives(r, p)
    print("-- Vector search (RediSearch FT.* surface)")
    raw_vector(r, p)
    meta = {"label": a.label, "date": time.strftime("%Y-%m-%d")}
    if not a.skip_litellm:
        import litellm
        from importlib.metadata import version
        meta["litellm"], meta["redis_py"] = version("litellm"), redis.__version__
        print(f"-- LiteLLM {meta['litellm']} end to end (stub provider)")
        litellm_checks(a.url, p, start_stub())
    if a.bench:
        print("-- Micro-benchmark")
        meta["bench"] = bench(a.url, p)
    for k in r.scan_iter(match=p + "*"): r.delete(k)
    json.dump({"meta": meta, "results": results}, open(f"results-{a.label}.json", "w"), indent=2)
    n = lambda s: sum(1 for x in results if x["status"] == s)
    print(f"\nPASS {n('PASS')}  WARN {n('WARN')}  FAIL {n('FAIL')}  -> results-{a.label}.json")
    sys.exit(1 if n("FAIL") else 0)


if __name__ == "__main__":
    main()
