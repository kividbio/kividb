#!/usr/bin/env python3
"""
Minimal reproductions of the FT.* gaps that break LiteLLM's semantic cache (type=redis-semantic)
on KiviDB. Run against KiviDB and Redis Stack and diff the output.

  python repro_gaps.py --url redis://127.0.0.1:6380   # KiviDB
  python repro_gaps.py --url redis://127.0.0.1:6381   # Redis Stack
"""
import argparse, uuid

import numpy as np
import redis


def vec(*xs):
    return np.asarray(xs, dtype=np.float32).tobytes()


def run(r, label, *cmd):
    shown = " ".join("<%d-byte vector>" % len(c) if isinstance(c, bytes) else str(c) for c in cmd)
    try:
        out = r.execute_command(*cmd)
    except redis.ResponseError as e:
        out = f"(error) {e}"
    print(f"[{label}] {shown}\n    -> {out!r}")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    a = ap.parse_args()
    r = redis.Redis.from_url(a.url, protocol=2)
    t = "repro_" + uuid.uuid4().hex[:6]
    made = []

    print("== GAP 1: FT.CREATE rejects per-field options after TEXT / TAG / NUMERIC")
    for opts in (["TEXT", "WEIGHT", "1"], ["TAG", "SEPARATOR", ","], ["TEXT", "SORTABLE"],
                 ["TAG", "CASESENSITIVE"], ["NUMERIC", "SORTABLE"]):
        name = f"{t}_{len(made)}"
        if run(r, "1", "FT.CREATE", name, "ON", "HASH", "PREFIX", "1", name + ":", "SCHEMA", "f", *opts) == b"OK":
            made.append(name)
    name = f"{t}_rvl"
    # The exact FT.CREATE RedisVL 0.27.2 sends for LiteLLM 1.103.2's semantic cache (captured with MONITOR).
    if run(r, "1", "FT.CREATE", name, "ON", "HASH", "PREFIX", "1", name, "SCORE", "1.0", "SCHEMA",
           "prompt", "TEXT", "WEIGHT", "1", "response", "TEXT", "WEIGHT", "1", "inserted_at", "NUMERIC",
           "updated_at", "NUMERIC", "prompt_vector", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "4",
           "DISTANCE_METRIC", "COSINE", "litellm_cache_key", "TAG", "SEPARATOR", ",") == b"OK":
        made.append(name)

    # Index without field options so the query-side gaps can be shown on both servers.
    idx, pre = f"{t}_q", f"{t}_q:"
    run(r, "setup", "FT.CREATE", idx, "ON", "HASH", "PREFIX", "1", pre, "SCHEMA", "prompt", "TEXT",
        "k", "TAG", "v", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "COSINE")
    made.append(idx)
    r.hset(pre + "1", mapping={"prompt": "hello", "k": "abc", "v": vec(1, 0, 0, 0)})

    print("\n== GAP 2: VECTOR_RANGE query returns no results (no error); same doc is found by KNN")
    q = vec(1, 0.1, 0, 0)  # cosine distance to stored vector ~= 0.005
    run(r, "2", "FT.SEARCH", idx, "@v:[VECTOR_RANGE $r $q]=>{$YIELD_DISTANCE_AS: d}",
        "PARAMS", "4", "r", "0.2", "q", q, "RETURN", "1", "d", "DIALECT", "2")
    run(r, "2", "FT.SEARCH", idx, "*=>[KNN 1 @v $q AS d]", "PARAMS", "2", "q", q,
        "RETURN", "1", "d", "DIALECT", "2")

    print("\n== GAP 3: non-vector FT.SEARCH returns an empty field list; RETURN / NOCONTENT ignored")
    run(r, "3", "FT.SEARCH", idx, "@k:{abc}", "RETURN", "1", "prompt", "DIALECT", "2")
    run(r, "3", "FT.SEARCH", idx, "*", "DIALECT", "2")
    run(r, "3", "FT.SEARCH", idx, "*", "NOCONTENT", "DIALECT", "2")

    print("\n== GAPS 4-6: KNN pre-filter expressions (expected: pf:a only, except where noted)")
    fidx, fpre = f"{t}_f", f"{t}_f:"
    run(r, "setup", "FT.CREATE", fidx, "ON", "HASH", "PREFIX", "1", fpre, "SCHEMA", "k", "TAG", "n", "NUMERIC",
        "v", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2", "DISTANCE_METRIC", "COSINE")
    made.append(fidx)
    r.hset(fpre + "a", mapping={"k": "tenantA", "n": 1, "v": vec(1, 0)})
    r.hset(fpre + "b", mapping={"k": "tenantB", "n": 50, "v": vec(1, 0.01)})
    for label, f in (("4 negation", "(-@k:{tenantB})"),
                     ("5 OR across clauses", "(@k:{tenantA}|@k:{nobody})"),
                     ("6 tag without braces (Redis: no match)", "(@k:tenantA)"),
                     ("ok tag", "(@k:{tenantA})"),
                     ("ok tag AND numeric", "(@k:{tenantA} @n:[0 10])")):
        run(r, label, "FT.SEARCH", fidx, f + "=>[KNN 5 @v $q AS d]", "PARAMS", "2", "q", vec(1, 0),
            "RETURN", "1", "k", "SORTBY", "d", "DIALECT", "2")
    r.delete(fpre + "a", fpre + "b")

    print("\n== PARITY: FT.SEARCH reply shape under RESP3 (Redis Stack returns a map)")
    r3 = redis.Redis.from_url(a.url, protocol=3)
    out = run(r3, "resp3", "FT.SEARCH", idx, "*=>[KNN 1 @v $q AS d]", "PARAMS", "2", "q", q,
              "RETURN", "1", "d", "DIALECT", "2")
    print(f"    reply type under RESP3: {type(out).__name__}")

    for n in made:
        try:
            r.execute_command("FT.DROPINDEX", n)
        except redis.ResponseError:
            pass
    r.delete(pre + "1")


if __name__ == "__main__":
    main()
