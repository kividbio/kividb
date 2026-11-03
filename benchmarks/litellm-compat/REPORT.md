# LiteLLM on KiviDB: compatibility results

## KiviDB 1.0.5 (2026-10-03): all checks pass, all eight 1.0.4 gaps fixed

**Verdict:** KiviDB 1.0.5 works as LiteLLM's Redis backend for everything the harness covers: the
exact cache, the router's TPM/RPM counters, and the semantic cache (`type: redis-semantic` via
RedisVL), with the same results as Redis Stack. This replaces the 1.0.4 verdict below.

Setup is the same as for 1.0.4 (same Python stack, same Redis Stack image digest, same host) except
the KiviDB binary: `kividb-full-linux-x86_64.tar.gz` from the GitHub v1.0.5 release, sha256
`ea1376e9edc8a7ce3d94bf64cc19e0e869641db0b45b069fa5dd2b6a4707c064`, matching `SHA256SUMS.txt`.
INFO reports `redis_version:8.10.0 kividb_version:1.0.5`. It was started in an empty data directory.
The harness is unchanged from the 1.0.4 run.

### Harness results

| Check | redis-stack | kividb 1.0.4 | kividb 1.0.5 |
|---|---|---|---|
| PING, SET EX/GET/TTL, INCR/EXPIRE/MGET, MULTI/EXEC, SCAN MATCH, SET NX EX, HSET/HGETALL, RPUSH/LRANGE/LPOP, EVAL+EVALSHA | PASS | PASS | PASS |
| INFO | PASS | PASS | PASS (`redis_version=8.10.0 kividb_version=1.0.5`) |
| FT.CREATE / FT.SEARCH KNN + TAG / FT.INFO / FT.DROPINDEX | PASS | PASS | PASS |
| LiteLLM exact cache | PASS | PASS | PASS |
| LiteLLM router TPM/RPM counters | PASS (rpm +3) | PASS (rpm +3) | PASS (2 counter keys grew, rpm +3) |
| LiteLLM semantic cache | PASS | FAIL (`unknown field type`) | **PASS** (paraphrase=hit, unrelated=miss) |
| **Totals** | 17 / 0 / 0 | 16 PASS / 1 FAIL | **17 PASS / 0 WARN / 0 FAIL** |

KiviDB 1.0.5 was run four times: one checks-only run (`results-kividb-1.0.5.checks.json`), two
back-to-back reruns (not kept), and one `--bench` run (`results-kividb-1.0.5.json`). All four gave
17 PASS. Redis Stack was rerun the same day: 17 PASS in both its checks-only and `--bench` runs.

### Status of each 1.0.4 gap on 1.0.5

Reproduced with `repro_gaps.py`, which now also covers gaps 4–6. Output is in
`repro-kividb-1.0.5.txt` and `repro-redis-stack.txt`.

| # | 1.0.4 gap | 1.0.5 result | Fixed |
|---|---|---|---|
| 1 | `FT.CREATE` field options (`WEIGHT`, `SEPARATOR`, `SORTABLE`, `CASESENSITIVE`) rejected | All return `OK`, including RedisVL's exact schema | Yes |
| 2 | `VECTOR_RANGE` returned `[0]` silently | Returns the document with `d=0.0049628615`, same as KNN | Yes |
| 3 | Non-vector FT.SEARCH returned empty field lists; `RETURN` / `NOCONTENT` ignored | `RETURN` returns only the asked field, `*` returns all fields, `NOCONTENT` omits them | Yes |
| 4 | Negated pre-filter `(-@k:{tenantB})` returned the excluded document | Returns only `tenantA` | Yes |
| 5 | OR across clauses `(@k:{tenantA}\|@k:{nobody})` returned nothing | Returns `tenantA` | Yes |
| 6 | Tag without braces `(@k:tenantA)` matched (Redis: no match) | `[0]`, same as Redis | Yes |
| 7 | FT.SEARCH sent the RESP2 array under RESP3 | Sends the RESP3 map (`attributes`, `format`, `results[{id, extra_attributes, values}]`, `total_results`, `warning`) | Yes |
| 8 | Misleading `FT.CREATE` errors; unparseable filters fell back to match-all | See below | Yes |

Gap 8, malformed input sent to both servers:

| Input | Redis Stack | KiviDB 1.0.5 |
|---|---|---|
| `SCHEMA f TEXT BOGUSOPT 1` | ``Invalid field type for field `BOGUSOPT` `` | ``Invalid field type `1` for field `BOGUSOPT` `` |
| `SCHEMA f NOTATYPE` | ``Invalid field type for field `f` `` | ``Invalid field type `NOTATYPE` for field `f` `` |
| Unbalanced paren in KNN pre-filter | `Syntax error at offset 15 near >[` | `Syntax error at offset 13 near =>[KNN 5 @v $q A` |
| Garbage pre-filter `(@@@ ### !!)` | `Syntax error at offset 11 near` | `Syntax error at offset 1 near @@@ ### !!)=>[KN` |
| Unknown FT.SEARCH argument | ``Unknown argument `FROBNICATE` at position 1 for <main>`` | ``Unknown argument `FROBNICATE` at position 3 for <main>`` |
| Undefined `$param` | ``No such parameter `nope` `` | ``No such parameter `nope` `` |

Every malformed input is now an error; none is silently widened to match-all. In the v1.0.5 source,
`src/vector/filter.rs` `parse_query` returns `Result` and uses `Filter::MatchAll` only for an empty
query, `*`, and the negation of a stopword.

### Remaining differences from Redis Stack (cosmetic; none affects LiteLLM)

- Vector distances come back with 10 significant digits (`0.0049628615`); Redis Stack gives 15
  (`0.00496286153793`).
- Error text: syntax-error offsets and the `Unknown argument … at position N` index differ from
  RediSearch, and KiviDB names the offending token. Clients that match on exact error strings may notice.
- `FT.SEARCH … *` without `RETURN` lists hash fields in a different order. Redis doesn't guarantee
  hash field order either.

### Micro-benchmark (`--bench`, one 5-second run each, 32 threads, INCR + MGET pairs)

| | redis-stack | kividb 1.0.5 |
|---|---|---|
| pairs/sec | 1665 | 1918 |
| p50 ms | 18.387 | 16.17 |
| p99 ms | 35.112 | 29.114 |

The same caveat as for 1.0.4 applies: client and servers share one 4-vCPU container, and Redis
Stack goes through docker port mapping. Not comparable or publishable.

### Still not covered on 1.0.5

The LiteLLM proxy server process, async paths (`acompletion`), Redis OSS / Valkey, the
`releases.kividb.io` download path (still blocked by egress policy), and benchmarks on separate
hardware. The 1.0.5 release notes list stemming and phonetic search as not implemented; LiteLLM's
semantic cache doesn't use them.

---

# KiviDB 1.0.4 (2026-10-01)

Harness: `litellm_kividb_compat.py` in this folder (fixed version, see "Harness bugs").

**Verdict (1.0.4):** KiviDB 1.0.4 works as LiteLLM's Redis backend for the exact cache (`type: redis`) and the
router's TPM/RPM counters. It does **not** work for the semantic cache (`type: redis-semantic`): the
RedisVL index cannot be created, and even with that fixed, two FT.SEARCH gaps would still make every
lookup a miss. LiteLLM swallows the error and logs it, so the proxy keeps serving with no semantic
caching.

## Setup

| Item | Value |
|---|---|
| KiviDB | 1.0.4, `kividb-full-linux-x86_64.tar.gz` (Lua + TLS) from the GitHub v1.0.4 release, sha256 `6b4b34cf67a4b6a87582a116120e67fc91322118795304aab063c85671cc34db` matches `SHA256SUMS.txt`. Run as `kividb --port 6380 --bind 127.0.0.1` |
| Redis Stack | `redis/redis-stack-server:latest` (`sha256:798ab84d9f266936b034ab11c4d04a2b8e4b441884c5aa7d17ac951eefdf742a`), redis 7.4.7, RediSearch 2.10.20 (module ver 21020), on port 6381 through docker port mapping |
| Python stack | Python 3.11.15, litellm 1.103.2, redis-py 8.1.0, redisvl 0.27.2, numpy 2.4.6 (pip pin `!=1.82.7,!=1.82.8`) |
| Host | One x86_64 cloud container, 4 vCPU. Client and both servers share it |

`releases.kividb.io` was blocked by this environment's egress policy (HTTP 403 on CONNECT), so the
binary came from the same release on GitHub instead, verified against the published checksum.

## Results

Fixed harness. Each server was run twice (with and without `--bench`), and both runs gave the same
status on every check. The 1.0.4 files are `results-kividb-1.0.4[.checks].json` and `results-redis-stack-run1.0.4[.checks].json`
(`.checks` = without `--bench`).

| Check | redis-stack | kividb | Notes |
|---|---|---|---|
| PING | PASS | PASS | |
| SET EX / GET / TTL | PASS | PASS | |
| INCR / EXPIRE / MGET | PASS | PASS | |
| MULTI/EXEC pipeline | PASS | PASS | |
| SCAN MATCH | PASS | PASS | |
| SET NX EX (locks) | PASS | PASS | |
| HSET / HGETALL | PASS | PASS | |
| RPUSH / LRANGE / LPOP | PASS | PASS | |
| EVAL + EVALSHA (Lua) | PASS | PASS | Needs the `-lua` or `-full` KiviDB build |
| INFO | PASS (`redis_version=7.4.7`) | PASS (`redis_version=7.0.15 kividb_version=1.0.4`) | |
| FT.CREATE (HNSW + TAG, COSINE) | PASS | PASS | |
| FT.SEARCH KNN with TAG pre-filter | PASS | PASS | On the original harness this check FAILED on redis-stack: harness bug 1 |
| FT.INFO | PASS | PASS | |
| FT.DROPINDEX | PASS | PASS | |
| LiteLLM exact cache (`type=redis`) | PASS | PASS | |
| LiteLLM router TPM/RPM counters | PASS (2 counter keys grew, rpm +3) | PASS (2 counter keys grew, rpm +3) | |
| LiteLLM semantic cache (`type=redis-semantic`) | PASS (paraphrase=hit, unrelated=miss) | **FAIL** | `RedisSearchError: Failed to create index '…semcache' on Redis: unknown field type`. KiviDB gaps 1, 2, 3 |
| **Totals** | **PASS 17, WARN 0, FAIL 0** | **PASS 16, WARN 0, FAIL 1** | |

### Micro-benchmark (`--bench`: 32 threads, 5 s, INCR + MGET pairs)

| | redis-stack | kividb |
|---|---|---|
| pairs/sec | 1735 | 1924 |
| p50 ms | 17.788 | 15.732 |
| p99 ms | 34.116 | 31.418 |

**These numbers can't be used to compare the servers.** The Python client and both servers shared one
4-vCPU container, and redis-stack was reached through docker port mapping while KiviDB was not. Per
the harness README, only numbers from a client on separate hardware in the same AZ are meaningful.
Each figure comes from one 5-second run.

## Original harness, first run (before fixes)

| Check | redis-stack | kividb |
|---|---|---|
| FT.SEARCH KNN with TAG pre-filter | FAIL `TypeError: unhashable type: 'slice'` | PASS |
| LiteLLM semantic cache | PASS on the first run. On a rerun 54 s later: FAIL `unrelated prompt wrongly hit (1 provider calls)` | FAIL `paraphrase missed the cache (2 provider calls)` |
| Everything else | PASS | PASS |

## Classification of every failure

| Failure | Classification | Cause |
|---|---|---|
| redis-stack: FT.SEARCH KNN `TypeError: unhashable type: 'slice'` | Harness bug, set off by a client version change | redis-py 8.1.0 defaults to RESP3 (`DEFAULT_RESP_VERSION = 3`). Under RESP3, Redis Stack returns FT.SEARCH as a map, and the harness sliced `res[1:]`, which assumes the RESP2 array. KiviDB passed only because it returns the RESP2 array under RESP3 too (see "Parity difference" below). |
| redis-stack: semantic cache `unrelated prompt wrongly hit` on a rerun | Harness bug | Every run shared LiteLLM's default index `litellm_semantic_cache_index`, and entries have TTL 60 s. The run-1 entry `what is the capital of france <tagA>` was re-indexed by run 2's `FT.CREATE` and scored cosine 6/7 = 0.857 against `what is the capital of france <tagB>`, above the 0.8 threshold. Run 2 began 54 s after run 1. |
| kividb: semantic cache `paraphrase missed the cache` | **KiviDB gap** (gaps 1–3). The failure message was misleading, which was a harness bug | LiteLLM logs and swallows the RedisVL index-build error, so the cache silently acts as all-miss. |
| (latent) router counters check | Harness bug, not triggered here | The check compared `dbsize` before and after. Counter keys are `<deployment-hash>:openai/gpt-4o-mini:rpm:HH-MM`, and the hash is stable across runs, so a rerun within the same minute writes no new keys and would WARN falsely. |

No failure was caused by a LiteLLM or RedisVL version quirk. The only version effect is the redis-py 8
RESP3 default behind harness bug 1.

## KiviDB gaps to file

Reproduce all of them with `python repro_gaps.py --url redis://HOST:PORT`. Captured output for both
servers is in `repro-kividb-1.0.4.txt` and `repro-redis-stack-run1.0.4.txt`.

### Gap 1: FT.CREATE does not accept per-field options after TEXT / TAG / NUMERIC (blocks LiteLLM semantic cache)

The exact command RedisVL 0.27.2 sends for LiteLLM 1.103.2 (captured with `MONITOR` on Redis Stack):

```
FT.CREATE litellm_semantic_cache_index ON HASH PREFIX 1 litellm_semantic_cache_index SCORE 1.0 SCHEMA
  prompt TEXT WEIGHT 1 response TEXT WEIGHT 1 inserted_at NUMERIC updated_at NUMERIC
  prompt_vector VECTOR FLAT 6 TYPE FLOAT32 DIM 256 DISTANCE_METRIC COSINE
  litellm_cache_key TAG SEPARATOR ,
```

Redis Stack: `OK`. KiviDB 1.0.4: `ERR unknown field type`.

Minimal reproductions (KiviDB output; Redis Stack returns `OK` for all of them):

| Schema fragment | KiviDB 1.0.4 |
|---|---|
| `f TEXT WEIGHT 1` | `ERR unknown field type` |
| `f TAG SEPARATOR ,` | `ERR unknown field type` |
| `f TEXT SORTABLE` | `ERR missing field type` |
| `f TAG CASESENSITIVE` | `ERR missing field type` |
| `f NUMERIC SORTABLE` | `ERR missing field type` |

Cause in source: `src/commands/vector.rs`, `ft_create`, lines 181–184. `"TEXT"`, `"TAG"` and
`"NUMERIC"` map straight to a `FieldType` without consuming any following options. The next token
(`WEIGHT`, `SEPARATOR`, `SORTABLE`) is then read as a new field name, and the token after it as that
field's type. The error messages hide this. `ft_alter` has the same structure (its `unknown field type` is at line 752). Bare
`TEXT`, `TAG`, `NUMERIC`, `VECTOR FLAT|HNSW`, and the index-level `SCORE 1.0` option are accepted.

### Gap 2: `VECTOR_RANGE` queries return zero results with no error (blocks LiteLLM semantic cache)

RedisVL's semantic-cache lookup is a range query, not KNN:

```
FT.SEARCH <idx> "@prompt_vector:[VECTOR_RANGE $distance_threshold $vector]=>{$YIELD_DISTANCE_AS: vector_distance} (@litellm_cache_key:{<key>})"
  RETURN 8 entry_id prompt response inserted_at updated_at metadata litellm_cache_key vector_distance
  SORTBY vector_distance ASC DIALECT 2 LIMIT 0 1
  PARAMS 4 vector <blob> distance_threshold 0.19999999999999996
```

With a stored vector at cosine distance ≈ 0.005 from the query, a minimal version
(`@v:[VECTOR_RANGE $r $q]=>{$YIELD_DISTANCE_AS: d}`, `r=0.2`) returns `[0]` on KiviDB and the
document on Redis Stack. `*=>[KNN 1 @v $q AS d]` finds the same document on KiviDB (`d=0.004963`).
The range-only form, the range + YIELD form, and the full RedisVL form (range + YIELD + TAG filter)
all return `[0]` on KiviDB. `VECTOR_RANGE` does not appear anywhere in `src/`. The query isn't
recognized as KNN, so it falls through to the text/tag filter path and matches nothing. It should
either be implemented or rejected with an error; a silent empty result makes a cache look healthy
while it never hits.

Confirmed impact: with gap 1 worked around client-side (a diagnostic shim stripping `WEIGHT` and
`SEPARATOR` from FT.CREATE, not part of the results above), LiteLLM's semantic check on KiviDB got
through RedisVL's index-existence probe (`FT.INFO`), `FT.CREATE` and the cache writes without error.
All three FT.SEARCH range lookups then returned `[0]`, so it still failed with
`paraphrase missed the cache (2 provider calls)`.

### Gap 3: non-vector FT.SEARCH returns an empty field list; `RETURN` and `NOCONTENT` are ignored

```
FT.SEARCH <idx> "@k:{abc}" RETURN 1 prompt DIALECT 2   -> KiviDB [1, "<key>", []]    Redis Stack [1, "<key>", ["prompt","hello"]]
FT.SEARCH <idx> "*" DIALECT 2                          -> KiviDB [1, "<key>", []]    Redis Stack: all hash fields
FT.SEARCH <idx> "*" NOCONTENT DIALECT 2                -> KiviDB [1, "<key>", []]    Redis Stack [1, "<key>"]
```

The source says so: `src/commands/vector.rs`, the pure-filter branch of `ft_search`, lines 1117–1119:
`// TODO: honour RETURN fields clause (same as KNN path)` followed by `out.push(resp_array(vec![]))`.
Only the KNN path returns fields. This sits right behind gap 2 for LiteLLM: once `VECTOR_RANGE`
goes through a non-KNN path, LiteLLM's `_cache_hit_matches_key` needs `litellm_cache_key` and
`response` in the reply, and a hit with no fields is treated as a miss. That last step is inferred
from LiteLLM's source; it was not run, because gap 2 stops execution first.

### Parity difference (not a failure in these tests): FT.SEARCH under RESP3 returns the RESP2 array shape

After `HELLO 3`, Redis Stack replies to FT.SEARCH with a map (`total_results`, `results[{id,
extra_attributes}]`, …), and KiviDB still replies `[total, id, [k, v, …]]`. redis-py 8 uses RESP3 by
default. No check failed because of this. Clients that parse the RESP3 map (redis-py's
`ft().search()` result parser, for example) may break; that was not tested here.

## Harness bugs fixed (diff against the original is in git history)

1. **FT.SEARCH reply parsing** accepts both RESP2 arrays and the RESP3 map.
2. **Semantic cache isolation:** each run uses its own index, `redis_semantic_cache_index_name=<run-prefix>semcache`
   (RedisVL uses the index name as the key prefix, so the run's cleanup removes the entries), and
   drops it at the end.
3. **Semantic cache diagnostics:** after the prompts, the check touches LiteLLM's lazily built RedisVL
   cache so a failed index build is reported (`RedisVL index build failed: …`) instead of `paraphrase missed`.
4. **Router counters:** compares the values of the `*gpt-4o-mini:[rt]pm:*` counter keys before and
   after, instead of `dbsize`.
5. **INFO** prints both `redis_version` and `kividb_version` (KiviDB reports `redis_version:7.0.15`).

After the fixes, two back-to-back runs against redis-stack (inside the 60 s TTL window that caused
the flake) both gave PASS 17, FAIL 0.

## Not run / not covered

- The `releases.kividb.io` download path. It was blocked by egress policy; the GitHub release asset
  with the same version and a matching checksum was used.
- Stock Redis OSS and Valkey were not re-run. The OSS baseline is the earlier `results-redis-oss.json`
  (not reproduced here).
- The LiteLLM **proxy server** (`litellm --config`) was not started. All checks use the Python SDK
  (`litellm.completion`, `Router`) in-process, which uses the same cache and router code paths.
- Async LiteLLM paths (`acompletion`, async RedisVL calls) were not exercised.
- Semantic quality: the stub embedding is a hashed bag of words. It shows the wiring works, not retrieval quality.
- The bench was a single run of each and isn't comparable across servers (see above).
