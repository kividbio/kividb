/**
 * OmniRoute <-> Redis-protocol server compatibility harness.
 *
 * Runs OmniRoute's own Redis code against any server (KiviDB, Redis 8, Valkey) and records
 * PASS / FAIL / WARN per check. A stub plays the upstream model provider (OmniRoute's own test
 * pattern: globalThis.fetch answers /chat/completions), so no real API keys or spend are involved.
 *
 * Run it from an OmniRoute checkout (v3.8.52, after `npm ci`), so its modules and tsx resolve:
 *
 *   cd OmniRoute
 *   node --import tsx/esm --import ./open-sse/utils/setupPolyfill.ts \
 *     /path/to/omniroute_kividb_compat.mts --url redis://HOST:6381 --label kividb-lua
 *
 * Writes results-<label>.json next to this file. Run the same command against stock Redis (for
 * example the `redis:8` image) with a different --label to get a comparison.
 *
 * What it covers. With REDIS_URL set, OmniRoute keeps four things on the server
 * (docs/ops/REDIS_PRODUCTION_CONFIG.md), plus an optional fifth:
 *   * the per-API-key rate limiter: one Lua script (EVAL) that checks and bumps fixed windows.
 *     On a server without Lua, OmniRoute logs the error and falls back to a per-process in-memory
 *     limiter, so requests still succeed but the limit is no longer shared between replicas.
 *     The checks below look at the server's keys and use a second process, so that fallback
 *     counts as a FAIL, not a pass.
 *   * the API-key auth cache (SET ... EX, DEL on revoke)
 *   * the quota store (QUOTA_STORE_DRIVER=redis): INCRBYFLOAT sliding windows
 *   * the warmup circuit breaker: hashes with HSET/HGETALL/EXPIRE/PERSIST
 *   * the semantic cache (OMNIROUTE_SEMANTIC_CACHE_BACKEND=redis): JSON strings with a TTL plus sets
 *     of ids; similarity is computed in OmniRoute, so no FT.* command is used
 * and drives one chat request end to end through handleChat (the /v1/chat/completions handler)
 * with a per-key rate limit, to show the 429 comes from the shared counter.
 *
 * Everything it creates is under a random REDIS_KEY_PREFIX and a temporary DATA_DIR, removed at
 * the end.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath, pathToFileURL } from "node:url";

const args = process.argv.slice(2);
const arg = (name: string, dflt: string) => {
  const i = args.indexOf(name);
  return i >= 0 ? args[i + 1] : dflt;
};
const URL_ = arg("--url", "redis://127.0.0.1:6380");
const LABEL = arg("--label", "kividb");
const CHILD = args.includes("--child-rate");
const ROOT = process.env.OMNIROUTE_DIR || process.cwd();
const HERE = path.dirname(fileURLToPath(import.meta.url));

// Environment first: OmniRoute reads these when its modules load.
const PREFIX = process.env.REDIS_KEY_PREFIX || `kvcompat${Math.random().toString(16).slice(2, 10)}:`;
process.env.REDIS_URL = URL_;
process.env.REDIS_KEY_PREFIX = PREFIX;
process.env.DATA_DIR =
  process.env.DATA_DIR || fs.mkdtempSync(path.join(os.tmpdir(), "omniroute-kividb-compat-"));
process.env.API_KEY_SECRET = process.env.API_KEY_SECRET || "kividb-compat-secret";
process.env.REQUIRE_API_KEY = "false";
process.env.DISABLE_SQLITE_AUTO_BACKUP = "true";
process.env.DASHBOARD_PASSWORD = "";
process.env.INITIAL_PASSWORD = "";
delete process.env.JWT_SECRET;
if (process.env.NODE_ENV === "test") delete process.env.NODE_ENV; // test mode skips Redis

const load = (rel: string) => import(pathToFileURL(path.join(ROOT, rel)).href);

// ---------------------------------------------------------------- child: one more "replica"
if (CHILD) {
  const keyId = args[args.indexOf("--child-rate") + 1];
  const { checkRateLimit } = await load("src/shared/utils/rateLimiter.ts");
  const out = await checkRateLimit(keyId, [{ limit: 3, window: 60 }]);
  process.stdout.write(JSON.stringify(out) + "\n");
  process.exit(0);
}

const Redis = (await import(pathToFileURL(path.join(ROOT, "node_modules/ioredis/built/index.js")).href))
  .default;
const r = new Redis(URL_);

type Result = { check: string; status: string; detail: string; ms: number };
const results: Result[] = [];

async function check(name: string, fn: () => Promise<unknown> | unknown, level = "FAIL") {
  const t0 = Date.now();
  let status = "PASS";
  let detail = "";
  try {
    const out = await fn();
    detail = typeof out === "string" ? out : "";
  } catch (e: any) {
    status = level;
    detail = `${e?.name || "Error"}: ${String(e?.message ?? e).slice(0, 220)}`;
  }
  results.push({ check: name, status, detail, ms: Date.now() - t0 });
  console.log(`  [${status.padEnd(4)}] ${name}${detail ? `  (${detail})` : ""}`);
}

async function group(title: string, fn: () => Promise<void>) {
  console.log(`\n== ${title}`);
  try {
    await fn();
  } catch (e: any) {
    const detail = `${e?.name || "Error"}: ${String(e?.message ?? e).slice(0, 220)}`;
    results.push({ check: `${title}: setup`, status: "FAIL", detail, ms: 0 });
    console.log(`  [FAIL] ${title}: setup  (${detail})`);
  }
}

function assert(cond: unknown, msg: string): asserts cond {
  if (!cond) throw new Error(msg);
}

async function keys(pattern: string): Promise<string[]> {
  const out: string[] = [];
  let cursor = "0";
  do {
    const [next, batch] = await r.scan(cursor, "MATCH", pattern, "COUNT", 500);
    out.push(...batch);
    cursor = next;
  } while (cursor !== "0");
  return out.sort();
}

// OmniRoute logs a Redis failure and carries on with its fallback; keep those lines, they are
// what a FAIL below is about.
const fallbackLog: string[] = [];
const origError = console.error;
console.error = (...a: unknown[]) => {
  fallbackLog.push(a.map((x) => (x instanceof Error ? x.message : String(x))).join(" ").slice(0, 300));
};

// The limiter uses fixed one-minute windows. A check that straddles a minute boundary would see
// its count reset half-way, so each one starts at least 25 s before the next boundary.
async function awayFromMinuteEdge() {
  const intoMinute = (Date.now() / 1000) % 60;
  if (intoMinute > 35) await new Promise((res) => setTimeout(res, (61 - intoMinute) * 1000));
}

// ---------------------------------------------------------------- the rate limiter (Lua)
async function rateLimiter() {
  await awayFromMinuteEdge();
  const { checkRateLimit } = await load("src/shared/utils/rateLimiter.ts");
  const keyId = `rl-${Math.random().toString(16).slice(2, 8)}`;
  const win = Math.floor(Date.now() / 1000 / 60);
  const counterKey = `${PREFIX}rl:api_key:${keyId}:60:${win}`;

  await check("rate limiter: 3 requests allowed, the 4th refused with its window and reset time", async () => {
    for (let i = 0; i < 3; i++) {
      const ok = await checkRateLimit(keyId, [{ limit: 3, window: 60 }]);
      assert(ok.allowed, `request ${i + 1} refused: ${JSON.stringify(ok)}`);
    }
    const no = await checkRateLimit(keyId, [{ limit: 3, window: 60 }]);
    assert(!no.allowed && no.failedWindow === 60, JSON.stringify(no));
    assert(no.resetAt === (win + 1) * 60 * 1000, `resetAt ${no.resetAt}`);
  });

  await check("rate limiter: the counter is on the server (the Lua script ran there)", async () => {
    const v = await r.get(counterKey);
    const last = fallbackLog.find((l) => l.includes("RATE_LIMITER"));
    assert(v === "3", `counter ${counterKey} is ${v}${last ? `; OmniRoute logged: ${last}` : ""}`);
    const ttl = await r.ttl(counterKey);
    assert(ttl > 0 && ttl <= 120, `ttl ${ttl}`);
  });

  await check("rate limiter: another process sharing the server is refused too (shared limit)", async () => {
    const out = spawnSync(
      process.execPath,
      [...process.execArgv, fileURLToPath(import.meta.url), "--url", URL_, "--child-rate", keyId],
      { cwd: ROOT, env: { ...process.env }, encoding: "utf8", timeout: 60000 }
    );
    const line = (out.stdout || "").trim().split("\n").pop() || "";
    const got = JSON.parse(line || "{}");
    assert(got.allowed === false, `the second process was allowed: ${line} ${out.stderr?.slice(0, 200)}`);
  });

  await check("rate limiter: two rules; the tighter one refuses, and a refusal does not count", async () => {
    const k2 = `${keyId}-b`;
    const rules = [
      { limit: 100, window: 60 },
      { limit: 2, window: 3600 },
    ];
    assert((await checkRateLimit(k2, rules)).allowed, "1st refused");
    assert((await checkRateLimit(k2, rules)).allowed, "2nd refused");
    const no = await checkRateLimit(k2, rules);
    assert(!no.allowed && no.failedWindow === 3600, JSON.stringify(no));
    const minute = await r.get(`${PREFIX}rl:api_key:${k2}:60:${win}`);
    assert(minute === "2", `the refused request was counted: minute counter ${minute}`);
  });
}

// ---------------------------------------------------------------- API keys: policy, auth cache, chat
async function apiKeys() {
  const apiKeysDb = await load("src/lib/db/apiKeys.ts");
  const providersDb = await load("src/lib/db/providers.ts");
  const policy = await load("src/shared/utils/apiKeyPolicy.ts");
  const { handleChat } = await load("src/sse/handlers/chat.ts");
  const { initTranslators } = await load("open-sse/translator/index.ts");
  initTranslators();

  const limited = await apiKeysDb.createApiKey("kividb-compat", "machine-kividb-compat");
  await apiKeysDb.updateApiKeyPermissions(limited.id, { rateLimits: [{ limit: 2, window: 60 }] });
  const req = (key: string) =>
    new Request("http://localhost/api/v1/chat/completions", {
      method: "POST",
      headers: { Authorization: `Bearer ${key}` },
    });

  await check("API key policy: a key limited to 2 a minute gets 429 rate_limit_exceeded on the 3rd", async () => {
    await awayFromMinuteEdge();
    for (let i = 0; i < 2; i++) {
      const ok = await policy.enforceApiKeyPolicy(req(limited.key), "openai/gpt-4o-mini");
      assert(ok.rejection === null, `request ${i + 1} rejected: ${ok.rejection?.status}`);
    }
    const no = await policy.enforceApiKeyPolicy(req(limited.key), "openai/gpt-4o-mini");
    assert(no.rejection?.status === 429, `3rd: ${no.rejection?.status}`);
    const body = await no.rejection.json();
    assert(body.error?.code === "rate_limit_exceeded", JSON.stringify(body).slice(0, 200));
    assert(no.rejection.headers.get("Retry-After"), "no Retry-After header");
    const v = await r.get(`${PREFIX}rl:api_key:${limited.id}:60:${Math.floor(Date.now() / 60000)}`);
    assert(v === "2", `server counter ${v}`);
  });

  await check("auth cache: a validated key is cached on the server for an hour", async () => {
    assert(await apiKeysDb.validateApiKey(limited.key), "the key did not validate");
    const found = await keys(`${PREFIX}auth:api_key:*`);
    assert(found.length >= 1, "no auth cache key on the server");
    const vals = await Promise.all(found.map((k) => r.get(k)));
    assert(vals.some((v) => v && JSON.parse(v).id === limited.id), `no entry for ${limited.id}`);
    const ttl = await r.ttl(found[0]);
    assert(ttl > 3500 && ttl <= 3600, `ttl ${ttl}`);
  });

  await check("auth cache: revoking the key removes its cache entry", async () => {
    await apiKeysDb.revokeApiKey(limited.id);
    const vals = await Promise.all((await keys(`${PREFIX}auth:api_key:*`)).map((k) => r.get(k)));
    assert(!vals.some((v) => v && JSON.parse(v).id === limited.id), "the revoked key is still cached");
    assert(!(await apiKeysDb.validateApiKey(limited.key)), "a revoked key still validates");
  });

  // One chat request end to end, with a stub upstream.
  await providersDb.createProviderConnection({
    provider: "openai",
    authType: "apikey",
    name: "openai-stub",
    apiKey: "sk-stub-upstream",
    isActive: true,
    testStatus: "active",
    providerSpecificData: {},
  });
  const chatKey = await apiKeysDb.createApiKey("kividb-compat-chat", "machine-kividb-compat");
  await apiKeysDb.updateApiKeyPermissions(chatKey.id, { rateLimits: [{ limit: 2, window: 60 }] });
  const upstream: string[] = [];
  const realFetch = globalThis.fetch;
  globalThis.fetch = (async (url: any, init: any = {}) => {
    if (!String(url).endsWith("/chat/completions")) return realFetch(url, init);
    const body = JSON.parse(String(init.body || "{}"));
    upstream.push(body.messages?.[0]?.content ?? "");
    return new Response(
      JSON.stringify({
        id: "chatcmpl-stub",
        object: "chat.completion",
        model: body.model,
        choices: [{ index: 0, finish_reason: "stop", message: { role: "assistant", content: `stub ${upstream.length}` } }],
        usage: { prompt_tokens: 4, completion_tokens: 2, total_tokens: 6 },
      }),
      { status: 200, headers: { "content-type": "application/json" } }
    );
  }) as typeof fetch;
  const chat = (n: number) =>
    handleChat(
      new Request("http://localhost/v1/chat/completions", {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${chatKey.key}` },
        body: JSON.stringify({
          model: "openai/gpt-4o-mini",
          stream: false,
          messages: [{ role: "user", content: `hello ${n}` }],
        }),
      })
    );

  await check("chat: /v1/chat/completions through the gateway to a stub upstream; 3rd request is 429", async () => {
    await awayFromMinuteEdge();
    for (const n of [1, 2]) {
      const res = await chat(n);
      const json: any = await res.json();
      assert(res.status === 200, `request ${n}: ${res.status} ${JSON.stringify(json).slice(0, 200)}`);
      assert(json.choices?.[0]?.message?.content === `stub ${n}`, JSON.stringify(json).slice(0, 200));
    }
    const res = await chat(3);
    assert(res.status === 429, `3rd: ${res.status}`);
    assert(upstream.length === 2, `the upstream saw ${upstream.length} requests`);
    const v = await r.get(`${PREFIX}rl:api_key:${chatKey.id}:60:${Math.floor(Date.now() / 60000)}`);
    assert(v === "2", `server counter ${v}`);
  });
  globalThis.fetch = realFetch;
}

// ---------------------------------------------------------------- quota store
async function quotaStore() {
  const { RedisQuotaStore, resetRedisClient } = await load("src/lib/quota/redisQuotaStore.ts");
  const store = new RedisQuotaStore(URL_);
  const dim = { poolId: "pool1", unit: "usd", window: "hourly" };
  const bucket = Math.floor(Date.now() / 3600000);
  const key = `${PREFIX}quota:key1:pool1:usd:hourly:${bucket}`;

  await check("quota store: consume adds the cost (INCRBYFLOAT) and peek reads it back", async () => {
    const a = await store.consume("key1", dim, 0.25);
    const b = await store.consume("key1", dim, 0.25);
    assert(Math.abs(a - 0.25) < 1e-9 && Math.abs(b - 0.5) < 1e-9, `${a} ${b}`);
    assert(Math.abs((await store.peek("key1", dim)) - 0.5) < 1e-9, "peek");
    assert((await r.get(key)) === "0.5", `stored ${await r.get(key)}`);
    const ttl = await r.ttl(key);
    assert(ttl > 7100 && ttl <= 7200, `ttl ${ttl}`);
  });

  // OmniRoute parses the value with parseFloat, so a difference in the last digits does not change
  // its totals: a WARN, not a FAIL (kividbio/kividb#387).
  await check("quota store: INCRBYFLOAT 0.1 + 0.2 is stored as 0.3, as Redis formats it", async () => {
    const d2 = { poolId: "pool2", unit: "usd", window: "hourly" };
    await store.consume("key1", d2, 0.1);
    await store.consume("key1", d2, 0.2);
    const raw = await r.get(`${PREFIX}quota:key1:pool2:usd:hourly:${bucket}`);
    assert(raw === "0.3", `stored ${raw}`);
  }, "WARN");

  await check("quota store: clear", async () => {
    await store.clear("key1", dim);
    assert((await store.peek("key1", dim)) === 0, "not cleared");
  });
  const client: any = await (await load("src/lib/quota/redisQuotaStore.ts")).getRedisClient(URL_);
  await client.quit().catch(() => {});
  resetRedisClient();
}

// ---------------------------------------------------------------- warmup circuit breaker
async function circuitBreaker() {
  const { getCircuitBreakerStore, __resetCircuitBreakerFactory } = await load(
    "src/lib/warmupScheduler/circuitBreakerFactory.ts"
  );
  const store: any = await getCircuitBreakerStore();
  // A real connection, so OmniRoute's SQLite backup of the breaker state has a row to point at.
  const providersDb = await load("src/lib/db/providers.ts");
  const conn = (
    await providersDb.createProviderConnection({
      provider: "openai", authType: "apikey", name: "openai-warmup", apiKey: "sk-stub-warmup",
      isActive: true, testStatus: "active", providerSpecificData: {},
    })
  ).id;
  const key = `${PREFIX}warmup:cb:${conn}`;
  const fail = { success: false, failureKind: "rate_limit", tokensUsed: 0, durationMs: 1 };

  await check("circuit breaker: with REDIS_URL set, OmniRoute picks the Redis store", async () => {
    assert(/Redis/.test(store.constructor.name), `got ${store.constructor.name}`);
  });

  await check("circuit breaker: failures raise the streak and back off (HSET, HGETALL, EXPIRE)", async () => {
    await store.recordResult(conn, fail);
    await store.recordResult(conn, fail);
    const st = await store.get(conn);
    assert(st?.streak === 2 && st.until && st.lastResult === "rate_limit", JSON.stringify(st));
    assert(await store.isInBackoff(conn), "not in backoff");
    const ttl = await r.ttl(key);
    assert(ttl > 86000, `ttl ${ttl}`);
  });

  await check("circuit breaker: a success resets it", async () => {
    await store.recordResult(conn, { success: true, tokensUsed: 1, durationMs: 1 });
    const st = await store.get(conn);
    assert(st?.streak === 0 && !(await store.isInBackoff(conn)), JSON.stringify(st));
  });

  await check("circuit breaker: forbidden is kept without a TTL (PERSIST)", async () => {
    await store.recordResult(conn, fail);
    await store.recordResult(conn, { success: false, failureKind: "forbidden", tokensUsed: 0, durationMs: 1 });
    assert((await r.ttl(key)) === -1, `ttl ${await r.ttl(key)}`);
    assert((await store.get(conn))?.lastResult === "forbidden", "not forbidden");
  });
  __resetCircuitBreakerFactory();
}

// ---------------------------------------------------------------- semantic cache vector store
async function vectorStore() {
  const { RedisVectorStore } = await load("open-sse/services/cache/redisVectorStore.ts");
  const vs = new RedisVectorStore({ redisUrl: URL_, keyPrefix: `${PREFIX}semcache:` });
  const now = Date.now();
  const entry = (id: string, model: string, emb: number[], apiKeyId?: string) => ({
    id, hash: `h-${id}`, embedding: emb, semanticProjectionVersion: 1, promptText: `prompt ${id}`,
    model, provider: "openai", apiKeyId, response: { text: `response ${id}` }, tokensSaved: 10,
    createdAt: now, expiresAt: now + 60000,
  });

  await check("semantic cache: set stores entries with a TTL; get and getByHash read them", async () => {
    await vs.set(entry("a", "m1", [1, 0, 0, 0]), 60000);
    await vs.set(entry("b", "m1", [0.9, 0.1, 0, 0], "key-1"), 60000);
    await vs.set(entry("c", "m2", [0, 1, 0, 0]), 60000);
    assert((await vs.get("a"))?.response?.text === "response a", "get");
    assert((await vs.getByHash("h-c"))?.id === "c", "getByHash");
    const ttl = await r.ttl(`${PREFIX}semcache:entry:a`);
    assert(ttl > 50 && ttl <= 60, `ttl ${ttl}`);
    assert((await vs.getStats()).entries === 3, JSON.stringify(await vs.getStats()));
  });

  await check("semantic cache: nearest search, by model, by API key and above a threshold", async () => {
    const top = await vs.searchNearest([1, 0, 0, 0], { model: "m1" }, 0.5, 2);
    assert(top.map((x: any) => x.entry.id).join() === "a,b", top.map((x: any) => x.entry.id).join());
    const mine = await vs.searchNearest([1, 0, 0, 0], { model: "m1", apiKeyId: "key-1" }, 0.5, 2);
    assert(mine.map((x: any) => x.entry.id).join() === "b", "apiKeyId filter");
    assert((await vs.searchNearest([1, 0, 0, 0], { model: "m2" }, 0.5, 2)).length === 0, "threshold");
  });

  await check("semantic cache: delete, deleteByModel and clear", async () => {
    assert(await vs.delete("a"), "delete");
    assert((await vs.get("a")) === null, "a still there");
    assert((await vs.deleteByModel("m1")) === 1, "deleteByModel");
    await vs.clear();
    assert((await keys(`${PREFIX}semcache:*`)).length === 0, "keys left after clear");
  });
  await vs.close?.();
}

// ---------------------------------------------------------------- main
const info: string = await r.info("server");
const serverVersion = /redis_version:([^\r\n]+)/.exec(info)?.[1] ?? "?";
console.log(`server ${LABEL} at ${URL_} (redis_version ${serverVersion}), key prefix ${PREFIX}`);
await check("connection: PING", async () => assert((await r.ping()) === "PONG", "no PONG"));

await group("The rate limiter (Lua EVAL)", rateLimiter);
await group("API keys: policy, auth cache, and a chat request end to end", apiKeys);
await group("The quota store", quotaStore);
await group("The warmup circuit breaker", circuitBreaker);
await group("The semantic cache's Redis vector store", vectorStore);

for (const k of await keys(`${PREFIX}*`)) await r.del(k);
const left = (await keys(`${PREFIX}*`)).length;
console.error = origError;

const counts: Record<string, number> = { PASS: 0, WARN: 0, FAIL: 0 };
for (const x of results) counts[x.status]++;
console.log(`\n${LABEL}: ${counts.PASS} PASS, ${counts.WARN} WARN, ${counts.FAIL} FAIL of ${results.length} checks` +
  (left ? ` (${left} keys left)` : ""));
const pkg = JSON.parse(fs.readFileSync(path.join(ROOT, "package.json"), "utf8"));
const ioredisPkg = JSON.parse(fs.readFileSync(path.join(ROOT, "node_modules/ioredis/package.json"), "utf8"));
const out = path.join(HERE, `results-${LABEL}.json`);
fs.writeFileSync(out, JSON.stringify({
  label: LABEL, url: URL_, redis_version: serverVersion, omniroute: pkg.version, ioredis: ioredisPkg.version,
  node: process.version, counts, checks: results,
  omniroute_logged: [...new Set(fallbackLog.filter((l) => /REDIS|RATE_LIMITER|Redis/.test(l)))].slice(0, 5),
}, null, 2) + "\n");
console.log(`wrote ${out}`);
await r.quit();
fs.rmSync(process.env.DATA_DIR!, { recursive: true, force: true });
process.exit(counts.FAIL ? 1 : 0);
