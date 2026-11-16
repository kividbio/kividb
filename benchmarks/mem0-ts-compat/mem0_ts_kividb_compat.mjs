#!/usr/bin/env node
// Mem0 TypeScript SDK (mem0ai/oss) <-> Redis-protocol server compatibility harness.
//
// The TypeScript side of ../mem0-compat/: runs the SDK's Redis vector store (RedisDB, on
// node-redis) and its Valkey vector store (ValkeyDB, on iovalkey) against any server, and the
// Memory API on top of the Redis one, recording PASS / FAIL per check. A deterministic stub
// plays the embedding model and no language model is called, so no API keys or spend.
//
//   npm ci
//   node mem0_ts_kividb_compat.mjs --url redis://HOST:6380 --label kividb
//   node mem0_ts_kividb_compat.mjs --url redis://HOST:6379 --label redis8
//
// Writes results-<label>.json next to the script.
//
// NOTE: the stub embedding is a hashed bag of words. It proves the wiring (index creation,
// search, filters), not semantic quality. Mem0's fact extraction (infer: true) needs a
// language model and is not exercised.
//
// Everything it creates has a random name and is dropped at the end. Mem0's telemetry is off.
process.env.MEM0_TELEMETRY = 'false'; // before mem0ai is loaded (it reads it at import)

import { createHash, randomUUID } from 'node:crypto';
import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';

const { values: args } = parseArgs({
  options: { url: { type: 'string', default: 'redis://localhost:6380' }, label: { type: 'string', default: 'kividb' } },
});
const HERE = dirname(fileURLToPath(import.meta.url));
const { Memory, RedisDB, ValkeyDB } = await import('mem0ai/oss');
const { createClient } = await import('redis');

const DIM = 64;
const results = [];
const made = new Set(); // index names this run created
const quiet = { log: console.log, warn: console.warn, error: console.error };

function version(pkg) {
  return JSON.parse(readFileSync(join(HERE, 'node_modules', pkg, 'package.json'), 'utf8')).version;
}

// ------------------------------------------------------------------ stub embedding model
function embed(text) {
  const v = new Array(DIM).fill(0);
  for (const tok of text.toLowerCase().match(/[a-z0-9]+/g) ?? []) {
    v[Number(BigInt(`0x${createHash('md5').update(tok).digest('hex')}`) % BigInt(DIM))] += 1;
  }
  const n = Math.sqrt(v.reduce((s, x) => s + x * x, 0)) || 1;
  return v.map((x) => x / n);
}
const stubEmbedder = { embed: async (text) => embed(text), embedBatch: async (texts) => texts.map(embed) };

// ------------------------------------------------------------------ check runner
function assert(cond, message) {
  if (!cond) throw new Error(message);
}

// The SDK logs every insert and delete; keep the harness output to the checks.
function silence() {
  console.log = () => {};
  console.warn = () => {};
  console.error = () => {};
}
function unsilence() {
  Object.assign(console, quiet);
}

async function check(name, fn) {
  const t0 = Date.now();
  let status = 'PASS';
  let detail = '';
  silence();
  try {
    detail = (await fn()) ?? '';
  } catch (error) {
    status = 'FAIL';
    detail = `${error?.name ?? 'Error'}: ${String(error?.message ?? error).slice(0, 300)}`;
  }
  unsilence();
  results.push({ check: name, status, detail, ms: Date.now() - t0 });
  console.log(`  [${status}] ${name}${detail ? `  (${detail})` : ''}`);
}

async function group(title, fn) {
  console.log(`\n== ${title}`);
  try {
    await fn();
  } catch (error) {
    unsilence();
    const detail = `${error?.name ?? 'Error'}: ${String(error?.message ?? error).slice(0, 300)}`;
    results.push({ check: `${title}: setup`, status: 'FAIL', detail, ms: 0 });
    console.log(`  [FAIL] ${title}: setup  (${detail})`);
  }
}

const unique = (prefix) => {
  const name = `${prefix}_${randomUUID().slice(0, 8)}`;
  made.add(name);
  made.add(`${name}_entities`); // Memory's entity store, made next to the collection
  return name;
};

// ------------------------------------------------------------------ a vector store, used directly
const NOW = new Date().toISOString();
const USER_A = `alice-${randomUUID()}`; // a UUID-style id: its hyphens must be escaped in TAG filters
const ITEMS = [ // id, text, user, agent, run
  ['m-cats', 'I like cats and warm mats', USER_A, 'agent-1', 'run-1'],
  ['m-rates', 'interest rates rose again this quarter', USER_A, 'agent-2', null],
  ['m-rust', 'a new rust compiler release is out', 'bob', 'agent-1', null],
];

function payload(text, user, agent, run) {
  return {
    data: text, userId: user, agentId: agent, hash: createHash('md5').update(text).digest('hex'),
    createdAt: NOW, topic: 'demo', ...(run ? { runId: run } : {}),
  };
}

async function vectorStoreChecks(label, makeStore) {
  const name = unique(`m0ts${label === 'ValkeyDB' ? 'v' : 'r'}`);
  let db;
  await check(`${label}: initialize creates the index`, async () => {
    db = makeStore(name);
    await db.initialize();
  });
  if (!db) return;
  const ids = (rows) => rows.map((x) => x.id).sort();

  await check(`${label}: insert stores the 3 memories`, async () => {
    await db.insert(ITEMS.map(([, t]) => embed(t)), ITEMS.map(([i]) => i), ITEMS.map(([, t, u, a, r]) => payload(t, u, a, r)));
    const [rows, total] = await db.list();
    assert(rows.length === 3, `${rows.length} listed (total ${total})`);
  });
  await check(`${label}: search returns the closest memory first, scores ordered`, async () => {
    const top = await db.search(embed('interest rates rose'), 3);
    assert(top[0]?.id === 'm-rates', ids(top).join());
    assert(top[0].score > 0.5, `score ${top[0].score}`);
    const scores = top.map((x) => x.score);
    assert(scores.every((s, i) => i === 0 || scores[i - 1] >= s), `scores ${scores}`);
  });
  await check(`${label}: topK is honoured`, async () => {
    assert((await db.search(embed('the'), 1)).length === 1, 'topK 1');
    assert((await db.search(embed('the'), 3)).length === 3, 'topK 3');
  });
  await check(`${label}: filter on a UUID-style user_id (hyphens escaped in the TAG query)`, async () => {
    const got = ids(await db.search(embed('anything'), 10, { user_id: USER_A }));
    assert(got.join() === 'm-cats,m-rates', got.join());
  });
  await check(`${label}: filter on user_id and agent_id together`, async () => {
    const got = ids(await db.search(embed('anything'), 10, { user_id: USER_A, agent_id: 'agent-2' }));
    assert(got.join() === 'm-rates', got.join());
  });
  await check(`${label}: filter on run_id`, async () => {
    const got = ids(await db.search(embed('anything'), 10, { run_id: 'run-1' }));
    assert(got.join() === 'm-cats', got.join());
    assert((await db.search(embed('anything'), 10, { run_id: 'no-such-run' })).length === 0, 'a run with no memories matched');
  });
  await check(`${label}: the payload comes back whole`, async () => {
    const [hit] = await db.search(embed('rust compiler'), 1);
    assert(hit.id === 'm-rust' && hit.payload.data === 'a new rust compiler release is out', JSON.stringify(hit.payload));
    assert(hit.payload.user_id === 'bob' && hit.payload.agent_id === 'agent-1' && hit.payload.topic === 'demo', JSON.stringify(hit.payload));
    assert(String(hit.payload.createdAt).slice(0, 10) === NOW.slice(0, 10), `createdAt ${hit.payload.createdAt}`);
  });
  await check(`${label}: get by id, and null for an id that does not exist`, async () => {
    const got = await db.get('m-rust');
    assert(got?.id === 'm-rust' && got.payload.data === 'a new rust compiler release is out', JSON.stringify(got));
    assert((await db.get('no-such-id')) === null, 'a missing id did not give null');
  });
  await check(`${label}: update changes the text and the vector`, async () => {
    await db.update('m-cats', embed('I like dogs'), { ...payload('I like dogs', USER_A, 'agent-1', 'run-1'), updatedAt: new Date().toISOString() });
    assert((await db.get('m-cats')).payload.data === 'I like dogs', 'text not updated');
    assert((await db.search(embed('I like dogs'), 1))[0].id === 'm-cats', 'vector not updated');
  });
  await check(`${label}: list, with a filter and with topK`, async () => {
    const [rows] = await db.list({ user_id: USER_A });
    assert(ids(rows).join() === 'm-cats,m-rates', ids(rows).join());
    assert((await db.list(undefined, 2))[0].length === 2, 'topK 2');
  });
  await check(`${label}: delete`, async () => {
    await db.delete('m-rates');
    const [rows] = await db.list();
    assert(ids(rows).join() === 'm-cats,m-rust', ids(rows).join());
  });
  await check(`${label}: getUserId and setUserId (the SDK's telemetry id key)`, async () => {
    await db.setUserId(`compat-${name}`);
    assert((await db.getUserId()) === `compat-${name}`, 'not read back');
  });
  await check(`${label}: deleteCol drops the index`, async () => {
    await db.deleteCol();
    const list = (await raw.sendCommand(['FT._LIST'])).map(String);
    assert(!list.includes(name), 'index still listed');
  });
  await db.close?.();
}

// ------------------------------------------------------------------ Memory, the public API
async function memoryChecks(url) {
  const name = unique('m0tsm');
  let memory;
  const madeIds = {};
  await check('Memory: initialize (provider "redis")', async () => {
    memory = new Memory({
      vectorStore: { provider: 'redis', config: { redisUrl: url, collectionName: name, embeddingModelDims: DIM, dimension: DIM } },
      // Never called: infer is false, and the embedder is replaced below.
      embedder: { provider: 'openai', config: { apiKey: 'unused', embeddingDims: DIM } },
      llm: { provider: 'openai', config: { apiKey: 'unused' } },
      historyStore: { provider: 'sqlite', config: { historyDbPath: join(mkdtempSync(join(tmpdir(), 'mem0ts-')), 'history.db') } },
    });
    memory.embedder = stubEmbedder;
    await memory.getAll({ filters: { user_id: 'nobody' } }); // waits for the store to be ready
  });
  if (!memory) return;

  await check('Memory: add (infer: false) stores a memory for a user', async () => {
    for (const [who, text] of [[USER_A, 'I love hiking in the mountains'], [USER_A, 'My cat is called Tom'], ['bob', 'I prefer tea to coffee']]) {
      const out = (await memory.add(text, { userId: who, infer: false })).results;
      assert(out.length === 1 && out[0].metadata?.event === 'ADD', JSON.stringify(out));
      madeIds[text] = out[0].id;
    }
  });
  await check('Memory: search is scoped to the user and finds the closest', async () => {
    const top = (await memory.search('hiking in the mountains', { filters: { user_id: USER_A }, topK: 3 })).results;
    assert(top[0]?.memory === 'I love hiking in the mountains', JSON.stringify(top.map((x) => x.memory)));
    assert(top.every((x) => x.user_id === USER_A), JSON.stringify(top.map((x) => x.user_id)));
  });
  await check('Memory: search does not return another user\'s memory (bob\'s own search finds it)', async () => {
    const own = (await memory.search('tea coffee', { filters: { user_id: 'bob' }, topK: 5 })).results;
    assert(own.map((x) => x.memory).join() === 'I prefer tea to coffee', JSON.stringify(own.map((x) => x.memory)));
    const other = (await memory.search('tea coffee', { filters: { user_id: USER_A }, topK: 5 })).results;
    assert(other.every((x) => !x.memory.includes('tea')), 'another user\'s memory came back');
  });
  await check('Memory: getAll for a user', async () => {
    const got = (await memory.getAll({ filters: { user_id: USER_A } })).results.map((x) => x.memory).sort();
    assert(got.join('|') === 'I love hiking in the mountains|My cat is called Tom', got.join('|'));
  });
  await check('Memory: get by id', async () => {
    const got = await memory.get(madeIds['My cat is called Tom']);
    assert(got?.memory === 'My cat is called Tom' && got.user_id === USER_A, JSON.stringify(got));
  });
  await check('Memory: update', async () => {
    await memory.update(madeIds['My cat is called Tom'], 'My cat is called Felix');
    assert((await memory.get(madeIds['My cat is called Tom'])).memory === 'My cat is called Felix', 'not updated');
  });
  await check('Memory: history records the ADD and the UPDATE (Mem0\'s local SQLite file, not the server)', async () => {
    const rows = await memory.history(madeIds['My cat is called Tom']);
    const events = rows.map((h) => h.event ?? h.action).sort();
    assert(events.join() === 'ADD,UPDATE', JSON.stringify(rows));
  });
  await check('Memory: delete', async () => {
    await memory.delete(madeIds['My cat is called Tom']);
    const got = (await memory.getAll({ filters: { user_id: USER_A } })).results.map((x) => x.memory);
    assert(got.join() === 'I love hiking in the mountains', got.join());
  });
  await check('Memory: deleteAll removes one user\'s memories only', async () => {
    await memory.deleteAll({ userId: USER_A });
    assert((await memory.getAll({ filters: { user_id: USER_A } })).results.length === 0, 'memories left');
    assert((await memory.getAll({ filters: { user_id: 'bob' } })).results.length === 1, 'another user\'s memory was deleted');
  });
}

// ------------------------------------------------------------------ main
let raw;
async function cleanup() {
  const list = (await raw.sendCommand(['FT._LIST'])).map(String);
  for (const idx of list) {
    if (made.has(idx)) await raw.sendCommand(['FT.DROPINDEX', idx, 'DD']).catch(() => {});
  }
  for (const idx of made) {
    for await (const keys of raw.scanIterator({ MATCH: `mem0:${idx}:*`, COUNT: 500 })) {
      for (const key of [].concat(keys)) await raw.del(key);
    }
  }
}

async function main() {
  raw = createClient({ url: args.url });
  raw.on('error', () => {});
  await raw.connect();
  const info = await raw.info('server');
  const serverVersion = (info.match(/redis_version:(\S+)/) ?? [])[1] ?? '?';
  console.log(`server ${args.label} at ${args.url} (redis_version ${serverVersion})`);

  await check('connection: PING, and MODULE LIST names the search module (RedisDB refuses to start without it)', async () => {
    assert((await raw.ping()) === 'PONG', 'no PONG');
    const mods = await raw.sendCommand(['MODULE', 'LIST']);
    const names = mods.map((m) => {
      const flat = Array.isArray(m) ? m.map(String) : [];
      const i = flat.indexOf('name');
      return i >= 0 ? flat[i + 1].toLowerCase() : '';
    });
    assert(names.includes('search') || names.includes('searchlight'), `modules: ${names.join(', ')}`);
    return `modules: ${names.join(', ')}`;
  });

  await group('RedisDB, the Redis vector store (node-redis)', () =>
    vectorStoreChecks('RedisDB', (name) => new RedisDB({ redisUrl: args.url, collectionName: name, embeddingModelDims: DIM })));
  await group('ValkeyDB, the Valkey vector store (iovalkey)', () =>
    vectorStoreChecks('ValkeyDB', (name) => new ValkeyDB({ valkeyUrl: args.url, collectionName: name, embeddingModelDims: DIM })));
  await group('Memory, the public API, on the Redis store', () => memoryChecks(args.url));
  await cleanup();
  await raw.del('memory_migrations:1').catch(() => {});

  const counts = { PASS: results.filter((r) => r.status === 'PASS').length, FAIL: results.filter((r) => r.status === 'FAIL').length };
  console.log(`\n${args.label}: ${counts.PASS} PASS, ${counts.FAIL} FAIL of ${results.length} checks`);
  const out = join(HERE, `results-${args.label}.json`);
  writeFileSync(out, JSON.stringify({
    label: args.label,
    url: args.url,
    redis_version: serverVersion,
    mem0ai: version('mem0ai'),
    redis: version('redis'),
    iovalkey: version('iovalkey'),
    counts,
    checks: results,
  }, null, 2) + '\n');
  console.log(`wrote ${out}`);
  await raw.quit();
  return counts.FAIL ? 1 : 0;
}

main().then((code) => process.exit(code), (error) => {
  unsilence();
  console.error(error);
  process.exit(2);
});
