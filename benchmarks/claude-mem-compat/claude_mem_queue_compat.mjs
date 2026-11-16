#!/usr/bin/env node
// claude-mem's server job queue (BullMQ) <-> Redis-protocol server compatibility harness.
//
// Drives a running claude-mem server stack (its own docker-compose.yml: Postgres, the HTTP
// server, the BullMQ generation worker, and the Redis-protocol server under test in place of
// its Valkey) through the HTTP API, and records PASS / FAIL per check. The worker generates
// with stub-provider.mjs, loaded through claude-mem's own custom-provider setting, so jobs
// are really processed, retried and failed with no model and no API key.
//
//   COMPOSE="docker compose -p cmq -f docker-compose.yml -f compose.kividb.yml -f compose.harness.yml" \
//   LABEL=kividb node claude_mem_queue_compat.mjs
//
// run.sh brings the stack up and calls this with the right COMPOSE for each server.
//
// Environment:
//   COMPOSE    the docker compose command for the stack (required; used to stop, start and
//              restart services, to create API keys and to read the worker's log)
//   LABEL      name for the results file, results-<LABEL>.json (default: kividb)
//   BASE_URL   claude-mem's HTTP server (default http://127.0.0.1:37877)
//   REDIS_URL  the server under test, as published by the compose files (default
//              redis://127.0.0.1:6399); read directly to check what BullMQ stored there
//   CLAUDE_MEM_REF  the claude-mem tag or commit under test, recorded in the results
import { execSync } from 'node:child_process';
import { writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import Redis from 'ioredis';

const COMPOSE = process.env.COMPOSE;
if (!COMPOSE) {
  console.error('COMPOSE is required (the docker compose command for the stack)');
  process.exit(2);
}
const LABEL = process.env.LABEL ?? 'kividb';
const BASE = process.env.BASE_URL ?? 'http://127.0.0.1:37877';
const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6399';
const RUN = `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`;
const EVENT_QUEUE = 'server_beta_generate_event';
const SUMMARY_QUEUE = 'server_beta_generate_summary';

const results = [];
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function assert(cond, message) {
  if (!cond) throw new Error(message);
}

async function check(name, fn) {
  const t0 = Date.now();
  let status = 'PASS';
  let detail = '';
  try {
    detail = (await fn()) ?? '';
  } catch (error) {
    status = 'FAIL';
    detail = `${error?.name ?? 'Error'}: ${String(error?.message ?? error).slice(0, 400)}`;
  }
  const ms = Date.now() - t0;
  results.push({ check: name, status, detail, ms });
  console.log(`  [${status}] ${name}${detail ? `  (${detail})` : ''}`);
}

// ------------------------------------------------------------------ stack control
function compose(args, { quiet = true } = {}) {
  return execSync(`${COMPOSE} ${args}`, {
    encoding: 'utf8',
    stdio: quiet ? ['ignore', 'pipe', 'pipe'] : 'inherit',
    maxBuffer: 64 * 1024 * 1024,
  });
}

async function waitFor(what, fn, timeoutMs = 120_000, everyMs = 500) {
  const deadline = Date.now() + timeoutMs;
  let last;
  while (Date.now() < deadline) {
    try {
      const value = await fn();
      if (value) return value;
    } catch (error) {
      last = error;
    }
    await sleep(everyMs);
  }
  throw new Error(`timed out after ${timeoutMs} ms waiting for ${what}${last ? ` (last error: ${last.message})` : ''}`);
}

async function waitServerReady() {
  await waitFor('the HTTP server', async () => {
    const health = await fetch(`${BASE}/healthz`);
    const ready = await fetch(`${BASE}/api/readiness`);
    return health.ok && ready.ok;
  }, 180_000, 1000);
}

// The worker prints {"status":"worker-running"} once its BullMQ Workers are attached. It has no
// HTTP port, so its log is the only readiness signal; the stub provider logs each call there too.
function workerLog() {
  return compose('logs --no-color claude-mem-worker');
}

function stubCalls() {
  const calls = new Map();
  for (const line of workerLog().split('\n')) {
    const m = line.match(/\[stub-provider\] job=(\S+)/);
    if (m) calls.set(m[1], (calls.get(m[1]) ?? 0) + 1);
  }
  return calls;
}

function stubCallsByContainer() {
  const by = new Map();
  for (const line of workerLog().split('\n')) {
    const m = line.match(/^(\S+)\s+\|.*\[stub-provider\] job=(\S+)/);
    if (m) by.set(m[1], (by.get(m[1]) ?? 0) + 1);
  }
  return by;
}

let redis;
async function redisUp() {
  await waitFor('PING on the Redis-protocol server', async () => (await redis.ping()) === 'PONG', 60_000);
}

// ------------------------------------------------------------------ HTTP API
let apiKey;
let projectId;

async function api(path, { method, json, key = apiKey } = {}) {
  const response = await fetch(`${BASE}${path}`, {
    method: method ?? (json === undefined ? 'GET' : 'POST'),
    headers: {
      ...(json !== undefined ? { 'content-type': 'application/json' } : {}),
      ...(key ? { authorization: `Bearer ${key}` } : {}),
    },
    body: json === undefined ? undefined : JSON.stringify(json),
  });
  const text = await response.text();
  let body = null;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = { raw: text };
  }
  return { status: response.status, body };
}

function createKey(name) {
  const out = compose(
    `exec -T claude-mem-server bun /opt/claude-mem/scripts/server-service.cjs server api-key create --name ${name} ` +
      '--scope events:write,sessions:write,observations:read,jobs:read,memories:read,memories:write',
  );
  const json = JSON.parse(out.slice(out.indexOf('{')));
  return json;
}

async function startSession(tag) {
  const { status, body } = await api('/v1/sessions/start', {
    json: { projectId, contentSessionId: `content-${RUN}-${tag}`, platformSource: 'kividb-compat' },
  });
  assert(status === 201 && body?.session?.id, `session start: HTTP ${status} ${JSON.stringify(body)}`);
  return body.session;
}

async function postEvent(session, stub, n = 0) {
  const { status, body } = await api('/v1/events', {
    json: {
      projectId,
      serverSessionId: session.id,
      sourceType: 'api',
      eventType: 'observation.created',
      contentSessionId: `content-${RUN}-${session.id}`,
      payload: { tool_name: 'Read', stub, run: RUN, n },
      occurredAtEpoch: Date.now(),
    },
  });
  assert(status === 201 && body?.event?.id, `event: HTTP ${status} ${JSON.stringify(body)}`);
  assert(body.generationJob?.id, `event response has no generationJob: ${JSON.stringify(body)}`);
  return { event: body.event, job: body.generationJob };
}

async function job(id) {
  const { status, body } = await api(`/v1/jobs/${id}`);
  assert(status === 200, `GET /v1/jobs/${id}: HTTP ${status} ${JSON.stringify(body)}`);
  return body.generationJob;
}

async function waitJob(id, wanted, timeoutMs = 90_000) {
  const states = Array.isArray(wanted) ? wanted : [wanted];
  let last;
  try {
    return await waitFor(`job ${id} to be ${states.join('/')}`, async () => {
      last = await job(id);
      return states.includes(last.status) ? last : null;
    }, timeoutMs);
  } catch (error) {
    throw new Error(`${error.message}; last seen ${JSON.stringify(last && { status: last.status, attempts: last.attempts, lastError: last.lastError })}`);
  }
}

async function observations(eventId) {
  const { status, body } = await api(`/v1/events/${eventId}/observations`);
  assert(status === 200, `GET observations: HTTP ${status}`);
  return body.observations;
}

async function queueHealth() {
  const { status, body } = await api('/api/health', { key: null });
  assert(status === 200, `/api/health: HTTP ${status}`);
  return body.queue;
}

// BullMQ's keys for a queue: <prefix>:<queue>:<suffix>.
let prefix;
const k = (queue, suffix) => `${prefix}:${queue}:${suffix}`;

async function inList(queue, list, bullmqId) {
  const ids = await redis.lrange(k(queue, list), 0, -1);
  return ids.includes(bullmqId);
}

async function inZset(queue, zset, bullmqId) {
  return (await redis.zscore(k(queue, zset), bullmqId)) !== null;
}

async function expectOneObservation(eventId, marker) {
  const obs = await observations(eventId);
  assert(obs.length === 1, `${obs.length} observations for event ${eventId}, expected 1`);
  assert(String(obs[0].content ?? JSON.stringify(obs[0])).includes(`(${marker})`),
    `observation does not carry the stub's text: ${JSON.stringify(obs[0]).slice(0, 200)}`);
}

// ------------------------------------------------------------------ checks
async function main() {
  console.log(`claude-mem queue harness: ${LABEL}, run ${RUN}`);
  redis = new Redis(REDIS_URL, { maxRetriesPerRequest: null, lazyConnect: false });
  redis.on('error', () => {});
  await redisUp();
  const info = await redis.info('server');
  const serverVersion = (info.match(/redis_version:(\S+)/) ?? [])[1] ?? '?';
  const kividbVersion = (info.match(/kividb_version:(\S+)/) ?? [])[1] ?? null;
  await waitServerReady();
  await waitFor('the generation worker to attach its BullMQ workers',
    () => /"status":"worker-running"/.test(workerLog()), 120_000, 1000);

  const key = createKey(`kividb-compat-${RUN}`);
  apiKey = key.key;
  projectId = key.projectId;
  assert(apiKey && projectId, `api-key create gave ${JSON.stringify(key)}`);
  const session = await startSession('main');

  await check('connection: the server answers PING and claude-mem reports the BullMQ queue healthy', async () => {
    assert((await redis.ping()) === 'PONG', 'no PONG');
    const q = await queueHealth();
    assert(q?.engine === 'bullmq', `queue engine ${JSON.stringify(q)}`);
    assert(q?.redis?.status === 'ok', `redis status ${JSON.stringify(q?.redis)}`);
    assert(q.lanes?.length === 2 && q.lanes.every((l) => !l.unavailable), `lanes ${JSON.stringify(q.lanes)}`);
    prefix = q.redis.prefix;
    return `prefix ${prefix}`;
  });

  // BullMQ's worker writes <prefix>:<queue>:stalled-check every 30 s; its prefix has to be the one
  // the server enqueues under. With claude-mem's stock docker-compose.yml it is not (compose.harness.yml).
  await check('the worker consumes the prefix the server enqueues under', async () => {
    const prefixes = await waitFor('the worker\'s stalled-check key', async () => {
      const found = new Set();
      for await (const keys of redis.scanStream({ match: `*:${EVENT_QUEUE}:stalled-check`, count: 1000 })) {
        for (const key of keys) found.add(key.slice(0, key.length - `:${EVENT_QUEUE}:stalled-check`.length));
      }
      return found.size ? [...found] : null;
    }, 90_000, 1000);
    assert(prefixes.length === 1 && prefixes[0] === prefix, `server enqueues under ${prefix}, worker uses ${prefixes.join(', ')}`);
    return `both ${prefix}`;
  });

  let first;
  let processing = false;
  await check('enqueue and process: an event\'s job completes and stores one observation', async () => {
    first = await postEvent(session, 'ok');
    const done = await waitJob(first.job.id, 'completed');
    assert(done.attempts === 1, `attempts ${done.attempts}`);
    await expectOneObservation(first.event.id, 'ok');
    processing = true;
  });
  if (!processing) {
    // Every later check needs a job to be processed; each would only time out.
    results.push({ check: 'the remaining checks', status: 'FAIL', detail: 'not run: no job was processed', ms: 0 });
    console.log('  [FAIL] the remaining checks  (not run: no job was processed)');
    return finish(serverVersion, kividbVersion);
  }

  await check('the job is in BullMQ under claude-mem\'s prefix, with its deterministic evt_ id, and completed there', async () => {
    const bullmqId = first.job.bullmqJobId;
    assert(/^evt_[0-9a-f]{64}$/.test(bullmqId ?? ''), `bullmqJobId ${bullmqId}`);
    assert(await redis.exists(k(EVENT_QUEUE, bullmqId)), `no hash ${k(EVENT_QUEUE, bullmqId)}`);
    assert(await inZset(EVENT_QUEUE, 'completed', bullmqId), 'not in the completed set');
    const data = JSON.parse(await redis.hget(k(EVENT_QUEUE, bullmqId), 'data'));
    assert(data.generation_job_id === first.job.id, `job data ${JSON.stringify(data)}`);
    return `${k(EVENT_QUEUE, '<id>')}`;
  });

  await check('a burst of 20 events: every job completes, one observation and one provider call each', async () => {
    const posted = await Promise.all(Array.from({ length: 20 }, (_, i) => postEvent(session, 'ok', i)));
    for (const p of posted) await waitJob(p.job.id, 'completed');
    const calls = stubCalls();
    for (const p of posted) {
      await expectOneObservation(p.event.id, 'ok');
      assert(calls.get(p.job.id) === 1, `job ${p.job.id} called the provider ${calls.get(p.job.id) ?? 0} times`);
    }
  });

  await check('retry: a rate-limited job waits in BullMQ\'s delayed set for its backoff, then completes on attempt 2', async () => {
    const p = await postEvent(session, 'flaky');
    await waitFor('the job in the delayed set', () => inZset(EVENT_QUEUE, 'delayed', p.job.bullmqJobId), 30_000, 200);
    const done = await waitJob(p.job.id, 'completed', 60_000);
    assert(done.attempts === 2, `attempts ${done.attempts}`);
    await expectOneObservation(p.event.id, 'flaky');
    assert(stubCalls().get(p.job.id) === 2, `provider calls ${stubCalls().get(p.job.id)}`);
    const made = await redis.hget(k(EVENT_QUEUE, p.job.bullmqJobId), 'atm'); // BullMQ 5's attemptsMade
    assert(Number(made) === 2, `BullMQ attemptsMade ${made}`);
    return `BullMQ attemptsMade ${made}`;
  });

  await check('failure: a job rate-limited every time fails after its 3 attempts, in Postgres and in BullMQ\'s failed set', async () => {
    const p = await postEvent(session, 'always-rate-limit');
    const failed = await waitJob(p.job.id, 'failed', 90_000);
    assert(failed.attempts === failed.maxAttempts && failed.attempts === 3, `attempts ${failed.attempts}/${failed.maxAttempts}`);
    assert((await observations(p.event.id)).length === 0, 'an observation was stored');
    await waitFor('the job in BullMQ\'s failed set', () => inZset(EVENT_QUEUE, 'failed', p.job.bullmqJobId), 30_000);
    const [made, reason] = await redis.hmget(k(EVENT_QUEUE, p.job.bullmqJobId), 'atm', 'failedReason');
    assert(Number(made) === 3, `BullMQ attemptsMade ${made}`);
    assert(/rate limited/.test(reason ?? ''), `failedReason ${reason}`);
    return `failedReason "${reason}"`;
  });

  let rejected;
  await check('non-retryable error: the job fails on its first attempt and stores nothing', async () => {
    rejected = await postEvent(session, 'reject-once');
    const failed = await waitJob(rejected.job.id, 'failed');
    assert(failed.attempts === 1, `attempts ${failed.attempts}`);
    assert((await observations(rejected.event.id)).length === 0, 'an observation was stored');
    await sleep(3000); // BullMQ's own retry of the throw finds the row failed and does not call the provider again
    assert(stubCalls().get(rejected.job.id) === 1, `provider calls ${stubCalls().get(rejected.job.id)}`);
  });

  await check('operator retry: POST /v1/jobs/:id/retry re-enqueues the failed job in BullMQ and it completes', async () => {
    const { status, body } = await api(`/v1/jobs/${rejected.job.id}/retry`, { json: {} });
    assert(status === 200 && body.alreadyQueued === false, `retry: HTTP ${status} ${JSON.stringify(body)}`);
    await waitJob(rejected.job.id, 'completed');
    await expectOneObservation(rejected.event.id, 'reject-once');
    assert(stubCalls().get(rejected.job.id) === 2, `provider calls ${stubCalls().get(rejected.job.id)}`);
  });

  await check('cancel: a job cancelled while it waits is removed from BullMQ and never runs', async () => {
    compose('stop claude-mem-worker');
    try {
      const p = await postEvent(session, 'ok', 'cancel');
      await waitFor('the job in the wait list', () => inList(EVENT_QUEUE, 'wait', p.job.bullmqJobId), 10_000, 200);
      const { status, body } = await api(`/v1/jobs/${p.job.id}/cancel`, { json: {} });
      assert(status === 200 && body.generationJob.status === 'cancelled', `cancel: HTTP ${status} ${JSON.stringify(body)}`);
      assert(!(await inList(EVENT_QUEUE, 'wait', p.job.bullmqJobId)), 'still in the wait list');
      assert(!(await redis.exists(k(EVENT_QUEUE, p.job.bullmqJobId))), 'job hash still there');
      compose('start claude-mem-worker');
      await sleep(5000);
      assert((await job(p.job.id)).status === 'cancelled', 'status changed');
      assert(!stubCalls().has(p.job.id), 'the provider was called');
      assert((await observations(p.event.id)).length === 0, 'an observation was stored');
    } finally {
      compose('start claude-mem-worker');
    }
  });

  let waiting = [];
  await check('worker down: 10 events wait in BullMQ\'s wait list, none is lost', async () => {
    compose('stop claude-mem-worker');
    waiting = [];
    for (let i = 0; i < 10; i++) waiting.push(await postEvent(session, 'ok', `down-${i}`));
    for (const p of waiting) {
      assert(await inList(EVENT_QUEUE, 'wait', p.job.bullmqJobId), `${p.job.bullmqJobId} not in the wait list`);
    }
    const q = await queueHealth();
    const lane = q.lanes.find((l) => l.name === EVENT_QUEUE);
    assert(lane.waiting >= 10, `lane ${JSON.stringify(lane)}`);
    return `waiting ${lane.waiting}`;
  });

  await check('server restart (append-only file on): the 10 waiting jobs are still there after it', async () => {
    compose('restart valkey');
    await redisUp();
    for (const p of waiting) {
      assert(await inList(EVENT_QUEUE, 'wait', p.job.bullmqJobId), `${p.job.bullmqJobId} lost from the wait list`);
      assert(await redis.exists(k(EVENT_QUEUE, p.job.bullmqJobId)), `${p.job.bullmqJobId} job hash lost`);
    }
    await waitFor('claude-mem to see the queue again', async () => (await queueHealth())?.redis?.status === 'ok', 60_000, 1000);
  });

  await check('worker back: the 10 jobs are processed, one observation each', async () => {
    compose('start claude-mem-worker');
    for (const p of waiting) await waitJob(p.job.id, 'completed', 120_000);
    const calls = stubCalls();
    for (const p of waiting) {
      await expectOneObservation(p.event.id, 'ok');
      assert(calls.get(p.job.id) === 1, `job ${p.job.id}: ${calls.get(p.job.id) ?? 0} provider calls`);
    }
  });

  await check('a retry\'s backoff survives a server restart: the delayed job stays delayed, then completes', async () => {
    const p = await postEvent(session, 'flaky', 'restart');
    await waitFor('the job in the delayed set', () => inZset(EVENT_QUEUE, 'delayed', p.job.bullmqJobId), 30_000, 100);
    compose('stop claude-mem-worker');
    compose('restart valkey');
    await redisUp();
    assert(await inZset(EVENT_QUEUE, 'delayed', p.job.bullmqJobId), 'lost from the delayed set');
    compose('start claude-mem-worker');
    const done = await waitJob(p.job.id, 'completed', 90_000);
    assert(done.attempts === 2, `attempts ${done.attempts}`);
    await expectOneObservation(p.event.id, 'flaky');
  });

  await check('claude-mem restart: server and worker come back, earlier jobs keep their state, new jobs run', async () => {
    compose('restart claude-mem-server claude-mem-worker');
    await waitServerReady();
    assert((await job(first.job.id)).status === 'completed', 'first job lost its state');
    const p = await postEvent(session, 'ok', 'after-restart');
    await waitJob(p.job.id, 'completed', 120_000);
    await expectOneObservation(p.event.id, 'ok');
  });

  await check('two workers: 12 slow jobs are shared between them, each processed exactly once', async () => {
    compose('up -d --no-recreate --scale claude-mem-worker=2 claude-mem-worker');
    try {
      await waitFor('the second worker', () => {
        const names = new Set([...workerLog().matchAll(/^(\S+)\s+\|.*"status":"worker-running"/gm)].map((m) => m[1]));
        return names.size >= 2;
      }, 120_000, 1000);
      const before = stubCallsByContainer();
      const posted = await Promise.all(Array.from({ length: 12 }, (_, i) => postEvent(session, 'slow', `two-${i}`)));
      for (const p of posted) await waitJob(p.job.id, 'completed', 120_000);
      const calls = stubCalls();
      for (const p of posted) {
        await expectOneObservation(p.event.id, 'slow');
        assert(calls.get(p.job.id) === 1, `job ${p.job.id}: ${calls.get(p.job.id) ?? 0} provider calls`);
      }
      const after = stubCallsByContainer();
      const share = [...after].map(([name, n]) => [name, n - (before.get(name) ?? 0)]).filter(([, n]) => n > 0);
      assert(share.length >= 2, `only ${share.length} worker took jobs: ${JSON.stringify(share)}`);
      return share.map(([name, n]) => `${name.replace(/.*worker-/, 'worker ')}: ${n}`).join(', ');
    } finally {
      compose('up -d --no-recreate --scale claude-mem-worker=1 claude-mem-worker');
    }
  });

  await check('session end: the summary job (the second queue) completes', async () => {
    const s = await startSession('summary');
    const p = await postEvent(s, 'ok', 'summary');
    await waitJob(p.job.id, 'completed');
    const { status, body } = await api(`/v1/sessions/${s.id}/end`, { json: {} });
    assert(status === 200 && body.generationJob?.id, `end: HTTP ${status} ${JSON.stringify(body)}`);
    const done = await waitJob(body.generationJob.id, 'completed');
    assert(done.sourceType === 'session_summary', `sourceType ${done.sourceType}`);
    assert(/^sum_[0-9a-f]{64}$/.test(done.bullmqJobId ?? ''), `bullmqJobId ${done.bullmqJobId}`);
    assert(await inZset(SUMMARY_QUEUE, 'completed', done.bullmqJobId), 'not in the summary queue\'s completed set');
  });

  await check('at the end: nothing left active or waiting, and no job reported stalled', async () => {
    const q = await waitFor('the queues to drain', async () => {
      const health = await queueHealth();
      return health.lanes.every((l) => l.active === 0 && l.waiting === 0 && l.delayed === 0) ? health : null;
    }, 30_000, 1000);
    const stalled = q.lanes.reduce((n, l) => n + (l.stalled ?? 0), 0);
    assert(stalled === 0, `stalled ${stalled}`);
    return q.lanes.map((l) => `${l.kind}: ${l.completed} completed, ${l.failed} failed`).join('; ');
  });

  return finish(serverVersion, kividbVersion);
}

async function finish(serverVersion, kividbVersion) {
  // What BullMQ sent, for the report.
  let commandstats = {};
  try {
    const raw = await redis.info('commandstats');
    for (const m of raw.matchAll(/^cmdstat_([^:]+):calls=(\d+)/gm)) commandstats[m[1]] = Number(m[2]);
  } catch {
    commandstats = null;
  }

  const counts = { PASS: results.filter((r) => r.status === 'PASS').length, FAIL: results.filter((r) => r.status === 'FAIL').length };
  console.log(`\n${LABEL}: ${counts.PASS} PASS, ${counts.FAIL} FAIL of ${results.length} checks`);
  const out = join(dirname(fileURLToPath(import.meta.url)), `results-${LABEL}.json`);
  writeFileSync(out, JSON.stringify({
    label: LABEL,
    redis_url: REDIS_URL,
    redis_version: serverVersion,
    kividb_version: kividbVersion,
    claude_mem: process.env.CLAUDE_MEM_REF ?? null,
    bullmq_prefix: prefix,
    counts,
    checks: results,
    commandstats,
  }, null, 2) + '\n');
  console.log(`wrote ${out}`);
  redis.disconnect();
  return counts.FAIL ? 1 : 0;
}

main().then((code) => process.exit(code), (error) => {
  console.error(error);
  process.exit(2);
});
