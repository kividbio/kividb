// BullMQ throughput against one endpoint.
//   REDIS_URL=... LABEL=kividb N=200000 PRODUCERS=8 WORKERS=8 CONCURRENCY=100 node bullmq-bench.cjs
// Three numbers: jobs added a second, jobs processed a second (no-op job, queue
// pre-filled, so this is the queue's cost and not the job's), and both at once.
// Workers run in this process; its CPU is reported so a client-bound run is visible.
const { Queue, Worker } = require("bullmq");
const IORedis = require("ioredis");
const os = require("os");

const url = process.env.REDIS_URL;
const LABEL = process.env.LABEL || "store";
const N = Number(process.env.N || 100000);
const PRODUCERS = Number(process.env.PRODUCERS || 8);
const WORKERS = Number(process.env.WORKERS || 8);
const CONCURRENCY = Number(process.env.CONCURRENCY || 100);
const PAYLOAD = "x".repeat(Number(process.env.PAYLOAD || 200));
// braces: fair to stores that route by hashtag. TAG tells apart the processes of one run,
// each with a queue of its own, which may start in the same millisecond.
// PREFIX names the queue's hash tag outright: to put each queue of a run on a shard of its
// own in a cluster.
const prefix = process.env.PREFIX || "{bench" + Date.now().toString(36) + (process.env.TAG || "") + "}";
// CLUSTER=1: REDIS_URL is one node of a cluster, and the client follows the slots.
const conn = () => {
  if (!process.env.CLUSTER) return new IORedis(url, { maxRetriesPerRequest: null, enableAutoPipelining: false });
  const node = new URL(url);
  return new IORedis.Cluster([{ host: node.hostname, port: Number(node.port || 6379) }], {
    enableAutoPipelining: false,
    redisOptions: { maxRetriesPerRequest: null },
  });
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const cpu = () => { const u = process.cpuUsage(); return (u.user + u.system) / 1e6; };

async function addJobs(queues, n) {
  const per = Math.ceil(n / queues.length);
  await Promise.all(queues.map(async (q) => {
    for (let i = 0; i < per; i += 500) {
      await q.addBulk(Array.from({ length: Math.min(500, per - i) }, (_, k) => ({ name: "j", data: { k, p: PAYLOAD }, opts: { removeOnComplete: true, removeOnFail: true } })));
    }
  }));
  return per * queues.length;
}

(async () => {
  const admin = conn();
  const info = await admin.info("server");
  const ver = (/kividb_version:(\S+)/.exec(info) || /redis_version:(\S+)/.exec(info) || [])[1];
  const producers = Array.from({ length: PRODUCERS }, () => new Queue("bench", { connection: conn(), prefix }));
  const counter = new Queue("bench", { connection: conn(), prefix });
  const out = { label: LABEL, version: ver, n: N, producers: PRODUCERS, workers: WORKERS, concurrency: CONCURRENCY, payload: PAYLOAD.length, client_cores: os.cpus().length };

  // 1. Add only.
  let t = Date.now(), c0 = cpu();
  const added = await addJobs(producers, N);
  let secs = (Date.now() - t) / 1000;
  out.add_per_s = Math.round(added / secs); out.add_client_cpu = +((cpu() - c0) / secs).toFixed(2);

  // 2. Process only, from the pre-filled queue.
  let done = 0;
  const lat = [];
  const mk = () => new Worker("bench", async (job) => { done++; if (done % 50 === 0) lat.push(Date.now() - job.timestamp); }, { connection: conn(), prefix, concurrency: CONCURRENCY });
  t = Date.now(); c0 = cpu();
  let workers = Array.from({ length: WORKERS }, mk);
  while (done < added) { await sleep(50); if (Date.now() - t > 900000) break; }
  secs = (Date.now() - t) / 1000;
  out.process_per_s = Math.round(done / secs); out.process_client_cpu = +((cpu() - c0) / secs).toFixed(2);

  // 3. Add and process together; latency is add-to-start for a sample of jobs.
  done = 0; lat.length = 0;
  t = Date.now(); c0 = cpu();
  const added2 = await addJobs(producers, N);
  while (done < added2) { await sleep(50); if (Date.now() - t > 900000) break; }
  secs = (Date.now() - t) / 1000;
  lat.sort((a, b) => a - b);
  out.both_per_s = Math.round(done / secs); out.both_client_cpu = +((cpu() - c0) / secs).toFixed(2);
  out.wait_ms_p50 = lat[Math.floor(lat.length * 0.5)]; out.wait_ms_p99 = lat[Math.floor(lat.length * 0.99)];
  out.completed = done === added2;

  for (const w of workers) await w.close();
  for (const q of producers) await q.close();
  await counter.obliterate({ force: true }).catch(() => {});
  await counter.close(); await admin.quit();
  console.log("RESULT " + JSON.stringify(out));
  process.exit(0);
})().catch((e) => { console.error("ERR " + (e.stack || e)); process.exit(1); });
