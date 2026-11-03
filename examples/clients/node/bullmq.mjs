// A BullMQ queue and worker on KiviDB.
//
// BullMQ runs Lua scripts, so it needs a KiviDB build with Lua: the `lua` or
// `full` variant (Docker tag `v1.1.2-lua`, release file `kividb-lua-…`). The
// default build answers its scripts with an error.
//
//   npm install
//   node bullmq.mjs

import assert from "node:assert/strict";
import { Queue, Worker, QueueEvents } from "bullmq";

const url = new URL(process.env.KIVIDB_URL ?? "redis://localhost:6380/0");
const connection = {
  host: url.hostname,
  port: Number(url.port || 6380),
  password: url.password || undefined,
  // BullMQ's workers block on the server; this is what BullMQ asks for.
  maxRetriesPerRequest: null,
};

const queue = new Queue("emails", { connection });
await queue.obliterate({ force: true });

const worker = new Worker(
  "emails",
  async (job) => `sent to ${job.data.to}`,
  { connection },
);
const events = new QueueEvents("emails", { connection });
await events.waitUntilReady();

const job = await queue.add("welcome", { to: "alice@example.com" });
const result = await job.waitUntilFinished(events, 10_000);
assert.equal(result, "sent to alice@example.com");

await worker.close();
await events.close();
await queue.close();
console.log("node bullmq: ok");
