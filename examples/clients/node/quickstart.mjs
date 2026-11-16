// KiviDB with ioredis: strings, expiry, hashes, a pipeline, a transaction
// and Pub/Sub.
//
//   npm install
//   node quickstart.mjs          # KiviDB on localhost:6380
//   KIVIDB_URL=redis://:secret@host:6380/0 node quickstart.mjs

import assert from "node:assert/strict";
import Redis from "ioredis";

const url = process.env.KIVIDB_URL ?? "redis://localhost:6380/0";
const redis = new Redis(url);

assert.equal(await redis.ping(), "PONG");

// Start clean: these are the keys this example uses.
await redis.del("greeting", "session:42", "user:1", "balance");

// Strings, with and without an expiry.
await redis.set("greeting", "hello");
assert.equal(await redis.get("greeting"), "hello");
await redis.set("session:42", "alice", "EX", 60);
const ttl = await redis.ttl("session:42");
assert.ok(ttl > 0 && ttl <= 60);

// Hashes.
await redis.hset("user:1", { name: "Alice", plan: "pro" });
assert.deepEqual(await redis.hgetall("user:1"), { name: "Alice", plan: "pro" });

// A pipeline: many commands, one round trip.
const pipeline = redis.pipeline();
for (let i = 0; i < 100; i++) pipeline.set(`item:${i}`, i);
await pipeline.exec();
assert.deepEqual(await redis.mget("item:0", "item:99"), ["0", "99"]);

// A transaction.
await redis.set("balance", 100);
const [[, result]] = await redis.multi().decrby("balance", 30).exec();
assert.equal(result, 70);

// Pub/Sub: a subscriber needs a connection of its own.
const subscriber = new Redis(url);
const received = new Promise((resolve) =>
  subscriber.on("message", (channel, message) => resolve([channel, message])),
);
await subscriber.subscribe("news");
await redis.publish("news", "KiviDB is up");
assert.deepEqual(await received, ["news", "KiviDB is up"]);

subscriber.disconnect();
redis.disconnect();
console.log("node quickstart: ok");
