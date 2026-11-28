// ioredis runner (run.py's contract). RESP3 with replyMapping "resp3", so maps
// and doubles come back as RESP3 sends them.
import Redis from "ioredis";
import { createRequire } from "node:module";
import { host, port, protocol, runAll } from "./common.mjs";

const { version } = createRequire(import.meta.url)("ioredis/package.json");
const r = new Redis({
  host, port, protocol,
  ...(protocol === 3 ? { replyMapping: "resp3" } : {}),
  maxRetriesPerRequest: 0,
  lazyConnect: true,
});
await r.connect();
await runAll("ioredis", version, ([name, ...args]) => r.call(name, ...args));
r.disconnect();
