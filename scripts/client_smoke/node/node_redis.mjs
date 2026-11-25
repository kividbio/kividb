// node-redis runner (run.py's contract).
import { createClient } from "redis";
import { createRequire } from "node:module";
import { host, port, protocol, runAll } from "./common.mjs";

const { version } = createRequire(import.meta.url)("redis/package.json");
const client = createClient({ socket: { host, port, reconnectStrategy: false }, RESP: protocol });
client.on("error", () => {});
await client.connect();
await runAll("node-redis", version, (cmd) => client.sendCommand(cmd));
client.destroy();
