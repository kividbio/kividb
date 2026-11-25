// ioredis through Sentinel: writes to the master Sentinel names, and keeps
// writing to the new one after a failover. The contract run.sh expects of
// each client is in run.sh.
const Redis = require("ioredis");

const sentinels = process.env.SENTINELS.split(",").map((a) => {
  const [host, port] = a.split(":");
  return { host, port: Number(port) };
});
const key = process.env.KEY;
const deadline = Date.now() + 1000 * Number(process.env.DEADLINE || 60);
const r = new Redis({
  sentinels,
  name: process.env.MASTER,
  password: process.env.PASSWORD,
  commandTimeout: 1000,
  maxRetriesPerRequest: 0,
  enableOfflineQueue: true,
});
r.on("error", () => {}); // counted below, as failed writes

const sleep = (ms) => new Promise((done) => setTimeout(done, ms));
const tcpPort = (info) => Number(/^tcp_port:(\d+)/m.exec(info)[1]);

(async () => {
  let first = null, errors = 0, after = 0;
  while (Date.now() < deadline) {
    let n, port;
    try {
      n = await r.incr(key);
      port = tcpPort(await r.info("server"));
    } catch (e) {
      errors++;
      await sleep(100);
      continue;
    }
    if (first === null) {
      first = port;
      console.log(`ready ${port}`);
    } else if (port !== first && ++after >= 10) {
      if (Number(await r.get(key)) !== n) {
        console.error("the new master does not read back the last write");
        process.exit(1);
      }
      console.log(`followed ${first} -> ${port} after ${errors} failed writes`);
      process.exit(0);
    }
    await sleep(100);
  }
  console.error(`still on ${first} at the deadline, ${errors} failed writes`);
  process.exit(1);
})();
