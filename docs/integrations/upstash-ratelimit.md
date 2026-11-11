# Upstash Ratelimit (`@upstash/ratelimit`)

`@upstash/ratelimit` doesn't speak the Redis protocol. It talks to Redis only through `@upstash/redis`,
an HTTP client for Upstash's REST API. **KiviDB has no REST or HTTP API**; it speaks RESP only. So the
library can't point at KiviDB directly.

It works through **Serverless Redis HTTP** (SRH, `hiett/serverless-redis-http`), the open-source proxy that
translates Upstash's REST API to RESP. Upstash's own docs suggest SRH for local development. Run SRH in front
of KiviDB's `-lua` image: the library's algorithms are Lua scripts (`EVALSHA`).

**Checked**, with ratelimit-js's own test suite (`bun test src`), through SRH 0.0.10, against Redis 8.10.2 as
the reference:

| ratelimit-js | Redis 8.10.2 | KiviDB 1.2.0-lua | KiviDB 1.2.0 (no Lua) |
|---|---|---|---|
| **2.0.8** | 171 pass, 1 fail (a timing-sensitive multi-region test) | **172 pass, 0 fail** | 18 pass: no Lua |
| **2.2.0** (latest) | 51 pass, 148 fail | 70 pass, 135 fail | not run |

**ratelimit-js 2.1.0 and later don't work with any non-Upstash Redis yet, Redis 8 included.** Every script
starts with `#!lua flags=allow-key-locking`, a flag only Upstash's servers know. Redis and KiviDB both answer
`Unexpected flag in script shebang: allow-key-locking`. This is the library's issue #162; an opt-out
(`keyLocking: false`) is proposed in its PR #168. Until that ships, use 2.0.x with SRH.

The commands and the logs are in
[`benchmarks/upstash-ratelimit-compat/`](../../benchmarks/upstash-ratelimit-compat/).

## Run it

```bash
docker run -d --name kividb -p 6380:6380 kividb/kividb:v1.2.0-lua
docker run -d --name srh -p 8079:80 \
  -e SRH_MODE=env -e SRH_TOKEN=change-me \
  -e SRH_CONNECTION_STRING=redis://host.docker.internal:6380 \
  hiett/serverless-redis-http:0.0.10
npm install @upstash/ratelimit@2.0.8 @upstash/redis
```

```ts
import { Ratelimit } from "@upstash/ratelimit";
import { Redis } from "@upstash/redis";

const ratelimit = new Ratelimit({
  redis: new Redis({ url: "http://localhost:8079", token: "change-me" }),
  limiter: Ratelimit.slidingWindow(10, "10 s"),
});
const { success } = await ratelimit.limit("user-1");
```

On Linux, replace `host.docker.internal` with the host's address, or put both containers on one Docker
network and use `redis://kividb:6380`.

## What to know

- **Two hops.** Each call goes over HTTP to SRH and then over RESP to KiviDB. For a Node service that can
  open TCP connections, a RESP-native rate limiter on KiviDB avoids the proxy.
- **Not tested:** SRH under load, multi-region setups with separate servers (the suite's three "regions"
  all pointed at one SRH), and Upstash's analytics.
