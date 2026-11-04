---
name: Redis compatibility gap
about: A command, option, reply or client library that behaves differently from Redis
title: "Redis compat: "
labels: redis-compat
assignees: ""
---

**What differs**

The command (with its arguments), or the client library and the call you made.

```
redis-cli -p 6380 ...
```

**What Redis or Valkey returns**

The reply from Redis or Valkey (which version?), and from KiviDB.

**Where it matters**

The library, framework or application that depends on it (for example a job queue, a
cache library, an ORM), if any. It helps us decide what to fix first.

**Environment**

- KiviDB version (`INFO server`, `kividb_version`) and variant (default, lua, tls, full):
- How it runs (Docker image tag, release binary, built from source):
