# Benchmarks and compatibility harnesses

Performance results live in [LATEST.md](LATEST.md) and the per-release folders. The `*-compat/` folders
hold one harness per framework: each runs that framework's own Redis tests against KiviDB and against
Redis, and its page in `docs/integrations/` says what passes.

## Compatibility harnesses in CI

[`.github/workflows/compat.yml`](../.github/workflows/compat.yml) runs the harnesses that follow the
contract below against KiviDB and Redis 8.10, one after another on one runner: on each release, and by
hand (with a pre-release image before a release). It fails when a check passes on Redis and fails on
KiviDB, when KiviDB stops passing a check its harness lists in `kividb-passes.txt`, or when a harness
could not run (its setup failed, a server did not start, or a run left no results), and the job
summary says which, with a table per harness of the checks that differ. Nothing in it needs a secret
or a model API key.

### The contract

A harness takes part by adding `ci.sh` to its folder:

```
ci.sh setup DIR              fetch the framework at its pinned version and install what it needs into DIR
ci.sh run DIR PORT OUT.xml   run its tests against a server already listening on 127.0.0.1:PORT,
                             writing JUnit XML to OUT.xml
```

- **`setup`** runs once, with no server. Pin everything: the framework's tag or commit, its dependencies
  (the folder's `requirements.txt`, a lock file), the language version. Exit non-zero if it fails.
- **`run`** runs once per server: [`ci/run-harness.sh`](ci/run-harness.sh) starts a fresh Redis, runs
  it, then a fresh KiviDB, and runs it again. Exit 0 when the tests ran, whatever their results, and
  non-zero only when they could not run (no `OUT.xml` written).
- **Results** are JUnit XML, which pytest (`--junitxml`), Maven's Surefire/Failsafe and most runners
  write. Each `<testcase>` is one check: a `<failure>` or `<error>` in it is a fail, `<skipped>` a skip,
  anything else a pass. [`ci/compare.py`](ci/compare.py) matches the two files by `classname::name`.
  A harness that runs its own checks rather than a test suite writes them with
  [`ci/compat_junit.py`](ci/compat_junit.py).
- A harness whose tests expect a fixed port (6379, say) checks `PORT` and exits 2 on another.
- **`kividb-passes.txt`**, optional, lists checks KiviDB passes, one `classname::name` a line (`#` for
  comments), for a harness whose checks Redis largely fails, so that comparing with Redis alone would
  miss most of them: the job fails if KiviDB stops passing one. Add a check to it once KiviDB passes it.
- **`kividb-variant`**, optional, one word, `lua`: the harness needs scripting, which the default image
  does not have, so its KiviDB side runs the `lua` variant of the image (`<image>-lua`, or
  `KIVIDB_LUA_IMAGE`). A harness that does not need Lua tests the default image, as users run it.
- No secrets, no model API keys, no network access beyond fetching the framework, its packages and
  its test fixtures.

To add a harness to the workflow, write its `ci.sh`, try it locally with
`KIVIDB_IMAGE=kividb/kividb:<tag> REDIS_IMAGE=redis:8.10.3 benchmarks/ci/run-harness.sh <name>`, and
add a step for it to `compat.yml` with its own `timeout-minutes`.

### Harnesses in CI so far

| Harness | Framework version | Suite time per server |
| --- | --- | --- |
| `semantic-kernel` | semantic-kernel 1.47.0 | a few seconds, after a `uv sync` of several minutes |
| `haystack-valkey` | valkey-haystack 1.0.0 | under 30 s |
| `gptcache` | GPTCache 0.1.44 | 1 to 5 min (the first run downloads its ONNX model) |
| `langchain` | langchain-redis 0.2.6, through this folder's own harness (33 checks) | under 10 s in all, the install included |
| `langgraph` | langgraph-checkpoint-redis 0.6.0 with langgraph 1.2.19, through this folder's own harness (23 checks) | under 10 s in all, the install included |
| `llamaindex` | llama-index-core 0.14.25 with its Redis stores, through this folder's own harness (54 checks) | under 10 s in all, the install included |
| `mem0` | mem0ai 2.2.1, through this folder's own harness (24 checks) | under 10 s in all, the install included |
| `stoplight` | Stoplight 5.8.3 (the version Mastodon uses), its whole rspec suite (765 examples), in a pinned Ruby image; `lua` variant | about 1 min per server after the first run pulls the image and installs the gems |
| `sidekiq-unique-jobs` | sidekiq-unique-jobs 8.1.0, its whole rspec suite (1,069 examples; 1 fails on Redis and on KiviDB); `lua` variant | under 1 min per server |
| `sidekiq-scheduler` | sidekiq-scheduler 6.0.2 with Sidekiq 8, its whole rspec suite (274 examples; the same 5 environment failures on Redis and on KiviDB); `lua` variant | about 30 s per server |
| `sidekiq-mastodon` | Sidekiq 8.1.7 with Mastodon v4.7.6's `config/sidekiq.yml` queues, 10,000 jobs at 25, 100, 500 and 1,000 threads, every job run once, a scheduled job and a retry (26 checks); `lua` variant | under 1 min per server |
| `sentry-spans` | getsentry/sentry's span buffer script `add-buffer.lua` (pinned commit), run as Sentry runs it, with real keys (13 checks); `lua` variant | seconds |
| `client-tracking` | `CLIENT TRACKING ON REDIRECT <id> NOLOOP` with invalidations on `__redis__:invalidate`, the way Frappe uses it (17 checks); default image | seconds |
| `open-webui-valkey` | Open WebUI's `ValkeyClient` (valkey.py at a pinned commit, valkey-glide-sync 2.5.2) as shipped, 17 checks; default image. Redis fails every check (it is not Valkey), so the job holds KiviDB to `kividb-passes.txt`, and `control.sh` runs the same checks against valkey/valkey-bundle 9.2.0 | seconds |
| `ratelimit-scripts` | the rate limiters' scripts called as their gateways call them: Spring Cloud Gateway v5.1.0 `request_rate_limiter.lua`, Traefik v3.7.17's token bucket, Kong 3.9.4's `incr` + `expire` (18 checks); `lua` variant | seconds |
