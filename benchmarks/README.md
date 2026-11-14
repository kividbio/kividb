# Benchmarks and compatibility harnesses

Performance results live in [LATEST.md](LATEST.md) and the per-release folders. The `*-compat/` folders
hold one harness per framework: each runs that framework's own Redis tests against KiviDB and against
Redis, and its page in `docs/integrations/` says what passes.

## Compatibility harnesses in CI

[`.github/workflows/compat.yml`](../.github/workflows/compat.yml) runs the harnesses that follow the
contract below against KiviDB and Redis 8.10, one after another on one runner: weekly, on each release,
and by hand. It fails only when a check passes on Redis and fails on KiviDB, and the job summary has a
table per harness of the checks that differ. Nothing in it needs a secret or a model API key.

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
- A harness whose tests expect a fixed port (6379, say) checks `PORT` and exits 2 on another.
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
| `redisvl` | redisvl 0.32.0, through this folder's own harness (67 checks) | seconds, after an install of under a minute (not yet timed in CI) |
