# Running CI's checks locally

Everything `.github/workflows/ci.yml` and `linux-verification.yml` check can
be run on a development machine, without GitHub Actions minutes. Linux runs
in Docker, so this works the same on Windows, macOS and Linux.

## Once per clone

```sh
bash scripts/install-hooks.sh
```

This sets `core.hooksPath` to `.githooks/`. It uses plain git hooks: there is
no Node, husky or package to install.

## What runs when

| When | What | Time (warm) |
|---|---|---|
| `git commit` | `cargo fmt --check`, if Rust files are staged | seconds |
| `git push` | clippy `-D warnings` without features and with `lua,tls`, on this OS **and** on Linux in Docker; only if the pushed commits touch Rust or Cargo files | 1–3 min |
| Before merging | `bash scripts/ci-local.sh full` | 30–60 min |

Clippy runs on both operating systems because each compiles code the other
does not. The default Linux I/O path (`src/server_uring.rs`, io_uring) is not
compiled at all on Windows or macOS. A clean Windows clippy says nothing
about it, and Windows-only code is not checked on Linux.

If Docker is not running, `git push` warns and skips the Linux half rather
than blocking. `git commit --no-verify`, `git push --no-verify` or
`KIVIDB_SKIP_HOOKS=1` skips the hooks.

## `scripts/ci-local.sh`

```sh
bash scripts/ci-local.sh quick          # Linux fmt + clippy (what pre-push runs)
bash scripts/ci-local.sh full           # the whole gate
bash scripts/ci-local.sh full compat    # ... plus Redis's own Tcl suite
bash scripts/ci-local.sh test matrix    # chosen stages
```

| Stage | Mirrors | Checks |
|---|---|---|
| `lint` | ci.yml `rust`, linux-verification `lint-linux-only` | fmt; clippy without features and with `lua,tls` |
| `msrv` | the product `Dockerfile` | `cargo check --features lua,tls` on Rust 1.89 |
| `test` | ci.yml `rust`, linux-verification `test-linux` | `cargo test`, without features and with `lua,tls` |
| `build` | ci.yml `rust` | release build with `lua,tls` |
| `matrix` | linux-verification `io-model-matrix`, ci.yml `e2e` | for tokio and io_uring: pub/sub and blocking matrices, replication determinism, three AOF durability tests, and the e2e suite under RESP2 and RESP3 |
| `compat` | ci.yml `redis-compat` | Redis 8.10.2's Tcl suite, one unit at a time. A measurement, not a gate |

Every stage runs even after a failure. A summary at the end lists each
result, and the exit status is 1 if any failed.

The checks run in the `kividb-ci` image (`scripts/ci/Dockerfile`), with
io_uring allowed (`seccomp=unconfined`). The image is rebuilt weekly, so its
clippy keeps up with the latest stable, like CI's.
`KIVIDB_CI_PULL=1` rebuilds it now.

The working tree is checked as it is, including uncommitted changes. Release
builds skip LTO for speed; `KIVIDB_CI_LTO=on` matches CI exactly.

Caches are kept in Docker volumes: `kividb-cargo-registry`,
`kividb-ci-target` and `kividb-ci-cache`. Server logs from the last matrix
run are in `kividb-ci-cache` under `logs/`. `docker volume rm` clears them.

## Keeping it in step

`scripts/ci/linux-gate.sh` and `scripts/ci/io_model_matrix.sh` mirror the
workflow steps. When a step is added to either workflow, add it there too.

Not mirrored:

- The release workflow (macOS and ARM builds, packaging).
- The benchmarks.

## Other suites

- BullMQ and Sidekiq, against KiviDB and Redis: `scripts/client_compat/`
  (about 35 min).
