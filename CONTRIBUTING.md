# Contributing to KiviDB

Thank you for your interest in KiviDB. Bug reports, fixes, tests, documentation and new
features are all welcome. This page explains how to build and test the code, what a pull
request needs, and the Contributor License Agreement every contributor signs.

Please follow the [Code of Conduct](CODE_OF_CONDUCT.md) in all project spaces. Report
security vulnerabilities privately, as described in [SECURITY.md](SECURITY.md), never in a
public issue.

## Before you start

- **Bugs**: open an issue with the bug report template. Include the KiviDB version
  (`kividb --version`), the build variant (`default`, `lua`, `tls` or `full`), the OS, the
  commands that reproduce the problem, and what you expected.
- **Larger changes** (a new command, a change in behaviour, a new flag): open an issue first,
  so the approach can be agreed before you spend time on it.
- **Small fixes** (typos, documentation, an obvious bug): a pull request on its own is fine.

## Building

You need a stable Rust toolchain; [`rust-toolchain.toml`](rust-toolchain.toml) selects
`stable` with `rustfmt` and `clippy`. The code must also build with Rust 1.89, the version
the [`Dockerfile`](Dockerfile) uses.

```sh
cargo build                          # default variant, debug
cargo build --release                # default variant, release
cargo build --release --features lua,tls
```

The features are `lua` (Lua scripting) and `tls` (TLS). A change must build and pass its
tests both **without features** and **with `lua,tls`**: code behind a feature is not compiled
at all without it.

A debug build never makes the startup update check, so running one locally sends nothing to
kividb.io.

## Testing

```sh
cargo test
cargo test --features lua,tls
```

The integration tests in `tests/` start real servers on local ports and share some
process-wide state. If you see flaky failures on a machine with many cores, limit the
parallelism the way CI does:

```sh
cargo test --no-fail-fast -- --test-threads=4
cargo test --no-fail-fast --features lua,tls -- --test-threads=4
```

Add a test for every bug you fix and every behaviour you add. Put it next to the tests for
the same area in `tests/` (for example `tests/acl.rs`, `tests/replication.rs`), or as a unit
test in the module you changed.

### Linux and io_uring

On Linux the server's default I/O path uses io_uring (`src/server_uring.rs`), which is not
compiled on Windows or macOS. If you work on Windows or macOS, run the Linux checks in
Docker before you open a pull request:

```sh
bash scripts/install-hooks.sh        # once per clone: fmt on commit, clippy on push
bash scripts/ci-local.sh quick       # Linux fmt + clippy
bash scripts/ci-local.sh full        # everything CI checks (30-60 minutes)
```

`scripts/ci-local.sh` runs the checks in a Linux container with io_uring allowed. It needs
Docker and Bash (Git Bash works on Windows). [docs/LOCAL_CI.md](docs/LOCAL_CI.md) describes
each stage and the hooks.

## Code style

- Format with `cargo fmt --all`. CI runs `cargo fmt --all -- --check`.
- No Clippy warnings, with and without features:

  ```sh
  cargo clippy --all-targets -- -D warnings
  cargo clippy --all-targets --features lua,tls -- -D warnings
  ```

  CI uses the latest stable Clippy, which may be newer than yours. If CI reports a lint you
  do not see locally, update your toolchain (`rustup update stable`).
- Follow the style of the code around your change. Comments explain *why* something is done,
  in plain sentences.
- Behaviour that matches Redis should match it exactly, including error replies. Where
  KiviDB differs on purpose, say so in a comment and in the documentation.
- Keep the hot path lean: a change to command dispatch, the network loop or the store should
  say in the pull request what it costs.
- Update [COMMANDS.md](COMMANDS.md), the documentation in `docs/` and `kividb --help` text
  when you add or change a command, flag or behaviour that users see.

## Commit messages

- The first line is a short sentence, under about 72 characters, that says what is now true
  or what changed, for example `A failed TLS handshake on the cluster bus is a warning`.
  An area prefix is fine when it helps (`Lua: …`, `Changelog: …`).
- After a blank line, explain why the change is needed and anything a reviewer should know.
- One logical change per commit. Do not mix formatting changes with behaviour changes.

## Pull requests

1. Fork the repository and create a branch from `master`.
2. Make your change, with tests and documentation.
3. Run `cargo fmt`, Clippy and the tests, with and without `--features lua,tls`.
4. Open a pull request and fill in the template.
5. Sign the Contributor License Agreement (below) if you have not already.

A maintainer will review the pull request. Please keep it focused, and respond to review
comments with new commits rather than force-pushes, so the discussion stays readable.

## Contributor License Agreement

KiviDB is copyright KIVIDB PRIVATE LIMITED and is released under the
[GNU Affero General Public License, version 3](LICENSE). KIVIDB PRIVATE LIMITED keeps
ownership of KiviDB and also offers it under a commercial license, alongside the AGPLv3.

Every version released under the AGPLv3 stays available under the AGPLv3: the CLA is what
lets the commercial license exist next to it, not a way to take the open source version
away.

So that it can do that, **contributions are accepted only under the
[Contributor License Agreement](CLA.md)** (CLA). You keep the copyright in your
contribution; the CLA grants KIVIDB PRIVATE LIMITED a license to use, sublicense and
relicense it, and a patent license.

To sign, read [CLA.md](CLA.md) and, on your first pull request, add this comment:

> I have read the CLA Document and I hereby sign the CLA

If you contribute on behalf of your employer or another entity, make sure you are authorized
to do so; the entity may need to sign the Entity agreement in [CLA.md](CLA.md). A pull
request whose author has not signed the CLA cannot be merged.
