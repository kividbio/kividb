# Releasing: notes for maintainers

The release workflow (`.github/workflows/release.yml`) runs when a `v*` tag is
pushed. This page covers the parts of a release that need an owner to set
something up or to act by hand.

- [crates.io](#cratesio)
- [Withdrawing a release](#withdrawing-a-release)

## crates.io

After a stable release's GitHub Release is published, the `crates-io` job
publishes the crate [`kividb`](https://crates.io/crates/kividb):

1. It is skipped, with a notice, while the `CARGO_REGISTRY_TOKEN` secret is
   not set.
2. It is skipped when crates.io already has the version (a re-run: a version
   can be published only once). Any answer from crates.io other than 200 or
   404 fails the job.
3. `cargo package --locked`, with no token in its environment, packs the files
   `Cargo.toml`'s `include` lists (everything under `src/`, the lockfile, the
   README, CHANGELOG, LICENSE and NOTICE, and the three fixtures the
   HyperLogLog unit tests read) and builds that package as a check.
4. `cargo publish --locked --no-verify` uploads it. It is the only step that
   has the token.

The library in the crate is built from the same sources so the integration
tests can use them; it is not a public API. The `kividb` binary is not built
from it: `src/main.rs` declares its own modules.

### One-time setup

1. **Reserve the name soon.** Until `kividb` is published, anyone can register
   it on crates.io. The first stable release after this job was added
   publishes it; to hold the name sooner, publish 1.2.0 by hand from a
   checkout of `v1.2.0` with the `Cargo.toml` changes that added this job
   applied (the `v1.2.0` tree has no `include`, so it would pack the
   benchmarks and go over crates.io's 10 MB limit):

   ```bash
   git checkout v1.2.0
   git diff <merge-commit>^1 <merge-commit> -- Cargo.toml | git apply
   cargo package --locked --allow-dirty
   cargo publish --locked --allow-dirty
   ```

   `<merge-commit>` is the commit on `master` that merged pull request #457.
   The tarballs `cargo binstall` downloads for 1.2.0 are on the v1.2.0
   GitHub Release already.
2. Create a crates.io API token with the scopes `publish-new` and
   `publish-update`, limited to the crate `kividb`, and add it as the
   `CARGO_REGISTRY_TOKEN` repository secret.

### The README section, after the first publish

The README gets its "With Cargo" install section once `kividb` is on
crates.io. What it should say:

```bash
cargo install --locked kividb                      # default variant
cargo install --locked kividb --features lua,tls   # full (or lua, or tls)
cargo binstall kividb                              # the release's binary
```

- KiviDB runs on Linux and macOS only.
- `cargo install` builds from source. `--locked` builds with the dependency
  versions the release was built and tested with. The release profile uses
  fat LTO, so the build takes several minutes. Since it compiles on the
  machine itself, it also works with a glibc older than 2.34.
- `cargo binstall` downloads the release's own binary of the default variant,
  for Linux (x86_64, aarch64) and macOS (x86_64, Apple Silicon), and compiles
  only on other targets. The Linux binaries need glibc 2.34 or newer (Debian
  12, Ubuntu 22.04, RHEL 9, Amazon Linux 2023, or later), and the aarch64 one
  an ARMv8.1 CPU or newer, for its LSE atomics: Graviton2 and later, Ampere,
  but not a Raspberry Pi 4. On an older system, use `cargo install`.

## Withdrawing a release

A faulty release is withdrawn (yanked), never deleted: its tag, its images and
its downloads stay.

1. Add its tag to `.github/withdrawn-releases`, with a comment saying why and
   which release replaces it. A stable release's notes then count from the
   release before it.
2. Mark it `[YANKED]` in `CHANGELOG.md`.
3. Yank it on crates.io, with a token that has the `yank` scope:

   ```bash
   cargo yank --version X.Y.Z kividb
   ```

   A yanked version stays downloadable for a `Cargo.lock` that already names
   it, but `cargo install kividb` and `cargo binstall kividb` no longer pick
   it. `cargo yank --version X.Y.Z --undo kividb` reverses it.
