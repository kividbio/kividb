# Releasing: notes for maintainers

The release workflow (`.github/workflows/release.yml`) runs when a `v*` tag is
pushed. This page covers the parts of a release that need an owner to set
something up or to act by hand.

- [Before tagging: the regression suite](#before-tagging-the-regression-suite)
- [crates.io](#cratesio)
- [Withdrawing a release](#withdrawing-a-release)

## Before tagging: the regression suite

Every performance, latency or memory improvement of a minor release has a
suite in `benchmarks/X.Y.0/regressions/` (for 1.3.0: scan, growth, give-back,
long commands), with the values it recorded and a tolerance. A stable tag is
refused unless a run of the whole suite on the final release candidate is
committed. The release workflow checks this in its first job
(`verify-version`, which every build waits for), so a missing result fails in
seconds and nothing is built or published.

1. Freeze the scope and cut the final release candidate. Do not measure a
   build that may still change.
2. Run the suite on it: **Actions, "Regression suite", Run workflow** (from
   `master`; the Azure sign-in trusts `master` runs only). `ref` is the
   release candidate's tag or commit (`v1.3.0-rc4`), `label` names the run
   (`v1.3.0-rc4-d4s_v5`), and `only` stays empty. Tick `full` for the long
   configurations: do it for a release. The workflow makes one
   `Standard_D4s_v5` (the size `baseline.json` was recorded on), builds the
   candidate on it, runs `run-all.sh`, uploads `results/<label>/` as an
   artifact and deletes the machine whatever happens. It fails when
   `run-all.sh` exits non-zero. It takes a release build on 4 vCPUs (fat LTO, several minutes) plus 13 to
   21 minutes of measuring.
3. Read `summary.md`. The verdict must be `ok`:
   - `REGRESSED`: a gate failed. Either fix the regression (and cut a new
     candidate), or, for an intended change, explain it in the release PR and
     re-record the baseline (below).
   - `INVALID`: the run cannot be judged, so it proves nothing. A suite or
     server failed, a recorded metric is missing, or the VM size or a
     reference binary (v1.2.2, Redis) differs from the recorded one. Fix the
     cause and run again; never commit an `INVALID` run as the release's
     result.
   - `SMOKE`: `SCALE` was not 1; only a harness check.
4. Download the artifact and commit `results/<label>/` (`env.json`,
   `summary.json`, `summary.md`, `raw.tar.gz`) on the release branch, with the
   version bump. Tag that commit. The gate needs the run's `candidate_commit`
   to be an ancestor of the tag with `src/`, `Cargo.toml`, `Cargo.lock` and
   `rust-toolchain.toml` unchanged since, apart from the package version line.
   Any code change after the run means running it again.
5. Check it before pushing the tag:

   ```bash
   scripts/release/check-regression-results.sh vX.Y.Z --ref HEAD
   ```

   It needs bash and python3 and reads the repository through git. It prints
   why each committed run does not count, or `OK`.

The gate counts a run only when its verdict is `ok`, with no failed gate, at
full scale (no `SCALE`, no `only`), with all suites and all three engines, and
with a candidate commit that exists. A minor release with no
`benchmarks/X.Y.0/regressions/` directory (every release before 1.3.0, and
their patch releases) is not checked; release candidates are never checked. A
patch release of a minor that has a suite needs a fresh run, because it changes
`src/`.

### Re-recording the baselines

`baseline.json` holds the recorded value and gate of every metric. Re-record
only for a change you can explain (a deliberate trade, a new suite, a new
tolerance), never to make a red run green:

```bash
# on a Standard_D4s_v5 with the candidate built, from a clean tree
FULL=1 benchmarks/1.3.0/regressions/run-all.sh --candidate target/release/kividb \
    --candidate-commit "$(git rev-parse --short HEAD)" --label <label> --record
```

`--record` refuses an `INVALID` run, a `SCALE` run, a run without all three
engines and a null value. Review the diff of `baseline.json`, then commit it
with `results/<label>/`. The suite's README ("Baselines") has the details; the
Actions workflow never records.

If the Azure workflow's Redis differs from the one the pin names (the workflow
builds 8.10.2 from its tag on the machine, where the pin is the build on our
VM image), it accepts the built binary's SHA-256 for that run; the gates
that compare with Redis compare within the same session, so the pin only
exists to keep the reference fixed for the recorded values.

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
