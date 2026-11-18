#!/usr/bin/env bash
# Release gate: a stable tag needs committed regression-suite results for the
# code it ships. Bash and python3 only; reads the repository through git, so it
# needs no checkout of the tag and runs the same locally and in CI.
#
#   scripts/release/check-regression-results.sh <tag> [--ref <git-ref>] [--repo <dir>]
#                                               [--runs <label>,<label>]
#
#   <tag>   vX.Y.Z (or X.Y.Z). A tag with a "-" (a release candidate) is not checked.
#   --ref   the commit whose tree is judged (default: <tag>). Locally, before
#           tagging, pass the release branch: --ref HEAD.
#   --repo  the git repository (default: the current directory).
#   --runs  judge only these committed results/<label>/ directories (default:
#           all of them).
#
# The suite of a minor release lives in benchmarks/X.Y.0/regressions/. If that
# directory does not exist in the tree (every release before the suite, and
# patch releases of a minor that never had one), nothing is checked.
# Otherwise the runs in results/ there must, together, cover EVERY suite in
# suites/ with a run that counts:
#
#   - the full run: FULL=1, full scale, all three engines, no --only, on the
#     baseline's machine (Standard_D4s_v5). A suite that declares
#     Suite(..., min_cpus=N) above that machine's CPUs is not run there
#     (env.json needs_more_cpus) and its metrics are "not run": allowed for
#     those suites only;
#   - for those suites, a `run-all.sh --min-cpus-only` run (FULL=1, full
#     scale, all engines) on a machine with at least N CPUs
#     (Standard_D8s_v5);
#
# and every run counted has summary.json verdict "ok" (not REGRESSED, INVALID
# or SMOKE), no failed gate and no failure in env.json, no configuration
# skipped for want of --candidate-full, no suite run on fewer CPUs than it
# declares, env.json's scale, only, engines_selected and full keys, and a
# candidate commit that exists, is an ancestor of the commit being judged, and
# left src/, Cargo.toml, Cargo.lock and rust-toolchain.toml alone since (the
# package version line aside: the release bumps it after the candidate was
# measured). It names every suite no run covers.
#
# The logic is scripts/release/regression_gate.py (tested by
# benchmarks/*/regressions/lib/test_compare.py).
#
# Exit 0: nothing to check, or the runs pass. Exit 1: refused. Exit 2: usage.
set -euo pipefail
exec python3 "$(cd "$(dirname "$0")" && pwd)/regression_gate.py" "$@"
