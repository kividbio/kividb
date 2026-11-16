#!/usr/bin/env bash
# Runs on the throwaway Azure VM of .github/workflows/regression-suite.yml
# (Ubuntu 24.04, Standard_D4s_v5, nothing installed): builds the candidate and
# runs the regression suite on it. Not for a laptop.
#
# Expects in $HOME: cand.tar (the candidate's source tree) and harness.tar (the
# suite's directory, e.g. benchmarks/1.3.0/regressions/, with its path).
# Settings (environment): SUITE_DIR (path of the suite in harness.tar), LABEL,
# CAND_COMMIT, ONLY (comma list, optional), FULL (1 for the long configurations).
# Writes the suite's exit status to $HOME/run.exit; the results are in
# $HOME/harness/$SUITE_DIR/results/$LABEL.
set -uo pipefail

cd "$HOME"
rc=1
finish() { echo "$rc" >"$HOME/run.exit"; }
trap finish EXIT
: "${SUITE_DIR:?}" "${LABEL:?}" "${CAND_COMMIT:?}"

export DEBIAN_FRONTEND=noninteractive
echo "=== packages"
# First boot: cloud-init and unattended upgrades may hold the apt lock.
cloud-init status --wait >/dev/null 2>&1 || true
sudo apt-get -o DPkg::Lock::Timeout=600 update -q
sudo apt-get -o DPkg::Lock::Timeout=600 install -y -q build-essential git curl pkg-config libssl-dev python3-venv xz-utils || exit 1

echo "=== unpack"
rm -rf cand harness
mkdir cand harness
tar -xf cand.tar -C cand
tar -xf harness.tar -C harness

echo "=== rust"
if ! command -v cargo >/dev/null 2>&1 && [ ! -x "$HOME/.cargo/bin/cargo" ]; then
  curl -fsSL --retry 3 https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain none || exit 1
fi
export PATH="$HOME/.cargo/bin:$PATH"

echo "=== build candidate ($CAND_COMMIT)"
# The toolchain the candidate pins (rust-toolchain.toml), as its own CI uses.
(cd cand && cargo build --release --locked) || exit 1
CAND_BIN=$HOME/cand/target/release/kividb
[ -x "$CAND_BIN" ] || { echo "no $CAND_BIN"; exit 1; }
"$CAND_BIN" --version | head -1

HERE=$HOME/harness/$SUITE_DIR
[ -x "$HERE/run-all.sh" ] || chmod +x "$HERE/run-all.sh"

echo "=== redis 8.10.2 (built from its tag; the suite's pin names the build on our VM image)"
export REGRESS_CACHE=$HOME/.cache/kividb-regress
# shellcheck source=/dev/null
source "$HERE/lib/engines.sh"
REDIS_BIN=$(engines_resolve_redis "$REGRESS_CACHE/redis") || exit 1
export REDIS_SERVER=$REDIS_BIN
REGRESS_REDIS_SHA256=$(sha256sum "$REDIS_BIN" | cut -d' ' -f1)
export REGRESS_REDIS_SHA256
echo "redis-server: $REDIS_BIN sha256 $REGRESS_REDIS_SHA256"

echo "=== suite"
args=(--candidate "$CAND_BIN" --candidate-commit "$CAND_COMMIT" --label "$LABEL")
[ -n "${ONLY:-}" ] && args+=(--only "$ONLY")
export FULL=${FULL:-0}
"$HERE/run-all.sh" "${args[@]}"
rc=$?
echo "=== run-all.sh exit $rc"

# What the machine was made of, next to the numbers: baselines are tied to it.
# (After run-all.sh, so only env.json is touched, not the verdict.)
ENV_JSON=$HERE/results/$LABEL/env.json
if [ -f "$ENV_JSON" ]; then
  IMDS=$(curl -fs -m 5 -H Metadata:true \
    "http://169.254.169.254/metadata/instance/compute/storageProfile/imageReference?api-version=2021-02-01&format=json" || echo '{}')
  python3 - "$ENV_JSON" "$IMDS" "$(uname -r)" "$(ldd --version | head -1)" "$(. /etc/os-release && echo "$PRETTY_NAME")" <<'PY' || true
import json, sys
path, imds, kernel, glibc, osname = sys.argv[1:6]
try:
    ref = json.loads(imds)
except ValueError:
    ref = {}
env = json.load(open(path))
env["ci_image"] = {"image_reference": ref, "os": osname, "kernel": kernel, "glibc": glibc}
json.dump(env, open(path, "w"), indent=1)
PY
fi
exit "$rc"
