#!/usr/bin/env bash
# The Linux checks, run inside the kividb-ci container by scripts/ci-local.sh.
# Mirrors .github/workflows/ci.yml and linux-verification.yml; keep them in
# step.
#
#   linux-gate.sh <stage>...
#
# Stages:
#   lint      fmt; clippy -D warnings, without features and with lua,tls
#   msrv      cargo check --features lua,tls on Rust 1.89 (the Dockerfile's)
#   test      cargo test, without features and with lua,tls
#   build     release binary with lua,tls
#   matrix    traffic through both I/O models: pub/sub, blocking, replication
#             determinism, AOF durability, e2e under RESP2 and RESP3
#   compat    Redis's own Tcl suite (a measurement, never fails the gate)
#
# Every stage runs even if an earlier one failed; the summary at the end says
# which did, and the exit status is 1 if any did.

set -u
STAGES=("$@")
[ ${#STAGES[@]} -gt 0 ] || STAGES=(lint test build matrix)

# Work on a copy (tests write into the working directory) of what CI would
# check out: tracked files and new files git does not ignore, with their
# uncommitted changes. Ignored files stay behind. Besides build output and
# venvs that take far longer to copy than to check, that is data a local
# server left in the tree (appendonly.aof, dump.kdb): every in-process test
# server would load it before listening, and fail to start in time.
# rust-toolchain.toml asks for the `stable` channel. The image's toolchain is
# the latest stable, under its version number, so rustup would download
# `stable` again into this throwaway container on every run (and once hung
# doing so). Use the image's.
RUSTUP_TOOLCHAIN=$(rustup default | cut -d' ' -f1)
export RUSTUP_TOOLCHAIN
echo "toolchain: $RUSTUP_TOOLCHAIN"

git config --global --add safe.directory /src
rm -rf /work && mkdir -p /work
(cd /src && git ls-files -z --cached --others --exclude-standard \
  | tar --null --ignore-failed-read -T - -cf - 2>/dev/null) | tar -xf - -C /work
cd /work

declare -A RESULT
ORDER=()
LOGDIR=/cache/logs
mkdir -p "$LOGDIR"

run() { # name, command...
  local name=$1
  shift
  ORDER+=("$name")
  echo
  echo "=================== $name"
  local start=$SECONDS
  if "$@"; then
    RESULT[$name]="PASS ($((SECONDS - start))s)"
  else
    RESULT[$name]="FAIL ($((SECONDS - start))s)"
  fi
}

venv() {
  # Cached across runs; reinstalled when requirements.txt changes.
  local want
  want=$(sha256sum tests/e2e/requirements.txt | cut -d' ' -f1)
  if [ "$(cat /cache/venv/.req 2>/dev/null)" != "$want" ]; then
    rm -rf /cache/venv
    python3 -m venv /cache/venv
    /cache/venv/bin/pip install -q -r tests/e2e/requirements.txt
    echo "$want" > /cache/venv/.req
  fi
  export PATH=/cache/venv/bin:$PATH
}

for stage in "${STAGES[@]}"; do
  case $stage in
    lint)
      run "io_uring module declared" grep -q 'pub mod server_uring' src/lib.rs
      run "fmt" cargo fmt --all -- --check
      run "clippy" cargo clippy --all-targets -- -D warnings
      run "clippy lua,tls" cargo clippy --all-targets --features lua,tls -- -D warnings
      ;;
    msrv)
      run "check on Rust 1.89" env RUSTUP_TOOLCHAIN=1.89.0 cargo check --features lua,tls
      ;;
    test)
      # --test-threads=4, as linux-verification.yml: some suites measure time.
      run "test" cargo test --no-fail-fast -- --test-threads=4
      run "test lua,tls" cargo test --no-fail-fast --features lua,tls -- --test-threads=4
      ;;
    build)
      run "release build lua,tls" cargo build --release --features lua,tls
      ;;
    matrix)
      venv
      [ -x "$CARGO_TARGET_DIR/release/kividb" ] || run "release build lua,tls" cargo build --release --features lua,tls
      for model in tokio uring; do
        run "matrix $model" bash scripts/ci/io_model_matrix.sh "$CARGO_TARGET_DIR/release/kividb" "$model" "$LOGDIR"
      done
      ;;
    compat)
      [ -x "$CARGO_TARGET_DIR/release/kividb" ] || run "release build lua,tls" cargo build --release --features lua,tls
      # As ci.yml's redis-compat job. Cached by tag, so a new tag is cloned.
      suite=/cache/redis-tests-8.10.2
      if [ ! -d "$suite" ]; then
        git clone -q --depth 1 --branch 8.10.2 https://github.com/redis/redis.git "$suite"
      fi
      (cd "$suite" && git checkout -q .)
      bash scripts/redis_compat.sh "$CARGO_TARGET_DIR/release/kividb" "$suite" "$LOGDIR/redis-compat"
      ORDER+=("redis compat")
      RESULT["redis compat"]="measured: $LOGDIR/redis-compat/summary.md (not a gate)"
      ;;
    *)
      echo "unknown stage: $stage" >&2
      exit 2
      ;;
  esac
done

echo
echo "=================== summary"
status=0
for name in "${ORDER[@]}"; do
  printf '  %-28s %s\n' "$name" "${RESULT[$name]}"
  case ${RESULT[$name]} in FAIL*) status=1 ;; esac
done
[ $status -eq 0 ] && echo "Linux gate passed." || echo "Linux gate FAILED. Logs: docker volume kividb-ci-cache, logs/"
exit $status
