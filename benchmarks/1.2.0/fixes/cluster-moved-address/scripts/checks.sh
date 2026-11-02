#!/usr/bin/env bash
# Stage 2 (runs niced on CPUs 3-7 beside the e2e): clippy, lib cluster tests, tests/cluster.rs; both trees.
set -u
source "$HOME/.cargo/env"
ulimit -n 65535
O="$HOME/out"; mkdir -p "$O"
R="nice -n 15 taskset -c 3-7"
cd "$HOME/patched"
export CARGO_BUILD_JOBS=5
$R cargo clippy --all-targets -- -D warnings > "$O/clippy-patched.log" 2>&1 < /dev/null; echo "clippy default: exit $?" | tee "$O/clippy-result.txt"
$R cargo clippy --all-targets --features lua,tls -- -D warnings > "$O/clippy-patched-lua-tls.log" 2>&1 < /dev/null; echo "clippy lua,tls: exit $?" | tee -a "$O/clippy-result.txt"
for t in patched base; do
  cd "$HOME/$t"
  export CARGO_TARGET_DIR="$HOME/tgt-$t"
  $R cargo test --release --lib cluster > "$O/lib-cluster-$t.log" 2>&1 < /dev/null; echo "lib cluster $t: exit $?" | tee -a "$O/tests-result.txt"
  $R cargo test --release --test cluster > "$O/it-cluster-$t.log" 2>&1 < /dev/null; echo "tests/cluster.rs $t run1: exit $?" | tee -a "$O/tests-result.txt"
done
unset CARGO_TARGET_DIR
echo CHECKS-DONE | tee -a "$O/tests-result.txt"
