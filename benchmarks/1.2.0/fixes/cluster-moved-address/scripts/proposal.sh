#!/usr/bin/env bash
# Build and check the proposed follow-up (tree: patched + proposal_patch.py).
set -u
source "$HOME/.cargo/env"
ulimit -n 65535
O="$HOME/out/proposal"; mkdir -p "$O"
rm -rf "$HOME/proposal"; cp -r "$HOME/patched" "$HOME/proposal"
cd "$HOME/proposal"
git add -A && git commit -qm "cargo fmt" || true
DRY=1 python3 "$HOME/proposal_patch.py" . && python3 "$HOME/proposal_patch.py" . || { echo PATCH-FAILED; exit 1; }
git diff > "$O/0002-proposed-unconfirmed-address.patch"
git diff --stat
cargo fmt --all -- --check > "$O/fmt.log" 2>&1; echo "fmt: exit $?" | tee "$O/result.txt"
[ -d "$HOME/tgt-proposal" ] || cp -r "$HOME/tgt-patched" "$HOME/tgt-proposal"
export CARGO_TARGET_DIR="$HOME/tgt-proposal"
R="nice -n 10 taskset -c 3-7"
$R cargo build --release > "$O/build.log" 2>&1 < /dev/null; echo "build: exit $?" | tee -a "$O/result.txt"
$R cargo test --release --lib cluster > "$O/lib-cluster.log" 2>&1 < /dev/null; echo "lib cluster: exit $?" | tee -a "$O/result.txt"
grep "test result" "$O/lib-cluster.log" | tee -a "$O/result.txt"
$R cargo clippy --all-targets -- -D warnings > "$O/clippy.log" 2>&1 < /dev/null; echo "clippy: exit $?" | tee -a "$O/result.txt"
$R cargo test --release --test cluster > "$O/it-cluster.log" 2>&1 < /dev/null; echo "tests/cluster.rs: exit $?" | tee -a "$O/result.txt"
grep "test result" "$O/it-cluster.log" | tee -a "$O/result.txt"
echo PROPOSAL-DONE | tee -a "$O/result.txt"
