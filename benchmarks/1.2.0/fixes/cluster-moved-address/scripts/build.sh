#!/usr/bin/env bash
# Stage 1: fmt check on patched (formatting it if needed), then release binaries for both trees.
set -u
source "$HOME/.cargo/env"
ulimit -n 65535
O="$HOME/out"; mkdir -p "$O"
cd "$HOME/patched"
if cargo fmt --all -- --check > "$O/fmt-check.log" 2>&1; then
  echo "FMT: clean" | tee "$O/fmt-result.txt"
else
  echo "FMT: check failed; running cargo fmt" | tee "$O/fmt-result.txt"
  cargo fmt --all
  git diff > "$O/cargo-fmt.patch"
  git diff --stat >> "$O/fmt-result.txt"
  cargo fmt --all -- --check >> "$O/fmt-result.txt" 2>&1 && echo "FMT: clean after cargo fmt" >> "$O/fmt-result.txt"
fi
for t in base patched; do
  ( cd "$HOME/$t" && CARGO_TARGET_DIR="$HOME/tgt-$t" cargo build --release > "$O/build-$t.log" 2>&1 ) \
    && echo "BUILD $t ok" || echo "BUILD $t FAILED"
done
ls -la "$HOME"/tgt-*/release/kividb
echo BUILD-DONE
