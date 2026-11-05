#!/usr/bin/env bash
# Regenerate the proposal diff with the rustfmt-clean line, and check fmt (no build).
set -u
source "$HOME/.cargo/env"
O="$HOME/out/proposal"
rm -rf "$HOME/propfmt"; cp -r "$HOME/patched" "$HOME/propfmt"
cd "$HOME/propfmt"
git add -A && git commit -qm "cargo fmt"
python3 "$HOME/proposal_patch.py" . || exit 1
cargo fmt --all -- --check > "$O/fmt-final.log" 2>&1; echo "fmt (final patch): exit $?" | tee -a "$O/result.txt"
git diff > "$O/0002-proposed-unconfirmed-address.patch"
# Only formatting differs from the tree that was built and tested:
diff <(cd "$HOME/proposal" && git diff | tr -d ' \r\n') <(git diff | tr -d ' \r\n') > /dev/null && echo "same tokens as tested tree" | tee -a "$O/result.txt"
git diff --stat
