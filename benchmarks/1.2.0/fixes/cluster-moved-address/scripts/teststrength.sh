#!/usr/bin/env bash
# Do the two new unit tests catch the bug? Run them against the patched tree with the fix's
# condition removed from upsert_node (tests kept).
set -u
source "$HOME/.cargo/env"
O="$HOME/out"
rm -rf "$HOME/testsonly"; cp -r "$HOME/patched" "$HOME/testsonly"
cd "$HOME/testsonly"
f=src/cluster/bus/listener.rs
sed -i 's/if direct_contact || is_new || entry.ip.is_empty() || moved {/let _ = moved;\r\n        if direct_contact || is_new || entry.ip.is_empty() {/' $f
git diff --stat
grep -n "let _ = moved" $f
CARGO_TARGET_DIR="$HOME/tgt-patched" nice -n 15 taskset -c 3-7 cargo test --release --lib -- \
  a_node_we_cannot_reach_takes_the_address_a_healthy_relay_gives \
  a_relayed_address_does_not_replace_a_reachable_one_or_come_from_a_suspect \
  > "$O/unit-tests-without-fix.log" 2>&1 < /dev/null
echo "exit $?" >> "$O/unit-tests-without-fix.log"
grep -E "^test |test result|exit" "$O/unit-tests-without-fix.log"
echo STRENGTH-DONE
