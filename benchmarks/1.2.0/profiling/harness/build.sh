#!/usr/bin/env bash
# build.sh NAME REV: release build (the shipped profile: lto fat, one codegen
# unit, default features) of REV from github.com/kividbio/kividb into
# ~/bin/kividb-NAME.
#   DEBUG=1  adds debug line tables (symbols and lines for perf)
#   FP=1     builds with frame pointers (-C force-frame-pointers=yes), so perf
#            can walk the user-space stack (its DWARF unwinder gives empty call
#            chains for most samples of the release binary on these VMs)
set -euo pipefail
source "$HOME/.cargo/env"
NAME=$1 REV=$2
cd ~/kividb
git fetch -q --tags origin
git -c advice.detachedHead=false checkout -q --detach "$REV"
export CARGO_TARGET_DIR=~/target-$NAME
[ "${DEBUG:-}" = 1 ] && export CARGO_PROFILE_RELEASE_DEBUG=line-tables-only
[ "${FP:-}" = 1 ] && export RUSTFLAGS="-C force-frame-pointers=yes"
cargo build --release 2>&1 | tail -2
cp "$CARGO_TARGET_DIR/release/kividb" ~/bin/kividb-"$NAME"
echo "BUILT $NAME $(git rev-parse HEAD) $(~/bin/kividb-"$NAME" --version) RUSTFLAGS=${RUSTFLAGS:-}"
