#!/usr/bin/env bash
# Put a KiviDB build on the server as ~/bin/kividb-<name>.
#
#   fetch-kividb.sh <git ref> <name> [release|build]
#
# release: the default-variant arm64 binary from the GitHub release of <ref>
# build:   clone <ref> and build it as release.yml builds arm64
#          (cargo build --release, RUSTFLAGS="-C target-feature=+lse")
# Default: release if the release has the asset, else build.
# Writes ~/bin/kividb-<name>.source with the ref, commit and how it was made.
set -euo pipefail
REF=${1:?git ref}; NAME=${2:?name}; HOW=${3:-auto}
REPO=https://github.com/kividbio/kividb
mkdir -p ~/bin ~/src && cd ~/src
commit=$(git ls-remote "$REPO" "refs/tags/$REF^{}" "refs/tags/$REF" "refs/heads/$REF" | head -1 | cut -f1)
url="$REPO/releases/download/$REF/kividb-linux-aarch64.tar.gz"
if [ "$HOW" != build ] && curl -fsSL "$url" -o "k-$NAME.tgz" 2>/dev/null; then
  rm -rf "k-$NAME" && mkdir "k-$NAME" && tar -xzf "k-$NAME.tgz" -C "k-$NAME"
  install -m 0755 "$(find "k-$NAME" -type f -name kividb | head -1)" ~/bin/kividb-"$NAME"
  echo "ref=$REF commit=$commit from=release-asset $url" > ~/bin/kividb-"$NAME".source
elif [ "$HOW" = release ]; then
  echo "no release asset at $url" >&2; exit 1
else
  rm -rf "kividb-$NAME"
  git clone -q --depth 1 --branch "$REF" "$REPO" "kividb-$NAME"
  cd "kividb-$NAME"
  commit=$(git rev-parse HEAD)
  RUSTFLAGS="-C target-feature=+lse" ~/.cargo/bin/cargo build --release > ~/src/build-$NAME.log 2>&1
  install -m 0755 target/release/kividb ~/bin/kividb-"$NAME"
  echo "ref=$REF commit=$commit from=source cargo build --release RUSTFLAGS=-C target-feature=+lse rustc=$(~/.cargo/bin/rustc --version)" > ~/bin/kividb-"$NAME".source
fi
cat ~/bin/kividb-"$NAME".source
~/bin/kividb-"$NAME" --version
