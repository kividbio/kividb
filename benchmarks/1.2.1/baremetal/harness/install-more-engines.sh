#!/usr/bin/env bash
# Install Valkey, KeyDB and Garnet on the server, next to KiviDB, Dragonfly
# and Redis. Run once, as the ubuntu user, with no benchmark in progress: the
# KeyDB build uses every core for a few minutes.
#
# Versions are pinned here; start-server.sh records what was actually run.
set -euo pipefail

VALKEY_VERSION="9.1.2"
KEYDB_VERSION="v6.3.4"
GARNET_VERSION="v2.2.5"
DOTNET_CHANNEL="10.0"

ARCH=$(uname -m)
case $ARCH in
  aarch64) VALKEY_ARCH=arm64 GARNET_ARCH=arm64 ;;
  x86_64) VALKEY_ARCH=x86_64 GARNET_ARCH=x64 ;;
  *) echo "unsupported architecture: $ARCH" >&2; exit 1 ;;
esac
CODENAME=$(lsb_release -cs)
mkdir -p "$HOME/bin" "$HOME/src"
cd "$HOME/src"

# ── Valkey: the project's own binary package ────────────────────────────────
if [[ ! -x $HOME/bin/valkey-server ]]; then
  curl -fsSL "https://download.valkey.io/releases/valkey-${VALKEY_VERSION}-${CODENAME}-${VALKEY_ARCH}.tar.gz" -o valkey.tar.gz
  tar -xzf valkey.tar.gz
  install -m 0755 "valkey-${VALKEY_VERSION}-${CODENAME}-${VALKEY_ARCH}/bin/valkey-server" "$HOME/bin/valkey-server"
  rm -rf valkey.tar.gz "valkey-${VALKEY_VERSION}-${CODENAME}-${VALKEY_ARCH}"
fi

# ── KeyDB: built from its release tag (it publishes no arm64 package) ───────
if [[ ! -x $HOME/bin/keydb-server ]]; then
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y build-essential nasm autotools-dev autoconf \
    libjemalloc-dev tcl tcl-dev uuid-dev libcurl4-openssl-dev libbz2-dev libzstd-dev \
    liblz4-dev libsnappy-dev libssl-dev pkg-config > /dev/null
  [[ -d KeyDB ]] || git clone --quiet --depth 1 --branch "$KEYDB_VERSION" https://github.com/Snapchat/KeyDB
  (cd KeyDB && make -j"$(nproc)" > "$HOME/src/keydb-build.log" 2>&1)
  install -m 0755 KeyDB/src/keydb-server "$HOME/bin/keydb-server"
fi

# ── Garnet: the release package, on the .NET runtime it needs ───────────────
if [[ ! -x $HOME/garnet/GarnetServer ]]; then
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y xz-utils libicu-dev > /dev/null
  curl -fsSL https://dot.net/v1/dotnet-install.sh -o dotnet-install.sh
  bash dotnet-install.sh --runtime dotnet --channel "$DOTNET_CHANNEL" --install-dir "$HOME/dotnet" > /dev/null
  curl -fsSL "https://github.com/microsoft/garnet/releases/download/${GARNET_VERSION}/linux-${GARNET_ARCH}-based.tar.xz" -o garnet.tar.xz
  rm -rf garnet-unpacked && mkdir garnet-unpacked && tar -xJf garnet.tar.xz -C garnet-unpacked
  rm -rf "$HOME/garnet" && mv "garnet-unpacked/net${DOTNET_CHANNEL}" "$HOME/garnet"
  chmod +x "$HOME/garnet/GarnetServer"
  echo "$GARNET_VERSION" > "$HOME/garnet/VERSION"
  rm -rf garnet.tar.xz garnet-unpacked
fi

echo "Valkey: $("$HOME/bin/valkey-server" --version)"
echo "KeyDB:  $("$HOME/bin/keydb-server" --version)"
echo "Garnet: $(cat "$HOME/garnet/VERSION") on .NET $("$HOME/dotnet/dotnet" --list-runtimes | head -1)"
