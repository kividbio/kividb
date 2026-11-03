#!/usr/bin/env bash
# On the fresh Ubuntu 24.04 VM: toolchain, two trees (base a6b1bf38 and base + git am patch).
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -qq
sudo apt-get install -y -qq build-essential cmake clang pkg-config libssl-dev git curl python3-venv redis-tools iproute2 > /dev/null
if [ ! -x "$HOME/.cargo/bin/cargo" ]; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal -c clippy,rustfmt > /dev/null
fi
source "$HOME/.cargo/env"
rustup update stable > /dev/null 2>&1 || true
rustc --version; cargo clippy --version
python3 -m venv "$HOME/venv"
"$HOME/venv/bin/pip" install -q redis matplotlib
git config --global user.name verify; git config --global user.email verify@localhost
for t in base patched; do
  rm -rf "$HOME/$t"; mkdir -p "$HOME/$t"; tar xzf "$HOME/base.tgz" -C "$HOME/$t"
  (cd "$HOME/$t" && git init -q && git add -A && git add -f tests/fixtures tests/e2e/requirements.txt 2>/dev/null; git commit -qm base)
done
cd "$HOME/patched" && git am "$HOME/fix.patch" && git log --oneline -2
nproc; free -g | sed -n 2p; df -h / | tail -1
echo SETUP-OK
