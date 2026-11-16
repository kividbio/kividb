#!/usr/bin/env bash
# Wait for setup-server-min.sh, then build rc10 and rc8 one after the other
# with fetch-kividb.sh (cargo build --release, RUSTFLAGS=-C target-feature=+lse).
set -uo pipefail
for _ in $(seq 360); do grep -q "azure" ~/setup.out 2>/dev/null && break; sleep 5; done
{
  echo "rustc: $(~/.cargo/bin/rustc --version)"; echo "cargo: $(~/.cargo/bin/cargo --version)"
  echo "== rc10"; bash ~/h/fetch-kividb.sh 39bfacd231bb294d390448eacfe0e243d365ebe8 rc10 build 2>&1
  echo "== rc8";  bash ~/h/fetch-kividb.sh 082dab3d8929c180bccf993835e28992ea054fb5 rc8 build 2>&1
  echo "== sha256"; cd ~/bin && sha256sum kividb-rc10 kividb-rc8
} > ~/builds.txt 2>&1
echo BUILDS-DONE >> ~/builds.txt
