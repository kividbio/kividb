#!/usr/bin/env bash
# The database machine: KiviDB 1.1.2, the release binary (default build),
# listening on 6380 for the client machine.
#
#   KIVIDB_PASSWORD=... ./server.sh
#
# Only the client machine should be able to reach 6380: keep the two machines
# in one private network and the port closed to the internet.
set -eu
: "${KIVIDB_PASSWORD:?set KIVIDB_PASSWORD}"
VERSION=${KIVIDB_VERSION:-1.1.2}

sudo sysctl -w net.core.somaxconn=65535 >/dev/null
cd "$HOME"
curl -sSL -o kividb.tgz \
  "https://github.com/kividbio/kividb/releases/download/v${VERSION}/kividb-linux-x86_64.tar.gz"
rm -rf "kividb-${VERSION}" && mkdir "kividb-${VERSION}" && tar xzf kividb.tgz -C "kividb-${VERSION}"
BIN=$(find "$HOME/kividb-${VERSION}" -type f -name kividb | head -1)
"$BIN" --version

mkdir -p "$HOME/kdata"
ulimit -n 65535
nohup "$BIN" --port 6380 --bind 0.0.0.0 --protected-mode no \
  --requirepass "$KIVIDB_PASSWORD" --dir "$HOME/kdata" --configfile none.conf \
  > "$HOME/kividb.log" 2>&1 &
sleep 2
grep -E "starting|Ready" "$HOME/kividb.log"
