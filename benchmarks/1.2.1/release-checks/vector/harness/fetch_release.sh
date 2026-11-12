#!/usr/bin/env bash
# Runs on the SERVER. Puts a KiviDB binary built by .github/workflows/release.yml
# (default variant, arm64) in ~/bins/<name>/. This replaces build_kividb.sh in the
# 1.2.1 round: both builds are release.yml's own binaries, not local rebuilds.
#   fetch_release.sh v1.2.0 120
#       downloads the release's kividb-linux-aarch64.tar.gz and checks it against
#       the release's SHA256SUMS.txt
#   fetch_release.sh ~/kividb-linux-aarch64.tar.gz 121 <expected tarball sha256>
#       a tarball fetched from a release.yml run (gh run download <run> -n
#       dist-linux-arm64-default), checked against the SHA-256 given
# -> ~/bins/<name>/{kividb,source,tarball-sha256,sha256,version}
set -euo pipefail
SRC=$1 NAME=$2 WANT=${3:-}
ASSET=kividb-linux-aarch64.tar.gz
D=~/bins/$NAME
rm -rf "$D" /tmp/rel-$NAME && mkdir -p "$D" /tmp/rel-$NAME && cd /tmp/rel-$NAME
if [ -f "$SRC" ]; then
  cp "$SRC" "$ASSET"
  [ -n "$WANT" ] || { echo "give the expected tarball SHA-256" >&2; exit 1; }
  echo "$WANT  $ASSET" | sha256sum -c -
else
  U=https://github.com/kividbio/kividb/releases/download/$SRC
  curl -fsSLO "$U/$ASSET"
  curl -fsSLO "$U/SHA256SUMS.txt"
  grep -E " (\./)?$ASSET\$" SHA256SUMS.txt | sed "s/^\([0-9a-f]*\) .*/\1  $ASSET/" | sha256sum -c -
fi
tar xzf "$ASSET"
BIN=$(find . -type f -name kividb -perm -u+x | head -1)
cp "$BIN" "$D/kividb"
echo "$SRC" > "$D/source"
sha256sum "$ASSET" | awk '{print $1}' > "$D/tarball-sha256"
sha256sum "$D/kividb" | awk '{print $1}' > "$D/sha256"
"$D/kividb" --version > "$D/version" 2>&1 || true
echo "FETCHED $NAME from $SRC tarball=$(cat "$D/tarball-sha256") binary=$(cat "$D/sha256") version=$(cat "$D/version")"
