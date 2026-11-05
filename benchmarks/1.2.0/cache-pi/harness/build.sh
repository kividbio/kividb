#!/bin/bash
# build.sh <kind> <ref> <name>   (runs on the builder VM)
#   kind=kividb  ref=<tag or commit>  cargo build --release --features tls (the reporter's build command)
#   kind=kividb-tar ref=<release tag> official release tarball kividb-tls-linux-aarch64.tar.gz
#   kind=redis   ref=<tag or commit>  make -j BUILD_TLS=yes (the reporter's build command)
# The binary lands in ~/bin/<name>, with ~/bin/<name>.info (source, commit, sha256, toolchain),
# and is copied to every address in ~/servers.txt (as ~/bin/<name> there; .info last, so its
# presence means the binary is complete).
set -ex
KIND=$1; REF=$2; NAME=$3
W=~/build/$NAME; rm -rf "$W"; mkdir -p "$W" ~/bin; cd "$W"
case $KIND in
kividb)
  . ~/.cargo/env
  git clone -q https://github.com/kividbio/kividb.git src && cd src && git checkout -q "$REF"
  INFO="kividb $REF $(git rev-parse HEAD) $(git log -1 --format=%cI); cargo build --release --features tls; $(rustc --version)"
  cargo build --release --features tls 2>&1 | tail -3
  cp target/release/kividb ~/bin/"$NAME"
  ;;
kividb-tar)
  curl -sSfL -o k.tgz "https://github.com/kividbio/kividb/releases/download/$REF/kividb-tls-linux-aarch64.tar.gz"
  mkdir t && tar xzf k.tgz -C t
  cp "$(find t -type f -name kividb | head -1)" ~/bin/"$NAME"
  INFO="kividb $REF release tarball kividb-tls-linux-aarch64.tar.gz (sha256 $(sha256sum k.tgz | cut -c1-64))"
  ;;
redis)
  git clone -q https://github.com/redis/redis.git src && cd src && git checkout -q "$REF"
  INFO="redis $REF $(git rev-parse HEAD) $(git log -1 --format=%cI); make -j BUILD_TLS=yes; $(gcc --version | head -1)"
  make -j"$(nproc)" BUILD_TLS=yes 2>&1 | tail -3
  cp src/redis-server ~/bin/"$NAME"
  ;;
*) echo "unknown kind $KIND"; exit 2 ;;
esac
cd ~/bin
echo "$INFO; binary sha256 $(sha256sum "$NAME" | cut -c1-64); --version: $(./"$NAME" --version 2>&1 | head -1)" > "$NAME.info"
cat "$NAME.info"
SSHO="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
for h in $(cat ~/servers.txt); do
  scp $SSHO "$NAME" "$h:bin/$NAME.part" && ssh $SSHO "$h" "mv bin/$NAME.part bin/$NAME" && scp $SSHO "$NAME.info" "$h:bin/"
done
echo "BUILT $NAME"
