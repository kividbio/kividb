#!/bin/bash
# Engine VM. Formats and mounts the local NVMe disk for data files, pulls the pinned competitor images, and
# prepares the two KiviDB builds, both binaries that .github/workflows/release.yml built (default variant,
# x86_64), neither rebuilt here:
#   kividb-1.2.0  kividb-linux-x86_64.tar.gz from the v1.2.0 GitHub release, checked against its SHA256SUMS.txt
#   kividb-1.2.1  kividb-linux-x86_64.tar.gz of release.yml's run for the commit under test
#                 (gh run download <run-id> -R kividbio/kividb -n dist-linux-amd64-default), copied to this VM
#                 and checked against the SHA-256 given on the command line
# Each binary is wrapped in the same minimal image (ubuntu:24.04 + the binary, ENTRYPOINT kividb, run as root)
# so that eng.sh and warefeats' compose files start both the same way.
#   setup-server.sh /var/tmp/kividb-linux-x86_64.tar.gz <its sha256>
# Writes /var/tmp/<name>-image (the tag eng.sh runs), -version, -sha256 (binary), -tarball-sha256, -kividb.
set -euxo pipefail
NEW_TGZ=$1; NEW_SHA=$2
if ! mountpoint -q /mnt/nvme; then
  DEV=$(lsblk -dpno NAME,TYPE | awk '$2=="disk"{print $1}' | while read -r d; do
    [ "$(lsblk -no NAME "$d" | wc -l)" = 1 ] && ! findmnt -S "$d" >/dev/null && echo "$d"; done | sed -n 1p)
  mkfs.ext4 -F -q -E nodiscard "$DEV"
  mkdir -p /mnt/nvme && mount -o noatime "$DEV" /mnt/nvme
fi
mkdir -p /mnt/nvme/data && chmod 777 /mnt/nvme/data
df -h /mnt/nvme
for i in redis:8.10.3 valkey/valkey:9.1.3 docker.dragonflydb.io/dragonflydb/dragonfly:v2.0.2; do
  docker pull "$i"
done

# 1.2.0: the release tarball, checked against the release's SHA256SUMS.txt
mkdir -p /var/tmp/rel120 && cd /var/tmp/rel120
U=https://github.com/kividbio/kividb/releases/download/v1.2.0
curl -fsSLO "$U/kividb-linux-x86_64.tar.gz"
curl -fsSLO "$U/SHA256SUMS.txt"
grep -E ' (./)?kividb-linux-x86_64.tar.gz$' SHA256SUMS.txt | sed 's/^\([0-9a-f]*\) .*/\1  kividb-linux-x86_64.tar.gz/' | sha256sum -c - | tee /var/tmp/kividb-1.2.0-tarball-check
tar xzf kividb-linux-x86_64.tar.gz
cp "$(find . -type f -name kividb -perm -u+x | head -1)" /var/tmp/kividb-1.2.0-kividb
sha256sum kividb-linux-x86_64.tar.gz | awk '{print $1}' > /var/tmp/kividb-1.2.0-tarball-sha256

# 1.2.1: release.yml's tarball for the commit under test, checked against the SHA-256 given
rm -rf /var/tmp/rel121 && mkdir -p /var/tmp/rel121 && cd /var/tmp/rel121
cp "$NEW_TGZ" kividb-linux-x86_64.tar.gz
echo "$NEW_SHA  kividb-linux-x86_64.tar.gz" | sha256sum -c - | tee /var/tmp/kividb-1.2.1-tarball-check
tar xzf kividb-linux-x86_64.tar.gz
cp "$(find . -type f -name kividb -perm -u+x | head -1)" /var/tmp/kividb-1.2.1-kividb
sha256sum kividb-linux-x86_64.tar.gz | awk '{print $1}' > /var/tmp/kividb-1.2.1-tarball-sha256

for NAME in kividb-1.2.0 kividb-1.2.1; do
  D=/var/tmp/img-$NAME; rm -rf "$D"; mkdir -p "$D"
  cp "/var/tmp/$NAME-kividb" "$D/kividb"
  printf 'FROM ubuntu:24.04\nCOPY kividb /usr/local/bin/kividb\nENTRYPOINT ["kividb"]\nCMD ["--protected-mode", "no"]\n' > "$D/Dockerfile"
  docker build -q -t "kividb/kividb:bin-$NAME" "$D"
  echo "kividb/kividb:bin-$NAME" > "/var/tmp/$NAME-image"
  docker run --rm "kividb/kividb:bin-$NAME" --version > "/var/tmp/$NAME-version" 2>&1 || true
  sha256sum "/var/tmp/$NAME-kividb" | awk '{print $1}' > "/var/tmp/$NAME-sha256"
done
docker images --digests > /var/tmp/engine-images.txt
uname -a > /var/tmp/kernel.txt; lscpu > /var/tmp/lscpu.txt; free -g > /var/tmp/free.txt
touch /var/tmp/engine-ready
