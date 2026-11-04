#!/bin/bash
# Engine VM: format and mount the local NVMe disk for data files, pull the pinned competitor images, and build
# each KiviDB under test from a git ref with the repo's own Dockerfile (`cargo build --release`, Rust 1.89,
# default variant; +lse on arm64), on this VM, the same way for every ref.
#   setup-server.sh <name>=<git-ref> ...     e.g. setup-server.sh kividb-cand=75482abb kividb-rc4=v1.2.0-rc4
# Writes /var/tmp/<name>-image (the image tag eng.sh runs) and /var/tmp/<name>-commit.
set -euxo pipefail
# local NVMe temp disk: the one whole disk with no partitions and no mount
if ! mountpoint -q /mnt/nvme; then
  DEV=$(lsblk -dpno NAME,TYPE | awk '$2=="disk"{print $1}' | while read -r d; do
    [ "$(lsblk -no NAME "$d" | wc -l)" = 1 ] && ! findmnt -S "$d" >/dev/null && echo "$d"; done | sed -n 1p)
  mkfs.ext4 -F -q -E nodiscard "$DEV"
  mkdir -p /mnt/nvme && mount -o noatime "$DEV" /mnt/nvme
fi
mkdir -p /mnt/nvme/data && chmod 777 /mnt/nvme/data
df -h /mnt/nvme
for i in redis:8.10.3 valkey/valkey:9.1.3 docker.dragonflydb.io/dragonflydb/dragonfly:v2.0.2 kividb/kividb:v1.1.3; do
  docker pull "$i"
done
rm -rf /opt/kividb
git clone https://github.com/kividbio/kividb.git /opt/kividb
for spec in "$@"; do
  NAME=${spec%%=*}; REF=${spec#*=}
  cd /opt/kividb && git checkout -f "$REF"
  SHA=$(git rev-parse --short=12 HEAD)
  git log -1 --format='%H %cI %s' > "/var/tmp/$NAME-commit"
  docker build -t "kividb/kividb:build-$SHA" . > "/var/tmp/$NAME-build.log" 2>&1
  echo "kividb/kividb:build-$SHA" > "/var/tmp/$NAME-image"
  docker run --rm "kividb/kividb:build-$SHA" --version > "/var/tmp/$NAME-version" 2>&1 || true
done
docker images --digests > /var/tmp/engine-images.txt
uname -a > /var/tmp/kernel.txt; lscpu > /var/tmp/lscpu.txt; free -g > /var/tmp/free.txt
touch /var/tmp/engine-ready
